

import pandas as pd
import geopandas as gpd
import numpy as np
import requests
import json
from scipy.spatial.distance import cdist
from scipy import interpolate
from sklearn.impute import KNNImputer
import warnings
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

warnings.filterwarnings('ignore')



class PipelineTemperaturasRM:
    
    def __init__(self, años_inicio=2019, años_fin=2026):
        self.años = range(años_inicio, años_fin)
        self.mail = os.getenv("MAIL")
        self.api_key = os.getenv("API_KEY")
        
    def paso_1_extraer_datos_api(self, forzar_descarga=False):
               
        estaciones_url = f"https://climatologia.meteochile.gob.cl/application/servicios/getEstacionesRedEma?usuario={self.mail}&token={self.api_key}"
        
        try:
            response = requests.get(estaciones_url)
            data = response.json()
            estaciones_data = data['datosEstacion']
            
            estaciones_list = []
            for est in estaciones_data:
                estaciones_list.append({
                    'Nombre_Estacion': est['nombreEstacion'],
                    'Latitud': est['latitud'],
                    'Longitud': est['longitud'],
                    'Codigo_Estacion': est['codigoNacional'],
                    'Region': est['region']
                })
            
            df_estaciones = pd.DataFrame(estaciones_list)
            
            df_estaciones_rm = df_estaciones[df_estaciones['Region'] == 13].copy()
            
        except Exception as e:
            return None
                
        datos_completos = []
        total_requests = len(df_estaciones_rm) * len(self.años)
        contador = 0
        
        for año in self.años:
            for _, estacion in df_estaciones_rm.iterrows():
                contador += 1
                codigo = estacion['Codigo_Estacion']
                nombre = estacion['Nombre_Estacion']
                
                temp_url = f"https://climatologia.meteochile.gob.cl/application/servicios/getTemperaturaHistoricaDiaria/{codigo}/{año}?usuario={self.mail}&token={self.api_key}"
                
                try:
                    response = requests.get(temp_url)
                    data = response.json()
                    
                    if 'datos' in data:
                        for mes, dias_data in data['datos'].items():
                            for dia, temp_data in dias_data.items():
                                if temp_data['media'] is not None:
                                    datos_completos.append({
                                        'Año': año,
                                        'Mes': int(mes),
                                        'Dias': int(dia),
                                        'Temperatura_Media': temp_data['media'],
                                        'Nombre_Estacion': nombre,
                                        'Latitud': estacion['Latitud'],
                                        'Longitud': estacion['Longitud'],
                                        'Region': estacion['Region'],
                                        'Codigo_Estacion': codigo
                                    })
                
                except Exception as e:
                    continue
        
        if datos_completos:
            df_temperaturas = pd.DataFrame(datos_completos)
            df_temperaturas['Fecha'] = pd.to_datetime(
                df_temperaturas['Año'].astype(str) + '-' + 
                df_temperaturas['Mes'].astype(str) + '-' + 
                df_temperaturas['Dias'].astype(str)
            )
            
            
            return df_temperaturas
        else:
            return None
    
    def paso_2_asignar_comunas(self, df_temperaturas, shapefile_path='datos/Comunas/comunas.shp'):
   
        try:
            comunas_gdf = gpd.read_file(shapefile_path)
            comunas_rm = comunas_gdf[comunas_gdf['codregion'] == 13].copy()
        except Exception as e:
            return None, None
        
        estaciones = df_temperaturas[['Nombre_Estacion', 'Latitud', 'Longitud', 'Codigo_Estacion']].drop_duplicates()
        estaciones = estaciones.dropna(subset=['Latitud', 'Longitud'])
        
        centroides = comunas_rm.to_crs('EPSG:32719')  
        centroides['centroid'] = centroides.geometry.centroid
        centroides = centroides.to_crs('EPSG:4326')
        centroides['centroid'] = centroides['centroid'].to_crs('EPSG:4326')
        centroides['lat_centroid'] = centroides.centroid.y
        centroides['lon_centroid'] = centroides.centroid.x
        
        coords_estaciones = estaciones[['Latitud', 'Longitud']].astype(float).values
        coords_comunas = centroides[['lat_centroid', 'lon_centroid']].astype(float).values
        
        distancias = cdist(coords_comunas, coords_estaciones, metric='euclidean')
        idx_estacion_cercana = np.argmin(distancias, axis=1)
        distancia_min = np.min(distancias, axis=1)
        
        centroides['estacion_cercana'] = estaciones.iloc[idx_estacion_cercana]['Codigo_Estacion'].values
        centroides['nombre_estacion_cercana'] = estaciones.iloc[idx_estacion_cercana]['Nombre_Estacion'].values
        centroides['distancia_km'] = distancia_min * 111  # Conversión a km
        
        df_temperaturas['Fecha'] = pd.to_datetime(df_temperaturas['Fecha'])
        series_comunas = []
        
        for _, comuna in centroides.iterrows():
            nombre_comuna = comuna['Comuna']
            codigo_estacion = comuna['estacion_cercana']
            
            datos_estacion = df_temperaturas[df_temperaturas['Codigo_Estacion'] == codigo_estacion].copy()
            
            if not datos_estacion.empty:
                datos_estacion['Comuna'] = nombre_comuna
                datos_estacion['Distancia_Estacion_km'] = comuna['distancia_km']
                series_comunas.append(datos_estacion)
        
        if series_comunas:
            df_comunas = pd.concat(series_comunas, ignore_index=True)
            
            
            asignaciones = centroides[['Comuna', 'nombre_estacion_cercana', 'distancia_km']].copy()
            asignaciones.columns = ['Comuna', 'Estacion_Cercana', 'Distancia_km']
                        
            return df_comunas, asignaciones
        else:
            return None, None
    
    def paso_3_reconstruir_series(self, df_comunas, metodo='knn'):
     
        nans_inicial = df_comunas['Temperatura_Media'].isna().sum()
        porcentaje_inicial = (nans_inicial / len(df_comunas)) * 100
        
        if nans_inicial == 0:
            return df_comunas
                
        if metodo.lower() == 'lineal':
            df_reconstruido = self._interpolacion_lineal(df_comunas)
        elif metodo.lower() == 'estacional':
            df_reconstruido = self._interpolacion_estacional(df_comunas)
        elif metodo.lower() == 'knn':
            df_reconstruido = self._interpolacion_knn(df_comunas)
        else:
            df_reconstruido = self._interpolacion_knn(df_comunas)
        
        nans_final = df_reconstruido['Temperatura_Media'].isna().sum()
        reduccion = ((nans_inicial - nans_final) / nans_inicial * 100) if nans_inicial > 0 else 0
        
        
        return df_reconstruido
    
    def _interpolacion_lineal(self, df):
        df_result = df.copy()
        
        for comuna in df['Comuna'].unique():
            mask = df_result['Comuna'] == comuna
            comuna_data = df_result.loc[mask].copy().sort_values('Fecha')
            comuna_data['Temperatura_Media'] = comuna_data['Temperatura_Media'].interpolate(method='linear')
            comuna_data['Temperatura_Media'] = comuna_data['Temperatura_Media'].fillna(method='ffill').fillna(method='bfill')
            df_result.loc[mask, 'Temperatura_Media'] = comuna_data['Temperatura_Media'].values
        
        return df_result
    
    def _interpolacion_estacional(self, df):
        df_result = df.copy()
        df_result['mes'] = df_result['Fecha'].dt.month
        df_result['dia_año'] = df_result['Fecha'].dt.dayofyear
        df_result['año'] = df_result['Fecha'].dt.year
        
        for comuna in df['Comuna'].unique():
            mask = df_result['Comuna'] == comuna
            comuna_data = df_result.loc[mask].copy().sort_values('Fecha')
            
            patron_estacional = comuna_data.groupby('dia_año')['Temperatura_Media'].median()
            missing_mask = comuna_data['Temperatura_Media'].isna()
            
            if missing_mask.any():
                for idx in comuna_data[missing_mask].index:
                    dia_año = comuna_data.loc[idx, 'dia_año']
                    
                    if dia_año in patron_estacional:
                        temp_base = patron_estacional[dia_año]
                    else:
                        mes = comuna_data.loc[idx, 'mes']
                        temp_base = comuna_data[comuna_data['mes'] == mes]['Temperatura_Media'].median()
                    
                    if pd.isna(temp_base):
                        temp_base = comuna_data['Temperatura_Media'].median()
                    
                    comuna_data.loc[idx, 'Temperatura_Media'] = temp_base
            
            df_result.loc[mask, 'Temperatura_Media'] = comuna_data['Temperatura_Media'].values
        
        return df_result
    
    def _interpolacion_knn(self, df):
        df_result = df.copy()
        fechas_unicas = sorted(df['Fecha'].unique())
        
        for fecha in fechas_unicas:
            mask_fecha = df_result['Fecha'] == fecha
            datos_fecha = df_result.loc[mask_fecha].copy()
            
            if datos_fecha['Temperatura_Media'].isna().any():
                features = datos_fecha[['Latitud', 'Longitud', 'Distancia_Estacion_km']].values
                temperatures = datos_fecha['Temperatura_Media'].values.reshape(-1, 1)
                
                imputer = KNNImputer(n_neighbors=3, weights='distance')
                combined_data = np.hstack([features, temperatures])
                imputed_data = imputer.fit_transform(combined_data)
                temperatures_imputed = imputed_data[:, -1]
                
                df_result.loc[mask_fecha, 'Temperatura_Media'] = temperatures_imputed
        
        return df_result
    
    def ejecutar_pipeline_completo(self, forzar_descarga_api=False, metodo_reconstruccion='knn'):

        df_temperaturas = self.paso_1_extraer_datos_api(forzar_descarga=forzar_descarga_api)
        if df_temperaturas is None:
            return None
        
        df_comunas, asignaciones = self.paso_2_asignar_comunas(df_temperaturas)
        if df_comunas is None:
            return None
        
        df_final = self.paso_3_reconstruir_series(df_comunas, metodo=metodo_reconstruccion)
        if df_final is None:
            return None

        
        return df_final

def main():
    
    pipeline = PipelineTemperaturasRM(años_inicio=2019, años_fin=2026)
    
    print("Ejecutando pipeline completo...")
    resultado = pipeline.ejecutar_pipeline_completo(
        forzar_descarga_api=False,  
        metodo_reconstruccion='knn' 
    )
    
if __name__ == "__main__":
    main()