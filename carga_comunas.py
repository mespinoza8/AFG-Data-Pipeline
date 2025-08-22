import geopandas as gpd
import pandas as pd
import numpy as np

def cargar_shapefile_comunas():
    ruta_shapefile = "datos/Comunas/comunas.shp"
    
    gdf_all = gpd.read_file(ruta_shapefile)
    print(f"Total comunas en archivo: {len(gdf_all)}")
    
    gdf = gdf_all[gdf_all['Region'] == 'Región Metropolitana de Santiago'].copy()
    
    gdf = gdf.reset_index(drop=True)
    
    gdf = gdf.to_crs('EPSG:32719')
    
    print(f"Número de comunas en RM cargadas: {len(gdf)}")
    print(f"Sistema de coordenadas: {gdf.crs}")
    
    return gdf

def detectar_comunas_vecinas(gdf):
    comunas_vecinas = {}
    
    for i, comuna_i in gdf.iterrows():
        vecinas = []
        
        for j, comuna_j in gdf.iterrows():
            if i != j:
                if comuna_i.geometry.touches(comuna_j.geometry):
                    vecinas.append(j)
        
        comunas_vecinas[i] = vecinas
    
    return comunas_vecinas

def crear_matriz_adyacencia(gdf, comunas_vecinas):

    n_comunas = len(gdf)
    matriz = np.zeros((n_comunas, n_comunas), dtype=int)
    
    for comuna_idx, vecinas in comunas_vecinas.items():
        for vecina_idx in vecinas:
            matriz[comuna_idx][vecina_idx] = 1
    
    return matriz

def crear_dataframes_gnn(gdf, matriz_adyacencia):
 
    n_comunas = len(gdf)
    
    edge_index = []
    for i in range(n_comunas):
        for j in range(n_comunas):
            if matriz_adyacencia[i][j] == 1:
                edge_index.append([i, j])
    
    edge_index = np.array(edge_index).T if edge_index else np.array([[], []], dtype=int)
    
    node_features = []
    for i in range(n_comunas):
        features = [
            gdf.iloc[i]['cod_comuna'], 
            gdf.iloc[i]['codregion'],  
            matriz_adyacencia[i].sum(),  
            gdf.iloc[i].geometry.area,   
            len(gdf.iloc[i]['Comuna']),  
        ]
        node_features.append(features)
    
    node_features = np.array(node_features, dtype=float)
    
    df_node_features = pd.DataFrame(
        node_features,
        columns=['cod_comuna', 'cod_region', 'grado', 'area', 'nombre_len']
    )
    df_node_features['id_nodo'] = range(n_comunas)
    df_node_features['nombre_comuna'] = [gdf.iloc[i]['Comuna'] for i in range(n_comunas)]
    df_node_features['provincia'] = [gdf.iloc[i]['Provincia'] for i in range(n_comunas)]
    
    cols = ['id_nodo', 'nombre_comuna', 'provincia', 'cod_comuna', 'cod_region', 'grado', 'area', 'nombre_len']
    df_node_features = df_node_features[cols]
    
    if edge_index.size > 0:
        df_edges = pd.DataFrame({
            'source': edge_index[0],
            'target': edge_index[1],
            'source_name': [gdf.iloc[i]['Comuna'] for i in edge_index[0]],
            'target_name': [gdf.iloc[i]['Comuna'] for i in edge_index[1]]
        })
    else:
        df_edges = pd.DataFrame(columns=['source', 'target', 'source_name', 'target_name'])
    
    nombres_unicos = []
    for i in range(n_comunas):
        nombre_base = gdf.iloc[i]['Comuna']
        nombre_unico = f"{nombre_base}_{i}"
        nombres_unicos.append(nombre_unico)
    
    df_adjacency = pd.DataFrame(
        matriz_adyacencia,
        index=nombres_unicos,
        columns=nombres_unicos
    )
    df_adjacency = df_adjacency.reset_index()
    df_adjacency.rename(columns={'index': 'comuna'}, inplace=True)
    
    return df_node_features, df_edges, df_adjacency

def carga_comunas_gnn():
    gdf = cargar_shapefile_comunas()
    
    comunas_vecinas = detectar_comunas_vecinas(gdf)
    
    total_conexiones = sum(len(vecinas) for vecinas in comunas_vecinas.values())
    
    matriz_adyacencia = crear_matriz_adyacencia(gdf, comunas_vecinas)
    df_node_features, df_edges, df_adjacency = crear_dataframes_gnn(gdf, matriz_adyacencia)
    
    tables_dict = {
        'comunas_node_features': df_node_features,
        'comunas_edges': df_edges,
        'comunas_adjacency_matrix': df_adjacency
    }
    
    print("DataFrames de comunas preparados para PostgreSQL:")
    for table_name, df in tables_dict.items():
        print(f"- {table_name}: {df.shape}")
    
    return tables_dict

if __name__ == "__main__":
    datasets = carga_comunas_gnn()
    
