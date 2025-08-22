import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pyodbc
import logging
from atenciones_urgencia import carga_atenciones_urgencia
from carga_comunas import carga_comunas_gnn
from temperaturas_rm import PipelineTemperaturasRM


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Ingestion:
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.data = None

    def save_to_db(self, data_dict):
        if not isinstance(data_dict, dict):
            logger.error("Error: data_dict must be a dictionary with table_name:dataframe pairs.")
            return
        
        if not data_dict:
            logger.error("Error: data_dict is empty.")
            return
        
        for table_name, dataframe in data_dict.items():
            if not isinstance(dataframe, pd.DataFrame):
                logger.error(f"Error: Value for table '{table_name}' is not a DataFrame.")
                return
            
            try:
                dataframe.to_sql(table_name, schema='public', con=self.db_uri, if_exists='replace', index=False,chunksize=10000)
                logger.info(f"Data saved to {table_name} table in the database.")
            except Exception as e:
                logger.error(f"Error saving DataFrame to {table_name}: {str(e)}")
        
        try:
            self.db_uri.dispose()
        except Exception as e:
            logger.error(f"Error disposing database connection: {str(e)}")


if __name__ == "__main__":

    load_dotenv()

    DB_HOST = os.getenv("db_host")
    DB_USER = os.getenv("db_user")
    DB_PASSWORD = os.getenv("db_password")
    DB_PORT = os.getenv("db_port")
    DB_NAME = os.getenv("db_name")

    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(DATABASE_URL)

    ingestion=Ingestion(engine)

    df_au = carga_atenciones_urgencia()
    
    comunas_dict = carga_comunas_gnn()
    
    pipeline_temp = PipelineTemperaturasRM(años_inicio=2019, años_fin=2026)
    
    try:
        df_temperaturas = pipeline_temp.ejecutar_pipeline_completo(
            forzar_descarga_api=False,
            metodo_reconstruccion='knn'
        )
        print(f"Datos de temperatura extraídos: {df_temperaturas.shape if df_temperaturas is not None else 'None'}")
    except Exception as e:
        print(f"Error en pipeline de temperaturas: {e}")
        df_temperaturas = None
    
    tables_dict = {
        "atenciones_urgencias": df_au,
        **comunas_dict  
    }
    
    if df_temperaturas is not None:
        tables_dict["temperaturas_rm"] = df_temperaturas
    
    print(f"\nTablas a cargar en PostgreSQL:")
    for table_name, df in tables_dict.items():
        print(f"- {table_name}: {df.shape}")

    ingestion.save_to_db(tables_dict)