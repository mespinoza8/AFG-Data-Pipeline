import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pyodbc
import logging
from atenciones_urgencia import carga_atenciones_urgencia


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
                dataframe.to_sql(table_name, schema='public', con=self.db_uri, if_exists='replace', index=False)
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
    tables_dict={"atenciones_urgencias":df_au}

    ingestion.save_to_db(tables_dict)