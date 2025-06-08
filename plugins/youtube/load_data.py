from youtube.data_transformation import transform
from youtube.settings import df1
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
import logging
logging.basicConfig(format = '%(levelname)s: %(message)s', level=logging.DEBUG)
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

df_transformed = transform(df1)

# Define the function that will be called by an Airflow task
def load_data_to_postgres_hook_version(data_frame: pd.DataFrame, target_table: str, postgres_conn_id: str):
    """
    Loads a Pandas DataFrame into a PostgreSQL table using Airflow PostgresHook.

    :param data_frame: The Pandas DataFrame to load.
    :param target_table: The name of the target table in PostgreSQL.
    :param postgres_conn_id: The Airflow connection ID for the PostgreSQL database.
    """
    # Instantiate the hook using the Airflow Connection ID
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # create the engine
    engine = pg_hook.get_sqlalchemy_engine()

    try:
        # 3. Use the engine with pandas.to_sql
        logging.info(f"Loading data into table: {target_table} using connection: {postgres_conn_id}")
        data_frame.to_sql(name=target_table, con=engine, if_exists="append", index=False)
        logging.info(f"Successfully loaded data into {target_table}")
    except Exception as e:
        logging.error(f"Error loading data into {target_table}: {e}")
        raise # Re-raise the exception to fail the Airflow task


