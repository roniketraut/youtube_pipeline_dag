from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append('/opt/airflow/plugins')  
import os
from youtube import settings
from youtube import load_data
from youtube import data_transformation
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
import pandas as pd

POSTGRES_CONN_ID = "postgres_default"
TARGET_TABLE_NAME = "youtube_channel_stats"

@dag(
    dag_id="youtube_data_pipeline", 
    start_date=datetime(2024, 4, 25), 
    schedule='@daily',
    catchup=False,
    tags=["youtube", "pipeline", "taskflow"] 
)
def youtube_data_pipeline(): 
    """
     YouTube Data Pipeline using TaskFlow API
    Extracts channel stats, transforms the data, and loads it into PostgreSQL.
    Handles DataFrame passing between tasks automatically.
    (Updated to use TaskFlow)
    """

    # Define tasks using the @task decorator
    @task
    def extract_data() -> pd.DataFrame:
        """Task to extract YouTube channel statistics."""
        print("Extracting aggregated channel data...")
        df_raw = settings.get_aggregated_channel_data()
        print(f"Extraction complete. DataFrame shape: {df_raw.shape}")
        return df_raw

    @task
    def transform_data(df_in: pd.DataFrame) -> pd.DataFrame:
        """Task to transform the raw YouTube data."""
        print(f"Transforming DataFrame with shape: {df_in.shape}")
        df_transformed = data_transformation.transform(df_in)
        print(f"Transformation complete. DataFrame shape: {df_transformed.shape}")
        return df_transformed

    @task
    def load_data_task(df_to_load: pd.DataFrame): # Renamed task function slightly
        """Task to load the transformed data into PostgreSQL."""
        print(f"Loading DataFrame with shape: {df_to_load.shape} to Postgres table {TARGET_TABLE_NAME}")
        load_data.load_data_to_postgres_hook_version(
            data_frame=df_to_load,
            target_table=TARGET_TABLE_NAME,
            postgres_conn_id=POSTGRES_CONN_ID
        )
        print("Loading complete.")

    # --- Define Task Dependencies via function calls ---
    raw_dataframe = extract_data()
    transformed_dataframe = transform_data(raw_dataframe)
    load_data_task(transformed_dataframe)
   

# Instantiate the DAG ---
youtube_data_pipeline()

