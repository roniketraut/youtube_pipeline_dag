from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append('/opt/airflow/plugins')  

from arthistory import extract  



with DAG (
    dag_id = "arthistory_dag",
    start_date = datetime(2025, 4, 16),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    t1 = PythonOperator(
        task_id = "extract_data",
        python_callable = extract.extract
    )