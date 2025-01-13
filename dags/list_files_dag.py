import sys,os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipeline.extractor.extractor import Extractor
from pipeline.extractor.extractor import Extractor
from utils.db_connection import db_connection
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'extractor_config.yaml'))
extractor = Extractor(config_path=config_path) 

def list_files_task(**kwargs): 
    """
    List files in the source directory and store the list in XCom.

    Args:
        **kwargs: Additional keyword arguments.

    Returns:
        None
    """
    ti = kwargs['ti'] 
    files = extractor.list_files() 
    ti.xcom_push(key='file_list',value=files)

# def insert_files_to_db(**context):
#     file_data = context['ti'].xcom_pull(key='file_list')
#     if file_data

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'retries' : 2,
    'start_date' : datetime(2024,12,18)
}

with DAG(
    dag_id="list_files_dag",
    default_args=default_args,
    schedule_interval="@daily"
) as dag:
    list_task = PythonOperator(
        task_id = "list_files",
        python_callable=list_files_task
    )
    
