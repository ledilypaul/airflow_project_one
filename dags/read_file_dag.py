from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipeline.extractor.extractor import Extractor
from pipeline.file_reader.base_reader import BaseFileReader
from utils.db_connection import db_connection

# Configuration du chemin du fichier de configuration
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'extractor_config.yaml'))
reader = BaseFileReader()

def read_file_task(**kwargs):
    """
    Read a file from the source directory and process its content.

    Args:
        **kwargs: Additional keyword arguments.

    Returns:
        None
    """
    ti = kwargs['ti']
    file_list = ti.xcom_pull(key='list_files_dag', task_ids='list_files_task')
    if not file_list:
        raise ValueError("No files found in XCom")

    for file in file_list:
        content = reader.read_file(file)
        # Process the content as needed
        print(f"Read content from {file}: {content}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'read_file_dag',
    default_args=default_args,
    description='A simple DAG to read files',
    schedule_interval=timedelta(days=1),
    catchup=False, # Set to False to disable historical DAG runs
)

read_file = PythonOperator(
    task_id='read_file_task',
    python_callable=read_file_task,
    dag=dag,
)

read_file