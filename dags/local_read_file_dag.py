from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipeline.extractor.extractor import Extractor
from pipeline.file_reader.base_reader import BaseFileReader
from utils.db_connection import db_connection
from utils.functions_utils import list_file_from_db
from utils.spark_session import get_spark_session
# Configuration du chemin du fichier de configuration
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'extractor_config.yaml'))

def list_file_task(**kwargs):
    raw_file_list = list_file_from_db()
    if not raw_file_list:
        raise ValueError("No files in file list")
    file_list = [
        {
            "id": file[0],
            "name": file[1],
            "path": file[2],
            "created_at": str(file[3]),  
            "processed_at": str(file[6]),
            "process_date": str(file[7])
        }
        for file in raw_file_list
    ] #Convert into dict to be able to pass through xcom
    kwargs['ti'].xcom_push(key='file_list', value=file_list)

def read_file_task(**kwargs):
    """
    Read a file from the source directory and process its content.

    Args:
        **kwargs: Additional keyword arguments.

    Returns:
        None
    """
    spark = get_spark_session()
    reader = BaseFileReader(spark)
    file_list = kwargs['ti'].xcom_pull(key='file_list', task_ids='list_file_task')

    for file in file_list:
        content = reader.read_file(file["path"])
        # Process the content as needed
        print(f"Read content from {file}: {str(content)}")
        content.show(truncate=False)  # Affiche les lignes dans les logs Airflow

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
    # schedule_interval=timedelta(days=1),
    catchup=False, # Set to False to disable historical DAG runs
)

list_file = PythonOperator(
    task_id='list_file_task',
    python_callable=list_file_task,
    dag=dag,
)

read_file = PythonOperator(
    task_id='read_file_task',
    python_callable=read_file_task,
    dag=dag,
)

list_file >> read_file