import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.extractor.extractor import Extractor
from utils.functions_utils import insert_into_file_list

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'extractor_config.yaml'))
extractor = Extractor(config_path=config_path)

def list_files_task(**kwargs): 
    """
    List files in the source directory and store the list in XCom.
    """
    files = extractor.list_files() 
    return files

def insert_files_to_db(**kwargs):
    ti = kwargs['ti']
    file_list = ti.xcom_pull(task_ids='list_files_task')
    
    if not file_list:
        raise ValueError('No files found in Xcoms')
    
    for file in file_list:
        print(f"Inserting {file} into the database")
        filename = file.split("/")[-1]  # Correction: parenthèse fermante manquante
        filepath = file
        mod_time = datetime.fromtimestamp(os.path.getmtime(file))
        data = [filename, filepath, mod_time]
        insert_into_file_list(data)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'list_files_dag',
    default_args=default_args,
    description='List files in the source directory',
    catchup=False
) as dag:
    
    list_files = PythonOperator(
        task_id='list_files_task',
        python_callable=list_files_task
    )
    
    insert_files = PythonOperator(
        task_id='insert_files_to_db',
        python_callable=insert_files_to_db  # Le contexte est automatiquement passé
    )
    
    list_files >> insert_files