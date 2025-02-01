import sys,os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta
from pipeline.extractor.extractor import Extractor
from utils.functions_utils import insert_into_file_list
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
    # ti.xcom_push(key='file_list',value=files)
    return files

def insert_files_to_db(**kwargs):
    file_list = kwargs['ti'].xcom_pull(task_ids='list_files_task')  # Récupération de la liste des fichiers depuis les XComs
    if not file_list:
        raise ValueError('No files found in Xcoms')
    
    for file in file_list:
        print(f"Inserting {file} into the database")
        data = [file.split("/"[-1]), file, datetime.fromtimestamp(os.path.getmtime(file))]
        insert_into_file_list(data)
        
default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024,12,18),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 0,
    'retry_delay' : timedelta(minutes=5)
}

dag = DAG(
    'list_files_dag',
    default_args=default_args,
    description='List files in the source directory',
    schedule_interval=timedelta(days=1),
    catchup=False # Set to False to disable historical DAG runs
)
list_files = PythonOperator(
    task_id='list_files_task',
    python_callable=list_files_task,
    dag=dag,
)

insert_files = PythonOperator(
    task_id='insert_files_to_db',
    provide_context=True,
    python_callable=insert_files_to_db,
    dag=dag,
)

list_files >> insert_files
    

    
