import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipeline.IMDBExtractor import get_files,process_files
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta

PATH = "../../Spark/data/"
FILE_FORMAT = "csv"

args = {
    'owner' : 'airflow',
    "start_date" : datetime(2024,10,21)
}

def list_files_tasks(path, **kwargs):
    return get_files(path)

def process_files_task(path, **kwargs):
    ti = kwargs['ti']
    
    valid_files = ti.xcom_pull(task_ids='list_IMDB_files')
    print("result process files = " + str(process_files(valid_files, path)))    
    return "valid_files"
    # if valid_files is None:
    #     raise ValueError("No valid files were found!")
    # return process_files(valid_files, path)


with DAG(
    dag_id='process_files_dag',
    default_args=args,
    schedule_interval= '@daily'
) as dag:
    list_task = PythonOperator(
        task_id = "list_IMDB_files",
        python_callable=list_files_tasks,
        op_kwargs={'path':PATH},
        provide_context=True,
        execution_timeout=timedelta(seconds=60),
        retries=1
    )
    
    process_task = PythonOperator(
        task_id = "process_IMDB_files",
        python_callable =process_files_task,
        provide_context=True,
        op_kwargs={'path': PATH}
    )
    
    list_task >> process_task #Fixed dependance between task