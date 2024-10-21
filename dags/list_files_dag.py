import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipeline.IMDBExtractor import main
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

PATH = "../../Spark/data/"
FILE_FORMAT = "csv"

args = {
    'owner' : 'airflow',
    "start_date" : datetime(2024,10,21)
}


with DAG(
    dag_id='list_files_dag',
    default_args=args,
    schedule_interval= '@daily'
) as dag:
    list_task = PythonOperator(
        task_id = "list_IMDB_files",
        python_callable=main,
        op_kwargs={'path':PATH,'file_format':FILE_FORMAT}
    )
    list_task