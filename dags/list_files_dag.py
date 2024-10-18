from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from pipeline.IMDBExtractor import main

PATH = "../../Spark/data/"
FILE_FORMAT = "csv"

args = {
    'owner' : 'airflow'
    # "start_date" : datetime(2024,10,16)
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