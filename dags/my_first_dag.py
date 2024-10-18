from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime,timedelta

default_args = {
    'owner' : 'corder2j',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id='my_first_dag',
    default_args=default_args,
    description="c'est le premier dag test",
    start_date=datetime(2024,10,18,10),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world premier tache le boss"
    )

    task1