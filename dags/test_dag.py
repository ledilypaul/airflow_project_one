from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 8, 1),
    # schedule_interval=None,
    catchup=False
) as dag:
    EmptyOperator(task_id="start")
