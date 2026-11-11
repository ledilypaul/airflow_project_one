from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 8, 1),
    # schedule_interval=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="start")
