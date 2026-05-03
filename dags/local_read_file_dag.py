"""
DAG: read_file_dag
Submits a Spark job (via spark-submit) to read and process files listed in the DB.

Airflow orchestre, Spark s'exécute dans son propre processus séparé.
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
JOB_PATH = PROJECT_ROOT / "pipeline" / "jobs" / "read_files_job.py"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="read_file_dag",
    description="Submit a Spark job to read files listed in the database",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["processing", "spark", "file-reading"],
)
def read_file_dag():
    BashOperator(
        task_id="spark_read_files",
        bash_command=f"spark-submit {JOB_PATH}",
        env={"PYTHONPATH": str(PROJECT_ROOT)},
    )


dag_instance = read_file_dag()
