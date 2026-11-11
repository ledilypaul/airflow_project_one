"""
DAG: read_file_dag
Submits a Spark job (via spark-submit) to read and process files listed in the DB.

Airflow orchestre, Spark s'exécute dans son propre processus séparé.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.sdk import Param

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

# run_id du run de list_files_dag dont on veut traiter les fichiers. Les deux
# DAGs restent déclenchés indépendamment (le chaînage via Asset est le
# backlog P1-4, hors périmètre ici) : ce paramètre doit être renseigné
# explicitement au déclenchement (Trigger DAG w/ config) pour scoper le job
# Spark à un run précis (idempotence : P1-3).
BASH_COMMAND = "spark-submit " + str(JOB_PATH) + " --run-id {{ params.run_id }}"


@dag(
    dag_id="read_file_dag",
    description="Submit a Spark job to read files listed in the database",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["processing", "spark", "file-reading"],
    params={
        "run_id": Param(
            type="string",
            description="run_id of the list_files_dag run to process. Required: must be "
            "supplied explicitly when triggering this DAG (Trigger DAG w/ config), no "
            "default is allowed (idempotence: P1-3/C-1).",
        )
    },
)
def read_file_dag():
    BashOperator(
        task_id="spark_read_files",
        bash_command=BASH_COMMAND,
        env={"PYTHONPATH": str(PROJECT_ROOT)},
    )


dag_instance = read_file_dag()
