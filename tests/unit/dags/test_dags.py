"""
DAG structure tests.
Validates that DAGs load without errors and have the expected task topology.
Uses Airflow's DagBag — no real database required.
"""
import pytest
from pathlib import Path
from airflow.models import DagBag

DAGS_FOLDER = str(Path(__file__).resolve().parent.parent.parent.parent / "dags")


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


# ---------------------------------------------------------------------------
# Import sanity
# ---------------------------------------------------------------------------

def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, (
        f"DAG import errors detected:\n"
        + "\n".join(f"  {f}: {e}" for f, e in dagbag.import_errors.items())
    )


# ---------------------------------------------------------------------------
# list_files_dag
# ---------------------------------------------------------------------------

def test_list_files_dag_exists(dagbag):
    assert "list_files_dag" in dagbag.dags


def test_list_files_dag_task_ids(dagbag):
    dag = dagbag.dags["list_files_dag"]
    assert {"list_files_task", "insert_files_to_db"} == set(dag.task_ids)


def test_list_files_dag_dependency(dagbag):
    """insert_files_to_db must run after list_files_task."""
    dag = dagbag.dags["list_files_dag"]
    upstream = {t.task_id for t in dag.get_task("insert_files_to_db").upstream_list}
    assert "list_files_task" in upstream


def test_list_files_dag_no_catchup(dagbag):
    assert dagbag.dags["list_files_dag"].catchup is False


# ---------------------------------------------------------------------------
# read_file_dag
# ---------------------------------------------------------------------------

def test_read_file_dag_exists(dagbag):
    assert "read_file_dag" in dagbag.dags


def test_read_file_dag_task_ids(dagbag):
    dag = dagbag.dags["read_file_dag"]
    assert "spark_read_files" in dag.task_ids


def test_read_file_dag_uses_bash_operator(dagbag):
    """The Spark task must be a BashOperator (not a PythonOperator)."""
    from airflow.operators.bash import BashOperator
    dag = dagbag.dags["read_file_dag"]
    task = dag.get_task("spark_read_files")
    assert isinstance(task, BashOperator)


def test_read_file_dag_bash_command_uses_spark_submit(dagbag):
    dag = dagbag.dags["read_file_dag"]
    task = dag.get_task("spark_read_files")
    assert "spark-submit" in task.bash_command
    assert "read_files_job.py" in task.bash_command


def test_read_file_dag_no_catchup(dagbag):
    assert dagbag.dags["read_file_dag"].catchup is False


def test_read_file_dag_schedule_is_none(dagbag):
    dag = dagbag.dags["read_file_dag"]
    assert dag.schedule_interval is None
