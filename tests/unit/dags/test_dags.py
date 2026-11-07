"""
DAG structure tests.
Validates that DAGs load without errors and have the expected task topology.
Uses Airflow's DagBag — no real database required.
"""

from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_FOLDER = str(Path(__file__).resolve().parent.parent.parent.parent / "dags")


@pytest.fixture(scope="module")
def dagbag():
    # Airflow 3 removed the `include_examples` kwarg from DagBag; example DAGs
    # are now excluded via AIRFLOW__CORE__LOAD_EXAMPLES=False (set in conftest.py).
    return DagBag(dag_folder=DAGS_FOLDER)


# ---------------------------------------------------------------------------
# Import sanity
# ---------------------------------------------------------------------------


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, "DAG import errors detected:\n" + "\n".join(
        f"  {f}: {e}" for f, e in dagbag.import_errors.items()
    )


# ---------------------------------------------------------------------------
# list_files_dag
# ---------------------------------------------------------------------------


def test_list_files_dag_exists(dagbag):
    assert "list_files_dag" in dagbag.dags


def test_list_files_dag_task_ids(dagbag):
    # list_files_dag is the TaskFlow (@task) version: its task ids are
    # "list_files" / "insert_files", not the classic PythonOperator DAG's
    # "list_files_task" / "insert_files_to_db".
    dag = dagbag.dags["list_files_dag"]
    assert {"list_files", "insert_files"} == set(dag.task_ids)


def test_list_files_dag_dependency(dagbag):
    """insert_files must run after list_files."""
    dag = dagbag.dags["list_files_dag"]
    upstream = {t.task_id for t in dag.get_task("insert_files").upstream_list}
    assert "list_files" in upstream


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
    # Airflow 3 removed DAG.schedule_interval; use .schedule (or .timetable) instead.
    dag = dagbag.dags["read_file_dag"]
    assert dag.schedule is None


# ---------------------------------------------------------------------------
# P0-3 / T1 — regression: DagBag loading under Airflow 3.3
# ---------------------------------------------------------------------------


def test_p0_3_dagbag_loads_without_include_examples_kwarg():
    """T1 cause 1: DagBag no longer accepts `include_examples`; passing it
    raises TypeError. Example DAGs must be excluded via
    AIRFLOW__CORE__LOAD_EXAMPLES=False instead (see conftest.py)."""
    bag = DagBag(dag_folder=DAGS_FOLDER)
    assert bag.import_errors == {}
    assert "example_bash_operator" not in bag.dags


def test_p0_3_list_files_dag_has_taskflow_task_ids(dagbag):
    """T1 cause 2: list_files_dag is TaskFlow-based (list_files/insert_files),
    not the classic DAG's task ids (list_files_task/insert_files_to_db)."""
    dag = dagbag.dags["list_files_dag"]
    assert "list_files_task" not in dag.task_ids
    assert "insert_files_to_db" not in dag.task_ids
    assert {"list_files", "insert_files"} == set(dag.task_ids)


def test_p0_3_dag_has_no_schedule_interval_attribute_access(dagbag):
    """T1 cause 3: DAG.schedule_interval was removed in Airflow 3; use
    DAG.schedule (or DAG.timetable) instead."""
    dag = dagbag.dags["read_file_dag"]
    assert not hasattr(dag, "schedule_interval")
    assert dag.schedule is None
