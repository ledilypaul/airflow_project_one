"""
DAG structure tests.
Validates that DAGs load without errors and have the expected task topology.
Uses Airflow's DagBag — no real database required.
"""

from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.sdk.exceptions import ParamValidationError

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
    from airflow.providers.standard.operators.bash import BashOperator

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


# ---------------------------------------------------------------------------
# P1-3 — idempotence: read_file_dag must scope spark-submit to a run_id
# ---------------------------------------------------------------------------


def test_p1_3_read_file_dag_passes_run_id_param(dagbag):
    dag = dagbag.dags["read_file_dag"]
    task = dag.get_task("spark_read_files")
    assert "--run-id" in task.bash_command
    assert "run_id" in dag.params


def test_p1_1_no_deprecated_airflow_imports():
    """C-1 follow-up: static, warning-mechanism-independent guard for P1-1.

    The `filterwarnings` rule in pyproject.toml is a defence in depth but
    depends on which warning category Airflow happens to raise for a given
    deprecated import (it has changed once already: UserWarning/
    DeprecationWarning do not actually fire for
    `airflow.utils.deprecation_tools.DeprecatedImportWarning`, see C-1). This
    test checks the DAG source text directly, so it still catches a
    regression even if Airflow changes warning categories again.
    """
    forbidden_patterns = (
        "from airflow.decorators import",
        "from airflow.operators.bash import",
        "from airflow.operators.python import",
        "from airflow import DAG",
    )
    dag_files = sorted(Path(DAGS_FOLDER).glob("*.py"))
    assert dag_files, f"no DAG files found under {DAGS_FOLDER}"

    violations = []
    for dag_file in dag_files:
        source = dag_file.read_text(encoding="utf-8")
        for pattern in forbidden_patterns:
            if pattern in source:
                violations.append(f"{dag_file.name}: {pattern!r}")

    assert not violations, "deprecated Airflow 2 import(s) found:\n" + "\n".join(violations)


def test_c1_run_id_param_has_no_default_and_rejects_null(dagbag):
    """C-1 (defence in depth): the `run_id` Param must not accept "null"/None
    implicitly — triggering the DAG without an explicit run_id must be
    flagged by the Airflow UI/validation, not silently resolve to None (which
    the Spark job would otherwise stringify to the literal "None")."""
    dag = dagbag.dags["read_file_dag"]
    run_id_param = dag.params.get_param("run_id")

    assert run_id_param.schema.get("type") != ["null", "string"]
    assert "null" not in (run_id_param.schema.get("type") or [])

    with pytest.raises(ParamValidationError):
        run_id_param.resolve()
