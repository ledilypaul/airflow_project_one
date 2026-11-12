"""
DAG structure tests.
Validates that DAGs load without errors and have the expected task topology.
Uses Airflow's DagBag — no real database required.
"""

from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.sdk.exceptions import AirflowFailException

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


def test_read_file_dag_no_catchup(dagbag):
    assert dagbag.dags["read_file_dag"].catchup is False


# ---------------------------------------------------------------------------
# P0-3 / T1 — regression: DagBag loading under Airflow 3.3
# ---------------------------------------------------------------------------


def test_p0_3_dagbag_loads_without_include_examples_kwarg():
    """T1 cause 1: DagBag no longer accepts `include_examples`; passing it
    raises TypeError. Example DAGs must be excluded via
    AIRFLOW__CORE__LOAD_EXAMPLES=False instead (see conftest.py).
    (Import errors are checked once, by test_no_import_errors.)"""
    bag = DagBag(dag_folder=DAGS_FOLDER)
    assert "example_bash_operator" not in bag.dags


def test_p0_3_dag_has_no_schedule_interval_attribute_access(dagbag):
    """T1 cause 3: DAG.schedule_interval was removed in Airflow 3; use
    DAG.schedule (or DAG.timetable) instead. (The schedule itself is checked
    once, by test_p1_4_read_file_dag_is_scheduled_on_file_list_asset.)"""
    assert not hasattr(dagbag.dags["read_file_dag"], "schedule_interval")


# ---------------------------------------------------------------------------
# P1-3 — idempotence: read_file_dag must scope spark-submit to a run_id
# ---------------------------------------------------------------------------


def test_p1_3_read_file_dag_passes_run_id_param():
    """P1-3: spark-submit is scoped to a run id. (Quoting and the exact
    argv are checked by test_p1_4_spark_command_round_trips_through_job_parser.)"""
    from dags.local_read_file_dag import build_spark_command

    assert "--run-id" in build_spark_command("some_run_id")


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


def test_c1_run_without_run_id_fails_explicitly():
    """C-1 (defence in depth): a run without an explicit scope must never
    silently resolve to None (which the Spark job would otherwise stringify
    to the literal "None").

    P1-4: the `run_id` Param had to become optional (Asset-triggered runs
    carry no conf), so the guard lives in resolve_source_run_ids: no
    triggering asset event and no `run_id` in the conf -> explicit failure.
    The job keeps its own guard (exit code 2)."""
    from dags.local_read_file_dag import dag_instance

    resolve = dag_instance.get_task("resolve_source_run_ids").python_callable
    with pytest.raises(AirflowFailException, match="No source run_id"):
        resolve()
