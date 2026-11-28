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
# ingest__<source> (P5-3): the examples of examples/config, see conftest.py
# ---------------------------------------------------------------------------

EXAMPLE_INGEST_DAGS = ["ingest__imdb_movies", "ingest__orders"]


def test_one_ingest_dag_per_source_plus_unmatched(dagbag):
    assert {d for d in dagbag.dags if d.startswith("ingest__")} == {
        *EXAMPLE_INGEST_DAGS,
        "ingest__unmatched",
    }


@pytest.mark.parametrize("dag_id", [*EXAMPLE_INGEST_DAGS, "ingest__unmatched"])
def test_ingest_dag_task_topology(dagbag, dag_id):
    dag = dagbag.dags[dag_id]
    expected = {"list_files", "insert_files", "spark_read_files", "fail_if_spark_failed"}
    if dag_id != "ingest__unmatched":  # raw zone only: no staging, no Asset
        expected.add("publish_lake_staging")
    assert set(dag.task_ids) == expected
    assert dag.get_task("insert_files").upstream_task_ids == {"list_files"}
    assert dag.get_task("spark_read_files").upstream_task_ids == {"insert_files"}


@pytest.mark.parametrize("dag_id", [*EXAMPLE_INGEST_DAGS, "ingest__unmatched"])
def test_ingest_dag_no_catchup_one_active_run(dagbag, dag_id):
    assert dagbag.dags[dag_id].catchup is False
    assert dagbag.dags[dag_id].max_active_runs == 1


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
    assert not hasattr(dagbag.dags["ingest__orders"], "schedule_interval")


# ---------------------------------------------------------------------------
# P1-3 — idempotence: read_file_dag must scope spark-submit to a run_id
# ---------------------------------------------------------------------------


def test_p1_3_ingest_dag_passes_run_id_and_source():
    """P1-3: spark-submit is scoped to a run id (P5-3: and to its source). (The exact
    argv is checked by test_p1_4_spark_command_round_trips_through_job_parser; P1-5:
    these are `application_args` entries of SparkSubmitOperator.)"""
    from utils.read_files_operator import build_application_args

    assert build_application_args("some_run_id", "orders") == [
        "--run-id=some_run_id",
        "--source-name=orders",
    ]


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
