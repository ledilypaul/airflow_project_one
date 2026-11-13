"""
P1-5 — SparkSubmitOperator + Spark pool (audit M6).

read_file_dag's `spark_read_files` no longer goes through a shell
(`BashOperator` + `bash -c`): it is a `SparkSubmitOperator` mapped on
`application_args`, with the Spark master taken from the `spark_default`
Connection and a dedicated `spark` pool limiting concurrent Spark applications.

Covers:
  - operator class and mapped field (the mapping itself is checked by
    test_p1_4_spark_task_is_mapped_over_the_run_ids), `conn_id`, `pool`,
    `application`, no hard-coded Spark resources;
  - C-2: a missing `spark_default` Connection fails `resolve_source_run_ids`
    explicitly instead of letting the hook fall back to `--master yarn`;
  - C-3: `dagrun_timeout` bounds a run blocked by a missing pool, and the
    versioned pools file declares the pool the DAG uses.

The run id reaching the job's parser intact is checked through a real
process by test_p1_4_spark_command_round_trips_through_job_parser; the
end-to-end execution (`dag.test()`, no Jinja rendering of the mapped
`application_args`, Connection master, worker environment, exit code -> task
state) by tests/integration/test_p1_4_read_file_dag_e2e.py.
"""

import fnmatch
import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.models import DagBag
from airflow.sdk.exceptions import AirflowFailException

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / "dags"
POOLS_FILE = PROJECT_ROOT / "deploy" / "airflow" / "pools.json"

# Spark resources that belong to each environment (Connection extras,
# spark-defaults.conf), not to the DAG.
RESOURCE_KWARGS = (
    "executor_memory",
    "driver_memory",
    "executor_cores",
    "num_executors",
    "total_executor_cores",
)
RESOURCE_CONF_PATTERNS = ("spark.*memory*", "spark.*cores*")


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_FOLDER))


@pytest.fixture
def spark_task(dagbag):
    return dagbag.dags["read_file_dag"].get_task("spark_read_files")


def _resolve_callable():
    from dags.local_read_file_dag import dag_instance

    return dag_instance.get_task("resolve_source_run_ids").python_callable


def test_p1_5_spark_task_is_spark_submit_operator_mapped_on_application_args(spark_task):
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

    assert spark_task.operator_class is SparkSubmitOperator
    assert set(spark_task.expand_input.value) == {"application_args"}


def test_p1_5_spark_task_conn_id_pool_and_application(spark_task):
    from dags.local_read_file_dag import JOB_PATH, SPARK_CONN_ID, SPARK_POOL

    assert SPARK_CONN_ID == "spark_default"
    assert SPARK_POOL == "spark"
    assert spark_task.partial_kwargs["conn_id"] == "spark_default"
    assert spark_task.pool == "spark"
    assert spark_task.partial_kwargs["application"] == str(JOB_PATH)
    assert JOB_PATH == PROJECT_ROOT / "pipeline" / "jobs" / "read_files_job.py"
    assert JOB_PATH.is_file()


def _hardcoded_resources(partial_kwargs: dict) -> list[str]:
    found = [k for k in RESOURCE_KWARGS if partial_kwargs.get(k) is not None]
    conf = partial_kwargs.get("conf") or {}
    found += [
        key
        for key in conf
        if any(fnmatch.fnmatchcase(key, pattern) for pattern in RESOURCE_CONF_PATTERNS)
    ]
    return found


def test_p1_5_hardcoded_resources_detector_sees_kwargs_and_conf_keys():
    """Guards the guard below: the detector must flag both the operator
    kwargs and the equivalent `conf` keys (spark.executor.memory...)."""
    assert _hardcoded_resources({"conf": {"spark.sql.shuffle.partitions": "8"}}) == []
    assert _hardcoded_resources(
        {
            "driver_memory": "2g",
            "conf": {
                "spark.executor.memory": "4g",
                "spark.driver.memoryOverhead": "1g",
                "spark.executor.cores": "2",
                "spark.cores.max": "8",
                "spark.app.name": "x",
            },
        }
    ) == [
        "driver_memory",
        "spark.executor.memory",
        "spark.driver.memoryOverhead",
        "spark.executor.cores",
        "spark.cores.max",
    ]


def test_p1_5_spark_task_sets_no_hardcoded_resources(spark_task):
    """M6: resources belong to the Connection / spark-defaults of each
    environment, not to the DAG: neither as operator kwargs
    (`executor_memory` & co) nor as `conf` keys (`spark.executor.memory`...)."""
    assert _hardcoded_resources(spark_task.partial_kwargs) == []


# ---------------------------------------------------------------------------
# C-2 — missing Spark Connection
# ---------------------------------------------------------------------------


def test_p1_5_resolve_fails_explicitly_without_spark_connection(monkeypatch):
    """Without the Connection, SparkSubmitHook silently runs
    `spark-submit --master yarn`. The run must fail before that, with a
    message pointing at the setup instructions."""
    monkeypatch.delenv("AIRFLOW_CONN_SPARK_DEFAULT", raising=False)

    with pytest.raises(AirflowFailException, match=r"spark_default.*CLAUDE\.md"):
        _resolve_callable()(
            triggering_asset_events=None, dag_run=SimpleNamespace(conf={"run_id": "manual__x"})
        )


def test_p1_5_resolve_with_spark_connection_logs_master_and_resolves(monkeypatch, caplog):
    monkeypatch.setenv(
        "AIRFLOW_CONN_SPARK_DEFAULT", json.dumps({"conn_type": "spark", "host": "local[3]"})
    )

    with caplog.at_level("INFO", logger="dags.local_read_file_dag"):
        result = _resolve_callable()(
            triggering_asset_events=None, dag_run=SimpleNamespace(conf={"run_id": "manual__x"})
        )

    assert result == ["manual__x"]
    assert "local[3]" in caplog.text


# ---------------------------------------------------------------------------
# C-3 — missing pool: bounded run duration, versioned pools file
# ---------------------------------------------------------------------------


def test_p1_5_read_file_dag_has_dagrun_timeout(dagbag):
    """A missing pool (or a stuck spark-submit) must not freeze the DAG
    forever: with max_active_runs=1 every later `file_list` event would pile
    up behind the stuck run."""
    assert dagbag.dags["read_file_dag"].dagrun_timeout == timedelta(hours=6)


def test_p1_5_pools_file_declares_the_spark_pool():
    """deploy/airflow/pools.json is what `airflow pools import` loads: a JSON
    object `{name: {"slots", "description", ["include_deferred"]}}`. An entry
    without `slots` or `description` makes the whole import fail."""
    from dags.local_read_file_dag import SPARK_POOL

    pools = json.loads(POOLS_FILE.read_text(encoding="utf-8"))

    assert isinstance(pools, dict)
    for name, pool in pools.items():
        assert {"slots", "description"} <= set(pool), name
        assert isinstance(pool["slots"], int), name
        assert isinstance(pool["description"], str), name
        assert isinstance(pool.get("include_deferred", False), bool), name
    assert pools[SPARK_POOL]["slots"] == 1
