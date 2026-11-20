"""
P3-3 - failure callbacks of the DAGs.

Every task and every DAG run of the four DAGs reports its failure with
`utils.failure_alert` (one structured `airflow_failure` line). The DAG files
import it inside the callback, never at parse time (the DAG processor does
not have the project root on `sys.path`, see test_p1_4_assets.py).

`spark_read_files` also records the exit code of spark-submit on its
operator, so the line says which of the job's codes (1 to 6) ended the task.
"""

import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.models import DagBag
from airflow.sdk.exceptions import AirflowException, AirflowFailException

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / "dags"
DAG_IDS = ["list_files_dag", "list_files_dag_classic", "read_file_dag", "transform_dag"]
# Sentinel of read_file_dag: it fails on purpose when a Spark instance failed,
# the failing instance has already reported itself.
SENTINEL = ("read_file_dag", "fail_if_spark_failed")


@pytest.fixture(scope="module")
def dagbag():
    bag = DagBag(dag_folder=str(DAGS_FOLDER))
    assert not bag.import_errors, bag.import_errors
    return bag


def _callbacks(value) -> list:
    if value is None:
        return []
    return list(value) if isinstance(value, list | tuple) else [value]


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_p3_3_every_task_and_the_dag_report_their_failure(dagbag, dag_id):
    dag = dagbag.dags[dag_id]

    assert _callbacks(dag.on_failure_callback), f"{dag_id}: no DAG-level on_failure_callback"
    missing = [
        t.task_id
        for t in dag.tasks
        if not _callbacks(t.on_failure_callback) and (dag_id, t.task_id) != SENTINEL
    ]
    assert not missing, f"{dag_id}: tasks without on_failure_callback: {missing}"


def test_p3_3_the_sentinel_task_does_not_report_a_second_time(dagbag):
    task = dagbag.dags["read_file_dag"].get_task("fail_if_spark_failed")
    assert not _callbacks(task.on_failure_callback)


def test_p3_3_failure_callbacks_do_not_change_the_retry_policy(dagbag):
    """Callbacks run on the final failure only: no retry is added or removed."""
    read = dagbag.dags["read_file_dag"]
    assert read.get_task("spark_read_files").retries == 1
    assert read.get_task("fail_if_spark_failed").retries == 0
    assert dagbag.dags["list_files_dag"].get_task("insert_files").retries == 1


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_p3_3_callbacks_log_through_utils_failure_alert(dagbag, dag_id, caplog):
    """Calling the DAG's own callbacks with a context ends in the shared
    line: the file's wrapper is wired to the right module and scope."""
    dag = dagbag.dags[dag_id]
    task = next(t for t in dag.tasks if (dag_id, t.task_id) != SENTINEL)
    context = {
        "ti": SimpleNamespace(
            dag_id=dag_id, task_id=task.task_id, run_id="r1", map_index=-1, try_number=1
        ),
        "task": task,
        "dag": dag,
        "run_id": "r1",
        "reason": "task_failure",
        "exception": RuntimeError("boom"),
    }
    caplog.set_level(logging.INFO)

    for callback in _callbacks(task.on_failure_callback):
        callback(context)
    for callback in _callbacks(dag.on_failure_callback):
        callback(context)

    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("airflow_failure")]
    records = [json.loads(line.split(" ", 1)[1]) for line in lines]
    assert [r["scope"] for r in records] == ["task", "dag_run"], lines
    assert {r["dag_id"] for r in records} == {dag_id}
    assert records[0]["task_id"] == task.task_id
    assert records[1]["reason"] == "task_failure"


# ---------------------------------------------------------------------------
# spark_read_files records the exit code of spark-submit
# ---------------------------------------------------------------------------


class _FakeProcess:
    def __init__(self, returncode):
        self.returncode = returncode


class _FakeHook:
    """Same shape as tests/unit/dags/test_p2_6_no_retry_exit_codes.py."""

    _should_track_driver_status = False
    _is_yarn_cluster_mode = False
    _is_kubernetes = False

    def __init__(self, returncode=None, error=None):
        self.returncode = returncode
        self.error = error
        self._submit_sp = None

    def _should_track_driver_via_k8s_api(self):
        return False

    def submit(self, application=""):
        if self.error is not None:
            raise self.error
        self._submit_sp = _FakeProcess(self.returncode)
        if self.returncode:
            raise AirflowException(
                f"Cannot execute: {application}. Error code is: {self.returncode}."
            )


def _operator(hook):
    from dags.local_read_file_dag import ReadFilesSparkSubmitOperator

    operator = ReadFilesSparkSubmitOperator(
        task_id="spark_read_files",
        application="job.py",
        conn_id="spark_default",
        application_args=["--run-id=r"],
    )
    operator._hook = hook
    return operator


@pytest.mark.parametrize("code", [1, 2, 3, 4, 5, 6])
def test_p3_3_spark_operator_records_the_exit_code(code):
    from dags.local_read_file_dag import NO_RETRY_EXIT_CODES

    operator = _operator(_FakeHook(returncode=code))
    expected = AirflowFailException if code in NO_RETRY_EXIT_CODES else AirflowException

    with pytest.raises(expected) as raised:
        operator.execute({})

    # Retry behaviour untouched (P2-6): deterministic codes fail for good.
    assert isinstance(raised.value, AirflowFailException) == (code in NO_RETRY_EXIT_CODES)
    assert operator.spark_exit_code == code


def test_p3_3_spark_operator_has_no_exit_code_before_spark_started():
    operator = _operator(_FakeHook(error=AirflowException("no spark-submit binary")))

    with pytest.raises(AirflowException):
        operator.execute({})

    assert operator.spark_exit_code is None


def test_p3_3_spark_exit_code_reaches_the_failure_line(caplog):
    """The task callback reads `spark_exit_code` on the operator of the
    context: the line names the code and its meaning."""
    from utils.failure_alert import log_task_failure

    operator = _operator(_FakeHook(returncode=6))
    with pytest.raises(AirflowFailException) as raised:
        operator.execute({})
    caplog.set_level(logging.INFO)

    log_task_failure({"task": operator, "exception": raised.value})

    (line,) = [
        r.getMessage() for r in caplog.records if r.getMessage().startswith("airflow_failure")
    ]
    record = json.loads(line.split(" ", 1)[1])
    assert record["spark_exit_code"] == 6
    assert "quality" in record["spark_exit_code_meaning"]


# ---------------------------------------------------------------------------
# C-6 - without the project on sys.path, the callbacks still report
# ---------------------------------------------------------------------------


@pytest.fixture
def dagbag_without_project(monkeypatch):
    """The DAG files as a DAG processor / a bare worker image sees them: the
    project root is not on sys.path and no project package is imported (same
    setup as test_p1_4_dags_parse_without_project_root_on_sys_path)."""
    import importlib
    import sys

    root = PROJECT_ROOT.resolve()
    kept = [p for p in sys.path if Path(p or os.getcwd()).resolve() != root]
    monkeypatch.setattr(sys, "path", [str(DAGS_FOLDER), *kept])
    for name in list(sys.modules):
        if name.split(".")[0] in {"dags", "pipeline", "utils", "storage"}:
            monkeypatch.delitem(sys.modules, name)
    importlib.invalidate_caches()
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("utils")
    bag = DagBag(dag_folder=str(DAGS_FOLDER))
    assert not bag.import_errors, bag.import_errors
    return bag


def _fallback_records(caplog) -> list[dict]:
    records = []
    for record in caplog.records:
        message = record.getMessage()
        if message.startswith("airflow_failure "):
            records.append(json.loads(message.split(" ", 1)[1]))
    return records


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_p3_3_c6_callbacks_report_a_minimal_line_when_utils_cannot_be_imported(
    dagbag_without_project, dag_id, caplog
):
    dag = dagbag_without_project.dags[dag_id]
    task = next(t for t in dag.tasks if (dag_id, t.task_id) != SENTINEL)
    context = {
        "ti": SimpleNamespace(dag_id=dag_id, task_id=task.task_id, run_id="r1", map_index=-1),
        "task": task,
        "dag": dag,
        "run_id": "r1",
        "reason": "task_failure",
        "exception": RuntimeError("boom"),
    }
    caplog.set_level(logging.INFO)

    for callback in _callbacks(task.on_failure_callback):
        callback(context)  # must not raise
    for callback in _callbacks(dag.on_failure_callback):
        callback(context)  # must not raise

    records = _fallback_records(caplog)
    assert [r["scope"] for r in records] == ["task", "dag_run"], caplog.text
    for record in records:
        assert record["dag_id"] == dag_id
        assert record["run_id"] == "r1"
    assert records[0]["task_id"] == task.task_id
    assert records[1]["reason"] == "task_failure"
    # The exception text is not copied here either (C-2).
    assert "boom" not in caplog.text


@pytest.mark.parametrize("dag_id", DAG_IDS)
@pytest.mark.parametrize("context", [None, {}, {"ti": None}, "not a mapping"], ids=repr)
def test_p3_3_c6_the_fallback_never_raises_on_a_bad_context(
    dagbag_without_project, dag_id, context
):
    dag = dagbag_without_project.dags[dag_id]
    task = next(t for t in dag.tasks if (dag_id, t.task_id) != SENTINEL)

    ours = {"on_task_failure", "on_dag_run_failure"}
    callbacks = [*_callbacks(task.on_failure_callback), *_callbacks(dag.on_failure_callback)]
    # Cosmos adds callbacks of its own on some tasks (XCom backup): not ours.
    callbacks = [c for c in callbacks if getattr(c, "__name__", "") in ours]
    assert len(callbacks) == 2
    for callback in callbacks:
        callback(context)  # must not raise


# ---------------------------------------------------------------------------
# C-7 - through the DAG processor's own entry point
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_p3_3_c7_the_dag_processor_runs_the_dag_level_callback(dagbag, dag_id, caplog):
    """`dagrun_timeout` (the reason `msg`) ends a run `failed` without any
    failing task: the scheduler sends a DagCallbackRequest, and the DAG
    processor runs `dag.on_failure_callback` through `_execute_dag_callbacks`
    (the code path of a real deployment, not a direct call)."""
    import structlog
    from airflow.callbacks.callback_requests import DagCallbackRequest
    from airflow.dag_processing.processor import _execute_dag_callbacks

    request = DagCallbackRequest(
        filepath=dagbag.dags[dag_id].fileloc,
        bundle_name="dags-folder",
        bundle_version=None,
        msg="timed_out",
        dag_id=dag_id,
        run_id="scheduled__2026-11-17",
        is_failure_callback=True,
    )
    caplog.set_level(logging.INFO)

    _execute_dag_callbacks(dagbag, request, structlog.get_logger())

    records = _fallback_records(caplog)  # same parsing as the C-6 test
    assert records == [
        {
            "scope": "dag_run",
            "dag_id": dag_id,
            "run_id": "scheduled__2026-11-17",
            "reason": "timed_out",
        }
    ]
