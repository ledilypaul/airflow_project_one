"""
P2-6: `spark_read_files` does not retry a deterministic failure of the job.

The job's exit codes 2 (invalid run id), 3 (no file_list row), 5 (invalid
config or lake root) and 6 (blocking quality checks only) fail the same way
on every attempt: `ReadFilesSparkSubmitOperator` turns them into an
`AirflowFailException`, which the Task SDK never retries. 1 (some file
failed), 4 (parent gone) and anything else (signal, spark-submit error,
exception before the process started) keep the operator's retries.

How the code is read (apache-airflow-providers-apache-spark 6.3.4, installed):
`SparkSubmitHook.submit()` keeps the `subprocess.Popen` of spark-submit in
`_submit_sp` and raises `AirflowException("Cannot execute: ... Error code
is: N.")` when its return code is non-zero. In local/client mode spark-submit
returns the exit code of the Python driver (checked with PySpark 4.3.0).
The test with the real hook pins that attribute.

The DAG keeps everything P1-5/P2-5 rely on: `.partial().expand()` on
`application_args`, the `spark` pool, `pre_execute=prepare_spark_lineage`,
`on_kill` (inherited). The e2e checks (`dag.test()`, one attempt on exit 6,
two on exit 1, `lake_staging` emitted with another file processed) are in
tests/integration/test_p1_4_read_file_dag_e2e.py.
"""

import json
import os
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk.exceptions import AirflowException, AirflowFailException

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_FOLDER))


@pytest.fixture
def spark_task():
    """From the imported module (DagBag loads the file under another module
    name, hence other class and function objects)."""
    from dags.local_read_file_dag import dag_instance

    return dag_instance.get_task("spark_read_files")


def test_p2_6_read_file_dag_still_loads(dagbag):
    assert not dagbag.import_errors, dagbag.import_errors
    task = dagbag.dags["read_file_dag"].get_task("spark_read_files")
    assert task.operator_class.__name__ == "ReadFilesSparkSubmitOperator"


def test_p2_6_no_retry_exit_codes_match_the_job():
    """The DAG does not import the job (pyspark at parse time): the codes
    are duplicated, and must stay equal."""
    from dags.local_read_file_dag import NO_RETRY_EXIT_CODES
    from pipeline.jobs import read_files_job as job

    assert (
        set(NO_RETRY_EXIT_CODES)
        == {
            job.EXIT_INVALID_RUN_ID,
            job.EXIT_NO_ROWS,
            job.EXIT_INVALID_CONFIG,
            job.EXIT_QUALITY_FAILED,
        }
        == {2, 3, 5, 6}
    )
    assert job.EXIT_FILES_FAILED not in NO_RETRY_EXIT_CODES
    assert job.EXIT_PARENT_GONE not in NO_RETRY_EXIT_CODES


def test_p2_6_spark_task_uses_the_no_retry_operator(spark_task):
    from dags.local_read_file_dag import (
        JOB_PATH,
        SPARK_CONN_ID,
        SPARK_POOL,
        ReadFilesSparkSubmitOperator,
        prepare_spark_lineage,
    )

    assert spark_task.operator_class is ReadFilesSparkSubmitOperator
    assert issubclass(ReadFilesSparkSubmitOperator, SparkSubmitOperator)
    # Mapping, pool, lineage hook and retries untouched.
    assert set(spark_task.expand_input.value) == {"application_args"}
    assert spark_task.pool == SPARK_POOL
    assert spark_task.partial_kwargs["conn_id"] == SPARK_CONN_ID
    assert spark_task.partial_kwargs["application"] == str(JOB_PATH)
    assert spark_task.partial_kwargs["pre_execute"] is prepare_spark_lineage
    assert spark_task.retries == 1
    assert spark_task.retry_delay == timedelta(minutes=5)
    # Same templated fields as the provider's operator.
    assert ReadFilesSparkSubmitOperator.template_fields == SparkSubmitOperator.template_fields
    assert "on_kill" not in vars(ReadFilesSparkSubmitOperator)


# ---------------------------------------------------------------------------
# Fake hook
# ---------------------------------------------------------------------------


class _FakeProcess:
    def __init__(self, returncode):
        self.returncode = returncode

    def poll(self):
        return self.returncode


class _FakeHook:
    """Local/client mode hook whose submit() behaves like the provider's:
    raises AirflowException on a non-zero return code of spark-submit."""

    _should_track_driver_status = False
    _is_yarn_cluster_mode = False
    _is_kubernetes = False

    def __init__(self, returncode=None, error=None):
        self.returncode = returncode
        self.error = error
        self._submit_sp = None
        self.killed = 0

    def _should_track_driver_via_k8s_api(self):
        return False

    def submit(self, application=""):
        if self.error is not None:
            raise self.error
        self._submit_sp = _FakeProcess(self.returncode)
        if self.returncode:
            raise AirflowException(
                f"Cannot execute: spark-submit {application}. Error code is: {self.returncode}."
            )

    def on_kill(self):
        self.killed += 1


def _operator(hook):
    from dags.local_read_file_dag import JOB_PATH, ReadFilesSparkSubmitOperator

    operator = ReadFilesSparkSubmitOperator(
        task_id="spark_read_files",
        application=str(JOB_PATH),
        application_args=["--run-id=manual__p2_6"],
    )
    operator._hook = hook
    return operator


@pytest.mark.parametrize(
    ("code", "meaning"),
    [
        (2, "invalid --run-id"),
        (3, "no file_list row"),
        (5, "invalid reading config or LAKE_ROOT"),
        (6, "blocking quality checks"),
    ],
)
def test_p2_6_deterministic_exit_codes_fail_without_retry(code, meaning):
    with pytest.raises(AirflowFailException) as exc_info:
        _operator(_FakeHook(code)).execute({})

    message = str(exc_info.value)
    assert f"exit code {code}" in message
    assert meaning in message
    assert "not retried" in message
    # The hook's own message (command, log tail) is kept.
    assert f"Error code is: {code}." in message
    assert isinstance(exc_info.value.__cause__, AirflowException)


@pytest.mark.parametrize("code", [1, 4, 7, 137, -9])
def test_p2_6_other_exit_codes_keep_the_retries(code):
    with pytest.raises(AirflowException) as exc_info:
        _operator(_FakeHook(code)).execute({})

    assert not isinstance(exc_info.value, AirflowFailException)
    assert str(exc_info.value).endswith(f"Error code is: {code}.")


@pytest.mark.parametrize(
    "error",
    [AirflowException("SparkSubmitHook env_vars is not supported"), RuntimeError("boom")],
    ids=["airflow_exception", "runtime_error"],
)
def test_p2_6_errors_before_spark_submit_ran_keep_the_retries(error):
    """No process, hence no exit code: the error propagates unchanged."""
    with pytest.raises(type(error)) as exc_info:
        _operator(_FakeHook(error=error)).execute({})
    assert exc_info.value is error


def test_p2_6_success_returns_normally():
    assert _operator(_FakeHook(0)).execute({}) is None


def test_p2_6_on_kill_still_reaches_the_hook():
    """Inherited on_kill: Airflow killing the task still kills spark-submit
    (the job's parent watchdog then stops the Python driver, P1-5)."""
    hook = _FakeHook(None)
    _operator(hook).on_kill()
    assert hook.killed == 1


# ---------------------------------------------------------------------------
# Real hook, fake spark-submit: pins how the installed provider exposes the code
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_spark_submit(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "spark-submit"
    stub.write_text(
        f"#!{sys.executable}\n"
        "import os, sys\n"
        "print('fake spark-submit')\n"
        "sys.exit(int(os.environ['P2_6_EXIT_CODE']))\n"
    )
    stub.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv(
        "AIRFLOW_CONN_SPARK_DEFAULT", json.dumps({"conn_type": "spark", "host": "local[1]"})
    )


def _real_operator():
    from dags.local_read_file_dag import JOB_PATH, SPARK_CONN_ID, ReadFilesSparkSubmitOperator

    return ReadFilesSparkSubmitOperator(
        task_id="spark_read_files",
        application=str(JOB_PATH),
        conn_id=SPARK_CONN_ID,
        application_args=["--run-id=manual__p2_6"],
    )


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX PATH")
@pytest.mark.parametrize(("code", "no_retry"), [(6, True), (3, True), (1, False), (4, False)])
def test_p2_6_real_hook_exit_code_decides_the_retry(fake_spark_submit, monkeypatch, code, no_retry):
    monkeypatch.setenv("P2_6_EXIT_CODE", str(code))

    with pytest.raises(AirflowException) as exc_info:
        _real_operator().execute({})

    assert isinstance(exc_info.value, AirflowFailException) is no_retry
    assert f"Error code is: {code}." in str(exc_info.value)
