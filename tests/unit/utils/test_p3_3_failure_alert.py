"""
P3-3 - structured log line when a task or a DAG run fails
(`utils/failure_alert.py`).

One line per failure, `airflow_failure {<JSON>}`, that a log shipper or a
`grep` can pick up: dag_id, run_id, task, attempt and, for the Spark job,
the exit code and what it means (CLAUDE.md, 'Running the Spark job
standalone'). The callback never raises: a failure report must not turn into
a second failure.

The module has no Airflow import (the Spark job's process imports `utils`,
and must never import Airflow).
"""

import json
import logging
import subprocess
import sys
from types import SimpleNamespace

import pytest

from utils import failure_alert
from utils.failure_alert import (
    EVENT,
    JOB_EXIT_CODES,
    failure_record,
    log_dag_run_failure,
    log_task_failure,
)


def _task_context(**overrides):
    ti = SimpleNamespace(
        dag_id="read_file_dag",
        task_id="spark_read_files",
        run_id="asset_triggered__2026-11-17",
        map_index=-1,
        try_number=2,
        max_tries=1,
    )
    context = {
        "ti": ti,
        "task": SimpleNamespace(task_type="ReadFilesSparkSubmitOperator"),
        "exception": RuntimeError("boom"),
        "run_id": ti.run_id,
    }
    context.update(overrides)
    return context


def _line(caplog) -> dict:
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith(EVENT + " ")]
    assert len(lines) == 1, [r.getMessage() for r in caplog.records]
    return json.loads(lines[0][len(EVENT) + 1 :])


def test_p3_3_task_failure_logs_one_structured_line(caplog):
    caplog.set_level(logging.INFO)

    log_task_failure(_task_context())

    record = _line(caplog)
    assert record == {
        "scope": "task",
        "dag_id": "read_file_dag",
        "run_id": "asset_triggered__2026-11-17",
        "task_id": "spark_read_files",
        "try_number": 2,
        "max_tries": 1,
        "operator": "ReadFilesSparkSubmitOperator",
        "exception_type": "RuntimeError",
    }
    assert [r.levelno for r in caplog.records if r.getMessage().startswith(EVENT)] == [
        logging.ERROR
    ]


def test_p3_3_mapped_task_reports_its_map_index_and_unmapped_does_not():
    mapped = failure_record(
        _task_context(ti=SimpleNamespace(**{**_task_context()["ti"].__dict__, "map_index": 3})),
        scope="task",
    )
    assert mapped["map_index"] == 3
    assert "map_index" not in failure_record(_task_context(), scope="task")


@pytest.mark.parametrize(("code", "expected"), sorted(JOB_EXIT_CODES.items()))
def test_p3_3_spark_exit_code_is_named(code, expected):
    context = _task_context(task=SimpleNamespace(task_type="X", spark_exit_code=code))

    record = failure_record(context, scope="task")

    assert record["spark_exit_code"] == code
    assert record["spark_exit_code_meaning"] == expected


def test_p3_3_unknown_or_missing_spark_exit_code():
    unknown = failure_record(
        _task_context(task=SimpleNamespace(task_type="X", spark_exit_code=137)), scope="task"
    )
    assert unknown["spark_exit_code"] == 137
    assert unknown["spark_exit_code_meaning"] == "unknown"
    # Not a Spark task, or spark-submit never started: no field at all.
    for task in (
        SimpleNamespace(task_type="X"),
        SimpleNamespace(task_type="X", spark_exit_code=None),
        SimpleNamespace(task_type="X", spark_exit_code=True),  # a bool is not a code
    ):
        record = failure_record(_task_context(task=task), scope="task")
        assert "spark_exit_code" not in record, record
        assert "spark_exit_code_meaning" not in record


def test_p3_3_job_exit_codes_match_the_job_and_the_dag():
    """The DAG and `utils` do not import the job (pyspark): its codes are
    duplicated, and must stay equal. Exit 0 is success, never a failure."""
    from pipeline.jobs import read_files_job as job
    from utils.read_files_operator import NO_RETRY_EXIT_CODES

    assert (
        set(JOB_EXIT_CODES)
        == {
            job.EXIT_FILES_FAILED,
            job.EXIT_INVALID_RUN_ID,
            job.EXIT_NO_ROWS,
            job.EXIT_PARENT_GONE,
            job.EXIT_INVALID_CONFIG,
            job.EXIT_QUALITY_FAILED,
        }
        == {1, 2, 3, 4, 5, 6}
    )
    assert set(NO_RETRY_EXIT_CODES) <= set(JOB_EXIT_CODES)


def test_p3_3_the_exception_text_is_never_copied(caplog):
    """C-2: only the exception's type is reported. The text is already in the
    task log, where Airflow masks the secrets it knows; a copy in a JSON line
    would escape that masking (json.dumps writes `"` as `\\"` and a non-ASCII
    letter as `\\uXXXX`, a one-line copy merges spaces, a bound cuts it): the
    masker no longer recognises the value."""
    # Built at run time, like every fake secret of the tests (gitleaks).
    secret = "s3" + '"' + "cr\u00e9t" + " " + "  value"
    caplog.set_level(logging.INFO)
    context = _task_context(exception=ValueError(f"connection failed: password={secret} ok"))

    record = failure_record(context, scope="task")
    log_task_failure(context)

    assert "exception" not in record
    assert record["exception_type"] == "ValueError"
    output = caplog.text + json.dumps(record) + json.dumps(record, ensure_ascii=False)
    for form in (
        secret,
        json.dumps(secret)[1:-1],  # as json.dumps writes it
        " ".join(secret.split()),  # spaces merged
        "cr\u00e9t",
        "connection failed",
    ):
        assert form not in output, form
    # A string error (the Task SDK may pass the text) leaves no trace either.
    text_only = failure_record(_task_context(exception=f"boom {secret}"), scope="task")
    assert "exception" not in text_only and "exception_type" not in text_only
    assert secret not in json.dumps(text_only)


def test_p3_3_hostile_text_cannot_break_the_json_line(caplog):
    """The fields that come from outside (a manual run id can be anything) go
    through json.dumps: no forged field."""
    caplog.set_level(logging.INFO)
    evil = 'x"}, "dag_id": "forged'
    context = _task_context(run_id=evil)

    log_task_failure(context)

    record = _line(caplog)
    assert record["dag_id"] == "read_file_dag"
    assert record["run_id"] == evil


def test_p3_3_dag_run_failure_reports_the_reason(caplog):
    caplog.set_level(logging.INFO)
    context = {
        "dag": SimpleNamespace(dag_id="read_file_dag"),
        "run_id": "asset_triggered__x",
        "reason": "timed_out",
    }

    log_dag_run_failure(context)

    assert _line(caplog) == {
        "scope": "dag_run",
        "dag_id": "read_file_dag",
        "run_id": "asset_triggered__x",
        "reason": "timed_out",
    }


def test_p3_3_dag_run_failure_names_the_last_task_when_known(caplog):
    caplog.set_level(logging.INFO)
    context = _task_context(
        dag=SimpleNamespace(dag_id="read_file_dag"), reason="task_failure", exception=None
    )

    log_dag_run_failure(context)

    record = _line(caplog)
    assert record["scope"] == "dag_run"
    assert record["last_task_id"] == "spark_read_files"
    assert record["reason"] == "task_failure"


class _Exploding:
    def __getattr__(self, name):
        raise RuntimeError(f"no {name}")


@pytest.mark.parametrize(
    "context",
    [None, {}, {"ti": None, "exception": object()}, {"ti": _Exploding(), "task": _Exploding()}],
    ids=["none", "empty", "none-ti", "exploding"],
)
@pytest.mark.parametrize("callback", [log_task_failure, log_dag_run_failure])
def test_p3_3_callbacks_never_raise(callback, context):
    callback(context)  # must not raise


def test_p3_3_a_failing_logger_does_not_raise(monkeypatch):
    class _BrokenLog:
        def error(self, *args, **kwargs):
            raise OSError("disk full")

        def warning(self, *args, **kwargs):
            raise OSError("disk full")

    monkeypatch.setattr(failure_alert, "log", _BrokenLog())

    log_task_failure(_task_context())  # must not raise


def test_p3_3_module_does_not_import_airflow():
    """The Spark job's process imports `utils`: never Airflow there."""
    code = (
        "import sys, utils.failure_alert; "
        "sys.exit(1 if any(m == 'airflow' or m.startswith('airflow.') for m in sys.modules) else 0)"
    )
    proc = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 0, proc.stderr
