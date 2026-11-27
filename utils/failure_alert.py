"""Structured log line when a task or a DAG run fails (P3-3).

One line per failure, `airflow_failure {<JSON>}`, written at ERROR level in
the log of the process that runs the callback (the worker for a task, the DAG
processor for a DAG run): a log shipper or a `grep airflow_failure` picks it
up. The JSON carries what a person needs to find the failure again: dag_id,
run_id, task, attempt and, for `spark_read_files`, the exit code of the Spark
job and what it means (CLAUDE.md, 'Running the Spark job standalone').

Rules:
- **never raises**: a failure report must not turn into a second failure
  (Airflow also catches a callback's exception, this is the second net);
- the JSON is built with `json.dumps`, never by interpolation: a hostile
  value (a manual run id can be anything) cannot forge a field;
- **the exception's text is not copied, only its type** (`exception_type`):
  the text is already in the task log, where Airflow masks the secrets it
  knows. Copied into a JSON line it would escape that masking, because
  `json.dumps` rewrites `"` and non-ASCII letters, a one-line copy merges the
  spaces and a bound cuts the value: the masker would no longer recognise it;
- **no Airflow import**: the Spark job's process imports `utils`, and must
  never import Airflow (the DAG files import this module inside their
  callback, not at parse time).

The callbacks take Airflow's `context` (a mapping). Only a few attributes are
read, each with a fallback: a task callback gets `ti`, `task` and
`exception`; a DAG callback gets `dag`, `run_id` and `reason` (and the last
task instance's context when the scheduler knows it).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

log = logging.getLogger(__name__)

EVENT = "airflow_failure"

# Exit codes of pipeline/jobs/read_files_job.py (`EXIT_*`). Duplicated: the
# job imports pyspark, which a callback must not load. A test keeps them equal.
JOB_EXIT_CODES: dict[int, str] = {
    1: "at least one file failed (read or write error, quality engine error, or mixed causes)",
    2: "invalid --run-id",
    3: "no file_list row for the run id",
    4: "parent process gone (watchdog), no status written",
    5: "invalid reading config or LAKE_ROOT, every file of the run marked failed",
    6: "blocking quality checks only, see file_list and quality/<table> in the lake",
}


def _get(obj: Any, name: str) -> Any:
    """`obj.name`, or None when `obj` is None or the access raises."""
    try:
        return getattr(obj, name, None)
    except Exception:
        return None


def _context_get(context: Any, key: str) -> Any:
    try:
        return context.get(key) if isinstance(context, Mapping) else None
    except Exception:
        return None


def _put(record: dict[str, Any], key: str, value: Any) -> None:
    if value is not None and value != "":
        record[key] = value


def failure_record(context: Any, *, scope: str) -> dict[str, Any]:
    """The fields of the failure line for `context`, `scope` being `task` or
    `dag_run`. Fields that are not known are left out."""
    ti = _context_get(context, "ti") or _context_get(context, "task_instance")
    task = _context_get(context, "task")
    dag = _context_get(context, "dag")
    record: dict[str, Any] = {"scope": scope}

    _put(record, "dag_id", _get(ti, "dag_id") or _get(dag, "dag_id"))
    _put(record, "run_id", _context_get(context, "run_id") or _get(ti, "run_id"))

    if scope == "dag_run":
        _put(record, "reason", _context_get(context, "reason"))
        _put(record, "last_task_id", _get(ti, "task_id"))
        return record

    _put(record, "task_id", _get(ti, "task_id"))
    map_index = _get(ti, "map_index")
    if isinstance(map_index, int) and not isinstance(map_index, bool) and map_index >= 0:
        record["map_index"] = map_index
    _put(record, "try_number", _get(ti, "try_number"))
    _put(record, "max_tries", _get(ti, "max_tries"))
    if task is not None:
        _put(record, "operator", _get(task, "task_type") or type(task).__name__)

    # Type only, never the text (see the module docstring).
    exception = _context_get(context, "exception")
    if isinstance(exception, BaseException):
        record["exception_type"] = type(exception).__name__

    # Set by ReadFilesSparkSubmitOperator (utils/read_files_operator.py) once
    # spark-submit has run. A bool is an int in Python, never an exit code.
    code = _get(task, "spark_exit_code")
    if isinstance(code, int) and not isinstance(code, bool):
        record["spark_exit_code"] = code
        record["spark_exit_code_meaning"] = JOB_EXIT_CODES.get(code, "unknown")
    return record


def _emit(context: Any, scope: str) -> None:
    try:
        record = failure_record(context, scope=scope)
        log.error("%s %s", EVENT, json.dumps(record, sort_keys=True, default=str))
    except Exception:
        try:
            log.warning("%s: could not report a %s failure (ignored)", EVENT, scope, exc_info=True)
        except Exception:
            pass


def log_task_failure(context: Any) -> None:
    """Task-level `on_failure_callback`: the task ended `failed` (retries
    exhausted, or a failure that is never retried)."""
    _emit(context, "task")


def log_dag_run_failure(context: Any) -> None:
    """DAG-level `on_failure_callback`: the run ended `failed`. Also covers
    what no task callback sees, such as `dagrun_timeout`."""
    _emit(context, "dag_run")
