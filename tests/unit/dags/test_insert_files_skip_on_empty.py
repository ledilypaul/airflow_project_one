"""
P1-6 — regression (audit M2): "nothing to do" is not an error.

When the source directories contain no file, the insertion task of each
list_files_dag variant must raise AirflowSkipException (task shown as
skipped, no useless retry) instead of:
  - raising ValueError (TaskFlow version: red run + retry), or
  - silently returning 0 (classic version: green run, invisible no-op).

In both cases no PostgresHook/engine must be built.
"""

from unittest.mock import MagicMock, patch

import pytest
from airflow.sdk.exceptions import AirflowSkipException

# ---------------------------------------------------------------------------
# TaskFlow version (dags/local_list_files_dag.py)
# ---------------------------------------------------------------------------


def test_p1_6_taskflow_insert_files_skips_on_empty_list():
    from dags.local_list_files_dag import dag_instance

    insert_files = dag_instance.get_task("insert_files").python_callable

    mock_hook_cls = MagicMock()
    mock_context = {"dag_run": MagicMock(run_id="run_123")}

    with (
        patch(
            "airflow.providers.postgres.hooks.postgres.PostgresHook",
            mock_hook_cls,
        ),
        patch("airflow.sdk.get_current_context", return_value=mock_context) as mock_get_context,
        patch("utils.functions_utils.insert_into_file_list") as mock_insert,
        pytest.raises(AirflowSkipException, match="No files found"),
    ):
        insert_files([])

    # The skip must happen before the task context is even read.
    mock_get_context.assert_not_called()
    mock_hook_cls.assert_not_called()
    mock_insert.assert_not_called()


# ---------------------------------------------------------------------------
# PythonOperator ("classic") version (dags/local_list_files_dag_python_operator.py)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("xcom_value", [[], None], ids=["empty_list", "none"])
def test_p1_6_classic_insert_files_to_db_skips_on_empty_list(xcom_value):
    from dags.local_list_files_dag_python_operator import insert_files_to_db

    mock_hook_cls = MagicMock()
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = xcom_value

    with (
        patch(
            "airflow.providers.postgres.hooks.postgres.PostgresHook",
            mock_hook_cls,
        ),
        patch("utils.functions_utils.insert_into_file_list") as mock_insert,
        pytest.raises(AirflowSkipException, match="No files found"),
    ):
        insert_files_to_db(ti=mock_ti, run_id="run_456")

    mock_ti.xcom_pull.assert_called_once_with(task_ids="list_files_task")
    mock_hook_cls.assert_not_called()
    mock_insert.assert_not_called()


# ---------------------------------------------------------------------------
# P1-4 / C-3 — no file recorded at all: skip, so no `file_list` asset event
# ---------------------------------------------------------------------------
#
# Invariant relied upon by read_file_dag / read_files_job (exit 3 on zero
# rows): an asset event implies at least one file_list row for its run id.
# The count is "files handled without error" (inserted, or already present
# and left untouched by ON CONFLICT DO NOTHING on a retry), never the SQL
# rowcount: a retry of a run whose rows already exist must still succeed.


def _run_taskflow_insert(file_list):
    from dags.local_list_files_dag import dag_instance

    insert_files = dag_instance.get_task("insert_files").python_callable
    with (
        patch("airflow.providers.postgres.hooks.postgres.PostgresHook", MagicMock()),
        patch(
            "airflow.sdk.get_current_context",
            return_value={"dag_run": MagicMock(run_id="run_123")},
        ),
        patch("utils.functions_utils.insert_into_file_list") as mock_insert,
    ):
        return insert_files(file_list), mock_insert


def _run_classic_insert(file_list):
    from dags.local_list_files_dag_python_operator import insert_files_to_db

    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = file_list
    with (
        patch("airflow.providers.postgres.hooks.postgres.PostgresHook", MagicMock()),
        patch("utils.functions_utils.insert_into_file_list") as mock_insert,
    ):
        return insert_files_to_db(ti=mock_ti, run_id="run_456"), mock_insert


RUNNERS = pytest.mark.parametrize(
    "run_insert", [_run_taskflow_insert, _run_classic_insert], ids=["taskflow", "classic"]
)


@RUNNERS
def test_p1_4_insert_skips_when_every_listed_file_vanished(tmp_path, run_insert):
    """All listed files were deleted between listing and insertion (one
    FileNotFoundError each): zero row for this run, so the task must skip
    (no asset event) instead of succeeding and triggering read_file_dag."""
    vanished = [str(tmp_path / "gone_1.csv"), str(tmp_path / "gone_2.csv")]

    with pytest.raises(AirflowSkipException, match="0/2"):
        run_insert(vanished)


@RUNNERS
def test_p1_4_insert_counts_handled_files_not_new_rows(tmp_path, run_insert):
    """One file vanished, one still there: success with count 1. The
    surviving file counts even when ON CONFLICT DO NOTHING inserts no new row
    (retry of the same run), since insert_into_file_list does not raise."""
    present = tmp_path / "present.csv"
    present.write_text("a,b\n1,2\n")

    handled, mock_insert = run_insert([str(tmp_path / "gone.csv"), str(present)])

    assert handled == 1
    mock_insert.assert_called_once()
