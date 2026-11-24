"""
P1-6 — regression (audit M2): "nothing to do" is not an error.

When the source directories contain no file, the insertion task of an ingest DAG
must raise AirflowSkipException (task shown as skipped, no useless retry, and the
Spark task skipped with it) instead of raising ValueError (red run + retry).
No PostgresHook/engine must be built.
"""

from unittest.mock import MagicMock, patch

import pytest
from airflow.sdk.exceptions import AirflowSkipException


def _insert_files_callable():
    from dags.source_pipelines import build_dags

    return build_dags()["ingest__orders"].get_task("insert_files").python_callable


def test_p1_6_insert_files_skips_on_empty_list():
    mock_hook_cls = MagicMock()
    mock_context = {"dag_run": MagicMock(run_id="run_123")}

    with (
        patch("airflow.providers.postgres.hooks.postgres.PostgresHook", mock_hook_cls),
        patch("airflow.sdk.get_current_context", return_value=mock_context) as mock_get_context,
        patch("utils.functions_utils.insert_into_file_list") as mock_insert,
        pytest.raises(AirflowSkipException, match="No files found"),
    ):
        _insert_files_callable()([])

    # The skip must happen before the task context is even read.
    mock_get_context.assert_not_called()
    mock_hook_cls.assert_not_called()
    mock_insert.assert_not_called()


# ---------------------------------------------------------------------------
# P1-4 / C-3 — no file recorded at all: skip, so no Spark task
# ---------------------------------------------------------------------------
#
# Invariant relied upon by the Spark task / read_files_job (exit 3 on zero
# rows): the Spark task only runs when at least one file_list row exists for the run.
# The count is "files handled without error" (inserted, or already present
# and left untouched by ON CONFLICT DO NOTHING on a retry), never the SQL
# rowcount: a retry of a run whose rows already exist must still succeed.


def _run_insert(file_list):
    with (
        patch("airflow.providers.postgres.hooks.postgres.PostgresHook", MagicMock()),
        patch(
            "airflow.sdk.get_current_context",
            return_value={"dag_run": MagicMock(run_id="run_123")},
        ),
        patch("utils.functions_utils.insert_into_file_list") as mock_insert,
    ):
        return _insert_files_callable()(file_list), mock_insert


def test_p1_4_insert_skips_when_every_listed_file_vanished(tmp_path):
    """All listed files were deleted between listing and insertion (one
    FileNotFoundError each): zero row for this run, so the task must skip
    (and the Spark task with it) instead of succeeding."""
    vanished = [str(tmp_path / "gone_1.csv"), str(tmp_path / "gone_2.csv")]

    with pytest.raises(AirflowSkipException, match="0/2"):
        _run_insert(vanished)


def test_p1_4_insert_counts_handled_files_not_new_rows(tmp_path):
    """One file vanished, one still there: success. The surviving file counts even
    when ON CONFLICT DO NOTHING inserts no new row (retry of the same run), since
    insert_into_file_list does not raise."""
    present = tmp_path / "present.csv"
    present.write_text("a,b\n1,2\n")

    job_args, mock_insert = _run_insert([str(tmp_path / "gone.csv"), str(present)])

    assert job_args == ["--run-id=run_123", "--source-name=orders"]
    mock_insert.assert_called_once()
