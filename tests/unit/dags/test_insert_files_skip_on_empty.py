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
