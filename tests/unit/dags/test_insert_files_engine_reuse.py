"""
P1-2 — regression: the insertion task of each list_files_dag variant must
build exactly one PostgresHook/engine per task run and pass it to every
insert_into_file_list() call, instead of rebuilding one per file (M3).

Both DAG files are imported directly (they live in the `dags` package, see
`dags/__init__.py`) to call their task callables in isolation, consistent
with how `tests/unit/dags/test_dags.py` already exercises DAG internals via
DagBag/get_task().
"""

from unittest.mock import MagicMock, patch


def _make_files(tmp_path, count=3):
    paths = []
    for i in range(count):
        p = tmp_path / f"file_{i}.csv"
        p.write_text("a,b\n1,2\n")
        paths.append(str(p))
    return paths


# ---------------------------------------------------------------------------
# TaskFlow version (dags/local_list_files_dag.py)
# ---------------------------------------------------------------------------


def test_p1_2_taskflow_insert_files_builds_hook_once_for_multiple_files(tmp_path):
    from dags.local_list_files_dag import dag_instance

    insert_files = dag_instance.get_task("insert_files").python_callable

    file_list = _make_files(tmp_path, count=3)
    mock_engine = MagicMock()
    mock_hook_cls = MagicMock()
    mock_hook_cls.return_value.get_sqlalchemy_engine.return_value = mock_engine

    mock_context = {"dag_run": MagicMock(run_id="run_123")}

    with (
        patch(
            "airflow.providers.postgres.hooks.postgres.PostgresHook",
            mock_hook_cls,
        ),
        patch("airflow.sdk.get_current_context", return_value=mock_context),
        patch("utils.functions_utils.insert_into_file_list") as mock_insert,
    ):
        inserted = insert_files(file_list)

    assert inserted == 3
    mock_hook_cls.assert_called_once_with(postgres_conn_id="file_list_db")
    mock_hook_cls.return_value.get_sqlalchemy_engine.assert_called_once()
    assert mock_insert.call_count == 3
    for call in mock_insert.call_args_list:
        assert call.kwargs["engine"] is mock_engine


# ---------------------------------------------------------------------------
# PythonOperator ("classic") version (dags/local_list_files_dag_python_operator.py)
# ---------------------------------------------------------------------------


def test_p1_2_classic_insert_files_to_db_builds_hook_once_for_multiple_files(tmp_path):
    from dags.local_list_files_dag_python_operator import insert_files_to_db

    file_list = _make_files(tmp_path, count=3)
    mock_engine = MagicMock()
    mock_hook_cls = MagicMock()
    mock_hook_cls.return_value.get_sqlalchemy_engine.return_value = mock_engine

    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = file_list

    with (
        patch(
            "airflow.providers.postgres.hooks.postgres.PostgresHook",
            mock_hook_cls,
        ),
        patch("utils.functions_utils.insert_into_file_list") as mock_insert,
    ):
        inserted = insert_files_to_db(ti=mock_ti, run_id="run_456")

    assert inserted == 3
    mock_hook_cls.assert_called_once_with(postgres_conn_id="file_list_db")
    mock_hook_cls.return_value.get_sqlalchemy_engine.assert_called_once()
    assert mock_insert.call_count == 3
    for call in mock_insert.call_args_list:
        assert call.kwargs["engine"] is mock_engine


def test_p1_2_classic_insert_files_to_db_passes_real_dag_run_id(tmp_path):
    """C-1 (audit code-auditor) : insert_files_to_db ne transmettait jamais
    dag_run_id à insert_into_file_list, si bien que toutes les lignes
    insérées par le DAG classique partageaient dag_run_id="" (défaut).
    Combiné à l'index UNIQUE(file_path, dag_run_id) + ON CONFLICT DO
    NOTHING (P1-2), un fichier déjà vu par un run classique antérieur
    n'était plus jamais réinséré par un run classique ultérieur.
    """
    from dags.local_list_files_dag_python_operator import insert_files_to_db

    file_list = _make_files(tmp_path, count=3)
    mock_engine = MagicMock()
    mock_hook_cls = MagicMock()
    mock_hook_cls.return_value.get_sqlalchemy_engine.return_value = mock_engine

    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = file_list

    real_run_id = "scheduled__2026-11-11T00:00:00+00:00"

    with (
        patch(
            "airflow.providers.postgres.hooks.postgres.PostgresHook",
            mock_hook_cls,
        ),
        patch("utils.functions_utils.insert_into_file_list") as mock_insert,
    ):
        inserted = insert_files_to_db(ti=mock_ti, run_id=real_run_id)

    assert inserted == 3
    assert mock_insert.call_count == 3
    for call in mock_insert.call_args_list:
        assert call.kwargs["dag_run_id"] == real_run_id
        assert call.kwargs["dag_run_id"] not in (None, "")
