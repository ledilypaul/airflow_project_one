"""
Unit tests for utils/functions_utils.py.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

# Real location of PostgresHook, imported lazily by insert_into_file_list().
POSTGRES_HOOK_PATH = "airflow.providers.postgres.hooks.postgres.PostgresHook"


def _make_engine_mock(fetchall_return=None):
    """Build a mock SQLAlchemy engine with a usable engine.begin() context manager."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    if fetchall_return is not None:
        mock_conn.execute.return_value.mappings.return_value.fetchall.return_value = fetchall_return
    return mock_engine, mock_conn


# ---------------------------------------------------------------------------
# insert_into_file_list
# ---------------------------------------------------------------------------


def _patch_postgres_hook(mock_engine):
    """Patch PostgresHook at its real location so that building it (when no
    explicit `engine` is passed to insert_into_file_list) returns mock_engine
    via .get_sqlalchemy_engine(), without touching real Airflow Connections.

    utils.functions_utils imports PostgresHook lazily, inside the call (the
    Spark job imports that module and must never import Airflow): patching
    `utils.functions_utils.PostgresHook` would not exist anymore."""
    mock_hook_cls = MagicMock()
    mock_hook_cls.return_value.get_sqlalchemy_engine.return_value = mock_engine
    return patch(POSTGRES_HOOK_PATH, mock_hook_cls)


def test_insert_calls_execute():
    mock_engine, mock_conn = _make_engine_mock()

    with _patch_postgres_hook(mock_engine):
        from utils.functions_utils import insert_into_file_list

        insert_into_file_list(["orders.csv", "/data/orders.csv", "2024-01-01"])

    mock_conn.execute.assert_called_once()
    query = str(mock_conn.execute.call_args[0][0])
    assert "INSERT INTO file_list" in query


def test_insert_passes_correct_values():
    mock_engine, mock_conn = _make_engine_mock()

    with _patch_postgres_hook(mock_engine):
        from utils.functions_utils import insert_into_file_list

        insert_into_file_list(["file.csv", "/path/file.csv", "2024-06-15"])

    params = mock_conn.execute.call_args[0][1]
    assert params["file_name"] == "file.csv"
    assert params["file_path"] == "/path/file.csv"
    assert params["status"] == "pending"


def test_insert_sets_dag_run_id():
    mock_engine, mock_conn = _make_engine_mock()

    with _patch_postgres_hook(mock_engine):
        from utils.functions_utils import insert_into_file_list

        insert_into_file_list(["f.csv", "/f.csv", "2024-01-01"], dag_run_id="run_20240101")

    params = mock_conn.execute.call_args[0][1]
    assert params["dag_run_id"] == "run_20240101"


def test_insert_dag_run_id_defaults_to_empty_string():
    mock_engine, mock_conn = _make_engine_mock()

    with _patch_postgres_hook(mock_engine):
        from utils.functions_utils import insert_into_file_list

        insert_into_file_list(["f.csv", "/f.csv", "2024-01-01"])

    params = mock_conn.execute.call_args[0][1]
    assert params["dag_run_id"] == ""


def test_insert_sqlalchemy_error_is_raised():
    mock_engine, mock_conn = _make_engine_mock()
    mock_conn.execute.side_effect = SQLAlchemyError("connection refused")

    with _patch_postgres_hook(mock_engine):
        from utils.functions_utils import insert_into_file_list

        with pytest.raises(SQLAlchemyError):
            insert_into_file_list(["file.csv", "/path/file.csv", "2024-01-01"])


# ---------------------------------------------------------------------------
# P1-2 — insert_into_file_list: PostgresHook, reused engine, ON CONFLICT
# ---------------------------------------------------------------------------


def test_p1_2_insert_uses_postgres_hook_with_correct_conn_id():
    mock_engine, _ = _make_engine_mock()
    mock_hook_cls = MagicMock()
    mock_hook_cls.return_value.get_sqlalchemy_engine.return_value = mock_engine

    with patch(POSTGRES_HOOK_PATH, mock_hook_cls):
        from utils.functions_utils import FILE_LIST_POSTGRES_CONN_ID, insert_into_file_list

        insert_into_file_list(["f.csv", "/f.csv", "2024-01-01"])

    mock_hook_cls.assert_called_once_with(postgres_conn_id=FILE_LIST_POSTGRES_CONN_ID)
    assert FILE_LIST_POSTGRES_CONN_ID == "file_list_db"


def test_p1_2_insert_reuses_passed_engine_without_building_hook():
    mock_engine, mock_conn = _make_engine_mock()
    mock_hook_cls = MagicMock()

    with patch(POSTGRES_HOOK_PATH, mock_hook_cls):
        from utils.functions_utils import insert_into_file_list

        insert_into_file_list(["f.csv", "/f.csv", "2024-01-01"], engine=mock_engine)

    mock_hook_cls.assert_not_called()
    mock_conn.execute.assert_called_once()


def test_p1_2_insert_query_has_on_conflict_do_nothing():
    mock_engine, mock_conn = _make_engine_mock()

    with _patch_postgres_hook(mock_engine):
        from utils.functions_utils import insert_into_file_list

        insert_into_file_list(["f.csv", "/f.csv", "2024-01-01"])

    query = str(mock_conn.execute.call_args[0][0])
    assert "ON CONFLICT" in query
    assert "DO NOTHING" in query
    assert "ON CONFLICT (file_path, dag_run_id)" in query


# ---------------------------------------------------------------------------
# list_file_from_db
# ---------------------------------------------------------------------------


def test_list_file_returns_rows():
    rows = [{"id": 1, "file_name": "a.csv", "file_path": "/data/a.csv"}]
    mock_engine, mock_conn = _make_engine_mock(fetchall_return=rows)

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import list_file_from_db

        result = list_file_from_db("run_123")

    assert result == rows
    query = str(mock_conn.execute.call_args[0][0])
    assert "SELECT" in query
    assert "file_name" in query


def test_list_file_returns_empty_list():
    mock_engine, _ = _make_engine_mock(fetchall_return=[])

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import list_file_from_db

        result = list_file_from_db("run_123")

    assert result == []


def test_list_file_sqlalchemy_error_is_raised():
    mock_engine, mock_conn = _make_engine_mock()
    mock_conn.execute.side_effect = SQLAlchemyError("timeout")

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import list_file_from_db

        with pytest.raises(SQLAlchemyError):
            list_file_from_db("run_123")


# ---------------------------------------------------------------------------
# P1-3 — list_file_from_db filters by dag_run_id, not by date/status
# ---------------------------------------------------------------------------


def test_p1_3_list_file_from_db_filters_by_run_id():
    rows = [{"id": 1, "file_name": "a.csv", "file_path": "/data/a.csv"}]
    mock_engine, mock_conn = _make_engine_mock(fetchall_return=rows)

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import list_file_from_db

        list_file_from_db("run_123")

    query = str(mock_conn.execute.call_args[0][0])
    assert "dag_run_id" in query
    assert "CURRENT_DATE" not in query
    params = mock_conn.execute.call_args[0][1]
    assert params["run_id"] == "run_123"


def test_p1_3_list_file_from_db_includes_processed_and_failed():
    """The query must NOT filter on status: a 'Clear' that reruns the same
    dag_run_id must pick up files already marked processed/failed so they
    get reprocessed (failed files must never be stuck forever)."""
    rows = [{"id": 1, "file_name": "a.csv", "file_path": "/data/a.csv"}]
    mock_engine, mock_conn = _make_engine_mock(fetchall_return=rows)

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import list_file_from_db

        list_file_from_db("run_123")

    query = str(mock_conn.execute.call_args[0][0])
    assert "WHERE status" not in query
    assert "status =" not in query


# ---------------------------------------------------------------------------
# update_file_status
# ---------------------------------------------------------------------------


def test_p1_3_update_file_status_marks_processed():
    mock_engine, mock_conn = _make_engine_mock()

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import update_file_status

        update_file_status(1, "processed")

    query = str(mock_conn.execute.call_args[0][0])
    assert "UPDATE file_list" in query
    params = mock_conn.execute.call_args[0][1]
    assert params["id"] == 1
    assert params["status"] == "processed"
    assert params["error_message"] is None


def test_p1_3_update_file_status_marks_failed_with_message():
    mock_engine, mock_conn = _make_engine_mock()

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import update_file_status

        update_file_status(2, "failed", error_message="boom")

    params = mock_conn.execute.call_args[0][1]
    assert params["id"] == 2
    assert params["status"] == "failed"
    assert params["error_message"] == "boom"


def test_p1_3_update_file_status_never_touches_dag_run_id():
    mock_engine, mock_conn = _make_engine_mock()

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import update_file_status

        update_file_status(1, "processed")

    query = str(mock_conn.execute.call_args[0][0])
    params = mock_conn.execute.call_args[0][1]
    assert "dag_run_id" not in query
    assert "dag_run_id" not in params


def test_p1_3_update_file_status_sqlalchemy_error_is_raised():
    mock_engine, mock_conn = _make_engine_mock()
    mock_conn.execute.side_effect = SQLAlchemyError("connection refused")

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import update_file_status

        with pytest.raises(SQLAlchemyError):
            update_file_status(1, "failed", error_message="boom")


def test_insert_without_engine_and_without_postgres_provider_raises_clear_error():
    """Spark-only environment (`uv sync --no-group airflow`): the provider is
    missing, so the default (engine=None) path must fail with an explicit
    message instead of a bare ImportError."""
    from utils.functions_utils import insert_into_file_list

    # A None entry in sys.modules makes `import` raise ImportError.
    with (
        patch.dict(sys.modules, {"airflow.providers.postgres.hooks.postgres": None}),
        pytest.raises(RuntimeError, match="apache-airflow-providers-postgres"),
    ):
        insert_into_file_list(["f.csv", "/f.csv", "2024-01-01"])
