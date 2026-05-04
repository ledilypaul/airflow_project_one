"""
Unit tests for utils/functions_utils.py.
"""
import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError


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

def test_insert_calls_execute():
    mock_engine, mock_conn = _make_engine_mock()

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import insert_into_file_list
        insert_into_file_list(["orders.csv", "/data/orders.csv", "2024-01-01"])

    mock_conn.execute.assert_called_once()
    query = str(mock_conn.execute.call_args[0][0])
    assert "INSERT INTO file_list" in query


def test_insert_passes_correct_values():
    mock_engine, mock_conn = _make_engine_mock()

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import insert_into_file_list
        insert_into_file_list(["file.csv", "/path/file.csv", "2024-06-15"])

    params = mock_conn.execute.call_args[0][1]
    assert params["file_name"] == "file.csv"
    assert params["file_path"] == "/path/file.csv"
    assert params["status"] == "pending"


def test_insert_sets_dag_run_id():
    mock_engine, mock_conn = _make_engine_mock()

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import insert_into_file_list
        insert_into_file_list(["f.csv", "/f.csv", "2024-01-01"], dag_run_id="run_20240101")

    params = mock_conn.execute.call_args[0][1]
    assert params["dag_run_id"] == "run_20240101"


def test_insert_dag_run_id_defaults_to_empty_string():
    mock_engine, mock_conn = _make_engine_mock()

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import insert_into_file_list
        insert_into_file_list(["f.csv", "/f.csv", "2024-01-01"])

    params = mock_conn.execute.call_args[0][1]
    assert params["dag_run_id"] == ""


def test_insert_sqlalchemy_error_is_raised():
    mock_engine, mock_conn = _make_engine_mock()
    mock_conn.execute.side_effect = SQLAlchemyError("connection refused")

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import insert_into_file_list
        with pytest.raises(SQLAlchemyError):
            insert_into_file_list(["file.csv", "/path/file.csv", "2024-01-01"])


# ---------------------------------------------------------------------------
# list_file_from_db
# ---------------------------------------------------------------------------

def test_list_file_returns_rows():
    rows = [{"id": 1, "file_name": "a.csv", "file_path": "/data/a.csv"}]
    mock_engine, mock_conn = _make_engine_mock(fetchall_return=rows)

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import list_file_from_db
        result = list_file_from_db()

    assert result == rows
    query = str(mock_conn.execute.call_args[0][0])
    assert "SELECT" in query
    assert "file_name" in query
    assert "CURRENT_DATE" in query


def test_list_file_returns_empty_list():
    mock_engine, _ = _make_engine_mock(fetchall_return=[])

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import list_file_from_db
        result = list_file_from_db()

    assert result == []


def test_list_file_sqlalchemy_error_is_raised():
    mock_engine, mock_conn = _make_engine_mock()
    mock_conn.execute.side_effect = SQLAlchemyError("timeout")

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import list_file_from_db
        with pytest.raises(SQLAlchemyError):
            list_file_from_db()
