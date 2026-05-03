"""
Unit tests for utils/functions_utils.py.
Fixes the original test which mocked psycopg2 cursors instead of SQLAlchemy.
"""
import sys
import pytest
from unittest.mock import patch, MagicMock

# psycopg2 is not installed in this venv. Stub it with a real Exception subclass
# so that `except psycopg2.DatabaseError` in functions_utils.py works correctly.
_psycopg2_stub = MagicMock()
_psycopg2_stub.DatabaseError = type("DatabaseError", (Exception,), {})
sys.modules.setdefault("psycopg2", _psycopg2_stub)
from sqlalchemy.exc import SQLAlchemyError


def _make_engine_mock(fetchall_return=None):
    """Build a mock SQLAlchemy engine with a usable context-manager connection."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    # Support: with engine.connect() as conn:
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    if fetchall_return is not None:
        mock_conn.execute.return_value.fetchall.return_value = fetchall_return
    return mock_engine, mock_conn


# ---------------------------------------------------------------------------
# insert_into_file_list
# ---------------------------------------------------------------------------

def test_insert_calls_execute():
    """insert_into_file_list() executes an INSERT via the SQLAlchemy connection."""
    mock_engine, mock_conn = _make_engine_mock()

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import insert_into_file_list
        insert_into_file_list(["orders.csv", "/data/orders.csv", "2024-01-01"])

    mock_conn.execute.assert_called_once()
    call_args = mock_conn.execute.call_args
    query = call_args[0][0]
    assert "INSERT INTO file_list" in query


def test_insert_passes_correct_values():
    """The three data values are forwarded correctly to execute()."""
    mock_engine, mock_conn = _make_engine_mock()

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import insert_into_file_list
        insert_into_file_list(["file.csv", "/path/file.csv", "2024-06-15"])

    params = mock_conn.execute.call_args[0][1]  # second positional arg = tuple of values
    assert params[0] == "file.csv"
    assert params[1] == "/path/file.csv"


def test_insert_db_error_propagates_uncaught():
    """
    BUG documentation: the except clause catches psycopg2.DatabaseError,
    but SQLAlchemy raises sqlalchemy.exc.SQLAlchemyError.
    Result: DB errors are NOT caught and propagate as the original exception.
    Fix: replace `except psycopg2.DatabaseError` with `except SQLAlchemyError`.
    """
    mock_engine, mock_conn = _make_engine_mock()
    mock_conn.execute.side_effect = Exception("connection refused")

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import insert_into_file_list
        # The original exception propagates because except psycopg2.DatabaseError
        # never matches a generic Exception raised by SQLAlchemy.
        with pytest.raises(Exception, match="connection refused"):
            insert_into_file_list(["file.csv", "/path/file.csv", "2024-01-01"])


# ---------------------------------------------------------------------------
# list_file_from_db
# ---------------------------------------------------------------------------

def test_list_file_returns_rows():
    """list_file_from_db() returns the rows fetched from DB."""
    rows = [(1, "a.csv", "/data/a.csv", None, None, None, None, None)]
    mock_engine, mock_conn = _make_engine_mock(fetchall_return=rows)

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import list_file_from_db
        result = list_file_from_db()

    assert result == rows
    mock_conn.execute.assert_called_once()
    query = mock_conn.execute.call_args[0][0]
    assert "SELECT" in query
    assert "CURRENT_DATE" in query


def test_list_file_returns_empty_list():
    mock_engine, _ = _make_engine_mock(fetchall_return=[])

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import list_file_from_db
        result = list_file_from_db()

    assert result == []


def test_list_file_db_error_propagates_uncaught():
    """Same bug as insert: SQLAlchemy errors bypass the except clause."""
    mock_engine, mock_conn = _make_engine_mock()
    mock_conn.execute.side_effect = Exception("timeout")

    with patch("utils.functions_utils.db_connection", return_value=mock_engine):
        from utils.functions_utils import list_file_from_db
        with pytest.raises(Exception, match="timeout"):
            list_file_from_db()
