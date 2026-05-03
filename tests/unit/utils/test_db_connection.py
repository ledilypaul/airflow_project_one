"""
Unit tests for utils/db_connection.py.
Fixes the original test which imported a non-existent close_connection()
and mocked psycopg2 instead of SQLAlchemy.
"""
import os
import pytest
from unittest.mock import patch, MagicMock


DB_ENV = {
    "DB_USER": "user",
    "DB_PASSWORD": "test_pw",  # NOSONAR — fixture value, not a real credential
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_NAME": "testdb",
}


# ---------------------------------------------------------------------------
# db_connection()
# ---------------------------------------------------------------------------

def test_db_connection_returns_engine():
    """Returns the engine created by SQLAlchemy create_engine."""
    mock_engine = MagicMock()
    with patch.dict(os.environ, DB_ENV), \
         patch("utils.db_connection.create_engine", return_value=mock_engine) as mock_create:

        from utils.db_connection import db_connection
        engine = db_connection()

        assert engine is mock_engine
        mock_create.assert_called_once()


def test_db_connection_url_format():
    """The connection URL follows postgresql+psycopg2://user:pass@host:port/db."""
    with patch.dict(os.environ, DB_ENV), \
         patch("utils.db_connection.create_engine") as mock_create:

        mock_create.return_value = MagicMock()
        from utils.db_connection import db_connection
        db_connection()

        url = mock_create.call_args[0][0]
        assert "postgresql+psycopg2://" in url
        assert "user:test_pw@localhost:5432/testdb" in url


def test_db_connection_missing_env_raises():
    """ValueError is raised when any required env var is missing."""
    incomplete_env = {k: v for k, v in DB_ENV.items() if k != "DB_PASSWORD"}
    with patch.dict(os.environ, incomplete_env, clear=True):
        from utils.db_connection import db_connection
        with pytest.raises(ValueError, match="environment variables"):
            db_connection()


def test_db_connection_all_vars_missing_raises():
    empty_env = dict.fromkeys(DB_ENV, "")
    with patch.dict(os.environ, empty_env):
        from utils.db_connection import db_connection
        with pytest.raises(ValueError):
            db_connection()


# ---------------------------------------------------------------------------
# jdbc_connection_props()
# ---------------------------------------------------------------------------

def test_jdbc_url_format():
    with patch.dict(os.environ, DB_ENV):
        from utils.db_connection import jdbc_connection_props
        url, _ = jdbc_connection_props()
        assert url == "jdbc:postgresql://localhost:5432/testdb"


def test_jdbc_connection_props_content():
    with patch.dict(os.environ, DB_ENV):
        from utils.db_connection import jdbc_connection_props
        _, props = jdbc_connection_props()
        assert props["user"] == "user"
        assert props["password"] == "test_pw"
        assert props["driver"] == "org.postgresql.Driver"


def test_jdbc_missing_env_raises():
    incomplete = {k: v for k, v in DB_ENV.items() if k != "DB_NAME"}
    with patch.dict(os.environ, incomplete, clear=True):
        from utils.db_connection import jdbc_connection_props
        with pytest.raises(ValueError):
            jdbc_connection_props()
