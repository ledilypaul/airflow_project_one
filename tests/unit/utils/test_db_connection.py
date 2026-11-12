"""
Unit tests for utils/db_connection.py.
Fixes the original test which imported a non-existent close_connection()
and mocked psycopg2 instead of SQLAlchemy.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

DB_ENV = {
    "DB_USER": "user",
    "DB_PASSWORD": "test_pw",  # NOSONAR — fixture value, not a real credential
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_NAME": "testdb",
}


@pytest.fixture(autouse=True)
def _reset_engine_cache():
    """P1-2: db_connection() now caches one Engine per URL at module scope.
    Several tests in this file reuse the same DB_ENV (same URL) but patch
    create_engine with a different MagicMock each time; without resetting
    the cache between tests, a later test would transparently get the
    Engine cached by an earlier test instead of calling its own patched
    create_engine, and mock_create.assert_called_once() would fail."""
    from utils.db_connection import _reset_engine_cache as reset

    reset()
    yield
    reset()


# ---------------------------------------------------------------------------
# db_connection()
# ---------------------------------------------------------------------------


def test_db_connection_returns_engine():
    """Returns the engine created by SQLAlchemy create_engine."""
    mock_engine = MagicMock()
    with (
        patch.dict(os.environ, DB_ENV),
        patch("utils.db_connection.create_engine", return_value=mock_engine) as mock_create,
    ):
        from utils.db_connection import db_connection

        engine = db_connection()

        assert engine is mock_engine
        mock_create.assert_called_once()


def test_db_connection_url_format():
    """The connection URL follows postgresql+psycopg2://user:pass@host:port/db."""
    with patch.dict(os.environ, DB_ENV), patch("utils.db_connection.create_engine") as mock_create:
        mock_create.return_value = MagicMock()
        from utils.db_connection import db_connection

        db_connection()

        url = mock_create.call_args[0][0]
        assert "postgresql+psycopg2://" in url
        assert "user:test_pw@localhost:5432/testdb" in url


def test_p1_2_db_connection_is_reused_across_calls():
    """M3: a single create_engine() call (one pooled Engine) must serve two
    successive db_connection() calls made with the same environment,
    instead of building a brand-new Engine per call."""
    mock_engine = MagicMock()
    with (
        patch.dict(os.environ, DB_ENV),
        patch("utils.db_connection.create_engine", return_value=mock_engine) as mock_create,
    ):
        from utils.db_connection import db_connection

        engine_1 = db_connection()
        engine_2 = db_connection()

        mock_create.assert_called_once()
        assert engine_1 is mock_engine
        assert engine_2 is mock_engine
        assert engine_1 is engine_2


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
