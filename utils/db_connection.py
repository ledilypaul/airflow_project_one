import logging
import os
import threading

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()

log = logging.getLogger(__name__)

# One Engine (= one connection pool) per connection URL for the lifetime of
# the process, instead of a new create_engine() on every db_connection()
# call (M3). Keyed on the URL rather than kept as a single global so that
# a process touching more than one database (unlikely here, but cheap to
# support) still gets one Engine per target.
_engine_cache: dict[str, Engine] = {}
_engine_cache_lock = threading.Lock()


def _get_db_env() -> dict:
    keys = ("DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME")
    env = {k: os.getenv(k) for k in keys}
    if not all(env.values()):
        raise ValueError("One or more required environment variables are not set.")
    return env


def db_connection() -> Engine:
    env = _get_db_env()
    url = (
        f"postgresql+psycopg2://{env['DB_USER']}:{env['DB_PASSWORD']}"
        f"@{env['DB_HOST']}:{env['DB_PORT']}/{env['DB_NAME']}"
    )
    with _engine_cache_lock:
        engine = _engine_cache.get(url)
        if engine is None:
            engine = create_engine(url)
            _engine_cache[url] = engine
    return engine


def _reset_engine_cache() -> None:
    """Test-only helper.

    Clears the module-level engine cache so each test that patches
    `create_engine` gets its own call instead of transparently reusing an
    Engine built (and cached) by a previous test that happened to use the
    same connection URL.
    """
    with _engine_cache_lock:
        _engine_cache.clear()


def jdbc_connection_props():
    env = _get_db_env()
    url = f"jdbc:postgresql://{env['DB_HOST']}:{env['DB_PORT']}/{env['DB_NAME']}"
    props = {
        "user": env["DB_USER"],
        "password": env["DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }
    return url, props
