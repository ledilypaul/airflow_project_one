import logging
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

log = logging.getLogger(__name__)


def _get_db_env() -> dict:
    keys = ("DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME")
    env = {k: os.getenv(k) for k in keys}
    if not all(env.values()):
        raise ValueError("One or more required environment variables are not set.")
    return env


def db_connection():
    env = _get_db_env()
    url = (
        f"postgresql+psycopg2://{env['DB_USER']}:{env['DB_PASSWORD']}"
        f"@{env['DB_HOST']}:{env['DB_PORT']}/{env['DB_NAME']}"
    )
    return create_engine(url)


def jdbc_connection_props():
    env = _get_db_env()
    url = f"jdbc:postgresql://{env['DB_HOST']}:{env['DB_PORT']}/{env['DB_NAME']}"
    props = {
        "user": env["DB_USER"],
        "password": env["DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }
    return url, props
