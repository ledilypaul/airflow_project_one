"""Create or update the Airflow Connections of the local dev environment.

Called by ``make airflow-init`` (after ``airflow db migrate``), with the same
``AIRFLOW_HOME`` as the other Airflow targets. Idempotent: a Connection that
already exists is updated in place, never duplicated.

- ``file_list_db`` (postgres): the ``file_list`` table of list_files_dag, built
  from ``DB_HOST``, ``DB_PORT``, ``DB_USER``, ``DB_PASSWORD`` and ``DB_NAME``.
  They come from an env file (``.env`` by default, overridable with
  ``--env-file`` or the ``ENV_FILE`` variable), loaded with python-dotenv.
  A variable already set in the process environment wins over the file, as
  with ``load_dotenv()`` in ``utils/db_connection.py``: the Connection then
  points at the same database as the Spark job.
- ``spark_default`` (spark): master ``local[*]``. Without it the Spark
  provider silently falls back to ``yarn`` (see CLAUDE.md).

Secrets: the password is only ever held in memory and written to the
metadata DB through the ORM (``airflow.models.connection.Connection``, the
same path as ``airflow connections add/import``), so it never appears in a
command line (``ps``), in ``make``'s output or in this script's output. Error
messages name the missing variables, never their values, and DB errors are
reported without the SQL parameters.
"""

from __future__ import annotations

import argparse
import os
import sys
import warnings
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import dotenv_values

# Same values as utils.functions_utils.FILE_LIST_POSTGRES_CONN_ID and
# dags/local_read_file_dag.SPARK_CONN_ID (checked by
# tests/unit/scripts/test_dev_airflow_connections.py). Not imported: importing
# utils.functions_utils runs load_dotenv() on the project's .env, which would
# bypass --env-file.
FILE_LIST_CONN_ID = "file_list_db"
SPARK_CONN_ID = "spark_default"
SPARK_DEV_MASTER = "local[*]"

DB_VARS = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")
DEFAULT_ENV_FILE = ".env"


class ConfigError(Exception):
    """Invalid or incomplete settings. The message never contains a value."""


@dataclass(frozen=True)
class DbSettings:
    host: str
    port: int
    database: str
    # repr=False: keep the credentials out of tracebacks and debug output.
    user: str = field(repr=False)
    password: str = field(repr=False)


def load_db_settings(env_file: Path, environ: Mapping[str, str] | None = None) -> DbSettings:
    """Read the DB_* variables: process environment first, then ``env_file``.

    Raises ConfigError naming the missing (unset or empty) variables, or the
    variable that is not a valid port. Values are never part of the message.
    """
    environ = os.environ if environ is None else environ
    from_file = dotenv_values(env_file) if env_file.is_file() else {}
    values = {name: environ.get(name) or from_file.get(name) or "" for name in DB_VARS}

    missing = [name for name in DB_VARS if not values[name].strip()]
    if missing:
        raise ConfigError(
            f"missing variable(s) {', '.join(missing)} "
            f"(neither in the environment nor in {env_file})"
        )
    try:
        port = int(values["DB_PORT"])
    except ValueError:
        raise ConfigError("DB_PORT is not an integer") from None
    if not 0 < port < 65536:
        raise ConfigError("DB_PORT is not a valid TCP port")

    return DbSettings(
        host=values["DB_HOST"].strip(),
        port=port,
        database=values["DB_NAME"].strip(),
        user=values["DB_USER"],
        password=values["DB_PASSWORD"],
    )


def _escalate_airflow_deprecations() -> None:
    """Same bar as pyproject.toml's pytest filterwarnings: fail on any Airflow deprecation."""
    from airflow.exceptions import AirflowProviderDeprecationWarning, RemovedInAirflow4Warning
    from airflow.utils.deprecation_tools import DeprecatedImportWarning

    for category in (
        RemovedInAirflow4Warning,
        AirflowProviderDeprecationWarning,
        DeprecatedImportWarning,
    ):
        warnings.filterwarnings("error", category=category)


def upsert_connections(db: DbSettings) -> list[str]:
    """Create or update both Connections in the Airflow metadata DB.

    Returns one human-readable line per Connection, without login or password.
    """
    # Airflow is imported here, not at module level: importing it initialises
    # its settings from AIRFLOW_HOME, which the env-file validation above does
    # not need (and the unit tests run without an Airflow metadata DB).
    _escalate_airflow_deprecations()
    from airflow.models.connection import Connection
    from airflow.utils.session import create_session
    from sqlalchemy import select

    wanted = [
        (
            Connection(
                conn_id=FILE_LIST_CONN_ID,
                conn_type="postgres",
                description="file_list table (docker-compose Postgres), see make airflow-init",
                host=db.host,
                port=db.port,
                schema=db.database,
                login=db.user,
                password=db.password,
            ),
            f" (host={db.host}, port={db.port}, database={db.database})",
        ),
        (
            Connection(
                conn_id=SPARK_CONN_ID,
                conn_type="spark",
                description="Local Spark master for dev, managed by make airflow-init",
                host=SPARK_DEV_MASTER,
            ),
            f" (host={SPARK_DEV_MASTER})",
        ),
    ]

    lines = []
    with create_session() as session:
        for conn, details in wanted:
            existing_id = session.scalar(
                select(Connection.id).where(Connection.conn_id == conn.conn_id)
            )
            if existing_id is None:
                session.add(conn)
                action = "created"
            else:
                # Same overwrite as `airflow connections import --overwrite`:
                # reuse the primary key and merge every field of the new object.
                conn.id = existing_id
                session.merge(conn)
                action = "updated"
            lines.append(f"Connection {conn.conn_id} {action}{details}")
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(os.environ.get("ENV_FILE") or DEFAULT_ENV_FILE),
        help="env file with the DB_* variables (default: $ENV_FILE, else .env)",
    )
    args = parser.parse_args(argv)

    if not args.env_file.is_file():
        print(
            f"note: env file {args.env_file} not found, using the process environment only",
            file=sys.stderr,
        )
    try:
        db = load_db_settings(args.env_file)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    from sqlalchemy.exc import SQLAlchemyError

    try:
        lines = upsert_connections(db)
    except SQLAlchemyError as exc:
        # str(exc) may embed the SQL parameters (the password, if no Fernet key
        # is configured): report the driver error only.
        detail = f": {exc.orig}" if getattr(exc, "orig", None) is not None else ""
        print(f"error: metadata DB write failed ({type(exc).__name__}{detail})", file=sys.stderr)
        return 1

    for line in lines:
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
