import logging
from datetime import date

from sqlalchemy import bindparam, text
from sqlalchemy.exc import SQLAlchemyError

from utils.db_connection import db_connection

log = logging.getLogger(__name__)

# Airflow Connection id to create manually (UI/CLI) with the real
# credentials; this is just a reference name, not a hardcoded secret.
FILE_LIST_POSTGRES_CONN_ID = "file_list_db"


def _file_list_engine_from_airflow_connection():
    """Build an engine from the `file_list_db` Airflow Connection.

    `PostgresHook` is imported here, never at module level: this module is
    also imported by the Spark job (pipeline/jobs/read_files_job.py), which
    must never import Airflow. The job runs in the environment of an Airflow 3
    task, where the Task SDK supervisor sets
    `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=airflow-db-not-allowed:///`: importing
    `airflow` there fails in `settings.initialize()` with a SQLAlchemy
    `ArgumentError`. It must also stay importable in a Spark-only environment
    (`uv sync --no-group airflow`) where the provider is not installed.
    """
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
    except ImportError as e:
        raise RuntimeError(
            "insert_into_file_list() / count_processed_files() without an explicit engine needs "
            "apache-airflow-providers-postgres (install the 'airflow' dependency "
            "group: `uv sync`), or pass engine=..."
        ) from e
    return PostgresHook(postgres_conn_id=FILE_LIST_POSTGRES_CONN_ID).get_sqlalchemy_engine()


def insert_into_file_list(data, dag_run_id: str = None, engine=None):
    try:
        if engine is None:
            engine = _file_list_engine_from_airflow_connection()
        with engine.begin() as conn:
            query = text("""
                INSERT INTO file_list (
                    file_name, file_path, last_modified, ingested_at, dag_run_id, status
                )
                VALUES (:file_name, :file_path, :last_modified, :ingested_at, :dag_run_id, :status)
                ON CONFLICT (file_path, dag_run_id) DO NOTHING
            """)
            conn.execute(
                query,
                {
                    "file_name": data[0],
                    "file_path": data[1],
                    "last_modified": data[2],
                    "ingested_at": date.today(),
                    "dag_run_id": dag_run_id or "",
                    "status": "pending",
                },
            )
            log.info("Inserted %s into file_list", data[0])
    except SQLAlchemyError as e:
        log.error("Error inserting %s: %s", data[0], e)
        raise


def list_file_from_db(run_id: str):
    """Return every row of `file_list` belonging to `run_id`.

    Deliberately no filter on `status` or on a date: a Clear that reruns the
    same `run_id` must find ALL its files again, including those already
    `processed` or `failed`, so a `failed` file is never stuck forever.
    """
    try:
        engine = db_connection()
        with engine.begin() as conn:
            query = text("""
                SELECT
                    id, file_name, file_path, last_modified,
                    status, dag_run_id, created_at, ingested_at
                FROM file_list
                WHERE dag_run_id = :run_id
            """)
            result = conn.execute(query, {"run_id": run_id})
            rows = result.mappings().fetchall()
            log.info("Retrieved %d files from file_list for run_id=%s", len(rows), run_id)
            return rows
    except SQLAlchemyError as e:
        log.error("Error querying file_list: %s", e)
        raise


def update_file_status(file_id: int, status: str, error_message: str | None = None):
    """Update the status (and error_message) of a single file_list row.

    Never touches dag_run_id: a file belongs to exactly one run, set once and
    for all at insertion time (insert_into_file_list). error_message is
    always written explicitly (defaults to None) so a file that succeeds
    after a previous failure has its stale error message cleared.
    """
    try:
        engine = db_connection()
        with engine.begin() as conn:
            query = text("""
                UPDATE file_list
                SET status = :status, error_message = :error_message
                WHERE id = :id
            """)
            conn.execute(query, {"id": file_id, "status": status, "error_message": error_message})
            log.info("Updated file_list id=%s to status=%s", file_id, status)
    except SQLAlchemyError as e:
        log.error("Error updating file_list id=%s: %s", file_id, e)
        raise


def count_processed_files(run_ids, engine=None) -> int:
    """Number of `file_list` rows with status `processed` among `run_ids`.

    Used by read_file_dag's `publish_lake_staging` (P2-4 / C-2) to emit
    `lake_staging` when at least one file of its run reached the lake, even
    if another one failed. Goes through the `file_list_db` Airflow Connection
    (the task runs in Airflow, not in the Spark job). Bound parameters only.
    No run id: 0, without querying.
    """
    run_ids = list(dict.fromkeys(run_ids))
    if not run_ids:
        return 0
    if engine is None:
        engine = _file_list_engine_from_airflow_connection()
    query = text("""
        SELECT COUNT(*)
        FROM file_list
        WHERE status = :status AND dag_run_id IN :run_ids
    """).bindparams(bindparam("run_ids", expanding=True))
    try:
        with engine.connect() as conn:
            count = conn.execute(query, {"status": "processed", "run_ids": run_ids}).scalar_one()
    except SQLAlchemyError as e:
        log.error("Error counting processed files in file_list: %s", e)
        raise
    log.info("%d processed file(s) in file_list for run_ids=%s", count, run_ids)
    return int(count)
