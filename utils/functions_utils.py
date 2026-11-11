import logging
from datetime import date

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from utils.db_connection import db_connection

log = logging.getLogger(__name__)


def insert_into_file_list(data, dag_run_id: str = None):
    try:
        engine = db_connection()
        with engine.begin() as conn:
            query = text("""
                INSERT INTO file_list (
                    file_name, file_path, last_modified, ingested_at, dag_run_id, status
                )
                VALUES (:file_name, :file_path, :last_modified, :ingested_at, :dag_run_id, :status)
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
