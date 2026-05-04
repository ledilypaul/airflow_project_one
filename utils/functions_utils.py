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
                INSERT INTO file_list (file_name, file_path, last_modified, ingested_at, dag_run_id, status)
                VALUES (:file_name, :file_path, :last_modified, :ingested_at, :dag_run_id, :status)
            """)
            conn.execute(query, {
                "file_name": data[0],
                "file_path": data[1],
                "last_modified": data[2],
                "ingested_at": date.today(),
                "dag_run_id": dag_run_id or "",
                "status": "pending",
            })
            log.info("Inserted %s into file_list", data[0])
    except SQLAlchemyError as e:
        log.error("Error inserting %s: %s", data[0], e)
        raise


def list_file_from_db():
    try:
        engine = db_connection()
        with engine.begin() as conn:
            query = text("""
                SELECT id, file_name, file_path, last_modified, status, dag_run_id, created_at, ingested_at
                FROM file_list
                WHERE ingested_at = CURRENT_DATE
            """)
            result = conn.execute(query)
            rows = result.mappings().fetchall()
            log.info("Retrieved %d files from file_list", len(rows))
            return rows
    except SQLAlchemyError as e:
        log.error("Error querying file_list: %s", e)
        raise
