import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

# airflow-project-one est installé en mode éditable (`uv sync`), donc
# `pipeline.*` / `utils.*` / `storage.*` sont importables sans manipuler
# sys.path (voir CLAUDE.md, M5).
PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG_PATH = str(PROJECT_ROOT / "config" / "extractor_config.yaml")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

log = logging.getLogger(__name__)


@dag(
    dag_id="list_files_dag",
    description="List files in the source directory and persist metadata",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 12, 18),
    schedule=None,
    catchup=False,
    tags=["extraction", "file-listing"],
)
def list_files_dag():

    @task
    def list_files() -> list[str]:
        from pipeline.extractor.extractor import Extractor

        extractor = Extractor(config_path=CONFIG_PATH)
        files = extractor.list_files()
        log.info("Found %d files to process", len(files))
        return files

    @task
    def insert_files(file_list: list[str]) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.sdk import get_current_context
        from airflow.sdk.exceptions import AirflowSkipException

        from utils.functions_utils import FILE_LIST_POSTGRES_CONN_ID, insert_into_file_list

        # "Nothing to process" is not an error: skip (no red run, no
        # useless retry) rather than fail (M2 / P1-6).
        if not file_list:
            raise AirflowSkipException("No files found in source directories, nothing to insert")

        context = get_current_context()
        run_id = context["dag_run"].run_id

        # One engine built once for the whole task, reused for every file
        # below, instead of one PostgresHook/engine per inserted row (M3).
        engine = PostgresHook(postgres_conn_id=FILE_LIST_POSTGRES_CONN_ID).get_sqlalchemy_engine()

        inserted = 0
        for filepath in file_list:
            try:
                filename = os.path.basename(filepath)
                mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                insert_into_file_list(
                    [filename, filepath, mod_time], dag_run_id=run_id, engine=engine
                )
                inserted += 1
            except FileNotFoundError:
                log.warning("File not found, skipping: %s", filepath)
            except Exception as e:
                log.error("Insertion error for %s: %s", filepath, e)
                raise

        log.info("%d/%d files inserted", inserted, len(file_list))
        return inserted

    insert_files(list_files())


dag_instance = list_files_dag()
