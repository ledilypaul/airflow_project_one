import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, dag, task

# Same name in every DAG file: Airflow matches Assets by name/URI, not by object
# identity. Not imported from a shared `dags.*` module, which only resolves with
# the project root on sys.path (C-1).
FILE_LIST_ASSET = Asset(name="file_list")

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

    # Emits the `file_list` Asset on success only, which schedules
    # read_file_dag (P1-4). A skipped run (empty listing, P1-6, or no file
    # recorded, C-3) emits nothing: an event always implies >= 1 row.
    @task(outlets=[FILE_LIST_ASSET])
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

        # Files handled without error: inserted, or already present for this
        # run (retry: ON CONFLICT DO NOTHING inserts no new row but does not
        # raise). Not a SQL rowcount, on purpose.
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

        if inserted == 0:
            # Every listed file vanished before insertion: no row for this run,
            # so no asset event (read_file_dag would find nothing, C-3).
            raise AirflowSkipException(
                f"0/{len(file_list)} listed files could be recorded (all vanished), "
                "nothing to process"
            )
        log.info("%d/%d files recorded", inserted, len(file_list))
        return inserted

    insert_files(list_files())


dag_instance = list_files_dag()
