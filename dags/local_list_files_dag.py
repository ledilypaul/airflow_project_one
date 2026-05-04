import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

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
        from airflow.operators.python import get_current_context
        from utils.functions_utils import insert_into_file_list

        if not file_list:
            log.warning("No files found, nothing to insert")
            raise ValueError("No files found")

        context = get_current_context()
        run_id = context["dag_run"].run_id

        inserted = 0
        for filepath in file_list:
            try:
                filename = os.path.basename(filepath)
                mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                insert_into_file_list([filename, filepath, mod_time], dag_run_id=run_id)
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
