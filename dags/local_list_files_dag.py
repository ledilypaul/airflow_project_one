import json
import logging
import os
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

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


# P3-3: failure reports (one structured `airflow_failure` log line, see
# utils/failure_alert.py). Imported inside the callbacks: a DAG file imports no
# project code at parse time (the DAG processor does not have the project root
# on sys.path). The report never raises. If `utils` cannot be imported (a worker
# image without the project), a minimal line is logged instead (dag_id, run_id,
# reason): a failure is never silent. The four DAG files carry the same few
# lines, because a shared module would itself be project code.
def _report_failure(context: Any, scope: str) -> None:
    try:
        from utils import failure_alert
    except ImportError:
        try:
            get = context.get if isinstance(context, Mapping) else {}.get
            ti, dag = get("ti"), get("dag")
            record = {
                "scope": scope,
                "dag_id": getattr(ti, "dag_id", None) or getattr(dag, "dag_id", None),
                "run_id": get("run_id") or getattr(ti, "run_id", None),
                "reason": get("reason"),
                "task_id": getattr(ti, "task_id", None),
            }
            line = json.dumps(
                {k: v for k, v in record.items() if v is not None}, sort_keys=True, default=str
            )
            logging.getLogger(__name__).error("airflow_failure %s", line)
        except Exception:
            pass
        return
    report = (
        failure_alert.log_task_failure if scope == "task" else failure_alert.log_dag_run_failure
    )
    report(context)


def on_task_failure(context: Any) -> None:
    _report_failure(context, "task")


def on_dag_run_failure(context: Any) -> None:
    _report_failure(context, "dag_run")


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
}

log = logging.getLogger(__name__)


@dag(
    dag_id="list_files_dag",
    description="List files in the source directory and persist metadata",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 12, 18),
    schedule=None,
    catchup=False,
    on_failure_callback=on_dag_run_failure,
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

        from utils.file_list_lineage import report_file_list_lineage
        from utils.functions_utils import FILE_LIST_POSTGRES_CONN_ID, insert_into_file_list

        # "Nothing to process" is not an error: skip (no red run, no
        # useless retry) rather than fail (M2 / P1-6).
        if not file_list:
            raise AirflowSkipException("No files found in source directories, nothing to insert")

        context = get_current_context()
        run_id = context["dag_run"].run_id

        # One engine built once for the whole task, reused for every file
        # below, instead of one PostgresHook/engine per inserted row (M3).
        hook = PostgresHook(postgres_conn_id=FILE_LIST_POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        # Files handled without error: inserted, or already present for this
        # run (retry: ON CONFLICT DO NOTHING inserts no new row but does not
        # raise). Not a SQL rowcount, on purpose.
        inserted = 0
        recorded: list[str] = []
        for filepath in file_list:
            try:
                filename = os.path.basename(filepath)
                mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                insert_into_file_list(
                    [filename, filepath, mod_time], dag_run_id=run_id, engine=engine
                )
                inserted += 1
                recorded.append(filepath)
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
        # P2-5: OpenLineage cannot see SQL sent through the engine: declare
        # the recorded files (inputs) and file_list (output). Never raises.
        report_file_list_lineage(hook, role="output", source_files=recorded)
        log.info("%d/%d files recorded", inserted, len(file_list))
        return inserted

    insert_files(list_files())


dag_instance = list_files_dag()
