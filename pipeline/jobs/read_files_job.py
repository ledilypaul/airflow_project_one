"""
Standalone Spark job: reads files listed in the DB and displays their content.

Run manually (recommended, uses the project's uv-managed venv):
    uv run spark-submit pipeline/jobs/read_files_job.py

Run via Airflow:
    read_file_dag (SparkSubmitOperator) → spark-submit pipeline/jobs/read_files_job.py --run-id=<id>

Run with a bare/system/cluster `spark-submit` whose driver Python is not the
project's venv (e.g. `ENV=prod` on YARN/Kubernetes where the interpreter
isn't the one `uv sync` installed into): `pipeline.*` / `utils.*` won't be on
sys.path in that case, so the fallback below adds the project root itself.

Exit codes:
    0 = every file processed;
    1 = at least one file failed (its row is `failed`, with the error);
    2 = missing/invalid --run-id (nothing queried, nothing written);
    3 = no file_list row for that run id (nothing written);
    4 = parent process gone (see `start_parent_watchdog`): the job stopped on
        its own, without updating any file_list status;
    5 = invalid reading config (`config/sources/`, `config/defaults/`): every
        row of the run is `failed` with the config error, Spark never started.
        Deterministic: a retry fails the same way until the YAML is fixed.

Checks run in this order, cheapest and most fundamental first: the run id
(2, no I/O), then its rows (3: without rows there is nothing to process nor
to mark failed, and a wrong run id is the more fundamental error), then the
reading configs (5, needs the rows to mark them), and only then Spark.
"""

import argparse
import logging
import os
import sys
import threading
import time
from pathlib import Path

# airflow-project-one est installé en mode éditable (`uv sync`), donc
# `pipeline.*` / `utils.*` / `storage.*` sont normalement importables sans
# manipuler sys.path (voir CLAUDE.md, M5). Mais ce script peut aussi être
# lancé avec un interpréteur qui n'est pas celui du venv uv (spark-submit
# "nu", driver/exécuteur de cluster sous ENV=prod) : dans ce cas l'install
# éditable n'existe pas pour cet interpréteur et l'import échoue. On ne
# bidouille donc sys.path qu'en dernier recours, si l'import direct rate.
try:
    from pipeline.config import DEFAULT_CONFIG_ROOT, ConfigError, SourceRegistry
    from pipeline.file_reader.base_reader import BaseFileReader
    from utils.functions_utils import list_file_from_db, update_file_status
    from utils.spark_session import get_spark_session
except ModuleNotFoundError:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(PROJECT_ROOT))
    from pipeline.config import DEFAULT_CONFIG_ROOT, ConfigError, SourceRegistry
    from pipeline.file_reader.base_reader import BaseFileReader
    from utils.functions_utils import list_file_from_db, update_file_status
    from utils.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger(__name__)

EXIT_PARENT_GONE = 4
EXIT_INVALID_CONFIG = 5

# Root of the reading configs (`sources/`, `defaults/`); a module attribute so
# that tests can point it at a temporary directory.
CONFIG_ROOT = DEFAULT_CONFIG_ROOT


def start_parent_watchdog(interval: float = 1.0) -> threading.Thread:
    """Exit the process with `EXIT_PARENT_GONE` as soon as its parent dies.

    P1-5 / C-1: in local/client mode `spark-submit` execs a JVM that starts
    this script as its child. When Airflow kills the task (timeout, "mark
    success/failed", dagrun_timeout...), `SparkSubmitHook.on_kill` sends
    SIGKILL to the spark-submit PID only: the JVM dies but this Python driver
    would keep running, re-parented, outside Airflow's control (pool slot
    released, `failed` written for the remaining files while a retry runs).

    A daemon thread records `os.getppid()` and checks every `interval`
    seconds that it has not changed: once the parent is gone the process is
    re-parented (to init or a subreaper) and its ppid changes for good. It
    then logs an error and calls `os._exit` (no cleanup, no `finally`, no
    status written to file_list: a retry of the run reprocesses every file
    of the run id idempotently, P1-3). Run directly from a shell, the parent
    is that shell: closing it stops the job the same way.
    """
    parent_pid = os.getppid()

    def _watch() -> None:
        while True:
            time.sleep(interval)
            if os.getppid() != parent_pid:
                try:
                    log.error(
                        "parent process (pid %d) is gone: spark-submit was killed (Airflow "
                        "task killed or timed out?). Exiting with code %d without updating "
                        "file_list; a retry of the run reprocesses its files.",
                        parent_pid,
                        EXIT_PARENT_GONE,
                    )
                finally:
                    os._exit(EXIT_PARENT_GONE)

    thread = threading.Thread(target=_watch, name="parent-watchdog", daemon=True)
    thread.start()
    return thread


def main(run_id: str):
    # First thing: the driver must never outlive a killed spark-submit (C-1).
    start_parent_watchdog()

    if run_id is None or run_id == "" or run_id == "None":
        log.error(
            "Missing or invalid run_id (%r): refusing to query file_list without a run_id "
            "to scope to (idempotence, P1-3). Pass a real --run-id.",
            run_id,
        )
        sys.exit(2)

    file_list = list_file_from_db(run_id)
    if not file_list:
        # P1-4 / C-3: a source run that emitted the `file_list` asset always
        # has at least one row (empty listings and "every file vanished" both
        # skip the insertion task, so no event). Zero rows is an anomaly
        # (typo in a manual run_id, purged rows...), not a green no-op.
        log.error(
            "no file_list rows for run_id=%r: typo in the run_id or purged data? "
            "Nothing was processed.",
            run_id,
        )
        sys.exit(3)

    # P2-1 / C-2: load and validate every reading config before starting
    # Spark. An invalid config is a deterministic error for the whole run, not
    # a file error: mark every row `failed` with it and exit with a dedicated
    # code, instead of leaving them `pending` behind an uncaught exception.
    try:
        registry = SourceRegistry.from_directory(CONFIG_ROOT)
    except ConfigError as exc:
        error_message = f"invalid reading config: {exc}"
        log.error(
            "%s\nNo file of run_id=%r was read; every row is marked failed.", error_message, run_id
        )
        for file in file_list:
            update_file_status(file["id"], "failed", error_message=error_message)
        sys.exit(EXIT_INVALID_CONFIG)

    spark = get_spark_session("ReadFilesJob")
    try:
        reader = BaseFileReader(spark, registry=registry)

        success_count = 0
        fail_count = 0
        for file in file_list:
            file_id = file["id"]
            file_name = file["file_name"]
            file_path = file["file_path"]
            try:
                content = reader.read_file(file_path)
                log.info("Read %s (id=%s)", file_name, file_id)
                content.show(truncate=False)
                update_file_status(file_id, "processed")
                success_count += 1
            except Exception as e:
                log.error("Error reading %s: %s", file_path, e)
                update_file_status(file_id, "failed", error_message=str(e))
                fail_count += 1
    finally:
        spark.stop()

    log.info("%d/%d files processed successfully.", success_count, len(file_list))
    sys.exit(1 if fail_count > 0 else 0)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the job's command line (`sys.argv[1:]` when `argv` is None).

    read_file_dag passes the run id as a single `--run-id=<value>` argument,
    so a run id starting with `-` is not mistaken for an option (P1-4 / C-4).
    """
    parser = argparse.ArgumentParser(
        description="Read files listed in the DB for a given Airflow run_id."
    )
    parser.add_argument(
        "--run-id", required=True, help="dag_run_id of the list_files_dag run to process."
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    main(run_id=parse_args().run_id)
