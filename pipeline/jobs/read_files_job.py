"""
Standalone Spark job: reads the files a listing run recorded in file_list and
writes them to the lake as partitioned Parquet (P2-2).

For each file of the run (`--run-id`):
    raw zone      <LAKE_ROOT>/<storage.raw_path><table>/ : the file as read;
    staging zone  <LAKE_ROOT>/<storage.staging_path><table>/ (only when the
                  source declares `staging_path`): the same rows minus fully
                  empty rows and exact duplicates, nothing else.
    quality zone  <LAKE_ROOT>/quality/<table>/ (only when the source declares
                  `quality`, P2-3): one row per check and per run, with a
                  status: `pass`, `fail` (failed, severity error), `warn`
                  (failed, severity warn) or `error` (the check could not
                  run). The checks run on the staging rows. A blocking check
                  (severity `error`, status `fail` or `error`) keeps raw,
                  removes and skips the staging partition of the file, and
                  marks it `failed` with a summary (`quality checks failed:
                  not_null(id): 3 rows; ...`). A `warn` check never blocks:
                  logged (at ERROR level when it could not run) and
                  recorded. If the engine itself raises, the file fails
                  closed the same way, with a short message. Whenever the
                  source has a staging zone, the quality partition of the
                  (run, file) is removed first: no orphan or stale results.
All three are partitioned by (`lake_run_id`, `lake_source_file`) and overwritten
partition by partition (storage/parquet_lake_writer.py): a retry or a re-run
of the same run id replaces exactly the partitions of its files (removes
them when the file now yields 0 row), without duplicates, and leaves every
other run and file alone. The config is resolved once per file and used for
both the read and the writes (`storage.mode` included).

Snapshot semantics (C-4): every listing run records each file again under a
new run id, so a table holds one snapshot per listing. The current state of
a table is, for each `lake_source_file`, the partition of the latest listing,
i.e. the highest `lake_file_id` (= file_list.id, assigned when the listing
recorded the file): `storage.parquet_lake_writer.latest_snapshot`. Not "the
last write wins": re-running an old run rewrites its own partition without
making it current. A re-run reads the source file as it is at that time,
under the old run id.

Statuses: right before its writes, a file is reset to `pending` (error
cleared): a row `processed` by a previous attempt must not keep claiming a
partition this attempt may be replacing (C-5). It becomes `processed` only
once the read and every write of the file succeeded, `failed` (with the
error) otherwise; the other files of the run are still processed.

`LAKE_ROOT` is read from the environment (inherited from the Airflow worker
through spark-submit); unset, it is `<project root>/lake`. A relative path is
refused (exit code 5): the worker's current directory is not a stable place.

The orchestration, shared by the engines, is `pipeline/jobs/ingest.py`
(P5-6); this module is the Spark entry point.

Run manually (recommended, uses the project's uv-managed venv):
    uv run spark-submit pipeline/jobs/read_files_job.py

Run via Airflow:
    ingest__<source> (spark_read_files) → spark-submit pipeline/jobs/read_files_job.py \
        --run-id=<id> --source-name=<source>

Run with a bare/system/cluster `spark-submit` whose driver Python is not the
project's venv (e.g. `ENV=prod` on YARN/Kubernetes where the interpreter
isn't the one `uv sync` installed into): `pipeline.*` / `utils.*` won't be on
sys.path in that case, so the fallback below adds the project root itself.

Exit codes (the ingest__<source> DAGs do not retry 2, 3, 5 and 6, see P2-6):
    0 = every file processed;
    1 = at least one file failed (its row is `failed`, with the error) for
        a reason a retry may change: read or write error, quality engine
        raising (`quality checks could not run`), blocking check that could
        not run (status `error`), quality results not recorded or stale
        staging partition not removed, two files with the same name. Also
        any mix of those with quality failures;
    2 = missing/invalid --run-id (nothing queried, nothing written);
    3 = no file_list row for that run id (nothing written);
    4 = parent process gone (see `start_parent_watchdog`): the job stopped on
        its own, without updating any file_list status;
    5 = invalid config, reading (`config/sources/`, `config/defaults/`, a
        source without `storage` or nested lake tables included) or
        `LAKE_ROOT` (relative path, `file:lake` included): every row of the
        run is `failed` with the config error, Spark never started. Also a
        quality regex that Java refuses (P2-3 / C-8), detected right after
        Spark starts and before any file is read: same marking, Spark
        stopped. Deterministic: a retry fails the same way until it is fixed;
    6 = at least one file failed, and every failed file failed only on
        blocking quality checks that found failing rows (status `fail`,
        severity `error`), their results recorded and the file's staging
        partition removed (P2-6). The other files may be `processed`.
        Deterministic: the same data fails the same checks, so a retry
        would only rewrite the same partitions.

Priority, for the final code of a run that reached the files: 0 when no
file failed; otherwise 6 when every failed file is a deterministic quality
failure; otherwise 1. 1 wins over 6 as soon as one failure is of another
kind: a retry must stay possible for that file.

Checks run in this order, cheapest and most fundamental first: the run id
(2, no I/O), then its rows (3: without rows there is nothing to process nor
to mark failed, and a wrong run id is the more fundamental error), then the
reading configs and LAKE_ROOT (5, needs the rows to mark them), then Spark,
then the Java compilation of every quality regex of the registry (5, needs
the JVM), and only then the files.
"""

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
    from pipeline.config import DEFAULT_CONFIG_ROOT, SourceRegistry
    from pipeline.engine.spark.engine import SparkEngine
    from pipeline.file_reader.base_reader import BaseFileReader
    from pipeline.jobs.ingest import (
        EXIT_FILES_FAILED,
        EXIT_INVALID_CONFIG,
        EXIT_INVALID_RUN_ID,
        EXIT_NO_ROWS,
        EXIT_PARENT_GONE,
        EXIT_QUALITY_FAILED,
        final_exit_code,
        parse_args,
        run,
    )
    from storage.parquet_lake_writer import ParquetLakeWriter
    from utils.spark_session import get_spark_session
except ModuleNotFoundError:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(PROJECT_ROOT))
    from pipeline.config import DEFAULT_CONFIG_ROOT, SourceRegistry
    from pipeline.engine.spark.engine import SparkEngine
    from pipeline.file_reader.base_reader import BaseFileReader
    from pipeline.jobs.ingest import (
        EXIT_FILES_FAILED,
        EXIT_INVALID_CONFIG,
        EXIT_INVALID_RUN_ID,
        EXIT_NO_ROWS,
        EXIT_PARENT_GONE,
        EXIT_QUALITY_FAILED,
        final_exit_code,
        parse_args,
        run,
    )
    from storage.parquet_lake_writer import ParquetLakeWriter
    from utils.spark_session import get_spark_session

__all__ = [
    "EXIT_FILES_FAILED",
    "EXIT_INVALID_CONFIG",
    "EXIT_INVALID_RUN_ID",
    "EXIT_NO_ROWS",
    "EXIT_PARENT_GONE",
    "EXIT_QUALITY_FAILED",
    "final_exit_code",
    "main",
    "parse_args",
    "start_parent_watchdog",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger(__name__)

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


def create_spark_engine(registry: SourceRegistry, lake_root: str) -> SparkEngine:
    spark = get_spark_session("ReadFilesJob")
    try:
        reader = BaseFileReader(spark, registry=registry)
        return SparkEngine(spark, ParquetLakeWriter(lake_root), reader)
    except Exception:
        spark.stop()
        raise


def main(run_id: str, source_name: str | None = None):
    # First thing: the driver must never outlive a killed spark-submit (C-1).
    start_parent_watchdog()
    run(run_id, source_name, config_root=CONFIG_ROOT, create_engine=create_spark_engine)


if __name__ == "__main__":
    args = parse_args()
    main(run_id=args.run_id, source_name=args.source_name)
