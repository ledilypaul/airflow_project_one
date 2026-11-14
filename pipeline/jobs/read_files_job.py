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
    1 = at least one file failed (its row is `failed`, with the error), a
        blocking quality check included (P2-3);
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
        stopped. Deterministic: a retry fails the same way until it is fixed.

Checks run in this order, cheapest and most fundamental first: the run id
(2, no I/O), then its rows (3: without rows there is nothing to process nor
to mark failed, and a wrong run id is the more fundamental error), then the
reading configs and LAKE_ROOT (5, needs the rows to mark them), then Spark,
then the Java compilation of every quality regex of the registry (5, needs
the JVM), and only then the files.
"""

import argparse
import logging
import os
import sys
import threading
import time
from pathlib import Path

from pyspark.sql import DataFrame

# airflow-project-one est installé en mode éditable (`uv sync`), donc
# `pipeline.*` / `utils.*` / `storage.*` sont normalement importables sans
# manipuler sys.path (voir CLAUDE.md, M5). Mais ce script peut aussi être
# lancé avec un interpréteur qui n'est pas celui du venv uv (spark-submit
# "nu", driver/exécuteur de cluster sous ENV=prod) : dans ce cas l'install
# éditable n'existe pas pour cet interpréteur et l'import échoue. On ne
# bidouille donc sys.path qu'en dernier recours, si l'import direct rate.
try:
    from pipeline.config import DEFAULT_CONFIG_ROOT, ConfigError, SourceConfig, SourceRegistry
    from pipeline.file_reader.base_reader import BaseFileReader
    from pipeline.processor.base_processor import BaseProcessor
    from pipeline.quality import (
        ERROR,
        WARN,
        QualityChecksError,
        QualityChecksFailed,
        blocking_failures,
        failure_summary,
        java_regex_problems,
        results_dataframe,
        run_quality_checks,
        short_error,
    )
    from storage.parquet_lake_writer import LakeRootError, ParquetLakeWriter, resolve_lake_root
    from utils.functions_utils import list_file_from_db, update_file_status
    from utils.spark_session import get_spark_session
except ModuleNotFoundError:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(PROJECT_ROOT))
    from pipeline.config import DEFAULT_CONFIG_ROOT, ConfigError, SourceConfig, SourceRegistry
    from pipeline.file_reader.base_reader import BaseFileReader
    from pipeline.processor.base_processor import BaseProcessor
    from pipeline.quality import (
        ERROR,
        WARN,
        QualityChecksError,
        QualityChecksFailed,
        blocking_failures,
        failure_summary,
        java_regex_problems,
        results_dataframe,
        run_quality_checks,
        short_error,
    )
    from storage.parquet_lake_writer import LakeRootError, ParquetLakeWriter, resolve_lake_root
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


def clean_for_staging(data: DataFrame) -> DataFrame:
    """Minimal, deterministic cleanup of the staging zone (P2-2): drop the
    rows whose columns are all null, then exact duplicates. Nothing else:
    business transformations belong to P2-4. The quality checks (P2-3) run
    on the result.

    Runs on the business columns only, before the writer adds the technical
    ones: `lake_run_id` and `lake_source_file` are never null, so a fully
    empty row would never be dropped once they are there.
    """
    return BaseProcessor(data).remove_empty_rows().remove_duplicates().df


def write_to_lake(
    writer: ParquetLakeWriter,
    data: DataFrame,
    config: SourceConfig,
    *,
    run_id: str,
    source_file: str,
    file_id: int,
) -> dict[str, int]:
    """Write one file read to the raw zone, then to the staging zone if its
    source declares one, with the config's `storage.mode`. Returns the
    number of rows written per zone (`quality`: one row per check).

    `file_id` (file_list.id) is stored with the rows: it orders the
    snapshots of a file across listings (see `latest_snapshot`).

    P2-3. First, when the source has a staging zone, the `quality/<table>`
    partition of this (run, file) is removed, whether the source declares
    `quality` or not (C-6): results of a previous attempt never outlive this
    one. Then, when the source declares `quality` (which requires a staging
    zone), the checks run on the data cleaned for staging and their results
    are recorded in `quality/<table>` (same partitioning), then:
    - a blocking check (severity `error`, status `fail` or `error`): the
      staging partition of this (run, file) is removed (a previous attempt
      may have written it), staging is not written, and `QualityChecksFailed`
      is raised (the caller marks the file `failed` with its summary). Raw
      stays written: the trace of the file received. If recording the
      results fails too, the summary is kept, with `results not recorded:
      <cause>`;
    - `warn` checks only (status `warn`, or `error` when they could not
      run): logged (ERROR level for `error`), recorded, staging written;
    - the engine itself raising (C-1): fail closed, the staging partition of
      this (run, file) is removed before `QualityChecksError` (short
      message) is raised.

    Caching: the read is persisted (raw, then staging); the cleaned rows too
    when checks run. The read is released as soon as the checks have
    materialized the cleaned rows (C-10); everything is released at the end,
    error or not.

    Any write error propagates: the caller marks the file `failed`. When raw
    succeeded and staging failed, a retry rewrites raw idempotently; the
    quality results recorded by this attempt are removed then (no `pass`
    without data).
    """
    staging_target = config.staging_target
    partition = {
        "mode": config.storage.mode,
        "run_id": run_id,
        "source_file": source_file,
        "file_id": file_id,
    }
    spark = data.sparkSession
    if staging_target is not None:
        writer.delete_partition(
            spark, config.quality_zone_target, run_id=run_id, source_file=source_file
        )
    persisted: list[DataFrame] = []
    if staging_target is not None:
        # Two writes from the same read: keep it rather than reading and
        # parsing the source file a second time.
        data = data.persist()
        persisted.append(data)
    try:
        written = {"raw": writer.write(data, config.raw_target, **partition)}
        if staging_target is not None:
            staged = clean_for_staging(data)
            if config.quality is not None:
                # The checks (one aggregation, one groupBy per `unique`) and
                # the staging write all read the cleaned rows: cleaned once.
                staged = staged.persist()
                persisted.append(staged)
                results = _run_checks(writer, staged, config, partition, staging_target)
                # C-10: the checks' aggregation materialized `staged`; the
                # read is no longer needed for the staging write.
                data.unpersist()
                written["quality"] = _record_results(
                    writer, staged, config, partition, staging_target, results
                )
            try:
                written["staging"] = writer.write(staged, staging_target, **partition)
            except Exception:
                if config.quality is not None:
                    _forget_results(writer, spark, config, partition)
                raise
    finally:
        for frame in reversed(persisted):
            frame.unpersist()
    return written


def _run_checks(
    writer: ParquetLakeWriter,
    staged: DataFrame,
    config: SourceConfig,
    partition: dict,
    staging_target: str,
) -> list:
    """`run_quality_checks`, closed by default (C-1): if it raises, the
    staging partition of this (run, file) is removed before
    `QualityChecksError` is raised, with a one-line message."""
    try:
        return run_quality_checks(staged, config.quality.checks)
    except Exception as exc:  # noqa: BLE001 - anything unexpected blocks staging
        message = f"quality checks could not run: {short_error(exc)}"
        try:
            removed = writer.delete_partition(
                staged.sparkSession,
                staging_target,
                run_id=partition["run_id"],
                source_file=partition["source_file"],
            )
            outcome = ", previous staging partition removed" if removed else ""
        except Exception as delete_exc:  # noqa: BLE001
            message += f" (previous staging partition not removed: {short_error(delete_exc)})"
            outcome = ""
        log.error("%s: %s. Staging not written%s.", partition["source_file"], message, outcome)
        raise QualityChecksError(message) from exc


def _record_results(
    writer: ParquetLakeWriter,
    staged: DataFrame,
    config: SourceConfig,
    partition: dict,
    staging_target: str,
    results: list,
) -> int:
    """Log and record the results; raise `QualityChecksFailed` (after
    removing this file's staging partition) on a blocking check. Returns
    the number of result rows written."""
    run_id, source_file = partition["run_id"], partition["source_file"]
    for result in results:
        if result.status == WARN:
            log.warning("quality warning on %s: %s: %s", source_file, result.name, result.message)
        elif result.status == ERROR and not result.blocking:
            log.error(
                "quality check %s could not run on %s (severity warn, staging still written): %s",
                result.name,
                source_file,
                result.message,
            )

    failures = blocking_failures(results)
    notes: list[str] = []
    if failures:
        # First, the invariant that matters most: no stale staging partition
        # left as a valid state, even if recording the results fails next.
        try:
            removed = writer.delete_partition(
                staged.sparkSession, staging_target, run_id=run_id, source_file=source_file
            )
        except Exception as exc:  # noqa: BLE001 - keep the quality summary
            notes.append(f"previous staging partition not removed: {short_error(exc)}")
            removed = False
        log.error(
            "%s: %s. Staging not written%s.",
            source_file,
            failure_summary(results),
            ", previous staging partition removed" if removed else "",
        )

    try:
        recorded = writer.write(
            results_dataframe(staged.sparkSession, results, source_name=config.source.name),
            config.quality_target,
            **partition,
        )
    except Exception as exc:
        if not failures:
            raise
        # C-6: the file fails on its checks; that summary stays the message.
        notes.append(f"results not recorded: {short_error(exc)}")
        raise QualityChecksFailed(results, notes=notes) from exc
    if failures:
        raise QualityChecksFailed(results, notes=notes)
    return recorded


def _forget_results(
    writer: ParquetLakeWriter, spark, config: SourceConfig, partition: dict
) -> None:
    """Remove the results this attempt recorded when its staging write
    failed (C-6): no `pass` left without staging data. Best effort: the
    staging write error is the one that matters."""
    try:
        writer.delete_partition(
            spark,
            config.quality_target,
            run_id=partition["run_id"],
            source_file=partition["source_file"],
        )
    except Exception as exc:  # noqa: BLE001
        log.error(
            "%s: staging write failed and its quality results could not be removed: %s",
            partition["source_file"],
            short_error(exc),
        )


def _fail_all(file_list, error_message: str) -> None:
    for file in file_list:
        update_file_status(file["id"], "failed", error_message=error_message)


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
        _fail_all(file_list, error_message)
        sys.exit(EXIT_INVALID_CONFIG)

    # P2-2: where to write, resolved before Spark too (deterministic error).
    try:
        writer = ParquetLakeWriter(resolve_lake_root())
    except LakeRootError as exc:
        error_message = f"invalid lake root: {exc}"
        log.error(
            "%s\nNo file of run_id=%r was read; every row is marked failed.", error_message, run_id
        )
        _fail_all(file_list, error_message)
        sys.exit(EXIT_INVALID_CONFIG)
    log.info("Writing to lake root %s", writer.lake_root)

    # P2-2: partitions are keyed by (run id, file name). Two files of the run
    # with the same name (two source directories) would overwrite each other:
    # refuse them rather than keep one silently.
    paths_by_name: dict[str, list[str]] = {}
    for file in file_list:
        paths_by_name.setdefault(_source_file_name(file["file_path"]), []).append(file["file_path"])

    spark = get_spark_session("ReadFilesJob")
    try:
        # P2-3 / C-8: Python validated the regexes at load, but some it
        # accepts are refused by Java (`a{`), which `rlike` uses: compile
        # them all now, before the first file, as part of the config.
        regex_problems = java_regex_problems(
            spark, [(loaded.path, loaded.config) for loaded in registry.sources + registry.defaults]
        )
        if regex_problems:
            error_message = "invalid reading config: " + "\n".join(regex_problems)
            log.error(
                "%s\nNo file of run_id=%r was read; every row is marked failed.",
                error_message,
                run_id,
            )
            _fail_all(file_list, error_message)
            sys.exit(EXIT_INVALID_CONFIG)

        reader = BaseFileReader(spark, registry=registry)

        success_count = 0
        fail_count = 0
        for file in file_list:
            file_id = file["id"]
            file_path = file["file_path"]
            source_file = _source_file_name(file_path)
            if len(paths_by_name[source_file]) > 1:
                error_message = (
                    f"file name {source_file!r} listed {len(paths_by_name[source_file])} times in "
                    f"run_id={run_id!r} ({', '.join(paths_by_name[source_file])}): their lake "
                    "partitions (lake_run_id, lake_source_file) would overwrite each other"
                )
                log.error(error_message)
                update_file_status(file_id, "failed", error_message=error_message)
                fail_count += 1
                continue
            try:
                # C-8: resolved once, used for the read and for the writes.
                config = reader.get_config(file_path)
                content = reader.read_file(file_path, config=config)
                # C-5: a row left `processed` by a previous attempt must not
                # stay so while its partitions are being replaced.
                update_file_status(file_id, "pending", error_message=None)
                written = write_to_lake(
                    writer,
                    content,
                    config,
                    run_id=run_id,
                    source_file=source_file,
                    file_id=file_id,
                )
                log.info(
                    "%s (id=%s): %s",
                    file_path,
                    file_id,
                    ", ".join(f"{zone}={rows} rows" for zone, rows in written.items()),
                )
                update_file_status(file_id, "processed")
                success_count += 1
            except (QualityChecksFailed, QualityChecksError) as e:
                # Already logged by write_to_lake, with the staging outcome.
                update_file_status(file_id, "failed", error_message=str(e))
                fail_count += 1
            except Exception as e:
                log.error("Error processing %s: %s", file_path, e)
                update_file_status(file_id, "failed", error_message=str(e))
                fail_count += 1
    finally:
        spark.stop()

    log.info("%d/%d files processed successfully.", success_count, len(file_list))
    sys.exit(1 if fail_count > 0 else 0)


def _source_file_name(file_path: str) -> str:
    """`lake_source_file` partition value: the file name, as the reading
    config matching uses it (`SourceRegistry.match`)."""
    return os.path.basename(str(file_path).rstrip("/\\"))


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
