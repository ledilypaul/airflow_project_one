"""Engine-independent orchestration of the ingestion job (P5-6): reads
`file_list`, resolves the configs, drives an `Engine`, writes the statuses and
computes the exit code. The contract (zones, statuses, exit codes 0 to 6) is
described in `pipeline/jobs/read_files_job.py`, the Spark entry point.

Imports neither an engine nor Airflow.
"""

import logging
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import NoReturn

from pipeline.config import ConfigError, SourceConfig, SourceRegistry
from pipeline.engine.base import Engine, Frame
from pipeline.quality.results import (
    ERROR,
    WARN,
    QualityChecksError,
    QualityChecksFailed,
    blocking_failures,
    failure_summary,
    short_error,
)
from storage.lake_root import LakeRootError, resolve_lake_root
from utils.functions_utils import list_file_from_db, update_file_status

log = logging.getLogger(__name__)

EXIT_FILES_FAILED = 1
EXIT_INVALID_RUN_ID = 2
EXIT_NO_ROWS = 3
EXIT_PARENT_GONE = 4
EXIT_INVALID_CONFIG = 5
EXIT_QUALITY_FAILED = 6


def write_to_lake(
    engine: Engine,
    data: Frame,
    config: SourceConfig,
    *,
    run_id: str,
    source_file: str,
    file_id: int,
) -> dict[str, int]:
    """Write one file read to the raw zone, then to the staging zone if its
    source declares one, with the config's `storage.mode`. Returns the
    number of rows written per zone (`quality`: one row per check).

    `file_id` (file_list.id) is stored with the rows: it sorts the
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
    if staging_target is not None:
        engine.delete_partition(config.quality_zone_target, run_id=run_id, source_file=source_file)
    persisted: list[Frame] = []
    if staging_target is not None:
        # Two writes from the same read: keep it rather than reading and
        # parsing the source file a second time.
        data = engine.persist(data)
        persisted.append(data)
    try:
        written = {"raw": engine.write_lake(data, config.raw_target, **partition)}
        if staging_target is not None:
            staged = engine.clean_for_staging(data)
            if config.quality is not None:
                # The checks (one aggregation, one groupBy per `unique`) and
                # the staging write all read the cleaned rows: cleaned once.
                staged = engine.persist(staged)
                persisted.append(staged)
                results = _run_checks(engine, staged, config, partition, staging_target)
                # C-10: the checks' aggregation materialized `staged`; the
                # read is no longer needed for the staging write.
                engine.unpersist(data)
                written["quality"] = _record_results(
                    engine, config, partition, staging_target, results
                )
            try:
                written["staging"] = engine.write_lake(staged, staging_target, **partition)
            except Exception:
                if config.quality is not None:
                    _forget_results(engine, config, partition)
                raise
    finally:
        for frame in reversed(persisted):
            engine.unpersist(frame)
    return written


def _run_checks(
    engine: Engine,
    staged: Frame,
    config: SourceConfig,
    partition: dict,
    staging_target: str,
) -> list:
    """`Engine.run_checks`, closed by default (C-1): if it raises, the
    staging partition of this (run, file) is removed before
    `QualityChecksError` is raised, with a one-line message."""
    try:
        return engine.run_checks(staged, config.quality.checks)
    except Exception as exc:  # noqa: BLE001 - anything unexpected blocks staging
        message = f"quality checks could not run: {short_error(exc)}"
        try:
            removed = engine.delete_partition(
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
    engine: Engine,
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
            removed = engine.delete_partition(
                staging_target, run_id=run_id, source_file=source_file
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
        recorded = engine.write_results(
            results, config.quality_target, source_name=config.source.name, **partition
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


def _forget_results(engine: Engine, config: SourceConfig, partition: dict) -> None:
    """Remove the results this attempt recorded when its staging write
    failed (C-6): no `pass` left without staging data. Best effort: the
    staging write error is the one that matters."""
    try:
        engine.delete_partition(
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


def final_exit_code(*, failed: int, deterministic_quality: int) -> int:
    """Exit code of a run that processed its files (P2-6).

    `failed`: number of files marked `failed`; `deterministic_quality`: how
    many of them failed only on blocking quality checks with status `fail`
    (`QualityChecksFailed.deterministic`). 0 when nothing failed,
    `EXIT_QUALITY_FAILED` when every failure is such a quality failure,
    `EXIT_FILES_FAILED` otherwise (one failure of another kind is enough).
    """
    if failed < 0 or deterministic_quality < 0 or deterministic_quality > failed:
        raise ValueError(
            f"inconsistent failure counts: failed={failed}, "
            f"deterministic_quality={deterministic_quality}"
        )
    if failed == 0:
        return 0
    if deterministic_quality == failed:
        return EXIT_QUALITY_FAILED
    return EXIT_FILES_FAILED


def _fail_all(file_list, error_message: str) -> None:
    for file in file_list:
        update_file_status(file["id"], "failed", error_message=error_message)


def run(
    run_id: str,
    source_name: str | None = None,
    *,
    config_root: Path,
    create_engine: Callable[[SourceRegistry, str], Engine],
) -> NoReturn:
    """Process the files of `run_id` and exit with the job's code (module
    docstring). `create_engine(registry, lake_root)` is only called once the
    rows, the configs and the lake root are valid: it starts the engine."""
    if run_id is None or run_id == "" or run_id == "None":
        log.error(
            "Missing or invalid run_id (%r): refusing to query file_list without a run_id "
            "to scope to (idempotence, P1-3). Pass a real --run-id.",
            run_id,
        )
        sys.exit(EXIT_INVALID_RUN_ID)

    file_list = list_file_from_db(run_id, source_name=source_name)
    if not file_list:
        # P1-4 / C-3: a source run that emitted the `file_list` asset always
        # has at least one row (empty listings and "every file vanished" both
        # skip the insertion task, so no event). Zero rows is an anomaly
        # (typo in a manual run_id, purged rows...), not a green no-op.
        log.error(
            "no file_list rows for run_id=%r (source %r): typo in the run_id or purged data? "
            "Nothing was processed.",
            run_id,
            source_name,
        )
        sys.exit(EXIT_NO_ROWS)

    # P2-1 / C-2: load and validate every reading config before starting
    # Spark. An invalid config is a deterministic error for the whole run, not
    # a file error: mark every row `failed` with it and exit with a dedicated
    # code, instead of leaving them `pending` behind an uncaught exception.
    try:
        registry = SourceRegistry.from_directory(config_root)
    except ConfigError as exc:
        error_message = f"invalid reading config: {exc}"
        log.error(
            "%s\nNo file of run_id=%r was read; every row is marked failed.", error_message, run_id
        )
        _fail_all(file_list, error_message)
        sys.exit(EXIT_INVALID_CONFIG)

    # P2-2: where to write, resolved before Spark too (deterministic error).
    try:
        lake_root = resolve_lake_root()
    except LakeRootError as exc:
        error_message = f"invalid lake root: {exc}"
        log.error(
            "%s\nNo file of run_id=%r was read; every row is marked failed.", error_message, run_id
        )
        _fail_all(file_list, error_message)
        sys.exit(EXIT_INVALID_CONFIG)
    log.info("Writing to lake root %s", lake_root)

    # P2-2: partitions are keyed by (run id, file name). Two files of the run
    # with the same name (two source directories) would overwrite each other:
    # refuse them rather than keep one silently.
    paths_by_name: dict[str, list[str]] = {}
    for file in file_list:
        paths_by_name.setdefault(_source_file_name(file["file_path"]), []).append(file["file_path"])

    engine = create_engine(registry, lake_root)
    try:
        # P2-3 / C-8: Python validated the regexes at load, but some it
        # accepts are refused by Java (`a{`), which `rlike` uses: compile
        # them all now, before the first file, as part of the config.
        regex_problems = engine.config_problems(registry)
        if regex_problems:
            error_message = "invalid reading config: " + "\n".join(regex_problems)
            log.error(
                "%s\nNo file of run_id=%r was read; every row is marked failed.",
                error_message,
                run_id,
            )
            _fail_all(file_list, error_message)
            sys.exit(EXIT_INVALID_CONFIG)

        success_count = 0
        fail_count = 0
        # P2-6: failures a retry cannot change (blocking checks, status fail).
        quality_fail_count = 0
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
                config = engine.get_config(file_path)
                content = engine.read(file_path, config)
                # C-5: a row left `processed` by a previous attempt must not
                # stay so while its partitions are being replaced.
                update_file_status(file_id, "pending", error_message=None)
                written = write_to_lake(
                    engine,
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
                if isinstance(e, QualityChecksFailed) and e.deterministic:
                    quality_fail_count += 1
            except Exception as e:
                log.error("Error processing %s: %s", file_path, e)
                update_file_status(file_id, "failed", error_message=str(e))
                fail_count += 1
    finally:
        engine.close()

    log.info("%d/%d files processed successfully.", success_count, len(file_list))
    code = final_exit_code(failed=fail_count, deterministic_quality=quality_fail_count)
    if code == EXIT_QUALITY_FAILED:
        log.error(
            "%d file(s) failed, all on blocking quality checks only: exit code %d "
            "(deterministic, not retried by the ingest DAG). Fix the data or the checks, "
            "then re-run the run id.",
            fail_count,
            code,
        )
    elif code == EXIT_FILES_FAILED and quality_fail_count:
        log.error(
            "%d file(s) failed, %d of them on quality checks only: exit code %d "
            "(retryable, the other failures may be transient).",
            fail_count,
            quality_fail_count,
            code,
        )
    sys.exit(code)


def _source_file_name(file_path: str) -> str:
    """`lake_source_file` partition value: the file name, as the reading
    config matching uses it (`SourceRegistry.match`)."""
    return os.path.basename(str(file_path).rstrip("/\\"))
