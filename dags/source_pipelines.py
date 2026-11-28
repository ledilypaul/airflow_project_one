"""
DAG factory (P5-3): one DAG `ingest__<source>` per YAML of `config/sources/`, plus
`ingest__unmatched` for the files no source claims.

    list_files -> insert_files -> spark_read_files -> publish_lake_staging
                                                   \\-> fail_if_spark_failed

    (`polars_read_files` and `fail_if_polars_failed` for a source `engine: polars`)

- `list_files`: the files of `listing.directories` (default: `default_directories`
  of `pipeline.yaml`) whose name matches `source.patterns`, minus `listing.exclude`.
  A missing directory fails the task: it is a deployment problem, not an empty
  listing.
- `insert_files`: one `pending` row per file in `file_list`, with this DAG run's id
  and the source name (`file_list.source_name`: two source DAGs can have the same
  run id). Skipped, with everything downstream, when nothing was listed or recorded
  (no red run, no useless retry). Returns the arguments of the Spark job.
- `spark_read_files`: `ReadFilesSparkSubmitOperator` (utils/read_files_operator.py),
  one application per run, in the `spark` pool. The job reads the rows of
  (run id, source name) and writes the lake. For a source `engine: polars` (P5-8),
  `polars_read_files` runs the Polars job in a subprocess instead, in the `polars`
  pool, with the same exit codes and the same retry rule (utils/polars_task.py).
  `ingest__unmatched` uses the engine default (`PIPELINE_DEFAULT_ENGINE`).
- `publish_lake_staging` (sources with a `staging_path` only): emits the Asset
  `lake_staging__<source>` when at least one file of the run is `processed`
  (`all_done`: one file failing for good must not block dbt forever). `transform_dag`
  is scheduled on the union of those Assets. `fail_if_spark_failed` (`one_failed`)
  keeps the run `failed` when Spark failed: a DagRun takes its state from its leaves.
- `ingest__unmatched` lists what matches no source and a default per extension
  (`config/defaults/`), so it only writes the raw zone: no Asset.

A source that cannot be built gives a DAG `invalid_source__<file name>` with one task
that fails with the validation message, visible in the UI; the other DAGs load
normally. Without that, one invalid YAML would raise at parse time and remove every
DAG of this file. (`AirflowDagImportErrors` does not see it, hence this DAG; CI
validates every config anyway.) Parsing stays pure: no database, no network, no
PySpark (tests/unit/dags/test_p5_3_dag_factory.py).

`schedule` is the source's own (cron, preset or null = manual); `max_active_runs=1`,
so a listing never overlaps the previous run of the same source. Not used: the YAML
DAG importer of Airflow 3.4, experimental (docs/tech-watch/2026-11-24-p5-3-yaml-dags.md).

The project must be importable at parse time (editable install, or PYTHONPATH in the
image): the registry is loaded here.
"""

import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.sdk import DAG, Asset, TriggerRule, task

from pipeline.config import SourceConfig, SourceRegistry, default_config_root, default_directories
from pipeline.config.engine import default_engine
from pipeline.config.models import UNMATCHED_SOURCE
from utils.failure_alert import log_dag_run_failure, log_task_failure
from utils.polars_task import POLARS_POOL, POLARS_TASK_ID, run_polars_job
from utils.read_files_operator import (
    JOB_PATH,
    SPARK_CONN_ID,
    SPARK_POOL,
    ReadFilesSparkSubmitOperator,
    build_application_args,
    job_env_vars,
    openlineage_spark_settings,
    prepare_spark_lineage,
)

log = logging.getLogger(__name__)

DEFAULT_RETRIES = 1
START_DATE = datetime(2024, 1, 1)
_INVALID_PREFIX = "invalid_source__"


def _default_args(retries: int) -> dict[str, Any]:
    return {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=5),
        # P3-3: one `airflow_failure` line per failed task (utils/failure_alert.py).
        "on_failure_callback": log_task_failure,
    }


def insert_listed_files(file_list: list[str], source_name: str) -> list[str]:
    """Record the listed files in `file_list` for the current run and return the
    arguments of the Spark job (`--run-id`, `--source-name`).

    Raises:
        AirflowSkipException: nothing listed, or every listed file vanished before
            it could be recorded (no row, so nothing for the job to read).
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.sdk import get_current_context
    from airflow.sdk.exceptions import AirflowSkipException

    from utils.file_list_lineage import report_file_list_lineage
    from utils.functions_utils import FILE_LIST_POSTGRES_CONN_ID, insert_into_file_list

    # "Nothing to process" is not an error: skip (no red run, no useless retry).
    if not file_list:
        raise AirflowSkipException("No files found in source directories, nothing to insert")

    run_id = get_current_context()["dag_run"].run_id

    # One engine for the whole task, reused for every file (M3).
    hook = PostgresHook(postgres_conn_id=FILE_LIST_POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # Files handled without error: inserted, or already present for this run (a
    # retry: ON CONFLICT DO NOTHING raises nothing). Not a SQL rowcount, on purpose.
    recorded: list[str] = []
    for filepath in file_list:
        try:
            insert_into_file_list(
                [
                    os.path.basename(filepath),
                    filepath,
                    datetime.fromtimestamp(os.path.getmtime(filepath)),
                ],
                dag_run_id=run_id,
                engine=engine,
                source_name=source_name,
            )
            recorded.append(filepath)
        except FileNotFoundError:
            log.warning("File not found, skipping: %s", filepath)
        except Exception as e:
            log.error("Insertion error for %s: %s", filepath, e)
            raise

    if not recorded:
        raise AirflowSkipException(
            f"0/{len(file_list)} listed files could be recorded (all vanished), nothing to process"
        )
    # P2-5: OpenLineage cannot see SQL sent through the engine: declare the recorded
    # files (inputs) and file_list (output). Never raises.
    report_file_list_lineage(hook, role="output", source_files=recorded)
    log.info("%d/%d files recorded", len(recorded), len(file_list))
    return build_application_args(run_id, source_name)


def count_run_processed_files(run_id: str, source_name: str) -> int:
    """Number of `processed` files of this run and source: what `publish_lake_staging`
    decides on."""
    from utils import functions_utils

    return functions_utils.count_processed_files([run_id], source_name=source_name)


def _add_read_tasks(
    dag: DAG,
    listed: Any,
    source_name: str,
    asset_name: str | None,
    spark_settings: dict[str, Any],
    *,
    engine: str,
    targets: list[str],
) -> None:
    """`insert_files` -> the read task of `engine` -> publish / guard, after the
    `listed` task. `targets`: the lake tables the job writes (Polars lineage)."""

    @task(task_id="insert_files")
    def insert_files(file_list: list[str]) -> list[str]:
        return insert_listed_files(file_list, source_name)

    if engine == "polars":

        @task(task_id=POLARS_TASK_ID, pool=POLARS_POOL)
        def polars_read_files(application_args: list[str], source_files: list[str]) -> None:
            from airflow.sdk import get_current_context

            run_polars_job(
                application_args,
                source_files=source_files,
                targets=targets,
                task=get_current_context()["task"],
            )

        read_files = polars_read_files(insert_files(listed), listed)
    else:
        read_files = _spark_read_files(insert_files(listed), spark_settings)
    _add_guard_and_publish(read_files, engine, source_name, asset_name)


def _spark_read_files(application_args: Any, spark_settings: dict[str, Any]) -> Any:
    return ReadFilesSparkSubmitOperator(
        task_id="spark_read_files",
        application=str(JOB_PATH),
        application_args=application_args,
        conn_id=SPARK_CONN_ID,
        pool=SPARK_POOL,
        # Added to the worker's environment in local/client mode (PATH, JAVA_HOME,
        # SPARK_HOME, ENV are kept); `spark.*.env` conf on YARN / Kubernetes. Never a
        # secret here: the hook logs them in clear (C-6).
        env_vars=job_env_vars(),
        # P2-5: OpenLineage listener, transport and parent job info (or nothing at
        # all when lineage is off), from the environment.
        **spark_settings,
        pre_execute=prepare_spark_lineage,
    )


def _add_guard_and_publish(
    read_files: Any, engine: str, source_name: str, asset_name: str | None
) -> None:
    # on_failure_callback=None: the failed read task has already reported
    # itself, and the DAG-level callback reports the run (P3-3).
    @task(
        task_id=f"fail_if_{engine}_failed",
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=0,
        on_failure_callback=None,
    )
    def fail_if_read_failed() -> None:
        from airflow.sdk.exceptions import AirflowFailException

        raise AirflowFailException(
            f"at least one {engine}_read_files instance failed or did not run: the run fails "
            "(`lake_staging__<source>` was emitted only if some file was processed)"
        )

    read_files >> fail_if_read_failed()
    if asset_name is None:
        return

    @task(
        task_id="publish_lake_staging",
        outlets=[Asset(name=asset_name)],
        trigger_rule=TriggerRule.ALL_DONE,
    )
    def publish_lake_staging(run_id: str | None = None) -> int:
        from airflow.sdk.exceptions import AirflowSkipException

        processed = count_run_processed_files(str(run_id), source_name)
        if processed == 0:
            raise AirflowSkipException(
                f"no file_list row is `processed` for run_id={run_id!r}, source {source_name!r}: "
                f"nothing new in the lake, `{asset_name}` not emitted"
            )
        log.info("%d processed file(s): emitting the `%s` asset", processed, asset_name)
        return processed

    read_files >> publish_lake_staging()


def _new_dag(
    dag_id: str, description: str, *, schedule: str | None, retries: int, tags: list[str]
) -> DAG:
    return DAG(
        dag_id=dag_id,
        description=description,
        default_args=_default_args(retries),
        start_date=START_DATE,
        schedule=schedule,
        catchup=False,
        # A listing never overlaps the previous run of its source.
        max_active_runs=1,
        # A missing `spark` pool leaves the task `scheduled` silently, and with
        # max_active_runs=1 every later run of the source would pile up behind it.
        dagrun_timeout=timedelta(hours=6),
        # P3-3: also covers what no task callback sees, such as that timeout.
        on_failure_callback=log_dag_run_failure,
        tags=list(dict.fromkeys(tags)),
        # The DAGs are returned and put in this module's globals. Autoregistration (any
        # DAG used in a `with` block) would also publish one that failed half-built:
        # the DAG the invalid-source DAG stands in for.
        auto_register=False,
    )


def build_source_dag(
    config: SourceConfig, default_dirs: list[str], spark_settings: dict[str, Any]
) -> DAG:
    """The DAG `ingest__<source.name>` of one validated source config."""
    from pipeline.extractor.extractor import list_source_files

    name = config.source.name
    directories = config.listing.directories or default_dirs
    patterns, exclude = config.source.patterns, config.listing.exclude
    dag = _new_dag(
        f"ingest__{name}",
        f"Lists, reads and writes to the lake the files of the source {name!r}",
        schedule=config.schedule,
        retries=DEFAULT_RETRIES if config.retries is None else config.retries,
        tags=["ingest", *config.tags],
    )
    with dag:

        @task(task_id="list_files")
        def list_files() -> list[str]:
            files = list_source_files(directories, patterns, exclude)
            log.info("Found %d file(s) for source %r in %s", len(files), name, directories)
            return files

        targets = [config.raw_target, config.staging_target]
        if config.quality is not None:
            targets.append(config.quality_target)
        _add_read_tasks(
            dag,
            list_files(),
            name,
            config.staging_asset_name,
            spark_settings,
            engine=config.engine,
            targets=[t for t in targets if t is not None],
        )
    # Airflow checks the schedule (`99 99 * * *`) here, not when the DAG is created:
    # raised later, by the DagBag, it would be an import error of the whole file.
    dag.validate()
    return dag


def build_unmatched_dag(
    registry: SourceRegistry, default_dirs: list[str], spark_settings: dict[str, Any]
) -> DAG:
    """`ingest__unmatched`: the files no dedicated source claims, read with the
    default config of their extension (raw zone only)."""
    from pipeline.extractor.extractor import list_unmatched_files

    source_patterns = [p for loaded in registry.sources for p in loaded.config.source.patterns]
    default_patterns = [p for loaded in registry.defaults for p in loaded.config.source.patterns]
    dag = _new_dag(
        f"ingest__{UNMATCHED_SOURCE}",
        "Lists, reads and writes to the raw zone the files that match no source",
        schedule=None,
        retries=DEFAULT_RETRIES,
        tags=["ingest", UNMATCHED_SOURCE],
    )
    with dag:

        @task(task_id="list_files")
        def list_files() -> list[str]:
            files = list_unmatched_files(default_dirs, source_patterns, default_patterns)
            log.info("Found %d file(s) matching no source in %s", len(files), default_dirs)
            return files

        _add_read_tasks(
            dag,
            list_files(),
            UNMATCHED_SOURCE,
            None,
            spark_settings,
            engine=default_engine(),
            targets=[loaded.config.raw_target for loaded in registry.defaults],
        )
    return dag


def build_invalid_dag(dag_id: str, message: str) -> DAG:
    """The DAG `dag_id` (`invalid_source__<file name>`): one task that fails with `message`."""
    dag = _new_dag(
        dag_id,
        "A source config that does not validate: see the log of its task",
        schedule=None,
        retries=0,
        tags=["invalid-source"],
    )
    with dag:

        @task(task_id="report_invalid_config", retries=0)
        def report_invalid_config() -> None:
            from airflow.sdk.exceptions import AirflowFailException

            raise AirflowFailException(message)

        report_invalid_config()
    return dag


def build_dags(config_root: Path | str | None = None) -> dict[str, DAG]:
    """Every DAG of a config root (`PIPELINE_CONFIG_ROOT` or `config/` by default),
    by `dag_id`: one per valid source, `ingest__unmatched`, and an
    `invalid_source__<file>` per config that is invalid or cannot be built.

    Raises:
        ConfigError: root or `pipeline.yaml` unusable (a global misconfiguration: the
            import error is the right signal).
    """
    root = Path(config_root) if config_root is not None else default_config_root()
    registry, invalid = SourceRegistry.load_valid(root)
    default_dirs = default_directories(root)
    spark_settings = openlineage_spark_settings()

    dags: dict[str, DAG] = {}
    for loaded in registry.sources:
        try:
            dag = build_source_dag(loaded.config, default_dirs, spark_settings)
            dags[dag.dag_id] = dag
        except Exception as exc:  # one source never takes the others down
            log.exception("source %s: DAG not built", loaded.path)
            invalid[loaded.path] = f"{type(exc).__name__}: {exc}"
    unmatched = build_unmatched_dag(registry, default_dirs, spark_settings)
    dags[unmatched.dag_id] = unmatched

    for path, message in invalid.items():
        slug = _INVALID_PREFIX + re.sub(r"[^a-z0-9_]", "_", path.stem.lower())
        dag_id, n = slug, 1
        while dag_id in dags:  # same file name in sources/ and defaults/, or after cleaning
            n += 1
            dag_id = f"{slug}_{n}"
        dags[dag_id] = build_invalid_dag(dag_id, f"{path}\n{message}")
    return dags


globals().update(build_dags())
