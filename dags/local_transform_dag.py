"""
DAG: transform_dag
Builds the dbt project of transform/ (DuckDB on top of the lake's staging
zone), rendered by Astronomer Cosmos (P2-4).

Second stage of the pipeline: `ingest__<source>` -> `lake_staging__<source>`
-> transform_dag -> `dbt_marts`.

- Schedule (P5-3): any of the `lake_staging__<source>` Assets, one per source with a
  staging zone, emitted by its `ingest__<source>` DAG when at least one file of the
  run is `processed` (`AssetAny`: dbt rebuilds the whole staging zone, whichever
  source changed). The names come from the valid configs of the config root at each
  parse; with no such source the DAG is manual. `max_active_runs=1` and
  `batch_asset_events=True` (P3-5: Airflow 3.4 creates one run per event by
  default): the events that arrive during a run are consumed together by the
  next one. An event emitted while the DAG is paused is not queued and no
  run consumes it (the next event rebuilds everything). Each run rebuilds the
  current state of the whole staging zone (`stg_*__current`, same rule as
  `latest_snapshot`), so it is idempotent and does not need the source run id.
- Rendering: `LoadMode.DBT_MANIFEST`, from `transform/target/manifest.json`
  produced by `make dbt-parse` (CI does it before pytest). The DAG processor
  never runs dbt. `RenderConfig.dbt_executable_path` is deliberately not
  set: in Cosmos 1.16 it goes through `is_dbt_installed_in_same_environment`,
  which wrongly answers True without dbt (`find_spec` returns None instead
  of raising) and then refuses a dbt from another venv.
  Missing manifest: explicit import error for THIS file (FileNotFoundError
  naming `make dbt-parse`), visible under "DAG Import Errors"; the other DAG
  files are parsed independently and are not affected. A silently missing
  DAG would let `lake_staging` events go unnoticed.
- Execution: `ExecutionMode.WATCHER`. One producer task runs a single
  `dbt build` (transform/.venv/bin/dbt, a separate uv project: dbt is not
  installed next to Airflow); one sensor per model / test reports its
  status. Model sensors are deferrable: a **triggerer** is required
  (`airflow standalone` runs one).
- DuckDB has a single writer: every dbt task runs in the `dbt_duckdb` pool
  (1 slot, deploy/airflow/pools.json), consumers included, because a
  consumer's retry falls back to running `dbt run` for its model itself.
  Deferred sensors do not hold the slot (`include_deferred: false`), so the
  producer is never blocked by them. `threads: 1` in profiles.yml.
  Consequence: the producer holds the only slot for the whole `dbt build`,
  so the sensors mostly run after it, one by one, instead of following it
  live. That is the price of a safe replay on a single writer.
- The rendered graph must match the project: a manifest older than a model,
  macro, test or dbt_project.yml is an import error too (`make dbt-parse`).
- `dag.test()` cannot run this DAG reliably. Cosmos 1.16 only wires the
  producer upstream of root nodes without dependencies, and a source counts
  as one, so no model sensor is downstream of the producer. `dag.test()` then
  runs its schedulable task instances in `set` order, which follows
  `PYTHONHASHSEED`. When a sensor comes first, its inline trigger polls
  forever for a producer that `dag.test()` never starts (seen: seeds 1
  and 3 hang, seeds 0 and 2 succeed). A real scheduler with a triggerer is
  unaffected, because the producer runs alongside.
- Assets: Cosmos' own Assets are disabled (`emit_datasets=False`: their URI
  embeds the absolute path of the DuckDB file). `publish_dbt_marts` emits
  the stable `dbt_marts` Asset once every dbt task succeeded.
- Environment of dbt (`ProjectConfig.env_vars`, added to the worker's):
  `LAKE_ROOT` (default `<project root>/lake`, the Spark job's default) and
  `DBT_DUCKDB_PATH` (default `<project root>/transform/warehouse.duckdb`).
  Read when the file is parsed, i.e. by the worker itself when it runs a
  task (Airflow 3 workers parse the DAG file). dbt refuses a relative value
  of either (on-run-start hook, transform/macros/lake.sql).

- Lineage (P2-5): the Cosmos producer reports, through the OpenLineage
  provider, the datasets of its `dbt build` (openlineage-integration-common
  reads dbt's artefacts). dbt names the staging zone by its DuckDB relation
  (`duckdb://<DBT_DUCKDB_PATH>`, `<database>.lake_staging.<table>`), Spark by
  its directory (`file`, `<LAKE_ROOT>/staging/<table>`): the graph would stop
  at the lake. `lineage_lake_staging` (LakeStagingLineageOperator) emits the
  link between the two names, one pair per `lake_staging` source of the
  manifest whose staging directory exists (local lake). The link is
  structural, not a proof that Spark wrote the table. It runs nothing, has
  no upstream or downstream task, and its lineage never fails it.

No project import here (pipeline/, utils/, storage/ pull pyspark and are not
on the DAG processor's path, see test_p1_4_dags_parse_without_project_root_on_sys_path).
"""

import json
import logging
import os
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from airflow.sdk import Asset, AssetAny, AssetTriggeredTimetable, BaseOperator, task
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import (
    PRODUCER_WATCHER_TASK_ID,
    ExecutionMode,
    InvocationMode,
    LoadMode,
    TestBehavior,
)

from utils.failure_alert import log_dag_run_failure, log_task_failure
from utils.lake_lineage import spark_dataset  # réexporté

DBT_MARTS_ASSET = Asset(name="dbt_marts")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TRANSFORM_DIR = PROJECT_ROOT / "transform"
DBT_MANIFEST = TRANSFORM_DIR / "target" / "manifest.json"
DBT_EXECUTABLE = TRANSFORM_DIR / ".venv" / "bin" / "dbt"
DEFAULT_LAKE_ROOT = PROJECT_ROOT / "lake"
DEFAULT_DBT_DUCKDB_PATH = TRANSFORM_DIR / "warehouse.duckdb"

# DuckDB: one writer per database file. Must exist (`make airflow-init`):
# with a missing pool the scheduler leaves the tasks in `scheduled`.
DBT_POOL = "dbt_duckdb"

# P2-5: the dbt source (models/staging/generated/_lake_staging__sources.yml) reading
# `<LAKE_ROOT>/<STAGING_ZONE>/<identifier>/`, written by the Spark job
# (`storage.staging_path: staging/` of config/sources/*.yaml).
LAKE_STAGING_SOURCE = "lake_staging"
STAGING_ZONE = "staging"
LINEAGE_TASK_ID = "lineage_lake_staging"


# P3-3: failure reports, one structured `airflow_failure` log line (utils/failure_alert.py).
# P5-3: imported at the top, like the project code this file reads at parse time
# (the Asset names come from the configs): a worker without the project could not
# parse it, so the minimal-line fallback this file used to carry is gone.
on_task_failure = log_task_failure
on_dag_run_failure = log_dag_run_failure

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # Consumers: a retry runs dbt for their model alone. The producer never
    # reruns `dbt build` on a retry (Cosmos skips it on purpose).
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
}

log = logging.getLogger(__name__)


def lake_staging_schedule() -> AssetTriggeredTimetable | None:
    """P5-3: the DAG runs when any `lake_staging__<source>` is emitted. One Asset per
    source of the config root with a `staging_path`, in the valid configs (an invalid
    one has its own `invalid_source__*` DAG and no Asset here). None, i.e. manual,
    when there is none.

    P3-5: the events queued during a run are consumed together by ONE next run: each
    run rebuilds the whole staging zone, one `dbt build` per event would repeat the
    same work. Explicit, because Airflow 3.4 creates one run per event by default.
    """
    from pipeline.config import SourceRegistry, default_config_root

    registry, _ = SourceRegistry.load_valid(default_config_root())
    assets = [
        Asset(name=loaded.config.staging_asset_name)
        for loaded in registry.sources
        if loaded.config.staging_asset_name
    ]
    if not assets:
        return None
    return AssetTriggeredTimetable(AssetAny(*assets), batch_asset_events=True)


def dbt_env(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Environment variables given to dbt, with the documented defaults.

    Empty or blank values count as unset, and `~` is expanded, as in
    resolve_lake_root (dbt does not expand it). Values are not validated
    here: dbt refuses invalid ones itself, for every caller (Makefile or
    Airflow).
    """
    env = os.environ if environ is None else environ

    def value(name: str, default: Path) -> str:
        raw = (env.get(name) or "").strip()
        return os.path.expanduser(raw) if raw else str(default)

    return {
        "LAKE_ROOT": value("LAKE_ROOT", DEFAULT_LAKE_ROOT),
        "DBT_DUCKDB_PATH": value("DBT_DUCKDB_PATH", DEFAULT_DBT_DUCKDB_PATH),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "False",
    }


# openlineage-spark 1.55.0, FilesystemDatasetUtils: object stores matched by
# scheme prefix, with their canonical scheme (s3a/s3n -> s3, abfs -> abfss,
# wasb -> wasbs). Everything else but `file` is generic.
def duckdb_database(path: str) -> str:
    """Database name dbt-duckdb 1.11 gives the file it opens
    (DuckDBCredentials.path_derived_database_name): the first non-empty
    dot-separated part of the file name (`warehouse.prod.duckdb` ->
    `warehouse`, `.hidden.duckdb` -> `hidden`), `memory` for `:memory:`."""
    if path == ":memory:":
        return "memory"
    base = os.path.basename(urlsplit(path).path)
    return base.lstrip(".").split(".")[0]


def lake_root_location(value: str) -> str:
    """The lake root as the Spark job resolves it (resolve_lake_root /
    normalize_spark_path, not imported here: they pull pyspark): a local
    path is expanded and resolved (symlinks), a URI is kept as is. Only the
    root is resolved; zones and tables are then joined as text."""
    value = value.strip()
    if urlsplit(value).scheme and len(urlsplit(value).scheme) > 1:
        return value.rstrip("/")
    return str(Path(value).expanduser().resolve())


def lake_staging_lineage(
    manifest: Mapping[str, Any], environ: Mapping[str, str]
) -> list[tuple[tuple[str, str], tuple[str, str]]]:
    """(Spark dataset, dbt dataset) of every `lake_staging` source of the dbt
    manifest, in manifest order.

    - Spark: the table directory `<LAKE_ROOT>/staging/<identifier>` (see
      spark_dataset, lake_root_location);
    - dbt: what openlineage-integration-common names a dbt-duckdb source:
      namespace `duckdb://<DBT_DUCKDB_PATH>`, name
      `<database>.<schema>.<identifier>`, the database being the one
      dbt-duckdb derives from the file it opens at run time (not the one the
      manifest was parsed with).

    The link is structural (this dbt source reads that directory), not a
    proof that Spark wrote it: for a local lake, only the staging tables
    whose directory exists are linked. A remote lake is not listed.

    `environ` is dbt's environment (dbt_env()): `LAKE_ROOT`, `DBT_DUCKDB_PATH`.
    """
    lake_root = lake_root_location(environ["LAKE_ROOT"])
    warehouse = environ["DBT_DUCKDB_PATH"]
    database = duckdb_database(warehouse)
    pairs = []
    for source in manifest.get("sources", {}).values():
        if source.get("source_name") != LAKE_STAGING_SOURCE:
            continue
        identifier = source.get("identifier") or source["name"]
        spark = spark_dataset(f"{lake_root}/{STAGING_ZONE}/{identifier}")
        if spark[0] == "file" and not Path(spark[1]).is_dir():
            continue
        dbt = (f"duckdb://{warehouse}", f"{database}.{source['schema']}.{identifier}")
        pairs.append((spark, dbt))
    return pairs


class LakeStagingLineageOperator(BaseOperator):
    """OpenLineage link between the Spark staging tables and the dbt sources
    that read them (P2-5). Runs nothing: its lineage is all it carries.

    Inputs: the Spark names of the staging tables; outputs: the dbt names of
    the `lake_staging` sources (see lake_staging_lineage: structural link,
    local staging directories that exist only). Computed from the manifest
    and dbt's environment when the provider asks (start and end of the
    task), never raising: an unreadable manifest gives an empty lineage and
    a warning.
    """

    ui_color = "#e8f4ea"

    def __init__(
        self, *, manifest_path: str, dbt_environment: Mapping[str, str], **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.manifest_path = str(manifest_path)
        self.dbt_environment = dict(dbt_environment)

    def execute(self, context: Any) -> None:
        log.info(
            "OpenLineage link between the Spark staging zone and the dbt `%s` sources "
            "(nothing to run)",
            LAKE_STAGING_SOURCE,
        )

    def _lineage(self) -> Any:
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.openlineage.extractors import OperatorLineage

        try:
            manifest = json.loads(Path(self.manifest_path).read_text(encoding="utf-8"))
            pairs = lake_staging_lineage(manifest, self.dbt_environment)
        except Exception:
            log.warning(
                "OpenLineage: no lake_staging link, cannot read %s (ignored)",
                self.manifest_path,
                exc_info=True,
            )
            return OperatorLineage()
        return OperatorLineage(
            inputs=[Dataset(namespace=ns, name=name) for (ns, name), _ in pairs],
            outputs=[Dataset(namespace=ns, name=name) for _, (ns, name) in pairs],
        )

    def get_openlineage_facets_on_start(self) -> Any:
        return self._lineage()

    def get_openlineage_facets_on_complete(self, task_instance: Any) -> Any:
        return self._lineage()


def _newest_dbt_source() -> tuple[float, Path] | None:
    """(mtime, path) of the most recently modified file the manifest is
    built from: models, macros, data tests and dbt_project.yml. A few `stat`
    calls, no read."""
    candidates = [TRANSFORM_DIR / "dbt_project.yml"]
    for folder in ("models", "macros", "data_tests"):
        for root, dirs, files in os.walk(TRANSFORM_DIR / folder):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            candidates += [Path(root) / f for f in files if not f.startswith(".")]
    stats = [(path.stat().st_mtime, path) for path in candidates if path.is_file()]
    return max(stats) if stats else None


if not DBT_MANIFEST.is_file():
    raise FileNotFoundError(
        f"dbt manifest not found: {DBT_MANIFEST}. transform_dag is rendered from it "
        "(LoadMode.DBT_MANIFEST): run `make dbt-parse` (after `make transform-sync`)."
    )
# C-4: a stale manifest renders a graph the producer's `dbt build` (which
# re-parses the project) no longer matches: nodes publish_dbt_marts would not
# wait for, or sensors waiting for nodes that no longer exist.
_newest = _newest_dbt_source()
if _newest is not None and _newest[0] > DBT_MANIFEST.stat().st_mtime:
    raise RuntimeError(
        f"dbt manifest {DBT_MANIFEST} is older than "
        f"{_newest[1].relative_to(TRANSFORM_DIR)}: transform_dag would not match the "
        "project dbt builds. Run `make dbt-parse`."
    )

transform_dag = DbtDag(
    dag_id="transform_dag",
    description="dbt build (DuckDB) of the lake's staging zone, rendered by Cosmos",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2023, 1, 1),
    schedule=lake_staging_schedule(),
    catchup=False,
    max_active_runs=1,
    # A stuck producer or sensor must not block the next `lake_staging`
    # events forever (max_active_runs=1).
    dagrun_timeout=timedelta(hours=2),
    # P3-3: also covers what no task callback sees, such as that timeout.
    on_failure_callback=on_dag_run_failure,
    tags=["transform", "dbt", "duckdb"],
    project_config=ProjectConfig(
        dbt_project_path=TRANSFORM_DIR,
        manifest_path=DBT_MANIFEST,
        env_vars=dbt_env(),
        # No dbt package: never run `dbt deps`. (DuckDB extensions are never
        # auto-installed either: profiles.yml.)
        install_dbt_deps=False,
    ),
    profile_config=ProfileConfig(
        profile_name="lake_transform",
        target_name="dev",
        profiles_yml_filepath=TRANSFORM_DIR / "profiles.yml",
    ),
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.WATCHER,
        # dbt lives in its own venv: a subprocess, never the in-process dbtRunner.
        invocation_mode=InvocationMode.SUBPROCESS,
        dbt_executable_path=str(DBT_EXECUTABLE),
    ),
    render_config=RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        test_behavior=TestBehavior.AFTER_EACH,
        emit_datasets=False,
    ),
    operator_args={"pool": DBT_POOL},
)

with transform_dag:
    # P2-5: no upstream or downstream task (it never waits for dbt nor delays
    # it), outside the DuckDB pool (it never opens the warehouse).
    LakeStagingLineageOperator(
        task_id=LINEAGE_TASK_ID,
        manifest_path=str(DBT_MANIFEST),
        dbt_environment=dbt_env(),
    )

    @task(outlets=[DBT_MARTS_ASSET])
    def publish_dbt_marts() -> None:
        """Emit `dbt_marts`: runs only when every dbt model and test task
        succeeded (default trigger rule all_success)."""
        log.info("dbt build succeeded: emitting the `dbt_marts` asset")

    # Downstream of every leaf dbt task (models and tests), hence of every
    # dbt task, but not of the producer: Cosmos skips a producer retry by
    # design, while the consumers can still succeed through their own retry.
    _leaves = [
        t
        for t in transform_dag.tasks
        if not t.downstream_task_ids
        and t.task_id not in (PRODUCER_WATCHER_TASK_ID, LINEAGE_TASK_ID)
    ]
    _leaves >> publish_dbt_marts()
