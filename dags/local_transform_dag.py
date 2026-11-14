"""
DAG: transform_dag
Builds the dbt project of transform/ (DuckDB on top of the lake's staging
zone), rendered by Astronomer Cosmos (P2-4).

Third stage of the pipeline: list_files_dag -> `file_list` -> read_file_dag
-> `lake_staging` -> transform_dag -> `dbt_marts`.

- Schedule: the `lake_staging` Asset, emitted by read_file_dag once every
  Spark application of its run succeeded. `max_active_runs=1`; events that
  arrive during a run are consumed by the next one. Each run rebuilds the
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

No project import here (pipeline/, utils/, storage/ pull pyspark and are not
on the DAG processor's path, see test_p1_4_dags_parse_without_project_root_on_sys_path).
"""

import logging
import os
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, task
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import (
    PRODUCER_WATCHER_TASK_ID,
    ExecutionMode,
    InvocationMode,
    LoadMode,
    TestBehavior,
)

# Same names as in read_file_dag (Airflow matches Assets by name).
LAKE_STAGING_ASSET = Asset(name="lake_staging")
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

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # Consumers: a retry runs dbt for their model alone. The producer never
    # reruns `dbt build` on a retry (Cosmos skips it on purpose).
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

log = logging.getLogger(__name__)


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
    schedule=[LAKE_STAGING_ASSET],
    catchup=False,
    max_active_runs=1,
    # A stuck producer or sensor must not block the next `lake_staging`
    # events forever (max_active_runs=1).
    dagrun_timeout=timedelta(hours=2),
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
        if not t.downstream_task_ids and t.task_id != PRODUCER_WATCHER_TASK_ID
    ]
    _leaves >> publish_dbt_marts()
