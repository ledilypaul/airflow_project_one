"""
P2-4 — dbt transformation layer orchestrated by Cosmos.

Covers:
  - `ingest__<source>` emits its `lake_staging__<source>` Asset from a final task
    that runs once the Spark task is done and only when a file was processed;
  - transform_dag (dags/local_transform_dag.py): scheduled on any
    `lake_staging__<source>` (P5-3),
    `max_active_runs=1`, no catchup, WATCHER execution with the dbt of
    transform/.venv (subprocess), manifest rendering without
    `RenderConfig.dbt_executable_path` (Cosmos 1.16 bug), every dbt task in
    the 1-slot `dbt_duckdb` pool, no Cosmos-generated Asset, and the stable
    `dbt_marts` Asset emitted after every dbt task;
  - the dbt environment (LAKE_ROOT, DBT_DUCKDB_PATH) and its defaults;
  - a missing manifest is an import error of that file only;
  - the dbt staging layer: generated, see tests/unit/test_p5_4_dbt_codegen.py.

The manifest (transform/target/manifest.json) comes from `make dbt-parse`,
which CI runs before pytest.
"""

import ast
import json
import shutil
from pathlib import Path

import pytest
import yaml
from airflow.models import DagBag
from airflow.sdk import Asset

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / "dags"
TRANSFORM_DAG_FILE = DAGS_FOLDER / "local_transform_dag.py"
TRANSFORM_DIR = PROJECT_ROOT / "transform"
MANIFEST = TRANSFORM_DIR / "target" / "manifest.json"
POOLS_FILE = PROJECT_ROOT / "deploy" / "airflow" / "pools.json"
SOURCES_YML = TRANSFORM_DIR / "models" / "staging" / "generated" / "_lake_staging__sources.yml"

PRODUCER = "dbt_producer_watcher"
PUBLISH = "publish_dbt_marts"
# P2-5: carries the OpenLineage link Spark staging -> dbt sources; not a dbt task.
LINEAGE = "lineage_lake_staging"


@pytest.fixture(scope="module")
def dagbag():
    if not MANIFEST.is_file():
        pytest.fail(f"{MANIFEST} missing: run `make dbt-parse` before the DAG tests")
    return DagBag(dag_folder=str(DAGS_FOLDER))


@pytest.fixture(scope="module")
def transform_dag(dagbag):
    assert not dagbag.import_errors, dagbag.import_errors
    return dagbag.dags["transform_dag"]


def _dbt_tasks(dag):
    return [t for t in dag.tasks if t.task_id not in (PUBLISH, LINEAGE)]


# ---------------------------------------------------------------------------
# ingest__<source> -> lake_staging__<source>
# ---------------------------------------------------------------------------

# Sources of examples/config with a staging zone (the unmatched DAG has none).
STAGED_SOURCES = ["imdb_movies", "orders"]


@pytest.mark.parametrize("source", STAGED_SOURCES)
def test_p2_4_c2_ingest_dag_publishes_lake_staging_once_spark_is_done(dagbag, source):
    """C-2: `all_done`, not `all_success`: one permanently failing file must
    not block `lake_staging__<source>` (hence dbt) forever. Whether to emit is
    decided from file_list (tests below), not from the Spark task state."""
    dag = dagbag.dags[f"ingest__{source}"]
    publish = dag.get_task("publish_lake_staging")

    assert [type(a) for a in publish.outlets] == [Asset]
    assert [a.name for a in publish.outlets] == [f"lake_staging__{source}"]
    assert publish.upstream_task_ids == {"spark_read_files"}
    assert publish.trigger_rule == "all_done"
    # Only that final task updates the Asset.
    for other in dag.tasks:
        if other.task_id != "publish_lake_staging":
            assert other.outlets == [], other.task_id


def test_p5_3_unmatched_dag_publishes_nothing(dagbag):
    """It reads with the defaults per extension, which have no staging zone."""
    dag = dagbag.dags["ingest__unmatched"]
    assert "publish_lake_staging" not in dag.task_ids
    assert all(t.outlets == [] for t in dag.tasks)


def test_p2_4_c2_failed_spark_task_still_fails_the_run(dagbag):
    """A DagRun takes its state from its leaves: an `all_done` leaf that
    succeeds would turn a run with a failed spark_read_files green.
    `fail_if_spark_failed` (`one_failed`, also a leaf) fails it again; it is
    skipped when the Spark task succeeded. Checked for real by
    tests/integration/test_p1_4_ingest_dag_e2e.py."""
    dag = dagbag.dags["ingest__orders"]
    guard = dag.get_task("fail_if_spark_failed")

    assert guard.trigger_rule == "one_failed"
    assert guard.upstream_task_ids == {"spark_read_files"}
    assert guard.retries == 0
    assert guard.outlets == []
    assert {t.task_id for t in dag.leaves} == {"publish_lake_staging", "fail_if_spark_failed"}


def _publish_callable():
    from dags.source_pipelines import build_dags

    return build_dags()["ingest__orders"].get_task("publish_lake_staging").python_callable


def test_p2_4_c2_publish_emits_when_at_least_one_file_was_processed(monkeypatch):
    import utils.functions_utils as functions_utils

    asked = []

    def fake_count(run_ids, source_name=None):
        asked.append((list(run_ids), source_name))
        return 1

    monkeypatch.setattr(functions_utils, "count_processed_files", fake_count)

    assert _publish_callable()(run_id="run_a") == 1
    # P5-3: the run id of this DAG run, and the source (run ids of two source DAGs
    # can be equal).
    assert asked == [(["run_a"], "orders")]


def test_p2_4_c2_publish_skips_when_no_file_was_processed(monkeypatch):
    from airflow.sdk.exceptions import AirflowSkipException

    import utils.functions_utils as functions_utils

    monkeypatch.setattr(functions_utils, "count_processed_files", lambda run_ids, source_name: 0)

    with pytest.raises(AirflowSkipException, match="no file_list row is `processed`"):
        _publish_callable()(run_id="run_a")


def test_p2_4_c2_guard_fails_explicitly():
    from airflow.sdk.exceptions import AirflowFailException

    from dags.source_pipelines import build_dags

    guard = build_dags()["ingest__orders"].get_task("fail_if_spark_failed")
    with pytest.raises(AirflowFailException, match="spark_read_files"):
        guard.python_callable()


def test_p2_4_lake_staging_is_what_transform_dag_waits_for(dagbag, transform_dag):
    """P5-3: the union of the Assets the ingest DAGs emit, in an `AssetAny` that
    keeps `batch_asset_events=True` (P3-5: one `dbt build` for the events queued)."""
    from airflow.sdk import AssetAny
    from airflow.sdk.definitions.timetables.assets import AssetTriggeredTimetable

    produced = [
        dagbag.dags[f"ingest__{name}"].get_task("publish_lake_staging").outlets[0]
        for name in STAGED_SOURCES
    ]
    assert isinstance(transform_dag.timetable, AssetTriggeredTimetable)
    assert transform_dag.timetable.batch_asset_events is True
    condition = transform_dag.timetable.asset_condition
    assert isinstance(condition, AssetAny)
    assert sorted(a.name for a in condition.objects) == sorted(a.name for a in produced)
    # No URI scheme a provider would try to validate.
    assert all("://" not in a.uri for a in produced)


# ---------------------------------------------------------------------------
# transform_dag
# ---------------------------------------------------------------------------


def test_p2_4_transform_dag_run_policy(transform_dag):
    """DuckDB has one writer: never two runs at once, no backfill."""
    assert transform_dag.max_active_runs == 1
    assert transform_dag.catchup is False
    assert transform_dag.dagrun_timeout is not None


def test_p2_4_transform_dag_has_one_task_pair_per_model(transform_dag):
    """Rendered from the manifest: a run + test task per dbt model, plus the
    watcher producer, the publishing task and (P2-5) the lineage link."""
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    models = {
        node["name"]
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model" and node["package_name"] == "lake_transform"
    }
    assert models == {
        "stg_movies__current",
        "stg_orders__current",
        "mart_movies__by_release_year_language",
    }
    expected = {f"{m}.{kind}" for m in models for kind in ("run", "test")}
    assert set(transform_dag.task_ids) == expected | {PRODUCER, PUBLISH, LINEAGE}


def test_p2_4_transform_dag_uses_watcher_mode_with_the_transform_venv(transform_dag):
    from cosmos.constants import InvocationMode
    from cosmos.operators.watcher import DbtConsumerWatcherSensor, DbtProducerWatcherOperator

    producer = transform_dag.get_task(PRODUCER)
    assert isinstance(producer, DbtProducerWatcherOperator)
    assert producer.dbt_executable_path == str(TRANSFORM_DIR / ".venv" / "bin" / "dbt")
    assert producer.invocation_mode == InvocationMode.SUBPROCESS
    assert Path(producer.project_dir) == TRANSFORM_DIR
    for consumer in _dbt_tasks(transform_dag):
        if consumer.task_id != PRODUCER:
            assert isinstance(consumer, DbtConsumerWatcherSensor), consumer.task_id
            assert consumer.producer_task_id == PRODUCER


def test_p2_4_render_config_has_no_dbt_executable_path():
    """Cosmos 1.16: RenderConfig.dbt_executable_path goes through the broken
    `is_dbt_installed_in_same_environment` check. The DAG is rendered from
    the manifest, so it must not be set (checked on the source: the
    RenderConfig object is not kept on the DAG)."""
    tree = ast.parse(TRANSFORM_DAG_FILE.read_text(encoding="utf-8"))
    calls = {
        node.func.id: node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    render_kwargs = {kw.arg: kw.value for kw in calls["RenderConfig"].keywords}
    assert "dbt_executable_path" not in render_kwargs
    assert ast.unparse(render_kwargs["load_method"]) == "LoadMode.DBT_MANIFEST"
    assert ast.unparse(render_kwargs["emit_datasets"]) == "False"
    execution_kwargs = {kw.arg for kw in calls["ExecutionConfig"].keywords}
    assert "dbt_executable_path" in execution_kwargs


def test_p2_4_every_dbt_task_runs_in_the_duckdb_pool(transform_dag):
    """The producer and the consumers (whose retry runs dbt itself) share the
    1-slot pool: at most one dbt process writes the DuckDB file."""
    from dags.local_transform_dag import DBT_POOL

    assert DBT_POOL == "dbt_duckdb"
    for dbt_task in _dbt_tasks(transform_dag):
        assert dbt_task.pool == DBT_POOL, dbt_task.task_id
    assert transform_dag.get_task(PUBLISH).pool != DBT_POOL


def test_p2_4_pools_file_declares_the_duckdb_pool():
    """One slot, and deferred sensors do not hold it: otherwise a waiting
    model sensor would block the producer it waits for."""
    pool = json.loads(POOLS_FILE.read_text(encoding="utf-8"))["dbt_duckdb"]
    assert pool["slots"] == 1
    assert pool["include_deferred"] is False
    assert pool["description"]


def test_p2_4_cosmos_assets_are_disabled_and_dbt_marts_is_published(transform_dag):
    """Cosmos' Assets embed the absolute path of the DuckDB file: disabled.
    The stable `dbt_marts` Asset is emitted once, after every dbt task."""
    for dbt_task in _dbt_tasks(transform_dag):
        assert dbt_task.outlets == [], dbt_task.task_id
        assert dbt_task.emit_datasets is False, dbt_task.task_id

    publish = transform_dag.get_task(PUBLISH)
    assert [a.name for a in publish.outlets] == ["dbt_marts"]
    assert "://" not in publish.outlets[0].uri
    assert publish.trigger_rule == "all_success"
    # Downstream of every dbt task (through the leaves) except the producer.
    ancestors = {t.task_id for t in publish.get_flat_relatives(upstream=True)}
    assert ancestors == {t.task_id for t in _dbt_tasks(transform_dag)} - {PRODUCER}


def test_p2_4_dbt_tasks_get_the_lake_environment(transform_dag):
    from dags.local_transform_dag import dbt_env

    for dbt_task in _dbt_tasks(transform_dag):
        assert dbt_task.env == dbt_env(), dbt_task.task_id
        # Added to the worker environment, not substituted for it.
        assert dbt_task.append_env is True, dbt_task.task_id


def test_p2_4_dbt_env_defaults_match_the_spark_job():
    from dags.local_transform_dag import dbt_env

    defaults = dbt_env({})
    # Same default lake as storage.parquet_lake_writer.DEFAULT_LAKE_ROOT,
    # computed without importing it (pyspark) in the DAG processor.
    assert defaults["LAKE_ROOT"] == str(PROJECT_ROOT / "lake")
    assert defaults["DBT_DUCKDB_PATH"] == str(TRANSFORM_DIR / "warehouse.duckdb")
    assert defaults["DBT_SEND_ANONYMOUS_USAGE_STATS"] == "False"
    assert dbt_env({"LAKE_ROOT": "  ", "DBT_DUCKDB_PATH": ""}) == defaults

    env = dbt_env({"LAKE_ROOT": " file:///data/lake ", "DBT_DUCKDB_PATH": "/data/w.duckdb"})
    assert env["LAKE_ROOT"] == "file:///data/lake"
    assert env["DBT_DUCKDB_PATH"] == "/data/w.duckdb"


def test_p2_4_dbt_env_default_lake_is_the_writer_default():
    from dags.local_transform_dag import dbt_env
    from storage.parquet_lake_writer import DEFAULT_LAKE_ROOT

    assert dbt_env({})["LAKE_ROOT"] == str(DEFAULT_LAKE_ROOT)


def test_p2_4_missing_manifest_is_an_import_error_of_that_file_only(tmp_path):
    """Explicit failure, visible under "DAG Import Errors", instead of a DAG
    that silently disappears; the other DAG files still load. The copied
    dags/ folder resolves the project root to tmp_path, where no manifest
    exists."""
    dags_copy = tmp_path / "dags"
    shutil.copytree(DAGS_FOLDER, dags_copy, ignore=shutil.ignore_patterns("__pycache__"))

    bag = DagBag(dag_folder=str(dags_copy))

    assert set(bag.import_errors) == {str(dags_copy / TRANSFORM_DAG_FILE.name)}
    (message,) = bag.import_errors.values()
    assert "dbt manifest not found" in message
    assert "make dbt-parse" in message
    assert {"ingest__orders", "ingest__unmatched"} <= set(bag.dag_ids)
    assert "transform_dag" not in bag.dag_ids


def _copy_project(tmp_path: Path) -> Path:
    """dags/ and the dbt project (manifest included, no venv) under tmp_path,
    which the copied DAG file then takes as its project root."""
    shutil.copytree(DAGS_FOLDER, tmp_path / "dags", ignore=shutil.ignore_patterns("__pycache__"))
    transform = tmp_path / "transform"
    for name in ("models", "macros", "data_tests"):
        shutil.copytree(TRANSFORM_DIR / name, transform / name)
    for name in ("dbt_project.yml", "profiles.yml"):
        shutil.copy2(TRANSFORM_DIR / name, transform / name)
    (transform / "target").mkdir()
    shutil.copy(MANIFEST, transform / "target" / "manifest.json")
    return tmp_path / "dags"


def test_p2_4_c4_stale_manifest_is_an_import_error(dagbag, tmp_path):
    """C-4: a manifest older than a model, macro, test or dbt_project.yml
    renders a graph the producer's `dbt build` no longer matches (nodes that
    publish_dbt_marts would not wait for). Explicit import error instead."""
    import os

    dags_copy = _copy_project(tmp_path)
    assert DagBag(dag_folder=str(dags_copy)).import_errors == {}  # fresh copy: loads

    edited = tmp_path / "transform" / "models" / "marts"
    edited /= "mart_movies__by_release_year_language.sql"
    manifest = tmp_path / "transform" / "target" / "manifest.json"
    later = manifest.stat().st_mtime + 60  # edited after the last `dbt parse`
    os.utime(edited, (later, later))

    bag = DagBag(dag_folder=str(dags_copy))

    assert set(bag.import_errors) == {str(dags_copy / TRANSFORM_DAG_FILE.name)}
    (message,) = bag.import_errors.values()
    assert "older than" in message
    assert "mart_movies__by_release_year_language.sql" in message
    assert "make dbt-parse" in message
    assert "ingest__orders" in bag.dag_ids


def test_p2_4_c4_every_manifest_node_is_waited_for(transform_dag):
    """Every model of the manifest has its `.run` task, and every test is
    reported by the `.test` task of its model (tests_per_model of the
    producer): nothing the producer builds escapes publish_dbt_marts."""
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    run_tasks = {
        t.model_unique_id: t.task_id for t in transform_dag.tasks if t.task_id.endswith(".run")
    }
    test_tasks = {t.model_unique_id for t in transform_dag.tasks if t.task_id.endswith(".test")}
    tests_per_model = transform_dag.get_task(PRODUCER).tests_per_model
    reported_tests = {test for tests in tests_per_model.values() for test in tests}

    for unique_id, node in manifest["nodes"].items():
        if node["package_name"] != "lake_transform":
            continue
        if node["resource_type"] == "model":
            assert unique_id in run_tasks, unique_id
        elif node["resource_type"] == "test":
            assert unique_id in reported_tests, unique_id
    assert set(tests_per_model) <= test_tasks


# ---------------------------------------------------------------------------
# dbt profile and environment
# ---------------------------------------------------------------------------


def test_p2_4_dbt_profile_reads_paths_from_the_environment():
    """No path in the repo: DuckDB file from DBT_DUCKDB_PATH, one thread."""
    profiles = yaml.safe_load((TRANSFORM_DIR / "profiles.yml").read_text(encoding="utf-8"))
    output = profiles["lake_transform"]["outputs"]["dev"]
    assert output["type"] == "duckdb"
    assert output["path"] == "{{ env_var('DBT_DUCKDB_PATH') }}"
    assert output["threads"] == 1


def test_p2_4_c5_duckdb_extensions_are_never_installed_or_loaded_implicitly():
    """C-5: no extension downloaded behind our back (the real setting is
    checked by tests/integration/test_p2_4_dbt_build.py)."""
    profiles = yaml.safe_load((TRANSFORM_DIR / "profiles.yml").read_text(encoding="utf-8"))
    output = profiles["lake_transform"]["outputs"]["dev"]
    assert output["settings"] == {
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
    }
    assert "extensions" not in output


def test_p2_4_c7_dbt_env_expands_the_home_directory():
    """C-7: like the Spark job (resolve_lake_root), `~` is expanded; dbt
    itself would refuse it."""
    from dags.local_transform_dag import dbt_env

    env = dbt_env({"LAKE_ROOT": "~/lake", "DBT_DUCKDB_PATH": "~/w.duckdb"})
    assert env["LAKE_ROOT"] == str(Path.home() / "lake")
    assert env["DBT_DUCKDB_PATH"] == str(Path.home() / "w.duckdb")
    # URIs are left alone.
    assert dbt_env({"LAKE_ROOT": "file:/data/lake"})["LAKE_ROOT"] == "file:/data/lake"
