"""
P2-4 — dbt transformation layer orchestrated by Cosmos.

Covers:
  - read_file_dag emits the `lake_staging` Asset from a final task that only
    runs when every mapped `spark_read_files` instance succeeded;
  - transform_dag (dags/local_transform_dag.py): scheduled on `lake_staging`,
    `max_active_runs=1`, no catchup, WATCHER execution with the dbt of
    transform/.venv (subprocess), manifest rendering without
    `RenderConfig.dbt_executable_path` (Cosmos 1.16 bug), every dbt task in
    the 1-slot `dbt_duckdb` pool, no Cosmos-generated Asset, and the stable
    `dbt_marts` Asset emitted after every dbt task;
  - the dbt environment (LAKE_ROOT, DBT_DUCKDB_PATH) and its defaults;
  - a missing manifest is an import error of that file only;
  - the dbt project matches the lake: sources = staging tables of the
    reading configs, movies columns = imdb.yaml `columns_type`, orders
    columns = orders.yaml `columns_type` (P2-6), both with an enforced
    contract.

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
SOURCES_YML = TRANSFORM_DIR / "models" / "staging" / "_lake_staging__sources.yml"

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
# read_file_dag -> lake_staging
# ---------------------------------------------------------------------------


def test_p2_4_c2_read_file_dag_publishes_lake_staging_once_spark_is_done(dagbag):
    """C-2: `all_done`, not `all_success`: one permanently failing file must
    not block `lake_staging` (hence dbt) forever. Whether to emit is decided
    from file_list (test below), not from the Spark task states."""
    dag = dagbag.dags["read_file_dag"]
    publish = dag.get_task("publish_lake_staging")

    assert [type(a) for a in publish.outlets] == [Asset]
    assert [a.name for a in publish.outlets] == ["lake_staging"]
    assert publish.upstream_task_ids == {"spark_read_files"}
    assert publish.trigger_rule == "all_done"
    # Only that final task updates the Asset.
    for other in dag.tasks:
        if other.task_id != "publish_lake_staging":
            assert other.outlets == [], other.task_id


def test_p2_4_c2_failed_spark_task_still_fails_the_run(dagbag):
    """A DagRun takes its state from its leaves: an `all_done` leaf that
    succeeds would turn a run with a failed spark_read_files green.
    `fail_if_spark_failed` (`one_failed`, also a leaf) fails it again; it is
    skipped when every Spark instance succeeded. Checked for real by
    tests/integration/test_p1_4_read_file_dag_e2e.py."""
    dag = dagbag.dags["read_file_dag"]
    guard = dag.get_task("fail_if_spark_failed")

    assert guard.trigger_rule == "one_failed"
    assert guard.upstream_task_ids == {"spark_read_files"}
    assert guard.retries == 0
    assert guard.outlets == []
    assert {t.task_id for t in dag.leaves} == {"publish_lake_staging", "fail_if_spark_failed"}


def _publish_callable():
    from dags.local_read_file_dag import dag_instance

    return dag_instance.get_task("publish_lake_staging").python_callable


class _FakeTI:
    def __init__(self, run_ids):
        self.run_ids = run_ids
        self.pulled = []

    def xcom_pull(self, task_ids):
        self.pulled.append(task_ids)
        return self.run_ids


def test_p2_4_c2_publish_emits_when_at_least_one_file_was_processed(monkeypatch):
    import utils.functions_utils as functions_utils

    asked = []

    def fake_count(run_ids):
        asked.append(list(run_ids))
        return 1

    monkeypatch.setattr(functions_utils, "count_processed_files", fake_count)
    ti = _FakeTI(["run_a", "run_b"])

    assert _publish_callable()(ti=ti) == 1
    assert ti.pulled == ["resolve_source_run_ids"]
    assert asked == [["run_a", "run_b"]]


def test_p2_4_c2_publish_skips_when_no_file_was_processed(monkeypatch):
    from airflow.sdk.exceptions import AirflowSkipException

    import utils.functions_utils as functions_utils

    monkeypatch.setattr(functions_utils, "count_processed_files", lambda run_ids: 0)

    with pytest.raises(AirflowSkipException, match="no file_list row is `processed`"):
        _publish_callable()(ti=_FakeTI(["run_a"]))


@pytest.mark.parametrize("run_ids", [None, []], ids=["resolve_failed", "empty"])
def test_p2_4_c2_publish_skips_without_resolved_run_ids(monkeypatch, run_ids):
    """resolve_source_run_ids failed (no XCom): nothing to look up, nothing
    emitted, and file_list is not queried."""
    from airflow.sdk.exceptions import AirflowSkipException

    import utils.functions_utils as functions_utils

    def must_not_be_called(run_ids):
        raise AssertionError("file_list must not be queried")

    monkeypatch.setattr(functions_utils, "count_processed_files", must_not_be_called)

    with pytest.raises(AirflowSkipException, match="no source run id"):
        _publish_callable()(ti=_FakeTI(run_ids))


def test_p2_4_c2_guard_fails_explicitly():
    from airflow.sdk.exceptions import AirflowFailException

    from dags.local_read_file_dag import dag_instance

    with pytest.raises(AirflowFailException, match="spark_read_files"):
        dag_instance.get_task("fail_if_spark_failed").python_callable()


def test_p2_4_lake_staging_is_what_transform_dag_waits_for(dagbag, transform_dag):
    from airflow.sdk.definitions.timetables.assets import AssetTriggeredTimetable

    produced = dagbag.dags["read_file_dag"].get_task("publish_lake_staging").outlets
    assert isinstance(transform_dag.timetable, AssetTriggeredTimetable)
    # P3-5: the condition is the Asset itself (AssetTriggeredTimetable with
    # an explicit batch_asset_events).
    assert [transform_dag.timetable.asset_condition] == list(produced)
    # No URI scheme a provider would try to validate.
    assert "://" not in produced[0].uri


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
    assert {"list_files_dag", "list_files_dag_classic", "read_file_dag"} <= set(bag.dag_ids)
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
    assert "read_file_dag" in bag.dag_ids


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
# dbt project <-> lake
# ---------------------------------------------------------------------------

# Spark types of pipeline/config/types.py -> DuckDB type read from the
# Parquet written by Spark (checked for real by tests/integration).
DUCKDB_TYPES = {
    "string": "VARCHAR",
    "int": "INTEGER",
    "integer": "INTEGER",
    "float": "FLOAT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
}
TECHNICAL_COLUMNS = [
    ("lake_file_id", "BIGINT"),
    ("lake_ingested_at", "TIMESTAMP"),
    ("lake_run_id", "VARCHAR"),
    ("lake_source_file", "VARCHAR"),
]


def _dbt_sources():
    (source,) = yaml.safe_load(SOURCES_YML.read_text(encoding="utf-8"))["sources"]
    return source


def test_p2_4_dbt_sources_are_staging_tables_of_the_reading_configs():
    """Every dbt source table is the staging table of a reading config: a
    renamed `storage.table_name` or a removed `staging_path` must not leave
    dbt reading a table nobody writes."""
    from pipeline.config.registry import SourceRegistry

    registry = SourceRegistry.from_directory(PROJECT_ROOT / "examples" / "config")
    staging_tables = {
        loaded.config.storage.staging_path + loaded.config.storage.table_name
        for loaded in registry.sources
        if loaded.config.storage.staging_path
    }
    source = _dbt_sources()
    assert source["name"] == "lake_staging"
    assert "/staging/$identifier/" in source["config"]["external_location"]
    for table in source["tables"]:
        assert f"staging/{table['name']}" in staging_tables, table["name"]


def test_p2_4_movies_source_columns_follow_the_imdb_schema():
    """The typed empty relation (no movies file read yet) is built from these
    columns: same names, order and types as the files Spark writes."""
    from pipeline.config.registry import load_source_config

    columns_type = load_source_config(
        PROJECT_ROOT / "examples" / "config" / "sources" / "imdb.yaml"
    ).reader.columns_type
    (movies,) = [t for t in _dbt_sources()["tables"] if t["name"] == "movies"]
    declared = [(c["name"], c["data_type"]) for c in movies["columns"]]

    expected = [(name, DUCKDB_TYPES[spark_type]) for name, spark_type in columns_type.items()]
    assert declared == expected + TECHNICAL_COLUMNS


def test_p2_6_orders_source_columns_follow_the_orders_schema():
    """P2-6 (replaces test_p2_4_orders_source_declares_only_technical_columns):
    orders.yaml declares `columns_type`, so the typed empty relation of
    orders has the business columns too, like movies."""
    from pipeline.config.registry import load_source_config

    columns_type = load_source_config(
        PROJECT_ROOT / "examples" / "config" / "sources" / "orders.yaml"
    ).reader.columns_type
    (orders,) = [t for t in _dbt_sources()["tables"] if t["name"] == "orders"]
    declared = [(c["name"], c["data_type"]) for c in orders["columns"]]

    expected = [(name, DUCKDB_TYPES[spark_type]) for name, spark_type in columns_type.items()]
    assert declared == expected + TECHNICAL_COLUMNS


def test_p2_6_every_dbt_source_table_is_typed_like_its_reading_config():
    """No staging table read with an inferred schema reaches dbt: each
    source table has a `columns_type` to align its columns on."""
    from pipeline.config.registry import SourceRegistry

    typed = {
        loaded.config.storage.table_name
        for loaded in SourceRegistry.from_directory(PROJECT_ROOT / "examples" / "config").sources
        if loaded.config.reader.columns_type
    }
    assert {t["name"] for t in _dbt_sources()["tables"]} <= typed


def test_p2_4_dbt_profile_reads_paths_from_the_environment():
    """No path in the repo: DuckDB file from DBT_DUCKDB_PATH, one thread."""
    profiles = yaml.safe_load((TRANSFORM_DIR / "profiles.yml").read_text(encoding="utf-8"))
    output = profiles["lake_transform"]["outputs"]["dev"]
    assert output["type"] == "duckdb"
    assert output["path"] == "{{ env_var('DBT_DUCKDB_PATH') }}"
    assert output["threads"] == 1


def test_p2_4_c3_movies_contract_matches_the_source_columns():
    """C-3: the enforced contract of stg_movies__current declares exactly the
    columns and types of the movies source (imdb.yaml + technical columns,
    checked above), so a type widened by union_by_name fails the build."""
    models = yaml.safe_load(
        (TRANSFORM_DIR / "models" / "staging" / "_staging__models.yml").read_text(encoding="utf-8")
    )["models"]
    (model,) = [m for m in models if m["name"] == "stg_movies__current"]
    (movies,) = [t for t in _dbt_sources()["tables"] if t["name"] == "movies"]

    assert model["config"]["contract"] == {"enforced": True}
    assert [(c["name"], c["data_type"]) for c in model["columns"]] == [
        (c["name"], c["data_type"]) for c in movies["columns"]
    ]


def test_p2_6_orders_contract_matches_the_source_columns():
    """P2-6: same enforced contract as stg_movies__current, on the orders
    source columns (orders.yaml + technical columns, checked above)."""
    models = yaml.safe_load(
        (TRANSFORM_DIR / "models" / "staging" / "_staging__models.yml").read_text(encoding="utf-8")
    )["models"]
    (model,) = [m for m in models if m["name"] == "stg_orders__current"]
    (orders,) = [t for t in _dbt_sources()["tables"] if t["name"] == "orders"]

    assert model["config"]["contract"] == {"enforced": True}
    assert [(c["name"], c["data_type"]) for c in model["columns"]] == [
        (c["name"], c["data_type"]) for c in orders["columns"]
    ]


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
