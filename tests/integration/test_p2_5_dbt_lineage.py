"""
P2-5 — the dataset names dbt really emits, and the bridge to Spark.

A real `dbt build` of transform/ runs on a mini lake written by the real
ParquetLakeWriter, in a copy of the project (as Cosmos runs it). The
artefacts are then parsed by openlineage-integration-common's
DbtLocalArtifactProcessor, the processor Cosmos calls in the producer task
of transform_dag (DbtLocalBaseOperator.calculate_openlineage_events_completes),
with the same arguments. The test checks that:
  - the staging models read the `lake_staging` sources under their DuckDB
    names, exactly the outputs of the bridge (lake_staging_lineage) for the
    staging tables that exist, so the graph goes on from Spark's staging
    tables to dbt;
  - the marts are outputs, downstream of the staging models;
  - every dbt dataset lives in the warehouse's namespace.

Needs the transform environment (`make transform-sync`), like the P2-4
tests (skipped without it, unless REQUIRE_DBT=1).
"""

import json
import shutil

import pytest

from tests.integration.test_p2_4_dbt_build import (
    TRANSFORM_DIR,
    _assert_ok,
    _dbt,
    _write_movies,
    dbt_executable,  # noqa: F401  (fixture)
)

COSMOS_PRODUCER = "https://github.com/astronomer/astronomer-cosmos/"


@pytest.fixture
def built_project(dbt_executable, spark, tmp_path):  # noqa: F811
    """(project copy with its target/, environment of dbt) after a build."""
    from storage.parquet_lake_writer import ParquetLakeWriter, resolve_lake_root

    lake = resolve_lake_root({"LAKE_ROOT": str(tmp_path / "lake")})
    _write_movies(spark, ParquetLakeWriter(lake))
    project = tmp_path / "project"
    shutil.copytree(
        TRANSFORM_DIR,
        project,
        ignore=shutil.ignore_patterns(".venv", "target", "logs", "*.duckdb*", ".user.yml"),
    )
    env = {"LAKE_ROOT": lake, "DBT_DUCKDB_PATH": str(tmp_path / "warehouse.duckdb")}
    proc = _dbt(tmp_path, "build", lake_root=env["LAKE_ROOT"], warehouse=env["DBT_DUCKDB_PATH"])
    _assert_ok(proc)
    # _dbt writes the artefacts outside transform/: move them where the
    # processor (and Cosmos' copy of the project) reads them.
    shutil.copytree(tmp_path / "dbt_target", project / "target")
    return project, env


def _dbt_events(project, env, monkeypatch):
    from openlineage.common.provider.dbt.local import DbtLocalArtifactProcessor

    # Cosmos exports dbt's environment before parsing: the profile renders
    # env_var('DBT_DUCKDB_PATH').
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    processor = DbtLocalArtifactProcessor(
        producer=COSMOS_PRODUCER,
        job_namespace="p2_5_it",
        project_dir=str(project),
        profile_name="lake_transform",
        target="dev",
    )
    processor.should_raise_on_unsupported_command = False
    return processor.parse().completes


def _names(datasets):
    return {(d.namespace, d.name) for d in datasets}


def _job(completes, model, step="run"):
    """The event of one model's step: job names are
    `<database>.<schema>.<project>.<model>.<command>.<step>`."""
    [event] = [e for e in completes if e.job.name.endswith(f".{model}.build.{step}")]
    return event


def test_p2_5_dbt_reads_the_staging_sources_the_bridge_feeds(built_project, monkeypatch):
    from dags.local_transform_dag import DBT_MANIFEST, lake_staging_lineage

    project, env = built_project
    completes = _dbt_events(project, env, monkeypatch)

    bridge = lake_staging_lineage(json.loads(DBT_MANIFEST.read_text(encoding="utf-8")), env)
    bridge_outputs = {dbt for _, dbt in bridge}
    warehouse = f"duckdb://{env['DBT_DUCKDB_PATH']}"

    movies = _job(completes, "stg_movies__current")
    orders = _job(completes, "stg_orders__current")
    assert _names(movies.inputs) == {(warehouse, "warehouse.lake_staging.movies")}
    assert _names(orders.inputs) == {(warehouse, "warehouse.lake_staging.orders")}
    # The bridge feeds the staging source dbt reads. C-9: only movies has a
    # staging directory in this lake (no orders file read), so orders, which
    # dbt reads as a typed empty relation, is not linked: the link is
    # structural, never invented for a directory that does not exist.
    assert bridge_outputs == _names(movies.inputs)

    mart = _job(completes, "mart_movies__by_release_year_language")
    assert _names(mart.inputs) == _names(movies.outputs)
    assert _names(mart.outputs) == {
        (warehouse, "warehouse.main.mart_movies__by_release_year_language")
    }
    namespaces = {ns for e in completes for ns, _ in _names(e.inputs) | _names(e.outputs)}
    assert namespaces == {warehouse}
