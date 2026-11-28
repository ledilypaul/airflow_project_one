"""
P2-5 — the OpenLineage link between the Spark staging zone and dbt.

Spark (openlineage-spark) names a staging table by its directory:
namespace `file`, name `<LAKE_ROOT>/staging/<table>`. dbt (the
openlineage-integration-common processor Cosmos uses) names the source that
reads it by its DuckDB relation: namespace `duckdb://<DBT_DUCKDB_PATH>`,
name `<database>.lake_staging.<table>`. Two names for the same data: the
graph would stop at the lake. `lineage_lake_staging`
(LakeStagingLineageOperator, dags/local_transform_dag.py) emits the link
between them: inputs = the Spark names, outputs = the dbt names.

These tests check the names on both sides with the code that produces them
(ParquetLakeWriter for Spark, the dbt manifest and profile rule for dbt);
the real listener and the real dbt processor are exercised by
tests/integration/test_p2_5_*.py.
"""

import json
from pathlib import Path

import pytest
import yaml
from airflow.models import DagBag

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / "dags"
TRANSFORM_DIR = PROJECT_ROOT / "transform"
MANIFEST = TRANSFORM_DIR / "target" / "manifest.json"
SOURCES_YML = TRANSFORM_DIR / "models" / "staging" / "generated" / "_lake_staging__sources.yml"
LINEAGE_TASK = "lineage_lake_staging"


@pytest.fixture(scope="module")
def transform_dag():
    bag = DagBag(dag_folder=str(DAGS_FOLDER / "local_transform_dag.py"))
    assert not bag.import_errors, bag.import_errors
    return bag.dags["transform_dag"]


@pytest.fixture(scope="module")
def manifest():
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def _env(lake_root, warehouse="/srv/dbt/warehouse.duckdb"):
    return {"LAKE_ROOT": lake_root, "DBT_DUCKDB_PATH": warehouse}


def _lake(root: Path, *tables: str) -> Path:
    """A local lake whose staging zone has these tables (C-9: the bridge only
    links the staging directories that exist)."""
    for table in tables or ("movies", "orders"):
        (root / "staging" / table).mkdir(parents=True, exist_ok=True)
    return root


# ---------------------------------------------------------------------------
# lake_staging_lineage(): one (Spark name, dbt name) pair per staging source
# ---------------------------------------------------------------------------


def test_p2_5_bridge_has_one_pair_per_lake_staging_source(manifest, tmp_path):
    from dags.local_transform_dag import lake_staging_lineage

    lake = _lake(tmp_path / "lake")
    pairs = lake_staging_lineage(manifest, _env(str(lake), "/srv/dbt/warehouse.duckdb"))
    dbt_namespace = "duckdb:///srv/dbt/warehouse.duckdb"
    assert pairs == [
        (("file", f"{lake}/staging/movies"), (dbt_namespace, "warehouse.lake_staging.movies")),
        (("file", f"{lake}/staging/orders"), (dbt_namespace, "warehouse.lake_staging.orders")),
    ]


def test_p2_5_c9_local_bridge_keeps_only_existing_staging_tables(manifest, tmp_path):
    """The link is structural (a dbt source reads that directory), not a
    proof that Spark wrote it: for a local lake, a staging table that does
    not exist yet is not linked."""
    from dags.local_transform_dag import lake_staging_lineage

    lake = _lake(tmp_path / "lake", "movies")
    pairs = lake_staging_lineage(manifest, _env(str(lake)))
    assert [spark for spark, _ in pairs] == [("file", f"{lake}/staging/movies")]
    assert lake_staging_lineage(manifest, _env(str(tmp_path / "nowhere"))) == []


def test_p2_5_c9_remote_bridge_keeps_every_source(manifest):
    """A remote lake is not listed (no credentials, no network here)."""
    from dags.local_transform_dag import lake_staging_lineage

    assert len(lake_staging_lineage(manifest, _env("s3://bucket/lake"))) == 2


@pytest.mark.parametrize(
    ("warehouse", "database"),
    [
        ("/data/prod_dw.duckdb", "prod_dw"),
        ("/data/warehouse.prod.duckdb", "warehouse"),
        ("/data/.hidden.duckdb", "hidden"),
        ("/data/plain", "plain"),
        (":memory:", "memory"),
    ],
)
def test_p2_5_c3_dbt_name_follows_dbt_duckdb_rule(manifest, tmp_path, warehouse, database):
    """dbt-duckdb 1.11 (credentials.py, path_derived_database_name): the
    first non-empty dot-separated part of the file name, at run time (not
    the file the manifest was parsed with)."""
    from dags.local_transform_dag import lake_staging_lineage

    pairs = lake_staging_lineage(manifest, _env(str(_lake(tmp_path / "lake")), warehouse))
    assert {dbt for _, dbt in pairs} == {
        (f"duckdb://{warehouse}", f"{database}.lake_staging.movies"),
        (f"duckdb://{warehouse}", f"{database}.lake_staging.orders"),
    }


@pytest.mark.parametrize("form", ["{p}", "{p}/", "file://{p}", "file:{p}", "file://{p}/"])
def test_p2_5_every_local_lake_root_form_gives_the_spark_name(manifest, tmp_path, form):
    """The forms the job and dbt accept for a local lake all name the table
    as Spark does: `file` + the absolute path, no trailing slash."""
    from dags.local_transform_dag import lake_staging_lineage

    lake = _lake(tmp_path / "lake")
    pairs = lake_staging_lineage(manifest, _env(form.format(p=lake)))
    assert pairs[0][0] == ("file", f"{lake}/staging/movies")


def test_p2_5_symlinked_lake_root_is_resolved_like_the_job_does(manifest, tmp_path):
    """resolve_lake_root resolves a local path (Path.resolve()): Spark then
    writes, and names, the real directory."""
    from dags.local_transform_dag import lake_staging_lineage

    real = _lake(tmp_path / "real_lake")
    (tmp_path / "lake").symlink_to(real, target_is_directory=True)
    pairs = lake_staging_lineage(manifest, _env(str(tmp_path / "lake")))
    assert pairs[0][0] == ("file", f"{real}/staging/movies")


def test_p2_5_c5_only_the_root_is_resolved(manifest, tmp_path):
    """`staging` itself a symlink: the job resolves the lake root only
    (normalize_spark_path), then joins the zone and table as text. Spark
    names `<root>/staging/movies`, not the link's target."""
    from dags.local_transform_dag import lake_staging_lineage
    from storage.parquet_lake_writer import ParquetLakeWriter, resolve_lake_root

    target = tmp_path / "elsewhere"
    (target / "movies").mkdir(parents=True)
    lake = tmp_path / "lake"
    lake.mkdir()
    (lake / "staging").symlink_to(target, target_is_directory=True)

    pairs = lake_staging_lineage(manifest, _env(str(lake)))
    writer = ParquetLakeWriter(resolve_lake_root({"LAKE_ROOT": str(lake)}))
    assert writer.location("staging/movies") == f"file://{lake}/staging/movies"
    assert pairs[0][0] == ("file", f"{lake}/staging/movies")


# Real outputs of FilesystemDatasetUtils.fromLocation (openlineage-spark
# 1.55.0, through py4j on PySpark 4.3.0): prefix match on s3/gs/abfs/wasb,
# canonical scheme, leading slash removed for those object stores only.
SPARK_NAMES = [
    ("file:/abs/lake/staging/movies", ("file", "/abs/lake/staging/movies")),
    ("file:///abs/lake/staging/movies", ("file", "/abs/lake/staging/movies")),
    ("file://localhost/abs/lake/staging/movies", ("file", "/abs/lake/staging/movies")),
    ("s3://bucket/lake/staging/movies", ("s3://bucket", "lake/staging/movies")),
    ("s3a://bucket/lake/staging/movies", ("s3://bucket", "lake/staging/movies")),
    ("s3n://bucket/lake/staging/movies", ("s3://bucket", "lake/staging/movies")),
    ("gs://bucket/lake/staging/movies", ("gs://bucket", "lake/staging/movies")),
    ("gcs://bucket/lake/staging/movies", ("gcs://bucket", "/lake/staging/movies")),
    (
        "abfss://cont@acct.dfs.core.windows.net/lake/staging/movies",
        ("abfss://cont@acct.dfs.core.windows.net", "lake/staging/movies"),
    ),
    (
        "abfs://cont@acct.dfs.core.windows.net/lake/staging/movies",
        ("abfss://cont@acct.dfs.core.windows.net", "lake/staging/movies"),
    ),
    (
        "wasbs://cont@acct.blob.core.windows.net/lake/staging/movies",
        ("wasbs://cont@acct.blob.core.windows.net", "lake/staging/movies"),
    ),
    ("hdfs://nn:8020/lake/staging/movies", ("hdfs://nn:8020", "/lake/staging/movies")),
    ("hdfs:///lake/staging/movies", ("hdfs", "/lake/staging/movies")),
    ("dbfs:/lake/staging/movies", ("dbfs", "/lake/staging/movies")),
]


@pytest.mark.parametrize(("location", "expected"), SPARK_NAMES, ids=[c[0] for c in SPARK_NAMES])
def test_p2_5_c4_spark_names_match_the_listener(location, expected):
    from dags.local_transform_dag import spark_dataset

    assert spark_dataset(location) == expected
    assert spark_dataset(location + "/") == expected


def test_p2_5_spark_name_is_the_writers_table_location(manifest, tmp_path):
    """Same directory as ParquetLakeWriter's (the job's writer), for every
    source config with a staging zone that dbt reads."""
    from dags.local_transform_dag import lake_staging_lineage, spark_dataset
    from pipeline.config.registry import SourceRegistry
    from storage.parquet_lake_writer import ParquetLakeWriter, resolve_lake_root

    lake = str(_lake(tmp_path / "lake"))
    writer = ParquetLakeWriter(resolve_lake_root({"LAKE_ROOT": lake}))
    registry = SourceRegistry.from_directory(PROJECT_ROOT / "examples" / "config")
    written = {
        spark_dataset(writer.location(f"{s.config.storage.staging_path}{s.config.table}"))
        for s in registry.sources
        if s.config.storage.staging_path
    }
    read_by_dbt = {spark for spark, _ in lake_staging_lineage(manifest, _env(lake))}
    assert read_by_dbt, "no lake_staging source in the manifest"
    assert read_by_dbt <= written


def test_p2_5_bridge_zone_is_the_one_the_dbt_sources_read():
    """The dbt source glob is the single definition of where dbt reads: the
    bridge's zone must be the one it names."""
    from dags.local_transform_dag import LAKE_STAGING_SOURCE, STAGING_ZONE

    sources = yaml.safe_load(SOURCES_YML.read_text(encoding="utf-8"))["sources"]
    [source] = [s for s in sources if s["name"] == LAKE_STAGING_SOURCE]
    location = source["config"]["external_location"]
    assert f"/{STAGING_ZONE}/$identifier/lake_run_id=*/" in location


# ---------------------------------------------------------------------------
# LakeStagingLineageOperator
# ---------------------------------------------------------------------------


def test_p2_5_lineage_task_is_isolated_from_the_dbt_graph(transform_dag):
    """No upstream, no downstream, not in the DuckDB pool: it never waits
    for dbt, never delays it, and never touches the warehouse."""
    from dags.local_transform_dag import DBT_POOL

    task = transform_dag.get_task(LINEAGE_TASK)
    # DagBag imports the file under its own module name: compare by name.
    assert type(task).__name__ == "LakeStagingLineageOperator"
    assert task.upstream_task_ids == set()
    assert task.downstream_task_ids == set()
    assert task.pool != DBT_POOL


def test_p2_5_operator_lineage_is_the_bridge(transform_dag, manifest):
    from dags.local_transform_dag import dbt_env, lake_staging_lineage

    task = transform_dag.get_task(LINEAGE_TASK)
    expected = lake_staging_lineage(manifest, dbt_env())
    for lineage in (
        task.get_openlineage_facets_on_start(),
        task.get_openlineage_facets_on_complete(None),
    ):
        assert [(d.namespace, d.name) for d in lineage.inputs] == [s for s, _ in expected]
        assert [(d.namespace, d.name) for d in lineage.outputs] == [d for _, d in expected]


def test_p2_5_operator_never_fails_on_lineage(tmp_path, caplog):
    """Unreadable manifest: empty lineage and a warning, never an error (the
    provider would only log it, but execute() must not depend on it)."""
    from dags.local_transform_dag import LakeStagingLineageOperator

    task = LakeStagingLineageOperator(
        task_id="x", manifest_path=str(tmp_path / "missing.json"), dbt_environment=_env("/l")
    )
    with caplog.at_level("WARNING"):
        lineage = task.get_openlineage_facets_on_start()
    assert lineage.inputs == [] and lineage.outputs == []
    assert "missing.json" in caplog.text
    assert task.execute({}) is None
