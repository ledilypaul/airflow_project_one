"""
P2-4 — real `dbt build` of transform/ on a mini lake written by Spark.

The staging zone is written with the real ParquetLakeWriter (shared `spark`
fixture), with several snapshots per file and hostile run ids (`:`, `+`,
`#`, spaces, accents, a purely numeric `0042`). The dbt project then runs
for real (transform/.venv/bin/dbt, DuckDB) and the tests check that:
  - stg_movies__current / stg_orders__current equal Spark's
    `latest_snapshot` of the same table, run ids and file names included
    (hive_types VARCHAR: `0042` stays text), `lake_ingested_at` read back in
    UTC (same epoch as Spark's);
  - the movies mart aggregates that current state;
  - a staging table without any file yet (no orders read) gives an empty
    relation typed like the real one, instead of DuckDB's "no files found"
    error, and the build still succeeds;
  - C-1: a lake without any staging file (mistyped LAKE_ROOT) or a scheme
    DuckDB cannot read (`hdfs://`) fails before any model, warehouse intact;
  - C-3: a type widened by an old snapshot breaks the movies contract, and
    the orders one (P2-6, orders.yaml now declares its schema);
  - C-5: no DuckDB extension is auto-installed or auto-loaded;
  - C-6 (dbt side): a failing dbt test fails the build;
  - C-7/C-8: `file:///...` (the job's form) and `file:/...` are accepted;
    `~`, relative paths and glob/partition characters are refused with an
    explicit message.

Needs the transform environment (`make transform-sync`). Without it the
tests are skipped, unless REQUIRE_DBT=1 (set in CI), where a missing dbt is
a failure: CI must run them.
"""

import json
import os
import subprocess
import textwrap
from datetime import date
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TRANSFORM_DIR = PROJECT_ROOT / "transform"
DBT_EXECUTABLE = TRANSFORM_DIR / ".venv" / "bin" / "dbt"
DBT_PYTHON = TRANSFORM_DIR / ".venv" / "bin" / "python"
IMDB_CONFIG = PROJECT_ROOT / "examples" / "config" / "sources" / "imdb.yaml"
ORDERS_CONFIG = PROJECT_ROOT / "examples" / "config" / "sources" / "orders.yaml"

pytestmark = pytest.mark.skipif(os.name != "posix", reason="needs a POSIX venv layout")

# Run ids of the listing DAGs are free text: every character that Spark
# escapes in a partition directory name, plus one DuckDB would type as an
# integer without hive_types.
RUN_OLD = "manual__2026-11-01T10:00:00+00:00"
RUN_NEW = "asset_triggered__2026-11-02T10:00:00.123+00:00_a#b"
RUN_NUMERIC = "0042"
RUN_HOSTILE = "run with spaces: é+#%"

FILE_A = "imdb_a.csv"
FILE_B = "IMDB é b.csv"
FILE_C = "imdb+plus.csv"


@pytest.fixture(scope="module")
def dbt_executable():
    if not DBT_EXECUTABLE.exists():
        message = f"{DBT_EXECUTABLE} not found: run `make transform-sync` (uv sync in transform/)"
        if os.environ.get("REQUIRE_DBT") == "1":
            pytest.fail(message + " (REQUIRE_DBT=1: these tests must run)")
        pytest.skip(message)
    return DBT_EXECUTABLE


def _dbt(tmp_path: Path, *args: str, lake_root: str, warehouse: str) -> subprocess.CompletedProcess:
    """A dbt command on transform/, with its artefacts outside the project
    (transform/target holds the manifest the DAG tests read)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("DBT_", "LAKE_ROOT"))}
    env.update(
        LAKE_ROOT=lake_root,
        DBT_DUCKDB_PATH=warehouse,
        DBT_TARGET_PATH=str(tmp_path / "dbt_target"),
        DBT_LOG_PATH=str(tmp_path / "dbt_logs"),
        DBT_SEND_ANONYMOUS_USAGE_STATS="False",
    )
    return subprocess.run(
        [
            str(DBT_EXECUTABLE),
            *args,
            "--project-dir",
            str(TRANSFORM_DIR),
            "--profiles-dir",
            str(TRANSFORM_DIR),
        ],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=300,
    )


def _dbt_build(tmp_path: Path, *, lake_root: str, warehouse: str) -> subprocess.CompletedProcess:
    return _dbt(tmp_path, "build", lake_root=lake_root, warehouse=warehouse)


def _assert_refused(proc: subprocess.CompletedProcess, message: str) -> None:
    """Refused by the on-run-start hook: non-zero exit, the explicit message,
    and no model run (the warehouse is not touched)."""
    output = proc.stdout + proc.stderr
    assert proc.returncode != 0, output[-3000:]
    assert message in output, output[-3000:]
    assert "START sql table model" not in output, output[-3000:]


def _assert_ok(proc: subprocess.CompletedProcess) -> None:
    assert proc.returncode == 0, proc.stdout[-6000:] + proc.stderr[-3000:]


QUERY_SCRIPT = textwrap.dedent(
    """
    import json, sys
    import duckdb

    database, queries = sys.argv[1], json.loads(sys.argv[2])
    con = duckdb.connect(database, read_only=True)
    out = {}
    for name, sql in queries.items():
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        out[name] = [dict(zip(cols, row)) for row in cur.fetchall()]
    print(json.dumps(out, default=str))
    """
)


def _query(warehouse: str, **queries: str) -> dict[str, list[dict]]:
    """Run SQL on the warehouse with the transform venv's duckdb (the root
    venv does not ship duckdb)."""
    proc = subprocess.run(
        [str(DBT_PYTHON), "-I", "-c", QUERY_SCRIPT, warehouse, json.dumps(queries)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def _describe(warehouse: str, relation: str) -> list[tuple[str, str]]:
    rows = _query(warehouse, d=f"describe {relation}")["d"]
    return [(r["column_name"], r["column_type"]) for r in rows]


# ---------------------------------------------------------------------------
# Mini lake
# ---------------------------------------------------------------------------


def _movie(schema, **values):
    row = dict.fromkeys(schema.fieldNames())
    row.update(values)
    return tuple(row[name] for name in schema.fieldNames())


def _write_movies(spark, writer) -> None:
    from pipeline.config.registry import load_source_config
    from pipeline.engine.spark.types import build_schema

    schema = build_schema(load_source_config(IMDB_CONFIG).reader.columns_type)

    def write(run_id, source_file, file_id, *movies):
        data = spark.createDataFrame([_movie(schema, **m) for m in movies], schema)
        writer.write(
            data, "staging/movies", run_id=run_id, source_file=source_file, file_id=file_id
        )

    m1 = {"id": 1, "title": "A", "release_year": 2001, "original_language": "en"}
    m1.update(vote_average=7.5, vote_count=100, release_date=date(2001, 5, 1), adult=False)
    m2 = {"id": 2, "title": "B", "release_year": 2001, "original_language": "en"}
    m2.update(vote_average=6.25, vote_count=50)
    m3 = {"id": 3, "title": "C", "release_year": 1999, "original_language": "fr"}
    m3.update(vote_average=8.0, vote_count=10)
    m4 = {"id": 4, "title": "D", "release_year": None, "original_language": None}

    # FILE_A: two listings, the newest (file_id 30) is the current one.
    write(RUN_OLD, FILE_A, 10, m1, m2)
    write(RUN_NEW, FILE_A, 30, {**m1, "title": "A (v2)"}, m2, m3)
    # FILE_B: the snapshot written LAST is the older listing (file_id 11):
    # the current one is still file_id 20 (not "last write wins").
    write(RUN_NUMERIC, FILE_B, 20, m3)
    write(RUN_OLD, FILE_B, 11, m3, m4)
    # FILE_C: one snapshot, under the most hostile run id.
    write(RUN_HOSTILE, FILE_C, 25, m4, m1)


def _write_orders(spark, writer) -> None:
    """P2-6: the schema of orders.yaml (`columns_type`), which the enforced
    contract of stg_orders__current mirrors."""
    from pipeline.config.registry import load_source_config
    from pipeline.engine.spark.types import build_schema

    schema = build_schema(load_source_config(ORDERS_CONFIG).reader.columns_type)

    def write(run_id, source_file, file_id, rows):
        data = spark.createDataFrame(rows, schema)
        writer.write(
            data, "staging/orders", run_id=run_id, source_file=source_file, file_id=file_id
        )

    day = date(2026, 11, 1)
    write(RUN_OLD, "orders_1.csv", 12, [(1, "x", 10.5, day), (2, "y", 3.0, None)])
    write(RUN_NEW, "orders_1.csv", 31, [(1, "x", 11.5, day)])
    write(RUN_NUMERIC, "orders_2.csv", 21, [(7, None, 1.0, day)])


def _spark_current(spark, lake_root: str, table: str, columns: list[str]) -> list[dict]:
    """latest_snapshot() of a staging table read back by Spark, partition
    columns forced to string (a numeric run id would be inferred as int)."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField

    from storage.parquet_lake_writer import ParquetLakeWriter, latest_snapshot

    location = ParquetLakeWriter(lake_root).location(f"staging/{table}")
    file_schema = spark.read.parquet(location).drop("lake_run_id", "lake_source_file").schema
    for name in ("lake_run_id", "lake_source_file"):
        file_schema = file_schema.add(StructField(name, StringType()))
    current = latest_snapshot(spark.read.schema(file_schema).parquet(location))
    projected = current.select(
        *columns,
        F.col("release_date").cast("string").alias("release_date")
        if "release_date" in current.columns
        else F.lit(None).alias("release_date"),
        F.unix_micros("lake_ingested_at").alias("ingested_us"),
    )
    return [row.asDict() for row in projected.collect()]


def _sorted(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: json.dumps(r, sort_keys=True, default=str))


MOVIE_COLUMNS = [
    "id",
    "title",
    "release_year",
    "original_language",
    "vote_average",
    "vote_count",
    "lake_run_id",
    "lake_source_file",
    "lake_file_id",
]
ORDER_COLUMNS = ["order_id", "amount", "customer", "lake_run_id", "lake_source_file"]
ORDER_COLUMNS += ["lake_file_id"]


def _duck_projection(columns: list[str], *, with_release_date: bool) -> str:
    release = "release_date::varchar" if with_release_date else "null"
    return ", ".join(
        [*(f'"{c}"' for c in columns), f"{release} as release_date"]
        + ["epoch_us(lake_ingested_at) as ingested_us"]
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_p2_4_dbt_build_current_state_matches_latest_snapshot(dbt_executable, spark, tmp_path):
    """Movies: several snapshots and hostile run ids; orders: absent first
    (typed empty relation), then written: the current state is Spark's
    latest_snapshot in both cases. LAKE_ROOT is given as the job resolves it
    (file:/// URI)."""
    from storage.parquet_lake_writer import ParquetLakeWriter, resolve_lake_root

    lake_uri = resolve_lake_root({"LAKE_ROOT": str(tmp_path / "lake")})
    assert lake_uri.startswith("file:///")
    writer = ParquetLakeWriter(lake_uri)
    _write_movies(spark, writer)
    warehouse = str(tmp_path / "warehouse.duckdb")

    # 1. No orders file read yet: the build succeeds, orders is empty.
    _assert_ok(_dbt_build(tmp_path, lake_root=lake_uri, warehouse=warehouse))
    result = _query(
        warehouse,
        movies=f"select {_duck_projection(MOVIE_COLUMNS, with_release_date=True)} "
        "from stg_movies__current",
        orders="select count(*) as n from stg_orders__current",
        mart="select * from mart_movies__by_release_year_language",
        run_id_type="select distinct typeof(lake_run_id) as t from stg_movies__current",
    )
    expected = _spark_current(spark, lake_uri, "movies", MOVIE_COLUMNS)

    assert _sorted(result["movies"]) == _sorted(expected)
    assert {r["lake_file_id"] for r in result["movies"]} == {30, 20, 25}
    assert {r["lake_run_id"] for r in result["movies"]} == {RUN_NEW, RUN_NUMERIC, RUN_HOSTILE}
    assert {r["lake_source_file"] for r in result["movies"]} == {FILE_A, FILE_B, FILE_C}
    assert result["run_id_type"] == [{"t": "VARCHAR"}]
    assert result["orders"] == [{"n": 0}]

    # Mart: the aggregate of that current state, null group included.
    groups: dict[tuple, list[dict]] = {}
    for row in expected:
        groups.setdefault((row["release_year"], row["original_language"]), []).append(row)
    mart = {(r["release_year"], r["original_language"]): r for r in result["mart"]}
    assert set(mart) == set(groups)
    for key, rows in groups.items():
        assert mart[key]["movie_rows"] == len(rows)
        assert mart[key]["distinct_movies"] == len({r["id"] for r in rows})
        votes = [r["vote_count"] for r in rows if r["vote_count"] is not None]
        assert mart[key]["total_vote_count"] == (sum(votes) if votes else None)

    # 2. Orders files arrive: same rule as movies.
    _write_orders(spark, writer)
    _assert_ok(_dbt_build(tmp_path, lake_root=lake_uri, warehouse=warehouse))
    orders = _query(
        warehouse,
        o=f"select {_duck_projection(ORDER_COLUMNS, with_release_date=False)} "
        "from stg_orders__current",
    )["o"]
    expected_orders = _spark_current(spark, lake_uri, "orders", ORDER_COLUMNS)
    assert _sorted(orders) == _sorted(expected_orders)
    assert {r["lake_file_id"] for r in orders} == {31, 21}


def test_p2_4_c1_table_without_file_keeps_a_typed_empty_relation(dbt_executable, spark, tmp_path):
    """A staging table that has no file yet (here: movies, while orders was
    read) gives an empty relation with exactly the columns and types of the
    one read from real files, and the build succeeds."""
    from storage.parquet_lake_writer import ParquetLakeWriter

    orders_only = tmp_path / "orders_only"
    _write_orders(spark, ParquetLakeWriter(str(orders_only)))
    empty_wh = str(tmp_path / "empty.duckdb")
    _assert_ok(_dbt_build(tmp_path, lake_root=str(orders_only), warehouse=empty_wh))
    counts = _query(
        empty_wh,
        m="select count(*) as n from stg_movies__current",
        o="select count(*) as n from stg_orders__current",
        mart="select count(*) as n from mart_movies__by_release_year_language",
    )
    assert counts == {"m": [{"n": 0}], "o": [{"n": 2}], "mart": [{"n": 0}]}

    lake = tmp_path / "lake"
    _write_movies(spark, ParquetLakeWriter(str(lake)))
    full_wh = str(tmp_path / "full.duckdb")
    _assert_ok(_dbt_build(tmp_path, lake_root=str(lake), warehouse=full_wh))

    for relation in ("stg_movies__current", "mart_movies__by_release_year_language"):
        assert _describe(empty_wh, relation) == _describe(full_wh, relation), relation
    # P2-6: orders not read in that lake, yet the same typed columns as when
    # it is (orders.yaml columns_type, enforced contract).
    assert _describe(full_wh, "stg_orders__current") == _describe(empty_wh, "stg_orders__current")
    assert _describe(empty_wh, "stg_orders__current") == [
        ("order_id", "INTEGER"),
        ("customer", "VARCHAR"),
        ("amount", "DOUBLE"),
        ("order_date", "DATE"),
        ("lake_file_id", "BIGINT"),
        ("lake_ingested_at", "TIMESTAMP"),
        ("lake_run_id", "VARCHAR"),
        ("lake_source_file", "VARCHAR"),
    ]


def test_p2_4_c1_wrong_lake_root_fails_and_keeps_the_warehouse(dbt_executable, spark, tmp_path):
    """C-1: a mistyped LAKE_ROOT (`.../lak`) used to give PASS, rc=0 and
    empty tables, then `dbt_marts`. transform_dag only runs after a staging
    write, so a lake without any staging file is an error, raised before any
    model: the previous tables are kept."""
    from storage.parquet_lake_writer import ParquetLakeWriter

    lake = tmp_path / "lake"
    _write_movies(spark, ParquetLakeWriter(str(lake)))
    warehouse = str(tmp_path / "warehouse.duckdb")
    _assert_ok(_dbt_build(tmp_path, lake_root=str(lake), warehouse=warehouse))

    for wrong in (tmp_path / "lak", tmp_path):  # typo; parent of the lake
        proc = _dbt_build(tmp_path, lake_root=str(wrong), warehouse=warehouse)
        _assert_refused(proc, "aucun fichier Parquet de staging")

    counts = _query(warehouse, m="select count(*) as n from stg_movies__current")
    assert counts == {"m": [{"n": 6}]}


def test_p2_4_c6_failing_dbt_test_fails_the_build(dbt_executable, spark, tmp_path):
    """C-6 (dbt side): a fabricated lake where one snapshot (lake_file_id 40)
    belongs to two source files breaks one_snapshot_per_source_file: the
    build fails, so the `.test` sensor of stg_movies__current fails and
    publish_dbt_marts (all_success) emits nothing. The Airflow side cannot be
    run by dag.test(), see transform/README.md."""
    from pipeline.config.registry import load_source_config
    from pipeline.engine.spark.types import build_schema
    from storage.parquet_lake_writer import ParquetLakeWriter

    schema = build_schema(load_source_config(IMDB_CONFIG).reader.columns_type)
    writer = ParquetLakeWriter(str(tmp_path / "lake"))
    for source_file in ("imdb_x.csv", "imdb_y.csv"):
        data = spark.createDataFrame([_movie(schema, id=1, title="X")], schema)
        writer.write(data, "staging/movies", run_id=RUN_OLD, source_file=source_file, file_id=40)

    proc = _dbt_build(
        tmp_path, lake_root=str(tmp_path / "lake"), warehouse=str(tmp_path / "w.duckdb")
    )

    output = proc.stdout + proc.stderr
    assert proc.returncode != 0, output[-3000:]
    assert "FAIL 1 one_snapshot_per_source_file_stg_movies__current" in output, output[-3000:]


@pytest.mark.parametrize("value", ["hdfs://namenode/lake", "dbfs:/lake", "abfs://c@a/lake"])
def test_p2_4_c1_scheme_duckdb_cannot_read_is_refused(dbt_executable, tmp_path, value):
    """`hdfs://...` used to scan nothing and succeed: a scheme DuckDB does not
    read is refused by name."""
    proc = _dbt_build(tmp_path, lake_root=value, warehouse=str(tmp_path / "w.duckdb"))
    _assert_refused(proc, "n'est pas lu par DuckDB")


def test_p2_4_c3_widened_old_snapshot_breaks_the_movies_contract(dbt_executable, spark, tmp_path):
    """C-3: union_by_name widens a column to the widest type of every
    snapshot, old ones included. stg_movies__current has an enforced
    contract (types of imdb.yaml): a widened type fails the build instead of
    silently changing the warehouse schema."""
    from pyspark.sql import functions as F

    from storage.parquet_lake_writer import ParquetLakeWriter

    lake = tmp_path / "lake"
    writer = ParquetLakeWriter(str(lake))
    _write_movies(spark, writer)
    old = spark.range(1).select(
        F.lit(9).cast("int").alias("id"), F.lit(5).cast("bigint").alias("vote_count")
    )
    writer.write(old, "staging/movies", run_id="manual__old", source_file="old.csv", file_id=1)

    proc = _dbt_build(tmp_path, lake_root=str(lake), warehouse=str(tmp_path / "w.duckdb"))

    output = proc.stdout + proc.stderr
    assert proc.returncode != 0, output[-3000:]
    assert "contract" in output.lower(), output[-3000:]
    assert "vote_count" in output, output[-3000:]


def test_p2_6_widened_old_snapshot_breaks_the_orders_contract(dbt_executable, spark, tmp_path):
    """P2-6: the same guard on stg_orders__current, now that orders.yaml
    declares its schema: an old snapshot written with `order_id` as BIGINT
    (an inferSchema read, before P2-6) fails the build."""
    from pyspark.sql import functions as F

    from storage.parquet_lake_writer import ParquetLakeWriter

    lake = tmp_path / "lake"
    writer = ParquetLakeWriter(str(lake))
    _write_movies(spark, writer)
    _write_orders(spark, writer)
    old = spark.range(1).select(
        F.lit(9).cast("bigint").alias("order_id"), F.lit(1.0).alias("amount")
    )
    writer.write(old, "staging/orders", run_id="manual__old", source_file="old.csv", file_id=1)

    proc = _dbt_build(tmp_path, lake_root=str(lake), warehouse=str(tmp_path / "w.duckdb"))

    output = proc.stdout + proc.stderr
    assert proc.returncode != 0, output[-3000:]
    assert "contract" in output.lower(), output[-3000:]
    assert "order_id" in output, output[-3000:]


def test_p2_4_c5_duckdb_never_autoloads_extensions(dbt_executable, spark, tmp_path):
    """C-5: no extension downloaded or loaded behind our back (a remote lake
    needs `extensions: [httpfs]` declared in profiles.yml)."""
    from storage.parquet_lake_writer import ParquetLakeWriter

    lake = tmp_path / "lake"
    _write_orders(spark, ParquetLakeWriter(str(lake)))
    proc = _dbt(
        tmp_path,
        "show",
        "--inline",
        "select current_setting('autoinstall_known_extensions') as install, "
        "current_setting('autoload_known_extensions') as autoload",
        "--output",
        "json",
        lake_root=str(lake),
        warehouse=str(tmp_path / "w.duckdb"),
    )
    _assert_ok(proc)
    assert '"install": false' in proc.stdout, proc.stdout[-3000:]
    assert '"autoload": false' in proc.stdout, proc.stdout[-3000:]


def test_p2_4_c7_file_uri_without_authority_is_accepted(dbt_executable, spark, tmp_path):
    """C-7: `file:/abs`, accepted by resolve_lake_root, is accepted too."""
    from storage.parquet_lake_writer import ParquetLakeWriter

    lake = tmp_path / "lake"
    _write_orders(spark, ParquetLakeWriter(str(lake)))
    warehouse = str(tmp_path / "w.duckdb")
    _assert_ok(_dbt_build(tmp_path, lake_root=f"file:{lake}", warehouse=warehouse))
    assert _query(warehouse, o="select count(*) as n from stg_orders__current") == {"o": [{"n": 2}]}


def test_p2_4_c7_tilde_gets_an_accurate_message(dbt_executable, tmp_path):
    """dbt does not expand `~` (the DAG and the Makefile do): say so."""
    proc = _dbt_build(tmp_path, lake_root="~/lake", warehouse=str(tmp_path / "w.duckdb"))
    _assert_refused(proc, "`~` n'est pas développé par dbt")


@pytest.mark.parametrize("char", ["=", "*", "?", "[", "{"])
def test_p2_4_c8_glob_and_partition_characters_are_refused(dbt_executable, tmp_path, char):
    """C-8: LAKE_ROOT is pasted into a glob and a hive layout: `=` would be
    read as a partition, `* ? [ {` as wildcards."""
    root = str(tmp_path / f"la{char}ke")
    proc = _dbt_build(tmp_path, lake_root=root, warehouse=str(tmp_path / "w.duckdb"))
    _assert_refused(proc, f"contient le caractère {char!r}")


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("LAKE_ROOT", "lake"),
        ("LAKE_ROOT", "file:lake"),
        ("DBT_DUCKDB_PATH", "warehouse.duckdb"),
    ],
)
def test_p2_4_dbt_build_refuses_relative_paths(dbt_executable, tmp_path, variable, value):
    """Like the Spark job (resolve_lake_root), a relative LAKE_ROOT is
    refused: dbt runs in a temporary copy of the project under Cosmos, so it
    would read (or create the warehouse) somewhere else on every run.
    (dbt-duckdb opens the database file before the hook: a relative
    DBT_DUCKDB_PATH still leaves an empty file in the current directory,
    never a populated warehouse.)"""
    paths = {"LAKE_ROOT": str(tmp_path / "lake"), "DBT_DUCKDB_PATH": str(tmp_path / "w.duckdb")}
    paths[variable] = value

    proc = _dbt_build(tmp_path, lake_root=paths["LAKE_ROOT"], warehouse=paths["DBT_DUCKDB_PATH"])

    _assert_refused(proc, f"{variable}={value} est un chemin relatif")


class _AlternatingWriter:
    """P5-7 : un appel sur deux passe par le writer Polars, si bien que chaque
    table du lake mélange des partitions écrites par les deux moteurs."""

    def __init__(self, lake_uri: str):
        from pipeline.engine.polars.writer import PolarsLakeWriter
        from storage.parquet_lake_writer import ParquetLakeWriter

        self.writers = [ParquetLakeWriter(lake_uri), PolarsLakeWriter(lake_uri)]
        self.calls = 0

    def write(self, data, target, **partition):
        import polars as pl

        self.calls += 1
        if self.calls % 2:
            return self.writers[0].write(data, target, **partition)
        return self.writers[1].write(pl.from_arrow(data.toArrow()), target, **partition)


def test_p5_7_lake_written_by_both_engines_builds(dbt_executable, spark, tmp_path):
    """Contrats compris : DuckDB lit une table écrite par Spark et Polars comme une seule."""
    from storage.parquet_lake_writer import resolve_lake_root

    lake_uri = resolve_lake_root({"LAKE_ROOT": str(tmp_path / "lake")})
    writer = _AlternatingWriter(lake_uri)
    _write_movies(spark, writer)
    _write_orders(spark, writer)
    warehouse = str(tmp_path / "warehouse.duckdb")
    _assert_ok(_dbt_build(tmp_path, lake_root=lake_uri, warehouse=warehouse))

    result = _query(
        warehouse,
        movies=f"select {_duck_projection(MOVIE_COLUMNS, with_release_date=True)} "
        "from stg_movies__current",
        orders=f"select {_duck_projection(ORDER_COLUMNS, with_release_date=False)} "
        "from stg_orders__current",
    )
    assert _sorted(result["movies"]) == _sorted(
        _spark_current(spark, lake_uri, "movies", MOVIE_COLUMNS)
    )
    assert _sorted(result["orders"]) == _sorted(
        _spark_current(spark, lake_uri, "orders", ORDER_COLUMNS)
    )
    assert {r["lake_file_id"] for r in result["movies"]} == {30, 20, 25}
