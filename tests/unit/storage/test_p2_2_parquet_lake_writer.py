"""
P2-2 (docs/audit/2026-11-05-baseline.md, constat A1): the Spark job really
writes what it reads, as partitioned Parquet under `LAKE_ROOT`.

- `resolve_lake_root`: `LAKE_ROOT`, or `<project root>/lake` resolved from the
  code (not from the current directory); a local path becomes a `file://` URI,
  a distributed URI is used as is, a relative path (or an URI with neither
  host nor absolute path, `file:lake`) is refused.
- `ParquetLakeWriter`: adds `lake_run_id`, `lake_source_file`, `lake_file_id`
  and `lake_ingested_at`, partitions by (`lake_run_id`, `lake_source_file`)
  and overwrites only the partitions it writes (`partitionOverwriteMode=dynamic`
  as a writer option, never as a session setting). Re-writing the same
  (run, file) replaces exactly its partition, even with 0 rows; other files
  and runs are left alone.
- No technical name starts with `_` or `.`: pyarrow (and pandas through it)
  skips such paths by default, a `_run_id=...` partition directory included,
  and would read 0 rows without any error (C-1).
- `latest_snapshot`: the current state of a table, one listing per file (C-4).

Uses the session-wide `spark` fixture of tests/unit/conftest.py and `tmp_path`
as the lake root.
"""

import subprocess
from pathlib import Path

import pytest
from pyspark.sql import DataFrame

from storage.base_writer import BaseWriter
from storage.parquet_lake_writer import (
    DEFAULT_LAKE_ROOT,
    FILE_ID_COLUMN,
    INGESTED_AT_COLUMN,
    PARTITION_COLUMNS,
    RUN_ID_COLUMN,
    SOURCE_FILE_COLUMN,
    TECHNICAL_COLUMNS,
    LakeRootError,
    ParquetLakeWriter,
    latest_snapshot,
    resolve_lake_root,
)
from utils.path_utils import normalize_spark_path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent

# A real Airflow run id: `:` is escaped in a partition directory name, `.`
# and `+` are not; all three must be read back unchanged.
AIRFLOW_RUN_ID = "manual__2026-11-13T14:21:36.519211+00:00"
# `#` is escaped by Spark too, the space is not.
ODD_FILE_NAME = "orders 2024#1.csv"


@pytest.fixture
def writer(tmp_path):
    return ParquetLakeWriter(resolve_lake_root({"LAKE_ROOT": str(tmp_path)}))


def _orders(spark, rows=((1, "a"), (2, "b"))) -> DataFrame:
    return spark.createDataFrame(list(rows), "order_id int, item string")


def _write(writer, data, target="raw/orders", *, run_id="run_1", source_file="orders_1.csv", **kw):
    kw.setdefault("file_id", 1)
    return writer.write(data, target, run_id=run_id, source_file=source_file, **kw)


def _read(spark, tmp_path, target, **options) -> DataFrame:
    return spark.read.options(**options).parquet((tmp_path / target).as_uri())


def _partition_dir(tmp_path, target, run_id_dir, source_file_dir) -> Path:
    return tmp_path / target / f"lake_run_id={run_id_dir}" / f"lake_source_file={source_file_dir}"


def _counts(df: DataFrame) -> dict[tuple[str, str], int]:
    return {
        (r[RUN_ID_COLUMN], r[SOURCE_FILE_COLUMN]): r["count"]
        for r in df.groupBy(RUN_ID_COLUMN, SOURCE_FILE_COLUMN).count().collect()
    }


# ---------------------------------------------------------------------------
# LAKE_ROOT
# ---------------------------------------------------------------------------


def test_p2_2_default_lake_root_is_resolved_from_the_code_not_the_cwd(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    assert DEFAULT_LAKE_ROOT == PROJECT_ROOT / "lake"
    assert resolve_lake_root({}) == (PROJECT_ROOT / "lake").as_uri()


def test_p2_2_lake_root_reads_the_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    assert resolve_lake_root() == (tmp_path / "lake").as_uri()


def test_p2_2_empty_lake_root_falls_back_on_the_default():
    assert resolve_lake_root({"LAKE_ROOT": "  "}) == (PROJECT_ROOT / "lake").as_uri()


@pytest.mark.parametrize(
    "uri",
    [
        "s3://bucket/lake",
        "s3://bucket/x",
        "hdfs://nn/x",
        "hdfs://namenode:8020/lake",
        "abfss://c@acct.dfs.core.windows.net/lake",
        "file:///abs/lake",
        "file:/abs/lake",
    ],
)
def test_p2_2_distributed_lake_root_is_used_as_is(uri):
    assert resolve_lake_root({"LAKE_ROOT": uri}) == uri


def test_p2_2_relative_lake_root_is_refused(monkeypatch, tmp_path):
    """Under spark-submit the current directory is whatever the worker uses:
    a relative root would move the lake from one run (or retry) to another."""
    monkeypatch.chdir(tmp_path)
    with pytest.raises(LakeRootError, match="LAKE_ROOT"):
        resolve_lake_root({"LAKE_ROOT": "lake"})


@pytest.mark.parametrize("uri", ["file:lake", "hdfs:lake", "file:", "s3://", "file:./lake"])
def test_p2_2_relative_uri_lake_root_is_refused(uri):
    """C-7: `file:lake` is a URI with neither host nor absolute path: Hadoop
    resolves it against the current directory, exactly like `lake`."""
    with pytest.raises(LakeRootError, match="LAKE_ROOT"):
        resolve_lake_root({"LAKE_ROOT": uri})


def test_p2_2_lake_directory_is_gitignored():
    if not (PROJECT_ROOT / ".git").exists():
        pytest.skip("not a git checkout")
    result = subprocess.run(
        ["git", "check-ignore", "--quiet", "lake/x"], cwd=PROJECT_ROOT, check=False
    )
    assert result.returncode == 0, "lake/ is not ignored by git"


# ---------------------------------------------------------------------------
# ParquetLakeWriter
# ---------------------------------------------------------------------------


def test_p2_2_writer_is_a_base_writer(writer):
    assert isinstance(writer, BaseWriter)


def test_p2_2_technical_columns_never_start_with_an_underscore_or_a_dot():
    """C-1: pyarrow's default `ignore_prefixes=['.', '_']` skips such paths."""
    assert PARTITION_COLUMNS == (RUN_ID_COLUMN, SOURCE_FILE_COLUMN)
    assert (RUN_ID_COLUMN, SOURCE_FILE_COLUMN, FILE_ID_COLUMN, INGESTED_AT_COLUMN) == (
        "lake_run_id",
        "lake_source_file",
        "lake_file_id",
        "lake_ingested_at",
    )
    assert set(TECHNICAL_COLUMNS) == {
        RUN_ID_COLUMN,
        SOURCE_FILE_COLUMN,
        FILE_ID_COLUMN,
        INGESTED_AT_COLUMN,
    }
    assert not [c for c in TECHNICAL_COLUMNS if c.startswith(("_", "."))]


def test_p2_2_location_joins_the_lake_root_and_the_target(tmp_path):
    root = normalize_spark_path(str(tmp_path))
    assert ParquetLakeWriter(root).location("raw/orders") == f"{root}/raw/orders"
    assert ParquetLakeWriter("s3://bucket/lake/").location("raw/orders") == (
        "s3://bucket/lake/raw/orders"
    )


@pytest.mark.parametrize("target", ["../orders", "/abs/orders", "s3://b/orders", "raw/../x", ""])
def test_p2_2_writer_refuses_a_target_outside_the_lake(writer, target):
    with pytest.raises(ValueError):
        writer.location(target)


def test_p2_2_writes_raw_at_the_right_path_with_partitions(spark, writer, tmp_path):
    rows = _write(writer, _orders(spark), file_id=42)

    assert rows == 2
    assert _partition_dir(tmp_path, "raw/orders", "run_1", "orders_1.csv").is_dir()

    df = _read(spark, tmp_path, "raw/orders")
    assert set(df.columns) == {"order_id", "item", *TECHNICAL_COLUMNS}
    assert dict(df.dtypes)[INGESTED_AT_COLUMN] == "timestamp"
    assert dict(df.dtypes)[FILE_ID_COLUMN] == "bigint"
    assert sorted(
        (r.order_id, r.item, r.lake_run_id, r.lake_source_file, r.lake_file_id)
        for r in df.collect()
    ) == [
        (1, "a", "run_1", "orders_1.csv", 42),
        (2, "b", "run_1", "orders_1.csv", 42),
    ]


def test_p2_2_lake_is_readable_outside_spark_with_pyarrow(spark, writer, tmp_path):
    """C-1: with `_run_id=` partition directories, pyarrow.dataset,
    pq.read_table and pd.read_parquet read 0 row, without any error."""
    import pandas as pd
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    _write(writer, _orders(spark), run_id=AIRFLOW_RUN_ID, source_file=ODD_FILE_NAME)
    _write(writer, _orders(spark, [(3, "c")]), run_id="run_2", source_file="orders_2.csv")
    table_dir = str(tmp_path / "raw" / "orders")

    table = ds.dataset(table_dir, format="parquet", partitioning="hive").to_table()
    assert table.num_rows == 3
    assert sorted(
        zip(
            table.column(RUN_ID_COLUMN).to_pylist(),
            table.column(SOURCE_FILE_COLUMN).to_pylist(),
            strict=True,
        )
    ) == [
        (AIRFLOW_RUN_ID, ODD_FILE_NAME),
        (AIRFLOW_RUN_ID, ODD_FILE_NAME),
        ("run_2", "orders_2.csv"),
    ]
    assert pq.read_table(table_dir).num_rows == 3
    assert len(pd.read_parquet(table_dir)) == 3


def test_p2_2_rewriting_the_same_run_and_file_replaces_its_partition(spark, writer, tmp_path):
    _write(writer, _orders(spark))
    _write(writer, _orders(spark))

    df = _read(spark, tmp_path, "raw/orders")
    assert df.count() == 2  # no duplicate
    assert sorted(r.order_id for r in df.collect()) == [1, 2]

    # Exact replacement: a different content for the same (run, file) wins.
    _write(writer, _orders(spark, [(3, "c")]))
    assert [r.order_id for r in _read(spark, tmp_path, "raw/orders").collect()] == [3]


def test_p2_2_two_files_of_the_same_run_are_both_kept(spark, writer, tmp_path):
    _write(writer, _orders(spark))
    _write(writer, _orders(spark, [(3, "c")]), source_file="orders_2.csv", file_id=2)

    assert _counts(_read(spark, tmp_path, "raw/orders")) == {
        ("run_1", "orders_1.csv"): 2,
        ("run_1", "orders_2.csv"): 1,
    }


def test_p2_2_another_run_is_not_touched(spark, writer, tmp_path):
    _write(writer, _orders(spark))
    _write(writer, _orders(spark, [(3, "c")]), run_id="run_2", file_id=2)
    # Re-running run_2 leaves run_1 alone.
    _write(writer, _orders(spark, [(3, "c")]), run_id="run_2", file_id=2)

    assert _counts(_read(spark, tmp_path, "raw/orders")) == {
        ("run_1", "orders_1.csv"): 2,
        ("run_2", "orders_1.csv"): 1,
    }


def test_p2_2_airflow_run_id_is_escaped_and_read_back_unchanged(spark, writer, tmp_path):
    _write(writer, _orders(spark), run_id=AIRFLOW_RUN_ID, source_file=ODD_FILE_NAME)
    _write(writer, _orders(spark), run_id=AIRFLOW_RUN_ID, source_file=ODD_FILE_NAME)

    df = _read(spark, tmp_path, "raw/orders")
    assert _counts(df) == {(AIRFLOW_RUN_ID, ODD_FILE_NAME): 2}


def test_p2_2_a_second_file_with_another_schema_does_not_break_the_first(spark, writer, tmp_path):
    """Default tables (`default_csv`...) receive unrelated files: each
    partition keeps its own schema. Reading the whole table needs mergeSchema."""
    _write(writer, _orders(spark), "raw/default_csv", source_file="a.csv")
    other = spark.createDataFrame([("x", 1.5)], "label string, score double")
    _write(writer, other, "raw/default_csv", source_file="b.csv", file_id=2)

    # Not `Path.as_uri()`: it percent-encodes `=` (`%3D`), which Hadoop keeps
    # as is in the file name.
    first = spark.read.parquet(
        normalize_spark_path(str(_partition_dir(tmp_path, "raw/default_csv", "run_1", "a.csv")))
    )
    assert set(first.columns) == {"order_id", "item", FILE_ID_COLUMN, INGESTED_AT_COLUMN}
    assert sorted(r.order_id for r in first.collect()) == [1, 2]

    merged = _read(spark, tmp_path, "raw/default_csv", mergeSchema="true")
    assert {"order_id", "item", "label", "score"} <= set(merged.columns)
    assert merged.count() == 3


def test_p2_2_overwrite_is_dynamic_per_write_not_a_session_setting(spark, writer):
    """The dynamic mode is a writer option: the session keeps its default."""
    before = spark.conf.get("spark.sql.sources.partitionOverwriteMode")
    _write(writer, _orders(spark))
    assert spark.conf.get("spark.sql.sources.partitionOverwriteMode") == before


def test_p2_2_append_is_an_explicit_choice(spark, writer, tmp_path):
    _write(writer, _orders(spark))
    _write(writer, _orders(spark), mode="append")
    assert _read(spark, tmp_path, "raw/orders").count() == 4


def test_p2_2_unknown_mode_is_refused(spark, writer):
    with pytest.raises(ValueError, match="mode"):
        _write(writer, _orders(spark), mode="ignore")


@pytest.mark.parametrize(
    "column", ["lake_run_id", "LAKE_SOURCE_FILE", "lake_ingested_at", "Lake_File_Id"]
)
def test_p2_2_a_technical_column_in_the_data_is_refused(spark, writer, column):
    """Spark compares column names case-insensitively."""
    df = spark.createDataFrame([(1, "x")], f"id int, `{column}` string")
    with pytest.raises(ValueError, match="colonne technique"):
        _write(writer, df)


def test_p2_2_business_columns_starting_with_an_underscore_are_kept(spark, writer, tmp_path):
    """The old technical names are ordinary business columns now."""
    df = spark.createDataFrame([(1, "x")], "id int, `_run_id` string")
    _write(writer, df)
    assert [r._run_id for r in _read(spark, tmp_path, "raw/orders").collect()] == ["x"]


@pytest.mark.parametrize(("run_id", "source_file"), [("", "a.csv"), ("run_1", ""), (None, "a.csv")])
def test_p2_2_missing_run_id_or_source_file_is_refused(spark, writer, run_id, source_file):
    with pytest.raises(ValueError):
        _write(writer, _orders(spark), run_id=run_id, source_file=source_file)


@pytest.mark.parametrize("file_id", [None, "1", True, 1.0, -1])
def test_p2_2_invalid_file_id_is_refused(spark, writer, file_id):
    with pytest.raises(ValueError, match="file_id"):
        _write(writer, _orders(spark), file_id=file_id)


# ---------------------------------------------------------------------------
# C-2: a rewrite with 0 row must not leave the previous partition behind
# ---------------------------------------------------------------------------


def test_p2_2_writing_zero_rows_to_a_new_partition_returns_zero(spark, writer, tmp_path):
    empty = spark.createDataFrame([], "order_id int, item string")
    assert _write(writer, empty, source_file="empty.csv") == 0
    assert not list((tmp_path / "raw" / "orders").glob("lake_run_id=*/lake_source_file=*"))


def test_p2_2_zero_row_rewrite_removes_previous_partition(spark, writer, tmp_path, caplog):
    """C-2: a dynamic overwrite without any row replaces no partition: the
    previous attempt's rows stayed in place while the file became `processed`.
    The writer now deletes the partition itself, with Spark's own escaping of
    the partition values (`:` and `#` here)."""
    _write(
        writer,
        _orders(spark, [(1, "a"), (2, "b"), (3, "c")]),
        run_id=AIRFLOW_RUN_ID,
        source_file=ODD_FILE_NAME,
    )
    _write(writer, _orders(spark), run_id=AIRFLOW_RUN_ID, source_file="orders_2.csv", file_id=2)
    _write(writer, _orders(spark), run_id="run_0", source_file=ODD_FILE_NAME, file_id=0)

    empty = spark.createDataFrame([], "order_id int, item string")
    with caplog.at_level("WARNING", logger="storage.parquet_lake_writer"):
        rows = _write(writer, empty, run_id=AIRFLOW_RUN_ID, source_file=ODD_FILE_NAME)

    assert rows == 0
    assert _counts(_read(spark, tmp_path, "raw/orders")) == {
        (AIRFLOW_RUN_ID, "orders_2.csv"): 2,
        ("run_0", ODD_FILE_NAME): 2,
    }
    # Gone from disk too, not just empty: only run_0 still has that file.
    remaining = list((tmp_path / "raw" / "orders").glob("lake_run_id=*/lake_source_file=*2024*"))
    assert [p.parent.name for p in remaining] == ["lake_run_id=run_0"]
    assert "removed" in caplog.text


def test_p2_2_zero_row_rewrite_leaves_no_empty_run_directory(spark, writer, tmp_path):
    """When the removed partition was the run's last one, its `lake_run_id=`
    directory goes too: no empty directory left in the table."""
    _write(writer, _orders(spark), run_id="run_0", file_id=0)
    _write(writer, _orders(spark), run_id=AIRFLOW_RUN_ID)
    empty = spark.createDataFrame([], "order_id int, item string")

    _write(writer, empty, run_id=AIRFLOW_RUN_ID)

    table = tmp_path / "raw" / "orders"
    assert sorted(p.name for p in table.glob("lake_run_id=*")) == ["lake_run_id=run_0"]
    assert _counts(_read(spark, tmp_path, "raw/orders")) == {("run_0", "orders_1.csv"): 2}


def test_p2_2_zero_row_append_leaves_the_partition(spark, writer, tmp_path):
    """`append` adds rows, it never replaces: 0 row appended changes nothing."""
    _write(writer, _orders(spark))
    empty = spark.createDataFrame([], "order_id int, item string")
    assert _write(writer, empty, mode="append") == 0
    assert _read(spark, tmp_path, "raw/orders").count() == 2


# ---------------------------------------------------------------------------
# C-4: the current state of a table
# ---------------------------------------------------------------------------


def test_p2_2_latest_snapshot_keeps_only_the_latest_listing_of_each_file(spark, writer, tmp_path):
    """Each listing rewrites every file under its own run id: a table holds
    one snapshot per listing. The current state keeps, per file, the
    snapshot of the latest listing, i.e. the highest `lake_file_id`
    (`file_list.id`, assigned by a Postgres sequence when the listing
    recorded the file).

    Run ids are chosen so that their lexical order is the reverse of the
    listing order, and the older listing is re-run last (a Clear): neither
    the run id nor the write time (`lake_ingested_at`) may decide."""
    _write(writer, _orders(spark, [(1, "old")]), run_id="run_b", file_id=10)
    _write(writer, _orders(spark, [(9, "other")]), run_id="run_b", source_file="o2.csv", file_id=11)
    _write(writer, _orders(spark, [(2, "new"), (3, "new")]), run_id="run_a", file_id=20)
    # Clear of the older listing: rewritten after, with a newer lake_ingested_at.
    _write(writer, _orders(spark, [(1, "old")]), run_id="run_b", file_id=10)

    current = latest_snapshot(_read(spark, tmp_path, "raw/orders"))

    assert current.columns == _read(spark, tmp_path, "raw/orders").columns
    assert sorted(
        (r.order_id, r.item, r.lake_run_id, r.lake_source_file) for r in current.collect()
    ) == [
        (2, "new", "run_a", "orders_1.csv"),
        (3, "new", "run_a", "orders_1.csv"),
        (9, "other", "run_b", "o2.csv"),
    ]


def test_p2_2_latest_snapshot_needs_the_technical_columns(spark):
    with pytest.raises(ValueError, match=FILE_ID_COLUMN):
        latest_snapshot(_orders(spark))
