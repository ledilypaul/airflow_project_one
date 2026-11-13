"""
P2-3: `ParquetLakeWriter.delete_partition`, the public form of the cleanup
the writer already did for a 0-row rewrite (C-2 of P2-2).

The job calls it when a blocking quality check fails: the staging partition
a previous attempt wrote for this (run, file) must not stay as a valid state.
Same escaping as Spark's partitioned write, nothing else touched, the run
directory removed when it becomes empty.
"""

import pytest

from storage.parquet_lake_writer import ParquetLakeWriter, resolve_lake_root

AIRFLOW_RUN_ID = "manual__2026-11-13T14:21:36.519211+00:00"
ODD_FILE_NAME = "orders 2024#1.csv"


@pytest.fixture
def writer(tmp_path):
    return ParquetLakeWriter(resolve_lake_root({"LAKE_ROOT": str(tmp_path)}))


def _write(spark, writer, run_id, source_file, target="staging/orders"):
    data = spark.createDataFrame([(1, "a")], "id int, item string")
    writer.write(data, target, run_id=run_id, source_file=source_file, file_id=1)


def _partitions(tmp_path, target="staging/orders"):
    table = tmp_path / target
    return sorted(str(p.relative_to(table)) for p in table.glob("*/*"))


def test_p2_3_delete_partition_removes_only_that_run_and_file(spark, writer, tmp_path):
    _write(spark, writer, AIRFLOW_RUN_ID, ODD_FILE_NAME)
    _write(spark, writer, AIRFLOW_RUN_ID, "orders_2.csv")
    _write(spark, writer, "run_2", ODD_FILE_NAME)

    removed = writer.delete_partition(
        spark, "staging/orders", run_id=AIRFLOW_RUN_ID, source_file=ODD_FILE_NAME
    )

    assert removed is True
    assert _partitions(tmp_path) == [
        "lake_run_id=manual__2026-11-13T14%3A21%3A36.519211+00%3A00/lake_source_file=orders_2.csv",
        "lake_run_id=run_2/lake_source_file=orders 2024%231.csv",
    ]
    assert spark.read.parquet((tmp_path / "staging" / "orders").as_uri()).count() == 2


def test_p2_3_delete_partition_removes_the_emptied_run_directory(spark, writer, tmp_path):
    _write(spark, writer, "run_1", "orders_1.csv")
    assert writer.delete_partition(
        spark, "staging/orders", run_id="run_1", source_file="orders_1.csv"
    )
    assert not list((tmp_path / "staging" / "orders").glob("lake_run_id=*"))


def test_p2_3_delete_partition_of_a_missing_partition_or_table_returns_false(
    spark, writer, tmp_path
):
    assert not writer.delete_partition(spark, "staging/none", run_id="r", source_file="f.csv")
    _write(spark, writer, "run_1", "orders_1.csv")
    assert not writer.delete_partition(spark, "staging/orders", run_id="r", source_file="f.csv")
    assert len(_partitions(tmp_path)) == 1


@pytest.mark.parametrize("target", ["../outside", "/abs/path", "s3://bucket/x"])
def test_p2_3_delete_partition_refuses_a_target_outside_the_lake(spark, writer, target):
    with pytest.raises(ValueError):
        writer.delete_partition(spark, target, run_id="r", source_file="f.csv")


@pytest.mark.parametrize("run_id, source_file", [("", "f.csv"), ("r", ""), (None, "f.csv")])
def test_p2_3_delete_partition_refuses_an_ambiguous_partition(spark, writer, run_id, source_file):
    """An empty value would point at the run directory or the whole table."""
    with pytest.raises(ValueError):
        writer.delete_partition(spark, "staging/orders", run_id=run_id, source_file=source_file)
