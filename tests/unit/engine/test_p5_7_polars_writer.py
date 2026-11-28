"""P5-7 (écriture) : le writer Polars écrit les mêmes partitions et les mêmes
types physiques Parquet que le writer Spark."""

import datetime
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from pipeline.engine.polars.writer import PolarsLakeWriter
from storage.lake_layout import escape_path_name
from storage.parquet_lake_writer import ParquetLakeWriter
from utils.path_utils import normalize_spark_path

RUN_ID = "manual__2026-11-28T10:00:00+00:00 #1%é/x?*[a]{b}^'\"\\"
ROW = (1, 2, 1.5, 2.5, True, datetime.date(2026, 11, 1), datetime.datetime(2026, 11, 1, 10), "x")
SPARK_SCHEMA = "i int, l long, f float, d double, b boolean, dd date, t timestamp, s string"
POLARS_SCHEMA = {
    "i": pl.Int32,
    "l": pl.Int64,
    "f": pl.Float32,
    "d": pl.Float64,
    "b": pl.Boolean,
    "dd": pl.Date,
    "t": pl.Datetime("us"),
    "s": pl.String,
}


def _write(engine, root: Path, rows, source_file="f 1.csv"):
    if engine == "spark":
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        data = spark.createDataFrame(rows, SPARK_SCHEMA)
        return ParquetLakeWriter(normalize_spark_path(str(root))).write(
            data, "raw/t", run_id=RUN_ID, source_file=source_file, file_id=7
        )
    data = pl.DataFrame(rows, schema=POLARS_SCHEMA, orient="row")
    return PolarsLakeWriter(normalize_spark_path(str(root))).write(
        data, "raw/t", run_id=RUN_ID, source_file=source_file, file_id=7
    )


def _files(root: Path):
    return sorted(root.glob("raw/t/*/*/*.parquet"))


def test_p5_7_escaping_matches_spark(spark):
    escape = spark._jvm.org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
    for value in [chr(c) for c in range(1, 128)] + ["é ü 中   😀", RUN_ID]:
        assert escape_path_name(value) == escape(value), repr(value)


def test_p5_7_same_partitions_and_physical_schema_as_spark(spark_utc, tmp_path):
    assert _write("spark", tmp_path / "spark", [ROW] * 3) == 3
    assert _write("polars", tmp_path / "polars", [ROW] * 3) == 3
    spark_files, polars_files = _files(tmp_path / "spark"), _files(tmp_path / "polars")
    assert {f.parent.relative_to(tmp_path / "spark") for f in spark_files} == {
        f.parent.relative_to(tmp_path / "polars") for f in polars_files
    }
    assert polars_files[0].name.startswith("part-00000-")
    assert polars_files[0].name.endswith(".c000.snappy.parquet")

    def physical(path):
        text = str(pq.ParquetFile(path).schema).replace("spark_schema", "schema")
        return text.split("\n", 1)[1]  # sans la ligne qui porte l'adresse de l'objet

    for spark_file in spark_files:
        assert physical(polars_files[0]) == physical(spark_file)
        assert (
            pq.read_schema(polars_files[0]).remove_metadata()
            == pq.read_schema(spark_file).remove_metadata()
        )
    assert "int96" in physical(polars_files[0])


def test_p5_7_overwrite_replaces_and_zero_rows_removes(tmp_path):
    assert _write("polars", tmp_path, [ROW] * 3) == 3
    assert _write("polars", tmp_path, [ROW]) == 1
    (only,) = _files(tmp_path)
    assert pq.read_table(only).num_rows == 1
    assert _write("polars", tmp_path, []) == 0
    assert not list((tmp_path / "raw" / "t").iterdir())  # ni partition ni run vide
    assert not list((tmp_path / "raw" / "t").glob(".polars-*"))


def test_p5_7_other_files_of_the_run_are_kept(tmp_path):
    _write("polars", tmp_path, [ROW], source_file="a.csv")
    _write("polars", tmp_path, [ROW], source_file="b.csv")
    _write("polars", tmp_path, [], source_file="a.csv")
    assert [f.parent.name for f in _files(tmp_path)] == ["lake_source_file=b.csv"]


def test_p5_7_refused_writes(tmp_path):
    writer = PolarsLakeWriter(str(tmp_path))
    clash = pl.DataFrame({"Lake_File_Id": [1]})
    with pytest.raises(ValueError, match="colonne technique"):
        writer.write(clash, "raw/t", run_id="r", source_file="f", file_id=1)
    with pytest.raises(ValueError, match="overwrite"):
        writer.write(clash, "raw/t", "append", run_id="r", source_file="f", file_id=1)
    with pytest.raises(ValueError, match="sortir|interdit|invalide"):
        writer.location("../x")
    with pytest.raises(NotImplementedError):
        PolarsLakeWriter("s3://bucket/lake")
