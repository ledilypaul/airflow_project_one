"""P5-7 (lecture) : le reader Polars donne les mêmes valeurs que Spark.

Chaque fichier est lu par `CSVReader` / `ParquetReader` / `ExcelReader` (Spark)
et par `pipeline.engine.polars.reader.read_file`, avec le même `columns_type`,
puis comparé valeur par valeur. Spark tourne dans une session dédiée en UTC
(`newSession` : la configuration de la session partagée ne change pas), le
fuseau où il prend un horodatage sans décalage.
"""

import datetime
import math
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest
import yaml

from pipeline.config import ConfigError, SourceConfig
from pipeline.config.reader_options import SPARK_TO_POLARS_OPTIONS
from pipeline.engine.polars.reader import read_file
from pipeline.file_reader.csv_reader import CSVReader
from pipeline.file_reader.excel_reader import ExcelReader
from pipeline.file_reader.parquet_reader import ParquetReader

PROJECT_ROOT = Path(__file__).resolve().parents[3]
STORAGE = {"format": "parquet", "mode": "overwrite", "raw_path": "raw/"}

VALUES = {
    "int": ["1", " 2", "3 ", "1.0", "abc", "", "2147483648", "+4", "-5", "007", "-2147483648"],
    "long": ["9223372036854775807", "9223372036854775808", " 7", "1.5", "-1"],
    "double": [
        *("1.5", " 2.5", "2.5 ", "NaN", " NaN", "nan", "Infinity", "+Infinity", "-Infinity"),
        *("Inf", "-Inf", "inf", "1e3", "1E-2", "abc", ".5", "1.", "+1.5", "1d", "1F", "0x10"),
        *("1,5", "", "-0.0", "1e400"),
    ],
    "float": ["1.5", "0.1", "3.4e39", "1e-50", "NaN", "16777217", " 2.5f", "x"],
    "boolean": ["true", "TRUE", "True", "false", "1", "0", "yes", "t", " true", "true ", ""],
    "date": [
        *("2026-11-01", "2026-1-5", "2026-11-1", "2026/11/01", "2026-11-01T10:00:00"),
        *("2026-11-01 10:00", "2026-02-30", "2024-02-29", "20261101", " 2026-11-01 "),
        *("2026-11", "2026", "+2026-11-01", "2026-13-01", "2026-11-01x", ""),
    ],
    "timestamp": [
        *("2026-11-01 10:00:00", "2026-11-01T10:00:00", "2026-11-01", "2026-11"),
        *("2026-11-01 10:00:00.123", "2026-11-01 10:00:00.1234567", "2026-11-01 10:00:00."),
        *("2026-11-01T10:00:00Z", "2026-11-01T10:00:00+02:00", "2026-11-01T10:00:00-05:30"),
        *("2026-11-01T10:00:00+0200", "2026-11-01T10:00:00+2", "2026-11-01 10:00:00 +01:00"),
        *("2026-11-01 10:00", "2026-11-01 10", "2026-1-5 1:2:3", "2026-11-01 24:00:00"),
        *("2026-11-01T10:00:00+19:00", "abc", "", " 2026-11-01 10:00:00 "),
    ],
    "string": ["a", " b ", "", '""', "NULL", "null", '"x,y"'],
}


def _config(engine: str, options: dict, columns_type=None, file_format="csv") -> SourceConfig:
    reader = {"options": options}
    if columns_type is not None:
        reader["columns_type"] = columns_type
    return SourceConfig.model_validate(
        {
            "source": {"name": "t", "format": file_format, "patterns": ["*"], "engine": engine},
            "reader": reader,
            "storage": STORAGE,
        }
    )


@pytest.fixture(scope="module")
def spark_utc(spark):
    session = spark.newSession()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    return session


def _norm(value):
    """Une valeur comparable entre moteurs (horodatages en UTC, flottants exacts)."""
    if isinstance(value, float):
        return "nan" if math.isnan(value) else repr(value)
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    return value


def _spark_rows(frame):
    from pyspark.sql import functions as F

    columns = [
        F.date_format(c, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").alias(c) if t == "timestamp" else F.col(c)
        for c, t in frame.dtypes
    ]
    rows = [tuple(_norm(v) for v in row) for row in frame.select(columns).collect()]
    return [tuple(_iso_micro(v) for v in row) for row in rows]


def _iso_micro(value):
    return value.replace(".000000", "") if isinstance(value, str) and "T" in value else value


def _polars_rows(frame: pl.LazyFrame):
    return [tuple(_iso_micro(_norm(v)) for v in row) for row in frame.collect().rows()]


@pytest.mark.parametrize("col_type", list(VALUES))
def test_p5_7_csv_conversion_matches_spark(spark_utc, tmp_path, col_type):
    path = tmp_path / f"{col_type}.csv"
    lines = [
        f'{i},"{v}"' if "," in v and not v.startswith('"') else f"{i},{v}"
        for i, v in enumerate(VALUES[col_type])
    ]
    path.write_text("i,c\n" + "\n".join(lines) + "\n", encoding="utf-8")
    columns_type = {"i": "int", "c": col_type}

    spark_frame = CSVReader(spark_utc).read(
        str(path), _config("spark", {"header": True}, columns_type)
    )
    expected = _spark_rows(spark_frame.orderBy("i"))
    got = _polars_rows(read_file(str(path), _config("polars", {}, columns_type)).sort("i"))

    diffs = [
        (VALUES[col_type][r[0]], r[1], g[1]) for r, g in zip(expected, got, strict=True) if r != g
    ]
    assert not diffs, diffs


def test_p5_7_polars_dtypes_follow_the_neutral_types(tmp_path):
    path = tmp_path / "t.csv"
    path.write_text("a,b,c,d,e,f,g,h\n1,2,3,4,true,2026-11-01,2026-11-01 10:00:00,x\n")
    types = ["int", "long", "float", "double", "boolean", "date", "timestamp", "string"]
    frame = read_file(str(path), _config("polars", {}, dict(zip("abcdefgh", types, strict=True))))
    assert list(frame.collect_schema().values()) == [
        *(pl.Int32, pl.Int64, pl.Float32, pl.Float64, pl.Boolean, pl.Date),
        *(pl.Datetime("us"), pl.String),
    ]


@pytest.mark.parametrize("example", ["imdb", "orders"])
def test_p5_7_example_sources_read_the_same(spark_utc, example):
    """Les sources d'exemple, traduites en options Polars, donnent les mêmes lignes."""
    raw = yaml.safe_load((PROJECT_ROOT / f"examples/config/sources/{example}.yaml").read_text())
    data = next((PROJECT_ROOT / "examples" / "data").glob(f"{example}_*.csv"))
    spark_config = SourceConfig.model_validate(raw)
    mapping = SPARK_TO_POLARS_OPTIONS["csv"]
    options = {
        mapping[k]: v
        for k, v in raw["reader"]["options"].items()
        if k in mapping and k != "inferSchema"
    }
    polars_config = _config("polars", options, raw["reader"]["columns_type"])

    expected = sorted(_spark_rows(CSVReader(spark_utc).read(str(data), spark_config)), key=str)
    got = sorted(_polars_rows(read_file(str(data), polars_config)), key=str)
    assert got == expected and got


def test_p5_7_header_is_checked_against_columns_type(tmp_path):
    path = tmp_path / "t.csv"
    path.write_text("ID,Name\n1,a\n")
    frame = read_file(str(path), _config("polars", {}, {"id": "int", "name": "string"}))
    assert frame.collect().rows(named=True) == [{"id": 1, "name": "a"}]  # casse ignorée

    for columns_type in ({"id": "int", "label": "string"}, {"id": "int"}):
        with pytest.raises(ValueError, match="différentes de reader.columns_type"):
            read_file(str(path), _config("polars", {}, columns_type))


def test_p5_7_without_header_columns_are_positional(tmp_path):
    path = tmp_path / "t.csv"
    path.write_text("1,a\n2,b\n")
    config = _config("polars", {"has_header": False}, {"id": "long", "name": "string"})
    assert read_file(str(path), config).collect().rows() == [(1, "a"), (2, "b")]
    with pytest.raises(ValueError):
        read_file(str(path), _config("polars", {"has_header": False}, {"id": "long"}))


def test_p5_7_empty_file(tmp_path):
    path = tmp_path / "t.csv"
    path.write_text("")
    columns_type = {"id": "int", "at": "timestamp"}
    with pytest.raises(pl.exceptions.NoDataError):
        read_file(str(path), _config("polars", {}, columns_type)).collect()
    frame = read_file(str(path), _config("polars", {"raise_if_empty": False}, columns_type))
    assert frame.collect().schema == pl.Schema({"id": pl.Int32, "at": pl.Datetime("us")})


def test_p5_7_without_columns_type_polars_options_apply(tmp_path):
    path = tmp_path / "t.csv"
    path.write_text("a;b\n1;x\n")
    frame = read_file(str(path), _config("polars", {"separator": ";"})).collect()
    assert frame.rows() == [(1, "x")] and frame.schema["a"] == pl.Int64


def test_p5_7_parquet_reads_the_same(spark, tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "t.parquet"
    table = pa.table({"id": [1, 2], "v": [1.5, None], "d": [datetime.date(2026, 11, 1), None]})
    pq.write_table(table, path)
    spark_rows = ParquetReader(spark).read(str(path), _config("spark", {}, file_format="parquet"))
    got = read_file(str(path), _config("polars", {}, file_format="parquet")).collect()
    assert [tuple(r) for r in spark_rows.collect()] == got.rows()


def test_p5_7_xlsx_reads_the_same(spark, tmp_path):
    import openpyxl

    path = tmp_path / "t.xlsx"
    book = openpyxl.Workbook()
    for row in (["id", "name", "amount"], [1, "a", 1.5], [2, "b", 2.25]):
        book.active.append(row)
    book.save(path)
    spark_rows = ExcelReader(spark).read(str(path), _config("spark", {}, file_format="xlsx"))
    got = read_file(str(path), _config("polars", {}, file_format="xlsx")).collect()
    assert [tuple(r) for r in spark_rows.collect()] == got.rows()


def test_p5_7_spark_config_is_refused(tmp_path):
    with pytest.raises(ConfigError, match="pas polars"):
        read_file(str(tmp_path / "t.csv"), _config("spark", {"header": True}))


def test_p5_7_polars_reader_does_not_load_spark():
    script = (
        "import sys, pipeline.engine.polars.reader; "
        "sys.exit('pyspark' in sys.modules or 'airflow' in sys.modules)"
    )
    assert subprocess.run([sys.executable, "-c", script], cwd=PROJECT_ROOT).returncode == 0
