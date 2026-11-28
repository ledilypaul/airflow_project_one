"""
P5-2 — the `long` type, end to end with Spark, and the names of the neutral
types table checked against what Spark really writes.

A CSV with one column of each neutral type (and a `long` beyond int32) is
read by the real CSVReader and written by the real ParquetLakeWriter. The
Parquet files are then read back by pyarrow (`ColumnType.arrow`) and by
DuckDB (`ColumnType.duckdb`, the type a dbt contract names), from the
transform environment: skipped without it, unless REQUIRE_DBT=1.
"""

import json
import os
import subprocess
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pipeline.config import SourceConfig
from pipeline.config.types import NEUTRAL_TYPES
from pipeline.file_reader.csv_reader import CSVReader
from storage.parquet_lake_writer import ParquetLakeWriter, latest_snapshot, resolve_lake_root

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DBT_PYTHON = PROJECT_ROOT / "transform" / ".venv" / "bin" / "python"

BIG = 3_000_000_000  # > 2**31 - 1
SAMPLE = {
    "string": "x",
    "int": "7",
    "long": str(BIG),
    "float": "1.5",
    "double": "2.25",
    "boolean": "true",
    "date": "2026-11-24",
    "timestamp": "2026-11-24 12:30:00",
}
RUN_ID, SOURCE_FILE = "run_p5_2", "types.csv"


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory) -> str:
    lake_root = resolve_lake_root({"LAKE_ROOT": str(tmp_path_factory.mktemp("lake"))})
    data_file = tmp_path_factory.mktemp("data") / SOURCE_FILE
    data_file.write_text(
        ",".join(SAMPLE) + "\n" + ",".join(SAMPLE.values()) + "\n", encoding="utf-8"
    )
    config = SourceConfig.model_validate(
        {
            "source": {"name": "types", "format": "csv", "patterns": ["*.csv"]},
            "reader": {
                "options": {"header": True, "enforceSchema": False},
                "columns_type": {name: name for name in SAMPLE},
            },
            "storage": {
                "format": "parquet",
                "mode": "overwrite",
                "raw_path": "raw/",
                "staging_path": "staging/",
            },
        }
    )
    frame = CSVReader(spark).read(str(data_file), config)
    ParquetLakeWriter(lake_root).write(
        frame, "staging/types", run_id=RUN_ID, source_file=SOURCE_FILE, file_id=1
    )
    return lake_root


def test_p5_2_long_keeps_a_value_beyond_int32_through_the_lake(spark, lake):
    current = latest_snapshot(spark.read.parquet(f"{lake}/staging/types"))
    assert current.schema["long"].dataType.simpleString() == "bigint"
    assert current.schema["int"].dataType.simpleString() == "int"
    assert current.select("long").first()["long"] == BIG


def test_p5_2_parquet_types_are_the_ones_of_the_neutral_table(lake):
    files = list(Path(lake.removeprefix("file://")).glob("staging/types/**/*.parquet"))
    assert len(files) == 1
    schema = pq.read_schema(files[0])
    for name, column_type in NEUTRAL_TYPES.items():
        assert str(schema.field(name).type) == column_type.arrow, name


def test_p5_2_duckdb_types_are_the_ones_of_the_neutral_table(lake):
    if not DBT_PYTHON.exists():
        message = f"{DBT_PYTHON} not found: run `make transform-sync`"
        if os.environ.get("REQUIRE_DBT") == "1":
            pytest.fail(message + " (REQUIRE_DBT=1: these tests must run)")
        pytest.skip(message)
    glob = f"{lake.removeprefix('file://')}/staging/types/*/*/*.parquet"
    columns = ", ".join(f'typeof("{name}") as "{name}"' for name in NEUTRAL_TYPES)
    script = "import duckdb, json, sys; print(json.dumps(duckdb.sql(sys.argv[1]).fetchone()))"
    sql = f"select {columns} from read_parquet('{glob}', hive_partitioning=true)"
    result = subprocess.run(
        [str(DBT_PYTHON), "-I", "-c", script, sql], capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, result.stderr
    assert dict(zip(NEUTRAL_TYPES, json.loads(result.stdout), strict=True)) == {
        name: column_type.duckdb for name, column_type in NEUTRAL_TYPES.items()
    }
