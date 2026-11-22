"""
P2-1 (constat M4): every reader consumes the same validated `SourceConfig`,
and the declared options really reach Spark / pandas.

Uses the session-wide `spark` fixture of tests/unit/conftest.py.
"""

from pathlib import Path

import pytest
import yaml
from pyspark.sql import DataFrame

from pipeline.config import SourceConfig, SourceRegistry, load_source_config
from pipeline.file_reader.base_reader import BaseFileReader
from pipeline.file_reader.csv_reader import CSVReader
from pipeline.file_reader.excel_reader import ExcelReader
from pipeline.file_reader.parquet_reader import ParquetReader

REPO_CONFIG_ROOT = Path(__file__).resolve().parent.parent.parent.parent / "config"


# P2-2: `storage` is mandatory in every reading config; these tests only read.
STORAGE = {"format": "parquet", "mode": "overwrite", "raw_path": "raw/"}


def _declared_format(config_file: Path) -> str | None:
    """`source.format` read without validation, only to pick the files to
    parametrize on: an invalid YAML must fail its own test, not the collection."""
    raw = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    source = raw.get("source") if isinstance(raw, dict) else None
    return source.get("format") if isinstance(source, dict) else None


# Every csv/txt reading config of the repository. parquet and xlsx have no
# `columns_type` (rejected for them) and are covered by their reader tests.
REPO_CSV_CONFIGS = [
    path
    for tier in ("sources", "defaults")
    for path in sorted((REPO_CONFIG_ROOT / tier).glob("*.yaml"))
    if _declared_format(path) in {"csv", "txt"}
]

# One value per supported `columns_type` type, written as text in the file:
# each must parse into its declared type (no null in the row read back).
SAMPLE_VALUES = {
    "string": "x",
    "int": "1",
    "integer": "1",
    "float": "1.5",
    "double": "1.5",
    "boolean": "true",
    "date": "2024-01-31",
    "timestamp": "2024-01-31 12:00:00",
}


def _source(fmt="csv", options=None, columns_type=None, name="test", patterns=("*",)):
    reader = {"options": options or {}}
    if columns_type is not None:
        reader["columns_type"] = columns_type
    return SourceConfig.model_validate(
        {
            "source": {"name": name, "format": fmt, "patterns": list(patterns)},
            "reader": reader,
            "storage": STORAGE,
        }
    )


# ---------------------------------------------------------------------------
# CSVReader
# ---------------------------------------------------------------------------


def test_p2_1_csv_reader_passes_encoding(spark, tmp_path):
    """M4: CSVReader used to ignore `encoding` (always read as UTF-8)."""
    csv_file = tmp_path / "latin1.csv"
    csv_file.write_bytes("name\ncafé\n".encode("latin-1"))

    latin1 = _source(options={"header": True, "encoding": "ISO-8859-1"})
    utf8 = _source(options={"header": True, "encoding": "UTF-8"})

    assert CSVReader(spark).read(str(csv_file), latin1).first()["name"] == "café"
    # Sanity check: the option is what makes the difference.
    assert CSVReader(spark).read(str(csv_file), utf8).first()["name"] != "café"


def test_p2_1_csv_reader_passes_every_option(spark, tmp_path):
    csv_file = tmp_path / "semicolon.csv"
    csv_file.write_text("a;b\n1;NA\n")

    config = _source(options={"header": True, "delimiter": ";", "nullValue": "NA"})
    row = CSVReader(spark).read(str(csv_file), config).first()

    assert row["a"] == "1"
    assert row["b"] is None


def test_p2_1_csv_reader_uses_columns_type_as_explicit_schema(spark, tmp_path):
    """The explicit schema comes from `reader.columns_type` (it used to live
    inside `reader.reader_options` in imdb_config.yaml) and wins over
    inference: "007" stays a string."""
    csv_file = tmp_path / "codes.csv"
    csv_file.write_text("id,code\n1,007\n")

    config = _source(
        options={"header": True, "inferSchema": True},
        columns_type={"id": "int", "code": "string"},
    )
    df = CSVReader(spark).read(str(csv_file), config)

    assert df.dtypes == [("id", "int"), ("code", "string")]
    assert df.first()["code"] == "007"


# ---------------------------------------------------------------------------
# ExcelReader / ParquetReader
# ---------------------------------------------------------------------------


def test_p2_1_excel_reader_reads_the_single_shape(spark, tmp_path):
    pytest.importorskip("openpyxl")
    import pandas as pd

    xlsx_file = tmp_path / "data.xlsx"
    pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}).to_excel(xlsx_file, index=False)

    config = _source(fmt="xlsx", options={"header": 0, "dtype": "str"})
    df = ExcelReader(spark).read(str(xlsx_file), config)

    assert isinstance(df, DataFrame)
    assert df.count() == 2
    # `dtype: str` really reached read_excel: the ids are strings.
    assert dict(df.dtypes)["id"] == "string"


def test_p2_1_parquet_reader_reads_the_single_shape(spark, tmp_path):
    """`mergeSchema` really reaches Spark: without it only one of the two
    schemas would be read."""
    root = tmp_path / "events.parquet"
    spark.createDataFrame([(1, "a")], ["id", "a"]).write.parquet(str(root / "k=1"))
    spark.createDataFrame([(2, "b")], ["id", "b"]).write.parquet(str(root / "k=2"))

    config = _source(fmt="parquet", options={"mergeSchema": True})
    df = ParquetReader(spark).read(str(root), config)

    assert sorted(df.columns) == ["a", "b", "id", "k"]
    assert df.count() == 2


# ---------------------------------------------------------------------------
# BaseFileReader end to end, with a small config directory
# ---------------------------------------------------------------------------


@pytest.fixture
def config_root(tmp_path):
    root = tmp_path / "config"
    (root / "sources").mkdir(parents=True)
    (root / "defaults").mkdir()
    (root / "sources" / "orders.yaml").write_text(
        yaml.safe_dump(
            {
                "source": {"name": "orders", "format": "csv", "patterns": ["orders_*"]},
                "reader": {
                    "options": {"header": True, "delimiter": ";"},
                    "columns_type": {"id": "int", "amount": "double"},
                },
                "storage": STORAGE,
            },
            # Keep the YAML order: columns_type is positional.
            sort_keys=False,
        )
    )
    (root / "defaults" / "csv.yaml").write_text(
        yaml.safe_dump(
            {
                "source": {"name": "default_csv", "format": "csv", "patterns": ["*.csv"]},
                "reader": {"options": {"header": True, "inferSchema": False}},
                "storage": STORAGE,
            }
        )
    )
    return root


def test_p2_1_base_reader_reads_with_the_dedicated_config(spark, tmp_path, config_root):
    data_file = tmp_path / "ORDERS_x.csv"
    data_file.write_text("id;amount\n1;9.5\n2;3.0\n")

    df = BaseFileReader(spark, config_path=config_root).read_file(str(data_file))

    assert df.dtypes == [("id", "int"), ("amount", "double")]
    assert df.count() == 2


def test_p2_1_base_reader_falls_back_on_the_default(spark, tmp_path, config_root):
    data_file = tmp_path / "random.csv"
    data_file.write_text("id,amount\n1,9.5\n")

    df = BaseFileReader(spark, config_path=config_root).read_file(str(data_file))

    assert df.dtypes == [("id", "string"), ("amount", "string")]


def test_p2_1_base_reader_unmatched_file_is_an_explicit_error(spark, tmp_path, config_root):
    from pipeline.config import NoMatchingConfigError

    reader = BaseFileReader(spark, config_path=config_root)
    with pytest.raises(NoMatchingConfigError, match="notes.parquet"):
        reader.read_file(str(tmp_path / "notes.parquet"))


def test_p2_1_base_reader_uses_the_injected_registry(spark, tmp_path, config_root):
    """C-2: the Spark job loads the registry before starting Spark and hands
    it over, instead of letting BaseFileReader load it a second time."""
    registry = SourceRegistry.from_directory(config_root)
    data_file = tmp_path / "orders_1.csv"
    data_file.write_text("id;amount\n1;9.5\n")

    reader = BaseFileReader(spark, registry=registry)

    assert reader.registry is registry
    assert reader.read_file(str(data_file)).dtypes == [("id", "int"), ("amount", "double")]
    # Either a directory to load or an already loaded registry, not both.
    with pytest.raises(ValueError, match="registry"):
        BaseFileReader(spark, config_path=config_root, registry=registry)


# ---------------------------------------------------------------------------
# C-1: every csv/txt config of the repository really reads a file
# ---------------------------------------------------------------------------


def _synthetic_file_name(pattern: str) -> str:
    return pattern.replace("*", "sample").replace("?", "s")


def test_p2_1_sample_values_cover_every_column_type():
    from pipeline.config.types import SUPPORTED_COLUMN_TYPES

    assert set(SAMPLE_VALUES) == set(SUPPORTED_COLUMN_TYPES)


def test_p2_1_repo_has_csv_configs_to_read():
    assert any((REPO_CONFIG_ROOT / "sources").glob("*.yaml"))
    assert {p.name for p in REPO_CSV_CONFIGS} >= {"imdb.yaml", "orders.yaml", "csv.yaml"}


@pytest.mark.parametrize("config_file", REPO_CSV_CONFIGS, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_p2_1_every_repo_config_reads_a_synthetic_file(spark, tmp_path, config_file):
    """C-1, and the intent of B2 ("IMDB files no longer crash"): building the
    `StructType` of a config is not enough. `genre_list: array` built a valid
    `ArrayType(StringType())` schema, then the CSV source refused it at read
    time (`UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE`): no IMDB CSV was readable.

    For each csv/txt YAML of the repository, write a one-row file consistent
    with it (header from `columns_type`, its delimiter, its encoding), named
    after its first pattern, and read it through the real path of the job:
    `BaseFileReader` with the repository registry, then the real reader.
    """
    config = load_source_config(config_file)
    options = config.reader.options
    columns_type = config.reader.columns_type or {"col_a": "string", "col_b": "int"}
    columns = list(columns_type)
    sep = options.get("sep", options.get("delimiter", ","))
    encoding = options.get("encoding", options.get("charset", "UTF-8"))

    data_file = tmp_path / _synthetic_file_name(config.source.patterns[0])
    lines = [sep.join(columns)] if options.get("header") else []
    lines.append(sep.join(SAMPLE_VALUES[columns_type[column]] for column in columns))
    data_file.write_bytes(("\n".join(lines) + "\n").encode(encoding))

    reader = BaseFileReader(spark)
    assert reader.get_config(str(data_file)).source.name == config.source.name

    df = reader.read_file(str(data_file))

    assert df.count() == 1
    assert len(df.columns) == len(columns)
    if config.reader.columns_type:
        assert df.columns == columns
    # `.count()` alone may skip parsing (CSV column pruning): read the values
    # too. A null means a sample value did not parse into its declared type.
    row = df.first().asDict()
    assert None not in row.values(), row
