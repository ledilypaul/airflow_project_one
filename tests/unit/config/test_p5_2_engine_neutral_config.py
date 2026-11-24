"""
P5-2 — a reading config no longer depends on an engine.

- loading it imports neither PySpark nor Polars (fresh subprocess: the pytest
  process has already imported everything);
- the neutral types (`long` added) and the Spark schema of the examples are
  unchanged;
- `source.engine` / `PIPELINE_DEFAULT_ENGINE` decide which `reader.options`
  and which regex syntax are valid.
"""

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml
from pyspark.sql import types as T

from pipeline.config import ConfigError, SourceConfig, load_source_config
from pipeline.config.engine import DEFAULT_ENGINE_ENV
from pipeline.config.reader_options import (
    POLARS_READER_OPTIONS_BY_FORMAT,
    SPARK_TO_POLARS_OPTIONS,
)
from pipeline.config.types import NEUTRAL_TYPES, SUPPORTED_COLUMN_TYPES, resolve_column_type
from pipeline.engine.spark.types import build_schema, get_spark_type

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
EXAMPLES_SOURCES = PROJECT_ROOT / "examples" / "config" / "sources"
STORAGE = {
    "format": "parquet",
    "mode": "overwrite",
    "raw_path": "raw/",
    "staging_path": "staging/",
}


def _config(engine=None, fmt="csv", options=None, columns_type=None, checks=None) -> dict:
    source = {"name": "t", "format": fmt, "patterns": ["*"]}
    if engine:
        source["engine"] = engine
    raw = {"source": source, "reader": {"options": options or {}}, "storage": STORAGE}
    if columns_type:
        raw["reader"]["columns_type"] = columns_type
    if checks:
        raw["quality"] = {"checks": checks}
    return raw


def _errors(raw: dict) -> str:
    with pytest.raises(ValueError) as excinfo:
        SourceConfig.model_validate(raw)
    return str(excinfo.value)


@pytest.fixture(autouse=True)
def _no_engine_env(monkeypatch):
    monkeypatch.delenv(DEFAULT_ENGINE_ENV, raising=False)


# ---------------------------------------------------------------------------
# No engine imported by a config
# ---------------------------------------------------------------------------


def test_p5_2_loading_a_config_imports_no_engine():
    script = """
        import sys
        from pathlib import Path

        from pipeline.config import SourceRegistry

        SourceRegistry.from_directory(Path("examples/config"))
        loaded = sorted(m for m in sys.modules if m.split(".")[0] in {"pyspark", "polars"})
        print("OK" if not loaded else "engine modules loaded: " + ", ".join(loaded[:5]))
        sys.exit(1 if loaded else 0)
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"


# ---------------------------------------------------------------------------
# Neutral types
# ---------------------------------------------------------------------------


def test_p5_2_neutral_types_are_the_documented_ones():
    assert set(NEUTRAL_TYPES) == {
        "string",
        "int",
        "long",
        "float",
        "double",
        "boolean",
        "date",
        "timestamp",
    }
    assert SUPPORTED_COLUMN_TYPES == (*NEUTRAL_TYPES, "integer")


def test_p5_2_long_is_64_bits_and_int_stays_32_bits():
    assert isinstance(get_spark_type("long"), T.LongType)
    assert isinstance(get_spark_type("LONG "), T.LongType)
    assert isinstance(get_spark_type("int"), T.IntegerType)
    assert isinstance(get_spark_type("integer"), T.IntegerType)
    assert resolve_column_type("long").duckdb == "BIGINT"
    with pytest.raises(ValueError, match="bigint"):
        get_spark_type("bigint")


def test_p5_2_every_neutral_type_has_a_spark_class():
    for name, column_type in NEUTRAL_TYPES.items():
        assert isinstance(getattr(T, column_type.spark)(), T.DataType), name


def test_p5_2_categories_match_what_pyspark_said():
    """The `numeric` / `fractional` flags replace `NumericType` /
    `FractionalType` of PySpark: they must agree on every neutral type."""
    for name in NEUTRAL_TYPES:
        spark_type = get_spark_type(name)
        assert resolve_column_type(name).numeric == isinstance(spark_type, T.NumericType), name
        assert resolve_column_type(name).fractional == isinstance(spark_type, T.FractionalType), (
            name
        )


# What `build_schema` returned before P5-2 (class names written by hand).
LEGACY_SPARK_TYPES = {
    "string": T.StringType,
    "int": T.IntegerType,
    "integer": T.IntegerType,
    "float": T.FloatType,
    "double": T.DoubleType,
    "boolean": T.BooleanType,
    "date": T.DateType,
    "timestamp": T.TimestampType,
}


@pytest.mark.parametrize(
    "config_file", sorted(EXAMPLES_SOURCES.glob("*.yaml")), ids=lambda p: p.stem
)
def test_p5_2_example_spark_schemas_are_unchanged(config_file):
    columns_type = load_source_config(config_file).reader.columns_type
    assert columns_type, f"{config_file.name} declares no columns_type"
    expected = T.StructType(
        [
            T.StructField(name, LEGACY_SPARK_TYPES[col_type.strip().lower()](), True)
            for name, col_type in columns_type.items()
        ]
    )
    assert build_schema(columns_type) == expected


def test_p5_2_range_on_long_is_allowed_and_on_string_is_not():
    check = {"type": "range", "column": "id", "min": 0}
    SourceConfig.model_validate(_config(columns_type={"id": "long"}, checks=[check]))
    assert "range exige un type numérique" in _errors(
        _config(columns_type={"id": "string"}, checks=[check])
    )


def test_p5_2_accepted_values_is_refused_on_fractional_types_only():
    check = {"type": "accepted_values", "column": "c", "values": ["1"]}
    SourceConfig.model_validate(_config(columns_type={"c": "long"}, checks=[check]))
    for fractional in ("float", "double"):
        assert "accepted_values compare la forme texte" in _errors(
            _config(columns_type={"c": fractional}, checks=[check])
        )


# ---------------------------------------------------------------------------
# source.engine and PIPELINE_DEFAULT_ENGINE
# ---------------------------------------------------------------------------


def test_p5_2_engine_defaults_to_spark(monkeypatch):
    assert SourceConfig.model_validate(_config()).engine == "spark"
    monkeypatch.setenv(DEFAULT_ENGINE_ENV, "polars")
    assert SourceConfig.model_validate(_config()).engine == "polars"


def test_p5_2_source_engine_wins_over_the_environment(monkeypatch):
    monkeypatch.setenv(DEFAULT_ENGINE_ENV, "polars")
    assert SourceConfig.model_validate(_config(engine="spark")).engine == "spark"


def test_p5_2_unknown_engine_is_refused():
    assert "source.engine" in _errors(_config(engine="duckdb"))


def test_p5_2_invalid_default_engine_variable_is_an_explicit_error(monkeypatch, tmp_path):
    monkeypatch.setenv(DEFAULT_ENGINE_ENV, "Polars ")
    assert "moteur inconnu" in _errors(_config())
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(_config()), encoding="utf-8")
    with pytest.raises(ConfigError, match="moteur inconnu"):
        load_source_config(path)


def test_p5_2_invalid_default_engine_does_not_hide_other_errors_in_load(monkeypatch, tmp_path):
    """`problems_of_valid_sections` reads the engine too: it must not raise a bare ValueError."""
    monkeypatch.setenv(DEFAULT_ENGINE_ENV, "nope")
    raw = _config(checks=[{"type": "not_null"}])  # field error in `quality`
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    with pytest.raises(ConfigError, match="moteur inconnu"):
        load_source_config(path)


# ---------------------------------------------------------------------------
# reader.options by (engine, format)
# ---------------------------------------------------------------------------


def test_p5_2_spark_config_is_unchanged_by_the_engine_field():
    SourceConfig.model_validate(_config(options={"header": True, "delimiter": ","}))
    SourceConfig.model_validate(_config(engine="spark", options={"header": True}))
    assert "infer_schema" in _errors(_config(options={"infer_schema": True}))


@pytest.mark.parametrize(
    ("option", "value", "equivalent"),
    [
        ("delimiter", ",", "separator"),
        ("sep", ",", "separator"),
        ("header", True, "has_header"),
        ("quote", '"', "quote_char"),
        ("comment", "#", "comment_prefix"),
        ("nullValue", "NA", "null_values"),
        ("inferSchema", True, "infer_schema"),
        ("lineSep", "\n", "eol_char"),
    ],
)
def test_p5_2_spark_option_on_a_polars_source_names_the_polars_one(option, value, equivalent):
    message = _errors(_config(engine="polars", options={option: value}))
    assert f"reader.options.{option}" in message
    assert f"utiliser '{equivalent}'" in message


def test_p5_2_spark_option_without_polars_equivalent_says_so():
    message = _errors(_config(engine="polars", options={"multiLine": True}))
    assert "pas d'équivalent Polars établi" in message


def test_p5_2_excel_pandas_option_on_a_polars_source():
    assert "utiliser 'has_header'" in _errors(
        _config(engine="polars", fmt="xlsx", options={"header": 0})
    )
    SourceConfig.model_validate(
        _config(engine="polars", fmt="xlsx", options={"sheet_name": "A", "has_header": True})
    )


def test_p5_2_polars_option_on_a_spark_source_is_refused():
    message = _errors(_config(engine="spark", options={"separator": ";"}))
    assert "option non supportée pour le format 'csv'" in message
    assert "moteur" not in message.split("separator", 1)[1].splitlines()[0]


def test_p5_2_polars_csv_and_parquet_options_are_validated():
    SourceConfig.model_validate(
        _config(
            engine="polars",
            options={
                "separator": ";",
                "has_header": True,
                "skip_rows": 1,
                "null_values": ["NA", ""],
                "infer_schema_length": None,
                "encoding": "utf8",
            },
        )
    )
    SourceConfig.model_validate(
        _config(engine="polars", fmt="parquet", options={"hive_partitioning": False})
    )
    assert "reader.options.has_header: doit être un booléen" in _errors(
        _config(engine="polars", options={"has_header": "yes"})
    )
    assert "reader.options.skip_rows: doit être un entier" in _errors(
        _config(engine="polars", options={"skip_rows": True})
    )
    assert "valeur scalaire" in _errors(_config(engine="polars", options={"separator": {"a": 1}}))
    assert "vouliez-vous dire 'separator'" in _errors(
        _config(engine="polars", options={"separtor": ";"})
    )


def test_p5_2_polars_encoding_accepts_only_what_scan_csv_accepts():
    message = _errors(_config(engine="polars", options={"encoding": "UTF-8"}))
    assert "Polars n'accepte que 'utf8', 'utf8-lossy'" in message


def test_p5_2_polars_has_no_spark_alias_rule():
    """`sep` / `delimiter` are aliases for Spark only; Polars has one name."""
    assert "deux noms de la même option Spark" in _errors(
        _config(options={"sep": ",", "delimiter": ","})
    )


def test_p5_2_equivalences_target_real_polars_options():
    for fmt, mapping in SPARK_TO_POLARS_OPTIONS.items():
        for spark_name, polars_name in mapping.items():
            assert polars_name in POLARS_READER_OPTIONS_BY_FORMAT[fmt], (fmt, spark_name)
