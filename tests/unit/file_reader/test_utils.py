# tests/unit/utils/test_functions_utils.py

from pathlib import Path

import pytest
import yaml
from pyspark.sql.types import ArrayType, BooleanType, DateType, FloatType, IntegerType, StringType

from pipeline.file_reader.utils import get_spark_type

CONFIG_DIR = Path(__file__).resolve().parent.parent.parent.parent / "config"


def test_get_spark_type_valid():
    assert isinstance(get_spark_type("string"), StringType)
    assert isinstance(get_spark_type("int"), IntegerType)
    assert isinstance(get_spark_type("float"), FloatType)
    assert isinstance(get_spark_type("boolean"), BooleanType)
    assert isinstance(get_spark_type("date"), DateType)


def test_get_spark_type_case_insensitive():
    assert isinstance(get_spark_type(" STRING "), StringType)


def test_get_spark_type_invalid():
    with pytest.raises(ValueError):
        get_spark_type("unknown_type")


def test_b2_array_type_is_supported():
    """
    Regression test for B2 (docs/audit/2026-11-05-baseline.md):
    config/imdb_config.yaml declares `genre_list: array`, which used to
    raise `ValueError: Type non reconnu dans YAML : 'array'` because
    `get_spark_type()` had no mapping for it.
    """
    spark_type = get_spark_type("array")
    assert spark_type == ArrayType(StringType())


def _iter_columns_type_blocks(config):
    """Yield every `columns_type` mapping nested anywhere in a config dict."""
    if isinstance(config, dict):
        if isinstance(config.get("columns_type"), dict):
            yield config["columns_type"]
        for value in config.values():
            yield from _iter_columns_type_blocks(value)


def test_b2_every_config_yaml_schema_builds():
    """
    Regression test for B2: every `columns_type` declared in config/*.yaml
    (not just imdb_config.yaml) must build a Spark schema without raising.
    """
    config_files = sorted(CONFIG_DIR.glob("*.yaml"))
    assert config_files, f"no config found in {CONFIG_DIR}"

    checked_any_columns_type = False
    for config_file in config_files:
        config = yaml.safe_load(config_file.read_text())
        for columns_type in _iter_columns_type_blocks(config):
            if not columns_type:
                continue
            checked_any_columns_type = True
            for _column_name, col_type in columns_type.items():
                get_spark_type(col_type)  # must not raise

    assert checked_any_columns_type, "no non-empty columns_type block found to exercise B2"
