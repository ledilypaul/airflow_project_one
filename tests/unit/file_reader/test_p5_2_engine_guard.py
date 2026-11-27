"""P5-2 — the Spark job refuses a source of another engine instead of reading it with Spark."""

import pytest

from pipeline.config import ConfigError, SourceConfig, SourceRegistry
from pipeline.file_reader.base_reader import BaseFileReader

STORAGE = {"format": "parquet", "mode": "overwrite", "raw_path": "raw/"}


def _config(engine: str, options: dict) -> SourceConfig:
    return SourceConfig.model_validate(
        {
            "source": {"name": "t", "format": "csv", "patterns": ["*.csv"], "engine": engine},
            "reader": {"options": options},
            "storage": STORAGE,
        }
    )


def test_p5_2_spark_job_refuses_a_polars_source(spark, tmp_path):
    data_file = tmp_path / "a.csv"
    data_file.write_text("a;b\n1;2\n", encoding="utf-8")
    config = _config("polars", {"separator": ";"})
    reader = BaseFileReader(spark, registry=SourceRegistry([], []))

    with pytest.raises(ConfigError, match="moteur 'polars'"):
        reader.read_file(str(data_file), config=config)


def test_p5_2_spark_source_is_still_read(spark, tmp_path):
    data_file = tmp_path / "a.csv"
    data_file.write_text("a;b\n1;2\n", encoding="utf-8")
    config = _config("spark", {"delimiter": ";", "header": True})
    reader = BaseFileReader(spark, registry=SourceRegistry([], []))

    assert reader.read_file(str(data_file), config=config).columns == ["a", "b"]
