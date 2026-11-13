import pytest

from pipeline.config import SourceConfig
from pipeline.file_reader.parquet_reader import ParquetReader

# P2-2: `storage` is mandatory in every reading config; these tests only read.
STORAGE = {"format": "parquet", "mode": "overwrite", "raw_path": "raw/"}


@pytest.fixture
def parquet_reader(spark):
    return ParquetReader(spark)


@pytest.fixture
def test_parquet_file(tmp_path, spark):
    test_file = tmp_path / "file1.parquet"
    data = [(1, "Alice"), (2, "Bob")]
    schema = ["id", "name"]
    df = spark.createDataFrame(data, schema=schema)
    df.write.parquet(str(test_file))
    return test_file


def test_parquet_reader(parquet_reader, test_parquet_file):
    config = SourceConfig.model_validate(
        {
            "source": {"name": "test", "format": "parquet", "patterns": ["*.parquet"]},
            "storage": STORAGE,
        }
    )
    result = parquet_reader.read(str(test_parquet_file), config)
    assert result.count() == 2
