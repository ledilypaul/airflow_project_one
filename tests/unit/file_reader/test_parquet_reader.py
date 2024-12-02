import pytest
from pyspark.sql import SparkSession
from pipeline.file_reader.parquet_reader import ParquetReader
from pathlib import Path
import yaml

@pytest.fixture(scope="module") 
def spark(): 
    return SparkSession.builder.appName("pytest").master("local[*]").getOrCreate()

@pytest.fixture
def parquet_reader(spark):
    return ParquetReader()


@pytest.fixture
def test_parquet_file(tmp_path,spark):
    test_file = tmp_path / "file1.parquet"
    data = [(1, "Alice"), (2, "Bob")]
    schema = ["id", "name"]
    df = spark.createDataFrame(data, schema=schema)
    df.write.parquet(str(test_file))
    return test_file

def test_parquet_reader_with_option(parquet_reader,test_parquet_file):
    config = {
        parquet_reader : {
            "compression": "snappy",
            "partition_column": "date",
            "infer_schema": "false" 
    }
    }
        
    result = parquet_reader.read(str(test_parquet_file),config)
    assert result.count() == 2