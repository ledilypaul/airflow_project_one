import os
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from pipeline.file_reader.base_reader import BaseFileReader
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()

@pytest.fixture
def base_reader(spark,config_dir):
    """Fixture pour initialiser un lecteur de base."""
    return BaseFileReader(spark, config_path=config_dir)

@pytest.fixture
def config_dir(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    # Exemple de config spécifique
    specific_config = config_dir / "example_file_config.yaml"
    specific_config.write_text("""
reader_options:
  header: true
  inferSchema: true
  delimiter: ","
config_name: "sample_config"
""")

    # Exemple de config par défaut
    default_config = config_dir / "basic_reading_config.yaml"
    default_config.write_text("""
default_format: csv
reader_options:
  header: true
  inferSchema: false
  delimiter: ","
config_name: "basic_reading_config"
csv:
  reader_options:
    header: true
    inferSchema: false
    delimiter: ","
  config_name: "csv_config"

parquet:
  reader_options:
    mergeSchema: true
  config_name: "parquet_config"
""")
    
    return config_dir

@pytest.fixture
def test_file(tmp_path):
    test_file = tmp_path / "file1.csv"
    test_file.write_text("col1,col2\n1,2\n3,4")
    return test_file

def test_get_config_file_specific(base_reader, config_dir):
    # Modifiez le répertoire courant pour le test 
    os.chdir(config_dir.parent)
    config_content = base_reader.get_config_file("example_file.csv")
    assert config_content['reader_options']['header'] == True
    assert config_content['config_name'] == "sample_config"
    
def test_get_config_file_default(base_reader, config_dir):
    os.chdir(config_dir.parent)
    config_content = base_reader.get_config_file("file1.csv")
    assert config_content['reader_options']['header'] == True


def test_read_file(base_reader,config_dir,test_file,monkeypatch):
    monkeypatch.chdir(config_dir.parent)
    dummy_df = base_reader.spark.createDataFrame([
        ("Alice", 1),
        ("Bob", 2)
    ], ["name", "id"])

    dummy_reader = MagicMock()
    dummy_reader.read.return_value = dummy_df

    with patch("pipeline.file_reader.reader_factory.ReaderFactory.get_reader", return_value=dummy_reader):
        df = base_reader.read_file(str(test_file))
        assert df.count() == 2