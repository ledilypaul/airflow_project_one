import os
import pytest 
from unittest.mock import patch, MagicMock
from pipeline.file_reader.base_reader import BaseFileReader
from pyspark.sql import SparkSession

@pytest.fixture
def base_reader():
    return BaseFileReader()

@patch("pipeline.file_reader.base_reader.os.listdir")
@patch("pipeline.file_reader.base_reader.yaml.safe_load")
def test_read_files(mock_get_config, mock_get_reader, base_reader):
    # Mock the configuration returned
    mock_file_config = {"file_options": {"header": True, "inferSchema": True}}
    mock_get_config.return_value = mock_file_config

    # Mock the reader instance
    mock_reader_instance = MagicMock()
    mock_reader_instance.read.return_value = MagicMock()
    mock_get_reader.return_value = mock_reader_instance

    # Test reading a file
    mock_file_path = "data/imdb.csv"
    base_reader.read_files(mock_file_path)

    # Assert that config file and reader are used correctly
    mock_get_config.assert_called_once_with(mock_file_path)
    mock_get_reader.assert_called_once_with(mock_file_config, "csv")
    mock_reader_instance.read.assert_called_once_with(mock_file_path, mock_file_config)

@patch("pipeline.file_reader.base_reader.SparkSession.builder.getOrCreate")
def test_spark_initialization(mock_spark):
    # Test Spark session is initialized during BaseFileReader creation
    BaseFileReader()
    mock_spark.assert_called_once()