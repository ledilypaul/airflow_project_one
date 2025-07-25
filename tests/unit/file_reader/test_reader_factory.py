import pytest
from pyspark.sql import SparkSession
from pipeline.file_reader.reader_factory import ReaderFactory
from pipeline.file_reader.csv_reader import CSVReader
from pipeline.file_reader.excel_reader import ExcelReader
from pipeline.file_reader.parquet_reader import ParquetReader

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("pytest").master("local[*]").getOrCreate()

def test_factory_returns_csv_reader(spark):
    config = {}
    reader = ReaderFactory.get_reader(config, 'csv', spark)
    assert isinstance(reader, CSVReader)

def test_factory_returns_txt_reader_as_csv_reader(spark):
    config = {}
    reader = ReaderFactory.get_reader(config, 'txt', spark)
    assert isinstance(reader, CSVReader)

def test_factory_returns_parquet_reader(spark):
    config = {}
    reader = ReaderFactory.get_reader(config, 'parquet', spark)
    assert isinstance(reader, ParquetReader)

def test_factory_returns_excel_reader_for_xlsx(spark):
    config = {}
    reader = ReaderFactory.get_reader(config, 'xlsx', spark)
    assert isinstance(reader, ExcelReader)

def test_factory_raises_for_unsupported_extension(spark):
    config = {}
    with pytest.raises(ValueError, match="Unsupported file extension: json"):
        ReaderFactory.get_reader(config, 'json', spark)
