import pytest

from pipeline.file_reader.csv_reader import CSVReader
from pipeline.file_reader.excel_reader import ExcelReader
from pipeline.file_reader.parquet_reader import ParquetReader
from pipeline.file_reader.reader_factory import ReaderFactory


def test_factory_returns_csv_reader(spark):
    reader = ReaderFactory.get_reader("csv", spark)
    assert isinstance(reader, CSVReader)


def test_factory_returns_txt_reader_as_csv_reader(spark):
    reader = ReaderFactory.get_reader("txt", spark)
    assert isinstance(reader, CSVReader)


def test_factory_returns_parquet_reader(spark):
    reader = ReaderFactory.get_reader("parquet", spark)
    assert isinstance(reader, ParquetReader)


def test_factory_returns_excel_reader_for_xlsx(spark):
    reader = ReaderFactory.get_reader("xlsx", spark)
    assert isinstance(reader, ExcelReader)


def test_factory_raises_for_unsupported_extension(spark):
    with pytest.raises(ValueError, match="Unsupported file format: json"):
        ReaderFactory.get_reader("json", spark)
