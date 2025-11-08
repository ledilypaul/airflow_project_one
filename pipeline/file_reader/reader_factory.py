import os
from pyspark.sql import SparkSession
class ReaderFactory:
    @staticmethod
    def get_reader(config, file_extension, spark: SparkSession):
        from .csv_reader import CSVReader
        from .parquet_reader import ParquetReader
        from .excel_reader import ExcelReader
        reader_classes = {
            'csv' : CSVReader,
            'parquet' : ParquetReader,
            'xls' : ExcelReader,
            'xlsx' : ExcelReader,
            'txt' : CSVReader
        }
        reader_class = reader_classes.get(file_extension)
        if not reader_class:
            raise ValueError(f"Unsupported file extension: {file_extension}")
        return reader_class(spark)