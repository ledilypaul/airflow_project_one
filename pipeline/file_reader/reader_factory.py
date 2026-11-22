from pyspark.sql import SparkSession

from .csv_reader import CSVReader
from .excel_reader import ExcelReader
from .parquet_reader import ParquetReader

# `source.format` -> reader. Les clés sont exactement les valeurs de
# `pipeline.config.models.FileFormat` (vérifié par les tests P2-1). Pas de
# `xls` : read_excel exige alors xlrd, qui n'est pas installé (C-6).
READER_CLASSES = {
    "csv": CSVReader,
    "txt": CSVReader,
    "parquet": ParquetReader,
    "xlsx": ExcelReader,
}


class ReaderFactory:
    @staticmethod
    def get_reader(file_format: str, spark: SparkSession):
        reader_class = READER_CLASSES.get(file_format)
        if not reader_class:
            raise ValueError(f"Unsupported file format: {file_format}")
        return reader_class(spark)
