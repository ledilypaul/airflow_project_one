import logging

import pyspark.pandas as ps
from pyspark.sql import SparkSession

from .base_reader import BaseFileReader

log = logging.getLogger(__name__)


class ExcelReader(BaseFileReader):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)

    def read(self, file_path: str, file_config: dict = None) -> ps.DataFrame:
        excel_options = file_config.get("reader_options", {}) if file_config else {}
        try:
            return ps.read_excel(file_path, **excel_options)
        except Exception as e:
            log.error("Error reading Excel file '%s': %s", file_path, e)
            raise

    def read_polars(self, file_path: str, file_config: dict = None):
        import polars as pl  # import local : polars est optionnel
        options = file_config.get("reader_options", {}) if file_config else {}
        try:
            return pl.read_excel(file_path, **options)
        except Exception as e:
            log.error("Error reading Excel file '%s' with Polars: %s", file_path, e)
            raise
