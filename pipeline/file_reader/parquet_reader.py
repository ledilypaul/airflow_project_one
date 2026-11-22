from pyspark.sql import DataFrame, SparkSession

from pipeline.config import SourceConfig


class ParquetReader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, file_path: str, config: SourceConfig) -> DataFrame:
        return self.spark.read.options(**config.reader.options).parquet(file_path)
