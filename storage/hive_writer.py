from pyspark.sql import DataFrame

from .base_writer import BaseWriter


class HiveWriter(BaseWriter):
    def __init__(self, spark):
        self.spark = spark

    def write(self, data: DataFrame, target: str, mode: str = "append", **_kwargs) -> None:
        data.write.mode(mode).saveAsTable(target)
