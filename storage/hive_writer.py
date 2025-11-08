from .base_writer import BaseWriter
from utils.db_connection import db_connection
from pyspark.sql import DataFrame

class HiveWriter:
    def __init__(self, spark):
        self.spark = spark

    def write_parquet(self, df: DataFrame, table_name) -> None:
        df.write.mode("append").saveAsTable(table_name)