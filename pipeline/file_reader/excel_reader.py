from .base_reader import BaseFileReader
from pyspark.sql import SparkSession
import pyspark.pandas as ps

class ExcelReader(BaseFileReader):
    def read(self,file_path,file_config):
        if file_config:
            df_reader = self.spark.read.format("com.crealytics.spark.excel")
            for item in file_config.get("excel_reader", []):
                for key, value in item.items():
                    df_reader = df_reader.option(key,value)
        return df_reader.excel.load(file_path)
