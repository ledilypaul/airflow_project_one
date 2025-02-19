from .base_reader import BaseFileReader
from pyspark.sql import SparkSession
import pyspark.pandas as ps
import polars as pl
class ExcelReader(BaseFileReader):
    # def __init__(self,spark):
    #     if "com.crealytics.spark.excel" not in spark.conf.get("spark.jars.packages",""):
    #         self.spark = SparkSession.builder.config("spark.jars.packages","com.crealytics:spark-excel_2.12:0.13.7").getOrCreate()
    #     else:
    #         self.spark = spark
    # def read(self,file_path,file_config):
        
    #     if file_config:
    #         df_reader = self.spark.read.format("com.crealytics.spark.excel")
    #         for item in file_config.get("excel_reader", []):
    #             for key, value in item.items():
    #                 df_reader = df_reader.option(key,value)
    #     return df_reader.load(file_path)
    def read(self, file_path, file_config):
        try:
            file_config = file_config.get("excel_reader", {}) if file_config else {}
            df_reader = pl.read_excel(file_path)#,**file_config
            return df_reader
        except Exception as e:
            print(f"Error reading file {file_path}, error: {e}")
