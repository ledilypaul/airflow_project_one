from .base_reader import BaseFileReader
from pyspark.sql import SparkSession
import pyspark.pandas as ps

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
        if file_config:
            df_reader = ps.read_excel(file_path,inferSchema=file_config.get("inferSchema", False),header=file_config.get("header", True),sheet_name=file_config.get("dataAddress", None))
            for item in file_config.get("excel_reader", []):
                for key, value in item.items():
                    df_reader = df_reader.option(key, value)
        return df_reader