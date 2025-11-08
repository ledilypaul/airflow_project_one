from .base_reader import BaseFileReader
from pyspark.sql import SparkSession, DataFrame
import pyspark.pandas as ps
import polars as pl

class ExcelReader(BaseFileReader):
    def __init__(self,spark : SparkSession):
        self.spark = spark
        
    def read(self, file_path: str, file_config: dict = None) -> DataFrame:
        # Get options specific to Excel Reading
        excel_options = file_config.get("reader_options", {}) if file_config else {}
        try:
            pdf = ps.read_excel(file_path, **excel_options)
            return pdf
            
        except Exception as e:
            print(f"Error during the reading of the Excel file '{file_path}' with pyspark.pandas: {e}")
            raise 
            
    def read_polars(self, file_path, file_config):
        try:
            file_config = file_config.get("reader_options", {}) if file_config else {}
            df_reader = pl.read_excel(file_path)#,**file_config 
            return df_reader
        except Exception as e:
            print(f"Error reading file {file_path}, error: {e}")
