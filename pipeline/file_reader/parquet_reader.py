from .base_reader import BaseFileReader
from .utils import get_spark_type
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

class ParquetReader(BaseFileReader):
    def read(self,file_path,file_config):
        parquet_config = {} 
        df_reader = self.spark.read 
        for item in file_config.get('parquet_reader', []): 
            for key, value in item.items():
                df_reader = df_reader.option(key, value)              
        return df_reader.parquet(file_path)