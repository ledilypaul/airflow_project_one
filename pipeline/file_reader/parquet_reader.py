from .base_reader import BaseFileReader
from .utils import get_spark_type
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

class ParquetReader(BaseFileReader):
    def read(self,file_path,file_config):
        df_reader = self.spark.read
        options = file_config.get("reader_options", {})
        if options:          
            for item in file_config.get("reader_options",[]):
                for key, value in item.items():
                    df_reader = df_reader.option(key, str(value))   
        return df_reader.parquet(file_path) 
