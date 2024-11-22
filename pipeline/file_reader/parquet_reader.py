from .base_reader import BaseFileReader
from .utils import get_spark_type
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

class ParquetReader(BaseFileReader):
    def read(self,file_path,file_config = None):
        parquet_config = dict(file_config.get('parquet_reader', {}))
        dictionnaire = {k: v for d in parquet_config for k, v in d.items()}
        compression = "res" 
        partition_column = "oui"
        print(parquet_config)
        options = {}
        if compression:
            options["compression"] = compression
        if partition_column:
            options["partitionColumn"] = partition_column

        return self.spark.read.option(parquet_config).parquet(file_path)
