from .utils import get_spark_type
from pyspark.sql.types import StructType, StructField
from pyspark.sql import SparkSession
class CSVReader:
    def __init__(self,spark : SparkSession):
        self.spark = spark
        
    def read(self,file_path,file_config):
        options = file_config.get("reader", {}).get("reader_options", {})
        header =options.get("header", True)
        infer_schema = options.get("inferSchema", True)
        delimiter = options.get("delimiter", ",")        
        columns_type = options.get("columns_type")
        
        if columns_type:
            schema = StructType([
            StructField(name, get_spark_type(col_type),True)
            for name, col_type in columns_type.items()
            ])
            return self.spark.read.csv(
                file_path,
                header=header,
                schema=schema,
                sep=delimiter
            )
        return self.spark.read.csv(
                file_path,
                header=header,
                inferSchema=infer_schema,
                sep=delimiter
            )
