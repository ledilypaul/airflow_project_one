from .base_reader import BaseFileReader
from .utils import get_spark_type
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import json
class JSONReader(BaseFileReader):
    def read(self, file_path, file_config):
        return self.spark.read.json(file_path)