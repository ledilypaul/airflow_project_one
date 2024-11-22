from .base_reader import BaseFileReader
from pyspark.sql import SparkSession

class ExcelReader(BaseFileReader):
    def read(self,file_path,file_config):
        return 0