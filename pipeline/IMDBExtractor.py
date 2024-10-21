import os
import json
from pyspark.sql.types import IntegerType, StringType, FloatType, BooleanType,DateType , StructType, StructField, ArrayType, DataType
from pyspark.sql import SparkSession

class IMDBExtractor:
    def __init__(self):
        self.spark = SparkSession.builder.appName('Extractor').getOrCreate()
            

    def list_files(self, path):
        return os.listdir(path)
    
    def check_integrity_files(self,files,path):
        valid_files = []
        for file in files:
            if os.path.getsize(path+"/"+file) == 0:
                raise ValueError("File {file} is empty")
            if file.lower().startswith("imdb") and file.lower().endswith("csv"):
                valid_files.append(file)            
        return valid_files

    def create_schema(self):
        with open('../config_file.json') as f:
            file = json.load(f)
        column_types = file["column_type"]
        type_mapping = {
            "int": IntegerType(),
            "string": StringType(),
            "float": FloatType(),
            "date": DateType(),
            "boolean": BooleanType(),
            "array" : ArrayType(StringType())
        }
        return StructType([
            StructField(col_name, type_mapping[col_type], True)
            for col_name, col_type in column_types.items()
        ])      
    def read_data(self,file,file_format):
        if file_format == "csv":
            df = self.spark.read.option("header",True) \
                            .option("inferSchema",True) \
                            .option("delimiter",",") \
                            .option("quote","\"") \
                            .option("escape","\"") \
                            .option("schema", self.create_schema())\
                            .csv(file)
        return df


def main(path, file_format):
    extractor = IMDBExtractor()
    files = extractor.list_files(path)
    valid_files = extractor.check_integrity_files(files,path)
    for file in valid_files:
        print(file)
    return valid_files


if __name__ == "__main__":
    main("../../Spark/data/", ".csv")
     