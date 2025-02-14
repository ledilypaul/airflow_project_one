import os
import yaml
from pyspark.sql import SparkSession
from .reader_factory import ReaderFactory

class BaseFileReader:
    def __init__(self):  
        self.spark = SparkSession.builder \
            .appName("DataReader") \
            .getOrCreate()


    def get_config_file(self,file_name: str):
        """Function that get the config file based on the name of the file
        Args:
            file_name (str): Name of file
        Returns:
            config_file: Configuration file YAML 
        """
        for config_file in os.listdir("config/"):
            if config_file.split("_")[0] in file_name.lower():
                config_path = os.path.join("config",config_file)
                break
        else:
            config_path = os.path.join("config","basic_reading_config.yaml") 
            
        with open(config_path, 'r') as f:
            config_content = yaml.safe_load(f)
        return config_content
    
    def read_files(self, file_path: str):
        file_config = self.get_config_file(file_path)
        extension_file = file_path.split(".")[-1].lower()
        reader_instance = ReaderFactory.get_reader(file_config,extension_file)
        # reader_instance = ReaderFactory.get_reader(file_config,extension_file,self.spark)
        df = reader_instance.read(file_path,file_config)
        return df
        # print(df.head(1))
