from extractor.extractor import Extractor
from file_reader.base_reader import BaseFileReader 
import os,sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.functions_utils import insert_into_file_list
ext = Extractor("config/extractor_config.yaml")
reader = BaseFileReader()
import datetime
# print(reader.get_config_file())
# print(ext.list_files())

def main_test():
    ext = Extractor("config/extractor_config.yaml")
    reader = BaseFileReader()
    files = ext.list_files()
    print(type(os.path.getmtime(files[0])))
    insert_into_file_list(["test", "test", datetime.datetime.now()])
main_test()
