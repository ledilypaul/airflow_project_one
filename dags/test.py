import os,sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipeline.extractor.extractor import Extractor
from pipeline.file_reader.base_reader import BaseFileReader

from pipeline.IMDBExtractor import get_files,process_files
from utils.functions_utils import insert_into_file_list,list_file_from_db

# dfs = process_files(get_files("../../Spark/data/"),"../../Spark/data/")
# print(dfs)
# res = list_file_from_db()
# reader = BaseFileReader()
# print(res)
# for r in res:
#     print(reader.read_files(r[2]))


config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'extractor_config.yaml'))
extractor = Extractor(config_path=config_path)
reader = BaseFileReader()
files = extractor.list_files() 

for file in files:
    content = reader.read_files(file)
    print(f"Read content from {file}: {content}")