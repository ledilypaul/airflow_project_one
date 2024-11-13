from extractor.extractor import Extractor
from file_reader.base_reader import BaseFileReader 

ext = Extractor("config/extractor_config.yaml")
reader = BaseFileReader()

# print(reader.get_config_file())
# print(ext.list_files())

def main_test():
    ext = Extractor("config/extractor_config.yaml")
    reader = BaseFileReader()
    files = ext.list_files()
    for file in files:
        print(file)
        reader.read_files(file)
        
main_test()