from utils.spark_session import get_spark_session
from pipeline.file_reader.base_reader import BaseFileReader
from pipeline.extractor.extractor import Extractor

try:
    extractor = Extractor("/config/extractor_config.yaml")
    files = extractor.list_files()
    print("test")
    print(files)
except Exception as e:
    print(e)