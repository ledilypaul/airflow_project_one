from utils.spark_session import get_spark_session
from pipeline.file_reader.base_reader import BaseFileReader
from pipeline.extractor.extractor import Extractor

try:
    extractor = Extractor("config/extractor_config.yaml")
    files = extractor.list_files()
    spark = get_spark_session()
    reader = BaseFileReader(spark)
    df = reader.read_files(files[0])
    print(df)
except Exception as e:
    print(e)