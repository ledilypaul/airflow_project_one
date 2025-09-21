from utils.spark_session import get_spark_session
from pipeline.file_reader.base_reader import BaseFileReader
from pipeline.extractor.extractor import Extractor
from utils.db_connection import db_connection
from utils.functions_utils import insert_into_file_list,list_file_from_db
from datetime import date
import traceback

try:
    extractor = Extractor("config/extractor_config.yaml")
    spark = get_spark_session()
    files = list_file_from_db()
    reader = BaseFileReader(spark)
    df = reader.read_file(files[0][2])
    df.show()
except Exception as e:
    print("Erreur de connexion :", e)
    print(traceback.format_exc())
    print(e)