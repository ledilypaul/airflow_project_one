from utils.spark_session import get_spark_session
from pipeline.file_reader.base_reader import BaseFileReader
from pipeline.extractor.extractor import Extractor
from utils.db_connection import db_connection
try:
    # extractor = Extractor("config/extractor_config.yaml")
    # files = extractor.list_files()
    # spark = get_spark_session()
    # reader = BaseFileReader(spark)
    # df = reader.read_files(files[0])
    # print(df)
    engine = db_connection()
    with engine.connect() as conn:
        result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';")
        print("Connexion réussie :", result.fetchone())
except Exception as e:
    print("Erreur de connexion :", e)

    print(e)