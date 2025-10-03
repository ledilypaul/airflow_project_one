from pyspark.sql import SparkSession
import os 

def get_spark_session(app_name="DataPipelineApp") -> SparkSession:  
    spark_builder = SparkSession.builder.appName(app_name)
    spark_builder.config("spark.sql.shuffle.partitions","4")
    spark_builder.config("spark.sql.ansi.enabled", "false")

    if os.getenv("ENV") == "prod":
        # In prod, master is managed by the cluster (YARN, K8s, etc.)
        # On ajoute les packages nécessaires
        spark_builder.config("spark.jars.packages", "com.crealytics:spark-excel_2.13:0.31.2")
    else:
        spark_builder.master("local[*]")
        spark_builder.config("spark.driver.host", "127.0.0.1")
        spark_builder.config("spark.driver.memory", "2g")
    return spark_builder.getOrCreate()