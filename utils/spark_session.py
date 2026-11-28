import os

from pyspark.sql import SparkSession


def get_spark_session(app_name="DataPipelineApp") -> SparkSession:
    spark_builder = SparkSession.builder.appName(app_name)
    spark_builder.config("spark.sql.shuffle.partitions", "4")
    spark_builder.config("spark.sql.ansi.enabled", "false")
    # Un horodatage sans décalage est lu dans ce fuseau : UTC, comme DuckDB relit
    # le lake et comme le moteur Polars, quel que soit le fuseau de la machine.
    spark_builder.config("spark.sql.session.timeZone", "UTC")

    if os.getenv("ENV") == "prod":
        # In prod, master is managed by the cluster (YARN, K8s, etc.)
        # On ajoute les packages nécessaires
        spark_builder.config("spark.jars.packages", "com.crealytics:spark-excel_2.13:0.31.2")
    else:
        spark_builder.master("local[*]")
        spark_builder.config("spark.driver.host", "127.0.0.1")
        spark_builder.config("spark.driver.memory", "2g")
    return spark_builder.getOrCreate()
