from pyspark.sql import SparkSession

def get_spark_session(app_name="DataPipelineApp") -> SparkSession:
    excel_package = "com.crealytics:spark-excel_2.13:0.31.2" # 0.31.2 not yet available in jar  
    
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()
        # .config("spark.jars.packages", excel_package) \
