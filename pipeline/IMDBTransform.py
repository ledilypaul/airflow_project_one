from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format
from pyspark.sql import functions  as F

class IMDBTransform:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform(self, df):
        return df
    
    def change_date_format(self,df):
        df = df.withColumn("release_date", to_date(df["release_date"], "yyyy-MM-dd"))
        df = df.withColumn("release_date", date_format(df["release_date"], "dd/MM/yyyy"))
        return df

    def clean_array_column(self,df,columns):
        for column in columns:
            df = df.withColumn(column, F.regexp_replace(F.col(column), "[\\[\\]']", "")) \
            .withColumn(column, F.split(F.col(column), ","))
        return df