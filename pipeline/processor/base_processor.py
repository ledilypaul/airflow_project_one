from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class BaseProcessor:
    def __init__(self, df : DataFrame):
        self.df = df

    def remove_empty_rows(self):
        self.df = self.df.na.drop(how='all')
        return self
    
    def drop_na_rows(self, subset= None):
        self.df = self.df.na.drop(subset=subset)
        return self
    
    def fill_na(self, fill_dict):
        self.df = self.df.fillna(fill_dict)
        return self
    
    def remove_duplicates(self):
        self.df = self.df.dropDuplicates()
        return self
    
    def cast_column(self, column_name, new_type):
        self.df = self.df.withColumn(column_name, col(column_name).cast(new_type))
        return self

    def rename_columns(self, rename_map):
        for old, new in rename_map.items():
            self.df = self.df.withColumnRenamed(old, new)
        return self