from .base_reader import BaseFileReader
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
class CSVReader(BaseFileReader):
    def read(self,file_path,file_config):
        header =file_config.get("header", True)
        infer_schema = file_config.get("inferSchema", True)
        delimiter = file_config.get("delimiter", ",")
        columns_type = file_config.get("columns_type")
        if columns_type:
            schema = StructType([
            StructField(name, self._get_spark_type(col_type),True)
            for name, col_type in columns_type.items()
            ])
            return self.spark.read.csv(
                file_path,
                header=header,
                schema=schema,
                sep=delimiter
            ).limit(20)
        return self.spark.read.csv(
                file_path,
                header=header,
                inferSchema=infer_schema,
                sep=delimiter
            ).limit(20)

    def _get_spark_type(self, col_type):
        """
        Mappe les types de colonnes spécifiés dans le YAML vers les types Spark.
        
        Args:
            col_type (str): Type de colonne spécifié dans le YAML.
        
        Returns:
            DataType: Type de données Spark correspondant.
        """
        type_mapping = {
            "int": IntegerType(),
            "string": StringType(),
            "float": FloatType()
            # Ajoute d'autres types si nécessaire
        }
        return type_mapping.get(col_type, StringType())  # Par défaut : StringType
