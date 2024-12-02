from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, BooleanType, DateType
def get_spark_type(col_type):
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
        "float": FloatType(),
        "boolean": BooleanType(),
        "date": DateType(),
        # Ajoute d'autres types si nécessaire
    }
    return type_mapping.get(col_type, StringType())  # Par défaut : StringType
