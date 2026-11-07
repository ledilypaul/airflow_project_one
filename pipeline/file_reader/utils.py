from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    TimestampType,
)


def get_spark_type(col_type: str):
    """
    Mappe les types YAML vers les types Spark.

    Args:
        col_type (str): Type de colonne spécifié dans le fichier YAML (ex: "string", "int").

    Returns:
        pyspark.sql.types.DataType: Type de données Spark correspondant.

    Raises:
        ValueError: Si le type spécifié n'est pas reconnu.
    """
    type_mapping = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "float": FloatType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "array": ArrayType(StringType()),
    }

    normalized_type = col_type.strip().lower()

    if normalized_type not in type_mapping:
        raise ValueError(f"[get_spark_type] Type non reconnu dans YAML : '{col_type}'")

    return type_mapping[normalized_type]
