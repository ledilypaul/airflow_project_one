"""Types Spark des types neutres de `reader.columns_type` (P5-2).

La table des noms est `pipeline/config/types.py::NEUTRAL_TYPES` ; ce module
la convertit en objets `pyspark.sql.types`.
"""

from collections.abc import Mapping

from pyspark.sql import types as spark_types
from pyspark.sql.types import DataType, StructField, StructType

from pipeline.config.types import resolve_column_type


def get_spark_type(col_type: str) -> DataType:
    """Type Spark d'un type YAML de `columns_type`.

    Raises:
        ValueError: type inconnu ou complexe (voir `resolve_column_type`).
    """
    return getattr(spark_types, resolve_column_type(col_type).spark)()


def build_schema(columns_type: Mapping[str, str] | None) -> StructType | None:
    """Schéma Spark explicite (colonnes nullables, dans l'ordre du YAML), ou
    None quand aucun `columns_type` n'est déclaré."""
    if not columns_type:
        return None
    return StructType(
        [
            StructField(name, get_spark_type(col_type), True)
            for name, col_type in columns_type.items()
        ]
    )
