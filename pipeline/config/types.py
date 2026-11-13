"""Types acceptés dans `reader.columns_type` et construction du schéma Spark
explicite (P2-1).

Seule source de vérité des types : la validation des configs
(`pipeline/config/models.py`) et `CSVReader` s'appuient dessus. Ce module ne
dépend de rien sous `pipeline.file_reader` (C-7 : les readers importent
`pipeline.config`, l'inverse créerait un import circulaire) ;
`pipeline/file_reader/utils.py` le réexporte pour la compatibilité.

Uniquement des types atomiques : `columns_type` n'est appliqué qu'aux formats
csv/txt (`SCHEMA_FORMATS`), et la source CSV de Spark refuse toute colonne
ARRAY, MAP ou STRUCT à la lecture (`UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE`).
Une liste encodée dans une colonne (ex. `genre_list` d'IMDB) se lit en
`string` et se découpe dans une transformation ultérieure (C-1).
"""

from collections.abc import Mapping

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_SPARK_TYPES: dict[str, DataType] = {
    "string": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "float": FloatType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}

SUPPORTED_COLUMN_TYPES: tuple[str, ...] = tuple(_SPARK_TYPES)

# Types complexes : explicitement refusés, avec la raison (la source CSV ne
# les lit pas), plutôt qu'un simple « type non reconnu ».
_COMPLEX_TYPE_PREFIXES: tuple[str, ...] = ("array", "map", "struct")


def get_spark_type(col_type: str) -> DataType:
    """Type Spark d'un type YAML de `columns_type` (insensible à la casse et
    aux espaces autour).

    Raises:
        ValueError: type inconnu, ou type complexe (array, map, struct) que la
            source CSV de Spark ne sait pas lire.
    """
    normalized_type = col_type.strip().lower()

    if normalized_type.startswith(_COMPLEX_TYPE_PREFIXES):
        raise ValueError(
            f"type de colonne {col_type!r} non supporté : la source CSV de Spark ne lit "
            "que des types atomiques ; lire la colonne en 'string' et la découper dans "
            "une transformation ultérieure"
        )
    if normalized_type not in _SPARK_TYPES:
        raise ValueError(
            f"type de colonne {col_type!r} non supporté "
            f"(types acceptés : {', '.join(SUPPORTED_COLUMN_TYPES)})"
        )
    return _SPARK_TYPES[normalized_type]


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
