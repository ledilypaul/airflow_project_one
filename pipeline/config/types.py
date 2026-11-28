"""Types neutres de `reader.columns_type` (P2-1, rendus indépendants du moteur en P5-2).

Une seule table, `NEUTRAL_TYPES` : pour chaque type du YAML, le nom du type
chez chaque moteur ou format (Spark, Polars, DuckDB, Arrow) **sous forme de
chaînes**, et ses catégories (`numeric`, `fractional`) pour les validations.
Ce module ne dépend que de la bibliothèque standard : charger une config
n'importe ni PySpark ni Polars. La conversion en objets de moteur se fait
dans le moteur (`pipeline/engine/<moteur>/types.py`).

Types : `string`, `int` (32 bits, alias `integer`), `long` (64 bits, P5-2),
`float` (32 bits), `double` (64 bits), `boolean`, `date`, `timestamp`.

Uniquement des types atomiques : `columns_type` n'est appliqué qu'aux formats
csv/txt (`SCHEMA_FORMATS`), et la source CSV de Spark refuse toute colonne
ARRAY, MAP ou STRUCT à la lecture (`UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE`).
Une liste encodée dans une colonne (ex. une liste de genres) se lit en
`string` et se découpe dans une transformation ultérieure (C-1).

Ce module ne dépend de rien sous `pipeline.file_reader` (C-7 : les readers
importent `pipeline.config`, l'inverse créerait un import circulaire).
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnType:
    """Un type neutre et ses noms chez chaque moteur ou format.

    `spark` est le nom de la classe de `pyspark.sql.types`, `polars` celui de
    l'attribut de `polars`, `duckdb` le type lu dans le Parquet écrit par
    Spark (et attendu par un contrat dbt), `arrow` le `str()` du type pyarrow
    de ce même Parquet (vérifié par tests/integration/test_p5_2_long_type.py).
    """

    spark: str
    polars: str
    duckdb: str
    arrow: str
    numeric: bool = False
    fractional: bool = False


NEUTRAL_TYPES: dict[str, ColumnType] = {
    "string": ColumnType("StringType", "String", "VARCHAR", "string"),
    "int": ColumnType("IntegerType", "Int32", "INTEGER", "int32", numeric=True),
    "long": ColumnType("LongType", "Int64", "BIGINT", "int64", numeric=True),
    "float": ColumnType("FloatType", "Float32", "FLOAT", "float", numeric=True, fractional=True),
    "double": ColumnType(
        "DoubleType", "Float64", "DOUBLE", "double", numeric=True, fractional=True
    ),
    "boolean": ColumnType("BooleanType", "Boolean", "BOOLEAN", "bool"),
    "date": ColumnType("DateType", "Date", "DATE", "date32[day]"),
    "timestamp": ColumnType("TimestampType", "Datetime", "TIMESTAMP", "timestamp[ns]"),
}

# Écrit autrement mais accepté depuis P2-1 : les configs existantes se chargent à l'identique.
TYPE_ALIASES: dict[str, str] = {"integer": "int"}

SUPPORTED_COLUMN_TYPES: tuple[str, ...] = (*NEUTRAL_TYPES, *TYPE_ALIASES)

# Types complexes : explicitement refusés, avec la raison (la source CSV ne
# les lit pas), plutôt qu'un simple « type non reconnu ».
_COMPLEX_TYPE_PREFIXES: tuple[str, ...] = ("array", "map", "struct")


def resolve_column_type(col_type: str) -> ColumnType:
    """Le `ColumnType` d'un type YAML de `columns_type` (insensible à la casse
    et aux espaces autour).

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
    normalized_type = TYPE_ALIASES.get(normalized_type, normalized_type)
    if normalized_type not in NEUTRAL_TYPES:
        raise ValueError(
            f"type de colonne {col_type!r} non supporté "
            f"(types acceptés : {', '.join(SUPPORTED_COLUMN_TYPES)})"
        )
    return NEUTRAL_TYPES[normalized_type]
