"""Options de lecture acceptées dans `reader.options`, par moteur et par format.

`READER_OPTIONS_BY_ENGINE[engine][format]` (P5-2) : les options s'interprètent
selon le moteur de la source (`source.engine`, voir `engine.py`). Les listes
Spark sont décrites ci-dessous ; les listes Polars, leurs équivalences avec
Spark et leurs sources, en bas du module.

Une option absente de ces listes est une erreur de validation : c'est ce qui
attrape une faute de frappe (`infer_schema` au lieu de `inferSchema`) que
Spark ignorerait en silence. Seules les options de **lecture** figurent ici
(les options d'écriture comme `quoteAll` ou `compression` n'ont pas de sens
pour un reader). Les noms sont sensibles à la casse : on écrit la forme
documentée.

Sources (versions installées : PySpark 4.3.0, pandas 2.3) :
- CSV : https://spark.apache.org/docs/4.3.0/sql-data-sources-csv.html
  (« Data Source Option », portée read ou read/write) ;
- Parquet : https://spark.apache.org/docs/4.3.0/sql-data-sources-parquet.html ;
- options génériques des sources fichiers (csv et parquet) :
  https://spark.apache.org/docs/4.3.0/sql-data-sources-generic-options.html ;
- `basePath` (découverte des partitions, toutes sources fichiers,
  `FileIndexOptions.BASE_PATH_PARAM`) : section « Partition Discovery » de
  https://spark.apache.org/docs/4.3.0/sql-data-sources-parquet.html ;
- `columnPruning` (CSV) : absent du tableau de la doc, mais option de lecture
  réelle, `CSVOptions.COLUMN_PRUNING` lue par `getBool` (constante présente
  dans le jar spark-catalyst 4.3.0 ; source :
  sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVOptions.scala) ;
- Excel : paramètres de `pyspark.pandas.read_excel`
  (https://spark.apache.org/docs/4.3.0/api/python/reference/pyspark.pandas/api/pyspark.pandas.read_excel.html),
  que `ExcelReader` appelle. `converters` et `date_parser` attendent des
  fonctions, inexprimables en YAML, et `**kwds` n'est pas ouvert.

Le format `xls` n'est pas proposé : `read_excel` exige alors `xlrd`, qui
n'est pas installé (C-6).

tests/unit/config/test_p2_1_source_config.py vérifie ces listes contre les
signatures de la version installée (`DataFrameReader.csv`,
`pyspark.pandas.read_excel`), contre la doc pour Parquet (`parquet()` n'a
pas de mot-clé par option), et épingle la version de PySpark : une montée de
version impose de revoir ces listes.
"""

GENERIC_FILE_SOURCE_OPTIONS: frozenset[str] = frozenset(
    {
        "ignoreCorruptFiles",
        "ignoreMissingFiles",
        "pathGlobFilter",
        "ignoredPathSegmentRegex",
        "recursiveFileLookup",
        "modifiedBefore",
        "modifiedAfter",
        "basePath",
    }
)

CSV_READ_OPTIONS: frozenset[str] = GENERIC_FILE_SOURCE_OPTIONS | {
    "sep",
    "delimiter",
    "encoding",
    "charset",
    "quote",
    "escape",
    "comment",
    "header",
    "inferSchema",
    "preferDate",
    "enforceSchema",
    "ignoreLeadingWhiteSpace",
    "ignoreTrailingWhiteSpace",
    "nullValue",
    "nanValue",
    "positiveInf",
    "negativeInf",
    "dateFormat",
    "timestampFormat",
    "timestampNTZFormat",
    "timeFormat",
    "enableDateTimeParsingFallback",
    "maxColumns",
    "maxCharsPerColumn",
    "mode",
    "columnNameOfCorruptRecord",
    "singleVariantColumn",
    "multiLine",
    "charToEscapeQuoteEscaping",
    "samplingRatio",
    "emptyValue",
    "locale",
    "lineSep",
    "unescapedQuoteHandling",
    "timeZone",
    "columnPruning",
}

PARQUET_READ_OPTIONS: frozenset[str] = GENERIC_FILE_SOURCE_OPTIONS | {
    "datetimeRebaseMode",
    "int96RebaseMode",
    "mergeSchema",
}

EXCEL_READ_OPTIONS: frozenset[str] = frozenset(
    {
        "sheet_name",
        "header",
        "names",
        "index_col",
        "usecols",
        "dtype",
        "engine",
        "true_values",
        "false_values",
        "skiprows",
        "nrows",
        "na_values",
        "keep_default_na",
        "verbose",
        "parse_dates",
        "thousands",
        "comment",
        "skipfooter",
    }
)

READER_OPTIONS_BY_FORMAT: dict[str, frozenset[str]] = {
    "csv": CSV_READ_OPTIONS,
    "txt": CSV_READ_OPTIONS,
    "parquet": PARQUET_READ_OPTIONS,
    "xlsx": EXCEL_READ_OPTIONS,
}

# Formats lus par Spark (`DataFrameReader.option`) : leurs options sont des
# scalaires. Les options pandas (Excel) acceptent aussi des listes
# (`usecols`, `na_values`...) ou `null` (`header: null`).
SPARK_FORMATS: frozenset[str] = frozenset({"csv", "txt", "parquet"})

# Formats pour lesquels `reader.columns_type` est appliqué (schéma explicite
# passé à `DataFrameReader.schema`).
SCHEMA_FORMATS: frozenset[str] = frozenset({"csv", "txt"})

# Deux noms pour une même option Spark CSV (`CSVOptions` lit l'un, puis
# l'autre) : les déclarer tous les deux est refusé, Spark en retiendrait un et
# ignorerait l'autre en silence.
ALIAS_GROUPS: tuple[frozenset[str], ...] = (
    frozenset({"sep", "delimiter"}),
    frozenset({"encoding", "charset"}),
)

# Type attendu des options Spark (csv, txt, parquet) qui ne sont pas du texte.
# Spark analyse ces valeurs à la lecture (`getBool`, `getInt`, `toDouble`) :
# sans ce typage, `header: "yes"` passerait la validation et échouerait au
# premier fichier lu. Les options absentes de ce dictionnaire sont du texte
# (toute valeur scalaire est acceptée, Spark la convertit en chaîne).
# `pathGlobFilter`, `modifiedBefore` et `modifiedAfter` sont annotés
# `Union[bool, str]` dans `DataFrameReader.csv` mais attendent un motif ou un
# horodatage : ils restent du texte. Ne s'applique pas à Excel, dont les
# options sont celles de pandas (`header: 0` y est un index de ligne).
SPARK_OPTION_TYPES: dict[str, type] = {
    # booléens
    "header": bool,
    "inferSchema": bool,
    "preferDate": bool,
    "enforceSchema": bool,
    "ignoreLeadingWhiteSpace": bool,
    "ignoreTrailingWhiteSpace": bool,
    "enableDateTimeParsingFallback": bool,
    "multiLine": bool,
    "columnPruning": bool,
    "mergeSchema": bool,
    "ignoreCorruptFiles": bool,
    "ignoreMissingFiles": bool,
    "recursiveFileLookup": bool,
    # entiers
    "maxColumns": int,
    "maxCharsPerColumn": int,
    # nombre (un entier est accepté : `samplingRatio: 1`)
    "samplingRatio": float,
}


# ---------------------------------------------------------------------------
# Polars (P5-2)
# ---------------------------------------------------------------------------
# Sources (Polars 2.2.0, vérifiées le 2026-11-24 contre les signatures de
# `pl.scan_csv`, `pl.read_csv`, `pl.scan_parquet` et `pl.read_excel`, et
# comparées à l'installation par tests/unit/config/test_p5_2_polars_options.py
# quand Polars est installé) :
# - CSV : https://docs.pola.rs/api/python/stable/reference/api/polars.scan_csv.html
# - Parquet : https://docs.pola.rs/api/python/stable/reference/api/polars.scan_parquet.html
# - Excel : https://docs.pola.rs/api/python/stable/reference/api/polars.read_excel.html
# Exclus volontairement : ce que le YAML dit déjà ailleurs ou que le moteur
# fixe (`source`, `schema`, `schema_overrides` en csv, `columns`, `new_columns`,
# `with_column_names`, `glob`, `storage_options`, `credential_provider`,
# `include_file_paths`, `row_index_*`), `n_rows` (une lecture partielle
# silencieuse) et `low_memory`/`cache`/`rechunk` (réglages de performance).

POLARS_CSV_READ_OPTIONS: frozenset[str] = frozenset(
    {
        "has_header",
        "separator",
        "comment_prefix",
        "quote_char",
        "skip_rows",
        "skip_lines",
        "null_values",
        "empty_string_is_null",
        "ignore_errors",
        "infer_schema",
        "infer_schema_length",
        "infer_schema_files",
        "encoding",
        "skip_rows_after_header",
        "try_parse_dates",
        "eol_char",
        "truncate_ragged_lines",
        "raise_if_empty",
        "decimal_comma",
        "extra_columns",
        "missing_columns",
    }
)

POLARS_PARQUET_READ_OPTIONS: frozenset[str] = frozenset(
    {
        "parallel",
        "use_statistics",
        "hive_partitioning",
        "hidden_file_prefix",
        "try_parse_hive_dates",
        "missing_columns",
        "extra_columns",
    }
)

POLARS_EXCEL_READ_OPTIONS: frozenset[str] = frozenset(
    {
        "sheet_id",
        "sheet_name",
        "table_name",
        "engine",
        "engine_options",
        "read_options",
        "has_header",
        "columns",
        "schema_overrides",
        "infer_schema_length",
        "drop_empty_rows",
        "drop_empty_cols",
        "raise_if_empty",
    }
)

POLARS_READER_OPTIONS_BY_FORMAT: dict[str, frozenset[str]] = {
    "csv": POLARS_CSV_READ_OPTIONS,
    "txt": POLARS_CSV_READ_OPTIONS,
    "parquet": POLARS_PARQUET_READ_OPTIONS,
    "xlsx": POLARS_EXCEL_READ_OPTIONS,
}

READER_OPTIONS_BY_ENGINE: dict[str, dict[str, frozenset[str]]] = {
    "spark": READER_OPTIONS_BY_FORMAT,
    "polars": POLARS_READER_OPTIONS_BY_FORMAT,
}

# Formats dont les options Polars sont des scalaires (ou `null`, ou une liste
# de scalaires : `null_values`). Excel y échappe : `engine_options`,
# `read_options` et `schema_overrides` sont des dictionnaires.
POLARS_SCALAR_FORMATS: frozenset[str] = frozenset({"csv", "txt", "parquet"})

# Type attendu des options Polars qui ne sont pas du texte (csv/txt/parquet).
POLARS_OPTION_TYPES: dict[str, type] = {
    "has_header": bool,
    "empty_string_is_null": bool,
    "ignore_errors": bool,
    "try_parse_dates": bool,
    "truncate_ragged_lines": bool,
    "raise_if_empty": bool,
    "decimal_comma": bool,
    "use_statistics": bool,
    "hive_partitioning": bool,
    "try_parse_hive_dates": bool,
    "skip_rows": int,
    "skip_lines": int,
    "skip_rows_after_header": int,
    "infer_schema_length": int,
    "infer_schema_files": int,
}

# `scan_csv` refuse tout autre encodage (vérifié avec Polars 2.2.0 : `UTF-8` et
# `latin1` lèvent `ValueError`), alors que Spark écrit `UTF-8`.
POLARS_OPTION_VALUES: dict[str, tuple[str, ...]] = {"encoding": ("utf8", "utf8-lossy")}

# Table d'équivalence Spark -> Polars (csv/txt), au plus proche : la
# sémantique n'est pas toujours identique (voir docs/configuration/sources.md).
# Sert à l'erreur d'une option Spark déclarée sur une source Polars, et à
# l'onboarding (P5-5). Une option Spark absente d'ici n'a pas d'équivalent
# établi : l'erreur ne propose rien.
SPARK_TO_POLARS_OPTIONS: dict[str, dict[str, str]] = {
    "csv": {
        "sep": "separator",
        "delimiter": "separator",
        "header": "has_header",
        "quote": "quote_char",
        "comment": "comment_prefix",
        "nullValue": "null_values",
        "inferSchema": "infer_schema",
        "lineSep": "eol_char",
    },
    # Spark Excel = options de pandas (`read_excel`) ; `sheet_name` et `engine`
    # portent le même nom des deux côtés.
    "xlsx": {
        "header": "has_header",
        "usecols": "columns",
        "dtype": "schema_overrides",
    },
}
SPARK_TO_POLARS_OPTIONS["txt"] = SPARK_TO_POLARS_OPTIONS["csv"]
