"""Options de lecture acceptées dans `reader.options`, par format.

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
