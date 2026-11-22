import logging

from pyspark.sql import DataFrame, SparkSession

from pipeline.config import SourceConfig

log = logging.getLogger(__name__)


class ExcelReader:
    """Lit xlsx avec `pyspark.pandas.read_excel` : `reader.options` sont des
    options pandas (`header: 0`, `dtype: str`...), validées au chargement.

    `pyspark.pandas` est importé dans `read()`, pas en tête de module (C-5) :
    ReaderFactory importe tous les readers, et lire un CSV ne doit pas exiger
    un pandas et un pyarrow compatibles avec pandas-on-Spark."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, file_path: str, config: SourceConfig) -> DataFrame:
        import pyspark.pandas as ps

        excel_options = config.reader.options
        try:
            # pyspark.pandas.read_excel renvoie un DataFrame pandas-on-Spark,
            # qui n'a pas l'API pyspark.sql.DataFrame (ex: .show()) : on
            # convertit explicitement pour respecter le contrat des readers
            # (voir CLAUDE.md, B3).
            return ps.read_excel(file_path, **excel_options).to_spark()
        except Exception as e:
            log.error("Error reading Excel file '%s': %s", file_path, e)
            raise

    def read_polars(self, file_path: str, file_config: dict = None):
        import polars as pl  # import local : polars est optionnel

        options = file_config.get("reader_options", {}) if file_config else {}
        try:
            return pl.read_excel(file_path, **options)
        except Exception as e:
            log.error("Error reading Excel file '%s' with Polars: %s", file_path, e)
            raise
