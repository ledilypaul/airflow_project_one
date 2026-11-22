from pyspark.sql import DataFrame, SparkSession

from pipeline.config import SourceConfig
from pipeline.config.types import build_schema


class CSVReader:
    """Lit csv/txt : `reader.options` est passé tel quel à Spark (liste
    blanche validée au chargement), `reader.columns_type` donne le schéma
    explicite. Aucun défaut caché : une option absente du YAML prend le
    défaut de Spark (ex. `header=false`)."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, file_path: str, config: SourceConfig) -> DataFrame:
        df_reader = self.spark.read.options(**config.reader.options)
        schema = build_schema(config.reader.columns_type)
        if schema is not None:
            df_reader = df_reader.schema(schema)
        return df_reader.csv(file_path)
