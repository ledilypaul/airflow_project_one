"""Moteur Spark (P5-6) : l'interface `Engine` sur le code Spark existant
(reader, processor, contrôles, writer), sans en changer les règles."""

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession

from pipeline.config import SourceConfig, SourceRegistry
from pipeline.config.quality import QualityCheck
from pipeline.file_reader.base_reader import BaseFileReader
from pipeline.processor.base_processor import BaseProcessor
from pipeline.quality.checks import java_regex_problems, results_dataframe, run_quality_checks
from pipeline.quality.results import CheckResult
from storage.parquet_lake_writer import ParquetLakeWriter


class SparkEngine:
    name = "spark"

    def __init__(
        self,
        spark: SparkSession,
        writer: ParquetLakeWriter,
        reader: BaseFileReader | None = None,
    ):
        self.spark = spark
        self.writer = writer
        self.reader = reader  # absent quand seules les écritures servent

    def config_problems(self, registry: SourceRegistry) -> list[str]:
        loaded = registry.sources + registry.defaults
        return java_regex_problems(self.spark, [(item.path, item.config) for item in loaded])

    def get_config(self, path: str) -> SourceConfig:
        return self.reader.get_config(path)

    def read(self, path: str, config: SourceConfig) -> DataFrame:
        return self.reader.read_file(path, config=config)

    def persist(self, frame: DataFrame) -> DataFrame:
        return frame.persist()

    def unpersist(self, frame: DataFrame) -> None:
        frame.unpersist()

    def clean_for_staging(self, frame: DataFrame) -> DataFrame:
        """Minimal, deterministic cleanup of the staging zone (P2-2): drop the
        rows whose columns are all null, then exact duplicates. Nothing else:
        business transformations belong to P2-4.

        Runs on the business columns only, before the writer adds the technical
        ones: `lake_run_id` and `lake_source_file` are never null, so a fully
        empty row would never be dropped once they are there.
        """
        return BaseProcessor(frame).remove_empty_rows().remove_duplicates().df

    def run_checks(self, frame: DataFrame, checks: Sequence[QualityCheck]) -> list[CheckResult]:
        return run_quality_checks(frame, checks)

    def location(self, target: str) -> str:
        return self.writer.location(target)

    def write_lake(self, frame: DataFrame, target: str, **partition) -> int:
        return self.writer.write(frame, target, **partition)

    def write_results(
        self, results: Sequence[CheckResult], target: str, *, source_name: str, **partition
    ) -> int:
        frame = results_dataframe(self.spark, results, source_name=source_name)
        return self.writer.write(frame, target, **partition)

    def delete_partition(self, target: str, *, run_id: str, source_file: str) -> bool:
        return self.writer.delete_partition(
            self.spark, target, run_id=run_id, source_file=source_file
        )

    def close(self) -> None:
        self.spark.stop()
