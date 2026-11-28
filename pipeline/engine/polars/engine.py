"""Moteur Polars (P5-7) : l'interface `Engine` sur la lecture, le nettoyage,
les contrôles et l'écriture Polars."""

from collections.abc import Sequence

import polars as pl
import pyarrow as pa

from pipeline.config import SourceConfig, SourceRegistry
from pipeline.config.quality import QualityCheck
from pipeline.engine.polars.clean import clean_for_staging
from pipeline.engine.polars.quality import polars_regex_problems, run_quality_checks
from pipeline.engine.polars.reader import read_file
from pipeline.engine.polars.writer import PolarsLakeWriter
from pipeline.quality.results import RESULTS_SCHEMA, CheckResult, result_rows


class PolarsEngine:
    name = "polars"

    def __init__(self, registry: SourceRegistry | None, writer: PolarsLakeWriter):
        self.registry = registry
        self.writer = writer

    def config_problems(self, registry: SourceRegistry) -> list[str]:
        loaded = registry.sources + registry.defaults
        return polars_regex_problems([(item.path, item.config) for item in loaded])

    def get_config(self, path: str) -> SourceConfig:
        return self.registry.match(path)

    def read(self, path: str, config: SourceConfig) -> pl.LazyFrame:
        return read_file(path, config)

    def persist(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        # Lu une fois pour raw, staging et les contrôles.
        return frame.collect().lazy()

    def unpersist(self, frame: pl.LazyFrame) -> None:
        """Rien à libérer : la mémoire suit la référence."""

    def clean_for_staging(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        return clean_for_staging(frame)

    def run_checks(self, frame: pl.LazyFrame, checks: Sequence[QualityCheck]) -> list[CheckResult]:
        return run_quality_checks(frame, checks)

    def location(self, target: str) -> str:
        return self.writer.location(target)

    def write_lake(self, frame: pl.LazyFrame, target: str, **partition) -> int:
        return self.writer.write(frame, target, **partition)

    def write_results(
        self, results: Sequence[CheckResult], target: str, *, source_name: str, **partition
    ) -> int:
        rows = result_rows(results, source_name=source_name)
        table = pa.Table.from_pylist(
            [dict(zip(RESULTS_SCHEMA.names, row, strict=True)) for row in rows], RESULTS_SCHEMA
        )
        required = [f.name for f in RESULTS_SCHEMA if not f.nullable]
        return self.writer.write(pl.from_arrow(table), target, required=required, **partition)

    def delete_partition(self, target: str, *, run_id: str, source_file: str) -> bool:
        return self.writer.delete_partition(target, run_id=run_id, source_file=source_file)

    def close(self) -> None:
        """Pas de session à fermer."""
