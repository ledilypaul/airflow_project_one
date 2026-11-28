"""Interface d'un moteur d'ingestion (P5-6). Aucun import de moteur ici."""

from collections.abc import Sequence
from typing import Any, Protocol

from pipeline.config import SourceConfig, SourceRegistry
from pipeline.config.quality import QualityCheck
from pipeline.quality.results import CheckResult

# Les données d'un fichier, dans le type propre au moteur.
Frame = Any


class Engine(Protocol):
    name: str  # "spark" | "polars"

    def config_problems(self, registry: SourceRegistry) -> list[str]:
        """Ce que seul le moteur peut refuser dans les configs (ex. regex)."""

    def get_config(self, path: str) -> SourceConfig: ...

    def read(self, path: str, config: SourceConfig) -> Frame: ...

    def persist(self, frame: Frame) -> Frame:
        """Garde les données lues plusieurs fois ; `unpersist` les libère."""

    def unpersist(self, frame: Frame) -> None: ...

    def clean_for_staging(self, frame: Frame) -> Frame:
        """Retire les lignes entièrement vides et les doublons exacts."""

    def run_checks(self, frame: Frame, checks: Sequence[QualityCheck]) -> list[CheckResult]: ...

    def location(self, target: str) -> str: ...

    def write_lake(
        self,
        frame: Frame,
        target: str,
        *,
        mode: str,
        run_id: str,
        source_file: str,
        file_id: int,
    ) -> int:
        """Écrit la partition (run, fichier) de `target` ; lignes écrites."""

    def write_results(
        self, results: Sequence[CheckResult], target: str, *, source_name: str, **partition
    ) -> int:
        """Écrit les résultats qualité, mêmes arguments de partition."""

    def delete_partition(self, target: str, *, run_id: str, source_file: str) -> bool: ...

    def close(self) -> None: ...
