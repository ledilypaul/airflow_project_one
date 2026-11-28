"""Résultats des contrôles qualité, sans moteur (P5-6) : statuts, exceptions
et schéma Arrow de `quality/<table>`. Les moteurs importent d'ici."""

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from pipeline.config.quality import QualityCheck

PASS = "pass"
FAIL = "fail"
WARN = "warn"
ERROR = "error"

_MAX_MESSAGE = 500

# Une ligne par contrôle ; le writer y ajoute lake_run_id, lake_source_file,
# lake_file_id et lake_ingested_at.
RESULTS_SCHEMA = pa.schema(
    [
        pa.field("source_name", pa.string(), False),
        pa.field("check_name", pa.string(), False),
        pa.field("check_type", pa.string(), False),
        pa.field("check_columns", pa.list_(pa.field("element", pa.string(), False)), False),
        pa.field("severity", pa.string(), False),
        # pass | fail | warn | error (voir pipeline/quality/checks.py).
        pa.field("status", pa.string(), False),
        # Lignes en échec ; null pour row_count et pour le statut error.
        pa.field("failed_rows", pa.int64(), True),
        # Lignes contrôlées (données du staging) ; null si le comptage a échoué.
        pa.field("checked_rows", pa.int64(), True),
        # Détail lisible ; null quand le contrôle passe ; `check error: ...` pour error.
        pa.field("message", pa.string(), True),
        # La règle appliquée (JSON), pour l'audit : la config peut changer d'un run à l'autre.
        pa.field("check_definition", pa.string(), False),
    ]
)


@dataclass(frozen=True)
class CheckResult:
    check: QualityCheck
    status: str
    failed_rows: int | None
    checked_rows: int | None
    message: str | None

    @property
    def name(self) -> str:
        return self.check.check_name

    @property
    def type(self) -> str:
        return self.check.type

    @property
    def columns(self) -> tuple[str, ...]:
        return self.check.check_columns

    @property
    def severity(self) -> str:
        return self.check.severity

    @property
    def blocking(self) -> bool:
        """Sévérité `error` et statut `fail` ou `error` : staging non écrit."""
        return self.severity == "error" and self.status in (FAIL, ERROR)


class QualityChecksFailed(Exception):
    """Au moins un contrôle bloquant : le staging n'est pas écrit.

    `notes` complète le résumé sans le remplacer (ex. `results not recorded:
    disk full` quand l'écriture des résultats échoue elle aussi)."""

    def __init__(self, results: Sequence[CheckResult], *, notes: Sequence[str] = ()):
        self.results = list(results)
        self.notes = list(notes)
        message = failure_summary(self.results)
        if self.notes:
            message += f" ({'; '.join(self.notes)})"
        super().__init__(message)

    @property
    def deterministic(self) -> bool:
        """Vrai quand une nouvelle tentative échouerait de la même façon
        (P2-6) : chaque contrôle bloquant a trouvé des lignes en échec
        (statut `fail`), et rien d'autre n'a échoué (aucune note : résultats
        enregistrés, partition staging précédente supprimée). Un contrôle
        bloquant qui n'a pas pu s'exécuter (statut `error`) ne l'est pas :
        sa cause peut être passagère (erreur Spark)."""
        failures = blocking_failures(self.results)
        return bool(failures) and not self.notes and all(r.status == FAIL for r in failures)


class QualityChecksError(Exception):
    """Le moteur lui-même a levé une exception (C-1) : aucun résultat, le
    staging n'est pas écrit (« fermé par défaut »). Message court."""


def blocking_failures(results: Sequence[CheckResult]) -> list[CheckResult]:
    return [r for r in results if r.blocking]


def failure_summary(results: Sequence[CheckResult]) -> str:
    """`quality checks failed: not_null(id): 3 rows; range(amount): 1 row`
    (contrôles bloquants seulement ; un contrôle `error` qui n'a pas pu
    s'exécuter y figure avec son message `check error: ...`)."""
    failures = "; ".join(f"{r.name}: {r.message}" for r in blocking_failures(results))
    return f"quality checks failed: {failures}"


def result_rows(results: Sequence[CheckResult], *, source_name: str) -> list[tuple]:
    """Les lignes de `RESULTS_SCHEMA`, une par contrôle."""
    return [
        (
            source_name,
            r.name,
            r.type,
            list(r.columns),
            r.severity,
            r.status,
            r.failed_rows,
            r.checked_rows,
            r.message,
            r.check.model_dump_json(exclude_none=True),
        )
        for r in results
    ]


def short_error(exc: BaseException) -> str:
    """Première ligne utile d'une exception (Spark/Py4J en font des pages)."""
    java_exception = getattr(exc, "java_exception", None)
    text = str(java_exception) if java_exception is not None else str(exc)
    first = next((line.strip() for line in text.splitlines() if line.strip()), type(exc).__name__)
    return first if len(first) <= _MAX_MESSAGE else first[: _MAX_MESSAGE - 3] + "..."
