"""Codes de sortie des jobs d'ingestion (`pipeline/jobs/ingest.py`), une seule
définition pour le job, l'opérateur Spark, la tâche Polars et l'alerte d'échec
(P5-8). Aucun import d'Airflow ni de moteur : chacun peut l'importer."""

EXIT_FILES_FAILED = 1
EXIT_INVALID_RUN_ID = 2
EXIT_NO_ROWS = 3
EXIT_PARENT_GONE = 4
EXIT_INVALID_CONFIG = 5
EXIT_QUALITY_FAILED = 6

MEANINGS: dict[int, str] = {
    EXIT_FILES_FAILED: (
        "at least one file failed (read or write error, quality engine error, or mixed causes)"
    ),
    EXIT_INVALID_RUN_ID: "invalid --run-id",
    EXIT_NO_ROWS: "no file_list row for the run id",
    EXIT_PARENT_GONE: "parent process gone (watchdog), no status written",
    EXIT_INVALID_CONFIG: (
        "invalid reading config or LAKE_ROOT, every file of the run marked failed"
    ),
    EXIT_QUALITY_FAILED: (
        "blocking quality checks only, see file_list and quality/<table> in the lake"
    ),
}

# Déterministes (P2-6) : une nouvelle tentative échouerait de la même façon.
NO_RETRY: frozenset[int] = frozenset(
    {EXIT_INVALID_RUN_ID, EXIT_NO_ROWS, EXIT_INVALID_CONFIG, EXIT_QUALITY_FAILED}
)
