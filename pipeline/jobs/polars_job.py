"""Job d'ingestion du moteur Polars (P5-7) : même contrat que
`pipeline/jobs/read_files_job.py` (zones, statuts, codes de sortie 0 à 6,
`pipeline/jobs/ingest.py`), sans Spark ni Airflow. Ne lit que les sources
`engine: polars` ; un fichier d'une source Spark est `failed`.

    uv run python -m pipeline.jobs.polars_job --run-id=<id> [--source-name=<source>]

Pas encore lancé par les DAG (P5-8), donc sans le watchdog de spark-submit.
"""

import logging

from pipeline.config import DEFAULT_CONFIG_ROOT, SourceRegistry
from pipeline.engine.polars.engine import PolarsEngine
from pipeline.engine.polars.writer import PolarsLakeWriter
from pipeline.jobs.ingest import parse_args, run

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

# Racine des configs ; attribut du module pour que les tests la changent.
CONFIG_ROOT = DEFAULT_CONFIG_ROOT


def create_polars_engine(registry: SourceRegistry, lake_root: str) -> PolarsEngine:
    return PolarsEngine(registry, PolarsLakeWriter(lake_root))


def main(run_id: str, source_name: str | None = None):
    run(run_id, source_name, config_root=CONFIG_ROOT, create_engine=create_polars_engine)


if __name__ == "__main__":
    args = parse_args()
    main(run_id=args.run_id, source_name=args.source_name)
