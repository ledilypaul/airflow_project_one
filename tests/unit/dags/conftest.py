import os
from pathlib import Path

import pytest

EXAMPLES_CONFIG_ROOT = Path(__file__).resolve().parents[3] / "examples" / "config"

# Airflow minimal setup for DAG tests (no real DB needed)
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:////tmp/test_airflow.db")
# Airflow 3 removed DagBag(include_examples=...); the example DAGs are excluded
# via this config/env var instead (must be set before any airflow.* import).
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
# P2-4: Cosmos (transform_dag) sends anonymous usage metrics over HTTP by
# default; never from the test suite.
os.environ.setdefault("AIRFLOW__COSMOS__ENABLE_TELEMETRY", "False")
# P2-5: OpenLineage is neutralised for the whole suite by tests/conftest.py.

# airflow-project-one est installé en mode éditable (`uv sync`), donc
# `pipeline.*` / `utils.*` / `storage.*` sont importables sans manipuler
# sys.path (voir CLAUDE.md, M5) — pas besoin d'ajouter la racine du projet ici.


@pytest.fixture(scope="session", autouse=True)
def _examples_config_root():
    """P5-3: the DAG factory builds one DAG per source of the config root. The repo's
    own root has none (P5-1), so the DagBags of these tests read the example sources
    (`ingest__orders`, `ingest__imdb_movies`) through `PIPELINE_CONFIG_ROOT`. Set when
    the first test runs, not at import: `pipeline.config.DEFAULT_CONFIG_ROOT` is read
    at import by the other suites and stays the repo's."""
    previous = os.environ.get("PIPELINE_CONFIG_ROOT")
    os.environ["PIPELINE_CONFIG_ROOT"] = str(EXAMPLES_CONFIG_ROOT)
    yield
    if previous is None:
        os.environ.pop("PIPELINE_CONFIG_ROOT", None)
    else:
        os.environ["PIPELINE_CONFIG_ROOT"] = previous
