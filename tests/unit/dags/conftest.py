import os

# Airflow minimal setup for DAG tests (no real DB needed)
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:////tmp/test_airflow.db")
# Airflow 3 removed DagBag(include_examples=...); the example DAGs are excluded
# via this config/env var instead (must be set before any airflow.* import).
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

# airflow-project-one est installé en mode éditable (`uv sync`), donc
# `pipeline.*` / `utils.*` / `storage.*` sont importables sans manipuler
# sys.path (voir CLAUDE.md, M5) — pas besoin d'ajouter la racine du projet ici.
