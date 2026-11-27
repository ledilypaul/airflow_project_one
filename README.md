# airflow_project_one

Pipeline d'ingestion de fichiers orchestré par **Apache Airflow 3**, exécuté par **Apache Spark** et
prolongé par **dbt** : des fichiers déposés dans un dossier deviennent un *lake* Parquet partitionné
(zones `raw`, `staging` et résultats de qualité), puis des tables DuckDB, avec le lignage de bout
en bout.

```text
fichiers ─▶ ingest__<source> (un DAG par YAML) ─▶ job Spark ─▶ lake Parquet
              │  file_list (Postgres)                              │
              └──────────────────────────────  tables DuckDB (marts) ◀─ transform_dag (dbt)
```

Un fichier YAML par source suffit : le DAG d'ingestion est généré. dbt démarre quand une ingestion a
réussi et l'a signalé par un *Asset* Airflow. Relancer un
run produit le même résultat (idempotence), et chaque fichier garde un statut lisible
(`pending`, `processed`, `failed`) avec la raison de l'échec.

## Démarrage rapide

Prérequis : Python 3.12 ou 3.13, [uv](https://docs.astral.sh/uv/) 0.11 ou plus, Java 17, Docker,
`make`.

```bash
git clone https://github.com/ledilypaul/airflow_project_one.git
cd airflow_project_one
uv sync
# créez un fichier .env (DB_USER, DB_PASSWORD, DB_HOST, DB_PORT=5434, DB_NAME), voir la documentation

make db-up           # Postgres et Redis (docker compose)
make airflow-init    # base Airflow, Connections, pools
make airflow         # http://localhost:8080
```

Dépausez ensuite `ingest__<source>` et `transform_dag`, puis lancez `ingest__<source>` (ou donnez-lui
un `schedule` dans son YAML) : le reste se déclenche seul.

## Documentation

La documentation complète est dans [`docs/`](docs/index.md), construite avec Sphinx :

```bash
make docs          # HTML dans docs/_build/html
make docs-serve    # http://127.0.0.1:8000, rechargé à chaque modification
```

Elle couvre l'installation, un premier run pas à pas, l'architecture, la configuration des sources
et des contrôles qualité, l'exploitation (lignage, métriques et alertes, Kubernetes, API REST, CI) et
la référence de l'API Python.

`make` seul liste les commandes du projet. `make check` rejoue ce que fait la CI (verrous `uv`,
`ruff`, tests).

## Contribuer

Une branche `feat/...` ou `fix/...` par changement, une PR, un test de non-régression pour chaque
correctif. Les règles et les conventions sont dans la page *Contribuer* de la documentation et dans
[`CLAUDE.md`](CLAUDE.md).
