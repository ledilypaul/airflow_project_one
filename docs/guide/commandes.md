# Commandes

Toutes les commandes courantes passent par le `Makefile` (`make` seul les liste). Cette page les
range par usage. Un test garde la page à jour : il échoue si une cible du Makefile n'y figure pas.

## Qualité (les mêmes commandes que la CI)

| Commande | Effet |
|---|---|
| `make install` | installe le code, Airflow et les outils de développement dans `.venv` (`uv sync`) |
| `make lint` | `ruff check` et `ruff format --check` |
| `make fmt` | formate avec `ruff`, puis applique ses corrections sûres |
| `make test` | génère le manifest dbt puis lance toute la suite `pytest` (le premier appel télécharge dbt) |
| `make check` | ordre de la CI : `uv lock --check` (racine et `transform/`), puis `lint`, puis `test` |

## Services (Docker Compose)

| Commande | Effet |
|---|---|
| `make db-up` | démarre Postgres (port hôte 5434) et Redis ; vérifie d'abord que le port est libre |
| `make db-port-check` | échoue si le port 5434 est tenu par autre chose que le Postgres du projet |
| `make db-down` | arrête les conteneurs, **sans** supprimer le volume de données |

## Airflow

| Commande | Effet |
|---|---|
| `make airflow-init` | migre la base, crée ou met à jour les Connections et les pools (idempotent) |
| `make airflow` | génère le manifest dbt, télécharge le jar de lignage, puis lance `airflow standalone` |
| `make demo` | copie les fichiers de `examples/data/` dans `data/`, puis lance `make airflow` avec `PIPELINE_CONFIG_ROOT=examples/config` (sources d'exemple `imdb_movies` et `orders`) |

## Spark et dbt, hors d'Airflow

| Commande | Effet |
|---|---|
| `make spark-job RUN_ID=<id> [SOURCE=<nom>]` | lance le job de lecture seul, pour le `dag_run_id` d'un run `ingest__<source>` (`SOURCE` : seulement les lignes de cette source) |
| `make transform-sync` | crée ou met à jour `transform/.venv` (dbt-core, dbt-duckdb) |
| `make dbt-codegen` | génère la couche staging dbt (`transform/models/staging/generated/`) depuis les YAML de sources ; `dbt-parse` et `dbt-build` la lancent, `make check` et la CI vérifient qu'elle est à jour |
| `make dbt-parse` | écrit `transform/target/manifest.json`, dont `transform_dag` est rendu |
| `make dbt-build` | lance `dbt build` ; `LAKE_ROOT` et `DBT_DUCKDB_PATH` sont surchargeables |

Ne lancez pas `make dbt-build` pendant que `transform_dag` écrit le même fichier DuckDB.

## API REST d'Airflow

| Commande | Effet |
|---|---|
| `make api-dags` | liste les DAGs, en pause ou actifs |
| `make api-runs DAG=<dag_id>` | derniers runs d'un DAG |
| `make api-trigger DAG=<dag_id> [CONF='{...}']` | démarre un run |

Détails : [](../exploitation/api-rest.md).

## Lignage (OpenLineage)

| Commande | Effet |
|---|---|
| `make lineage-jar` | télécharge le jar Spark épinglé, ou revérifie son empreinte SHA-256 |
| `make lineage-transport` | affiche le transport (JSON) utilisé par `make airflow` |
| `make lineage-show` | résume les événements de `lineage/events` : jobs, entrées, sorties |
| `make lineage-clean` | supprime les fichiers d'événements, garde le jar |
| `make lineage-up` | démarre Marquez (API `127.0.0.1:5002`, interface `127.0.0.1:3002`) |
| `make lineage-down` | arrête et supprime les conteneurs Marquez, garde leur volume |

Détails : [](../exploitation/lignage.md).

## Métriques et alertes (OpenTelemetry, Prometheus)

| Commande | Effet |
|---|---|
| `make metrics-env` | affiche les variables OpenTelemetry que `make airflow` exporte |
| `make observability-up` | démarre le collecteur (`127.0.0.1:4318`) et Prometheus (`127.0.0.1:9092`) |
| `make observability-down` | les arrête et les supprime, garde le volume Prometheus |

Détails : [](../exploitation/observabilite.md).

## Kubernetes (kind et Helm)

Optionnel : `make airflow`, `make check` et `make spark-job` n'y touchent jamais.

| Commande | Effet |
|---|---|
| `make k8s-tools` | installe `kind`, `kubectl` et `helm` (versions épinglées, SHA-256 vérifié) |
| `make kind-up` | crée le cluster local à un nœud |
| `make image` | construit l'image du projet (Airflow, projet, dbt, Spark) |
| `make kind-load` | charge l'image dans le nœud |
| `make k8s-chart` | télécharge le chart Helm officiel d'Airflow (version épinglée) |
| `make k8s-install` | installe ou met à jour le déploiement (`helm upgrade --install`) |
| `make k8s-load-data DATA=<fichier>` | copie un fichier source dans le volume partagé |
| `make k8s-ui` | redirige l'interface Airflow vers `http://127.0.0.1:8081` |
| `make k8s-ui-password` | affiche le mot de passe de l'utilisateur `admin` (dev seulement) |
| `make k8s-spark-clean` | supprime les pods de pilote Spark terminés |
| `make k8s-uninstall` | retire les deux releases Helm, garde les Secrets et les volumes |
| `make kind-down` | supprime le cluster et tout ce qu'il contient |

Détails : [](../exploitation/kubernetes.md).

## Documentation

| Commande | Effet |
|---|---|
| `make docs` | construit le HTML dans `docs/_build/html` ; les avertissements sont des erreurs, comme en CI |
| `make docs-serve` | sert la documentation sur `http://127.0.0.1:8000`, reconstruite à chaque modification |
| `make docs-clean` | supprime `docs/_build` |
