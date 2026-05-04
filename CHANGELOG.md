# Changelog

Toutes les évolutions notables du projet sont documentées ici.

Format : [Keep a Changelog](https://keepachangelog.com/fr/1.0.0/)  
Versioning : [Semantic Versioning](https://semver.org/lang/fr/) — `MAJOR.MINOR.PATCH`

---

## Comment lire ce fichier

| Section | Signification |
|---|---|
| `Added` | Nouvelle fonctionnalité |
| `Changed` | Modification d'une fonctionnalité existante |
| `Fixed` | Correction de bug |
| `Removed` | Suppression de code |
| `Deprecated` | Fonctionnalité qui sera supprimée dans une prochaine version |

---

## [Unreleased]

> Changements en cours, pas encore versionnés.

---

## [0.3.0] — 2026-05-04

### Fixed

- **`base_processor.py`** — `remove_duplicates()` ne faisait rien : `self.df.dropDuplicates()` ne réassignait pas le résultat. Corrigé en `self.df = self.df.dropDuplicates()`.
- **`functions_utils.py`** — Les `INSERT` n'étaient jamais persistés : `engine.connect()` remplacé par `engine.begin()` qui commit automatiquement à la sortie du `with`.
- **`functions_utils.py`** — `except psycopg2.DatabaseError` ne catchait jamais les erreurs SQLAlchemy. Remplacé par `except SQLAlchemyError`.
- **`functions_utils.py`** — `SELECT *` remplacé par des colonnes explicites ; accès par index numériques (`file[0]`) remplacé par accès par nom (`file["file_path"]`).
- **`local_list_files_dag.py`** — Annotation de type incorrecte `file_list = list[str]` (valeur par défaut) corrigée en `file_list: list[str]` (type hint).
- **`local_list_files_dag.py`** — Code mort (`return 0` après `raise ValueError`) supprimé.
- **`parquet_reader.py`** — Itération incorrecte sur les options (`for item in dict` donnait les clés, pas les paires). Corrigé en `for key, value in options.items()`.
- **`hive_writer.py`** — `HiveWriter` n'héritait pas de `BaseWriter` alors que l'import était présent. Héritage rétabli, imports inutiles (`db_connection`) supprimés, méthode renommée `write()` pour respecter l'interface abstraite.
- **`excel_reader.py`** — `import polars as pl` au niveau module causait un `ModuleNotFoundError` si Polars n'est pas installé. Import déplacé à l'intérieur de `read_polars()`.
- **`excel_reader.py`** — `super().__init__(spark)` manquant dans le constructeur, rendant `self.config_path` indisponible.
- **`local_list_files_dag_python_operator.py`** — `dag_id="list_files_dag"` en conflit avec `local_list_files_dag.py`. Renommé en `list_files_dag_classic`.

### Changed

- **`functions_utils.py`** — `insert_into_file_list()` accepte maintenant un paramètre optionnel `dag_run_id` pour tracer l'origine de chaque insertion.
- **`functions_utils.py`** — `list_file_from_db()` retourne désormais des `RowMapping` (accès par nom de colonne) au lieu de tuples.
- **`functions_utils.py`** — Requêtes SQL wrappées avec `sqlalchemy.text()` pour compatibilité SQLAlchemy 2.0.
- **`db_connection.py`** — Duplication des 5 variables d'environnement supprimée via helper privé `_get_db_env()`.
- **`postgresql_writer.py`** — Typo corrigé : `PostgresWroter` → `PostgresWriter`.
- **`local_list_files_dag.py`** — La task `insert_files` renseigne désormais le `dag_run_id` via `get_current_context()`.
- **`read_files_job.py`** — Accès aux champs par nom (`file["file_path"]`) au lieu d'index numériques.
- **`pyproject.toml`** — Version PySpark alignée sur `requirements.txt` : `^4.0.0` → `^3.5.4`.
- **`extractor_config.yaml`** — Chemins source inexistants remplacés par `./data/` avec commentaire explicatif.
- `print()` remplacé par `logging.getLogger(__name__)` dans tous les modules métier.

### Added (tests)

- `tests/unit/jobs/test_read_files_job.py` — Tests unitaires pour le job Spark standalone.
- `tests/unit/processor/test_base_processor.py` — Tests pour toutes les méthodes de `BaseProcessor`.
- `tests/unit/utils/test_path_utils.py` — Tests pour `normalize_spark_path()`.
- `tests/unit/storage/test_postgresql_writer.py` — Tests pour `PostgresWriter`.
- `tests/unit/dags/test_dags.py` — Tests de structure des DAGs (chargement, tâches, dépendances).
- `tests/unit/utils/test_db_connection.py` — Réécriture complète (l'original mockait psycopg2 au lieu de SQLAlchemy).
- `tests/unit/utils/test_functions_utils.py` — Réécriture complète (l'original utilisait des curseurs psycopg2 inexistants).

---

## [0.2.0] — 2025-11-08

### Added

- `pipeline/jobs/read_files_job.py` — Script Spark standalone exécutable via `spark-submit`, indépendant d'Airflow.
- `dags/local_list_files_dag_python_operator.py` — Version alternative du DAG en style PythonOperator classique, avec documentation détaillée des bonnes pratiques Airflow.

### Changed

- **`local_read_file_dag.py`** — Refactorisé : `PythonOperator` avec SparkSession remplacé par `BashOperator` + `spark-submit`. Spark tourne désormais dans un processus séparé.
- **`local_list_files_dag.py`** — Migré vers TaskFlow API (`@dag` / `@task`). Imports métier déplacés à l'intérieur des tasks pour éviter l'exécution au parse.
- **`local_read_file_dag.py`** — Migré vers TaskFlow API. Les deux tâches (list + read) fusionnées en une seule (le job Spark requête lui-même la DB).
- `schedule_interval` (déprécié) remplacé par `schedule` dans tous les DAGs.

### Fixed

- Instantiation de `Extractor` au niveau module dans les DAGs (exécutée à chaque parse Airflow ~30s). Déplacée à l'intérieur des callables.

---

## [0.1.0] — 2024-12-18

### Added

- Structure initiale du projet : `pipeline/`, `utils/`, `storage/`, `dags/`, `config/`, `tests/`.
- `Extractor` : liste les fichiers par répertoire, extension, patterns include/exclude.
- `BaseFileReader` + `ReaderFactory` : lecture CSV, Parquet, Excel via Spark.
- `BaseProcessor` : transformations chaînables (dedup, cast, rename, fill_na…).
- `BaseWriter` + `PostgresWriter` + `HiveWriter` : écriture vers PostgreSQL (JDBC + SQLAlchemy) et Hive.
- `db_connection.py` / `functions_utils.py` : connexion PostgreSQL via SQLAlchemy, insert/select sur `file_list`.
- `docker-compose.yaml` : PostgreSQL 17 + Redis 7 pour le développement local.
- `utils/SQL/script_tables.sql` : schéma de la table `file_list`.
- Configs YAML par dataset : `imdb_config.yaml`, `orders_config.yaml`, `basic_reading_config.yaml`.
