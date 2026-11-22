# Glossaire

```{glossary}
Asset
  Objet d'Airflow 3 qui représente des données. Un DAG le **produit** (`outlets`) ou s'en sert pour
  se **déclencher** (`schedule`). Les trois Assets du projet sont `file_list`, `lake_staging` et
  `dbt_marts`, identifiés par leur nom.

Cosmos
  Bibliothèque d'Astronomer qui transforme un projet dbt en DAG Airflow : un producteur lance
  `dbt build`, un capteur par modèle ou test rapporte son statut.

DAG run
  Une exécution d'un DAG, identifiée par son `run_id`. Le `dag_run_id` d'un listing sert de clé de
  partition dans le lake.

dbt
  Outil de transformation SQL. Ici, il construit des tables DuckDB à partir de la zone `staging`.

DuckDB
  Base analytique embarquée dans un fichier, avec un seul écrivain à la fois.

file_list
  Table Postgres qui garde, pour chaque listing, une ligne par fichier avec son statut.

Idempotence
  Propriété d'un traitement dont la relance produit exactement le même résultat. Règle n°1 du projet.

Lake
  Les dossiers Parquet écrits par le job Spark sous `LAKE_ROOT` : zones `raw`, `staging` et
  `quality`.

Lignage
  Le graphe « quelle donnée vient de quelle donnée, produite par quel traitement ». Ici, par
  OpenLineage.

Listing
  Une exécution de `list_files_dag` : elle enregistre les fichiers trouvés, sous un même
  `dag_run_id`.

OpenLineage
  Standard ouvert d'événements de lignage, que le projet émet depuis Airflow, Spark et dbt.

Partition
  Sous-dossier d'une table du lake, ici `lake_run_id=.../lake_source_file=...`.

Pool
  Limite de concurrence d'Airflow. `spark` : une application Spark à la fois en local ; `dbt_duckdb` :
  un seul écrivain DuckDB.

Raw, staging
  Les deux zones d'écriture du lake : le fichier tel qu'il a été lu, puis le même nettoyé et contrôlé.

Snapshot (instantané)
  La partition d'un fichier pour un listing donné. L'état courant est le plus récent
  (`latest_snapshot`).

Source (config de)
  Fichier YAML de `config/sources/` qui décrit comment lire un type de fichier.

Triggerer
  Processus d'Airflow qui exécute les tâches *différables* : ici, les capteurs de `transform_dag`.
```
