# Vue d'ensemble

Le pipeline a deux étages : un DAG d'ingestion **par source** (généré depuis son YAML, P5-3), puis
un DAG dbt commun. Le passage de l'un à l'autre se fait par des *Assets*, pas par un planning.

```{mermaid}
sequenceDiagram
    autonumber
    participant S as Fichiers (data/)
    participant I as ingest__SOURCE
    participant P as Postgres (file_list)
    participant J as Job Spark
    participant K as Lake Parquet
    participant T as transform_dag (dbt)
    participant D as DuckDB

    I->>S: cherche les fichiers (répertoires, motifs de la source)
    I->>P: une ligne pending par fichier (run, source)
    I->>J: spark-submit --run-id=... --source-name=...
    J->>P: lit les lignes (run, source)
    J->>K: écrit raw, puis staging et quality
    J->>P: processed ou failed (avec la raison)
    I-->>T: Asset lake_staging__SOURCE
    T->>K: lit la zone staging
    T->>D: construit les tables (dbt build)
    T-->>T: Asset dbt_marts
```

## Les étages

| DAG | Rôle | Déclenché par |
|---|---|---|
| `ingest__<source>` (un par YAML de `config/sources/`) | cherche les fichiers de la source, les enregistre dans `file_list`, lance Spark, qui les lit et les écrit dans le lake | le `schedule` du YAML (cron, preset, ou à la main) |
| `ingest__unmatched` | pareil pour les fichiers qu'aucune source ne réclame (défauts par extension, zone raw seulement) | à la main |
| `transform_dag` | reconstruit les tables DuckDB avec dbt | l'un des Assets `lake_staging__<source>` |

Le détail de chaque DAG est dans [](dags.md).

## Principes de conception

**Idempotence d'abord.** Relancer un run produit exactement le même résultat. Les tâches travaillent
sur le périmètre du run (son `run_id`), jamais sur « aujourd'hui ». Le lake est écrit par
partition `(lake_run_id, lake_source_file)` : une écriture remplace sa partition et n'ajoute jamais.
Un retry réécrit donc les mêmes fichiers, sans doublon.

**Airflow orchestre, Spark travaille.** Airflow ne lit aucune donnée. Il lance le job Spark dans
son propre processus (`spark-submit`) et lit son code de sortie. Le job est exécutable seul,
sans Airflow installé (`make spark-job`).

:::{important}
Le job Spark **n'importe jamais Airflow**, ni directement ni par `pipeline/`, `utils/` ou
`storage/`. Dans une tâche Airflow 3, le superviseur du Task SDK remplace la connexion à la base de
métadonnées par une valeur qui l'interdit, et `spark-submit` hérite de cet environnement : importer
`airflow` y échoue. Le code qui a besoin d'Airflow l'importe donc *dans* les fonctions concernées.
Un test garde cette règle.
:::

**Un statut par fichier, et il dit la vérité.** Chaque ligne de `file_list` passe de `pending` à
`processed` ou `failed`, avec le message d'erreur. Un DAG ne réussit pas si une partie de son
travail a échoué en silence.

**Config plutôt que code.** Lire un nouveau type de fichier, c'est écrire un YAML validé
(voir [](../configuration/sources.md)), pas du Python.

**Le lignage et l'observabilité ne cassent jamais le pipeline.** Ils sont désactivés tant qu'on ne
les demande pas, et quoi qu'il leur arrive, le job tourne avec la commande d'avant, plus un
avertissement qui dit pourquoi.

## Où se trouve quoi

```text
airflow_project_one/
├── dags/              source_pipelines.py (la fabrique de DAGs d'ingestion), local_transform_dag.py
├── pipeline/
│   ├── extractor/     liste les fichiers d'une source (motifs, exclusions)
│   ├── config/        modèles Pydantic des configs de lecture, registre, contrôles qualité
│   ├── file_reader/   lecteurs CSV, Parquet, Excel et la fabrique qui les choisit
│   ├── processor/     nettoyage d'un DataFrame (lignes vides, doublons…)
│   ├── quality/       moteur des contrôles qualité (Spark pur)
│   └── jobs/          le job Spark read_files_job
├── storage/           écriture : ParquetLakeWriter (le lake), PostgreSQL, Hive
├── utils/             connexion DB, session Spark, chemins, lignage, alertes d'échec
├── config/
│   ├── pipeline.yaml           les répertoires listés par défaut
│   ├── sources/*.yaml          une config dédiée par source : lecture, écriture, listing, planning
│   └── defaults/*.yaml         le repli par extension
├── transform/         projet dbt (son propre projet uv)
├── deploy/            Helm, Kubernetes, observabilité, pools Airflow
├── scripts/           outils en ligne de commande (connexions, API, lignage…)
├── docs/              cette documentation
└── tests/             la suite de tests (unitaires et d'intégration)
```

Les fichiers de `dags/` sont importés par chemin, mais le projet doit être importable à ce moment
(installation éditable avec `uv sync`, ou `PYTHONPATH` dans l'image) : la fabrique charge les configs
au parsing, sans jamais importer PySpark.

## Technologies

| Rôle | Outil |
|---|---|
| Orchestration | Apache Airflow 3.4 (Task SDK, Assets) |
| Traitement | Apache Spark 4 (PySpark) |
| Stockage | Parquet partitionné (le lake), Postgres (`file_list`), DuckDB (les marts) |
| Transformation | dbt-core et dbt-duckdb, orchestrés par Astronomer Cosmos |
| Validation de config | Pydantic |
| Lignage | OpenLineage (sortie fichier, ou Marquez) |
| Métriques | OpenTelemetry, Prometheus |
| Déploiement local | Docker Compose ; kind et Helm pour Kubernetes |
