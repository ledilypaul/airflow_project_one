# Vue d'ensemble

Le pipeline a trois étages. Chacun est un DAG Airflow ; le passage de l'un à l'autre se fait par
des *Assets*, pas par un planning.

```{mermaid}
sequenceDiagram
    autonumber
    participant S as Fichiers (data/)
    participant L as list_files_dag
    participant P as Postgres (file_list)
    participant R as read_file_dag
    participant J as Job Spark
    participant K as Lake Parquet
    participant T as transform_dag (dbt)
    participant D as DuckDB

    L->>S: cherche les fichiers (extension, motifs)
    L->>P: une ligne pending par fichier
    L-->>R: Asset file_list
    R->>P: lit les lignes du listing
    R->>J: spark-submit --run-id=...
    J->>K: écrit raw, puis staging et quality
    J->>P: processed ou failed (avec la raison)
    R-->>T: Asset lake_staging
    T->>K: lit la zone staging
    T->>D: construit les tables (dbt build)
    T-->>T: Asset dbt_marts
```

## Les trois étages

| DAG | Rôle | Déclenché par |
|---|---|---|
| `list_files_dag` | cherche les fichiers sources et les enregistre dans `file_list` | à la main (pas de planning) |
| `read_file_dag` | lance Spark, qui lit chaque fichier et l'écrit dans le lake | l'Asset `file_list` |
| `transform_dag` | reconstruit les tables DuckDB avec dbt | l'Asset `lake_staging` |

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
├── dags/              les DAGs Airflow (importés par chemin, voir ci-dessous)
├── pipeline/
│   ├── extractor/     liste les fichiers sources (Extractor)
│   ├── config/        modèles Pydantic des configs de lecture, registre, contrôles qualité
│   ├── file_reader/   lecteurs CSV, Parquet, Excel et la fabrique qui les choisit
│   ├── processor/     nettoyage d'un DataFrame (lignes vides, doublons…)
│   ├── quality/       moteur des contrôles qualité (Spark pur)
│   └── jobs/          le job Spark read_files_job
├── storage/           écriture : ParquetLakeWriter (le lake), PostgreSQL, Hive
├── utils/             connexion DB, session Spark, chemins, lignage, alertes d'échec
├── config/
│   ├── extractor_config.yaml   quoi lister
│   ├── sources/*.yaml          une config de lecture dédiée par source
│   └── defaults/*.yaml         le repli par extension
├── transform/         projet dbt (son propre projet uv)
├── deploy/            Helm, Kubernetes, observabilité, pools Airflow
├── scripts/           outils en ligne de commande (connexions, API, lignage…)
├── docs/              cette documentation
└── tests/             la suite de tests (unitaires et d'intégration)
```

Les fichiers de `dags/` ajoutent la racine du projet à `sys.path` : Airflow les importe directement
par chemin, sans passer par un paquet installé.

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
