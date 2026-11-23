# airflow_project_one

Pipeline d'ingestion de fichiers orchestré par **Apache Airflow 3**, exécuté par **Apache Spark**
et prolongé par **dbt** : des fichiers déposés dans un dossier deviennent un *lake* Parquet
partitionné (zones `raw`, `staging` et résultats de qualité), puis des tables DuckDB prêtes à
l'analyse, avec le lignage de bout en bout.

```{mermaid}
flowchart LR
    src[("Fichiers sources<br/>data/")] --> L["list_files_dag"]
    L -->|"lignes pending"| FL[("Postgres<br/>file_list")]
    L -. "Asset file_list" .-> R["read_file_dag"]
    FL --> R
    R -->|"spark-submit"| J["Job Spark<br/>read_files_job"]
    J --> Lake[("Lake Parquet<br/>raw · staging · quality")]
    R -. "Asset lake_staging" .-> T["transform_dag<br/>dbt via Cosmos"]
    Lake --> T
    T --> D[("DuckDB<br/>warehouse.duckdb")]
    T -. "Asset dbt_marts" .-> fin(["marts prêts"])
```

Chaque étape ne démarre que lorsque la précédente a **réussi et l'a signalé** par un *Asset*
Airflow : il n'y a ni déclenchement manuel ni planning à tenir. Relancer un run produit exactement
le même résultat (idempotence), et chaque fichier garde un statut lisible : `pending`,
`processed` ou `failed`, avec la raison.

## Par où commencer

- **Faire tourner le projet** : [](guide/installation.md), puis [](guide/premier-run.md).
- **Comprendre comment il est construit** : [](concepts/vue-densemble.md).
- **Ajouter une source de données** : [](configuration/sources.md).
- **Exploiter ou déployer** : [](exploitation/observabilite.md) et [](exploitation/kubernetes.md).

## Contenu

```{toctree}
:caption: Guide de démarrage
:maxdepth: 1

guide/installation
guide/premier-run
guide/commandes
```

```{toctree}
:caption: Concepts et architecture
:maxdepth: 1

concepts/vue-densemble
concepts/dags
concepts/job-spark
concepts/lake
concepts/dbt
```

```{toctree}
:caption: Configuration
:maxdepth: 1

configuration/sources
configuration/qualite
```

```{toctree}
:caption: Exploitation
:maxdepth: 1

exploitation/base-de-donnees
exploitation/lignage
exploitation/observabilite
exploitation/api-rest
exploitation/kubernetes
exploitation/ci-securite
```

```{toctree}
:caption: Référence
:maxdepth: 2

reference/api
glossaire
```

```{toctree}
:caption: Projet
:maxdepth: 1

contribuer
decisions
```
