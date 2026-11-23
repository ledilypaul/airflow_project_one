# Les DAGs

Le projet a trois DAGs de production, dans `dags/`. Les fichiers `local_list_files_dag_python_operator.py`
(`list_files_dag_classic`, la même chose écrite avec `PythonOperator` plutôt que l'API TaskFlow) et
`test_dag.py` (un DAG vide) sont des exemples.

```{mermaid}
flowchart TB
    subgraph LIST["list_files_dag (planning : aucun)"]
        lf["list_files"] --> inf["insert_files<br/>outlet : file_list"]
    end
    subgraph READ["read_file_dag (planning : Asset file_list, max_active_runs=1)"]
        res["resolve_source_run_ids"] --> sp["spark_read_files<br/>une instance par listing"]
        sp --> pub["publish_lake_staging<br/>outlet : lake_staging"]
        sp --> fail["fail_if_spark_failed"]
    end
    subgraph TRANS["transform_dag (planning : Asset lake_staging, max_active_runs=1)"]
        prod["producteur : un seul dbt build"] --> sens["un capteur par modèle et test"]
        sens --> mart["publish_dbt_marts<br/>outlet : dbt_marts"]
    end
    inf -. file_list .-> res
    pub -. lake_staging .-> prod
```

## `list_files_dag`

Liste les fichiers sources et les enregistre.

| Tâche | Rôle |
|---|---|
| `list_files` | l'`Extractor` parcourt les dossiers de `config/extractor_config.yaml`, garde les extensions voulues, applique `include_files` puis retire `exclude_files` (motifs `fnmatch`) |
| `insert_files` | écrit une ligne `pending` par fichier dans `file_list`, marquée du `dag_run_id`, par la Connection Airflow `file_list_db` |

- **Aucun planning** : on le lance à la main (ou par l'[API REST](../exploitation/api-rest.md)).
- **Rien à traiter n'est pas une erreur** : si aucun fichier n'est trouvé, `insert_files` est
  *skipped* (pas de run rouge, pas de retry inutile) et n'émet rien.
- **L'Asset `file_list` n'est émis que si au moins un fichier a été enregistré** : un événement
  implique toujours au moins une ligne.
- Réexécuter la tâche pour le même run ne crée pas de doublon : la table a un index unique sur
  `(file_path, dag_run_id)` et l'insertion fait `ON CONFLICT DO NOTHING`.

## `read_file_dag`

Lance Spark pour chaque listing et signale que le lake a de nouvelles données.

| Tâche | Rôle |
|---|---|
| `resolve_source_run_ids` | collecte les `source_run_id` des événements `file_list` qui l'ont déclenché ; vérifie que la Connection `spark_default` existe |
| `spark_read_files` | un `spark-submit` par listing (tâche *mappée* : un statut, un code de sortie et des retries par listing) |
| `publish_lake_staging` | émet l'Asset `lake_staging` si au moins un fichier est `processed` |
| `fail_if_spark_failed` | garde le run en échec si une application Spark a échoué |

**Un run par listing.** Le DAG est planifié sur l'Asset `file_list` avec
`batch_asset_events=False` et `max_active_runs=1` : les événements qui arrivent pendant un run
attendent en file, et chacun donne son propre run, l'un après l'autre. Ce réglage est explicite
depuis Airflow 3.4, dont la valeur par défaut est un run par événement.

**Lancement manuel.** Sans événement, `resolve_source_run_ids` lit `dag_run.conf["run_id"]`
(le `dag_run_id` d'un listing). Sans événement ni `run_id`, le run échoue explicitement.

**Pool `spark`.** Il limite le nombre d'applications Spark simultanées : une seule en local,
puisque chaque application `local[*]` prend tous les cœurs. Un pool manquant est **silencieux** :
le DAG se charge, la tâche reste en `scheduled` jusqu'à `dagrun_timeout` (6 heures), et seul le
journal du scheduler le dit.

**Master Spark.** C'est l'hôte de la Connection `spark_default` (`local[*]` en dev). Si elle
manquait, le fournisseur retomberait sans bruit sur `yarn` ; `resolve_source_run_ids` la vérifie et
fait échouer le run avec un message clair.

**Retries.** `ReadFilesSparkSubmitOperator`, une sous-classe de `SparkSubmitOperator`, lit le code
de sortie du job. Les codes **déterministes** (2, 3, 5, 6 : voir [](job-spark.md)) deviennent une
`AirflowFailException`, jamais rejouée puisqu'elle échouerait de la même façon. Les autres échecs
(1, 4, un signal, une erreur avant le lancement) gardent `retries=1`.

**Pas de ressources Spark dans le DAG.** La mémoire et les cœurs relèvent de la Connection ou du
`spark-defaults.conf` de chaque environnement.

## `transform_dag`

Reconstruit les tables DuckDB avec dbt, orchestré par Cosmos. Détails : [](dbt.md).

- Planifié sur l'Asset `lake_staging` avec `batch_asset_events=True` : chaque exécution reconstruit
  toute la zone `staging`, donc les événements arrivés pendant un `dbt build` sont consommés
  ensemble par **une seule** exécution suivante.
- `max_active_runs=1`, sans rattrapage.
- Émet l'Asset `dbt_marts` quand tout a réussi.

## Callbacks d'échec

Les quatre DAGs écrivent une ligne `airflow_failure {...}` au niveau ERROR à chaque échec définitif
(tâche ou run). Voir [](../exploitation/observabilite.md).

## Les Assets

Les trois Assets (`file_list`, `lake_staging`, `dbt_marts`) sont identifiés **par leur nom** :
chaque fichier de DAG déclare `Asset(name="...")` lui-même, et Airflow les rapproche par ce nom.
Un module partagé ne marcherait pas : il ne s'importe qu'avec la racine du projet dans `sys.path`.

| Asset | Émis par | Consommé par |
|---|---|---|
| `file_list` | `insert_files` (les deux DAGs de listing), si au moins un fichier est enregistré | `read_file_dag` |
| `lake_staging` | `publish_lake_staging`, si au moins un fichier est `processed` | `transform_dag` |
| `dbt_marts` | `publish_dbt_marts` | *(aucun consommateur pour l'instant)* |
