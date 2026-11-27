# Les DAGs

Le projet n'a pas de DAG d'ingestion écrit à la main : `dags/source_pipelines.py` en **génère un par
source**, à partir des YAML de `config/sources/` (P5-3). Un YAML ajouté suffit : le DAG
`ingest__<nom>` apparaît au prochain parsing. Il reste un DAG écrit à la main, `transform_dag`.

```{mermaid}
flowchart TB
    subgraph ING["ingest__SOURCE (planning : celui du YAML, max_active_runs=1)"]
        lf["list_files"] --> inf["insert_files"]
        inf --> sp["spark_read_files"]
        sp --> pub["publish_lake_staging<br/>outlet : lake_staging__SOURCE"]
        sp --> fail["fail_if_spark_failed"]
    end
    subgraph TRANS["transform_dag (planning : l'un des Assets lake_staging__*, max_active_runs=1)"]
        prod["producteur : un seul dbt build"] --> sens["un capteur par modèle et test"]
        sens --> mart["publish_dbt_marts<br/>outlet : dbt_marts"]
    end
    pub -. "lake_staging__SOURCE" .-> prod
```

## Ce que la fabrique génère

| `dag_id` | Quand | Contenu |
|---|---|---|
| `ingest__<source>` | une par YAML valide de `config/sources/` | liste, enregistre, lit, publie |
| `ingest__unmatched` | toujours | les fichiers qu'**aucune source ne réclame** et qu'un défaut par extension sait lire (`config/defaults/`) : zone raw seulement, planning manuel, pas d'Asset |
| `invalid_source__<fichier>` | une par YAML invalide, ou dont le DAG n'a pas pu être construit | une tâche qui **échoue** avec le message de validation |

**Une config invalide ne casse pas les autres DAGs.** Une exception au parsing supprimerait tous les
DAGs du fichier. Chaque YAML est donc chargé à part ; celui qui ne passe pas donne un DAG
`invalid_source__<nom du fichier>`, visible dans l'interface, dont la seule tâche
(`report_invalid_config`) échoue en affichant le chemin du fichier et chaque erreur. Sont
invalides : une erreur de champ, un nom de source déjà pris (le premier fichier gagne), des tables
du lake imbriquées dans celles d'une source déjà retenue, un planning qu'Airflow refuse
(`99 99 * * *`). La règle d'alerte `AirflowDagImportErrors` ne voit pas ces DAGs (ce ne sont pas des
erreurs d'import) : la CI valide toutes les configs. Une racine de config introuvable, un
`pipeline.yaml` invalide ou un `PIPELINE_DEFAULT_ENGINE` inconnu restent, eux, une **erreur
d'import** du fichier : c'est une erreur globale, pas celle d'une source.

Le parsing reste **pur** : ni base, ni réseau, ni PySpark (un test le vérifie dans un processus
neuf) ; 50 sources se construisent en un dixième de seconde environ (un test impose moins d'une
seconde). Le projet doit être importable au parsing (installation éditable, ou `PYTHONPATH` dans
l'image) : la fabrique charge le registre des configs.

## `ingest__<source>`

Liste les fichiers de la source, les enregistre, lance Spark, signale que le lake a de nouvelles
données.

| Tâche | Rôle |
|---|---|
| `list_files` | les fichiers des répertoires `listing.directories` (sinon `default_directories` de `config/pipeline.yaml`) dont le **nom** correspond à `source.patterns`, moins `listing.exclude` (motifs `fnmatch`, casse ignorée, pas de récursion) |
| `insert_files` | écrit une ligne `pending` par fichier dans `file_list`, marquée du `dag_run_id` **et du nom de la source**, par la Connection Airflow `file_list_db` ; rend les arguments du job (`--run-id`, `--source-name`) |
| `spark_read_files` | un `spark-submit` pour ce run, dans le pool `spark` |
| `publish_lake_staging` | émet l'Asset `lake_staging__<source>` si au moins un fichier est `processed` (seulement pour une source qui a un `staging_path`) |
| `fail_if_spark_failed` | garde le run en échec si l'application Spark a échoué |

- **Planning : celui du YAML.** `schedule` est une expression cron (UTC), un preset Airflow
  (`@daily`) ou `null` : lancement manuel (ou par l'[API REST](../exploitation/api-rest.md)).
  `retries` (défaut 1) et `tags` sont aussi du YAML. Voir [](../configuration/sources.md).
- **`max_active_runs=1`** : un listing ne chevauche jamais le run précédent de sa source.
- **Rien à traiter n'est pas une erreur** : si aucun fichier n'est trouvé (ou si tous ont disparu
  avant l'insertion), `insert_files` est *skipped*, et avec lui `spark_read_files` : pas de run
  rouge, pas de retry inutile, aucun `spark-submit`, aucun Asset.
- **Un répertoire absent est une erreur** : `list_files` échoue (problème de déploiement, pas un
  listing vide).
- Réexécuter `insert_files` pour le même run ne crée pas de doublon : la table a un index unique sur
  `(file_path, dag_run_id, source_name)` et l'insertion fait `ON CONFLICT DO NOTHING`. Le nom de la
  source fait partie de la clé : deux DAGs de sources différentes peuvent avoir le même
  `dag_run_id` (un `scheduled__...` à la même minute), et deux sources peuvent réclamer le même
  fichier.
- **Relancer une lecture** : *Clear* de `spark_read_files` relit les fichiers du même listing (le
  job relit les lignes `(run_id, source_name)`, sans re-lister). Un nouveau run relance un listing.

**Pool `spark`.** Il limite le nombre d'applications Spark simultanées : une seule en local,
puisque chaque application `local[*]` prend tous les cœurs (donc deux sources ne lisent pas en même
temps). Un pool manquant est **silencieux** : le DAG se charge, la tâche reste en `scheduled`
jusqu'à `dagrun_timeout` (6 heures), et seul le journal du scheduler le dit.

**Master Spark.** C'est l'hôte de la Connection `spark_default` (`local[*]` en dev). Si elle
manquait, le fournisseur retomberait sans bruit sur `yarn` ; `spark_read_files` la vérifie d'abord
et fait échouer la tâche avec un message clair.

**Environnement du job.** `PIPELINE_DEFAULT_ENGINE` et `PIPELINE_CONFIG_ROOT` sont passés au job
avec les valeurs **vues par le parsing du DAG** (`env_vars` de l'opérateur : ajoutées à
l'environnement du worker en local, écrites en `spark.*.env` sur YARN et Kubernetes). Le job
valide ainsi les configs sous le même moteur par défaut et la même racine que le DAG, même quand le
pod du driver a un autre environnement.

**Retries.** `ReadFilesSparkSubmitOperator`, une sous-classe de `SparkSubmitOperator`, lit le code
de sortie du job. Les codes **déterministes** (2, 3, 5, 6 : voir [](job-spark.md)) deviennent une
`AirflowFailException`, jamais rejouée puisqu'elle échouerait de la même façon. Les autres échecs
(1, 4, un signal, une erreur avant le lancement) gardent les `retries` de la source.

**Pas de ressources Spark dans le DAG.** La mémoire et les cœurs relèvent de la Connection ou du
`spark-defaults.conf` de chaque environnement.

**Le moteur (`source.engine`) n'est pas encore consulté** : toute source est lue par Spark. Polars
s'exécutera dans Airflow avec P5-8.

## `transform_dag`

Reconstruit les tables DuckDB avec dbt, orchestré par Cosmos. Détails : [](dbt.md).

- Planifié sur **l'un des Assets `lake_staging__<source>`** (un `AssetAny`, un Asset par source qui
  a un `staging_path`, parmi les configs valides) avec `batch_asset_events=True` : chaque exécution
  reconstruit toute la zone `staging`, donc les événements arrivés pendant un `dbt build`, de
  n'importe quelles sources, sont consommés ensemble par **une seule** exécution suivante. Sans
  aucune source à staging, le DAG est manuel.
- `max_active_runs=1`, sans rattrapage.
- Émet l'Asset `dbt_marts` quand tout a réussi.
- **Un événement émis pendant la pause du DAG n'est jamais consommé** (Airflow 3.4) : dépausez
  `transform_dag` avant le premier listing. Le prochain événement reconstruit tout de toute façon.

## Callbacks d'échec

Tous les DAGs écrivent une ligne `airflow_failure {...}` au niveau ERROR à chaque échec définitif
(tâche ou run). Voir [](../exploitation/observabilite.md).

## Les Assets

Les Assets sont identifiés **par leur nom** (Airflow les rapproche par ce nom, pas par l'objet).
Ils portent le nom de la source, validé comme identifiant (`[a-z0-9_]+`).

| Asset | Émis par | Consommé par |
|---|---|---|
| `lake_staging__<source>` | `publish_lake_staging` de `ingest__<source>`, si au moins un fichier est `processed` | `transform_dag` (union) |
| `dbt_marts` | `publish_dbt_marts` | *(aucun consommateur pour l'instant)* |

L'Asset `file_list` d'avant P5-3 a disparu : listage et lecture sont dans le même DAG, la table
`file_list` n'est plus qu'un état partagé avec le job.

Les trois DAGs historiques (`list_files_dag`, `list_files_dag_classic`, `read_file_dag`) et
`config/extractor_config.yaml` ont été retirés en même temps que la fabrique : les laisser actifs
aurait fait traiter deux fois les mêmes fichiers. Le chargement de DAGs depuis du YAML annoncé par
Airflow 3.4 est expérimental : il n'est pas utilisé.
