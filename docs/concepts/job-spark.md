# Le job Spark

`pipeline/jobs/read_files_job.py` (point d'entrée Spark ; l'orchestration, commune aux moteurs, est
dans `pipeline/jobs/ingest.py`) lit les fichiers d'**un listing** et les écrit dans le lake. Il
tourne dans son propre processus, lancé par `spark-submit`, et se lance aussi seul, sans Airflow :

```bash
make spark-job RUN_ID=<dag_run_id_du_listing> SOURCE=<nom_de_la_source>
# équivaut à : uv run spark-submit pipeline/jobs/read_files_job.py --run-id=<id> --source-name=<nom>
```

`--run-id` est obligatoire. `--source-name` (P5-3) restreint le job aux lignes `(run_id, source_name)` :
les `dag_run_id` de deux DAGs de sources différentes peuvent être égaux. `ingest__<source>` le
passe toujours ; sans lui (`make spark-job RUN_ID=...` seul), le job lit toutes les lignes du run. Le job lit ses lignes dans `file_list` par la connexion du `.env`
(`DB_*`), pas par Airflow. Il écrit sous `LAKE_ROOT` (par défaut `./lake/`, ignoré par git).

## Ce qu'il fait pour chaque fichier

1. **résout la config** du fichier (dédiée si un motif correspond, sinon le repli par extension) ;
2. **lit** le fichier avec le lecteur de son format ;
3. **remet la ligne à `pending`** (un retry repart d'un état propre) ;
4. **écrit** selon `storage` :
   - le fichier tel quel dans la zone `raw` ;
   - si la source a un `staging_path` : retire les lignes entièrement vides et les doublons exacts,
     lance les contrôles qualité s'il y en a ([](../configuration/qualite.md)), puis écrit `staging`
     sauf si un contrôle est bloquant ;
5. **marque la ligne** `processed`, ou `failed` avec l'erreur.

Deux fichiers de même nom dans un même listing sont tous deux `failed` : leurs partitions
entreraient en collision.

## Codes de sortie

| Code | Constante | Signification | Rejoué par Airflow ? |
|---|---|---|---|
| 0 | | tous les fichiers sont `processed` | — |
| 1 | `EXIT_FILES_FAILED` | au moins un fichier `failed` pour une autre raison (erreur de lecture ou d'écriture, moteur de qualité en erreur, deux fichiers de même nom…) | oui |
| 2 | `EXIT_INVALID_RUN_ID` | `--run-id` absent ou vide | non |
| 3 | `EXIT_NO_ROWS` | le listing n'a aucune ligne (faute de frappe, données purgées) | non |
| 4 | `EXIT_PARENT_GONE` | le processus parent a disparu (voir plus bas) ; aucun statut écrit | oui |
| 5 | `EXIT_INVALID_CONFIG` | config de lecture invalide, source sans `storage`, tables du lake imbriquées, `LAKE_ROOT` relatif, ou expression régulière de qualité refusée par Java | non |
| 6 | `EXIT_QUALITY_FAILED` | des fichiers `failed` **uniquement** à cause de contrôles qualité bloquants qui ont trouvé des lignes en échec, résultats enregistrés | non |

À la fin d'un run, la priorité est : **0** si rien n'a échoué, sinon **6** si chaque échec est de ce
type qualité, sinon **1**. Les codes 2, 3, 5 et 6 sont *déterministes* : un retry échouerait pareil,
donc les DAGs `ingest__<source>` ne les rejouent pas ([](dags.md)).

Avec le code 5, chaque ligne du listing est marquée `failed` (`invalid reading config: ...` ou
`invalid lake root: ...`) et Spark n'est même pas démarré.

## Ordre des vérifications

Le job échoue le plus tôt possible, avant de payer le démarrage de Spark :

1. le chien de garde du parent (code 4) ;
2. l'identifiant de run (code 2) ;
3. les lignes du listing (code 3) ;
4. la validation des configs et la résolution de `LAKE_ROOT` (code 5) ;
5. le démarrage de Spark ;
6. la compilation, en Java, de chaque expression régulière de qualité (code 5) ;
7. enfin les fichiers.

## Le chien de garde du parent

Quand Airflow tue la tâche, `SparkSubmitHook.on_kill` ne tue que le processus `spark-submit` (la
JVM), pas le pilote Python qu'il a lancé. Sans protection, ce pilote continuerait d'écrire dans le
lake pendant que le retry démarre. Le job lance donc un fil de surveillance
(`start_parent_watchdog`) qui sort avec le code 4, sans écrire de statut, dès que son parent a
disparu.

## Idempotence d'un retry

Un retry retraite les fichiers du listing sans effet de bord :

- chaque écriture ne remplace que **sa** partition `(lake_run_id, lake_source_file)`, avec
  `partitionOverwriteMode=dynamic` passé comme option d'écriture (pas comme réglage de session) ;
- une réécriture qui produit zéro ligne **supprime** la partition ;
- chaque listing ajoute un instantané par fichier : l'état courant est donné par
  `latest_snapshot` (plus grand `lake_file_id`), pas par la dernière écriture ([](lake.md)).

Le job copie `file_list.id` dans le lake (`lake_file_id`) pour ordonner les instantanés : ne
réinitialisez donc jamais cette séquence (`TRUNCATE ... RESTART IDENTITY`, restauration).

## Environnement

| Variable | Rôle |
|---|---|
| `LAKE_ROOT` | racine du lake : un chemin absolu ou une URI avec hôte (`s3://...`). Par défaut `<racine du projet>/lake` |
| `ENV=prod` | passe Spark en mode cluster (master géré par YARN ou Kubernetes) et ajoute le paquet `spark-excel` pour les `.xlsx` ; sinon `local[*]` |
| `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_NAME` | connexion à Postgres |

En mode cluster, le pilote n'hérite pas de l'environnement du worker : `LAKE_ROOT` se passe par la
conf Spark de l'environnement (`spark.kubernetes.driverEnv.LAKE_ROOT`,
`spark.yarn.appMasterEnv.LAKE_ROOT`). Le déploiement Kubernetes le fait déjà
([](../exploitation/kubernetes.md)).
