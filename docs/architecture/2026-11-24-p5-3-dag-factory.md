# P5-3 — Fabrique de DAGs par source

- **Date :** 2026-11-24
- **Branche :** `feat/p5-3-dag-factory` (depuis `feat/p5-2-neutral-config`, P5-2 étant en PR #54)
- **Backlog :** `docs/audit/2026-11-23-backlog-phase5.md`, fiche P5-3 et règles transverses
- **Veille associée :** `docs/tech-watch/2026-11-24-p5-3-yaml-dags.md`

## Pourquoi

Trois DAGs fixes ingéraient tout : un listing **global** (`list_files_dag`, lisant
`config/extractor_config.yaml`), un DAG de lecture déclenché par l'Asset `file_list`
(`read_file_dag`) et `transform_dag`. Les motifs de fichiers étaient écrits deux fois
(`extractor_config.yaml` et `source.patterns`), un listing mélangeait toutes les sources dans un même
run, et ajouter une source demandait de toucher à deux endroits. Objectif de la phase 5 : **un YAML
de source suffit à obtenir son pipeline**.

## Ce qui a changé

- **Fabrique** (`dags/source_pipelines.py`). Pour chaque YAML valide de `config/sources/` : un DAG
  `ingest__<source.name>` = `list_files` -> `insert_files` -> `spark_read_files` ->
  `publish_lake_staging` (+ `fail_if_spark_failed`). Plus `ingest__unmatched` (fichiers qu'aucune
  source ne réclame et qu'un défaut par extension sait lire : zone raw seulement, manuel, pas
  d'Asset) et un `invalid_source__<fichier>` par config invalide (une tâche qui échoue avec le
  message de validation). Une source sans `staging_path` n'a ni `publish_lake_staging` ni Asset.
- **Le listing et la lecture sont dans le même DAG.** Plus d'Asset `file_list`, plus de
  `resolve_source_run_ids`, plus de `Param run_id` : le run qui liste est celui qui lit. Conséquence
  voulue : *Clear* de `spark_read_files` relit les fichiers du même run (le job relit les lignes
  `(run_id, source_name)`). `insert_files` rend les arguments du job (`--run-id`, `--source-name`),
  qui alimentent `application_args` par XCom (jamais rendus par Jinja).
- **Modèle** (`pipeline/config/models.py`) : sections `listing` (`directories`, `exclude`),
  `schedule` (cron, preset, `null`), `retries` (0 à 10), `tags` ; `source.name` devient un
  identifiant (`[a-z0-9_]+`, 100 au plus, `unmatched` réservé) : il nomme le DAG, l'Asset
  (`lake_staging__<nom>`, `SourceConfig.staging_asset_name`) et `file_list.source_name`.
- **Registre** (`pipeline/config/registry.py`) : `SourceRegistry.load_valid` rend les configs
  valides **et** le message de chacune qui ne l'est pas (erreur de fichier, nom en double, tables du
  lake imbriquées : la première gagne) ; `from_directory` (le job, strict, code de sortie 5) ne
  change pas de comportement. `default_directories` lit `pipeline.yaml` de la racine (sinon
  `config/pipeline.yaml` du moteur : `./data/`). `default_config_root()` relit
  `PIPELINE_CONFIG_ROOT` à chaque parsing (`DEFAULT_CONFIG_ROOT` est figée à l'import).
- **Listing** (`pipeline/extractor/extractor.py`) : `list_source_files` (motifs `source.patterns`
  moins `listing.exclude`, fnmatch sur le nom en minuscules, non récursif) et `list_unmatched_files`.
  Un répertoire absent lève toujours `FileNotFoundError` (P1-6). La classe `Extractor` et
  `check_integrity_files` (jamais appelée) sont retirées.
- **`file_list`** : colonne `source_name VARCHAR(100) NOT NULL DEFAULT ''`, index unique
  `(file_path, dag_run_id, source_name)` à la place de `idx_file_path`. `script_tables.sql` est
  **idempotent d'un bout à l'autre** et sert de migration. `insert_into_file_list`,
  `list_file_from_db` et `count_processed_files` prennent `source_name` ; le job accepte
  `--source-name` (facultatif : sans lui, comportement d'avant, pour `make spark-job RUN_ID=...`,
  qui accepte maintenant `SOURCE=`).
- **`transform_dag`** : planifié sur `AssetTriggeredTimetable(AssetAny(*assets),
  batch_asset_events=True)`, un Asset par source valide qui a un `staging_path`, relu à chaque
  parsing ; manuel sans aucune. Ses callbacks d'échec importent `utils.failure_alert` en tête de
  fichier (le repli « ligne minimale » pour un worker sans le projet disparaît : le fichier lit
  maintenant les configs au parsing, il ne se charge plus sans le projet).
- **Opérateur** (`utils/read_files_operator.py`, ex-corps de `read_file_dag`, code déplacé sans
  changement de comportement) : `ReadFilesSparkSubmitOperator`, `openlineage_spark_settings`, le mode
  Kubernetes. Nouveauté : `execute` vérifie d'abord que la Connection `spark_default` existe (ce
  que faisait `resolve_source_run_ids`).
- **Propagation de `PIPELINE_DEFAULT_ENGINE`** (suite du rapport de P5-2). `job_env_vars()` passe au
  job, dans `env_vars` de l'opérateur, `PYTHONPATH`, **`PIPELINE_DEFAULT_ENGINE`** (valeur lue par le
  parsing du DAG) et **`PIPELINE_CONFIG_ROOT`** (idem). En local/client le hook les ajoute à
  l'environnement de `spark-submit` ; sur YARN et Kubernetes il les écrit en
  `spark.*.env` / `spark.kubernetes.driverEnv.*` / `spark.executorEnv.*` (la conf n'est pas un
  secret). Le job valide donc les configs sous le moteur par défaut et la racine que le DAG a vus.
  Un `PIPELINE_DEFAULT_ENGINE` inconnu est une erreur d'import du fichier (globale), pas une
  config de source.
- **Retirés dans la même entrée** : `dags/local_list_files_dag.py`,
  `dags/local_list_files_dag_python_operator.py`, `dags/local_read_file_dag.py`,
  `config/extractor_config.yaml`. Interprétation de la fiche (« les trois DAGs historiques ») :
  `list_files_dag`, `list_files_dag_classic` et `read_file_dag` ; `transform_dag` reste, la fiche le
  planifie sur l'union des Assets.
- **Tests** : `tests/unit/dags/test_p5_3_dag_factory.py`, `tests/unit/config/test_p5_3_source_dag_sections.py`,
  `tests/unit/jobs/test_p5_3_source_scope.py`, `tests/unit/utils/test_p5_3_file_list_ddl.py`,
  `tests/unit/extractor/test_extractor.py` (réécrit) et `tests/integration/test_p1_4_ingest_dag_e2e.py`
  (ex `test_p1_4_read_file_dag_e2e.py`, porté). Portés sans changer leur sens : tests DAG des
  P1-2 à P3-5 (voir la PR). Retirés avec le code qu'ils testaient : `resolve_source_run_ids`, le
  `Param run_id`, la lecture par `triggering_asset_events`, le scheduler de `read_file_dag`
  (`test_p3_5_asset_scheduling.py` ne garde que `transform_dag`), le repli « ligne minimale ».
- **Docs** : `docs/concepts/dags.md` (réécrit), `vue-densemble.md`, `index.md`, `glossaire.md`,
  `configuration/sources.md`, `guide/premier-run.md`, `guide/commandes.md`, `exploitation/{base-de-donnees,
  api-rest,kubernetes,lignage}.md`, `concepts/job-spark.md`, `README.md`, `config/README.md`,
  `CLAUDE.md`, règles d'alerte (textes seulement).

## Faits vérifiés

- **Airflow 3.4.0, lu à la source** : `AssetAny` existe dans `airflow.sdk` ; `AssetTriggeredTimetable(AssetAny(a, b),
  batch_asset_events=True)` est accepté et se sérialise ; au niveau du scheduler (SQLite réelle,
  `_create_dagruns_for_dags`), trois événements de **deux** Assets en file donnent **un** run qui les
  consomme tous, avec `[scheduler] batch_asset_events` à `True` comme à `False`. Un événement émis
  pendant la pause n'est pas mis en file et n'est jamais consommé.
- **Les DAGs en YAML d'Airflow 3.4 sont expérimentaux et ne couvrent pas le besoin** (Assets,
  callbacks, `.expand()` non supportés) : non utilisés, voir la note de veille.
- **`DAG(auto_register=...)`**. Un DAG utilisé dans un bloc `with` est publié d'office par le
  DagBag, même abandonné après une exception : sans `auto_register=False`, une source dont la
  construction échoue donnait **à la fois** `invalid_source__<nom>` et un DAG à moitié construit.
  `dag.validate()` est appelé dans le `try` de la source : Airflow ne refuse un cron comme
  `99 99 * * *` qu'à ce moment-là, et l'erreur, tardive, aurait été une erreur d'import du fichier.
- **Parsing pur** : un processus neuf qui importe `dags.source_pipelines` n'a ni `pyspark`, ni
  `py4j`, ni `polars` dans `sys.modules`. 50 sources se construisent en ~0,12 s (le test impose
  moins d'une seconde).
- **Migration `file_list` sur PostgreSQL 17 réel** (conteneur jetable) : table de l'ancien schéma
  avec une ligne, `script_tables.sql` rejoué **deux fois** (sans erreur, la ligne garde
  `source_name = ''`, `idx_file_path_source` remplace `idx_file_path`), puis base vide, deux fois.
  Les requêtes (`ON CONFLICT (file_path, dag_run_id, source_name)`, filtre `CAST(:source_name AS TEXT)
  IS NULL OR ...`, `count_processed_files`) passent sous psycopg2 : dédoublonnage par source, filtre
  par source, sans source = toutes les lignes. **Un défaut trouvé ainsi** : la première version
  créait l'index avant d'ajouter la colonne, ce qui échouait sur une base existante ; l'ordre est
  maintenant colonnes, nouvel index, retrait de l'ancien (un test garde l'ordre).
- **Exécution réelle de bout en bout, `dag.test()`** sur les sources d'exemple
  (`test_p1_4_ingest_dag_e2e.py`, faux `spark-submit`, `file_list` SQLite) : `ingest__orders`,
  `ingest__imdb_movies` et `ingest__unmatched` (le fichier d'exemple de chacun, son nom de source,
  l'Asset `lake_staging__<nom>` émis une fois, aucun pour `unmatched`) ; un listing vide donne un run
  vert et aucun `spark-submit` ; les codes de sortie, les reprises (6 jamais rejoué, 1 rejoué),
  l'émission quand un fichier est `processed`, le lignage (parent, jar, empreinte) et la ligne
  `airflow_failure` portent sur le nouveau DAG. `PIPELINE_DEFAULT_ENGINE` et `PIPELINE_CONFIG_ROOT`
  arrivent dans l'environnement de `spark-submit`.
- **Le vrai job Spark, avec le vrai écouteur OpenLineage**, passe toujours
  (`test_p2_5_spark_lineage.py`, 3 tests) : la seule différence est la signature de
  `list_file_from_db`.
- **Airflow refuse déjà les `run_id` hostiles** (`[scheduler] allowed_run_id_pattern`, défaut
  `^[A-Za-z0-9_.~:+-]+$`) et l'API d'exécution ne sait pas adresser un `run_id` qui contient `/`. Le
  test d'injection garde sa valeur (un administrateur peut relâcher le motif) : il le relâche, avec
  un `run_id` sans `/` et des fichiers `P1..P4` que les commandes injectées auraient créés.
- **Prometheus** : les règles d'alerte filtrent par `job` et non par `dag_id` ; seuls leurs textes
  nommaient `read_file_dag` (mis à jour, leurs tests aussi).

## Non vérifié, limites, à savoir

- **Le run sur kind n'a pas été fait.** Le chart n'est touché que par des commentaires et le
  Dockerfile par un commentaire ; l'image n'a pas été reconstruite, ni les pods relancés. Reste à
  contrôler sur kind : `make image kind-load k8s-install`, le DAG `ingest__orders` de bout en bout
  (listing sur le volume partagé, Spark en mode cluster, `lake_staging__orders`, puis dbt), et le
  chemin de mise à jour : sur un volume Postgres existant, rejouer `script_tables.sql` **avant** de
  déployer la nouvelle image (le chart ne l'exécute qu'à la création du volume).
- **Pas de Spark réel via le DAG** : le faux `spark-submit` ne lance pas le job. Le job réel est
  testé à part (lignage, quartet de tests de la P2) ; la jonction DAG -> job est la ligne de commande
  (`--run-id`, `--source-name`), vérifiée par un vrai `argparse` (`test_p1_4_assets.py`). Un run
  `make demo` complet n'a pas été fait.
- **Changements visibles** : les `dag_id` (`ingest__<source>` pour `list_files_dag` et
  `read_file_dag`), les noms de jobs OpenLineage (`ingest__<source>.spark_read_files` : l'historique
  de lignage se scinde), la clé d'unicité de `file_list`, l'Asset `file_list` (disparu). Un script ou
  un tableau de bord qui nomme les anciens DAGs est à adapter ; les règles d'alerte ne sont pas
  concernées.
- **Reprendre un listing** : plus de déclenchement manuel avec `{"run_id": ...}` : *Clear* de
  `spark_read_files` relit le même run. `make spark-job RUN_ID=...` reste l'outil hors Airflow.
- **Deux sources qui réclament le même fichier** : chacune l'enregistre (la clé inclut la source),
  puis le job échoue pour ce fichier avec `AmbiguousConfigError` (`SourceRegistry.match`), comme
  avant. Le job ne choisit pas la config « de la source » du DAG ; à envisager si le cas arrive.
- **Le moteur n'est pas encore consulté** : `source.engine` n'a pas d'effet sur le DAG (P5-8).
- **Le fuseau d'un `schedule`** est l'UTC d'Airflow ; pas de champ `timezone` dans le YAML.
- **`schedule` : la syntaxe cron** n'est vérifiée finement que par Airflow (`dag.validate()`) ; le
  modèle ne refuse que ce qui n'a pas la forme d'un cron à cinq champs ou d'un preset.
- **Un seul pool `spark`** (une place en local) : deux sources ne lisent pas en même temps.
- **`ingest__unmatched`** ignore `listing.exclude` (il n'y a pas de source) ; `pipeline.yaml` n'a que
  `default_directories`.
- **Suppression d'un YAML** : son DAG disparaît au prochain parsing ; ses lignes `file_list`, ses
  tables du lake et l'Asset restent. `transform_dag` ne l'attend plus.
- **dbt reste statique** (`stg_movies__current`, `stg_orders__current`) : sa génération depuis les YAML
  est P5-4. Une source ajoutée donne un DAG d'ingestion et un Asset que `transform_dag` attend, mais
  aucun modèle dbt pour elle.
- Suite complète, `make check` et docs : voir le résumé de la PR.
