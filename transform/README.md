# transform/ : couche transformation (dbt + DuckDB), P2-4

Troisième étape du pipeline. Elle lit la zone **staging** du lake Parquet
écrite par le job Spark et construit, dans un fichier DuckDB, l'état courant
de chaque table puis des marts.

Pourquoi cette couche et les choix qui la composent : voir
[`docs/architecture/2026-11-14-p2-4-transform-layer.md`](../docs/architecture/2026-11-14-p2-4-transform-layer.md).

```
list_files_dag --file_list--> read_file_dag --lake_staging--> transform_dag --dbt_marts-->
                              (Spark : raw, staging)          (dbt build, DuckDB)
```

- `read_file_dag` émet l'Asset `lake_staging` depuis sa tâche finale
  `publish_lake_staging`.
  - Elle s'exécute une fois toutes ses applications Spark terminées
    (`all_done`), et émet si **au moins un** fichier de ses run ids est
    `processed` dans `file_list`. Un fichier en échec permanent ne bloque
    donc pas dbt.
  - Elle se met en `skipped` (rien d'émis) si aucun fichier n'a été traité,
    ou si les run ids n'ont pas pu être résolus.
  - Le run reste `failed` si une application Spark a échoué : la feuille
    `fail_if_spark_failed` (`one_failed`) l'y maintient.
- `transform_dag` (`dags/local_transform_dag.py`, Cosmos) est planifié sur
  cet Asset et émet `dbt_marts` (tâche `publish_dbt_marts`) quand toutes ses
  tâches dbt ont réussi.
- Chaque run reconstruit tout l'état courant (tables, pas d'incrémental) : il
  est idempotent et n'a pas besoin du run id source.

## Environnement séparé

`transform/` est un projet uv distinct (`package = false`, son propre
`uv.lock`) : dbt-core et Airflow ont des dépendances qui se gênent. Cosmos
appelle l'exécutable `transform/.venv/bin/dbt` en sous-processus. Le DAG
processor n'exécute jamais dbt : il lit le manifest.

```bash
make transform-sync   # uv sync --locked dans transform/ (crée transform/.venv)
make dbt-parse        # transform/target/manifest.json, lu par transform_dag
make dbt-build        # dbt build en ligne de commande, hors Airflow
```

Pour modifier les dépendances, utiliser `uv add` / `uv lock` **dans
`transform/`** avec uv ≥ 0.11 (`uv --directory transform add ...`).
`make check` et la CI vérifient les deux `uv.lock` (`uv lock --check`).

## Variables d'environnement

| Variable | Défaut (Makefile et DAG) | Rôle |
|---|---|---|
| `LAKE_ROOT` | `<racine du projet>/lake` (le défaut du job Spark) | racine du lake lu |
| `DBT_DUCKDB_PATH` | `<racine du projet>/transform/warehouse.duckdb` (gitignoré) | fichier DuckDB écrit |

- Une valeur exportée par le shell (ou passée à `make`) gagne. `transform_dag`
  passe ces deux variables à dbt via `ProjectConfig.env_vars`. Elles sont
  ajoutées à l'environnement du worker, qui les lit quand il analyse le
  fichier DAG.
- Le DAG et le Makefile développent un `~` initial, comme le job. dbt ne le
  développe pas : il le refuse, avec un message qui le dit.
- dbt vérifie les deux au démarrage (`on-run-start`, macro
  `check_lake_environment`), avant tout modèle. Un refus ne touche donc pas
  l'entrepôt :
  - **chemins relatifs refusés.** Sous Cosmos, dbt tourne dans une copie
    temporaire du projet.
  - **`LAKE_ROOT` : uniquement ce que DuckDB sait lire.**
    - Accepté : un chemin absolu, `file:` suivi d'un chemin absolu
      (`file:/x`, `file:///x` (la forme de `resolve_lake_root`),
      `file://localhost/x`).
    - Accepté aussi : une URI `s3://`, `gs://`, `gcs://`, `r2://` (extension
      `httpfs`), `az://`, `azure://`, `abfss://` (extension `azure`), avec
      un bucket ou un conteneur. Schémas tirés de la doc DuckDB 1.5.
    - Refusé par son nom : tout autre schéma (`hdfs`, `dbfs`, `abfs`...).
      DuckDB n'y trouverait aucun fichier, sans erreur.
    - Refusés aussi : `'`, `=`, `*`, `?`, `[` et `{`. Ils casseraient le SQL,
      ou seraient lus comme une partition hive ou comme des jokers du glob.
  - **au moins un fichier Parquet de staging** sous
    `<LAKE_ROOT>/staging/*/lake_run_id=*/lake_source_file=*/*.parquet`.
    `transform_dag` ne tourne qu'après une écriture staging. Un lake sans
    aucun fichier staging est donc un `LAKE_ROOT` faux (faute de frappe,
    autre machine), pas un état vide à publier : le build échoue, et les
    tables existantes restent intactes. Conséquence : `make dbt-build` échoue
    tant que le lake local n'a pas de zone staging.
- **Aucune extension DuckDB n'est installée ni chargée implicitement**
  (`autoinstall_known_extensions` et `autoload_known_extensions` à `false`
  dans `profiles.yml`). Un lake distant demande de déclarer
  `extensions: [httpfs]` (ou `azure`) et ses identifiants dans le profil.
  Ce n'est pas configuré aujourd'hui : un `LAKE_ROOT` distant échoue
  explicitement (« requires the extension httpfs to be loaded »).

## Lecture du lake

Les sources sont déclarées dans `models/staging/_lake_staging__sources.yml`
(`external_location`, `formatter: template`) :

```sql
read_parquet('<LAKE_ROOT>/staging/<table>/lake_run_id=*/lake_source_file=*/*.parquet',
  hive_partitioning = true, union_by_name = true,
  hive_types = {'lake_run_id': 'VARCHAR', 'lake_source_file': 'VARCHAR'})
```

- **Glob explicite.** Il ignore `_SUCCESS`, les `.crc`, `.spark-staging-*`
  et `_temporary`.
- **`hive_types` VARCHAR obligatoire.** Sans lui, DuckDB type `0042` ou
  `12345` en entier. Les noms échappés par Spark (`:`, `+`, `#`, espaces,
  accents) sont relus à l'identique (test d'intégration).
- **`union_by_name`.** Les fichiers d'une table peuvent avoir des colonnes
  différentes : une colonne absente d'un fichier vaut NULL.
- **`lake_ingested_at`.** Il est écrit par Spark en INT96 et relu en
  `TIMESTAMP` **sans fuseau, exprimé en UTC**. Il ne sert qu'au lignage.

**Table staging sans fichier.** C'est le cas tant qu'aucun fichier de la
source n'a été lu (par exemple aucun `orders_*.csv`), alors que d'autres
tables du lake en ont. DuckDB échoue alors sur un glob vide. Les modèles ne lisent donc pas `source()` directement mais la
macro `lake_staging()` (`macros/lake.sql`). Si le glob ne trouve aucun
fichier, elle renvoie une **relation vide typée** par les colonnes déclarées
dans la source, et le build réussit avec 0 ligne.

- `movies` déclare toutes ses colonnes, avec les types de
  `config/sources/imdb.yaml` (un test pytest le vérifie). La relation vide a
  donc exactement le schéma réel.
- `orders` fait de même avec les types de `config/sources/orders.yaml`
  (P2-6, même test).
- Une nouvelle source doit déclarer au moins ses colonnes techniques, chacune
  avec son `data_type`.

## Modèles

- **`stg_movies__current` et `stg_orders__current`.** État courant :
  `qualify lake_file_id = max(lake_file_id) over (partition by lake_source_file)`,
  l'équivalent de `storage.parquet_lake_writer.latest_snapshot`.
  - Le plus grand `file_id` gagne, pas la dernière écriture.
  - Tests : `not_null` sur les colonnes techniques, et
    `one_snapshot_per_source_file` (un seul instantané par fichier, un
    instantané pour un seul fichier).
  - Aucune règle métier : les contrôles métier relèvent de `quality:` (P2-3).
- **`mart_movies__by_release_year_language`.** Exemple de mart, par
  (`release_year`, `original_language`), nuls compris. Il calcule
  `movie_rows`, `distinct_movies`, `avg_vote_average` et `total_vote_count`.
  Un test singulier vérifie son grain.

Tout est matérialisé en **table** : l'entrepôt reflète le lake au dernier
`dbt build`, pas au moment de la requête.

**Types et `union_by_name`.** Pour chaque colonne, `union_by_name` prend le
type le plus large de **tous** les instantanés de la table, anciens compris :
un vieil instantané lu en `BIGINT` élargit `INTEGER` partout.

- **`stg_movies__current`** et **`stg_orders__current`** ont un **contrat
  appliqué** (`contract: {enforced: true}`), avec les colonnes et types de
  leur source, eux-mêmes alignés sur `reader.columns_type` d'`imdb.yaml` et
  d'`orders.yaml` (vérifié par pytest). Un type élargi ou une colonne en plus
  fait échouer le build au lieu de changer le schéma de l'entrepôt en
  silence (un test d'intégration par modèle).
- **Une nouvelle source** suit le même chemin : `reader.columns_type` dans
  sa config de lecture, les mêmes colonnes dans la source dbt, puis le
  contrat. Un test vérifie que chaque table de `lake_staging` a un
  `columns_type`.
- **orders (P2-6).** `orders.yaml` déclare aussi `enforceSchema: false` : le
  schéma étant positionnel, Spark compare l'en-tête de chaque fichier aux
  noms déclarés. Un fichier d'une autre disposition est `failed` à la
  lecture au lieu d'arriver dans le staging avec des colonnes décalées.
  Un instantané écrit avant P2-6 (lu avec `inferSchema`) dont un type ou
  une colonne diffère (par exemple `order_id` en `BIGINT`) fait échouer le
  contrat. Relire le fichier ne suffit pas, puisque `union_by_name` lit
  aussi les anciens instantanés : il faut supprimer ces partitions.

## DuckDB : un seul écrivain

Un fichier DuckDB n'accepte qu'un processus écrivain. Plusieurs mécanismes le
garantissent :

- `threads: 1` dans `profiles.yml` ;
- `max_active_runs=1` sur `transform_dag` ;
- le pool Airflow `dbt_duckdb` (1 slot, `deploy/airflow/pools.json`, importé
  par `make airflow-init`), sur **toutes** les tâches dbt. Les consommateurs
  sont inclus, car leur retry relance dbt pour leur modèle. Les capteurs
  différés ne tiennent pas le slot (`include_deferred: false`).

Conséquence du slot unique : le producteur le garde pendant tout son
`dbt build`. Les capteurs s'exécutent donc pour la plupart **après** lui, un
par un, au lieu de suivre le build en direct. C'est le prix d'un rejeu sûr
sur un seul écrivain.

Ne lancez donc pas `make dbt-build` pendant qu'un run de `transform_dag`
écrit le même `DBT_DUCKDB_PATH` : dbt échouerait sur le verrou du fichier.

## Exécution sous Airflow (Cosmos 1.16)

- **`ExecutionMode.WATCHER`.** Un producteur lance un seul `dbt build`, et un
  capteur par modèle ou test en reporte le statut. Les capteurs de modèles
  sont différables : un **triggerer** est requis (`airflow standalone`, donc
  `make airflow`, en lance un).
- **`LoadMode.DBT_MANIFEST`.** `local_transform_dag.py` lève une erreur
  d'import explicite (« run `make dbt-parse` »), visible dans l'UI, dans deux
  cas :
  - le manifest est absent ;
  - il est **périmé** : plus ancien qu'un fichier de `models/`, `macros/`,
    `data_tests/` ou que `dbt_project.yml` (comparaison des `mtime`). Le
    graphe rendu ne correspondrait plus au `dbt build` du producteur.

  Les autres DAGs ne sont pas touchés. `make airflow` et `make test`
  génèrent le manifest d'abord, et la CI aussi avant pytest. Après un
  `git checkout` ou un `git pull` qui touche ces fichiers, relancer
  `make dbt-parse`.
- **Pas de `RenderConfig.dbt_executable_path`.** Le contrôle
  `is_dbt_installed_in_same_environment` de Cosmos 1.16 est faux sans dbt
  dans l'environnement Airflow.
- **Assets Cosmos désactivés** (`emit_datasets=False`) : leur URI contient le
  chemin absolu du fichier DuckDB. L'Asset stable est `dbt_marts`.
- **`dag.test()` ne peut pas exécuter `transform_dag` de façon fiable.**
  - Cosmos 1.16 ne relie le producteur qu'aux nœuds racines sans dépendance,
    et une source compte comme une dépendance : aucun capteur de modèle
    n'est en aval du producteur.
  - `dag.test()` exécute ses TaskInstances prêtes dans l'ordre d'un `set`,
    qui dépend de `PYTHONHASHSEED`.
  - Quand un capteur passe en premier, son trigger exécuté en ligne attend
    sans fin un producteur que `dag.test()` ne lance jamais. Constaté : les
    graines 1 et 3 bloquent, les graines 0 et 2 réussissent.
  - Fixer la graine serait un contournement fragile. Le DAG se vérifie avec
    un vrai scheduler et un triggerer (`make airflow`), le projet dbt avec
    `dbt build` (`tests/integration/test_p2_4_dbt_build.py`).

## Lignage OpenLineage (P2-5)

Pourquoi et comment : voir
[`docs/architecture/2026-11-14-p2-5-openlineage.md`](../docs/architecture/2026-11-14-p2-5-openlineage.md).

- **Qui émet.** Le producteur watcher de Cosmos (`dbt_producer_watcher`)
  analyse les artefacts de son `dbt build` (`openlineage-integration-common`,
  installé avec Cosmos), et le provider OpenLineage d'Airflow émet ses
  entrées et sorties. Rien n'est ajouté à ce projet uv : ni `dbt-ol` ni
  `openlineage-dbt`.
- **Noms DuckDB.** Une relation est nommée `duckdb://<DBT_DUCKDB_PATH>` +
  `<base>.<schéma>.<table>`. La base est le nom que dbt-duckdb 1.11 donne au
  fichier ouvert à l'exécution : la première partie non vide de son nom
  (`warehouse.duckdb` et `warehouse.prod.duckdb` donnent `warehouse`,
  `.hidden.duckdb` donne `hidden`). Les sources du lake
  (`_lake_staging__sources.yml`) sont donc nommées
  `warehouse.lake_staging.movies` : le processeur ne tient pas compte de
  `external_location`.
- **Le pont.** Spark nomme la même table par son répertoire (`file`,
  `<LAKE_ROOT>/staging/movies`). La tâche `lineage_lake_staging` de
  `transform_dag` (`LakeStagingLineageOperator`) émet le lien entre les deux
  noms, une paire par source `lake_staging` du manifest. Les noms sont
  recalculés à l'exécution depuis `LAKE_ROOT` et `DBT_DUCKDB_PATH`.
  - **Lien structurel.** Il dit qu'une source dbt lit ce répertoire, pas que
    Spark l'a écrit pendant ce run. Pour un lake local, seules les tables
    staging dont le répertoire existe sont reliées ; un lake distant n'est
    pas listé.
  - **Sans risque.** La tâche n'exécute rien, n'a ni amont ni aval, n'est
    pas dans le pool `dbt_duckdb`, et un manifest illisible ne donne qu'un
    lignage vide avec un warning.
- **Ajouter une source.** Une nouvelle table de `lake_staging` est reliée
  automatiquement, à condition de garder le glob
  `<LAKE_ROOT>/staging/$identifier/...` (un test le vérifie).
