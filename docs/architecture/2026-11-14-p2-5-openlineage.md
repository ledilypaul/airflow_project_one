# P2-5 — Lignage OpenLineage, des fichiers sources aux marts dbt

- **Date :** 2026-11-14
- **Item de backlog :** P2-5 (`docs/audit/2026-11-05-baseline.md`)
- **Code :** `dags/local_read_file_dag.py` (`openlineage_spark_settings`, `prepare_spark_lineage`), `dags/local_transform_dag.py` (`lineage_lake_staging`), `utils/file_list_lineage.py`, `scripts/lineage_show.py`, `scripts/openlineage_transport.py`, `Makefile` (cibles `lineage-*`), `docker-compose.yaml` (profil `lineage`)
- **Détails dbt :** `transform/README.md`, section « Lignage OpenLineage »
- **Détails d'exploitation :** `CLAUDE.md`, section « Lineage (OpenLineage, P2-5) »

## 1. Le problème de départ

Après P2-4, le pipeline produit des tables métier, mais personne ne peut dire **d'où elles viennent**.

- **Trois mondes séparés.** Les fichiers sont listés dans Postgres par Airflow, lus et écrits dans le lake par Spark (un processus JVM à part), puis transformés par dbt dans DuckDB. Chacun a ses logs, aucun ne connaît les autres.
- **Une question sans réponse.** « Ce mart, quels fichiers l'ont alimenté, et quel run l'a produit ? » demandait de recouper à la main les logs Airflow, `file_list`, les partitions du lake et le `run_results.json` de dbt.
- **Un piège de nommage.** Même avec des intégrations OpenLineage partout, le graphe serait **coupé au milieu**. Spark nomme la table staging par son répertoire (`file`, `<LAKE_ROOT>/staging/movies`). dbt nomme la source qui la lit par sa relation DuckDB (`duckdb://<entrepôt>`, `warehouse.lake_staging.movies`). Pour OpenLineage, ce sont deux datasets sans rapport.

## 2. La solution : un graphe continu, configuré par l'environnement

```
fichiers sources ──▶ list_files_dag.insert_files ──▶ file_list (Postgres)
      │                                                  │
      │                                                  ▼
      └──────────▶ job Spark (enfant de read_file_dag.spark_read_files)
                        │
                        ▼
              lake raw/ et staging/ (Parquet)
                        │
                        ▼  transform_dag.lineage_lake_staging (le pont)
              sources dbt warehouse.lake_staging.*
                        │
                        ▼  transform_dag.dbt_producer_watcher
              stg_*__current ──▶ mart_movies__by_release_year_language
```

- **Chaque maillon émet ses propres événements OpenLineage**, avec des noms qui suivent la spécification de nommage. Les jobs se rejoignent donc par leurs datasets communs.
- **Aucune valeur en dur dans les DAGs.** Le transport, le namespace et le jar viennent de l'environnement (le Makefile en dev).
- **En dev**, les événements vont dans des fichiers sous `lineage/events/`, et `make lineage-show` les résume. Avec `OPENLINEAGE_URL`, ils partent en HTTP, vers Marquez par exemple (`make lineage-up`).

## 3. Les briques et leurs raisons

| Brique | Rôle | Pourquoi ce choix |
|---|---|---|
| **`apache-airflow-providers-openlineage` 2.21** (groupe `airflow`) | Événements des DAGs et des tâches ; injection de la conf Spark | Provider officiel. Compatible Airflow ≥ 2.11, donc 3.3.2. Un seul paquet ajouté au lock : ses dépendances (`openlineage-python`, `openlineage-integration-common` 1.55) étaient déjà installées par Cosmos. |
| **Collecteur de lignage des hooks** (`airflow.sdk.lineage`) | Déclarer `file_list` et les fichiers sources | Les tâches passent par un engine SQLAlchemy, que le provider ne voit pas. Le collecteur est le mécanisme prévu pour ça. Les noms sont produits par les convertisseurs des providers `postgres` et `common.io`. |
| **`openlineage-spark_2.13` 1.55.0** (jar JVM) | Lignage des lectures et écritures Spark | Listener officiel. Testé ici sur PySpark 4.3.0 : CSV lu en entrée, tables raw, staging et quality en sortie (écriture partitionnée, `partitionOverwriteMode=dynamic`). |
| **Jar épinglé et vérifié** (`make lineage-jar`, SHA-256 dans le Makefile) | Rendre le listener disponible sans réseau à l'exécution | `spark.jars.packages` téléchargerait depuis Maven Central à chaque premier lancement. Hors ligne, `spark-submit` échouerait avant le job. Le jar fait 42 Mo, autonome, sans dépendance. |
| **Tâche `lineage_lake_staging`** (`LakeStagingLineageOperator`) | Relier le nom Spark et le nom dbt de chaque table staging | Ni Spark ni le processeur dbt ne savent qu'une source `external_location` lit un répertoire Parquet. Le lien est calculé depuis le manifest dbt et l'environnement de dbt. |
| **Marquez 0.50.0** (profil compose `lineage`) | Visualiser le graphe | Implémentation de référence d'OpenLineage. Optionnel, jamais lancé par `make db-up`. |

### Faits vérifiés avant de coder

- **Provider.** `spark_inject_parent_job_info` et `spark_inject_transport_info` existent, et `SparkSubmitOperator` fait partie des opérateurs qui les appliquent. L'injection de transport ne gère **que le HTTP**. Le provider est désactivé quand aucun transport n'est configuré. `OPENLINEAGE_DISABLED` est encore lu. Sources : [référence de configuration](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/configurations-ref.html), [intégration Spark](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/spark.html), code installé (`airflow/providers/openlineage/conf.py`, `utils/spark.py`).
- **Spark.** La CI d'OpenLineage 1.55.0 testait Spark jusqu'à 4.2.0. Spark 4.3.0 n'a été ajouté que sur `main`, le 2026-11-09, sans changement de code (seulement la CI). Le test d'intégration du projet le confirme sur 4.3.0. Sources : [installation](https://openlineage.io/docs/integrations/spark/installation), `.circleci/workflows/openlineage-spark.yml` du dépôt OpenLineage (tag `1.55.0` et commit `66dea8b`), [Maven Central](https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.13/).
- **dbt.** Cosmos 1.16 émet bien du lignage en mode watcher. Le producteur hérite de `DbtLocalBaseOperator`, qui analyse les artefacts de son `dbt build` avec `DbtLocalArtifactProcessor`, puis les expose par `get_openlineage_facets_on_complete`. Ni `dbt-ol` ni `openlineage-dbt` ne sont donc nécessaires. Le processeur nomme une source DuckDB `duckdb://<path>` + `<database>.<schema>.<identifier>`, sans tenir compte d'`external_location`. Source : code installé (`cosmos/operators/local.py`, `openlineage/common/provider/dbt/processor.py`), vérifié par un vrai build.

### Alternatives écartées

- **`spark.jars.packages`** (téléchargement Maven au lancement) : dépendance réseau dans le chemin critique du job.
- **Import d'un client OpenLineage dans le job Python** : le job ne doit rien importer pour le lignage, et le listener JVM voit les plans réels.
- **`dbt-ol` (`openlineage-dbt`) dans `transform/`** : un second émetteur pour les mêmes runs, alors que Cosmos émet déjà. Il garderait de plus le même nommage DuckDB.
- **Facet `symlinks` sur les sources dbt** : le processeur ne l'émet que pour Athena, et Marquez ne fusionne pas les nœuds sur cette base.
- **Assets Airflow `inlets`/`outlets` pour le pont** : aucun convertisseur Asset vers OpenLineage n'existe pour `duckdb://`, et des Assets en dur figeraient les chemins à l'analyse du DAG.

## 4. Ce qui est construit

### Configuration (`Makefile`, cibles Airflow)

- **`AIRFLOW__OPENLINEAGE__TRANSPORT`** (exporté par `make airflow`), construit avec `json.dumps` par `scripts/openlineage_transport.py`, à partir de variables d'environnement, jamais par interpolation (`make lineage-transport` l'affiche) :
  - par défaut, `{"type": "file", "log_file_path": "<racine>/lineage/events/airflow", "append": false}` : un fichier JSON par événement, sans risque d'entrelacement entre le scheduler et les workers ;
  - avec `OPENLINEAGE_URL`, `{"type": "http", "url": ...}`.
- **`AIRFLOW__OPENLINEAGE__NAMESPACE`** : `airflow_project_one` par défaut.
- **`OPENLINEAGE_SPARK_JAR` et `OPENLINEAGE_SPARK_SHA256`** : le chemin du jar et son empreinte épinglée.
- **`AIRFLOW__LINEAGE__MAX_ASSETS_PER_COLLECTOR=1000`** : le collecteur de hooks ne garde que 100 entrées par tâche par défaut.
- **Priorité.** Une valeur exportée par le shell l'emporte. `OPENLINEAGE_DISABLED=true` ou `[openlineage] disabled = True` coupent tout.

### Spark (`read_file_dag`)

`openlineage_spark_settings()` produit les arguments de `SparkSubmitOperator`. Son corps est entièrement protégé : en cas d'erreur, le lignage Spark est coupé avec un warning, et le DAG s'importe toujours.

- **Aucune conf** (commande identique à celle d'avant P2-5, vérifiée par le hook) quand :
  - le lignage est coupé ;
  - `OPENLINEAGE_SPARK_JAR` est absent (log de niveau debug, car lu à chaque analyse), relatif, introuvable, pas un zip, ou sans la classe du listener ;
  - la configuration est invalide (JSON cassé, liste, provider absent) ;
  - le transport porte des identifiants, ou n'est pas lisible dans `[openlineage] transport` (voir le §8) ;
  - un transport `file` a un `log_file_path` qui n'est pas local et absolu.
- **Sinon, la conf ajoute :**
  - `spark.jars` et `spark.extraListeners` ;
  - `spark.redaction.regex`, étendu aux clés d'API, aux jetons et à `openlineage.transport.url` ;
  - `spark.openlineage.namespace` ;
  - `spark.openlineage.dataset.disabledTrimmers=KeyValueTrimmer`, qui évite que `orders_dt=2026-11-14.csv` prenne le nom de son répertoire. Vérifié : les tables du lake, écrites à leur racine, gardent leur nom.
- **Transport.** En `file`, on fixe `spark.openlineage.transport.type=file`. Pour tout autre transport sans identifiant, c'est le provider qui injecte (HTTP seulement).
- **À l'exécution, `prepare_spark_lineage` (`pre_execute`, ne lève jamais) :**
  - vérifie le SHA-256 du jar, ou retire le listener ;
  - donne à chaque application son fichier `spark-<dag>-<run>-<hash>-<map index>-<try>.jsonl` ;
  - déclare `file_list` en entrée.
- **Info parent toujours injectée.** Le run Spark est l'enfant de `read_file_dag.spark_read_files`, sous le run du DAG. Le test e2e vérifie que le `parentRunId` reçu par `spark-submit` est bien le `runId` de l'événement Airflow de la tâche.
- **Ce qui est écrasé.** Les valeurs explicites `openlineage_inject_*` priment sur `[openlineage] spark_inject_*`, et `--conf spark.jars` / `spark.extraListeners` sur `spark-defaults.conf`.

### `file_list`

`utils/file_list_lineage.py` déclare au collecteur, sans jamais faire échouer la tâche, et sort tout de suite quand le lignage est coupé :

- pour les deux DAGs de listing, les fichiers enregistrés en entrée (chemins résolus comme `normalize_spark_path`, donc **les mêmes noms que ceux de Spark**) et `file_list` en sortie ;
- pour `spark_read_files`, `file_list` en entrée ;
- au-delà du plafond du collecteur, un warning donne le nombre de fichiers non déclarés ;
- un fichier dont le nom contient `#` ou `?` est ignoré avec un warning. Une URI d'Asset s'arrête à ces caractères, et `quote()` donne `%23` / `%3F`, alors que Spark nomme le fichier tel quel : pas de dataset plutôt qu'un dataset faux.

### dbt (`transform_dag`)

- **Le producteur watcher** émet les entrées et sorties de tous les modèles de son build.
- **`lineage_lake_staging`** émet le pont, avec pour entrées les tables staging nommées comme Spark, et pour sorties les sources nommées comme dbt :
  - **Nom de la base.** C'est celui que dbt-duckdb 1.11 donne au fichier ouvert à l'exécution : la première partie non vide de son nom (`warehouse.prod.duckdb` donne `warehouse`).
  - **Côté Spark.** Les noms sont ceux de `FilesystemDatasetUtils` du jar 1.55.0, vérifiés par py4j. Seule la racine du lake est résolue, comme le fait le job. Le `/` initial n'est retiré que pour `s3*`, `gs`, `abfs*` et `wasb*` ; `hdfs://nn:8020/lake` donne `/lake`, `dbfs:/lake` donne (`dbfs`, `/lake`).
  - **Lien structurel.** Il dit qu'une source dbt lit ce répertoire, pas que Spark l'a écrit pendant ce run. Pour un lake local, seules les tables staging qui existent sont reliées.
- **Une tâche sans risque.** Elle n'a ni amont ni aval et n'est pas dans le pool DuckDB. Sans manifest lisible, elle donne un lignage vide et un avertissement.

### Outils

- **`make lineage-show`** résume les jobs (dernier état, parent, entrées, sorties) et les arêtes dataset → job → dataset. Il lit en streaming, ligne à ligne, et une entrée illisible est comptée et signalée, jamais fatale (non-UTF-8, répertoire nommé `.json`, types inattendus).
- **`make lineage-clean`** supprime les seuls fichiers d'événements (`airflow-*.json`, `spark*.jsonl`) et garde le jar.
- **`make lineage-up` / `make lineage-down`** démarrent et arrêtent Marquez :
  - API sur `127.0.0.1:5002`, UI sur `127.0.0.1:3002` ;
  - port admin et base non publiés, réseau compose dédié `lineage` ;
  - images épinglées par tag et digest.
- **`make lineage-jar`** télécharge en HTTPS seulement (`--proto '=https' --proto-redir '=https'`). Il revérifie toujours un jar présent, et le supprime puis le retélécharge s'il ne correspond pas.
- **CI.** La clé du cache contient la version et l'empreinte, `make lineage-jar` revérifie le jar restauré, et `REQUIRE_LINEAGE=1` rend le test Spark obligatoire.

## 5. Exemple réel

Les trois DAGs ont été lancés par `dag.test()` dans un `AIRFLOW_HOME` jetable, avec :

- une base Postgres de démonstration (supprimée ensuite) ;
- le vrai Spark avec son listener ;
- le vrai dbt.

Pour `transform_dag`, seuls le producteur et le pont ont été lancés, à cause de la limite de `dag.test()` décrite plus bas. Extrait de `make lineage-show` (avant les corrections du §8, qui ne changent pas ces noms) :

```
list_files_dag.insert_files  COMPLETE
  in : file <demo>/data/imdb_demo.csv
  in : file <demo>/data/orders_2026_11.csv
  out: postgres://localhost:5434 p2_5_lineage_demo.public.file_list
read_file_dag.spark_read_files  COMPLETE
  in : postgres://localhost:5434 p2_5_lineage_demo.public.file_list
read_files_job  COMPLETE
  parent: airflow_project_one/read_file_dag.spark_read_files
read_files_job.execute_insert_into_hadoop_fs_relation_command.raw_movies  COMPLETE
  in : file <demo>/data/imdb_demo.csv
  out: file <demo>/lake/raw/movies
read_files_job.adaptive_spark_plan.staging_movies  COMPLETE
  in : file <demo>/data/imdb_demo.csv
  out: file <demo>/lake/staging/movies
transform_dag.lineage_lake_staging  COMPLETE
  in : file <demo>/lake/staging/movies
  out: duckdb://<demo>/warehouse.duckdb warehouse.lake_staging.movies
transform_dag.dbt_producer_watcher  COMPLETE
  in : duckdb://<demo>/warehouse.duckdb warehouse.lake_staging.movies
  out: duckdb://<demo>/warehouse.duckdb warehouse.main.stg_movies__current
  out: duckdb://<demo>/warehouse.duckdb warehouse.main.mart_movies__by_release_year_language
```

Rejoués dans Marquez, les 53 événements donnent un graphe connexe. Le graphe de `warehouse.main.mart_movies__by_release_year_language` compte 23 nœuds, jusqu'aux deux CSV sources et à `file_list`.

## 6. À savoir à l'usage

- **Le jar doit correspondre à la version Scala de Spark.** PySpark 4 est en Scala 2.13. Un jar `_2.12` contient la même classe et passe donc le contrôle zip, puis fait échouer le job : à la première requête, le thread du listener lève `NoSuchMethodError` et arrête le SparkContext. C'est le SHA-256 vérifié à l'exécution qui l'écarte, d'où `OPENLINEAGE_SPARK_SHA256`, exporté par le Makefile. Un backend injoignable, lui, n'est jamais fatal : le listener se contente de journaliser.
- **`spark.jars` remplace celui de `spark-defaults.conf`.** Un environnement qui y déclare déjà des jars doit y ajouter le listener plutôt que de passer par `OPENLINEAGE_SPARK_JAR`.
- **Événements Spark bruités.** Le listener émet aussi des jobs sans dataset (application, `collect_limit` de l'inférence de schéma). `lineage-show` les masque, `--all` les montre.
- **`dag.test()` et `transform_dag`.** La tâche supplémentaire change l'ordre d'exécution, si bien qu'un capteur a attendu indéfiniment, même avec `PYTHONHASHSEED=0`. La limite de P2-4 reste entière : `dag.partial_subset(["dbt_producer_watcher", "lineage_lake_staging"])` suffit pour observer le lignage.
- **Tests.** Ils n'émettent jamais : `tests/conftest.py`, chargé avant toute la suite, force `AIRFLOW__OPENLINEAGE__DISABLED=True` et retire les variables `OPENLINEAGE_*` / `AIRFLOW__OPENLINEAGE__*` du shell.
- **Réseau Docker.** `make lineage-down` laisse le réseau `airflow_project_one_lineage`, vide (`docker compose rm` ne supprime pas les réseaux).

## 7. Points ouverts

- **Lignage par colonne.** Spark et dbt l'émettent déjà (facet `columnLineage`). Il n'est ni exploité ni testé ici.
- **Lake distant.** Les noms `s3://`, `gs://`, `abfss://`, `hdfs://` et `dbfs:/` sont alignés sur ceux du listener (vérifiés par py4j), mais pas vérifiés de bout en bout : le lake de dev est local, et un lake distant n'est pas listé par le pont.
- **Mode cluster.** Le chemin du jar doit être lisible depuis le driver, ce qui demande de distribuer le jar. Le transport `file` n'a de sens qu'en local.
- **Backend authentifié.** Un transport avec clé d'API, en-têtes ou identifiants dans l'URL coupe le lignage Spark (fail closed). Pour l'activer, il faudrait passer ces secrets à Spark sans la ligne de commande journalisée, par exemple par un `spark-defaults.conf` propre à l'environnement.
- **Rétention.** `make lineage-clean` vide les événements à la main ; aucune purge automatique.
- **Fichiers `#` / `?`.** Ils sont lus par Spark, mais absents du lignage du listing.
- **`spark_read_files` côté Airflow** ne déclare que `file_list`. Ses sorties sont portées par le run Spark enfant (facet `parent`), pas par la tâche elle-même.
- **PostgreSQL 17 pour Marquez.** Marquez 0.50.0 ne documente que PostgreSQL 14, et son Flyway 8.5.13 date d'avant PostgreSQL 17 : à revoir avec une version plus récente de Marquez.

## 8. Garde-fous ajoutés après les relectures (code et sécurité)

Règle commune : **le lignage ne casse jamais le pipeline**, et aucun secret n'atteint un log.

1. **Jar présent mais invalide (C-1, SEC-2).**
   - **Avant :** un jar tronqué, étranger ou `_2.12` faisait sortir le job en 1 (« Exception when registering SparkListener »), et les lignes restaient `pending`.
   - **Désormais :**
     - **À l'analyse :** on vérifie que le jar est un zip contenant `OpenLineageSparkListener.class` (environ 35 ms).
     - **À l'exécution :** on vérifie le SHA-256 épinglé (environ 110 ms pour 42 Mo, mis en cache par chemin, taille et mtime). Il n'est pas calculé à l'analyse, que le DAG processor répète en continu.
     - **En cas d'échec :** le listener est retiré, et la commande redevient celle d'avant P2-5.
2. **Configuration invalide (C-2, SEC-4).** Un transport en JSON cassé empêchait l'import de `read_file_dag`. Désormais, toute erreur coupe le lignage Spark avec un warning. Le JSON du Makefile est produit par `json.dumps`, et un `"` ou un `\` dans `LINEAGE_DIR` ou `OPENLINEAGE_URL` ne peut plus casser le JSON ni y injecter de clés.
3. **Secrets dans `Spark-Submit cmd` (SEC-1).** Le provider injectait la clé d'API et l'URL dans les propriétés Spark, que le hook journalise en clair.
   - **Fail closed :** un transport avec `auth`, `headers`, `custom_headers` ou des identifiants dans l'URL, `composite` compris, n'est jamais injecté. Un transport configuré ailleurs que dans `[openlineage] transport` non plus, puisqu'on ne peut pas le vérifier.
   - **Redaction :** `spark.redaction.regex` est étendu.
   - Un test construit la vraie commande par le hook et y cherche le secret.
4. **Noms alignés sur le listener (C-3, C-4, C-5).**
   - **Base dbt-duckdb :** la règle exacte (`base.lstrip(".").split(".")[0]`).
   - **`FilesystemDatasetUtils` :** ses règles exactes, reprises de la source du tag 1.55.0 et vérifiées par py4j.
   - **Racine du lake :** seule la racine est résolue, comme le fait le job.
5. **Robustesse et confort.**
   - Plafond du collecteur relevé et signalé (C-6).
   - Noms `#` / `?` écartés, `KeyValueTrimmer` désactivé (C-7).
   - `lineage-show` en streaming et tolérant (C-8).
   - Pont limité aux répertoires existants (C-9).
   - `report_file_list_input` sans exception possible (C-10).
   - Documentation des valeurs écrasées et log « jar absent » en debug (C-11).
   - Un fichier d'événements par application et `make lineage-clean` (C-13).
6. **Exposition et chaîne d'approvisionnement.**
   - **Marquez :** sur `127.0.0.1` seulement, réseau dédié, sans port admin publié (SEC-3) ; images épinglées par digest (SEC-5).
   - **Jar :** téléchargé en HTTPS seulement.
   - **Empreinte :** sa provenance est notée dans le Makefile (SEC-6). Elle est égale au `.sha256` de Maven Central, et la signature `.asc` du jar est valide pour la clé `B4AD C6AE E76F 4EED 97FA 78A1 C6C9 B79C 9583 2D71`, qui signe aussi `openlineage-java` 1.55.0. Cette clé n'est certifiée par aucun chemin de confiance local.
