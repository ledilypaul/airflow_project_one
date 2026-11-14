# P2-4 — Couche de transformation : dbt + DuckDB, orchestrée par Cosmos

- **Date :** 2026-11-14
- **Item de backlog :** P2-4 (`docs/audit/2026-11-05-baseline.md`)
- **Code :** `transform/`, `dags/local_transform_dag.py`, `dags/local_read_file_dag.py` (`publish_lake_staging`)
- **Détails d'exploitation :** `transform/README.md` et `CLAUDE.md`, section « dbt transformation layer »

## 1. Le problème de départ

Avant P2-4, le pipeline **charge** les données mais ne les **rend pas exploitables**.

- **Un instantané par listing.** Chaque listing réécrit chaque fichier sous son propre `lake_run_id`. Après 10 listings, `staging/movies` contient 10 copies du même CSV. La version courante d'un fichier est celle du plus grand `lake_file_id`.
- **Un calcul à refaire à chaque lecture.** Pour obtenir l'état courant, il fallait refaire ce tri en Spark (`latest_snapshot`) à chaque lecture.
- **Aucune table métier.** Rien ne produisait de table prête pour une analyse, un tableau de bord ou un modèle de ML.

Le projet avait donc une bonne **zone de stockage**, mais pas de **couche de transformation**.

## 2. La solution : une troisième étape automatique

```
list_files_dag ──file_list──▶ read_file_dag ──lake_staging──▶ transform_dag ──dbt_marts──▶ (BI, ML, lignage…)
  liste les fichiers            Spark : raw + staging           dbt + DuckDB :
  (Postgres)                    + contrôles qualité             état courant + tables métier
```

Chaque étape déclenche la suivante par un **Asset** Airflow, sans planification à heure fixe. Dès que des fichiers sont écrits en staging, dbt reconstruit les tables.

## 3. Les briques et leurs raisons

| Brique | Rôle | Pourquoi ce choix |
|---|---|---|
| **dbt** (dbt-core 1.12) | Écrire les transformations en **SQL** versionné, testé et documenté | Standard du métier. Pour des transformations, le SQL est plus lisible et plus facile à relire que du PySpark. Chaque modèle a ses tests : `not_null`, `unique`, contrats de types. |
| **DuckDB** (dbt-duckdb 1.11, duckdb < 2) | Exécuter ce SQL **directement sur les Parquet du lake** | Aucun serveur, rapide sur les volumes du projet. Le spike a vérifié qu'il relit le lake Spark à l'identique, y compris avec des `run_id` contenant `:`, `+`, `#`, des espaces et des accents. |
| **Cosmos** (astronomer-cosmos 1.16) | Transformer le projet dbt en **DAG Airflow** | Chaque modèle et chaque test dbt devient une tâche visible dans l'UI, avec son statut et ses retries. Un échec se voit au niveau du modèle, pas comme un seul « dbt a planté ». |
| **Sous-projet `transform/`** | dbt dans **son propre environnement uv**, avec son `uv.lock` | Installer dbt à côté d'Airflow **rétrograderait** `protobuf` et trois autres paquets (constaté pendant le spike). Une fois séparés, aucun conflit : Cosmos n'ajoute que 11 paquets côté Airflow, sans en modifier aucun. |

### Alternatives écartées

- **dbt dans l'environnement Airflow :** il rétrograde des dépendances d'Airflow (voir ci-dessus).
- **Transformations en PySpark dans le job existant :** elles mélangeraient chargement et transformation. On perdrait aussi les tests, la documentation et le graphe de dépendances que dbt apporte.
- **Mode d'exécution Cosmos `LOCAL` (un `dbt run` par modèle) :** plus lent, et plusieurs processus dbt se disputeraient l'unique écrivain DuckDB. Le mode **watcher** lance un seul `dbt build` et rapporte le statut modèle par modèle.

## 4. Ce qui est construit

### Modèles dbt (`transform/models/`)

- **`stg_movies__current` / `stg_orders__current`** donnent l'**état courant** : pour chaque fichier source, seules les lignes de son instantané le plus récent (`qualify lake_file_id = max(lake_file_id) over (partition by lake_source_file)`). C'est l'équivalent SQL exact de `latest_snapshot`, vérifié par un test d'intégration qui compare les deux.
- **`mart_movies__by_release_year_language`** est un exemple de table métier : par année de sortie et par langue, le nombre de films, la note moyenne et le total des votes. Il sert de modèle pour les marts suivants.
- **Tests dbt :**
  - un seul instantané par fichier dans l'état courant ;
  - unicité du grain du mart ;
  - colonnes techniques non nulles.
- **Contrat de types sur `stg_movies__current`.** Il est aligné sur `config/sources/imdb.yaml`. DuckDB (`union_by_name`) élargit une colonne au type le plus large de tous les instantanés, anciens compris. Le contrat fait échouer le build au lieu de laisser le type changer en silence.

### Lecture du lake

- **Les sources lisent** `read_parquet('<LAKE_ROOT>/staging/<table>/lake_run_id=*/lake_source_file=*/*.parquet', hive_partitioning=true, union_by_name=true, hive_types={...: VARCHAR})`.
- **`hive_types` en VARCHAR est obligatoire.** Sinon, un run id numérique serait typé en entier.
- **Table sans aucun fichier.** Quand une table staging n'a encore aucun fichier (aucun `orders` lu, par exemple), la macro `lake_staging()` renvoie une relation vide typée, au lieu de l'erreur de DuckDB sur un glob vide.

### DAG `transform_dag`

- **Déclenchement et concurrence.** Il est déclenché par l'Asset `lake_staging`. Une seule exécution à la fois (`max_active_runs=1`), et un pool `dbt_duckdb` à 1 slot, parce que DuckDB n'accepte qu'un seul écrivain par fichier.
- **Exécution.** Mode **watcher** : un producteur lance un unique `dbt build`, puis un capteur par modèle ou test en rapporte le statut. Avec le pool à 1 slot, les capteurs passent après le producteur. C'est le prix d'un rejeu sûr avec un seul écrivain.
- **Asset publié.** Le DAG publie l'Asset stable **`dbt_marts`** quand tout a réussi : c'est le point d'accroche pour la suite (lignage P2-5, ML P4-2). Les Assets propres à Cosmos sont désactivés, parce que leur URI contient le chemin absolu du fichier DuckDB.
- **Rendu.** Le DAG est rendu depuis `transform/target/manifest.json` (`LoadMode.DBT_MANIFEST`), si bien que le DAG processor n'exécute jamais dbt.

### Commandes

```bash
make transform-sync   # installe dbt dans transform/.venv
make dbt-parse        # génère transform/target/manifest.json, lu par Airflow
make dbt-build        # lance dbt build à la main (LAKE_ROOT, DBT_DUCKDB_PATH surchargeables)
```

## 5. Garde-fous ajoutés après la relecture de code

La relecture (`code-auditor`) a relevé deux problèmes élevés, corrigés et couverts par des tests de non-régression.

1. **Un mauvais `LAKE_ROOT` vidait les tables sans erreur.**
   - Avec une faute de frappe, ou un schéma que DuckDB ne lit pas (`hdfs://`, `dbfs:/`), dbt trouvait 0 fichier. Il reconstruisait alors des tables vides, réussissait, et publiait `dbt_marts`.
   - Désormais, le hook `on-run-start` vérifie le chemin avant tout modèle :
     - chemin absolu ;
     - schéma lisible par DuckDB (`file:`, `s3`, `gs`, `gcs`, `r2`, `az`, `azure`, `abfss`) ;
     - aucun caractère de glob ou de partition (`' = * ? [ {`) ;
     - au moins un fichier présent en staging.
   - Si l'une de ces conditions n'est pas remplie, dbt s'arrête et l'entrepôt reste intact.
2. **Un seul fichier en échec permanent bloquait dbt pour toujours.**
   - Un fichier corrompu, ou qui échoue à un contrôle qualité bloquant, est re-listé à chaque run, donc chaque run échouait. Comme `lake_staging` n'était publié qu'en cas de succès total, dbt ne tournait plus jamais.
   - Désormais, `publish_lake_staging` (règle `all_done`) publie dès qu'au moins un fichier du run est `processed` dans `file_list`. La tâche `fail_if_spark_failed` (règle `one_failed`) garde malgré tout le run en échec, pour que le fichier cassé reste visible.

Autres protections :

- **Manifest absent ou périmé.** Si le manifest manque, ou s'il est plus ancien que les modèles, les macros ou `dbt_project.yml`, Airflow affiche une erreur d'import explicite. Elle ne concerne que ce DAG.
- **Extensions DuckDB.** DuckDB n'installe ni ne charge jamais d'extension tout seul : pas de téléchargement imprévu pendant un run. Un lake distant devra déclarer `extensions: [httpfs]` dans `profiles.yml`.
- **Dossier personnel.** Le DAG et le Makefile développent `~` dans `LAKE_ROOT`, comme le job Spark.

## 6. À savoir à l'usage

- **Triggerer obligatoire.** Les capteurs du mode watcher sont différables. `make airflow` (`airflow standalone`) lance un triggerer.
- **Après un `git pull` ou un `checkout` qui touche `transform/`,** relancer `make dbt-parse`. Sinon le manifest est signalé comme périmé.
- **Horodatages.** `lake_ingested_at` (INT96 écrit par Spark) est relu par DuckDB en `TIMESTAMP` sans fuseau, **en UTC** : 14:21 à Paris s'affiche 13:21.
- **Une seule exécution dbt à la fois.** Ne pas lancer `make dbt-build` pendant que `transform_dag` écrit dans le même fichier DuckDB.
- **Version de uv.** Utiliser uv ≥ 0.11 pour toute commande qui écrit un lock. Le shim pyenv (0.6.3) réécrit `uv.lock` dans un format plus ancien.
- **Limite de test.** `dag.test()` ne peut pas exécuter `transform_dag` de façon fiable.
  - Cosmos 1.16 ne relie pas le producteur aux capteurs des modèles, et `dag.test()` choisit l'ordre des tâches prêtes selon `PYTHONHASHSEED`.
  - Quand un capteur passe en premier, il attend indéfiniment.
  - Le DAG a donc été validé avec un vrai `airflow standalone`, et la partie dbt est couverte par de vrais builds dans `tests/integration/test_p2_4_dbt_build.py`.

## 7. Points ouverts

- **`orders`.** Sans `columns_type` dans `config/sources/orders.yaml`, ses types peuvent varier d'un fichier à l'autre, donc pas de contrat sur `stg_orders__current`. À déclarer une fois le schéma réel connu, puis activer le contrat. *Fait en P2-6 (`docs/architecture/2026-11-14-p2-6-phase2-followups.md`).*
- **Exécutions sans effet.** `publish_lake_staging` compte aussi les fichiers `processed` des sources sans zone staging. dbt peut alors tourner pour rien, ce qui reste idempotent et sans effet.
- **Lake distant** (S3, Azure) : déclarer l'extension DuckDB et les identifiants.
- **Points antérieurs :** un code de sortie sans retry pour les échecs qualité déterministes, et le passage des contrôles IMDB en `error`.
- **Suite du backlog : P2-5 (OpenLineage).** Cosmos a déjà installé `openlineage-python`, `openlineage-sql` et `openlineage-integration-common`.
