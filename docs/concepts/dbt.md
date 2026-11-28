# La couche dbt

`transform/` est un projet **dbt** qui construit des tables DuckDB au-dessus de la zone `staging` du
lake. C'est un projet `uv` à part (`package = false`, son propre `uv.lock`) : dbt n'est donc pas
installé à côté d'Airflow, mais dans `transform/.venv` (`make transform-sync`).

## Les modèles

| Modèle | Rôle |
|---|---|
| `stg_movies__current`, `stg_orders__current` | l'**état courant** d'une table du lake : pour chaque fichier, son instantané le plus récent (`latest_snapshot`, voir [](lake.md)) |
| `mart_movies__by_release_year_language` | un mart d'exemple : les films de l'état courant par année de sortie et langue originale |

Tout est matérialisé en **table**, dans le schéma `main` du fichier DuckDB
(`transform/warehouse.duckdb` par défaut).

- **Contrats.** `stg_movies__current` et `stg_orders__current` ont un contrat imposé (colonnes et
  types de leur source, alignés sur le `columns_type` de `imdb.yaml` et `orders.yaml`). Raison :
  `union_by_name` élargit une colonne au type le plus large de **tous** les instantanés, anciens
  compris, ce qui ferait dériver les types sans contrat. Une table sans `columns_type` n'a pas de
  modèle généré.
- **Génération.** Les sources dbt et les modèles `stg_*__current` sont écrits par `make dbt-codegen`
  depuis les YAML (voir [](../configuration/sources.md)), jamais à la main.
- **Table vide.** Les modèles lisent le lake par la macro `lake_staging()`, pas par `source()`.
  Quand une table n'a encore aucun fichier (aucune commande lue par exemple), la macro renvoie une
  relation vide typée par les colonnes déclarées, au lieu de l'erreur de DuckDB sur un motif sans
  résultat.

## Lecture du lake

Les sources utilisent `external_location` avec un modèle de requête de la forme :

```text
read_parquet('<LAKE_ROOT>/staging/<table>/lake_run_id=*/lake_source_file=*/*.parquet',
             hive_partitioning=true, union_by_name=true,
             hive_types={'lake_run_id': 'VARCHAR', 'lake_source_file': 'VARCHAR'})
```

- `hive_types` est obligatoire : sans lui, un `lake_run_id` numérique serait lu comme un entier.
- Le motif explicite ignore `_SUCCESS`, les `.crc` et les dossiers temporaires de Spark.
- Une `LAKE_ROOT` distante demande de déclarer l'extension (`httpfs` ou `azure`) dans
  `profiles.yml` : le projet n'installe ni ne charge jamais une extension DuckDB tout seul.

## Les garde-fous du démarrage

Avant tout modèle, le hook `on-run-start` (macro `check_lake_environment`) vérifie l'environnement
et refuse de toucher à l'entrepôt si quelque chose cloche :

- `LAKE_ROOT` et `DBT_DUCKDB_PATH` doivent être **absolus** (un `~` doit avoir été développé) ;
- `LAKE_ROOT` doit être lisible par DuckDB : un chemin absolu, `file:` suivi d'un chemin absolu, ou
  `s3`, `gs`, `gcs`, `r2`, `az`, `azure`, `abfss` avec un bucket ou un conteneur ; tout autre schéma
  (`hdfs`, `dbfs`…) est refusé par son nom ;
- les caractères `'`, `=`, `*`, `?`, `[` et `{` sont refusés ;
- au moins un fichier Parquet doit exister sous `<LAKE_ROOT>/staging/*/lake_run_id=*/...` : un
  mauvais `LAKE_ROOT` construisait autrefois des tables vides et émettait quand même `dbt_marts`.

## Orchestration par Cosmos

`transform_dag` est un `DbtDag` d'Astronomer Cosmos (1.16), rendu depuis
`transform/target/manifest.json` : le processeur de DAGs ne lance jamais dbt.

- **Manifest requis.** Sans lui, l'import de ce fichier échoue avec un message qui nomme
  `make dbt-parse` ; les autres DAGs se chargent. Un manifest **plus ancien** qu'un modèle, une
  macro, un test ou `dbt_project.yml` est refusé de la même façon : relancez `make dbt-parse` après
  un `git pull` qui les touche.
- **Mode d'exécution `WATCHER`.** Un producteur lance un seul `dbt build`, et un capteur par modèle
  et par test rapporte son statut. Les capteurs de modèles sont *différables* : un **triggerer** est
  nécessaire (`airflow standalone` en lance un).
- **Un seul écrivain.** DuckDB n'accepte qu'un écrivain par fichier. Toutes les tâches dbt tournent
  donc dans le pool `dbt_duckdb` (1 emplacement ; les tâches différées n'y comptent pas) et
  `threads: 1` est fixé dans `profiles.yml`. Conséquence : le producteur garde l'unique emplacement
  pendant tout son `dbt build`, et les capteurs passent surtout après lui, un par un.
- **Assets.** Les Assets propres à Cosmos sont désactivés (leur URI contiendrait le chemin du
  fichier DuckDB). `publish_dbt_marts`, en aval de toutes les tâches dbt, émet l'Asset stable
  `dbt_marts`.
- **Environnement.** `LAKE_ROOT` et `DBT_DUCKDB_PATH` sont passés à dbt avec les mêmes valeurs par
  défaut que le Makefile (`<racine>/lake` et `<racine>/transform/warehouse.duckdb`).

:::{warning}
`dag.test()` ne peut pas exécuter `transform_dag` de façon fiable : un bogue de câblage de Cosmos 1.16
laisse des capteurs sans le producteur en amont, et `dag.test()` les exécute dans un ordre qui dépend
de `PYTHONHASHSEED`. Quand un capteur passe en premier, il attend à l'infini un producteur que
`dag.test()` ne lance jamais. Testez ce DAG avec un vrai scheduler et un triggerer
(`make airflow`), et le projet dbt avec `dbt build`.
:::

## En dehors d'Airflow

```bash
make transform-sync   # crée transform/.venv
make dbt-parse        # écrit transform/target/manifest.json
make dbt-build        # dbt build ; LAKE_ROOT et DBT_DUCKDB_PATH surchargeables
```

Ne lancez pas `dbt build` pendant que `transform_dag` écrit le même fichier DuckDB.
Le détail du projet est dans `transform/README.md`.
