# Premier run

Ce pas à pas fait traverser tout le pipeline à un fichier d'exemple, `examples/data/orders_sample.csv`
(quatre colonnes : `order_id`, `customer`, `amount`, `order_date`). Il suppose l'installation
terminée et `make demo` en cours d'exécution ([](installation.md)) : le moteur ne livre aucune
source, `make demo` lit celles de `examples/config` (`PIPELINE_CONFIG_ROOT`) et copie les fichiers
d'exemple dans `data/`. Pour vos propres sources, déposez vos YAML dans `config/sources/` et lancez
`make airflow`.

## 1. Dépauser les DAGs

Ouvrez <http://localhost:8080> (utilisateur `admin`, mot de passe affiché par `make airflow`). Les
sources de `examples/config` donnent les DAGs `ingest__orders` et `ingest__imdb_movies` (un par YAML,
générés par `dags/source_pipelines.py`), plus `ingest__unmatched` pour les fichiers sans source.
**Dépausez** `ingest__orders` et `transform_dag`.

:::{warning}
C'est l'étape qu'on oublie le plus. Airflow ne met en file une exécution déclenchée par un Asset
que pour un DAG **non pausé**. Un événement `lake_staging__orders` émis pendant la pause de
`transform_dag` est perdu, et depuis Airflow 3.4 aucune exécution ultérieure ne le rattrape : le
prochain événement reconstruit tout de toute façon, mais tant qu'il n'y en a pas, les tables DuckDB
ne bougent pas.
:::

## 2. Lancer un listing

Dans l'interface, déclenchez `ingest__orders` (bouton *Trigger*). Autre possibilité, en ligne de
commande, avec le [client de l'API REST](../exploitation/api-rest.md) :

```bash
export AIRFLOW_API_PASSWORD=<le mot de passe admin>
make api-trigger DAG=ingest__orders
```

Le DAG cherche dans `./data/` (`default_directories` de `config/pipeline.yaml`) les fichiers dont le
nom correspond aux `source.patterns` de la source (`orders_*.csv`), puis écrit une ligne `pending`
par fichier dans la table `file_list`, marquée du `dag_run_id` du run et du nom de la source. Une
source qui a un `schedule` partirait seule ; ici le YAML n'en a pas, donc à la main.

## 3. Regarder la suite se déclencher seule

Dans le même DAG, une fois les lignes écrites :

1. **`spark_read_files`** lance une application Spark (`spark-submit`) qui lit chaque fichier du
   listing et l'écrit dans le lake ;
2. **`publish_lake_staging`** émet l'Asset `lake_staging__orders` dès qu'au moins un fichier est
   `processed` ;
3. **`transform_dag`** démarre sur cet Asset : dbt reconstruit les tables DuckDB à partir de la zone
   `staging`.

Chaque DAG a sa vue en grille dans l'interface. Le premier démarrage de Spark prend quelques
dizaines de secondes. Sans fichier à traiter, `insert_files` et `spark_read_files` sont *skipped* :
le run est vert et rien ne part.

## 4. Vérifier le résultat

**Le statut des fichiers**, dans Postgres :

```bash
set -a; source .env; set +a
docker exec airflow_postgres psql -U "$DB_USER" -d "$DB_NAME" \
  -c "select id, file_name, status, error_message from file_list order by id desc limit 5;"
```

Un fichier réussi est `processed`. Un échec est `failed`, avec la raison dans `error_message`.

**Le lake Parquet**, dans `./lake/` (ignoré par git) :

```text
lake/
├── raw/orders/        le fichier tel qu'il a été lu
├── staging/orders/    le même, sans lignes vides ni doublons exacts
└── quality/orders/    un résultat par contrôle qualité
```

Chaque table est partitionnée par `lake_run_id` et `lake_source_file` : voir [](../concepts/lake.md).

**Les tables DuckDB**, une fois `transform_dag` terminé (DuckDB n'accepte qu'un écrivain : n'ouvrez
pas le fichier pendant le run) :

```bash
transform/.venv/bin/python -c "
import duckdb
c = duckdb.connect('transform/warehouse.duckdb', read_only=True)
print(c.sql('select * from main.stg_orders__current order by order_id'))
"
```

## 5. Relancer : c'est sans danger

Relancer un listing ajoute un instantané par fichier, il n'écrase rien. Relancer
`spark_read_files` (*Clear*) pour un même listing réécrit les mêmes partitions à l'identique. L'état courant
d'une table est toujours « la partition du listing le plus récent » (voir `latest_snapshot` dans
[](../concepts/lake.md)).

## Quand quelque chose ne démarre pas

| Symptôme | Cause probable | Remède |
|---|---|---|
| `spark_read_files` reste en `scheduled` | le pool `spark` n'existe pas (aucun avertissement dans l'interface) | `make airflow-init`, qui importe `deploy/airflow/pools.json` |
| `spark-submit` échoue avec « HADOOP_CONF_DIR or YARN_CONF_DIR must be set » | la Connection `spark_default` manque : le fournisseur retombe sur `yarn` | `make airflow-init` |
| `transform_dag` absent, ou erreur d'import | le manifest dbt manque ou est plus ancien que les modèles | `make dbt-parse` (`make airflow` le fait) |
| `transform_dag` ne démarre pas après un listing | le DAG était en pause lors de l'événement | dépauser, puis relancer un listing |
| Un DAG `invalid_source__<fichier>` apparaît | le YAML de ce fichier ne valide pas | lancer sa tâche : le journal donne chaque erreur ; corriger le YAML |
| `list_files` échoue avec « source director ... does not exist » | le répertoire du listing n'existe pas pour ce worker | créer `./data/`, ou régler `listing.directories` |
| Fichier `failed` avec `invalid reading config` | une config de lecture est invalide : le job sort avec le code 5 | corriger le YAML, voir [](../configuration/sources.md) |
| Fichier `failed` avec `quality checks failed` | un contrôle bloquant a échoué : staging non écrit, code 6 | voir [](../configuration/qualite.md) |

Les codes de sortie du job Spark sont détaillés dans [](../concepts/job-spark.md).
