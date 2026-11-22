# Le lake Parquet

Le lake est l'ensemble de dossiers Parquet que le job Spark écrit sous `LAKE_ROOT`. Il a trois
zones, au sein desquelles chaque **table** correspond à une source.

```text
<LAKE_ROOT>/
├── raw/<table>/        le fichier tel qu'il a été lu
├── staging/<table>/    le même après nettoyage, seulement si les contrôles ne bloquent pas
└── quality/<table>/    un résultat par contrôle qualité exécuté
    └── lake_run_id=<id du listing>/lake_source_file=<nom du fichier>/part-*.parquet
```

Les noms de zone (`raw/`, `staging/`) viennent de `storage.raw_path` et `storage.staging_path` de la
config de la source ; `quality/` est réservé aux résultats, et aucun chemin de config ne peut
commencer par `quality/`.

## Partitions

Chaque table est partitionnée par deux colonnes :

| Partition | Valeur |
|---|---|
| `lake_run_id` | le `dag_run_id` du listing qui a enregistré le fichier |
| `lake_source_file` | le nom du fichier source |

Une écriture **remplace** sa partition `(lake_run_id, lake_source_file)` et ne touche à aucune
autre : relancer un run réécrit les mêmes fichiers à l'identique. C'est ce qui rend les retries
sûrs (voir [](job-spark.md)).

## Colonnes techniques

Le writer (`ParquetLakeWriter`) ajoute deux colonnes à chaque ligne :

| Colonne | Contenu |
|---|---|
| `lake_file_id` | l'`id` de la ligne `file_list` du fichier : il ordonne les instantanés |
| `lake_ingested_at` | l'instant d'écriture (stocké en INT96 ; DuckDB le relit en `TIMESTAMP` UTC) |

Les noms techniques ne commencent **jamais** par `_` ni `.` : pyarrow et pandas ignorent ces
dossiers de partition et liraient zéro ligne, alors que Spark les lit.

## Un instantané par listing, et l'état courant

Chaque listing d'un même fichier ajoute une nouvelle partition (un nouveau `lake_run_id`) sans
effacer les précédentes. Le lake garde donc l'**historique** des versions d'un fichier.

Pour obtenir l'**état courant**, il faut garder, pour chaque `lake_source_file`, la partition au plus
grand `lake_file_id` (c'est-à-dire le listing le plus récent). C'est la fonction `latest_snapshot`
de `storage/parquet_lake_writer.py`, et c'est aussi ce que font les modèles dbt `stg_*__current`
([](dbt.md)).

:::{note}
« Le plus récent » ne veut pas dire « la dernière écriture ». Relancer un ancien run réécrit sa
partition, sans la rendre courante : seul le `lake_file_id` compte.
:::

## Règles à connaître

- **Aucune table n'est imbriquée dans une autre**, qu'il s'agisse du raw et du staging d'une même
  source ou de tables de sources différentes ; la vérification ignore la casse. Deux sources
  peuvent en revanche partager exactement la même table.
- **`LAKE_ROOT` doit être absolu** (un chemin, `file:` suivi d'un chemin absolu, ou une URI
  `s3://`, `gs://`… avec un hôte). Un chemin relatif fait sortir le job avec le code 5. Le `~`
  initial est développé.
- **Les chemins locaux sont convertis en URI `file://`** pour Spark (`normalize_spark_path`).
  Hadoop ne décode pas `%3D` : dans un test, n'utilisez pas `Path.as_uri()` pour un dossier de
  partition.
- **Lire une table `default_*` en entier demande `mergeSchema`** : elle mélange des fichiers sans
  rapport. Une source lue en `inferSchema` sans `columns_type` peut inférer des types différents
  d'un fichier à l'autre : déclarez `columns_type` pour toute source lue en entier.

## Résultats de qualité

`<LAKE_ROOT>/quality/<table>/` reçoit une ligne par contrôle exécuté, avec le même partitionnement.
À lire avec le statut du fichier dans `file_list`. Voir [](../configuration/qualite.md).
