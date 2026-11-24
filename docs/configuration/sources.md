# Configurer une source

Chaque source est décrite par un fichier YAML, validé au chargement par **un seul** modèle
Pydantic, `pipeline/config/models.py::SourceConfig`. Ce fichier suffit à obtenir un pipeline
d'ingestion : le DAG `ingest__<nom>` est **généré** depuis lui ([](../concepts/dags.md)). Le modèle refuse toute clé inconnue : une faute
de frappe est une erreur, jamais ignorée en silence.

:::{note}
La racine se choisit par `PIPELINE_CONFIG_ROOT` (chemin absolu, défaut `config/`). Sans dossier `defaults/` dans cette racine, les défauts par extension du moteur (`config/defaults/`) s'appliquent. `make demo` utilise `examples/config`.
:::

## Un exemple complet

```yaml
source:
  name: orders                  # identifiant [a-z0-9_]+, unique ; nomme le DAG ingest__orders
  format: csv                   # csv | txt | parquet | xlsx : choisit le lecteur
  patterns: ["orders_*.csv"]    # fnmatch sur le NOM du fichier, insensible à la casse
  # engine: spark               # facultatif : spark | polars (voir « Le moteur d'une source »)

reader:
  options: {header: true, delimiter: ",", encoding: "UTF-8", enforceSchema: false}
  columns_type:                 # schéma explicite, csv/txt seulement, dans l'ordre du fichier
    order_id: int
    customer: string
    amount: double
    order_date: date

storage:                        # obligatoire : où le job écrit dans le lake
  table_name: orders            # facultatif, par défaut source.name
  format: parquet               # seule valeur acceptée
  mode: overwrite               # seule valeur acceptée (append est refusé : idempotence)
  raw_path: raw/
  staging_path: staging/        # facultatif : sans lui, pas de zone staging, raw seulement

quality:                        # facultatif, exige staging_path (voir « Contrôles qualité »)
  checks:
    - {type: not_null, column: order_id}

listing:                        # facultatif : où chercher les fichiers (voir « Le DAG de la source »)
  directories: ["./data/"]      # défaut : default_directories de config/pipeline.yaml
  exclude: ["*_old.csv"]        # motifs fnmatch sur le nom, retirés du listing
schedule: "0 6 * * *"           # cron (UTC), preset (@daily) ou null : lancement manuel
retries: 2                      # facultatif, 1 par défaut
tags: [finance]                 # facultatif : étiquettes du DAG dans l'interface
```

## Où vivent les configs

| Dossier | Contenu |
|---|---|
| `config/sources/*.yaml` | une config **dédiée** par source, la vôtre (vide dans le dépôt). Les exemples `orders.yaml` et `imdb.yaml` sont dans `examples/config/sources/` |
| `config/defaults/*.yaml` | le **repli** par extension (`*.csv`, `*.txt`, `*.xlsx`, `*.parquet`), avec le même modèle |
| `config/pipeline.yaml` | les répertoires listés **par défaut** (`default_directories`) ; la racine de config peut avoir le sien |

Une source dédiée décrit un seul format : ses motifs incluent l'extension (`orders_*.csv`, pas
`orders_*`). Un fichier IMDB au format `.parquet` prend donc le repli `default_parquet` au lieu
d'être lu comme un CSV.

## Le DAG de la source

Les clés `listing`, `schedule`, `retries` et `tags` ne servent pas à la lecture : elles règlent le
DAG `ingest__<source.name>`.

- **Quels fichiers.** Le listing garde, dans chaque répertoire de `listing.directories` (non
  récursif), les fichiers dont le **nom** correspond à `source.patterns`, moins `listing.exclude`.
  Les motifs d'inclusion restent `source.patterns` : une seule source de vérité. Sans
  `listing.directories`, la source prend `default_directories` de `pipeline.yaml` (`./data/`,
  relatif au répertoire de lancement du worker). Un répertoire qui n'existe pas fait échouer le
  listing : ce n'est pas un listing vide.
- **Quand.** `schedule` est une expression cron à cinq champs (fuseau UTC), un preset Airflow
  (`@hourly`, `@daily`, `@weekly`, `@monthly`, `@quarterly`, `@yearly`, `@once`) ou `null`, le
  défaut : le DAG ne part qu'à la main. Airflow vérifie la syntaxe fine : un cron refusé
  (`99 99 * * *`) donne un DAG `invalid_source__<fichier>`, pas une erreur d'import.
- **Combien de reprises.** `retries` (0 à 10, défaut 1) vaut pour les tâches du DAG ; les codes de
  sortie déterministes du job ne sont jamais rejoués.
- **Le nom est un identifiant.** `source.name` : lettres minuscules, chiffres et `_`, 100
  caractères au plus ; il devient le `dag_id` (`ingest__<nom>`), l'Asset (`lake_staging__<nom>`) et
  la valeur de `file_list.source_name`. `unmatched` est réservé (DAG des fichiers sans source).
- **Les fichiers sans source.** Ceux qu'aucune source dédiée ne réclame passent par le DAG
  `ingest__unmatched`, qui les lit avec le repli de leur extension : zone raw seulement.
- **Une config invalide** ne casse pas les autres DAGs : elle donne un DAG
  `invalid_source__<fichier>` dont la tâche échoue avec le message de validation.

## Quelle config pour quel fichier ?

`SourceRegistry.match` raisonne sur le **nom** du fichier, en minuscules :

1. les sources dédiées d'abord : exactement une correspondance gagne ; **deux ou plus** lèvent une
   `AmbiguousConfigError` qui les nomme ;
2. sinon les replis, avec la même règle ;
3. sinon une `NoMatchingConfigError` : le job marque le fichier `failed`.

Il n'y a **pas de format par défaut** : une extension inconnue est une erreur, pas une lecture CSV.

## Le moteur d'une source

`source.engine` vaut `spark` ou `polars`. Absent, il vient de la variable d'environnement
`PIPELINE_DEFAULT_ENGINE`, puis de `spark`. Le moteur décide de ce qui est valide dans la config :
les options de `reader.options` et la syntaxe des regex qualité. Il s'applique à **chaque** config,
repli par extension compris.

:::{note}
Seul Spark exécute des fichiers aujourd'hui : le moteur Polars arrive avec le backlog P5-7. Une
source `engine: polars` se valide et se charge, mais le job Spark marque ses fichiers `failed`
(`la source ... déclare le moteur 'polars'`). Elle ne change rien pour les sources Spark.
:::

La config n'importe ni PySpark ni Polars : on la charge, au parsing d'un DAG par exemple, sans
moteur installé.

## Les options de lecture

`reader.options` est interprété selon le moteur de la source. Il est vérifié contre une **liste
blanche par (moteur, format)** (`pipeline/config/reader_options.py`, qui cite ses sources) : c'est
ce qui refuse `infer_schema` quand Spark attend `inferSchema`.

**Moteur `spark`** : passé tel quel à Spark (csv, txt et parquet, valeurs scalaires) ou à
`pyspark.pandas.read_excel` (xlsx).

- **Alias.** Déclarer les deux d'une paire (`sep` et `delimiter`, `encoding` et `charset`) est
  refusé : Spark n'en garderait qu'un, sans le dire.
- **Types.** Les options booléennes, entières et numériques sont typées : `header: "yes"` est une
  erreur de validation, écrivez `header: true`.
- **Pas de valeur cachée.** Une option absente du YAML prend le défaut de Spark ou de pandas (par
  exemple `header=false` en CSV).
- `xls` n'est pas pris en charge (`xlrd` n'est pas installé).

**Moteur `polars`** : les noms de `pl.scan_csv`, `pl.scan_parquet` et `pl.read_excel` (Polars
2.2.0). Une option peut être `null` (`infer_schema_length: null`) ou une liste (`null_values`). Les
options typées le sont aussi (`has_header: "yes"` est refusé), et `encoding` n'accepte que `utf8` et
`utf8-lossy`, comme `scan_csv`.

Une option **Spark** déclarée sur une source Polars est refusée avec le nom de son équivalent quand
il est établi :

| Spark (csv, txt) | Polars | | pandas (xlsx) | Polars |
|---|---|---|---|---|
| `sep`, `delimiter` | `separator` | | `header` | `has_header` |
| `header` | `has_header` | | `usecols` | `columns` |
| `quote` | `quote_char` | | `dtype` | `schema_overrides` |
| `comment` | `comment_prefix` | | | |
| `nullValue` | `null_values` | | | |
| `inferSchema` | `infer_schema` | | | |
| `lineSep` | `eol_char` | | | |

L'équivalence est celle du **nom** : la sémantique peut différer (`header` pandas est un indice de
ligne, `has_header` un booléen). Une option Spark absente de ce tableau n'a pas d'équivalent établi
et le message le dit.

## Le schéma explicite : `columns_type`

Les types sont **neutres**, communs à tous les moteurs ; la table de `pipeline/config/types.py`
(`NEUTRAL_TYPES`) donne pour chacun son nom chez Spark, Polars, DuckDB et Arrow :

| Type YAML | Spark | Polars | DuckDB (contrat dbt) | Arrow |
|---|---|---|---|---|
| `string` | `StringType` | `String` | `VARCHAR` | `string` |
| `int` (alias `integer`) | `IntegerType` (32 bits) | `Int32` | `INTEGER` | `int32` |
| `long` | `LongType` (64 bits) | `Int64` | `BIGINT` | `int64` |
| `float` | `FloatType` | `Float32` | `FLOAT` | `float` |
| `double` | `DoubleType` | `Float64` | `DOUBLE` | `double` |
| `boolean` | `BooleanType` | `Boolean` | `BOOLEAN` | `bool` |
| `date` | `DateType` | `Date` | `DATE` | `date32[day]` |
| `timestamp` | `TimestampType` | `Datetime` | `TIMESTAMP` | `timestamp[ns]` |

Une colonne d'identifiants qui dépasse 2³¹ se déclare `long` (`int` reste sur 32 bits). La
conversion en objets de moteur se fait dans le moteur (`pipeline/engine/spark/types.py`).

Seuls les types **atomiques** sont acceptés : le lecteur CSV de Spark ne sait pas lire un tableau, une map ni une
structure. Une colonne de liste comme `genre_list` (IMDB) est donc lue en `string`, puis découpée
plus tard.

Le schéma est **positionnel** : gardez l'ordre des colonnes du fichier (Spark ignore alors
`inferSchema`). Avec `enforceSchema: false`, Spark compare aussi l'en-tête de chaque fichier aux noms
déclarés (nombre et noms, casse ignorée) et fait échouer le fichier sur un écart, au lieu de lire
un autre format avec des colonnes décalées ou nulles.

## Ce que la validation refuse

Les erreurs d'un même fichier sont agrégées dans **une** `ConfigError`, qui nomme chaque fichier YAML
et chaque chemin de champ (`reader.options.infer_schema: ...`). Au chargement, elle refuse aussi :

- un `source.name` en double, ou qui n'est pas un identifiant (`[a-z0-9_]+`) ;
- des tables du lake imbriquées (entre le raw et le staging d'une source, ou entre sources ; la
  casse est ignorée) ;
- un `raw_path` ou `staging_path` qui commence par `quality/` (réservé aux résultats).

Le job charge le registre **avant** de démarrer Spark et sort avec le code 5 si quelque chose
cloche : toutes les lignes du listing sont alors marquées `failed` ([](../concepts/job-spark.md)).

## Les lecteurs

`CSVReader`, `ParquetReader` et `ExcelReader` implémentent tous `read(path, config)` et rendent un
DataFrame Spark. Ils n'héritent pas de `BaseFileReader`, qui est le **routeur** : il charge le
registre une fois (depuis `DEFAULT_CONFIG_ROOT`, résolu depuis le code et non depuis le répertoire
courant), trouve la config du fichier, puis demande à `ReaderFactory` le lecteur de son format.
`ExcelReader` importe `pyspark.pandas` dans `read()` : lire un CSV ne doit pas le charger.

## Ajouter une source

1. Créez `config/sources/<nom>.yaml`, avec des motifs qui ne recouvrent ceux d'aucune autre source
   dédiée et une section `storage` dont les tables ne contiennent ni ne sont contenues dans celles
   d'une autre.
2. Vérifiez :
   ```bash
   uv run pytest tests/unit/config                       # paramétré sur chaque YAML présent
   uv run pytest tests/unit/file_reader -k synthetic     # lit un fichier synthétique
   ```
3. Si la liste blanche refuse une option légitime, ajoutez-la à `reader_options.py` **avec sa source
   de documentation** (et, si elle n'est pas du texte, à `SPARK_OPTION_TYPES` ou
   `POLARS_OPTION_TYPES`).
4. Pour un nouveau **format**, il faut un lecteur, une entrée dans `READER_CLASSES` et `FileFormat`
   (qui doivent rester égaux : un test le vérifie), une liste blanche et un test.

5. Dépausez `ingest__<nom>` (les DAGs d'Airflow démarrent en pause) : il apparaît au prochain
   parsing, sans autre code à écrire. Sans `schedule`, lancez-le à la main.

Les fichiers à lire se règlent dans le même YAML (`listing`, `source.patterns`) : il n'y a plus de
fichier de listing à part.
