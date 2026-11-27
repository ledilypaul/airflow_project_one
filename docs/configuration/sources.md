# Configurer la lecture d'une source

Chaque type de fichier est décrit par un fichier YAML, validé au chargement par **un seul** modèle
Pydantic, `pipeline/config/models.py::SourceConfig`. Le modèle refuse toute clé inconnue : une faute
de frappe est une erreur, jamais ignorée en silence.

:::{note}
La racine se choisit par `PIPELINE_CONFIG_ROOT` (chemin absolu, défaut `config/`). Sans dossier `defaults/` dans cette racine, les défauts par extension du moteur (`config/defaults/`) s'appliquent. `make demo` utilise `examples/config`.
:::

## Un exemple complet

```yaml
source:
  name: orders                  # unique parmi tous les fichiers de config
  format: csv                   # csv | txt | parquet | xlsx : choisit le lecteur
  patterns: ["orders_*.csv"]    # fnmatch sur le NOM du fichier, insensible à la casse

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
```

## Où vivent les configs

| Dossier | Contenu |
|---|---|
| `config/sources/*.yaml` | une config **dédiée** par source, la vôtre (vide dans le dépôt). Les exemples `orders.yaml` et `imdb.yaml` sont dans `examples/config/sources/` |
| `config/defaults/*.yaml` | le **repli** par extension (`*.csv`, `*.txt`, `*.xlsx`, `*.parquet`), avec le même modèle |
| `config/extractor_config.yaml` | la config du **listing** ; le lecteur ne la lit jamais |

Une source dédiée décrit un seul format : ses motifs incluent l'extension (`orders_*.csv`, pas
`orders_*`). Un fichier IMDB au format `.parquet` prend donc le repli `default_parquet` au lieu
d'être lu comme un CSV.

## Quelle config pour quel fichier ?

`SourceRegistry.match` raisonne sur le **nom** du fichier, en minuscules :

1. les sources dédiées d'abord : exactement une correspondance gagne ; **deux ou plus** lèvent une
   `AmbiguousConfigError` qui les nomme ;
2. sinon les replis, avec la même règle ;
3. sinon une `NoMatchingConfigError` : le job marque le fichier `failed`.

Il n'y a **pas de format par défaut** : une extension inconnue est une erreur, pas une lecture CSV.

## Les options de lecture

`reader.options` est passé tel quel à Spark (csv, txt et parquet, valeurs scalaires) ou à
`pyspark.pandas.read_excel` (xlsx). Elles sont vérifiées contre une **liste blanche** par format
(`pipeline/config/reader_options.py`, qui cite ses sources) : c'est ce qui refuse `infer_schema`
quand Spark attend `inferSchema`.

- **Alias.** Déclarer les deux d'une paire (`sep` et `delimiter`, `encoding` et `charset`) est
  refusé : Spark n'en garderait qu'un, sans le dire.
- **Types.** Les options booléennes, entières et numériques sont typées : `header: "yes"` est une
  erreur de validation, écrivez `header: true`.
- **Pas de valeur cachée.** Une option absente du YAML prend le défaut de Spark ou de pandas (par
  exemple `header=false` en CSV).
- `xls` n'est pas pris en charge (`xlrd` n'est pas installé).

## Le schéma explicite : `columns_type`

Les types sont ceux de `pipeline/config/types.py` (`SUPPORTED_COLUMN_TYPES`). Seuls les types
**atomiques** sont acceptés : le lecteur CSV de Spark ne sait pas lire un tableau, une map ni une
structure. Une colonne de liste comme `genre_list` (IMDB) est donc lue en `string`, puis découpée
plus tard.

Le schéma est **positionnel** : gardez l'ordre des colonnes du fichier (Spark ignore alors
`inferSchema`). Avec `enforceSchema: false`, Spark compare aussi l'en-tête de chaque fichier aux noms
déclarés (nombre et noms, casse ignorée) et fait échouer le fichier sur un écart, au lieu de lire
un autre format avec des colonnes décalées ou nulles.

## Ce que la validation refuse

Les erreurs d'un même fichier sont agrégées dans **une** `ConfigError`, qui nomme chaque fichier YAML
et chaque chemin de champ (`reader.options.infer_schema: ...`). Au chargement, elle refuse aussi :

- un `source.name` en double ;
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
   de documentation** (et, si elle n'est pas du texte, à `SPARK_OPTION_TYPES`).
4. Pour un nouveau **format**, il faut un lecteur, une entrée dans `READER_CLASSES` et `FileFormat`
   (qui doivent rester égaux : un test le vérifie), une liste blanche et un test.

La liste des fichiers que le listing trouve se règle à part, dans `config/extractor_config.yaml`
(dossiers, extensions, motifs d'inclusion et d'exclusion).
