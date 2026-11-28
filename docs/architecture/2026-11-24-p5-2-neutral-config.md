# P5-2 — Config indépendante du moteur

- **Date :** 2026-11-24
- **Branche :** `feat/p5-2-neutral-config` (depuis `feat/p5-1-examples`, P5-1 étant en PR #53)
- **Backlog :** `docs/audit/2026-11-23-backlog-phase5.md`, fiche P5-2

## Pourquoi

Charger une config importait PySpark (`pipeline/config/types.py` construisait des types Spark,
`quality.py` importait `NumericType` et `FractionalType`). Impossible donc de la lire au parsing d'un
DAG (P5-3) ni dans un job Polars sans Spark (P5-7). Autres limites : `int` seul, sur 32 bits (pas de
colonne d'identifiants au-delà de 2³¹), et des options de lecture décrites avec les seuls noms Spark.

## Ce qui a changé

- **Types neutres** (`pipeline/config/types.py`). Une table, `NEUTRAL_TYPES`, en chaînes pures
  (bibliothèque standard seulement) : pour `string`, `int`, `long` (nouveau), `float`, `double`,
  `boolean`, `date`, `timestamp`, le nom chez Spark, Polars, DuckDB et Arrow, et les drapeaux
  `numeric` / `fractional` qui remplacent `NumericType` / `FractionalType`. `integer` reste un alias
  de `int` : les configs existantes se chargent à l'identique. `resolve_column_type` remplace
  `get_spark_type` côté config.
- **Conversion Spark dans le moteur** : `pipeline/engine/spark/types.py` (`get_spark_type`,
  `build_schema`), qui lit la classe de `pyspark.sql.types` par son nom dans la table.
  `pipeline/file_reader/utils.py` réexporte (compatibilité). Pas de `pipeline/engine/polars/` :
  il n'aurait aucun appelant avant P5-7.
- **`source.engine`** (`spark` | `polars`, optionnel), sinon `PIPELINE_DEFAULT_ENGINE`, sinon `spark`
  (`pipeline/config/engine.py`). La variable est lue à chaque validation ; une valeur inconnue est
  une erreur de config qui la nomme. `SourceConfig.engine` donne la valeur effective.
- **Options par (moteur, format)** : `READER_OPTIONS_BY_ENGINE` dans `reader_options.py`. Les listes
  Spark sont inchangées. Les listes Polars (csv/txt, parquet, xlsx) viennent des signatures de
  `scan_csv`, `scan_parquet` et `read_excel` ; les paramètres exclus l'ont été exprès (schéma, chemins,
  stockage, `n_rows` qui ferait une lecture partielle silencieuse, réglages de performance).
  Une option Spark déclarée sur une source Polars est refusée avec le nom de l'équivalent quand il
  est établi (`SPARK_TO_POLARS_OPTIONS`), sinon avec « pas d'équivalent Polars établi ». Les
  alias `sep`/`delimiter` ne concernent que Spark. Typage Polars : booléens et entiers,
  `null` et listes de scalaires admis, `encoding` limité à `utf8` / `utf8-lossy`.
- **Regex qualité par moteur** (`rust_regex_problem`). Pour Polars : refus au chargement des
  lookaround, groupes atomiques et références arrière ; rien n'est exigé de `re` de Python
  (`\p{L}` est valide). Pour Spark : inchangé. Le job ne compile en Java que les regex des sources
  Spark ; le code de sortie 5 ne change pas.
- **Garde transitoire** : `BaseFileReader.read_file` lève une `ConfigError` pour une source dont le
  moteur n'est pas `spark` (sinon les options Polars partiraient dans `DataFrameReader.option`).
  Le fichier est `failed` (code 1, une reprise). Il disparaît avec P5-6/P5-7.
- **Tests** (régression nommés P5-2) : `tests/unit/config/test_p5_2_engine_neutral_config.py`,
  `test_p5_2_regex_by_engine.py`, `test_p5_2_polars_options.py`,
  `tests/unit/file_reader/test_p5_2_engine_guard.py`, `tests/integration/test_p5_2_long_type.py`.
  Tests existants adaptés : `build_schema` importé du moteur, `long` ajouté aux valeurs d'échantillon,
  et la table DuckDB de `test_p2_4_transform_dag.py` remplacée par `NEUTRAL_TYPES`.
- Docs : `docs/configuration/sources.md` (moteur, options par moteur, table des types, table
  d'équivalence), `docs/configuration/qualite.md` (regex Polars), `CLAUDE.md`, skill
  `spark-pipeline-standards`.

## Faits vérifiés

- **PySpark absent** : un sous-processus charge `examples/config` avec `SourceRegistry` puis vérifie
  qu'aucun module `pyspark` ni `polars` n'est dans `sys.modules`.
- **Schéma Spark des exemples identique** : `build_schema` des deux exemples est égal à un
  `StructType` construit à la main avec les classes d'avant P5-2.
- **Drapeaux `numeric` / `fractional`** : égaux à `isinstance(..., NumericType / FractionalType)` de
  PySpark pour les huit types.
- **`long` de bout en bout en Spark** (test d'intégration, vrai CSVReader et vrai ParquetLakeWriter) :
  la valeur 3 000 000 000 survit au lake, la colonne est `bigint`. Parquet relu par pyarrow : les types
  de la table (`arrow`) sont ceux écrits par Spark (`timestamp[ns]` pour l'INT96, `date32[day]`).
  Relu par DuckDB (`typeof`, depuis `transform/.venv`) : les types de la table (`duckdb`) sont ceux lus,
  `BIGINT` pour `long`.
- **Contrats dbt** : le test de `test_p2_4_transform_dag.py` compare les `data_type` des contrats à la
  table (`resolve_column_type(...).duckdb`) : toujours vert.
- **Polars 2.2.0** (installé hors du projet, `uv run --with polars`, le 2026-11-24) :
  - signatures de `scan_csv`, `scan_parquet`, `read_excel` : les listes blanches en sont un sous-ensemble,
    et tout paramètre non retenu figure dans la liste d'exclusions du test ;
  - `str.contains` refuse `(?=`, `(?!`, `(?<=`, `(?<!`, `(?>`, `(a)\1`, `(?P=n)`, `\k<n>`, `\h`, `a{` ;
    accepte `\p{L}`, `\z`, `(?P<n>)`, `(?<n>)`, `[[a]]` ;
  - `scan_csv(encoding=...)` ne prend que `utf8` et `utf8-lossy` (`UTF-8` et `latin1` lèvent `ValueError`).
  Ces tests sont ignorés sans Polars : `uv run --with polars pytest tests/unit/config/test_p5_2_polars_options.py
  tests/unit/config/test_p5_2_regex_by_engine.py` (24 passés).
- `uv.lock` et `pyproject.toml` ne changent pas : Polars n'est pas une dépendance avant P5-7.

## Non vérifié, limites

- **Syntaxe Rust incomplète au chargement.** `rust_regex_problem` est un balayage, pas un compilateur :
  `a{` (refusé par Rust), `\h` ou une faute de syntaxe ordinaire passent la validation et échouent à
  l'exécution dans Polars. Un faux positif existe aussi pour `[(?=]` (le balayage ne suit pas les
  classes). Remède prévu : compiler avec Polars quand il est installé (P5-7). Le backlog parle de
  « sans lookaround ni backreference » avec un ❓ : seul ce périmètre est validé.
- **Table d'équivalence = noms.** Les correspondances (`delimiter` → `separator`...) sont celles des
  noms, d'après les signatures ; la sémantique n'a pas été comparée sur des fichiers (valeurs par défaut
  de Spark et de Polars différentes pour plusieurs options). Les options Spark sans équivalent évident
  (`mode`, `dateFormat`, `multiLine`...) n'ont volontairement pas de correspondance.
- **Excel Polars** : seuls les noms des paramètres de `read_excel` sont vérifiés. Le moteur `calamine`
  (dépendance `fastexcel`) n'est pas exercé, ni la forme des dictionnaires `engine_options` /
  `read_options`.
- **`timestamp` en Arrow** : `timestamp[ns]` est ce que pyarrow lit dans l'INT96 écrit par Spark. Un
  lake écrit par Polars en sera peut-être un autre type physique : à trancher en P5-7 (« même schéma
  Parquet, types physiques compris »).
- **Pas de modèle dbt avec une colonne `long`** : le test ne construit pas de contrat dbt sur `long`,
  il vérifie que DuckDB lit `BIGINT`, le type que la table donne pour les contrats.
- **`PIPELINE_DEFAULT_ENGINE` dans les workers** : non propagée explicitement vers les jobs
  `spark-submit` ni les pods Kubernetes ; elle suit l'environnement du processus qui charge la config.
  À traiter avec la fabrique de DAGs (P5-3).
- **Portée du refus Spark pour Polars** : une source `engine: polars` dans un registre ne casse pas le
  job (seuls ses fichiers échouent), mais n'a pas de code de sortie dédié.
- Suite complète, `make check` et docs : voir le résumé de la PR.
