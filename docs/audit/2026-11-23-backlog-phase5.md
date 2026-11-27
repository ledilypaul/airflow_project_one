# Backlog phase 5 — produit d'ingestion générique, moteur Polars ou Spark

- **Date :** 2026-11-23
- **Périmètre :** branche `develop` @ `2002c04` (P0 à P4-1 livrés ; `main` en retard de 3 PR)
- **Statut :** remplace `2026-11-05-baseline.md` comme **source de vérité du backlog** pour `/modernize`
  (le fichier le plus récent de `docs/audit/`). La baseline reste la référence des constats d'audit
  (S1…S5, B1…B3, M1…M6, A1) et de l'historique des entrées P0 à P4-1.
- **Légende :** 🔍 constaté à la lecture du code au 2026-11-23 · ❓ à vérifier dans la doc officielle
  avant d'implémenter (règle de l'agent `airflow-modernizer`)

## Décisions validées

1. **Le moteur ne connaît aucune source.** IMDB et orders sont des exemples. Ils sortent du moteur
   et vont dans `examples/`.
2. **Un fichier YAML de source suffit pour obtenir un pipeline complet** : un DAG d'ingestion
   généré, une couche staging dbt générée et des contrôles qualité. Aucun code à écrire.
3. **Deux moteurs de calcul, Polars et Spark, derrière une même interface.** On ne supprime pas le
   travail Spark : il passe derrière l'interface sans changer de comportement. Le moteur se choisit
   par source (`source.engine`), avec un défaut global. Le défaut reste `spark` tant que Polars n'a
   pas atteint la parité (P5-7), puis passe à `polars` (P5-8).
4. **Kubernetes reste un mode de déploiement parmi d'autres.** Avec Polars, son intérêt est
   d'isoler chaque tâche dans un pod dimensionné, pas de distribuer le calcul.
5. **Le lake reste la source de vérité.** Les autres destinations (Postgres d'abord) sont des
   exports du staging, deux ou trois au maximum.
6. **Le ML (P4-2) vient après la phase 5.**

## Règles transverses (toutes les entrées P5)

- **Aucune régression des sources Spark existantes.** Chaque entrée garde la suite complète verte,
  y compris le run e2e `dag.test()` et kind quand le chart est touché. Le contrat des codes de
  sortie du job (0, 1 à 6, voir `CLAUDE.md`) ne change que par ajout (P5-9 ajoute 7).
- **Une branche et une note de conception par entrée** dans `docs/architecture/`, au format des notes
  existantes : pourquoi, faits vérifiés, ce qui n'a pas été vérifié.
- **Aucun import de PySpark au parsing des DAGs, aucun import d'Airflow dans les jobs.** La seconde
  règle existe déjà. La première devient testable dès P5-2 et obligatoire dès P5-3.
- **Idempotence inchangée** : une relance réécrit exactement la partition `(lake_run_id,
  lake_source_file)` et rien d'autre, quel que soit le moteur.
- **Les deux moteurs produisent un lake identique** : mêmes chemins de partition, même schéma
  Parquet (types physiques compris), même table de résultats qualité. dbt, `latest_snapshot` et
  OpenLineage ne doivent pas savoir quel moteur a écrit un fichier.
- **Dépendances** via `uv add` uniquement ; `pyproject.toml` et `uv.lock` commités ensemble.
- **Documentation Sphinx** : chaque entrée met à jour les pages concernées (`docs/configuration/`,
  `docs/exploitation/`, `docs/guide/commandes.md` pour toute nouvelle cible Makefile). Les tests
  `tests/unit/test_docs_setup.py` doivent rester verts.

## Backlog priorisé (source de vérité pour `/modernize`)

| ID | Phase | Tâche | Prérequis | Effort |
|---|---|---|---|---|
| P5-0 | 5 · Produit | Release : `develop` → `main`, CHANGELOG 0.4.0, tag `v0.4.0`, ménage des branches fusionnées | — | S |
| P5-1 | 5 · Produit | Séparer le moteur des exemples (`examples/`), retirer les fichiers obsolètes | P5-0 | M |
| P5-2 | 5 · Produit | Config indépendante du moteur : types neutres (+ `long`), `source.engine`, options validées par (moteur, format), plus de PySpark dans `pipeline.config` | P5-1 | M |
| P5-3 | 5 · Produit | Fabrique de DAGs : un DAG `ingest__<source>` par YAML (listing, lecture, Asset), section `listing` et `schedule` dans le YAML, fin de `extractor_config.yaml` | P5-2 | L |
| P5-4 | 5 · Produit | Génération de la couche staging dbt (sources, `stg_<table>__current`, contrats) depuis les YAML, vérifiée en CI | P5-3 | M |
| P5-5 | 5 · Produit | Onboarding : génère un YAML de source validé à partir d'un fichier exemple (schéma inféré, contrôles proposés) | P5-2 | M |
| P5-6 | 5 · Moteur | Interface `Engine` ; le code Spark actuel passe derrière, sans changement de comportement | P5-2 | M |
| P5-7 | 5 · Moteur | Moteur Polars : lecture, nettoyage, contrôles qualité, écriture du lake, lignage ; tests de parité avec Spark | P5-6 | L |
| P5-8 | 5 · Moteur | Exécution Polars dans Airflow (tâche, pool, codes de sortie), ressources par source sur Kubernetes, Spark en groupe optionnel, défaut `polars` | P5-3, P5-7 | M |
| P5-9 | 5 · Produit | Détection de dérive de schéma (colonnes ajoutées, retirées, type changé), politique par source, code de sortie 7 | P5-6 | M |
| P5-10 | 5 · Produit | Explication de la dérive par LLM (optionnelle, local par défaut), correctif YAML proposé, jamais appliqué seul | P5-9 | S |
| P5-11 | 5 · Produit | Export Postgres du staging (`exports:`), idempotent, par Connection Airflow | P5-3 | M |
| P5-12 | 5 · Produit | Étude : partitionnement métier paramétrable et passage à Iceberg | P5-7 | S |
| P3-1 | 3 · Reste | Services Airflow dans le compose, **ou** retrait de Redis (rien ne l'utilise, licence 7.4 en suspens) | — | S |
| P3-2 | 3 · Reste | Production Kubernetes : stockage multi-nœuds (RWX ou lake objet), NetworkPolicies, `spark.authenticate`, Ingress, suivi des digests | — | L |
| P3-3 | 3 · Reste | Alertmanager et notification (Slack, e-mail), Grafana | — | M |
| P4-1 | 4 · Reste | Client API : `--wait`, dépause, lecture des tâches, essai sur kind | — | S |
| P4-2 | 4 · API / ML | Premier pipeline ML (features → entraînement → registre) — **après la phase 5** | P5-4 | L |

Effort : XS < 1 h · S ≈ demi-journée · M ≈ 1–2 jours · L > 3 jours.

## Fiches techniques

### P5-0 — Release 0.4.0

**Pourquoi.** `main` n'a ni P4-1, ni la doc Sphinx, ni le scan Trivy des images. Il n'existe aucun
tag. `CHANGELOG.md` s'arrête à 0.3.0 (2026-05-04). La phase 5 va changer les identifiants de DAG
et le format des YAML : il faut un point de retour stable avant.

**Étapes.**
1. Section `[0.4.0]` du CHANGELOG, résumée depuis les entrées « Fait » de la baseline (P0-1 à P4-1),
   au format Keep a Changelog déjà utilisé.
2. PR `develop` → `main` (commit de merge), tag `v0.4.0`, release GitHub reprenant la section.
3. Supprimer les branches distantes déjà fusionnées (`git branch -r --merged origin/develop`), après
   avoir listé ce qui sera supprimé dans le résumé.

**Versionnement.** Rester en 0.x pendant la phase 5, puisque le format des YAML et les `dag_id`
changent : `v0.5.0` après P5-4 (jalon de démo), `v1.0.0` après P5-8 (jalon produit).

### P5-1 — Séparer le moteur des exemples

**Constat** 🔍.
- Les configs d'exemple sont dans `config/sources/imdb.yaml` et `orders.yaml`. Les modèles dbt
  d'exemple sont `stg_movies__current`, `stg_orders__current` et
  `mart_movies__by_release_year_language`, dans `transform/models/`.
- `config/extractor_config.yaml` liste `*IMDB*`, `*creditcard*`, `*person*` et `orders_*`.
- Des commentaires et docstrings citent IMDB ou orders dans `pipeline/config/models.py`,
  `registry.py`, `storage/parquet_lake_writer.py`, `pipeline/jobs/read_files_job.py` et
  `dags/local_read_file_dag.py`.
- Environ 25 fichiers de tests lisent les configs d'exemple.
- Fichiers obsolètes relevés à la baseline, toujours présents : `pipeline/IMDBExtractor.py`,
  `pipeline/IMDBTransform.py`, `pipeline/config_file.json`, `dags/test_dag.py`, `sandbox/`,
  `test_notebook.ipynb`, `start_env.sh`, `setup_airflow.sh`.
- Code mort : `ExcelReader.read_polars` (aucun appel, ancien format de config),
  `pipeline/file_reader/json_reader.py` (hérite du routeur `BaseFileReader`, absent de
  `READER_CLASSES`) et `storage/hive_writer.py` (n'est utilisé nulle part).
- `dags/local_list_files_dag_python_operator.py` est une version pédagogique, avec le
  `dag_id` `list_files_dag_classic`.

**Cible.**
```text
config/                 # configs de l'utilisateur ; vide dans le dépôt (README + .gitkeep)
  sources/  defaults/
examples/
  README.md             # comment lancer la démo
  config/sources/       # imdb.yaml, orders.yaml
  data/                 # petits échantillons synthétiques générés par un script (pas de données IMDB réelles)
  transform/            # marts d'exemple (package dbt local)
```
- La racine de config se choisit par `PIPELINE_CONFIG_ROOT` (défaut : `config/`), lue par
  `pipeline.config` et transmise aux jobs et aux DAGs. La démo se lance avec
  `PIPELINE_CONFIG_ROOT=examples/config`. Une cible Makefile `demo` exporte la variable et
  enchaîne le premier run.
- Les marts d'exemple deviennent un package dbt local (`packages.yml` en `local:`), installé
  seulement en démo. Ils peuvent aussi rester dans `transform/models/examples/`, désactivés par
  défaut via `+enabled`. Retenir la solution la plus simple qui laisse le projet dbt générique
  vide de toute source.
- Les configs `config/defaults/*.yaml` restent dans le moteur : ce sont les défauts par extension.

**Étapes.**
1. Déplacer les configs, données et marts d'exemple ; ajouter `PIPELINE_CONFIG_ROOT`.
2. Tests : une constante `EXAMPLES_CONFIG_ROOT`. Les tests qui valident « toutes les configs du
   dépôt » valident `config/` **et** `examples/config/`. Les tests de comportement utilisent des
   configs de `tests/fixtures/` ou les exemples, explicitement.
3. Rendre génériques les docstrings et commentaires du moteur (`<source>`, `<table>`).
4. Supprimer les fichiers obsolètes et le code mort listés plus haut, après `grep` de leurs usages.
   Déplacer le DAG pédagogique dans la documentation (page *concepts*) ou le supprimer. Mettre à jour
   `extend-exclude` de ruff et le `.gitignore`.
5. Docs : `docs/guide/premier-run.md` passe par la cible `demo`.

**Critères d'acceptation.**
- Un test parcourt `pipeline/`, `storage/`, `utils/`, `dags/` et `scripts/` et échoue s'il trouve
  `imdb`, `movies` ou `orders`, casse ignorée.
- La démo produit les mêmes zones du lake et les mêmes tables DuckDB qu'avant.
- La suite est verte et l'e2e passe sur les exemples.

**Hors périmètre.** La génération dbt (P5-4) : ici, on déplace seulement les modèles écrits à la
main.

### P5-2 — Config indépendante du moteur

**Constat** 🔍.
- `pipeline/config/types.py` construit des types Spark (`get_spark_type`, `build_schema`).
  `pipeline/config/quality.py` importe `pyspark.sql.types` (`FractionalType`, `NumericType`).
  Charger une config importe donc PySpark, ce qui empêche de la charger au parsing d'un DAG (P5-3)
  ou dans un job Polars sans Spark installé.
- `int` correspond à `IntegerType`, sur 32 bits. Il n'existe aucun type 64 bits : une colonne
  d'identifiants qui dépasse 2³¹ ne peut pas être déclarée. L'onboarding (P5-5) en aura besoin.
- `reader.options` est une liste blanche par format (`pipeline/config/reader_options.py`), décrite
  avec les noms Spark (et les noms pandas pour Excel).

**Cible.**
- `pipeline/config/` ne dépend que de pydantic et yaml. Un test importe `pipeline.config` et vérifie
  que `pyspark` n'est pas dans `sys.modules`.
- **Types neutres** : `string`, `int` (32 bits), `long` (64 bits, nouveau), `float`, `double`,
  `boolean`, `date`, `timestamp`. Une table de correspondance unique (neutre → Spark, Polars,
  DuckDB, Arrow) vit dans `pipeline/config/types.py` sous forme de chaînes. La conversion en objets
  Spark ou Polars se fait dans le moteur (`pipeline/engine/<moteur>/types.py`), importée
  paresseusement. Les catégories utiles aux validations (`numeric`, `fractional`) sont déclarées
  dans la table et ne viennent plus de PySpark.
- **`source.engine`** : `spark` ou `polars`, optionnel. Sans lui, la valeur vient de
  `PIPELINE_DEFAULT_ENGINE` (défaut : `spark` jusqu'à P5-8).
- **`reader.options` est interprété selon le moteur** : une liste blanche par (moteur, format).
  Les configs Spark existantes ne changent pas. Une option Spark déclarée sur une source Polars
  donne une erreur qui nomme l'équivalent Polars quand il existe (`delimiter` → `separator`,
  `header` → `has_header`, …). Une table d'équivalence documentée sert aussi à P5-5.
- La validation des regex qualité dépend du moteur : syntaxe Java pour Spark, crate `regex` de
  Rust pour Polars (sans lookaround ni backreference) ❓. Elle se fait au chargement de la config
  et non plus au démarrage de Spark pour le cas Polars. Le code de sortie 5 reste inchangé.

**Critères d'acceptation.** Les configs existantes se chargent à l'identique. Le schéma Spark
construit est identique à l'actuel (test de non-régression sur les exemples). Le type `long`
fonctionne de bout en bout en Spark (lecture, lake, contrat dbt `BIGINT`).

### P5-3 — Fabrique de DAGs

**Constat** 🔍.
- Trois DAGs fixes : `list_files_dag` (`schedule=None`, lit `config/extractor_config.yaml`),
  `read_file_dag` (Asset `file_list`) et `transform_dag` (Asset `lake_staging`).
- Le listing est global, avec des motifs dupliqués : `extractor_config.yaml` et `source.patterns`
  décrivent les mêmes fichiers deux fois.
- Un listing mélange toutes les sources dans un même run.

**Cible.**
- Un fichier `dags/source_pipelines.py` charge le registre au parsing et génère, pour chaque
  source de `config/sources/`, un DAG `ingest__<source.name>` :
  `list_files` → `read_files` (Spark ou Polars selon `engine`, P5-8) → émission de l'Asset
  `lake_staging__<source>`.
- `transform_dag` est planifié sur l'union des Assets `lake_staging__*` (`AssetAny` ❓ sous Airflow
  3.4), avec `batch_asset_events=True` conservé.
- Nouvelles sections du YAML (modèle Pydantic, `extra="forbid"`) :
  ```yaml
  listing:
    directories: ["./data/"]     # défaut : config/pipeline.yaml (default_directories)
    exclude: []                  # motifs fnmatch sur le nom de fichier
  schedule: "0 6 * * *"          # cron, preset Airflow, ou null = manuel
  retries: 2                     # optionnel
  tags: [finance]                # optionnel
  ```
  Les motifs d'inclusion restent `source.patterns` : une seule source de vérité.
- `file_list` reçoit une colonne `source_name`. Le DDL de `utils/SQL/script_tables.sql` est mis à
  jour ; le chart le lit déjà via `--set-file fileListSql`. Il faut aussi un `ALTER TABLE`
  idempotent pour les bases existantes. Le job ne lit que les lignes `(run_id, source_name)`.
- Les fichiers qui ne correspondent à aucune source passent par un DAG `ingest__unmatched`, qui
  utilise les défauts par extension (zone raw seulement), comme aujourd'hui.
- **Une config invalide ne doit pas casser les autres DAGs** : une exception au parsing
  supprimerait tous les DAGs du fichier. Chaque source est construite dans un `try`. Une source
  invalide donne un DAG `invalid_source__<nom>` à une seule tâche, qui échoue avec le message de
  validation, pour la rendre visible dans l'UI. La règle d'alerte `AirflowDagImportErrors` ne la
  voit pas, d'où ce DAG ; la CI valide de toute façon toutes les configs.
- Le parsing reste pur : ni base, ni réseau, ni PySpark. Les `dag_id` et noms d'Assets sont
  dérivés de `source.name`, validé comme identifiant (`[a-z0-9_]+`).
- Les trois DAGs historiques et `extractor_config.yaml` sont retirés **dans la même entrée**, une
  fois la parité atteinte. Laisser `list_files_dag` actif à côté ferait traiter deux fois les mêmes
  fichiers.

**Étapes.** Modèle YAML et migration `file_list` → fabrique, avec tests DagBag → portage des tests
e2e et du lignage (`utils/file_list_lineage.py`, noms d'Assets) → retrait des DAGs historiques →
docs (`docs/configuration/sources.md`, `docs/concepts/`).

**Critères d'acceptation.**
- Un YAML ajouté dans une racine de config temporaire fait apparaître `ingest__<nom>` dans la DagBag,
  avec les tâches et le planning attendus.
- Un YAML invalide donne `invalid_source__<nom>`, et les autres DAGs se chargent.
- Après import de `dags/source_pipelines.py`, `pyspark` n'est pas dans `sys.modules`.
- Le parsing de 50 sources générées reste sous une seconde (test de temps, marge large).
- L'e2e `dag.test()` passe sur les exemples, le run kind aussi.

**À vérifier** ❓. Le chargement de DAGs depuis YAML annoncé dans Airflow 3.4 : ne pas s'appuyer
dessus tant qu'il est expérimental, et le noter dans la veille. Vérifier aussi les déclencheurs
d'Assets événementiels (AssetWatcher) pour un planning « à l'arrivée du fichier » : à étudier,
hors périmètre.

### P5-4 — Génération de la couche staging dbt

**Constat** 🔍. `_lake_staging__sources.yml`, `_staging__models.yml` et les `stg_*__current.sql`
sont écrits à la main. Ils dupliquent `reader.columns_type` (un pytest vérifie la cohérence), et
chaque nouvelle source demande trois éditions manuelles.

**Cible.**
- Un générateur, `scripts/generate_dbt_staging.py` (cible Makefile `dbt-codegen`), lit le registre.
  Pour chaque source avec `staging_path` et `columns_type`, il écrit dans
  `transform/models/staging/generated/` :
  - la table de la source `lake_staging` (colonnes métier dans l'ordre du YAML, puis colonnes
    techniques dans l'ordre où DuckDB les relit, comme aujourd'hui) ;
  - `stg_<table>__current.sql` (même `qualify` que `latest_snapshot`, même macro `lake_staging()`) ;
  - le modèle avec `contract: enforced: true` et le test `one_snapshot_per_source_file`.
- Les types passent par la table neutre → DuckDB de P5-2.
- Une source sans `columns_type` est ignorée avec un avertissement : pas de contrat possible.
- Échappatoire : `dbt: {generate: false}` dans le YAML, pour une source dont l'utilisateur écrit
  lui-même le modèle staging.
- **Les fichiers générés sont commités** et un mode `--check` échoue s'ils ne sont pas à jour,
  comme `uv lock --check`. Ce mode tourne dans `make check` et en CI. Avantages : diffs relisibles,
  manifest Cosmos stable. `make dbt-parse` lance le générateur avant le parse.
- Le test de cohérence actuel devient un test du générateur.

**Critères d'acceptation.**
- `dbt build` passe sur les exemples avec les modèles générés, et les tables DuckDB sont identiques
  aux tables actuelles.
- Ajouter une source puis régénérer suffit à construire son modèle staging, sans SQL écrit à la main.
- `--check` échoue après modification d'un `columns_type` sans régénération.

### P5-5 — Onboarding d'une source depuis un fichier exemple

**Cible.**
- Une commande `scripts/onboard_source.py <fichier> --name <nom> [--engine polars|spark]
  [--output config/sources/<nom>.yaml]` (cible Makefile `onboard`). Elle utilise Polars en
  autonome, sans Airflow ni Spark, même pour une source Spark.
- Inférences :
  - format par extension ;
  - séparateur et en-tête (échantillon) ;
  - encodage : UTF-8, BOM, sinon repli signalé ;
  - schéma neutre par Polars : `Int64` → `int` si les valeurs tiennent sur 32 bits, sinon `long` ;
    dates et timestamps détectés ;
  - motif de fichier dérivé du nom, avec chiffres et dates remplacés par `*` ;
  - options écrites pour le moteur choisi, via la table d'équivalence de P5-2.
- Contrôles proposés, **tous en `warn`**, chacun avec un commentaire YAML qui donne la raison
  observée :
  - `not_null` sur les colonnes sans null dans l'échantillon ;
  - `unique` sur une colonne `id` ou `*_id` entièrement distincte ;
  - `accepted_values` sur une colonne texte de 10 valeurs distinctes au plus ;
  - `row_count` `min: 1` ;
  - les bornes de `range` observées sont seulement **commentées**, jamais actives.
- Le YAML produit est validé par `SourceConfig` avant écriture. La commande refuse d'écraser un
  fichier existant sans `--force`. La sortie est déterministe.
- Aucune donnée ne quitte la machine ; aucun LLM (voir P5-10 pour l'option).

**Critères d'acceptation.** Les fixtures CSV (`;` et latin-1, `,` avec en-tête), XLSX et Parquet
donnent des YAML valides. La lecture d'une fixture avec le YAML produit ne donne aucune colonne
entièrement nulle due à un mauvais type, en Spark comme en Polars une fois P5-7 livré.

### P5-6 — Interface `Engine` ; Spark derrière, sans changement de comportement

**Constat** 🔍. Environ 15 modules importent PySpark : readers, `BaseProcessor`, `pipeline/quality/
checks.py`, `ParquetLakeWriter`, `read_files_job.py`, `PostgresWriter`, `utils/spark_session.py`.
L'orchestration du job (statuts `file_list`, codes de sortie, zones raw/staging/qualité,
suppression de la partition staging sur échec bloquant) est mêlée aux appels Spark dans
`read_files_job.py` (627 lignes).

**Cible.**
```python
# pipeline/engine/base.py — aucun import de moteur
class Engine(Protocol):
    name: str  # "spark" | "polars"

    def read(self, path: str, config: SourceConfig) -> Frame: ...
    def clean_for_staging(self, frame: Frame) -> Frame: ...  # lignes vides, doublons exacts
    def run_checks(self, frame: Frame, checks: Sequence[QualityCheck]) -> list[CheckResult]: ...
    def write_lake(
        self, frame: Frame, target: str, *, run_id: str, source_file: str, file_id: int
    ) -> int: ...
    def delete_partition(self, target: str, *, run_id: str, source_file: str) -> bool: ...
    def write_results(self, results: list[CheckResult], target: str, **partition) -> None: ...
    def close(self) -> None: ...
```
- `pipeline/engine/spark/` déplace le code Spark existant derrière l'interface, sans modifier ses
  règles (sémantique NaN, `count_if`, ANSI désactivé, `partitionOverwriteMode=dynamic`, échappement
  des partitions).
- `pipeline/jobs/ingest.py` contient l'orchestration commune : il lit `file_list`, résout les
  configs, appelle l'`Engine`, écrit les statuts et calcule le code de sortie
  (`final_exit_code`). C'est elle qui porte le contrat 0 à 6.
- `pipeline/jobs/read_files_job.py` reste le point d'entrée Spark : même chemin, même CLI
  `--run-id` et même watchdog. `SparkSubmitOperator`, l'image et le chart ne changent pas.
- Le schéma des résultats qualité devient neutre, en schéma Arrow (`pyarrow`), et chaque moteur le
  convertit. `RESULTS_SCHEMA` Spark en est dérivé.

**Critères d'acceptation.** Toute la suite passe sans modifier les assertions des tests Spark
existants, à part les imports. Les lakes produits avant et après le refactoring sont identiques
octet pour octet hors horodatages : même liste de chemins, mêmes schémas Arrow et mêmes lignes
triées.

### P5-7 — Moteur Polars

**Cible.** `pipeline/engine/polars/`, en API **lazy** de bout en bout (`scan_*`, puis
`sink_parquet` ou collecte des seuls agrégats). Un fichier plus gros que la mémoire doit passer
en streaming quand le format le permet.

**Parité à garantir, point par point :**

| Sujet | Spark aujourd'hui | Exigence Polars |
|---|---|---|
| CSV avec `columns_type` | mode PERMISSIVE, valeur non convertible → null (ANSI désactivé) | lire en `String` puis `cast(strict=False)` vers le type neutre : même résultat null |
| En-tête vs schéma | `enforceSchema: false` échoue si noms ou nombre de colonnes diffèrent | même contrôle explicite avant lecture, même échec `failed` |
| Encodage | toute option `encoding` de Spark | `scan_csv` n'accepte que `utf8` / `utf8-lossy` ❓ : décodage préalable signalé, ou refus explicite au chargement de la config |
| Excel | `pyspark.pandas.read_excel` (options pandas) | `pl.read_excel` (moteur calamine, dépendance `fastexcel`) ❓ ; options propres, liste blanche de P5-2 |
| `not_null` | NaN n'est pas null | `is_null()` seul (NaN n'est pas null en Polars) |
| `range` | NaN toujours hors bornes | test explicite `is_nan()` → hors bornes |
| `regex` | `rlike` Java, recherche partielle | `str.contains` (Rust) ; syntaxe validée au chargement (P5-2) |
| `unique` | lignes dont la clé est partagée, nulls ignorés | même définition (`is_duplicated` sur les lignes sans null dans la clé) |
| `accepted_values` | comparaison de la forme texte, `"true"`/`"false"` | même cast texte, mêmes littéraux booléens |
| Partitions | `partitionBy(lake_run_id, lake_source_file)`, valeurs échappées par Spark (`:` → `%3A`, …) | chemins construits **explicitement** avec une fonction d'échappement commune, identique à celle de Spark (et testée contre elle), pas le `partition_by` de Polars tant que son échappement n'est pas prouvé identique ❓ |
| Overwrite | dynamique, partition remplacée, 0 ligne → partition supprimée | écriture dans un répertoire temporaire de la table, puis remplacement de la partition ; même cas 0 ligne |
| Types physiques Parquet | ceux de Spark 4.3 (dont la représentation des timestamps) ❓ | identiques, sinon `union_by_name` de DuckDB élargit le type et le contrat dbt échoue |
| Lignage | listener OpenLineage Spark | événements émis par le job avec le client `openlineage-python`, **mêmes namespace et noms de datasets** que le listener Spark (voir la note P2-5) |

**Tests.**
- Les tests des readers, de la qualité et du writer sont paramétrés par une fixture `engine` qui
  vaut `spark` ou `polars`.
- Un test doré lance les deux moteurs sur les mêmes fichiers et compare : chemins de partition,
  `pyarrow.parquet.read_schema` de chaque fichier, lignes triées, résultats qualité (hors
  horodatages).
- `dbt build` passe sur un lake écrit pour moitié par chaque moteur.

**Hors périmètre.** Exécution dans Airflow (P5-8). Les types `decimal` et complexes.

### P5-8 — Exécution Polars dans Airflow, Kubernetes, dépendances

**Cible.**
- Point d'entrée `python -m pipeline.jobs.ingest --engine polars --run-id <id> --source <nom>`,
  avec les mêmes codes de sortie.
- Dans le DAG généré, une tâche `@task` lance ce sous-processus, renvoie sa sortie dans le log de la
  tâche et applique la règle actuelle : `AirflowFailException` (pas de retry) pour 2, 3, 5, 6, et
  7 après P5-9 ; retry pour le reste. La liste des codes sans retry est partagée avec
  `ReadFilesSparkSubmitOperator` dans un seul module. Un test garantit l'égalité avec les
  constantes du job.
- Le sous-processus garde l'isolation mémoire : un OOM tue le job, pas le worker. Pool `polars`
  dans `deploy/airflow/pools.json`.
- `utils/failure_alert.py` couvre la tâche Polars (code de sortie et sens).
- **Kubernetes** : section optionnelle `execution: {memory: 4Gi, cpu: "2"}` dans le YAML, traduite
  en `executor_config` avec `pod_override` pour le KubernetesExecutor, et ignorée avec un autre
  exécuteur. L'image reste unique (JRE et Polars). Une variante sans JRE pour les déploiements
  100 % Polars est notée en suite.
- **Dépendances** : `polars` et `fastexcel` au cœur ; `pyspark` et `pandas<3` dans un groupe ou un
  extra `spark`, inclus par défaut. `uv sync --no-group spark` donne un environnement Polars seul,
  testé en CI par un job léger.
- Bascule du défaut `PIPELINE_DEFAULT_ENGINE=polars`, mentionnée dans le CHANGELOG. Les sources
  d'exemple déclarent leur moteur explicitement.

**Critères d'acceptation.**
- e2e `dag.test()` vert pour une source Polars et une source Spark dans la même démo, et
  `transform_dag` vert derrière.
- Run kind vert avec une source Polars en pod dimensionné.
- Le job Polars se lance sans PySpark installé.

### P5-9 — Détection de dérive de schéma

**Cible.**
- Avant la lecture typée, le moteur lit le schéma observé du fichier (en-tête et types inférés sur
  un échantillon, ou schéma Parquet). L'orchestration commune le compare à `columns_type` et
  calcule : colonnes ajoutées, retirées, réordonnées (le schéma CSV est positionnel), type
  incompatible et renommage probable (similarité de nom, uniquement pour l'explication).
- Sans `columns_type` (défauts, raw seul), la comparaison se fait avec le schéma observé du
  dernier fichier `processed` de la même source.
- Politique dans le YAML :
  ```yaml
  schema_drift:
    on_added: warn        # ignore | warn | fail
    on_removed: fail
    on_reordered: fail
    on_type_change: fail
  ```
- Les résultats vont dans la table qualité existante (`check_type: schema_drift`, détail JSON dans
  `message`), ce qui réutilise dbt, les alertes et la doc.
- Un nouveau code de sortie **7** (`EXIT_SCHEMA_DRIFT`) est déterministe, donc sans retry. Il faut
  mettre à jour `CLAUDE.md`, `failure_alert`, la priorité de `final_exit_code` (à documenter :
  7 passe avant 6 ?) et les tests.

**Critères d'acceptation.** Une fixture pour chaque type de dérive et pour chaque moteur. La
politique `warn` laisse passer le fichier en `processed`, la politique `fail` donne `failed` et le
code 7.

### P5-10 — Explication de la dérive par LLM (optionnelle)

**Cible.**
- Quand une dérive est détectée et que `llm:` est configuré, une tâche du DAG produit une
  explication et un **correctif YAML proposé**, sous forme de diff. Le correctif est publié dans
  le log et dans un fichier du lake (`drift_reports/`). Il n'est **jamais appliqué
  automatiquement** : l'application passe par une PR, ou par une validation humaine dans Airflow ❓.
- Le provider se configure en local (Ollama) par défaut ou par API.
- Seuls les noms de colonnes, les types et des statistiques agrégées sont envoyés. Aucune ligne de
  données n'est envoyée sauf `llm.include_samples: true` explicite.
- La tâche ne fait jamais échouer le pipeline : en cas d'erreur, un avertissement suffit.

**À vérifier** ❓. Le provider Airflow `common.ai` (version stable, compatibilité avec 3.4,
opérateurs disponibles, validation humaine). Le noter dans la veille avant de coder.

### P5-11 — Export Postgres du staging

**Constat** 🔍. `storage/postgresql_writer.py` lit ses identifiants dans `.env` via
`utils/db_connection.py`. Il accepte Spark ou pandas, en `append` par défaut, donc sans
idempotence.

**Cible.**
- Section du YAML :
  ```yaml
  exports:
    - type: postgres
      connection_id: warehouse_pg    # Connection Airflow
      table: analytics.orders
      mode: snapshot                 # snapshot | upsert
      keys: [order_id]               # requis pour upsert
  ```
- C'est une tâche du DAG généré, après la lecture, indépendante du moteur. Elle lit l'état courant
  du staging (`latest_snapshot`) avec Polars ou DuckDB et écrit via `PostgresHook`, en COPY dans
  une seule transaction.
- `snapshot` remplace les lignes du même `lake_source_file`. `upsert` fait un
  `INSERT … ON CONFLICT (keys)`. Une relance donne le même état.
- Le DDL est généré depuis `columns_type` (table neutre → Postgres), avec création si absente.
  Toute dérive de colonnes est refusée avec un message ; aucune migration automatique.
- Les identifiants viennent uniquement de la Connection : jamais en argument, jamais dans un log.
- Lignage : dataset de sortie Postgres, émis par la tâche.

**Hors périmètre.** Un export Spark JDBC pour les gros volumes, à noter en suite si un besoin réel
apparaît.

### P5-12 — Étude : partitionnement métier et Iceberg

**Constat** 🔍. Les partitions sont fixes, `(lake_run_id, lake_source_file)`, et non
paramétrables (`PARTITION_COLUMNS` dans `storage/parquet_lake_writer.py`). Chaque listing réécrit
un instantané complet : le volume croît à chaque run et l'état courant se recalcule
(`latest_snapshot`, `stg_*__current`).

**Questions de l'étude (note, pas de migration).**
1. Ajouter `storage.partition_by` (colonnes métier sous les partitions techniques). Attention :
   l'overwrite dynamique ne remplace que les partitions présentes dans les nouvelles données. Une
   relance qui produit moins de partitions métier laisserait d'anciennes partitions, ce qui casse
   l'idempotence. Il faudrait supprimer `(run, fichier)` avant d'écrire. Impact sur le glob dbt
   `lake_run_id=*/lake_source_file=*/*.parquet`.
2. Passer à Iceberg :
   - partitionnement caché et modifiable ;
   - `MERGE` à la place de l'instantané par listing ;
   - historique des versions ;
   - écriture Spark par le runtime Iceberg, écriture Polars par `pyiceberg` ❓ ;
   - lecture DuckDB par l'extension `iceberg` ❓ ;
   - catalogue : SQL sur le Postgres existant, ou REST.
3. Coût de migration, effet sur l'idempotence, le lignage, les contrats dbt et Kubernetes (stockage
   objet).

**Livrable.** `docs/architecture/<date>-p5-12-partitioning-iceberg.md`, avec une recommandation et
un prototype minimal pour chaque moteur, puis des entrées de backlog si la migration est retenue.

## Ordre et dépendances

- P5-0 → P5-1 → P5-2, puis trois branches indépendantes à partir de P5-2 :
  - produit : P5-3 → P5-4, et P5-3 → P5-11 ;
  - onboarding : P5-5 ;
  - moteur : P5-6 → P5-7, et P5-6 → P5-9 → P5-10.
- P5-8 attend P5-3 **et** P5-7. P5-12 attend P5-7.
- Ordre conseillé pour un seul agent : P5-0, P5-1, P5-2, P5-3, P5-4, P5-5, P5-6, P5-7, P5-8, P5-9,
  P5-10, P5-11, P5-12.

Jalon de démo après P5-4 (`v0.5.0`) : « je dépose un YAML, j'obtiens un pipeline complet avec
staging dbt ». Jalon produit après P5-8 (`v1.0.0`) : les deux moteurs, Polars par défaut.
