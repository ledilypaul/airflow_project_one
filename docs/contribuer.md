# Contribuer

## Règles de travail

- **Une branche par changement**, jamais de commit direct sur `main` ni sur `develop` : `feat/...` ou
  `fix/...` depuis `develop`, puis une PR vers `develop`.
- **Jamais de secret dans git**, et ne lisez jamais `.env`. `gitleaks` parcourt tout l'historique.
- **Un correctif de bogue arrive avec un test de non-régression**, nommé d'après l'identifiant du
  backlog (`test_p2_6_...`).
- **Dépendances : uniquement `uv`.** Voir plus bas.
- Le backlog priorisé et la ligne de base de l'audit sont dans `docs/audit/` ; une note de conception
  par élément livré est dans `docs/architecture/` ([](decisions.md)).

## Les règles du code

| Règle | Pourquoi |
|---|---|
| **Idempotence d'abord** : travailler sur le périmètre du run, jamais sur « aujourd'hui » ; une écriture remplace sa partition | un retry doit produire le même résultat |
| **Le job Spark n'importe jamais Airflow** (directement ou par `pipeline/`, `utils/`, `storage/`) | le Task SDK interdit la base de métadonnées dans une tâche, et `spark-submit` hérite de cet environnement ; un test garde la règle |
| Importer Airflow **dans** les fonctions qui en ont besoin (`utils/functions_utils.py` par exemple) | même raison |
| Pas de code coûteux au niveau module d'un fichier de DAG (lecture de YAML, connexion, `SparkSession`, `Variable.get()`) | le processeur de DAGs ré-importe les fichiers en continu |
| Imports `airflow.sdk` et `airflow.providers.*`, jamais `airflow.decorators` ni `airflow.operators.*` | ces anciens chemins émettent un `DeprecatedImportWarning`, que la suite de tests transforme en erreur |
| Les lecteurs rendent un **DataFrame Spark** et prennent la `SourceConfig` validée | une seule forme de config ; testez un lecteur avec une config dédiée **et** son repli |
| `quality` et `pipeline.config` n'importent jamais Airflow ni `pipeline.file_reader` | pas de dépendance circulaire ; un test le vérifie |
| Les fichiers de DAG ajoutent la racine du projet à `sys.path` | Airflow les importe par chemin |

## Dépendances : uniquement `uv`

```bash
uv add <paquet>                      # dépendance d'exécution
uv add --group dev <paquet>          # outil de développement
uv add --group airflow <paquet>      # fournisseur Airflow
uv add --group docs <paquet>         # outil de documentation
uv lock --upgrade-package <paquet>   # mettre à jour un seul paquet
uv --directory transform add <paquet>  # côté dbt : un projet uv séparé, son propre lock
```

Utilisez `uv` **0.11 ou plus** pour tout ce qui écrit un lock : le `uv` d'un `pyenv` ancien le
réécrit dans un ancien format. `make check` lance `uv lock --check` à la racine **et** dans
`transform/`. Quelques bornes sont volontaires : `pandas<3` (groupe `spark` : pandas 3 casse `pyspark.pandas`),
`apache-airflow==3.4.0` exact, `astronomer-cosmos>=1.16,<1.17`.

## Lancer les tests

```bash
uv run pytest tests/                                              # toute la suite
uv run pytest tests/unit/file_reader/test_csv_reader.py            # un fichier
uv run pytest tests/unit/file_reader/test_reader_factory.py::test_factory_returns_csv_reader
uv run ruff check . && uv run ruff format --check .
make check                                                        # ce que fait la CI
```

Les tests de DAGs (`tests/unit/dags/`) n'ont besoin ni d'une vraie base Airflow ni d'un scheduler :
leur `conftest.py` active le mode test et pointe la base vers un fichier SQLite jetable. La suite ne
**publie jamais** de lignage ni de métriques (`tests/conftest.py` coupe OpenLineage et OpenTelemetry
et supprime leurs variables d'environnement) ; un test qui en a besoin les active avec `monkeypatch`.

Trois groupes de tests sont **ignorés quand l'outil manque**, sauf si une variable les rend
obligatoires (la CI les pose) :

| Tests | Outil | Rendus obligatoires par |
|---|---|---|
| builds dbt réels (`test_p2_4_dbt_build.py`) | `transform/.venv` | `REQUIRE_DBT=1` |
| job Spark avec l'écouteur de lignage (`test_p2_5_spark_lineage.py`) | le jar de `make lineage-jar` | `REQUIRE_LINEAGE=1` |
| règles Prometheus (`test_p3_3_promtool.py`) | `promtool`, ou Docker | `REQUIRE_PROMTOOL=1` |
| rendu Helm, contexte de build (`test_p3_2_*`) | `helm`, Docker | `REQUIRE_K8S=1` |

`transform_dag` est rendu depuis `transform/target/manifest.json` : sans lui, son import échoue (et
`test_no_import_errors` avec). `make test` lance `make dbt-parse` d'abord.

## Ajouter un élément de pipeline

- **Une source de données** : [](configuration/sources.md#ajouter-une-source).
- **Un contrôle qualité** : un modèle dans `pipeline/config/quality.py`, son exécution dans
  `pipeline/quality/checks.py` (Spark) et `pipeline/engine/polars/quality.py` (Polars), avec un
  cas dans le test de parité `tests/unit/engine/test_p5_7_polars_quality.py` ; ces modules
  n'importent pas Airflow.
- **Un format de fichier** : un lecteur, une entrée dans `READER_CLASSES` et `FileFormat`, une liste
  blanche d'options et un test.
- **Une règle d'alerte** : `deploy/observability/rules/airflow.yml` **et** son test `promtool`. Chaque
  métrique utilisée est vérifiée contre le `metrics_template.yaml` d'Airflow.

## Écrire cette documentation

La documentation est en **Markdown** (MyST) dans `docs/`, avec Sphinx et le thème Furo. Le groupe
`docs` de `pyproject.toml` n'est installé ni par `uv sync` ni dans l'image Docker.

```bash
make docs         # construit docs/_build/html ; un avertissement est une erreur (comme en CI)
make docs-serve   # http://127.0.0.1:8000, reconstruit à chaque modification
make docs-clean
```

Quelques usages :

- les liens entre pages sont des liens Markdown relatifs vers le fichier : `[](guide/installation.md)` ;
- un diagramme s'écrit dans un bloc `{mermaid}` ;
- une mise en garde s'écrit `:::{warning}`, `:::{note}` ou `:::{important}` ;
- la référence de l'API (`reference/autodoc/`) est **générée** à chaque build depuis les docstrings :
  on ne l'édite pas. Les docstrings sont lues comme du Markdown (MyST) : des backticks simples pour
  le code, des listes avec `-`.
- les notes de `docs/architecture/`, `audit/` et `tech-watch/` sont datées et **ne se réécrivent pas** ;
- ajoutez toute nouvelle page à un `toctree` : un document hors de tout `toctree` est un
  avertissement, donc une erreur.
