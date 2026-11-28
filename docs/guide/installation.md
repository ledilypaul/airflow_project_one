# Installation

Cette page prépare une machine de développement : tout tourne en local, sans cluster.

## Prérequis

| Outil | Version | Pourquoi |
|---|---|---|
| Python | 3.12 ou 3.13 | `requires-python = ">=3.12,<3.14"` |
| [uv](https://docs.astral.sh/uv/) | 0.11 ou plus | gestion des dépendances (`pyproject.toml` et `uv.lock`) |
| Java (JRE ou JDK) | 17 | PySpark lance une JVM, y compris en local |
| Docker avec Compose | récent | Postgres (la table `file_list`) et Redis, en local |
| GNU make | — | les commandes courantes du projet |

:::{important}
Les dépendances se gèrent **uniquement avec uv**. N'utilisez ni `pip install`, ni Poetry, ni un
`requirements.txt` pour ajouter un paquet : `pyproject.toml` est la source unique et `uv.lock` est
versionné. Avec un `uv` plus ancien que 0.11, `uv.lock` est réécrit dans un ancien format : si le
`uv` de votre `PATH` est trop vieux, appelez celui de `~/.local/bin/uv`.
:::

## Récupérer le code et les dépendances

```bash
git clone https://github.com/ledilypaul/airflow_project_one.git
cd airflow_project_one
uv sync
```

`uv sync` crée le dossier `.venv` avec le code (installé en mode éditable : `import pipeline.*`
fonctionne sans `PYTHONPATH`), Airflow et les outils de développement. Préfixez ensuite vos
commandes par `uv run`, ou activez `.venv`.

Pour un environnement **Spark seul**, sans Airflow :

```bash
uv sync --no-group airflow
```

Les groupes de dépendances sont les suivants.

| Groupe | Contenu | Installé par `uv sync` |
|---|---|---|
| *(projet)* | PySpark, pandas, pyarrow, SQLAlchemy, Pydantic… | oui |
| `airflow` | Airflow 3.4.0, providers (Postgres, Spark, OpenLineage, Kubernetes), Cosmos | oui |
| `dev` | pytest, ruff, pre-commit | oui |
| `docs` | Sphinx et son thème | non : `uv sync --group docs` |

## Configurer l'accès à la base

Le job Spark et les scripts lisent la connexion Postgres dans un fichier `.env` à la racine
(ignoré par git). Créez-le avec ces cinq variables :

```ini
DB_USER=airflow
DB_PASSWORD=<un mot de passe de votre choix>
DB_HOST=localhost
DB_PORT=5434
DB_NAME=airflow
```

- Le Postgres du projet écoute sur le port **5434** de la machine (pour cohabiter avec un autre
  Postgres sur 5433). `make db-up` vérifie que le port est libre.
- `docker compose` relit ces mêmes variables : il crée Postgres avec cet utilisateur et ce mot de
  passe. Postgres ne les applique qu'à la **création** de son volume ; si le volume existe déjà,
  il garde les identifiants de son premier démarrage.
- Les ports publiés (Postgres 5434, Redis 6379) sont liés à `127.0.0.1` : rien n'est joignable
  depuis le réseau.
- `db_connection()` lève une erreur si une variable manque : il n'y a pas de valeur par défaut.

:::{warning}
Ne committez jamais `.env`. Le dépôt contient une règle `gitleaks` en CI qui parcourt tout
l'historique : un secret poussé par erreur fait échouer le pipeline.
:::

## Démarrer les services et Airflow

```bash
make db-up         # Postgres (port 5434) et Redis, via docker compose
make airflow-init  # migre la base Airflow, crée les Connections et les pools
make airflow       # génère le manifest dbt puis lance Airflow : http://localhost:8080
```

`make airflow-init` est idempotent. Il crée, sans jamais dupliquer :

- la Connection `file_list_db` (Postgres), construite depuis `.env` ;
- la Connection `spark_default` (type `spark`, hôte `local[*]`) ;
- les pools de `deploy/airflow/pools.json` : `spark` (une application Spark à la fois en local),
  `polars` (un job Polars à la fois) et `dbt_duckdb` (un seul écrivain DuckDB).

Airflow tourne ici en mode `standalone` : le serveur d'API, le scheduler, le processeur de DAGs et
le triggerer dans un seul processus. Il affiche au démarrage le mot de passe généré du compte
`admin` (conservé dans `simple_auth_manager_passwords.json.generated`).

:::{note}
Les cibles Airflow du Makefile fixent `AIRFLOW_HOME` à la racine du projet. C'est important : sans
cela, Airflow lit en silence `~/airflow/airflow.cfg`, donc une autre base de métadonnées et
d'autres réglages. La base est le fichier SQLite `./airflow.db` (ignoré par git) ; seule la table
`file_list` vit dans le Postgres de Docker.
:::

## Vérifier l'installation

```bash
make check
```

`make check` rejoue ce que fait la CI : cohérence des deux `uv.lock`, `ruff`, génération du
manifest dbt, puis les tests. Le **premier** `make test` télécharge dbt dans `transform/.venv`.

Étape suivante : [](premier-run.md).
