---
name: airflow3-standards
description: Règles et standards Apache Airflow 3.x pour ce projet - écriture et revue de DAGs, imports airflow.sdk, idempotence, Assets, connexions, tests de DAG. À charger dès qu'on crée, modifie, revoit ou migre un DAG ou un test de DAG.
when_to_use: "Fichiers dans dags/ ou tests/unit/dags/, mots-clés DAG, TaskFlow, operator, schedule, Asset, XCom, Connection, migration Airflow 2 vers 3."
paths:
  - "dags/**"
  - "tests/unit/dags/**"
  - "plugins/**"
---

# Standards Airflow 3 — airflow_project_one

Cible : **Airflow 3.3.x** (Airflow 2 est en fin de vie depuis avril 2026). Toujours vérifier la doc officielle de la version installée avant d'affirmer qu'une API existe.

## 1. Fichier DAG : rien de coûteux au niveau module
Le DAG processor ré-importe les fichiers en continu. Au top-level : uniquement constantes, imports légers et définition du DAG.
- ❌ Lecture de YAML, connexion DB, `SparkSession`, appel réseau, `Variable.get()` au top-level
- ✅ Imports métier (`pipeline.*`, `utils.*`) **dans** les fonctions de tâche

## 2. Imports Airflow 3
Utiliser `airflow.sdk` pour l'API d'écriture et `airflow.providers.*` pour les opérateurs. Table complète : [references/import-map.md](references/import-map.md).
Contrôle automatique : règles `AIR3xx` de ruff si la version installée les propose (`ruff check --select AIR3 --preview`).

## 3. Style
- **TaskFlow** (`@dag`, `@task`) par défaut. `dag_id` unique par fichier, explicite, en snake_case.
- `start_date` fixe dans le passé, `catchup=False` sauf besoin de backfill, `schedule` (jamais `schedule_interval`).
- `default_args` : `retries` ≥ 1 avec `retry_delay`, `owner` réel, `tags` cohérents.
- Logs via `logging.getLogger(__name__)`, jamais `print`.

## 4. Idempotence (règle n°1 de ce projet)
Relancer un run doit produire exactement le même résultat.
- Les tâches travaillent sur le périmètre **du run** (`dag_run.run_id`, `logical_date`, `data_interval_start/end`), jamais sur « aujourd'hui » (`CURRENT_DATE`, `date.today()`, `datetime.now()`).
- Écritures en upsert/merge ou overwrite de partition, jamais en append aveugle.
- Statut de traitement explicite (`pending` → `processed` / `failed`) mis à jour par la tâche qui traite.

## 5. Données entre tâches
- XCom uniquement pour des **métadonnées** légères (chemins, ids, compteurs). Jamais un DataFrame.
- Dépendances inter-DAG via **Assets** (`airflow.sdk.Asset`) : le DAG producteur déclare `outlets=[asset]`, le consommateur `schedule=[asset]`.
- Un traitement par élément → envisager le **dynamic task mapping** (`.expand()`), en gardant Spark batch quand c'est plus efficace.

## 6. Échecs et cas vides
- « Rien à traiter » n'est pas une erreur : `AirflowSkipException` ou short-circuit, pas `ValueError`.
- Une tâche ne doit pas réussir si une partie de son travail a échoué silencieusement : agréger les erreurs et échouer à la fin.

## 7. Connexions et secrets
- Identifiants via **Connections Airflow** (`PostgresHook(postgres_conn_id=...)`) ou un secrets backend — pas de `.env` lu par le code métier, rien en dur.
- Le code des tâches ne doit pas accéder directement à la base de métadonnées Airflow (interdit en Airflow 3 : passer par le Task SDK / l'API).

## 8. Spark depuis Airflow
`SparkSubmitOperator` (provider `apache-airflow-providers-apache-spark`) plutôt que `BashOperator("spark-submit …")`, avec `conn_id`, `application_args` (dont le `run_id`), et un **pool** dédié pour limiter la concurrence.

## 9. Tests de DAG
Tout DAG a des tests qui vérifient : chargement sans erreur d'import, `dag_id`, ensemble des `task_id`, dépendances, `catchup`, `schedule`. En Airflow 3 :
- `DagBag(dag_folder=...)` sans `include_examples` ; désactiver les exemples via `AIRFLOW__CORE__LOAD_EXAMPLES=False`.
- Vérifier `dag.schedule` / `dag.timetable`, pas `schedule_interval`.
- Pour un test d'exécution local : `dag.test()`.

## 10. Commandes CLI Airflow 3
`airflow db migrate` (plus `db init`), `airflow api-server` (plus `webserver`), `airflow dag-processor` séparé du scheduler, `airflow standalone` pour le dev local.
