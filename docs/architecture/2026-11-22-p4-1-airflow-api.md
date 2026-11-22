# P4-1 — Statut et déclenchement par l'API REST d'Airflow

- **Date :** 2026-11-22
- **Item de backlog :** P4-1 (`docs/audit/2026-11-05-baseline.md`)
- **Code :** `scripts/airflow_api.py`, cibles `api-dags` / `api-runs` / `api-trigger` du `Makefile`
- **Tests :** `tests/unit/scripts/test_p4_1_airflow_api.py` (21 cas)

## 1. Ce qui est fait

Airflow 3 expose déjà `/api/v2` : rien à installer côté serveur. Le projet ajoute un petit client en ligne de commande (bibliothèque standard seule) pour les trois appels utiles :

| Commande | Appel |
|---|---|
| `dags` | `GET /api/v2/dags` : id et état (`paused` / `active`) |
| `runs DAG [--limit N]` | `GET /api/v2/dags/{dag}/dagRuns?order_by=-run_after` : run, état, début, fin |
| `trigger DAG [--conf JSON]` | `GET` du DAG puis `POST /api/v2/dags/{dag}/dagRuns` |

Authentification du Simple Auth Manager : `POST /auth/token` avec `{username, password}`, ou un jeton fourni (`AIRFLOW_API_TOKEN`).

## 2. Faits lus dans l'Airflow 3.4.0 installé

- `TriggerDAGRunPostBody.logical_date` n'a **pas de valeur par défaut** : la clé est obligatoire, `null` accepté. Le client envoie `{"logical_date": null, "conf": {...}}`.
- Un run créé sur un DAG **en pause** est accepté mais ne démarre jamais (cf. `CLAUDE.md`, « Running Airflow locally ») et ferait sonner `AirflowDagRunStuckQueued`. `trigger` refuse donc un DAG en pause, sauf `--even-if-paused`.
- `read_file_dag` lancé à la main attend `conf["run_id"]` (le `dag_run_id` d'un listing) : `make api-trigger DAG=read_file_dag CONF='{"run_id": "..."}'`.

## 3. Sécurité

- Identifiants par l'environnement seulement (un mot de passe en argument se voit dans `ps` et l'historique du shell) ; jamais imprimés, ni dans les erreurs.
- Refus avant toute requête : `http` hors localhost, informations utilisateur, requête ou fragment dans l'URL (même règle que l'endpoint OTel, P3-3).
- Redirections **non suivies** : `urllib` renverrait l'en-tête `Authorization` au nouvel hôte.
- Le Makefile passe `DAG` et `CONF` par l'environnement (comme `RUN_ID` pour `spark-job`) : un guillemet ou une espace ne sort pas de la commande.

## 4. Vérifié (2026-11-22)

- Tests unitaires contre un faux serveur HTTP local (flux d'authentification, corps envoyés, DAG en pause, 404, injoignable, redirection, URL refusées, jamais d'identifiant dans la sortie).
- **Contre un vrai `airflow api-server` 3.4.0** (`AIRFLOW_HOME` jetable, SQLite, Simple Auth Manager) : `dags` liste les 5 DAGs, `trigger` refuse le DAG en pause puis, avec `--even-if-paused`, crée un run `queued` avec sa `conf`, `runs` le relit, un DAG inconnu donne `HTTP 404`, un mauvais mot de passe `HTTP 401`.

## 5. Non vérifié

- Pas essayé sur le déploiement kind (même API ; port-forward `make k8s-ui`, soit `AIRFLOW_API_URL=http://127.0.0.1:8081`).
- Pas de commande pour suivre un run jusqu'à la fin (`--wait`), pour dépauser un DAG ni pour lire les tâches : `runs` rappelé en boucle suffit pour l'instant.
- Le jeton JWT d'Airflow a une durée de vie limitée : une commande longue qui le dépasserait n'existe pas ici (chaque appel obtient un jeton neuf).
