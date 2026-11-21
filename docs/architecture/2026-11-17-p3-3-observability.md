# P3-3 — Observabilité : métriques OpenTelemetry, règles d'alerte, rapport d'échec

- **Date :** 2026-11-17
- **Item de backlog :** P3-3 (`docs/audit/2026-11-05-baseline.md`)
- **Code :** `Makefile` (section « Metrics », cibles `metrics-env`, `observability-up/down`), `docker-compose.yaml` (profil `observability`), `deploy/observability/` (collecteur, Prometheus, règles, tests des règles), `utils/failure_alert.py`, `scripts/otel_endpoint_check.py`, callbacks dans les quatre fichiers de `dags/`, `ReadFilesSparkSubmitOperator.spark_exit_code`
- **Détails d'exploitation :** `CLAUDE.md`, section « Observability (metrics and alerts, P3-3) »
- **Modèle suivi :** la note P2-5 (`2026-11-14-p2-5-openlineage.md`) : configuration par l'environnement, jamais de rupture du pipeline, tout optionnel, local d'abord.

## 1. Le problème de départ

Après P2-5, on sait **d'où vient** une table. On ne sait toujours pas **si le pipeline va bien**.

- **Les échecs silencieux sont documentés, pas surveillés.** Un pool manquant laisse `spark_read_files` en `scheduled` pendant 6 heures sans aucune erreur dans l'interface. Un scheduler figé ne crée plus aucun run, et les événements `file_list` s'empilent. Un run qui échoue ne prévient personne.
- **Un échec Spark se lit mal.** Le job sort avec un code de 1 à 6 (`CLAUDE.md`), mais ce code n'apparaît que dans un message d'exception noyé dans le log de la tâche. Pour savoir si c'est une erreur de qualité (6, inutile de relancer) ou une erreur de lecture (1, à examiner), il faut fouiller.
- **Rien n'est mesuré.** Ni la santé du scheduler, ni le nombre de tâches en attente, ni la durée des runs.

## 2. La solution : trois étages, tous optionnels

```
Airflow (scheduler, workers, triggerer, dag-processor, api-server)
   │  métriques OTLP/HTTP (OTEL_EXPORTER_OTLP_ENDPOINT, port 4318)
   ▼
collecteur OpenTelemetry ──(exposition Prometheus, réseau compose seulement)──▶ Prometheus
                                                                                  │
                                                       5 règles d'alerte (deploy/observability/rules/)
                                                       visibles dans http://127.0.0.1:9092/alerts

tâche ou run en échec ──▶ une ligne de log `airflow_failure {JSON}`
                          (dag_id, run_id, tâche, tentative, type d'exception, code de sortie Spark et sa signification)
```

- **Sans `OTEL_EXPORTER_OTLP_ENDPOINT`, rien ne change.** Aucune variable n'est exportée, Airflow garde son défaut (`[metrics] otel_on = False`). Les tests n'émettent jamais.
- **La ligne d'échec est toujours écrite**, avec ou sans pile de métriques : c'est du log, pas une dépendance.

## 3. Les briques et leurs raisons

| Brique | Rôle | Pourquoi ce choix |
|---|---|---|
| **Métriques OTel natives d'Airflow** (`[metrics] otel_on`) | Émettre scheduler, pools, tâches, runs | Intégré à Airflow 3.3.2, aucune dépendance ajoutée : `apache-airflow-core` installe déjà `opentelemetry-exporter-otlp`. |
| **Variables standard `OTEL_*`** (pas `otel_host` / `otel_port`) | Endpoint, protocole, intervalle, nom de service | Depuis Airflow 3.2, les options `[metrics] otel_host`, `otel_port`, `otel_interval_milliseconds`, `otel_debugging_on`, `otel_service` et `otel_ssl_active` sont **dépréciées** au profit des variables standard. Quand les deux sont présentes, Airflow n'utilise que les variables et le dit dans un warning. |
| **Collecteur `otel/opentelemetry-collector-contrib` 0.165.0** | Recevoir l'OTLP, l'exposer en format Prometheus | La distribution « core » n'a pas l'exporteur `prometheus`. Le collecteur découple Airflow du backend : changer de destination ne touche pas Airflow. |
| **Prometheus v3.13.5** (ligne LTS 3.13) | Stocker, évaluer les règles | Destination locale simple, sans compte. 3.13 est la ligne à support long (fin le 2027-07-31), 3.16 n'est pas LTS. Son image contient aussi `promtool`, qui teste les règles. |
| **Règles d'alerte versionnées + tests `promtool`** | Prévenir d'un échec | Le comportement des règles (quand elles se déclenchent, quand elles se taisent) est testé par le vrai moteur PromQL. |
| **`utils/failure_alert.py`** | Une ligne de log structurée par échec | Aucune dépendance, aucun secret, ne lève jamais. Un agrégateur de logs ou un `grep airflow_failure` suffit. |
| **`scripts/otel_endpoint_check.py`** | Ne pas démarrer avec des métriques vers un collecteur absent | Voir §6. |

### Faits vérifiés avant de coder

- **Airflow 3.3.2.** La page [Metrics](https://airflow.apache.org/docs/apache-airflow/3.3.2/administration-and-deployment/logging-monitoring/metrics.html) demande `pip install 'apache-airflow[otel]'` et dit que le SDK « doit être configuré avec les variables standard `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL`... ». Le code installé le confirme : `airflow/_shared/observability/common.py` lit l'endpoint du type de donnée avant le générique (`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` avant `OTEL_EXPORTER_OTLP_ENDPOINT`), prend `http/protobuf` sauf si `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`, et `OTEL_METRIC_EXPORT_INTERVAL` règle l'intervalle (60 s par défaut). Le nom du service vient de `OTEL_SERVICE_NAME`.
- **Pas d'extra à ajouter.** `uv tree --invert --package opentelemetry-sdk` montre `apache-airflow-core` 3.3.2 comme demandeur de `opentelemetry-exporter-otlp` : le lock actuel suffit (`pyproject.toml` et `uv.lock` ne changent pas).
- **Images.** Tags et digests relevés le 2026-11-17 avec `docker buildx imagetools inspect` : `otel/opentelemetry-collector-contrib:0.165.0` (le plus récent tag stable sur Docker Hub), `prom/prometheus:v3.13.5`. Lignes de support : [endoflife.date/prometheus](https://endoflife.date/prometheus). Options de l'exporteur Prometheus du collecteur (`metric_expiration` 5 min par défaut, stabilité « beta ») : [README de l'exporteur](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.165.0/exporter/prometheusexporter).
- **Run réel, 2026-11-17** (`make airflow` avec `OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318`, collecteur et Prometheus du compose, DAGs de sonde dans un `AIRFLOW_HOME` jetable). Noms vus dans Prometheus :

  | Signal | Série Prometheus | Étiquettes utiles |
  |---|---|---|
  | Heartbeat du scheduler | `airflow_scheduler_heartbeat_total` | `instance` |
  | Run en échec (timer, histogramme en ms) | `airflow_dagrun_duration_failed_milliseconds_count` / `_sum` / `_bucket` | `dag_id`, `run_type` |
  | Échec de tâche | `airflow_ti_failures_total` | `dag_id`, `task_id` |
  | Tâches `scheduled`, `queued`, `running` | `airflow_ti_scheduled`, `airflow_ti_queued`, `airflow_ti_running` | `dag_id`, `task_id`, `queue` |
  | Pools | `airflow_pool_open_slots`, `_running_slots`, `_scheduled_slots`, `_starving_tasks` | `pool_name` |

  Les histogrammes exponentiels qu'Airflow produit pour ses timers sont bien exposés par l'exporteur du collecteur en histogrammes classiques (`_bucket`). Airflow émet aussi, pour compatibilité, des séries au nom « legacy » qui contiennent le `dag_id` dans le nom (`airflow_dagrun_duration_failed_probe_fail_milliseconds_count`) : les règles ne s'en servent pas.

## 4. Les règles d'alerte (trois de P3-3, deux de P3-8)

Fichier `deploy/observability/rules/airflow.yml`, testé par `deploy/observability/tests/airflow_rules_test.yml`.

| Alerte | Signal | Choix |
|---|---|---|
| `AirflowDagRunFailed` (critical) | `airflow_dagrun_duration_failed_milliseconds_count` | Le scheduler émet ce timer quand il marque un run `failed`. **Le premier échec d'un DAG est le premier échantillon de la série** (valeur 1) : `increase()` ne voit rien. La règle ajoute donc les séries apparues dans la fenêtre de 15 min (`... unless ... offset 15m`). **Cette branche n'est crue que si le scrape du collecteur est resté continu** : après une coupure de scrape (collecteur ou réseau coupé 15 min ou plus), une série revient avec son ancienne valeur et ressemble à une série « née » sans qu'aucun échec ait eu lieu. La porte exige `min_over_time(up{job="otel-collector"}[16m]) == 1`, au moins 28 échantillons de `up` sur 16 min (32 attendus à 30 s) et un `up` valide il y a 15 min. Limite connue : pendant ses 15 premières minutes, Prometheus peut rater un premier échec (le prix d'une fausse alerte en moins). Testé : sans la branche, « premier échec » échoue ; sans la porte, « trou de scrape, même valeur » (`2x20 _x40 2x40`) échoue ; « redémarrage du scheduler (nouvelle instance) » déclenche. Vérifié en réel : `firing` sur le premier échec de `probe_fail`. |
| `AirflowSchedulerHeartbeatMissing` (critical) | `airflow_scheduler_heartbeat_total` | `sum(increase(...[5m])) == 0` pour un scheduler figé, `absent_over_time(...[5m])` pour un scheduler mort (le collecteur retire une série 5 min après sa dernière mise à jour, `metric_expiration: 5m`). `sum` : après un redémarrage, l'ancienne série reste exposée, constante, pendant 5 minutes, et ne doit pas alerter. Délai : environ 6 minutes après le dernier mouvement du compteur (fenêtre de 5 min, 2 min de `for`), soit **7 minutes ou plus après l'arrêt du scheduler** (7 à 8 mesurés en réel). |
| `AirflowTaskStuckScheduled` (warning) | `airflow_ti_scheduled` | Une tâche `scheduled` depuis 15 min **alors que rien ne tourne ni n'est en file** ne peut pas obtenir de slot : deux causes : le pool manquant de `CLAUDE.md` (un pool inexistant n'exporte aucune métrique de pool, d'où ce signal), ou un **DAG en pause** (le scheduler ne met pas en file les tâches d'un DAG en pause). Pas d'alerte si une autre tâche tourne : un pool `spark` à 1 slot occupé par une instance mappée est une file normale. |
| `AirflowDagImportErrors` (warning, P3-8) | `airflow_dag_processing_import_errors` | Gauge posée par le DAG processor à la fin de chaque boucle d'analyse complète (somme des erreurs d'import de tous les fichiers) : rien avant la première boucle, donc pas de fausse alerte au démarrage. `max(...) > 0` pendant 10 min : le `for` absorbe un fichier en cours d'édition ou un `make dbt-parse` pas fini (`transform_dag` ne s'importe pas sans manifest). `max` et non `sum` : après un redémarrage le collecteur garde 5 min la dernière valeur de l'ancienne instance. Une série absente ou expirée ne déclenche pas (un processor mort n'est pas surveillé ici). `warning` : le scheduler continue avec ce qui est déjà chargé. |
| `AirflowDagRunStuckQueued` (warning, P3-8) | `airflow_scheduler_dagruns_queued`, `airflow_scheduler_dagruns_running` | `queued` est nouvelle en 3.4 (#68068). Un run `queued` pendant que rien ne tourne ne peut pas démarrer : le scheduler ne prend que les runs en file d'un DAG ni en pause ni obsolète (`DagRun.get_queued_dag_runs_to_set_running`), par exemple un déclenchement manuel d'un DAG en pause. Un run derrière `max_active_runs=1` a un run `running` : file normale, pas d'alerte. `AirflowTaskStuckScheduled` ne le voit pas, car aucune instance de tâche n'existe encore. 15 min, `warning`. |

**Filtre sur le service (SEC-4).** Toutes les séries Airflow des règles sont sélectionnées avec `job="airflow_project_one"`, la valeur d'`OTEL_SERVICE_NAME` que `make airflow` fixe et que le scrape conserve (`honor_labels`, label réel vérifié dans Prometheus). Le collecteur accepte de l'OTLP de quiconque l'atteint : une autre instance qui lui enverrait les mêmes noms de séries ne doit ni déclencher ces alertes ni masquer un scheduler mort (cas testés). Si vous changez `OTEL_SERVICE_NAME`, changez-le aussi dans `rules/airflow.yml` (un test compare avec le Makefile).

Garde-fou : `tests/unit/test_p3_3_alert_rules.py` vérifie que chaque série utilisée existe dans le registre de métriques (`metrics_template.yaml`) de l'Airflow **installé**, sous le nom que Prometheus lui donne. Un renommage lors d'une montée de version casse ce test, pas l'alerte.

## 5. Le rapport d'échec

`utils/failure_alert.py`, branché par `on_failure_callback` :

- **Tâche** (`default_args` des quatre DAGs, donc aussi les tâches Cosmos) : **une ligne par tâche en échec**, à la **dernière** tentative seulement, pas à chaque essai. Pour `transform_dag`, un échec de dbt peut donc donner 1 ligne pour le producteur, plus une par capteur qui échoue à son tour (k), plus 1 ligne `dag_run` : 1 + k lignes `task` et 1 ligne `dag_run`.
- **Run** (`on_failure_callback` du DAG) : ce que aucun callback de tâche ne voit, comme `dagrun_timeout` (6 h pour `read_file_dag`, 2 h pour `transform_dag`).
- **Exemple réel** (`dag.test()`, exit 6 du job) :

  ```
  airflow_failure {"dag_id": "read_file_dag", "exception_type": "AirflowFailException", "map_index": 0,
    "max_tries": 1, "operator": "ReadFilesSparkSubmitOperator", "run_id": "manual__...", "scope": "task",
    "spark_exit_code": 6, "spark_exit_code_meaning": "blocking quality checks only, see file_list and quality/<table> in the lake",
    "task_id": "spark_read_files", "try_number": 1, ...}
  airflow_failure {"dag_id": "read_file_dag", "last_task_id": "fail_if_spark_failed", "reason": "task_failure", "run_id": "manual__...", "scope": "dag_run"}
  ```
- **Code de sortie.** `ReadFilesSparkSubmitOperator` garde le code lu sur le hook dans `spark_exit_code` (None tant que `spark-submit` n'a pas tourné). La politique de retry de P2-6 est inchangée. Les codes 1 à 6 sont dupliqués dans `utils/failure_alert.py` (le job importe pyspark), et un test les garde égaux à ceux du job.
- **Sentinelle.** `fail_if_spark_failed` n'a pas de callback : l'instance Spark en échec s'est déjà signalée.
- **Le texte de l'exception n'est pas copié, seulement son type** (`exception_type`). Le texte est déjà dans le log de la tâche, où Airflow masque les secrets qu'il connaît. Recopié dans une ligne JSON, il échapperait à ce masquage : `json.dumps` réécrit `"` et les lettres non ASCII, une copie sur une ligne fusionne les espaces, une troncature coupe la valeur, et le masqueur ne reconnaît plus rien (reproduit puis corrigé, test avec un secret contenant `"`, `é` et des espaces).
- **Sans le projet.** Si `utils` est introuvable (image de worker sans le projet), chaque callback journalise une ligne minimale (`scope`, `dag_id`, `run_id`, `reason`, `task_id`) au lieu de lever : un échec n'est jamais silencieux. Les quatre fichiers de DAG portent ces quelques lignes, car un module partagé serait lui-même du code du projet. Testé avec la racine du projet retirée de `sys.path`, et par `_execute_dag_callbacks` avec un `DagCallbackRequest` (le chemin du DAG processor).
- **Règles de sûreté.** Ne lève jamais. JSON fabriqué par `json.dumps` : une valeur hostile (un `run_id` manuel peut être n'importe quoi) ne peut pas forger un champ. Aucun import d'Airflow, donc rien de nouveau pour le job Spark.

## 6. Un collecteur absent ne casse rien, mais coûte du temps

Mesuré (2026-11-17, un processus qui enregistre une métrique puis sort, comme les processus d'Airflow, `atexit` + `force_flush`) :

| Cas | Durée de sortie |
|---|---|
| Collecteur joignable | 0,2 s |
| Collecteur absent (réglages par défaut) | **10,2 s** (délai du flush final) |
| Collecteur absent, `OTEL_EXPORTER_OTLP_TIMEOUT=1000` ou `3000` | **bloqué, tué à 100 s** (dans les deux cas) |

Avec `airflow dags test`, la tâche réussit (état `success`, code 0) et le processus traîne environ 18 s de plus, avec une erreur `Failed to export metrics batch` toutes les 10 s. **Aucune tâche n'échoue**, mais chaque processus Airflow garde son slot d'exécuteur ce temps-là.

Deux conséquences :

1. **`make airflow` vérifie l'endpoint avant d'activer les métriques** (`scripts/otel_endpoint_check.py`, connexion TCP, 2 s). Injoignable : Airflow démarre **sans métriques**, avec un warning qui nomme `make observability-up`. C'est la même règle que pour le jar OpenLineage absent. Le script ne lève jamais : hôte invalide, port hors plage, étiquette DNS trop longue (`UnicodeError`), crochet IPv6 non fermé sont des « problèmes », pas des exceptions.
   **Identifiants dans l'endpoint (SEC-2).** Un endpoint avec user info (`user:mdp@hôte`) ou avec une query string (où un jeton se met d'ordinaire), dans `OTEL_EXPORTER_OTLP_ENDPOINT` **ou** `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, est refusé même s'il répond : Airflow journalise l'endpoint au niveau INFO. `make airflow` démarre alors sans métriques, avec un avertissement qui nomme la variable, jamais sa valeur. `make metrics-env` masque le user info et la query string (`***`) et n'affiche jamais les `*_HEADERS`.
2. **N'abaissez pas `OTEL_EXPORTER_OTLP_TIMEOUT`.** Contre toute attente, 1 s et 3 s sont bien pires que le défaut (10 s). Cause non élucidée (probablement la boucle de nouvelle tentative de l'exporteur HTTP d'OpenTelemetry 1.45 : seuls les effets ont été mesurés). Le Makefile ne fixe pas ce réglage.

Un collecteur qui tombe **pendant** une session n'est pas détecté : il coûte le même délai par processus, sans autre effet.

## 7. Alternatives écartées

- **Prometheus seul, sans collecteur** (Prometheus 3 sait recevoir de l'OTLP lui-même : non essayé ici). Moins de pièces, mais la demande était un collecteur, qui est aussi le point d'extension naturel (autre backend, échantillonnage, étiquettes). À reconsidérer si le collecteur ne sert à rien d'autre.
- **StatsD + `statsd-exporter`.** Le chemin historique d'Airflow. Il demande un composant et un fichier de correspondance de plus pour retrouver des étiquettes à partir de noms qui contiennent le `dag_id` et le `task_id`. OTel les porte nativement (`dag_id`, `task_id`, `pool_name`), et c'est lui qu'Airflow 3.2 met en avant avec les variables standard.
- **`[metrics] otel_host` / `otel_port`.** Déprécié depuis 3.2.
- **Grafana.** Une image de plus à épingler et à provisionner, pour des tableaux de bord que personne n'a demandés. L'interface de Prometheus suffit pour les trois alertes. Suite possible.
- **Alertmanager.** Il apporterait la notification, donc les secrets (webhook Slack, SMTP). Voir « Hors périmètre ».
- **Notification Slack ou e-mail depuis le callback.** Il faudrait un `Connection` ou une variable d'environnement avec un secret, un appel réseau dans le chemin d'échec, et une dépendance de plus (`apache-airflow-providers-slack` ou `smtp`). Rien de tout cela n'était vérifié ici : laissé de côté plutôt qu'à moitié fait.
- **Métrique « DAG en échec » basée sur `airflow_ti_failures_total`.** Les processus de tâche sont éphémères : chaque échec crée une série neuve (une instance par processus) qui disparaît après 5 minutes. Utilisable, mais plus fragile que le timer du scheduler, qui vit dans un processus long.
- **Un callback `on_failure_callback` qui émet une métrique.** Airflow émet déjà `ti_failures` et la durée des runs.

## 8. Mode d'emploi

```bash
make observability-up                                      # collecteur 127.0.0.1:4318, Prometheus http://127.0.0.1:9092
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 make airflow
make metrics-env                                           # ce que make airflow exporte
make observability-down                                    # arrête les deux conteneurs (garde le volume prometheus-data)
```

- **Priorité.** Une valeur exportée par le shell l'emporte : `AIRFLOW__METRICS__OTEL_ON=False` coupe tout, `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` change de protocole (le collecteur du compose ne reçoit que du HTTP).
- **Un collecteur distant avec authentification.** Les identifiants vont dans `OTEL_EXPORTER_OTLP_HEADERS`, jamais dans l'URL (Airflow journalise l'endpoint). `make metrics-env` n'affiche jamais les variables `*_HEADERS`, et le Makefile avertit d'un `@` dans l'endpoint.
- **Lire les alertes.** `http://127.0.0.1:9092/alerts`. Pour une requête libre : `http://127.0.0.1:9092/graph`, par exemple `airflow_ti_scheduled > 0`.
- **Chercher un échec.** `grep airflow_failure` dans les logs des tâches (`logs/`) ou de votre agrégateur ; le champ `spark_exit_code_meaning` dit si cela vaut la peine de relancer.
- **Ports.** OTLP 4318 et Prometheus 9092, sur `127.0.0.1` seulement (aucun des deux n'a d'authentification), réseau compose `observability` dédié. L'exporteur Prometheus du collecteur (8889) n'est pas publié.
- **Durcissement des conteneurs (SEC-1, SEC-3).** Les deux : `cap_drop: [ALL]` et `no-new-privileges`. Le collecteur : système de fichiers en lecture seule (vérifié : `make observability-up` démarre, reçoit de l'OTLP et l'expose) et `mem_limit: 512m` (son propre limiteur, 256 MiB, reste dessous). Prometheus : `--storage.tsdb.retention.size=1GB` en plus de `7d`, et `--web.cors.origin=https?://(127\.0\.0\.1|localhost):9092` : le défaut, `.*`, laisserait n'importe quelle page web ouverte dans le navigateur interroger `127.0.0.1:9092` (vérifié : une origine étrangère ne reçoit pas d'`Access-Control-Allow-Origin`).
- **Hors poste local, authentifier le récepteur.** Le collecteur accepte l'OTLP de quiconque l'atteint (d'où le filtre `job` des règles, qui n'est pas de la sécurité). Dès que le port n'est plus limité à `127.0.0.1`, il faut une authentification sur le récepteur OTLP (extension `bearertokenauth`, ou mTLS) et la fournir côté Airflow par `OTEL_EXPORTER_OTLP_HEADERS`. Rien de cela n'est configuré ici : le déploiement local n'expose rien.
- **Tests.** `tests/conftest.py` force `AIRFLOW__METRICS__OTEL_ON=False` et retire les variables `OTEL_*` du shell : la suite n'émet jamais. `tests/integration/test_p3_3_promtool.py` utilise `promtool` du PATH, sinon l'image Prometheus du compose, sinon il est ignoré (`REQUIRE_PROMTOOL=1` en fait un échec).

## 9. Hors périmètre, à reprendre

- **Notification.** Alertmanager (Slack, e-mail) ou un `Notifier` Airflow, avec ses secrets dans une `Connection` ou un gestionnaire de secrets, jamais dans le dépôt.
- **Une règle sur `airflow_dag_processing_import_errors`.** Ajoutée par P3-8 (voir ci-dessous).
- **Plusieurs schedulers.** `sum` masque la panne de l'un d'eux tant que l'autre bat. Il faudra des règles par instance et une durée de vie de série adaptée.
- **Premières 15 minutes de Prometheus.** La porte de `AirflowDagRunFailed` est fermée tant qu'il n'a pas 15 minutes de scrape continu : un premier échec dans cette fenêtre n'est pas signalé.
- **Métriques du job Spark.** Le job est un processus à part, sans Airflow : ses métriques (lignes écrites, échecs qualité) demanderaient un autre chemin (listener Spark, ou lecture des résultats `quality/` du lake).
- **Traces OTel.** `[traces] otel_on` existe, non activé ici.
- **Grafana, rétention longue.** Prometheus garde 7 jours ou 1 GB, le premier atteint.
- **CI.** `promtool` passe par l'image Prometheus : les runners GitHub ont Docker, aucune étape à ajouter. Dependabot (`docker-compose`) suivra les deux nouvelles images ; il proposera Prometheus 3.16 (hors LTS) : ajoutez une règle `ignore` si la ligne LTS doit rester.
- **Instrumentation supplémentaire d'Airflow.** `airflow.cfg` du dépôt n'est pas touché : tout passe par l'environnement.

## 10. P3-8 : règles complémentaires (2026-11-22)

Noms vérifiés sur Airflow 3.4.0 installé : lus dans son `metrics_template.yaml`, puis observés sur le vrai collecteur 0.165.0 (conteneur jetable, config du projet, métriques émises par le client de stats d'Airflow) : `airflow_dag_processing_import_errors`, `airflow_scheduler_dagruns_queued` et `airflow_scheduler_dagruns_running` sont des gauges sans suffixe, `airflow_scheduler_loop_exceptions_total` un compteur, avec `job="airflow_project_one"`.

Métriques nouvelles de 3.4 par rapport à 3.3.2 (diff des deux `metrics_template.yaml`), et décision :

| Métrique | Décision |
|---|---|
| `scheduler.dagruns.queued` | **Retenue** (`AirflowDagRunStuckQueued`), voir ci-dessus. |
| `scheduler.loop_exceptions` (compteur) | Écartée. L'exception est relancée : le scheduler sort, ce que `AirflowSchedulerHeartbeatMissing` voit déjà (7 à 8 min). Elle n'ajouterait que `exception_class`, et, comme le premier échec d'un DAG, sa série naît à 1 : il faudrait recopier la porte de scrape de `AirflowDagRunFailed`. Le trou réel est un scheduler qui redémarre en boucle sous un superviseur (le heartbeat reprend), mais les métriques OTel ne sont pas déployées sur Kubernetes. À reprendre avec elles. |
| `scheduler.executor_events.failed` | Écartée : l'exception est relancée, elle devient une `loop_exceptions`. |
| `scheduler.zombies.detected` | Écartée : une tâche sans heartbeat est échouée puis relancée (`retries=1`) ; si elle échoue pour de bon, le run échoue et `AirflowDagRunFailed` alerte. Une alerte par zombie rattrapé par un retry serait du bruit. |
| `scheduler.executor_events.processed` / `batch_size` | Écartées : débit, pas de seuil sensé. |
| `task_state_store.*`, `asset_state_store.*`, `dag.serialization.*`, `triggerer.*`, `kubernetes_executor.*`, `managed_agent.*`, `common_ai.*`, `deadline_alerts.*` | Hors périmètre (le projet n'utilise pas ces fonctions ; ce ne sont pas des métriques de scheduler). |

`scheduler.orphaned_tasks.*`, `scheduler.tasks.killed_externally` et `scheduler.tasks.starving` existaient déjà en 3.3.2 : non nouvelles, non retenues (un pool plein est une file normale).

Limite : la gauge d'erreurs d'import n'est posée qu'à la fin d'une boucle d'analyse. Si une boucle dure plus de 5 min (très gros dossier de DAGs), la série expire côté collecteur et l'alerte ne se déclenche plus (absence = pas d'alerte). Non mesuré ici : le projet n'a que quatre fichiers.
