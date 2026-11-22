# P3-9 — Politiques de retry 3.4 : étude, pas de migration

- **Date :** 2026-11-22
- **Item de backlog :** P3-9 (`docs/audit/2026-11-05-baseline.md`), veille `docs/tech-watch/2026-11-21-airflow-3.4.md`
- **Code étudié :** `dags/local_read_file_dag.py` (`ReadFilesSparkSubmitOperator`, `NO_RETRY_EXIT_CODES`, `_spark_submit_returncode`), `utils/failure_alert.py`
- **Versions lues :** Airflow 3.4.0, Task SDK 1.4.0 (`airflow/sdk/definitions/retry_policy.py`, `execution_time/task_runner.py`), provider Spark 6.3.4

## 1. Question

Les politiques de retry de 3.4 (`retry_policy=` sur une tâche : `ExceptionRetryPolicy`, `ChainRetryPolicy`, `RetryDecision.fail()`) peuvent-elles remplacer le « pas de retry sur les codes 2/3/5/6 », qui lit aujourd'hui `hook._submit_sp.returncode` (attribut privé) ?

## 2. Ce que fait une politique (code lu)

- `RetryPolicy.evaluate(exception, try_number, max_tries, context)` tourne dans le worker, **après** l'échec de `execute`. Elle voit l'exception et le contexte, rien d'autre.
- `FAIL` marque la tâche `failed` sans consommer les retries et stocke `retry_reason` (500 caractères) sur l'instance, visible dans l'UI. `RETRY` ne dépasse jamais `retries`.
- `AirflowFailException` et `AirflowSensorTimeout` échouent toujours tout de suite : la politique n'est **jamais** consultée pour eux (docstring de `evaluate`, `_handle_handler_failure`).

## 3. Pourquoi ça ne retire pas la dépendance au hook

Le code de sortie de spark-submit n'est exposé nulle part de façon publique. Une politique n'aurait que deux sources :

| Source | Problème |
|---|---|
| `hook._submit_sp.returncode` (aujourd'hui) | attribut privé, épinglé par `test_p2_6_no_retry_exit_codes.py` |
| texte de l'exception `AirflowException("Cannot execute: ... Error code is: N.")` | une chaîne du provider : aussi fragile, et à parser par regex ; elle change sans avertissement |
| `context["task"].spark_exit_code` | c'est la valeur que l'opérateur remplit déjà depuis `_submit_sp` : on déplace la lecture, on ne la supprime pas |

En mode Kubernetes, le code vient d'un parsing du journal de spark-submit (`exit code: N`, `_spark_exit_code`) : une politique ne l'améliore pas non plus.

## 4. Ce que la migration changerait

Gains réels, mais minces :

- `retry_reason` visible sur l'instance de tâche (aujourd'hui le message de l'`AirflowFailException` est dans le journal et l'UI du log).
- Règle de retry déclarative (`ExceptionRetryPolicy`) plutôt qu'un `raise` dans `execute`.

Coûts :

- L'opérateur lèverait l'`AirflowException` d'origine : `exception_type` de la ligne `airflow_failure` (`utils/failure_alert.py`) passerait de `AirflowFailException` à `AirflowException`, et le test de la ligne serait à refaire.
- Le test e2e « une tentative sur le code 6, deux sur le code 1 » (`test_p1_4_read_file_dag_e2e.py`) dépend de `dag.test()`, dont le traitement d'une politique n'est pas vérifié ici.
- Une politique ne sait pas décider avant que l'exception soit levée : `_prepare_k8s_cluster` / `_delete_identified_driver` doivent de toute façon rester dans `execute`.

## 5. Décision

**Pas de migration.** Le gain (un `retry_reason` dans l'UI) ne justifie ni le déplacement de la logique, ni le test e2e à refaire, ni le changement du champ `exception_type` des alertes, alors que la dépendance à l'attribut privé reste identique.

À reconsidérer si l'une de ces conditions change :

1. le provider Spark expose le code de sortie publiquement (champ de l'exception ou accesseur du hook) : la politique pourrait alors lire ce champ et `_spark_submit_returncode` disparaîtrait ;
2. on veut un `RESCHEDULE` borné pour une capacité Kubernetes insuffisante (« cluster sans ressources libres »), cas d'usage que `AirflowFailException` ne couvre pas.

## 6. Non vérifié

Le comportement de `retry_policy` sous `dag.test()` et avec `.partial().expand()` (mapped task) n'a pas été exécuté : la décision repose sur la lecture du code de 3.4.0, pas sur un essai.
