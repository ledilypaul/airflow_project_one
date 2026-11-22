# Table de correspondance des imports Airflow 2 → 3

À vérifier contre la doc de la version installée : les chemins de compatibilité peuvent disparaître d'une version mineure à l'autre.

| Airflow 2 (déprécié) | Airflow 3 |
|---|---|
| `from airflow.decorators import dag, task` | `from airflow.sdk import dag, task` |
| `from airflow import DAG` / `from airflow.models import DAG` | `from airflow.sdk import DAG` |
| `from airflow.operators.python import get_current_context` | `from airflow.sdk import get_current_context` |
| `from airflow.datasets import Dataset` | `from airflow.sdk import Asset` |
| `from airflow.models import Variable` | `from airflow.sdk import Variable` |
| `from airflow.hooks.base import BaseHook` | `from airflow.sdk import BaseHook` |
| `from airflow.models.param import Param` | `from airflow.sdk import Param` |
| `from airflow.utils.task_group import TaskGroup` | `from airflow.sdk import TaskGroup` |
| `from airflow.operators.python import PythonOperator` | `from airflow.providers.standard.operators.python import PythonOperator` |
| `from airflow.operators.bash import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` |
| `from airflow.operators.empty import EmptyOperator` | `from airflow.providers.standard.operators.empty import EmptyOperator` |
| `from airflow.sensors.filesystem import FileSensor` | `from airflow.providers.standard.sensors.filesystem import FileSensor` |
| `spark-submit` via `BashOperator` | `from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator` |
| `psycopg2` / SQLAlchemy avec `.env` | `from airflow.providers.postgres.hooks.postgres import PostgresHook` |

## Paramètres et clés de contexte supprimés
| Avant | Après |
|---|---|
| `schedule_interval=` | `schedule=` |
| `context["execution_date"]` | `context["logical_date"]` |
| `next_ds`, `prev_ds`, `tomorrow_ds`, `yesterday_ds`… | `data_interval_start` / `data_interval_end` |
| `DagBag(include_examples=False)` | `AIRFLOW__CORE__LOAD_EXAMPLES=False` |
| `airflow db init` | `airflow db migrate` |
| `airflow webserver` | `airflow api-server` |

Vérifié sur ce projet (Airflow 3.3.2) : les imports `airflow.decorators.*`, `airflow.operators.bash.*` et `airflow.operators.python.*` émettent un avertissement de dépréciation ; `DagBag(include_examples=...)` lève `TypeError`.
