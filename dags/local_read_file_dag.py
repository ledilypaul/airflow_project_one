"""
DAG: read_file_dag
Submits a Spark job (SparkSubmitOperator) to read and process files listed in the DB.

Airflow orchestre, Spark s'exécute dans son propre processus séparé.

P1-4 : le DAG est planifié sur l'Asset `file_list`, émis par la tâche
d'insertion de `list_files_dag` / `list_files_dag_classic` quand elle a
enregistré au moins un fichier.

1. `resolve_source_run_ids` détermine les run ids sources à traiter :
   - run déclenché par l'Asset : le `source_run_id` de **tous** les événements
     `file_list` consommés (Airflow peut en regrouper plusieurs dans un seul
     run), dédupliqués, dans l'ordre ;
   - déclenchement manuel (aucun événement) : `dag_run.conf["run_id"]`, sans
     espaces autour ;
   - ni l'un ni l'autre : échec explicite (jamais de succès silencieux).
2. `spark_read_files` (P1-5) est un `SparkSubmitOperator` mappé dynamiquement
   sur `application_args=resolve_source_run_ids().map(build_application_args)` :
   une instance par run id source, avec son propre code de sortie, son retry
   et son statut. Le hook lance `spark-submit` sous forme de liste
   d'arguments, sans shell : le run id est un seul argument `--run-id=<id>`,
   jamais découpé ni interprété.
   - Master Spark : Connection `spark_default` (`local[*]` en dev, voir
     CLAUDE.md). Le hook retomberait silencieusement sur `yarn` si elle
     manquait : `resolve_source_run_ids` vérifie donc qu'elle existe et fait
     échouer le run sinon (P1-5 / C-2).
   - Pool `spark` (deploy/airflow/pools.json) : limite le nombre
     d'applications Spark simultanées. S'il n'existe pas, le scheduler laisse
     la tâche en `scheduled` sans erreur visible dans l'UI (avertissement
     dans le log du scheduler seulement), jusqu'à `dagrun_timeout`.
   - Ressources (mémoire, cœurs...) : pas dans le DAG, mais dans la
     Connection / `spark-defaults.conf` de chaque environnement.
3. `max_active_runs=1` : deux runs ne traitent jamais le même run source en
   parallèle ; les événements arrivés pendant un run sont regroupés dans le
   suivant.

`application_args` est un champ templaté de `SparkSubmitOperator` : la valeur
mappée depuis l'XCom n'est pas rendue par Jinja (Airflow ignore les valeurs
résolues à l'expansion). Ne jamais construire le run id dans une chaîne
templatée (f-string dans `application_args` passé à `.partial()`,
`@task.bash`...) : Jinja la rendrait, ce qui rouvrirait l'injection de template
depuis un run_id manuel (voir tests/integration/test_p1_4_read_file_dag_e2e.py).
"""

import logging
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Asset, Param, dag, task

# Same name as in the listing DAGs: Airflow matches Assets by name/URI, not by
# object identity. Declared here rather than in a shared `dags.*` module, which
# would only be importable with the project root on sys.path (C-1).
FILE_LIST_ASSET = Asset(name="file_list")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
JOB_PATH = PROJECT_ROOT / "pipeline" / "jobs" / "read_files_job.py"

# Spark master (and deploy mode, queue...) come from this Connection, not from
# the DAG. Missing Connection: the provider's hook silently falls back to
# `--master yarn`, which fails locally without HADOOP_CONF_DIR.
SPARK_CONN_ID = "spark_default"
# Dedicated pool: caps concurrent Spark applications across DAGs and mapped
# instances. Must exist (`airflow pools set spark ...`): with a missing pool
# the scheduler leaves the task in `scheduled` forever.
SPARK_POOL = "spark"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Valeurs de `run_id` qui ne désignent aucun run : refusées comme le fait déjà
# le job Spark (code de sortie 2), mais ici avant même de lancer spark-submit.
_MISSING_RUN_ID_VALUES = {"", "None"}

log = logging.getLogger(__name__)


def build_application_args(run_id: str) -> list[str]:
    """Return the job arguments (`application_args`) for one source run id.

    The run id is passed as a single `--run-id=<value>` argument so that a
    value starting with `-` (possible from a manual trigger) is not parsed as
    an option by the job (C-4). No shell is involved: SparkSubmitHook passes
    the argument list to `subprocess.Popen` as is.
    """
    return [f"--run-id={run_id}"]


@dag(
    dag_id="read_file_dag",
    description="Submit a Spark job to read files listed in the database",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2023, 1, 1),
    schedule=[FILE_LIST_ASSET],
    catchup=False,
    max_active_runs=1,
    # P1-5 / C-3: without it, a missing `spark` pool (task left in
    # `scheduled`, silently) or a stuck spark-submit freezes the run forever,
    # and with max_active_runs=1 every later `file_list` event piles up behind
    # it. On timeout the scheduler fails the run and marks its unfinished
    # tasks `skipped` (a running spark-submit is killed; the job's parent
    # watchdog then stops the Python driver, C-1).
    dagrun_timeout=timedelta(hours=6),
    tags=["processing", "spark", "file-reading"],
    params={
        # UI form + validation of the run conf only: the task reads
        # dag_run.conf, not params (Param.resolve() memoises its value, C-5).
        "run_id": Param(
            None,
            type=["null", "string"],
            description="Manual trigger only: run_id of the list_files_dag run to process. "
            "Leave empty for Asset-triggered runs (the source run ids come from the "
            "triggering `file_list` events). A run with neither fails explicitly "
            "(idempotence: P1-3/C-1, P1-4).",
        )
    },
)
def read_file_dag():
    @task
    def resolve_source_run_ids(
        triggering_asset_events: Mapping[Any, Any] | None = None,
        dag_run: Any = None,
    ) -> list[str]:
        from airflow.sdk import BaseHook
        from airflow.sdk.exceptions import AirflowFailException, AirflowNotFoundException

        # P1-5 / C-2: SparkSubmitHook silently falls back to `--master yarn`
        # when its Connection is missing. Fail here, explicitly, instead.
        try:
            spark_conn = BaseHook.get_connection(SPARK_CONN_ID)
        except AirflowNotFoundException:
            raise AirflowFailException(
                f"Airflow Connection {SPARK_CONN_ID!r} not found: spark_read_files would "
                "silently run `spark-submit --master yarn`. Create it first (see CLAUDE.md, "
                "'Running Airflow locally')."
            ) from None
        log.info("Spark master from Connection %r: host=%r", SPARK_CONN_ID, spark_conn.host)

        events = (triggering_asset_events or {}).get(FILE_LIST_ASSET, [])
        run_ids: list[str] = []
        for event in events:
            if event.source_run_id is None:
                # Event created through the REST API, not by a producer task:
                # it cannot be scoped to a list_files_dag run.
                log.warning("Ignoring `file_list` asset event without source_run_id: %s", event)
                continue
            if event.source_run_id not in run_ids:
                run_ids.append(event.source_run_id)

        # Read from this run's conf (already validated against the Param
        # schema when the run was created), not from `params`: the Param
        # object memoises the last resolved value across runs (C-5).
        conf = getattr(dag_run, "conf", None) or {}
        manual_run_id = conf.get("run_id")
        if manual_run_id is not None:
            manual_run_id = str(manual_run_id).strip()
            if manual_run_id in _MISSING_RUN_ID_VALUES:
                manual_run_id = None

        if run_ids:
            if manual_run_id is not None:
                log.warning(
                    "conf run_id=%r ignored: run triggered by `file_list` asset events",
                    manual_run_id,
                )
            log.info("Source run ids from asset events: %s", run_ids)
            return run_ids

        if manual_run_id is not None:
            log.info("No asset event, using conf run_id=%r", manual_run_id)
            return [manual_run_id]

        raise AirflowFailException(
            "No source run_id to process: no `file_list` asset event triggered this run and "
            "the run conf has no run_id. Trigger read_file_dag with a run_id (Trigger DAG w/ "
            "config) to target a specific list_files_dag run (idempotence: P1-3/C-1, P1-4)."
        )

    SparkSubmitOperator.partial(
        task_id="spark_read_files",
        application=str(JOB_PATH),
        conn_id=SPARK_CONN_ID,
        pool=SPARK_POOL,
        # Local/client mode: the hook *adds* these to the worker environment
        # (os.environ.copy().update(env_vars)), PATH/JAVA_HOME/SPARK_HOME/ENV
        # are kept. Lets the job's Python workers import pipeline.*/utils.*
        # even with an interpreter that lacks the editable install.
        # Never pass a secret here: in yarn/k8s mode the hook writes env_vars
        # into the logged spark-submit command (`--conf spark.*.env.X=...`) in
        # clear, masking only keys containing `secret` or `password` (C-6).
        env_vars={"PYTHONPATH": str(PROJECT_ROOT)},
    ).expand(application_args=resolve_source_run_ids().map(build_application_args))


dag_instance = read_file_dag()
