"""
DAG: read_file_dag
Submits a Spark job (via spark-submit) to read and process files listed in the DB.

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
2. `spark_read_files` est mappé dynamiquement sur
   `resolve_source_run_ids().map(build_spark_command)` : une instance par run
   id source, avec son propre code de sortie, son retry et son statut. La
   commande est construite à l'expansion, sur le worker qui exécute la tâche,
   chaque argument échappé par `shlex.quote` : aucune valeur fournie par
   l'utilisateur n'est interpolée telle quelle dans le shell.
3. `max_active_runs=1` : deux runs ne traitent jamais le même run source en
   parallèle ; les événements arrivés pendant un run sont regroupés dans le
   suivant.

Ne pas migrer `spark_read_files` vers `@task.bash` : il re-rend par Jinja la
chaîne retournée, ce qui rouvrirait l'injection de template depuis un run_id
manuel (voir tests/integration/test_p1_4_read_file_dag_e2e.py).

La sortie de `resolve_source_run_ids` est une simple `list[str]` de run ids,
indépendante de l'opérateur : passer à `SparkSubmitOperator` (P1-5) revient à
remplacer le `.map(build_spark_command)` + `BashOperator` par un mapping sur
`application_args`.
"""

import logging
import shlex
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, Param, dag, task

# Same name as in the listing DAGs: Airflow matches Assets by name/URI, not by
# object identity. Declared here rather than in a shared `dags.*` module, which
# would only be importable with the project root on sys.path (C-1).
FILE_LIST_ASSET = Asset(name="file_list")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
JOB_PATH = PROJECT_ROOT / "pipeline" / "jobs" / "read_files_job.py"

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


def build_spark_command(run_id: str) -> str:
    """Return the spark-submit command line for one source run id.

    Every argument is shell-quoted: `run_id` may come from a manual trigger
    (`dag_run.conf`) and BashOperator runs the string through `bash -c`. The
    run id is passed as a single `--run-id=<value>` argument so that a value
    starting with `-` is not parsed as an option by the job (C-4).
    """
    return f"spark-submit {shlex.quote(str(JOB_PATH))} {shlex.quote('--run-id=' + run_id)}"


@dag(
    dag_id="read_file_dag",
    description="Submit a Spark job to read files listed in the database",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2023, 1, 1),
    schedule=[FILE_LIST_ASSET],
    catchup=False,
    max_active_runs=1,
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
        from airflow.sdk.exceptions import AirflowFailException

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

    BashOperator.partial(
        task_id="spark_read_files",
        env={"PYTHONPATH": str(PROJECT_ROOT)},
        # Without it, `env` replaces the whole environment (PATH, JAVA_HOME,
        # SPARK_HOME, ENV...) of the spark-submit subprocess.
        append_env=True,
    ).expand(bash_command=resolve_source_run_ids().map(build_spark_command))


dag_instance = read_file_dag()
