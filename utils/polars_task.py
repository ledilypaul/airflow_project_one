"""Tâche `polars_read_files` des DAG `ingest__<source>` d'une source
`engine: polars` (P5-8), le pendant de `ReadFilesSparkSubmitOperator`.

- Le job (`pipeline/jobs/polars_job.py`) tourne dans un **sous-processus** :
  un manque de mémoire tue le job, pas le worker. Sa sortie va dans le log de
  la tâche, ligne à ligne. Lancé sans shell, avec la liste d'arguments de
  `build_application_args` (un run id qui commence par `-` reste une valeur).
- Environnement : celui du worker, plus `job_env_vars()` (racine du projet,
  moteur par défaut et racine de config du parsing), comme la tâche Spark.
- Codes de sortie (`utils/exit_codes.py`) : 2, 3, 5 et 6 donnent une
  `AirflowFailException`, jamais retentée ; les autres (1, 4, un signal) une
  `AirflowException`, retentée selon `retries`. Le code est gardé dans
  `polars_exit_code` de la tâche, que lit `utils/failure_alert.py`.
- Si la tâche est interrompue (timeout, arrêt manuel), le job est arrêté ; et
  s'il perd son parent, son watchdog le fait sortir avec le code 4.
- Lignage (OpenLineage, par le collecteur de l'Airflow) : en entrée
  `file_list` et les fichiers listés, en sortie les tables du lake de la
  source, nommés comme le listener Spark les nomme. Jamais d'échec à cause de
  lui.
"""

import logging
import os
import subprocess
import sys
from collections.abc import Iterable
from typing import Any

from utils.exit_codes import MEANINGS, NO_RETRY
from utils.job_args import job_env_vars

log = logging.getLogger(__name__)

POLARS_POOL = "polars"
POLARS_TASK_ID = "polars_read_files"
JOB_MODULE = "pipeline.jobs.polars_job"
# Délai laissé au job pour sortir après SIGTERM, avant SIGKILL.
_STOP_TIMEOUT = 10


def job_command(application_args: list[str]) -> list[str]:
    return [sys.executable, "-m", JOB_MODULE, *application_args]


def run_polars_job(
    application_args: list[str],
    *,
    source_files: Iterable[str] = (),
    targets: Iterable[str] = (),
    task: Any = None,
) -> int:
    """Lance le job Polars et applique la règle des codes de sortie ; 0 s'il réussit."""
    from airflow.sdk.exceptions import AirflowException, AirflowFailException

    command = job_command(application_args)
    log.info("Running %s", command)  # les arguments seulement : aucun secret
    proc = subprocess.Popen(
        command,
        env={**os.environ, **job_env_vars()},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
    )
    try:
        for line in proc.stdout:
            log.info("%s", line.rstrip("\n"))
        code = proc.wait()
    finally:
        _stop(proc)
        report_polars_lineage(source_files, targets)
    if task is not None:
        task.polars_exit_code = code
    if code == 0:
        return code
    meaning = MEANINGS.get(code)
    if code in NO_RETRY:
        raise AirflowFailException(
            f"polars_job exit code {code} ({meaning}): deterministic, not retried"
        )
    raise AirflowException(
        f"polars_job exit code {code} ({meaning or 'unknown: killed by a signal?'}): retried"
    )


def _stop(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(_STOP_TIMEOUT)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def report_polars_lineage(source_files: Iterable[str], targets: Iterable[str]) -> None:
    """Déclare au collecteur d'Airflow ce que le job a lu et écrit. Ne lève jamais."""
    try:
        from airflow.sdk.lineage import NoOpCollector, get_hook_lineage_collector

        collector = get_hook_lineage_collector()
        if isinstance(collector, NoOpCollector):
            return
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from storage.lake_layout import table_location
        from storage.lake_root import resolve_lake_root
        from utils.file_list_lineage import report_file_list_lineage
        from utils.functions_utils import FILE_LIST_POSTGRES_CONN_ID
        from utils.lake_lineage import spark_dataset

        hook = PostgresHook(postgres_conn_id=FILE_LIST_POSTGRES_CONN_ID)
        report_file_list_lineage(hook, role="input", source_files=source_files)
        root = resolve_lake_root()
        for target in targets:
            namespace, name = spark_dataset(table_location(root, target))
            if namespace != "file":  # le job Polars n'écrit qu'en local (P5-7)
                log.warning("OpenLineage: %s not declared (namespace %s)", name, namespace)
                continue
            collector.add_output_asset(context=hook, scheme="file", asset_kwargs={"path": name})
    except Exception:
        log.warning("OpenLineage: could not report the Polars job lineage (ignored)", exc_info=True)
