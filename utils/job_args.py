"""Arguments et environnement des jobs d'ingestion, communs à la tâche Spark et
à la tâche Polars (P5-8). Pas d'import d'Airflow ni de moteur."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def build_application_args(run_id: str, source_name: str) -> list[str]:
    """Return the job arguments (`application_args`) for one run of one source.

    The run id is passed as a single `--run-id=<value>` argument so that a
    value starting with `-` (possible from a manual trigger) is not parsed as
    an option by the job (C-4). No shell is involved: SparkSubmitHook passes
    the argument list to `subprocess.Popen` as is. `--source-name` scopes the
    job to the rows of this source (P5-3): two source DAGs can share a run id.
    """
    return [f"--run-id={run_id}", f"--source-name={source_name}"]


def job_env_vars() -> dict[str, str]:
    """`env_vars` of the Spark task (P5-3): the project root for the job's Python
    workers, and the engine default and config root **this parse used**, so the
    job validates the configs as the DAG did (a worker or driver pod whose
    environment differs would otherwise judge them differently).

    Added to the worker's environment in local/client mode, written as
    `spark.*.env` conf (in the logged command: no secret here) on YARN and
    Kubernetes. Raises `ValueError` for an invalid `PIPELINE_DEFAULT_ENGINE`.
    """
    from pipeline.config import default_config_root
    from pipeline.config.engine import DEFAULT_ENGINE_ENV, default_engine

    return {
        "PYTHONPATH": str(PROJECT_ROOT),
        DEFAULT_ENGINE_ENV: default_engine(),
        "PIPELINE_CONFIG_ROOT": str(default_config_root()),
    }
