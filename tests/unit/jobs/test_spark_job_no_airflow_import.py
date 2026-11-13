"""
Regression: the Spark job must never import Airflow, directly or indirectly.

`read_file_dag` runs `pipeline/jobs/read_files_job.py` through
`SparkSubmitOperator`, i.e. inside the environment of an Airflow 3 task. The
Task SDK supervisor deliberately sets
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=airflow-db-not-allowed:///` in every task
process to forbid direct access to the metadata DB, and the operator merges
that environment into the one of `spark-submit`. As soon as anything imports
`airflow`, `airflow/__init__.py` runs `settings.initialize()`, which fails on
that URL with a `sqlalchemy.exc.ArgumentError` (not an `ImportError`). The job
used to import `PostgresHook` at the top of `utils/functions_utils.py` and
crashed at import time in a real end-to-end run.

The check runs in a fresh subprocess: the pytest process has already
imported Airflow (DAG tests), so `sys.modules` there proves nothing.
"""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent

# Exactly what the Airflow 3 Task SDK supervisor puts in the environment of a
# task process (airflow/sdk/execution_time/supervisor.py).
FORBIDDEN_METADATA_DB_URL = "airflow-db-not-allowed:///"

CHILD_SCRIPT = textwrap.dedent(
    """
    import sys

    import pipeline.jobs.read_files_job  # noqa: F401

    loaded = sorted(m for m in sys.modules if m == "airflow" or m.startswith("airflow."))
    if loaded:
        print("airflow modules loaded: " + ", ".join(loaded))
        sys.exit(1)
    print("OK")
    """
)


def test_spark_job_does_not_import_airflow():
    # The job runs in the environment of an Airflow 3 task, which forbids
    # access to the metadata DB: importing `airflow` there blows up.
    env = {**os.environ, "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": FORBIDDEN_METADATA_DB_URL}

    result = subprocess.run(
        [sys.executable, "-c", CHILD_SCRIPT],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode == 0, (
        f"importing the Spark job failed or loaded Airflow\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert result.stdout.strip().endswith("OK")
