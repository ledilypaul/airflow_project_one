"""
P1-4 — spark-submit scope of the ingest DAGs (P5-3: it covered the Asset hand-off
`file_list` between `list_files_dag` and `read_file_dag`, now one DAG).

Covers:
  - the Spark task's environment (P1-5: `env_vars` of SparkSubmitOperator, merged
    into the worker's; P5-3: the engine default and config root the DAG parsed);
  - the DAG's `max_active_runs`;
  - argparse safety of the spark-submit argv (C-4). P1-5: there is no shell any
    more, the argv is built by the installed SparkSubmitHook; this real-process
    round trip is the reference test for "the run id reaches the job's parser
    intact" (P1-5 / C-5: duplicates removed).

The end-to-end "no shell injection / no Jinja rendering" guarantee is covered
by tests/integration/test_p1_4_ingest_dag_e2e.py (C-2).
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from airflow.models import DagBag

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_FOLDER))


def test_p1_4_spark_task_inherits_worker_environment(dagbag, monkeypatch):
    """The spark-submit subprocess must get the *whole* worker environment
    (PATH, JAVA_HOME, SPARK_HOME, ENV...) plus PYTHONPATH pointing at the
    project root: replacing the environment instead of extending it was the
    P1-4 `append_env` bug.

    P1-5: `env_vars` of SparkSubmitOperator. Checked on the *installed*
    SparkSubmitHook with a local master: `submit()` must pass `os.environ`
    updated with `env_vars` to Popen, not `env_vars` alone."""
    from airflow.providers.apache.spark.hooks import spark_submit

    task = dagbag.dags["ingest__orders"].get_task("spark_read_files")
    env_vars = task.env_vars
    assert env_vars["PYTHONPATH"] == str(PROJECT_ROOT)

    monkeypatch.setenv(
        "AIRFLOW_CONN_SPARK_P1_4_ENV", json.dumps({"conn_type": "spark", "host": "local[*]"})
    )
    monkeypatch.setenv("P1_4_WORKER_ONLY_VAR", "kept")
    captured = {}

    class FakePopen:
        def __init__(self, cmd, **kwargs):
            captured["env"] = kwargs.get("env")
            self.stdout = iter(())

        def wait(self):
            return 0

    # `spark_submit.subprocess` is the global `subprocess` module: this
    # replaces `subprocess.Popen` for the whole process (restored by
    # monkeypatch at teardown), not only inside the hook. Nothing else spawns
    # a process during `submit()`.
    monkeypatch.setattr(spark_submit.subprocess, "Popen", FakePopen)
    hook = spark_submit.SparkSubmitHook(conn_id="spark_p1_4_env", env_vars=env_vars)
    hook.submit("job.py")

    env = captured["env"]
    assert env is not None
    assert env["PYTHONPATH"] == str(PROJECT_ROOT)
    assert env["PATH"] == os.environ["PATH"]
    assert env["P1_4_WORKER_ONLY_VAR"] == "kept"


# ---------------------------------------------------------------------------
# Argparse safety of the spark-submit argv
# ---------------------------------------------------------------------------

# The single list of hostile run ids (P1-5 / C-5: the P1-4 and P1-5 copies
# were merged here, with their duplicate tests). Jinja markers are kept: the
# argv must carry them verbatim; that the operator does not *render* them is
# a property of the operator, covered end to end by the integration test.
HOSTILE_RUN_IDS = [
    "x; rm -rf ~",
    "run with spaces",
    "a'b\"c",
    "$(touch pwned)",
    "`touch pwned`",
    "x && touch pwned",
    "-rf",
    "-h",
    "--run-id=other",
    "a=b",
    "trailing.sh",
    "{{ 7*7 }}",
    "{% raw %}x",
]


@pytest.mark.parametrize("run_id", HOSTILE_RUN_IDS)
def test_p1_4_spark_command_round_trips_through_job_parser(run_id, monkeypatch, tmp_path):
    """C-4: the argv spark-submit receives must reach the job's real argparse
    parser with the run id intact. `--run-id -rf` used to be read as an option
    (argparse usage error, exit 2): the run id is passed as a single
    `--run-id=<value>` argument.

    P1-5: no shell any more. The argv is the one the installed SparkSubmitHook
    hands to Popen (list form, no `bash -c`); a real process records the
    arguments it actually received, so nothing in between can split or
    expand them."""
    from airflow.providers.apache.spark.hooks import spark_submit

    from pipeline.jobs.read_files_job import parse_args
    from utils.read_files_operator import JOB_PATH, build_application_args

    monkeypatch.setenv(
        "AIRFLOW_CONN_SPARK_P1_4_ARGV", json.dumps({"conn_type": "spark", "host": "local[*]"})
    )
    real_popen = subprocess.Popen
    received = tmp_path / "argv.json"

    def recording_popen(cmd, **kwargs):
        # Same argv, executed by a real process that records what it got.
        script = "import json, sys; json.dump(sys.argv[2:], open(sys.argv[1], 'w'))"
        return real_popen([sys.executable, "-c", script, str(received), *cmd], **kwargs)

    # `spark_submit.subprocess` is the global `subprocess` module: this
    # replaces `subprocess.Popen` for the whole process (restored by
    # monkeypatch at teardown), not only inside the hook. `real_popen` was
    # captured before, so the recording process itself is really started.
    monkeypatch.setattr(spark_submit.subprocess, "Popen", recording_popen)
    hook = spark_submit.SparkSubmitHook(
        conn_id="spark_p1_4_argv", application_args=build_application_args(run_id, "orders")
    )
    hook.submit(str(JOB_PATH))

    argv = json.loads(received.read_text())
    assert argv[0] == "spark-submit"
    assert argv[-3:] == [str(JOB_PATH), f"--run-id={run_id}", "--source-name=orders"]
    parsed = parse_args(argv[-2:])
    assert (parsed.run_id, parsed.source_name) == (run_id, "orders")
