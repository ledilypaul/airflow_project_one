"""
P1-4 / C-2 — end-to-end: read_file_dag really executed with `dag.test()`.

A manual run is triggered with a hostile `run_id` in its conf. A fake
`spark-submit` (first on PATH) records its argv. The test proves, on the real
operator stack (TaskFlow context injection, dynamic mapping, BashOperator,
`bash -c`), that:
  - the run succeeds;
  - nothing from the run id is executed by the shell (no `P*` file created);
  - spark-submit receives exactly `--run-id=<hostile value>` with `{{ 7*7 }}`
    NOT rendered by Jinja;
  - `triggering_asset_events` / `dag_run` are injected by name (otherwise the
    run id could not be resolved and the run would fail);
  - the worker environment reaches spark-submit (`append_env=True`: without
    it, the fake spark-submit is not on PATH and the run fails);
  - C-5: a second `dag.test()` without conf, in the same process, fails
    instead of inheriting the previous run's run id.

DO NOT migrate `spark_read_files` to `@task.bash`: `@task.bash` renders the
string returned by the decorated function through Jinja again, which would
reopen template injection from a manual run id. This test is the one that must
catch it (the `{{ 7*7 }}` assertion).

Isolation: everything Airflow runs in a subprocess with its own AIRFLOW_HOME
and SQLite metadata DB (migrated once per module) under pytest's tmp dir, so
the in-process Airflow configuration of the other tests
(tests/unit/dags/conftest.py) is untouched. No real Spark, no absolute path of
a given machine: runnable as is on the GitHub Actions runner.
"""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / "dags"
READ_DAG_FILE = DAGS_FOLDER / "local_read_file_dag.py"
JOB_PATH = PROJECT_ROOT / "pipeline" / "jobs" / "read_files_job.py"

pytestmark = pytest.mark.skipif(os.name != "posix", reason="needs bash and a POSIX PATH")

# Runs in the subprocess: two chained dag.test() calls in the same process,
# exactly the situation where the memoised Param leaked (C-5).
DAG_TEST_SCRIPT = textwrap.dedent(
    """
    import json
    import sys

    from airflow.models import DagBag

    dag_file, hostile_run_id, result_file = sys.argv[1:4]
    bag = DagBag(dag_folder=dag_file)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags["read_file_dag"]
    # dag.test() waits for retry_delay (5 min) before a retry: a regression
    # must fail fast instead. Only the retry policy is changed.
    for task in dag.tasks:
        task.retries = 0

    first = dag.test(run_conf={"run_id": hostile_run_id})
    second = dag.test()

    with open(result_file, "w") as f:
        json.dump({"first": str(first.state), "second": str(second.state)}, f)
    """
)


@pytest.fixture(scope="module")
def airflow_env(tmp_path_factory):
    """Environment of an isolated Airflow with a migrated SQLite metadata DB."""
    home = tmp_path_factory.mktemp("airflow_home")
    env = {k: v for k, v in os.environ.items() if not k.startswith("AIRFLOW")}
    env.update(
        AIRFLOW_HOME=str(home),
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{home / 'airflow.db'}",
        AIRFLOW__CORE__DAGS_FOLDER=str(DAGS_FOLDER),
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__CORE__UNIT_TEST_MODE="True",
    )
    subprocess.run(
        [sys.executable, "-m", "airflow", "db", "migrate"],
        env=env,
        check=True,
        capture_output=True,
        timeout=600,
    )
    return env


@pytest.fixture
def fake_spark_submit(tmp_path):
    """A `spark-submit` that appends its argv (one JSON list per call)."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls_file = tmp_path / "spark_submit_calls.jsonl"
    stub = bin_dir / "spark-submit"
    stub.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['FAKE_SPARK_SUBMIT_CALLS'], 'a') as f:\n"
        "    f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
    )
    stub.chmod(0o755)
    return bin_dir, calls_file


def test_p1_4_read_file_dag_e2e_hostile_run_id_not_injected(
    airflow_env, fake_spark_submit, tmp_path
):
    bin_dir, calls_file = fake_spark_submit
    pwn_dir = tmp_path / "pwn"
    pwn_dir.mkdir()
    hostile = (
        f"x; touch {pwn_dir}/P1 $(touch {pwn_dir}/P2) `touch {pwn_dir}/P3` "
        "{{ 7*7 }} && touch " + f"{pwn_dir}/P4"
    )
    result_file = tmp_path / "result.json"

    env = dict(airflow_env)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"
    env["FAKE_SPARK_SUBMIT_CALLS"] = str(calls_file)

    proc = subprocess.run(
        [sys.executable, "-c", DAG_TEST_SCRIPT, str(READ_DAG_FILE), hostile, str(result_file)],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert proc.returncode == 0, proc.stdout[-5000:] + proc.stderr[-5000:]
    states = json.loads(result_file.read_text())
    logs = proc.stdout[-5000:] + proc.stderr[-5000:]

    assert states["first"] == "success", logs
    assert sorted(p.name for p in pwn_dir.iterdir()) == []
    calls = [json.loads(line) for line in calls_file.read_text().splitlines()]
    # Exactly one spark-submit, only from the first run (C-5: the second run,
    # without conf, must not reuse the first run's run id).
    assert calls == [[str(JOB_PATH), f"--run-id={hostile}"]]
    assert calls[0][1].endswith("{{ 7*7 }} && touch " + f"{pwn_dir}/P4")  # not rendered
    assert states["second"] == "failed", logs
