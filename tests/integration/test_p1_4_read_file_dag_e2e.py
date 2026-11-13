"""
P1-4 / C-2, P1-5 — end-to-end: read_file_dag really executed with `dag.test()`.

A manual run is triggered with a hostile `run_id` in its conf. A fake
`spark-submit` (first on PATH) records its argv and its PYTHONPATH. The test
proves, on the real operator stack (TaskFlow context injection, dynamic
mapping, SparkSubmitOperator, SparkSubmitHook, the Spark Connection), that:
  - the run succeeds, with one `spark_read_files` instance, in the `spark`
    pool;
  - nothing from the run id is executed by a shell (no `P*` file created);
  - spark-submit receives, at the end of its argv, exactly
    `<JOB_PATH> --run-id=<hostile value>` with `{{ 7*7 }}` NOT rendered by
    Jinja: `application_args` is a templated field of SparkSubmitOperator,
    but a value mapped from an XCom must be skipped by the renderer;
  - the master is the one of the test `spark_default` Connection (not the
    hook's silent `yarn` fallback when the Connection is missing);
  - `triggering_asset_events` / `dag_run` are injected by name (otherwise the
    run id could not be resolved and the run would fail);
  - the worker environment reaches spark-submit, extended with `env_vars`
    (PYTHONPATH): the fake spark-submit is only found through the worker's
    PATH and reads its output file from the worker's environment;
  - C-5: a second `dag.test()` without conf, in the same process, fails
    instead of inheriting the previous run's run id;
  - P1-5 / C-4: a non-zero exit code of spark-submit (3 = no file_list row
    for the run id) fails `spark_read_files` and the DAG run.

Never build the run id into a templated string (`@task.bash`, an f-string in
`application_args`...): Jinja would render it, which reopens template
injection from a manual run id. This test is the one that must catch it (the
`{{ 7*7 }}` assertion).

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

pytestmark = pytest.mark.skipif(os.name != "posix", reason="needs a POSIX PATH")

# Master of the test Spark Connection: deliberately not `local[*]` (the
# documented dev value) nor `yarn` (the hook's fallback when the Connection
# is missing), so the assertion proves where the master came from.
TEST_SPARK_MASTER = "local[2]"

# Runs in the subprocess: a manual dag.test() with `run_id` in its conf,
# optionally followed (chain_second_run="1") by a second dag.test() without
# conf in the same process, exactly the situation where the memoised Param
# leaked (C-5).
DAG_TEST_SCRIPT = textwrap.dedent(
    """
    import json
    import sys

    from airflow.models import DagBag

    dag_file, run_id, result_file, chain_second_run = sys.argv[1:5]
    bag = DagBag(dag_folder=dag_file)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags["read_file_dag"]
    # dag.test() waits for retry_delay (5 min) before a retry: a regression
    # must fail fast instead. Only the retry policy is changed.
    for task in dag.tasks:
        task.retries = 0

    first = dag.test(run_conf={"run_id": run_id})
    first_spark_tis = sorted(
        (ti.map_index, ti.pool, str(ti.state))
        for ti in first.get_task_instances()
        if ti.task_id == "spark_read_files"
    )
    second = dag.test() if chain_second_run == "1" else None

    with open(result_file, "w") as f:
        json.dump(
            {
                "first": str(first.state),
                "first_spark_tis": first_spark_tis,
                "second": None if second is None else str(second.state),
            },
            f,
        )
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
    """A `spark-submit` (one of the binary names the hook accepts) that
    appends its argv and PYTHONPATH, one JSON object per call, then exits
    with `FAKE_SPARK_SUBMIT_EXIT_CODE` (default 0). It reads both from the
    environment: if the hook replaced the worker environment by `env_vars`
    instead of extending it, it would crash."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls_file = tmp_path / "spark_submit_calls.jsonl"
    stub = bin_dir / "spark-submit"
    stub.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['FAKE_SPARK_SUBMIT_CALLS'], 'a') as f:\n"
        "    record = {'argv': sys.argv[1:], 'pythonpath': os.environ.get('PYTHONPATH')}\n"
        "    f.write(json.dumps(record) + '\\n')\n"
        "sys.exit(int(os.environ.get('FAKE_SPARK_SUBMIT_EXIT_CODE', '0')))\n"
    )
    stub.chmod(0o755)
    return bin_dir, calls_file


def _run_dag_test(airflow_env, fake_spark_submit, tmp_path, run_id, *, exit_code, chain_second):
    """Run DAG_TEST_SCRIPT in an isolated Airflow; return (states, logs)."""
    bin_dir, calls_file = fake_spark_submit
    result_file = tmp_path / "result.json"

    env = dict(airflow_env)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"
    env["FAKE_SPARK_SUBMIT_CALLS"] = str(calls_file)
    env["FAKE_SPARK_SUBMIT_EXIT_CODE"] = str(exit_code)
    env.pop("PYTHONPATH", None)
    # The test's own Spark Connection (JSON form: a `spark://local[2]` URI
    # does not parse and silently falls back to yarn). No `spark` pool is
    # created on purpose: dag.test() runs the task instances itself, without
    # the scheduler's pool check, so a missing pool does not block it (a real
    # scheduler would leave the task in `scheduled` until `dagrun_timeout`
    # fails the run, see CLAUDE.md).
    env["AIRFLOW_CONN_SPARK_DEFAULT"] = json.dumps(
        {"conn_type": "spark", "host": TEST_SPARK_MASTER}
    )

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            DAG_TEST_SCRIPT,
            str(READ_DAG_FILE),
            run_id,
            str(result_file),
            "1" if chain_second else "0",
        ],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=600,
    )
    logs = proc.stdout[-5000:] + proc.stderr[-5000:]
    assert proc.returncode == 0, logs
    return json.loads(result_file.read_text()), logs


def test_p1_4_read_file_dag_e2e_hostile_run_id_not_injected(
    airflow_env, fake_spark_submit, tmp_path
):
    _, calls_file = fake_spark_submit
    pwn_dir = tmp_path / "pwn"
    pwn_dir.mkdir()
    hostile = (
        f"x; touch {pwn_dir}/P1 $(touch {pwn_dir}/P2) `touch {pwn_dir}/P3` "
        "{{ 7*7 }} && touch " + f"{pwn_dir}/P4"
    )

    states, logs = _run_dag_test(
        airflow_env, fake_spark_submit, tmp_path, hostile, exit_code=0, chain_second=True
    )

    assert states["first"] == "success", logs
    # One mapped instance (one source run id), in the dedicated pool.
    assert states["first_spark_tis"] == [[0, "spark", "success"]], logs
    assert sorted(p.name for p in pwn_dir.iterdir()) == []
    calls = [json.loads(line) for line in calls_file.read_text().splitlines()]
    # Exactly one spark-submit, only from the first run (C-5: the second run,
    # without conf, must not reuse the first run's run id).
    assert len(calls) == 1, calls
    argv = calls[0]["argv"]
    # The hook puts its options (--master, --name...) first: the job and its
    # single argument must close the argv, untouched.
    assert argv[-2:] == [str(JOB_PATH), f"--run-id={hostile}"]
    assert argv[-1].endswith("{{ 7*7 }} && touch " + f"{pwn_dir}/P4")  # not rendered
    assert argv[argv.index("--master") + 1] == TEST_SPARK_MASTER
    assert "--deploy-mode" not in argv
    assert calls[0]["pythonpath"] == str(PROJECT_ROOT)
    assert states["second"] == "failed", logs


def test_p1_5_read_file_dag_e2e_nonzero_spark_submit_exit_fails_task_and_run(
    airflow_env, fake_spark_submit, tmp_path
):
    """C-4: the contract the job's exit codes rely on. spark-submit exiting 3
    (what the job returns when the run id has no file_list row, propagated by
    spark-submit in client mode) must end `spark_read_files` in `failed`, and
    the DAG run too. Never a green run on a failed Spark application."""
    _, calls_file = fake_spark_submit

    states, logs = _run_dag_test(
        airflow_env,
        fake_spark_submit,
        tmp_path,
        "manual__p1_5_exit_3",
        exit_code=3,
        chain_second=False,
    )

    calls = [json.loads(line) for line in calls_file.read_text().splitlines()]
    assert len(calls) == 1, calls  # retries=0: one attempt
    assert calls[0]["argv"][-1] == "--run-id=manual__p1_5_exit_3"
    assert states["first_spark_tis"] == [[0, "spark", "failed"]], logs
    assert states["first"] == "failed", logs
