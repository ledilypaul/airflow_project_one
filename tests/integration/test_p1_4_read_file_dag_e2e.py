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
    for the run id) fails `spark_read_files` and the DAG run;
  - P2-4 / C-2: `publish_lake_staging` emits the `lake_staging` Asset when at
    least one file of the run is `processed` (Spark succeeded, or exit 1 with
    another file processed), and nothing otherwise (no file processed, exit
    3, or no run id); `fail_if_spark_failed` keeps a run with a failed Spark
    application failed.

Never build the run id into a templated string (`@task.bash`, an f-string in
`application_args`...): Jinja would render it, which reopens template
injection from a manual run id. This test is the one that must catch it (the
`{{ 7*7 }}` assertion).

P2-5: OpenLineage is off for every run (the developer's shell variables are
dropped), except in the lineage test, which turns it on with a `file`
transport under pytest's tmp dir and checks that spark-submit receives the
listener, its jar, its transport and the parent job info of the real task
instance, and that the Airflow event of `spark_read_files` is that parent.

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

    dag_file, run_id, result_file, chain_second_run, file_list_db, statuses = sys.argv[1:7]

    # P2-4 / C-2: publish_lake_staging reads file_list through the
    # `file_list_db` Connection (Postgres). Stand-in: a SQLite file_list with
    # the statuses the job would have written for this run id, behind the
    # same engine builder (dag.test() runs the tasks in this process).
    from sqlalchemy import create_engine, text

    import utils.functions_utils as functions_utils

    engine = create_engine(f"sqlite:///{file_list_db}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE file_list (id INTEGER, dag_run_id TEXT, status TEXT)"))
        for i, status in enumerate(json.loads(statuses)):
            conn.execute(
                text("INSERT INTO file_list VALUES (:i, :run_id, :status)"),
                {"i": i, "run_id": run_id, "status": status},
            )
    functions_utils._file_list_engine_from_airflow_connection = lambda: engine

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
    first_publish_state = str(first.get_task_instance("publish_lake_staging").state)
    first_guard_state = str(first.get_task_instance("fail_if_spark_failed").state)
    second = dag.test() if chain_second_run == "1" else None

    # P2-4: `lake_staging` events actually emitted by the first run.
    from airflow.models.asset import AssetEvent
    from airflow.utils.session import create_session

    with create_session() as session:
        lake_staging_events = [
            e.source_task_id
            for e in session.query(AssetEvent).filter_by(
                source_dag_id="read_file_dag", source_run_id=first.run_id
            )
            if e.asset.name == "lake_staging"
        ]

    with open(result_file, "w") as f:
        json.dump(
            {
                "first": str(first.state),
                "first_spark_tis": first_spark_tis,
                "first_publish": first_publish_state,
                "first_guard": first_guard_state,
                "first_lake_staging_events": lake_staging_events,
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
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AIRFLOW", "OPENLINEAGE"))}
    env.update(
        AIRFLOW_HOME=str(home),
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{home / 'airflow.db'}",
        AIRFLOW__CORE__DAGS_FOLDER=str(DAGS_FOLDER),
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__CORE__UNIT_TEST_MODE="True",
        # P2-5: never any lineage event, unless a test turns it on.
        AIRFLOW__OPENLINEAGE__DISABLED="True",
        AIRFLOW__COSMOS__ENABLE_TELEMETRY="False",
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


def _run_dag_test(
    airflow_env,
    fake_spark_submit,
    tmp_path,
    run_id,
    *,
    exit_code,
    chain_second,
    statuses,
    extra_env=None,
):
    """Run DAG_TEST_SCRIPT in an isolated Airflow; return (states, logs).
    `statuses`: file_list statuses of the run id's files after the job."""
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
    env.update(extra_env or {})

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            DAG_TEST_SCRIPT,
            str(READ_DAG_FILE),
            run_id,
            str(result_file),
            "1" if chain_second else "0",
            str(tmp_path / "file_list.db"),
            json.dumps(statuses),
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
        airflow_env,
        fake_spark_submit,
        tmp_path,
        hostile,
        exit_code=0,
        chain_second=True,
        statuses=["processed"],
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
    # P2-4: every Spark application succeeded, so `lake_staging` is emitted,
    # once, by the final task; the failure guard is skipped.
    assert states["first_publish"] == "success", logs
    assert states["first_lake_staging_events"] == ["publish_lake_staging"], logs
    assert states["first_guard"] == "skipped", logs


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
        statuses=[],
    )

    calls = [json.loads(line) for line in calls_file.read_text().splitlines()]
    assert len(calls) == 1, calls  # retries=0: one attempt
    assert calls[0]["argv"][-1] == "--run-id=manual__p1_5_exit_3"
    assert states["first_spark_tis"] == [[0, "spark", "failed"]], logs
    assert states["first"] == "failed", logs
    # P2-4 / C-2: exit 3 = no file_list row, so nothing processed: no
    # `lake_staging` event, and the guard keeps the run failed.
    assert states["first_publish"] == "skipped", logs
    assert states["first_lake_staging_events"] == [], logs
    assert states["first_guard"] == "failed", logs


@pytest.mark.parametrize(
    ("statuses", "publish", "events"),
    [
        (["processed", "failed"], "success", ["publish_lake_staging"]),
        (["failed", "failed"], "skipped", []),
    ],
    ids=["one_file_processed", "no_file_processed"],
)
def test_p2_4_c2_read_file_dag_e2e_exit_1_publishes_only_processed_files(
    airflow_env, fake_spark_submit, tmp_path, statuses, publish, events
):
    """C-2: exit 1 = at least one file failed. With `all_success`, one file
    failing for good blocked `lake_staging`, hence dbt, forever. Now the
    event is emitted when another file of the run was processed, and not
    otherwise; in both cases the run itself stays failed (the `all_done`
    publishing leaf must not turn it green)."""
    states, logs = _run_dag_test(
        airflow_env,
        fake_spark_submit,
        tmp_path,
        "manual__p2_4_exit_1",
        exit_code=1,
        chain_second=False,
        statuses=statuses,
    )

    assert states["first_spark_tis"] == [[0, "spark", "failed"]], logs
    assert states["first_publish"] == publish, logs
    assert states["first_lake_staging_events"] == events, logs
    assert states["first_guard"] == "failed", logs
    assert states["first"] == "failed", logs


def _confs(argv: list[str]) -> dict[str, str]:
    values = [argv[i + 1] for i, arg in enumerate(argv) if arg == "--conf"]
    return dict(value.split("=", 1) for value in values)


def test_p2_5_read_file_dag_e2e_without_lineage_passes_no_openlineage_conf(
    airflow_env, fake_spark_submit, tmp_path
):
    """Lineage off (the default of this module): spark-submit gets no
    listener and no `spark.openlineage.*` property, as before P2-5."""
    _, calls_file = fake_spark_submit
    states, logs = _run_dag_test(
        airflow_env,
        fake_spark_submit,
        tmp_path,
        "manual__p2_5_off",
        exit_code=0,
        chain_second=False,
        statuses=["processed"],
    )
    assert states["first"] == "success", logs
    [call] = [json.loads(line) for line in calls_file.read_text().splitlines()]
    assert not [arg for arg in call["argv"] if "openlineage" in arg.lower()], call["argv"]


def test_p2_5_read_file_dag_e2e_spark_run_is_attached_to_its_task(
    airflow_env, fake_spark_submit, tmp_path
):
    """Lineage on (`file` transport, a listener jar): the real operator,
    with the real task instance, passes the listener, the jar, the transport
    and the parent job info; the parent run id is the run id of the Airflow
    event of `spark_read_files`, whose input is `file_list`."""
    _, calls_file = fake_spark_submit
    events_dir = tmp_path / "lineage" / "events"
    events_dir.mkdir(parents=True)
    from tests.unit.dags.lineage_jar import make_listener_jar, sha256

    jar = make_listener_jar(tmp_path / "openlineage-spark_2.13-test.jar")
    extra_env = {
        "AIRFLOW__OPENLINEAGE__DISABLED": "False",
        "AIRFLOW__OPENLINEAGE__TRANSPORT": json.dumps(
            {"type": "file", "log_file_path": str(events_dir / "airflow"), "append": False}
        ),
        "AIRFLOW__OPENLINEAGE__NAMESPACE": "p2_5_e2e",
        "OPENLINEAGE_SPARK_JAR": str(jar),
        "OPENLINEAGE_SPARK_SHA256": sha256(jar),
        # Only read for the dataset name of file_list (host, port, database).
        "AIRFLOW_CONN_FILE_LIST_DB": json.dumps(
            {"conn_type": "postgres", "host": "pg.example", "port": 5434, "schema": "files"}
        ),
    }

    states, logs = _run_dag_test(
        airflow_env,
        fake_spark_submit,
        tmp_path,
        "manual__p2_5_on",
        exit_code=0,
        chain_second=False,
        statuses=["processed"],
        extra_env=extra_env,
    )

    assert states["first"] == "success", logs
    [call] = [json.loads(line) for line in calls_file.read_text().splitlines()]
    confs = _confs(call["argv"])
    assert confs["spark.extraListeners"] == "io.openlineage.spark.agent.OpenLineageSparkListener"
    assert confs["spark.jars"] == str(jar)
    assert confs["spark.openlineage.transport.type"] == "file"
    # C-13: one event file per Spark application: read_file_dag's own run
    # (a manual dag.test() run), map index 0, try 1.
    location = Path(confs["spark.openlineage.transport.location"])
    assert location.parent == events_dir
    assert location.name.startswith("spark-read_file_dag-manual__")
    assert location.name.endswith("-0-1.jsonl")
    assert confs["spark.openlineage.namespace"] == "p2_5_e2e"
    assert confs["spark.openlineage.parentJobNamespace"] == "p2_5_e2e"
    assert confs["spark.openlineage.parentJobName"] == "read_file_dag.spark_read_files"
    assert confs["spark.openlineage.rootParentJobName"] == "read_file_dag"
    assert call["argv"][-1] == "--run-id=manual__p2_5_on"

    events = [json.loads(p.read_text()) for p in sorted(events_dir.glob("airflow-*.json"))]
    task_events = [
        e
        for e in events
        if e["job"]["name"] == "read_file_dag.spark_read_files" and e["eventType"] == "COMPLETE"
    ]
    assert task_events, [(e["eventType"], e["job"]["name"]) for e in events]
    [complete] = task_events
    # The Spark run's parent is this very task run, under its DAG run.
    assert complete["run"]["runId"] == confs["spark.openlineage.parentRunId"]
    root = complete["run"]["facets"]["parent"]["root"]["run"]["runId"]
    assert root == confs["spark.openlineage.rootParentRunId"]
    # pre_execute declared file_list as the task's input.
    inputs = {(d["namespace"], d["name"]) for d in complete["inputs"]}
    assert inputs == {("postgres://pg.example:5434", "files.public.file_list")}


def test_p2_5_sec2_read_file_dag_e2e_wrong_jar_digest_runs_without_lineage(
    airflow_env, fake_spark_submit, tmp_path
):
    """The jar is present and a zip with the listener, but not the pinned
    one: the real pre_execute strips the listener, the job runs as without
    lineage and the run succeeds."""
    from tests.unit.dags.lineage_jar import make_listener_jar

    _, calls_file = fake_spark_submit
    events_dir = tmp_path / "lineage" / "events"
    events_dir.mkdir(parents=True)
    jar = make_listener_jar(tmp_path / "openlineage-spark_2.13-other.jar")
    extra_env = {
        "AIRFLOW__OPENLINEAGE__DISABLED": "False",
        "AIRFLOW__OPENLINEAGE__TRANSPORT": json.dumps(
            {"type": "file", "log_file_path": str(events_dir / "airflow"), "append": False}
        ),
        "OPENLINEAGE_SPARK_JAR": str(jar),
        "OPENLINEAGE_SPARK_SHA256": "0" * 64,
    }
    states, logs = _run_dag_test(
        airflow_env,
        fake_spark_submit,
        tmp_path,
        "manual__p2_5_bad_jar",
        exit_code=0,
        chain_second=False,
        statuses=["processed"],
        extra_env=extra_env,
    )
    assert states["first"] == "success", logs
    [call] = [json.loads(line) for line in calls_file.read_text().splitlines()]
    assert not [a for a in call["argv"] if "openlineage" in a.lower() or a.startswith("spark.jars")]
