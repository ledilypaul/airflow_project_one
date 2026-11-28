"""
P1-4 / C-2, P1-5, P5-3 — end-to-end: the `ingest__<source>` DAGs really executed with
`dag.test()`, on the example sources of examples/config.

A run is started with a hostile `run_id` (what the REST API accepts as `dag_run_id`
when `[scheduler] allowed_run_id_pattern` is relaxed; its default refuses it).
A fake `spark-submit` (first on PATH) records its argv and its environment, and
stands in for the job by writing the `file_list` statuses it is told to. The test
proves, on the real operator stack (TaskFlow context injection, XCom of
`insert_files` into `application_args`, SparkSubmitOperator, SparkSubmitHook, the
Spark Connection, a SQLite `file_list`), that:
  - the run succeeds, with one `spark_read_files` task, in the `spark` pool;
  - nothing from the run id is executed by a shell (no `P*` file created);
  - spark-submit receives, at the end of its argv, exactly
    `<JOB_PATH> --run-id=<hostile value> --source-name=<source>` with `{{ 7*7 }}` NOT
    rendered by Jinja: `application_args` is a templated field of SparkSubmitOperator,
    but a value coming from an XCom must be skipped by the renderer;
  - the master is the one of the test `spark_default` Connection (not the
    hook's silent `yarn` fallback when the Connection is missing);
  - the worker environment reaches spark-submit, extended with `env_vars`
    (PYTHONPATH, and P5-3: the engine default and the config root the DAG parsed);
  - P1-5 / C-4: a non-zero exit code of spark-submit (3 = no file_list row
    for the run id) fails `spark_read_files` and the DAG run;
  - P2-4 / C-2: `publish_lake_staging` emits `lake_staging__<source>` when at
    least one file of the run is `processed` (Spark succeeded, or exit 1 with
    another file processed), and nothing otherwise; `fail_if_spark_failed`
    keeps a run with a failed Spark application failed;
  - P2-6: with the DAG's `retries=1` kept (retried at once), exit 6 (quality
    failure only) is tried once, exit 1 twice; with another file processed,
    `lake_staging__<source>` is still emitted and the run stays failed;
  - P5-3: no file listed = a green run with the Spark task skipped; the other example
    source and `ingest__unmatched` run the same way, each with its own source name.

Never build the run id into a templated string (`@task.bash`, an f-string in
`application_args`...): Jinja would render it, which reopens template
injection from a run id. This test is the one that must catch it (the
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
FACTORY_FILE = DAGS_FOLDER / "source_pipelines.py"
JOB_PATH = PROJECT_ROOT / "pipeline" / "jobs" / "read_files_job.py"
EXAMPLES_CONFIG_ROOT = PROJECT_ROOT / "examples" / "config"
EXAMPLES_DATA = PROJECT_ROOT / "examples" / "data"

pytestmark = pytest.mark.skipif(os.name != "posix", reason="needs a POSIX PATH")

# Master of the test Spark Connection: deliberately not `local[*]` (the
# documented dev value) nor `yarn` (the hook's fallback when the Connection
# is missing), so the assertion proves where the master came from.
TEST_SPARK_MASTER = "local[2]"
ORDERS_ROW = "order_id,customer,amount,order_date\n1,a,1.5,2026-01-01\n"

# Runs in the subprocess: `dag.test()` of one ingest DAG under a chosen run id.
# `file_list` is a SQLite stand-in behind the engine builder of the insertion task and
# of `publish_lake_staging` (dag.test() runs the tasks in this process); the fake
# spark-submit writes the statuses into it, as the job would.
DAG_TEST_SCRIPT = textwrap.dedent(
    """
    import json
    import sys
    from datetime import timedelta

    import airflow.models.dagrun as dagrun_module
    from airflow.models import DagBag
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from sqlalchemy import create_engine, text

    import utils.functions_utils as functions_utils

    dag_file, dag_id, run_id, result_file, file_list_db, keep_spark_retries = sys.argv[1:7]
    keep_spark_retries = keep_spark_retries == "1"

    engine = create_engine(f"sqlite:///{file_list_db}")
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE file_list (id INTEGER PRIMARY KEY, file_name TEXT, file_path TEXT, "
                "last_modified TEXT, status TEXT, dag_run_id TEXT, ingested_at TEXT, "
                "source_name TEXT NOT NULL DEFAULT '')"
            )
        )
        conn.execute(
            text("CREATE UNIQUE INDEX i ON file_list (file_path, dag_run_id, source_name)")
        )
    PostgresHook.get_sqlalchemy_engine = lambda self, *args, **kwargs: engine
    functions_utils._file_list_engine_from_airflow_connection = lambda: engine

    # dag.test() names its run itself (`manual__<date>`): give it the one under test.
    original = dagrun_module.get_or_create_dagrun
    dagrun_module.get_or_create_dagrun = lambda **kw: original(**{**kw, "run_id": run_id})

    bag = DagBag(dag_folder=dag_file)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags[dag_id]
    # dag.test() waits for retry_delay (5 min) before a retry: a regression
    # must fail fast instead. Only the retry policy is changed.
    for task in dag.tasks:
        if keep_spark_retries and task.task_id == "spark_read_files":
            # P2-6: keep the DAG's retries, retried at once.
            assert task.retries == 1, task.retries
            task.retry_delay = timedelta(0)
        else:
            task.retries = 0

    first = dag.test()
    tis = {ti.task_id: ti for ti in first.get_task_instances()}
    spark = tis["spark_read_files"]

    # P2-4: `lake_staging__<source>` events actually emitted by the run.
    from airflow.models.asset import AssetEvent
    from airflow.utils.session import create_session

    with create_session() as session:
        lake_staging_events = [
            [e.source_task_id, e.asset.name]
            for e in session.query(AssetEvent).filter_by(
                source_dag_id=dag_id, source_run_id=first.run_id
            )
        ]
    with engine.connect() as conn:
        rows = [
            list(r) for r in conn.execute(
                text("SELECT file_name, status, source_name, dag_run_id FROM file_list ORDER BY id")
            )
        ]

    with open(result_file, "w") as f:
        json.dump(
            {
                "state": str(first.state),
                "run_id": first.run_id,
                "spark_ti": [spark.map_index, spark.pool, str(spark.state)],
                "spark_tries": spark.try_number,
                "states": {task_id: str(ti.state) for task_id, ti in tis.items()},
                "lake_staging_events": lake_staging_events,
                "file_list": rows,
            },
            f,
        )
    """
)


@pytest.fixture(scope="module")
def airflow_env(tmp_path_factory):
    """Environment of an isolated Airflow with a migrated SQLite metadata DB."""
    home = tmp_path_factory.mktemp("airflow_home")
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("AIRFLOW", "OPENLINEAGE", "PIPELINE_"))
    }
    env.update(
        AIRFLOW_HOME=str(home),
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{home / 'airflow.db'}",
        AIRFLOW__CORE__DAGS_FOLDER=str(DAGS_FOLDER),
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__CORE__UNIT_TEST_MODE="True",
        # P2-5: never any lineage event, unless a test turns it on.
        AIRFLOW__OPENLINEAGE__DISABLED="True",
        # P3-3: nor any metric.
        AIRFLOW__METRICS__OTEL_ON="False",
        AIRFLOW__COSMOS__ENABLE_TELEMETRY="False",
        # Airflow's default `[scheduler] allowed_run_id_pattern` already refuses the
        # hostile run ids below. An admin can relax it (and the API takes any
        # `dag_run_id` it allows): the argv must stay safe on its own.
        AIRFLOW__SCHEDULER__ALLOWED_RUN_ID_PATTERN=".*",
        # P5-3: the example sources, whose DAGs these tests run.
        PIPELINE_CONFIG_ROOT=str(EXAMPLES_CONFIG_ROOT),
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
    """A `spark-submit` (one of the binary names the hook accepts) that appends its
    argv, PYTHONPATH and the P5-3 variables, one JSON object per call, writes the
    `file_list` statuses of `FAKE_STATUSES` (one per row, in order), then exits with
    `FAKE_SPARK_SUBMIT_EXIT_CODE` (default 0). It reads them from the environment: if
    the hook replaced the worker environment by `env_vars` instead of extending it, it
    would crash."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls_file = tmp_path / "spark_submit_calls.jsonl"
    stub = bin_dir / "spark-submit"
    stub.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sqlite3, sys\n"
        "with open(os.environ['FAKE_SPARK_SUBMIT_CALLS'], 'a') as f:\n"
        "    record = {'argv': sys.argv[1:], 'pythonpath': os.environ.get('PYTHONPATH'),\n"
        "              'engine': os.environ.get('PIPELINE_DEFAULT_ENGINE'),\n"
        "              'config_root': os.environ.get('PIPELINE_CONFIG_ROOT')}\n"
        "    f.write(json.dumps(record) + '\\n')\n"
        "con = sqlite3.connect(os.environ['FAKE_FILE_LIST_DB'])\n"
        "ids = [r[0] for r in con.execute('SELECT id FROM file_list ORDER BY id')]\n"
        "for i, status in zip(ids, json.loads(os.environ['FAKE_STATUSES'])):\n"
        "    con.execute('UPDATE file_list SET status = ? WHERE id = ?', (status, i))\n"
        "con.commit()\n"
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
    exit_code=0,
    statuses=("processed",),
    dag_id="ingest__orders",
    data_files=None,
    extra_env=None,
    keep_spark_retries=False,
):
    """Run DAG_TEST_SCRIPT in an isolated Airflow; return (result, logs).
    `statuses`: file_list statuses of the run's files after the job. `data_files`:
    {name: content} of the files of `./data/` (default: one `orders_<i>.csv` per
    status, at least one)."""
    bin_dir, calls_file = fake_spark_submit
    result_file = tmp_path / "result.json"
    if data_files is None:
        data_files = {f"orders_{i}.csv": ORDERS_ROW for i in range(max(1, len(statuses)))}
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    for name, content in data_files.items():
        (data_dir / name).write_text(content)

    env = dict(airflow_env)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"
    env["FAKE_SPARK_SUBMIT_CALLS"] = str(calls_file)
    env["FAKE_SPARK_SUBMIT_EXIT_CODE"] = str(exit_code)
    env["FAKE_FILE_LIST_DB"] = str(tmp_path / "file_list.db")
    env["FAKE_STATUSES"] = json.dumps(list(statuses))
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
            str(FACTORY_FILE),
            dag_id,
            run_id,
            str(result_file),
            str(tmp_path / "file_list.db"),
            "1" if keep_spark_retries else "0",
        ],
        env=env,
        # `./data/` of config/pipeline.yaml is relative to the worker's directory.
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=600,
    )
    logs = proc.stdout[-5000:] + proc.stderr[-5000:]
    # P3-3: the whole output, for assertions on a line that the tail above
    # may not keep.
    (tmp_path / "dag_test_output.log").write_text(proc.stdout + proc.stderr)
    assert proc.returncode == 0, logs
    return json.loads(result_file.read_text()), logs


def _calls(calls_file):
    return [json.loads(line) for line in calls_file.read_text().splitlines()]


def test_p1_4_ingest_dag_e2e_hostile_run_id_not_injected(airflow_env, fake_spark_submit, tmp_path):
    _, calls_file = fake_spark_submit
    # No `/`: the Execution API addresses a task instance by its run id in the URL path.
    # The DAG runs in `tmp_path` (cwd), where a shell would create these files.
    hostile = "x; touch P1 $(touch P2) `touch P3` {{ 7*7 }} && touch P4"

    states, logs = _run_dag_test(airflow_env, fake_spark_submit, tmp_path, hostile)

    assert states["state"] == "success", logs
    assert states["run_id"] == hostile
    # One application, in the dedicated pool (not mapped any more: index -1).
    assert states["spark_ti"] == [-1, "spark", "success"], logs
    assert [p.name for p in tmp_path.glob("P[0-9]")] == []
    [call] = _calls(calls_file)
    argv = call["argv"]
    # The hook puts its options (--master, --name...) first: the job and its
    # arguments must close the argv, untouched.
    assert argv[-3:] == [str(JOB_PATH), f"--run-id={hostile}", "--source-name=orders"]
    assert argv[-2].endswith("{{ 7*7 }} && touch P4")  # not rendered
    assert argv[argv.index("--master") + 1] == TEST_SPARK_MASTER
    assert "--deploy-mode" not in argv
    assert call["pythonpath"] == str(PROJECT_ROOT)
    # P5-3: the engine default and the config root this DAG parsed.
    assert call["engine"] == "spark"
    assert call["config_root"] == str(EXAMPLES_CONFIG_ROOT)
    # The row of the listing carries the run id and the source.
    assert [r[1:] for r in states["file_list"]] == [["processed", "orders", hostile]], logs
    # P2-4: the Spark application succeeded and a file is processed, so
    # `lake_staging__orders` is emitted, once, by the final task; the guard is skipped.
    assert states["states"]["publish_lake_staging"] == "success", logs
    assert states["lake_staging_events"] == [["publish_lake_staging", "lake_staging__orders"]]
    assert states["states"]["fail_if_spark_failed"] == "skipped", logs


def test_p5_3_ingest_dag_e2e_spark_job_gets_the_engine_default_of_the_parse(
    airflow_env, fake_spark_submit, tmp_path
):
    """`PIPELINE_DEFAULT_ENGINE` is passed to the job explicitly (it used to follow
    whatever environment the driver had). A source with its own engine does not
    change the default."""
    _, calls_file = fake_spark_submit
    states, logs = _run_dag_test(
        airflow_env,
        fake_spark_submit,
        tmp_path,
        "manual__p5_3_engine",
        extra_env={"PIPELINE_DEFAULT_ENGINE": "spark"},
    )
    assert states["state"] == "success", logs
    [call] = _calls(calls_file)
    assert call["engine"] == "spark"


@pytest.mark.parametrize(
    ("dag_id", "files", "source"),
    [
        ("ingest__orders", {"orders_sample.csv": None}, "orders"),
        ("ingest__imdb_movies", {"imdb_sample.csv": None}, "imdb_movies"),
        ("ingest__unmatched", {"notes_2026.csv": ORDERS_ROW}, "unmatched"),
    ],
    ids=["orders", "imdb_movies", "unmatched"],
)
def test_p5_3_ingest_dag_e2e_runs_on_the_examples(
    airflow_env, fake_spark_submit, tmp_path, dag_id, files, source
):
    """The acceptance run: each DAG of the example config lists its own files (the
    sample files of examples/data), records them with its source name, and submits one
    application for them. The unmatched DAG takes what no source claims, and has no
    Asset."""
    _, calls_file = fake_spark_submit
    data_files = {
        name: content if content is not None else (EXAMPLES_DATA / name).read_text()
        for name, content in files.items()
    }
    # Another source's file in the same directory: not this DAG's business (the
    # unmatched DAG: not the other sources').
    data_files.setdefault("orders_sample.csv", (EXAMPLES_DATA / "orders_sample.csv").read_text())
    data_files.setdefault("imdb_sample.csv", (EXAMPLES_DATA / "imdb_sample.csv").read_text())

    states, logs = _run_dag_test(
        airflow_env,
        fake_spark_submit,
        tmp_path,
        f"manual__p5_3_{source}",
        dag_id=dag_id,
        statuses=["processed"],
        data_files=data_files,
    )

    assert states["state"] == "success", logs
    [call] = _calls(calls_file)
    assert call["argv"][-2:] == [f"--run-id=manual__p5_3_{source}", f"--source-name={source}"]
    [(file_name, status, source_name, _)] = states["file_list"]
    assert (status, source_name) == ("processed", source)
    assert file_name == next(iter(files))
    events = states["lake_staging_events"]
    assert events == (
        [] if source == "unmatched" else [["publish_lake_staging", f"lake_staging__{source}"]]
    )


def test_p5_3_ingest_dag_e2e_without_file_is_a_green_run_that_reads_nothing(
    airflow_env, fake_spark_submit, tmp_path
):
    """Nothing to list: `insert_files` skips, and so does everything after it. No
    spark-submit, no event, no red run (P1-6)."""
    _, calls_file = fake_spark_submit

    states, logs = _run_dag_test(
        airflow_env, fake_spark_submit, tmp_path, "manual__p5_3_empty", data_files={}, statuses=[]
    )

    assert states["state"] == "success", logs
    assert states["states"]["list_files"] == "success", logs
    assert states["states"]["insert_files"] == "skipped", logs
    assert states["spark_ti"][2] == "skipped", logs
    assert states["states"]["publish_lake_staging"] == "skipped", logs
    assert states["states"]["fail_if_spark_failed"] == "skipped", logs
    assert states["lake_staging_events"] == []
    assert not calls_file.exists()


def test_p1_5_ingest_dag_e2e_nonzero_spark_submit_exit_fails_task_and_run(
    airflow_env, fake_spark_submit, tmp_path
):
    """C-4: the contract the job's exit codes rely on. spark-submit exiting 3
    (what the job returns when the run id has no file_list row, propagated by
    spark-submit in client mode) must end `spark_read_files` in `failed`, and
    the DAG run too. Never a green run on a failed Spark application."""
    _, calls_file = fake_spark_submit

    states, logs = _run_dag_test(
        airflow_env, fake_spark_submit, tmp_path, "manual__p1_5_exit_3", exit_code=3, statuses=[]
    )

    [call] = _calls(calls_file)  # retries=0: one attempt
    assert call["argv"][-2:] == ["--run-id=manual__p1_5_exit_3", "--source-name=orders"]
    assert states["spark_ti"] == [-1, "spark", "failed"], logs
    assert states["state"] == "failed", logs
    # P2-4 / C-2: nothing processed: no `lake_staging__orders` event, and the guard
    # keeps the run failed.
    assert states["states"]["publish_lake_staging"] == "skipped", logs
    assert states["lake_staging_events"] == []
    assert states["states"]["fail_if_spark_failed"] == "failed", logs


@pytest.mark.parametrize(
    ("statuses", "publish", "events"),
    [
        (["processed", "failed"], "success", [["publish_lake_staging", "lake_staging__orders"]]),
        (["failed", "failed"], "skipped", []),
    ],
    ids=["one_file_processed", "no_file_processed"],
)
def test_p2_4_c2_ingest_dag_e2e_exit_1_publishes_only_processed_files(
    airflow_env, fake_spark_submit, tmp_path, statuses, publish, events
):
    """C-2: exit 1 = at least one file failed. With `all_success`, one file
    failing for good blocked `lake_staging__<source>`, hence dbt, forever. Now the
    event is emitted when another file of the run was processed, and not
    otherwise; in both cases the run itself stays failed (the `all_done`
    publishing leaf must not turn it green)."""
    states, logs = _run_dag_test(
        airflow_env,
        fake_spark_submit,
        tmp_path,
        f"manual__p2_4_exit_1_{publish}",  # a run id is unique in the module's metadata DB
        exit_code=1,
        statuses=statuses,
    )

    assert states["spark_ti"] == [-1, "spark", "failed"], logs
    assert states["states"]["publish_lake_staging"] == publish, logs
    assert states["lake_staging_events"] == events, logs
    assert states["states"]["fail_if_spark_failed"] == "failed", logs
    assert states["state"] == "failed", logs


def _confs(argv: list[str]) -> dict[str, str]:
    values = [argv[i + 1] for i, arg in enumerate(argv) if arg == "--conf"]
    return dict(value.split("=", 1) for value in values)


def test_p2_5_ingest_dag_e2e_without_lineage_passes_no_openlineage_conf(
    airflow_env, fake_spark_submit, tmp_path
):
    """Lineage off (the default of this module): spark-submit gets no
    listener and no `spark.openlineage.*` property, as before P2-5."""
    _, calls_file = fake_spark_submit
    states, logs = _run_dag_test(airflow_env, fake_spark_submit, tmp_path, "manual__p2_5_off")
    assert states["state"] == "success", logs
    [call] = _calls(calls_file)
    assert not [arg for arg in call["argv"] if "openlineage" in arg.lower()], call["argv"]


def test_p2_5_ingest_dag_e2e_spark_run_is_attached_to_its_task(
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
        airflow_env, fake_spark_submit, tmp_path, "manual__p2_5_on", extra_env=extra_env
    )

    assert states["state"] == "success", logs
    [call] = _calls(calls_file)
    confs = _confs(call["argv"])
    assert confs["spark.extraListeners"] == "io.openlineage.spark.agent.OpenLineageSparkListener"
    assert confs["spark.jars"] == str(jar)
    assert confs["spark.openlineage.transport.type"] == "file"
    # C-13: one event file per Spark application: the ingest DAG's own run (a
    # dag.test() run), not mapped (index -1), try 1.
    location = Path(confs["spark.openlineage.transport.location"])
    assert location.parent == events_dir
    assert location.name.startswith("spark-ingest__orders-manual__")
    assert location.name.endswith("--1-1.jsonl")
    assert confs["spark.openlineage.namespace"] == "p2_5_e2e"
    assert confs["spark.openlineage.parentJobNamespace"] == "p2_5_e2e"
    assert confs["spark.openlineage.parentJobName"] == "ingest__orders.spark_read_files"
    assert confs["spark.openlineage.rootParentJobName"] == "ingest__orders"
    assert call["argv"][-2:] == ["--run-id=manual__p2_5_on", "--source-name=orders"]

    events = [json.loads(p.read_text()) for p in sorted(events_dir.glob("airflow-*.json"))]
    task_events = [
        e
        for e in events
        if e["job"]["name"] == "ingest__orders.spark_read_files" and e["eventType"] == "COMPLETE"
    ]
    assert task_events, [(e["eventType"], e["job"]["name"]) for e in events]
    [complete] = task_events
    # The Spark run's parent is this very task run, under its DAG run.
    assert complete["run"]["runId"] == confs["spark.openlineage.parentRunId"]
    root = complete["run"]["facets"]["parent"]["root"]["run"]["runId"]
    assert root == confs["spark.openlineage.rootParentRunId"]
    # pre_execute declared file_list as the task's input. (dag.test() runs every task in
    # one process, whose hook lineage collector keeps the source files `insert_files`
    # declared: a worker runs each task in its own process.)
    inputs = {(d["namespace"], d["name"]) for d in complete["inputs"]}
    assert ("postgres://pg.example:5434", "files.public.file_list") in inputs


def test_p2_5_sec2_ingest_dag_e2e_wrong_jar_digest_runs_without_lineage(
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
        airflow_env, fake_spark_submit, tmp_path, "manual__p2_5_bad_jar", extra_env=extra_env
    )
    assert states["state"] == "success", logs
    [call] = _calls(calls_file)
    assert not [a for a in call["argv"] if "openlineage" in a.lower() or a.startswith("spark.jars")]


# ---------------------------------------------------------------------------
# P2-6 — no retry for a deterministic failure
# ---------------------------------------------------------------------------


def test_p2_6_ingest_dag_e2e_quality_exit_6_is_not_retried(
    airflow_env, fake_spark_submit, tmp_path
):
    """Exit 6 (every failed file failed on blocking quality checks only):
    one attempt although spark_read_files keeps `retries=1`. Another file of
    the run is `processed`: `lake_staging__orders` is emitted, and the run stays
    failed (`fail_if_spark_failed`)."""
    _, calls_file = fake_spark_submit

    states, logs = _run_dag_test(
        airflow_env,
        fake_spark_submit,
        tmp_path,
        "manual__p2_6_exit_6",
        exit_code=6,
        statuses=["processed", "failed"],
        keep_spark_retries=True,
    )

    assert len(_calls(calls_file)) == 1
    assert states["spark_ti"] == [-1, "spark", "failed"], logs
    # retries=1 kept, yet a single try: AirflowFailException (its message is
    # checked by tests/unit/dags/test_p2_6_no_retry_exit_codes.py).
    assert states["spark_tries"] == 1, logs
    assert states["states"]["publish_lake_staging"] == "success", logs
    assert states["lake_staging_events"] == [["publish_lake_staging", "lake_staging__orders"]]
    assert states["states"]["fail_if_spark_failed"] == "failed", logs
    assert states["state"] == "failed", logs


def test_p2_6_ingest_dag_e2e_exit_1_is_still_retried(airflow_env, fake_spark_submit, tmp_path):
    """Exit 1 (a file failed for another reason): the retry still happens,
    same setup as the test above."""
    _, calls_file = fake_spark_submit

    states, logs = _run_dag_test(
        airflow_env,
        fake_spark_submit,
        tmp_path,
        "manual__p2_6_exit_1",
        exit_code=1,
        statuses=["processed", "failed"],
        keep_spark_retries=True,
    )

    calls = _calls(calls_file)
    assert len(calls) == 2, calls
    assert [c["argv"][-2] for c in calls] == ["--run-id=manual__p2_6_exit_1"] * 2
    assert states["spark_ti"] == [-1, "spark", "failed"], logs
    assert states["spark_tries"] == 2, logs
    assert states["states"]["publish_lake_staging"] == "success", logs
    assert states["states"]["fail_if_spark_failed"] == "failed", logs
    assert states["state"] == "failed", logs


# ---------------------------------------------------------------------------
# P3-3 - the failure of spark_read_files is reported with the job's exit code
# ---------------------------------------------------------------------------


def _failure_lines(tmp_path):
    """The `airflow_failure` records (utils/failure_alert.py) of the run. The
    log renderer appends its own suffix after the JSON: decode the first
    JSON value only."""
    marker = "airflow_failure "
    records = []
    for line in (tmp_path / "dag_test_output.log").read_text().splitlines():
        if marker + "{" in line:
            start = line.index(marker + "{") + len(marker)
            record, _ = json.JSONDecoder().raw_decode(line[start:])
            records.append(record)
    return records


@pytest.mark.parametrize(
    ("exit_code", "attempts", "meaning"),
    [(6, 1, "quality"), (1, 2, "at least one file failed")],
    ids=["exit-6-no-retry", "exit-1-after-retry"],
)
def test_p3_3_ingest_dag_e2e_failure_line_names_the_spark_exit_code(
    airflow_env, fake_spark_submit, tmp_path, exit_code, attempts, meaning
):
    """One line for the task, on its final failure only (exit 1: after the
    retry, not on each attempt), with the exit code of spark-submit and what
    it means. The sentinel task `fail_if_spark_failed` does not report a
    second time."""
    states, logs = _run_dag_test(
        airflow_env,
        fake_spark_submit,
        tmp_path,
        f"manual__p3_3_exit_{exit_code}",
        exit_code=exit_code,
        statuses=["processed", "failed"],
        keep_spark_retries=True,
    )

    assert states["spark_tries"] == attempts, logs
    task_lines = [r for r in _failure_lines(tmp_path) if r["scope"] == "task"]
    assert len(task_lines) == 1, task_lines
    (record,) = task_lines
    assert record["dag_id"] == "ingest__orders"
    assert record["run_id"], record
    assert record["task_id"] == "spark_read_files"
    assert "map_index" not in record  # not mapped any more (P5-3)
    assert record["try_number"] == attempts
    assert record["spark_exit_code"] == exit_code
    assert meaning in record["spark_exit_code_meaning"]
    # C-2: the exception's type only, never its text (the task log has it,
    # masked by Airflow).
    # exit 6 is never retried (AirflowFailException), exit 1 is (AirflowException).
    assert record["exception_type"] == (
        "AirflowFailException" if exit_code == 6 else "AirflowException"
    )
    assert "exception" not in record, record
    # C-7: the run reports once as well, with the reason the scheduler gives;
    # the sentinel task (`fail_if_spark_failed`) reports nothing by itself.
    run_lines = [r for r in _failure_lines(tmp_path) if r["scope"] == "dag_run"]
    assert len(run_lines) == 1, run_lines
    assert run_lines[0]["dag_id"] == "ingest__orders"
    assert run_lines[0]["run_id"] == record["run_id"]
    assert run_lines[0]["reason"] == "task_failure"
    assert not [r for r in _failure_lines(tmp_path) if r.get("task_id") == "fail_if_spark_failed"]
