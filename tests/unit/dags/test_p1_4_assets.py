"""
P1-4 — Assets (audit A1): list_files_dag produces the `file_list` Asset and
read_file_dag is scheduled on it, instead of being triggered by hand.

Covers:
  - DAG files parse without the project root on sys.path (C-1): each DAG file
    declares the Asset itself, matched by Airflow on its *name*;
  - the producers' outlets and read_file_dag's Asset schedule, task topology,
    `max_active_runs`, optional `run_id` Param and Spark task environment
    (P1-5: `env_vars` of SparkSubmitOperator, merged into the worker's);
  - resolve_source_run_ids: run ids taken from *all* triggering asset events
    (deduplicated, order kept), manual `dag_run.conf["run_id"]` fallback
    (stripped, read from the conf and not from the memoised Param: C-3, C-5),
    explicit failure when there is no scope at all;
  - argparse safety of the spark-submit argv (C-4). P1-5: there is no shell
    any more, the argv is built by the installed SparkSubmitHook; this
    real-process round trip is the reference test for "the run id reaches the
    job's parser intact" (P1-5 / C-5: duplicates removed).

The end-to-end "no shell injection / no Jinja rendering" guarantee is covered
by tests/integration/test_p1_4_read_file_dag_e2e.py (C-2).
"""

import copy
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.models import DagBag
from airflow.sdk import Asset
from airflow.sdk.exceptions import AirflowFailException, ParamValidationError

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / "dags"

FILE_LIST_ASSET_NAME = "file_list"
PROJECT_DAG_IDS = {"list_files_dag", "list_files_dag_classic", "read_file_dag", "test_dag"}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_FOLDER))


@pytest.fixture
def spark_connection(monkeypatch):
    """P1-5 / C-2: resolve_source_run_ids fails when the `spark_default`
    Connection is missing (test_p1_5_spark_submit.py). Its tests here are
    about run-id resolution, so they get one (env var form, no DB)."""
    monkeypatch.setenv(
        "AIRFLOW_CONN_SPARK_DEFAULT", json.dumps({"conn_type": "spark", "host": "local[*]"})
    )


def _resolve_callable():
    """Run with the `spark_connection` fixture (applied to the whole
    resolve_source_run_ids section below)."""
    from dags.local_read_file_dag import dag_instance

    return dag_instance.get_task("resolve_source_run_ids").python_callable


def _dag_run(conf):
    """Stand-in for the Task SDK `dag_run` context object: only `.conf` is read."""
    return SimpleNamespace(conf=conf)


def _events(*source_run_ids: str | None, asset_name: str = FILE_LIST_ASSET_NAME):
    """Build a real TriggeringAssetEventsAccessor, as the Task SDK does from
    dag_run.consumed_asset_events (airflow/sdk/execution_time/task_runner.py)."""
    from airflow.sdk.execution_time.comms import AssetEventDagRunReferenceResult
    from airflow.sdk.execution_time.context import TriggeringAssetEventsAccessor

    asset = Asset(name=asset_name)
    events = [
        AssetEventDagRunReferenceResult(
            asset={"name": asset.name, "uri": asset.uri, "extra": {}},
            extra={},
            source_task_id="insert_files",
            source_dag_id="list_files_dag",
            source_run_id=run_id,
            source_map_index=-1,
            source_aliases=[],
            timestamp=datetime(2026, 11, 12, i, 0, tzinfo=UTC),
        )
        for i, run_id in enumerate(source_run_ids)
    ]
    return TriggeringAssetEventsAccessor.build(events)


# ---------------------------------------------------------------------------
# C-1 — DAG files must not depend on the project root being importable
# ---------------------------------------------------------------------------


def test_p1_4_dags_parse_without_project_root_on_sys_path(monkeypatch):
    """C-1 regression: the DAG processor only puts the dags folder on sys.path.
    In a non-editable image (the wheel ships pipeline/, utils/, storage/ but
    not dags/) or with git-sync, `from dags.assets import ...` raised
    ModuleNotFoundError at parse time and all three DAGs disappeared.

    The project root is removed from sys.path and every project package is
    evicted from sys.modules, so neither `dags.*` nor business code
    (`pipeline.*`, `utils.*`, `storage.*`, imported inside the tasks only)
    can be resolved while parsing."""
    import importlib

    project_root = PROJECT_ROOT.resolve()
    kept = [p for p in sys.path if Path(p or os.getcwd()).resolve() != project_root]
    # setattr (not syspath_prepend, whose undo would rewrite the restored list
    # with this truncated copy): the original sys.path object is put back.
    monkeypatch.setattr(sys, "path", [str(DAGS_FOLDER), *kept])
    for name in list(sys.modules):
        if name.split(".")[0] in {"dags", "pipeline", "utils", "storage"}:
            monkeypatch.delitem(sys.modules, name)
    importlib.invalidate_caches()
    for package in ("dags", "pipeline", "utils"):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(package)

    bag = DagBag(dag_folder=str(DAGS_FOLDER))

    assert bag.import_errors == {}, bag.import_errors
    assert set(bag.dag_ids) == PROJECT_DAG_IDS


def test_p1_4_file_list_asset_name_shared_by_producers_and_consumer(dagbag):
    """Each DAG file declares its own Asset object (C-1): Airflow matches
    producers and consumers by name/URI, not by object identity, so what must
    hold is that the names are the same everywhere."""
    producer_outlets = [
        dagbag.dags["list_files_dag"].get_task("insert_files").outlets,
        dagbag.dags["list_files_dag_classic"].get_task("insert_files_to_db").outlets,
    ]
    consumer_schedule = dagbag.dags["read_file_dag"].schedule

    for assets in [*producer_outlets, consumer_schedule]:
        assert [type(a) for a in assets] == [Asset]
        assert [a.name for a in assets] == [FILE_LIST_ASSET_NAME]
        # No URI scheme (e.g. postgres://) that a provider would try to validate.
        assert "://" not in assets[0].uri


@pytest.mark.parametrize(
    ("dag_id", "task_id"),
    [("list_files_dag", "list_files"), ("list_files_dag_classic", "list_files_task")],
)
def test_p1_4_listing_task_does_not_emit_the_asset(dagbag, dag_id, task_id):
    """Only the insertion task updates `file_list`: listing alone must not
    trigger read_file_dag."""
    assert dagbag.dags[dag_id].get_task(task_id).outlets == []


# ---------------------------------------------------------------------------
# Consumer: read_file_dag
# ---------------------------------------------------------------------------


def test_p1_4_read_file_dag_is_scheduled_on_file_list_asset(dagbag):
    from airflow.sdk.definitions.timetables.assets import AssetTriggeredTimetable

    dag = dagbag.dags["read_file_dag"]
    assert isinstance(dag.timetable, AssetTriggeredTimetable)
    assert [a.name for a in dag.timetable.asset_condition.objects] == [FILE_LIST_ASSET_NAME]


def test_p1_4_read_file_dag_task_ids(dagbag):
    """C-6: no intermediate `build_spark_commands` task any more."""
    assert set(dagbag.dags["read_file_dag"].task_ids) == {
        "resolve_source_run_ids",
        "spark_read_files",
    }


def test_p1_4_read_file_dag_dependencies_resolution_before_spark(dagbag):
    dag = dagbag.dags["read_file_dag"]
    assert dag.get_task("resolve_source_run_ids").upstream_task_ids == set()
    assert dag.get_task("spark_read_files").upstream_task_ids == {"resolve_source_run_ids"}


def test_p1_4_spark_task_is_mapped_over_the_run_ids(dagbag):
    """One spark_read_files instance per source run id (own exit code, retry
    and status), mapped through `.map(...)` on the output of
    resolve_source_run_ids (C-6: the arguments are built at expansion time on
    the worker that runs the task, not frozen in an XCom by an extra task).
    P1-5: mapped on `application_args` of SparkSubmitOperator (operator class
    and field are checked by test_p1_5_spark_submit.py)."""
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    from airflow.sdk.definitions.xcom_arg import MapXComArg

    task = dagbag.dags["read_file_dag"].get_task("spark_read_files")
    assert isinstance(task, MappedOperator)
    assert len(task.expand_input.value) == 1

    (mapped,) = task.expand_input.value.values()
    assert isinstance(mapped, MapXComArg)
    assert mapped.arg.operator.task_id == "resolve_source_run_ids"
    assert [f.__name__ for f in mapped.callables] == ["build_application_args"]


def test_p1_4_spark_task_inherits_worker_environment(dagbag, monkeypatch):
    """The spark-submit subprocess must get the *whole* worker environment
    (PATH, JAVA_HOME, SPARK_HOME, ENV...) plus PYTHONPATH pointing at the
    project root: replacing the environment instead of extending it was the
    P1-4 `append_env` bug.

    P1-5: `env_vars` of SparkSubmitOperator. Checked on the *installed*
    SparkSubmitHook with a local master: `submit()` must pass `os.environ`
    updated with `env_vars` to Popen, not `env_vars` alone."""
    from airflow.providers.apache.spark.hooks import spark_submit

    task = dagbag.dags["read_file_dag"].get_task("spark_read_files")
    env_vars = task.partial_kwargs["env_vars"]
    assert env_vars == {"PYTHONPATH": str(PROJECT_ROOT)}

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


def test_p1_4_read_file_dag_max_active_runs_is_1(dagbag):
    """C-7: two consumer runs must not process the same source run in
    parallel; events arriving during a run are batched into the next one,
    which resolve_source_run_ids already handles."""
    assert dagbag.dags["read_file_dag"].max_active_runs == 1


def test_p1_4_run_id_param_is_optional(dagbag):
    """Asset-triggered runs carry no conf: a required Param would block them
    (and Airflow refuses a schedule with a required Param without default).

    Works on a copy: Param.resolve() memoises its value on the object (C-5),
    and the `dagbag` fixture is module-scoped."""
    param = copy.deepcopy(dagbag.dags["read_file_dag"].params.get_param("run_id"))
    assert param.schema.get("type") == ["null", "string"]
    assert param.resolve() is None
    assert param.resolve("manual__2026-11-12T00:00:00+00:00") == (
        "manual__2026-11-12T00:00:00+00:00"
    )
    with pytest.raises(ParamValidationError):
        param.resolve(123)


# ---------------------------------------------------------------------------
# resolve_source_run_ids
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("spark_connection")
def test_p1_4_resolve_single_event_returns_its_source_run_id():
    result = _resolve_callable()(
        triggering_asset_events=_events("manual__run_a"), dag_run=_dag_run({})
    )
    assert result == ["manual__run_a"]


@pytest.mark.usefixtures("spark_connection")
def test_p1_4_resolve_multiple_events_deduplicated_in_order():
    """Airflow may batch several listings into one consumer run: every
    source run must be processed, once, in event order."""
    result = _resolve_callable()(
        triggering_asset_events=_events("run_b", "run_a", "run_b", "run_c", "run_a"),
        dag_run=_dag_run(None),
    )
    assert result == ["run_b", "run_a", "run_c"]


@pytest.mark.usefixtures("spark_connection")
def test_p1_4_resolve_ignores_events_of_other_assets():
    result = _resolve_callable()(
        triggering_asset_events=_events("run_other", asset_name="some_other_asset"),
        dag_run=_dag_run({"run_id": "manual_target"}),
    )
    assert result == ["manual_target"]


@pytest.mark.usefixtures("spark_connection")
def test_p1_4_resolve_events_take_precedence_over_conf():
    result = _resolve_callable()(
        triggering_asset_events=_events("run_a"), dag_run=_dag_run({"run_id": "ignored"})
    )
    assert result == ["run_a"]


@pytest.mark.usefixtures("spark_connection")
def test_p1_4_resolve_skips_events_without_source_run_id():
    """An event created through the REST API has no source run: it cannot be
    scoped, so it is ignored (the other events are still processed)."""
    result = _resolve_callable()(
        triggering_asset_events=_events(None, "run_a"), dag_run=_dag_run({})
    )
    assert result == ["run_a"]


@pytest.mark.usefixtures("spark_connection")
@pytest.mark.parametrize("events", [None, "empty"], ids=["no_accessor", "no_event"])
def test_p1_4_resolve_manual_trigger_uses_conf_run_id(events):
    accessor = _events() if events == "empty" else None
    result = _resolve_callable()(
        triggering_asset_events=accessor, dag_run=_dag_run({"run_id": "manual__run_x"})
    )
    assert result == ["manual__run_x"]


@pytest.mark.usefixtures("spark_connection")
def test_p1_4_resolve_manual_run_id_is_stripped():
    """C-3: `" manual__x\\n"` pasted in the trigger form must target
    `manual__x`, not a run id that matches no file_list row."""
    result = _resolve_callable()(
        triggering_asset_events=_events(), dag_run=_dag_run({"run_id": "  manual__x \n"})
    )
    assert result == ["manual__x"]


@pytest.mark.usefixtures("spark_connection")
@pytest.mark.parametrize(
    "conf",
    [None, {}, {"run_id": None}, {"run_id": ""}, {"run_id": "   "}, {"run_id": " None "}],
    ids=["no_conf", "empty_conf", "null", "empty", "blank", "literal_None"],
)
def test_p1_4_resolve_blank_conf_run_id_is_treated_as_missing(conf):
    with pytest.raises(AirflowFailException, match="No source run_id"):
        _resolve_callable()(triggering_asset_events=_events(), dag_run=_dag_run(conf))


@pytest.mark.usefixtures("spark_connection")
def test_p1_4_resolve_reads_conf_not_the_memoised_param(dagbag):
    """C-5: Param.resolve() memoises the value on the DAG's Param object, so
    in one process (chained `dag.test()`), a run without conf inherited the
    previous run's `params.run_id`. The manual run id is read from
    `dag_run.conf` instead: a stale value left on the Param is ignored."""
    from dags.local_read_file_dag import dag_instance

    param = dag_instance.params.get_param("run_id")
    previous = param.value
    try:
        param.resolve("manual__previous_run")  # what a previous run leaves behind
        with pytest.raises(AirflowFailException, match="No source run_id"):
            _resolve_callable()(triggering_asset_events=_events(), dag_run=_dag_run({}))
    finally:
        param.value = previous


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

    from dags.local_read_file_dag import JOB_PATH, build_application_args
    from pipeline.jobs.read_files_job import parse_args

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
        conn_id="spark_p1_4_argv", application_args=build_application_args(run_id)
    )
    hook.submit(str(JOB_PATH))

    argv = json.loads(received.read_text())
    assert argv[0] == "spark-submit"
    assert argv[-2:] == [str(JOB_PATH), f"--run-id={run_id}"]
    assert parse_args(argv[-1:]).run_id == run_id
