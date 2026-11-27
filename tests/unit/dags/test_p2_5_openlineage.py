"""
P2-5 — OpenLineage configuration of the Airflow side and of the Spark job.

- The test suite never emits lineage: OpenLineage is disabled by
  tests/unit/dags/conftest.py, whatever the developer's shell exports.
- `openlineage_spark_settings()` (utils/read_files_operator.py) turns the
  Airflow OpenLineage configuration (environment variables) into the
  SparkSubmitOperator kwargs that enable the JVM listener:
  - disabled, or no listener jar: nothing at all, spark-submit gets exactly
    the command it got before P2-5;
  - `file` transport: the Spark events go next to the Airflow ones
    (`spark.jsonl`), the provider does not inject a transport;
  - any other transport (http...): the provider injects it
    (`openlineage_inject_transport_info`), we set none.
  The parent job info is always injected when the listener is on: the Spark
  run is attached to its Airflow task.
- The spark-submit command is checked through the real SparkSubmitHook,
  without running Spark.
- Review fixes (C-n / SEC-n): lineage never breaks the pipeline. An invalid
  jar (truncated, no listener class, wrong SHA-256) or an invalid
  configuration turns Spark lineage off, with the pre-P2-5 command; no
  secret of the transport ever reaches the logged spark-submit command; one
  Spark event file per application.
"""

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
LISTENER = "io.openlineage.spark.agent.OpenLineageSparkListener"
REDACTION = (
    r"(?i)secret|password|token|access[.]?key|apikey|authorization|openlineage\.transport\.url"
)
KEY_VALUE_TRIMMER = "io.openlineage.client.dataset.partition.trimmer.KeyValueTrimmer"
OFF = {
    "conf": None,
    "openlineage_inject_parent_job_info": False,
    "openlineage_inject_transport_info": False,
}


@pytest.fixture
def lineage_on(monkeypatch, tmp_path):
    """OpenLineage enabled with the dev `file` transport, under tmp_path."""
    events = tmp_path / "lineage" / "events"
    events.mkdir(parents=True)
    from tests.unit.dags.lineage_jar import make_listener_jar

    jar = make_listener_jar(tmp_path / "openlineage-spark_2.13-test.jar")
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__DISABLED", "False")
    monkeypatch.setenv(
        "AIRFLOW__OPENLINEAGE__TRANSPORT",
        json.dumps({"type": "file", "log_file_path": str(events / "airflow"), "append": False}),
    )
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__NAMESPACE", "p2_5_tests")
    monkeypatch.setenv("OPENLINEAGE_SPARK_JAR", str(jar))
    monkeypatch.delenv("OPENLINEAGE_SPARK_SHA256", raising=False)
    # The provider builds its client once per process: start from none, so
    # each test's transport is the one injected.
    from airflow.providers.openlineage.plugins.listener import get_openlineage_listener

    monkeypatch.setattr(get_openlineage_listener().adapter, "_client", None)
    return SimpleNamespace(events=events, jar=jar)


def _settings():
    from utils.read_files_operator import openlineage_spark_settings

    return openlineage_spark_settings()


# ---------------------------------------------------------------------------
# No lineage from the test suite
# ---------------------------------------------------------------------------


def test_p2_5_test_suite_never_emits_lineage():
    """conftest.py forces OpenLineage off and drops the client's own
    variables: no event can leave the test process (no file, no network)."""
    from airflow.providers.openlineage import conf as ol_conf

    assert ol_conf.is_disabled() is True
    for name in ("OPENLINEAGE_URL", "OPENLINEAGE_CONFIG", "OPENLINEAGE_API_KEY"):
        assert name not in os.environ, name
    assert not [k for k in os.environ if k.startswith("OPENLINEAGE__")]


def test_p2_5_ingest_dag_parsed_in_tests_has_no_listener():
    """The DAG as the suite parses it: no listener, no injection."""
    from dags.source_pipelines import build_dags

    task = build_dags()["ingest__orders"].get_task("spark_read_files")
    assert not task.conf
    assert task._openlineage_inject_parent_job_info is False
    assert task._openlineage_inject_transport_info is False


# ---------------------------------------------------------------------------
# openlineage_spark_settings()
# ---------------------------------------------------------------------------


def test_p2_5_settings_off_when_openlineage_is_disabled(lineage_on, monkeypatch):
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__DISABLED", "True")
    assert _settings() == OFF


def test_p2_5_settings_off_with_the_legacy_disabled_variable(lineage_on, monkeypatch):
    """OPENLINEAGE_DISABLED=true (documented switch) is honoured too."""
    monkeypatch.setenv("OPENLINEAGE_DISABLED", "true")
    assert _settings()["conf"] is None


def test_p2_5_settings_off_without_any_transport(lineage_on, monkeypatch):
    """No transport configured at all: the provider itself is disabled."""
    monkeypatch.delenv("AIRFLOW__OPENLINEAGE__TRANSPORT")
    assert _settings()["conf"] is None


@pytest.mark.parametrize("jar", ["", "   "])
def test_p2_5_settings_off_without_listener_jar(lineage_on, monkeypatch, jar, caplog):
    """Airflow emits, but no listener jar is configured for Spark: the job
    runs as before (a listener class missing from the classpath would make
    SparkContext fail to start). C-11: logged at debug level only, this is
    read at every parse of the DAG file."""
    monkeypatch.setenv("OPENLINEAGE_SPARK_JAR", jar)
    with caplog.at_level("INFO"):
        assert _settings()["conf"] is None
    assert caplog.text == ""


def test_p2_5_settings_off_and_warns_when_the_jar_is_missing(
    lineage_on, monkeypatch, tmp_path, caplog
):
    missing = tmp_path / "nowhere" / "openlineage-spark.jar"
    monkeypatch.setenv("OPENLINEAGE_SPARK_JAR", str(missing))
    with caplog.at_level("WARNING"):
        settings = _settings()
    assert settings["conf"] is None
    assert settings["openlineage_inject_parent_job_info"] is False
    assert str(missing) in caplog.text
    assert "make lineage-jar" in caplog.text


def test_p2_5_settings_off_and_warn_with_a_relative_jar(lineage_on, monkeypatch, caplog):
    """A relative path would depend on the worker's current directory."""
    monkeypatch.chdir(lineage_on.jar.parent)
    monkeypatch.setenv("OPENLINEAGE_SPARK_JAR", lineage_on.jar.name)
    with caplog.at_level("WARNING"):
        assert _settings()["conf"] is None
    assert "absolute" in caplog.text


def test_p2_5_settings_expand_a_home_relative_jar(lineage_on, monkeypatch):
    monkeypatch.setenv("HOME", str(lineage_on.jar.parent))
    monkeypatch.setenv("OPENLINEAGE_SPARK_JAR", f"~/{lineage_on.jar.name}")
    assert _settings()["conf"]["spark.jars"] == str(lineage_on.jar)


def test_p2_5_settings_file_transport(lineage_on):
    """Dev: listener + jar + namespace, Spark events in spark.jsonl next to
    the Airflow ones; parent info injected, transport not (ours is set)."""
    settings = _settings()
    assert settings["conf"] == {
        "spark.jars": str(lineage_on.jar),
        "spark.extraListeners": LISTENER,
        "spark.redaction.regex": REDACTION,
        "spark.openlineage.namespace": "p2_5_tests",
        "spark.openlineage.dataset.disabledTrimmers": KEY_VALUE_TRIMMER,
        "spark.openlineage.transport.type": "file",
        "spark.openlineage.transport.location": str(lineage_on.events / "spark.jsonl"),
    }
    assert settings["openlineage_inject_parent_job_info"] is True
    assert settings["openlineage_inject_transport_info"] is False


def test_p2_5_settings_http_transport_is_injected_by_the_provider(lineage_on, monkeypatch):
    """OPENLINEAGE_URL -> http: the provider injects url/endpoint/auth itself
    (only http is supported by its injection), we add no transport key."""
    monkeypatch.setenv(
        "AIRFLOW__OPENLINEAGE__TRANSPORT",
        json.dumps({"type": "http", "url": "http://127.0.0.1:9"}),
    )
    settings = _settings()
    assert settings["conf"] == {
        "spark.jars": str(lineage_on.jar),
        "spark.extraListeners": LISTENER,
        "spark.redaction.regex": REDACTION,
        "spark.openlineage.namespace": "p2_5_tests",
        "spark.openlineage.dataset.disabledTrimmers": KEY_VALUE_TRIMMER,
    }
    assert not [k for k in settings["conf"] if k.startswith("spark.openlineage.transport")]
    assert settings["openlineage_inject_parent_job_info"] is True
    assert settings["openlineage_inject_transport_info"] is True


def test_p2_5_settings_set_no_spark_resources(lineage_on):
    """M6 still holds with the listener on: no memory/cores key."""
    from tests.unit.dags.test_p1_5_spark_submit import _hardcoded_resources

    assert _hardcoded_resources({"conf": _settings()["conf"]}) == []


# ---------------------------------------------------------------------------
# The spark-submit command, through the real operator and hook
# ---------------------------------------------------------------------------


class _FakeDagRun:
    conf: dict = {}
    clear_number = 0
    logical_date = datetime(2026, 11, 14, tzinfo=UTC)
    run_after = logical_date
    run_id = "manual__p2_5"


class _FakeTaskInstance:
    """What the provider's lineage macros read from a task instance."""

    dag_id = "ingest__orders"
    task_id = "spark_read_files"
    try_number = 1
    map_index = 0
    logical_date = _FakeDagRun.logical_date

    def __init__(self):
        self.dag_run = _FakeDagRun()

    def get_template_context(self):
        return {"dag_run": self.dag_run}


def _context():
    ti = _FakeTaskInstance()
    return {"ti": ti, "task_instance": ti, "dag_run": ti.dag_run}


def _submitted_command(monkeypatch, **operator_kwargs) -> list[str]:
    """Run SparkSubmitOperator.execute() with the hook's submit() replaced by
    a recorder of the command the hook would launch."""
    from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

    from utils.read_files_operator import JOB_PATH, PROJECT_ROOT, SPARK_CONN_ID

    monkeypatch.setenv("AIRFLOW_CONN_SPARK_DEFAULT", '{"conn_type": "spark", "host": "local[*]"}')
    commands = []

    def submit(self, application="", **kwargs):
        commands.append(self._build_spark_submit_command(application))

    monkeypatch.setattr(SparkSubmitHook, "submit", submit)
    operator = SparkSubmitOperator(
        task_id="spark_read_files",
        application=str(JOB_PATH),
        conn_id=SPARK_CONN_ID,
        env_vars={"PYTHONPATH": str(PROJECT_ROOT)},
        application_args=["--run-id=manual__p2_5"],
        **operator_kwargs,
    )
    operator.execute(_context())
    assert len(commands) == 1
    return commands[0]


def _confs(command: list[str]) -> dict[str, str]:
    values = [command[i + 1] for i, arg in enumerate(command) if arg == "--conf"]
    return dict(value.split("=", 1) for value in values)


def test_p2_5_spark_submit_command_unchanged_when_lineage_is_off(monkeypatch):
    """Disabled (the suite's state): byte-for-byte the pre-P2-5 command."""
    from utils.read_files_operator import openlineage_spark_settings

    before = _submitted_command(monkeypatch)
    after = _submitted_command(monkeypatch, **openlineage_spark_settings())
    assert after == before
    assert not [arg for arg in after if "openlineage" in arg.lower()]


def test_p2_5_spark_submit_command_carries_listener_and_parent(lineage_on, monkeypatch):
    """Listener on: the hook's command has the listener, the jar, the file
    transport, and the parent job info injected by the operator (the Spark
    run is attached to ingest__orders.spark_read_files)."""
    from utils.read_files_operator import JOB_PATH

    command = _submitted_command(monkeypatch, **_settings())
    confs = _confs(command)
    assert confs["spark.extraListeners"] == LISTENER
    assert confs["spark.jars"] == str(lineage_on.jar)
    assert confs["spark.openlineage.transport.type"] == "file"
    assert confs["spark.openlineage.transport.location"] == str(lineage_on.events / "spark.jsonl")
    assert confs["spark.openlineage.namespace"] == "p2_5_tests"
    assert confs["spark.openlineage.parentJobNamespace"] == "p2_5_tests"
    assert confs["spark.openlineage.parentJobName"] == "ingest__orders.spark_read_files"
    assert confs["spark.openlineage.rootParentJobName"] == "ingest__orders"
    for key in ("spark.openlineage.parentRunId", "spark.openlineage.rootParentRunId"):
        assert len(confs[key]) == 36, (key, confs[key])  # a UUID
    # The job and its single argument still close the command.
    assert command[-2:] == [str(JOB_PATH), "--run-id=manual__p2_5"]


def test_p2_5_http_transport_is_injected_into_the_command(lineage_on, monkeypatch):
    monkeypatch.setenv(
        "AIRFLOW__OPENLINEAGE__TRANSPORT",
        json.dumps({"type": "http", "url": "http://127.0.0.1:9", "endpoint": "api/v1/lineage"}),
    )
    confs = _confs(_submitted_command(monkeypatch, **_settings()))
    assert confs["spark.openlineage.transport.type"] == "http"
    assert confs["spark.openlineage.transport.url"] == "http://127.0.0.1:9"
    assert confs["spark.openlineage.transport.endpoint"] == "api/v1/lineage"


# ---------------------------------------------------------------------------
# file_list as input of spark_read_files (the job selects its files there)
# ---------------------------------------------------------------------------


def test_p2_5_spark_task_prepares_lineage_before_execution():
    """pre_execute: SHA-256 of the jar, one event file per application, and
    file_list as input (the job selects its files there)."""
    from dags.source_pipelines import build_dags
    from utils.read_files_operator import prepare_spark_lineage

    task = build_dags()["ingest__orders"].get_task("spark_read_files")
    assert task._pre_execute_hook is prepare_spark_lineage


# ---------------------------------------------------------------------------
# C-1 / SEC-2 — a present but invalid jar must not stop Spark
# ---------------------------------------------------------------------------


def test_p2_5_c1_truncated_jar_turns_spark_lineage_off(lineage_on, caplog):
    data = lineage_on.jar.read_bytes()
    lineage_on.jar.write_bytes(data[: len(data) // 2])
    with caplog.at_level("WARNING"):
        assert _settings() == OFF
    assert str(lineage_on.jar) in caplog.text


def test_p2_5_c1_jar_without_the_listener_class_turns_spark_lineage_off(lineage_on, caplog):
    from tests.unit.dags.lineage_jar import make_listener_jar

    make_listener_jar(lineage_on.jar, with_listener=False)
    with caplog.at_level("WARNING"):
        assert _settings() == OFF
    assert "OpenLineageSparkListener" in caplog.text


def _prepared(monkeypatch, settings, *, map_index=0, try_number=1, run_id="manual__p2_5"):
    """A SparkSubmitOperator built with `settings`, after prepare_spark_lineage
    (its pre_execute), and the command its hook would launch."""
    from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

    from utils.read_files_operator import JOB_PATH, SPARK_CONN_ID, prepare_spark_lineage

    monkeypatch.setenv("AIRFLOW_CONN_SPARK_DEFAULT", '{"conn_type": "spark", "host": "local[*]"}')
    commands = []
    monkeypatch.setattr(
        SparkSubmitHook,
        "submit",
        lambda self, application="", **kw: commands.append(
            self._build_spark_submit_command(application)
        ),
    )
    # file_list reporting is covered elsewhere: keep this one offline.
    monkeypatch.setattr("utils.read_files_operator.report_file_list_input", lambda context: None)
    operator = SparkSubmitOperator(
        task_id="spark_read_files",
        application=str(JOB_PATH),
        conn_id=SPARK_CONN_ID,
        application_args=["--run-id=x"],
        **settings,
    )
    context = _context()
    context["ti"].map_index = map_index
    context["ti"].try_number = try_number
    context["ti"].dag_run.run_id = run_id
    context["run_id"] = run_id
    context["task"] = operator
    prepare_spark_lineage(context)
    operator.execute(context)
    return operator, commands[0]


def test_p2_5_sec2_wrong_sha256_strips_lineage_at_execution(lineage_on, monkeypatch, caplog):
    """The jar is hashed when the task runs (not at every parse of the DAG):
    a mismatch removes everything P2-5 added, so the command is the one of a
    run without lineage, and the job runs."""
    off_operator, off_command = _prepared(monkeypatch, OFF)
    monkeypatch.setenv("OPENLINEAGE_SPARK_SHA256", "0" * 64)
    with caplog.at_level("WARNING"):
        operator, command = _prepared(monkeypatch, _settings())
    assert command == off_command
    assert not [arg for arg in command if "openlineage" in arg.lower()]
    assert "SHA-256" in caplog.text


def test_p2_5_sec2_matching_sha256_keeps_lineage(lineage_on, monkeypatch):
    from tests.unit.dags.lineage_jar import sha256

    monkeypatch.setenv("OPENLINEAGE_SPARK_SHA256", sha256(lineage_on.jar).upper())
    _, command = _prepared(monkeypatch, _settings())
    assert _confs(command)["spark.extraListeners"] == LISTENER


def test_p2_5_sec2_jar_hash_is_cached_by_path_size_and_mtime(lineage_on, monkeypatch):
    import utils.read_files_operator as module

    calls = []
    real = module._file_sha256
    monkeypatch.setattr(module, "_file_sha256", lambda *a: calls.append(a) or real(*a))
    module._jar_digest_cache.clear()
    monkeypatch.setenv("OPENLINEAGE_SPARK_SHA256", "0" * 64)
    for _ in range(3):
        _prepared(monkeypatch, _settings())
    assert len(calls) == 1
    stat = lineage_on.jar.stat()
    os.utime(lineage_on.jar, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10**9))  # new mtime
    _prepared(monkeypatch, _settings())
    assert len(calls) == 2


# ---------------------------------------------------------------------------
# C-13 — one Spark event file per application
# ---------------------------------------------------------------------------


def test_p2_5_c13_one_spark_event_file_per_application(lineage_on, monkeypatch):
    run_id = "asset_triggered__2026-11-14T10:00:00+00:00 é/../x"
    locations = set()
    for map_index, try_number in ((0, 1), (1, 1), (0, 2)):
        _, command = _prepared(
            monkeypatch, _settings(), map_index=map_index, try_number=try_number, run_id=run_id
        )
        location = Path(_confs(command)["spark.openlineage.transport.location"])
        assert location.parent == lineage_on.events  # no `..`, no `/` from the run id
        assert location.name.startswith("spark-ingest__orders-")
        assert location.suffix == ".jsonl"
        assert f"-{map_index}-{try_number}.jsonl" in location.name
        locations.add(location)
    assert len(locations) == 3


# ---------------------------------------------------------------------------
# C-2 — an invalid OpenLineage configuration never breaks the ingest DAGs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "transport", ['{"type": "file", "log_file_path": ', '["file"]', '"http"'], ids=str
)
def test_p2_5_c2_invalid_transport_turns_spark_lineage_off(lineage_on, monkeypatch, transport):
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__TRANSPORT", transport)
    assert _settings() == OFF


def test_p2_5_c2_invalid_transport_keeps_the_ingest_dags_importable(lineage_on, monkeypatch):
    from airflow.models import DagBag

    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__TRANSPORT", "{not json")
    bag = DagBag(dag_folder=str(PROJECT_ROOT / "dags" / "source_pipelines.py"))
    assert bag.import_errors == {}
    assert "ingest__orders" in bag.dags


def test_p2_5_c2_missing_provider_turns_spark_lineage_off(lineage_on, monkeypatch):
    monkeypatch.setitem(sys.modules, "airflow.providers.openlineage", None)
    monkeypatch.setitem(sys.modules, "airflow.providers.openlineage.conf", None)
    assert _settings() == OFF


# ---------------------------------------------------------------------------
# C-11 — the file transport is only derived from a local absolute path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", ["s3://bucket/lineage/airflow", "lineage/airflow", "file:x"])
def test_p2_5_c11_non_local_file_transport_turns_spark_lineage_off(
    lineage_on, monkeypatch, path, caplog
):
    monkeypatch.setenv(
        "AIRFLOW__OPENLINEAGE__TRANSPORT", json.dumps({"type": "file", "log_file_path": path})
    )
    with caplog.at_level("WARNING"):
        assert _settings() == OFF
    assert "log_file_path" in caplog.text


# ---------------------------------------------------------------------------
# SEC-1 — no secret of the transport in the logged spark-submit command
# ---------------------------------------------------------------------------

SECRET = "s3cr3t-T0KEN"


@pytest.mark.parametrize(
    "transport",
    [
        {"type": "http", "url": "http://ol:5000", "auth": {"type": "api_key", "apiKey": SECRET}},
        {"type": "http", "url": f"http://user:{SECRET}@ol:5000"},
        {"type": "http", "url": "http://ol:5000", "custom_headers": {"X-Token": SECRET}},
        {"type": "http", "url": "http://ol:5000", "headers": {"Authorization": SECRET}},
        {
            "type": "composite",
            "transports": {
                "marquez": {"type": "http", "url": "http://ol:5000"},
                "secured": {"type": "http", "url": "http://x", "auth": {"apiKey": SECRET}},
            },
        },
        {
            "type": "composite",
            "transports": [{"type": "http", "url": f"https://u:{SECRET}@x"}],
        },
    ],
    ids=["api_key", "url_userinfo", "custom_headers", "headers", "composite", "composite_list"],
)
def test_p2_5_sec1_no_secret_in_spark_submit_cmd(lineage_on, monkeypatch, transport, caplog):
    """Fail closed: a transport carrying credentials is never injected into
    the Spark properties, which the hook logs (`Spark-Submit cmd`) masking
    only keys with `secret` or `password`. Spark lineage is then off (no
    transport to send to), with a warning that says why."""
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__TRANSPORT", json.dumps(transport))
    with caplog.at_level("WARNING"):
        settings = _settings()
    assert settings["openlineage_inject_transport_info"] is False
    command = " ".join(_submitted_command(monkeypatch, **settings))
    assert SECRET not in command
    assert "credentials" in caplog.text
    assert SECRET not in caplog.text


def test_p2_5_sec1_uninspectable_transport_is_not_injected(lineage_on, monkeypatch, caplog):
    """Configured elsewhere (config_path, OPENLINEAGE_URL + API key...): we
    cannot check it has no secret, so it is not injected either."""
    monkeypatch.delenv("AIRFLOW__OPENLINEAGE__TRANSPORT")
    monkeypatch.setenv("OPENLINEAGE_URL", "http://ol:5000")
    monkeypatch.setenv("OPENLINEAGE_API_KEY", SECRET)
    with caplog.at_level("WARNING"):
        settings = _settings()
    assert settings == OFF
    assert SECRET not in " ".join(_submitted_command(monkeypatch, **settings))


def test_p2_5_sec1_clean_http_transport_gets_the_extended_redaction(lineage_on, monkeypatch):
    monkeypatch.setenv(
        "AIRFLOW__OPENLINEAGE__TRANSPORT", json.dumps({"type": "http", "url": "http://ol:5000"})
    )
    confs = _confs(_submitted_command(monkeypatch, **_settings()))
    assert confs["spark.redaction.regex"] == REDACTION
    assert confs["spark.openlineage.transport.url"] == "http://ol:5000"


# ---------------------------------------------------------------------------
# C-10 — report_file_list_input never raises
# ---------------------------------------------------------------------------


def test_p2_5_c10_report_file_list_input_returns_at_once_without_a_collector():
    """OpenLineage off: the collector is a no-op, no hook is even built."""
    from unittest.mock import patch

    from utils.read_files_operator import report_file_list_input

    with patch("airflow.providers.postgres.hooks.postgres.PostgresHook") as hook_cls:
        report_file_list_input({})
    hook_cls.assert_not_called()


def test_p2_5_c10_report_file_list_input_never_raises(monkeypatch, caplog):
    from unittest.mock import patch

    from airflow.sdk.lineage import HookLineageCollector

    from utils.read_files_operator import report_file_list_input

    monkeypatch.setattr(
        "airflow.sdk.lineage.get_hook_lineage_collector", lambda: HookLineageCollector()
    )
    with (
        caplog.at_level("WARNING"),
        patch(
            "airflow.providers.postgres.hooks.postgres.PostgresHook",
            side_effect=RuntimeError("boom"),
        ),
    ):
        report_file_list_input({})
    assert "file_list" in caplog.text
    monkeypatch.setitem(sys.modules, "airflow.providers.postgres.hooks.postgres", None)
    report_file_list_input({})  # ImportError: swallowed too
