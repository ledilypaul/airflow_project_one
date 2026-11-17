"""
P3-3 - guards on the observability setup around the code.

- docker-compose.yaml: the collector and Prometheus only run with the
  `observability` profile (`make observability-up`), never with `make db-up`;
  images pinned by tag and digest; host ports on 127.0.0.1 only; own network.
- the collector and Prometheus configurations agree with each other and with
  the compose file (ports, service names, mounted paths).
- Makefile: Airflow's OpenTelemetry metrics are configured for `make airflow`
  only, and off unless OTEL_EXPORTER_OTLP_ENDPOINT is set.
- the test suite never emits metrics, whatever the shell exports.
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OBSERVABILITY_DIR = PROJECT_ROOT / "deploy" / "observability"
OBSERVABILITY_SERVICES = {"otel-collector", "prometheus"}
# Host ports taken by the rest of the project (Postgres, Redis, Airflow UI,
# Marquez API and web UI).
OTHER_HOST_PORTS = {"5434", "6379", "8080", "5002", "3002"}


# Fake credentials, built at run time: nothing in the file that looks like a
# secret to gitleaks (it scans the whole history).
FAKE_USER = "svc" + "_user"
FAKE_SECRET = "p" + "w-" + "9x" + "Qz" + "-fake"


def _userinfo_url(host_port: str) -> str:
    return "http://" + FAKE_USER + ":" + FAKE_SECRET + "@" + host_port


def _compose() -> dict:
    return yaml.safe_load((PROJECT_ROOT / "docker-compose.yaml").read_text())


def _services() -> dict:
    return _compose()["services"]


def _collector_config() -> dict:
    return yaml.safe_load((OBSERVABILITY_DIR / "otel-collector.yaml").read_text())


def _prometheus_config() -> dict:
    return yaml.safe_load((OBSERVABILITY_DIR / "prometheus.yml").read_text())


def _port(endpoint: str) -> str:
    return endpoint.rsplit(":", 1)[1]


# ---------------------------------------------------------------------------
# docker-compose.yaml
# ---------------------------------------------------------------------------


def test_p3_3_observability_only_runs_with_its_profile():
    services = _services()
    assert OBSERVABILITY_SERVICES <= set(services)
    for name in OBSERVABILITY_SERVICES:
        assert services[name].get("profiles") == ["observability"], name


def test_p3_3_observability_images_are_pinned_by_tag_and_digest():
    for name in OBSERVABILITY_SERVICES:
        image = _services()[name]["image"]
        # A tag for the reader, a digest for the pull (SEC-5).
        assert re.fullmatch(r"[\w./-]+:v?\d+(\.\d+)+@sha256:[0-9a-f]{64}", image), image


def test_p3_3_observability_ports_are_localhost_only_and_do_not_collide():
    services = _services()
    published = {name: services[name].get("ports", []) for name in OBSERVABILITY_SERVICES}
    # Exactly the 127.0.0.1:<host>:<container> form: a bare "4318:4318"
    # listens on every interface (neither service has authentication).
    assert published == {
        "otel-collector": ["127.0.0.1:4318:4318"],
        "prometheus": ["127.0.0.1:9092:9090"],
    }
    host_ports = {p.split(":")[1] for ports in published.values() for p in ports}
    assert not host_ports & OTHER_HOST_PORTS


def test_p3_3_the_collectors_prometheus_exporter_is_not_published():
    """Prometheus reaches it by service name on the compose network."""
    collector = _services()["otel-collector"]
    assert not any("8889" in str(p) for p in collector["ports"])
    assert "expose" not in collector or "8889" not in str(collector["expose"])


def test_p3_3_observability_has_its_own_network():
    compose = _compose()
    for name in OBSERVABILITY_SERVICES:
        assert _services()[name]["networks"] == ["observability"], name
    assert "observability" in compose["networks"]
    for name in ("postgres", "redis"):  # default network (kept by P3-1)
        assert "networks" not in _services()[name], name


def test_p3_3_configuration_is_mounted_read_only_from_files_that_exist():
    services = _services()
    mounts = [
        v for name in OBSERVABILITY_SERVICES for v in services[name].get("volumes", []) if "./" in v
    ]
    assert len(mounts) == 3, mounts
    for mount in mounts:
        source, _, rest = mount.partition(":")
        assert rest.endswith(":ro"), f"{mount}: configuration must be mounted read-only"
        assert (PROJECT_ROOT / source).exists(), source
    # Data lives in a named volume, declared at the top level.
    assert "prometheus-data" in _compose()["volumes"]
    assert any(v.startswith("prometheus-data:") for v in services["prometheus"]["volumes"])


def test_p3_3_no_credential_in_the_observability_files():
    """Nothing to hide in these files: no token, password or key at all."""
    pattern = re.compile(r"(?i)password|passwd|secret|api[_-]?key|token|authorization|bearer")
    for path in OBSERVABILITY_DIR.rglob("*"):
        if path.is_file():
            assert not pattern.search(path.read_text()), path
    compose = yaml.safe_dump({n: _services()[n] for n in OBSERVABILITY_SERVICES})
    assert not pattern.search(compose)


# ---------------------------------------------------------------------------
# OpenTelemetry Collector / Prometheus configuration
# ---------------------------------------------------------------------------


def test_p3_3_collector_receives_otlp_http_on_the_published_port():
    config = _collector_config()
    http = config["receivers"]["otlp"]["protocols"]
    # Airflow's exporter defaults to http/protobuf (4318), not gRPC.
    assert set(http) == {"http"}
    published = _services()["otel-collector"]["ports"][0].split(":")
    assert _port(http["http"]["endpoint"]) == published[2] == "4318"


def test_p3_3_collector_pipeline_is_metrics_only_and_capped():
    config = _collector_config()
    pipelines = config["service"]["pipelines"]
    assert set(pipelines) == {"metrics"}  # no traces, no logs
    metrics = pipelines["metrics"]
    assert metrics["receivers"] == ["otlp"]
    assert metrics["exporters"] == ["prometheus"]  # not `debug`: it would log every metric
    assert metrics["processors"][0] == "memory_limiter"  # first, as the collector requires
    assert set(config["exporters"]) == {"prometheus"}
    # Its own metrics are off: they would open another port.
    assert config["service"]["telemetry"]["metrics"]["level"] == "none"


def test_p3_3_prometheus_scrapes_the_collectors_exporter_by_service_name():
    exporter = _collector_config()["exporters"]["prometheus"]
    (job,) = _prometheus_config()["scrape_configs"]
    (target,) = job["static_configs"][0]["targets"]
    assert target == f"otel-collector:{_port(exporter['endpoint'])}"
    assert "otel-collector" in _services()


def test_p3_3_prometheus_loads_the_mounted_rules():
    config = _prometheus_config()
    volumes = _services()["prometheus"]["volumes"]
    rules_mount = next(v for v in volumes if v.startswith("./deploy/observability/rules:"))
    container_dir = rules_mount.split(":")[1]
    assert config["rule_files"] == [f"{container_dir}/*.yml"]
    config_mount = next(v for v in volumes if v.endswith("/prometheus.yml:ro"))
    assert f"--config.file={config_mount.split(':')[1]}" in _services()["prometheus"]["command"]
    # The unit tests of the rules must NOT sit in the loaded directory.
    assert not list((OBSERVABILITY_DIR / "rules").glob("*test*"))


def test_p3_3_a_series_outlives_a_dead_scheduler_for_five_minutes_only():
    """The heartbeat rule's 5 minute window assumes it (rules/airflow.yml)."""
    assert _collector_config()["exporters"]["prometheus"]["metric_expiration"] == "5m"


# ---------------------------------------------------------------------------
# Makefile
# ---------------------------------------------------------------------------


def _make(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    base = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("OTEL_", "AIRFLOW__METRICS__", "OPENLINEAGE_"))
    }
    return subprocess.run(
        ["make", "--no-print-directory", "-s", "-C", str(PROJECT_ROOT), *args],
        capture_output=True,
        text=True,
        timeout=60,
        env={**base, **(env or {})},
    )


def _exported(proc: subprocess.CompletedProcess) -> dict[str, str]:
    assert proc.returncode == 0, proc.stderr
    return dict(line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line)


def test_p3_3_metrics_are_off_without_an_endpoint():
    proc = _make("metrics-env")

    assert proc.returncode == 0, proc.stderr
    # Nothing exported at all: Airflow keeps its own default (otel_on = False).
    assert _exported(proc) == {}
    assert "metrics off" in proc.stdout


def test_p3_3_a_blank_endpoint_does_not_turn_the_metrics_on():
    proc = _make("metrics-env", env={"OTEL_EXPORTER_OTLP_ENDPOINT": "  "})

    # The shell's own (blank) variable stays what it is; what matters is that
    # `otel_on` is not set, so Airflow emits nothing.
    assert "AIRFLOW__METRICS__OTEL_ON" not in _exported(proc)


def test_p3_3_endpoint_turns_the_metrics_on_with_the_standard_variables():
    proc = _make("metrics-env", env={"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318"})

    assert _exported(proc) == {
        "AIRFLOW__METRICS__OTEL_ON": "True",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
        "OTEL_SERVICE_NAME": "airflow_project_one",
        "OTEL_METRIC_EXPORT_INTERVAL": "15000",
    }


def test_p3_3_endpoint_can_be_given_on_the_command_line():
    proc = _make("metrics-env", "OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318")

    exported = _exported(proc)
    assert exported["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://collector:4318"
    assert exported["AIRFLOW__METRICS__OTEL_ON"] == "True"


def test_p3_3_a_value_exported_by_the_shell_wins():
    proc = _make(
        "metrics-env",
        env={
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318",
            "AIRFLOW__METRICS__OTEL_ON": "False",
            "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
            "OTEL_SERVICE_NAME": "my_airflow",
            "OTEL_METRIC_EXPORT_INTERVAL": "5000",
        },
    )

    exported = _exported(proc)
    assert exported["AIRFLOW__METRICS__OTEL_ON"] == "False"
    assert exported["OTEL_EXPORTER_OTLP_PROTOCOL"] == "grpc"
    assert exported["OTEL_SERVICE_NAME"] == "my_airflow"
    assert exported["OTEL_METRIC_EXPORT_INTERVAL"] == "5000"


def test_p3_3_headers_are_never_printed():
    """OTEL_EXPORTER_OTLP_HEADERS may carry the credentials of a remote
    collector: `make metrics-env` does not show them."""
    proc = _make(
        "metrics-env",
        env={
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318",
            "OTEL_EXPORTER_OTLP_HEADERS": "x-" + "key=" + FAKE_SECRET,
            "OTEL_EXPORTER_OTLP_METRICS_HEADERS": "x-" + "key=" + FAKE_SECRET,
        },
    )

    assert FAKE_SECRET not in proc.stdout + proc.stderr
    assert "HEADERS" not in proc.stdout


def test_p3_3_credentials_in_the_endpoint_are_warned_about():
    proc = _make("metrics-env", env={"OTEL_EXPORTER_OTLP_ENDPOINT": _userinfo_url("host:4318")})

    assert "OTEL_EXPORTER_OTLP_HEADERS" in proc.stderr
    assert FAKE_SECRET not in proc.stderr  # the warning never shows the value


@pytest.mark.parametrize(
    "variable", ["OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"]
)
def test_p3_3_sec2_metrics_env_masks_the_user_info_and_the_query_string(variable):
    env = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318",
        variable: _userinfo_url("host:4318") + "/v1/metrics?api_key=" + FAKE_SECRET,
    }

    proc = _make("metrics-env", env=env)

    assert proc.returncode == 0, proc.stderr
    assert FAKE_SECRET not in proc.stdout + proc.stderr
    assert FAKE_USER not in proc.stdout + proc.stderr
    # What is left says where, not who: scheme, host, port, path.
    shown = _exported(proc)[variable]
    assert shown.startswith("http://") and "host:4318/v1/metrics" in shown


def test_p3_3_sec2_make_airflow_starts_without_metrics_when_the_endpoint_has_credentials(
    fake_uv, tmp_path
):
    """Even when the endpoint answers: Airflow would log it."""
    import socket

    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen(5)
        endpoint = _userinfo_url(f"127.0.0.1:{server.getsockname()[1]}")

        proc = _make_airflow(fake_uv, tmp_path, {"OTEL_EXPORTER_OTLP_ENDPOINT": endpoint})

    assert proc.returncode == 0, proc.stderr
    assert "STARTED OTEL_ON=False" in proc.stdout
    assert "credentials" in proc.stderr
    for secret in (FAKE_SECRET, FAKE_USER):
        assert secret not in proc.stdout + proc.stderr


def test_p3_3_only_the_airflow_targets_export_the_metrics_variables():
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    lines = [
        line
        for line in makefile.splitlines()
        if re.match(r"^\$\(\w+\): export (OTEL_|AIRFLOW__METRICS__)", line)
    ]
    assert len(lines) == 5, lines
    assert all(line.startswith("$(METRICS_TARGETS):") for line in lines)
    assert re.search(r"^METRICS_TARGETS := airflow metrics-env$", makefile, re.MULTILINE)
    # `make test` and the dbt/Spark targets keep the CI environment.
    for target in ("test", "check", "spark-job", "dbt-build", "dbt-parse"):
        assert target not in makefile.split("METRICS_TARGETS :=")[1].split("\n")[0].split()


def _airflow_target_variables(endpoint: str | None) -> dict[str, str]:
    """Target-specific variables make resolved for `airflow` (dry run, -p:
    nothing starts), the metrics ones only."""
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("OTEL_", "AIRFLOW__METRICS__", "OPENLINEAGE_"))
    }
    if endpoint is not None:
        env["OTEL_EXPORTER_OTLP_ENDPOINT"] = endpoint
    proc = subprocess.run(
        ["make", "-C", str(PROJECT_ROOT), "-p", "-n", "airflow", "UV=true"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    found = re.findall(
        r"^airflow: ((?:OTEL_|AIRFLOW__METRICS__)\w+) :?= (.*)$", proc.stdout, re.MULTILINE
    )
    return dict(found)


def test_p3_3_make_airflow_gets_the_metrics_environment_only_with_an_endpoint():
    assert _airflow_target_variables(None) == {}
    with_endpoint = _airflow_target_variables("http://127.0.0.1:4318")
    assert set(with_endpoint) == {
        "AIRFLOW__METRICS__OTEL_ON",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_SERVICE_NAME",
        "OTEL_METRIC_EXPORT_INTERVAL",
    }
    assert with_endpoint["AIRFLOW__METRICS__OTEL_ON"] == "True"
    assert with_endpoint["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://127.0.0.1:4318"
    # The values `make airflow` really exports are checked through
    # `make metrics-env` above (a `?=` shows here unexpanded).


# ---------------------------------------------------------------------------
# The suite never emits metrics
# ---------------------------------------------------------------------------


def test_p3_3_suite_environment_is_neutral():
    """tests/conftest.py ran first: metrics forced off, OTEL_* gone."""
    assert os.environ["AIRFLOW__METRICS__OTEL_ON"] == "False"
    assert os.environ["AIRFLOW__TRACES__OTEL_ON"] == "False"
    leaked = [
        k for k in os.environ if k.startswith(("OTEL_", "AIRFLOW__METRICS__", "AIRFLOW__TRACES__"))
    ]
    assert sorted(leaked) == ["AIRFLOW__METRICS__OTEL_ON", "AIRFLOW__TRACES__OTEL_ON"]


def test_p3_3_airflow_reads_the_metrics_as_off():
    from airflow.configuration import conf

    assert conf.getboolean("metrics", "otel_on") is False
    assert conf.getboolean("metrics", "statsd_on") is False


def test_p3_3_a_shell_set_up_for_metrics_does_not_reach_the_suite():
    """Run the neutrality test above under the environment `make airflow`
    would export (and a StatsD one): conftest.py must clean it."""
    env = {
        **os.environ,
        "AIRFLOW__METRICS__OTEL_ON": "True",
        "AIRFLOW__METRICS__STATSD_ON": "True",
        "AIRFLOW__TRACES__OTEL_ON": "True",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318",
        "OTEL_EXPORTER_OTLP_HEADERS": "x=y",
        "OTEL_METRICS_EXPORTER": "console",
    }
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            f"{__file__}::test_p3_3_suite_environment_is_neutral",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
        cwd=PROJECT_ROOT,
    )

    assert proc.returncode == 0, proc.stdout[-2000:] + proc.stderr[-2000:]
    assert "1 passed" in proc.stdout, proc.stdout[-500:]


# ---------------------------------------------------------------------------
# `make airflow` starts without metrics when the collector is not there
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_uv(tmp_path):
    """A `uv` that runs `python` for real (the Makefile's helper scripts) and
    only reports, for `airflow standalone`, what metrics setting it would
    start with."""
    stub = tmp_path / "uv"
    stub.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = run ] && [ "$2" = airflow ]; then\n'
        '  echo "STARTED OTEL_ON=${AIRFLOW__METRICS__OTEL_ON:-unset}"; exit 0\n'
        "fi\n"
        "exit 0\n"
    )
    stub.chmod(0o755)
    return stub


def _make_airflow(fake_uv, tmp_path, env: dict[str, str]) -> subprocess.CompletedProcess:
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    base = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("OTEL_", "AIRFLOW__METRICS__", "OPENLINEAGE_"))
    }
    return subprocess.run(
        [
            "make",
            "-C",
            str(PROJECT_ROOT),
            "-o",
            "dbt-parse",  # the prerequisite (downloads dbt) is not under test
            "airflow",
            f"UV={fake_uv}",
            f"PYTHON={sys.executable}",
            f"LINEAGE_DIR={tmp_path / 'lineage'}",
            "OPENLINEAGE_DISABLED=true",
            f"AIRFLOW_HOME={tmp_path}",
        ],
        capture_output=True,
        text=True,
        timeout=120,
        env={**base, **env},
    )


def test_p3_3_make_airflow_keeps_the_metrics_when_the_collector_answers(fake_uv, tmp_path):
    import socket

    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen(5)
        endpoint = f"http://127.0.0.1:{server.getsockname()[1]}"

        proc = _make_airflow(fake_uv, tmp_path, {"OTEL_EXPORTER_OTLP_ENDPOINT": endpoint})

    assert proc.returncode == 0, proc.stderr
    assert "STARTED OTEL_ON=True" in proc.stdout
    assert "warning" not in proc.stderr


def test_p3_3_make_airflow_starts_without_metrics_when_the_collector_is_down(fake_uv, tmp_path):
    import socket

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]  # closed again when the block ends

    proc = _make_airflow(
        fake_uv, tmp_path, {"OTEL_EXPORTER_OTLP_ENDPOINT": f"http://127.0.0.1:{port}"}
    )

    assert proc.returncode == 0, proc.stderr  # Airflow still starts
    assert "STARTED OTEL_ON=False" in proc.stdout
    assert "starts without its OpenTelemetry metrics" in proc.stderr
    assert "make observability-up" in proc.stderr


def test_p3_3_make_airflow_without_endpoint_leaves_the_metrics_setting_alone(fake_uv, tmp_path):
    proc = _make_airflow(fake_uv, tmp_path, {})

    assert proc.returncode == 0, proc.stderr
    assert "STARTED OTEL_ON=unset" in proc.stdout
    assert "OpenTelemetry" not in proc.stderr


# ---------------------------------------------------------------------------
# Container hardening (SEC-1, SEC-3)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(OBSERVABILITY_SERVICES))
def test_p3_3_sec3_containers_drop_every_capability_and_cannot_gain_privileges(name):
    service = _services()[name]
    assert service["cap_drop"] == ["ALL"]
    assert "cap_add" not in service
    assert service["security_opt"] == ["no-new-privileges:true"]
    assert not service.get("privileged")


def test_p3_3_sec3_the_collector_has_a_read_only_filesystem_and_a_memory_cap():
    collector = _services()["otel-collector"]
    assert collector["read_only"] is True
    assert collector["mem_limit"] == "512m"
    # The collector's own limiter (256 MiB) stays under the container's cap.
    assert _collector_config()["processors"]["memory_limiter"]["limit_mib"] < 512


def test_p3_3_sec3_prometheus_data_is_capped_in_size_as_well_as_in_time():
    command = _services()["prometheus"]["command"]
    assert "--storage.tsdb.retention.time=7d" in command
    assert "--storage.tsdb.retention.size=1GB" in command


def test_p3_3_sec1_prometheus_only_accepts_cross_origin_calls_from_its_own_ui():
    """Prometheus' default CORS origin is `.*`: any web page the developer
    opens could query 127.0.0.1:9092 from the browser. Restricted to the UI's
    own origin (localhost, the published port)."""
    command = _services()["prometheus"]["command"]
    (flag,) = [c for c in command if c.startswith("--web.cors.origin=")]
    pattern = flag.split("=", 1)[1]
    regex = re.compile(f"(?:{pattern})")
    for allowed in ("http://127.0.0.1:9092", "http://localhost:9092", "https://localhost:9092"):
        assert regex.fullmatch(allowed), allowed
    for refused in (
        "http://evil.example",
        "http://127.0.0.1:9092.evil.example",
        "http://localhost:9093",
        "http://127.0.0.1:80",
        "null",
    ):
        assert not regex.fullmatch(refused), refused
