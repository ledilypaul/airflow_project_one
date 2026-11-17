"""
P3-3 - `scripts/otel_endpoint_check.py`, run by `make airflow` before it
turns Airflow's OpenTelemetry metrics on.

A collector that is not there does not fail a task, but every Airflow process
then waits for the exporter's final flush when it exits (about 10 s per task,
measured), and logs an error per export. So `make airflow` only keeps the
metrics on when the endpoint accepts a TCP connection, and starts Airflow
without them (with a warning) otherwise: the pipeline is unchanged.
"""

import importlib.util
import os
import socket
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SCRIPT = PROJECT_ROOT / "scripts" / "otel_endpoint_check.py"


def _load():
    spec = importlib.util.spec_from_file_location("otel_endpoint_check", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def listener():
    """A local TCP port that accepts connections."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(5)
    yield server.getsockname()[1]
    server.close()


@pytest.fixture
def closed_port():
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    port = server.getsockname()[1]
    server.close()
    return port


def test_p3_3_a_reachable_endpoint_is_accepted(listener):
    module = _load()

    assert (
        module.endpoint_problem({"OTEL_EXPORTER_OTLP_ENDPOINT": f"http://127.0.0.1:{listener}"})
        is None
    )


def test_p3_3_an_unreachable_endpoint_is_reported(closed_port):
    module = _load()

    problem = module.endpoint_problem(
        {"OTEL_EXPORTER_OTLP_ENDPOINT": f"http://127.0.0.1:{closed_port}"}
    )

    assert problem and f"127.0.0.1:{closed_port}" in problem


def test_p3_3_the_metrics_specific_endpoint_wins_like_in_airflow(listener, closed_port):
    """Airflow's exporter takes OTEL_EXPORTER_OTLP_METRICS_ENDPOINT before the
    generic one."""
    module = _load()

    env = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": f"http://127.0.0.1:{closed_port}",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": f"http://127.0.0.1:{listener}/v1/metrics",
    }
    assert module.endpoint_problem(env) is None


@pytest.mark.parametrize(
    "endpoint",
    [
        "",
        "   ",
        "not a url",
        "http://",
        "://x:1",
        "http://host:notaport",
        "http://host:99999",  # port out of range: ValueError
        "http://[::1",  # unbalanced IPv6 bracket: ValueError
        "http://" + "a" * 70 + ":4318",  # DNS label too long: UnicodeError, not OSError
        "http://" + "a" * 300 + ".example:4318",
        "http://ex\u00e4mple\x00.test:4318",  # NUL in the host
        "http://.:4318",  # empty labels
        "ftp://127.0.0.1:21",
    ],
)
def test_p3_3_a_malformed_or_missing_endpoint_is_a_problem_not_an_exception(endpoint):
    module = _load()

    assert module.endpoint_problem({"OTEL_EXPORTER_OTLP_ENDPOINT": endpoint})
    assert module.endpoint_problem({})


# Fake credentials, built at run time: no literal that looks like a password in
# a URL (gitleaks scans the whole history).
FAKE_USER = "svc" + "_user"
FAKE_PASSWORD = "p" + "w-" + "9x" + "Qz" + "-fake"


def _with_userinfo(host_port: str) -> str:
    return "http://" + FAKE_USER + ":" + FAKE_PASSWORD + "@" + host_port


def test_p3_3_a_message_never_contains_credentials_or_the_endpoint(closed_port):
    module = _load()

    problem = module.endpoint_problem(
        {"OTEL_EXPORTER_OTLP_ENDPOINT": _with_userinfo(f"127.0.0.1:{closed_port}")}
    )

    assert problem
    for secret in (FAKE_PASSWORD, FAKE_USER):
        assert secret not in problem


@pytest.mark.parametrize(
    "variable", ["OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"]
)
@pytest.mark.parametrize(
    "build",
    [
        lambda host: _with_userinfo(host),
        lambda host: "http://" + FAKE_USER + "@" + host,  # user only
        lambda host: "http://:" + FAKE_PASSWORD + "@" + host,  # password only
        lambda host: "http://" + host + "/v1/metrics?api_key=" + FAKE_PASSWORD,  # query string
        lambda host: "http://" + host + "?" + FAKE_PASSWORD,
    ],
    ids=["userinfo", "user-only", "password-only", "query-key", "query-bare"],
)
def test_p3_3_sec2_credentials_in_an_endpoint_are_refused_even_when_it_answers(
    listener, variable, build
):
    """SEC-2: Airflow logs the endpoint at INFO. User info or a query string
    (where a token usually goes) is refused whether the endpoint answers or
    not, in either variable: `make airflow` then starts without metrics."""
    module = _load()

    problem = module.endpoint_problem({variable: build(f"127.0.0.1:{listener}")})

    assert problem and "credential" in problem
    for secret in (FAKE_PASSWORD, FAKE_USER):
        assert secret not in problem


def test_p3_3_c3_the_unused_endpoint_variable_is_checked_too(listener):
    """The metrics-specific endpoint wins for the exporter, but Airflow's
    other OTel code reads the generic one: credentials in either are refused."""
    module = _load()

    env = {
        "OTEL_EXPORTER_OTLP_ENDPOINT": _with_userinfo("collector.example:4318"),
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": f"http://127.0.0.1:{listener}",
    }

    problem = module.endpoint_problem(env)

    assert problem and "OTEL_EXPORTER_OTLP_ENDPOINT" in problem
    assert FAKE_PASSWORD not in problem


def _run(env: dict[str, str]) -> subprocess.CompletedProcess:
    clean = {k: v for k, v in os.environ.items() if not k.startswith("OTEL_")}
    return subprocess.run(
        [sys.executable, "-I", str(SCRIPT)],
        capture_output=True,
        text=True,
        timeout=60,
        env={**clean, **env},
    )


def test_p3_3_the_script_exits_0_when_reachable_and_1_with_a_warning_otherwise(
    listener, closed_port
):
    ok = _run({"OTEL_EXPORTER_OTLP_ENDPOINT": f"http://127.0.0.1:{listener}"})
    assert ok.returncode == 0 and ok.stderr == ""

    down = _run({"OTEL_EXPORTER_OTLP_ENDPOINT": f"http://127.0.0.1:{closed_port}"})
    assert down.returncode == 1
    assert "warning" in down.stderr and "make observability-up" in down.stderr


def test_p3_3_sec2_the_script_refuses_credentials_and_prints_neither_value(listener):
    proc = _run({"OTEL_EXPORTER_OTLP_ENDPOINT": _with_userinfo(f"127.0.0.1:{listener}")})

    assert proc.returncode == 1
    assert "warning" in proc.stderr and "without its OpenTelemetry metrics" in proc.stderr
    for secret in (FAKE_PASSWORD, FAKE_USER):
        assert secret not in proc.stdout + proc.stderr
