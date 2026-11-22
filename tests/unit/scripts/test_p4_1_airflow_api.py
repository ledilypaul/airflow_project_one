"""
P4-1 - `scripts/airflow_api.py`: list the DAGs, read the runs of one, trigger one, through
Airflow 3's REST API.

A local HTTP server plays Airflow and records what it receives. The shapes it answers with
(`/auth/token`, `dags`, `dagRuns`, `logical_date` required in the trigger body) were read in
the installed Airflow 3.4.0 (`airflow/api_fastapi`).
"""

import importlib.util
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SCRIPT = PROJECT_ROOT / "scripts" / "airflow_api.py"
PASSWORD = "s3cret-pw"
TOKEN = "jwt-token-value"


def _load():
    spec = importlib.util.spec_from_file_location("airflow_api", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


api = _load()


class _Airflow:
    """Fake API: `routes` maps (method, path) to (status, body, headers)."""

    def __init__(self):
        self.requests = []
        self.routes = {}
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def _answer(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length)) if length else None
                owner.requests.append(
                    (self.command, self.path, self.headers.get("Authorization"), body)
                )
                status, payload, headers = owner.routes.get(
                    (self.command, self.path), (404, {"detail": "Not Found"}, {})
                )
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                for key, value in headers.items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode())

            do_GET = do_POST = _answer

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.routes[("POST", "/auth/token")] = (201, {"access_token": TOKEN}, {})

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def airflow():
    fake = _Airflow()
    yield fake
    fake.close()


def _env(fake, **extra):
    return {"AIRFLOW_API_URL": fake.url, "AIRFLOW_API_PASSWORD": PASSWORD, **extra}


def test_p4_1_dags_logs_in_then_lists_the_dags_with_their_paused_flag(airflow, capsys):
    airflow.routes[("GET", "/api/v2/dags?limit=100&order_by=dag_id")] = (
        200,
        {
            "dags": [
                {"dag_id": "list_files_dag", "is_paused": False},
                {"dag_id": "x", "is_paused": True},
            ]
        },
        {},
    )
    assert api.main(["dags"], _env(airflow)) == 0
    assert capsys.readouterr().out.splitlines() == ["list_files_dag\tactive", "x\tpaused"]
    # the password goes in the login body only, the token in the Authorization header after it
    login, listing = airflow.requests
    assert login == ("POST", "/auth/token", None, {"username": "admin", "password": PASSWORD})
    assert listing[2] == f"Bearer {TOKEN}"


def test_p4_1_a_ready_token_skips_the_login(airflow, capsys):
    airflow.routes[("GET", "/api/v2/dags?limit=100&order_by=dag_id")] = (200, {"dags": []}, {})
    env = {"AIRFLOW_API_URL": airflow.url, "AIRFLOW_API_TOKEN": "mine"}
    assert api.main(["dags"], env) == 0
    assert [r[:2] for r in airflow.requests] == [("GET", "/api/v2/dags?limit=100&order_by=dag_id")]
    assert airflow.requests[0][2] == "Bearer mine"


def test_p4_1_runs_prints_the_latest_runs_of_the_dag(airflow, capsys):
    path = "/api/v2/dags/read_file_dag/dagRuns?limit=2&order_by=-run_after"
    airflow.routes[("GET", path)] = (
        200,
        {
            "dag_runs": [
                {"dag_run_id": "r1", "state": "success", "start_date": "t0", "end_date": "t1"},
                {"dag_run_id": "r2", "state": "running", "start_date": "t2", "end_date": None},
            ]
        },
        {},
    )
    assert api.main(["runs", "read_file_dag", "--limit", "2"], _env(airflow)) == 0
    assert capsys.readouterr().out.splitlines() == ["r1\tsuccess\tt0\tt1", "r2\trunning\tt2\tNone"]


def test_p4_1_trigger_posts_a_null_logical_date_and_the_conf(airflow, capsys):
    airflow.routes[("GET", "/api/v2/dags/read_file_dag")] = (200, {"is_paused": False}, {})
    airflow.routes[("POST", "/api/v2/dags/read_file_dag/dagRuns")] = (
        200,
        {"dag_run_id": "manual__1", "state": "queued"},
        {},
    )
    argv = ["trigger", "read_file_dag", "--conf", '{"run_id": "abc"}']
    assert api.main(argv, _env(airflow)) == 0
    assert capsys.readouterr().out == "manual__1\tqueued\n"
    # `logical_date` has no default in the API's body: it must be sent, as null
    assert airflow.requests[-1][3] == {"logical_date": None, "conf": {"run_id": "abc"}}


def test_p4_1_trigger_refuses_a_paused_dag_unless_told_otherwise(airflow, capsys):
    airflow.routes[("GET", "/api/v2/dags/read_file_dag")] = (200, {"is_paused": True}, {})
    airflow.routes[("POST", "/api/v2/dags/read_file_dag/dagRuns")] = (
        200,
        {"dag_run_id": "manual__2", "state": "queued"},
        {},
    )
    assert api.main(["trigger", "read_file_dag"], _env(airflow)) == 1
    assert "paused" in capsys.readouterr().err
    assert not [r for r in airflow.requests if r[0] == "POST" and "dagRuns" in r[1]]
    assert api.main(["trigger", "read_file_dag", "--even-if-paused"], _env(airflow)) == 0


def test_p4_1_an_api_error_exits_1_and_never_prints_the_credentials(airflow, capsys):
    assert api.main(["runs", "nope"], _env(airflow)) == 1
    captured = capsys.readouterr()
    assert "HTTP 404" in captured.err and "Not Found" in captured.err
    assert PASSWORD not in captured.out + captured.err
    assert TOKEN not in captured.out + captured.err


def test_p4_1_an_unreachable_api_exits_1_without_the_traceback(capsys):
    # a port that was just released: the connection is refused at once
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    env = {"AIRFLOW_API_URL": f"http://127.0.0.1:{port}", "AIRFLOW_API_PASSWORD": PASSWORD}
    assert api.main(["dags"], env) == 1
    err = capsys.readouterr().err
    assert err.startswith(f"error: cannot use the API at 127.0.0.1:{port}")
    assert "Traceback" not in err


def test_p4_1_a_redirect_is_not_followed_so_the_token_never_reaches_another_host(airflow, capsys):
    airflow.routes[("GET", "/api/v2/dags?limit=100&order_by=dag_id")] = (
        302,
        {},
        {"Location": f"{airflow.url}/elsewhere"},
    )
    assert api.main(["dags"], _env(airflow)) == 1
    assert "HTTP 302" in capsys.readouterr().err
    assert not [r for r in airflow.requests if r[1] == "/elsewhere"]


def test_p4_1_missing_password_exits_2_before_any_request(airflow, capsys):
    assert api.main(["dags"], {"AIRFLOW_API_URL": airflow.url}) == 2
    assert "AIRFLOW_API_PASSWORD" in capsys.readouterr().err
    assert airflow.requests == []


@pytest.mark.parametrize(
    "url",
    [
        "http://airflow.example.com",  # clear text outside the local machine
        "ftp://localhost",
        "http://admin:pw@localhost:8080",  # user info
        "http://localhost:8080?token=x",  # query string
        "localhost:8080",  # no scheme
    ],
)
def test_p4_1_refuses_an_unsafe_url_before_sending_anything(url, capsys):
    env = {"AIRFLOW_API_URL": url, "AIRFLOW_API_PASSWORD": PASSWORD}
    assert api.main(["dags"], env) == 2
    err = capsys.readouterr().err
    assert "AIRFLOW_API_URL" in err and PASSWORD not in err and "pw@" not in err


def test_p4_1_https_is_accepted_for_a_remote_host():
    assert api.check_base_url("https://airflow.example.com/") == "https://airflow.example.com"


@pytest.mark.parametrize("conf", ["not json", "[1, 2]", '"text"'])
def test_p4_1_conf_must_be_a_json_object(conf, capsys):
    with pytest.raises(SystemExit) as exit_info:
        api.main(["trigger", "d", "--conf", conf], {"AIRFLOW_API_PASSWORD": PASSWORD})
    assert exit_info.value.code == 2
    assert "--conf" in capsys.readouterr().err


def _make(*args):
    import subprocess

    return subprocess.run(
        ["make", *args], cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=60
    )


@pytest.mark.parametrize("target", ["api-runs", "api-trigger"])
def test_p4_1_make_targets_need_a_dag(target):
    result = _make(target)
    assert result.returncode != 0  # make reports a failed recipe as 2
    assert "DAG is empty" in result.stderr


def test_p4_1_make_never_puts_the_values_on_the_command_line():
    # DAG and CONF reach the script through the environment (like RUN_ID for spark-job):
    # a quote or a space in them cannot break out of the command.
    result = _make("-n", "api-trigger", "DAG=read_file_dag", 'CONF={"run_id": "a b"}')
    assert result.returncode == 0
    assert "scripts/airflow_api.py trigger" in result.stdout
    assert "read_file_dag" not in result.stdout and "a b" not in result.stdout
