"""Client for Airflow 3's REST API (P4-1): list the DAGs, read the runs of one, trigger one.

Used through ``make api-dags``, ``make api-runs DAG=...`` and ``make api-trigger DAG=...``.
Airflow 3 serves the API itself (``/api/v2``); this only wraps the three calls the project
needs, with the authentication of the Simple Auth Manager.

Configuration, by environment only (a password on a command line shows in ``ps`` and in
the shell history):

- ``AIRFLOW_API_URL``: default ``http://localhost:8080`` (``http://127.0.0.1:8081`` for
  ``make k8s-ui``). ``https`` is required outside the local machine, and a URL with user
  info or a query string is refused: the credentials never travel in clear text or in an URL.
- ``AIRFLOW_API_TOKEN``: a JWT you already have. Otherwise ``AIRFLOW_API_USER`` (default
  ``admin``) and ``AIRFLOW_API_PASSWORD`` are exchanged for one at ``POST /auth/token``.

Redirections are not followed (urllib would resend the token to the new host). Output: one
tab-separated line per item. Exit 0 on success, 1 when the API refuses or cannot be reached,
2 on a wrong argument or configuration. The password and the token are never printed.
Standard library only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import quote, urlsplit

DEFAULT_URL = "http://localhost:8080"
TIMEOUT_SECONDS = 30
LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}


class ConfigError(Exception):
    """A wrong argument or environment: nothing was sent."""


class ApiError(Exception):
    """The API refused the call, or did not answer."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None  # the 3xx then surfaces as an HTTPError


_OPENER = urllib.request.build_opener(_NoRedirect)


def check_base_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        host = parts.hostname
    except ValueError as exc:
        raise ConfigError("AIRFLOW_API_URL is not a valid URL") from exc
    if parts.scheme not in ("http", "https") or not host:
        raise ConfigError("AIRFLOW_API_URL must be an http(s) URL with a host")
    if "@" in parts.netloc or parts.query or parts.fragment:
        raise ConfigError("AIRFLOW_API_URL must not carry user info, a query string or a fragment")
    if parts.scheme == "http" and host not in LOOPBACK_HOSTS:
        raise ConfigError("AIRFLOW_API_URL: http is only accepted for localhost, use https")
    return url.rstrip("/")


def _request(method: str, base: str, path: str, token: str | None = None, body: Any = None) -> Any:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(base + path, data=data, method=method, headers=headers)
    try:
        with _OPENER.open(request, timeout=TIMEOUT_SECONDS) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            raw = json.load(exc).get("detail")
            detail = raw if isinstance(raw, str) else ""
        except (ValueError, AttributeError):
            pass
        suffix = f": {detail[:200]}" if detail else ""
        raise ApiError(f"HTTP {exc.code} on {method} {path}{suffix}") from exc
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise ApiError(
            f"cannot use the API at {urlsplit(base).netloc}: {type(exc).__name__}"
        ) from exc


def get_token(base: str, env: Mapping[str, str]) -> str:
    if env.get("AIRFLOW_API_TOKEN"):
        return env["AIRFLOW_API_TOKEN"]
    password = env.get("AIRFLOW_API_PASSWORD")
    if not password:
        raise ConfigError("set AIRFLOW_API_PASSWORD (or AIRFLOW_API_TOKEN) in the environment")
    body = {"username": env.get("AIRFLOW_API_USER", "admin"), "password": password}
    return _request("POST", base, "/auth/token", body=body)["access_token"]


def _dag_path(dag_id: str) -> str:
    return f"/api/v2/dags/{quote(dag_id, safe='')}"


def cmd_dags(base: str, token: str, args: argparse.Namespace) -> None:
    for dag in _request("GET", base, "/api/v2/dags?limit=100&order_by=dag_id", token)["dags"]:
        print(dag["dag_id"], "paused" if dag["is_paused"] else "active", sep="\t")


def cmd_runs(base: str, token: str, args: argparse.Namespace) -> None:
    path = f"{_dag_path(args.dag)}/dagRuns?limit={args.limit}&order_by=-run_after"
    for run in _request("GET", base, path, token)["dag_runs"]:
        print(run["dag_run_id"], run["state"], run["start_date"], run["end_date"], sep="\t")


def cmd_trigger(base: str, token: str, args: argparse.Namespace) -> None:
    if _request("GET", base, _dag_path(args.dag), token)["is_paused"] and not args.even_if_paused:
        # Airflow accepts the run but never starts it (see CLAUDE.md, "Running Airflow locally").
        raise ApiError(f"{args.dag} is paused: unpause it first, or pass --even-if-paused")
    # `logical_date` has no default in the API's body: null is required.
    body = {"logical_date": None, "conf": args.conf}
    run = _request("POST", base, f"{_dag_path(args.dag)}/dagRuns", token, body)
    print(run["dag_run_id"], run["state"], sep="\t")


def _conf(value: str) -> dict:
    try:
        conf = json.loads(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--conf must be JSON") from exc
    if not isinstance(conf, dict):
        raise argparse.ArgumentTypeError("--conf must be a JSON object")
    return conf


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Airflow 3 REST API: DAGs, runs, trigger.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("dags", help="list the DAGs and whether they are paused").set_defaults(
        run=cmd_dags
    )
    runs = commands.add_parser("runs", help="the latest runs of a DAG")
    runs.add_argument("dag")
    runs.add_argument("--limit", type=int, default=5)
    runs.set_defaults(run=cmd_runs)
    trigger = commands.add_parser("trigger", help="start a run of a DAG")
    trigger.add_argument("dag")
    trigger.add_argument(
        "--conf", type=_conf, default={}, help='JSON object, e.g. \'{"run_id": "..."}\''
    )
    trigger.add_argument("--even-if-paused", action="store_true")
    trigger.set_defaults(run=cmd_trigger)
    return parser


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    env = os.environ if env is None else env
    args = build_parser().parse_args(argv)  # exits 2 itself on a wrong argument
    try:
        base = check_base_url(env.get("AIRFLOW_API_URL", DEFAULT_URL))
        token = get_token(base, env)
        args.run(base, token, args)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except ApiError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
