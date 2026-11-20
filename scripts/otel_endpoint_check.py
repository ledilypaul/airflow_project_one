"""Check the OpenTelemetry endpoint before Airflow's metrics are turned on (P3-3).

Called by ``make airflow`` when the metrics are about to be enabled. Exit 0:
the endpoint is usable. Exit 1, with a warning on stderr: it is not, and
``make airflow`` then starts Airflow without metrics.

Two reasons to refuse:

- **Credentials in the endpoint** (user info, or a query string where a token
  usually goes), in ``OTEL_EXPORTER_OTLP_ENDPOINT`` or
  ``OTEL_EXPORTER_OTLP_METRICS_ENDPOINT``: Airflow logs the endpoint at INFO
  (``Connecting to OpenTelemetry Collector at <endpoint>``). Credentials go in
  ``OTEL_EXPORTER_OTLP_HEADERS``. Refused even when the endpoint answers, and
  both variables are checked: the exporter uses the metrics one, but a value in
  the generic one is just as likely to reach a log.
- **No connection** (collector not started): it never fails a task, but each
  Airflow process then waits for the exporter's final flush when it exits
  (about 10 s per task, measured, see
  docs/architecture/2026-11-17-p3-3-observability.md) and logs an error per
  export. Better to start without metrics, and say so. It cannot help when the
  collector dies later: that costs the same delay, and nothing else.

The endpoint is read the way Airflow's exporter reads it (the metrics one
before the generic one). The warning never contains the endpoint, only the
name of the variable, and for a connection problem the host and port (never
the user info). Never raises. Standard library only.
"""

from __future__ import annotations

import os
import socket
import sys
from collections.abc import Mapping
from urllib.parse import urlsplit

TIMEOUT_SECONDS = 2.0
DEFAULT_PORTS = {"http": 80, "https": 443}
GENERIC = "OTEL_EXPORTER_OTLP_ENDPOINT"
METRICS = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"


def _has_credentials(value: str) -> bool:
    """User info (`<user>:<password>@host`, `<user>@host`, `:<password>@host`) or a query string."""
    if "?" in value:
        return True
    try:
        return "@" in urlsplit(value).netloc
    except ValueError:  # malformed: reported later, as such
        return False


def endpoint_problem(environ: Mapping[str, str]) -> str | None:
    """Why the endpoint of `environ` is not usable, or None when it carries no
    credentials and accepts a TCP connection. The text never contains the
    endpoint's value."""
    for name in (METRICS, GENERIC):
        if _has_credentials((environ.get(name) or "").strip()):
            return (
                f"{name} contains credentials (user info or a query string), "
                "which Airflow would log: use OTEL_EXPORTER_OTLP_HEADERS"
            )
    name = METRICS if (environ.get(METRICS) or "").strip() else GENERIC
    endpoint = (environ.get(name) or "").strip()
    if not endpoint:
        return f"no {GENERIC}"
    try:
        parts = urlsplit(endpoint)
        host, port = parts.hostname, parts.port
    except ValueError:
        return f"{name} is not a valid URL"
    if parts.scheme not in DEFAULT_PORTS or not host:
        return f"{name} is not an http(s) URL with a host"
    port = port or DEFAULT_PORTS[parts.scheme]
    try:
        with socket.create_connection((host, port), timeout=TIMEOUT_SECONDS):
            return None
    except (UnicodeError, ValueError):  # a host that is not a valid DNS name
        return f"{name} has an invalid host or port"
    except OSError as exc:
        # host and port only: never the user info (there is none at this point).
        return f"cannot connect to {host[:100]}:{port} ({exc.__class__.__name__})"


def main() -> int:
    try:
        problem = endpoint_problem(os.environ)
    except Exception:  # never raise: a check that crashes would stop `make airflow`
        problem = "the endpoint could not be checked"
    if problem is None:
        return 0
    hint = " (is the collector up? `make observability-up`)" if "cannot connect" in problem else ""
    print(
        f"warning: {problem}: Airflow starts without its OpenTelemetry metrics{hint}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
