"""
P2-5 — guards on the lineage setup around the code.

- docker-compose.yaml: Marquez is only started with the `lineage` profile
  (`make lineage-up`), never by `make db-up`; images pinned by version; host
  ports away from the project's (5434, 6379, 8080).
- Makefile: the openlineage-spark jar is pinned by version and SHA-256, for
  Scala 2.13 (the Scala version of PySpark 4).
- The events and the jar are never committed; CI requires the lineage tests.
"""

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MARQUEZ_SERVICES = {"marquez-db", "marquez-api", "marquez-web"}
PROJECT_HOST_PORTS = {"5434", "6379", "8080"}


def _compose():
    return yaml.safe_load((PROJECT_ROOT / "docker-compose.yaml").read_text())


def test_p2_5_marquez_only_runs_with_the_lineage_profile():
    services = _compose()["services"]
    assert MARQUEZ_SERVICES <= set(services)
    for name, service in services.items():
        if name in MARQUEZ_SERVICES:
            assert service.get("profiles") == ["lineage"], name
        else:  # `make db-up` (docker compose up -d) starts these only.
            assert "profiles" not in service, name


def test_p2_5_marquez_images_are_pinned_and_ports_do_not_collide():
    services = _compose()["services"]
    host_ports = []
    for name in MARQUEZ_SERVICES:
        image = services[name]["image"]
        # SEC-5: a tag for the reader, a digest for the pull.
        assert re.fullmatch(r"[\w./-]+:\d+(\.\d+)*@sha256:[0-9a-f]{64}", image), image
        host_ports += [str(p).split(":")[-2] for p in services[name].get("ports", [])]
    assert host_ports and not set(host_ports) & PROJECT_HOST_PORTS, host_ports
    assert "ports" not in services["marquez-db"]  # no host port for its database


def test_p2_5_sec3_marquez_listens_on_localhost_only_on_its_own_network():
    compose = _compose()
    services = compose["services"]
    published = [p for name in MARQUEZ_SERVICES for p in services[name].get("ports", [])]
    assert sorted(published) == ["127.0.0.1:3002:3000", "127.0.0.1:5002:5000"]
    for name in MARQUEZ_SERVICES:
        assert services[name]["networks"] == ["lineage"], name
    assert "lineage" in compose["networks"]
    for name in ("postgres", "redis"):  # untouched (P3-1)
        assert "networks" not in services[name], name
    # The web UI reaches the API by its service name, on that network.
    assert services["marquez-web"]["environment"]["MARQUEZ_HOST"] == "marquez-api"


def test_p2_5_listener_jar_is_pinned():
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    version = re.search(r"^OPENLINEAGE_SPARK_VERSION := (\S+)$", makefile, re.MULTILINE)
    sha256 = re.search(r"^OPENLINEAGE_SPARK_SHA256 := (\S+)$", makefile, re.MULTILINE)
    assert version and re.fullmatch(r"\d+\.\d+\.\d+", version.group(1))
    assert sha256 and re.fullmatch(r"[0-9a-f]{64}", sha256.group(1))
    # Scala 2.13: the only Scala version of PySpark 4 (a 2.12 jar fails at
    # the first query with NoSuchMethodError and stops the SparkContext).
    assert "openlineage-spark_2.13" in makefile
    assert "openlineage-spark_2.12" not in makefile


def test_p2_5_lineage_artefacts_are_gitignored():
    ignored = (PROJECT_ROOT / ".gitignore").read_text().splitlines()
    assert "lineage/" in ignored


def test_p2_5_ci_requires_the_lineage_tests():
    ci = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert "make lineage-jar" in ci
    assert 'REQUIRE_LINEAGE: "1"' in ci
    # SEC-2: the cache key changes with the pinned digest, not only the version.
    assert "OPENLINEAGE_SPARK_SHA256" in ci


def _makefile() -> str:
    return (PROJECT_ROOT / "Makefile").read_text()


def _make(*args: str) -> subprocess.CompletedProcess:
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    return subprocess.run(
        ["make", "--no-print-directory", "-s", "-C", str(PROJECT_ROOT), *args],
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_p2_5_sec6_jar_download_is_https_only_and_always_verified():
    makefile = _makefile()
    recipe = makefile[makefile.index("lineage-jar:") :].split("\n\n")[0]
    assert "--proto '=https' --proto-redir '=https'" in recipe
    # A present file is re-checked, removed and downloaded again on mismatch.
    assert recipe.count("sha256sum -c") >= 2
    assert "rm -f" in recipe
    assert "Provenance" in makefile or "provenance" in makefile


@pytest.mark.parametrize(
    ("variables", "expected"),
    [
        (
            {"LINEAGE_DIR": '/tmp/a"b\\c', "OPENLINEAGE_URL": ""},
            {"type": "file", "log_file_path": '/tmp/a"b\\c/events/airflow', "append": False},
        ),
        (
            {"OPENLINEAGE_URL": 'http://localhost:5002/"},"x":{"y'},
            {"type": "http", "url": 'http://localhost:5002/"},"x":{"y'},
        ),
    ],
    ids=["file", "http"],
)
def test_p2_5_c2_sec4_makefile_transport_is_valid_json(variables, expected):
    """Built with json.dumps, never by interpolation: `"` or `\\` in
    LINEAGE_DIR / OPENLINEAGE_URL cannot break or inject into the JSON."""
    args = [f"{k}={v}" for k, v in variables.items()]
    proc = _make("lineage-transport", f"PYTHON={sys.executable}", *args)
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout) == expected


def test_p2_5_c6_airflow_targets_raise_the_lineage_collector_limit():
    match = re.search(r"export AIRFLOW__LINEAGE__MAX_ASSETS_PER_COLLECTOR := (\S+)", _makefile())
    assert match and int(match.group(1)) >= 1000


def test_p2_5_c13_lineage_clean_removes_only_the_events():
    makefile = _makefile()
    assert re.search(r"^lineage-clean:", makefile, re.MULTILINE)
    recipe = makefile[makefile.index("lineage-clean:") :].split("\n\n")[0]
    assert "LINEAGE_EVENTS_DIR" in recipe and "jars" not in recipe
