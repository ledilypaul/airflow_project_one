"""
P3-3 - the alert rules, checked and unit-tested by `promtool`.

`promtool test rules` (deploy/observability/tests/airflow_rules_test.yml)
replays series through the real PromQL engine and checks which alerts fire
at which time: the first failure of a DAG, a frozen or vanished scheduler, a
task waiting behind a running one...

promtool comes from, in this order:
- a `promtool` binary on the PATH;
- the Prometheus image of docker-compose.yaml (the same tag and digest
  `make observability-up` runs), through `docker run`.
With neither, the tests are skipped, unless REQUIRE_PROMTOOL=1 turns the skip
into a failure (the structure of the rules is checked without it by
tests/unit/test_p3_3_alert_rules.py).
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OBSERVABILITY_DIR = PROJECT_ROOT / "deploy" / "observability"
RULES = OBSERVABILITY_DIR / "rules" / "airflow.yml"
RULES_TESTS = OBSERVABILITY_DIR / "tests" / "airflow_rules_test.yml"


def _prometheus_image() -> str:
    compose = yaml.safe_load((PROJECT_ROOT / "docker-compose.yaml").read_text())
    return compose["services"]["prometheus"]["image"]


def _docker_usable() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=60).returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def _to_host(arg: str, mounts: dict[Path, str]) -> str:
    """`arg` with a container path replaced by its host path (local binary)."""
    for host, container in mounts.items():
        if arg == container or arg.startswith(container + "/"):
            return str(host) + arg[len(container) :]
    return arg


def _promtool(*args: str, mounts: dict[Path, str] | None = None) -> subprocess.CompletedProcess:
    """Run promtool with `args`; `mounts` maps host paths to the path they
    have in the container (docker mode) and are translated in `args`."""
    mounts = mounts or {}
    if shutil.which("promtool"):
        return subprocess.run(
            ["promtool", *(_to_host(a, mounts) for a in args)],
            capture_output=True,
            text=True,
            timeout=300,
        )
    if not _docker_usable():
        message = "neither promtool nor a usable Docker daemon"
        if os.environ.get("REQUIRE_PROMTOOL") == "1":
            pytest.fail(f"REQUIRE_PROMTOOL=1 but {message}")
        pytest.skip(message)
    volumes = [f"{host}:{container}:ro" for host, container in mounts.items()]
    command = ["docker", "run", "--rm", "--network", "none", "--entrypoint", "promtool"]
    for volume in volumes:
        command += ["-v", volume]
    return subprocess.run(
        [*command, _prometheus_image(), *args], capture_output=True, text=True, timeout=600
    )


def test_p3_3_the_alert_rules_are_valid_promql():
    proc = _promtool("check", "rules", "/w/airflow.yml", mounts={RULES: "/w/airflow.yml"})

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "SUCCESS: 3 rules found" in proc.stdout + proc.stderr


def test_p3_3_the_alert_rules_behave_as_documented():
    """Fires on the first failure and on a frozen or vanished scheduler, stays
    quiet on a healthy one, resolves 15 minutes after a failure."""
    proc = _promtool(
        "test",
        "rules",
        "/w/tests/airflow_rules_test.yml",
        mounts={OBSERVABILITY_DIR: "/w"},
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "SUCCESS" in proc.stdout + proc.stderr


def test_p3_3_prometheus_accepts_its_configuration_with_the_mounted_rules():
    """The configuration as the container sees it: the paths of prometheus.yml
    and of the rules directory, as docker-compose.yaml mounts them."""
    if shutil.which("promtool"):
        pytest.skip("needs the container's paths: runs in Docker mode only")
    proc = _promtool(
        "check",
        "config",
        "/etc/prometheus/prometheus.yml",
        mounts={
            OBSERVABILITY_DIR / "prometheus.yml": "/etc/prometheus/prometheus.yml",
            OBSERVABILITY_DIR / "rules": "/etc/prometheus/rules",
        },
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "SUCCESS" in proc.stdout + proc.stderr
    assert "1 rule files found" in proc.stdout + proc.stderr


def test_p3_3_a_broken_rule_is_caught(tmp_path):
    """The check is not vacuous: an expression that does not parse fails."""
    broken = tmp_path / "broken.yml"
    broken.write_text("groups:\n  - name: x\n    rules:\n      - alert: A\n        expr: 'sum(('\n")

    proc = _promtool("check", "rules", "/w/broken.yml", mounts={broken: "/w/broken.yml"})

    assert proc.returncode != 0
