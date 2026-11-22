"""P3-2: the Kubernetes targets of the Makefile and the kind configuration.

- Every tool, the node image and the Airflow chart are pinned (version and
  SHA-256 / digest) and downloaded over HTTPS only.
- Every kubectl / helm command names the kind context explicitly: a target
  never acts on another cluster of ~/.kube/config.
- Nothing is published outside 127.0.0.1 (API server, port-forward).
- Optional: `make check`, `make test` and `make airflow` never reach a
  Kubernetes target (dry run).
"""

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MAKEFILE = PROJECT_ROOT / "Makefile"
KIND_CONFIG = PROJECT_ROOT / "deploy" / "k8s" / "kind-config.yaml"

K8S_TARGETS = (
    "k8s-tools",
    "kind-up",
    "kind-down",
    "image",
    "kind-load",
    "k8s-chart",
    "k8s-install",
    "k8s-uninstall",
    "k8s-load-data",
    "k8s-spark-clean",
    "k8s-ui",
    "k8s-ui-password",
)
SHA256 = re.compile(r"[0-9a-f]{64}")


def _text() -> str:
    return MAKEFILE.read_text()


def _var(name: str) -> str:
    match = re.search(rf"^{name} [:?]?= (\S+)$", _text(), re.MULTILINE)
    assert match, name
    return match.group(1)


def _recipe(target: str) -> str:
    """Recipe lines of `target` (tab-indented lines after its rule)."""
    lines = _text().splitlines()
    start = next(
        i
        for i, line in enumerate(lines)
        if re.match(rf"^{re.escape(target)}:(?!=)", line) and "export" not in line
    )
    body = []
    for line in lines[start + 1 :]:
        if line.startswith("\t") or (body and line.startswith(" ")):
            body.append(line)
        elif line.strip() == "" and not body:
            continue
        else:
            break
    return "\n".join(body)


def test_p3_2_targets_exist_documented_and_phony():
    text = _text()
    phony = " ".join(re.findall(r"^\.PHONY:((?:.*\\\n)*.*)$", text, re.MULTILINE)).replace("\\", "")
    for target in K8S_TARGETS:
        assert re.search(rf"^{re.escape(target)}:[^=]*## ", text, re.MULTILINE), target
        assert target in phony.split(), target


@pytest.mark.parametrize(
    ("version", "digest"),
    [
        ("KIND_VERSION", "KIND_SHA256"),
        ("KUBECTL_VERSION", "KUBECTL_SHA256"),
        ("HELM_VERSION", "HELM_SHA256"),
        ("HELM_VERSION", "HELM_BIN_SHA256"),
        ("AIRFLOW_CHART_VERSION", "AIRFLOW_CHART_SHA256"),
    ],
)
def test_p3_2_tools_and_chart_are_pinned(version, digest):
    assert re.fullmatch(r"v?\d+\.\d+\.\d+", _var(version))
    assert SHA256.fullmatch(_var(digest))


def test_p3_2_node_image_is_pinned_by_digest():
    assert re.fullmatch(r"kindest/node:v\d+\.\d+\.\d+@sha256:[0-9a-f]{64}", _var("KIND_NODE_IMAGE"))
    # kubectl stays within one minor version of the cluster (skew policy).
    node_minor = int(re.search(r":v1\.(\d+)\.", _var("KIND_NODE_IMAGE")).group(1))
    kubectl_minor = int(re.match(r"v1\.(\d+)\.", _var("KUBECTL_VERSION")).group(1))
    assert abs(node_minor - kubectl_minor) <= 1


@pytest.mark.parametrize("target", ["k8s-tools", "k8s-chart"])
def test_p3_2_downloads_are_https_only_and_checked(target):
    recipe = _recipe(target)
    for line in re.findall(r"curl [^\n]*", recipe):
        assert "--proto '=https' --proto-redir '=https'" in line, line
    assert "sha256sum -c" in recipe
    for url in re.findall(r"https?://\S+", recipe):
        assert url.startswith("https://"), url
    assert "https://" in _var("AIRFLOW_CHART_URL")


def test_p3_2_every_kubectl_and_helm_call_names_the_kind_context():
    calls = []
    for target in K8S_TARGETS:
        recipe = _recipe(target).replace("\\\n", " ")
        calls += re.findall(r"\$\((?:KUBECTL|HELM)\)[^\n|;]*", recipe)
    assert calls
    for call in calls:
        assert (
            "$(K8S_ARGS)" in call or "$(HELM_ARGS)" in call or "--context '$(K8S_CONTEXT)'" in call
        ), call
    assert _var("K8S_CONTEXT") == "kind-$(KIND_CLUSTER)"


def test_p3_2_port_forward_binds_localhost_only():
    recipe = _recipe("k8s-ui")
    assert "port-forward --address 127.0.0.1 " in recipe


def test_p3_2_kind_cluster_is_local_and_sober():
    config = yaml.safe_load(KIND_CONFIG.read_text())
    assert config["kind"] == "Cluster"
    assert config["networking"]["apiServerAddress"] == "127.0.0.1"
    assert len(config["nodes"]) == 1
    for node in config["nodes"]:
        assert "extraPortMappings" not in node
        assert "extraMounts" not in node
    recipe = _recipe("kind-up")
    assert "--image '$(KIND_NODE_IMAGE)'" in recipe
    assert "--config deploy/k8s/kind-config.yaml" in recipe


def test_p3_2_install_passes_the_ddl_and_the_values_files():
    recipe = _recipe("k8s-install").replace("\\\n", " ")
    assert "--set-file fileListSql=utils/SQL/script_tables.sql" in recipe
    assert "-f deploy/k8s/values-airflow.yaml" in recipe
    assert "scripts/k8s_secrets.py" in recipe
    assert "pools import /opt/airflow/deploy/airflow/pools.json" in recipe
    # Secrets come before the charts that read them.
    assert recipe.index("k8s_secrets.py") < recipe.index("$(PROJECT_CHART)")
    assert recipe.index("$(PROJECT_CHART)") < recipe.index("values-airflow.yaml")


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
@pytest.mark.parametrize(
    "target",
    [["check"], ["test"], ["lint"], ["airflow"], ["airflow-init"], ["spark-job", "RUN_ID=x"]],
    ids=lambda t: " ".join(t),
)
def test_p3_2_quality_and_local_targets_never_reach_kubernetes(target):
    proc = subprocess.run(
        ["make", "-n", *target, "UV=uv"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    for word in ("kind ", "kubectl", "helm ", "docker build", "k8s_secrets"):
        assert word not in proc.stdout, (target, word)


def test_p3_2_namespace_enforces_the_restricted_pod_security_standard():
    """SEC-1: Pod Security Admission, level `restricted`, enforced, warned and
    audited, on the namespace, before anything is installed in it."""
    recipe = _recipe("k8s-install").replace("\\\n", " ")
    for mode in ("enforce", "warn", "audit"):
        assert f"pod-security.kubernetes.io/{mode}=$(PSA_LEVEL)" in recipe, mode
        assert f"pod-security.kubernetes.io/{mode}-version=$(PSA_VERSION)" in recipe, mode
    assert _var("PSA_LEVEL") == "restricted"
    assert re.fullmatch(r"v1\.\d+", _var("PSA_VERSION"))
    node_minor = re.search(r":v(1\.\d+)\.", _var("KIND_NODE_IMAGE")).group(1)
    assert _var("PSA_VERSION") == f"v{node_minor}"
    assert recipe.index("pod-security.kubernetes.io") < recipe.index("k8s_secrets.py")


def test_p3_2_project_chart_path_is_overridable():
    """C-2: a copy of the chart with another version can be installed by the
    same target (upgrade check on kind)."""
    assert _var("PROJECT_CHART") == "deploy/k8s/charts/project"
    assert "$(PROJECT_CHART)" in _recipe("k8s-install")


def test_p3_2_ui_password_is_read_from_its_secret():
    """SEC-3: the password is generated by scripts/k8s_secrets.py, not logged
    by the api-server; the target reads the Secret."""
    recipe = _recipe("k8s-ui-password")
    assert "get secret airflow-ui-admin" in recipe
    assert "exec" not in recipe
