"""P3-2: the manifests rendered by `helm template` for both releases.

Needs `helm` (PATH, or K8S_TOOLS_DIR / ~/.local/bin as installed by `make
k8s-tools`) and, for the official chart, its archive (`make k8s-chart`). Without
them the tests are skipped, unless REQUIRE_K8S=1, which turns the skip into a
failure. The schema check uses kubeconform (PATH, else its pinned image through
Docker), which downloads the Kubernetes JSON schemas: skipped offline, unless
REQUIRE_K8S=1.

Checked on the real render: RBAC minimal (namespaced Roles, no wildcard, no
Secret access), pods non-root without privilege, no Service published outside
the cluster, every image the project image or pinned by digest, the Spark
configuration of the worker pods, and the chart's own choices (KubernetesExecutor,
no bundled database).
"""

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
K8S_DIR = PROJECT_ROOT / "deploy" / "k8s"
PROJECT_CHART = K8S_DIR / "charts" / "project"
AIRFLOW_VALUES = K8S_DIR / "values-airflow.yaml"
DDL = PROJECT_ROOT / "utils" / "SQL" / "script_tables.sql"
MAKEFILE = PROJECT_ROOT / "Makefile"
NAMESPACE = "airflow-project-one"
REQUIRED = os.environ.get("REQUIRE_K8S") == "1"
# ghcr.io/yannh/kubeconform:v0.8.0, index digest read on 2026-11-19
# (`docker buildx imagetools inspect`).
KUBECONFORM_IMAGE = (
    "ghcr.io/yannh/kubeconform:v0.8.0"
    "@sha256:faffaf43f95aa6425306e1ab8d6fcad72acb9049158f38e574c085ea1ec0f64e"
)
PROJECT_IMAGE = "airflow-project-one:dev"
PROJECT_CHART_VERSION = yaml.safe_load((PROJECT_CHART / "Chart.yaml").read_text())["version"]
# RBAC of Spark on Kubernetes, (apiGroup, resource) -> verbs: exactly the rules
# with which a real run on kind logged no 403 (docs/architecture/2026-11-19-p3-2-kubernetes.md).
SPARK_DRIVER_GRANTS = {
    ("", "pods"): {"create", "get", "list", "watch", "delete", "deletecollection"},
}
SCHEDULER_GRANTS = {
    ("", "pods"): {"create", "get", "list", "watch", "patch", "delete"},
    ("", "events"): {"list", "watch"},
}
SPARK_SUBMIT_GRANTS = {
    ("", "pods"): {"create", "get", "list", "watch", "delete"},
    ("", "configmaps"): {"create", "patch", "delete"},
    ("", "services"): {"create", "patch", "delete"},
    ("networking.k8s.io", "networkpolicies"): {"create", "patch", "delete"},
}


def _skip(reason: str):
    if REQUIRED:
        pytest.fail(f"REQUIRE_K8S=1: {reason}")
    pytest.skip(reason)


def _make_var(name: str) -> str:
    return re.search(rf"^{name} :?= (\S+)$", MAKEFILE.read_text(), re.M).group(1)


def _tool(name: str) -> str | None:
    found = shutil.which(name)
    if found:
        return found
    tools_dir = Path(os.environ.get("K8S_TOOLS_DIR", Path.home() / ".local" / "bin"))
    candidate = tools_dir / name
    return str(candidate) if candidate.is_file() and os.access(candidate, os.X_OK) else None


@pytest.fixture(scope="module")
def helm():
    path = _tool("helm")
    if not path:
        _skip("helm not installed (make k8s-tools)")
    return path


def _render(helm, release, chart, *args) -> list[dict]:
    proc = subprocess.run(
        [helm, "template", release, str(chart), "--namespace", NAMESPACE, *args],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return [doc for doc in yaml.safe_load_all(proc.stdout) if doc]


@pytest.fixture(scope="module")
def project_docs(helm):
    return _render(helm, "project", PROJECT_CHART, "--set-file", f"fileListSql={DDL}")


@pytest.fixture(scope="module")
def airflow_docs(helm):
    chart = PROJECT_ROOT / ".k8s" / f"airflow-{_make_var('AIRFLOW_CHART_VERSION')}.tgz"
    if not chart.is_file():
        _skip(f"{chart.name} missing (make k8s-chart)")
    return _render(helm, "airflow", chart, "-f", str(AIRFLOW_VALUES))


def _by_kind(docs, kind):
    return [d for d in docs if d["kind"] == kind]


def _one(docs, kind, name):
    found = [d for d in docs if d["kind"] == kind and d["metadata"]["name"] == name]
    assert len(found) == 1, (kind, name)
    return found[0]


def _pod_specs(docs):
    for doc in docs:
        if doc["kind"] in ("Deployment", "StatefulSet", "Job", "DaemonSet"):
            yield doc["metadata"]["name"], doc["spec"]["template"]["spec"]
        elif doc["kind"] == "CronJob":
            yield doc["metadata"]["name"], doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
        elif doc["kind"] == "Pod":
            yield doc["metadata"]["name"], doc["spec"]


def _spark_defaults(project_docs) -> dict[str, str]:
    conf = _one(project_docs, "ConfigMap", "spark-conf")["data"]["spark-defaults.conf"]
    pairs = (line.split(None, 1) for line in conf.splitlines() if line.strip())
    return {key: value.strip() for key, value in pairs}


# --- Project chart --------------------------------------------------------------


def test_p3_2_project_chart_holds_no_secret(project_docs):
    assert not _by_kind(project_docs, "Secret")
    text = yaml.safe_dump_all(project_docs)
    assert "postgresql://" not in text


def test_p3_2_rbac_is_namespaced_and_minimal(project_docs):
    assert not _by_kind(project_docs, "ClusterRole")
    assert not _by_kind(project_docs, "ClusterRoleBinding")
    roles = {r["metadata"]["name"]: r for r in _by_kind(project_docs, "Role")}
    assert set(roles) == {"spark-driver", "spark-submit", "airflow-scheduler"}
    for role in roles.values():
        for rule in role["rules"]:
            for field in ("apiGroups", "resources", "verbs"):
                assert "*" not in rule[field], (role["metadata"]["name"], rule)
            assert set(rule["apiGroups"]) <= {"", "networking.k8s.io"}
            assert "secrets" not in rule["resources"]
            assert not {"escalate", "bind", "impersonate"} & set(rule["verbs"])

    def grants(role):
        return {
            (group, resource): set(rule["verbs"])
            for rule in roles[role]["rules"]
            for group in rule["apiGroups"]
            for resource in rule["resources"]
        }

    # Exactly what a real run on kind needed (see the template's comment).
    assert grants("spark-driver") == SPARK_DRIVER_GRANTS
    assert grants("spark-submit") == SPARK_SUBMIT_GRANTS
    # SEC-2: the scheduler launches the worker pods, nothing else; no pods/exec.
    assert grants("airflow-scheduler") == SCHEDULER_GRANTS
    bindings = {b["metadata"]["name"]: b for b in _by_kind(project_docs, "RoleBinding")}
    assert bindings["spark-driver"]["subjects"] == [
        {"kind": "ServiceAccount", "name": "spark", "namespace": NAMESPACE}
    ]
    assert bindings["spark-submit"]["subjects"] == [
        {"kind": "ServiceAccount", "name": "airflow-worker", "namespace": NAMESPACE}
    ]
    assert bindings["airflow-scheduler"]["subjects"] == [
        {"kind": "ServiceAccount", "name": "airflow-scheduler", "namespace": NAMESPACE}
    ]


def test_p3_2_project_pods_are_non_root_without_privilege(project_docs):
    specs = dict(_pod_specs(project_docs))
    assert set(specs) == {"project-postgres", "project-toolbox"}
    for name, spec in specs.items():
        assert spec["automountServiceAccountToken"] is False, name
        assert spec["securityContext"]["runAsNonRoot"] is True, name
        assert spec["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}, name
        for container in spec["containers"]:
            context = container["securityContext"]
            assert context["allowPrivilegeEscalation"] is False, name
            assert context["capabilities"]["drop"] == ["ALL"], name


def test_p3_2_project_services_stay_in_the_cluster(project_docs):
    for service in _by_kind(project_docs, "Service"):
        assert service["spec"]["type"] == "ClusterIP"


def test_p3_2_postgres_receives_the_ddl_of_file_list(project_docs):
    init = _one(project_docs, "ConfigMap", "project-postgres-init")["data"]
    assert init["20-file-list.sql.in"].strip() == DDL.read_text().strip()
    script = init["10-roles-and-databases.sh"]
    # Passwords as psql variables, never interpolated into SQL.
    assert "PASSWORD :'airflow_password'" in script
    assert "PASSWORD :'file_list_password'" in script
    postgres = _one(project_docs, "StatefulSet", "project-postgres")
    image = postgres["spec"]["template"]["spec"]["containers"][0]["image"]
    assert re.fullmatch(r"postgres:17\.\d+@sha256:[0-9a-f]{64}", image)


def test_p3_2_volumes_survive_helm_uninstall(project_docs):
    claims = {c["metadata"]["name"]: c for c in _by_kind(project_docs, "PersistentVolumeClaim")}
    assert set(claims) == {"project-shared", "airflow-logs"}
    for claim in claims.values():
        assert claim["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"


def test_p3_2_spark_defaults_configure_the_driver_and_executors(project_docs):
    conf = _spark_defaults(project_docs)
    assert conf["spark.kubernetes.container.image"] == PROJECT_IMAGE
    assert conf["spark.kubernetes.authenticate.driver.serviceAccountName"] == "spark"
    assert conf["spark.kubernetes.submission.waitAppCompletion"] == "true"
    # Spark cleanups that would need more than pods in the driver's Role.
    assert conf["spark.kubernetes.driver.service.deleteOnTermination"] == "false"
    assert conf["spark.kubernetes.driver.ownPersistentVolumeClaim"] == "false"
    assert conf["spark.kubernetes.executor.disableConfigMap"] == "true"
    # ENV=prod: utils/spark_session.py keeps the k8s master instead of local[*].
    assert conf["spark.kubernetes.driverEnv.ENV"] == "prod"
    assert conf["spark.kubernetes.driverEnv.LAKE_ROOT"] == "/opt/airflow/shared/lake"
    assert conf["spark.kubernetes.driverEnv.DB_HOST"] == "project-postgres"
    assert conf["spark.kubernetes.driverEnv.DB_NAME"] == "file_list"
    # The password is a reference to the Secret, resolved by the kubelet.
    assert conf["spark.kubernetes.driver.secretKeyRef.DB_PASSWORD"] == "file-list-db:password"
    assert not any("PASSWORD" in k and "secretKeyRef" not in k for k in conf)
    for role in ("driver", "executor"):
        prefix = f"spark.kubernetes.{role}.volumes.persistentVolumeClaim.shared"
        assert conf[f"{prefix}.mount.path"] == "/opt/airflow/shared"
        assert conf[f"{prefix}.options.claimName"] == "project-shared"
        assert conf[f"spark.kubernetes.{role}.podTemplateFile"] == (
            f"/opt/spark-conf/{role}-pod-template.yaml"
        )


def test_p3_2_spark_pod_templates_are_hardened(project_docs):
    data = _one(project_docs, "ConfigMap", "spark-conf")["data"]
    for role in ("driver", "executor"):
        template = yaml.safe_load(data[f"{role}-pod-template.yaml"])
        spec = template["spec"]
        assert spec["securityContext"]["runAsNonRoot"] is True
        assert spec["securityContext"]["runAsUser"] == 50000
        container = spec["containers"][0]
        assert container["securityContext"]["allowPrivilegeEscalation"] is False
        assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]
    executor = yaml.safe_load(data["executor-pod-template.yaml"])
    assert executor["spec"]["automountServiceAccountToken"] is False


# --- Official Airflow chart -------------------------------------------------------


def test_p3_2_airflow_uses_the_kubernetes_executor_and_no_bundled_db(airflow_docs):
    config = _one(airflow_docs, "ConfigMap", "airflow-config")["data"]["airflow.cfg"]
    assert re.search(r"^executor = KubernetesExecutor$", config, re.M)
    assert "SimpleAuthManager" in config
    kinds = {(d["kind"], d["metadata"]["name"]) for d in airflow_docs}
    assert not any(
        "postgresql" in name or "pgbouncer" in name or "redis" in name
        for kind, name in kinds
        if kind in ("StatefulSet", "Deployment")
    )
    assert ("StatefulSet", "airflow-triggerer") in kinds or (
        "Deployment",
        "airflow-triggerer",
    ) in kinds


def test_p3_2_airflow_images_are_the_project_image(airflow_docs):
    images = {
        container["image"]
        for _, spec in _pod_specs(airflow_docs)
        for container in spec.get("containers", []) + spec.get("initContainers", [])
    }
    assert images == {PROJECT_IMAGE}
    pod_template = yaml.safe_load(
        _one(airflow_docs, "ConfigMap", "airflow-config")["data"]["pod_template_file.yaml"]
    )
    assert pod_template["spec"]["containers"][0]["image"] == PROJECT_IMAGE


def test_p3_2_worker_pod_template_runs_spark_submit(airflow_docs):
    pod = yaml.safe_load(
        _one(airflow_docs, "ConfigMap", "airflow-config")["data"]["pod_template_file.yaml"]
    )
    spec = pod["spec"]
    assert spec["serviceAccountName"] == "airflow-worker"
    container = spec["containers"][0]
    env = {e["name"]: e.get("value") for e in container["env"]}
    assert env["SPARK_CONF_DIR"] == "/opt/spark-conf"
    assert env["LAKE_ROOT"] == "/opt/airflow/shared/lake"
    # SEC-5: the Connections as secretKeyRef, not the whole Secret.
    assert {"secretRef": {"name": "airflow-connections"}} not in (container.get("envFrom") or [])
    assert "AIRFLOW_CONN_SPARK_DEFAULT" in env
    mounts = {m["name"]: m["mountPath"] for m in container["volumeMounts"]}
    assert mounts["shared"] == "/opt/airflow/shared"
    assert mounts["spark-conf"] == "/opt/spark-conf"
    claims = {v["name"]: v for v in spec["volumes"]}
    assert claims["shared"]["persistentVolumeClaim"]["claimName"] == "project-shared"
    assert claims["logs"]["persistentVolumeClaim"]["claimName"] == "airflow-logs"
    assert spec["securityContext"]["runAsNonRoot"] is True
    assert container["securityContext"]["allowPrivilegeEscalation"] is False
    # The ServiceAccount bound to the Role `spark-submit` is created by the chart.
    accounts = {d["metadata"]["name"] for d in _by_kind(airflow_docs, "ServiceAccount")}
    assert "airflow-worker" in accounts


def test_p3_2_airflow_services_stay_in_the_cluster(airflow_docs):
    for service in _by_kind(airflow_docs, "Service"):
        assert service["spec"].get("type", "ClusterIP") == "ClusterIP", service["metadata"]["name"]
    assert not _by_kind(airflow_docs, "Ingress")


# --- Schemas ----------------------------------------------------------------------


def _kubeconform_command() -> list[str] | None:
    local = _tool("kubeconform")
    if local:
        return [local]
    docker = shutil.which("docker")
    if (
        docker
        and subprocess.run([docker, "info"], capture_output=True, check=False).returncode == 0
    ):
        return [docker, "run", "--rm", "-i", KUBECONFORM_IMAGE]
    return None


@pytest.mark.parametrize("release", ["project", "airflow"])
def test_p3_2_manifests_match_the_kubernetes_schemas(release, request):
    docs = request.getfixturevalue(f"{release}_docs")
    command = _kubeconform_command()
    if not command:
        _skip("neither kubeconform nor Docker available")
    kube_version = re.search(r":v(\d+\.\d+\.\d+)@", _make_var("KIND_NODE_IMAGE")).group(1)
    proc = subprocess.run(
        [*command, "-strict", "-summary", "-kubernetes-version", kube_version, "-"],
        input=yaml.safe_dump_all(docs),
        capture_output=True,
        text=True,
        check=False,
    )
    output = proc.stdout + proc.stderr
    if proc.returncode != 0 and re.search(
        r"(?i)could not (find|download)|dial tcp|no such host", output
    ):
        _skip(f"kubeconform could not fetch the schemas: {output.strip()[:300]}")
    assert proc.returncode == 0, output
    assert re.search(r"Invalid: 0, Errors: 0", output), output


# --- Fixes of the P3-2 reviews ------------------------------------------------------


def _all_containers(spec):
    return (spec.get("initContainers") or []) + (spec.get("containers") or [])


def _restricted_problems(name, spec):
    """What the Pod Security Standard `restricted` would refuse in a pod spec
    (the checks of the standard that these manifests can fail)."""
    problems = []
    pod = spec.get("securityContext") or {}
    allowed_volumes = {
        "configMap",
        "csi",
        "downwardAPI",
        "emptyDir",
        "ephemeral",
        "persistentVolumeClaim",
        "projected",
        "secret",
    }
    for volume in spec.get("volumes") or []:
        kinds = set(volume) - {"name"}
        if not kinds <= allowed_volumes:
            problems.append(f"{name}: volume {volume['name']} {kinds}")
    for container in _all_containers(spec):
        context = container.get("securityContext") or {}
        cname = f"{name}/{container['name']}"
        if context.get("runAsNonRoot", pod.get("runAsNonRoot")) is not True:
            problems.append(f"{cname}: runAsNonRoot")
        if context.get("runAsUser", pod.get("runAsUser")) == 0:
            problems.append(f"{cname}: runAsUser 0")
        seccomp = (context.get("seccompProfile") or pod.get("seccompProfile") or {}).get("type")
        if seccomp not in ("RuntimeDefault", "Localhost"):
            problems.append(f"{cname}: seccompProfile")
        if context.get("allowPrivilegeEscalation") is not False:
            problems.append(f"{cname}: allowPrivilegeEscalation")
        if (context.get("capabilities") or {}).get("drop") != ["ALL"]:
            problems.append(f"{cname}: capabilities.drop")
        if set((context.get("capabilities") or {}).get("add") or []) - {"NET_BIND_SERVICE"}:
            problems.append(f"{cname}: capabilities.add")
        if context.get("privileged"):
            problems.append(f"{cname}: privileged")
    return problems


def test_p3_2_every_pod_meets_the_restricted_standard(project_docs, airflow_docs):
    """SEC-1, static side (the real one is the namespace label on kind): every
    pod of both releases, init containers included, the KubernetesExecutor
    worker template and the Spark pod templates."""
    specs = dict(_pod_specs(project_docs)) | dict(_pod_specs(airflow_docs))
    config = _one(airflow_docs, "ConfigMap", "airflow-config")["data"]
    specs["worker-pod-template"] = yaml.safe_load(config["pod_template_file.yaml"])["spec"]
    spark = _one(project_docs, "ConfigMap", "spark-conf")["data"]
    for role in ("driver", "executor"):
        specs[f"spark-{role}"] = yaml.safe_load(spark[f"{role}-pod-template.yaml"])["spec"]
    problems = [p for name, spec in specs.items() for p in _restricted_problems(name, spec)]
    assert not problems, problems


def test_p3_2_airflow_chart_grants_no_pod_launching(airflow_docs):
    """SEC-2: no Role of the official chart lets anyone create pods or exec."""
    for role in _by_kind(airflow_docs, "Role") + _by_kind(airflow_docs, "ClusterRole"):
        for rule in role.get("rules", []):
            resources, verbs = set(rule["resources"]), set(rule["verbs"])
            assert not ("pods" in resources and verbs & {"create", "delete", "patch"}), role
            assert "pods/exec" not in resources, role["metadata"]["name"]


def test_p3_2_connections_are_in_the_worker_pods_only(airflow_docs):
    """SEC-5: no Airflow component gets the Secret airflow-connections."""
    for name, spec in _pod_specs(airflow_docs):
        for container in _all_containers(spec):
            assert {"secretRef": {"name": "airflow-connections"}} not in (
                container.get("envFrom") or []
            ), name
            assert not any(
                e["name"].startswith("AIRFLOW_CONN_FILE") for e in container.get("env") or []
            ), name
    config = _one(airflow_docs, "ConfigMap", "airflow-config")["data"]
    worker = yaml.safe_load(config["pod_template_file.yaml"])["spec"]["containers"][0]
    refs = {
        e["name"]: e["valueFrom"]["secretKeyRef"]
        for e in worker["env"]
        if e["name"] in ("AIRFLOW_CONN_FILE_LIST_DB", "AIRFLOW_CONN_SPARK_DEFAULT")
    }
    assert refs == {
        "AIRFLOW_CONN_FILE_LIST_DB": {
            "name": "airflow-connections",
            "key": "AIRFLOW_CONN_FILE_LIST_DB",
        },
        "AIRFLOW_CONN_SPARK_DEFAULT": {
            "name": "airflow-connections",
            "key": "AIRFLOW_CONN_SPARK_DEFAULT",
        },
    }


def test_p3_2_api_server_reads_the_ui_password_from_its_secret(airflow_docs):
    """SEC-3 / C-9."""
    api = _one(airflow_docs, "Deployment", "airflow-api-server")["spec"]["template"]["spec"]
    init = {c["name"]: c for c in api["initContainers"]}["ui-passwords"]
    assert init["image"] == PROJECT_IMAGE
    assert init["command"] == [
        "cp",
        "/opt/airflow/simple-auth-secret/passwords.json",
        "/opt/airflow/simple-auth/passwords.json",
    ]
    volumes = {v["name"]: v for v in api["volumes"]}
    assert volumes["ui-passwords-secret"]["secret"]["secretName"] == "airflow-ui-passwords"
    config = _one(airflow_docs, "ConfigMap", "airflow-config")["data"]["airflow.cfg"]
    assert "simple_auth_manager_passwords_file = /opt/airflow/simple-auth/passwords.json" in config


def test_p3_2_volume_claim_templates_have_stable_labels(project_docs):
    """C-2: volumeClaimTemplates are immutable; a label with the chart version
    would make every upgrade of the chart fail."""
    templates = [
        claim
        for doc in project_docs
        if doc["kind"] == "StatefulSet"
        for claim in doc["spec"].get("volumeClaimTemplates", [])
    ]
    assert templates
    for claim in templates:
        labels = claim["metadata"].get("labels", {})
        assert "helm.sh/chart" not in labels and "app.kubernetes.io/version" not in labels
        assert not any(str(v).endswith(PROJECT_CHART_VERSION) for v in labels.values())


def test_p3_2_admission_policy_limits_the_pods_spark_can_create(project_docs):
    """SEC-2: pods created by the ServiceAccounts spark and airflow-worker run
    as `spark`, with the project image, without any Secret volume, and read
    no Secret but the file_list password."""
    (policy,) = _by_kind(project_docs, "ValidatingAdmissionPolicy")
    (binding,) = _by_kind(project_docs, "ValidatingAdmissionPolicyBinding")
    assert policy["spec"]["failurePolicy"] == "Fail"
    rules = policy["spec"]["matchConstraints"]["resourceRules"]
    assert rules == [
        {"apiGroups": [""], "apiVersions": ["v1"], "operations": ["CREATE"], "resources": ["pods"]}
    ]
    users = " ".join(c["expression"] for c in policy["spec"]["matchConditions"])
    assert f"system:serviceaccount:{NAMESPACE}:spark" in users
    assert f"system:serviceaccount:{NAMESPACE}:airflow-worker" in users
    expressions = " ".join(v["expression"] for v in policy["spec"]["validations"])
    for needle in ("serviceAccountName", PROJECT_IMAGE, "secret", "secretKeyRef", "file-list-db"):
        assert needle in expressions, needle
    assert binding["spec"]["policyName"] == policy["metadata"]["name"]
    assert binding["spec"]["validationActions"] == ["Deny"]
    assert binding["spec"]["matchResources"]["namespaceSelector"] == {
        "matchLabels": {"kubernetes.io/metadata.name": NAMESPACE}
    }
