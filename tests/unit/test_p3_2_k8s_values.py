"""P3-2: the values of the two Helm releases agree with each other and with
the code (no Helm needed; the rendered manifests are checked by
tests/integration/test_p3_2_helm_render.py).

The Airflow chart's worker pods run spark-submit: they must use the
ServiceAccount bound to the `spark-submit` Role, mount the Spark configuration
where SPARK_CONF_DIR points, and mount the shared volume at the path the Spark
pods use. The paths given to dbt and to the Spark driver must be the same.
"""

import re
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
K8S_DIR = PROJECT_ROOT / "deploy" / "k8s"
PROJECT_VALUES = K8S_DIR / "charts" / "project" / "values.yaml"
AIRFLOW_VALUES = K8S_DIR / "values-airflow.yaml"


def _project():
    return yaml.safe_load(PROJECT_VALUES.read_text())


def _airflow():
    return yaml.safe_load(AIRFLOW_VALUES.read_text())


def _env(values):
    return {item["name"]: item.get("value") for item in values}


def test_p3_2_airflow_runs_the_kubernetes_executor_with_the_project_image():
    airflow = _airflow()
    assert airflow["executor"] == "KubernetesExecutor"
    assert airflow["images"]["airflow"]["repository"] == _project()["image"]["repository"]
    assert airflow["images"]["airflow"]["tag"] == _project()["image"]["tag"]
    # DAGs are in the image: neither git-sync nor a DAG volume.
    assert "dags" not in airflow
    # The triggerer runs the deferrable Cosmos sensors of transform_dag.
    assert airflow["triggerer"]["enabled"] is True


def test_p3_2_no_bundled_database_nor_unused_service():
    airflow = _airflow()
    assert airflow["postgresql"]["enabled"] is False
    assert airflow["pgbouncer"]["enabled"] is False
    assert airflow["redis"]["enabled"] is False
    assert airflow["statsd"]["enabled"] is False
    assert airflow["data"]["metadataSecretName"]
    # Without it, `helm --wait` deadlocks on the migration hook.
    assert airflow["migrateDatabaseJob"]["useHelmHooks"] is False


def test_p3_2_no_credential_in_the_values():
    for path in (PROJECT_VALUES, AIRFLOW_VALUES):
        text = path.read_text()
        assert not re.search(
            r"(?i)^[ \t]*(password|pass|secret|token|fernetKey|jwtSecret)[ \t]*:[ \t]*[^\s{]",
            text,
            re.M,
        ), path
        assert "postgresql://" not in text, path
    airflow = _airflow()
    for key in ("fernetKey", "jwtSecret", "apiSecretKey", "webserverSecretKey"):
        assert key not in airflow


def test_p3_2_auth_manager_is_the_simple_one():
    core = _airflow()["config"]["core"]
    assert core["auth_manager"].endswith("simple_auth_manager.SimpleAuthManager")
    assert _airflow()["createUserJob"]["enabled"] is False


def test_p3_2_worker_pods_can_run_spark_submit():
    project, workers = _project(), _airflow()["workers"]["kubernetes"]
    spark = project["spark"]
    assert workers["serviceAccount"]["name"] == spark["submitterServiceAccountName"]
    assert _env(workers["env"])["SPARK_CONF_DIR"] == spark["confDir"]
    volumes = {v["name"]: v for v in workers["extraVolumes"]}
    mounts = {m["name"]: m for m in workers["extraVolumeMounts"]}
    assert volumes["spark-conf"]["configMap"]["name"] == spark["configMapName"]
    assert mounts["spark-conf"]["mountPath"] == spark["confDir"]
    assert mounts["spark-conf"]["readOnly"] is True
    assert volumes["shared"]["persistentVolumeClaim"]["claimName"] == project["shared"]["claimName"]
    assert mounts["shared"]["mountPath"] == project["shared"]["mountPath"]


def test_p3_2_dbt_and_the_driver_share_the_lake():
    project, env = _project(), _env(_airflow()["env"])
    shared = project["shared"]["mountPath"]
    assert env["LAKE_ROOT"] == project["spark"]["lakeRoot"]
    assert env["LAKE_ROOT"].startswith(shared + "/")
    assert env["DBT_DUCKDB_PATH"].startswith(shared + "/")
    # The listing reads ./data/ = <AIRFLOW_HOME>/data -> shared/data (Dockerfile).
    extractor = yaml.safe_load((PROJECT_ROOT / "config" / "extractor_config.yaml").read_text())
    assert extractor["extractor_config"]["source_directories"] == ["./data/"]
    assert "ln -s shared/data /opt/airflow/data" in (PROJECT_ROOT / "Dockerfile").read_text()
    assert shared == "/opt/airflow/shared"


def test_p3_2_logs_use_the_project_volume():
    logs = _airflow()["logs"]["persistence"]
    assert logs["enabled"] is True
    assert logs["existingClaim"] == _project()["logs"]["claimName"]


def test_p3_2_project_images_are_pinned():
    assert re.fullmatch(r"postgres:17\.\d+@sha256:[0-9a-f]{64}", _project()["postgres"]["image"])


def test_p3_2_spark_pods_run_as_the_airflow_user():
    security = _project()["podSecurity"]
    assert security == {"runAsUser": 50000, "runAsGroup": 0}
    assert re.search(r"^USER 50000$", (PROJECT_ROOT / "Dockerfile").read_text(), re.M)


def test_p3_2_chart_grants_no_pod_launching():
    """SEC-2: the chart's pod-launcher Role (pods/exec included, bound to the
    scheduler and the workers) is not created; the project chart grants the
    scheduler what the KubernetesExecutor needs, the workers what spark-submit
    needs."""
    assert _airflow()["allowPodLaunching"] is False
    assert _airflow().get("allowJobLaunching", False) is False


def test_p3_2_connections_reach_the_worker_pods_only():
    """SEC-5: no AIRFLOW_CONN_* in the scheduler, api-server, dag-processor or
    triggerer: only the tasks use the Connections."""
    airflow = _airflow()
    assert "extraEnvFrom" not in airflow
    assert "secret" not in airflow
    assert not any(e["name"].startswith("AIRFLOW_CONN_") for e in airflow["env"])
    worker_env = {e["name"]: e for e in airflow["workers"]["kubernetes"]["env"]}
    for name in ("AIRFLOW_CONN_FILE_LIST_DB", "AIRFLOW_CONN_SPARK_DEFAULT"):
        assert worker_env[name]["valueFrom"]["secretKeyRef"]["name"] == "airflow-connections"


def test_p3_2_ui_password_comes_from_a_secret():
    """SEC-3 / C-9: the Simple Auth Manager reads a passwords file copied from
    a Secret into an emptyDir (it opens the file for writing: a Secret volume is
    read-only), so it never generates nor logs a password."""
    airflow = _airflow()
    path = airflow["config"]["core"]["simple_auth_manager_passwords_file"]
    api = airflow["apiServer"]
    volumes = {v["name"]: v for v in api["extraVolumes"]}
    assert volumes["ui-passwords"] == {"name": "ui-passwords", "emptyDir": {}}
    assert volumes["ui-passwords-secret"]["secret"]["secretName"] == "airflow-ui-passwords"
    mounts = {m["name"]: m["mountPath"] for m in api["extraVolumeMounts"]}
    assert path.startswith(mounts["ui-passwords"] + "/")
    (init,) = api["extraInitContainers"]
    assert init["command"][0] == "cp"
    assert init["command"][-1] == path
    assert init["securityContext"]["allowPrivilegeEscalation"] is False
    assert init["securityContext"]["capabilities"]["drop"] == ["ALL"]


def test_p3_2_sonar_toolbox_has_a_storage_limit():
    # Sonar kubernetes:S6870 on PR #30: every container of the toolbox has an
    # ephemeral-storage request and limit.
    resources = _project()["toolbox"]["resources"]
    assert resources["requests"]["ephemeral-storage"]
    assert resources["limits"]["ephemeral-storage"]
