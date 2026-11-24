"""P3-2: the ingest DAGs submit to Kubernetes in cluster mode when the Connection
says so, and keeps the P2-6 retry policy there.

Observed on kind with spark-submit 4.3.0 (docs/architecture/2026-11-19-p3-2-kubernetes.md):

- In cluster mode spark-submit exits **0** whatever the driver's exit code.
  Its pod watcher (spark.kubernetes.submission.waitAppCompletion=true, the
  default) prints the driver container's status when the pod ends, including
  `exit code: N`. The provider's hook (apache-airflow-providers-apache-spark
  6.3.4) parses it into `_spark_exit_code` and fails the task when it is not 0
  ("Error code is: 0. Kubernetes spark exit code is: 3").
- When the submission itself fails (a 403, an unreachable API server),
  spark-submit exits 1 and prints no driver exit code: unknown code, retried.
- A path without a scheme would be uploaded by spark-submit: the job is given
  as `local://<path>` (it is in the image of the driver pod).

The fake spark-submit below prints exactly those lines (copied from the real
run) and exits with spark-submit's code, through the real hook.
"""

import json
import os
import re
import sys
from types import SimpleNamespace

import pytest
from airflow.sdk.exceptions import AirflowException, AirflowFailException

# Last lines of the real spark-submit output on kind (driver exited with 3),
# two long lines shortened.
WATCHER_OUTPUT = """\
26/11/19 18:57:04 INFO LoggingPodStatusWatcherImpl: State changed, new state:
\t pod name: arrow-spark-f0eb5ca1f52b4f28-driver
\t namespace: airflow-project-one
\t labels: spark-app-selector -> spark-4f3fac104a37461d9fbd14d23c29e689, spark-role -> driver
\t phase: Failed
\t container status:
\t\t container name: spark-kubernetes-driver
\t\t container image: docker.io/library/airflow-project-one:dev2
\t\t container state: terminated
\t\t exit code: {code}
\t\t termination reason: Error
26/11/19 18:57:04 INFO LoggingPodStatusWatcherImpl: Container final statuses:
\t\t container name: spark-kubernetes-driver
\t\t container state: terminated
\t\t exit code: {code}
\t\t termination reason: Error
26/11/19 18:57:04 INFO LoggingPodStatusWatcherImpl: Application arrow-spark [...] finished
"""
FORBIDDEN_OUTPUT = (
    'Exception in thread "main" io.fabric8.kubernetes.client.KubernetesClientException: '
    "Failure executing: PATCH at: https://kubernetes.default.svc:443/apis/networking.k8s.io/v1/"
    "namespaces/airflow-project-one/networkpolicies/arrow-spark-38d72ca1f52693d8-policy. "
    'Message: networkpolicies.networking.k8s.io "arrow-spark-38d72ca1f52693d8-policy" is '
    "forbidden\n"
)

K8S_CONNECTION = {
    "conn_type": "spark",
    "host": "k8s://https://kubernetes.default.svc",
    "port": 443,
    "extra": {"deploy-mode": "cluster", "namespace": "airflow-project-one"},
}


TI = SimpleNamespace(
    dag_id="ingest__orders", run_id="manual__p3_2", task_id="spark_read_files", map_index=0
)


def _context():
    return {"ti": TI}


class FakePod:
    def __init__(self, name, phase):
        self.metadata = SimpleNamespace(name=name)
        self.status = SimpleNamespace(phase=phase)


class FakeKube:
    """The few CoreV1Api calls the operator makes, recorded."""

    def __init__(self, pods=(), fail_list=False):
        self.pods = {pod.metadata.name: pod for pod in pods}
        self.fail_list = fail_list
        self.selectors: list[str] = []
        self.deleted: list[tuple[str, str]] = []

    def list_namespaced_pod(self, namespace, label_selector):
        if self.fail_list:
            from kubernetes.client.exceptions import ApiException

            raise ApiException(status=403, reason="Forbidden")
        self.selectors.append(label_selector)
        return SimpleNamespace(items=list(self.pods.values()))

    def delete_namespaced_pod(self, name, namespace, **kwargs):
        self.deleted.append((namespace, name))
        self.pods.pop(name, None)

    def read_namespaced_pod(self, name, namespace):
        from kubernetes.client.exceptions import ApiException

        if name not in self.pods:
            raise ApiException(status=404, reason="Not Found")
        return self.pods[name]


@pytest.fixture(autouse=True)
def fake_kube(monkeypatch):
    import utils.read_files_operator as module

    kube = FakeKube()
    monkeypatch.setattr(module, "_kube_client", lambda: kube)
    return kube


@pytest.fixture
def fake_spark_submit(tmp_path, monkeypatch):
    """A spark-submit that records its arguments, prints P3_2_OUTPUT and exits
    with P3_2_RETURNCODE."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "spark-submit"
    stub.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "open(os.environ['P3_2_ARGS_FILE'], 'w').write(json.dumps(sys.argv[1:]))\n"
        "sys.stdout.write(os.environ.get('P3_2_OUTPUT', ''))\n"
        "sys.exit(int(os.environ['P3_2_RETURNCODE']))\n"
    )
    stub.chmod(0o755)
    args_file = tmp_path / "args.json"
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("P3_2_ARGS_FILE", str(args_file))
    return args_file


def _operator():
    from utils.read_files_operator import (
        JOB_PATH,
        PROJECT_ROOT,
        SPARK_CONN_ID,
        ReadFilesSparkSubmitOperator,
    )

    return ReadFilesSparkSubmitOperator(
        task_id="spark_read_files",
        application=str(JOB_PATH),
        conn_id=SPARK_CONN_ID,
        env_vars={"PYTHONPATH": str(PROJECT_ROOT)},
        application_args=["--run-id=manual__p3_2"],
    )


def _k8s(monkeypatch, *, output="", returncode=0):
    monkeypatch.setenv("AIRFLOW_CONN_SPARK_DEFAULT", json.dumps(K8S_CONNECTION))
    monkeypatch.setenv("P3_2_OUTPUT", output)
    monkeypatch.setenv("P3_2_RETURNCODE", str(returncode))


def test_p3_2_k8s_application_is_given_as_local_uri():
    from utils.read_files_operator import k8s_cluster_application

    assert (
        k8s_cluster_application("/opt/airflow/pipeline/jobs/read_files_job.py")
        == "local:///opt/airflow/pipeline/jobs/read_files_job.py"
    )
    for kept in ("local:///opt/job.py", "s3a://bucket/job.py", "file:///opt/job.py"):
        assert k8s_cluster_application(kept) == kept


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX PATH")
def test_p3_2_cluster_mode_command(fake_spark_submit, monkeypatch):
    from utils.read_files_operator import JOB_PATH, PROJECT_ROOT

    _k8s(monkeypatch, output=WATCHER_OUTPUT.format(code=0), returncode=0)
    operator = _operator()
    assert operator.execute(_context()) is None

    argv = json.loads(fake_spark_submit.read_text())
    assert argv[:2] == ["--master", "k8s://https://kubernetes.default.svc:443"]
    assert f"spark.kubernetes.driverEnv.PYTHONPATH={PROJECT_ROOT}" in argv
    assert "spark.kubernetes.namespace=airflow-project-one" in argv
    assert argv[argv.index("--deploy-mode") + 1] == "cluster"
    # The job in the driver's image, then its single argument, last.
    assert argv[-2:] == [f"local://{JOB_PATH}", "--run-id=manual__p3_2"]
    assert operator.spark_exit_code is None


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX PATH")
@pytest.mark.parametrize("code", [2, 3, 5, 6])
def test_p3_2_deterministic_driver_exit_is_not_retried(fake_spark_submit, monkeypatch, code):
    # spark-submit itself exits 0: the code comes from the pod watcher's lines.
    _k8s(monkeypatch, output=WATCHER_OUTPUT.format(code=code), returncode=0)
    operator = _operator()
    with pytest.raises(AirflowFailException) as exc_info:
        operator.execute(_context())
    assert f"read_files_job exit code {code}" in str(exc_info.value)
    assert f"Error code is: 0. Kubernetes spark exit code is: {code}" in str(exc_info.value)
    assert operator.spark_exit_code == code


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX PATH")
@pytest.mark.parametrize("code", [1, 4, 137])
def test_p3_2_other_driver_exits_keep_the_retries(fake_spark_submit, monkeypatch, code):
    _k8s(monkeypatch, output=WATCHER_OUTPUT.format(code=code), returncode=0)
    operator = _operator()
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(_context())
    assert not isinstance(exc_info.value, AirflowFailException)
    assert operator.spark_exit_code == code


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX PATH")
def test_p3_2_failed_submission_has_no_exit_code_and_is_retried(fake_spark_submit, monkeypatch):
    """A 403 (observed: missing RBAC on networkpolicies): spark-submit exits 1,
    no driver ran, no `exit code:` line."""
    _k8s(monkeypatch, output=FORBIDDEN_OUTPUT, returncode=1)
    operator = _operator()
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(_context())
    assert not isinstance(exc_info.value, AirflowFailException)
    assert "Kubernetes spark exit code is: None" in str(exc_info.value)
    assert operator.spark_exit_code is None


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX PATH")
def test_p3_2_local_mode_is_unchanged(fake_spark_submit, monkeypatch):
    """Connection `local[*]`: no local:// prefix, spark-submit's own return
    code decides (P2-6), nothing Kubernetes in the command."""
    from utils.read_files_operator import JOB_PATH

    monkeypatch.setenv(
        "AIRFLOW_CONN_SPARK_DEFAULT", json.dumps({"conn_type": "spark", "host": "local[1]"})
    )
    monkeypatch.setenv("P3_2_OUTPUT", WATCHER_OUTPUT.format(code=6))
    monkeypatch.setenv("P3_2_RETURNCODE", "1")
    operator = _operator()
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(_context())
    # The `exit code: 6` text is ignored outside Kubernetes: the return code is 1.
    assert not isinstance(exc_info.value, AirflowFailException)
    assert operator.spark_exit_code == 1
    argv = json.loads(fake_spark_submit.read_text())
    assert argv[-2:] == [str(JOB_PATH), "--run-id=manual__p3_2"]
    assert not any("kubernetes" in arg for arg in argv)


def test_p3_2_k8s_client_mode_keeps_the_return_code():
    """`deploy-mode` client on Kubernetes: the driver is spark-submit itself,
    whose return code is the job's (as in local mode)."""
    from utils.read_files_operator import _is_k8s_cluster_mode, _job_exit_code

    class Process:
        returncode = 6

    class Hook:
        _is_kubernetes = True
        _connection = {"deploy_mode": "client"}
        _spark_exit_code = 0
        _submit_sp = Process()

    assert not _is_k8s_cluster_mode(Hook())
    assert _job_exit_code(Hook()) == 6


# --- C-3: Kubernetes detection by the master's scheme -------------------------------


@pytest.mark.parametrize(
    ("master", "deploy_mode", "expected"),
    [
        ("k8s://https://kubernetes.default.svc:443", "cluster", True),
        ("k8s://https://kubernetes.default.svc:443", "client", False),
        ("k8s://https://kubernetes.default.svc:443", None, False),
        # The provider takes any master containing "k8s" for Kubernetes.
        ("spark://k8s-master:7077", "cluster", False),
        ("yarn", "cluster", False),
        ("local[*]", None, False),
    ],
)
def test_p3_2_k8s_cluster_mode_is_detected_by_scheme(master, deploy_mode, expected):
    from utils.read_files_operator import _is_k8s_cluster_mode

    hook = SimpleNamespace(
        _is_kubernetes="k8s" in master, _connection={"master": master, "deploy_mode": deploy_mode}
    )
    assert _is_k8s_cluster_mode(hook) is expected


# --- C-1: a retry never starts a second driver ----------------------------------------


def test_p3_2_driver_label_is_stable_and_valid():
    from utils.read_files_operator import k8s_task_instance_label

    value = k8s_task_instance_label(TI)
    assert value == k8s_task_instance_label(SimpleNamespace(**vars(TI)))
    assert re.fullmatch(r"[a-z0-9]{1,63}", value)
    for change in ({"run_id": "other"}, {"map_index": 1}, {"task_id": "x"}, {"dag_id": "y"}):
        assert k8s_task_instance_label(SimpleNamespace(**{**vars(TI), **change})) != value


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX PATH")
def test_p3_2_retry_deletes_the_orphan_driver(fake_spark_submit, monkeypatch, fake_kube):
    """Attempt 1's worker pod died (OOM, node lost): its driver may still run.
    Attempt 2 deletes the Running/Pending drivers of the same task instance
    before submitting, never a finished one."""
    from utils.read_files_operator import K8S_TI_LABEL, k8s_task_instance_label

    for pod in (
        FakePod("old-running-driver", "Running"),
        FakePod("old-pending-driver", "Pending"),
        FakePod("old-failed-driver", "Failed"),
    ):
        fake_kube.pods[pod.metadata.name] = pod
    _k8s(monkeypatch, output=WATCHER_OUTPUT.format(code=0), returncode=0)
    assert _operator().execute(_context()) is None

    label = k8s_task_instance_label(TI)
    assert fake_kube.selectors == [f"{K8S_TI_LABEL}={label},spark-role=driver"]
    assert sorted(fake_kube.deleted) == [
        ("airflow-project-one", "old-pending-driver"),
        ("airflow-project-one", "old-running-driver"),
    ]
    argv = json.loads(fake_spark_submit.read_text())
    assert f"spark.kubernetes.driver.label.{K8S_TI_LABEL}={label}" in argv


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX PATH")
def test_p3_2_orphan_driver_check_failing_blocks_the_submission(
    fake_spark_submit, monkeypatch, fake_kube
):
    """Unable to list the drivers: no submission (a duplicate driver could run),
    the attempt fails and is retried."""
    fake_kube.fail_list = True
    _k8s(monkeypatch, output=WATCHER_OUTPUT.format(code=0), returncode=0)
    operator = _operator()
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(_context())
    assert not isinstance(exc_info.value, AirflowFailException)
    assert "orphan" in str(exc_info.value)
    assert not fake_spark_submit.exists()


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX PATH")
def test_p3_2_local_mode_has_no_label_and_no_purge(fake_spark_submit, monkeypatch, fake_kube):
    monkeypatch.setenv(
        "AIRFLOW_CONN_SPARK_DEFAULT", json.dumps({"conn_type": "spark", "host": "local[1]"})
    )
    monkeypatch.setenv("P3_2_RETURNCODE", "0")
    assert _operator().execute(_context()) is None
    assert fake_kube.selectors == [] and fake_kube.deleted == []
    argv = json.loads(fake_spark_submit.read_text())
    assert not any("spark.kubernetes" in arg for arg in argv)


# --- C-7: unknown driver state, and a non-zero spark-submit with a driver code ----------

DELETED_DRIVER_OUTPUT = """\
26/11/19 18:57:04 INFO LoggingPodStatusWatcherImpl: State changed, new state:
\t pod name: arrow-spark-f0eb5ca1f52b4f28-driver
\t namespace: airflow-project-one
\t phase: Running
26/11/19 18:57:09 INFO LoggingPodStatusWatcherImpl: Application arrow-spark [...] finished
"""


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX PATH")
def test_p3_2_driver_gone_without_exit_code_is_retried(fake_spark_submit, monkeypatch, fake_kube):
    """The driver pod was deleted (or the watcher cut) before its container ended:
    spark-submit exits 0 without any `exit code:` line. Unknown code: retried,
    and the identified driver pod is deleted, so the retry cannot overlap it."""
    _k8s(monkeypatch, output=DELETED_DRIVER_OUTPUT, returncode=0)
    operator = _operator()
    with pytest.raises(AirflowException) as exc_info:
        operator.execute(_context())
    assert not isinstance(exc_info.value, AirflowFailException)
    assert "Kubernetes spark exit code is: None" in str(exc_info.value)
    assert operator.spark_exit_code is None
    assert ("airflow-project-one", "arrow-spark-f0eb5ca1f52b4f28-driver") in fake_kube.deleted


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX PATH")
def test_p3_2_nonzero_spark_submit_with_driver_code_6_is_not_retried(
    fake_spark_submit, monkeypatch
):
    _k8s(monkeypatch, output=WATCHER_OUTPUT.format(code=6), returncode=1)
    operator = _operator()
    with pytest.raises(AirflowFailException) as exc_info:
        operator.execute(_context())
    assert "read_files_job exit code 6" in str(exc_info.value)
    assert operator.spark_exit_code == 6


# --- C-6: Spark lineage is removed in Kubernetes cluster mode -------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="needs a POSIX PATH")
def test_p3_2_lineage_is_removed_in_k8s_cluster_mode(fake_spark_submit, monkeypatch, caplog):
    """The listener jar would be a client file to upload (no local://) and the
    file transport writes inside the pod: the submission goes without lineage
    and says so, it never fails because of it."""
    from utils.read_files_operator import OPENLINEAGE_SPARK_LISTENER

    _k8s(monkeypatch, output=WATCHER_OUTPUT.format(code=0), returncode=0)
    operator = _operator()
    operator.conf = {
        "spark.jars": "/opt/airflow/lineage/jars/openlineage-spark_2.13-1.55.0.jar",
        "spark.extraListeners": OPENLINEAGE_SPARK_LISTENER,
        "spark.openlineage.transport.type": "file",
        "spark.executor.memory": "1g",
    }
    with caplog.at_level("WARNING"):
        assert operator.execute(_context()) is None
    argv = " ".join(json.loads(fake_spark_submit.read_text()))
    assert "spark.jars" not in argv and "openlineage" not in argv.lower()
    assert "spark.executor.memory=1g" in argv
    assert any(
        "lineage" in r.getMessage() and "Kubernetes" in r.getMessage() for r in caplog.records
    )
