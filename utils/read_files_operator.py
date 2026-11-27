"""The spark-submit task of the `ingest__<source>` DAGs (P5-3; it was the body of
`read_file_dag` until then): the operator that never retries a deterministic exit
of `pipeline/jobs/read_files_job.py`, its Kubernetes cluster mode (P3-2) and its
OpenLineage Spark settings (P2-5).

Airflow orchestrates, Spark runs in its own process. Imported by
`dags/source_pipelines.py` at parse time, never by the Spark job (it must not
import Airflow: tests/unit/jobs/test_spark_job_no_airflow_import.py).

- Master Spark: Connection `spark_default` (`local[*]` in dev, see CLAUDE.md). The
  hook would silently fall back to `yarn` if it were missing: `execute` checks
  that it exists first and fails the task otherwise (P1-5 / C-2).
- Pool `spark` (deploy/airflow/pools.json): limits the concurrent Spark
  applications. A missing pool leaves the task in `scheduled` without any error in
  the UI, until `dagrun_timeout`.
- Resources (memory, cores...): not in the DAG, but in the Connection /
  `spark-defaults.conf` of each environment.
- Environment of the job (P5-3): `PIPELINE_DEFAULT_ENGINE` and
  `PIPELINE_CONFIG_ROOT` are passed in `env_vars` with the values the DAG parsed,
  so the job validates the configs under the same engine and root as the DAG did,
  in local mode and as `spark.*.env` conf on YARN / Kubernetes (`job_env_vars`).

`application_args` is a templated field of `SparkSubmitOperator`: the value mapped
from an XCom is not rendered by Jinja, but never build the run id into a templated
string (an f-string passed to the operator, `@task.bash`...): the DAG's own run id
can be free text (manual run through the API). tests/integration/test_p1_4_ingest_dag_e2e.py
guards against that.
"""

import logging
import os
from pathlib import Path
from typing import Any

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
JOB_PATH = PROJECT_ROOT / "pipeline" / "jobs" / "read_files_job.py"

# Spark master (and deploy mode, queue...) come from this Connection, not from
# the DAG. Missing Connection: the provider's hook silently falls back to
# `--master yarn`, which fails locally without HADOOP_CONF_DIR.
SPARK_CONN_ID = "spark_default"
# Dedicated pool: caps concurrent Spark applications across DAGs and mapped
# instances. Must exist (`airflow pools set spark ...`): with a missing pool
# the scheduler leaves the task in `scheduled` forever.
SPARK_POOL = "spark"

# P2-5: OpenLineage Spark listener (JVM), from the openlineage-spark jar whose
# path is given by OPENLINEAGE_SPARK_JAR (`make lineage-jar` downloads the
# pinned version, Scala 2.13 like PySpark 4). Spark events of the dev `file`
# transport go to this file, next to the Airflow ones.
OPENLINEAGE_SPARK_LISTENER = "io.openlineage.spark.agent.OpenLineageSparkListener"
OPENLINEAGE_SPARK_LISTENER_ENTRY = "io/openlineage/spark/agent/OpenLineageSparkListener.class"
OPENLINEAGE_SPARK_JAR_ENV = "OPENLINEAGE_SPARK_JAR"
OPENLINEAGE_SPARK_SHA256_ENV = "OPENLINEAGE_SPARK_SHA256"
SPARK_LINEAGE_FILE = "spark.jsonl"
# Spark's default `(?i)secret|password|token|access[.]?key`, plus what an
# OpenLineage transport can carry: redacted from the Spark UI and event log.
SPARK_REDACTION_REGEX = (
    r"(?i)secret|password|token|access[.]?key|apikey|authorization|openlineage\.transport\.url"
)
# KeyValueTrimmer names a file such as `report_dt=2026-11-14.csv` after its
# directory (it takes the name for a partition): the listing declares the
# file. The lake tables are written at their root, so they keep their names.
OPENLINEAGE_DISABLED_TRIMMERS = "io.openlineage.client.dataset.partition.trimmer.KeyValueTrimmer"
# Keys added by openlineage_spark_settings (besides spark.openlineage.*).
_LINEAGE_CONF_KEYS = {"spark.jars", "spark.extraListeners", "spark.redaction.regex"}

# P2-6: exit codes of pipeline/jobs/read_files_job.py that a retry cannot
# change (same values as its EXIT_* constants, checked by a test: the DAG does
# not import the job, which would load pyspark at every parse).
NO_RETRY_EXIT_CODES: dict[int, str] = {
    2: "invalid --run-id",
    3: "no file_list row for the run id",
    5: "invalid reading config or LAKE_ROOT, every file of the run marked failed",
    6: "blocking quality checks only, see file_list and quality/<table> in the lake",
}

log = logging.getLogger(__name__)


def build_application_args(run_id: str, source_name: str) -> list[str]:
    """Return the job arguments (`application_args`) for one run of one source.

    The run id is passed as a single `--run-id=<value>` argument so that a
    value starting with `-` (possible from a manual trigger) is not parsed as
    an option by the job (C-4). No shell is involved: SparkSubmitHook passes
    the argument list to `subprocess.Popen` as is. `--source-name` scopes the
    job to the rows of this source (P5-3): two source DAGs can share a run id.
    """
    return [f"--run-id={run_id}", f"--source-name={source_name}"]


def job_env_vars() -> dict[str, str]:
    """`env_vars` of the Spark task (P5-3): the project root for the job's Python
    workers, and the engine default and config root **this parse used**, so the
    job validates the configs as the DAG did (a worker or driver pod whose
    environment differs would otherwise judge them differently).

    Added to the worker's environment in local/client mode, written as
    `spark.*.env` conf (in the logged command: no secret here) on YARN and
    Kubernetes. Raises `ValueError` for an invalid `PIPELINE_DEFAULT_ENGINE`.
    """
    from pipeline.config import default_config_root
    from pipeline.config.engine import DEFAULT_ENGINE_ENV, default_engine

    return {
        "PYTHONPATH": str(PROJECT_ROOT),
        DEFAULT_ENGINE_ENV: default_engine(),
        "PIPELINE_CONFIG_ROOT": str(default_config_root()),
    }


def _spark_submit_returncode(hook: Any) -> int | None:
    """Return code of the spark-submit process `hook` ran, or None when it
    never started.

    SparkSubmitHook (apache-airflow-providers-apache-spark 6.3.4) keeps the
    `subprocess.Popen` of spark-submit in `_submit_sp` and raises
    `AirflowException("Cannot execute: ... Error code is: N.")` when it
    exits non-zero. In local/client mode, spark-submit exits with the code
    of the Python driver, i.e. the job's. There is no public accessor: the
    attribute is pinned by tests/unit/dags/test_p2_6_no_retry_exit_codes.py.
    """
    returncode = getattr(getattr(hook, "_submit_sp", None), "returncode", None)
    return returncode if isinstance(returncode, int) else None


# P3-2 / C-1: label put on every driver pod of one task instance (dag, run,
# task, map index), the same on every try: a retry finds and deletes a driver
# of an earlier try that is still alive before submitting its own.
K8S_TI_LABEL = "airflow-project-one/task-instance"
# Phases of a driver pod that may still write to the lake.
_LIVE_POD_PHASES = {"Pending", "Running"}
# Seconds to wait for a deleted orphan driver to be gone.
_ORPHAN_DELETE_TIMEOUT = 120


def _is_k8s_cluster_mode(hook: Any) -> bool:
    """True when `hook` submits to Kubernetes in cluster deploy mode (P3-2):
    master with the `k8s://` scheme and `deploy-mode` cluster, both from the
    Connection. By the scheme, not by the provider's `_is_kubernetes` (any
    master containing "k8s", `spark://k8s-master:7077` included, C-3)."""
    connection = getattr(hook, "_connection", None) or {}
    master = str(connection.get("master") or "")
    return master.startswith("k8s://") and connection.get("deploy_mode") == "cluster"


def _job_exit_code(hook: Any) -> int | None:
    """Exit code of read_files_job for the submission `hook` ran, or None
    when it is unknown (the job's failures are then retried, the safe side).

    - Local/client mode: spark-submit's return code (the driver's).
    - Kubernetes cluster mode (P3-2): the driver runs in its own pod, and
      spark-submit 4.3.0 exits 0 whatever the driver's exit code (checked on
      kind, docs/architecture/2026-11-19-p3-2-kubernetes.md). The provider's
      hook reads the driver container's code from spark-submit's log (Spark's
      pod watcher prints `exit code: N` when the driver pod ends, as long as
      spark.kubernetes.submission.waitAppCompletion is true, Spark's
      default) into `_spark_exit_code` and fails the task when it is not 0.
      None when the line never came (submission refused, watcher cut, driver
      pod deleted).
    """
    if _is_k8s_cluster_mode(hook):
        code = getattr(hook, "_spark_exit_code", None)
        return code if isinstance(code, int) else None
    return _spark_submit_returncode(hook)


def k8s_cluster_application(application: str) -> str:
    """`application` as the driver pod sees it in Kubernetes cluster mode.

    A path without a scheme is a file of the submitting machine, which
    spark-submit would have to upload (spark.kubernetes.file.upload.path);
    the job is in the image, at the same path, so it is given as
    `local://<path>`. A value with a scheme is kept.
    """
    if "://" in application or application.startswith("local:"):
        return application
    return f"local://{application}"


def k8s_task_instance_label(ti: Any) -> str:
    """Value of `K8S_TI_LABEL` for a task instance: 40 hex characters, a
    valid label value whatever the run id (free text for a manual run)."""
    import hashlib

    key = "\0".join(str(part) for part in (ti.dag_id, ti.run_id, ti.task_id, ti.map_index))
    return hashlib.sha256(key.encode()).hexdigest()[:40]


def _kube_client() -> Any:
    """CoreV1Api of the cluster the worker runs in (provider cncf.kubernetes,
    as SparkSubmitHook.on_kill uses it)."""
    from airflow.providers.cncf.kubernetes import kube_client

    return kube_client.get_kube_client()


def _wait_pod_gone(client: Any, namespace: str, name: str, timeout: float) -> bool:
    import time

    from kubernetes.client.exceptions import ApiException

    deadline = time.monotonic() + timeout
    while True:
        try:
            client.read_namespaced_pod(name, namespace)
        except ApiException as exc:
            if exc.status == 404:
                return True
            raise
        if time.monotonic() >= deadline:
            return False
        time.sleep(2)


class ReadFilesSparkSubmitOperator(SparkSubmitOperator):
    """SparkSubmitOperator that never retries a deterministic failure of
    read_files_job (P2-6).

    When spark-submit exits with one of `NO_RETRY_EXIT_CODES` (2, 3, 5, 6),
    the hook's `AirflowException` becomes an `AirflowFailException`, which
    the Task SDK fails without retry; its message names the code and keeps
    the hook's (masked command, log tail). Any other failure (exit 1 or 4,
    a signal, an error before spark-submit started) propagates unchanged
    and keeps the task's `retries`.

    Only `execute` is overridden: `.partial().expand()`, the templated
    fields, `pre_execute` (P2-5 lineage) and `on_kill` are the provider's.
    In YARN cluster mode, spark-submit's return code is not the driver's:
    those runs keep retrying, the safe side.

    P3-2, Kubernetes cluster mode (master `k8s://...` and `deploy-mode`
    cluster in the Connection, nothing in the DAG), before the submission:
    - the job is given as `local://<path>` (it is in the driver's image);
    - Spark lineage is removed, with a warning (C-6): the listener jar would
      be a file of the worker to upload, and the `file` transport writes in
      the driver pod;
    - the driver gets the label `K8S_TI_LABEL` of the task instance, and the
      live drivers (Pending, Running) carrying it, left by an earlier try
      whose worker pod died, are deleted (C-1). When they cannot be listed,
      nothing is submitted and the try fails (retried).
    After it, the exit code is the driver container's, read by the hook from
    spark-submit's log (see `_job_exit_code`): the same codes are not
    retried. When that code is unknown, the driver pod the hook identified
    is deleted, so that the retry never runs next to it.
    Local/client mode is unchanged.

    P3-3: the code read on a failure is kept in `spark_exit_code` (None while
    spark-submit did not run), for the failure callback of the task, which
    reports it in its `airflow_failure` line. Nothing else changes.
    """

    spark_exit_code: int | None = None

    def execute(self, context: Any) -> None:
        from airflow.sdk import BaseHook
        from airflow.sdk.exceptions import (
            AirflowException,
            AirflowFailException,
            AirflowNotFoundException,
        )

        # P1-5 / C-2: SparkSubmitHook silently falls back to `--master yarn` when
        # its Connection is missing. Fail here, explicitly, instead.
        try:
            spark_conn = BaseHook.get_connection(self._conn_id)
        except AirflowNotFoundException:
            raise AirflowFailException(
                f"Airflow Connection {self._conn_id!r} not found: spark_read_files would "
                "silently run `spark-submit --master yarn`. Create it first (see CLAUDE.md, "
                "'Running Airflow locally')."
            ) from None
        log.info("Spark master from Connection %r: host=%r", self._conn_id, spark_conn.host)

        # The mode comes from the Connection: a hook built only to read it
        # (the provider builds the one that submits, from the final conf).
        probe = self._hook if self._hook is not None else super()._get_hook()
        k8s_cluster = _is_k8s_cluster_mode(probe)
        if k8s_cluster:
            self._prepare_k8s_cluster(context, probe)
        try:
            return super().execute(context)
        except AirflowException as exc:
            code = _job_exit_code(self._hook)
            self.spark_exit_code = code
            meaning = NO_RETRY_EXIT_CODES.get(code) if code is not None else None
            if code is None:
                self.log.warning(
                    "read_files_job exit code unavailable (spark-submit did not "
                    "start, or in Kubernetes cluster mode no driver exit code in "
                    "its log): deterministic failures of read_files_job will be "
                    "retried."
                )
                if k8s_cluster:
                    self._delete_identified_driver()
            if meaning is None:
                raise
            raise AirflowFailException(
                f"read_files_job exit code {code} ({meaning}): deterministic, not retried. {exc}"
            ) from exc

    def _k8s_namespace(self, hook: Any) -> str:
        return (getattr(hook, "_connection", None) or {}).get("namespace") or "default"

    def _prepare_k8s_cluster(self, context: Any, probe: Any) -> None:
        from airflow.sdk.exceptions import AirflowException

        self.application = k8s_cluster_application(self.application)
        conf = self.conf or {}
        if conf.get("spark.extraListeners") == OPENLINEAGE_SPARK_LISTENER or any(
            key.startswith("spark.openlineage.") for key in conf
        ):
            log.warning(
                "OpenLineage: Spark lineage is not supported in Kubernetes cluster mode "
                "(the listener jar would have to be uploaded, the file transport writes "
                "in the driver pod): Spark runs without its listener"
            )
        _strip_lineage(self)

        ti = context.get("ti") if hasattr(context, "get") else None
        if ti is None:
            self.log.warning(
                "no task instance in the context: driver not labelled, orphan drivers "
                "of earlier tries not checked"
            )
            return
        label = k8s_task_instance_label(ti)
        self.conf = {**(self.conf or {}), f"spark.kubernetes.driver.label.{K8S_TI_LABEL}": label}
        namespace = self._k8s_namespace(probe)
        try:
            client = _kube_client()
            pods = client.list_namespaced_pod(
                namespace, label_selector=f"{K8S_TI_LABEL}={label},spark-role=driver"
            )
            for pod in pods.items:
                if pod.status.phase not in _LIVE_POD_PHASES:
                    continue
                name = pod.metadata.name
                self.log.warning(
                    "Deleting driver pod %s (%s) of an earlier try of this task instance "
                    "before submitting a new one",
                    name,
                    pod.status.phase,
                )
                client.delete_namespaced_pod(name, namespace)
                if not _wait_pod_gone(client, namespace, name, _ORPHAN_DELETE_TIMEOUT):
                    raise RuntimeError(f"driver pod {name} still present after deletion")
        except Exception as exc:
            raise AirflowException(
                f"could not check or delete the orphan Spark drivers of this task instance "
                f"in namespace {namespace!r} ({type(exc).__name__}: {exc}): not submitting, "
                "a second driver could run next to the first one"
            ) from exc

    def _delete_identified_driver(self) -> None:
        """Delete the driver pod spark-submit created, when its outcome is
        unknown. Never raises."""
        hook = self._hook
        name = getattr(hook, "_kubernetes_driver_pod", None)
        if not name:
            return
        namespace = self._k8s_namespace(hook)
        try:
            from kubernetes.client.exceptions import ApiException

            try:
                _kube_client().delete_namespaced_pod(name, namespace)
                self.log.warning("Deleted driver pod %s: its outcome is unknown", name)
            except ApiException as exc:
                if exc.status != 404:
                    raise
        except Exception:
            self.log.warning("could not delete driver pod %s", name, exc_info=True)


def _lineage_off() -> dict[str, Any]:
    return {
        "conf": None,
        "openlineage_inject_parent_job_info": False,
        "openlineage_inject_transport_info": False,
    }


def openlineage_spark_settings() -> dict[str, Any]:
    """SparkSubmitOperator kwargs enabling the OpenLineage Spark listener (P2-5).

    Read when the file is parsed, i.e. by the worker itself when it runs the
    task, from the Airflow OpenLineage configuration (`[openlineage]` /
    `AIRFLOW__OPENLINEAGE__*`, `OPENLINEAGE_DISABLED`) and from
    `OPENLINEAGE_SPARK_JAR`. Cheap: environment, config, and the central
    directory of the jar (about 35 ms); the jar is hashed at execution only
    (prepare_spark_lineage).

    Lineage never breaks the pipeline: whenever Spark lineage cannot be
    enabled safely, the result is `conf=None` and no injection, i.e. exactly
    the spark-submit command of before P2-5, with a warning:
    - lineage off (disabled, or no transport configured): silently;
    - `OPENLINEAGE_SPARK_JAR` unset (debug log only, read at every parse),
      not absolute (`~` is expanded), missing, not a zip, or without the
      listener class: a listener absent from the classpath stops the
      SparkContext;
    - a transport that is not a JSON object, or any other error (the
      provider missing included): the DAG still imports;
    - a transport carrying credentials (`auth`, `headers`/`custom_headers`,
      user info in a URL, also inside `composite`), or one configured
      elsewhere than `[openlineage] transport` (config_path, config_conn_id,
      OPENLINEAGE_URL...), which cannot be checked: the provider would inject
      it into the Spark properties, which SparkSubmitHook logs in clear
      (`Spark-Submit cmd`, masking only `secret`/`password` keys);
    - a `file` transport whose `log_file_path` is not a local absolute path.

    Otherwise: the jar, the listener, `spark.redaction.regex` extended to
    the OpenLineage properties, the namespace, KeyValueTrimmer disabled (a
    source file `report_dt=2026-11-14.csv` keeps its own name), and:
    - `file` transport: Spark events in the directory of the Airflow ones
      (one file per application, set by prepare_spark_lineage);
    - any other transport (http): injected by the provider at execution.
    The parent job info is always injected: the Spark run is the child of
    its Airflow task. These explicit `openlineage_inject_*` values override
    `[openlineage] spark_inject_*`, and `--conf spark.jars` /
    `spark.extraListeners` override the ones of spark-defaults.conf.
    """
    try:
        return _openlineage_spark_settings()
    except Exception:
        log.warning(
            "OpenLineage: invalid configuration, Spark runs without its listener",
            exc_info=True,
        )
        return _lineage_off()


def _openlineage_spark_settings() -> dict[str, Any]:
    from airflow.providers.openlineage import conf as openlineage_conf

    if openlineage_conf.is_disabled():
        return _lineage_off()
    jar = (os.environ.get(OPENLINEAGE_SPARK_JAR_ENV) or "").strip()
    if not jar:
        log.debug("%s is not set: Spark runs without its listener", OPENLINEAGE_SPARK_JAR_ENV)
        return _lineage_off()
    jar_path = Path(jar).expanduser()
    problem = _jar_problem(jar_path)
    if problem:
        log.warning(
            "%s=%s %s: Spark runs without its OpenLineage listener (run `make lineage-jar`)",
            OPENLINEAGE_SPARK_JAR_ENV,
            jar,
            problem,
        )
        return _lineage_off()

    conf = {
        "spark.jars": str(jar_path),
        "spark.extraListeners": OPENLINEAGE_SPARK_LISTENER,
        "spark.redaction.regex": SPARK_REDACTION_REGEX,
        "spark.openlineage.namespace": openlineage_conf.namespace(),
        "spark.openlineage.dataset.disabledTrimmers": OPENLINEAGE_DISABLED_TRIMMERS,
    }
    transport = openlineage_conf.transport()
    if not transport:
        log.warning(
            "OpenLineage transport not set in [openlineage] transport (config_path, "
            "config_conn_id or OPENLINEAGE_* variables): it cannot be checked for "
            "credentials, so it is not injected into Spark, which runs without its listener"
        )
        return _lineage_off()
    if transport.get("type") == "file":
        directory = _local_directory(transport.get("log_file_path"))
        if directory is None:
            log.warning(
                "OpenLineage file transport: log_file_path=%r is not a local absolute path, "
                "Spark runs without its listener",
                transport.get("log_file_path"),
            )
            return _lineage_off()
        conf["spark.openlineage.transport.type"] = "file"
        # Default location; prepare_spark_lineage gives each application its own.
        conf["spark.openlineage.transport.location"] = str(directory / SPARK_LINEAGE_FILE)
        inject_transport = False
    else:
        reason = _transport_credentials(transport)
        if reason:
            log.warning(
                "OpenLineage transport carries credentials (%s): it is never injected into "
                "the Spark properties, which spark-submit logs in clear. Spark runs without "
                "its listener",
                reason,
            )
            return _lineage_off()
        inject_transport = True
    return {
        "conf": conf,
        "openlineage_inject_parent_job_info": True,
        "openlineage_inject_transport_info": inject_transport,
    }


def _jar_problem(jar_path: Path) -> str | None:
    """Why `jar_path` cannot be the listener jar, or None."""
    import zipfile

    if not jar_path.is_absolute():
        return "is not an absolute path (it would depend on the worker's current directory)"
    if not jar_path.is_file():
        return "does not exist"
    try:
        with zipfile.ZipFile(jar_path) as archive:
            archive.getinfo(OPENLINEAGE_SPARK_LISTENER_ENTRY)
    except KeyError:
        return f"has no {OPENLINEAGE_SPARK_LISTENER} class"
    except (OSError, zipfile.BadZipFile) as exc:
        return f"is not a readable jar ({type(exc).__name__}: {exc})"
    return None


def _local_directory(log_file_path: Any) -> Path | None:
    """Directory of a local absolute `log_file_path` (`/x/airflow` or
    `file:///x/airflow`), else None."""
    from urllib.parse import urlsplit

    if not isinstance(log_file_path, str) or not log_file_path:
        return None
    parts = urlsplit(log_file_path)
    if parts.scheme == "file":
        path = parts.path
    elif parts.scheme:
        return None
    else:
        path = log_file_path
    return Path(path).parent if path.startswith("/") else None


def _transport_credentials(transport: Any) -> str | None:
    """What in `transport` (composite included) is a credential, or None."""
    from urllib.parse import urlsplit

    if not isinstance(transport, dict):
        return None
    for key in ("auth", "headers", "custom_headers"):
        if transport.get(key):
            return f"`{key}`"
    url = transport.get("url")
    if isinstance(url, str) and (urlsplit(url).username or urlsplit(url).password):
        return "user info in `url`"
    nested = transport.get("transports") or []
    for sub in nested.values() if isinstance(nested, dict) else nested:
        reason = _transport_credentials(sub)
        if reason:
            return reason
    return None


# (path, size, mtime_ns) -> SHA-256 of the jar: hashed once per file version.
_jar_digest_cache: dict[tuple[str, int, int], str] = {}


def _file_sha256(path: str) -> str:
    import hashlib

    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _jar_sha256(path: str) -> str:
    stat = os.stat(path)
    key = (path, stat.st_size, stat.st_mtime_ns)
    if key not in _jar_digest_cache:
        _jar_digest_cache[key] = _file_sha256(path)
    return _jar_digest_cache[key]


def _application_lineage_file(context: Any, directory: Path) -> Path:
    """One Spark event file per application: DAG, run (slug + hash: the run
    id is free text), map index and try."""
    import hashlib
    import re

    ti = context["ti"]
    run_id = str(context.get("run_id") or ti.dag_run.run_id)
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("._")[:60]
    digest = hashlib.sha1(run_id.encode()).hexdigest()[:8]
    name = f"spark-{ti.dag_id}-{slug}-{digest}-{ti.map_index}-{ti.try_number}.jsonl"
    return directory / name


def _strip_lineage(task: Any) -> None:
    """Remove everything openlineage_spark_settings added: the command is
    then the one of a run without lineage."""
    kept = {
        key: value
        for key, value in (task.conf or {}).items()
        if key not in _LINEAGE_CONF_KEYS and not key.startswith("spark.openlineage.")
    }
    task.conf = kept or None
    task._openlineage_inject_parent_job_info = False
    task._openlineage_inject_transport_info = False


def prepare_spark_lineage(context: Any) -> None:
    """`pre_execute` of spark_read_files (P2-5). Never raises.

    - SEC-2: when `OPENLINEAGE_SPARK_SHA256` is set (the Makefile exports
      the pinned digest), the jar must match it, or the listener is removed
      (a jar for another Scala version passes the zip check, then stops the
      SparkContext). Hashed here, when the task runs, rather than at every
      parse of the DAG file (about 110 ms for 42 MB), and cached by path,
      size and mtime.
    - C-13: the `file` transport gets one location per application.
    - `file_list` is declared as the task's input (report_file_list_input).
    """
    try:
        task = context["task"]
        conf = task.conf or {}
        if conf.get("spark.extraListeners") == OPENLINEAGE_SPARK_LISTENER:
            expected = (os.environ.get(OPENLINEAGE_SPARK_SHA256_ENV) or "").strip().lower()
            jar = conf["spark.jars"]
            actual = _jar_sha256(jar) if expected else None
            if expected and actual != expected:
                log.warning(
                    "%s has SHA-256 %s, not the pinned %s (%s): Spark runs without its "
                    "OpenLineage listener (run `make lineage-jar`)",
                    jar,
                    actual,
                    expected,
                    OPENLINEAGE_SPARK_SHA256_ENV,
                )
                _strip_lineage(task)
            elif conf.get("spark.openlineage.transport.type") == "file":
                default = Path(conf["spark.openlineage.transport.location"])
                location = _application_lineage_file(context, default.parent)
                task.conf = {**conf, "spark.openlineage.transport.location": str(location)}
    except Exception:
        log.warning("OpenLineage: could not prepare the Spark lineage (ignored)", exc_info=True)
    report_file_list_input(context)


def report_file_list_input(context: Any) -> None:
    """Declare `file_list` as the OpenLineage input of spark_read_files: the
    job selects its files there through SQLAlchemy, which no integration
    sees. Returns at once when lineage is off (no-op collector); never
    raises (see utils/file_list_lineage.py)."""
    try:
        from airflow.sdk.lineage import NoOpCollector, get_hook_lineage_collector

        if isinstance(get_hook_lineage_collector(), NoOpCollector):
            return
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from utils.file_list_lineage import report_file_list_lineage
        from utils.functions_utils import FILE_LIST_POSTGRES_CONN_ID

        hook = PostgresHook(postgres_conn_id=FILE_LIST_POSTGRES_CONN_ID)
        report_file_list_lineage(hook, role="input")
    except Exception:
        log.warning("OpenLineage: could not report file_list as input (ignored)", exc_info=True)
