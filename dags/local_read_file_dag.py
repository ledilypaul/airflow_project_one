"""
DAG: read_file_dag
Submits a Spark job (SparkSubmitOperator) to read and process files listed in the DB.

Airflow orchestre, Spark s'exécute dans son propre processus séparé.

P1-4 : le DAG est planifié sur l'Asset `file_list`, émis par la tâche
d'insertion de `list_files_dag` / `list_files_dag_classic` quand elle a
enregistré au moins un fichier.

1. `resolve_source_run_ids` détermine les run ids sources à traiter :
   - run déclenché par l'Asset : le `source_run_id` de **tous** les événements
     `file_list` consommés, dédupliqués, dans l'ordre. Avec
     `batch_asset_events=False` (P3-5, Airflow 3.4) un run consomme un seul
     événement, donc un seul listing ; la boucle reste correcte si le
     regroupement est réactivé ;
   - déclenchement manuel (aucun événement) : `dag_run.conf["run_id"]`, sans
     espaces autour ;
   - ni l'un ni l'autre : échec explicite (jamais de succès silencieux).
2. `spark_read_files` (P1-5) est un `SparkSubmitOperator` mappé dynamiquement
   sur `application_args=resolve_source_run_ids().map(build_application_args)` :
   une instance par run id source, avec son propre code de sortie, son retry
   et son statut. Le hook lance `spark-submit` sous forme de liste
   d'arguments, sans shell : le run id est un seul argument `--run-id=<id>`,
   jamais découpé ni interprété.
   - Master Spark : Connection `spark_default` (`local[*]` en dev, voir
     CLAUDE.md). Le hook retomberait silencieusement sur `yarn` si elle
     manquait : `resolve_source_run_ids` vérifie donc qu'elle existe et fait
     échouer le run sinon (P1-5 / C-2).
   - Pool `spark` (deploy/airflow/pools.json) : limite le nombre
     d'applications Spark simultanées. S'il n'existe pas, le scheduler laisse
     la tâche en `scheduled` sans erreur visible dans l'UI (avertissement
     dans le log du scheduler seulement), jusqu'à `dagrun_timeout`.
   - Ressources (mémoire, cœurs...) : pas dans le DAG, mais dans la
     Connection / `spark-defaults.conf` de chaque environnement.
   - Retries (P2-6) : `ReadFilesSparkSubmitOperator`, une sous-classe de
     `SparkSubmitOperator`, convertit les codes de sortie déterministes du
     job (2 run id invalide, 3 aucune ligne, 5 config ou LAKE_ROOT invalide,
     6 échec qualité bloquant seulement) en `AirflowFailException` : pas de
     retry, qui échouerait de la même façon. 1 (un fichier en échec pour une
     autre raison), 4 (parent disparu) et toute autre erreur gardent le retry.
   - Sortie (P2-2) : le job écrit chaque fichier en Parquet partitionné sous
     `LAKE_ROOT` (zones raw et staging, voir pipeline/jobs/read_files_job.py).
     `LAKE_ROOT` n'est pas fixé ici : le job le lit dans l'environnement du
     worker, hérité par `spark-submit` en mode local/client (en mode cluster,
     le passer au driver par la conf Spark de l'environnement).
   - Kubernetes (P3-2) : seule la Connection change (master `k8s://...`,
     `deploy-mode` cluster, `namespace`) ; le reste vient du
     `spark-defaults.conf` de l'environnement (deploy/k8s/charts/project).
     Le job est alors passé en `local://<chemin>` (il est dans l'image du
     pod driver) et le code de sortie est celui du conteneur driver, lu par
     le hook dans la sortie de spark-submit (qui, lui, sort en 0) : mêmes
     codes sans retry qu'en local.
3. `publish_lake_staging` (P2-4) émet l'Asset `lake_staging`, qui planifie
   `transform_dag` (dbt). Règle `all_done` (C-2) : elle s'exécute une fois
   toutes les instances `spark_read_files` terminées, réussies ou non. Un seul
   fichier en échec permanent (exit 1) ne doit pas bloquer dbt pour toujours :
   elle émet dès qu'au moins un fichier des run ids résolus est `processed`
   dans `file_list` (Connection `file_list_db`), et se met en `skipped` (rien
   d'émis) sinon, ou quand `resolve_source_run_ids` a échoué (aucun run id).
   dbt reconstruit l'état courant de toute la zone staging : il prend les
   fichiers traités, pas les fichiers en échec.
   `fail_if_spark_failed` (règle `one_failed`) garde le run en échec : l'état
   d'un DagRun vient de ses feuilles, et `publish_lake_staging` réussie aurait
   sinon rendu vert un run dont une application Spark a échoué. Elle est
   `skipped` quand tout a réussi.
4. `max_active_runs=1` : deux runs ne traitent jamais le même run source en
   parallèle. Les événements arrivés pendant un run restent en file et
   donnent chacun leur run, l'un après l'autre (P3-5 : un run par événement,
   `batch_asset_events=False`). Un événement émis pendant que le DAG est en
   pause n'est pas mis en file : aucun run ne le consomme ensuite (Airflow
   3.4, `catchup=False`), voir CLAUDE.md.
5. Lignage OpenLineage (P2-5), configuré par l'environnement seulement :
   - `openlineage_spark_settings()` active le listener JVM
     (`OpenLineageSparkListener`) par la conf de `spark-submit`, jamais par un
     import dans le job. Rien n'est ajouté (commande identique à avant P2-5)
     quand OpenLineage est désactivé ou qu'aucun jar n'est configuré
     (`OPENLINEAGE_SPARK_JAR`, fichier présent) : une classe de listener
     absente du classpath empêcherait SparkContext de démarrer ;
   - transport `file` (dev) : les événements Spark vont dans `spark.jsonl`, à
     côté de ceux d'Airflow ; tout autre transport (http) est injecté par le
     provider (`openlineage_inject_transport_info`) ;
   - le run Spark est rattaché à sa tâche (`openlineage_inject_parent_job_info`) ;
   - `prepare_spark_lineage` (`pre_execute`) vérifie le SHA-256 du jar
     (`OPENLINEAGE_SPARK_SHA256`), donne à chaque application son fichier
     d'événements et déclare `file_list` en entrée de `spark_read_files` (le
     job y choisit ses fichiers). Le lignage ne fait jamais échouer la tâche.

`application_args` est un champ templaté de `SparkSubmitOperator` : la valeur
mappée depuis l'XCom n'est pas rendue par Jinja (Airflow ignore les valeurs
résolues à l'expansion). Ne jamais construire le run id dans une chaîne
templatée (f-string dans `application_args` passé à `.partial()`,
`@task.bash`...) : Jinja la rendrait, ce qui rouvrirait l'injection de template
depuis un run_id manuel (voir tests/integration/test_p1_4_read_file_dag_e2e.py).
"""

import json
import logging
import os
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Asset, AssetTriggeredTimetable, Param, TriggerRule, dag, task

# Same name as in the listing DAGs: Airflow matches Assets by name/URI, not by
# object identity. Declared here rather than in a shared `dags.*` module, which
# would only be importable with the project root on sys.path (C-1).
FILE_LIST_ASSET = Asset(name="file_list")
# P2-4: emitted once the Spark job wrote the staging zone; transform_dag is
# scheduled on it (same name declared in dags/local_transform_dag.py).
LAKE_STAGING_ASSET = Asset(name="lake_staging")

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


# P3-3: failure reports (one structured `airflow_failure` log line, see
# utils/failure_alert.py). Imported inside the callbacks: a DAG file imports no
# project code at parse time (the DAG processor does not have the project root
# on sys.path). The report never raises. If `utils` cannot be imported (a worker
# image without the project), a minimal line is logged instead (dag_id, run_id,
# reason): a failure is never silent. The four DAG files carry the same few
# lines, because a shared module would itself be project code.
def _report_failure(context: Any, scope: str) -> None:
    try:
        from utils import failure_alert
    except ImportError:
        try:
            get = context.get if isinstance(context, Mapping) else {}.get
            ti, dag = get("ti"), get("dag")
            record = {
                "scope": scope,
                "dag_id": getattr(ti, "dag_id", None) or getattr(dag, "dag_id", None),
                "run_id": get("run_id") or getattr(ti, "run_id", None),
                "reason": get("reason"),
                "task_id": getattr(ti, "task_id", None),
            }
            line = json.dumps(
                {k: v for k, v in record.items() if v is not None}, sort_keys=True, default=str
            )
            logging.getLogger(__name__).error("airflow_failure %s", line)
        except Exception:
            pass
        return
    report = (
        failure_alert.log_task_failure if scope == "task" else failure_alert.log_dag_run_failure
    )
    report(context)


def on_task_failure(context: Any) -> None:
    _report_failure(context, "task")


def on_dag_run_failure(context: Any) -> None:
    _report_failure(context, "dag_run")


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
}

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
# KeyValueTrimmer names a file such as `orders_dt=2026-11-14.csv` after its
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

# Valeurs de `run_id` qui ne désignent aucun run : refusées comme le fait déjà
# le job Spark (code de sortie 2), mais ici avant même de lancer spark-submit.
_MISSING_RUN_ID_VALUES = {"", "None"}

log = logging.getLogger(__name__)


def build_application_args(run_id: str) -> list[str]:
    """Return the job arguments (`application_args`) for one source run id.

    The run id is passed as a single `--run-id=<value>` argument so that a
    value starting with `-` (possible from a manual trigger) is not parsed as
    an option by the job (C-4). No shell is involved: SparkSubmitHook passes
    the argument list to `subprocess.Popen` as is.
    """
    return [f"--run-id={run_id}"]


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
        from airflow.sdk.exceptions import AirflowException, AirflowFailException

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
    source file `orders_dt=2026-11-14.csv` keeps its own name), and:
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


@dag(
    dag_id="read_file_dag",
    description="Submit a Spark job to read files listed in the database",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2023, 1, 1),
    # P3-5: one run per `file_list` event, i.e. per listing: one Spark
    # application per source run, with its own retries and state. Explicit,
    # because Airflow 3.4's default comes from `[scheduler] batch_asset_events`
    # (False) and 3.3 batched every queued event into one run.
    schedule=AssetTriggeredTimetable(FILE_LIST_ASSET, batch_asset_events=False),
    catchup=False,
    max_active_runs=1,
    # P1-5 / C-3: without it, a missing `spark` pool (task left in
    # `scheduled`, silently) or a stuck spark-submit freezes the run forever,
    # and with max_active_runs=1 every later `file_list` event piles up behind
    # it. On timeout the scheduler fails the run and marks its unfinished
    # tasks `skipped` (a running spark-submit is killed; the job's parent
    # watchdog then stops the Python driver, C-1).
    dagrun_timeout=timedelta(hours=6),
    # P3-3: also covers what no task callback sees, such as that timeout.
    on_failure_callback=on_dag_run_failure,
    tags=["processing", "spark", "file-reading"],
    params={
        # UI form + validation of the run conf only: the task reads
        # dag_run.conf, not params (Param.resolve() memoises its value, C-5).
        "run_id": Param(
            None,
            type=["null", "string"],
            description="Manual trigger only: run_id of the list_files_dag run to process. "
            "Leave empty for Asset-triggered runs (the source run ids come from the "
            "triggering `file_list` events). A run with neither fails explicitly "
            "(idempotence: P1-3/C-1, P1-4).",
        )
    },
)
def read_file_dag():
    @task
    def resolve_source_run_ids(
        triggering_asset_events: Mapping[Any, Any] | None = None,
        dag_run: Any = None,
    ) -> list[str]:
        from airflow.sdk import BaseHook
        from airflow.sdk.exceptions import AirflowFailException, AirflowNotFoundException

        # P1-5 / C-2: SparkSubmitHook silently falls back to `--master yarn`
        # when its Connection is missing. Fail here, explicitly, instead.
        try:
            spark_conn = BaseHook.get_connection(SPARK_CONN_ID)
        except AirflowNotFoundException:
            raise AirflowFailException(
                f"Airflow Connection {SPARK_CONN_ID!r} not found: spark_read_files would "
                "silently run `spark-submit --master yarn`. Create it first (see CLAUDE.md, "
                "'Running Airflow locally')."
            ) from None
        log.info("Spark master from Connection %r: host=%r", SPARK_CONN_ID, spark_conn.host)

        events = (triggering_asset_events or {}).get(FILE_LIST_ASSET, [])
        run_ids: list[str] = []
        for event in events:
            if event.source_run_id is None:
                # Event created through the REST API, not by a producer task:
                # it cannot be scoped to a list_files_dag run.
                log.warning("Ignoring `file_list` asset event without source_run_id: %s", event)
                continue
            if event.source_run_id not in run_ids:
                run_ids.append(event.source_run_id)

        # Read from this run's conf (already validated against the Param
        # schema when the run was created), not from `params`: the Param
        # object memoises the last resolved value across runs (C-5).
        conf = getattr(dag_run, "conf", None) or {}
        manual_run_id = conf.get("run_id")
        if manual_run_id is not None:
            manual_run_id = str(manual_run_id).strip()
            if manual_run_id in _MISSING_RUN_ID_VALUES:
                manual_run_id = None

        if run_ids:
            if manual_run_id is not None:
                log.warning(
                    "conf run_id=%r ignored: run triggered by `file_list` asset events",
                    manual_run_id,
                )
            log.info("Source run ids from asset events: %s", run_ids)
            return run_ids

        if manual_run_id is not None:
            log.info("No asset event, using conf run_id=%r", manual_run_id)
            return [manual_run_id]

        raise AirflowFailException(
            "No source run_id to process: no `file_list` asset event triggered this run and "
            "the run conf has no run_id. Trigger read_file_dag with a run_id (Trigger DAG w/ "
            "config) to target a specific list_files_dag run (idempotence: P1-3/C-1, P1-4)."
        )

    @task(outlets=[LAKE_STAGING_ASSET], trigger_rule=TriggerRule.ALL_DONE)
    def publish_lake_staging(ti: Any = None) -> int:
        """Emit `lake_staging` (P2-4 / C-2) when at least one file of the
        resolved run ids is `processed` in file_list; skip (no event) otherwise.

        Runs once every spark_read_files instance is done (`all_done`), so a
        single permanently failing file does not block dbt forever. The run
        ids are pulled from the XCom rather than passed as an argument: when
        resolve_source_run_ids failed there is none, and the task skips
        instead of failing on a missing XCom.
        """
        from airflow.sdk.exceptions import AirflowSkipException

        from utils import functions_utils

        run_ids = ti.xcom_pull(task_ids="resolve_source_run_ids")
        if not run_ids:
            raise AirflowSkipException(
                "no source run id (resolve_source_run_ids failed): `lake_staging` not emitted"
            )
        processed = functions_utils.count_processed_files(run_ids)
        if processed == 0:
            raise AirflowSkipException(
                f"no file_list row is `processed` for run_ids={run_ids}: nothing new in the "
                "lake, `lake_staging` not emitted"
            )
        log.info("%d processed file(s): emitting the `lake_staging` asset", processed)
        return processed

    # on_failure_callback=None: the failed spark_read_files instance has already
    # reported itself, and the DAG-level callback reports the run (P3-3).
    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0, on_failure_callback=None)
    def fail_if_spark_failed() -> None:
        """Keep the run failed when a spark_read_files instance failed (or was
        upstream_failed): a DagRun takes its state from its leaves, and the
        `all_done` publish_lake_staging leaf would otherwise turn it green.
        Skipped when every instance succeeded."""
        from airflow.sdk.exceptions import AirflowFailException

        raise AirflowFailException(
            "at least one spark_read_files instance failed or did not run: the run fails "
            "(`lake_staging` was emitted only if some file was processed)"
        )

    # P2-6: the provider's operator, minus the retries of deterministic exits.
    spark_read_files = ReadFilesSparkSubmitOperator.partial(
        task_id="spark_read_files",
        application=str(JOB_PATH),
        conn_id=SPARK_CONN_ID,
        pool=SPARK_POOL,
        # Local/client mode: the hook *adds* these to the worker environment
        # (os.environ.copy().update(env_vars)), PATH/JAVA_HOME/SPARK_HOME/ENV
        # are kept. Lets the job's Python workers import pipeline.*/utils.*
        # even with an interpreter that lacks the editable install.
        # Never pass a secret here: in yarn/k8s mode the hook writes env_vars
        # into the logged spark-submit command (`--conf spark.*.env.X=...`) in
        # clear, masking only keys containing `secret` or `password` (C-6).
        env_vars={"PYTHONPATH": str(PROJECT_ROOT)},
        # P2-5: OpenLineage listener, transport and parent job info (or
        # nothing at all when lineage is off), from the environment.
        **openlineage_spark_settings(),
        pre_execute=prepare_spark_lineage,
    ).expand(application_args=resolve_source_run_ids().map(build_application_args))

    spark_read_files >> [publish_lake_staging(), fail_if_spark_failed()]


dag_instance = read_file_dag()
