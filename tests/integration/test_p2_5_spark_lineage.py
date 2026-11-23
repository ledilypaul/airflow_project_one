"""
P2-5 — the real Spark job, under spark-submit, with the OpenLineage listener.

The job is pipeline/jobs/read_files_job.py itself (reader, cleaning,
quality checks, ParquetLakeWriter), started by spark-submit with the conf
read_file_dag builds (`openlineage_spark_settings()`, `file` transport under
tmp_path) plus the parent job info the operator injects. Only file_list is
replaced, by a driver wrapper (no Postgres): the rows of the run come from
the command line, the statuses go to a file.

The test checks the events the listener (JVM) wrote:
  - the CSV read is an input, named as the listing names it (`file`, the
    resolved path): the listing and the Spark run share that dataset;
  - raw/movies and staging/movies are outputs (`file`, the table directory,
    written with partitionOverwriteMode=dynamic);
  - staging/movies has exactly the name the dbt bridge expects
    (lake_staging_lineage, dags/local_transform_dag.py): Spark and dbt meet;
  - the Spark application is attached to its Airflow task (parent facet);
and that lineage never fails the job: with an unreachable http backend the
job still exits 0 and writes the lake.

The Task SDK's `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=airflow-db-not-allowed:///`
is set as in a real task: the job must still not import Airflow.

Needs the pinned openlineage-spark jar (`make lineage-jar`, version and
SHA-256 in the Makefile). Without it the tests are skipped, unless
REQUIRE_LINEAGE=1 (set in CI), where a missing jar is a failure.
"""

import json
import os
import re
import socket
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MAKEFILE = PROJECT_ROOT / "Makefile"
IMDB_CONFIG = PROJECT_ROOT / "examples" / "config" / "sources" / "imdb.yaml"
VENV_BIN = Path(sys.executable).parent
LISTENER = "io.openlineage.spark.agent.OpenLineageSparkListener"

pytestmark = pytest.mark.skipif(os.name != "posix", reason="needs a POSIX venv layout")

RUN_ID = "manual__p2_5 lineage:+é"
FILE_ID = 7
PARENT_RUN_ID = str(uuid.uuid4())
ROOT_RUN_ID = str(uuid.uuid4())

DRIVER = textwrap.dedent(
    """
    import json
    import sys

    import pipeline.jobs.read_files_job as job

    run_id, rows, status_file = sys.argv[1], json.loads(sys.argv[2]), sys.argv[3]

    def update_file_status(file_id, status, error_message=None):
        with open(status_file, "a") as f:
            f.write(json.dumps([file_id, status, error_message]) + "\\n")

    job.list_file_from_db = lambda wanted: rows if wanted == run_id else []
    job.update_file_status = update_file_status
    job.main(run_id=run_id)
    """
)


def _makefile_value(name: str) -> str:
    match = re.search(rf"^{name}\s*:=\s*(\S+)\s*$", MAKEFILE.read_text(), re.MULTILINE)
    assert match, f"{name} not found in the Makefile"
    return match.group(1)


@pytest.fixture(scope="module")
def listener_jar() -> Path:
    version = _makefile_value("OPENLINEAGE_SPARK_VERSION")
    jar = PROJECT_ROOT / "lineage" / "jars" / f"openlineage-spark_2.13-{version}.jar"
    if not jar.is_file():
        message = f"{jar} not found: run `make lineage-jar`"
        if os.environ.get("REQUIRE_LINEAGE") == "1":
            pytest.fail(message + " (REQUIRE_LINEAGE=1: these tests must run)")
        pytest.skip(message)
    return jar


def _imdb_csv(path: Path) -> Path:
    """Two rows with every column of config/sources/imdb.yaml, in order."""
    from pipeline.config.registry import load_source_config

    columns = load_source_config(IMDB_CONFIG).reader.columns_type
    sample = {"int": "1", "float": "1.5", "date": "2001-05-01", "boolean": "false"}
    lines = [",".join(columns)]
    for movie_id in (1, 2):
        values = [
            str(movie_id) if name == "id" else sample.get(kind, f"v{movie_id}")
            for name, kind in columns.items()
        ]
        lines.append(",".join(values))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _settings(monkeypatch, jar: Path, events: Path) -> dict:
    """read_file_dag's settings for the dev `file` transport."""
    from dags.local_read_file_dag import openlineage_spark_settings

    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__DISABLED", "False")
    monkeypatch.setenv(
        "AIRFLOW__OPENLINEAGE__TRANSPORT",
        json.dumps({"type": "file", "log_file_path": str(events / "airflow"), "append": False}),
    )
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__NAMESPACE", "p2_5_it")
    monkeypatch.setenv("OPENLINEAGE_SPARK_JAR", str(jar))
    settings = openlineage_spark_settings()
    assert settings["conf"] and settings["openlineage_inject_parent_job_info"], settings
    return settings["conf"]


PARENT_CONF = {
    # What SparkSubmitOperator injects (openlineage_inject_parent_job_info);
    # the injection itself is covered by the unit and dag.test() tests.
    "spark.openlineage.parentJobNamespace": "p2_5_it",
    "spark.openlineage.parentJobName": "read_file_dag.spark_read_files",
    "spark.openlineage.parentRunId": PARENT_RUN_ID,
    "spark.openlineage.rootParentJobNamespace": "p2_5_it",
    "spark.openlineage.rootParentJobName": "read_file_dag",
    "spark.openlineage.rootParentRunId": ROOT_RUN_ID,
}


def _run_job(tmp_path: Path, conf: dict, csv: Path, lake: Path):
    driver = tmp_path / "driver" / "run_job.py"
    driver.parent.mkdir()
    driver.write_text(DRIVER, encoding="utf-8")
    statuses = tmp_path / "statuses.jsonl"
    rows = [{"id": FILE_ID, "file_path": str(csv), "file_name": csv.name}]

    env = {k: v for k, v in os.environ.items() if k not in ("ENV", "VIRTUAL_ENV", "SPARK_HOME")}
    env.update(
        # spark-submit finds Spark through the `python3` of PATH: the venv's
        # one (PySpark 4.3), never another interpreter's PySpark.
        PATH=f"{VENV_BIN}{os.pathsep}{env.get('PATH', '')}",
        PYSPARK_PYTHON=sys.executable,
        PYTHONPATH=str(PROJECT_ROOT),
        LAKE_ROOT=str(lake),
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="airflow-db-not-allowed:///",
    )
    command = [str(VENV_BIN / "spark-submit"), "--master", "local[1]"]
    for key, value in {**conf, "spark.ui.enabled": "false"}.items():
        command += ["--conf", f"{key}={value}"]
    command += [str(driver), RUN_ID, json.dumps(rows), str(statuses)]
    proc = subprocess.run(
        command, env=env, cwd=tmp_path, capture_output=True, text=True, timeout=600
    )
    logs = proc.stdout[-4000:] + proc.stderr[-8000:]
    recorded = [json.loads(line) for line in statuses.read_text().splitlines()]
    return proc.returncode, recorded, logs


def _events(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _events_in(folder: Path) -> list[dict]:
    """Every Spark event of the directory (one file per application)."""
    return [e for path in sorted(folder.glob("spark*.jsonl")) for e in _events(path)]


def _datasets(events: list[dict], key: str) -> set[tuple[str, str]]:
    return {(d["namespace"], d["name"]) for e in events for d in e.get(key, [])}


def _staging_rows(lake: Path) -> int:
    import pyarrow.dataset as ds

    return ds.dataset(lake / "staging" / "movies", format="parquet").count_rows()


def test_p2_5_spark_job_emits_its_inputs_outputs_and_parent(listener_jar, monkeypatch, tmp_path):
    from dags.local_transform_dag import DBT_MANIFEST, lake_staging_lineage

    events_dir = tmp_path / "lineage" / "events"
    conf = _settings(monkeypatch, listener_jar, events_dir)
    csv = _imdb_csv(tmp_path / "data" / "imdb_p2_5.csv")
    lake = tmp_path / "lake"

    code, statuses, logs = _run_job(tmp_path, {**conf, **PARENT_CONF}, csv, lake)

    assert code == 0, logs
    assert statuses == [[FILE_ID, "pending", None], [FILE_ID, "processed", None]], logs
    assert _staging_rows(lake) == 2
    events = _events_in(events_dir)
    assert events, logs

    # Spark 4.3 / Scala 2.13 seen by the listener, every job in our namespace.
    engines = {e["run"]["facets"]["processing_engine"]["version"] for e in events}
    assert engines == {"4.3.0"}
    assert {e["job"]["namespace"] for e in events} == {"p2_5_it"}

    # The application run is the child of the Airflow task.
    [app_start] = [
        e
        for e in events
        if e["eventType"] == "START"
        and e["run"]["facets"]["parent"]["job"]["namespace"] == "p2_5_it"
        and e["run"]["facets"]["parent"]["job"]["name"] == "read_file_dag.spark_read_files"
    ]
    parent = app_start["run"]["facets"]["parent"]
    assert parent["run"]["runId"] == PARENT_RUN_ID
    assert parent["root"]["job"]["name"] == "read_file_dag"
    assert parent["root"]["run"]["runId"] == ROOT_RUN_ID

    completes = [e for e in events if e["eventType"] == "COMPLETE"]
    inputs, outputs = _datasets(completes, "inputs"), _datasets(completes, "outputs")
    # The file read, named as the listing names it (utils/file_list_lineage.py).
    assert ("file", str(csv.resolve())) in inputs
    raw = ("file", f"{lake}/raw/movies")
    staging = ("file", f"{lake}/staging/movies")
    assert {raw, staging} <= outputs, outputs

    # Spark and dbt meet: staging/movies is the input of the dbt bridge.
    manifest = json.loads(DBT_MANIFEST.read_text(encoding="utf-8"))
    bridge = dict(
        lake_staging_lineage(
            manifest, {"LAKE_ROOT": str(lake), "DBT_DUCKDB_PATH": str(tmp_path / "w.duckdb")}
        )
    )
    assert staging in bridge, (staging, list(bridge))
    assert bridge[staging][1].endswith(".lake_staging.movies")


def _closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_p2_5_unreachable_lineage_backend_never_fails_the_job(listener_jar, monkeypatch, tmp_path):
    """http transport (what the provider injects for OPENLINEAGE_URL) to a
    port nobody listens on: the listener logs its errors, the job succeeds
    and writes the lake as without lineage."""
    conf = _settings(monkeypatch, listener_jar, tmp_path / "events")
    for key in [k for k in conf if k.startswith("spark.openlineage.transport")]:
        del conf[key]
    conf.update(
        {
            "spark.openlineage.transport.type": "http",
            "spark.openlineage.transport.url": f"http://127.0.0.1:{_closed_port()}",
            "spark.openlineage.transport.endpoint": "api/v1/lineage",
            "spark.openlineage.transport.timeoutInMillis": "2000",
        }
    )
    csv = _imdb_csv(tmp_path / "data" / "imdb_p2_5.csv")
    lake = tmp_path / "lake"

    code, statuses, logs = _run_job(tmp_path, {**conf, **PARENT_CONF}, csv, lake)

    assert code == 0, logs
    assert statuses[-1] == [FILE_ID, "processed", None], logs
    assert _staging_rows(lake) == 2
    assert LISTENER in logs  # the listener was really there


def test_p2_5_c7_key_value_file_name_is_not_trimmed(listener_jar, monkeypatch, tmp_path):
    """openlineage-spark's KeyValueTrimmer names `orders_dt=2026-11-14.csv`
    as its directory, which is not the dataset the listing declares. It is
    disabled (spark.openlineage.dataset.disabledTrimmers): the input is the
    file itself, and the lake tables (partitioned `key=value` below them)
    keep their names."""
    conf = _settings(monkeypatch, listener_jar, tmp_path / "events")
    csv = tmp_path / "data" / "orders_dt=2026-11-14.csv"
    csv.parent.mkdir(parents=True)
    # P2-6: the header of orders.yaml's columns_type (checked by Spark).
    csv.write_text(
        "order_id,customer,amount,order_date\n1,a,10.5,2026-11-14\n2,b,3.0,2026-11-14\n",
        encoding="utf-8",
    )
    lake = tmp_path / "lake"

    code, statuses, logs = _run_job(tmp_path, conf, csv, lake)

    assert code == 0, logs
    assert statuses[-1] == [FILE_ID, "processed", None], logs
    completes = [e for e in _events_in(tmp_path / "events") if e["eventType"] == "COMPLETE"]
    inputs, outputs = _datasets(completes, "inputs"), _datasets(completes, "outputs")
    assert ("file", str(csv.resolve())) in inputs
    assert ("file", str(csv.parent.resolve())) not in inputs
    assert {("file", f"{lake}/raw/orders"), ("file", f"{lake}/staging/orders")} <= outputs
