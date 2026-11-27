"""
P5-3 — the DAG factory (`dags/source_pipelines.py`): one DAG `ingest__<source>` per YAML
of the config root, `ingest__unmatched` for the files no source claims, and an
`invalid_source__<file>` for each config that does not validate.

Covers the acceptance criteria of the backlog entry:
  - a YAML added to a temporary config root makes its DAG appear in the DagBag, with the
    expected tasks, schedule, retries, tags and Asset;
  - an invalid YAML gives `invalid_source__<file>` (one task failing with the validation
    message) and the other DAGs load, with no import error;
  - importing the file loads neither PySpark nor Polars;
  - parsing 50 generated sources stays under a second;
  - the engine default and the config root of the parse reach the job, in local mode and
    as Spark conf on Kubernetes (`PIPELINE_DEFAULT_ENGINE` was not propagated before);
  - `transform_dag` is scheduled on the Assets of the valid sources only.
"""

import json
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest
import yaml
from airflow.models import DagBag
from airflow.sdk.exceptions import AirflowFailException

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / "dags"
FACTORY_FILE = DAGS_FOLDER / "source_pipelines.py"
TRANSFORM_DAG_FILE = DAGS_FOLDER / "local_transform_dag.py"


def _source(name: str, **extra) -> dict:
    return {
        "source": {"name": name, "format": "csv", "patterns": [f"{name}_*.csv"]},
        "reader": {"options": {"header": True}},
        "storage": {
            "format": "parquet",
            "mode": "overwrite",
            "raw_path": "raw/",
            "staging_path": "staging/",
        },
        **extra,
    }


def _root(tmp_path: Path, sources: dict[str, dict | str]) -> Path:
    """A config root with the given sources/<file>.yaml (a str is written as is)."""
    root = tmp_path / "config"
    (root / "sources").mkdir(parents=True)
    for stem, content in sources.items():
        text = content if isinstance(content, str) else yaml.safe_dump(content)
        (root / "sources" / f"{stem}.yaml").write_text(text)
    return root


def _schedule(dag):
    """The cron expression or preset of a DAG, None for a manual one."""
    return dag.schedule


def _bag(root: Path, monkeypatch) -> DagBag:
    monkeypatch.setenv("PIPELINE_CONFIG_ROOT", str(root))
    return DagBag(dag_folder=str(FACTORY_FILE))


# ---------------------------------------------------------------------------
# A YAML makes a DAG
# ---------------------------------------------------------------------------


def test_p5_3_a_yaml_in_a_temporary_root_makes_its_dag(tmp_path, monkeypatch):
    invoices = _source(
        "invoices",
        schedule="0 6 * * *",
        retries=2,
        tags=["finance"],
        listing={"directories": ["/data/in"], "exclude": ["*_old.csv"]},
    )
    bag = _bag(_root(tmp_path, {"invoices": invoices}), monkeypatch)

    assert bag.import_errors == {}
    assert set(bag.dag_ids) == {"ingest__invoices", "ingest__unmatched"}
    dag = bag.dags["ingest__invoices"]
    assert set(dag.task_ids) == {
        "list_files",
        "insert_files",
        "spark_read_files",
        "publish_lake_staging",
        "fail_if_spark_failed",
    }
    assert _schedule(dag) == "0 6 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert {"ingest", "finance"} <= set(dag.tags)
    # `retries` is the source's, for every task but the guard.
    assert dag.get_task("spark_read_files").retries == 2
    assert dag.get_task("insert_files").retries == 2
    assert dag.get_task("fail_if_spark_failed").retries == 0
    assert [a.name for a in dag.get_task("publish_lake_staging").outlets] == [
        "lake_staging__invoices"
    ]


def test_p5_3_defaults_are_manual_one_retry_and_the_default_directories(tmp_path, monkeypatch):
    bag = _bag(_root(tmp_path, {"invoices": _source("invoices")}), monkeypatch)

    dag = bag.dags["ingest__invoices"]
    assert _schedule(dag) is None  # NullTimetable: manual
    assert dag.get_task("spark_read_files").retries == 1
    assert set(dag.tags) == {"ingest"}


def test_p5_3_preset_schedule_and_null_schedule(tmp_path, monkeypatch):
    bag = _bag(
        _root(
            tmp_path,
            {
                "daily": _source("daily", schedule="@daily"),
                "manual": _source("manual", schedule=None),
            },
        ),
        monkeypatch,
    )
    assert bag.import_errors == {}
    assert _schedule(bag.dags["ingest__daily"]) == "@daily"
    assert _schedule(bag.dags["ingest__manual"]) is None


def test_p5_3_a_source_without_staging_has_no_asset_and_no_publish_task(tmp_path, monkeypatch):
    raw_only = _source("raw_only")
    del raw_only["storage"]["staging_path"]
    bag = _bag(_root(tmp_path, {"raw_only": raw_only}), monkeypatch)

    dag = bag.dags["ingest__raw_only"]
    assert "publish_lake_staging" not in dag.task_ids
    assert all(task.outlets == [] for task in dag.tasks)


def test_p5_3_list_files_uses_the_patterns_the_exclusions_and_the_directories(
    tmp_path, monkeypatch
):
    data = tmp_path / "in"
    data.mkdir()
    for name in ("inv_1.csv", "INV_2.CSV", "inv_old.csv", "other.csv"):
        (data / name).write_text("a\n1\n")
    source = _source("inv", listing={"directories": [str(data)], "exclude": ["*_old.csv"]})
    source["source"]["patterns"] = ["inv_*.csv"]
    bag = _bag(_root(tmp_path, {"inv": source}), monkeypatch)

    files = bag.dags["ingest__inv"].get_task("list_files").python_callable()

    assert [Path(f).name for f in files] == ["INV_2.CSV", "inv_1.csv"]


def test_p5_3_unmatched_dag_lists_what_no_source_claims(tmp_path, monkeypatch):
    data = tmp_path / "data"
    data.mkdir()
    for name in ("inv_1.csv", "stray.csv", "stray.parquet", "notes.txt", "x.json"):
        (data / name).write_text("x")
    source = _source("inv")
    source["source"]["patterns"] = ["inv_*.csv"]
    monkeypatch.chdir(tmp_path)  # `./data/` of config/pipeline.yaml
    bag = _bag(_root(tmp_path, {"inv": source}), monkeypatch)

    files = bag.dags["ingest__unmatched"].get_task("list_files").python_callable()

    # A default per extension reads csv, txt, parquet and xlsx: not json, not inv_*.csv.
    assert sorted(Path(f).name for f in files) == ["notes.txt", "stray.csv", "stray.parquet"]
    assert _schedule(bag.dags["ingest__unmatched"]) is None


# ---------------------------------------------------------------------------
# An invalid config does not break the others
# ---------------------------------------------------------------------------

BAD_NAME = _source("Bad Name")
UNKNOWN_KEY = {**_source("typo"), "listin": {"directories": ["./x"]}}
BAD_SCHEDULE = _source("sched", schedule="every day")
BAD_RETRIES = _source("retr", retries=-1)
RESERVED = _source("unmatched")
SYNTAX_ERROR = "source: [unclosed\n"
CRON_AIRFLOW_REFUSES = _source("cron", schedule="99 99 * * *")


@pytest.mark.parametrize(
    ("stem", "content", "message"),
    [
        ("bad_name", BAD_NAME, "source.name"),
        ("typo", UNKNOWN_KEY, "listin"),
        ("sched", BAD_SCHEDULE, "schedule"),
        ("retr", BAD_RETRIES, "retries"),
        ("reserved", RESERVED, "unmatched"),
        ("syntax", SYNTAX_ERROR, "illisible"),
        # Passes the model's light check, refused by Airflow when the DAG is built.
        ("cron", CRON_AIRFLOW_REFUSES, ""),
    ],
)
def test_p5_3_invalid_yaml_gives_an_invalid_source_dag_and_the_others_load(
    tmp_path, monkeypatch, stem, content, message
):
    bag = _bag(_root(tmp_path, {"good": _source("good"), stem: content}), monkeypatch)

    assert bag.import_errors == {}
    assert set(bag.dag_ids) == {"ingest__good", "ingest__unmatched", f"invalid_source__{stem}"}
    dag = bag.dags[f"invalid_source__{stem}"]
    assert dag.task_ids == ["report_invalid_config"]
    assert _schedule(dag) is None
    with pytest.raises(AirflowFailException) as raised:
        dag.get_task("report_invalid_config").python_callable()
    assert f"{stem}.yaml" in str(raised.value)
    assert message in str(raised.value)
    # The valid source is untouched.
    assert "spark_read_files" in bag.dags["ingest__good"].task_ids


def test_p5_3_two_sources_with_the_same_name_the_second_is_invalid(tmp_path, monkeypatch):
    bag = _bag(_root(tmp_path, {"a": _source("same"), "b": _source("same")}), monkeypatch)

    assert bag.import_errors == {}
    assert set(bag.dag_ids) == {"ingest__same", "ingest__unmatched", "invalid_source__b"}
    with pytest.raises(AirflowFailException, match="déclaré deux fois"):
        bag.dags["invalid_source__b"].get_task("report_invalid_config").python_callable()


def test_p5_3_nested_lake_tables_make_the_later_source_invalid(tmp_path, monkeypatch):
    nested = _source("deep")
    nested["storage"]["raw_path"] = "raw/orders/"  # raw/orders/deep sits inside raw/orders
    bag = _bag(
        _root(tmp_path, {"a_orders": _source("orders"), "b_deep": nested}),
        monkeypatch,
    )

    assert set(bag.dag_ids) == {"ingest__orders", "ingest__unmatched", "invalid_source__b_deep"}
    with pytest.raises(AirflowFailException, match="imbriquées"):
        bag.dags["invalid_source__b_deep"].get_task("report_invalid_config").python_callable()


def test_p5_3_an_invalid_default_config_is_an_invalid_source_too(tmp_path, monkeypatch):
    root = _root(tmp_path, {"good": _source("good")})
    (root / "defaults").mkdir()
    (root / "defaults" / "csv.yaml").write_text("source: {name: default_csv}\n")
    bag = _bag(root, monkeypatch)

    assert bag.import_errors == {}
    assert "invalid_source__csv" in bag.dag_ids
    assert "ingest__good" in bag.dag_ids


def test_p5_3_two_invalid_files_with_the_same_name_get_two_dags(tmp_path, monkeypatch):
    root = _root(tmp_path, {"csv": "source: [x\n"})
    (root / "defaults").mkdir()
    (root / "defaults" / "csv.yaml").write_text("source: [y\n")
    bag = _bag(root, monkeypatch)

    assert {"invalid_source__csv", "invalid_source__csv_2"} <= set(bag.dag_ids)


def test_p5_3_a_missing_config_root_is_an_import_error(tmp_path, monkeypatch):
    """A global misconfiguration: the import error (and its alert) is the signal."""
    bag = _bag(tmp_path / "nope", monkeypatch)

    assert set(bag.import_errors) == {str(FACTORY_FILE)}
    assert "introuvable" in next(iter(bag.import_errors.values()))


# ---------------------------------------------------------------------------
# Parsing is pure and fast
# ---------------------------------------------------------------------------


def _fresh(script: str, **env) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        cwd=PROJECT_ROOT,
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=300,
    )


def test_p5_3_importing_the_factory_loads_no_pyspark_and_no_polars(tmp_path):
    """Checked in a fresh process: the pytest one has imported everything."""
    result = _fresh(
        """
        import sys

        import dags.source_pipelines as module

        assert any(k.startswith("ingest__") for k in vars(module)), "no DAG built"
        loaded = sorted({m.split(".")[0] for m in sys.modules} & {"pyspark", "polars", "py4j"})
        if loaded:
            print("loaded: " + ", ".join(loaded))
            sys.exit(1)
        print("OK")
        """,
        PIPELINE_CONFIG_ROOT=str(PROJECT_ROOT / "examples" / "config"),
        AIRFLOW__CORE__UNIT_TEST_MODE="True",
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{tmp_path / 'airflow.db'}",
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr[-3000:]}"
    assert result.stdout.strip().endswith("OK")


def test_p5_3_parsing_50_sources_takes_less_than_a_second(tmp_path):
    from dags.source_pipelines import build_dags

    root = _root(tmp_path, {f"source_{i:02d}": _source(f"source_{i:02d}") for i in range(50)})

    started = time.perf_counter()
    dags = build_dags(root)
    elapsed = time.perf_counter() - started

    assert len([d for d in dags if d.startswith("ingest__source_")]) == 50
    assert not [d for d in dags if d.startswith("invalid_source__")]
    assert elapsed < 1.0, f"{elapsed:.2f}s for 50 sources"


# ---------------------------------------------------------------------------
# The job's environment: engine default and config root of the parse (P5-2 follow-up)
# ---------------------------------------------------------------------------


def test_p5_3_job_env_vars_carry_the_engine_default_and_the_config_root(monkeypatch, tmp_path):
    from utils.read_files_operator import job_env_vars

    monkeypatch.delenv("PIPELINE_DEFAULT_ENGINE", raising=False)
    monkeypatch.setenv("PIPELINE_CONFIG_ROOT", str(tmp_path))
    assert job_env_vars() == {
        "PYTHONPATH": str(PROJECT_ROOT),
        "PIPELINE_DEFAULT_ENGINE": "spark",
        "PIPELINE_CONFIG_ROOT": str(tmp_path),
    }

    monkeypatch.setenv("PIPELINE_DEFAULT_ENGINE", "polars")
    assert job_env_vars()["PIPELINE_DEFAULT_ENGINE"] == "polars"


def test_p5_3_an_unknown_default_engine_is_an_import_error_naming_the_variable(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("PIPELINE_DEFAULT_ENGINE", "duckdb")
    bag = _bag(_root(tmp_path, {"good": _source("good")}), monkeypatch)

    (message,) = bag.import_errors.values()
    assert "PIPELINE_DEFAULT_ENGINE" in message


def test_p5_3_the_spark_task_of_a_dag_has_the_parse_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DEFAULT_ENGINE", "polars")
    spark_source = _source("inv")
    spark_source["source"]["engine"] = "spark"  # its Spark options are not valid for Polars
    bag = _bag(_root(tmp_path, {"inv": spark_source}), monkeypatch)
    assert bag.import_errors == {}
    env = bag.dags["ingest__inv"].get_task("spark_read_files").env_vars
    assert env["PIPELINE_DEFAULT_ENGINE"] == "polars"
    assert env["PIPELINE_CONFIG_ROOT"] == str(tmp_path / "config")


def test_p5_3_kubernetes_driver_and_executors_get_the_engine_default(monkeypatch):
    """On Kubernetes the driver does not inherit the worker's environment: the hook
    writes `env_vars` as `spark.kubernetes.driverEnv.*` / `spark.executorEnv.*`."""
    from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook

    from utils.read_files_operator import job_env_vars

    monkeypatch.setenv(
        "AIRFLOW_CONN_SPARK_K8S_P5_3",
        json.dumps(
            {
                "conn_type": "spark",
                "host": "k8s://https://kubernetes.default.svc",
                "port": 443,
                "extra": {"deploy-mode": "cluster", "namespace": "ns"},
            }
        ),
    )
    monkeypatch.setenv("PIPELINE_DEFAULT_ENGINE", "polars")
    hook = SparkSubmitHook(conn_id="spark_k8s_p5_3", env_vars=job_env_vars())

    command = hook._build_spark_submit_command("local:///opt/airflow/job.py")

    assert "spark.kubernetes.driverEnv.PIPELINE_DEFAULT_ENGINE=polars" in command
    assert (
        "spark.kubernetes.driverEnv.PIPELINE_CONFIG_ROOT="
        + os.environ.get("PIPELINE_CONFIG_ROOT", str(PROJECT_ROOT / "config"))
        in command
    )


# ---------------------------------------------------------------------------
# transform_dag follows the sources
# ---------------------------------------------------------------------------


def test_p5_3_transform_dag_is_scheduled_on_the_assets_of_the_valid_sources(tmp_path, monkeypatch):
    from airflow.sdk import Asset, AssetAny

    raw_only = _source("raw_only")
    del raw_only["storage"]["staging_path"]
    root = _root(
        tmp_path,
        {"a": _source("a"), "b": _source("b"), "raw_only": raw_only, "broken": "source: [x\n"},
    )
    monkeypatch.setenv("PIPELINE_CONFIG_ROOT", str(root))
    bag = DagBag(dag_folder=str(TRANSFORM_DAG_FILE))

    assert bag.import_errors == {}
    timetable = bag.dags["transform_dag"].timetable
    assert timetable.batch_asset_events is True
    assert timetable.asset_condition == AssetAny(
        Asset(name="lake_staging__a"), Asset(name="lake_staging__b")
    )


def test_p5_3_transform_dag_is_manual_without_a_staged_source(tmp_path, monkeypatch):
    raw_only = _source("raw_only")
    del raw_only["storage"]["staging_path"]
    monkeypatch.setenv("PIPELINE_CONFIG_ROOT", str(_root(tmp_path, {"raw_only": raw_only})))
    bag = DagBag(dag_folder=str(TRANSFORM_DAG_FILE))

    assert bag.import_errors == {}
    assert _schedule(bag.dags["transform_dag"]) is None
