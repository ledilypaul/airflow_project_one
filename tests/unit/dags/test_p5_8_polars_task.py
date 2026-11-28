"""P5-8 : la tâche `polars_read_files` des DAG d'une source `engine: polars`.

Vit sous tests/unit/dags : elle importe Airflow, qui a besoin de la
configuration isolée du conftest de ce dossier.
"""

import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import yaml

from utils import exit_codes, polars_task

FACTORY = "dags/source_pipelines.py"


def _source(name: str, engine: str, **extra) -> dict:
    options = {"has_header": True} if engine == "polars" else {"header": True}
    return {
        "source": {"name": name, "format": "csv", "patterns": [f"{name}_*.csv"], "engine": engine},
        "reader": {"options": options},
        "storage": {
            "format": "parquet",
            "mode": "overwrite",
            "raw_path": "raw/",
            "staging_path": "staging/",
        },
        **extra,
    }


def _dags(tmp_path, monkeypatch):
    from dags.source_pipelines import build_dags

    root = tmp_path / "config"
    (root / "sources").mkdir(parents=True)
    quality = {"quality": {"checks": [{"type": "row_count", "min": 1}]}}
    for name, engine in (("pol", "polars"), ("spk", "spark")):
        source = _source(name, engine, **(quality if engine == "polars" else {}))
        (root / "sources" / f"{name}.yaml").write_text(yaml.safe_dump(source))
    monkeypatch.setenv("PIPELINE_CONFIG_ROOT", str(root))
    return build_dags(root)


def test_p5_8_polars_source_gets_the_polars_task(tmp_path, monkeypatch):
    dags = _dags(tmp_path, monkeypatch)
    polars, spark = dags["ingest__pol"], dags["ingest__spk"]
    assert set(polars.task_ids) == {
        "list_files",
        "insert_files",
        "polars_read_files",
        "fail_if_polars_failed",
        "publish_lake_staging",
    }
    read = polars.get_task("polars_read_files")
    assert read.pool == "polars"
    assert read.retries == 1
    assert {t.task_id for t in read.downstream_list} == {
        "fail_if_polars_failed",
        "publish_lake_staging",
    }
    assert {"spark_read_files", "fail_if_spark_failed"} <= set(spark.task_ids)


def test_p5_8_pool_is_declared():
    import json
    from pathlib import Path

    pools = json.loads(Path("deploy/airflow/pools.json").read_text())
    assert pools["polars"]["slots"] >= 1


def _fake_job(code: int, lines: str = "line 1\\nline 2\\n"):
    script = f"import sys; sys.stdout.write('{lines}'); sys.stdout.flush(); sys.exit({code})"
    return lambda args: [sys.executable, "-c", script, *args]


@pytest.mark.parametrize("code", [0, 1, 2, 3, 4, 5, 6, 7])
def test_p5_8_exit_codes_follow_the_retry_rule(code, caplog):
    from airflow.sdk.exceptions import AirflowException, AirflowFailException

    task = SimpleNamespace()
    with patch.object(polars_task, "job_command", _fake_job(code)), caplog.at_level("INFO"):
        if code == 0:
            polars_task.run_polars_job(["--run-id=r"], task=task)
        else:
            with pytest.raises(AirflowException) as raised:
                polars_task.run_polars_job(["--run-id=r"], task=task)
            deterministic = isinstance(raised.value, AirflowFailException)
            assert deterministic == (code in exit_codes.NO_RETRY)
            assert f"exit code {code}" in str(raised.value)
    assert task.polars_exit_code == code
    assert "line 2" in caplog.text  # la sortie du job va dans le log de la tâche


def test_p5_8_real_job_without_run_id_is_not_retried():
    """Le vrai job, lancé par la tâche : `--run-id=` vide sort en 2, avant toute base."""
    from airflow.sdk.exceptions import AirflowFailException

    with pytest.raises(AirflowFailException, match="exit code 2"):
        polars_task.run_polars_job(["--run-id="])


def test_p5_8_job_gets_the_parse_environment(tmp_path, monkeypatch):
    out = tmp_path / "env.txt"
    monkeypatch.setenv("PIPELINE_CONFIG_ROOT", str(tmp_path))
    script = (
        "import os, sys; open(sys.argv[1], 'w').write("
        "os.environ['PIPELINE_CONFIG_ROOT'] + '|' + os.environ['PYTHONPATH'])"
    )
    command = lambda args: [sys.executable, "-c", script, str(out)]  # noqa: E731
    with patch.object(polars_task, "job_command", command):
        polars_task.run_polars_job([])
    config_root, pythonpath = out.read_text().split("|")
    assert config_root == str(tmp_path)
    assert pythonpath == str(polars_task.job_env_vars()["PYTHONPATH"])


def test_p5_8_interrupted_task_stops_the_job(tmp_path):
    """Une interruption (timeout, arrêt manuel) pendant la lecture de la sortie arrête le job."""
    pid_file = tmp_path / "pid"
    script = (
        "import os, sys, time; open(sys.argv[1], 'w').write(str(os.getpid())); "
        "print('started', flush=True); time.sleep(60)"
    )
    command = lambda args: [sys.executable, "-c", script, str(pid_file)]  # noqa: E731

    with (
        patch.object(polars_task, "job_command", command),
        patch.object(polars_task.log, "info", side_effect=[None, KeyboardInterrupt]),
        pytest.raises(KeyboardInterrupt),
    ):
        polars_task.run_polars_job([])
    import os

    pid = int(pid_file.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_p5_8_exit_codes_have_one_definition():
    from pipeline.jobs import ingest
    from utils.failure_alert import JOB_EXIT_CODES
    from utils.read_files_operator import NO_RETRY_EXIT_CODES

    assert set(NO_RETRY_EXIT_CODES) == set(exit_codes.NO_RETRY) == {2, 3, 5, 6}
    assert JOB_EXIT_CODES is exit_codes.MEANINGS
    for name in ("EXIT_FILES_FAILED", "EXIT_PARENT_GONE", "EXIT_QUALITY_FAILED"):
        assert getattr(ingest, name) == getattr(exit_codes, name)


def test_p5_8_failure_line_carries_the_polars_exit_code():
    from utils.failure_alert import failure_record

    task = SimpleNamespace(task_type="_PythonDecoratedOperator", polars_exit_code=6)
    record = failure_record({"task": task}, scope="task")
    assert record["polars_exit_code"] == 6
    assert record["polars_exit_code_meaning"].startswith("blocking quality checks")
    assert "spark_exit_code" not in record


def test_p5_8_lineage_names_like_the_spark_listener(tmp_path, monkeypatch):
    from airflow.sdk.lineage import HookLineageCollector

    from tests.unit.dags.test_p2_5_file_list_lineage import _Hook, _ol

    collector = HookLineageCollector()
    monkeypatch.setattr("airflow.sdk.lineage.get_hook_lineage_collector", lambda: collector)
    monkeypatch.setattr(
        "airflow.providers.postgres.hooks.postgres.PostgresHook", lambda **kwargs: _Hook()
    )
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    source = tmp_path / "data" / "pol_1.csv"
    polars_task.report_polars_lineage([str(source)], ["raw/pol", "staging/pol"])

    lake = str(tmp_path / "lake")
    assert _ol(collector.collected_assets.outputs) == [
        ("file", f"{lake}/raw/pol"),
        ("file", f"{lake}/staging/pol"),
    ]
    assert ("file", str(source.resolve())) in _ol(collector.collected_assets.inputs)
    assert ("postgres://db.example:5434", "files.public.file_list") in _ol(
        collector.collected_assets.inputs
    )


def test_p5_8_lineage_never_fails(monkeypatch):
    monkeypatch.setattr(
        "airflow.sdk.lineage.get_hook_lineage_collector", lambda: (_ for _ in ()).throw(OSError)
    )
    polars_task.report_polars_lineage(["x"], ["raw/x"])  # ne lève pas
