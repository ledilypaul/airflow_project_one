"""P5-7, test doré : le job Spark et le job Polars, sur les mêmes fichiers et le
même YAML (options traduites), écrivent le même lake : mêmes partitions, mêmes
types physiques, mêmes lignes (hors `lake_ingested_at`), mêmes résultats
qualité, mêmes statuts `file_list` et même code de sortie."""

import math
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest
import yaml

from pipeline.config.reader_options import SPARK_TO_POLARS_OPTIONS
from pipeline.engine.spark.engine import SparkEngine
from pipeline.jobs import ingest, polars_job, read_files_job

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RUN_ID = "manual__2026-11-28T10:00:00+00:00:é#1"
STRICT = {
    "source": {"name": "strict", "format": "csv", "patterns": ["strict_*.csv"]},
    "reader": {
        "options": {"header": True, "enforceSchema": False},
        "columns_type": {
            "id": "long",
            "amount": "double",
            "ratio": "float",
            "ok": "boolean",
            "day": "date",
            "at": "timestamp",
            "label": "string",
        },
    },
    "storage": {
        "format": "parquet",
        "mode": "overwrite",
        "raw_path": "raw/",
        "staging_path": "staging/",
    },
    "quality": {
        "checks": [
            {"type": "not_null", "column": "id"},
            {"type": "range", "column": "amount", "min": 0},
            {"type": "unique", "columns": ["id"], "severity": "warn"},
            {
                "type": "accepted_values",
                "column": "label",
                "values": ["a", "b"],
                "severity": "warn",
            },
            {"type": "regex", "column": "at", "pattern": "^2026-11", "severity": "warn"},
            {"type": "row_count", "min": 1},
        ]
    },
}
HEADER = "id,amount,ratio,ok,day,at,label\n"
FILES = {
    "strict_ok.csv": HEADER
    + "1,1.5,0.1,true,2026-11-01,2026-11-01 10:00:00,a\n"
    + "2, 2.5 ,NaN,FALSE,2026-11,2026-11-01T10:00:00+02:00,\n"
    + "2, 2.5 ,NaN,FALSE,2026-11,2026-11-01T10:00:00+02:00,\n"
    + '3,Infinity,1e3,yes,2026-02-30,2026-11-01 10:00:00.1234567,"b"\n'
    + ",,,,,,\n"
    + "4,-0.0,,,2026-1-5 10:00,2026-11-02,c\n",
    "strict_bad.csv": HEADER + "1,-1,0,true,2026-11-01,2026-11-01,a\n,2,0,true,,,b\n",
    "strict_shape.csv": "id,amount\n1,2\n",
}


def _polars_config(raw: dict) -> dict:
    mapping = SPARK_TO_POLARS_OPTIONS["csv"]
    options = {
        mapping[k]: v
        for k, v in raw["reader"]["options"].items()
        if k in mapping and k != "inferSchema"
    }
    return {
        **raw,
        "source": {**raw["source"], "engine": "polars"},
        "reader": {**raw["reader"], "options": options},
    }


@pytest.fixture(scope="module")
def setup(tmp_path_factory):
    base = tmp_path_factory.mktemp("golden")
    configs = [
        yaml.safe_load(p.read_text())
        for p in sorted((PROJECT_ROOT / "examples/config/sources").glob("*.yaml"))
    ] + [STRICT]
    for engine in ("spark", "polars"):
        sources = base / engine / "config" / "sources"
        sources.mkdir(parents=True)
        for config in configs:
            data = config if engine == "spark" else _polars_config(config)
            (sources / f"{config['source']['name']}.yaml").write_text(
                yaml.safe_dump(data, sort_keys=False)
            )
    data = base / "data"
    data.mkdir()
    for sample in (PROJECT_ROOT / "examples/data").glob("*.csv"):
        shutil.copy(sample, data)
    for name, text in FILES.items():
        (data / name).write_text(text)
    rows = [{"id": i, "file_path": str(p)} for i, p in enumerate(sorted(data.iterdir()), 10)]
    return base, rows


def _run(job, monkeypatch, base, engine, rows):
    monkeypatch.setenv("LAKE_ROOT", str(base / engine / "lake"))
    monkeypatch.setattr(job, "CONFIG_ROOT", base / engine / "config")
    update = MagicMock()
    with (
        patch.object(ingest, "list_file_from_db", return_value=rows),
        patch.object(ingest, "update_file_status", update),
        pytest.raises(SystemExit) as exc,
    ):
        job.main(run_id=RUN_ID)
    return exc.value.code, [(c.args, c.kwargs.get("error_message")) for c in update.call_args_list]


@pytest.fixture(scope="module")
def outcomes(setup, spark_utc):
    base, rows = setup
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(read_files_job, "start_parent_watchdog", lambda: None)
        monkeypatch.setattr(read_files_job, "get_spark_session", lambda name: spark_utc)
        monkeypatch.setattr(SparkEngine, "close", lambda self: None)  # session partagée
        spark = _run(read_files_job, monkeypatch, base, "spark", rows)
        polars = _run(polars_job, monkeypatch, base, "polars", rows)
    return base, spark, polars


def _final(calls):
    """Statut final et message par ligne de `file_list`. Polars lit l'en-tête
    tout de suite : un fichier mal formé est `failed` sans passer par
    `pending`, alors que Spark (lecture paresseuse) y passe d'abord."""
    return {args[0]: (args[1], message) for args, message in calls}


def test_p5_7_same_exit_code_and_statuses(outcomes):
    _, (spark_code, spark_calls), (polars_code, polars_calls) = outcomes
    assert spark_code == polars_code == 1  # strict_shape.csv : en-tête faux
    spark, polars = _final(spark_calls), _final(polars_calls)
    assert {i: s for i, (s, _) in spark.items()} == {i: s for i, (s, _) in polars.items()}
    assert sorted(s for s, _ in polars.values()) == [
        "failed",
        "failed",
        "processed",
        "processed",
        "processed",
    ]
    for file_id, (status, message) in polars.items():
        if status == "failed" and "strict_shape" not in message:
            assert message == spark[file_id][1]  # contrôles bloquants : même résumé


def _partitions(lake: Path) -> dict[str, list[Path]]:
    found: dict[str, list[Path]] = {}
    for file in lake.rglob("*.parquet"):
        found.setdefault(str(file.parent.relative_to(lake)), []).append(file)
    return found


def _rows(files: list[Path]):
    rows = []
    for file in files:
        for row in pq.read_table(file).to_pylist():
            row.pop("lake_ingested_at")
            rows.append(
                tuple("nan" if isinstance(v, float) and math.isnan(v) else v for v in row.values())
            )
    return sorted(rows, key=repr)


def _physical(file: Path) -> str:
    return str(pq.ParquetFile(file).schema).replace("spark_schema", "schema").split("\n", 1)[1]


def test_p5_7_same_lake(outcomes):
    base = outcomes[0]
    spark, polars = _partitions(base / "spark" / "lake"), _partitions(base / "polars" / "lake")
    assert sorted(spark) == sorted(polars)
    assert {p.split("/")[0] for p in spark} == {"raw", "staging", "quality"}
    for partition, spark_files in spark.items():
        assert _rows(polars[partition]) == _rows(spark_files), partition
        assert {_physical(f) for f in polars[partition]} == {_physical(f) for f in spark_files}
    # strict_bad.csv : contrôle bloquant, raw écrit, staging non écrit.
    assert any("strict_bad" in p for p in polars if p.startswith("raw/"))
    assert not any("strict_bad" in p for p in polars if p.startswith("staging/"))
