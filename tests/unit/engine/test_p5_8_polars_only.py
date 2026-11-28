"""P5-8 : le job Polars tourne sans PySpark. Ce fichier n'importe ni PySpark ni
le moteur Spark : la CI le lance aussi dans un environnement `uv sync
--no-group spark` (job `polars-only`), où PySpark n'est pas installé."""

import json
import subprocess
import sys
from pathlib import Path

import yaml

from pipeline.config.reader_options import SPARK_TO_POLARS_OPTIONS

PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Le vrai job, dans un processus neuf : `file_list` remplacé par des fonctions.
SCRIPT = """
import json, sys
from pipeline.jobs import ingest, polars_job

config_root, rows, out = sys.argv[1], json.loads(sys.argv[2]), sys.argv[3]
statuses = []
ingest.list_file_from_db = lambda run_id, source_name=None: rows
ingest.update_file_status = lambda i, s, error_message=None: statuses.append([i, s, error_message])
ingest.start_parent_watchdog = lambda: None
polars_job.CONFIG_ROOT = __import__("pathlib").Path(config_root)
try:
    polars_job.main(run_id="manual__p5_8")
except SystemExit as exc:
    code = exc.code
loaded = sorted(m for m in ("pyspark", "airflow") if m in sys.modules)
json.dump({"code": code, "statuses": statuses, "loaded": loaded}, open(out, "w"))
"""


def _polars_root(tmp_path: Path) -> Path:
    root = tmp_path / "config"
    (root / "sources").mkdir(parents=True)
    mapping = SPARK_TO_POLARS_OPTIONS["csv"]
    for path in (PROJECT_ROOT / "examples/config/sources").glob("*.yaml"):
        config = yaml.safe_load(path.read_text())
        options = config["reader"]["options"]
        config["reader"]["options"] = {
            mapping[k]: v for k, v in options.items() if k in mapping and k != "inferSchema"
        }
        config["source"]["engine"] = "polars"
        (root / "sources" / path.name).write_text(yaml.safe_dump(config, sort_keys=False))
    return root


def test_p5_8_polars_job_runs_without_pyspark(tmp_path):
    data = sorted((PROJECT_ROOT / "examples" / "data").glob("*.csv"))
    rows = [{"id": i, "file_path": str(p)} for i, p in enumerate(data, 1)]
    out = tmp_path / "result.json"
    env = {"LAKE_ROOT": str(tmp_path / "lake"), "PATH": "/usr/bin:/bin"}
    proc = subprocess.run(
        [sys.executable, "-c", SCRIPT, str(_polars_root(tmp_path)), json.dumps(rows), str(out)],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    result = json.loads(out.read_text())

    assert result["code"] == 0, result
    assert [s for _, s, _ in result["statuses"] if s != "pending"] == ["processed"] * len(rows)
    assert result["loaded"] == []  # ni PySpark ni Airflow
    staged = list((tmp_path / "lake" / "staging").rglob("*.parquet"))
    assert {p.parents[2].name for p in staged} == {"movies", "orders"}


if __name__ == "__main__":
    # Sans pytest : la CI le lance dans l'environnement du cœur seul (job `polars-only`).
    import tempfile

    with tempfile.TemporaryDirectory() as directory:
        test_p5_8_polars_job_runs_without_pyspark(Path(directory))
    print("polars job OK without pyspark")
