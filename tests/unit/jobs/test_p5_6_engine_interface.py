"""P5-6: the common orchestration does not depend on an engine, and the Spark
results schema is derived from the neutral (Arrow) one."""

import subprocess
import sys
from pathlib import Path

from pipeline.engine.base import Engine
from pipeline.engine.spark.engine import SparkEngine

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_p5_6_orchestration_imports_without_an_engine():
    script = (
        "import sys; import pipeline.jobs.ingest, pipeline.engine.base; "
        "loaded = {m.split('.')[0] for m in sys.modules} & {'pyspark', 'polars', 'airflow'}; "
        "sys.exit(str(loaded) if loaded else 0)"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=PROJECT_ROOT, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_p5_6_spark_engine_implements_the_protocol():
    declared = {n for n, v in vars(Engine).items() if callable(v) and not n.startswith("_")}
    assert declared <= set(vars(SparkEngine))
    assert SparkEngine.name == "spark"


def test_p5_6_spark_results_schema_follows_the_arrow_one():
    from pipeline.quality import checks, results

    assert [(f.name, f.nullable) for f in checks.RESULTS_SCHEMA] == [
        (f.name, f.nullable) for f in results.RESULTS_SCHEMA
    ]
    assert checks.RESULTS_SCHEMA.simpleString() == (
        "struct<source_name:string,check_name:string,check_type:string,"
        "check_columns:array<string>,severity:string,status:string,failed_rows:bigint,"
        "checked_rows:bigint,message:string,check_definition:string>"
    )
    assert checks.RESULTS_SCHEMA["check_columns"].dataType.containsNull is False
