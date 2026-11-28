"""P5-7 (nettoyage et qualité) : le moteur Polars donne les mêmes lignes
nettoyées et les mêmes résultats de contrôle que Spark, messages compris."""

import datetime
import math

import polars as pl
import pytest

from pipeline.config import SourceConfig
from pipeline.engine.polars.clean import clean_for_staging
from pipeline.engine.polars.quality import polars_regex_problems, run_quality_checks
from pipeline.engine.spark.engine import SparkEngine
from pipeline.quality.checks import run_quality_checks as spark_checks

NAN, INF = float("nan"), float("inf")
D = datetime.date
SCHEMA = {
    "id": ("int", pl.Int32),
    "amount": ("double", pl.Float64),
    "label": ("string", pl.String),
    "flag": ("boolean", pl.Boolean),
    "day": ("date", pl.Date),
    "at": ("string", pl.String),  # converti en timestamp dans chaque moteur
    "code": ("string", pl.String),
}
ROWS = [
    (1, 1.5, "a", True, D(2026, 11, 1), "2026-11-01 10:00:00", "AB-1"),
    (2, NAN, "b", False, None, "2026-11-01 10:00:00.120", "ab-2"),
    (2, -0.0, "B", None, D(2026, 11, 2), None, None),
    (None, INF, None, True, D(2026, 11, 1), "2026-11-02 00:00:00", "XY-12"),
    (3, None, "c", False, D(2026, 11, 3), "2026-11-01 10:00:00", "AB-1"),
    (3, 0.0, "c", False, D(2026, 11, 3), "2026-11-01 10:00:00", "AB-1"),
    (5, -3.0, "", True, D(2026, 11, 5), "2026-11-05 23:59:59.000001", "AB-77"),
]
CHECKS = [
    {"type": "not_null", "column": "id"},
    {"type": "not_null", "column": "amount"},
    {"type": "not_null", "column": "Label"},
    {"type": "unique", "columns": ["id"]},
    {"type": "unique", "columns": ["id", "label"]},
    {"type": "unique", "columns": ["amount"]},
    {"type": "unique", "columns": ["nope"]},
    {"type": "accepted_values", "column": "flag", "values": ["true"]},
    {"type": "accepted_values", "column": "day", "values": ["2026-11-01"]},
    {"type": "accepted_values", "column": "at", "values": ["2026-11-01 10:00:00.12"]},
    {"type": "accepted_values", "column": "id", "values": ["1", "2"], "severity": "warn"},
    {"type": "range", "column": "amount", "min": 0, "name": "amount_min"},
    {"type": "range", "column": "amount", "max": 10, "name": "amount_max"},
    {"type": "range", "column": "id", "min": 1, "max": 2},
    {"type": "range", "column": "label", "min": 0},
    {"type": "regex", "column": "code", "pattern": r"^[A-Z]{2}-\d+$"},
    {"type": "regex", "column": "at", "pattern": r"^2026-11-0[12] \d\d:00:00$", "name": "at_text"},
    {"type": "regex", "column": "amount", "pattern": "^(Infinity|NaN|-0\\.0)$", "name": "amt"},
    {"type": "regex", "column": "flag", "pattern": "^true$", "severity": "warn"},
    {"type": "row_count", "min": 10, "name": "rc_min"},
    {"type": "row_count", "max": 100, "name": "rc_max"},
]


def _checks():
    config = SourceConfig.model_validate(
        {
            "source": {"name": "t", "format": "csv", "patterns": ["*"], "engine": "polars"},
            "storage": {
                "format": "parquet",
                "mode": "overwrite",
                "raw_path": "raw/",
                "staging_path": "staging/",
            },
            "quality": {"checks": CHECKS},
        }
    )
    return config.quality.checks


def _spark_frame(spark, rows):
    from pyspark.sql import functions as F

    schema = ", ".join(f"{name} {spark_type}" for name, (spark_type, _) in SCHEMA.items())
    return spark.createDataFrame(rows, schema).withColumn("at", F.col("at").cast("timestamp"))


def _polars_frame(rows):
    frame = pl.DataFrame(rows, schema={n: t for n, (_, t) in SCHEMA.items()}, orient="row")
    return frame.with_columns(pl.col("at").str.to_datetime(time_unit="us")).lazy()


def _summary(results):
    return [(r.name, r.status, r.failed_rows, r.checked_rows, r.message) for r in results]


def test_p5_7_quality_results_match_spark(spark_utc):
    checks = _checks()
    expected = _summary(spark_checks(_spark_frame(spark_utc, ROWS), checks))
    got = _summary(run_quality_checks(_polars_frame(ROWS), checks))
    regex_error = "check error: invalid"
    diffs = [
        (e, g)
        for e, g in zip(expected, got, strict=True)
        if e != g and not (e[4] or "").startswith(regex_error)
    ]
    assert not diffs, diffs
    statuses = {status for _, status, *_ in got}
    assert statuses == {"pass", "fail", "warn", "error"}  # chaque statut est couvert


def test_p5_7_invalid_regex_is_an_error_status_not_a_crash():
    config_checks = [c for c in _checks() if c.type == "regex"][:1]
    bad = config_checks[0].model_copy(update={"pattern": "a{"})
    (result,) = run_quality_checks(_polars_frame(ROWS), [bad])
    assert result.status == "error"
    assert result.message.startswith("check error: invalid regex (str.contains): ")


def test_p5_7_polars_regex_problems_name_the_source(tmp_path):
    config = SourceConfig.model_validate(
        {
            "source": {"name": "p", "format": "csv", "patterns": ["*"], "engine": "polars"},
            "storage": {
                "format": "parquet",
                "mode": "overwrite",
                "raw_path": "raw/",
                "staging_path": "staging/",
            },
            "quality": {"checks": [{"type": "regex", "column": "c", "pattern": "a{"}]},
        }
    )
    (problem,) = polars_regex_problems([("p.yaml", config)])
    assert "p.yaml" in problem and "'p'" in problem and "'a{'" in problem
    spark_config = config.model_copy(
        update={"source": config.source.model_copy(update={"engine": "spark"})}
    )
    assert polars_regex_problems([("s.yaml", spark_config)]) == []


def _norm(row):
    return tuple("nan" if isinstance(v, float) and math.isnan(v) else v for v in row)


def test_p5_7_clean_for_staging_matches_spark(spark_utc):
    rows = [
        (1, NAN, "a"),
        (2, NAN, "a"),
        (None, NAN, None),
        (None, None, None),
        (3, 0.0, "b"),
        (3, -0.0, "b"),
        (4, NAN, "c"),
        (4, NAN, "c"),
        (5, INF, ""),
    ]
    spark_rows = (
        SparkEngine(spark_utc, None)
        .clean_for_staging(spark_utc.createDataFrame(rows, "i int, d double, s string"))
        .collect()
    )
    polars_rows = (
        clean_for_staging(
            pl.DataFrame(
                rows, schema={"i": pl.Int32, "d": pl.Float64, "s": pl.String}, orient="row"
            ).lazy()
        )
        .collect()
        .rows()
    )
    expected = sorted((_norm(tuple(r)) for r in spark_rows), key=str)
    assert sorted((_norm(r) for r in polars_rows), key=str) == expected
    assert len(expected) == 5  # 2 lignes vides, 2 doublons (NaN = NaN, -0.0 = 0.0)


@pytest.mark.parametrize("rows", [[], [(None, None)]])
def test_p5_7_clean_keeps_the_schema(rows):
    frame = pl.DataFrame(rows, schema={"a": pl.Int32, "b": pl.Float64}, orient="row").lazy()
    cleaned = clean_for_staging(frame).collect()
    assert cleaned.height == 0 and cleaned.schema == frame.collect_schema()
