"""
P2-3: the quality check engine (`pipeline/quality/checks.py`), pure Spark.

- Per-row checks (`not_null`, `accepted_values`, `range`, `regex`) and the
  total count of `row_count` share ONE aggregation (conditional counts).
- `unique` needs a `groupBy`: it counts the rows that share their key with
  another row (not the duplicated keys).
- Nulls are not counted by `accepted_values`, `range`, `regex` (that is
  `not_null`'s job), nor by `unique` (a row with a null key is ignored).
- A check that cannot run (missing column, non numeric `range`, regex Java
  refuses, Spark error, any unexpected exception) gets the status `error` on
  its own, without crashing the engine nor the other checks. It blocks
  staging only when its severity is `error` (C-2).
- `range` counts NaN as out of bounds on float/double columns, whatever the
  bound (C-4); `not_null` does not see NaN.

Real Spark (session-wide `spark` fixture), tiny in-memory DataFrames.
"""

import json
from pathlib import Path

import pytest

from pipeline.config import load_source_config
from pipeline.config.quality import QualitySection, RangeCheck
from pipeline.quality import checks as engine
from pipeline.quality.checks import (
    ERROR,
    FAIL,
    PASS,
    WARN,
    QualityChecksFailed,
    blocking_failures,
    failure_summary,
    results_dataframe,
    run_quality_checks,
)

SCHEMA = "id int, status string, amount double, code string"
ROWS = [
    (1, "new", 10.0, "AB-1"),
    (2, "done", 0.0, "CD-2"),
    (3, None, None, None),
    (4, "new", 100.0, "EF-3"),
]


def _checks(*checks):
    return QualitySection.model_validate({"checks": list(checks)}).checks


@pytest.fixture
def df(spark):
    return spark.createDataFrame(ROWS, SCHEMA)


def _run(data, *checks):
    return run_quality_checks(data, _checks(*checks))


def _only(results):
    (result,) = results
    return result


# ---------------------------------------------------------------------------
# One passing and one failing case per type, with the right row count
# ---------------------------------------------------------------------------


def test_p2_3_not_null(spark, df):
    passing = _only(_run(df, {"type": "not_null", "column": "id"}))
    assert (passing.status, passing.failed_rows, passing.checked_rows) == (PASS, 0, 4)
    assert passing.message is None

    failing = _only(_run(df, {"type": "not_null", "column": "status"}))
    assert (failing.status, failing.failed_rows) == (FAIL, 1)
    assert failing.message == "1 row"


def test_p2_3_unique_counts_duplicated_rows_not_keys(spark):
    data = spark.createDataFrame(
        [(1,), (1,), (1,), (2,), (3,), (3,), (4,)], "id int"
    )  # keys 1 and 3 are duplicated: 3 + 2 = 5 rows
    result = _only(_run(data, {"type": "unique", "columns": ["id"]}))
    assert (result.status, result.failed_rows, result.checked_rows) == (FAIL, 5, 7)
    assert result.message == "5 rows"

    passing = _only(_run(data.dropDuplicates(), {"type": "unique", "columns": ["id"]}))
    assert (passing.status, passing.failed_rows) == (PASS, 0)


def test_p2_3_unique_checks_the_combination_of_columns(spark):
    data = spark.createDataFrame([(1, "a"), (1, "b"), (2, "a"), (1, "a")], "id int, k string")
    assert _only(_run(data, {"type": "unique", "columns": ["id", "k"]})).failed_rows == 2
    assert _only(_run(data, {"type": "unique", "columns": ["id"]})).failed_rows == 3


def test_p2_3_unique_ignores_rows_with_a_null_key(spark):
    data = spark.createDataFrame(
        [(None, "a"), (None, "a"), (1, None), (1, None)], "id int, k string"
    )
    assert _only(_run(data, {"type": "unique", "columns": ["id"]})).failed_rows == 2
    assert _only(_run(data, {"type": "unique", "columns": ["k"]})).failed_rows == 2
    assert _only(_run(data, {"type": "unique", "columns": ["id", "k"]})).failed_rows == 0


def test_p2_3_accepted_values(spark, df):
    passing = _only(
        _run(df, {"type": "accepted_values", "column": "status", "values": ["new", "done"]})
    )
    assert (passing.status, passing.failed_rows) == (PASS, 0)  # the null is ignored

    failing = _only(_run(df, {"type": "accepted_values", "column": "status", "values": ["new"]}))
    assert (failing.status, failing.failed_rows) == (FAIL, 1)


def test_p2_3_accepted_values_compares_the_text_form_of_the_column(spark):
    """No implicit cast of the column to the values' type (ANSI mode would
    fail the whole aggregation on `'abc' = 1`): the column's text form is
    compared to the values written as text (`true`/`false` for booleans)."""
    data = spark.createDataFrame([(1, True), (2, False), (3, True)], "id int, flag boolean")
    assert (
        _only(
            _run(data, {"type": "accepted_values", "column": "id", "values": ["1", "2"]})
        ).failed_rows
        == 1
    )
    assert (
        _only(
            _run(data, {"type": "accepted_values", "column": "flag", "values": ["true"]})
        ).failed_rows
        == 1
    )


def test_p2_3_range_is_inclusive_and_ignores_nulls(spark, df):
    both = _only(_run(df, {"type": "range", "column": "amount", "min": 0, "max": 100}))
    assert (both.status, both.failed_rows) == (PASS, 0)

    assert _only(_run(df, {"type": "range", "column": "amount", "min": 0.5})).failed_rows == 1
    assert _only(_run(df, {"type": "range", "column": "amount", "max": 50})).failed_rows == 1
    assert (
        _only(_run(df, {"type": "range", "column": "amount", "min": 1, "max": 50})).failed_rows == 2
    )


def test_p2_3_range_on_a_non_numeric_column_fails_without_crashing(spark, df):
    result = _only(_run(df, {"type": "range", "column": "status", "min": 0}))
    assert result.status == ERROR
    assert result.failed_rows is None
    assert "status" in result.message
    assert "numeric" in result.message


def test_p2_3_regex_finds_a_match_and_ignores_nulls(spark, df):
    # rlike: Java `find()` semantics, anchor with ^ and $ for a full match.
    partial = _only(_run(df, {"type": "regex", "column": "code", "pattern": "[0-9]"}))
    assert (partial.status, partial.failed_rows) == (PASS, 0)

    failing = _only(_run(df, {"type": "regex", "column": "code", "pattern": "^[A-D]{2}-[0-9]$"}))
    assert (failing.status, failing.failed_rows) == (FAIL, 1)  # EF-3; the null is ignored


def test_p2_3_regex_applies_to_the_text_form_of_non_string_columns(spark, df):
    assert _only(_run(df, {"type": "regex", "column": "id", "pattern": "^[1-3]$"})).failed_rows == 1


def test_p2_3_regex_refused_by_java_fails_without_crashing(spark, df):
    """`(?P<x>...)` compiles in Python but not in Java (the engine is
    Java's `rlike`): the config load refuses it, the engine still copes."""
    results = _run(
        df,
        {"type": "regex", "column": "code", "pattern": "(?P<x>A)"},
        {"type": "not_null", "column": "id"},
    )
    regex, not_null = results
    assert regex.status == ERROR
    assert regex.failed_rows is None
    assert "Java" in regex.message
    assert not_null.status == PASS


def test_p2_3_row_count(spark, df):
    passing = _only(_run(df, {"type": "row_count", "min": 1, "max": 4}))
    assert (passing.status, passing.failed_rows, passing.checked_rows) == (PASS, None, 4)

    too_few = _only(_run(df, {"type": "row_count", "min": 5}))
    assert (too_few.status, too_few.message) == (FAIL, "4 rows, expected >= 5")

    too_many = _only(_run(df, {"type": "row_count", "max": 3}))
    assert (too_many.status, too_many.message) == (FAIL, "4 rows, expected <= 3")

    empty = _only(_run(df.limit(0), {"type": "row_count", "min": 1}))
    assert (empty.status, empty.checked_rows, empty.message) == (FAIL, 0, "0 rows, expected >= 1")


def test_p2_3_checks_on_an_empty_dataframe_pass(spark, df):
    results = _run(
        df.limit(0),
        {"type": "not_null", "column": "id"},
        {"type": "unique", "columns": ["id"]},
        {"type": "range", "column": "amount", "min": 0},
    )
    assert [(r.status, r.failed_rows, r.checked_rows) for r in results] == [(PASS, 0, 0)] * 3


# ---------------------------------------------------------------------------
# Missing columns, severities, crashes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "check",
    [
        {"type": "not_null", "column": "idd"},
        {"type": "unique", "columns": ["id", "idd"]},
        {"type": "accepted_values", "column": "idd", "values": ["1"]},
        {"type": "range", "column": "idd", "min": 0},
        {"type": "regex", "column": "idd", "pattern": "a"},
    ],
)
def test_p2_3_missing_column_fails_the_check_not_the_engine(spark, df, check):
    missing, other = _run(df, check, {"type": "not_null", "column": "id"})
    assert missing.status == ERROR
    assert missing.failed_rows is None
    assert "'idd'" in missing.message
    assert "not found" in missing.message
    assert "'id'" in missing.message  # close match suggested
    assert other.status == PASS


def test_p2_3_column_names_are_matched_exactly(spark, df):
    result = _only(_run(df, {"type": "not_null", "column": "ID"}))
    assert result.status == ERROR
    assert "'ID'" in result.message


def test_p2_3_column_names_with_dots_are_not_struct_paths(spark):
    data = spark.createDataFrame([(1,), (None,)], "`a.b` int")
    assert _only(_run(data, {"type": "not_null", "column": "a.b"})).failed_rows == 1


def test_p2_3_failing_warn_check_has_status_warn(spark, df):
    result = _only(_run(df, {"type": "not_null", "column": "status", "severity": "warn"}))
    assert (result.status, result.severity, result.failed_rows) == (WARN, "warn", 1)
    assert blocking_failures([result]) == []


def test_p2_3_crashed_warn_check_is_recorded_but_not_blocking(spark, df):
    """C-2: a `warn` check that cannot run has the status `error` (not `fail`:
    its outcome is unknown) and, being a warning, does not block staging."""
    result = _only(_run(df, {"type": "not_null", "column": "nope", "severity": "warn"}))
    assert result.status == ERROR
    assert result.severity == "warn"
    assert result.failed_rows is None
    assert not result.blocking
    assert blocking_failures([result]) == []


def test_p2_3_crashed_error_check_is_blocking(spark, df):
    """C-2: an `error` check that cannot run blocks staging."""
    result = _only(_run(df, {"type": "not_null", "column": "nope"}))
    assert (result.status, result.severity) == (ERROR, "error")
    assert result.blocking
    assert blocking_failures([result]) == [result]


def test_p2_3_unexpected_exception_while_building_a_check_fails_that_check_only(spark, df):
    """C-1: not only `_CheckError`. A bound outside int64 (refused at load
    now, built here without validation) makes `F.lit` raise a Py4JError: the
    check gets a short `error`, the others still run."""
    huge = RangeCheck.model_construct(type="range", column="amount", max=10**20)
    checks = [huge, *_checks({"type": "not_null", "column": "id"})]

    out_of_int64, not_null = run_quality_checks(df, checks)

    assert out_of_int64.status == ERROR
    assert out_of_int64.message.startswith("check error: ")
    assert "\n" not in out_of_int64.message
    assert len(out_of_int64.message) <= 600
    assert not_null.status == PASS


def test_p2_3_unexpected_exception_in_any_check_construction_is_contained(spark, df, monkeypatch):
    def _boom(data, check):
        if check.type == "regex":
            raise RuntimeError("first line\n" + "trace " * 500)
        return original(data, check)

    original = engine._failing_row_condition
    monkeypatch.setattr(engine, "_failing_row_condition", _boom)
    regex, not_null = _run(
        df,
        {"type": "regex", "column": "code", "pattern": "a"},
        {"type": "not_null", "column": "id"},
    )
    assert (regex.status, regex.message) == (ERROR, "check error: first line")
    assert not_null.status == PASS


def test_p2_3_failed_combined_aggregation_falls_back_to_one_check_at_a_time(spark, df, monkeypatch):
    """An unexpected Spark error in the shared aggregation must not fail
    every check: each is then evaluated on its own."""
    monkeypatch.setattr(engine, "_java_regex_error", lambda data, pattern: None)
    results = _run(
        df,
        {"type": "not_null", "column": "id"},
        {"type": "regex", "column": "code", "pattern": "(?P<x>A)"},  # fails at execution
        {"type": "row_count", "min": 1},
    )
    assert [r.status for r in results] == [PASS, ERROR, PASS]
    assert results[0].checked_rows == 4
    assert results[1].failed_rows is None
    assert "check error" in results[1].message


# ---------------------------------------------------------------------------
# One aggregation for every per-row check
# ---------------------------------------------------------------------------


def test_p2_3_per_row_checks_share_a_single_aggregation(spark, df, monkeypatch):
    calls = []
    original = type(df).agg

    def _spy(self, *exprs):
        calls.append(len(exprs))
        return original(self, *exprs)

    monkeypatch.setattr(type(df), "agg", _spy)
    results = _run(
        df,
        {"type": "not_null", "column": "id"},
        {"type": "not_null", "column": "status"},
        {"type": "accepted_values", "column": "status", "values": ["new", "done"]},
        {"type": "range", "column": "amount", "min": 0, "max": 50},
        {"type": "regex", "column": "code", "pattern": "^[A-Z]{2}-"},
        {"type": "row_count", "min": 1},
    )

    assert [r.failed_rows for r in results] == [0, 1, 0, 1, 0, None]
    # One aggregation: the total count + one conditional count per row check.
    assert calls == [6]


def test_p2_3_each_unique_check_adds_one_grouped_aggregation(spark, df, monkeypatch):
    calls = []
    original = type(df).groupBy

    def _spy(self, *cols):
        calls.append(cols)
        return original(self, *cols)

    monkeypatch.setattr(type(df), "groupBy", _spy)
    _run(
        df,
        {"type": "unique", "columns": ["id"]},
        {"type": "unique", "columns": ["status", "code"]},
        {"type": "not_null", "column": "id"},
    )
    # The shared aggregation (`agg` = `groupBy()` without columns) aside, one
    # grouped aggregation per unique check.
    assert len([c for c in calls if c]) == 2


def test_p2_3_engine_never_collects_or_shows_data():
    source = Path(engine.__file__).read_text(encoding="utf-8")
    for forbidden in (".collect(", ".toPandas(", ".show("):
        assert forbidden not in source


# ---------------------------------------------------------------------------
# Results: summary and DataFrame
# ---------------------------------------------------------------------------


def test_p2_3_failure_summary_lists_blocking_failures_only(spark, df):
    results = _run(
        df,
        {"type": "not_null", "column": "status"},
        {"type": "range", "column": "amount", "max": 50},
        {"type": "unique", "columns": ["id"]},
        {"type": "not_null", "column": "code", "severity": "warn"},
        {"type": "row_count", "min": 10, "name": "big_enough"},
    )
    assert [r.status for r in results] == [FAIL, FAIL, PASS, WARN, FAIL]
    assert failure_summary(results) == (
        "quality checks failed: not_null(status): 1 row; range(amount): 1 row; "
        "big_enough: 4 rows, expected >= 10"
    )
    error = QualityChecksFailed(results)
    assert str(error) == failure_summary(results)
    assert error.results == results


def test_p2_3_results_dataframe_has_one_row_per_check(spark, df):
    checks = _checks(
        {"type": "unique", "columns": ["id", "code"]},
        {"type": "regex", "column": "code", "pattern": "^Z", "severity": "warn"},
        {"type": "row_count", "min": 1},
    )
    results = run_quality_checks(df, checks)
    out = results_dataframe(spark, results, source_name="orders")

    assert out.columns == [
        "source_name",
        "check_name",
        "check_type",
        "check_columns",
        "severity",
        "status",
        "failed_rows",
        "checked_rows",
        "message",
        "check_definition",
    ]
    rows = [r.asDict() for r in out.orderBy("check_name").toLocalIterator()]
    assert [(r["check_name"], r["check_columns"], r["status"], r["failed_rows"]) for r in rows] == [
        ("regex(code)", ["code"], WARN, 3),
        ("row_count", [], PASS, None),
        ("unique(id,code)", ["id", "code"], PASS, 0),
    ]
    assert {r["source_name"] for r in rows} == {"orders"}
    assert {r["checked_rows"] for r in rows} == {4}
    assert json.loads(rows[0]["check_definition"]) == {
        "type": "regex",
        "column": "code",
        "pattern": "^Z",
        "severity": "warn",
    }


def test_p2_3_results_dataframe_of_a_crashed_check_keeps_nulls(spark, df):
    results = _run(df, {"type": "not_null", "column": "nope"})
    (row,) = results_dataframe(spark, results, source_name="orders").toLocalIterator()
    assert (row.status, row.failed_rows, row.checked_rows) == (ERROR, None, 4)
    assert "nope" in row.message


def test_p2_3_failure_summary_includes_blocking_errors_only(spark, df):
    results = _run(
        df,
        {"type": "not_null", "column": "nope"},
        {"type": "not_null", "column": "gone", "severity": "warn"},
        {"type": "not_null", "column": "status"},
    )
    assert [(r.status, r.blocking) for r in results] == [
        (ERROR, True),
        (ERROR, False),
        (FAIL, True),
    ]
    summary = failure_summary(results)
    assert summary.startswith("quality checks failed: not_null(nope): check error: column 'nope'")
    assert summary.endswith("; not_null(status): 1 row")
    assert "gone" not in summary


# ---------------------------------------------------------------------------
# C-4: NaN
# ---------------------------------------------------------------------------

NAN_ROWS = [(1.0,), (float("nan"),), (None,), (5.0,)]


@pytest.mark.parametrize("column_type", ["double", "float"])
@pytest.mark.parametrize(
    "bounds", [{"min": 0}, {"max": 10}, {"min": 0, "max": 10}], ids=["min", "max", "both"]
)
def test_p2_3_range_counts_nan_as_out_of_bounds_whatever_the_bound(spark, column_type, bounds):
    """Spark orders NaN above every number: with `min` alone it used to pass,
    with `max` it failed. Now it always fails; the null is still ignored."""
    data = spark.createDataFrame(NAN_ROWS, f"x {column_type}")
    result = _only(_run(data, {"type": "range", "column": "x", **bounds}))
    assert (result.status, result.failed_rows, result.checked_rows) == (FAIL, 1, 4)


def test_p2_3_range_on_an_integer_column_is_unchanged(spark):
    data = spark.createDataFrame([(1,), (None,), (5,)], "x int")
    assert _only(_run(data, {"type": "range", "column": "x", "min": 0})).failed_rows == 0


def test_p2_3_not_null_does_not_count_nan(spark):
    data = spark.createDataFrame(NAN_ROWS, "x double")
    assert _only(_run(data, {"type": "not_null", "column": "x"})).failed_rows == 1


# ---------------------------------------------------------------------------
# C-3: quoted YAML values, end to end
# ---------------------------------------------------------------------------


def test_p2_3_quoted_yaml_values_match_the_text_in_real_spark(spark, tmp_path):
    path = tmp_path / "sources" / "countries.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(
        "source: {name: countries, format: csv, patterns: ['c_*.csv']}\n"
        "storage: {format: parquet, mode: overwrite, raw_path: raw/, staging_path: staging/}\n"
        "quality:\n  checks:\n"
        "    - {type: accepted_values, column: country, values: ['FR', 'NO', 'yes', '007']}\n"
    )
    checks = load_source_config(path).quality.checks
    data = spark.createDataFrame(
        [("FR",), ("NO",), ("yes",), ("007",), ("7",), ("false",), (None,)], "country string"
    )
    result = _only(run_quality_checks(data, checks))
    assert (result.status, result.failed_rows) == (FAIL, 2)  # "7" and "false"
