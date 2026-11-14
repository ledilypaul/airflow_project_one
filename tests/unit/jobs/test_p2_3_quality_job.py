"""
P2-3: quality checks in the Spark job (`write_to_lake`, then `main()`).

For a source that declares `quality`:
- raw is always written (trace of the file received);
- the checks run on the DataFrame cleaned for staging, before the technical
  columns are added;
- their results are recorded on every run, pass or fail, in
  `<LAKE_ROOT>/quality/<table>/`, partitioned by (`lake_run_id`,
  `lake_source_file`) and overwritten: a re-run replaces them;
- at least one blocking check (an `error` check with the status `fail`, or
  `error` when it could not run): staging is NOT written and the staging
  partition a previous attempt may have left for this (run, file) is
  removed; the file is `failed` with a readable summary, the other files go
  on, exit code 1;
- only `warn` checks failing or unable to run: staging written, file
  `processed`, nothing in `error_message`;
- the engine itself raising (C-1): fail closed, the previous staging
  partition is removed, the file is `failed` with a short message;
- the quality partition of the (run, file) is removed first, whenever the
  source has a staging zone (C-6): no orphan or stale results;
- every regex of the registry is compiled by Java right after Spark starts
  (C-8): one refused regex fails the whole run with exit code 5.
A source without `quality` behaves as in P2-2 (no quality table written).

Real Spark (session-wide `spark` fixture), `tmp_path` as `LAKE_ROOT`; only
the file_list database is mocked.
"""

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
import yaml

from pipeline.config import SourceConfig
from pipeline.jobs import read_files_job
from pipeline.jobs.read_files_job import write_to_lake
from pipeline.quality.checks import ERROR, FAIL, PASS, WARN, QualityChecksFailed
from storage.parquet_lake_writer import ParquetLakeWriter, resolve_lake_root

RUN_ID = "manual__2026-11-13T14:21:36.519211+00:00"
FILE_ID = 7
SOURCE_FILE = "orders_1.csv"
SCHEMA = "order_id int, item string, amount double"
GOOD = [(1, "a", 1.5), (2, "b", 2.0), (3, "c", 3.0)]
BAD = [(1, "a", 1.5), (None, "b", 2.0), (None, "c", -3.0), (4, "d", 4.0)]

STORAGE = {
    "table_name": "orders",
    "format": "parquet",
    "mode": "overwrite",
    "raw_path": "raw/",
    "staging_path": "staging/",
}
CHECKS = [
    {"type": "not_null", "column": "order_id"},
    {"type": "range", "column": "amount", "min": 0},
    {"type": "unique", "columns": ["order_id"]},
    {"type": "row_count", "min": 10, "severity": "warn"},
]


def _config_dict(checks=CHECKS, storage=STORAGE) -> dict:
    config = {
        "source": {"name": "orders", "format": "csv", "patterns": ["orders_*.csv"]},
        "reader": {"options": {"header": True, "inferSchema": True}},
        "storage": dict(storage),
    }
    if checks is not None:
        config["quality"] = {"checks": checks}
    return config


def _config(checks=CHECKS) -> SourceConfig:
    return SourceConfig.model_validate(_config_dict(checks))


@pytest.fixture
def writer(tmp_path):
    return ParquetLakeWriter(resolve_lake_root({"LAKE_ROOT": str(tmp_path)}))


def _write(writer, data, config, run_id=RUN_ID, source_file=SOURCE_FILE):
    return write_to_lake(
        writer, data, config, run_id=run_id, source_file=source_file, file_id=FILE_ID
    )


def _partitions(root: Path, target: str) -> list[str]:
    table = root / target
    return sorted(str(p.relative_to(table)) for p in table.glob("lake_run_id=*/lake_source_file=*"))


def _quality_rows(spark, root: Path, table="orders"):
    df = spark.read.parquet((root / "quality" / table).as_uri())
    return sorted(
        (r.lake_run_id, r.lake_source_file, r.check_name, r.status, r.failed_rows)
        for r in df.toLocalIterator()
    )


# ---------------------------------------------------------------------------
# write_to_lake
# ---------------------------------------------------------------------------


def test_p2_3_passing_checks_write_raw_results_and_staging(spark, writer, tmp_path):
    written = _write(writer, spark.createDataFrame(GOOD, SCHEMA), _config())

    assert written == {"raw": 3, "quality": 4, "staging": 3}
    assert _quality_rows(spark, tmp_path) == [
        (RUN_ID, SOURCE_FILE, "not_null(order_id)", PASS, 0),
        (RUN_ID, SOURCE_FILE, "range(amount)", PASS, 0),
        (RUN_ID, SOURCE_FILE, "row_count", WARN, None),
        (RUN_ID, SOURCE_FILE, "unique(order_id)", PASS, 0),
    ]
    results = spark.read.parquet((tmp_path / "quality" / "orders").as_uri())
    assert {r.lake_file_id for r in results.toLocalIterator()} == {FILE_ID}


def test_p2_3_blocking_failure_keeps_raw_skips_staging_and_removes_previous_staging(
    spark, writer, tmp_path
):
    config = _config()
    _write(writer, spark.createDataFrame(GOOD, SCHEMA), config)
    assert _partitions(tmp_path, "staging/orders")  # a previous, valid attempt

    with pytest.raises(QualityChecksFailed) as exc_info:
        _write(writer, spark.createDataFrame(BAD, SCHEMA), config)

    assert str(exc_info.value) == (
        "quality checks failed: not_null(order_id): 2 rows; range(amount): 1 row"
    )
    # raw: the file as received, rewritten.
    assert spark.read.parquet((tmp_path / "raw" / "orders").as_uri()).count() == 4
    # staging: the partition of the previous attempt is gone.
    assert _partitions(tmp_path, "staging/orders") == []
    # results: recorded for the failed attempt too, replacing the previous ones.
    assert _quality_rows(spark, tmp_path) == [
        (RUN_ID, SOURCE_FILE, "not_null(order_id)", FAIL, 2),
        (RUN_ID, SOURCE_FILE, "range(amount)", FAIL, 1),
        (RUN_ID, SOURCE_FILE, "row_count", WARN, None),
        (RUN_ID, SOURCE_FILE, "unique(order_id)", PASS, 0),
    ]


def test_p2_3_blocking_failure_leaves_the_other_staging_partitions_alone(spark, writer, tmp_path):
    config = _config()
    _write(writer, spark.createDataFrame(GOOD, SCHEMA), config, source_file="orders_2.csv")
    _write(writer, spark.createDataFrame(GOOD, SCHEMA), config, run_id="other_run")

    with pytest.raises(QualityChecksFailed):
        _write(writer, spark.createDataFrame(BAD, SCHEMA), config)

    assert _partitions(tmp_path, "staging/orders") == [
        f"lake_run_id={RUN_ID.replace(':', '%3A')}/lake_source_file=orders_2.csv",
        "lake_run_id=other_run/lake_source_file=orders_1.csv",
    ]


def test_p2_3_warn_only_failure_writes_staging(spark, writer, tmp_path):
    checks = [
        {"type": "not_null", "column": "order_id", "severity": "warn"},
        {"type": "range", "column": "amount", "min": 0, "severity": "warn"},
    ]
    written = _write(writer, spark.createDataFrame(BAD, SCHEMA), _config(checks))

    assert written == {"raw": 4, "quality": 2, "staging": 4}
    assert [s for *_, s, _ in _quality_rows(spark, tmp_path)] == [WARN, WARN]


def test_p2_3_checks_run_on_the_data_cleaned_for_staging(spark, writer, tmp_path):
    """Exact duplicates and fully empty rows are removed before the checks:
    they never reach staging, they must not fail it."""
    rows = [(1, "a", 1.0), (1, "a", 1.0), (None, None, None), (2, "b", 2.0)]
    checks = [{"type": "unique", "columns": ["order_id"]}, {"type": "not_null", "column": "item"}]

    written = _write(writer, spark.createDataFrame(rows, SCHEMA), _config(checks))

    assert written == {"raw": 4, "quality": 2, "staging": 2}
    assert [s for *_, s, _ in _quality_rows(spark, tmp_path)] == [PASS, PASS]


def test_p2_3_rerun_replaces_the_results_without_duplicates(spark, writer, tmp_path):
    config = _config()
    _write(writer, spark.createDataFrame(GOOD, SCHEMA), config)
    _write(writer, spark.createDataFrame(GOOD, SCHEMA), config, source_file="orders_2.csv")
    with pytest.raises(QualityChecksFailed):
        _write(writer, spark.createDataFrame(BAD, SCHEMA), config)
    _write(writer, spark.createDataFrame(GOOD, SCHEMA), config)

    rows = _quality_rows(spark, tmp_path)
    assert len(rows) == 8  # 4 checks x 2 files, no duplicate
    assert {(file, status) for _, file, name, status, _ in rows if name != "row_count"} == {
        (SOURCE_FILE, PASS),
        ("orders_2.csv", PASS),
    }
    assert len(_partitions(tmp_path, "staging/orders")) == 2


def test_p2_3_crashed_check_blocks_staging_with_a_clear_message(spark, writer, tmp_path):
    checks = [{"type": "not_null", "column": "order_idd"}]
    with pytest.raises(QualityChecksFailed) as exc_info:
        _write(writer, spark.createDataFrame(GOOD, SCHEMA), _config(checks))

    message = str(exc_info.value)
    assert message.startswith("quality checks failed: not_null(order_idd): ")
    assert "'order_idd' not found" in message
    assert not (tmp_path / "staging").exists()
    assert (tmp_path / "raw" / "orders").is_dir()


def test_p2_3_source_without_quality_keeps_the_p2_2_behaviour(spark, writer, tmp_path):
    written = _write(writer, spark.createDataFrame(BAD, SCHEMA), _config(checks=None))
    assert written == {"raw": 4, "staging": 4}
    assert not (tmp_path / "quality").exists()


def test_p2_3_dataframes_are_unpersisted_after_a_quality_failure(spark, writer):
    """The read and the cleaned DataFrames are both persisted (several
    actions each); both are released, failure or not."""
    spark.catalog.clearCache()
    data = spark.createDataFrame(BAD, SCHEMA)
    with pytest.raises(QualityChecksFailed):
        _write(writer, data, _config())
    assert not data.is_cached
    assert spark._jsparkSession.sharedState().cacheManager().isEmpty()


def test_p2_3_staging_is_deleted_before_results_are_recorded(spark, tmp_path):
    """The invariant that matters most comes first: if recording the results
    fails, the stale staging partition is already gone."""
    events = []
    writer = MagicMock()
    writer.write.side_effect = lambda data, target, **kw: events.append(("write", target)) or 1
    writer.delete_partition.side_effect = lambda spark, target, **kw: (
        events.append(("delete", target)) or True
    )

    with pytest.raises(QualityChecksFailed):
        _write(writer, spark.createDataFrame(BAD, SCHEMA), _config())

    assert events == [
        ("delete", "quality/orders"),  # C-6: no stale results, whatever happens next
        ("write", "raw/orders"),
        ("delete", "staging/orders"),
        ("write", "quality/orders"),
    ]
    assert writer.delete_partition.call_args.kwargs == {
        "run_id": RUN_ID,
        "source_file": SOURCE_FILE,
    }


# ---------------------------------------------------------------------------
# main(): statuses and exit code
# ---------------------------------------------------------------------------


def _config_root(tmp_path: Path, checks) -> Path:
    root = tmp_path / "config"
    path = root / "sources" / "orders.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(_config_dict(checks), sort_keys=False))
    return root


def _run_main(spark, tmp_path, monkeypatch, files: dict[str, str], checks=CHECKS, stop=None):
    inbox = tmp_path / "in"
    inbox.mkdir(exist_ok=True)
    rows = []
    for file_id, (name, content) in enumerate(files.items(), start=1):
        (inbox / name).write_text(content, encoding="utf-8")
        rows.append({"id": file_id, "file_name": name, "file_path": str(inbox / name)})
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.setattr(read_files_job, "CONFIG_ROOT", _config_root(tmp_path, checks))
    # The session is shared by the whole test run: main() must not stop it.
    monkeypatch.setattr(spark, "stop", stop or (lambda: None))
    update = MagicMock()
    with (
        patch.object(read_files_job, "get_spark_session", return_value=spark),
        patch.object(read_files_job, "list_file_from_db", return_value=rows),
        patch.object(read_files_job, "update_file_status", update),
    ):
        with pytest.raises(SystemExit) as exc:
            read_files_job.main(run_id=RUN_ID)
    return exc.value.code, update


GOOD_CSV = "order_id,item,amount\n1,a,1.5\n2,b,2.0\n"
BAD_CSV = "order_id,item,amount\n1,a,1.5\n,b,2.0\n3,c,-1.0\n"


def test_p2_3_main_marks_a_blocking_failure_failed_and_goes_on(spark, tmp_path, monkeypatch):
    code, update = _run_main(
        spark, tmp_path, monkeypatch, {"orders_bad.csv": BAD_CSV, "orders_good.csv": GOOD_CSV}
    )

    assert code == 1
    assert update.call_args_list == [
        call(1, "pending", error_message=None),
        call(
            1,
            "failed",
            error_message=(
                "quality checks failed: not_null(order_id): 1 row; range(amount): 1 row"
            ),
        ),
        call(2, "pending", error_message=None),
        call(2, "processed"),
    ]
    lake = tmp_path / "lake"
    assert [p.split("/")[-1] for p in _partitions(lake, "raw/orders")] == [
        "lake_source_file=orders_bad.csv",
        "lake_source_file=orders_good.csv",
    ]
    assert [p.split("/")[-1] for p in _partitions(lake, "staging/orders")] == [
        "lake_source_file=orders_good.csv"
    ]
    assert {(f, n, s) for _, f, n, s, _ in _quality_rows(spark, lake) if n == "range(amount)"} == {
        ("orders_bad.csv", "range(amount)", FAIL),
        ("orders_good.csv", "range(amount)", PASS),
    }


def test_p2_3_main_warn_only_failure_is_processed_without_error_message(
    spark, tmp_path, monkeypatch, caplog
):
    checks = [{"type": "row_count", "min": 10, "severity": "warn"}]
    with caplog.at_level("WARNING"):
        code, update = _run_main(
            spark, tmp_path, monkeypatch, {"orders_good.csv": GOOD_CSV}, checks=checks
        )

    assert code == 0
    assert update.call_args_list == [
        call(1, "pending", error_message=None),
        call(1, "processed"),
    ]
    assert "row_count: 2 rows, expected >= 10" in caplog.text
    assert _partitions(tmp_path / "lake", "staging/orders")


# ---------------------------------------------------------------------------
# C-1: the engine fails closed
# ---------------------------------------------------------------------------


def test_p2_3_engine_crash_removes_the_previous_staging_and_fails_the_file_shortly(
    spark, tmp_path, monkeypatch
):
    code, _ = _run_main(spark, tmp_path, monkeypatch, {"orders_good.csv": GOOD_CSV})
    lake = tmp_path / "lake"
    assert code == 0
    assert _partitions(lake, "staging/orders")  # a previous, successful attempt
    assert _partitions(lake, "quality/orders")

    def _crash(data, checks):
        raise RuntimeError(
            "An error occurred while calling z:org.apache.spark.sql.functions.lit. Trace:\n"
            + "\tat java.base/java.lang.NumberFormatException.forInputString\n" * 20
        )

    monkeypatch.setattr(read_files_job, "run_quality_checks", _crash)
    code, update = _run_main(spark, tmp_path, monkeypatch, {"orders_good.csv": GOOD_CSV})

    assert code == 1
    assert _partitions(lake, "staging/orders") == []
    assert _partitions(lake, "quality/orders") == []  # no stale `pass` left either
    assert _partitions(lake, "raw/orders")
    (_, status), kwargs = update.call_args_list[-1]
    assert status == "failed"
    assert kwargs["error_message"] == (
        "quality checks could not run: An error occurred while calling "
        "z:org.apache.spark.sql.functions.lit. Trace:"
    )


# ---------------------------------------------------------------------------
# C-2: a `warn` check that cannot run does not block staging
# ---------------------------------------------------------------------------


def _quality_statuses(spark, root):
    df = spark.read.parquet((root / "quality" / "orders").as_uri())
    return sorted((r.check_name, r.severity, r.status) for r in df.toLocalIterator())


def test_p2_3_warn_check_that_cannot_run_is_logged_as_error_and_staging_written(
    spark, writer, tmp_path, caplog
):
    checks = [
        {"type": "not_null", "column": "order_idd", "severity": "warn"},
        {"type": "not_null", "column": "order_id"},
    ]
    with caplog.at_level("WARNING"):
        written = _write(writer, spark.createDataFrame(GOOD, SCHEMA), _config(checks))

    assert written == {"raw": 3, "quality": 2, "staging": 3}
    assert _quality_statuses(spark, tmp_path) == [
        ("not_null(order_id)", "error", PASS),
        ("not_null(order_idd)", "warn", ERROR),
    ]
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1
    assert "not_null(order_idd)" in errors[0].getMessage()
    assert "'order_idd' not found" in errors[0].getMessage()


def test_p2_3_error_check_that_cannot_run_blocks_staging(spark, writer, tmp_path):
    checks = [{"type": "not_null", "column": "order_idd"}]
    with pytest.raises(QualityChecksFailed) as exc_info:
        _write(writer, spark.createDataFrame(GOOD, SCHEMA), _config(checks))

    assert str(exc_info.value).startswith("quality checks failed: not_null(order_idd): check error")
    assert _quality_statuses(spark, tmp_path) == [("not_null(order_idd)", "error", ERROR)]
    assert not (tmp_path / "staging").exists()


# ---------------------------------------------------------------------------
# C-6: no orphan or stale results
# ---------------------------------------------------------------------------


def test_p2_3_source_that_lost_its_quality_section_keeps_no_old_results(spark, writer, tmp_path):
    _write(writer, spark.createDataFrame(GOOD, SCHEMA), _config())
    _write(writer, spark.createDataFrame(GOOD, SCHEMA), _config(), source_file="orders_2.csv")
    assert len(_partitions(tmp_path, "quality/orders")) == 2

    written = _write(writer, spark.createDataFrame(GOOD, SCHEMA), _config(checks=None))

    assert written == {"raw": 3, "staging": 3}
    # Only the other file's results remain.
    assert [p.split("/")[-1] for p in _partitions(tmp_path, "quality/orders")] == [
        "lake_source_file=orders_2.csv"
    ]


class _FailingWriter(ParquetLakeWriter):
    def __init__(self, lake_root, failing_target):
        super().__init__(lake_root)
        self.failing_target = failing_target

    def write(self, data, target, **kwargs):
        if target == self.failing_target:
            raise RuntimeError("disk full\n" + "\tat org.apache.hadoop...\n" * 30)
        return super().write(data, target, **kwargs)


def test_p2_3_results_write_failure_keeps_the_quality_summary(spark, tmp_path):
    writer = _FailingWriter(resolve_lake_root({"LAKE_ROOT": str(tmp_path)}), "quality/orders")
    with pytest.raises(QualityChecksFailed) as exc_info:
        _write(writer, spark.createDataFrame(BAD, SCHEMA), _config())

    assert str(exc_info.value) == (
        "quality checks failed: not_null(order_id): 2 rows; range(amount): 1 row "
        "(results not recorded: disk full)"
    )
    assert not (tmp_path / "staging").exists()


def test_p2_3_staging_write_failure_after_passing_checks_leaves_no_results(spark, tmp_path):
    good = ParquetLakeWriter(resolve_lake_root({"LAKE_ROOT": str(tmp_path)}))
    _write(good, spark.createDataFrame(GOOD, SCHEMA), _config())
    writer = _FailingWriter(good.lake_root, "staging/orders")

    with pytest.raises(RuntimeError, match="disk full"):
        _write(writer, spark.createDataFrame(GOOD, SCHEMA), _config())

    assert _partitions(tmp_path, "quality/orders") == []  # no `pass` without data


# ---------------------------------------------------------------------------
# C-10: the read DataFrame is released as soon as the checks materialized staging
# ---------------------------------------------------------------------------


class _SpyWriter(ParquetLakeWriter):
    def __init__(self, lake_root, data):
        super().__init__(lake_root)
        self.data = data
        self.seen = {}

    def write(self, frame, target, **kwargs):
        self.seen[target] = (self.data.is_cached, frame.is_cached)
        return super().write(frame, target, **kwargs)


def test_p2_3_read_dataframe_is_released_before_the_staging_write(spark, tmp_path):
    spark.catalog.clearCache()
    data = spark.createDataFrame(GOOD, SCHEMA)
    writer = _SpyWriter(resolve_lake_root({"LAKE_ROOT": str(tmp_path)}), data)

    _write(writer, data, _config())

    assert writer.seen["raw/orders"] == (True, True)
    # (read DataFrame cached?, written DataFrame = staged cached?)
    assert writer.seen["staging/orders"] == (False, True)
    assert spark._jsparkSession.sharedState().cacheManager().isEmpty()


def test_p2_3_staged_dataframe_is_released_when_the_staging_write_fails(spark, tmp_path):
    spark.catalog.clearCache()
    writer = _FailingWriter(resolve_lake_root({"LAKE_ROOT": str(tmp_path)}), "staging/orders")
    with pytest.raises(RuntimeError):
        _write(writer, spark.createDataFrame(GOOD, SCHEMA), _config())
    assert spark._jsparkSession.sharedState().cacheManager().isEmpty()


# ---------------------------------------------------------------------------
# C-8: every regex compiled by Java before any file is processed
# ---------------------------------------------------------------------------


def test_p2_3_regex_refused_by_java_fails_the_run_with_exit_5_before_any_file(
    spark, tmp_path, monkeypatch
):
    """`a{` compiles in Python (a literal `{`) but not in Java ("Illegal
    repetition"): caught right after Spark starts, before reading anything."""
    checks = [{"type": "regex", "column": "item", "pattern": "a{", "name": "item_code"}]
    stop = MagicMock()
    reader_class = MagicMock()
    monkeypatch.setattr(read_files_job, "BaseFileReader", reader_class)

    code, update = _run_main(
        spark,
        tmp_path,
        monkeypatch,
        {"orders_1.csv": GOOD_CSV, "orders_2.csv": GOOD_CSV},
        checks=checks,
        stop=stop,
    )

    assert code == read_files_job.EXIT_INVALID_CONFIG == 5
    assert [c.args[:2] for c in update.call_args_list] == [(1, "failed"), (2, "failed")]
    message = update.call_args_list[0].kwargs["error_message"]
    assert message.startswith("invalid reading config: ")
    for expected in ("orders.yaml", "'orders'", "item_code", "'a{'", "Illegal repetition"):
        assert expected in message
    reader_class.return_value.read_file.assert_not_called()
    stop.assert_called_once()
    assert not (tmp_path / "lake").exists()
