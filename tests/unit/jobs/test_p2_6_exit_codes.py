"""
P2-6: a dedicated exit code for a deterministic quality failure.

Until P2-3, a blocking quality check ended the job with exit 1, like any
failed file, so read_file_dag retried a run whose result could not change.
Now:
- 6 (`EXIT_QUALITY_FAILED`) when every failed file of the run failed only
  because of blocking checks that found failing rows (status `fail`,
  severity `error`), the results recorded and the staging partition
  removed: a retry would fail the same way, so read_file_dag does not retry;
- 1 for any other failure, alone or mixed with quality failures: a read or
  write error, the engine raising (`quality checks could not run`), a
  blocking check that could not run (status `error`), results not recorded
  or staging not removed, two files with the same name. A retry may change
  the outcome, or must at least be possible.
- 0 when every file is processed.

Real Spark (session-wide `spark` fixture), `tmp_path` as `LAKE_ROOT`; only
the file_list database is mocked, as in test_p2_3_quality_job.py.
"""

from functools import partial
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from pipeline.config.quality import NotNullCheck
from pipeline.jobs import read_files_job
from pipeline.quality.checks import ERROR, FAIL, CheckResult, QualityChecksFailed
from tests.unit.jobs.test_p2_3_quality_job import (
    BAD_CSV,
    CHECKS,
    GOOD_CSV,
    RUN_ID,
    _config_dict,
    _FailingWriter,
)

# ---------------------------------------------------------------------------
# Constants and the pure decision
# ---------------------------------------------------------------------------


def test_p2_6_exit_code_constants():
    assert read_files_job.EXIT_FILES_FAILED == 1
    assert read_files_job.EXIT_INVALID_RUN_ID == 2
    assert read_files_job.EXIT_NO_ROWS == 3
    assert read_files_job.EXIT_PARENT_GONE == 4
    assert read_files_job.EXIT_INVALID_CONFIG == 5
    assert read_files_job.EXIT_QUALITY_FAILED == 6


@pytest.mark.parametrize(
    ("failed", "quality_only", "expected"),
    [
        (0, 0, 0),
        (1, 1, 6),
        (3, 3, 6),
        (1, 0, 1),
        (2, 1, 1),
        (3, 2, 1),
    ],
)
def test_p2_6_final_exit_code(failed, quality_only, expected):
    assert (
        read_files_job.final_exit_code(failed=failed, deterministic_quality=quality_only)
        == expected
    )


@pytest.mark.parametrize(("failed", "quality_only"), [(0, 1), (1, 2), (-1, 0), (1, -1)])
def test_p2_6_final_exit_code_refuses_inconsistent_counts(failed, quality_only):
    with pytest.raises(ValueError):
        read_files_job.final_exit_code(failed=failed, deterministic_quality=quality_only)


def _result(status: str, severity: str = "error", column: str = "id") -> CheckResult:
    check = NotNullCheck(type="not_null", column=column, severity=severity)
    failed_rows = None if status == ERROR else 2
    return CheckResult(check, status, failed_rows, 10, f"{status} on {column}")


@pytest.mark.parametrize(
    ("results", "notes", "expected"),
    [
        ([_result(FAIL)], [], True),
        ([_result(FAIL), _result(FAIL, column="b")], [], True),
        # A `warn` check that could not run never blocks: still deterministic.
        ([_result(FAIL), _result(ERROR, severity="warn", column="b")], [], True),
        # A blocking check that could not run (missing column, Spark error...).
        ([_result(ERROR)], [], False),
        ([_result(FAIL), _result(ERROR, column="b")], [], False),
        # Something else went wrong: a retry may record the results or remove
        # the stale staging partition.
        ([_result(FAIL)], ["results not recorded: disk full"], False),
        ([_result(FAIL)], ["previous staging partition not removed: x"], False),
    ],
    ids=[
        "fail",
        "two_fails",
        "fail_and_warn_error",
        "error",
        "fail_and_error",
        "results_not_recorded",
        "staging_not_removed",
    ],
)
def test_p2_6_quality_failure_is_deterministic(results, notes, expected):
    assert QualityChecksFailed(results, notes=notes).deterministic is expected


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------


def _config_root(tmp_path: Path, checks) -> Path:
    root = tmp_path / "config"
    path = root / "sources" / "orders.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(_config_dict(checks), sort_keys=False))
    return root


def _run_main(spark, tmp_path, monkeypatch, files: list[tuple[str, str]], checks=CHECKS):
    """main() on `files` ((relative path, content), in file_list order);
    returns (exit code, file_list updates)."""
    rows = []
    for file_id, (relative, content) in enumerate(files, start=1):
        path = tmp_path / "in" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        rows.append({"id": file_id, "file_name": path.name, "file_path": str(path)})
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.setattr(read_files_job, "CONFIG_ROOT", _config_root(tmp_path, checks))
    monkeypatch.setattr(spark, "stop", lambda: None)
    update = MagicMock()
    with (
        patch.object(read_files_job, "get_spark_session", return_value=spark),
        patch.object(read_files_job, "list_file_from_db", return_value=rows),
        patch.object(read_files_job, "update_file_status", update),
    ):
        with pytest.raises(SystemExit) as exc:
            read_files_job.main(run_id=RUN_ID)
    return exc.value.code, update


def _final_statuses(update) -> dict[int, str]:
    statuses = {}
    for c in update.call_args_list:
        statuses[c.args[0]] = c.args[1]
    return statuses


def test_p2_6_quality_failure_with_other_files_processed_exits_6(
    spark, tmp_path, monkeypatch, caplog
):
    with caplog.at_level("INFO"):
        code, update = _run_main(
            spark,
            tmp_path,
            monkeypatch,
            [("orders_bad.csv", BAD_CSV), ("orders_good.csv", GOOD_CSV)],
        )

    assert code == read_files_job.EXIT_QUALITY_FAILED == 6
    assert _final_statuses(update) == {1: "failed", 2: "processed"}
    assert "not retried" in caplog.text


def test_p2_6_every_file_failing_its_checks_exits_6(spark, tmp_path, monkeypatch):
    code, update = _run_main(
        spark,
        tmp_path,
        monkeypatch,
        [("orders_bad_1.csv", BAD_CSV), ("orders_bad_2.csv", BAD_CSV)],
    )

    assert code == 6
    assert _final_statuses(update) == {1: "failed", 2: "failed"}


def test_p2_6_success_still_exits_0(spark, tmp_path, monkeypatch):
    code, _ = _run_main(spark, tmp_path, monkeypatch, [("orders_good.csv", GOOD_CSV)])
    assert code == 0


def test_p2_6_quality_failure_mixed_with_a_read_failure_exits_1(spark, tmp_path, monkeypatch):
    """`orders.json` matches no config (the test root has no defaults): a
    file error, not a quality one. Mixed failures keep the retry."""
    code, update = _run_main(
        spark,
        tmp_path,
        monkeypatch,
        [("orders_bad.csv", BAD_CSV), ("orders.json", "{}")],
    )

    assert code == read_files_job.EXIT_FILES_FAILED == 1
    assert _final_statuses(update) == {1: "failed", 2: "failed"}


def test_p2_6_quality_failure_mixed_with_homonyms_exits_1(spark, tmp_path, monkeypatch):
    code, update = _run_main(
        spark,
        tmp_path,
        monkeypatch,
        [
            ("orders_bad.csv", BAD_CSV),
            ("a/orders_same.csv", GOOD_CSV),
            ("b/orders_same.csv", GOOD_CSV),
        ],
    )

    assert code == 1
    assert _final_statuses(update) == {1: "failed", 2: "failed", 3: "failed"}


def test_p2_6_engine_that_cannot_run_exits_1(spark, tmp_path, monkeypatch):
    """`quality checks could not run` (the engine raised) is not
    deterministic: a transient Spark error may be gone on the retry."""

    def _crash(data, checks):
        raise RuntimeError("py4j: connection reset")

    monkeypatch.setattr(read_files_job, "run_quality_checks", _crash)
    code, update = _run_main(spark, tmp_path, monkeypatch, [("orders_good.csv", GOOD_CSV)])

    assert code == 1
    message = update.call_args_list[-1].kwargs["error_message"]
    assert message.startswith("quality checks could not run: ")


def test_p2_6_blocking_check_that_could_not_run_exits_1(spark, tmp_path, monkeypatch):
    """Status `error`, severity `error` (here a missing column): blocking,
    but the check never produced a result; kept retryable."""
    checks = [{"type": "not_null", "column": "order_idd"}]
    code, update = _run_main(
        spark, tmp_path, monkeypatch, [("orders_good.csv", GOOD_CSV)], checks=checks
    )

    assert code == 1
    assert (
        update.call_args_list[-1]
        .kwargs["error_message"]
        .startswith("quality checks failed: not_null(order_idd): check error")
    )


def test_p2_6_quality_failure_whose_results_were_not_recorded_exits_1(spark, tmp_path, monkeypatch):
    """The checks failed, but recording their results failed too: a retry
    may record them, so it must stay possible."""
    monkeypatch.setattr(
        read_files_job,
        "ParquetLakeWriter",
        partial(_FailingWriter, failing_target="quality/orders"),
    )
    code, update = _run_main(spark, tmp_path, monkeypatch, [("orders_bad.csv", BAD_CSV)])

    assert code == 1
    assert "results not recorded: disk full" in update.call_args_list[-1].kwargs["error_message"]
