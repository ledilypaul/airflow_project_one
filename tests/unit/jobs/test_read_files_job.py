"""
Unit tests for pipeline/jobs/read_files_job.py.
All external dependencies (DB, Spark) are mocked.
"""

import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.jobs.read_files_job import main

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
READ_FILES_JOB = PROJECT_ROOT / "pipeline" / "jobs" / "read_files_job.py"

RUN_ID = "run_123"

# list_file_from_db now returns dicts (RowMapping) — use plain dicts in tests
SAMPLE_FILES = [
    {"id": 1, "file_name": "orders.csv", "file_path": "/data/orders.csv", "status": "pending"},
    {"id": 2, "file_name": "imdb.parquet", "file_path": "/data/imdb.parquet", "status": "pending"},
]


def _mock_reader(side_effects=None):
    reader = MagicMock()
    if side_effects:
        reader.read_file.side_effect = side_effects
    else:
        reader.read_file.return_value = MagicMock()
    return reader


# ---------------------------------------------------------------------------
# Empty file list
# ---------------------------------------------------------------------------


def test_p1_4_main_no_rows_for_run_id_exits_3(caplog):
    """P1-4 / C-3 (was `test_main_no_files_exits_0`): the invariant changed.

    Since P1-6 an empty listing skips the insertion task, and since P1-4 the
    insertion task skips too when no file could be recorded: a `file_list`
    asset event, hence a read_file_dag run, therefore always implies at least
    one row for its source run id. Zero rows now signals an anomaly (typo in a
    manual `run_id`, purged data...) and must not end as a green no-op: exit 3,
    distinct from 0=success, 1=file failure(s), 2=missing run_id."""
    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=[]),
        patch("pipeline.jobs.read_files_job.get_spark_session") as mock_spark,
        caplog.at_level("ERROR", logger="pipeline.jobs.read_files_job"),
    ):
        with pytest.raises(SystemExit) as exc:
            main(run_id=RUN_ID)

        assert exc.value.code == 3
        mock_spark.assert_not_called()
    assert f"no file_list rows for run_id={RUN_ID!r}" in caplog.text


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_main_processes_all_files():
    mock_spark = MagicMock()
    mock_reader = _mock_reader()

    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES),
        patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark),
        patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader),
        patch("pipeline.jobs.read_files_job.update_file_status") as mock_update,
    ):
        with pytest.raises(SystemExit) as exc:
            main(run_id=RUN_ID)

        assert exc.value.code == 0
        assert mock_reader.read_file.call_count == 2
        mock_reader.read_file.assert_any_call("/data/orders.csv")
        mock_reader.read_file.assert_any_call("/data/imdb.parquet")
        mock_spark.stop.assert_called_once()
        assert mock_update.call_count == 2


# ---------------------------------------------------------------------------
# Partial failure
# ---------------------------------------------------------------------------


def test_main_continues_after_one_file_error():
    mock_spark = MagicMock()
    mock_reader = _mock_reader(side_effects=[Exception("corrupt"), MagicMock()])

    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES),
        patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark),
        patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader),
        patch("pipeline.jobs.read_files_job.update_file_status"),
    ):
        with pytest.raises(SystemExit) as exc:
            main(run_id=RUN_ID)

        # A partial failure must NOT exit 0 (old bug): at least one file failed.
        assert exc.value.code == 1
        assert mock_reader.read_file.call_count == 2
        mock_spark.stop.assert_called_once()


# ---------------------------------------------------------------------------
# Total failure
# ---------------------------------------------------------------------------


def test_main_exits_1_when_all_files_fail():
    mock_spark = MagicMock()
    mock_reader = _mock_reader(side_effects=[Exception("err1"), Exception("err2")])

    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES),
        patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark),
        patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader),
        patch("pipeline.jobs.read_files_job.update_file_status"),
    ):
        with pytest.raises(SystemExit) as exc:
            main(run_id=RUN_ID)

        assert exc.value.code == 1
        mock_spark.stop.assert_called_once()


# ---------------------------------------------------------------------------
# spark.stop() behaviour on unexpected construction error
# ---------------------------------------------------------------------------


def test_spark_stop_not_called_if_reader_init_fails():
    """P1-3: spark.stop() now lives in a `finally` that also covers
    BaseFileReader construction, so spark.stop() MUST be called even if the
    reader fails to build (no leaked Spark session)."""
    mock_spark = MagicMock()

    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES),
        patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark),
        patch(
            "pipeline.jobs.read_files_job.BaseFileReader", side_effect=RuntimeError("init failed")
        ),
    ):
        with pytest.raises(RuntimeError):
            main(run_id=RUN_ID)

        mock_spark.stop.assert_called_once()


# ---------------------------------------------------------------------------
# P1-3 — idempotence: run_id scoping, statuses, exit code, rerun/Clear
# ---------------------------------------------------------------------------


def test_p1_3_processes_only_files_of_run_id():
    mock_spark = MagicMock()
    mock_reader = _mock_reader()

    with (
        patch(
            "pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES
        ) as mock_list,
        patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark),
        patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader),
        patch("pipeline.jobs.read_files_job.update_file_status"),
    ):
        with pytest.raises(SystemExit):
            main(run_id=RUN_ID)

        mock_list.assert_called_once_with(RUN_ID)


def test_p1_3_marks_processed_on_success():
    mock_spark = MagicMock()
    mock_reader = _mock_reader()

    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES),
        patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark),
        patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader),
        patch("pipeline.jobs.read_files_job.update_file_status") as mock_update,
    ):
        with pytest.raises(SystemExit):
            main(run_id=RUN_ID)

        mock_update.assert_any_call(1, "processed")
        mock_update.assert_any_call(2, "processed")


def test_p1_3_marks_failed_with_error_message():
    mock_spark = MagicMock()
    mock_reader = _mock_reader(side_effects=[Exception("corrupt file"), MagicMock()])

    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES),
        patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark),
        patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader),
        patch("pipeline.jobs.read_files_job.update_file_status") as mock_update,
    ):
        with pytest.raises(SystemExit):
            main(run_id=RUN_ID)

        mock_update.assert_any_call(1, "failed", error_message="corrupt file")
        mock_update.assert_any_call(2, "processed")


def test_p1_3_exit_code_nonzero_on_any_failure():
    mock_spark = MagicMock()
    mock_reader = _mock_reader(side_effects=[Exception("corrupt"), MagicMock()])

    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES),
        patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark),
        patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader),
        patch("pipeline.jobs.read_files_job.update_file_status"),
    ):
        with pytest.raises(SystemExit) as exc:
            main(run_id=RUN_ID)

        assert exc.value.code == 1


def test_p1_3_rerun_same_run_id_no_duplicates():
    """Rerunning main() twice with the same run_id/rows must only UPDATE
    (never INSERT) and must not accumulate calls across runs: 2 files ->
    2 update_file_status calls per run, every time."""
    mock_spark = MagicMock()

    for _ in range(2):
        mock_reader = _mock_reader()
        with (
            patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES),
            patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark),
            patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader),
            patch("pipeline.jobs.read_files_job.update_file_status") as mock_update,
        ):
            with pytest.raises(SystemExit):
                main(run_id=RUN_ID)

            assert mock_update.call_count == 2


def test_c2_read_files_job_never_inserts_rows():
    """C-2: the job must only ever UPDATE file_list rows (via
    update_file_status), never INSERT new ones — that's list_files_dag's job.
    `pipeline.jobs.read_files_job` doesn't import/expose `insert_into_file_list`
    at all, so patching it with `create=True` (as the old version of this test
    did) always passes regardless of what the source actually does. Assert on
    the source text instead, the way test_p0_4_packaging.py does."""
    source = READ_FILES_JOB.read_text(encoding="utf-8")
    assert "insert into" not in source.lower()


def test_p1_3_main_rejects_missing_run_id():
    """C-1: run_id=None must not reach list_file_from_db (which would
    otherwise silently query `dag_run_id = NULL` and return an empty list,
    masking a config/usage error) — it must exit 2, a code distinct from
    0=success and 1=partial failure."""
    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db") as mock_list,
        patch("pipeline.jobs.read_files_job.get_spark_session") as mock_spark,
    ):
        with pytest.raises(SystemExit) as exc:
            main(run_id=None)

        assert exc.value.code == 2
        mock_list.assert_not_called()
        mock_spark.assert_not_called()


def test_p1_3_main_rejects_literal_none_run_id():
    """C-1: run_id="None" (e.g. an unrendered `{{ params.run_id }}` Jinja
    default stringified by the shell) must be rejected the same way as a
    real None, not treated as a valid run_id."""
    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db") as mock_list,
        patch("pipeline.jobs.read_files_job.get_spark_session") as mock_spark,
    ):
        with pytest.raises(SystemExit) as exc:
            main(run_id="None")

        assert exc.value.code == 2
        mock_list.assert_not_called()
        mock_spark.assert_not_called()


def test_p1_3_clear_rerun_reprocesses_failed_and_processed():
    """list_file_from_db no longer filters on status, so rows can come back
    already marked processed/failed; main() must still process every one of
    them (not skip based on current status) and rewrite their status."""
    files_with_prior_status = [
        {
            "id": 1,
            "file_name": "orders.csv",
            "file_path": "/data/orders.csv",
            "status": "processed",
        },
        {
            "id": 2,
            "file_name": "imdb.parquet",
            "file_path": "/data/imdb.parquet",
            "status": "failed",
        },
    ]
    mock_spark = MagicMock()
    mock_reader = _mock_reader()

    with (
        patch(
            "pipeline.jobs.read_files_job.list_file_from_db", return_value=files_with_prior_status
        ),
        patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark),
        patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader),
        patch("pipeline.jobs.read_files_job.update_file_status") as mock_update,
    ):
        with pytest.raises(SystemExit):
            main(run_id=RUN_ID)

        assert mock_reader.read_file.call_count == 2
        mock_update.assert_any_call(1, "processed")
        mock_update.assert_any_call(2, "processed")


# ---------------------------------------------------------------------------
# C-4 — CLI entrypoint (argparse `--run-id`) exercised as a subprocess
# ---------------------------------------------------------------------------


def test_c4_cli_requires_run_id_argument():
    """Running the script with no --run-id must fail at argparse level
    (exit code 2, the code argparse itself uses for a usage error) before
    ever touching the DB or Spark."""
    result = subprocess.run(
        [sys.executable, str(READ_FILES_JOB)],
        cwd="/tmp",
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 2
    assert "--run-id" in result.stderr or "required" in result.stderr.lower()


def test_c4_cli_accepts_run_id_argument_past_argparse():
    """Running the script with --run-id must get past argument parsing: any
    later failure (missing DB env vars in this sandboxed test run, same as
    test_p0_4_read_files_job_imports_without_editable_install_on_syspath)
    must NOT be an argparse usage error."""
    result = subprocess.run(
        [sys.executable, str(READ_FILES_JOB), "--run-id", "foo"],
        cwd="/tmp",
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert "unrecognized arguments" not in result.stderr
    assert "the following arguments are required" not in result.stderr
    assert "usage:" not in result.stderr.lower() or result.returncode != 2


# ---------------------------------------------------------------------------
# P2-1 / C-2 — an invalid reading config fails before Spark, with exit code 5
# ---------------------------------------------------------------------------


def test_p2_1_invalid_config_fails_before_spark_and_marks_rows_failed(tmp_path, monkeypatch):
    """C-2: the registry used to be loaded by BaseFileReader, after
    get_spark_session: a ConfigError then crossed main() uncaught, exited 1
    (the code of "at least one file failed"), left every row `pending` and
    had started Spark for nothing.

    It is now loaded before Spark: on ConfigError every row of the run is
    marked `failed` with the error, Spark never starts, and the job exits
    with its own code, 5 (a deterministic config error, not a file error)."""
    from pipeline.jobs import read_files_job

    config_root = tmp_path / "config"
    (config_root / "defaults").mkdir(parents=True)
    (config_root / "defaults" / "broken.yaml").write_text(
        "source:\n"
        "  name: broken\n"
        "  format: csv\n"
        "  patterns: ['*.csv']\n"
        "reader:\n"
        "  options:\n"
        "    infer_schema: true\n"
    )
    monkeypatch.setattr(read_files_job, "CONFIG_ROOT", config_root)

    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES),
        patch("pipeline.jobs.read_files_job.get_spark_session") as mock_spark,
        patch("pipeline.jobs.read_files_job.BaseFileReader") as mock_reader,
        patch("pipeline.jobs.read_files_job.update_file_status") as mock_update,
    ):
        with pytest.raises(SystemExit) as exc:
            main(run_id=RUN_ID)

    assert exc.value.code == read_files_job.EXIT_INVALID_CONFIG == 5
    mock_spark.assert_not_called()
    mock_reader.assert_not_called()

    assert [c.args for c in mock_update.call_args_list] == [(1, "failed"), (2, "failed")]
    for update in mock_update.call_args_list:
        message = update.kwargs["error_message"]
        assert "broken.yaml" in message
        assert "reader.options.infer_schema" in message


def test_p2_1_valid_config_registry_is_handed_to_the_reader():
    """C-2: the registry loaded before Spark is the one BaseFileReader uses
    (no second load of the YAML files)."""
    from pipeline.config import SourceRegistry

    mock_spark = MagicMock()
    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES),
        patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark),
        patch(
            "pipeline.jobs.read_files_job.BaseFileReader", return_value=_mock_reader()
        ) as mock_reader_cls,
        patch("pipeline.jobs.read_files_job.update_file_status"),
    ):
        with pytest.raises(SystemExit) as exc:
            main(run_id=RUN_ID)

    assert exc.value.code == 0
    mock_reader_cls.assert_called_once()
    assert mock_reader_cls.call_args.args == (mock_spark,)
    assert isinstance(mock_reader_cls.call_args.kwargs["registry"], SourceRegistry)
