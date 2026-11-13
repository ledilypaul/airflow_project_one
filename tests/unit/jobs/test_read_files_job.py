"""
Unit tests for pipeline/jobs/read_files_job.py.
All external dependencies (DB, Spark) are mocked.
"""

import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

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


@pytest.fixture(autouse=True)
def lake(monkeypatch, tmp_path):
    """P2-2: main() now writes every file read to the lake. These tests mock
    the reader (and Spark): replace the write with a recorder, and point
    LAKE_ROOT at a temporary directory in case anything slips through.
    The real writes are covered by test_p2_2_lake_zones.py and
    tests/unit/storage/test_p2_2_parquet_lake_writer.py."""
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    write = MagicMock(return_value={"raw": 2})
    monkeypatch.setattr("pipeline.jobs.read_files_job.write_to_lake", write)
    return write


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
        config = mock_reader.get_config.return_value
        mock_reader.read_file.assert_any_call("/data/orders.csv", config=config)
        mock_reader.read_file.assert_any_call("/data/imdb.parquet", config=config)
        mock_spark.stop.assert_called_once()
        # P2-2 / C-5: `pending` (reset before the write), then `processed`.
        assert mock_update.call_count == 4


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
    4 update_file_status calls per run (P2-2 / C-5: `pending` before the
    write, then `processed`), every time."""
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

            assert mock_update.call_count == 4


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
        "storage:\n"
        "  format: parquet\n"
        "  mode: overwrite\n"
        "  raw_path: raw/\n"
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


# ---------------------------------------------------------------------------
# P2-2 — the job writes what it reads (raw, then staging), then the status
# ---------------------------------------------------------------------------


def _run_main(files, reader, update):
    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=files),
        patch("pipeline.jobs.read_files_job.get_spark_session", return_value=MagicMock()),
        patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=reader),
        patch("pipeline.jobs.read_files_job.update_file_status", update),
    ):
        with pytest.raises(SystemExit) as exc:
            main(run_id=RUN_ID)
    return exc.value.code


def test_p2_2_writes_each_file_read_with_its_config_run_id_and_file_name(lake):
    reader = _mock_reader()
    code = _run_main(SAMPLE_FILES, reader, MagicMock())

    assert code == 0
    assert lake.call_count == 2
    first = lake.call_args_list[0]
    writer, data, config = first.args
    assert writer.location("raw/x").endswith("/lake/raw/x")
    assert data is reader.read_file.return_value
    assert config is reader.get_config.return_value
    reader.get_config.assert_any_call("/data/orders.csv")
    assert first.kwargs == {"run_id": RUN_ID, "source_file": "orders.csv", "file_id": 1}
    assert lake.call_args_list[1].kwargs["source_file"] == "imdb.parquet"
    assert lake.call_args_list[1].kwargs["file_id"] == 2


def test_p2_2_status_reset_before_write(lake):
    """C-5: a row already `processed` by a previous attempt is reset to
    `pending` (error cleared) right before its write: if the driver dies
    while the partition is being replaced (deleted, then renamed), the row
    no longer claims a partition that may be gone. `processed` comes only
    after every write of the file."""
    events = []
    lake.side_effect = lambda *a, **k: events.append(("write", k["source_file"])) or {"raw": 1}
    update = MagicMock(
        side_effect=lambda file_id, status, **kw: events.append((status, file_id, kw))
    )
    files = [{**f, "status": "processed"} for f in SAMPLE_FILES]

    _run_main(files, _mock_reader(), update)

    assert events == [
        ("pending", 1, {"error_message": None}),
        ("write", "orders.csv"),
        ("processed", 1, {}),
        ("pending", 2, {"error_message": None}),
        ("write", "imdb.parquet"),
        ("processed", 2, {}),
    ]


def test_p2_2_config_resolved_once_and_reused_for_read_and_write(lake):
    """C-8: one `get_config` per file; the same config is read with and written with."""
    reader = _mock_reader()
    configs = [MagicMock(name="orders_config"), MagicMock(name="imdb_config")]
    reader.get_config.side_effect = configs

    _run_main(SAMPLE_FILES, reader, MagicMock())

    assert reader.get_config.call_args_list == [
        call("/data/orders.csv"),
        call("/data/imdb.parquet"),
    ]
    assert reader.read_file.call_args_list == [
        call("/data/orders.csv", config=configs[0]),
        call("/data/imdb.parquet", config=configs[1]),
    ]
    assert [c.args[2] for c in lake.call_args_list] == configs


def test_p2_2_write_failure_marks_failed_and_continues(lake):
    files = SAMPLE_FILES + [
        {"id": 3, "file_name": "c.csv", "file_path": "/data/c.csv", "status": "pending"}
    ]
    lake.side_effect = [{"raw": 1}, RuntimeError("staging write failed: disk full"), {"raw": 1}]
    update = MagicMock()

    code = _run_main(files, _mock_reader(), update)

    assert code == 1
    assert lake.call_count == 3
    assert update.call_args_list == [
        call(1, "pending", error_message=None),
        call(1, "processed"),
        call(2, "pending", error_message=None),
        call(2, "failed", error_message="staging write failed: disk full"),
        call(3, "pending", error_message=None),
        call(3, "processed"),
    ]


def test_p2_2_read_failure_writes_nothing_for_that_file(lake):
    reader = _mock_reader(side_effects=[Exception("corrupt"), MagicMock()])
    _run_main(SAMPLE_FILES, reader, MagicMock())
    assert [c.kwargs["source_file"] for c in lake.call_args_list] == ["imdb.parquet"]


def test_p2_2_same_file_name_twice_in_a_run_fails_both_without_writing(lake):
    """Partitions are keyed by (run id, file name): two files of one run with
    the same name (two source directories) would overwrite each other."""
    files = [
        {"id": 1, "file_name": "orders_1.csv", "file_path": "/in/a/orders_1.csv"},
        {"id": 2, "file_name": "orders_1.csv", "file_path": "/in/b/orders_1.csv"},
        {"id": 3, "file_name": "orders_2.csv", "file_path": "/in/a/orders_2.csv"},
    ]
    update = MagicMock()
    code = _run_main(files, _mock_reader(), update)

    assert code == 1
    assert [c.kwargs["source_file"] for c in lake.call_args_list] == ["orders_2.csv"]
    assert update.call_args_list[2:] == [
        call(3, "pending", error_message=None),
        call(3, "processed"),
    ]
    for failed in update.call_args_list[:2]:
        assert failed.args[1] == "failed"
        assert "/in/a/orders_1.csv" in failed.kwargs["error_message"]
        assert "/in/b/orders_1.csv" in failed.kwargs["error_message"]


def test_p2_2_relative_lake_root_fails_before_spark_with_exit_5(monkeypatch, lake):
    from pipeline.jobs import read_files_job

    monkeypatch.setenv("LAKE_ROOT", "relative/lake")
    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES),
        patch("pipeline.jobs.read_files_job.get_spark_session") as mock_spark,
        patch("pipeline.jobs.read_files_job.update_file_status") as mock_update,
    ):
        with pytest.raises(SystemExit) as exc:
            main(run_id=RUN_ID)

    assert exc.value.code == read_files_job.EXIT_INVALID_CONFIG
    mock_spark.assert_not_called()
    lake.assert_not_called()
    assert [c.args for c in mock_update.call_args_list] == [(1, "failed"), (2, "failed")]
    assert all("LAKE_ROOT" in c.kwargs["error_message"] for c in mock_update.call_args_list)


def test_p2_2_config_without_storage_fails_before_spark_with_exit_5(tmp_path, monkeypatch):
    from pipeline.jobs import read_files_job

    config_root = tmp_path / "config"
    (config_root / "defaults").mkdir(parents=True)
    (config_root / "defaults" / "csv.yaml").write_text(
        "source:\n  name: default_csv\n  format: csv\n  patterns: ['*.csv']\n"
    )
    monkeypatch.setattr(read_files_job, "CONFIG_ROOT", config_root)

    with (
        patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES),
        patch("pipeline.jobs.read_files_job.get_spark_session") as mock_spark,
        patch("pipeline.jobs.read_files_job.update_file_status") as mock_update,
    ):
        with pytest.raises(SystemExit) as exc:
            main(run_id=RUN_ID)

    assert exc.value.code == read_files_job.EXIT_INVALID_CONFIG
    mock_spark.assert_not_called()
    assert "storage" in mock_update.call_args_list[0].kwargs["error_message"]


def test_p2_2_job_never_shows_dataframes():
    """Skill §3: no `.show()` in production code (unbounded output)."""
    assert ".show(" not in READ_FILES_JOB.read_text(encoding="utf-8")
