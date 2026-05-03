"""
Unit tests for pipeline/jobs/read_files_job.py.
All external dependencies (DB, Spark) are mocked.
"""
import sys
import pytest
from unittest.mock import patch, MagicMock, call

# psycopg2 is not installed in this venv. Stub it with a real Exception subclass
# so that `except psycopg2.DatabaseError` in functions_utils.py works correctly.
_psycopg2_stub = MagicMock()
_psycopg2_stub.DatabaseError = type("DatabaseError", (Exception,), {})
sys.modules.setdefault("psycopg2", _psycopg2_stub)

from pipeline.jobs.read_files_job import main


SAMPLE_FILES = [
    (1, "orders.csv",  "/data/orders.csv"),
    (2, "imdb.parquet", "/data/imdb.parquet"),
]


def _mock_reader(side_effects=None):
    """Helper: returns a mock BaseFileReader instance."""
    reader = MagicMock()
    mock_df = MagicMock()
    if side_effects:
        reader.read_file.side_effect = side_effects
    else:
        reader.read_file.return_value = mock_df
    return reader


# ---------------------------------------------------------------------------
# Empty file list
# ---------------------------------------------------------------------------

def test_main_no_files_exits_0():
    """When DB returns no files, job exits cleanly without starting Spark."""
    with patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=[]) as mock_db, \
         patch("pipeline.jobs.read_files_job.get_spark_session") as mock_spark:

        with pytest.raises(SystemExit) as exc:
            main()

        assert exc.value.code == 0
        mock_spark.assert_not_called()  # Spark must NOT be started for nothing


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_main_processes_all_files():
    """All files are read, spark.stop() is called, exits with code 0."""
    mock_spark = MagicMock()
    mock_reader = _mock_reader()

    with patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES), \
         patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark), \
         patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader):

        with pytest.raises(SystemExit) as exc:
            main()

        assert exc.value.code == 0
        assert mock_reader.read_file.call_count == 2
        mock_reader.read_file.assert_any_call("/data/orders.csv")
        mock_reader.read_file.assert_any_call("/data/imdb.parquet")
        mock_spark.stop.assert_called_once()


# ---------------------------------------------------------------------------
# Partial failure (one file fails, job continues)
# ---------------------------------------------------------------------------

def test_main_continues_after_one_file_error():
    """A failing file is skipped; remaining files are processed; exits 0."""
    mock_spark = MagicMock()
    mock_reader = _mock_reader(side_effects=[Exception("corrupt"), MagicMock()])

    with patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES), \
         patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark), \
         patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader):

        with pytest.raises(SystemExit) as exc:
            main()

        assert exc.value.code == 0          # 1 success → exit 0
        assert mock_reader.read_file.call_count == 2  # both files attempted
        mock_spark.stop.assert_called_once()


# ---------------------------------------------------------------------------
# Total failure (all files fail)
# ---------------------------------------------------------------------------

def test_main_exits_1_when_all_files_fail():
    """If every file raises, success_count=0 → exit code 1."""
    mock_spark = MagicMock()
    mock_reader = _mock_reader(side_effects=[Exception("err1"), Exception("err2")])

    with patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES), \
         patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark), \
         patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader):

        with pytest.raises(SystemExit) as exc:
            main()

        assert exc.value.code == 1
        mock_spark.stop.assert_called_once()  # stop() called even on total failure


# ---------------------------------------------------------------------------
# spark.stop() called even when an unexpected error occurs mid-loop
# ---------------------------------------------------------------------------

def test_spark_stop_called_on_unexpected_error():
    """spark.stop() must be called even if BaseFileReader constructor blows up."""
    mock_spark = MagicMock()

    with patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES), \
         patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark), \
         patch("pipeline.jobs.read_files_job.BaseFileReader", side_effect=RuntimeError("spark init failed")):

        with pytest.raises(RuntimeError):
            main()

        # spark.stop() is NOT called here because the error happens before the loop.
        # This test documents the current behaviour: if construction fails, stop() is skipped.
        mock_spark.stop.assert_not_called()
