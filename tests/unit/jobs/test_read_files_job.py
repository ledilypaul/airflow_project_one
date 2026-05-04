"""
Unit tests for pipeline/jobs/read_files_job.py.
All external dependencies (DB, Spark) are mocked.
"""
import pytest
from unittest.mock import patch, MagicMock

from pipeline.jobs.read_files_job import main


# list_file_from_db now returns dicts (RowMapping) — use plain dicts in tests
SAMPLE_FILES = [
    {"id": 1, "file_name": "orders.csv",   "file_path": "/data/orders.csv"},
    {"id": 2, "file_name": "imdb.parquet", "file_path": "/data/imdb.parquet"},
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

def test_main_no_files_exits_0():
    with patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=[]), \
         patch("pipeline.jobs.read_files_job.get_spark_session") as mock_spark:

        with pytest.raises(SystemExit) as exc:
            main()

        assert exc.value.code == 0
        mock_spark.assert_not_called()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_main_processes_all_files():
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
# Partial failure
# ---------------------------------------------------------------------------

def test_main_continues_after_one_file_error():
    mock_spark = MagicMock()
    mock_reader = _mock_reader(side_effects=[Exception("corrupt"), MagicMock()])

    with patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES), \
         patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark), \
         patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader):

        with pytest.raises(SystemExit) as exc:
            main()

        assert exc.value.code == 0
        assert mock_reader.read_file.call_count == 2
        mock_spark.stop.assert_called_once()


# ---------------------------------------------------------------------------
# Total failure
# ---------------------------------------------------------------------------

def test_main_exits_1_when_all_files_fail():
    mock_spark = MagicMock()
    mock_reader = _mock_reader(side_effects=[Exception("err1"), Exception("err2")])

    with patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES), \
         patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark), \
         patch("pipeline.jobs.read_files_job.BaseFileReader", return_value=mock_reader):

        with pytest.raises(SystemExit) as exc:
            main()

        assert exc.value.code == 1
        mock_spark.stop.assert_called_once()


# ---------------------------------------------------------------------------
# spark.stop() behaviour on unexpected construction error
# ---------------------------------------------------------------------------

def test_spark_stop_not_called_if_reader_init_fails():
    mock_spark = MagicMock()

    with patch("pipeline.jobs.read_files_job.list_file_from_db", return_value=SAMPLE_FILES), \
         patch("pipeline.jobs.read_files_job.get_spark_session", return_value=mock_spark), \
         patch("pipeline.jobs.read_files_job.BaseFileReader", side_effect=RuntimeError("init failed")):

        with pytest.raises(RuntimeError):
            main()

        mock_spark.stop.assert_not_called()
