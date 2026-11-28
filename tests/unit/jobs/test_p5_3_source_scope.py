"""P5-3 — the job reads the `file_list` rows of (run id, source name): the run ids of two
source DAGs can be equal."""

from unittest.mock import MagicMock, patch

import pytest

from pipeline.jobs.read_files_job import main, parse_args


def test_p5_3_source_name_is_an_optional_argument():
    assert parse_args(["--run-id=r1"]).source_name is None
    parsed = parse_args(["--run-id=r1", "--source-name=orders"])
    assert (parsed.run_id, parsed.source_name) == ("r1", "orders")


@pytest.mark.parametrize("source_name", ["orders", None])
def test_p5_3_main_scopes_the_rows_to_the_source(source_name):
    with (
        patch("pipeline.jobs.ingest.list_file_from_db", return_value=[]) as mock_list,
        pytest.raises(SystemExit) as exit_info,
    ):
        main(run_id="r1", source_name=source_name)

    assert exit_info.value.code == 3  # no row for (run id, source)
    mock_list.assert_called_once_with("r1", source_name=source_name)


def test_p5_3_the_no_row_message_names_the_source(caplog):
    with (
        patch("pipeline.jobs.ingest.list_file_from_db", return_value=[]),
        pytest.raises(SystemExit),
    ):
        main(run_id="r1", source_name="orders")
    assert "'orders'" in caplog.text


def test_p5_3_the_job_is_started_with_the_source_name():
    """`python read_files_job.py --run-id=x --source-name=y` reaches main()."""
    import pipeline.jobs.read_files_job as job

    main_mock = MagicMock()
    with (
        patch.object(job, "main", main_mock),
        patch("sys.argv", ["job", "--run-id=x", "--source-name=y"]),
    ):
        args = job.parse_args()
        job.main(run_id=args.run_id, source_name=args.source_name)
    main_mock.assert_called_once_with(run_id="x", source_name="y")
