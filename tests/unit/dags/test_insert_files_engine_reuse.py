"""
P1-2 — regression: the insertion task of an ingest DAG must build exactly one
PostgresHook/engine per task run and pass it to every insert_into_file_list()
call, instead of rebuilding one per file (M3).

P5-3: the task is the `insert_files` of `ingest__<source>`; every row carries the
run id and the source name (two source DAGs can have the same run id).
"""

from unittest.mock import MagicMock, patch

from utils.read_files_operator import build_application_args


def _make_files(tmp_path, count=3):
    paths = []
    for i in range(count):
        p = tmp_path / f"file_{i}.csv"
        p.write_text("a,b\n1,2\n")
        paths.append(str(p))
    return paths


def _insert_files(source="orders"):
    from dags.source_pipelines import build_dags

    return build_dags()[f"ingest__{source}"].get_task("insert_files").python_callable


def test_p1_2_insert_files_builds_hook_once_for_multiple_files(tmp_path):
    file_list = _make_files(tmp_path, count=3)
    mock_engine = MagicMock()
    mock_hook_cls = MagicMock()
    mock_hook_cls.return_value.get_sqlalchemy_engine.return_value = mock_engine
    mock_context = {"dag_run": MagicMock(run_id="run_123")}

    with (
        patch("airflow.providers.postgres.hooks.postgres.PostgresHook", mock_hook_cls),
        patch("airflow.sdk.get_current_context", return_value=mock_context),
        patch("utils.functions_utils.insert_into_file_list") as mock_insert,
    ):
        job_args = _insert_files()(file_list)

    # The task returns the arguments of the Spark job: one application per run.
    assert job_args == build_application_args("run_123", "orders")
    mock_hook_cls.assert_called_once_with(postgres_conn_id="file_list_db")
    mock_hook_cls.return_value.get_sqlalchemy_engine.assert_called_once()
    assert mock_insert.call_count == 3
    for call in mock_insert.call_args_list:
        assert call.kwargs["engine"] is mock_engine


def test_p1_2_every_row_carries_the_real_run_id_and_the_source(tmp_path):
    """C-1 (audit code-auditor): a row without the run id shares `dag_run_id=""` with
    every other, and with the unique index + ON CONFLICT DO NOTHING a file already
    seen by an earlier run was never inserted again. P5-3: the source name is part of
    the key too."""
    file_list = _make_files(tmp_path, count=3)
    real_run_id = "scheduled__2026-11-11T00:00:00+00:00"
    mock_context = {"dag_run": MagicMock(run_id=real_run_id)}

    with (
        patch("airflow.providers.postgres.hooks.postgres.PostgresHook", MagicMock()),
        patch("airflow.sdk.get_current_context", return_value=mock_context),
        patch("utils.functions_utils.insert_into_file_list") as mock_insert,
    ):
        _insert_files("imdb_movies")(file_list)

    assert mock_insert.call_count == 3
    for call in mock_insert.call_args_list:
        assert call.kwargs["dag_run_id"] == real_run_id
        assert call.kwargs["source_name"] == "imdb_movies"
