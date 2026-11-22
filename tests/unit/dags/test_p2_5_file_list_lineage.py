"""
P2-5 — lineage of the `file_list` table, reported through Airflow's hook
lineage collector (the tasks reach Postgres through a SQLAlchemy engine,
which the provider cannot see by itself).

- The listing tasks: inputs = the recorded source files (`file` datasets,
  same names as the ones the Spark listener gives the files it reads),
  output = `file_list` (`postgres://host:port`, `db.public.file_list`).
- spark_read_files: input = `file_list` (the job selects its files there).
- Reporting lineage never fails a task.

Lives under tests/unit/dags: it imports Airflow, which needs the isolated
configuration of this folder's conftest.py.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


class _Hook:
    """Stand-in for PostgresHook: only what the helper reads."""

    conn_name_attr = "postgres_conn_id"

    def __init__(self, host="db.example", port=5434, schema="files", database=None):
        self.postgres_conn_id = "file_list_db"
        self.database = database
        self._conn = SimpleNamespace(host=host, port=port, schema=schema)

    def get_connection(self, conn_id):
        assert conn_id == "file_list_db"
        return self._conn


@pytest.fixture
def collector(monkeypatch):
    """A real HookLineageCollector instead of the suite's no-op one
    (OpenLineage is disabled in tests, so no reader is registered)."""
    from airflow.sdk.lineage import HookLineageCollector

    real = HookLineageCollector()
    monkeypatch.setattr("airflow.sdk.lineage.get_hook_lineage_collector", lambda: real)
    return real


def _ol(assets):
    """Collected Airflow Assets -> OpenLineage (namespace, name), with the
    providers' own converters (the ones the OpenLineage listener uses)."""
    from airflow.providers.common.io.assets.file import (
        convert_asset_to_openlineage as file_to_ol,
    )
    from airflow.providers.postgres.assets.postgres import (
        convert_asset_to_openlineage as postgres_to_ol,
    )

    out = []
    for item in assets:
        convert = postgres_to_ol if item.asset.uri.startswith("postgres://") else file_to_ol
        dataset = convert(item.asset, None)
        out.append((dataset.namespace, dataset.name))
    return sorted(out)


def test_p2_5_listing_reports_source_files_in_and_file_list_out(collector, tmp_path):
    from utils.file_list_lineage import report_file_list_lineage

    a = tmp_path / "data" / "imdb_a.csv"
    b = tmp_path / "data" / "orders 1.csv"
    report_file_list_lineage(_Hook(), role="output", source_files=[str(a), str(b)])

    lineage = collector.collected_assets
    assert _ol(lineage.outputs) == [("postgres://db.example:5434", "files.public.file_list")]
    assert _ol(lineage.inputs) == [("file", str(a.resolve())), ("file", str(b.resolve()))]


def test_p2_5_source_files_are_named_like_spark_reads_them(collector, tmp_path, monkeypatch):
    """Relative path, symlinked directory: resolved like normalize_spark_path
    (what the job hands to Spark), so the listing and the Spark run share
    the same input dataset."""
    from utils.file_list_lineage import report_file_list_lineage
    from utils.path_utils import normalize_spark_path

    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "link").symlink_to(real, target_is_directory=True)
    monkeypatch.chdir(tmp_path)
    report_file_list_lineage(_Hook(), role="output", source_files=["link/x.csv"])

    [(namespace, name)] = _ol(collector.collected_assets.inputs)
    assert namespace == "file"
    assert "file://" + name == normalize_spark_path("link/x.csv")


def test_p2_5_spark_task_reports_file_list_as_input(collector):
    from utils.file_list_lineage import report_file_list_lineage

    report_file_list_lineage(
        _Hook(host="pg", port=None, schema="x", database="files"), role="input"
    )
    lineage = collector.collected_assets
    # PostgresHook.database overrides the Connection's schema; default port.
    assert _ol(lineage.inputs) == [("postgres://pg:5432", "files.public.file_list")]
    assert lineage.outputs == []


def test_p2_5_reporting_lineage_never_raises(collector, caplog):
    from utils.file_list_lineage import report_file_list_lineage

    hook = _Hook()
    hook.get_connection = MagicMock(side_effect=RuntimeError("no such connection"))
    with caplog.at_level("WARNING"):
        report_file_list_lineage(hook, role="output", source_files=["/x.csv"])
    assert "file_list" in caplog.text
    assert collector.collected_assets.outputs == []


def test_p2_5_unknown_role_is_a_programming_error():
    from utils.file_list_lineage import report_file_list_lineage

    with pytest.raises(ValueError, match="role"):
        report_file_list_lineage(_Hook(), role="both")


def test_p2_5_helper_does_not_import_airflow_at_module_level():
    """utils/ is imported by the Spark job, which must never import Airflow."""
    source = (Path(__file__).resolve().parents[3] / "utils" / "file_list_lineage.py").read_text()
    top_level = [line for line in source.splitlines() if line.startswith(("import ", "from "))]
    assert not [line for line in top_level if "airflow" in line], top_level


# ---------------------------------------------------------------------------
# The listing tasks call it with the files they recorded
# ---------------------------------------------------------------------------


def _files(tmp_path, count=2):
    paths = []
    for i in range(count):
        p = tmp_path / f"file_{i}.csv"
        p.write_text("a\n1\n")
        paths.append(str(p))
    return paths


def test_p2_5_taskflow_listing_reports_recorded_files(tmp_path):
    from dags.local_list_files_dag import dag_instance

    insert_files = dag_instance.get_task("insert_files").python_callable
    files = _files(tmp_path)
    vanished = str(tmp_path / "vanished.csv")
    hook_cls = MagicMock()
    with (
        patch("airflow.providers.postgres.hooks.postgres.PostgresHook", hook_cls),
        patch("airflow.sdk.get_current_context", return_value={"dag_run": MagicMock(run_id="r")}),
        patch("utils.functions_utils.insert_into_file_list"),
        patch("utils.file_list_lineage.report_file_list_lineage") as report,
    ):
        assert insert_files([*files, vanished]) == 2

    report.assert_called_once_with(hook_cls.return_value, role="output", source_files=files)


def test_p2_5_classic_listing_reports_recorded_files(tmp_path):
    from dags.local_list_files_dag_python_operator import insert_files_to_db

    files = _files(tmp_path)
    ti = MagicMock()
    ti.xcom_pull.return_value = files
    hook_cls = MagicMock()
    with (
        patch("airflow.providers.postgres.hooks.postgres.PostgresHook", hook_cls),
        patch("utils.functions_utils.insert_into_file_list"),
        patch("utils.file_list_lineage.report_file_list_lineage") as report,
    ):
        assert insert_files_to_db(ti=ti, run_id="r") == 2

    report.assert_called_once_with(hook_cls.return_value, role="output", source_files=files)


def test_p2_5_report_file_list_input_uses_the_file_list_connection(collector):
    from dags.local_read_file_dag import report_file_list_input

    hook_cls = MagicMock()
    with (
        patch("airflow.providers.postgres.hooks.postgres.PostgresHook", hook_cls),
        patch("utils.file_list_lineage.report_file_list_lineage") as report,
    ):
        report_file_list_input({})
    hook_cls.assert_called_once_with(postgres_conn_id="file_list_db")
    report.assert_called_once_with(hook_cls.return_value, role="input")


# ---------------------------------------------------------------------------
# Review fixes
# ---------------------------------------------------------------------------


def test_p2_5_c6_files_beyond_the_collector_limit_are_reported(collector, tmp_path, caplog):
    """The collector keeps at most [lineage] max_assets_per_collector inputs
    (100 by default, read when airflow.sdk.lineage is imported): the files
    beyond are dropped, and the warning says how many."""
    import airflow.sdk.lineage as sdk_lineage

    from utils.file_list_lineage import report_file_list_lineage

    limit = sdk_lineage.MAX_COLLECTED_ASSETS
    assert limit == 100  # the default; the Makefile raises it for Airflow
    files = [str(tmp_path / f"f{i}.csv") for i in range(limit + 50)]
    with caplog.at_level("WARNING"):
        report_file_list_lineage(_Hook(), role="output", source_files=files)
    assert len(collector.collected_assets.inputs) == limit
    assert "50 source file(s)" in caplog.text
    assert "AIRFLOW__LINEAGE__MAX_ASSETS_PER_COLLECTOR" in caplog.text


@pytest.mark.parametrize("name", ["orders_a#b.csv", "orders_q?x.csv"])
def test_p2_5_c7_hash_or_question_mark_file_is_skipped_with_a_warning(
    collector, tmp_path, caplog, name
):
    """An Asset URI ends its path at `#` / `?`, and quoting gives `%23` /
    `%3F`: neither is the name Spark gives the file (verbatim). Better no
    dataset than a wrong one."""
    from utils.file_list_lineage import report_file_list_lineage

    kept = tmp_path / "sp ace é.csv"
    with caplog.at_level("WARNING"):
        report_file_list_lineage(
            _Hook(), role="output", source_files=[str(tmp_path / name), str(kept)]
        )
    assert _ol(collector.collected_assets.inputs) == [("file", str(kept.resolve()))]
    assert name in caplog.text


def test_p2_5_c10_noop_collector_returns_before_touching_the_connection():
    """OpenLineage off: no Connection lookup at all."""
    from utils.file_list_lineage import report_file_list_lineage

    hook = _Hook()
    hook.get_connection = MagicMock(side_effect=AssertionError("must not be called"))
    report_file_list_lineage(hook, role="output", source_files=["/x.csv"])
    hook.get_connection.assert_not_called()
