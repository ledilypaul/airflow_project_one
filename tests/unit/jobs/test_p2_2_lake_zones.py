"""
P2-2 (constat A1): what the Spark job writes for one file.

- raw zone: always, the DataFrame exactly as read (plus the technical columns);
- staging zone: only when the source declares `storage.staging_path`, with a
  minimal, deterministic cleanup: fully empty rows and exact duplicates
  removed, nothing else. The cleanup runs on the business columns, before the
  technical columns are added: `lake_run_id`/`lake_source_file` are never
  null, so a fully empty row would otherwise never be dropped.
- `main()` end to end with the real Spark session, reader, configs and writer
  (C-9): only the file_list database is mocked.

Real Spark (session-wide `spark` fixture), `tmp_path` as `LAKE_ROOT`.
"""

from unittest.mock import MagicMock, call, patch

import pytest

from pipeline.config import SourceConfig
from pipeline.jobs import read_files_job
from pipeline.jobs.read_files_job import clean_for_staging, write_to_lake
from storage.parquet_lake_writer import ParquetLakeWriter, resolve_lake_root

RUN_ID = "manual__2026-11-13T14:21:36.519211+00:00"
FILE_ID = 7
BUSINESS_COLUMNS = ["order_id", "item", "amount"]
ROWS = [
    (1, "a", 1.5),
    (1, "a", 1.5),  # exact duplicate
    (None, None, None),  # fully empty
    (2, None, None),  # partially empty: kept
    (3, "c", 2.0),
]


def _config(staging_path="staging/") -> SourceConfig:
    storage = {"table_name": "orders", "format": "parquet", "mode": "overwrite", "raw_path": "raw/"}
    if staging_path is not None:
        storage["staging_path"] = staging_path
    return SourceConfig.model_validate(
        {
            "source": {"name": "orders", "format": "csv", "patterns": ["orders_*.csv"]},
            "storage": storage,
        }
    )


@pytest.fixture
def writer(tmp_path):
    return ParquetLakeWriter(resolve_lake_root({"LAKE_ROOT": str(tmp_path)}))


@pytest.fixture
def orders(spark):
    return spark.createDataFrame(ROWS, "order_id int, item string, amount double")


def _write(writer, data, config, source_file="orders_1.csv"):
    return write_to_lake(
        writer, data, config, run_id=RUN_ID, source_file=source_file, file_id=FILE_ID
    )


def _business_rows(spark, root, target):
    df = spark.read.parquet((root / target).as_uri())
    return sorted(
        (tuple(r[c] for c in BUSINESS_COLUMNS) for r in df.collect()),
        key=lambda row: tuple((v is None, v) for v in row),
    )


def test_p2_2_raw_only_when_no_staging_path(spark, writer, orders, tmp_path):
    written = _write(writer, orders, _config(staging_path=None))

    assert written == {"raw": 5}
    assert (tmp_path / "raw" / "orders").is_dir()
    assert not (tmp_path / "staging").exists()


def test_p2_2_raw_keeps_the_data_as_read(spark, writer, orders, tmp_path):
    _write(writer, orders, _config())
    assert len(_business_rows(spark, tmp_path, "raw/orders")) == 5


def test_p2_2_staging_removes_empty_rows_and_exact_duplicates_only(spark, writer, orders, tmp_path):
    written = _write(writer, orders, _config())

    assert written == {"raw": 5, "staging": 3}
    assert _business_rows(spark, tmp_path, "staging/orders") == [
        (1, "a", 1.5),
        (2, None, None),
        (3, "c", 2.0),
    ]
    staging = spark.read.parquet((tmp_path / "staging" / "orders").as_uri())
    # No other transformation: same business columns, same types.
    assert [(c, t) for c, t in staging.dtypes if not c.startswith("lake_")] == orders.dtypes
    assert {(r.lake_run_id, r.lake_source_file, r.lake_file_id) for r in staging.collect()} == {
        (RUN_ID, "orders_1.csv", FILE_ID)
    }


def test_p2_2_staging_rerun_is_idempotent(spark, writer, orders, tmp_path):
    for _ in range(2):
        _write(writer, orders, _config())

    assert len(_business_rows(spark, tmp_path, "raw/orders")) == 5
    assert len(_business_rows(spark, tmp_path, "staging/orders")) == 3


def test_p2_2_staging_with_zero_rows_removes_the_previous_staging_partition(
    spark, writer, orders, tmp_path
):
    """C-2: raw has rows, staging none (every row fully empty): the staging
    partition of a previous attempt must not survive the rewrite."""
    _write(writer, orders, _config())
    only_empty = spark.createDataFrame(
        [(None, None, None)] * 2, "order_id int, item string, amount double"
    )

    assert _write(writer, only_empty, _config()) == {"raw": 2, "staging": 0}
    assert len(_business_rows(spark, tmp_path, "raw/orders")) == 2
    assert not list((tmp_path / "staging" / "orders").glob("lake_run_id=*/lake_source_file=*"))


def test_p2_2_clean_for_staging_ignores_nothing_but_empty_rows_and_duplicates(spark, orders):
    cleaned = clean_for_staging(orders)
    assert cleaned.columns == orders.columns
    assert cleaned.count() == 3


def test_p2_2_staging_failure_propagates_after_raw(spark, orders):
    """raw written, staging failed: the error reaches the job (file `failed`);
    a retry rewrites raw idempotently. The DataFrame persisted for the two
    writes is released all the same."""
    cached_during_writes = []

    def _write_side_effect(data, target, **kwargs):
        cached_during_writes.append(orders.is_cached)
        if target == "staging/orders":
            raise RuntimeError("staging disk full")
        return 5

    writer = MagicMock()
    writer.write.side_effect = _write_side_effect

    with pytest.raises(RuntimeError, match="staging disk full"):
        _write(writer, orders, _config())

    assert [c.args[1] for c in writer.write.call_args_list] == ["raw/orders", "staging/orders"]
    for c in writer.write.call_args_list:
        assert c.kwargs == {
            "mode": "overwrite",
            "run_id": RUN_ID,
            "source_file": "orders_1.csv",
            "file_id": FILE_ID,
        }
    assert cached_during_writes == [True, True]
    assert not orders.is_cached


def test_p2_2_write_passes_the_storage_mode_of_the_config(orders):
    """C-8: `storage.mode` is the config's choice, not the writer's default."""
    writer = MagicMock()
    writer.write.return_value = 1
    config = _config(staging_path=None)

    _write(writer, orders, config)

    assert writer.write.call_args.kwargs["mode"] == config.storage.mode


# ---------------------------------------------------------------------------
# C-9: main() end to end, with real Spark, reader, configs and writer
# ---------------------------------------------------------------------------


def test_p2_2_main_writes_real_file_end_to_end(spark, tmp_path, monkeypatch):
    source = tmp_path / "in" / "orders_x.csv"
    source.parent.mkdir()
    source.write_text("order_id,item,amount\n1,a,1.5\n1,a,1.5\n,,\n3,c,2.0\n", encoding="utf-8")
    lake_root = tmp_path / "lake"
    monkeypatch.setenv("LAKE_ROOT", str(lake_root))
    # The session is shared by the whole test run: main() must not stop it.
    monkeypatch.setattr(spark, "stop", lambda: None)
    update = MagicMock()
    rows = [{"id": FILE_ID, "file_name": source.name, "file_path": str(source)}]

    with (
        patch.object(read_files_job, "get_spark_session", return_value=spark),
        patch.object(read_files_job, "list_file_from_db", return_value=rows),
        patch.object(read_files_job, "update_file_status", update),
    ):
        with pytest.raises(SystemExit) as exc:
            read_files_job.main(run_id=RUN_ID)

    assert exc.value.code == 0
    assert update.call_args_list == [
        call(FILE_ID, "pending", error_message=None),
        call(FILE_ID, "processed"),
    ]
    # config/sources/orders.yaml: table `orders`, raw and staging zones.
    assert _business_rows(spark, lake_root, "raw/orders") == [
        (1, "a", 1.5),
        (1, "a", 1.5),
        (3, "c", 2.0),
        (None, None, None),
    ]
    assert _business_rows(spark, lake_root, "staging/orders") == [(1, "a", 1.5), (3, "c", 2.0)]
    for zone in ("raw", "staging"):
        df = spark.read.parquet((lake_root / zone / "orders").as_uri())
        assert {(r.lake_run_id, r.lake_source_file, r.lake_file_id) for r in df.collect()} == {
            (RUN_ID, "orders_x.csv", FILE_ID)
        }
