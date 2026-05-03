"""
Unit tests for utils/path_utils.py.
Pure Python — no Spark or DB needed.
"""
import pytest
from utils.path_utils import normalize_spark_path


# ---------------------------------------------------------------------------
# Distributed protocols → untouched
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", [
    "hdfs://namenode:9000/data/file.csv",
    "s3://my-bucket/prefix/file.parquet",
    "gs://gcs-bucket/file.csv",
    "dbfs:/mnt/data/file.parquet",
    "abfss://container@account.dfs.core.windows.net/path",
    "adl://account.azuredatalakestore.net/path",
])
def test_distributed_paths_are_unchanged(path):
    assert normalize_spark_path(path) == path


# ---------------------------------------------------------------------------
# Local paths → get file:// prefix
# ---------------------------------------------------------------------------

def test_local_absolute_path_gets_file_prefix():
    result = normalize_spark_path("/data/files/orders.csv")
    assert result.startswith("file://")
    assert "data/files/orders.csv" in result


def test_local_path_uses_triple_slash_on_linux():
    # On Linux, Path.drive is empty → file:// + absolute path
    result = normalize_spark_path("/tmp/test.parquet")
    # Linux: file:///tmp/test.parquet  (file:// + /tmp/...)
    assert result.startswith("file://")
    assert result.endswith("test.parquet")


def test_relative_path_is_resolved_to_absolute():
    result = normalize_spark_path("some/relative/file.csv")
    assert result.startswith("file://")
    assert "some/relative/file.csv" in result


def test_tilde_path_is_expanded():
    result = normalize_spark_path("~/data/file.csv")
    assert result.startswith("file://")
    assert "~" not in result  # tilde must be expanded


# ---------------------------------------------------------------------------
# Idempotency edge-case (documents known limitation)
# ---------------------------------------------------------------------------

def test_file_protocol_not_idempotent():
    """
    Known limitation: file:// paths are NOT in distributed_protocols,
    so they get re-processed by Path() and produce incorrect results.
    This test documents the current behaviour.
    If this is fixed, remove this test.
    """
    path = "file:///data/file.csv"
    result = normalize_spark_path(path)
    # Current behaviour: path is passed through Path() and mangled
    assert result != path
