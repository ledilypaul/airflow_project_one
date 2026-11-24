"""P5-2 — the Polars whitelists against the installed Polars (skipped without it)."""

import inspect

import pytest

from pipeline.config.reader_options import POLARS_READER_OPTIONS_BY_FORMAT
from pipeline.config.types import NEUTRAL_TYPES

pl = pytest.importorskip("polars")

# Parameters that the whitelists leave out on purpose (see reader_options.py).
EXCLUDED = {
    "csv": {
        "source", "schema", "schema_overrides", "columns", "new_columns", "with_column_names",
        "glob", "storage_options", "credential_provider", "include_file_paths", "row_index_name",
        "row_index_offset", "n_rows", "low_memory", "use_pyarrow",
    },  # fmt: skip
    "parquet": {
        "source", "n_rows", "row_index_name", "row_index_offset", "glob", "schema", "hive_schema",
        "low_memory", "cache", "storage_options", "credential_provider", "include_file_paths",
        "cast_options", "decryption", "rechunk",
    },  # fmt: skip
    "xlsx": {"source", "include_file_paths"},
}  # fmt: skip


def _parameters(function) -> set[str]:
    return {name for name in inspect.signature(function).parameters if not name.startswith("_")}


@pytest.mark.parametrize(
    ("fmt", "function"),
    [("csv", pl.scan_csv), ("parquet", pl.scan_parquet), ("xlsx", pl.read_excel)],
)
def test_p5_2_polars_whitelist_is_a_subset_of_the_installed_signature(fmt, function):
    assert POLARS_READER_OPTIONS_BY_FORMAT[fmt] <= _parameters(function)


@pytest.mark.parametrize(
    ("fmt", "function"),
    [("csv", pl.scan_csv), ("parquet", pl.scan_parquet), ("xlsx", pl.read_excel)],
)
def test_p5_2_polars_whitelist_covers_the_signature_but_the_exclusions(fmt, function):
    """A parameter added by a Polars upgrade shows up here: review it."""
    missing = _parameters(function) - POLARS_READER_OPTIONS_BY_FORMAT[fmt] - EXCLUDED[fmt]
    assert not missing, f"new Polars options to classify: {sorted(missing)}"


def test_p5_2_polars_type_names_exist():
    for name, column_type in NEUTRAL_TYPES.items():
        assert hasattr(pl, column_type.polars), name
