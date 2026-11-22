"""
P2-2 (constat A1): the `storage` section of the reading configs.

- `storage` is mandatory: every file read is written at least to the raw
  zone, nothing is dropped silently. A config without it is a config error
  (exit code 5 of the Spark job, at load).
- `staging_path` is optional: absent means no staging zone for that source.
- `raw_path`, `staging_path` and `table_name` (or `source.name` when
  `table_name` is absent) can never leave `LAKE_ROOT`: no `..`, no absolute
  path, no URI scheme.
- No table may sit inside another one (C-3), within a source (raw vs
  staging) or across sources: Spark would find two partition layouts in the
  outer table and refuse to read it (`CONFLICTING_DIRECTORY_STRUCTURES`).
  Compared case-insensitively (case-insensitive file systems). Two sources
  sharing exactly the same table are fine: their partitions differ.
"""

from pathlib import Path

import pytest
import yaml

from pipeline.config import ConfigError, SourceConfig, SourceRegistry, load_source_config

REPO_CONFIG_ROOT = Path(__file__).resolve().parent.parent.parent.parent / "config"
REPO_SOURCE_FILES = sorted((REPO_CONFIG_ROOT / "sources").glob("*.yaml"))
REPO_DEFAULT_FILES = sorted((REPO_CONFIG_ROOT / "defaults").glob("*.yaml"))

RAW_ONLY = {"format": "parquet", "mode": "overwrite", "raw_path": "raw/"}


def _config(storage=RAW_ONLY, name="orders") -> dict:
    config = {
        "source": {"name": name, "format": "csv", "patterns": ["orders_*.csv"]},
        "reader": {"options": {"header": True}},
    }
    if storage is not None:
        config["storage"] = storage
    return config


def _error(config: dict, tmp_path: Path) -> str:
    path = tmp_path / "sources" / "bad.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, sort_keys=False))
    with pytest.raises(ConfigError) as exc_info:
        load_source_config(path)
    return str(exc_info.value)


# ---------------------------------------------------------------------------
# storage is mandatory, staging is optional
# ---------------------------------------------------------------------------


def test_p2_2_storage_is_mandatory(tmp_path):
    message = _error(_config(storage=None), tmp_path)
    assert "bad.yaml" in message
    assert "storage" in message


@pytest.mark.parametrize("missing", ["format", "mode", "raw_path"])
def test_p2_2_incomplete_storage_is_refused(tmp_path, missing):
    storage = {key: value for key, value in RAW_ONLY.items() if key != missing}
    assert f"storage.{missing}" in _error(_config(storage=storage), tmp_path)


def test_p2_2_staging_path_is_optional():
    config = SourceConfig.model_validate(_config())
    assert config.storage.staging_path is None
    assert config.staging_target is None
    assert config.raw_target == "raw/orders"


def test_p2_2_targets_use_table_name_or_source_name():
    with_table = SourceConfig.model_validate(
        _config(storage={**RAW_ONLY, "table_name": "sales", "staging_path": "zones/staging"})
    )
    assert with_table.table == "sales"
    assert with_table.raw_target == "raw/sales"
    assert with_table.staging_target == "zones/staging/sales"

    without_table = SourceConfig.model_validate(_config(name="default_csv"))
    assert without_table.table == "default_csv"
    assert without_table.raw_target == "raw/default_csv"


def test_p2_2_raw_and_staging_must_differ(tmp_path):
    message = _error(_config(storage={**RAW_ONLY, "staging_path": "raw"}), tmp_path)
    assert "staging_path" in message


@pytest.mark.parametrize(
    ("raw_path", "staging_path"),
    [
        ("raw/", "raw/orders/"),  # staging/orders inside raw/orders
        ("staging/orders", "staging/"),  # raw/orders inside staging/orders
        ("raw/", "raw/orders/sub"),
    ],
)
def test_p2_2_staging_nested_in_raw_table_is_refused(tmp_path, raw_path, staging_path):
    """C-3: `raw/orders/orders/` inside `raw/orders/` made raw unreadable."""
    message = _error(
        _config(storage={**RAW_ONLY, "raw_path": raw_path, "staging_path": staging_path}),
        tmp_path,
    )
    assert "storage" in message
    assert "imbriqu" in message


@pytest.mark.parametrize(("raw_path", "staging_path"), [("raw/", "RAW/"), ("raw/", "Raw/Orders/")])
def test_p2_2_raw_and_staging_overlap_is_case_insensitive(tmp_path, raw_path, staging_path):
    """Same directory, or nested, on a case-insensitive file system."""
    message = _error(
        _config(storage={**RAW_ONLY, "raw_path": raw_path, "staging_path": staging_path}),
        tmp_path,
    )
    assert "staging_path" in message


def test_p2_2_sibling_zones_with_a_common_prefix_are_accepted():
    """`raw/orders` and `raw_v2/orders`: a common string prefix is not a
    directory prefix."""
    config = SourceConfig.model_validate(
        _config(storage={**RAW_ONLY, "raw_path": "raw", "staging_path": "raw_v2"})
    )
    assert (config.raw_target, config.staging_target) == ("raw/orders", "raw_v2/orders")


# ---------------------------------------------------------------------------
# C-6: a config without storage still reports its other errors
# ---------------------------------------------------------------------------


def test_p2_2_missing_storage_does_not_hide_the_other_errors(tmp_path):
    """Pydantic skips `after` validators when a required field is missing:
    the option typo used to be reported only once storage was added."""
    config = _config(storage=None)
    config["reader"]["options"] = {"header": True, "infer_schema": True}

    message = _error(config, tmp_path)

    assert "storage" in message
    assert "reader.options.infer_schema" in message
    assert "inferSchema" in message  # the suggestion


def test_p2_2_targets_of_a_config_without_storage_fail_explicitly():
    config = SourceConfig.model_construct(
        source=SourceConfig.model_validate(_config()).source, storage=None
    )
    for prop in ("table", "raw_target", "staging_target"):
        with pytest.raises(ValueError, match="storage"):
            getattr(config, prop)


# ---------------------------------------------------------------------------
# C-3 across sources: SourceRegistry checks every table of every config
# ---------------------------------------------------------------------------


def _registry_error(tmp_path: Path, *configs: tuple[str, dict]) -> str:
    root = tmp_path / "config"
    for relative, config in configs:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(config, sort_keys=False))
    with pytest.raises(ConfigError) as exc_info:
        SourceRegistry.from_directory(root)
    return str(exc_info.value)


def _named(name, pattern, storage):
    config = _config(storage=storage, name=name)
    config["source"]["patterns"] = [pattern]
    return config


ORDERS = _named("orders", "orders_*.csv", {**RAW_ONLY, "staging_path": "staging/"})


@pytest.mark.parametrize("raw_path", ["raw/orders/", "staging/orders", "RAW/Orders/"])
def test_p2_2_table_nested_in_another_source_table_is_refused(tmp_path, raw_path):
    """C-3: source B's table inside source A's (raw/orders/b) breaks A's."""
    other = _named("b", "b_*.csv", {**RAW_ONLY, "raw_path": raw_path})
    message = _registry_error(tmp_path, ("sources/orders.yaml", ORDERS), ("sources/b.yaml", other))
    assert "orders.yaml" in message
    assert "b.yaml" in message
    assert "imbriqu" in message


def test_p2_2_default_table_nested_in_a_source_table_is_refused(tmp_path):
    default = _named("default_csv", "*.csv", {**RAW_ONLY, "raw_path": "staging/orders/x"})
    message = _registry_error(
        tmp_path, ("sources/orders.yaml", ORDERS), ("defaults/csv.yaml", default)
    )
    assert "csv.yaml" in message


def test_p2_2_same_table_with_another_case_is_refused(tmp_path):
    """`raw/Orders` and `raw/orders`: one directory on a case-insensitive file
    system, two on the others. Declaring a shared table means writing it
    identically."""
    other = _named("b", "b_*.csv", {**RAW_ONLY, "table_name": "Orders"})
    message = _registry_error(tmp_path, ("sources/orders.yaml", ORDERS), ("sources/b.yaml", other))
    assert "raw/Orders" in message
    assert "raw/orders" in message


def test_p2_2_two_sources_may_share_exactly_the_same_table(tmp_path):
    root = tmp_path / "config"
    other = _named("b", "b_*.csv", {**RAW_ONLY, "table_name": "orders"})
    for relative, config in (("sources/orders.yaml", ORDERS), ("sources/b.yaml", other)):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(config, sort_keys=False))

    registry = SourceRegistry.from_directory(root)

    assert registry.match("b_1.csv").raw_target == registry.match("orders_1.csv").raw_target


def test_p2_2_append_mode_is_still_refused(tmp_path):
    assert "overwrite" in _error(_config(storage={**RAW_ONLY, "mode": "append"}), tmp_path)


# ---------------------------------------------------------------------------
# Paths can never leave LAKE_ROOT
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("field", ["raw_path", "staging_path"])
@pytest.mark.parametrize(
    "value",
    [
        "../raw",
        "raw/../../etc",
        "..",
        "/abs/raw",
        "s3://bucket/raw",
        "file:/tmp/raw",
        "C:\\raw",
        "raw\\sub",
        "raw//sub",
        "./raw",
        "_raw",
        "raw/_run_id=x",
        "raw/lake_run_id=x",
        "",
    ],
)
def test_p2_2_dangerous_zone_paths_are_refused(tmp_path, field, value):
    storage = {**RAW_ONLY, "staging_path": "staging/"}
    storage[field] = value
    assert f"storage.{field}" in _error(_config(storage=storage), tmp_path)


@pytest.mark.parametrize("value", ["..", "../orders", "a/b", "s3:x", "/orders", "_orders", "a b"])
def test_p2_2_dangerous_table_names_are_refused(tmp_path, value):
    message = _error(_config(storage={**RAW_ONLY, "table_name": value}), tmp_path)
    assert "storage.table_name" in message


def test_p2_2_source_name_used_as_table_must_be_a_safe_name(tmp_path):
    message = _error(_config(name="../orders"), tmp_path)
    assert "table_name" in message
    # An explicit table_name lifts the constraint on source.name.
    SourceConfig.model_validate(_config(name="../orders", storage={**RAW_ONLY, "table_name": "o"}))


@pytest.mark.parametrize("value", ["raw", "raw/", "zones/raw/", "lake-v1.0/raw_zone"])
def test_p2_2_safe_zone_paths_are_accepted(value):
    config = SourceConfig.model_validate(_config(storage={**RAW_ONLY, "raw_path": value}))
    assert config.raw_target == f"{value.rstrip('/')}/orders"


# ---------------------------------------------------------------------------
# The YAML files of the repository
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "config_file",
    REPO_SOURCE_FILES + REPO_DEFAULT_FILES,
    ids=lambda p: f"{p.parent.name}/{p.name}",
)
def test_p2_2_every_repo_config_writes_to_the_raw_zone(config_file):
    config = load_source_config(config_file)
    assert config.storage.format == "parquet"
    assert config.raw_target.startswith("raw/")


@pytest.mark.parametrize("config_file", REPO_DEFAULT_FILES, ids=lambda p: p.name)
def test_p2_2_defaults_land_in_raw_only(config_file):
    config = load_source_config(config_file)
    assert config.table == f"default_{config.source.format}"
    assert config.storage.staging_path is None
    assert config.staging_target is None


def test_p2_2_dedicated_sources_have_a_staging_zone():
    registry = SourceRegistry.from_directory(REPO_CONFIG_ROOT)
    orders = registry.match("orders_2024.csv")
    assert (orders.raw_target, orders.staging_target) == ("raw/orders", "staging/orders")
    movies = registry.match("IMDB_movies.csv")
    assert (movies.raw_target, movies.staging_target) == ("raw/movies", "staging/movies")
    assert registry.match("random.csv").raw_target == "raw/default_csv"
