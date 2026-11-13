"""
P2-3 (docs/audit/2026-11-05-baseline.md): quality checks declared in the YAML
of each source, validated at load by `SourceConfig`.

    quality:
      checks:
        - {type: not_null, column: id}
        - {type: unique, columns: [id]}
        - {type: accepted_values, column: status, values: [a, b], severity: warn}
        - {type: range, column: amount, min: 0, max: 100}
        - {type: regex, column: code, pattern: "^[A-Z]{3}$"}
        - {type: row_count, min: 1}

- A discriminated union on `type`, `extra="forbid"` everywhere,
  `severity` = `error` (default, blocks staging) or `warn`.
- `quality` requires `storage.staging_path`: the checks decide whether the
  staging zone is written.
- With `columns_type`, every column a check names must be declared there
  (typos caught at load); `range` needs a numeric declared type.
- No two identical checks, no two checks with the same (stable) name.
- Semantic errors are aggregated with the other errors of the same file in
  one `ConfigError` (C-6).
- `quality/` is reserved under LAKE_ROOT for the check results.
"""

from pathlib import Path

import pytest
import yaml

from pipeline.config import ConfigError, SourceConfig, SourceRegistry, load_source_config
from pipeline.config.quality import QUALITY_ZONE

REPO_CONFIG_ROOT = Path(__file__).resolve().parent.parent.parent.parent / "config"

STORAGE = {
    "table_name": "orders",
    "format": "parquet",
    "mode": "overwrite",
    "raw_path": "raw/",
    "staging_path": "staging/",
}


def _config(checks, storage=STORAGE, columns_type=None, options=None) -> dict:
    config = {
        "source": {"name": "orders", "format": "csv", "patterns": ["orders_*.csv"]},
        "reader": {"options": options if options is not None else {"header": True}},
        "storage": dict(storage),
    }
    if columns_type is not None:
        config["reader"]["columns_type"] = columns_type
    if checks is not None:
        config["quality"] = {"checks": checks}
    return config


def _error(config: dict, tmp_path: Path) -> str:
    path = tmp_path / "sources" / "bad.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, sort_keys=False))
    with pytest.raises(ConfigError) as exc_info:
        load_source_config(path)
    message = str(exc_info.value)
    assert "bad.yaml" in message
    return message


# ---------------------------------------------------------------------------
# Every check type: valid
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "check, expected_name, expected_columns",
    [
        ({"type": "not_null", "column": "id"}, "not_null(id)", ("id",)),
        ({"type": "unique", "columns": ["id"]}, "unique(id)", ("id",)),
        ({"type": "unique", "columns": ["a", "b"]}, "unique(a,b)", ("a", "b")),
        (
            {"type": "accepted_values", "column": "status", "values": ["new", "done", "1", "true"]},
            "accepted_values(status)",
            ("status",),
        ),
        ({"type": "range", "column": "amount", "min": 0}, "range(amount)", ("amount",)),
        ({"type": "range", "column": "amount", "max": 9.5}, "range(amount)", ("amount",)),
        ({"type": "range", "column": "amount", "min": 1, "max": 1}, "range(amount)", ("amount",)),
        ({"type": "regex", "column": "code", "pattern": "^[A-Z]{3}$"}, "regex(code)", ("code",)),
        ({"type": "row_count", "min": 1}, "row_count", ()),
        ({"type": "row_count", "max": 10}, "row_count", ()),
        ({"type": "row_count", "min": 0, "max": 0}, "row_count", ()),
    ],
)
def test_p2_3_each_check_type_is_accepted(check, expected_name, expected_columns):
    config = SourceConfig.model_validate(_config([check]))

    (parsed,) = config.quality.checks
    assert parsed.type == check["type"]
    assert parsed.check_name == expected_name
    assert parsed.check_columns == expected_columns
    assert parsed.severity == "error"  # default: blocks staging


def test_p2_3_severity_warn_and_explicit_name():
    config = SourceConfig.model_validate(
        _config([{"type": "row_count", "min": 1, "severity": "warn", "name": "not_empty"}])
    )
    (check,) = config.quality.checks
    assert (check.severity, check.check_name) == ("warn", "not_empty")


def test_p2_3_quality_is_optional_and_targets_a_reserved_zone():
    without = SourceConfig.model_validate(_config(None))
    assert without.quality is None
    assert without.quality_target is None

    with_checks = SourceConfig.model_validate(_config([{"type": "row_count", "min": 1}]))
    assert QUALITY_ZONE == "quality"
    assert with_checks.quality_target == "quality/orders"


# ---------------------------------------------------------------------------
# Every check type: invalid
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "check, detail",
    [
        # Missing or empty fields
        ({"type": "not_null"}, "column"),
        ({"type": "not_null", "column": "  "}, "column"),
        ({"type": "unique"}, "columns"),
        ({"type": "unique", "columns": []}, "columns"),
        ({"type": "unique", "columns": ["a", "a"]}, "deux fois"),
        ({"type": "accepted_values", "column": "s"}, "values"),
        ({"type": "accepted_values", "column": "s", "values": []}, "values"),
        ({"type": "accepted_values", "column": "s", "values": ["a", "a"]}, "deux fois"),
        ({"type": "accepted_values", "column": "s", "values": [1.5]}, "values"),
        ({"type": "range", "column": "amount"}, "min ou max"),
        ({"type": "range", "min": 0}, "column"),
        ({"type": "range", "column": "amount", "min": 5, "max": 1}, "min (5) > max (1)"),
        ({"type": "range", "column": "amount", "min": "0"}, "min"),
        ({"type": "range", "column": "amount", "min": True}, "min"),
        ({"type": "regex", "column": "code"}, "pattern"),
        ({"type": "regex", "column": "code", "pattern": "([A-Z"}, "regex invalide"),
        # Valid in Java (Unicode class), refused by Python: the common subset only.
        ({"type": "regex", "column": "code", "pattern": r"^\p{L}+$"}, "regex invalide"),
        # Python reads `[[` as a literal `[`, Java as a nested class: refused.
        ({"type": "regex", "column": "code", "pattern": "[[:alpha:]]+"}, "regex invalide"),
        # Python-only syntax for named groups (Java: `(?<name>...)`).
        ({"type": "regex", "column": "code", "pattern": "(?P<x>a)"}, "(?P<"),
        ({"type": "row_count"}, "min ou max"),
        ({"type": "row_count", "min": -1}, "min"),
        ({"type": "row_count", "min": 3, "max": 2}, "min (3) > max (2)"),
        ({"type": "row_count", "min": 1.5}, "min"),
        # Unknown type, unknown key, bad severity, bad name
        ({"type": "not_empty", "column": "id"}, "not_empty"),
        ({"column": "id"}, "type"),
        ({"type": "not_null", "colum": "id"}, "colum"),
        ({"type": "not_null", "column": "id", "severity": "fatal"}, "severity"),
        ({"type": "not_null", "column": "id", "name": ""}, "name"),
        ({"type": "row_count", "column": "id", "min": 1}, "column"),
    ],
)
def test_p2_3_invalid_check_is_rejected_with_its_field(tmp_path, check, detail):
    message = _error(_config([check]), tmp_path)
    assert "quality.checks" in message
    assert detail in message


def test_p2_3_empty_check_list_and_unknown_quality_key_are_rejected(tmp_path):
    assert "checks" in _error(_config([]), tmp_path)
    config = _config([{"type": "row_count", "min": 1}])
    config["quality"]["fail_fast"] = True
    assert "fail_fast" in _error(config, tmp_path)


# ---------------------------------------------------------------------------
# Cross-section rules
# ---------------------------------------------------------------------------


def test_p2_3_quality_without_staging_path_is_refused(tmp_path):
    storage = {k: v for k, v in STORAGE.items() if k != "staging_path"}
    message = _error(_config([{"type": "row_count", "min": 1}], storage=storage), tmp_path)
    assert "quality" in message
    assert "staging_path" in message


@pytest.mark.parametrize(
    "check, missing",
    [
        ({"type": "not_null", "column": "idd"}, "idd"),
        ({"type": "unique", "columns": ["id", "itme"]}, "itme"),
        ({"type": "accepted_values", "column": "Item", "values": ["a"]}, "Item"),
        ({"type": "range", "column": "amout", "min": 0}, "amout"),
        ({"type": "regex", "column": "x", "pattern": "a"}, "x"),
    ],
)
def test_p2_3_column_absent_from_columns_type_is_refused(tmp_path, check, missing):
    columns_type = {"id": "int", "item": "string", "amount": "double"}
    message = _error(_config([check], columns_type=columns_type), tmp_path)
    assert f"{missing!r}" in message
    assert "columns_type" in message


def test_p2_3_column_absent_from_columns_type_suggests_the_close_name(tmp_path):
    message = _error(
        _config([{"type": "not_null", "column": "idd"}], columns_type={"id": "int"}), tmp_path
    )
    assert "'id'" in message


def test_p2_3_without_columns_type_any_column_is_accepted_at_load():
    """The existence is then checked at run time (failed check, clear message)."""
    config = SourceConfig.model_validate(_config([{"type": "not_null", "column": "anything"}]))
    assert config.quality.checks[0].column == "anything"


def test_p2_3_range_on_a_declared_non_numeric_column_is_refused(tmp_path):
    message = _error(
        _config(
            [{"type": "range", "column": "item", "min": 0}],
            columns_type={"item": "string", "amount": "double"},
        ),
        tmp_path,
    )
    assert "range(item)" in message
    assert "numérique" in message


def test_p2_3_range_on_declared_numeric_columns_is_accepted():
    columns_type = {"a": "int", "b": "integer", "c": "float", "d": "double"}
    checks = [{"type": "range", "column": c, "min": 0} for c in columns_type]
    config = SourceConfig.model_validate(_config(checks, columns_type=columns_type))
    assert len(config.quality.checks) == 4


@pytest.mark.parametrize(
    "first, second",
    [
        ({"type": "not_null", "column": "id"}, {"type": "not_null", "column": "id"}),
        # Same rule, contradictory severities: still the same check.
        (
            {"type": "not_null", "column": "id"},
            {"type": "not_null", "column": "id", "severity": "warn", "name": "other"},
        ),
        # The combination is a set: [a, b] and [b, a] are the same check.
        ({"type": "unique", "columns": ["a", "b"]}, {"type": "unique", "columns": ["b", "a"]}),
        (
            {"type": "accepted_values", "column": "s", "values": ["x", "y"]},
            {"type": "accepted_values", "column": "s", "values": ["y", "x"]},
        ),
        ({"type": "row_count", "min": 1}, {"type": "row_count", "min": 1, "name": "again"}),
    ],
)
def test_p2_3_duplicate_check_is_refused(tmp_path, first, second):
    message = _error(_config([first, second]), tmp_path)
    assert "quality.checks.1" in message
    assert "identique" in message


def test_p2_3_two_checks_with_the_same_derived_name_need_an_explicit_name(tmp_path):
    checks = [
        {"type": "regex", "column": "code", "pattern": "^[A-Z]"},
        {"type": "regex", "column": "code", "pattern": "[0-9]$"},
    ]
    message = _error(_config(checks), tmp_path)
    assert "regex(code)" in message
    assert "name" in message

    checks[1]["name"] = "code_ends_with_digit"
    config = SourceConfig.model_validate(_config(checks))
    assert [c.check_name for c in config.quality.checks] == ["regex(code)", "code_ends_with_digit"]


def test_p2_3_explicit_name_colliding_with_a_derived_one_is_refused(tmp_path):
    checks = [
        {"type": "not_null", "column": "id"},
        {"type": "row_count", "min": 1, "name": "not_null(id)"},
    ]
    assert "not_null(id)" in _error(_config(checks), tmp_path)


def test_p2_3_quality_errors_are_aggregated_with_the_other_errors_of_the_file(tmp_path):
    """C-6: one load reports everything wrong in the file, quality included."""
    storage = {k: v for k, v in STORAGE.items() if k != "staging_path"}
    config = _config(
        [
            {"type": "range", "column": "amount", "min": 5, "max": 1},
            {"type": "not_null", "column": "idd"},
            {"type": "not_null", "column": "idd"},
        ],
        storage=storage,
        columns_type={"id": "int", "amount": "double"},
        options={"header": True, "infer_schema": True},
    )
    message = _error(config, tmp_path)

    assert "reader.options.infer_schema" in message  # reader
    assert "staging_path" in message  # quality without staging
    assert "min (5) > max (1)" in message  # range bounds
    assert "'idd'" in message  # column not declared
    assert "identique" in message  # duplicate


def test_p2_3_structural_quality_error_is_reported_with_other_field_errors(tmp_path):
    config = _config([{"type": "not_null"}])
    config["source"]["format"] = "jsonl"
    message = _error(config, tmp_path)
    assert "source.format" in message
    assert "quality.checks.0" in message


# ---------------------------------------------------------------------------
# The `quality/` zone is reserved for the check results
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field, value",
    [("raw_path", "quality/"), ("staging_path", "Quality/staging/"), ("raw_path", "QUALITY")],
)
def test_p2_3_quality_zone_is_reserved(tmp_path, field, value):
    storage = {**STORAGE, field: value}
    message = _error(_config(None, storage=storage), tmp_path)
    assert f"storage.{field}" in message
    assert "réservé" in message


def test_p2_3_zone_merely_starting_with_quality_is_accepted():
    storage = {**STORAGE, "raw_path": "quality_raw/", "staging_path": "zones/quality/"}
    config = SourceConfig.model_validate(_config(None, storage=storage))
    assert config.raw_target == "quality_raw/orders"


def test_p2_3_quality_tables_differing_only_by_case_are_refused(tmp_path):
    def _source(name, table, zone):
        storage = {
            "table_name": table,
            "format": "parquet",
            "mode": "overwrite",
            "raw_path": f"raw_{zone}/",
            "staging_path": f"staging_{zone}/",
        }
        config = _config([{"type": "row_count", "min": 1}], storage=storage)
        config["source"].update(name=name, patterns=[f"{name}_*.csv"])
        return config

    root = tmp_path / "config"
    for name, table, zone in (("a", "Orders", "a"), ("b", "orders", "b")):
        path = root / "sources" / f"{name}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(_source(name, table, zone)))

    with pytest.raises(ConfigError) as exc_info:
        SourceRegistry.from_directory(root)
    assert "quality/Orders" in str(exc_info.value)
    assert "quality/orders" in str(exc_info.value)


# ---------------------------------------------------------------------------
# The repository configs (conservative examples, to be adjusted)
# ---------------------------------------------------------------------------


def test_p2_3_repo_imdb_checks_id_not_null_and_unique_as_warnings():
    """C-5: introduced as `warn` for a first real run (the real IMDB file may
    hold duplicated ids); to be raised to `error` once quality/movies has been
    reviewed."""
    imdb = load_source_config(REPO_CONFIG_ROOT / "sources" / "imdb.yaml")
    assert [(c.check_name, c.severity) for c in imdb.quality.checks] == [
        ("not_null(id)", "warn"),
        ("unique(id)", "warn"),
    ]
    assert imdb.quality_target == "quality/movies"


def test_p2_3_repo_orders_warns_on_empty_file_only():
    orders = load_source_config(REPO_CONFIG_ROOT / "sources" / "orders.yaml")
    (check,) = orders.quality.checks
    assert (check.type, check.min, check.max, check.severity) == ("row_count", 1, None, "warn")


@pytest.mark.parametrize("config_file", sorted((REPO_CONFIG_ROOT / "defaults").glob("*.yaml")))
def test_p2_3_repo_defaults_declare_no_checks(config_file):
    """Defaults have no staging zone, hence no quality checks."""
    assert load_source_config(config_file).quality is None


# ---------------------------------------------------------------------------
# C-1: bounds Spark can compare (int64, finite numbers)
# ---------------------------------------------------------------------------

INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1


@pytest.mark.parametrize(
    "check",
    [
        {"type": "range", "column": "amount", "max": 99999999999999999999},
        {"type": "range", "column": "amount", "min": INT64_MIN - 1},
        {"type": "range", "column": "amount", "max": INT64_MAX + 1},
        {"type": "row_count", "max": INT64_MAX + 1},
    ],
)
def test_p2_3_bound_outside_int64_is_refused_at_load(tmp_path, check):
    """C-1: such a bound used to slip through as a float and crash `F.lit`
    (Py4J NumberFormatException) at run time."""
    message = _error(_config([check]), tmp_path)
    assert "quality.checks.0" in message
    assert "int64" in message


def test_p2_3_int64_limits_are_accepted():
    checks = [
        {"type": "range", "column": "amount", "min": INT64_MIN, "max": INT64_MAX},
        {"type": "row_count", "max": INT64_MAX},
    ]
    range_check, row_count = SourceConfig.model_validate(_config(checks)).quality.checks
    assert (range_check.min, range_check.max, row_count.max) == (INT64_MIN, INT64_MAX, INT64_MAX)
    assert type(range_check.max) is int


@pytest.mark.parametrize("bound", [".nan", ".inf", "-.inf"])
@pytest.mark.parametrize("field", ["min", "max"])
def test_p2_3_nan_and_infinite_bounds_are_refused(tmp_path, field, bound):
    path = tmp_path / "sources" / "bad.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(
        yaml.safe_dump(_config(None), sort_keys=False)
        + f"quality:\n  checks:\n    - {{type: range, column: amount, {field}: {bound}}}\n"
    )
    with pytest.raises(ConfigError) as exc_info:
        load_source_config(path)
    assert f"quality.checks.0.range.{field}" in str(exc_info.value)


# ---------------------------------------------------------------------------
# C-3: accepted_values are text only (YAML 1.1 conversions)
# ---------------------------------------------------------------------------


def _load_yaml_text(tmp_path: Path, quality_yaml: str):
    path = tmp_path / "sources" / "orders.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(_config(None), sort_keys=False) + quality_yaml)
    return load_source_config(path)


@pytest.mark.parametrize("values", ["[FR, NO, SE]", "[yes]", "[on, off]", "['7', 007]", "[1:30]"])
def test_p2_3_unquoted_yaml_scalars_in_accepted_values_are_refused(tmp_path, values):
    """PyYAML (YAML 1.1) reads NO/yes/on/off as booleans, 007 as 7, 0123 as
    83 (octal), 1:30 as 90: the check would compare the wrong values."""
    with pytest.raises(ConfigError) as exc_info:
        _load_yaml_text(
            tmp_path,
            "quality:\n  checks:\n"
            f"    - {{type: accepted_values, column: country, values: {values}}}\n",
        )
    message = str(exc_info.value)
    assert "quality.checks.0.accepted_values.values" in message
    assert "guillemets" in message
    assert "YAML 1.1" in message


def test_p2_3_quoted_yaml_scalars_in_accepted_values_are_kept_as_written(tmp_path):
    config = _load_yaml_text(
        tmp_path,
        "quality:\n  checks:\n"
        "    - {type: accepted_values, column: country, values: ['FR', 'NO', \"yes\", '007']}\n",
    )
    assert config.quality.checks[0].values == ["FR", "NO", "yes", "007"]


@pytest.mark.parametrize("values", [[1], [True], [1.5], [None]])
def test_p2_3_non_text_accepted_values_are_refused(tmp_path, values):
    message = _error(
        _config([{"type": "accepted_values", "column": "s", "values": values}]), tmp_path
    )
    assert "guillemets" in message


@pytest.mark.parametrize("declared", ["double", "float", "DOUBLE"])
def test_p2_3_accepted_values_on_a_declared_fractional_column_is_refused(tmp_path, declared):
    """The text form of a double is `1.0` (`1.00` for a decimal): `values: ["1"]`
    would reject every row. `range` is the check for numbers."""
    message = _error(
        _config(
            [{"type": "accepted_values", "column": "amount", "values": ["1"]}],
            columns_type={"amount": declared},
        ),
        tmp_path,
    )
    assert "accepted_values(amount)" in message
    assert "range" in message


def test_p2_3_accepted_values_on_a_declared_integer_or_text_column_is_accepted():
    checks = [
        {"type": "accepted_values", "column": "id", "values": ["1", "2"]},
        {"type": "accepted_values", "column": "flag", "values": ["true"]},
        {"type": "accepted_values", "column": "status", "values": ["NO"]},
    ]
    columns_type = {"id": "int", "flag": "boolean", "status": "string"}
    config = SourceConfig.model_validate(_config(checks, columns_type=columns_type))
    assert len(config.quality.checks) == 3


# ---------------------------------------------------------------------------
# C-7: a shape error in `quality` does not hide the other sections' errors
# ---------------------------------------------------------------------------


def test_p2_3_quality_shape_error_does_not_hide_reader_option_errors(tmp_path):
    config = _config([{"type": "not_null"}], options={"header": True, "infer_schema": True})
    message = _error(config, tmp_path)
    assert "quality.checks.0.not_null.column" in message  # shape (field) error
    assert "reader.options.infer_schema" in message  # cross-section error


def test_p2_3_quality_shape_error_does_not_hide_a_missing_storage(tmp_path):
    config = _config([{"type": "not_null"}])
    del config["storage"]
    message = _error(config, tmp_path)
    assert "quality.checks.0" in message
    assert "storage: section obligatoire" in message


def test_p2_3_field_error_elsewhere_does_not_hide_quality_semantic_errors(tmp_path):
    config = _config([{"type": "range", "column": "amount", "min": 5, "max": 1}])
    config["source"]["format"] = "jsonl"
    message = _error(config, tmp_path)
    assert "source.format" in message
    assert "min (5) > max (1)" in message


def test_p2_3_python_regex_advice_mentions_java_only_syntax(tmp_path):
    message = _error(
        _config([{"type": "regex", "column": "code", "pattern": r"\Qa.b\E"}]), tmp_path
    )
    assert "regex invalide" in message
    assert r"\Q...\E" in message
    assert r"\e" in message
