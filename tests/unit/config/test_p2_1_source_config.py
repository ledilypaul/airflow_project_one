"""
P2-1 (docs/audit/2026-11-05-baseline.md, constat M4): one validated config
shape for every reading source, and an explicit, deterministic file -> config
mapping.

- Every YAML of `config/sources/` and `config/defaults/` loads and validates
  (parametrized on the files actually present: a new YAML is covered
  automatically).
- An unknown key, an option outside the per-format whitelist, an unsupported
  column type or `mode: append` is rejected, with a message naming the YAML
  file and the faulty field.
- Matching: dedicated source over default, case-insensitive, ambiguity and
  no match are explicit errors, `extractor_config.yaml` is never scanned.
"""

import inspect
from pathlib import Path
from typing import get_args

import pyspark
import pytest
import yaml

from pipeline.config import (
    DEFAULT_CONFIG_ROOT,
    READER_OPTIONS_BY_FORMAT,
    AmbiguousConfigError,
    ConfigError,
    FileFormat,
    NoMatchingConfigError,
    SourceConfig,
    SourceRegistry,
    load_source_config,
)
from pipeline.config.reader_options import (
    ALIAS_GROUPS,
    CSV_READ_OPTIONS,
    PARQUET_READ_OPTIONS,
    SPARK_OPTION_TYPES,
)
from pipeline.config.types import build_schema
from pipeline.file_reader.reader_factory import READER_CLASSES

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
REPO_CONFIG_ROOT = PROJECT_ROOT / "config"
REPO_SOURCE_FILES = sorted((REPO_CONFIG_ROOT / "sources").glob("*.yaml"))
REPO_DEFAULT_FILES = sorted((REPO_CONFIG_ROOT / "defaults").glob("*.yaml"))


def _config(
    name="orders",
    fmt="csv",
    patterns=("orders_*",),
    options=None,
    columns_type=None,
    storage=None,
) -> dict:
    config = {
        "source": {"name": name, "format": fmt, "patterns": list(patterns)},
        "reader": {"options": options if options is not None else {"header": True}},
    }
    if columns_type is not None:
        config["reader"]["columns_type"] = columns_type
    if storage is not None:
        config["storage"] = storage
    return config


def _write(path: Path, config: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, sort_keys=False))
    return path


# ---------------------------------------------------------------------------
# The real YAML files of the repository
# ---------------------------------------------------------------------------


def test_p2_1_repo_has_dedicated_sources_and_defaults():
    assert REPO_SOURCE_FILES, f"no dedicated source config in {REPO_CONFIG_ROOT / 'sources'}"
    assert REPO_DEFAULT_FILES, f"no default config in {REPO_CONFIG_ROOT / 'defaults'}"


@pytest.mark.parametrize(
    "config_file",
    REPO_SOURCE_FILES + REPO_DEFAULT_FILES,
    ids=lambda p: f"{p.parent.name}/{p.name}",
)
def test_p2_1_every_reading_config_loads_and_validates(config_file):
    config = load_source_config(config_file)

    assert isinstance(config, SourceConfig)
    assert config.source.patterns
    # The explicit schema of every config builds (B2 still holds on the new shape).
    if config.reader.columns_type:
        schema = build_schema(config.reader.columns_type)
        assert [f.name for f in schema.fields] == list(config.reader.columns_type)
    # Idempotence: whatever declares a storage section overwrites.
    if config.storage is not None:
        assert config.storage.mode == "overwrite"


def test_p2_1_default_config_root_is_resolved_from_the_code_not_the_cwd(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    assert DEFAULT_CONFIG_ROOT == REPO_CONFIG_ROOT
    registry = SourceRegistry.from_directory(DEFAULT_CONFIG_ROOT)
    assert registry.match("orders_2024.csv").source.name == "orders"


# The 42 columns of the IMDB CSV, in the file's order (that of the former
# config/imdb_config.yaml): the explicit schema is positional, a reordered
# key would silently put the values of one column under the name of another.
IMDB_COLUMNS = [
    "id",
    "title",
    "vote_average",
    "vote_count",
    "status",
    "release_date",
    "revenue",
    "runtime",
    "adult",
    "backdrop_path",
    "budget",
    "homepage",
    "imdb_db",
    "original_language",
    "original_title",
    "overview",
    "popularity",
    "poster_path",
    "tagline",
    "production_companies",
    "production_countries",
    "spoken_languages",
    "keywords",
    "release_year",
    "Director",
    "AverageRating",
    "Poster_Link",
    "Certificate",
    "IMDB_Rating",
    "Metascore",
    "Star1",
    "Star2",
    "Star3",
    "Star4",
    "Writer",
    "Director_of_Photography",
    "Producer",
    "Music_Composer",
    "genre_list",
    "Cast_list",
    "overview_sentiment",
    "all_combined_keywords",
]


def test_p2_1_migrated_configs_keep_their_information():
    """The migration of the old YAML files lost nothing that mattered."""
    imdb = load_source_config(REPO_CONFIG_ROOT / "sources" / "imdb.yaml")
    assert imdb.source.patterns == ["*imdb*.csv"]
    assert imdb.reader.options["encoding"] == "UTF-8"
    assert list(imdb.reader.columns_type) == IMDB_COLUMNS
    # C-1: the CSV source cannot read an ARRAY column; split it later.
    assert imdb.reader.columns_type["genre_list"] == "string"
    assert imdb.storage.table_name == "movies"

    orders = load_source_config(REPO_CONFIG_ROOT / "sources" / "orders.yaml")
    assert orders.source.patterns == ["orders_*.csv"]
    assert orders.reader.options == {
        "header": True,
        "inferSchema": True,
        "delimiter": ",",
        "encoding": "UTF-8",
    }

    txt = load_source_config(REPO_CONFIG_ROOT / "defaults" / "txt.yaml")
    # M4: the old `infer_schema` typo is gone, the effective behaviour is kept.
    assert txt.reader.options["inferSchema"] is True
    assert "infer_schema" not in txt.reader.options


@pytest.mark.parametrize(
    ("file_name", "expected_source"),
    [
        ("IMDB_movies.csv", "imdb_movies"),
        ("top_imdb_2024.csv", "imdb_movies"),
        # A dedicated config describes a CSV: an IMDB or orders file in
        # another format falls back on its extension default, never read as CSV.
        ("IMDB_movies.parquet", "default_parquet"),
        ("imdb_ratings.xlsx", "default_xlsx"),
        ("orders_2024.parquet", "default_parquet"),
        ("orders_2024.csv", "orders"),
        ("random_data.csv", "default_csv"),
        ("notes.TXT", "default_txt"),
        ("report.xlsx", "default_xlsx"),
        ("events.parquet", "default_parquet"),
        ("/data/in/orders_1.csv", "orders"),
        ("file:///data/in/orders_1.csv", "orders"),
        # The pattern applies to the file name, not to its directories.
        ("/data/orders_archive/random.csv", "default_csv"),
        # M4: "extractor" in the name never selects extractor_config.yaml.
        ("extractor_dump.csv", "default_csv"),
    ],
)
def test_p2_1_real_registry_matching(file_name, expected_source):
    registry = SourceRegistry.from_directory(REPO_CONFIG_ROOT)
    assert registry.match(file_name).source.name == expected_source


def test_p2_1_real_registry_has_no_match_for_unknown_extension():
    """The old `default_format: csv` fallback is gone: an unknown extension is
    an explicit error (a `failed` row in file_list), not a CSV read."""
    registry = SourceRegistry.from_directory(REPO_CONFIG_ROOT)
    with pytest.raises(NoMatchingConfigError, match="notes.json"):
        registry.match("notes.json")


# ---------------------------------------------------------------------------
# Validation errors name the YAML file and the field
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("config", "field_path", "detail"),
    [
        pytest.param(
            {**_config(), "reader": {"reader_options": {"header": True}}},
            "reader.reader_options",
            "Extra inputs are not permitted",
            id="unknown-key",
        ),
        pytest.param(
            _config(fmt="txt", patterns=["*.txt"], options={"infer_schema": True}),
            "reader.options.infer_schema",
            "inferSchema",
            id="infer_schema-typo",
        ),
        pytest.param(
            _config(fmt="xlsx", patterns=["*.xlsx"], options={"inferSchema": True}),
            "reader.options.inferSchema",
            "xlsx",
            id="spark-option-on-excel",
        ),
        pytest.param(
            _config(fmt="parquet", patterns=["*.parquet"], options={"header": True}),
            "reader.options.header",
            "parquet",
            id="csv-option-on-parquet",
        ),
        pytest.param(
            _config(options={"delimiter": [",", ";"]}),
            "reader.options.delimiter",
            "scalaire",
            id="non-scalar-spark-option",
        ),
        pytest.param(
            _config(columns_type={"id": "int", "amount": "decimal"}),
            "reader.columns_type.amount",
            "decimal",
            id="unsupported-column-type",
        ),
        pytest.param(
            _config(fmt="parquet", patterns=["*.parquet"], options={}, columns_type={"id": "int"}),
            "reader.columns_type",
            "csv",
            id="columns_type-on-parquet",
        ),
        pytest.param(
            _config(
                storage={
                    "format": "parquet",
                    "mode": "append",
                    "raw_path": "raw/",
                    "staging_path": "staging/",
                }
            ),
            "storage.mode",
            "overwrite",
            id="append-mode",
        ),
        pytest.param(
            _config(fmt="json", patterns=["*.json"]),
            "source.format",
            "csv",
            id="unsupported-format",
        ),
        pytest.param(
            _config(columns_type={"id": "int", "genres": "array"}),
            "reader.columns_type.genres",
            "atomiques",
            id="array-column-type",
        ),
        pytest.param(
            _config(fmt="xls", patterns=["*.xls"], options={}),
            "source.format",
            "xlsx",
            id="xls-format-without-xlrd",
        ),
        pytest.param(
            _config(options={"header": True, "sep": ";", "delimiter": ","}),
            "reader.options",
            "delimiter",
            id="sep-and-delimiter",
        ),
        pytest.param(
            _config(options={"encoding": "UTF-8", "charset": "ISO-8859-1"}),
            "reader.options",
            "charset",
            id="encoding-and-charset",
        ),
        pytest.param(
            _config(options={"header": "yes"}),
            "reader.options.header",
            "booléen",
            id="header-yes-string",
        ),
        pytest.param(
            _config(options={"inferSchema": 1}),
            "reader.options.inferSchema",
            "booléen",
            id="inferSchema-int",
        ),
        pytest.param(
            _config(options={"maxColumns": "100"}),
            "reader.options.maxColumns",
            "entier",
            id="maxColumns-string",
        ),
        pytest.param(
            _config(options={"maxCharsPerColumn": True}),
            "reader.options.maxCharsPerColumn",
            "entier",
            id="maxCharsPerColumn-bool",
        ),
        pytest.param(
            _config(options={"samplingRatio": "0.5"}),
            "reader.options.samplingRatio",
            "nombre",
            id="samplingRatio-string",
        ),
        pytest.param(
            _config(fmt="parquet", patterns=["*.parquet"], options={"mergeSchema": "true"}),
            "reader.options.mergeSchema",
            "booléen",
            id="mergeSchema-string",
        ),
        pytest.param(
            _config(patterns=["data/orders_*"]),
            "source.patterns",
            "nom du fichier",
            id="pattern-with-directory",
        ),
        pytest.param(
            _config(patterns=[]),
            "source.patterns",
            "",
            id="no-pattern",
        ),
    ],
)
def test_p2_1_invalid_config_is_rejected_with_file_and_field(tmp_path, config, field_path, detail):
    config_file = _write(tmp_path / "bad_source.yaml", config)

    with pytest.raises(ConfigError) as exc_info:
        load_source_config(config_file)

    message = str(exc_info.value)
    assert str(config_file) in message
    assert field_path in message
    assert detail in message


def test_p2_1_yaml_that_is_not_a_mapping_names_the_file(tmp_path):
    config_file = tmp_path / "list.yaml"
    config_file.write_text("- a\n- b\n")
    with pytest.raises(ConfigError, match="list.yaml"):
        load_source_config(config_file)


def test_p2_1_whitelists_cover_exactly_the_supported_formats():
    formats = set(get_args(FileFormat))
    assert set(READER_OPTIONS_BY_FORMAT) == formats
    # ReaderFactory and the model agree on the formats (no format the model
    # accepts but no reader can read, and conversely).
    assert set(READER_CLASSES) == formats


def test_p2_1_excel_whitelist_matches_the_installed_read_excel():
    """The Excel whitelist only holds real `pyspark.pandas.read_excel`
    parameters of the installed version (no `**kwds` passthrough)."""
    import pyspark.pandas as ps

    params = set(inspect.signature(ps.read_excel).parameters) - {"io", "kwds"}
    assert READER_OPTIONS_BY_FORMAT["xlsx"] <= params


def test_p2_1_csv_whitelist_matches_the_installed_spark_reader():
    """Every CSV option is a keyword of the installed `DataFrameReader.csv`,
    or one of the read options documented by Spark 4.3.0 without a keyword
    (aliases, newer options, generic file source options)."""
    from pyspark.sql.readwriter import DataFrameReader

    keywords = set(inspect.signature(DataFrameReader.csv).parameters) - {"self", "path", "schema"}
    documented_without_keyword = {
        "delimiter",
        "charset",
        "preferDate",
        "timestampNTZFormat",
        "timeFormat",
        "enableDateTimeParsingFallback",
        "singleVariantColumn",
        "timeZone",
        "ignoreCorruptFiles",
        "ignoreMissingFiles",
        "ignoredPathSegmentRegex",
        # Not in the option table of the doc: CSVOptions.COLUMN_PRUNING, read
        # with getBool (sql/catalyst/.../csv/CSVOptions.scala, Spark 4.3.0 jar).
        "columnPruning",
        # Partition discovery of every file source (FileIndexOptions.BASE_PATH_PARAM),
        # documented in sql-data-sources-parquet.html#partition-discovery.
        "basePath",
    }
    assert READER_OPTIONS_BY_FORMAT["csv"] <= keywords | documented_without_keyword
    assert "infer_schema" not in READER_OPTIONS_BY_FORMAT["csv"]


def test_p2_1_parquet_whitelist_matches_the_spark_documentation():
    """C-4: `DataFrameReader.parquet` has no keyword per option (only
    `**options`), so the Parquet whitelist is compared to the documentation
    of the installed version instead:
    - option table of https://spark.apache.org/docs/4.3.0/sql-data-sources-parquet.html
      (scope read; `compression` is write-only);
    - `basePath`, section "Partition Discovery" of the same page;
    - https://spark.apache.org/docs/4.3.0/sql-data-sources-generic-options.html.
    If a PySpark upgrade adds keywords to `parquet()`, compare to them."""
    from pyspark.sql.readwriter import DataFrameReader

    parameters = inspect.signature(DataFrameReader.parquet).parameters
    assert set(parameters) == {"self", "paths", "options"}, (
        "DataFrameReader.parquet now has named keywords: compare PARQUET_READ_OPTIONS to them"
    )

    parquet_table_read_scope = {"datetimeRebaseMode", "int96RebaseMode", "mergeSchema"}
    partition_discovery = {"basePath"}
    generic_file_source_options = {
        "ignoreCorruptFiles",
        "ignoreMissingFiles",
        "pathGlobFilter",
        "ignoredPathSegmentRegex",
        "recursiveFileLookup",
        "modifiedBefore",
        "modifiedAfter",
    }
    assert PARQUET_READ_OPTIONS == (
        parquet_table_read_scope | partition_discovery | generic_file_source_options
    )
    # The generic options that do have a keyword on csv() (same file source
    # code path) confirm the names.
    csv_keywords = set(inspect.signature(DataFrameReader.csv).parameters)
    assert {"pathGlobFilter", "recursiveFileLookup", "modifiedBefore", "modifiedAfter"} <= (
        csv_keywords
    )


def test_p2_1_pyspark_version_is_the_one_the_whitelists_were_checked_against():
    """C-4: the whitelists of reader_options.py (and SPARK_OPTION_TYPES) were
    checked against PySpark 4.3 (signatures, documentation, jars)."""
    assert pyspark.__version__.split(".")[:2] == ["4", "3"], (
        f"montée de version de PySpark ({pyspark.__version__}) : revoir les listes "
        "blanches de pipeline/config/reader_options.py (et SPARK_OPTION_TYPES) "
        "contre la documentation de la nouvelle version, puis mettre à jour ce test"
    )


def test_p2_1_column_pruning_and_base_path_are_accepted():
    """C-3: real read options of Spark 4.3.0 missing from the whitelists."""
    csv = SourceConfig.model_validate(
        _config(options={"header": True, "columnPruning": False, "basePath": "/data/in"})
    )
    assert csv.reader.options["columnPruning"] is False
    parquet = SourceConfig.model_validate(
        _config(fmt="parquet", patterns=["*.parquet"], options={"basePath": "/data/in"})
    )
    assert parquet.reader.options["basePath"] == "/data/in"


def test_p2_1_alias_groups_are_csv_options():
    """C-3: `sep`/`delimiter` and `encoding`/`charset` are two names of one
    Spark option each; declaring both is refused (Spark keeps one silently)."""
    assert {frozenset(group) for group in ALIAS_GROUPS} == {
        frozenset({"sep", "delimiter"}),
        frozenset({"encoding", "charset"}),
    }
    for group in ALIAS_GROUPS:
        assert group <= CSV_READ_OPTIONS


def test_p2_1_typed_options_match_the_installed_spark_reader():
    """C-3: every typed option is in a Spark whitelist, and the type agrees
    with the annotation of the installed `DataFrameReader.csv` when it has a
    keyword there. `pathGlobFilter`, `modifiedBefore` and `modifiedAfter` are
    annotated `Union[bool, str]` upstream but are strings (a glob, timestamps):
    they are left untyped."""
    from pyspark.sql.readwriter import DataFrameReader

    assert set(SPARK_OPTION_TYPES) <= CSV_READ_OPTIONS | PARQUET_READ_OPTIONS
    mis_annotated_upstream = {"pathGlobFilter", "modifiedBefore", "modifiedAfter"}
    for name, parameter in inspect.signature(DataFrameReader.csv).parameters.items():
        annotation = str(parameter.annotation)
        if name in mis_annotated_upstream or name not in CSV_READ_OPTIONS:
            continue
        if "bool" in annotation:
            assert SPARK_OPTION_TYPES.get(name) is bool, name
        elif "int" in annotation:
            assert SPARK_OPTION_TYPES.get(name) is int, name
        elif "float" in annotation:
            assert SPARK_OPTION_TYPES.get(name) is float, name
        else:
            assert name not in SPARK_OPTION_TYPES, name


def test_p2_1_typed_options_accept_well_typed_values():
    config = SourceConfig.model_validate(
        _config(
            options={
                "header": False,
                "maxColumns": 100,
                "samplingRatio": 1,
                "multiLine": True,
            }
        )
    )
    assert config.reader.options["maxColumns"] == 100


# ---------------------------------------------------------------------------
# Loading a config directory
# ---------------------------------------------------------------------------


@pytest.fixture
def config_root(tmp_path):
    root = tmp_path / "config"
    _write(root / "sources" / "orders.yaml", _config())
    _write(root / "sources" / "imdb.yaml", _config(name="imdb", patterns=["*imdb*"]))
    _write(root / "defaults" / "csv.yaml", _config(name="default_csv", patterns=["*.csv"]))
    # The listing config sits at the root, as in the repository: it is not a
    # reading config and must never be loaded (it would not even validate).
    (root / "extractor_config.yaml").write_text(
        "extractor_config:\n  source_directories: ['./data/']\n"
    )
    return root


def test_p2_1_dedicated_source_wins_over_default(config_root):
    registry = SourceRegistry.from_directory(config_root)
    assert registry.match("orders_2024.csv").source.name == "orders"
    assert registry.match("random.csv").source.name == "default_csv"


def test_p2_1_matching_is_case_insensitive(config_root):
    _write(config_root / "sources" / "shouting.yaml", _config(name="shout", patterns=["LOUD_*"]))
    registry = SourceRegistry.from_directory(config_root)
    assert registry.match("IMDB_movies.csv").source.name == "imdb"
    assert registry.match("ORDERS_2024.CSV").source.name == "orders"
    assert registry.match("loud_file.csv").source.name == "shout"


def test_p2_1_two_dedicated_sources_matching_is_an_error(config_root):
    registry = SourceRegistry.from_directory(config_root)
    with pytest.raises(AmbiguousConfigError) as exc_info:
        registry.match("orders_imdb.csv")
    message = str(exc_info.value)
    assert "orders" in message
    assert "imdb" in message
    assert "orders_imdb.csv" in message


def test_p2_1_two_defaults_matching_is_an_error(config_root):
    _write(config_root / "defaults" / "csv_bis.yaml", _config(name="other_csv", patterns=["*.csv"]))
    registry = SourceRegistry.from_directory(config_root)
    with pytest.raises(AmbiguousConfigError, match="default_csv"):
        registry.match("random.csv")


def test_p2_1_no_match_is_an_error(config_root):
    registry = SourceRegistry.from_directory(config_root)
    with pytest.raises(NoMatchingConfigError, match="notes.parquet"):
        registry.match("notes.parquet")


def test_p2_1_extractor_in_the_name_never_selects_the_extractor_config(config_root):
    registry = SourceRegistry.from_directory(config_root)
    assert registry.match("extractor_report.csv").source.name == "default_csv"
    assert "extractor_config.yaml" not in {path.name for path in registry.config_files}


def test_p2_1_duplicate_source_name_is_rejected_at_load(config_root):
    _write(config_root / "sources" / "orders_copy.yaml", _config(patterns=["orders2_*"]))
    with pytest.raises(ConfigError) as exc_info:
        SourceRegistry.from_directory(config_root)
    message = str(exc_info.value)
    assert "orders.yaml" in message
    assert "orders_copy.yaml" in message


def test_p2_1_one_invalid_file_fails_the_whole_load(config_root):
    """Fail fast: every config is validated at load, before any file is read."""
    _write(
        config_root / "sources" / "broken.yaml",
        _config(name="broken", patterns=["broken_*"], options={"infer_schema": True}),
    )
    with pytest.raises(ConfigError, match=r"broken\.yaml"):
        SourceRegistry.from_directory(config_root)


def test_p2_1_missing_config_root_is_an_error(tmp_path):
    with pytest.raises(ConfigError, match="nowhere"):
        SourceRegistry.from_directory(tmp_path / "nowhere")


def test_p2_1_empty_config_root_is_an_error(tmp_path):
    (tmp_path / "sources").mkdir()
    (tmp_path / "defaults").mkdir()
    with pytest.raises(ConfigError, match="aucune config"):
        SourceRegistry.from_directory(tmp_path)
