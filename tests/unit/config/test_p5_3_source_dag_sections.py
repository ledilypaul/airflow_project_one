"""
P5-3 — what a source YAML says about its DAG: `listing`, `schedule`, `retries`, `tags`,
the identifier `source.name`, the registry that keeps the valid configs when others are
not, and `pipeline.yaml` (`default_directories`). Pure: no Airflow, no Spark.
"""

from pathlib import Path

import pytest
import yaml

from pipeline.config import ConfigError, SourceConfig, SourceRegistry, default_directories
from pipeline.config.registry import ENGINE_PIPELINE_FILE, load_source_config

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent


def _config(**extra) -> dict:
    return {
        "source": {"name": "orders", "format": "csv", "patterns": ["orders_*.csv"]},
        "storage": {
            "format": "parquet",
            "mode": "overwrite",
            "raw_path": "raw/",
            "staging_path": "staging/",
        },
        **extra,
    }


def _problem(config: dict) -> str:
    with pytest.raises(ValueError) as raised:
        SourceConfig.model_validate(config)
    return str(raised.value)


def test_p5_3_the_new_sections_are_optional_with_neutral_defaults():
    config = SourceConfig.model_validate(_config())
    assert config.listing.directories is None
    assert config.listing.exclude == []
    assert (config.schedule, config.retries, config.tags) == (None, None, [])


def test_p5_3_the_full_dag_sections_validate():
    config = SourceConfig.model_validate(
        _config(
            listing={"directories": ["./data/", "/mnt/in"], "exclude": ["*_OLD.csv"]},
            schedule="0 6 * * *",
            retries=2,
            tags=["finance", " daily "],
        )
    )
    assert config.listing.directories == ["./data/", "/mnt/in"]
    assert config.listing.exclude == ["*_OLD.csv"]
    assert config.schedule == "0 6 * * *"
    assert config.retries == 2
    assert config.tags == ["finance", "daily"]


@pytest.mark.parametrize(
    "schedule", ["0 6 * * *", "*/15 * * * *", "0 0 1 jan *", "@daily", "@hourly", "@once", None]
)
def test_p5_3_schedule_accepts_cron_presets_and_null(schedule):
    assert SourceConfig.model_validate(_config(schedule=schedule)).schedule == schedule


@pytest.mark.parametrize(
    "schedule", ["", "every day", "0 6 * *", "0 6 * * * *", "@continuous", "@sometimes", 5]
)
def test_p5_3_schedule_refuses_anything_else(schedule):
    assert "schedule" in _problem(_config(schedule=schedule))


@pytest.mark.parametrize(
    ("section", "value"),
    [
        ("retries", -1),
        ("retries", 11),
        ("retries", "two"),
        ("tags", "finance"),
        ("tags", [""]),
        ("tags", ["x" * 101]),
        ("listing", {"directories": []}),
        ("listing", {"directories": [""]}),
        ("listing", {"exclude": ["a/b.csv"]}),
        ("listing", {"dirs": ["./x"]}),
        ("listin", {"directories": ["./x"]}),
    ],
)
def test_p5_3_invalid_dag_sections_are_refused(section, value):
    assert section in _problem(_config(**{section: value}))


@pytest.mark.parametrize("name", ["orders", "imdb_movies", "v2", "a_b_c", "0"])
def test_p5_3_source_name_is_an_identifier(name):
    config = _config()
    config["source"]["name"] = name
    assert SourceConfig.model_validate(config).source.name == name


@pytest.mark.parametrize(
    "name", ["Orders", "a-b", "a b", "a.b", "../x", "é", "x" * 101, "unmatched"]
)
def test_p5_3_source_name_refuses_what_cannot_name_a_dag(name):
    config = _config()
    config["source"]["name"] = name
    assert "source.name" in _problem(config)


def test_p5_3_staging_asset_name():
    assert SourceConfig.model_validate(_config()).staging_asset_name == "lake_staging__orders"
    raw_only = _config()
    del raw_only["storage"]["staging_path"]
    assert SourceConfig.model_validate(raw_only).staging_asset_name is None


# ---------------------------------------------------------------------------
# The registry keeps the configs that are valid
# ---------------------------------------------------------------------------


def _root(tmp_path: Path, **sources) -> Path:
    root = tmp_path / "root"
    (root / "sources").mkdir(parents=True)
    for stem, content in sources.items():
        text = content if isinstance(content, str) else yaml.safe_dump(content)
        (root / "sources" / f"{stem}.yaml").write_text(text)
    return root


def test_p5_3_load_valid_returns_the_valid_configs_and_the_message_of_the_others(tmp_path):
    root = _root(tmp_path, a=_config(), broken="source: [x\n", typo={**_config(), "oops": 1})

    registry, invalid = SourceRegistry.load_valid(root)

    assert [loaded.name for loaded in registry.sources] == ["orders"]
    assert {path.stem for path in invalid} == {"broken", "typo"}
    assert "oops" in next(m for p, m in invalid.items() if p.stem == "typo")
    # The strict loader still refuses the whole root (the job's exit 5).
    with pytest.raises(ConfigError):
        SourceRegistry.from_directory(root)


def test_p5_3_load_valid_keeps_the_first_of_two_sources_with_the_same_name(tmp_path):
    registry, invalid = SourceRegistry.load_valid(_root(tmp_path, a=_config(), b=_config()))

    assert [loaded.path.stem for loaded in registry.sources] == ["a"]
    assert [p.stem for p in invalid] == ["b"]
    assert "déclaré deux fois" in next(iter(invalid.values()))


def test_p5_3_load_valid_drops_the_source_whose_tables_nest_in_a_kept_one(tmp_path):
    inner = _config()
    inner["source"]["name"] = "deep"
    inner["storage"]["raw_path"] = "raw/orders/"
    registry, invalid = SourceRegistry.load_valid(_root(tmp_path, a=_config(), b=inner))

    assert [loaded.name for loaded in registry.sources] == ["orders"]
    assert "imbriquées" in next(iter(invalid.values()))


def test_p5_3_load_valid_of_a_clean_root_has_no_invalid_config():
    registry, invalid = SourceRegistry.load_valid(PROJECT_ROOT / "examples" / "config")

    assert invalid == {}
    assert sorted(loaded.name for loaded in registry.sources) == ["imdb_movies", "orders"]
    assert {loaded.name for loaded in registry.defaults} >= {"default_csv", "default_parquet"}


def test_p5_3_load_valid_needs_a_root(tmp_path):
    with pytest.raises(ConfigError, match="introuvable"):
        SourceRegistry.load_valid(tmp_path / "nope")


# ---------------------------------------------------------------------------
# pipeline.yaml
# ---------------------------------------------------------------------------


def test_p5_3_the_engine_ships_its_default_directories():
    assert yaml.safe_load(ENGINE_PIPELINE_FILE.read_text()) == {"default_directories": ["./data/"]}
    assert default_directories(PROJECT_ROOT / "examples" / "config") == ["./data/"]


def test_p5_3_a_config_root_can_bring_its_own_pipeline_yaml(tmp_path):
    root = _root(tmp_path)
    (root / "pipeline.yaml").write_text("default_directories: [' /mnt/a ', /mnt/b]\n")
    assert default_directories(root) == ["/mnt/a", "/mnt/b"]


@pytest.mark.parametrize(
    "content", ["", "{}", "default_directories: []", "default_directories: [1]", "[a]", "x: ["]
)
def test_p5_3_pipeline_yaml_must_list_directories(tmp_path, content):
    root = _root(tmp_path)
    (root / "pipeline.yaml").write_text(content)
    with pytest.raises(ConfigError, match="pipeline.yaml"):
        default_directories(root)


def test_p5_3_the_dag_sections_are_read_from_a_file(tmp_path):
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(_config(schedule="@daily", retries=3)))
    config = load_source_config(path)
    assert (config.schedule, config.retries) == ("@daily", 3)
