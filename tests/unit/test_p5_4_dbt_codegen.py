"""P5-4: the dbt staging layer is generated from the reading configs."""

from pathlib import Path

import pytest
import yaml

from scripts.generate_dbt_staging import (
    MODELS_FILE,
    OUTPUT_DIR,
    SOURCES_FILE,
    config_roots,
    main,
    render,
    staging_tables,
)

ROOT = Path(__file__).resolve().parent.parent.parent


@pytest.fixture(autouse=True)
def _repo_roots(monkeypatch):
    monkeypatch.delenv("PIPELINE_CONFIG_ROOT", raising=False)


def _source(root: Path, file: str, name: str, columns: str, extra: str = "") -> None:
    (root / "sources").mkdir(parents=True, exist_ok=True)
    (root / "sources" / file).write_text(
        f"source: {{name: {name}, format: csv, patterns: ['{name}_*.csv']}}\n"
        f"reader: {{options: {{header: true}}{columns}}}\n"
        "storage: {table_name: t1, format: parquet, mode: overwrite, raw_path: raw/, "
        f"staging_path: staging/}}\n{extra}",
        encoding="utf-8",
    )


def test_p5_4_committed_files_are_up_to_date():
    assert main(["--check"]) == 0


def test_p5_4_source_and_contract_follow_columns_type(tmp_path):
    _source(tmp_path, "a.yaml", "a", ", columns_type: {id: long, Name: string}")
    tables, warnings = staging_tables([tmp_path])
    files = render(tables)
    (table,) = yaml.safe_load(files[SOURCES_FILE])["sources"][0]["tables"]
    (model,) = yaml.safe_load(files[MODELS_FILE])["models"]
    expected = [
        ("id", "BIGINT"),
        ("Name", "VARCHAR"),
        ("lake_file_id", "BIGINT"),
        ("lake_ingested_at", "TIMESTAMP"),
        ("lake_run_id", "VARCHAR"),
        ("lake_source_file", "VARCHAR"),
    ]
    for declared in (table, model):
        assert [(c["name"], c["data_type"]) for c in declared["columns"]] == expected
    assert model["config"]["contract"] == {"enforced": True}
    assert "lake_staging('lake_staging', 't1')" in files["stg_t1__current.sql"]
    assert not warnings


def test_p5_4_generate_false_keeps_the_source_only(tmp_path):
    _source(tmp_path, "a.yaml", "a", ", columns_type: {id: int}", "dbt: {generate: false}\n")
    assert list(render(staging_tables([tmp_path])[0])) == [SOURCES_FILE]


def test_p5_4_source_without_columns_type_is_skipped_with_a_warning(tmp_path):
    _source(tmp_path, "a.yaml", "a", "")
    tables, warnings = staging_tables([tmp_path])
    assert render(tables) == {}
    assert "columns_type" in warnings[0]


@pytest.mark.parametrize(
    "columns",
    [", columns_type: {id: long}", """, columns_type: {id: int, "{{ x }}": int}"""],
    ids=["shared table, other columns", "jinja in a column name"],
)
def test_p5_4_refused_configs(tmp_path, columns):
    from pipeline.config.registry import ConfigError

    _source(tmp_path, "a.yaml", "a", ", columns_type: {id: int}")
    _source(tmp_path, "b.yaml", "b", columns)
    with pytest.raises(ConfigError):
        staging_tables([tmp_path])


def test_p5_4_check_detects_a_change_and_removes_stale_models(tmp_path, monkeypatch):
    root, output = tmp_path / "root", tmp_path / "generated"
    monkeypatch.setenv("PIPELINE_CONFIG_ROOT", str(root))
    assert root.resolve() in config_roots()
    assert main([], output) == 0
    before = (output / SOURCES_FILE).stat().st_mtime_ns
    assert main([], output) == 0
    assert (output / SOURCES_FILE).stat().st_mtime_ns == before  # manifest stays fresh
    _source(root, "a.yaml", "a", ", columns_type: {id: int}")
    assert main(["--check"], output) == 1
    assert main([], output) == 0
    assert (output / "stg_t1__current.sql").is_file()
    (root / "sources" / "a.yaml").unlink()
    assert main([], output) == 0
    assert not (output / "stg_t1__current.sql").exists()
    assert OUTPUT_DIR.is_relative_to(ROOT / "transform" / "models")
