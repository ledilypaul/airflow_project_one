"""
Documentation (Sphinx, `docs/`): guards that keep it true to the code.

The build itself (`make docs`, warnings are errors) runs in the CI `docs` job: the `docs` group
is not installed by `uv sync`. These tests need no Sphinx; they check what drifts silently:

- the `docs` group stays out of `default-groups` (the Docker image runs `uv sync --no-dev`);
- every Makefile target is listed in the commands page, and every `make <target>` the pages
  mention exists;
- the YAML examples of the configuration pages are valid for the real models;
- the CI builds the docs with `-W`, like `make docs`.
"""

import re
import tomllib
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DOCS = PROJECT_ROOT / "docs"
MAKEFILE = (PROJECT_ROOT / "Makefile").read_text()
# the pages written for readers; docs/architecture, audit and tech-watch are dated notes
HAND_WRITTEN = [
    *DOCS.glob("index.md"),
    *DOCS.glob("contribuer.md"),
    *DOCS.glob("glossaire.md"),
    *DOCS.glob("guide/*.md"),
    *DOCS.glob("concepts/*.md"),
    *DOCS.glob("configuration/*.md"),
    *DOCS.glob("exploitation/*.md"),
]


def _make_targets() -> set[str]:
    return set(re.findall(r"^([a-zA-Z0-9_-]+):.*##", MAKEFILE, re.M))


def test_docs_group_is_not_installed_by_default():
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    assert "docs" in pyproject["dependency-groups"]
    assert "docs" not in pyproject["tool"]["uv"]["default-groups"]


def test_docs_makefile_targets_build_with_warnings_as_errors():
    assert {"docs", "docs-serve", "docs-clean"} <= _make_targets()
    recipe = re.search(r"^docs:.*\n(\t.*\n)+", MAKEFILE, re.M).group(0)
    assert "sphinx-build" in recipe and " -W" in recipe
    # only the docs group: the build is static, it needs neither Spark nor Airflow
    assert "--only-group docs" in recipe


def test_docs_ci_job_builds_with_warnings_as_errors():
    from tests.unit.test_p3_4_supply_chain import WORKFLOWS_DIR, _load

    job = _load(WORKFLOWS_DIR / "ci.yml")["jobs"]["docs"]
    assert "schedule" in job["if"]  # the Monday cron is for the security jobs
    assert any(step.get("run") == "make docs" for step in job["steps"])


def test_docs_commands_page_lists_every_makefile_target():
    page = (DOCS / "guide" / "commandes.md").read_text()
    missing = sorted(t for t in _make_targets() if t != "help" and f"`make {t}" not in page)
    assert not missing, f"docs/guide/commandes.md does not list: {missing}"


@pytest.mark.parametrize("page", HAND_WRITTEN, ids=lambda p: str(p.relative_to(DOCS)))
def test_docs_pages_only_mention_make_targets_that_exist(page):
    mentioned = set(re.findall(r"\bmake ([a-z][a-z0-9-]*)", page.read_text()))
    unknown = sorted(mentioned - _make_targets())
    assert not unknown, f"{page.relative_to(DOCS)} mentions unknown make targets: {unknown}"


def _yaml_blocks(page: str) -> list[dict]:
    text = (DOCS / page).read_text()
    return [yaml.safe_load(b) for b in re.findall(r"```yaml\n(.*?)```", text, re.S)]


def test_docs_source_config_example_is_valid():
    from pipeline.config.models import SourceConfig

    full = _yaml_blocks("configuration/sources.md")[0]
    config = SourceConfig.model_validate(full)
    assert config.source.name == "orders" and config.storage.staging_path == "staging/"


def test_docs_quality_example_is_valid():
    from pipeline.config.models import SourceConfig

    checks = _yaml_blocks("configuration/qualite.md")[0]["quality"]
    config = SourceConfig.model_validate(
        {
            "source": {"name": "demo", "format": "csv", "patterns": ["demo_*.csv"]},
            "reader": {"options": {"header": True}},
            "storage": {
                "format": "parquet",
                "mode": "overwrite",
                "raw_path": "raw/",
                "staging_path": "staging/",
            },
            "quality": checks,
        }
    )
    assert len(config.quality.checks) == 5
