"""P5-1: the engine knows no source. IMDB and orders live in `examples/`."""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
ENGINE_DIRS = ("pipeline", "storage", "utils", "dags", "scripts")
FORBIDDEN = re.compile(r"imdb|movies|orders", re.IGNORECASE)


def test_p5_1_engine_code_names_no_example_source():
    hits = [
        f"{path.relative_to(ROOT)}:{n}"
        for directory in ENGINE_DIRS
        for path in (ROOT / directory).rglob("*")
        if path.is_file() and path.suffix in {".py", ".sql", ".sh", ".yaml", ".json"}
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if FORBIDDEN.search(line)
    ]
    assert not hits, hits


def test_p5_1_repo_config_has_no_dedicated_source():
    sources = [p.name for p in (ROOT / "config" / "sources").glob("*.y*ml")]
    assert sources == []
    assert {p.name for p in (ROOT / "examples" / "config" / "sources").glob("*.yaml")} == {
        "imdb.yaml",
        "orders.yaml",
    }


def test_p5_1_example_samples_are_tracked_by_git():
    # `data/` of .gitignore once hid them: green locally, missing from every clone.
    tracked = subprocess.run(
        ["git", "ls-files", "examples/data"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    assert sorted(tracked) == [
        "examples/data/imdb_sample.csv",
        "examples/data/orders_sample.csv",
    ]
