"""P3-4 - scripts/openlineage_jar_watch.py: pinned version vs. Maven Central, offline."""

import importlib.util
import json
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SCRIPT = PROJECT_ROOT / "scripts" / "openlineage_jar_watch.py"

spec = importlib.util.spec_from_file_location("openlineage_jar_watch", SCRIPT)
watch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(watch)


def _metadata(*versions: str) -> str:
    items = "".join(f"<version>{v}</version>" for v in versions)
    return f"<metadata><versioning><versions>{items}</versions></versioning></metadata>"


def test_p3_4_pinned_version_is_read_from_the_real_makefile():
    pinned = watch.pinned_version((PROJECT_ROOT / "Makefile").read_text())
    assert watch.PLAIN_RELEASE.fullmatch(pinned)


def test_p3_4_missing_pin_is_an_error():
    with pytest.raises(ValueError, match="OPENLINEAGE_SPARK_VERSION"):
        watch.pinned_version("OTHER := 1.0.0\n")


def test_p3_4_newer_release_is_reported():
    result = watch.check(
        "OPENLINEAGE_SPARK_VERSION := 1.55.0\n", _metadata("1.9.1", "1.55.0", "1.56.0")
    )
    assert result == {"pinned": "1.55.0", "latest": "1.56.0", "outdated": True}


def test_p3_4_versions_compare_numerically_not_as_text():
    # "1.9.1" > "1.55.0" as text; 1.55.0 is the newer release
    result = watch.check("OPENLINEAGE_SPARK_VERSION := 1.55.0\n", _metadata("1.9.1", "1.55.0"))
    assert result["latest"] == "1.55.0"
    assert result["outdated"] is False


def test_p3_4_snapshots_and_candidates_never_raise_the_alarm():
    result = watch.check(
        "OPENLINEAGE_SPARK_VERSION := 1.55.0\n",
        _metadata("1.55.0", "1.56.0-SNAPSHOT", "1.56.0-rc1", "2.0.0-beta"),
    )
    assert result["outdated"] is False


def test_p3_4_metadata_without_a_plain_release_is_an_error():
    with pytest.raises(ValueError, match="no plain release"):
        watch.latest_release(_metadata("1.0.0-SNAPSHOT"))


def test_p3_4_metadata_is_only_fetched_over_https():
    with pytest.raises(ValueError, match="HTTPS"):
        watch.fetch_metadata("http://repo1.maven.org/maven2/x/maven-metadata.xml")


def test_p3_4_main_fails_when_the_check_cannot_run(monkeypatch, capsys):
    def unreachable(*_args, **_kwargs):
        raise OSError("network down")

    monkeypatch.setattr(watch, "fetch_metadata", unreachable)
    assert watch.main() == 1
    assert "network down" in capsys.readouterr().err


def test_p3_4_main_prints_one_json_object(monkeypatch, capsys):
    monkeypatch.setattr(watch, "fetch_metadata", lambda *a, **k: _metadata("1.55.0", "1.99.0"))
    assert watch.main() == 0
    out = json.loads(capsys.readouterr().out)
    assert out["latest"] == "1.99.0" and out["outdated"] is True
