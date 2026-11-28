"""Listing d'une source : `pipeline.extractor.extractor` (P5-3, remplace la config
`extractor_config.yaml`). Les tests P1-6 gardent leur sens : un répertoire absent
échoue, un répertoire sans correspondance donne `[]`."""

import pytest

from pipeline.extractor.extractor import list_source_files, list_unmatched_files


@pytest.fixture
def data_dir(tmp_path):
    directory = tmp_path / "data"
    directory.mkdir()
    for name in ("orders_1.csv", "ORDERS_2.CSV", "orders_old.csv", "other.csv", "notes.txt"):
        (directory / name).write_text("a\n1\n")
    (directory / "orders_dir.csv").mkdir()  # un répertoire n'est pas un fichier
    return directory


def _names(paths):
    return [p.rsplit("/", 1)[-1] for p in paths]


def test_p5_3_lists_names_matching_the_patterns_case_insensitively(data_dir):
    files = list_source_files([str(data_dir)], ["orders_*.csv"])
    assert _names(files) == ["ORDERS_2.CSV", "orders_1.csv", "orders_old.csv"]


def test_p5_3_exclude_removes_names(data_dir):
    files = list_source_files([str(data_dir)], ["orders_*.csv"], exclude=["*_OLD.csv"])
    assert _names(files) == ["ORDERS_2.CSV", "orders_1.csv"]


def test_p5_3_unmatched_files_are_the_ones_no_source_claims(data_dir):
    files = list_unmatched_files([str(data_dir)], ["orders_*.csv"], ["*.csv", "*.parquet"])
    assert _names(files) == ["other.csv"]


def test_p1_6_list_files_raises_on_missing_directory(tmp_path):
    missing = tmp_path / "does_not_exist"

    with pytest.raises(FileNotFoundError) as exc_info:
        list_source_files([str(missing)], ["*.csv"])

    message = str(exc_info.value)
    assert str(missing) in message
    assert str(missing.resolve()) in message


def test_p1_6_list_files_reports_resolved_path_for_relative_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    with pytest.raises(FileNotFoundError) as exc_info:
        list_source_files(["./no_such_data/"], ["*.csv"])

    message = str(exc_info.value)
    assert "./no_such_data/" in message
    assert str((tmp_path / "no_such_data").resolve()) in message


def test_p1_6_list_files_reports_only_missing_directories(tmp_path):
    existing = tmp_path / "existing"
    existing.mkdir()
    (existing / "file.csv").write_text("a,b\n1,2\n")
    missing_1 = tmp_path / "missing_one"
    missing_2 = tmp_path / "missing_two"

    with pytest.raises(FileNotFoundError) as exc_info:
        list_source_files([str(existing), str(missing_1), str(missing_2)], ["*.csv"])

    message = str(exc_info.value)
    assert str(missing_1) in message
    assert str(missing_2) in message
    assert str(existing) not in message.replace(str(missing_1), "").replace(str(missing_2), "")


def test_p1_6_list_files_returns_empty_list_when_directory_has_no_match(tmp_path):
    (tmp_path / "notes.txt").write_text("not a csv")
    assert list_source_files([str(tmp_path)], ["*.csv"]) == []


def test_p1_6_list_files_returns_empty_list_for_empty_directory(tmp_path):
    assert list_source_files([str(tmp_path)], ["*.csv"]) == []
