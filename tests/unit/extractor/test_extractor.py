import os
import tempfile

import pytest
import yaml

from pipeline.extractor.extractor import Extractor  # Import de la classe à tester


@pytest.fixture
def config_file():
    # Création d'une configuration temporaire pour le test
    config_content = {
        "extractor_config": {
            "source_directories": [],
            "files_extensions": [".csv", ".json"],
        }
    }
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".yaml") as tmp:
        yaml.dump(config_content, tmp)
        yield tmp.name
    os.remove(tmp.name)


@pytest.fixture
def temp_directory_with_files():
    # Création d'un répertoire temporaire
    with tempfile.TemporaryDirectory() as temp_dir:
        # Création de fichiers temporaires
        file_paths = [
            os.path.join(temp_dir, "file1.csv"),
            os.path.join(temp_dir, "file2.json"),
            os.path.join(temp_dir, "file3.txt"),  # Ce fichier ne devrait pas être listé
        ]
        for file_path in file_paths:
            with open(file_path, "w") as f:
                f.write("test data")  # Écriture de contenu de test

        yield temp_dir, file_paths[:2]  # On retourne le répertoire et les fichiers attendus


def test_list_files(config_file, temp_directory_with_files):
    temp_dir, expected_files = temp_directory_with_files

    with open(config_file) as file:
        config_content = yaml.safe_load(file)
    config_content["extractor_config"]["source_directories"] = [temp_dir]

    with open(config_file, "w") as file:
        yaml.dump(config_content, file)
    extractor = Extractor(config_file)

    listed_files = extractor.list_files()
    assert sorted(listed_files) == sorted(expected_files), (
        f"Les fichiers listés {listed_files} ne correspondent pas "
        f"aux fichiers attendus {expected_files}"
    )


def test_check_integrity_files(config_file, temp_directory_with_files):
    temp_dir, expected_files = temp_directory_with_files

    with open(config_file) as file:
        config_content = yaml.safe_load(file)
    config_content["extractor_config"]["source_directories"] = [temp_dir]

    with open(config_file, "w") as file:
        yaml.dump(config_content, file)
    extractor = Extractor(config_file)

    listed_files = extractor.check_integrity_files(temp_directory_with_files[1])
    assert sorted(listed_files) == sorted(expected_files), (
        f"Les fichiers listés {listed_files} ne correspondent pas "
        f"aux fichiers attendus {expected_files}"
    )


# ---------------------------------------------------------------------------
# P1-6 / C-1 — a missing source directory must fail loudly, not look "empty".
#
# Since P1-6, an empty listing makes the insertion task skip (green run).
# A configured directory that does not exist (typo, unmounted volume, other
# working directory on the worker) must therefore raise instead of silently
# producing [], otherwise a deployment failure turns into a green run.
# ---------------------------------------------------------------------------


def _make_extractor(tmp_path, source_directories, extensions=(".csv",)):
    config_path = tmp_path / "extractor_config.yaml"
    config_path.write_text(
        yaml.dump(
            {
                "extractor_config": {
                    "source_directories": [str(d) for d in source_directories],
                    "files_extensions": list(extensions),
                }
            }
        )
    )
    return Extractor(str(config_path))


def test_p1_6_list_files_raises_on_missing_directory(tmp_path):
    missing = tmp_path / "does_not_exist"
    extractor = _make_extractor(tmp_path, [missing])

    with pytest.raises(FileNotFoundError) as exc_info:
        extractor.list_files()

    message = str(exc_info.value)
    assert str(missing) in message
    assert str(missing.resolve()) in message


def test_p1_6_list_files_reports_resolved_path_for_relative_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    extractor = _make_extractor(tmp_path, ["./no_such_data/"])

    with pytest.raises(FileNotFoundError) as exc_info:
        extractor.list_files()

    message = str(exc_info.value)
    assert "./no_such_data/" in message
    assert str((tmp_path / "no_such_data").resolve()) in message


def test_p1_6_list_files_reports_only_missing_directories(tmp_path):
    existing = tmp_path / "existing"
    existing.mkdir()
    (existing / "file.csv").write_text("a,b\n1,2\n")
    missing_1 = tmp_path / "missing_one"
    missing_2 = tmp_path / "missing_two"
    extractor = _make_extractor(tmp_path, [existing, missing_1, missing_2])

    with pytest.raises(FileNotFoundError) as exc_info:
        extractor.list_files()

    message = str(exc_info.value)
    assert str(missing_1) in message
    assert str(missing_2) in message
    assert str(existing) not in message.replace(str(missing_1), "").replace(str(missing_2), "")


def test_p1_6_list_files_returns_empty_list_when_directory_has_no_match(tmp_path):
    existing = tmp_path / "existing"
    existing.mkdir()
    (existing / "notes.txt").write_text("not a csv")
    extractor = _make_extractor(tmp_path, [existing])

    assert extractor.list_files() == []


def test_p1_6_list_files_returns_empty_list_for_empty_directory(tmp_path):
    existing = tmp_path / "empty"
    existing.mkdir()
    extractor = _make_extractor(tmp_path, [existing])

    assert extractor.list_files() == []
