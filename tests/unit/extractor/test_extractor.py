import os
import tempfile
import yaml
import pytest
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
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".yaml") as tmp:
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
            os.path.join(temp_dir, "file3.txt")  # Ce fichier ne devrait pas être listé
        ]
        for file_path in file_paths:
            with open(file_path, 'w') as f:
                f.write("test data")  # Écriture de contenu de test

        yield temp_dir, file_paths[:2]  # On retourne le répertoire et les fichiers attendus

def test_list_files(config_file, temp_directory_with_files):
    temp_dir, expected_files = temp_directory_with_files
    
    with open(config_file, 'r') as file:
        config_content = yaml.safe_load(file)
    config_content["extractor_config"]["source_directories"] = [temp_dir]

    with open(config_file, 'w') as file:
        yaml.dump(config_content, file)
    extractor = Extractor(config_file)

    listed_files = extractor.list_files()
    assert sorted(listed_files) == sorted(expected_files), \
        f"Les fichiers listés {listed_files} ne correspondent pas aux fichiers attendus {expected_files}"

def test_check_integrity_files(config_file, temp_directory_with_files):
    temp_dir, expected_files = temp_directory_with_files
    
    with open(config_file, 'r') as file:
        config_content = yaml.safe_load(file)
    config_content["extractor_config"]["source_directories"] = [temp_dir]

    with open(config_file, 'w') as file:
        yaml.dump(config_content, file)
    extractor = Extractor(config_file)

    listed_files = extractor.check_integrity_files(temp_directory_with_files[1])
    assert sorted(listed_files) == sorted(expected_files), \
        f"Les fichiers listés {listed_files} ne correspondent pas aux fichiers attendus {expected_files}"