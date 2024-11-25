import os
import pytest, shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
from pipeline.file_reader.base_reader import BaseFileReader
from pyspark.sql import SparkSession

@pytest.fixture
def base_reader():
    """Fixture pour initialiser un lecteur de base."""
    return BaseFileReader()

@pytest.fixture
def config_dir(tmp_path):
    """Fixture pour créer un dossier temporaire de configuration avec des fichiers."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    # Créer des fichiers de configuration
    specific_config = config_dir / "example_config.yaml"
    specific_config.write_text("""
config_name: "sample_config"
header: true
inferSchema: true
delimiter: ","
""")

    default_config = config_dir / "basic_reading_config.yaml"
    default_config.write_text("""
config_name: "basic_reading_config"
header: true
inferSchema: false
delimiter: ","
""")
    
    return config_dir

@pytest.fixture
def test_file(tmp_path):
    test_file = tmp_path / "file1.csv"
    test_file.write_text("col1,col2\n1,2\n3,4")
    return test_file

def test_get_config_file_specific(base_reader, config_dir):
    reader = base_reader
    # Modifiez le répertoire courant pour le test 
    os.chdir(config_dir.parent)
    config_content = base_reader.get_config_file("example_file.csv")
    assert config_content['header'] == True
    assert config_content['config_name'] == "sample_config"
    
def test_get_config_file(base_reader, config_dir):
    reader = base_reader
    os.chdir(config_dir.parent)
    config_content = reader.get_config_file("file1")
    assert config_content['header'] == True
    assert config_content['config_name'] == "basic_reading_config"

def test_read_files(base_reader,config_dir,test_file):
    reader = base_reader
    os.chdir(config_dir.parent)
    df = reader.read_files(str(test_file))
    assert df.count() == 2 # Nombre de lignes