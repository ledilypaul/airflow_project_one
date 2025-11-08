import pytest
from pyspark.sql import SparkSession
from pipeline.file_reader.csv_reader import CSVReader
from pathlib import Path

@pytest.fixture(scope="module") 
def spark(): 
    return SparkSession.builder.appName("pytest").master("local[*]").getOrCreate()

@pytest.fixture
def csv_reader(spark):
    return CSVReader(spark)

@pytest.fixture
def config_dir(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    # Créer des fichiers de configuration
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

def test_csv_reader_with_schema(csv_reader, test_file):
    config = {
        "header" : True,
        "inferSchema" : True,
        "delimiter" : ",",
        "columns_type" : {
            "col1" : int,
            "col2" : int
        }
    }
    df = csv_reader.read(str(test_file), config)
    assert df.count() == 2
    assert len(df.columns) == 2
    assert df.columns == ["col1", "col2"]
    assert df.dtypes == [("col1", "int"), ("col2", "int")]

def test_csv_reader_without_schema(csv_reader,test_file):
    config = {
        "header" : True,
        "inferSchema" : True,
        "delimiter" : ","
    }
    df = csv_reader.read(str(test_file), config)
    assert df.count() == 2
    assert len(df.columns) == 2
    assert df.columns == ["col1", "col2"]