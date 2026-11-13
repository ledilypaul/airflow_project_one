import pytest

from pipeline.config import SourceConfig
from pipeline.file_reader.csv_reader import CSVReader


@pytest.fixture
def csv_reader(spark):
    return CSVReader(spark)


@pytest.fixture
def test_file(tmp_path):
    test_file = tmp_path / "file1.csv"
    test_file.write_text("col1,col2\n1,2\n3,4")
    return test_file


def _csv_config(options, columns_type=None) -> SourceConfig:
    reader = {"options": options}
    if columns_type is not None:
        reader["columns_type"] = columns_type
    return SourceConfig.model_validate(
        {"source": {"name": "test", "format": "csv", "patterns": ["*.csv"]}, "reader": reader}
    )


def test_csv_reader_with_schema(csv_reader, test_file):
    config = _csv_config(
        {"header": True, "inferSchema": True, "delimiter": ","},
        columns_type={"col1": "int", "col2": "string"},
    )
    df = csv_reader.read(str(test_file), config)
    assert df.count() == 2
    assert len(df.columns) == 2
    assert df.columns == ["col1", "col2"]
    # col2 would be inferred as int: "string" proves the explicit schema is used.
    assert df.dtypes == [("col1", "int"), ("col2", "string")]


def test_csv_reader_without_schema(csv_reader, test_file):
    config = _csv_config({"header": True, "inferSchema": True, "delimiter": ","})
    df = csv_reader.read(str(test_file), config)
    assert df.count() == 2
    assert len(df.columns) == 2
    assert df.columns == ["col1", "col2"]
    assert df.dtypes == [("col1", "int"), ("col2", "int")]
