import pytest
from pyspark.sql import DataFrame, SparkSession

from pipeline.file_reader.excel_reader import ExcelReader


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("pytest").master("local[*]").getOrCreate()


@pytest.fixture
def excel_reader(spark):
    return ExcelReader(spark)


@pytest.fixture
def test_excel_file(tmp_path):
    pytest.importorskip("openpyxl")
    import pandas as pd

    test_file = tmp_path / "file1.xlsx"
    pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]}).to_excel(test_file, index=False)
    return test_file


def test_b3_excel_reader_returns_native_spark_dataframe(excel_reader, test_excel_file):
    """
    Regression test for B3 (docs/audit/2026-11-05-baseline.md): `ExcelReader.read()`
    used to return a pandas-on-Spark DataFrame (`pyspark.pandas.DataFrame`),
    which has no `.show()` method — crashing `read_files_job.py`, which calls
    `content.show(...)` on whatever a reader returns. It must now return a
    native `pyspark.sql.DataFrame`.
    """
    df = excel_reader.read(str(test_excel_file), {"reader_options": {}})

    assert isinstance(df, DataFrame)
    # .show() must not raise (this is exactly what read_files_job.py calls).
    df.show()
    assert df.count() == 2
    assert sorted(df.columns) == ["col1", "col2"]
