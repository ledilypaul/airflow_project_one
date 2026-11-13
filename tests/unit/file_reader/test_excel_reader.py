from pathlib import Path

import pytest
from pyspark.sql import DataFrame

from pipeline.config import SourceConfig, load_source_config
from pipeline.file_reader.excel_reader import ExcelReader

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
# Was the `xlsx:` section of config/basic_reading_config.yaml before P2-1.
DEFAULT_XLSX_CONFIG = PROJECT_ROOT / "config" / "defaults" / "xlsx.yaml"


# P2-2: `storage` is mandatory in every reading config; these tests only read.
STORAGE = {"format": "parquet", "mode": "overwrite", "raw_path": "raw/"}


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
    config = SourceConfig.model_validate(
        {
            "source": {"name": "test", "format": "xlsx", "patterns": ["*.xlsx"]},
            "storage": STORAGE,
        }
    )
    df = excel_reader.read(str(test_excel_file), config)

    assert isinstance(df, DataFrame)
    # .show() must not raise (this is exactly what read_files_job.py calls).
    df.show()
    assert df.count() == 2
    assert sorted(df.columns) == ["col1", "col2"]


def test_c_new1_basic_reading_config_xlsx_header_preserves_all_rows(excel_reader, tmp_path):
    """
    Regression test for C-NEW1 (follow-up finding on the B3 fix,
    docs/audit/2026-11-05-baseline.md): the real `config/basic_reading_config.yaml`
    used to declare `xlsx.reader_options.header: 1`. `pandas.read_excel()`
    treats `header` as a row index (0-based), so `header: 1` makes it use the
    *second* row as column names and silently drop the first data row. This
    was invisible before the B3 fix because `ExcelReader.read()` crashed on
    `.show()` (pandas-on-Spark has no such method) before anyone could see
    the corrupted data; now that `.to_spark()` succeeds, a wrong `header`
    value would silently corrupt data instead of crashing. This test loads
    the *actual* fallback config (since P2-1: config/defaults/xlsx.yaml,
    loaded and validated as `BaseFileReader` does), not a simplified mock,
    so it would catch a regression of this config value directly.
    """
    pytest.importorskip("openpyxl")
    import pandas as pd

    xlsx_config = load_source_config(DEFAULT_XLSX_CONFIG)

    test_file = tmp_path / "data.xlsx"
    pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "price": [1.5, 2.5, 3.5],
            "is_active": [True, False, True],
        }
    ).to_excel(test_file, index=False)

    df = excel_reader.read(str(test_file), xlsx_config)

    assert sorted(df.columns) == ["id", "is_active", "name", "price"]
    assert df.count() == 3
