"""`pipeline.file_reader.utils` is kept as a re-export of `pipeline.config.types`
(P2-1 / C-7): these tests go through the old import path on purpose.

The B2 regression tests (`genre_list: array` crashed every IMDB file) used to
live here and only checked that the schema *built*. They are merged into
tests/unit/file_reader/test_p2_1_readers.py::test_p2_1_every_repo_config_reads_a_synthetic_file,
which *reads* a file with every csv/txt config of the repository (C-1).
"""

import pytest
from pyspark.sql.types import BooleanType, DateType, FloatType, IntegerType, StringType

from pipeline.file_reader.utils import build_schema, get_spark_type


def test_get_spark_type_valid():
    assert isinstance(get_spark_type("string"), StringType)
    assert isinstance(get_spark_type("int"), IntegerType)
    assert isinstance(get_spark_type("float"), FloatType)
    assert isinstance(get_spark_type("boolean"), BooleanType)
    assert isinstance(get_spark_type("date"), DateType)


def test_get_spark_type_case_insensitive():
    assert isinstance(get_spark_type(" STRING "), StringType)


def test_get_spark_type_invalid():
    with pytest.raises(ValueError):
        get_spark_type("unknown_type")


def test_p2_1_array_type_is_not_supported():
    """C-1: `columns_type` only applies to csv/txt, and Spark's CSV source only
    reads atomic types (an ARRAY column fails the read with
    UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE). `array` is therefore usable
    nowhere: read the column as `string`, split it in a later transformation."""
    with pytest.raises(ValueError, match="array"):
        get_spark_type("array")


def test_p2_1_file_reader_utils_reexports_the_config_types():
    from pipeline.config import types

    assert get_spark_type is types.get_spark_type
    assert build_schema is types.build_schema
