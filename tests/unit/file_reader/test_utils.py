# tests/unit/utils/test_functions_utils.py

import pytest
from pipeline.file_reader.utils import get_spark_type
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType, DateType

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
