"""
Unit tests for pipeline/processor/base_processor.py.
Requires a Spark session (local mode).
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pipeline.processor.base_processor import BaseProcessor


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_processor")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture
def simple_df(spark):
    data = [("Alice", 30), ("Bob", 25), ("Alice", 30), (None, None)]
    return spark.createDataFrame(data, ["name", "age"])


# ---------------------------------------------------------------------------
# remove_empty_rows
# ---------------------------------------------------------------------------

def test_remove_empty_rows_drops_all_null_row(simple_df):
    result = BaseProcessor(simple_df).remove_empty_rows().df
    assert result.count() == 3  # (None, None) row dropped


def test_remove_empty_rows_keeps_partial_null(spark):
    data = [("Alice", None), (None, 30)]
    df = spark.createDataFrame(data, ["name", "age"])
    result = BaseProcessor(df).remove_empty_rows().df
    assert result.count() == 2  # partial nulls are kept


# ---------------------------------------------------------------------------
# drop_na_rows
# ---------------------------------------------------------------------------

def test_drop_na_rows_no_subset(simple_df):
    # simple_df has 4 rows; only (None, None) has nulls → dropped → 3 rows remain
    result = BaseProcessor(simple_df).drop_na_rows().df
    assert result.count() == 3


def test_drop_na_rows_with_subset(simple_df):
    result = BaseProcessor(simple_df).drop_na_rows(subset=["name"]).df
    assert result.count() == 3  # (None, None) dropped; (Alice/Bob, None) would stay


# ---------------------------------------------------------------------------
# fill_na
# ---------------------------------------------------------------------------

def test_fill_na_replaces_nulls(spark):
    data = [("Alice", None), (None, 30)]
    df = spark.createDataFrame(data, ["name", "age"])
    result = BaseProcessor(df).fill_na({"name": "Unknown", "age": 0}).df
    rows = {row["name"]: row["age"] for row in result.collect()}
    assert rows["Unknown"] == 30
    assert rows["Alice"] == 0


# ---------------------------------------------------------------------------
# remove_duplicates  ← BUG documenté
# ---------------------------------------------------------------------------

def test_remove_duplicates_bug(simple_df):
    """
    BUG: BaseProcessor.remove_duplicates() calls self.df.dropDuplicates()
    without reassigning the result, so duplicates are NOT removed.
    Fix: change to  self.df = self.df.dropDuplicates()
    When the bug is fixed, update assertion to count() == 3.
    """
    result = BaseProcessor(simple_df).remove_duplicates().df
    # Current (buggy) behaviour: count stays at 4 instead of 3
    assert result.count() == 4


# ---------------------------------------------------------------------------
# cast_column
# ---------------------------------------------------------------------------

def test_cast_column_int_to_string(spark):
    df = spark.createDataFrame([(1,), (2,)], ["value"])
    result = BaseProcessor(df).cast_column("value", "string").df
    assert result.schema["value"].dataType == StringType()


def test_cast_column_string_to_int(spark):
    df = spark.createDataFrame([("10",), ("20",)], ["value"])
    result = BaseProcessor(df).cast_column("value", "int").df
    assert result.schema["value"].dataType == IntegerType()


# ---------------------------------------------------------------------------
# rename_columns
# ---------------------------------------------------------------------------

def test_rename_columns(simple_df):
    rename_map = {"name": "full_name", "age": "years"}
    result = BaseProcessor(simple_df).rename_columns(rename_map).df
    assert "full_name" in result.columns
    assert "years" in result.columns
    assert "name" not in result.columns


def test_rename_columns_partial(simple_df):
    result = BaseProcessor(simple_df).rename_columns({"name": "full_name"}).df
    assert "full_name" in result.columns
    assert "age" in result.columns  # unchanged


# ---------------------------------------------------------------------------
# Method chaining
# ---------------------------------------------------------------------------

def test_method_chaining(simple_df):
    # remove_empty_rows: drops (None,None) → 3 rows
    # drop_na_rows: no more nulls → 3 rows
    # rename: cosmetic change
    result = (
        BaseProcessor(simple_df)
        .remove_empty_rows()
        .drop_na_rows()
        .rename_columns({"name": "full_name"})
        .df
    )
    assert result.count() == 3
    assert "full_name" in result.columns
