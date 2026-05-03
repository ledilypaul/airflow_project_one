"""
Unit tests for storage/postgresql_writer.py.
DB and Spark connections are mocked.
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


MOCK_JDBC_RETURN = ("jdbc:postgresql://localhost:5432/testdb", {"user": "u", "password": "p", "driver": "org.postgresql.Driver"})


@pytest.fixture
def writer():
    """Instantiate PostgresWroter with all external calls mocked."""
    with patch("storage.postgresql_writer.db_connection", return_value=MagicMock()), \
         patch("storage.postgresql_writer.jdbc_connection_props", return_value=MOCK_JDBC_RETURN):
        from storage.postgresql_writer import PostgresWroter
        return PostgresWroter()


# ---------------------------------------------------------------------------
# Spark DataFrame → JDBC
# ---------------------------------------------------------------------------

def test_write_spark_dataframe_uses_jdbc(writer):
    mock_df = MagicMock()
    # pyspark.sql.DataFrame is not imported — simulate isinstance check with spec
    from pyspark.sql import DataFrame as SparkDF
    mock_df.__class__ = SparkDF

    writer.write(mock_df, "target_table", mode="append")

    mock_df.write.jdbc.assert_called_once_with(
        url=MOCK_JDBC_RETURN[0],
        table="target_table",
        mode="append",
        properties=MOCK_JDBC_RETURN[1],
    )


def test_write_spark_dataframe_overwrite_mode(writer):
    from pyspark.sql import DataFrame as SparkDF
    mock_df = MagicMock()
    mock_df.__class__ = SparkDF

    writer.write(mock_df, "target_table", mode="overwrite")

    mock_df.write.jdbc.assert_called_once_with(
        url=MOCK_JDBC_RETURN[0],
        table="target_table",
        mode="overwrite",
        properties=MOCK_JDBC_RETURN[1],
    )


# ---------------------------------------------------------------------------
# Pandas DataFrame → SQLAlchemy (to_sql)
# ---------------------------------------------------------------------------

def test_write_pandas_dataframe_uses_to_sql(writer):
    mock_df = MagicMock(spec=pd.DataFrame)

    writer.write(mock_df, "target_table", mode="append")

    mock_df.to_sql.assert_called_once_with(
        "target_table",
        writer.engine,
        if_exists="append",
        index=False,
    )


def test_write_pandas_overwrite_maps_to_replace(writer):
    mock_df = MagicMock(spec=pd.DataFrame)

    writer.write(mock_df, "target_table", mode="overwrite")

    mock_df.to_sql.assert_called_once_with(
        "target_table",
        writer.engine,
        if_exists="replace",
        index=False,
    )


# ---------------------------------------------------------------------------
# Unsupported type → TypeError
# ---------------------------------------------------------------------------

def test_write_unsupported_type_raises_type_error(writer):
    with pytest.raises(TypeError, match="only support Spark or Pandas DataFrame"):
        writer.write({"key": "value"}, "target_table")


def test_write_list_raises_type_error(writer):
    with pytest.raises(TypeError):
        writer.write([1, 2, 3], "target_table")
