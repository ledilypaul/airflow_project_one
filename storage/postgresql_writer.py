from .base_writer import BaseWriter
from utils.db_connection import db_connection, jdbc_connection_props
from pyspark.sql import DataFrame
import pandas as pd

class PostgresWroter(BaseWriter):
    def __init__(self):
        # SQLAlchemy connection (for Pandas / SQL queries)
        self.engine = db_connection()
        # JDBC connection (for Spark)
        self.jdbc_url, self.jdbc_props = jdbc_connection_props()

    def write(self, data, target: str, mode: str = "append", **kwargs):
         # Spark DataFrame → JDBC
        if isinstance(data, DataFrame):
            data.write.jdbc(
                url=self.jdbc_url,
                table=target,
                mode=mode,
                properties=self.jdbc_props
            )

        # Pandas DataFrame → SQLAlchemy
        elif isinstance(data, pd.DataFrame):
            data.to_sql(
                target,
                self.engine,
                if_exists="replace" if mode == "overwrite" else "append",
                index=False,
                **kwargs
            )
        else:
            raise TypeError("PostgresWriter.write() only support Spark or Pandas DataFrame")