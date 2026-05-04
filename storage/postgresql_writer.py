import logging

import pandas as pd
from pyspark.sql import DataFrame

from utils.db_connection import db_connection, jdbc_connection_props

from .base_writer import BaseWriter

log = logging.getLogger(__name__)


class PostgresWriter(BaseWriter):
    def __init__(self):
        self.engine = db_connection()
        self.jdbc_url, self.jdbc_props = jdbc_connection_props()

    def write(self, data, target: str, mode: str = "append", **kwargs):
        if isinstance(data, DataFrame):
            log.info("Writing Spark DataFrame to %s (mode=%s)", target, mode)
            data.write.jdbc(
                url=self.jdbc_url,
                table=target,
                mode=mode,
                properties=self.jdbc_props,
            )
        elif isinstance(data, pd.DataFrame):
            log.info("Writing Pandas DataFrame to %s (mode=%s)", target, mode)
            data.to_sql(
                target,
                self.engine,
                if_exists="replace" if mode == "overwrite" else "append",
                index=False,
                **kwargs,
            )
        else:
            raise TypeError("PostgresWriter.write() only supports Spark or Pandas DataFrame")
