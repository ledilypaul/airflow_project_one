"""Fuseau de la session Spark du job : UTC, pas celui de la machine (constat P5-7)."""

from unittest.mock import MagicMock

from utils import spark_session


def test_p5_7_job_session_reads_timestamps_in_utc(monkeypatch):
    builder = MagicMock()
    builder.appName.return_value = builder
    monkeypatch.setattr(spark_session.SparkSession, "builder", builder)
    for env in ("dev", "prod"):
        monkeypatch.setenv("ENV", env)
        builder.config.reset_mock()
        spark_session.get_spark_session("t")
        builder.config.assert_any_call("spark.sql.session.timeZone", "UTC")
