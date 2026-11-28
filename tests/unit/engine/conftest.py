import pytest


@pytest.fixture(scope="module")
def spark_utc(spark):
    """Session Spark en UTC, isolée (`newSession`) : la session partagée ne change pas."""
    session = spark.newSession()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    return session
