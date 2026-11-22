"""Fixtures shared by every unit test (P2-1 / C-8)."""

import pytest


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for the whole test session.

    Every Spark test module used to declare its own `spark` fixture with a
    different master (`local[1]`, `local[*]`) and, for one of them,
    `spark.sql.shuffle.partitions=1`. `getOrCreate()` silently returned the
    first session created, then applied the later builder's SQL configs to
    it: the effective configuration depended on the test order. There is now
    a single, explicit configuration:

    - `local[1]`: tiny files, no need for several cores;
    - `spark.ui.enabled=false`: no web UI port to bind per run;
    - `spark.sql.shuffle.partitions=1`: tiny data (was set by the processor
      tests only, and leaked to every later test).

    No test may change the session configuration (`spark.conf.set`): it
    would leak to every test that runs after it.
    """
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("airflow_project_one-unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
