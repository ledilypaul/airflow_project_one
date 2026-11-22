"""Fixtures of the integration tests.

`spark` is the session-scoped fixture of tests/unit/conftest.py, reused as
is (same `local[1]` configuration) rather than redefined: the P2-4 dbt test
writes its mini lake with the real ParquetLakeWriter.

P2-5: the lineage tests import the DAG files in process. Same isolated
Airflow configuration as tests/unit/dags/conftest.py (no real metadata DB,
no example DAGs, no telemetry, OpenLineage off unless a test turns it on),
applied before anything under `airflow.*` is imported.
"""

import tests.unit.dags.conftest  # noqa: F401  (environment set up at import)
from tests.unit.conftest import spark  # noqa: F401
