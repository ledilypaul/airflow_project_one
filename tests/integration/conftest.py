"""Fixtures of the integration tests.

`spark` is the session-scoped fixture of tests/unit/conftest.py, reused as
is (same `local[1]` configuration) rather than redefined: the P2-4 dbt test
writes its mini lake with the real ParquetLakeWriter.
"""

from tests.unit.conftest import spark  # noqa: F401
