"""
Regression tests for backlog item P0-4 (finalize the uv migration): no file
should manually mutate `sys.path` to make `pipeline.*` / `utils.*` /
`storage.*` importable, since the project is installed in editable mode by
`uv sync` (see CLAUDE.md, M5).

See docs/audit/2026-11-05-baseline.md, constats D1 / M5.
"""

from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# DAG files and the standalone Spark job entrypoint: historically these were
# the files that did `sys.path.append(...)` to reach the project root.
PACKAGING_SENSITIVE_FILES = [
    PROJECT_ROOT / "dags" / "local_list_files_dag.py",
    PROJECT_ROOT / "dags" / "local_list_files_dag_python_operator.py",
    PROJECT_ROOT / "dags" / "local_read_file_dag.py",
    PROJECT_ROOT / "pipeline" / "jobs" / "read_files_job.py",
    PROJECT_ROOT / "tests" / "unit" / "dags" / "conftest.py",
]

FORBIDDEN_SNIPPETS = ("sys.path.insert", "sys.path.append")


@pytest.mark.parametrize("file_path", PACKAGING_SENSITIVE_FILES, ids=lambda p: p.name)
def test_p0_4_no_manual_sys_path_mutation(file_path: Path):
    """Nothing re-introduces a manual sys.path hack in these files."""
    source = file_path.read_text(encoding="utf-8")
    found = [snippet for snippet in FORBIDDEN_SNIPPETS if snippet in source]
    assert not found, (
        f"{file_path} still manually mutates sys.path ({found}); "
        "the project is installed in editable mode via `uv sync`, "
        "`pipeline.*` / `utils.*` / `storage.*` are importable without it."
    )


def test_p0_4_business_packages_importable_without_sys_path_hack():
    """
    The modules actually imported by the DAGs/job (Extractor,
    insert_into_file_list, list_file_from_db, get_spark_session,
    BaseFileReader) resolve through the editable install, not through any
    sys.path mutation performed by this test or by application code.
    """
    from pipeline.extractor.extractor import Extractor  # noqa: F401
    from pipeline.file_reader.base_reader import BaseFileReader  # noqa: F401
    from storage.postgresql_writer import PostgresWriter  # noqa: F401
    from utils.functions_utils import insert_into_file_list, list_file_from_db  # noqa: F401
    from utils.spark_session import get_spark_session  # noqa: F401
