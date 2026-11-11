"""
Regression tests for backlog item P0-4 (finalize the uv migration).

See docs/audit/2026-11-05-baseline.md, constats D1 / M5, and the C-1 follow-up
finding on commit 599af82: `pipeline.jobs.read_files_job` must keep working
when run with an interpreter that does NOT have the project's editable
install (bare/system/cluster `spark-submit`, `ENV=prod` on YARN/Kubernetes),
not only under `uv run spark-submit ...`.
"""

import ast
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Files that must never manually mutate sys.path: the project is installed
# in editable mode by `uv sync`, so `pipeline.*` / `utils.*` / `storage.*`
# are importable without it for anything that only ever runs inside that
# venv (DAGs loaded by the Airflow scheduler/dag-processor, test conftest).
#
# `pipeline/jobs/read_files_job.py` is deliberately excluded here: unlike
# the DAG files, it can also be invoked by a `spark-submit` whose driver
# Python is NOT the uv-managed venv (C-1), so it keeps a guarded sys.path
# fallback — see test_p0_4_read_files_job_sys_path_fallback_is_guarded below.
PACKAGING_SENSITIVE_FILES = [
    PROJECT_ROOT / "dags" / "local_list_files_dag.py",
    PROJECT_ROOT / "dags" / "local_list_files_dag_python_operator.py",
    PROJECT_ROOT / "dags" / "local_read_file_dag.py",
    PROJECT_ROOT / "tests" / "unit" / "dags" / "conftest.py",
]

FORBIDDEN_SNIPPETS = ("sys.path.insert", "sys.path.append")

READ_FILES_JOB = PROJECT_ROOT / "pipeline" / "jobs" / "read_files_job.py"


@pytest.mark.parametrize("file_path", PACKAGING_SENSITIVE_FILES, ids=lambda p: p.name)
def test_p0_4_no_manual_sys_path_mutation(file_path: Path):
    """Nothing re-introduces an unconditional sys.path hack in these files."""
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


def _except_handler_line_ranges(tree: ast.Module, names: set[str]) -> list[tuple[int, int]]:
    """Line ranges covered by `except <one of names>:` handlers in `tree`."""
    ranges = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        for handler in node.handlers:
            handler_names: set[str] = set()
            if isinstance(handler.type, ast.Name):
                handler_names = {handler.type.id}
            elif isinstance(handler.type, ast.Tuple):
                handler_names = {e.id for e in handler.type.elts if isinstance(e, ast.Name)}
            if handler_names & names:
                ranges.append((handler.lineno, handler.end_lineno))
    return ranges


def _sys_path_mutation_lines(tree: ast.Module) -> list[int]:
    lines = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"insert", "append"}
            and isinstance(node.func.value, ast.Attribute)
            and node.func.value.attr == "path"
            and isinstance(node.func.value.value, ast.Name)
            and node.func.value.value.id == "sys"
        ):
            lines.append(node.lineno)
    return lines


def test_p0_4_read_files_job_sys_path_fallback_is_guarded():
    """
    C-1: read_files_job.py is allowed a sys.path fallback (it may run under
    a bare/cluster spark-submit without the project's editable install),
    but only as a last resort inside `except ModuleNotFoundError:` — never
    unconditionally at module top-level, which would defeat the point of
    the editable install for the normal `uv run spark-submit ...` path.
    """
    tree = ast.parse(READ_FILES_JOB.read_text(encoding="utf-8"))
    guarded_ranges = _except_handler_line_ranges(tree, {"ModuleNotFoundError", "ImportError"})
    mutation_lines = _sys_path_mutation_lines(tree)

    assert mutation_lines, (
        "expected a guarded sys.path fallback in read_files_job.py (C-1), found none"
    )
    for lineno in mutation_lines:
        assert any(start <= lineno <= end for start, end in guarded_ranges), (
            f"{READ_FILES_JOB}:{lineno} mutates sys.path outside of an "
            "`except ModuleNotFoundError` guard"
        )


def test_p0_4_read_files_job_imports_without_editable_install_on_syspath():
    """
    Reproduces C-1 directly: run read_files_job.py with the project's own
    venv interpreter, but in isolated mode (`-S`) with PYTHONPATH pointing
    only at site-packages — i.e. pyspark/sqlalchemy/etc. are still found,
    but the editable-install `.pth` that normally puts the project root on
    sys.path is never processed, exactly like a bare system/cluster
    `spark-submit` whose driver Python never ran `uv sync`.

    Before the C-1 fix this failed with `ModuleNotFoundError: No module
    named 'pipeline'` at the import line. After the fix it must get past
    the imports and fail later, on the (expected, unrelated) missing DB
    env vars — proving the guarded sys.path fallback works.
    """
    site_packages = next(
        p for p in sys.path if p.endswith("site-packages") and str(PROJECT_ROOT) in p
    )

    result = subprocess.run(
        [sys.executable, "-S", str(READ_FILES_JOB), "--run-id", "run_test"],
        cwd="/tmp",
        env={"PYTHONPATH": site_packages},
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert "ModuleNotFoundError: No module named 'pipeline'" not in result.stderr, (
        "read_files_job.py can't be imported without the editable install "
        f"on sys.path (C-1 regression). stderr:\n{result.stderr}"
    )
    # Expected failure point in this sandboxed run: no DB credentials.
    assert "ValueError: One or more required environment variables are not set." in result.stderr
