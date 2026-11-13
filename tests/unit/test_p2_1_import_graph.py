"""
P2-1 / C-5 and C-7: what an import loads, checked in a fresh subprocess (the
pytest process has already imported everything, `sys.modules` there proves
nothing).
"""

import subprocess
import sys
import textwrap
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _run(script: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_p2_1_spark_job_import_does_not_load_pyspark_pandas():
    """C-5: ReaderFactory imports every reader, and ExcelReader imported
    `pyspark.pandas` at module level: reading a CSV then required a pandas and
    a pyarrow compatible with pandas-on-Spark. It is only needed for xlsx."""
    result = _run(
        """
        import sys

        import pipeline.jobs.read_files_job  # noqa: F401

        if "pyspark.pandas" in sys.modules:
            print("pyspark.pandas loaded")
            sys.exit(1)
        print("OK")
        """
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert result.stdout.strip().endswith("OK")


def test_p2_1_pipeline_config_imports_without_file_reader():
    """C-7: pipeline.config.models used to import pipeline.file_reader.utils
    while the readers import pipeline.config: a re-export in
    pipeline/file_reader/__init__.py was enough to break `import
    pipeline.config` with a circular import. pipeline.config must depend on
    nothing under pipeline.file_reader."""
    result = _run(
        """
        import sys

        import pipeline.config  # noqa: F401
        from pipeline.config import SourceRegistry  # noqa: F401
        from pipeline.config.types import build_schema, get_spark_type  # noqa: F401

        loaded = sorted(m for m in sys.modules if m.startswith("pipeline.file_reader"))
        if loaded:
            print("pipeline.file_reader modules loaded: " + ", ".join(loaded))
            sys.exit(1)
        print("OK")
        """
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert result.stdout.strip().endswith("OK")
