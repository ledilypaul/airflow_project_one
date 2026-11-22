"""P3-2: the real Docker build context of the project image (.dockerignore).

Docker itself applies .dockerignore: a throwaway image `FROM scratch` copies the
whole context, then its file list is read back (no base image, no network).
Never in the context: .env and the other local secrets, the local lake, the
SQLite metadata DB, airflow.cfg, logs, the local virtualenvs, source data. Skipped
without Docker, unless REQUIRE_K8S=1.

The forbidden paths are only meaningful where they exist (a developer's tree:
.env, airflow.db, lake/, .venv...); the allow-list of .dockerignore keeps them
out anyway. .env is never read: the listing gives names only.
"""

import io
import os
import shutil
import subprocess
import tarfile
import uuid
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
REQUIRED = os.environ.get("REQUIRE_K8S") == "1"

# Must never be in the context (any of them may exist in a developer's tree).
FORBIDDEN = (
    ".env",
    "airflow.cfg",
    "airflow.db",
    "lake/",
    "logs/",
    ".venv/",
    "data/",
    "lineage/",
    ".k8s/",
    ".git/",
    "tests/",
    "transform/.venv/",
    "transform/target/",
    "transform/warehouse.duckdb",
    "simple_auth_manager_passwords.json.generated",
)
REQUIRED_FILES = (
    "Dockerfile",
    "pyproject.toml",
    "uv.lock",
    "dags/local_read_file_dag.py",
    "pipeline/jobs/read_files_job.py",
    "utils/spark_session.py",
    "storage/parquet_lake_writer.py",
    "config/sources/orders.yaml",
    "transform/dbt_project.yml",
    "transform/uv.lock",
    "deploy/airflow/pools.json",
    "deploy/k8s/image/entrypoint.sh",
    "deploy/k8s/image/spark-entrypoint.sh",
)


def _docker() -> str:
    docker = shutil.which("docker")
    if (
        docker
        and subprocess.run([docker, "info"], capture_output=True, check=False).returncode == 0
    ):
        return docker
    if REQUIRED:
        pytest.fail("REQUIRE_K8S=1: Docker is not available")
    pytest.skip("Docker is not available")


@pytest.fixture(scope="module")
def context_files():
    docker = _docker()
    tag = f"p3-2-context-check:{uuid.uuid4().hex[:12]}"
    dockerfile = "FROM scratch\nCOPY . /ctx/\n"
    build = subprocess.run(
        [docker, "build", "-q", "-t", tag, "-f", "-", str(PROJECT_ROOT)],
        input=dockerfile,
        capture_output=True,
        text=True,
        check=False,
    )
    assert build.returncode == 0, build.stderr
    container = None
    try:
        create = subprocess.run(
            [docker, "create", tag, "/none"], capture_output=True, text=True, check=True
        )
        container = create.stdout.strip()
        export = subprocess.run([docker, "export", container], capture_output=True, check=True)
        with tarfile.open(fileobj=io.BytesIO(export.stdout)) as archive:
            names = {
                member.name[len("ctx/") :] + ("/" if member.isdir() else "")
                for member in archive.getmembers()
                if member.name.startswith("ctx/")
            }
    finally:
        if container:
            subprocess.run([docker, "rm", "-f", container], capture_output=True, check=False)
        subprocess.run([docker, "rmi", "-f", tag], capture_output=True, check=False)
    return names


def test_p3_2_context_holds_what_the_image_needs(context_files):
    for path in REQUIRED_FILES:
        assert path in context_files, path


def test_p3_2_context_never_holds_secrets_or_local_state(context_files):
    for forbidden in FORBIDDEN:
        hits = [name for name in context_files if name == forbidden or name.startswith(forbidden)]
        assert not hits, (forbidden, hits[:5])
    for name in context_files:
        base = name.rstrip("/").rsplit("/", 1)[-1]
        assert base != ".env" and not base.startswith(".env."), name
        assert "__pycache__" not in name, name
        assert not name.endswith(".duckdb"), name
