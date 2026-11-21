"""P3-2: the project image (Dockerfile, .dockerignore, entrypoints).

Static checks, no build: pinned bases (tag + digest), Airflow version equal to
the lock's, non-root user, the OpenLineage jar checked by SHA-256 with the
Makefile's pin, Spark's Kubernetes entrypoint vendored unchanged, the build
context an allow-list without secrets or local data. The real build context is
checked by tests/integration/test_p3_2_image_context.py (Docker).
"""

import hashlib
import re
import subprocess
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DOCKERFILE = PROJECT_ROOT / "Dockerfile"
DOCKERIGNORE = PROJECT_ROOT / ".dockerignore"
IMAGE_DIR = PROJECT_ROOT / "deploy" / "k8s" / "image"
AIRFLOW_VALUES = PROJECT_ROOT / "deploy" / "k8s" / "values-airflow.yaml"
MAKEFILE = PROJECT_ROOT / "Makefile"

# apache/spark-docker, commit f933b4486c0b7f91545c2bbc63cd6b960787190b
# ("[SPARK-60864] Publish Apache Spark 4.3.0 to docker registry"),
# 4.3.0/scala2.13-java17-ubuntu/entrypoint.sh: SHA-256 of the upstream file.
SPARK_ENTRYPOINT_SHA256 = "ec72936bfb2abed8cfe36e47ae79d0b04fbcddfacb0e96fd1a0cd7f2d592ecf3"
SPARK_DOCKER_COMMIT = "f933b4486c0b7f91545c2bbc63cd6b960787190b"

DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")


def _dockerfile() -> str:
    return DOCKERFILE.read_text()


def _arg_defaults() -> dict[str, str]:
    return dict(re.findall(r"^ARG (\w+)=(\S+)$", _dockerfile(), re.MULTILINE))


def _make_var(name: str) -> str:
    match = re.search(rf"^{name} :?= (\S+)$", MAKEFILE.read_text(), re.MULTILINE)
    assert match, name
    return match.group(1)


def test_p3_2_every_base_image_is_pinned_by_tag_and_digest():
    args = _arg_defaults()
    froms = re.findall(r"^FROM (\S+)", _dockerfile(), re.MULTILINE)
    assert froms, "no FROM"
    stages = set(re.findall(r"^FROM \S+ AS (\S+)", _dockerfile(), re.MULTILINE))
    for image in froms:
        if image in stages:
            continue
        match = re.fullmatch(r"\$\{(\w+)\}", image)
        assert match, f"FROM {image}: use a pinned ARG"
        reference = args[match.group(1)]
        assert DIGEST.search(reference), reference
        assert ":" in reference.split("@")[0], f"{reference}: tag missing"
    # COPY --from=<image> would bypass the pins: only stage names are allowed.
    for source in re.findall(r"COPY [^\n]*--from=(\S+)", _dockerfile()):
        assert source in stages, source


def test_p3_2_airflow_base_is_the_version_of_the_lock():
    airflow_image = _arg_defaults()["AIRFLOW_IMAGE"]
    assert airflow_image.startswith("apache/airflow:slim-3.3.2-python3.12@sha256:")
    assert '"apache-airflow==3.3.2"' in (PROJECT_ROOT / "pyproject.toml").read_text()
    values = yaml.safe_load(AIRFLOW_VALUES.read_text())
    assert values["airflowVersion"] == "3.3.2"
    # PySpark of the lock, Python of the image: the driver and the executors run
    # this same image, hence the same versions.
    # .python-version is gitignored (absent in CI): compare with the tracked
    # Python of the CI workflow instead.
    ci = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert set(re.findall(r'python-version: "([^"]+)"', ci)) == {"3.12"}
    lock = (PROJECT_ROOT / "uv.lock").read_text()
    assert re.search(r'name = "pyspark"\nversion = "4\.3\.0"', lock)


def test_p3_2_final_image_runs_as_a_non_root_numeric_user():
    users = re.findall(r"^USER (\S+)$", _dockerfile(), re.MULTILINE)
    assert users[-1] == "50000"
    assert 'ENTRYPOINT ["/opt/project-entrypoint.sh"]' in _dockerfile()


def test_p3_2_uv_lock_is_respected():
    text = _dockerfile()
    syncs = re.findall(r"uv sync[^\n]*", text)
    assert syncs and all("--frozen" in s for s in syncs), syncs
    assert any("--no-dev" in s for s in syncs)
    assert "pip install" not in text
    assert "uv pip" not in text


def test_p3_2_openlineage_jar_is_checked_with_the_makefile_pin():
    text = _dockerfile()
    # No default: the Makefile is the only source of the version and digest.
    assert re.search(r"^ARG OPENLINEAGE_SPARK_VERSION$", text, re.MULTILINE)
    assert re.search(r"^ARG OPENLINEAGE_SPARK_SHA256$", text, re.MULTILINE)
    assert "sha256sum -c" in text
    assert "--proto '=https' --proto-redir '=https'" in text
    makefile = MAKEFILE.read_text()
    image_recipe = makefile.split("\nimage:", 1)[1].split("\n\n", 1)[0]
    assert "OPENLINEAGE_SPARK_VERSION=$(OPENLINEAGE_SPARK_VERSION)" in image_recipe
    assert "OPENLINEAGE_SPARK_SHA256=$(OPENLINEAGE_SPARK_SHA256)" in image_recipe


def test_p3_2_dbt_manifest_is_parsed_with_the_deployment_paths():
    args = _arg_defaults()
    env = {
        item["name"]: item["value"] for item in yaml.safe_load(AIRFLOW_VALUES.read_text())["env"]
    }
    assert args["LAKE_ROOT"] == env["LAKE_ROOT"]
    assert args["DBT_DUCKDB_PATH"] == env["DBT_DUCKDB_PATH"]
    assert "dbt parse" in _dockerfile()


def test_p3_2_nothing_secret_or_local_is_copied():
    text = _dockerfile()
    for line in re.findall(r"^COPY [^\n]*", text, re.MULTILINE):
        assert ".env" not in line, line
        assert "airflow.cfg" not in line, line
        assert "airflow.db" not in line, line
        assert " . " not in f"{line} ", f"{line}: copy explicit paths, not the whole context"


def test_p3_2_dockerignore_is_an_allow_list_without_secrets():
    lines = [
        line.strip()
        for line in DOCKERIGNORE.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert lines[0] == "*", "the first rule must exclude everything"
    allowed = {line[1:].rstrip("/") for line in lines if line.startswith("!")}
    for forbidden in (".env", "lake", "airflow.db", "airflow.cfg", "logs", ".venv", "data"):
        assert forbidden not in allowed, forbidden
    # Re-excluded inside the allowed directories.
    for pattern in (
        "**/.env",
        "transform/.venv/",
        "transform/*.duckdb",
        "**/__pycache__/",
        # SEC-8: secrets or local state that could sit in an allowed directory.
        "**/airflow.cfg",
        "**/*.generated",
        "**/*.db",
        "**/*.pem",
        "**/*.key",
        "**/*credentials*",
        "**/.user.yml",
    ):
        assert pattern in lines, pattern
    # C-5: the comment names the test that checks the real build context.
    assert "tests/integration/test_p3_2_image_context.py" in DOCKERIGNORE.read_text()


def test_p3_2_spark_entrypoint_is_vendored_unchanged():
    digest = hashlib.sha256((IMAGE_DIR / "spark-entrypoint.sh").read_bytes()).hexdigest()
    assert digest == SPARK_ENTRYPOINT_SHA256
    # Its provenance is written in the Dockerfile (the vendored file stays byte
    # for byte the upstream one, so its digest can be compared).
    dockerfile = _dockerfile()
    assert SPARK_DOCKER_COMMIT in dockerfile
    assert "4.3.0/scala2.13-java17-ubuntu/entrypoint.sh" in dockerfile
    assert SPARK_ENTRYPOINT_SHA256 in dockerfile


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (["driver", "--properties-file", "/opt/spark/conf/spark.properties"], "spark"),
        (["executor"], "spark"),
        (["bash", "-c", "exec airflow scheduler"], "airflow"),
        (["airflow", "version"], "airflow"),
        ([], "airflow"),
    ],
)
def test_p3_2_entrypoint_dispatches_spark_and_airflow(tmp_path, args, expected):
    """Run the dispatcher with both targets replaced by recorders."""
    script = (IMAGE_DIR / "entrypoint.sh").read_text()
    spark = tmp_path / "spark.sh"
    airflow = tmp_path / "airflow.sh"
    for path, name in ((spark, "spark"), (airflow, "airflow")):
        path.write_text(f'#!/bin/bash\necho {name} "$@"\n')
        path.chmod(0o755)
    # dumb-init is replaced by a script that drops its `--`.
    dumb_init = tmp_path / "dumb-init"
    dumb_init.write_text(f'#!/bin/bash\nshift\nexec {airflow} "$@"\n')
    dumb_init.chmod(0o755)
    patched = script.replace("/opt/spark-k8s/entrypoint.sh", str(spark)).replace(
        "/usr/bin/dumb-init", str(dumb_init)
    )
    entry = tmp_path / "entrypoint.sh"
    entry.write_text(patched)
    entry.chmod(0o755)

    out = subprocess.run([str(entry), *args], capture_output=True, text=True, check=True).stdout
    if expected == "spark":
        # Spark's entrypoint gets `driver ...` / `executor ...` unchanged.
        assert out.strip() == " ".join(["spark", *args])
    else:
        # The Airflow image's entrypoint, under dumb-init, with the arguments.
        assert out.strip() == " ".join(["airflow", "/entrypoint", *args])


def test_p3_2_sonar_uv_sync_without_no_build_is_justified():
    # Sonar docker:S8541 on PR #30: `uv sync` builds the sdist-only packages of
    # the locks. Each such sync stays --frozen (hashes of the lock) and says why.
    dockerfile = _dockerfile()
    syncs = re.findall(r"((?:#[^\n]*\n)*)RUN [^\n]*\\\n\s*(uv sync [^\n]*)", dockerfile)
    assert len(syncs) == 2, syncs
    for comment, command in syncs:
        assert "--frozen" in command, command
        assert "--no-build" not in command or "--no-build-package" in command
        assert "docker:S8541" in comment, command
