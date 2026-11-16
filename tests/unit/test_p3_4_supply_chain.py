"""
P3-4 - supply chain: guards on what lives in `.github/` and `.gitleaksignore`.

- every workflow's GITHUB_TOKEN is read-only at workflow level, and a job that
  needs more asks for it itself, with a short allow-list;
- every `uses:` of a third-party action is pinned to a full 40-character commit
  SHA, followed by a `# vX.Y.Z` comment;
- Dependabot covers the expected ecosystems (uv at the root AND in transform/,
  github-actions, docker-compose), weekly, never merges on its own, and keeps the
  version ceilings of CLAUDE.md out of its proposals;
- the CI runs pip-audit on both locks, gitleaks on the whole history and Trivy
  with an explicit threshold;
- `.gitleaksignore` only lists `commit:file:rule:line` fingerprints, never a value.

The workflows themselves can only run on GitHub: these tests read the YAML.
"""

import re
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
WORKFLOWS_DIR = PROJECT_ROOT / ".github" / "workflows"
DEPENDABOT = PROJECT_ROOT / ".github" / "dependabot.yml"
GITLEAKS_IGNORE = PROJECT_ROOT / ".gitleaksignore"

FULL_SHA = re.compile(r"[0-9a-f]{40}")
USES_LINE = re.compile(r"^\s*-?\s*uses:\s*(?P<ref>\S+)(?P<rest>.*)$")
VERSION_COMMENT = re.compile(r"#\s*v\d+(\.\d+)*")

WORKFLOW_FILES = sorted(WORKFLOWS_DIR.glob("*.y*ml"))
# (workflow file, job) -> permissions it may add to `contents: read`.
ALLOWED_JOB_PERMISSIONS = {
    ("ci.yml", "gitleaks"): {"pull-requests": "read"},
    ("openlineage-jar-watch.yml", "watch"): {"issues": "write"},
}


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def _updates() -> list[dict]:
    return _load(DEPENDABOT)["updates"]


def _update(ecosystem: str, directory: str) -> dict:
    matches = [
        u for u in _updates() if u["package-ecosystem"] == ecosystem and u["directory"] == directory
    ]
    assert len(matches) == 1, f"expected one {ecosystem!r} entry for {directory!r}"
    return matches[0]


def _ignored(update: dict) -> dict[str, set[str]]:
    return {
        i["dependency-name"]: set(i.get("update-types", ["*"])) for i in update.get("ignore", [])
    }


def test_p3_4_workflows_exist():
    names = {p.name for p in WORKFLOW_FILES}
    assert {"ci.yml", "openlineage-jar-watch.yml"} <= names


@pytest.mark.parametrize("workflow", WORKFLOW_FILES, ids=lambda p: p.name)
def test_p3_4_workflow_token_is_read_only(workflow):
    data = _load(workflow)
    assert data.get("permissions") == {"contents": "read"}, (
        f"{workflow.name}: top-level `permissions: contents: read` is required"
    )
    for job_id, job in data["jobs"].items():
        job_permissions = job.get("permissions")
        if job_permissions is None:
            continue
        extra = {k: v for k, v in job_permissions.items() if k != "contents"}
        assert job_permissions.get("contents") == "read", f"{workflow.name}:{job_id}"
        assert extra == ALLOWED_JOB_PERMISSIONS.get((workflow.name, job_id), {}), (
            f"{workflow.name}:{job_id} asks for permissions beyond the allow-list"
        )


@pytest.mark.parametrize("workflow", WORKFLOW_FILES, ids=lambda p: p.name)
def test_p3_4_every_action_is_pinned_by_full_sha(workflow):
    found = 0
    for number, line in enumerate(workflow.read_text().splitlines(), start=1):
        match = USES_LINE.match(line)
        if not match:
            continue
        found += 1
        ref = match.group("ref")
        where = f"{workflow.name}:{number} ({ref})"
        assert not ref.startswith("./"), f"{where}: local actions are not expected here"
        assert "@" in ref, f"{where}: no version pin"
        sha = ref.split("@", 1)[1]
        assert FULL_SHA.fullmatch(sha), f"{where}: pin to a 40-character commit SHA, not a tag"
        assert VERSION_COMMENT.search(match.group("rest")), (
            f"{where}: keep the `# vX.Y.Z` comment, Dependabot and reviewers read it"
        )
    assert found, f"{workflow.name} uses no action: is the pattern still right?"


def test_p3_4_ci_has_the_security_jobs():
    jobs = _load(WORKFLOWS_DIR / "ci.yml")["jobs"]
    assert {"test", "pip-audit", "trivy", "gitleaks"} <= set(jobs)
    # the scheduled run is for the security jobs: `test` is skipped there
    assert "schedule" in _load(WORKFLOWS_DIR / "ci.yml")[True]
    assert "schedule" in jobs["test"]["if"]


def test_p3_4_pip_audit_covers_both_locks_and_is_blocking():
    job = _load(WORKFLOWS_DIR / "ci.yml")["jobs"]["pip-audit"]
    steps = "\n".join(str(step.get("run", "")) for step in job["steps"])
    assert "uv export --frozen" in steps
    assert "uv --directory transform export --frozen" in steps
    assert steps.count("pip-audit -r") == 2
    assert "|| true" not in steps
    assert not any(step.get("continue-on-error") for step in job["steps"])
    assert "continue-on-error" not in job


def test_p3_4_gitleaks_scans_the_whole_history():
    steps = _load(WORKFLOWS_DIR / "ci.yml")["jobs"]["gitleaks"]["steps"]
    checkout = next(s for s in steps if s["uses"].startswith("actions/checkout@"))
    assert checkout["with"]["fetch-depth"] == 0
    assert any(s["uses"].startswith("gitleaks/gitleaks-action@") for s in steps)
    assert not any(s.get("continue-on-error") for s in steps)


def test_p3_4_trivy_has_an_explicit_threshold():
    steps = _load(WORKFLOWS_DIR / "ci.yml")["jobs"]["trivy"]["steps"]
    scan = next(s for s in steps if s["uses"].startswith("aquasecurity/trivy-action@"))
    options = scan["with"]
    assert options["scan-type"] == "fs"
    assert options["severity"] == "HIGH,CRITICAL"
    assert options["ignore-unfixed"] is True
    # explicit, not the action's default (which would drift with its release)
    assert re.fullmatch(r"v\d+\.\d+\.\d+", options["version"])
    # Non-blocking until a first report has been read (see the comment in ci.yml).
    # Turning it blocking means changing this value on purpose.
    assert options["exit-code"] == "0"


def test_p3_4_dependabot_covers_the_expected_ecosystems():
    updates = _updates()
    ecosystems = {(u["package-ecosystem"], u["directory"]) for u in updates}
    assert ecosystems == {
        ("uv", "/"),
        ("uv", "/transform"),
        ("github-actions", "/"),
        ("docker-compose", "/"),
    }
    assert _load(DEPENDABOT)["version"] == 2


def test_p3_4_dependabot_directories_hold_what_it_updates():
    # one lock per uv entry (transform/ has its own), the compose file by its real name
    assert (PROJECT_ROOT / "uv.lock").is_file()
    assert (PROJECT_ROOT / "transform" / "uv.lock").is_file()
    assert (PROJECT_ROOT / "docker-compose.yaml").is_file()
    assert any(WORKFLOWS_DIR.iterdir())


def test_p3_4_dependabot_is_weekly_and_never_merges_itself():
    for update in _updates():
        assert update["schedule"]["interval"] == "weekly", update["package-ecosystem"]
    text = DEPENDABOT.read_text()
    assert "automerge" not in text.lower()
    assert "auto-merge" not in text.lower()
    # no workflow approves or merges Dependabot's pull requests either
    for workflow in WORKFLOW_FILES:
        body = workflow.read_text()
        assert "gh pr merge" not in body and "--auto" not in body, workflow.name


def test_p3_4_dependabot_keeps_the_project_version_ceilings():
    root = _ignored(_update("uv", "/"))
    minor, major = "version-update:semver-minor", "version-update:semver-major"
    # apache-airflow==3.3.2 exact, cosmos <1.17, PySpark 4.3 tested: patches only
    for name in ("apache-airflow", "astronomer-cosmos", "pyspark"):
        assert {minor, major} <= root[name], name
    # pandas 3 breaks pyspark.pandas
    assert major in root["pandas"]
    transform = _ignored(_update("uv", "/transform"))
    assert major in transform["duckdb"]
    assert {minor, major} <= transform["dbt-core"]
    assert {minor, major} <= transform["dbt-duckdb"]


def test_p3_4_dependabot_ceilings_match_the_pins_of_the_project():
    # If one of these pins is relaxed in pyproject.toml, review the `ignore` rules too.
    pyproject = (PROJECT_ROOT / "pyproject.toml").read_text()
    assert '"apache-airflow==3.3.2"' in pyproject
    assert '"pandas>=2.2,<3"' in pyproject
    assert '"astronomer-cosmos>=1.16,<1.17"' in pyproject
    assert '"pyspark>=4.0,<4.4"' in pyproject


def test_p3_4_compose_databases_do_not_get_major_bumps():
    ignored = _ignored(_update("docker-compose", "/"))
    for image in ("postgres", "redis"):
        assert "version-update:semver-major" in ignored[image]


def test_p3_4_openlineage_jar_is_not_faked_as_a_dependabot_ecosystem():
    # No Dependabot ecosystem reads a Makefile pin: it is followed by the watch workflow.
    ecosystems = {u["package-ecosystem"] for u in _updates()}
    assert ecosystems.isdisjoint({"maven", "gradle", "docker", "pip"})
    watch = _load(WORKFLOWS_DIR / "openlineage-jar-watch.yml")
    assert "schedule" in watch[True]
    assert (PROJECT_ROOT / "scripts" / "openlineage_jar_watch.py").is_file()


def test_p3_4_gitleaksignore_lists_fingerprints_only():
    entries = [
        line
        for line in GITLEAKS_IGNORE.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert entries, "the ignore file is expected to carry the known historical findings"
    fingerprint = re.compile(r"[0-9a-f]{40}:[\w./-]+:[a-z0-9-]+:\d+")
    for entry in entries:
        assert fingerprint.fullmatch(entry), f"not a commit:file:rule:line fingerprint: {entry!r}"
    # airflow.cfg is no longer tracked and stays ignored: the findings are history only
    assert "airflow.cfg" in (PROJECT_ROOT / ".gitignore").read_text().splitlines()
