"""P3-2: scripts/k8s_secrets.py creates the Secrets of the kind deployment.

A fake kubectl records the calls: no cluster is needed. Checked: the generated
Secrets are created once and then kept (idempotent), the derived ones are
rebuilt from them, no value ever reaches a command line or the script's output,
and the names agree with the charts' values.
"""

import base64
import importlib.util
import json
import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SCRIPT = PROJECT_ROOT / "scripts" / "k8s_secrets.py"
PROJECT_VALUES = PROJECT_ROOT / "deploy" / "k8s" / "charts" / "project" / "values.yaml"
AIRFLOW_VALUES = PROJECT_ROOT / "deploy" / "k8s" / "values-airflow.yaml"


def _load():
    spec = importlib.util.spec_from_file_location("k8s_secrets", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    # dataclasses resolve the module through sys.modules.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ks = _load()


class FakeKubectl:
    """In-memory Secrets of one namespace, driven like kubectl."""

    def __init__(self, existing=None):
        self.secrets = {name: dict(data) for name, data in (existing or {}).items()}
        self.calls: list[tuple[list[str], str | None]] = []

    def __call__(self, args, stdin):
        self.calls.append((args, stdin))
        verb = args[5]
        if verb == "get":
            name = args[7]
            if name not in self.secrets:
                return ks.Result(1, "", f'Error from server (NotFound): secrets "{name}" not found')
            encoded = {
                k: base64.b64encode(v.encode()).decode() for k, v in self.secrets[name].items()
            }
            return ks.Result(0, json.dumps({"data": encoded}), "")
        manifest = json.loads(stdin)
        name = manifest["metadata"]["name"]
        decoded = {k: base64.b64decode(v).decode() for k, v in manifest["data"].items()}
        if verb == "create":
            if name in self.secrets:
                return ks.Result(1, "", f'secrets "{name}" already exists')
            self.secrets[name] = decoded
        elif verb == "apply":
            assert "--server-side" in args
            self.secrets[name] = decoded
        return ks.Result(0, "", "")


def _client(fake, namespace="airflow-project-one"):
    return ks.Secrets("kubectl", "kind-test", namespace, runner=fake)


def test_p3_2_first_run_creates_every_secret():
    fake = FakeKubectl()
    lines: list[str] = []
    ks.ensure_secrets(_client(fake), log=lines.append)

    assert set(fake.secrets) == set(ks.GENERATED) | set(ks.DERIVED_SECRETS)
    for name, keys in ks.GENERATED.items():
        assert set(fake.secrets[name]) == set(keys)
        assert all(len(value) >= 32 for value in fake.secrets[name].values())
    # Every command targets the given context and namespace.
    for args, _ in fake.calls:
        assert args[:5] == [
            "kubectl",
            "--context",
            "kind-test",
            "--namespace",
            "airflow-project-one",
        ]


def test_p3_2_values_never_reach_a_command_line_or_the_output():
    fake = FakeKubectl()
    lines: list[str] = []
    ks.ensure_secrets(_client(fake), log=lines.append)

    values = [v for data in fake.secrets.values() for v in data.values()]
    output = "\n".join(lines)
    for args, _stdin in fake.calls:
        command = " ".join(args)
        for value in values:
            assert value not in command
    for name in ks.GENERATED:
        for value in fake.secrets[name].values():
            assert value not in output
    # No client-side apply: it would copy the values into an annotation.
    for args, _ in fake.calls:
        if args[5] == "apply":
            assert "--server-side" in args


def test_p3_2_second_run_keeps_the_generated_secrets():
    fake = FakeKubectl()
    ks.ensure_secrets(_client(fake), log=lambda _: None)
    first = json.loads(json.dumps(fake.secrets))

    lines: list[str] = []
    ks.ensure_secrets(_client(fake), log=lines.append)

    assert fake.secrets == first
    assert sum("kept" in line for line in lines) == len(ks.GENERATED)
    assert not any(
        args[5] == "create"
        for args, _ in fake.calls[-(len(ks.GENERATED) + len(ks.DERIVED_SECRETS)) :]
    )


def test_p3_2_derived_secrets_follow_the_generated_ones():
    fake = FakeKubectl()
    ks.ensure_secrets(_client(fake, namespace="ns-x"), log=lambda _: None)

    airflow_password = fake.secrets[ks.POSTGRES_SECRET]["airflow-password"]
    file_list_password = fake.secrets[ks.FILE_LIST_SECRET]["password"]
    assert fake.secrets[ks.METADATA_SECRET]["connection"] == (
        f"postgresql://airflow:{airflow_password}@project-postgres:5432/airflow"
    )
    connections = fake.secrets[ks.CONNECTIONS_SECRET]
    file_list = json.loads(connections["AIRFLOW_CONN_FILE_LIST_DB"])
    assert file_list == {
        "conn_type": "postgres",
        "host": "project-postgres",
        "port": 5432,
        "login": "file_list",
        "password": file_list_password,
        "schema": "file_list",
    }
    spark = json.loads(connections["AIRFLOW_CONN_SPARK_DEFAULT"])
    assert spark == {
        "conn_type": "spark",
        "host": "k8s://https://kubernetes.default.svc",
        "port": 443,
        "extra": {"deploy-mode": "cluster", "namespace": "ns-x"},
    }


def test_p3_2_spark_connection_resolves_to_the_in_cluster_master(monkeypatch):
    """The provider's hook turns the JSON Connection into the k8s master URL."""
    from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook

    spark = ks.derived_secrets("ns-x", _generated())[ks.CONNECTIONS_SECRET]
    monkeypatch.setenv("AIRFLOW_CONN_SPARK_DEFAULT", spark["AIRFLOW_CONN_SPARK_DEFAULT"])
    hook = SparkSubmitHook(conn_id="spark_default")
    assert hook._connection["master"] == "k8s://https://kubernetes.default.svc:443"
    assert hook._connection["deploy_mode"] == "cluster"
    assert hook._connection["namespace"] == "ns-x"
    assert hook._is_kubernetes


def _generated():
    return {name: {key: "x" * 40 for key in keys} for name, keys in ks.GENERATED.items()}


def test_p3_2_a_secret_without_its_key_is_an_error_never_overwritten():
    fake = FakeKubectl({ks.FILE_LIST_SECRET: {"other": "v"}})
    with pytest.raises(ks.SecretError) as exc_info:
        ks.ensure_secrets(_client(fake), log=lambda _: None)
    assert "password" in str(exc_info.value)
    assert fake.secrets[ks.FILE_LIST_SECRET] == {"other": "v"}


def test_p3_2_kubectl_failure_is_reported(monkeypatch, capsys):
    def failing(args, stdin):
        return ks.Result(1, "", "error: You must be logged in to the server (Unauthorized)")

    monkeypatch.setattr(ks, "run_kubectl", failing)
    monkeypatch.setattr(ks.Secrets.__init__, "__defaults__", (failing,))
    assert ks.main(["--context", "kind-x", "--namespace", "ns"]) == 1
    assert "Unauthorized" in capsys.readouterr().err


def test_p3_2_fernet_key_has_the_fernet_format():
    key = ks._fernet_key()
    assert len(base64.urlsafe_b64decode(key)) == 32
    from cryptography.fernet import Fernet

    Fernet(key)  # raises on a malformed key


def test_p3_2_secret_names_match_the_charts():
    project = yaml.safe_load(PROJECT_VALUES.read_text())
    airflow = yaml.safe_load(AIRFLOW_VALUES.read_text())
    assert project["postgres"]["secretName"] == ks.POSTGRES_SECRET
    assert project["postgres"]["serviceName"] == ks.POSTGRES_HOST
    assert project["fileListDb"]["secretName"] == ks.FILE_LIST_SECRET
    assert project["fileListDb"]["secretKey"] == "password"
    assert project["fileListDb"]["name"] == ks.FILE_LIST_DB
    assert project["fileListDb"]["user"] == ks.FILE_LIST_USER
    assert project["fileListDb"]["port"] == ks.POSTGRES_PORT
    assert airflow["data"]["metadataSecretName"] == ks.METADATA_SECRET
    assert airflow["fernetKeySecretName"] in ks.GENERATED
    assert airflow["apiSecretKeySecretName"] in ks.GENERATED
    assert airflow["jwtSecretName"] in ks.GENERATED
    worker_env = {e["name"]: e for e in airflow["workers"]["kubernetes"]["env"]}
    for name in ("AIRFLOW_CONN_FILE_LIST_DB", "AIRFLOW_CONN_SPARK_DEFAULT"):
        ref = worker_env[name]["valueFrom"]["secretKeyRef"]
        assert ref == {"name": ks.CONNECTIONS_SECRET, "key": name}
    volumes = {v["name"]: v for v in airflow["apiServer"]["extraVolumes"]}
    assert volumes["ui-passwords-secret"]["secret"]["secretName"] == ks.UI_PASSWORDS_SECRET
    # Connection ids of the DAGs.
    from utils.functions_utils import FILE_LIST_POSTGRES_CONN_ID

    assert (
        f"AIRFLOW_CONN_{FILE_LIST_POSTGRES_CONN_ID.upper()}"
        in ks.derived_secrets("ns", _generated())[ks.CONNECTIONS_SECRET]
    )


# --- C-8: kubectl errors -----------------------------------------------------------------


@pytest.mark.parametrize(
    "error", [FileNotFoundError(2, "No such file"), PermissionError(13, "denied")]
)
def test_p3_2_kubectl_that_cannot_run_is_a_secret_error(error, capsys):
    def broken(args, stdin):
        raise error

    with pytest.raises(ks.SecretError) as exc_info:
        ks.ensure_secrets(ks.Secrets("/no/kubectl", "kind-x", "ns", runner=broken), log=print)
    assert "kubectl" in str(exc_info.value)


def test_p3_2_kubectl_missing_makes_main_fail_cleanly(capsys):
    assert ks.main(["--kubectl", "/nonexistent/kubectl", "--context", "c", "--namespace", "n"]) == 1
    err = capsys.readouterr().err
    assert err.startswith("error: ") and "Traceback" not in err


@pytest.mark.parametrize(
    "stderr",
    [
        # Another object not found (the namespace, a typo in the resource...):
        # not "this Secret does not exist".
        'Error from server (NotFound): namespaces "ns" not found',
        'Error from server (NotFound): secrets "file-list-db-other" not found',
        'error: the server doesn\'t have a resource type "secret" (not found)',
    ],
)
def test_p3_2_only_the_secret_itself_not_found_means_absent(stderr):
    def runner(args, stdin):
        return ks.Result(1, "", stderr)

    client = ks.Secrets("kubectl", "kind-x", "ns", runner=runner)
    with pytest.raises(ks.SecretError):
        client.read(ks.FILE_LIST_SECRET)


def test_p3_2_secret_not_found_is_absent():
    def runner(args, stdin):
        return ks.Result(1, "", f'Error from server (NotFound): secrets "{args[7]}" not found')

    assert ks.Secrets("kubectl", "kind-x", "ns", runner=runner).read(ks.FILE_LIST_SECRET) is None


# --- SEC-3 / C-9: password of the UI user, generated once --------------------------------


def test_p3_2_ui_password_file_follows_the_generated_password():
    fake = FakeKubectl()
    ks.ensure_secrets(_client(fake), log=lambda _: None)
    password = fake.secrets[ks.UI_ADMIN_SECRET]["password"]
    assert len(password) >= 32
    passwords = json.loads(fake.secrets[ks.UI_PASSWORDS_SECRET]["passwords.json"])
    assert passwords == {"admin": password}
    # Stable across runs.
    ks.ensure_secrets(_client(fake), log=lambda _: None)
    assert fake.secrets[ks.UI_ADMIN_SECRET]["password"] == password
