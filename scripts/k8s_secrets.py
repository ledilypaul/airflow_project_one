"""Create the Kubernetes Secrets of the dev deployment on kind (P3-2).

Called by ``make k8s-install`` before the two Helm releases. No secret is ever
written in a values file, in git, on a command line or in this script's output:
values are generated here (``secrets`` module), held in memory, and sent to
``kubectl`` on its standard input.

Two kinds of Secrets:

- **Generated once** (``GENERATED``): passwords of Postgres (superuser, role
  ``airflow``), of the ``file_list`` role and of the UI user ``admin``, and
  Airflow's Fernet key, API secret key and JWT secret. An existing Secret is
  kept as is (Postgres applies its passwords only when it creates its data
  directory, as the docker-compose volume does), so the script is idempotent.
  One that lacks a key is an error, never overwritten.
- **Derived** (``derived_secrets``), rebuilt on every run from the generated
  ones and from the namespace, and applied server-side:
  - ``airflow-metadata`` (key ``connection``): the metadata DB URL, read by the
    Airflow chart (``data.metadataSecretName``);
  - ``airflow-ui-passwords`` (key ``passwords.json``): the password of the UI
    user ``admin`` (generated in ``airflow-ui-admin``), as the Simple Auth
    Manager's passwords file, copied by an init container of the api-server;
  - ``airflow-connections``: ``AIRFLOW_CONN_FILE_LIST_DB`` (postgres) and
    ``AIRFLOW_CONN_SPARK_DEFAULT`` (master
    ``k8s://https://kubernetes.default.svc:443``, ``deploy-mode`` cluster, this
    namespace), as JSON (``json.dumps``, never by interpolation), loaded as
    environment variables by the worker pods only (``workers.kubernetes.env``).

Secrets are created with ``kubectl create`` and updated with ``kubectl apply
--server-side``: a client-side ``apply`` would copy the whole manifest, values
included, into the ``last-applied-configuration`` annotation.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import secrets
import subprocess
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass

# Same values as deploy/k8s/charts/project/values.yaml and
# deploy/k8s/values-airflow.yaml (checked by tests/unit/test_p3_2_k8s_secrets.py).
POSTGRES_SECRET = "project-postgres"
FILE_LIST_SECRET = "file-list-db"
METADATA_SECRET = "airflow-metadata"
CONNECTIONS_SECRET = "airflow-connections"
# SEC-3: password of the UI user `admin` (Simple Auth Manager), generated here
# rather than by the api-server, which would log it; the api-server reads the
# JSON file of the second Secret (simple_auth_manager_passwords_file).
UI_ADMIN_SECRET = "airflow-ui-admin"
UI_PASSWORDS_SECRET = "airflow-ui-passwords"
UI_USER = "admin"
POSTGRES_HOST = "project-postgres"
POSTGRES_PORT = 5432
FILE_LIST_DB = "file_list"
FILE_LIST_USER = "file_list"
# In-cluster API server: the master of spark-submit run by the worker pods.
SPARK_K8S_MASTER = "k8s://https://kubernetes.default.svc"
SPARK_K8S_PORT = 443
FIELD_MANAGER = "airflow-project-one"
LABELS = {
    "app.kubernetes.io/part-of": "airflow-project-one",
    "app.kubernetes.io/managed-by": "k8s_secrets.py",
}


def _password() -> str:
    # URL-safe alphabet: usable as is in a SQLAlchemy URL.
    return secrets.token_urlsafe(32)


def _fernet_key() -> str:
    # The format of cryptography.fernet.Fernet.generate_key().
    return base64.urlsafe_b64encode(os.urandom(32)).decode()


def _token() -> str:
    return secrets.token_urlsafe(48)


# Secret name -> {key: generator}. Generated once, then kept.
GENERATED: dict[str, dict[str, Callable[[], str]]] = {
    POSTGRES_SECRET: {"postgres-password": _password, "airflow-password": _password},
    FILE_LIST_SECRET: {"password": _password},
    "airflow-fernet-key": {"fernet-key": _fernet_key},
    "airflow-api-secret-key": {"api-secret-key": _token},
    "airflow-jwt-secret": {"jwt-secret": _token},
    UI_ADMIN_SECRET: {"password": _password},
}


class SecretError(Exception):
    """A Secret cannot be used as is. The message never contains a value."""


@dataclass
class Result:
    returncode: int
    stdout: str
    stderr: str


Runner = Callable[[list[str], str | None], Result]


def run_kubectl(args: list[str], stdin: str | None) -> Result:
    proc = subprocess.run(args, input=stdin, capture_output=True, text=True, check=False)
    return Result(proc.returncode, proc.stdout, proc.stderr)


def secret_manifest(name: str, namespace: str, data: Mapping[str, str]) -> str:
    """JSON manifest of an Opaque Secret (values base64-encoded in `data`)."""
    return json.dumps(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "Opaque",
            "metadata": {"name": name, "namespace": namespace, "labels": LABELS},
            "data": {key: base64.b64encode(value.encode()).decode() for key, value in data.items()},
        }
    )


def derived_secrets(namespace: str, values: Mapping[str, Mapping[str, str]]) -> dict:
    """Secrets built from the generated ones: {name: {key: value}}."""
    airflow_password = values[POSTGRES_SECRET]["airflow-password"]
    file_list_password = values[FILE_LIST_SECRET]["password"]
    file_list_db = {
        "conn_type": "postgres",
        "host": POSTGRES_HOST,
        "port": POSTGRES_PORT,
        "login": FILE_LIST_USER,
        "password": file_list_password,
        "schema": FILE_LIST_DB,
    }
    spark_default = {
        "conn_type": "spark",
        "host": SPARK_K8S_MASTER,
        "port": SPARK_K8S_PORT,
        "extra": {"deploy-mode": "cluster", "namespace": namespace},
    }
    return {
        METADATA_SECRET: {
            "connection": (
                f"postgresql://airflow:{airflow_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/airflow"
            )
        },
        CONNECTIONS_SECRET: {
            "AIRFLOW_CONN_FILE_LIST_DB": json.dumps(file_list_db),
            "AIRFLOW_CONN_SPARK_DEFAULT": json.dumps(spark_default),
        },
        UI_PASSWORDS_SECRET: {
            "passwords.json": json.dumps({UI_USER: values[UI_ADMIN_SECRET]["password"]})
        },
    }


# Names of the Secrets derived_secrets() builds.
DERIVED_SECRETS = (METADATA_SECRET, CONNECTIONS_SECRET, UI_PASSWORDS_SECRET)


class Secrets:
    def __init__(self, kubectl: str, context: str, namespace: str, runner: Runner = run_kubectl):
        self.base = [kubectl, "--context", context, "--namespace", namespace]
        self.namespace = namespace
        self.runner = runner

    def _call(self, args: list[str], stdin: str | None) -> Result:
        try:
            return self.runner(self.base + args, stdin)
        except OSError as exc:
            # kubectl missing or not executable: a clean error, no traceback.
            raise SecretError(f"cannot run {self.base[0]!r}: {exc.strerror or exc}") from exc

    def _run(self, args: list[str], stdin: str | None = None) -> Result:
        result = self._call(args, stdin)
        if result.returncode != 0:
            # kubectl's own error (names, reasons), never the data sent on stdin.
            raise SecretError(f"kubectl {args[0]} failed: {result.stderr.strip()}")
        return result

    def read(self, name: str) -> dict[str, str] | None:
        """Decoded data of Secret `name`, or None when it does not exist."""
        result = self._call(["get", "secret", name, "-o", "json"], None)
        if result.returncode != 0:
            # Only this Secret missing means "absent": a missing namespace or
            # any other NotFound is an error (C-8).
            if f'secrets "{name}" not found' in result.stderr:
                return None
            raise SecretError(f"kubectl get secret {name} failed: {result.stderr.strip()}")
        data = json.loads(result.stdout).get("data") or {}
        return {key: base64.b64decode(value).decode() for key, value in data.items()}

    def create(self, name: str, data: Mapping[str, str]) -> None:
        self._run(["create", "-f", "-"], secret_manifest(name, self.namespace, data))

    def apply(self, name: str, data: Mapping[str, str]) -> None:
        self._run(
            ["apply", "--server-side", f"--field-manager={FIELD_MANAGER}", "--force-conflicts"]
            + ["-f", "-"],
            secret_manifest(name, self.namespace, data),
        )


def ensure_secrets(client: Secrets, log: Callable[[str], None] = print) -> None:
    values: dict[str, dict[str, str]] = {}
    for name, keys in GENERATED.items():
        existing = client.read(name)
        if existing is None:
            values[name] = {key: generate() for key, generate in keys.items()}
            client.create(name, values[name])
            log(f"secret/{name}: created (generated values)")
            continue
        missing = sorted(set(keys) - set(existing))
        if missing:
            raise SecretError(
                f"secret/{name} exists without the key(s) {missing}: delete it to "
                "regenerate it (a Postgres password also needs a new data volume)"
            )
        values[name] = existing
        log(f"secret/{name}: kept")
    for name, data in derived_secrets(client.namespace, values).items():
        client.apply(name, data)
        log(f"secret/{name}: applied (derived)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--kubectl", default="kubectl")
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args(argv)
    try:
        ensure_secrets(Secrets(args.kubectl, args.context, args.namespace))
    except SecretError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
