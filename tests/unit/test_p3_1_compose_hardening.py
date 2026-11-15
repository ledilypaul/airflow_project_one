"""
P3-1 (audit S3) - hardening of the `postgres` and `redis` services of
docker-compose.yaml (the two `make db-up` starts).

- host ports bound to 127.0.0.1 only;
- Redis requires a password, read from the environment, never written in the file;
- Postgres credentials come from the same DB_* variables as its clients;
- the healthchecks authenticate the same way, and a NOAUTH reply is not healthy;
- the obsolete `version:` key is gone.

The Marquez services have their own guards in test_p2_5_lineage_setup.py.
"""

import re
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yaml"
# ${NAME} or ${NAME:-fallback}: never a bare literal.
INTERPOLATED = re.compile(r"\$\{(?P<name>[A-Z_]+)(?::-(?P<default>[^}]*))?\}")


def _compose() -> dict:
    return yaml.safe_load(COMPOSE_FILE.read_text())


def _service(name: str) -> dict:
    return _compose()["services"][name]


def _variable(value: str) -> str:
    match = INTERPOLATED.fullmatch(str(value))
    assert match, f"{value!r} must be a ${{VAR:-default}} reference, not a literal"
    return match.group("name")


def test_p3_1_obsolete_version_key_is_removed():
    # Compose v2 ignores it and prints a warning on every command.
    assert "version" not in _compose()


@pytest.mark.parametrize(
    ("service", "host_port", "container_port"),
    [("postgres", 5434, 5432), ("redis", 6379, 6379)],
)
def test_p3_1_s3_ports_are_bound_to_localhost(service, host_port, container_port):
    # Exactly the 127.0.0.1:<host>:<container> form: a bare
    # "5434:5432" listens on every interface.
    assert _service(service)["ports"] == [f"127.0.0.1:{host_port}:{container_port}"]


def test_p3_1_s3_no_published_port_listens_on_every_interface():
    for name, service in _compose()["services"].items():
        for port in service.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), (name, port)


def test_p3_1_s3_redis_requires_a_password_that_is_not_in_the_file():
    redis = _service("redis")
    command = redis["command"]
    assert "redis-server" in " ".join(command)
    assert "--requirepass" in " ".join(command)
    # The server reads the password from its environment (`$$` is compose's
    # escape for a `$` left to the container's shell).
    assert '"$$REDIS_PASSWORD"' in command[-1]
    assert _variable(redis["environment"]["REDIS_PASSWORD"]) == "REDIS_PASSWORD"
    # An empty value would start Redis without authentication.
    default = INTERPOLATED.fullmatch(redis["environment"]["REDIS_PASSWORD"]).group("default")
    assert default, "the dev fallback of REDIS_PASSWORD must not be empty"
    assert default not in " ".join(command)


def test_p3_1_s3_redis_healthcheck_authenticates_without_a_password_on_the_command_line():
    redis = _service("redis")
    # Same variable as the server, so the check follows a password change.
    assert redis["environment"]["REDISCLI_AUTH"] == redis["environment"]["REDIS_PASSWORD"]
    test = redis["healthcheck"]["test"]
    joined = " ".join(test)
    assert "redis-cli" in joined and "ping" in joined
    assert "-a " not in joined and "--pass" not in joined
    # `redis-cli ping` answers NOAUTH with exit code 0: only PONG is healthy.
    assert "grep -q PONG" in joined


def test_p3_1_s3_postgres_credentials_follow_the_projects_db_variables():
    environment = _service("postgres")["environment"]
    assert _variable(environment["POSTGRES_USER"]) == "DB_USER"
    assert _variable(environment["POSTGRES_PASSWORD"]) == "DB_PASSWORD"
    assert _variable(environment["POSTGRES_DB"]) == "DB_NAME"
    # Fallbacks keep `make db-up` working without a `.env`: the previous values.
    defaults = {
        key: INTERPOLATED.fullmatch(value).group("default") for key, value in environment.items()
    }
    assert defaults == {
        "POSTGRES_USER": "airflow",
        "POSTGRES_PASSWORD": "airflow",
        "POSTGRES_DB": "airflow",
    }


def test_p3_1_s3_postgres_healthcheck_does_not_hard_code_the_user():
    test = " ".join(_service("postgres")["healthcheck"]["test"])
    assert "pg_isready" in test
    assert "$$POSTGRES_USER" in test and "$$POSTGRES_DB" in test
    assert "-U airflow" not in test


def test_p3_1_makefile_still_matches_the_compose_postgres():
    # db-port-check compares `docker ps --filter publish=<port>` with the
    # container name: both come from docker-compose.yaml.
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    port = re.search(r"^PG_HOST_PORT := (\d+)$", makefile, re.MULTILINE)
    container = re.search(r"^PG_CONTAINER := (\S+)$", makefile, re.MULTILINE)
    postgres = _service("postgres")
    assert container and postgres["container_name"] == container.group(1)
    assert port and postgres["ports"][0].split(":")[1] == port.group(1)
