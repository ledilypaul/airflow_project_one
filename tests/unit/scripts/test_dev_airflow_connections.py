"""scripts/dev_airflow_connections.py (make airflow-init): env-file handling.

No Airflow metadata DB here: every case stops at the validation of the DB_*
variables, before Airflow is imported. The real .env is never read: each test
uses its own env file under tmp_path and an explicit (or cleared) environment.
The create-then-update path against a real metadata DB is exercised by
`make airflow-init` run twice (see CLAUDE.md).
"""

import ast
import importlib.util
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SCRIPT = PROJECT_ROOT / "scripts" / "dev_airflow_connections.py"

SECRET_PASSWORD = "s3cr3t-Do-Not-Print"
SECRET_USER = "secret_login_name"


def _load_script():
    spec = importlib.util.spec_from_file_location("dev_airflow_connections", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    # Registered before exec: @dataclass resolves the module's (postponed)
    # annotations through sys.modules.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


script = _load_script()


def _write_env(path: Path, **values: str) -> Path:
    path.write_text("".join(f"{k}={v}\n" for k, v in values.items()))
    return path


FULL = {
    "DB_HOST": "localhost",
    "DB_PORT": "5434",
    "DB_USER": SECRET_USER,
    "DB_PASSWORD": SECRET_PASSWORD,
    "DB_NAME": "airflow",
}


def test_reads_every_variable_from_the_env_file(tmp_path):
    env_file = _write_env(tmp_path / "dev.env", **FULL)

    db = script.load_db_settings(env_file, environ={})

    assert (db.host, db.port, db.database) == ("localhost", 5434, "airflow")
    assert (db.user, db.password) == (SECRET_USER, SECRET_PASSWORD)


def test_process_environment_wins_over_the_env_file(tmp_path):
    # Same precedence as load_dotenv() in utils/db_connection.py, so the
    # Connection targets the same database as the Spark job.
    env_file = _write_env(tmp_path / "dev.env", **FULL)

    db = script.load_db_settings(env_file, environ={"DB_PORT": "6543"})

    assert db.port == 6543


@pytest.mark.parametrize("missing", script.DB_VARS)
def test_missing_variable_is_named_and_no_value_leaks(tmp_path, missing):
    values = {k: v for k, v in FULL.items() if k != missing}
    env_file = _write_env(tmp_path / "dev.env", **values)

    with pytest.raises(script.ConfigError) as exc_info:
        script.load_db_settings(env_file, environ={})

    message = str(exc_info.value)
    assert missing in message
    for value in values.values():
        assert value not in message


def test_blank_value_counts_as_missing(tmp_path):
    env_file = _write_env(tmp_path / "dev.env", **{**FULL, "DB_HOST": "   "})

    with pytest.raises(script.ConfigError, match="DB_HOST"):
        script.load_db_settings(env_file, environ={})


def test_missing_env_file_lists_every_variable(tmp_path):
    with pytest.raises(script.ConfigError) as exc_info:
        script.load_db_settings(tmp_path / "absent.env", environ={})

    for name in script.DB_VARS:
        assert name in str(exc_info.value)


@pytest.mark.parametrize("bad_port", ["not-a-port", "0", "70000"])
def test_invalid_port_is_rejected_without_echoing_it(tmp_path, bad_port):
    env_file = _write_env(tmp_path / "dev.env", **{**FULL, "DB_PORT": bad_port})

    with pytest.raises(script.ConfigError, match="DB_PORT") as exc_info:
        script.load_db_settings(env_file, environ={})

    assert bad_port not in str(exc_info.value)


def test_repr_hides_credentials(tmp_path):
    db = script.load_db_settings(_write_env(tmp_path / "dev.env", **FULL), environ={})

    assert SECRET_PASSWORD not in repr(db)
    assert SECRET_USER not in repr(db)


def test_main_fails_on_missing_variable_without_printing_secrets(tmp_path, monkeypatch, capsys):
    for name in script.DB_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("ENV_FILE", raising=False)
    values = {k: v for k, v in FULL.items() if k != "DB_NAME"}
    env_file = _write_env(tmp_path / "dev.env", **values)

    rc = script.main(["--env-file", str(env_file)])

    out, err = capsys.readouterr()
    assert rc == 2
    assert "DB_NAME" in err
    assert SECRET_PASSWORD not in out + err
    assert SECRET_USER not in out + err


def test_env_file_variable_is_the_default(tmp_path, monkeypatch, capsys):
    for name in script.DB_VARS:
        monkeypatch.delenv(name, raising=False)
    env_file = _write_env(tmp_path / "from_env_var.env", DB_HOST="localhost")
    monkeypatch.setenv("ENV_FILE", str(env_file))

    rc = script.main([])

    _, err = capsys.readouterr()
    assert rc == 2
    # DB_HOST came from the file named by ENV_FILE: it is not reported missing.
    assert "missing variable(s) DB_PORT, DB_USER, DB_PASSWORD, DB_NAME " in err
    assert str(env_file) in err


def _string_constant(path: Path, name: str) -> str:
    """Value of a module-level `NAME = "..."`, without importing the module.

    Importing utils.functions_utils would run load_dotenv() on the real .env,
    and the DAG module needs a configured Airflow.
    """
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {path}")


def test_connection_ids_match_the_ones_used_by_the_pipeline():
    assert script.FILE_LIST_CONN_ID == _string_constant(
        PROJECT_ROOT / "utils" / "functions_utils.py", "FILE_LIST_POSTGRES_CONN_ID"
    )
    assert script.SPARK_CONN_ID == _string_constant(
        PROJECT_ROOT / "dags" / "local_read_file_dag.py", "SPARK_CONN_ID"
    )
