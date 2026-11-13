# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Dependencies are managed with **uv**. `pyproject.toml` (PEP 621) is the single source of truth and `uv.lock` is committed. Do not use pip, Poetry or `requirements.txt` to add dependencies.

```bash
uv sync                          # creates .venv with everything (runtime + airflow + dev groups)
uv sync --no-group airflow       # Spark-only environment (no Airflow)
uv add <pkg>                     # runtime dependency
uv add --group dev <pkg>         # dev tool
uv add --group airflow <pkg>     # Airflow provider
uv lock --upgrade-package <pkg>  # upgrade one package
uv export --no-hashes --no-dev -o requirements.txt   # only if a tool needs requirements.txt
```

The project is installed in editable mode in `.venv`, so `import pipeline.*` / `utils.*` / `storage.*` works without `PYTHONPATH`. Always prefix commands with `uv run` (or activate `.venv`).

Version pins worth knowing: `pandas<3` because pandas 3 breaks `pyspark.pandas` (verified with PySpark 4.3.0); `apache-airflow==3.3.2` exact.

### Tests

```bash
uv run pytest tests/                                            # full suite
uv run pytest tests/unit/file_reader/test_csv_reader.py          # one file
uv run pytest tests/unit/file_reader/test_reader_factory.py::test_factory_returns_csv_reader  # one test
uv run ruff check . && uv run ruff format --check .
```

DAG tests (`tests/unit/dags/`) don't need a real Airflow metadata DB or scheduler: `tests/unit/dags/conftest.py` sets `AIRFLOW__CORE__UNIT_TEST_MODE=True` and points `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` at a throwaway sqlite file before anything under `airflow.*` is imported.

### Running Airflow locally

The `Makefile` wraps the dev commands (`make` lists them). Quality targets mirror CI: `make check` = `uv lock --check`, then `make lint`, then `make test`.

```bash
make db-up          # docker-compose Postgres on host port 5434 (+ Redis); checks the port first
make airflow-init   # airflow db migrate + Connections spark_default/file_list_db + pools (idempotent)
make airflow        # airflow standalone, http://localhost:8080
```

The Airflow targets export `AIRFLOW_HOME=<project root>`, `AIRFLOW__CORE__DAGS_FOLDER=<project root>/dags` and `AIRFLOW__CORE__LOAD_EXAMPLES=False`. `AIRFLOW_HOME` must be the project root: when it is unset, Airflow silently reads `~/airflow/airflow.cfg`, i.e. another metadata DB and other settings. The metadata DB is then the SQLite file `./airflow.db` (gitignored), while the `file_list` table stays in the docker-compose Postgres. Running `airflow` by hand needs the same three variables. Both are overridable for a throwaway setup: `make airflow-init AIRFLOW_HOME=/tmp/af ENV_FILE=/tmp/dev.env`. The Makefile also drops `VIRTUAL_ENV` (an activated pyenv virtualenv makes `uv` warn that it does not match `.venv`).

`make airflow-init` runs `scripts/dev_airflow_connections.py`, which creates or updates (never duplicates) two Connections:
- `file_list_db` (postgres) from `DB_HOST`/`DB_PORT`/`DB_USER`/`DB_PASSWORD`/`DB_NAME`, read from `.env` (or `--env-file` / `ENV_FILE`; a variable already exported in the shell wins, as with `load_dotenv()`). The password goes through Airflow's ORM, never on a command line, and is never printed.
- `spark_default` (spark, host `local[*]`). It must be created with that conn type and host, not as a URI: `spark://local[*]` does not parse. Without touching the DB, `AIRFLOW_CONN_SPARK_DEFAULT='{"conn_type": "spark", "host": "local[*]"}'` also works.

Then it imports the pools versioned in `deploy/airflow/pools.json` (re-run it after editing the file). `read_file_dag` needs both the Spark Connection and the `spark` pool:

- **Connection `spark_default`.** Its `host` is the `--master` (`local[*]`, `yarn`, `spark://host:7077`, `k8s://...`). The extras `deploy-mode`, `queue`, `namespace` and `spark-binary` (one of `spark-submit`, `spark2-submit`, `spark3-submit`, found on the worker's `PATH`) are optional. If the Connection is **missing**, the provider itself does not fail: it logs `Could not load connection string spark_default, defaulting to yarn` at INFO level and runs `spark-submit --master yarn`, which fails locally ("HADOOP_CONF_DIR or YARN_CONF_DIR must be set"). That is why `resolve_source_run_ids` checks the Connection first and fails the run with an explicit `AirflowFailException` when it is missing (it logs the Connection's `host` otherwise). `airflow connections create-default-connections` also creates a `spark_default` with `host=yarn` and `queue=root.default`: `make airflow-init` overwrites it (host `local[*]`, extras cleared). Outside `ENV=prod`, `utils/spark_session.py` forces `master("local[*]")` in the job itself, so in dev the Connection's master only decides how `spark-submit` launches the driver.
- **Pool `spark`.** One slot locally: each `local[*]` application uses every core of the machine and starts its own driver JVM. Two at once just compete for the same CPU and RAM. Raise it to the number of applications your cluster can run side by side. A **missing pool is silent**: the DAG still parses (no DAG warning in the UI), `resolve_source_run_ids` succeeds, then `spark_read_files` stays in `scheduled` forever. The only traces are a scheduler log line (`Tasks using non-existent pool 'spark' will not be scheduled`) and the task instance's dependency details. The run stays stuck until `dagrun_timeout` (6 hours) fails it, and with `max_active_runs=1` the next `file_list` events pile up behind it meanwhile. `dag.test()` does not check pools: it is not blocked.
- `spark-submit` must be on the worker's `PATH` (it is with `uv run`: PySpark ships it in `.venv/bin`).

Airflow ≥3.0 uses the Simple Auth Manager by default, which has no `airflow users create` (`airflow standalone` prints the generated admin password and stores it in `simple_auth_manager_passwords.json.generated`). To get FAB-based user management, install `apache-airflow-providers-fab` and set `auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager` under `[core]` in `airflow.cfg`.

### Running the Spark job standalone (outside Airflow)

```bash
make spark-job RUN_ID=<dag_run_id of a list_files_dag run>
# = uv run spark-submit pipeline/jobs/read_files_job.py --run-id=<id>
```

`--run-id` is required: the job reads the `file_list` rows of that run (exit code 3 when there are none). It connects through `.env` (`DB_*`), not through Airflow.

### Docker

```bash
make db-up     # docker compose up -d: Postgres on host port 5434, Redis on 6379
make db-down   # docker compose down (keeps the postgres-data volume, no -v)
```

The project's Postgres listens on host port **5434** (container `airflow_postgres`), so it can run next to another project's Postgres on 5433; set `DB_PORT=5434` in `.env`. `make db-up` first checks the port (`make db-port-check`): it fails, naming the container or process, if anything but `airflow_postgres` holds it. There is no Airflow service in the compose file yet. In Airflow 3, `airflow db init` and `airflow webserver` no longer exist: use `airflow db migrate` and `airflow api-server`.

### Database

The `file_list` table (see `utils/SQL/script_tables.sql`) must exist in Postgres before running `list_files_dag`. `list_files_dag`'s insertion task (`insert_into_file_list`) goes through the Airflow Connection `postgres_conn_id="file_list_db"` (created by `make airflow-init` from `.env`), using `PostgresHook(...).get_sqlalchemy_engine()` built once per task run and reused for every file. `.env`/`DB_USER`/`DB_PASSWORD`/`DB_HOST`/`DB_PORT`/`DB_NAME` (`utils/db_connection.py`, `python-dotenv`) remain required for everything that does **not** go through Airflow: the Spark job (`pipeline/jobs/read_files_job.py`, which can run standalone without Airflow installed) and any standalone/script usage. `db_connection()` raises if any var is missing — there is no default/fallback — and caches one `Engine` per connection URL for the life of the process.

Set `ENV=prod` to switch Spark to cluster mode (YARN/K8s-managed master, adds the `spark-excel` package for xlsx support); otherwise Spark runs `local[*]` (see `utils/spark_session.py`).

## Architecture

This is a two-stage Airflow pipeline that hands off from Airflow's Python/SQL world to a separate Spark process:

1. **`dags/local_list_files_dag.py`** (`list_files_dag`) — `Extractor` (`pipeline/extractor/extractor.py`) globs `extractor_config.yaml`'s `source_directories` for files matching `files_extensions` / `include_files`, minus `exclude_files` (fnmatch patterns), then `utils/functions_utils.insert_into_file_list` writes metadata rows (status=`pending`) into the Postgres `file_list` table, tagged with the Airflow `dag_run_id`.
2. **`dags/local_read_file_dag.py`** (`read_file_dag`) — scheduled on the `file_list` Asset, with `max_active_runs=1`. The Asset is declared as `Asset(name="file_list")` in each of the three DAG files (Airflow matches Assets by name; a shared `dags.*` module would only import with the project root on `sys.path`). The insertion task of both listing DAGs declares it in `outlets`, and emits it only when it succeeds with at least one recorded file; an empty listing is skipped and emits nothing. `resolve_source_run_ids` collects the deduplicated `source_run_id` of every triggering `file_list` event (Airflow may batch several listings into one run). On a manual trigger (no event) it uses `dag_run.conf["run_id"]`, stripped, rather than `params` (`Param.resolve()` memoises its value). With neither, it fails explicitly. `spark_read_files` is a `SparkSubmitOperator` (provider `apache-airflow-providers-apache-spark`) mapped over `application_args=resolve_source_run_ids().map(build_application_args)`: one instance per source run, each with its own exit code and retries. It uses the `spark_default` Connection for the master and the `spark` pool, both described under "Running Airflow locally". Each instance runs `spark-submit --master <from the Connection> --name ... pipeline/jobs/read_files_job.py --run-id=<id>`. The hook passes this argument list to `subprocess.Popen` with no shell, and the run id is a single `--run-id=<id>` argument, so a value starting with `-` is not taken for an option. `env_vars={"PYTHONPATH": <project root>}` is added to the worker environment, not substituted for it (local/client mode). `application_args` is a templated field: the value mapped from the XCom is skipped by Jinja, but never build the run id into a templated string (an f-string passed to `.partial()`, `@task.bash`...). `tests/integration/test_p1_4_read_file_dag_e2e.py` guards against that. No Spark resources (`executor_memory`...) are set in the DAG: they belong to the Connection or `spark-defaults.conf` of each environment. Airflow only orchestrates; Spark runs in its own process. The job queries `list_file_from_db(run_id)` for the rows of that `dag_run_id`, reads each one with `BaseFileReader`, and exits 3 when the run id has no rows (typo or purged data). When Airflow kills the task, `SparkSubmitHook.on_kill` only kills the `spark-submit` process (the JVM), not the Python driver it started: the job therefore runs a parent watchdog thread (`start_parent_watchdog`) that exits with code 4, without writing any status, as soon as its parent process is gone. A retry reprocesses the run's files idempotently.

There's also `dags/local_list_files_dag_python_operator.py`, an equivalent of DAG 1 built with `PythonOperator` instead of the `@task` TaskFlow API — its `dag_id` was deliberately renamed to `list_files_dag_classic` to avoid colliding with `local_list_files_dag.py`.

Every DAG/job file prepends the project root to `sys.path` manually (`Path(__file__).resolve().parent.parent...`) rather than relying on package install, since Airflow imports DAG files directly by path.

### Config-driven file reading — two config shapes coexist

`pipeline/file_reader/base_reader.py` (`BaseFileReader.get_config_file`) resolves which YAML config to use for a given file by filename convention, not by any explicit mapping:

- It scans `config/*.yaml` for a file whose name prefix (`split("_")[0]`) appears as a substring of the target filename — e.g. `imdb_config.yaml` matches any file with "imdb" in its name (`config/imdb_config.yaml`, `config/orders_config.yaml`). These per-source configs are structured as `source:` / `reader:` (with `reader_options` nested inside) / `storage:`.
- If nothing matches, it falls back to `config/basic_reading_config.yaml`, keyed directly by extension (`csv:`, `txt:`, `xlsx:`, `parquet:`) with `reader_options` at the top level of that block, not nested under a `reader:` key.

Readers are not consistent about which shape they expect: `CSVReader.read()` looks for `file_config["reader"]["reader_options"]` (the per-source shape), while `ExcelReader.read()` looks for `file_config["reader_options"]` directly (the basic-config shape). Keep this in mind when adding a new source config or a new reader — pick the shape that matches the reader you're touching, or fix both sides together.

`ReaderFactory` (`pipeline/file_reader/reader_factory.py`) maps the resolved `source.format` string to a reader class: `csv`/`txt` → `CSVReader`, `parquet` → `ParquetReader`, `xls`/`xlsx` → `ExcelReader`. `pipeline/file_reader/utils.py` maps the YAML `columns_type` strings (string/int/float/double/boolean/date/timestamp) to PySpark types for explicit-schema reads.

### Processing and storage layers

- `pipeline/processor/base_processor.py` (`BaseProcessor`) wraps a Spark `DataFrame` with chainable cleanup methods (`remove_empty_rows`, `drop_na_rows`, `fill_na`, `remove_duplicates`, `cast_column`, `rename_columns`), each returning `self`.
- `storage/base_writer.py` defines the `BaseWriter` ABC (`write(data, target, **kwargs)`); `storage/postgresql_writer.py` (`PostgresWriter`) implements it for both Spark DataFrames (via JDBC) and Pandas DataFrames (via `to_sql`); `storage/hive_writer.py` implements the Hive equivalent. Writers must subclass `BaseWriter` and implement `write()` to satisfy the interface.
- `utils/path_utils.normalize_spark_path` converts local paths to `file://` (or `file:///` on Windows) URIs for Spark, leaving already-distributed URIs (`hdfs://`, `s3://`, `gs://`, `dbfs:/`, `abfss://`, `adl://`) untouched.

### `pipeline/IMDBExtractor.py` / `pipeline/IMDBTransform.py`

A separate, older/ad-hoc IMDB-specific extract+transform script pair, not wired into the DAG pipeline above — check these before assuming all data flows through `Extractor`/`BaseFileReader`.

## Targets & ground rules (read before changing code)

- **Target stack:** Airflow 3.3.x (`airflow.sdk` imports, providers for operators), PySpark 4.x, Python 3.12+. Airflow 2 is end-of-life.
- **Idempotency first:** tasks and the Spark job work on the scope of the current run (`run_id` / data interval), never on "today".
- **Readers return a Spark DataFrame.** Config shape mismatches between readers are a known bug source — test both the dedicated config and the `basic_reading_config.yaml` fallback.
- **Never** read `.env`, commit secrets, or push. Work on a `feat/` or `fix/` branch, never directly on `main`/`develop`.
- Every bug fix comes with a regression test named after its backlog ID.
- The current audit baseline and the prioritized backlog live in `docs/audit/` (start with `2026-11-05-baseline.md`).

## Agent team

| Agent / skill | Use it for |
|---|---|
| `airflow-modernizer` (agent) | Any change to the code: backlog items, migrations, refactors |
| `code-auditor` (agent, read-only) | Quality, bugs, Spark/Airflow best practices, tests |
| `security-auditor` (agent, read-only) | Secrets, credentials, dependencies, docker, injection |
| `tech-scout` (agent, read-only) | What's new and worth adopting in the stack |
| `/full-audit` | Both auditors in parallel → one report in `docs/audit/` |
| `/modernize <ID>` | Build → review → fix loop on one backlog item |
| `/tech-watch [topic]` | Tech watch report in `docs/tech-watch/` |
