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

```bash
export AIRFLOW_HOME="$(pwd)"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
uv run airflow db migrate
uv run airflow standalone             # http://localhost:8080
```

Airflow ≥3.0 uses the Simple Auth Manager by default, which has no `airflow users create`. To get FAB-based user management, install `apache-airflow-providers-fab` and set `auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager` under `[core]` in `airflow.cfg`.

### Running the Spark job standalone (outside Airflow)

```bash
uv run spark-submit pipeline/jobs/read_files_job.py
```

### Docker

```bash
docker compose up -d   # starts only Postgres (host port 5433) and Redis — there is no Airflow service in the compose file yet
```
In Airflow 3, `airflow db init` and `airflow webserver` no longer exist: use `airflow db migrate` and `airflow api-server`.

### Database

The `file_list` table (see `utils/SQL/script_tables.sql`) must exist in Postgres before running `list_files_dag`. `list_files_dag`'s insertion task (`insert_into_file_list`) goes through an Airflow Connection, `postgres_conn_id="file_list_db"` (create it via the Airflow UI/CLI before running the DAG), using `PostgresHook(...).get_sqlalchemy_engine()` built once per task run and reused for every file. `.env`/`DB_USER`/`DB_PASSWORD`/`DB_HOST`/`DB_PORT`/`DB_NAME` (`utils/db_connection.py`, `python-dotenv`) remain required for everything that does **not** go through Airflow: the Spark job (`pipeline/jobs/read_files_job.py`, which can run standalone without Airflow installed) and any standalone/script usage. `db_connection()` raises if any var is missing — there is no default/fallback — and caches one `Engine` per connection URL for the life of the process.

Set `ENV=prod` to switch Spark to cluster mode (YARN/K8s-managed master, adds the `spark-excel` package for xlsx support); otherwise Spark runs `local[*]` (see `utils/spark_session.py`).

## Architecture

This is a two-stage Airflow pipeline that hands off from Airflow's Python/SQL world to a separate Spark process:

1. **`dags/local_list_files_dag.py`** (`list_files_dag`) — `Extractor` (`pipeline/extractor/extractor.py`) globs `extractor_config.yaml`'s `source_directories` for files matching `files_extensions` / `include_files`, minus `exclude_files` (fnmatch patterns), then `utils/functions_utils.insert_into_file_list` writes metadata rows (status=`pending`) into the Postgres `file_list` table, tagged with the Airflow `dag_run_id`.
2. **`dags/local_read_file_dag.py`** (`read_file_dag`) — a `BashOperator` does `spark-submit pipeline/jobs/read_files_job.py`. Airflow only orchestrates; Spark runs in its own process with `PYTHONPATH` set to the project root. The job queries `list_file_from_db()` for rows ingested today and reads each one with `BaseFileReader`.

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
