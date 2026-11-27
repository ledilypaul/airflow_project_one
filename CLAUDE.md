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
uv --directory transform add <pkg>   # dbt side: transform/ is a separate uv project with its own uv.lock
```

Use uv ≥ 0.11 for anything that writes a lock (`~/.local/bin/uv` here): the pyenv `uv` shim (0.6.3) rewrites `uv.lock` in an older format. `make check` and CI run `uv lock --check` in the root **and** in `transform/`; pass `make check UV=~/.local/bin/uv` if the shim comes first on `PATH`.

The project is installed in editable mode in `.venv`, so `import pipeline.*` / `utils.*` / `storage.*` works without `PYTHONPATH`. Always prefix commands with `uv run` (or activate `.venv`).

Version pins worth knowing: `pandas<3` because pandas 3 breaks `pyspark.pandas` (verified with PySpark 4.3.0); `apache-airflow==3.4.0` exact (P3-5; Task SDK 1.4.0 comes with it); `astronomer-cosmos>=1.16,<1.17` (group `airflow`); `apache-airflow-providers-cncf-kubernetes>=11.0,<12` (group `airflow`, P3-2: the KubernetesExecutor, and the Spark hook refuses a `k8s://` master without it). Cosmos 1.16 still uses deprecated Airflow imports: `pyproject.toml` ignores `DeprecatedImportWarning` raised from the `cosmos` package only.

### Tests

```bash
uv run pytest tests/                                            # full suite
uv run pytest tests/unit/file_reader/test_csv_reader.py          # one file
uv run pytest tests/unit/file_reader/test_reader_factory.py::test_factory_returns_csv_reader  # one test
uv run ruff check . && uv run ruff format --check .
```

DAG tests (`tests/unit/dags/`) don't need a real Airflow metadata DB or scheduler: `tests/unit/dags/conftest.py` sets `AIRFLOW__CORE__UNIT_TEST_MODE=True` and points `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` at a throwaway sqlite file before anything under `airflow.*` is imported. `tests/integration/conftest.py` applies the same setup. For the whole suite, `tests/conftest.py` forces `AIRFLOW__OPENLINEAGE__DISABLED=True` and drops every `OPENLINEAGE_*` / `AIRFLOW__OPENLINEAGE__*` variable of the shell (and `AIRFLOW__LINEAGE__MAX_ASSETS_PER_COLLECTOR`): the suite never emits lineage. A test that needs it turns it on with `monkeypatch`, towards `tmp_path`, and the `dag.test()` e2e runs with lineage off unless a test enables it. Likewise it forces `AIRFLOW__METRICS__OTEL_ON=False` and drops the `OTEL_*` / `AIRFLOW__METRICS__*` variables (P3-3): the suite never emits metrics.

`transform_dag` is rendered from `transform/target/manifest.json`: without it, its import fails (and `test_no_import_errors` with it). `make test` runs `make dbt-parse` first (which runs `make transform-sync`: the first `make test` downloads dbt into `transform/.venv`); CI does the same before pytest. The real dbt builds (`tests/integration/test_p2_4_dbt_build.py`) are skipped when `transform/.venv` is missing, unless `REQUIRE_DBT=1` (set in CI), which turns the skip into a failure. Likewise, the real Spark job with its OpenLineage listener (`tests/integration/test_p2_5_spark_lineage.py`) needs the jar of `make lineage-jar`: it is skipped without it, unless `REQUIRE_LINEAGE=1` (set in CI).

### Running Airflow locally

The `Makefile` wraps the dev commands (`make` lists them). Quality targets mirror CI: `make check` = `uv lock --check` in the root and in `transform/`, then `make lint`, then `make test` (`make dbt-parse`, then pytest).

```bash
make db-up          # docker-compose Postgres on host port 5434 (+ Redis); checks the port first
make airflow-init   # airflow db migrate + Connections spark_default/file_list_db + pools (idempotent)
make airflow        # make dbt-parse, then airflow standalone, http://localhost:8080
```

The Airflow targets export `AIRFLOW_HOME=<project root>`, `AIRFLOW__CORE__DAGS_FOLDER=<project root>/dags` and `AIRFLOW__CORE__LOAD_EXAMPLES=False`. `AIRFLOW_HOME` must be the project root: when it is unset, Airflow silently reads `~/airflow/airflow.cfg`, i.e. another metadata DB and other settings. The metadata DB is then the SQLite file `./airflow.db` (gitignored), while the `file_list` table stays in the docker-compose Postgres. The Spark job writes the lake to `./lake/` (gitignored) unless `LAKE_ROOT` is set; the Makefile does not export it. Running `airflow` by hand needs the same three variables. Both are overridable for a throwaway setup: `make airflow-init AIRFLOW_HOME=/tmp/af ENV_FILE=/tmp/dev.env`. The Makefile also drops `VIRTUAL_ENV` (an activated pyenv virtualenv makes `uv` warn that it does not match `.venv`).

`make airflow-init` runs `scripts/dev_airflow_connections.py`, which creates or updates (never duplicates) two Connections:
- `file_list_db` (postgres) from `DB_HOST`/`DB_PORT`/`DB_USER`/`DB_PASSWORD`/`DB_NAME`, read from `.env` (or `--env-file` / `ENV_FILE`; a variable already exported in the shell wins, as with `load_dotenv()`). The password goes through Airflow's ORM, never on a command line, and is never printed.
- `spark_default` (spark, host `local[*]`). It must be created with that conn type and host, not as a URI: `spark://local[*]` does not parse. Without touching the DB, `AIRFLOW_CONN_SPARK_DEFAULT='{"conn_type": "spark", "host": "local[*]"}'` also works.

Then it imports the pools versioned in `deploy/airflow/pools.json` (re-run it after editing the file): `spark` for `read_file_dag`, `dbt_duckdb` for `transform_dag` (see "dbt transformation layer"). The Airflow targets also export `AIRFLOW__COSMOS__ENABLE_TELEMETRY=False`. `read_file_dag` needs both the Spark Connection and the `spark` pool:

- **Connection `spark_default`.** Its `host` is the `--master` (`local[*]`, `yarn`, `spark://host:7077`, `k8s://...`). The extras `deploy-mode`, `queue`, `namespace` and `spark-binary` (one of `spark-submit`, `spark2-submit`, `spark3-submit`, found on the worker's `PATH`) are optional. If the Connection is **missing**, the provider itself does not fail: it logs `Could not load connection string spark_default, defaulting to yarn` at INFO level and runs `spark-submit --master yarn`, which fails locally ("HADOOP_CONF_DIR or YARN_CONF_DIR must be set"). That is why `resolve_source_run_ids` checks the Connection first and fails the run with an explicit `AirflowFailException` when it is missing (it logs the Connection's `host` otherwise). `airflow connections create-default-connections` also creates a `spark_default` with `host=yarn` and `queue=root.default`: `make airflow-init` overwrites it (host `local[*]`, extras cleared). Outside `ENV=prod`, `utils/spark_session.py` forces `master("local[*]")` in the job itself, so in dev the Connection's master only decides how `spark-submit` launches the driver.
- **Pool `spark`.** One slot locally: each `local[*]` application uses every core of the machine and starts its own driver JVM. Two at once just compete for the same CPU and RAM. Raise it to the number of applications your cluster can run side by side. A **missing pool is silent**: the DAG still parses (no DAG warning in the UI), `resolve_source_run_ids` succeeds, then `spark_read_files` stays in `scheduled` forever. The only traces are a scheduler log line (`Tasks using non-existent pool 'spark' will not be scheduled`) and the task instance's dependency details. The run stays stuck until `dagrun_timeout` (6 hours) fails it, and with `max_active_runs=1` the next `file_list` events pile up behind it meanwhile. `dag.test()` does not check pools: it is not blocked.
- `spark-submit` must be on the worker's `PATH` (it is with `uv run`: PySpark ships it in `.venv/bin`).

The Airflow targets also configure OpenLineage (see "Lineage (OpenLineage, P2-5)"): `AIRFLOW__OPENLINEAGE__NAMESPACE` (`airflow_project_one`), `OPENLINEAGE_SPARK_JAR`, `OPENLINEAGE_SPARK_SHA256` and `AIRFLOW__LINEAGE__MAX_ASSETS_PER_COLLECTOR=1000`. `make airflow` adds `AIRFLOW__OPENLINEAGE__TRANSPORT`: `file` into `lineage/events/` by default, `http` when `OPENLINEAGE_URL` is set. It is built with `json.dumps` by `scripts/openlineage_transport.py`, never by interpolation; `make lineage-transport` prints it. A value exported by the shell wins, and it overrides `[openlineage] transport` of `airflow.cfg`. `make airflow` downloads the listener jar, or re-checks its SHA-256 (`make lineage-jar`); offline, it only warns. With `OTEL_EXPORTER_OTLP_ENDPOINT` set, `make airflow` also turns on Airflow's OpenTelemetry metrics (see "Observability (metrics and alerts, P3-3)"); without it, metrics stay off.

Airflow ≥3.0 uses the Simple Auth Manager by default, which has no `airflow users create` (`airflow standalone` prints the generated admin password and stores it in `simple_auth_manager_passwords.json.generated`). To get FAB-based user management, install `apache-airflow-providers-fab` and set `auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager` under `[core]` in `airflow.cfg`.

### Running dbt standalone (outside Airflow)

```bash
make transform-sync   # uv sync --locked in transform/ (dbt-core 1.12, dbt-duckdb 1.11, duckdb<2)
make dbt-parse        # transform/target/manifest.json (read by transform_dag)
make dbt-build        # dbt build; LAKE_ROOT and DBT_DUCKDB_PATH overridable
```

The dbt targets export `LAKE_ROOT` (default `<project root>/lake`, the job's default) and `DBT_DUCKDB_PATH` (default `<project root>/transform/warehouse.duckdb`, gitignored); a value from the shell or the command line wins. Do not run it while `transform_dag` writes the same DuckDB file. See `transform/README.md`.

### Running the Spark job standalone (outside Airflow)

```bash
make spark-job RUN_ID=<dag_run_id of a list_files_dag run>
# = uv run spark-submit pipeline/jobs/read_files_job.py --run-id=<id>
```

`--run-id` is required: the job reads the `file_list` rows of that run and writes each file to the Parquet lake under `LAKE_ROOT`. `LAKE_ROOT` defaults to `<project root>/lake` (gitignored) and is resolved from the code. The job exits (constants `EXIT_*` in `pipeline/jobs/read_files_job.py`):
- **0** when every file is `processed`;
- **2** when `--run-id` is missing or empty;
- **3** when the run has no rows;
- **4** when its parent process is gone (see the watchdog below), without writing any status;
- **5** when the setup is invalid: a reading config fails validation, a source has no `storage`, two lake tables are nested, or `LAKE_ROOT` is relative (including `file:lake`). Every row of the run is then marked `failed` and Spark is not started. Exit 5 is also used, right after Spark starts and before any file, when a quality regex of the registry does not compile in Java;
- **6** (P2-6) when at least one file is `failed` and every failed file failed only on blocking quality checks that found failing rows (status `fail`, severity `error`), with their results recorded and the staging partition removed. Other files may be `processed`;
- **1** when at least one file is `failed` for any other reason, alone or mixed with quality failures: read or write error, quality engine raising (`quality checks could not run`), blocking check that could not run (status `error`), results not recorded, two files with the same name.

Priority at the end of a run: 0 if nothing failed, else 6 if every failure is such a quality failure, else 1. `read_file_dag` does not retry 2, 3, 5 and 6, which are deterministic; it retries 1, 4 and any other failure.

It connects through `.env` (`DB_*`), not through Airflow.

### Airflow REST API (P4-1)

Airflow 3 serves `/api/v2` itself. `scripts/airflow_api.py` (standard library only) wraps the three calls the project needs; design note `docs/architecture/2026-11-22-p4-1-airflow-api.md`.

```bash
export AIRFLOW_API_URL=http://localhost:8080   # default; http://127.0.0.1:8081 for make k8s-ui
export AIRFLOW_API_PASSWORD=...                # the admin password (make k8s-ui-password; airflow standalone prints it)
make api-dags                                  # the DAGs, paused or active
make api-runs DAG=read_file_dag                # latest runs: run id, state, start, end
make api-trigger DAG=read_file_dag CONF='{"run_id": "<dag_run_id of a listing>"}'
```

- **Credentials by environment only.** `AIRFLOW_API_PASSWORD` (with `AIRFLOW_API_USER`, default `admin`) is exchanged at `POST /auth/token`, or `AIRFLOW_API_TOKEN` is used as is. Never on a command line; never printed.
- **Refused before any request:** `http` for a host other than localhost, user info, a query string or a fragment in the URL. Redirections are not followed (urllib would resend the token to the new host).
- **`trigger` refuses a paused DAG** (`--even-if-paused` to override): Airflow accepts the run but never starts it. `logical_date` is sent as `null`: the API's body requires the key.
- Exit 0 ok, 1 API refusal or unreachable, 2 wrong argument or configuration. `make` itself reports any failed recipe as 2.

### Documentation (Sphinx)

The reader-facing documentation is in `docs/` (French, Markdown through MyST, theme Furo, entry `docs/index.md`); this file stays the contributor/agent reference. Build it with `make docs` (`make docs-serve` for a live preview): **warnings are errors** (`-W`), in CI too (job `docs`).

- **Own dependency group.** `docs` (sphinx, furo, myst-parser, sphinx-copybutton, sphinxcontrib-mermaid, sphinx-autobuild, sphinx-autodoc2) is **not** in `default-groups`, so `uv sync` and the Docker image (`uv sync --no-dev`) never install it. `make docs` runs `uv run --locked --only-group docs`: the API pages are generated by **static analysis** (`sphinx-autodoc2`), so neither PySpark nor Airflow is needed. `pip-audit` exports the group on purpose (see "CI and supply chain").
- **API reference.** `docs/reference/autodoc/` is generated at each build (git-ignored): never edit it. The code's docstrings are read as **Markdown (MyST)**, not reStructuredText: single backticks for code, `-` lists. `suppress_warnings = ["ref.python"]` only hides the ambiguity of `dict[str, type]` (the builtin `type` vs the `type` attributes of the quality models).
- **The dated notes** (`docs/architecture/`, `audit/`, `tech-watch/`) are included as they are (`docs/decisions.md`, three globbed `toctree`s): new notes appear without editing a list. Do not rewrite them.
- **Mermaid** diagrams load their library from jsDelivr (version pinned by `sphinxcontrib-mermaid`) when the page is opened: offline, the diagram's text shows instead.
- **Kept true by tests** (`tests/unit/test_docs_setup.py`): the commands page lists every Makefile target, pages only mention `make` targets that exist, the YAML examples of the configuration pages validate against the real models, `docs` stays out of `default-groups`, the CI job builds with `-W`.
- **Adding a page** means adding it to a `toctree` (an orphan is a warning, hence an error).

### Docker

```bash
make db-up     # docker compose up -d: Postgres on host port 5434, Redis on 6379
make db-down   # docker compose down (keeps the postgres-data volume, no -v)
```

Marquez (OpenLineage UI, optional) sits in the same compose file under the profile `lineage`, so `make db-up` never starts it:

```bash
make lineage-up     # Marquez API http://127.0.0.1:5002, web UI http://127.0.0.1:3002 (localhost only)
make lineage-down   # stops and removes its containers (keeps the marquez-data volume)
OPENLINEAGE_URL=http://localhost:5002 make airflow   # send the events to Marquez instead of files
```

The metrics stack (OpenTelemetry collector + Prometheus, optional, P3-3) is the same kind of optional service, under the profile `observability` (see "Observability (metrics and alerts, P3-3)"):

```bash
make observability-up     # collector OTLP/HTTP http://127.0.0.1:4318, Prometheus UI http://127.0.0.1:9092 (localhost only)
make observability-down   # stops and removes both containers (keeps the prometheus-data volume)
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 make airflow   # Airflow sends its metrics there
```

The project's Postgres listens on host port **5434** (container `airflow_postgres`), so it can run next to another project's Postgres on 5433; set `DB_PORT=5434` in `.env`. `make db-up` first checks the port (`make db-port-check`): it fails, naming the container or process, if anything but `airflow_postgres` holds it. Both published ports (Postgres 5434, Redis 6379) are bound to `127.0.0.1` (P3-1): nothing is reachable from the network. `docker compose` also reads `.env` for interpolation:
- **Postgres** starts with `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` taken from `DB_USER` / `DB_PASSWORD` / `DB_NAME`, i.e. the same values the Spark job and `make airflow-init` use to connect. Without them, the fallback is `airflow` / `airflow` / `airflow` (dev only). Postgres applies them **only when it creates its data directory**: an existing `postgres-data` volume keeps the credentials of its first start (`make db-down` then `docker volume rm airflow_project_one_postgres-data`, which deletes `file_list`, if you want new ones).
- **Redis** runs with `--requirepass`, read by the container's shell from `REDIS_PASSWORD` (dev fallback `airflow`, never empty), so it is not written on the compose command line. The healthcheck authenticates through `REDISCLI_AUTH` (same value) and requires `PONG`: `redis-cli ping` answers `NOAUTH` with exit code 0. Nothing in the project uses Redis yet (no Celery executor); a client needs `redis-cli -a "$REDIS_PASSWORD"` or `REDISCLI_AUTH`.
- Marquez keeps its own fixed `marquez` credentials (imposed by the image) and is not covered by this.

There is no Airflow service in the compose file yet (the backlog's P3-1 "full official Airflow 3.3 compose" is not done: only the hardening above is). In Airflow 3, `airflow db init` and `airflow webserver` no longer exist: use `airflow db migrate` and `airflow api-server`.

### Observability (metrics and alerts, P3-3)

Airflow's native OpenTelemetry metrics, a local collector + Prometheus with five alert rules (three from P3-3, two from P3-8), and one structured log line per failure. Design note: `docs/architecture/2026-11-17-p3-3-observability.md`. Rule, as for lineage: **observability never breaks the pipeline**, and it is **off unless asked for**.

- **Metrics: environment only.** Airflow ≥ 3.2 reads the standard `OTEL_*` variables (the `[metrics] otel_host` / `otel_port` / `otel_interval_milliseconds`... options are deprecated). `make airflow` exports `AIRFLOW__METRICS__OTEL_ON=True`, `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`, `OTEL_SERVICE_NAME` (the OpenLineage namespace) and `OTEL_METRIC_EXPORT_INTERVAL=15000` **only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set** (shell or command line). Without it nothing is exported and Airflow keeps `otel_on = False`. A value exported by the shell wins; `make metrics-env` prints what is exported (never the `*_HEADERS`). Credentials of a remote collector go in `OTEL_EXPORTER_OTLP_HEADERS`, never in the endpoint: Airflow logs the endpoint at INFO, so an endpoint with user info or a query string (in `OTEL_EXPORTER_OTLP_ENDPOINT` **or** `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`) is refused by `scripts/otel_endpoint_check.py` even when it answers: `make airflow` starts without metrics and a warning that names the variable, never its value (the Makefile also warns about an `@`; `make metrics-env` masks the user info and the query string with `***`). No dependency to add: `apache-airflow-core` already installs `opentelemetry-exporter-otlp`.
- **Collector down.** It never fails a task, but every Airflow process then waits about 10 s for the exporter's final flush when it exits (measured). So `make airflow` runs `scripts/otel_endpoint_check.py` (TCP connect, 2 s) and, with an endpoint that does not answer, starts Airflow **without metrics** and a warning naming `make observability-up`. The check never raises (invalid host, port out of range, over-long DNS label are "problems"). Do **not** lower `OTEL_EXPORTER_OTLP_TIMEOUT`: with 1000 or 3000 ms a process did not exit in 100 s (the default 10 s costs 10 s). A collector that dies during a session is not detected.
- **Stack** (compose profile `observability`, `make observability-up` / `observability-down`; `make db-up` never starts it): `otel-collector` (contrib 0.165.0, OTLP/HTTP on `127.0.0.1:4318`) then `prometheus` (v3.13.5, LTS line, UI `127.0.0.1:9092`, 7 days of data in the `prometheus-data` volume). Same rules as Marquez: ports on `127.0.0.1` only (neither has authentication), own network `observability`, images pinned by tag and digest, configuration mounted read-only from `deploy/observability/`. Hardening: `cap_drop: [ALL]` and `no-new-privileges` on both; the collector has a read-only root filesystem and `mem_limit: 512m`; Prometheus keeps 7 days **or** 1 GB (`--storage.tsdb.retention.size`) and answers cross-origin browser calls only to its own UI (`--web.cors.origin`, its default `.*` would let any open web page query it). Neither has authentication: **outside a local machine, the OTLP receiver needs `bearertokenauth` or mTLS** (not configured here). The collector's Prometheus exporter (8889) is reached by service name, never published; its series expire 5 minutes after their last update (`metric_expiration`).
- **Alert rules.** `deploy/observability/rules/airflow.yml`, unit-tested by `promtool test rules` (`deploy/observability/tests/airflow_rules_test.yml`, not in the loaded directory):
  - Every Airflow series is selected with `job="airflow_project_one"` (the `OTEL_SERVICE_NAME` of `make airflow`, kept by the scrape): the collector accepts OTLP from anyone who can reach it, and another service's series must neither fire nor hide a dead scheduler. Change the service name in both places.
  - `AirflowDagRunFailed` (critical): `airflow_dagrun_duration_failed_milliseconds_count`. The first failure of a DAG is the first sample of its series, which `increase()` cannot see, so the rule also takes the series born in the last 15 minutes, **only when the scrape of the collector (`up{job="otel-collector"}`) was continuous over the window** (after a scrape gap of 15 minutes or more a series returns with its old value and looks new). Limit: during its first 15 minutes Prometheus may miss a first failure.
  - `AirflowSchedulerHeartbeatMissing` (critical): `airflow_scheduler_heartbeat_total` frozen or absent for 5 minutes (`sum`, so that a restart's stale series does not alert; one scheduler assumed). It fires 7 minutes or more after the scheduler stopped (7 to 8 measured).
  - `AirflowTaskStuckScheduled` (warning): `airflow_ti_scheduled > 0` for 15 minutes while nothing runs or is queued, i.e. a missing pool ("Running Airflow locally"; a missing pool exports no pool metric) **or a paused DAG** (the scheduler does not queue the tasks of a paused DAG). A busy pool is a normal queue and does not alert.
  - `AirflowDagImportErrors` (warning, P3-8): `max(airflow_dag_processing_import_errors) > 0` for 10 minutes. The DAG processor sets that gauge once per complete parse loop (nothing before the first loop, so no alert at startup). An absent or expired series does not fire (a dead processor is not watched). `max`, not `sum`: after a restart the collector still exposes the old instance for 5 minutes. The `for` absorbs a file being edited or a `make dbt-parse` still running (`transform_dag` does not import without its manifest).
  - `AirflowDagRunStuckQueued` (warning, P3-8): `scheduler.dagruns.queued` (new in Airflow 3.4) above 0 while `scheduler.dagruns.running` is 0 (`or vector(0)`), for 15 minutes. A queue behind `max_active_runs=1` has a run `running` and is normal. The scheduler only starts the queued runs of a DAG that is neither paused nor stale (`get_queued_dag_runs_to_set_running`), e.g. a manual trigger of a paused DAG; `AirflowTaskStuckScheduled` misses it, since no task instance exists yet.
  - Read in `http://127.0.0.1:9092/alerts`. There is **no Alertmanager** and no Slack/e-mail notification yet. Every metric a rule uses is checked against the installed Airflow's `metrics_template.yaml` (`tests/unit/test_p3_3_alert_rules.py`): an upgrade that renames one fails that test.
- **Failure line.** `utils/failure_alert.py`, set as `on_failure_callback` in the `default_args` (every task, Cosmos included) and on the DAG (a run: also `dagrun_timeout`, which no task callback sees) of the four DAGs. One ERROR line `airflow_failure {<JSON>}` per failure: `dag_id`, `run_id`, `task_id`, `map_index`, `try_number`, `max_tries`, `operator`, `exception_type` (the exception's **type only**: its text is already in the task log, where Airflow masks the secrets it knows, and a JSON copy would escape that masking: `json.dumps` rewrites `"` and non-ASCII letters) and, for `spark_read_files`, `spark_exit_code` + `spark_exit_code_meaning` (the codes 1 to 6 of "Running the Spark job standalone"). It is written on the final attempt only, once **per failed task** (a failing dbt build of `transform_dag` gives 1 + k `task` lines, the producer and k sensors, and one `dag_run` line). If `utils` cannot be imported (a worker image without the project) each wrapper logs a minimal line (`scope`, `dag_id`, `run_id`, `reason`, `task_id`) instead of raising; the four DAG files carry the same few lines on purpose, since a shared module would be project code. `ReadFilesSparkSubmitOperator` keeps the code in `spark_exit_code` (None while spark-submit did not run); retries are unchanged. `fail_if_spark_failed` has no callback (the failed instance already reported). The callback never raises, builds its JSON with `json.dumps`, and imports no Airflow: `utils/` stays importable by the Spark job. The exit codes are duplicated in `utils/failure_alert.py` (the job imports pyspark) and a test keeps them equal to the job's `EXIT_*`.
- **Tests never emit metrics.** `tests/conftest.py` forces `AIRFLOW__METRICS__OTEL_ON=False` and `AIRFLOW__TRACES__OTEL_ON=False` and drops every `OTEL_*`, `AIRFLOW__METRICS__*` and `AIRFLOW__TRACES__*` variable of the shell (a test runs the suite's neutrality check under a shell set up for `make airflow`). `tests/integration/test_p3_3_promtool.py` runs `promtool` from the `PATH`, else from the compose's Prometheus image through Docker, else it is skipped (`REQUIRE_PROMTOOL=1` turns the skip into a failure).
- **Not done:** notification (Alertmanager, Slack, e-mail), Grafana, OTel traces, a rule on the other Airflow 3.4 scheduler metrics (`scheduler.loop_exceptions` and the rest: see the P3-8 paragraph of the design note), metrics of the Spark job, several schedulers.

### Kubernetes (kind + Helm, P3-2)

Optional, local (kind), dev only: `make airflow`, `make check` and `make spark-job` never touch it (a test runs them with `make -n`). Design note, versions, digests and the real run: `docs/architecture/2026-11-19-p3-2-kubernetes.md`.

```bash
make k8s-tools        # kind v0.33.0, kubectl v1.37.2, helm v4.3.2 into ~/.local/bin (K8S_TOOLS_DIR), SHA-256 checked, HTTPS only
make kind-up          # one-node cluster `airflow-project-one`, kindest/node v1.37.0 pinned by digest, API server on 127.0.0.1
make image kind-load  # build the project image (IMAGE_REPOSITORY:IMAGE_TAG, default airflow-project-one:dev) and load it into the node
make k8s-install      # namespace (PSA restricted), Secrets, chart of the project, official Airflow chart 1.22.0 (SHA-256 checked, .k8s/), pools
make k8s-load-data DATA=examples/data/orders_sample.csv   # copy a source file into the shared volume (data/)
make k8s-ui           # kubectl port-forward on http://127.0.0.1:8081 (K8S_UI_PORT); user admin, `make k8s-ui-password`
make k8s-spark-clean  # delete the finished Spark driver pods (their Service, ConfigMaps, NetworkPolicy go with them)
make k8s-uninstall    # remove both Helm releases, keep the Secrets and the volumes
make kind-down        # delete the cluster and everything in it
```

- **Every kubectl/helm call names the context** `kind-airflow-project-one` (`K8S_ARGS` / `HELM_ARGS`): never another cluster of `~/.kube/config` (`kind create cluster` still makes it the current context). Nothing is published on the host except the port-forward, bound to `127.0.0.1`.
- **cgroup v1 (WSL2).** The kubelet refuses cgroup v1 by default since Kubernetes 1.35: `deploy/k8s/kind-config.yaml` sets `failCgroupV1: false` (ignored on a cgroup v2 host).
- **One image** (`Dockerfile`, `.dockerignore` allow-list) for the Airflow pods and the Spark driver/executors: `apache/airflow:slim-3.4.0-python3.12` (P3-5: its Airflow must be the one of `uv.lock`, a test keeps them equal; the 3.4 image is a Docker Hardened Image on Debian 13, rebuilt and revalidated on kind in P3-6, 2026-11-22: list → read → transform; chart 1.22.0 is still the latest published, its `airflowVersion` is the image's and a test keeps both equal) and `eclipse-temurin:17.0.21_9-jre-noble` pinned by digest, the image's venv synced exactly to `uv.lock` (`uv sync --frozen --no-dev --no-install-project`), `PYTHONPATH=/opt/airflow` instead of an editable install, `transform/.venv` and a dbt manifest parsed at build time with the deployment's `LAKE_ROOT` / `DBT_DUCKDB_PATH`, the OpenLineage jar checked against the Makefile's SHA-256 (`make image` passes it), `SPARK_HOME` = the PySpark wheel's distribution, user 50000. `deploy/k8s/image/entrypoint.sh` sends `driver`/`executor` to Spark's Kubernetes entrypoint (vendored unchanged from `apache/spark-docker` 4.3.0, SHA-256 pinned by a test) and anything else to the Airflow image's entrypoint. `/opt/airflow/data` is a symlink to `shared/data` (the listing reads `./data/`). Never in the context: `.env`, `airflow.cfg`, `airflow.db`, `lake/`, `logs/`, `.venv`, `data/`, `lineage/` (checked on the real build context); inside the allowed directories, `**/.env*`, `**/airflow.cfg`, `**/*.db`, `**/*.pem`, `**/*.key`, `**/*credentials*`, `**/*.generated` are excluded too. A new image needs a new `IMAGE_TAG` for `helm upgrade` to replace the pods.
- **Official chart** (`deploy/k8s/values-airflow.yaml`): `KubernetesExecutor`, DAGs in the image (no git-sync), no bundled Postgres/PgBouncer/Redis/StatsD (`bitnamilegacy` image unmaintained), Simple Auth Manager (`admin`; password generated once by `scripts/k8s_secrets.py` in the Secret `airflow-ui-admin`, never by the api-server, which would log it; an init container copies the derived `airflow-ui-passwords` into an `emptyDir`, since the api-server opens that file for writing), triggerer on, logs on the PVC `airflow-logs`, failed worker pods kept. `migrateDatabaseJob.useHelmHooks: false` (with `helm --wait` the hook deadlocks); its finished Job is deleted before each upgrade (immutable template). Fernet key, API secret key and JWT secret come from Secrets generated once. `AIRFLOW_CONN_FILE_LIST_DB` / `AIRFLOW_CONN_SPARK_DEFAULT` reach the worker pods only (`workers.kubernetes.env`, `secretKeyRef`), never the scheduler, api-server, dag-processor or triggerer. `allowPodLaunching: false`: the chart's `pod-launcher` Role (pods and `pods/exec` for the scheduler and the workers) is not created.
- **Project chart** (`deploy/k8s/charts/project`): Postgres 17.12 by digest (dev; databases `airflow` and `file_list`, DDL from `utils/SQL/script_tables.sql` by `--set-file`; an existing PVC keeps its first passwords), PVCs `project-shared` (sources, lake, `warehouse.duckdb`) and `airflow-logs` in `ReadWriteOnce` (one node; `helm.sh/resource-policy: keep`), ConfigMap `spark-conf`, RBAC, admission policy, toolbox pod. The StatefulSet's selector and `volumeClaimTemplates` (immutable) carry only stable labels (`project.stableLabels`, no chart version, C-2): a chart upgrade is never refused. Paths: `LAKE_ROOT=/opt/airflow/shared/lake`, `DBT_DUCKDB_PATH=/opt/airflow/shared/warehouse.duckdb`, the same in the worker pods and the driver.
- **Secrets** (`scripts/k8s_secrets.py`, run by `make k8s-install`): generated with `secrets`, sent to kubectl on stdin, never on a command line or in the output. Generated ones (`project-postgres`, `file-list-db`, `airflow-fernet-key`, `airflow-api-secret-key`, `airflow-jwt-secret`, `airflow-ui-admin`) are created once then kept; one that lacks an expected key is an error, never overwritten (delete it to regenerate it; a Postgres password also needs a new volume). Derived ones (`airflow-metadata`, `airflow-connections` with `AIRFLOW_CONN_FILE_LIST_DB` and `AIRFLOW_CONN_SPARK_DEFAULT`, `airflow-ui-passwords`) are re-applied server-side (a client-side apply would copy them into `last-applied-configuration`). kubectl missing or refused: one `error: ...` line, exit 1, no traceback, no value.
- **Spark on Kubernetes: only the Connection changes.** `spark_default` = master `k8s://https://kubernetes.default.svc:443`, extras `deploy-mode` cluster and `namespace`. The rest is the environment's `spark-defaults.conf` (ConfigMap `spark-conf`, `SPARK_CONF_DIR=/opt/spark-conf` in the worker pods): image, ServiceAccount `spark`, pod templates (non-root, no privilege, no token for executors), `driverEnv` `ENV=prod` (else `utils/spark_session.py` forces `local[*]`), `LAKE_ROOT`, `DB_*`, `DB_PASSWORD` by `secretKeyRef` (never a Spark property: spark-submit logs them), the shared volume in the driver and executors, `waitAppCompletion=true`. That mode is a master **starting with** `k8s://` and `deploy-mode` cluster, both from the Connection (not the provider's `_is_kubernetes`, true for any master containing `k8s`). Then `ReadFilesSparkSubmitOperator` passes the job as `local:///opt/airflow/pipeline/jobs/read_files_job.py`, removes the Spark lineage conf with a warning, and takes the driver container's exit code from the hook (`_spark_exit_code`): checked on kind, spark-submit 4.3.0 exits 0 when the driver exits 3, and prints `exit code: 3`. A refused submission (403...) exits 1 with no code: retried. Killing the task deletes the driver pod (`on_kill`, checked). An unknown code (driver pod gone, watcher cut) is retried, after deleting the driver pod the hook identified.
- **One driver per task instance (C-1).** A worker pod that dies (OOM, lost node) leaves its driver running, and the retry would start a second one on the same partitions. Every driver gets the label `airflow-project-one/task-instance` (`K8S_TI_LABEL`, `k8s_task_instance_label`: SHA-256 of dag, run, task, map index, the same on every try). Before submitting, the operator deletes the `Pending`/`Running` drivers with that label and waits until they are gone (120 s); finished ones are kept. If it cannot list or delete them, it submits nothing and the try fails (retried). Local/client mode sets no label. xlsx is not available there (the `spark-excel` package of `ENV=prod` is never downloaded into a running JVM). Spark lineage is off on Kubernetes (no transport configured, and the operator strips it anyway; it would need `spark.jars` as `local://` and an http transport).
- **RBAC** (namespaced Roles, no wildcard, no `secrets` resource): `spark-driver` (SA `spark`): pods only. `spark-submit` (SA `airflow-worker` of the worker pods, its only Role): pods (create/get/list/watch/delete, also used by the C-1 check), configmaps and services (create/patch/delete, server-side apply), and `networking.k8s.io/networkpolicies` (Spark >= 4.2 applies one per application; without it the submission fails with a 403). `airflow-scheduler` (SA `airflow-scheduler`, SEC-2): pods (create/get/list/watch/patch/delete) and events (list/watch) for the KubernetesExecutor, no `pods/exec`. The api-server keeps the chart's `pod-log-reader`. Three Spark cleanups that would need more are off in `spark-defaults.conf`: `driver.service.deleteOnTermination=false`, `driver.ownPersistentVolumeClaim=false` (else a `deletecollection` on PVCs, which could reach Postgres'), `executor.disableConfigMap=true`. A real run with these Roles logs no 403.
- **Admission.** `pods:create` would let a task start a pod that mounts any Secret: a `ValidatingAdmissionPolicy` (`admission-policy.yaml`, this namespace only) makes every pod created by `spark` or `airflow-worker` run as SA `spark`, with the project image only, no Secret volume and no Secret read but `file-list-db` (the driver's `secretKeyRef`). `make k8s-install` labels the namespace for Pod Security Admission `restricted` (enforce, warn, audit; `PSA_LEVEL` / `PSA_VERSION=v1.37`, the node's minor): every pod complies (Postgres as uid 999, toolbox, Airflow pods and init containers, worker template, Spark driver/executor templates), checked on the render and on kind.
- **Host restarted during `make kind-load`:** the kind node may keep a corrupted copy of the image (seen: `module 'pydantic' has no attribute 'VERSION'` in the pods, the migration Job and every `wait-for-airflow-migrations` in `CrashLoopBackOff`, while `docker run` of the same image is fine) that a new `kind load` does not replace. Recreate the cluster: `make kind-down kind-up kind-load k8s-install`.
- **Before the first listing**, unpause `list_files_dag`, `read_file_dag` and `transform_dag` (UI or `kubectl exec deploy/airflow-scheduler -c scheduler -- airflow dags unpause <dag>`).
- **Tests** (`test_p3_2_*`): image, values, Makefile, Secrets script and the operator's Kubernetes mode always run. `tests/integration/test_p3_2_helm_render.py` (`helm template` of both charts, RBAC, security contexts, Spark conf, kubeconform 0.8.0 by its pinned image) and `test_p3_2_image_context.py` (real Docker build context) are skipped without helm / the chart archive / Docker, unless `REQUIRE_K8S=1`.
- **Not done:** multi-node storage (RWX or object-store lake), production database/auth/Ingress/TLS, OpenLineage and OTel metrics on Kubernetes, NetworkPolicies for Airflow/Postgres/toolbox and `spark.authenticate`, Dependabot for the image digests (Dockerfile, charts), the chart and the kind tools, `trivy image` in CI, image size (4.15 GB), CI building the image.

### CI and supply chain (P3-4)

`.github/workflows/ci.yml` (jobs `test`, `pip-audit`, `trivy`, `gitleaks`), `.github/workflows/openlineage-jar-watch.yml` and `.github/dependabot.yml`. `tests/unit/test_p3_4_supply_chain.py` reads them (the workflows themselves only run on GitHub).

- **Token.** `permissions: contents: read` at the top of every workflow. A job that needs more declares it itself, and the test holds an allow-list (`gitleaks`: `pull-requests: read`; the jar watch: `issues: write`).
- **Actions.** Every `uses:` is pinned to a full 40-character commit SHA followed by a `# vX.Y.Z` comment (a test checks it). Resolve a SHA with `gh api repos/<owner>/<repo>/git/ref/tags/<tag>`; an annotated tag (`"type": "tag"`) must be dereferenced to its commit through `git/tags/<sha>`. Dependabot (`github-actions`) updates SHA and comment together.
- **Dependabot.** Weekly (Monday), grouped, `cooldown` of 7 days (security updates ignore it), never merges. Ecosystems: `uv` at `/` **and** `/transform` (two locks), `github-actions`, `docker-compose` (reads `docker-compose.yaml`, also `tag@sha256:` pins). The `ignore` rules keep the pins above out of its proposals: `apache-airflow`, `astronomer-cosmos` and `pyspark` get patches only, `pandas`, `pydantic` and `duckdb` no new major, `dbt-core` / `dbt-duckdb` patches only; `postgres` and `redis` images no new major (data volume), `prom/prometheus` patches only (LTS line 3.13.x). Every image of `docker-compose.yaml`, `postgres` (`17.12`, the index digest of the Kubernetes chart's pin) and `redis` (`7.4.12`, last 7.x of Docker Hub) included, is pinned `tag@sha256:<index digest>` (SEC-5, a test checks it, and that the Postgres pin equals the chart's); read the digest with `docker buildx imagetools inspect <image>:<tag>`, never from memory, and keep tag and digest changing together. If you relax one of those pins in `pyproject.toml`, relax its `ignore` rule in the same PR.
- **OpenLineage jar.** The jar (Makefile: `OPENLINEAGE_SPARK_VERSION` + `OPENLINEAGE_SPARK_SHA256`) is **outside Dependabot**: no ecosystem reads a Makefile. `openlineage-jar-watch.yml` runs `scripts/openlineage_jar_watch.py` every Monday, compares the pin with the latest plain `X.Y.Z` of Maven Central and opens one reminder issue when there is a newer one. It never edits the pin: the new digest is checked by hand (see the provenance comment in the Makefile).
- **`pip-audit`** (blocking): `uv export --frozen --group docs` of the root lock (the `docs` group is not a default group: it must be asked for, or it is never audited) and `uv export --frozen` of `transform/`, then `pip-audit --no-deps --disable-pip`. To run it locally: `uv export --frozen --no-hashes --no-emit-project --group docs -o /tmp/r.txt && uvx pip-audit -r /tmp/r.txt --no-deps --disable-pip`. A CVE with no fix is ignored with `--ignore-vuln <ID>` and a comment, never by disabling the step.
- **`gitleaks`** (blocking): scans the whole git history (`fetch-depth: 0`). The four findings already in the history (the generated `airflow.cfg` of the first commits, since untracked) are listed by fingerprint in `.gitleaksignore`, with their origin and no value. Locally: `gitleaks git . --redact` (never `gitleaks dir .`, it would read `.env`).
- **`trivy`** (`fs`: lockfiles, secrets, configuration): **blocking** (`exit-code: "1"`) since 2026-11-21, after reading the report of a `develop` run (Trivy v0.70.0: 0 vulnerability in both `uv.lock`, 0 secret, 0 misconfiguration in the `Dockerfile`); threshold `HIGH,CRITICAL`, `ignore-unfixed`. A finding is fixed or listed in `.trivyignore` with a comment, never turned non-blocking again (a test checks the value). Known gaps: Trivy has no built-in check for `docker-compose.yaml`; the images it references have their own job, `trivy-images` (report only, see below); the project's Helm chart is not rendered by `trivy fs` (it needs `--set-file fileListSql=...`), so not analysed; development dependencies of the lockfiles are skipped by Trivy (`pip-audit` covers them). Trivy is pinned to `v0.70.0` (after the March 2026 compromise of `trivy-action`, CVE-2026-33634).
- **`trivy-images`** (images of `docker-compose.yaml`): **report only**. Each image (list read from the compose, nothing copied into the workflow) is scanned by the official Trivy image, pinned by digest and at the version of the `trivy` job; HIGH/CRITICAL with a fix, one `::warning::` per image with findings, the table in the log. First scan (2026-11-22): `marquez-web` 171, `marquez` 33, both `postgres` 27 (26 of them the Go `stdlib` of the `gosu` binary, 1 `zlib1g`), `prometheus` 4, OTel collector and `redis` 0. Blocking is decided image by image after triage (update the image, or a dated `.trivyignore`); an error of the scan itself (network, vulnerability DB) already fails the job. The Dockerfile image is not scanned (CI does not build it).
- **Schedule.** The workflow also runs on a Monday cron: only the three security jobs run then, so a CVE published after the last push is still seen.

### Database

The `file_list` table (see `utils/SQL/script_tables.sql`) must exist in Postgres before running `list_files_dag`. `list_files_dag`'s insertion task (`insert_into_file_list`) goes through the Airflow Connection `postgres_conn_id="file_list_db"` (created by `make airflow-init` from `.env`), using `PostgresHook(...).get_sqlalchemy_engine()` built once per task run and reused for every file. `.env`/`DB_USER`/`DB_PASSWORD`/`DB_HOST`/`DB_PORT`/`DB_NAME` (`utils/db_connection.py`, `python-dotenv`) remain required for everything that does **not** go through Airflow: the Spark job (`pipeline/jobs/read_files_job.py`, which can run standalone without Airflow installed) and any standalone/script usage. `db_connection()` raises if any var is missing — there is no default/fallback — and caches one `Engine` per connection URL for the life of the process. The job resets each row to `pending` before writing it to the lake. It also copies `file_list.id` into the lake (`lake_file_id`) to order snapshots, so never reset that sequence (`TRUNCATE ... RESTART IDENTITY`, a restore).

Set `ENV=prod` to switch Spark to cluster mode (YARN/K8s-managed master, adds the `spark-excel` package for xlsx support); otherwise Spark runs `local[*]` (see `utils/spark_session.py`). In cluster deploy mode the driver does not inherit the worker's environment: pass `LAKE_ROOT` (a URI with a host, or an absolute path) through the Spark conf of that environment (`spark.yarn.appMasterEnv.LAKE_ROOT`, `spark.kubernetes.driverEnv.LAKE_ROOT`). The Kubernetes deployment (P3-2) does exactly that in its `spark-defaults.conf`, with `ENV=prod` and the `DB_*` variables (password by `secretKeyRef`).

## Architecture

This is a three-stage Airflow pipeline. It hands off from Airflow's Python/SQL world to a separate Spark process, which writes the files to a partitioned Parquet lake (raw, then staging), then to dbt, which builds DuckDB tables on top of the staging zone (`file_list` -> `lake_staging` -> `dbt_marts` Assets):

1. **`dags/local_list_files_dag.py`** (`list_files_dag`) — `Extractor` (`pipeline/extractor/extractor.py`) globs `extractor_config.yaml`'s `source_directories` for files matching `files_extensions` / `include_files`, minus `exclude_files` (fnmatch patterns), then `utils/functions_utils.insert_into_file_list` writes metadata rows (status=`pending`) into the Postgres `file_list` table, tagged with the Airflow `dag_run_id`.
2. **`dags/local_read_file_dag.py`** (`read_file_dag`) — scheduled on the `file_list` Asset (`AssetTriggeredTimetable(..., batch_asset_events=False)`, P3-5: **one run per listing**), with `max_active_runs=1`: the events that arrive during a run stay queued and each gets its own run, one after the other. The Asset is declared as `Asset(name="file_list")` in each of the three DAG files (Airflow matches Assets by name; a shared `dags.*` module would only import with the project root on `sys.path`). The insertion task of both listing DAGs declares it in `outlets`, and emits it only when it succeeds with at least one recorded file; an empty listing is skipped and emits nothing. `resolve_source_run_ids` collects the deduplicated `source_run_id` of every triggering `file_list` event (one per run since P3-5; the loop still handles several if batching is turned back on). On a manual trigger (no event) it uses `dag_run.conf["run_id"]`, stripped, rather than `params` (`Param.resolve()` memoises its value). With neither, it fails explicitly. `spark_read_files` is a `ReadFilesSparkSubmitOperator`, a subclass of `SparkSubmitOperator` (provider `apache-airflow-providers-apache-spark`) that only overrides `execute` (its extra steps only act in Kubernetes cluster mode, P3-2), mapped over `application_args=resolve_source_run_ids().map(build_application_args)`: one instance per source run, each with its own exit code and retries. It uses the `spark_default` Connection for the master and the `spark` pool, both described under "Running Airflow locally". Each instance runs `spark-submit --master <from the Connection> --name ... pipeline/jobs/read_files_job.py --run-id=<id>`. The hook passes this argument list to `subprocess.Popen` with no shell, and the run id is a single `--run-id=<id>` argument, so a value starting with `-` is not taken for an option. `env_vars={"PYTHONPATH": <project root>}` is added to the worker environment, not substituted for it (local/client mode). `LAKE_ROOT` is not set in the DAG: the job reads it from the environment inherited from the worker. `application_args` is a templated field: the value mapped from the XCom is skipped by Jinja, but never build the run id into a templated string (an f-string passed to `.partial()`, `@task.bash`...). `tests/integration/test_p1_4_read_file_dag_e2e.py` guards against that. No Spark resources (`executor_memory`...) are set in the DAG: they belong to the Connection or `spark-defaults.conf` of each environment. Airflow only orchestrates; Spark runs in its own process. The job queries `list_file_from_db(run_id)` for the rows of that `dag_run_id` and exits 3 when the run id has no rows (typo or purged data). It then loads and validates the `SourceRegistry` and resolves `LAKE_ROOT`, both **before** starting Spark. On a `ConfigError` or an invalid lake root, it marks every row of the run `failed` (`invalid reading config: ...` / `invalid lake root: ...`) and exits 5 (`EXIT_INVALID_CONFIG`). The checks run in this order: watchdog (4), run id (2), rows (3), configs and lake root (5), Spark, Java compilation of every quality regex (5), then the files. Exits 2, 3, 5 and 6 are deterministic: `ReadFilesSparkSubmitOperator` reads spark-submit's return code from the hook (`_submit_sp.returncode`, no public accessor in provider 6.3.4, pinned by `tests/unit/dags/test_p2_6_no_retry_exit_codes.py`) and turns them into an `AirflowFailException`, which the Task SDK never retries. Exits 1 and 4, signals and errors before spark-submit started keep `retries=1`. In local/client mode spark-submit returns the driver's exit code (checked with PySpark 4.3.0). In Kubernetes cluster mode (P3-2) spark-submit exits 0 whatever the driver did; the operator then reads the driver container's code that the hook parses from spark-submit's pod watcher output (`exit code: N`, `_spark_exit_code`), so the same codes are not retried (see "Kubernetes"). In YARN cluster mode the code is not available: those runs keep retrying. A final task, `publish_lake_staging`, emits the `lake_staging` Asset.
   - It runs once every mapped `spark_read_files` instance is done (`all_done`), so one permanently failing file does not block dbt forever.
   - It emits when at least one file of the resolved run ids is `processed` in `file_list` (`count_processed_files`, through the `file_list_db` Connection).
   - Otherwise it is `skipped` and emits nothing: no file processed, or `resolve_source_run_ids` failed (no XCom).
   - A DagRun takes its state from its leaves: the `fail_if_spark_failed` leaf (`one_failed`, no retry) keeps a run with a failed Spark application `failed`. It is skipped when everything succeeded.
3. **`dags/local_transform_dag.py`** (`transform_dag`, P2-4) — see "dbt transformation layer" below.

For each file, the job:
1. resolves the config once (`get_config`);
2. reads the file with `read_file(path, config=config)`;
3. resets the row to `pending`;
4. calls `write_to_lake`, with `storage.mode`:
   - writes raw;
   - if the source has a `staging_path`, cleans the rows (fully empty rows and exact duplicates removed), runs its quality checks if any (see "Quality checks"), then writes staging unless a check is blocking;
5. marks the row `processed`, or `failed` with the error.

Two files with the same name in one run are both marked `failed`, because their partitions would collide. When Airflow kills the task, `SparkSubmitHook.on_kill` only kills the `spark-submit` process (the JVM), not the Python driver it started: the job therefore runs a parent watchdog thread (`start_parent_watchdog`) that exits with code 4, without writing any status, as soon as its parent process is gone. A retry reprocesses the run's files idempotently:
- each write overwrites only its own (`lake_run_id`, `lake_source_file`) partition, with `partitionOverwriteMode=dynamic` passed as a write option, not as a session setting;
- a rewrite that produces 0 rows deletes the partition;
- every listing adds one snapshot per file; the current state is given by `latest_snapshot` (highest `lake_file_id`), not by the last write.

**The Spark job must never import Airflow**, directly or through `pipeline/`, `utils/` or `storage/`: it runs in the environment of an Airflow 3 task, where the Task SDK supervisor sets `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=airflow-db-not-allowed:///` to forbid metadata DB access, and `SparkSubmitOperator` passes that environment on to `spark-submit`. Importing `airflow` there fails in `settings.initialize()` with a SQLAlchemy `ArgumentError` (not an `ImportError`). Import Airflow code (e.g. `PostgresHook` in `utils/functions_utils.py`) inside the functions that need it. `tests/unit/jobs/test_spark_job_no_airflow_import.py` guards this. **Unpause `read_file_dag` before running a listing**: Airflow only queues Asset-triggered runs for unpaused DAGs. An event emitted while it is paused is not queued, and since Airflow 3.4 **no later run consumes it** (3.3 took every event since the previous run; 3.4 only consumes queued events when `catchup=False`, checked by `tests/integration/test_p3_5_asset_scheduling.py`). The files of that listing stay `pending`: trigger `read_file_dag` by hand with `{"run_id": "<dag_run_id of the listing>"}`, or run a new listing (it lists the files again). The same holds for `transform_dag` (the next `lake_staging` event rebuilds everything anyway).

### dbt transformation layer (`transform/`, Cosmos)

- **Project.** `transform/` is a separate uv project (`package = false`, own `uv.lock`): dbt is not installed next to Airflow.
- **Rendering.** `transform_dag` is a Cosmos (`astronomer-cosmos` 1.16) `DbtDag`, scheduled on `Asset(name="lake_staging")` with `batch_asset_events=True` (P3-5: the events queued during a `dbt build` are consumed together by **one** next run, since each run rebuilds the whole staging zone; Airflow 3.4's default is one run per event), with `max_active_runs=1` and no catchup.
  - `LoadMode.DBT_MANIFEST`: the DAG processor never runs dbt.
  - A missing manifest is an explicit import error of that file only (naming `make dbt-parse`); the other DAGs still load.
  - So is a **stale** one: a manifest older (`mtime`) than a file of `transform/{models,macros,data_tests}` or `dbt_project.yml`, whose graph would no longer match the producer's `dbt build`. Rerun `make dbt-parse` after a checkout or a pull that touches them.
  - Never set `RenderConfig.dbt_executable_path`: Cosmos 1.16's `is_dbt_installed_in_same_environment` answers True without dbt and then refuses a separate dbt.
- **Execution.** `ExecutionMode.WATCHER` with `ExecutionConfig(dbt_executable_path=<root>/transform/.venv/bin/dbt, invocation_mode=SUBPROCESS)`: one producer runs a single `dbt build`, and one sensor per model/test reports its status.
  - Model sensors are deferrable, so a **triggerer** is required (`airflow standalone` runs one).
  - The producer never reruns dbt on a retry (Cosmos skips it); a consumer's retry runs dbt for its model alone.
- **Single writer.** DuckDB accepts one writer per file:
  - every dbt task, consumers included, runs in the `dbt_duckdb` pool (1 slot, `include_deferred: false`, so a deferred sensor never blocks the producer);
  - `threads: 1` in `transform/profiles.yml`.
  - Consequence: the producer holds the only slot for its whole `dbt build`, so the sensors mostly run after it, one by one, instead of following it live. That is the price of a safe replay on a single writer.
- **`dag.test()` cannot run `transform_dag` reliably.** The Cosmos 1.16 wiring bug leaves the model sensors without the producer upstream, and `dag.test()` runs its schedulable task instances in `set` order. That order depends on `PYTHONHASHSEED`. When a sensor comes first, its inline trigger waits forever for a producer that `dag.test()` never starts. Test the DAG with a real scheduler and triggerer (`make airflow`), and the dbt project with `dbt build` (`tests/integration/test_p2_4_dbt_build.py`).
- **Assets.** Cosmos' own Assets are disabled (`emit_datasets=False`), because their URI embeds the DuckDB file path. `publish_dbt_marts`, downstream of every dbt task except the producer, emits the stable `Asset(name="dbt_marts")`.
- **Environment.** dbt gets `LAKE_ROOT` and `DBT_DUCKDB_PATH` through `ProjectConfig.env_vars`, added to the worker's environment, with the Makefile's defaults.
  - They are computed in the DAG file, without importing `storage` (pyspark); Airflow 3 workers parse the DAG file, so the worker's values apply.
  - The DAG and the Makefile expand a leading `~`, as the job does; dbt refuses it, with a message that says so.
  - dbt checks both at `on-run-start` (`transform/macros/lake.sql`), before any model, so a refusal never touches the warehouse:
    - relative paths are refused;
    - `LAKE_ROOT` must be readable by DuckDB: an absolute path, `file:` followed by an absolute path, or `s3`/`gs`/`gcs`/`r2`/`az`/`azure`/`abfss` with a bucket or container. Any other scheme (`hdfs`, `dbfs`...) is refused by name;
    - `'`, `=`, `*`, `?`, `[` and `{` are refused;
    - at least one Parquet file must exist under `<LAKE_ROOT>/staging/*/lake_run_id=*/lake_source_file=*/`. A wrong `LAKE_ROOT` used to build empty tables and still emit `dbt_marts`.
  - `profiles.yml` never auto-installs or auto-loads a DuckDB extension: a remote lake needs `extensions: [httpfs]` (or `azure`) declared there.
- **Reading the lake.** Sources use `external_location` + `formatter: template`: `read_parquet('<LAKE_ROOT>/staging/<table>/lake_run_id=*/lake_source_file=*/*.parquet', hive_partitioning=true, union_by_name=true, hive_types={'lake_run_id': 'VARCHAR', 'lake_source_file': 'VARCHAR'})`.
  - `hive_types` is mandatory: a numeric run id would otherwise be typed as an integer.
  - The explicit glob skips `_SUCCESS`, `.crc` and Spark temporary directories.
  - `lake_ingested_at` (INT96) is read back as a `TIMESTAMP` without time zone, in UTC.
- **Models.**
  - `stg_movies__current` / `stg_orders__current` = `latest_snapshot` (`qualify lake_file_id = max(lake_file_id) over (partition by lake_source_file)`).
  - `stg_movies__current` and `stg_orders__current` have an enforced contract (columns and types of their source, aligned on the `columns_type` of `imdb.yaml` / `orders.yaml`), because `union_by_name` widens a column to the widest type of every snapshot, old ones included. A test checks that every `lake_staging` table has a `columns_type`.
  - `mart_movies__by_release_year_language` is an example mart.
  - Everything is a table.
- **Empty table.** Models read through the `lake_staging()` macro, not `source()`. When a staging table has no file yet (no orders read), the macro returns an empty relation typed by the columns declared in the source, instead of DuckDB's empty-glob error.
  - `movies` and `orders` declare all the `columns_type` columns of `imdb.yaml` / `orders.yaml` (a test checks names, order and types), then the technical ones.

### Lineage (OpenLineage, P2-5)

Answers "where does this table come from, and what produced it?", from the source files to `dbt_marts`. Design note: `docs/architecture/2026-11-14-p2-5-openlineage.md`. Rule: **lineage never breaks the pipeline**; whatever goes wrong with it, the job runs with the pre-P2-5 command and a warning says why.

- **Configuration.** Environment only, nothing in the DAGs: `apache-airflow-providers-openlineage` (group `airflow`) reads `[openlineage]` / `AIRFLOW__OPENLINEAGE__*`. With no transport configured the provider is disabled. `OPENLINEAGE_DISABLED=true` or `[openlineage] disabled = True` turns it off. `make lineage-show` (`scripts/lineage_show.py`, streaming, tolerant of malformed events) summarises the dev `file` events: jobs, inputs, outputs, edges. `make lineage-clean` deletes the event files.
- **Dataset names.** They follow the OpenLineage naming spec, so the integrations meet:
  - a source file: `file` + its resolved absolute path (listing and Spark);
  - `file_list`: `postgres://<host>:<port>` + `<db>.public.file_list`;
  - a lake table: as `FilesystemDatasetUtils` of openlineage-spark 1.55.0 names it: `file` + `<LAKE_ROOT>/<zone>/<table>` (root resolved, the rest joined as text); `s3*`/`gs`/`abfs*`/`wasb*` without the leading slash; other schemes (`hdfs`, `dbfs`, `gcs`) with it;
  - a DuckDB relation: `duckdb://<DBT_DUCKDB_PATH>` + `<db>.<schema>.<table>`, `<db>` being dbt-duckdb's name for the file (`warehouse.prod.duckdb` -> `warehouse`).
- **`file_list`.** The tasks reach it through a SQLAlchemy engine, which the provider cannot see. `utils/file_list_lineage.py` declares it to Airflow's hook lineage collector: the listing tasks have the recorded files as inputs and `file_list` as output; `spark_read_files` has `file_list` as input. It never fails a task and returns at once when lineage is off.
  - The collector keeps `[lineage] max_assets_per_collector` inputs (100 by default, 1000 from the Makefile): a warning gives the number of files not declared.
  - A file whose name contains `#` or `?` is skipped with a warning: an Asset URI cannot carry it as Spark names it.
- **Spark.** The JVM listener `io.openlineage.spark.agent.OpenLineageSparkListener` (`openlineage-spark_2.13`, version and SHA-256 pinned in the Makefile, with their provenance; `make lineage-jar` downloads it over HTTPS only into `lineage/jars/`, gitignored, and always re-checks a present file) is enabled by the `spark-submit` conf that `openlineage_spark_settings()` builds in `read_file_dag`, never by an import in the job. `make spark-job` never adds it.
  - At parse time, no conf at all when: lineage is off; `OPENLINEAGE_SPARK_JAR` is unset (debug log), relative, missing, not a zip or without the listener class; the configuration is invalid (bad JSON, provider missing); the transport carries credentials (`auth`, `headers`, `custom_headers`, user info in a URL, `composite` included) or is configured outside `[openlineage] transport` (it cannot be checked), since the hook logs the Spark properties in clear; a `file` transport's `log_file_path` is not local and absolute.
  - At execution (`pre_execute=prepare_spark_lineage`, never raises): the jar must match `OPENLINEAGE_SPARK_SHA256` (hashed once per path, size and mtime), or the listener is removed; a jar for another Scala version passes the zip check but stops the SparkContext. Each application gets its own `spark-<dag>-<run>-<hash>-<map index>-<try>.jsonl`. Then `file_list` is reported.
  - Otherwise the conf adds the jar, the listener, `spark.redaction.regex` (extended to API keys, tokens and `openlineage.transport.url`), the namespace and `disabledTrimmers=KeyValueTrimmer` (`orders_dt=2026-11-14.csv` keeps its name; the lake tables, written at their root, are unaffected). A `file` transport is set by us, any other one (http) is injected by the provider.
  - The parent job info is always injected: the Spark application is the child of `read_file_dag.spark_read_files`, under the DAG run.
  - The explicit `openlineage_inject_*` values override `[openlineage] spark_inject_*`, and `--conf spark.jars` / `spark.extraListeners` override `spark-defaults.conf`. An unreachable backend never fails the job.
- **dbt.** The Cosmos watcher producer reports the datasets of its `dbt build` (`openlineage-integration-common`, already installed with Cosmos). dbt names the staging source by its DuckDB relation, Spark by its directory: `lineage_lake_staging` (`LakeStagingLineageOperator` in `transform_dag`, no upstream or downstream, outside the DuckDB pool) emits the link between the two names, one pair per `lake_staging` source of the manifest whose staging directory exists (local lake). The link is structural, not a proof that Spark wrote the table. See `transform/README.md`.
- **Tests.** `tests/integration/test_p2_5_spark_lineage.py` (real job and listener), `test_p2_5_dbt_lineage.py` (real build, Cosmos' processor), the `dag.test()` e2e (real parent injection, wrong jar digest) and `tests/unit/dags/test_p2_5_transform_lineage.py` (names checked against the listener's, by py4j) keep the Spark, bridge and dbt names equal.

There's also `dags/local_list_files_dag_python_operator.py`, an equivalent of DAG 1 built with `PythonOperator` instead of the `@task` TaskFlow API — its `dag_id` was deliberately renamed to `list_files_dag_classic` to avoid colliding with `local_list_files_dag.py`.

Every DAG/job file prepends the project root to `sys.path` manually (`Path(__file__).resolve().parent.parent...`) rather than relying on package install, since Airflow imports DAG files directly by path.

### Config-driven file reading — one validated shape

Every reading config is a YAML file validated at load time by one Pydantic model, `pipeline/config/models.py::SourceConfig`. The model uses `extra="forbid"` everywhere, so an unknown or misspelled key is an error.

```yaml
source:
  name: orders              # unique across all files
  format: csv               # csv | txt | parquet | xlsx -> ReaderFactory
  patterns: ["orders_*.csv"]  # fnmatch on the file NAME (not its directories), case-insensitive
reader:
  options: {header: true, inferSchema: true, delimiter: ",", encoding: "UTF-8"}
  columns_type: {id: int}   # optional explicit schema, csv/txt only, atomic types, in the file's column order
storage:                    # mandatory: where the job writes the file in the lake
  table_name: orders        # optional, defaults to source.name
  format: parquet           # only value accepted
  mode: overwrite           # only value accepted: append is rejected (idempotency)
  raw_path: raw/
  staging_path: staging/    # optional: no staging zone without it (the defaults write raw only)
quality:                    # optional, requires storage.staging_path (see "Quality checks")
  checks:
    - {type: not_null, column: id}
    - {type: unique, columns: [id]}
    - {type: accepted_values, column: country, values: ["FR", "NO"], severity: warn}
```

- **Layout.** `config/sources/*.yaml` holds one dedicated config per source: **empty in the repo** (P5-1, the engine knows no source; a test greps `pipeline/ storage/ utils/ dags/ scripts/` for `imdb|movies|orders`). The example sources `imdb.yaml` / `orders.yaml` and their sample files live in `examples/` (`examples/config/sources/`, `examples/data/`; `make demo` runs on them). `PIPELINE_CONFIG_ROOT` (absolute path, read once at import of `pipeline.config.registry`) replaces the root `config/`; a root without `defaults/` falls back to the engine's `config/defaults/`. `config/defaults/*.yaml` holds the fallback for each extension (`*.csv`, `*.txt`, `*.xlsx`, `*.parquet`), with the same model. The Dockerfile bakes `examples/config` with `PIPELINE_CONFIG_ROOT` (dev image, not built in P5-1: unverified). `config/extractor_config.yaml` is the listing config and is never scanned by the reader.
- **Patterns.** A dedicated source describes one format, so its patterns include the extension (`*imdb*.csv`, `orders_*.csv`). An IMDB file in `.parquet` then takes `default_parquet` instead of being read as CSV.
- **Options.** `reader.options` is passed as-is to Spark (csv/txt and parquet, scalar values only) or to `pyspark.pandas.read_excel` (xlsx). `xls` is not supported, because `xlrd` is not installed. The options are checked against a per-format whitelist in `pipeline/config/reader_options.py`, which cites its sources. That check is what rejects a typo such as `infer_schema`.
  - **Tests.** The CSV whitelist is compared with the keywords of the installed `DataFrameReader.csv` and the Excel one with `ps.read_excel`. The Parquet whitelist is compared with the Spark 4.3.0 docs (`parquet()` has no option keywords). A test pins PySpark 4.3: after an upgrade, review the whitelists.
  - **Aliases.** Declaring both aliases of a pair (`sep` and `delimiter`, `encoding` and `charset`) is rejected, because Spark would silently keep only one.
  - **Types.** Boolean, integer and numeric Spark options are typed (`SPARK_OPTION_TYPES`), so `header: "yes"` is a validation error: write `true`/`false` without quotes.
  - **No hidden defaults.** An option missing from the YAML takes the Spark or pandas default (for example, CSV `header=false`).
- **Explicit schema.** `columns_type` types are checked against `pipeline/config/types.py` (`SUPPORTED_COLUMN_TYPES` / `get_spark_type`). `pipeline/file_reader/utils.py` only re-exports them, and `pipeline.config` must not import anything from `pipeline.file_reader` (a circular import, which a test checks). Only atomic types are accepted, because Spark's CSV source cannot read ARRAY, MAP or STRUCT columns. A list column such as IMDB `genre_list` is therefore read as `string` and split later. `build_schema` turns the types into the `StructType` passed to `DataFrameReader.schema`. The schema is positional, so keep the file's column order (Spark then ignores `inferSchema`). With `enforceSchema: false` (as in `orders.yaml`), Spark also checks each file's header against the declared names (count and names, case-insensitive) and fails the file on a mismatch, instead of reading another layout with shifted or null columns.
- **Loading.** `BaseFileReader(spark)` builds a `SourceRegistry` once from `DEFAULT_CONFIG_ROOT`, which is resolved from the code, not from the cwd. It can also receive an already-loaded registry through `registry=`, which cannot be combined with `config_path`. An invalid file, a duplicate `source.name` or nested lake tables raise one `ConfigError`, naming each YAML file and field path (for example `reader.options.infer_schema: ...`). The errors of one file are aggregated, including when `storage` is missing. Through `load_source_config` (the registry and the job), a field error in one section does not hide the cross-section errors of the valid ones; a direct `SourceConfig.model_validate` only reports the field errors.
  - Lake tables may not be nested, whether between the raw and staging tables of one source or between sources, and the check ignores case. Two sources may share exactly the same table, but not the same table with another case. `quality/` is reserved for the check results: no `raw_path` or `staging_path` may start with it.
  - The Spark job loads the registry itself before starting Spark and exits 5 (see above).
- **Matching** (`SourceRegistry.match`) works on the file's basename, lowercased:
  1. Dedicated sources first. Exactly one match wins; two or more raise `AmbiguousConfigError`, naming them.
  2. Otherwise the defaults, with the same rule.
  3. Otherwise `NoMatchingConfigError`, and the job writes a `failed` row.
  
  There is no `default_format`: an unknown extension is an error, not a CSV read.
- **Readers.** `CSVReader`, `ParquetReader` and `ExcelReader` all implement `read(path, config: SourceConfig) -> DataFrame` and do not inherit from `BaseFileReader` (the router). The router calls `ReaderFactory.get_reader(config.source.format, spark)`. The keys of `READER_CLASSES` must equal `FileFormat`, which a test checks. `ExcelReader` imports `pyspark.pandas` inside `read()`: reading a CSV must not load pandas-on-Spark, which a subprocess test checks.
- **Adding a source.** Create `config/sources/<name>.yaml` with patterns that do not overlap another dedicated source, and a `storage` section whose tables neither contain nor sit inside another source's tables. Then run `uv run pytest tests/unit/config` (parametrized on every YAML present) and `uv run pytest tests/unit/file_reader -k synthetic`, which reads a synthetic file with every csv/txt config. If the whitelist rejects a legitimate option, add it to `reader_options.py` with its documentation source, and add a non-text option to `SPARK_OPTION_TYPES` too. A new format means a reader, an entry in `READER_CLASSES` and `FileFormat`, a whitelist and a test.
- **Dependencies.** `pydantic` is a runtime dependency, because the job runs with `uv sync --no-group airflow`. `pipeline/config` must not import Airflow.

### Quality checks

Quality checks are native: Pydantic models in `pipeline/config/quality.py`, a pure-Spark engine in `pipeline/quality/checks.py`, and no Soda or Great Expectations dependency. Neither module may import Airflow or `pipeline.file_reader`.

- **Declaring checks.** `quality.checks` is a list discriminated on `type`:
  - `not_null` (`column`);
  - `unique` (`columns`, the combination; rows with a null key are ignored);
  - `accepted_values` (`column`, `values`);
  - `range` (`column`, `min` and/or `max`, inclusive, numeric column);
  - `regex` (`column`, `pattern`);
  - `row_count` (`min` and/or `max`).
  
  Each check has a stable name, either `name` or a derived one (`not_null(id)`, `unique(a,b)`, `row_count`), and a `severity` of `error` (default) or `warn`.
- **Load-time rules.**
  - `quality` requires `storage.staging_path`, because the checks decide whether staging is written.
  - With `columns_type`, every column a check names must be declared there, and `range` needs a numeric type.
  - Identical checks and duplicate names are refused.
  - Bounds are int64 integers or finite numbers.
- **Pitfalls.**
  - `accepted_values` only takes **text**, compared with the text form of the column. PyYAML reads YAML 1.1 and converts unquoted scalars (`NO`/`yes`/`on`/`off` become booleans, `007` becomes 7, `0123` becomes 83), so write `"NO"`, `"007"`, `"true"`, `"1"`. It is refused on a column declared `float`/`double`: use `range`.
  - Nulls are only counted by `not_null`. NaN is not null, but `range` counts it as out of bounds on a float/double column.
  - `regex` uses Spark's `rlike`, a partial match, so anchor with `^...$`. At load it must compile in Python without syntax specific to Java or to Python. The job compiles every regex in Java right after Spark starts (exit 5 on failure).
- **Execution.** The checks run on the rows cleaned for staging: one aggregation for all per-row checks and the row count, plus one `groupBy` per `unique`, with no `collect` of data.
- **Statuses.** Each check ends in one of four statuses:
  - `pass`: no failing row;
  - `fail`: failing rows, severity `error`;
  - `warn`: failing rows, severity `warn`;
  - `error`: the check could not run (missing column, regex refused, Spark error), and `failed_rows` is null.
  
  A check is **blocking** when its severity is `error` and its status `fail` or `error`. A `warn` never blocks, even in status `error`, which is logged at ERROR level.
- **Blocking failure.** Raw is written. The file's staging partition is removed (`ParquetLakeWriter.delete_partition`) and not rewritten. The file becomes `failed` with `quality checks failed: <name>: <detail>; ...`, and the job exits 6 at the end when no file failed for another reason (1 otherwise). If the engine itself raises, the same thing happens (it fails closed), with `quality checks could not run: <cause>`, but the job exits 1: that cause may be transient.
- **Results.** They go to `<LAKE_ROOT>/quality/<table>/`, one row per check, partitioned by (`lake_run_id`, `lake_source_file`) and overwritten by a retry. When a source has a staging zone, the job first removes that (run, file) partition of the results, whether or not the source has a `quality` section. If staging fails after passing checks, the results of that attempt are removed. Read the results together with the status in `file_list`.
- **Repo configs.** `examples/config/sources/imdb.yaml` declares `not_null(id)` and `unique(id)` as `warn` for a first real run: switch them to `error` once `quality/movies` has been checked on the real file (no IMDB file was available for P2-6: the verification procedure, including the `multiLine` comparison, is in `docs/architecture/2026-11-14-p2-6-phase2-followups.md`). `orders.yaml` declares `not_null(order_id)`, `unique(order_id)` and `row_count min 1`, all `warn`. The defaults have no checks, since they have no staging.
- **Retries.** A deterministic quality failure exits 6, which `read_file_dag` does not retry (P2-6, see "Running the Spark job standalone").

### Processing and storage layers

- `pipeline/processor/base_processor.py` (`BaseProcessor`) wraps a Spark `DataFrame` with chainable cleanup methods (`remove_empty_rows`, `drop_na_rows`, `fill_na`, `remove_duplicates`, `cast_column`, `rename_columns`), each returning `self`.
- `storage/base_writer.py` defines the `BaseWriter` ABC (`write(data, target, **kwargs)`); `storage/postgresql_writer.py` (`PostgresWriter`) implements it for both Spark DataFrames (via JDBC) and Pandas DataFrames (via `to_sql`). Writers must subclass `BaseWriter` and implement `write()` to satisfy the interface.
- `storage/parquet_lake_writer.py` is the writer the Spark job uses.
  - `ParquetLakeWriter.write(data, target, mode="overwrite", *, run_id, source_file, file_id)` writes `<LAKE_ROOT>/<zone path><table>/`, partitioned by `lake_run_id` and `lake_source_file`. It adds the columns `lake_file_id` and `lake_ingested_at`, and returns the number of rows written, measured with an `Observation` (no extra job).
  - It also provides `resolve_lake_root` and `latest_snapshot`. `delete_partition(spark, target, run_id=, source_file=)` removes one (run, file) partition, and the run directory if it becomes empty.
  - Technical columns never start with `_` or `.`: pyarrow and pandas ignore such partition directories and would read 0 rows, while Spark reads them.
  - Reading a `default_*` table as a whole needs `mergeSchema`, because it mixes unrelated files.
  - A source read with `inferSchema` and no `columns_type` can infer different types from one file to the next, and a mixed table may then fail to read. Declare `columns_type` for sources read as a whole.
- `utils/path_utils.normalize_spark_path` converts local paths to `file://` (or `file:///` on Windows) URIs for Spark, leaving already-distributed URIs (`hdfs://`, `s3://`, `gs://`, `dbfs:/`, `abfss://`, `adl://`) untouched. `lake_relative_parts` and `check_lake_name` keep zone paths and table names inside `LAKE_ROOT`. In tests, `Path.as_uri()` encodes `=` as `%3D`, which Hadoop does not decode: use `normalize_spark_path` for a partition directory.

## Targets & ground rules (read before changing code)

- **Target stack:** Airflow 3.4.x (`airflow.sdk` imports, providers for operators), PySpark 4.x, Python 3.12+. Airflow 2 is end-of-life.
- **Idempotency first:** tasks and the Spark job work on the scope of the current run (`run_id` / data interval), never on "today". Lake writes replace the (run, file) partition and never append. Read the current state with `latest_snapshot`.
- **Readers return a Spark DataFrame** and take the validated `SourceConfig`. When touching a reader, test it with a dedicated source config (`examples/config/sources/` or a `tmp_path` one) and with its `config/defaults/` fallback.
- **Never** read `.env`, commit secrets, or push. Work on a `feat/` or `fix/` branch, never directly on `main`/`develop`.
- Every bug fix comes with a regression test named after its backlog ID.
- The current audit baseline and the prioritized backlog live in `docs/audit/` (start with `2026-11-05-baseline.md`).
- Design notes for delivered backlog items live in `docs/architecture/` (e.g. `2026-11-14-p2-4-transform-layer.md` for the dbt layer).

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
