# Local dev entry points. `make` (or `make help`) lists the targets.
# The quality targets run the same commands as CI (.github/workflows/ci.yml).

.DEFAULT_GOAL := help

UV ?= uv

# A virtualenv activated in the shell (pyenv...) makes uv warn that
# `VIRTUAL_ENV ... does not match the project environment path .venv`.
# uv always uses the project's .venv anyway: drop the variable for recipes.
unexport VIRTUAL_ENV

# --- Airflow environment ------------------------------------------------------
# AIRFLOW_HOME must be the project root: without it Airflow falls back to
# ~/airflow/airflow.cfg (another metadata DB, other settings). The metadata DB
# is then the SQLite file ./airflow.db created by `airflow db migrate`.
# `:=` ignores an AIRFLOW_HOME exported by the shell; a command-line value
# (`make airflow-init AIRFLOW_HOME=/tmp/x`) still wins.
# Exported only for the Airflow targets: `make test` keeps the CI environment.
AIRFLOW_HOME := $(CURDIR)
AIRFLOW_TARGETS := airflow-init airflow
$(AIRFLOW_TARGETS): export AIRFLOW_HOME := $(AIRFLOW_HOME)
$(AIRFLOW_TARGETS): export AIRFLOW__CORE__DAGS_FOLDER := $(CURDIR)/dags
$(AIRFLOW_TARGETS): export AIRFLOW__CORE__LOAD_EXAMPLES := False
# Cosmos sends anonymous usage metrics over HTTP by default: off in dev.
$(AIRFLOW_TARGETS): export AIRFLOW__COSMOS__ENABLE_TELEMETRY := False

# --- OpenLineage (P2-5) ---------------------------------------------------------
# Lineage of the pipeline, configured for the Airflow targets only (`make test`
# keeps the CI environment, where it is off). Default transport: `file`, one
# JSON file per Airflow event and one spark-<application>.jsonl per Spark
# application, under $(LINEAGE_DIR)/events (gitignored); `make lineage-show`
# summarises them, `make lineage-clean` deletes them.
# OPENLINEAGE_URL=http://localhost:5002 (Marquez, `make lineage-up`) switches
# to http. OPENLINEAGE_DISABLED=true (or `[openlineage] disabled = True`)
# turns it off. A shell-exported AIRFLOW__OPENLINEAGE__TRANSPORT/NAMESPACE wins.
# The transport JSON is built by scripts/openlineage_transport.py from
# environment variables (json.dumps), never by interpolation: `make
# lineage-transport` prints it.
LINEAGE_DIR ?= $(CURDIR)/lineage
LINEAGE_EVENTS_DIR = $(LINEAGE_DIR)/events
OPENLINEAGE_URL ?=
OPENLINEAGE_NAMESPACE ?= airflow_project_one
AIRFLOW__OPENLINEAGE__NAMESPACE ?= $(OPENLINEAGE_NAMESPACE)
PYTHON ?= $(UV) run python
# Spark listener (JVM): openlineage-spark for Scala 2.13 (PySpark 4), one
# self-contained jar from Maven Central, pinned by version and SHA-256.
# Without a valid file, read_file_dag runs Spark without its listener.
# Provenance of the digest (2026-11-14): equal to the `.sha256` Maven Central
# publishes next to the jar, and the jar's `.asc` signature verifies with the
# key B4AD C6AE E76F 4EED 97FA 78A1 C6C9 B79C 9583 2D71 (an OpenLineage
# maintainer's; the same key signs openlineage-java 1.55.0).
OPENLINEAGE_SPARK_VERSION := 1.55.0
OPENLINEAGE_SPARK_SHA256 := da278ffadc9c43b063169d3991890a97924d5bb1d21954c65e2fe785dd373d70
OPENLINEAGE_SPARK_JAR ?= $(LINEAGE_DIR)/jars/openlineage-spark_2.13-$(OPENLINEAGE_SPARK_VERSION).jar
OPENLINEAGE_SPARK_URL := https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.13/$(OPENLINEAGE_SPARK_VERSION)/openlineage-spark_2.13-$(OPENLINEAGE_SPARK_VERSION).jar
$(AIRFLOW_TARGETS): export AIRFLOW__OPENLINEAGE__NAMESPACE := $(AIRFLOW__OPENLINEAGE__NAMESPACE)
$(AIRFLOW_TARGETS): export OPENLINEAGE_SPARK_JAR := $(OPENLINEAGE_SPARK_JAR)
# read_file_dag checks the jar against it when the task runs.
$(AIRFLOW_TARGETS): export OPENLINEAGE_SPARK_SHA256 := $(OPENLINEAGE_SPARK_SHA256)
# The hook lineage collector keeps 100 inputs per task by default: a listing
# of more files would declare only the first 100 (a warning says how many
# are missing). Larger values make larger events.
$(AIRFLOW_TARGETS): export AIRFLOW__LINEAGE__MAX_ASSETS_PER_COLLECTOR := 1000
# Passed to recipes through the environment only, never interpolated.
airflow lineage-transport lineage-clean: export LINEAGE_EVENTS_DIR := $(LINEAGE_EVENTS_DIR)
airflow lineage-transport: export OPENLINEAGE_URL := $(OPENLINEAGE_URL)

# --- Metrics (OpenTelemetry, P3-3) -------------------------------------------------
# Airflow's native OpenTelemetry metrics, configured for `make airflow` only
# (`make test` keeps the CI environment, where metrics are off). Off unless
# OTEL_EXPORTER_OTLP_ENDPOINT is set: with no endpoint, nothing below is
# exported and Airflow keeps its own default (`[metrics] otel_on = False`).
#   make observability-up                                   # collector + Prometheus
#   OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 make airflow
# Airflow >= 3.2 reads the standard OTEL_* variables (the [metrics] otel_host /
# otel_port options are deprecated). A value exported by the shell wins, so
# does OTEL_EXPORTER_OTLP_HEADERS (credentials of a remote collector: never in
# the endpoint, Airflow logs it: an endpoint with user info or a query string is
# refused, `make airflow` then starts without metrics). `make metrics-env`
# prints what is exported, user info and query string masked.
# A collector that is down never fails a task, but every process waits about
# 10 s at exit: `make airflow` checks the endpoint first and starts without
# metrics when it does not answer (scripts/otel_endpoint_check.py,
# docs/architecture/2026-11-17-p3-3-observability.md).
OTEL_EXPORTER_OTLP_ENDPOINT ?=
METRICS_TARGETS := airflow metrics-env
ifneq ($(strip $(OTEL_EXPORTER_OTLP_ENDPOINT)),)
$(if $(findstring @,$(OTEL_EXPORTER_OTLP_ENDPOINT)),$(warning OTEL_EXPORTER_OTLP_ENDPOINT contains `@`: Airflow logs the endpoint, pass credentials in OTEL_EXPORTER_OTLP_HEADERS instead))
$(METRICS_TARGETS): export AIRFLOW__METRICS__OTEL_ON ?= True
$(METRICS_TARGETS): export OTEL_EXPORTER_OTLP_ENDPOINT := $(strip $(OTEL_EXPORTER_OTLP_ENDPOINT))
$(METRICS_TARGETS): export OTEL_EXPORTER_OTLP_PROTOCOL ?= http/protobuf
$(METRICS_TARGETS): export OTEL_SERVICE_NAME ?= $(OPENLINEAGE_NAMESPACE)
# Airflow's default is 60 s; Prometheus scrapes the collector every 30 s.
$(METRICS_TARGETS): export OTEL_METRIC_EXPORT_INTERVAL ?= 15000
endif

# Env file with DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME for the
# file_list_db Connection. Empty = the script's default (.env).
ENV_FILE ?=

# --- dbt (transform/, P2-4) ----------------------------------------------------
# Separate uv project: dbt is not installed next to Airflow. transform_dag
# (Cosmos) calls this same executable.
TRANSFORM_DIR := $(CURDIR)/transform
DBT := $(TRANSFORM_DIR)/.venv/bin/dbt
DBT_FLAGS := --project-dir $(TRANSFORM_DIR) --profiles-dir $(TRANSFORM_DIR)
# Same defaults as the Spark job (LAKE_ROOT) and transform_dag. A value
# exported by the shell or given on the command line wins; a leading `~/` is
# expanded (as the job and the DAG do), dbt refuses a relative value.
# Exported for the dbt targets only (`make spark-job` keeps reading LAKE_ROOT
# from the shell, or its own default).
LAKE_ROOT ?= $(CURDIR)/lake
DBT_DUCKDB_PATH ?= $(TRANSFORM_DIR)/warehouse.duckdb
expand_home = $(patsubst ~/%,$(HOME)/%,$(patsubst ~,$(HOME),$(strip $(1))))
DBT_TARGETS := dbt-parse dbt-build
$(DBT_TARGETS): override export LAKE_ROOT := $(call expand_home,$(LAKE_ROOT))
$(DBT_TARGETS): override export DBT_DUCKDB_PATH := $(call expand_home,$(DBT_DUCKDB_PATH))
$(DBT_TARGETS): export DBT_SEND_ANONYMOUS_USAGE_STATS := False

# --- docker-compose Postgres ----------------------------------------------------
# Host port of the project's Postgres: must match docker-compose.yaml.
# Override it only to try db-port-check against another port.
PG_HOST_PORT := 5434
PG_CONTAINER := airflow_postgres

.PHONY: help install lint fmt test check db-port-check db-up db-down \
	airflow-init airflow spark-job transform-sync dbt-parse dbt-build \
	lineage-jar lineage-show lineage-transport lineage-clean \
	lineage-up lineage-down metrics-env observability-up observability-down

##@ Help

help: ## List the targets
	@awk 'BEGIN {FS = ":.*## "} \
		/^##@ / {printf "\n%s\n", substr($$0, 5)} \
		/^[a-zA-Z0-9_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

##@ Quality (same commands as CI)

install: ## Install runtime + airflow + dev dependencies into .venv (uv sync)
	$(UV) sync

lint: ## Ruff lint and format check
	$(UV) run ruff check .
	$(UV) run ruff format --check .

fmt: ## Format with ruff, then apply ruff's safe lint fixes
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

test: dbt-parse ## Run the test suite (includes the DAG import check; generates the dbt manifest first)
	$(UV) run pytest tests/ -q

check: ## CI order: both uv.lock up to date, then lint, then tests
	$(UV) lock --check
	$(UV) --directory transform lock --check
	@$(MAKE) --no-print-directory lint
	@$(MAKE) --no-print-directory test

##@ Services (docker compose)

db-port-check: ## Fail if host port 5434 is held by anything but this project's Postgres
	@owner="$$(docker ps --filter 'publish=$(PG_HOST_PORT)' --format '{{.Names}}' 2>/dev/null | tr '\n' ' ' | sed 's/ $$//')"; \
	if [ "$$owner" = "$(PG_CONTAINER)" ]; then \
		echo "Port $(PG_HOST_PORT) already held by $(PG_CONTAINER) (this project): OK"; \
	elif [ -n "$$owner" ]; then \
		echo "error: host port $(PG_HOST_PORT) is held by Docker container(s): $$owner" >&2; \
		echo "       stop it, or change the port in docker-compose.yaml and DB_PORT in .env" >&2; \
		exit 1; \
	else \
		listener="$$( (ss -ltnpH 'sport = :$(PG_HOST_PORT)' || lsof -nP -iTCP:$(PG_HOST_PORT) -sTCP:LISTEN) 2>/dev/null )"; \
		if [ -n "$$listener" ]; then \
			echo "error: host port $(PG_HOST_PORT) is held by a process outside Docker:" >&2; \
			echo "$$listener" >&2; \
			exit 1; \
		fi; \
		echo "Port $(PG_HOST_PORT) is free"; \
	fi

db-up: db-port-check ## Start Postgres (host port 5434) and Redis in the background
	docker compose up -d

db-down: ## Stop the containers (keeps the data volume: no -v)
	docker compose down

##@ Airflow (AIRFLOW_HOME = project root, SQLite metadata DB)

airflow-init: ## Migrate the metadata DB, create/update Connections, import pools (idempotent)
	$(UV) run airflow db migrate
	$(UV) run python scripts/dev_airflow_connections.py $(if $(ENV_FILE),--env-file '$(ENV_FILE)')
	$(UV) run airflow pools import deploy/airflow/pools.json

# dbt-parse first: transform_dag is rendered from the dbt manifest. The
# triggerer started by `standalone` is required by transform_dag (deferrable
# Cosmos sensors). LAKE_ROOT / DBT_DUCKDB_PATH are not exported: the DAG
# applies the same defaults as the dbt targets above.
# The lineage jar is fetched (or re-checked) once; a failed download
# (offline) only warns: Spark then runs without its listener, the pipeline is
# unchanged.
# Same rule for the metrics (P3-3): with an endpoint that accepts no connection
# (collector not started), Airflow starts with its metrics off and a warning,
# instead of every process waiting for a final flush that cannot succeed.
airflow: dbt-parse ## Run Airflow standalone (api-server, scheduler, dag-processor, triggerer)
	@mkdir -p -- "$$LINEAGE_EVENTS_DIR"
	@$(if $(filter true TRUE True 1 t T,$(strip $(OPENLINEAGE_DISABLED))),true,\
		$(MAKE) --no-print-directory lineage-jar || \
		echo "warning: no OpenLineage Spark jar, Spark runs without its listener" >&2)
	case "$$AIRFLOW__METRICS__OTEL_ON" in True|true|TRUE|1|t|T) \
		$(PYTHON) scripts/otel_endpoint_check.py || export AIRFLOW__METRICS__OTEL_ON=False ;; \
	esac; \
	AIRFLOW__OPENLINEAGE__TRANSPORT="$${AIRFLOW__OPENLINEAGE__TRANSPORT:-$$($(PYTHON) scripts/openlineage_transport.py)}" \
		$(UV) run airflow standalone

##@ Spark

spark-job: export RUN_ID := $(RUN_ID)
spark-job: ## Run the Spark job outside Airflow: make spark-job RUN_ID=<dag_run_id>
	@if [ -z "$$RUN_ID" ]; then \
		echo "error: RUN_ID is empty. Usage: make spark-job RUN_ID=<dag_run_id of list_files_dag>" >&2; \
		exit 1; \
	fi
	$(UV) run spark-submit pipeline/jobs/read_files_job.py "--run-id=$$RUN_ID"

##@ dbt (transform/: DuckDB on top of the lake's staging zone)

transform-sync: ## Create/update transform/.venv (dbt-core, dbt-duckdb) from transform/uv.lock
	$(UV) --directory transform sync --locked

dbt-parse: transform-sync ## Write transform/target/manifest.json (transform_dag is rendered from it)
	$(DBT) parse $(DBT_FLAGS)

dbt-build: transform-sync ## dbt build outside Airflow (LAKE_ROOT, DBT_DUCKDB_PATH overridable)
	$(DBT) build $(DBT_FLAGS)

##@ Lineage (OpenLineage, P2-5)

lineage-jar: export OL_JAR := $(OPENLINEAGE_SPARK_JAR)
lineage-jar: export OL_SHA256 := $(OPENLINEAGE_SPARK_SHA256)
lineage-jar: export OL_URL := $(OPENLINEAGE_SPARK_URL)
lineage-jar: ## Download the pinned openlineage-spark jar, or re-check the present one (SHA-256)
	@set -e; \
	if [ -f "$$OL_JAR" ]; then \
		if echo "$$OL_SHA256  $$OL_JAR" | sha256sum -c --status -; then \
			echo "$$OL_JAR: present, SHA-256 OK"; exit 0; \
		fi; \
		echo "warning: $$OL_JAR does not match the pinned SHA-256: removed, downloading again" >&2; \
		rm -f -- "$$OL_JAR"; \
	fi; \
	mkdir -p -- "$$(dirname -- "$$OL_JAR")"; \
	trap 'rm -f -- "$$OL_JAR.part"' EXIT; \
	curl -fsSL --proto '=https' --proto-redir '=https' --retry 3 -o "$$OL_JAR.part" "$$OL_URL"; \
	echo "$$OL_SHA256  $$OL_JAR.part" | sha256sum -c --quiet -; \
	mv -- "$$OL_JAR.part" "$$OL_JAR"; \
	echo "$$OL_JAR: downloaded, SHA-256 OK"

lineage-transport: ## Print the OpenLineage transport (JSON) that `make airflow` uses
	@$(PYTHON) scripts/openlineage_transport.py

# Only the event files the transports write (airflow-*.json, spark*.jsonl),
# at the top of the events directory: never the jar, never a subdirectory.
lineage-clean: ## Delete the lineage event files of lineage/events (keeps the listener jar)
	@test -n "$$LINEAGE_EVENTS_DIR" && test -d "$$LINEAGE_EVENTS_DIR" || \
		{ echo "$$LINEAGE_EVENTS_DIR: nothing to clean"; exit 0; }; \
	find "$$LINEAGE_EVENTS_DIR" -maxdepth 1 -type f \
		\( -name 'airflow-*.json' -o -name 'spark*.jsonl' \) -print -delete | wc -l | \
		xargs printf "%s event file(s) deleted from $$LINEAGE_EVENTS_DIR\n"

lineage-show: ## Summarise the lineage events of lineage/events: jobs, inputs, outputs
	$(UV) run python scripts/lineage_show.py "$(LINEAGE_EVENTS_DIR)"

lineage-up: ## Start Marquez (API 127.0.0.1:5002, web UI http://127.0.0.1:3002), profile `lineage`
	docker compose --profile lineage up -d marquez-db marquez-api marquez-web

lineage-down: ## Stop and remove the Marquez containers (keeps their data volume)
	docker compose --profile lineage rm --stop --force marquez-web marquez-api marquez-db

##@ Metrics and alerts (OpenTelemetry + Prometheus, P3-3)

# The user info of a URL and its query string are masked (`***`): an endpoint
# that carries credentials is refused by `make airflow` (scripts/otel_endpoint_check.py),
# but this target must not print them either. *_HEADERS are never printed.
metrics-env: ## Print the OpenTelemetry variables `make airflow` exports (metrics are off without OTEL_EXPORTER_OTLP_ENDPOINT)
	@vars="$$(env | grep -E '^(AIRFLOW__METRICS__|OTEL_)' | grep -v '_HEADERS=' \
		| sed -E 's|(://)[^/?[:space:]]*@|\1***@|; s|\?.*|?***|' | sort || true)"; \
	if [ -n "$$vars" ]; then printf '%s\n' "$$vars"; \
	else echo "metrics off: OTEL_EXPORTER_OTLP_ENDPOINT is not set"; fi

observability-up: ## Start the OTel collector (OTLP 127.0.0.1:4318) and Prometheus (http://127.0.0.1:9092), profile `observability`
	docker compose --profile observability up -d otel-collector prometheus

observability-down: ## Stop and remove the collector and Prometheus containers (keeps the prometheus-data volume)
	docker compose --profile observability rm --stop --force otel-collector prometheus
