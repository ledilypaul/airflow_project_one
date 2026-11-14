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
	airflow-init airflow spark-job transform-sync dbt-parse dbt-build

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
airflow: dbt-parse ## Run Airflow standalone (api-server, scheduler, dag-processor, triggerer)
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
