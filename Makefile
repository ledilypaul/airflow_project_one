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

# --- Kubernetes (kind + Helm, P3-2) -------------------------------------------------
# Optional: nothing below is used by `make airflow`, `make test` or `make check`.
# Design note: docs/architecture/2026-11-19-p3-2-kubernetes.md.
# Tools are installed in K8S_TOOLS_DIR (default ~/.local/bin), downloaded over
# HTTPS only, each checked against the SHA-256 pinned here. Provenance
# (2026-11-19): every digest equals the checksum file published next to the
# binary (kind: GitHub release asset `.sha256sum`; kubectl: dl.k8s.io `.sha256`;
# Helm: get.helm.sh `.sha256sum`, also listed in the release notes, and the
# tarball's `.asc` verifies with the Helm KEYS file).
K8S_TOOLS_DIR ?= $(HOME)/.local/bin
KIND_VERSION := v0.33.0
KIND_SHA256 := aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d
KUBECTL_VERSION := v1.37.2
KUBECTL_SHA256 := c04487523e7aa7d1493f7808174c899db93d7a9cf57531420cd76b9bd415f8f4
HELM_VERSION := v4.3.2
HELM_SHA256 := d1c0278d46beab79582d7e50f5b3dc206944ba8e577cde618513fd07daa53983
# SHA-256 of the `helm` binary inside that tarball (computed from the verified
# tarball), so that an installed helm can be re-checked like kind and kubectl.
HELM_BIN_SHA256 := 876242ea89b1fc07692e179d9abdc47028979fb64b20d85448e85193c8b04931
KIND := $(K8S_TOOLS_DIR)/kind
KUBECTL := $(K8S_TOOLS_DIR)/kubectl
HELM := $(K8S_TOOLS_DIR)/helm
# Node image of kind v0.33.0 for Kubernetes 1.37.0 (digest from the kind release
# notes: "You *must* use the @sha256 digest").
KIND_NODE_IMAGE := kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
KIND_CLUSTER ?= airflow-project-one
K8S_NAMESPACE ?= airflow-project-one
K8S_CONTEXT := kind-$(KIND_CLUSTER)
# Official Apache Airflow chart: version, SHA-256 (= the `digest` of
# https://airflow.apache.org/index.yaml, and the .sha512 published next to it
# matches; its .asc verifies with the Airflow KEYS file).
AIRFLOW_CHART_VERSION := 1.22.0
AIRFLOW_CHART_SHA256 := 1f7d1dfe3d58e2c54899950aba907a43625a18c8b6fb3c54760c21592129a5b6
AIRFLOW_CHART_URL := https://archive.apache.org/dist/airflow/helm-chart/$(AIRFLOW_CHART_VERSION)/airflow-$(AIRFLOW_CHART_VERSION).tgz
K8S_CACHE_DIR := $(CURDIR)/.k8s
AIRFLOW_CHART := $(K8S_CACHE_DIR)/airflow-$(AIRFLOW_CHART_VERSION).tgz
K8S_RELEASE_AIRFLOW := airflow
K8S_RELEASE_PROJECT := project
# The project chart; overridable to install a copy with another version (the
# chart-version upgrade check of P3-2).
PROJECT_CHART ?= deploy/k8s/charts/project
# Pod Security Admission (SEC-1): the namespace enforces, warns and audits the
# `restricted` standard, pinned to the cluster's minor version (kind node 1.37).
PSA_LEVEL := restricted
PSA_VERSION := v1.37
# The project image (Dockerfile): Airflow 3.4.0 + the project + dbt + Spark (JRE,
# PySpark's spark-submit, Spark's Kubernetes entrypoint). One image for the
# Airflow pods and for the Spark driver and executors.
IMAGE_REPOSITORY ?= airflow-project-one
IMAGE_TAG ?= dev
IMAGE := $(IMAGE_REPOSITORY):$(IMAGE_TAG)

.PHONY: help install lint fmt test check db-port-check db-up db-down \
	airflow-init airflow spark-job transform-sync dbt-parse dbt-build \
	lineage-jar lineage-show lineage-transport lineage-clean \
	lineage-up lineage-down metrics-env observability-up observability-down \
	k8s-tools kind-up kind-down image kind-load k8s-chart k8s-install \
	k8s-uninstall k8s-load-data k8s-spark-clean k8s-ui k8s-ui-password \
	api-dags api-runs api-trigger

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

##@ Airflow REST API (P4-1; AIRFLOW_API_URL, AIRFLOW_API_PASSWORD or AIRFLOW_API_TOKEN from the environment)

api-dags: ## List the DAGs and whether they are paused
	$(UV) run python scripts/airflow_api.py dags

api-runs: export API_DAG := $(DAG)
api-runs: ## Latest runs of a DAG: make api-runs DAG=<dag_id>
	@if [ -z "$$API_DAG" ]; then echo "error: DAG is empty. Usage: make api-runs DAG=<dag_id>" >&2; exit 1; fi
	$(UV) run python scripts/airflow_api.py runs "$$API_DAG"

api-trigger: export API_DAG := $(DAG)
api-trigger: export API_CONF := $(if $(CONF),$(CONF),{})
api-trigger: ## Start a run: make api-trigger DAG=<dag_id> [CONF='{"run_id": "..."}']
	@if [ -z "$$API_DAG" ]; then echo "error: DAG is empty. Usage: make api-trigger DAG=<dag_id> [CONF=<json>]" >&2; exit 1; fi
	$(UV) run python scripts/airflow_api.py trigger "$$API_DAG" --conf "$$API_CONF"

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

##@ Kubernetes (kind + Helm, P3-2; optional, never used by make airflow / make check)

# Each tool is kept when the installed file has the pinned SHA-256, else it is
# downloaded (HTTPS only) into a temporary directory, checked, then installed.
# linux/amd64 only (the pinned digests).
k8s-tools: export TOOLS_DIR := $(K8S_TOOLS_DIR)
k8s-tools: ## Install kind, kubectl and helm (pinned versions, SHA-256 checked) into K8S_TOOLS_DIR (~/.local/bin)
	@set -e; \
	if [ "$$(uname -s)-$$(uname -m)" != "Linux-x86_64" ]; then \
		echo "error: k8s-tools pins linux/amd64 binaries only (this is $$(uname -s)-$$(uname -m))" >&2; exit 1; \
	fi; \
	mkdir -p -- "$$TOOLS_DIR"; tmp="$$(mktemp -d)"; \
	trap 'rm -f -- "$$tmp/kind" "$$tmp/kubectl" "$$tmp/helm.tgz" "$$tmp/linux-amd64/helm"; \
		rmdir -- "$$tmp/linux-amd64" 2>/dev/null || true; rmdir -- "$$tmp"' EXIT; \
	fetch() { curl -fsSL --proto '=https' --proto-redir '=https' --retry 3 -o "$$1" "$$2"; }; \
	ok() { [ -f "$$2" ] && echo "$$1  $$2" | sha256sum -c --status -; }; \
	if ok $(KIND_SHA256) "$$TOOLS_DIR/kind"; then echo "kind $(KIND_VERSION): present, SHA-256 OK"; else \
		fetch "$$tmp/kind" "https://github.com/kubernetes-sigs/kind/releases/download/$(KIND_VERSION)/kind-linux-amd64"; \
		echo "$(KIND_SHA256)  $$tmp/kind" | sha256sum -c --quiet -; \
		install -m 0755 "$$tmp/kind" "$$TOOLS_DIR/kind"; echo "kind $(KIND_VERSION): installed, SHA-256 OK"; fi; \
	if ok $(KUBECTL_SHA256) "$$TOOLS_DIR/kubectl"; then echo "kubectl $(KUBECTL_VERSION): present, SHA-256 OK"; else \
		fetch "$$tmp/kubectl" "https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/linux/amd64/kubectl"; \
		echo "$(KUBECTL_SHA256)  $$tmp/kubectl" | sha256sum -c --quiet -; \
		install -m 0755 "$$tmp/kubectl" "$$TOOLS_DIR/kubectl"; echo "kubectl $(KUBECTL_VERSION): installed, SHA-256 OK"; fi; \
	if ok $(HELM_BIN_SHA256) "$$TOOLS_DIR/helm"; then echo "helm $(HELM_VERSION): present, SHA-256 OK"; else \
		fetch "$$tmp/helm.tgz" "https://get.helm.sh/helm-$(HELM_VERSION)-linux-amd64.tar.gz"; \
		echo "$(HELM_SHA256)  $$tmp/helm.tgz" | sha256sum -c --quiet -; \
		tar -xzf "$$tmp/helm.tgz" -C "$$tmp" linux-amd64/helm; \
		echo "$(HELM_BIN_SHA256)  $$tmp/linux-amd64/helm" | sha256sum -c --quiet -; \
		install -m 0755 "$$tmp/linux-amd64/helm" "$$TOOLS_DIR/helm"; echo "helm $(HELM_VERSION): installed, SHA-256 OK"; fi

# One node, API server on 127.0.0.1 only, no host port mapping: the UI is reached
# through `make k8s-ui` (kubectl port-forward on 127.0.0.1).
kind-up: ## Create the local kind cluster (1 node, Kubernetes 1.37.0, pinned node image)
	@if $(KIND) get clusters 2>/dev/null | grep -qx '$(KIND_CLUSTER)'; then \
		echo "kind cluster $(KIND_CLUSTER) already exists"; \
	else \
		$(KIND) create cluster --name '$(KIND_CLUSTER)' --image '$(KIND_NODE_IMAGE)' \
			--config deploy/k8s/kind-config.yaml --wait 120s; \
	fi

kind-down: ## Delete the kind cluster (and everything in it: Postgres, lake, logs)
	$(KIND) delete cluster --name '$(KIND_CLUSTER)'

image: ## Build the project image (Airflow + project + dbt + Spark), tag IMAGE_REPOSITORY:IMAGE_TAG
	docker build \
		--build-arg OPENLINEAGE_SPARK_VERSION=$(OPENLINEAGE_SPARK_VERSION) \
		--build-arg OPENLINEAGE_SPARK_SHA256=$(OPENLINEAGE_SPARK_SHA256) \
		-t '$(IMAGE)' .

kind-load: ## Load the project image into the kind cluster's node
	$(KIND) load docker-image '$(IMAGE)' --name '$(KIND_CLUSTER)'

k8s-chart: export CHART := $(AIRFLOW_CHART)
k8s-chart: ## Download the official Airflow Helm chart (pinned version, SHA-256 checked) into .k8s/
	@set -e; \
	if [ -f "$$CHART" ] && echo "$(AIRFLOW_CHART_SHA256)  $$CHART" | sha256sum -c --status -; then \
		echo "$$CHART: present, SHA-256 OK"; exit 0; fi; \
	mkdir -p -- "$$(dirname -- "$$CHART")"; \
	trap 'rm -f -- "$$CHART.part"' EXIT; \
	curl -fsSL --proto '=https' --proto-redir '=https' --retry 3 -o "$$CHART.part" '$(AIRFLOW_CHART_URL)'; \
	echo "$(AIRFLOW_CHART_SHA256)  $$CHART.part" | sha256sum -c --quiet -; \
	mv -- "$$CHART.part" "$$CHART"; \
	echo "$$CHART: downloaded, SHA-256 OK"

# Every kubectl / helm command names the kind context explicitly: never another cluster.
K8S_ARGS := --context '$(K8S_CONTEXT)' --namespace '$(K8S_NAMESPACE)'
HELM_ARGS := --kube-context '$(K8S_CONTEXT)' --namespace '$(K8S_NAMESPACE)'
IMAGE_SETS := --set image.repository='$(IMAGE_REPOSITORY)' --set image.tag='$(IMAGE_TAG)'
AIRFLOW_IMAGE_SETS := --set images.airflow.repository='$(IMAGE_REPOSITORY)' \
	--set images.airflow.tag='$(IMAGE_TAG)' \
	--set defaultAirflowRepository='$(IMAGE_REPOSITORY)' --set defaultAirflowTag='$(IMAGE_TAG)'

# 1. the namespace, labelled for Pod Security Admission `restricted` (SEC-1);
# 2. the Secrets (generated once, never printed: scripts/k8s_secrets.py);
# 3. the project chart (Postgres, volumes, Spark conf and RBAC, toolbox), then
#    wait for Postgres and the toolbox (not `helm --wait`: the logs PVC stays
#    Pending until the Airflow pods use it, local-path binds on first consumer);
# 4. the official Airflow chart (KubernetesExecutor, DAGs in the image). The
#    migration Job is a plain Job (values-airflow.yaml): its pod template is
#    immutable, so the finished Job of the previous install is deleted first and
#    recreated by the upgrade (`airflow db migrate` is idempotent);
# 5. the pools of deploy/airflow/pools.json (idempotent, as `make airflow-init`).
k8s-install: k8s-chart ## Install or upgrade the deployment on the kind cluster (helm upgrade --install)
	$(KUBECTL) --context '$(K8S_CONTEXT)' create namespace '$(K8S_NAMESPACE)' --dry-run=client -o yaml \
		| $(KUBECTL) --context '$(K8S_CONTEXT)' apply -f -
	$(KUBECTL) --context '$(K8S_CONTEXT)' label namespace '$(K8S_NAMESPACE)' --overwrite \
		pod-security.kubernetes.io/enforce=$(PSA_LEVEL) pod-security.kubernetes.io/enforce-version=$(PSA_VERSION) \
		pod-security.kubernetes.io/warn=$(PSA_LEVEL) pod-security.kubernetes.io/warn-version=$(PSA_VERSION) \
		pod-security.kubernetes.io/audit=$(PSA_LEVEL) pod-security.kubernetes.io/audit-version=$(PSA_VERSION)
	$(PYTHON) scripts/k8s_secrets.py --kubectl '$(KUBECTL)' --context '$(K8S_CONTEXT)' --namespace '$(K8S_NAMESPACE)'
	$(HELM) upgrade --install $(K8S_RELEASE_PROJECT) '$(PROJECT_CHART)' $(HELM_ARGS) \
		$(IMAGE_SETS) --set-file fileListSql=utils/SQL/script_tables.sql
	$(KUBECTL) $(K8S_ARGS) rollout status statefulset/project-postgres --timeout 5m
	$(KUBECTL) $(K8S_ARGS) rollout status deployment/project-toolbox --timeout 5m
	$(KUBECTL) $(K8S_ARGS) delete job $(K8S_RELEASE_AIRFLOW)-run-airflow-migrations --ignore-not-found
	$(HELM) upgrade --install $(K8S_RELEASE_AIRFLOW) '$(AIRFLOW_CHART)' $(HELM_ARGS) \
		-f deploy/k8s/values-airflow.yaml $(AIRFLOW_IMAGE_SETS) --wait --timeout 15m
	$(KUBECTL) $(K8S_ARGS) exec deploy/$(K8S_RELEASE_AIRFLOW)-scheduler -c scheduler -- \
		airflow pools import /opt/airflow/deploy/airflow/pools.json

# Keeps the namespace, the Secrets and the PVCs (Postgres, shared volume, logs):
# a new `make k8s-install` finds its data again. `make kind-down` removes all.
k8s-uninstall: ## Uninstall both Helm releases (keeps Secrets and volumes)
	-$(HELM) uninstall $(K8S_RELEASE_AIRFLOW) $(HELM_ARGS) --wait
	-$(HELM) uninstall $(K8S_RELEASE_PROJECT) $(HELM_ARGS) --wait

# Into data/ of the shared volume (the listing reads ./data/ = shared/data).
DATA ?= data/orders_sample.csv
k8s-load-data: export DATA_FILE := $(DATA)
k8s-load-data: ## Copy a source file into the shared volume: make k8s-load-data DATA=data/orders_sample.csv
	@test -f "$$DATA_FILE" || { echo "error: $$DATA_FILE: no such file" >&2; exit 1; }
	$(KUBECTL) $(K8S_ARGS) cp "$$DATA_FILE" \
		"$$($(KUBECTL) $(K8S_ARGS) get pod -l app.kubernetes.io/name=toolbox -o jsonpath='{.items[0].metadata.name}'):/opt/airflow/shared/data/$$(basename -- "$$DATA_FILE")" \
		-c toolbox
	$(KUBECTL) $(K8S_ARGS) exec deploy/project-toolbox -c toolbox -- ls -l /opt/airflow/shared/data

# A finished driver pod stays (Completed / Error) with its logs, as Spark
# documents; its Service, ConfigMaps and NetworkPolicy are owned by it and go
# with it (Kubernetes garbage collection).
k8s-spark-clean: ## Delete the finished Spark driver pods (and what they own)
	$(KUBECTL) $(K8S_ARGS) delete pod -l spark-role=driver \
		--field-selector=status.phase!=Running,status.phase!=Pending

K8S_UI_PORT ?= 8081
k8s-ui: ## Forward the Airflow UI to http://127.0.0.1:8081 (K8S_UI_PORT), foreground
	$(KUBECTL) $(K8S_ARGS) port-forward --address 127.0.0.1 svc/$(K8S_RELEASE_AIRFLOW)-api-server '$(K8S_UI_PORT)':8080

k8s-ui-password: ## Print the password of the UI user `admin` (Secret airflow-ui-admin, dev only)
	@$(KUBECTL) $(K8S_ARGS) get secret airflow-ui-admin -o jsonpath='{.data.password}' | base64 -d; echo
