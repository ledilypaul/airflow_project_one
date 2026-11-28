# Project image for Kubernetes (P3-2): Airflow 3.4.0 + the project + dbt + Spark.
#
# One image for every pod of the deployment:
# - the Airflow pods of the official Helm chart (api-server, scheduler,
#   dag-processor, triggerer, and the KubernetesExecutor worker pods);
# - the Spark driver and executor pods that `spark-submit --master k8s://...
#   --deploy-mode cluster` creates (Spark's Kubernetes entrypoint, a JRE, and the
#   Spark distribution shipped by the PySpark wheel of uv.lock).
#
# Build with `make image` (it passes the OpenLineage jar pin of the Makefile).
# See docs/architecture/2026-11-19-p3-2-kubernetes.md and CLAUDE.md, "Kubernetes".
#
# Base images are pinned by tag AND digest (index digests, read with
# `docker buildx imagetools inspect` on 2026-11-19, and on 2026-11-21 for Airflow):
# - apache/airflow:slim-3.4.0-python3.12: the official image, slim variant (Airflow
#   core only, no extra providers): the providers come from uv.lock instead. Its
#   Airflow MUST be the one of uv.lock (`uv sync --frozen` below replaces the
#   image's packages with the lock's; a test keeps both equal). Since 3.4 it is a
#   Docker Hardened Image on Debian 13: `make image` builds on it unchanged
#   (checked on 2026-11-21, P3-5) and revalidated on kind in P3-6.
# - eclipse-temurin:17.0.21_9-jre-noble: the JRE of the official Spark 4.3.0 images
#   (apache/spark-docker builds on eclipse-temurin:17); only /opt/java/openjdk
#   is copied.
ARG AIRFLOW_IMAGE=apache/airflow:slim-3.4.0-python3.12@sha256:2d3e42b9de0eca948b9a869817068fedcb8a8eac9827b1e6215de3329d59335f
ARG JRE_IMAGE=eclipse-temurin:17.0.21_9-jre-noble@sha256:853d06b546da4eadfa7226e7ab5692b3745857cad782255830e230036b5f3889

# --- OpenLineage Spark listener (P2-5): pinned version, checked by SHA-256 ----------
# No default: `make image` passes the pin of the Makefile (OPENLINEAGE_SPARK_*), the
# single source of truth. HTTPS only, the build fails on a digest mismatch.
FROM ${AIRFLOW_IMAGE} AS openlineage-jar
ARG OPENLINEAGE_SPARK_VERSION
ARG OPENLINEAGE_SPARK_SHA256
RUN set -eu; \
    test -n "${OPENLINEAGE_SPARK_VERSION}" && test -n "${OPENLINEAGE_SPARK_SHA256}" || \
      { echo "OPENLINEAGE_SPARK_VERSION and OPENLINEAGE_SPARK_SHA256 are required (make image)" >&2; exit 1; }; \
    jar="openlineage-spark_2.13-${OPENLINEAGE_SPARK_VERSION}.jar"; \
    mkdir -p /tmp/openlineage; \
    curl -fsSL --proto '=https' --proto-redir '=https' --retry 3 -o "/tmp/openlineage/${jar}" \
      "https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.13/${OPENLINEAGE_SPARK_VERSION}/${jar}"; \
    echo "${OPENLINEAGE_SPARK_SHA256}  /tmp/openlineage/${jar}" | sha256sum -c -

# --- dbt (transform/, P2-4): its own uv project, venv and manifest ------------------
# Built at its final path (a venv's scripts hold absolute paths). The manifest is
# parsed with the LAKE_ROOT / DBT_DUCKDB_PATH of the deployment (the defaults
# below are the ones of deploy/k8s/values-airflow.yaml, a test keeps them equal).
FROM ${AIRFLOW_IMAGE} AS transform
ARG LAKE_ROOT=/opt/airflow/shared/lake
ARG DBT_DUCKDB_PATH=/opt/airflow/shared/warehouse.duckdb
ENV UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    DBT_SEND_ANONYMOUS_USAGE_STATS=False
WORKDIR /opt/airflow/transform
COPY --chown=airflow:0 transform/pyproject.toml transform/uv.lock ./
# No --no-build (Sonar docker:S8541, reviewed): dbt-core-experimental-parser is
# published as an sdist only, so uv must build it. Its hash is pinned in
# transform/uv.lock and --frozen refuses any other file.
RUN --mount=type=cache,target=/tmp/.cache/uv,uid=50000,gid=0 \
    uv sync --frozen --python /usr/python/bin/python3.12
COPY --chown=airflow:0 transform/ ./
RUN LAKE_ROOT="${LAKE_ROOT}" DBT_DUCKDB_PATH="${DBT_DUCKDB_PATH}" \
      .venv/bin/dbt parse --project-dir . --profiles-dir . \
    && test -s target/manifest.json \
    && rm -rf logs

# --- Final image ------------------------------------------------------------------
FROM ${JRE_IMAGE} AS jre

FROM ${AIRFLOW_IMAGE}

USER root
# tini: Spark's Kubernetes entrypoint runs the driver and the executors under
# /usr/bin/tini (Debian package, from the signed Debian archive of the base image).
RUN set -eu; \
    apt-get update; \
    apt-get install -y --no-install-recommends tini; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
    install -m 0755 /home/airflow/.local/bin/uv /usr/local/bin/uv; \
    mkdir -p /opt/spark-k8s
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
# Spark's Kubernetes entrypoint, vendored unchanged (byte for byte, so that its
# digest can be compared) from apache/spark-docker, commit
# f933b4486c0b7f91545c2bbc63cd6b960787190b ("[SPARK-60864] Publish Apache Spark
# 4.3.0 to docker registry"), file 4.3.0/scala2.13-java17-ubuntu/entrypoint.sh,
# SHA-256 ec72936bfb2abed8cfe36e47ae79d0b04fbcddfacb0e96fd1a0cd7f2d592ecf3.
COPY --chmod=0755 deploy/k8s/image/spark-entrypoint.sh /opt/spark-k8s/entrypoint.sh
COPY --chmod=0755 deploy/k8s/image/entrypoint.sh /opt/project-entrypoint.sh

USER airflow
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}" \
    UV_PROJECT_ENVIRONMENT=/home/airflow/.local \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1
WORKDIR /opt/airflow

# The image's virtualenv (/home/airflow/.local) is synced to uv.lock exactly:
# runtime + `airflow` group, no `dev` group; packages of the base image that are
# not in the lock are removed. The project itself is not installed (its build
# backend is not in the lock): PYTHONPATH below plays the role of the editable
# install, and keeps config/ and transform/ next to the code that resolves them.
COPY --chown=airflow:0 pyproject.toml uv.lock README.md ./
# No --no-build (Sonar docker:S8541, reviewed): pyspark and pyspark-client are
# published as sdists only, so uv must build them. Their hashes are pinned in
# uv.lock and --frozen refuses any other file.
RUN --mount=type=cache,target=/tmp/.cache/uv,uid=50000,gid=0 \
    uv sync --frozen --no-dev --no-install-project

COPY --chown=airflow:0 --from=transform /opt/airflow/transform /opt/airflow/transform
COPY --chown=airflow:0 --from=openlineage-jar /tmp/openlineage/ /opt/airflow/lineage/jars/
COPY --chown=airflow:0 dags/ /opt/airflow/dags/
COPY --chown=airflow:0 pipeline/ /opt/airflow/pipeline/
COPY --chown=airflow:0 utils/ /opt/airflow/utils/
COPY --chown=airflow:0 storage/ /opt/airflow/storage/
COPY --chown=airflow:0 config/ /opt/airflow/config/
# P5-1: the engine ships no source; this dev image carries the example ones
# (the kind run). A real deployment mounts or bakes its own config/sources.
COPY --chown=airflow:0 examples/config/ /opt/airflow/examples/config/
ENV PIPELINE_CONFIG_ROOT=/opt/airflow/examples/config
COPY --chown=airflow:0 deploy/airflow/pools.json /opt/airflow/deploy/airflow/pools.json

# Source files are read from the shared volume (mounted at /opt/airflow/shared):
# config/pipeline.yaml lists `./data/` (default_directories), relative to the working directory.
RUN ln -s shared/data /opt/airflow/data

# SPARK_HOME: the Spark distribution of the PySpark wheel (bin/spark-submit,
# jars/ with spark-kubernetes), used by spark-submit in the worker pods and by
# Spark's entrypoint in the driver and executor pods.
ENV PYTHONPATH=/opt/airflow \
    SPARK_HOME=/home/airflow/.local/lib/python3.12/site-packages/pyspark

USER 50000
ENTRYPOINT ["/opt/project-entrypoint.sh"]
