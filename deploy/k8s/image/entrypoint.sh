#!/bin/bash
# Entrypoint of the project image (P3-2): one image for the Airflow pods and for
# the Spark driver and executor pods.
#
# Spark on Kubernetes starts its pods with the image's entrypoint and the
# arguments `driver ...` or `executor ...` (it sets no `command`). Those go to
# Spark's own entrypoint (spark-entrypoint.sh, vendored unchanged from
# apache/spark-docker 4.3.0). Anything else goes to the entrypoint of the
# official Airflow image, unchanged (dumb-init, then /entrypoint: `bash -c ...`,
# `airflow ...`, `scheduler`...), which is what the Helm chart expects.
set -euo pipefail

case "${1:-}" in
  driver | executor)
    exec /opt/spark-k8s/entrypoint.sh "$@"
    ;;
  *)
    exec /usr/bin/dumb-init -- /entrypoint "$@"
    ;;
esac
