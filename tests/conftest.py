"""Settings applied to the whole test suite, before any test module is
imported (pytest loads this file first).

P2-5: the suite never emits lineage, whatever the developer's shell exports
(a Marquez URL, the Makefile's file transport, a listener jar...).
`[openlineage] disabled` is forced, not defaulted: the provider checks it
first. The client's own variables (`OPENLINEAGE_*`, which also covers
`OPENLINEAGE__TRANSPORT__*`) and the other `AIRFLOW__OPENLINEAGE__*` ones are
dropped, for code that builds a client directly. A test that needs lineage
turns it on with `monkeypatch`, towards `tmp_path`.

P3-3: the suite never emits metrics either (OpenTelemetry, StatsD). Airflow's
`[metrics] otel_on` is forced to False, and the standard `OTEL_*` variables
(endpoint, protocol, headers, exporter...) and the other
`AIRFLOW__METRICS__*` / `AIRFLOW__TRACES__*` ones are dropped, so that a shell
set up for `make airflow` cannot send the tests' DAG runs to a collector.
"""

import os

# P5-1: the suite reads the repo config root, never the shell's.
os.environ.pop("PIPELINE_CONFIG_ROOT", None)
os.environ["AIRFLOW__METRICS__OTEL_ON"] = "False"
os.environ["AIRFLOW__TRACES__OTEL_ON"] = "False"
for _name in [
    k for k in os.environ if k.startswith(("OTEL_", "AIRFLOW__METRICS__", "AIRFLOW__TRACES__"))
]:
    if _name not in {"AIRFLOW__METRICS__OTEL_ON", "AIRFLOW__TRACES__OTEL_ON"}:
        del os.environ[_name]

os.environ["AIRFLOW__OPENLINEAGE__DISABLED"] = "True"
for _name in [k for k in os.environ if k.startswith(("OPENLINEAGE_", "AIRFLOW__OPENLINEAGE__"))]:
    if _name != "AIRFLOW__OPENLINEAGE__DISABLED":
        del os.environ[_name]
# The hook lineage collector limit the Makefile raises for Airflow: the
# suite tests the default.
os.environ.pop("AIRFLOW__LINEAGE__MAX_ASSETS_PER_COLLECTOR", None)
