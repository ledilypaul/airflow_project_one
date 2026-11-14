"""Settings applied to the whole test suite, before any test module is
imported (pytest loads this file first).

P2-5: the suite never emits lineage, whatever the developer's shell exports
(a Marquez URL, the Makefile's file transport, a listener jar...).
`[openlineage] disabled` is forced, not defaulted: the provider checks it
first. The client's own variables (`OPENLINEAGE_*`, which also covers
`OPENLINEAGE__TRANSPORT__*`) and the other `AIRFLOW__OPENLINEAGE__*` ones are
dropped, for code that builds a client directly. A test that needs lineage
turns it on with `monkeypatch`, towards `tmp_path`.
"""

import os

os.environ["AIRFLOW__OPENLINEAGE__DISABLED"] = "True"
for _name in [k for k in os.environ if k.startswith(("OPENLINEAGE_", "AIRFLOW__OPENLINEAGE__"))]:
    if _name != "AIRFLOW__OPENLINEAGE__DISABLED":
        del os.environ[_name]
# The hook lineage collector limit the Makefile raises for Airflow: the
# suite tests the default.
os.environ.pop("AIRFLOW__LINEAGE__MAX_ASSETS_PER_COLLECTOR", None)
