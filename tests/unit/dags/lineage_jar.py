"""Test helper (P2-5): stand-ins for the openlineage-spark jar.

A real jar is a zip with the listener class; these never reach a JVM (the
tests do not run spark-submit), they only need to pass or fail the checks of
`openlineage_spark_settings()` and `prepare_spark_lineage()`.
"""

import hashlib
import zipfile
from pathlib import Path

LISTENER_ENTRY = "io/openlineage/spark/agent/OpenLineageSparkListener.class"


def make_listener_jar(path: Path, *, with_listener: bool = True) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as jar:
        jar.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
        jar.writestr(LISTENER_ENTRY if with_listener else "io/other/Thing.class", b"\xca\xfe")
    return path


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
