"""Nom OpenLineage d'une table du lake, tel que le listener Spark le donne
(P2-5), partagé par `transform_dag` et la tâche Polars (P5-8)."""

from urllib.parse import urlsplit

_OBJECT_STORES = (("s3", "s3"), ("gs", "gs"), ("abfs", "abfss"), ("wasb", "wasbs"))


def spark_dataset(location: str) -> tuple[str, str]:
    """OpenLineage (namespace, name) the Spark listener gives the directory
    `location`, as FilesystemDatasetUtils.fromLocation (openlineage-spark
    1.55.0) does, checked against it through py4j:

    - path or `file:` URI: `file` + the path (no resolution here: the
      caller resolves the lake root only, like the job);
    - object store (`s3*`, `gs`, `abfs*`, `wasb*`): `<canonical
      scheme>://<authority>` + the path without its leading slashes;
    - any other scheme (`hdfs`, `dbfs`, `gcs`...): `<scheme>://<authority>`
      (or `<scheme>` without authority) + the path, leading slash kept.
    Trailing slashes are removed, an empty path is `/`.
    """
    parts = urlsplit(location)
    scheme = parts.scheme.lower()
    if not scheme or len(scheme) == 1 or scheme == "file":  # path, Windows drive, file:
        path = parts.path if scheme == "file" else location
        return "file", path.rstrip("/") or "/"
    for prefix, canonical in _OBJECT_STORES:
        if scheme.startswith(prefix):
            name = parts.path.strip("/") or "/"
            return f"{canonical}://{parts.netloc}", name
    namespace = f"{scheme}://{parts.netloc}" if parts.netloc else scheme
    return namespace, parts.path.rstrip("/") or "/"
