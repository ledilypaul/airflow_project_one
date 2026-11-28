"""Racine du lake (`LAKE_ROOT`), sans moteur : partagée par les moteurs (P5-6)."""

import os
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

from utils.path_utils import normalize_spark_path

LAKE_ROOT_ENV = "LAKE_ROOT"
DEFAULT_LAKE_ROOT = Path(__file__).resolve().parent.parent / "lake"


class LakeRootError(ValueError):
    """`LAKE_ROOT` invalide (chemin relatif, URI comprise)."""


def _is_uri(value: str) -> bool:
    # Schéma d'au moins deux caractères : `C:\lake` (lecteur Windows) n'en est pas un.
    scheme, sep, _ = value.partition(":")
    return bool(sep) and len(scheme) > 1 and scheme.replace("+", "").replace("-", "").isalnum()


def _relative_root_error(value: str) -> LakeRootError:
    return LakeRootError(
        f"{LAKE_ROOT_ENV}={value!r} est un chemin relatif : il dépendrait du répertoire "
        "courant du worker. Donner un chemin absolu ou une URI avec un hôte ou un chemin "
        "absolu (file:///..., s3://bucket/..., hdfs://namenode/...)."
    )


def resolve_lake_root(environ: Mapping[str, str] | None = None) -> str:
    """Racine du lake, sous forme d'URI utilisable par Spark.

    Args:
        environ: l'environnement à lire (`os.environ` par défaut).

    Raises:
        LakeRootError: `LAKE_ROOT` est un chemin local relatif, ou une URI
            sans hôte ni chemin absolu (`file:lake`, `hdfs:lake`, C-7).
            Sous spark-submit, le répertoire courant est celui du worker : un
            chemin relatif déplacerait le lake d'un run (ou d'un retry) à
            l'autre.
    """
    env = os.environ if environ is None else environ
    value = (env.get(LAKE_ROOT_ENV) or "").strip()
    if not value:
        return normalize_spark_path(str(DEFAULT_LAKE_ROOT))
    if _is_uri(value):
        parts = urlsplit(value)
        if not parts.netloc and not parts.path.startswith("/"):
            raise _relative_root_error(value)
        return value
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise _relative_root_error(value)
    return normalize_spark_path(str(path))
