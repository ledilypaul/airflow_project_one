import re
from pathlib import Path


def normalize_spark_path(path_str: str) -> str:
    """
    Normalise un chemin pour Spark :
    - Si c'est un chemin local, ajoute le préfixe file://
    - Si c'est déjà un chemin distribué (hdfs://, s3://, etc.), ne change rien
    - Compatible Mac, Linux et Windows
    """
    # Protocoles déjà distribués
    distributed_protocols = ("hdfs://", "s3://", "gs://", "dbfs:/", "abfss://", "adl://")

    # Si le chemin commence déjà par un protocole distribué → on le garde tel quel
    if path_str.startswith(distributed_protocols):
        return path_str

    # Conversion en Path pour uniformiser les séparateurs
    local_path = Path(path_str).expanduser().resolve()

    # Spark sur Windows nécessite un triple slash après file:
    if local_path.drive:  # Windows (ex: C:\)
        return f"file:///{local_path.as_posix()}"
    else:  # Linux / Mac
        return f"file://{local_path.as_posix()}"


# P2-2: one segment of a path inside the lake (zone directory or table name).
# Letters, digits, `_`, `.`, `-`, never first `_` or `.`: Spark, Hadoop and
# pyarrow (`ignore_prefixes`) skip such "hidden" children when listing a
# directory, and `=` (refused)
# would make a directory look like a partition. `/`, `\` and `:` are refused
# with the rest: no subdirectory in a name, no Windows separator or drive, no
# URI scheme.
_LAKE_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
_URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


def _check_lake_segment(segment: str, value: str) -> None:
    if segment in {".", ".."}:
        raise ValueError(f"{value!r} : {segment!r} interdit, le chemin doit rester sous LAKE_ROOT")
    if not _LAKE_SEGMENT.fullmatch(segment):
        raise ValueError(
            f"{value!r} : {segment!r} invalide (lettres, chiffres, '_', '.', '-' ; "
            "ne commence ni par '_' ni par '.')"
        )


def lake_relative_parts(value: str) -> tuple[str, ...]:
    """Split a zone path relative to LAKE_ROOT (`raw/`, `zones/staging`) into
    its segments, refusing anything that could leave LAKE_ROOT.

    Raises:
        ValueError: empty path, absolute path, URI scheme, `..`, `.`, an
            empty segment (`raw//x`) or a segment with another character.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError("chemin vide : attendu un chemin relatif à LAKE_ROOT, ex. 'raw/'")
    if _URI_SCHEME.match(value):
        raise ValueError(
            f"{value!r} : schéma d'URI interdit, le chemin est relatif à LAKE_ROOT "
            "(la racine se règle par la variable d'environnement LAKE_ROOT)"
        )
    if value.startswith(("/", "\\")):
        raise ValueError(
            f"{value!r} : chemin absolu interdit, attendu un chemin relatif à LAKE_ROOT"
        )
    segments = value[:-1].split("/") if value.endswith("/") else value.split("/")
    for segment in segments:
        if segment == "":
            raise ValueError(f"{value!r} : segment vide ('//')")
        _check_lake_segment(segment, value)
    return tuple(segments)


def check_lake_name(value: str) -> str:
    """Check a table name: a single, safe segment under a zone path."""
    if not isinstance(value, str) or not value:
        raise ValueError("nom de table vide")
    if "/" in value or "\\" in value:
        raise ValueError(f"{value!r} : un nom de table ne contient pas de '/' ni de '\\'")
    if _URI_SCHEME.match(value):
        raise ValueError(f"{value!r} : schéma d'URI interdit dans un nom de table")
    _check_lake_segment(value, value)
    return value
