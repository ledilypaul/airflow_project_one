"""Disposition du lake, sans moteur (P5-7) : colonnes techniques, chemin d'une
table et d'une partition, contrôles avant écriture. Partagé par le writer Spark
(`parquet_lake_writer.py`) et le writer Polars (`pipeline/engine/polars/writer.py`).
"""

from utils.path_utils import lake_relative_parts

RUN_ID_COLUMN = "lake_run_id"
SOURCE_FILE_COLUMN = "lake_source_file"
FILE_ID_COLUMN = "lake_file_id"
INGESTED_AT_COLUMN = "lake_ingested_at"
PARTITION_COLUMNS = (RUN_ID_COLUMN, SOURCE_FILE_COLUMN)
TECHNICAL_COLUMNS = (*PARTITION_COLUMNS, FILE_ID_COLUMN, INGESTED_AT_COLUMN)

# Caractères que Spark échappe dans un nom de partition
# (`ExternalCatalogUtils.escapePathName`, hors Windows), en `%XX` majuscule ;
# le reste, non-ASCII compris, est gardé tel quel. Comparé à Spark par
# tests/unit/engine/test_p5_7_polars_writer.py.
_ESCAPED = frozenset(map(chr, range(0x01, 0x20))) | frozenset("\"#%'*/:=?[\\]^{\x7f")


def table_location(lake_root: str, target: str) -> str:
    """URI de la table `target` (relative au lake, ex. `raw/<table>`).

    Raises:
        ValueError: `target` sortirait du lake (`..`, chemin absolu, URI).
    """
    parts = lake_relative_parts(target)
    root = lake_root if lake_root.endswith("/") else f"{lake_root}/"
    return root + "/".join(parts)


def escape_path_name(value: str) -> str:
    return "".join(f"%{ord(c):02X}" if c in _ESCAPED else c for c in value)


def partition_path(location: str, run_id: str, source_file: str) -> tuple[str, str]:
    """(répertoire du run, répertoire de la partition) dans la table `location`."""
    run_dir = f"{location.rstrip('/')}/{RUN_ID_COLUMN}={escape_path_name(run_id)}"
    return run_dir, f"{run_dir}/{SOURCE_FILE_COLUMN}={escape_path_name(source_file)}"


def check_partition_values(run_id: str, source_file: str) -> None:
    for name, value in (("run_id", run_id), ("source_file", source_file)):
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} vide ou absent ({value!r}) : la partition serait ambiguë")


def check_write(columns: list[str], run_id: str, source_file: str, file_id: int) -> None:
    """Refuse une écriture ambiguë : partition vide, `file_id` invalide, ou
    colonne technique déjà dans les données (casse ignorée, comme Spark)."""
    check_partition_values(run_id, source_file)
    if not isinstance(file_id, int) or isinstance(file_id, bool) or file_id < 0:
        raise ValueError(f"file_id doit être un entier >= 0 (file_list.id), pas {file_id!r}")
    reserved = {t.lower() for t in TECHNICAL_COLUMNS}
    clashes = sorted(c for c in columns if c.lower() in reserved)
    if clashes:
        raise ValueError(
            f"colonne technique déjà présente dans les données : {', '.join(clashes)} "
            f"(réservées : {', '.join(TECHNICAL_COLUMNS)})"
        )
