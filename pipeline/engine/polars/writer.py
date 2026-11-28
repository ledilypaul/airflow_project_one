"""Écriture du lake par le moteur Polars (P5-7), fichier pour fichier comme
Spark (`storage/parquet_lake_writer.py`) :

- même disposition : `<table>/lake_run_id=<run>/lake_source_file=<fichier>/
  part-00000-<uuid>.c000.snappy.parquet`, valeurs échappées comme Spark ;
- mêmes types physiques : horodatages en INT96 (pyarrow,
  `use_deprecated_int96_timestamps`), colonnes métier facultatives,
  `lake_file_id` et `lake_ingested_at` obligatoires. DuckDB et le contrat dbt
  lisent donc une table écrite par les deux moteurs comme une seule ;
- overwrite de la partition : le fichier est écrit dans un répertoire caché de
  la table, puis remplace la partition ; 0 ligne supprime la partition, et le
  répertoire du run s'il devient vide.

`append` n'est pas proposé (la config le refuse). Lac local seulement.
"""

import datetime
import logging
import os
import shutil
import uuid
from collections.abc import Sequence
from urllib.parse import urlsplit

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from storage.lake_layout import (
    FILE_ID_COLUMN,
    INGESTED_AT_COLUMN,
    check_partition_values,
    check_write,
    partition_path,
    table_location,
)

log = logging.getLogger(__name__)


class PolarsLakeWriter:
    def __init__(self, lake_root: str):
        parts = urlsplit(lake_root)
        if parts.scheme not in ("", "file") or parts.netloc:
            # shortcut: lac local seulement ; un lac objet (s3, gs) passera par
            # pyarrow.fs quand le déploiement en aura un (backlog P3-2).
            raise NotImplementedError(f"lac {lake_root!r} : le moteur polars écrit en local")
        self.lake_root = parts.path

    def location(self, target: str) -> str:
        return table_location(self.lake_root, target)

    def write(
        self,
        frame: pl.LazyFrame | pl.DataFrame,
        target: str,
        mode: str = "overwrite",
        *,
        run_id: str,
        source_file: str,
        file_id: int,
        required: Sequence[str] = (),
    ) -> int:
        """Remplace la partition (`run_id`, `source_file`) de `target` ; lignes écrites.

        `required` : colonnes obligatoires dans le Parquet, en plus des colonnes
        techniques (les résultats qualité, comme Spark les écrit)."""
        if mode != "overwrite":
            raise ValueError(f"mode {mode!r} non supporté par le moteur polars : overwrite")
        data = frame.collect() if isinstance(frame, pl.LazyFrame) else frame
        check_write(data.columns, run_id, source_file, file_id)
        location = self.location(target)
        if data.height == 0:
            removed = _delete_partition(location, run_id, source_file)
            log.warning(
                "0 row written to %s for %r: previous partition %s",
                location,
                source_file,
                "removed" if removed else "absent, nothing removed",
            )
            return 0

        now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
        table = _arrow(
            data.with_columns(
                pl.lit(file_id, pl.Int64).alias(FILE_ID_COLUMN),
                pl.lit(now, pl.Datetime("us")).alias(INGESTED_AT_COLUMN),
            ),
            required={FILE_ID_COLUMN, INGESTED_AT_COLUMN, *required},
        )
        run_dir, partition = partition_path(location, run_id, source_file)
        staging = f"{location}/.polars-{uuid.uuid4()}"
        os.makedirs(staging)
        try:
            pq.write_table(
                table,
                f"{staging}/part-00000-{uuid.uuid4()}.c000.snappy.parquet",
                compression="snappy",
                use_deprecated_int96_timestamps=True,
                store_schema=False,
            )
            shutil.rmtree(partition, ignore_errors=True)
            os.makedirs(run_dir, exist_ok=True)
            os.replace(staging, partition)
        finally:
            shutil.rmtree(staging, ignore_errors=True)
        log.info("%d rows written to %s (lake_source_file=%r)", data.height, location, source_file)
        return data.height

    def delete_partition(self, target: str, *, run_id: str, source_file: str) -> bool:
        check_partition_values(run_id, source_file)
        return _delete_partition(self.location(target), run_id, source_file)


def _arrow(data: pl.DataFrame, required: set[str]) -> pa.Table:
    """La table Arrow que Spark écrirait : `string` et `list` (pas leurs
    variantes `large_`), colonnes facultatives sauf `required`."""
    table = data.to_arrow(compat_level=pl.CompatLevel.oldest())
    fields = [
        pa.field(
            f.name, _small(f.type, element_required=f.name in required), f.name not in required
        )
        for f in table.schema
    ]
    return table.cast(pa.schema(fields))


def _small(arrow: pa.DataType, element_required: bool = False) -> pa.DataType:
    if pa.types.is_large_string(arrow):
        return pa.string()
    if pa.types.is_large_list(arrow) or pa.types.is_list(arrow):
        element = pa.field("element", _small(arrow.value_type), not element_required)
        return pa.list_(element)
    return arrow


def _delete_partition(location: str, run_id: str, source_file: str) -> bool:
    run_dir, partition = partition_path(location, run_id, source_file)
    if not os.path.isdir(partition):
        return False
    shutil.rmtree(partition)
    if not os.listdir(run_dir):
        os.rmdir(run_dir)
    return True
