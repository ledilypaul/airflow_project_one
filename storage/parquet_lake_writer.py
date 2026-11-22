"""Écriture des fichiers lus dans le lake, en Parquet partitionné (P2-2, constat A1).

    <LAKE_ROOT>/<zone>/<table>/lake_run_id=<run id>/lake_source_file=<nom du fichier>/part-*.parquet

- `LAKE_ROOT` : variable d'environnement du job ; par défaut
  `<racine du projet>/lake`, résolu depuis l'emplacement du code (jamais
  depuis le répertoire courant). Un chemin local absolu devient une URI
  `file://`. Une URI est utilisée telle quelle si elle a un hôte
  (`s3://bucket/...`, `hdfs://nn/...`) ou un chemin absolu (`file:///...`) ;
  un chemin relatif, URI comprise (`file:lake`), est refusé.
- Colonnes techniques ajoutées à chaque écriture, jamais préfixées par `_`
  ni `.` : pyarrow (et pandas, qui passe par lui) ignore par défaut tout
  chemin qui commence ainsi, répertoires de partition compris, et lirait
  0 ligne sans erreur (C-1).
    - `lake_run_id` : dag_run_id du run de listing (partition) ;
    - `lake_source_file` : nom du fichier source (partition) ;
    - `lake_file_id` : `file_list.id` de la ligne de ce fichier pour ce run,
      attribué par une séquence Postgres quand le listing l'a enregistré,
      jamais modifié ensuite : l'ordre des listings (voir `latest_snapshot`) ;
    - `lake_ingested_at` : horodatage de l'écriture, pour le lignage
      seulement : il change à chaque relance, la partition étant réécrite.
- Idempotence : `mode("overwrite")` avec `partitionOverwriteMode=dynamic`
  passé en option de CETTE écriture (prioritaire sur la conf de session,
  qui n'est pas modifiée) : seules les partitions présentes dans les données
  écrites sont remplacées. Relancer le même (run, fichier) remplace
  exactement sa partition ; les autres fichiers et runs ne sont pas touchés.
  Une réécriture à 0 ligne ne contient aucune partition, donc n'en remplace
  aucune : le writer supprime alors lui-même celle du (run, fichier) (C-2).
  `delete_partition` expose cette suppression (P2-3 : le job retire ainsi la
  partition staging d'un fichier dont un contrôle qualité bloquant échoue).
  `append` reste possible, mais seulement si l'appelant le demande.
- Contenu d'une table : un instantané par listing (C-4). Chaque run de
  listing réécrit chacun de ses fichiers sous son propre `lake_run_id` ;
  l'état courant d'une table est, pour chaque `lake_source_file`, la
  partition du listing le plus récent : `latest_snapshot`. Ce n'est pas
  « la dernière écriture gagne » : relancer un ancien run réécrit sa
  partition sans la rendre courante.
- Les noms de partition sont échappés par Spark (`:` -> `%3A`...) et relus à
  l'identique, par Spark comme par pyarrow. Une table peut mélanger des
  schémas d'une partition à l'autre (tables par défaut `default_csv`...) :
  la lire en entier demande `mergeSchema`, et un lecteur qui veut
  `lake_run_id` en texte quoi qu'il arrive (un run id purement numérique
  serait inféré en entier) fournit un schéma ou désactive
  `spark.sql.sources.partitionColumnTypeInference.enabled`.

Ce module ne doit pas importer Airflow : le job Spark le charge
(tests/unit/jobs/test_spark_job_no_airflow_import.py).
"""

import logging
import os
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

from pyspark.sql import DataFrame, Observation, SparkSession, Window
from pyspark.sql import functions as F

from utils.path_utils import lake_relative_parts, normalize_spark_path

from .base_writer import BaseWriter

log = logging.getLogger(__name__)

LAKE_ROOT_ENV = "LAKE_ROOT"
DEFAULT_LAKE_ROOT = Path(__file__).resolve().parent.parent / "lake"

RUN_ID_COLUMN = "lake_run_id"
SOURCE_FILE_COLUMN = "lake_source_file"
FILE_ID_COLUMN = "lake_file_id"
INGESTED_AT_COLUMN = "lake_ingested_at"
PARTITION_COLUMNS = (RUN_ID_COLUMN, SOURCE_FILE_COLUMN)
TECHNICAL_COLUMNS = (*PARTITION_COLUMNS, FILE_ID_COLUMN, INGESTED_AT_COLUMN)

WRITE_MODES = ("overwrite", "append")


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


def latest_snapshot(data: DataFrame) -> DataFrame:
    """État courant d'une table du lake : pour chaque `lake_source_file`, les
    seules lignes du listing le plus récent, c'est-à-dire du plus grand
    `lake_file_id` (C-4).

    Pourquoi `lake_file_id` (`file_list.id`) et pas `lake_ingested_at` ni
    `lake_run_id` :
    - `lake_ingested_at` est l'heure de l'écriture : un Clear ou un retry d'un
      ancien run la renouvelle, et son instantané deviendrait courant (« la
      dernière écriture gagne »). Elle dépend en plus de l'horloge du driver ;
    - `lake_run_id` n'est pas ordonné : `asset_triggered__...`, `manual__...`,
      `scheduled__...` ou un run id libre ne se comparent pas entre eux ;
    - `file_list.id` vient d'une séquence Postgres, attribuée quand le listing
      enregistre le fichier, jamais modifiée par une relance du job ni du
      DAG : il ne dépend d'aucune horloge et ne peut pas être égal pour deux
      runs (une ligne par (fichier, run)).

    Fonction Spark pure (une fenêtre par `lake_source_file`, pas d'action) :
    elle s'applique à une table lue en entier comme à une sélection.

    Limite : un instantané à 0 ligne n'a pas de partition (le writer la
    supprime). Si le listing le plus récent d'un fichier l'a trouvé vide,
    c'est l'instantané non vide précédent qui est renvoyé ; `file_list`
    reste la référence pour savoir quels fichiers un listing a vus.

    Raises:
        ValueError: une colonne technique nécessaire manque.
    """
    missing = [c for c in (SOURCE_FILE_COLUMN, FILE_ID_COLUMN) if c not in data.columns]
    if missing:
        raise ValueError(
            f"latest_snapshot : colonne(s) technique(s) absente(s) : {', '.join(missing)}"
        )
    latest = "__lake_latest_file_id"
    return (
        data.withColumn(latest, F.max(FILE_ID_COLUMN).over(Window.partitionBy(SOURCE_FILE_COLUMN)))
        .filter(F.col(FILE_ID_COLUMN) == F.col(latest))
        .drop(latest)
    )


class ParquetLakeWriter(BaseWriter):
    """Écrit un DataFrame Spark dans une table Parquet du lake, partitionnée
    par (`lake_run_id`, `lake_source_file`) : un instantané par listing et
    par fichier (voir le docstring du module et `latest_snapshot`)."""

    def __init__(self, lake_root: str):
        self.lake_root = lake_root

    def location(self, target: str) -> str:
        """URI de la table `target` (relative au lake, ex. `raw/orders`).

        Raises:
            ValueError: `target` sortirait du lake (`..`, chemin absolu, URI).
        """
        parts = lake_relative_parts(target)
        root = self.lake_root if self.lake_root.endswith("/") else f"{self.lake_root}/"
        return root + "/".join(parts)

    def write(
        self,
        data: DataFrame,
        target: str,
        mode: str = "overwrite",
        *,
        run_id: str,
        source_file: str,
        file_id: int,
    ) -> int:
        """Écrit `data` dans la partition (`run_id`, `source_file`) de `target`.

        Args:
            data: les données, sans colonne technique (elles sont ajoutées ici).
            target: la table, relative au lake (ex. `raw/orders`).
            mode: `overwrite` (défaut, idempotent : remplace la partition du
                (run, fichier), même par 0 ligne) ou `append` (choix
                explicite : un retry duplique les lignes).
            run_id: dag_run_id du run de listing.
            source_file: nom du fichier source.
            file_id: `file_list.id` du fichier pour ce run (entier >= 0).

        Returns:
            Le nombre de lignes écrites, mesuré pendant l'écriture même
            (`Observation`), sans relire les données.
        """
        if mode not in WRITE_MODES:
            raise ValueError(f"mode {mode!r} non supporté : {' ou '.join(WRITE_MODES)}")
        _check_partition_values(run_id, source_file)
        if not isinstance(file_id, int) or isinstance(file_id, bool) or file_id < 0:
            raise ValueError(f"file_id doit être un entier >= 0 (file_list.id), pas {file_id!r}")
        # Spark compare les noms de colonnes sans tenir compte de la casse.
        reserved = {t.lower() for t in TECHNICAL_COLUMNS}
        clashes = sorted(c for c in data.columns if c.lower() in reserved)
        if clashes:
            raise ValueError(
                f"colonne technique déjà présente dans les données : {', '.join(clashes)} "
                f"(réservées : {', '.join(TECHNICAL_COLUMNS)})"
            )

        location = self.location(target)
        observation = Observation()
        output = (
            data.withColumn(RUN_ID_COLUMN, F.lit(run_id))
            .withColumn(SOURCE_FILE_COLUMN, F.lit(source_file))
            .withColumn(FILE_ID_COLUMN, F.lit(file_id).cast("bigint"))
            .withColumn(INGESTED_AT_COLUMN, F.current_timestamp())
            .observe(observation, F.count(F.lit(1)).alias("rows"))
        )
        writer = output.write.mode(mode).partitionBy(*PARTITION_COLUMNS)
        if mode == "overwrite":
            writer = writer.option("partitionOverwriteMode", "dynamic")
        writer.parquet(location)

        rows = observation.get["rows"]
        if rows > 0:
            log.info(
                "%d rows written to %s (%s=%r)", rows, location, SOURCE_FILE_COLUMN, source_file
            )
        elif mode == "overwrite":
            # C-2 : aucune ligne, donc aucune partition remplacée ; celle d'une
            # tentative précédente du même (run, fichier) resterait en place.
            removed = _delete_partition(data.sparkSession, location, run_id, source_file)
            log.warning(
                "0 row written to %s for %s=%r, %s=%r: previous partition %s",
                location,
                RUN_ID_COLUMN,
                run_id,
                SOURCE_FILE_COLUMN,
                source_file,
                "removed" if removed else "absent, nothing removed",
            )
        else:
            log.warning(
                "0 row appended to %s for %s=%r, %s=%r: partition left unchanged",
                location,
                RUN_ID_COLUMN,
                run_id,
                SOURCE_FILE_COLUMN,
                source_file,
            )
        return rows

    def delete_partition(
        self, spark: SparkSession, target: str, *, run_id: str, source_file: str
    ) -> bool:
        """Supprime la partition (`run_id`, `source_file`) de la table `target`
        (relative au lake), si elle existe ; les autres ne sont pas touchées.

        Utilisée par le job quand un contrôle qualité bloquant échoue (P2-3) :
        la partition staging qu'une tentative précédente a pu écrire ne doit
        pas rester comme un état valide.

        Returns:
            True si la partition existait.

        Raises:
            ValueError: `target` sortirait du lake, ou `run_id`/`source_file`
                vide (le chemin désignerait le run entier ou la table).
        """
        _check_partition_values(run_id, source_file)
        return _delete_partition(spark, self.location(target), run_id, source_file)


def _check_partition_values(run_id: str, source_file: str) -> None:
    for name, value in (("run_id", run_id), ("source_file", source_file)):
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} vide ou absent ({value!r}) : la partition serait ambiguë")


def _delete_partition(spark: SparkSession, location: str, run_id: str, source_file: str) -> bool:
    """Supprime la partition (`run_id`, `source_file`) de la table `location`
    par l'API Hadoop FileSystem de la session (même FS que l'écriture :
    local, HDFS, S3A...). Le chemin est construit avec l'échappement de
    Spark lui-même (`ExternalCatalogUtils.escapePathName`, celui de
    l'écriture partitionnée) : `:`, `#`, `%`... donnent exactement le
    répertoire écrit. Le répertoire `lake_run_id=` du run est supprimé aussi
    s'il devient vide (pas de répertoire vide laissé dans la table ; les
    fichiers d'un run sont écrits un par un par le même job, aucun autre ne
    peut s'y ajouter entre-temps). Renvoie True si la partition existait.
    """
    jvm = spark._jvm
    escape = jvm.org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
    hadoop_path = jvm.org.apache.hadoop.fs.Path
    run_dir = hadoop_path(f"{location.rstrip('/')}/{escape(RUN_ID_COLUMN)}={escape(run_id)}")
    partition = hadoop_path(run_dir, f"{escape(SOURCE_FILE_COLUMN)}={escape(source_file)}")
    fs = partition.getFileSystem(spark._jsc.hadoopConfiguration())
    removed = bool(fs.delete(partition, True))
    if removed and len(fs.listStatus(run_dir)) == 0:
        fs.delete(run_dir, False)
    return removed
