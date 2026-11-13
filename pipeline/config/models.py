"""Modèle unique (Pydantic) des configs de lecture : `config/sources/*.yaml`
et `config/defaults/*.yaml` (P2-1, constat M4).

    source:   name, format, patterns   -> quelle source, quels fichiers
    reader:   options, columns_type    -> comment la lire
    storage:  table_name, format, mode,
              raw_path, staging_path   -> où l'écrire (P2-2, obligatoire)

`extra="forbid"` partout : une clé inconnue ou mal orthographiée est une
erreur, jamais ignorée. Les contrôles qui croisent les sections (storage
présent, nom de table, tables raw/staging disjointes, options du reader
selon le format) sont faits par un seul validateur, qui agrège leurs
messages : une erreur n'en masque pas une autre.

Ce module ne doit pas importer Airflow : le job Spark le charge
(tests/unit/jobs/test_spark_job_no_airflow_import.py).
"""

import difflib
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from pipeline.config.reader_options import (
    ALIAS_GROUPS,
    READER_OPTIONS_BY_FORMAT,
    SCHEMA_FORMATS,
    SPARK_FORMATS,
    SPARK_OPTION_TYPES,
)
from pipeline.config.types import get_spark_type
from utils.path_utils import check_lake_name, lake_relative_parts

# Aligné sur ReaderFactory (vérifié par test_p2_1_whitelists_cover_exactly_the_supported_formats).
# Pas de `xls` : read_excel exige alors xlrd, qui n'est pas installé (C-6).
FileFormat = Literal["csv", "txt", "parquet", "xlsx"]

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


def _check_column_type(col_type: str) -> str:
    get_spark_type(col_type)  # ValueError avec le motif (type inconnu ou complexe)
    return col_type


_TYPE_LABELS: dict[type, str] = {
    bool: "un booléen (true ou false, sans guillemets)",
    int: "un entier (sans guillemets)",
    float: "un nombre (sans guillemets)",
}


def _has_type(value: Any, expected: type) -> bool:
    if expected is bool:
        return isinstance(value, bool)
    # bool est une sous-classe d'int : `maxColumns: true` est refusé.
    if isinstance(value, bool):
        return False
    if expected is float:
        return isinstance(value, int | float)
    return isinstance(value, expected)


def _check_pattern(pattern: str) -> str:
    if "/" in pattern or "\\" in pattern:
        raise ValueError(
            f"motif {pattern!r} : il s'applique au nom du fichier, pas à son chemin "
            "(retirer les répertoires)"
        )
    return pattern


ColumnType = Annotated[str, AfterValidator(_check_column_type)]
Pattern = Annotated[NonEmptyStr, AfterValidator(_check_pattern)]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceSection(_StrictModel):
    name: NonEmptyStr = Field(description="Identifiant unique de la source.")
    format: FileFormat = Field(description="Choisit le reader dans ReaderFactory.")
    patterns: list[Pattern] = Field(
        min_length=1,
        description="Motifs fnmatch sur le NOM du fichier, insensibles à la casse.",
    )


class ReaderSection(_StrictModel):
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Options passées telles quelles au reader (liste blanche par format).",
    )
    columns_type: dict[str, ColumnType] | None = Field(
        default=None,
        description="Schéma explicite {colonne: type}, dans l'ordre du fichier (csv/txt).",
    )


def _check_zone_path(value: str) -> str:
    lake_relative_parts(value)  # ValueError : hors de LAKE_ROOT, segment invalide
    return value


ZonePath = Annotated[str, AfterValidator(_check_zone_path)]
TableName = Annotated[str, AfterValidator(check_lake_name)]


def lake_target_key(target: str) -> tuple[str, ...]:
    """Segments d'une table du lake (`raw/orders`), sans tenir compte de la
    casse : deux tables qui ne diffèrent que par la casse sont un seul
    répertoire sur un FS insensible à la casse (macOS, Windows)."""
    return tuple(segment.casefold() for segment in target.split("/"))


def lake_targets_nested(first: str, second: str) -> tuple[str, str] | None:
    """(englobante, imbriquée) si l'une des deux tables est un préfixe de
    segments strict de l'autre (`raw/orders` et `raw/orders/x`), casse
    ignorée ; sinon None. `raw/orders` et `raw/orders_v2` ne sont pas
    imbriquées."""
    a, b = lake_target_key(first), lake_target_key(second)
    if len(a) == len(b):
        return None
    (outer, outer_key), (inner, inner_key) = sorted(
        ((first, a), (second, b)), key=lambda item: len(item[1])
    )
    return (outer, inner) if inner_key[: len(outer_key)] == outer_key else None


class StorageSection(_StrictModel):
    """Où le job Spark écrit les fichiers de la source (P2-2).

    Chemins relatifs à `LAKE_ROOT` (variable d'environnement du job) :
    `<LAKE_ROOT>/<raw_path>/<table>/` et, si `staging_path` est déclaré,
    `<LAKE_ROOT>/<staging_path>/<table>/`. Aucun ne peut sortir de
    `LAKE_ROOT` (pas de `..`, de chemin absolu ni de schéma d'URI), et les
    tables raw et staging ne peuvent être ni la même ni imbriquées l'une dans
    l'autre (contrôlé par `SourceConfig`, entre sources par `SourceRegistry`).

    Chaque table contient un instantané par listing, partitionné par
    (`lake_run_id`, `lake_source_file`) : voir storage/parquet_lake_writer.py.
    """

    table_name: TableName | None = Field(
        default=None, description="Nom de la table ; `source.name` s'il est absent."
    )
    format: Literal["parquet"]
    mode: Literal["overwrite"]
    raw_path: ZonePath = Field(description="Zone raw : chaque fichier lu, tel quel.")
    staging_path: ZonePath | None = Field(
        default=None,
        description=(
            "Zone staging (lignes vides et doublons exacts retirés) ; absent = pas de staging."
        ),
    )

    @field_validator("mode", mode="before")
    @classmethod
    def _only_overwrite(cls, value: Any) -> Any:
        if value != "overwrite":
            raise ValueError(
                f"mode {value!r} interdit : seul 'overwrite' est accepté (idempotence : "
                "un retry ou un re-run ne doit pas dupliquer les données)"
            )
        return value


class SourceConfig(_StrictModel):
    source: SourceSection
    reader: ReaderSection = Field(default_factory=ReaderSection)
    # Obligatoire (P2-2) : tout fichier lu est au moins écrit en zone raw,
    # rien n'est lu puis perdu en silence. Déclaré optionnel pour Pydantic
    # seulement (C-6) : un champ requis absent empêche les validateurs
    # `after` de tourner, et masquait les erreurs du reader du même fichier.
    # `_check_consistency` le rend obligatoire, avec les autres contrôles.
    storage: StorageSection | None = None

    def _require_storage(self) -> StorageSection:
        if self.storage is None:  # seulement via model_construct, sans validation
            raise ValueError("storage absent : la config n'a pas été validée")
        return self.storage

    @property
    def table(self) -> str:
        """Nom de la table dans le lake : `storage.table_name` ou `source.name`."""
        return self._require_storage().table_name or self.source.name

    @property
    def raw_target(self) -> str:
        """Table raw, relative à LAKE_ROOT (ex. `raw/orders`)."""
        storage = self._require_storage()
        return "/".join((*lake_relative_parts(storage.raw_path), self.table))

    @property
    def staging_target(self) -> str | None:
        """Table staging, relative à LAKE_ROOT ; None sans `staging_path`."""
        storage = self._require_storage()
        if storage.staging_path is None:
            return None
        return "/".join((*lake_relative_parts(storage.staging_path), self.table))

    @model_validator(mode="after")
    def _check_consistency(self) -> "SourceConfig":
        """Tous les contrôles qui croisent les sections, en un seul
        validateur : leurs messages sont agrégés dans une seule erreur (C-6)."""
        problems = self._storage_problems() + self._reader_problems()
        if problems:
            raise ValueError("\n".join(problems))
        return self

    def _storage_problems(self) -> list[str]:
        if self.storage is None:
            return [
                "storage: section obligatoire (P2-2) : tout fichier lu est écrit au moins "
                "en zone raw. Déclarer storage.format, storage.mode et storage.raw_path."
            ]
        if self.storage.table_name is None:
            try:
                check_lake_name(self.source.name)
            except ValueError as exc:
                return [
                    f"storage.table_name absent : source.name sert de nom de table et "
                    f"doit alors être un nom sûr ({exc}). Déclarer storage.table_name."
                ]
        raw, staging = self.raw_target, self.staging_target
        if staging is None:
            return []
        if lake_target_key(raw) == lake_target_key(staging):
            return [
                f"storage.staging_path {self.storage.staging_path!r} : même table que raw "
                f"({raw!r}, casse ignorée), la zone staging écraserait les partitions raw"
            ]
        nested = lake_targets_nested(raw, staging)
        if nested is not None:
            outer, inner = nested
            return [
                f"storage.staging_path {self.storage.staging_path!r} : tables raw {raw!r} et "
                f"staging {staging!r} imbriquées ({inner!r} dans {outer!r}, casse ignorée) : "
                f"Spark ne pourrait plus lire {outer!r} (CONFLICTING_DIRECTORY_STRUCTURES). "
                "Choisir des répertoires disjoints."
            ]
        return []

    def _reader_problems(self) -> list[str]:
        fmt = self.source.format
        allowed = READER_OPTIONS_BY_FORMAT[fmt]
        problems: list[str] = []

        for key, value in self.reader.options.items():
            if key not in allowed:
                hint = difflib.get_close_matches(key, sorted(allowed), n=1, cutoff=0.6)
                suggestion = f" (vouliez-vous dire {hint[0]!r} ?)" if hint else ""
                problems.append(
                    f"reader.options.{key}: option non supportée pour le format {fmt!r}{suggestion}"
                )
            elif fmt in SPARK_FORMATS and not isinstance(value, str | bool | int | float):
                problems.append(
                    f"reader.options.{key}: une option Spark doit être une valeur scalaire "
                    f"(texte, nombre ou booléen), pas {value!r}"
                )
            elif (
                fmt in SPARK_FORMATS
                and key in SPARK_OPTION_TYPES
                and not _has_type(value, SPARK_OPTION_TYPES[key])
            ):
                problems.append(
                    f"reader.options.{key}: doit être "
                    f"{_TYPE_LABELS[SPARK_OPTION_TYPES[key]]}, pas {value!r}"
                )

        for group in ALIAS_GROUPS:
            declared = sorted(group & self.reader.options.keys())
            if len(declared) > 1:
                problems.append(
                    f"reader.options: {' et '.join(declared)} sont deux noms de la même "
                    "option Spark, Spark n'en retiendrait qu'un : n'en déclarer qu'un"
                )

        if self.reader.columns_type and fmt not in SCHEMA_FORMATS:
            problems.append(
                f"reader.columns_type: n'est appliqué que pour les formats "
                f"{', '.join(sorted(SCHEMA_FORMATS))} (format {fmt!r})"
            )
        return problems
