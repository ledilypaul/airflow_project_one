"""Modèle unique (Pydantic) des configs de lecture : `config/sources/*.yaml`
et `config/defaults/*.yaml` (P2-1, constat M4).

    source:   name, format, patterns   -> quelle source, quels fichiers
    reader:   options, columns_type    -> comment la lire
    storage:  (optionnel)              -> où l'écrire (consommé par P2-2)

`extra="forbid"` partout : une clé inconnue ou mal orthographiée est une
erreur, jamais ignorée. Ce module ne doit pas importer Airflow : le job Spark
le charge (tests/unit/jobs/test_spark_job_no_airflow_import.py).
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


class StorageSection(_StrictModel):
    table_name: NonEmptyStr | None = None
    format: Literal["parquet"]
    mode: Literal["overwrite"]
    raw_path: NonEmptyStr
    staging_path: NonEmptyStr

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
    storage: StorageSection | None = None

    @model_validator(mode="after")
    def _check_reader_against_format(self) -> "SourceConfig":
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

        if problems:
            raise ValueError("\n".join(problems))
        return self
