"""Contrôles qualité déclarés dans la config de chaque source (P2-3).

    quality:
      checks:
        - type: not_null          # column
          column: id
        - type: unique            # columns : unicité de la combinaison
          columns: [id]
        - type: accepted_values   # column, values : du texte, entre guillemets
          column: status
          values: ["new", "done"]
          severity: warn          # error (défaut) | warn
        - type: range             # column, min et/ou max, inclusifs, colonne numérique
          column: amount          # entiers dans l'intervalle int64, nombres finis
          min: 0
        - type: regex             # column, pattern (rlike : recherche, pas correspondance totale)
          column: code
          pattern: "^[A-Z]{3}$"
        - type: row_count         # min et/ou max
          min: 1
          name: not_empty         # optionnel : nom stable, sinon dérivé (`row_count`)

Union discriminée sur `type`, `extra="forbid"` partout. Les contrôles
s'exécutent en Spark pur (`pipeline/quality/checks.py`) sur le DataFrame
nettoyé pour le staging. Statut de chaque contrôle : `pass`, `fail` (échec,
sévérité `error`), `warn` (échec, sévérité `warn`) ou `error` (il n'a pas pu
s'exécuter). Un contrôle de sévérité `error` en statut `fail` ou `error`
empêche l'écriture du staging ; un contrôle `warn` est seulement consigné,
même s'il n'a pas pu s'exécuter.

`accepted_values` : uniquement du texte, comparé à la forme texte de la
colonne. PyYAML lit les configs en YAML 1.1, qui convertit les scalaires
non quotés : `NO`, `yes`, `on`, `off` deviennent des booléens, `007` vaut 7,
`0123` vaut 83 (octal), `1:30` vaut 90 (sexagésimal). Une valeur non
textuelle est donc refusée : écrire `"NO"`, `"007"`, `"true"`, `"1"`. Sur
une colonne déclarée `float`/`double` dans `columns_type`, la forme texte
vaut `1.0` : `accepted_values` y est refusé, utiliser `range`.

`range` : bornes entières dans l'intervalle int64 (au-delà, Spark ne sait
pas construire le littéral), nombres finis (NaN et inf refusés). Sur une
colonne float/double, NaN compte comme hors bornes. `not_null` ne détecte
pas NaN (NaN n'est pas null pour Spark).

Ce module valide la forme de chaque contrôle (champs, types, clés). Les
règles sémantiques (bornes, regex, colonnes de `columns_type`, doublons,
staging obligatoire) sont vérifiées par `SourceConfig`, dans son validateur
unique : leurs messages s'agrègent avec ceux des autres sections (C-6).

Regex : le moteur exécute `rlike`, donc `java.util.regex`. La config exige
une regex valide pour Python ET pour Java, c'est-à-dire le sous-ensemble
commun (classes, quantificateurs, ancres, `(?i)`, `(?:...)`, lookarounds) :
- au chargement (avant Spark), `re.compile` : `\\p{L}`, `\\h`, `\\z`
  (Java seulement) sont refusés ; les groupes nommés aussi (`(?P<n>...)` en
  Python, `(?<n>...)` en Java : aucune syntaxe commune), ainsi que `[[`
  (classe imbriquée en Java, `[` littéral en Python) ;
- au démarrage de Spark, le job compile en Java toutes les regex du registre
  (`java_regex_problems`) : une regex que Python accepte mais que Java
  refuse (`a{` : `{` littéral en Python, répétition invalide en Java) fait
  échouer tout le run, code 5, avant tout fichier ;
- à l'exécution, `java.util.regex.Pattern.compile` encore, avant
  l'agrégation : une regex que Java refuse donne le statut `error` à ce
  contrôle seul (défense en profondeur).
`rlike` cherche une correspondance n'importe où dans la valeur (`find`) :
ancrer avec `^` et `$` pour exiger une correspondance complète.

Ce module ne doit pas importer Airflow ni `pipeline.file_reader`.
"""

import difflib
import re
import warnings
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    AllowInfNan,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    StrictStr,
    StringConstraints,
    field_validator,
)
from pyspark.sql.types import FractionalType, NumericType

from pipeline.config.types import get_spark_type

# Zone réservée sous LAKE_ROOT aux résultats des contrôles :
# <LAKE_ROOT>/quality/<table>/ (aucun raw_path ni staging_path ne peut y entrer).
QUALITY_ZONE = "quality"

Severity = Literal["error", "warn"]

# Bornes d'un entier que Spark sait comparer (LongType) : au-delà, `F.lit`
# lève une Py4JError (NumberFormatException) à l'exécution (C-1).
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


def _check_column_name(value: str) -> str:
    if not value.strip():
        raise ValueError("nom de colonne vide")
    return value


# Pas de strip : un nom de colonne est comparé tel quel à celui des données.
ColumnName = Annotated[str, AfterValidator(_check_column_name)]
CheckName = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


def _check_int64(value: Any) -> Any:
    # Avant la validation : sinon un entier trop grand glisse vers la branche
    # float de `Number` (1e20) au lieu d'être refusé.
    if isinstance(value, int) and not isinstance(value, bool):
        if not INT64_MIN <= value <= INT64_MAX:
            raise ValueError(
                f"{value} hors de l'intervalle int64 [{INT64_MIN}, {INT64_MAX}] : "
                "Spark ne peut pas comparer une colonne à cette borne"
            )
    return value


Int64 = Annotated[StrictInt, Field(ge=INT64_MIN, le=INT64_MAX)]
Number = Annotated[
    Int64 | Annotated[StrictFloat, AllowInfNan(False)], BeforeValidator(_check_int64)
]
Count = Annotated[StrictInt, Field(ge=0, le=INT64_MAX), BeforeValidator(_check_int64)]


class _Check(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: CheckName | None = Field(
        default=None, description="Nom stable dans les résultats ; dérivé s'il est absent."
    )
    severity: Severity = Field(
        default="error",
        description="error : un échec empêche le staging ; warn : consigné seulement.",
    )

    @property
    def check_columns(self) -> tuple[str, ...]:
        """Colonnes contrôlées, dans l'ordre déclaré (aucune pour row_count)."""
        column = getattr(self, "column", None)
        return (column,) if column is not None else ()

    @property
    def check_name(self) -> str:
        """`name` s'il est fourni, sinon `<type>(<colonnes>)` (`row_count` seul)."""
        if self.name:
            return self.name
        columns = self.check_columns
        return f"{self.type}({','.join(columns)})" if columns else self.type

    def identity(self) -> tuple:
        """La règle elle-même, sans `name` ni `severity` : deux contrôles de
        même identité sont des doublons."""
        return (self.type, *sorted(self._parameters().items()))

    def _parameters(self) -> dict[str, Any]:
        return self.model_dump(exclude={"name", "severity", "type"})

    def semantic_problems(self) -> list[str]:
        """Erreurs que la forme seule n'attrape pas (bornes, regex)."""
        return []


def _bounds_problems(minimum: float | None, maximum: float | None) -> list[str]:
    if minimum is None and maximum is None:
        return ["déclarer min ou max (ou les deux)"]
    if minimum is not None and maximum is not None and minimum > maximum:
        return [f"min ({minimum}) > max ({maximum})"]
    return []


class NotNullCheck(_Check):
    type: Literal["not_null"]
    column: ColumnName


class UniqueCheck(_Check):
    type: Literal["unique"]
    columns: list[ColumnName] = Field(
        min_length=1, description="Unicité de la combinaison ; une ligne à clé nulle est ignorée."
    )

    @field_validator("columns")
    @classmethod
    def _distinct_columns(cls, columns: list[str]) -> list[str]:
        doubles = sorted({c for c in columns if columns.count(c) > 1})
        if doubles:
            raise ValueError(f"colonne(s) citée(s) deux fois : {', '.join(map(repr, doubles))}")
        return columns

    @property
    def check_columns(self) -> tuple[str, ...]:
        return tuple(self.columns)

    def _parameters(self) -> dict[str, Any]:
        return {"columns": frozenset(self.columns)}


_YAML_11_ADVICE = (
    "accepted_values n'accepte que du texte : écrire chaque valeur entre guillemets "
    '("NO", "007", "true", "1"). PyYAML lit la config en YAML 1.1 et convertit les '
    "valeurs non quotées : NO/yes/on/off deviennent des booléens, 007 vaut 7, "
    "0123 vaut 83 (octal), 1:30 vaut 90"
)


class AcceptedValuesCheck(_Check):
    type: Literal["accepted_values"]
    column: ColumnName
    values: list[StrictStr] = Field(
        min_length=1,
        description="Texte comparé à la forme texte de la colonne ; les nulls sont ignorés.",
    )

    @field_validator("values", mode="before")
    @classmethod
    def _text_only(cls, values: Any) -> Any:
        if isinstance(values, list):
            converted = [v for v in values if not isinstance(v, str)]
            if converted:
                read_as = ", ".join(f"{v!r} ({type(v).__name__})" for v in converted)
                raise ValueError(f"valeur(s) lue(s) comme {read_as} : {_YAML_11_ADVICE}")
        return values

    @field_validator("values")
    @classmethod
    def _distinct_values(cls, values: list[str]) -> list[str]:
        doubles = sorted({v for v in values if values.count(v) > 1})
        if doubles:
            raise ValueError(f"valeur(s) citée(s) deux fois : {', '.join(map(repr, doubles))}")
        return values

    def _parameters(self) -> dict[str, Any]:
        return {"column": self.column, "values": frozenset(self.values)}


class RangeCheck(_Check):
    type: Literal["range"]
    column: ColumnName
    min: Number | None = Field(default=None, description="Borne basse incluse.")
    max: Number | None = Field(default=None, description="Borne haute incluse.")

    def semantic_problems(self) -> list[str]:
        return _bounds_problems(self.min, self.max)


class RegexCheck(_Check):
    type: Literal["regex"]
    column: ColumnName
    pattern: Annotated[str, StringConstraints(min_length=1)]

    def semantic_problems(self) -> list[str]:
        problem = python_regex_problem(self.pattern)
        return [problem] if problem else []


class RowCountCheck(_Check):
    type: Literal["row_count"]
    min: Count | None = Field(default=None, description="Nombre de lignes minimum, inclus.")
    max: Count | None = Field(default=None, description="Nombre de lignes maximum, inclus.")

    def semantic_problems(self) -> list[str]:
        return _bounds_problems(self.min, self.max)


QualityCheck = Annotated[
    NotNullCheck | UniqueCheck | AcceptedValuesCheck | RangeCheck | RegexCheck | RowCountCheck,
    Field(discriminator="type"),
]


class QualitySection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    checks: list[QualityCheck] = Field(min_length=1)


def python_regex_problem(pattern: str) -> str | None:
    """Message d'erreur si `pattern` sort du sous-ensemble commun à Python et
    Java vérifiable sans JVM, None sinon (voir le docstring du module)."""
    advice = (
        "la regex doit être valide pour Python (re) et pour Java (rlike, qui l'exécute) : "
        r"pas de \p{...}, \h, \z, \Q...\E (citer avec \ chaque caractère spécial), "
        r"\e (écrire \x1b), de groupe nommé ni de classe imbriquée [[...]]"
    )
    re.purge()  # sinon un motif déjà compilé ne réémet pas son FutureWarning
    with warnings.catch_warnings():
        # « Possible nested set » : `[[` est une classe imbriquée pour Java.
        warnings.simplefilter("error", FutureWarning)
        try:
            compiled = re.compile(pattern)
        except (re.error, FutureWarning) as exc:
            return f"regex invalide {pattern!r} ({exc}) : {advice}"
    if compiled.groupindex:
        return (
            f"regex {pattern!r} : groupe nommé (?P<...>) propre à Python, que Java refuse : "
            "utiliser (...) ou (?:...)"
        )
    return None


def quality_problems(
    quality: QualitySection, columns_type: dict[str, str] | None, staging_path: str | None
) -> list[str]:
    """Règles qui croisent les contrôles et les autres sections ; messages
    préfixés par leur chemin (`quality.checks.<i> (<nom>)`)."""
    problems: list[str] = []
    if staging_path is None:
        problems.append(
            "quality: exige storage.staging_path : les contrôles décident de l'écriture en "
            "staging, une source sans zone staging ne peut pas en déclarer"
        )

    seen_rules: dict[tuple, int] = {}
    seen_names: dict[str, int] = {}
    for index, check in enumerate(quality.checks):
        where = f"quality.checks.{index} ({check.check_name})"
        problems.extend(f"{where}: {p}" for p in check.semantic_problems())
        if columns_type is not None:
            problems.extend(f"{where}: {p}" for p in _column_problems(check, columns_type))

        rule = check.identity()
        if rule in seen_rules:
            problems.append(
                f"{where}: contrôle identique à quality.checks.{seen_rules[rule]} "
                "(même règle, nom et sévérité mis à part) : n'en garder qu'un"
            )
        else:
            seen_rules[rule] = index
            name = check.check_name
            if name in seen_names:
                problems.append(
                    f"{where}: nom {name!r} déjà pris par quality.checks.{seen_names[name]} : "
                    "donner à l'un des deux un `name` explicite"
                )
            else:
                seen_names[name] = index
    return problems


def _column_problems(check: _Check, columns_type: dict[str, str]) -> list[str]:
    problems = []
    for column in check.check_columns:
        if column not in columns_type:
            hint = difflib.get_close_matches(column, list(columns_type), n=1, cutoff=0.6)
            suggestion = f" (vouliez-vous dire {hint[0]!r} ?)" if hint else ""
            problems.append(f"colonne {column!r} absente de reader.columns_type{suggestion}")
        elif isinstance(check, RangeCheck | AcceptedValuesCheck):
            declared = columns_type[column]
            try:
                spark_type = get_spark_type(declared)
            except ValueError:  # type invalide : déjà signalé sur reader.columns_type
                continue
            if isinstance(check, RangeCheck) and not isinstance(spark_type, NumericType):
                problems.append(
                    f"colonne {column!r} de type {declared!r} : range exige un type numérique"
                )
            elif isinstance(check, AcceptedValuesCheck) and isinstance(spark_type, FractionalType):
                problems.append(
                    f"colonne {column!r} de type {declared!r} : accepted_values compare la "
                    "forme texte de la colonne (1.0, pas 1 ; 1.00 pour un decimal), qui "
                    "rejetterait des valeurs attendues. Utiliser range (min/max) pour un nombre"
                )
    return problems
