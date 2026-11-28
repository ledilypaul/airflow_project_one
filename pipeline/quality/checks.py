"""Moteur des contrôles qualité (P2-3), en Spark pur.

Les contrôles (`pipeline/config/quality.py`) s'exécutent sur le DataFrame
nettoyé pour le staging, avant l'ajout des colonnes techniques du lake.

Exécution, sans rapatrier de données sur le driver (une ligne d'agrégat par action) :
- une seule agrégation pour le nombre total de lignes (`row_count`) et tous
  les contrôles par ligne (`not_null`, `accepted_values`, `range`, `regex`),
  chacun étant un compte conditionnel (`count_if`) ;
- un `groupBy` par contrôle `unique`, qui compte les lignes dont la clé est
  partagée avec au moins une autre ligne (pas le nombre de clés en double).

Règles :
- `accepted_values`, `range`, `regex` ignorent les nulls (c'est le rôle de
  `not_null`) ; `unique` ignore les lignes dont une colonne de la clé est
  nulle (comme une contrainte UNIQUE SQL) ;
- `accepted_values` compare la forme texte de la colonne (`cast("string")`)
  aux valeurs, qui sont du texte (`"true"`/`"false"` pour un booléen) :
  jamais de conversion implicite de la colonne, qui ferait échouer
  l'agrégation entière en mode ANSI (`'abc' = 1`) ;
- `range` exige une colonne numérique ; sur une colonne float/double (type
  réel du DataFrame), NaN compte comme hors bornes, quelle que soit la
  borne (Spark classe NaN au-dessus de tout nombre : sans cela, `min` seul
  l'accepterait et `max` le rejetterait) ;
- `not_null` ne compte que les nulls : NaN n'est pas null et passe ;
- `regex` applique `rlike` (Java, recherche partielle) à la forme texte de
  la colonne ;
- les noms de colonnes sont comparés exactement (sans tenir compte de
  `spark.sql.caseSensitive`), un point n'est pas un chemin de struct.

Statuts (colonne `status` des résultats) :
- `pass`  : le contrôle a tourné et n'a trouvé aucune ligne en échec ;
- `fail`  : il a trouvé des lignes en échec, sévérité `error` ;
- `warn`  : il a trouvé des lignes en échec, sévérité `warn` ;
- `error` : il n'a pas pu s'exécuter (colonne absente, type non numérique,
  regex refusée par Java, erreur Spark, toute exception imprévue) : son
  résultat est inconnu, `failed_rows` est null.
Un résultat est bloquant (staging non écrit) si sa sévérité est `error` et
son statut `fail` ou `error`. Un contrôle `warn` ne bloque jamais, même en
statut `error` : le job le consigne au niveau ERROR.

Un contrôle qui ne peut pas s'exécuter échoue seul : toute exception levée
en construisant sa condition est convertie en statut `error`. Si
l'agrégation commune échoue, chaque contrôle est réévalué séparément pour
qu'une erreur n'emporte pas les autres.

Ce module ne doit pas importer Airflow : le job Spark le charge.
"""

import difflib
import logging
from collections.abc import Iterable, Sequence
from functools import reduce

import pyarrow as pa
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DataType,
    DoubleType,
    FloatType,
    LongType,
    NumericType,
    StringType,
    StructField,
    StructType,
)

from pipeline.config.quality import (
    AcceptedValuesCheck,
    NotNullCheck,
    QualityCheck,
    RangeCheck,
    RegexCheck,
    RowCountCheck,
    UniqueCheck,
)
from pipeline.quality import results as neutral
from pipeline.quality.results import (
    ERROR,
    FAIL,
    PASS,
    WARN,
    CheckResult,
    QualityChecksError,
    QualityChecksFailed,
    blocking_failures,
    failure_summary,
    result_rows,
    short_error,
)

log = logging.getLogger(__name__)

__all__ = [
    "ERROR",
    "FAIL",
    "PASS",
    "RESULTS_SCHEMA",
    "WARN",
    "CheckResult",
    "QualityChecksError",
    "QualityChecksFailed",
    "blocking_failures",
    "failure_summary",
    "java_regex_problems",
    "results_dataframe",
    "run_quality_checks",
    "short_error",
]


def _spark_type(arrow: pa.DataType) -> DataType:
    if pa.types.is_list(arrow):
        return ArrayType(_spark_type(arrow.value_type), arrow.value_field.nullable)
    return {pa.string(): StringType(), pa.int64(): LongType()}[arrow]


# Le schéma Spark des résultats est dérivé du schéma neutre (Arrow).
RESULTS_SCHEMA = StructType(
    [StructField(f.name, _spark_type(f.type), f.nullable) for f in neutral.RESULTS_SCHEMA]
)


class _CheckError(Exception):
    """Le contrôle ne peut pas s'exécuter sur ces données."""


def run_quality_checks(data: DataFrame, checks: Sequence[QualityCheck]) -> list[CheckResult]:
    """Exécute `checks` sur `data` ; un résultat par contrôle, dans l'ordre.

    Coût : une agrégation, plus un `groupBy` par contrôle `unique`. `data`
    devrait être en cache (plusieurs actions) : c'est à l'appelant de le
    faire, il sait s'il le réutilise ensuite.
    """
    checks = list(checks)
    errors: dict[int, str] = {}
    conditions: dict[int, Column] = {}
    unique_keys: dict[int, list[Column]] = {}
    for index, check in enumerate(checks):
        try:
            if isinstance(check, UniqueCheck):
                unique_keys[index] = [_column(data, name) for name in check.columns]
            elif not isinstance(check, RowCountCheck):
                conditions[index] = _failing_row_condition(data, check)
        except Exception as exc:  # noqa: BLE001 - C-1 : un contrôle n'arrête pas les autres
            errors[index] = short_error(exc)

    total, counts = _aggregate(data, conditions, errors)
    if total is None:
        for index, check in enumerate(checks):
            if isinstance(check, RowCountCheck):
                errors.setdefault(index, errors.get(-1, "row count failed"))

    for index, keys in unique_keys.items():
        try:
            counts[index] = _duplicated_rows(data, keys)
        except Exception as exc:  # noqa: BLE001 - un contrôle n'arrête pas les autres
            errors[index] = short_error(exc)

    return [
        _result(check, counts.get(index), total, errors.get(index))
        for index, check in enumerate(checks)
    ]


def results_dataframe(
    spark: SparkSession, results: Sequence[CheckResult], *, source_name: str
) -> DataFrame:
    """Les résultats en DataFrame (`RESULTS_SCHEMA`), une ligne par contrôle."""
    return spark.createDataFrame(result_rows(results, source_name=source_name), RESULTS_SCHEMA)


# ---------------------------------------------------------------------------
# Construction des expressions (analyse seulement, aucune action)
# ---------------------------------------------------------------------------


def _column(data: DataFrame, name: str) -> Column:
    """La colonne `name`, nom exact ; erreur claire si elle est absente."""
    if name not in data.columns:
        same_case = [c for c in data.columns if c.casefold() == name.casefold()]
        hint = same_case or difflib.get_close_matches(name, data.columns, n=1, cutoff=0.6)
        suggestion = f" (did you mean {hint[0]!r}?)" if hint else ""
        raise _CheckError(f"check error: column {name!r} not found in the data{suggestion}")
    # Backquotes : `a.b` est un nom de colonne, pas le champ b d'une struct a.
    return F.col("`" + name.replace("`", "``") + "`")


def _as_text(value: bool | int | str) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _failing_row_condition(data: DataFrame, check: QualityCheck) -> Column:
    """Condition vraie pour une ligne en échec (jamais vraie sur un null,
    sauf pour not_null)."""
    column = _column(data, check.column)
    if isinstance(check, NotNullCheck):
        return column.isNull()
    if isinstance(check, AcceptedValuesCheck):
        accepted = [_as_text(v) for v in check.values]
        return column.isNotNull() & ~column.cast("string").isin(accepted)
    if isinstance(check, RangeCheck):
        data_type = data.schema[check.column].dataType
        if not isinstance(data_type, NumericType):
            raise _CheckError(
                f"check error: column {check.column!r} is {data_type.simpleString()}, "
                "not numeric: range needs a numeric column"
            )
        out_of_range = [
            column < F.lit(check.min) if check.min is not None else None,
            column > F.lit(check.max) if check.max is not None else None,
            # C-4 : NaN hors bornes, quelle que soit la borne déclarée.
            F.isnan(column) if isinstance(data_type, FloatType | DoubleType) else None,
        ]
        return column.isNotNull() & reduce(
            lambda a, b: a | b, [c for c in out_of_range if c is not None]
        )
    if isinstance(check, RegexCheck):
        java_error = _java_regex_error(data, check.pattern)
        if java_error is not None:
            raise _CheckError(f"check error: invalid Java regex (rlike): {java_error}")
        return column.isNotNull() & ~column.cast("string").rlike(check.pattern)
    raise TypeError(f"unsupported check type {check.type!r}")  # pragma: no cover


def _java_regex_error(data: DataFrame, pattern: str) -> str | None:
    """Compile `pattern` avec `java.util.regex` (celui de `rlike`) avant
    l'agrégation : une regex que Java refuse ferait échouer l'agrégation
    commune. None si elle compile, ou sans accès à la JVM (Spark Connect :
    l'agrégation échoue alors et chaque contrôle est réévalué seul)."""
    return _compile_java_regex(data.sparkSession, pattern)


def _compile_java_regex(spark: SparkSession, pattern: str) -> str | None:
    jvm = getattr(spark, "_jvm", None)
    if jvm is None:
        return None
    try:
        jvm.java.util.regex.Pattern.compile(pattern)
    except Exception as exc:  # noqa: BLE001 - Py4JJavaError
        return short_error(exc)
    return None


def java_regex_problems(spark: SparkSession, configs: Iterable[tuple[object, object]]) -> list[str]:
    """Compile avec `java.util.regex` (celui de `rlike`) chaque regex des
    `configs` (couples (fichier, `SourceConfig`)) ; un message par regex
    refusée, qui nomme le fichier, la source, le contrôle, la regex et
    l'erreur Java.

    C-8 : Python valide la regex au chargement, mais certaines regex qu'il
    accepte sont refusées par Java (`a{`). Le job l'appelle juste après le
    démarrage de Spark, avant le premier fichier : une config invalide est
    une erreur de tout le run (code 5), pas d'un fichier. Sans accès à la JVM
    (Spark Connect) : liste vide, le moteur garde son contrôle par contrôle.
    """
    problems = []
    for path, config in configs:
        # P5-2 : la regex d'une source Polars s'exécute en Rust, pas en Java.
        if config.quality is None or config.engine != "spark":
            continue
        for index, check in enumerate(config.quality.checks):
            if not isinstance(check, RegexCheck):
                continue
            error = _compile_java_regex(spark, check.pattern)
            if error is not None:
                problems.append(
                    f"{path}: source {config.source.name!r}, quality.checks.{index} "
                    f"({check.check_name}): regex {check.pattern!r} refusée par Java "
                    f"(rlike) : {error}"
                )
    return problems


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------


def _aggregate(
    data: DataFrame, conditions: dict[int, Column], errors: dict[int, str]
) -> tuple[int | None, dict[int, int]]:
    """(nombre de lignes, lignes en échec par contrôle), en une agrégation ;
    si elle échoue, une agrégation par contrôle pour isoler l'erreur."""
    indexes = list(conditions)
    try:
        row = data.agg(
            F.count(F.lit(1)),
            *(F.count_if(conditions[i]).alias(f"check_{i}") for i in indexes),
        ).first()
        return row[0], {i: row[k + 1] for k, i in enumerate(indexes)}
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "quality: the combined aggregation failed (%s); evaluating each check on its own",
            short_error(exc),
        )

    total = None
    try:
        total = data.agg(F.count(F.lit(1))).first()[0]
    except Exception as exc:  # noqa: BLE001
        errors[-1] = short_error(exc)
    counts = {}
    for i in indexes:
        try:
            counts[i] = data.agg(F.count_if(conditions[i])).first()[0]
        except Exception as exc:  # noqa: BLE001
            errors[i] = short_error(exc)
    return total, counts


def _duplicated_rows(data: DataFrame, keys: list[Column]) -> int:
    """Lignes dont la clé (non nulle) apparaît plus d'une fois."""
    group_rows = "__quality_group_rows"
    duplicated = (
        data.where(reduce(lambda a, b: a & b, [k.isNotNull() for k in keys]))
        .groupBy(*keys)
        .agg(F.count(F.lit(1)).alias(group_rows))
        .where(F.col(group_rows) > 1)
        .select(F.sum(group_rows))
        .first()[0]
    )
    return int(duplicated or 0)


# ---------------------------------------------------------------------------
# Résultats
# ---------------------------------------------------------------------------


def _rows(count: int) -> str:
    return f"{count} row" if count == 1 else f"{count} rows"


def _result(
    check: QualityCheck, failed: int | None, total: int | None, error: str | None
) -> CheckResult:
    if error is not None:
        if not error.startswith("check error:"):
            error = f"check error: {error}"
        return CheckResult(check, ERROR, None, total, error)

    if isinstance(check, RowCountCheck):
        too_few = check.min is not None and total < check.min
        too_many = check.max is not None and total > check.max
        if not (too_few or too_many):
            return CheckResult(check, PASS, None, total, None)
        if check.min is not None and check.max is not None:
            expected = f"between {check.min} and {check.max}"
        elif check.min is not None:
            expected = f">= {check.min}"
        else:
            expected = f"<= {check.max}"
        message = f"{_rows(total)}, expected {expected}"
        return CheckResult(check, _failed_status(check), None, total, message)

    failed = int(failed)
    if failed == 0:
        return CheckResult(check, PASS, 0, total, None)
    return CheckResult(check, _failed_status(check), failed, total, _rows(failed))


def _failed_status(check: QualityCheck) -> str:
    return WARN if check.severity == "warn" else FAIL
