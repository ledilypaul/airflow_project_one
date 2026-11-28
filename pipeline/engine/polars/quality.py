"""Contrôles qualité par le moteur Polars (P5-7), mêmes règles et mêmes
résultats que le moteur Spark (`pipeline/quality/checks.py`, dont le docstring
décrit les règles et les statuts) :

- une seule agrégation pour le nombre de lignes et les contrôles par ligne,
  un `group_by` par contrôle `unique` ; aucune ligne de données collectée ;
- si l'agrégation commune échoue, chaque contrôle est réévalué seul ;
- `accepted_values` et `regex` lisent la forme texte de la colonne, celle de
  Spark (`cast("string")`) : `true`/`false`, `2026-11-01 10:00:00.12` (zéros
  de fin retirés), `NaN`, `Infinity`. Écart connu : Spark écrit `1.0E10` et
  `1.5E-5`, Polars `10000000000.0` et `0.000015` ;
- `regex` : `str.contains` (moteur Rust, recherche partielle).
"""

import logging
from collections.abc import Iterable, Sequence

import polars as pl

from pipeline.config.quality import (
    AcceptedValuesCheck,
    NotNullCheck,
    QualityCheck,
    RangeCheck,
    RegexCheck,
    RowCountCheck,
    UniqueCheck,
)
from pipeline.quality.results import (
    CheckResult,
    as_text,
    check_result,
    missing_column_error,
    short_error,
)

log = logging.getLogger(__name__)

# Nom Spark (`simpleString`) des types, pour des messages identiques entre moteurs.
_SPARK_NAMES = {
    pl.String: "string",
    pl.Int32: "int",
    pl.Int64: "bigint",
    pl.Float32: "float",
    pl.Float64: "double",
    pl.Boolean: "boolean",
    pl.Date: "date",
}


class _CheckError(Exception):
    """Le contrôle ne peut pas s'exécuter sur ces données."""


def run_quality_checks(data: pl.LazyFrame, checks: Sequence[QualityCheck]) -> list[CheckResult]:
    """Exécute `checks` sur `data` ; un résultat par contrôle, dans l'ordre."""
    checks = list(checks)
    schema = data.collect_schema()
    errors: dict[int, str] = {}
    conditions: dict[int, pl.Expr] = {}
    unique_keys: dict[int, list[str]] = {}
    for index, check in enumerate(checks):
        try:
            if isinstance(check, UniqueCheck):
                unique_keys[index] = [_column(schema, name) for name in check.columns]
            elif not isinstance(check, RowCountCheck):
                conditions[index] = _failing_row_condition(schema, check)
        except Exception as exc:  # noqa: BLE001 - un contrôle n'arrête pas les autres
            errors[index] = short_error(exc)

    total, counts = _aggregate(data, conditions, errors)
    if total is None:
        for index, check in enumerate(checks):
            if isinstance(check, RowCountCheck):
                errors.setdefault(index, errors.get(-1, "row count failed"))

    for index, keys in unique_keys.items():
        try:
            counts[index] = _duplicated_rows(data, keys)
        except Exception as exc:  # noqa: BLE001
            errors[index] = short_error(exc)

    return [
        check_result(check, counts.get(index), total, errors.get(index))
        for index, check in enumerate(checks)
    ]


def polars_regex_problems(configs: Iterable[tuple[object, object]]) -> list[str]:
    """Compile chaque regex des sources Polars de `configs` (couples (fichier,
    `SourceConfig`)) ; un message par regex refusée, comme `java_regex_problems`
    pour Spark : une regex invalide est une erreur de config du run (code 5)."""
    problems = []
    for path, config in configs:
        if config.quality is None or config.engine != "polars":
            continue
        for index, check in enumerate(config.quality.checks):
            error = _regex_error(check.pattern) if isinstance(check, RegexCheck) else None
            if error is not None:
                problems.append(
                    f"{path}: source {config.source.name!r}, quality.checks.{index} "
                    f"({check.check_name}): regex {check.pattern!r} refusée par Polars "
                    f"(str.contains) : {error}"
                )
    return problems


def _regex_error(pattern: str) -> str | None:
    try:
        pl.select(pl.lit("").str.contains(pattern))
    except Exception as exc:  # noqa: BLE001 - ComputeError de Polars
        return short_error(exc)
    return None


def _column(schema: pl.Schema, name: str) -> str:
    if name not in schema:
        raise _CheckError(missing_column_error(name, schema.names()))
    return name


def _text(column: pl.Expr, dtype: pl.DataType) -> pl.Expr:
    """La forme texte de Spark (`cast("string")`) d'une colonne."""
    if isinstance(dtype, pl.Datetime):
        micros = column.dt.microsecond().cast(pl.String).str.zfill(6).str.strip_chars_end("0")
        seconds = column.dt.strftime("%Y-%m-%d %H:%M:%S")
        return pl.when(micros == "").then(seconds).otherwise(seconds + "." + micros)
    if dtype.is_float():
        return (
            pl.when(column == float("inf"))
            .then(pl.lit("Infinity"))
            .when(column == float("-inf"))
            .then(pl.lit("-Infinity"))
            .otherwise(column.cast(pl.String))
        )
    return column.cast(pl.String)


def _failing_row_condition(schema: pl.Schema, check: QualityCheck) -> pl.Expr:
    """Vraie pour une ligne en échec (jamais sur un null, sauf pour not_null)."""
    column = pl.col(_column(schema, check.column))
    dtype = schema[check.column]
    if isinstance(check, NotNullCheck):
        return column.is_null()
    if isinstance(check, AcceptedValuesCheck):
        accepted = [as_text(v) for v in check.values]
        return column.is_not_null() & ~_text(column, dtype).is_in(accepted)
    if isinstance(check, RangeCheck):
        if not dtype.is_numeric():
            name = _SPARK_NAMES.get(dtype, str(dtype).lower())
            raise _CheckError(
                f"check error: column {check.column!r} is {name}, "
                "not numeric: range needs a numeric column"
            )
        out_of_range = [
            column < check.min if check.min is not None else None,
            column > check.max if check.max is not None else None,
            column.is_nan() if dtype.is_float() else None,  # NaN toujours hors bornes
        ]
        return column.is_not_null() & pl.any_horizontal(c for c in out_of_range if c is not None)
    if isinstance(check, RegexCheck):
        error = _regex_error(check.pattern)
        if error is not None:
            raise _CheckError(f"check error: invalid regex (str.contains): {error}")
        return column.is_not_null() & ~_text(column, dtype).str.contains(check.pattern)
    raise TypeError(f"unsupported check type {check.type!r}")  # pragma: no cover


def _aggregate(
    data: pl.LazyFrame, conditions: dict[int, pl.Expr], errors: dict[int, str]
) -> tuple[int | None, dict[int, int]]:
    indexes = list(conditions)
    try:
        row = (
            data.select(pl.len(), *(conditions[i].sum().alias(f"check_{i}") for i in indexes))
            .collect()
            .row(0)
        )
        return row[0], {i: row[k + 1] for k, i in enumerate(indexes)}
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "quality: the combined aggregation failed (%s); evaluating each check on its own",
            short_error(exc),
        )

    total = None
    try:
        total = data.select(pl.len()).collect().item()
    except Exception as exc:  # noqa: BLE001
        errors[-1] = short_error(exc)
    counts = {}
    for i in indexes:
        try:
            counts[i] = data.select(conditions[i].sum()).collect().item()
        except Exception as exc:  # noqa: BLE001
            errors[i] = short_error(exc)
    return total, counts


def _duplicated_rows(data: pl.LazyFrame, keys: list[str]) -> int:
    """Lignes dont la clé (non nulle) apparaît plus d'une fois."""
    group_rows = "__quality_group_rows"
    return (
        data.filter(pl.all_horizontal(pl.col(k).is_not_null() for k in keys))
        .group_by(keys)
        .agg(pl.len().alias(group_rows))
        .filter(pl.col(group_rows) > 1)
        .select(pl.col(group_rows).sum())
        .collect()
        .item()
    )
