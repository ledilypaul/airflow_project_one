"""Types neutres en Polars, et leur conversion depuis le texte d'un CSV (P5-7).

`from_text` reproduit la conversion de la source CSV de Spark 4.3 (mode
PERMISSIVE, ANSI désactivé, sans `dateFormat`/`timestampFormat`) : le même
`columns_type` donne les mêmes valeurs avec les deux moteurs, et une valeur
non convertible devient null, sans erreur. Règles relevées sur Spark 4.3.0 et
gardées par tests/unit/engine/test_p5_7_polars_reader.py :

- `string` : `""` (même entre guillemets) devient null ;
- `int`, `long` : entier décimal signé, sans espace autour ;
- `float`, `double` : espaces autour ignorés ; `NaN`, `Infinity`, `Inf`,
  `-Inf` ; suffixe `f`/`d` accepté ; `nan` et `inf` en minuscules refusés ;
- `boolean` : `true` / `false`, casse ignorée, sans espace ;
- `date` : `yyyy`, `yyyy-[m]m`, `yyyy-[m]m-[d]d`, suivi au besoin d'un espace
  ou d'un `T` et de n'importe quoi (l'heure est ignorée) ;
- `timestamp` : date, puis `[T ]h[h][:m[m][:s[s][.fraction]]]`, puis `Z` ou
  un décalage `±h[h][[:]mm]`. Fraction tronquée à la microseconde. Résultat
  en UTC, sans fuseau (`Datetime("us")`), comme le lit DuckDB : un horodatage
  sans décalage est pris en UTC (Spark le prend dans le fuseau de sa session).

Écarts connus (null en Polars, valeur en Spark) : nombres hexadécimaux, années
de plus de 4 chiffres ou négatives, fuseau nommé (`UTC`, `Europe/Paris`),
heure seule (Spark la daterait du jour courant).
"""

import polars as pl

from pipeline.config.types import resolve_column_type

_DECIMAL = r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?[fFdD]?$"
_DATE = r"^\+?(\d{4})(?:-(\d{1,2})(?:-(\d{1,2})(?:[T ].*)?)?)?$"
_TIMESTAMP = (
    r"^\+?(\d{4})(?:-(\d{1,2})(?:-(\d{1,2})"
    r"(?:[T ](\d{1,2})(?::(\d{1,2})(?::(\d{1,2})(?:\.(\d*))?)?)?)?)?)?"
    r" ?(Z|[+-]\d{1,2}(?::?\d{2})?)?$"
)


def polars_type(col_type: str) -> pl.DataType:
    """Le type Polars d'un type neutre de `columns_type`."""
    name = resolve_column_type(col_type).polars
    return pl.Datetime("us") if name == "Datetime" else getattr(pl, name)()


def from_text(text: pl.Expr, col_type: str) -> pl.Expr:
    """`text` (colonne `String` lue telle quelle) converti en `col_type`."""
    target = polars_type(col_type)
    if target == pl.String:
        return pl.when(text != "").then(text)
    if target == pl.Boolean:
        lower = text.str.to_lowercase()
        return pl.when(lower == "true").then(True).when(lower == "false").then(False)
    if target.is_integer():
        return text.cast(target, strict=False)
    if target.is_float():
        return _fractional(text, target)
    if target == pl.Date:
        return _date(text)
    return _timestamp(text)


def _fractional(text: pl.Expr, target: pl.DataType) -> pl.Expr:
    trimmed = text.str.strip_chars()
    number = trimmed.str.replace(r"[fFdD]$", "").cast(target, strict=False)
    return (
        # Les valeurs spéciales de Spark (`nanValue`, `positiveInf`,
        # `negativeInf`) sont comparées avant tout nettoyage.
        pl.when((text == "Inf") | trimmed.str.contains(r"^\+?Infinity$"))
        .then(pl.lit(float("inf"), target))
        .when((text == "-Inf") | (trimmed == "-Infinity"))
        .then(pl.lit(float("-inf"), target))
        .when(trimmed.str.contains(r"^[+-]?NaN$"))
        .then(pl.lit(float("nan"), target))
        .when(trimmed.str.contains(_DECIMAL))
        .then(number)
    )


def _date(text: pl.Expr) -> pl.Expr:
    parts = text.str.strip_chars().str.extract_groups(_DATE)
    year, month, day = (parts.struct.field(str(i)) for i in (1, 2, 3))
    return pl.format("{}-{}-{}", year, month.fill_null("1"), day.fill_null("1")).str.to_date(
        "%Y-%m-%d", strict=False
    )


def _timestamp(text: pl.Expr) -> pl.Expr:
    parts = text.str.strip_chars().str.extract_groups(_TIMESTAMP)
    year, month, day, hour, minute, second, fraction, zone = (
        parts.struct.field(str(i)) for i in range(1, 9)
    )

    def two(field: pl.Expr) -> pl.Expr:
        return field.fill_null("0").str.zfill(2)

    micros = fraction.fill_null("").str.slice(0, 6).str.pad_end(6, "0")
    local = pl.format(
        "{}-{}-{} {}:{}:{}.{}",
        year,
        month.fill_null("1").str.zfill(2),
        day.fill_null("1").str.zfill(2),
        two(hour),
        two(minute),
        two(second),
        micros,
    ).str.to_datetime("%Y-%m-%d %H:%M:%S%.f", time_unit="us", strict=False)
    return local - _offset(zone)


def _offset(zone: pl.Expr) -> pl.Expr:
    """Décalage `Z` / `±h[h][[:]mm]` en durée ; null s'il est hors bornes."""
    offset = zone.str.extract_groups(r"^([+-])(\d{1,2}):?(\d{2})?$")
    sign = pl.when(offset.struct.field("1") == "-").then(-1).otherwise(1)
    hours = offset.struct.field("2").cast(pl.Int64)
    minutes = offset.struct.field("3").fill_null("0").cast(pl.Int64)
    valid = (hours <= 18) & (minutes < 60)
    return (
        pl.when(zone.is_null() | (zone == "Z"))
        .then(pl.duration(minutes=0))
        .when(valid)
        .then(pl.duration(minutes=sign * (hours * 60 + minutes)))
    )
