"""Lecture d'un fichier par le moteur Polars (P5-7), en API lazy.

`reader.options` sont les paramètres de `pl.scan_csv`, `pl.scan_parquet` ou
`pl.read_excel` (liste blanche de `pipeline/config/reader_options.py`), passés
tels quels : une option absente prend le défaut de Polars (ex. CSV
`has_header=True`, `raise_if_empty=True`).

Avec `columns_type` (csv/txt), le fichier est lu en texte puis converti par
`from_text`, qui reproduit les règles de Spark (voir `types.py`). L'en-tête
est toujours comparé aux colonnes déclarées (nombre, et noms sans tenir compte
de la casse quand `has_header`), comme Spark avec `enforceSchema: false` : un
fichier d'une autre forme échoue au lieu d'être lu décalé. Les options
d'inférence (`infer_schema`, `try_parse_dates`...) sont alors sans effet,
comme `inferSchema` chez Spark avec un schéma.

Écarts connus avec Spark, options par défaut : une ligne vide donne une ligne
de nulls (Spark l'ignore ; le staging la retire) ; une ligne trop longue fait
échouer le fichier (Spark tronque) ; un fichier vide aussi, sauf
`raise_if_empty: false` (Spark lit 0 ligne).
"""

import polars as pl

from pipeline.config import ConfigError, SourceConfig
from pipeline.engine.polars.types import from_text, polars_type


def read_file(path: str, config: SourceConfig) -> pl.LazyFrame:
    if config.engine != "polars":
        raise ConfigError(
            f"{path}: la source {config.source.name!r} déclare le moteur {config.engine!r}, "
            "pas polars"
        )
    options = dict(config.reader.options)
    file_format = config.source.format
    if file_format in ("csv", "txt"):
        return _read_csv(path, options, config.reader.columns_type)
    if file_format == "parquet":
        return pl.scan_parquet(path, **options)
    if file_format == "xlsx":
        return pl.read_excel(path, **options).lazy()
    raise ConfigError(f"{path}: format {file_format!r} non lu par le moteur polars")


def _read_csv(path: str, options: dict, columns_type: dict[str, str] | None) -> pl.LazyFrame:
    if columns_type is None:
        return pl.scan_csv(path, **options)
    text = pl.scan_csv(path, **{**options, "infer_schema": False})
    found = text.collect_schema().names()  # lit l'en-tête seulement
    if not found:  # fichier vide, avec `raise_if_empty: false`
        return pl.LazyFrame(schema={name: polars_type(t) for name, t in columns_type.items()})
    declared = list(columns_type)
    names_differ = options.get("has_header", True) and [n.casefold() for n in found] != [
        n.casefold() for n in declared
    ]
    if len(found) != len(declared) or names_differ:
        raise ValueError(
            f"{path}: colonnes du fichier {found} différentes de reader.columns_type {declared}"
        )
    return text.select(
        from_text(pl.col(src), col_type).alias(name)
        for src, (name, col_type) in zip(found, columns_type.items(), strict=True)
    )
