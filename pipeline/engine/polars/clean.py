"""Nettoyage de la zone staging par le moteur Polars (P5-7), comme Spark
(`BaseProcessor.remove_empty_rows().remove_duplicates()`) :

- une ligne est vide quand chaque colonne est null, ou NaN pour une colonne
  flottante (`na.drop(how="all")` de Spark compte NaN comme null) ;
- les doublons exacts sont retirés ; NaN est égal à NaN et `-0.0` à `0.0`,
  comme dans `dropDuplicates`. Le premier exemplaire est gardé, l'ordre aussi.
"""

import polars as pl


def clean_for_staging(frame: pl.LazyFrame) -> pl.LazyFrame:
    schema = frame.collect_schema()
    if not schema:
        return frame
    empty = [
        pl.col(name).is_null() | pl.col(name).is_nan()
        if dtype.is_float()
        else pl.col(name).is_null()
        for name, dtype in schema.items()
    ]
    return frame.filter(~pl.all_horizontal(empty)).unique(maintain_order=True)
