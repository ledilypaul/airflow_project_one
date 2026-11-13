"""Réexportation, pour la compatibilité : les types de `reader.columns_type`
vivent dans `pipeline/config/types.py` depuis P2-1 (C-7). Importer depuis
`pipeline.config.types` dans le nouveau code."""

from pipeline.config.types import SUPPORTED_COLUMN_TYPES, build_schema, get_spark_type

__all__ = ["SUPPORTED_COLUMN_TYPES", "build_schema", "get_spark_type"]
