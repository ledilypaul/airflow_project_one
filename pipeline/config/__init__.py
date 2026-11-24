"""Configs de lecture validées (P2-1). Voir CLAUDE.md, « Config-driven file reading »."""

from pipeline.config.models import FileFormat, SourceConfig
from pipeline.config.reader_options import READER_OPTIONS_BY_FORMAT
from pipeline.config.registry import (
    DEFAULT_CONFIG_ROOT,
    AmbiguousConfigError,
    ConfigError,
    NoMatchingConfigError,
    SourceRegistry,
    default_config_root,
    default_directories,
    load_source_config,
)

__all__ = [
    "DEFAULT_CONFIG_ROOT",
    "READER_OPTIONS_BY_FORMAT",
    "AmbiguousConfigError",
    "ConfigError",
    "FileFormat",
    "NoMatchingConfigError",
    "SourceConfig",
    "SourceRegistry",
    "default_config_root",
    "default_directories",
    "load_source_config",
]
