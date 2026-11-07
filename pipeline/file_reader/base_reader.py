import os
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

from utils.path_utils import normalize_spark_path

from .reader_factory import ReaderFactory


class BaseFileReader:
    def __init__(self, spark: SparkSession, config_path: Path = None):
        self.spark = spark
        if config_path is not None:
            self.config_path = config_path
        else:
            self.base_path = Path(__file__).resolve().parent.parent.parent
            self.config_path = self.base_path / "config"

    def get_config_file(self, file_name: str):
        file_basename = os.path.basename(file_name).lower()
        file_extension = file_name.split(".")[-1].lower()
        # Search for a specific config file
        for config_file in os.listdir(self.config_path):
            if config_file.endswith(".yaml") and config_file.split("_")[0] in file_basename:
                with open(self.config_path / config_file) as f:
                    return yaml.safe_load(f)

        # Fallback on basic_reading_config.yaml
        default_config_path = self.config_path / "basic_reading_config.yaml"
        with open(default_config_path) as f:
            all_defaults = yaml.safe_load(f)
        # Search for specific section to the extension
        if file_extension in all_defaults:
            return all_defaults[file_extension]

        # Sinon fallback sur le format par défaut (ex: 'csv')
        default_format = all_defaults.get("default_format")
        if default_format and default_format in all_defaults:
            return all_defaults[default_format]

        raise ValueError(f"Aucune configuration valide trouvée pour le fichier : {file_name}")

    def read_file(self, file_path: str):
        file_path = normalize_spark_path(file_path)
        file_config = self.get_config_file(file_path)
        # Les configs dédiées (config/<source>_config.yaml) ont la forme
        # source.format, tandis que le fallback basic_reading_config.yaml a
        # un "format" à plat (voir CLAUDE.md, B1) : on tolère les deux.
        extension_file = (file_config.get("source") or {}).get("format") or file_config.get(
            "format"
        )
        # ReaderFactory.get_reader lève déjà une erreur explicite si
        # extension_file est None/non supporté (pas besoin de dupliquer la
        # validation ici).
        reader_instance = ReaderFactory.get_reader(file_config, extension_file, self.spark)
        return reader_instance.read(file_path, file_config)
