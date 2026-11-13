import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from pipeline.config import DEFAULT_CONFIG_ROOT, SourceConfig, SourceRegistry
from utils.path_utils import normalize_spark_path

from .reader_factory import ReaderFactory

log = logging.getLogger(__name__)


class BaseFileReader:
    """Routeur : trouve la config de lecture d'un fichier et délègue au reader
    de son `source.format`.

    Les configs viennent soit d'un `registry` déjà chargé (le job Spark le
    charge avant de démarrer Spark, C-2), soit de `config_path`
    (`<config_path>/sources`, `<config_path>/defaults`, par défaut
    `DEFAULT_CONFIG_ROOT`), chargé et validé ici une fois : une config
    invalide fait alors échouer la construction, avant la lecture du moindre
    fichier.
    """

    def __init__(
        self,
        spark: SparkSession,
        config_path: Path | None = None,
        registry: SourceRegistry | None = None,
    ):
        if registry is not None and config_path is not None:
            raise ValueError("passer config_path ou registry, pas les deux")
        self.spark = spark
        if registry is not None:
            self.config_path = None
            self.registry = registry
        else:
            self.config_path = Path(config_path) if config_path is not None else DEFAULT_CONFIG_ROOT
            self.registry = SourceRegistry.from_directory(self.config_path)

    def get_config(self, file_path: str) -> SourceConfig:
        """Config de lecture du fichier (voir `SourceRegistry.match`).

        Raises:
            NoMatchingConfigError / AmbiguousConfigError (des `ConfigError`).
        """
        return self.registry.match(file_path)

    def read_file(self, file_path: str, config: SourceConfig | None = None) -> DataFrame:
        """Lit `file_path` avec `config`, ou avec sa config résolue ici si
        `config` est absent. Le job Spark résout la config une seule fois
        et la passe : la même sert à la lecture et à l'écriture (P2-2 / C-8)."""
        if config is None:
            config = self.get_config(file_path)
        log.info("%s: source %r (format %s)", file_path, config.source.name, config.source.format)
        reader_instance = ReaderFactory.get_reader(config.source.format, self.spark)
        return reader_instance.read(normalize_spark_path(file_path), config)
