"""Chargement des configs de lecture et correspondance fichier -> config (P2-1).

Arborescence (sous `DEFAULT_CONFIG_ROOT`, résolu depuis l'emplacement du code,
pas depuis le répertoire courant) :

    config/sources/*.yaml   une config dédiée par source (orders, imdb...)
    config/defaults/*.yaml  les défauts par extension (motifs `*.csv`...)

Rien d'autre n'est lu : `config/extractor_config.yaml` (la config du listing)
n'est jamais scanné.

Correspondance, sur le nom du fichier (sans les répertoires), insensible à la
casse :
1. les sources dédiées dont un motif correspond ; si deux sources ou plus
   correspondent, c'est une erreur qui les nomme ;
2. sinon les défauts, avec la même règle ;
3. sinon une erreur explicite (le job écrit alors `failed` pour ce fichier).
"""

import os
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path

import yaml
from pydantic import ValidationError

from pipeline.config.models import SourceConfig

DEFAULT_CONFIG_ROOT = Path(__file__).resolve().parent.parent.parent / "config"
SOURCES_DIR = "sources"
DEFAULTS_DIR = "defaults"


class ConfigError(ValueError):
    """Config de lecture invalide, introuvable ou ambiguë."""


class NoMatchingConfigError(ConfigError):
    """Aucune config ne correspond au fichier."""


class AmbiguousConfigError(ConfigError):
    """Plusieurs configs du même niveau correspondent au fichier."""


@dataclass(frozen=True)
class LoadedConfig:
    config: SourceConfig
    path: Path

    @property
    def name(self) -> str:
        return self.config.source.name

    def matches(self, file_name: str) -> bool:
        lowered = file_name.lower()
        return any(fnmatchcase(lowered, p.lower()) for p in self.config.source.patterns)


def _format_validation_error(error: ValidationError) -> list[str]:
    lines = []
    for err in error.errors():
        location = ".".join(str(part) for part in err["loc"])
        if err["type"] == "value_error":
            # Messages de nos validateurs : on retire le préfixe "Value error, ".
            message = str(err["ctx"]["error"])
        else:
            message = err["msg"]
        for line in message.splitlines():
            lines.append(f"{location}: {line}" if location else line)
    return lines


def load_source_config(path: Path) -> SourceConfig:
    """Charge et valide un YAML de config de lecture.

    Raises:
        ConfigError: YAML illisible ou invalide ; le message nomme le fichier
            et le chemin de chaque champ en faute.
    """
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"config de lecture illisible : {path}\n  - {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(
            f"config de lecture invalide : {path}\n"
            f"  - le contenu doit être un mapping (source/reader/storage), "
            f"pas {type(raw).__name__}"
        )
    try:
        return SourceConfig.model_validate(raw)
    except ValidationError as exc:
        details = "\n".join(f"  - {line}" for line in _format_validation_error(exc))
        raise ConfigError(f"config de lecture invalide : {path}\n{details}") from exc


def _yaml_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.iterdir() if p.is_file() and p.suffix in {".yaml", ".yml"})


class SourceRegistry:
    """Toutes les configs de lecture, chargées et validées une seule fois."""

    def __init__(self, sources: list[LoadedConfig], defaults: list[LoadedConfig]):
        self.sources = list(sources)
        self.defaults = list(defaults)

    @property
    def config_files(self) -> list[Path]:
        return [loaded.path for loaded in self.sources + self.defaults]

    @classmethod
    def from_directory(cls, config_root: Path = DEFAULT_CONFIG_ROOT) -> "SourceRegistry":
        """Charge `<config_root>/sources` et `<config_root>/defaults`.

        Échoue vite : toutes les erreurs (de tous les fichiers, plus les noms
        de source en double) sont levées ensemble dans une `ConfigError`.
        """
        config_root = Path(config_root)
        if not config_root.is_dir():
            raise ConfigError(f"dossier de configs de lecture introuvable : {config_root}")

        errors: list[str] = []
        tiers: dict[str, list[LoadedConfig]] = {SOURCES_DIR: [], DEFAULTS_DIR: []}
        for tier, loaded in tiers.items():
            for path in _yaml_files(config_root / tier):
                try:
                    loaded.append(LoadedConfig(load_source_config(path), path))
                except ConfigError as exc:
                    errors.append(str(exc))

        seen: dict[str, Path] = {}
        for loaded in tiers[SOURCES_DIR] + tiers[DEFAULTS_DIR]:
            if loaded.name in seen:
                errors.append(
                    f"source.name {loaded.name!r} déclaré deux fois : "
                    f"{seen[loaded.name]} et {loaded.path}"
                )
            else:
                seen[loaded.name] = loaded.path

        if errors:
            raise ConfigError("\n".join(errors))
        if not seen:
            raise ConfigError(
                f"aucune config de lecture dans {config_root / SOURCES_DIR} "
                f"ni dans {config_root / DEFAULTS_DIR}"
            )
        return cls(tiers[SOURCES_DIR], tiers[DEFAULTS_DIR])

    def match(self, file_path: str) -> SourceConfig:
        """Config de lecture du fichier `file_path` (chemin ou URI)."""
        file_name = os.path.basename(str(file_path).rstrip("/\\"))
        for tier_label, tier in (("sources dédiées", self.sources), ("défauts", self.defaults)):
            matching = [loaded for loaded in tier if loaded.matches(file_name)]
            if len(matching) > 1:
                candidates = ", ".join(f"{m.name!r} ({m.path})" for m in matching)
                raise AmbiguousConfigError(
                    f"{file_name!r} correspond à plusieurs {tier_label} : {candidates}. "
                    "Rendre leurs motifs exclusifs."
                )
            if matching:
                return matching[0].config
        raise NoMatchingConfigError(
            f"aucune config de lecture ne correspond à {file_name!r} "
            f"(sources dédiées : {', '.join(m.name for m in self.sources) or 'aucune'} ; "
            f"défauts : {', '.join(m.name for m in self.defaults) or 'aucun'})"
        )
