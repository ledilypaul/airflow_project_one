"""Chargement des configs de lecture et correspondance fichier -> config (P2-1).

Arborescence (sous `DEFAULT_CONFIG_ROOT`, résolu depuis l'emplacement du code,
pas depuis le répertoire courant) :

    config/sources/*.yaml   une config dédiée par source (la vôtre)
    config/defaults/*.yaml  les défauts par extension (motifs `*.csv`...) ;
                            sans ce dossier, ceux du moteur (`config/defaults`)

La racine se choisit par `PIPELINE_CONFIG_ROOT` (chemin absolu) ; les sources
d'exemple sont dans `examples/config`.

`pipeline.yaml` de la racine (sinon celui du moteur) donne `default_directories`,
les répertoires listés par défaut (P5-3). Aucun autre fichier n'est scanné.

Correspondance, sur le nom du fichier (sans les répertoires), insensible à la
casse :
1. les sources dédiées dont un motif correspond ; si deux sources ou plus
   correspondent, c'est une erreur qui les nomme ;
2. sinon les défauts, avec la même règle ;
3. sinon une erreur explicite (le job écrit alors `failed` pour ce fichier).

Au chargement, les tables du lake de toutes les configs (raw, staging et
résultats qualité `quality/<table>`, sources et défauts) sont comparées :
aucune ne peut être imbriquée dans une autre (P2-2 / C-3), casse ignorée.
Deux sources peuvent partager exactement la même table : leurs partitions
(run, fichier) sont distinctes.
"""

import os
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path

import yaml
from pydantic import ValidationError

from pipeline.config.models import SourceConfig, lake_target_key, lake_targets_nested

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
# Défauts par extension livrés avec le moteur : utilisés quand la racine de
# config choisie n'a pas de dossier `defaults/` (ex. `examples/config`).
ENGINE_DEFAULTS_DIR = _PROJECT_ROOT / "config" / "defaults"
CONFIG_ROOT_ENV = "PIPELINE_CONFIG_ROOT"


def _default_config_root() -> Path:
    """`$PIPELINE_CONFIG_ROOT` (absolu requis) sinon `<projet>/config`."""
    value = os.environ.get(CONFIG_ROOT_ENV, "").strip()
    if not value:
        return _PROJECT_ROOT / "config"
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ConfigError(f"{CONFIG_ROOT_ENV} doit être un chemin absolu : {value!r}")
    return path


def default_config_root() -> Path:
    """Racine de config selon l'environnement **actuel** (`DEFAULT_CONFIG_ROOT`
    est figée à l'import) : ce que lit un DAG à chaque parsing (P5-3)."""
    return _default_config_root()


SOURCES_DIR = "sources"
DEFAULTS_DIR = "defaults"
# Réglages communs du listing (P5-3) ; sans fichier dans la racine choisie, celui du moteur.
PIPELINE_FILE = "pipeline.yaml"
ENGINE_PIPELINE_FILE = _PROJECT_ROOT / "config" / PIPELINE_FILE


class ConfigError(ValueError):
    """Config de lecture invalide, introuvable ou ambiguë."""


class NoMatchingConfigError(ConfigError):
    """Aucune config ne correspond au fichier."""


class AmbiguousConfigError(ConfigError):
    """Plusieurs configs du même niveau correspondent au fichier."""


DEFAULT_CONFIG_ROOT = _default_config_root()


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
        lines = _format_validation_error(exc)
        # C-7 : une erreur de champ empêche les contrôles croisés de Pydantic ;
        # ceux des sections valides sont ajoutés pour ne rien masquer.
        invalid_sections = {str(err["loc"][0]) for err in exc.errors() if err["loc"]}
        if invalid_sections:
            lines += SourceConfig.problems_of_valid_sections(raw, invalid_sections)
        details = "\n".join(f"  - {line}" for line in lines)
        raise ConfigError(f"config de lecture invalide : {path}\n{details}") from exc


def default_directories(config_root: Path) -> list[str]:
    """`default_directories` de `<config_root>/pipeline.yaml` (sinon du fichier
    du moteur) : les répertoires que listent les sources sans `listing.directories`
    et le DAG `ingest__unmatched`.

    Raises:
        ConfigError: fichier illisible ou clé absente ou invalide.
    """
    path = Path(config_root) / PIPELINE_FILE
    if not path.is_file():
        path = ENGINE_PIPELINE_FILE
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"{path} illisible\n  - {exc}") from exc
    directories = raw.get("default_directories") if isinstance(raw, dict) else None
    if (
        not isinstance(directories, list)
        or not directories
        or not all(isinstance(d, str) and d.strip() for d in directories)
    ):
        raise ConfigError(
            f"{path} : `default_directories` doit être une liste non vide de répertoires"
        )
    return [d.strip() for d in directories]


def _yaml_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.iterdir() if p.is_file() and p.suffix in {".yaml", ".yml"})


def _lake_target_conflicts(configs: list[LoadedConfig]) -> list[str]:
    """Tables du lake incompatibles entre configs différentes (C-3).

    - imbriquées (`raw/<table>` et `raw/<table>/x/b`) : Spark trouverait deux
      structures de partitions sous la table englobante et refuserait de la
      lire (`CONFLICTING_DIRECTORY_STRUCTURES`) ;
    - identiques à la casse près (`raw/<table>`, `raw/<Table>`) : un seul
      répertoire sur un FS insensible à la casse, deux ailleurs.
    Une même table écrite à l'identique par deux sources reste permise. Le
    cas raw/staging d'une même config est contrôlé par `SourceConfig`.
    """
    targets = [
        (target, zone, loaded)
        for loaded in configs
        for zone, target in (
            ("raw", loaded.config.raw_target),
            ("staging", loaded.config.staging_target),
            ("quality", loaded.config.quality_target),
        )
        if target is not None
    ]
    problems: list[str] = []
    for i, (first, first_zone, first_loaded) in enumerate(targets):
        for second, second_zone, second_loaded in targets[i + 1 :]:
            if first_loaded is second_loaded:
                continue
            where = (
                f"{first_zone} de {first_loaded.name!r} ({first_loaded.path}), "
                f"{second_zone} de {second_loaded.name!r} ({second_loaded.path})"
            )
            if lake_target_key(first) == lake_target_key(second):
                if first != second:
                    problems.append(
                        f"tables {first!r} et {second!r} identiques à la casse près ({where}) : "
                        "un seul répertoire sur un FS insensible à la casse, deux ailleurs. "
                        "Pour partager une table, l'écrire à l'identique."
                    )
                continue
            nested = lake_targets_nested(first, second)
            if nested is not None:
                outer, inner = nested
                problems.append(
                    f"tables imbriquées : {inner!r} dans {outer!r} ({where}, casse ignorée) : "
                    f"Spark ne pourrait plus lire {outer!r} (CONFLICTING_DIRECTORY_STRUCTURES). "
                    "Choisir des répertoires disjoints."
                )
    return problems


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
        de source en double et les tables du lake imbriquées) sont levées
        ensemble dans une `ConfigError`.
        """
        tiers, file_errors = cls._scan(Path(config_root))
        errors = list(file_errors.values())

        seen: dict[str, Path] = {}
        for loaded in tiers[SOURCES_DIR] + tiers[DEFAULTS_DIR]:
            if loaded.name in seen:
                errors.append(
                    f"source.name {loaded.name!r} déclaré deux fois : "
                    f"{seen[loaded.name]} et {loaded.path}"
                )
            else:
                seen[loaded.name] = loaded.path

        errors.extend(_lake_target_conflicts(tiers[SOURCES_DIR] + tiers[DEFAULTS_DIR]))

        if errors:
            raise ConfigError("\n".join(errors))
        if not seen:
            raise ConfigError(
                f"aucune config de lecture dans {config_root / SOURCES_DIR} "
                f"ni dans {config_root / DEFAULTS_DIR}"
            )
        return cls(tiers[SOURCES_DIR], tiers[DEFAULTS_DIR])

    @staticmethod
    def _scan(config_root: Path) -> tuple[dict[str, list[LoadedConfig]], dict[Path, str]]:
        """Charge chaque YAML de `<config_root>/sources` et `defaults` ; les
        fichiers invalides sont rendus à part (chemin -> message)."""
        if not config_root.is_dir():
            raise ConfigError(f"dossier de configs de lecture introuvable : {config_root}")
        errors: dict[Path, str] = {}
        tiers: dict[str, list[LoadedConfig]] = {SOURCES_DIR: [], DEFAULTS_DIR: []}
        for tier, loaded in tiers.items():
            directory = config_root / tier
            if tier == DEFAULTS_DIR and not directory.is_dir():
                directory = ENGINE_DEFAULTS_DIR
            for path in _yaml_files(directory):
                try:
                    loaded.append(LoadedConfig(load_source_config(path), path))
                except ConfigError as exc:
                    errors[path] = str(exc)
        return tiers, errors

    @classmethod
    def load_valid(cls, config_root: Path) -> tuple["SourceRegistry", dict[Path, str]]:
        """Le registre des configs valides, et le message de chacune qui ne l'est pas
        (P5-3) : une config invalide n'empêche pas les autres de se charger.

        Sont invalides, en plus des erreurs de fichier, une config dont le nom est
        déjà pris et une dont les tables du lake sont imbriquées dans celles d'une
        config déjà retenue (l'ordre est celui des fichiers : le premier gagne).

        Raises:
            ConfigError: dossier de racine introuvable.
        """
        tiers, invalid = cls._scan(Path(config_root))
        everyone = tiers[SOURCES_DIR] + tiers[DEFAULTS_DIR]
        names: dict[str, Path] = {}
        kept = []
        for loaded in everyone:
            if loaded.name in names:
                invalid[loaded.path] = (
                    f"source.name {loaded.name!r} déclaré deux fois : "
                    f"{names[loaded.name]} et {loaded.path}"
                )
            else:
                names[loaded.name] = loaded.path
                kept.append(loaded)
        if _lake_target_conflicts(kept):  # chemin lent, seulement en cas d'erreur
            accepted: list[LoadedConfig] = []
            for loaded in kept:
                problems = _lake_target_conflicts([*accepted, loaded])
                if problems:
                    invalid[loaded.path] = "\n".join(problems)
                else:
                    accepted.append(loaded)
            kept = accepted
        ok = {id(loaded) for loaded in kept}
        registry = cls(
            [c for c in tiers[SOURCES_DIR] if id(c) in ok],
            [c for c in tiers[DEFAULTS_DIR] if id(c) in ok],
        )
        return registry, invalid

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
