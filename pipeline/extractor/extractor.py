"""Listing des fichiers d'une source (P5-3) : les noms qui correspondent à ses
`source.patterns`, moins `listing.exclude`, dans ses répertoires. Pur : pas de
base ni de réseau, jamais PySpark ni Airflow."""

from collections.abc import Callable, Iterable, Sequence
from fnmatch import fnmatchcase
from pathlib import Path


def _any_match(name: str, patterns: Iterable[str]) -> bool:
    return any(fnmatchcase(name, pattern.lower()) for pattern in patterns)


def _list(directories: Sequence[str], keep: Callable[[str], bool]) -> list[str]:
    """Fichiers de `directories` (non récursif) dont le nom en minuscules passe `keep`.

    Raises:
        FileNotFoundError: si au moins un répertoire n'existe pas. Tous les
            manquants sont signalés, avec leur forme configurée et résolue (un
            chemin relatif se résout depuis le répertoire courant). Un
            répertoire absent est un problème de déploiement et ne doit pas
            passer pour un répertoire vide, qui donne légitimement `[]` (et un
            skip en aval).
    """
    missing = [d for d in directories if not Path(d).exists()]
    if missing:
        details = "\n".join(f"  - {d!s} (resolved: {Path(d).resolve()!s})" for d in missing)
        raise FileNotFoundError(
            f"{len(missing)} configured source director"
            f"{'y does' if len(missing) == 1 else 'ies do'} not exist "
            f"(cwd: {Path.cwd()!s}):\n{details}"
        )
    return sorted(
        str(path)
        for directory in directories
        for path in Path(directory).iterdir()
        if path.is_file() and keep(path.name.lower())
    )


def list_source_files(
    directories: Sequence[str], patterns: Iterable[str], exclude: Iterable[str] = ()
) -> list[str]:
    """Les fichiers de la source : un motif de `patterns` correspond, aucun de
    `exclude` (fnmatch sur le nom, casse ignorée, comme `SourceRegistry.match`)."""
    patterns, exclude = list(patterns), list(exclude)
    return _list(
        directories, lambda name: _any_match(name, patterns) and not _any_match(name, exclude)
    )


def list_unmatched_files(
    directories: Sequence[str], source_patterns: Iterable[str], default_patterns: Iterable[str]
) -> list[str]:
    """Les fichiers qu'aucune source dédiée ne réclame (`source_patterns`) et
    qu'un défaut par extension (`default_patterns`) sait lire."""
    source_patterns, default_patterns = list(source_patterns), list(default_patterns)
    return _list(
        directories,
        lambda name: not _any_match(name, source_patterns) and _any_match(name, default_patterns),
    )
