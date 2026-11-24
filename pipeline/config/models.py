"""Modèle unique (Pydantic) des configs de lecture : `config/sources/*.yaml`
et `config/defaults/*.yaml` (P2-1, constat M4).

    source:   name, format, patterns,
              engine                   -> quelle source, quels fichiers, quel moteur
    reader:   options, columns_type    -> comment la lire
    storage:  table_name, format, mode,
              raw_path, staging_path   -> où l'écrire (P2-2, obligatoire)
    quality:  checks                   -> ce que le staging exige (P2-3, optionnel,
                                          voir pipeline/config/quality.py)

`extra="forbid"` partout : une clé inconnue ou mal orthographiée est une
erreur, jamais ignorée. Les contrôles qui croisent les sections (storage
présent, nom de table, tables raw/staging disjointes, zone `quality/`
réservée, options du reader selon le format, contrôles qualité) sont faits
par un seul validateur, qui agrège leurs
messages : une erreur n'en masque pas une autre.

Limite de Pydantic : ce validateur `after` ne tourne que si toutes les
sections ont la bonne forme. Quand l'une d'elles a une erreur de champ (par
exemple un contrôle `quality` sans `column`), `load_source_config` complète
le message avec `SourceConfig.problems_of_valid_sections` : les contrôles
croisés des sections qui, elles, sont valides (P2-3 / C-7). Un appel direct à
`SourceConfig.model_validate` ne donne que les erreurs de champ.

Ce module ne doit pas importer Airflow : le job Spark le charge
(tests/unit/jobs/test_spark_job_no_airflow_import.py). Ni PySpark (P5-2) :
une config se charge sans moteur installé (tests/unit/test_p5_2_config_is_engine_neutral.py).
"""

import difflib
import re
from collections.abc import Collection, Mapping
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

from pipeline.config.engine import Engine, default_engine
from pipeline.config.quality import QUALITY_ZONE, QualitySection, quality_problems
from pipeline.config.reader_options import (
    ALIAS_GROUPS,
    POLARS_OPTION_TYPES,
    POLARS_OPTION_VALUES,
    POLARS_SCALAR_FORMATS,
    READER_OPTIONS_BY_ENGINE,
    READER_OPTIONS_BY_FORMAT,
    SCHEMA_FORMATS,
    SPARK_FORMATS,
    SPARK_OPTION_TYPES,
    SPARK_TO_POLARS_OPTIONS,
)
from pipeline.config.types import resolve_column_type
from utils.path_utils import check_lake_name, lake_relative_parts

# Aligné sur ReaderFactory (vérifié par test_p2_1_whitelists_cover_exactly_the_supported_formats).
# Pas de `xls` : read_excel exige alors xlrd, qui n'est pas installé (C-6).
FileFormat = Literal["csv", "txt", "parquet", "xlsx"]

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


def _check_column_type(col_type: str) -> str:
    resolve_column_type(col_type)  # ValueError avec le motif (type inconnu ou complexe)
    return col_type


_TYPE_LABELS: dict[type, str] = {
    bool: "un booléen (true ou false, sans guillemets)",
    int: "un entier (sans guillemets)",
    float: "un nombre (sans guillemets)",
}


def _has_type(value: Any, expected: type) -> bool:
    if expected is bool:
        return isinstance(value, bool)
    # bool est une sous-classe d'int : `maxColumns: true` est refusé.
    if isinstance(value, bool):
        return False
    if expected is float:
        return isinstance(value, int | float)
    return isinstance(value, expected)


def _check_pattern(pattern: str) -> str:
    if "/" in pattern or "\\" in pattern:
        raise ValueError(
            f"motif {pattern!r} : il s'applique au nom du fichier, pas à son chemin "
            "(retirer les répertoires)"
        )
    return pattern


# P5-3 : `source.name` devient le `dag_id` (`ingest__<name>`) et le nom d'Asset
# (`lake_staging__<name>`) : un identifiant, jamais du texte libre.
SOURCE_NAME_RE = re.compile(r"[a-z0-9_]+")
SOURCE_NAME_MAX_LENGTH = 100
# `ingest__unmatched` est le DAG des fichiers sans source (défauts par extension).
UNMATCHED_SOURCE = "unmatched"
SCHEDULE_PRESETS = frozenset(
    {"@once", "@hourly", "@daily", "@weekly", "@monthly", "@quarterly", "@yearly"}
)
# Cinq champs cron ; la syntaxe fine est vérifiée par Airflow quand le DAG se
# construit (un planning refusé donne un DAG `invalid_source__<nom>`).
_CRON_RE = re.compile(r"[0-9A-Za-z*/,\-?#]+( +[0-9A-Za-z*/,\-?#]+){4}")


def _check_source_name(name: str) -> str:
    if len(name) > SOURCE_NAME_MAX_LENGTH or not SOURCE_NAME_RE.fullmatch(name):
        raise ValueError(
            f"nom {name!r} invalide : lettres minuscules, chiffres et `_` seulement, "
            f"{SOURCE_NAME_MAX_LENGTH} caractères au plus (il sert d'identifiant de DAG et d'Asset)"
        )
    if name == UNMATCHED_SOURCE:
        raise ValueError(
            f"nom {name!r} réservé : `ingest__{UNMATCHED_SOURCE}` est le DAG des fichiers "
            "qui ne correspondent à aucune source"
        )
    return name


def _check_schedule(value: str) -> str:
    if value not in SCHEDULE_PRESETS and not _CRON_RE.fullmatch(value):
        raise ValueError(
            f"planning {value!r} : attendu une expression cron à cinq champs "
            f"(`0 6 * * *`), l'un de {', '.join(sorted(SCHEDULE_PRESETS))}, ou null (manuel)"
        )
    return value


ColumnType = Annotated[str, AfterValidator(_check_column_type)]
Pattern = Annotated[NonEmptyStr, AfterValidator(_check_pattern)]
SourceName = Annotated[NonEmptyStr, AfterValidator(_check_source_name)]
Schedule = Annotated[NonEmptyStr, AfterValidator(_check_schedule)]
Tag = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceSection(_StrictModel):
    name: SourceName = Field(
        description="Identifiant unique de la source (`[a-z0-9_]+`) : DAG `ingest__<name>`."
    )
    format: FileFormat = Field(description="Choisit le reader dans ReaderFactory.")
    patterns: list[Pattern] = Field(
        min_length=1,
        description="Motifs fnmatch sur le NOM du fichier, insensibles à la casse.",
    )
    engine: Engine | None = Field(
        default=None,
        description=(
            "Moteur de calcul de la source ; `PIPELINE_DEFAULT_ENGINE` (défaut spark) s'il est "
            "absent. Il décide des options de `reader.options` et de la syntaxe des regex."
        ),
    )


class ReaderSection(_StrictModel):
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Options passées telles quelles au reader (liste blanche par format).",
    )
    columns_type: dict[str, ColumnType] | None = Field(
        default=None,
        description="Schéma explicite {colonne: type}, dans l'ordre du fichier (csv/txt).",
    )


def _check_zone_path(value: str) -> str:
    lake_relative_parts(value)  # ValueError : hors de LAKE_ROOT, segment invalide
    return value


ZonePath = Annotated[str, AfterValidator(_check_zone_path)]
TableName = Annotated[str, AfterValidator(check_lake_name)]


def lake_target_key(target: str) -> tuple[str, ...]:
    """Segments d'une table du lake (`raw/<table>`), sans tenir compte de la
    casse : deux tables qui ne diffèrent que par la casse sont un seul
    répertoire sur un FS insensible à la casse (macOS, Windows)."""
    return tuple(segment.casefold() for segment in target.split("/"))


def lake_targets_nested(first: str, second: str) -> tuple[str, str] | None:
    """(englobante, imbriquée) si l'une des deux tables est un préfixe de
    segments strict de l'autre (`raw/<table>` et `raw/<table>/x`), casse
    ignorée ; sinon None. `raw/<table>` et `raw/<table>_v2` ne sont pas
    imbriquées."""
    a, b = lake_target_key(first), lake_target_key(second)
    if len(a) == len(b):
        return None
    (outer, outer_key), (inner, inner_key) = sorted(
        ((first, a), (second, b)), key=lambda item: len(item[1])
    )
    return (outer, inner) if inner_key[: len(outer_key)] == outer_key else None


class StorageSection(_StrictModel):
    """Où le job Spark écrit les fichiers de la source (P2-2).

    Chemins relatifs à `LAKE_ROOT` (variable d'environnement du job) :
    `<LAKE_ROOT>/<raw_path>/<table>/` et, si `staging_path` est déclaré,
    `<LAKE_ROOT>/<staging_path>/<table>/`. Aucun ne peut sortir de
    `LAKE_ROOT` (pas de `..`, de chemin absolu ni de schéma d'URI), et les
    tables raw et staging ne peuvent être ni la même ni imbriquées l'une dans
    l'autre (contrôlé par `SourceConfig`, entre sources par `SourceRegistry`).

    Chaque table contient un instantané par listing, partitionné par
    (`lake_run_id`, `lake_source_file`) : voir storage/parquet_lake_writer.py.
    """

    table_name: TableName | None = Field(
        default=None, description="Nom de la table ; `source.name` s'il est absent."
    )
    format: Literal["parquet"]
    mode: Literal["overwrite"]
    raw_path: ZonePath = Field(description="Zone raw : chaque fichier lu, tel quel.")
    staging_path: ZonePath | None = Field(
        default=None,
        description=(
            "Zone staging (lignes vides et doublons exacts retirés) ; absent = pas de staging."
        ),
    )

    @field_validator("mode", mode="before")
    @classmethod
    def _only_overwrite(cls, value: Any) -> Any:
        if value != "overwrite":
            raise ValueError(
                f"mode {value!r} interdit : seul 'overwrite' est accepté (idempotence : "
                "un retry ou un re-run ne doit pas dupliquer les données)"
            )
        return value


class ListingSection(_StrictModel):
    """Où chercher les fichiers de la source (P5-3). Ceux qui comptent sont
    ceux dont le nom correspond à `source.patterns`, moins `exclude`."""

    directories: list[NonEmptyStr] | None = Field(
        default=None,
        min_length=1,
        description=(
            "Répertoires listés (relatifs au répertoire de lancement du worker, ou absolus) ; "
            "`default_directories` de `pipeline.yaml` s'il est absent."
        ),
    )
    exclude: list[Pattern] = Field(
        default_factory=list,
        description="Motifs fnmatch sur le NOM du fichier (casse ignorée), retirés du listing.",
    )


class SourceConfig(_StrictModel):
    source: SourceSection
    reader: ReaderSection = Field(default_factory=ReaderSection)
    # Obligatoire (P2-2) : tout fichier lu est au moins écrit en zone raw,
    # rien n'est lu puis perdu en silence. Déclaré optionnel pour Pydantic
    # seulement (C-6) : un champ requis absent empêche les validateurs
    # `after` de tourner, et masquait les erreurs du reader du même fichier.
    # `_check_consistency` le rend obligatoire, avec les autres contrôles.
    storage: StorageSection | None = None
    # P2-3 : contrôles exécutés sur les données du staging ; exige staging_path.
    quality: QualitySection | None = None
    # P5-3 : le DAG `ingest__<source.name>` généré. Sans effet pour la lecture.
    listing: ListingSection = Field(default_factory=ListingSection)
    schedule: Schedule | None = Field(
        default=None, description="Cron, preset Airflow (`@daily`) ou null : déclenchement manuel."
    )
    retries: int | None = Field(
        default=None, ge=0, le=10, description="Reprises des tâches du DAG ; 1 s'il est absent."
    )
    tags: list[Tag] = Field(default_factory=list, description="Étiquettes du DAG dans l'UI.")

    def _require_storage(self) -> StorageSection:
        if self.storage is None:  # seulement via model_construct, sans validation
            raise ValueError("storage absent : la config n'a pas été validée")
        return self.storage

    @property
    def engine(self) -> Engine:
        """Moteur de la source : `source.engine`, sinon `PIPELINE_DEFAULT_ENGINE`
        (lue à chaque appel), sinon `spark`.

        Raises:
            ValueError: variable d'environnement invalide.
        """
        return self.source.engine or default_engine()

    @property
    def table(self) -> str:
        """Nom de la table dans le lake : `storage.table_name` ou `source.name`."""
        return self._require_storage().table_name or self.source.name

    @property
    def staging_asset_name(self) -> str | None:
        """Asset émis quand la source a écrit sa zone staging
        (`lake_staging__<name>`, P5-3) ; None sans `staging_path`."""
        if self.storage is None or self.storage.staging_path is None:
            return None
        return f"lake_staging__{self.source.name}"

    @property
    def raw_target(self) -> str:
        """Table raw, relative à LAKE_ROOT (ex. `raw/<table>`)."""
        storage = self._require_storage()
        return "/".join((*lake_relative_parts(storage.raw_path), self.table))

    @property
    def staging_target(self) -> str | None:
        """Table staging, relative à LAKE_ROOT ; None sans `staging_path`."""
        storage = self._require_storage()
        if storage.staging_path is None:
            return None
        return "/".join((*lake_relative_parts(storage.staging_path), self.table))

    @property
    def quality_target(self) -> str | None:
        """Table des résultats des contrôles (`quality/<table>`), relative à
        LAKE_ROOT ; None sans section `quality`."""
        if self.quality is None:
            return None
        return self.quality_zone_target

    @property
    def quality_zone_target(self) -> str | None:
        """`quality/<table>` dès que la source a une zone staging, section
        `quality` ou non (même règle de nommage que `quality_target`) ; None
        sans staging. Le job y supprime la partition du (run, fichier) avant
        chaque écriture (C-6) : une source qui a perdu sa section `quality`
        ne garde pas d'anciens résultats."""
        if self.staging_target is None:
            return None
        return f"{QUALITY_ZONE}/{self.table}"

    @classmethod
    def problems_of_valid_sections(
        cls, raw: Mapping[str, Any], invalid_sections: Collection[str]
    ) -> list[str]:
        """Contrôles croisés des sections valides d'une config dont d'autres
        sections ont une erreur de champ (C-7).

        Pydantic n'exécute pas `_check_consistency` dans ce cas : sans ceci,
        une erreur de forme dans `quality` masquerait une option inconnue du
        reader ou une section `storage` absente. Chaque contrôle croisé ne
        tourne que si toutes les sections dont il dépend sont valides ;
        `invalid_sections` vient des `loc` de la `ValidationError`.
        """
        sections: dict[str, Any] = {}
        for name, model in _SECTION_MODELS.items():
            if name in invalid_sections:
                continue
            if name not in raw or raw[name] is None:
                sections[name] = model() if name == "reader" else None
                continue
            try:
                sections[name] = model.model_validate(raw[name])
            except ValidationError:
                continue
        source, problems = sections.get("source"), []
        engine: Engine = "spark"
        if source is not None:
            try:
                engine = source.engine or default_engine()
            except ValueError as exc:
                return [f"source.engine: {exc}"]
            # model_construct : pas de validation ; seules les sections
            # valides sont lues par les méthodes appelées.
            partial = cls.model_construct(
                source=source,
                reader=sections.get("reader") or ReaderSection(),
                storage=sections.get("storage"),
            )
            if "storage" in sections:
                problems += partial._storage_problems()
            if "reader" in sections:
                problems += partial._reader_problems(engine)
        quality = sections.get("quality")
        if quality is not None and "storage" in sections:
            reader, storage = sections.get("reader"), sections["storage"]
            problems += quality_problems(
                quality,
                reader.columns_type if reader is not None else None,
                storage.staging_path if storage is not None else None,
                engine,
            )
        return problems

    @model_validator(mode="after")
    def _check_consistency(self) -> "SourceConfig":
        """Tous les contrôles qui croisent les sections, en un seul
        validateur : leurs messages sont agrégés dans une seule erreur (C-6)."""
        try:
            engine = self.engine
        except ValueError as exc:
            raise ValueError(f"source.engine: {exc}") from exc
        problems = self._storage_problems() + self._reader_problems(engine)
        if self.quality is not None:
            problems += quality_problems(
                self.quality,
                self.reader.columns_type,
                self.storage.staging_path if self.storage is not None else None,
                engine,
            )
        if problems:
            raise ValueError("\n".join(problems))
        return self

    def _storage_problems(self) -> list[str]:
        if self.storage is None:
            return [
                "storage: section obligatoire (P2-2) : tout fichier lu est écrit au moins "
                "en zone raw. Déclarer storage.format, storage.mode et storage.raw_path."
            ]
        reserved = [
            f"storage.{field} {value!r} : la zone {QUALITY_ZONE!r} (casse ignorée) est réservée "
            f"aux résultats des contrôles qualité (P2-3, {QUALITY_ZONE}/<table>/). "
            "Choisir un autre répertoire."
            for field, value in (
                ("raw_path", self.storage.raw_path),
                ("staging_path", self.storage.staging_path),
            )
            if value is not None and lake_relative_parts(value)[0].casefold() == QUALITY_ZONE
        ]
        if reserved:
            return reserved
        if self.storage.table_name is None:
            try:
                check_lake_name(self.source.name)
            except ValueError as exc:
                return [
                    f"storage.table_name absent : source.name sert de nom de table et "
                    f"doit alors être un nom sûr ({exc}). Déclarer storage.table_name."
                ]
        raw, staging = self.raw_target, self.staging_target
        if staging is None:
            return []
        if lake_target_key(raw) == lake_target_key(staging):
            return [
                f"storage.staging_path {self.storage.staging_path!r} : même table que raw "
                f"({raw!r}, casse ignorée), la zone staging écraserait les partitions raw"
            ]
        nested = lake_targets_nested(raw, staging)
        if nested is not None:
            outer, inner = nested
            return [
                f"storage.staging_path {self.storage.staging_path!r} : tables raw {raw!r} et "
                f"staging {staging!r} imbriquées ({inner!r} dans {outer!r}, casse ignorée) : "
                f"Spark ne pourrait plus lire {outer!r} (CONFLICTING_DIRECTORY_STRUCTURES). "
                "Choisir des répertoires disjoints."
            ]
        return []

    def _reader_problems(self, engine: Engine) -> list[str]:
        fmt = self.source.format
        allowed = READER_OPTIONS_BY_ENGINE[engine][fmt]
        spark = engine == "spark"
        scalar_formats = SPARK_FORMATS if spark else POLARS_SCALAR_FORMATS
        option_types = SPARK_OPTION_TYPES if spark else POLARS_OPTION_TYPES
        label = "Spark" if spark else "Polars"
        problems: list[str] = []

        for key, value in self.reader.options.items():
            if key not in allowed:
                problems.append(f"reader.options.{key}: {_unknown_option(key, fmt, engine)}")
            elif fmt in scalar_formats and not _is_scalar_option(value, spark):
                problems.append(
                    f"reader.options.{key}: une option {label} doit être une valeur scalaire "
                    f"(texte, nombre ou booléen{'' if spark else ', null ou liste de scalaires'}), "
                    f"pas {value!r}"
                )
            elif (
                fmt in scalar_formats
                and key in option_types
                and not (value is None and not spark)
                and not _has_type(value, option_types[key])
            ):
                problems.append(
                    f"reader.options.{key}: doit être "
                    f"{_TYPE_LABELS[option_types[key]]}, pas {value!r}"
                )
            elif (
                not spark
                and fmt in scalar_formats
                and key in POLARS_OPTION_VALUES
                and value not in POLARS_OPTION_VALUES[key]
            ):
                problems.append(
                    f"reader.options.{key}: Polars n'accepte que "
                    f"{', '.join(map(repr, POLARS_OPTION_VALUES[key]))}, pas {value!r}"
                )

        for group in ALIAS_GROUPS if spark else ():
            declared = sorted(group & self.reader.options.keys())
            if len(declared) > 1:
                problems.append(
                    f"reader.options: {' et '.join(declared)} sont deux noms de la même "
                    "option Spark, Spark n'en retiendrait qu'un : n'en déclarer qu'un"
                )

        if self.reader.columns_type and fmt not in SCHEMA_FORMATS:
            problems.append(
                f"reader.columns_type: n'est appliqué que pour les formats "
                f"{', '.join(sorted(SCHEMA_FORMATS))} (format {fmt!r})"
            )
        return problems


def _is_scalar_option(value: Any, spark: bool) -> bool:
    if isinstance(value, str | bool | int | float):
        return True
    # Polars : `null` (`infer_schema_length: null`) et listes (`null_values`).
    return not spark and (
        value is None
        or (isinstance(value, list) and all(isinstance(v, str | bool | int | float) for v in value))
    )


def _unknown_option(key: str, fmt: str, engine: Engine) -> str:
    """Pourquoi `key` n'est pas une option de lecture de (`engine`, `fmt`) :
    l'équivalent Polars d'une option Spark, ou la faute de frappe probable."""
    if engine == "polars" and key in READER_OPTIONS_BY_FORMAT[fmt]:
        equivalent = SPARK_TO_POLARS_OPTIONS.get(fmt, {}).get(key)
        advice = (
            f"utiliser {equivalent!r}"
            if equivalent
            else "pas d'équivalent Polars établi (voir docs/configuration/sources.md)"
        )
        return f"option du moteur Spark, la source utilise le moteur 'polars' : {advice}"
    allowed = sorted(READER_OPTIONS_BY_ENGINE[engine][fmt])
    hint = difflib.get_close_matches(key, allowed, n=1, cutoff=0.6)
    suggestion = f" (vouliez-vous dire {hint[0]!r} ?)" if hint else ""
    where = "" if engine == "spark" else f" (moteur {engine!r})"
    return f"option non supportée pour le format {fmt!r}{where}{suggestion}"


_SECTION_MODELS: dict[str, type[BaseModel]] = {
    "source": SourceSection,
    "reader": ReaderSection,
    "storage": StorageSection,
    "quality": QualitySection,
}
