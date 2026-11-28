"""Generate the dbt staging layer from the reading configs (P5-4).

For each source with `storage.staging_path` and `reader.columns_type`, writes in
`transform/models/staging/generated/` the `lake_staging` source table,
`stg_<table>__current.sql` and its contracted model. Reads `config/`,
`examples/config/` and `PIPELINE_CONFIG_ROOT` when set. `--check` writes
nothing and exits 1 when the committed files are out of date.
"""

import argparse
import re
import sys
from pathlib import Path

import yaml

from pipeline.config.registry import ConfigError, default_config_root, load_source_config
from pipeline.config.types import resolve_column_type

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "transform" / "models" / "staging" / "generated"
SOURCES_FILE = "_lake_staging__sources.yml"
MODELS_FILE = "_staging__models.yml"

# Added by ParquetLakeWriter, in the order DuckDB reads them back.
TECHNICAL_COLUMNS = (
    ("lake_file_id", "BIGINT"),
    ("lake_ingested_at", "TIMESTAMP"),
    ("lake_run_id", "VARCHAR"),
    ("lake_source_file", "VARCHAR"),
)
# A table name becomes a model name, a file name and a piece of SQL.
_TABLE_RE = re.compile(r"[A-Za-z0-9_]+")
# dbt renders YAML strings with Jinja, and the name is quoted in the SQL.
_FORBIDDEN_IN_COLUMN = ("{", "}", '"')

HEADER = "# Généré par scripts/generate_dbt_staging.py (`make dbt-codegen`) : ne pas modifier.\n"
EXTERNAL_LOCATION = (
    "read_parquet('{{ env_var('LAKE_ROOT').strip().rstrip('/') }}/staging/$identifier/"
    "lake_run_id=*/lake_source_file=*/*.parquet', hive_partitioning = true, "
    "union_by_name = true, "
    "hive_types = {'lake_run_id': 'VARCHAR', 'lake_source_file': 'VARCHAR'})"
)
MODEL_SQL = """\
-- Généré par scripts/generate_dbt_staging.py (`make dbt-codegen`) : ne pas modifier.
-- État courant de staging/{table} : pour chaque fichier source, les seules lignes
-- de son instantané le plus récent (plus grand lake_file_id), comme
-- storage.parquet_lake_writer.latest_snapshot. Colonnes fixées par le contrat.
select *
from {{{{ lake_staging('lake_staging', '{table}') }}}} as snapshots
qualify lake_file_id = max(lake_file_id) over (partition by lake_source_file)
"""


def config_roots() -> list[Path]:
    roots = [PROJECT_ROOT / "config", PROJECT_ROOT / "examples" / "config", default_config_root()]
    return list(dict.fromkeys(root.resolve() for root in roots))


def staging_tables(roots: list[Path]) -> tuple[dict[str, tuple], list[str]]:
    """({table: (columns, generate_model)}, warnings)."""
    tables: dict[str, tuple] = {}
    warnings: list[str] = []
    for root in roots:
        for path in sorted((root / "sources").glob("*.y*ml")):
            config = load_source_config(path)
            if config.storage.staging_path is None:
                continue
            table = config.table
            if config.reader.columns_type is None:
                warnings.append(f"{path} : pas de reader.columns_type, aucun contrat possible")
                continue
            if config.staging_target != f"staging/{table}":
                # shortcut: the dbt source glob is `staging/<table>` ; add a
                # per-table external_location if another staging_path is needed.
                warnings.append(f"{path} : staging_path autre que staging/, non généré")
                continue
            if not _TABLE_RE.fullmatch(table):
                raise ConfigError(f"{path} : table {table!r} : `[A-Za-z0-9_]+` requis par dbt")
            bad = [
                c for c in config.reader.columns_type if any(x in c for x in _FORBIDDEN_IN_COLUMN)
            ]
            if bad:
                raise ConfigError(f'{path} : colonnes {bad} : `{{`, `}}` et `"` refusés par dbt')
            columns = tuple(
                (name, resolve_column_type(col_type).duckdb)
                for name, col_type in config.reader.columns_type.items()
            )
            entry = (columns + TECHNICAL_COLUMNS, config.dbt.generate)
            if tables.setdefault(table, entry) != entry:
                raise ConfigError(
                    f"{path} : la table {table!r} est partagée avec une source dont les "
                    "colonnes ou `dbt.generate` diffèrent"
                )
    return tables, warnings


def _yaml(data: dict) -> str:
    return HEADER + yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=1000)


def _column(name: str, data_type: str) -> dict:
    return {"name": name, "quote": True, "data_type": data_type}


def render(tables: dict[str, tuple]) -> dict[str, str]:
    """{file name: content} of the generated directory."""
    if not tables:
        return {}
    technical = {name for name, _ in TECHNICAL_COLUMNS}
    sources, models, files = [], [], {}
    for table, (columns, generate) in sorted(tables.items()):
        sources.append(
            {
                "name": table,
                "description": f"staging/{table} du lake.",
                "columns": [_column(*c) for c in columns],
            }
        )
        if not generate:
            continue
        model = f"stg_{table}__current"
        models.append(
            {
                "name": model,
                "description": (
                    f"État courant de staging/{table} : un seul instantané par fichier source. "
                    "lake_ingested_at : TIMESTAMP sans fuseau, en UTC."
                ),
                "config": {"contract": {"enforced": True}},
                "data_tests": ["one_snapshot_per_source_file"],
                "columns": [
                    _column(*c) | ({"data_tests": ["not_null"]} if c[0] in technical else {})
                    for c in columns
                ],
            }
        )
        files[f"{model}.sql"] = MODEL_SQL.format(table=table)
    source = {
        "name": "lake_staging",
        "description": "Zone staging du lake (fichiers lus, nettoyés, contrôlés).",
        "config": {"formatter": "template", "external_location": EXTERNAL_LOCATION},
        "tables": sources,
    }
    files[SOURCES_FILE] = _yaml({"version": 2, "sources": [source]})
    if models:
        files[MODELS_FILE] = _yaml({"version": 2, "models": models})
    return files


def sync(files: dict[str, str], directory: Path, check: bool) -> list[str]:
    """Names of the files that differ; unless `check`, writes or removes them."""
    managed = {SOURCES_FILE, MODELS_FILE} | {p.name for p in directory.glob("stg_*__current.sql")}
    changed = []
    for name in sorted(managed | files.keys()):
        path = directory / name
        current = path.read_text(encoding="utf-8") if path.is_file() else None
        if current == files.get(name):
            continue  # never rewritten: transform_dag compares mtimes with the manifest
        changed.append(name)
        if check:
            continue
        if name in files:
            directory.mkdir(parents=True, exist_ok=True)
            path.write_text(files[name], encoding="utf-8")
        else:
            path.unlink()
    return changed


def main(argv: list[str] | None = None, output: Path = OUTPUT_DIR) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if the files are out of date")
    args = parser.parse_args(argv)
    try:
        tables, warnings = staging_tables(config_roots())
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    for warning in warnings:
        print(f"warning: {warning}", file=sys.stderr)
    changed = sync(render(tables), output, args.check)
    if args.check and changed:
        print(
            f"error: {output} n'est pas à jour ({', '.join(changed)}) : lancer `make dbt-codegen`",
            file=sys.stderr,
        )
        return 1
    for name in changed:
        print(f"updated {output / name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
