"""OpenLineage lineage of the `file_list` table (P2-5).

The Airflow tasks reach `file_list` through a SQLAlchemy engine built from
PostgresHook (`get_sqlalchemy_engine()`): the OpenLineage provider sees no
SQL there, so it cannot tell which datasets they read or write. They declare
them to Airflow's hook lineage collector instead, which the provider reads
when the task ends:

- the listing tasks: inputs = the source files they recorded, output =
  `file_list`;
- `spark_read_files`: input = `file_list` (the job selects its files there).

Dataset names come from the providers' own converters, so they match what
the other integrations emit:

- a source file: namespace `file`, name = absolute path, resolved like
  `normalize_spark_path` does it for the job. The Spark listener gives the
  file it reads the same name, so the listing and the Spark run share that
  input dataset;
- `file_list`: namespace `postgres://<host>:<port>`, name
  `<database>.public.file_list`.

Airflow is imported inside the function only: `utils/` is also imported by
the Spark job, which must never import Airflow. Reporting lineage never
fails a task: any error is logged as a warning and ignored. When OpenLineage
is disabled, the collector is a no-op.
"""

import logging
from collections.abc import Iterable
from pathlib import Path

log = logging.getLogger(__name__)

# utils/SQL/script_tables.sql creates `file_list` without a schema, in the
# default `search_path`, i.e. `public`.
FILE_LIST_SCHEMA = "public"
FILE_LIST_TABLE = "file_list"
POSTGRES_DEFAULT_PORT = 5432

_ROLES = ("input", "output")


def report_file_list_lineage(hook, *, role: str, source_files: Iterable[str] = ()) -> None:
    """Declare `file_list` as an input or an output of the running task, and
    `source_files` as its inputs.

    - Lineage off (no-op collector): returns before any Connection lookup.
    - A file whose name contains `#` or `?` is skipped with a warning: an
      Asset URI ends its path there, and quoting gives `%23` / `%3F`, while
      Spark names the file verbatim. No dataset rather than a wrong one.
    - The collector keeps at most `[lineage] max_assets_per_collector`
      inputs (100 by default; the Makefile raises it with
      `AIRFLOW__LINEAGE__MAX_ASSETS_PER_COLLECTOR`): the files beyond are
      dropped, and a warning says how many.

    Args:
        hook: the PostgresHook of the `file_list_db` Connection (the
            Connection gives the host, port and database of the dataset).
        role: "output" (the listing writes `file_list`) or "input".
        source_files: paths of the files recorded by the listing.

    Raises:
        ValueError: unknown `role` (a programming error, not a lineage one).
    """
    if role not in _ROLES:
        raise ValueError(f"role must be one of {_ROLES}, got {role!r}")
    try:
        import airflow.sdk.lineage as sdk_lineage

        collector = sdk_lineage.get_hook_lineage_collector()
        if isinstance(collector, sdk_lineage.NoOpCollector):
            return
        conn = hook.get_connection(getattr(hook, hook.conn_name_attr))
        table = {
            "host": conn.host,
            "port": conn.port or POSTGRES_DEFAULT_PORT,
            # PostgresHook(database=...) overrides the Connection's schema.
            "database": getattr(hook, "database", None) or conn.schema,
            "schema": FILE_LIST_SCHEMA,
            "table": FILE_LIST_TABLE,
        }
        add = collector.add_output_asset if role == "output" else collector.add_input_asset
        add(context=hook, scheme="postgres", asset_kwargs=table)

        files: dict[str, None] = {}
        skipped = []
        for path in source_files:
            local = str(Path(path).expanduser().resolve())
            if "#" in local or "?" in local:
                skipped.append(local)
            else:
                files.setdefault(local)
        if skipped:
            log.warning(
                "OpenLineage: %d source file(s) not declared, their name contains `#` or `?` "
                "(an Asset URI cannot carry it as Spark names it): %s",
                len(skipped),
                ", ".join(skipped),
            )
        before = len(collector.collected_assets.inputs)
        for local in files:
            collector.add_input_asset(context=hook, scheme="file", asset_kwargs={"path": local})
        dropped = before + len(files) - len(collector.collected_assets.inputs)
        if dropped > 0:
            log.warning(
                "OpenLineage: %d source file(s) not declared: the hook lineage collector keeps "
                "at most %d inputs per task ([lineage] max_assets_per_collector, "
                "AIRFLOW__LINEAGE__MAX_ASSETS_PER_COLLECTOR)",
                dropped,
                sdk_lineage.MAX_COLLECTED_ASSETS,
            )
    except Exception:
        log.warning(
            "OpenLineage: could not report the lineage of %s (ignored)",
            FILE_LIST_TABLE,
            exc_info=True,
        )
