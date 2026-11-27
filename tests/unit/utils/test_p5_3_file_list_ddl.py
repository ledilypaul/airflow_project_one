"""
P5-3 — `utils/SQL/script_tables.sql` is also the migration of an existing `file_list`:
every statement must be safe to run again, and the column must exist before the index
that uses it. (Checked once against PostgreSQL 17 on a table of the previous schema,
twice in a row, and on an empty database: see docs/architecture/2026-11-24-p5-3-dag-factory.md.)
"""

import re
from pathlib import Path

DDL = Path(__file__).resolve().parent.parent.parent.parent / "utils" / "SQL" / "script_tables.sql"


def _statements() -> list[str]:
    without_comments = re.sub(r"--[^\n]*", "", DDL.read_text())
    return [" ".join(s.split()) for s in without_comments.split(";") if s.strip()]


def test_p5_3_every_statement_can_run_again():
    allowed = (
        "CREATE TABLE IF NOT EXISTS",
        "CREATE UNIQUE INDEX IF NOT EXISTS",
        "DROP INDEX IF EXISTS",
    )
    for statement in _statements():
        if statement.startswith("ALTER TABLE"):
            assert "ADD COLUMN IF NOT EXISTS" in statement, statement
        else:
            assert statement.startswith(allowed), statement


def test_p5_3_source_name_exists_before_the_index_on_it():
    statements = _statements()
    column = next(
        i for i, s in enumerate(statements) if "ADD COLUMN IF NOT EXISTS source_name" in s
    )
    index = next(
        i for i, s in enumerate(statements) if "idx_file_path_source" in s and "CREATE" in s
    )
    old_index = next(i for i, s in enumerate(statements) if s.startswith("DROP INDEX"))
    assert column < index < old_index  # the new index exists before the old one goes


def test_p5_3_fresh_tables_have_the_column_too():
    create = next(s for s in _statements() if s.startswith("CREATE TABLE"))
    assert "source_name VARCHAR(100) NOT NULL DEFAULT ''" in create


def test_p5_3_the_unique_key_includes_the_source():
    index = next(s for s in _statements() if "idx_file_path_source" in s and "CREATE" in s)
    assert "(file_path, dag_run_id, source_name)" in index
