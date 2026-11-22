"""
P2-6: explicit schema of the `orders` source, then its dbt contract.

`config/sources/orders.yaml` was read with `inferSchema` and no
`columns_type`, so the dbt source could only declare the technical columns
and `stg_orders__current` had no contract. The schema is the one of
`data/orders_sample.csv` (header `order_id,customer,amount,order_date`):
- `columns_type` in the file's column order, atomic types only;
- `enforceSchema: false`: the schema is positional, so Spark checks each
  file's header against the declared names and fails on a mismatch, instead
  of reading another orders layout with shifted or null columns;
- quality: `not_null(order_id)` and `unique(order_id)` (plus the existing
  `row_count`), all `warn` until quality/orders has been reviewed.

The readings use files written in `tmp_path` (never `data/`), the first one
byte for byte the content of data/orders_sample.csv, through the job's path:
`BaseFileReader` with the repository registry, then the real reader. The
dbt side (source columns, enforced contract) is checked by
tests/unit/dags/test_p2_4_transform_dag.py and, with a real `dbt build`, by
tests/integration/test_p2_4_dbt_build.py.
"""

from datetime import date
from pathlib import Path

import pytest

from pipeline.config import load_source_config
from pipeline.file_reader.base_reader import BaseFileReader

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
ORDERS_YAML = PROJECT_ROOT / "config" / "sources" / "orders.yaml"

# Same bytes as data/orders_sample.csv (UTF-8, LF, accented name).
SAMPLE = (
    "order_id,customer,amount,order_date\n"
    "1,Alice,120.50,2026-11-01\n"
    "2,Bob,75.00,2026-11-02\n"
    "3,Chloé,210.99,2026-11-03\n"
    "4,David,15.25,2026-11-04\n"
    "5,Emma,99.90,2026-11-05\n"
)


def test_p2_6_orders_declares_the_sample_schema():
    reader = load_source_config(ORDERS_YAML).reader
    assert list(reader.columns_type.items()) == [
        ("order_id", "int"),
        ("customer", "string"),
        ("amount", "double"),
        ("order_date", "date"),
    ]
    assert reader.options["enforceSchema"] is False
    # Ignored by Spark once a schema is given: not declared.
    assert "inferSchema" not in reader.options


def test_p2_6_repo_orders_checks_are_warnings():
    """Replaces test_p2_3_repo_orders_warns_on_empty_file_only: the order id
    is now checked too, as `warn` (no real orders file reviewed yet)."""
    orders = load_source_config(ORDERS_YAML)
    assert [(c.check_name, c.severity) for c in orders.quality.checks] == [
        ("not_null(order_id)", "warn"),
        ("unique(order_id)", "warn"),
        ("row_count", "warn"),
    ]
    (row_count,) = [c for c in orders.quality.checks if c.type == "row_count"]
    assert (row_count.min, row_count.max) == (1, None)


def _read(spark, tmp_path: Path, name: str, content: str):
    path = tmp_path / name
    path.write_bytes(content.encode("utf-8"))
    reader = BaseFileReader(spark)
    assert reader.get_config(str(path)).source.name == "orders"
    return reader.read_file(str(path))


def test_p2_6_orders_config_reads_the_sample_typed(spark, tmp_path):
    df = _read(spark, tmp_path, "orders_sample.csv", SAMPLE)

    assert [(f.name, f.dataType.simpleString()) for f in df.schema.fields] == [
        ("order_id", "int"),
        ("customer", "string"),
        ("amount", "double"),
        ("order_date", "date"),
    ]
    rows = [tuple(r) for r in df.orderBy("order_id").collect()]
    assert rows == [
        (1, "Alice", 120.5, date(2026, 11, 1)),
        (2, "Bob", 75.0, date(2026, 11, 2)),
        (3, "Chloé", 210.99, date(2026, 11, 3)),
        (4, "David", 15.25, date(2026, 11, 4)),
        (5, "Emma", 99.9, date(2026, 11, 5)),
    ]


@pytest.mark.parametrize(
    "header",
    [
        # Another orders layout found in the wild: 15 columns.
        "order_id,customer_id,order_date,status,payment_method,shipping_address,"
        "billing_address,discount_amount,tax_amount,shipping_cost,total_amount,currency,"
        "created_at,updated_at,subtotal",
        # Same number of columns, another order.
        "order_id,amount,customer,order_date",
    ],
    ids=["other_layout", "swapped_columns"],
)
def test_p2_6_orders_header_mismatch_fails_instead_of_shifting_columns(spark, tmp_path, header):
    """enforceSchema=false: without it, Spark applies the positional schema
    to any header and silently yields shifted values or nulls."""
    n = header.count(",") + 1
    content = f"{header}\n" + ",".join(["1"] * n) + "\n"
    df = _read(spark, tmp_path, "orders_other.csv", content)

    with pytest.raises(Exception, match=r"(?i)header|CSV"):
        df.collect()
