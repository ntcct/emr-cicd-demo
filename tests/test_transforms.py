import datetime as dt
from pathlib import Path

import pytest

from main import parse_args
from sales_pipeline.transforms import (
    SALES_SCHEMA,
    add_revenue,
    aggregate_by_category,
    clean_sales,
    run_pipeline,
    validate_not_empty,
)

SAMPLE_CSV = Path(__file__).resolve().parents[1] / "data" / "sales.csv"


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=SALES_SCHEMA)


def test_clean_sales_removes_invalid_and_duplicate_rows(spark):
    rows = [
        ("1", "2026-10-01", " Books ", 1, 10.0),
        ("1", "2026-10-01", " Books ", 1, 10.0),  # duplicate order
        ("2", "2026-10-01", "Toys", 0, 5.0),  # zero quantity
        ("3", "2026-10-01", "Toys", 1, -1.0),  # negative price
        ("4", "2026-10-01", None, 1, 5.0),  # missing category
        ("5", "2026-10-01", "   ", 1, 5.0),  # blank category
        (None, "2026-10-01", "Toys", 1, 5.0),  # missing id
    ]
    result = clean_sales(_df(spark, rows)).collect()

    assert len(result) == 1
    assert result[0]["category"] == "books"
    assert result[0]["order_date"] == dt.date(2026, 10, 1)


def test_add_revenue_rounds_to_two_decimals(spark):
    df = _df(spark, [("1", "2026-10-01", "toys", 3, 3.333)])
    assert add_revenue(df).first()["revenue"] == pytest.approx(10.0)


def test_aggregate_by_category_orders_by_revenue(spark):
    df = add_revenue(
        _df(
            spark,
            [
                ("1", "2026-10-01", "books", 2, 10.0),
                ("2", "2026-10-01", "books", 1, 5.0),
                ("3", "2026-10-01", "toys", 1, 100.0),
            ],
        )
    )
    rows = [r.asDict() for r in aggregate_by_category(df).collect()]

    assert rows == [
        {"category": "toys", "total_revenue": 100.0, "total_units": 1, "orders": 1},
        {"category": "books", "total_revenue": 25.0, "total_units": 3, "orders": 2},
    ]


def test_validate_not_empty_raises_on_empty(spark):
    with pytest.raises(ValueError, match="empty"):
        validate_not_empty(_df(spark, []), "empty_df")


def test_run_pipeline_on_sample_file(spark):
    raw = spark.read.csv(str(SAMPLE_CSV), header=True, schema=SALES_SCHEMA)
    result = {r["category"]: r["total_revenue"] for r in run_pipeline(raw).collect()}

    assert result == {
        "electronics": pytest.approx(898.98),
        "books": pytest.approx(67.5),
        "garden": pytest.approx(49.5),
        "toys": pytest.approx(39.96),
    }


def test_parse_args_rejects_unknown_env():
    with pytest.raises(SystemExit):
        parse_args(["--input", "a", "--output", "b", "--env", "qa"])
