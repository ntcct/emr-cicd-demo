"""Pure DataFrame -> DataFrame transforms.

Kept free of I/O so they can be unit-tested locally with pytest and a
local SparkSession, then run unchanged on EMR Serverless.
Code is Python 3.9 compatible (EMR 7.x default runtime).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

SALES_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), True),
        T.StructField("order_date", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("quantity", T.IntegerType(), True),
        T.StructField("unit_price", T.DoubleType(), True),
    ]
)


def clean_sales(df: DataFrame) -> DataFrame:
    """Drop invalid rows, normalise category, parse dates, de-duplicate orders."""
    return (
        df.dropna(subset=["order_id", "category", "quantity", "unit_price"])
        .filter((F.col("quantity") > 0) & (F.col("unit_price") >= 0))
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .filter(F.col("category") != "")
        .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
        .dropDuplicates(["order_id"])
    )


def add_revenue(df: DataFrame) -> DataFrame:
    """Add revenue = quantity * unit_price, rounded to 2 decimals."""
    return df.withColumn(
        "revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)
    )


def aggregate_by_category(df: DataFrame) -> DataFrame:
    """Per-category totals, highest revenue first."""
    return (
        df.groupBy("category")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.sum("quantity").alias("total_units"),
            F.countDistinct("order_id").alias("orders"),
        )
        .orderBy(F.col("total_revenue").desc(), F.col("category"))
    )


def validate_not_empty(df: DataFrame, name: str) -> None:
    """Data-quality gate: fail the job if a stage produced no rows."""
    if df.limit(1).count() == 0:
        raise ValueError(f"Data quality check failed: '{name}' is empty")


def run_pipeline(raw: DataFrame) -> DataFrame:
    cleaned = clean_sales(raw)
    validate_not_empty(cleaned, "cleaned_sales")
    return aggregate_by_category(add_revenue(cleaned))
