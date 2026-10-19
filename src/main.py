"""EMR Serverless entry point.

spark-submit main.py --input s3://.../sales.csv --output s3://.../out/ --env dev
(sales_pipeline package is shipped via --py-files sales_pipeline.zip)
"""
from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from pyspark.sql import SparkSession

from sales_pipeline.transforms import SALES_SCHEMA, run_pipeline


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sales aggregation Spark job")
    parser.add_argument("--input", required=True, help="Input CSV path (s3:// or local)")
    parser.add_argument("--output", required=True, help="Output Parquet path")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None, spark: Optional[SparkSession] = None) -> int:
    args = parse_args(argv)
    owns_session = spark is None  # tests inject a session; EMR creates its own
    if spark is None:
        spark = SparkSession.builder.appName(f"sales-pipeline-{args.env}").getOrCreate()
    try:
        raw = spark.read.csv(args.input, header=True, schema=SALES_SCHEMA)
        result = run_pipeline(raw)
        result.show(truncate=False)  # visible in SPARK_DRIVER/stdout logs
        result.coalesce(1).write.mode("overwrite").parquet(args.output)
        print(f"[{args.env}] wrote {result.count()} category rows to {args.output}")
        return 0
    finally:
        if owns_session:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
