import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Small, fast local SparkSession shared by all tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pytest-sales-pipeline")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
