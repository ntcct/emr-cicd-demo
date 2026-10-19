import os
import sys

import pytest

from main import main

# Local Parquet writes on Windows need Hadoop winutils; CI runs on Linux.
needs_hadoop = pytest.mark.skipif(
    sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
    reason="Parquet write on Windows requires HADOOP_HOME/winutils",
)


@needs_hadoop
def test_main_end_to_end_writes_parquet(spark, tmp_path):
    from tests.test_transforms import SAMPLE_CSV

    out = tmp_path / "out"
    rc = main(["--input", str(SAMPLE_CSV), "--output", str(out), "--env", "dev"], spark=spark)

    assert rc == 0
    assert (out / "_SUCCESS").exists()
    rows = spark.read.parquet(str(out)).collect()
    assert {r["category"] for r in rows} == {"electronics", "books", "garden", "toys"}
