"""
Shared pytest fixture: one small local SparkSession for all tests.

Unit tests use hand-made DataFrames of a few rows, so they need neither Kafka nor
MinIO and run in seconds. Run them inside the workspace container:

    docker compose exec workspace pytest /home/jovyan/tests -q
"""

import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, "/home/jovyan/src")


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
