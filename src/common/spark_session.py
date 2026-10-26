"""
Shared Spark setup: one place that knows how to talk to Delta Lake and MinIO.

Every job (streaming or batch) and every notebook calls `get_spark()` instead of
building its own SparkSession, so the lake settings can never drift apart.

KEY CONCEPTS YOU'LL MEET HERE
    Delta Lake : a table format on top of Parquet files. The `_delta_log/` folder
                 next to the data records every write as a transaction, which is
                 what gives you ACID writes, schema enforcement and time travel.
    S3A        : Hadoop's S3 connector. MinIO speaks the same API as AWS S3, so the
                 exact code here works against real S3 later (Phase 9) by changing
                 only the endpoint and credentials.
    path style : MinIO serves buckets as http://minio:9000/lake/... instead of
                 AWS's http://lake.s3.amazonaws.com/..., so we switch that on.

USAGE
    from common.spark_session import get_spark, lake_path
    spark = get_spark("my-job")
    df.write.format("delta").save(lake_path("bronze", "orders"))
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin123")
LAKE_BUCKET = os.environ.get("LAKE_BUCKET", "lake")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def lake_path(*parts: str) -> str:
    """s3a://lake/bronze/orders style path for a layer + table."""
    return "/".join([f"s3a://{LAKE_BUCKET}", *parts])


def ensure_bucket() -> None:
    """Create the lake bucket if it doesn't exist yet (S3A can't create buckets itself)."""
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT,
                      aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    try:
        s3.head_bucket(Bucket=LAKE_BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=LAKE_BUCKET)
        print(f"Created bucket '{LAKE_BUCKET}' in MinIO")


def get_spark(app_name: str) -> SparkSession:
    """A local SparkSession wired for Delta Lake on MinIO."""
    ensure_bucket()
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        # Small local data: 200 shuffle partitions (the default) would mean 200 tiny files
        .config("spark.sql.shuffle.partitions", "4")
        # Our scripts print their own progress; Spark's [Stage 6:===>] bars just clutter it
        .config("spark.ui.showConsoleProgress", "false")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO through the S3A connector
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Our jobs scan whole files. The default policy flips to "random" after Parquet's
        # footer seek and then fetches tiny ranges, one HTTP request each: a 25 MB table
        # took 12 minutes to read instead of 1 second.
        .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
