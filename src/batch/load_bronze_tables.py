#!/usr/bin/env python3
"""
Bronze batch job: the 8 other Olist CSVs -> Delta tables under s3a://lake/bronze/

WHY BATCH HERE BUT STREAMING FOR ORDERS?
    Orders are events: they happen continuously, so they flow through Kafka.
    Customers, products, sellers etc. are reference data that change slowly. In a
    real company they'd arrive as a nightly export or a CDC feed, so a batch load
    is the honest way to model them. Knowing which data deserves streaming (and
    which doesn't) is a classic interview question.

KEY CONCEPTS YOU'LL MEET HERE
    all-strings read : no inferSchema. Bronze keeps values exactly as the file had
                       them; a bad date stays a visible bad string instead of
                       silently becoming null. Silver casts types.
    multiLine        : review comments contain line breaks inside quoted fields.
                       Without this option one review splits into several broken rows.
    overwrite        : each run replaces the table with a full fresh copy, and
                       Delta keeps the old version (time travel), so reruns are safe.

USAGE (inside the workspace container; see README Phase 3)
    python /home/jovyan/src/batch/load_bronze_tables.py
    python /home/jovyan/src/batch/load_bronze_tables.py --only customers products
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from common.spark_session import get_spark, lake_path

RAW_DIR = "/home/jovyan/data/raw/olist"

# Bronze table name -> source CSV (orders is missing on purpose: it arrives via Kafka)
TABLES = {
    "customers": "olist_customers_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv",
}


def main() -> None:
    p = argparse.ArgumentParser(description="Load the Olist reference CSVs into Bronze Delta tables.")
    p.add_argument("--only", nargs="+", choices=sorted(TABLES), help="load just these tables")
    args = p.parse_args()

    spark = get_spark("bronze-batch-load")

    for table in args.only or TABLES:
        source = f"{RAW_DIR}/{TABLES[table]}"
        df = (
            spark.read
            .option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .option("encoding", "UTF-8")
            .csv(source)
            .withColumn("_source_file", F.lit(TABLES[table]))
            .withColumn("_bronze_loaded_at", F.current_timestamp())
        )
        target = lake_path("bronze", table)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(target)
        print(f"  {table:<30} {spark.read.format('delta').load(target).count():>9,} rows -> {target}")

    print("Done.")


if __name__ == "__main__":
    main()
