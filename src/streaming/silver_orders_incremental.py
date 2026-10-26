#!/usr/bin/env python3
"""
Incremental Silver: new Bronze order events -> MERGE into silver.orders (and refresh
the matching rows of silver.order_items_enriched).

WHY NOT JUST REBUILD?
    build_silver.py recomputes all 99k orders every run. Fine today, but a real shop
    adds millions of events a day and changes only a few thousand orders. This job
    touches only what's new: it reads Bronze as a stream (the checkpoint remembers
    which Bronze rows it already processed) and MERGEs each micro-batch.

KEY CONCEPTS YOU'LL MEET HERE
    MERGE (upsert)  : one atomic statement that updates matching rows and inserts new
                      ones. Delta's version of SQL's MERGE INTO.
    latest wins     : an update is applied only if its event is newer than what Silver
                      has (last_event_at), so replaying old events can't roll an order
                      back, and rerunning the job changes nothing (idempotent).
    foreachBatch    : run ordinary batch code (like MERGE) on every streaming micro-batch.
    CDC             : change data capture. Kafka now carries *changes* to orders (see
                      src/producers/order_updates.py), and this job applies them.
    Change Data Feed: silver.orders records which rows each MERGE changed, so the Gold
                      job (src/batch/build_gold.py) can update only what's affected.

USAGE (inside the workspace container; see README Phase 5)
    python /home/jovyan/src/streaming/silver_orders_incremental.py --available-now
    python /home/jovyan/src/streaming/silver_orders_incremental.py     # keep running
"""

from __future__ import annotations

import argparse
import sys

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.spark_session import get_spark, lake_path
from silver import transforms as T

SILVER_ORDERS = lake_path("silver", "orders")
SILVER_ENRICHED = lake_path("silver", "order_items_enriched")


def merge_orders(spark, changes: DataFrame) -> int:
    """Upsert the latest version of each order. Returns the number of orders inserted or updated."""
    before = DeltaTable.forPath(spark, SILVER_ORDERS).history(1).first()["version"]
    (
        DeltaTable.forPath(spark, SILVER_ORDERS).alias("t")
        .merge(changes.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="s.last_event_at > t.last_event_at")
        .whenNotMatchedInsertAll()
        .execute()
    )
    last = DeltaTable.forPath(spark, SILVER_ORDERS).history(1).first()
    if last["version"] == before:
        return 0
    m = last["operationMetrics"]
    return int(m.get("numTargetRowsUpdated", 0)) + int(m.get("numTargetRowsInserted", 0))


def refresh_enriched(spark, order_ids: DataFrame) -> None:
    """Rebuild the enriched rows for just these orders and MERGE them in."""
    def silver(name):
        return spark.read.format("delta").load(lake_path("silver", name))

    items = silver("order_items").join(order_ids, "order_id", "left_semi")
    fresh = T.order_items_enriched(
        items, silver("orders"), silver("customers"), silver("products"), silver("sellers"),
        silver("geolocation"), silver("order_payments"), silver("order_reviews"),
    )
    (
        DeltaTable.forPath(spark, SILVER_ENRICHED).alias("t")
        .merge(fresh.alias("s"), "t.order_id = s.order_id AND t.order_item_id = s.order_item_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_batch(spark, bronze_batch: DataFrame, batch_id: int) -> None:
    changes = T.orders(bronze_batch).cache()  # same cleaning as the full build, so both agree
    if changes.isEmpty():
        return
    received = changes.count()
    changed = merge_orders(spark, changes)
    if changed:
        # Refresh the enriched rows of this batch's orders (skipped when nothing changed,
        # e.g. the first run replaying events the full build already applied)
        refresh_enriched(spark, changes.select("order_id"))
    print(f"  batch {batch_id}: {received:,} orders received, {changed:,} inserted or updated")
    changes.unpersist()


def main() -> None:
    p = argparse.ArgumentParser(description="MERGE new Bronze order events into Silver.")
    p.add_argument("--available-now", action="store_true", help="process what's new, then stop")
    p.add_argument("--interval", default="30 seconds")
    p.add_argument("--checkpoint", default=lake_path("_checkpoints", "silver_orders"))
    args = p.parse_args()

    spark = get_spark("silver-orders-incremental")
    if not DeltaTable.isDeltaTable(spark, SILVER_ORDERS):
        sys.exit("silver.orders doesn't exist yet. Run the full build first:\n"
                 "  python /home/jovyan/src/batch/build_silver.py")
    if "last_event_at" not in spark.read.format("delta").load(SILVER_ORDERS).columns:
        sys.exit("silver.orders predates Phase 5 (no last_event_at column). Rebuild it once:\n"
                 "  python /home/jovyan/src/batch/build_silver.py")

    stream = spark.readStream.format("delta").load(lake_path("bronze", "orders"))
    writer = (
        stream.writeStream
        .foreachBatch(lambda df, batch_id: process_batch(spark, df, batch_id))
        .option("checkpointLocation", args.checkpoint)
        .queryName("silver_orders_merge")
    )
    writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime=args.interval)

    print(f"Merging new Bronze order events into {SILVER_ORDERS}")
    query = writer.start()
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping (Ctrl+C). The checkpoint remembers where we got to.")
        query.stop()
    total = spark.read.format("delta").load(SILVER_ORDERS).count()
    print(f"Done. silver.orders has {total:,} orders.")


if __name__ == "__main__":
    main()
