#!/usr/bin/env python3
"""
Gold build: Silver -> business marts under s3a://lake/gold/, incrementally where possible.

    s3a://lake/silver/*  --(src/gold/marts.py)-->  s3a://lake/gold/{sales_daily, delivery_by_state, seller_scorecard}

HOW A RUN DECIDES WHAT TO DO
    The job keeps a watermark (s3a://lake/_state/gold_watermark): the version of every
    Silver table it last built from. Each run compares that with the current versions:

    first run, or --full             -> FULL rebuild of every mart
    a reference table changed        -> FULL rebuild (e.g. after build_silver.py; products
                                        or sellers changing can move any row)
    only silver.orders changed       -> INCREMENTAL: read just the changed rows from
                                        silver.orders' Change Data Feed, work out which
                                        slices they touch (days, states+months, sellers),
                                        recompute only those, and MERGE them in
    nothing changed                  -> nothing to do

KEY CONCEPTS YOU'LL MEET HERE
    Change Data Feed : Delta records every row a write inserted, updated (before and
                       after images) or deleted. readChangeFeed between two versions
                       returns exactly what changed: the "CDC" of the lake.
    watermark        : "processed up to version N". Saved only after a successful run,
                       so a crash just means the next run redoes the same work.
    snapshot reads   : every Silver table is read at one fixed version (versionAsOf),
                       so a write landing mid-run can't give a half-old, half-new mart.
    MERGE ... NOT MATCHED BY SOURCE DELETE : also removes rows that no longer exist in an
                       affected slice (a seller whose only order was canceled).
    --verify         : recompute everything from scratch and compare with what's
                       stored. Incremental logic is easy to get subtly wrong; this
                       proves it isn't.

USAGE (inside the workspace container; see README Phase 5)
    python /home/jovyan/src/batch/build_gold.py              # incremental when possible
    python /home/jovyan/src/batch/build_gold.py --full       # force a full rebuild
    python /home/jovyan/src/batch/build_gold.py --verify     # then check against a full recompute
"""

from __future__ import annotations

import argparse
import sys

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.spark_session import get_spark, lake_path
from gold.marts import MARTS, Mart, base, slice_key

SOURCES = ["orders", "order_items", "products", "customers", "sellers", "order_reviews"]
WATERMARK = lake_path("_state", "gold_watermark")
# Beyond this many affected slices a full rebuild of the mart is simpler and about as fast
MAX_SLICES = 2000


def latest_version(spark, path: str) -> int:
    return DeltaTable.forPath(spark, path).history(1).first()["version"]


def read_watermark(spark) -> dict | None:
    if not DeltaTable.isDeltaTable(spark, WATERMARK):
        return None
    return {r.silver_table: r.version for r in spark.read.format("delta").load(WATERMARK).collect()}


def write_watermark(spark, versions: dict) -> None:
    (spark.createDataFrame(list(versions.items()), "silver_table string, version long")
     .withColumn("updated_at", F.current_timestamp())
     .write.format("delta").mode("overwrite").save(WATERMARK))


def build_base(spark, versions: dict, orders: DataFrame | None = None) -> DataFrame:
    def silver(name):
        return spark.read.format("delta").option("versionAsOf", versions[name]).load(lake_path("silver", name))
    return base(orders if orders is not None else silver("orders"), silver("order_items"),
                silver("products"), silver("customers"), silver("sellers"), silver("order_reviews"))


def stamped(df: DataFrame) -> DataFrame:
    return df.withColumn("_gold_updated_at", F.current_timestamp())


def full_rebuild(mart: Mart, full_base: DataFrame) -> None:
    out = stamped(mart.build(full_base))
    out.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(lake_path("gold", mart.name))
    print(f"  {mart.name:<18} FULL         {out.count():>7,} rows")


def incremental(spark, mart: Mart, full_base: DataFrame, changed_base: DataFrame) -> None:
    key = slice_key(mart.slice)
    affected = changed_base.select(F.expr(key).alias("_slice")).distinct()
    keys = [r._slice for r in affected.collect()]
    if len(keys) > MAX_SLICES:
        print(f"  {mart.name:<18} {len(keys):,} slices affected, rebuilding fully instead")
        return full_rebuild(mart, full_base)

    target_path = lake_path("gold", mart.name)
    fresh = stamped(mart.build(
        full_base.withColumn("_slice", F.expr(key)).join(affected, "_slice", "left_semi").drop("_slice")))
    in_list = ", ".join("'" + k.replace("'", "''") + "'" for k in keys)
    (
        DeltaTable.forPath(spark, target_path).alias("t")
        .merge(fresh.alias("s"), " AND ".join(f"t.{c} <=> s.{c}" for c in mart.grain))
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(condition=f"{slice_key(mart.slice, 't.')} IN ({in_list})")
        .execute()
    )
    m = DeltaTable.forPath(spark, target_path).history(1).first()["operationMetrics"]
    print(f"  {mart.name:<18} INCREMENTAL  {len(keys):>4} slices -> "
          f"{m.get('numTargetRowsUpdated', 0)} updated, {m.get('numTargetRowsInserted', 0)} inserted, "
          f"{m.get('numTargetRowsDeleted', 0)} deleted")


def verify(spark, full_base: DataFrame) -> bool:
    print("\nVerifying Gold against a full recompute from Silver...")
    ok = True
    for mart in MARTS:
        stored = spark.read.format("delta").load(lake_path("gold", mart.name)).drop("_gold_updated_at")
        expected = mart.build(full_base).select(*stored.columns)
        # Stored rows count +1, recomputed rows -1: any row whose total isn't 0 differs.
        # (Same idea as exceptAll, which trips a Spark 3.5 planner bug on cached inputs.)
        diff = (stored.withColumn("_side", F.lit(1))
                .unionByName(expected.withColumn("_side", F.lit(-1)))
                .groupBy(*stored.columns).agg(F.sum("_side").alias("_net"))
                .filter("_net != 0"))
        extra, missing = diff.filter("_net > 0").count(), diff.filter("_net < 0").count()
        status = "MATCH" if extra == missing == 0 else f"MISMATCH ({extra} extra, {missing} missing rows)"
        print(f"  {mart.name:<18} {status}")
        ok &= extra == missing == 0
    return ok


def main() -> None:
    p = argparse.ArgumentParser(description="Build the Gold marts from Silver.")
    p.add_argument("--full", action="store_true", help="ignore the watermark and rebuild everything")
    p.add_argument("--verify", action="store_true", help="afterwards, compare Gold with a full recompute")
    args = p.parse_args()

    spark = get_spark("gold-build")
    current = {t: latest_version(spark, lake_path("silver", t)) for t in SOURCES}
    previous = read_watermark(spark)
    gold_exists = all(DeltaTable.isDeltaTable(spark, lake_path("gold", m.name)) for m in MARTS)
    full_base = build_base(spark, current).cache()

    reason = None
    if args.full:
        reason = "--full requested"
    elif previous is None or not gold_exists:
        reason = "first run"
    elif set(previous) != set(current):
        reason = "watermark is from an older version of this job"
    elif changed_refs := [t for t in SOURCES if t != "orders" and current[t] != previous[t]]:
        reason = f"reference tables changed: {', '.join(changed_refs)}"

    if reason is None and current["orders"] == previous["orders"]:
        print(f"Nothing new: silver.orders is still at version {current['orders']}.")
    elif reason is None:
        start, end = previous["orders"] + 1, current["orders"]
        try:
            changes = (spark.read.format("delta").option("readChangeFeed", "true")
                       .option("startingVersion", start).option("endingVersion", end)
                       .load(lake_path("silver", "orders")))
            n_changes = changes.count()
        except AnalysisException as e:
            reason = f"Change Data Feed not available for versions {start}-{end} ({str(e).splitlines()[0][:80]})"
        else:
            print(f"INCREMENTAL: silver.orders versions {start}-{end}, {n_changes:,} change rows")
            # Old and new images both count: an order moving between slices affects both
            changed_base = build_base(spark, current, orders=changes.drop(
                "_change_type", "_commit_version", "_commit_timestamp")).cache()
            for mart in MARTS:
                incremental(spark, mart, full_base, changed_base)

    if reason is not None:
        print(f"FULL rebuild: {reason}")
        for mart in MARTS:
            full_rebuild(mart, full_base)

    write_watermark(spark, current)
    print(f"Watermark saved: {current}")

    if args.verify and not verify(spark, full_base):
        sys.exit(1)


if __name__ == "__main__":
    main()
