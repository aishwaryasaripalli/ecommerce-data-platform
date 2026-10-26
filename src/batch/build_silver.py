#!/usr/bin/env python3
"""
Silver build: Bronze Delta tables -> cleaned, typed, deduplicated Silver Delta tables.

    s3a://lake/bronze/<table>  --(src/silver/transforms.py)-->  s3a://lake/silver/<table>

HOW A RUN WORKS (a pattern called write-audit-publish)
    1. build   : every Silver table is computed in memory from Bronze
    2. audit   : data quality checks run on those in-memory tables
    3. publish : only if no `error` check failed are the tables written to the lake.
                 A failed run leaves yesterday's good Silver untouched.
    Check results are saved to s3a://lake/_quality/silver_checks either way.

    Every run is a full rebuild from Bronze (Silver is always reproducible from
    Bronze). Use it for the first load and for backfills; day to day, Phase 5's
    src/streaming/silver_orders_incremental.py MERGEs only new and changed orders.

USAGE (inside the workspace container; see README Phase 4)
    python /home/jovyan/src/batch/build_silver.py
    python /home/jovyan/src/batch/build_silver.py --dry-run     # build + audit, write nothing
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from common.spark_session import get_spark, lake_path
from silver import transforms as T
from silver.quality import QualitySuite

ORDER_STATUSES = ["created", "approved", "invoiced", "processing", "shipped",
                  "delivered", "canceled", "unavailable"]
PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]
# Tables the incremental Gold job (Phase 5) reads row-level changes from
CDF_TABLES = ["orders"]


def build(spark) -> dict:
    def bronze(name):
        return spark.read.format("delta").load(lake_path("bronze", name))

    silver = {
        "orders": T.orders(bronze("orders")),
        "customers": T.customers(bronze("customers")),
        "sellers": T.sellers(bronze("sellers")),
        "order_items": T.order_items(bronze("order_items")),
        "order_payments": T.order_payments(bronze("order_payments")),
        "order_reviews": T.order_reviews(bronze("order_reviews")),
        "products": T.products(bronze("products"), bronze("product_category_translation")),
        "geolocation": T.geolocation(bronze("geolocation")),
    }
    # Cache: each table is read by several checks and by the enriched join
    for df in silver.values():
        df.cache()
    silver["order_items_enriched"] = T.order_items_enriched(
        silver["order_items"], silver["orders"], silver["customers"], silver["products"],
        silver["sellers"], silver["geolocation"], silver["order_payments"], silver["order_reviews"],
    ).cache()
    return silver


def audit(s: dict) -> QualitySuite:
    dq = QualitySuite()

    o = s["orders"]
    dq.unique("orders", o, ["order_id"])
    dq.not_null("orders", o, ["order_id", "customer_id", "order_purchase_timestamp"])
    dq.accepted_values("orders", o, "order_status", ORDER_STATUSES)
    dq.relationship("orders", o, "customer_id", "customers", s["customers"], "customer_id")
    dq.expect("orders", o, "delivered_at >= purchased_at",
              "order_delivered_customer_date >= order_purchase_timestamp", severity="warn")
    dq.expect("orders", o, "delivered orders have a delivery date",
              "order_status != 'delivered' OR order_delivered_customer_date IS NOT NULL", severity="warn")

    dq.unique("customers", s["customers"], ["customer_id"])
    dq.expect("customers", s["customers"], "state is 2 letters", "customer_state RLIKE '^[A-Z]{2}$'")

    dq.unique("sellers", s["sellers"], ["seller_id"])

    i = s["order_items"]
    dq.unique("order_items", i, ["order_id", "order_item_id"])
    dq.expect("order_items", i, "price > 0", "price > 0")
    dq.expect("order_items", i, "freight_value >= 0", "freight_value >= 0")
    dq.relationship("order_items", i, "order_id", "orders", o, "order_id")
    dq.relationship("order_items", i, "product_id", "products", s["products"], "product_id")
    dq.relationship("order_items", i, "seller_id", "sellers", s["sellers"], "seller_id")

    p = s["order_payments"]
    dq.unique("order_payments", p, ["order_id", "payment_sequential"])
    dq.expect("order_payments", p, "payment_value >= 0", "payment_value >= 0")
    dq.accepted_values("order_payments", p, "payment_type", PAYMENT_TYPES, severity="warn")
    dq.expect("order_payments", p, "installments >= 1", "payment_installments >= 1", severity="warn")

    r = s["order_reviews"]
    dq.unique("order_reviews", r, ["review_id", "order_id"])
    dq.expect("order_reviews", r, "score between 1 and 5", "review_score BETWEEN 1 AND 5")
    dq.relationship("order_reviews", r, "order_id", "orders", o, "order_id")

    dq.unique("products", s["products"], ["product_id"])
    dq.not_null("products", s["products"], ["product_category_name_english"])

    g = s["geolocation"]
    dq.unique("geolocation", g, ["zip_code_prefix"])
    dq.expect("geolocation", g, "inside Brazil",
              f"lat BETWEEN {T.BRAZIL_LAT[0]} AND {T.BRAZIL_LAT[1]} "
              f"AND lng BETWEEN {T.BRAZIL_LNG[0]} AND {T.BRAZIL_LNG[1]}")

    e = s["order_items_enriched"]
    # The classic join bug: a one-to-many join silently multiplying rows
    dq.row_count_equals("order_items_enriched", e, i.count(), "order_items rows (no join fan-out)")
    dq.not_null("order_items_enriched", e, ["customer_unique_id", "product_category_name_english",
                                            "seller_state"])
    dq.not_null("order_items_enriched", e, ["customer_lat", "seller_lat", "order_payment_total",
                                            "order_review_score"], severity="warn")
    return dq


def main() -> None:
    p = argparse.ArgumentParser(description="Build the Silver layer from Bronze.")
    p.add_argument("--dry-run", action="store_true", help="build and run checks, but write nothing")
    args = p.parse_args()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    spark = get_spark("silver-build")

    print("1/3 Building Silver tables from Bronze...")
    silver = build(spark)

    print("2/3 Running data quality checks...")
    dq = audit(silver)
    dq.print_report()
    dq.to_dataframe(spark, run_id).write.format("delta").mode("append") \
      .save(lake_path("_quality", "silver_checks"))

    failed = [r for r in dq.results if r.severity == "error" and not r.passed]
    if failed:
        print("\nNOT publishing: fix the errors above. The existing Silver tables are unchanged.")
        sys.exit(1)
    if args.dry_run:
        print("\n--dry-run: checks passed, nothing written.")
        return

    print("\n3/3 Publishing to the lake...")
    for name, df in silver.items():
        target = lake_path("silver", name)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(target)
        print(f"  {name:<22} {df.count():>9,} rows -> {target}")
    # Phase 5: record row-level changes so Gold can update incrementally (Change Data Feed)
    for name in CDF_TABLES:
        spark.sql(f"ALTER TABLE delta.`{lake_path('silver', name)}` "
                  "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print(f"Done. Run id {run_id}.")


if __name__ == "__main__":
    main()
