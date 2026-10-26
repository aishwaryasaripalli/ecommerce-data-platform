"""
Gold marts: business-ready aggregates built from Silver.

Silver is organised around entities (orders, products, sellers). Gold is organised
around questions: "how much did we sell per day and category?", "which states get
late deliveries?", "which sellers are good?". Analysts and dashboards read Gold.

Each mart is described by a `Mart`:
    grain  : the columns that make a row unique (what one row means)
    slice  : the unit we recompute when orders change. If one order from 2018-03-07
             changes, only the 2018-03-07 rows of sales_daily are recomputed, not
             all ~700 days. Picking a good slice is the heart of incremental design.

All functions are pure (DataFrames in, DataFrame out), so tests/test_gold_marts.py
can check them on tiny hand-made tables.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver.transforms import MONEY, reviews_per_order

# Orders that never turned into a sale
NOT_SOLD = ["canceled", "unavailable"]


def base(orders: DataFrame, items: DataFrame, products: DataFrame, customers: DataFrame,
         sellers: DataFrame, reviews: DataFrame) -> DataFrame:
    """One row per order item with the attributes every mart needs."""
    o = orders.select(
        "order_id", "customer_id", "order_status", "order_purchase_date",
        F.trunc("order_purchase_date", "month").alias("purchase_month"),
        "order_delivered_customer_date", "delivery_days", "is_delivered_late",
    )
    return (
        items.select("order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value")
        .join(o, "order_id")
        .join(products.select("product_id", F.col("product_category_name_english").alias("category")),
              "product_id", "left")
        .join(customers.select("customer_id", "customer_state"), "customer_id", "left")
        .join(sellers.select("seller_id", "seller_state"), "seller_id", "left")
        .join(reviews_per_order(reviews), "order_id", "left")
        .withColumn("category", F.coalesce("category", F.lit("unknown")))
    )


def sales_daily(b: DataFrame) -> DataFrame:
    """Revenue per purchase day and product category. Canceled/unavailable orders excluded."""
    return (
        b.filter(~F.col("order_status").isin(NOT_SOLD))
        .groupBy("order_purchase_date", "category")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.count("*").alias("items"),
            F.sum("price").cast(MONEY).alias("revenue"),
            F.sum("freight_value").cast(MONEY).alias("freight"),
        )
    )


def delivery_by_state(b: DataFrame) -> DataFrame:
    """Delivery speed, lateness and review score per customer state and month (delivered orders only)."""
    delivered_orders = (
        b.filter((F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNotNull())
        .select("order_id", "customer_state", "purchase_month", "delivery_days",
                "is_delivered_late", "order_review_score")
        .dropDuplicates(["order_id"])  # base is item grain; this mart is order grain
    )
    return delivered_orders.groupBy("customer_state", "purchase_month").agg(
        F.count("*").alias("delivered_orders"),
        F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
        F.sum(F.col("is_delivered_late").cast("int")).alias("late_orders"),
        F.round(F.avg(F.col("is_delivered_late").cast("int")), 3).alias("late_rate"),
        F.round(F.avg("order_review_score"), 2).alias("avg_review_score"),
    )


def seller_scorecard(b: DataFrame) -> DataFrame:
    """One row per seller: volume, revenue, customer satisfaction and lateness."""
    sold = b.filter(~F.col("order_status").isin(NOT_SOLD))
    volume = sold.groupBy("seller_id", "seller_state").agg(
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("items"),
        F.sum("price").cast(MONEY).alias("revenue"),
        F.min("order_purchase_date").alias("first_sale_date"),
        F.max("order_purchase_date").alias("last_sale_date"),
    )
    # Review and lateness belong to the order, so count each (seller, order) once
    per_order = sold.dropDuplicates(["seller_id", "order_id"]).groupBy("seller_id").agg(
        F.round(F.avg("order_review_score"), 2).alias("avg_review_score"),
        F.round(F.avg(F.col("is_delivered_late").cast("int")), 3).alias("late_rate"),
    )
    return volume.join(per_order, "seller_id", "left")


@dataclass(frozen=True)
class Mart:
    name: str
    build: Callable[[DataFrame], DataFrame]
    grain: tuple[str, ...]
    slice: tuple[str, ...]


MARTS = [
    Mart("sales_daily", sales_daily, grain=("order_purchase_date", "category"),
         slice=("order_purchase_date",)),
    Mart("delivery_by_state", delivery_by_state, grain=("customer_state", "purchase_month"),
         slice=("customer_state", "purchase_month")),
    Mart("seller_scorecard", seller_scorecard, grain=("seller_id",), slice=("seller_id",)),
]


def slice_key(cols: tuple[str, ...], prefix: str = "") -> str:
    """SQL expression turning a slice into one comparable string, e.g. 'SP|2018-03-01'."""
    return "concat_ws('|', " + ", ".join(f"cast({prefix}{c} as string)" for c in cols) + ")"
