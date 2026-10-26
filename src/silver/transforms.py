"""
Silver transforms: Bronze (raw strings, duplicates) -> clean, typed, conformed tables.

Every function takes Bronze DataFrames and returns a Silver DataFrame. None of them
read or write storage, so they can be unit-tested on tiny hand-made DataFrames
(see tests/test_silver_transforms.py) without Kafka or MinIO.

WHAT SILVER MEANS
    Bronze answered "what arrived?". Silver answers "what is true?": one row per
    real-world thing (order, customer, product), correct types, consistent
    spellings, and the known quirks of the source fixed in one documented place.

KEY CONCEPTS YOU'LL MEET HERE
    dedupe with a window : row_number() over (partition by key order by newest first),
                           keep row 1. Deterministic, unlike dropDuplicates(), which
                           keeps an arbitrary copy.
    decimal for money    : float can't store 0.1 exactly; DECIMAL(12,2) can.
    grain                : what one row means. order_items_enriched is one row per
                           item, so order-level facts (payments, reviews) are first
                           summarised to one row per order, otherwise the join would
                           multiply rows (the "fan-out" bug).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

MONEY = "decimal(12,2)"

# Two categories in products have no row in the translation CSV (a known Olist gap)
MISSING_CATEGORY_TRANSLATIONS = {
    "pc_gamer": "pc_gamer",
    "portateis_cozinha_e_preparadores_de_alimentos": "portable_kitchen_food_preparers",
}

# Rough bounding box of Brazil; geolocation has a few points in Europe and the ocean
BRAZIL_LAT = (-34.0, 5.5)
BRAZIL_LNG = (-74.0, -34.0)

_ACCENTED = "áàâãäéèêëíìîïóòôõöúùûüçñ"
_PLAIN = "aaaaaeeeeiiiiooooouuuucn"


def normalize_city(col: str) -> F.Column:
    """'São Paulo ' -> 'sao paulo', so customer, seller and geolocation cities match."""
    return F.translate(F.lower(F.trim(F.col(col))), _ACCENTED, _PLAIN)


def latest_per_key(df: DataFrame, keys: list[str], newest_first: list[F.Column]) -> DataFrame:
    """Keep exactly one row per key: the first one by `newest_first` ordering."""
    w = Window.partitionBy(*keys).orderBy(*newest_first)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _drop_bronze_metadata(df: DataFrame) -> DataFrame:
    return df.drop(*[c for c in df.columns if c.startswith("_")])


# ---------------------------------------------------------------------------
# One function per entity
# ---------------------------------------------------------------------------

def orders(bronze_orders: DataFrame) -> DataFrame:
    """One row per order_id (latest Kafka message wins), timestamps typed, delivery metrics added."""
    deduped = latest_per_key(
        bronze_orders, ["order_id"],
        [F.col("_kafka_timestamp").desc(), F.col("_kafka_offset").desc()],
    )
    ts_cols = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date"]
    out = _drop_bronze_metadata(deduped)
    for c in ts_cols:
        out = out.withColumn(c, F.to_timestamp(c))
    return (
        out.withColumn("order_status", F.lower(F.trim("order_status")))
        .withColumn("order_purchase_date", F.to_date("order_purchase_timestamp"))
        .withColumn("delivery_days",
                    F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
        .withColumn("is_delivered_late",
                    F.when(F.col("order_delivered_customer_date").isNull(), None)
                    .otherwise(F.to_date("order_delivered_customer_date")
                               > F.to_date("order_estimated_delivery_date")))
    )


def customers(bronze: DataFrame) -> DataFrame:
    """customer_id is per order; customer_unique_id is the real person (kept for repeat-buyer analysis)."""
    return _drop_bronze_metadata(bronze).select(
        "customer_id",
        "customer_unique_id",
        F.lpad("customer_zip_code_prefix", 5, "0").alias("customer_zip_code_prefix"),
        normalize_city("customer_city").alias("customer_city"),
        F.upper(F.trim("customer_state")).alias("customer_state"),
    ).dropDuplicates(["customer_id"])


def sellers(bronze: DataFrame) -> DataFrame:
    return _drop_bronze_metadata(bronze).select(
        "seller_id",
        F.lpad("seller_zip_code_prefix", 5, "0").alias("seller_zip_code_prefix"),
        normalize_city("seller_city").alias("seller_city"),
        F.upper(F.trim("seller_state")).alias("seller_state"),
    ).dropDuplicates(["seller_id"])


def order_items(bronze: DataFrame) -> DataFrame:
    return _drop_bronze_metadata(bronze).select(
        "order_id",
        F.col("order_item_id").cast("int"),
        "product_id",
        "seller_id",
        F.to_timestamp("shipping_limit_date").alias("shipping_limit_date"),
        F.col("price").cast(MONEY).alias("price"),
        F.col("freight_value").cast(MONEY).alias("freight_value"),
    ).withColumn("item_total", (F.col("price") + F.col("freight_value")).cast(MONEY)) \
     .dropDuplicates(["order_id", "order_item_id"])


def order_payments(bronze: DataFrame) -> DataFrame:
    return _drop_bronze_metadata(bronze).select(
        "order_id",
        F.col("payment_sequential").cast("int"),
        F.lower(F.trim("payment_type")).alias("payment_type"),
        F.col("payment_installments").cast("int"),
        F.col("payment_value").cast(MONEY).alias("payment_value"),
    ).dropDuplicates(["order_id", "payment_sequential"])


def order_reviews(bronze: DataFrame) -> DataFrame:
    """review_id alone is NOT unique in Olist (one review can cover several orders), so the key is (review_id, order_id)."""
    out = _drop_bronze_metadata(bronze).select(
        "review_id",
        "order_id",
        F.col("review_score").cast("int"),
        F.trim("review_comment_title").alias("review_comment_title"),
        F.trim("review_comment_message").alias("review_comment_message"),
        F.to_timestamp("review_creation_date").alias("review_creation_date"),
        F.to_timestamp("review_answer_timestamp").alias("review_answer_timestamp"),
    )
    return latest_per_key(out, ["review_id", "order_id"], [F.col("review_answer_timestamp").desc_nulls_last()])


def products(bronze: DataFrame, bronze_translation: DataFrame) -> DataFrame:
    """Fixes the source's misspelled columns and joins in the English category name."""
    translation = _drop_bronze_metadata(bronze_translation)
    patch = bronze.sparkSession.createDataFrame(
        list(MISSING_CATEGORY_TRANSLATIONS.items()),
        ["product_category_name", "product_category_name_english"],
    )
    translation = translation.unionByName(patch).dropDuplicates(["product_category_name"])

    p = _drop_bronze_metadata(bronze).select(
        "product_id",
        "product_category_name",
        F.col("product_name_lenght").cast("int").alias("product_name_length"),
        F.col("product_description_lenght").cast("int").alias("product_description_length"),
        F.col("product_photos_qty").cast("int"),
        F.col("product_weight_g").cast("int"),
        F.col("product_length_cm").cast("int"),
        F.col("product_height_cm").cast("int"),
        F.col("product_width_cm").cast("int"),
    ).dropDuplicates(["product_id"])

    return (
        p.join(translation, "product_category_name", "left")
        .withColumn("product_category_name", F.coalesce("product_category_name", F.lit("unknown")))
        .withColumn("product_category_name_english",
                    F.coalesce("product_category_name_english", F.lit("unknown")))
    )


def geolocation(bronze: DataFrame) -> DataFrame:
    """1M raw points -> one representative point per zip prefix, inside Brazil only."""
    pts = (
        _drop_bronze_metadata(bronze)
        .select(
            F.lpad("geolocation_zip_code_prefix", 5, "0").alias("zip_code_prefix"),
            F.col("geolocation_lat").cast("double").alias("lat"),
            F.col("geolocation_lng").cast("double").alias("lng"),
            normalize_city("geolocation_city").alias("city"),
            F.upper(F.trim("geolocation_state")).alias("state"),
        )
        .filter(F.col("lat").between(*BRAZIL_LAT) & F.col("lng").between(*BRAZIL_LNG))
    )
    # Most common (city, state) spelling per prefix
    names = (
        pts.groupBy("zip_code_prefix", "city", "state").count()
        .transform(lambda d: latest_per_key(d, ["zip_code_prefix"], [F.col("count").desc(), F.col("city")]))
        .drop("count")
    )
    coords = pts.groupBy("zip_code_prefix").agg(
        F.round(F.avg("lat"), 6).alias("lat"),
        F.round(F.avg("lng"), 6).alias("lng"),
        F.count("*").alias("source_points"),
    )
    return coords.join(names, "zip_code_prefix")


# ---------------------------------------------------------------------------
# The joined, analysis-ready table
# ---------------------------------------------------------------------------

def payments_per_order(payments: DataFrame) -> DataFrame:
    return payments.groupBy("order_id").agg(
        F.sum("payment_value").cast(MONEY).alias("order_payment_total"),
        F.max("payment_installments").alias("order_max_installments"),
        F.array_sort(F.collect_set("payment_type")).alias("order_payment_types"),
    )


def reviews_per_order(reviews: DataFrame) -> DataFrame:
    latest = latest_per_key(reviews, ["order_id"], [F.col("review_answer_timestamp").desc_nulls_last()])
    return latest.select("order_id", F.col("review_score").alias("order_review_score"))


def order_items_enriched(
    items: DataFrame, orders_df: DataFrame, customers_df: DataFrame, products_df: DataFrame,
    sellers_df: DataFrame, geo: DataFrame, payments: DataFrame, reviews: DataFrame,
) -> DataFrame:
    """One row per order item, with everything an analyst needs joined in. Must have exactly as many rows as `items`."""
    cust_geo = geo.select(F.col("zip_code_prefix").alias("customer_zip_code_prefix"),
                          F.col("lat").alias("customer_lat"), F.col("lng").alias("customer_lng"))
    seller_geo = geo.select(F.col("zip_code_prefix").alias("seller_zip_code_prefix"),
                            F.col("lat").alias("seller_lat"), F.col("lng").alias("seller_lng"))
    return (
        items
        .join(orders_df, "order_id", "inner")
        .join(customers_df, "customer_id", "left")
        .join(products_df, "product_id", "left")
        .join(sellers_df, "seller_id", "left")
        .join(cust_geo, "customer_zip_code_prefix", "left")
        .join(seller_geo, "seller_zip_code_prefix", "left")
        .join(payments_per_order(payments), "order_id", "left")
        .join(reviews_per_order(reviews), "order_id", "left")
    )
