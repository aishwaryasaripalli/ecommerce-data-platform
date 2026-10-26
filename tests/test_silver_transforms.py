"""
Unit tests for src/silver/transforms.py and src/silver/quality.py.

Each test builds a tiny Bronze-shaped DataFrame that contains one specific quirk of
the real data, and asserts Silver handles it. If someone "simplifies" a transform
later and breaks the dedupe or the fan-out protection, these fail.
"""

from datetime import datetime
from decimal import Decimal

from silver import transforms as T
from silver.quality import QualitySuite

# Explicit schema: Spark can't infer a type for columns that are None in every test row
ORDER_COLS = ("order_id string, customer_id string, order_status string, "
              "order_purchase_timestamp string, order_approved_at string, "
              "order_delivered_carrier_date string, order_delivered_customer_date string, "
              "order_estimated_delivery_date string, _kafka_timestamp timestamp, _kafka_offset long")


def bronze_order(order_id, status="delivered", delivered="2018-01-05 10:00:00",
                 kafka_ts="2026-01-01 00:00:00", offset=0):
    return (order_id, "c1", status, "2018-01-01 09:00:00", None, None, delivered,
            "2018-01-10 00:00:00", datetime.fromisoformat(kafka_ts), offset)


def test_orders_keeps_one_row_per_order_latest_message_wins(spark):
    df = spark.createDataFrame([
        bronze_order("o1", status="shipped", offset=1),
        bronze_order("o1", status="delivered", kafka_ts="2026-01-02 00:00:00", offset=7),
        bronze_order("o2", offset=2),
    ], ORDER_COLS)
    out = {r.order_id: r for r in T.orders(df).collect()}
    assert len(out) == 2
    assert out["o1"].order_status == "delivered"
    assert out["o1"].last_event_at == datetime(2026, 1, 2)


def test_orders_types_and_delivery_metrics(spark):
    df = spark.createDataFrame([
        bronze_order("on_time", delivered="2018-01-05 10:00:00"),
        bronze_order("late", delivered="2018-01-12 10:00:00"),
        bronze_order("not_yet", status="shipped", delivered=None),
    ], ORDER_COLS)
    out = {r.order_id: r for r in T.orders(df).collect()}
    assert isinstance(out["on_time"].order_purchase_timestamp, datetime)
    assert out["on_time"].delivery_days == 4
    assert out["on_time"].is_delivered_late is False
    assert out["late"].is_delivered_late is True
    assert out["not_yet"].is_delivered_late is None
    assert not any(c.startswith("_") for c in T.orders(df).columns)


def test_normalize_city_removes_accents_case_and_spaces(spark):
    df = spark.createDataFrame([(" São Paulo ",), ("BRASÍLIA",)], ["city"])
    assert [r[0] for r in df.select(T.normalize_city("city")).collect()] == ["sao paulo", "brasilia"]


def test_order_items_money_is_decimal(spark):
    df = spark.createDataFrame(
        [("o1", "1", "p1", "s1", "2018-01-01 00:00:00", "0.10", "0.20")],
        ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date",
         "price", "freight_value"])
    row = T.order_items(df).first()
    assert row.item_total == Decimal("0.30")  # a float would give 0.30000000000000004
    assert row.order_item_id == 1


def test_products_fixes_typo_columns_and_fills_categories(spark):
    cols = ["product_id", "product_category_name", "product_name_lenght",
            "product_description_lenght", "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm"]
    products = spark.createDataFrame([
        ("p1", "beleza_saude", "40", "300", "1", "500", "10", "10", "10"),
        ("p2", "pc_gamer", "40", "300", "1", "500", "10", "10", "10"),   # not in translation CSV
        ("p3", None, None, None, None, "500", "10", "10", "10"),          # no category at all
    ], cols)
    translation = spark.createDataFrame([("beleza_saude", "health_beauty")],
                                        ["product_category_name", "product_category_name_english"])
    out = T.products(products, translation)
    assert "product_name_length" in out.columns and "product_name_lenght" not in out.columns
    english = {r.product_id: r.product_category_name_english for r in out.collect()}
    assert english == {"p1": "health_beauty", "p2": "pc_gamer", "p3": "unknown"}


def test_geolocation_one_row_per_zip_and_drops_points_outside_brazil(spark):
    df = spark.createDataFrame([
        ("01001", "-23.55", "-46.63", "são paulo", "SP"),
        ("01001", "-23.57", "-46.65", "sao paulo", "SP"),
        ("01001", "45.06", "9.00", "sao paulo", "SP"),     # a point in Italy: bad source data
    ], ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
        "geolocation_city", "geolocation_state"])
    rows = T.geolocation(df).collect()
    assert len(rows) == 1
    assert rows[0].source_points == 2
    assert rows[0].lat == -23.56 and rows[0].city == "sao paulo"


def test_enriched_join_does_not_multiply_rows(spark):
    """An order with 2 payments and 2 reviews must still give one row per item."""
    items = spark.createDataFrame([("o1", 1, "p1", "s1"), ("o1", 2, "p1", "s1")],
                                  ["order_id", "order_item_id", "product_id", "seller_id"])
    orders = spark.createDataFrame([("o1", "c1")], ["order_id", "customer_id"])
    customers = spark.createDataFrame([("c1", "u1", "01001")],
                                      ["customer_id", "customer_unique_id", "customer_zip_code_prefix"])
    products = spark.createDataFrame([("p1", "toys")], ["product_id", "product_category_name_english"])
    sellers = spark.createDataFrame([("s1", "01001", "SP")],
                                    ["seller_id", "seller_zip_code_prefix", "seller_state"])
    geo = spark.createDataFrame([("01001", -23.5, -46.6)], ["zip_code_prefix", "lat", "lng"])
    payments = spark.createDataFrame(
        [("o1", 1, "credit_card", 3, Decimal("10.00")), ("o1", 2, "voucher", 1, Decimal("5.00"))],
        "order_id string, payment_sequential int, payment_type string, "
        "payment_installments int, payment_value decimal(12,2)")
    reviews = spark.createDataFrame(
        [("r1", "o1", 2, datetime(2018, 1, 1)), ("r2", "o1", 5, datetime(2018, 2, 1))],
        "review_id string, order_id string, review_score int, review_answer_timestamp timestamp")

    out = T.order_items_enriched(items, orders, customers, products, sellers, geo, payments, reviews)
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0].order_payment_total == Decimal("15.00")
    assert rows[0].order_payment_types == ["credit_card", "voucher"]
    assert rows[0].order_review_score == 5  # latest review wins


def test_quality_suite_counts_failures_and_severity(spark):
    df = spark.createDataFrame([("a", 1), ("a", 2), ("b", None)], ["id", "score"])
    dq = QualitySuite()
    dq.unique("t", df, ["id"])
    dq.not_null("t", df, ["score"], severity="warn")
    dq.expect("t", df, "score positive", "score > 0")
    results = {r.check: r for r in dq.results}
    assert results["unique(id)"].failing_rows == 1
    assert results["not_null(score)"].failing_rows == 1
    assert results["score positive"].passed  # NULL isn't counted as a failure
