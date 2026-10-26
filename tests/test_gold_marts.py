"""
Unit tests for src/gold/marts.py.

One small shop: two orders on the same day from SP (one with two items), one
canceled order, and one order still in transit. Each test checks a business rule
a mart must respect.
"""

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from gold import marts as G


@pytest.fixture(scope="module")
def shop(spark):
    orders = spark.createDataFrame([
        ("o1", "c1", "delivered", date(2018, 3, 7), datetime(2018, 3, 10), 3, False),
        ("o2", "c2", "delivered", date(2018, 3, 7), datetime(2018, 3, 30), 23, True),
        ("o3", "c1", "canceled", date(2018, 3, 7), None, None, None),
        ("o4", "c2", "shipped", date(2018, 3, 8), None, None, None),
    ], "order_id string, customer_id string, order_status string, order_purchase_date date, "
       "order_delivered_customer_date timestamp, delivery_days int, is_delivered_late boolean")
    items = spark.createDataFrame([
        ("o1", 1, "p1", "s1", Decimal("10.00"), Decimal("1.00")),
        ("o1", 2, "p2", "s2", Decimal("20.00"), Decimal("2.00")),
        ("o2", 1, "p1", "s1", Decimal("30.00"), Decimal("3.00")),
        ("o3", 1, "p1", "s1", Decimal("99.00"), Decimal("9.00")),
        ("o4", 1, "p2", "s2", Decimal("40.00"), Decimal("4.00")),
    ], "order_id string, order_item_id int, product_id string, seller_id string, "
       "price decimal(12,2), freight_value decimal(12,2)")
    products = spark.createDataFrame([("p1", "toys"), ("p2", "books")],
                                     ["product_id", "product_category_name_english"])
    customers = spark.createDataFrame([("c1", "SP"), ("c2", "SP")], ["customer_id", "customer_state"])
    sellers = spark.createDataFrame([("s1", "SP"), ("s2", "RJ")], ["seller_id", "seller_state"])
    reviews = spark.createDataFrame([
        ("r1", "o1", 5, datetime(2018, 3, 11)),
        ("r2", "o2", 1, datetime(2018, 3, 31)),
    ], "review_id string, order_id string, review_score int, review_answer_timestamp timestamp")
    return G.base(orders, items, products, customers, sellers, reviews)


def test_sales_daily_excludes_canceled_orders(shop):
    rows = {(r.order_purchase_date, r.category): r for r in G.sales_daily(shop).collect()}
    toys_7th = rows[(date(2018, 3, 7), "toys")]
    assert toys_7th.orders == 2 and toys_7th.items == 2
    assert toys_7th.revenue == Decimal("40.00")  # o1 + o2; the canceled o3 (99.00) is left out
    assert rows[(date(2018, 3, 8), "books")].revenue == Decimal("40.00")  # in transit still counts as sold


def test_delivery_by_state_counts_orders_not_items(shop):
    rows = G.delivery_by_state(shop).collect()
    assert len(rows) == 1
    sp = rows[0]
    assert sp.purchase_month == date(2018, 3, 1)
    assert sp.delivered_orders == 2      # o1 has two items but is one order
    assert sp.late_orders == 1 and sp.late_rate == 0.5
    assert sp.avg_review_score == 3.0    # (5 + 1) / 2
    assert sp.avg_delivery_days == 13.0  # (3 + 23) / 2


def test_seller_scorecard(shop):
    rows = {r.seller_id: r for r in G.seller_scorecard(shop).collect()}
    s1 = rows["s1"]
    assert s1.orders == 2 and s1.revenue == Decimal("40.00")
    assert s1.avg_review_score == 3.0 and s1.late_rate == 0.5
    assert rows["s2"].first_sale_date == date(2018, 3, 7)
    assert rows["s2"].last_sale_date == date(2018, 3, 8)


def test_every_mart_is_unique_on_its_grain(shop):
    for mart in G.MARTS:
        out = mart.build(shop)
        assert out.count() == out.dropDuplicates(list(mart.grain)).count(), mart.name


def test_slice_key_builds_one_string_per_slice(spark):
    df = spark.createDataFrame([("SP", date(2018, 3, 1))], "customer_state string, purchase_month date")
    key = G.slice_key(("customer_state", "purchase_month"))
    assert df.select(F.expr(key)).first()[0] == "SP|2018-03-01"
