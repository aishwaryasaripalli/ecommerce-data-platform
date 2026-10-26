#!/usr/bin/env python3
"""
Order update simulator: sends *changes* to existing orders into Kafka (CDC-style events).

The replay producer sends every order once, frozen at its final status. Real orders
change after they're placed: a shipped parcel gets delivered, a processing order gets
canceled. This script picks some orders and publishes their new state to the same
`orders` topic, the way a change data capture (CDC) tool such as Debezium would
publish row changes from the shop's database.

Downstream, nothing special is needed: Bronze appends the new events, the
incremental Silver job MERGEs them (latest event wins), and the incremental Gold
job recomputes only the dates, sellers and states those orders touch.

WHAT IT CHANGES (deterministic for a given --seed, so reruns pick the same orders)
    --deliver N : N 'shipped' orders become 'delivered', with a delivery date 1-20
                  days after they left the carrier (so some arrive late)
    --cancel N  : N 'processing' / 'invoiced' orders become 'canceled'

USAGE (inside the workspace container; see README Phase 5)
    python /home/jovyan/src/producers/order_updates.py --deliver 50 --cancel 20
    python /home/jovyan/src/producers/order_updates.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta

from producers.order_producer import (DEFAULT_BOOTSTRAP, DEFAULT_CSV, TS_FORMAT,
                                      ensure_topic, load_orders, make_event)


def plan_updates(rows: list[dict], n_deliver: int, n_cancel: int, seed: int) -> list[dict]:
    rng = random.Random(seed)
    by_status = lambda *s: sorted((r for r in rows if r["order_status"] in s), key=lambda r: r["order_id"])

    updates = []
    for r in rng.sample(by_status("shipped"), n_deliver):
        left_carrier = datetime.strptime(r["order_delivered_carrier_date"] or r["order_purchase_timestamp"], TS_FORMAT)
        delivered = left_carrier + timedelta(days=rng.randint(1, 20), hours=rng.randint(0, 23))
        updates.append({**r, "order_status": "delivered",
                        "order_delivered_customer_date": delivered.strftime(TS_FORMAT)})
    for r in rng.sample(by_status("processing", "invoiced"), n_cancel):
        updates.append({**r, "order_status": "canceled"})
    return updates


def main() -> None:
    p = argparse.ArgumentParser(description="Publish order status changes to Kafka.")
    p.add_argument("--deliver", type=int, default=50, help="shipped orders to mark delivered")
    p.add_argument("--cancel", type=int, default=20, help="processing/invoiced orders to cancel")
    p.add_argument("--seed", type=int, default=42, help="change it to pick different orders")
    p.add_argument("--csv", default=DEFAULT_CSV)
    p.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    p.add_argument("--topic", default="orders")
    p.add_argument("--dry-run", action="store_true", help="print the changes without sending")
    args = p.parse_args()

    updates = plan_updates(load_orders(args.csv), args.deliver, args.cancel, args.seed)
    for u in updates[:3] + updates[args.deliver:args.deliver + 2]:
        print(f"  {u['order_id']}  -> {u['order_status']:<9} delivered={u['order_delivered_customer_date']}")
    print(f"  ... {len(updates)} changes in total")

    if args.dry_run:
        print("Dry run: nothing sent.")
        return

    from kafka import KafkaProducer

    ensure_topic(args.bootstrap_servers, args.topic, partitions=3)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
    )
    for u in updates:
        # Same key as the original order -> same partition -> Kafka keeps them in order
        producer.send(args.topic, key=u["order_id"], value={**make_event(u), "_source": "order_update_simulator"})
    producer.flush()
    producer.close()
    print(f"Sent {len(updates)} order changes to '{args.topic}'.")


if __name__ == "__main__":
    main()
