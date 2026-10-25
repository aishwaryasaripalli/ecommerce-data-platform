#!/usr/bin/env python3
"""
Order producer: replays the Olist `orders` table into Kafka as a simulated live feed.

WHY REPLAY?
    Real companies have a live stream of orders. We don't, so we take historical
    orders, sort them by purchase time, and publish them one by one with a
    (scaled-down) pause between them. Downstream code (Spark, dbt, Airflow) can't
    tell the difference between this and a real stream. This is a standard
    technique for developing and testing streaming pipelines.

KEY CONCEPTS YOU'LL MEET HERE
    topic      : a named channel in Kafka (here: "orders")
    producer   : a program that writes messages to a topic
    key        : we use order_id, so every message for one order goes to the same
                 partition and keeps its order
    partition  : a topic is split into partitions so it can scale out
    serializer : Kafka only stores bytes; we turn our dict into JSON bytes

USAGE (run inside the workspace container; see README)
    python /home/jovyan/src/producers/order_producer.py --limit 200
    python /home/jovyan/src/producers/order_producer.py --dry-run --limit 5
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

DEFAULT_CSV = "/home/jovyan/data/raw/olist/olist_orders_dataset.csv"
DEFAULT_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TS_FIELD = "order_purchase_timestamp"
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay Olist orders into Kafka.")
    p.add_argument("--csv", default=DEFAULT_CSV, help="path to olist_orders_dataset.csv")
    p.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP,
                   help="Kafka address (kafka:9092 inside Docker, localhost:9094 from your Mac)")
    p.add_argument("--topic", default="orders")
    p.add_argument("--partitions", type=int, default=3, help="partitions if the topic must be created")
    p.add_argument("--limit", type=int, default=None, help="only send the first N orders (good for testing)")
    p.add_argument("--speedup", type=float, default=50000,
                   help="time compression: 50000 means 50000 real seconds pass in 1 second. 0 = no pauses")
    p.add_argument("--max-sleep", type=float, default=0.5,
                   help="never pause longer than this many seconds between orders")
    p.add_argument("--log-every", type=int, default=100, help="print progress every N orders")
    p.add_argument("--dry-run", action="store_true", help="don't touch Kafka; just print what would be sent")
    return p.parse_args()


def load_orders(path: str) -> list[dict]:
    """Read the CSV, turn empty strings into None, and sort chronologically."""
    if not os.path.exists(path):
        sys.exit(
            f"ERROR: cannot find {path}\n"
            "  -> Did you download the Olist dataset into data/raw/olist/ ?\n"
            "  -> On your Mac run: bash scripts/check_data.sh"
        )
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = [{k: (v if v != "" else None) for k, v in r.items()} for r in csv.DictReader(f)]
    rows = [r for r in rows if r.get(TS_FIELD)]
    # ISO-style timestamps sort correctly as plain strings
    rows.sort(key=lambda r: r[TS_FIELD])
    return rows


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    """Create the topic if it doesn't exist yet (idempotent)."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
        print(f"Created topic '{topic}' with {partitions} partitions")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic}' already exists, reusing it")
    finally:
        admin.close()


def make_event(row: dict) -> dict:
    """The message body: the original order row plus two metadata fields."""
    return {
        **row,
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
        "_source": "olist_replay",
    }


def main() -> None:
    args = parse_args()
    rows = load_orders(args.csv)
    if args.limit:
        rows = rows[: args.limit]
    print(f"Loaded {len(rows):,} orders "
          f"({rows[0][TS_FIELD]} -> {rows[-1][TS_FIELD]}), speedup={args.speedup:g}")

    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer

        ensure_topic(args.bootstrap_servers, args.topic, args.partitions)
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",      # wait until Kafka confirms the write
            linger_ms=20,    # tiny batching window for efficiency
        )

    started = time.time()
    prev_ts = None
    sent = 0
    try:
        for row in rows:
            ts = datetime.strptime(row[TS_FIELD], TS_FORMAT)
            if prev_ts is not None and args.speedup > 0:
                gap = (ts - prev_ts).total_seconds() / args.speedup
                time.sleep(min(max(gap, 0.0), args.max_sleep))
            prev_ts = ts

            event = make_event(row)
            if args.dry_run:
                if sent < 3:
                    print("DRY-RUN event:", json.dumps(event)[:300])
            else:
                producer.send(args.topic, key=row["order_id"], value=event)
            sent += 1

            if sent % args.log_every == 0:
                print(f"  sent {sent:,}/{len(rows):,}  (order time {row[TS_FIELD]})")
    except KeyboardInterrupt:
        print("\nStopped by you (Ctrl+C), flushing what was already sent...")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()

    print(f"Done: {sent:,} orders {'(dry run) ' if args.dry_run else ''}in {time.time() - started:.1f}s")


if __name__ == "__main__":
    main()
