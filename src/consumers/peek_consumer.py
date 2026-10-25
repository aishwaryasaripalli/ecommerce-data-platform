#!/usr/bin/env python3
"""
Peek consumer: read the first N messages from a topic and print them.

Use it to prove the producer worked without opening Kafka UI.

CONCEPT: a consumer reads messages from a topic. `auto_offset_reset="earliest"`
means "start from the oldest message", and offsets are the message positions.

USAGE (inside the workspace container)
    python /home/jovyan/src/consumers/peek_consumer.py --count 5
"""

import argparse
import json
import os

from kafka import KafkaConsumer


def main() -> None:
    p = argparse.ArgumentParser(description="Print the first N messages of a Kafka topic.")
    p.add_argument("--topic", default="orders")
    p.add_argument("--count", type=int, default=5)
    p.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    args = p.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=None,                  # no consumer group: we only want to look
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=8000,       # stop waiting after 8 quiet seconds
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    seen = 0
    for msg in consumer:
        v = msg.value
        print(f"partition={msg.partition} offset={msg.offset} key={msg.key.decode()} "
              f"status={v.get('order_status')} purchased={v.get('order_purchase_timestamp')}")
        seen += 1
        if seen >= args.count:
            break

    consumer.close()
    if seen == 0:
        print("No messages found. Run the producer first (see README, step 6).")
    else:
        print(f"Showed {seen} message(s).")


if __name__ == "__main__":
    main()
