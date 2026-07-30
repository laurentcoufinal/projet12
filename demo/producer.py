#!/usr/bin/env python3
"""Producteur Kafka : publie quelques événements JSON sur demo-events."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "demo-events"


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    events = [
        {"type": "order.created", "order_id": "ORD-001", "amount": 42.5},
        {"type": "order.created", "order_id": "ORD-002", "amount": 19.9},
        {"type": "payment.completed", "order_id": "ORD-001", "status": "ok"},
    ]

    print(f"Connexion à {BOOTSTRAP_SERVERS}, topic={TOPIC}")
    for event in events:
        event["ts"] = datetime.now(timezone.utc).isoformat()
        key = event.get("order_id", "unknown")
        future = producer.send(TOPIC, key=key, value=event)
        metadata = future.get(timeout=10)
        print(
            f"Envoyé: {event} "
            f"(partition={metadata.partition}, offset={metadata.offset})"
        )
        time.sleep(0.3)

    producer.flush()
    producer.close()
    print("Terminé.")


if __name__ == "__main__":
    main()
