#!/usr/bin/env python3
"""Consommateur Kafka : lit en boucle le topic demo-events."""

from __future__ import annotations

import json
import signal
import sys

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "demo-events"
GROUP_ID = "demo-group"


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )

    running = True

    def stop(_signum, _frame) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(
        f"Écoute de {TOPIC} sur {BOOTSTRAP_SERVERS} "
        f"(group={GROUP_ID}). Ctrl+C pour quitter."
    )

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)
            for _tp, messages in records.items():
                for msg in messages:
                    print(
                        f"Reçu: key={msg.key} value={msg.value} "
                        f"(partition={msg.partition}, offset={msg.offset})"
                    )
    finally:
        consumer.close()
        print("Consommateur arrêté.", file=sys.stderr)


if __name__ == "__main__":
    main()
