#!/usr/bin/env python3
"""Consommateur d'exemple DonnÉlite — Kafka + Schema Registry (Avro)."""

from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.getenv("TOPIC_NAME", "logistique.shipment_event.v1")
GROUP = os.getenv("CONSUMER_GROUP", "donnelite-onboarding-scoring")
SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "shipment_event.avsc"


def main() -> None:
    schema_str = SCHEMA_PATH.read_text(encoding="utf-8")
    sr_client = SchemaRegistryClient({"url": SR_URL})
    avro_deserializer = AvroDeserializer(sr_client, schema_str)

    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": avro_deserializer,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    print(f"Écoute de {TOPIC} (group={GROUP}) — Ctrl+C pour arrêter")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erreur consumer : {msg.error()}")
                continue
            print("Message reçu :")
            print(json.dumps(msg.value(), indent=2, ensure_ascii=False, default=str))
            consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        print("\nArrêt demandé.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
