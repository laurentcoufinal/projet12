#!/usr/bin/env python3
"""Producteur d'exemple DonnÉlite — Kafka + Schema Registry (Avro)."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.getenv("TOPIC_NAME", "logistique.shipment_event.v1")
SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "shipment_event.avsc"


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"ERREUR livraison : {err}")
    else:
        print(
            f"OK topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


def main() -> None:
    schema_str = SCHEMA_PATH.read_text(encoding="utf-8")
    sr_client = SchemaRegistryClient({"url": SR_URL})
    avro_serializer = AvroSerializer(sr_client, schema_str)

    producer = SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
            "acks": "all",
            "enable.idempotence": True,
            "client.id": "donnelite-onboarding-producer",
        }
    )

    event_id = str(uuid.uuid4())
    event = {
        "event_id": event_id,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "producer": "onboarding-producer",
        "tenant_id": "tenant-demo",
        "shipment_id": f"SHP-{event_id[:8]}",
        "status": "IN_TRANSIT",
        "weight_kg": 12.5,
    }

    producer.produce(
        topic=TOPIC,
        key=event["tenant_id"],
        value=event,
        on_delivery=delivery_report,
    )
    producer.flush()
    print("Événement publié :")
    print(json.dumps(event, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
