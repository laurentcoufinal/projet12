#!/usr/bin/env python3
"""Gateway WebSocket → Kafka pour clients navigateur (ex. Angular)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "demo-events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "angular-gateway")

app = FastAPI(title="Kafka WebSocket Gateway")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200", "http://127.0.0.1:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_loop: asyncio.AbstractEventLoop | None = None
_producer: KafkaProducer | None = None
_consumer_thread: threading.Thread | None = None
_stop_consumer = threading.Event()
_clients: set[WebSocket] = set()
_clients_lock = asyncio.Lock()


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        logger.info("Producer Kafka prêt (%s)", KAFKA_BOOTSTRAP)
    return _producer


async def _broadcast(payload: dict[str, Any]) -> None:
    data = json.dumps(payload)
    async with _clients_lock:
        dead: list[WebSocket] = []
        for ws in _clients:
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            _clients.discard(ws)


def _consumer_loop() -> None:
    """Lit Kafka et pousse les messages vers le loop asyncio."""
    while not _stop_consumer.is_set():
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                key_deserializer=lambda b: b.decode("utf-8") if b else None,
            )
            logger.info("Consumer Kafka démarré (topic=%s, group=%s)", TOPIC, GROUP_ID)
            try:
                while not _stop_consumer.is_set():
                    records = consumer.poll(timeout_ms=1000)
                    for _tp, messages in records.items():
                        for msg in messages:
                            envelope = {
                                "source": "kafka",
                                "topic": msg.topic,
                                "partition": msg.partition,
                                "offset": msg.offset,
                                "key": msg.key,
                                "value": msg.value,
                            }
                            if _loop is not None:
                                asyncio.run_coroutine_threadsafe(
                                    _broadcast(envelope), _loop
                                )
            finally:
                consumer.close()
            break
        except Exception as exc:
            logger.warning("Consumer Kafka erreur: %s — nouvel essai dans 3s", exc)
            _stop_consumer.wait(3)


@app.on_event("startup")
async def on_startup() -> None:
    global _loop, _consumer_thread
    _loop = asyncio.get_running_loop()
    _stop_consumer.clear()
    _get_producer()
    _consumer_thread = threading.Thread(target=_consumer_loop, name="kafka-consumer", daemon=True)
    _consumer_thread.start()
    logger.info("Gateway démarrée")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global _producer
    _stop_consumer.set()
    if _producer is not None:
        _producer.close()
        _producer = None
    logger.info("Gateway arrêtée")


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "ok",
        "kafka_bootstrap": KAFKA_BOOTSTRAP,
        "topic": TOPIC,
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    async with _clients_lock:
        _clients.add(websocket)
    logger.info("Client WS connecté (%d clients)", len(_clients))

    try:
        await websocket.send_text(
            json.dumps(
                {
                    "source": "gateway",
                    "type": "connected",
                    "topic": TOPIC,
                    "message": "Connexion WebSocket établie vers le bridge Kafka",
                }
            )
        )
        while True:
            raw = await websocket.receive_text()
            try:
                body = json.loads(raw)
            except json.JSONDecodeError:
                body = {"text": raw}

            if isinstance(body, dict) and "text" in body and "type" not in body:
                event: dict[str, Any] = {
                    "type": "ui.message",
                    "text": body["text"],
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
                key = body.get("key") or "angular"
            elif isinstance(body, dict):
                event = {**body, "ts": body.get("ts") or datetime.now(timezone.utc).isoformat()}
                key = body.get("key") or body.get("order_id") or "angular"
            else:
                event = {
                    "type": "ui.message",
                    "text": str(body),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
                key = "angular"

            producer = _get_producer()
            future = producer.send(TOPIC, key=str(key), value=event)
            meta = future.get(timeout=10)
            producer.flush()
            await websocket.send_text(
                json.dumps(
                    {
                        "source": "gateway",
                        "type": "published",
                        "topic": TOPIC,
                        "partition": meta.partition,
                        "offset": meta.offset,
                        "value": event,
                    }
                )
            )
    except WebSocketDisconnect:
        logger.info("Client WS déconnecté")
    finally:
        async with _clients_lock:
            _clients.discard(websocket)
