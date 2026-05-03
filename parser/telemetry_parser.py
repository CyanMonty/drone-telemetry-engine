"""
Telemetry parser — Kafka consumer / producer.

Reads raw MAVLink-style JSON from RAW_TOPIC, enriches each message with
computed fields (ground speed, battery alert level, GPS health) and
republishes to PARSED_TOPIC.
"""

from __future__ import annotations

import json
import logging
import math
import signal
import sys
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

import config as cfg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("telemetry_parser")

_running = True


def _signal_handler(sig, frame) -> None:  # noqa: ANN001
    global _running
    logger.info("Shutdown signal received — stopping parser…")
    _running = False


# ---------------------------------------------------------------------------
# Enrichment logic
# ---------------------------------------------------------------------------

def _battery_alert(remaining_pct: int) -> str:
    if remaining_pct < 15:
        return "CRITICAL"
    if remaining_pct < 25:
        return "WARNING"
    return "OK"


def _gps_status(fix_type: int) -> str:
    if fix_type < 2:
        return "NO_FIX"
    if fix_type == 2:
        return "2D_FIX"
    return "3D_FIX"


def enrich(raw: dict) -> dict:
    """
    Return a new dict that is a shallow copy of *raw* augmented with a
    ``computed`` sub-dict containing derived telemetry fields.

    This function is intentionally side-effect-free so it can be tested
    without Kafka.
    """
    parsed = dict(raw)
    parsed["parsed_at"] = time.time()

    pos = raw.get("position", {})
    vx = pos.get("vx", 0.0)
    vy = pos.get("vy", 0.0)
    vz = pos.get("vz", 0.0)
    ground_speed_ms = math.sqrt(vx ** 2 + vy ** 2)
    total_speed_ms = math.sqrt(vx ** 2 + vy ** 2 + vz ** 2)

    battery = raw.get("battery", {})
    remaining_pct = battery.get("remaining_pct", 100)

    gps = raw.get("gps", {})
    fix_type = gps.get("fix_type", 0)
    satellites = gps.get("satellites_visible", 0)

    parsed["computed"] = {
        "ground_speed_ms": round(ground_speed_ms, 2),
        "ground_speed_kmh": round(ground_speed_ms * 3.6, 2),
        "total_speed_ms": round(total_speed_ms, 2),
        "battery_alert": _battery_alert(remaining_pct),
        "gps_status": _gps_status(fix_type),
        "gps_healthy": fix_type >= 3 and satellites >= 6,
    }

    return parsed


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def _connect(retries: int = 10, backoff_s: float = 5.0) -> tuple[KafkaConsumer, KafkaProducer]:
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                cfg.RAW_TOPIC,
                bootstrap_servers=cfg.KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id="telemetry-parser",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                consumer_timeout_ms=500,
            )
            producer = KafkaProducer(
                bootstrap_servers=cfg.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                linger_ms=50,
            )
            logger.info("Connected to Kafka at %s", cfg.KAFKA_BOOTSTRAP_SERVERS)
            return consumer, producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not reachable (attempt %d/%d) — retrying in %ds…",
                attempt,
                retries,
                backoff_s,
            )
            time.sleep(backoff_s)

    logger.error("Could not connect to Kafka after %d attempts — exiting.", retries)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run() -> None:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    logger.info(
        "Telemetry parser | %s → %s", cfg.RAW_TOPIC, cfg.PARSED_TOPIC
    )

    consumer, producer = _connect()
    msg_count = 0

    while _running:
        try:
            records = consumer.poll(timeout_ms=1000)
        except Exception as exc:
            logger.error("Poll error: %s", exc)
            continue

        for _tp, messages in records.items():
            for msg in messages:
                try:
                    raw = msg.value
                    parsed = enrich(raw)
                    producer.send(
                        cfg.PARSED_TOPIC,
                        key=parsed.get("drone_id", "unknown"),
                        value=parsed,
                    )
                    msg_count += 1
                    if msg_count % 100 == 0:
                        logger.info("Parsed %d messages", msg_count)
                except Exception as exc:  # noqa: BLE001
                    logger.error("Failed to process message: %s", exc)

        producer.flush()

    consumer.close()
    producer.close()
    logger.info("Parser stopped. Total messages processed: %d", msg_count)


if __name__ == "__main__":
    run()
