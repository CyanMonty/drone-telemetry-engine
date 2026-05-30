import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError, Producer

from db import init_db
from proximity_detector import check_proximity

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "drone-telemetry")
KAFKA_ALERTS_TOPIC = os.environ.get("KAFKA_ALERTS_TOPIC", "proximity-alerts")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "proximity-consumer")

POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

PROXIMITY_THRESHOLD_M = float(os.environ.get("PROXIMITY_THRESHOLD_M", "50.0"))
PROXIMITY_COOLDOWN_S = float(os.environ.get("PROXIMITY_COOLDOWN_S", "10.0"))


def connect_db(retries: int = 10, delay: float = 3.0) -> psycopg2.extensions.connection:
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            log.info("Connected to TimescaleDB.")
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("DB not ready (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to TimescaleDB after {retries} attempts.")


def main() -> None:
    conn = connect_db()
    init_db(conn)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    log.info(
        "Proximity consumer started | topic=%s threshold=%.1fm cooldown=%.1fs",
        KAFKA_TOPIC,
        PROXIMITY_THRESHOLD_M,
        PROXIMITY_COOLDOWN_S,
    )

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
                continue
            try:
                record = json.loads(msg.value())
                alerts = check_proximity(conn, record, PROXIMITY_THRESHOLD_M, PROXIMITY_COOLDOWN_S)
                for alert in alerts:
                    payload = json.dumps(alert)
                    producer.produce(
                        KAFKA_ALERTS_TOPIC,
                        key=alert["drone_id_a"],
                        value=payload,
                    )
                    log.warning(
                        "PROXIMITY_ALERT | %s <-> %s | %.1fm",
                        alert["drone_id_a"],
                        alert["drone_id_b"],
                        alert["distance_m"],
                    )
                if alerts:
                    producer.poll(0)
            except Exception as exc:
                log.error("Failed to process message: %s", exc)
                conn.rollback()
    finally:
        consumer.close()
        producer.flush()
        conn.close()


if __name__ == "__main__":
    main()
