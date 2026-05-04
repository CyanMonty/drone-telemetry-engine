import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError

from db import init_db, insert_telemetry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "drone-telemetry")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "telemetry-consumer")

POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]


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


def main():
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
    log.info("Consuming from topic '%s'", KAFKA_TOPIC)

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
                insert_telemetry(conn, record)
            except Exception as exc:
                log.error("Failed to process message: %s", exc)
                conn.rollback()
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
