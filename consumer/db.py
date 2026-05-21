import json
import logging

import psycopg2

log = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS drone_telemetry (
    time          TIMESTAMPTZ      NOT NULL,
    drone_id      TEXT             NOT NULL,
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION,
    alt           DOUBLE PRECISION,
    speed         DOUBLE PRECISION,
    heading       DOUBLE PRECISION,
    battery_level DOUBLE PRECISION,
    motor_status  JSONB,
    payload       JSONB
);
"""

_CREATE_HYPERTABLE = """
SELECT create_hypertable('drone_telemetry', 'time', if_not_exists => TRUE);
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_drone_telemetry_drone_id
    ON drone_telemetry (drone_id, time DESC);
"""

_INSERT = """
INSERT INTO drone_telemetry
    (time, drone_id, lat, lon, alt, speed, heading, battery_level, motor_status, payload)
VALUES
    (%(timestamp)s, %(drone_id)s, %(lat)s, %(lon)s, %(alt)s, %(speed)s,
     %(heading)s, %(battery_level)s, %(motor_status)s, %(payload)s);
"""


_CREATE_STREAMING_METRICS = """
CREATE TABLE IF NOT EXISTS streaming_metrics (
    time              TIMESTAMPTZ      NOT NULL DEFAULT now(),
    test_name         TEXT             NOT NULL,
    message_count     INTEGER,
    throughput_msg_s  DOUBLE PRECISION,
    latency_p50_ms    DOUBLE PRECISION,
    latency_p95_ms    DOUBLE PRECISION,
    error_rate        DOUBLE PRECISION,
    notes             TEXT
);
"""

_CREATE_STREAMING_METRICS_HYPERTABLE = """
SELECT create_hypertable('streaming_metrics', 'time', if_not_exists => TRUE);
"""

_INSERT_STREAMING_METRICS = """
INSERT INTO streaming_metrics
    (test_name, message_count, throughput_msg_s, latency_p50_ms, latency_p95_ms, error_rate, notes)
VALUES
    (%(test_name)s, %(message_count)s, %(throughput_msg_s)s,
     %(latency_p50_ms)s, %(latency_p95_ms)s, %(error_rate)s,
     %(notes)s);
"""


def init_db(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE)
        cur.execute(_CREATE_HYPERTABLE)
        cur.execute(_CREATE_INDEX)
        cur.execute(_CREATE_STREAMING_METRICS)
        cur.execute(_CREATE_STREAMING_METRICS_HYPERTABLE)
    conn.commit()
    log.info("Database schema ready.")


def insert_streaming_metrics(conn: psycopg2.extensions.connection, record: dict) -> None:
    row = {
        "test_name": record["test_name"],
        "message_count": record.get("message_count"),
        "throughput_msg_s": record.get("throughput_msg_s"),
        "latency_p50_ms": record.get("latency_p50_ms"),
        "latency_p95_ms": record.get("latency_p95_ms"),
        "error_rate": record.get("error_rate"),
        "notes": record.get("notes"),
    }
    with conn.cursor() as cur:
        cur.execute(_INSERT_STREAMING_METRICS, row)
    conn.commit()


def insert_telemetry(conn: psycopg2.extensions.connection, record: dict) -> None:
    row = {
        **record,
        "motor_status": json.dumps(record["motor_status"]),
        "payload": json.dumps(record["payload"]),
    }
    with conn.cursor() as cur:
        cur.execute(_INSERT, row)
    conn.commit()
