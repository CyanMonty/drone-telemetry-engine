"""
Database layer for the proximity consumer.

Schema:
  drone_latest_positions  — one row per drone, upserted on each message.
                            Uses a PostGIS GEOGRAPHY(POINT) for ST_DWithin.
  proximity_alerts        — TimescaleDB hypertable; one row per alert event.
"""

import logging

import psycopg2

log = logging.getLogger(__name__)

_ENABLE_EXTENSIONS = """
CREATE EXTENSION IF NOT EXISTS postgis;
"""

_CREATE_POSITIONS = """
CREATE TABLE IF NOT EXISTS drone_latest_positions (
    drone_id   TEXT        PRIMARY KEY,
    position   GEOGRAPHY(POINT, 4326) NOT NULL,
    alt        DOUBLE PRECISION,
    speed      DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL
);
"""

_CREATE_ALERTS = """
CREATE TABLE IF NOT EXISTS proximity_alerts (
    time          TIMESTAMPTZ      NOT NULL DEFAULT now(),
    drone_id_a    TEXT             NOT NULL,
    drone_id_b    TEXT             NOT NULL,
    distance_m    DOUBLE PRECISION NOT NULL,
    threshold_m   DOUBLE PRECISION NOT NULL,
    lat_a         DOUBLE PRECISION,
    lon_a         DOUBLE PRECISION,
    lat_b         DOUBLE PRECISION,
    lon_b         DOUBLE PRECISION
);
"""

_CREATE_ALERTS_HYPERTABLE = """
SELECT create_hypertable('proximity_alerts', 'time', if_not_exists => TRUE);
"""

_CREATE_ALERTS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_proximity_alerts_drones
    ON proximity_alerts (drone_id_a, drone_id_b, time DESC);
"""

_UPSERT_POSITION = """
INSERT INTO drone_latest_positions (drone_id, position, alt, speed, updated_at)
VALUES (
    %(drone_id)s,
    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::GEOGRAPHY,
    %(alt)s,
    %(speed)s,
    %(timestamp)s
)
ON CONFLICT (drone_id) DO UPDATE SET
    position   = EXCLUDED.position,
    alt        = EXCLUDED.alt,
    speed      = EXCLUDED.speed,
    updated_at = EXCLUDED.updated_at;
"""

# Return all drones within threshold_m metres, excluding the source drone.
# Also returns the geodesic distance so we can log it.
_FIND_NEARBY = """
SELECT
    other.drone_id,
    ST_X(other.position::GEOMETRY)                       AS lon,
    ST_Y(other.position::GEOMETRY)                       AS lat,
    ST_Distance(src.position, other.position)            AS distance_m
FROM drone_latest_positions src
JOIN drone_latest_positions other
    ON src.drone_id = %(drone_id)s
    AND other.drone_id <> %(drone_id)s
    AND ST_DWithin(src.position, other.position, %(threshold_m)s)
WHERE src.drone_id = %(drone_id)s;
"""

_INSERT_ALERT = """
INSERT INTO proximity_alerts
    (time, drone_id_a, drone_id_b, distance_m, threshold_m, lat_a, lon_a, lat_b, lon_b)
VALUES
    (%(timestamp)s, %(drone_id_a)s, %(drone_id_b)s, %(distance_m)s, %(threshold_m)s,
     %(lat_a)s, %(lon_a)s, %(lat_b)s, %(lon_b)s);
"""

# True if an alert for this pair was written within cooldown_s seconds.
_WAS_RECENTLY_ALERTED = """
SELECT 1 FROM proximity_alerts
WHERE drone_id_a = %(drone_id_a)s
  AND drone_id_b = %(drone_id_b)s
  AND time > now() - (%(cooldown_s)s || ' seconds')::INTERVAL
LIMIT 1;
"""


def init_db(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(_ENABLE_EXTENSIONS)
        cur.execute(_CREATE_POSITIONS)
        cur.execute(_CREATE_ALERTS)
        cur.execute(_CREATE_ALERTS_HYPERTABLE)
        cur.execute(_CREATE_ALERTS_INDEX)
    conn.commit()
    log.info("Proximity schema ready.")


def upsert_position(conn: psycopg2.extensions.connection, record: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(_UPSERT_POSITION, record)
    conn.commit()


def find_nearby(
    conn: psycopg2.extensions.connection,
    drone_id: str,
    threshold_m: float,
) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(_FIND_NEARBY, {"drone_id": drone_id, "threshold_m": threshold_m})
        rows = cur.fetchall()
    return [
        {"drone_id": row[0], "lon": row[1], "lat": row[2], "distance_m": row[3]}
        for row in rows
    ]


def was_recently_alerted(
    conn: psycopg2.extensions.connection,
    drone_id_a: str,
    drone_id_b: str,
    cooldown_s: float,
) -> bool:
    # Store pairs in canonical order so (A,B) and (B,A) hit the same rows.
    a, b = sorted([drone_id_a, drone_id_b])
    with conn.cursor() as cur:
        cur.execute(_WAS_RECENTLY_ALERTED, {"drone_id_a": a, "drone_id_b": b, "cooldown_s": cooldown_s})
        return cur.fetchone() is not None


def insert_alert(conn: psycopg2.extensions.connection, alert: dict) -> None:
    # Canonical pair order for deduplication queries.
    a, b = sorted([alert["drone_id_a"], alert["drone_id_b"]])
    row = {**alert, "drone_id_a": a, "drone_id_b": b}
    with conn.cursor() as cur:
        cur.execute(_INSERT_ALERT, row)
    conn.commit()
