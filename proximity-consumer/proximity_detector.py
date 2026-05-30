"""
Proximity detection for the drone fleet.

check_proximity() is the main entry point: given a freshly-received telemetry
record and an open DB connection, it upserts the drone's position and returns
alert dicts for any neighbouring drones within threshold_m metres (subject to
the cooldown so the same pair is not spammed).

haversine_distance() is a pure-Python fallback used in unit tests (no DB needed).
"""

import math

import psycopg2

import db


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance in metres between two WGS-84 points."""
    R = 6_371_000.0  # Earth radius in metres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def check_proximity(
    conn: psycopg2.extensions.connection,
    record: dict,
    threshold_m: float,
    cooldown_s: float,
) -> list[dict]:
    """
    Upsert *record*'s position and return alert dicts for every nearby drone
    that has not been alerted within *cooldown_s* seconds.
    """
    db.upsert_position(conn, record)

    nearby = db.find_nearby(conn, record["drone_id"], threshold_m)
    alerts = []

    for neighbour in nearby:
        if db.was_recently_alerted(conn, record["drone_id"], neighbour["drone_id"], cooldown_s):
            continue

        alert = {
            "timestamp": record["timestamp"],
            "drone_id_a": record["drone_id"],
            "drone_id_b": neighbour["drone_id"],
            "distance_m": round(neighbour["distance_m"], 2),
            "threshold_m": threshold_m,
            "lat_a": record["lat"],
            "lon_a": record["lon"],
            "lat_b": neighbour["lat"],
            "lon_b": neighbour["lon"],
        }
        db.insert_alert(conn, alert)
        alerts.append(alert)

    return alerts
