# proximity-consumer — agent notes

## Purpose

Reads telemetry records from the `drone-telemetry` Kafka topic, detects when any two drones are within a configurable distance of each other, persists alerts to TimescaleDB, and publishes alert records to the `proximity-alerts` Kafka topic.

## Key files

| File | Role |
|---|---|
| `main.py` | Entry point; Kafka consumer + producer loop |
| `proximity_detector.py` | `check_proximity()` — upserts position, queries neighbours, deduplicates via cooldown |
| `db.py` | `init_db()`, `upsert_position()`, `find_nearby()`, `was_recently_alerted()`, `insert_alert()` |

## How detection works

1. On each telemetry record, `db.upsert_position()` writes/updates the drone's current position in a `drone_positions` table.
2. `db.find_nearby()` queries using PostGIS `ST_DWithin` (geography type) for accurate spherical distance — this is why `timescale/timescaledb-ha` (which bundles PostGIS) is required.
3. For each nearby drone, `db.was_recently_alerted()` checks `proximity_alerts` to suppress repeat alerts within `PROXIMITY_COOLDOWN_S` seconds.
4. New alerts are inserted via `db.insert_alert()` and returned as dicts.

## Alert record schema

```json
{
  "timestamp": "2026-05-30T12:00:00Z",
  "drone_id_a": "drone-001",
  "drone_id_b": "drone-042",
  "distance_m": 23.7,
  "threshold_m": 50.0,
  "lat_a": 40.7128,
  "lon_a": -74.0060,
  "lat_b": 40.7130,
  "lon_b": -74.0058
}
```

## `haversine_distance()`

A pure-Python fallback in `proximity_detector.py` used exclusively by unit tests (no DB required). The production path always uses PostGIS.

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | required | |
| `KAFKA_TOPIC` | `drone-telemetry` | Input topic |
| `KAFKA_ALERTS_TOPIC` | `proximity-alerts` | Output topic |
| `KAFKA_GROUP_ID` | `proximity-consumer` | |
| `POSTGRES_HOST` | required | |
| `POSTGRES_DB` | required | |
| `POSTGRES_USER` | required | |
| `POSTGRES_PASSWORD` | required | |
| `PROXIMITY_THRESHOLD_M` | `50.0` | Alert distance in metres |
| `PROXIMITY_COOLDOWN_S` | `10.0` | Seconds before re-alerting the same pair |

## Agent guidance

- PostGIS is required. Never swap `timescale/timescaledb-ha` for the plain `timescale/timescaledb` image without also replacing the `ST_DWithin` query.
- `check_proximity()` is the sole public entry point of `proximity_detector.py`; tests call it directly with a real or mocked DB connection.
- `haversine_distance()` is kept purely for unit testing — do not wire it into the production path.
- The Kafka producer in `main.py` calls `producer.poll(0)` after each batch of alerts to drain the delivery queue without blocking; do not remove this.
- The consumer and producer share the same event loop iteration (no separate thread) — keep the main loop simple and synchronous.
- Cooldown logic lives in `db.was_recently_alerted()`; changing the cooldown only requires updating the `PROXIMITY_COOLDOWN_S` env variable, not code changes.
