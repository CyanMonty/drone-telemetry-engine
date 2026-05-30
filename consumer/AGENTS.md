# consumer — agent notes

## Purpose

Reads telemetry records from the `drone-telemetry` Kafka topic, validates them, runs anomaly detection, and persists them to the `drone_telemetry` TimescaleDB hypertable. Malformed records are flagged via the DLQ handler.

## Key files

| File | Role |
|---|---|
| `main.py` | Entry point; Kafka consumer loop; wires DB, anomaly detector, DLQ |
| `db.py` | `init_db()`, `insert_telemetry()`, streaming metrics helpers |
| `anomaly_detector.py` | Threshold-based anomaly detection; returns alert dicts |
| `dlq_handler.py` | `validate_record()` — checks required fields before DB insert |
| `windowed_agg.py` | Rolling-window per-drone averages (used in tests/dashboards) |

## Database schema

Table `drone_telemetry` (TimescaleDB hypertable, partitioned by `time`):

| Column | Type |
|---|---|
| `time` | `TIMESTAMPTZ` (partition key) |
| `drone_id` | `TEXT` |
| `lat`, `lon`, `alt`, `speed`, `heading`, `battery_level` | `DOUBLE PRECISION` |
| `motor_status` | `JSONB` |
| `payload` | `JSONB` |

Index: `(drone_id, time DESC)`.

A second hypertable `streaming_metrics` stores pipeline load-test results.

## Anomaly thresholds

Defined as module-level constants in `anomaly_detector.py`:

| Check | Constant | Value |
|---|---|---|
| Low battery | `BATTERY_LOW` | 15% |
| Max altitude | `ALT_MAX` | 150 m |
| Min altitude | `ALT_MIN` | 10 m |
| Max speed | `SPEED_MAX` | 25 m/s |

## DLQ validation

`validate_record(record)` returns `(True, None)` or `(False, reason)`. Required fields: `drone_id`, `timestamp`, `lat`, `lon`, `alt`, `speed`, `heading`, `battery_level`. Extra fields are allowed.

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | required | |
| `KAFKA_TOPIC` | `drone-telemetry` | |
| `KAFKA_GROUP_ID` | `telemetry-consumer` | |
| `POSTGRES_HOST` | required | |
| `POSTGRES_DB` | required | |
| `POSTGRES_USER` | required | |
| `POSTGRES_PASSWORD` | required | |

## Agent guidance

- `init_db()` is idempotent — safe to call on every restart; no migration tool.
- `anomaly_detector.py` is pure Python with no side effects — easy to unit test in isolation.
- `windowed_agg.py` is also stateless with respect to external dependencies; unit tests do not need a DB or Kafka.
- `dlq_handler.py` does not write to a DLQ topic itself — the caller in `main.py` decides routing.
- When adding new anomaly checks, add a constant at the top of `anomaly_detector.py` and a new block in `detect_anomalies()`.
- DB errors in the consumer loop roll back the transaction but do not crash the consumer — the next message starts fresh.
