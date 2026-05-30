# simulator — agent notes

## Purpose

Produces drone telemetry records and publishes them to a Kafka topic. Supports two run modes:

- **scale** (default) — generates synthetic telemetry for `NUM_DRONES` virtual drones using physics-based simulation (`drone_physics.py`).
- **dev** — connects to real PX4 SITL instances via MAVSDK (`drone.py`), one coroutine per drone.

## Key files

| File | Role |
|---|---|
| `main.py` | Entry point; selects run mode, wires up asyncio tasks |
| `drone_physics.py` | Synthetic drone state machine (position, speed, heading, battery) |
| `drone.py` | MAVSDK bridge for PX4 SITL (dev mode only) |
| `fault_injector.py` | Probabilistic fault injection into outgoing records |
| `demo.py` | Standalone demo script (not used in Docker) |

## Telemetry record schema

Every record published to Kafka is a JSON object with these fields:

```json
{
  "drone_id": "drone-042",
  "timestamp": "2026-05-30T12:00:00Z",
  "lat": 40.7128,
  "lon": -74.0060,
  "alt": 50.0,
  "speed": 12.3,
  "heading": 270.0,
  "battery_level": 87.5,
  "motor_status": [{"motor_id": 1, "output": 0.72}, ...],
  "payload": {}
}
```

## Fault injection

`FaultInjector.maybe_inject(record)` randomly replaces records with a faulty version at `fault_rate` probability. Fault types: `CORRUPT_PAYLOAD`, `ANOMALOUS_BATTERY`, `ANOMALOUS_ALTITUDE`, `ANOMALOUS_SPEED`, `STALE_TIMESTAMP`, `DUPLICATE`. Disabled when `FAULT_RATE=0.0` (default).

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `RUN_MODE` | `scale` | `scale` or `dev` |
| `NUM_DRONES` | `100` | Scale mode only |
| `TELEMETRY_RATE_HZ` | `2` | Publish rate per drone |
| `FAULT_RATE` | `0.0` | 0.0–1.0; fraction of records to corrupt |
| `KAFKA_BOOTSTRAP_SERVERS` | required | e.g. `kafka:9092` |
| `KAFKA_TOPIC` | `drone-telemetry` | Target topic |
| `PX4_ADDRESSES` | `udp://px4-swarm:14540,...` | Dev mode MAVSDK addresses |

## Agent guidance

- Do not add new fault types unless `FaultType` enum and `_DEFAULT_FAULTS` list in `fault_injector.py` are both updated.
- Scale-mode drone state lives entirely in `drone_physics.py`; changes to position/battery logic go there.
- `main.py` uses a single shared asyncio queue fanned out to a Kafka writer and a stats logger — keep the fan-out pattern intact when adding new sinks.
- No database dependency — this service only writes to Kafka.
