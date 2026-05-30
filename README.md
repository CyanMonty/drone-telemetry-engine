# drone-telemetry-engine

A real-time drone swarm telemetry pipeline. Drones publish position and health data to Kafka; consumers persist to TimescaleDB; Grafana visualises the fleet. A dedicated proximity consumer detects and alerts when drones fly too close together.

## Architecture

```
Simulator ──► Kafka (drone-telemetry) ──► Consumer ──────────────► TimescaleDB
                                     └──► Proximity Consumer ──► TimescaleDB
                                                              └──► Kafka (proximity-alerts)
                                                                        │
TimescaleDB ◄──────────────────────────────────────────────────────────┘
     │
Grafana dashboards
```

| Service | Description |
|---|---|
| `simulator` | Generates synthetic drone telemetry (scale mode) or bridges real PX4 SITL drones (dev mode) |
| `consumer` | Persists telemetry to TimescaleDB; runs anomaly detection and DLQ validation |
| `proximity-consumer` | Detects when any two drones are within a configurable distance and publishes alerts |
| `kafka` | KRaft-mode Apache Kafka broker (no ZooKeeper) |
| `kafka-ui` | Web UI for inspecting topics and messages |
| `timescaledb` | TimescaleDB (PostgreSQL + TimescaleDB + PostGIS) for time-series storage |
| `grafana` | Pre-provisioned dashboards for fleet telemetry and proximity alerts |

---

## Quick start

### Prerequisites

- Docker and Docker Compose
- Ports `3000`, `5432`, `8080`, `9094` free on the host

### 1. Start the stack

```bash
docker compose up --build
```

This starts Kafka, TimescaleDB, Grafana, the simulator (100 synthetic drones), the telemetry consumer, and the proximity consumer.

### 2. Open Grafana

Navigate to [http://localhost:3000](http://localhost:3000).

Default credentials: `admin` / `admin`

Three dashboards are pre-provisioned:

| Dashboard | What it shows |
|---|---|
| **Drone Telemetry** | Per-drone altitude, speed, battery, heading over time |
| **Drone Map** | Live 2-D map of the fleet using lat/lon from TimescaleDB |
| **Streaming Test Metrics** | Throughput and latency data from pipeline load tests |

### 3. Inspect Kafka topics

Kafka UI is at [http://localhost:8080](http://localhost:8080).

Topics created automatically:
- `drone-telemetry` — raw telemetry from the simulator
- `proximity-alerts` — alert records when drones are within the threshold distance

---

## Configuration

All settings are passed as environment variables. The defaults in `docker-compose.yml` work out of the box.

### Simulator

| Variable | Default | Description |
|---|---|---|
| `RUN_MODE` | `scale` | `scale` for synthetic drones, `dev` for PX4 SITL bridge |
| `NUM_DRONES` | `100` | Number of synthetic drones (scale mode only) |
| `TELEMETRY_RATE_HZ` | `2` | Telemetry publish rate per drone |
| `FAULT_RATE` | `0.0` | Fraction of records to corrupt (0.0 = disabled, 0.05 = 5%) |
| `KAFKA_TOPIC` | `drone-telemetry` | Target Kafka topic |
| `PX4_ADDRESSES` | `udp://px4-swarm:14540,...` | MAVSDK addresses for PX4 instances (dev mode) |

### Consumer

| Variable | Default | Description |
|---|---|---|
| `KAFKA_GROUP_ID` | `telemetry-consumer` | Kafka consumer group ID |
| `KAFKA_TOPIC` | `drone-telemetry` | Topic to consume |

### Proximity consumer

| Variable | Default | Description |
|---|---|---|
| `PROXIMITY_THRESHOLD_M` | `50.0` | Alert distance in metres |
| `PROXIMITY_COOLDOWN_S` | `10.0` | Minimum seconds between repeated alerts for the same pair |
| `KAFKA_ALERTS_TOPIC` | `proximity-alerts` | Topic where alerts are published |
| `KAFKA_GROUP_ID` | `proximity-consumer` | Kafka consumer group ID |

### Database / Grafana

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_DB` | `telemetry` | Database name |
| `POSTGRES_USER` | `telemetry` | Database user |
| `POSTGRES_PASSWORD` | `telemetry` | Database password |
| `GRAFANA_USER` | `admin` | Grafana admin username |
| `GRAFANA_PASSWORD` | `admin` | Grafana admin password |

Override any variable by creating a `.env` file in the project root, e.g.:

```env
NUM_DRONES=50
FAULT_RATE=0.05
PROXIMITY_THRESHOLD_M=30.0
POSTGRES_PASSWORD=mysecretpassword
```

---

## Dev mode (real PX4 SITL drones)

Dev mode connects the simulator to PX4 Software-In-The-Loop instances instead of generating synthetic data.

```bash
docker compose --profile dev up --build
```

The `px4-swarm` service starts `NUM_DRONES` PX4 instances. Each instance exposes a MAVSDK UDP port starting at `14540`. The simulator bridges each instance to Kafka.

| Variable | Default | Description |
|---|---|---|
| `PX4_NUM_DRONES` | `3` | Number of PX4 SITL instances |
| `PX4_VEHICLE` | `gz_x500` | Vehicle model |
| `PX4_WORLD` | `default` | Gazebo world |
| `PX4_DRONE_SPACING` | `2` | Spacing between spawned vehicles (metres) |

---

## Fault injection

Set `FAULT_RATE` to a value between `0.0` and `1.0` to have the simulator corrupt a fraction of outgoing records. Faults injected:

| Fault type | What happens | Caught by |
|---|---|---|
| `CORRUPT_PAYLOAD` | Removes a required field | DLQ validator |
| `ANOMALOUS_BATTERY` | Battery set near zero | Anomaly detector |
| `ANOMALOUS_ALTITUDE` | Altitude set above ceiling | Anomaly detector |
| `ANOMALOUS_SPEED` | Speed set above max | Anomaly detector |
| `STALE_TIMESTAMP` | Timestamp set 2 hours in the past | Anomaly detector |
| `DUPLICATE` | Record returned unchanged | (simulates re-delivery) |

---

## Anomaly detection thresholds

Configured as constants in `consumer/anomaly_detector.py`:

| Check | Threshold |
|---|---|
| Low battery | < 15% |
| Altitude too high | > 150 m |
| Altitude too low | < 10 m |
| Speed too high | > 25 m/s |

---

## Running tests

Install test dependencies:

```bash
pip install -r tests/requirements-test.txt
```

Run unit tests (no external dependencies):

```bash
pytest -m unit
```

Run all tests including integration tests (requires Docker for Testcontainers):

```bash
pytest
```

Test reports are written to `reports/test-report.html`.

---

## Database schema

Two hypertables are created automatically on first run:

### `drone_telemetry`

| Column | Type | Description |
|---|---|---|
| `time` | `TIMESTAMPTZ` | Partition key |
| `drone_id` | `TEXT` | Unique drone identifier |
| `lat`, `lon` | `DOUBLE PRECISION` | WGS-84 position |
| `alt` | `DOUBLE PRECISION` | Altitude in metres |
| `speed` | `DOUBLE PRECISION` | Speed in m/s |
| `heading` | `DOUBLE PRECISION` | Heading in degrees |
| `battery_level` | `DOUBLE PRECISION` | Battery percentage |
| `motor_status` | `JSONB` | Per-motor output values |
| `payload` | `JSONB` | Raw telemetry payload |

### `proximity_alerts`

Stores every alert emitted by the proximity consumer, keyed on the drone pair and timestamp.

---

## Stopping the stack

```bash
docker compose down
```

To also remove persisted data volumes:

```bash
docker compose down -v
```

> **Note:** If you switch from `timescale/timescaledb` to `timescale/timescaledb-ha` (which bundles PostGIS), run `docker compose down -v` first to drop the old volume.
