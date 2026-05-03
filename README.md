# drone-telemetry-engine

> **Drone swarm simulation using PX4 SITL streamed to Kafka and parsed into a real-time telemetry dashboard.**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Docker Compose Stack                             │
│                                                                         │
│   ┌──────────────┐   px4.telemetry.raw   ┌──────────────────────────┐  │
│   │   Simulator  │ ────────────────────► │       Kafka Broker       │  │
│   │  (swarm of N │                       │   (+ Zookeeper)          │  │
│   │    drones)   │                       │                          │  │
│   └──────────────┘                       └────────────┬─────────────┘  │
│                                                        │               │
│   ┌──────────────┐                                     │               │
│   │    Parser    │ ◄───── px4.telemetry.raw ───────────┘               │
│   │  (enrichment)│ ──────────── px4.telemetry.parsed ──────────────►   │
│   └──────────────┘                                                     │
│                                          ┌─────────────────────────┐   │
│                                          │  Streamlit Dashboard    │   │
│                                          │  http://localhost:8501  │   │
│                                          └─────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

### Components

| Service | Description |
|---|---|
| **Simulator** (`sim/swarm_simulator.py`) | Spawns *N* virtual PX4 drones, updates physics at configurable Hz, produces MAVLink-style JSON telemetry to `px4.telemetry.raw` |
| **Parser** (`parser/telemetry_parser.py`) | Kafka consumer that enriches raw messages (ground speed, battery alert level, GPS health) and republishes to `px4.telemetry.parsed` |
| **Dashboard** (`dashboard/app.py`) | Streamlit app with live map, KPI bar, per-drone metric cards, and time-series charts — auto-refreshes every N seconds |
| **Kafka + Zookeeper** | Message broker (Confluent Platform images) |

### Telemetry message format

Each drone publishes a JSON message per tick:

```json
{
  "drone_id": "drone_001",
  "timestamp": 1735000000.0,
  "sequence": 42,
  "heartbeat": { "armed": true, "flight_mode": "AUTO.MISSION", "system_status": "ACTIVE" },
  "position":  { "lat": 37.8044, "lon": -122.4679, "alt_msl": 120.3, "relative_alt": 110.3,
                  "vx": -8.1, "vy": 5.3, "vz": 0.1, "heading_deg": 147.2 },
  "attitude":  { "roll_deg": 5.2, "pitch_deg": -0.3, "yaw_deg": 147.2,
                  "rollspeed": 0.0, "pitchspeed": 0.0, "yawspeed": 0.08 },
  "battery":   { "voltage_v": 15.6, "current_a": 2.1, "remaining_pct": 87, "consumed_mah": 1300.0 },
  "gps":       { "fix_type": 3, "satellites_visible": 13, "eph": 0.8, "epv": 1.2 }
}
```

The parser adds a `computed` block:

```json
{
  "parsed_at": 1735000000.1,
  "computed": {
    "ground_speed_ms": 9.7, "ground_speed_kmh": 34.9, "total_speed_ms": 9.7,
    "battery_alert": "OK",   "gps_status": "3D_FIX",  "gps_healthy": true
  }
}
```

---

## Quick Start

### Prerequisites

* [Docker](https://docs.docker.com/get-docker/) ≥ 24
* [Docker Compose](https://docs.docker.com/compose/install/) v2

### 1 — Clone and launch the full stack

```bash
git clone https://github.com/CyanMonty/drone-telemetry-engine.git
cd drone-telemetry-engine
docker compose up --build
```

Open **http://localhost:8501** to view the live dashboard.

### 2 — Stop

```bash
docker compose down
```

---

## Local Development (without Docker)

Requires Python 3.11+ and a Kafka broker on `localhost:9092`.

```bash
# Install dependencies
pip install -r requirements.txt

# Terminal 1 — simulator
python -m sim.swarm_simulator

# Terminal 2 — parser
python -m parser.telemetry_parser

# Terminal 3 — dashboard
streamlit run dashboard/app.py
```

> **Demo mode**: if Kafka is unreachable the dashboard automatically switches to
> synthetic demo data so you can explore the UI without infrastructure.

---

## Configuration

All settings are controlled via environment variables:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `RAW_TOPIC` | `px4.telemetry.raw` | Topic for raw simulator output |
| `PARSED_TOPIC` | `px4.telemetry.parsed` | Topic for enriched telemetry |
| `NUM_DRONES` | `5` | Number of simulated drones |
| `SIM_UPDATE_HZ` | `2.0` | Telemetry publish rate (Hz) |
| `HOME_LAT` / `HOME_LON` | `37.8044` / `-122.4679` | Swarm home coordinates (SF) |
| `HOME_ALT` | `10.0` | Home altitude AMSL (m) |
| `DASHBOARD_REFRESH_MS` | `2000` | Dashboard auto-refresh interval |
| `MAX_HISTORY_POINTS` | `120` | Chart history depth per drone |

---

## Running Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

Tests cover drone physics (`tests/test_drone.py`) and the enrichment logic
(`tests/test_telemetry_parser.py`). No Kafka broker is required to run tests.

---

## Project Layout

```
drone-telemetry-engine/
├── config.py                   # Centralised configuration
├── sim/
│   ├── drone.py                # Drone physics model
│   └── swarm_simulator.py      # Swarm orchestrator + Kafka producer
├── parser/
│   └── telemetry_parser.py     # Kafka consumer / enrichment / re-publisher
├── dashboard/
│   └── app.py                  # Streamlit real-time dashboard
├── tests/
│   ├── test_drone.py
│   └── test_telemetry_parser.py
├── docker-compose.yml
├── Dockerfile
├── Makefile
└── requirements.txt
```

---

## License

MIT — see [LICENSE](LICENSE).
