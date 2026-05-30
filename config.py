"""
Global configuration for the drone telemetry engine.
All values are read from environment variables with sensible defaults.
"""

import os

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC: str = os.getenv("RAW_TOPIC", "px4.telemetry.raw")
PARSED_TOPIC: str = os.getenv("PARSED_TOPIC", "px4.telemetry.parsed")

# ---------------------------------------------------------------------------
# Swarm simulation
# ---------------------------------------------------------------------------
NUM_DRONES: int = int(os.getenv("NUM_DRONES", "5"))
SIM_UPDATE_HZ: float = float(os.getenv("SIM_UPDATE_HZ", "2.0"))

# Home / launch point (default: San Francisco Crissy Field area)
HOME_LAT: float = float(os.getenv("HOME_LAT", "37.8044"))
HOME_LON: float = float(os.getenv("HOME_LON", "-122.4679"))
HOME_ALT: float = float(os.getenv("HOME_ALT", "10.0"))  # metres AMSL

# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
DASHBOARD_REFRESH_MS: int = int(os.getenv("DASHBOARD_REFRESH_MS", "2000"))
MAX_HISTORY_POINTS: int = int(os.getenv("MAX_HISTORY_POINTS", "120"))
