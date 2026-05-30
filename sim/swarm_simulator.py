"""
PX4 SITL swarm simulator.

Spawns NUM_DRONES virtual drones, updates their physics at SIM_UPDATE_HZ,
and streams MAVLink-style JSON telemetry to the Kafka topic RAW_TOPIC.
"""

from __future__ import annotations

import json
import logging
import math
import signal
import sys
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

import config as cfg
from sim.drone import Drone, DroneConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("swarm_simulator")

_running = True


def _signal_handler(sig, frame) -> None:  # noqa: ANN001
    global _running
    logger.info("Shutdown signal received — stopping simulator…")
    _running = False


# ---------------------------------------------------------------------------
# Swarm factory
# ---------------------------------------------------------------------------

def build_swarm(num_drones: int) -> list[Drone]:
    """
    Create *num_drones* drones arranged in a ring around the home point.
    Odd-numbered drones orbit clockwise, even-numbered counter-clockwise.
    Orbit radii and altitudes are staggered to produce a visually interesting
    3-D pattern.
    """
    drones: list[Drone] = []
    spread_m = 400.0  # metres between individual home points

    for i in range(num_drones):
        # Spread home points evenly around the global home location
        angle = (2.0 * math.pi * i) / max(num_drones, 1)
        offset_x = spread_m * math.cos(angle)
        offset_y = spread_m * math.sin(angle)
        home_lat = cfg.HOME_LAT + offset_x / 111_111.0
        home_lon = cfg.HOME_LON + offset_y / (
            111_111.0 * math.cos(math.radians(cfg.HOME_LAT))
        )

        drone_cfg = DroneConfig(
            drone_id=f"drone_{i + 1:03d}",
            home_lat=home_lat,
            home_lon=home_lon,
            home_alt=cfg.HOME_ALT,
            orbit_radius=60.0 + i * 35.0,      # staggered radii
            orbit_speed=8.0 + i * 1.2,          # staggered speeds
            initial_alt_agl=80.0 + i * 25.0,    # staggered altitudes
            clockwise=(i % 2 == 0),              # alternate directions
            initial_battery_pct=100.0 - i * 5.0,  # staggered battery levels
        )
        drones.append(Drone(drone_cfg))

    return drones


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _connect_producer(retries: int = 10, backoff_s: float = 5.0) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=cfg.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=50,
            )
            logger.info("Connected to Kafka at %s", cfg.KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not reachable (attempt %d/%d) — retrying in %ds…",
                attempt,
                retries,
                backoff_s,
            )
            time.sleep(backoff_s)

    logger.error("Could not connect to Kafka after %d attempts — exiting.", retries)
    sys.exit(1)


def run() -> None:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    logger.info(
        "PX4 SITL swarm simulator | drones=%d | rate=%.1f Hz | topic=%s",
        cfg.NUM_DRONES,
        cfg.SIM_UPDATE_HZ,
        cfg.RAW_TOPIC,
    )

    producer = _connect_producer()
    drones = build_swarm(cfg.NUM_DRONES)
    interval = 1.0 / cfg.SIM_UPDATE_HZ

    while _running:
        tick_start = time.monotonic()

        for drone in drones:
            telemetry = drone.update()
            producer.send(
                cfg.RAW_TOPIC,
                key=drone.drone_id,
                value=telemetry,
            )

        producer.flush()

        elapsed = time.monotonic() - tick_start
        sleep_for = max(0.0, interval - elapsed)
        if sleep_for:
            time.sleep(sleep_for)

    producer.close()
    logger.info("Simulator stopped.")


if __name__ == "__main__":
    run()
