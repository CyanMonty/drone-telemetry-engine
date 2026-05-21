"""
Anomaly detection for drone telemetry.

Reads a telemetry record dict and returns a list of alert dicts,
one per violation. Returns an empty list if the record is healthy.
"""

BATTERY_LOW: float = 15.0   # %
ALT_MAX: float = 150.0       # metres
ALT_MIN: float = 10.0        # metres
SPEED_MAX: float = 25.0      # m/s


def detect_anomalies(record: dict) -> list[dict]:
    """Return alert dicts for any threshold violations in *record*."""
    alerts = []
    drone_id = record["drone_id"]
    timestamp = record["timestamp"]

    if record["battery_level"] < BATTERY_LOW:
        alerts.append({
            "drone_id": drone_id,
            "timestamp": timestamp,
            "type": "LOW_BATTERY",
            "value": record["battery_level"],
            "threshold": BATTERY_LOW,
        })

    if record["alt"] > ALT_MAX:
        alerts.append({
            "drone_id": drone_id,
            "timestamp": timestamp,
            "type": "ALT_HIGH",
            "value": record["alt"],
            "threshold": ALT_MAX,
        })

    if record["alt"] < ALT_MIN:
        alerts.append({
            "drone_id": drone_id,
            "timestamp": timestamp,
            "type": "ALT_LOW",
            "value": record["alt"],
            "threshold": ALT_MIN,
        })

    if record["speed"] > SPEED_MAX:
        alerts.append({
            "drone_id": drone_id,
            "timestamp": timestamp,
            "type": "SPEED_HIGH",
            "value": record["speed"],
            "threshold": SPEED_MAX,
        })

    return alerts
