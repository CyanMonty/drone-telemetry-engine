"""
TDD tests for anomaly_detector.detect_anomalies.

These tests define the contract BEFORE the module exists.
Run `pytest -m unit` — they will be RED until consumer/anomaly_detector.py is created.
"""
import pytest

from anomaly_detector import detect_anomalies

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _record(**overrides) -> dict:
    base = {
        "drone_id": "drone-1",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "lat": 40.7, "lon": -74.0,
        "alt": 50.0,
        "speed": 10.0,
        "heading": 90.0,
        "battery_level": 80.0,
        "motor_status": [],
        "payload": None,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Normal record — no alerts
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_no_alerts_for_healthy_record():
    assert detect_anomalies(_record()) == []


# ---------------------------------------------------------------------------
# Battery anomalies
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_low_battery_alert_below_threshold():
    alerts = detect_anomalies(_record(battery_level=14.9))
    types = [a["type"] for a in alerts]
    assert "LOW_BATTERY" in types


@pytest.mark.unit
def test_no_low_battery_alert_at_threshold():
    """Exactly at 15.0 is not a violation (strict less-than)."""
    alerts = detect_anomalies(_record(battery_level=15.0))
    types = [a["type"] for a in alerts]
    assert "LOW_BATTERY" not in types


@pytest.mark.unit
def test_low_battery_alert_contains_drone_id_and_value():
    alerts = detect_anomalies(_record(drone_id="alpha", battery_level=5.0))
    alert = next(a for a in alerts if a["type"] == "LOW_BATTERY")
    assert alert["drone_id"] == "alpha"
    assert alert["value"] == 5.0


# ---------------------------------------------------------------------------
# Altitude anomalies
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_alt_high_alert_above_max():
    alerts = detect_anomalies(_record(alt=150.1))
    assert any(a["type"] == "ALT_HIGH" for a in alerts)


@pytest.mark.unit
def test_no_alt_high_alert_at_max():
    alerts = detect_anomalies(_record(alt=150.0))
    assert not any(a["type"] == "ALT_HIGH" for a in alerts)


@pytest.mark.unit
def test_alt_low_alert_below_min():
    alerts = detect_anomalies(_record(alt=9.9))
    assert any(a["type"] == "ALT_LOW" for a in alerts)


@pytest.mark.unit
def test_no_alt_low_alert_at_min():
    alerts = detect_anomalies(_record(alt=10.0))
    assert not any(a["type"] == "ALT_LOW" for a in alerts)


# ---------------------------------------------------------------------------
# Speed anomalies
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_speed_high_alert_above_max():
    alerts = detect_anomalies(_record(speed=25.1))
    assert any(a["type"] == "SPEED_HIGH" for a in alerts)


@pytest.mark.unit
def test_no_speed_high_alert_at_max():
    alerts = detect_anomalies(_record(speed=25.0))
    assert not any(a["type"] == "SPEED_HIGH" for a in alerts)


# ---------------------------------------------------------------------------
# Multiple simultaneous violations
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_multiple_anomalies_in_one_record():
    alerts = detect_anomalies(_record(battery_level=5.0, alt=200.0, speed=30.0))
    types = {a["type"] for a in alerts}
    assert types == {"LOW_BATTERY", "ALT_HIGH", "SPEED_HIGH"}
