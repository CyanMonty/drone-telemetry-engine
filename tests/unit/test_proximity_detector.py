"""
Unit tests for proximity_detector.

haversine_distance() is tested against known real-world coordinates.
check_proximity() is tested with a mocked DB connection so no PostGIS is needed.
"""

import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Make proximity_detector importable without psycopg2 or db on the path
# ---------------------------------------------------------------------------

# Stub psycopg2 so the import doesn't fail in environments without it.
psycopg2_stub = types.ModuleType("psycopg2")
sys.modules.setdefault("psycopg2", psycopg2_stub)

# Stub the db module — we'll mock individual functions per test.
db_stub = types.ModuleType("db")
db_stub.upsert_position = MagicMock()
db_stub.find_nearby = MagicMock(return_value=[])
db_stub.was_recently_alerted = MagicMock(return_value=False)
db_stub.insert_alert = MagicMock()
sys.modules["db"] = db_stub

from proximity_detector import check_proximity, haversine_distance  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _record(**overrides) -> dict:
    base = {
        "drone_id": "drone-0001",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "lat": 40.7128,
        "lon": -74.0060,
        "alt": 50.0,
        "speed": 10.0,
        "heading": 90.0,
        "battery_level": 80.0,
        "motor_status": [],
        "payload": {},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# haversine_distance
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_haversine_zero_distance():
    assert haversine_distance(40.7128, -74.0060, 40.7128, -74.0060) == pytest.approx(0.0)


@pytest.mark.unit
def test_haversine_known_distance_nyc_to_la():
    # NYC (40.7128, -74.0060) to LA (34.0522, -118.2437) ≈ 3,940 km
    dist = haversine_distance(40.7128, -74.0060, 34.0522, -118.2437)
    assert 3_900_000 < dist < 3_980_000


@pytest.mark.unit
def test_haversine_short_distance_50m():
    # Move ~50 m north along the same longitude (1° lat ≈ 111,320 m)
    delta = 50 / 111_320
    dist = haversine_distance(40.7128, -74.0060, 40.7128 + delta, -74.0060)
    assert dist == pytest.approx(50.0, rel=0.01)


@pytest.mark.unit
def test_haversine_symmetry():
    a = (40.7128, -74.0060)
    b = (51.5074, -0.1278)
    assert haversine_distance(*a, *b) == pytest.approx(haversine_distance(*b, *a))


# ---------------------------------------------------------------------------
# check_proximity — no nearby drones
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_check_proximity_no_nearby_returns_empty(monkeypatch):
    monkeypatch.setattr("proximity_detector.db.upsert_position", MagicMock())
    monkeypatch.setattr("proximity_detector.db.find_nearby", MagicMock(return_value=[]))
    monkeypatch.setattr("proximity_detector.db.was_recently_alerted", MagicMock(return_value=False))
    monkeypatch.setattr("proximity_detector.db.insert_alert", MagicMock())

    conn = MagicMock()
    alerts = check_proximity(conn, _record(), threshold_m=50.0, cooldown_s=10.0)
    assert alerts == []


# ---------------------------------------------------------------------------
# check_proximity — nearby drone triggers alert
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_check_proximity_nearby_drone_returns_alert(monkeypatch):
    neighbour = {"drone_id": "drone-0002", "lat": 40.7129, "lon": -74.0061, "distance_m": 15.3}
    mock_insert = MagicMock()

    monkeypatch.setattr("proximity_detector.db.upsert_position", MagicMock())
    monkeypatch.setattr("proximity_detector.db.find_nearby", MagicMock(return_value=[neighbour]))
    monkeypatch.setattr("proximity_detector.db.was_recently_alerted", MagicMock(return_value=False))
    monkeypatch.setattr("proximity_detector.db.insert_alert", mock_insert)

    conn = MagicMock()
    alerts = check_proximity(conn, _record(), threshold_m=50.0, cooldown_s=10.0)

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert["drone_id_a"] == "drone-0001"
    assert alert["drone_id_b"] == "drone-0002"
    assert alert["distance_m"] == 15.3
    assert alert["threshold_m"] == 50.0
    mock_insert.assert_called_once()


# ---------------------------------------------------------------------------
# check_proximity — cooldown suppresses duplicate alert
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_check_proximity_cooldown_suppresses_alert(monkeypatch):
    neighbour = {"drone_id": "drone-0002", "lat": 40.7129, "lon": -74.0061, "distance_m": 15.3}
    mock_insert = MagicMock()

    monkeypatch.setattr("proximity_detector.db.upsert_position", MagicMock())
    monkeypatch.setattr("proximity_detector.db.find_nearby", MagicMock(return_value=[neighbour]))
    monkeypatch.setattr("proximity_detector.db.was_recently_alerted", MagicMock(return_value=True))
    monkeypatch.setattr("proximity_detector.db.insert_alert", mock_insert)

    conn = MagicMock()
    alerts = check_proximity(conn, _record(), threshold_m=50.0, cooldown_s=10.0)

    assert alerts == []
    mock_insert.assert_not_called()


# ---------------------------------------------------------------------------
# check_proximity — alert shape contains all required keys
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_check_proximity_alert_has_required_keys(monkeypatch):
    neighbour = {"drone_id": "drone-0002", "lat": 40.7129, "lon": -74.0061, "distance_m": 10.0}

    monkeypatch.setattr("proximity_detector.db.upsert_position", MagicMock())
    monkeypatch.setattr("proximity_detector.db.find_nearby", MagicMock(return_value=[neighbour]))
    monkeypatch.setattr("proximity_detector.db.was_recently_alerted", MagicMock(return_value=False))
    monkeypatch.setattr("proximity_detector.db.insert_alert", MagicMock())

    conn = MagicMock()
    alerts = check_proximity(conn, _record(), threshold_m=50.0, cooldown_s=10.0)

    required = {"timestamp", "drone_id_a", "drone_id_b", "distance_m", "threshold_m",
                "lat_a", "lon_a", "lat_b", "lon_b"}
    assert required.issubset(alerts[0].keys())
