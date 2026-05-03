"""Unit tests for parser/telemetry_parser.py — no Kafka required."""

from __future__ import annotations

import math
import time

import pytest

from parser.telemetry_parser import enrich, _battery_alert, _gps_status


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_raw(
    *,
    drone_id: str = "drone_001",
    vx: float = 10.0,
    vy: float = 0.0,
    vz: float = 0.0,
    battery_pct: int = 80,
    fix_type: int = 3,
    satellites: int = 12,
) -> dict:
    return {
        "drone_id": drone_id,
        "timestamp": time.time(),
        "sequence": 1,
        "heartbeat": {
            "armed": True,
            "flight_mode": "AUTO.MISSION",
            "system_status": "ACTIVE",
        },
        "position": {
            "lat": 37.8044,
            "lon": -122.4679,
            "alt_msl": 110.0,
            "relative_alt": 100.0,
            "vx": vx,
            "vy": vy,
            "vz": vz,
            "heading_deg": 45.0,
        },
        "attitude": {
            "roll_deg": 2.0,
            "pitch_deg": -0.5,
            "yaw_deg": 45.0,
            "rollspeed": 0.0,
            "pitchspeed": 0.0,
            "yawspeed": 0.05,
        },
        "battery": {
            "voltage_v": 15.2,
            "current_a": 2.0,
            "remaining_pct": battery_pct,
            "consumed_mah": 500.0,
        },
        "gps": {
            "fix_type": fix_type,
            "satellites_visible": satellites,
            "eph": 0.9,
            "epv": 1.1,
        },
    }


# ---------------------------------------------------------------------------
# _battery_alert
# ---------------------------------------------------------------------------

class TestBatteryAlert:
    def test_ok_above_25(self):
        assert _battery_alert(26) == "OK"
        assert _battery_alert(100) == "OK"

    def test_warning_between_15_and_25(self):
        assert _battery_alert(24) == "WARNING"
        assert _battery_alert(15) == "WARNING"

    def test_critical_below_15(self):
        assert _battery_alert(14) == "CRITICAL"
        assert _battery_alert(0) == "CRITICAL"


# ---------------------------------------------------------------------------
# _gps_status
# ---------------------------------------------------------------------------

class TestGpsStatus:
    def test_no_fix(self):
        assert _gps_status(0) == "NO_FIX"
        assert _gps_status(1) == "NO_FIX"

    def test_2d_fix(self):
        assert _gps_status(2) == "2D_FIX"

    def test_3d_fix(self):
        assert _gps_status(3) == "3D_FIX"
        assert _gps_status(4) == "3D_FIX"


# ---------------------------------------------------------------------------
# enrich function
# ---------------------------------------------------------------------------

class TestEnrich:
    def test_returns_dict(self):
        raw = _make_raw()
        result = enrich(raw)
        assert isinstance(result, dict)

    def test_preserves_original_fields(self):
        raw = _make_raw(drone_id="probe_42")
        result = enrich(raw)
        assert result["drone_id"] == "probe_42"
        assert result["sequence"] == 1

    def test_adds_parsed_at(self):
        raw = _make_raw()
        before = time.time()
        result = enrich(raw)
        after = time.time()
        assert before <= result["parsed_at"] <= after

    def test_adds_computed_block(self):
        raw = _make_raw()
        result = enrich(raw)
        assert "computed" in result

    def test_ground_speed_calculation(self):
        raw = _make_raw(vx=3.0, vy=4.0, vz=0.0)
        result = enrich(raw)
        expected = 5.0  # sqrt(9 + 16)
        assert abs(result["computed"]["ground_speed_ms"] - expected) < 0.01

    def test_ground_speed_kmh(self):
        raw = _make_raw(vx=10.0, vy=0.0)
        result = enrich(raw)
        spd_ms = result["computed"]["ground_speed_ms"]
        assert abs(result["computed"]["ground_speed_kmh"] - spd_ms * 3.6) < 0.1

    def test_total_speed_includes_vertical(self):
        raw = _make_raw(vx=3.0, vy=4.0, vz=5.0)
        result = enrich(raw)
        expected = math.sqrt(3**2 + 4**2 + 5**2)
        assert abs(result["computed"]["total_speed_ms"] - expected) < 0.01

    def test_battery_alert_ok(self):
        raw = _make_raw(battery_pct=80)
        assert enrich(raw)["computed"]["battery_alert"] == "OK"

    def test_battery_alert_warning(self):
        raw = _make_raw(battery_pct=20)
        assert enrich(raw)["computed"]["battery_alert"] == "WARNING"

    def test_battery_alert_critical(self):
        raw = _make_raw(battery_pct=10)
        assert enrich(raw)["computed"]["battery_alert"] == "CRITICAL"

    def test_gps_status_3d(self):
        raw = _make_raw(fix_type=3, satellites=12)
        result = enrich(raw)
        assert result["computed"]["gps_status"] == "3D_FIX"
        assert result["computed"]["gps_healthy"] is True

    def test_gps_status_no_fix(self):
        raw = _make_raw(fix_type=0, satellites=0)
        result = enrich(raw)
        assert result["computed"]["gps_status"] == "NO_FIX"
        assert result["computed"]["gps_healthy"] is False

    def test_gps_unhealthy_if_satellites_low(self):
        # fix_type=3 but only 4 satellites
        raw = _make_raw(fix_type=3, satellites=4)
        result = enrich(raw)
        assert result["computed"]["gps_healthy"] is False

    def test_does_not_mutate_original(self):
        raw = _make_raw()
        raw_copy = dict(raw)
        enrich(raw)
        assert raw == raw_copy

    def test_zero_velocity(self):
        raw = _make_raw(vx=0.0, vy=0.0, vz=0.0)
        result = enrich(raw)
        assert result["computed"]["ground_speed_ms"] == 0.0

    def test_missing_position_block(self):
        raw = _make_raw()
        del raw["position"]
        result = enrich(raw)
        assert result["computed"]["ground_speed_ms"] == 0.0

    def test_missing_battery_block(self):
        raw = _make_raw()
        del raw["battery"]
        result = enrich(raw)
        # Default remaining_pct=100 → OK
        assert result["computed"]["battery_alert"] == "OK"

    def test_missing_gps_block(self):
        raw = _make_raw()
        del raw["gps"]
        result = enrich(raw)
        # Default fix_type=0 → NO_FIX
        assert result["computed"]["gps_status"] == "NO_FIX"
