"""Unit tests for sim/drone.py — no Kafka required."""

from __future__ import annotations

import math
import time

import pytest

from sim.drone import Drone, DroneConfig, _meters_to_lat, _meters_to_lon


# ---------------------------------------------------------------------------
# Coordinate helper tests
# ---------------------------------------------------------------------------

def test_meters_to_lat_positive():
    # 1 degree of latitude ≈ 111,111 m at any longitude
    lat_delta = _meters_to_lat(111_111.0)
    assert abs(lat_delta - 1.0) < 0.001


def test_meters_to_lon_at_equator():
    lon_delta = _meters_to_lon(111_111.0, 0.0)
    assert abs(lon_delta - 1.0) < 0.001


def test_meters_to_lon_shrinks_at_higher_latitude():
    lon_0 = _meters_to_lon(1000.0, 0.0)
    lon_60 = _meters_to_lon(1000.0, 60.0)
    assert lon_60 > lon_0  # same metres covers more degrees at higher lat


# ---------------------------------------------------------------------------
# DroneConfig defaults
# ---------------------------------------------------------------------------

def _make_drone(**kwargs) -> Drone:
    defaults = dict(
        drone_id="test_001",
        home_lat=37.8044,
        home_lon=-122.4679,
        home_alt=10.0,
        orbit_radius=100.0,
        orbit_speed=10.0,
        initial_alt_agl=100.0,
        clockwise=True,
        initial_battery_pct=100.0,
    )
    defaults.update(kwargs)
    return Drone(DroneConfig(**defaults))


# ---------------------------------------------------------------------------
# Initial state tests
# ---------------------------------------------------------------------------

class TestDroneInitialState:
    def test_drone_id_set(self):
        d = _make_drone()
        assert d.drone_id == "test_001"

    def test_initial_altitude(self):
        d = _make_drone(home_alt=10.0, initial_alt_agl=100.0)
        assert abs(d.alt_msl - 110.0) < 0.01

    def test_armed_initially(self):
        d = _make_drone()
        assert d.armed is True

    def test_flight_mode(self):
        d = _make_drone()
        assert d.flight_mode == "AUTO.MISSION"

    def test_gps_fix_type(self):
        d = _make_drone()
        assert d.gps_fix_type == 3

    def test_battery_at_full(self):
        d = _make_drone(initial_battery_pct=100.0)
        assert d._battery_remaining_pct() == 100

    def test_battery_at_partial(self):
        d = _make_drone(initial_battery_pct=50.0)
        pct = d._battery_remaining_pct()
        assert 48 <= pct <= 52  # small float tolerance


# ---------------------------------------------------------------------------
# Physics update tests
# ---------------------------------------------------------------------------

class TestDroneUpdate:
    def test_update_returns_dict(self):
        d = _make_drone()
        result = d.update()
        assert isinstance(result, dict)

    def test_telemetry_keys_present(self):
        d = _make_drone()
        t = d.update()
        for key in ("drone_id", "timestamp", "sequence", "heartbeat",
                    "position", "attitude", "battery", "gps"):
            assert key in t, f"Missing key: {key}"

    def test_position_keys(self):
        d = _make_drone()
        pos = d.update()["position"]
        for key in ("lat", "lon", "alt_msl", "relative_alt", "vx", "vy", "vz", "heading_deg"):
            assert key in pos

    def test_sequence_increments(self):
        d = _make_drone()
        t1 = d.update()
        t2 = d.update()
        assert t2["sequence"] == t1["sequence"] + 1

    def test_position_changes_over_time(self):
        d = _make_drone()
        pos1 = d.update()["position"]
        time.sleep(0.05)
        pos2 = d.update()["position"]
        assert pos1["lat"] != pos2["lat"] or pos1["lon"] != pos2["lon"]

    def test_altitude_within_plausible_range(self):
        d = _make_drone(home_alt=0.0, initial_alt_agl=100.0)
        for _ in range(20):
            pos = d.update()["position"]
        assert 80.0 <= pos["relative_alt"] <= 120.0

    def test_battery_drains(self):
        d = _make_drone(initial_battery_pct=100.0)
        d.battery_current_a = 10.0  # fast drain for test
        initial_consumed = d.battery_consumed_mah
        time.sleep(0.1)
        d.update()
        assert d.battery_consumed_mah > initial_consumed

    def test_battery_voltage_decreases_with_drain(self):
        full = _make_drone(initial_battery_pct=100.0)
        empty = _make_drone(initial_battery_pct=10.0)
        assert full._battery_voltage > empty._battery_voltage

    def test_heading_is_0_to_360(self):
        d = _make_drone()
        for _ in range(10):
            heading = d.update()["position"]["heading_deg"]
        assert 0.0 <= heading < 360.0

    def test_roll_is_positive_for_clockwise_orbit(self):
        d = _make_drone(clockwise=True, orbit_radius=100.0, orbit_speed=10.0)
        # After a few updates the bank angle should be positive
        rolls = []
        for _ in range(5):
            roll = d.update()["attitude"]["roll_deg"]
            rolls.append(roll)
        # Centripetal roll should be non-trivially positive (at least some are)
        assert any(r > 0 for r in rolls)

    def test_counter_clockwise_has_negative_angular_velocity(self):
        d = _make_drone(clockwise=False)
        assert d._angular_velocity < 0

    def test_to_dict_is_serialisable(self):
        import json
        d = _make_drone()
        d.update()
        # Should not raise
        json.dumps(d.to_dict())


# ---------------------------------------------------------------------------
# Edge-case tests
# ---------------------------------------------------------------------------

class TestDroneEdgeCases:
    def test_battery_cannot_go_below_zero_pct(self):
        d = _make_drone(initial_battery_pct=0.0)
        d.battery_current_a = 100.0
        for _ in range(100):
            d.update()
        assert d._battery_remaining_pct() == 0

    def test_drone_id_preserved_in_telemetry(self):
        d = _make_drone(drone_id="alpha_99")
        t = d.update()
        assert t["drone_id"] == "alpha_99"

    def test_gps_satellites_stay_in_range(self):
        d = _make_drone()
        for _ in range(50):
            gps = d.update()["gps"]
        assert 6 <= gps["satellites_visible"] <= 16
