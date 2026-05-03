"""Drone physics model for PX4 SITL swarm simulation."""

from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Coordinate helpers
# ---------------------------------------------------------------------------

def _meters_to_lat(metres: float) -> float:
    """Convert a north/south offset in metres to degrees of latitude."""
    return metres / 111_111.0


def _meters_to_lon(metres: float, lat_deg: float) -> float:
    """Convert an east/west offset in metres to degrees of longitude."""
    return metres / (111_111.0 * math.cos(math.radians(lat_deg)))


# ---------------------------------------------------------------------------
# Drone configuration
# ---------------------------------------------------------------------------

@dataclass
class DroneConfig:
    drone_id: str
    home_lat: float
    home_lon: float
    home_alt: float = 10.0          # metres AMSL of launch point
    orbit_radius: float = 100.0     # metres
    orbit_speed: float = 10.0       # m/s (tangential)
    initial_alt_agl: float = 100.0  # metres above launch point
    clockwise: bool = True          # orbit direction
    initial_battery_pct: float = 100.0


# ---------------------------------------------------------------------------
# Drone state machine
# ---------------------------------------------------------------------------

class Drone:
    """
    Simulates a single PX4-based drone flying a circular orbit.

    Call :meth:`update` on each tick to advance the simulation and obtain
    the latest telemetry snapshot as a plain dict that mirrors the JSON
    message produced by a real MAVLink/PX4 SITL instance.
    """

    # 4S LiPo voltage range (V)
    _VMAX = 16.8
    _VMIN = 13.2

    def __init__(self, config: DroneConfig) -> None:
        self.config = config
        self.drone_id = config.drone_id

        # ---------- position ----------
        self.lat = config.home_lat
        self.lon = config.home_lon
        self.alt_msl = config.home_alt + config.initial_alt_agl
        self.relative_alt = config.initial_alt_agl

        # ---------- velocity (NED, m/s) ----------
        self.vx = 0.0
        self.vy = 0.0
        self.vz = 0.0

        # ---------- attitude (radians) ----------
        self.roll = 0.0
        self.pitch = 0.0
        self.yaw = random.uniform(0.0, 2.0 * math.pi)
        self.rollspeed = 0.0
        self.pitchspeed = 0.0
        self.yawspeed = 0.0

        # ---------- battery (10 Ah 4S LiPo) ----------
        self.battery_capacity_mah = 10_000.0
        self.battery_consumed_mah = (
            (1.0 - config.initial_battery_pct / 100.0) * self.battery_capacity_mah
        )
        self.battery_current_a = random.uniform(1.5, 3.0)  # slow demo drain
        self._update_battery_voltage()

        # ---------- GPS ----------
        self.gps_fix_type = 3           # MAV_GPS_FIX_TYPE_3D
        self.satellites_visible = random.randint(11, 15)
        self.eph = 0.8                  # horizontal position error (m)
        self.epv = 1.2                  # vertical position error (m)

        # ---------- flight state ----------
        self.armed = True
        self.flight_mode = "AUTO.MISSION"
        self.system_status = "ACTIVE"   # MAV_STATE_ACTIVE

        # ---------- circular orbit ----------
        sign = 1 if config.clockwise else -1
        self._angular_velocity = sign * config.orbit_speed / max(config.orbit_radius, 1.0)
        self._orbit_angle = random.uniform(0.0, 2.0 * math.pi)

        # altitude sinusoidal variation
        self._alt_phase = random.uniform(0.0, 2.0 * math.pi)
        self._alt_amplitude = 10.0  # ±10 m

        self._last_update = time.monotonic()
        self._sequence = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(self) -> dict:
        """Advance simulation by one timestep and return the telemetry dict."""
        now = time.monotonic()
        dt = max(now - self._last_update, 1e-6)
        self._last_update = now
        self._sequence += 1

        self._update_position(dt)
        self._update_battery(dt)
        self._update_gps_noise()

        return self.to_dict()

    def to_dict(self) -> dict:
        """Return a snapshot of the drone state as a serialisable dict."""
        heading_deg = math.degrees(self.yaw) % 360.0
        return {
            "drone_id": self.drone_id,
            "timestamp": time.time(),
            "sequence": self._sequence,
            "heartbeat": {
                "armed": self.armed,
                "flight_mode": self.flight_mode,
                "system_status": self.system_status,
            },
            "position": {
                "lat": round(self.lat, 7),
                "lon": round(self.lon, 7),
                "alt_msl": round(self.alt_msl, 2),
                "relative_alt": round(self.relative_alt, 2),
                "vx": round(self.vx, 3),
                "vy": round(self.vy, 3),
                "vz": round(self.vz, 3),
                "heading_deg": round(heading_deg, 1),
            },
            "attitude": {
                "roll_deg": round(math.degrees(self.roll), 3),
                "pitch_deg": round(math.degrees(self.pitch), 3),
                "yaw_deg": round(math.degrees(self.yaw) % 360.0, 3),
                "rollspeed": round(self.rollspeed, 4),
                "pitchspeed": round(self.pitchspeed, 4),
                "yawspeed": round(self.yawspeed, 4),
            },
            "battery": {
                "voltage_v": round(self._battery_voltage, 2),
                "current_a": round(self.battery_current_a, 2),
                "remaining_pct": self._battery_remaining_pct(),
                "consumed_mah": round(self.battery_consumed_mah, 1),
            },
            "gps": {
                "fix_type": self.gps_fix_type,
                "satellites_visible": self.satellites_visible,
                "eph": round(self.eph, 2),
                "epv": round(self.epv, 2),
            },
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _update_position(self, dt: float) -> None:
        """Advance the circular orbit by *dt* seconds."""
        self._orbit_angle += self._angular_velocity * dt
        self._alt_phase += 0.08 * dt  # slow altitude oscillation

        # Orbit position in metres relative to home
        r = self.config.orbit_radius
        x_m = r * math.cos(self._orbit_angle)
        y_m = r * math.sin(self._orbit_angle)

        self.lat = self.config.home_lat + _meters_to_lat(x_m)
        self.lon = self.config.home_lon + _meters_to_lon(y_m, self.lat)

        # Altitude
        self.relative_alt = (
            self.config.initial_alt_agl
            + self._alt_amplitude * math.sin(self._alt_phase)
        )
        self.alt_msl = self.config.home_alt + self.relative_alt

        # Velocity (tangential to orbit)
        spd = self.config.orbit_speed
        self.vx = -spd * math.sin(self._orbit_angle) + random.gauss(0, 0.05)
        self.vy = spd * math.cos(self._orbit_angle) + random.gauss(0, 0.05)
        self.vz = (
            -self._alt_amplitude
            * self._angular_velocity
            * math.cos(self._alt_phase)
            + random.gauss(0, 0.02)
        )

        # Yaw tracks velocity direction
        self.yaw = math.atan2(self.vy, self.vx)

        # Bank angle for centripetal acceleration
        centripetal = spd ** 2 / max(r, 1.0)
        self.roll = math.atan2(centripetal, 9.81) + random.gauss(0, 0.005)
        self.pitch = random.gauss(0, 0.01)

        # Approximate angular rates
        self.yawspeed = self._angular_velocity + random.gauss(0, 0.001)

    def _update_battery(self, dt: float) -> None:
        """Drain battery by *dt* seconds of current draw."""
        used_mah = (self.battery_current_a * dt) / 3600.0 * 1000.0
        self.battery_consumed_mah = min(
            self.battery_consumed_mah + used_mah, self.battery_capacity_mah
        )
        self._update_battery_voltage()

    def _update_battery_voltage(self) -> None:
        remaining_frac = 1.0 - self.battery_consumed_mah / self.battery_capacity_mah
        self._battery_voltage = (
            self._VMIN + (self._VMAX - self._VMIN) * remaining_frac
        )

    def _battery_remaining_pct(self) -> int:
        return max(
            0,
            int(100.0 * (1.0 - self.battery_consumed_mah / self.battery_capacity_mah)),
        )

    def _update_gps_noise(self) -> None:
        self.eph = max(0.3, self.eph + random.gauss(0, 0.02))
        self.satellites_visible = max(
            6, min(16, self.satellites_visible + random.randint(-1, 1))
        )
