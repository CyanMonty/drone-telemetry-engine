import asyncio
import math
import random
from datetime import datetime, timezone


async def stream_drone_physics(
    drone_id: str,
    queue: asyncio.Queue,
    publish_rate_hz: float = 2.0,
    home_lat: float = 40.7128,
    home_lon: float = -74.0060,
) -> None:
    """Lightweight physics coroutine — no external dependencies."""
    lat = home_lat + random.uniform(-0.1, 0.1)
    lon = home_lon + random.uniform(-0.1, 0.1)
    alt = random.uniform(30, 100)
    speed = random.uniform(5, 20)
    heading = random.uniform(0, 360)
    battery_level = random.uniform(80, 100)
    drain_rate = 0.005  # % per second

    # Environmental / sensor state
    wind_speed: float = random.uniform(0.0, 8.0)       # m/s
    wind_heading: float = random.uniform(0.0, 360.0)   # degrees
    gps_satellites: int = random.randint(8, 12)
    signal_strength: float = 100.0                     # %

    # Motor fault state — one motor can degrade over time
    motor_fault: int | None = None   # 1–4 which motor is affected
    motor_degradation: float = 0.0  # 0.0 = healthy, 1.0 = fully failed

    interval = 1.0 / publish_rate_hz

    # Stagger start so coroutines don't all wake up simultaneously
    await asyncio.sleep(random.uniform(0, interval))

    while True:
        dt = interval

        # --- wind: slow random walk ---
        wind_speed = max(0.0, min(15.0, wind_speed + random.gauss(0, 0.3) * dt))
        wind_heading = (wind_heading + random.gauss(0, 8.0) * dt) % 360

        # --- position: heading drifts with wind crosswind component ---
        wind_drift = math.sin(math.radians(wind_heading - heading))
        heading = (heading + random.gauss(0, 3) + wind_speed * 0.05 * wind_drift) % 360
        dist_deg = speed * dt / 111_320
        lat += dist_deg * math.cos(math.radians(heading))
        cos_lat = math.cos(math.radians(lat)) or 1e-9
        lon += dist_deg * math.sin(math.radians(heading)) / cos_lat

        alt = max(10.0, min(150.0, alt + random.gauss(0, 0.5)))

        # --- battery ---
        battery_level = max(0.0, battery_level - drain_rate * dt)
        if battery_level < 10.0:
            speed = max(0.0, speed * 0.99)

        # --- GPS: occasionally drops a satellite ---
        if random.random() < 0.02 * dt:
            gps_satellites = max(4, min(12, gps_satellites + random.choice([-1, 0, 0, 1])))

        # --- signal strength: falls off with distance from home ---
        dist_m = math.sqrt(
            ((lat - home_lat) * 111_320) ** 2
            + ((lon - home_lon) * 111_320 * math.cos(math.radians(home_lat))) ** 2
        )
        base_signal = max(0.0, 100.0 - dist_m / 80.0)
        signal_strength = max(0.0, min(100.0, base_signal + random.gauss(0, 4)))

        # --- motor fault: low-probability random onset, then progressive degradation ---
        if motor_fault is None and random.random() < 0.0005 * dt:
            motor_fault = random.randint(1, 4)
            motor_degradation = 0.0
        if motor_fault is not None:
            motor_degradation = min(1.0, motor_degradation + random.uniform(0.0, 0.02) * dt)

        # --- motor outputs (faulted motor loses output proportionally) ---
        base_output = min(1.0, 0.4 + speed * 0.02)
        motors = []
        for i in range(4):
            output = base_output + random.gauss(0, 0.01)
            if motor_fault == i + 1:
                output = max(0.0, output * (1.0 - motor_degradation))
            motors.append({"motor_id": i + 1, "output": round(output, 3)})

        # --- derived flight mode ---
        if battery_level <= 5.0 or (motor_fault is not None and motor_degradation >= 0.5):
            flight_mode = "EMERGENCY"
        elif battery_level <= 20.0:
            flight_mode = "RTL"
        else:
            flight_mode = "MISSION"

        # --- derived health status ---
        if (
            battery_level <= 10.0
            or (motor_fault is not None and motor_degradation >= 0.3)
            or signal_strength < 15.0
            or gps_satellites < 4
        ):
            drone_status = "CRITICAL"
        elif (
            battery_level <= 20.0
            or motor_fault is not None
            or signal_strength < 30.0
            or gps_satellites < 6
        ):
            drone_status = "WARNING"
        else:
            drone_status = "HEALTHY"

        record = {
            "drone_id": drone_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "alt": round(alt, 2),
            "speed": round(speed, 2),
            "heading": round(heading, 2),
            "battery_level": round(battery_level, 2),
            "motor_status": motors,
            "payload": {
                "flight_mode": flight_mode,
                "status": drone_status,
                "signal_strength": round(signal_strength, 1),
                "gps_satellites": gps_satellites,
                "wind_speed": round(wind_speed, 1),
                "wind_heading": round(wind_heading, 1),
                "motor_fault": motor_fault,
                "motor_degradation": round(motor_degradation, 3) if motor_fault is not None else None,
            },
        }
        await queue.put(record)
        await asyncio.sleep(interval)
