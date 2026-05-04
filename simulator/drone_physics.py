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

    interval = 1.0 / publish_rate_hz

    # Stagger start so coroutines don't all wake up simultaneously
    await asyncio.sleep(random.uniform(0, interval))

    while True:
        dt = interval

        dist_deg = speed * dt / 111_320
        lat += dist_deg * math.cos(math.radians(heading))
        cos_lat = math.cos(math.radians(lat)) or 1e-9
        lon += dist_deg * math.sin(math.radians(heading)) / cos_lat

        heading = (heading + random.gauss(0, 3)) % 360
        alt = max(10.0, min(150.0, alt + random.gauss(0, 0.5)))

        battery_level = max(0.0, battery_level - drain_rate * dt)
        if battery_level < 10.0:
            speed = max(0.0, speed * 0.99)

        base_output = min(1.0, 0.4 + speed * 0.02)
        motors = [
            {"motor_id": i + 1, "output": round(base_output + random.gauss(0, 0.01), 3)}
            for i in range(4)
        ]

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
            "payload": None,
        }
        await queue.put(record)
        await asyncio.sleep(interval)
