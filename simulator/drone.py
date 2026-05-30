import asyncio
import logging
import math
from datetime import datetime, timezone

from mavsdk import System

log = logging.getLogger(__name__)


async def stream_drone_telemetry(
    drone_id: str,
    address: str,
    queue: asyncio.Queue,
    publish_rate_hz: float = 2.0,
) -> None:
    """Connect to a PX4 SITL instance, arm it, take off, and stream telemetry to queue."""
    drone = System()
    log.info("[%s] Connecting to %s ...", drone_id, address)
    await drone.connect(system_address=address)

    async for state in drone.core.connection_state():
        if state.is_connected:
            log.info("[%s] Connected.", drone_id)
            break

    log.info("[%s] Waiting for global position estimate ...", drone_id)
    async with asyncio.timeout(120):
        async for health in drone.telemetry.health():
            if health.is_global_position_ok and health.is_home_position_ok:
                break

    log.info("[%s] Arming and taking off.", drone_id)
    await drone.action.arm()
    await drone.action.takeoff()

    # Shared state updated by concurrent subscription coroutines
    telemetry: dict = {
        "lat": 0.0, "lon": 0.0, "alt": 0.0,
        "speed": 0.0, "heading": 0.0,
        "battery_level": 0.0,
        "motor_outputs": [],
    }

    async def watch_position() -> None:
        async for pos in drone.telemetry.position():
            telemetry["lat"] = pos.latitude_deg
            telemetry["lon"] = pos.longitude_deg
            telemetry["alt"] = pos.absolute_altitude_m

    async def watch_velocity() -> None:
        async for vel in drone.telemetry.velocity_ned():
            telemetry["speed"] = math.hypot(vel.north_m_s, vel.east_m_s)

    async def watch_heading() -> None:
        async for hdg in drone.telemetry.heading():
            telemetry["heading"] = hdg.heading_deg

    async def watch_battery() -> None:
        async for bat in drone.telemetry.battery():
            pct = bat.remaining_percent
            telemetry["battery_level"] = round(max(0.0, min(100.0, (pct or 0.0) * 100)), 1)

    async def watch_actuators() -> None:
        async for act in drone.telemetry.actuator_output_status():
            telemetry["motor_outputs"] = list(act.actuator[:4])

    async def publish_loop() -> None:
        interval = 1.0 / publish_rate_hz
        while True:
            motors = [
                {
                    "motor_id": i + 1,
                    "output": round(telemetry["motor_outputs"][i], 3)
                    if i < len(telemetry["motor_outputs"])
                    else 0.0,
                }
                for i in range(4)
            ]
            record = {
                "drone_id": drone_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "lat": round(telemetry["lat"], 6),
                "lon": round(telemetry["lon"], 6),
                "alt": round(telemetry["alt"], 2),
                "speed": round(telemetry["speed"], 2),
                "heading": round(telemetry["heading"], 2),
                "battery_level": telemetry["battery_level"],
                "motor_status": motors,
                "payload": None,
            }
            await queue.put(record)
            await asyncio.sleep(interval)

    await asyncio.gather(
        watch_position(),
        watch_velocity(),
        watch_heading(),
        watch_battery(),
        watch_actuators(),
        publish_loop(),
    )
