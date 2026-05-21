"""
Unit tests for drone_physics.stream_drone_physics.

Runs the coroutine at a high publish rate to get a record quickly,
then validates structure, field types, and value ranges.
"""
import asyncio
import pytest

from drone_physics import stream_drone_physics

REQUIRED_FIELDS = {
    "drone_id", "timestamp", "lat", "lon", "alt", "speed",
    "heading", "battery_level", "motor_status", "payload",
}


async def _get_one_record(drone_id: str = "drone-test") -> dict:
    queue: asyncio.Queue = asyncio.Queue()
    task = asyncio.create_task(
        stream_drone_physics(drone_id, queue, publish_rate_hz=100)
    )
    record = await asyncio.wait_for(queue.get(), timeout=2.0)
    task.cancel()
    return record


@pytest.mark.unit
async def test_record_has_required_fields():
    record = await _get_one_record()
    assert REQUIRED_FIELDS.issubset(record.keys())


@pytest.mark.unit
async def test_drone_id_matches():
    record = await _get_one_record("alpha-7")
    assert record["drone_id"] == "alpha-7"


@pytest.mark.unit
async def test_battery_starts_in_valid_range():
    record = await _get_one_record()
    assert 0.0 <= record["battery_level"] <= 100.0


@pytest.mark.unit
async def test_altitude_within_physics_bounds():
    record = await _get_one_record()
    assert 10.0 <= record["alt"] <= 150.0


@pytest.mark.unit
async def test_heading_within_compass_range():
    record = await _get_one_record()
    assert 0.0 <= record["heading"] < 360.0


@pytest.mark.unit
async def test_motor_status_has_four_motors():
    record = await _get_one_record()
    motors = record["motor_status"]
    assert len(motors) == 4
    for i, motor in enumerate(motors, start=1):
        assert motor["motor_id"] == i
        assert isinstance(motor["output"], float)


@pytest.mark.unit
async def test_battery_drains_over_time():
    """Battery should decrease across consecutive records."""
    queue: asyncio.Queue = asyncio.Queue()
    task = asyncio.create_task(
        stream_drone_physics("drain-test", queue, publish_rate_hz=100)
    )
    records = []
    for _ in range(5):
        records.append(await asyncio.wait_for(queue.get(), timeout=2.0))
    task.cancel()

    batteries = [r["battery_level"] for r in records]
    assert batteries[-1] <= batteries[0], "Battery should not increase over time"
