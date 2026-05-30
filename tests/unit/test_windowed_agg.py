"""
TDD tests for windowed_agg.WindowedAggregator.

These tests define the contract BEFORE the module exists.
Run `pytest -m unit` — they will be RED until consumer/windowed_agg.py is created.
"""
import pytest

from windowed_agg import WindowedAggregator


def _record(drone_id: str = "d-1", speed=10.0, alt=50.0, battery=80.0) -> dict:
    return {
        "drone_id": drone_id,
        "timestamp": "2026-01-01T00:00:00+00:00",
        "speed": speed,
        "alt": alt,
        "battery_level": battery,
    }


@pytest.mark.unit
def test_single_record_returns_its_own_values():
    agg = WindowedAggregator(window_seconds=10.0)
    result = agg.update(_record(speed=12.0, alt=55.0, battery=90.0))
    assert result["avg_speed"] == pytest.approx(12.0)
    assert result["avg_alt"] == pytest.approx(55.0)
    assert result["avg_battery"] == pytest.approx(90.0)


@pytest.mark.unit
def test_two_records_average_correctly():
    agg = WindowedAggregator(window_seconds=10.0)
    agg.update(_record(speed=10.0, alt=40.0, battery=70.0))
    result = agg.update(_record(speed=20.0, alt=60.0, battery=90.0))
    assert result["avg_speed"] == pytest.approx(15.0)
    assert result["avg_alt"] == pytest.approx(50.0)
    assert result["avg_battery"] == pytest.approx(80.0)
    assert result["sample_count"] == 2


@pytest.mark.unit
def test_result_contains_drone_id_and_window_seconds():
    agg = WindowedAggregator(window_seconds=30.0)
    result = agg.update(_record(drone_id="alpha"))
    assert result["drone_id"] == "alpha"
    assert result["window_seconds"] == 30.0


@pytest.mark.unit
def test_multiple_drones_tracked_independently():
    agg = WindowedAggregator(window_seconds=10.0)
    agg.update(_record(drone_id="a", speed=5.0, alt=20.0, battery=50.0))
    agg.update(_record(drone_id="b", speed=15.0, alt=80.0, battery=95.0))

    result_a = agg.update(_record(drone_id="a", speed=5.0, alt=20.0, battery=50.0))
    result_b = agg.update(_record(drone_id="b", speed=15.0, alt=80.0, battery=95.0))

    assert result_a["avg_speed"] == pytest.approx(5.0)
    assert result_b["avg_speed"] == pytest.approx(15.0)


@pytest.mark.unit
def test_old_records_outside_window_are_evicted(monkeypatch):
    """Records older than window_seconds should be dropped from the average."""
    import time as time_module

    fake_time = [0.0]

    def mock_monotonic():
        return fake_time[0]

    import windowed_agg
    monkeypatch.setattr(windowed_agg.time, "monotonic", mock_monotonic)

    agg = WindowedAggregator(window_seconds=10.0)

    fake_time[0] = 0.0
    agg.update(_record(speed=100.0, alt=200.0, battery=10.0))

    # Advance time past the window — the first record should be evicted
    fake_time[0] = 11.0
    result = agg.update(_record(speed=10.0, alt=50.0, battery=80.0))

    assert result["sample_count"] == 1
    assert result["avg_speed"] == pytest.approx(10.0)


@pytest.mark.unit
def test_default_window_is_ten_seconds():
    agg = WindowedAggregator()
    assert agg.window_seconds == 10.0
