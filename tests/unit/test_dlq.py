"""
TDD tests for dlq_handler.validate_record.

These tests define the contract BEFORE the module exists.
Run `pytest -m unit` — they will be RED until consumer/dlq_handler.py is created.
"""
import pytest

from dlq_handler import validate_record


def _valid_record(**overrides) -> dict:
    base = {
        "drone_id": "drone-1",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "lat": 40.7, "lon": -74.0,
        "alt": 50.0, "speed": 10.0,
        "heading": 90.0, "battery_level": 80.0,
    }
    base.update(overrides)
    return base


@pytest.mark.unit
def test_valid_record_passes():
    ok, reason = validate_record(_valid_record())
    assert ok is True
    assert reason is None


@pytest.mark.unit
def test_missing_drone_id_fails():
    record = _valid_record()
    del record["drone_id"]
    ok, reason = validate_record(record)
    assert ok is False
    assert reason is not None


@pytest.mark.unit
def test_missing_timestamp_fails():
    record = _valid_record()
    del record["timestamp"]
    ok, reason = validate_record(record)
    assert ok is False


@pytest.mark.unit
def test_multiple_missing_fields_reported():
    record = _valid_record()
    del record["drone_id"]
    del record["lat"]
    ok, reason = validate_record(record)
    assert ok is False
    assert "drone_id" in reason
    assert "lat" in reason


@pytest.mark.unit
def test_extra_unknown_fields_are_allowed():
    """Unknown fields (e.g. from future schema additions) should not fail validation."""
    record = _valid_record(extra_field="whatever", another=123)
    ok, reason = validate_record(record)
    assert ok is True


@pytest.mark.unit
def test_empty_record_fails():
    ok, reason = validate_record({})
    assert ok is False


@pytest.mark.unit
def test_reason_is_none_for_valid_record():
    _, reason = validate_record(_valid_record())
    assert reason is None
