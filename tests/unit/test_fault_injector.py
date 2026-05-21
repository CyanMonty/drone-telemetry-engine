"""
TDD tests for simulator/fault_injector.FaultInjector.

These are RED until simulator/fault_injector.py is created.

Design contract:
  - FaultInjector(fault_rate, enabled_faults) wraps records.
  - maybe_inject(record) returns the (possibly mutated) record.
  - A record is never mutated in-place — a copy is returned.
  - With fault_rate=1.0 a fault is ALWAYS injected.
  - With fault_rate=0.0 no fault is EVER injected.
  - Each fault type maps to a specific, predictable mutation detectable
    by the existing DLQ handler or anomaly detector.
"""
import pytest

from fault_injector import FaultInjector, FaultType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _record(**overrides) -> dict:
    base = {
        "drone_id": "drone-1",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "lat": 40.7, "lon": -74.0,
        "alt": 50.0, "speed": 10.0,
        "heading": 90.0, "battery_level": 80.0,
        "motor_status": [], "payload": None,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Basic behaviour
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_rate_zero_never_injects():
    injector = FaultInjector(fault_rate=0.0)
    original = _record()
    for _ in range(50):
        result = injector.maybe_inject(_record())
        assert result == original


@pytest.mark.unit
def test_rate_one_always_injects():
    injector = FaultInjector(fault_rate=1.0)
    for _ in range(20):
        result = injector.maybe_inject(_record())
        assert result != _record(), "Expected a mutation but record was unchanged"


@pytest.mark.unit
def test_original_record_not_mutated():
    """maybe_inject must return a copy — never mutate the input."""
    injector = FaultInjector(fault_rate=1.0, enabled_faults=[FaultType.CORRUPT_PAYLOAD])
    original = _record()
    original_copy = dict(original)
    injector.maybe_inject(original)
    assert original == original_copy


# ---------------------------------------------------------------------------
# CORRUPT_PAYLOAD: removes a required field → caught by dlq_handler
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_corrupt_payload_removes_a_required_field():
    from dlq_handler import validate_record, REQUIRED_FIELDS

    injector = FaultInjector(fault_rate=1.0, enabled_faults=[FaultType.CORRUPT_PAYLOAD])
    result = injector.maybe_inject(_record())

    ok, reason = validate_record(result)
    assert ok is False, "DLQ handler should reject a CORRUPT_PAYLOAD record"


# ---------------------------------------------------------------------------
# ANOMALOUS_BATTERY: battery set below LOW_BATTERY threshold
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_anomalous_battery_triggers_low_battery_alert():
    from anomaly_detector import detect_anomalies, BATTERY_LOW

    injector = FaultInjector(fault_rate=1.0, enabled_faults=[FaultType.ANOMALOUS_BATTERY])
    result = injector.maybe_inject(_record())

    assert result["battery_level"] < BATTERY_LOW
    alerts = detect_anomalies(result)
    assert any(a["type"] == "LOW_BATTERY" for a in alerts)


# ---------------------------------------------------------------------------
# ANOMALOUS_ALTITUDE: altitude set above ALT_MAX threshold
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_anomalous_altitude_triggers_alt_high_alert():
    from anomaly_detector import detect_anomalies, ALT_MAX

    injector = FaultInjector(fault_rate=1.0, enabled_faults=[FaultType.ANOMALOUS_ALTITUDE])
    result = injector.maybe_inject(_record())

    assert result["alt"] > ALT_MAX
    alerts = detect_anomalies(result)
    assert any(a["type"] == "ALT_HIGH" for a in alerts)


# ---------------------------------------------------------------------------
# ANOMALOUS_SPEED: speed set above SPEED_MAX threshold
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_anomalous_speed_triggers_speed_high_alert():
    from anomaly_detector import detect_anomalies, SPEED_MAX

    injector = FaultInjector(fault_rate=1.0, enabled_faults=[FaultType.ANOMALOUS_SPEED])
    result = injector.maybe_inject(_record())

    assert result["speed"] > SPEED_MAX
    alerts = detect_anomalies(result)
    assert any(a["type"] == "SPEED_HIGH" for a in alerts)


# ---------------------------------------------------------------------------
# STALE_TIMESTAMP: timestamp moved far into the past
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_stale_timestamp_is_in_the_past():
    from datetime import datetime, timezone

    injector = FaultInjector(fault_rate=1.0, enabled_faults=[FaultType.STALE_TIMESTAMP])
    result = injector.maybe_inject(_record())

    ts = datetime.fromisoformat(result["timestamp"])
    now = datetime.now(timezone.utc)
    age_seconds = (now - ts).total_seconds()
    assert age_seconds > 3600, f"Expected timestamp to be >1 hour old, was {age_seconds:.0f}s ago"


# ---------------------------------------------------------------------------
# DUPLICATE: returned record is identical to original (simulates re-delivery)
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_duplicate_fault_returns_identical_copy():
    injector = FaultInjector(fault_rate=1.0, enabled_faults=[FaultType.DUPLICATE])
    original = _record()
    result = injector.maybe_inject(original)
    assert result == original
    assert result is not original  # still a copy, not the same object


# ---------------------------------------------------------------------------
# Fault selection: only enabled faults are used
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_only_enabled_faults_are_applied():
    """If only ANOMALOUS_BATTERY is enabled, the anomaly must be a battery fault."""
    from anomaly_detector import detect_anomalies

    injector = FaultInjector(fault_rate=1.0, enabled_faults=[FaultType.ANOMALOUS_BATTERY])
    for _ in range(20):
        result = injector.maybe_inject(_record())
        alerts = detect_anomalies(result)
        assert any(a["type"] == "LOW_BATTERY" for a in alerts)


# ---------------------------------------------------------------------------
# Fault stats tracking
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_injector_tracks_injection_count():
    injector = FaultInjector(fault_rate=1.0)
    for _ in range(10):
        injector.maybe_inject(_record())
    assert injector.injected_count == 10


@pytest.mark.unit
def test_injector_tracks_pass_through_count():
    injector = FaultInjector(fault_rate=0.0)
    for _ in range(10):
        injector.maybe_inject(_record())
    assert injector.clean_count == 10


@pytest.mark.unit
def test_injector_total_count():
    injector = FaultInjector(fault_rate=0.5)
    for _ in range(100):
        injector.maybe_inject(_record())
    assert injector.injected_count + injector.clean_count == 100
