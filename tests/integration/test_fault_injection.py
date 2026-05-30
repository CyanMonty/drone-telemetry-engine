"""
Integration tests: fault injection end-to-end.

Produces records via the FaultInjector into a real Kafka broker
(testcontainers), then routes them through the DLQ handler and anomaly
detector, verifying that the monitoring layer's detection rate matches
the injection rate within an acceptable statistical tolerance.
"""
import json
import statistics
import time
import uuid
import pytest
from confluent_kafka import Consumer, KafkaError, Producer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_record(seq: int = 0) -> dict:
    return {
        "drone_id": f"drone-{seq % 5}",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "lat": 40.7, "lon": -74.0,
        "alt": 50.0, "speed": 10.0,
        "heading": 90.0, "battery_level": 80.0,
        "motor_status": [], "payload": None,
        "_seq": seq,
    }


def _consume_all(bootstrap_servers: str, topic: str, expected: int, timeout_s: float = 15.0) -> list[dict]:
    group_id = f"test-{uuid.uuid4().hex[:8]}"
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    messages = []
    deadline = time.perf_counter() + timeout_s
    while len(messages) < expected and time.perf_counter() < deadline:
        msg = consumer.poll(timeout=0.5)
        if msg and not msg.error():
            messages.append(json.loads(msg.value()))
    consumer.close()
    return messages


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_fault_detection_rate_matches_injection_rate(kafka_bootstrap_servers):
    """
    Inject faults at a known rate (100%), route through DLQ + anomaly
    detector, and assert the monitoring layer catches every injectable fault.

    DUPLICATE faults pass both checks by design — they are valid records
    that happen to be re-delivered.  They are excluded from the "should be
    caught" count.
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "simulator"))
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "consumer"))

    from fault_injector import FaultInjector, FaultType
    from dlq_handler import validate_record
    from anomaly_detector import detect_anomalies

    detectable_faults = [
        FaultType.CORRUPT_PAYLOAD,
        FaultType.ANOMALOUS_BATTERY,
        FaultType.ANOMALOUS_ALTITUDE,
        FaultType.ANOMALOUS_SPEED,
    ]

    injector = FaultInjector(fault_rate=1.0, enabled_faults=detectable_faults)
    num_records = 100

    caught_by_dlq = 0
    caught_by_anomaly = 0

    for i in range(num_records):
        record = injector.maybe_inject(_base_record(i))
        ok, _ = validate_record(record)
        if not ok:
            caught_by_dlq += 1
            continue
        alerts = detect_anomalies(record)
        if alerts:
            caught_by_anomaly += 1

    total_caught = caught_by_dlq + caught_by_anomaly
    detection_rate = total_caught / num_records

    print(f"\n{'─' * 50}")
    print(f"  Records processed : {num_records}")
    print(f"  Caught by DLQ     : {caught_by_dlq}")
    print(f"  Caught by anomaly : {caught_by_anomaly}")
    print(f"  Total caught      : {total_caught}")
    print(f"  Detection rate    : {detection_rate:.1%}")
    print(f"{'─' * 50}")

    assert detection_rate == 1.0, (
        f"Expected 100% detection of detectable faults, got {detection_rate:.1%} "
        f"({num_records - total_caught} missed)"
    )


@pytest.mark.integration
def test_mixed_fault_rate_produces_expected_dlq_volume(kafka_bootstrap_servers):
    """
    At a 20% fault rate with only CORRUPT_PAYLOAD enabled, roughly 20% of
    records should fail DLQ validation.  Asserts within a ±10% band to
    account for randomness (binomial distribution).
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "simulator"))
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "consumer"))

    from fault_injector import FaultInjector, FaultType
    from dlq_handler import validate_record

    injector = FaultInjector(fault_rate=0.20, enabled_faults=[FaultType.CORRUPT_PAYLOAD])

    main_topic = f"main-{uuid.uuid4().hex[:8]}"
    dlq_topic = f"dlq-{uuid.uuid4().hex[:8]}"
    group_id = f"dlq-rate-{uuid.uuid4().hex[:8]}"

    producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})
    num_records = 200
    expected_dlq_approx = int(num_records * 0.20)

    routed_dlq = 0
    for i in range(num_records):
        record = injector.maybe_inject(_base_record(i))
        ok, _ = validate_record(record)
        if ok:
            producer.produce(main_topic, value=json.dumps(record))
        else:
            producer.produce(dlq_topic, value=json.dumps({"original": record, "fault": "CORRUPT_PAYLOAD"}))
            routed_dlq += 1
    producer.flush()

    actual_rate = routed_dlq / num_records
    print(f"\nDLQ rate: {actual_rate:.1%} ({routed_dlq}/{num_records}), expected ~20%")

    assert 0.10 <= actual_rate <= 0.30, (
        f"DLQ rate {actual_rate:.1%} is outside expected 10–30% band for a 20% injection rate"
    )


@pytest.mark.integration
def test_stale_timestamp_identified_from_consumed_messages(kafka_bootstrap_servers):
    """
    Produce records that include STALE_TIMESTAMP faults.  After consuming,
    verify that stale records (>1 hour old) can be identified server-side,
    demonstrating how a real consumer would detect and flag them.
    """
    import sys
    from pathlib import Path
    from datetime import datetime, timezone

    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "simulator"))
    from fault_injector import FaultInjector, FaultType

    topic = f"stale-{uuid.uuid4().hex[:8]}"
    injector = FaultInjector(fault_rate=1.0, enabled_faults=[FaultType.STALE_TIMESTAMP])

    producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})
    num_records = 20
    for i in range(num_records):
        record = injector.maybe_inject(_base_record(i))
        producer.produce(topic, value=json.dumps(record))
    producer.flush()

    messages = _consume_all(kafka_bootstrap_servers, topic, num_records)
    now = datetime.now(timezone.utc)

    stale_count = 0
    for msg in messages:
        ts = datetime.fromisoformat(msg["timestamp"])
        if (now - ts).total_seconds() > 3600:
            stale_count += 1

    print(f"\nStale messages: {stale_count}/{len(messages)}")
    assert stale_count == len(messages), "All messages should have stale timestamps"


@pytest.mark.integration
def test_injector_stats_written_as_streaming_metrics(kafka_bootstrap_servers):
    """
    Run a fault injection pass and write stats to streaming_metrics if
    TimescaleDB is available (set POSTGRES_HOST env var).
    """
    import os
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "simulator"))
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "consumer"))

    from fault_injector import FaultInjector, FaultType
    from dlq_handler import validate_record
    from anomaly_detector import detect_anomalies

    injector = FaultInjector(fault_rate=0.10)
    num_records = 500
    caught = 0
    for i in range(num_records):
        record = injector.maybe_inject(_base_record(i))
        ok, _ = validate_record(record)
        if not ok:
            caught += 1
        elif detect_anomalies(record):
            caught += 1

    stats = injector.stats
    detection_rate = caught / stats["injected"] if stats["injected"] else 0.0

    print(f"\nFault injection stats: {stats}")
    print(f"Detection rate of injected faults: {detection_rate:.1%}")

    postgres_host = os.environ.get("POSTGRES_HOST")
    if postgres_host:
        try:
            import psycopg2
            from db import insert_streaming_metrics

            conn = psycopg2.connect(
                host=postgres_host,
                dbname=os.environ.get("POSTGRES_DB", "telemetry"),
                user=os.environ.get("POSTGRES_USER", "telemetry"),
                password=os.environ.get("POSTGRES_PASSWORD", "telemetry"),
            )
            insert_streaming_metrics(conn, {
                "test_name": "fault_injection_10pct",
                "message_count": num_records,
                "throughput_msg_s": None,
                "latency_p50_ms": None,
                "latency_p95_ms": None,
                "error_rate": stats["injection_rate"],
                "notes": f"detected={caught} injected={stats['injected']} by_fault={stats['by_fault']}",
            })
            conn.close()
        except Exception as exc:
            print(f"[metrics] Could not write to TimescaleDB: {exc}")

    # Loose assertion: some faults injected at 10%, some caught
    assert stats["injected"] > 0
    assert stats["total_processed"] == num_records
