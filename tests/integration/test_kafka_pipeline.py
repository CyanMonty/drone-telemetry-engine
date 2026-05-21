"""
Integration tests for the Kafka telemetry pipeline.

Measures end-to-end throughput and latency using a real Kafka broker
(via testcontainers). Results are printed to stdout so they appear in
the pytest-html report and can be written to streaming_metrics in
TimescaleDB when the full compose stack is running.
"""
import json
import statistics
import time
import uuid
import pytest
from confluent_kafka import Consumer, KafkaError, Producer

TOPIC = "test-drone-telemetry"


def _make_record(seq: int) -> dict:
    return {
        "drone_id": f"drone-{seq % 10}",
        "timestamp": "",  # filled at produce time
        "lat": 40.7 + seq * 0.0001,
        "lon": -74.0,
        "alt": 50.0,
        "speed": 10.0,
        "heading": 90.0,
        "battery_level": 80.0,
        "motor_status": [],
        "payload": None,
        "_seq": seq,
        "_produced_ms": 0.0,  # filled at produce time
    }


@pytest.mark.integration
def test_pipeline_throughput_and_latency(kafka_bootstrap_servers):
    """
    Produce 500 messages to Kafka, consume them all, then assert:
    - Zero message loss
    - P95 end-to-end latency < 5 000 ms (very generous for a cold container)

    Metrics printed here appear in the pytest-html report.
    If POSTGRES_HOST is set in the environment, results are also written
    to the streaming_metrics table in TimescaleDB.
    """
    num_messages = 500
    group_id = f"test-group-{uuid.uuid4().hex[:8]}"

    producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})

    produce_start = time.perf_counter()
    for i in range(num_messages):
        record = _make_record(i)
        record["_produced_ms"] = time.time() * 1000
        producer.produce(TOPIC, key=str(i), value=json.dumps(record))
    producer.flush()
    produce_elapsed = time.perf_counter() - produce_start

    consumer = Consumer({
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "true",
    })
    consumer.subscribe([TOPIC])

    latencies_ms: list[float] = []
    received = 0
    deadline = time.perf_counter() + 30.0  # 30 s hard timeout

    while received < num_messages and time.perf_counter() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                pytest.fail(f"Kafka error: {msg.error()}")
            continue
        consumed_ms = time.time() * 1000
        data = json.loads(msg.value())
        latencies_ms.append(consumed_ms - data["_produced_ms"])
        received += 1

    consumer.close()

    # --- metrics ---
    total_elapsed = time.perf_counter() - produce_start
    throughput = received / total_elapsed if total_elapsed > 0 else 0.0
    p50 = statistics.median(latencies_ms) if latencies_ms else float("inf")
    p95 = (
        statistics.quantiles(latencies_ms, n=20)[18]
        if len(latencies_ms) >= 20
        else max(latencies_ms, default=float("inf"))
    )
    error_rate = (num_messages - received) / num_messages

    print(f"\n{'─' * 50}")
    print(f"  Messages produced : {num_messages}")
    print(f"  Messages received : {received}")
    print(f"  Throughput        : {throughput:.1f} msg/s")
    print(f"  Latency P50       : {p50:.1f} ms")
    print(f"  Latency P95       : {p95:.1f} ms")
    print(f"  Error rate        : {error_rate:.2%}")
    print(f"{'─' * 50}")

    _maybe_record_metrics(
        test_name="test_pipeline_throughput_and_latency",
        message_count=num_messages,
        throughput_msg_s=round(throughput, 2),
        latency_p50_ms=round(p50, 2),
        latency_p95_ms=round(p95, 2),
        error_rate=round(error_rate, 4),
    )

    assert received == num_messages, f"Lost {num_messages - received} messages"
    assert p95 < 5000, f"P95 latency {p95:.1f} ms exceeds 5 000 ms threshold"


@pytest.mark.integration
def test_anomaly_routing_to_alert_topic(kafka_bootstrap_servers):
    """
    Produce a mix of healthy and anomalous records, run them through the
    anomaly detector, and verify that only anomalous records generate alerts.
    This test does NOT require a running consumer service — it exercises
    the anomaly_detector module directly.
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "consumer"))
    from anomaly_detector import detect_anomalies

    healthy = {
        "drone_id": "d-1", "timestamp": "2026-01-01T00:00:00+00:00",
        "lat": 40.7, "lon": -74.0, "alt": 50.0, "speed": 10.0,
        "heading": 90.0, "battery_level": 80.0,
        "motor_status": [], "payload": None,
    }
    critical = {**healthy, "battery_level": 5.0, "alt": 200.0}

    assert detect_anomalies(healthy) == []
    alerts = detect_anomalies(critical)
    alert_types = {a["type"] for a in alerts}
    assert "LOW_BATTERY" in alert_types
    assert "ALT_HIGH" in alert_types

    # Produce both to Kafka — simulates what a real router service would do
    group_id = f"alert-test-{uuid.uuid4().hex[:8]}"
    alert_topic = "test-drone-alerts"
    raw_topic = "test-drone-raw"

    producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})
    for record in [healthy, critical]:
        producer.produce(raw_topic, key=record["drone_id"], value=json.dumps(record))
        for alert in detect_anomalies(record):
            producer.produce(alert_topic, key=alert["drone_id"], value=json.dumps(alert))
    producer.flush()

    # Consume alert topic and confirm we got exactly 2 alerts (LOW_BATTERY + ALT_HIGH)
    consumer = Consumer({
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([alert_topic])

    received_alerts = []
    deadline = time.perf_counter() + 10.0
    while len(received_alerts) < len(alerts) and time.perf_counter() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg and not msg.error():
            received_alerts.append(json.loads(msg.value()))
    consumer.close()

    assert len(received_alerts) == len(alerts)


@pytest.mark.integration
def test_dlq_routing_for_malformed_messages(kafka_bootstrap_servers):
    """
    Produce a mix of valid and malformed messages.  Verify that malformed
    messages are correctly identified by dlq_handler.validate_record and
    would be routed to the DLQ topic.
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "consumer"))
    from dlq_handler import validate_record

    messages = [
        # valid
        {"drone_id": "d-1", "timestamp": "t", "lat": 0, "lon": 0,
         "alt": 50, "speed": 10, "heading": 90, "battery_level": 80},
        # missing drone_id
        {"timestamp": "t", "lat": 0, "lon": 0,
         "alt": 50, "speed": 10, "heading": 90, "battery_level": 80},
        # completely empty
        {},
    ]

    dlq_topic = "test-drone-dlq"
    main_topic = "test-drone-valid"
    group_id = f"dlq-test-{uuid.uuid4().hex[:8]}"

    producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})
    routed_to_dlq = 0
    routed_to_main = 0

    for msg in messages:
        ok, _ = validate_record(msg)
        if ok:
            producer.produce(main_topic, value=json.dumps(msg))
            routed_to_main += 1
        else:
            producer.produce(dlq_topic, value=json.dumps(msg))
            routed_to_dlq += 1
    producer.flush()

    assert routed_to_main == 1
    assert routed_to_dlq == 2

    # Consume the DLQ and confirm content
    consumer = Consumer({
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([dlq_topic])

    dlq_messages = []
    deadline = time.perf_counter() + 10.0
    while len(dlq_messages) < routed_to_dlq and time.perf_counter() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg and not msg.error():
            dlq_messages.append(json.loads(msg.value()))
    consumer.close()

    assert len(dlq_messages) == 2


def _maybe_record_metrics(**kwargs) -> None:
    """Write metrics to TimescaleDB if POSTGRES_HOST is available in the environment."""
    import os
    host = os.environ.get("POSTGRES_HOST")
    if not host:
        return
    try:
        import psycopg2
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "consumer"))
        from db import insert_streaming_metrics

        conn = psycopg2.connect(
            host=host,
            dbname=os.environ.get("POSTGRES_DB", "telemetry"),
            user=os.environ.get("POSTGRES_USER", "telemetry"),
            password=os.environ.get("POSTGRES_PASSWORD", "telemetry"),
        )
        insert_streaming_metrics(conn, kwargs)
        conn.close()
    except Exception as exc:
        print(f"\n[metrics] Could not write to TimescaleDB: {exc}")
