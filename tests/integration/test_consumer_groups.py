"""
Integration tests for Kafka consumer group behaviour.

Verifies that when multiple consumers join the same group, Kafka distributes
partitions between them — each partition is owned by exactly one consumer.
"""
import json
import time
import uuid
import pytest
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


def _create_topic(bootstrap_servers: str, topic: str, num_partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    fs = admin.create_topics([NewTopic(topic, num_partitions=num_partitions, replication_factor=1)])
    for t, f in fs.items():
        try:
            f.result()
        except Exception:
            pass  # topic may already exist


@pytest.mark.integration
def test_consumer_group_partitions_assigned(kafka_bootstrap_servers):
    """
    Create a topic with 4 partitions, start 2 consumers in the same group,
    produce 40 messages evenly across partitions, and assert:
    - Both consumers received messages (partition assignment happened)
    - Total received == total produced (no message loss)
    """
    group_id = f"cg-test-{uuid.uuid4().hex[:8]}"
    topic = f"cg-topic-{uuid.uuid4().hex[:8]}"
    num_partitions = 4
    num_messages = 40

    _create_topic(kafka_bootstrap_servers, topic, num_partitions)
    time.sleep(1.0)  # let the topic propagate

    producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})
    for i in range(num_messages):
        producer.produce(topic, key=str(i % num_partitions), value=json.dumps({"seq": i}))
    producer.flush()

    consumer_cfg = {
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "true",
        "session.timeout.ms": "6000",
    }
    c1 = Consumer(consumer_cfg)
    c2 = Consumer(consumer_cfg)
    c1.subscribe([topic])
    c2.subscribe([topic])

    counts = {c1: 0, c2: 0}
    deadline = time.perf_counter() + 20.0

    while sum(counts.values()) < num_messages and time.perf_counter() < deadline:
        for c in (c1, c2):
            msg = c.poll(timeout=0.1)
            if msg and not msg.error():
                counts[c] += 1

    c1.close()
    c2.close()

    total = sum(counts.values())
    print(f"\nConsumer 1 received: {counts[c1]}")
    print(f"Consumer 2 received: {counts[c2]}")
    print(f"Total: {total}/{num_messages}")

    assert total == num_messages, f"Lost {num_messages - total} messages"
    assert counts[c1] > 0, "Consumer 1 received no messages — partition assignment may have failed"
    assert counts[c2] > 0, "Consumer 2 received no messages — partition assignment may have failed"


@pytest.mark.integration
def test_consumer_group_no_duplicate_delivery(kafka_bootstrap_servers):
    """
    Two consumers in the same group should not receive the same message.
    Sequence numbers must be unique across both consumers.
    """
    group_id = f"dedup-test-{uuid.uuid4().hex[:8]}"
    topic = f"dedup-topic-{uuid.uuid4().hex[:8]}"
    num_messages = 20

    _create_topic(kafka_bootstrap_servers, topic, 2)
    time.sleep(1.0)

    producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})
    for i in range(num_messages):
        producer.produce(topic, key=str(i), value=json.dumps({"seq": i}))
    producer.flush()

    consumer_cfg = {
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "true",
        "session.timeout.ms": "6000",
    }
    c1 = Consumer(consumer_cfg)
    c2 = Consumer(consumer_cfg)
    c1.subscribe([topic])
    c2.subscribe([topic])

    seen: set[int] = set()
    duplicates = 0
    deadline = time.perf_counter() + 20.0

    while len(seen) < num_messages and time.perf_counter() < deadline:
        for c in (c1, c2):
            msg = c.poll(timeout=0.1)
            if msg and not msg.error():
                seq = json.loads(msg.value())["seq"]
                if seq in seen:
                    duplicates += 1
                seen.add(seq)

    c1.close()
    c2.close()

    print(f"\nUnique messages: {len(seen)}/{num_messages}, duplicates: {duplicates}")
    assert duplicates == 0, f"{duplicates} duplicate message(s) delivered to same consumer group"
    assert len(seen) == num_messages
