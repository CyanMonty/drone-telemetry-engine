"""
Shared fixtures for integration tests.

Spins up a real Kafka broker via testcontainers so integration tests have
no dependency on a running docker-compose stack.
"""
import pytest
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="session")
def kafka_bootstrap_servers():
    """Start a Kafka container once per test session and yield its bootstrap address."""
    with KafkaContainer() as kafka:
        yield kafka.get_bootstrap_server()
