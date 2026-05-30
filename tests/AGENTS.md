# tests — agent notes

## Purpose

Contains unit and integration tests for all services. Tests run without modifying service code — modules are imported via `sys.path` patching in `conftest.py`.

## Structure

```
tests/
  conftest.py              # sys.path patching — makes all services importable
  requirements-test.txt    # test dependencies
  unit/                    # no external dependencies (no Docker, no Kafka, no DB)
  integration/             # requires Docker (Testcontainers spins up Kafka and Postgres)
```

## Running tests

```bash
# Install dependencies
pip install -r tests/requirements-test.txt

# Unit tests only (fast, no Docker needed)
pytest -m unit

# All tests (requires Docker)
pytest
```

Reports are written to `reports/test-report.html` (HTML, self-contained).

## Test markers

| Marker | When to use |
|---|---|
| `@pytest.mark.unit` | No external dependencies |
| `@pytest.mark.integration` | Requires Docker / Testcontainers |

Every test must have exactly one marker. pytest will warn about unmarked tests.

## `conftest.py` — sys.path patching

`tests/conftest.py` inserts `simulator/`, `consumer/`, and `proximity-consumer/` into `sys.path` so tests can do `from anomaly_detector import detect_anomalies` matching how services import each other at runtime inside Docker.

Do not add package `__init__.py` files to the service directories — that would break the flat-import convention.

## Integration test setup (`tests/integration/conftest.py`)

Testcontainers spins up:
- **Kafka** — `apache/kafka:3.9.0` — available via `kafka_bootstrap_servers` fixture
- **Postgres/TimescaleDB** — `timescale/timescaledb-ha:pg16` — available via `pg_conn` fixture

Fixtures are session-scoped to avoid restarting containers between tests in the same session.

## Agent guidance

- Unit tests for `anomaly_detector`, `dlq_handler`, `windowed_agg`, `fault_injector`, and `proximity_detector` (haversine) do not need a DB or Kafka connection — keep them that way.
- When adding a new service module, add its directory to the `sys.path` block in `tests/conftest.py`.
- Integration tests that create DB tables should use the `init_db()` calls from the service under test — do not hand-craft DDL in test files.
- `asyncio_mode = auto` is set in `pytest.ini` — test functions can be `async def` without extra decoration.
- The `reports/` directory is gitignored; do not commit test report HTML files.
