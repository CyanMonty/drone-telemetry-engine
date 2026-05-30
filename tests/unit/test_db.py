"""
Unit tests for consumer/db.py.

Uses unittest.mock to avoid a real database connection.
"""
import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock, call, patch
import pytest

_DB_PATH = Path(__file__).resolve().parents[2] / "consumer" / "db.py"
_DB_SPEC = importlib.util.spec_from_file_location("consumer_db", _DB_PATH)
consumer_db = importlib.util.module_from_spec(_DB_SPEC)
assert _DB_SPEC.loader is not None
_DB_SPEC.loader.exec_module(consumer_db)

init_db = consumer_db.init_db
insert_telemetry = consumer_db.insert_telemetry


def _make_conn():
    """Return a mock psycopg2 connection with a usable cursor context manager."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


@pytest.mark.unit
def test_init_db_creates_table_and_hypertable():
    conn, cursor = _make_conn()
    init_db(conn)

    executed_sql = [call_args[0][0] for call_args in cursor.execute.call_args_list]
    assert any("CREATE TABLE IF NOT EXISTS drone_telemetry" in sql for sql in executed_sql)
    assert any("create_hypertable" in sql for sql in executed_sql)
    conn.commit.assert_called_once()


@pytest.mark.unit
def test_init_db_creates_drone_id_index():
    conn, cursor = _make_conn()
    init_db(conn)

    executed_sql = [call_args[0][0] for call_args in cursor.execute.call_args_list]
    assert any("CREATE INDEX IF NOT EXISTS" in sql for sql in executed_sql)


@pytest.mark.unit
def test_insert_telemetry_serializes_motor_status():
    conn, cursor = _make_conn()
    record = {
        "drone_id": "d-1",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "lat": 40.7, "lon": -74.0, "alt": 50.0,
        "speed": 10.0, "heading": 90.0, "battery_level": 85.0,
        "motor_status": [{"motor_id": 1, "output": 0.5}],
        "payload": None,
    }
    insert_telemetry(conn, record)

    _, kwargs = cursor.execute.call_args
    # psycopg2 named-param style: second arg is the dict
    passed_row = cursor.execute.call_args[0][1]
    assert passed_row["motor_status"] == json.dumps(record["motor_status"])


@pytest.mark.unit
def test_insert_telemetry_commits():
    conn, cursor = _make_conn()
    record = {
        "drone_id": "d-1",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "lat": 40.7, "lon": -74.0, "alt": 50.0,
        "speed": 10.0, "heading": 90.0, "battery_level": 85.0,
        "motor_status": [],
        "payload": None,
    }
    insert_telemetry(conn, record)
    conn.commit.assert_called_once()
