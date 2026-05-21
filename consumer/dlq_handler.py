"""
Dead-letter-queue validation for drone telemetry records.

validate_record() is the single entry point: it checks that all required
fields are present before the record is written to the database.
Messages that fail validation should be routed to the DLQ topic instead
of being silently dropped.
"""

REQUIRED_FIELDS: frozenset[str] = frozenset({
    "drone_id",
    "timestamp",
    "lat",
    "lon",
    "alt",
    "speed",
    "heading",
    "battery_level",
})


def validate_record(record: dict) -> tuple[bool, str | None]:
    """
    Return (True, None) if *record* contains all required fields.
    Return (False, reason) otherwise, where reason names the missing fields.
    Extra/unknown fields are allowed.
    """
    missing = sorted(REQUIRED_FIELDS - record.keys())
    if missing:
        return False, f"Missing fields: {missing}"
    return True, None
