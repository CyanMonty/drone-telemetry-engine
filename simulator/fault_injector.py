"""
Fault injector for the drone telemetry simulator.

Wraps outgoing telemetry records and probabilistically injects one of several
fault types to simulate real-world failure conditions.  The consumer's DLQ
handler and anomaly detector are the monitoring layer expected to catch them.

Usage (scale mode, 5% fault rate):
    injector = FaultInjector(fault_rate=0.05)
    record = injector.maybe_inject(record)
"""
import copy
import logging
import random
from datetime import datetime, timedelta, timezone
from enum import Enum

log = logging.getLogger(__name__)


class FaultType(str, Enum):
    """Fault types that can be injected into a telemetry record."""
    CORRUPT_PAYLOAD   = "CORRUPT_PAYLOAD"    # removes a required field → caught by DLQ
    ANOMALOUS_BATTERY = "ANOMALOUS_BATTERY"  # battery → near-zero → caught by anomaly detector
    ANOMALOUS_ALTITUDE = "ANOMALOUS_ALTITUDE"  # alt → above ceiling → anomaly detector
    ANOMALOUS_SPEED   = "ANOMALOUS_SPEED"    # speed → above max → anomaly detector
    STALE_TIMESTAMP   = "STALE_TIMESTAMP"    # timestamp → 2 hours ago → stale data alert
    DUPLICATE         = "DUPLICATE"          # record returned unchanged (simulates re-delivery)


_REQUIRED_FIELDS = [
    "drone_id", "timestamp", "lat", "lon",
    "alt", "speed", "heading", "battery_level",
]

_DEFAULT_FAULTS = [
    FaultType.CORRUPT_PAYLOAD,
    FaultType.ANOMALOUS_BATTERY,
    FaultType.ANOMALOUS_ALTITUDE,
    FaultType.ANOMALOUS_SPEED,
    FaultType.STALE_TIMESTAMP,
    FaultType.DUPLICATE,
]


class FaultInjector:
    """
    Probabilistically injects faults into telemetry records.

    Parameters
    ----------
    fault_rate:
        Fraction of records that will have a fault injected (0.0–1.0).
    enabled_faults:
        Which fault types to choose from.  Defaults to all fault types.
    """

    def __init__(
        self,
        fault_rate: float = 0.05,
        enabled_faults: list[FaultType] | None = None,
    ) -> None:
        if not 0.0 <= fault_rate <= 1.0:
            raise ValueError(f"fault_rate must be in [0, 1], got {fault_rate}")
        self.fault_rate = fault_rate
        self.enabled_faults: list[FaultType] = enabled_faults or list(_DEFAULT_FAULTS)
        self.injected_count: int = 0
        self.clean_count: int = 0
        self._fault_counts: dict[FaultType, int] = {f: 0 for f in self.enabled_faults}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def maybe_inject(self, record: dict) -> dict:
        """Return *record* unchanged, or a copy with a fault injected."""
        if random.random() >= self.fault_rate:
            self.clean_count += 1
            return copy.copy(record)

        fault = random.choice(self.enabled_faults)
        mutated = self._apply(copy.copy(record), fault)
        self.injected_count += 1
        self._fault_counts[fault] = self._fault_counts.get(fault, 0) + 1
        log.debug("Injected fault %s into drone %s", fault, record.get("drone_id"))
        return mutated

    @property
    def stats(self) -> dict:
        """Summary of injection activity, suitable for logging or metrics."""
        total = self.injected_count + self.clean_count
        return {
            "total_processed": total,
            "injected": self.injected_count,
            "clean": self.clean_count,
            "injection_rate": self.injected_count / total if total else 0.0,
            "by_fault": dict(self._fault_counts),
        }

    # ------------------------------------------------------------------
    # Fault implementations
    # ------------------------------------------------------------------

    @staticmethod
    def _apply(record: dict, fault: FaultType) -> dict:
        if fault == FaultType.CORRUPT_PAYLOAD:
            field = random.choice(_REQUIRED_FIELDS)
            record.pop(field, None)

        elif fault == FaultType.ANOMALOUS_BATTERY:
            record["battery_level"] = round(random.uniform(0.0, 5.0), 2)

        elif fault == FaultType.ANOMALOUS_ALTITUDE:
            record["alt"] = round(random.uniform(200.0, 500.0), 2)

        elif fault == FaultType.ANOMALOUS_SPEED:
            record["speed"] = round(random.uniform(30.0, 60.0), 2)

        elif fault == FaultType.STALE_TIMESTAMP:
            stale = datetime.now(timezone.utc) - timedelta(hours=random.uniform(2, 24))
            record["timestamp"] = stale.isoformat()

        elif fault == FaultType.DUPLICATE:
            pass  # record is returned as-is — simulates Kafka re-delivery

        return record
