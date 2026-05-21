"""
Rolling-window aggregator for drone telemetry.

WindowedAggregator keeps a per-drone deque of (monotonic_time, record) pairs.
On each update() call it evicts records older than *window_seconds* and returns
the current per-drone averages.
"""
import time
from collections import defaultdict, deque


class WindowedAggregator:
    def __init__(self, window_seconds: float = 10.0) -> None:
        self.window_seconds = window_seconds
        self._windows: dict[str, deque] = defaultdict(deque)

    def update(self, record: dict) -> dict:
        """Add *record* to the window and return the current aggregate for its drone."""
        drone_id = record["drone_id"]
        now = time.monotonic()
        window = self._windows[drone_id]
        window.append((now, record))

        cutoff = now - self.window_seconds
        while window and window[0][0] < cutoff:
            window.popleft()

        records = [r for _, r in window]
        n = len(records)
        return {
            "drone_id": drone_id,
            "timestamp": record["timestamp"],
            "window_seconds": self.window_seconds,
            "sample_count": n,
            "avg_speed": sum(r["speed"] for r in records) / n,
            "avg_alt": sum(r["alt"] for r in records) / n,
            "avg_battery": sum(r["battery_level"] for r in records) / n,
        }
