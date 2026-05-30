"""
Standalone demo — no Kafka or Docker required.

Scale mode (default):
    cd simulator
    pip install mavsdk confluent-kafka   # only mavsdk needed if using dev mode
    python demo.py

    # or override defaults:
    NUM_DRONES=20 TELEMETRY_RATE_HZ=4 python demo.py

Dev mode (requires PX4 SITL containers running):
    RUN_MODE=dev PX4_ADDRESSES=udp://localhost:14540 python demo.py
"""

import asyncio
import os
import sys
import time

# Allow running from project root: python simulator/demo.py
sys.path.insert(0, os.path.dirname(__file__))

NUM_DRONES = int(os.environ.get("NUM_DRONES", "5"))
TELEMETRY_RATE_HZ = float(os.environ.get("TELEMETRY_RATE_HZ", "2"))
STATS_INTERVAL = float(os.environ.get("STATS_INTERVAL", "2"))
RUN_MODE = os.environ.get("RUN_MODE", "scale").lower()

# ANSI helpers
_CLEAR = "\033[2J\033[H"
_BOLD = "\033[1m"
_RESET = "\033[0m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_GREEN = "\033[32m"
_DIM = "\033[2m"


def _bat_color(pct: float) -> str:
    if pct < 20:
        return _RED
    if pct < 50:
        return _YELLOW
    return _GREEN


async def stats_printer(queue: asyncio.Queue, num_drones: int) -> None:
    state: dict = {}
    total = 0
    start = time.monotonic()
    last_print = start
    last_count = 0

    while True:
        # Drain as many messages as are available without blocking
        drained = 0
        while drained < 200:
            try:
                record = queue.get_nowait()
                state[record["drone_id"]] = record
                total += 1
                drained += 1
            except asyncio.QueueEmpty:
                break

        await asyncio.sleep(0.05)
        now = time.monotonic()

        if now - last_print < STATS_INTERVAL or not state:
            continue

        elapsed = now - start
        window = now - last_print
        rate = (total - last_count) / window
        last_count = total
        last_print = now

        rows = sorted(state.values(), key=lambda r: r["drone_id"])

        # Aggregate
        avg_bat = sum(r["battery_level"] for r in rows) / len(rows)
        avg_alt = sum(r["alt"] for r in rows) / len(rows)
        avg_spd = sum(r["speed"] for r in rows) / len(rows)
        low_bat = min(rows, key=lambda r: r["battery_level"])

        print(_CLEAR, end="", flush=True)
        mode_label = f"[{RUN_MODE}]"
        print(
            f"{_BOLD}=== Drone Telemetry Demo {mode_label} — "
            f"{num_drones} drones @ {TELEMETRY_RATE_HZ:.0f} Hz ==={_RESET}"
        )
        print(
            f"{'Drone':<14} {'Lat':>10} {'Lon':>11} {'Alt m':>7} "
            f"{'m/s':>6} {'Hdg°':>6} {'Battery':>9}"
        )
        print("─" * 68)
        for r in rows:
            bat = r["battery_level"]
            col = _bat_color(bat)
            print(
                f"{r['drone_id']:<14} {r['lat']:>10.5f} {r['lon']:>11.5f} "
                f"{r['alt']:>7.1f} {r['speed']:>6.1f} {r['heading']:>6.1f} "
                f"{col}{bat:>8.1f}%{_RESET}"
            )
        print("─" * 68)
        print(
            f"Avg  bat {_bat_color(avg_bat)}{avg_bat:.1f}%{_RESET}"
            f"  alt {avg_alt:.1f} m"
            f"  spd {avg_spd:.1f} m/s"
            f"  {_DIM}|{_RESET}"
            f"  {rate:.1f} msg/s  total {total}  uptime {elapsed:.0f}s"
            f"  {_DIM}Ctrl+C to stop{_RESET}"
        )
        if low_bat["battery_level"] < 20:
            print(
                f"{_RED}  ⚠  Low battery: {low_bat['drone_id']} "
                f"({low_bat['battery_level']:.1f}%){_RESET}"
            )


async def main() -> None:
    queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    tasks = [asyncio.create_task(stats_printer(queue, NUM_DRONES))]

    if RUN_MODE == "dev":
        from drone import stream_drone_telemetry

        addresses = [
            a.strip()
            for a in os.environ.get("PX4_ADDRESSES", "udp://localhost:14540").split(",")
        ]
        print(f"[dev] Connecting to {len(addresses)} PX4 SITL instance(s)...")
        for i, address in enumerate(addresses):
            tasks.append(
                asyncio.create_task(
                    stream_drone_telemetry(f"drone-{i:03d}", address, queue, TELEMETRY_RATE_HZ)
                )
            )
    else:
        from drone_physics import stream_drone_physics

        print(f"[scale] Starting {NUM_DRONES} simulated drones ...")
        for i in range(NUM_DRONES):
            tasks.append(
                asyncio.create_task(
                    stream_drone_physics(f"drone-{i:04d}", queue, TELEMETRY_RATE_HZ)
                )
            )

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
