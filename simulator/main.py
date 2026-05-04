import asyncio
import json
import logging
import os

from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "drone-telemetry")
TELEMETRY_RATE_HZ = float(os.environ.get("TELEMETRY_RATE_HZ", "2"))

# --- dev mode ---
# Comma-separated MAVSDK system addresses, one per PX4 SITL container (1–5).
PX4_ADDRESSES = os.environ.get(
    "PX4_ADDRESSES",
    "udp://px4-sitl-0:14540",
)

# --- scale mode ---
NUM_DRONES = int(os.environ.get("NUM_DRONES", "100"))

RUN_MODE = os.environ.get("RUN_MODE", "scale").lower()
if RUN_MODE not in ("dev", "scale"):
    raise ValueError(f"RUN_MODE must be 'dev' or 'scale', got '{RUN_MODE}'")


STATS_INTERVAL = float(os.environ.get("STATS_INTERVAL", "30"))


async def kafka_writer(queue: asyncio.Queue, producer: Producer) -> None:
    loop = asyncio.get_running_loop()
    while True:
        record = await queue.get()
        await loop.run_in_executor(
            None,
            lambda r=record: producer.produce(
                KAFKA_TOPIC, key=r["drone_id"], value=json.dumps(r)
            ),
        )
        producer.poll(0)


async def stats_logger(queue: asyncio.Queue) -> None:
    """Periodically logs aggregate telemetry stats to stdout (visible in docker logs)."""
    import time

    state: dict = {}
    total = 0
    last = time.monotonic()
    last_count = 0

    while True:
        try:
            record = queue.get_nowait()
            state[record["drone_id"]] = record
            total += 1
        except asyncio.QueueEmpty:
            await asyncio.sleep(0.05)
            now = time.monotonic()
            if now - last >= STATS_INTERVAL and state:
                rows = list(state.values())
                avg_bat = sum(r["battery_level"] for r in rows) / len(rows)
                avg_alt = sum(r["alt"] for r in rows) / len(rows)
                avg_spd = sum(r["speed"] for r in rows) / len(rows)
                rate = (total - last_count) / (now - last)
                last_count = total
                last = now
                log.info(
                    "stats | drones=%d  avg_battery=%.1f%%  avg_alt=%.1fm"
                    "  avg_speed=%.1fm/s  msg/s=%.1f  total=%d",
                    len(rows), avg_bat, avg_alt, avg_spd, rate, total,
                )


async def main() -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    kafka_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    stats_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    shared_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)

    async def relay() -> None:
        """Fan each record out to the Kafka writer and the stats logger."""
        while True:
            record = await shared_queue.get()
            await kafka_queue.put(record)
            try:
                stats_queue.put_nowait(record)
            except asyncio.QueueFull:
                pass  # drop stats copy under backpressure; never drop Kafka

    tasks = [
        asyncio.create_task(relay()),
        asyncio.create_task(kafka_writer(kafka_queue, producer)),
        asyncio.create_task(stats_logger(stats_queue)),
    ]

    if RUN_MODE == "dev":
        from drone import stream_drone_telemetry

        addresses = [a.strip() for a in PX4_ADDRESSES.split(",")]
        log.info(
            "[dev] Bridging %d PX4 SITL instance(s) → Kafka topic '%s' at %.1f Hz",
            len(addresses),
            KAFKA_TOPIC,
            TELEMETRY_RATE_HZ,
        )
        for i, address in enumerate(addresses):
            tasks.append(
                asyncio.create_task(
                    stream_drone_telemetry(f"drone-{i:03d}", address, shared_queue, TELEMETRY_RATE_HZ)
                )
            )

    else:  # scale
        from drone_physics import stream_drone_physics

        log.info(
            "[scale] Simulating %d drones → Kafka topic '%s' at %.1f Hz",
            NUM_DRONES,
            KAFKA_TOPIC,
            TELEMETRY_RATE_HZ,
        )
        for i in range(NUM_DRONES):
            tasks.append(
                asyncio.create_task(
                    stream_drone_physics(f"drone-{i:04d}", shared_queue, TELEMETRY_RATE_HZ)
                )
            )

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
