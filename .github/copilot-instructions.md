# drone-telemetry-engine — Copilot instructions

## Project overview

Real-time drone swarm telemetry pipeline:
- **simulator** — produces synthetic or PX4-bridged telemetry to Kafka
- **consumer** — persists telemetry to TimescaleDB; anomaly detection + DLQ
- **proximity-consumer** — detects drone pairs within a threshold distance, publishes alerts to Kafka
- **grafana** — pre-provisioned dashboards (drone-telemetry, drone-map, streaming-test-metrics)
- **tests/** — unit tests (no external deps) and integration tests (Testcontainers)

## Key conventions

- Each service runs as a standalone Python module from its own directory (no package prefix).
- `sys.path` patching in `tests/conftest.py` makes simulator, consumer, and proximity-consumer importable during tests.
- DB schema is created at startup via `init_db()` in each service's `db.py`; no migration tool.
- TimescaleDB image must be `timescale/timescaledb-ha` (bundles PostGIS for proximity queries). Switching images requires `docker compose down -v`.
- Proximity detection uses PostGIS `ST_DWithin` (geography type) for accurate spherical distance.
- Fault injection is off by default (`FAULT_RATE=0.0`). Enable in `.env` or environment.
- Tests are marked `unit` or `integration`. Integration tests require Docker (Testcontainers spins up Kafka/Postgres).
- Run unit tests: `pytest -m unit`. Run all: `pytest`. Reports go to `reports/test-report.html`.

## Behavioral guidelines to reduce common LLM coding mistakes.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
