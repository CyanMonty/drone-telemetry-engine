# grafana — agent notes

## Purpose

Grafana is configured entirely via provisioning files — no manual UI setup required. On first start, Grafana automatically connects to TimescaleDB and loads all dashboards.

## Structure

```
grafana/
  provisioning/
    datasources/
      timescaledb.yaml     # PostgreSQL datasource pointing at TimescaleDB
    dashboards/
      dashboard.yaml       # Dashboard provisioning config (folder, path)
      drone-telemetry.json # Fleet telemetry panels (altitude, speed, battery, heading)
      drone-map.json       # 2-D lat/lon fleet map
      streaming-test-metrics.json  # Pipeline throughput and latency metrics
```

## Datasource

The datasource is PostgreSQL (`timescaledb.yaml`) pointing at `timescaledb:5432`. Credentials are injected at runtime via environment variables (`POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`) from `docker-compose.yml`.

## Dashboards

| Dashboard | Source table | Key metrics |
|---|---|---|
| Drone Telemetry | `drone_telemetry` | altitude, speed, battery, heading per drone |
| Drone Map | `drone_telemetry` | lat/lon scatter — current fleet positions |
| Streaming Test Metrics | `streaming_metrics` | throughput (msg/s), latency p50/p95, error rate |

## Access

URL: [http://localhost:3000](http://localhost:3000)  
Default credentials: `admin` / `admin` (override with `GRAFANA_USER` / `GRAFANA_PASSWORD`)

## Agent guidance

- Dashboard JSON files are exported directly from Grafana. When editing dashboards, export the updated JSON from the Grafana UI and replace the file — do not hand-edit panel JSON.
- The `dashboard.yaml` provisioning file sets `updateIntervalSeconds: 10` — Grafana reloads dashboards from disk automatically during development.
- Do not change the datasource UID in `timescaledb.yaml` without also updating the `datasource` field in all dashboard JSON files — the UID is how dashboards reference the datasource.
- Grafana version is pinned at `11.0.0` in `docker-compose.yml`. Check plugin/panel compatibility before upgrading.
