# docker — agent notes

## Purpose

Contains Docker build context for services that are not built from the top-level service directories. Currently holds the PX4 SITL swarm image.

## Structure

```
docker/
  px4-swarm/
    Dockerfile     # Builds a PX4 SITL image with Gazebo
    entrypoint.sh  # Launches NUM_DRONES PX4 instances with configurable spacing
```

## px4-swarm

Used only when the `dev` Docker Compose profile is active (`docker compose --profile dev up`).

The entrypoint script starts `NUM_DRONES` PX4 SITL instances. Each instance listens on a separate MAVSDK UDP port starting at `14540` (instance 0 = `14540`, instance 1 = `14541`, etc.).

| Variable | Default | Description |
|---|---|---|
| `NUM_DRONES` | `3` | Number of PX4 SITL instances |
| `VEHICLE` | `gz_x500` | Gazebo vehicle model |
| `WORLD` | `default` | Gazebo world |
| `DRONE_SPACING` | `2` | Spacing between spawned vehicles (metres) |
| `PX4_HOME_LAT` | `40.7128` | Home latitude |
| `PX4_HOME_LON` | `-74.0060` | Home longitude |
| `PX4_HOME_ALT` | `10` | Home altitude (metres) |

## Agent guidance

- This image is heavy (PX4 + Gazebo). Rebuilds are slow — only touch `Dockerfile` and `entrypoint.sh` when necessary.
- Ports `14540–14544` are exposed in `docker-compose.yml` to support up to 5 simultaneous PX4 instances.
- The simulator's `PX4_ADDRESSES` env variable must list one `udp://px4-swarm:<port>` entry per running instance.
- Changes here are only needed for dev/simulation mode — the production synthetic (`scale`) mode has no dependency on this directory.
