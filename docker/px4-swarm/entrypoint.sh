#!/bin/bash
#
# Starts NUM_DRONES PX4 SITL instances inside a single shared Gazebo world.
#
# How it works:
#   Instance 0  — launches gz-sim with PX4_GZ_WORLD, then spawns drone 0.
#   Instance N  — waits for gz-sim, then connects (PX4_GZ_STANDALONE=1)
#                 and spawns drone N at a 2 m Y-offset per instance.
#
# MAVSDK ports (per PX4 rcS: udp_offboard_port = 14540 + px4_instance):
#   drone-0 → 14540 | drone-1 → 14541 | drone-2 → 14542 | …
#
# Environment variables:
#   NUM_DRONES   — number of drones to spawn (default: 3, max: 5)
#   VEHICLE      — PX4_SIM_MODEL value         (default: gz_x500)
#   WORLD        — PX4_GZ_WORLD value          (default: default)
#   DRONE_SPACING — Y-axis spacing in metres   (default: 2)

set -e

NUM_DRONES=${NUM_DRONES:-3}
VEHICLE=${VEHICLE:-gz_x500}
WORLD=${WORLD:-default}
DRONE_SPACING=${DRONE_SPACING:-2}

if [ "$NUM_DRONES" -gt 5 ]; then
    echo "[swarm] WARNING: NUM_DRONES capped at 5 (Gazebo memory limits)"
    NUM_DRONES=5
fi

echo "[swarm] Starting ${NUM_DRONES} x ${VEHICLE} in world '${WORLD}'"

# Virtual framebuffer — required by some Gazebo plugins even in headless mode.
Xvfb :99 -screen 0 1600x1200x24+32 &
export DISPLAY=:99

# ---------------------------------------------------------------------------
# Instance 0: no PX4_GZ_STANDALONE — this instance launches gz-sim.
# ---------------------------------------------------------------------------
echo "[swarm] Instance 0 — launching gz-sim + spawning drone at (0, 0)"
HEADLESS=1 \
  PX4_SIM_MODEL="${VEHICLE}" \
  PX4_GZ_WORLD="${WORLD}" \
  PX4_GZ_MODEL_POSE="0,0,0.1" \
  PX4_HOME_LAT="${PX4_HOME_LAT:-40.7128}" \
  PX4_HOME_LON="${PX4_HOME_LON:--74.0060}" \
  PX4_HOME_ALT="${PX4_HOME_ALT:-10}" \
  "${FIRMWARE_DIR}/build/bin/px4" -i 0 -d &

# ---------------------------------------------------------------------------
# Wait for gz-sim to be responsive before spawning other instances.
# PX4_GZ_STANDALONE instances retry automatically, but spacing the starts
# avoids race conditions in the GZ transport layer.
# ---------------------------------------------------------------------------
echo "[swarm] Waiting 15s for gz-sim world to settle ..."
sleep 15

# ---------------------------------------------------------------------------
# Instances 1..N-1: PX4_GZ_STANDALONE=1 — join the running gz-sim world.
# ---------------------------------------------------------------------------
for i in $(seq 1 $((NUM_DRONES - 1))); do
    Y=$((i * DRONE_SPACING))
    echo "[swarm] Instance ${i} — connecting to gz-sim, spawning at (0, ${Y})"
    HEADLESS=1 \
      PX4_GZ_STANDALONE=1 \
      PX4_SIM_MODEL="${VEHICLE}" \
      PX4_GZ_MODEL_POSE="0,${Y},0.1" \
      PX4_HOME_LAT="${PX4_HOME_LAT:-40.7128}" \
      PX4_HOME_LON="${PX4_HOME_LON:--74.0060}" \
      PX4_HOME_ALT="${PX4_HOME_ALT:-10}" \
      "${FIRMWARE_DIR}/build/bin/px4" -i "${i}" -d &
    # Stagger starts — each GZ model spawn needs a moment to register.
    sleep 3
done

PORTS=$(seq -s ', ' 14540 $((14540 + NUM_DRONES - 1)))
echo "[swarm] All ${NUM_DRONES} instances started."
echo "[swarm] MAVSDK ports: ${PORTS}"
echo "[swarm] Connect with: udp://px4-swarm:14540 … udp://px4-swarm:$((14540 + NUM_DRONES - 1))"

# Keep container alive — exit if any PX4 instance dies unexpectedly.
wait -n
echo "[swarm] A PX4 instance exited — shutting down."
exit 1
