"""
Drone Swarm Real-Time Telemetry Dashboard
==========================================

Streamlit application that consumes parsed telemetry from Kafka and renders:
  • Live 2-D map of all drone positions
  • Summary KPI bar (total drones, armed count, avg battery, avg altitude)
  • Per-drone metric cards
  • Altitude & ground-speed time-series charts for any selected drone

Run with:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import collections
import json
import logging
import time
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import config as cfg

logger = logging.getLogger("dashboard")

# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Drone Swarm Telemetry",
    page_icon="🚁",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS tweaks
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
        .metric-card {
            background: #1e2130;
            border-radius: 8px;
            padding: 12px 16px;
            margin-bottom: 8px;
        }
        .alert-ok     { color: #2ecc71; font-weight: bold; }
        .alert-warn   { color: #f39c12; font-weight: bold; }
        .alert-crit   { color: #e74c3c; font-weight: bold; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Session-state initialisation
# ---------------------------------------------------------------------------
if "consumer" not in st.session_state:
    st.session_state.consumer = None
if "drones" not in st.session_state:
    st.session_state.drones: dict[str, dict] = {}
if "history" not in st.session_state:
    # drone_id → deque of {"timestamp", "alt_m", "ground_speed_kmh", "battery_pct"}
    st.session_state.history: dict[str, collections.deque] = collections.defaultdict(
        lambda: collections.deque(maxlen=cfg.MAX_HISTORY_POINTS)
    )
if "kafka_ok" not in st.session_state:
    st.session_state.kafka_ok = False


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def _init_consumer() -> Optional[KafkaConsumer]:
    try:
        consumer = KafkaConsumer(
            cfg.PARSED_TOPIC,
            bootstrap_servers=cfg.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            # Unique group so every dashboard instance reads all messages
            group_id=f"dashboard-{int(time.time() * 1000)}",
            auto_offset_reset="latest",
            enable_auto_commit=False,
            consumer_timeout_ms=200,
        )
        return consumer
    except NoBrokersAvailable:
        return None


def _poll(consumer: KafkaConsumer) -> dict[str, dict]:
    """Return the latest message keyed by drone_id."""
    latest: dict[str, dict] = {}
    try:
        records = consumer.poll(timeout_ms=400)
        for _tp, messages in records.items():
            for msg in messages:
                data = msg.value
                did = data.get("drone_id")
                if did:
                    latest[did] = data
    except (OSError, RuntimeError) as exc:  # connection/poll errors
        logger.warning("Kafka poll error: %s", exc)
    return latest


# ---------------------------------------------------------------------------
# Demo-mode data generator (used when Kafka is unavailable)
# ---------------------------------------------------------------------------

def _demo_data() -> dict[str, dict]:
    """Generate synthetic telemetry for offline / demo usage."""
    import math
    import random

    t = time.time()
    drones = {}
    for i in range(5):
        angle = t * 0.08 * (1 if i % 2 == 0 else -1) + i * (2 * math.pi / 5)
        r_deg = 0.002 + i * 0.0008
        bat = max(5, 100 - i * 8 - (t % 3600) / 120)
        drone_id = f"drone_{i + 1:03d}"
        drones[drone_id] = {
            "drone_id": drone_id,
            "timestamp": t,
            "sequence": int(t * 2),
            "heartbeat": {
                "armed": True,
                "flight_mode": "AUTO.MISSION",
                "system_status": "ACTIVE",
            },
            "position": {
                "lat": round(cfg.HOME_LAT + r_deg * math.cos(angle), 7),
                "lon": round(cfg.HOME_LON + r_deg * math.sin(angle), 7),
                "alt_msl": round(cfg.HOME_ALT + 80 + i * 25 + 10 * math.sin(t * 0.05), 2),
                "relative_alt": round(80 + i * 25 + 10 * math.sin(t * 0.05), 2),
                "vx": round(-8 * math.sin(angle), 3),
                "vy": round(8 * math.cos(angle), 3),
                "vz": round(random.gauss(0, 0.1), 3),
                "heading_deg": round(math.degrees(math.atan2(8 * math.cos(angle), -8 * math.sin(angle))) % 360, 1),
            },
            "attitude": {
                "roll_deg": round(5 * math.sin(angle), 3),
                "pitch_deg": round(random.gauss(0, 0.5), 3),
                "yaw_deg": round(math.degrees(math.atan2(8 * math.cos(angle), -8 * math.sin(angle))) % 360, 3),
                "rollspeed": 0.0,
                "pitchspeed": 0.0,
                "yawspeed": 0.05,
            },
            "battery": {
                "voltage_v": round(13.2 + 3.6 * bat / 100, 2),
                "current_a": round(random.uniform(1.5, 3.0), 2),
                "remaining_pct": int(bat),
                "consumed_mah": round((1 - bat / 100) * 10000, 1),
            },
            "gps": {
                "fix_type": 3,
                "satellites_visible": random.randint(10, 15),
                "eph": 0.8,
                "epv": 1.2,
            },
            "computed": {
                "ground_speed_ms": round(8.0 + random.gauss(0, 0.2), 2),
                "ground_speed_kmh": round((8.0 + random.gauss(0, 0.2)) * 3.6, 2),
                "total_speed_ms": round(8.0 + random.gauss(0, 0.2), 2),
                "battery_alert": "CRITICAL" if bat < 15 else ("WARNING" if bat < 25 else "OK"),
                "gps_status": "3D_FIX",
                "gps_healthy": True,
            },
        }
    return drones


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

_ALERT_COLOUR = {"OK": "#2ecc71", "WARNING": "#f39c12", "CRITICAL": "#e74c3c"}
_ALERT_MAP_COLOUR = {"OK": "green", "WARNING": "orange", "CRITICAL": "red"}


def _render_map(df: pd.DataFrame, container) -> None:
    if df.empty:
        container.info("No position data yet.")
        return

    fig = px.scatter_mapbox(
        df,
        lat="lat",
        lon="lon",
        hover_name="drone_id",
        hover_data={
            "alt_m": ":.1f",
            "ground_speed_kmh": ":.1f",
            "battery_pct": ":.0f",
            "flight_mode": True,
            "lat": False,
            "lon": False,
        },
        color="battery_alert",
        color_discrete_map=_ALERT_MAP_COLOUR,
        size="alt_m",
        size_max=22,
        zoom=13,
        mapbox_style="open-street-map",
        title="Swarm Position Map",
        labels={
            "battery_alert": "Battery",
            "alt_m": "Alt (m)",
            "ground_speed_kmh": "Speed (km/h)",
            "battery_pct": "Battery (%)",
        },
        height=480,
    )
    fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    container.plotly_chart(fig, use_container_width=True)


def _render_kpi_bar(drones: dict[str, dict], container) -> None:
    total = len(drones)
    armed = sum(1 for d in drones.values() if d.get("heartbeat", {}).get("armed"))
    bat_pcts = [d["battery"]["remaining_pct"] for d in drones.values() if "battery" in d]
    avg_bat = sum(bat_pcts) / len(bat_pcts) if bat_pcts else 0
    alts = [d["position"]["relative_alt"] for d in drones.values() if "position" in d]
    avg_alt = sum(alts) / len(alts) if alts else 0
    spd = [d["computed"]["ground_speed_kmh"] for d in drones.values() if "computed" in d]
    avg_spd = sum(spd) / len(spd) if spd else 0

    c1, c2, c3, c4, c5 = container.columns(5)
    c1.metric("🚁 Total Drones", total)
    c2.metric("⚡ Armed", armed)
    c3.metric("🔋 Avg Battery", f"{avg_bat:.0f}%")
    c4.metric("📐 Avg Altitude", f"{avg_alt:.0f} m")
    c5.metric("💨 Avg Speed", f"{avg_spd:.1f} km/h")


def _render_drone_cards(drones: dict[str, dict], container) -> None:
    cols = container.columns(min(len(drones), 3))
    for idx, (drone_id, data) in enumerate(sorted(drones.items())):
        col = cols[idx % len(cols)]
        pos = data.get("position", {})
        bat = data.get("battery", {})
        gps = data.get("gps", {})
        att = data.get("attitude", {})
        computed = data.get("computed", {})
        hb = data.get("heartbeat", {})

        alert = computed.get("battery_alert", "OK")
        col.markdown(
            f"""
            <div class="metric-card">
                <b>{drone_id}</b>
                &nbsp;
                <span class="alert-{'ok' if alert=='OK' else 'warn' if alert=='WARNING' else 'crit'}">
                    {'🔴 CRITICAL' if alert=='CRITICAL' else '🟡 WARNING' if alert=='WARNING' else '🟢 OK'}
                </span>
                <br>
                <small>Mode: <b>{hb.get('flight_mode','—')}</b>
                &nbsp;|&nbsp;Armed: <b>{'✅' if hb.get('armed') else '❌'}</b></small>
            </div>
            """,
            unsafe_allow_html=True,
        )
        col.metric("🔋 Battery", f"{bat.get('remaining_pct', 0)}%", f"{bat.get('voltage_v', 0):.1f} V")
        col.metric("📐 Alt AGL", f"{pos.get('relative_alt', 0):.0f} m")
        col.metric("💨 Speed", f"{computed.get('ground_speed_kmh', 0):.1f} km/h")
        col.metric("🧭 Heading", f"{pos.get('heading_deg', 0):.0f}°")
        col.metric("🛰 Satellites", gps.get("satellites_visible", 0))
        col.metric("↔ Roll", f"{att.get('roll_deg', 0):.1f}°")


def _render_history_charts(history: dict, selected_id: str, container) -> None:
    if selected_id not in history or len(history[selected_id]) < 2:
        container.info("Waiting for enough history to plot…")
        return

    records = list(history[selected_id])
    df = pd.DataFrame(records)
    df["time"] = pd.to_datetime(df["timestamp"], unit="s")

    col_a, col_b = container.columns(2)

    # Altitude chart
    fig_alt = go.Figure()
    fig_alt.add_trace(go.Scatter(
        x=df["time"], y=df["alt_m"],
        mode="lines", name="Alt AGL (m)",
        line={"color": "#3498db", "width": 2},
    ))
    fig_alt.update_layout(
        title=f"{selected_id} — Altitude AGL",
        xaxis_title="Time", yaxis_title="m",
        height=260, margin={"t": 40, "b": 30},
        template="plotly_dark",
    )
    col_a.plotly_chart(fig_alt, use_container_width=True)

    # Speed chart
    fig_spd = go.Figure()
    fig_spd.add_trace(go.Scatter(
        x=df["time"], y=df["ground_speed_kmh"],
        mode="lines", name="Speed (km/h)",
        line={"color": "#2ecc71", "width": 2},
    ))
    fig_spd.update_layout(
        title=f"{selected_id} — Ground Speed",
        xaxis_title="Time", yaxis_title="km/h",
        height=260, margin={"t": 40, "b": 30},
        template="plotly_dark",
    )
    col_b.plotly_chart(fig_spd, use_container_width=True)


# ---------------------------------------------------------------------------
# Main application
# ---------------------------------------------------------------------------

def main() -> None:
    # --- Sidebar -----------------------------------------------------------
    st.sidebar.title("⚙️ Settings")
    st.sidebar.markdown(f"**Kafka:** `{cfg.KAFKA_BOOTSTRAP_SERVERS}`")
    st.sidebar.markdown(f"**Topic:** `{cfg.PARSED_TOPIC}`")

    refresh_s = st.sidebar.slider(
        "Refresh interval (s)", min_value=1, max_value=10, value=2
    )

    # --- Title -------------------------------------------------------------
    st.title("🚁 Drone Swarm Real-Time Telemetry")

    kafka_status = st.sidebar.empty()
    status_placeholder = st.empty()

    # --- Kafka consumer init ----------------------------------------------
    if st.session_state.consumer is None and not st.session_state.kafka_ok:
        with st.spinner("Connecting to Kafka…"):
            consumer = _init_consumer()
        if consumer is not None:
            st.session_state.consumer = consumer
            st.session_state.kafka_ok = True
            kafka_status.success("Kafka connected ✅")
        else:
            kafka_status.warning("Kafka unavailable — showing demo data 🎭")

    # --- Poll / generate data ---------------------------------------------
    if st.session_state.kafka_ok and st.session_state.consumer is not None:
        new_data = _poll(st.session_state.consumer)
        if new_data:
            st.session_state.drones.update(new_data)
    else:
        # Demo mode: generate fresh synthetic data each refresh
        st.session_state.drones = _demo_data()

    drones = st.session_state.drones

    # --- Update history ---------------------------------------------------
    for drone_id, data in drones.items():
        pos = data.get("position", {})
        bat = data.get("battery", {})
        computed = data.get("computed", {})
        st.session_state.history[drone_id].append({
            "timestamp": data.get("timestamp", time.time()),
            "alt_m": pos.get("relative_alt", 0),
            "ground_speed_kmh": computed.get("ground_speed_kmh", 0),
            "battery_pct": bat.get("remaining_pct", 0),
        })

    if not drones:
        status_placeholder.warning(
            "No telemetry received yet. Make sure the simulator and parser are running."
        )
        time.sleep(refresh_s)
        st.rerun()
        return

    status_placeholder.empty()

    # --- KPI bar ----------------------------------------------------------
    kpi_bar = st.container()
    _render_kpi_bar(drones, kpi_bar)

    st.divider()

    # --- Map + drone cards ------------------------------------------------
    left_col, right_col = st.columns([3, 2])

    # Build DataFrame for map
    rows = []
    for drone_id, data in drones.items():
        pos = data.get("position", {})
        bat = data.get("battery", {})
        computed = data.get("computed", {})
        hb = data.get("heartbeat", {})
        rows.append({
            "drone_id": drone_id,
            "lat": pos.get("lat", 0.0),
            "lon": pos.get("lon", 0.0),
            "alt_m": pos.get("relative_alt", 0.0),
            "ground_speed_kmh": computed.get("ground_speed_kmh", 0.0),
            "battery_pct": bat.get("remaining_pct", 0),
            "battery_alert": computed.get("battery_alert", "OK"),
            "flight_mode": hb.get("flight_mode", "—"),
        })
    df_map = pd.DataFrame(rows)

    with left_col:
        map_placeholder = st.empty()
        _render_map(df_map, map_placeholder)

    with right_col:
        _render_drone_cards(drones, st.container())

    st.divider()

    # --- Time-series charts -----------------------------------------------
    st.subheader("📈 Time-Series Charts")
    drone_ids = sorted(drones.keys())
    if drone_ids:
        selected = st.selectbox("Select drone", drone_ids, key="selected_drone")
        _render_history_charts(st.session_state.history, selected, st.container())

    # --- Auto-refresh -----------------------------------------------------
    time.sleep(refresh_s)
    st.rerun()


if __name__ == "__main__":
    main()
