import time
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

BACKEND = "http://localhost:8000"

st.set_page_config(
    page_title="Solar Panel Telemetry",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Helpers ────────────────────────────────────────────────────────────────

def fetch(endpoint: str, params: dict = None):
    try:
        r = requests.get(f"{BACKEND}{endpoint}", params=params, timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        st.error("Cannot reach backend at http://localhost:8000 — is it running?")
        return None
    except Exception as exc:
        st.error(f"API error [{endpoint}]: {exc}")
        return None


def status_color(val: str) -> str:
    return {
        "OK":      "background-color: #1a3a1a; color: #6fcf6f",
        "FAULT":   "background-color: #3a1a1a; color: #ff6b6b",
        "WARNING": "background-color: #3a2e00; color: #f0c040",
    }.get(val, "")


# ── Sidebar ────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("Solar Telemetry")

    health = fetch("/health")
    if health and health.get("status") == "ok":
        st.success("Backend online")
    else:
        st.error("Backend offline")

    st.divider()

    refresh_interval = st.slider("Auto-refresh (seconds)", 5, 60, 10)

    sites_data = fetch("/api/sites") or []
    site_ids = ["All"] + [s["site_id"] for s in sites_data]
    selected_site = st.selectbox("Filter by site", site_ids)

    history_minutes = st.slider("History window (minutes)", 10, 120, 60)

    st.divider()
    st.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')}")

# ── KPIs ───────────────────────────────────────────────────────────────────

st.header("Fleet Overview — last 5 minutes")

kpis = fetch("/api/kpis") or {}

c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
c1.metric("Total Panels",       kpis.get("total_panels",    "—"))
c2.metric("OK",                 kpis.get("panels_ok",       "—"))
c3.metric("Faults",             kpis.get("panels_fault",    "—"))
c4.metric("Warnings",           kpis.get("panels_warning",  "—"))
c5.metric("Total Power (W)",    kpis.get("total_power_w",   "—"))
c6.metric("Irradiance (W/m²)",  kpis.get("avg_irradiance",  "—"))
c7.metric("Cell Temp (°C)",     kpis.get("avg_cell_temp_c", "—"))

st.divider()

# ── Sites & Edge Nodes ─────────────────────────────────────────────────────

col_sites, col_nodes = st.columns([3, 2])

with col_sites:
    st.subheader("Sites")
    if sites_data:
        df_sites = pd.DataFrame(sites_data)
        df_sites.columns = [c.replace("_", " ").title() for c in df_sites.columns]
        st.dataframe(df_sites, use_container_width=True, hide_index=True)
    else:
        st.info("No site data in the last 5 minutes.")

with col_nodes:
    st.subheader("Edge Nodes")
    heartbeats = fetch("/api/heartbeats") or []
    if heartbeats:
        df_hb = pd.DataFrame(heartbeats)
        df_hb.columns = [c.replace("_", " ").title() for c in df_hb.columns]
        st.dataframe(df_hb, use_container_width=True, hide_index=True)
    else:
        st.warning("No edge nodes seen in last 30 seconds.")

st.divider()

# ── Panels table ───────────────────────────────────────────────────────────

st.subheader("Panels — Latest Reading")

panel_params: dict = {}
if selected_site != "All":
    panel_params["site_id"] = selected_site

panels = fetch("/api/panels", panel_params) or []

if panels:
    df_panels = pd.DataFrame(panels)

    display_cols = ["panel_id", "site_id", "string_id", "status",
                    "power_w", "voltage_v", "current_a", "cell_temp_c", "fault", "time"]
    display_cols = [c for c in display_cols if c in df_panels.columns]
    df_display = df_panels[display_cols].copy()

    styled = df_display.style.map(status_color, subset=["status"])
    st.dataframe(styled, use_container_width=True, hide_index=True)
else:
    st.info("No panel data available.")

st.divider()

# ── Panel History ──────────────────────────────────────────────────────────

st.subheader("Panel History")

panel_ids = [p["panel_id"] for p in panels] if panels else []

if panel_ids:
    selected_panel = st.selectbox("Select panel", panel_ids)

    history = fetch(f"/api/panels/{selected_panel}/history", {"minutes": history_minutes}) or []

    if history:
        df_hist = pd.DataFrame(history)
        df_hist["time"] = pd.to_datetime(df_hist["time"])
        df_hist = df_hist.sort_values("time").set_index("time")

        ch1, ch2 = st.columns(2)
        with ch1:
            st.markdown("**Power (W)**")
            st.line_chart(df_hist[["power_w"]], height=220)
        with ch2:
            st.markdown("**Voltage (V) & Current (A)**")
            st.line_chart(df_hist[["voltage_v", "current_a"]], height=220)

        ch3, ch4 = st.columns(2)
        with ch3:
            st.markdown("**Cell Temperature (°C)**")
            st.line_chart(df_hist[["cell_temp_c"]], height=220)
        with ch4:
            if "irradiance_wm2" in df_hist.columns:
                st.markdown("**Irradiance (W/m²)**")
                st.line_chart(df_hist[["irradiance_wm2"]], height=220)
    else:
        st.info(f"No history for panel {selected_panel} in the last {history_minutes} minutes.")
else:
    st.info("No panels available for the selected site.")

st.divider()

# ── Faults ─────────────────────────────────────────────────────────────────

st.subheader("Recent Faults")

faults = fetch("/api/faults", {"limit": 50}) or []

if faults:
    df_faults = pd.DataFrame(faults)
    df_faults.columns = [c.replace("_", " ").title() for c in df_faults.columns]
    st.dataframe(df_faults, use_container_width=True, hide_index=True)
else:
    st.success("No faults recorded.")

# ── Auto-refresh ───────────────────────────────────────────────────────────

time.sleep(refresh_interval)
st.rerun()
