import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

BACKEND = "http://localhost:8000"

STATUS_COLORS = {"OK": "#2ecc71", "FAULT": "#e74c3c", "WARNING": "#f39c12"}

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


def _apply_status_color(val: str) -> str:
    return {
        "OK":      "background-color: #1a3a1a; color: #6fcf6f",
        "FAULT":   "background-color: #3a1a1a; color: #ff6b6b",
        "WARNING": "background-color: #3a2e00; color: #f0c040",
    }.get(val, "")


def style_status(df: pd.DataFrame):
    styler = df.style
    if "status" in df.columns:
        try:
            styler = styler.map(_apply_status_color, subset=["status"])
        except AttributeError:
            styler = styler.applymap(_apply_status_color, subset=["status"])
    return styler


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
c1.metric("Total Panels",      kpis.get("total_panels",    "—"))
c2.metric("OK",                kpis.get("panels_ok",       "—"))
c3.metric("Faults",            kpis.get("panels_fault",    "—"))
c4.metric("Warnings",          kpis.get("panels_warning",  "—"))
c5.metric("Total Power (W)",   kpis.get("total_power_w",   "—"))
c6.metric("Irradiance (W/m²)", kpis.get("avg_irradiance",  "—"))
c7.metric("Cell Temp (°C)",    kpis.get("avg_cell_temp_c", "—"))

st.divider()

# ── Top charts: Power per site | Status donut | Edge nodes ─────────────────

col_power, col_status, col_nodes = st.columns([2, 1, 1])

with col_power:
    st.subheader("Power per Site (W)")
    if sites_data:
        df_sites = pd.DataFrame(sites_data)
        fig = px.bar(
            df_sites,
            x="site_id",
            y="total_power_w",
            color="site_id",
            labels={"site_id": "Site", "total_power_w": "Power (W)"},
            text_auto=True,
            height=280,
        )
        fig.update_layout(showlegend=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No site data in the last 5 minutes.")

with col_status:
    st.subheader("Panel Status")
    ok      = int(kpis.get("panels_ok",      0) or 0)
    fault   = int(kpis.get("panels_fault",   0) or 0)
    warning = int(kpis.get("panels_warning", 0) or 0)
    if ok + fault + warning > 0:
        fig = go.Figure(go.Pie(
            labels=["OK", "Fault", "Warning"],
            values=[ok, fault, warning],
            marker_colors=[STATUS_COLORS["OK"], STATUS_COLORS["FAULT"], STATUS_COLORS["WARNING"]],
            hole=0.5,
            textinfo="label+value",
        ))
        fig.update_layout(showlegend=False, margin=dict(t=10, b=10), height=280)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data.")

with col_nodes:
    st.subheader("Edge Nodes")
    heartbeats = fetch("/api/heartbeats") or []
    if heartbeats:
        df_hb = pd.DataFrame(heartbeats)
        df_hb.columns = [c.replace("_", " ").title() for c in df_hb.columns]
        st.dataframe(df_hb, use_container_width=True, hide_index=True)
    else:
        st.warning("No edge nodes seen in last 30 s.")

st.divider()

# ── Sites table ─────────────────────────────────────────────────────────────

with st.expander("Sites — full table", expanded=False):
    if sites_data:
        df_sites_display = pd.DataFrame(sites_data)
        df_sites_display.columns = [c.replace("_", " ").title() for c in df_sites_display.columns]
        st.dataframe(df_sites_display, use_container_width=True, hide_index=True)

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
    st.dataframe(style_status(df_display), use_container_width=True, hide_index=True)
else:
    st.info("No panel data available.")

st.divider()

# ── Power per panel bar chart ──────────────────────────────────────────────

if panels:
    st.subheader("Power per Panel (W)")
    df_bar = pd.DataFrame(panels).sort_values("power_w", ascending=False)
    fig = px.bar(
        df_bar,
        x="panel_id",
        y="power_w",
        color="status",
        color_discrete_map=STATUS_COLORS,
        labels={"panel_id": "Panel", "power_w": "Power (W)", "status": "Status"},
        hover_data=["site_id", "voltage_v", "current_a", "cell_temp_c"],
        text_auto=True,
        height=300,
    )
    fig.update_layout(margin=dict(t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)
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
        df_hist = df_hist.sort_values("time")

        ch1, ch2 = st.columns(2)

        with ch1:
            fig = px.line(
                df_hist, x="time", y="power_w",
                title="Power (W)",
                labels={"time": "Time", "power_w": "Power (W)"},
                height=250,
            )
            fig.update_traces(line_color="#f1c40f")
            fig.update_layout(margin=dict(t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)

        with ch2:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_hist["time"], y=df_hist["voltage_v"],
                name="Voltage (V)", line=dict(color="#3498db"),
            ))
            fig.add_trace(go.Scatter(
                x=df_hist["time"], y=df_hist["current_a"],
                name="Current (A)", line=dict(color="#2ecc71"), yaxis="y2",
            ))
            fig.update_layout(
                title="Voltage & Current",
                yaxis=dict(title="Voltage (V)"),
                yaxis2=dict(title="Current (A)", overlaying="y", side="right"),
                legend=dict(orientation="h", y=-0.2),
                height=250,
                margin=dict(t=40, b=10),
            )
            st.plotly_chart(fig, use_container_width=True)

        ch3, ch4 = st.columns(2)

        with ch3:
            fig = px.line(
                df_hist, x="time", y="cell_temp_c",
                title="Cell Temperature (°C)",
                labels={"time": "Time", "cell_temp_c": "Temp (°C)"},
                height=250,
            )
            fig.update_traces(line_color="#e74c3c")
            fig.update_layout(margin=dict(t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)

        with ch4:
            if "irradiance_wm2" in df_hist.columns:
                fig = px.line(
                    df_hist, x="time", y="irradiance_wm2",
                    title="Irradiance (W/m²)",
                    labels={"time": "Time", "irradiance_wm2": "Irradiance (W/m²)"},
                    height=250,
                )
                fig.update_traces(line_color="#e67e22")
                fig.update_layout(margin=dict(t=40, b=10))
                st.plotly_chart(fig, use_container_width=True)
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

    col_tbl, col_chart = st.columns([2, 1])
    with col_tbl:
        df_faults_display = df_faults.copy()
        df_faults_display.columns = [c.replace("_", " ").title() for c in df_faults_display.columns]
        st.dataframe(df_faults_display, use_container_width=True, hide_index=True)

    with col_chart:
        if "fault_type" in df_faults.columns:
            fig = px.pie(
                df_faults,
                names="fault_type",
                title="Fault Types",
                hole=0.4,
                height=280,
            )
            fig.update_layout(margin=dict(t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)
else:
    st.success("No faults recorded.")

# ── Auto-refresh ───────────────────────────────────────────────────────────

# time.sleep(refresh_interval)
# st.experimental_rerun()