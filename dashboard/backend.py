# dashboard/backend.py
import sys
import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, str(Path(__file__).parent.parent))
from dashboard.dds_listener import DDSListener

DEFAULT_DSN = (
    "host=localhost dbname=solar user=postgres password=telemetry"
)
DDS_DOMAIN  = 1

# ── Global state ───────────────────────────────────────────────────────────
_live_queue:   asyncio.Queue | None = None
_dds_listener: DDSListener  | None = None
_db_conn:      Any                  = None   # psycopg2 connection


# ── WebSocket connection manager ───────────────────────────────────────────

class ConnectionManager:
    """Tracks all open WebSocket connections and broadcasts to all of them."""

    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.add(ws)
        print(f"[ws] client connected  total={len(self._clients)}")

    def disconnect(self, ws: WebSocket) -> None:
        self._clients.discard(ws)
        print(f"[ws] client disconnected  total={len(self._clients)}")

    async def broadcast(self, message: str) -> None:
        dead: set[WebSocket] = set()
        for client in self._clients:
            try:
                await client.send_text(message)
            except Exception:
                dead.add(client)
        self._clients -= dead


manager = ConnectionManager()


# ── Background broadcast task ──────────────────────────────────────────────

async def _broadcast_loop() -> None:
    """
    Continuously drain the asyncio Queue and broadcast every
    JSON message to all connected WebSocket clients.
    """
    assert _live_queue is not None
    while True:
        try:
            message = await asyncio.wait_for(
                _live_queue.get(), timeout=1.0
            )
            await manager.broadcast(message)
        except asyncio.TimeoutError:
            continue
        except Exception as exc:
            print(f"[ws] WARN: broadcast error — {exc}")


# ── App lifespan ───────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _live_queue, _dds_listener, _db_conn

    # 1. TimescaleDB connection
    _db_conn = psycopg2.connect(DEFAULT_DSN)
    print("[backend] TimescaleDB connected")

    # 2. asyncio Queue + DDS listener
    _live_queue   = asyncio.Queue(maxsize=2000)
    loop          = asyncio.get_event_loop()
    _dds_listener = DDSListener(
        domain_id=DDS_DOMAIN,
        queue=_live_queue,
        loop=loop,
    )
    _dds_listener.start()
    print(f"[backend] DDS listener started — domain={DDS_DOMAIN}")

    # 3. Background broadcast task
    broadcast_task = asyncio.create_task(_broadcast_loop())

    yield   # ← server runs here

    # Shutdown
    broadcast_task.cancel()
    _dds_listener.stop()
    _db_conn.close()
    print("[backend] Shutdown complete")


# ── FastAPI app ────────────────────────────────────────────────────────────

app = FastAPI(title="Solar Telemetry Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],    # restrict in production
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── WebSocket endpoint ─────────────────────────────────────────────────────

@app.websocket("/ws/live")
async def websocket_live(ws: WebSocket):
    await manager.connect(ws)
    try:
        # Keep the connection open; we only push, never pull here.
        # The client can send a ping frame which we echo back.
        while True:
            try:
                text = await asyncio.wait_for(ws.receive_text(), timeout=30)
                if text == "ping":
                    await ws.send_text('{"type":"pong"}')
            except asyncio.TimeoutError:
                # Send a keepalive so the browser doesn't timeout
                await ws.send_text('{"type":"keepalive"}')
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(ws)


# ── REST helpers ───────────────────────────────────────────────────────────

def _query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT and return a list of dicts."""
    assert _db_conn is not None
    with _db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


# ── REST endpoints ─────────────────────────────────────────────────────────

@app.get("/api/kpis")
def get_kpis():
    """Fleet-wide KPIs for the last 5 minutes."""
    rows = _query("""
        SELECT
            COUNT(DISTINCT panel_id)                        AS total_panels,
            COUNT(DISTINCT panel_id)
                FILTER (WHERE status = 'OK')                AS panels_ok,
            COUNT(DISTINCT panel_id)
                FILTER (WHERE status = 'FAULT')             AS panels_fault,
            COUNT(DISTINCT panel_id)
                FILTER (WHERE status = 'WARNING')           AS panels_warning,
            ROUND(SUM(power_w)::numeric,        1)          AS total_power_w,
            ROUND(AVG(irradiance_wm2)::numeric, 1)          AS avg_irradiance,
            ROUND(AVG(cell_temp_c)::numeric,    2)          AS avg_cell_temp_c
        FROM panel_telemetry
        WHERE time > NOW() - INTERVAL '5 minutes'
    """)
    return rows[0] if rows else {}


@app.get("/api/sites")
def get_sites():
    """Per-site aggregate for the last 5 minutes."""
    return _query("""
        SELECT
            site_id,
            COUNT(DISTINCT panel_id)                AS panels,
            ROUND(SUM(power_w)::numeric,  1)        AS total_power_w,
            ROUND(AVG(irradiance_wm2)::numeric, 1)  AS avg_irradiance,
            MAX(time)                               AS last_seen
        FROM panel_telemetry
        WHERE time > NOW() - INTERVAL '5 minutes'
        GROUP BY site_id
        ORDER BY site_id
    """)


@app.get("/api/panels")
def get_panels(site_id: str | None = None, limit: int = 200):
    """Latest reading per panel, optionally filtered by site."""
    if site_id:
        return _query("""
            SELECT DISTINCT ON (panel_id)
                panel_id, string_id, site_id,
                time, power_w, voltage_v, current_a,
                cell_temp_c, status, fault
            FROM panel_telemetry
            WHERE site_id = %s
            ORDER BY panel_id, time DESC
            LIMIT %s
        """, (site_id, limit))
    return _query("""
        SELECT DISTINCT ON (panel_id)
            panel_id, string_id, site_id,
            time, power_w, voltage_v, current_a,
            cell_temp_c, status, fault
        FROM panel_telemetry
        ORDER BY panel_id, time DESC
        LIMIT %s
    """, (limit,))


@app.get("/api/panels/{panel_id}/history")
def get_panel_history(panel_id: str, minutes: int = 60):
    """Time-series history for one panel."""
    return _query("""
        SELECT
            time, power_w, voltage_v, current_a,
            irradiance_wm2, cell_temp_c, status, fault
        FROM panel_telemetry
        WHERE panel_id = %s
          AND time > NOW() - INTERVAL '%s minutes'
        ORDER BY time ASC
    """, (panel_id, minutes))


@app.get("/api/faults")
def get_faults(limit: int = 100):
    """Most recent fault events."""
    return _query("""
        SELECT
            time, panel_id, site_id,
            fault_type, severity, power_w, cell_temp_c, message
        FROM panel_faults
        ORDER BY time DESC
        LIMIT %s
    """, (limit,))


@app.get("/api/heartbeats")
def get_heartbeats():
    """Latest heartbeat per edge node — shows which nodes are online."""
    return _query("""
        SELECT DISTINCT ON (edge_node_id)
            edge_node_id,
            MAX(time) AS last_seen,
            COUNT(*)  AS total_readings
        FROM panel_telemetry
        WHERE time > NOW() - INTERVAL '30 seconds'
        GROUP BY edge_node_id
        ORDER BY edge_node_id
    """)


@app.get("/health")
def health():
    return {"status": "ok"}