# edge_collector/buffer.py
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "buffer.db"


def init_db(path: Path = DB_PATH) -> sqlite3.Connection:
    # check_same_thread=False: main thread writes, replay thread reads.
    # WAL mode allows one writer + concurrent readers without blocking.
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")  # faster than FULL, safe with WAL

    conn.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_buffer (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at     TEXT    NOT NULL
                                DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            timestamp_utc   TEXT    NOT NULL,
            panel_id        TEXT    NOT NULL,
            string_id       TEXT    NOT NULL,
            site_id         TEXT    NOT NULL,
            edge_node_id    TEXT,
            sequence_num    INTEGER,
            power_w         REAL,
            voltage_v       REAL,
            current_a       REAL,
            irradiance_wm2  REAL,
            ambient_temp_c  REAL,
            cell_temp_c     REAL,
            orientation_deg REAL,
            tilt_deg        REAL,
            status          TEXT,
            fault           TEXT,
            forwarded       INTEGER NOT NULL DEFAULT 0
        )
    """)

    # Index makes the replay query (WHERE forwarded=0 ORDER BY id) fast.
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_unforwarded
        ON telemetry_buffer (forwarded, id)
    """)

    conn.commit()
    return conn


def insert_telemetry(conn: sqlite3.Connection, sample) -> int:
    """Insert one PanelTelemetry sample. Returns the new row id."""
    cur = conn.execute(
        """
        INSERT INTO telemetry_buffer (
            timestamp_utc, panel_id, string_id, site_id,
            edge_node_id, sequence_num,
            power_w, voltage_v, current_a, irradiance_wm2,
            ambient_temp_c, cell_temp_c, orientation_deg, tilt_deg,
            status, fault
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            sample.timestamp_utc, sample.panel_id, sample.string_id, sample.site_id,
            sample.edge_node_id,  sample.sequence_num,
            sample.power_w,       sample.voltage_v,  sample.current_a,
            sample.irradiance_wm2, sample.ambient_temp_c, sample.cell_temp_c,
            sample.orientation_deg, sample.tilt_deg,
            sample.status, sample.fault,
        ),
    )
    conn.commit()
    return cur.lastrowid


def mark_forwarded(conn: sqlite3.Connection, row_id: int) -> None:
    conn.execute(
        "UPDATE telemetry_buffer SET forwarded=1 WHERE id=?", (row_id,)
    )
    conn.commit()


def get_unforwarded(conn: sqlite3.Connection, limit: int = 500) -> list:
    """Return up to `limit` rows not yet forwarded, oldest first."""
    cur = conn.execute(
        """
        SELECT id, timestamp_utc, panel_id, string_id, site_id,
               edge_node_id, sequence_num,
               power_w, voltage_v, current_a, irradiance_wm2,
               ambient_temp_c, cell_temp_c, orientation_deg, tilt_deg,
               status, fault
        FROM   telemetry_buffer
        WHERE  forwarded = 0
        ORDER  BY id ASC
        LIMIT  ?
        """,
        (limit,),
    )
    return cur.fetchall()


def row_to_sample(row: tuple, panel_telemetry_cls):
    """Reconstruct a PanelTelemetry from a buffer row."""
    (row_id, ts, panel_id, string_id, site_id,
     edge_node_id, seq_num,
     power_w, voltage_v, current_a, irradiance_wm2,
     ambient_temp_c, cell_temp_c, orientation_deg, tilt_deg,
     status, fault) = row

    return row_id, panel_telemetry_cls(
        panel_id        = panel_id,
        string_id       = string_id,
        site_id         = site_id,
        timestamp_utc   = ts,
        edge_node_id    = edge_node_id or "",
        sequence_num    = seq_num or 0,
        power_w         = power_w   or 0.0,
        voltage_v       = voltage_v or 0.0,
        current_a       = current_a or 0.0,
        irradiance_wm2  = irradiance_wm2  or 0.0,
        ambient_temp_c  = ambient_temp_c  or 0.0,
        cell_temp_c     = cell_temp_c     or 0.0,
        orientation_deg = orientation_deg or 0.0,
        tilt_deg        = tilt_deg        or 0.0,
        status          = status or "",
        fault           = fault  or "",
    )