# central_collector/db.py
import threading
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values


def init_db(dsn: str) -> psycopg2.extensions.connection:
    """Create schema, hypertables, indexes. Return open connection."""
    conn = psycopg2.connect(dsn)

    with conn.cursor() as cur:

        # TimescaleDB extension (safe to run if already exists)
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        conn.commit()

        # ── panel_telemetry hypertable ─────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS panel_telemetry (
                time            TIMESTAMPTZ      NOT NULL,
                panel_id        TEXT             NOT NULL,
                string_id       TEXT             NOT NULL,
                site_id         TEXT             NOT NULL,
                edge_node_id    TEXT,
                sequence_num    BIGINT,
                power_w         DOUBLE PRECISION,
                voltage_v       DOUBLE PRECISION,
                current_a       DOUBLE PRECISION,
                irradiance_wm2  DOUBLE PRECISION,
                ambient_temp_c  DOUBLE PRECISION,
                cell_temp_c     DOUBLE PRECISION,
                orientation_deg DOUBLE PRECISION,
                tilt_deg        DOUBLE PRECISION,
                status          TEXT,
                fault           TEXT
            )
        """)
        conn.commit()

        cur.execute("""
            SELECT create_hypertable(
                'panel_telemetry', 'time',
                if_not_exists => TRUE
            )
        """)
        conn.commit()

        # Indexes for the two most common query patterns
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tel_panel_time
            ON panel_telemetry (panel_id, time DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_tel_site_time
            ON panel_telemetry (site_id, time DESC)
        """)
        conn.commit()

        # ── panel_faults hypertable ────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS panel_faults (
                time        TIMESTAMPTZ      NOT NULL,
                panel_id    TEXT             NOT NULL,
                site_id     TEXT             NOT NULL,
                fault_type  TEXT,
                severity    TEXT,
                power_w     DOUBLE PRECISION,
                cell_temp_c DOUBLE PRECISION,
                message     TEXT
            )
        """)
        conn.commit()

        cur.execute("""
            SELECT create_hypertable(
                'panel_faults', 'time',
                if_not_exists => TRUE
            )
        """)
        conn.commit()

    print("[db] Schema ready — panel_telemetry and panel_faults hypertables exist")
    return conn


def _parse_ts(ts_str: str) -> datetime:
    """Parse ISO-8601 UTC string from DDS sample into a datetime."""
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return datetime.now(timezone.utc)


def _sample_to_row(sample) -> tuple:
    return (
        _parse_ts(sample.timestamp_utc),
        sample.panel_id,
        sample.string_id,
        sample.site_id,
        sample.edge_node_id,
        sample.sequence_num,
        sample.power_w,
        sample.voltage_v,
        sample.current_a,
        sample.irradiance_wm2,
        sample.ambient_temp_c,
        sample.cell_temp_c,
        sample.orientation_deg,
        sample.tilt_deg,
        sample.status,
        sample.fault,
    )


def _fault_to_row(alert) -> tuple:
    return (
        _parse_ts(alert.timestamp_utc),
        alert.panel_id,
        alert.site_id,
        alert.fault_type,
        alert.severity,
        alert.power_w,
        alert.cell_temp_c,
        alert.message,
    )


class BatchWriter:
    """
    Accumulates DDS samples and flushes them to TimescaleDB in bulk.
    Flushes when either BATCH_SIZE rows accumulate OR FLUSH_INTERVAL
    seconds pass — whichever comes first.
    """
    BATCH_SIZE     = 500
    FLUSH_INTERVAL = 2.0

    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self._conn        = conn
        self._lock        = threading.Lock()
        self._tel_batch:   list[tuple] = []
        self._fault_batch: list[tuple] = []
        self._rows_written = 0
        self._running      = False

    def add_telemetry(self, sample) -> None:
        with self._lock:
            self._tel_batch.append(_sample_to_row(sample))
            should_flush = len(self._tel_batch) >= self.BATCH_SIZE
        if should_flush:
            self._flush()

    def add_fault(self, alert) -> None:
        # Faults are rare and important — flush immediately
        with self._lock:
            self._fault_batch.append(_fault_to_row(alert))
        self._flush()

    def _flush(self) -> None:
        with self._lock:
            tel_rows   = self._tel_batch[:]
            fault_rows = self._fault_batch[:]
            self._tel_batch.clear()
            self._fault_batch.clear()

        if not tel_rows and not fault_rows:
            return

        try:
            with self._conn.cursor() as cur:
                if tel_rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO panel_telemetry (
                            time, panel_id, string_id, site_id,
                            edge_node_id, sequence_num,
                            power_w, voltage_v, current_a, irradiance_wm2,
                            ambient_temp_c, cell_temp_c,
                            orientation_deg, tilt_deg,
                            status, fault
                        ) VALUES %s
                        """,
                        tel_rows,
                        page_size=500,
                    )
                if fault_rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO panel_faults (
                            time, panel_id, site_id,
                            fault_type, severity,
                            power_w, cell_temp_c, message
                        ) VALUES %s
                        """,
                        fault_rows,
                        page_size=500,
                    )
            self._conn.commit()
            self._rows_written += len(tel_rows) + len(fault_rows)

        except Exception as exc:
            print(f"[db] WARN: flush failed — {exc}")
            try:
                self._conn.rollback()
            except Exception:
                pass

    def flush_loop(self) -> None:
        """Background thread — flush on a timer regardless of batch size."""
        self._running = True
        while self._running:
            time.sleep(self.FLUSH_INTERVAL)
            self._flush()

    def stop(self) -> None:
        """Stop the flush loop and do one final flush."""
        self._running = False
        self._flush()

    @property
    def rows_written(self) -> int:
        return self._rows_written