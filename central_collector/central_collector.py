# central_collector/central_collector.py
import sys
import threading
import argparse
from pathlib import Path

import rti.connextdds as dds

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.dds_types import PanelTelemetry, FaultAlert, HeartbeatSample
from central_collector.qos import (
    forwarded_reader_qos,
    fault_reader_qos,
    heartbeat_reader_qos,
)
from central_collector.db import init_db, BatchWriter

TOPIC_FORWARDED = "solar/edge/telemetry/forwarded"
TOPIC_FAULTS    = "solar/alerts/faults"
TOPIC_HEARTBEAT = "solar/health/heartbeat"
DEFAULT_DSN     = "host=localhost dbname=solar user=postgres password=telemetry"


def parse_args():
    ap = argparse.ArgumentParser(description="Central Collector")
    ap.add_argument("--domain", type=int, default=1)
    ap.add_argument("--dsn",    type=str, default=DEFAULT_DSN)
    return ap.parse_args()


class CentralCollector:

    def __init__(self, domain_id: int, dsn: str) -> None:
        self._msgs_telemetry = 0
        self._msgs_faults    = 0
        self._msgs_heartbeat = 0
        # Track last sequence number per edge node to detect gaps
        self._seq_tracker: dict[str, int] = {}

        self._setup_dds(domain_id)
        conn = init_db(dsn)
        self._db = BatchWriter(conn)

    # ── DDS setup ─────────────────────────────────────────────────────────

    def _setup_dds(self, domain_id: int) -> None:
        self._participant = dds.DomainParticipant(domain_id=domain_id)

        fwd_topic = dds.Topic(
            self._participant, TOPIC_FORWARDED, PanelTelemetry
        )
        self._fwd_reader = dds.DataReader(
            self._participant.implicit_subscriber,
            fwd_topic,
            forwarded_reader_qos(),
        )

        fault_topic = dds.Topic(
            self._participant, TOPIC_FAULTS, FaultAlert
        )
        self._fault_reader = dds.DataReader(
            self._participant.implicit_subscriber,
            fault_topic,
            fault_reader_qos(),
        )

        hb_topic = dds.Topic(
            self._participant, TOPIC_HEARTBEAT, HeartbeatSample
        )
        self._hb_reader = dds.DataReader(
            self._participant.implicit_subscriber,
            hb_topic,
            heartbeat_reader_qos(),
        )

    # ── Gap detection ──────────────────────────────────────────────────────

    def _check_gap(self, sample: PanelTelemetry) -> None:
        key  = sample.edge_node_id or "unknown"
        prev = self._seq_tracker.get(key)
        seq  = sample.sequence_num
        if prev is not None and seq > prev + 1:
            print(
                f"[central] GAP — edge={key}"
                f"  expected={prev + 1}  got={seq}"
                f"  missing={seq - prev - 1} samples"
            )
        self._seq_tracker[key] = seq

    # ── Per-topic handlers ─────────────────────────────────────────────────

    def _handle_telemetry(self) -> None:
        for data, info in self._fwd_reader.take():
            if not info.valid:
                continue
            self._check_gap(data)
            self._db.add_telemetry(data)
            self._msgs_telemetry += 1

            if self._msgs_telemetry % 100 == 0:
                print(
                    f"[central] tel={self._msgs_telemetry}"
                    f"  faults={self._msgs_faults}"
                    f"  db_rows={self._db.rows_written}"
                    f"  panel={data.panel_id}"
                    f"  power={data.power_w:.1f}W"
                    f"  fault={data.fault}"
                )

    def _handle_faults(self) -> None:
        for data, info in self._fault_reader.take():
            if not info.valid:
                continue
            self._db.add_fault(data)
            self._msgs_faults += 1
            print(
                f"[central] FAULT stored — panel={data.panel_id}"
                f"  type={data.fault_type}"
                f"  severity={data.severity}"
                f"  time={data.timestamp_utc}"
            )

    def _handle_heartbeat(self) -> None:
        for data, info in self._hb_reader.take():
            if not info.valid:
                continue
            self._msgs_heartbeat += 1
            print(
                f"[central] HEARTBEAT — node={data.node_id}"
                f"  type={data.node_type}"
                f"  panels={data.panels_active}"
                f"  fwd={data.msgs_sent_last_min}"
            )

    # ── Main loop ──────────────────────────────────────────────────────────

    def run(self) -> None:
        # One StatusCondition per reader, all attached to one WaitSet
        fwd_cond   = dds.StatusCondition(self._fwd_reader)
        fault_cond = dds.StatusCondition(self._fault_reader)
        hb_cond    = dds.StatusCondition(self._hb_reader)

        for cond in (fwd_cond, fault_cond, hb_cond):
            cond.enabled_statuses = dds.StatusMask.DATA_AVAILABLE

        wait_set = dds.WaitSet()
        wait_set.attach_condition(fwd_cond)
        wait_set.attach_condition(fault_cond)
        wait_set.attach_condition(hb_cond)

        # Start the background DB flush thread
        flush_thread = threading.Thread(
            target=self._db.flush_loop, daemon=True
        )
        flush_thread.start()

        print(
            f"[central] Running — domain={self._participant.domain_id}"
            f"  TimescaleDB connected"
        )

        try:
            while True:
                try:
                    active = wait_set.wait(dds.Duration(seconds=1))
                except dds.TimeoutError:
                    continue

                if fwd_cond in active:
                    self._handle_telemetry()

                if fault_cond in active:
                    self._handle_faults()

                if hb_cond in active:
                    self._handle_heartbeat()

        except KeyboardInterrupt:
            print("\n[central] Interrupted — shutting down cleanly...")

        finally:
            self._db.stop()
            flush_thread.join(timeout=5)
            self._participant.close()
            print(
                f"[central] Done —"
                f"  tel={self._msgs_telemetry}"
                f"  faults={self._msgs_faults}"
                f"  db_rows={self._db.rows_written}"
            )


def main():
    args = parse_args()
    collector = CentralCollector(domain_id=args.domain, dsn=args.dsn)
    collector.run()


if __name__ == "__main__":
    main()