# edge_collector/edge_collector.py
import sys
import socket
import threading
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path

import rti.connextdds as dds

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.dds_types import PanelTelemetry, FaultAlert, HeartbeatSample
from edge_collector.qos import (
    raw_reader_qos,
    forwarded_writer_qos,
    fault_writer_qos,
    heartbeat_writer_qos,
)
from edge_collector.buffer import (
    init_db,
    insert_telemetry,
    mark_forwarded,
    get_unforwarded,
    row_to_sample,
)

TOPIC_RAW       = "solar/panel/telemetry/raw"
TOPIC_FORWARDED = "solar/edge/telemetry/forwarded"
TOPIC_FAULTS    = "solar/alerts/faults"
TOPIC_HEARTBEAT = "solar/health/heartbeat"


def parse_args():
    ap = argparse.ArgumentParser(description="Edge Collector")
    ap.add_argument("--d0", type=int, default=0, help="LAN domain ID")
    ap.add_argument("--d1", type=int, default=1, help="WAN domain ID")
    return ap.parse_args()


class EdgeCollector:
    def __init__(self, d0_id: int, d1_id: int) -> None:
        self._running = False
        self._lock = threading.Lock()   # guards SQLite + counters
        self._msgs_received = 0
        self._msgs_forwarded = 0

        # Per-instance handle caches for the Domain 1 forwarded writer
        self._fwd_handles: dict[tuple, dds.InstanceHandle] = {}
        self._fwd_samples: dict[tuple, PanelTelemetry] = {}

        self._setup_dds(d0_id, d1_id)
        self._conn = init_db()

        self.hb_timer = 30

    # ── DDS setup ─────────────────────────────────────────────────────────

    def _setup_dds(self, d0_id: int, d1_id: int) -> None:
        # Domain 0 — subscriber
        self._d0 = dds.DomainParticipant(domain_id=d0_id)
        d0_topic = dds.Topic(self._d0, TOPIC_RAW, PanelTelemetry)
        self._raw_reader = dds.DataReader(
            self._d0.implicit_subscriber,
            d0_topic,
            raw_reader_qos(),
        )

        # Domain 1 — publishers
        self._d1 = dds.DomainParticipant(domain_id=d1_id)

        fwd_topic = dds.Topic(self._d1, TOPIC_FORWARDED, PanelTelemetry)
        self._fwd_writer = dds.DataWriter(
            self._d1.implicit_publisher,
            fwd_topic,
            forwarded_writer_qos(),
        )

        fault_topic = dds.Topic(self._d1, TOPIC_FAULTS, FaultAlert)
        self._fault_writer = dds.DataWriter(
            self._d1.implicit_publisher,
            fault_topic,
            fault_writer_qos(),
        )

        hb_topic = dds.Topic(self._d1, TOPIC_HEARTBEAT, HeartbeatSample)
        self._hb_writer = dds.DataWriter(
            self._d1.implicit_publisher,
            hb_topic,
            heartbeat_writer_qos(),
        )

    # ── Forwarding helpers ────────────────────────────────────────────────

    def _forward(self, sample: PanelTelemetry) -> bool:
        """Write one sample to the Domain 1 forwarded topic.
        Returns True on success, False if the write fails (e.g. WAN down)."""
        try:
            key = (sample.panel_id, sample.string_id, sample.site_id)
            if key not in self._fwd_handles:
                handle = self._fwd_writer.register_instance(sample)
                self._fwd_handles[key] = handle
                self._fwd_samples[key] = sample
            else:
                handle = self._fwd_handles[key]
                self._fwd_samples[key] = sample

            self._fwd_writer.write(sample, handle)
            return True
        except Exception as exc:
            print(f"[collector] WARN: forward failed — {exc}")
            return False

    def _publish_fault(self, sample: PanelTelemetry) -> None:
        if sample.fault == "NONE":
            return

        alert = FaultAlert(
            panel_id=sample.panel_id,
            site_id=sample.site_id,
            timestamp_utc=sample.timestamp_utc,
            fault_type=sample.fault,
            severity=sample.status,
            power_w=sample.power_w,
            cell_temp_c=sample.cell_temp_c,
            message=(
                f"Panel {sample.panel_id} fault: {sample.fault} "
                f"at {sample.timestamp_utc}"
            ),
        )
        try:
            self._fault_writer.write(alert)
            print(f"[collector] FAULT published — {sample.panel_id} {sample.fault}")
        except Exception as exc:
            print(f"[collector] WARN: fault publish failed — {exc}")

    def _handle_lifecycle_event(self, info: dds.SampleInfo) -> None:
        state = info.state.instance_state
        handle = info.instance_handle

        messages = {
            dds.InstanceState.NOT_ALIVE_DISPOSED:
                "instance disposed",
            dds.InstanceState.NOT_ALIVE_NO_WRITERS:
                "instance has no writers",
            dds.InstanceState.ALIVE:
                "instance alive",
        }

        message = messages.get(state, f"unknown state {state}")

        print(f"[collector] lifecycle: {message} (handle={handle})")

    # ── Background threads ────────────────────────────────────────────────
    # TODO: Understand this function 
    def _replay_loop(self) -> None:
        """Every 10 s, attempt to forward buffered rows not yet sent."""
        while self._running:
            time.sleep(10)
            try:
                with self._lock:
                    rows = get_unforwarded(self._conn, limit=500)

                forwarded_count = 0
                for row in rows:
                    row_id, sample = row_to_sample(row, PanelTelemetry)
                    if self._forward(sample):
                        with self._lock:
                            mark_forwarded(self._conn, row_id)
                            self._msgs_forwarded += 1
                        forwarded_count += 1
                    else:
                        break

                if forwarded_count:
                    print(f"[collector] replay: forwarded {forwarded_count} buffered rows")

            except Exception as exc:
                print(f"[collector] WARN: replay loop error — {exc}")

    def _heartbeat_loop(self) -> None:
        """Publish a HeartbeatSample every self.hb_timer seconds."""
        node_id = socket.gethostname()

        while self._running:
            try:
                with self._lock:
                    msgs_forwarded = self._msgs_forwarded
                    panels_active = len(self._fwd_handles)

                hb = HeartbeatSample(
                    node_id=node_id,
                    node_type="edge_collector",
                    timestamp_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    panels_active=panels_active,
                    msgs_sent_last_min=msgs_forwarded,
                    buffer_utilization_pct=0.0,
                )
                self._hb_writer.write(hb)

            except Exception as exc:
                print(f"[collector] WARN: heartbeat failed — {exc}")

            time.sleep(self.hb_timer)

    # ── Main read loop ────────────────────────────────────────────────────

    def run(self) -> None:
        self._running = True

        replay_t = threading.Thread(target=self._replay_loop, daemon=True)
        hb_t = threading.Thread(target=self._heartbeat_loop, daemon=True)
        replay_t.start()
        hb_t.start()

        status_condition = dds.StatusCondition(self._raw_reader)
        status_condition.enabled_statuses = dds.StatusMask.DATA_AVAILABLE

        wait_set = dds.WaitSet()
        wait_set.attach_condition(status_condition)

        print(
            f"[collector] Running — "
            f"D0={self._d0.domain_id}  D1={self._d1.domain_id}"
        )

        try:
            while True:
                try:
                    active_conditions = wait_set.wait(dds.Duration(seconds=1))
                    if not active_conditions:
                        continue
                except dds.TimeoutError:
                    continue

                for data, info in self._raw_reader.take():
                    if not info.valid:
                        self._handle_lifecycle_event(info)
                        continue

                    with self._lock:
                        self._msgs_received += 1

                    # 1. Persist to SQLite first — survives a process crash
                    with self._lock:
                        row_id = insert_telemetry(self._conn, data)

                    # 2. Fault detection — publish alert to Domain 1
                    self._publish_fault(data)

                    # 3. Forward to Domain 1
                    if self._forward(data):
                        with self._lock:
                            self._msgs_forwarded += 1
                            mark_forwarded(self._conn, row_id)

                    # 4. Progress log every 100 messages
                    with self._lock:
                        msgs_received = self._msgs_received
                        msgs_forwarded = self._msgs_forwarded

                    if msgs_received % 100 == 0:
                        print(
                            f"[collector] "
                            f"recv={msgs_received}  "
                            f"fwd={msgs_forwarded}  "
                            f"panel={data.panel_id}  "
                            f"power={data.power_w:.1f}W  "
                            f"fault={data.fault}"
                        )

        except KeyboardInterrupt:
            print("\n[collector] Interrupted — shutting down cleanly...")

        finally:
            self._running = False

            # Give background threads a chance to exit cleanly
            replay_t.join(timeout=2)
            hb_t.join(timeout=2)

            # Unregister all forwarded instances so central side sees a clean shutdown
            for key, handle in list(self._fwd_handles.items()):
                try:
                    self._fwd_writer.dispose_instance(handle)
                except Exception as exc:
                    print(f"[collector] WARN: dispose failed for {key}: {exc}")

                try:
                    self._fwd_writer.unregister_instance(handle)
                except Exception as exc:
                    print(f"[collector] WARN: unregister failed for {key}: {exc}")

            self._d0.close()
            self._d1.close()
            self._conn.close()

            print(
                f"[collector] Done — "
                f"recv={self._msgs_received}  fwd={self._msgs_forwarded}"
            )


def main():
    args = parse_args()
    collector = EdgeCollector(d0_id=args.d0, d1_id=args.d1)
    collector.run()


if __name__ == "__main__":
    main()