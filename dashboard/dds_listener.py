# dashboard/dds_listener.py
import sys
import asyncio
import threading
import json
from pathlib import Path
from dataclasses import asdict

import rti.connextdds as dds

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.dds_types import PanelTelemetry, FaultAlert, HeartbeatSample
from dashboard.qos import (
    forwarded_reader_qos,
    fault_reader_qos,
    heartbeat_reader_qos,
)

TOPIC_FORWARDED = "solar/edge/telemetry/forwarded"
TOPIC_FAULTS    = "solar/alerts/faults"
TOPIC_HEARTBEAT = "solar/health/heartbeat"


def _sample_to_dict(sample, msg_type: str) -> dict:
    """
    Convert a DDS sample to a JSON-serialisable dict.
    We build it manually to avoid dataclass field order surprises
    and to inject the message type tag the frontend needs.
    """
    if msg_type == "telemetry":
        return {
            "type":           "telemetry",
            "panel_id":       sample.panel_id,
            "string_id":      sample.string_id,
            "site_id":        sample.site_id,
            "timestamp_utc":  sample.timestamp_utc,
            "power_w":        round(sample.power_w,        2),
            "voltage_v":      round(sample.voltage_v,      2),
            "current_a":      round(sample.current_a,      3),
            "irradiance_wm2": round(sample.irradiance_wm2, 1),
            "ambient_temp_c": round(sample.ambient_temp_c, 2),
            "cell_temp_c":    round(sample.cell_temp_c,    2),
            "status":         sample.status,
            "fault":          sample.fault,
            "edge_node_id":   sample.edge_node_id,
            "sequence_num":   sample.sequence_num,
        }
    if msg_type == "fault":
        return {
            "type":           "fault",
            "panel_id":       sample.panel_id,
            "site_id":        sample.site_id,
            "timestamp_utc":  sample.timestamp_utc,
            "fault_type":     sample.fault_type,
            "severity":       sample.severity,
            "power_w":        round(sample.power_w,    2),
            "cell_temp_c":    round(sample.cell_temp_c, 2),
            "message":        sample.message,
        }
    if msg_type == "heartbeat":
        return {
            "type":                    "heartbeat",
            "node_id":                 sample.node_id,
            "node_type":               sample.node_type,
            "timestamp_utc":           sample.timestamp_utc,
            "panels_active":           sample.panels_active,
            "msgs_sent_last_min":      sample.msgs_sent_last_min,
            "buffer_utilization_pct":  sample.buffer_utilization_pct,
        }
    return {}


class DDSListener:
    """
    Runs a blocking DDS WaitSet loop in a background thread.
    Converts each received sample to a JSON string and puts it
    into an asyncio.Queue so the FastAPI event loop can broadcast it.
    """

    def __init__(self, domain_id: int, queue: asyncio.Queue,
                 loop: asyncio.AbstractEventLoop) -> None:
        self._queue   = queue
        self._loop    = loop
        self._running = False
        self._thread: threading.Thread | None = None

        self._setup_dds(domain_id)

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

    def _enqueue(self, payload: dict) -> None:
        """
        Thread-safe bridge: schedule a put on the asyncio queue
        from the DDS (non-async) thread.
        call_soon_threadsafe is the correct way to hand off data
        from any thread into a running event loop.
        """
        self._loop.call_soon_threadsafe(
            self._queue.put_nowait,
            json.dumps(payload),
        )

    def _run(self) -> None:
        fwd_cond   = dds.StatusCondition(self._fwd_reader)
        fault_cond = dds.StatusCondition(self._fault_reader)
        hb_cond    = dds.StatusCondition(self._hb_reader)

        for cond in (fwd_cond, fault_cond, hb_cond):
            cond.enabled_statuses = dds.StatusMask.DATA_AVAILABLE

        wait_set = dds.WaitSet()
        wait_set.attach_condition(fwd_cond)
        wait_set.attach_condition(fault_cond)
        wait_set.attach_condition(hb_cond)

        print("[dds] Listener thread started")

        while self._running:
            try:
                active = wait_set.wait(dds.Duration(seconds=1))
            except dds.TimeoutError:
                continue
            except Exception as exc:
                print(f"[dds] WARN: wait failed — {exc}")
                continue

            if fwd_cond in active:
                for data, info in self._fwd_reader.take():
                    if not info.valid:
                        continue
                    self._enqueue(_sample_to_dict(data, "telemetry"))

            if fault_cond in active:
                for data, info in self._fault_reader.take():
                    if not info.valid:
                        continue
                    self._enqueue(_sample_to_dict(data, "fault"))

            if hb_cond in active:
                for data, info in self._hb_reader.take():
                    if not info.valid:
                        continue
                    self._enqueue(_sample_to_dict(data, "heartbeat"))

        print("[dds] Listener thread stopped")

    def start(self) -> None:
        self._running = True
        self._thread  = threading.Thread(
            target=self._run, daemon=True, name="dds-listener"
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)
        self._participant.close()