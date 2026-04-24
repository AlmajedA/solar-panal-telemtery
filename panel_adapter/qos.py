# panel_adapter/qos.py
import rti.connextdds as dds

def raw_telemetry_writer_qos() -> dds.DataWriterQos:
    qos = dds.DataWriterQos()

    # BEST_EFFORT: no retransmission on the LAN.
    # Rationale: we have 50–100 panels writing every few seconds.
    # Requiring ACKs from the edge collector for every sample would
    # create backpressure that isn't worth it for sensor data.
    qos.reliability.kind = dds.ReliabilityKind.BEST_EFFORT

    # VOLATILE: don't cache samples for late-joining readers.
    # Rationale: old raw readings have no value — only fresh ones matter.
    qos.durability.kind = dds.DurabilityKind.VOLATILE

    # KEEP_LAST 50: buffer the last 50 samples per panel instance.
    # At --step 5 (one sample per 5 seconds), this covers ~4 minutes
    # of LAN micro-bursts without excessive memory use.
    qos.history.kind  = dds.HistoryKind.KEEP_LAST
    qos.history.depth = 50

    # DEADLINE 10s: if a panel stops writing for 10 seconds,
    # the DataReader's deadline callback fires. The edge collector
    # uses this to detect silent panels.
    qos.deadline.period = dds.Duration(seconds=10)

    # AUTOMATIC liveliness: RTI sends a liveliness ping on our behalf
    # as long as the process is alive. The edge collector detects
    # a dead adapter within 5 seconds of process crash.
    qos.liveliness.kind          = dds.LivelinessKind.AUTOMATIC
    qos.liveliness.lease_duration = dds.Duration(seconds=5)

    return qos