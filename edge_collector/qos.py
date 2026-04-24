# edge_collector/qos.py
import rti.connextdds as dds


def raw_reader_qos() -> dds.DataReaderQos:
    qos = dds.DataReaderQos()

    # Match the panel adapter's BEST_EFFORT writer.
    # A RELIABLE reader would also match, but BEST_EFFORT is sufficient here
    # because the SQLite buffer — not DDS retransmission — is our durability.
    qos.reliability.kind = dds.ReliabilityKind.BEST_EFFORT
    qos.durability.kind  = dds.DurabilityKind.VOLATILE

    # Match the writer's history so the DataReader can absorb bursts.
    qos.history.kind  = dds.HistoryKind.KEEP_LAST
    qos.history.depth = 50

    # Mirror the writer's deadline so the reader fires its own callback
    # when a panel stops sending — lets the edge collector log silent panels.
    qos.deadline.period = dds.Duration(seconds=10)

    return qos


def forwarded_writer_qos() -> dds.DataWriterQos:
    qos = dds.DataWriterQos()

    # RELIABLE: every forwarded sample must eventually reach the central side.
    qos.reliability.kind = dds.ReliabilityKind.RELIABLE

    # TRANSIENT_LOCAL: DDS holds up to history.depth samples in memory.
    # When a disconnected central collector reconnects, DDS replays this
    # queue automatically — no custom retry code needed.
    qos.durability.kind = dds.DurabilityKind.TRANSIENT_LOCAL

    qos.history.kind  = dds.HistoryKind.KEEP_LAST
    qos.history.depth = 1000   # ~100 seconds at 10 msg/s burst

    qos.resource_limits.max_samples = 5000   # memory cap on the edge node

    qos.liveliness.kind           = dds.LivelinessKind.MANUAL_BY_PARTICIPANT
    qos.liveliness.lease_duration = dds.Duration(seconds=15)

    return qos


def fault_writer_qos() -> dds.DataWriterQos:
    qos = dds.DataWriterQos()
    qos.reliability.kind = dds.ReliabilityKind.RELIABLE
    qos.durability.kind  = dds.DurabilityKind.TRANSIENT_LOCAL

    # KEEP_ALL: no fault alert is ever silently dropped.
    qos.history.kind = dds.HistoryKind.KEEP_ALL

    return qos


def heartbeat_writer_qos() -> dds.DataWriterQos:
    qos = dds.DataWriterQos()
    qos.reliability.kind          = dds.ReliabilityKind.BEST_EFFORT
    qos.durability.kind           = dds.DurabilityKind.VOLATILE
    qos.history.kind              = dds.HistoryKind.KEEP_LAST
    qos.history.depth             = 1
    qos.liveliness.kind           = dds.LivelinessKind.AUTOMATIC
    qos.liveliness.lease_duration = dds.Duration(seconds=5)

    return qos