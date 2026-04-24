# central_collector/qos.py
import rti.connextdds as dds


def forwarded_reader_qos() -> dds.DataReaderQos:
    qos = dds.DataReaderQos()

    # Must match the edge's forwarded writer (RELIABLE + TRANSIENT_LOCAL).
    # TRANSIENT_LOCAL on the reader means: when this reader connects or
    # reconnects, the edge writer replays its cached queue automatically.
    # This is the store-and-forward mechanism completing itself.
    qos.reliability.kind = dds.ReliabilityKind.RELIABLE
    qos.durability.kind  = dds.DurabilityKind.TRANSIENT_LOCAL
    qos.history.kind     = dds.HistoryKind.KEEP_LAST
    qos.history.depth    = 1000

    return qos


def fault_reader_qos() -> dds.DataReaderQos:
    qos = dds.DataReaderQos()
    qos.reliability.kind = dds.ReliabilityKind.RELIABLE
    qos.durability.kind  = dds.DurabilityKind.TRANSIENT_LOCAL
    # KEEP_ALL: never drop a fault alert from the reader buffer
    qos.history.kind     = dds.HistoryKind.KEEP_ALL
    return qos


def heartbeat_reader_qos() -> dds.DataReaderQos:
    qos = dds.DataReaderQos()
    # Heartbeat is fire-and-forget — no retransmission needed
    qos.reliability.kind = dds.ReliabilityKind.BEST_EFFORT
    qos.durability.kind  = dds.DurabilityKind.VOLATILE
    qos.history.kind     = dds.HistoryKind.KEEP_LAST
    qos.history.depth    = 1
    return qos