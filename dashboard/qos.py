# dashboard/qos.py
import rti.connextdds as dds


def forwarded_reader_qos() -> dds.DataReaderQos:
    qos = dds.DataReaderQos()
    qos.reliability.kind = dds.ReliabilityKind.RELIABLE
    qos.durability.kind  = dds.DurabilityKind.TRANSIENT_LOCAL
    qos.history.kind     = dds.HistoryKind.KEEP_LAST
    # Only keep the latest sample per panel — dashboard shows current state,
    # not history (history comes from TimescaleDB REST queries)
    qos.history.depth    = 1
    return qos


def fault_reader_qos() -> dds.DataReaderQos:
    qos = dds.DataReaderQos()
    qos.reliability.kind = dds.ReliabilityKind.RELIABLE
    qos.durability.kind  = dds.DurabilityKind.TRANSIENT_LOCAL
    qos.history.kind     = dds.HistoryKind.KEEP_LAST
    qos.history.depth    = 100
    return qos


def heartbeat_reader_qos() -> dds.DataReaderQos:
    qos = dds.DataReaderQos()
    qos.reliability.kind = dds.ReliabilityKind.BEST_EFFORT
    qos.durability.kind  = dds.DurabilityKind.VOLATILE
    qos.history.kind     = dds.HistoryKind.KEEP_LAST
    qos.history.depth    = 1
    return qos