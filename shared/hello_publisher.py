# shared/hello_publisher.py
import time
import rti.connextdds as dds
import rti.idl as idl
import sys
sys.path.insert(0, '..')
from shared.dds_types import PanelTelemetry

def main():
    # 1. Join Domain 0
    participant = dds.DomainParticipant(domain_id=0)

    # 2. Declare the topic — name + type must match the subscriber exactly
    topic = dds.Topic(participant, "solar/panel/telemetry/raw", PanelTelemetry)

    # Reliable writer — DDS will retransmit if the subscriber misses a sample
    writer_qos = dds.DataWriterQos()
    writer_qos.reliability.kind = dds.ReliabilityKind.BEST_EFFORT

    writer = dds.DataWriter(participant.implicit_publisher, topic, writer_qos)
    # 3. Create a DataWriter with default QoS for now
    # writer = dds.DataWriter(participant.implicit_publisher, topic)

    print("Publisher ready. Writing samples every 2 seconds...")
    seq = 0
    try:
        while True:
            sample = PanelTelemetry(
                panel_id      = "P00001",
                string_id     = "S01",
                site_id       = "Site-A",
                timestamp_utc = "2025-10-18T10:00:00Z",
                power_w       = 350.5,
                voltage_v     = 36.2,
                current_a     = 9.68,
                status        = "OK",
                fault         = "NONE",
                sequence_num  = seq,
            )
            writer.write(sample)
            print(f"  -> wrote sample seq={seq} panel={sample.panel_id}")
            seq += 1
            time.sleep(2)
    except KeyboardInterrupt:
        print("Publisher stopped.")
    finally:
        participant.close()

if __name__ == "__main__":
    main()