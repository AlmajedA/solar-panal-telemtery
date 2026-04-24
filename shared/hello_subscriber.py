# shared/hello_subscriber.py
import argparse
import rti.connextdds as dds
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.dds_types import PanelTelemetry

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", type=int, default=0)
    ap.add_argument("--topic",  type=str,
                    default="solar/panel/telemetry/raw")
    args = ap.parse_args()

    participant = dds.DomainParticipant(domain_id=args.domain)
    topic  = dds.Topic(participant, args.topic, PanelTelemetry)
    reader = dds.DataReader(participant.implicit_subscriber, topic)

    print(f"Subscriber ready — domain={args.domain}  topic={args.topic}")

    status_condition = dds.StatusCondition(reader)
    status_condition.enabled_statuses = dds.StatusMask.DATA_AVAILABLE
    wait_set = dds.WaitSet()
    wait_set.attach_condition(status_condition)

    try:
        while True:
            try:
                active = wait_set.wait(dds.Duration(seconds=5))
                if not active:
                    continue
            except dds.TimeoutError:
                continue

            for data, info in reader.take():
                if not info.valid:
                    continue
                print(
                    f"  <- domain={args.domain}"
                    f"  panel={data.panel_id}"
                    f"  power={data.power_w:.1f}W"
                    f"  fault={data.fault}"
                    f"  seq={data.sequence_num}"
                )
    except KeyboardInterrupt:
        print("Subscriber stopped.")
    finally:
        participant.close()

if __name__ == "__main__":
    main()