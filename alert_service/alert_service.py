# alert_service/alert_service.py
import sys
import argparse
from pathlib import Path

import rti.connextdds as dds

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.dds_types import FaultAlert, HeartbeatSample
from alert_service.rules    import AlertRuleEngine
from alert_service.notifier import dispatch

TOPIC_FAULTS    = "solar/alerts/faults"
TOPIC_HEARTBEAT = "solar/health/heartbeat"


def parse_args():
    ap = argparse.ArgumentParser(description="Alert Service")
    ap.add_argument("--domain", type=int, default=1)
    return ap.parse_args()


def fault_reader_qos() -> dds.DataReaderQos:
    qos = dds.DataReaderQos()
    qos.reliability.kind = dds.ReliabilityKind.RELIABLE
    qos.durability.kind  = dds.DurabilityKind.TRANSIENT_LOCAL
    # KEEP_ALL: never drop a fault alert before the rule engine sees it
    qos.history.kind     = dds.HistoryKind.KEEP_ALL
    return qos


def heartbeat_reader_qos() -> dds.DataReaderQos:
    qos = dds.DataReaderQos()
    qos.reliability.kind          = dds.ReliabilityKind.BEST_EFFORT
    qos.durability.kind           = dds.DurabilityKind.VOLATILE
    qos.history.kind              = dds.HistoryKind.KEEP_LAST
    qos.history.depth             = 1
    # Must match the edge's heartbeat writer lease duration (5 s).
    # If the edge writer stops asserting liveliness, the reader fires
    # LIVELINESS_CHANGED within lease_duration seconds.
    qos.liveliness.kind           = dds.LivelinessKind.AUTOMATIC
    qos.liveliness.lease_duration = dds.Duration(seconds=5)
    return qos


class AlertService:

    def __init__(self, domain_id: int) -> None:
        self._engine       = AlertRuleEngine()
        self._msgs_faults  = 0
        self._msgs_hb      = 0
        self._alerts_fired = 0

        self._setup_dds(domain_id)

    # ── DDS setup ──────────────────────────────────────────────────────────

    def _setup_dds(self, domain_id: int) -> None:
        self._participant = dds.DomainParticipant(domain_id=domain_id)

        fault_topic = dds.Topic(
            self._participant, TOPIC_FAULTS, FaultAlert
        )

        filtered_fault_topic = dds.ContentFilteredTopic(
            fault_topic,
            "filtered_faults_critical",
            dds.Filter("severity = 'FAULT'"),
        )

        self._fault_reader = dds.DataReader(
            self._participant.implicit_subscriber,
            filtered_fault_topic,
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

    # ── Handlers ──────────────────────────────────────────────────────────

    def _handle_faults(self) -> None:
        for data, info in self._fault_reader.take():
            if not info.valid:
                continue
            self._msgs_faults += 1
            alerts = self._engine.evaluate_fault(data)
            for alert in alerts:
                self._alerts_fired += 1
                dispatch(alert)

    def _handle_heartbeat(self) -> None:

        # ── 1. Drain data samples ──────────────────────────────────────────
        for data, info in self._hb_reader.take():
            if not info.valid:
                continue
            self._msgs_hb += 1
            print(
                f"[alert] heartbeat — node={data.node_id}"
                f"  panels={data.panels_active}"
            )

        # ── 2. SUBSCRIPTION_MATCHED — catches clean Ctrl+C shutdowns ──────
        # When participant.close() is called, an RTPS BYE is sent.
        # The reader sees the writer count drop via SUBSCRIPTION_MATCHED
        # before the liveliness lease has time to expire.
        try:
            sm = self._hb_reader.subscription_matched_status

            if sm.current_count_change < 0:
                # A writer disconnected cleanly
                print(
                    f"[alert] SUBSCRIPTION_MATCHED: writer disconnected —"
                    f"  writers_now={sm.current_count}"
                )
                alert = self._engine.edge_node_lost(
                    f"edge-node (clean disconnect, writers={sm.current_count})"
                )
                self._alerts_fired += 1
                dispatch(alert)

            elif sm.current_count_change > 0:
                # A new writer connected — edge came back online
                print(
                    f"[alert] SUBSCRIPTION_MATCHED: writer connected —"
                    f"  writers_now={sm.current_count}"
                )
                alert = self._engine.edge_node_recovered(
                    f"edge-node (reconnected, writers={sm.current_count})"
                )
                dispatch(alert)

        except Exception as exc:
            print(f"[alert] WARN: subscription_matched check failed — {exc}")

        # ── 3. LIVELINESS_CHANGED — catches crashes / kill -9 ─────────────
        # This fires only after the writer's 5-second liveliness lease
        # expires with no assertion. It won't fire for clean shutdowns
        # because RTPS BYE already notified us via SUBSCRIPTION_MATCHED.
        try:
            lc = self._hb_reader.liveliness_changed_status

            if lc.not_alive_count_change > 0:
                print(
                    f"[alert] LIVELINESS LOST (crash) —"
                    f"  not_alive={lc.not_alive_count}"
                    f"  change={lc.not_alive_count_change}"
                )
                alert = self._engine.edge_node_lost(
                    f"edge-node (crash/timeout, not_alive={lc.not_alive_count})"
                )
                self._alerts_fired += 1
                dispatch(alert)

            elif lc.alive_count_change > 0:
                print(
                    f"[alert] LIVELINESS RESTORED —"
                    f"  alive={lc.alive_count}"
                )
                alert = self._engine.edge_node_recovered(
                    f"edge-node (liveliness restored, alive={lc.alive_count})"
                )
                dispatch(alert)

        except Exception as exc:
            print(f"[alert] WARN: liveliness check failed — {exc}")

    # ── Main loop ──────────────────────────────────────────────────────────

    def run(self) -> None:
        fault_cond = dds.StatusCondition(self._fault_reader)
        hb_cond    = dds.StatusCondition(self._hb_reader)

        # Enable both data arrival AND liveliness changes on the
        # heartbeat condition — one condition covers both event types
        fault_cond.enabled_statuses = dds.StatusMask.DATA_AVAILABLE
        hb_cond.enabled_statuses    = (
            dds.StatusMask.DATA_AVAILABLE
            | dds.StatusMask.LIVELINESS_CHANGED
            | dds.StatusMask.SUBSCRIPTION_MATCHED
        )

        wait_set = dds.WaitSet()
        wait_set.attach_condition(fault_cond)
        wait_set.attach_condition(hb_cond)

        print(
            f"[alert] Running — domain={self._participant.domain_id}\n"
            f"[alert] ContentFilteredTopic: severity = 'FAULT' only\n"
            f"[alert] Liveliness monitoring: crash detected in 5 s, "
            f"clean shutdown detected immediately"
        )

        try:
            while True:
                try:
                    active = wait_set.wait(dds.Duration(seconds=1))
                except dds.TimeoutError:
                    continue

                if fault_cond in active:
                    self._handle_faults()

                if hb_cond in active:
                    self._handle_heartbeat()

        except KeyboardInterrupt:
            print("\n[alert] Interrupted.")

        finally:
            self._participant.close()
            print(
                f"[alert] Done —"
                f"  faults={self._msgs_faults}"
                f"  heartbeats={self._msgs_hb}"
                f"  alerts_fired={self._alerts_fired}"
            )


def main():
    args = parse_args()
    service = AlertService(domain_id=args.domain)
    service.run()


if __name__ == "__main__":
    main()