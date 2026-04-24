# alert_service/rules.py
from dataclasses import dataclass, field
from collections import deque

# ── Thresholds (tune these for your demo) ─────────────────────────────────
HOTSPOT_TEMP_THRESHOLD    = 70.0   # °C — thermal runaway trigger
CONSECUTIVE_FAULT_LIMIT   = 3      # same fault type N times → escalate


@dataclass
class PanelState:
    panel_id:             str
    site_id:              str
    last_fault_type:      str  = "NONE"
    consecutive_count:    int  = 0
    total_faults:         int  = 0
    fault_history:        deque = field(
        default_factory=lambda: deque(maxlen=10)
    )


class AlertRuleEngine:

    def __init__(self) -> None:
        self._panels: dict[str, PanelState] = {}

    def _state(self, panel_id: str, site_id: str) -> PanelState:
        if panel_id not in self._panels:
            self._panels[panel_id] = PanelState(
                panel_id=panel_id, site_id=site_id
            )
        return self._panels[panel_id]

    def evaluate_fault(self, alert) -> list[dict]:
        """
        Apply all rules to a FaultAlert sample.
        Returns a (possibly empty) list of alert dicts to dispatch.
        """
        triggered = []
        s = self._state(alert.panel_id, alert.site_id)
        s.total_faults += 1
        s.fault_history.append(alert.fault_type)

        # ── Rule 1: any FAULT severity ─────────────────────────────────
        # ContentFilteredTopic already ensures only FAULT samples arrive,
        # but we keep the check here as a safety net.
        if alert.severity == "FAULT":
            triggered.append({
                "level":  "CRITICAL",
                "rule":   "SINGLE_FAULT",
                "panel":  alert.panel_id,
                "site":   alert.site_id,
                "detail": (
                    f"type={alert.fault_type}"
                    f"  power={alert.power_w:.1f}W"
                    f"  cell_temp={alert.cell_temp_c:.1f}°C"
                ),
                "time":   alert.timestamp_utc,
            })

        # ── Rule 2: consecutive same fault type ────────────────────────
        if alert.fault_type == s.last_fault_type:
            s.consecutive_count += 1
        else:
            s.consecutive_count = 1
        s.last_fault_type = alert.fault_type

        if s.consecutive_count >= CONSECUTIVE_FAULT_LIMIT:
            triggered.append({
                "level":  "ESCALATED",
                "rule":   "CONSECUTIVE_FAULT",
                "panel":  alert.panel_id,
                "site":   alert.site_id,
                "detail": (
                    f"{alert.fault_type} repeated "
                    f"{s.consecutive_count}x in a row"
                ),
                "time":   alert.timestamp_utc,
            })

        # ── Rule 3: thermal runaway (HOTSPOT + high temperature) ───────
        if (alert.fault_type == "HOTSPOT"
                and alert.cell_temp_c >= HOTSPOT_TEMP_THRESHOLD):
            triggered.append({
                "level":  "CRITICAL",
                "rule":   "THERMAL_RUNAWAY",
                "panel":  alert.panel_id,
                "site":   alert.site_id,
                "detail": (
                    f"cell_temp={alert.cell_temp_c:.1f}°C"
                    f" ≥ threshold {HOTSPOT_TEMP_THRESHOLD}°C"
                ),
                "time":   alert.timestamp_utc,
            })

        return triggered

    def edge_node_lost(self, node_id: str) -> dict:
        return {
            "level":  "CRITICAL",
            "rule":   "EDGE_NODE_OFFLINE",
            "panel":  "N/A",
            "site":   node_id,
            "detail": f"Edge node '{node_id}' stopped sending heartbeats",
            "time":   "now",
        }

    def edge_node_recovered(self, node_id: str) -> dict:
        return {
            "level":  "INFO",
            "rule":   "EDGE_NODE_ONLINE",
            "panel":  "N/A",
            "site":   node_id,
            "detail": f"Edge node '{node_id}' liveliness restored",
            "time":   "now",
        }