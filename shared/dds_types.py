# shared/types.py
import rti.idl as idl
from dataclasses import field

@idl.struct(
    member_annotations={
        'panel_id':  [idl.key, idl.bound(32)],
        'string_id': [idl.key, idl.bound(16)],
        'site_id':   [idl.key, idl.bound(32)],
    }
)
@idl.struct
class PanelTelemetry:
    panel_id:        str = ""
    string_id:       str = ""
    site_id:         str = ""
    timestamp_utc:   str = ""
    power_w:         float = 0.0
    voltage_v:       float = 0.0
    current_a:       float = 0.0
    irradiance_wm2:  float = 0.0
    ambient_temp_c:  float = 0.0
    cell_temp_c:     float = 0.0
    orientation_deg: float = 0.0
    tilt_deg:        float = 0.0
    status:          str = ""
    fault:           str = ""
    edge_node_id:    str = ""
    sequence_num:    int  = 0


@idl.struct
class FaultAlert:
    panel_id:     str   = ""
    site_id:      str   = ""
    timestamp_utc:str   = ""
    fault_type:   str   = ""
    severity:     str   = ""
    power_w:      float = 0.0
    cell_temp_c:  float = 0.0
    message:      str   = ""

@idl.struct
class HeartbeatSample:
    node_id:              str   = ""
    node_type:            str   = ""
    timestamp_utc:        str   = ""
    panels_active:        int   = 0
    msgs_sent_last_min:   int   = 0
    buffer_utilization_pct: float = 0.0