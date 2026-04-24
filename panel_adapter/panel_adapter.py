# panel_adapter/panel_adapter.py
import sys
import os
import json
import subprocess
import argparse
import time
import threading
from pathlib import Path

import rti.connextdds as dds

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.dds_types import PanelTelemetry
from panel_adapter.qos import raw_telemetry_writer_qos


TOPIC_NAME = "solar/panel/telemetry/raw"


def parse_args():
    ap = argparse.ArgumentParser(description="Solar Panel DDS Adapter")
    ap.add_argument("--site", default="Site-A")
    ap.add_argument("--panels", type=int, default=20)
    ap.add_argument("--step", type=int, default=5,
                    help="Telemetry cadence in seconds")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--domain", type=int, default=0)
    ap.add_argument("--hours", type=float, default=None)
    ap.add_argument("--minutes", type=float, default=None)
    return ap.parse_args()


def build_emulator_command(args) -> list[str]:
    """Construct the subprocess command for solar_panel_telemetry.py."""
    script = Path(__file__).parent.parent / "solar_panel_telemetry.py"
    cmd = [
        sys.executable, str(script),
        "--panels", str(args.panels),
        "--step", str(args.step),
        "--seed", str(args.seed),
        "--site", args.site,
        "--format", "jsonl",
    ]
    if args.hours:
        cmd += ["--hours", str(args.hours)]
    elif args.minutes:
        cmd += ["--minutes", str(args.minutes)]
    else:
        cmd += ["--hours", "8"]
    return cmd


def instance_key(sample: PanelTelemetry) -> tuple[str, str, str]:
    """
    Return the DDS key tuple for this sample.

    Adjust this if your IDL marks different fields as @key.
    This version assumes the keyed identity is:
      (panel_id, string_id, site_id)
    """
    return (sample.panel_id, sample.string_id, sample.site_id)


def record_to_sample(rec: dict, site_id: str, seq: int) -> PanelTelemetry:
    """Convert one parsed JSONL record into a PanelTelemetry DDS sample."""
    return PanelTelemetry(
        panel_id=rec["panel_id"],
        string_id=rec["string_id"],
        site_id=site_id,
        timestamp_utc=rec["timestamp_utc"],
        power_w=float(rec["power_w"]),
        voltage_v=float(rec["voltage_v"]),
        current_a=float(rec["current_a"]),
        irradiance_wm2=float(rec["irradiance_wm2"]),
        ambient_temp_c=float(rec["ambient_temp_c"]),
        cell_temp_c=float(rec["cell_temp_c"]),
        orientation_deg=float(rec["orientation_deg"]),
        tilt_deg=float(rec["tilt_deg"]),
        status=rec["status"],
        fault=rec["fault"],
        edge_node_id=os.uname().nodename,
        sequence_num=seq,
    )


def drain_stderr(proc: subprocess.Popen) -> None:
    """Continuously drain emulator stderr to avoid subprocess pipe deadlock."""
    try:
        assert proc.stderr is not None
        for line in proc.stderr:
            line = line.rstrip()
            if line:
                print(f"[emulator][stderr] {line}")
    except Exception as exc:
        print(f"[adapter] WARN: stderr drain failed: {exc}")


def main():
    args = parse_args()

    participant = None
    proc = None
    stderr_thread = None

    # Track one representative sample and handle per keyed instance
    instance_samples: dict[tuple[str, str, str], PanelTelemetry] = {}
    instance_handles: dict[tuple[str, str, str], dds.InstanceHandle] = {}

    seq = 0
    msgs_sent = 0
    start_time = time.monotonic()

    try:
        # ── DDS setup ──────────────────────────────────────────────────────
        participant = dds.DomainParticipant(domain_id=args.domain)
        topic = dds.Topic(participant, TOPIC_NAME, PanelTelemetry)
        writer = dds.DataWriter(
            participant.implicit_publisher,
            topic,
            raw_telemetry_writer_qos()
        )

        print(
            f"[adapter] DDS ready — domain={args.domain}  site={args.site}"
            f"  panels={args.panels}  step={args.step}s"
        )

        # ── Subprocess ─────────────────────────────────────────────────────
        cmd = build_emulator_command(args)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )

        stderr_thread = threading.Thread(
            target=drain_stderr,
            args=(proc,),
            daemon=True
        )
        stderr_thread.start()

        print(f"[adapter] emulator PID={proc.pid}  cmd={' '.join(cmd[:6])}...")

        assert proc.stdout is not None

        for raw_line in proc.stdout:
            line = raw_line.strip()
            if not line:
                continue

            # ── Parse ──────────────────────────────────────────────────────
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"[adapter] WARN: bad JSON — {exc} — line: {line[:120]}")
                continue

            # ── Convert and write ──────────────────────────────────────────
            try:
                sample = record_to_sample(rec, args.site, seq)
                key = instance_key(sample)

                # Explicitly register each keyed instance once.
                # This gives us a reusable handle for write/dispose/unregister.
                if key not in instance_handles:
                    handle = writer.register_instance(sample)
                    instance_handles[key] = handle
                    instance_samples[key] = sample
                else:
                    handle = instance_handles[key]
                    instance_samples[key] = sample

                writer.write(sample, handle)

                seq += 1
                msgs_sent += 1

            except Exception as exc:
                print(f"[adapter] WARN: write failed — {exc}")
                continue

            # ── Progress log every 100 messages ────────────────────────────
            if msgs_sent % 100 == 0:
                elapsed = time.monotonic() - start_time
                rate = msgs_sent / max(elapsed, 1e-9)
                print(
                    f"[adapter] {msgs_sent} msgs sent  "
                    f"rate={rate:.1f} msg/s  "
                    f"last={sample.panel_id}  "
                    f"power={sample.power_w:.1f}W  "
                    f"fault={sample.fault}"
                )

        # Surface non-zero process exits if stdout closes naturally
        return_code = proc.wait()
        if return_code != 0:
            print(f"[adapter] WARN: emulator exited with code {return_code}")

    except KeyboardInterrupt:
        print("\n[adapter] Interrupted — shutting down cleanly...")

    finally:
        # Tell subscribers that each known panel instance is intentionally gone.
        #
        # DDS semantics:
        #   dispose(handle)            -> NOT_ALIVE_DISPOSED
        #   unregister_instance(handle)-> NOT_ALIVE_NO_WRITERS
        #
        # If your domain model says a panel telemetry instance is intentionally
        # no longer valid when this adapter stops, explicitly dispose first,
        # then unregister.
        try:
            if participant is not None:
                # writer only exists if DDS setup succeeded
                if 'writer' in locals():
                    for key, handle in list(instance_handles.items()):
                        sample = instance_samples[key]

                        try:
                            writer.dispose_instance(handle)
                        except Exception as exc:
                            print(f"[adapter] WARN: dispose failed for {key}: {exc}")

                        try:
                            writer.unregister_instance(handle)
                        except Exception as exc:
                            print(f"[adapter] WARN: unregister failed for {key}: {exc}")
        finally:
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

            if participant is not None:
                participant.close()

            elapsed = time.monotonic() - start_time
            print(
                f"[adapter] Done. {msgs_sent} total messages in {elapsed:.1f}s"
                f"  avg={msgs_sent / max(elapsed, 1e-9):.1f} msg/s"
            )


if __name__ == "__main__":
    main()