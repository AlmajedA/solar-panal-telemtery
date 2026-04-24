# alert_service/notifier.py

# ANSI colour codes for terminal output
_LEVEL_COLOUR = {
    "CRITICAL":  "\033[91m",   # bright red
    "ESCALATED": "\033[93m",   # bright yellow
    "WARNING":   "\033[93m",   # bright yellow
    "INFO":      "\033[92m",   # bright green
}
_RESET = "\033[0m"

_total = 0


def dispatch(alert: dict) -> None:
    """
    Print the alert to the terminal.
    Extend this function to send emails, webhooks, SMS, etc.
    """
    global _total
    _total += 1

    level  = alert.get("level",  "UNKNOWN")
    rule   = alert.get("rule",   "UNKNOWN")
    panel  = alert.get("panel",  "N/A")
    site   = alert.get("site",   "N/A")
    detail = alert.get("detail", "")
    time   = alert.get("time",   "")

    colour = _LEVEL_COLOUR.get(level, "")

    print(
        f"\n{colour}"
        f"╔═ ALERT #{_total} ═══════════════════════════════\n"
        f"║  level  : {level}\n"
        f"║  rule   : {rule}\n"
        f"║  panel  : {panel}   site: {site}\n"
        f"║  detail : {detail}\n"
        f"║  time   : {time}\n"
        f"╚══════════════════════════════════════════"
        f"{_RESET}"
    )

    # ── Extension points ───────────────────────────────────────────────
    # _send_webhook(alert)
    # _send_email(alert)


def _send_webhook(alert: dict) -> None:
    """POST alert JSON to a webhook URL."""
    import json, urllib.request
    url  = "https://your-webhook-url.example.com/alerts"
    body = json.dumps(alert).encode()
    req  = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=3)
    except Exception as exc:
        print(f"[notifier] WARN: webhook failed — {exc}")