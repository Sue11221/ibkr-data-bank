"""Tests for run_log.format_run_log / write_run_log. No network, no GUI.
    python engine/run_log_selftest.py"""
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_log as rl

_P = [0]
_F = [0]


def check(cond, name):
    if cond:
        _P[0] += 1
    else:
        _F[0] += 1
        print("  FAIL:", name)


PARALLEL_REP = {
    "run": "ibkr-20260622-120000", "root": r"C:\X\Stock Data Storage",
    "started": "2026-06-22T12:00:00", "account": "DU007 + DU008",
    "cancelled": False, "recovery_rounds": 1, "aborted_ports": {6000: "down"},
    "series": [{"ticker": "AAPL", "interval": "1m", "added": 1726},
               {"ticker": "TSLA", "interval": "1m", "halt": "series failed: x"}],
    "totals": {"added": 1726, "requests": 10, "dup_existing": 0,
               "conflicts": 0, "halted_series": 1},
    "per_port": {2000: {"account": "DU007", "added": 1726, "series": 1},
                 6000: {"account": "DU013", "added": 0, "series": 0,
                        "aborted": "lost"}},
    "report_path": r"C:\X\_ingest_reports\ibkr-20260622-120000\report.json",
}


def test_format_basic():
    txt = rl.format_run_log(PARALLEL_REP,
                            {"mode": "parallel", "ports": [2000, 6000],
                             "extended": False, "elapsed_s": 399,
                             "finished": "2026-06-22T12:06:39", "flow": "update"})
    check("ibkr-20260622-120000" in txt, "log includes the run id")
    check("Mode          : parallel" in txt, "log includes the mode")
    check("Elapsed       : 6m 39s" in txt, "elapsed formatted m/s")
    check("AAPL 1m: +1726 bars" in txt, "per-series added line")
    check("TSLA 1m: HALT" in txt, "a halted series is shown as HALT")
    check("Bars added    : 1726" in txt and "Halted series : 1" in txt,
          "totals rendered")
    check("Recovery      : 1 round(s); still down: [6000]" in txt,
          "recovery + still-down ports rendered")
    check("port 6000: DU013" in txt and "ABORTED: lost" in txt,
          "per-port aborted detail rendered")
    check("Extended hrs  : off" in txt, "extended-hours flag rendered")
    check("report.json" in txt, "engine report path included")


def test_format_robust_to_empty():
    txt = rl.format_run_log({}, None)               # nothing should crash
    check("RUN LOG" in txt and "Series        : 0" in txt,
          "format_run_log is robust to an empty report")


def test_volatility_reconciliation_rendered_with_exact_debt_counts():
    txt = rl.format_run_log({
        "run": "m3-log", "series": [], "totals": {},
        "vol_value_reconcile": {
            "planned_count": 17,
            "completed_count": 11,
            "request_count": 8,
            "request_count_unknown": 2,
            "settled_count": 4,
            "corrected_count": 3,
            "unresolved_count": 4,
            "queue_pending_count": 10,
            "plan_error_count": 2,
            "rows_truncated": 5,
            "pending_rows": [{}, {}],
            "pending_rows_truncated": 3,
            "pending_plan_tickers": ["A"],
            "pending_plan_tickers_truncated": 4,
            "pending_fill_tickers": ["B", "C"],
            "pending_fill_tickers_truncated": 5,
            "halted_fill_tickers": ["H"],
            "halted_fill_tickers_truncated": 6,
            "status": "incomplete",
            "report": r"C:\Run Logs\m3-report.json",
            "report_error": "secondary publication warning",
        },
    }, {})
    check("-- Volatility reconciliation" in txt
          and "Planned       : 17 day(s)" in txt
          and "Completed     : 11 day(s)" in txt,
          "run log renders planned and completed reconciliation work")
    check("Requests      : 8 known; 2 unknown" in txt
          and "Outcomes      : 4 settled; 3 corrected; 4 unresolved" in txt,
          "run log distinguishes known requests and every outcome bucket")
    check("Queue pending : 10 row(s)" in txt
          and "Plan errors   : 2" in txt,
          "run log exposes durable queue debt and planning failures")
    check("Pending work  : 17 item(s)" in txt
          and "Truncated     : 23 detail item(s) omitted" in txt
          and "Fill-blocked  : 7 ticker(s)" in txt,
          "run log totals retained and truncated pending/fill debt exactly")
    check("Status        : incomplete" in txt
          and r"Report        : C:\Run Logs\m3-report.json" in txt
          and "Report error  : secondary publication warning" in txt,
          "run log includes status and both durable-report fields")


def test_volatility_reconciliation_requested_fallback_and_zero_optionals():
    txt = rl.format_run_log({
        "run": "fix-data-m3", "series": [], "totals": {},
        "vol_value_reconcile": {
            "requested_count": 3, "completed_count": 3,
            "request_count": 3, "request_count_unknown": 0,
            "settled_count": 2, "corrected_count": 1,
            "unresolved_count": 0, "queue_pending_count": 0,
            "rows_truncated": 0, "status": "complete",
        },
    }, {})
    check("Planned       : 3 day(s)" in txt
          and "Plan errors   : 0" in txt
          and "Pending work  : 0 item(s)" in txt
          and "Fill-blocked  : 0 ticker(s)" in txt,
          "shared-ledger requested_count renders as planned work")


def test_volatility_reconciliation_hostile_payload_is_bounded_and_safe():
    class Hostile:
        def __str__(self):
            raise RuntimeError("must not stringify")

        def __int__(self):
            raise RuntimeError("must not coerce")

    class HostileList(list):
        def __len__(self):
            raise RuntimeError("must not measure")

    long_error = ("trader@example.com DU123456\n" + "x" * 100_000)
    txt = rl.format_run_log({
        "run": "hostile-m3", "series": [], "totals": {},
        "vol_value_reconcile": {
            "planned_count": Hostile(), "completed_count": True,
            "request_count": -1, "request_count_unknown": 1.5,
            "settled_count": "4", "corrected_count": 10 ** 100,
            "unresolved_count": None, "queue_pending_count": Hostile(),
            "plan_error_count": False,
            "pending_rows": HostileList([{}]),
            "pending_rows_truncated": 1,
            "rows_truncated": -9,
            "halted_fill_tickers": Hostile(),
            "status": Hostile(), "report": Hostile(),
            "report_error": long_error,
        },
    }, {})
    check("Planned       : ? day(s)" in txt
          and "Requests      : ? known; ? unknown" in txt
          and "Pending work  : ? item(s)" in txt
          and "Truncated     : ? detail item(s) omitted" in txt
          and "Fill-blocked  : ? ticker(s)" in txt
          and "Status        : ?" in txt
          and "Report        : ?" in txt,
          "malformed reconciliation values render placeholders without callbacks")
    error_lines = [line for line in txt.splitlines()
                   if line.startswith("Report error  :")]
    check(len(error_lines) == 1
          and len(error_lines[0].split(": ", 1)[1])
          <= rl._RECONCILE_TEXT_CAP
          and "trader@example.com" not in error_lines[0]
          and "DU123456" not in error_lines[0]
          and "\n" not in error_lines[0],
          "reconciliation text is single-line, redacted, and capped")

    class HostileMapping(dict):
        def get(self, *_args, **_kwargs):
            raise SystemExit("must not call hostile mapping")

    invalid = rl.format_run_log({
        "run": "hostile-map", "series": [], "totals": {},
        "vol_value_reconcile": HostileMapping(),
    }, {})
    check("Payload       : invalid (details omitted)" in invalid,
          "hostile reconciliation mappings are rejected without callbacks")


def _restart_event(**changes):
    event = {
        "phase": "recover", "round": 1, "port": 3000,
        "attempted": False, "decision": "popup_decision_timeout",
        "callback_ok": False, "final_probe": "up",
        "recovered_by_probe": True, "ok": True,
        "reason": "confirmation timed out",
    }
    event.update(changes)
    return event


def test_restart_decisions_rendered_and_redacted():
    event = _restart_event(
        reason="contact trader@example.com for DU123456\nthen retry",
        popup_timing={
            "scheduled_at": "2026-07-16T23:55:09.000Z",
            "callback_entered_at": "2026-07-16T23:55:10.000Z",
            "rendered_at": "2026-07-16T23:55:11.000Z",
            "countdown_armed_at": "2026-07-16T23:55:11.100Z",
            "settled_at": "2026-07-17T00:00:09.000Z",
            "expired_at": "2026-07-17T00:00:09.000Z",
        },
        ignored_extra="must not persist")
    txt = rl.format_run_log(
        {"run": "r", "series": [], "totals": {},
         "restart_events": [event]}, {})
    check("-- Restart decisions" in txt
          and "recover round=1 port=3000 attempted=no" in txt
          and "decision=popup_decision_timeout" in txt
          and "callback_ok=no final_probe=up recovered_by_probe=yes ok=yes" in txt,
          "restart section renders every normalized outcome field")
    check("trader@example.com" not in txt and "DU123456" not in txt
          and "[redacted-email]" in txt and "[redacted-account]" in txt
          and "ignored_extra" not in txt,
          "restart rendering redacts reasons and drops extra callback fields")
    check("popup_timing scheduled=2026-07-16T23:55:09.000Z" in txt
          and "entered=2026-07-16T23:55:10.000Z" in txt
          and "armed=2026-07-16T23:55:11.100Z" in txt,
          "restart rendering exposes the bounded popup stage timeline")


def test_restart_event_loop_starvation_alert():
    txt = rl.format_run_log(
        {"run": "r", "series": [], "totals": {},
         "restart_events": [_restart_event(
             decision="event_loop_starved",
             reason="GUI event loop did not enter popup callback")]}, {})
    check("ALERT: GUI event loop starvation" in txt
          and "no blind restart was attempted" in txt
          and "decision=event_loop_starved" in txt,
          "restart rendering makes event-loop starvation explicit")


def test_restart_decisions_bounded_and_truncated():
    events = [_restart_event(port=1000 + i, reason="x" * 10_000)
              for i in range(rl._RESTART_RENDER_CAP + 2)]
    txt = rl.format_run_log(
        {"run": "r", "series": [], "totals": {},
         "restart_events": events, "restart_events_truncated": 7}, {})
    lines = [line for line in txt.splitlines() if line.startswith("  recover ")]
    reason = lines[0].split("reason=", 1)[1]
    check(len(lines) == rl._RESTART_RENDER_CAP
          and len(reason) == rl._RESTART_REASON_CAP,
          "restart rendering caps event count and each oversized reason")
    check("... 9 restart event(s) omitted" in txt,
          "restart rendering combines engine and formatter truncation counts")


def test_restart_decisions_malformed_never_raise():
    class Hostile:
        def __str__(self):
            raise RuntimeError("do not stringify")

    txt = rl.format_run_log(
        {"run": "r", "series": [], "totals": {},
         "restart_events": [None, {
             "phase": [], "round": True, "port": 10 ** 10,
             "attempted": 1, "decision": Hostile(),
             "callback_ok": None, "final_probe": "sideways",
             "recovered_by_probe": "yes", "ok": 0,
             "reason": Hostile()}]}, {})
    check("event 1: invalid payload" in txt
          and "? round=? port=? attempted=?" in txt
          and "decision=<Hostile>" in txt and "final_probe=?" in txt,
          "malformed restart events render bounded placeholders without callbacks")
    invalid = rl.format_run_log(
        {"run": "r", "series": [], "totals": {},
         "restart_events": "not-a-list", "restart_events_truncated": 3}, {})
    oversized = rl.format_run_log(
        {"run": "r", "series": [], "totals": {},
         "restart_events": [], "restart_events_truncated": 10 ** 10_000}, {})
    check("invalid restart_events payload omitted" in invalid
          and "... 3 restart event(s) omitted" in invalid
          and "IBKR DATA-FETCH RUN LOG" in oversized,
          "wrong-shape restart payload is explicit and non-fatal")


def test_validation_stopped_marker():
    txt = rl.format_run_log({"run": "r", "validation_stopped": True,
                             "series": [], "totals": {}}, {})
    check("STOPPED by the >20% cross-check gate" in txt,
          "a validation-stopped run is flagged in the log")


def test_write_run_log():
    base = tempfile.mkdtemp(prefix="runlog_")
    try:
        p = rl.write_run_log(PARALLEL_REP, Path(base) / "Run Logs",
                             {"mode": "parallel"})
        check(p is not None and Path(p).is_file(), "write_run_log writes a file")
        check(Path(p).name == "ibkr-20260622-120000.log",
              f"log file named <run_id>.log (got {Path(p).name})")
        check(Path(p).parent.name == "Run Logs", "written into the Run Logs dir")
        check("IBKR DATA-FETCH RUN LOG" in
              Path(p).read_text(encoding="utf-8"), "file has the log content")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_no_raise_on_non_dict():
    txt = rl.format_run_log(RuntimeError("boom"), {})   # truthy non-dict
    check("RUN LOG" in txt, "format_run_log tolerates a non-dict report")
    base = tempfile.mkdtemp(prefix="rlnd_")
    try:
        p = rl.write_run_log(RuntimeError("boom"), Path(base) / "Run Logs", {})
        check(p is not None and Path(p).is_file(),
              "write_run_log tolerates a non-dict report (no AttributeError)")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_started_at_consistency():
    txt = rl.format_run_log({"run": "r", "started": "ENGINE-TS",
                             "series": [], "totals": {}},
                            {"started_at": "GUI-TS", "finished": "F"})
    check("Started       : GUI-TS" in txt,
          "Started prefers extras['started_at'] (same clock as Elapsed/Finished)")


def main():
    for t in [v for k, v in sorted(globals().items()) if k.startswith("test_")]:
        t()
    total = _P[0] + _F[0]
    print(f"\nrun_log_selftest: {_P[0]}/{total} passed, {_F[0]} failed")
    return 1 if _F[0] else 0


if __name__ == "__main__":
    sys.exit(main())
