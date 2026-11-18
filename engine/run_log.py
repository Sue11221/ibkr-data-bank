"""Human-readable RUN LOG for a completed IBKR data-fetch run — the parameters
that were USED plus what happened — distinct from the engine's machine-readable
report.json. Pure and unit-testable: takes the gap_fill report dict and a few
GUI-known extras (mode/ports/since/extended/sensitivity/elapsed)."""
import re
from pathlib import Path


_RESTART_RENDER_CAP = 256
_RESTART_REASON_CAP = 240
_RECONCILE_COUNT_CAP = 1_000_000
_RECONCILE_TEXT_CAP = 400
_RESTART_EMAIL_RE = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_RESTART_ACCOUNT_RE = re.compile(r"\b(?:DU|U)\d{3,12}\b", re.IGNORECASE)


def _restart_text(value, cap, fallback="?"):
    if type(value) is str:
        raw = value[:max(cap * 8, cap)]
    elif value is None or type(value) in (bool, int, float):
        try:
            raw = str(value)
        except (TypeError, ValueError):
            raw = ""
    else:
        raw = f"<{type(value).__name__}>"
    raw = " ".join(raw.split())
    raw = _RESTART_EMAIL_RE.sub("[redacted-email]", raw)
    raw = _RESTART_ACCOUNT_RE.sub("[redacted-account]", raw)
    return (raw or fallback)[:cap]


def _restart_bool(value):
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return "?"


def _restart_int(value, *, lo=0, hi=1_000_000):
    return str(value) if type(value) is int and lo <= value <= hi else "?"


def _restart_rows(report):
    raw = report.get("restart_events")
    truncated = report.get("restart_events_truncated", 0)
    truncated = (truncated if type(truncated) is int
                 and 0 <= truncated <= 1_000_000 else 0)
    if raw is None and not truncated:
        return []
    if not isinstance(raw, list):
        rows = ["  invalid restart_events payload omitted"]
        if truncated:
            rows.append(f"  ... {truncated} restart event(s) omitted")
        return rows

    rows = []
    starved = 0
    phases = {"preflight", "recover"}
    probes = {"up", "down", "error", "not_run"}
    for index, event in enumerate(raw[:_RESTART_RENDER_CAP], start=1):
        if not isinstance(event, dict):
            rows.append(f"  event {index}: invalid payload")
            continue
        if event.get("decision") == "event_loop_starved":
            starved += 1
        phase = event.get("phase")
        phase = phase if type(phase) is str and phase in phases else "?"
        probe = event.get("final_probe")
        probe = probe if type(probe) is str and probe in probes else "?"
        rows.append(
            f"  {phase} round={_restart_int(event.get('round'))} "
            f"port={_restart_int(event.get('port'), lo=1, hi=65535)} "
            f"attempted={_restart_bool(event.get('attempted'))} "
            f"decision={_restart_text(event.get('decision'), 64)} "
            f"callback_ok={_restart_bool(event.get('callback_ok'))} "
            f"final_probe={probe} "
            f"recovered_by_probe="
            f"{_restart_bool(event.get('recovered_by_probe'))} "
            f"ok={_restart_bool(event.get('ok'))} "
            f"reason={_restart_text(event.get('reason'), _RESTART_REASON_CAP)}")
        timing = event.get("popup_timing")
        if isinstance(timing, dict):
            rows.append(
                "    popup_timing "
                f"scheduled={_restart_text(timing.get('scheduled_at'), 40)} "
                f"entered={_restart_text(timing.get('callback_entered_at'), 40)} "
                f"rendered={_restart_text(timing.get('rendered_at'), 40)} "
                f"armed={_restart_text(timing.get('countdown_armed_at'), 40)} "
                f"settled={_restart_text(timing.get('settled_at'), 40)} "
                f"expired={_restart_text(timing.get('expired_at'), 40)}")
    omitted = truncated + max(0, len(raw) - _RESTART_RENDER_CAP)
    if omitted:
        rows.append(f"  ... {omitted} restart event(s) omitted")
    if starved:
        rows.insert(
            0,
            "  ALERT: GUI event loop starvation prevented restart confirmation "
            f"for {starved} event(s); no blind restart was attempted")
    return rows


def _reconcile_count(value):
    """Render an untrusted aggregate count without coercion or callbacks."""
    if (type(value) is not int or value < 0
            or value > _RECONCILE_COUNT_CAP):
        return "?"
    return f"{value:,}"


def _reconcile_text(value, cap, fallback="?"):
    """Render one bounded, single-line string from reconciliation evidence."""
    if type(value) is not str:
        return fallback
    raw = " ".join(value[:max(cap * 8, cap)].split())
    raw = _RESTART_EMAIL_RE.sub("[redacted-email]", raw)
    raw = _RESTART_ACCOUNT_RE.sub("[redacted-account]", raw)
    return (raw or fallback)[:cap]


def _reconcile_total(payload, fields):
    """Return an exact bounded total for integer and retained-list fields."""
    total = 0
    for name, kind in fields:
        if name not in payload:
            continue
        value = payload[name]
        if kind == "list":
            if type(value) is not list:
                return "?"
            count = len(value)
        else:
            if (type(value) is not int or value < 0
                    or value > _RECONCILE_COUNT_CAP):
                return "?"
            count = value
        if count > _RECONCILE_COUNT_CAP - total:
            return "?"
        total += count
    return f"{total:,}"


def _reconcile_rows(report):
    """Render bounded aggregate M3 evidence; never inspect individual rows."""
    try:
        payload = report.get("vol_value_reconcile")
        if payload is None:
            return []
        if type(payload) is not dict:
            return ["Payload       : invalid (details omitted)"]

        planned = (payload.get("planned_count")
                   if "planned_count" in payload
                   else payload.get("requested_count"))
        pending = _reconcile_total(payload, (
            ("pending_rows", "list"),
            ("pending_rows_truncated", "int"),
            ("pending_plan_tickers", "list"),
            ("pending_plan_tickers_truncated", "int"),
            ("pending_fill_tickers", "list"),
            ("pending_fill_tickers_truncated", "int"),
        ))
        truncated = _reconcile_total(payload, (
            ("rows_truncated", "int"),
            ("pending_rows_truncated", "int"),
            ("pending_plan_tickers_truncated", "int"),
            ("pending_fill_tickers_truncated", "int"),
            ("halted_fill_tickers_truncated", "int"),
        ))
        fill_blocked = _reconcile_total(payload, (
            ("halted_fill_tickers", "list"),
            ("halted_fill_tickers_truncated", "int"),
        ))
        rows = [
            f"Planned       : {_reconcile_count(planned)} day(s)",
            ("Completed     : "
             f"{_reconcile_count(payload.get('completed_count'))} day(s)"),
            ("Requests      : "
             f"{_reconcile_count(payload.get('request_count'))} known; "
             f"{_reconcile_count(payload.get('request_count_unknown'))} "
             "unknown"),
            ("Outcomes      : "
             f"{_reconcile_count(payload.get('settled_count'))} settled; "
             f"{_reconcile_count(payload.get('corrected_count'))} corrected; "
             f"{_reconcile_count(payload.get('unresolved_count'))} "
             "unresolved"),
            ("Queue pending : "
             f"{_reconcile_count(payload.get('queue_pending_count'))} row(s)"),
            ("Plan errors   : "
             f"{_reconcile_count(payload.get('plan_error_count', 0))}"),
            f"Pending work  : {pending} item(s)",
            f"Truncated     : {truncated} detail item(s) omitted",
            f"Fill-blocked  : {fill_blocked} ticker(s)",
            ("Status        : "
             f"{_reconcile_text(payload.get('status'), 64)}"),
        ]
        if "report" in payload:
            rows.append(
                "Report        : "
                + _reconcile_text(
                    payload.get("report"), _RECONCILE_TEXT_CAP))
        if "report_error" in payload:
            rows.append(
                "Report error  : "
                + _reconcile_text(
                    payload.get("report_error"), _RECONCILE_TEXT_CAP))
        return rows
    except BaseException:  # hostile mapping/value callbacks must not escape
        return ["Payload       : invalid (details omitted)"]


def _series_row(s):
    t, iv = s.get("ticker", "?"), s.get("interval", "?")
    notes = []
    if s.get("note"):
        notes.append(str(s["note"]))
    notes.extend(str(n) for n in (s.get("notes") or []))
    if s.get("halt"):
        proposal = _split_proposal_text(s)
        if proposal:
            notes.append(proposal)
        tail = ("; " + "; ".join(notes)) if notes else ""
        return f"  {t} {iv}: HALT — {s['halt']}{tail}"
    parts = [f"+{s.get('added', 0)} bars"]
    if s.get("dup_existing"):
        parts.append(f"{s['dup_existing']} dup")
    if s.get("conflicts"):
        parts.append(f"{s['conflicts']} conflict(s)")
    if notes:
        parts.append("; ".join(notes))
    return f"  {t} {iv}: " + ", ".join(parts)


def _split_proposal_text(series):
    enrichment = series.get("split_enrichment")
    if not isinstance(enrichment, dict):
        return None
    if (enrichment.get("status") != "confirmed_split_proposal"
            or enrichment.get("requires_user_approval") is not True
            or enrichment.get("applied") is not False):
        return None
    action = enrichment.get("action")
    if not isinstance(action, dict):
        return None
    factor = action.get("factor")
    if isinstance(factor, bool) or not isinstance(factor, (int, float)):
        return None
    boundary = _restart_text(enrichment.get("boundary_date"), 16)
    return (f"SPLIT PROPOSAL: cached evidence matched {boundary}; price "
            f"factor {factor:.8g} requires approval and is NOT APPLIED")


def format_run_log(report, extras=None):
    """A text run log from a gap_fill report. `extras` (optional) carries the
    GUI-known run parameters: mode, ports, since, extended, sensitivity,
    elapsed_s, finished, flow."""
    if not isinstance(report, dict):     # never raise on an Exception / bad rep
        report = {}
    extras = extras or {}
    out = []
    add = out.append
    add("=" * 64)
    add(f"IBKR DATA-FETCH RUN LOG  ({extras.get('flow', 'run')})")
    add("=" * 64)
    add(f"Run ID        : {report.get('run', '?')}")
    # Started/Finished/Elapsed all derive from the SAME GUI clock (started_at)
    # when available, so they can't disagree (report['started'] is stamped
    # post-preflight / post-join, which would make Elapsed inconsistent).
    add(f"Started       : {extras.get('started_at') or report.get('started', '?')}")
    if extras.get("finished"):
        add(f"Finished      : {extras['finished']}")
    if extras.get("elapsed_s") is not None:
        m, s = divmod(int(extras["elapsed_s"]), 60)
        add(f"Elapsed       : {m}m {s}s")

    add("")
    add("-- Connection ----------------------------------------------------")
    add(f"Mode          : {extras.get('mode', '?')}")
    add(f"Port(s)       : {extras.get('ports', report.get('port', '?'))}")
    add(f"Account(s)    : {report.get('account', '?')}")
    for p, v in (report.get("per_port") or {}).items():
        add(f"    port {p}: {v.get('account')}  +{v.get('added', 0)} bars "
            f"({v.get('series', 0)} series)"
            + (f"  ABORTED: {v['aborted']}" if v.get("aborted") else ""))

    restart_rows = _restart_rows(report)
    if restart_rows:
        add("")
        add("-- Restart decisions ---------------------------------------------")
        out.extend(restart_rows)

    add("")
    add("-- Request -------------------------------------------------------")
    series = report.get("series", []) or []
    add(f"Series        : {len(series)}")
    if extras.get("since") is not None:
        add(f"History since : {extras['since']}")
    if extras.get("extended") is not None:
        add(f"Extended hrs  : {'on' if extras['extended'] else 'off'}")
    if extras.get("sensitivity"):
        add(f"Validation    : {extras['sensitivity']} sensitivity")

    add("")
    add("-- Per series ----------------------------------------------------")
    for s in series:
        add(_series_row(s))

    add("")
    add("-- Totals --------------------------------------------------------")
    t = report.get("totals", {}) or {}
    add(f"Bars added    : {t.get('added', 0)}")
    add(f"Requests      : {t.get('requests', 0)}")
    add(f"Dup existing  : {t.get('dup_existing', 0)}")
    add(f"Conflicts     : {t.get('conflicts', 0)}")
    add(f"Halted series : {t.get('halted_series', 0)}")
    if report.get("recovery_rounds") is not None:
        line = f"Recovery      : {report['recovery_rounds']} round(s)"
        if report.get("aborted_ports"):
            line += f"; still down: {sorted(report['aborted_ports'])}"
        add(line)
    add(f"Cancelled     : {'YES' if report.get('cancelled') else 'no'}")
    if report.get("validation_stopped"):
        add("Validation    : STOPPED by the >20% cross-check gate")
    if report.get("aborted"):
        add(f"Aborted       : {report['aborted']}")
    for n in (report.get("notes") or []):
        add(f"Note          : {n}")

    reconcile_rows = _reconcile_rows(report)
    if reconcile_rows:
        add("")
        add("-- Volatility reconciliation ------------------------------------")
        out.extend(reconcile_rows)

    add("")
    add("-- Output --------------------------------------------------------")
    add(f"Committed to  : {report.get('root', '?')}")
    if report.get("report_path"):
        add(f"Engine report : {report['report_path']}")
    add("=" * 64)
    return "\n".join(out) + "\n"


def write_run_log(report, out_dir, extras=None):
    """Write the run log to <out_dir>/<run_id>.log. Best-effort — returns the
    path, or None on failure (never raises into the GUI)."""
    if not isinstance(report, dict):
        report = {}
    try:
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        run = (report or {}).get("run") or "ibkr-run"
        path = out / f"{run}.log"
        path.write_text(format_run_log(report, extras), encoding="utf-8")
        return str(path)
    except (OSError, TypeError, ValueError):
        return None
