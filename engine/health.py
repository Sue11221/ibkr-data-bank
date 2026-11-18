"""Unified storage-bank health report.

DATA_INTEGRITY_HARDENING.md WS5: aggregate the standing detectors into one
report the GUI can run from Verify/Fix-data. The default audit is offline:
no network, no fetches, no remediation. Cached split evidence is audited before
triage so exact current context wins over the legacy ratio heuristic. A caller
must explicitly inject a triage reference to use that legacy external path;
normal health remains offline. ``write=True`` persists sidecars only.
"""

from __future__ import annotations

import datetime as _dt
import json
import sys
from pathlib import Path

import calendar_reconciler
import coverage_audit
import gap_evidence
import identity as identity_audit
import market_calendar as mc
import split_audit as split_audit_report
import stock_storage as ss
import stock_validate as sv
import triage_classifier


HEALTH_REPORT_FILE = "_health_report.json"
ACTIVE_WINDOW_DAYS = 4
SPLIT_REPAIR_VERDICTS = {
    "PHANTOM", "MISSING", "REGRESSION", "IDENTITY_BASIS",
}


def _active_ticker_dirs(root):
    try:
        entries = sorted(Path(root).iterdir(), key=lambda p: p.name.lower())
    except OSError:
        return []
    return [
        p for p in entries
        if p.is_dir() and not p.name.startswith("_")
        and ss.TICKER_DIR_RE.match(p.name)
    ]


def _month_key(value):
    s = str(value or "").strip()
    if len(s) < 7:
        return None
    try:
        y = int(s[:4])
        m = int(s[5:7])
    except ValueError:
        return None
    if not (1 <= m <= 12):
        return None
    return f"{y:04d}-{m:02d}"


def _presentish_months(manifest, interval):
    try:
        raw = ((manifest.get("intervals") or {}).get(interval) or {}).get(
            "months") or {}
    except AttributeError:
        return []
    out = []
    for key, ent in raw.items():
        if not isinstance(ent, dict):
            continue
        status = str(ent.get("status") or "present").upper()
        if status == "MISSING":
            continue
        mk = _month_key(key)
        if mk:
            out.append(mk)
    return sorted(set(out))


def audit_frontiers(root, write=False, asof=None):
    """Strict-read each series frontier month and report halt-risk files.

    This is WS2a's offline detector only. It never moves/quarantines files and
    never updates manifests.
    """
    root = Path(root)
    asof = asof or _dt.datetime.now().isoformat(timespec="seconds")
    frozen = []
    missing = []
    errors = []
    series_checked = 0
    tickers_seen = 0

    for tdir in _active_ticker_dirs(root):
        tickers_seen += 1
        man = ss.load_manifest(tdir)
        if not man:
            errors.append({"ticker": tdir.name, "error": "manifest unavailable"})
            continue
        for interval in sorted((man.get("intervals") or {})):
            months = _presentish_months(man, interval)
            if not months:
                continue
            month = months[-1]
            series_checked += 1
            year, mon = int(month[:4]), int(month[5:7])
            path = ss.find_month_file(root, tdir.name, year, mon, interval)
            if path is None:
                missing.append({
                    "ticker": tdir.name,
                    "interval": interval,
                    "month": month,
                    "path": str(ss.month_file_path(
                        root, tdir.name, year, mon, interval)),
                    "error": "frontier file missing",
                })
                continue
            try:
                ss.read_month_file(path, validate=True)
            except Exception as exc:  # noqa: BLE001 - report, do not abort
                frozen.append({
                    "ticker": tdir.name,
                    "interval": interval,
                    "month": month,
                    "path": str(path),
                    "error": f"{type(exc).__name__}: {exc}",
                })

    return {
        "kind": "frontier_audit",
        "version": 1,
        "asof": asof,
        "root": str(root),
        "ticker_count": tickers_seen,
        "series_checked": series_checked,
        "frozen_frontiers": sorted(
            frozen, key=lambda r: (r["ticker"], r["interval"], r["month"])),
        "missing_frontiers": sorted(
            missing, key=lambda r: (r["ticker"], r["interval"], r["month"])),
        "errors": errors,
    }


def reconcile_calendar(root, min_tickers=2, consensus_fn=None,
                       is_trading_day_fn=None, asof=None, write=False):
    """Compare the data-derived consensus calendar with market_calendar.

    Production uses calendar_reconciler.maintain(), which can write the
    auto-maintained sidecar. The injected-function branch preserves the old
    lightweight harness behavior for tests.
    """
    root = Path(root)
    asof = asof or _dt.datetime.now().isoformat(timespec="seconds")
    if consensus_fn is None and is_trading_day_fn is None:
        return calendar_reconciler.maintain(
            root, min_tickers=min_tickers, write=write, asof=asof)

    consensus_fn = consensus_fn or sv.consensus_calendar
    is_trading_day_fn = is_trading_day_fn or mc.is_trading_day
    cal = consensus_fn(root, min_tickers=min_tickers)
    if not cal:
        return {
            "kind": "calendar_reconciler",
            "version": 1,
            "asof": asof,
            "root": str(root),
            "min_tickers": min_tickers,
            "error": "no consensus calendar (bank has no 1d series)",
            "data_closed": [],
            "missing": [],
        }
    cal = set(cal)
    lo, hi = min(cal), max(cal)
    one = _dt.timedelta(days=1)

    def market_active_around(day):
        for k in range(1, ACTIVE_WINDOW_DAYS + 1):
            if (day - k * one) in cal or (day + k * one) in cal:
                return True
        return False

    data_closed = []
    day = lo
    while day <= hi:
        if day.weekday() < 5 and day not in cal and market_active_around(day):
            data_closed.append(day)
        day += one
    missing = [d for d in data_closed if is_trading_day_fn(d)]
    return {
        "kind": "calendar_reconciler",
        "version": 1,
        "asof": asof,
        "root": str(root),
        "min_tickers": min_tickers,
        "active_window_days": ACTIVE_WINDOW_DAYS,
        "range": [lo.isoformat(), hi.isoformat()],
        "data_closed": [d.isoformat() for d in data_closed],
        "missing": [d.isoformat() for d in missing],
    }


def _component_error(kind, exc):
    return {
        "kind": kind,
        "error": f"{type(exc).__name__}: {exc}",
    }


def _capture_bank_state(root, state_fn=None):
    fn = state_fn or ss.bank_manifest_state_fingerprint
    try:
        value = fn(root)
        if (not isinstance(value, dict)
                or value.get("schema_version")
                != ss.BANK_STATE_FINGERPRINT_VERSION
                or value.get("algorithm") != "sha256"
                or not isinstance(value.get("sha256"), str)
                or len(value["sha256"]) != 64
                or any(char not in "0123456789abcdef"
                       for char in value["sha256"])
                or isinstance(value.get("manifest_count"), bool)
                or not isinstance(value.get("manifest_count"), int)
                or value["manifest_count"] < 0):
            raise ValueError("bank-state helper returned an invalid contract")
        return value, None
    except Exception as exc:  # noqa: BLE001 - evidence failure is report data
        return None, f"{type(exc).__name__}: {exc}"


def _source_state(before, before_error, after, after_error):
    reasons = []
    if before_error:
        reasons.append(f"before: {before_error}")
    if after_error:
        reasons.append(f"after: {after_error}")
    before_sha = before.get("sha256") if isinstance(before, dict) else None
    after_sha = after.get("sha256") if isinstance(after, dict) else None
    if not reasons and before_sha != after_sha:
        reasons.append("bank manifest state changed during health audit")
    out = {
        "schema_version": ss.BANK_STATE_FINGERPRINT_VERSION,
        "algorithm": "sha256",
        "before_sha256": before_sha,
        "after_sha256": after_sha,
        "manifest_count": (
            after.get("manifest_count") if isinstance(after, dict) else None),
        "current": not reasons and before_sha == after_sha and bool(after_sha),
    }
    if reasons:
        out["error"] = "; ".join(reasons)[:320]
    return out


def _offline_triage_reference(_ticker, rng="Max"):
    del rng
    raise RuntimeError("offline health has no matching cached split context")


def audit(root, project_root=None, write=False, write_components=None,
          asof=None, calendar_min_tickers=2, triage_flags=None,
          triage_ref_fn=None, split_audit_fn=None, gap_audit_fn=None,
          source_state_fn=None):
    """Return the unified health report.

    ``write=True`` writes ``_health_report.json``. By default it also refreshes
    the component report sidecars for coverage, identity, calendar, and splits,
    preserving the GUI behavior where Verify/Fix-data updates cached reports.
    """
    root = Path(root)
    asof = asof or _dt.datetime.now().isoformat(timespec="seconds")
    if write_components is None:
        write_components = bool(write)
    errors = []
    bank_before, bank_before_error = _capture_bank_state(
        root, state_fn=source_state_fn)

    try:
        cov = coverage_audit.audit(root, write=write_components, asof=asof)
    except Exception as exc:  # noqa: BLE001
        cov = _component_error("coverage_audit", exc)
        errors.append({"component": "coverage", "error": cov["error"]})
    try:
        ident = identity_audit.audit(
            root, project_root=project_root, write=write_components, asof=asof)
    except Exception as exc:  # noqa: BLE001
        ident = _component_error("identity_audit", exc)
        errors.append({"component": "identity", "error": ident["error"]})
    try:
        gaps_fn = gap_audit_fn or gap_evidence.evaluate
        gaps = gaps_fn(root)
    except Exception as exc:  # noqa: BLE001
        gaps = _component_error("gap_evidence_evaluation", exc)
        errors.append({"component": "gaps", "error": gaps["error"]})
    try:
        frontier = audit_frontiers(root, asof=asof)
    except Exception as exc:  # noqa: BLE001
        frontier = _component_error("frontier_audit", exc)
        errors.append({"component": "frontier", "error": frontier["error"]})
    try:
        calendar = reconcile_calendar(
            root, min_tickers=calendar_min_tickers, asof=asof,
            write=write_components)
    except Exception as exc:  # noqa: BLE001
        calendar = _component_error("calendar_reconciler", exc)
        errors.append({"component": "calendar", "error": calendar["error"]})
    try:
        split_fn = split_audit_fn or split_audit_report.audit
        splits = split_fn(root, write=write_components)
    except Exception as exc:  # noqa: BLE001
        splits = _component_error("split_audit", exc)
        errors.append({"component": "splits", "error": splits["error"]})
    try:
        triage = triage_classifier.audit(
            root, flags=triage_flags or [],
            ref_fn=triage_ref_fn or _offline_triage_reference,
            split_context=splits, asof=asof)
    except Exception as exc:  # noqa: BLE001
        triage = _component_error("triage_classifier", exc)
        errors.append({"component": "triage", "error": triage["error"]})

    bank_after, bank_after_error = _capture_bank_state(
        root, state_fn=source_state_fn)
    source_state = _source_state(
        bank_before, bank_before_error, bank_after, bank_after_error)
    if not source_state["current"]:
        errors.append({
            "component": "source_state",
            "error": source_state.get("error") or "health source state is unavailable",
        })

    report = {
        "kind": "health_report",
        "version": 2,
        "asof": asof,
        "root": str(root),
        "coverage": cov,
        "identity": ident,
        "gaps": gaps,
        "frontier": frontier,
        "calendar": calendar,
        "splits": splits,
        "triage": triage,
        "source_state": source_state,
        "errors": errors,
    }
    report["counts"] = summary_counts(report)
    report["queue"] = format_queue(report)
    report["clean"] = _is_clean(report)
    if write:
        report["report_path"] = str(root / HEALTH_REPORT_FILE)
        write_report(root, report)
    return report


def _is_clean(report):
    c = _report_counts(report)
    return not any([
        c.get("coverage_forward_stale"),
        c.get("coverage_front_short"),
        c.get("coverage_unknown"),
        c.get("gap_fillable_series"),
        c.get("gap_unavailable_series"),
        c.get("gap_evidence_errors"),
        c.get("identity_duplicate_conids"),
        c.get("identity_quarantine_reuse_unresolved"),
        c.get("frontier_frozen"),
        c.get("frontier_missing"),
        c.get("calendar_missing"),
        c.get("split_repair_queue"),
        c.get("triage_needs_human"),
        c.get("triage_errors"),
        not c.get("health_source_current"),
        c.get("component_errors"),
    ])


def _report_counts(report):
    """Return counts with fail-closed compatibility for pre-identity-v2 caches."""
    c = dict(report.get("counts") or summary_counts(report))
    raw = int(c.get("identity_quarantine_reuse") or 0)
    if "identity_quarantine_reuse_unresolved" not in c:
        c["identity_quarantine_reuse_unresolved"] = raw
    if "identity_quarantine_reuse_resolved" not in c:
        c["identity_quarantine_reuse_resolved"] = max(
            0, raw - int(c["identity_quarantine_reuse_unresolved"] or 0))
    return c


def _split_repair_rows(splits):
    if not isinstance(splits, dict) or splits.get("error"):
        return []
    return [
        row for row in splits.get("repair_queue") or []
        if isinstance(row, dict)
        and str(row.get("verdict") or "").strip().upper()
        in SPLIT_REPAIR_VERDICTS
        and row.get("repair_queue")
    ]


def write_report(root, report):
    target = Path(root) / HEALTH_REPORT_FILE
    out = dict(report or {})
    out["report_path"] = str(target)
    # This machine sidecar is consumed through Tier 0's bounded 2 MiB reader.
    # Pretty-printing can exceed that contract without adding information.
    payload = json.dumps(
        out, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    ss._atomic_write_bytes(target, payload)
    return str(target)


def load_report(root):
    try:
        data = json.loads(
            (Path(root) / HEALTH_REPORT_FILE).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def summary_counts(report):
    cov = report.get("coverage") or {}
    ident = report.get("identity") or {}
    gaps = report.get("gaps") or {}
    if not isinstance(gaps, dict):
        gaps = {}
    frontier = report.get("frontier") or {}
    calendar = report.get("calendar") or {}
    splits = report.get("splits") or {}
    triage = report.get("triage") or {}
    cov_counts = (coverage_audit.summary_counts(cov)
                  if isinstance(cov, dict) and not cov.get("error") else {})
    ident_counts = (identity_audit.summary_counts(ident)
                    if isinstance(ident, dict) and not ident.get("error")
                    else {})
    triage_counts = (triage_classifier.summary_counts(triage)
                     if isinstance(triage, dict) and not triage.get("error")
                     else {})
    gap_rows = gaps.get("rows") if isinstance(gaps, dict) else []
    gap_fillable = gaps.get("fillable") if isinstance(gaps, dict) else []
    gap_absent = gaps.get("source_absent") if isinstance(gaps, dict) else []
    gap_interior = gaps.get("interior") if isinstance(gaps, dict) else []
    gap_unavailable = gaps.get("unavailable") if isinstance(gaps, dict) else []
    gap_errors = gaps.get("errors") if isinstance(gaps, dict) else []
    if "gaps" not in report:
        gap_errors = [{"state": "missing", "reason": "gap component absent"}]
    source_state = report.get("source_state") or {}
    if not isinstance(source_state, dict):
        source_state = {}
    return {
        "tickers": int(cov.get("ticker_count")
                       or ident.get("ticker_count")
                       or frontier.get("ticker_count")
                       or (splits.get("summary") or {}).get("ticker_count")
                       or 0),
        "coverage_forward_stale": int(cov_counts.get("forward_stale") or 0),
        "coverage_front_short": int(cov_counts.get("front_short") or 0),
        "coverage_unknown": int(cov_counts.get("unknown") or 0),
        "coverage_kind_forward_stale": int(
            cov_counts.get("kind_forward_stale") or 0),
        "coverage_kind_unevaluated": int(
            cov_counts.get("kind_unevaluated") or 0),
        "coverage_kind_series": int(cov_counts.get("kind_series") or 0),
        "gap_expected_series": int(gaps.get("expected_series") or 0),
        "gap_current_series": len(gap_rows or []),
        "gap_fillable_series": len(gap_fillable or []),
        "gap_fillable_days": sum(
            int(row.get("missing_days") or 0)
            for row in (gap_fillable or []) if isinstance(row, dict)),
        "gap_source_absent_series": len(gap_absent or []),
        "gap_source_absent_days": sum(
            int(row.get("source_absent") or 0)
            for row in (gap_absent or []) if isinstance(row, dict)),
        "gap_interior_series": len(gap_interior or []),
        "gap_interior_bars": sum(
            int(row.get("missing_total") or 0)
            for row in (gap_interior or []) if isinstance(row, dict)),
        "gap_unavailable_series": len(gap_unavailable or []),
        "gap_evidence_errors": len(gap_errors or [])
        + (1 if gaps.get("error") else 0),
        "identity_duplicate_conids": int(
            ident_counts.get("duplicate_conids") or 0),
        "identity_duplicate_tickers": int(
            ident_counts.get("duplicate_tickers") or 0),
        "identity_quarantine_reuse": int(
            ident_counts.get("quarantine_reuse") or 0),
        "identity_quarantine_reuse_resolved": int(
            ident_counts.get("quarantine_reuse_resolved") or 0),
        "identity_quarantine_reuse_unresolved": int(
            ident_counts.get("quarantine_reuse_unresolved") or 0),
        "frontier_frozen": len(frontier.get("frozen_frontiers") or []),
        "frontier_missing": len(frontier.get("missing_frontiers") or []),
        "frontier_errors": len(frontier.get("errors") or []),
        "calendar_missing": len(calendar.get("missing") or []),
        "calendar_auto_added": len(calendar.get("auto_added") or []),
        "calendar_data_closed": len(calendar.get("data_closed") or []),
        "split_phantom": int(
            (splits.get("verdict_counts") or {}).get("PHANTOM") or 0),
        "split_missing": int(
            (splits.get("verdict_counts") or {}).get("MISSING") or 0),
        "split_regression": int(
            (splits.get("verdict_counts") or {}).get("REGRESSION") or 0),
        "split_identity_basis": int(
            (splits.get("verdict_counts") or {}).get("IDENTITY_BASIS") or 0),
        "split_real": int(
            (splits.get("verdict_counts") or {}).get("REAL") or 0),
        "split_resolved": int(
            (splits.get("verdict_counts") or {}).get("RESOLVED") or 0),
        "split_stale": int(
            (splits.get("verdict_counts") or {}).get("STALE") or 0),
        "split_unverifiable": int(
            (splits.get("verdict_counts") or {}).get("UNVERIFIABLE") or 0),
        "split_repair_queue": len(_split_repair_rows(splits)),
        "split_needs_confirmation": len(
            splits.get("needs_confirmation") or []),
        "split_operational_warnings": len(
            splits.get("stale_unverifiable_rows") or []),
        "triage_classified": int(triage_counts.get("classified") or 0),
        "triage_needs_human": int(triage_counts.get("needs_human") or 0),
        "triage_needs_confirmation": int(
            triage_counts.get("needs_confirmation") or 0),
        "triage_operational": int(triage_counts.get("operational") or 0),
        "triage_auto_benign": int(triage_counts.get("auto_benign") or 0),
        "triage_phantom": int(triage_counts.get("phantom") or 0),
        "triage_identity_basis": int(
            triage_counts.get("identity_basis") or 0),
        "triage_local_suspect": int(
            triage_counts.get("local_suspect") or 0),
        "triage_errors": int(triage_counts.get("errors") or 0),
        "health_source_current": int(source_state.get("current") is True),
        "component_errors": len(report.get("errors") or []),
    }


def format_queue(report):
    source_state = report.get("source_state")
    if isinstance(source_state, dict) and source_state.get("current") is not True:
        return []
    out = []
    cov = report.get("coverage") or {}
    ident = report.get("identity") or {}
    if isinstance(cov, dict) and not cov.get("error"):
        out.extend(coverage_audit.format_queue(cov))
    if isinstance(ident, dict) and not ident.get("error"):
        out.extend(identity_audit.format_queue(ident))
    gaps = report.get("gaps") or {}
    if isinstance(gaps, dict) and not gaps.get("error"):
        for row in gaps.get("fillable") or []:
            ticker = row.get("ticker") if isinstance(row, dict) else None
            if ticker:
                out.append(str(ticker))
    frontier = report.get("frontier") or {}
    for row in ((frontier.get("frozen_frontiers") or [])
                + (frontier.get("missing_frontiers") or [])):
        ticker = row.get("ticker") if isinstance(row, dict) else None
        if ticker:
            out.append(str(ticker))
    splits = report.get("splits") or {}
    if isinstance(splits, dict) and not splits.get("error"):
        for row in _split_repair_rows(splits):
            ticker = row.get("ticker") if isinstance(row, dict) else None
            if ticker:
                out.append(str(ticker))
    triage = report.get("triage") or {}
    if isinstance(triage, dict) and not triage.get("error"):
        out.extend(triage_classifier.format_queue(triage))
    return sorted(set(out))


def _names(vals, max_names=12):
    vals = [str(v) for v in vals if v]
    if not vals:
        return "none"
    if len(vals) <= max_names:
        return ", ".join(vals)
    return ", ".join(vals[:max_names]) + f", ... +{len(vals) - max_names}"


def split_action_lines(report, max_rows=12):
    """Bounded current split repair/review actions for GUI and text reports."""
    splits = (report or {}).get("splits") or {}
    if not isinstance(splits, dict) or splits.get("error"):
        return []
    limit = max(0, int(max_rows))
    if not limit:
        return []
    rows = _split_repair_rows(splits)
    rows.extend(splits.get("needs_confirmation") or [])
    out = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        verdict = str(row.get("verdict") or "").strip().upper()
        day = str(row.get("date") or "").strip()
        action = str(row.get("recommended_action") or "review split evidence")
        key = (ticker, verdict, day, action)
        if not ticker or key in seen:
            continue
        seen.add(key)
        where = f"@{day}" if day else ""
        out.append(f"{ticker}:{verdict}{where} -> {action}")
        if len(out) >= limit:
            break
    return out


def summarize_report(report, max_names=12):
    c = _report_counts(report)
    lines = [
        ("Health report: "
         f"{c['tickers']} ticker(s), queue={len(report.get('queue') or [])}, "
         f"clean={bool(report.get('clean'))}.")
    ]
    cov = report.get("coverage") or {}
    if cov.get("error"):
        lines.append("Coverage: skipped -> " + str(cov.get("error")))
    else:
        lines.append(
            f"Coverage: {c['coverage_forward_stale']} forward-stale, "
            f"{c['coverage_front_short']} front-short, "
            f"{c['coverage_unknown']} unknown earliest.")
        kind_rows = [
            (f"{row.get('ticker')} {row.get('kind')} "
             f"(last={row.get('kind_last')}, "
             f"primary={row.get('primary_last')}, "
             f"lag={row.get('lag_months')}mo)")
            for row in cov.get("kind_forward_stale") or []
            if isinstance(row, dict)
        ]
        lines.append(
            "Per-kind staleness: "
            f"{int(c.get('coverage_kind_forward_stale') or 0)} "
            "forward-stale, "
            f"{int(c.get('coverage_kind_unevaluated') or 0)} "
            "unevaluated -> "
            f"{_names(kind_rows, max_names=max_names)}")

    gaps = report.get("gaps") or {}
    if gaps.get("error"):
        lines.append("Gaps: skipped -> " + str(gaps.get("error")))
    else:
        fillable_rows = []
        for row in gaps.get("fillable") or []:
            if not isinstance(row, dict):
                continue
            run = row.get("largest_missing_day_run") or {}
            fillable_rows.append(
                f"{row.get('ticker')} {row.get('interval')} "
                f"({row.get('missing_days', 0)}d, largest={run.get('count', 0)})")
        lines.append(
            "Gaps: "
            f"{int(c.get('gap_fillable_series') or 0)} fillable series / "
            f"{int(c.get('gap_fillable_days') or 0)} missing day(s), "
            f"{int(c.get('gap_interior_bars') or 0)} interior bar(s); "
            f"{int(c.get('gap_source_absent_days') or 0)} source-absent day(s) -> "
            f"{_names(fillable_rows, max_names=max_names)}")
        unavailable = [
            f"{row.get('ticker')} {row.get('interval')}:{row.get('state')}"
            for row in gaps.get("unavailable") or [] if isinstance(row, dict)
        ]
        if unavailable or c.get("gap_evidence_errors"):
            lines.append(
                "Gap evidence unavailable: "
                f"{int(c.get('gap_unavailable_series') or 0)} series, "
                f"{int(c.get('gap_evidence_errors') or 0)} source error(s) -> "
                f"{_names(unavailable, max_names=max_names)}")

    ident = report.get("identity") or {}
    if ident.get("error"):
        lines.append("Identity: skipped -> " + str(ident.get("error")))
    else:
        lines.append(
            f"Identity: {c['identity_duplicate_conids']} duplicate conId group(s), "
            f"{c['identity_quarantine_reuse']} raw quarantined-conId reuse "
            f"({c['identity_quarantine_reuse_resolved']} resolved, "
            f"{c['identity_quarantine_reuse_unresolved']} unresolved).")

    frontier = report.get("frontier") or {}
    frozen = [f"{r.get('ticker')} {r.get('interval')}"
              for r in frontier.get("frozen_frontiers") or []]
    missing = [f"{r.get('ticker')} {r.get('interval')}"
               for r in frontier.get("missing_frontiers") or []]
    if frontier.get("error"):
        lines.append("Frozen frontier: skipped -> " + str(frontier.get("error")))
    else:
        lines.append(
            "Frozen frontier: "
            f"{c['frontier_frozen']} strict-read failure(s) -> "
            f"{_names(frozen, max_names=max_names)}")
        if missing:
            lines.append(
                "Frozen frontier: "
                f"{c['frontier_missing']} missing frontier file(s) -> "
                f"{_names(missing, max_names=max_names)}")

    calendar = report.get("calendar") or {}
    if calendar.get("error"):
        lines.append("Calendar drift: skipped -> " + str(calendar.get("error")))
    else:
        added = calendar.get("auto_added") or []
        extra = (f"; auto-pinned {len(added)} -> "
                 f"{_names(added, max_names=max_names)}") if added else ""
        lines.append(
            "Calendar drift: "
            f"{c['calendar_missing']} missing static closure(s) -> "
            f"{_names(calendar.get('missing') or [], max_names=max_names)}"
            + extra)

    splits = report.get("splits") or {}
    if splits.get("error"):
        lines.append("Splits: skipped -> " + str(splits.get("error")))
    else:
        lines.append(
            "Splits: "
            f"PHANTOM={c['split_phantom']}, MISSING={c['split_missing']}, "
            f"REGRESSION={c['split_regression']}, "
            f"IDENTITY_BASIS={c['split_identity_basis']}; "
            f"REAL={c['split_real']}, RESOLVED={c['split_resolved']}; "
            f"STALE={c['split_stale']}, "
            f"UNVERIFIABLE={c['split_unverifiable']}.")
        actions = split_action_lines(report, max_rows=max_names)
        if actions:
            lines.append("Split actions: " + " | ".join(actions))
        if c["split_needs_confirmation"]:
            names = [
                f"{row.get('ticker')}:{row.get('verdict')}"
                for row in splits.get("needs_confirmation") or []
                if isinstance(row, dict)
            ]
            lines.append(
                "Split needs-confirmation: "
                f"{c['split_needs_confirmation']} -> "
                f"{_names(names, max_names=max_names)}")
        if c["split_operational_warnings"]:
            lines.append(
                "Split operational warnings: "
                f"{c['split_operational_warnings']} (not in repair queue).")

    triage = report.get("triage") or {}
    if triage.get("error"):
        lines.append("Triage: skipped -> " + str(triage.get("error")))
    elif triage.get("input_count") or triage.get("classified"):
        lines.extend(triage_classifier.summarize_report(
            triage, max_names=max_names))

    source_state = report.get("source_state") or {}
    if source_state.get("current") is not True:
        lines.append(
            "Health source state: NON-CURRENT -> "
            + str(source_state.get("error") or "source fingerprint unavailable"))

    queue = list(report.get("queue") or format_queue(report))
    if queue:
        lines.append("Health repair queue: " + ", ".join(queue))
    rp = report.get("report_path")
    if rp:
        lines.append(f"Health report saved: {rp}")
    return lines


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    write = "--write" in argv
    if "--no-write" in argv:
        write = False
    root = ss.storage_root(".")
    project_root = None
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in ("--write", "--no-write"):
            i += 1
            continue
        if arg == "--project-root":
            if i + 1 >= len(argv):
                print("--project-root requires a path", file=sys.stderr)
                return 2
            project_root = Path(argv[i + 1])
            i += 2
            continue
        if not arg.startswith("-"):
            root = Path(arg)
        i += 1
    report = audit(root, project_root=project_root, write=write)
    for line in summarize_report(report):
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
