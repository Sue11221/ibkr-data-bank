"""Offline coverage-completeness audit for the storage bank.

This is COVERAGE_AUDIT_PLAN.md Phase 1: detection/reporting only. It joins
manifest coverage with the cached IBKR earliest-available sidecar, without any
network access and without changing fetch or seal behavior.
"""

from __future__ import annotations

import datetime as _dt
import json
import sys
from pathlib import Path

import stock_storage as ss
import stock_validate as sv


COVERAGE_REPORT_FILE = "_coverage_audit.json"
REPORT_VERSION = 2
FORWARD_STALE_MONTHS = 2
KIND_FORWARD_STALE_MONTHS = 2
FRONT_LATE_MONTHS = 18
FRONT_AVAIL_MONTHS = 18
PRIMARY_INTERVAL = "1m"
FALLBACK_INTERVALS = ("1d",)


def month_ord(ym) -> int:
    """Convert YYYY-MM or YYYY-MM-DD-ish text to a monotonic month ordinal."""
    s = str(ym or "").strip()
    if len(s) < 7 or s[4:5] != "-":
        raise ValueError(f"bad year-month value: {ym!r}")
    year, month = int(s[:4]), int(s[5:7])
    if month < 1 or month > 12:
        raise ValueError(f"bad year-month value: {ym!r}")
    return year * 12 + month - 1


def month_delta(later, earlier) -> int:
    return month_ord(later) - month_ord(earlier)


def _ym(value):
    s = str(value or "").strip()
    return s[:7] if len(s) >= 7 else None


def _earliest_month(raw):
    if isinstance(raw, dict):
        raw = raw.get("earliest") or raw.get("date")
    return _ym(raw)


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


def _present_months(manifest, interval):
    try:
        raw = ((manifest.get("intervals") or {}).get(interval) or {}).get(
            "months") or {}
    except AttributeError:
        return []
    out = []
    for key, ent in raw.items():
        if not isinstance(ent, dict):
            continue
        if str(ent.get("status") or "present").upper() == "MISSING":
            continue
        try:
            month_ord(key)
        except (TypeError, ValueError):
            continue
        out.append(str(key)[:7])
    return sorted(set(out))


def _coverage_months(manifest, primary_interval=PRIMARY_INTERVAL,
                     fallback_intervals=FALLBACK_INTERVALS):
    for iv in (primary_interval, *tuple(fallback_intervals or ())):
        months = _present_months(manifest, iv)
        if months:
            return iv, months
    return None, []


def classify_kind_staleness(manifest, primary_interval=PRIMARY_INTERVAL):
    """Classify every established non-primary series against exact primary."""
    intervals = manifest.get("intervals") or {}
    if not isinstance(intervals, dict):
        return []
    primary = _present_months(manifest, primary_interval)
    rows = []
    for token in sorted(intervals):
        if (token == primary_interval or not isinstance(token, str)
                or not ss.INTERVAL_RE.fullmatch(token)):
            continue
        months = _present_months(manifest, token)
        if not months:
            continue
        if not primary:
            rows.append({
                "kind": token,
                "kind_last": months[-1],
                "primary_last": None,
                "lag_months": None,
                "verdict": "UNEVALUATED",
            })
            continue
        lag = month_delta(primary[-1], months[-1])
        rows.append({
            "kind": token,
            "kind_last": months[-1],
            "primary_last": primary[-1],
            "lag_months": lag,
            "verdict": (
                "KIND_FORWARD_STALE"
                if lag >= KIND_FORWARD_STALE_MONTHS else "CURRENT"),
        })
    return rows


def _identity_coverage_floor(manifest, interval=PRIMARY_INTERVAL, ticker=None):
    """Return the latest intentional identity floor for this price series."""
    boundary = ss.identity_floor(manifest, interval, ticker=ticker)
    return boundary.isoformat()[:7] if boundary is not None else None


def _median_start(starts):
    vals = sorted(starts)
    if not vals:
        return None
    # Match Claude's reference gate: for an even population choose the upper
    # middle month instead of averaging two YYYY-MM strings.
    return vals[len(vals) // 2]


def _sort_items(items):
    return sorted(items, key=lambda r: (
        r.get("ticker", ""), r.get("interval") or r.get("kind", "")))


def audit(root, primary_interval=PRIMARY_INTERVAL,
          fallback_intervals=FALLBACK_INTERVALS, write=False, asof=None):
    """Return a coverage report dict. Offline and network-free.

    ``write=True`` persists the report sidecar in the storage root. It does not
    touch manifests or data files.
    """
    root = Path(root)
    asof = asof or _dt.datetime.now().isoformat(timespec="seconds")
    earliest = {
        str(k).strip().upper(): v
        for k, v in sv.load_ibkr_earliest(root).items()
    }
    cov = {}
    errors = []
    manifest_ticker_count = 0
    kind_series_count = 0
    kind_current_count = 0
    kind_forward = []
    kind_unevaluated = []
    for tdir in _active_ticker_dirs(root):
        man = ss.load_manifest(tdir)
        if not man:
            errors.append({"ticker": tdir.name, "error": "manifest unavailable"})
            continue
        manifest_ticker_count += 1
        for row in classify_kind_staleness(man, primary_interval):
            item = {"ticker": tdir.name, **row}
            kind_series_count += 1
            if row["verdict"] == "KIND_FORWARD_STALE":
                kind_forward.append(item)
            elif row["verdict"] == "UNEVALUATED":
                kind_unevaluated.append(item)
            else:
                kind_current_count += 1
        iv, months = _coverage_months(man, primary_interval, fallback_intervals)
        if not months:
            continue
        try:
            coverage_floor = _identity_coverage_floor(
                man, iv, ticker=tdir.name)
        except ss.StorageError as exc:
            errors.append({
                "ticker": tdir.name,
                "error": f"identity floor invalid: {exc}",
            })
            continue
        cov[tdir.name] = {
            "ticker": tdir.name,
            "interval": iv,
            "stored_first": months[0],
            "stored_last": months[-1],
            "nmonths": len(months),
            "coverage_floor": coverage_floor,
        }

    starts = [v["stored_first"] for v in cov.values()]
    lasts = [v["stored_last"] for v in cov.values()]
    baseline = _median_start(starts)
    bank_latest = max(lasts) if lasts else None

    forward = []
    front = []
    unknown = []
    info = []
    summary = {}
    for ticker, row in sorted(cov.items()):
        flags = []
        months_behind = (
            month_delta(bank_latest, row["stored_last"]) if bank_latest else 0)
        if months_behind >= FORWARD_STALE_MONTHS:
            flags.append("forward-stale")
            forward.append({
                "ticker": ticker,
                "interval": row["interval"],
                "stored_last": row["stored_last"],
                "months_behind": months_behind,
            })
        source_earliest = _earliest_month(earliest.get(ticker))
        coverage_floor = row.get("coverage_floor")
        effective_earliest = source_earliest
        if (coverage_floor is not None
                and month_delta(row["stored_first"], coverage_floor) >= 0):
            if (effective_earliest is None
                    or month_ord(coverage_floor) > month_ord(effective_earliest)):
                effective_earliest = coverage_floor
        front_late = month_delta(row["stored_first"], baseline) if baseline else 0
        available_before = (
            month_delta(row["stored_first"], effective_earliest)
            if effective_earliest is not None else None)
        if effective_earliest is None:
            flags.append("unknown-earliest")
            unknown.append(ticker)
        else:
            if available_before > 0:
                info.append({
                    "ticker": ticker,
                    "interval": row["interval"],
                    "stored_first": row["stored_first"],
                    "earliest_available": source_earliest,
                    "coverage_floor": coverage_floor,
                    "effective_earliest": effective_earliest,
                    "months_available_before": available_before,
                })
            if (front_late >= FRONT_LATE_MONTHS
                    and available_before >= FRONT_AVAIL_MONTHS):
                flags.append("front-short")
                front.append({
                    "ticker": ticker,
                    "interval": row["interval"],
                    "stored_first": row["stored_first"],
                    "earliest_available": source_earliest,
                    "coverage_floor": coverage_floor,
                    "effective_earliest": effective_earliest,
                    "months_available_before": available_before,
                    "front_late_months": front_late,
                })
        summary[ticker] = {
            **row,
            "earliest_available": source_earliest,
            "effective_earliest": effective_earliest,
            "months_behind_latest": months_behind,
            "front_late_months": front_late,
            "months_available_before": available_before,
            "flags": flags,
        }

    report = {
        "kind": "coverage_audit",
        "version": REPORT_VERSION,
        "report_only": True,
        "network": False,
        "asof": asof,
        "root": str(root),
        "primary_interval": primary_interval,
        "fallback_intervals": list(fallback_intervals or ()),
        "thresholds": {
            "forward_stale_months": FORWARD_STALE_MONTHS,
            "kind_forward_stale_months": KIND_FORWARD_STALE_MONTHS,
            "front_late_months": FRONT_LATE_MONTHS,
            "front_avail_months": FRONT_AVAIL_MONTHS,
        },
        "ticker_count": len(cov),
        "manifest_ticker_count": manifest_ticker_count,
        "bank_latest": bank_latest,
        "baseline_start": baseline,
        "forward_stale": _sort_items(forward),
        "front_short": _sort_items(front),
        "unknown": sorted(unknown),
        "info": _sort_items(info),
        "kind_series_count": kind_series_count,
        "kind_current_count": kind_current_count,
        "kind_forward_stale": _sort_items(kind_forward),
        "kind_unevaluated": _sort_items(kind_unevaluated),
        "summary": summary,
        "errors": errors,
    }
    if write:
        report["report_path"] = str(Path(root) / COVERAGE_REPORT_FILE)
        write_report(root, report)
    return report


def write_report(root, report):
    target = Path(root) / COVERAGE_REPORT_FILE
    out = dict(report or {})
    out["report_path"] = str(target)
    payload = json.dumps(out, indent=2, sort_keys=True).encode("utf-8")
    ss._atomic_write_bytes(target, payload)
    return str(target)


def load_report(root):
    try:
        data = json.loads(
            (Path(root) / COVERAGE_REPORT_FILE).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def summary_counts(report):
    return {
        "forward_stale": len(report.get("forward_stale") or []),
        "front_short": len(report.get("front_short") or []),
        "unknown": len(report.get("unknown") or []),
        "kind_forward_stale": len(
            report.get("kind_forward_stale") or []),
        "kind_unevaluated": len(report.get("kind_unevaluated") or []),
        "kind_series": int(report.get("kind_series_count") or 0),
        "tickers": int(report.get("ticker_count") or 0),
    }


def format_queue(report):
    fwd = [r.get("ticker") for r in report.get("forward_stale") or []]
    front = [r.get("ticker") for r in report.get("front_short") or []]
    return [t for t in fwd + front if t]


def summarize_report(report, max_names=12):
    c = summary_counts(report)
    lines = [
        ("Coverage audit: "
         f"{c['tickers']} ticker(s), bank_latest={report.get('bank_latest')}, "
         f"baseline_start={report.get('baseline_start')}.")
    ]
    fwd = [r.get("ticker") for r in report.get("forward_stale") or []]
    front = [r.get("ticker") for r in report.get("front_short") or []]
    unknown = list(report.get("unknown") or [])

    def names(vals):
        vals = [str(v) for v in vals if v]
        if len(vals) <= max_names:
            return ", ".join(vals) if vals else "none"
        return ", ".join(vals[:max_names]) + f", ... +{len(vals) - max_names}"

    lines.append(f"Coverage: {len(fwd)} forward-stale -> {names(fwd)}")
    lines.append(f"Coverage: {len(front)} front-short -> {names(front)}")
    lines.append(f"Coverage: {len(unknown)} unknown earliest -> {names(unknown)}")
    kind_rows = report.get("kind_forward_stale") or []
    kind_labels = [
        (f"{row.get('ticker')} {row.get('kind')} "
         f"(last={row.get('kind_last')}, primary={row.get('primary_last')}, "
         f"lag={row.get('lag_months')}mo)")
        for row in kind_rows if isinstance(row, dict)
    ]
    lines.append(
        f"Per-kind staleness: {len(kind_rows)} forward-stale -> "
        f"{names(kind_labels)}")
    kind_unevaluated = report.get("kind_unevaluated") or []
    if kind_unevaluated:
        labels = [
            f"{row.get('ticker')} {row.get('kind')}"
            for row in kind_unevaluated if isinstance(row, dict)
        ]
        lines.append(
            f"Per-kind staleness: {len(kind_unevaluated)} unevaluated -> "
            f"{names(labels)}")
    queue = format_queue(report)
    if queue:
        lines.append("Coverage re-fetch queue: " + ", ".join(queue))
    rp = report.get("report_path")
    if rp:
        lines.append(f"Coverage report saved: {rp}")
    return lines


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    write = "--write" in argv
    if "--no-write" in argv:
        write = False
    root = ss.storage_root(".")
    for arg in argv:
        if not arg.startswith("-"):
            root = Path(arg)
            break
    report = audit(root, write=write)
    for line in summarize_report(report):
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
