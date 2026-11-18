"""Versioned, fingerprinted gap evidence for offline consumers.

The producer lives in ``stock_validate``. This module owns the sidecar schema,
strict currentness checks, and expected-session run summaries shared by health,
Tier 0, and export-quality reporting. It never writes or contacts a provider.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path

import stock_storage as storage


BASENAME = "_data_gaps.json"
KIND = "data_gaps"
SCHEMA_VERSION = 1
PROVENANCE_VERSION = 1
MAX_BYTES = 4 * 1024 * 1024
MAX_SERIES = 4_000
MAX_DATES = 20_000
MAX_COUNT = 100_000_000
MAX_ERROR = 320
KIND_SERIES_EVIDENCE = True


class GapEvidenceError(RuntimeError):
    """Gap evidence is missing, malformed, changing, or outside the schema."""


def is_primary_price_interval(interval):
    interval = str(interval or "").strip()
    return bool(
        storage.INTERVAL_RE.fullmatch(interval)
        and storage.kind_of(interval) == ""
        and storage.session_of(interval) == "rth"
    )


def is_gap_evidence_interval(interval):
    """True for an explicitly requested regular-session stored series.

    Discovery remains TRADES-only through ``is_primary_price_interval``.
    Explicit callers may additionally request stored kind series, but never
    pre/post session variants.
    """
    interval = str(interval or "").strip()
    return bool(
        storage.INTERVAL_RE.fullmatch(interval)
        and storage.session_of(interval) == "rth"
    )


def discover_primary_series(root, tickers=None):
    """Return regular-session TRADES series plus manifest discovery errors."""
    root = Path(root)
    wanted = None
    if tickers is not None:
        wanted = {storage.canonical_ticker(value) for value in tickers}
    rows = []
    errors = []
    try:
        entries = sorted(root.iterdir(), key=lambda path: path.name)
    except OSError as exc:
        return [], [{"state": "bank_unavailable",
                     "reason": f"{type(exc).__name__}: {exc}"}]
    for ticker_dir in entries:
        ticker = ticker_dir.name
        if (not ticker_dir.is_dir() or ticker.startswith("_")
                or not storage.TICKER_DIR_RE.fullmatch(ticker)
                or (wanted is not None and ticker not in wanted)):
            continue
        manifest = storage.load_manifest(ticker_dir)
        if manifest is None:
            errors.append({
                "ticker": ticker,
                "state": "manifest_unavailable",
                "reason": "manifest is missing or malformed",
            })
            continue
        for interval in sorted((manifest.get("intervals") or {})):
            if is_primary_price_interval(interval):
                rows.append((ticker, interval))
    return sorted(set(rows)), errors


def capture_interval(root, ticker, interval, fingerprint_fn=None):
    fn = fingerprint_fn or storage.interval_state_fingerprint
    try:
        value = fn(root, ticker, interval)
        if (not isinstance(value, dict)
                or value.get("schema_version")
                != storage.INTERVAL_FINGERPRINT_VERSION
                or value.get("algorithm") != "sha256"
                or value.get("ticker") != storage.canonical_ticker(ticker)
                or value.get("interval") != interval
                or value.get("present") is not True
                or not isinstance(value.get("backfill_incomplete"), bool)
                or not _valid_sha(value.get("sha256"))):
            raise ValueError("fingerprint helper returned an invalid contract")
        return value, None
    except Exception as exc:  # noqa: BLE001 - evidence failure is report data
        return None, _bounded(f"{type(exc).__name__}: {exc}")


def build_provenance(before, before_error, after, after_error,
                     operation_error=None):
    reasons = []
    if before_error:
        reasons.append(f"before: {before_error}")
    if after_error:
        reasons.append(f"after: {after_error}")
    if operation_error:
        reasons.append(f"scan: {_bounded(operation_error)}")
    before_sha = before.get("sha256") if isinstance(before, dict) else None
    after_sha = after.get("sha256") if isinstance(after, dict) else None
    if not reasons and before_sha != after_sha:
        reasons.append("interval changed during gap scan")
    current = not reasons and _valid_sha(before_sha) and before_sha == after_sha
    out = {
        "schema_version": PROVENANCE_VERSION,
        "algorithm": "sha256",
        "before_sha256": before_sha,
        "after_sha256": after_sha,
        "current": bool(current),
        "month_count": (
            after.get("month_count") if isinstance(after, dict) else None),
        "verified_absent_count": (
            after.get("verified_absent_count")
            if isinstance(after, dict) else None),
    }
    if reasons:
        out["error"] = _bounded("; ".join(reasons))
    return out


def expected_session_runs(days, calendar_days):
    """Compress missing dates into consecutive expected-session runs."""
    missing = sorted(set(str(value) for value in (days or [])))
    if not missing:
        return []
    ordered_calendar = sorted({
        value.isoformat() if isinstance(value, date) else str(value)
        for value in (calendar_days or [])
    })
    positions = {value: idx for idx, value in enumerate(ordered_calendar)}
    runs = []
    current = []
    previous = None
    for value in missing:
        position = positions.get(value)
        consecutive = (
            current and position is not None and previous is not None
            and position == previous + 1
        )
        if current and not consecutive:
            runs.append(_run_row(current))
            current = []
        current.append(value)
        previous = position
    if current:
        runs.append(_run_row(current))
    return runs


def entry_from_scan(scan, calendar_days=None):
    scan = scan if isinstance(scan, dict) else {}
    missing_days = sorted(set(str(value) for value in
                              (scan.get("missing_days") or [])))
    source_absent = sorted(set(str(value) for value in
                               (scan.get("source_absent") or [])))
    runs = expected_session_runs(missing_days, calendar_days)
    largest = max(runs, key=lambda row: row["count"], default={
        "count": 0, "start": None, "end": None,
    })
    out = {
        "missing_total": _scan_count(scan.get("missing_total")),
        "gap_events": _scan_count(scan.get("gap_count")),
        "days": len(set(str(value) for value in
                        (scan.get("days_with_gaps") or []))),
        "missing_days": len(missing_days),
        "missing_day_list": missing_days,
        "missing_day_runs": runs,
        "largest_missing_day_run": dict(largest),
        "source_absent": len(source_absent),
        "source_absent_list": source_absent,
        "interval_fingerprint": dict(
            scan.get("interval_fingerprint") or {}),
    }
    if scan.get("error"):
        out["scan_error"] = _bounded(scan.get("error"))
    return out


def payload(summary, asof):
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "asof": str(asof),
        "series": dict(summary or {}),
    }


def load_payload(root):
    root = Path(root).resolve()
    path = (root / BASENAME).resolve()
    if path.parent != root:
        raise GapEvidenceError("gap sidecar escapes the storage root")
    try:
        raw = storage._read_stable_manifest_bytes(path)
    except Exception as exc:  # noqa: BLE001 - normalize storage errors
        raise GapEvidenceError(
            f"cannot read {BASENAME}: {type(exc).__name__}: {exc}") from exc
    if len(raw) > MAX_BYTES:
        raise GapEvidenceError(f"{BASENAME} exceeds the evidence size limit")
    try:
        data = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=storage._json_object_without_duplicate_keys)
    except (UnicodeError, ValueError) as exc:
        raise GapEvidenceError(f"{BASENAME} is malformed: {exc}") from None
    if not isinstance(data, dict):
        raise GapEvidenceError(f"{BASENAME} root is not an object")
    source = {
        "basename": BASENAME,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }
    return data, source


def evaluate(root, series=None, fingerprint_fn=None):
    discovery_errors = []
    if series is None:
        series, discovery_errors = discover_primary_series(root)
    expected = _normalize_series(series)
    try:
        data, source = load_payload(root)
        return evaluate_payload(
            root, data, expected, fingerprint_fn=fingerprint_fn,
            source=source, discovery_errors=discovery_errors)
    except GapEvidenceError as exc:
        unavailable = [
            _unavailable(ticker, interval, "source_unavailable", str(exc))
            for ticker, interval in expected
        ]
        return _evaluation(
            expected, [], unavailable, None, None,
            [*discovery_errors, {"state": "source_unavailable",
                                 "reason": str(exc)}])


def evaluate_payload(root, data, series, fingerprint_fn=None, source=None,
                     discovery_errors=None):
    expected = _normalize_series(series)
    errors = list(discovery_errors or [])
    try:
        asof, entries = _validate_payload_header(data)
    except GapEvidenceError as exc:
        unavailable = [
            _unavailable(ticker, interval, "source_invalid", str(exc))
            for ticker, interval in expected
        ]
        return _evaluation(
            expected, [], unavailable, None, source,
            [*errors, {"state": "source_invalid", "reason": str(exc)}])

    current_rows = []
    unavailable = []
    for ticker, interval in expected:
        key = f"{ticker} {interval}"
        value = entries.get(key)
        if value is None:
            unavailable.append(_unavailable(
                ticker, interval, "missing", "gap evidence entry is missing"))
            continue
        if (isinstance(value, dict)
                and "interval_fingerprint" not in value):
            unavailable.append(_unavailable(
                ticker, interval, "legacy",
                "gap evidence entry predates interval fingerprints and has not been rescanned"))
            continue
        try:
            row = _validate_entry(ticker, interval, value)
        except GapEvidenceError as exc:
            unavailable.append(_unavailable(
                ticker, interval, "malformed", str(exc)))
            continue
        provenance = row["interval_fingerprint"]
        if provenance.get("current") is not True:
            unavailable.append(_unavailable(
                ticker, interval, "noncurrent",
                provenance.get("error") or "gap scan was not current"))
            continue
        observed, observed_error = capture_interval(
            root, ticker, interval, fingerprint_fn=fingerprint_fn)
        if observed_error:
            unavailable.append(_unavailable(
                ticker, interval, "unreadable", observed_error))
            continue
        if observed["sha256"] != provenance["after_sha256"]:
            unavailable.append(_unavailable(
                ticker, interval, "stale",
                "stored interval changed after the gap scan"))
            continue
        row.update({
            "ticker": ticker,
            "interval": interval,
            "asof": asof,
            "current": True,
        })
        current_rows.append(row)
    return _evaluation(
        expected, current_rows, unavailable, asof, source, errors)


def _validate_payload_header(data):
    if not isinstance(data, dict):
        raise GapEvidenceError("gap evidence root is not an object")
    if data.get("kind") != KIND or data.get("schema_version") != SCHEMA_VERSION:
        raise GapEvidenceError("gap evidence schema is legacy or unsupported")
    asof = data.get("asof")
    if not isinstance(asof, str) or not asof or len(asof) > 64:
        raise GapEvidenceError("gap evidence as-of value is invalid")
    try:
        datetime.fromisoformat(asof)
    except ValueError:
        raise GapEvidenceError("gap evidence as-of value is invalid") from None
    entries = data.get("series")
    if not isinstance(entries, dict) or len(entries) > MAX_SERIES:
        raise GapEvidenceError("gap evidence series map is invalid or too large")
    return asof, entries


def _validate_entry(ticker, interval, value):
    label = f"{ticker} {interval}"
    if not isinstance(value, dict):
        raise GapEvidenceError(f"{label} entry is not an object")
    row = {}
    for field in ("missing_total", "gap_events", "days", "missing_days",
                  "source_absent"):
        row[field] = _evidence_count(value.get(field), f"{label} {field}")
    for field, count_field in (
            ("missing_day_list", "missing_days"),
            ("source_absent_list", "source_absent")):
        values = _date_list(value.get(field), f"{label} {field}")
        if len(values) != row[count_field]:
            raise GapEvidenceError(
                f"{label} {field} does not match {count_field}")
        row[field] = values
    runs = value.get("missing_day_runs")
    if not isinstance(runs, list) or len(runs) > MAX_DATES:
        raise GapEvidenceError(f"{label} missing-day runs are invalid")
    normalized_runs = [_validate_run(item, label) for item in runs]
    if sum(item["count"] for item in normalized_runs) != row["missing_days"]:
        raise GapEvidenceError(f"{label} missing-day runs do not cover the list")
    cursor = 0
    previous_end = None
    for item in normalized_runs:
        members = row["missing_day_list"][cursor:cursor + item["count"]]
        if (len(members) != item["count"] or not members
                or members[0] != item["start"]
                or members[-1] != item["end"]
                or any(not item["start"] <= day <= item["end"]
                       for day in members)
                or (previous_end is not None and item["start"] <= previous_end)):
            raise GapEvidenceError(
                f"{label} missing-day run bounds do not match the list")
        cursor += item["count"]
        previous_end = item["end"]
    if cursor != len(row["missing_day_list"]):
        raise GapEvidenceError(f"{label} missing-day runs do not partition the list")
    largest = value.get("largest_missing_day_run")
    largest = _validate_run(largest, label, allow_empty=True)
    expected_largest = max(
        normalized_runs, key=lambda item: item["count"],
        default={"count": 0, "start": None, "end": None})
    if largest != expected_largest:
        raise GapEvidenceError(f"{label} largest missing-day run is inconsistent")
    row["missing_day_runs"] = normalized_runs
    row["largest_missing_day_run"] = largest
    provenance = value.get("interval_fingerprint")
    row["interval_fingerprint"] = _validate_provenance(provenance, label)
    if value.get("scan_error") is not None:
        scan_error = value.get("scan_error")
        if not isinstance(scan_error, str) or not scan_error or len(scan_error) > MAX_ERROR:
            raise GapEvidenceError(f"{label} scan error is invalid")
        row["scan_error"] = scan_error
        if row["interval_fingerprint"].get("current") is True:
            raise GapEvidenceError(f"{label} failed scan cannot be current")
    return row


def _validate_provenance(value, label):
    if (not isinstance(value, dict)
            or value.get("schema_version") != PROVENANCE_VERSION
            or value.get("algorithm") != "sha256"
            or not isinstance(value.get("current"), bool)):
        raise GapEvidenceError(f"{label} interval provenance is invalid")
    before = value.get("before_sha256")
    after = value.get("after_sha256")
    if before is not None and not _valid_sha(before):
        raise GapEvidenceError(f"{label} before fingerprint is invalid")
    if after is not None and not _valid_sha(after):
        raise GapEvidenceError(f"{label} after fingerprint is invalid")
    current = value["current"]
    error = value.get("error")
    counts = {}
    for field in ("month_count", "verified_absent_count"):
        count = value.get(field)
        if (count is not None
                and (isinstance(count, bool) or not isinstance(count, int)
                     or not 0 <= count <= MAX_COUNT)):
            raise GapEvidenceError(f"{label} {field} is invalid")
        counts[field] = count
    if current and (before != after or not _valid_sha(after) or error is not None):
        raise GapEvidenceError(f"{label} current provenance is inconsistent")
    if current and any(count is None for count in counts.values()):
        raise GapEvidenceError(f"{label} current provenance counts are missing")
    if not current and (not isinstance(error, str) or not error
                        or len(error) > MAX_ERROR):
        raise GapEvidenceError(f"{label} non-current provenance lacks a reason")
    out = {
        "schema_version": PROVENANCE_VERSION,
        "algorithm": "sha256",
        "before_sha256": before,
        "after_sha256": after,
        "current": current,
        **counts,
    }
    if error is not None:
        out["error"] = error
    return out


def _evaluation(expected, rows, unavailable, asof, source, errors):
    rows = sorted(rows, key=lambda row: (row["ticker"], row["interval"]))
    unavailable = sorted(
        unavailable, key=lambda row: (row.get("ticker", ""),
                                      row.get("interval", "")))
    fillable = [row for row in rows if row["missing_days"]]
    source_absent = [row for row in rows if row["source_absent"]]
    interior = [row for row in rows if row["missing_total"]]
    source_out = dict(source or {}) if source else None
    if source_out is not None:
        source_out.update({
            "kind": KIND,
            "schema_version": SCHEMA_VERSION,
            "asof": asof,
            "requested_series": len(expected),
            "current_series": len(rows),
            "unavailable_series": len(unavailable),
        })
    return {
        "kind": "gap_evidence_evaluation",
        "schema_version": SCHEMA_VERSION,
        "report_only": True,
        "network": False,
        "asof": asof,
        "expected_series": len(expected),
        "current_series": len(rows),
        "fillable": fillable,
        "source_absent": source_absent,
        "interior": interior,
        "rows": rows,
        "unavailable": unavailable,
        "errors": list(errors or []),
        "source": source_out,
    }


def _normalize_series(series):
    out = []
    for ticker, interval in series or []:
        ticker = storage.canonical_ticker(ticker)
        interval = str(interval or "").strip()
        if not is_gap_evidence_interval(interval):
            continue
        out.append((ticker, interval))
    out = sorted(set(out))
    if len(out) > MAX_SERIES:
        raise GapEvidenceError("too many requested gap-evidence series")
    return out


def _date_list(value, label):
    if not isinstance(value, list) or len(value) > MAX_DATES:
        raise GapEvidenceError(f"{label} is invalid or too large")
    out = []
    for item in value:
        if not isinstance(item, str):
            raise GapEvidenceError(f"{label} contains a non-string date")
        try:
            parsed = date.fromisoformat(item)
        except ValueError:
            raise GapEvidenceError(f"{label} contains an invalid date") from None
        if parsed.isoformat() != item:
            raise GapEvidenceError(f"{label} contains a noncanonical date")
        out.append(item)
    if out != sorted(set(out)):
        raise GapEvidenceError(f"{label} must be sorted and unique")
    return out


def _validate_run(value, label, allow_empty=False):
    if not isinstance(value, dict):
        raise GapEvidenceError(f"{label} missing-day run is invalid")
    count = _evidence_count(value.get("count"), f"{label} run count")
    start, end = value.get("start"), value.get("end")
    if count == 0 and allow_empty and start is None and end is None:
        return {"count": 0, "start": None, "end": None}
    bounds = []
    for item in (start, end):
        if not isinstance(item, str):
            raise GapEvidenceError(f"{label} missing-day run bounds are invalid")
        try:
            parsed = date.fromisoformat(item)
        except ValueError:
            raise GapEvidenceError(
                f"{label} missing-day run bounds are invalid") from None
        if parsed.isoformat() != item:
            raise GapEvidenceError(f"{label} missing-day run bounds are invalid")
        bounds.append(item)
    if not count or bounds[0] > bounds[1]:
        raise GapEvidenceError(f"{label} missing-day run bounds are invalid")
    return {"count": count, "start": bounds[0], "end": bounds[1]}


def _run_row(values):
    return {"count": len(values), "start": values[0], "end": values[-1]}


def _unavailable(ticker, interval, state, reason):
    return {
        "ticker": ticker,
        "interval": interval,
        "state": state,
        "reason": _bounded(reason),
    }


def _scan_count(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return 0
    return min(value, MAX_COUNT)


def _evidence_count(value, label):
    if (isinstance(value, bool) or not isinstance(value, int)
            or not 0 <= value <= MAX_COUNT):
        raise GapEvidenceError(f"{label} is invalid")
    return value


def _valid_sha(value):
    return (isinstance(value, str) and len(value) == 64
            and all(char in "0123456789abcdef" for char in value))


def _bounded(value):
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    return text[:MAX_ERROR] or "gap evidence unavailable"


__all__ = [
    "BASENAME",
    "GapEvidenceError",
    "KIND",
    "PROVENANCE_VERSION",
    "SCHEMA_VERSION",
    "build_provenance",
    "capture_interval",
    "discover_primary_series",
    "entry_from_scan",
    "evaluate",
    "evaluate_payload",
    "expected_session_runs",
    "is_gap_evidence_interval",
    "is_primary_price_interval",
    "load_payload",
    "payload",
]
