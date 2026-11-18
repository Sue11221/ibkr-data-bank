"""Read-only OHLC-vs-derived-kind coverage audit (Row 57).

The detector reports ratio-kind months that are present while the matching
base OHLC month is absent *inside* established base coverage.  Leading and
trailing kind-only months are intentionally ignored as ordinary frontier
skew.  This module never fetches data, changes a manifest, or gates a write.
Its only optional mutation is the derived queue sidecar at the storage root.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import stock_storage as storage


QUEUE_BASENAME = "_ohlc_kind_coverage_queue.json"
REPORT_KIND = "ohlc_kind_coverage_audit"
QUEUE_KIND = "ohlc_kind_coverage_queue"
VERSION = 1

_MONTH_RE = re.compile(r"^[12]\d{3}-(?:0[1-9]|1[0-2])$")


class CoverageAuditError(RuntimeError):
    """The requested audit input or derived sidecar is unsafe to use."""


def _intervals(manifest):
    if not isinstance(manifest, dict):
        raise CoverageAuditError("manifest root is not an object")
    intervals = manifest.get("intervals")
    if not isinstance(intervals, dict):
        raise CoverageAuditError("manifest intervals are unavailable")
    return intervals


def _present_months(intervals, token):
    interval = intervals.get(token)
    if not isinstance(interval, dict):
        raise CoverageAuditError(f"{token}: interval record is malformed")
    months = interval.get("months")
    if not isinstance(months, dict):
        raise CoverageAuditError(f"{token}: month map is malformed")

    present = set()
    for month, entry in months.items():
        if not isinstance(month, str) or _MONTH_RE.fullmatch(month) is None:
            raise CoverageAuditError(f"{token}: month key is malformed")
        if not isinstance(entry, dict):
            raise CoverageAuditError(f"{token} {month}: entry is malformed")
        if entry.get("status") == "present":
            present.add(month)
    return present


def coverage_rows(manifest):
    """Return deterministic interior kind-without-base rows for one manifest."""
    intervals = _intervals(manifest)
    rows = []
    for token in sorted(intervals):
        if not isinstance(token, str):
            raise CoverageAuditError("manifest interval token is malformed")
        if storage.INTERVAL_RE.fullmatch(token) is None:
            raise CoverageAuditError(f"invalid interval token: {token!r}")
        if storage.kind_of(token) not in storage.RATIO_KINDS:
            continue

        base = storage.base_interval(token)
        if base not in intervals:
            continue
        kind_present = _present_months(intervals, token)
        base_present = _present_months(intervals, base)
        if not base_present:
            continue

        first_base = min(base_present)
        last_base = max(base_present)
        interior = sorted(
            month for month in kind_present - base_present
            if first_base < month < last_base
        )
        if not interior:
            continue
        rows.append({
            "kind_token": token,
            "base": base,
            "interior_count": len(interior),
            "first_month": interior[0],
            "last_month": interior[-1],
            "interior_months": interior,
        })
    return rows


def _normalize_tickers(tickers):
    if tickers is None:
        return None
    if isinstance(tickers, str):
        raw = [tickers]
    else:
        try:
            raw = list(tickers)
        except TypeError as exc:
            raise CoverageAuditError("tickers must be iterable") from exc
    normalized = set()
    for ticker in raw:
        try:
            normalized.add(storage.canonical_ticker(ticker))
        except Exception as exc:  # normalize the storage validation boundary
            raise CoverageAuditError(f"invalid ticker: {ticker!r}") from exc
    return sorted(normalized)


def _ticker_paths(root, tickers):
    requested = _normalize_tickers(tickers)
    if requested is not None:
        return [(ticker, root / ticker) for ticker in requested]

    try:
        entries = sorted(root.iterdir(), key=lambda path: path.name)
    except OSError as exc:
        raise CoverageAuditError(f"cannot enumerate storage root: {root}") \
            from exc
    return [
        (path.name, path)
        for path in entries
        if (not path.name.startswith("_")
            and storage.TICKER_DIR_RE.fullmatch(path.name)
            and path.is_dir())
    ]


def _queue_rows(flagged):
    rows = []
    for item in flagged:
        ticker = item["ticker"]
        for row in item["rows"]:
            rows.append({"ticker": ticker, **row})
    return rows


def _write_queue(root, *, complete, scanned, flagged):
    target = root / QUEUE_BASENAME
    if target.is_symlink() or target.is_dir():
        raise CoverageAuditError(
            f"refusing unsafe queue target: {target.name}")
    payload = {
        "kind": QUEUE_KIND,
        "version": VERSION,
        "complete": bool(complete),
        "scanned": int(scanned),
        "rows": _queue_rows(flagged),
    }
    try:
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False,
        ).encode("utf-8") + b"\n"
        storage._atomic_write_bytes(target, encoded)
    except CoverageAuditError:
        raise
    except Exception as exc:  # normalize the optional sidecar write boundary
        raise CoverageAuditError(
            f"cannot write queue sidecar: {type(exc).__name__}") from exc
    return str(target)


def audit(root, tickers=None, *, write_queue=True):
    """Scan stored manifests and optionally publish the derived flag queue."""
    root = Path(root).resolve()
    if not root.is_dir():
        raise CoverageAuditError(f"storage root is not a directory: {root}")

    complete = True
    scanned = 0
    flagged = []
    for ticker, ticker_dir in _ticker_paths(root, tickers):
        if (ticker_dir.is_symlink() or not ticker_dir.is_dir()
                or ticker_dir.resolve(strict=False).parent != root):
            complete = False
            continue
        scanned += 1
        try:
            manifest = storage.load_manifest(ticker_dir)
            if manifest is None:
                raise CoverageAuditError("manifest is unavailable")
            rows = coverage_rows(manifest)
        except (CoverageAuditError, OSError, TypeError, ValueError):
            complete = False
            continue
        if rows:
            flagged.append({"ticker": ticker, "rows": rows})

    report = {
        "kind": REPORT_KIND,
        "version": VERSION,
        "complete": complete,
        "scanned": scanned,
        "flagged": flagged,
        "queue_path": None,
    }
    if write_queue:
        report["queue_path"] = _write_queue(
            root, complete=complete, scanned=scanned, flagged=flagged)
    return report


__all__ = [
    "CoverageAuditError",
    "QUEUE_BASENAME",
    "audit",
    "coverage_rows",
]
