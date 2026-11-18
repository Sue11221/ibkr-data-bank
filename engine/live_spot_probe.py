"""Report-only Live Spot Check for minute days and opt-in daily spans.

Offline planning remains the default surface. The explicitly authorized live
path uses one paced RTH request per selected minute day or daily span under the
operation gate, revalidates its stored evidence after the request, and writes
only a bounded Run Logs artifact. The embedded path uses caller-owned
lifecycle/gate/artifact boundaries and returns one public row. Nothing in this
module writes the stock bank.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import importlib
import json
import math
import os
import re
import sys
import threading
import uuid
from collections import deque
from itertools import islice
from pathlib import Path
from statistics import median
from types import SimpleNamespace

import operation_gate
import stock_basis as basis
import stock_storage as storage
import stock_ibkr as sk
import fetch_ibkr_bridge as fib
import fetch_operations as fops
import fetch_operation_report as freport
from fetch_authority import AuthorityError, CalendarUnsupported, strict_json
from fetch_ledger import LedgerError, inspect_ledger
from fetch_run_context import RequestCancelled, RequestRefused, normalize_rows, row_evidence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STORAGE_ROOT = storage.storage_root(PROJECT_ROOT)
RUN_LOGS_ROOT = PROJECT_ROOT / "Run Logs"

REPORT_VERSION = 1
DEFAULT_COUNT = 10
MAX_COUNT = 100
MAX_DAILY_COUNT = DEFAULT_COUNT
MAX_CANDIDATES = 10_000
MAX_SELECTION_ERRORS = 100
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_ARTIFACT_BYTES = 4 * 1024 * 1024
MAX_BASIS_ACTIONS = 256
MAX_CORRECTIONS = 256
MAX_RECONCILE_PLAN = 500_000
MAX_RECONCILE_RESULTS = 4_096
MAX_COMMIT_EVIDENCE_BYTES = 24 * 1024
MAX_MONTHS = 2_400

DAILY_SPAN_DAYS = 365
DAILY_REQUEST_DURATION = "1 Y"
DAILY_BAR_SIZE = "1 day"
DAYLEVEL_SPOT_CHECK = True
VOLKIND_SPOT_CHECK = True

PRICE_TOL = 0.005
FACTOR_SPREAD_TOL = 0.02
VOL_TOL = 0.0001
VOL_SPREAD_TOL = 0.0001
SCATTER_MIN_BARS = 3
COVERAGE_RATIO_MIN = 0.5

VERDICTS = (
    "NO_LIVE_DATA",
    "COVERAGE_MISMATCH",
    "MATCH",
    "DOCUMENTED_DIVERGENCE",
    "BASIS_STEP",
    "SCATTERED_MISMATCH",
)
UNIFORM_RECOMPUTE = "UNIFORM_RECOMPUTE"
VOL_VERDICTS = VERDICTS + (UNIFORM_RECOMPUTE,)
PROBE_ERROR = "PROBE_ERROR"

_SHA_RE = re.compile(r"[0-9a-f]{64}")
_MONTH_RE = re.compile(r"\d{4}-(0[1-9]|1[0-2])")
_OWNER_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,39}")
_LCG_MASK = 0x7FFFFFFF
_PROBE_INTERVALS = ("1m", "1d", "1d-iv", "1d-hvol")
_ATOMIC_RESULT_FIELDS = frozenset({
    "commit_state", "rollback_verified", "bank_write_completed",
    "bank_written", "correction_recorded", "commit_evidence",
    "bank_committed",
})
_ATOMIC_REQUIRED_FIELDS = _ATOMIC_RESULT_FIELDS - {"bank_committed"}
_COMMIT_EVIDENCE_FIELDS = frozenset({
    "rollback_verified", "month_write_completed", "bank_written",
    "correction_recorded", "month_state", "manifest_state",
    "original_month_file", "written_month_file", "old_month_sha256",
    "new_month_sha256", "observed_original_sha256",
    "observed_written_sha256", "correction", "restore_errors",
})
_CORRECTION_FIELDS = frozenset({
    "version", "type", "day", "run", "source", "confirmed",
    "what_to_show", "request_count", "bars", "old_value", "new_value",
    "old_day_sha256", "new_day_sha256", "old_month_sha256",
    "new_month_sha256", "reason", "reasons",
})
_MONTH_STATES = frozenset({
    "original", "corrected", "corrected-active-original-retained",
    "indeterminate",
})
_MANIFEST_STATES = frozenset({
    "original", "unreadable", "corrected", "other-valid", "malformed",
})
_VOL_ANOMALY_REASONS = frozenset({
    "hard_nonfinite", "hard_negative", "hard_ceiling", "hard_ohlc",
    "hard_volume", "unit_flip_month", "jump_up", "jump_down",
    "iv_zero_run", "iv_hvol_band",
})
_VOL_WHAT_TO_SHOW = frozenset({
    "OPTION_IMPLIED_VOLATILITY", "HISTORICAL_VOLATILITY",
})
_RECONCILE_QUEUE_BASE_FIELDS = frozenset({
    "ticker", "kind", "kind_token", "day", "value", "reason", "reasons",
})
_RECONCILE_QUEUE_CHANGED_FIELDS = frozenset({
    "settled_value_changed", "settled_value",
})


class LiveSpotProbeError(RuntimeError):
    pass


class EvidenceError(LiveSpotProbeError):
    pass


class LiveFetchError(LiveSpotProbeError):
    pass


def _sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def _canonical_json(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _utc_timestamp(value=None):
    if value is None:
        current = dt.datetime.now(dt.timezone.utc)
    elif isinstance(value, dt.datetime):
        current = value
    else:
        raise LiveSpotProbeError("invalid report clock")
    if current.tzinfo is None:
        raise LiveSpotProbeError("report clock must include a timezone")
    return current.astimezone(dt.timezone.utc).isoformat(timespec="seconds")


def _date(value, label):
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError) as exc:
        raise EvidenceError(f"invalid {label}") from exc


def _positive_factor(value, label):
    if isinstance(value, bool):
        raise EvidenceError(f"invalid {label}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceError(f"invalid {label}") from exc
    if not math.isfinite(number) or number <= 0:
        raise EvidenceError(f"invalid {label}")
    return number


def _count(value):
    if isinstance(value, bool):
        raise LiveSpotProbeError("count must be an integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise LiveSpotProbeError("count must be an integer") from exc
    if number < 1 or number > MAX_COUNT:
        raise LiveSpotProbeError(f"count must be between 1 and {MAX_COUNT}")
    return number


def _interval_count(value, interval):
    number = _count(value)
    if _is_daily_probe(interval) and number > MAX_DAILY_COUNT:
        raise LiveSpotProbeError(
            f"daily probe count must be at most {MAX_DAILY_COUNT}")
    return number


def _port(value):
    if isinstance(value, bool):
        raise LiveSpotProbeError("port must be an integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise LiveSpotProbeError("port must be an integer") from exc
    if number < 1 or number > 65_535:
        raise LiveSpotProbeError("port must be between 1 and 65535")
    return number


def _nonnegative_number(value, label):
    if isinstance(value, bool):
        raise EvidenceError(f"invalid {label}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceError(f"invalid {label}") from exc
    if not math.isfinite(number) or number < 0:
        raise EvidenceError(f"invalid {label}")
    return number


def _nonnegative_int(value, label):
    if isinstance(value, bool):
        raise EvidenceError(f"invalid {label}")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceError(f"invalid {label}") from exc
    if number < 0:
        raise EvidenceError(f"invalid {label}")
    return number


def normalize_seed(value):
    if isinstance(value, bool):
        raise LiveSpotProbeError("seed must be an integer")
    try:
        seed = int(value)
    except (TypeError, ValueError) as exc:
        raise LiveSpotProbeError("seed must be an integer") from exc
    if seed < 0 or seed > _LCG_MASK:
        raise LiveSpotProbeError(
            f"seed must be between 0 and {_LCG_MASK}")
    return seed


def default_seed(now=None):
    stamp = _utc_timestamp(now)
    return int(stamp[:10].replace("-", ""))


def _lcg(seed):
    return (int(seed) * 1103515245 + 12345) & _LCG_MASK


def pick_probe(months, seed):
    """Return the reference-harness deterministic month and next state."""
    if not months:
        raise ValueError("no stored months to probe")
    ordered = sorted(months)
    first = _lcg(seed)
    month = ordered[first % len(ordered)]
    return month, _lcg(first)


def _price_map(value, label, *, allow_zero=False):
    if not isinstance(value, dict):
        raise EvidenceError(f"{label} bars must be an object")
    out = {}
    for key, raw in value.items():
        token = str(key)
        price = (_nonnegative_number(raw, f"{label} close")
                 if allow_zero
                 else _positive_factor(raw, f"{label} close"))
        if token in out:
            raise EvidenceError(f"duplicate {label} minute")
        out[token] = price
    return out


def _exceeds_absolute_tolerance(value, tolerance):
    """Return true only when a finite magnitude is robustly above a bound."""
    magnitude = abs(float(value))
    bound = float(tolerance)
    return (magnitude > bound
            and not math.isclose(
                magnitude, bound, rel_tol=1e-12, abs_tol=1e-15))


def classify_day(stored, live, *, ledger_factor=1.0,
                 correction_factor=None, price_tol=PRICE_TOL,
                 spread_tol=FACTOR_SPREAD_TOL, vol_mode=False):
    """Classify one stored/live close map using the proven WS8 ordering."""
    vol_mode = bool(vol_mode)
    stored = _price_map(stored, "stored", allow_zero=vol_mode)
    live = _price_map(live, "live", allow_zero=vol_mode)
    ledger_factor = _positive_factor(ledger_factor, "ledger factor")
    if correction_factor is not None:
        correction_factor = _positive_factor(
            correction_factor, "correction factor")
    if not live:
        return {"verdict": "NO_LIVE_DATA", "shared": 0,
                "stored_bars": len(stored), "live_bars": 0,
                "off_bar_count": None}

    shared = sorted(set(stored) & set(live))
    out = {
        "stored_bars": len(stored),
        "live_bars": len(live),
        "shared": len(shared),
        "off_bar_count": None,
    }
    if (len(shared) < COVERAGE_RATIO_MIN * len(stored)
            or len(shared) < COVERAGE_RATIO_MIN * len(live)):
        out["verdict"] = "COVERAGE_MISMATCH"
        return out

    if not shared:
        out["verdict"] = "COVERAGE_MISMATCH"
        return out
    if vol_mode:
        if len(shared) < SCATTER_MIN_BARS:
            out["verdict"] = "COVERAGE_MISMATCH"
            return out
        deltas = [live[key] - stored[key] for key in shared]
        middle = median(deltas)
        spread = max(abs(delta - middle) for delta in deltas)
        out.update({
            "median_delta": middle,
            "max_delta_spread": spread,
            "comparison_mode": "absolute_volatility",
            "vol_tolerance": VOL_TOL,
            "vol_spread_tolerance": VOL_SPREAD_TOL,
        })
        off_absolute = sum(
            1 for delta in deltas
            if _exceeds_absolute_tolerance(delta, VOL_TOL))
        out["off_bar_count"] = off_absolute
        sparse_majority = off_absolute * 2 > len(shared)
        if off_absolute < SCATTER_MIN_BARS and not sparse_majority:
            out["verdict"] = "MATCH"
            return out
        if not _exceeds_absolute_tolerance(spread, VOL_SPREAD_TOL):
            out["verdict"] = UNIFORM_RECOMPUTE
            return out
        out["verdict"] = "SCATTERED_MISMATCH"
        return out

    ratios = [live[key] / stored[key] for key in shared]
    middle = median(ratios)
    spread = max(abs(ratio / middle - 1) for ratio in ratios)
    out.update({"median_factor": middle, "max_spread": spread})

    off_ledger = sum(
        1 for ratio in ratios
        if abs(ratio / ledger_factor - 1) > price_tol)
    out["off_bar_count"] = off_ledger
    if off_ledger < SCATTER_MIN_BARS:
        out["verdict"] = "MATCH"
        return out

    if correction_factor is not None:
        # A later legitimate basis action still applies to corrected history.
        expected = ledger_factor / correction_factor
        off_correction = sum(
            1 for ratio in ratios
            if abs(ratio / expected - 1) > price_tol)
        out["correction_off_bar_count"] = off_correction
        if off_correction < SCATTER_MIN_BARS:
            out["verdict"] = "DOCUMENTED_DIVERGENCE"
            out["off_bar_count"] = off_correction
            return out

    if spread <= spread_tol:
        out["verdict"] = "BASIS_STEP"
        return out
    out["verdict"] = "SCATTERED_MISMATCH"
    return out


def classify_probe(stored, live, *, day, ledger_factor=1.0,
                   correction=None, verified_absent=(), vol_mode=False):
    """Add correction and source-absence safety routing to classify_day()."""
    day_token = _date(day, "probe day").isoformat()
    if correction is not None and not isinstance(correction, dict):
        raise EvidenceError("correction evidence must be an object")
    correction = dict(correction) if correction is not None else None
    mode = correction.get("mode") if correction else None
    if correction is not None:
        if mode not in ("expected_divergence", "expected_absent"):
            raise EvidenceError("correction evidence has an invalid mode")
        records = correction.get("records")
        if not isinstance(records, list) or not records:
            raise EvidenceError("correction evidence has no source records")
        if mode == "expected_divergence":
            correction["factor"] = _positive_factor(
                correction.get("factor"), "correction factor")
    correction_factor = (
        correction.get("factor") if mode == "expected_divergence" else None)
    result = classify_day(
        stored, live, ledger_factor=ledger_factor,
        correction_factor=correction_factor, vol_mode=vol_mode)
    result.update({
        "day": day_token,
        "ledger_factor": _positive_factor(ledger_factor, "ledger factor"),
        "correction": correction,
        "source_absent": False,
        "pass_equivalent": False,
        "needs_human": False,
        "safety_status": "REVIEW",
        "remedy": None,
    })

    verdict = result["verdict"]
    if mode == "expected_absent":
        result.update({
            "safety_status": "REGRESSION",
            "needs_human": True,
            "remedy": "correction_regression_review",
        })
        return result

    if verdict == "NO_LIVE_DATA":
        absent = day_token in {
            _date(value, "verified-absent day").isoformat()
            for value in verified_absent
        }
        result.update({
            "source_absent": absent,
            "pass_equivalent": absent,
            "safety_status": (
                "EXPECTED_SOURCE_ABSENCE" if absent else "COVERAGE_QUESTION"),
            "needs_human": not absent,
            "remedy": None if absent else "coverage_review",
        })
        return result

    if (mode == "expected_divergence"
            and verdict == "DOCUMENTED_DIVERGENCE"):
        result.update({
            "pass_equivalent": True,
            "safety_status": "PASS",
        })
        return result
    if mode == "expected_divergence":
        if verdict == "COVERAGE_MISMATCH":
            result.update({
                "safety_status": "NEEDS_HUMAN",
                "needs_human": True,
                "remedy": "correction_evidence_review",
            })
        else:
            result.update({
                "safety_status": "REGRESSION",
                "needs_human": True,
                "remedy": "correction_regression_review",
            })
        return result

    routes = {
        "MATCH": ("PASS", True, False, None),
        "BASIS_STEP": (
            "REVIEW", False, True, "basis_action_review"),
        UNIFORM_RECOMPUTE: (
            "REVIEW", False, True, "volatility_recompute_review"),
        "SCATTERED_MISMATCH": (
            "REVIEW", False, True, "month_refetch_candidate"),
        "COVERAGE_MISMATCH": (
            "REVIEW", False, True, "month_refetch_candidate"),
    }
    status, passed, human, remedy = routes[verdict]
    result.update({
        "safety_status": status,
        "pass_equivalent": passed,
        "needs_human": human,
        "remedy": remedy,
    })
    return result


def _read_stable_bytes(path, max_bytes):
    path = Path(path)
    try:
        before = path.stat()
        if before.st_size <= 0 or before.st_size > max_bytes:
            raise EvidenceError(f"{path.name} has an invalid size")
        raw = path.read_bytes()
        after = path.stat()
    except OSError as exc:
        raise EvidenceError(
            f"cannot read {path.name}: {type(exc).__name__}") from exc
    if (len(raw) != before.st_size or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns):
        raise EvidenceError(f"{path.name} changed while reading")
    return raw


def _ticker(value):
    try:
        return storage.canonical_ticker(value)
    except Exception as exc:  # noqa: BLE001 - normalize evidence errors
        raise EvidenceError(f"invalid ticker: {value!r}") from exc


def _conid(value):
    if isinstance(value, bool):
        raise EvidenceError("manifest conId is invalid")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceError("manifest conId is missing or invalid") from exc
    if number <= 0:
        raise EvidenceError("manifest conId is missing or invalid")
    return number


def _probe_interval(value):
    token = str(value or "").strip()
    if token not in _PROBE_INTERVALS:
        raise EvidenceError(
            "probe interval must be '1m', '1d', '1d-iv', or '1d-hvol'")
    return token


def _is_daily_probe(interval):
    return storage.base_interval(_probe_interval(interval)) == "1d"


def _is_vol_probe(interval):
    return storage.kind_of(_probe_interval(interval)) in storage.RATIO_KINDS


def _interval_months(manifest, interval):
    interval = _probe_interval(interval)
    intervals = manifest.get("intervals")
    if not isinstance(intervals, dict):
        raise EvidenceError("manifest intervals are invalid")
    section = intervals.get(interval)
    if section is None:
        return {}
    if not isinstance(section, dict):
        raise EvidenceError(f"{interval} manifest section is invalid")
    months = section.get("months")
    if not isinstance(months, dict):
        raise EvidenceError(f"{interval} manifest months are invalid")
    if len(months) > MAX_MONTHS:
        raise EvidenceError(f"{interval} manifest has too many months")
    out = {}
    for raw_month, raw_entry in months.items():
        month = str(raw_month)
        if not _MONTH_RE.fullmatch(month) or not isinstance(raw_entry, dict):
            raise EvidenceError(
                f"invalid {interval} manifest month {month!r}")
        status = str(raw_entry.get("status") or "present").lower()
        if status != "present":
            continue
        digest = str(raw_entry.get("sha256") or "").lower()
        if not _SHA_RE.fullmatch(digest):
            raise EvidenceError(
                f"invalid {interval} manifest SHA for {month}")
        rows = raw_entry.get("rows")
        if isinstance(rows, bool):
            raise EvidenceError(
                f"invalid {interval} manifest row count for {month}")
        if rows is not None:
            try:
                rows = int(rows)
            except (TypeError, ValueError) as exc:
                raise EvidenceError(
                    f"invalid {interval} manifest row count for {month}") from exc
            if rows < 1:
                raise EvidenceError(
                    f"invalid {interval} manifest row count for {month}")
        out[month] = {"sha256": digest, "rows": rows}
    return out


def _minute_months(manifest):
    return _interval_months(manifest, "1m")


def _verified_absent(manifest, interval="1m"):
    interval = _probe_interval(interval)
    intervals = manifest.get("intervals")
    section = intervals.get(interval) if isinstance(intervals, dict) else None
    raw = section.get("verified_absent", []) if isinstance(section, dict) else []
    if not isinstance(raw, list):
        raise EvidenceError(f"{interval} verified_absent must be a list")
    if len(raw) > 100_000:
        raise EvidenceError(f"{interval} verified_absent is too large")
    return sorted({_date(value, "verified-absent day").isoformat()
                   for value in raw})


def _validated_price_actions(manifest):
    raw = manifest.get("actions", [])
    if not isinstance(raw, list):
        raise EvidenceError("manifest actions must be a list")
    if len(raw) > MAX_BASIS_ACTIONS:
        raise EvidenceError("manifest has too many basis actions")
    selected = []
    price_dates = set()
    for index, action in enumerate(raw):
        try:
            basis.validate_action(action)
        except (storage.StorageError, TypeError, ValueError) as exc:
            raise EvidenceError(
                f"manifest basis action {index} is invalid") from exc
        if action.get("applies") not in ("price", "both"):
            continue
        day = _date(action.get("date"), "basis action date").isoformat()
        if day in price_dates:
            raise EvidenceError(
                f"manifest has ambiguous price actions on {day}")
        price_dates.add(day)
        selected.append({
            "date": day,
            "kind": str(action["kind"]),
            "factor": float(action["factor"]),
            "applies": str(action["applies"]),
        })
    selected.sort(key=lambda item: (item["date"], item["kind"]))
    return selected


def _validated_corrections(manifest, ticker, interval="1m"):
    interval = _probe_interval(interval)
    raw = manifest.get("data_corrections")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise EvidenceError("manifest data_corrections must be a list")
    if len(raw) > MAX_CORRECTIONS:
        raise EvidenceError("manifest has too many data corrections")
    out = []
    for index, note in enumerate(raw):
        if not isinstance(note, dict):
            raise EvidenceError(f"manifest correction {index} is invalid")
        if note.get("ticker") is not None and _ticker(note["ticker"]) != ticker:
            raise EvidenceError(f"manifest correction {index} ticker mismatches")
        kind = str(note.get("type") or "")
        intervals = note.get("intervals")
        if kind == "identity_listing_truncation" and intervals is None:
            # Listing identity is a ticker-wide floor (Row 47's GOOG-shaped
            # correction record), unlike the older interval-scoped repairs.
            applies_to_interval = True
        else:
            if not isinstance(intervals, list) or not intervals or not all(
                    isinstance(value, str) and value for value in intervals):
                raise EvidenceError(
                    f"manifest correction {index} intervals are invalid")
            applies_to_interval = (
                interval in intervals
                or storage.base_interval(interval) in intervals)
        if not applies_to_interval:
            continue
        if kind == "phantom_split_correction":
            if _is_vol_probe(interval):
                raise EvidenceError(
                    f"phantom split correction cannot target {interval}")
            out.append({
                "type": kind,
                "mode": "expected_divergence",
                "boundary": _date(
                    note.get("ex_date"), "correction ex-date").isoformat(),
                "factor": _positive_factor(
                    note.get("factor"), "correction factor"),
            })
        elif kind in storage.IDENTITY_CORRECTION_TYPES:
            boundary = note.get("cutover") or note.get("ex_date")
            out.append({
                "type": kind,
                "mode": "expected_absent",
                "boundary": _date(
                    boundary, "correction boundary").isoformat(),
                "factor": None,
            })
        else:
            raise EvidenceError(
                f"unsupported {interval} correction type {kind!r}")
    out.sort(key=lambda item: (item["boundary"], item["type"]))
    return out


def correction_for_day(corrections, day):
    day_token = _date(day, "probe day").isoformat()
    relevant = [item for item in corrections
                if day_token < item["boundary"]]
    if not relevant:
        return None
    absent = [item for item in relevant
              if item["mode"] == "expected_absent"]
    if absent:
        return {
            "mode": "expected_absent",
            "factor": None,
            "records": absent,
        }
    factor = 1.0
    for item in relevant:
        factor *= _positive_factor(item.get("factor"), "correction factor")
    if not math.isfinite(factor) or factor <= 0:
        raise EvidenceError("combined correction factor is invalid")
    return {
        "mode": "expected_divergence",
        "factor": factor,
        "records": relevant,
    }


def _read_manifest_record(root, ticker, interval="1m"):
    root = Path(root).resolve()
    ticker = _ticker(ticker)
    interval = _probe_interval(interval)
    ticker_dir = root / ticker
    try:
        if ticker_dir.resolve().parent != root:
            raise EvidenceError(f"ticker path escapes bank: {ticker}")
    except OSError as exc:
        raise EvidenceError(f"cannot resolve ticker path: {ticker}") from exc
    path = ticker_dir / storage.MANIFEST_NAME
    raw = _read_stable_bytes(path, MAX_MANIFEST_BYTES)
    try:
        manifest = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise EvidenceError(f"manifest is invalid for {ticker}") from exc
    if not isinstance(manifest, dict):
        raise EvidenceError(f"manifest is not an object for {ticker}")
    folder = _ticker(manifest.get("folder") or ticker)
    if folder != ticker:
        raise EvidenceError(f"manifest folder {folder} does not match {ticker}")
    months = _interval_months(manifest, interval)
    record = {
        "ticker": ticker,
        "conid": _conid(manifest.get("conid")),
        "manifest": manifest,
        "manifest_path": path,
        "manifest_fingerprint": _sha256(raw),
        "months": months,
    }
    if interval != "1m":
        record["interval"] = interval
    return record


def discover_candidates(root=STORAGE_ROOT, interval="1m"):
    interval = _probe_interval(interval)
    root = Path(root).resolve()
    if not root.is_dir():
        raise EvidenceError("stock bank is unavailable")
    paths = [path for path in sorted(
        root.iterdir(), key=lambda item: item.name.casefold())
        if path.is_dir() and not path.name.startswith("_")
        and (path / storage.MANIFEST_NAME).is_file()]
    if len(paths) > MAX_CANDIDATES:
        raise EvidenceError("stock bank has too many ticker directories")
    candidates = []
    errors = []
    overflow = 0
    for path in paths:
        try:
            record = _read_manifest_record(root, path.name, interval)
            if record["months"]:
                candidate = {
                    "ticker": record["ticker"],
                    "conid": record["conid"],
                    "manifest_fingerprint": record["manifest_fingerprint"],
                    "months": record["months"],
                }
                if interval != "1m":
                    candidate["interval"] = interval
                candidates.append(candidate)
        except Exception as exc:  # noqa: BLE001 - bounded per-ticker evidence
            row = {
                "ticker": str(path.name)[:64],
                "error_type": type(exc).__name__,
                "reason": str(exc)[:300],
            }
            if len(errors) < MAX_SELECTION_ERRORS:
                errors.append(row)
            else:
                overflow += 1
    return candidates, errors, overflow


def _month_bars(root, record, month):
    interval = _probe_interval(record.get("interval", "1m"))
    entry = record["months"].get(month)
    if not isinstance(entry, dict):
        raise EvidenceError(
            f"selected {interval} month {month} is unavailable")
    year, number = int(month[:4]), int(month[5:7])
    path = (storage.find_month_file(
        root, record["ticker"], year, number, interval)
        or storage.month_file_path(
            root, record["ticker"], year, number, interval))
    try:
        bars, stats = storage.read_month_file_fast(
            path, manifest_sha=entry["sha256"])
    except Exception as exc:  # noqa: BLE001 - strict evidence boundary
        raise EvidenceError(
            f"unreadable {interval} month {month}: "
            f"{type(exc).__name__}") from exc
    if stats.get("sha256") != entry["sha256"]:
        raise EvidenceError(
            f"{interval} month {month} does not match its manifest")
    if entry["rows"] is not None and entry["rows"] != len(bars):
        raise EvidenceError(f"{interval} month {month} row count changed")
    return bars, stats


def _day_context_from_evidence(actions, corrections, verified_absent, day):
    day_token = _date(day, "probe day").isoformat()
    ledger_factor = basis.adjustment_factor(
        actions, day_token, applies=("price", "both"))
    ledger_factor = _positive_factor(ledger_factor, "ledger factor")
    correction = correction_for_day(corrections, day_token)
    applied_actions = [
        item for item in actions if item["date"] > day_token]
    return {
        "ledger_factor": ledger_factor,
        "basis_actions_applied": applied_actions,
        "correction": correction,
        "verified_absent": verified_absent,
    }


def _day_context(record, day):
    interval = _probe_interval(record.get("interval", "1m"))
    actions = _validated_price_actions(record["manifest"])
    corrections = _validated_corrections(
        record["manifest"], record["ticker"], interval)
    verified_absent = _verified_absent(record["manifest"], interval)
    return _day_context_from_evidence(
        actions, corrections, verified_absent, day)


def stored_month_snapshot(root, ticker, month, *, day_selector=0, day=None,
                          expected_manifest_fingerprint=None):
    """Strictly read one selected 1m month and return one deterministic day."""
    root = Path(root).resolve()
    if not _MONTH_RE.fullmatch(str(month)):
        raise EvidenceError("invalid selected month")
    record = _read_manifest_record(root, ticker)
    if (expected_manifest_fingerprint is not None
            and record["manifest_fingerprint"]
            != str(expected_manifest_fingerprint)):
        raise EvidenceError("manifest changed after probe selection")
    bars, stats = _month_bars(root, record, str(month))
    grouped = {}
    evidence = {}
    for bar in bars:
        try:
            stamp = bar[0]
            if not isinstance(stamp, dt.datetime):
                raise TypeError
            bar_day = stamp.date().isoformat()
            if bar_day[:7] != str(month):
                raise ValueError
            minute = stamp.time().replace(tzinfo=None).isoformat(
                timespec="seconds")
            close = _positive_factor(bar[4], "stored close")
            volume = float(bar[5])
            if not math.isfinite(volume) or volume < 0:
                raise ValueError
        except (IndexError, TypeError, ValueError) as exc:
            raise EvidenceError(f"invalid 1m bar in {month}") from exc
        day_map = grouped.setdefault(bar_day, {})
        if minute in day_map:
            raise EvidenceError(f"duplicate stored minute on {bar_day}")
        day_map[minute] = close
        evidence.setdefault(bar_day, []).append([minute, close, volume])
    days = sorted(grouped)
    if not days:
        raise EvidenceError(f"stored 1m month {month} is empty")
    if day is not None:
        selected_day = _date(day, "selected day").isoformat()
        if selected_day[:7] != str(month) or selected_day not in grouped:
            raise EvidenceError(
                f"selected day {selected_day} has no stored 1m bars")
    else:
        if isinstance(day_selector, bool):
            raise EvidenceError("invalid day selector")
        try:
            index = int(day_selector) % len(days)
        except (TypeError, ValueError) as exc:
            raise EvidenceError("invalid day selector") from exc
        selected_day = days[index]

    context = _day_context(record, selected_day)
    if selected_day in context["verified_absent"]:
        raise EvidenceError(
            f"selected day {selected_day} is both stored and verified absent")

    current = _read_manifest_record(root, ticker)
    if current["manifest_fingerprint"] != record["manifest_fingerprint"]:
        raise EvidenceError("manifest changed while reading probe evidence")
    digest = _sha256(
        _canonical_json(evidence[selected_day]).encode("utf-8"))
    offline_status = (
        "REGRESSION" if context["correction"]
        and context["correction"]["mode"] == "expected_absent" else "READY")
    return {
        "ticker": record["ticker"],
        "conid": record["conid"],
        "day": selected_day,
        "month": str(month),
        "stored": grouped[selected_day],
        "stored_bars": len(grouped[selected_day]),
        "stored_digest": digest,
        "stored_volume": sum(row[2] for row in evidence[selected_day]),
        "ledger_factor": context["ledger_factor"],
        "basis_actions_applied": context["basis_actions_applied"],
        "correction": context["correction"],
        "verified_absent": context["verified_absent"],
        "manifest_fingerprint": record["manifest_fingerprint"],
        "month_sha256": stats["sha256"],
        "offline_safety_status": offline_status,
        "snapshot_mode": "stored_day",
        "needs_human": offline_status == "REGRESSION",
        "request_count": 0,
        "network": False,
        "written": False,
    }


def stored_day_snapshot(root, ticker, day, *,
                        expected_manifest_fingerprint=None):
    """Read an exact stored day or a manifest-verified absent day."""
    root = Path(root).resolve()
    day_token = _date(day, "selected day").isoformat()
    month = day_token[:7]
    record = _read_manifest_record(root, ticker)
    if (expected_manifest_fingerprint is not None
            and record["manifest_fingerprint"]
            != str(expected_manifest_fingerprint)):
        raise EvidenceError("manifest changed after probe selection")
    context = _day_context(record, day_token)
    if day_token not in context["verified_absent"]:
        return stored_month_snapshot(
            root, ticker, month, day=day_token,
            expected_manifest_fingerprint=record["manifest_fingerprint"])

    month_sha = None
    if month in record["months"]:
        bars, stats = _month_bars(root, record, month)
        if any(bar[0].date().isoformat() == day_token for bar in bars):
            raise EvidenceError(
                f"selected day {day_token} is both stored and verified absent")
        month_sha = stats["sha256"]
    else:
        year, number = int(month[:4]), int(month[5:7])
        if storage.find_month_file(
                root, record["ticker"], year, number, "1m") is not None:
            raise EvidenceError(
                f"verified-absent month {month} has an unmanifested data file")
    current = _read_manifest_record(root, ticker)
    if current["manifest_fingerprint"] != record["manifest_fingerprint"]:
        raise EvidenceError("manifest changed while reading probe evidence")
    return {
        "ticker": record["ticker"],
        "conid": record["conid"],
        "day": day_token,
        "month": month,
        "stored": {},
        "stored_bars": 0,
        "stored_digest": _sha256(b"[]"),
        "stored_volume": 0.0,
        "ledger_factor": context["ledger_factor"],
        "basis_actions_applied": context["basis_actions_applied"],
        "correction": context["correction"],
        "verified_absent": context["verified_absent"],
        "manifest_fingerprint": record["manifest_fingerprint"],
        "month_sha256": month_sha,
        "offline_safety_status": "READY",
        "snapshot_mode": "verified_absence",
        "needs_human": False,
        "request_count": 0,
        "network": False,
        "written": False,
    }


def _daily_span_bounds(day):
    end = _date(day, "daily span anchor")
    start = end - dt.timedelta(days=DAILY_SPAN_DAYS - 1)
    return start.isoformat(), end.isoformat()


def _month_tokens_between(start, end):
    first = _date(start, "span start")
    last = _date(end, "span end")
    if first > last:
        raise EvidenceError("daily span starts after it ends")
    current = dt.date(first.year, first.month, 1)
    stop = dt.date(last.year, last.month, 1)
    out = []
    while current <= stop:
        out.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = dt.date(current.year + 1, 1, 1)
        else:
            current = dt.date(current.year, current.month + 1, 1)
    return out


def _daily_month_rows(bars, month, interval="1d"):
    interval = _probe_interval(interval)
    if not _is_daily_probe(interval):
        raise EvidenceError("daily rows require a daily probe interval")
    allow_zero = _is_vol_probe(interval)
    rows = {}
    for bar in bars:
        try:
            stamp = bar[0]
            if isinstance(stamp, dt.datetime):
                day = stamp.date()
            elif isinstance(stamp, dt.date):
                day = stamp
            else:
                raise TypeError
            day_token = day.isoformat()
            if day_token[:7] != month:
                raise ValueError
            close = (_nonnegative_number(bar[4], "stored daily close")
                     if allow_zero else
                     _positive_factor(bar[4], "stored daily close"))
            volume = _nonnegative_number(bar[5], "stored daily volume")
        except (IndexError, TypeError, ValueError) as exc:
            raise EvidenceError(
                f"invalid {interval} bar in {month}") from exc
        if day_token in rows:
            raise EvidenceError(f"duplicate stored daily bar on {day_token}")
        rows[day_token] = {"close": close, "volume": volume}
    return rows


def _context_identity(context):
    return _canonical_json({
        "ledger_factor": context["ledger_factor"],
        "basis_actions_applied": context["basis_actions_applied"],
        "correction": context["correction"],
    })


def stored_daily_span_snapshot(
        root, ticker, month=None, *, day_selector=0, day=None,
        expected_manifest_fingerprint=None, interval="1d"):
    """Read one strict trailing-365-day daily snapshot.

    The selected stored day is the inclusive span end. Every manifest-present
    daily month intersecting that span is SHA/row-count gated. Calendar context
    segments preserve basis/correction boundaries while one future live
    payload is shared across the full span.
    """
    root = Path(root).resolve()
    interval = _probe_interval(interval)
    if not _is_daily_probe(interval):
        raise EvidenceError("daily span requires a daily probe interval")
    vol_mode = _is_vol_probe(interval)
    record = _read_manifest_record(root, ticker, interval)
    if (expected_manifest_fingerprint is not None
            and record["manifest_fingerprint"]
            != str(expected_manifest_fingerprint)):
        raise EvidenceError("manifest changed after probe selection")
    verified_absent = _verified_absent(record["manifest"], interval)

    parsed = {}
    month_stats = {}
    if day is not None:
        anchor = _date(day, "selected daily anchor").isoformat()
        selected_month = anchor[:7]
    else:
        selected_month = str(month or "")
        if not _MONTH_RE.fullmatch(selected_month):
            raise EvidenceError("invalid selected daily month")
        bars, stats = _month_bars(root, record, selected_month)
        parsed[selected_month] = _daily_month_rows(
            bars, selected_month, interval)
        month_stats[selected_month] = stats["sha256"]
        days = sorted(parsed[selected_month])
        if not days:
            raise EvidenceError(
                f"stored {interval} month {selected_month} is empty")
        if isinstance(day_selector, bool):
            raise EvidenceError("invalid daily day selector")
        try:
            index = int(day_selector) % len(days)
        except (TypeError, ValueError) as exc:
            raise EvidenceError("invalid daily day selector") from exc
        anchor = days[index]

    anchor_absent = vol_mode and anchor in verified_absent
    if selected_month not in record["months"] and not anchor_absent:
        raise EvidenceError(
            f"selected {interval} month {selected_month} is unavailable")
    span_start, span_end = _daily_span_bounds(anchor)
    span_months = _month_tokens_between(span_start, span_end)
    for token in span_months:
        if token not in record["months"]:
            year, number = int(token[:4]), int(token[5:7])
            if storage.find_month_file(
                    root, record["ticker"], year, number,
                    interval) is not None:
                raise EvidenceError(
                    f"daily span has unmanifested {interval} month {token}")
            continue
        if token not in parsed:
            bars, stats = _month_bars(root, record, token)
            parsed[token] = _daily_month_rows(bars, token, interval)
            month_stats[token] = stats["sha256"]

    stored_rows = {}
    for token in span_months:
        for day_token, row in parsed.get(token, {}).items():
            if span_start <= day_token <= span_end:
                if day_token in stored_rows:
                    raise EvidenceError(
                        f"duplicate stored daily bar on {day_token}")
                stored_rows[day_token] = row
    if anchor not in stored_rows and not anchor_absent:
        raise EvidenceError(
            f"selected day {anchor} has no stored {interval} bar")

    validated_actions = _validated_price_actions(record["manifest"])
    actions = [] if vol_mode else validated_actions
    corrections = _validated_corrections(
        record["manifest"], record["ticker"], interval)
    conflicts = sorted(set(stored_rows) & set(verified_absent))
    if conflicts:
        raise EvidenceError(
            f"stored {interval} day {conflicts[0]} is also verified absent")

    segments = []
    segment_for_day = {}
    cursor = _date(span_start, "span start")
    end_date = _date(span_end, "span end")
    while cursor <= end_date:
        token = cursor.isoformat()
        context = _day_context_from_evidence(
            actions, corrections, verified_absent, token)
        identity = _context_identity(context)
        if not segments or segments[-1]["context_identity"] != identity:
            segments.append({
                "context_identity": identity,
                "context_start": token,
                "context_end": token,
                "ledger_factor": context["ledger_factor"],
                "basis_actions_applied": context["basis_actions_applied"],
                "correction": context["correction"],
                "verified_absent": [],
                "stored": {},
                "stored_volume": 0.0,
            })
        else:
            segments[-1]["context_end"] = token
        segment_for_day[token] = len(segments) - 1
        cursor += dt.timedelta(days=1)

    absent_set = set(verified_absent)
    for token in sorted(stored_rows):
        segment = segments[segment_for_day[token]]
        segment["stored"][token] = stored_rows[token]["close"]
        segment["stored_volume"] += stored_rows[token]["volume"]
    for token in sorted(absent_set):
        if span_start <= token <= span_end:
            segments[segment_for_day[token]]["verified_absent"].append(token)
    for segment in segments:
        evidence = [[token, segment["stored"][token]]
                    for token in sorted(segment["stored"])]
        segment["stored_bars"] = len(evidence)
        segment["stored_digest"] = _sha256(
            _canonical_json(evidence).encode("utf-8"))

    combined_evidence = [[
        token, stored_rows[token]["close"], stored_rows[token]["volume"]]
        for token in sorted(stored_rows)]
    stored_digest = _sha256(
        _canonical_json(combined_evidence).encode("utf-8"))
    span_fingerprint = _sha256(_canonical_json([
        record["ticker"], record["conid"], interval, span_start, span_end,
        record["manifest_fingerprint"], stored_digest,
        sorted(month_stats.items()),
    ]).encode("utf-8"))
    current = _read_manifest_record(root, ticker, interval)
    if current["manifest_fingerprint"] != record["manifest_fingerprint"]:
        raise EvidenceError("manifest changed while reading daily span evidence")

    active_segments = [
        item for item in segments
        if item["stored_bars"] or (vol_mode and item["verified_absent"])
    ]
    regression = any(
        item["stored_bars"] and item.get("correction")
        and item["correction"].get("mode") == "expected_absent"
        for item in active_segments)
    return {
        "ticker": record["ticker"],
        "conid": record["conid"],
        "interval": interval,
        "day": anchor,
        "anchor_day": anchor,
        "month": selected_month,
        "span_start": span_start,
        "span_end": span_end,
        "span_days": DAILY_SPAN_DAYS,
        "span_fingerprint": span_fingerprint,
        "stored": {token: stored_rows[token]["close"]
                   for token in sorted(stored_rows)},
        "stored_bars": len(stored_rows),
        "stored_digest": stored_digest,
        "stored_volume": sum(
            row["volume"] for row in stored_rows.values()),
        "basis_actions": actions,
        "corrections": corrections,
        "manifest_fingerprint": record["manifest_fingerprint"],
        "month_sha256s": dict(sorted(month_stats.items())),
        "contexts": segments,
        "context_count": len(active_segments),
        "small_context_count": sum(
            1 for item in active_segments
            if item["stored_bars"]
            and item["stored_bars"] < SCATTER_MIN_BARS),
        "verified_absent": [
            token for token in verified_absent
            if span_start <= token <= span_end],
        "offline_safety_status": "REGRESSION" if regression else "READY",
        "snapshot_mode": "stored_daily_span",
        "needs_human": regression,
        "request_count": 0,
        "network": False,
        "written": False,
    }


def _public_snapshot(snapshot):
    return {key: value for key, value in snapshot.items()
            if key not in (
                "stored", "verified_absent", "contexts",
                "basis_actions", "corrections")}


def _candidate_fingerprint(candidates, interval="1m"):
    interval = _probe_interval(interval)
    if interval == "1m":
        payload = [[
            item["ticker"], item["conid"], item["manifest_fingerprint"],
            sorted(item["months"]),
        ] for item in candidates]
    else:
        payload = [[
            interval, item["ticker"], item["conid"],
            item["manifest_fingerprint"], sorted(item["months"]),
        ] for item in candidates]
    return _sha256(_canonical_json(payload).encode("utf-8"))


def build_plan(root=STORAGE_ROOT, *, seed=None, count=DEFAULT_COUNT, now=None,
               interval="1m"):
    """Build a reproducible, offline-only probe plan for the fixed bank."""
    interval = _probe_interval(interval)
    generated_at = _utc_timestamp(now)
    seed = default_seed(now) if seed is None else normalize_seed(seed)
    count = _interval_count(count, interval)
    candidates, errors, overflow = discover_candidates(root, interval)
    if not candidates:
        raise EvidenceError(f"no eligible stored {interval} series")
    fingerprint = _candidate_fingerprint(candidates, interval)
    pool = list(candidates)
    probes = []
    state = seed
    while pool and len(probes) < count:
        state = _lcg(state)
        candidate = pool.pop(state % len(pool))
        month, state = pick_probe(candidate["months"], state)
        try:
            if interval == "1m":
                snapshot = stored_month_snapshot(
                    root, candidate["ticker"], month,
                    day_selector=state,
                    expected_manifest_fingerprint=(
                        candidate["manifest_fingerprint"]))
            else:
                snapshot = stored_daily_span_snapshot(
                    root, candidate["ticker"], month,
                    day_selector=state,
                    expected_manifest_fingerprint=(
                        candidate["manifest_fingerprint"]),
                    interval=interval)
            probes.append(_public_snapshot(snapshot))
        except Exception as exc:  # noqa: BLE001 - one bounded probe-error row
            row = {
                "ticker": candidate["ticker"],
                "month": month,
                "error_type": type(exc).__name__,
                "reason": str(exc)[:300],
            }
            if len(errors) < MAX_SELECTION_ERRORS:
                errors.append(row)
            else:
                overflow += 1
    regression_count = sum(
        1 for row in probes
        if row.get("offline_safety_status") == "REGRESSION")
    status = "ready"
    if len(probes) < count or errors or overflow:
        status = "partial"
    if regression_count:
        status = "review_required"
    report = {
        "kind": "live_spot_probe_plan",
        "version": REPORT_VERSION,
        "generated_at": generated_at,
        "seed": seed,
        "requested_count": count,
        "selected_count": len(probes),
        "candidate_count": len(candidates),
        "candidate_fingerprint": fingerprint,
        "status": status,
        "offline_regression_count": regression_count,
        "probes": probes,
        "errors": errors,
        "error_overflow": overflow,
        "network": False,
        "written": False,
        "bank_written": False,
        "artifact_written": False,
        "live_capability": False,
        "request_count": 0,
    }
    if _is_daily_probe(interval):
        report.update({
            "interval": interval,
            "span_days": DAILY_SPAN_DAYS,
            "request_duration": DAILY_REQUEST_DURATION,
        })
    return report


def build_embedded_plan(root, tickers, *, seed=None, now=None,
                        interval="1m"):
    """Select one deterministic stored day or daily span per eligible ticker.

    Selection failures are returned as bounded report rows so a parent run can
    finish without guessing or dropping a processed ticker.
    """
    interval = _probe_interval(interval)
    seed = default_seed(now) if seed is None else normalize_seed(seed)
    requested = sorted({str(ticker or "").strip().upper()
                        for ticker in tickers if str(ticker or "").strip()})
    candidates, discovery_errors, overflow = discover_candidates(
        root, interval)
    by_ticker = {row["ticker"]: row for row in candidates}
    items = {}
    rows = []
    state = seed
    for ticker in requested:
        candidate = by_ticker.get(ticker)
        try:
            if candidate is None:
                raise EvidenceError(
                    f"no eligible stored {interval} RTH series")
            month, state = pick_probe(candidate["months"], state)
            if interval == "1m":
                snapshot = stored_month_snapshot(
                    root, ticker, month, day_selector=state,
                    expected_manifest_fingerprint=(
                        candidate["manifest_fingerprint"]))
            else:
                snapshot = stored_daily_span_snapshot(
                    root, ticker, month, day_selector=state,
                    expected_manifest_fingerprint=(
                        candidate["manifest_fingerprint"]),
                    interval=interval)
            selection = {
                "day": snapshot["day"],
                "manifest_fingerprint": snapshot["manifest_fingerprint"],
            }
            if _is_daily_probe(interval):
                selection.update({
                    "interval": interval,
                    "span_start": snapshot["span_start"],
                    "span_end": snapshot["span_end"],
                    "span_fingerprint": snapshot["span_fingerprint"],
                    "stored_digest": snapshot["stored_digest"],
                    "month_sha256s": snapshot["month_sha256s"],
                })
            items[ticker] = selection
        except Exception as exc:  # noqa: BLE001 - bounded parent evidence
            rows.append(_probe_error_row(
                {"ticker": ticker}, exc, request_count=0, network=False))
        state = _lcg(state)
    report = {
        "seed": seed,
        "items": items,
        "rows": rows,
        "requested_count": len(requested),
        "selected_count": len(items),
        "selection_error_count": len(rows),
        "discovery_errors": list(discovery_errors),
        "discovery_error_overflow": int(overflow),
    }
    if _is_daily_probe(interval):
        report["interval"] = interval
    return report


def _explicit_plan(root, ticker, day, *, seed=None, now=None,
                   interval="1m"):
    interval = _probe_interval(interval)
    day_token = _date(day, "selected day").isoformat()
    if interval == "1m":
        snapshot = stored_day_snapshot(root, ticker, day_token)
    else:
        snapshot = stored_daily_span_snapshot(
            root, ticker, day=day_token, interval=interval)
    public = _public_snapshot(snapshot)
    candidate_months = (
        {public["month"]: {
            "sha256": public["month_sha256"], "rows": None}}
        if interval == "1m" else
        {month: {"sha256": digest, "rows": None}
         for month, digest in public["month_sha256s"].items()})
    candidate_row = {
        "ticker": public["ticker"],
        "conid": public["conid"],
        "manifest_fingerprint": public["manifest_fingerprint"],
        "months": candidate_months,
    }
    if _is_daily_probe(interval):
        candidate_row["interval"] = interval
    candidate = [candidate_row]
    regression_count = int(
        public.get("offline_safety_status") == "REGRESSION")
    report = {
        "kind": "live_spot_probe_plan",
        "version": REPORT_VERSION,
        "generated_at": _utc_timestamp(now),
        "seed": None if seed is None else normalize_seed(seed),
        "requested_count": 1,
        "selected_count": 1,
        "candidate_count": 1,
        "candidate_fingerprint": _candidate_fingerprint(
            candidate, interval),
        "status": "review_required" if regression_count else "ready",
        "offline_regression_count": regression_count,
        "probes": [public],
        "errors": [],
        "error_overflow": 0,
        "network": False,
        "written": False,
        "bank_written": False,
        "artifact_written": False,
        "live_capability": False,
        "request_count": 0,
    }
    if _is_daily_probe(interval):
        report.update({
            "interval": interval,
            "span_days": DAILY_SPAN_DAYS,
            "request_duration": DAILY_REQUEST_DURATION,
        })
    return report


def bank_tree_metadata(root=STORAGE_ROOT):
    """Return a complete path/type/size/mtime fingerprint of the bank tree."""
    root = Path(root).resolve()
    if not root.is_dir():
        raise EvidenceError("stock bank is unavailable")
    try:
        paths = [root] + sorted(
            root.rglob("*"), key=lambda item: item.relative_to(root).as_posix())
        rows = []
        for path in paths:
            stat = path.lstat()
            relative = "." if path == root else path.relative_to(root).as_posix()
            if path.is_symlink():
                kind = "symlink"
            elif path.is_dir():
                kind = "dir"
            elif path.is_file():
                kind = "file"
            else:
                kind = "other"
            rows.append([
                relative, kind, int(stat.st_size), int(stat.st_mtime_ns)])
    except OSError as exc:
        raise EvidenceError(
            f"cannot fingerprint stock bank: {type(exc).__name__}") from exc
    return {
        "entries": len(rows),
        "sha256": _sha256(_canonical_json(rows).encode("utf-8")),
    }


def _revalidate_snapshot(root, snapshot):
    interval = _probe_interval(snapshot.get("interval", "1m"))
    if _is_daily_probe(interval):
        current = stored_daily_span_snapshot(
            root, snapshot["ticker"], day=snapshot["day"],
            expected_manifest_fingerprint=snapshot["manifest_fingerprint"],
            interval=interval)
        keys = (
            "ticker", "conid", "interval", "day", "anchor_day",
            "span_start", "span_end", "span_days", "span_fingerprint",
            "stored_digest", "stored_volume", "stored_bars",
            "manifest_fingerprint", "month_sha256s", "contexts",
            "basis_actions", "corrections", "verified_absent",
            "snapshot_mode",
        )
        before = {key: snapshot.get(key) for key in keys}
        after = {key: current.get(key) for key in keys}
        if _canonical_json(before) != _canonical_json(after):
            raise EvidenceError(
                "stored daily span evidence changed during live request")
        return current
    current = stored_day_snapshot(
        root, snapshot["ticker"], snapshot["day"],
        expected_manifest_fingerprint=snapshot["manifest_fingerprint"])
    keys = (
        "ticker", "conid", "day", "month", "stored_digest",
        "stored_volume", "ledger_factor", "basis_actions_applied",
        "correction", "verified_absent", "manifest_fingerprint",
        "month_sha256", "snapshot_mode",
    )
    before = {key: snapshot.get(key) for key in keys}
    after = {key: current.get(key) for key in keys}
    if _canonical_json(before) != _canonical_json(after):
        raise EvidenceError("stored probe evidence changed during live request")
    return current


def _live_digest(closes):
    rows = [[key, closes[key]] for key in sorted(closes)]
    return _sha256(_canonical_json(rows).encode("utf-8"))


def _normalize_live_result(value, *, allow_zero=False):
    if not isinstance(value, dict):
        raise LiveFetchError("live fetcher returned a non-object result")
    closes = _price_map(
        value.get("closes"), "live", allow_zero=allow_zero)
    volume = _nonnegative_number(value.get("volume", 0), "live volume")
    raw_bars = _nonnegative_int(
        value.get("raw_bar_count", len(closes)), "raw live bar count")
    accepted_bars = _nonnegative_int(
        value.get("accepted_bar_count", len(closes)),
        "accepted live bar count")
    if accepted_bars != len(closes) or raw_bars < accepted_bars:
        raise LiveFetchError("live fetcher returned inconsistent bar counts")
    counters = value.get("discarded", {})
    if not isinstance(counters, dict):
        raise LiveFetchError("live fetcher returned invalid discard counters")
    discarded = {}
    for key in ("invalid", "outside_day", "non_rth"):
        discarded[key] = _nonnegative_int(
            counters.get(key, 0), f"live {key} count")
    result = {
        "closes": closes,
        "volume": volume,
        "raw_bar_count": raw_bars,
        "accepted_bar_count": accepted_bars,
        "discarded": discarded,
    }
    for key in ("port", "client_id"):
        if value.get(key) is not None:
            result[key] = _nonnegative_int(value[key], f"live {key}")
    return result


def _normalize_daily_live_result(value, snapshot):
    interval = _probe_interval(snapshot.get("interval"))
    if not _is_daily_probe(interval):
        raise LiveFetchError("daily result requires a daily probe interval")
    result = _normalize_live_result(
        value, allow_zero=_is_vol_probe(interval))
    span_start = _date(snapshot.get("span_start"), "span start").isoformat()
    span_end = _date(snapshot.get("span_end"), "span end").isoformat()
    for token in result["closes"]:
        day = _date(token, "live daily key").isoformat()
        if day != token:
            raise LiveFetchError("live daily key is not canonical ISO date")
        if not span_start <= day <= span_end:
            raise LiveFetchError("live daily result escapes its requested span")
    return result


def _daily_evidence_summary(records):
    records = list(records or [])
    return {
        "count": len(records),
        "sha256": _sha256(_canonical_json(records).encode("utf-8")),
    }


def _daily_correction_summary(correction):
    if correction is None:
        return None
    records = correction.get("records", [])
    return {
        "mode": correction.get("mode"),
        "factor": correction.get("factor"),
        "record_count": len(records),
        "records_sha256": _sha256(
            _canonical_json(records).encode("utf-8")),
    }


def _daily_segment_error(segment, message):
    return {
        "context_start": segment["context_start"],
        "context_end": segment["context_end"],
        "stored_bars": segment["stored_bars"],
        "stored_digest": segment["stored_digest"],
        "ledger_factor": segment["ledger_factor"],
        "basis_actions_applied": _daily_evidence_summary(
            segment["basis_actions_applied"]),
        "correction": _daily_correction_summary(segment["correction"]),
        "verdict": PROBE_ERROR,
        "safety_status": PROBE_ERROR,
        "pass_equivalent": False,
        "needs_human": True,
        "remedy": "daily_context_review",
        "error": {
            "type": "EvidenceError",
            "message": str(message)[:500],
        },
    }


def _daily_segment_rank(row):
    status = row.get("safety_status")
    verdict = row.get("verdict")
    if verdict == PROBE_ERROR:
        return 100
    if status == "REGRESSION":
        return 95
    if status == "SOURCE_DATA_REAPPEARED":
        return 90
    if row.get("needs_human"):
        return {
            "SCATTERED_MISMATCH": 85,
            "COVERAGE_MISMATCH": 84,
            "NO_LIVE_DATA": 83,
            UNIFORM_RECOMPUTE: 82,
            "BASIS_STEP": 82,
        }.get(verdict, 80)
    if verdict == "NO_LIVE_DATA":
        return 40
    if verdict == "DOCUMENTED_DIVERGENCE":
        return 20
    return 10


def classify_daily_span_live(
        snapshot, live_result, *, request_count=1, network=True,
        evidence_source=None, reused_request_count=0):
    """Classify one daily span while preserving basis/correction segments."""
    if not isinstance(snapshot, dict):
        raise EvidenceError("stored daily span snapshot must be an object")
    interval = _probe_interval(snapshot.get("interval"))
    if not _is_daily_probe(interval):
        raise EvidenceError("stored daily span interval is invalid")
    vol_mode = _is_vol_probe(interval)
    required = (
        "contexts", "span_start", "span_end", "span_fingerprint",
        "stored_digest", "stored_volume", "manifest_fingerprint",
        "month_sha256s", "basis_actions", "corrections", "snapshot_mode",
    )
    missing = [key for key in required if key not in snapshot]
    if missing:
        raise EvidenceError(
            f"stored daily span snapshot is missing {missing[0]}")
    if snapshot.get("snapshot_mode") != "stored_daily_span":
        raise EvidenceError("stored daily span mode is invalid")
    request_count = _nonnegative_int(request_count, "probe request count")
    reused_request_count = _nonnegative_int(
        reused_request_count, "probe reused request count")
    if request_count > 1 or reused_request_count > 1:
        raise EvidenceError("probe span request counts must not exceed one")
    if request_count and reused_request_count:
        raise EvidenceError(
            "probe span cannot request and reuse the same evidence")

    live = _normalize_daily_live_result(live_result, snapshot)
    segments = []
    for context in snapshot["contexts"]:
        stored = context["stored"]
        live_closes = {
            token: close for token, close in live["closes"].items()
            if context["context_start"] <= token <= context["context_end"]}
        reappeared = sorted(
            set(live_closes) & set(context["verified_absent"]))
        correction_absent = (
            isinstance(context.get("correction"), dict)
            and context["correction"].get("mode") == "expected_absent")
        absence_only = bool(
            vol_mode and not stored and not live_closes
            and context["verified_absent"])
        if not stored and not live_closes and not absence_only:
            continue
        if (stored and len(stored) < SCATTER_MIN_BARS
                and not reappeared and not correction_absent):
            segments.append(_daily_segment_error(
                context,
                "daily context has fewer than three stored observations"))
            continue
        selected_day = (
            sorted(stored)[-1] if stored else
            sorted(live_closes)[-1] if live_closes else
            sorted(context["verified_absent"])[-1])
        result = classify_probe(
            stored, live_closes, day=selected_day,
            ledger_factor=context["ledger_factor"],
            correction=(None if absence_only else context["correction"]),
            verified_absent=context["verified_absent"],
            vol_mode=vol_mode)
        result.update({
            "context_start": context["context_start"],
            "context_end": context["context_end"],
            "stored_digest": context["stored_digest"],
            "basis_actions_applied": _daily_evidence_summary(
                context["basis_actions_applied"]),
            "correction": _daily_correction_summary(
                context["correction"]),
            "verified_absent_count": len(context["verified_absent"]),
        })
        if reappeared:
            result.update({
                "verdict": "COVERAGE_MISMATCH",
                "safety_status": "SOURCE_DATA_REAPPEARED",
                "pass_equivalent": False,
                "needs_human": True,
                "remedy": "source_absence_review",
                "source_reappeared_days": reappeared,
            })
        segments.append(result)
    if not segments:
        raise EvidenceError("daily span has no stored or live observations")

    worst = max(segments, key=_daily_segment_rank)
    counts = {}
    for segment in segments:
        verdict = str(segment.get("verdict") or PROBE_ERROR)
        counts[verdict] = counts.get(verdict, 0) + 1
    row = _public_snapshot(snapshot)
    row.update({
        "verdict": worst["verdict"],
        "safety_status": worst["safety_status"],
        "pass_equivalent": all(
            segment.get("pass_equivalent") is True for segment in segments),
        "needs_human": any(
            segment.get("needs_human") is True for segment in segments),
        "remedy": worst.get("remedy"),
        "segments": segments,
        "segment_count": len(segments),
        "segment_verdict_counts": counts,
        "basis_actions": snapshot["basis_actions"],
        "corrections": snapshot["corrections"],
        "live_digest": _live_digest(live["closes"]),
        "live_volume": live["volume"],
        "volume_delta": live["volume"] - snapshot["stored_volume"],
        "raw_live_bars": live["raw_bar_count"],
        "accepted_live_bars": live["accepted_bar_count"],
        "discarded_live_bars": live["discarded"],
        "covered_day_count": snapshot["stored_bars"],
        "request_count": request_count,
        "reused_request_count": reused_request_count,
        "span_request_count": request_count,
        "span_reuse_count": reused_request_count,
        "network": bool(network),
        "written": False,
        "evidence_current": True,
    })
    if evidence_source is not None:
        token = str(evidence_source or "").strip()
        if token not in {"embedded_request", "reused_read_only"}:
            raise EvidenceError("invalid embedded evidence source")
        row["evidence_source"] = token
    if "port" in live:
        row["port"] = live["port"]
    if "client_id" in live:
        row["client_id"] = live["client_id"]
    return row


def classify_snapshot_live(snapshot, live_result, *, request_count=1,
                           network=True, evidence_source=None,
                           reused_request_count=0):
    """Build one public WS8 row from a strict stored snapshot and live bars."""
    if not isinstance(snapshot, dict):
        raise EvidenceError("stored probe snapshot must be an object")
    required = (
        "stored", "day", "ledger_factor", "correction",
        "verified_absent", "snapshot_mode", "stored_volume",
    )
    missing = [key for key in required if key not in snapshot]
    if missing:
        raise EvidenceError(
            f"stored probe snapshot is missing {missing[0]}")
    request_count = _nonnegative_int(request_count, "probe request count")
    reused_request_count = _nonnegative_int(
        reused_request_count, "probe reused request count")
    if request_count > 1 or reused_request_count > 1:
        raise EvidenceError("probe request counts must not exceed one")
    if request_count and reused_request_count:
        raise EvidenceError("probe cannot request and reuse the same evidence")

    live = _normalize_live_result(live_result)
    absence_only = snapshot.get("snapshot_mode") == "verified_absence"
    result = classify_probe(
        snapshot["stored"], live["closes"], day=snapshot["day"],
        ledger_factor=snapshot["ledger_factor"],
        correction=(None if absence_only else snapshot["correction"]),
        verified_absent=snapshot["verified_absent"])
    if absence_only:
        result["correction"] = snapshot["correction"]
        if result["verdict"] != "NO_LIVE_DATA":
            result.update({
                "safety_status": "SOURCE_DATA_REAPPEARED",
                "pass_equivalent": False,
                "needs_human": True,
                "remedy": "source_absence_review",
            })

    row = _public_snapshot(snapshot)
    row.update(result)
    row.update({
        "live_digest": _live_digest(live["closes"]),
        "live_volume": live["volume"],
        "volume_delta": live["volume"] - snapshot["stored_volume"],
        "raw_live_bars": live["raw_bar_count"],
        "accepted_live_bars": live["accepted_bar_count"],
        "discarded_live_bars": live["discarded"],
        "request_count": request_count,
        "network": bool(network),
        "written": False,
        "evidence_current": True,
    })
    if evidence_source is not None:
        token = str(evidence_source or "").strip()
        if token not in {"embedded_request", "reused_read_only"}:
            raise EvidenceError("invalid embedded evidence source")
        row["evidence_source"] = token
        row["reused_request_count"] = reused_request_count
    if "port" in live:
        row["port"] = live["port"]
    if "client_id" in live:
        row["client_id"] = live["client_id"]
    return row


def _probe_adapter(fetcher):
    """Resolve the shipped transport without lending authority to factories."""
    adapter = fib.without_authority(getattr)(fetcher, "_adapter")
    if type(adapter) is sk.ReusableAdapter:
        adapter = fib.without_authority(lambda: adapter._live())()
    if type(adapter) is not sk.LiveIB:
        raise RequestRefused("probe requires the shipped LiveIB transport")
    return adapter


def _probe_request_evidence(request, raw, token):
    """Bind a fresh synchronous result to its own durable physical attempt."""
    context = request.worker.context
    with context.ledger._lock:
        if context.ledger.failure:
            raise LedgerError("probe ledger failed before result publication")
        events = inspect_ledger(context.ledger.path, require_seal=False)["events"]
    pair = [event for event in events if event.get("logical_id") == request.logical_id]
    if (len(pair) != 2 or [event["event"] for event in pair] != ["decision", "result"]
            or pair[0]["attempt_id"] != pair[1]["attempt_id"]):
        raise RequestRefused("probe result lacks its single durable attempt")
    decision, result = pair[0]["payload"], pair[1]["payload"]
    observed = row_evidence(normalize_rows(fib.normalize_bars(raw, token)))
    if result.get("accepted") != observed or result.get("outcome") not in {"returned", "empty"}:
        raise RequestRefused("probe result differs from its durable accepted rows")
    return {"operation_id": context.operation_id, "attempt_id": pair[0]["attempt_id"],
            "logical_id": request.logical_id, "producer_id": request.producer_id,
            "ledger_path": str(context.ledger.path), "accepted": observed,
            "raw_bar_count": result["observed"]["count"], "dropped": result["dropped"],
            "requested": decision["requested"], "effective": decision["effective"],
            "captured_now": context.captured_now.isoformat(),
            "coverage_complete": decision["requested"]["intended_end"] == decision["effective"]["intended_end"]}


class IBKRMinuteDayFetcher:
    """One-connection, no-retry adapter for one RTH 1m request per call."""

    def __init__(self, *, port=2000, host=None, adapter_factory=None,
                 ibkr_module=None, pacer=None):
        self.requested_port = _port(port)
        self.host = host
        self._adapter_factory = adapter_factory
        self._ibkr = ibkr_module
        self._pacer = pacer
        self._adapter = None
        self._borrowed = False

    @classmethod
    def from_borrowed_adapter(cls, adapter, *, ibkr_module=None, pacer=None):
        """Wrap a parent-worker adapter without taking over its lifecycle."""
        if adapter is None:
            raise LiveFetchError("borrowed live adapter is required")
        obj = cls(
            port=getattr(adapter, "port", 2000),
            ibkr_module=ibkr_module, pacer=pacer)
        obj._adapter = adapter
        obj._borrowed = True
        return obj

    def start(self):
        if self._adapter is not None:
            return self
        module = self._ibkr or importlib.import_module("stock_ibkr")
        self._ibkr = module
        if self._pacer is None:
            self._pacer = module.Pacer()
        if self._adapter_factory is None:
            host = self.host if self.host is not None else module.HOST_DEFAULT
            self._adapter = module.LiveIB(
                host=host, ports=(self.requested_port,),
                client_id=module.CLIENT_ID_SPOT_PROBE).connect()
        else:
            self._adapter = self._adapter_factory()
        if self._adapter is None:
            raise LiveFetchError("live adapter factory returned no adapter")
        return self

    @fib.worker_scope
    def fetch_day(self, snapshot, *, pacer=None, cancel=None):
        if self._adapter is None:
            raise LiveFetchError("live fetcher has not been started")
        day = _date(snapshot.get("day"), "probe day")
        requests = sk._covered_session_requests("1m", day, fib.current_worker().context)
        if len(requests) != 1:
            raise LiveFetchError("1m day did not produce exactly one request")
        adapter = _probe_adapter(self)
        contract = fib.without_authority(sk.LiveIB.contract_for)(adapter, _conid(snapshot.get("conid")))
        first, end_dt, duration = requests[0]
        request = fib.bar_request("ibkr.live_spot_probe.minute", contract, "1m", end_dt,
                                  duration, symbol=snapshot["ticker"], start=first)
        try:
            sender = SimpleNamespace(fetch=fib.without_authority(getattr)(adapter, "fetch"))
            with fib.adapter_session(adapter, True), fib.send_scope(request, fib.acquire_turn):
                raw = sender.fetch(contract, end_dt, duration, "1 min", "TRADES")
        except sk.PacingViolation:
            fib.pacer().saturate()
            raise
        raw = list(raw or [])
        evidence = _probe_request_evidence(request, raw, "1m")
        if cancel is not None and cancel.is_set():
            raise sk.Cancelled("minute probe cancelled before consumption")
        counters = {"invalid": 0, "outside_day": 0, "non_rth": 0}
        bars = sk.convert_bars(raw, day, counters, interval="1m")
        closes = {}
        volume = 0.0
        for bar in bars:
            try:
                stamp = bar[0]
                if not isinstance(stamp, dt.datetime):
                    raise TypeError
                minute = stamp.time().replace(tzinfo=None).isoformat(
                    timespec="seconds")
                close = _positive_factor(bar[4], "live close")
                bar_volume = _nonnegative_number(bar[5], "live volume")
            except (IndexError, TypeError, ValueError) as exc:
                raise LiveFetchError("live converter returned an invalid bar") from exc
            if minute in closes:
                raise LiveFetchError(f"duplicate live minute on {day}")
            closes[minute] = close
            volume += bar_volume
        return {
            "closes": closes,
            "volume": volume,
            "raw_bar_count": evidence["raw_bar_count"],
            "fetch_provenance": evidence,
            "accepted_bar_count": len(bars),
            "discarded": counters,
            "port": fib.without_authority(getattr)(self._adapter, "port", self.requested_port),
            "client_id": fib.without_authority(getattr)(self._adapter, "client_id", None),
        }

    def close(self):
        adapter, self._adapter = self._adapter, None
        if adapter is not None and not self._borrowed:
            adapter.disconnect()


class IBKRDailySpanFetcher(IBKRMinuteDayFetcher):
    """One-connection, no-retry adapter for one trailing-year daily request."""

    @fib.worker_scope
    def fetch_span(self, snapshot, *, pacer=None, cancel=None):
        if self._adapter is None:
            raise LiveFetchError("live fetcher has not been started")
        interval = _probe_interval(snapshot.get("interval"))
        if not _is_daily_probe(interval):
            raise LiveFetchError(
                "daily fetcher requires a daily span snapshot")
        vol_mode = _is_vol_probe(interval)
        span_start = _date(snapshot.get("span_start"), "span start")
        span_end = _date(snapshot.get("span_end"), "span end")
        if (span_end - span_start).days != DAILY_SPAN_DAYS - 1:
            raise LiveFetchError("daily span is not exactly 365 days")
        window = fib.current_worker().context.authority.window(interval, span_end)
        if window is None:
            raise RequestRefused("daily probe anchor is a closed session")
        adapter = _probe_adapter(self)
        contract = fib.without_authority(sk.LiveIB.contract_for)(adapter, _conid(snapshot.get("conid")))
        end_dt = dt.datetime.combine(span_end, dt.time(16, 0))
        what_to_show = sk._what_to_show(interval)
        request = fib.bar_request("ibkr.live_spot_probe.daily", contract, interval, end_dt,
            DAILY_REQUEST_DURATION, symbol=snapshot["ticker"],
            start=dt.datetime.combine(span_start, dt.time.min), intended_end=window[1])
        try:
            sender = SimpleNamespace(fetch=fib.without_authority(getattr)(adapter, "fetch"))
            with fib.adapter_session(adapter, True), fib.send_scope(request, fib.acquire_turn):
                raw = sender.fetch(contract, end_dt, DAILY_REQUEST_DURATION,
                                    DAILY_BAR_SIZE, what_to_show)
        except sk.PacingViolation:
            fib.pacer().saturate()
            raise
        raw = list(raw or [])
        evidence = _probe_request_evidence(request, raw, interval)
        if cancel is not None and cancel.is_set():
            raise sk.Cancelled("daily probe cancelled before consumption")
        counters = {"invalid": 0, "outside_day": 0, "non_rth": 0}
        valid_days = frozenset(
            span_start + dt.timedelta(days=index)
            for index in range(DAILY_SPAN_DAYS))
        by_day = sk.split_session_bars(
            raw, valid_days, counters, interval)
        closes = {}
        volume = 0.0
        accepted = 0
        for day in sorted(by_day):
            bars = by_day[day]
            if len(bars) != 1:
                raise LiveFetchError(
                    f"daily converter returned {len(bars)} bars on {day}")
            bar = bars[0]
            try:
                close = (_nonnegative_number(bar[4], "live daily close")
                         if vol_mode else
                         _positive_factor(bar[4], "live daily close"))
                bar_volume = _nonnegative_number(
                    bar[5], "live daily volume")
            except (IndexError, TypeError, ValueError) as exc:
                raise LiveFetchError(
                    "daily converter returned an invalid bar") from exc
            closes[day.isoformat()] = close
            volume += bar_volume
            accepted += 1
        return {
            "closes": closes,
            "volume": volume,
            "raw_bar_count": evidence["raw_bar_count"],
            "fetch_provenance": evidence,
            "accepted_bar_count": accepted,
            "discarded": counters,
            "port": fib.without_authority(getattr)(self._adapter, "port", self.requested_port),
            "client_id": fib.without_authority(getattr)(self._adapter, "client_id", None),
        }


def _probe_error_row(snapshot, exc, *, request_count, network):
    row = _public_snapshot(snapshot)
    row.update({
        "verdict": PROBE_ERROR,
        "safety_status": PROBE_ERROR,
        "pass_equivalent": False,
        "needs_human": True,
        "remedy": "rerun_live_probe",
        "request_count": int(request_count),
        "network": bool(network),
        "written": False,
        "evidence_current": False,
        "error": {
            "type": type(exc).__name__,
            "message": fib.without_authority(str)(exc)[:500],
        },
    })
    return row


_ADDSTOCK_RECONCILE_STATUSES = frozenset({
    "settled", "corrected", "unresolved", "stale", "cancelled",
    "ambiguous",
})


def _addstock_reconcile_error(ticker, row, exc, *, ambiguous=False):
    try:
        message = f"{type(exc).__name__}: {exc}"[:500]
    except Exception:  # noqa: BLE001 - even hostile exceptions stay bounded
        message = type(exc).__name__[:500]
    result = {
        "status": "ambiguous" if ambiguous else "unresolved",
        "ticker": str(ticker),
        "kind": row.get("kind_token"),
        "kind_token": row.get("kind_token"),
        "day": row.get("day"),
        "request_count": 0,
        "queue_resolved": False,
        "error": message,
    }
    if ambiguous:
        result["request_count_unknown"] = True
    return result


def _commit_text(value, label, *, limit=320):
    if (not isinstance(value, str) or not value
            or len(value) > limit or any(c in value for c in "\r\n\x00")):
        raise EvidenceError(f"Add Stocks reconcile {label} is invalid")
    return value


def _commit_sha(value, label, *, nullable=False):
    if value is None and nullable:
        return None
    if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
        raise EvidenceError(f"Add Stocks reconcile {label} is invalid")
    return value


def _commit_number(value, label, *, nullable=False):
    if value is None and nullable:
        return None
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(float(value))):
        raise EvidenceError(f"Add Stocks reconcile {label} must be finite")
    return float(value)


def _commit_reasons(reason, reasons):
    if (not isinstance(reason, str) or not reason or len(reason) > 320
            or any(c in reason for c in "\r\n\x00")
            or not isinstance(reasons, list) or not reasons or len(reasons) > 16
            or any(not isinstance(item, str) or not item
                   or len(item) > 320 or any(c in item for c in "\r\n\x00")
                   for item in reasons)
            or reasons != sorted(set(reasons))
            or any(item not in _VOL_ANOMALY_REASONS for item in reasons)
            or reason != ",".join(reasons)):
        raise EvidenceError(
            "Add Stocks reconcile atomic reason fields are invalid")
    return reason, list(reasons)


def _commit_correction(value, *, normalized):
    if not isinstance(value, dict):
        raise EvidenceError(
            "Add Stocks reconcile correction evidence fields are invalid")
    value = dict(value)
    if set(value) != _CORRECTION_FIELDS:
        raise EvidenceError(
            "Add Stocks reconcile correction evidence fields are invalid")
    if value.get("version") != 1 \
            or value.get("type") != "vol_value_refetch_reconcile":
        raise EvidenceError(
            "Add Stocks reconcile correction evidence version/type is invalid")
    if value.get("day") != normalized["day"]:
        raise EvidenceError("Add Stocks reconcile correction day disagrees")
    if value.get("request_count") != 1:
        raise EvidenceError(
            "Add Stocks reconcile correction request count is invalid")
    bars = value.get("bars")
    if type(bars) is not int or not 0 < bars <= 2_000_000:
        raise EvidenceError(
            "Add Stocks reconcile correction bar count is invalid")
    reason, reasons = _commit_reasons(
        value.get("reason"), value.get("reasons"))
    if (normalized.get("reason") != reason
            or normalized.get("reasons") != reasons):
        raise EvidenceError(
            "Add Stocks reconcile correction provenance disagrees")
    what = _commit_text(
        value.get("what_to_show"), "correction what_to_show", limit=64)
    if what not in _VOL_WHAT_TO_SHOW:
        raise EvidenceError(
            "Add Stocks reconcile correction what_to_show is invalid")
    return {
        "version": 1,
        "type": "vol_value_refetch_reconcile",
        "day": normalized["day"],
        "run": _commit_text(value.get("run"), "correction run"),
        "source": _commit_text(value.get("source"), "correction source"),
        "confirmed": _commit_text(
            value.get("confirmed"), "correction confirmed"),
        "what_to_show": what,
        "request_count": 1,
        "bars": bars,
        "old_value": _commit_number(
            value.get("old_value"), "correction old_value", nullable=True),
        "new_value": _commit_number(
            value.get("new_value"), "correction new_value"),
        "old_day_sha256": _commit_sha(
            value.get("old_day_sha256"), "correction old-day SHA-256"),
        "new_day_sha256": _commit_sha(
            value.get("new_day_sha256"), "correction new-day SHA-256"),
        "old_month_sha256": _commit_sha(
            value.get("old_month_sha256"), "correction old-month SHA-256"),
        "new_month_sha256": _commit_sha(
            value.get("new_month_sha256"), "correction new-month SHA-256"),
        "reason": reason,
        "reasons": reasons,
    }


def _retain_atomic_commit(result, normalized):
    """Retain only the exact bounded rollback/current-state evidence schema."""
    present = set(result).intersection(_ATOMIC_REQUIRED_FIELDS)
    if not present:
        return
    if not _ATOMIC_REQUIRED_FIELDS.issubset(result):
        raise EvidenceError(
            "Add Stocks reconcile atomic commit evidence is incomplete")
    state = result.get("commit_state")
    if (not isinstance(state, str)
            or state not in {"rolled_back", "recovery_required"}):
        raise EvidenceError(
            "Add Stocks reconcile atomic commit state is invalid")
    if (normalized["request_count"] != 1 or normalized["queue_resolved"]
            or normalized["status"] != (
                "unresolved" if state == "rolled_back" else "ambiguous")):
        raise EvidenceError(
            "Add Stocks reconcile atomic commit outcome is contradictory")
    rollback = result.get("rollback_verified")
    month_written = result.get("bank_write_completed")
    bank_written = result.get("bank_written")
    recorded = result.get("correction_recorded")
    if (type(rollback) is not bool or type(month_written) is not bool
            or (bank_written is not None and type(bank_written) is not bool)
            or (recorded is not None and type(recorded) is not bool)):
        raise EvidenceError(
            "Add Stocks reconcile atomic commit truth fields are invalid")
    if rollback != (state == "rolled_back"):
        raise EvidenceError(
            "Add Stocks reconcile atomic rollback state disagrees")
    committed_present = "bank_committed" in result
    if isinstance(bank_written, bool):
        if (not committed_present
                or result.get("bank_committed") is not bank_written):
            raise EvidenceError(
                "Add Stocks reconcile atomic bank commit truth disagrees")
    elif committed_present:
        raise EvidenceError(
            "indeterminate bank state cannot claim bank_committed")
    evidence = result.get("commit_evidence")
    if not isinstance(evidence, dict):
        raise EvidenceError(
            "Add Stocks reconcile commit_evidence fields are invalid")
    evidence = dict(evidence)
    if set(evidence) != _COMMIT_EVIDENCE_FIELDS:
        raise EvidenceError(
            "Add Stocks reconcile commit_evidence fields are invalid")
    if (evidence.get("rollback_verified") is not rollback
            or evidence.get("month_write_completed") is not month_written
            or evidence.get("bank_written") is not bank_written
            or evidence.get("correction_recorded") is not recorded):
        raise EvidenceError(
            "Add Stocks reconcile commit_evidence truth disagrees")
    month_state = evidence.get("month_state")
    manifest_state = evidence.get("manifest_state")
    if (not isinstance(month_state, str) or month_state not in _MONTH_STATES
            or not isinstance(manifest_state, str)
            or manifest_state not in _MANIFEST_STATES):
        raise EvidenceError(
            "Add Stocks reconcile commit_evidence state is invalid")
    expected_bank_written = {
        "original": False,
        "corrected": True,
        "corrected-active-original-retained": True,
        "indeterminate": None,
    }[month_state]
    expected_correction_recorded = {
        "original": False,
        "corrected": True,
        "other-valid": False,
        "unreadable": None,
        "malformed": None,
    }[manifest_state]
    if (bank_written is not expected_bank_written
            or recorded is not expected_correction_recorded):
        raise EvidenceError(
            "Add Stocks reconcile commit_evidence state/truth disagrees")
    correction = _commit_correction(
        evidence.get("correction"), normalized=normalized)
    errors = evidence.get("restore_errors")
    if (not isinstance(errors, list) or len(errors) > 4
            or any(not isinstance(item, str) or len(item) > 320
                   or any(c in item for c in "\r\n\x00") for item in errors)):
        raise EvidenceError(
            "Add Stocks reconcile restore error evidence is invalid")
    original_file = _commit_text(
        evidence.get("original_month_file"), "original month filename",
        limit=255)
    written_file = _commit_text(
        evidence.get("written_month_file"), "written month filename",
        limit=255)
    if any(c in original_file + written_file for c in "/\\"):
        raise EvidenceError(
            "Add Stocks reconcile month evidence filename is invalid")
    safe = {
        "rollback_verified": rollback,
        "month_write_completed": month_written,
        "bank_written": bank_written,
        "correction_recorded": recorded,
        "month_state": month_state,
        "manifest_state": manifest_state,
        "original_month_file": original_file,
        "written_month_file": written_file,
        "old_month_sha256": _commit_sha(
            evidence.get("old_month_sha256"), "old month SHA-256"),
        "new_month_sha256": _commit_sha(
            evidence.get("new_month_sha256"), "new month SHA-256"),
        "observed_original_sha256": _commit_sha(
            evidence.get("observed_original_sha256"),
            "observed original SHA-256", nullable=True),
        "observed_written_sha256": _commit_sha(
            evidence.get("observed_written_sha256"),
            "observed written SHA-256", nullable=True),
        "correction": correction,
        "restore_errors": list(errors),
    }
    if (correction["old_month_sha256"] != safe["old_month_sha256"]
            or correction["new_month_sha256"] != safe["new_month_sha256"]):
        raise EvidenceError(
            "Add Stocks reconcile commit_evidence correction SHA disagrees")
    if state == "rolled_back" and not (
            bank_written is False and recorded is False
            and month_state == manifest_state == "original"):
        raise EvidenceError(
            "Add Stocks reconcile verified rollback evidence is contradictory")
    try:
        encoded = json.dumps(
            safe, ensure_ascii=False, allow_nan=False,
            separators=(",", ":"), sort_keys=True).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise EvidenceError(
            "Add Stocks reconcile commit_evidence is not JSON-safe") from exc
    if len(encoded) > MAX_COMMIT_EVIDENCE_BYTES:
        raise EvidenceError(
            "Add Stocks reconcile commit_evidence exceeds its byte limit")
    normalized.update({
        "commit_state": state,
        "rollback_verified": rollback,
        "bank_write_completed": month_written,
        "bank_written": bank_written,
        "correction_recorded": recorded,
        "commit_evidence": safe,
    })


def _normalize_addstock_plan_row(ticker, value):
    """Detach one exact durable queue row; never retain callback extras."""
    if not isinstance(value, dict):
        raise EvidenceError("Add Stocks reconcile plan row must be an object")
    row = dict(value)
    fields = frozenset(row)
    base = _RECONCILE_QUEUE_BASE_FIELDS
    changed = _RECONCILE_QUEUE_CHANGED_FIELDS
    if fields not in {base, base | changed}:
        raise EvidenceError("Add Stocks reconcile plan row fields are invalid")
    raw_ticker = row.get("ticker")
    try:
        canonical_ticker = storage.canonical_ticker(raw_ticker)
    except Exception as exc:  # noqa: BLE001 - normalize the callback boundary
        raise EvidenceError(
            "Add Stocks reconcile plan ticker is invalid") from exc
    if raw_ticker != canonical_ticker or canonical_ticker != str(ticker):
        raise EvidenceError(
            "Add Stocks reconcile plan ticker disagrees")
    token = row.get("kind_token")
    if (not isinstance(token, str) or not token
            or token != token.strip()
            or storage.INTERVAL_RE.fullmatch(token) is None
            or storage.kind_of(token) not in storage.RATIO_KINDS
            or row.get("kind") != token):
        raise EvidenceError(
            "Add Stocks reconcile plan kind fields are invalid")
    raw_day = row.get("day")
    if not isinstance(raw_day, str):
        raise EvidenceError("Add Stocks reconcile plan day is invalid")
    try:
        parsed_day = dt.date.fromisoformat(raw_day)
    except ValueError as exc:
        raise EvidenceError("Add Stocks reconcile plan day is invalid") from exc
    if raw_day != parsed_day.isoformat():
        raise EvidenceError("Add Stocks reconcile plan day is invalid")
    reasons = row.get("reasons")
    if (not isinstance(reasons, list) or not reasons or len(reasons) > 16
            or any(not isinstance(item, str) or not item
                   or len(item) > 320 or item != item.strip()
                   or any(c in item for c in "\r\n\x00")
                   for item in reasons)
            or reasons != sorted(set(reasons))
            or any(item not in _VOL_ANOMALY_REASONS for item in reasons)
            or row.get("reason") != ",".join(reasons)):
        raise EvidenceError(
            "Add Stocks reconcile plan reason fields are invalid")
    current = row.get("value")
    if current is not None:
        if (isinstance(current, bool)
                or not isinstance(current, (int, float))
                or not math.isfinite(float(current))):
            raise EvidenceError(
                "Add Stocks reconcile plan value is invalid")
        current = float(current)
    normalized = {
        "ticker": canonical_ticker,
        "kind": token,
        "kind_token": token,
        "day": raw_day,
        "value": current,
        "reason": row["reason"],
        "reasons": list(reasons),
    }
    if fields == base | changed:
        prior = row.get("settled_value")
        if (row.get("settled_value_changed") is not True
                or isinstance(prior, bool)
                or not isinstance(prior, (int, float))
                or not math.isfinite(float(prior))):
            raise EvidenceError(
                "Add Stocks reconcile prior settlement is invalid")
        normalized.update({
            "settled_value_changed": True,
            "settled_value": float(prior),
        })
    return normalized


def _normalize_addstock_reconcile(ticker, row, outcome):
    """Validate a callback outcome completely before scheduler mutation."""
    if not isinstance(outcome, dict):
        raise TypeError("Add Stocks reconcile must return an object")
    result = dict(outcome)
    expected = (
        str(ticker), str(row.get("kind_token") or ""),
        str(row.get("day") or ""),
    )
    actual = (
        str(result.get("ticker") or "").upper(),
        str(result.get("kind_token") or result.get("kind") or ""),
        str(result.get("day") or ""),
    )
    if actual != expected:
        raise EvidenceError("Add Stocks reconcile result identity disagrees")
    status = str(result.get("status") or "").strip().lower()
    if status not in _ADDSTOCK_RECONCILE_STATUSES:
        raise EvidenceError("Add Stocks reconcile result status is invalid")
    request_count = result.get("request_count")
    if (isinstance(request_count, bool) or not isinstance(request_count, int)
            or request_count not in (0, 1)):
        raise EvidenceError(
            "Add Stocks reconcile request_count must be 0 or 1")
    queue_resolved = result.get("queue_resolved")
    if not isinstance(queue_resolved, bool):
        raise EvidenceError(
            "Add Stocks reconcile queue_resolved must be boolean")
    if queue_resolved and status not in {"settled", "corrected"}:
        raise EvidenceError(
            "only settled/corrected outcomes may resolve queue debt")
    if status in {"settled", "corrected"} and request_count != 1:
        raise EvidenceError(
            "settled/corrected outcomes require exactly one request")
    request_unknown = result.get("request_count_unknown", False)
    if not isinstance(request_unknown, bool):
        raise EvidenceError(
            "Add Stocks request_count_unknown must be boolean")
    if request_unknown and request_count:
        raise EvidenceError(
            "Add Stocks request count cannot be both known and unknown")
    normalized = {
        "status": status, "ticker": expected[0],
        "kind": expected[1], "kind_token": expected[1],
        "day": expected[2], "request_count": request_count,
        "request_count_unknown": request_unknown,
        "queue_resolved": queue_resolved,
    }
    error = result.get("error")
    if error is not None:
        if not isinstance(error, str):
            raise EvidenceError("Add Stocks reconcile error must be text")
        normalized["error"] = error[:500]
    for field in ("stored_value", "served_value"):
        if field not in result:
            continue
        value = result[field]
        if field == "stored_value" and value is None:
            normalized[field] = None
            continue
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(float(value))):
            raise EvidenceError(
                f"Add Stocks reconcile {field} must be finite")
        normalized[field] = float(value)
    for field in (
            "same_value", "same_day", "bank_committed",
            "registry_committed"):
        if field not in result:
            continue
        if not isinstance(result[field], bool):
            raise EvidenceError(
                f"Add Stocks reconcile {field} must be boolean")
        normalized[field] = result[field]
    if status == "settled" and normalized.get("registry_committed") is not True:
        raise EvidenceError(
            "settled Add Stocks rows require a committed registry entry")
    if status != "settled" and normalized.get("registry_committed") is True:
        raise EvidenceError(
            "Add Stocks registry commit truth disagrees with status")
    if status == "corrected" and normalized.get("bank_committed") is not True:
        raise EvidenceError(
            "corrected Add Stocks rows require committed bank bytes")
    if queue_resolved and not (
            (status == "settled"
             and normalized.get("registry_committed") is True)
            or (status == "corrected"
                and normalized.get("bank_committed") is True)):
        raise EvidenceError(
            "Add Stocks queue retirement lacks durable action proof")
    if "what_to_show" in result:
        value = result["what_to_show"]
        if (not isinstance(value, str) or not value or len(value) > 64
                or any(c in value for c in "\r\n\x00")):
            raise EvidenceError("Add Stocks reconcile what_to_show is invalid")
        normalized["what_to_show"] = value
    if "month_sha256" in result:
        value = result["month_sha256"]
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise EvidenceError("Add Stocks reconcile month_sha256 is invalid")
        normalized["month_sha256"] = value
    if "reason" in result:
        value = result["reason"]
        if (not isinstance(value, str) or len(value) > 320
                or any(c in value for c in "\r\n\x00")):
            raise EvidenceError("Add Stocks reconcile reason is invalid")
        normalized["reason"] = value
    if "reasons" in result:
        value = result["reasons"]
        if (not isinstance(value, list) or len(value) > 16
                or any(not isinstance(item, str) or not item
                       or len(item) > 320
                       or any(c in item for c in "\r\n\x00")
                       for item in value)
                or value != sorted(set(value))):
            raise EvidenceError("Add Stocks reconcile reasons are invalid")
        normalized["reasons"] = list(value)
    if ("reason" in result) != ("reasons" in result):
        raise EvidenceError(
            "Add Stocks reconcile reason fields must travel together")
    if ("reason" in normalized and "reasons" in normalized
            and (normalized["reason"] != ",".join(normalized["reasons"])
                 or any(item not in _VOL_ANOMALY_REASONS
                        for item in normalized["reasons"]))):
        raise EvidenceError("Add Stocks reconcile reason fields disagree")
    try:
        _retain_atomic_commit(result, normalized)
    except EvidenceError:
        raise
    except Exception as exc:  # noqa: BLE001 - hostile callback evidence
        raise EvidenceError(
            "Add Stocks reconcile atomic evidence cannot be normalized") from exc
    if (normalized.get("bank_committed") is True
            and status != "corrected"
            and "commit_state" not in normalized):
        raise EvidenceError(
            "Add Stocks bank commit proof requires correction/recovery evidence")
    return normalized


def _addstock_observer_failure(error):
    """Bound diagnostics without losing either an original or new stop signal."""
    terminal_types = (AuthorityError, LedgerError, RequestCancelled)
    hard = error if isinstance(error, terminal_types) or not isinstance(error, Exception) else None
    try:
        detail = fib.without_authority(lambda: f"{type(error).__name__}: {error}"[:500])()
    except BaseException as formatting:
        if hard is None and (isinstance(formatting, terminal_types)
                             or not isinstance(formatting, Exception)):
            hard = formatting
        detail = "observer failure; diagnostic unavailable"
    return detail, hard


class AddStockProbeCoordinator:
    """Thread-safe ticker boundary for embedded Add Stocks WS8 work.

    Fetch workers publish terminal series rows. A ticker becomes check-ready
    only after every selected series has completed successfully; a later
    recovery success replaces an earlier halt. Workers claim checks before
    probes, and probes are capped at two while checks remain.
    """

    def __init__(self, selections, *, check_fn, probe_fn=None, seed, cancel=None,
                 progress=None, reconcile_plan_fn=None, reconcile_fn=None,
                 reconcile_run_id=None, pause=None, engine_requests=False):
        if type(engine_requests) is not bool:
            raise TypeError("engine_requests must be a boolean")
        if engine_requests and (probe_fn is not None or reconcile_fn is not None):
            raise TypeError("engine request mode accepts no injected request callbacks")
        if not callable(check_fn) or (not engine_requests and not callable(probe_fn)):
            raise TypeError("Add Stocks check/probe callbacks must be callable")
        if engine_requests:
            if reconcile_plan_fn is not None and not callable(reconcile_plan_fn):
                raise TypeError("Add Stocks reconciliation plan must be callable")
        elif ((reconcile_plan_fn is None) != (reconcile_fn is None)
                or (reconcile_plan_fn is not None
                    and (not callable(reconcile_plan_fn)
                         or not callable(reconcile_fn)))):
            raise TypeError(
                "Add Stocks reconciliation requires callable plan/run callbacks")
        expected = {}
        for ticker, interval in selections:
            ticker = str(ticker or "").strip().upper()
            interval = str(interval or "").strip()
            if not ticker or not interval:
                raise ValueError("Add Stocks selections require ticker/interval")
            expected.setdefault(ticker, set()).add((ticker, interval))
        self._expected = expected
        self._check_fn = check_fn
        self._engine_requests = engine_requests
        self._probe_fn = probe_fn
        self._seed = normalize_seed(seed)
        self._cancel = cancel
        self._pause = pause
        self._progress = progress
        self._reconcile_plan_fn = reconcile_plan_fn
        self._reconcile_fn = reconcile_fn
        self._reconcile_run_id = str(
            reconcile_run_id or f"add-stocks-{self._seed}")
        self._reconcile_enabled = reconcile_plan_fn is not None
        self._lock = threading.Lock()
        self._results = {}
        self._reconcile_plan_ready = deque()
        self._reconcile_ready = deque()
        self._reconcile_pending = {}
        self._reconcile_active_rows = {}
        self._reconcile_active_plan_tickers = set()
        self._reconcile_manifest_locks = {
            ticker: threading.Lock() for ticker in expected}
        self._check_ready = deque()
        self._probe_ready = deque()
        self._scheduled = set()
        self._active_checks = 0
        self._active_probes = 0
        self._active_reconcile_plans = 0
        self._active_reconciles = 0
        self._rows = []
        self._reconcile_rows = []
        self._reconcile_rows_truncated = 0
        self._reconcile_abandoned_rows = []
        self._reconcile_abandoned_rows_truncated = 0
        self._reconcile_plan_errors = 0
        self._reconcile_request_unknown = 0
        self._reconcile_status_counts = {}
        self._reconcile_request_count = 0
        self._reconcile_queue_resolved = 0
        self._reconcile_queue_pending = 0
        self._reconcile_planned = 0
        self._reconcile_completed = 0
        self._checked = 0
        self._selected = 0

    def _cancelled(self):
        return self._cancel is not None and self._cancel.is_set()

    def _paused(self):
        return self._pause is not None and self._pause.is_set()

    def _say(self, message):
        if self._progress is not None:
            try:
                self._progress(str(message))
            except Exception as exc:  # Ordinary reporting failures remain non-fatal.
                if self._engine_requests and isinstance(exc, (AuthorityError, LedgerError, RequestCancelled)):
                    raise

    def _append_reconcile_evidence_locked(self, row):
        if len(self._reconcile_rows) < MAX_RECONCILE_RESULTS:
            self._reconcile_rows.append(dict(row))
        else:
            self._reconcile_rows_truncated += 1

    def _append_abandoned_locked(self, row, reason):
        value = dict(row)
        value["debt_reason"] = str(reason)[:64]
        if len(self._reconcile_abandoned_rows) < MAX_RECONCILE_RESULTS:
            self._reconcile_abandoned_rows.append(value)
        else:
            self._reconcile_abandoned_rows_truncated += 1

    def _record_reconcile_locked(self, outcome):
        """No-raise transition for an already normalized exact-row outcome."""
        self._reconcile_completed += 1
        status = outcome["status"]
        self._reconcile_status_counts[status] = (
            self._reconcile_status_counts.get(status, 0) + 1)
        self._reconcile_request_count += outcome["request_count"]
        resolved = outcome["queue_resolved"] is True
        self._reconcile_queue_resolved += int(resolved)
        self._reconcile_queue_pending += int(not resolved)
        self._reconcile_request_unknown += int(
            outcome.get("request_count_unknown") is True)
        self._append_reconcile_evidence_locked(outcome)

    def _abandon_ticker_reconcile_locked(self, ticker, reason):
        pending = self._reconcile_pending.get(ticker)
        while pending:
            self._append_abandoned_locked(pending.popleft(), reason)

    def record_series(self, rows):
        """Publish terminal fill rows; recovery success supersedes a halt."""
        with self._lock:
            touched = set()
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                ticker = str(row.get("ticker") or "").strip().upper()
                interval = str(row.get("interval") or "").strip()
                key = (ticker, interval)
                if key not in self._expected.get(ticker, set()):
                    continue
                previous = self._results.get(key)
                if previous is None or (previous.get("halt")
                                        and not row.get("halt")):
                    self._results[key] = dict(row)
                touched.add(ticker)
            for ticker in sorted(touched):
                if ticker in self._scheduled:
                    continue
                keys = self._expected[ticker]
                if not all(key in self._results for key in keys):
                    continue
                if any(self._results[key].get("halt") for key in keys):
                    continue
                self._scheduled.add(ticker)
                if self._reconcile_enabled:
                    self._reconcile_plan_ready.append(ticker)
                else:
                    self._check_ready.append(ticker)

    def claim(self):
        """Claim one check/probe task according to the WS8 priority rule."""
        with self._lock:
            if self._cancelled() or self._paused():
                return None
            if self._reconcile_plan_ready:
                ticker = self._reconcile_plan_ready.popleft()
                self._active_reconcile_plans += 1
                self._reconcile_active_plan_tickers.add(ticker)
                return ("reconcile-plan", ticker)
            if self._reconcile_ready:
                ticker = self._reconcile_ready.popleft()
                pending = self._reconcile_pending.get(ticker)
                if not pending:
                    raise EvidenceError(
                        "reconcile-ready ticker has no pending queue row")
                row = pending.popleft()
                self._active_reconciles += 1
                self._reconcile_active_rows[ticker] = dict(row)
                return ("reconcile", ticker, dict(row))
            if self._check_ready:
                ticker = self._check_ready.popleft()
                self._active_checks += 1
                return ("check", ticker)
            checks_remain = bool(self._active_checks)
            if self._probe_ready and (not checks_remain
                                      or self._active_probes < 2):
                ticker, selection = self._probe_ready.popleft()
                self._active_probes += 1
                return ("probe", ticker, selection)
            return None

    def drain_checks(self, adapter=None, pacer=None):
        """Finish only ready port-free checks; leave reconciles/probes pending."""
        drained = 0
        while not self._cancelled():
            with self._lock:
                if not self._check_ready:
                    break
                ticker = self._check_ready.popleft()
                self._active_checks += 1
            self.execute(("check", ticker), adapter, pacer)
            drained += 1
        return drained

    def execute(self, task, adapter, pacer):
        """Execute one claimed task with a worker-owned adapter and pacer."""
        phase, ticker = task[:2]
        if phase == "reconcile-plan":
            plan_error = None
            hard_failure = None
            try:
                self._say(f"Volatility audit: {ticker}...")
                with self._lock:
                    series_rows = [dict(self._results[key])
                                   for key in sorted(self._expected[ticker])]
                planned = self._reconcile_plan_fn(ticker, series_rows)
                if isinstance(planned, (str, bytes, dict)):
                    raise TypeError(
                        "Add Stocks reconcile plan must return queue rows")
                planned = [dict(row) for row in islice(
                    iter(planned), MAX_RECONCILE_PLAN + 1)]
                if len(planned) > MAX_RECONCILE_PLAN:
                    raise EvidenceError(
                        "Add Stocks reconcile plan exceeds its item limit")
                normalized_plan = []
                keys = []
                for row in planned:
                    row = _normalize_addstock_plan_row(ticker, row)
                    key = (row["ticker"], row["kind_token"], row["day"])
                    keys.append(key)
                    normalized_plan.append(row)
                if len(keys) != len(set(keys)):
                    raise EvidenceError(
                        "Add Stocks reconcile plan returned duplicate rows")
                planned = normalized_plan
                planned.sort(key=lambda row: (
                    str(row.get("kind_token") or ""),
                    str(row.get("day") or "")))
            except BaseException as exc:  # Settle counters even if observer diagnostics fail.
                detail, hard_failure = _addstock_observer_failure(exc)
                planned = []
                plan_error = {
                    "status": "ambiguous" if hard_failure is not None else "unresolved", "ticker": ticker,
                    "kind": None, "kind_token": None, "day": None,
                    "request_count": 0, "queue_resolved": False,
                    "error": detail,
                    "phase": "plan",
                }
                if hard_failure is not None:
                    plan_error["request_count_unknown"] = False
            with self._lock:
                self._active_reconcile_plans -= 1
                self._reconcile_active_plan_tickers.discard(ticker)
                self._reconcile_planned += len(planned)
                self._reconcile_pending[ticker] = deque(planned)
                if plan_error is not None:
                    self._reconcile_plan_errors += 1
                    self._append_reconcile_evidence_locked(plan_error)
                if planned:
                    self._reconcile_ready.append(ticker)
                elif hard_failure is None:
                    self._check_ready.append(ticker)
            if hard_failure is not None:
                if self._engine_requests or isinstance(hard_failure, (AuthorityError, LedgerError, RequestCancelled)):
                    raise hard_failure
                raise EvidenceError(
                    "Add Stocks volatility planning ended ambiguously") \
                    from hard_failure
            return
        if phase == "reconcile":
            options = self.request_options(task)
            try:
                if self._reconcile_fn is None:
                    raise RequestRefused("Add Stocks reconcile requires fixed engine dispatch")
                outcome = self._reconcile_fn(
                    adapter, pacer, dict(task[2]), options["run_id"],
                    manifest_lock=options["manifest_lock"])
            except BaseException as exc:
                return self.complete_request(task, error=exc)
            return self.complete_request(task, outcome)
        if phase == "check":
            hard_failure = None
            try:
                self._say(f"WS8 check: {ticker}...")
                with self._lock:
                    series_rows = [dict(self._results[key])
                                   for key in sorted(self._expected[ticker])]
                outcome = self._check_fn(
                    adapter, pacer, ticker, series_rows, self._seed)
                if not isinstance(outcome, dict):
                    raise TypeError("Add Stocks check must return an object")
                # Resolve caller-owned mappings before touching counters. Only
                # detached plain data enters the final bookkeeping section.
                selection = outcome.get("selection")
                row = outcome.get("row")
                if selection is not None and row is not None:
                    raise EvidenceError("check returned both selection and row")
                if selection is not None:
                    selection = dict(selection)
                    row = None
                elif isinstance(row, dict):
                    row = dict(row)
                else:
                    raise EvidenceError("check returned no probe outcome")
            except BaseException as exc:  # Bounded evidence, with terminal identity retained.
                detail, hard_failure = _addstock_observer_failure(exc)
                selection = None
                row = _probe_error_row({"ticker": ticker}, EvidenceError(detail),
                                       request_count=0, network=False)
                row["error"]["type"] = type(exc).__name__
            with self._lock:
                self._active_checks -= 1
                self._checked += 1
                if selection is not None:
                    self._selected += 1
                    self._probe_ready.append((ticker, selection))
                else:
                    self._rows.append(row)
            if hard_failure is not None:
                raise hard_failure
            return
        if phase != "probe":
            raise ValueError(f"unknown Add Stocks WS8 phase: {phase}")
        selection = task[2]
        self.request_options(task)
        try:
            if self._probe_fn is None:
                raise RequestRefused("Add Stocks probe requires fixed engine dispatch")
            row = self._probe_fn(adapter, pacer, ticker, selection)
        except BaseException as exc:
            return self.complete_request(task, error=exc)
        return self.complete_request(task, row)

    def request_options(self, task):
        """Observer-only lifecycle data; never a callable request dispatcher."""
        phase, ticker = task[:2]
        if phase == "reconcile":
            row = task[2]
            self._say(f"Volatility reconcile: {ticker} "
                      f"{row.get('kind_token')} {row.get('day')}...")
            return {"run_id": self._reconcile_run_id,
                    "manifest_lock": self._reconcile_manifest_locks[ticker]}
        if phase == "probe":
            self._say(f"WS8 probe: {ticker}...")
            return {}
        raise ValueError("only request phases have request options")

    def complete_request(self, task, outcome=None, *, error=None):
        """Record a fixed engine or legacy observer result without authority.

        An escaped request cannot prove how many sends/commits occurred. Keep
        ambiguous debt and preserve typed terminal failures after bookkeeping.
        """
        phase, ticker = task[:2]
        if phase not in {"probe", "reconcile"}:
            raise ValueError("only request phases have request results")
        hard_failure = None
        try:
            if error is not None:
                raise error
            if phase == "reconcile":
                outcome = _normalize_addstock_reconcile(ticker, task[2], outcome)
            elif not isinstance(outcome, dict):
                raise TypeError("Add Stocks probe must return an object")
            else:
                outcome = dict(outcome)
        except BaseException as exc:
            if (isinstance(exc, (AuthorityError, LedgerError, RequestCancelled))
                    or not isinstance(exc, Exception)):
                hard_failure = exc
            def error_row(reason):
                if phase == "reconcile":
                    return _addstock_reconcile_error(
                        ticker, task[2], reason, ambiguous=True)
                return _probe_error_row(
                    {"ticker": ticker, "day": task[2].get("day")}, reason,
                    request_count=0, network=False)
            try:
                outcome = error_row(exc)
            except BaseException as formatting:
                # Formatting is observer code too. It cannot strand an active
                # task or replace an already established terminal failure.
                if hard_failure is None and (
                        isinstance(formatting, (AuthorityError, LedgerError, RequestCancelled))
                        or not isinstance(formatting, Exception)):
                    hard_failure = formatting
                outcome = error_row(EvidenceError("request error details unavailable"))
            if phase == "probe":
                # Zero is not proof of no request when control escaped.
                outcome["request_count_unknown"] = True
        with self._lock:
            if phase == "probe":
                self._active_probes -= 1
                self._rows.append(outcome)
            else:
                self._active_reconciles -= 1
                self._reconcile_active_rows.pop(ticker, None)
                self._record_reconcile_locked(outcome)
                pending = self._reconcile_pending.get(ticker)
                if hard_failure is not None:
                    self._abandon_ticker_reconcile_locked(
                        ticker, "worker-failed-unstarted")
                elif pending:
                    # One ticker is re-enqueued only after its prior row
                    # completed, preventing cross-worker month/manifest races.
                    self._reconcile_ready.append(ticker)
                else:
                    self._check_ready.append(ticker)
        if hard_failure is not None:
            if isinstance(hard_failure, (AuthorityError, LedgerError, RequestCancelled)):
                raise hard_failure
            raise EvidenceError("Add Stocks request ended ambiguously") from hard_failure

    def has_pending(self):
        with self._lock:
            return bool(
                self._reconcile_plan_ready or self._reconcile_ready
                or self._active_reconcile_plans or self._active_reconciles
                or self._check_ready or self._probe_ready
                or self._active_checks or self._active_probes)

    def status_snapshot(self):
        """Return observational WS8 queue/active/probe progress counters.

        The completed/total pair is the coordinator's existing run-level
        result ledger: every expected ticker contributes exactly one terminal
        row, whether its check finishes without a live request or a selected
        probe finishes later.  The denominator is therefore stable from the
        start instead of growing as checks select live probes.
        """
        with self._lock:
            result = {
                "expected": len(self._expected),
                "scheduled": len(self._scheduled),
                "fill_waiting": max(
                    0, len(self._expected) - len(self._scheduled)),
                "check_ready": len(self._check_ready),
                "active_checks": self._active_checks,
                "checked": self._checked,
                "probe_ready": len(self._probe_ready),
                "active_probes": self._active_probes,
                "probe_done": len(self._rows),
                "probe_total": len(self._expected),
            }
            if self._reconcile_enabled:
                result.update({
                    "reconcile_plan_ready": len(self._reconcile_plan_ready),
                    "active_reconcile_plans": self._active_reconcile_plans,
                    "reconcile_ready": sum(
                        len(rows) for rows in self._reconcile_pending.values()),
                    "active_reconciles": self._active_reconciles,
                    "reconcile_planned": self._reconcile_planned,
                    "reconcile_completed": self._reconcile_completed,
                })
            return result

    def reconcile_result(self, *, skip_reason=None, down_ports=None):
        """Return reconciliation evidence separately from the WS8 ledger."""
        with self._lock:
            rows = [dict(row) for row in self._reconcile_rows]
            pending = []
            pending_seen = 0

            def retain_pending(row):
                nonlocal pending_seen
                pending_seen += 1
                if len(pending) < MAX_RECONCILE_RESULTS:
                    pending.append(dict(row))

            for row in self._reconcile_active_rows.values():
                retain_pending(row)
            for ticker in sorted(self._reconcile_pending):
                for row in self._reconcile_pending[ticker]:
                    retain_pending(row)
            work_pending_count = pending_seen
            for row in self._reconcile_abandoned_rows:
                retain_pending(row)
            plan_pending = sorted(
                set(self._reconcile_plan_ready)
                | self._reconcile_active_plan_tickers)
            unscheduled = (sorted(set(self._expected) - self._scheduled)
                           if self._reconcile_enabled else [])
            halted_fill = []
            pending_fill = []
            for ticker in unscheduled:
                keys = self._expected[ticker]
                if (all(key in self._results for key in keys)
                        and any(self._results[key].get("halt") for key in keys)):
                    halted_fill.append(ticker)
                else:
                    pending_fill.append(ticker)
            statuses = dict(self._reconcile_status_counts)
            row_unresolved = sum(
                count for status, count in statuses.items()
                if status not in {"settled", "corrected"})
            queue_pending = max(
                0, self._reconcile_planned
                - self._reconcile_queue_resolved)
            result = {
                "kind": "vol_value_reconcile",
                "version": 1,
                "enabled": self._reconcile_enabled,
                "planned_count": self._reconcile_planned,
                "completed_count": self._reconcile_completed,
                "request_count": self._reconcile_request_count,
                "request_count_unknown": self._reconcile_request_unknown,
                "settled_count": statuses.get("settled", 0),
                "corrected_count": statuses.get("corrected", 0),
                "unresolved_count": row_unresolved,
                "plan_error_count": self._reconcile_plan_errors,
                "queue_resolved_count": self._reconcile_queue_resolved,
                "queue_pending_count": queue_pending,
                "rows": rows,
                "rows_truncated": self._reconcile_rows_truncated,
                "pending_rows": pending,
                "pending_rows_truncated": max(
                    0, pending_seen - len(pending))
                    + self._reconcile_abandoned_rows_truncated,
                "pending_plan_tickers": plan_pending[:MAX_RECONCILE_RESULTS],
                "pending_plan_tickers_truncated": max(
                    0, len(plan_pending) - MAX_RECONCILE_RESULTS),
                "pending_fill_tickers": pending_fill[:MAX_RECONCILE_RESULTS],
                "pending_fill_tickers_truncated": max(
                    0, len(pending_fill) - MAX_RECONCILE_RESULTS),
                "halted_fill_tickers": halted_fill[:MAX_RECONCILE_RESULTS],
                "halted_fill_tickers_truncated": max(
                    0, len(halted_fill) - MAX_RECONCILE_RESULTS),
            }
            has_pending_work = bool(
                work_pending_count or plan_pending or self._active_reconcile_plans
                or self._active_reconciles or pending_fill)
            has_unresolved_debt = bool(
                row_unresolved or self._reconcile_plan_errors
                or queue_pending
                or self._reconcile_request_unknown
                or self._reconcile_abandoned_rows_truncated or halted_fill)
            if skip_reason and has_pending_work:
                result.update({
                    "status": "pending",
                    "skip_reason": str(skip_reason)[:64],
                    "down_ports": sorted({int(port) for port in
                                           (down_ports or [])}),
                })
            elif has_pending_work:
                result["status"] = "pending"
            elif has_unresolved_debt:
                result["status"] = "incomplete"
            else:
                result["status"] = "complete"
            return result

    def result(self, *, skip_reason=None, down_ports=None):
        """Return bounded parent evidence, including unprocessed tickers."""
        with self._lock:
            rows = [dict(row) for row in self._rows]
            completed = {str(row.get("ticker") or "").upper()
                         for row in rows}
            skipped = 0
            for ticker in sorted(self._expected):
                if ticker in completed:
                    continue
                if skip_reason:
                    skipped += 1
                    continue
                missing = [key for key in sorted(self._expected[ticker])
                           if key not in self._results]
                halted = [key for key in sorted(self._expected[ticker])
                          if self._results.get(key, {}).get("halt")]
                if self._cancelled():
                    reason = "parent run cancelled before WS8 completion"
                elif halted:
                    reason = "ticker fill did not complete successfully"
                elif missing:
                    reason = "ticker fill did not publish every selected series"
                else:
                    reason = "WS8 work remained unfinished"
                rows.append(_probe_error_row(
                    {"ticker": ticker}, EvidenceError(reason),
                    request_count=0, network=False))
            rows.sort(key=lambda row: (str(row.get("ticker") or ""),
                                       str(row.get("day") or "")))
            result = {
                "seed": self._seed,
                "requested_count": len(self._expected),
                "checked_count": self._checked,
                "selected_count": self._selected,
                "completed_count": len(rows),
                "request_count": sum(int(row.get("request_count") or 0)
                                     for row in rows),
                "reused_request_count": sum(int(
                    row.get("reused_request_count") or 0) for row in rows),
                "issue_count": sum(row.get("pass_equivalent") is not True
                                   for row in rows),
                "rows": rows,
            }
            if skip_reason:
                result.update({
                    "status": "skipped",
                    "skip_reason": str(skip_reason)[:64],
                    "skipped_count": skipped,
                    "down_ports": sorted({int(port) for port in
                                           (down_ports or [])}),
                })
            return result


def fleet_probe_gate(report, ports):
    """Return an aggregate skip when every configured worker port is down."""
    configured = sorted({int(port) for port in ports})
    if not configured or not isinstance(report, dict):
        return {}
    raw = report.get("aborted_ports")
    if not isinstance(raw, (dict, list, tuple, set)):
        return {}
    down = {int(port) for port in raw if type(port) is int}
    if not set(configured).issubset(down):
        return {}
    return {"skip_reason": "fleet_down", "down_ports": configured}


def _fetch_method(fetcher, interval="1m"):
    interval = _probe_interval(interval)
    method_name = "fetch_day" if interval == "1m" else "fetch_span"
    method = getattr(fetcher, method_name, None)
    if method is None and callable(fetcher):
        method = fetcher
    if not callable(method):
        raise LiveFetchError("live fetcher is not callable")
    return method


def _reuse_document(path):
    with Path(path).open("rb") as stream:
        payload = stream.read(MAX_ARTIFACT_BYTES + 1)
    if len(payload) > MAX_ARTIFACT_BYTES:
        raise EvidenceError("reused evidence file exceeds the size limit")
    value = strict_json(payload)
    if not isinstance(value, dict):
        raise EvidenceError("reused evidence file must be an object")
    return value, payload


def _verified_reused_result(evidence, snapshot):
    """Read-only source proof; never create a request or trust supplied flags."""
    live = evidence.get("live_result")
    provenance = live.get("fetch_provenance") if isinstance(live, dict) else None
    if not isinstance(provenance, dict):
        raise EvidenceError("reused evidence lacks source provenance")
    locator = evidence.get("source_companion_path", provenance.get("source_companion_path"))
    if not isinstance(locator, str) or not locator:
        raise EvidenceError("reused evidence lacks its source companion")
    companion_path = Path(locator).resolve()
    companion, _ = _reuse_document(companion_path)
    required = {"report_schema_version": 1, "purpose": "diagnostic",
                "verified": True, "ledger_verified": True, "state": "VERIFIED",
                "outcome": "returned", "report_persisted": True, "artifact_state": "written"}
    if any(type(companion.get(key)) is not type(value) or companion[key] != value
           for key, value in required.items()) or any(companion.get(key) for key in
           ("failure", "publication_refusals", "outcome_unknown", "inspection_error")):
        raise EvidenceError("reused source companion is not verified completion")
    operation_id = provenance.get("operation_id")
    if (companion.get("operation_id") != operation_id
            or companion_path.name != str(operation_id) + ".operation.json"):
        raise EvidenceError("reused source operation identity mismatch")
    paths = {}
    for kind in ("ledger", "artifact"):
        relative = companion.get(kind + "_relative_path")
        # A cross-volume artifact has no relative path. Its current locator
        # must be explicit; never fall back to the historical absolute path.
        if kind == "artifact" and relative is None and evidence.get("source_artifact_path"):
            paths[kind] = Path(evidence["source_artifact_path"]).resolve()
            continue
        if not isinstance(relative, str) or not relative or Path(relative).is_absolute() or Path(relative).drive:
            raise EvidenceError("reused source requires relative bundle references")
        paths[kind] = (companion_path.parent / relative).resolve()
    if (str(companion.get("path", "")).replace("\\", "/").rsplit("/", 1)[-1] != paths["ledger"].name
            or companion.get("path") != provenance.get("ledger_path")):
        raise EvidenceError("reused source ledger identity mismatch")
    inspected = inspect_ledger(paths["ledger"])
    if inspected["operation_id"] != operation_id:
        raise EvidenceError("reused sealed ledger operation mismatch")
    pair = [event for event in inspected["events"]
            if event.get("logical_id") == provenance.get("logical_id")]
    if (len(pair) != 2 or [event["event"] for event in pair] != ["decision", "result"]
            or any(event["attempt_id"] != provenance.get("attempt_id")
                   or event["producer_id"] != provenance.get("producer_id") for event in pair)):
        raise EvidenceError("reused evidence lacks its exact durable attempt")
    decision, result = pair[0]["payload"], pair[1]["payload"]
    token = _probe_interval(snapshot.get("interval", "1m"))
    producer = "ibkr.live_spot_probe.minute" if token == "1m" else "ibkr.live_spot_probe.daily"
    if (provenance.get("producer_id") != producer or result.get("outcome") not in {"returned", "empty"}
            or _canonical_json(result.get("accepted")) != _canonical_json(provenance.get("accepted"))
            or _canonical_json(result.get("observed", {}).get("count")) != _canonical_json(provenance.get("raw_bar_count"))
            or _canonical_json(result.get("dropped")) != _canonical_json(provenance.get("dropped"))
            or provenance.get("captured_now") != companion.get("captured_now")):
        raise EvidenceError("reused result differs from durable source evidence")
    for name in ("requested", "effective"):
        envelope = decision.get(name)
        if (not isinstance(envelope, dict) or _canonical_json(envelope) != _canonical_json(provenance.get(name))
                or envelope.get("token") != token or envelope.get("con_id") != _conid(snapshot.get("conid"))
                or envelope.get("symbol") != snapshot.get("ticker")):
            raise EvidenceError("reused request identity or range mismatch")
    if (provenance.get("coverage_complete") is not True
            or decision["requested"]["intended_start"] != decision["effective"]["intended_start"]
            or decision["requested"]["intended_end"] != decision["effective"]["intended_end"]):
        raise EvidenceError("reused request coverage is incomplete")
    artifact, payload = _reuse_document(paths["artifact"])
    if hashlib.sha256(payload).hexdigest() != companion.get("artifact_sha256"):
        raise EvidenceError("reused artifact hash mismatch")
    pending = artifact.get("fetch_ledger", {})
    if (pending.get("operation_id") != operation_id or pending.get("companion_required") is not True
            or pending.get("outcome") != "companion_pending" or artifact.get("status") != "complete"):
        raise EvidenceError("reused artifact source completion mismatch")
    rows = [row for row in artifact.get("probes", []) if isinstance(row, dict)
            and _canonical_json(row.get("fetch_provenance")) == _canonical_json(provenance)]
    if len(rows) != 1:
        raise EvidenceError("reused artifact lacks its unique source row")
    row = rows[0]
    expected = _public_snapshot(snapshot)
    # A stored snapshot describes no request; its published source row does.
    expected.update(request_count=1, network=True)
    if _canonical_json({key: row.get(key) for key in expected}) != _canonical_json(expected):
        mismatched = [key for key in expected if _canonical_json(row.get(key)) != _canonical_json(expected[key])]
        raise EvidenceError("reused artifact does not match the current stored snapshot: " + ", ".join(mismatched))
    normalized = (_normalize_live_result(live) if token == "1m"
                  else _normalize_daily_live_result(live, snapshot))
    projection = {"live_digest": _live_digest(normalized["closes"]), "live_volume": normalized["volume"],
                  "raw_live_bars": normalized["raw_bar_count"],
                  "accepted_live_bars": normalized["accepted_bar_count"],
                  "discarded_live_bars": normalized["discarded"],
                  "port": normalized.get("port"), "client_id": normalized.get("client_id")}
    if _canonical_json({key: row.get(key) for key in projection}) != _canonical_json(projection):
        raise EvidenceError("reused live projection differs from the published source")
    # Detached validated values only; preserve source IDs, never relabel them
    # with a current parent's identity or obtain another pacing turn.
    normalized["fetch_provenance"] = strict_json(_canonical_json(provenance).encode("utf-8"))
    return normalized


@fib.without_authority
def _reusable_live_result(evidence, snapshot):
    if not isinstance(evidence, dict):
        raise EvidenceError("reused live evidence must be an object")
    if evidence.get("read_only") is not True:
        raise EvidenceError("reused live evidence must be read-only")
    if evidence.get("used_for_commit") is not False:
        raise EvidenceError("commit-derived live evidence cannot be reused")
    if evidence.get("fetched_after_boundary") is not True:
        raise EvidenceError(
            "reused live evidence must follow the repair boundary")
    expected = {
        "ticker": snapshot.get("ticker"),
        "day": snapshot.get("day"),
        "manifest_fingerprint": snapshot.get("manifest_fingerprint"),
    }
    actual = {key: evidence.get(key) for key in expected}
    if _canonical_json(actual) != _canonical_json(expected):
        raise EvidenceError(
            "reused live evidence does not match the stored boundary")
    if "live_result" not in evidence:
        raise EvidenceError("reused live evidence has no live result")
    return _verified_reused_result(evidence, snapshot)


@fib.without_authority
def _reusable_daily_live_result(evidence, snapshot):
    if not isinstance(evidence, dict):
        raise EvidenceError("reused daily evidence must be an object")
    if evidence.get("read_only") is not True:
        raise EvidenceError("reused daily evidence must be read-only")
    if evidence.get("used_for_commit") is not False:
        raise EvidenceError("commit-derived daily evidence cannot be reused")
    if evidence.get("fetched_after_boundary") is not True:
        raise EvidenceError(
            "reused daily evidence must follow the repair boundary")
    interval = _probe_interval(snapshot.get("interval"))
    if not _is_daily_probe(interval):
        raise EvidenceError("reused daily evidence interval is invalid")
    expected = {
        "ticker": snapshot.get("ticker"),
        "interval": interval,
        "anchor_day": snapshot.get("anchor_day"),
        "span_start": snapshot.get("span_start"),
        "span_end": snapshot.get("span_end"),
        "span_days": snapshot.get("span_days"),
        "span_fingerprint": snapshot.get("span_fingerprint"),
        "manifest_fingerprint": snapshot.get("manifest_fingerprint"),
        "stored_digest": snapshot.get("stored_digest"),
        "month_sha256s": snapshot.get("month_sha256s"),
    }
    actual = {key: evidence.get(key) for key in expected}
    if _canonical_json(actual) != _canonical_json(expected):
        raise EvidenceError(
            "reused daily evidence does not match the stored span boundary")
    if "live_result" not in evidence:
        raise EvidenceError("reused daily evidence has no live result")
    return _verified_reused_result(evidence, snapshot)


def run_embedded_probe(*args, _fetch_child=None, **kwargs):
    """Fixed child dispatch, or explicitly request-free reuse of sealed evidence.

    The parent pipeline owns its operation lease, adapter thread, pacing, and
    aggregate artifact. No injected callable receives its child authority.
    """
    live_evidence = kwargs.pop("live_evidence", None)
    if sum(value is not None for value in
           (kwargs.get("adapter"), kwargs.get("live_fetcher"), live_evidence)) != 1:
        reason = "provide exactly one adapter, live fetcher or reusable evidence object"
        if _fetch_child is not None:
            return fib.refuse_transferred_child(_fetch_child, reason)
        raise LiveSpotProbeError(reason)
    if live_evidence is not None:
        if _fetch_child is not None:
            return fib.refuse_transferred_child(_fetch_child, "evidence-only reuse accepts no request child")
        kwargs.pop("adapter", None)
        return fib.without_authority(_embedded_probe_result)(
            *args, live_evidence=live_evidence, **kwargs)
    # Binding happens inside worker_scope, after it assumes only a valid
    # transferred inactive child's cleanup. Malformed arguments cannot leak it.
    return _run_embedded_probe_request(*args, _fetch_child=_fetch_child, **kwargs)


@fib.worker_scope
def _run_embedded_probe_request(root, *, ticker, day, adapter=None,
                                live_fetcher=None, interval="1m", pacer=None, cancel=None):
    interval = fib.without_authority(_probe_interval)(interval)
    expected = IBKRMinuteDayFetcher if interval == "1m" else IBKRDailySpanFetcher
    if adapter is not None:
        live_fetcher = fib.without_authority(expected.from_borrowed_adapter)(adapter, pacer=pacer)
    if type(live_fetcher) is not expected:
        raise RequestRefused("custom embedded fetcher is not an authorized request dispatcher")
    return _embedded_probe_result(root, ticker=ticker, day=day,
        live_fetcher=live_fetcher, interval=interval, cancel=cancel, pacer=pacer)


def _embedded_probe_result(root, *, ticker, day, live_fetcher=None,
                            live_evidence=None, interval="1m", cancel=None, pacer=None):
    """Shared row construction; only the fixed request entry owns authority."""
    interval = _probe_interval(interval)
    def detached_selection():
        # PathLike/string conversion may execute caller code, just like a
        # callback descriptor. Keep it outside the transferred worker scope.
        return Path(root).resolve(), {
            "ticker": str(ticker or "").strip().upper(),
            "day": str(day or "")[:10],
        }
    root, snapshot = fib.without_authority(detached_selection)()
    if _is_daily_probe(interval):
        snapshot["interval"] = interval
    request_count = 0
    network = False
    evidence_source = (
        "embedded_request" if live_fetcher is not None
        else "reused_read_only")
    reused_request_count = 0
    provenance = None
    try:
        if interval == "1m":
            snapshot = fib.without_authority(stored_day_snapshot)(root, ticker, day)
        else:
            snapshot = fib.without_authority(stored_daily_span_snapshot)(
                root, ticker, day=day, interval=interval)
        if live_fetcher is not None:
            # Select the shipped unbound implementation, never an instance
            # callback or a replaced execute method on a coordinator.
            method = (IBKRMinuteDayFetcher.fetch_day if interval == "1m"
                      else IBKRDailySpanFetcher.fetch_span)
            producer = ("ibkr.live_spot_probe.minute" if interval == "1m"
                        else "ibkr.live_spot_probe.daily")
            request_count = 1
            network = True
            live_result = method(live_fetcher, snapshot, pacer=pacer, cancel=cancel,
                _fetch_child=fops.narrow_worker_child(fib.current_worker(),
                    "embedded-probe-" + uuid.uuid4().hex, rights={producer}))
            provenance = live_result["fetch_provenance"]
            if cancel is not None and cancel.is_set():
                raise sk.Cancelled("embedded probe cancelled before classification")
            if not provenance["coverage_complete"]:
                raise EvidenceError("embedded probe has incomplete settled coverage")
        else:
            if interval == "1m":
                live_result = _reusable_live_result(
                    live_evidence, snapshot)
            else:
                live_result = _reusable_daily_live_result(
                    live_evidence, snapshot)
            reused_request_count = 1
            network = True
        current = fib.without_authority(_revalidate_snapshot)(root, snapshot)
        if interval == "1m":
            row = fib.without_authority(classify_snapshot_live)(
                current, live_result, request_count=request_count,
                network=network, evidence_source=evidence_source,
                reused_request_count=reused_request_count)
        else:
            row = fib.without_authority(classify_daily_span_live)(
                current, live_result, request_count=request_count,
                network=network, evidence_source=evidence_source,
                reused_request_count=reused_request_count)
        row["fetch_provenance"] = live_result["fetch_provenance"]
        return row
    except Exception as exc:  # noqa: BLE001 - bounded parent-run evidence row
        if live_fetcher is not None and isinstance(exc, (AuthorityError, LedgerError, sk.Cancelled)):
            raise
        if live_fetcher is not None:
            fib.notify_request_error(exc)
        row = fib.without_authority(_probe_error_row)(
            snapshot, exc, request_count=request_count, network=network)
        row["evidence_source"] = evidence_source
        row["reused_request_count"] = reused_request_count
        if provenance is not None:
            row["fetch_provenance"] = provenance
        if _is_daily_probe(interval):
            row["span_request_count"] = request_count
            row["span_reuse_count"] = reused_request_count
        return row


def run_live_probe(root=STORAGE_ROOT, *, seed=None, count=DEFAULT_COUNT,
                   ticker=None, day=None, port=2000, owner="manual", now=None,
                   live_fetcher=None, gate_path=None,
                   run_logs_root=RUN_LOGS_ROOT, interval="1m",
                   evidence_dir=None, _test_capability=None):
    """One held diagnostic parent; publish only after all children join."""
    directory = sk._auxiliary_directory(evidence_dir).resolve()
    bank = Path(root).resolve()
    if directory == bank or bank in directory.parents:
        raise EvidenceError("probe ledger must stay outside the stock bank")
    operation = fops.begin_operation("diagnostic", directory, test_capability=_test_capability)
    context = operation.context
    output, failure, outcome = None, None, "returned"
    try:
        fops.require_root_admission(operation)
        output = _run_live_probe_body(root, seed=seed, count=count, ticker=ticker,
            day=day, port=port, owner=owner, now=context.captured_now,
            live_fetcher=live_fetcher, gate_path=gate_path, run_logs_root=run_logs_root,
            interval=interval, _fetch_child=operation.child("standalone-probe"))
        outcome = "returned" if output["status"] == "complete" else "partial"
        operation.seal()
    except BaseException as exc:
        failure, outcome = exc, "failed"
    try:
        operation.close()
    except BaseException as exc:
        failure, outcome = failure or exc, "failed"
    report_path = str(directory / (context.operation_id + ".operation.json"))
    delivery = {"artifact_state": "not_written"}
    if failure is None:
        # The artifact is evidence, not the final completion record. A reader
        # must inspect its companion; a crash or failed companion write cannot
        # leave an optimistic VERIFIED claim in this first durable file.
        pending = fib.without_authority(freport.evidence)(
            context, "diagnostic", outcome="companion_pending")
        pending.update(report_path=report_path, report_persisted=False,
                       companion_required=True, operation_outcome=outcome)
        output["fetch_ledger"] = pending
        target = Path(output["artifact"])
        target = target.with_name(target.stem + "-" + context.operation_id + target.suffix)
        output["artifact"] = str(target)
        for row in output["probes"]:
            if "fetch_provenance" in row:
                row["fetch_provenance"]["source_companion_path"] = report_path
        delivery.update(artifact_state="unconfirmed", artifact_path=str(target))
        try:
            try:
                relative_artifact = os.path.relpath(target, directory).replace(os.sep, "/")
            except ValueError:  # Different Windows volumes; delivery is still valid.
                relative_artifact = None
            delivery.update(artifact_relative_path=relative_artifact,
                            ledger_relative_path=context.ledger.path.name)
            write_artifact(target, output, bank_root=root, run_logs_root=run_logs_root)
            with target.open("rb") as stream:
                payload = stream.read(MAX_ARTIFACT_BYTES + 1)
            if len(payload) > MAX_ARTIFACT_BYTES:
                raise EvidenceError("published artifact exceeds the size limit")
            delivery.update(artifact_state="written",
                            artifact_sha256=hashlib.sha256(payload).hexdigest())
            output["artifact_written"] = True
        except BaseException as exc:
            failure, outcome = exc, "failed"
    evidence = fib.without_authority(freport.evidence)(
        context, "diagnostic", outcome=outcome, error=failure)
    evidence.update(delivery, report_path=report_path, report_persisted=True)
    try:
        freport.write_report(evidence, directory)
    except BaseException as report_error:
        # This detached fallback is explicitly not a persisted companion.
        evidence.update(verified=False, state="UNVERIFIED", outcome="report_failed",
            operation_outcome=outcome, report_persisted=False,
            report_failure=fib.without_authority(str)(report_error)[:500])
        try:
            fib.without_authority(setattr)(report_error, "fetch_ledger", evidence)
        except Exception:
            pass  # Preserve the reporting error if its attachment hook fails.
        if failure is not None:
            raise report_error from failure
        raise
    if failure is not None:
        try:
            fib.without_authority(setattr)(failure, "fetch_ledger", evidence)
        except Exception:
            pass  # A provider's attachment hook must not replace its failure.
        raise failure
    output["fetch_ledger"] = evidence
    return output


@fib.worker_scope
def _run_live_probe_body(root=STORAGE_ROOT, *, seed=None, count=DEFAULT_COUNT,
                   ticker=None, day=None, port=2000, owner="manual", now=None,
                   live_fetcher=None, gate_path=None,
                   run_logs_root=RUN_LOGS_ROOT, interval="1m"):
    """Run one explicitly authorized report-only probe and write its artifact."""
    interval = _probe_interval(interval)
    context = fib.current_worker().context
    selection_adjustments = []
    def settled_selection(selected):
        selected = _date(selected, "selected probe day")
        horizon = context.horizons[interval]
        if horizon is None:
            raise CalendarUnsupported("probe token has no settled covered horizon")
        if _is_daily_probe(interval) and selected > horizon.date():
            selection_adjustments.append({"requested_day": selected.isoformat(),
                "selected_day": horizon.date().isoformat(), "reason": "fully settled span at entry"})
            return horizon.date().isoformat()
        return selected.isoformat()
    if day is not None:
        day = settled_selection(day)
    count = _interval_count(count, interval)
    root = Path(root).resolve()
    owner = _owner(owner)
    port = _port(port)
    if (ticker is None) != (day is None):
        raise LiveSpotProbeError("ticker and day must be provided together")
    if ticker is not None and count != DEFAULT_COUNT:
        raise LiveSpotProbeError(
            "count cannot be combined with ticker and day")
    target = artifact_path(
        owner, run_logs_root=run_logs_root, now=now).resolve()
    if target == root or root in target.parents:
        raise EvidenceError("artifact path must stay outside the stock bank")
    resolved_gate = Path(
        operation_gate.LOCK_PATH if gate_path is None else gate_path).resolve()
    if resolved_gate == root or root in resolved_gate.parents:
        raise EvidenceError("operation gate must stay outside the stock bank")
    if live_fetcher is None and gate_path is not None:
        production_gate = Path(operation_gate.LOCK_PATH).resolve()
        if resolved_gate != production_gate:
            raise LiveSpotProbeError(
                "a custom operation gate requires an injected live fetcher")

    lease = operation_gate.acquire(
        "fetch", owner=f"live spot probe {owner}", path=gate_path)
    with lease:
        bank_before = bank_tree_metadata(root)
        if ticker is None:
            plan = build_plan(
                root, seed=seed, count=count, now=now,
                interval=interval)
            selection_mode = "random"
        else:
            plan = _explicit_plan(
                root, ticker, day, seed=seed, now=now,
                interval=interval)
            selection_mode = "explicit"

        snapshots = []
        preparation_errors = []
        for public in plan["probes"]:
            try:
                if interval == "1m":
                    snapshot = stored_day_snapshot(
                        root, public["ticker"], public["day"],
                        expected_manifest_fingerprint=(
                            public["manifest_fingerprint"]))
                else:
                    snapshot = stored_daily_span_snapshot(
                        root, public["ticker"], day=settled_selection(public["day"]),
                        expected_manifest_fingerprint=(
                            public["manifest_fingerprint"]),
                        interval=interval)
            except (AuthorityError, LedgerError, sk.Cancelled):
                raise
            except Exception as exc:  # noqa: BLE001 - bounded evidence row
                preparation_errors.append((public, exc))
            else:
                snapshots.append(snapshot)

        fetcher = live_fetcher if live_fetcher is not None else (
            IBKRMinuteDayFetcher(port=port) if interval == "1m"
            else IBKRDailySpanFetcher(port=port))
        setup_error = None
        network_attempted = False
        rows = []
        total_requests = 0
        close_error = None
        try:
            try:
                expected = IBKRMinuteDayFetcher if interval == "1m" else IBKRDailySpanFetcher
                if type(fetcher) is not expected:
                    raise RequestRefused("custom probe fetcher is not an authorized request dispatcher")
                method = expected.fetch_day if interval == "1m" else expected.fetch_span
                if snapshots:
                    network_attempted = True
                    fib.without_authority(IBKRMinuteDayFetcher.start)(fetcher)
            except (AuthorityError, LedgerError, sk.Cancelled):
                raise
            except Exception as exc:  # noqa: BLE001 - bounded setup failure
                setup_error = exc
            for public, exc in preparation_errors:
                error_row = _probe_error_row(
                    public, exc, request_count=0, network=False)
                if _is_daily_probe(interval):
                    error_row.update({
                        "span_request_count": 0,
                        "span_reuse_count": 0,
                    })
                rows.append(error_row)
            for snapshot in snapshots:
                if setup_error is not None:
                    error_row = _probe_error_row(
                        snapshot, setup_error, request_count=0,
                        network=network_attempted)
                    if _is_daily_probe(interval):
                        error_row.update({
                            "span_request_count": 0,
                            "span_reuse_count": 0,
                        })
                    rows.append(error_row)
                    continue
                try:
                    total_requests += 1
                    producer = "ibkr.live_spot_probe.minute" if interval == "1m" else "ibkr.live_spot_probe.daily"
                    live = method(fetcher, snapshot, _fetch_child=fops.narrow_worker_child(
                        fib.current_worker(), f"probe-{total_requests}", rights={producer}))
                    if not live["fetch_provenance"]["coverage_complete"]:
                        raise RequestRefused("probe selection has incomplete settled coverage")
                    current = _revalidate_snapshot(root, snapshot)
                    if interval == "1m":
                        row = classify_snapshot_live(current, live)
                    else:
                        row = classify_daily_span_live(current, live)
                    row["fetch_provenance"] = live["fetch_provenance"]
                    rows.append(row)
                except (AuthorityError, LedgerError, sk.Cancelled):
                    raise
                except Exception as exc:  # noqa: BLE001 - one bounded error row
                    error_row = _probe_error_row(
                        snapshot, exc, request_count=1, network=True)
                    if _is_daily_probe(interval):
                        error_row.update({
                            "span_request_count": 1,
                            "span_reuse_count": 0,
                        })
                    rows.append(error_row)
        finally:
            prior_failure = sys.exception()
            if type(fetcher) in {IBKRMinuteDayFetcher, IBKRDailySpanFetcher}:
                try:
                    fib.without_authority(IBKRMinuteDayFetcher.close)(fetcher)
                except (AuthorityError, LedgerError, sk.Cancelled):
                    if prior_failure is None:
                        raise
                except Exception as exc:  # noqa: BLE001 - report teardown issue
                    close_error = {
                        "type": type(exc).__name__,
                        "message": fib.without_authority(str)(exc)[:500],
                    }

        bank_after = bank_tree_metadata(root)
        bank_unchanged = bank_before == bank_after
        verdict_domain = VOL_VERDICTS if _is_vol_probe(interval) else VERDICTS
        verdict_counts = {
            verdict: sum(1 for row in rows
                         if row.get("verdict") == verdict)
            for verdict in verdict_domain + (PROBE_ERROR,)
        }
        safety_counts = {}
        for row in rows:
            status = str(row.get("safety_status") or "UNKNOWN")
            safety_counts[status] = safety_counts.get(status, 0) + 1
        probe_error_count = verdict_counts[PROBE_ERROR]
        incomplete = (
            probe_error_count or plan["errors"] or plan["error_overflow"]
            or len(rows) < plan["requested_count"] or close_error
            or not bank_unchanged)
        needs_human = any(
            row.get("needs_human") for row in rows
            if row.get("verdict") != PROBE_ERROR)
        if incomplete:
            status = "partial"
        elif needs_human:
            status = "review_required"
        else:
            status = "complete"
        report = {
            "kind": "live_spot_probe_report",
            "version": REPORT_VERSION,
            "generated_at": plan["generated_at"],
            "completed_at": _utc_timestamp(now),
            "selection_mode": selection_mode,
            "selection_adjustments": selection_adjustments,
            "seed": plan["seed"],
            "requested_count": plan["requested_count"],
            "selected_count": len(rows),
            "candidate_count": plan["candidate_count"],
            "candidate_fingerprint": plan["candidate_fingerprint"],
            "status": status,
            "report_only": True,
            "probes": rows,
            "selection_errors": plan["errors"],
            "selection_error_overflow": plan["error_overflow"],
            "close_error": close_error,
            "verdict_counts": verdict_counts,
            "safety_status_counts": safety_counts,
            "probe_error_count": probe_error_count,
            "request_count": total_requests,
            "port": port,
            "network": network_attempted,
            "written": False,
            "bank_written": False,
            "bank_tree_before": bank_before,
            "bank_tree_after": bank_after,
            "bank_tree_unchanged": bank_unchanged,
            "artifact_written": False,
            "live_capability": True,
        }
        if interval == "1m":
            report.update({
                "max_requests_per_day": max(
                    (row.get("request_count", 0) for row in rows), default=0),
                "request_limit_per_day": 1,
            })
        else:
            report.update({
                "interval": interval,
                "span_days": DAILY_SPAN_DAYS,
                "span_request_count": total_requests,
                "reused_request_count": 0,
                "max_requests_per_span": max(
                    (row.get("request_count", 0) for row in rows), default=0),
                "request_limit_per_span": 1,
                "covered_day_count": sum(
                    int(row.get("covered_day_count")
                        or row.get("stored_bars") or 0)
                    for row in rows),
            })
        report["artifact"] = str(target.resolve())
        return report


def _owner(value):
    owner = str(value or "").strip().lower()
    if not _OWNER_RE.fullmatch(owner):
        raise LiveSpotProbeError(f"invalid artifact owner: {value!r}")
    return owner


def _direct_child(root, basename, suffix):
    name = str(basename or "")
    if (not name or name != Path(name).name or not name.endswith(suffix)
            or "/" in name or "\\" in name):
        raise EvidenceError(f"unsafe artifact basename: {basename!r}")
    resolved_root = Path(root).resolve()
    path = resolved_root / name
    try:
        if path.resolve().parent != resolved_root:
            raise EvidenceError("artifact escapes Run Logs")
    except OSError as exc:
        raise EvidenceError("cannot resolve artifact path") from exc
    return path


def artifact_path(owner, *, run_logs_root=RUN_LOGS_ROOT, now=None):
    stamp = dt.datetime.fromisoformat(_utc_timestamp(now))
    return _direct_child(
        run_logs_root,
        f"live-spot-probe-{stamp:%Y%m%d}-{_owner(owner)}.json",
        ".json")


def write_artifact(path, report, *, bank_root=STORAGE_ROOT,
                   run_logs_root=RUN_LOGS_ROOT):
    target = Path(path).resolve()
    bank = Path(bank_root).resolve()
    if target == bank or bank in target.parents:
        raise EvidenceError("artifact path must stay outside the stock bank")
    expected = _direct_child(run_logs_root, target.name, ".json")
    if target != expected:
        raise EvidenceError("artifact must be a direct Run Logs child")
    if not isinstance(report, dict):
        raise EvidenceError("artifact report must be an object")
    payload = dict(report)
    payload.update({
        "artifact": str(target),
        "artifact_written": True,
        "bank_written": False,
    })
    encoded = (json.dumps(
        payload, indent=2, sort_keys=True, allow_nan=False) + "\n").encode(
            "utf-8")
    if len(encoded) > MAX_ARTIFACT_BYTES:
        raise EvidenceError("artifact exceeds the size limit")
    target.parent.mkdir(parents=True, exist_ok=True)
    storage._atomic_write_bytes(target, encoded)
    return target
