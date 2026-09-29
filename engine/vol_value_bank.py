"""Exact-day volatility refetch and guarded bank replacement seams.

This module is deliberately smaller than the normal gap-fill engine.  It owns
only the market-data part of Row 51's reconciliation contract:

* take a content-addressed snapshot of one already-stored ratio day;
* fetch that exact RTH day with one kind-correct historical request; and
* replace only that day after a second content/identity check.

The caller owns the surrounding ``fetch`` operation lease and the audit queue.
Nothing here opens a port by itself, scans a production bank, or removes a
queue row.  Keeping queue finalization separate is intentional: a queue item
may be cleared only after both the month and manifest are durable.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import os
import re
import struct
import time
from contextlib import contextmanager, nullcontext
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import fetch_ibkr_bridge as fib
from fetch_authority import AuthorityError
from fetch_ledger import LedgerError, inspect_ledger
from fetch_run_context import RequestRefused, normalize_rows, row_evidence
import stock_ibkr as ibkr
import stock_storage as storage
import vol_value_audit as audit


SNAPSHOT_VERSION = 1
FETCH_VERSION = 1
CORRECTION_VERSION = 1
MAX_RAW_BARS = 2_000_000
RTH_SESSION_SECONDS = 23_400
RAW_RESPONSE_TOLERANCE = 8
MAX_VALUE_CORRECTIONS = 4_096
MAX_TEXT = 320
STAGE_DIR_NAME = storage.VOL_VALUE_RECONCILE_STAGE_DIR
TXN_MARKER_NAME = "transaction.json"
TXN_MANIFEST_BEFORE = "manifest.before"
TXN_MANIFEST_AFTER = storage.MANIFEST_NAME
TXN_MONTH_BEFORE = "month.before"
TXN_MONTH_AFTER = "month.after"
TXN_VERSION = 1

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITY_CORRECTIONS = storage.IDENTITY_CORRECTION_TYPES
ANOMALY_REASONS = frozenset({
    "hard_nonfinite", "hard_negative", "hard_ceiling", "hard_ohlc",
    "hard_volume", "unit_flip_month", "jump_up", "jump_down",
    "iv_zero_run", "iv_hvol_band",
})
_SNAPSHOT_FIELDS = frozenset({
    "version", "ticker", "kind_token", "day", "month", "month_file",
    "conid", "what_to_show", "identity_floor", "kind_earliest",
    "month_sha256",
    "stored_day_sha256", "stored_grid_sha256", "stored_bars", "stored_value",
})
_FETCH_FIELDS = frozenset({
    "version", "ticker", "kind_token", "day", "conid", "what_to_show",
    "bars", "served_bars", "served_day_sha256", "served_value",
    "served_grid_sha256", "request_count", "counters",
})


class RatioDayError(RuntimeError):
    """One exact-day volatility operation could not be completed safely."""

    def __init__(self, reason, *, request_count=0):
        super().__init__(reason)
        if (isinstance(request_count, bool) or not isinstance(request_count, int)
                or request_count not in (0, 1)):
            raise ValueError("request_count must be 0 or 1")
        self.request_count = request_count


class RatioDayStale(RatioDayError):
    """The snapshotted month/day/identity changed before correction commit."""


class RatioDayCommitError(RatioDayError):
    """A post-month commit failure with explicit rollback/current-state evidence."""

    def __init__(self, reason, *, evidence, request_count=0):
        super().__init__(reason, request_count=request_count)
        if not isinstance(evidence, dict):
            raise ValueError("commit evidence must be an object")
        self.evidence = dict(evidence)
        self.rollback_verified = evidence.get("rollback_verified") is True
        self.bank_written = evidence.get("bank_written")
        self.correction_recorded = evidence.get("correction_recorded")


class _UnpublishedStageError(RatioDayError):
    """This attempt created a stage directory that it could not remove."""

    def __init__(self, reason, *, new_stats, cleanup_error):
        super().__init__(reason)
        self.new_stats = dict(new_stats)
        self.cleanup_error = cleanup_error


class _CallableCancel:
    """Event-compatible view over a lightweight cancellation callback."""

    def __init__(self, callback):
        self._callback = callback

    def is_set(self):
        return bool(self._callback())


def _cancel_event(cancel):
    if cancel is None:
        return None
    if callable(getattr(cancel, "is_set", None)):
        return cancel
    if callable(cancel):
        return _CallableCancel(cancel)
    raise RatioDayError("cancel must be an Event-like object or callable")


def _text(value, label):
    if not isinstance(value, str):
        raise RatioDayError(f"invalid {label}")
    value = value.strip()
    if (not value or len(value) > MAX_TEXT
            or any(char in value for char in "\r\n\x00")):
        raise RatioDayError(f"invalid {label}")
    return value


def _sha256(value, label):
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise RatioDayError(f"invalid {label}")
    return value


def canonical_anomaly_reasons(value, *, reason=None):
    """Validate the bounded detector-domain provenance carried by queue rows."""
    if (not isinstance(value, (list, tuple, set, frozenset)) or not value
            or len(value) > 16):
        raise RatioDayError("volatility anomaly reasons are invalid")
    canonical = sorted({_text(item, "volatility anomaly reason")
                        for item in value})
    if (not canonical or len(canonical) > 16
            or any(item not in ANOMALY_REASONS for item in canonical)):
        raise RatioDayError("volatility anomaly reasons are outside the audit domain")
    joined = ",".join(canonical)
    if reason is not None and reason != joined:
        raise RatioDayError("volatility anomaly reason fields disagree")
    return canonical


def _day(value, label="day"):
    if isinstance(value, datetime):
        raise RatioDayError(f"invalid {label}")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise RatioDayError(f"invalid {label}")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise RatioDayError(f"invalid {label}") from exc
    if parsed.isoformat() != value:
        raise RatioDayError(f"invalid {label}")
    return parsed


def _ticker(value):
    try:
        return storage.canonical_ticker(value)
    except Exception as exc:  # noqa: BLE001 - normalize the public seam
        raise RatioDayError(f"invalid ticker: {value!r}") from exc


def _kind_token(value, *, for_day=None):
    token = _text(value, "ratio kind token")
    if (storage.INTERVAL_RE.fullmatch(token) is None
            or storage.kind_of(token) not in storage.RATIO_KINDS
            or storage.session_of(token) != "rth"):
        raise RatioDayError(
            "reconcile kind must be an exact RTH IV/HVOL interval token")
    base = storage.base_interval(token)
    if base not in ibkr._BAR_SIZES:  # shared canonical IBKR bar-size table
        raise RatioDayError(f"unsupported reconcile interval: {token}")
    if for_day is not None and len(ibkr.day_requests(token, for_day)) != 1:
        raise RatioDayError(
            f"{token} needs more than one request for a day; refusing reconcile")
    return token


def _raw_response_limit(kind_token):
    """Tight bound for one RTH day before materializing adapter output."""
    base = storage.base_interval(kind_token)
    if base == "1d":
        return RAW_RESPONSE_TOLERANCE
    match = re.fullmatch(r"(\d+)([smh])", base)
    if match is None:
        raise RatioDayError(f"unsupported reconcile interval: {kind_token}")
    count = int(match.group(1))
    seconds = count * {"s": 1, "m": 60, "h": 3_600}[match.group(2)]
    expected = math.ceil(RTH_SESSION_SECONDS / seconds)
    # A small allowance tolerates a vendor boundary label while still limiting
    # a faulty/endless iterator to hundreds of objects (30s is the largest
    # accepted single-request grid at 780 normal RTH bars).
    return expected + RAW_RESPONSE_TOLERANCE


def _conid(value):
    if isinstance(value, bool):
        raise RatioDayError("manifest conId is missing or invalid")
    try:
        value = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RatioDayError("manifest conId is missing or invalid") from exc
    if value <= 0:
        raise RatioDayError("manifest conId is missing or invalid")
    return value


def _finite_or_none(value, label):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RatioDayError(f"invalid {label}")
    number = float(value)
    if not math.isfinite(number):
        raise RatioDayError(f"invalid {label}")
    return number


def _grid_sha256(bars):
    """Fingerprint the ordered timestamp grid without binding bar values."""
    digest = hashlib.sha256()
    epoch = datetime(1970, 1, 1)
    previous = None
    count = 0
    for bar in bars:
        if not isinstance(bar, (tuple, list)) or len(bar) != 6:
            raise RatioDayError("ratio-day grid row is malformed")
        stamp = bar[0]
        if (not isinstance(stamp, datetime) or stamp.tzinfo is not None
                or stamp.microsecond
                or (previous is not None and stamp <= previous)):
            raise RatioDayError("ratio-day timestamp grid is invalid")
        previous = stamp
        try:
            digest.update(struct.pack(">q", int((stamp - epoch).total_seconds())))
        except (OverflowError, struct.error) as exc:
            raise RatioDayError("ratio-day timestamp grid is invalid") from exc
        count += 1
    if count <= 0:
        raise RatioDayError("ratio-day timestamp grid is empty")
    return digest.hexdigest()


def _root_and_ticker_dir(root, ticker):
    try:
        root = Path(root).resolve(strict=True)
    except OSError as exc:
        raise RatioDayError("storage root is unavailable") from exc
    if not root.is_dir():
        raise RatioDayError("storage root is not a directory")
    ticker_dir = (root / ticker).resolve(strict=False)
    if ticker_dir.parent != root or not ticker_dir.is_dir():
        raise RatioDayError(f"ticker directory is unavailable: {ticker}")
    return root, ticker_dir


def _unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RatioDayError(f"duplicate manifest JSON key: {key}")
        result[key] = value
    return result


def _load_manifest_strict(ticker_dir, ticker):
    path = Path(ticker_dir) / storage.MANIFEST_NAME
    try:
        raw = storage._read_stable_manifest_bytes(path)
        manifest = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_json_object)
    except RatioDayError:
        raise
    except (OSError, UnicodeError, ValueError, storage.StorageError) as exc:
        raise RatioDayError(
            f"{ticker} manifest is missing, malformed, or unstable") from exc
    if not isinstance(manifest, dict):
        raise RatioDayError(f"{ticker} manifest is not an object")
    if manifest.get("folder") != ticker:
        raise RatioDayError(f"{ticker} manifest folder does not match")
    intervals = manifest.get("intervals")
    if not isinstance(intervals, dict):
        raise RatioDayError(f"{ticker} manifest intervals are malformed")
    for interval, section in intervals.items():
        if not isinstance(interval, str) or not isinstance(section, dict):
            raise RatioDayError(f"{ticker} manifest interval is malformed")
        if not isinstance(section.get("months"), dict):
            raise RatioDayError(
                f"{ticker} manifest months for {interval!r} are malformed")
    return manifest


def identity_floor(manifest, kind_token, *, ticker=None):
    """Return the latest strict identity-cutover date applying to a series.

    Historical correction records scope intervals by base token (``1d``/
    ``1m``), while listing-wide records omit the interval list.  Both shapes
    therefore protect derived ratio tokens such as ``1d-hvol``. The shared
    storage parser also accepts price/session tokens for the normal fetch
    engine; Row 51's reconciliation entrypoints remain ratio-only.
    """
    try:
        return storage.identity_floor(
            manifest, kind_token, ticker=ticker)
    except storage.StorageError as exc:
        raise RatioDayError(str(exc)) from exc


def kind_earliest(manifest, kind_token):
    """Return exact per-kind demonstrated source reach, never TRADES fallback."""
    token = _kind_token(kind_token)
    if not isinstance(manifest, dict):
        raise RatioDayError("manifest is not an object")
    intervals = manifest.get("intervals")
    section = intervals.get(token) if isinstance(intervals, dict) else None
    if not isinstance(section, dict):
        raise RatioDayError(f"manifest has no usable {token} interval state")
    raw = section.get("backfill_served_earliest")
    if raw is None:
        return None
    earliest = _day(raw, f"{token} served earliest")
    seal = section.get("backfill_seal")
    if isinstance(seal, dict) and seal.get("served_earliest") is not None:
        sealed = _day(
            seal.get("served_earliest"), f"{token} sealed served earliest")
        if sealed != earliest:
            raise RatioDayError(
                f"manifest {token} served-earliest evidence disagrees")
    return earliest


def _month_path(root, ticker_dir, ticker, token, day):
    path = storage.find_month_file(
        root, ticker, day.year, day.month, token)
    if path is None:
        raise RatioDayError(
            f"stored month is missing for {ticker} {token} {day:%Y-%m}")
    try:
        resolved = Path(path).resolve(strict=True)
    except OSError as exc:
        raise RatioDayError("stored month is unavailable") from exc
    if not resolved.is_relative_to(ticker_dir):
        raise RatioDayError("stored month escapes the ticker directory")
    return resolved


def _month_parts(path, target_day):
    try:
        bars, stats = storage.read_month_file(path, validate=False)
    except (OSError, storage.StorageError, TypeError, ValueError) as exc:
        raise RatioDayError(
            f"cannot read stored ratio month {Path(path).name}: {exc}") from exc
    if not bars:
        raise RatioDayError("stored ratio month is empty")
    previous = None
    target = []
    for index, bar in enumerate(bars):
        if not isinstance(bar, (tuple, list)) or len(bar) != 6:
            raise RatioDayError(f"stored ratio month row {index + 1} is malformed")
        stamp = bar[0]
        if (not isinstance(stamp, datetime) or stamp.tzinfo is not None
                or stamp.microsecond
                or (stamp.year, stamp.month)
                != (target_day.year, target_day.month)):
            raise RatioDayError(
                f"stored ratio month row {index + 1} has an invalid timestamp")
        if previous is not None and stamp <= previous:
            raise RatioDayError(
                "stored ratio month timestamps are not strictly increasing")
        previous = stamp
        if stamp.date() == target_day:
            target.append(tuple(bar))
            continue
        # Other queued hard-invalid days may coexist in this month. Their
        # exact tuple bits are retained by the correction-specific staging
        # codec so each one can be repaired without a mutual-blocking cycle.
    if not target:
        raise RatioDayError(f"stored ratio day is missing: {target_day}")
    try:
        fingerprint = audit.day_fingerprint(target)
    except Exception as exc:  # noqa: BLE001 - normalize audit/storage shape errors
        raise RatioDayError("stored ratio day cannot be fingerprinted") from exc
    try:
        close = float(target[-1][4])
    except (TypeError, ValueError, OverflowError):
        close = None
    if close is not None and not math.isfinite(close):
        close = None
    return list(map(tuple, bars)), target, fingerprint, close, dict(stats)


def _snapshot(value):
    if not isinstance(value, dict) or set(value) != _SNAPSHOT_FIELDS:
        raise RatioDayError("ratio-day snapshot fields are invalid")
    if value.get("version") != SNAPSHOT_VERSION:
        raise RatioDayError("ratio-day snapshot version is invalid")
    day = _day(value.get("day"))
    token = _kind_token(value.get("kind_token"), for_day=day)
    ticker = _ticker(value.get("ticker"))
    if value.get("ticker") != ticker or value.get("kind_token") != token:
        raise RatioDayError("ratio-day snapshot identity is not canonical")
    month = _text(value.get("month"), "snapshot month")
    if month != day.isoformat()[:7]:
        raise RatioDayError("ratio-day snapshot month disagrees")
    month_file = _text(value.get("month_file"), "snapshot month filename")
    if Path(month_file).name != month_file:
        raise RatioDayError("ratio-day snapshot filename is invalid")
    floor = value.get("identity_floor")
    if floor is not None:
        floor_day = _day(floor, "snapshot identity floor")
        if floor_day > day:
            raise RatioDayError("snapshot day predates the identity floor")
        floor = floor_day.isoformat()
    earliest = value.get("kind_earliest")
    if earliest is not None:
        earliest_day = _day(earliest, "snapshot kind earliest")
        if earliest_day > day:
            raise RatioDayError("snapshot day predates the kind source reach")
        earliest = earliest_day.isoformat()
    stored_bars = value.get("stored_bars")
    if (isinstance(stored_bars, bool) or not isinstance(stored_bars, int)
            or stored_bars <= 0
            or stored_bars > _raw_response_limit(token)):
        raise RatioDayError("snapshot stored-bar count is invalid")
    what = _text(value.get("what_to_show"), "snapshot whatToShow")
    if what != ibkr._what_to_show(token):
        raise RatioDayError("snapshot whatToShow disagrees with the kind")
    return {
        "version": SNAPSHOT_VERSION,
        "ticker": ticker,
        "kind_token": token,
        "day": day.isoformat(),
        "month": month,
        "month_file": month_file,
        "conid": _conid(value.get("conid")),
        "what_to_show": what,
        "identity_floor": floor,
        "kind_earliest": earliest,
        "month_sha256": _sha256(
            value.get("month_sha256"), "snapshot month SHA-256"),
        "stored_day_sha256": _sha256(
            value.get("stored_day_sha256"), "snapshot day SHA-256"),
        "stored_grid_sha256": _sha256(
            value.get("stored_grid_sha256"), "snapshot grid SHA-256"),
        "stored_bars": stored_bars,
        "stored_value": _finite_or_none(
            value.get("stored_value"), "snapshot stored value"),
    }


def snapshot_ratio_day(root, ticker, kind_token, day, today=None):
    """Snapshot one stored ratio day before the single source request.

    The raw month SHA and whole-day tuple fingerprint are independent CAS
    tokens.  Identity-floor rejection happens here, before a caller can spend a
    historical request, and is repeated at commit.
    """
    ticker = _ticker(ticker)
    day = _day(day)
    token = _kind_token(kind_token, for_day=day)
    today = ibkr.now_ny().date() if today is None else _day(today, "today")
    if day >= today:
        raise RatioDayError("only a completed past RTH day can be reconciled")
    root, ticker_dir = _root_and_ticker_dir(root, ticker)
    with storage.ticker_transaction(ticker_dir):
        _recover_pending_transaction(root, ticker_dir, ticker)
    manifest = _load_manifest_strict(ticker_dir, ticker)
    conid = _conid(manifest.get("conid"))
    section = manifest["intervals"].get(token)
    if not isinstance(section, dict):
        raise RatioDayError(
            f"{ticker} manifest has no usable {token} interval state")
    floor = identity_floor(manifest, token, ticker=ticker)
    if floor is not None and day < floor:
        raise RatioDayError(
            f"{ticker} {token} {day} predates identity floor {floor}")
    earliest = kind_earliest(manifest, token)
    if earliest is not None and day < earliest:
        raise RatioDayError(
            f"{ticker} {token} {day} predates demonstrated source reach "
            f"{earliest}")
    path = _month_path(root, ticker_dir, ticker, token, day)
    _bars, target, fingerprint, close, stats = _month_parts(path, day)
    month_entry = (section.get("months") or {}).get(day.isoformat()[:7])
    if (not isinstance(month_entry, dict)
            or month_entry.get("sha256") != stats.get("sha256")):
        raise RatioDayError(
            "manifest target month does not match the stored bytes; "
            "run a storage scan before reconciliation")
    result = {
        "version": SNAPSHOT_VERSION,
        "ticker": ticker,
        "kind_token": token,
        "day": day.isoformat(),
        "month": day.isoformat()[:7],
        "month_file": path.name,
        "conid": conid,
        "what_to_show": ibkr._what_to_show(token),
        "identity_floor": floor.isoformat() if floor is not None else None,
        "kind_earliest": (
            earliest.isoformat() if earliest is not None else None),
        "month_sha256": _sha256(stats.get("sha256"), "stored month SHA-256"),
        "stored_day_sha256": fingerprint["sha256"],
        "stored_grid_sha256": _grid_sha256(target),
        "stored_bars": fingerprint["bars"],
        "stored_value": close,
    }
    return _snapshot(result)


def _served(value):
    if not isinstance(value, dict) or set(value) != _FETCH_FIELDS:
        raise RatioDayError("served ratio-day fields are invalid")
    if value.get("version") != FETCH_VERSION:
        raise RatioDayError("served ratio-day version is invalid")
    day = _day(value.get("day"))
    token = _kind_token(value.get("kind_token"), for_day=day)
    ticker = _ticker(value.get("ticker"))
    if value.get("ticker") != ticker or value.get("kind_token") != token:
        raise RatioDayError("served ratio-day identity is not canonical")
    what = _text(value.get("what_to_show"), "served whatToShow")
    if what != ibkr._what_to_show(token):
        raise RatioDayError("served whatToShow disagrees with the kind")
    if value.get("request_count") != 1:
        raise RatioDayError("ratio-day refetch must contain exactly one request")
    bars = value.get("bars")
    if not isinstance(bars, (list, tuple)):
        raise RatioDayError("served bars are invalid")
    bars = [tuple(bar) if isinstance(bar, (tuple, list)) else bar
            for bar in bars]
    served_bars = value.get("served_bars")
    if (isinstance(served_bars, bool) or not isinstance(served_bars, int)
            or served_bars <= 0 or served_bars != len(bars)
            or served_bars > _raw_response_limit(token)):
        raise RatioDayError("served bar count is invalid")
    for bar in bars:
        if (not isinstance(bar, tuple) or len(bar) != 6
                or not isinstance(bar[0], datetime)
                or bar[0].date() != day):
            raise RatioDayError("served bar is outside the requested day")
        problems = storage.validate_bar(
            *bar, window=storage.session_window(token),
            allow_zero_prices=True)
        try:
            over_ceiling = any(
                float(number) > ibkr.VOL_HARD_CEILING for number in bar[1:5])
        except (TypeError, ValueError, OverflowError) as exc:
            raise RatioDayError("served ratio day has a non-numeric value") from exc
        if problems or over_ceiling:
            raise RatioDayError("served ratio day violates the ingest gate")
    try:
        fingerprint = audit.day_fingerprint(bars)
    except Exception as exc:  # noqa: BLE001
        raise RatioDayError("served ratio day cannot be fingerprinted") from exc
    if value.get("served_day_sha256") != fingerprint["sha256"]:
        raise RatioDayError("served day fingerprint disagrees")
    grid_sha = _grid_sha256(bars)
    if value.get("served_grid_sha256") != grid_sha:
        raise RatioDayError("served timestamp grid fingerprint disagrees")
    counters = value.get("counters")
    if (not isinstance(counters, dict)
            or set(counters) != {"invalid", "outside_day", "non_rth"}
            or any(isinstance(count, bool) or not isinstance(count, int)
                   or count < 0 for count in counters.values())
            or counters["invalid"]):
        raise RatioDayError("served conversion counters are invalid")
    close = _finite_or_none(value.get("served_value"), "served value")
    if close is None or close != float(bars[-1][4]):
        raise RatioDayError("served value disagrees with the final close")
    return {
        "version": FETCH_VERSION,
        "ticker": ticker,
        "kind_token": token,
        "day": day.isoformat(),
        "conid": _conid(value.get("conid")),
        "what_to_show": what,
        "bars": bars,
        "served_bars": served_bars,
        "served_day_sha256": fingerprint["sha256"],
        "served_grid_sha256": grid_sha,
        "served_value": close,
        "request_count": 1,
        "counters": dict(counters),
    }


def _ratio_request_evidence(request, raw, token):
    """Validate the current parent's durable result before any ratio consumer.

    The parent is still running: require the exact physical pair, not its final
    seal. This is fresh synchronous evidence, never a reusable cache grant.
    """
    context = request.worker.context
    with context.ledger._lock:
        if context.ledger.failure:
            raise LedgerError("ratio ledger failed before result publication")
        events = inspect_ledger(context.ledger.path, require_seal=False)["events"]
    pair = [event for event in events if event.get("logical_id") == request.logical_id]
    if (len(pair) != 2 or [event["event"] for event in pair] != ["decision", "result"]
            or pair[0]["attempt_id"] != pair[1]["attempt_id"]):
        raise RequestRefused("ratio result lacks its single durable attempt")
    decision, result = pair[0]["payload"], pair[1]["payload"]
    accepted = row_evidence(normalize_rows(fib.normalize_bars(raw, token)))
    if result.get("accepted") != accepted or result.get("outcome") not in {"returned", "empty"}:
        raise RequestRefused("ratio result differs from its durable accepted rows")
    if decision["requested"]["intended_end"] != decision["effective"]["intended_end"]:
        raise RequestRefused("ratio refetch has incomplete session coverage")


@fib.worker_scope
def fetch_ratio_day(adapter, pacer, snapshot, cancel=None, progress=None):
    """Child-only, one guarded RTH request for the exact snapshotted ratio day."""
    snap = fib.without_authority(_snapshot)(snapshot)
    cancel_event = _cancel_event(cancel)
    day = _day(snap["day"])
    requests = ibkr._covered_session_requests(
        snap["kind_token"], day, fib.current_worker().context)
    if len(requests) != 1:
        raise RatioDayError("ratio-day reconcile request is not singular")
    first, end_dt, duration = requests[0]
    bar_size, _window = ibkr._BAR_SIZES[
        storage.base_interval(snap["kind_token"])]
    request_count = 0
    if cancel_event is not None:
        try:
            cancelled = bool(cancel_event.is_set())
        except Exception as exc:  # noqa: BLE001 - callback seam
            raise RatioDayError("cancel callback failed") from exc
        if cancelled:
            raise ibkr.Cancelled()
    try:
        if type(adapter) is ibkr.ReusableAdapter:
            adapter = fib.without_authority(lambda: adapter._live())()
        if type(adapter) is not ibkr.LiveIB:
            raise RequestRefused("ratio refetch requires the shipped LiveIB transport")
        contract = fib.without_authority(ibkr.LiveIB.contract_for)(adapter, snap["conid"])
        if _conid(getattr(contract, "conId", None)) != snap["conid"]:
            raise RatioDayError(
                "adapter contract does not match the pinned manifest conId")
    except (AuthorityError, LedgerError):
        raise
    except ibkr.Cancelled as exc:
        try:
            exc.request_count = request_count
        except Exception:  # noqa: BLE001 - best-effort evidence on foreign type
            pass
        raise
    except Exception as exc:  # noqa: BLE001 - adapter/fake boundary
        raise RatioDayError("ratio-day refetch could not start") from exc
    request = fib.bar_request("ibkr.vol_value_bank.ratio_day", contract,
        snap["kind_token"], end_dt, duration, symbol=snap["ticker"], start=first)
    try:
        sender = SimpleNamespace(fetch=fib.without_authority(getattr)(adapter, "fetch"))
        with fib.adapter_session(adapter, True), fib.send_scope(request, fib.acquire_turn):
            request_count = 1
            raw_iter = sender.fetch(
                contract, end_dt, duration, bar_size, snap["what_to_show"])
        if raw_iter is None:
            raw_iter = ()
        raw_limit = _raw_response_limit(snap["kind_token"])
        raw = fib.without_authority(lambda: list(itertools.islice(iter(raw_iter), raw_limit + 1)))()
    except (AuthorityError, LedgerError):
        raise
    except ibkr.Cancelled as exc:
        try:
            exc.request_count = request_count
        except Exception:  # noqa: BLE001 - best-effort evidence on foreign type
            pass
        raise
    except ibkr.PacingViolation as exc:
        fib.pacer().saturate()
        raise RatioDayError("ratio-day source request paced", request_count=request_count) from exc
    except Exception as exc:  # noqa: BLE001 - normalize live/fake implementations
        raise RatioDayError(
            "ratio-day source request failed",
            request_count=request_count) from exc
    if len(raw) > raw_limit:
        raise RatioDayError(
            "ratio-day source response is unreasonably large", request_count=1)
    _ratio_request_evidence(request, raw, snap["kind_token"])
    if cancel_event is not None and cancel_event.is_set():
        raise ibkr.Cancelled("ratio refetch cancelled before consumption")
    counters = {"invalid": 0, "outside_day": 0, "non_rth": 0}
    try:
        by_day = ibkr.split_session_bars(
            raw, frozenset((day,)), counters, snap["kind_token"])
    except Exception as exc:  # noqa: BLE001 - normalize converter failures
        raise RatioDayError(
            "ratio-day source response could not be cleaned",
            request_count=1) from exc
    try:
        bars = list(by_day.get(day, ()))
    except Exception as exc:  # noqa: BLE001 - normalize converter result shape
        raise RatioDayError(
            "ratio-day cleaned response is malformed", request_count=1) from exc
    if counters["invalid"]:
        raise RatioDayError(
            "ratio-day source response contained invalid volatility bars",
            request_count=1)
    if not bars:
        raise RatioDayError(
            "ratio-day source returned no valid RTH bars", request_count=1)
    try:
        fingerprint = audit.day_fingerprint(bars)
    except Exception as exc:  # noqa: BLE001 - normalize evidence failures
        raise RatioDayError(
            "ratio-day source evidence could not be fingerprinted",
            request_count=1) from exc
    served = {
        "version": FETCH_VERSION,
        "ticker": snap["ticker"],
        "kind_token": snap["kind_token"],
        "day": snap["day"],
        "conid": snap["conid"],
        "what_to_show": snap["what_to_show"],
        "bars": bars,
        "served_bars": len(bars),
        "served_day_sha256": fingerprint["sha256"],
        "served_grid_sha256": _grid_sha256(bars),
        "served_value": float(bars[-1][4]),
        "request_count": 1,
        "counters": counters,
    }
    try:
        return _served(served)
    except RatioDayError as exc:
        raise RatioDayError(str(exc), request_count=1) from exc


def _matching_context(snapshot, served):
    for field in ("ticker", "kind_token", "day", "conid", "what_to_show"):
        if snapshot[field] != served[field]:
            raise RatioDayError(f"served {field} disagrees with the snapshot")
    if served["served_bars"] != snapshot["stored_bars"]:
        raise RatioDayError("served bar count disagrees with the stored day")
    if served["served_grid_sha256"] != snapshot["stored_grid_sha256"]:
        raise RatioDayError(
            "served timestamp grid disagrees with the stored day")


def settlement_evidence(snapshot, served, reasons):
    """Build the exact strong-evidence shape consumed by vol_value_audit."""
    snap = _snapshot(snapshot)
    fresh = _served(served)
    _matching_context(snap, fresh)
    if (not isinstance(reasons, (list, tuple, set, frozenset))
            or not reasons or len(reasons) > 16):
        raise RatioDayError("settlement reasons are invalid")
    canonical = sorted({_text(reason, "settlement reason") for reason in reasons})
    if not canonical:
        raise RatioDayError("settlement reasons are invalid")
    return {
        "version": 1,
        "method": "ibkr_ratio_day_refetch",
        "what_to_show": fresh["what_to_show"],
        "stored_day_sha256": snap["stored_day_sha256"],
        "served_day_sha256": fresh["served_day_sha256"],
        "bars": fresh["served_bars"],
        "request_count": fresh["request_count"],
        "reasons": canonical,
        "month_sha256": snap["month_sha256"],
    }


def _current_identity_context(manifest, snap):
    if _conid(manifest.get("conid")) != snap["conid"]:
        raise RatioDayStale("pinned conId changed after the day snapshot")
    floor = identity_floor(
        manifest, snap["kind_token"], ticker=snap["ticker"])
    current_floor = floor.isoformat() if floor is not None else None
    if current_floor != snap["identity_floor"]:
        raise RatioDayStale("identity floor changed after the day snapshot")
    earliest = kind_earliest(manifest, snap["kind_token"])
    current_earliest = earliest.isoformat() if earliest is not None else None
    if current_earliest != snap["kind_earliest"]:
        raise RatioDayStale("kind source reach changed after the day snapshot")


def _current_target_state(root, ticker_dir, snap):
    path = _month_path(
        root, ticker_dir, snap["ticker"], snap["kind_token"],
        _day(snap["day"]))
    bars, target, fingerprint, close, stats = _month_parts(
        path, _day(snap["day"]))
    manifest = _load_manifest_strict(ticker_dir, snap["ticker"])
    _current_identity_context(manifest, snap)
    entry = ((((manifest.get("intervals") or {})
               .get(snap["kind_token"]) or {}).get("months") or {}).get(
                   snap["month"]))
    if (not isinstance(entry, dict)
            or entry.get("sha256") != stats.get("sha256")):
        raise RatioDayStale(
            "manifest target month no longer matches the stored bytes")
    return path, bars, target, fingerprint, close, stats, entry


@contextmanager
def finalization_guard(root, snapshot, served, *, action,
                       correction=None, manifest_lock=None):
    """Hold bank CAS protection through registry/queue finalization.

    Callers perform ``audit.finalize_reconcile`` inside this context. The lock
    order is manifest lock -> cross-process ticker transaction -> audit sidecar
    transaction, matching every cooperating bank writer.
    """
    snap = _snapshot(snapshot)
    fresh = _served(served)
    _matching_context(snap, fresh)
    if action not in {"settled", "corrected"}:
        raise RatioDayError("invalid ratio-day finalization action")
    root, ticker_dir = _root_and_ticker_dir(root, snap["ticker"])
    guard = manifest_lock if manifest_lock is not None else nullcontext()
    with guard, storage.ticker_transaction(ticker_dir):
        _recover_pending_transaction(root, ticker_dir, snap["ticker"])
        path, _bars, target, fingerprint, close, stats, entry = (
            _current_target_state(root, ticker_dir, snap))
        if action == "settled":
            if (path.name != snap["month_file"]
                    or stats.get("sha256") != snap["month_sha256"]
                    or fingerprint["sha256"] != snap["stored_day_sha256"]
                    or fingerprint["bars"] != snap["stored_bars"]
                    or _grid_sha256(target) != snap["stored_grid_sha256"]
                    or close != snap["stored_value"]):
                raise RatioDayStale(
                    "stored ratio day changed before settlement publication")
        else:
            if not isinstance(correction, dict):
                raise RatioDayError(
                    "corrected finalization lacks commit evidence")
            record = correction.get("correction")
            ledger = entry.get("value_corrections")
            if (not isinstance(record, dict)
                    or not isinstance(ledger, list) or not ledger
                    or len(ledger) > MAX_VALUE_CORRECTIONS
                    or ledger[-1] != record
                    or stats.get("sha256") != correction.get("month_sha256")
                    or entry.get("sha256") != correction.get("month_sha256")
                    or fingerprint["sha256"] != fresh["served_day_sha256"]
                    or fingerprint["bars"] != fresh["served_bars"]
                    or _grid_sha256(target) != fresh["served_grid_sha256"]
                    or close != fresh["served_value"]):
                raise RatioDayStale(
                    "corrected ratio day changed before queue retirement")
        yield


def _correction_ledger(month_entry):
    raw = month_entry.get("value_corrections")
    if raw is None:
        return []
    if not isinstance(raw, list) or len(raw) >= MAX_VALUE_CORRECTIONS:
        raise RatioDayError("manifest volatility-correction ledger is malformed/full")
    if not all(isinstance(item, dict) for item in raw):
        raise RatioDayError("manifest volatility-correction ledger is malformed")
    return list(raw)


def _correction_record(*, snapshot, served, run_id, source, reasons,
                       old_value, old_day_sha256, old_month_sha256,
                       new_month_sha256):
    confirmed = datetime.now(timezone.utc).replace(
        microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "version": CORRECTION_VERSION,
        "type": "vol_value_refetch_reconcile",
        "day": snapshot["day"],
        "run": run_id,
        "source": source,
        "confirmed": confirmed,
        "what_to_show": served["what_to_show"],
        "request_count": served["request_count"],
        "bars": served["served_bars"],
        "old_value": old_value,
        "new_value": served["served_value"],
        "old_day_sha256": old_day_sha256,
        "new_day_sha256": served["served_day_sha256"],
        "old_month_sha256": old_month_sha256,
        "new_month_sha256": new_month_sha256,
        "reason": ",".join(reasons),
        "reasons": reasons,
    }


def _rollback_manifest_bytes(ticker_dir, manifest):
    """Return the exact manifest generation represented by ``manifest``."""
    path = Path(ticker_dir) / storage.MANIFEST_NAME
    try:
        raw = storage._read_stable_manifest_bytes(path)
        decoded = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_json_object)
    except RatioDayError:
        raise
    except (OSError, UnicodeError, ValueError, storage.StorageError) as exc:
        raise RatioDayError(
            "manifest cannot be snapshotted for correction rollback") from exc
    if decoded != manifest:
        raise RatioDayStale(
            "manifest changed while correction rollback state was captured")
    return raw


def _rollback_month_bytes(path, expected_sha):
    """Return exact pre-write month bytes only when they match the CAS read."""
    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise RatioDayStale(
            "stored month became unreadable before rollback state was captured") from exc
    if hashlib.sha256(raw).hexdigest() != expected_sha:
        raise RatioDayStale(
            "stored month changed while correction rollback state was captured")
    return raw


def _raw_sha(path):
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return None


def _path_absent(path):
    path = Path(path)
    return not path.exists() and not path.is_symlink()


def _exact_file_sha(path, expected_sha):
    path = Path(path)
    return bool(
        not path.is_symlink() and path.is_file()
        and _raw_sha(path) == expected_sha)


def _canonical_transaction_paths(root, ticker_dir, ticker, token, day,
                                 *, original_path=None, write_path=None,
                                 candidate_name=None):
    """Validate the only authoritative slots a reconcile WAL may own."""
    try:
        root = Path(root).resolve(strict=False)
        ticker_dir = Path(ticker_dir).resolve(strict=False)
        expected_ticker_dir = (root / ticker).resolve(strict=False)
        csv_path = storage.month_file_path(
            root, ticker, day.year, day.month, token,
            fmt="csv").resolve(strict=False)
        parquet_path = storage.month_file_path(
            root, ticker, day.year, day.month, token,
            fmt="parquet").resolve(strict=False)
    except OSError as exc:
        raise RatioDayError(
            "volatility correction canonical month path is unavailable") from exc
    if ticker_dir != expected_ticker_dir:
        raise RatioDayError(
            "volatility correction ticker directory is non-canonical")
    if original_path is not None:
        try:
            original_path = Path(original_path).resolve(strict=False)
        except OSError as exc:
            raise RatioDayError(
                "volatility correction original path is unavailable") from exc
        if original_path not in {csv_path, parquet_path}:
            raise RatioDayError(
                "volatility correction original path is not its canonical slot")
    if write_path is not None:
        try:
            write_path = Path(write_path).resolve(strict=False)
        except OSError as exc:
            raise RatioDayError(
                "volatility correction write path is unavailable") from exc
        if write_path != parquet_path:
            raise RatioDayError(
                "volatility correction destination is not canonical Parquet")
    if candidate_name is not None and candidate_name != parquet_path.name:
        raise RatioDayError(
            "volatility correction candidate is not canonical Parquet")
    return csv_path, parquet_path


def _retire_exact_csv(path, expected_sha):
    """Delete the canonical legacy twin only at its recorded old SHA."""
    path = Path(path)
    if not path.exists() and not path.is_symlink():
        return
    if (path.is_symlink() or not path.is_file()
            or _raw_sha(path) != expected_sha):
        raise RatioDayError(
            "volatility correction legacy CSV changed while recovery was "
            "pending; recovery remains pending")
    try:
        path.unlink()
    except OSError as exc:
        raise RatioDayError(
            "volatility correction could not retire the legacy CSV; "
            "recovery remains pending") from exc


def _month_bits_sha256(bars, *, year, month):
    """Bind every stored tuple bit while allowing pre-existing hard values."""
    digest = hashlib.sha256()
    epoch = datetime(1970, 1, 1)
    previous = None
    for index, bar in enumerate(bars):
        if not isinstance(bar, (tuple, list)) or len(bar) != 6:
            raise RatioDayError(
                f"ratio correction month row {index + 1} is malformed")
        stamp = bar[0]
        if (not isinstance(stamp, datetime) or stamp.tzinfo is not None
                or stamp.microsecond or (stamp.year, stamp.month) != (year, month)
                or (previous is not None and stamp <= previous)):
            raise RatioDayError(
                f"ratio correction month row {index + 1} timestamp is invalid")
        previous = stamp
        try:
            values = tuple(float(value) for value in bar[1:5])
            volume = int(bar[5])
            if isinstance(bar[5], bool) or volume != bar[5]:
                raise ValueError("volume is not an exact integer")
            digest.update(struct.pack(
                ">q4dq", int((stamp - epoch).total_seconds()),
                *values, volume))
        except (TypeError, ValueError, OverflowError, struct.error) as exc:
            raise RatioDayError(
                f"ratio correction month row {index + 1} is not encodable") from exc
    if previous is None:
        raise RatioDayError("ratio correction month is empty")
    return digest.hexdigest()


def _prepare_month_file(root, ticker_dir, ticker, token, day,
                        write_path, bars):
    """Build candidate bytes inside a new durable transaction marker dir."""
    ticker_dir = Path(ticker_dir).resolve(strict=True)
    stage_dir = ticker_dir / STAGE_DIR_NAME
    _csv_path, canonical_parquet = _canonical_transaction_paths(
        root, ticker_dir, ticker, token, day, write_path=write_path)
    staged_path = stage_dir / canonical_parquet.name
    match = storage.FILENAME_RE.fullmatch(staged_path.name)
    if match is None:
        raise RatioDayError("ratio correction filename is invalid")
    try:
        year, month = int(match.group(2)), int(match.group(3))
        bars = sorted((tuple(bar) for bar in bars), key=lambda bar: bar[0])
        before_bits = _month_bits_sha256(bars, year=year, month=month)
        payload = storage._bars_to_parquet(bars)
        decoded = storage._parse_parquet_payload(
            payload, f"{staged_path.name} (correction round-trip)",
            validate=False)
        if (_month_bits_sha256(decoded, year=year, month=month)
                != before_bits):
            raise RatioDayError(
                "ratio correction candidate codec changed stored tuple bits")
        stats = {
            "rows": len(bars),
            "first": f"{storage.format_date(bars[0][0])} "
                     f"{storage.format_time(bars[0][0].time())}",
            "last": f"{storage.format_date(bars[-1][0])} "
                    f"{storage.format_time(bars[-1][0].time())}",
            "size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "mtime_ns": 0,
        }
    except RatioDayError:
        raise
    except (OSError, storage.StorageError, TypeError, ValueError) as exc:
        raise RatioDayError(
            "corrected ratio month could not be prepared") from exc

    created = False
    try:
        if stage_dir.exists() or stage_dir.is_symlink():
            raise RatioDayError(
                "a volatility correction transaction requires recovery")
        stage_dir.mkdir(exist_ok=False)
        created = True
        if (not stage_dir.is_dir()
                or stage_dir.resolve(strict=True).parent != ticker_dir):
            raise RatioDayError("ratio correction staging directory is unsafe")
        storage._atomic_write_bytes(staged_path, payload)
        if _raw_sha(staged_path) != stats["sha256"]:
            raise RatioDayError(
                "prepared ratio month failed its content verification")
        stats["mtime_ns"] = staged_path.stat().st_mtime_ns
    except Exception as exc:
        if created:
            try:
                _clear_stage_dir(stage_dir, staged_path.name)
            except Exception as cleanup_exc:
                raise _UnpublishedStageError(
                    "ratio correction preparation failed and its stage "
                    "requires recovery", new_stats=stats,
                    cleanup_error=cleanup_exc) from exc
        if isinstance(exc, RatioDayError):
            raise
        raise RatioDayError(
            "ratio correction staging directory is unavailable") from exc
    return staged_path, stats


def _stage_transaction(*, root, ticker_dir, ticker, token, day, original_path,
                       write_path, original_month_bytes,
                       original_manifest_bytes, manifest, new_stats,
                       correction):
    """Durably record both recoverable states before authoritative writes."""
    stage_dir = Path(ticker_dir) / STAGE_DIR_NAME
    staged_path = stage_dir / Path(write_path).name
    _canonical_transaction_paths(
        root, ticker_dir, ticker, token, day,
        original_path=original_path, write_path=write_path,
        candidate_name=staged_path.name)
    try:
        month_after = staged_path.read_bytes()
        if hashlib.sha256(month_after).hexdigest() != new_stats["sha256"]:
            raise RatioDayError("prepared ratio month changed before staging")
        storage._atomic_write_bytes(
            stage_dir / TXN_MANIFEST_BEFORE, original_manifest_bytes)
        storage._atomic_write_bytes(
            stage_dir / TXN_MONTH_BEFORE, original_month_bytes)
        storage._atomic_write_bytes(
            stage_dir / TXN_MONTH_AFTER, month_after)
        # Reuse the canonical encoder/generation bump, but write it only into
        # the transaction directory until the marker below is durable.
        storage.save_manifest(stage_dir, manifest)
        manifest_after = (stage_dir / TXN_MANIFEST_AFTER).read_bytes()
        marker = {
            "version": TXN_VERSION,
            "ticker": ticker,
            "kind_token": token,
            "day": day.isoformat(),
            "month": day.isoformat()[:7],
            "original_rel": Path(original_path).relative_to(
                ticker_dir).as_posix(),
            "write_rel": Path(write_path).relative_to(
                ticker_dir).as_posix(),
            "candidate_name": staged_path.name,
            "old_month_sha256": hashlib.sha256(
                original_month_bytes).hexdigest(),
            "new_month_sha256": new_stats["sha256"],
            "manifest_before_sha256": hashlib.sha256(
                original_manifest_bytes).hexdigest(),
            "manifest_after_sha256": hashlib.sha256(
                manifest_after).hexdigest(),
            "new_mtime_ns": new_stats["mtime_ns"],
            "correction": correction,
        }
        raw = json.dumps(
            marker, sort_keys=True, separators=(",", ":"),
            allow_nan=False).encode("utf-8")
        storage._atomic_write_bytes(stage_dir / TXN_MARKER_NAME, raw)
    except RatioDayError:
        raise
    except Exception as exc:  # noqa: BLE001 - no authority was written yet
        raise RatioDayError(
            "volatility correction transaction could not be staged") from exc
    return manifest_after


def _publish_prepared_month(staged_path, write_path, expected_sha,
                            expected_before_sha):
    """Atomically promote verified same-volume bytes without changing mtime."""
    staged_path = Path(staged_path)
    write_path = Path(write_path)
    if (staged_path.is_symlink() or not staged_path.is_file()
            or _raw_sha(staged_path) != expected_sha):
        raise RatioDayError(
            "prepared ratio month changed before promotion")
    write_present = write_path.exists() or write_path.is_symlink()
    if expected_before_sha is None:
        if write_present:
            raise RatioDayStale(
                "canonical Parquet destination appeared before promotion")
    elif (not write_present or write_path.is_symlink()
          or not write_path.is_file()
          or _raw_sha(write_path) != expected_before_sha):
        raise RatioDayStale(
            "canonical Parquet destination changed before promotion")
    delay = 0.05
    try:
        for attempt in range(6):
            try:
                os.replace(staged_path, write_path)
                break
            except PermissionError:
                if attempt == 5:
                    raise
                time.sleep(delay)
                delay *= 2
        else:  # pragma: no cover - loop either breaks or raises
            raise OSError("prepared month promotion did not run")
    except OSError as exc:
        raise RatioDayError(
            "prepared ratio month could not replace the stored month") from exc
    if _raw_sha(write_path) != expected_sha:
        raise RatioDayError(
            "published ratio month failed its content verification")


def _clear_stage_dir(stage_dir, candidate_name=None):
    """Remove only the exact files owned by one validated transaction."""
    stage_dir = Path(stage_dir)
    if not stage_dir.exists() and not stage_dir.is_symlink():
        return
    if stage_dir.is_symlink() or not stage_dir.is_dir():
        raise RatioDayError("ratio correction staging marker is unsafe")
    allowed = {
        TXN_MARKER_NAME, TXN_MANIFEST_BEFORE, TXN_MANIFEST_AFTER,
        TXN_MONTH_BEFORE, TXN_MONTH_AFTER,
    }
    if candidate_name:
        allowed.add(str(candidate_name))

    def owned_temp(name):
        return any(re.fullmatch(
            re.escape(target) + r"\.\d+-\d+-\d+\.tmp\Z", name)
                   is not None for target in allowed)

    try:
        entries = list(stage_dir.iterdir())
        for entry in entries:
            if (entry.is_symlink() or not entry.is_file()
                    or (entry.name not in allowed
                        and not owned_temp(entry.name))):
                raise RatioDayError(
                    "ratio correction staging directory has foreign content")
        # Once a transaction marker exists it is the only signal that recovery
        # still needs every backup. Remove it first after validating the whole
        # directory. A hard exit during the remaining deletions then resumes as
        # safe markerless cleanup instead of a marker with missing evidence.
        marker = stage_dir / TXN_MARKER_NAME
        if marker in entries:
            marker.unlink()
        for entry in entries:
            if entry != marker:
                entry.unlink()
        stage_dir.rmdir()
    except RatioDayError:
        raise
    except OSError as exc:
        raise RatioDayError(
            "ratio correction staging marker could not be cleared") from exc


def _cleanup_unpublished_stage(stage_dir, candidate_name=None):
    """Clear a preparation that could not have touched authoritative files."""
    try:
        _clear_stage_dir(stage_dir, candidate_name)
    except RatioDayError as exc:
        # The directory remains a fail-closed marker for explicit recovery.
        return exc
    return None


def _read_txn_file(stage_dir, name):
    path = Path(stage_dir) / name
    if path.is_symlink() or not path.is_file():
        raise RatioDayError(
            f"volatility correction transaction is missing {name}")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise RatioDayError(
            f"volatility correction transaction cannot read {name}") from exc


def _recover_pending_transaction(root, ticker_dir, ticker):
    """Resolve a prior hard-exit transaction to one internally consistent state."""
    stage_dir = Path(ticker_dir) / STAGE_DIR_NAME
    if not stage_dir.exists() and not stage_dir.is_symlink():
        return None
    if stage_dir.is_symlink() or not stage_dir.is_dir():
        raise RatioDayError("volatility correction transaction marker is unsafe")
    marker_path = stage_dir / TXN_MARKER_NAME
    if not marker_path.exists():
        candidate_bases = set()
        for entry in stage_dir.iterdir():
            if not entry.is_file() or entry.is_symlink():
                continue
            if storage.FILENAME_RE.fullmatch(entry.name):
                candidate_bases.add(entry.name)
                continue
            match = re.fullmatch(
                r"(.+)\.\d+-\d+-\d+\.tmp\Z", entry.name)
            if (match is not None
                    and storage.FILENAME_RE.fullmatch(match.group(1))):
                candidate_bases.add(match.group(1))
        if len(candidate_bases) > 1:
            raise RatioDayError(
                "unpublished volatility correction has multiple candidates")
        candidate = next(iter(candidate_bases), None)
        _clear_stage_dir(stage_dir, candidate)
        return {"status": "aborted", "reason": "unpublished preparation"}
    try:
        marker = json.loads(
            _read_txn_file(stage_dir, TXN_MARKER_NAME).decode("utf-8"),
            object_pairs_hook=_unique_json_object)
    except (UnicodeError, ValueError) as exc:
        raise RatioDayError(
            "volatility correction transaction marker is malformed") from exc
    expected_fields = {
        "version", "ticker", "kind_token", "day", "month",
        "original_rel", "write_rel", "candidate_name",
        "old_month_sha256", "new_month_sha256",
        "manifest_before_sha256", "manifest_after_sha256",
        "new_mtime_ns", "correction",
    }
    if not isinstance(marker, dict) or set(marker) != expected_fields:
        raise RatioDayError(
            "volatility correction transaction fields are invalid")
    day = _day(marker.get("day"))
    token = _kind_token(marker.get("kind_token"), for_day=day)
    if (marker.get("version") != TXN_VERSION
            or marker.get("ticker") != ticker
            or marker.get("month") != day.isoformat()[:7]):
        raise RatioDayError(
            "volatility correction transaction identity is invalid")
    candidate_name = marker.get("candidate_name")
    if (not isinstance(candidate_name, str)
            or storage.FILENAME_RE.fullmatch(candidate_name) is None):
        raise RatioDayError(
            "volatility correction candidate filename is invalid")
    old_sha = _sha256(marker.get("old_month_sha256"), "old month SHA-256")
    new_sha = _sha256(marker.get("new_month_sha256"), "new month SHA-256")
    before_manifest_sha = _sha256(
        marker.get("manifest_before_sha256"), "old manifest SHA-256")
    after_manifest_sha = _sha256(
        marker.get("manifest_after_sha256"), "new manifest SHA-256")
    if (isinstance(marker.get("new_mtime_ns"), bool)
            or not isinstance(marker.get("new_mtime_ns"), int)
            or marker["new_mtime_ns"] <= 0):
        raise RatioDayError(
            "volatility correction transaction mtime is invalid")

    def resolved_rel(name, label):
        value = marker.get(name)
        if (not isinstance(value, str) or not value or "\\" in value
                or Path(value).is_absolute()):
            raise RatioDayError(
                f"volatility correction {label} path is invalid")
        path = (Path(ticker_dir) / Path(value)).resolve(strict=False)
        if not path.is_relative_to(Path(ticker_dir)):
            raise RatioDayError(
                f"volatility correction {label} path escapes the ticker")
        return path

    original_path = resolved_rel("original_rel", "original")
    write_path = resolved_rel("write_rel", "write")
    canonical_csv, _canonical_parquet = _canonical_transaction_paths(
        root, ticker_dir, ticker, token, day,
        original_path=original_path, write_path=write_path,
        candidate_name=candidate_name)
    manifest_before = _read_txn_file(stage_dir, TXN_MANIFEST_BEFORE)
    manifest_after = _read_txn_file(stage_dir, TXN_MANIFEST_AFTER)
    month_before = _read_txn_file(stage_dir, TXN_MONTH_BEFORE)
    month_after = _read_txn_file(stage_dir, TXN_MONTH_AFTER)
    if (hashlib.sha256(manifest_before).hexdigest() != before_manifest_sha
            or hashlib.sha256(manifest_after).hexdigest() != after_manifest_sha
            or hashlib.sha256(month_before).hexdigest() != old_sha
            or hashlib.sha256(month_after).hexdigest() != new_sha):
        raise RatioDayError(
            "volatility correction transaction backup hash mismatch")
    try:
        after_payload = json.loads(
            manifest_after.decode("utf-8"),
            object_pairs_hook=_unique_json_object)
        after_entry = (((after_payload.get("intervals") or {}).get(token)
                        or {}).get("months") or {}).get(marker["month"]) or {}
        after_ledger = after_entry.get("value_corrections") or []
    except (AttributeError, UnicodeError, ValueError) as exc:
        raise RatioDayError(
            "volatility correction staged manifest is malformed") from exc
    if (after_entry.get("sha256") != new_sha or not after_ledger
            or after_ledger[-1] != marker.get("correction")):
        raise RatioDayError(
            "volatility correction staged manifest lacks its exact ledger")
    manifest_path = Path(ticker_dir) / storage.MANIFEST_NAME
    current_manifest = _read_txn_file(
        Path(ticker_dir), storage.MANIFEST_NAME)
    try:
        active = storage.find_month_file(
            root, ticker, day.year, day.month, token)
        active = None if active is None else Path(active).resolve(strict=False)
    except (OSError, storage.StorageError) as exc:
        raise RatioDayError(
            "volatility correction cannot resolve the active month") from exc
    old_state = bool(
        _exact_file_sha(original_path, old_sha)
        and (original_path == write_path
             or _path_absent(write_path))
        and active == original_path)
    new_state = bool(
        _exact_file_sha(write_path, new_sha) and active == write_path)
    manifest_state = (
        "before" if current_manifest == manifest_before else
        "after" if current_manifest == manifest_after else "other")
    if manifest_state == "after" and old_state:
        storage._atomic_write_bytes(manifest_path, manifest_before)
        if manifest_path.read_bytes() != manifest_before:
            raise RatioDayError(
                "volatility correction could not restore the old manifest")
        outcome = "aborted"
    elif manifest_state == "before" and new_state:
        storage._atomic_write_bytes(manifest_path, manifest_after)
        if manifest_path.read_bytes() != manifest_after:
            raise RatioDayError(
                "volatility correction could not restore the new manifest")
        outcome = "committed"
    elif manifest_state == "before" and old_state:
        outcome = "aborted"
    elif manifest_state == "after" and new_state:
        outcome = "committed"
    else:
        raise RatioDayError(
            "volatility correction transaction state is indeterminate")
    if outcome == "committed":
        # The committed Parquet is already authoritative, but the marker must
        # remain until the exact snapshotted legacy twin is retired.  A CSV
        # that appeared or changed meanwhile is never unlinked.
        _retire_exact_csv(canonical_csv, old_sha)
    _clear_stage_dir(stage_dir, candidate_name)
    return {
        "status": outcome,
        "ticker": ticker,
        "kind_token": token,
        "day": day.isoformat(),
        "old_month_sha256": old_sha,
        "new_month_sha256": new_sha,
    }


def recover_ratio_transaction(root, ticker, *, manifest_lock=None):
    """Recover one ticker's reconcile WAL before any queue-writing audit."""
    ticker = _ticker(ticker)
    root, ticker_dir = _root_and_ticker_dir(root, ticker)
    guard = manifest_lock if manifest_lock is not None else nullcontext()
    with guard, storage.ticker_transaction(ticker_dir):
        return _recover_pending_transaction(root, ticker_dir, ticker)


def _commit_state_evidence(*, root, ticker, token, day, original_path,
                           write_path, original_month_sha, new_month_sha,
                           manifest_path, original_manifest_bytes, correction,
                           restore_errors, month_write_completed):
    """Describe the exact observable bank state after a rollback attempt."""
    original_path = Path(original_path)
    write_path = Path(write_path)
    original_sha = (_raw_sha(original_path)
                    if not original_path.is_symlink()
                    and original_path.is_file() else None)
    written_sha = (_raw_sha(write_path)
                   if not write_path.is_symlink()
                   and write_path.is_file() else None)
    same_path = original_path == write_path
    try:
        active = storage.find_month_file(
            root, ticker, day.year, day.month, token)
        active = None if active is None else Path(active).resolve(strict=False)
    except (OSError, storage.StorageError):
        active = None
    original_resolved = original_path.resolve(strict=False)
    written_resolved = write_path.resolve(strict=False)

    if same_path and _exact_file_sha(original_path, original_month_sha):
        month_state, bank_written = "original", False
    elif same_path and _exact_file_sha(write_path, new_month_sha):
        month_state, bank_written = "corrected", True
    elif (not same_path
          and _exact_file_sha(original_path, original_month_sha)
          and _path_absent(write_path) and active == original_resolved):
        month_state, bank_written = "original", False
    elif (not same_path and _exact_file_sha(write_path, new_month_sha)
          and active == written_resolved
          and (_exact_file_sha(original_path, original_month_sha)
               or _path_absent(original_path))):
        month_state = (
            "corrected-active-original-retained"
            if _exact_file_sha(original_path, original_month_sha)
            else "corrected")
        bank_written = True
    else:
        month_state, bank_written = "indeterminate", None

    manifest_raw = None
    manifest_path = Path(manifest_path)
    if not manifest_path.is_symlink() and manifest_path.is_file():
        try:
            manifest_raw = manifest_path.read_bytes()
        except OSError:
            pass
    correction_recorded = None
    if manifest_raw == original_manifest_bytes:
        manifest_state, correction_recorded = "original", False
    elif manifest_raw is None:
        manifest_state = "unreadable"
    else:
        try:
            observed = json.loads(
                manifest_raw.decode("utf-8"),
                object_pairs_hook=_unique_json_object)
            observed_entry = (((observed.get("intervals") or {}).get(token)
                               or {}).get("months") or {}).get(
                                   day.isoformat()[:7]) or {}
            observed_ledger = observed_entry.get("value_corrections") or []
            correction_recorded = bool(
                observed_entry.get("sha256") == new_month_sha
                and observed_ledger and observed_ledger[-1] == correction)
            manifest_state = (
                "corrected" if correction_recorded else "other-valid")
        except (AttributeError, TypeError, UnicodeError, ValueError,
                RatioDayError):
            manifest_state = "malformed"

    rollback_verified = bool(
        month_state == "original"
        and manifest_state == "original"
        and active == original_resolved)
    return {
        "rollback_verified": rollback_verified,
        "month_write_completed": bool(month_write_completed),
        "bank_written": bank_written,
        "correction_recorded": correction_recorded,
        "month_state": month_state,
        "manifest_state": manifest_state,
        "original_month_file": original_path.name,
        "written_month_file": write_path.name,
        "old_month_sha256": original_month_sha,
        "new_month_sha256": new_month_sha,
        "observed_original_sha256": original_sha,
        "observed_written_sha256": written_sha,
        "correction": dict(correction),
        "restore_errors": [str(item)[:MAX_TEXT] for item in restore_errors[:4]],
    }


def _rollback_post_manifest_failure(*, root, ticker, token, day,
                                    original_path, write_path,
                                    original_month_bytes,
                                    original_month_sha, new_month_sha,
                                    manifest_path, original_manifest_bytes,
                                    manifest_after_bytes,
                                    correction, month_write_completed):
    """Restore and verify exact pre-correction month/format + manifest bytes."""
    original_path = Path(original_path).resolve(strict=False)
    write_path = Path(write_path).resolve(strict=False)
    _canonical_transaction_paths(
        root, Path(manifest_path).parent, ticker, token, day,
        original_path=original_path, write_path=write_path,
        candidate_name=write_path.name)
    errors = []
    if hashlib.sha256(original_month_bytes).hexdigest() != original_month_sha:
        errors.append("old month backup changed; authoritative bytes untouched")
    elif original_path == write_path:
        if _exact_file_sha(original_path, new_month_sha):
            try:
                storage._atomic_write_bytes(original_path, original_month_bytes)
            except Exception as exc:  # noqa: BLE001 - evidence decides certainty
                errors.append(
                    f"month restore failed: {type(exc).__name__}: {exc}")
        elif not _exact_file_sha(original_path, original_month_sha):
            errors.append(
                "canonical month is neither the recorded old nor new SHA; "
                "unproven bytes were not overwritten")
    else:
        if not _exact_file_sha(original_path, original_month_sha):
            errors.append(
                "legacy source month changed; unproven bytes were not "
                "overwritten")
        if write_path.exists() or write_path.is_symlink():
            if (write_path.is_symlink() or not write_path.is_file()
                    or _raw_sha(write_path) != new_month_sha):
                errors.append(
                    "Parquet replacement is not the recorded new SHA; "
                    "unproven bytes were not removed")
            else:
                try:
                    write_path.unlink()
                except OSError as exc:
                    errors.append(
                        "new-format removal failed: "
                        f"{type(exc).__name__}: {exc}")
    try:
        active = storage.find_month_file(
            root, ticker, day.year, day.month, token)
        active = None if active is None else Path(active).resolve(strict=False)
    except (OSError, storage.StorageError):
        active = None
    original_resolved = original_path.resolve(strict=False)
    month_restored = bool(
        _exact_file_sha(original_path, original_month_sha)
        and (write_path == original_path or _path_absent(write_path))
        and active == original_resolved)
    try:
        manifest_path = Path(manifest_path)
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise OSError("manifest path is not an exact regular file")
        current_manifest_bytes = manifest_path.read_bytes()
    except OSError:
        current_manifest_bytes = None
    if month_restored and current_manifest_bytes == manifest_after_bytes:
        try:
            storage._atomic_write_bytes(manifest_path, original_manifest_bytes)
        except Exception as exc:  # noqa: BLE001 - final evidence decides certainty
            errors.append(f"manifest restore failed: {type(exc).__name__}: {exc}")
    elif (month_restored
          and current_manifest_bytes not in {
              original_manifest_bytes, manifest_after_bytes}):
        errors.append(
            "manifest is neither the recorded old nor new state; unproven "
            "bytes were not overwritten")
    elif not month_restored and current_manifest_bytes != original_manifest_bytes:
        errors.append(
            "manifest retained because the original month was not restored")
    return _commit_state_evidence(
        root=root, ticker=ticker, token=token, day=day,
        original_path=original_path, write_path=write_path,
        original_month_sha=original_month_sha,
        new_month_sha=new_month_sha, manifest_path=manifest_path,
        original_manifest_bytes=original_manifest_bytes,
        correction=correction, restore_errors=errors,
        month_write_completed=month_write_completed)


def replace_ratio_day(root, snapshot, served, run_id, *, reasons,
                      source="IBKR:vol-value-reconcile", manifest_lock=None):
    """CAS-replace only the snapshotted day and durably record the correction.

    The old month SHA *and* old day fingerprint must still match.  The exact
    identity floor and pinned conId are re-read while the optional per-ticker
    manifest lock is held. Candidate bytes are prepared off-tree, the manifest
    correction record is published first, and only then is the prepared month
    atomically promoted. A process death can therefore leave old data with
    visible queue debt, or corrected data with its ledger, but never corrected
    data whose ledger was not durable. The caller finalizes the queue only
    after this function returns.
    """
    snap = _snapshot(snapshot)
    fresh = _served(served)
    _matching_context(snap, fresh)
    run_id = _text(run_id, "reconcile run id")
    source = _text(source, "reconcile source")
    reasons = canonical_anomaly_reasons(reasons)
    ticker = snap["ticker"]
    day = _day(snap["day"])
    token = snap["kind_token"]
    root, ticker_dir = _root_and_ticker_dir(root, ticker)
    guard = manifest_lock if manifest_lock is not None else nullcontext()
    with guard, storage.ticker_transaction(ticker_dir):
        manifest = _load_manifest_strict(ticker_dir, ticker)
        manifest_path = ticker_dir / storage.MANIFEST_NAME
        original_manifest_bytes = _rollback_manifest_bytes(
            ticker_dir, manifest)
        # The entire identity/source-reach context is a CAS token.  A floor
        # change can be material even when this particular day remains after
        # the new floor, so a mere ``day >= floor`` check is insufficient.
        _current_identity_context(manifest, snap)
        current_path = _month_path(root, ticker_dir, ticker, token, day)
        if current_path.name != snap["month_file"]:
            raise RatioDayStale("stored month format/path changed after snapshot")
        bars, target, fingerprint, old_value, current_stats = _month_parts(
            current_path, day)
        if current_stats.get("sha256") != snap["month_sha256"]:
            raise RatioDayStale("stored month changed after snapshot")
        if fingerprint["sha256"] != snap["stored_day_sha256"]:
            raise RatioDayStale("stored ratio day changed after snapshot")
        if fingerprint["bars"] != snap["stored_bars"]:
            raise RatioDayStale("stored ratio-day bar count changed after snapshot")
        if _grid_sha256(target) != snap["stored_grid_sha256"]:
            raise RatioDayStale(
                "stored ratio-day timestamp grid changed after snapshot")
        original_month_bytes = _rollback_month_bytes(
            current_path, current_stats["sha256"])

        section = manifest["intervals"].get(token)
        if not isinstance(section, dict):
            raise RatioDayStale("manifest ratio interval disappeared")
        months = section.get("months")
        if not isinstance(months, dict):
            raise RatioDayStale("manifest ratio months became malformed")
        key = snap["month"]
        old_entry = months.get(key)
        if (not isinstance(old_entry, dict)
                or old_entry.get("sha256") != current_stats.get("sha256")):
            raise RatioDayStale(
                "manifest target month changed or no longer matches the bytes")
        old_entry = dict(old_entry)
        ledger = _correction_ledger(old_entry)  # validate before the data write

        candidate = [bar for bar in bars if bar[0].date() != day]
        candidate.extend(fresh["bars"])
        candidate.sort(key=lambda bar: bar[0])
        gate_result = {"months": {}}
        try:
            ibkr._guard_ratio_month_commit(
                root, ticker, token, (day.year, day.month), fresh["bars"],
                gate_result, {"manifest": manifest})
        except ibkr.SeriesHalt as exc:
            raise RatioDayError(
                f"volatility ingest gate refused the corrected day: {exc}") from exc
        write_path = storage.month_file_path(
            root, ticker, day.year, day.month, token)
        canonical_csv, canonical_parquet = _canonical_transaction_paths(
            root, ticker_dir, ticker, token, day,
            original_path=current_path, write_path=write_path,
            candidate_name=Path(write_path).name)
        if Path(write_path).resolve(strict=False) != canonical_parquet:
            raise RatioDayStale(
                "ratio correction destination is non-canonical")
        if (current_path != write_path
                and (Path(write_path).exists()
                     or Path(write_path).is_symlink())):
            raise RatioDayStale(
                "canonical Parquet destination was not absent at snapshot")
        staged_path = ticker_dir / STAGE_DIR_NAME / write_path.name
        try:
            staged_path, new_stats = _prepare_month_file(
                root, ticker_dir, ticker, token, day, write_path, candidate)
        except _UnpublishedStageError as exc:
            new_stats = exc.new_stats
            correction = _correction_record(
                snapshot=snap, served=fresh, run_id=run_id, source=source,
                reasons=reasons, old_value=old_value,
                old_day_sha256=fingerprint["sha256"],
                old_month_sha256=current_stats["sha256"],
                new_month_sha256=new_stats["sha256"])
            evidence = _commit_state_evidence(
                root=root, ticker=ticker, token=token, day=day,
                original_path=current_path, write_path=write_path,
                original_month_sha=current_stats["sha256"],
                new_month_sha=new_stats["sha256"],
                manifest_path=manifest_path,
                original_manifest_bytes=original_manifest_bytes,
                correction=correction,
                restore_errors=[
                    f"stage cleanup failed: "
                    f"{type(exc.cleanup_error).__name__}: "
                    f"{exc.cleanup_error}"],
                month_write_completed=False)
            evidence["rollback_verified"] = False
            raise RatioDayCommitError(
                "ratio correction preparation left a recovery marker; "
                "authoritative bank state remains guarded",
                evidence=evidence) from exc
        correction = _correction_record(
            snapshot=snap, served=fresh, run_id=run_id, source=source,
            reasons=reasons, old_value=old_value,
            old_day_sha256=fingerprint["sha256"],
            old_month_sha256=current_stats["sha256"],
            new_month_sha256=new_stats["sha256"])
        new_entry = dict(old_entry)
        new_entry.update(new_stats)
        new_entry["status"] = "present"
        new_entry.setdefault("source", "found-by-scan")
        ledger.append(correction)
        new_entry["value_corrections"] = ledger
        months[key] = new_entry
        try:
            manifest_after = _stage_transaction(
                root=root, ticker_dir=ticker_dir, ticker=ticker,
                token=token, day=day,
                original_path=current_path, write_path=write_path,
                original_month_bytes=original_month_bytes,
                original_manifest_bytes=original_manifest_bytes,
                manifest=manifest, new_stats=new_stats,
                correction=correction)
        except Exception as exc:
            cleanup_error = _cleanup_unpublished_stage(
                ticker_dir / STAGE_DIR_NAME, write_path.name)
            if cleanup_error is not None:
                evidence = _commit_state_evidence(
                    root=root, ticker=ticker, token=token, day=day,
                    original_path=current_path, write_path=write_path,
                    original_month_sha=current_stats["sha256"],
                    new_month_sha=new_stats["sha256"],
                    manifest_path=manifest_path,
                    original_manifest_bytes=original_manifest_bytes,
                    correction=correction,
                    restore_errors=[
                        f"stage cleanup failed: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"],
                    month_write_completed=False)
                evidence["rollback_verified"] = False
                raise RatioDayCommitError(
                    "ratio correction staging failed and left a recovery "
                    "marker; authoritative bank state remains guarded",
                    evidence=evidence) from exc
            raise
        try:
            # Manifest-first is deliberate. If the process dies before the
            # promotion below, the old anomalous month remains authoritative
            # and the queue remains owed. If it dies after promotion, this
            # correction record already binds the new month SHA.
            if (manifest_path.read_bytes() != original_manifest_bytes
                    or _raw_sha(current_path) != current_stats["sha256"]
                    or (current_path != write_path
                        and (Path(write_path).exists()
                             or Path(write_path).is_symlink()))):
                raise RatioDayStale(
                    "ratio month/manifest changed before publication")
            storage._atomic_write_bytes(manifest_path, manifest_after)
            verified = _load_manifest_strict(ticker_dir, ticker)
            verified_section = verified["intervals"].get(token) or {}
            verified_entry = (verified_section.get("months") or {}).get(key) or {}
            verified_ledger = verified_entry.get("value_corrections") or []
            if (verified_entry.get("sha256") != new_stats["sha256"]
                    or not verified_ledger
                    or verified_ledger[-1] != correction):
                raise RatioDayError(
                    "ratio month manifest verification failed")
            _publish_prepared_month(
                staged_path, write_path, new_stats["sha256"],
                (current_stats["sha256"]
                 if current_path == write_path else None))
            final_manifest = _load_manifest_strict(ticker_dir, ticker)
            final_section = final_manifest["intervals"].get(token) or {}
            final_entry = (final_section.get("months") or {}).get(key) or {}
            final_ledger = final_entry.get("value_corrections") or []
            active_path = storage.find_month_file(
                root, ticker, day.year, day.month, token)
            active_path = (None if active_path is None else
                           Path(active_path).resolve(strict=False))
            if (final_entry.get("sha256") != new_stats["sha256"]
                    or not final_ledger
                    or final_ledger[-1] != correction
                    or not _exact_file_sha(
                        write_path, new_stats["sha256"])
                    or active_path != write_path.resolve(strict=False)):
                raise RatioDayError(
                    "ratio month/manifest final verification failed")
        except Exception as exc:  # noqa: BLE001 - rollback every catchable stage
            month_write_completed = (
                _exact_file_sha(write_path, new_stats["sha256"]))
            evidence = _rollback_post_manifest_failure(
                root=root, ticker=ticker, token=token, day=day,
                original_path=current_path, write_path=write_path,
                original_month_bytes=original_month_bytes,
                original_month_sha=current_stats["sha256"],
                new_month_sha=new_stats["sha256"],
                manifest_path=manifest_path,
                original_manifest_bytes=original_manifest_bytes,
                manifest_after_bytes=manifest_after,
                correction=correction,
                month_write_completed=month_write_completed)
            if evidence["rollback_verified"]:
                try:
                    _clear_stage_dir(
                        ticker_dir / STAGE_DIR_NAME, write_path.name)
                except Exception as cleanup_exc:  # noqa: BLE001 - marker is debt
                    evidence["rollback_verified"] = False
                    evidence["restore_errors"] = (
                        list(evidence.get("restore_errors") or [])
                        + [f"stage cleanup failed: {type(cleanup_exc).__name__}: "
                           f"{cleanup_exc}"[:MAX_TEXT]])[:4]
            if evidence["rollback_verified"]:
                message = (
                    "ratio month/manifest publication failed; exact rollback "
                    "verified and queue left unresolved")
            else:
                message = (
                    "ratio month/manifest publication failed and rollback "
                    "could not be verified; explicit recovery required")
            raise RatioDayCommitError(
                message, evidence=evidence) from exc

        # Once the verified Parquet target is canonical, retiring a readable
        # legacy CSV is safe and repeatable. Do it before removing the recovery
        # marker so a hard exit cannot strand an untracked twin.
        csv_twin = canonical_csv
        if csv_twin != write_path:
            try:
                _retire_exact_csv(csv_twin, current_stats["sha256"])
            except RatioDayError as exc:
                evidence = _commit_state_evidence(
                    root=root, ticker=ticker, token=token, day=day,
                    original_path=current_path, write_path=write_path,
                    original_month_sha=current_stats["sha256"],
                    new_month_sha=new_stats["sha256"],
                    manifest_path=manifest_path,
                    original_manifest_bytes=original_manifest_bytes,
                    correction=correction,
                    restore_errors=[
                        f"CSV twin cleanup failed: {type(exc).__name__}: "
                        f"{exc}"],
                    month_write_completed=True)
                evidence["rollback_verified"] = False
                raise RatioDayCommitError(
                    "ratio month and correction ledger committed, but the "
                    "legacy CSV twin was not safely retired; recovery marker "
                    "retained", evidence=evidence) from exc

        try:
            _clear_stage_dir(ticker_dir / STAGE_DIR_NAME, write_path.name)
        except Exception as exc:  # committed, but marker still blocks all writers
            evidence = _commit_state_evidence(
                root=root, ticker=ticker, token=token, day=day,
                original_path=current_path, write_path=write_path,
                original_month_sha=current_stats["sha256"],
                new_month_sha=new_stats["sha256"],
                manifest_path=manifest_path,
                original_manifest_bytes=original_manifest_bytes,
                correction=correction,
                restore_errors=[
                    f"stage cleanup failed: {type(exc).__name__}: {exc}"],
                month_write_completed=True)
            evidence["rollback_verified"] = False
            raise RatioDayCommitError(
                "ratio month and correction ledger committed, but the "
                "recovery marker could not be cleared", evidence=evidence) from exc

    return {
        "status": "corrected",
        "ticker": ticker,
        "kind_token": token,
        "day": snap["day"],
        "month": snap["month"],
        "month_sha256": new_stats["sha256"],
        "old_month_sha256": snap["month_sha256"],
        "new_month_sha256": new_stats["sha256"],
        "old_day_sha256": snap["stored_day_sha256"],
        "new_day_sha256": fresh["served_day_sha256"],
        "old_value": old_value,
        "new_value": fresh["served_value"],
        "bars": fresh["served_bars"],
        "correction": correction,
    }


__all__ = [
    "ANOMALY_REASONS",
    "CORRECTION_VERSION",
    "FETCH_VERSION",
    "RatioDayCommitError",
    "RatioDayError",
    "RatioDayStale",
    "SNAPSHOT_VERSION",
    "canonical_anomaly_reasons",
    "fetch_ratio_day",
    "finalization_guard",
    "identity_floor",
    "kind_earliest",
    "recover_ratio_transaction",
    "replace_ratio_day",
    "settlement_evidence",
    "snapshot_ratio_day",
]
