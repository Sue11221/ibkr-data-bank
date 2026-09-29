"""Offline volatility-value audit, settlement registry, and export evidence.

The market-data tree is read-only here.  Optional writes are limited to three
atomic JSON sidecars at the storage root: the audit report, the refetch queue,
and the source-confirmed settlement registry.  No network, GUI, or port code is
imported.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re
import struct
import threading
import time
from contextlib import contextmanager, nullcontext
from pathlib import Path
from statistics import median

import market_calendar
import numpy as np
import operation_gate
import stock_storage as storage


VOL_VALUE_AUDIT = True

VOL_HARD_CEILING = 10.0
UNIT_FLIP_RATIO = 50.0
JUMP_RATIO = 8.0
IV_HVOL_BAND = (0.05, 20.0)
IV_ZERO_RUN = 3
SETTLE_TOL = 0.0001

AUDIT_BASENAME = "_vol_value_audit.json"
QUEUE_BASENAME = "_vol_value_queue.json"
SETTLED_BASENAME = "_vol_value_settled.json"
AUDIT_VERSION = 1
QUEUE_VERSION = 1

MAX_SIDECAR_BYTES = 16 * 1024 * 1024
MAX_SETTLED_ENTRIES = 250_000
MAX_QUEUE_ROWS = 500_000
MAX_ERRORS = 4_096
MAX_TEXT = 320
_READ_ATTEMPTS = 4
_WRITE_LOCK = threading.RLock()
_SIDECAR_LOCK_WAIT_S = 30.0
_SIDECAR_LOCK_POLL_S = 0.02
_CLOSE_SETTLE_REASONS = frozenset({
    "jump_up", "jump_down", "iv_zero_run", "iv_hvol_band",
})
_FULL_DAY_SETTLE_REASONS = frozenset({"unit_flip_month"})
_RECONCILE_EVIDENCE_METHOD = "ibkr_ratio_day_refetch"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


class VolValueAuditError(RuntimeError):
    """The audit or one of its durable evidence sidecars is unsafe to use."""


class ReconcileFinalizeError(VolValueAuditError):
    """A reconcile publication failed after one or more durable stages.

    ``registry_committed`` and ``queue_resolved`` describe only writes whose
    atomic writer returned successfully.  Callers can therefore report the
    durable action independently from any queue debt that remains.
    """

    def __init__(self, reason, *, action, registry_committed=False,
                 queue_resolved=False, registry_source=None,
                 queue_source=None):
        super().__init__(reason)
        if action not in {"settled", "corrected"}:
            raise ValueError("invalid reconcile finalization action")
        if not isinstance(registry_committed, bool):
            raise ValueError("registry_committed must be boolean")
        if not isinstance(queue_resolved, bool):
            raise ValueError("queue_resolved must be boolean")
        self.action = action
        self.registry_committed = registry_committed
        self.queue_resolved = queue_resolved
        self.registry_source = registry_source
        self.queue_source = queue_source


def _record_error(errors, item):
    """Keep diagnostics bounded without hiding the scan's incomplete state."""
    if len(errors) < MAX_ERRORS:
        errors.append(item)
    elif len(errors) == MAX_ERRORS:
        errors.append({
            "source": "vol_value_audit",
            "error": "additional audit errors suppressed",
        })


def _bounded_text(value, label, *, allow_empty=False):
    if not isinstance(value, str):
        raise VolValueAuditError(f"invalid {label}")
    text = value.strip()
    if ((not text and not allow_empty) or len(text) > MAX_TEXT
            or any(ch in text for ch in "\r\n\x00")):
        raise VolValueAuditError(f"invalid {label}")
    return text


def _number(value, label, *, allow_none=False):
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise VolValueAuditError(f"invalid {label}")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise VolValueAuditError(f"invalid {label}") from exc
    if not math.isfinite(number):
        raise VolValueAuditError(f"invalid {label}")
    return number


def _sha256(value, label):
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise VolValueAuditError(f"invalid {label}")
    return value


def _day_fingerprint_parts(rows):
    """Hash canonical timestamp/OHLCV tuples, including invalid float bits."""
    digest = hashlib.sha256()
    count = 0
    for stamp, open_, high, low, close, volume in rows:
        try:
            stamp = int(stamp)
            values = tuple(float(value) for value in (
                open_, high, low, close, volume))
            packed = struct.pack(">q5d", stamp, *values)
        except (TypeError, ValueError, OverflowError, struct.error) as exc:
            raise VolValueAuditError(
                "cannot fingerprint volatility day") from exc
        digest.update(packed)
        count += 1
    if count <= 0:
        raise VolValueAuditError("cannot fingerprint an empty volatility day")
    return {"sha256": digest.hexdigest(), "bars": count}


def day_fingerprint(bars):
    """Return a deterministic fingerprint for one timestamp-sorted stored day."""
    try:
        rows = list(bars)
    except TypeError as exc:
        raise VolValueAuditError("volatility day bars must be iterable") from exc
    normalized = []
    previous = None
    epoch = dt.datetime(1970, 1, 1)
    for bar in rows:
        if not isinstance(bar, (tuple, list)) or len(bar) != 6:
            raise VolValueAuditError("volatility day bar shape is invalid")
        stamp = bar[0]
        if (not isinstance(stamp, dt.datetime) or stamp.tzinfo is not None
                or stamp.microsecond):
            raise VolValueAuditError("volatility day timestamp is invalid")
        if previous is not None and stamp <= previous:
            raise VolValueAuditError(
                "volatility day timestamps must be strictly increasing")
        previous = stamp
        normalized.append((int((stamp - epoch).total_seconds()), *bar[1:]))
    return _day_fingerprint_parts(normalized)


def _reconcile_evidence(value):
    if not isinstance(value, dict) or set(value) != {
            "version", "method", "what_to_show", "stored_day_sha256",
            "served_day_sha256", "bars", "request_count", "reasons",
            "month_sha256"}:
        raise VolValueAuditError("settlement refetch evidence fields are invalid")
    if value.get("version") != 1 or value.get("method") != _RECONCILE_EVIDENCE_METHOD:
        raise VolValueAuditError("settlement refetch evidence version is invalid")
    what = _bounded_text(value.get("what_to_show"), "refetch whatToShow")
    if what not in {"OPTION_IMPLIED_VOLATILITY", "HISTORICAL_VOLATILITY"}:
        raise VolValueAuditError("settlement refetch whatToShow is invalid")
    bars = value.get("bars")
    requests = value.get("request_count")
    if (isinstance(bars, bool) or not isinstance(bars, int) or bars <= 0
            or bars > 2_000_000):
        raise VolValueAuditError("settlement refetch bar count is invalid")
    if requests != 1:
        raise VolValueAuditError("settlement refetch must use exactly one request")
    reasons = value.get("reasons")
    if (not isinstance(reasons, list) or not reasons or len(reasons) > 16):
        raise VolValueAuditError("settlement refetch reasons are invalid")
    reasons = sorted({_bounded_text(item, "refetch reason") for item in reasons})
    if reasons != value["reasons"]:
        raise VolValueAuditError("settlement refetch reasons are not canonical")
    return {
        "version": 1,
        "method": _RECONCILE_EVIDENCE_METHOD,
        "what_to_show": what,
        "stored_day_sha256": _sha256(
            value.get("stored_day_sha256"), "stored-day SHA-256"),
        "served_day_sha256": _sha256(
            value.get("served_day_sha256"), "served-day SHA-256"),
        "bars": bars,
        "request_count": 1,
        "reasons": reasons,
        "month_sha256": _sha256(
            value.get("month_sha256"), "month SHA-256"),
    }


def _ticker(value):
    if not isinstance(value, str) or not value.strip() or len(value) > MAX_TEXT:
        raise VolValueAuditError(f"invalid ticker {value!r}")
    try:
        return storage.canonical_ticker(value)
    except Exception as exc:  # noqa: BLE001 - normalize storage validation
        raise VolValueAuditError(f"invalid ticker {value!r}") from exc


def _kind_token(value):
    token = str(value or "").strip()
    if (not storage.INTERVAL_RE.fullmatch(token)
            or storage.kind_of(token) not in storage.RATIO_KINDS):
        raise VolValueAuditError(f"invalid volatility kind token {value!r}")
    return token


def _day(value):
    token = str(value or "").strip()
    try:
        parsed = dt.date.fromisoformat(token)
    except (TypeError, ValueError) as exc:
        raise VolValueAuditError(f"invalid ISO day {value!r}") from exc
    if token != parsed.isoformat():
        raise VolValueAuditError(f"invalid ISO day {value!r}")
    return token


def _clock(value=None):
    if value is None:
        stamp = dt.datetime.now(dt.timezone.utc)
    elif isinstance(value, dt.datetime):
        stamp = value
    else:
        raise VolValueAuditError("injected clock must be a datetime")
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise VolValueAuditError("injected clock must include a timezone")
    return stamp.astimezone(dt.timezone.utc)


def _run_logs_root(root):
    return Path(root).resolve().parent / "Run Logs"


def _market_operation_path(root):
    return _run_logs_root(root) / ".market_data_operation.lock"


def _sidecar_lock_path(root):
    identity = hashlib.sha256(
        str(Path(root).resolve()).casefold().encode("utf-8")).hexdigest()[:16]
    return _run_logs_root(root) / f".vol_value_sidecars-{identity}.lock"


@contextmanager
def _cross_process_sidecar_lease(root):
    deadline = time.monotonic() + _SIDECAR_LOCK_WAIT_S
    lease = None
    while lease is None:
        try:
            lease = operation_gate.acquire(
                "vol_value_sidecar", owner="volatility value sidecars",
                path=_sidecar_lock_path(root))
        except operation_gate.OperationBusy as exc:
            if time.monotonic() >= deadline:
                raise VolValueAuditError(
                    "volatility sidecars are busy in another process") from exc
            time.sleep(_SIDECAR_LOCK_POLL_S)
        except operation_gate.OperationGateError as exc:
            raise VolValueAuditError(
                f"cannot lock volatility sidecars: {exc}") from exc
    try:
        yield
    finally:
        lease.release()


@contextmanager
def _sidecar_transaction(root):
    with _WRITE_LOCK:
        with _cross_process_sidecar_lease(root):
            yield


@contextmanager
def _audit_guard(root, write_queue, operation_mode="vol_value_audit"):
    """Freeze cooperating bank writers and serialize durable publications."""
    if not write_queue:
        # The diagnostic form is byte-pure: no lock/control artifact is made.
        with nullcontext():
            yield
        return
    if operation_mode not in {"vol_value_audit", "fetch"}:
        raise VolValueAuditError("invalid volatility audit operation mode")
    try:
        market_lease = operation_gate.acquire(
            operation_mode, owner="volatility value audit",
            path=_market_operation_path(root))
    except operation_gate.OperationBusy as exc:
        raise VolValueAuditError(
            "market-data operation is active; volatility audit refused") from exc
    except operation_gate.OperationGateError as exc:
        raise VolValueAuditError(
            f"cannot acquire volatility audit operation gate: {exc}") from exc
    try:
        yield
    finally:
        market_lease.release()


def _sidecar_path(root, basename):
    root = Path(root).resolve()
    if not root.is_dir():
        raise VolValueAuditError(f"storage root is not a directory: {root}")
    path = root / basename
    if path.exists() and path.is_symlink():
        raise VolValueAuditError(f"refusing symlink sidecar: {basename}")
    if path.resolve(strict=False).parent != root:
        raise VolValueAuditError(f"sidecar escapes storage root: {basename}")
    return path


def _read_stable_bytes(path, *, missing_ok=False):
    path = Path(path)
    for _attempt in range(_READ_ATTEMPTS):
        try:
            before = path.stat()
            if before.st_size > MAX_SIDECAR_BYTES:
                raise VolValueAuditError(
                    f"{path.name} exceeds the sidecar read limit")
            raw = path.read_bytes()
            after = path.stat()
        except FileNotFoundError:
            if missing_ok:
                return None
            raise VolValueAuditError(f"missing sidecar: {path.name}") from None
        except OSError as exc:
            raise VolValueAuditError(
                f"cannot read {path.name}: {type(exc).__name__}") from exc
        if (before.st_size == after.st_size == len(raw)
                and before.st_mtime_ns == after.st_mtime_ns):
            return raw
    raise VolValueAuditError(f"{path.name} changed while being read")


def _load_json(path, *, missing_ok=False):
    raw = _read_stable_bytes(path, missing_ok=missing_ok)
    if raw is None:
        return None, None

    def unique_object(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise VolValueAuditError(
                    f"duplicate JSON key in {Path(path).name}: {key}")
            out[key] = value
        return out

    try:
        payload = json.loads(
            raw.decode("utf-8"), object_pairs_hook=unique_object)
    except (UnicodeDecodeError, ValueError) as exc:
        raise VolValueAuditError(f"invalid JSON in {Path(path).name}") from exc
    return payload, {
        "basename": Path(path).name,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }


def _encode_json(path, payload):
    try:
        raw = json.dumps(
            payload, sort_keys=True, separators=(",", ":"),
            allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise VolValueAuditError(
            f"cannot encode {Path(path).name}") from exc
    if len(raw) > MAX_SIDECAR_BYTES:
        raise VolValueAuditError(
            f"{Path(path).name} exceeds the sidecar write limit")
    return raw


def _write_encoded_json(path, raw):
    try:
        storage._atomic_write_bytes(Path(path), raw)
    except Exception as exc:  # noqa: BLE001 - normalize storage failures
        raise VolValueAuditError(
            f"cannot write {Path(path).name}: {type(exc).__name__}") from exc
    return {
        "basename": Path(path).name,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }


def _write_json(path, payload):
    return _write_encoded_json(path, _encode_json(path, payload))


def _validate_registry(payload):
    if not isinstance(payload, dict):
        raise VolValueAuditError("settled registry must be an object")
    out = {}
    count = 0
    for raw_ticker, raw_kinds in payload.items():
        ticker = _ticker(raw_ticker)
        if raw_ticker != ticker or not isinstance(raw_kinds, dict):
            raise VolValueAuditError("settled registry ticker shape is invalid")
        kinds = {}
        for raw_kind, raw_days in raw_kinds.items():
            kind = _kind_token(raw_kind)
            if raw_kind != kind or not isinstance(raw_days, dict):
                raise VolValueAuditError("settled registry kind shape is invalid")
            days = {}
            for raw_day, raw_entry in raw_days.items():
                day = _day(raw_day)
                if raw_day != day or not isinstance(raw_entry, dict):
                    raise VolValueAuditError("settled registry day shape is invalid")
                base_fields = {"value", "reason", "confirmed", "run"}
                fields = set(raw_entry)
                if fields != base_fields and fields != base_fields | {"evidence"}:
                    raise VolValueAuditError(
                        "settled registry entry fields are invalid")
                value = _number(raw_entry.get("value"), "settled value")
                if value < 0:
                    raise VolValueAuditError("settled value cannot be negative")
                entry = {
                    "value": value,
                    "reason": _bounded_text(
                        raw_entry.get("reason"), "settled reason"),
                    "confirmed": _day(raw_entry.get("confirmed")),
                    "run": _bounded_text(raw_entry.get("run"), "settled run"),
                }
                if "evidence" in raw_entry:
                    entry["evidence"] = _reconcile_evidence(
                        raw_entry.get("evidence"))
                days[day] = entry
                count += 1
                if count > MAX_SETTLED_ENTRIES:
                    raise VolValueAuditError(
                        "settled registry has too many entries")
            kinds[kind] = days
        out[ticker] = kinds
    return out, count


def _load_registry(root):
    path = _sidecar_path(root, SETTLED_BASENAME)
    payload, source = _load_json(path, missing_ok=True)
    if payload is None:
        return {}, None
    registry, count = _validate_registry(payload)
    source["entries"] = count
    return registry, source


def _queue_row(value):
    if not isinstance(value, dict):
        raise VolValueAuditError("queue row must be an object")
    base_fields = {
        "ticker", "kind", "kind_token", "day", "value", "reason",
        "reasons",
    }
    changed_fields = {"settled_value_changed", "settled_value"}
    fields = frozenset(value)
    if fields not in {frozenset(base_fields),
                      frozenset(base_fields | changed_fields)}:
        raise VolValueAuditError("queue row fields are invalid")
    ticker = _ticker(value.get("ticker"))
    kind = _kind_token(value.get("kind_token") or value.get("kind"))
    if value.get("kind") != kind or value.get("kind_token") != kind:
        raise VolValueAuditError("queue kind fields disagree")
    day = _day(value.get("day"))
    reasons = value.get("reasons")
    if (not isinstance(reasons, list) or not reasons
            or len(reasons) > 16):
        raise VolValueAuditError("queue reasons are invalid")
    reasons = sorted({_bounded_text(item, "queue reason") for item in reasons})
    if value.get("reason") != ",".join(reasons):
        raise VolValueAuditError("queue reason fields disagree")
    current = _number(value.get("value"), "queue value", allow_none=True)
    row = {
        "ticker": ticker,
        "kind": kind,
        "kind_token": kind,
        "day": day,
        "value": current,
        "reason": ",".join(reasons),
        "reasons": reasons,
    }
    if set(value) == base_fields | changed_fields:
        if value.get("settled_value_changed") is not True:
            raise VolValueAuditError("queue settlement-change flag is invalid")
        row["settled_value_changed"] = True
        row["settled_value"] = _number(
            value.get("settled_value"), "prior settled value")
    return row


def _load_queue(root):
    path = _sidecar_path(root, QUEUE_BASENAME)
    payload, source = _load_json(path, missing_ok=True)
    if payload is None:
        return [], None
    expected = {
        "kind", "version", "generated_at", "partial", "scope_tickers",
        "rows",
    }
    if (not isinstance(payload, dict) or set(payload) != expected
            or payload.get("kind") != "vol_value_queue"
            or payload.get("version") != QUEUE_VERSION
            or not isinstance(payload.get("partial"), bool)
            or not isinstance(payload.get("rows"), list)
            or len(payload["rows"]) > MAX_QUEUE_ROWS):
        raise VolValueAuditError("volatility queue envelope is invalid")
    try:
        generated = dt.datetime.fromisoformat(payload["generated_at"])
    except (TypeError, ValueError) as exc:
        raise VolValueAuditError(
            "volatility queue timestamp is invalid") from exc
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise VolValueAuditError("volatility queue timestamp is invalid")
    scope = payload.get("scope_tickers")
    if payload["partial"]:
        if (not isinstance(scope, list)
                or len(scope) > MAX_QUEUE_ROWS):
            raise VolValueAuditError("volatility queue scope is invalid")
        normalized_scope = [_ticker(item) for item in scope]
        if normalized_scope != sorted(set(normalized_scope)):
            raise VolValueAuditError("volatility queue scope is invalid")
    elif scope is not None:
        raise VolValueAuditError("full volatility queue has a partial scope")
    rows = [_queue_row(row) for row in payload["rows"]]
    keys = [
        (row["ticker"], row["kind_token"], row["day"])
        for row in rows
    ]
    if len(keys) != len(set(keys)):
        raise VolValueAuditError("volatility queue has duplicate rows")
    source["rows"] = len(rows)
    return rows, source


def queue_snapshot(root, tickers=None, *, include_source=False):
    """Return a validated, deterministic snapshot of durable refetch debt."""
    requested = None
    if tickers is not None:
        if isinstance(tickers, (str, bytes)):
            raise VolValueAuditError("tickers must be an iterable, not text")
        try:
            requested = {_ticker(item) for item in tickers}
        except TypeError as exc:
            raise VolValueAuditError("tickers must be iterable") from exc
    rows, source = _load_queue(root)
    if requested is not None:
        rows = [row for row in rows if row["ticker"] in requested]
    rows = [dict(row, reasons=list(row["reasons"])) for row in rows]
    return (rows, source) if include_source else rows


def _queue_cas_index(rows, expected):
    key = (expected["ticker"], expected["kind_token"], expected["day"])
    matches = [index for index, row in enumerate(rows)
               if (row["ticker"], row["kind_token"], row["day"]) == key]
    if len(matches) != 1:
        return None
    index = matches[0]
    return index if rows[index] == expected else None


def finalize_reconcile(root, expected_row, *, action, run, value=None,
                       reason=None, confirmed=None, evidence=None):
    """CAS-finalize one refetch after its bank/registry evidence is durable.

    ``settled`` writes the source-confirmed registry entry first and then
    retires the exact queue row. ``corrected`` only retires the exact row; the
    caller must already have verified the month and manifest commit. A stale
    queue snapshot causes zero writes.
    """
    root = Path(root).resolve()
    expected = _queue_row(expected_row)
    action = str(action or "").strip().casefold()
    if action not in {"settled", "corrected"}:
        raise VolValueAuditError("invalid reconcile finalization action")
    run = _bounded_text(run, "reconcile run")
    stamp = _clock()
    confirmed_day = (_day(confirmed) if confirmed is not None
                     else stamp.date().isoformat())
    entry = None
    if action == "settled":
        settled_value = _number(value, "settled value")
        if settled_value < 0:
            raise VolValueAuditError("settled value cannot be negative")
        settled_reason = _bounded_text(reason, "settled reason")
        entry = {
            "value": settled_value,
            "reason": settled_reason,
            "confirmed": confirmed_day,
            "run": run,
        }
        if evidence is not None:
            entry["evidence"] = _reconcile_evidence(evidence)
        current_fingerprint = (
            entry.get("evidence", {}).get("stored_day_sha256"))
        if (expected.get("value") is None
                or not _within_settle(expected["value"], settled_value)
                or not _settlement_covers(
                    expected.get("reasons"), entry,
                    current_fingerprint=current_fingerprint)):
            raise VolValueAuditError(
                "settlement does not cover the exact queued finding")

    registry_source = None
    queue_source = None
    try:
        with _sidecar_transaction(root):
            queue_rows, _queue_source = _load_queue(root)
            queue_payload, _payload_source = _load_json(
                _sidecar_path(root, QUEUE_BASENAME), missing_ok=True)
            index = _queue_cas_index(queue_rows, expected)
            if index is None or queue_payload is None:
                return {"status": "stale", "action": action,
                        "queue_source": _queue_source}
            if entry is not None:
                registry, _registry_source = _load_registry(root)
                registry.setdefault(expected["ticker"], {}).setdefault(
                    expected["kind_token"], {})[expected["day"]] = entry
                _validate_registry(registry)
                registry_source = _write_json(
                    _sidecar_path(root, SETTLED_BASENAME), registry)
            kept = queue_rows[:index] + queue_rows[index + 1:]
            queue_payload = dict(queue_payload)
            queue_payload["generated_at"] = stamp.isoformat(timespec="seconds")
            queue_payload["rows"] = kept
            queue_source = _write_json(
                _sidecar_path(root, QUEUE_BASENAME), queue_payload)
    except Exception as exc:  # noqa: BLE001 - preserve completed stage truth
        registry_committed = registry_source is not None
        queue_resolved = queue_source is not None
        if registry_committed or queue_resolved:
            raise ReconcileFinalizeError(
                "reconcile finalization failed after a durable stage",
                action=action,
                registry_committed=registry_committed,
                queue_resolved=queue_resolved,
                registry_source=registry_source,
                queue_source=queue_source,
            ) from exc
        raise
    result = {
        "status": action,
        "action": action,
        "ticker": expected["ticker"],
        "kind": expected["kind_token"],
        "day": expected["day"],
        "queue_source": queue_source,
    }
    if entry is not None:
        result.update(entry)
        result["registry_source"] = registry_source
    return result


def _ticker_dirs(root, tickers):
    root = Path(root).resolve()
    requested = None
    if tickers is not None:
        if isinstance(tickers, (str, bytes)):
            raise VolValueAuditError("tickers must be an iterable, not text")
        requested = {_ticker(item) for item in tickers}
    try:
        entries = sorted(root.iterdir(), key=lambda path: path.name)
    except OSError as exc:
        raise VolValueAuditError(
            f"cannot enumerate storage root: {type(exc).__name__}") from exc
    dirs = []
    for path in entries:
        if (path.name.startswith("_")
                or not storage.TICKER_DIR_RE.fullmatch(path.name)):
            continue
        if path.is_symlink():
            raise VolValueAuditError(
                f"refusing symlink ticker directory: {path.name}")
        if path.is_dir() and (requested is None or path.name in requested):
            dirs.append(path)
    return dirs, requested


def _correction_ledger_mismatch(root, ticker_dir):
    """Return a bounded reason when correction evidence names other bytes."""
    manifest = storage.load_manifest(ticker_dir)
    if not isinstance(manifest, dict):
        return None
    for token, section in (manifest.get("intervals") or {}).items():
        if storage.kind_of(token) not in storage.RATIO_KINDS:
            continue
        for month, entry in ((section or {}).get("months") or {}).items():
            if not isinstance(entry, dict) or "value_corrections" not in entry:
                continue
            ledger = entry.get("value_corrections")
            if not isinstance(ledger, list) or not ledger:
                return f"{token} {month} has malformed correction evidence"
            try:
                year, number = (int(part) for part in month.split("-", 1))
                path = storage.find_month_file(
                    root, ticker_dir.name, year, number, token)
            except (TypeError, ValueError, OSError, storage.StorageError):
                path = None
            actual = (storage._sha256_of_file(path)
                      if path is not None else None)
            if actual is None or actual != entry.get("sha256"):
                return (f"{token} {month} correction evidence does not "
                        "match the active month bytes")
    return None


def _discover_month_files(ticker_dir, errors, failed_tickers):
    slots = {}
    try:
        years = sorted(ticker_dir.iterdir(), key=lambda path: path.name)
    except OSError as exc:
        failed_tickers.add(ticker_dir.name)
        _record_error(errors, {
            "ticker": ticker_dir.name,
            "error": f"cannot enumerate ticker: {type(exc).__name__}",
        })
        return slots
    for year_dir in years:
        if not storage.YEAR_DIR_RE.fullmatch(year_dir.name):
            continue
        if year_dir.is_symlink():
            failed_tickers.add(ticker_dir.name)
            _record_error(errors, {
                "ticker": ticker_dir.name,
                "error": f"refusing symlink year directory: {year_dir.name}",
            })
            continue
        if not year_dir.is_dir():
            continue
        try:
            month_dirs = sorted(year_dir.iterdir(), key=lambda path: path.name)
        except OSError as exc:
            failed_tickers.add(ticker_dir.name)
            _record_error(errors, {
                "ticker": ticker_dir.name,
                "error": f"cannot enumerate {year_dir.name}: {type(exc).__name__}",
            })
            continue
        for month_dir in month_dirs:
            if month_dir.name not in storage.MONTH_DIRS:
                continue
            if month_dir.is_symlink():
                failed_tickers.add(ticker_dir.name)
                _record_error(errors, {
                    "ticker": ticker_dir.name,
                    "error": (
                        f"refusing symlink month directory: "
                        f"{year_dir.name}/{month_dir.name}"),
                })
                continue
            if not month_dir.is_dir():
                continue
            month = storage.MONTH_DIRS.index(month_dir.name) + 1
            try:
                files = sorted(month_dir.iterdir(), key=lambda path: path.name)
            except OSError as exc:
                failed_tickers.add(ticker_dir.name)
                _record_error(errors, {
                    "ticker": ticker_dir.name,
                    "error": (f"cannot enumerate {year_dir.name}/{month_dir.name}: "
                              f"{type(exc).__name__}"),
                })
                continue
            for path in files:
                if path.is_symlink():
                    match = storage.FILENAME_RE.fullmatch(path.name)
                    if (match is not None and storage.kind_of(match.group(4))
                            in storage.RATIO_KINDS):
                        failed_tickers.add(ticker_dir.name)
                        _record_error(errors, {
                            "ticker": ticker_dir.name,
                            "path": str(path.relative_to(ticker_dir)),
                            "error": "refusing symlink ratio month file",
                        })
                    continue
                if not path.is_file():
                    continue
                match = storage.FILENAME_RE.fullmatch(path.name)
                if match is None:
                    continue
                interval = match.group(4)
                if storage.kind_of(interval) not in storage.RATIO_KINDS:
                    continue
                year = int(match.group(2))
                named_month = int(match.group(3))
                if (match.group(1) != ticker_dir.name
                        or year != int(year_dir.name) or named_month != month):
                    failed_tickers.add(ticker_dir.name)
                    _record_error(errors, {
                        "ticker": ticker_dir.name,
                        "path": str(path.relative_to(ticker_dir)),
                        "error": "ratio month file is in the wrong canonical slot",
                    })
                    continue
                key = (interval, year, month)
                old = slots.get(key)
                if old is None or (old.suffix != ".parquet"
                                   and path.suffix == ".parquet"):
                    slots[key] = path
    return slots


def _is_consecutive_session(first, second, special_closures):
    cursor = first + dt.timedelta(days=1)
    while cursor < second and not market_calendar.is_trading_day(
            cursor, special_closures=special_closures):
        cursor += dt.timedelta(days=1)
    return cursor == second and market_calendar.is_trading_day(
        second, special_closures=special_closures)


def _at_or_above(value, threshold):
    return value > threshold or math.isclose(
        value, threshold, rel_tol=1e-12, abs_tol=1e-15)


def _at_or_below(value, threshold):
    return value < threshold or math.isclose(
        value, threshold, rel_tol=1e-12, abs_tol=1e-15)


def _within_settle(value, settled):
    delta = abs(float(value) - float(settled))
    return delta < SETTLE_TOL or math.isclose(
        delta, SETTLE_TOL, rel_tol=1e-12, abs_tol=1e-15)


def _series_from_files(slots, ticker_dir):
    try:
        manifest = storage.load_manifest(ticker_dir) or {}
    except Exception:  # noqa: BLE001 - tree remains authoritative
        manifest = {}
    manifest_intervals = manifest.get("intervals", {})
    if not isinstance(manifest_intervals, dict):
        manifest_intervals = {}
    series = {}
    for (interval, year, month), path in sorted(slots.items()):
        month_key = storage.month_key(year, month)
        interval_entry = manifest_intervals.get(interval) or {}
        month_entry = (interval_entry.get("months", {}).get(month_key)
                       if isinstance(interval_entry, dict) else None)
        manifest_sha = (month_entry.get("sha256")
                        if isinstance(month_entry, dict) else None)
        months = series.setdefault(interval, [])
        months.append({
            "month": month_key,
            "path": path,
            # An exact manifest hash is only an optional parse accelerator.
            # Discovery and month selection remain tree-authoritative.
            "manifest_sha": manifest_sha,
        })
    return series


def _add_reason(issues, ticker, interval, day, reason):
    issues.setdefault((ticker, interval, day), set()).add(reason)


def _analyze_series(ticker, interval, months, issues, errors, failed_tickers,
                    special_closures, finding_fingerprints=None):
    if finding_fingerprints is None:
        finding_fingerprints = {}
    daily = {}
    history = []
    months_scanned = 0
    rows_scanned = 0
    complete = True
    for month in sorted(months, key=lambda item: item["month"]):
        try:
            ts, opens, highs, lows, closes, volumes = (
                storage.read_month_file_cols(
                    month["path"], month.get("manifest_sha"),
                    validate=False))
            ts = np.asarray(ts, dtype=np.int64)
            opens = np.asarray(opens, dtype=np.float64)
            highs = np.asarray(highs, dtype=np.float64)
            lows = np.asarray(lows, dtype=np.float64)
            closes = np.asarray(closes, dtype=np.float64)
            volumes = np.asarray(volumes)
            lengths = {
                len(ts), len(opens), len(highs), len(lows), len(closes),
                len(volumes),
            }
            if len(lengths) != 1:
                raise ValueError("stored columns have inconsistent lengths")
        except Exception as exc:  # noqa: BLE001 - corruption becomes evidence
            failed_tickers.add(ticker)
            _record_error(errors, {
                "ticker": ticker,
                "kind": interval,
                "month": month["month"],
                "error": f"cannot read month: {type(exc).__name__}: {exc}"[:MAX_TEXT],
            })
            complete = False
            continue
        months_scanned += 1
        row_count = len(ts)
        rows_scanned += row_count
        if row_count == 0:
            failed_tickers.add(ticker)
            _record_error(errors, {
                "ticker": ticker,
                "kind": interval,
                "month": month["month"],
                "error": "stored ratio month is empty",
            })
            complete = False
            continue
        year, month_number = map(int, month["month"].split("-"))
        start = dt.datetime(year, month_number, 1)
        end = (dt.datetime(year + 1, 1, 1) if month_number == 12
               else dt.datetime(year, month_number + 1, 1))
        epoch = dt.datetime(1970, 1, 1)
        start_s = int((start - epoch).total_seconds())
        end_s = int((end - epoch).total_seconds())
        in_slot = (ts >= start_s) & (ts < end_s)
        increasing = np.r_[True, ts[1:] > ts[:-1]] if row_count else np.array(
            [], dtype=bool)
        structurally_valid = in_slot & increasing
        if row_count and not bool(np.all(structurally_valid)):
            failed_tickers.add(ticker)
            _record_error(errors, {
                "ticker": ticker,
                "kind": interval,
                "month": month["month"],
                "error": "stored timestamps leave their month or are not increasing",
            })
            complete = False

        def add_mask(mask, reason):
            selected = structurally_valid & np.asarray(mask, dtype=bool)
            if not bool(np.any(selected)):
                return
            for day_number in np.unique(ts[selected] // 86_400).tolist():
                day = dt.date(1970, 1, 1) + dt.timedelta(
                    days=int(day_number))
                _add_reason(issues, ticker, interval, day, reason)

        finite_o = np.isfinite(opens)
        finite_h = np.isfinite(highs)
        finite_l = np.isfinite(lows)
        finite_c = np.isfinite(closes)
        all_finite = finite_o & finite_h & finite_l & finite_c
        add_mask(~all_finite, "hard_nonfinite")
        add_mask(
            (finite_o & (opens < 0)) | (finite_h & (highs < 0))
            | (finite_l & (lows < 0)) | (finite_c & (closes < 0)),
            "hard_negative")
        add_mask(
            (finite_o & (opens > VOL_HARD_CEILING))
            | (finite_h & (highs > VOL_HARD_CEILING))
            | (finite_l & (lows > VOL_HARD_CEILING))
            | (finite_c & (closes > VOL_HARD_CEILING)),
            "hard_ceiling")
        add_mask(
            all_finite
            & ((lows > np.minimum(opens, closes))
               | (highs < np.maximum(opens, closes)) | (lows > highs)),
            "hard_ohlc")
        try:
            add_mask(volumes < 0, "hard_volume")
        except (TypeError, ValueError):
            failed_tickers.add(ticker)
            _record_error(errors, {
                "ticker": ticker,
                "kind": interval,
                "month": month["month"],
                "error": "stored volume column is non-numeric",
            })
            complete = False

        valid_indexes = np.flatnonzero(structurally_valid)
        if (len(valid_indexes) > 1
                and not bool(np.all(ts[valid_indexes][1:]
                                    > ts[valid_indexes][:-1]))):
            valid_indexes = valid_indexes[
                np.argsort(ts[valid_indexes], kind="stable")]
        if len(valid_indexes):
            day_numbers = ts[valid_indexes] // 86_400
            last_mask = np.r_[day_numbers[1:] != day_numbers[:-1], True]
            last_indexes = valid_indexes[last_mask]
        else:
            last_indexes = np.array([], dtype=np.int64)
        month_daily = {}
        for index in last_indexes.tolist():
            day = dt.date(1970, 1, 1) + dt.timedelta(
                days=int(ts[index] // 86_400))
            close = float(closes[index])
            daily[day] = close
            month_daily[day] = close
        days = sorted(month_daily)
        values = [month_daily[day] for day in days
                  if math.isfinite(month_daily[day])]
        current = {
            "month": month["month"],
            "values": values,
            "days": days,
        }
        current_month_index = year * 12 + month_number - 1
        history = [
            item for item in history
            if 1 <= current_month_index - item["month_index"] <= 12
        ]
        prior_days = {day for item in history for day in item["days"]}
        prior_values = [value for item in history for value in item["values"]]
        if (len(prior_days) >= 20 and prior_values and current["values"]):
            prior_median = float(median(prior_values))
            current_median = float(median(current["values"]))
            if (prior_median > 0 and _at_or_above(
                    current_median, UNIT_FLIP_RATIO * prior_median)):
                for flipped_day in current["days"]:
                    _add_reason(
                        issues, ticker, interval, flipped_day,
                        "unit_flip_month")
        # Strong M3 settlements are bound to the whole stored day. Compute
        # fingerprints only for hard/month-scale findings; ordinary close
        # detectors retain the cheaper scalar binding used by M2.
        protected = {
            issue_day for (issue_ticker, issue_interval, issue_day), reasons
            in issues.items()
            if issue_ticker == ticker and issue_interval == interval
            and issue_day.year == year and issue_day.month == month_number
            and any(reason.startswith("hard_")
                    or reason == "unit_flip_month" for reason in reasons)
        }
        for protected_day in protected:
            day_number = (protected_day - dt.date(1970, 1, 1)).days
            indexes = np.flatnonzero((ts // 86_400) == day_number)
            if (not len(indexes)
                    or not bool(np.all(structurally_valid[indexes]))):
                continue
            try:
                evidence = _day_fingerprint_parts((
                    int(ts[index]), float(opens[index]), float(highs[index]),
                    float(lows[index]), float(closes[index]),
                    float(volumes[index]))
                    for index in indexes.tolist())
            except (TypeError, ValueError, OverflowError,
                    VolValueAuditError):
                continue
            finding_fingerprints[(
                ticker, interval, protected_day)] = evidence["sha256"]
        current["month_index"] = current_month_index
        history.append(current)

    ordered_days = sorted(daily)
    for before, current in zip(ordered_days, ordered_days[1:]):
        if not _is_consecutive_session(before, current, special_closures):
            continue
        old, new = daily[before], daily[current]
        if not (math.isfinite(old) and math.isfinite(new) and old > 0 and new > 0):
            continue
        ratio = new / old
        if _at_or_above(ratio, JUMP_RATIO):
            _add_reason(issues, ticker, interval, current, "jump_up")
        elif _at_or_below(ratio, 1.0 / JUMP_RATIO):
            _add_reason(issues, ticker, interval, current, "jump_down")

    if storage.kind_of(interval) == "iv":
        run = []
        for day in ordered_days:
            close = daily[day]
            if close == 0 and (not run or _is_consecutive_session(
                    run[-1], day, special_closures)):
                run.append(day)
                continue
            if len(run) >= IV_ZERO_RUN:
                for zero_day in run:
                    _add_reason(
                        issues, ticker, interval, zero_day, "iv_zero_run")
            run = [day] if close == 0 else []
        if len(run) >= IV_ZERO_RUN:
            for zero_day in run:
                _add_reason(issues, ticker, interval, zero_day, "iv_zero_run")

    return daily, months_scanned, rows_scanned, complete


def _coherence_findings(by_ticker, issues):
    for ticker, kinds in by_ticker.items():
        iv_items = [item for item in kinds if item[0] == "iv"]
        hv_items = [item for item in kinds if item[0] == "hvol"]
        for _iv_kind, iv_token, iv_days in iv_items:
            for _hv_kind, hv_token, hv_days in hv_items:
                for day in sorted(set(iv_days) & set(hv_days)):
                    iv_value, hv_value = iv_days[day], hv_days[day]
                    if not (math.isfinite(iv_value) and math.isfinite(hv_value)):
                        violation = True
                    elif hv_value <= 0:
                        violation = True
                    else:
                        ratio = iv_value / hv_value
                        violation = (
                            not math.isfinite(ratio)
                            or (ratio < IV_HVOL_BAND[0]
                                and not math.isclose(
                                    ratio, IV_HVOL_BAND[0],
                                    rel_tol=1e-12, abs_tol=1e-15))
                            or (ratio > IV_HVOL_BAND[1]
                                and not math.isclose(
                                    ratio, IV_HVOL_BAND[1],
                                    rel_tol=1e-12, abs_tol=1e-15)))
                    if violation:
                        _add_reason(
                            issues, ticker, iv_token, day, "iv_hvol_band")
                        _add_reason(
                            issues, ticker, hv_token, day, "iv_hvol_band")


def _registry_entry(registry, ticker, interval, day):
    return (((registry.get(ticker) or {}).get(interval) or {})
            .get(day.isoformat()))


def _settlement_covers(reasons, settlement, *, current_fingerprint=None):
    """Whether scalar or full-day source evidence covers this exact set."""
    current = set(reasons or [])
    if isinstance(settlement, str):
        settlement = {"reason": settlement}
    if not current or not isinstance(settlement, dict):
        return False
    declared = {
        token.strip().casefold()
        for token in str(settlement.get("reason") or "")
        .replace(";", ",").split(",")
        if token.strip()
    }
    if "jump" in declared:
        declared.remove("jump")
        # The frozen M0 contract used the direction-neutral legacy token.  It
        # stands for the one directional jump present in the current finding,
        # not for arbitrary additional detector reasons.
        declared.update(current & {"jump_up", "jump_down"})
    if current != declared:
        return False
    if current.issubset(_CLOSE_SETTLE_REASONS):
        return True
    # A single-day source replay can establish that a month-scale unit flip is
    # genuinely served for this exact day. Hard-invalid bars cannot reach this
    # path because the ingest converter rejects them; they require correction.
    if not current.issubset(
            _CLOSE_SETTLE_REASONS | _FULL_DAY_SETTLE_REASONS):
        return False
    evidence = settlement.get("evidence")
    if not isinstance(evidence, dict):
        return False
    try:
        evidence = _reconcile_evidence(evidence)
    except VolValueAuditError:
        return False
    return (
        evidence["reasons"] == sorted(current)
        and evidence["stored_day_sha256"]
        == evidence["served_day_sha256"]
        and current_fingerprint is not None
        and evidence["stored_day_sha256"] == current_fingerprint
    )


def _classify_findings(issues, finding_values, finding_fingerprints, registry):
    flagged, suppressed = [], []
    for (ticker, interval, day), reasons in sorted(
            issues.items(), key=lambda item: (
                item[0][0], item[0][1], item[0][2])):
        value = finding_values.get((ticker, interval, day))
        row = {
            "ticker": ticker,
            "kind": interval,
            "kind_token": interval,
            "day": day.isoformat(),
            "value": value,
            "reason": ",".join(sorted(reasons)),
            "reasons": sorted(reasons),
        }
        settled = _registry_entry(registry, ticker, interval, day)
        if (settled is not None and value is not None
                and _within_settle(value, settled["value"])
                and _settlement_covers(
                    reasons, settled,
                    current_fingerprint=finding_fingerprints.get(
                        (ticker, interval, day)))):
            suppressed.append({**row, "settled": dict(settled)})
            continue
        if (settled is not None
                and (value is None
                     or not _within_settle(value, settled["value"]))):
            row.update({
                "settled_value_changed": True,
                "settled_value": float(settled["value"]),
            })
        flagged.append(row)
    return flagged, suppressed


def _finish_audit(root, stamp, requested, ticker_dirs, failed_tickers,
                  issues, finding_values, finding_fingerprints,
                  registry, registry_source, errors,
                  series_scanned, months_scanned, rows_scanned, write_queue):
    flagged, suppressed = _classify_findings(
        issues, finding_values, finding_fingerprints, registry)
    queue_rows = list(flagged)
    queue_source = None
    queue_path = None
    queue_raw = None
    if write_queue:
        old_rows = []
        successful_tickers = {
            path.name for path in ticker_dirs
        } - failed_tickers
        preserve_old = requested is not None or bool(failed_tickers)
        if preserve_old:
            old_rows, _old_source = _load_queue(root)
        # A complete ticker scan replaces its old slice.  Unscanned/failed
        # slices retain prior unresolved rows, while newly proven findings
        # from readable months are merged by exact key.
        merged = {
            (row["ticker"], row["kind_token"], row["day"]): row
            for row in old_rows
            if row["ticker"] not in successful_tickers
        }
        for row in flagged:
            key = (row["ticker"], row["kind_token"], row["day"])
            old = merged.get(key)
            if old is not None and row["ticker"] in failed_tickers:
                reasons = sorted(set(old["reasons"]) | set(row["reasons"]))
                merged[key] = {
                    **old,
                    "reason": ",".join(reasons),
                    "reasons": reasons,
                }
            else:
                merged[key] = row
        for row in suppressed:
            if row["ticker"] not in failed_tickers:
                merged.pop(
                    (row["ticker"], row["kind_token"], row["day"]), None)
        queue_rows = sorted(
            merged.values(),
            key=lambda row: (
                row["ticker"], row["kind_token"], row["day"]),
        )
        if len(queue_rows) > MAX_QUEUE_ROWS:
            raise VolValueAuditError("volatility queue has too many rows")
        queue_payload = {
            "kind": "vol_value_queue",
            "version": QUEUE_VERSION,
            "generated_at": stamp.isoformat(timespec="seconds"),
            "partial": requested is not None,
            "scope_tickers": sorted(requested) if requested is not None else None,
            "rows": queue_rows,
        }
        queue_path = _sidecar_path(root, QUEUE_BASENAME)
        queue_raw = _encode_json(queue_path, queue_payload)
        queue_source = {
            "basename": queue_path.name,
            "sha256": hashlib.sha256(queue_raw).hexdigest(),
            "bytes": len(queue_raw),
        }

    report = {
        "kind": "vol_value_audit",
        "version": AUDIT_VERSION,
        "report_only": True,
        "network": False,
        "market_data_written": False,
        "generated_at": stamp.isoformat(timespec="seconds"),
        "partial": requested is not None,
        "scope_tickers": sorted(requested) if requested is not None else None,
        "thresholds": {
            "vol_hard_ceiling": VOL_HARD_CEILING,
            "unit_flip_ratio": UNIT_FLIP_RATIO,
            "jump_ratio": JUMP_RATIO,
            "iv_hvol_band": list(IV_HVOL_BAND),
            "iv_zero_run": IV_ZERO_RUN,
            "settle_tolerance": SETTLE_TOL,
        },
        "tickers_scanned": len(ticker_dirs),
        "series_scanned": series_scanned,
        "months_scanned": months_scanned,
        "rows_scanned": rows_scanned,
        "flagged": flagged,
        "suppressed": suppressed,
        "queue_rows": len(queue_rows),
        "queue_source": queue_source,
        "registry_source": registry_source,
        "errors": errors,
        "complete": not errors,
    }
    if write_queue:
        report_path = _sidecar_path(root, AUDIT_BASENAME)
        report_raw = _encode_json(report_path, report)
        # Both envelopes are valid and within their bounds before the first
        # replace.  Queue-first/report-last makes the report the commit record.
        _write_encoded_json(queue_path, queue_raw)
        report_source = _write_encoded_json(report_path, report_raw)
        report["report_source"] = report_source
    return report


def _audit_under_guard(root, tickers=None, write_queue=True, *, now=None):
    """Audit stored IV/HVOL series and optionally replace their queue slice.

    A filtered audit preserves queue rows for unrequested tickers. Returned
    ``flagged`` rows describe only the current scan; ``queue_rows`` is the
    durable merged-row count, while the queue sidecar carries the rows.
    """
    root = Path(root).resolve()
    stamp = _clock(now)
    ticker_dirs, requested = _ticker_dirs(root, tickers)
    errors = []
    failed_tickers = set()
    try:
        special_closures = market_calendar.read_special_closures(
            root=root, strict=True)
    except (OSError, ValueError) as exc:
        special_closures = frozenset()
        failed_tickers.update(path.name for path in ticker_dirs)
        if requested is not None:
            failed_tickers.update(requested)
        _record_error(errors, {
            "source": market_calendar.SPECIAL_CLOSURES_SIDECAR,
            "error": ("cannot read special closures: "
                      f"{type(exc).__name__}: {exc}")[:MAX_TEXT],
        })
    if requested is not None:
        found = {path.name for path in ticker_dirs}
        for missing in sorted(requested - found):
            failed_tickers.add(missing)
            _record_error(errors, {
                "ticker": missing,
                "error": "requested ticker directory is unavailable",
            })
    registry, registry_source = {}, None
    if not write_queue:
        try:
            registry, registry_source = _load_registry(root)
        except VolValueAuditError as exc:
            _record_error(errors, {
                "source": SETTLED_BASENAME,
                "error": str(exc)[:MAX_TEXT],
            })

    issues = {}
    finding_values = {}
    finding_fingerprints = {}
    series_scanned = 0
    months_scanned = 0
    rows_scanned = 0
    for ticker_dir in ticker_dirs:
        stage = ticker_dir / storage.VOL_VALUE_RECONCILE_STAGE_DIR
        stage_active = stage.exists() or stage.is_symlink()
        ledger_mismatch = _correction_ledger_mismatch(root, ticker_dir)
        if stage_active or ledger_mismatch:
            failed_tickers.add(ticker_dir.name)
            _record_error(errors, {
                "ticker": ticker_dir.name,
                "error": (
                    "volatility correction transaction requires recovery"
                    if stage_active else ledger_mismatch)[:MAX_TEXT],
            })
            continue
        ticker_issues = {}
        ticker_daily = {}
        ticker_kinds = []
        slots = _discover_month_files(ticker_dir, errors, failed_tickers)
        series = _series_from_files(slots, ticker_dir)
        for interval, months in sorted(series.items()):
            daily, month_count, row_count, _complete = _analyze_series(
                ticker_dir.name, interval, months, ticker_issues, errors,
                failed_tickers, special_closures,
                finding_fingerprints=finding_fingerprints)
            series_scanned += 1
            months_scanned += month_count
            rows_scanned += row_count
            ticker_daily[interval] = daily
            ticker_kinds.append((storage.kind_of(interval), interval, daily))
        _coherence_findings({ticker_dir.name: ticker_kinds}, ticker_issues)
        for key, reasons in ticker_issues.items():
            issues[key] = reasons
            _ticker, interval, day = key
            value = ticker_daily.get(interval, {}).get(day)
            if value is not None and math.isfinite(float(value)):
                finding_values[key] = float(value)

    if write_queue:
        with _sidecar_transaction(root):
            try:
                registry, registry_source = _load_registry(root)
            except VolValueAuditError as exc:
                registry, registry_source = {}, None
                _record_error(errors, {
                    "source": SETTLED_BASENAME,
                    "error": str(exc)[:MAX_TEXT],
                })
            return _finish_audit(
                root, stamp, requested, ticker_dirs, failed_tickers,
                issues, finding_values, finding_fingerprints, registry,
                registry_source, errors, series_scanned, months_scanned,
                rows_scanned, True)
    return _finish_audit(
        root, stamp, requested, ticker_dirs, failed_tickers,
        issues, finding_values, finding_fingerprints, registry,
        registry_source, errors, series_scanned, months_scanned,
        rows_scanned, False)


def audit(root, tickers=None, write_queue=True, *, now=None,
          operation_mode="vol_value_audit"):
    """Audit ratio series under the shared bank-operation safety fence."""
    root = Path(root).resolve()
    with _audit_guard(root, bool(write_queue), operation_mode):
        return _audit_under_guard(
            root, tickers=tickers, write_queue=write_queue, now=now)


def record_settled(root, ticker, kind_token, day, value, *, reason, run,
                   confirmed=None):
    """Atomically add/replace one value-bound source confirmation."""
    root = Path(root).resolve()
    ticker = _ticker(ticker)
    kind_token = _kind_token(kind_token)
    day = _day(day)
    value = _number(value, "settled value")
    if value < 0:
        raise VolValueAuditError("settled value cannot be negative")
    reason = _bounded_text(reason, "settled reason")
    run = _bounded_text(run, "settled run")
    confirmed_day = (_day(confirmed) if confirmed is not None
                     else _clock().date().isoformat())
    with _sidecar_transaction(root):
        registry, _source = _load_registry(root)
        queue_rows, _queue_source = _load_queue(root)
        queue_payload, _queue_payload_source = _load_json(
            _sidecar_path(root, QUEUE_BASENAME), missing_ok=True)
        registry.setdefault(ticker, {}).setdefault(kind_token, {})[day] = {
            "value": value,
            "reason": reason,
            "confirmed": confirmed_day,
            "run": run,
        }
        _validate_registry(registry)
        source = _write_json(
            _sidecar_path(root, SETTLED_BASENAME), registry)
        kept_rows = [
            row for row in queue_rows
            if not (
                row["ticker"] == ticker
                and row["kind_token"] == kind_token
                and row["day"] == day
                and row.get("value") is not None
                and _within_settle(row["value"], value)
                and _settlement_covers(
                    row.get("reasons"), {"reason": reason})
            )
        ]
        queue_source = None
        if queue_payload is not None and len(kept_rows) != len(queue_rows):
            queue_payload = dict(queue_payload)
            queue_payload["generated_at"] = _clock().isoformat(
                timespec="seconds")
            queue_payload["rows"] = kept_rows
            queue_source = _write_json(
                _sidecar_path(root, QUEUE_BASENAME), queue_payload)
    return {
        "ticker": ticker,
        "kind": kind_token,
        "kind_token": kind_token,
        "day": day,
        **registry[ticker][kind_token][day],
        "source": source,
        "queue_source": queue_source,
    }


def settled_for_export(root, selections, *, include_source=False):
    """Return historical settlements for the exact exported ratio series.

    History intentionally remains visible after a stored value changes; the
    audit then re-queues that day, but the prior source confirmation is still
    part of the export record required by the user and the frozen F6 contract.
    """
    if isinstance(selections, (str, bytes)):
        raise VolValueAuditError("export selections must be pairs")
    wanted = set()
    try:
        raw_items = list(selections or [])
    except TypeError as exc:
        raise VolValueAuditError("export selections must be iterable") from exc
    for item in raw_items:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise VolValueAuditError("export selection must be a ticker/kind pair")
        wanted.add((_ticker(item[0]), _kind_token(item[1])))
    registry, source = _load_registry(root)
    rows = []
    for ticker, kind in sorted(wanted):
        for day, entry in sorted(
                ((registry.get(ticker) or {}).get(kind) or {}).items()):
            rows.append({
                "ticker": ticker,
                "kind": kind,
                "kind_token": kind,
                "day": day,
                "value": float(entry["value"]),
                "reason": entry["reason"],
                "confirmed": entry["confirmed"],
                "run": entry["run"],
            })
    return (rows, source) if include_source else rows


__all__ = [
    "AUDIT_BASENAME",
    "IV_HVOL_BAND",
    "IV_ZERO_RUN",
    "JUMP_RATIO",
    "QUEUE_BASENAME",
    "SETTLED_BASENAME",
    "SETTLE_TOL",
    "UNIT_FLIP_RATIO",
    "VOL_HARD_CEILING",
    "VOL_VALUE_AUDIT",
    "ReconcileFinalizeError",
    "VolValueAuditError",
    "audit",
    "day_fingerprint",
    "finalize_reconcile",
    "queue_snapshot",
    "record_settled",
    "settled_for_export",
]
