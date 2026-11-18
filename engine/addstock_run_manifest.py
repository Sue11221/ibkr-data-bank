"""Durable Add Stocks run intent and verification-debt state.

This module is GUI-free and provider-free. It owns only the additive
``_addstock_run.json`` sidecar; bar commits and stock manifests remain owned by
the existing storage engine.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
import tempfile
import threading
import time

import stock_storage as ss


SCHEMA = 1
ACTIVE_NAME = "_addstock_run.json"
MAX_BYTES = 4 * 1024 * 1024
MAX_TICKERS = 10_000
MAX_SERIES_PER_TICKER = 64
MAX_PORTS = 64
MAX_NOTE = 400
MAX_SEAL_PENDING = 20_000

RUN_STATES = {"active", "interrupted", "complete"}
TICKER_STATES = {"pending", "building", "built", "verified"}
SERIES_STATES = {"pending", "building", "complete"}
DEBT_STAGES = ("xval", "gaps", "earliest")
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
_LOCK = threading.RLock()


class ManifestError(RuntimeError):
    """The active sidecar is absent, malformed, unsafe, or inconsistent."""


class ActiveRunExists(ManifestError):
    """A new run attempted to overwrite durable unfinished intent."""


class RunMismatch(ManifestError):
    """A stale callback attempted to mutate a different run."""


def _valid_sha256(value):
    return (isinstance(value, str) and len(value) == 64
            and all(char in "0123456789abcdef" for char in value))


def active_path(root):
    return Path(root) / ACTIVE_NAME


def default_archive_dir(root):
    return Path(root).resolve().parent / "Run Logs"


def _utc_text(now=None):
    value = now() if callable(now) else now
    if value is None:
        value = datetime.now(timezone.utc)
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if not isinstance(value, datetime):
        raise ManifestError("clock must return a datetime")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def new_run_id(now_ns=None):
    stamp = int(time.time_ns() if now_ns is None else now_ns)
    return f"addstock-{stamp}"


def _text(value, field, limit, *, allow_empty=False):
    if not isinstance(value, str):
        raise ManifestError(f"{field} must be a string")
    value = value.strip()
    if (not value and not allow_empty) or len(value) > limit:
        raise ManifestError(f"{field} has invalid length")
    return value


def _timestamp(value, field, *, allow_none=False):
    if value is None and allow_none:
        return None
    value = _text(value, field, 64)
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ManifestError(f"{field} is not an ISO timestamp") from exc
    return value


def _canon_selection(ticker, interval):
    try:
        ticker = ss.canonical_ticker(str(ticker))
    except Exception as exc:  # noqa: BLE001 - normalized as manifest error
        raise ManifestError(f"invalid ticker: {ticker!r}") from exc
    interval = str(interval or "").strip()
    if not ss.INTERVAL_RE.fullmatch(interval):
        raise ManifestError(f"invalid interval: {interval!r}")
    return ticker, interval


def _is_rth(interval):
    try:
        return ss.session_of(interval) == "rth"
    except Exception:  # noqa: BLE001 - validation catches malformed intervals
        return False


def _series_record(interval):
    needs_checks = _is_rth(interval)
    return {
        "state": "pending",
        "xval": not needs_checks,
        "gaps": not needs_checks,
    }


def _normalize_params(params, selections):
    params = dict(params or {})
    allowed = {
        "mode", "since", "intervals", "kinds", "extended", "store_daily",
        "ports_at_start",
    }
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise ManifestError(f"unsupported params: {', '.join(unknown)}")
    mode = _text(str(params.get("mode") or "add"), "params.mode", 24)
    since = params.get("since")
    if isinstance(since, (date, datetime)):
        since = since.date().isoformat() if isinstance(since, datetime) \
            else since.isoformat()
    if since is not None:
        since = _text(str(since), "params.since", 32)
        try:
            date.fromisoformat(since)
        except ValueError as exc:
            raise ManifestError("params.since must be an ISO date") from exc
    intervals = sorted({iv for _ticker, iv in selections})
    supplied_intervals = params.get("intervals")
    if supplied_intervals is not None:
        supplied_intervals = [str(v).strip() for v in supplied_intervals]
        if supplied_intervals != intervals:
            raise ManifestError("params.intervals does not match selections")
    kinds = sorted({ss.kind_of(iv) or "trades" for iv in intervals})
    supplied_kinds = params.get("kinds")
    if supplied_kinds is not None and [str(v) for v in supplied_kinds] != kinds:
        raise ManifestError("params.kinds does not match selections")
    ports = []
    for value in params.get("ports_at_start") or []:
        try:
            port = int(value)
        except (TypeError, ValueError) as exc:
            raise ManifestError("params.ports_at_start contains a non-port") from exc
        if not 1 <= port <= 65535:
            raise ManifestError("params.ports_at_start contains an invalid port")
        if port not in ports:
            ports.append(port)
    if len(ports) > MAX_PORTS:
        raise ManifestError("too many starting ports")
    return {
        "mode": mode,
        "since": since,
        "intervals": intervals,
        "kinds": kinds,
        "extended": bool(params.get("extended", False)),
        "store_daily": bool(params.get("store_daily", False)),
        "ports_at_start": ports,
    }


def _validate_series_map(value, field):
    if not isinstance(value, dict) or len(value) > MAX_SERIES_PER_TICKER:
        raise ManifestError(f"{field} must be a bounded object")
    out = {}
    for interval, raw in value.items():
        _ticker, interval = _canon_selection("VALID", interval)
        if not isinstance(raw, dict) or set(raw) != {"state", "xval", "gaps"}:
            raise ManifestError(f"{field}.{interval} has invalid fields")
        state = _text(raw.get("state"), f"{field}.{interval}.state", 16)
        if state not in SERIES_STATES:
            raise ManifestError(f"{field}.{interval}.state is unsupported")
        if not isinstance(raw.get("xval"), bool) or not isinstance(
                raw.get("gaps"), bool):
            raise ManifestError(f"{field}.{interval} stage flags must be booleans")
        if not _is_rth(interval) and not (raw["xval"] and raw["gaps"]):
            raise ManifestError(f"{field}.{interval} cannot owe RTH checks")
        out[interval] = dict(raw)
    if not out:
        raise ManifestError(f"{field} cannot be empty")
    return out


def _recompute_ticker(record):
    series = record["series"]
    complete = all(v["state"] == "complete" for v in series.values())
    if not complete:
        record["state"] = (
            "building" if any(v["state"] != "pending" for v in series.values())
            else "pending"
        )
        record["missing"] = []
        return
    missing = []
    rth = [v for iv, v in series.items() if _is_rth(iv)]
    if any(not v["xval"] for v in rth):
        missing.append("xval")
    if any(not v["gaps"] for v in rth):
        missing.append("gaps")
    if not record["earliest"]:
        missing.append("earliest")
    record["missing"] = missing
    record["state"] = "built" if missing else "verified"


def validate_manifest(data):
    if not isinstance(data, dict):
        raise ManifestError("manifest root must be an object")
    expected = {
        "schema", "run_id", "created_at", "updated_at", "state",
        "fetch_finished", "params", "tickers", "finalize",
    }
    if set(data) != expected:
        raise ManifestError("manifest root fields do not match schema 1")
    if data.get("schema") != SCHEMA:
        raise ManifestError("unsupported manifest schema")
    run_id = _text(data.get("run_id"), "run_id", 80)
    if not _RUN_ID_RE.fullmatch(run_id):
        raise ManifestError("run_id has unsafe characters")
    _timestamp(data.get("created_at"), "created_at")
    _timestamp(data.get("updated_at"), "updated_at")
    state = _text(data.get("state"), "state", 16)
    if state not in RUN_STATES:
        raise ManifestError("unsupported run state")
    if not isinstance(data.get("fetch_finished"), bool):
        raise ManifestError("fetch_finished must be a boolean")

    if not isinstance(data.get("tickers"), dict) or not data["tickers"]:
        raise ManifestError("tickers must be a non-empty object")
    if len(data["tickers"]) > MAX_TICKERS:
        raise ManifestError("too many tickers")
    normalized_selections = []
    for ticker, record in data["tickers"].items():
        canon, _interval = _canon_selection(ticker, "1m")
        if canon != ticker or not isinstance(record, dict):
            raise ManifestError(f"invalid ticker record: {ticker!r}")
        fields = {
            "state", "updated_at", "missing", "seal_pending", "note",
            "earliest", "series",
        }
        if set(record) != fields:
            raise ManifestError(f"{ticker} fields do not match schema 1")
        ticker_state = _text(record.get("state"), f"{ticker}.state", 16)
        if ticker_state not in TICKER_STATES:
            raise ManifestError(f"{ticker}.state is unsupported")
        _timestamp(record.get("updated_at"), f"{ticker}.updated_at")
        if not isinstance(record.get("missing"), list):
            raise ManifestError(f"{ticker}.missing must be a list")
        missing = [str(v) for v in record["missing"]]
        if len(missing) != len(set(missing)) or any(
                value not in DEBT_STAGES for value in missing):
            raise ManifestError(f"{ticker}.missing is invalid")
        if not isinstance(record.get("seal_pending"), bool):
            raise ManifestError(f"{ticker}.seal_pending must be a boolean")
        _text(record.get("note"), f"{ticker}.note", MAX_NOTE, allow_empty=True)
        if not isinstance(record.get("earliest"), bool):
            raise ManifestError(f"{ticker}.earliest must be a boolean")
        series = _validate_series_map(record.get("series"), f"{ticker}.series")
        normalized_selections.extend((ticker, iv) for iv in series)
        copy = dict(record)
        copy["series"] = series
        _recompute_ticker(copy)
        if copy["state"] != ticker_state or copy["missing"] != missing:
            raise ManifestError(f"{ticker} derived state is inconsistent")

    normalized_params = _normalize_params(data.get("params"), normalized_selections)
    if normalized_params != data["params"]:
        raise ManifestError("params are not normalized")

    finalize = data.get("finalize")
    if not isinstance(finalize, dict) or set(finalize) != {
            "at", "reason", "seal_pending"}:
        raise ManifestError("finalize fields do not match schema 1")
    _timestamp(finalize.get("at"), "finalize.at", allow_none=True)
    reason = finalize.get("reason")
    if reason is not None:
        _text(reason, "finalize.reason", 80)
    seal_pending = finalize.get("seal_pending")
    if not isinstance(seal_pending, list) or len(seal_pending) > MAX_SEAL_PENDING:
        raise ManifestError("finalize.seal_pending must be a bounded list")
    for index, item in enumerate(seal_pending):
        _text(item, f"finalize.seal_pending[{index}]", 128)

    if state == "complete" and (not data["fetch_finished"] or any(
            rec["state"] != "verified" for rec in data["tickers"].values())):
        raise ManifestError("complete run has unfinished work")
    return data


def _encode(data):
    validate_manifest(data)
    raw = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(raw) > MAX_BYTES:
        raise ManifestError("manifest exceeds size limit")
    return raw


def _atomic_write(path, raw, replace_fn=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        (replace_fn or os.replace)(temp, path)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _write_manifest(root, data, replace_fn=None):
    data["updated_at"] = data.get("updated_at") or data["created_at"]
    _atomic_write(active_path(root), _encode(data), replace_fn=replace_fn)
    return data


def load_run(root):
    path = active_path(root)
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ManifestError(f"cannot stat active manifest: {exc}") from exc
    if size <= 0 or size > MAX_BYTES:
        raise ManifestError("active manifest size is invalid")
    try:
        raw = path.read_bytes()
        data = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, ValueError, RecursionError) as exc:
        raise ManifestError(f"active manifest is unreadable: {exc}") from exc
    return validate_manifest(data)


def create_run(root, selections, params=None, *, run_id=None, now=None,
               earliest_tickers=(), replace_fn=None):
    normalized = []
    seen = set()
    for ticker, interval in selections or []:
        item = _canon_selection(ticker, interval)
        if item not in seen:
            seen.add(item)
            normalized.append(item)
    if not normalized:
        raise ManifestError("a run needs at least one selection")
    if len({ticker for ticker, _iv in normalized}) > MAX_TICKERS:
        raise ManifestError("too many tickers")
    per_ticker = {}
    for ticker, interval in normalized:
        per_ticker.setdefault(ticker, {})[interval] = _series_record(interval)
    if any(len(series) > MAX_SERIES_PER_TICKER for series in per_ticker.values()):
        raise ManifestError("too many series for one ticker")
    earliest = set()
    for ticker in earliest_tickers or ():
        try:
            earliest.add(ss.canonical_ticker(str(ticker)))
        except Exception:  # noqa: BLE001 - irrelevant sidecar junk is ignored
            continue
    stamp = _utc_text(now)
    run_id = run_id or new_run_id()
    if not _RUN_ID_RE.fullmatch(str(run_id)):
        raise ManifestError("run_id has unsafe characters")
    data = {
        "schema": SCHEMA,
        "run_id": str(run_id),
        "created_at": stamp,
        "updated_at": stamp,
        "state": "active",
        "fetch_finished": False,
        "params": _normalize_params(params, normalized),
        "tickers": {},
        "finalize": {"at": None, "reason": None, "seal_pending": []},
    }
    for ticker, series in per_ticker.items():
        data["tickers"][ticker] = {
            "state": "pending",
            "updated_at": stamp,
            "missing": [],
            "seal_pending": False,
            "note": "",
            "earliest": ticker in earliest,
            "series": series,
        }
    with _LOCK:
        if active_path(root).exists():
            try:
                current = load_run(root)
                detail = f"run {current['run_id']} is {current['state']}"
            except ManifestError as exc:
                detail = f"existing manifest is invalid ({exc})"
            raise ActiveRunExists(f"active Add Stocks state exists: {detail}")
        return _write_manifest(root, data, replace_fn=replace_fn)


def _load_expected(root, run_id):
    data = load_run(root)
    if data is None:
        raise ManifestError("active Add Stocks manifest is missing")
    if data["run_id"] != str(run_id):
        raise RunMismatch(
            f"stale run {run_id!r}; active run is {data['run_id']!r}")
    return data


def _ticker_record(data, ticker):
    try:
        ticker = ss.canonical_ticker(str(ticker))
    except Exception as exc:  # noqa: BLE001
        raise ManifestError(f"invalid ticker: {ticker!r}") from exc
    try:
        return ticker, data["tickers"][ticker]
    except KeyError as exc:
        raise ManifestError(f"ticker is not part of run: {ticker}") from exc


def _save_mutation(root, data, stamp, replace_fn=None):
    data["updated_at"] = stamp
    return _write_manifest(root, data, replace_fn=replace_fn)


def mark_series_started(root, run_id, ticker, interval, *, now=None):
    ticker, interval = _canon_selection(ticker, interval)
    with _LOCK:
        data = _load_expected(root, run_id)
        ticker, record = _ticker_record(data, ticker)
        if interval not in record["series"]:
            raise ManifestError(f"series is not part of run: {ticker} {interval}")
        stamp = _utc_text(now)
        series = record["series"][interval]
        series["state"] = "building"
        if _is_rth(interval):
            series["xval"] = False
            series["gaps"] = False
        record["updated_at"] = stamp
        record["note"] = ""
        _recompute_ticker(record)
        return _save_mutation(root, data, stamp)


def mark_series_pending(root, run_id, ticker, interval, note="", *, now=None):
    ticker, interval = _canon_selection(ticker, interval)
    with _LOCK:
        data = _load_expected(root, run_id)
        ticker, record = _ticker_record(data, ticker)
        if interval not in record["series"]:
            raise ManifestError(f"series is not part of run: {ticker} {interval}")
        stamp = _utc_text(now)
        record["series"][interval]["state"] = "pending"
        record["updated_at"] = stamp
        record["note"] = " ".join(str(note).split())[:MAX_NOTE]
        _recompute_ticker(record)
        return _save_mutation(root, data, stamp)


def mark_series_complete(root, run_id, ticker, interval, *, empty=False,
                         now=None):
    if not isinstance(empty, bool):
        raise ManifestError("empty must be a boolean")
    ticker, interval = _canon_selection(ticker, interval)
    with _LOCK:
        data = _load_expected(root, run_id)
        ticker, record = _ticker_record(data, ticker)
        if interval not in record["series"]:
            raise ManifestError(f"series is not part of run: {ticker} {interval}")
        stamp = _utc_text(now)
        series = record["series"][interval]
        series["state"] = "complete"
        if empty:
            series["xval"] = True
            series["gaps"] = True
        record["updated_at"] = stamp
        record["note"] = ""
        _recompute_ticker(record)
        return _save_mutation(root, data, stamp)


def _archive_complete(root, data, archive_dir=None, replace_fn=None):
    archive_dir = Path(archive_dir or default_archive_dir(root))
    archive_dir.mkdir(parents=True, exist_ok=True)
    target = archive_dir / f"addstock-run-{data['run_id']}.json"
    raw = _encode(data)
    if target.exists():
        try:
            existing = target.read_bytes()
        except OSError as exc:
            raise ManifestError(f"cannot read existing archive: {exc}") from exc
        if existing != raw:
            raise ManifestError(
                f"archive already exists with different bytes: {target.name}")
    else:
        _atomic_write(target, raw, replace_fn=replace_fn)
    try:
        active_path(root).unlink()
    except OSError as exc:
        raise ManifestError(f"could not remove active manifest: {exc}") from exc
    return target


def archive_complete(root, run_id, archive_dir=None):
    """Retry the final archive/remove boundary after an interrupted finalize."""
    with _LOCK:
        data = _load_expected(root, run_id)
        if data["state"] != "complete":
            raise ManifestError("only a complete run can be archived")
        return str(_archive_complete(root, data, archive_dir=archive_dir))


def _maybe_complete(root, data, stamp, archive_dir=None):
    if (not data["fetch_finished"]
            or data["finalize"].get("seal_pending")
            or any(record.get("seal_pending")
                   for record in data["tickers"].values())
            or any(
                record["state"] != "verified"
                for record in data["tickers"].values())):
        return {"manifest": _save_mutation(root, data, stamp),
                "archived": None}
    data["state"] = "complete"
    data["finalize"]["at"] = stamp
    data["finalize"]["reason"] = "complete"
    data["updated_at"] = stamp
    _write_manifest(root, data)
    target = _archive_complete(root, data, archive_dir=archive_dir)
    return {"manifest": data, "archived": str(target)}


def exempt_empty_verification(root, run_id, empty_fn, *, now=None,
                              archive_dir=None):
    """Excuse checks for completed RTH series proven to contain no data.

    The injected predicate is read-only and runs under the run-manifest lock so
    a concurrent restart cannot turn an earlier empty verdict into a waiver for
    a newly completed real series.  Predicate errors and non-boolean truthy
    values fail safe.
    """
    if not callable(empty_fn):
        raise ManifestError("empty_fn must be callable")

    with _LOCK:
        data = _load_expected(root, run_id)
        exempted = []
        stamp = None
        for ticker, record in data["tickers"].items():
            changed = False
            for interval, series in record["series"].items():
                if (not _is_rth(interval)
                        or series["state"] != "complete"
                        or (series["xval"] and series["gaps"])):
                    continue
                try:
                    is_empty = empty_fn(ticker, interval)
                except Exception:  # noqa: BLE001 - retain uncertain debt
                    continue
                if is_empty is not True:
                    continue
                if stamp is None:
                    stamp = _utc_text(now)
                series["xval"] = True
                series["gaps"] = True
                exempted.append((ticker, interval))
                changed = True
            if changed:
                record["updated_at"] = stamp
                _recompute_ticker(record)

        if not exempted:
            return {"exempted": [], "archived": None}

        result = _maybe_complete(
            root, data, stamp, archive_dir=archive_dir)
        return {"exempted": exempted, "archived": result["archived"]}


def mark_verification(root, run_id, ticker, stage, intervals=None, *, now=None,
                      archive_dir=None):
    stage = str(stage or "").strip().lower()
    if stage not in DEBT_STAGES:
        raise ManifestError(f"unsupported verification stage: {stage}")
    with _LOCK:
        data = _load_expected(root, run_id)
        ticker, record = _ticker_record(data, ticker)
        if stage == "earliest":
            record["earliest"] = True
        else:
            chosen = None
            if intervals is not None:
                chosen = {_canon_selection(ticker, iv)[1] for iv in intervals}
            matched = False
            for interval, series in record["series"].items():
                if not _is_rth(interval) or (chosen is not None
                                             and interval not in chosen):
                    continue
                series[stage] = True
                matched = True
            if chosen is not None and not matched:
                raise ManifestError(
                    f"no requested RTH series matched {ticker} {sorted(chosen)}")
        stamp = _utc_text(now)
        record["updated_at"] = stamp
        _recompute_ticker(record)
        return _maybe_complete(root, data, stamp, archive_dir=archive_dir)


def mark_fetch_finished(root, run_id, reason="complete", seal_pending=(), *,
                        now=None, archive_dir=None):
    reason = " ".join(str(reason or "complete").split())[:80]
    seal_pending = list(dict.fromkeys(
        " ".join(str(item).split())[:128] for item in seal_pending or ()
        if str(item).strip()))
    if len(seal_pending) > MAX_SEAL_PENDING:
        raise ManifestError("too many seal-pending entries")
    with _LOCK:
        data = _load_expected(root, run_id)
        stamp = _utc_text(now)
        data["fetch_finished"] = True
        data["state"] = "interrupted"
        data["finalize"] = {
            "at": stamp,
            "reason": reason,
            "seal_pending": seal_pending,
        }
        pending_tickers = {item.split()[0] for item in seal_pending}
        for ticker, record in data["tickers"].items():
            record["seal_pending"] = ticker in pending_tickers
            _recompute_ticker(record)
        return _maybe_complete(root, data, stamp, archive_dir=archive_dir)


def replace_seal_pending(root, run_id, seal_pending=(), *, now=None,
                         archive_dir=None):
    """Replace seal debt after the existing sealer and a fresh gap scan."""
    seal_pending = list(dict.fromkeys(
        " ".join(str(item).split())[:128] for item in seal_pending or ()
        if str(item).strip()))
    if len(seal_pending) > MAX_SEAL_PENDING:
        raise ManifestError("too many seal-pending entries")
    with _LOCK:
        data = _load_expected(root, run_id)
        stamp = _utc_text(now)
        pending_tickers = {item.split()[0] for item in seal_pending}
        data["finalize"]["seal_pending"] = seal_pending
        for ticker, record in data["tickers"].items():
            record["seal_pending"] = ticker in pending_tickers
            record["updated_at"] = stamp
            _recompute_ticker(record)
        return _maybe_complete(
            root, data, stamp, archive_dir=archive_dir)


def resume_run(root, run_id, *, now=None):
    with _LOCK:
        data = _load_expected(root, run_id)
        if data["state"] == "complete":
            raise ManifestError("a complete run cannot be resumed")
        stamp = _utc_text(now)
        data["state"] = "active"
        data["fetch_finished"] = False
        data["finalize"]["at"] = None
        data["finalize"]["reason"] = None
        for record in data["tickers"].values():
            for series in record["series"].values():
                if series["state"] == "building":
                    series["state"] = "pending"
            record["updated_at"] = stamp
            _recompute_ticker(record)
        return _save_mutation(root, data, stamp)


def pending_selections(data, *, include_earliest_probe=True):
    validate_manifest(data)
    ordered = []
    seen = set()

    def add(ticker, interval):
        item = (ticker, interval)
        if item not in seen:
            seen.add(item)
            ordered.append(item)

    for item in data["finalize"].get("seal_pending") or []:
        parts = str(item).split()
        if len(parts) == 2:
            try:
                ticker, interval = _canon_selection(parts[0], parts[1])
            except ManifestError:
                continue
            if ticker in data["tickers"] and interval in data["tickers"][ticker]["series"]:
                add(ticker, interval)
    for ticker, record in data["tickers"].items():
        for interval, series in record["series"].items():
            if series["state"] != "complete":
                add(ticker, interval)
        if (include_earliest_probe and record["state"] == "built"
                and "earliest" in record["missing"]
                and not any(item[0] == ticker for item in ordered)):
            interval = next((iv for iv in record["series"] if _is_rth(iv)),
                            next(iter(record["series"])))
            add(ticker, interval)
    return ordered


def current_xval_intervals(root, entries, *, fingerprint_fn=None,
                           optional_fingerprint_fn=None):
    """Return persisted xval intervals that still match stored input state."""
    fingerprint_fn = fingerprint_fn or ss.interval_state_fingerprint
    optional_fingerprint_fn = (
        optional_fingerprint_fn or ss.optional_interval_state_fingerprint)
    out = []
    seen = set()
    for entry in entries or ():
        if not isinstance(entry, dict):
            continue
        try:
            ticker, interval = _canon_selection(
                entry.get("ticker"), entry.get("interval"))
        except ManifestError:
            continue
        selected = entry.get("interval_fingerprint")
        if (not isinstance(selected, dict)
                or selected.get("current") is not True
                or not _valid_sha256(selected.get("after_sha256"))):
            continue
        try:
            observed = fingerprint_fn(root, ticker, interval)
            if (not isinstance(observed, dict)
                    or observed.get("sha256")
                    != selected.get("after_sha256")):
                continue
            reference = entry.get("reference_interval_fingerprint")
            if reference is not None:
                if (not isinstance(reference, dict)
                        or reference.get("current") is not True
                        or not _valid_sha256(
                            reference.get("after_sha256"))):
                    continue
                _ticker, reference_interval = _canon_selection(
                    ticker, entry.get("reference_interval"))
                observed_reference = optional_fingerprint_fn(
                    root, ticker, reference_interval)
                if (not isinstance(observed_reference, dict)
                        or observed_reference.get("sha256")
                        != reference.get("after_sha256")):
                    continue
        except Exception:  # noqa: BLE001 - unreadable evidence stays debt
            continue
        if interval not in seen:
            seen.add(interval)
            out.append(interval)
    return out


def summary(data):
    validate_manifest(data)
    pending = sum(record["state"] in {"pending", "building"}
                  for record in data["tickers"].values())
    debt = sum(record["state"] == "built"
               for record in data["tickers"].values())
    verified = sum(record["state"] == "verified"
                   for record in data["tickers"].values())
    return {
        "run_id": data["run_id"],
        "state": data["state"],
        "pending": pending,
        "built_unverified": debt,
        "verified": verified,
        "total": len(data["tickers"]),
        "remaining_series": len(pending_selections(
            data, include_earliest_probe=False)),
    }


def discard_active(root, archive_dir=None, *, now=None):
    with _LOCK:
        path = active_path(root)
        if not path.exists():
            return None
        archive_dir = Path(archive_dir or default_archive_dir(root))
        archive_dir.mkdir(parents=True, exist_ok=True)
        stamp = _utc_text(now).replace(":", "").replace("+", "-")
        try:
            data = load_run(root)
            identity = data["run_id"]
        except ManifestError:
            identity = "invalid"
        target = archive_dir / (
            f"addstock-run-{identity}-discarded-{stamp}.json")
        counter = 1
        while target.exists():
            target = archive_dir / (
                f"addstock-run-{identity}-discarded-{stamp}-{counter}.json")
            counter += 1
        try:
            os.replace(path, target)
        except OSError as exc:
            raise ManifestError(f"could not discard active manifest: {exc}") from exc
    return str(target)
