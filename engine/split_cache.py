"""Identity-safe split-history cache and explicit refresh CLI.

Normal operation is offline. The three refresh roots remain production-held;
admitted offline proofs use the engine-owned guarded provider. Cache files live under the
reserved ``_split_history`` storage-root namespace; no ticker manifest or bar
file is modified.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import hashlib
import math
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

import deep_seam_scan as seam_scan
import split_detector as detector
import stock_storage as storage
import stock_validate as validate
import fetch_operations as fops
from fetch_authority import (AuthorityError, ResponseQuarantined,
                             digest_value, strict_json)
from fetch_ledger import HTTP_EVIDENCE_BUDGET, LedgerError, inspect_ledger
from fetch_run_context import RequestCancelled, RequestRefused


CACHE_DIR_NAME = "_split_history"
CACHE_KIND = "split_history_cache"
CACHE_VERSION = 1
MAX_BANK_REFRESH = 1000
DEFAULT_PACE_SECONDS = 0.25
SPOOL_ITEM_BYTES = 32 * 1024 * 1024
SPOOL_TOTAL_BYTES = 128 * 1024 * 1024
MANIFEST_READ_BYTES = 16 * 1024 * 1024
OPTIONS_BYTES = 1024 * 1024
MAX_REQUESTED_NAMES = 10000
ROOT_BUDGET_SECONDS = 21600.0
_SPLIT_RIGHTS = frozenset({"http.split_provider.yahoo",
    "http.split_provider.sec_tickers", "http.split_provider.sec_facts",
    "http.split_provider.issuer"})
_UNSET_AGENT = object()

_LOCKS_GUARD = threading.Lock()
_PATH_LOCKS = {}


class SplitCacheError(RuntimeError):
    """Base cache/refresh error."""


class CacheValidationError(SplitCacheError):
    """Cache bytes do not satisfy the schema contract."""


class CacheIdentityError(SplitCacheError):
    """Cache identity does not match the active manifest identity."""


class PublicationError(LedgerError):
    """A sealed operation has exact recovery debt, not clean batch success."""

    def __init__(self, debt):
        self.debt = debt
        super().__init__("split publication stopped; inspect recovery debt")


def _utc_now(now=None):
    value = now() if callable(now) else now
    if value is None:
        value = dt.datetime.now(dt.timezone.utc)
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        value = dt.datetime.combine(value, dt.time(), tzinfo=dt.timezone.utc)
    if not isinstance(value, dt.datetime):
        raise CacheValidationError("invalid current time")
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _timestamp(value, label):
    text = str(value or "").strip()
    if not text:
        raise CacheValidationError(f"missing {label}")
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CacheValidationError(f"invalid {label}: {value!r}") from exc
    if parsed.tzinfo is None:
        raise CacheValidationError(f"{label} must include a timezone")
    return parsed.astimezone(dt.timezone.utc)


def _coerce_conid(value):
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise CacheIdentityError(f"invalid conId: {value!r}")
    try:
        conid = int(value)
    except (TypeError, ValueError) as exc:
        raise CacheIdentityError(f"invalid conId: {value!r}") from exc
    if conid <= 0:
        raise CacheIdentityError(f"invalid conId: {value!r}")
    return conid


def _ticker(value):
    try:
        return storage.canonical_ticker(value)
    except Exception as exc:  # noqa: BLE001 - normalize storage exception
        raise CacheIdentityError(f"invalid ticker: {value!r}") from exc


def _provider_symbol(value):
    symbol = str(value or "").strip().upper()
    if not symbol or len(symbol) > 40 or any(ch in symbol for ch in "\r\n\t"):
        raise CacheIdentityError(f"invalid provider symbol: {value!r}")
    return symbol


def normalize_identity(identity):
    if not isinstance(identity, dict):
        raise CacheIdentityError("identity is not an object")
    return {
        "ticker": _ticker(identity.get("ticker")),
        "conid": _coerce_conid(identity.get("conid")),
        "provider_symbol": _provider_symbol(identity.get("provider_symbol")),
    }


def manifest_identity(root, ticker, provider_symbol=None):
    """Load the active folder/symbol/conId identity without modifying it."""
    root = Path(root)
    ticker = _ticker(ticker)
    manifest = storage.load_manifest(root / ticker)
    if not manifest:
        raise CacheIdentityError(f"manifest unavailable for {ticker}")
    manifest_folder = _ticker(manifest.get("folder") or ticker)
    if manifest_folder != ticker:
        raise CacheIdentityError(
            f"manifest folder {manifest_folder} does not match {ticker}")
    symbol = provider_symbol
    if symbol is None:
        symbol = manifest.get("provider_symbol") or manifest.get("symbol")
    return normalize_identity({
        "ticker": ticker,
        "conid": manifest.get("conid"),
        "provider_symbol": symbol,
    })


def cache_root(root):
    return Path(root) / CACHE_DIR_NAME


def cache_path(root, identity):
    identity = normalize_identity(identity)
    conid = identity["conid"] if identity["conid"] is not None else "unknown"
    return cache_root(root) / f"{identity['ticker']}__{conid}.json"


def _path_lock(path):
    key = str(Path(path).resolve()).casefold()
    with _LOCKS_GUARD:
        return _PATH_LOCKS.setdefault(key, threading.Lock())


def _json_object(path):
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise CacheValidationError(
            f"cannot read split cache {Path(path).name}: {exc}") from exc
    if not isinstance(payload, dict):
        raise CacheValidationError("split cache is not an object")
    return payload


def _valid_fingerprint(value):
    if value is None:
        return True
    text = str(value).strip().lower()
    return (len(text) == 64
            and all(ch in "0123456789abcdef" for ch in text))


def validate_cache_payload(payload, expected_identity=None):
    """Validate and normalize one version-1 cache object."""
    if not isinstance(payload, dict):
        raise CacheValidationError("split cache is not an object")
    if payload.get("kind") != CACHE_KIND:
        raise CacheValidationError("wrong split cache kind")
    if payload.get("version") != CACHE_VERSION:
        raise CacheValidationError("unsupported split cache version")
    try:
        identity = normalize_identity({
            "ticker": payload.get("ticker"),
            "conid": payload.get("conid"),
            "provider_symbol": payload.get("provider_symbol"),
        })
    except CacheIdentityError as exc:
        raise CacheValidationError(str(exc)) from exc
    if expected_identity is not None:
        expected = normalize_identity(expected_identity)
        mismatches = [
            key for key in ("ticker", "conid", "provider_symbol")
            if identity[key] != expected[key]
        ]
        if mismatches:
            detail = ", ".join(
                f"{key}={identity[key]!r} expected {expected[key]!r}"
                for key in mismatches)
            raise CacheIdentityError(f"split cache identity mismatch: {detail}")
    fetched = _timestamp(payload.get("fetched_at"), "fetched_at")
    provider = str(payload.get("provider") or "").strip()
    if not provider:
        raise CacheValidationError("split cache provider is missing")
    if payload.get("provider_status") != "ok":
        raise CacheValidationError("split cache provider status is not ok")
    sources = payload.get("sources")
    if not isinstance(sources, dict) or not sources:
        raise CacheValidationError("split cache source provenance is missing")
    if not all(isinstance(value, dict) for value in sources.values()):
        raise CacheValidationError("split cache source provenance is malformed")

    history = detector.normalize_history({
        "events": payload.get("events"),
        "coverage": payload.get("coverage"),
        "source_status": "ok",
    })
    if history["errors"]:
        raise CacheValidationError(
            "invalid split events: " + "; ".join(history["errors"]))
    if history["coverage"]["errors"]:
        raise CacheValidationError(
            "invalid split coverage: "
            + "; ".join(history["coverage"]["errors"]))
    captured_source_ids = set(sources)
    referenced_source_ids = set(history["coverage"]["source_ids"])
    for event in history["events"]:
        referenced_source_ids.update(event["source_ids"])
    missing_sources = sorted(referenced_source_ids - captured_source_ids)
    if missing_sources:
        raise CacheValidationError(
            "split cache has uncaptured source identifiers: "
            + ", ".join(missing_sources))

    detection = payload.get("detection")
    if not isinstance(detection, dict):
        raise CacheValidationError("split cache detection is not an object")
    raw_candidates = detection.get("candidates")
    if not isinstance(raw_candidates, list):
        raise CacheValidationError("split cache candidates are not a list")
    fingerprint = detection.get("series_fingerprint")
    if not _valid_fingerprint(fingerprint):
        raise CacheValidationError("invalid series fingerprint")
    if fingerprint is not None:
        fingerprint = str(fingerprint).strip().lower()
    reference_asof = detection.get("reference_asof")
    if reference_asof is not None:
        reference_asof = _timestamp(
            reference_asof, "detection reference_asof").isoformat(
                timespec="seconds")
    candidates = []
    for index, raw_candidate in enumerate(raw_candidates):
        try:
            candidate = detector.normalize_candidate(raw_candidate)
        except (TypeError, ValueError) as exc:
            raise CacheValidationError(
                f"invalid split candidate {index}: {exc}") from exc
        if candidate["ticker"] != identity["ticker"]:
            raise CacheValidationError(
                f"split candidate {index} ticker {candidate['ticker']} does not "
                f"match {identity['ticker']}")
        candidates.append(candidate)
    normalized_detection = dict(detection)
    normalized_detection.update({
        "series_fingerprint": fingerprint,
        "reference_asof": reference_asof,
        "candidates": candidates,
    })

    cik = payload.get("cik")
    if cik is not None:
        if isinstance(cik, bool):
            raise CacheValidationError("invalid cache CIK")
        try:
            cik = int(cik)
        except (TypeError, ValueError) as exc:
            raise CacheValidationError("invalid cache CIK") from exc
        if cik <= 0:
            raise CacheValidationError("invalid cache CIK")

    normalized = dict(payload)
    normalized.update({
        **identity,
        "cik": cik,
        "fetched_at": fetched.isoformat(timespec="seconds"),
        "provider": provider,
        "provider_status": "ok",
        "coverage": history["coverage"],
        "events": history["events"],
        "sources": dict(sources),
        "detection": normalized_detection,
    })
    return normalized


def build_cache_payload(identity, provider_result, detection=None):
    identity = normalize_identity(identity)
    if not isinstance(provider_result, dict):
        raise CacheValidationError("provider result is not an object")
    provider_ticker = _ticker(provider_result.get("ticker"))
    provider_symbol = _provider_symbol(provider_result.get("provider_symbol"))
    if provider_ticker != identity["ticker"]:
        raise CacheIdentityError(
            f"provider ticker {provider_ticker} != {identity['ticker']}")
    if provider_symbol != identity["provider_symbol"]:
        raise CacheIdentityError(
            f"provider symbol {provider_symbol} != "
            f"{identity['provider_symbol']}")
    fetched_at = provider_result.get("fetched_at")
    if detection is None:
        detection = {
            "series_fingerprint": None,
            "reference_asof": fetched_at,
            "candidates": [],
        }
    payload = {
        "kind": CACHE_KIND,
        "version": CACHE_VERSION,
        **identity,
        "cik": provider_result.get("cik"),
        "fetched_at": fetched_at,
        "provider": provider_result.get("provider"),
        "provider_status": provider_result.get("provider_status"),
        "coverage": provider_result.get("coverage"),
        "events": provider_result.get("events"),
        "sources": provider_result.get("sources"),
        "detection": detection,
    }
    return validate_cache_payload(payload, expected_identity=identity)


def write_cache(root, identity, payload):
    """Atomically replace one validated per-identity cache."""
    identity = normalize_identity(identity)
    normalized = validate_cache_payload(payload, expected_identity=identity)
    path = cache_path(root, identity)
    encoded = (json.dumps(
        normalized, sort_keys=True, separators=(",", ":")) + "\n").encode(
            "utf-8")
    with _path_lock(path):
        storage._atomic_write_bytes(path, encoded)
        committed = validate_cache_payload(
            _json_object(path), expected_identity=identity)
    return path, committed


def _other_identity_files(root, identity):
    directory = cache_root(root)
    if not directory.is_dir():
        return []
    identity = normalize_identity(identity)
    paths = set(directory.glob(f"{identity['ticker']}__*.json"))
    if identity["conid"] is not None:
        paths.update(directory.glob(f"*__{identity['conid']}.json"))
    paths.discard(cache_path(root, identity))
    return sorted(paths, key=lambda path: path.name.casefold())


def load_cache(root, identity, *, max_age=None, now=None):
    """Load one cache with exact ticker/conId/provider-symbol gating."""
    identity = normalize_identity(identity)
    path = cache_path(root, identity)
    if not path.is_file():
        others = _other_identity_files(root, identity)
        if others:
            return {
                "status": "identity_mismatch",
                "usable": False,
                "path": str(path),
                "other_identity_files": [str(item) for item in others],
            }
        return {"status": "missing", "usable": False, "path": str(path)}
    try:
        payload = validate_cache_payload(
            _json_object(path), expected_identity=identity)
    except CacheIdentityError as exc:
        return {
            "status": "identity_mismatch",
            "usable": False,
            "path": str(path),
            "error": str(exc),
        }
    except CacheValidationError as exc:
        return {
            "status": "corrupt",
            "usable": False,
            "path": str(path),
            "error": str(exc),
        }
    if identity["conid"] is None:
        return {
            "status": "identity_unknown",
            "usable": False,
            "path": str(path),
            "cache": payload,
        }
    fetched = _timestamp(payload["fetched_at"], "fetched_at")
    current = _utc_now(now)
    age = current - fetched
    if age < -dt.timedelta(minutes=5):
        return {
            "status": "stale",
            "usable": False,
            "path": str(path),
            "age_seconds": age.total_seconds(),
            "cache": payload,
            "reason": "fetched_at_is_in_the_future",
        }
    if max_age is not None:
        age_limit = (max_age if isinstance(max_age, dt.timedelta)
                     else dt.timedelta(seconds=float(max_age)))
        if age > age_limit:
            return {
                "status": "stale",
                "usable": False,
                "path": str(path),
                "age_seconds": age.total_seconds(),
                "cache": payload,
            }
    return {
        "status": "ok",
        "usable": True,
        "fetch_verified": False,
        "fetch_evidence": "unverified_offline",
        "path": str(path),
        "cache": payload,
    }


def list_manifest_tickers(root):
    root = Path(root)
    if not root.is_dir():
        return []
    out = []
    for path in sorted(root.iterdir(), key=lambda item: item.name.casefold()):
        if (path.is_dir() and not path.name.startswith("_")
                and storage.load_manifest(path)):
            try:
                out.append(_ticker(path.name))
            except CacheIdentityError:
                continue
    return out


def offline_status(root, tickers=None, *, provider_symbols=None,
                   max_age=None, now=None):
    root = Path(root)
    provider_symbols = dict(provider_symbols or {})
    names = list_manifest_tickers(root) if tickers is None else list(tickers)
    rows = []
    for ticker in sorted({_ticker(name) for name in names}):
        try:
            identity = manifest_identity(
                root, ticker, provider_symbol=provider_symbols.get(ticker))
            row = load_cache(
                root, identity, max_age=max_age, now=now)
            row["ticker"] = ticker
            row["identity"] = identity
        except SplitCacheError as exc:
            row = {
                "ticker": ticker,
                "status": "identity_error",
                "usable": False,
                "error": str(exc),
            }
        rows.append(row)
    return {
        "kind": "split_cache_offline_status",
        "version": 1,
        "network": False,
        "root": str(root),
        "rows": rows,
        "counts": {
            status: sum(row["status"] == status for row in rows)
            for status in sorted({row["status"] for row in rows})
        },
    }


def _detection_from_scan(identity, scan, reference_asof):
    if not isinstance(scan, dict):
        raise CacheValidationError("candidate scan result is not an object")
    scan_ticker = _ticker(scan.get("ticker"))
    if scan_ticker != identity["ticker"]:
        raise CacheIdentityError(
            f"candidate scan ticker {scan_ticker} != {identity['ticker']}")
    verdict = str(scan.get("verdict") or "").strip().upper()
    candidates = scan.get("candidates")
    if verdict not in {"CLEAN", "SEAM"}:
        reason = str(scan.get("why") or scan.get("error") or verdict or "unknown")
        raise CacheValidationError(
            f"candidate detection is not usable: {reason}")
    if not isinstance(candidates, list):
        raise CacheValidationError("candidate scan candidates are not a list")
    if verdict == "CLEAN" and candidates:
        raise CacheValidationError("CLEAN candidate scan contains candidates")
    if verdict == "SEAM" and not candidates:
        raise CacheValidationError("SEAM candidate scan contains no candidates")
    fingerprint = scan.get("series_fingerprint")
    if fingerprint is None or not _valid_fingerprint(fingerprint):
        raise CacheValidationError("candidate scan fingerprint is missing or invalid")
    return {
        "series_fingerprint": str(fingerprint).strip().lower(),
        "reference_asof": reference_asof,
        "candidates": candidates,
    }


def _cancel_requested(cancel):
    def observe_cancel():
        if cancel is None:
            return False
        if callable(cancel):
            return bool(cancel())
        is_set = getattr(cancel, "is_set", None)
        return bool(is_set()) if callable(is_set) else bool(cancel)
    return _observe(observe_cancel)


def _observe(callback, *args, **kwargs):
    from fetch_ibkr_bridge import without_authority
    # wraps() itself reads callback attributes: construction is an observer too.
    with fops.observer_scope():
        return without_authority(callback)(*args, **kwargs)


def _emit_progress(progress, stage, ticker):
    if progress is None:
        return
    def observe_progress():
        try:
            progress(str(stage), str(ticker))
        except (AuthorityError, LedgerError, RequestCancelled, ResponseQuarantined):
            raise
        except Exception:  # Ordinary UI status faults do not grant authority.
            pass
    _observe(observe_progress)


def _bounded_json(value, maximum):
    encoded = bytearray()
    for part in json.JSONEncoder(sort_keys=True, separators=(",", ":"),
                                 ensure_ascii=True, allow_nan=False).iterencode(value):
        block = part.encode("utf-8")
        if len(encoded) + len(block) > maximum:
            raise LedgerError("split evidence byte limit exceeded")
        encoded.extend(block)
    return bytes(encoded)


def _bounded_read(path, maximum, *, missing=False):
    try:
        with Path(path).open("rb") as stream:
            raw = stream.read(maximum + 1)
    except FileNotFoundError:
        if missing:
            return None
        raise
    if len(raw) > maximum:
        raise LedgerError("split input byte limit exceeded")
    return raw


def _bytes_digest(raw):
    return None if raw is None else hashlib.sha256(raw).hexdigest()


def _manifest_snapshot(root, ticker, provider_symbol=None):
    ticker = _ticker(ticker)
    path = Path(root) / ticker / storage.MANIFEST_NAME
    raw = _bounded_read(path, MANIFEST_READ_BYTES, missing=True)
    if raw is None:
        raise CacheIdentityError("manifest unavailable")
    try:
        manifest = strict_json(raw)
    except AuthorityError as error:
        raise CacheIdentityError("manifest is not valid JSON") from error
    if (type(manifest) is not dict or type(manifest.get("intervals")) is not dict
            or any(type(item) is not dict
                   or type(item.get("months")) is not dict
                   or any(type(month) is not dict for month in item["months"].values())
                   or type(item.get("verified_absent", [])) is not list
                   for item in manifest["intervals"].values())):
        raise CacheIdentityError("manifest is unavailable or malformed")
    if _ticker(manifest.get("folder") or ticker) != ticker:
        raise CacheIdentityError("manifest folder identity differs")
    symbol = (provider_symbol if provider_symbol is not None else
              manifest.get("provider_symbol") or manifest.get("symbol"))
    identity = normalize_identity(dict(ticker=ticker, conid=manifest.get("conid"),
                                       provider_symbol=symbol))
    return identity, manifest, _bytes_digest(raw)


def _split_directory(evidence_dir):
    if evidence_dir is not None and type(evidence_dir) not in (str, type(Path("."))):
        raise RequestRefused("split evidence directory must be a detached path")
    return (Path(evidence_dir) if evidence_dir is not None else
            Path(__file__).resolve().parents[1] / "_ingest_reports" / "fetch-ledgers")


class _RootCheck:
    """Owned cancel proxy; foreign observation is confined on every lookup/call."""

    def __init__(self, operation, cancel, budget_seconds):
        self.operation = operation
        self.cancel = cancel
        if (type(budget_seconds) not in (int, float)
                or not math.isfinite(budget_seconds)
                or not 0 < budget_seconds <= ROOT_BUDGET_SECONDS):
            raise RequestRefused("invalid split root deadline budget")
        self.deadline = time.monotonic() + budget_seconds

    def check(self, *, sealed=False):
        context = fops.require_root_admission(self.operation)
        if not sealed:
            context.ledger.require_live()
        self.is_set()
        return context

    def is_set(self):
        # Executors call cancellation with authority cleared. Admission checks
        # belong to the owning boundary/executor, never this observer proxy.
        if time.monotonic() >= self.deadline:
            raise RequestRefused("split root deadline expired")
        if _cancel_requested(self.cancel):
            raise RequestCancelled("split refresh cancelled")
        return False


class _PublicationSpool:
    """Bounded detached values outside the bank, retained even after failure."""

    def __init__(self, context, root):
        bank = Path(root).resolve()
        parent = context.ledger.path.parent.resolve()
        if parent == bank or parent.is_relative_to(bank):
            raise RequestRefused("split evidence directory must be outside the bank")
        self.path = Path(tempfile.mkdtemp(prefix="split-" + context.operation_id + "-",
                                          dir=parent))
        self.items = []
        self.total_bytes = 0

    @staticmethod
    def _save(path, raw):
        with path.open("xb") as stream:
            if stream.write(raw) != len(raw):
                raise LedgerError("short split spool write")
            stream.flush()
            os.fsync(stream.fileno())
        if _bounded_read(path, SPOOL_ITEM_BYTES) != raw:
            raise LedgerError("split spool verification failed")

    def append(self, item, row_index):
        if len(self.items) >= MAX_BANK_REFRESH:
            raise LedgerError("split spool item limit exceeded")
        raw = _bounded_json(item, SPOOL_ITEM_BYTES)
        if self.total_bytes + len(raw) > SPOOL_TOTAL_BYTES:
            raise LedgerError("split spool aggregate limit exceeded")
        name = "item-" + str(len(self.items)).zfill(4) + ".json"
        try:
            self._save(self.path / name, raw)
        except OSError as error:
            raise LedgerError("split spool persistence failed") from error
        self.total_bytes += len(raw)
        self.items.append(dict(name=name, sha256=_bytes_digest(raw), bytes=len(raw),
                               row_index=row_index, ticker=item["cache"]["ticker"]))

    def load(self, entry):
        raw = _bounded_read(self.path / entry["name"], SPOOL_ITEM_BYTES)
        if len(raw) != entry["bytes"] or _bytes_digest(raw) != entry["sha256"]:
            raise LedgerError("split spool custody differs")
        return strict_json(raw)

    def record(self, name, value):
        raw = _bounded_json(value, OPTIONS_BYTES)
        try:
            self._save(self.path / name, raw)
        except OSError as error:
            raise LedgerError("split recovery evidence persistence failed") from error


def _reference_preflight(context):
    from fetch_envelopes import decide, parse_envelope
    from fetch_http_labels import stockanalysis_bounds
    bounds = stockanalysis_bounds(context.authority, context.captured_now, "Max")
    request = parse_envelope(dict(variant="http-series", endpoint="stockanalysis.history",
        subject="SPLIT-PREFLIGHT", token="1d", intended_start=bounds["intended_start"],
        intended_end=bounds["intended_end"]))
    decision = decide(request, context.authority, context.horizons,
                      captured_now=context.captured_now)
    if decision.effective is None:
        raise RequestRefused(decision.reason)


def _detached_options(tickers, provider_symbols, cik_bindings, limit, pace_seconds):
    from fetch_http_splits import _cik, _symbol
    def detach():
        if type(limit) is not int or not 1 <= limit <= MAX_BANK_REFRESH:
            raise RequestRefused("invalid split refresh limit")
        if (type(pace_seconds) not in (int, float) or not math.isfinite(pace_seconds)
                or not 0 <= pace_seconds <= 1):
            raise RequestRefused("split pace above canonical floor is not supported")
        if type(tickers) not in (list, tuple) or len(tickers) > MAX_REQUESTED_NAMES:
            raise RequestRefused("split ticker input must be a bounded sequence")
        if (any(type(name) is not str or len(name) > 40 for name in tickers)
                or type(provider_symbols) is not dict or type(cik_bindings) is not dict):
            raise RequestRefused("split options must contain detached values")
        raw = _bounded_json(dict(tickers=tickers, provider_symbols=provider_symbols,
                                  cik_bindings=cik_bindings), OPTIONS_BYTES)
        values = strict_json(raw)
        names = sorted({_ticker(name) for name in values["tickers"]})
        symbols = {_ticker(key): _symbol(value)
                   for key, value in values["provider_symbols"].items()}
        ciks = {_ticker(key): _cik(value) for key, value in values["cik_bindings"].items()}
        return names, symbols, ciks
    return _observe(detach)


def _bank_names(root):
    root = Path(root)
    if not root.is_dir():
        return []
    names = []
    for index, path in enumerate(root.iterdir()):
        if index >= MAX_REQUESTED_NAMES:
            raise RequestRefused("split bank directory limit exceeded")
        if path.is_dir() and not path.name.startswith("_"):
            try:
                identity, _manifest, _digest = _manifest_snapshot(root, path.name)
                names.append(identity["ticker"])
            except CacheIdentityError:
                continue
    return names


def _row_failure(ticker, error, *, path=None, prior_usable=False):
    detail = _observe(str, error)[:500]
    row = dict(ticker=ticker, status="error", preserved_last_good=prior_usable,
               error=type(error).__name__ + ": " + detail)
    if path is not None:
        row["path"] = str(path)
    return row


def _require_quarantined_row(context, worker_id, reference_worker, error):
    locator = _observe(getattr, error, "quarantine_path", None)
    audit = inspect_ledger(context.ledger.path, require_seal=False)
    if (type(locator) is not str or not audit["events"]
            or audit["operation_id"] != context.operation_id):
        raise LedgerError("split ROW has no durable quarantine") from error
    event = audit["events"][-1]
    if (event["event"] != "result" or event["worker_id"] not in {worker_id, reference_worker}
            or event["payload"].get("error_type") != "ResponseQuarantined"):
        raise LedgerError("split quarantine has no matching rejected result") from error
    path = context.ledger.path.with_suffix("." + event["attempt_id"] + ".quarantine.json")
    if Path(locator).name != path.name:
        raise LedgerError("split quarantine locator differs") from error
    try:
        retained = strict_json(_bounded_read(path, HTTP_EVIDENCE_BUDGET))
        raw = base64.b64decode(retained["response"]["raw_response_base64"], validate=True)
    except (OSError, AuthorityError, KeyError, ValueError, TypeError) as invalid:
        raise LedgerError("split quarantine evidence is unavailable") from invalid
    if _bytes_digest(raw) != event["payload"].get("raw_response_digest"):
        raise LedgerError("split quarantine bytes differ from rejected result") from error


def _refresh_ticker_body(root, ticker, provider, worker, operation, spool, check,
                         row_index, *, provider_symbol=None, cik=None,
                         scan_fn=None, progress=None):
    """No mint, callback authority or publication: only one staged candidate."""
    from fetch_http_splits import _cik
    from split_provider import ProviderError
    check.check()
    _emit_progress(progress, "identity", ticker)
    path, prior_usable = None, False
    try:
        identity, manifest, manifest_digest = _manifest_snapshot(root, ticker, provider_symbol)
        if identity["conid"] is None:
            raise CacheIdentityError("manifest conId is missing; refusing symbol-only refresh")
        try:
            validate.stockanalysis_symbol(identity["provider_symbol"])
        except storage.StorageError as error:
            raise RequestRefused("split provider symbol has no supported reference identity") from error
        bound_cik = manifest.get("cik")
        if bound_cik is not None:
            bound_cik = _cik(bound_cik)
            if cik is not None and bound_cik != cik:
                raise CacheIdentityError("configured CIK differs from manifest")
            cik = bound_cik
        path = cache_path(root, identity)
        old_raw = _bounded_read(path, SPOOL_ITEM_BYTES, missing=True)
        if old_raw is not None:
            try:
                validate_cache_payload(strict_json(old_raw), expected_identity=identity)
                prior_usable = True
            except (SplitCacheError, AuthorityError, TypeError, ValueError):
                pass  # Retain even unusable old bytes until a successful CAS.
        old_digest = _bytes_digest(old_raw)
        del old_raw
        check.check()
        _emit_progress(progress, "provider_history", ticker)
        history = provider.fetch_history(ticker, identity["provider_symbol"], cik=cik,
                                          _fetch_worker=worker, _cancel=check)
        check.check()
        payload = build_cache_payload(identity, history)
        pending_reference = []
        reference_worker = "split-reference-" + str(row_index)
        _emit_progress(progress, "daily_reference", ticker)
        # A sibling with only validation rights; never widen the split worker.
        child = operation.child(reference_worker, rights={"http.stockanalysis.validation"})
        with fops.scoped_worker(child, close=True) as nested:
            reference = validate.fetch_daily_reference(identity["provider_symbol"], rng="Max",
                _fetch_worker=nested, _pending_cache=pending_reference, _force_fresh=True,
                _now=worker.context.captured_now.timestamp(), _cancel=check)
        check.check()
        if len(pending_reference) != 1:
            raise LedgerError("split reference has no unique guarded companion")
        _emit_progress(progress, "candidate_scan", ticker)
        def observe_scan():
            # Detachment, lookup and invocation all lack operation/send authority.
            detached_reference = strict_json(_bounded_json(reference, SPOOL_ITEM_BYTES))
            scanner = seam_scan.scan_ticker_steps if scan_fn is None else scan_fn
            scan = scanner(Path(root), ticker, ref=detached_reference)
            try:
                scan = strict_json(_bounded_json(scan, SPOOL_ITEM_BYTES))
            except (TypeError, ValueError) as error:
                if isinstance(error, AuthorityError):
                    raise
                raise CacheValidationError("candidate scan is not detached finite JSON") from error
            return scan
        scan = _observe(observe_scan)
        payload["detection"] = _detection_from_scan(identity, scan, payload["fetched_at"])
        if payload["detection"]["series_fingerprint"] != seam_scan.manifest_fingerprint(
                storage.manifest_months(manifest, "1d")):
            raise CacheValidationError("candidate fingerprint differs from the captured manifest")
        payload = validate_cache_payload(payload, expected_identity=identity)
        check.check()
        key, reference_entry = pending_reference[0]
        reference_entry["provider_symbol"] = key[0]
        companion = dict(identity=identity, provider_symbol_override=provider_symbol,
            configured_cik=cik, resolved_cik=payload["cik"], manifest_sha256=manifest_digest,
            old_cache_sha256=old_digest, cache_digest=digest_value(payload),
            authority_fingerprint=worker.context.authority.fingerprint,
            table_digest=worker.context.authority.table_digest,
            reference_worker=reference_worker,
            series_fingerprint=payload["detection"]["series_fingerprint"])
        spool.append(dict(cache=payload, reference=reference_entry, companion=companion), row_index)
        return dict(ticker=ticker, status="staged", path=str(path),
            conid=identity["conid"], provider_symbol=identity["provider_symbol"], cik=payload["cik"],
            event_count=len(payload["events"]), candidate_count=len(payload["detection"]["candidates"]),
            series_fingerprint=payload["detection"]["series_fingerprint"],
            coverage_complete=payload["coverage"]["complete"], preserved_last_good=prior_usable)
    except ResponseQuarantined as error:
        _require_quarantined_row(worker.context, worker.worker_id,
                                  "split-reference-" + str(row_index), error)
        check.check()
        return _row_failure(ticker, error, path=path, prior_usable=prior_usable)
    except (AuthorityError, LedgerError, RequestCancelled):
        raise
    except (ProviderError, SplitCacheError, TimeoutError, OSError) as error:
        check.check()
        return _row_failure(ticker, error, path=path, prior_usable=prior_usable)


def _audit_indexes(audit):
    if not audit["verified"] or audit["events"][-1]["event"] != "seal":
        raise LedgerError("split publication has no verified seal")
    logical, workers, decisions = {}, {}, {}
    for event in audit["events"]:
        if event["event"] == "decision":
            decisions[event["attempt_id"]] = event
        if event["event"] != "seal":
            workers.setdefault(event["worker_id"], []).append(event)
        if event["event"] == "result":
            logical.setdefault(event["logical_id"], []).append((decisions[event["attempt_id"]], event))
    return logical, workers


def _verify_item(item, context, audit, indexes):
    companion, reference = item["companion"], item["reference"]
    payload = validate_cache_payload(item["cache"], companion["identity"])
    if (companion["cache_digest"] != digest_value(payload)
            or companion["authority_fingerprint"] != context.authority.fingerprint
            or companion["table_digest"] != context.authority.table_digest
            or companion["resolved_cik"] != payload["cik"]
            or (companion["configured_cik"] is not None
                and companion["configured_cik"] != payload["cik"])
            or companion["series_fingerprint"] != payload["detection"]["series_fingerprint"]):
        raise LedgerError("split staged companion differs")
    if (reference["provider_symbol"] != validate.stockanalysis_symbol(payload["provider_symbol"])
            or reference["authority_fingerprint"] != context.authority.fingerprint
            or reference["table_digest"] != context.authority.table_digest
            or reference["operation_id"] != context.operation_id):
        raise LedgerError("split guarded reference identity differs")
    logical, workers = indexes
    attempts = []
    endpoints = set()
    for source in payload["sources"].values():
        provenance = source.get("provenance")
        if provenance is None:
            continue
        if provenance.get("operation_id") != context.operation_id:
            raise LedgerError("split payload source is outside the owning operation")
        request = provenance["request_provenance"]
        endpoint = request["endpoint"]
        matches = []
        for decision, result in logical.get(provenance["logical_id"], []):
            evidence, outcome = decision["payload"], result["payload"]
            accepted = (outcome.get("accepted", {}).get("digest")
                        if endpoint == "yahoo.splits" else outcome.get("response_digest"))
            if (result["producer_id"] == request["producer"]
                    and result["producer_id"] in _SPLIT_RIGHTS
                    and outcome.get("outcome") in {"returned", "empty"}
                    and outcome.get("raw_response_digest") == source["payload_sha256"]
                    and accepted == provenance["accepted_digest"]
                    and evidence.get("http_request") == request
                    and evidence.get("authority_fingerprint") == context.authority.fingerprint
                    and evidence.get("table_digest") == context.authority.table_digest
                    and evidence.get("captured_now") == context.captured_now.isoformat()
                    and (endpoint == "sec.tickers"
                         or request["requested_symbol"] == payload["provider_symbol"])
                    and (endpoint != "sec.companyfacts"
                         or request["subject"] == f"CIK:{payload['cik']:010d}")):
                matches.append(result)
        if len(matches) != 1:
            raise LedgerError("split source has no unique durable accepted result")
        endpoints.add(endpoint)
        event = matches[0]
        attempts.append(dict(producer=event["producer_id"], logical_id=event["logical_id"],
                              attempt_id=event["attempt_id"], result_seq=event["seq"]))
    if not {"yahoo.splits", "sec.companyfacts"} <= endpoints or (
            payload["ticker"] == "FTNT" and "fortinet.history" not in endpoints):
        raise LedgerError("split required source evidence is missing")
    reference_audit = dict(audit, events=workers.get(companion["reference_worker"], [])
                            + [audit["events"][-1]])
    attempt_id = validate._reference_ledger_attempt(reference, reference_audit)
    reference_result = next(event for event in reference_audit["events"]
                            if event["event"] == "result" and event["attempt_id"] == attempt_id)
    seal = audit["events"][-1]
    verified = dict(version=1, operation_id=context.operation_id,
        authority_fingerprint=context.authority.fingerprint, table_digest=context.authority.table_digest,
        captured_now=context.captured_now.isoformat(), seal_seq=seal["seq"], seal_digest=digest_value(seal),
        manifest_sha256=companion["manifest_sha256"], old_cache_sha256=companion["old_cache_sha256"],
        cache_digest=companion["cache_digest"], sources=attempts,
        reference=dict(attempt_id=attempt_id, result_seq=reference_result["seq"],
            accepted_digest=reference["accepted_digest"], raw_digest=reference["source_bytes_sha256"]),
        series_fingerprint=companion["series_fingerprint"])
    project = Path(__file__).resolve().parents[1]
    verified["ledger_locator"] = (context.ledger.path.relative_to(project).as_posix()
        if context.ledger.path.is_relative_to(project) else context.ledger.path.name)
    verified["receipt_locator"] = str(Path(verified["ledger_locator"]).with_suffix(".seal.json"))
    payload["sealed_fetch"] = verified
    return payload


def _publish_spool(root, provider, spool, operation, rows, check, progress):
    context = operation.context
    check.check(sealed=True)
    audit = inspect_ledger(context.ledger.path)
    if audit["operation_id"] != context.operation_id:
        raise LedgerError("split source operation identity differs")
    indexes = _audit_indexes(audit)
    # Recovery must not do a new fallible receipt read after a primary write
    # fault. Bind its digest before ANY bank publication, while faults can
    # still stop with every old cache unchanged.
    seal_sha256 = _bytes_digest(_bounded_read(context.ledger.receipt_path, OPTIONS_BYTES))
    # Verify EVERY staged companion before the first bank write, one item at a time.
    for entry in spool.items:
        check.check(sealed=True)
        _verify_item(spool.load(entry), context, audit, indexes)
    verified_map = None
    if provider._pending_map is not None:
        from copy import deepcopy
        if not provider._map_value_matches(provider._pending_map, context):
            raise LedgerError("pending split map differs before publication")
        identity = provider._map_result_identity(provider._pending_map, audit)
        verified_map = deepcopy(dict(provider._pending_map, sealed_identity=identity))
    published, uncertain = [], []
    current = None
    try:
        for entry in spool.items:
            current = entry["ticker"]
            check.check(sealed=True)
            item = spool.load(entry)
            payload = _verify_item(item, context, audit, indexes)
            companion = item["companion"]
            path = cache_path(root, companion["identity"])
            _emit_progress(progress, "commit", current)
            # Recheck after the observer, including expiry/cancel and current CAS values.
            check.check(sealed=True)
            with _path_lock(path):
                identity, manifest, digest = _manifest_snapshot(root, current,
                                            companion["provider_symbol_override"])
                if (digest != companion["manifest_sha256"] or identity != companion["identity"]
                        or manifest.get("cik") not in (None, companion["resolved_cik"])
                        or _bytes_digest(_bounded_read(path, SPOOL_ITEM_BYTES, missing=True))
                            != companion["old_cache_sha256"]):
                    raise LedgerError("split publication CAS differs")
                raw = _bounded_json(payload, SPOOL_ITEM_BYTES - 1) + b"\n"
                # A failed atomic writer might have replaced the target before raising.
                uncertain.append(current)
                storage._atomic_write_bytes(path, raw)
                if _bounded_read(path, SPOOL_ITEM_BYTES) != raw:
                    raise LedgerError("split published cache verification differs")
                uncertain.remove(current)
                published.append(current)
            rows[entry["row_index"]].update(status="refreshed", preserved_last_good=False)
            _emit_progress(progress, "done", current)
            current = None
        # RAM reference entries are not another route to bank publication.
        for entry in spool.items:
            check.check(sealed=True)
            item = spool.load(entry)
            reference = item["reference"]
            view = dict(audit, events=indexes[1][item["companion"]["reference_worker"]]
                         + [audit["events"][-1]])
            reference["attempt_id"] = validate._reference_ledger_attempt(reference, view)
            reference["ledger_path"] = str(context.ledger.path)
            with validate._REF_LOCK:
                validate._REF_CACHE[(reference["provider_symbol"], reference["range"])] = reference
        if verified_map is not None:
            check.check(sealed=True)
            provider._sealed_map = verified_map
            provider._sealed_map_path = context.ledger.path
            provider._pending_map = provider._pending_map_path = None
        spool.record("published.json", dict(operation_id=context.operation_id,
            published=published, spool_bytes=spool.total_bytes, batch_atomic=False))
    except BaseException as error:
        debt = dict(kind="split_publication_debt", operation_id=context.operation_id,
            seal_sha256=seal_sha256,
            published=published, uncertain=uncertain,
            unpublished=[entry["ticker"] for entry in spool.items
                         if entry["ticker"] not in published and entry["ticker"] not in uncertain],
            current=current, fault_type=type(error).__name__, recovery_required=True)
        try:
            spool.record("publication-debt.json", debt)
        except BaseException:
            BaseException.add_note(error, "split publication debt companion could not be persisted")
        if not isinstance(error, Exception):
            raise
        if isinstance(error, (AuthorityError, LedgerError, RequestCancelled)):
            error.publication_debt = debt
            raise
        raise PublicationError(debt) from error


def _run_split(operation, root, tickers, *, provider=None, reference_fetcher=None,
               provider_symbols=None, cik_bindings=None, limit=MAX_BANK_REFRESH,
               pace_seconds=DEFAULT_PACE_SECONDS, scan_fn=None, progress=None,
               cancel=None, timeout=30, sec_user_agent=_UNSET_AGENT,
               refresh_bank=False, artifact=None, budget_seconds=ROOT_BUDGET_SECONDS):
    from fetch_http_splits import validate_sec_user_agent
    context = fops.require_root_admission(operation)
    _reference_preflight(context)
    if sec_user_agent is _UNSET_AGENT:
        # Offline tests must explicitly supply a synthetic value, never read real env.
        sec_user_agent = (None if context.test_capability is not None else
                          os.environ.get("SEC_USER_AGENT"))
    agent = validate_sec_user_agent(sec_user_agent)
    if provider is not None or reference_fetcher is not None:
        raise RequestRefused("caller split provider/reference objects are not supported")
    check = _RootCheck(operation, cancel, budget_seconds)
    check.check()
    if type(root) not in (str, type(Path("."))):
        raise RequestRefused("split root path must be a detached path")
    root = Path(root)
    if artifact is not None:
        if type(artifact) not in (str, type(Path("."))):
            raise RequestRefused("split artifact must be a detached path")
        artifact_path = Path(artifact).resolve()
        bank_path = root.resolve()
        if (artifact_path == bank_path or artifact_path.is_relative_to(bank_path)
                or artifact_path in {context.ledger.path.resolve(), context.ledger.receipt_path.resolve()}):
            raise RequestRefused("split artifact would overwrite bank or ledger evidence")
    if refresh_bank:
        tickers = _bank_names(root)
    names, symbols, ciks = _detached_options(tickers,
        {} if provider_symbols is None else provider_symbols,
        {} if cik_bindings is None else cik_bindings, limit, pace_seconds)
    from split_provider import YahooSecProvider
    owned_provider = YahooSecProvider(timeout=timeout, sec_user_agent=agent)
    if type(owned_provider) is not YahooSecProvider or owned_provider.fetcher is not None:
        raise RequestRefused("split factory did not return the engine-owned provider")
    spool = _PublicationSpool(context, root)
    rows = []
    try:
        for index, ticker in enumerate(names[:limit]):
            check.check()
            with fops.scoped_worker(operation.child("split-history-" + str(index),
                                        rights=_SPLIT_RIGHTS), close=True) as worker:
                rows.append(_refresh_ticker_body(root, ticker, owned_provider, worker,
                    operation, spool, check, index, provider_symbol=symbols.get(ticker),
                    cik=ciks.get(ticker), scan_fn=scan_fn, progress=progress))
        check.check()
        operation.seal()
        _publish_spool(root, owned_provider, spool, operation, rows, check, progress)
        result = dict(kind="split_cache_refresh", version=1, network=True, root=str(root),
            operation_id=context.operation_id, requested=len(names), processed=len(rows),
            truncated=len(names) > limit, limit=limit, pace_seconds=float(pace_seconds),
            rows=rows, failures=sum(row["status"] != "refreshed" for row in rows),
            spool_bytes=spool.total_bytes, spool_items=len(spool.items), batch_atomic=False)
        if artifact is not None:
            check.check(sealed=True)
            write_artifact(artifact, result)
        return result
    except BaseException as error:
        # Preserve the primary typed STOP even if failure bookkeeping itself fails.
        try:
            spool.record("root-stop.json", dict(operation_id=context.operation_id,
                fault_type=type(error).__name__, staged=[entry["ticker"] for entry in spool.items],
                sealed=context.ledger.verified, no_clean_success=True))
        except BaseException:
            BaseException.add_note(error, "split root failure companion could not be persisted")
        raise


def _close_split(operation):
    primary = sys.exc_info()[1]
    try:
        operation.close()
    except BaseException:
        if primary is None:
            raise
        BaseException.add_note(primary, "split operation cleanup also failed")


def refresh_ticker(root, ticker, provider=None, *, provider_symbol=None, cik=None,
                   reference_fetcher=None, scan_fn=None, progress=None, cancel=None,
                   _test_capability=None, _evidence_dir=None, _authority=None,
                   _clock=None, _governors=None, _fault=None,
                   _sec_user_agent=_UNSET_AGENT, _budget_seconds=ROOT_BUDGET_SECONDS):
    """One held root. Admission/preflight precede every caller or bank effect."""
    operation = fops.begin_operation("split_refresh", _split_directory(_evidence_dir),
        test_capability=_test_capability, authority=_authority, clock=_clock,
        governors=_governors, fault=_fault)
    try:
        fops.require_root_admission(operation)
        # Do not traverse opaque identity arguments before admission/preflight.
        if type(ticker) is not str:
            raise RequestRefused("split ticker must be a detached string")
        result = _run_split(operation, root, [ticker], provider=provider,
            reference_fetcher=reference_fetcher,
            provider_symbols={} if provider_symbol is None else {ticker: provider_symbol},
            cik_bindings={} if cik is None else {ticker: cik}, scan_fn=scan_fn,
            progress=progress, cancel=cancel, sec_user_agent=_sec_user_agent,
            budget_seconds=_budget_seconds)
        return dict(result["rows"][0], operation_id=result["operation_id"])
    finally:
        _close_split(operation)


def refresh_many(root, tickers, provider=None, *, provider_symbols=None,
                  cik_bindings=None, pace_seconds=DEFAULT_PACE_SECONDS,
                  limit=MAX_BANK_REFRESH, reference_fetcher=None, scan_fn=None,
                  sleep_fn=time.sleep, progress=None, cancel=None,
                  _test_capability=None, _evidence_dir=None, _authority=None,
                  _clock=None, _governors=None, _fault=None,
                  _sec_user_agent=_UNSET_AGENT, _budget_seconds=ROOT_BUDGET_SECONDS):
    operation = fops.begin_operation("split_refresh", _split_directory(_evidence_dir),
        test_capability=_test_capability, authority=_authority, clock=_clock,
        governors=_governors, fault=_fault)
    try:
        fops.require_root_admission(operation)
        return _run_split(operation, root, tickers, provider=provider,
            reference_fetcher=reference_fetcher, provider_symbols=provider_symbols,
            cik_bindings=cik_bindings, limit=limit, pace_seconds=pace_seconds,
            scan_fn=scan_fn, progress=progress, cancel=cancel,
            sec_user_agent=_sec_user_agent, budget_seconds=_budget_seconds)
    finally:
        _close_split(operation)


def run_refresh_cli(root, tickers=None, *, refresh_bank=False, provider=None,
                    reference_fetcher=None, provider_symbols=None, cik_bindings=None,
                    pace_seconds=DEFAULT_PACE_SECONDS, limit=MAX_BANK_REFRESH,
                    timeout=30, artifact=None, scan_fn=None, progress=None, cancel=None,
                    _test_capability=None, _evidence_dir=None, _authority=None,
                    _clock=None, _governors=None, _fault=None,
                    _sec_user_agent=_UNSET_AGENT, _budget_seconds=ROOT_BUDGET_SECONDS):
    """CLI's own root; never nests refresh_many/refresh_ticker."""
    operation = fops.begin_operation("split_refresh", _split_directory(_evidence_dir),
        test_capability=_test_capability, authority=_authority, clock=_clock,
        governors=_governors, fault=_fault)
    try:
        fops.require_root_admission(operation)
        return _run_split(operation, root, tickers, refresh_bank=refresh_bank,
            provider=provider, reference_fetcher=reference_fetcher,
            provider_symbols=provider_symbols, cik_bindings=cik_bindings,
            pace_seconds=pace_seconds, limit=limit, timeout=timeout, artifact=artifact,
            scan_fn=scan_fn, progress=progress, cancel=cancel,
            sec_user_agent=_sec_user_agent, budget_seconds=_budget_seconds)
    finally:
        _close_split(operation)


def write_artifact(path, result):
    encoded = (json.dumps(
        result, indent=2, sort_keys=True) + "\n").encode("utf-8")
    storage._atomic_write_bytes(Path(path), encoded)


def _assignments(values, label, converter=str):
    out = {}
    for raw in values or []:
        if "=" not in raw:
            raise SplitCacheError(f"{label} must use TICKER=VALUE: {raw!r}")
        ticker, value = raw.split("=", 1)
        ticker = _ticker(ticker)
        try:
            out[ticker] = converter(value)
        except (TypeError, ValueError) as exc:
            raise SplitCacheError(
                f"invalid {label} for {ticker}: {value!r}") from exc
    return out


def main(argv=None, *, provider=None, reference_fetcher=None, scan_fn=None,
         sleep_fn=time.sleep, _test_capability=None, _evidence_dir=None,
         _authority=None, _clock=None, _governors=None, _fault=None,
         _sec_user_agent=_UNSET_AGENT, _budget_seconds=ROOT_BUDGET_SECONDS):
    parser = argparse.ArgumentParser(
        description="Offline split-cache status or explicit provider refresh")
    parser.add_argument("--root", default=str(storage.storage_root(".")))
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--offline", nargs="*", metavar="TICKER")
    modes.add_argument("--refresh", nargs="+", metavar="TICKER")
    modes.add_argument("--refresh-bank", action="store_true")
    parser.add_argument("--provider-symbol", action="append", default=[])
    parser.add_argument("--cik", action="append", default=[])
    parser.add_argument("--max-age-days", type=float)
    parser.add_argument("--pace", type=float, default=DEFAULT_PACE_SECONDS)
    parser.add_argument("--limit", type=int, default=MAX_BANK_REFRESH)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--artifact")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    root = Path(args.root)
    try:
        provider_symbols = _assignments(
            args.provider_symbol, "provider symbol", str)
        cik_bindings = _assignments(args.cik, "CIK", int)
        if any(int(value) <= 0 for value in cik_bindings.values()):
            raise SplitCacheError("CIK values must be positive")
        if args.refresh is not None or args.refresh_bank:
            result = run_refresh_cli(
                root, args.refresh, refresh_bank=args.refresh_bank, provider=provider,
                provider_symbols=provider_symbols,
                cik_bindings=cik_bindings,
                pace_seconds=args.pace,
                limit=args.limit,
                reference_fetcher=reference_fetcher,
                scan_fn=scan_fn, timeout=args.timeout, artifact=args.artifact,
                _test_capability=_test_capability, _evidence_dir=_evidence_dir,
                _authority=_authority, _clock=_clock, _governors=_governors,
                _fault=_fault, _sec_user_agent=_sec_user_agent,
                _budget_seconds=_budget_seconds)
            exit_code = 1 if result["failures"] else 0
        else:
            tickers = args.offline
            if tickers is None or not tickers:
                tickers = None
            max_age = (None if args.max_age_days is None else
                       dt.timedelta(days=args.max_age_days))
            result = offline_status(
                root, tickers, provider_symbols=provider_symbols,
                max_age=max_age)
            exit_code = 0
    except Exception as exc:  # noqa: BLE001 - CLI emits structured failure
        detail = _observe(str, exc)[:500]
        result = {
            "kind": "split_cache_command_error",
            "version": 1,
            "network": bool(args.refresh is not None or args.refresh_bank),
            "root": str(root),
            "error": f"{type(exc).__name__}: {detail}",
        }
        if isinstance(exc, PublicationError):
            result["publication_debt"] = exc.debt
        elif isinstance(exc, (AuthorityError, LedgerError, RequestCancelled)):
            debt = _observe(getattr, exc, "publication_debt", None)
            if debt is not None:
                result["publication_debt"] = debt
        exit_code = 1
    # A refused/failed refresh never manufactures an artifact outside its root.
    if args.artifact and args.refresh is None and not args.refresh_bank:
        write_artifact(args.artifact, result)
    print(json.dumps(result, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
