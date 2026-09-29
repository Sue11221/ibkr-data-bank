"""Identity-safe split-history cache and explicit refresh CLI.

Normal operation is offline. Network behavior is reachable only through an
explicit refresh mode and an injected provider. Cache files live under the
reserved ``_split_history`` storage-root namespace; no ticker manifest or bar
file is modified.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import threading
import time
from pathlib import Path

import deep_seam_scan as seam_scan
import split_detector as detector
import stock_storage as storage
import stock_validate as validate


CACHE_DIR_NAME = "_split_history"
CACHE_KIND = "split_history_cache"
CACHE_VERSION = 1
MAX_BANK_REFRESH = 1000
DEFAULT_PACE_SECONDS = 0.25

_LOCKS_GUARD = threading.Lock()
_PATH_LOCKS = {}


class SplitCacheError(RuntimeError):
    """Base cache/refresh error."""


class CacheValidationError(SplitCacheError):
    """Cache bytes do not satisfy the schema contract."""


class CacheIdentityError(SplitCacheError):
    """Cache identity does not match the active manifest identity."""


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
    if cancel is None:
        return False
    if callable(cancel):
        return bool(cancel())
    is_set = getattr(cancel, "is_set", None)
    return bool(is_set()) if callable(is_set) else bool(cancel)


def _emit_progress(progress, stage, ticker):
    if progress is None:
        return
    try:
        progress(str(stage), str(ticker))
    except Exception:  # noqa: BLE001 - status callbacks cannot break refresh
        pass


def refresh_ticker(root, ticker, provider, *, provider_symbol=None, cik=None,
                   reference_fetcher=None, scan_fn=None, progress=None,
                   cancel=None):
    """Refresh one cache; failures/cancellation before commit preserve bytes."""
    root = Path(root)
    if _cancel_requested(cancel):
        return {
            "ticker": str(ticker).strip().upper(),
            "status": "cancelled",
            "stage": "before_identity",
            "preserved_last_good": False,
        }
    _emit_progress(progress, "identity", ticker)
    try:
        identity = manifest_identity(
            root, ticker, provider_symbol=provider_symbol)
    except SplitCacheError as exc:
        return {
            "ticker": str(ticker).strip().upper(),
            "status": "error",
            "preserved_last_good": False,
            "error": str(exc),
        }
    path = cache_path(root, identity)
    before = path.read_bytes() if path.is_file() else None
    prior = load_cache(root, identity)

    def cancelled(stage):
        unchanged = (
            (before is None and not path.exists())
            or (before is not None and path.is_file()
                and path.read_bytes() == before))
        _emit_progress(progress, "cancelled", identity["ticker"])
        return {
            "ticker": identity["ticker"],
            "status": "cancelled",
            "stage": stage,
            "path": str(path),
            "preserved_last_good": bool(prior.get("usable") and unchanged),
        }

    if _cancel_requested(cancel):
        return cancelled("after_identity")
    if identity["conid"] is None:
        return {
            "ticker": identity["ticker"],
            "status": "error",
            "preserved_last_good": prior.get("status") == "ok",
            "error": "manifest conId is missing; refusing symbol-only refresh",
        }
    try:
        _emit_progress(progress, "provider_history", identity["ticker"])
        provider_result = provider.fetch_history(
            identity["ticker"], identity["provider_symbol"], cik=cik)
        if _cancel_requested(cancel):
            return cancelled("after_provider_history")
        payload = build_cache_payload(identity, provider_result)
        reference_fetcher = reference_fetcher or validate.fetch_daily_reference
        scan_fn = scan_fn or seam_scan.scan_ticker_steps
        _emit_progress(progress, "daily_reference", identity["ticker"])
        reference = reference_fetcher(
            identity["provider_symbol"], rng="Max")
        if _cancel_requested(cancel):
            return cancelled("after_daily_reference")
        if not isinstance(reference, dict):
            raise CacheValidationError("daily reference is not a mapping")
        _emit_progress(progress, "candidate_scan", identity["ticker"])
        scan = scan_fn(root, identity["ticker"], ref=reference)
        payload["detection"] = _detection_from_scan(
            identity, scan, payload["fetched_at"])
        if _cancel_requested(cancel):
            return cancelled("after_candidate_scan")
        _emit_progress(progress, "commit", identity["ticker"])
        committed_path, committed = write_cache(root, identity, payload)
    except Exception as exc:  # noqa: BLE001 - result records provider/cache error
        unchanged = (
            before is not None and path.is_file() and path.read_bytes() == before)
        return {
            "ticker": identity["ticker"],
            "status": "error",
            "path": str(path),
            "preserved_last_good": bool(prior.get("usable") and unchanged),
            "error": f"{type(exc).__name__}: {exc}",
        }
    _emit_progress(progress, "done", identity["ticker"])
    return {
        "ticker": identity["ticker"],
        "status": "refreshed",
        "path": str(committed_path),
        "conid": identity["conid"],
        "provider_symbol": identity["provider_symbol"],
        "cik": committed.get("cik"),
        "event_count": len(committed["events"]),
        "candidate_count": len(committed["detection"]["candidates"]),
        "series_fingerprint": committed["detection"]["series_fingerprint"],
        "coverage_complete": committed["coverage"]["complete"],
        "preserved_last_good": False,
    }


def refresh_many(root, tickers, provider, *, provider_symbols=None,
                  cik_bindings=None, pace_seconds=DEFAULT_PACE_SECONDS,
                  limit=MAX_BANK_REFRESH, reference_fetcher=None, scan_fn=None,
                  sleep_fn=time.sleep):
    provider_symbols = {
        _ticker(key): value for key, value in dict(provider_symbols or {}).items()
    }
    cik_bindings = {
        _ticker(key): value for key, value in dict(cik_bindings or {}).items()
    }
    limit = int(limit)
    if limit <= 0 or limit > MAX_BANK_REFRESH:
        raise SplitCacheError(
            f"refresh limit must be between 1 and {MAX_BANK_REFRESH}")
    requested_names = sorted({_ticker(name) for name in list(tickers)})
    names = requested_names[:limit]
    rows = []
    for index, ticker in enumerate(names):
        if index and pace_seconds:
            sleep_fn(max(0.0, float(pace_seconds)))
        rows.append(refresh_ticker(
            root, ticker, provider,
            provider_symbol=provider_symbols.get(ticker),
            cik=cik_bindings.get(ticker),
            reference_fetcher=reference_fetcher,
            scan_fn=scan_fn))
    return {
        "kind": "split_cache_refresh",
        "version": 1,
        "network": True,
        "root": str(Path(root)),
        "requested": len(requested_names),
        "processed": len(rows),
        "truncated": len(requested_names) > len(names),
        "limit": limit,
        "pace_seconds": float(pace_seconds),
        "rows": rows,
        "failures": sum(row["status"] != "refreshed" for row in rows),
    }


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
         sleep_fn=time.sleep):
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
            tickers = (list_manifest_tickers(root) if args.refresh_bank
                       else args.refresh)
            if provider is None:
                from split_provider import YahooSecProvider
                provider = YahooSecProvider(
                    timeout=args.timeout, cik_overrides=cik_bindings)
            result = refresh_many(
                root, tickers, provider,
                provider_symbols=provider_symbols,
                cik_bindings=cik_bindings,
                pace_seconds=args.pace,
                limit=args.limit,
                reference_fetcher=reference_fetcher,
                scan_fn=scan_fn,
                sleep_fn=sleep_fn)
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
        result = {
            "kind": "split_cache_command_error",
            "version": 1,
            "network": bool(args.refresh is not None or args.refresh_bank),
            "root": str(root),
            "error": f"{type(exc).__name__}: {exc}",
        }
        exit_code = 1
    if args.artifact:
        write_artifact(args.artifact, result)
    print(json.dumps(result, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
