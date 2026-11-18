"""Month-granular intraday-to-daily cache outside the storage bank.

The bank manifest and month files remain authoritative. Cache hits require an
exact manifest identity plus a matching current file stat; every miss reads the
source through stock_storage's SHA-gated strict fallback. Cache failures never
prevent source-derived output.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
from copy import deepcopy
from datetime import date, datetime, time
from pathlib import Path

import stock_storage as ss


CACHE_KIND = "derived_daily_cache"
CACHE_VERSION = 1
AGGREGATION_VERSION = 1
RTH_WINDOW_VERSION = 1
DEFAULT_CACHE_DIR_NAME = "_derived_daily_cache"
MAX_CACHE_BYTES = 32 * (1 << 20)
MAX_MONTHS = 1200
MAX_DAYS_PER_MONTH = 31
MAX_ROWS = 50_000_000
MAX_FILE_BYTES = 1 << 40
MAX_MTIME_NS = (1 << 63) - 1
MAX_DAY_BARS = 10_000_000
MAX_VOLUME = (1 << 63) - 1

_MONTH_RE = re.compile(r"^[12]\d{3}-(?:0[1-9]|1[0-2])$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_ROOT_KEYS = frozenset({
    "kind", "version", "aggregation_version", "rth_window_version",
    "bank_digest", "ticker", "interval", "months",
})
_MONTH_KEYS = frozenset({
    "aggregation_version", "rth_window_version", "source", "records",
    "records_sha256",
})
_SOURCE_KEYS = frozenset({
    "month", "status", "sha256", "rows", "size", "mtime_ns", "first",
    "last",
})
_SOURCE_FIELDS = ("status", "sha256", "rows", "size", "mtime_ns", "first", "last")

_LOCKS_GUARD = threading.Lock()
_PATH_LOCKS = {}


class DerivedDailyCacheError(RuntimeError):
    """The cache request or source identity is invalid."""


class CacheValidationError(DerivedDailyCacheError):
    """A cache document failed its bounded schema or identity contract."""


class SourceNotCurrentError(DerivedDailyCacheError):
    """Source-derived rows did not remain current for the captured manifest."""


def _read_manifest(root, ticker):
    return ss.load_manifest(Path(root) / ticker)


def _read_source_month(path, manifest_sha):
    return ss.read_month_file_fast(path, manifest_sha)


def _atomic_write_cache(path, payload):
    ss._atomic_write_bytes(path, payload)


def _stat_path(path):
    return Path(path).stat()


def _path_lock(path):
    key = os.path.normcase(str(Path(path).resolve()))
    with _LOCKS_GUARD:
        return _PATH_LOCKS.setdefault(key, threading.Lock())


def _is_within(path, parent):
    try:
        Path(path).relative_to(parent)
        return True
    except ValueError:
        return False


def _namespace_digest(root):
    normalized = os.path.normcase(str(Path(root).resolve()))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def default_cache_root(root):
    return Path(root).resolve().parent / DEFAULT_CACHE_DIR_NAME


def _validate_interval(interval):
    token = str(interval or "").strip()
    if not ss.INTERVAL_RE.fullmatch(token):
        raise DerivedDailyCacheError(f"invalid interval token: {interval!r}")
    base, kind, session = ss.parse_interval(token)
    if kind or session != "rth" or not base or base[-1] not in "smh":
        raise DerivedDailyCacheError(
            f"unsupported derived-daily interval: {token!r}")
    return token


def _identity(root, ticker, interval, cache_root):
    bank = Path(root).resolve()
    canon = ss.canonical_ticker(ticker)
    token = _validate_interval(interval)
    target_root = (Path(cache_root).resolve() if cache_root is not None
                   else default_cache_root(bank).resolve())
    if _is_within(target_root, bank):
        raise DerivedDailyCacheError("cache root cannot be inside the storage bank")
    digest = _namespace_digest(bank)
    target = target_root / digest[:16] / canon / f"{token}.json"
    if not _is_within(target.resolve(), target_root):
        raise DerivedDailyCacheError("cache path escapes its configured root")
    return bank, canon, token, target_root, digest, target


def cache_path(root, ticker, interval, cache_root=None):
    return _identity(root, ticker, interval, cache_root)[-1]


def _strict_int(value, label, low, high):
    if isinstance(value, bool) or not isinstance(value, int):
        raise CacheValidationError(f"{label} is not an integer")
    if not low <= value <= high:
        raise CacheValidationError(f"{label} is out of range")
    return value


def _strict_float(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CacheValidationError(f"{label} is not numeric")
    result = float(value)
    if not math.isfinite(result):
        raise CacheValidationError(f"{label} is not finite")
    return result


def _manifest_timestamp(value, label):
    if not isinstance(value, str) or len(value) > 40:
        raise CacheValidationError(f"invalid {label}")
    try:
        day_text, clock_text = value.split(" ", 1)
        month, day, year = (int(part) for part in day_text.split("/"))
        hour, minute, second = (int(part) for part in clock_text.split(":"))
        parsed = datetime(year, month, day, hour, minute, second)
    except (TypeError, ValueError):
        raise CacheValidationError(f"invalid {label}") from None
    canonical = f"{ss.format_date(parsed)} {ss.format_time(parsed.time())}"
    if canonical != value:
        raise CacheValidationError(f"non-canonical {label}")
    return parsed


def _month_parts(month):
    if not isinstance(month, str) or not _MONTH_RE.fullmatch(month):
        raise CacheValidationError(f"invalid month key: {month!r}")
    return int(month[:4]), int(month[5:7])


def _source_from_manifest(month, entry):
    return {"month": month, **{key: entry.get(key) for key in _SOURCE_FIELDS}}


def _validate_source(source, month):
    if not isinstance(source, dict) or set(source) != _SOURCE_KEYS:
        raise CacheValidationError(f"{month}: invalid source metadata shape")
    if source.get("month") != month:
        raise CacheValidationError(f"{month}: source month mismatch")
    if source.get("status") != "present":
        raise CacheValidationError(f"{month}: source is not present")
    sha = source.get("sha256")
    if not isinstance(sha, str) or not _SHA_RE.fullmatch(sha):
        raise CacheValidationError(f"{month}: invalid source sha256")
    _strict_int(source.get("rows"), f"{month} rows", 1, MAX_ROWS)
    _strict_int(source.get("size"), f"{month} size", 1, MAX_FILE_BYTES)
    _strict_int(source.get("mtime_ns"), f"{month} mtime_ns", 1, MAX_MTIME_NS)
    first = _manifest_timestamp(source.get("first"), f"{month} first")
    last = _manifest_timestamp(source.get("last"), f"{month} last")
    year, number = _month_parts(month)
    if first > last or (first.year, first.month) != (year, number) \
            or (last.year, last.month) != (year, number):
        raise CacheValidationError(f"{month}: source bounds do not match month")
    return dict(source)


def _canonical_json(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CacheValidationError(f"cache JSON is not canonicalizable: {exc}") from exc


def _records_digest(records):
    return hashlib.sha256(_canonical_json(records)).hexdigest()


def _validate_records(records, month):
    if not isinstance(records, list) or len(records) > MAX_DAYS_PER_MONTH:
        raise CacheValidationError(f"{month}: invalid daily record count")
    normalized = []
    previous = None
    year, number = _month_parts(month)
    for index, raw in enumerate(records):
        if not isinstance(raw, list) or len(raw) != 7:
            raise CacheValidationError(f"{month}: invalid record {index}")
        day_text = raw[0]
        if not isinstance(day_text, str):
            raise CacheValidationError(f"{month}: invalid date at record {index}")
        try:
            parsed = date.fromisoformat(day_text)
        except ValueError:
            raise CacheValidationError(
                f"{month}: invalid date at record {index}") from None
        if parsed.isoformat() != day_text or (parsed.year, parsed.month) != (year, number):
            raise CacheValidationError(f"{month}: date outside source month")
        if previous is not None and day_text <= previous:
            raise CacheValidationError(f"{month}: daily dates are not unique/sorted")
        previous = day_text
        count = _strict_int(raw[1], f"{month} bar count", 1, MAX_DAY_BARS)
        o, h, lo, c = (_strict_float(raw[i], f"{month} OHLC") for i in range(2, 6))
        volume = _strict_int(raw[6], f"{month} volume", 0, MAX_VOLUME)
        if min(o, h, lo, c) < 0 or h < max(o, c, lo) or lo > min(o, c, h):
            raise CacheValidationError(f"{month}: invalid OHLC relationship")
        normalized.append([day_text, count, o, h, lo, c, volume])
    return normalized


def _validate_month_entry(entry, month):
    if not isinstance(entry, dict) or set(entry) != _MONTH_KEYS:
        raise CacheValidationError(f"{month}: invalid cache month shape")
    if entry.get("aggregation_version") != AGGREGATION_VERSION \
            or entry.get("rth_window_version") != RTH_WINDOW_VERSION:
        raise CacheValidationError(f"{month}: unsupported aggregation schema")
    source = _validate_source(entry.get("source"), month)
    records = _validate_records(entry.get("records"), month)
    digest = entry.get("records_sha256")
    if not isinstance(digest, str) or not _SHA_RE.fullmatch(digest) \
            or digest != _records_digest(records):
        raise CacheValidationError(f"{month}: daily record digest mismatch")
    return {
        "aggregation_version": AGGREGATION_VERSION,
        "rth_window_version": RTH_WINDOW_VERSION,
        "source": source,
        "records": records,
        "records_sha256": digest,
    }


def validate_cache_document(document, *, bank_digest, ticker, interval):
    if not isinstance(document, dict) or set(document) != _ROOT_KEYS:
        raise CacheValidationError("invalid cache document shape")
    if document.get("kind") != CACHE_KIND or document.get("version") != CACHE_VERSION:
        raise CacheValidationError("unsupported cache document")
    if document.get("aggregation_version") != AGGREGATION_VERSION \
            or document.get("rth_window_version") != RTH_WINDOW_VERSION:
        raise CacheValidationError("unsupported cache aggregation version")
    if document.get("bank_digest") != bank_digest:
        raise CacheValidationError("cache bank identity mismatch")
    if document.get("ticker") != ticker or document.get("interval") != interval:
        raise CacheValidationError("cache series identity mismatch")
    months = document.get("months")
    if not isinstance(months, dict) or len(months) > MAX_MONTHS:
        raise CacheValidationError("invalid cache month map")
    normalized = {}
    for month in sorted(months):
        _month_parts(month)
        normalized[month] = _validate_month_entry(months[month], month)
    return {
        "kind": CACHE_KIND,
        "version": CACHE_VERSION,
        "aggregation_version": AGGREGATION_VERSION,
        "rth_window_version": RTH_WINDOW_VERSION,
        "bank_digest": bank_digest,
        "ticker": ticker,
        "interval": interval,
        "months": normalized,
    }


def _strict_json(raw):
    def reject_constant(value):
        raise CacheValidationError(f"invalid JSON constant: {value}")

    def reject_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise CacheValidationError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        return json.loads(raw.decode("utf-8"), parse_constant=reject_constant,
                          object_pairs_hook=reject_duplicates)
    except CacheValidationError:
        raise
    except (UnicodeDecodeError, ValueError) as exc:
        raise CacheValidationError(f"malformed cache JSON: {exc}") from exc


def _load_document(path, bank_digest, ticker, interval):
    path = Path(path)
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_CACHE_BYTES + 1)
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise CacheValidationError(f"cannot read cache: {exc}") from exc
    if len(raw) > MAX_CACHE_BYTES:
        raise CacheValidationError("cache document exceeds size bound")
    return validate_cache_document(
        _strict_json(raw), bank_digest=bank_digest, ticker=ticker,
        interval=interval)


def _try_load_document(path, bank_digest, ticker, interval):
    try:
        return _load_document(path, bank_digest, ticker, interval), 0
    except FileNotFoundError:
        return None, 0
    except CacheValidationError:
        return None, 1


def _manifest_snapshot(root, ticker, interval):
    manifest = _read_manifest(root, ticker)
    if not isinstance(manifest, dict):
        raise DerivedDailyCacheError(f"manifest unavailable for {ticker}")
    section = (manifest.get("intervals") or {}).get(interval)
    months = section.get("months") if isinstance(section, dict) else None
    if not isinstance(months, dict):
        raise DerivedDailyCacheError(
            f"manifest interval unavailable for {ticker} {interval}")
    current = {}
    for month, entry in months.items():
        _month_parts(month)
        if not isinstance(entry, dict):
            raise DerivedDailyCacheError(f"invalid manifest entry for {month}")
        if entry.get("status") != "MISSING":
            current[month] = deepcopy(entry)
    if len(current) > MAX_MONTHS:
        raise DerivedDailyCacheError("manifest month count exceeds cache bound")
    return current


def _month_path(root, ticker, interval, month):
    year, number = _month_parts(month)
    path = (ss.find_month_file(root, ticker, year, number, interval)
            or ss.month_file_path(root, ticker, year, number, interval))
    resolved = Path(path).resolve()
    if not _is_within(resolved, (Path(root) / ticker).resolve()):
        raise DerivedDailyCacheError(f"source path escapes series: {month}")
    return resolved


def _source_matches_manifest(source, entry):
    return (isinstance(entry, dict) and entry.get("status") != "MISSING"
            and all(entry.get(field) == source.get(field)
                    for field in _SOURCE_FIELDS))


def _entry_is_warm(root, ticker, interval, month, entry, manifest_entry):
    if not isinstance(entry, dict) or not _source_matches_manifest(
            entry.get("source") or {}, manifest_entry):
        return False
    source = entry["source"]
    try:
        path = _month_path(root, ticker, interval, month)
        stat = _stat_path(path)
    except (OSError, DerivedDailyCacheError):
        return False
    return (stat.st_size == source["size"]
            and stat.st_mtime_ns == source["mtime_ns"])


def _actual_bounds(bars):
    if not bars:
        return None, None
    ordered = sorted(bars, key=lambda item: item[0])
    first = ordered[0][0]
    last = ordered[-1][0]
    return (f"{ss.format_date(first)} {ss.format_time(first.time())}",
            f"{ss.format_date(last)} {ss.format_time(last.time())}")


def _post_read_state(root, ticker, interval, month, captured, path, bars, stats):
    first, last = _actual_bounds(bars)
    source_current = (captured.get("status") == "present"
                      and captured.get("sha256") == stats.get("sha256")
                      and captured.get("rows") == stats.get("rows") == len(bars)
                      and captured.get("first") == first
                      and captured.get("last") == last)
    try:
        fresh = _manifest_snapshot(root, ticker, interval).get(month)
        current_path = _month_path(root, ticker, interval, month)
        post_stat = _stat_path(current_path)
        source_current = (source_current and fresh == captured
                          and current_path == path
                          and post_stat.st_size == stats.get("size")
                          and post_stat.st_mtime_ns == stats.get("mtime_ns"))
    except (OSError, DerivedDailyCacheError):
        source_current = False
    cache_source = None
    if source_current:
        candidate = _source_from_manifest(month, captured)
        if (captured.get("size") == stats.get("size")
                and captured.get("mtime_ns") == stats.get("mtime_ns")):
            try:
                cache_source = _validate_source(candidate, month)
            except CacheValidationError:
                cache_source = None
    return bool(source_current), cache_source


def _aggregate_month(bars, month):
    grouped = {}
    first_label, last_label = ss.SESSION_WINDOWS["rth"]
    for bar in sorted(bars, key=lambda item: item[0]):
        stamp = bar[0]
        if not isinstance(stamp, datetime) or not first_label <= stamp.time() <= last_label:
            continue
        values = grouped.get(stamp.date())
        if values is None:
            grouped[stamp.date()] = [1, float(bar[1]), float(bar[2]),
                                     float(bar[3]), float(bar[4]), int(bar[5])]
        else:
            values[0] += 1
            values[2] = max(values[2], float(bar[2]))
            values[3] = min(values[3], float(bar[3]))
            values[4] = float(bar[4])
            values[5] += int(bar[5])
    records = [[day.isoformat(), *values] for day, values in sorted(grouped.items())]
    return _validate_records(records, month)


def _cache_entry(source, records):
    return {
        "aggregation_version": AGGREGATION_VERSION,
        "rth_window_version": RTH_WINDOW_VERSION,
        "source": source,
        "records": records,
        "records_sha256": _records_digest(records),
    }


def _document(bank_digest, ticker, interval, months):
    return {
        "kind": CACHE_KIND,
        "version": CACHE_VERSION,
        "aggregation_version": AGGREGATION_VERSION,
        "rth_window_version": RTH_WINDOW_VERSION,
        "bank_digest": bank_digest,
        "ticker": ticker,
        "interval": interval,
        "months": {month: months[month] for month in sorted(months)},
    }


def _persist(path, root, ticker, interval, bank_digest, safe_entries):
    attempted = False
    try:
        with _path_lock(path):
            latest, _invalid = _try_load_document(
                path, bank_digest, ticker, interval)
            candidates = dict((latest or {}).get("months") or {})
            candidates.update(safe_entries)
            fresh = _manifest_snapshot(root, ticker, interval)
            retained = {
                month: entry for month, entry in candidates.items()
                if month in fresh and _entry_is_warm(
                    root, ticker, interval, month, entry, fresh[month])
            }
            wanted = _document(bank_digest, ticker, interval, retained)
            if latest != wanted:
                attempted = True
                payload = _canonical_json(wanted)
                if len(payload) > MAX_CACHE_BYTES:
                    raise CacheValidationError("cache document exceeds size bound")
                _atomic_write_cache(path, payload)
                loaded = _load_document(path, bank_digest, ticker, interval)
                if loaded != wanted:
                    raise CacheValidationError("cache readback mismatch")
            else:
                loaded = latest
            verify_manifest = _manifest_snapshot(root, ticker, interval)
            if any(month not in verify_manifest or not _entry_is_warm(
                    root, ticker, interval, month, entry,
                    verify_manifest[month])
                    for month, entry in (loaded or {}).get("months", {}).items()):
                raise CacheValidationError("source changed during cache publication")
        return attempted, True
    except (OSError, DerivedDailyCacheError, CacheValidationError):
        return attempted, False


def _records_to_daily(entries, min_bars):
    daily = {}
    for month in sorted(entries):
        for record in entries[month]["records"]:
            if record[1] < min_bars:
                continue
            day = date.fromisoformat(record[0])
            if day in daily:
                raise CacheValidationError(f"duplicate daily record: {day}")
            daily[day] = (record[2], record[3], record[4], record[5], record[6])
    return daily


def derive_series(root, ticker, interval, min_bars=0, cache_root=None):
    """Return daily OHLCV plus bounded cache/source diagnostics.

    Only regular-session TRADES sub-daily tokens are supported in version 1.
    Cache parse/write failures are contained; source read failures retain the
    legacy behavior and propagate.
    """
    if isinstance(min_bars, bool) or not isinstance(min_bars, int) \
            or not 0 <= min_bars <= MAX_DAY_BARS:
        raise DerivedDailyCacheError("min_bars must be a bounded non-negative integer")
    bank, canon, token, _cache_root, bank_digest, path = _identity(
        root, ticker, interval, cache_root)
    captured = _manifest_snapshot(bank, canon, token)
    cached, invalid_count = _try_load_document(
        path, bank_digest, canon, token)
    cached_months = dict((cached or {}).get("months") or {})
    usable_entries = {}
    safe_entries = {}
    hits = misses = source_months = source_rows = 0
    month_current = {}
    month_cacheable = {}
    observed_files = {}

    for month in sorted(captured):
        manifest_entry = captured[month]
        cached_entry = cached_months.get(month)
        if cached_entry is not None and _entry_is_warm(
                bank, canon, token, month, cached_entry, manifest_entry):
            hits += 1
            usable_entries[month] = cached_entry
            safe_entries[month] = cached_entry
            month_current[month] = True
            month_cacheable[month] = True
            observed_files[month] = {
                "path": _month_path(bank, canon, token, month),
                "size": cached_entry["source"]["size"],
                "mtime_ns": cached_entry["source"]["mtime_ns"],
            }
            continue

        misses += 1
        source_months += 1
        source_path = _month_path(bank, canon, token, month)
        bars, stats = _read_source_month(
            source_path, manifest_entry.get("sha256"))
        source_rows += len(bars)
        records = _aggregate_month(bars, month)
        current, cache_source = _post_read_state(
            bank, canon, token, month, manifest_entry,
            source_path, bars, stats)
        usable_entries[month] = _cache_entry(
            cache_source or _source_from_manifest(month, manifest_entry), records)
        month_current[month] = current
        month_cacheable[month] = cache_source is not None
        observed_files[month] = {
            "path": source_path,
            "size": stats.get("size"),
            "mtime_ns": stats.get("mtime_ns"),
        }
        if cache_source is not None:
            safe_entries[month] = _cache_entry(cache_source, records)

    try:
        final_manifest = _manifest_snapshot(bank, canon, token)
    except DerivedDailyCacheError:
        final_manifest = {}
    for month in captured:
        observed = observed_files.get(month) or {}
        try:
            final_path = _month_path(bank, canon, token, month)
            final_stat = _stat_path(final_path)
            still_current = (
                final_manifest.get(month) == captured[month]
                and final_path == observed.get("path")
                and final_stat.st_size == observed.get("size")
                and final_stat.st_mtime_ns == observed.get("mtime_ns"))
        except (OSError, DerivedDailyCacheError):
            still_current = False
        month_current[month] = bool(
            month_current.get(month) and still_current)
        month_cacheable[month] = bool(
            month_cacheable.get(month) and month_current[month])
        if not month_cacheable[month]:
            safe_entries.pop(month, None)

    needs_persist = (invalid_count > 0 or misses > 0
                     or set(cached_months) != set(safe_entries))
    persistence_attempted = persistence_succeeded = False
    if needs_persist:
        persistence_attempted, persistence_succeeded = _persist(
            path, bank, canon, token, bank_digest, safe_entries)

    daily = _records_to_daily(usable_entries, min_bars)
    try:
        cache_bytes = _stat_path(path).st_size
    except OSError:
        cache_bytes = 0
    stats = {
        "current_months": len(captured),
        "cache_hits": hits,
        "cache_misses": misses,
        "source_months_decoded": source_months,
        "source_rows_decoded": source_rows,
        "days_returned": len(daily),
        "invalid_cache_entries": invalid_count,
        "persistence_attempted": persistence_attempted,
        "persistence_succeeded": persistence_succeeded,
        "source_current": all(month_current.values()),
        "cacheable": all(month_cacheable.values()),
        "cache_bytes": int(cache_bytes),
        "cache_path": str(path),
    }
    return daily, stats
