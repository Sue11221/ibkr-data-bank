"""WS7 full-range stored-versus-external close sweep.

The engine is report-only. It reads active 1d series and recorded price-basis
actions, fetches only the allowlisted StockAnalysis daily reference, and writes
only an outside-bank resume cache plus one caller-selected Run Logs artifact.
It never imports IBKR code and cannot record an action or mutate bank data.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import os
import re
import statistics as st
import tempfile
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError
from pathlib import Path

import operation_gate
import fetch_operations as fops
from fetch_authority import AuthorityError, ResponseQuarantined
from fetch_ledger import LedgerError
from fetch_run_context import RequestCancelled, RequestRefused
import stock_basis as basis
import stock_storage as storage
import stock_validate as validate


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STORAGE_ROOT = PROJECT_ROOT / storage.STORAGE_DIR_NAME
RUN_LOGS_ROOT = PROJECT_ROOT / "Run Logs"
CACHE_ROOT = RUN_LOGS_ROOT / "_external_sweep_cache"

CACHE_KIND = "external_sweep_reference_cache"
CACHE_VERSION = 1
REPORT_KIND = "external_full_range_sweep"
REPORT_VERSION = 2
PROVIDER = "stockanalysis"
REFERENCE_HOST = "stockanalysis.com"
REFERENCE_RANGE = "Max"
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_REFERENCE_BYTES = 8 * 1024 * 1024
MAX_REFERENCE_ROWS = 20_000
MAX_TICKERS = 1_000
MIN_REQUEST_INTERVAL = 1.2
DEFAULT_ATTEMPTS = 3
DEFAULT_BACKOFF = 1.2

CLEAN_TOL = 0.02
STEP_TOL = 0.05
DRIFT_CV = 0.05
DRIFT_MIN_MONTHS = 6
CURRENT_MONTHS = 6
MIN_MONTHS = 12
SPLIT_MIN_MAGNITUDE = 1.25
SPLIT_MAX_DENOMINATOR = 10
SPLIT_RATIO_TOL = 0.03
MAX_OVERLAP_AGE_MONTHS = 3
MAX_BASIS_ACTIONS = 32

VERDICTS = (
    "UNVERIFIABLE",
    "CURRENT_MISMATCH",
    "DRIFT_ANOMALY",
    "SEAM_CANDIDATE",
    "HISTORIC_BASIS_OFFSET",
    "CLEAN",
)

_OWNER_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}\Z")
_PROVIDER_SYMBOL_RE = re.compile(r"[A-Z0-9][A-Z0-9.\-/]{0,39}\Z")


class ExternalSweepError(RuntimeError):
    """The sweep contract cannot safely continue."""


class EvidenceError(ExternalSweepError):
    """Stored or cached evidence is missing, malformed, or changed."""


class ProviderError(ExternalSweepError):
    """The external reference failed validation."""


class SpoolError(ExternalSweepError):
    """A detached batch candidate could not be safely staged."""


class PublicationRecoveryDebt(ExternalSweepError):
    """The operation sealed, but detached publication needs recovery."""

    def __init__(self, items, report=None):
        self.items = list(items)
        self.report = report
        super().__init__(f"post-seal publication recovery debt: {len(self.items)} item(s)")


def _sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def _valid_sha(value):
    text = str(value or "").strip().lower()
    return (len(text) == 64
            and all(ch in "0123456789abcdef" for ch in text))


def _timestamp(value, label="timestamp"):
    if value is None:
        parsed = dt.datetime.now(dt.timezone.utc)
    elif isinstance(value, dt.datetime):
        parsed = value
    else:
        try:
            parsed = dt.datetime.fromisoformat(
                str(value).strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise EvidenceError(f"invalid {label}") from exc
    if parsed.tzinfo is None:
        raise EvidenceError(f"{label} must include a timezone")
    return parsed.astimezone(dt.timezone.utc).isoformat(timespec="seconds")


def _ticker(value):
    try:
        return storage.canonical_ticker(value)
    except Exception as exc:  # noqa: BLE001 - normalize storage exception
        raise EvidenceError(f"invalid ticker: {value!r}") from exc


def _conid(value):
    if isinstance(value, bool):
        raise EvidenceError("manifest conId is invalid")
    try:
        conid = int(value)
    except (TypeError, ValueError) as exc:
        raise EvidenceError("manifest conId is missing or invalid") from exc
    if conid <= 0:
        raise EvidenceError("manifest conId is missing or invalid")
    return conid


def _provider_symbol(value):
    symbol = str(value or "").strip().upper()
    if not _PROVIDER_SYMBOL_RE.fullmatch(symbol):
        raise EvidenceError(f"invalid provider symbol: {value!r}")
    return symbol


def _provider_identity_symbol(value):
    symbol = _provider_symbol(value)
    return _provider_symbol(validate.stockanalysis_symbol(symbol))


def _direct_child(root, basename, suffix):
    name = str(basename or "")
    if (not name or name != Path(name).name or not name.endswith(suffix)
            or "/" in name or "\\" in name):
        raise EvidenceError(f"unsafe evidence basename: {basename!r}")
    resolved_root = Path(root).resolve()
    path = resolved_root / name
    try:
        if path.resolve().parent != resolved_root:
            raise EvidenceError(f"evidence escapes fixed root: {name}")
    except OSError as exc:
        raise EvidenceError(f"cannot resolve evidence path: {name}") from exc
    return path


def _outside_bank(path, bank_root):
    candidate = Path(path).resolve()
    bank = Path(bank_root).resolve()
    if candidate == bank or bank in candidate.parents:
        raise EvidenceError("cache/artifact path must stay outside the bank")
    return candidate


def _read_stable_bytes(path, max_bytes):
    path = Path(path)
    try:
        before = path.stat()
        if before.st_size <= 0 or before.st_size > max_bytes:
            raise EvidenceError(f"{path.name} has an invalid size")
        raw = path.read_bytes()
        after = path.stat()
    except OSError as exc:
        raise EvidenceError(f"cannot read {path.name}: {type(exc).__name__}") from exc
    if (len(raw) != before.st_size or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns):
        raise EvidenceError(f"{path.name} changed while reading")
    return raw


def _manifest_record(root, ticker):
    root = Path(root).resolve()
    ticker = _ticker(ticker)
    ticker_dir = root / ticker
    try:
        if ticker_dir.resolve().parent != root:
            raise EvidenceError(f"ticker path escapes bank: {ticker}")
    except OSError as exc:
        raise EvidenceError(f"cannot resolve ticker path: {ticker}") from exc
    manifest_path = ticker_dir / storage.MANIFEST_NAME
    raw = _read_stable_bytes(manifest_path, MAX_MANIFEST_BYTES)
    try:
        manifest = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise EvidenceError(f"manifest is invalid for {ticker}") from exc
    if not isinstance(manifest, dict):
        raise EvidenceError(f"manifest is not an object for {ticker}")
    folder = _ticker(manifest.get("folder") or ticker)
    if folder != ticker:
        raise EvidenceError(
            f"manifest folder {folder} does not match {ticker}")
    symbol = _provider_identity_symbol(
        manifest.get("provider_symbol") or manifest.get("symbol") or ticker)
    return {
        "ticker": ticker,
        "conid": _conid(manifest.get("conid")),
        "provider_symbol": symbol,
        "manifest": manifest,
        "manifest_fingerprint": _sha256(raw),
        "manifest_path": manifest_path,
    }


def _manifest_unchanged(snapshot):
    raw = _read_stable_bytes(snapshot["manifest_path"], MAX_MANIFEST_BYTES)
    return _sha256(raw) == snapshot["manifest_fingerprint"]


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
        day = str(action["date"])
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
    selected.sort(key=lambda action: (action["date"], action["kind"]))
    return selected


def stored_daily_snapshot(root, ticker):
    """Strict full-manifest and manifest-SHA-gated 1d close snapshot."""
    root = Path(root).resolve()
    snapshot = _manifest_record(root, ticker)
    months = storage.manifest_months(snapshot["manifest"], "1d")
    if not isinstance(months, dict) or not months:
        raise EvidenceError(f"stored 1d series is unavailable for {ticker}")
    closes = {}
    month_evidence = []
    for month in sorted(months):
        entry = months.get(month)
        if not isinstance(entry, dict) or not _valid_sha(entry.get("sha256")):
            raise EvidenceError(f"invalid 1d manifest month {month!r}")
        try:
            year, number = int(str(month)[:4]), int(str(month)[5:7])
            if not 1 <= number <= 12:
                raise ValueError
        except (TypeError, ValueError) as exc:
            raise EvidenceError(f"invalid 1d manifest month {month!r}") from exc
        path = (storage.find_month_file(
                    root, snapshot["ticker"], year, number, "1d")
                or storage.month_file_path(
                    root, snapshot["ticker"], year, number, "1d"))
        try:
            bars, stats = storage.read_month_file_fast(
                path, manifest_sha=entry["sha256"])
        except Exception as exc:  # noqa: BLE001 - strict evidence boundary
            raise EvidenceError(
                f"unreadable 1d month {month}: {type(exc).__name__}") from exc
        if stats.get("sha256") != entry["sha256"]:
            raise EvidenceError(f"1d month {month} does not match its manifest")
        expected_rows = entry.get("rows")
        if expected_rows is not None and int(expected_rows) != len(bars):
            raise EvidenceError(f"1d month {month} row count changed")
        month_evidence.append({
            "month": str(month),
            "sha256": stats["sha256"],
            "rows": len(bars),
        })
        for bar in bars:
            try:
                day = str(bar[0])[:10]
                close = float(bar[4])
            except (IndexError, TypeError, ValueError) as exc:
                raise EvidenceError(f"invalid daily bar in {month}") from exc
            if not math.isfinite(close) or close <= 0:
                raise EvidenceError(f"invalid daily close in {month}")
            closes[day] = close
    if not closes:
        raise EvidenceError(f"stored 1d series is empty for {ticker}")
    price_actions = _validated_price_actions(snapshot["manifest"])
    adjusted = {}
    for day, close in closes.items():
        factor = basis.adjustment_factor(
            price_actions, day, applies=("price", "both"))
        value = close * factor
        if not math.isfinite(value) or value <= 0:
            raise EvidenceError(f"invalid adjusted daily close on {day}")
        adjusted[day] = value
    if not _manifest_unchanged(snapshot):
        raise EvidenceError(
            f"manifest changed while reading {snapshot['ticker']}")
    snapshot.update({
        "stored": adjusted,
        "stored_rows": len(closes),
        "stored_first": min(closes),
        "stored_last": max(closes),
        "month_evidence": month_evidence,
        "stored_basis": ("newest_recorded" if price_actions else "raw"),
        "basis_actions_applied": price_actions,
    })
    snapshot.pop("manifest", None)
    return snapshot


def _close(value, tuple_index=None):
    if isinstance(value, dict):
        value = value.get("close", value.get("c"))
    elif isinstance(value, (list, tuple)):
        if tuple_index is None:
            raise ValueError("tuple close index is missing")
        value = value[tuple_index]
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("close is not positive and finite")
    return number


def normalize_reference(reference):
    if not isinstance(reference, dict):
        raise ProviderError("daily reference is not a mapping")
    out = {}
    for raw_day, raw_value in reference.items():
        day = validate._norm_date_key(raw_day)
        if not (len(day) == 10 and day[4] == "-" and day[7] == "-"):
            continue
        try:
            out[day] = _close(raw_value, 3)
        except (IndexError, KeyError, TypeError, ValueError):
            continue
    if not out or len(out) > MAX_REFERENCE_ROWS:
        raise ProviderError("daily reference row count is invalid")
    return dict(sorted(out.items()))


def reference_digest(reference):
    normalized = normalize_reference(reference)
    raw = "".join(
        f"{day}:{format(close, '.17g')}\n"
        for day, close in normalized.items()).encode("ascii")
    return _sha256(raw)


def monthly_medians(stored, external):
    """Return sorted monthly medians of stored/external shared-day closes."""
    per_month = {}
    for day, stored_close in (stored or {}).items():
        external_close = (external or {}).get(day)
        if external_close is None:
            continue
        try:
            ratio = _close(stored_close) / _close(external_close)
        except (TypeError, ValueError):
            continue
        per_month.setdefault(str(day)[:7], []).append(ratio)
    return [
        (month, st.median(values), len(values))
        for month, values in sorted(per_month.items())
    ]


def _asof_month(value=None):
    if value is None:
        parsed = dt.datetime.now().astimezone().date()
    elif isinstance(value, dt.datetime):
        parsed = value.date()
    elif isinstance(value, dt.date):
        parsed = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = (dt.date.fromisoformat(text)
                      if len(text) == 10 else
                      dt.datetime.fromisoformat(text).date())
        except ValueError as exc:
            raise EvidenceError("invalid sweep as-of date") from exc
    return f"{parsed.year:04d}-{parsed.month:02d}"


def classify_with_recency(months, *, asof=None,
                          max_age_months=MAX_OVERLAP_AGE_MONTHS):
    """Apply WS7's current-overlap gate before the six-way classifier."""
    asof_month = _asof_month(asof)
    if not months:
        result = classify(months)
        result.update({
            "asof_month": asof_month,
            "latest_overlap_month": None,
            "overlap_age_months": None,
            "max_overlap_age_months": int(max_age_months),
        })
        return result
    latest = str(months[-1][0])
    try:
        ay, am = map(int, asof_month.split("-"))
        ly, lm = map(int, latest.split("-"))
    except (TypeError, ValueError) as exc:
        raise EvidenceError("invalid overlap month") from exc
    age = (ay - ly) * 12 + am - lm
    evidence = {
        "asof_month": asof_month,
        "latest_overlap_month": latest,
        "overlap_age_months": age,
        "max_overlap_age_months": int(max_age_months),
    }
    if age < 0:
        return {
            "verdict": "UNVERIFIABLE",
            "reason": "overlap_after_asof",
            "months": len(months),
            "steps": [],
            "segments": [],
            **evidence,
        }
    if age > int(max_age_months):
        return {
            "verdict": "UNVERIFIABLE",
            "reason": "overlap_not_current",
            "months": len(months),
            "steps": [],
            "segments": [],
            **evidence,
        }
    return {**classify(months), **evidence}


def detect_steps(months, step_tol=STEP_TOL):
    steps = []
    threshold = math.log(1 + float(step_tol))
    for index in range(1, len(months)):
        previous, current = months[index - 1][1], months[index][1]
        if abs(math.log(current / previous)) > threshold:
            steps.append({
                "month": months[index][0],
                "from": previous,
                "to": current,
            })
    return steps


def segment(months, steps):
    cut_months = {row["month"] for row in steps}
    runs, run = [], []
    for row in months:
        if row[0] in cut_months and run:
            runs.append(run)
            run = []
        run.append(row)
    if run:
        runs.append(run)
    out = []
    for values in runs:
        logs = [math.log(row[1]) for row in values]
        mean = sum(logs) / len(logs)
        variance = sum((value - mean) ** 2 for value in logs) / len(logs)
        out.append({
            "start": values[0][0],
            "end": values[-1][0],
            "months": len(values),
            "ratio": math.exp(mean),
            "log_sd": math.sqrt(variance),
        })
    return out


def split_like(factor, *, max_den=SPLIT_MAX_DENOMINATOR,
               tol=SPLIT_RATIO_TOL,
               min_magnitude=SPLIT_MIN_MAGNITUDE):
    if factor <= 0:
        return False
    magnitude = factor if factor >= 1 else 1.0 / factor
    if magnitude < min_magnitude:
        return False
    for denominator in range(1, max_den + 1):
        numerator = round(magnitude * denominator)
        if numerator <= denominator or numerator > max_den * 4:
            continue
        if abs(magnitude / (numerator / denominator) - 1) <= tol:
            return True
    return False


def classify(months, *, clean_tol=CLEAN_TOL, step_tol=STEP_TOL,
             drift_cv=DRIFT_CV, current_months=CURRENT_MONTHS,
             min_months=MIN_MONTHS):
    """Classify monthly ratios with the reviewed WS7 verdict precedence."""
    if len(months) < min_months:
        return {
            "verdict": "UNVERIFIABLE",
            "reason": "insufficient_overlap",
            "months": len(months),
            "steps": [],
            "segments": [],
        }
    steps = detect_steps(months, step_tol)
    segments = segment(months, steps)
    latest_start = segments[-1]["start"]
    latest_rows = [row for row in months if row[0] >= latest_start]
    current_rows = latest_rows[-current_months:]
    current = st.median(row[1] for row in current_rows)
    out = {
        "verdict": None,
        "months": len(months),
        "steps": steps,
        "segments": segments,
        "current_ratio": current,
        "current_segment_start": latest_start,
        "current_window_months": len(current_rows),
    }
    if abs(math.log(current)) > math.log(1 + clean_tol):
        out["verdict"] = "CURRENT_MISMATCH"
        return out
    for item in segments:
        if item["months"] >= DRIFT_MIN_MONTHS and item["log_sd"] > drift_cv:
            out["verdict"] = "DRIFT_ANOMALY"
            out["drift_segment"] = item
            return out
    offsets = [
        item for item in segments
        if abs(math.log(item["ratio"])) > math.log(1 + clean_tol)
    ]
    if not offsets:
        out["verdict"] = "CLEAN"
        return out
    factors = [item["ratio"] / current for item in offsets]
    out["verdict"] = (
        "SEAM_CANDIDATE" if any(split_like(factor) for factor in factors)
        else "HISTORIC_BASIS_OFFSET")
    out["offset_segments"] = offsets
    out["offset_factors"] = factors
    out["boundary_month"] = offsets[-1]["end"]
    return out


def _round_result(value):
    if isinstance(value, float):
        if not math.isfinite(value):
            raise EvidenceError("non-finite classifier output")
        return round(value, 8)
    if isinstance(value, list):
        return [_round_result(item) for item in value]
    if isinstance(value, dict):
        return {key: _round_result(item) for key, item in value.items()}
    return value


class StockAnalysisProvider:
    """Bounded HTTPS client pinned to the one reviewed reference host."""

    def __init__(self, *, timeout=20, opener=None, now=None):
        self.timeout = max(1.0, min(float(timeout), 120.0))
        self.opener = opener
        self.now = now

    def _read(self, url, *, _fetch_worker=None, provider_symbol=None, _logical_request=None):
        if _fetch_worker is not None:
            from fetch_http_labels import (guarded_stockanalysis_attempt,
                                           stockanalysis_url)
            from fetch_run_context import RequestRefused

            if url != stockanalysis_url(provider_symbol, REFERENCE_RANGE):
                raise RequestRefused("sweep URL differs from guarded StockAnalysis endpoint")
            captured = {}

            def one_send(expected_url):
                raw = self.opener(expected_url, self.timeout)
                if isinstance(raw, str):
                    raw = raw.encode("utf-8")
                if not isinstance(raw, bytes):
                    raise RequestRefused("guarded StockAnalysis sender must return bytes")
                captured["raw_digest"] = _sha256(raw)
                return raw

            rows = guarded_stockanalysis_attempt(
                _fetch_worker, "http.stockanalysis.external_sweep",
                provider_symbol, REFERENCE_RANGE, one_send if self.opener is not None else None,
                _logical_request=_logical_request, timeout=self.timeout, _capture=captured)
            return rows, captured["raw_digest"]
        from fetch_ibkr_bridge import refuse_a2
        refuse_a2()
        if self.opener is not None:
            raw = self.opener(url, self.timeout)
        else:
            request = urllib.request.Request(url, headers=validate._UA)
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read(MAX_REFERENCE_BYTES + 1)
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        if not isinstance(raw, bytes) or not raw or len(raw) > MAX_REFERENCE_BYTES:
            raise ProviderError("reference response size is invalid")
        return raw

    def fetch(self, provider_symbol, *, _fetch_worker=None, _logical_request=None):
        provider_symbol = _provider_symbol(provider_symbol)
        encoded = urllib.parse.quote(provider_symbol, safe=".-")
        url = validate.REF_URL.format(t=encoded, r=REFERENCE_RANGE)
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname != REFERENCE_HOST:
            raise ProviderError("reference URL is not allowlisted")
        response = self._read(url, _fetch_worker=_fetch_worker,
                              provider_symbol=provider_symbol,
                              _logical_request=_logical_request)
        if _fetch_worker is not None:
            rows, source_digest = response
            reference = normalize_reference({row["label"]: row["c"]
                                             for row in rows})
        else:
            raw = response
            try:
                reference = normalize_reference(
                    validate._parse_reference(raw.decode("utf-8")))
            except (UnicodeError, ValueError, TypeError) as exc:
                raise ProviderError("reference response is invalid") from exc
            source_digest = _sha256(raw)
        fetched_at = _timestamp(
            self.now() if callable(self.now) else self.now, "fetched_at")
        result = {
            "provider": PROVIDER,
            "provider_symbol": provider_symbol,
            "range": REFERENCE_RANGE,
            "fetched_at": fetched_at,
            "source_bytes_sha256": source_digest,
            "response_digest": reference_digest(reference),
            "reference": reference,
        }
        if _fetch_worker is not None:
            from fetch_authority import digest_value
            from fetch_http_labels import stockanalysis_bounds
            context = _fetch_worker.context
            bounds = stockanalysis_bounds(context.authority,
                                          context.captured_now,
                                          REFERENCE_RANGE)
            result["http_attempt"] = {
                "url": url, "range": REFERENCE_RANGE,
                "requested_start_date": bounds["requested_start_date"],
                "truncated_coverage": bounds["truncated_coverage"],
                "intended_start": bounds["intended_start"].isoformat(),
                "intended_end": bounds["intended_end"].isoformat(),
                "authority_fingerprint": context.authority.fingerprint,
                "table_digest": context.authority.table_digest,
                "operation_id": context.operation_id,
                "accepted_digest": digest_value(rows),
            }
            result["accepted_rows"] = rows
        return result


class RequestPacer:
    def __init__(self, interval=MIN_REQUEST_INTERVAL, *, clock=time.monotonic,
                 sleep_fn=time.sleep):
        interval = float(interval)
        if interval < MIN_REQUEST_INTERVAL:
            raise ExternalSweepError(
                f"request interval must be at least {MIN_REQUEST_INTERVAL}")
        self.interval = interval
        self.clock = clock
        self.sleep_fn = sleep_fn
        self.last_request = None

    def wait(self):
        now = float(self.clock())
        if self.last_request is not None:
            delay = self.interval - (now - self.last_request)
            if delay > 0:
                self.sleep_fn(delay)
                now = float(self.clock())
        self.last_request = now


def _cache_path(cache_root, identity):
    return _direct_child(
        cache_root,
        f"{identity['ticker']}__{identity['conid']}.json",
        ".json")


def _cache_payload(identity, provider_result):
    reference = normalize_reference(provider_result.get("reference"))
    digest = reference_digest(reference)
    supplied = provider_result.get("response_digest")
    if supplied is not None and str(supplied).lower() != digest:
        raise ProviderError("provider response digest does not match its rows")
    if _provider_symbol(provider_result.get("provider_symbol")) != (
            identity["provider_symbol"]):
        raise ProviderError("provider symbol does not match manifest identity")
    if str(provider_result.get("provider") or PROVIDER) != PROVIDER:
        raise ProviderError("unexpected reference provider")
    if str(provider_result.get("range") or REFERENCE_RANGE) != REFERENCE_RANGE:
        raise ProviderError("reference range is not Max")
    source_digest = str(provider_result.get("source_bytes_sha256") or "")
    if source_digest and not _valid_sha(source_digest):
        raise ProviderError("invalid source response digest")
    return {
        "kind": CACHE_KIND,
        "version": CACHE_VERSION,
        "ticker": identity["ticker"],
        "conid": identity["conid"],
        "provider_symbol": identity["provider_symbol"],
        "manifest_fingerprint": identity["manifest_fingerprint"],
        "provider": PROVIDER,
        "range": REFERENCE_RANGE,
        "fetched_at": _timestamp(provider_result.get("fetched_at"), "fetched_at"),
        "source_bytes_sha256": source_digest or None,
        "response_digest": digest,
        "reference": reference,
        "http_attempt": provider_result.get("http_attempt"),
        "accepted_rows": provider_result.get("accepted_rows"),
    }


def validate_cache_payload(payload, expected):
    if not isinstance(payload, dict):
        raise EvidenceError("cache is not an object")
    if payload.get("kind") != CACHE_KIND or payload.get("version") != CACHE_VERSION:
        raise EvidenceError("cache kind/version is invalid")
    actual = {
        "ticker": _ticker(payload.get("ticker")),
        "conid": _conid(payload.get("conid")),
        "provider_symbol": _provider_symbol(payload.get("provider_symbol")),
        "manifest_fingerprint": str(
            payload.get("manifest_fingerprint") or "").lower(),
    }
    for key in actual:
        if actual[key] != expected[key]:
            raise EvidenceError(f"cache identity mismatch: {key}")
    if payload.get("provider") != PROVIDER or payload.get("range") != REFERENCE_RANGE:
        raise EvidenceError("cache provider/range is invalid")
    fetched_at = _timestamp(payload.get("fetched_at"), "fetched_at")
    source_digest = payload.get("source_bytes_sha256")
    if source_digest is not None and not _valid_sha(source_digest):
        raise EvidenceError("cache source digest is invalid")
    reference = normalize_reference(payload.get("reference"))
    digest = reference_digest(reference)
    if str(payload.get("response_digest") or "").lower() != digest:
        raise EvidenceError("cache response digest is invalid")
    attempt = payload.get("http_attempt")
    if attempt is not None:
        from fetch_http_labels import stockanalysis_url
        if (not isinstance(attempt, dict)
                or set(attempt) != {"url", "range", "requested_start_date",
                                    "truncated_coverage", "intended_start",
                                    "intended_end", "authority_fingerprint",
                                    "table_digest", "operation_id",
                                    "accepted_digest"}
                or attempt.get("url") != stockanalysis_url(
                    actual["provider_symbol"], REFERENCE_RANGE)
                or attempt.get("range") != REFERENCE_RANGE
                or type(attempt.get("truncated_coverage")) is not bool
                or not all(isinstance(attempt.get(key), str)
                           for key in ("requested_start_date", "intended_start",
                                       "intended_end", "authority_fingerprint",
                                       "table_digest", "operation_id",
                                       "accepted_digest"))):
            raise EvidenceError("cache HTTP attempt provenance is invalid")
    return {
        **payload,
        **actual,
        "fetched_at": fetched_at,
        "response_digest": digest,
        "reference": reference,
    }


def load_cache(cache_root, identity):
    path = _cache_path(cache_root, identity)
    if not path.is_file():
        return {"status": "missing", "usable": False, "path": str(path)}
    try:
        raw = _read_stable_bytes(path, MAX_REFERENCE_BYTES)
        payload = json.loads(raw)
        cache = validate_cache_payload(payload, identity)
    except EvidenceError as exc:
        return {
            "status": "invalid",
            "usable": False,
            "path": str(path),
            "error": str(exc),
        }
    except (TypeError, ValueError, RecursionError):
        return {
            "status": "invalid",
            "usable": False,
            "path": str(path),
            "error": "cache JSON is invalid",
        }
    return {"status": "ok", "usable": True, "path": str(path), "cache": cache}


def write_cache(cache_root, bank_root, identity, payload):
    cache_root = _outside_bank(cache_root, bank_root)
    cache_root.mkdir(parents=True, exist_ok=True)
    normalized = validate_cache_payload(payload, identity)
    path = _cache_path(cache_root, identity)
    encoded = (json.dumps(
        normalized, sort_keys=True, separators=(",", ":"),
        allow_nan=False) + "\n").encode("utf-8")
    if len(encoded) > MAX_REFERENCE_BYTES:
        raise EvidenceError("cache candidate exceeds the reference byte bound")
    with tempfile.TemporaryDirectory(prefix="external-sweep-candidate-",
                                     dir=cache_root) as temporary:
        candidate = Path(temporary) / path.name
        storage._atomic_write_bytes(candidate, encoded)
        committed = validate_cache_payload(
            json.loads(_read_stable_bytes(candidate, MAX_REFERENCE_BYTES)),
            identity)
        os.replace(candidate, path)
    return path, committed


def _sweep_ledger_attempt(payload, audit, *, required_attempt=None):
    """Bind a sweep cache candidate to one sealed physical result."""
    from fetch_authority import digest_value

    attempt = payload.get("http_attempt")
    rows = payload.get("accepted_rows")
    if (not audit["verified"] or not isinstance(attempt, dict)
            or not isinstance(rows, list)
            or audit["operation_id"] != attempt.get("operation_id")
            or attempt.get("accepted_digest") != digest_value(rows)
            or normalize_reference({row["label"]: row["c"] for row in rows})
            != payload.get("reference")):
        raise EvidenceError("sweep cache lacks matching accepted evidence")
    requested = {
        "variant": "http-series", "endpoint": "stockanalysis.history",
        "subject": payload["provider_symbol"], "token": "1d",
        "intended_start": attempt["intended_start"],
        "intended_end": attempt["intended_end"],
    }
    http_request = {key: attempt[key] for key in
                    ("url", "range", "requested_start_date",
                     "truncated_coverage")}
    decisions, matches = {}, []
    for event in audit["events"]:
        attempt_id = event["attempt_id"]
        if event["event"] == "decision":
            decisions[attempt_id] = event
        elif event["event"] == "result" and attempt_id in decisions:
            decision = decisions[attempt_id]
            outcome = event["payload"]
            accepted = outcome.get("accepted")
            if (event["operation_id"] == attempt["operation_id"]
                    and event["producer_id"] ==
                    "http.stockanalysis.external_sweep"
                    and decision["payload"].get("requested") == requested
                    and decision["payload"].get("effective") == requested
                    and decision["payload"].get("http_request") == http_request
                    and decision["payload"].get("authority_fingerprint") ==
                    attempt["authority_fingerprint"]
                    and decision["payload"].get("table_digest") ==
                    attempt["table_digest"]
                    and outcome.get("outcome") in {"returned", "empty"}
                    and outcome.get("raw_response_digest") ==
                    payload["source_bytes_sha256"]
                    and isinstance(accepted, dict)
                    and accepted.get("digest") == attempt["accepted_digest"]
                    and accepted.get("count") == len(rows)
                    and (required_attempt is None
                         or attempt_id == required_attempt)):
                matches.append(attempt_id)
    if len(matches) != 1:
        raise EvidenceError("sweep cache has no unique durable result")
    return matches[0]


def _bind_sweep_cache(payload, context, cache_root):
    """Attach portable receipt-verified provenance only after root seal."""
    from fetch_ledger import inspect_ledger

    audit = inspect_ledger(context.ledger.path)
    item = dict(payload)
    item["attempt_id"] = _sweep_ledger_attempt(item, audit)
    logs_root = Path(cache_root).parent.resolve(strict=True)
    ledger = context.ledger.path.resolve(strict=True)
    try:
        item["ledger_locator"] = ledger.relative_to(logs_root).as_posix()
    except ValueError as exc:
        raise EvidenceError("sweep ledger is outside the cache evidence root") from exc
    return item


def _verified_sweep_cache_hit(payload, context, cache_root):
    """A legacy or damaged entry is a miss, never verified replay."""
    from fetch_authority import digest_value
    from fetch_envelopes import parse_envelope
    from fetch_http_labels import (filter_stockanalysis_rows,
                                   stockanalysis_bounds, stockanalysis_url)
    from fetch_ledger import inspect_ledger

    try:
        attempt = payload["http_attempt"]
        rows = payload["accepted_rows"]
        locator = payload["ledger_locator"]
        attempt_id = payload["attempt_id"]
        if (not isinstance(attempt, dict) or not isinstance(rows, list)
                or not rows or len(rows) > MAX_REFERENCE_ROWS
                or not isinstance(locator, str) or not locator
                or Path(locator).is_absolute()
                or Path(locator).as_posix() != locator
                or ".." in Path(locator).parts
                or not isinstance(attempt_id, str) or not attempt_id
                or not _valid_sha(payload["source_bytes_sha256"])):
            return False
        bounds = stockanalysis_bounds(context.authority,
                                      context.captured_now, REFERENCE_RANGE)
        expected = {
            "url": stockanalysis_url(payload["provider_symbol"], REFERENCE_RANGE),
            "range": REFERENCE_RANGE,
            "requested_start_date": bounds["requested_start_date"],
            "truncated_coverage": bounds["truncated_coverage"],
            "intended_start": bounds["intended_start"].isoformat(),
            "intended_end": bounds["intended_end"].isoformat(),
            "authority_fingerprint": context.authority.fingerprint,
            "table_digest": context.authority.table_digest,
            "operation_id": attempt["operation_id"],
            "accepted_digest": digest_value(rows),
        }
        if (attempt != expected
                or normalize_reference({row["label"]: row["c"]
                                        for row in rows}) != payload["reference"]):
            return False
        envelope = parse_envelope({
            "variant": "http-series", "endpoint": "stockanalysis.history",
            "subject": payload["provider_symbol"], "token": "1d",
            "intended_start": bounds["intended_start"],
            "intended_end": bounds["intended_end"],
        })
        if filter_stockanalysis_rows(rows, envelope, context) != rows:
            return False
        logs_root = Path(cache_root).parent.resolve(strict=True)
        ledger = (logs_root / locator).resolve(strict=True)
        if not ledger.is_relative_to(logs_root) or ledger.suffix != ".jsonl":
            return False
        audit = inspect_ledger(ledger)
        return _sweep_ledger_attempt(
            payload, audit, required_attempt=attempt_id) == attempt_id
    except (AuthorityError, LedgerError, EvidenceError, KeyError, TypeError,
            ValueError, OSError, OverflowError, ArithmeticError,
            AttributeError, IndexError, RecursionError):
        return False


class _CacheSpool:
    """Bounded outside-bank candidate files; retain only paths in memory."""

    def __init__(self, run_logs_root):
        self.run_logs_root = Path(run_logs_root)
        self._temporary = None
        self._entries = []
        self._total_bytes = 0

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        if self._temporary is not None:
            self._temporary.cleanup()

    def append(self, item):
        cache_root, bank, identity, payload, snapshot = item
        try:
            encoded = (json.dumps(payload, sort_keys=True,
                                  separators=(",", ":"), allow_nan=False)
                       + "\n").encode("utf-8")
            if (not encoded or len(encoded) > MAX_REFERENCE_BYTES
                    or len(self._entries) >= MAX_TICKERS
                    or self._total_bytes + len(encoded) >
                    MAX_TICKERS * MAX_REFERENCE_BYTES):
                raise SpoolError("sweep candidate exceeds the spool bound")
            if self._temporary is None:
                self.run_logs_root.mkdir(parents=True, exist_ok=True)
                self._temporary = tempfile.TemporaryDirectory(
                    prefix="external-sweep-spool-", dir=self.run_logs_root)
            candidate = Path(self._temporary.name) / (
                f"candidate-{len(self._entries):04d}.json")
            storage._atomic_write_bytes(candidate, encoded)
        except (OSError, storage.StorageError, TypeError, ValueError,
                RecursionError) as exc:
            raise SpoolError("sweep candidate staging failed") from exc
        self._entries.append((cache_root, bank, identity,
                              {"manifest_path": snapshot.get("manifest_path"),
                               "manifest_fingerprint":
                               snapshot["manifest_fingerprint"]}, candidate))
        self._total_bytes += len(encoded)

    def __iter__(self):
        for cache_root, bank, identity, snapshot, candidate in self._entries:
            try:
                payload = json.loads(_read_stable_bytes(
                    candidate, MAX_REFERENCE_BYTES))
            except (OSError, EvidenceError, TypeError, ValueError,
                    RecursionError) as exc:
                raise SpoolError("sweep candidate replay failed") from exc
            yield cache_root, bank, identity, payload, snapshot


def _provider_fetch(provider, symbol, *, _fetch_worker=None, _logical_request=None):
    options = {}
    if _fetch_worker is not None:
        from fetch_run_context import RequestRefused
        if type(provider) is not StockAnalysisProvider:
            raise RequestRefused("sweep provider must use the guarded StockAnalysis client")
        options["_fetch_worker"] = _fetch_worker
        options["_logical_request"] = _logical_request
    if hasattr(provider, "fetch"):
        result = provider.fetch(symbol, **options)
    else:
        result = provider(symbol)
    if isinstance(result, dict) and "reference" in result:
        out = dict(result)
    else:
        out = {"reference": result}
    out.setdefault("provider", PROVIDER)
    out.setdefault("provider_symbol", symbol)
    out.setdefault("range", REFERENCE_RANGE)
    out.setdefault("fetched_at", _timestamp(None, "fetched_at"))
    return out


def _fetch_with_retry(provider, symbol, pacer, attempts, backoff,
                      sleep_fn=time.sleep, _fetch_worker=None):
    attempts = int(attempts)
    if attempts < 1 or attempts > 5:
        raise ExternalSweepError("attempts must be between 1 and 5")
    backoff = max(0.0, min(float(backoff), 60.0))
    logical_request = None
    if _fetch_worker is not None:
        if type(provider) is not StockAnalysisProvider:
            raise RequestRefused("sweep provider must use the guarded StockAnalysis client")
        from fetch_http_labels import stockanalysis_request
        logical_request = stockanalysis_request(_fetch_worker,
            "http.stockanalysis.external_sweep", symbol, REFERENCE_RANGE)
    last = None
    for attempt in range(1, attempts + 1):
        pacer.wait()
        try:
            return _provider_fetch(provider, symbol,
                                   _fetch_worker=_fetch_worker,
                                   _logical_request=logical_request), attempt
        except Exception as exc:  # noqa: BLE001 - bounded retry then fail closed
            if isinstance(exc, ResponseQuarantined):
                exc.attempts = attempt
                raise
            if isinstance(exc, (AuthorityError, LedgerError, RequestCancelled,
                                RequestRefused)):
                raise
            if isinstance(exc, HTTPError):
                if exc.code == 403:
                    raise RequestRefused("StockAnalysis provider blocked (HTTP 403)") from exc
                if not (exc.code in (408, 429) or 500 <= exc.code <= 599):
                    exc.attempts = attempt
                    raise
            last = exc
            if attempt < attempts and backoff:
                sleep_fn(backoff * (2 ** (attempt - 1)))
    error = ProviderError(
        f"reference fetch failed after {attempts} attempt(s): "
        f"{type(last).__name__}")
    error.attempts = attempts
    raise error from last


def _unverifiable(ticker, reason, *, error_type=None, identity=None,
                  cache_status=None, network_requests=0, quarantine=None):
    row = {
        "ticker": str(ticker).strip().upper(),
        "verdict": "UNVERIFIABLE",
        "reason": str(reason)[:200],
        "months": 0,
        "steps": [],
        "segments": [],
        "network_requests": int(network_requests),
    }
    if error_type:
        row["error_type"] = str(error_type)
    if quarantine:
        row["quarantine"] = str(quarantine)
    if identity:
        row.update({
            key: identity[key]
            for key in ("conid", "provider_symbol", "manifest_fingerprint")
            if key in identity
        })
    if cache_status:
        row["cache_status"] = cache_status
    return row


def sweep_ticker(root, ticker, *, provider, cache_root=CACHE_ROOT,
                 pacer=None, attempts=DEFAULT_ATTEMPTS,
                 backoff=DEFAULT_BACKOFF, sleep_fn=time.sleep, asof=None,
                 _test_capability=None, _evidence_dir=None,
                 _authority=None, _clock=None, _governors=None):
    """One held sweep root; classify under a child and publish after seal."""
    directory = (Path(_evidence_dir) if _evidence_dir is not None else
                 RUN_LOGS_ROOT / "_fetch_ledgers")
    operation = fops.begin_operation(
        "external_sweep", directory, test_capability=_test_capability,
        authority=_authority, clock=_clock, governors=_governors)
    try:
        fops.require_root_admission(operation)
        pending = []
        with fops.scoped_worker(operation.child(
                "sweep-ticker", rights={"http.stockanalysis.external_sweep"}),
                close=True) as worker:
            result = _sweep_ticker_body(
                root, ticker, provider=provider, cache_root=cache_root,
                pacer=pacer, attempts=attempts, backoff=backoff,
                sleep_fn=sleep_fn,
                asof=asof or operation.context.captured_now,
                _fetch_worker=worker, _pending_caches=pending)
        operation.seal()
        for target_cache, bank, identity, payload, snapshot in pending:
            try:
                if not _manifest_unchanged(snapshot):
                    raise EvidenceError("manifest changed before cache publication")
                write_cache(target_cache, bank, identity,
                            _bind_sweep_cache(payload, operation.context,
                                              target_cache))
            except Exception as exc:
                raise PublicationRecoveryDebt([{
                    "ticker": identity["ticker"], "stage": "cache",
                    "cause_type": type(exc).__name__,
                }]) from exc
        return result
    finally:
        operation.close()


def _sweep_ticker_body(root, ticker, *, provider, cache_root=CACHE_ROOT,
                       pacer=None, attempts=DEFAULT_ATTEMPTS,
                       backoff=DEFAULT_BACKOFF, sleep_fn=time.sleep, asof=None,
                       _fetch_worker=None, _pending_caches=None):
    """Sweep one ticker using exact cache evidence or an explicit fetch."""
    ticker = _ticker(ticker)
    try:
        snapshot = stored_daily_snapshot(root, ticker)
    except Exception as exc:  # noqa: BLE001 - one ticker fails closed
        return _unverifiable(
            ticker, str(exc), error_type=type(exc).__name__)

    identity = {
        key: snapshot[key]
        for key in ("ticker", "conid", "provider_symbol",
                    "manifest_fingerprint")
    }
    if _fetch_worker is not None:
        from fetch_envelopes import parse_envelope
        from fetch_http_labels import stockanalysis_bounds

        context = _fetch_worker.context
        fops.check_worker(context, _fetch_worker.worker_id)
        bounds = stockanalysis_bounds(context.authority,
                                      context.captured_now, REFERENCE_RANGE)
        envelope = parse_envelope({
            "variant": "http-series", "endpoint": "stockanalysis.history",
            "subject": identity["provider_symbol"], "token": "1d",
            "intended_start": bounds["intended_start"],
            "intended_end": bounds["intended_end"],
        })
        if not context.permits("http.stockanalysis.external_sweep", envelope):
            raise RequestRefused("sweep cache lacks operation rights")
    cache = load_cache(cache_root, identity)
    if _fetch_worker is not None:
        if not (cache["usable"] and _verified_sweep_cache_hit(
                cache["cache"], context, cache_root)):
            cache = {"status": "UNVERIFIED", "usable": False,
                     "path": cache["path"]}
    requests = 0
    source = "cache"
    try:
        if cache["usable"]:
            payload = cache["cache"]
        else:
            source = "network"
            pacer = pacer or RequestPacer(sleep_fn=sleep_fn)
            provider_result, requests = _fetch_with_retry(
                provider, identity["provider_symbol"], pacer,
                attempts, backoff, sleep_fn=sleep_fn,
                _fetch_worker=_fetch_worker)
            if not _manifest_unchanged(snapshot):
                raise EvidenceError("manifest changed during reference fetch")
            payload = _cache_payload(identity, provider_result)
            if _pending_caches is None:
                _path, payload = write_cache(
                    cache_root, root, identity, payload)
            else:
                _pending_caches.append((cache_root, root, identity,
                                        payload, snapshot))
        months = monthly_medians(snapshot["stored"], payload["reference"])
        result = classify_with_recency(months, asof=asof)
        if not _manifest_unchanged(snapshot):
            raise EvidenceError("manifest changed during classification")
    except ResponseQuarantined as exc:
        return _unverifiable(
            ticker, f"reference response quarantined: {exc}",
            error_type="ResponseQuarantined", identity=identity,
            cache_status=cache["status"],
            network_requests=max(requests, int(getattr(exc, "attempts", 0))),
            quarantine=exc.quarantine_path)
    except (AuthorityError, LedgerError, RequestCancelled, RequestRefused,
            SpoolError):
        # A terminal guard/ledger failure cannot become a successful batch
        # seal with one ordinary UNVERIFIABLE row.
        raise
    except Exception as exc:  # noqa: BLE001 - fail closed per ticker
        requests = max(requests, int(getattr(exc, "attempts", 0)))
        return _unverifiable(
            ticker, str(exc), error_type=type(exc).__name__,
            identity=identity, cache_status=cache["status"],
            network_requests=requests)

    shared_days = sorted(set(snapshot["stored"]) & set(payload["reference"]))
    row = _round_result({
        "ticker": ticker,
        "conid": identity["conid"],
        "provider_symbol": identity["provider_symbol"],
        "manifest_fingerprint": identity["manifest_fingerprint"],
        "reference_digest": payload["response_digest"],
        "reference_fetched_at": payload["fetched_at"],
        "reference_source": source,
        "cache_status": cache["status"],
        "network_requests": requests,
        "stored_rows": snapshot["stored_rows"],
        "stored_first": snapshot["stored_first"],
        "stored_last": snapshot["stored_last"],
        "stored_basis": snapshot["stored_basis"],
        "basis_actions_applied": snapshot["basis_actions_applied"],
        "overlap_days": len(shared_days),
        "overlap_first": shared_days[0] if shared_days else None,
        "overlap_last": shared_days[-1] if shared_days else None,
        **result,
    })
    return row


def list_tickers(root):
    root = Path(root)
    if not root.is_dir():
        return []
    out = []
    for path in sorted(root.iterdir(), key=lambda item: item.name.casefold()):
        if path.is_dir() and not path.name.startswith("_"):
            try:
                if (path / storage.MANIFEST_NAME).is_file():
                    out.append(_ticker(path.name))
            except EvidenceError:
                continue
    return out


def _owner(value):
    text = str(value or "").strip().lower()
    if not _OWNER_RE.fullmatch(text):
        raise ExternalSweepError(f"invalid artifact owner: {value!r}")
    return text


def artifact_path(owner, *, run_logs_root=RUN_LOGS_ROOT, now=None):
    owner = _owner(owner)
    if now is None:
        current = dt.datetime.now().astimezone()
    elif isinstance(now, dt.datetime):
        current = now
    else:
        raise ExternalSweepError("invalid artifact clock")
    return _direct_child(
        run_logs_root,
        f"external-sweep-{current:%Y%m%d}-{owner}.json",
        ".json")


def write_artifact(path, report, *, bank_root=STORAGE_ROOT,
                   run_logs_root=RUN_LOGS_ROOT):
    path = _outside_bank(path, bank_root)
    expected = _direct_child(run_logs_root, path.name, ".json")
    if path != expected:
        raise EvidenceError("artifact must be a direct Run Logs child")
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(
        report, indent=2, sort_keys=True, allow_nan=False) + "\n").encode(
            "utf-8")
    storage._atomic_write_bytes(path, encoded)
    return path


def sweep_bank(root=STORAGE_ROOT, tickers=None, *, provider=None,
               cache_root=CACHE_ROOT, run_logs_root=RUN_LOGS_ROOT,
               owner="manual", artifact=None, pace=MIN_REQUEST_INTERVAL,
               attempts=DEFAULT_ATTEMPTS, backoff=DEFAULT_BACKOFF,
               gate_path=None, clock=time.monotonic, sleep_fn=time.sleep,
               now=None, _test_capability=None, _evidence_dir=None,
               _authority=None, _clock=None, _governors=None):
    """One held batch root; child bodies share a single captured operation."""
    directory = (Path(_evidence_dir) if _evidence_dir is not None else
                 RUN_LOGS_ROOT / "_fetch_ledgers")
    operation = fops.begin_operation(
        "external_sweep", directory, test_capability=_test_capability,
        authority=_authority, clock=_clock, governors=_governors)
    try:
        fops.require_root_admission(operation)
        spool_root = _outside_bank(run_logs_root, root)
        with _CacheSpool(spool_root) as pending_caches:
            pending_artifacts = []
            with fops.scoped_worker(operation.child(
                    "sweep-bank", rights={"http.stockanalysis.external_sweep"}),
                    close=True) as worker:
                report = _sweep_bank_body(
                    root, tickers, provider=provider, cache_root=cache_root,
                    run_logs_root=run_logs_root, owner=owner, artifact=artifact,
                    pace=pace, attempts=attempts, backoff=backoff,
                    gate_path=gate_path, clock=clock, sleep_fn=sleep_fn,
                    now=now or operation.context.captured_now,
                    _fetch_worker=worker, _pending_caches=pending_caches,
                    _pending_artifacts=pending_artifacts)
            operation.seal()
            debt, published = [], 0
            try:
                for target_cache, bank, identity, payload, snapshot in pending_caches:
                    if not _manifest_unchanged(snapshot):
                        raise EvidenceError("manifest changed before cache publication")
                    write_cache(target_cache, bank, identity,
                                _bind_sweep_cache(payload, operation.context,
                                                  target_cache))
                    published += 1
            except Exception as exc:
                for index, entry in enumerate(pending_caches._entries[published:]):
                    debt.append({
                        "ticker": entry[2]["ticker"],
                        "stage": "cache" if index == 0 else "cache_not_attempted",
                        "cause_type": type(exc).__name__ if index == 0 else None,
                    })
            report["publication_state"] = (
                "recovery_debt" if debt else "complete")
            report["recovery_debt"] = debt
            for target, report, bank, logs in pending_artifacts:
                try:
                    path = write_artifact(target, report, bank_root=bank,
                                          run_logs_root=logs)
                    report["artifact"] = str(path)
                except Exception as exc:
                    debt.append({
                        "ticker": None, "stage": "report",
                        "cause_type": type(exc).__name__,
                    })
                    report["publication_state"] = "recovery_debt"
                    report["recovery_debt"] = debt
                    break
            if debt:
                raise PublicationRecoveryDebt(debt, report)
            return report
    finally:
        operation.close()


def _sweep_bank_body(root=STORAGE_ROOT, tickers=None, *, provider=None,
                     cache_root=CACHE_ROOT, run_logs_root=RUN_LOGS_ROOT,
                     owner="manual", artifact=None, pace=MIN_REQUEST_INTERVAL,
                     attempts=DEFAULT_ATTEMPTS, backoff=DEFAULT_BACKOFF,
                     gate_path=None, clock=time.monotonic,
                     sleep_fn=time.sleep, now=None, _fetch_worker=None,
                     _pending_caches=None, _pending_artifacts=None):
    """Run an explicitly network-enabled, operation-gated sweep."""
    root = Path(root).resolve()
    cache_root = _outside_bank(cache_root, root)
    run_logs_root = _outside_bank(run_logs_root, root)
    names = list_tickers(root) if tickers is None else [_ticker(t) for t in tickers]
    names = sorted(set(names))
    if not names or len(names) > MAX_TICKERS:
        raise ExternalSweepError(
            f"ticker count must be between 1 and {MAX_TICKERS}")
    provider = provider or StockAnalysisProvider(now=now)
    pacer = RequestPacer(pace, clock=clock, sleep_fn=sleep_fn)
    started_value = now() if callable(now) else now
    started = _timestamp(started_value, "started_at")

    lease = operation_gate.acquire(
        "external_sweep", owner=f"WS7 {owner}", path=gate_path)
    try:
        rows = [
            _sweep_ticker_body(
                root, ticker, provider=provider, cache_root=cache_root,
                pacer=pacer, attempts=attempts, backoff=backoff,
                sleep_fn=sleep_fn, asof=started_value,
                _fetch_worker=_fetch_worker,
                _pending_caches=_pending_caches)
            for ticker in names
        ]
        for row in rows:
            fingerprint = row.get("manifest_fingerprint")
            if not fingerprint or row.get("verdict") == "UNVERIFIABLE":
                continue
            try:
                current = _manifest_record(root, row["ticker"])
                unchanged = current["manifest_fingerprint"] == fingerprint
            except Exception:  # noqa: BLE001 - final race check fails closed
                unchanged = False
            if not unchanged:
                row.update({
                    "verdict": "UNVERIFIABLE",
                    "reason": "manifest changed before sweep completion",
                    "error_type": "EvidenceChanged",
                    "months": 0,
                    "steps": [],
                    "segments": [],
                })
                for key in ("current_ratio", "drift_segment",
                            "offset_segments", "offset_factors",
                            "boundary_month", "current_segment_start",
                            "current_window_months"):
                    row.pop(key, None)
        counts = {
            verdict: sum(row["verdict"] == verdict for row in rows)
            for verdict in VERDICTS
        }
        report = {
            "kind": REPORT_KIND,
            "version": REPORT_VERSION,
            "report_only": True,
            "network": True,
            "provider": PROVIDER,
            "reference_host": REFERENCE_HOST,
            "reference_range": REFERENCE_RANGE,
            "started_at": started,
            "finished_at": _timestamp(
                now() if callable(now) else now, "finished_at"),
            "params": {
                "ticker_count": len(names),
                "pace_seconds": float(pace),
                "attempts": int(attempts),
                "backoff_seconds": float(backoff),
                "owner": _owner(owner),
            },
            "counts": counts,
            "cache_hits": sum(row.get("reference_source") == "cache"
                              for row in rows),
            "network_requests": sum(int(row.get("network_requests") or 0)
                                    for row in rows),
            "rows": rows,
        }
        artifact_now = now() if callable(now) else now
        target = (Path(artifact) if artifact is not None else artifact_path(
            owner, run_logs_root=run_logs_root,
            now=(artifact_now if isinstance(artifact_now, dt.datetime)
                 else None)))
        if _pending_artifacts is None:
            target = write_artifact(
                target, report, bank_root=root, run_logs_root=run_logs_root)
            report["artifact"] = str(target)
        else:
            _pending_artifacts.append((target, report, root, run_logs_root))
        return report
    finally:
        lease.release()


def offline_status(root=STORAGE_ROOT, tickers=None, *, cache_root=CACHE_ROOT):
    """Read cache readiness only; no network, gate, directory, or file write."""
    root = Path(root).resolve()
    cache_root = _outside_bank(cache_root, root)
    names = list_tickers(root) if tickers is None else [_ticker(t) for t in tickers]
    names = sorted(set(names))
    rows = []
    for ticker in names:
        try:
            identity = _manifest_record(root, ticker)
            expected = {
                key: identity[key]
                for key in ("ticker", "conid", "provider_symbol",
                            "manifest_fingerprint")
            }
            cache = load_cache(cache_root, expected)
            rows.append({
                **expected,
                "status": cache["status"],
                "usable": bool(cache["usable"]),
            })
        except Exception as exc:  # noqa: BLE001 - status is fail closed
            rows.append({
                "ticker": ticker,
                "status": "identity_error",
                "usable": False,
                "error_type": type(exc).__name__,
            })
    return {
        "kind": "external_sweep_offline_status",
        "version": REPORT_VERSION,
        "network": False,
        "ticker_count": len(rows),
        "counts": {
            state: sum(row["status"] == state for row in rows)
            for state in sorted({row["status"] for row in rows})
        },
        "rows": rows,
    }
