"""Report-only data-quality notes for multi-ticker folder exports.

Report construction reads stable manifests, current WS7 v2 artifacts,
fingerprinted cross-validation sidecars, source-confirmed volatility
settlements, and the proven-pure Tier 0 query facade. Report construction
itself performs no network, process, bank, or sidecar write.
``ensure_current_health`` is the explicit pre-export health-cache refresh and
``write_report_note`` writes only outside the storage bank.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import itertools
import json
import math
import os
import re
from pathlib import Path

import health as health_report
import stock_storage as storage
import tier0_queries
import vol_value_audit


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STORAGE_ROOT = PROJECT_ROOT / storage.STORAGE_DIR_NAME
RUN_LOGS_ROOT = PROJECT_ROOT / "Run Logs"

REPORT_KIND = "export_data_quality_report"
REPORT_VERSION = 6
WS7_KIND = "external_full_range_sweep"
WS7_VERSION = 2
XVAL_BASENAME = "_cross_validation.json"
XVAL_SCHEMA_VERSION = 4
XVAL_FULL_HISTORY_HEAD_TOLERANCE_DAYS = 45
XVAL_DENSE_PRICE_PERCENT = 2
XVAL_DENSE_MIN_CHECKED_DAYS = 252
COMBINED_BASENAME = "_combined_flags.json"
COMBINED_SCHEMA_VERSION = 3
COMBINED_INTERNAL_INTERVAL = "1d"
COVERAGE_BASENAME = "_coverage_audit.json"
COVERAGE_SCHEMA_VERSION = 2
COVERAGE_PRIMARY_INTERVAL = "1m"
COVERAGE_STALE_MONTHS = 2

MAX_TICKERS = 1_000
MAX_ARTIFACTS = 64
MAX_WS7_BYTES = 16 * 1024 * 1024
MAX_XVAL_BYTES = 16 * 1024 * 1024
MAX_COMBINED_BYTES = 8 * 1024 * 1024
MAX_COVERAGE_BYTES = 16 * 1024 * 1024
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_REPORT_BYTES = 2 * 1024 * 1024
MAX_NOTE_BYTES = 512 * 1024
MAX_REASON = 320
MAX_DETAIL = 160
MAX_PATH = 1_024
MAX_MISSING_MONTHS = 240
MAX_SOURCE_ERRORS = 32
MAX_SIDECAR_ENTRIES = 4_000
MAX_XVAL_ROWS = 300
MAX_SOURCE_INTERVALS = 16
MAX_XVAL_EVIDENCE_PER_TICKER = 4
MAX_QUALITY_FINDINGS = MAX_XVAL_EVIDENCE_PER_TICKER + 6
MAX_EXAMPLE_DATES = 3
MAX_SETTLED_VOL_ROWS = 500

EXPORT_STATUSES = (
    "WRITTEN",
    "EMPTY",
    "FAILED",
    "CANCELLED",
    "NOT_IN_BANK",
)
QUALITY_ASSESSMENTS = (
    "CONFIRMED_BAD",
    "REVIEW_REQUIRED",
    "KNOWN_DIFFERENCE",
    "UNVERIFIABLE",
    "CLEAN",
    "NO_CURRENT_EVIDENCE",
)
WS7_VERDICTS = (
    "UNVERIFIABLE",
    "CURRENT_MISMATCH",
    "DRIFT_ANOMALY",
    "SEAM_CANDIDATE",
    "HISTORIC_BASIS_OFFSET",
    "CLEAN",
)
ACTIONABLE_SPLIT_VERDICTS = {
    "PHANTOM", "MISSING", "REGRESSION", "IDENTITY_BASIS",
}
XVAL_STATUSES = {
    "validated", "discrepancy", "unavailable", "inconclusive",
    "structural-ok", "structural-flag",
}
COMBINED_STATUSES = {"flagged", "ok", "error", "inconclusive"}
XVAL_FIELDS = (
    "open", "high", "low", "close", "volume", "level_shift",
    "bar", "gap", "daily-close", "other",
)
XVAL_PRICE_FIELDS = frozenset({
    "open", "high", "low", "close", "level_shift",
})
XVAL_OHLC_FIELDS = frozenset({"open", "high", "low", "close"})
XVAL_OBSERVED_STATUSES = XVAL_STATUSES

# Exact events classified by the reviewed 2026-07-14 Max-history sweep. A
# ticker-only match is deliberately insufficient: unknown dates fail closed.
XVAL_KNOWN_CORPORATE_ACTIONS = frozenset({
    ("ABT", "2013-01-02"),
    ("ADP", "2014-10-01"),
    ("APD", "2016-10-05"),
    ("APTV", "2017-12-05"),
    ("BAX", "2015-06-29"),
    ("BDX", "2026-02-06"),
    ("BIIB", "2017-01-31"),
    ("CAG", "2016-11-09"),
    ("CMCSA", "2026-01-06"),
    ("COP", "2012-04-26"),
    ("CRWD", "2026-06-24"),
    ("DD", "2025-10-30"),
    ("DHR", "2016-07-08"),
    ("DOC", "2016-11-01"),
    ("DOV", "2018-05-08"),
    ("DRI", "2015-11-05"),
    ("DTE", "2021-06-30"),
    ("EBAY", "2015-07-16"),
    ("ECHO", "2019-09-10"),
    ("EXC", "2022-02-02"),
    ("EXPE", "2011-12-20"),
    ("FDX", "2026-06-01"),
    ("FLEX", "2023-12-28"),
    ("FTV", "2025-06-26"),
    ("GE", "2023-01-04"),
    ("HLT", "2016-12-29"),
    ("HON", "2026-06-24"),
    ("HPE", "2017-03-31"),
    ("HPQ", "2015-10-28"),
    ("HSIC", "2013-03-11"),
    ("HWM", "2020-03-27"),
    ("IBM", "2021-11-01"),
    ("IRM", "2012-10-18"),
    ("J", "2024-09-30"),
    ("KMB", "2014-11-03"),
    ("LEN", "2025-01-16"),
    ("LH", "2023-06-30"),
    ("MAS", "2015-06-26"),
    ("MDLZ", "2012-10-03"),
    ("MET", "2017-08-02"),
    ("MMM", "2024-03-26"),
    ("MRK", "2021-06-02"),
    ("NI", "2015-06-29"),
    ("OXY", "2014-12-02"),
    ("PNR", "2018-04-27"),
    ("PPL", "2015-05-28"),
    ("RJF", "2021-09-22"),
    ("RTX", "2020-03-31"),
    ("SPG", "2014-06-02"),
    ("T", "2022-04-07"),
    ("TT", "2020-03-03"),
    ("VLO", "2013-05-02"),
    ("VTR", "2015-08-13"),
    ("WDC", "2025-02-21"),
    ("WMB", "2011-12-28"),
    ("YUM", "2016-10-28"),
    ("ZBH", "2022-02-24"),
})

_RUN_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}\Z")
_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,31}\Z")
_MONTH_RE = re.compile(r"\d{4}-\d{2}\Z")
_DATE_RE = re.compile(r"[12]\d{3}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])\Z")
_SHA_RE = re.compile(r"[0-9a-f]{64}\Z")
_NOTE_COUNTER = itertools.count(1)


class ExportQualityError(RuntimeError):
    """The quality-note contract cannot safely continue."""


class EvidenceError(ExportQualityError):
    """An evidence source failed validation."""


class HistoricalEvidence(EvidenceError):
    """A valid older WS7 artifact is context, not current evidence."""

    def __init__(self, version):
        super().__init__(f"historical WS7 schema version {version}")
        self.version = version


class UnrelatedEvidence(EvidenceError):
    """A Run Logs file matched the broad glob but is not a WS7 report."""


def _sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def _bounded(value, limit=MAX_REASON, default=""):
    text = " ".join(str(value if value is not None else default).split())
    if len(text) <= limit:
        return text
    return text[:max(0, limit - 3)].rstrip() + "..."


def _input_text(value, label, *, limit=MAX_DETAIL, allow_empty=False):
    text = " ".join(str(value or "").split())
    if (not text and not allow_empty) or len(text) > limit:
        raise ExportQualityError(f"invalid {label}")
    return text


def _canonical_ticker(value):
    try:
        ticker = storage.canonical_ticker(value)
    except Exception as exc:  # noqa: BLE001 - normalize storage error
        raise ExportQualityError(f"invalid ticker: {value!r}") from exc
    return ticker


def _valid_sha(value):
    return bool(_SHA_RE.fullmatch(str(value or "").strip().lower()))


def _health_report_current(root, report):
    """Match Tier 0's cached-health bank-state currency check."""
    try:
        if not isinstance(report, dict) or report.get("kind") != "health_report":
            return False
        source = report.get("source_state")
        if (not isinstance(source, dict)
                or source.get("schema_version")
                != storage.BANK_STATE_FINGERPRINT_VERSION
                or source.get("algorithm") != "sha256"
                or source.get("current") is not True
                or not _SHA_RE.fullmatch(
                    str(source.get("after_sha256") or ""))
                or source.get("before_sha256") != source.get("after_sha256")):
            return False
        current = storage.bank_manifest_state_fingerprint(root)
        return current["sha256"] == source["after_sha256"]
    except Exception:  # noqa: BLE001 - non-current is the safe fallback
        return False


def ensure_current_health(root, *, audit_fn=None, current_fn=None):
    """Return current cached health, regenerating it only when necessary."""
    root = Path(root).resolve()
    cached = health_report.load_report(root)
    current = (_health_report_current(root, cached)
               if current_fn is None else bool(current_fn(root)))
    if current:
        return cached
    if audit_fn is not None:
        return audit_fn(root)
    return health_report.audit(root, write=True)


def _parse_timestamp(value, label):
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        try:
            parsed = dt.datetime.fromisoformat(
                str(value or "").strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise EvidenceError(f"invalid {label}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EvidenceError(f"{label} must include a timezone")
    return parsed


def _generated_timestamp(value=None):
    if value is None:
        parsed = dt.datetime.now().astimezone()
    elif isinstance(value, dt.datetime):
        parsed = value
    else:
        raise ExportQualityError("injected report clock must be a datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExportQualityError("injected report clock must include a timezone")
    return parsed.isoformat(timespec="seconds")


def _reject_constant(value):
    raise ValueError(f"non-finite JSON constant {value}")


def _object_without_duplicate_keys(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key {key!r}")
        value[key] = item
    return value


def _load_json(raw, label):
    try:
        payload = json.loads(
            raw, parse_constant=_reject_constant,
            object_pairs_hook=_object_without_duplicate_keys)
    except (TypeError, ValueError, UnicodeError) as exc:
        raise EvidenceError(f"invalid JSON in {label}") from exc
    if not isinstance(payload, dict):
        raise EvidenceError(f"{label} is not an object")
    return payload


def _iso_date(value, label):
    text = str(value or "").strip()
    if not _DATE_RE.fullmatch(text):
        raise EvidenceError(f"invalid {label}")
    try:
        dt.date.fromisoformat(text)
    except ValueError as exc:
        raise EvidenceError(f"invalid {label}") from exc
    return text


def _iso_month(value, label):
    text = str(value or "").strip()
    if not _MONTH_RE.fullmatch(text):
        raise EvidenceError(f"invalid {label}")
    try:
        dt.date(int(text[:4]), int(text[5:7]), 1)
    except ValueError as exc:
        raise EvidenceError(f"invalid {label}") from exc
    return text


def _evidence_timestamp(value, label):
    return _parse_timestamp(value, label).isoformat(timespec="seconds")


def _finite_tree(value, *, depth=0):
    if depth > 12:
        raise EvidenceError("evidence nesting is too deep")
    if isinstance(value, float) and not math.isfinite(value):
        raise EvidenceError("evidence contains a non-finite number")
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise EvidenceError("evidence has a non-string key")
        for item in value.values():
            _finite_tree(item, depth=depth + 1)
    elif isinstance(value, list):
        for item in value:
            _finite_tree(item, depth=depth + 1)
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise EvidenceError("evidence contains a non-JSON value")


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
    return raw, after


def _inside(path, root):
    path, root = Path(path).resolve(), Path(root).resolve()
    return path == root or root in path.parents


def _direct_child(path, root):
    path, root = Path(path).resolve(), Path(root).resolve()
    if path.parent != root:
        raise EvidenceError("evidence path is not a direct Run Logs child")
    return path


def _manifest_fingerprint(root, ticker):
    root = Path(root).resolve()
    ticker_dir = (root / ticker).resolve()
    path = (ticker_dir / storage.MANIFEST_NAME).resolve()
    try:
        if ticker_dir.parent != root or path.parent != ticker_dir:
            raise EvidenceError(f"manifest path escapes the bank for {ticker}")
    except OSError as exc:
        raise EvidenceError(f"cannot resolve manifest for {ticker}") from exc
    raw, stat = _read_stable_bytes(path, MAX_MANIFEST_BYTES)
    manifest = _load_json(raw, path.name)
    _finite_tree(manifest)
    if (manifest.get("folder") or ticker) != ticker:
        raise EvidenceError(f"manifest folder does not match {ticker}")
    intervals = manifest.get("intervals")
    if not isinstance(intervals, dict):
        raise EvidenceError(f"manifest intervals are invalid for {ticker}")
    for token, item in intervals.items():
        if (not isinstance(token, str)
                or not storage.INTERVAL_RE.fullmatch(token)
                or not isinstance(item, dict)
                or not isinstance(item.get("months"), dict)
                or not isinstance(item.get("verified_absent", []), list)
                or any(not isinstance(month, str)
                       or not _MONTH_RE.fullmatch(month)
                       or not isinstance(entry, dict)
                       for month, entry in item["months"].items())):
            raise EvidenceError(f"manifest series shape is invalid for {ticker}")
    return {
        "ticker": ticker,
        "sha256": _sha256(raw),
        "bytes": len(raw),
        "mtime_ns": stat.st_mtime_ns,
    }


def _normalize_batch(batch, now):
    if not isinstance(batch, dict):
        raise ExportQualityError("batch must be an object")
    run_id = _input_text(batch.get("run_id"), "batch run id", limit=80)
    if not _RUN_ID_RE.fullmatch(run_id):
        raise ExportQualityError("invalid batch run id")
    fmt = _input_text(batch.get("format"), "batch format", limit=32)
    if not _TOKEN_RE.fullmatch(fmt):
        raise ExportQualityError("invalid batch format")
    interval = _input_text(batch.get("interval"), "batch interval", limit=32)
    if not storage.INTERVAL_RE.fullmatch(interval):
        raise ExportQualityError("invalid batch interval")
    state = _input_text(batch.get("state"), "batch state", limit=16).upper()
    if state not in {"COMPLETE", "PARTIAL", "CANCELLED", "FAILED"}:
        raise ExportQualityError("invalid batch state")
    destination = _input_text(
        batch.get("destination"), "batch destination", limit=MAX_PATH)
    return {
        "run_id": run_id,
        "generated_at": _generated_timestamp(now),
        "format": fmt.lower(),
        "interval": interval,
        "sessions": _input_text(
            batch.get("sessions") or "RTH", "batch sessions", limit=64),
        "start": _input_text(
            batch.get("start"), "batch start", limit=32, allow_empty=True),
        "end": _input_text(
            batch.get("end"), "batch end", limit=32, allow_empty=True),
        "destination": destination,
        "state": state,
    }


def _normalize_selection(selected, present, outcomes):
    if not isinstance(selected, (list, tuple)):
        raise ExportQualityError("selected tickers must be a list")
    names = [_canonical_ticker(value) for value in selected]
    if not names or len(names) > MAX_TICKERS or len(set(names)) != len(names):
        raise ExportQualityError("selected tickers are empty, duplicated, or too many")
    names = sorted(names)
    present_set = {_canonical_ticker(value) for value in (present or [])}
    if not present_set.issubset(names):
        raise ExportQualityError("present tickers are not a selected subset")
    if not isinstance(outcomes, dict):
        raise ExportQualityError("outcomes must be an object")
    normalized_outcomes = {}
    for raw_ticker, value in outcomes.items():
        ticker = _canonical_ticker(raw_ticker)
        if ticker not in names or ticker in normalized_outcomes:
            raise ExportQualityError("outcome ticker is invalid or duplicated")
        if not isinstance(value, dict):
            raise ExportQualityError(f"outcome is invalid for {ticker}")
        normalized_outcomes[ticker] = value
    return names, present_set, normalized_outcomes


def _nonnegative_int(value, label, default=0):
    if value is None:
        return default
    if isinstance(value, bool):
        raise ExportQualityError(f"invalid {label}")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and value.isascii() and value.isdigit():
        number = int(value)
    else:
        raise ExportQualityError(f"invalid {label}")
    if number < 0:
        raise ExportQualityError(f"invalid {label}")
    return number


def _evidence_int(value, label, *, maximum=1_000_000):
    if (isinstance(value, bool) or not isinstance(value, int)
            or value < 0 or value > maximum):
        raise EvidenceError(f"invalid {label}")
    return value


def _evidence_number(value, label, *, optional=False):
    if value is None and optional:
        return None
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value)):
        raise EvidenceError(f"invalid {label}")
    return value


def _evidence_ticker(value, label):
    if not isinstance(value, str):
        raise EvidenceError(f"invalid {label}")
    try:
        ticker = storage.canonical_ticker(value)
    except Exception as exc:  # noqa: BLE001 - normalize storage validation
        raise EvidenceError(f"invalid {label}") from exc
    if value != ticker:
        raise EvidenceError(f"non-canonical {label}")
    return ticker


def _read_sidecar_map(root, basename, max_bytes, schema_version):
    root = Path(root).resolve()
    path = (root / basename).resolve()
    if path.parent != root:
        raise EvidenceError(f"{basename} escapes the storage root")
    raw, _stat = _read_stable_bytes(path, max_bytes)
    payload = _load_json(raw, basename)
    _finite_tree(payload)
    if len(payload) > MAX_SIDECAR_ENTRIES:
        raise EvidenceError(f"{basename} has too many entries")
    return payload, {
        "basename": basename,
        "sha256": _sha256(raw),
        "bytes": len(raw),
        "schema_version": schema_version,
        "entries": len(payload),
    }


def _validate_interval_provenance(value, label,
                                  observed_statuses=XVAL_OBSERVED_STATUSES):
    if not isinstance(value, dict):
        raise EvidenceError(f"invalid {label}")
    _finite_tree(value)
    if (value.get("schema_version") != storage.INTERVAL_FINGERPRINT_VERSION
            or value.get("algorithm") != "sha256"
            or not isinstance(value.get("current"), bool)):
        raise EvidenceError(f"invalid {label} contract")
    before = value.get("before_sha256")
    after = value.get("after_sha256")
    current_sha = value.get("sha256")
    if any(item is not None and not _valid_sha(item)
           for item in (before, after, current_sha)):
        raise EvidenceError(f"invalid {label} hash")
    current = value["current"]
    present = value.get("present")
    backfill_incomplete = value.get("backfill_incomplete")
    if (present is not None and not isinstance(present, bool)):
        raise EvidenceError(f"invalid {label} presence state")
    if (backfill_incomplete is not None
            and not isinstance(backfill_incomplete, bool)):
        raise EvidenceError(f"invalid {label} backfill state")
    if current:
        if not (_valid_sha(before) and before == after == current_sha):
            raise EvidenceError(f"inconsistent current {label}")
        if not isinstance(present, bool) or not isinstance(
                backfill_incomplete, bool):
            raise EvidenceError(f"current {label} lacks canonical state")
        if "observed_status" in value:
            raise EvidenceError(f"current {label} has an observed status")
    elif current_sha is not None:
        raise EvidenceError(f"non-current {label} publishes a current hash")
    for key in ("month_count", "verified_absent_count"):
        item = value.get(key)
        if item is not None:
            _evidence_int(item, f"{label} {key}")
    observed = value.get("observed_status")
    if observed is not None and observed not in observed_statuses:
        raise EvidenceError(f"invalid {label} observed status")
    error = value.get("error")
    if error is not None and (not isinstance(error, str) or not error.strip()
                              or len(error) > MAX_REASON):
        raise EvidenceError(f"invalid {label} error")
    return {
        "current": current,
        "sha256": str(current_sha).lower() if current_sha else None,
        "present": present,
        "backfill_incomplete": backfill_incomplete,
        "observed_status": observed,
        "error": _bounded(error, MAX_REASON) or None,
    }


def _current_interval_sha(root, ticker, interval, *, allow_missing=False):
    try:
        fingerprint = (storage.optional_interval_state_fingerprint
                       if allow_missing else storage.interval_state_fingerprint)
        value = fingerprint(root, ticker, interval)
    except Exception as exc:  # noqa: BLE001 - stale evidence is report data
        raise EvidenceError(
            f"cannot fingerprint {ticker} {interval}: {type(exc).__name__}: {exc}") from exc
    if (not isinstance(value, dict)
            or value.get("schema_version") != storage.INTERVAL_FINGERPRINT_VERSION
            or value.get("algorithm") != "sha256"
            or value.get("ticker") != ticker
            or value.get("interval") != interval
            or not isinstance(value.get("present"), bool)
            or (not allow_missing and value.get("present") is not True)
            or not isinstance(value.get("backfill_incomplete"), bool)
            or not _valid_sha(value.get("sha256"))):
        raise EvidenceError(f"invalid current fingerprint for {ticker} {interval}")
    return str(value["sha256"]).lower()


def _validate_xval_counts(value, rows):
    if not isinstance(value, dict):
        raise EvidenceError("invalid cross-validation evidence counts")
    allowed = {
        "row_count", "persisted_row_count", "row_cap", "truncated",
        "distinct_dates", "field_counts",
    }
    if set(value) != allowed or not isinstance(value.get("truncated"), bool):
        raise EvidenceError("invalid cross-validation evidence-count shape")
    row_count = _evidence_int(value["row_count"], "cross-validation row count")
    persisted = _evidence_int(
        value["persisted_row_count"], "cross-validation persisted row count",
        maximum=MAX_XVAL_ROWS)
    row_cap = _evidence_int(
        value["row_cap"], "cross-validation row cap", maximum=MAX_XVAL_ROWS)
    distinct = _evidence_int(
        value["distinct_dates"], "cross-validation distinct-date count")
    fields = value.get("field_counts")
    if (not isinstance(fields, dict) or len(fields) > len(XVAL_FIELDS)
            or any(field not in XVAL_FIELDS for field in fields)):
        raise EvidenceError("invalid cross-validation field counts")
    normalized_fields = {
        field: _evidence_int(count, f"cross-validation {field} count")
        for field, count in fields.items()
    }
    if (persisted != len(rows) or row_cap != MAX_XVAL_ROWS
            or row_count < persisted or distinct > row_count
            or sum(normalized_fields.values()) != row_count
            or value["truncated"] != (row_count > persisted)):
        raise EvidenceError("inconsistent cross-validation evidence counts")
    observed_fields = {}
    observed_dates = set()
    for row in rows:
        observed_fields[row["field"]] = observed_fields.get(row["field"], 0) + 1
        if row["date"]:
            observed_dates.add(row["date"])
    if (len(observed_dates) > distinct
            or any(normalized_fields.get(field, 0) < count
                   for field, count in observed_fields.items())):
        raise EvidenceError("cross-validation counts contradict persisted rows")
    return {
        "row_count": row_count,
        "persisted_row_count": persisted,
        "distinct_dates": distinct,
        "field_counts": normalized_fields,
        "truncated": value["truncated"],
    }


def _validate_reference_coverage(value, requested_range, status):
    if value is None:
        if status in {"validated", "discrepancy"}:
            raise EvidenceError(
                "current cross-validation verdict lacks received-range evidence")
        return None
    if not isinstance(value, dict):
        raise EvidenceError("invalid cross-validation reference coverage")
    _finite_tree(value)
    allowed = {
        "reference_first_date", "reference_last_date", "reference_day_count",
        "stored_first_date", "stored_last_date", "stored_day_count",
        "full_history_requested", "head_gap_days", "head_tolerance_days",
        "full_history_head_ok",
    }
    if set(value) != allowed:
        raise EvidenceError("invalid cross-validation reference coverage fields")
    reference_first = _iso_date(
        value["reference_first_date"], "reference first date")
    reference_last = _iso_date(
        value["reference_last_date"], "reference last date")
    stored_first = _iso_date(value["stored_first_date"], "stored first date")
    stored_last = _iso_date(value["stored_last_date"], "stored last date")
    if reference_first > reference_last or stored_first > stored_last:
        raise EvidenceError("reversed cross-validation reference coverage")
    reference_days = _evidence_int(
        value["reference_day_count"], "reference day count")
    stored_days = _evidence_int(value["stored_day_count"], "stored day count")
    if reference_days == 0 or stored_days == 0:
        raise EvidenceError("empty cross-validation reference coverage")
    full_requested = value["full_history_requested"]
    expected_full = requested_range.casefold() == "max"
    if not isinstance(full_requested, bool) or full_requested != expected_full:
        raise EvidenceError("inconsistent full-history request state")
    tolerance = _evidence_int(
        value["head_tolerance_days"], "reference head tolerance")
    if tolerance != XVAL_FULL_HISTORY_HEAD_TOLERANCE_DAYS:
        raise EvidenceError("unsupported reference head tolerance")
    expected_gap = max(
        0,
        (dt.date.fromisoformat(reference_first)
         - dt.date.fromisoformat(stored_first)).days,
    )
    gap = _evidence_int(value["head_gap_days"], "reference head gap")
    if gap != expected_gap:
        raise EvidenceError("inconsistent reference head gap")
    head_ok = value["full_history_head_ok"]
    expected_ok = gap <= tolerance if full_requested else None
    if head_ok is not expected_ok:
        raise EvidenceError("inconsistent full-history head verdict")
    if full_requested and not head_ok and status in {"validated", "discrepancy"}:
        raise EvidenceError("short full-history reference published a verdict")
    return {
        "reference_first_date": reference_first,
        "reference_last_date": reference_last,
        "reference_day_count": reference_days,
        "stored_first_date": stored_first,
        "stored_last_date": stored_last,
        "stored_day_count": stored_days,
        "full_history_requested": full_requested,
        "head_gap_days": gap,
        "head_tolerance_days": tolerance,
        "full_history_head_ok": head_ok,
    }


def _validate_xval_entry(key, value):
    if not isinstance(key, str) or not isinstance(value, dict):
        raise EvidenceError("invalid cross-validation entry")
    _finite_tree(value)
    if value.get("schema_version") != XVAL_SCHEMA_VERSION:
        raise EvidenceError("unsupported cross-validation schema")
    ticker = _evidence_ticker(value.get("ticker"), "cross-validation ticker")
    interval = value.get("interval")
    if (not isinstance(interval, str)
            or not storage.INTERVAL_RE.fullmatch(interval)
            or key != f"{ticker} {interval}"):
        raise EvidenceError("cross-validation key/series mismatch")
    status = value.get("status")
    if status not in XVAL_STATUSES:
        raise EvidenceError("invalid cross-validation status")
    provider = value.get("provider")
    if provider not in {"stockanalysis", "internal-structural"}:
        raise EvidenceError("invalid cross-validation provider")
    requested_range = value.get("requested_range")
    if provider == "stockanalysis":
        if (not isinstance(requested_range, str)
                or not _TOKEN_RE.fullmatch(requested_range)):
            raise EvidenceError("invalid cross-validation requested range")
    elif requested_range is not None:
        raise EvidenceError("structural validation has a requested range")
    reference_coverage = value.get("reference_coverage")
    if provider == "stockanalysis":
        reference_coverage = _validate_reference_coverage(
            reference_coverage, requested_range, status)
    elif reference_coverage is not None:
        raise EvidenceError("structural validation has reference coverage")
    asof = _evidence_timestamp(value.get("asof"), "cross-validation as-of")
    started = _evidence_timestamp(
        value.get("started_at"), "cross-validation started-at")
    finished = _evidence_timestamp(
        value.get("finished_at"), "cross-validation finished-at")
    if (_parse_timestamp(finished, "cross-validation finished-at")
            < _parse_timestamp(started, "cross-validation started-at")):
        raise EvidenceError("cross-validation finish precedes start")
    note = value.get("note")
    if not isinstance(note, str) or len(note) > MAX_REASON * 4:
        raise EvidenceError("invalid cross-validation note")
    provenance = _validate_interval_provenance(
        value.get("interval_fingerprint"), "cross-validation provenance")
    reference_interval = value.get("reference_interval")
    reference_provenance = value.get("reference_interval_fingerprint")
    needs_reference = (provider == "internal-structural"
                       and storage.base_interval(interval) != "1d")
    if needs_reference:
        expected_reference = storage.with_kind("1d", storage.kind_of(interval))
        if reference_interval != expected_reference:
            raise EvidenceError("invalid structural reference interval")
        reference_provenance = _validate_interval_provenance(
            reference_provenance,
            "cross-validation structural reference provenance")
    elif reference_interval is not None or reference_provenance is not None:
        raise EvidenceError("unexpected cross-validation reference provenance")

    detail = value.get("detail")
    if detail is None:
        detail = {}
    if not isinstance(detail, dict):
        raise EvidenceError("invalid cross-validation detail")
    raw_rows = detail.get("rows", [])
    if not isinstance(raw_rows, list) or len(raw_rows) > MAX_XVAL_ROWS:
        raise EvidenceError("invalid cross-validation detail rows")
    rows = []
    for raw in raw_rows:
        if not isinstance(raw, dict):
            raise EvidenceError("invalid cross-validation detail row")
        field = raw.get("field")
        if field not in XVAL_FIELDS or field == "other":
            raise EvidenceError("invalid cross-validation detail field")
        raw_date = raw.get("date")
        if provider == "internal-structural" and field == "bar" and not raw_date:
            row_date = ""
        else:
            row_date = _iso_date(raw_date, "cross-validation row date")
        optional_values = provider == "internal-structural"
        row = {
            "date": row_date,
            "field": field,
            "derived": _evidence_number(
                raw.get("derived"), "cross-validation derived value",
                optional=optional_values),
            "reference": _evidence_number(
                raw.get("reference"), "cross-validation reference value",
                optional=optional_values),
            "percent": _evidence_number(
                raw.get("percent"), "cross-validation percent",
                optional=optional_values),
        }
        if row["percent"] is not None and row["percent"] < 0:
            raise EvidenceError("negative cross-validation percent")
        rows.append(row)
    counts = _validate_xval_counts(value.get("evidence_counts"), rows)

    level_shift = detail.get("level_shift")
    level_shift_date = None
    level_shift_factor = None
    if level_shift is not None:
        if not isinstance(level_shift, dict):
            raise EvidenceError("invalid cross-validation level shift")
        _finite_tree(level_shift)
        if level_shift.get("date") is not None:
            level_shift_date = _iso_date(
                level_shift["date"], "cross-validation level-shift date")
        for field in ("factor", "pct", "trail", "lead"):
            if level_shift.get(field) is not None:
                number = _evidence_number(
                    level_shift[field], f"cross-validation level-shift {field}")
                if field == "factor":
                    level_shift_factor = number
    if level_shift_factor is not None and level_shift_factor <= 0:
        raise EvidenceError("invalid cross-validation level-shift factor")
    if ((level_shift is not None)
            != bool(counts["field_counts"].get("level_shift", 0))):
        raise EvidenceError("inconsistent cross-validation level-shift evidence")
    suspected = _evidence_number(
        detail.get("suspected_factor"), "cross-validation suspected factor",
        optional=True)
    checked_raw = detail.get("checked")
    has_external_price_evidence = (
        provider == "stockanalysis"
        and (any(counts["field_counts"].get(field, 0)
                 for field in XVAL_PRICE_FIELDS)
             or level_shift is not None or suspected is not None)
    )
    if checked_raw is None:
        if has_external_price_evidence:
            raise EvidenceError(
                "cross-validation price evidence lacks checked-day count")
        checked = 0
    else:
        checked = _evidence_int(
            checked_raw, "cross-validation checked-day count")
    if has_external_price_evidence:
        if checked == 0 or counts["distinct_dates"] > checked:
            raise EvidenceError(
                "inconsistent cross-validation checked-day count")
        if (reference_coverage is not None
                and checked > min(
                    reference_coverage["reference_day_count"],
                    reference_coverage["stored_day_count"])):
            raise EvidenceError(
                "cross-validation checked days exceed received coverage")
    price_row_count = sum(
        counts["field_counts"].get(field, 0) for field in XVAL_OHLC_FIELDS)
    provenances = [provenance]
    if reference_provenance is not None:
        provenances.append(reference_provenance)
    if status == "discrepancy" and any(
            not item["current"] for item in provenances):
        raise EvidenceError("published discrepancy has non-current provenance")
    if (any(not item["current"] for item in provenances)
            and status != "inconclusive"):
        raise EvidenceError("non-current provenance is not inconclusive")
    return {
        "ticker": ticker,
        "interval": interval,
        "status": status,
        "provider": provider,
        "requested_range": requested_range,
        "reference_coverage": reference_coverage,
        "asof": asof,
        "finished_at": finished,
        "note": _bounded(note, MAX_REASON),
        "provenance": provenance,
        "reference_interval": reference_interval,
        "reference_provenance": reference_provenance,
        "counts": counts,
        "rows": rows,
        "level_shift": level_shift is not None,
        "level_shift_date": level_shift_date,
        "level_shift_factor": level_shift_factor,
        "suspected_factor": suspected,
        "checked_days": checked,
        "price_row_count": price_row_count,
    }


def _xval_evidence(entry, *, diagnostic=False):
    fields = [
        field for field in XVAL_FIELDS
        if entry["counts"]["field_counts"].get(field, 0)
    ]
    field_set = set(fields)
    price_fields = field_set & XVAL_PRICE_FIELDS
    price = bool(price_fields
                  or entry["level_shift"]
                  or entry["suspected_factor"] is not None)
    checked = entry["checked_days"]
    price_rows = entry["price_row_count"]
    density_eligible = checked >= XVAL_DENSE_MIN_CHECKED_DAYS
    dense_price = bool(
        density_eligible
        and price_rows * 100 > checked * XVAL_DENSE_PRICE_PERCENT)
    density_pct = round(100 * price_rows / checked, 4) if checked else 0.0
    event_key = (entry["ticker"], entry["level_shift_date"])
    known_action = bool(
        entry["level_shift"]
        and entry["level_shift_factor"] is not None
        and event_key in XVAL_KNOWN_CORPORATE_ACTIONS)
    unexplained_basis = bool(
        entry["suspected_factor"] is not None
        or (entry["level_shift"] and not known_action))
    volume_only = field_set == {"volume"}
    auction = (not entry["level_shift"]
               and entry["suspected_factor"] is None
               and "open" in price_fields
               and price_fields.issubset({"open", "low"}))
    dates = sorted({row["date"] for row in entry["rows"]})
    if diagnostic:
        code = "noncurrent_observed_discrepancy"
        impact = "diagnostic"
    elif unexplained_basis:
        code = "unexplained_price_basis"
        impact = "review"
    elif dense_price:
        code = "dense_price_discrepancy"
        impact = "review"
    elif known_action:
        code = "known_corporate_action_basis"
        impact = "known_difference"
    elif price:
        code = "sparse_price_discrepancy"
        impact = "informational"
    elif volume_only:
        code = "volume_only_discrepancy"
        impact = "informational"
    else:
        code = "nonprice_discrepancy"
        impact = "informational"
    field_label = ",".join(fields) or "uniform-level"
    summary = (
        f"{entry['interval']} cross-validation reports {field_label} on "
        f"{entry['counts']['distinct_dates']} distinct day(s)"
    )
    if known_action:
        summary += (f"; level shift {entry['level_shift_date']} matches the "
                    "reviewed corporate-action basis catalog")
    elif entry["level_shift"]:
        summary += (f"; level shift {entry['level_shift_date'] or 'without a date'} "
                    "has no reviewed corporate-action match")
    if entry["suspected_factor"] is not None:
        summary += "; suspected uniform price factor has no dated catalog match"
    if dense_price:
        summary += (f"; {price_rows}/{checked} price rows ({density_pct:g}%) "
                    f"exceed the {XVAL_DENSE_PRICE_PERCENT}% density gate")
    if auction:
        summary += " (auction candidate)"
    if diagnostic:
        summary = "Non-current observed discrepancy: " + summary
    return {
        "source": "cross_validation",
        "code": code,
        "interval": entry["interval"],
        "fields": fields,
        "distinct_days": entry["counts"]["distinct_dates"],
        "example_dates": dates[:MAX_EXAMPLE_DATES],
        "_all_dates": dates,
        "asof": entry["finished_at"],
        "current": not diagnostic,
        "diagnostic": diagnostic,
        "verdict_affecting": impact == "review",
        "auction_candidate": auction,
        "price_row_count": price_rows,
        "checked_days": checked,
        "price_row_density_pct": density_pct,
        "density_eligible": density_eligible,
        "dense_price_evidence": dense_price,
        "level_shift_date": entry["level_shift_date"],
        "level_shift_factor": entry["level_shift_factor"],
        "suspected_factor": entry["suspected_factor"],
        "known_corporate_action": known_action,
        "known_difference": impact == "known_difference",
        "_price_evidence": price,
        "three_source_checked": False,
        "three_source_persistent": False,
        "three_source_dates": [],
        "fingerprint": entry["provenance"]["sha256"],
        "reference_interval": entry.get("reference_interval"),
        "reference_fingerprint": (
            (entry.get("reference_provenance") or {}).get("sha256")),
        "summary": _bounded(summary, MAX_REASON),
        "impact": impact,
    }


def _load_cross_validation(root, selected):
    rows = {ticker: [] for ticker in selected}
    errors = []
    try:
        payload, source = _read_sidecar_map(
            root, XVAL_BASENAME, MAX_XVAL_BYTES, XVAL_SCHEMA_VERSION)
    except Exception as exc:  # noqa: BLE001 - source failure is diagnostics
        return {
            "rows": rows,
            "source": None,
            "errors": [_bounded(
                f"{type(exc).__name__}: {exc}", MAX_REASON)],
        }
    stats = {
        "current_entries": 0,
        "current_discrepancies": 0,
        "current_volume_only": 0,
        "current_price_review": 0,
        "current_price_informational": 0,
        "current_known_difference": 0,
        "diagnostic_entries": 0,
        "ignored_legacy": 0,
        "ignored_stale": 0,
        "ignored_malformed": 0,
        "ignored_nonselected": 0,
    }
    latest = None
    selected = set(selected)
    for key, raw_entry in sorted(payload.items()):
        schema = raw_entry.get("schema_version") if isinstance(raw_entry, dict) else None
        if schema is None or (isinstance(schema, int) and schema < XVAL_SCHEMA_VERSION):
            stats["ignored_legacy"] += 1
            continue
        try:
            entry = _validate_xval_entry(key, raw_entry)
        except Exception as exc:  # noqa: BLE001 - one entry fails closed
            stats["ignored_malformed"] += 1
            if len(errors) < MAX_SOURCE_ERRORS:
                errors.append(_bounded(
                    f"{key}: {type(exc).__name__}: {exc}", MAX_REASON))
            continue
        stamp = _parse_timestamp(
            entry["finished_at"], "cross-validation finished-at")
        if latest is None or stamp > latest[0]:
            latest = (stamp, entry["finished_at"])
        ticker = entry["ticker"]
        if ticker not in selected:
            stats["ignored_nonselected"] += 1
            continue
        provenance = entry["provenance"]
        provenances = [(entry["interval"], provenance, False)]
        if entry.get("reference_provenance") is not None:
            provenances.append((entry["reference_interval"],
                                entry["reference_provenance"], True))
        if any(not item[1]["current"] for item in provenances):
            stats["ignored_stale"] += 1
            observed = next((item[1].get("observed_status")
                             for item in provenances
                             if not item[1]["current"]), None)
            if (entry["status"] == "inconclusive"
                    and observed == "discrepancy"):
                rows[ticker].append(_xval_evidence(entry, diagnostic=True))
                stats["diagnostic_entries"] += 1
            continue
        try:
            current_shas = {
                interval_token: _current_interval_sha(
                    root, ticker, interval_token, allow_missing=allow_missing)
                for interval_token, _item, allow_missing in provenances
            }
        except Exception as exc:  # noqa: BLE001 - no stale verdict effect
            stats["ignored_stale"] += 1
            if len(errors) < MAX_SOURCE_ERRORS:
                errors.append(_bounded(
                    f"{key}: {type(exc).__name__}: {exc}", MAX_REASON))
            continue
        if any(current_shas[interval_token] != item["sha256"]
               for interval_token, item, _allow_missing in provenances):
            stats["ignored_stale"] += 1
            continue
        stats["current_entries"] += 1
        if entry["status"] == "discrepancy":
            evidence = _xval_evidence(entry)
            rows[ticker].append(evidence)
            stats["current_discrepancies"] += 1
            if evidence["code"] == "volume_only_discrepancy":
                stats["current_volume_only"] += 1
            elif evidence["impact"] == "known_difference":
                stats["current_known_difference"] += 1
            elif evidence.get("_price_evidence"):
                key = ("current_price_review" if evidence["verdict_affecting"]
                       else "current_price_informational")
                stats[key] += 1
    for ticker in rows:
        ordered = sorted(
            rows[ticker],
            key=lambda item: (
                not item["verdict_affecting"], item["diagnostic"],
                item["interval"], item["code"]),
        )
        selected_rows = ordered[:MAX_XVAL_EVIDENCE_PER_TICKER]
        diagnostics = [item for item in ordered if item["diagnostic"]]
        if (diagnostics and selected_rows
                and not any(item["diagnostic"] for item in selected_rows)):
            selected_rows[-1] = diagnostics[0]
        rows[ticker] = selected_rows
    source.update(stats)
    source["latest_finished_at"] = latest[1] if latest else None
    return {"rows": rows, "source": source, "errors": errors}


def _validate_date_list(value, label):
    if not isinstance(value, list) or len(value) > MAX_XVAL_ROWS:
        raise EvidenceError(f"invalid {label}")
    dates = [_iso_date(item, label) for item in value]
    if len(dates) != len(set(dates)):
        raise EvidenceError(f"duplicate {label}")
    return sorted(dates)


def _validate_combined_coverage_item(value, label, *, internal=False):
    if value is None:
        return None
    if not isinstance(value, dict):
        raise EvidenceError(f"invalid combined {label} coverage")
    allowed = {
        "first_date", "last_date", "day_count",
        "derived_first_date", "derived_last_date", "derived_day_count",
        "head_gap_days", "head_tolerance_days", "head_ok",
        "requested_range",
    }
    if set(value) != allowed:
        raise EvidenceError(f"invalid combined {label} coverage fields")
    first = _iso_date(value["first_date"], f"combined {label} first date")
    last = _iso_date(value["last_date"], f"combined {label} last date")
    derived_first = _iso_date(
        value["derived_first_date"], f"combined {label} derived first date")
    derived_last = _iso_date(
        value["derived_last_date"], f"combined {label} derived last date")
    if first > last or derived_first > derived_last:
        raise EvidenceError(f"reversed combined {label} coverage")
    day_count = _evidence_int(
        value["day_count"], f"combined {label} day count")
    derived_count = _evidence_int(
        value["derived_day_count"], f"combined {label} derived day count")
    reference_span = (
        dt.date.fromisoformat(last) - dt.date.fromisoformat(first)).days + 1
    derived_span = (
        dt.date.fromisoformat(derived_last)
        - dt.date.fromisoformat(derived_first)).days + 1
    if (day_count == 0 or derived_count == 0
            or day_count > reference_span
            or derived_count > derived_span):
        raise EvidenceError(f"invalid combined {label} coverage counts")
    tolerance = _evidence_int(
        value["head_tolerance_days"],
        f"combined {label} head tolerance")
    if tolerance != XVAL_FULL_HISTORY_HEAD_TOLERANCE_DAYS:
        raise EvidenceError(f"unsupported combined {label} head tolerance")
    requested_range = value["requested_range"]
    if internal:
        if requested_range is not None:
            raise EvidenceError("combined internal coverage has a range")
    elif (not isinstance(requested_range, str)
          or not _TOKEN_RE.fullmatch(requested_range)):
        raise EvidenceError("invalid combined external requested range")
    expected_gap = max(
        0,
        (dt.date.fromisoformat(first)
         - dt.date.fromisoformat(derived_first)).days,
    )
    gap = _evidence_int(
        value["head_gap_days"], f"combined {label} head gap")
    head_ok = value["head_ok"]
    full_history = internal or requested_range.casefold() == "max"
    expected_head_ok = (gap <= tolerance) if full_history else None
    if gap != expected_gap or head_ok is not expected_head_ok:
        raise EvidenceError(f"inconsistent combined {label} head verdict")
    return {
        "first_date": first,
        "last_date": last,
        "day_count": day_count,
        "derived_first_date": derived_first,
        "derived_last_date": derived_last,
        "derived_day_count": derived_count,
        "head_gap_days": gap,
        "head_tolerance_days": tolerance,
        "head_ok": head_ok,
        "requested_range": requested_range,
    }


def _validate_combined_coverage(value, status):
    if not isinstance(value, dict) or set(value) != {"external", "internal"}:
        raise EvidenceError("invalid combined reference coverage")
    external = _validate_combined_coverage_item(
        value["external"], "external")
    internal = _validate_combined_coverage_item(
        value["internal"], "internal", internal=True)
    if status in {"flagged", "ok"} and (
            external is None or internal is None):
        raise EvidenceError(
            "combined verdict lacks both reference coverages")
    if status == "ok" and (not external["head_ok"]
                            or not internal["head_ok"]):
        raise EvidenceError("combined ok verdict lacks full reference coverage")
    return {"external": external, "internal": internal}


def _validate_combined_entry(key, value):
    if not isinstance(key, str) or not isinstance(value, dict):
        raise EvidenceError("invalid combined-flags entry")
    _finite_tree(value)
    if value.get("schema_version") != COMBINED_SCHEMA_VERSION:
        raise EvidenceError("unsupported combined-flags schema")
    ticker = _evidence_ticker(value.get("ticker"), "combined-flags ticker")
    interval = value.get("interval")
    internal_interval = value.get("internal_interval")
    if (not isinstance(interval, str)
            or not storage.INTERVAL_RE.fullmatch(interval)):
        raise EvidenceError("combined-flags key/series mismatch")
    if key != f"{ticker} {interval}":
        raise EvidenceError("combined-flags key/series mismatch")
    if (internal_interval != COMBINED_INTERNAL_INTERVAL
            or interval == internal_interval):
        raise EvidenceError("invalid combined-flags internal interval")
    if value.get("source") != "3-source":
        raise EvidenceError("invalid combined-flags source")
    status = value.get("status")
    if status not in COMBINED_STATUSES:
        raise EvidenceError("invalid combined-flags status")
    asof = _evidence_timestamp(value.get("asof"), "combined-flags as-of")
    started = _evidence_timestamp(
        value.get("started_at"), "combined-flags started-at")
    finished = _evidence_timestamp(
        value.get("finished_at"), "combined-flags finished-at")
    if (_parse_timestamp(finished, "combined-flags finished-at")
            < _parse_timestamp(started, "combined-flags started-at")):
        raise EvidenceError("combined-flags finish precedes start")
    reference_coverage = _validate_combined_coverage(
        value.get("reference_coverage"), status)
    coverage_error = value.get("reference_coverage_error")
    if coverage_error is not None and (
            not isinstance(coverage_error, str) or not coverage_error
            or len(coverage_error) > MAX_REASON
            or any(ch in coverage_error for ch in "\r\n\x00")):
        raise EvidenceError("invalid combined reference coverage error")
    if status in {"flagged", "ok"} and coverage_error is not None:
        raise EvidenceError("combined verdict has a reference coverage error")
    candidates = _validate_date_list(
        value.get("candidates", []), "combined-flags candidate date")
    persistent = _validate_date_list(
        value.get("persistent", []), "combined-flags persistent date")
    cleared = _validate_date_list(
        value.get("cleared", []), "combined-flags cleared date")
    candidate_count = _evidence_int(
        value.get("candidate_count"), "combined-flags candidate count")
    persistent_count = _evidence_int(
        value.get("persistent_count"), "combined-flags persistent count")
    cleared_count = _evidence_int(
        value.get("cleared_count"), "combined-flags cleared count")
    row_cap = _evidence_int(
        value.get("row_cap"), "combined-flags row cap",
        maximum=MAX_XVAL_ROWS)
    truncated = value.get("truncated")
    if (row_cap != MAX_XVAL_ROWS or not isinstance(truncated, bool)
            or candidate_count < len(candidates)
            or persistent_count < len(persistent)
            or cleared_count < len(cleared)
            or candidate_count < persistent_count + cleared_count
            or truncated != any((candidate_count > len(candidates),
                                 persistent_count > len(persistent),
                                 cleared_count > len(cleared)))):
        raise EvidenceError("inconsistent combined-flags evidence counts")
    if (set(persistent) & set(cleared)
            or not (set(persistent) | set(cleared)).issubset(candidates)
            or (status == "flagged" and not persistent_count)
            or (status == "ok" and persistent_count)):
        raise EvidenceError("inconsistent combined-flags dates/status")
    note = value.get("note")
    if note is not None and (not isinstance(note, str)
                             or not note or len(note) > MAX_REASON):
        raise EvidenceError("invalid combined-flags note")
    provenance = _validate_interval_provenance(
        value.get("interval_fingerprint"), "combined-flags provenance",
        observed_statuses=COMBINED_STATUSES)
    internal_provenance = _validate_interval_provenance(
        value.get("internal_interval_fingerprint"),
        "combined-flags internal provenance",
        observed_statuses=COMBINED_STATUSES)
    return {
        "ticker": ticker,
        "interval": interval,
        "internal_interval": internal_interval,
        "status": status,
        "asof": asof,
        "started_at": started,
        "finished_at": finished,
        "reference_coverage": reference_coverage,
        "reference_coverage_error": coverage_error,
        "persistent": persistent,
        "persistent_count": persistent_count,
        "truncated": truncated,
        "provenance": provenance,
        "internal_provenance": internal_provenance,
    }


def _load_combined_flags(root, selected):
    rows = {ticker: [] for ticker in selected}
    errors = []
    try:
        payload, source = _read_sidecar_map(
            root, COMBINED_BASENAME, MAX_COMBINED_BYTES,
            COMBINED_SCHEMA_VERSION)
    except Exception as exc:  # noqa: BLE001 - source failure is diagnostics
        return {
            "rows": rows,
            "source": None,
            "errors": [_bounded(
                f"{type(exc).__name__}: {exc}", MAX_REASON)],
        }
    stats = {
        "current_entries": 0,
        "current_ok": 0,
        "current_flagged": 0,
        "current_error": 0,
        "current_inconclusive": 0,
        "ignored_legacy": 0,
        "ignored_stale": 0,
        "ignored_malformed": 0,
        "ignored_nonselected": 0,
    }
    latest = None
    selected = set(selected)
    for key, raw_entry in sorted(payload.items()):
        schema = raw_entry.get("schema_version") if isinstance(raw_entry, dict) else None
        if schema is None or (isinstance(schema, int)
                              and schema < COMBINED_SCHEMA_VERSION):
            stats["ignored_legacy"] += 1
            continue
        try:
            entry = _validate_combined_entry(key, raw_entry)
        except Exception as exc:  # noqa: BLE001 - one entry fails closed
            stats["ignored_malformed"] += 1
            if len(errors) < MAX_SOURCE_ERRORS:
                errors.append(_bounded(
                    f"{key}: {type(exc).__name__}: {exc}", MAX_REASON))
            continue
        stamp = _parse_timestamp(entry["asof"], "combined-flags as-of")
        if latest is None or stamp > latest[0]:
            latest = (stamp, entry["asof"])
        ticker = entry["ticker"]
        if ticker not in selected:
            stats["ignored_nonselected"] += 1
            continue
        provenances = (
            (entry["interval"], entry["provenance"]),
            (entry["internal_interval"], entry["internal_provenance"]),
        )
        if any(not provenance["current"]
               for _interval, provenance in provenances):
            stats["ignored_stale"] += 1
            continue
        try:
            current_shas = {
                interval_token: _current_interval_sha(
                    root, ticker, interval_token)
                for interval_token, _provenance in provenances
            }
        except Exception as exc:  # noqa: BLE001 - no stale verdict effect
            stats["ignored_stale"] += 1
            if len(errors) < MAX_SOURCE_ERRORS:
                errors.append(_bounded(
                    f"{key}: {type(exc).__name__}: {exc}", MAX_REASON))
            continue
        if any(current_shas[interval_token] != provenance["sha256"]
               for interval_token, provenance in provenances):
            stats["ignored_stale"] += 1
            continue
        stats["current_entries"] += 1
        stats[f"current_{entry['status']}"] += 1
        if entry["status"] not in {"ok", "flagged"}:
            continue
        dates = entry["persistent"]
        item = {
            "interval": entry["interval"],
            "_all_dates": dates,
            "_emit": entry["status"] == "flagged",
        }
        if entry["status"] == "flagged":
            shown = ", ".join(dates[:MAX_EXAMPLE_DATES])
            item.update({
                "source": "combined_flags",
                "code": "three_source_persistent",
                "interval": entry["interval"],
                "fields": [],
                "distinct_days": entry["persistent_count"],
                "example_dates": dates[:MAX_EXAMPLE_DATES],
                "asof": entry["asof"],
                "current": True,
                "diagnostic": False,
                "verdict_affecting": True,
                "auction_candidate": False,
                "fingerprint": entry["provenance"]["sha256"],
                "summary": _bounded(
                    f"Current three-source disagreement persists on "
                    f"{entry['persistent_count']} day(s)"
                    + (f": {shown}" if shown else "")
                    + (" (examples capped)" if entry["truncated"] else ""),
                    MAX_REASON),
                "impact": "review",
            })
        rows[ticker].append(item)
    for ticker in rows:
        rows[ticker].sort(key=lambda item: item.get("interval") or "")
    source.update(stats)
    source["latest_asof"] = latest[1] if latest else None
    return {"rows": rows, "source": source, "errors": errors}


def _coverage_month_ord(value):
    return int(value[:4]) * 12 + int(value[5:7]) - 1


def _validate_coverage_row(value, *, unevaluated=False):
    if not isinstance(value, dict):
        raise EvidenceError("invalid per-kind coverage row")
    expected = {
        "ticker", "kind", "kind_last", "primary_last", "lag_months",
        "verdict",
    }
    if set(value) != expected:
        raise EvidenceError("invalid per-kind coverage row shape")
    ticker = _evidence_ticker(value.get("ticker"), "coverage ticker")
    interval = value.get("kind")
    if (not isinstance(interval, str)
            or not storage.INTERVAL_RE.fullmatch(interval)
            or interval == COVERAGE_PRIMARY_INTERVAL):
        raise EvidenceError("invalid per-kind coverage interval")
    kind_last = _iso_month(value.get("kind_last"), "coverage kind last")
    if unevaluated:
        if (value.get("verdict") != "UNEVALUATED"
                or value.get("primary_last") is not None
                or value.get("lag_months") is not None):
            raise EvidenceError("invalid unevaluated per-kind coverage row")
        return {
            "ticker": ticker,
            "kind": interval,
            "kind_last": kind_last,
            "primary_last": None,
            "lag_months": None,
            "verdict": "UNEVALUATED",
        }
    primary_last = _iso_month(
        value.get("primary_last"), "coverage primary last")
    lag = _evidence_int(
        value.get("lag_months"), "coverage lag months", maximum=20_000)
    if (value.get("verdict") != "KIND_FORWARD_STALE"
            or lag < COVERAGE_STALE_MONTHS
            or lag != (_coverage_month_ord(primary_last)
                       - _coverage_month_ord(kind_last))):
        raise EvidenceError("inconsistent stale per-kind coverage row")
    return {
        "ticker": ticker,
        "kind": interval,
        "kind_last": kind_last,
        "primary_last": primary_last,
        "lag_months": lag,
        "verdict": "KIND_FORWARD_STALE",
    }


def _load_coverage(root, selected):
    rows = {ticker: [] for ticker in selected}
    root = Path(root).resolve()
    path = (root / COVERAGE_BASENAME).resolve()
    try:
        if path.parent != root:
            raise EvidenceError(f"{COVERAGE_BASENAME} escapes the storage root")
        raw, stat = _read_stable_bytes(path, MAX_COVERAGE_BYTES)
        payload = _load_json(raw, COVERAGE_BASENAME)
        _finite_tree(payload)
        if (payload.get("kind") != "coverage_audit"
                or payload.get("version") != COVERAGE_SCHEMA_VERSION
                or payload.get("report_only") is not True
                or payload.get("network") is not False
                or payload.get("primary_interval")
                != COVERAGE_PRIMARY_INTERVAL):
            raise EvidenceError("unsupported coverage-audit contract")
        asof = payload.get("asof")
        if not isinstance(asof, str) or not asof.strip() or len(asof) > 64:
            raise EvidenceError("invalid coverage-audit as-of")
        thresholds = payload.get("thresholds")
        if (not isinstance(thresholds, dict)
                or thresholds.get("kind_forward_stale_months")
                != COVERAGE_STALE_MONTHS):
            raise EvidenceError("invalid per-kind coverage threshold")
        raw_stale = payload.get("kind_forward_stale")
        raw_unevaluated = payload.get("kind_unevaluated")
        if (not isinstance(raw_stale, list)
                or not isinstance(raw_unevaluated, list)
                or len(raw_stale) + len(raw_unevaluated)
                > MAX_SIDECAR_ENTRIES):
            raise EvidenceError("invalid per-kind coverage row collection")
        stale = [
            _validate_coverage_row(value) for value in raw_stale
        ]
        unevaluated = [
            _validate_coverage_row(value, unevaluated=True)
            for value in raw_unevaluated
        ]
        seen = set()
        for row in [*stale, *unevaluated]:
            key = (row["ticker"], row["kind"])
            if key in seen:
                raise EvidenceError("duplicate per-kind coverage row")
            seen.add(key)
        series_count = _evidence_int(
            payload.get("kind_series_count"), "coverage kind-series count")
        current_count = _evidence_int(
            payload.get("kind_current_count"), "coverage current-kind count")
        if series_count != current_count + len(stale) + len(unevaluated):
            raise EvidenceError("inconsistent per-kind coverage counts")
    except Exception as exc:  # noqa: BLE001 - source failure is diagnostics
        return {
            "rows": rows,
            "source": None,
            "errors": [_bounded(
                f"{type(exc).__name__}: {exc}", MAX_REASON)],
        }
    selected_set = set(selected)
    for row in stale:
        if row["ticker"] in selected_set:
            rows[row["ticker"]].append({**row, "asof": asof.strip()})
    for ticker in rows:
        rows[ticker].sort(key=lambda item: item["kind"])
    source = {
        "basename": COVERAGE_BASENAME,
        "sha256": _sha256(raw),
        "bytes": len(raw),
        "mtime_ns": stat.st_mtime_ns,
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "asof": asof.strip(),
        "kind_series": series_count,
        "kind_current": current_count,
        "kind_forward_stale": len(stale),
        "kind_unevaluated": len(unevaluated),
        "selected_stale": sum(len(value) for value in rows.values()),
        "ignored_nonselected": sum(
            row["ticker"] not in selected_set for row in stale),
    }
    return {"rows": rows, "source": source, "errors": []}


def _normalize_export_row(ticker, present, outcome, batch_state, batch_interval):
    warnings = []
    export_start = None
    export_end = None
    if not present:
        status = "NOT_IN_BANK"
        rows = 0
        file_name = None
        error = "Ticker was selected but was not present in the storage bank."
    else:
        outcome = outcome or {}
        raw_start = outcome.get("start")
        raw_end = outcome.get("end")
        if (raw_start is None) != (raw_end is None):
            raise ExportQualityError(
                f"incomplete exported range for {ticker}")
        if raw_start is not None:
            export_start = _iso_date(raw_start, f"{ticker} export start")
            export_end = _iso_date(raw_end, f"{ticker} export end")
            if export_start > export_end:
                raise ExportQualityError(
                    f"exported range is reversed for {ticker}")
        raw_status = str(outcome.get("status") or "").strip().upper()
        if not raw_status:
            raw_status = "CANCELLED" if batch_state == "CANCELLED" else "FAILED"
        status = "WRITTEN" if raw_status == "MISSING_MONTHS" else raw_status
        if status not in EXPORT_STATUSES or status == "NOT_IN_BANK":
            raise ExportQualityError(f"invalid export status for {ticker}")
        rows = _nonnegative_int(outcome.get("rows"), f"{ticker} rows")
        if status == "WRITTEN" and rows <= 0:
            raise ExportQualityError(f"WRITTEN outcome has no rows for {ticker}")
        if status != "WRITTEN" and rows:
            raise ExportQualityError(f"non-written outcome has rows for {ticker}")
        raw_file = outcome.get("file")
        file_name = None
        if raw_file:
            file_name = _input_text(raw_file, f"{ticker} file", limit=240)
            if file_name != Path(file_name).name:
                raise ExportQualityError(f"invalid export filename for {ticker}")
        if status == "WRITTEN" and not file_name:
            raise ExportQualityError(f"WRITTEN outcome has no file for {ticker}")
        if status != "WRITTEN" and file_name:
            raise ExportQualityError(f"non-written outcome has a file for {ticker}")
        error = _bounded(outcome.get("error"), MAX_REASON)
        missing = outcome.get("missing_months") or []
        if not isinstance(missing, list) or len(missing) > MAX_MISSING_MONTHS:
            raise ExportQualityError(f"invalid missing-month list for {ticker}")
        missing = [str(value) for value in missing]
        if any(not _MONTH_RE.fullmatch(value) for value in missing):
            raise ExportQualityError(f"invalid missing month for {ticker}")
        if missing and status != "WRITTEN":
            raise ExportQualityError(
                f"missing months require a WRITTEN outcome for {ticker}")
        if raw_status == "MISSING_MONTHS" and not missing:
            warnings.append("Exporter reported missing months without month details.")
        if missing:
            warnings.append("Missing months: " + ", ".join(sorted(set(missing))))
        raw_sources = outcome.get("source_intervals")
        if raw_sources is None and status == "WRITTEN":
            raw_sources = [batch_interval]
        raw_sources = raw_sources or []
        if (not isinstance(raw_sources, list)
                or len(raw_sources) > MAX_SOURCE_INTERVALS):
            raise ExportQualityError(
                f"invalid source-interval list for {ticker}")
        source_intervals = sorted(set(str(value) for value in raw_sources))
        if (len(source_intervals) != len(raw_sources)
                or any(not storage.INTERVAL_RE.fullmatch(value)
                       for value in source_intervals)
                or (status != "WRITTEN" and source_intervals)):
            raise ExportQualityError(
                f"invalid source-interval list for {ticker}")
    if not present:
        source_intervals = []
    if status == "EMPTY" and not error:
        error = "No rows matched the requested export range."
    elif status == "FAILED" and not error:
        error = "The ticker export failed without a detailed error."
    elif status == "CANCELLED" and not error:
        error = "The ticker export did not complete before cancellation."
    return {
        "ticker": ticker,
        "export_status": status,
        "rows": rows,
        "file": file_name,
        "export_start": export_start,
        "export_end": export_end,
        "export_error": error or None,
        "export_warnings": warnings,
        "source_intervals": source_intervals,
    }


def _validate_tier0_envelope(payload, kind):
    _finite_tree(payload)
    if (not isinstance(payload, dict) or payload.get("kind") != kind
            or payload.get("network") is not False
            or payload.get("written") is not False):
        raise EvidenceError(f"invalid Tier 0 {kind} envelope")
    evidence = payload.get("evidence")
    if not isinstance(evidence, dict) or not _valid_sha(evidence.get("fingerprint")):
        raise EvidenceError(f"Tier 0 {kind} evidence is invalid")
    paths = evidence.get("paths")
    if (not isinstance(paths, list) or len(paths) > MAX_ARTIFACTS
            or any(not isinstance(path, str) or len(path) > MAX_PATH
                   for path in paths)):
        raise EvidenceError(f"Tier 0 {kind} evidence paths are invalid")
    return evidence


def _tier0_gap_row(ticker, interval, payload):
    if payload.get("filters") != {"ticker": ticker, "interval": interval}:
        raise EvidenceError("Tier 0 gap filters do not match the request")
    if payload.get("applicable") is not True:
        raise EvidenceError("Tier 0 rejected a primary price interval as inapplicable")
    evidence_current = payload.get("evidence_current")
    if not isinstance(evidence_current, bool):
        raise EvidenceError("Tier 0 gap currentness is invalid")
    rows = payload.get("series")
    unavailable = payload.get("unavailable")
    errors = payload.get("errors")
    if (not isinstance(rows, list) or len(rows) > 1
            or not isinstance(unavailable, list) or len(unavailable) > 4
            or not isinstance(errors, list) or len(errors) > 4):
        raise EvidenceError("Tier 0 gap result is not bounded")
    pagination = payload.get("pagination")
    if (not isinstance(pagination, dict)
            or pagination.get("cursor") != 0
            or isinstance(pagination.get("returned"), bool)
            or pagination.get("returned") != len(rows)
            or pagination.get("next_cursor") is not None):
        raise EvidenceError("Tier 0 exact gap pagination is invalid")
    if not evidence_current:
        reason = "current gap evidence is unavailable"
        state = "unavailable"
        if unavailable:
            item = unavailable[0]
            if not isinstance(item, dict):
                raise EvidenceError("Tier 0 gap unavailable row is invalid")
            state = _input_text(
                item.get("state"), "Tier 0 gap state", limit=32)
            reason = _input_text(
                item.get("reason"), "Tier 0 gap reason", limit=MAX_REASON)
        elif errors:
            item = errors[0]
            if not isinstance(item, dict):
                raise EvidenceError("Tier 0 gap error row is invalid")
            state = _bounded(item.get("state") or "source_error", 32)
            reason = _bounded(item.get("reason") or "gap source error", MAX_REASON)
        return {
            "ticker": ticker, "interval": interval, "current": False,
            "state": state, "reason": reason,
            "asof": _bounded(payload.get("asof"), 64) or None,
        }
    if unavailable or errors or len(rows) != 1:
        raise EvidenceError("Tier 0 current gap result is incomplete")
    row = rows[0]
    if (not isinstance(row, dict) or row.get("ticker") != ticker
            or row.get("interval") != interval or row.get("current") is not True):
        raise EvidenceError("Tier 0 gap row identity/currentness is invalid")
    out = {"ticker": ticker, "interval": interval, "current": True}
    for field in ("missing_total", "gap_events", "days", "missing_days",
                  "source_absent"):
        out[field] = _evidence_int(row.get(field), f"Tier 0 gap {field}")
    for field, count_field in (
            ("missing_day_list", "missing_days"),
            ("source_absent_list", "source_absent")):
        values = row.get(field)
        if (not isinstance(values, list) or len(values) != out[count_field]
                or len(values) > 20_000):
            raise EvidenceError(f"Tier 0 gap {field} is invalid")
        parsed = [_iso_date(value, f"Tier 0 gap {field}") for value in values]
        if parsed != sorted(set(parsed)):
            raise EvidenceError(f"Tier 0 gap {field} is not sorted/unique")
        out[field] = parsed
    runs = row.get("missing_day_runs")
    if not isinstance(runs, list) or len(runs) > len(out["missing_day_list"]):
        raise EvidenceError("Tier 0 gap run list is invalid")
    normalized_runs = []
    for run in runs:
        if not isinstance(run, dict):
            raise EvidenceError("Tier 0 gap run is invalid")
        count = _evidence_int(run.get("count"), "Tier 0 gap run count")
        start = _iso_date(run.get("start"), "Tier 0 gap run start")
        end = _iso_date(run.get("end"), "Tier 0 gap run end")
        if not count or start > end:
            raise EvidenceError("Tier 0 gap run bounds are invalid")
        normalized_runs.append({"count": count, "start": start, "end": end})
    if sum(run["count"] for run in normalized_runs) != out["missing_days"]:
        raise EvidenceError("Tier 0 gap runs do not cover missing dates")
    cursor = 0
    previous_end = None
    for run in normalized_runs:
        members = out["missing_day_list"][cursor:cursor + run["count"]]
        if (len(members) != run["count"] or not members
                or members[0] != run["start"] or members[-1] != run["end"]
                or any(not run["start"] <= day <= run["end"]
                       for day in members)
                or (previous_end is not None and run["start"] <= previous_end)):
            raise EvidenceError("Tier 0 gap runs do not partition missing dates")
        cursor += run["count"]
        previous_end = run["end"]
    if cursor != len(out["missing_day_list"]):
        raise EvidenceError("Tier 0 gap runs do not partition missing dates")
    largest = row.get("largest_missing_day_run")
    if not isinstance(largest, dict):
        raise EvidenceError("Tier 0 largest gap run is invalid")
    largest_count = _evidence_int(
        largest.get("count"), "Tier 0 largest gap run count")
    if largest_count:
        largest_row = {
            "count": largest_count,
            "start": _iso_date(largest.get("start"), "Tier 0 largest run start"),
            "end": _iso_date(largest.get("end"), "Tier 0 largest run end"),
        }
    elif largest.get("start") is None and largest.get("end") is None:
        largest_row = {"count": 0, "start": None, "end": None}
    else:
        raise EvidenceError("Tier 0 empty largest gap run is invalid")
    expected_largest = max(
        normalized_runs, key=lambda item: item["count"],
        default={"count": 0, "start": None, "end": None})
    if largest_row != expected_largest:
        raise EvidenceError("Tier 0 largest gap run is inconsistent")
    provenance = row.get("interval_fingerprint")
    if (not isinstance(provenance, dict) or provenance.get("current") is not True
            or provenance.get("before_sha256") != provenance.get("after_sha256")
            or not _valid_sha(provenance.get("after_sha256"))):
        raise EvidenceError("Tier 0 gap interval provenance is invalid")
    out.update({
        "missing_day_runs": normalized_runs,
        "largest_missing_day_run": largest_row,
        "interval_fingerprint": {
            "algorithm": "sha256",
            "sha256": provenance["after_sha256"],
        },
        "asof": _input_text(row.get("asof"), "Tier 0 gap as-of", limit=64),
    })
    return out


def _load_tier0(adapter, selected, interval):
    queue = set()
    evidence_rows = []
    cursor = 0
    asof = None
    health_current = None
    while True:
        page = adapter.repair_queue(cursor=cursor, limit=100)
        evidence = _validate_tier0_envelope(page, "tier0_repair_queue")
        page_current = page.get("evidence_current")
        if not isinstance(page_current, bool):
            raise EvidenceError("Tier 0 repair currentness is invalid")
        if health_current is not None and page_current != health_current:
            raise EvidenceError("Tier 0 repair currentness changed between pages")
        health_current = page_current
        page_asof = _input_text(
            page.get("asof"), "Tier 0 repair as-of", limit=64)
        if asof is not None and page_asof != asof:
            raise EvidenceError("Tier 0 repair queue changed between pages")
        asof = page_asof
        evidence_rows.append({
            "fingerprint": str(evidence["fingerprint"]).lower(),
            "paths": [_bounded(value, 240) for value in evidence.get("paths") or []],
        })
        values = page.get("queue")
        if not isinstance(values, list) or len(values) > 100:
            raise EvidenceError("Tier 0 repair queue page is invalid")
        if not page_current and values:
            raise EvidenceError("non-current Tier 0 repair queue was not suppressed")
        for value in values:
            ticker = _canonical_ticker(value)
            if ticker in queue:
                raise EvidenceError("Tier 0 repair queue contains a duplicate ticker")
            queue.add(ticker)
            if len(queue) > MAX_TICKERS:
                raise EvidenceError("Tier 0 repair queue is too large")
        pagination = page.get("pagination")
        if not isinstance(pagination, dict):
            raise EvidenceError("Tier 0 repair pagination is invalid")
        next_cursor = pagination.get("next_cursor")
        if next_cursor is None:
            break
        next_cursor = _nonnegative_int(next_cursor, "Tier 0 next cursor")
        if next_cursor <= cursor or next_cursor > MAX_TICKERS:
            raise EvidenceError("Tier 0 repair pagination did not advance")
        cursor = next_cursor

    details = {}
    detail_sources = {}
    detail_errors = {}
    for ticker in sorted(queue & set(selected)):
        try:
            status = adapter.ticker_status(ticker)
            evidence = _validate_tier0_envelope(
                status, "tier0_ticker_status")
            if status.get("ticker") != ticker:
                raise EvidenceError("Tier 0 ticker status identity mismatch")
            details[ticker] = status
            detail_sources[ticker] = {
                "ticker": ticker,
                "fingerprint": str(evidence["fingerprint"]).lower(),
                "paths": [
                    _bounded(value, 240) for value in evidence.get("paths") or []
                ],
            }
        except Exception as exc:  # noqa: BLE001 - fail closed per ticker
            detail_errors[ticker] = _bounded(
                f"{type(exc).__name__}: {exc}", MAX_REASON)
    gaps = {}
    gap_sources = {}
    gap_errors = {}
    gap_applicable = (
        storage.INTERVAL_RE.fullmatch(str(interval or ""))
        and storage.kind_of(interval) == ""
        and storage.session_of(interval) == "rth"
    )
    if gap_applicable:
        for ticker in selected:
            try:
                payload = adapter.cached_gap_summary(
                    ticker=ticker, interval=interval, cursor=0, limit=10)
                evidence = _validate_tier0_envelope(
                    payload, "tier0_cached_gap_summary")
                gaps[ticker] = _tier0_gap_row(ticker, interval, payload)
                gap_sources[ticker] = {
                    "ticker": ticker,
                    "fingerprint": str(evidence["fingerprint"]).lower(),
                    "paths": [_bounded(value, 240)
                              for value in evidence.get("paths") or []],
                }
            except Exception as exc:  # noqa: BLE001 - fail closed per ticker
                error = _bounded(f"{type(exc).__name__}: {exc}", MAX_REASON)
                gap_errors[ticker] = error
                gaps[ticker] = {
                    "ticker": ticker, "interval": interval, "current": False,
                    "state": "query_error", "reason": error, "asof": None,
                }
    health_error = None if health_current else (
        "cached Tier 0 health is non-current for the bank manifest state")
    return {
        "ok": bool(health_current),
        "asof": asof,
        "queue": queue,
        "details": details,
        "detail_sources": detail_sources,
        "detail_errors": detail_errors,
        "gaps": gaps,
        "gap_errors": gap_errors,
        "gap_applicable": bool(gap_applicable),
        "source": {
            "kind": "tier0_repair_queue",
            "asof": asof,
            "pages": evidence_rows,
            "ticker_details": [
                detail_sources[ticker] for ticker in sorted(detail_sources)
            ],
            "gap_details": [gap_sources[ticker] for ticker in sorted(gap_sources)],
        },
        "error": health_error,
    }


def _actionable_tier0_row(ticker, status):
    current = status.get("current") if isinstance(status, dict) else None
    split = current.get("split") if isinstance(current, dict) else None
    if not isinstance(split, dict) or split.get("detection_current") is not True:
        return None
    rows = split.get("repair_queue") if isinstance(split, dict) else None
    if not isinstance(rows, list):
        return None
    matches = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            row_ticker = _canonical_ticker(row.get("ticker"))
        except ExportQualityError:
            continue
        verdict = str(row.get("verdict") or "").upper()
        if (row_ticker == ticker and row.get("scope") == "current"
                and row.get("repair_queue") is True
                and verdict in ACTIONABLE_SPLIT_VERDICTS):
            reason = _bounded(row.get("reason"), MAX_REASON)
            action = _bounded(row.get("recommended_action"), MAX_REASON)
            if reason and action:
                matches.append({
                    "verdict": verdict,
                    "reason": reason,
                    "recommended_action": action,
                    "date": _bounded(row.get("date"), 32),
                    "row_fingerprint": _sha256(json.dumps(
                        row, sort_keys=True, separators=(",", ":"),
                        allow_nan=False).encode("utf-8")),
                })
    return matches[0] if len(matches) == 1 else None


def _ws7_candidate_paths(run_logs_root, paths):
    root = Path(run_logs_root).resolve()
    if paths is None:
        try:
            values = [
                path for path in root.glob("external-sweep-*.json")
                if not path.name.endswith("-proof.json")
            ]
        except OSError as exc:
            raise EvidenceError("cannot enumerate WS7 artifacts") from exc
    else:
        if not isinstance(paths, (list, tuple)):
            raise EvidenceError("WS7 paths must be a list")
        values = [Path(path) for path in paths]
    if len(values) > MAX_ARTIFACTS:
        raise EvidenceError("too many WS7 artifact candidates")
    out = []
    for path in values:
        path = _direct_child(path, root)
        if (not path.name.startswith("external-sweep-")
                or not path.name.endswith(".json")
                or path.name.endswith("-proof.json")):
            raise EvidenceError("invalid WS7 artifact basename")
        out.append(path)
    return out


def _load_ws7_artifact(path):
    raw, stat = _read_stable_bytes(path, MAX_WS7_BYTES)
    payload = _load_json(raw, path.name)
    _finite_tree(payload)
    if payload.get("kind") != WS7_KIND:
        raise UnrelatedEvidence(f"{path.name} is not a WS7 sweep report")
    version = payload.get("version")
    if version == 1:
        raise HistoricalEvidence(version)
    if version != WS7_VERSION:
        raise EvidenceError(f"{path.name} has an unknown WS7 schema version")
    if (payload.get("report_only") is not True
            or payload.get("provider") != "stockanalysis"
            or payload.get("reference_range") != "Max"):
        raise EvidenceError(f"{path.name} is not a compatible WS7 v2 artifact")
    finished = _parse_timestamp(payload.get("finished_at"), "WS7 finished_at")
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows or len(rows) > MAX_TICKERS:
        raise EvidenceError(f"{path.name} has an invalid WS7 row count")
    params = payload.get("params")
    if (not isinstance(params, dict)
            or _nonnegative_int(params.get("ticker_count"), "WS7 ticker_count")
            != len(rows)):
        raise EvidenceError(f"{path.name} ticker count does not match rows")
    mapped = {}
    computed = {verdict: 0 for verdict in WS7_VERDICTS}
    for row in rows:
        if not isinstance(row, dict):
            raise EvidenceError(f"{path.name} contains a non-object row")
        ticker = _canonical_ticker(row.get("ticker"))
        if ticker in mapped:
            raise EvidenceError(f"{path.name} contains duplicate ticker {ticker}")
        verdict = str(row.get("verdict") or "")
        if verdict not in WS7_VERDICTS:
            raise EvidenceError(f"{path.name} has an unknown WS7 verdict")
        fingerprint = str(row.get("manifest_fingerprint") or "").lower()
        if not _valid_sha(fingerprint):
            raise EvidenceError(f"{path.name} has an invalid manifest fingerprint")
        encoded = json.dumps(
            row, sort_keys=True, separators=(",", ":"),
            allow_nan=False).encode("utf-8")
        mapped[ticker] = {
            "row": row,
            "row_fingerprint": _sha256(encoded),
        }
        computed[verdict] += 1
    counts = payload.get("counts")
    if (not isinstance(counts, dict)
            or any(_nonnegative_int(counts.get(verdict), f"WS7 {verdict} count")
                   != count for verdict, count in computed.items())):
        raise EvidenceError(f"{path.name} verdict counts do not match rows")
    return {
        "path": path,
        "basename": path.name,
        "sha256": _sha256(raw),
        "bytes": len(raw),
        "mtime_ns": stat.st_mtime_ns,
        "finished_at": finished.isoformat(timespec="seconds"),
        "finished_sort": finished.timestamp(),
        "version": WS7_VERSION,
        "rows": mapped,
    }


def _load_ws7(run_logs_root, paths, selected):
    errors = []
    artifacts = []
    historical = []
    ignored = 0
    try:
        candidates = _ws7_candidate_paths(run_logs_root, paths)
    except Exception as exc:  # noqa: BLE001 - normalized source failure
        return {
            "rows": {},
            "sources": [],
            "historical": [],
            "ignored_candidates": 0,
            "errors": [
                _bounded(f"{type(exc).__name__}: {exc}", MAX_REASON)],
        }
    for path in candidates:
        try:
            artifacts.append(_load_ws7_artifact(path))
        except HistoricalEvidence as exc:
            historical.append({
                "basename": path.name,
                "version": exc.version,
            })
        except UnrelatedEvidence as exc:
            if paths is None:
                ignored += 1
            else:
                errors.append(_bounded(
                    f"{path.name}: {type(exc).__name__}: {exc}", MAX_REASON))
        except Exception as exc:  # noqa: BLE001 - one artifact fails closed
            errors.append(_bounded(
                f"{path.name}: {type(exc).__name__}: {exc}", MAX_REASON))
    artifacts.sort(
        key=lambda item: (item["finished_sort"], item["mtime_ns"], item["basename"]),
        reverse=True)
    chosen = {}
    used = {}
    wanted = set(selected)
    for artifact in artifacts:
        for ticker in sorted(wanted - set(chosen)):
            if ticker in artifact["rows"]:
                item = artifact["rows"][ticker]
                chosen[ticker] = {
                    **item,
                    "artifact": artifact["basename"],
                    "artifact_sha256": artifact["sha256"],
                    "artifact_finished_at": artifact["finished_at"],
                    "artifact_version": artifact["version"],
                }
                used[artifact["basename"]] = {
                    "basename": artifact["basename"],
                    "sha256": artifact["sha256"],
                    "bytes": artifact["bytes"],
                    "finished_at": artifact["finished_at"],
                    "version": artifact["version"],
                }
        if wanted.issubset(chosen):
            break
    return {
        "rows": chosen,
        "sources": [used[key] for key in sorted(used)],
        "historical": historical[:MAX_ARTIFACTS],
        "ignored_candidates": ignored,
        "errors": errors[:MAX_SOURCE_ERRORS],
    }


def _ws7_assessment(ticker, manifest, ws7_item, tier0_ok):
    if manifest is None:
        return {
            "quality": "NO_CURRENT_EVIDENCE",
            "reason": "Current manifest evidence is unavailable for this ticker.",
            "quality_source": "manifest",
        }
    if ws7_item is None:
        return {
            "quality": "NO_CURRENT_EVIDENCE",
            "reason": "No compatible current WS7 v2 row covers this ticker.",
            "quality_source": "ws7",
        }
    row = ws7_item["row"]
    verdict = row["verdict"]
    base = {
        "ws7_verdict": verdict,
        "ws7_row_fingerprint": ws7_item["row_fingerprint"],
        "ws7_artifact": ws7_item["artifact"],
        "evidence_asof": ws7_item["artifact_finished_at"],
    }
    if str(row.get("manifest_fingerprint") or "").lower() != manifest["sha256"]:
        return {
            **base,
            "quality": "NO_CURRENT_EVIDENCE",
            "reason": (f"Historical WS7 verdict {verdict} is stale because the "
                       "manifest fingerprint changed."),
            "quality_source": "ws7_stale",
            "historical_verdict": verdict,
        }
    if verdict == "CLEAN":
        if not tier0_ok:
            return {
                **base,
                "quality": "NO_CURRENT_EVIDENCE",
                "reason": "WS7 is clean, but current Tier 0 repair evidence is unavailable.",
                "quality_source": "ws7+tier0_unavailable",
            }
        quality = "CLEAN"
        reason = "Current WS7 v2 full-range evidence is clean and Tier 0 has no repair finding."
    elif verdict == "HISTORIC_BASIS_OFFSET":
        quality = "KNOWN_DIFFERENCE"
        reason = ("WS7 reports a historic basis or corporate-action convention "
                  "difference; its current segment is aligned.")
    elif verdict in {"CURRENT_MISMATCH", "DRIFT_ANOMALY", "SEAM_CANDIDATE"}:
        quality = "REVIEW_REQUIRED"
        reason = f"WS7 reports {verdict}; review evidence before any repair."
    else:
        quality = "UNVERIFIABLE"
        why = _bounded(row.get("reason") or "source evidence was inconclusive")
        reason = f"WS7 could not verify this ticker: {why}"
    return {
        **base,
        "quality": quality,
        "reason": reason,
        "quality_source": "ws7",
    }


def _assessment_finding(assessment):
    code = (assessment.get("detector") or assessment.get("evidence_code")
            or assessment.get("ws7_verdict") or assessment.get("quality"))
    return {
        "source": _bounded(assessment.get("quality_source"), 64),
        "code": _bounded(code, 96),
        "summary": _bounded(assessment.get("reason"), MAX_REASON),
        "asof": _bounded(assessment.get("evidence_asof"), 64) or None,
        "current": assessment.get("historical_verdict") is None,
        "verdict_affecting": True,
    }


def _public_validation_evidence(value):
    return {
        key: item for key, item in value.items()
        if not key.startswith("_")
    }


def _apply_validation_overlay(assessment, xval_items, combined_items, export):
    base = dict(assessment)
    if export.get("export_status") != "WRITTEN":
        result = dict(base)
        result["quality_findings"] = [_assessment_finding(base)]
        result["cross_validation_evidence"] = []
        return result
    source_intervals = set(export.get("source_intervals") or [])
    evidence = [dict(item) for item in xval_items
                if item.get("interval") in source_intervals]
    for combined_item in combined_items or []:
        if combined_item.get("interval") not in source_intervals:
            continue
        combined = dict(combined_item)
        combined_dates = set(combined.get("_all_dates") or [])
        for item in evidence:
            if (item.get("source") == "cross_validation"
                    and item.get("interval") == combined.get("interval")):
                item["three_source_checked"] = True
                persistent = (sorted(
                    set(item.get("_all_dates") or []) & combined_dates)
                    if item.get("_price_evidence") else [])
                item["three_source_persistent"] = bool(persistent)
                item["three_source_dates"] = persistent[:MAX_EXAMPLE_DATES]
                if persistent:
                    item.update({
                        "code": "three_source_persistent_price",
                        "impact": "review",
                        "verdict_affecting": True,
                        "known_difference": False,
                        "summary": _bounded(
                            item["summary"]
                            + "; current three-source price evidence persists on "
                            + ", ".join(persistent[:MAX_EXAMPLE_DATES]),
                            MAX_REASON),
                    })
        if combined.get("_emit"):
            evidence.append(combined)
    public = [_public_validation_evidence(item) for item in evidence]
    affecting = [
        item for item in public
        if item.get("current") is True
        and item.get("verdict_affecting") is True
    ]
    known_differences = [
        item for item in public
        if item.get("current") is True
        and item.get("known_difference") is True
    ]
    affecting.sort(key=lambda item: (
        item.get("source") != "combined_flags",
        item.get("interval") or "", item.get("code") or ""))
    result = dict(base)
    if affecting and result.get("quality") != "CONFIRMED_BAD":
        if result.get("quality") != "REVIEW_REQUIRED":
            result["reason"] = affecting[0]["summary"]
            result["evidence_asof"] = affecting[0].get("asof")
        result["quality"] = "REVIEW_REQUIRED"
        sources = [str(result.get("quality_source") or "base")]
        for source in ("cross_validation", "combined_flags"):
            if any(item.get("source") == source for item in affecting):
                sources.append(source)
        result["quality_source"] = "+".join(dict.fromkeys(sources))
    elif known_differences and result.get("quality") in {
            "CLEAN", "NO_CURRENT_EVIDENCE"}:
        result.update({
            "quality": "KNOWN_DIFFERENCE",
            "reason": known_differences[0]["summary"],
            "evidence_asof": known_differences[0].get("asof"),
        })
        sources = [str(result.get("quality_source") or "base"),
                   "cross_validation"]
        result["quality_source"] = "+".join(dict.fromkeys(sources))
    elif known_differences and result.get("quality") == "KNOWN_DIFFERENCE":
        sources = [str(result.get("quality_source") or "base"),
                   "cross_validation"]
        result["quality_source"] = "+".join(dict.fromkeys(sources))
    findings = [_assessment_finding(base), *public]
    result["quality_findings"] = findings[:MAX_QUALITY_FINDINGS]
    result["cross_validation_evidence"] = public[
        :MAX_XVAL_EVIDENCE_PER_TICKER + 1]
    return result


def _range_month(value, *, start):
    text = str(value or "").strip()
    lowered = text.lower()
    open_tokens = ({"", "all", "preset:furthest"} if start else {
        "", "latest", "per-ticker-latest",
    })
    if lowered in open_tokens:
        return None, True
    if _DATE_RE.fullmatch(text):
        try:
            return dt.date.fromisoformat(text).strftime("%Y-%m"), True
        except ValueError:
            pass
    return None, False


def _coverage_finding(row, export, batch):
    if (export.get("export_status") != "WRITTEN"
            or row.get("kind") not in set(
                export.get("source_intervals") or [])):
        return None
    start_value = export.get("export_start")
    end_value = export.get("export_end")
    if start_value is None:
        start_value = batch.get("start")
    if end_value is None:
        end_value = batch.get("end")
    start_month, start_known = _range_month(start_value, start=True)
    end_month, end_known = _range_month(end_value, start=False)
    if not start_known or not end_known:
        return None
    if (start_month is not None
            and _coverage_month_ord(start_month)
            > _coverage_month_ord(row["primary_last"])):
        return None
    if (end_month is not None
            and _coverage_month_ord(end_month)
            < _coverage_month_ord(row["kind_last"])):
        return None
    shown_start = start_month or "open"
    shown_end = end_month or "open"
    if end_month == row["kind_last"]:
        range_claim = (
            "reaches the source-frontier month; month-level coverage cannot "
            "prove that the joined values reach every exported day")
    else:
        range_claim = "extends beyond the source-frontier month"
    summary = _bounded(
        f"{row['kind']} is forward-stale: stored through "
        f"{row['kind_last']} while primary {COVERAGE_PRIMARY_INTERVAL} reaches "
        f"{row['primary_last']} ({row['lag_months']}-month lag); export range "
        f"{shown_start}..{shown_end} {range_claim}.",
        MAX_REASON)
    return {
        "source": "coverage_audit",
        "code": "KIND_FORWARD_STALE",
        "interval": row["kind"],
        "kind_last": row["kind_last"],
        "primary_last": row["primary_last"],
        "lag_months": row["lag_months"],
        "export_start_month": start_month,
        "export_end_month": end_month,
        "summary": summary,
        "asof": row.get("asof"),
        "current": True,
        "verdict_affecting": True,
        "impact": "warning",
    }


def _apply_coverage_overlay(assessment, coverage_rows, export, batch):
    finding = next((
        item for item in (
            _coverage_finding(row, export, batch) for row in coverage_rows)
        if item is not None
    ), None)
    result = dict(assessment)
    if finding is None:
        result["coverage_evidence"] = []
        return result
    if result.get("quality") not in {"CONFIRMED_BAD", "REVIEW_REQUIRED"}:
        result["quality"] = "REVIEW_REQUIRED"
        result["reason"] = finding["summary"]
        result["evidence_asof"] = finding.get("asof")
    sources = [str(result.get("quality_source") or "base"), "coverage_audit"]
    result["quality_source"] = "+".join(dict.fromkeys(sources))
    result["quality_findings"] = [
        *(result.get("quality_findings") or []), finding,
    ][:MAX_QUALITY_FINDINGS]
    result["coverage_evidence"] = [finding]
    return result


def _range_date(value, *, start):
    text = str(value or "").strip()
    lowered = text.lower()
    open_tokens = ({"", "all", "preset:furthest"} if start else {
        "", "latest", "per-ticker-latest",
    })
    if lowered in open_tokens:
        return None, True
    if _DATE_RE.fullmatch(text):
        try:
            return dt.date.fromisoformat(text), True
        except ValueError:
            return None, False
    if _MONTH_RE.fullmatch(text):
        try:
            year, month = int(text[:4]), int(text[5:7])
            first = dt.date(year, month, 1)
            if start:
                return first, True
            following = (dt.date(year + 1, 1, 1) if month == 12
                         else dt.date(year, month + 1, 1))
            return following - dt.timedelta(days=1), True
        except ValueError:
            return None, False
    return None, False


def _gap_export_bounds(export, batch):
    start_value = export.get("export_start")
    end_value = export.get("export_end")
    if start_value is None:
        start_value = batch.get("start")
    if end_value is None:
        end_value = batch.get("end")
    start, start_known = _range_date(start_value, start=True)
    end, end_known = _range_date(end_value, start=False)
    return start, end, start_known and end_known


def _within_export(day, start, end):
    parsed = dt.date.fromisoformat(day)
    return (start is None or parsed >= start) and (end is None or parsed <= end)


def _apply_gap_overlay(assessment, gap_row, export, batch, applicable):
    result = dict(assessment)
    result["gap_evidence"] = []
    if export.get("export_status") != "WRITTEN" or not applicable:
        return result
    if not isinstance(gap_row, dict) or gap_row.get("current") is not True:
        reason = _bounded(
            (gap_row or {}).get("reason")
            or "Current gap evidence is unavailable for the exported interval.",
            MAX_REASON)
        finding = {
            "source": "gap_evidence",
            "code": "GAP_EVIDENCE_NONCURRENT",
            "interval": batch["interval"],
            "summary": reason,
            "asof": (gap_row or {}).get("asof"),
            "current": False,
            "verdict_affecting": False,
            "impact": "diagnostic",
            "state": (gap_row or {}).get("state") or "unavailable",
        }
        if result.get("quality") == "CLEAN":
            result.update({
                "quality": "NO_CURRENT_EVIDENCE",
                "reason": reason,
                "quality_source": "gap_evidence_noncurrent",
                "evidence_asof": finding.get("asof"),
                "evidence_code": finding["code"],
            })
        result["quality_findings"] = [
            *(result.get("quality_findings") or []), finding,
        ][:MAX_QUALITY_FINDINGS]
        result["gap_evidence"] = [finding]
        return result

    start, end, bounds_known = _gap_export_bounds(export, batch)
    if not bounds_known:
        return _apply_gap_overlay(
            result,
            {"current": False, "state": "range_unknown",
             "reason": "Gap evidence cannot be matched to the exported date range.",
             "asof": gap_row.get("asof")},
            export, batch, applicable)
    missing = [day for day in gap_row.get("missing_day_list") or []
               if _within_export(day, start, end)]
    absent = [day for day in gap_row.get("source_absent_list") or []
              if _within_export(day, start, end)]
    findings = []
    if missing:
        selected = set(missing)
        largest = max((
            sum(day in selected for day in gap_row.get("missing_day_list") or []
                if run["start"] <= day <= run["end"])
            for run in gap_row.get("missing_day_runs") or []
        ), default=1)
        examples = missing[:MAX_EXAMPLE_DATES]
        summary = _bounded(
            f"{len(missing)} fillable trading day(s) are missing from "
            f"{batch['interval']} within the exported range; largest consecutive "
            f"expected-session run={largest}; examples={','.join(examples)}.",
            MAX_REASON)
        finding = {
            "source": "gap_evidence",
            "code": "FILLABLE_DAY_GAP",
            "interval": batch["interval"],
            "missing_days": len(missing),
            "largest_run": largest,
            "examples": examples,
            "summary": summary,
            "asof": gap_row.get("asof"),
            "current": True,
            "verdict_affecting": True,
            "impact": "warning",
            "fingerprint": gap_row.get("interval_fingerprint"),
        }
        findings.append(finding)
        if result.get("quality") != "CONFIRMED_BAD":
            if (result.get("quality") != "REVIEW_REQUIRED"
                    or result.get("quality_source") == "tier0_repair_queue"):
                result["reason"] = summary
                result["evidence_asof"] = gap_row.get("asof")
            result["quality"] = "REVIEW_REQUIRED"
            sources = [str(result.get("quality_source") or "base"), "gap_evidence"]
            result["quality_source"] = "+".join(dict.fromkeys(sources))
            result["evidence_code"] = "FILLABLE_DAY_GAP"
    if absent:
        examples = absent[:MAX_EXAMPLE_DATES]
        findings.append({
            "source": "gap_evidence",
            "code": "SOURCE_ABSENT_DAY",
            "interval": batch["interval"],
            "source_absent_days": len(absent),
            "examples": examples,
            "summary": _bounded(
                f"{len(absent)} trading day(s) in the exported range are marked "
                f"source-absent; examples={','.join(examples)}.", MAX_REASON),
            "asof": gap_row.get("asof"),
            "current": True,
            "verdict_affecting": False,
            "impact": "information",
            "fingerprint": gap_row.get("interval_fingerprint"),
        })
    result["quality_findings"] = [
        *(result.get("quality_findings") or []), *findings,
    ][:MAX_QUALITY_FINDINGS]
    result["gap_evidence"] = findings
    return result


def _quality_counts(rows):
    return {
        status: sum(row["quality"] == status for row in rows)
        for status in QUALITY_ASSESSMENTS
    }


def _export_counts(rows):
    out = {
        status: sum(row["export_status"] == status for row in rows)
        for status in EXPORT_STATUSES
    }
    out["MISSING_MONTHS"] = sum(bool(row["export_warnings"]) for row in rows)
    out["ROWS"] = sum(row["rows"] for row in rows)
    return out


def _load_settled_volatility(root, rows):
    """Load historical settlements for exact ratio series actually written."""
    selections = sorted({
        (row["ticker"], interval)
        for row in rows if row.get("export_status") == "WRITTEN"
        for interval in (row.get("source_intervals") or [])
        if storage.kind_of(interval) in storage.RATIO_KINDS
    })
    if not selections:
        return {
            "rows": [], "total": 0, "truncated": False,
            "source": None, "errors": [],
        }
    try:
        settled, source = vol_value_audit.settled_for_export(
            root, selections, include_source=True)
        if not isinstance(settled, list):
            raise ExportQualityError(
                "source-confirmed volatility anomaly list is invalid")
        total_rows = len(settled)
        truncated = total_rows > MAX_SETTLED_VOL_ROWS
        settled = settled[:MAX_SETTLED_VOL_ROWS]
        rows_out = []
        for value in settled:
            if not isinstance(value, dict):
                raise ExportQualityError(
                    "source-confirmed volatility anomaly row is invalid")
            ticker = _canonical_ticker(value.get("ticker"))
            interval = str(value.get("kind") or "")
            if ((ticker, interval) not in selections
                    or not storage.INTERVAL_RE.fullmatch(interval)
                    or storage.kind_of(interval) not in storage.RATIO_KINDS):
                raise ExportQualityError(
                    "source-confirmed volatility anomaly selection is invalid")
            day = _iso_date(value.get("day"), "settled volatility day")
            number = _evidence_number(
                value.get("value"), "settled volatility value")
            if number < 0:
                raise ExportQualityError(
                    "source-confirmed volatility value is negative")
            rows_out.append({
                "ticker": ticker,
                "kind": interval,
                "day": day,
                "value": number,
                "reason": _input_text(
                    value.get("reason"), "settled volatility reason",
                    limit=MAX_REASON),
                "confirmed": _iso_date(
                    value.get("confirmed"),
                    "settled volatility confirmation date"),
                "run": _input_text(
                    value.get("run"), "settled volatility run",
                    limit=MAX_REASON),
            })
        rows_out.sort(key=lambda item: (
            item["ticker"], item["kind"], item["day"]))
        errors = []
        if truncated:
            errors.append({
                "source": "vol_value_settled",
                "error": (
                    f"source-confirmed volatility anomalies capped at "
                    f"{MAX_SETTLED_VOL_ROWS} of {total_rows} selected entries"),
            })
        if source is not None:
            source = {
                **source,
                "selected_entries": total_rows,
                "reported_entries": len(rows_out),
                "truncated": truncated,
            }
        return {
            "rows": rows_out,
            "total": total_rows,
            "truncated": truncated,
            "source": source,
            "errors": errors,
        }
    except Exception as exc:  # noqa: BLE001 - evidence failure is report data
        return {
            "rows": [],
            "total": 0,
            "truncated": False,
            "source": None,
            "errors": [{
                "source": "vol_value_settled",
                "error": _bounded(
                    f"{type(exc).__name__}: {exc}", MAX_REASON),
            }],
        }


def build_report(batch, selected, present, outcomes, *, storage_root=STORAGE_ROOT,
                 run_logs_root=RUN_LOGS_ROOT, ws7_paths=None,
                 tier0_adapter=None, now=None):
    """Build one bounded report. Reads evidence only; writes nothing."""
    batch = _normalize_batch(batch, now)
    selected, present, outcomes = _normalize_selection(
        selected, present, outcomes)
    storage_root = Path(storage_root).resolve()
    run_logs_root = Path(run_logs_root).resolve()
    if _inside(run_logs_root, storage_root):
        raise ExportQualityError("Run Logs evidence root must stay outside the bank")

    before, manifest_errors = {}, {}
    for ticker in selected:
        if ticker not in present:
            before[ticker] = None
            continue
        try:
            before[ticker] = _manifest_fingerprint(storage_root, ticker)
        except Exception as exc:  # noqa: BLE001 - fail closed per ticker
            before[ticker] = None
            manifest_errors[ticker] = _bounded(
                f"{type(exc).__name__}: {exc}", MAX_REASON)

    adapter = tier0_adapter or tier0_queries
    try:
        tier0 = _load_tier0(adapter, selected, batch["interval"])
    except Exception as exc:  # noqa: BLE001 - source failure is report data
        tier0 = {
            "ok": False,
            "asof": None,
            "queue": set(),
            "details": {},
            "detail_sources": {},
            "detail_errors": {},
            "gaps": {},
            "gap_errors": {},
            "gap_applicable": bool(
                storage.INTERVAL_RE.fullmatch(batch["interval"])
                and storage.kind_of(batch["interval"]) == ""
                and storage.session_of(batch["interval"]) == "rth"),
            "source": None,
            "error": _bounded(f"{type(exc).__name__}: {exc}", MAX_REASON),
        }
    ws7 = _load_ws7(run_logs_root, ws7_paths, selected)
    xval = _load_cross_validation(storage_root, selected)
    combined = _load_combined_flags(storage_root, selected)
    coverage = _load_coverage(storage_root, selected)

    rows = []
    for ticker in selected:
        export = _normalize_export_row(
            ticker, ticker in present, outcomes.get(ticker), batch["state"],
            batch["interval"])
        assessment = _ws7_assessment(
            ticker, before.get(ticker), ws7["rows"].get(ticker), tier0["ok"])
        if (ticker in present and before.get(ticker) is not None
                and ticker in tier0["queue"]):
            detail = tier0["details"].get(ticker)
            actionable = _actionable_tier0_row(ticker, detail)
            if actionable is not None:
                assessment = {
                    **assessment,
                    "quality": "CONFIRMED_BAD",
                    "reason": actionable["reason"],
                    "detector": f"split_audit:{actionable['verdict']}",
                    "recommended_action": actionable["recommended_action"],
                    "quality_source": "tier0_split_repair_queue",
                    "quality_source_fingerprint": (
                        tier0["detail_sources"].get(ticker, {}).get(
                            "fingerprint")),
                    "detector_row_fingerprint": actionable["row_fingerprint"],
                    "evidence_asof": tier0.get("asof"),
                }
            else:
                detail_error = tier0["detail_errors"].get(ticker)
                assessment = {
                    **assessment,
                    "quality": "REVIEW_REQUIRED",
                    "reason": _bounded(
                        "Tier 0 health lists this ticker for repair, but no single "
                        "current actionable detector row was available"
                        + (f" ({detail_error})" if detail_error else "."),
                        MAX_REASON),
                    "quality_source": "tier0_repair_queue",
                    "evidence_asof": tier0.get("asof"),
                }
        assessment = _apply_validation_overlay(
            assessment, xval["rows"].get(ticker, []),
            combined["rows"].get(ticker, []), export)
        assessment = _apply_coverage_overlay(
            assessment, coverage["rows"].get(ticker, []), export, batch)
        assessment = _apply_gap_overlay(
            assessment, tier0["gaps"].get(ticker), export, batch,
            tier0.get("gap_applicable") is True)
        rows.append({
            **export,
            "quality": assessment["quality"],
            "quality_reason": _bounded(assessment.get("reason"), MAX_REASON),
            "quality_source": _bounded(
                assessment.get("quality_source"), 64),
            "evidence_asof": _bounded(
                assessment.get("evidence_asof"), 64) or None,
            "detector": _bounded(assessment.get("detector"), 96) or None,
            "recommended_action": _bounded(
                assessment.get("recommended_action"), MAX_REASON) or None,
            "manifest_fingerprint": (
                before[ticker]["sha256"] if before.get(ticker) else None),
            "ws7_verdict": assessment.get("ws7_verdict"),
            "historical_verdict": assessment.get("historical_verdict"),
            "ws7_artifact": assessment.get("ws7_artifact"),
            "ws7_row_fingerprint": assessment.get("ws7_row_fingerprint"),
            "quality_source_fingerprint": assessment.get(
                "quality_source_fingerprint"),
            "detector_row_fingerprint": assessment.get(
                "detector_row_fingerprint"),
            "evidence_code": assessment.get("evidence_code"),
            "quality_findings": assessment.get("quality_findings") or [],
            "cross_validation_evidence": assessment.get(
                "cross_validation_evidence") or [],
            "coverage_evidence": assessment.get("coverage_evidence") or [],
            "gap_evidence": assessment.get("gap_evidence") or [],
        })

    after = {}
    for ticker in selected:
        if ticker not in present:
            after[ticker] = None
            continue
        try:
            after[ticker] = _manifest_fingerprint(storage_root, ticker)
        except Exception:  # noqa: BLE001 - an unreadable recheck is a race/failure
            after[ticker] = None
    for row in rows:
        ticker = row["ticker"]
        old = before.get(ticker)
        new = after.get(ticker)
        evidence_changed = (
            (old is None) != (new is None)
            or (old is not None and new is not None
                and old["sha256"] != new["sha256"])
        )
        if ticker in present and evidence_changed:
            if row.get("ws7_verdict") and not row.get("historical_verdict"):
                row["historical_verdict"] = row["ws7_verdict"]
            diagnostics = []
            for item in row.get("cross_validation_evidence") or []:
                diagnostic = dict(item)
                diagnostic.update({
                    "current": False,
                    "diagnostic": True,
                    "verdict_affecting": False,
                    "impact": "diagnostic",
                })
                diagnostics.append(diagnostic)
            coverage_diagnostics = []
            for item in row.get("coverage_evidence") or []:
                diagnostic = dict(item)
                diagnostic.update({
                    "current": False,
                    "verdict_affecting": False,
                    "impact": "diagnostic",
                })
                coverage_diagnostics.append(diagnostic)
            gap_diagnostics = []
            for item in row.get("gap_evidence") or []:
                diagnostic = dict(item)
                diagnostic.update({
                    "current": False,
                    "verdict_affecting": False,
                    "impact": "diagnostic",
                })
                gap_diagnostics.append(diagnostic)
            row.update({
                "quality": "NO_CURRENT_EVIDENCE",
                "quality_reason": "Evidence changed during export note generation.",
                "quality_source": "evidence_race",
                "quality_source_fingerprint": None,
                "evidence_code": "evidence_changed_during_export",
                "detector": None,
                "detector_row_fingerprint": None,
                "recommended_action": None,
                "cross_validation_evidence": diagnostics,
                "coverage_evidence": coverage_diagnostics,
                "gap_evidence": gap_diagnostics,
                "quality_findings": [{
                    "source": "evidence_race",
                    "code": "evidence_changed_during_export",
                    "summary": "Evidence changed during export note generation.",
                    "asof": None,
                    "current": False,
                    "verdict_affecting": False,
                }, *diagnostics, *coverage_diagnostics, *gap_diagnostics][
                    :MAX_QUALITY_FINDINGS],
            })

    settled_volatility = _load_settled_volatility(storage_root, rows)

    # Keep a malformed settlement registry visible even when older evidence
    # sources already fill the bounded diagnostics budget.
    source_errors = list(settled_volatility["errors"])
    if tier0.get("error"):
        source_errors.append({"source": "tier0", "error": tier0["error"]})
    source_errors.extend(
        {"source": "tier0", "ticker": ticker, "error": error}
        for ticker, error in sorted(tier0["detail_errors"].items()))
    source_errors.extend(
        {"source": "gap_evidence", "ticker": ticker, "error": error}
        for ticker, error in sorted(tier0["gap_errors"].items()))
    source_errors.extend(
        {"source": "ws7", "error": error} for error in ws7["errors"])
    source_errors.extend(
        {"source": "cross_validation", "error": error}
        for error in xval["errors"])
    source_errors.extend(
        {"source": "combined_flags", "error": error}
        for error in combined["errors"])
    source_errors.extend(
        {"source": "coverage_audit", "error": error}
        for error in coverage["errors"])
    source_errors.extend(
        {"source": "manifest", "ticker": ticker, "error": error}
        for ticker, error in sorted(manifest_errors.items()))
    source_errors = source_errors[:MAX_SOURCE_ERRORS]

    report = {
        "kind": REPORT_KIND,
        "version": REPORT_VERSION,
        "report_only": True,
        "network": False,
        "bank_written": False,
        "batch": batch,
        "summary": {
            "selected": len(selected),
            "present": len(present),
            "export": _export_counts(rows),
            "quality": _quality_counts(rows),
            "source_confirmed_volatility_anomalies": len(
                settled_volatility["rows"]),
            "source_confirmed_volatility_anomalies_total": (
                settled_volatility["total"]),
            "source_confirmed_volatility_anomalies_truncated": (
                settled_volatility["truncated"]),
        },
        "rows": rows,
        "source_confirmed_volatility_anomalies": settled_volatility["rows"],
        "sources": {
            "tier0": tier0.get("source"),
            "ws7": ws7["sources"],
            "ws7_historical": ws7["historical"],
            "ws7_ignored_candidates": ws7["ignored_candidates"],
            "cross_validation": xval.get("source"),
            "combined_flags": combined.get("source"),
            "coverage_audit": coverage.get("source"),
            "vol_value_settled": settled_volatility.get("source"),
            "errors": source_errors,
        },
    }
    _finite_tree(report)
    encoded = json.dumps(
        report, sort_keys=True, separators=(",", ":"),
        allow_nan=False).encode("utf-8")
    if len(encoded) > MAX_REPORT_BYTES:
        raise ExportQualityError("structured export quality report is too large")
    return report


def _section(lines, title, rows):
    lines.extend(("", title))
    if not rows:
        lines.append("None.")
        return
    for row in rows:
        detail = f"{row['ticker']}: {row['quality_reason']}"
        if row.get("detector"):
            detail += f" Detector={row['detector']}."
        if row.get("recommended_action"):
            detail += f" Next={row['recommended_action']}"
        if row.get("evidence_asof"):
            detail += f" As-of={row['evidence_asof']}."
        lines.append(detail)
        for finding in row.get("quality_findings") or []:
            if (finding.get("source") in {
                    "cross_validation", "combined_flags", "coverage_audit"}
                    or finding.get("summary") == row.get("quality_reason")):
                continue
            lines.append(
                f"  Base evidence [{finding.get('source') or 'unknown'} / "
                f"{finding.get('code') or 'context'}]: "
                f"{finding.get('summary') or 'no detail'}")


def _render_coverage_evidence(lines, rows):
    lines.extend(("", "PER-KIND COVERAGE WARNINGS"))
    found = False
    for row in rows:
        for item in row.get("coverage_evidence") or []:
            if item.get("current") is not True:
                continue
            found = True
            lines.append(
                f"{row.get('ticker')} {item.get('interval')} [warning]: "
                f"{item.get('summary')} As-of={item.get('asof') or 'unknown'}.")
    if not found:
        lines.append("None.")


def _render_gap_evidence(lines, rows):
    lines.extend(("", "GAP EVIDENCE"))
    found = False
    for row in rows:
        for item in row.get("gap_evidence") or []:
            found = True
            state = ("warning" if item.get("code") == "FILLABLE_DAY_GAP"
                     else "information" if item.get("current") is True
                     else "non-current")
            lines.append(
                f"{row.get('ticker')} {item.get('interval') or 'unknown'} "
                f"[{state}]: {item.get('summary') or 'no detail'} "
                f"As-of={item.get('asof') or 'unknown'}.")
    if not found:
        lines.append("None.")


def _render_validation_evidence(lines, rows):
    lines.extend(("", "CROSS-VALIDATION EVIDENCE"))
    found = False
    for row in rows:
        ticker = row.get("ticker")
        for item in row.get("cross_validation_evidence") or []:
            found = True
            interval = item.get("interval") or "unknown"
            if item.get("source") == "combined_flags":
                lines.append(
                    f"{ticker} {interval} [review]: "
                    f"{item.get('summary')}. As-of={item.get('asof') or 'unknown'}.")
                continue
            fields = ",".join(item.get("fields") or []) or "uniform-level"
            examples = ",".join(item.get("example_dates") or []) or "none"
            if item.get("three_source_checked"):
                three_source = (
                    "yes" if item.get("three_source_persistent") else "no")
            else:
                three_source = "not-currently-checked"
            detail = (
                f"{ticker} {interval} [{item.get('impact') or 'informational'}]: "
                f"fields={fields}; distinct-days={item.get('distinct_days', 0)}; "
                f"examples={examples}; as-of={item.get('asof') or 'unknown'}; "
                f"current-three-source-persistent={three_source}")
            if (item.get("price_row_count")
                    or item.get("level_shift_date")
                    or item.get("suspected_factor") is not None):
                detail += (
                    f"; price-rows={item.get('price_row_count', 0)}"
                    f"/{item.get('checked_days', 0)}"
                    f" ({item.get('price_row_density_pct', 0):g}%)")
            if item.get("level_shift_date"):
                detail += f"; level-shift={item['level_shift_date']}"
            if item.get("level_shift_factor") is not None:
                detail += f"; level-shift-factor={item['level_shift_factor']:g}"
            if item.get("known_corporate_action"):
                detail += "; reviewed-corporate-action=yes"
            if item.get("three_source_dates"):
                detail += " (" + ",".join(item["three_source_dates"]) + ")"
            if item.get("auction_candidate"):
                detail += "; auction-candidate=yes"
            lines.append(detail + ".")
    if not found:
        lines.append("None.")


def _render_settled_volatility(lines, report):
    rows = report.get("source_confirmed_volatility_anomalies") or []
    if not isinstance(rows, list):
        raise ExportQualityError(
            "source-confirmed volatility anomalies are invalid")
    if not rows:
        return
    lines.extend(("", "SOURCE-CONFIRMED VOLATILITY ANOMALIES"))
    summary = report.get("summary") or {}
    total = summary.get(
        "source_confirmed_volatility_anomalies_total", len(rows))
    if summary.get("source_confirmed_volatility_anomalies_truncated") is True:
        lines.append(f"Showing {len(rows)} of {total} matching settlements.")
    for row in rows:
        if not isinstance(row, dict):
            raise ExportQualityError(
                "source-confirmed volatility anomaly row is invalid")
        lines.append(
            f"{row.get('ticker')} {row.get('kind')} {row.get('day')}: "
            f"value={row.get('value'):g}; reason={row.get('reason')}; "
            f"confirmed={row.get('confirmed')}; run={row.get('run')}.")


def render_text(report):
    """Render deterministic UTF-8 note text from a validated report object."""
    if (not isinstance(report, dict) or report.get("kind") != REPORT_KIND
            or report.get("version") != REPORT_VERSION):
        raise ExportQualityError("invalid export quality report")
    rows = report.get("rows")
    if not isinstance(rows, list):
        raise ExportQualityError("export quality rows are invalid")
    quality = (report.get("summary") or {}).get("quality") or {}
    confirmed = [row for row in rows if row.get("quality") == "CONFIRMED_BAD"]
    headline = ("none" if not confirmed
                else ", ".join(row["ticker"] for row in confirmed))
    batch = report["batch"]
    export = report["summary"]["export"]
    lines = [
        "EXPORT DATA QUALITY NOTE",
        f"Confirmed bad tickers: {headline}",
        "",
        "BATCH IDENTITY",
        f"Run: {batch['run_id']}",
        f"Generated: {batch['generated_at']}",
        f"State: {batch['state']}",
        f"Format / interval / sessions: {batch['format']} / {batch['interval']} / {batch['sessions']}",
        f"Range: {batch['start'] or 'all'} to {batch['end'] or 'latest'}",
        f"Destination: {batch['destination']}",
        "",
        "EXPORT SUMMARY",
        (f"Selected={report['summary']['selected']}; Present={report['summary']['present']}; "
         f"Written={export['WRITTEN']}; Rows={export['ROWS']}; Empty={export['EMPTY']}; "
         f"Failed={export['FAILED']}; Cancelled={export['CANCELLED']}; "
         f"Not-in-bank={export['NOT_IN_BANK']}; Missing-month warnings={export['MISSING_MONTHS']}"),
        "",
        "QUALITY SUMMARY",
        (f"Confirmed-bad={quality.get('CONFIRMED_BAD', 0)}; "
         f"Review-required={quality.get('REVIEW_REQUIRED', 0)}; "
         f"Known-difference={quality.get('KNOWN_DIFFERENCE', 0)}; "
         f"Unverifiable={quality.get('UNVERIFIABLE', 0)}; "
         f"Clean={quality.get('CLEAN', 0)}; "
         f"No-current-evidence={quality.get('NO_CURRENT_EVIDENCE', 0)}"),
    ]
    _section(lines, "CONFIRMED BAD - ACTION REQUIRED", confirmed)
    _section(lines, "REVIEW REQUIRED", [
        row for row in rows if row.get("quality") == "REVIEW_REQUIRED"])
    _section(lines, "KNOWN DIFFERENCES", [
        row for row in rows if row.get("quality") == "KNOWN_DIFFERENCE"])
    _section(lines, "UNVERIFIABLE OR STALE", [
        row for row in rows if row.get("quality") in {
            "UNVERIFIABLE", "NO_CURRENT_EVIDENCE"}])
    _render_coverage_evidence(lines, rows)
    _render_gap_evidence(lines, rows)
    _render_validation_evidence(lines, rows)
    _render_settled_volatility(lines, report)

    warning_rows = [
        row for row in rows
        if row.get("export_status") != "WRITTEN" or row.get("export_warnings")
    ]
    lines.extend(("", "EXPORT WARNINGS"))
    if not warning_rows:
        lines.append("None.")
    else:
        for row in warning_rows:
            detail = f"{row['ticker']}: {row['export_status']}"
            if row.get("export_error"):
                detail += f" - {row['export_error']}"
            if row.get("export_warnings"):
                detail += " - " + "; ".join(row["export_warnings"])
            lines.append(detail)

    lines.extend(("", "EVIDENCE"))
    tier0 = (report.get("sources") or {}).get("tier0")
    if tier0:
        page_hashes = sorted({
            str(item.get("fingerprint"))
            for item in tier0.get("pages") or [] if item.get("fingerprint")
        })
        lines.append(
            f"Tier 0 repair evidence as-of: {tier0.get('asof') or 'unknown'} | "
            f"fingerprints={','.join(page_hashes) or 'unavailable'}")
        for source in tier0.get("ticker_details") or []:
            lines.append(
                f"Tier 0 ticker detail: {source['ticker']} | "
                f"fingerprint={source['fingerprint']}")
        for source in tier0.get("gap_details") or []:
            lines.append(
                f"Tier 0 gap detail: {source['ticker']} | "
                f"fingerprint={source['fingerprint']}")
    for source in (report.get("sources") or {}).get("ws7") or []:
        lines.append(
            f"WS7: {source['basename']} | SHA-256={source['sha256']} | "
            f"version={source['version']} | finished={source['finished_at']}")
    historical = (report.get("sources") or {}).get("ws7_historical") or []
    if historical:
        labels = ", ".join(
            f"{item['basename']} (v{item['version']})" for item in historical)
        lines.append(f"Historical WS7 artifacts ignored: {labels}")
    xval = (report.get("sources") or {}).get("cross_validation")
    if xval:
        lines.append(
            f"Cross-validation: {xval['basename']} | SHA-256={xval['sha256']} | "
            f"schema={xval['schema_version']} | "
            f"latest={xval.get('latest_finished_at') or 'unknown'} | "
            f"current={xval.get('current_entries', 0)} | "
            f"price-review={xval.get('current_price_review', 0)} | "
            f"price-informational={xval.get('current_price_informational', 0)} | "
            f"known-difference={xval.get('current_known_difference', 0)} | "
            f"volume-only={xval.get('current_volume_only', 0)} | "
            f"legacy-ignored={xval.get('ignored_legacy', 0)} | "
            f"stale-ignored={xval.get('ignored_stale', 0)} | "
            f"malformed-ignored={xval.get('ignored_malformed', 0)}")
    combined = (report.get("sources") or {}).get("combined_flags")
    if combined:
        lines.append(
            f"Combined flags: {combined['basename']} | "
            f"SHA-256={combined['sha256']} | "
            f"schema={combined['schema_version']} | "
            f"latest={combined.get('latest_asof') or 'unknown'} | "
            f"current={combined.get('current_entries', 0)} | "
            f"ok={combined.get('current_ok', 0)} | "
            f"flagged={combined.get('current_flagged', 0)} | "
            f"error={combined.get('current_error', 0)} | "
            f"inconclusive={combined.get('current_inconclusive', 0)} | "
            f"legacy-ignored={combined.get('ignored_legacy', 0)} | "
            f"stale-ignored={combined.get('ignored_stale', 0)} | "
            f"malformed-ignored={combined.get('ignored_malformed', 0)}")
    coverage = (report.get("sources") or {}).get("coverage_audit")
    if coverage:
        lines.append(
            f"Coverage audit: {coverage['basename']} | "
            f"SHA-256={coverage['sha256']} | "
            f"schema={coverage['schema_version']} | "
            f"as-of={coverage.get('asof') or 'unknown'} | "
            f"kind-stale={coverage.get('kind_forward_stale', 0)} | "
            f"selected-stale={coverage.get('selected_stale', 0)}")
    settled_source = (report.get("sources") or {}).get("vol_value_settled")
    if settled_source:
        lines.append(
            f"Volatility settlements: {settled_source['basename']} | "
            f"SHA-256={settled_source['sha256']} | "
            f"entries={settled_source.get('entries', 0)}")
    for error in (report.get("sources") or {}).get("errors") or []:
        label = error.get("source") or "evidence"
        ticker = f" {error['ticker']}" if error.get("ticker") else ""
        lines.append(f"Evidence warning [{label}{ticker}]: {error.get('error')}")
    lines.extend((
        "This note is informational. It never authorizes an automatic fetch, basis action, correction, truncation, or bank rewrite.",
        "Market-data file bytes and schemas are not modified by this note.",
    ))
    text = "\n".join(lines) + "\n"
    if len(text.encode("utf-8")) > MAX_NOTE_BYTES:
        raise ExportQualityError("export quality note is too large")
    return text


def _note_clock(value=None):
    if value is None:
        parsed = dt.datetime.now().astimezone()
    elif isinstance(value, dt.datetime):
        parsed = value
    else:
        raise ExportQualityError("injected note clock must be a datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExportQualityError("injected note clock must include a timezone")
    return parsed


def _reserve_note_path(destination, *, now=None, pid=None, counter=None):
    destination = Path(destination).resolve()
    stamp = _note_clock(now).strftime("%Y%m%d-%H%M%S")
    process = os.getpid() if pid is None else _nonnegative_int(pid, "note pid")
    base = next(_NOTE_COUNTER) if counter is None else _nonnegative_int(
        counter, "note counter")
    for collision in range(1_000):
        serial = base + collision
        path = destination / (
            f"EXPORT_DATA_QUALITY_{stamp}-{process}-{serial:04d}.txt")
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            continue
        except OSError as exc:
            raise ExportQualityError(
                f"cannot reserve export quality note: {type(exc).__name__}") from exc
        os.close(fd)
        return path
    raise ExportQualityError("could not allocate a unique export quality note name")


def write_report_note(report, destination, *, storage_root=STORAGE_ROOT,
                      now=None, pid=None, counter=None):
    """Atomically write one unique note outside the bank; never overwrite."""
    text = render_text(report)
    payload = text.encode("utf-8")
    destination = Path(destination).resolve()
    bank = Path(storage_root).resolve()
    if _inside(destination, bank):
        raise ExportQualityError("export quality notes cannot be written inside the bank")
    if not destination.is_dir():
        raise ExportQualityError("export quality note destination is not a directory")
    path = _reserve_note_path(
        destination, now=now, pid=pid, counter=counter)
    committed = False
    try:
        try:
            storage._atomic_write_bytes(path, payload)
            raw, _stat = _read_stable_bytes(path, MAX_NOTE_BYTES)
            if raw != payload:
                raise ExportQualityError("export quality note readback mismatch")
            committed = True
            return path
        except ExportQualityError:
            raise
        except Exception as exc:  # noqa: BLE001 - normalize writer failures
            raise ExportQualityError(
                f"cannot write export quality note: {type(exc).__name__}") from exc
    finally:
        if not committed and path.exists():
            try:
                path.unlink()
            except OSError:
                pass


__all__ = [
    "ExportQualityError",
    "EvidenceError",
    "EXPORT_STATUSES",
    "QUALITY_ASSESSMENTS",
    "build_report",
    "ensure_current_health",
    "render_text",
    "write_report_note",
]
