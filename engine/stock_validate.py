"""Daily-OHLC accuracy validator (GUI-free).

Cross-checks a stored intraday or canonical daily series against a free daily
reference. Intraday RTH bars are rolled up; daily bars are compared on their
canonical calendar dates. Both paths compare high/low/open/close — but
flagging ONLY SIGNIFICANT discrepancies (splits, scale errors, wrong symbol,
missing chunks), never penny/auction noise. Tolerances are configurable and
calibrated empirically (see _validate_calibrate.py).

NOT a bottleneck by design:
  * ONE cached HTTP call per ticker (decades of daily data in ~25 KB), 15-min TTL;
  * a single O(bars) aggregation pass (a 10-yr 1m series rolls up in seconds);
  * run ON DEMAND only — never on the fetch hot path.

Reference: stockanalysis.com's history API returns RAW (unadjusted) o/h/l/c plus
an adjusted close `a`; we compare against the RAW fields, matching IBKR TRADES.
The network call lives ONLY in fetch_daily_reference; everything else is pure and
unit-testable with an injected reference.
"""
import hashlib
import json
import threading
import time as _time
import urllib.request
from collections import defaultdict
from copy import deepcopy
from datetime import date, datetime, time as dtime, timedelta as _timedelta
from pathlib import Path

import gap_evidence
import derived_daily_cache as ddc
import fetch_operations as fops
import stock_storage as ss

_CROSS_LOCK = threading.Lock()     # serialises sidecar read-modify-write

RTH_FIRST = dtime(9, 30)
RTH_LAST = dtime(15, 59, 59)
RTH_FIRST_SEC = 9 * 3600 + 30 * 60
RTH_LAST_SEC = 15 * 3600 + 59 * 60 + 59

REF_URL = ("https://stockanalysis.com/api/symbol/s/{t}/history"
           "?range={r}&period=Daily")
_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
_STOCKANALYSIS_SYMBOL_OVERRIDES = {
    "BF-B": "BF.B",
    "BRK-B": "BRK.B",
}
_REF_CACHE = {}        # (ticker, range) -> (fetched_ts, {date_iso: (o,h,l,c,v)})
_REF_TTL = 900         # 15 min — a re-validate within the window reuses the call
_REF_LOCK = threading.RLock()

# Calibrated against 10 yr of AAPL 1m vs the live reference (see
# _validate_calibrate.py). With the rolling-baseline comparison, on IBKR (raw)
# data even 'strict' gives ~0 false positives; the 'normal'/'loose' presets also
# suppress the ex-dividend-day basis steps that appear when comparing
# dividend-ADJUSTED data against the raw reference — so only BIG, real anomalies
# (splits, scale errors, wrong symbol, missing chunks) flag. Default = "normal".
TOL_HIGH_LOW = 0.010        # 1.0%
TOL_OPEN_CLOSE = 0.020      # 2.0%
TOL_VOLUME = 0.5            # 50% off the local volume trend = missing/extra chunk
MIN_DAY_BARS = 50           # a day with fewer stored bars isn't a fair daily OHLC
FULL_HISTORY_HEAD_TOLERANCE_DAYS = 45
FULL_HISTORY_RANGE = "Max"
_FULL_HISTORY_RANGE_TOKENS = frozenset({"max"})

SENSITIVITY = {             # (tol_high_low, tol_open_close, tol_volume)
    "strict": (0.004, 0.012, 0.5),
    "normal": (0.010, 0.020, 0.5),
    "loose":  (0.025, 0.040, 0.75),
}


# --- reference (the only network touch) -------------------------------------

def stockanalysis_symbol(ticker):
    """Return the provider spelling while preserving storage identity aliases."""
    raw = str(ticker or "").strip().upper()
    canonical = ss.canonical_ticker(raw)
    return _STOCKANALYSIS_SYMBOL_OVERRIDES.get(canonical, raw)


def _terminal_reference_error(exc, worker):
    """Guard and ledger faults must escape result-oriented legacy wrappers."""
    if worker is None:
        return False
    from fetch_authority import AuthorityError
    from fetch_ledger import LedgerError
    from fetch_run_context import RequestCancelled, RequestRefused
    return isinstance(exc, (AuthorityError, LedgerError, RequestCancelled,
                            RequestRefused))


def fetch_daily_reference(ticker, rng="5Y", _now=None, _opener=None,
                          _fetch_worker=None, _pending_cache=None,
                          _force_fresh=False, _run_cache=None, _cancel=None):
    """{date_iso: (o,h,l,c,v)} RAW daily OHLC. Cached 15 min per (ticker,range).
    `_opener(url)->text` is injectable for parser tests (no network).
    A guarded worker path never trusts a legacy cache hit or raw opener."""
    provider_symbol = stockanalysis_symbol(ticker)
    if _fetch_worker is not None:
        from fetch_authority import digest_value
        from fetch_envelopes import parse_envelope
        from fetch_http_labels import (filter_stockanalysis_rows,
            guarded_stockanalysis_attempt, stockanalysis_bounds,
            stockanalysis_url)
        from fetch_run_context import FetchWorker, RequestRefused
        from fetch_test_policy import is_live_capability

        if type(_fetch_worker) is not FetchWorker:
            raise RequestRefused("StockAnalysis cache requires an operation worker")
        context = _fetch_worker.context
        fops.check_worker(context, _fetch_worker.worker_id)
        if ((context.test_capability is not None or _opener is not None)
                and not is_live_capability(context.test_capability)):
            raise RequestRefused("StockAnalysis injected cache and transport require offline policy")
        bounds = stockanalysis_bounds(context.authority, context.captured_now,
                                      rng)
        envelope = parse_envelope({
            "variant": "http-series", "endpoint": "stockanalysis.history",
            "subject": provider_symbol, "token": "1d",
            "intended_start": bounds["intended_start"],
            "intended_end": bounds["intended_end"]})
        if not context.permits("http.stockanalysis.validation", envelope):
            raise RequestRefused("validation cache lacks operation rights")
        url = stockanalysis_url(provider_symbol, rng)
        key = (provider_symbol, rng)
        now = _now if _now is not None else _time.time()
        if _run_cache is not None and key in _run_cache:
            return deepcopy(_run_cache[key])
        with _REF_LOCK:
            hit = deepcopy(_REF_CACHE.get(key))
        expected = {
            "url": url, "range": rng,
            "requested_start_date": bounds["requested_start_date"],
            "truncated_coverage": bounds["truncated_coverage"],
            "intended_start": bounds["intended_start"].isoformat(),
            "intended_end": bounds["intended_end"].isoformat(),
            "authority_fingerprint": context.authority.fingerprint,
            "table_digest": context.authority.table_digest,
        }
        if (not _force_fresh
                and _verified_reference_hit(hit, expected, now, envelope,
                                            context)):
            return _reference_from_accepted(hit["rows"])
        captured = {}
        def one_send(url):
            raw = _opener(url)
            raw = raw.encode("utf-8") if isinstance(raw, str) else raw
            if isinstance(raw, bytes):
                captured["raw_digest"] = hashlib.sha256(raw).hexdigest()
            return raw
        rows = guarded_stockanalysis_attempt(
            _fetch_worker, "http.stockanalysis.validation",
            provider_symbol, rng, one_send if _opener is not None else None,
            cancel=_cancel, _capture=captured)
        reference = _reference_from_accepted(rows)
        if _run_cache is not None:
            _run_cache[key] = deepcopy(reference)
        if _pending_cache is not None:
            _pending_cache.append((key, {
                "kind": "verified_http_reference", "version": 1,
                "fetched_ts": now, **expected,
                "source_bytes_sha256": captured["raw_digest"],
                "operation_id": context.operation_id,
                "accepted_digest": digest_value(rows),
                "rows": deepcopy(rows),
            }))
        return reference
    from fetch_ibkr_bridge import refuse_a2
    refuse_a2()  # Including a warm legacy cache hit: no context-less replay.
    key = (provider_symbol, rng)
    now = _now if _now is not None else _time.time()
    hit = _REF_CACHE.get(key)
    if hit and now - hit[0] < _REF_TTL:
        return hit[1]
    if _opener is not None:
        raw = _opener(REF_URL.format(t=provider_symbol, r=rng))
    else:
        req = urllib.request.Request(REF_URL.format(t=provider_symbol, r=rng),
                                     headers=_UA)
        raw = urllib.request.urlopen(req, timeout=20).read().decode("utf-8")
    out = _parse_reference(raw)
    _REF_CACHE[key] = (now, out)
    return out


def _reference_from_accepted(rows):
    return {row["label"]: (row["o"], row["h"], row["l"],
                           row["c"], int(round(row["v"]))) for row in rows}


def _verified_reference_hit(hit, expected, now, envelope, context):
    """A damaged or legacy entry is a miss, never reusable evidence."""
    from fetch_authority import AuthorityError, digest_value
    from fetch_http_labels import filter_stockanalysis_rows
    from fetch_ledger import LedgerError, inspect_ledger

    if not (isinstance(hit, dict)
            and hit.get("kind") == "verified_http_reference"
            and hit.get("version") == 1
            and all(hit.get(k) == v for k, v in expected.items())
            and type(hit.get("fetched_ts")) in (int, float)
            and 0 <= now - hit["fetched_ts"] < _REF_TTL
            and isinstance(hit.get("rows"), list)):
        return False
    try:
        rows = hit["rows"]
        return (hit.get("accepted_digest") == digest_value(rows)
                and filter_stockanalysis_rows(rows, envelope, context) == rows
                and _reference_ledger_attempt(
                    hit, inspect_ledger(Path(hit["ledger_path"])),
                    required_attempt=hit["attempt_id"]) == hit["attempt_id"])
    except (AuthorityError, LedgerError, KeyError, TypeError, ValueError,
            OverflowError, ArithmeticError):
        return False


def _reference_ledger_attempt(entry, audit, *, required_attempt=None):
    """Bind one cached value to its exact sealed decision/result pair."""
    from fetch_authority import digest_value
    from fetch_ledger import LedgerError

    if not audit["verified"] or audit["operation_id"] != entry["operation_id"]:
        raise LedgerError("reference cache lacks a sealed source operation")
    if (not isinstance(entry.get("rows"), list)
            or entry.get("accepted_digest") != digest_value(entry["rows"])):
        raise LedgerError("reference cache rows differ from the durable result")
    request = {
        "variant": "http-series", "endpoint": "stockanalysis.history",
        "subject": entry["provider_symbol"], "token": "1d",
        "intended_start": entry["intended_start"],
        "intended_end": entry["intended_end"],
    }
    http_request = {key: entry[key] for key in
                    ("url", "range", "requested_start_date",
                     "truncated_coverage")}
    decisions = {}
    matches = []
    for event in audit["events"]:
        attempt = event["attempt_id"]
        if event["event"] == "decision":
            decisions[attempt] = event
        elif event["event"] == "result" and attempt in decisions:
            decision = decisions[attempt]
            payload = event["payload"]
            accepted = payload.get("accepted")
            if (event["operation_id"] == entry["operation_id"]
                    and event["producer_id"] ==
                    "http.stockanalysis.validation"
                    and decision["payload"].get("requested") == request
                    and decision["payload"].get("effective") == request
                    and decision["payload"].get("http_request") == http_request
                    and decision["payload"].get("authority_fingerprint") ==
                    entry["authority_fingerprint"]
                    and decision["payload"].get("table_digest") ==
                    entry["table_digest"]
                    and payload.get("outcome") in {"returned", "empty"}
                    and payload.get("raw_response_digest") ==
                    entry["source_bytes_sha256"]
                    and isinstance(accepted, dict)
                    and accepted.get("digest") == entry["accepted_digest"]
                    and accepted.get("count") == len(entry["rows"])
                    and (required_attempt is None
                         or attempt == required_attempt)):
                matches.append(attempt)
    if len(matches) != 1:
        raise LedgerError("reference cache has no unique durable result")
    return matches[0]


def _publish_reference_cache(pending, context):
    """Publish only entries tied to the root's receipt-verified result."""
    from fetch_ledger import inspect_ledger

    if not pending:
        return
    audit = inspect_ledger(context.ledger.path)
    verified = []
    for key, entry in pending:
        item = deepcopy(entry)
        item["provider_symbol"] = key[0]
        item["attempt_id"] = _reference_ledger_attempt(item, audit)
        item["ledger_path"] = str(context.ledger.path)
        verified.append((key, item))
    with _REF_LOCK:
        for key, entry in verified:
            _REF_CACHE[key] = deepcopy(entry)


def _norm_date_key(t):
    """Normalise a reference date field to 'YYYY-MM-DD'. stockanalysis usually
    returns an ISO string, but the API has been seen to return a Unix epoch
    (seconds OR milliseconds) for `t`; an un-normalised epoch key would never
    match a derived `date.isoformat()`, silently yielding ZERO overlap (a clean
    'nothing wrong' result on nothing checked). Returns the original string if
    it can't be interpreted."""
    s = str(t).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":   # already ISO-ish
        return s[:10]
    try:
        v = int(float(s))
    except (ValueError, TypeError):
        return s
    if v > 10_000_000_000:          # looks like milliseconds
        v //= 1000
    if v < 100_000_000:             # too small to be a plausible epoch date
        return s
    from datetime import datetime, timezone
    try:
        return datetime.fromtimestamp(v, tz=timezone.utc).date().isoformat()
    except (ValueError, OSError, OverflowError):
        return s


def _parse_reference(raw):
    """stockanalysis history JSON -> {date_iso: (o,h,l,c,v)} (raw OHLC)."""
    data = json.loads(raw)
    out = {}
    for row in (data.get("data") if isinstance(data, dict) else data) or []:
        try:
            out[_norm_date_key(row["t"])] = (float(row["o"]), float(row["h"]),
                                             float(row["l"]), float(row["c"]),
                                             int(round(float(row["v"]))))
        except (KeyError, TypeError, ValueError):
            continue
    return out


# --- aggregation (pure) -----------------------------------------------------

def derive_daily(bars, min_bars=0, window=None):
    """{date: (o,h,l,c,v)} from stored bars in one pass.

    ``window`` is an inclusive pair of wall-clock times. The default remains
    RTH for every existing intraday caller; canonical daily validation passes
    the full-day storage window explicitly.
    """
    first_time, last_time = window or (RTH_FIRST, RTH_LAST)
    by_day = defaultdict(list)
    for b in bars:
        if first_time <= b[0].time() <= last_time:
            by_day[b[0].date()].append(b)
    out = {}
    for d, bs in by_day.items():
        if len(bs) < min_bars:
            continue
        bs.sort(key=lambda b: b[0])
        out[d] = (bs[0][1], max(b[2] for b in bs), min(b[3] for b in bs),
                  bs[-1][4], sum(b[5] for b in bs))
    return out


def resample_minute(sec_bars):
    """1s bars -> derived 1m {minute_dt: (o,h,l,c,v)} — ONE pass (the 1s->1m
    chain check, needs no internet)."""
    by_min = defaultdict(list)
    for b in sec_bars:
        by_min[b[0].replace(second=0)].append(b)
    out = {}
    for k, bs in by_min.items():
        bs.sort(key=lambda b: b[0])
        out[k] = (bs[0][1], max(b[2] for b in bs), min(b[3] for b in bs),
                  bs[-1][4], sum(b[5] for b in bs))
    return out


# --- comparison (pure) ------------------------------------------------------

def _median(xs):
    s = sorted(xs)
    n = len(s)
    if not n:
        return 1.0
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _rolling_median(values, window):
    """Per-element median over a centered window — tracks a slow drift (e.g. the
    dividend-adjustment basis) so only LOCAL departures stand out."""
    n = len(values)
    half = window // 2
    return [_median(values[max(0, i - half):min(n, i + half + 1)])
            for i in range(n)]


def _mad(values, center):
    """Median absolute deviation about `center` — a robust spread measure."""
    if not values:
        return 0.0
    return _median([abs(v - center) for v in values])


def _detect_level_shift(ratios, dates, min_side, window, step_tol):
    """Find the largest SUSTAINED level-shift (a STEP) in a ratio series — the
    signature of a split / basis change applied to only PART of the series. A
    centered rolling median self-adapts to any run >= half_window, so it
    silently absorbs exactly this case; this is the explicit detector that
    compare_daily relies on so a mid-series split is never missed.

    For each candidate boundary i, compares the median of the `min_side` points
    just BEFORE i against the median of the `min_side` points AT/after i (tight
    ADJACENT windows, so the boundary is localised to within ~min_side rather
    than smeared across the rolling window). A step needs `min_side` real points
    on BOTH sides, so a lone outlier can't fake one. Returns {index, date, lead,
    trail, factor, pct} for the biggest step whose |factor-1| exceeds `step_tol`,
    else None. (`window` is accepted for signature stability; the adjacent
    windows are sized by min_side.)"""
    n = len(ratios)
    if n < 2 * min_side:
        return None
    best = None
    for i in range(min_side, n - min_side + 1):
        lead = _median(ratios[i - min_side:i])
        trail = _median(ratios[i:i + min_side])
        if lead <= 0:
            continue
        factor = trail / lead
        dev = abs(factor - 1.0)
        if dev > step_tol and (best is None or dev > best["pct"]):
            best = {"index": i, "date": dates[i].isoformat(),
                    "lead": round(lead, 5), "trail": round(trail, 5),
                    "factor": round(factor, 5), "pct": round(dev, 5)}
    return best


def _interval_seconds(interval):
    """Seconds per bar for an interval token (suffix-stripped). Defaults to 60
    (a minute) for anything unrecognised."""
    base = ss.base_interval(interval)
    # leading digits then a single unit char (s/m/h)
    digits, unit = "", ""
    for ch in base:
        if ch.isdigit():
            digits += ch
        else:
            unit = ch
            break
    try:
        n = int(digits)
    except ValueError:
        return 60
    return n * {"s": 1, "m": 60, "h": 3600}.get(unit, 60)


# A regular session is 09:30..16:00 = 390 minutes.
RTH_SESSION_MINUTES = 390


def _expected_rth_bars(interval):
    """Bars a COMPLETE regular session yields at `interval` (e.g. 1m->390,
    15m->26, 30m->13, 1h->6, 1s->23400)."""
    return max(1, int(RTH_SESSION_MINUTES * 60 // _interval_seconds(interval)))


# index of each field in a (open, high, low, close, volume) tuple
_FIELD_IDX = {"open": 0, "high": 1, "low": 2, "close": 3, "volume": 4}


def compare_daily(derived, reference, tol_hl=TOL_HIGH_LOW,
                  tol_oc=TOL_OPEN_CLOSE, tol_vol=TOL_VOLUME, window=21):
    """Compare derived vs reference daily OHLC, ROBUST to a systematic, possibly
    DRIFTING price/volume BASIS (your data may be dividend- or split-adjusted
    while the free reference is raw — a smooth ~1%/yr drift, NOT noise). For each
    field the per-day (derived/reference) ratio is divided by its ROLLING-MEDIAN
    baseline (a `window`-day local trend), so the smooth basis drift is tracked
    out and ONLY SHARP departures flag: a missing chunk is a SPIKE the rolling
    baseline reveals.

    A SPLIT / partial-series basis change is a STEP, NOT a spike — and a centered
    rolling median self-adapts to any run >= half the window, so it would
    SILENTLY ABSORB exactly that case (the headline failure this function must
    avoid). So in addition to the per-day spike test we run an explicit
    level-shift detector (`_detect_level_shift`) on the high+low ratio series;
    a detected step is reported as `level_shift` AND its boundary day is flagged
    so the score and the 'matches within tolerance' verdict reflect it.

    high/low use tol_hl, open/close tol_oc, volume tol_vol. A UNIFORM
    off-by-a-factor (whole-series split / wrong basis) is surfaced as
    `basis_ratio`/`suspected_factor` — but ONLY when the ratio distribution is
    actually unimodal; a bimodal distribution (a real mid-series split, half ~2
    half ~1) whose median is a meaningless ~1.5 is reported as a `level_shift`
    instead, never as a phantom uniform factor. Returns {checked, matched,
    score, basis_ratio, suspected_factor, level_shift, max_norm_dev, flagged,
    flagged_days}."""
    pairs = [(d, derived[d], reference[d.isoformat()])
             for d in sorted(set(derived)) if d.isoformat() in reference]
    if not pairs:
        return {"checked": 0, "matched": 0, "score": None, "basis_ratio": 1.0,
                "suspected_factor": None, "level_shift": None,
                "max_norm_dev": 0.0, "flagged": [], "flagged_days": []}
    tol_for = {"high": tol_hl, "low": tol_hl, "close": tol_oc, "open": tol_oc,
               "volume": tol_vol}
    dates = [d for d, _der, _ref in pairs]
    ratios = {f: [] for f in _FIELD_IDX}
    for _d, der, ref in pairs:
        for f, idx in _FIELD_IDX.items():
            ratios[f].append(der[idx] / ref[idx] if ref[idx] else 1.0)
    baseline = {f: _rolling_median(ratios[f], window) for f in _FIELD_IDX}
    flagged, max_norm = [], 0.0
    for i, (d, der, ref) in enumerate(pairs):
        for f, idx in _FIELD_IDX.items():
            base = baseline[f][i] or 1.0
            ndev = abs(ratios[f][i] / base - 1.0)
            if f in ("high", "low"):
                max_norm = max(max_norm, ndev)
            if ndev > tol_for[f]:
                flagged.append({"date": d.isoformat(), "field": f,
                                "derived": der[idx], "reference": ref[idx],
                                "norm_dev": round(ndev, 5)})
    checked = len(pairs)

    # --- explicit STEP detection (what the rolling median absorbs) ----------
    hl_ratios = [(ratios["high"][i] + ratios["low"][i]) / 2.0
                 for i in range(checked)]
    min_side = max(5, window // 3)
    step_tol = max(2 * tol_hl, 0.02)        # a real split/basis step is sizable;
    #                                         smooth dividend drift stays under it
    shift = _detect_level_shift(hl_ratios, dates, min_side, window, step_tol)
    if shift is not None:
        # surface the boundary day so the score / verdict can't read 'clean'
        bday = shift["date"]
        flagged.append({"date": bday, "field": "level_shift",
                        "derived": shift["trail"], "reference": shift["lead"],
                        "norm_dev": shift["pct"]})

    flagged_days = sorted({f["date"] for f in flagged})

    # --- uniform-basis factor — ONLY when genuinely uniform (unimodal) ------
    hl = ratios["high"] + ratios["low"]
    basis = _median(hl)
    spread = _mad(hl, basis) / (basis or 1.0)
    uniform = shift is None and spread < 0.02     # tight cluster about the median
    suspected = round(basis, 4) if (abs(basis - 1.0) > 0.10 and uniform) else None

    return {"checked": checked, "matched": checked - len(flagged_days),
            "score": round(1 - len(flagged_days) / checked, 4),
            "basis_ratio": round(basis, 5), "suspected_factor": suspected,
            "level_shift": shift, "max_norm_dev": round(max_norm, 5),
            "flagged": flagged, "flagged_days": flagged_days}


def compare_minute_chain(sec_bars, min_bars, tol_hl=0.002):
    """Resample 1s -> 1m and compare to the stored 1m bars (internal, no
    internet). Returns match counts for high/low over the shared minutes."""
    derived = resample_minute(sec_bars)
    m1 = {b[0]: b for b in min_bars}
    common = sorted(set(derived) & set(m1))
    hi = sum(1 for k in common
             if abs(derived[k][1] - m1[k][2]) / (m1[k][2] or 1) <= tol_hl)
    lo = sum(1 for k in common
             if abs(derived[k][2] - m1[k][3]) / (m1[k][3] or 1) <= tol_hl)
    return {"common_minutes": len(common), "high_match": hi, "low_match": lo,
            "score": round((hi + lo) / (2 * len(common)), 4) if common else None}


def _d(iso):
    from datetime import date
    try:
        return date.fromisoformat(iso)
    except (ValueError, TypeError):
        return None


# --- orchestration ----------------------------------------------------------

def _readable_month_keys(mm, recent_months=None):
    months = [mk for mk in sorted(mm)
              if not (isinstance(mm.get(mk), dict)
                      and mm.get(mk, {}).get("status") == "MISSING")]
    if recent_months is not None and recent_months > 0:
        months = months[-recent_months:]
    return months


def read_series(root, ticker, interval, recent_months=None):
    """All stored bars for a (ticker, interval) series across its months. With
    `recent_months=N`, reads ONLY the most recent N months — so an ingest-time
    cross-check of just-fetched data doesn't re-read a decade of bars per
    series (and the daily reference range can be matched to the window)."""
    canon = ss.canonical_ticker(ticker)
    man = ss.load_manifest(Path(root) / canon) or {}
    mm = ss.manifest_months(man, interval)
    months = _readable_month_keys(mm, recent_months)
    bars = []
    for mk in months:
        fp = (ss.find_month_file(root, canon, int(mk[:4]), int(mk[5:7]), interval)
              or ss.month_file_path(root, canon, int(mk[:4]), int(mk[5:7]),
                                    interval))
        ent = mm.get(mk)
        sha = ent.get("sha256") if isinstance(ent, dict) else None
        b, _ = ss.read_month_file_fast(fp, sha)
        bars.extend(b)
    return bars


_EPOCH_DT = datetime(1970, 1, 1)          # the ts-only read's naive epoch anchor
_EPOCH_ORD = date(1970, 1, 1).toordinal()  # ordinal of day index 0


def read_series_ts(root, ticker, interval, recent_months=None):
    """TIMESTAMP-ONLY read_series for the gap scans (which never look at
    o/h/l/c/v): the same manifest walk and month ordering, but each month read
    through ss.read_month_file_ts — the sha-gated ts-column fast path with the
    STRICT read as fallback, so a modified/corrupt file surfaces exactly like
    read_series. Returns one list of int epoch-seconds of the stored naive-
    Eastern wall stamps (month-sorted, in-file order preserved)."""
    canon = ss.canonical_ticker(ticker)
    man = ss.load_manifest(Path(root) / canon) or {}
    mm = ss.manifest_months(man, interval)
    months = _readable_month_keys(mm, recent_months)
    ts = []
    for mk in months:
        fp = (ss.find_month_file(root, canon, int(mk[:4]), int(mk[5:7]), interval)
              or ss.month_file_path(root, canon, int(mk[:4]), int(mk[5:7]),
                                    interval))
        ent = mm.get(mk)
        sha = ent.get("sha256") if isinstance(ent, dict) else None
        ts.extend(ss.read_month_file_ts(fp, sha))
    return ts


def read_series_columns(root, ticker, interval, recent_months=None):
    """Columnar read_series for cross-validation: same manifest walk as
    read_series, but concatenates the six numpy columns per readable month.
    """
    import numpy as np
    canon = ss.canonical_ticker(ticker)
    man = ss.load_manifest(Path(root) / canon) or {}
    mm = ss.manifest_months(man, interval)
    months = _readable_month_keys(mm, recent_months)
    cols = [[], [], [], [], [], []]
    for mk in months:
        fp = (ss.find_month_file(root, canon, int(mk[:4]), int(mk[5:7]), interval)
              or ss.month_file_path(root, canon, int(mk[:4]), int(mk[5:7]),
                                    interval))
        ent = mm.get(mk)
        sha = ent.get("sha256") if isinstance(ent, dict) else None
        part = ss.read_month_file_cols(fp, sha)
        for i in range(6):
            cols[i].append(part[i])
    dtypes = [np.int64, np.float64, np.float64, np.float64, np.float64,
              np.int64]
    return tuple(np.concatenate(c) if c else np.array([], dtype=dtypes[i])
                 for i, c in enumerate(cols))


def derive_daily_fast(cols, min_bars=0, window=None):
    """Vectorized equivalent of derive_daily for columnar OHLCV arrays."""
    import numpy as np
    ts, o, h, lo, c, v = cols
    ts = np.asarray(ts, dtype=np.int64)
    if ts.size == 0:
        return {}
    o = np.asarray(o, dtype=np.float64)
    h = np.asarray(h, dtype=np.float64)
    lo = np.asarray(lo, dtype=np.float64)
    c = np.asarray(c, dtype=np.float64)
    v = np.asarray(v, dtype=np.int64)
    if not np.all(np.diff(ts) >= 0):
        order = np.argsort(ts, kind="stable")
        ts, o, h, lo, c, v = (ts[order], o[order], h[order], lo[order],
                              c[order], v[order])
    if window is None:
        first_sec, last_sec = RTH_FIRST_SEC, RTH_LAST_SEC
    else:
        first_time, last_time = window
        first_sec = (first_time.hour * 3600 + first_time.minute * 60
                     + first_time.second)
        last_sec = (last_time.hour * 3600 + last_time.minute * 60
                    + last_time.second)
    sod = ts % 86400
    mask = (sod >= first_sec) & (sod <= last_sec)
    ts, o, h, lo, c, v = ts[mask], o[mask], h[mask], lo[mask], c[mask], v[mask]
    if ts.size == 0:
        return {}
    day = ts // 86400
    uniq, first, counts = np.unique(day, return_index=True, return_counts=True)
    last = first + counts - 1
    hi = np.maximum.reduceat(h, first)
    low = np.minimum.reduceat(lo, first)
    vol = np.add.reduceat(v, first)
    out = {}
    for i in range(uniq.size):
        if counts[i] < min_bars:
            continue
        out[date.fromordinal(_EPOCH_ORD + int(uniq[i]))] = (
            float(o[first[i]]), float(hi[i]), float(low[i]),
            float(c[last[i]]), int(vol[i]))
    return out


def _xval_columnar_available():
    try:
        import numpy  # noqa: F401
        ss._require_pyarrow()
        return True
    except Exception:  # noqa: BLE001
        return False


def _num(x):
    """Format a price dropping trailing zeros (294.04, 291.9, 300) — matches the
    canonical export style."""
    s = f"{float(x):.6f}".rstrip("0").rstrip(".")
    return s if s else "0"


def save_daily_reference(root, ticker, rng, reference, asof=None):
    """Persist a fetched daily reference SNAPSHOT to a sidecar CSV for audit /
    provenance — written to a 'Validation Reference' folder ALONGSIDE the data
    bank (never inside it), stamped with the fetch date so you can later tell
    whether YOUR data or the online reference changed. Best-effort: returns the
    path, or None on failure (never raises into a validation)."""
    from datetime import date
    asof = asof or date.today().isoformat()
    out_dir = Path(root).parent / "Validation Reference"
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{ticker.upper()}_daily_{rng}_asof-{asof}.csv"
        lines = ["Date,open,high,low,close,volume"]
        for k in sorted(reference):
            o, h, lo, c, v = reference[k]
            lines.append(f"{k},{_num(o)},{_num(h)},{_num(lo)},{_num(c)},"
                         f"{int(round(float(v)))}")
        ss._atomic_write_bytes(
            path, ("\n".join(lines) + "\n").encode("utf-8"))
        return str(path)
    except (OSError, ValueError, TypeError, ss.StorageError):
        return None


def _coverage_day(value, label):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"invalid {label} date") from exc
    raise TypeError(f"invalid {label} date")


def _reference_coverage(derived, reference, rng):
    """Describe the range actually received and gate full-history claims."""
    stored_days = sorted(_coverage_day(value, "stored") for value in derived)
    reference_days = sorted(
        _coverage_day(value, "reference") for value in reference)
    if not stored_days or not reference_days:
        raise ValueError("reference range metadata is empty")
    stored_first, stored_last = stored_days[0], stored_days[-1]
    reference_first, reference_last = reference_days[0], reference_days[-1]
    head_gap_days = max(0, (reference_first - stored_first).days)
    full_history_requested = (
        str(rng or "").strip().casefold() in _FULL_HISTORY_RANGE_TOKENS)
    return {
        "reference_first_date": reference_first.isoformat(),
        "reference_last_date": reference_last.isoformat(),
        "reference_day_count": len(reference_days),
        "stored_first_date": stored_first.isoformat(),
        "stored_last_date": stored_last.isoformat(),
        "stored_day_count": len(stored_days),
        "full_history_requested": full_history_requested,
        "head_gap_days": head_gap_days,
        "head_tolerance_days": FULL_HISTORY_HEAD_TOLERANCE_DAYS,
        "full_history_head_ok": (
            head_gap_days <= FULL_HISTORY_HEAD_TOLERANCE_DAYS
            if full_history_requested else None),
    }


def _validation_directory(evidence_dir):
    return (Path(evidence_dir) if evidence_dir is not None else
            Path(__file__).resolve().parents[1] / "_ingest_reports" / "fetch-ledgers")


def fetch_daily_reference_single_request(ticker, rng="5Y", *,
                                         _test_capability=None,
                                         _evidence_dir=None, _authority=None,
                                         _clock=None, _governors=None,
                                         _opener=None):
    """Documented standalone one-request root; the helper itself never mints."""
    operation = fops.begin_operation(
        "validation", _validation_directory(_evidence_dir),
        test_capability=_test_capability, authority=_authority,
        clock=_clock, governors=_governors)
    try:
        fops.require_root_admission(operation)
        pending_cache = []
        with fops.scoped_worker(operation.child(
                "single-reference", rights={"http.stockanalysis.validation"}),
                close=True) as worker:
            result = fetch_daily_reference(
                ticker, rng, _fetch_worker=worker, _opener=_opener,
                _pending_cache=pending_cache)
        operation.seal()
        _publish_reference_cache(pending_cache, operation.context)
        return result
    finally:
        operation.close()


def validate_series(root, ticker, interval="1m", rng="5Y", sensitivity="normal",
                    read_fn=None, ref_fn=None, recent_months=None,
                    save_reference=False, *, _test_capability=None,
                    _evidence_dir=None, _authority=None, _clock=None,
                    _governors=None, _opener=None):
    """One unconditional held validation root; child body never mints."""
    operation = fops.begin_operation(
        "validation", _validation_directory(_evidence_dir),
        test_capability=_test_capability, authority=_authority,
        clock=_clock, governors=_governors)
    try:
        fops.require_root_admission(operation)
        pending_saves = []
        pending_cache = []
        with fops.scoped_worker(operation.child(
                "validation-series", rights={"http.stockanalysis.validation"}),
                close=True) as worker:
            result = _validate_series_body(
                root, ticker, interval, rng, sensitivity, read_fn, ref_fn,
                recent_months, save_reference,
                _fetch_worker=worker, _opener=_opener,
                _pending_saves=pending_saves,
                _pending_cache=pending_cache)
        operation.seal()
        _publish_reference_cache(pending_cache, operation.context)
        if pending_saves:
            saved = [save_daily_reference(*item) for item in pending_saves]
            result["reference_saved"] = saved[-1]
        return result
    finally:
        operation.close()


def _validate_series_body(root, ticker, interval="1m", rng="5Y",
                          sensitivity="normal", read_fn=None, ref_fn=None,
                          recent_months=None, save_reference=False,
                          *, _fetch_worker=None, _opener=None,
                          _pending_saves=None, _pending_cache=None,
                          _force_fresh=False, _run_cache=None):
    """Read the stored RTH/daily series, derive daily, fetch reference, compare.
    `sensitivity` ∈ SENSITIVITY (strict/normal/loose). `recent_months` bounds the
    read to the last N months (for an ingest-time check of just-fetched data).
    `save_reference` writes the fetched daily reference to a sidecar snapshot
    (path returned as `reference_saved`). `read_fn`/`ref_fn` are injectable for
    tests. Returns the compare_daily dict plus ticker/interval/sensitivity, or
    {'error': …}."""
    if ss.kind_of(interval) in ss.RATIO_KINDS:
        return {"ticker": ticker, "interval": interval, "sensitivity": sensitivity,
                "skipped": True, "score": None,
                "note": f"{ss.kind_of(interval)} is a volatility RATIO (not a "
                        f"dollar price) — no daily-price reference applies"}
    # Interval-aware day floor: a COMPLETE regular session yields only
    # 390/interval_minutes bars, so a flat MIN_DAY_BARS=50 would drop EVERY
    # 15m/30m/1h day (26/13/6 bars) and report 'no stored RTH bars' on perfectly
    # good data. Require ~half a full session instead, which still drops a
    # genuinely partial/corrupt day while accepting coarse intervals (and real
    # early-close half-days).
    is_daily = ss.base_interval(interval) == "1d"
    min_bars = (1 if is_daily else
                max(1, min(MIN_DAY_BARS,
                           int(_expected_rth_bars(interval) * 0.5))))
    aggregation_window = ss.DAILY_WINDOW if is_daily else None
    if read_fn is not None:
        bars = read_fn(root, ticker, interval)
        derived = derive_daily(
            bars, min_bars=min_bars, window=aggregation_window)
    elif _xval_columnar_available():
        derived = derive_daily_fast(
            read_series_columns(root, ticker, interval,
                                recent_months=recent_months),
            min_bars=min_bars, window=aggregation_window)
    else:
        bars = read_series(root, ticker, interval, recent_months=recent_months)
        derived = derive_daily(
            bars, min_bars=min_bars, window=aggregation_window)
    if not derived:
        label = "daily" if is_daily else "RTH"
        return {"error": f"no stored {label} bars to validate", "ticker": ticker}
    try:
        reference = (ref_fn(ticker, rng) if ref_fn is not None else
                     fetch_daily_reference(ticker, rng,
                        _fetch_worker=_fetch_worker, _opener=_opener,
                        _pending_cache=_pending_cache,
                        _force_fresh=_force_fresh,
                        _run_cache=_run_cache))
    except Exception as exc:  # noqa: BLE001
        from fetch_authority import ResponseQuarantined
        if _terminal_reference_error(exc, _fetch_worker):
            raise
        if _fetch_worker is not None and isinstance(exc, ResponseQuarantined):
            return {"error": f"reference response quarantined: {exc}",
                    "error_type": "ResponseQuarantined",
                    "quarantine": exc.quarantine_path, "ticker": ticker}
        return {"error": f"reference fetch failed: {exc}", "ticker": ticker}
    try:
        reference_coverage = _reference_coverage(derived, reference, rng)
        reference_coverage_error = None
    except (TypeError, ValueError) as exc:
        reference_coverage = None
        reference_coverage_error = _bounded_xval_text(exc)
    range_meta = {"reference_coverage": reference_coverage}
    if reference_coverage_error:
        range_meta["reference_coverage_error"] = reference_coverage_error
    # snapshot the reference (audit/provenance) right after fetch, regardless of
    # the comparison outcome, so a no-overlap run still records what was pulled.
    if save_reference and reference and _pending_saves is not None:
        _pending_saves.append((root, ticker, rng, reference))
        ref_path = None  # The root publishes only after a successful seal.
    else:
        ref_path = (save_daily_reference(root, ticker, rng, reference)
                    if save_reference and reference else None)
    tol_hl, tol_oc, tol_vol = SENSITIVITY.get(sensitivity, SENSITIVITY["normal"])
    res = compare_daily(derived, reference, tol_hl, tol_oc, tol_vol)
    # Zero overlap is INCONCLUSIVE, not a clean pass: if no derived day matched a
    # reference day (mismatched date format, non-overlapping ranges, wrong
    # symbol), compare_daily returns checked=0/score=None — surface that as an
    # explicit error so the UI never renders a green 'matches within tolerance'
    # having validated nothing.
    if res.get("checked", 0) == 0:
        return {"error": (f"no overlapping days between the stored {interval} "
                          f"data ({len(derived)} day(s)) and the reference "
                          f"({len(reference)} day(s)) — nothing could be "
                          f"validated"),
                "ticker": ticker, "interval": interval,
                "days_derived": len(derived), "reference_saved": ref_path,
                **range_meta}
    res.update(ticker=ticker, interval=interval, sensitivity=sensitivity,
               days_derived=len(derived), reference_saved=ref_path,
               **range_meta)
    return res


# --- extended-hours validation (no external reference exists) ---------------

def _day_groups(bars):
    """{date: time-sorted bars} for datetime-first bar tuples."""
    by = defaultdict(list)
    for b in bars:
        by[b[0].date()].append(b)
    for d in by:
        by[d].sort(key=lambda x: x[0])
    return by


def _ohlc_ok(b):
    """high ≥ max(open,close), low ≤ min(open,close), high ≥ low."""
    _dt, o, h, lo, c, _v = b
    return h >= max(o, c) and lo <= min(o, c) and h >= lo


# session windows for the structural check (storage also enforces these)
_PRE_WIN = (dtime(4, 0), dtime(9, 29, 59))
_POST_WIN = (dtime(16, 0), dtime(19, 59, 59))


# --- INTERNAL cross-check: stored intraday vs IBKR's OWN daily bars -----------
# Same compare machinery as validate_series, but the reference is IBKR daily TRADES
# (IBKR-vs-IBKR) instead of the external stockanalysis source — confirms the bank
# faithfully copies IBKR. STRUCTURE ONLY here; the daily-fetch RULES + the
# auto-fire-on-every-intraday-run wiring are TODO (need live ports) — see TODO.md.

def parse_ibkr_daily_reference(bars):
    """IBKR daily TRADES bars -> {date_iso: (o,h,l,c,v)} — the SAME shape as the
    stockanalysis reference, so compare_daily can use IBKR's own daily bars as the
    reference. Accepts cleaned tuples (dt,o,h,l,c,v) OR raw bar objects
    (.date/.open/.high/.low/.close/.volume)."""
    out = {}
    for b in bars or []:
        if hasattr(b, "open"):
            dt, o, h, lo, c, v = b.date, b.open, b.high, b.low, b.close, b.volume
        else:
            dt, o, h, lo, c, v = b[0], b[1], b[2], b[3], b[4], b[5]
        key = dt.date() if hasattr(dt, "date") else dt
        out[str(key)] = (float(o), float(h), float(lo), float(c), float(v))
    return out


# --- INTERNAL daily audit (IBKR-vs-IBKR fidelity, O/H/L exact) ---------------
# LIVE-CALIBRATED on the full 10yr bank (2026-06-24): VOLUME matched on ALL 197
# flagged dates -> it is the TRUE "every minute captured" fidelity signal. 93% of
# price flags were OPEN (median 0.27%, max 1.55%) = the opening auction/cross print
# vs the first 1m trade (the open-side twin of the closing-auction gap); close gaps
# reached 2.14% on a crash day. So PRICE below ~2% is auction noise, not a fetch issue.
INTERNAL_PRICE_TOL = 0.03      # O/H/L/CLOSE: 3% — catches splits (100%+)/garbage only
INTERNAL_VOL_TOL = 0.005       # VOLUME: 0.5% — gross drops (fine drops -> the gap scanner)


def _internal_norm_ref(daily_ref):
    """daily_ref keys may be iso-strings (parse_ibkr_daily_reference) or date
    objects -> normalize to dates for the join with derive_daily's date keys."""
    out = {}
    for k, v in (daily_ref or {}).items():
        d = k if hasattr(k, "year") else datetime.fromisoformat(str(k)[:10]).date()
        out[d] = v
    return out


def audit_internal_daily(derived, daily_ref):
    """Compare derived (1m->daily, {date:(o,h,l,c,v)}) vs IBKR's OWN daily.
    LIVE-CALIBRATED RULE (full-bank, see memory internal-daily-close-gap): VOLUME is
    the fidelity signal (flags >0.5% — it matched bank-wide, so a mismatch = dropped
    minutes); O/H/L/CLOSE flag only >3% (the OPEN/close auction-cross print vs the 1m
    bar inherently differs up to ~2%, so a smaller price gap is auction noise, not a
    fetch issue — this catches splits/garbage). -> {date: {field:
    {derived,ref,abs,rel}}} for the DISCREPANT dates only (empty dict == clean)."""
    ref = _internal_norm_ref(daily_ref)
    out = {}
    for d in sorted(set(derived) & set(ref)):
        bad = {}
        for name, dval, rval, kind in zip(
                ("open", "high", "low", "close", "volume"),
                (float(x) for x in derived[d]), (float(x) for x in ref[d]),
                ("price", "price", "price", "close", "vol")):
            ad = abs(dval - rval)
            rel = ad / abs(rval) if rval else (0.0 if ad == 0 else 1.0)
            flagged = (rel > INTERNAL_VOL_TOL if kind == "vol"
                       else rel > INTERNAL_PRICE_TOL)
            if flagged:
                bad[name] = {"derived": dval, "ref": rval, "abs": ad, "rel": rel}
        if bad:
            out[d] = bad
    return out


def internal_daily_audit(root, ticker, interval="1m", daily_ref=None,
                         read_fn=None, refetch=None, max_refetch=40):
    """One series' INTERNAL daily audit: aggregate the stored intraday series ->
    daily, compare vs IBKR's own daily (audit_internal_daily). For each DISCREPANT
    date, if `refetch(date) -> (derived_day, ref_day)` is provided, RE-FETCH + re-
    audit THAT date; a date that STILL disagrees is a CONFIRMED disagreement (✗),
    one that clears was a transient fetch glitch. `max_refetch` caps the paced
    re-fetches per series (excess discrepancies are flagged WITHOUT a refetch, so a
    badly-off series can't trigger a runaway). Returns the sidecar verdict."""
    from fetch_authority import AuthorityError
    from fetch_ledger import LedgerError
    from fetch_run_context import RequestCancelled
    bars = (read_fn(root, ticker, interval) if read_fn is not None
            else read_series(root, ticker, interval))
    derived = derive_daily(bars)
    cand = audit_internal_daily(derived, daily_ref)
    confirmed, transient = {}, {}
    n_ref = 0
    for d, bad in sorted(cand.items()):
        if refetch is None:
            confirmed[d] = bad
            continue
        if n_ref >= max_refetch:
            confirmed[d] = {**bad, "_refetch": "capped"}
            continue
        n_ref += 1
        try:
            d2, r2 = refetch(d)
            recheck = audit_internal_daily(d2, r2)
        except (AuthorityError, LedgerError, RequestCancelled):
            raise
        except Exception as exc:  # noqa: BLE001 — a failed refetch stays flagged
            confirmed[d] = {**bad, "_refetch_error": str(exc)}
            continue
        (confirmed if d in recheck else transient)[d] = recheck.get(d, bad)
    n = len(set(derived) & set(_internal_norm_ref(daily_ref)))
    status = ("inconclusive" if not n else "disagree" if confirmed else "ok")
    return {"ticker": ticker, "interval": interval, "status": status,
            "compared": n, "source": "ibkr-daily",
            "score": (round(1.0 - len(confirmed) / n, 4) if n else None),
            "confirmed": {str(k): v for k, v in sorted(confirmed.items())},
            "transient": {str(k): v for k, v in sorted(transient.items())}}


# --- 3-SOURCE agreement (external ∩ internal) + refetch-confirm -------------
_COMBINED_FILE = "_combined_flags.json"
_COMBINED_LOCK = threading.Lock()
COMBINED_FLAGS_SCHEMA_VERSION = 3
COMBINED_INTERNAL_INTERVAL = "1d"


def load_combined_flags(root):
    """Compound-keyed 3-source verdicts from the bank sidecar."""
    try:
        data = json.loads(
            (Path(root) / _COMBINED_FILE).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def combined_flags_key(ticker, interval):
    """Return the canonical `TICKER interval` combined-evidence key."""
    try:
        ticker = ss.canonical_ticker(ticker)
    except Exception:  # noqa: BLE001 - invalid identities cannot be persisted
        return ""
    interval = str(interval or "").strip()
    if not ss.INTERVAL_RE.fullmatch(interval):
        return ""
    return f"{ticker} {interval}"


def combined_entries_for_ticker(data, ticker, *, schema_version=None):
    """Return valid-looking legacy/compound entries for one canonical ticker."""
    try:
        ticker = ss.canonical_ticker(ticker)
    except Exception:  # noqa: BLE001
        return []
    out = []
    for key, entry in (data or {}).items():
        if not isinstance(entry, dict):
            continue
        if (schema_version is not None
                and entry.get("schema_version") != schema_version):
            continue
        try:
            entry_ticker = ss.canonical_ticker(entry.get("ticker"))
        except Exception:  # noqa: BLE001
            continue
        interval = str(entry.get("interval") or "").strip()
        expected = combined_flags_key(entry_ticker, interval)
        if (entry_ticker == ticker and expected
                and key in {ticker, expected}):
            out.append(entry)
    return sorted(out, key=lambda item: str(item.get("interval") or ""))


def record_combined_flags(root, entry):
    """Persist one series verdict under a compound key (thread-safe, atomic)."""
    key = combined_flags_key(
        (entry or {}).get("ticker"), (entry or {}).get("interval"))
    if not key:
        return None
    ticker = ss.canonical_ticker((entry or {}).get("ticker"))
    target = Path(root) / _COMBINED_FILE
    with _COMBINED_LOCK:
        data = load_combined_flags(root)
        data[key] = entry
        legacy = data.get(ticker)
        if (isinstance(legacy, dict)
                and str(legacy.get("interval") or "").strip()
                == str((entry or {}).get("interval") or "").strip()):
            data.pop(ticker, None)
        try:
            Path(root).mkdir(parents=True, exist_ok=True)
            ss._atomic_write_bytes(
                target,
                json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))
            return str(target)
        except Exception:  # noqa: BLE001 — best-effort
            return None


def combined_double_flag(derived, ext_ref, int_ref):
    """A date is DOUBLE-FLAGGED only when BOTH references disagree with the stored-
    1m-derived daily: the EXTERNAL (vs stockanalysis — compare_daily's flagged_days)
    AND the INTERNAL (vs IBKR's own daily — audit_internal_daily). High-signal:
    single-source noise (the open/close auction gap, a reference's adjustment basis)
    can NEVER fire it. -> sorted iso-date strings flagged by BOTH."""
    ext_iso = {(k if isinstance(k, str) else k.isoformat()): v
               for k, v in (ext_ref or {}).items()}   # compare_daily needs iso keys
    ext_days = set(compare_daily(derived, ext_iso).get("flagged_days") or [])
    int_days = {d.isoformat() for d in audit_internal_daily(derived, int_ref or {})}
    return sorted(ext_days & int_days)


def _combined_reference_coverage(derived, reference, requested_range=None):
    if not derived or not reference:
        return None
    coverage = _reference_coverage(
        derived, reference, requested_range or FULL_HISTORY_RANGE)
    return {
        "first_date": coverage["reference_first_date"],
        "last_date": coverage["reference_last_date"],
        "day_count": coverage["reference_day_count"],
        "derived_first_date": coverage["stored_first_date"],
        "derived_last_date": coverage["stored_last_date"],
        "derived_day_count": coverage["stored_day_count"],
        "head_gap_days": coverage["head_gap_days"],
        "head_tolerance_days": coverage["head_tolerance_days"],
        "head_ok": coverage["full_history_head_ok"],
        "requested_range": requested_range,
    }


def combined_crosscheck(root, ticker, interval="1m", ext_ref=None, int_ref=None,
                        read_fn=None, refetch_month=None,
                        requested_range=FULL_HISTORY_RANGE):
    """3-SOURCE agreement check for one series. Flag a date only when the external
    (stockanalysis) AND internal (IBKR daily) BOTH disagree with the stored 1m. For
    each flagged date's MONTH, if `refetch_month(year, month) -> (derived_m, ext_m,
    int_m)` is given, RE-FETCH all three for that month ONCE and re-test; a date that
    STILL double-flags is PERSISTENT (real, surface it), one that clears was a
    transient fetch glitch. Returns the sidecar verdict."""
    if read_fn is not None:
        bars = read_fn(root, ticker, interval)
        derived = derive_daily(bars)
    else:
        derived, cache_stats = ddc.derive_series(root, ticker, interval)
        if cache_stats.get("source_current") is not True:
            raise ddc.SourceNotCurrentError(
                f"non-current derived source for {ticker} {interval}")
    coverage_errors = []
    try:
        external_coverage = _combined_reference_coverage(
            derived, ext_ref, requested_range)
    except (TypeError, ValueError) as exc:
        external_coverage = None
        coverage_errors.append(f"external: {_bounded_xval_text(exc)}")
    try:
        internal_coverage = _combined_reference_coverage(derived, int_ref)
    except (TypeError, ValueError) as exc:
        internal_coverage = None
        coverage_errors.append(f"internal: {_bounded_xval_text(exc)}")
    cand = (combined_double_flag(derived, ext_ref, int_ref)
            if external_coverage is not None and internal_coverage is not None
            else [])
    persistent, cleared = [], []
    by_month = {}
    for d in cand:
        by_month.setdefault((int(d[:4]), int(d[5:7])), []).append(d)
    for (y, m), dates in sorted(by_month.items()):
        if refetch_month is None:
            persistent.extend(dates)
            continue
        try:
            d2, e2, i2 = refetch_month(y, m)
            # Re-test in FULL-SERIES context: compare_daily's rolling baseline self-
            # adapts to a lone ~20-day month (it would absorb the anomaly), and a
            # MISSING/partial refetch then keeps the STORED day's value so a genuine
            # discrepancy stays flagged instead of being silently cleared.
            still = set(combined_double_flag({**derived, **(d2 or {})},
                                             {**(ext_ref or {}), **(e2 or {})},
                                             {**(int_ref or {}), **(i2 or {})}))
        except Exception as exc:  # noqa: BLE001 — a failed refetch stays flagged
            persistent.extend(dates)
            continue
        for d in dates:
            (persistent if d in still else cleared).append(d)
    shallow_or_missing = (external_coverage is None
                          or internal_coverage is None
                          or not external_coverage.get("head_ok")
                          or not internal_coverage.get("head_ok"))
    status = ("flagged" if persistent else
              "inconclusive" if shallow_or_missing else "ok")
    result = {"ticker": ticker, "interval": interval, "source": "3-source",
            "status": status,
            "candidates": cand, "persistent": sorted(persistent),
            "cleared": sorted(cleared),
            "reference_coverage": {
                "external": external_coverage,
                "internal": internal_coverage,
            }}
    if coverage_errors:
        result["reference_coverage_error"] = "; ".join(coverage_errors)
    if status == "inconclusive":
        result["note"] = "combined references are missing, malformed, or shallow"
    return result


def internal_crosscheck(root, ticker, interval="1m", daily_ref=None, ref_fn=None,
                        rng="5Y", sensitivity="normal", read_fn=None):
    """INTERNAL integrity check: derive daily from the stored INTRADAY series and
    compare it to IBKR's OWN daily TRADES bars (vs the external stockanalysis
    source). Pass a fetched `daily_ref` dict OR a `ref_fn(ticker, rng)`; reuses
    validate_series' compare machinery and tags source='ibkr-daily'.

    STRUCTURE ONLY: the `ref_fn` that actually pulls IBKR daily (span/bar-size
    rules) and the auto-fire wiring (every <1d run also fetches daily + checks) are
    TODO — need live ports. See TODO.md. Today: call with an injected ref."""
    rf = ref_fn if ref_fn is not None else (lambda _t, _r: dict(daily_ref or {}))
    res = _validate_series_body(root, ticker, interval, rng=rng,
                                sensitivity=sensitivity,
                                read_fn=read_fn, ref_fn=rf)
    res["source"] = "ibkr-daily"
    return res


def validate_extended(root, ticker, base_interval="1m", tol_gap=0.25,
                      read_fn=None):
    """Validate EXTENDED-hours (-pre/-post) data WITHOUT an external reference —
    none exists (daily bars are RTH-only everywhere). Instead it ANCHORS the
    extended sessions to the RTH series (which IS reference-validated penny-
    exact), plus structural sanity that needs nothing external:

      * BOUNDARY CONTINUITY — pre last-close ≈ RTH open (09:29→09:30) and RTH
        close ≈ post first-open (15:59→16:00). A 2×/10× scale error, a wrong
        symbol, or a misaligned day breaks the seam; a normal overnight gap
        doesn't (hence the generous `tol_gap`, default 25%).
      * STRUCTURE — OHLC integrity (h≥o/c≥l), every extended bar inside its
        session window, and extended volume not EXCEEDING RTH volume (extended
        is thin; more-than-RTH means lots/wrong data).

    `read_fn` injectable for tests. Returns {ticker, base_interval, days_checked,
    pre_days, post_days, flagged, flagged_days, score} or {'error': …}. A flag
    is {date, kind, detail}; kind ∈ boundary_pre|boundary_post|volume|ohlc|
    window."""
    rd = read_fn or read_series
    base = ss.base_interval(base_interval)
    rth = rd(root, ticker, base)
    pre = rd(root, ticker, base + "-pre")
    post = rd(root, ticker, base + "-post")
    if not pre and not post:
        return {"error": f"no extended (-pre/-post) {base} data stored for "
                         f"{ticker}", "ticker": ticker, "base_interval": base}
    rg, pg, sg = _day_groups(rth), _day_groups(pre), _day_groups(post)
    flagged = []

    def flag(d, kind, detail):
        flagged.append({"date": d.isoformat(), "kind": kind, "detail": detail})

    days = sorted(set(pg) | set(sg))
    anchored = 0
    for d in days:
        rbars, prebars, postbars = rg.get(d), pg.get(d), sg.get(d)
        if rbars:                                # boundary needs an RTH anchor
            anchored += 1
            r_open, r_close = rbars[0][1], rbars[-1][4]
            r_vol = sum(b[5] for b in rbars)
            if prebars and r_open and \
                    abs(prebars[-1][4] / r_open - 1.0) > tol_gap:
                flag(d, "boundary_pre",
                     f"pre close {prebars[-1][4]} vs RTH open {r_open} "
                     f"(off {abs(prebars[-1][4] / r_open - 1.0):.1%})")
            if postbars and r_close and \
                    abs(postbars[0][1] / r_close - 1.0) > tol_gap:
                flag(d, "boundary_post",
                     f"post open {postbars[0][1]} vs RTH close {r_close} "
                     f"(off {abs(postbars[0][1] / r_close - 1.0):.1%})")
            ext_vol = sum(b[5] for b in (prebars or [])) \
                + sum(b[5] for b in (postbars or []))
            if r_vol and ext_vol > r_vol:
                flag(d, "volume", f"extended volume {ext_vol} exceeds RTH "
                                  f"volume {r_vol}")
        for tag, bars, win in (("pre", prebars, _PRE_WIN),
                               ("post", postbars, _POST_WIN)):
            if not bars:
                continue
            bad = next((b for b in bars if not _ohlc_ok(b)), None)
            if bad is not None:
                flag(d, "ohlc", f"{tag} bar {bad[0].time()} OHLC invalid "
                                f"(o{bad[1]} h{bad[2]} l{bad[3]} c{bad[4]})")
            out_win = sum(1 for b in bars
                          if not (win[0] <= b[0].time() <= win[1]))
            if out_win:
                flag(d, "window", f"{out_win} {tag} bar(s) outside "
                                  f"{win[0]}–{win[1]}")
    # No RTH day to anchor a single boundary check = INCONCLUSIVE, not a clean
    # 1.0 — the boundary-continuity check (the main scale/symbol guard) never
    # ran, so structural-only passes must NOT read as "joins cleanly".
    if anchored == 0:
        return {"error": (f"no overlapping regular-session ({base}) data to "
                          f"anchor the extended-hours boundary check for "
                          f"{ticker} — stored {len(pg)} pre / {len(sg)} post "
                          f"day(s), but no matching RTH bars"),
                "ticker": ticker, "base_interval": base,
                "pre_days": len(pg), "post_days": len(sg)}
    flagged_days = sorted({f["date"] for f in flagged})
    checked = len(days)
    return {"ticker": ticker, "base_interval": base, "days_checked": checked,
            "anchored_days": anchored, "pre_days": len(pg), "post_days": len(sg),
            "flagged": flagged, "flagged_days": flagged_days,
            "score": round(1 - len(flagged_days) / checked, 4) if checked
            else None}


# --- intraday gap detection (interior holes; no external reference) ---------

def _iv_step_seconds(interval):
    """Seconds per bar for an INTRADAY token ('1m'->60, '5m'->300, '1h'->3600,
    '1s'->1). Strips any -pre/-post session suffix. Returns None for daily /
    unknown tokens (interior-gap detection is intraday-only)."""
    tok = ss.base_interval(interval)
    try:
        n, unit = int(tok[:-1]), tok[-1]
    except (ValueError, IndexError):
        return None
    mult = {"s": 1, "m": 60, "h": 3600}.get(unit)
    if mult is None or n <= 0:
        return None
    return n * mult


def find_intraday_gaps(bars, interval="1m", max_report=None):
    """Flag INTERIOR gaps: two consecutive stored bars on the SAME calendar day
    whose timestamps jump by MORE than one bar-step — i.e. a missing minute (or
    N minutes) BETWEEN two bars that DO exist (e.g. 11:17 then 11:19, no 11:18).

    Session-aware by construction: only same-day consecutive bars are compared,
    so the overnight / weekend / holiday boundary, a half-day early close, and the
    RTH<->extended seam (stored as separate series) are NEVER flagged — those are
    not interior holes. A genuine trading HALT also surfaces here (it is a real
    no-data span); it is reported, not judged — read a lone gap on a liquid name
    as a likely fetch drop, a multi-minute one as a possible halt. CAVEAT: with
    whatToShow=TRADES a minute with ZERO trades yields no bar, so on a THIN /
    illiquid name legitimately-untraded minutes also show here — the count is
    reliable for liquid S&P names, noisier for thin tickers.

    `bars`: list of (datetime, o,h,l,c,v), any order. `interval`: '1m'/'5m'/'1h'
    or a session token; the step is the BASE interval's seconds. `max_report`
    caps the detailed `gaps` list (None = no cap — persistence paths pass None so
    the parquet's row count always matches missing_total).
    Returns {interval, step_seconds, bars, gap_count, reported, missing_total,
             days_with_gaps, largest_gap, gaps:[{date, after, before, missing,
             span_minutes}]}  or  {'error': …} when the interval isn't intraday."""
    step = _iv_step_seconds(interval)
    if step is None:
        return {"error": f"interior-gap scan needs an intraday interval, "
                         f"got {interval!r}", "interval": interval}
    try:                                           # mixed tz-aware/naive -> error
        groups = _day_groups(bars)
    except TypeError as exc:
        return {"error": f"interior-gap scan got incomparable timestamps "
                         f"(mixed tz-aware/naive?): {exc}", "interval": interval}
    gaps = []
    gap_events = 0
    missing_total = 0
    largest = 0
    days_with_gaps = set()
    # a real gap is STRICTLY more than ~1.5 steps apart, so a clean one-step grid
    # (and any sub-second jitter on the stamps) never trips a false positive.
    thresh = step * 1.5
    for d in sorted(groups):
        day_bars = groups[d]                       # _day_groups returns sorted
        for a, b in zip(day_bars, day_bars[1:]):
            try:
                delta = (b[0] - a[0]).total_seconds()
            except TypeError as exc:
                return {"error": f"interior-gap scan got incomparable "
                                 f"timestamps: {exc}", "interval": interval}
            if delta <= thresh:
                continue
            # round-half-UP (deterministic, grid-exact, symmetric off-grid —
            # avoids banker's-rounding asymmetry on jittered stamps).
            miss = int((delta + 0.5 * step) // step) - 1
            if miss < 1:
                continue
            gap_events += 1
            missing_total += miss
            largest = max(largest, miss)
            days_with_gaps.add(d.isoformat())
            if max_report is None or len(gaps) < max_report:
                gaps.append({"date": d.isoformat(),
                             "after": a[0].isoformat(sep=" "),
                             "before": b[0].isoformat(sep=" "),
                             "missing": miss,
                             "span_minutes": round(delta / 60.0, 2)})
    return {"interval": interval, "step_seconds": step, "bars": len(bars),
            "gap_count": gap_events, "reported": len(gaps),
            "missing_total": missing_total, "largest_gap": largest,
            "days_with_gaps": sorted(days_with_gaps), "gaps": gaps}


def _find_intraday_gaps_ts(ts, interval, max_report=None):
    """find_intraday_gaps on int EPOCH-SECOND stamps (read_series_ts) — the
    identical result dict, computed vectorized. Exactness argument: the stamps
    are the stored NAIVE wall grid, so `sec // 86400` equals .date() grouping
    bit-for-bit (DST days included); a stable sort by ts reproduces
    _day_groups' (sorted days, per-day time order), so gap order — and any
    max_report cap — matches; the miss/threshold arithmetic is the original
    scalar formula on the same float delta total_seconds() yields; and the
    after/before strings rebuild the same whole-second naive datetimes the
    strict parse materializes. Regression-locked by _selftest + the live
    deep-equal harness."""
    step = _iv_step_seconds(interval)
    if step is None:
        return {"error": f"interior-gap scan needs an intraday interval, "
                         f"got {interval!r}", "interval": interval}
    import numpy as np                    # lazy — matches ss._require_pyarrow
    arr = np.sort(np.asarray(ts, dtype=np.int64), kind="stable")
    gaps = []
    gap_events = 0
    missing_total = 0
    largest = 0
    days_with_gaps = set()
    thresh = step * 1.5
    if arr.size > 1:
        delta = np.diff(arr)
        same_day = (arr[1:] // 86400) == (arr[:-1] // 86400)
        for i in np.nonzero(same_day & (delta > thresh))[0].tolist():
            d_ = float(delta[i])                       # == (b-a).total_seconds()
            miss = int((d_ + 0.5 * step) // step) - 1  # original scalar formula
            if miss < 1:
                continue
            gap_events += 1
            missing_total += miss
            largest = max(largest, miss)
            a = _EPOCH_DT + _timedelta(seconds=int(arr[i]))
            b = _EPOCH_DT + _timedelta(seconds=int(arr[i + 1]))
            days_with_gaps.add(a.date().isoformat())
            if max_report is None or len(gaps) < max_report:
                gaps.append({"date": a.date().isoformat(),
                             "after": a.isoformat(sep=" "),
                             "before": b.isoformat(sep=" "),
                             "missing": miss,
                             "span_minutes": round(d_ / 60.0, 2)})
    return {"interval": interval, "step_seconds": step, "bars": len(ts),
            "gap_count": gap_events, "reported": len(gaps),
            "missing_total": missing_total, "largest_gap": largest,
            "days_with_gaps": sorted(days_with_gaps), "gaps": gaps}


def _day_set_ts(ts):
    """Distinct calendar dates in an epoch-second series (== _bar_days of the
    same bars: the stamps are naive wall time, so day = sec // 86400)."""
    return {date.fromordinal(_EPOCH_ORD + s // 86400) for s in set(ts)}


def _bar_days(bars):
    """The set of distinct calendar dates present in a bar series (intraday or
    daily; daily bars are stored at naive midnight, so .date() works for both)."""
    out = set()
    for b in bars or []:
        dt = b[0]
        out.add(dt.date() if hasattr(dt, "date") else dt)
    return out


_CAL_FILE = "_consensus_calendar.json"
_CAL_LOCK = threading.Lock()           # guards the calendar-cache sidecar write


def _cal_fingerprint(mm):
    """Fingerprint of ONE ticker's 1d months dict: sha256 over the sorted
    (month, sha256, mtime_ns, rows) entries the manifest stores. The reads the
    cache replaces only ever see data THROUGH the manifest (read_series walks
    manifest_months), so keying on the manifest's own month state makes the
    cache exactly as fresh as the reads themselves — a lagging manifest hides
    a month from BOTH equally, and heals by the next scan either way."""
    h = hashlib.sha256()
    for mk in sorted(mm):
        e = mm.get(mk) if isinstance(mm.get(mk), dict) else {}
        h.update(f"{mk}:{e.get('sha256')}:{e.get('mtime_ns')}:{e.get('rows')}\n"
                 .encode("utf-8"))
    return h.hexdigest()


def consensus_calendar(root, read_fn=None, min_tickers=2):
    """The bank's OWN trading-day calendar, built OFFLINE from the stored daily
    series — the set of dates present in at least `min_tickers` tickers' 1d series.
    Every name trades the same sessions, so a real trading day shows up in ~every
    in-range ticker while a lone spurious date (only one ticker) is dropped. This is
    the 'connected pattern' yardstick for missing-day detection: no external market
    calendar and no network needed. Returns a set of date objects (possibly empty if
    the bank has no daily series yet).

    PRODUCTION (read_fn=None) is CACHED: per-ticker 1d day-sets persist in the
    `_consensus_calendar.json` bank sidecar keyed by each ticker's manifest-months
    fingerprint — an unchanged ticker costs one manifest load instead of re-reading
    its ~180 tiny 1d files (measured 220 s bank-wide, rebuilt on EVERY fetch/
    Fix-data/Fill-gaps/seal before this cache). A changed/unknown ticker re-reads
    just itself via the sha-gated ts-only path. An injected `read_fn` (tests)
    bypasses the cache entirely and runs the original bar-tuple loop."""
    if read_fn is not None:               # injected reader: exact legacy path
        counts = {}
        for t, iv in discover_series(root, rth_only=True, kinds=("",)):
            if ss.base_interval(str(iv)) != "1d":
                continue
            try:
                bars = read_fn(root, t, iv)
            except Exception:  # noqa: BLE001
                continue
            for d in _bar_days(bars):
                counts[d] = counts.get(d, 0) + 1
        return {d for d, n in counts.items() if n >= min_tickers}
    try:
        cache = json.loads((Path(root) / _CAL_FILE).read_text(encoding="utf-8"))
        cache = cache.get("tickers", {}) if isinstance(cache, dict) else {}
    except (OSError, ValueError):
        cache = {}
    fresh = {}
    counts = {}
    dirty = False
    for t, iv in discover_series(root, rth_only=True, kinds=("",)):
        if ss.base_interval(str(iv)) != "1d":
            continue
        canon = ss.canonical_ticker(t)
        man = ss.load_manifest(Path(root) / canon) or {}
        key = _cal_fingerprint(ss.manifest_months(man, iv))
        ent = cache.get(f"{canon} {iv}")
        if (isinstance(ent, dict) and ent.get("key") == key
                and isinstance(ent.get("days"), list)):
            days = ent["days"]            # unchanged ticker -> cached day indices
        else:
            try:
                days = sorted({s // 86400 for s in read_series_ts(root, t, iv)})
            except Exception:  # noqa: BLE001
                dirty = True
                continue
            dirty = True
        fresh[f"{canon} {iv}"] = {"key": key, "days": days}
        for di in days:
            counts[di] = counts.get(di, 0) + 1
    if dirty or set(fresh) != set(cache):     # new/changed/removed series only
        with _CAL_LOCK:
            try:
                ss._atomic_write_bytes(
                    Path(root) / _CAL_FILE,
                    json.dumps({"tickers": fresh}, sort_keys=True,
                               separators=(",", ":")).encode("utf-8"))
            except Exception:  # noqa: BLE001 — cache persistence is best-effort
                pass
    return {date.fromordinal(_EPOCH_ORD + di) for di, n in counts.items()
            if n >= min_tickers}


def _verified_absent_state(root, ticker, interval):
    try:
        man = ss.load_manifest(Path(root) / ss.canonical_ticker(ticker))
        section = (((man or {}).get("intervals") or {}).get(interval) or {})
        absent = set(section.get("verified_absent", []))
        evidence = section.get("verified_absent_evidence", {})
        return absent, (dict(evidence) if isinstance(evidence, dict) else {})
    except Exception:  # noqa: BLE001
        return set(), {}


def verified_absent_days(root, ticker, interval):
    """Dates a series' manifest marks as SOURCE-ABSENT (the source confirmed it has no
    data for that day — `stock_ibkr.fill_missing_days` records these). Reported
    SEPARATELY from fillable gaps so the UI can flag them 'data does not exist'."""
    return _verified_absent_state(root, ticker, interval)[0]


_ABSENCE_EXPIRY_FILE = "_absence_expiry.json"
_ABSENCE_POLICY_VERSION = 1
_ABSENCE_EXPIRY_MAX_BYTES = 256 * 1024
_ABSENCE_EXPIRY_DEFAULTS = {
    "weak_days": 30,
    "strong_days": 365,
    "reprobe_budget_per_run": 25,
}


def _lower_hex(value, minimum, maximum):
    return (isinstance(value, str)
            and minimum <= len(value) <= maximum
            and all(char in "0123456789abcdef" for char in value))


def _absence_pin_key(value):
    if not isinstance(value, dict):
        return None
    ticker = value.get("ticker")
    interval = value.get("interval")
    if not isinstance(ticker, str) or not isinstance(interval, str):
        return None
    try:
        canonical = ss.canonical_ticker(ticker)
    except Exception:  # noqa: BLE001 - malformed policy is report state
        return None
    if (ticker != canonical or not ss.TICKER_DIR_RE.fullmatch(canonical)
            or not ss.INTERVAL_RE.fullmatch(interval)):
        return None
    return canonical, interval


def _valid_absence_pin(value, key):
    if set(value) != {
            "ticker", "interval", "start", "end", "disposition",
            "interval_fingerprint", "review"}:
        return False, "pin fields do not match schema version 1"
    if value.get("disposition") != "do_not_refetch":
        return False, "pin disposition is not do_not_refetch"
    try:
        start = date.fromisoformat(value.get("start"))
        end = date.fromisoformat(value.get("end"))
    except (TypeError, ValueError):
        return False, "pin date range is malformed"
    if start > end:
        return False, "pin date range is reversed"

    fingerprint = value.get("interval_fingerprint")
    expected_fingerprint_fields = {
        "schema_version", "algorithm", "sha256", "ticker", "interval",
        "present", "backfill_incomplete", "month_count",
        "verified_absent_count",
    }
    if (not isinstance(fingerprint, dict)
            or set(fingerprint) != expected_fingerprint_fields
            or fingerprint.get("schema_version")
            != ss.INTERVAL_FINGERPRINT_VERSION
            or fingerprint.get("algorithm") != "sha256"
            or not _lower_hex(fingerprint.get("sha256"), 64, 64)
            or fingerprint.get("ticker") != key[0]
            or fingerprint.get("interval") != key[1]
            or fingerprint.get("present") is not True
            or not isinstance(fingerprint.get("backfill_incomplete"), bool)
            or isinstance(fingerprint.get("month_count"), bool)
            or not isinstance(fingerprint.get("month_count"), int)
            or fingerprint.get("month_count") < 0
            or isinstance(fingerprint.get("verified_absent_count"), bool)
            or not isinstance(fingerprint.get("verified_absent_count"), int)
            or fingerprint.get("verified_absent_count") < 1):
        return False, "pin interval fingerprint is malformed"

    review = value.get("review")
    if (not isinstance(review, dict)
            or set(review) != {
                "checkpoint", "approval_commit", "approved_by", "approved_at"}
            or not _lower_hex(review.get("checkpoint"), 7, 40)
            or not _lower_hex(review.get("approval_commit"), 7, 40)
            or review.get("approved_by") not in {"CLAUDE", "USER"}
            or not isinstance(review.get("approved_at"), str)
            or not review.get("approved_at")):
        return False, "pin review provenance is malformed"
    try:
        datetime.fromisoformat(review["approved_at"].replace("Z", "+00:00"))
    except ValueError:
        return False, "pin approval time is malformed"
    return True, None


def _absence_expiry_config(root):
    """Hot-load expiry windows and optional reviewed do-not-refetch pins.

    A missing sidecar preserves the generic expiry defaults. Once a
    ``known_deliberate`` policy is present, malformed global policy state halts
    every source-absence re-probe; an identifiable bad or duplicate pin halts
    only that exact series. Policy errors can therefore remove repair
    candidates, never create them.
    """
    out = dict(_ABSENCE_EXPIRY_DEFAULTS)
    out["_known_deliberate"] = {}
    out["_known_deliberate_halted"] = {}
    out["_known_deliberate_error"] = None
    path = Path(root) / _ABSENCE_EXPIRY_FILE
    try:
        encoded = path.read_bytes()
    except FileNotFoundError:
        return out
    except OSError as exc:
        out["_known_deliberate_error"] = (
            f"cannot read {_ABSENCE_EXPIRY_FILE}: {type(exc).__name__}: {exc}")
        return out
    if len(encoded) > _ABSENCE_EXPIRY_MAX_BYTES:
        out["_known_deliberate_error"] = (
            f"{_ABSENCE_EXPIRY_FILE} exceeds policy size limit")
        return out
    try:
        raw = json.loads(
            encoded.decode("utf-8"),
            object_pairs_hook=ss._json_object_without_duplicate_keys)
    except (UnicodeError, ValueError, TypeError) as exc:
        out["_known_deliberate_error"] = (
            f"{_ABSENCE_EXPIRY_FILE} is malformed: {exc}")
        return out
    if not isinstance(raw, dict):
        out["_known_deliberate_error"] = (
            f"{_ABSENCE_EXPIRY_FILE} root is not an object")
        return out
    for key in _ABSENCE_EXPIRY_DEFAULTS:
        value = raw.get(key)
        if (isinstance(value, int) and not isinstance(value, bool)
                and 0 <= value <= 100_000):
            out[key] = value
    if out["strong_days"] < out["weak_days"]:
        out["strong_days"] = max(
            _ABSENCE_EXPIRY_DEFAULTS["strong_days"], out["weak_days"])

    if "known_deliberate" not in raw:
        return out
    if raw.get("version") != _ABSENCE_POLICY_VERSION:
        out["_known_deliberate_error"] = (
            "known_deliberate requires policy version 1")
        return out
    entries = raw.get("known_deliberate")
    if not isinstance(entries, list):
        out["_known_deliberate_error"] = (
            "known_deliberate is not a list")
        return out
    for index, value in enumerate(entries):
        key = _absence_pin_key(value)
        if key is None:
            out["_known_deliberate_error"] = (
                f"known_deliberate[{index}] has no valid series identity")
            out["_known_deliberate"].clear()
            return out
        if (key in out["_known_deliberate"]
                or key in out["_known_deliberate_halted"]):
            out["_known_deliberate"].pop(key, None)
            out["_known_deliberate_halted"][key] = (
                "multiple known_deliberate pins target this series")
            continue
        valid, error = _valid_absence_pin(value, key)
        if not valid:
            out["_known_deliberate_halted"][key] = error
            continue
        out["_known_deliberate"][key] = dict(value)
    return out


def _new_absence_reprobe_budget(root):
    config = _absence_expiry_config(root)
    return {"config": config,
            "remaining": config["reprobe_budget_per_run"]}


def _known_deliberate_policy(root, ticker, interval, absent, config):
    """Return a subtract-only pin decision for one exact series."""
    key = (ss.canonical_ticker(ticker), str(interval or "").strip())
    global_error = config.get("_known_deliberate_error")
    if global_error:
        return {"status": "halted", "error": str(global_error)}
    halted = config.get("_known_deliberate_halted") or {}
    if key in halted:
        return {"status": "halted", "error": str(halted[key])}
    pin = (config.get("_known_deliberate") or {}).get(key)
    if pin is None:
        return {"status": "not_configured", "pinned": frozenset()}

    try:
        before = ss.interval_state_fingerprint(root, key[0], key[1])
    except Exception as exc:  # noqa: BLE001 - stale policy halts repair
        return {
            "status": "halted",
            "error": ("cannot validate known_deliberate fingerprint: "
                      f"{type(exc).__name__}: {exc}"),
        }
    if before != pin.get("interval_fingerprint"):
        return {
            "status": "halted",
            "error": "known_deliberate interval fingerprint is stale",
        }

    observed_absent, _observed_evidence = _verified_absent_state(
        root, key[0], key[1])
    try:
        after = ss.interval_state_fingerprint(root, key[0], key[1])
    except Exception as exc:  # noqa: BLE001 - changing state halts repair
        return {
            "status": "halted",
            "error": ("cannot revalidate known_deliberate fingerprint: "
                      f"{type(exc).__name__}: {exc}"),
        }
    if after != before or observed_absent != absent:
        return {
            "status": "halted",
            "error": "source-absence state changed during policy evaluation",
        }

    start = date.fromisoformat(pin["start"])
    end = date.fromisoformat(pin["end"])
    pinned = frozenset(
        day for day in absent
        if start <= date.fromisoformat(day) <= end)
    if not pinned:
        return {
            "status": "halted",
            "error": "known_deliberate range contradicts current absence state",
        }
    return {
        "status": "applied",
        "disposition": "do_not_refetch",
        "start": pin["start"],
        "end": pin["end"],
        "pinned": pinned,
        "pinned_count": len(pinned),
    }


def _valid_absence_evidence(value):
    return (isinstance(value, dict)
            and isinstance(value.get("method"), str)
            and bool(value.get("method"))
            and "control" in value
            and isinstance(value.get("at"), str)
            and bool(value.get("at")))


def _stamp_legacy_absence_evidence(root, ticker, interval, days, at):
    """Best-effort transactional stamp; never makes legacy days immediately due."""
    wanted = {str(day) for day in (days or [])}
    if not wanted:
        return
    tdir = Path(root) / ss.canonical_ticker(ticker)
    try:
        with ss.ticker_transaction(tdir):
            manifest = ss.load_manifest(tdir)
            if not isinstance(manifest, dict):
                return
            section = (((manifest.get("intervals") or {}).get(interval))
                       or {})
            if not isinstance(section, dict):
                return
            absent = set(section.get("verified_absent", []))
            evidence = section.get("verified_absent_evidence", {})
            evidence = dict(evidence) if isinstance(evidence, dict) else {}
            changed = False
            for day in sorted(wanted & absent):
                if not _valid_absence_evidence(evidence.get(day)):
                    evidence[day] = {
                        "method": "legacy_backfill",
                        "control": "legacy",
                        "at": at,
                    }
                    changed = True
            if changed:
                section["verified_absent_evidence"] = evidence
                ss.save_manifest(tdir, manifest)
    except Exception:  # noqa: BLE001 - evidence stamping is advisory/best-effort
        pass


def _absence_evidence_expired(value, config, now):
    if not _valid_absence_evidence(value):
        return False
    try:
        stamped = datetime.fromisoformat(value["at"].replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    current = (datetime.now(stamped.tzinfo)
               if stamped.tzinfo is not None else now)
    window = (config["weak_days"]
              if value.get("method") == "empty_month_control"
              else config["strong_days"])
    return current - stamped >= _timedelta(days=window)


def _absence_reprobe_state(root, ticker, interval, candidates, budget=None):
    """Return source absence, bounded due rows, and subtract-only policy state."""
    absent, evidence = _verified_absent_state(root, ticker, interval)
    candidate_set = {str(day) for day in (candidates or [])}
    if not candidate_set:
        return absent, set(), {"status": "not_evaluated"}
    state = budget if isinstance(budget, dict) else _new_absence_reprobe_budget(root)
    config = state.get("config")
    if not isinstance(config, dict):
        config = _absence_expiry_config(root)
        state["config"] = config
    policy = _known_deliberate_policy(
        root, ticker, interval, absent, config)
    if policy.get("status") == "halted":
        return absent, set(), policy
    pinned = set(policy.get("pinned") or ())
    relevant = absent & candidate_set
    if not relevant:
        return absent, set(), policy
    now = datetime.now()
    at = datetime.now().astimezone().isoformat(timespec="seconds")
    legacy = {day for day in relevant
              if day not in pinned
              and not _valid_absence_evidence(evidence.get(day))}
    if legacy:
        _stamp_legacy_absence_evidence(
            root, ticker, interval, legacy, at)
        for day in legacy:
            evidence[day] = {
                "method": "legacy_backfill",
                "control": "legacy",
                "at": at,
            }

    remaining = state.get("remaining", config["reprobe_budget_per_run"])
    if isinstance(remaining, bool) or not isinstance(remaining, int):
        remaining = 0
    remaining = max(0, remaining)
    due = [day for day in sorted(relevant)
           if day not in pinned
           and _absence_evidence_expired(evidence.get(day), config, now)]
    selected = set(due[:remaining])
    state["remaining"] = max(0, remaining - len(selected))
    return absent, selected, policy


def _missing_from_present(present, calendar_days):
    """find_missing_days' core on a PRECOMPUTED present-day set (shared by the
    bar-tuple path and the ts-only fast path — one implementation, one result)."""
    if not present:
        return {"missing_days": [], "present_days": 0, "expected_days": 0,
                "span": None}
    lo, hi = min(present), max(present)
    expected = {d for d in (calendar_days or ()) if lo <= d <= hi}
    missing = sorted(d.isoformat() for d in (expected - present))
    return {"missing_days": missing, "present_days": len(present),
            "expected_days": len(expected),
            "span": [lo.isoformat(), hi.isoformat()]}


def find_missing_days(bars, calendar_days, interval=None):
    """CONNECTED-PATTERN check: trading days the calendar expects but this series
    LACKS, restricted to the series' OWN [first, last] span (so history that simply
    hasn't started/ended is never flagged). Catches WHOLE missing days — the across-
    day holes the interior scan (same-day-only) can't see — for ANY interval, daily
    included. Returns {missing_days:[iso…], present_days, expected_days, span}."""
    return _missing_from_present(_bar_days(bars), calendar_days)


def scan_series_gaps(root, ticker, interval="1m", read_fn=None, max_report=None,
                     calendar_days=None, absence_reprobe_budget=None):
    """Read a stored series and scan it for gaps. INTERIOR gaps (missing bars
    between two bars that exist) are found for intraday series; CONNECTED-PATTERN
    whole-missing-days (vs `calendar_days`, when given) are found for ANY interval
    incl. daily. Offline. `read_fn` injectable for tests. The result `ticker` is the
    CANONICAL spelling (so a vendor BRK.B and the stored BRK-B never split into two
    rows). Returns a gap result with `ticker`/`missing_days` attached, or {'error'}."""
    canon = ss.canonical_ticker(ticker)
    if read_fn is None:            # PRODUCTION: sha-gated TIMESTAMP-ONLY reads —
        try:
            ts = read_series_ts(root, ticker, interval)   # gap math never needs OHLCV
        except Exception as exc:  # noqa: BLE001
            return {"error": f"{type(exc).__name__}: {exc}",
                    "ticker": canon, "interval": interval}
        if not ts:
            return {"error": f"no {interval} data stored for {ticker}",
                    "ticker": canon, "interval": interval}
        if _iv_step_seconds(interval) is not None:    # intraday -> interior scan
            res = _find_intraday_gaps_ts(ts, interval, max_report=max_report)
        else:                                         # daily -> no interior concept
            res = {"interval": interval, "step_seconds": None, "bars": len(ts),
                   "gap_count": 0, "reported": 0, "missing_total": 0,
                   "largest_gap": 0, "days_with_gaps": [], "gaps": []}
        present = _day_set_ts(ts)
    else:                          # injected reader (tests): legacy bar-tuple path
        try:
            bars = read_fn(root, ticker, interval)
        except Exception as exc:  # noqa: BLE001
            return {"error": f"{type(exc).__name__}: {exc}",
                    "ticker": canon, "interval": interval}
        if not bars:
            return {"error": f"no {interval} data stored for {ticker}",
                    "ticker": canon, "interval": interval}
        if _iv_step_seconds(interval) is not None:    # intraday -> interior scan
            res = find_intraday_gaps(bars, interval, max_report=max_report)
        else:                                         # daily -> no interior concept
            res = {"interval": interval, "step_seconds": None, "bars": len(bars),
                   "gap_count": 0, "reported": 0, "missing_total": 0,
                   "largest_gap": 0, "days_with_gaps": [], "gaps": []}
        present = _bar_days(bars)
    res["ticker"] = canon
    md = _missing_from_present(present, calendar_days) if calendar_days else None
    all_missing = (md or {}).get("missing_days", [])
    # split off SOURCE-ABSENT days (manifest verified_absent) so they flag distinctly
    # ('data does not exist') and don't count as fillable gaps.
    absent, expired_absent, absence_policy = _absence_reprobe_state(
        root, canon, interval, all_missing,
        budget=absence_reprobe_budget)
    res["source_absent"] = sorted(d for d in all_missing if d in absent)
    public_policy = {
        key: value for key, value in absence_policy.items()
        if key != "pinned"
    }
    if public_policy.get("status") not in {None, "not_configured",
                                            "not_evaluated"}:
        res["source_absence_policy"] = public_policy
    if absence_policy.get("status") == "halted":
        res["missing_days"] = []
        res["missing_day_count"] = 0
        res["source_absent_count"] = len(res["source_absent"])
        res["error"] = (
            "source-absence policy halted automated repair: "
            f"{absence_policy.get('error') or 'invalid policy'}")
        return res
    res["missing_days"] = sorted(
        {d for d in all_missing if d not in absent} | expired_absent)
    res["missing_day_count"] = len(res["missing_days"])
    res["source_absent_count"] = len(res["source_absent"])
    return res


# the single time-ordered gap report lives at the OUTERMOST folder (the data
# bank's PARENT), alongside 'Validation Reference' — never inside the bank.
GAP_REPORT_NAME = "data_gaps.parquet"
_GAP_REPORT_COLS = ["ticker", "interval", "missing", "prev_bar", "next_bar",
                    "gap_minutes"]


def _gap_report_rows(scans):
    """Flatten gap scans into ONE ROW PER MISSING BAR: (ticker, interval,
    missing_dt, prev_dt, next_dt, gap_minutes). Each missing slot is reconstructed
    from the gap's `after` stamp + the interval step (so a 3-minute hole yields 3
    rows, one per absent minute). Sorted earliest->latest by the missing time."""
    from datetime import datetime, timedelta
    rows = []
    for s in scans or []:
        if not s or s.get("error"):
            continue
        tic = (s.get("ticker") or "").upper()
        iv = s.get("interval") or ""
        step = s.get("step_seconds") or _iv_step_seconds(iv)
        if not step:                  # unknown / non-intraday token: can't grid
            continue
        step = int(step)
        for g in s.get("gaps", []):
            try:
                prev_dt = datetime.fromisoformat(g["after"])
                next_dt = datetime.fromisoformat(g["before"])
            except (ValueError, KeyError, TypeError):
                continue
            span = g.get("span_minutes")
            for k in range(1, int(g.get("missing", 0)) + 1):
                rows.append((tic, iv, prev_dt + timedelta(seconds=step * k),
                             prev_dt, next_dt, span))
    rows.sort(key=lambda r: (r[2], r[0], r[1]))      # earliest missing first
    return rows


def _write_gap_parquet(path, rows):
    """Write the flattened gap rows to a typed Parquet file (zstd, atomic)."""
    pa, _pq = ss._require_pyarrow()
    import io
    import pyarrow.parquet as pq
    cols = list(zip(*rows)) if rows else ([], [], [], [], [], [])
    tbl = pa.table({
        "ticker": pa.array(list(cols[0]), pa.string()),
        "interval": pa.array(list(cols[1]), pa.string()),
        "missing": pa.array(list(cols[2]), pa.timestamp("s")),
        "prev_bar": pa.array(list(cols[3]), pa.timestamp("s")),
        "next_bar": pa.array(list(cols[4]), pa.timestamp("s")),
        "gap_minutes": pa.array(
            [None if x is None else float(x) for x in cols[5]], pa.float64()),
    })
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    ss._atomic_write_bytes(Path(path), buf.getvalue())
    return str(path)


_GAPS_FILE = gap_evidence.BASENAME
_GAPS_LOCK = threading.Lock()             # guards the _data_gaps.json sidecar
_GAP_PARQUET_LOCK = threading.Lock()      # serialises data_gaps.parquet read-mod-write


def _gap_scan_with_provenance(root, ticker, interval, *, read_fn=None,
                              max_report=None, calendar_days=None,
                              fingerprint_fn=None, absence_reprobe_budget=None):
    try:
        canonical = ss.canonical_ticker(ticker)
    except Exception:  # noqa: BLE001 - preserve a bounded diagnostic
        canonical = str(ticker or "").strip().upper()
    before, before_error = gap_evidence.capture_interval(
        root, ticker, interval, fingerprint_fn=fingerprint_fn)
    try:
        scan = scan_series_gaps(
            root, ticker, interval, read_fn=read_fn, max_report=max_report,
            calendar_days=calendar_days,
            absence_reprobe_budget=absence_reprobe_budget)
        if not isinstance(scan, dict):
            raise TypeError("gap scanner returned a non-object result")
    except Exception as exc:  # noqa: BLE001 - failed attempt is evidence
        scan = {
            "ticker": canonical,
            "interval": str(interval or "").strip(),
            "error": f"{type(exc).__name__}: {exc}"[:gap_evidence.MAX_ERROR],
        }
    scan.setdefault("ticker", canonical)
    scan.setdefault("interval", interval)
    after, after_error = gap_evidence.capture_interval(
        root, ticker, interval, fingerprint_fn=fingerprint_fn)
    scan["interval_fingerprint"] = gap_evidence.build_provenance(
        before, before_error, after, after_error,
        operation_error=scan.get("error"))
    return scan


def _gap_asof(value=None):
    return str(value or datetime.now().astimezone().isoformat(
        timespec="seconds"))


def evaluate_gap_evidence(root, series=None, fingerprint_fn=None):
    """Strict, read-only currentness evaluation for health/export consumers."""
    return gap_evidence.evaluate(
        root, series=series, fingerprint_fn=fingerprint_fn)


def load_data_gaps(root):
    """The gap-scan summary from the bank's `_data_gaps.json` sidecar:
    {'asof': …, 'series': {'TICKER iv': {missing_total, gap_events, days}}}.
    Returns {} on a missing/corrupt file; always returns a dict whose 'series'
    sub-key IS a dict (a hand-corrupted non-dict 'series' is coerced to {} so a
    consumer's `gmap.get(...)` can never raise and blank the main table)."""
    try:
        data = json.loads((Path(root) / _GAPS_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    if not isinstance(data.get("series"), dict):
        data["series"] = {}
    return data


def record_data_gaps(root, summary, asof=None):
    """Persist a WHOLE-bank gap-scan summary to `_data_gaps.json` (atomic, lock-
    guarded). A scan is bank-wide, so this REPLACES the file. `summary` is keyed
    'TICKER iv' -> {missing_total, gap_events, days}. Best-effort -> path|None."""
    target = Path(root) / _GAPS_FILE
    payload = gap_evidence.payload(summary, _gap_asof(asof))
    with _GAPS_LOCK:
        try:
            Path(root).mkdir(parents=True, exist_ok=True)
            ss._atomic_write_bytes(
                target,
                json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"))
            return str(target)
        except Exception:  # noqa: BLE001 — persistence is best-effort
            return None


_KEEP_EXISTING_GAP_KINDS = object()


def merge_data_gaps(root, summary, asof=None,
                    replace_kinds=_KEEP_EXISTING_GAP_KINDS):
    """Atomically MERGE per-series gap entries into `_data_gaps.json` under the lock
    — load+update+write as ONE critical section, so two concurrent post-fetch passes
    (e.g. a fetch's own gap scan racing the gap-heal's re-scan) can't lose each
    other's update by reading the same stale base. Best-effort -> path|None."""
    target = Path(root) / _GAPS_FILE
    with _GAPS_LOCK:
        try:
            data = json.loads(target.read_text(encoding="utf-8"))
            if not isinstance(data, dict) or not isinstance(data.get("series"), dict):
                data = {"series": {}}
        except (OSError, ValueError):
            data = {"series": {}}
        if replace_kinds is not _KEEP_EXISTING_GAP_KINDS:
            wanted = _normalize_kinds(replace_kinds)
            if wanted is None:
                data["series"] = {}
            else:
                data["series"] = {
                    key: value for key, value in data["series"].items()
                    if ss.kind_of(str(key).rpartition(" ")[2]) not in wanted
                }
        data["series"].update(summary)
        data["kind"] = gap_evidence.KIND
        data["schema_version"] = gap_evidence.SCHEMA_VERSION
        data["asof"] = _gap_asof(asof)
        try:
            Path(root).mkdir(parents=True, exist_ok=True)
            ss._atomic_write_bytes(
                target, json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))
            return str(target)
        except Exception:  # noqa: BLE001 — persistence is best-effort
            return None


def scan_all_gaps(root, series=None, write=True, read_fn=None, max_report=None,
                   asof=None, progress=None, calendar_days=None,
                   fingerprint_fn=None, kinds=("",), preserve_existing=False,
                   absence_reprobe_budget=None):
    """Scan stored INTRADAY regular-session series for interior gaps, collect
    EVERY missing bar's date/time, and (write=True) save them all to a single
    time-ordered Parquet at the OUTERMOST folder (the data bank's parent) named
    `data_gaps.parquet`, plus a per-series summary sidecar `_data_gaps.json`
    inside the bank (so the main table can show a Gaps column without re-scanning).
    Extended (-pre/-post) series are skipped — thin sessions legitimately miss many
    minutes, which would swamp the report. When `series` is omitted, `kinds`
    selects stored data kinds and defaults to TRADES only. `progress(i, n, ticker,
    interval)` (when
    given) is called once per series BEFORE it is scanned, so a UI can show a live
    'scanning X/N' bar. Returns {series_scanned, gap_series, gap_events,
    missing_total, rows_written, path, sidecar, summary, by_series} or carries an
    'error' on a write failure. ``preserve_existing=True`` replaces only the
    selected kind scope while retaining every unselected kind in both persisted
    artifacts; an unreadable prior Parquet report fails closed rather than being
    partially overwritten."""
    explicit_series = series is not None
    cal = (calendar_days if calendar_days is not None       # caller-supplied, or
           else consensus_calendar(root, read_fn=read_fn))  # the (cached) rebuild
    if series is None:
        # ALL regular-session series (intraday AND daily): intraday gets the
        # interior scan + missing-days, daily gets missing-days (kills the '—').
        series = discover_series(root, rth_only=True, kinds=kinds)
    series = list(series)
    n = len(series)
    attempts = []
    scans = []
    absence_reprobe_budget = (
        absence_reprobe_budget
        if isinstance(absence_reprobe_budget, dict)
        else _new_absence_reprobe_budget(root))
    for i, (t, iv) in enumerate(series):
        if progress is not None:
            try:
                progress(i, n, t, iv)
            except Exception:  # noqa: BLE001 — a UI hiccup must not abort the scan
                pass
        s = _gap_scan_with_provenance(
            root, t, iv, read_fn=read_fn, max_report=max_report,
            calendar_days=cal,
            fingerprint_fn=fingerprint_fn,
            absence_reprobe_budget=absence_reprobe_budget)
        attempts.append(s)
        if not s.get("error"):
            scans.append(s)
    rows = _gap_report_rows(scans)
    # EVERY scanned series is recorded (clean ones with totals 0) so the main table
    # can tell "scanned, clean" (✓) from "never scanned" (—). missing_days are the
    # connected-pattern candidates; the fill step clears the fetchable ones.
    summary = {
        f"{(s.get('ticker') or '').upper()} {s['interval']}":
        gap_evidence.entry_from_scan(s, cal)
        for s in attempts
    }
    by_series = {k: v["missing_total"] + v["missing_days"]
                 for k, v in summary.items()
                 if v["missing_total"] or v["missing_days"]}
    out = {"series_attempted": len(attempts),
           "series_scanned": len(scans),
           "series_failed": len(attempts) - len(scans),
           "gap_series": len(by_series),
           "gap_events": sum(s.get("gap_count", 0) for s in scans),
           "missing_total": sum(s.get("missing_total", 0) for s in scans),
           "missing_days_total": sum(s.get("missing_day_count", 0) for s in scans),
           "rows_written": len(rows),
           "summary": summary,
           "by_series": dict(sorted(by_series.items(), key=lambda kv: -kv[1])),
           "path": None, "sidecar": None}
    if write:
        with _GAP_PARQUET_LOCK:                  # serialise vs update_gap_report
            try:
                if preserve_existing:
                    prior = _read_gap_parquet(
                        Path(root).parent / GAP_REPORT_NAME)
                    if prior is None:
                        raise ss.StorageError(
                            "existing gap report is unreadable; refusing a "
                            "partial replacement")
                    attempted = {
                        ((s.get("ticker") or "").upper(), s.get("interval"))
                        for s in attempts
                    }
                    scanned = {
                        ((s.get("ticker") or "").upper(), s.get("interval"))
                        for s in scans
                    }
                    failed = attempted - scanned
                    wanted = _normalize_kinds(kinds)

                    def selected(row):
                        key = (str(row[0]).upper(), row[1])
                        if explicit_series:
                            return key in attempted
                        return (wanted is None
                                or ss.kind_of(str(row[1])) in wanted)

                    kept = [row for row in prior
                            if not selected(row)
                            or (str(row[0]).upper(), row[1]) in failed]
                    rows = kept + rows
                    rows.sort(key=lambda row: (row[2], row[0], row[1]))
                    out["rows_written"] = len(rows)
                out["path"] = _write_gap_parquet(
                    Path(root).parent / GAP_REPORT_NAME, rows)
            except (ss.StorageError, OSError) as exc:
                out["error"] = f"gap report write failed: {exc}"
        if not out.get("error"):
            if preserve_existing and not explicit_series:
                out["sidecar"] = merge_data_gaps(
                    root, summary, asof=asof, replace_kinds=kinds)
            elif preserve_existing:
                out["sidecar"] = merge_data_gaps(root, summary, asof=asof)
            else:
                out["sidecar"] = record_data_gaps(root, summary, asof=asof)
    return out


def _read_gap_parquet(path):
    """Read back the persisted gap rows -> [(ticker, interval, missing_dt,
    prev_dt, next_dt, gap_minutes)]. Returns [] when the file is ABSENT, and
    None when it EXISTS but is unreadable / wrong-schema — so a caller can tell a
    legitimate first run (proceed with empty base) from a corrupt report it must
    NOT silently discard. timestamps come back as naive datetimes (same shape
    _gap_report_rows emits)."""
    p = Path(path)
    if not p.exists():
        return []
    try:
        ss._require_pyarrow()
        import pyarrow.parquet as pq
        tbl = pq.read_table(p)
    except Exception:  # noqa: BLE001 — exists but corrupt/old: signal, don't crash
        return None
    if list(tbl.column_names) != _GAP_REPORT_COLS:
        return None
    d = {c: tbl.column(c).to_pylist() for c in _GAP_REPORT_COLS}
    return list(zip(d["ticker"], d["interval"], d["missing"],
                    d["prev_bar"], d["next_bar"], d["gap_minutes"]))


def update_gap_report(root, series, read_fn=None, asof=None, max_report=None,
                      calendar_days=None, fingerprint_fn=None, kinds=("",),
                      absence_reprobe_budget=None):
    """INCREMENTAL gap refresh for just the `series` a fetch touched: re-scan only
    those (ticker, interval) pairs, then MERGE. The rows of series that scanned
    SUCCESSFULLY replace their old rows in `data_gaps.parquet`; every other series'
    rows are kept — INCLUDING a touched series whose re-scan ERRORED (a transient
    read miss). Its old Parquet rows remain, while its sidecar entry is replaced by
    explicit non-current scan-error evidence. The whole read-merge-write is
    serialised by _GAP_PARQUET_LOCK
    so concurrent post-fetch scans can't clobber each other, and an EXISTING-but-
    unreadable report triggers an all-kind full rebuild (scan_all_gaps) instead
    of a destructive merge onto an empty base. `kinds` otherwise does not alter
    the explicitly supplied `series`. Returns
    {series_scanned, gap_series,
    missing_run, missing_all, by_series, path, sidecar} or carries an 'error'."""
    series = list(series or [])           # incl. daily — connected-pattern needs it
    cal = (calendar_days if calendar_days is not None
           else consensus_calendar(root, read_fn=read_fn))
    attempts = []
    scans = []
    absence_reprobe_budget = (
        absence_reprobe_budget
        if isinstance(absence_reprobe_budget, dict)
        else _new_absence_reprobe_budget(root))
    for t, iv in series:
        s = _gap_scan_with_provenance(
            root, t, iv, read_fn=read_fn, max_report=max_report,
            calendar_days=cal,
            fingerprint_fn=fingerprint_fn,
            absence_reprobe_budget=absence_reprobe_budget)
        attempts.append(s)
        if not s.get("error"):
            scans.append(s)
    fresh_rows = _gap_report_rows(scans)
    # evict ONLY series that actually scanned (canonical key) — an errored re-scan
    # keeps its old rows, so a momentary read miss never deletes recorded gaps.
    scanned = {((s.get("ticker") or "").upper(), s.get("interval"))
               for s in scans}
    summary = {
        f"{(s.get('ticker') or '').upper()} {s['interval']}":
        gap_evidence.entry_from_scan(s, cal)
        for s in attempts
    }
    by_series = {k: v["missing_total"] + v["missing_days"]
                 for k, v in summary.items()
                 if v["missing_total"] or v["missing_days"]}
    gp = Path(root).parent / GAP_REPORT_NAME
    out = {"series_attempted": len(attempts),
           "series_scanned": len(scans),
           "series_failed": len(attempts) - len(scans),
           "gap_series": len(by_series),
           "missing_run": sum(s.get("missing_total", 0) for s in scans),
           "missing_days_run": sum(s.get("missing_day_count", 0) for s in scans),
           "missing_all": 0,
           "by_series": dict(sorted(by_series.items(), key=lambda kv: -kv[1])),
           "path": None, "sidecar": None}
    rebuild = False
    with _GAP_PARQUET_LOCK:
        prior = _read_gap_parquet(gp)
        if prior is None:                  # exists but unreadable -> do NOT gut it
            rebuild = True
        else:
            kept = [r for r in prior if (str(r[0]).upper(), r[1]) not in scanned]
            all_rows = kept + fresh_rows
            all_rows.sort(key=lambda r: (r[2], r[0], r[1]))   # earliest first
            out["missing_all"] = len(all_rows)
            try:
                out["path"] = _write_gap_parquet(gp, all_rows)
            except (ss.StorageError, OSError) as exc:
                out["error"] = f"gap report write failed: {exc}"
    if rebuild:                            # corrupt/old report -> full self-heal
        full = scan_all_gaps(root, write=True, read_fn=read_fn, asof=asof,
                             fingerprint_fn=fingerprint_fn, kinds=None,
                             absence_reprobe_budget=absence_reprobe_budget)
        full["missing_run"] = full.get("missing_total", 0)
        full["missing_days_run"] = sum(    # consumers read this key (was missing -> 0)
            v.get("missing_days", 0) for v in (full.get("summary") or {}).values())
        return full
    out["sidecar"] = merge_data_gaps(root, summary, asof=asof)   # atomic load+merge+write
    return out


# --- batch online RE-validation ---------------------------------------------

def clear_reference_cache(ticker=None, rng=None):
    """Drop cached online reference data so the NEXT fetch pulls LIVE — what
    turns a re-validation into an actual re-check against the internet rather
    than a replay of the 15-min cache. No args clears everything; with `ticker`
    (and optional `rng`) clears just that key(s)."""
    if ticker is None:
        _REF_CACHE.clear()
        return
    t = stockanalysis_symbol(ticker)
    for k in [k for k in list(_REF_CACHE)
              if k[0] == t and (rng is None or k[1] == rng)]:
        _REF_CACHE.pop(k, None)


def _normalize_kinds(kinds):
    """Canonical kind filter; None selects every stored kind."""
    if kinds is None:
        return None
    values = (kinds,) if isinstance(kinds, str) else tuple(kinds)
    return frozenset("" if str(value).strip().casefold() == "trades"
                     else str(value).strip().casefold()
                     for value in values)


def discover_series(root, rth_only=True, kinds=("",)):
    """Every stored (ticker, interval) under `root`, from the manifests only (no
    internet, no bar reads). `rth_only` filters parsed -pre/-post sessions;
    `kinds` filters parsed data kinds (TRADES-only by default, None selects all)."""
    wanted_kinds = _normalize_kinds(kinds)
    out = []
    try:
        entries = sorted(Path(root).iterdir())
    except OSError:
        return out
    for d in entries:
        try:
            if not (d.is_dir() and ss.TICKER_DIR_RE.match(d.name)):
                continue
        except Exception:  # noqa: BLE001 — be lenient if the RE is unavailable
            if not d.is_dir():
                continue
        man = ss.load_manifest(d) or {}
        for iv in sorted(man.get("intervals", {})):
            _base, kind, session = ss.parse_interval(iv)
            if rth_only and session != "rth":
                continue
            if wanted_kinds is not None and kind not in wanted_kinds:
                continue
            out.append((d.name, iv))
    return out


def _classify(res):
    """(status, severity, note) for a validate_series result. Higher severity =
    more urgent. status ∈ split | discrepancy | error | inconclusive | ok."""
    if res.get("error"):
        err = str(res["error"])
        if "no overlapping" in err or "no stored" in err:
            return ("inconclusive", 1, err)
        return ("error", 2, err)
    ls, sf = res.get("level_shift"), res.get("suspected_factor")
    if ls or sf:
        bits = []
        if ls:
            bits.append(f"level shift ×{ls.get('factor')} at {ls.get('date')}")
        if sf:
            bits.append(f"uniform basis ×{sf}")
        return ("split", 4, "; ".join(bits))
    fd = res.get("flagged_days") or []
    if fd:
        shown = ", ".join(fd[:5]) + ("…" if len(fd) > 5 else "")
        return ("discrepancy", 3, f"{len(fd)} day(s) off: {shown}")
    return ("ok", 0, f"{res.get('checked', 0)} day(s) match "
                     f"(score {res.get('score')})")


def _split_mag(r):
    """Magnitude (|factor-1|) of a split/basis discrepancy, for sort
    tie-breaking — so a whole-series WRONG SCALE (uniform factor, which scores
    a deceptive 1.0 because the rolling baseline divides the constant out) does
    NOT sort below a milder mid-series step that scores < 1.0."""
    m = 0.0
    sf, ls = r.get("suspected_factor"), r.get("level_shift")
    if sf:
        m = max(m, abs(sf - 1.0))
    if ls and ls.get("factor") is not None:
        m = max(m, abs(ls["factor"] - 1.0))
    return m


def revalidate_library(root, series=None, sensitivity="normal", rng="5Y",
                       on_progress=None, read_fn=None, ref_fn=None,
                       force_fresh=True, save_reference=False, *,
                       _test_capability=None, _evidence_dir=None,
                       _authority=None, _clock=None, _governors=None,
                       _opener=None):
    """One held batch root; each series runs in its private child body."""
    operation = fops.begin_operation(
        "validation", _validation_directory(_evidence_dir),
        test_capability=_test_capability, authority=_authority,
        clock=_clock, governors=_governors)
    try:
        fops.require_root_admission(operation)
        pending_saves = []
        pending_cache = []
        with fops.scoped_worker(operation.child(
                "validation-library", rights={"http.stockanalysis.validation"}),
                close=True) as worker:
            result = _revalidate_library_body(
                root, series, sensitivity, rng, on_progress, read_fn, ref_fn,
                force_fresh, save_reference, _fetch_worker=worker,
                _opener=_opener, _pending_saves=pending_saves,
                _pending_cache=pending_cache)
        operation.seal()
        _publish_reference_cache(pending_cache, operation.context)
        for item in pending_saves:
            save_daily_reference(*item)
        return result
    finally:
        operation.close()


def _revalidate_library_body(root, series=None, sensitivity="normal", rng="5Y",
                             on_progress=None, read_fn=None, ref_fn=None,
                             force_fresh=True, save_reference=False, *,
                             _fetch_worker=None, _opener=None,
                             _pending_saves=None, _pending_cache=None):
    """Re-validate MANY stored series against the online daily reference in ONE
    pass — the batch form of validate_series. `series` is an iterable of
    (ticker, interval); None discovers every stored RTH series under `root`.
    `force_fresh` (default True) bypasses the shared cache under a guarded
    worker (legacy parser-only calls clear it); each ticker is fetched once
    into a run-local cache, so several intervals share the same response.
    Nothing enters the shared cache until a successful operation seal.
    One bad series can't stop the rest. Returns
    {summary, results} with results sorted worst-first. read_fn/ref_fn inject."""
    log = on_progress or (lambda m: None)
    if force_fresh and ref_fn is None and _fetch_worker is None:
        clear_reference_cache()
    run_cache = {} if _fetch_worker is not None else None
    series = list(discover_series(root) if series is None else series)
    if not series:
        return {"summary": {"total": 0, "ok": 0, "problems": 0,
                            "needs_attention": 0, "counts": {},
                            "sensitivity": sensitivity}, "results": []}
    log(f"revalidating {len(series)} series against the online reference…")
    results = []
    for i, (ticker, interval) in enumerate(series, 1):
        log(f"[{i}/{len(series)}] {ticker} {interval}…")
        try:
            res = _validate_series_body(
                root, ticker, interval, rng=rng, sensitivity=sensitivity,
                read_fn=read_fn, ref_fn=ref_fn,
                save_reference=save_reference, _fetch_worker=_fetch_worker,
                _opener=_opener, _pending_saves=_pending_saves,
                _pending_cache=_pending_cache,
                _force_fresh=force_fresh, _run_cache=run_cache)
        except Exception as exc:  # noqa: BLE001 — isolate one bad series
            if _terminal_reference_error(exc, _fetch_worker):
                raise
            res = {"error": f"validate raised: {exc!r}"}
        status, severity, note = _classify(res)
        row = {"ticker": ticker, "interval": interval,
               "status": status, "severity": severity, "note": note,
               # a uniform wrong-scale series scores a deceptive 1.0
               # (baseline divides the constant out) — suppress it so
               # the row never reads 'perfect score on a broken series'
               "score": (None if (res.get("suspected_factor")
                                  and not res.get("flagged_days"))
                         else res.get("score")),
               "checked": res.get("checked", 0),
               "days_derived": res.get("days_derived"),
               "level_shift": res.get("level_shift"),
               "suspected_factor": res.get("suspected_factor"),
               "flagged_days": res.get("flagged_days", []),
               "error": res.get("error")}
        if res.get("error_type"):
            row["error_type"] = res["error_type"]
        if res.get("quarantine"):
            row["quarantine"] = res["quarantine"]
        results.append(row)
        log(f"    -> {status}: {note}")
    results.sort(key=lambda r: (-r["severity"], -_split_mag(r),
                                r["score"] if r["score"] is not None else 1.0,
                                r["ticker"], r["interval"]))
    counts = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    summary = {"total": len(results), "ok": counts.get("ok", 0),
               "problems": sum(1 for r in results if r["severity"] >= 3),
               "needs_attention": sum(1 for r in results
                                      if r["status"] in ("error",
                                                         "inconclusive")),
               "counts": counts, "sensitivity": sensitivity}
    log(f"done — {summary['ok']}/{summary['total']} clean, "
        f"{summary['problems']} with discrepancies, "
        f"{summary['needs_attention']} couldn't be checked")
    return {"summary": summary, "results": results}


# --- per-stock cross-validation status (the main-table column) --------------

_CROSS_FILE = "_cross_validation.json"
_XVAL_ROW_CAP = 300              # flagged rows persisted per ticker (file stays small)
CROSS_VALIDATION_SCHEMA_VERSION = 4
_XVAL_ERROR_CAP = 240
_XVAL_EVIDENCE_FIELDS = (
    "open", "high", "low", "close", "volume", "level_shift",
    "bar", "gap", "daily-close")
_LINEAGE_BOUNDARY_FILE = "_lineage_boundaries.json"
_LINEAGE_BOUNDARY_VERSION = 1
_LINEAGE_BOUNDARY_MAX_BYTES = 256 * 1024
LINEAGE_BOUNDARY_PINS = True


def _lineage_pin_ticker(value):
    if not isinstance(value, dict):
        return None
    ticker = value.get("ticker")
    if not isinstance(ticker, str):
        return None
    try:
        canonical = ss.canonical_ticker(ticker)
    except Exception:  # noqa: BLE001 - malformed policy is report state
        return None
    if ticker != canonical or not ss.TICKER_DIR_RE.fullmatch(canonical):
        return None
    return canonical


def _valid_lineage_fingerprint(value, ticker, boundary_month):
    """A pin binds the LINEAGE-ERA identity, never the whole interval.

    F-CLAUDE-80-1: a whole-interval fingerprint changes on every ordinary
    forward fetch, which would stale the pin into loud re-flagging of a
    settled era after each new month. The pin therefore carries a
    scope-through-month identity covering exactly the months that can hold
    pre-boundary days; the unscoped fingerprint keeps its separate job of
    detecting a mid-run interval race.
    """
    fields = {
        "schema_version", "algorithm", "sha256", "ticker", "interval",
        "present", "backfill_incomplete", "month_count",
        "verified_absent_count", "scope_through_month",
    }
    if (not isinstance(value, dict) or set(value) != fields
            or value.get("scope_through_month") != boundary_month
            or value.get("schema_version") != ss.INTERVAL_FINGERPRINT_VERSION
            or value.get("algorithm") != "sha256"
            or not _lower_hex(value.get("sha256"), 64, 64)
            or value.get("ticker") != ticker
            or not isinstance(value.get("interval"), str)
            or not ss.INTERVAL_RE.fullmatch(value["interval"])
            or ss.kind_of(value["interval"]) in ss.RATIO_KINDS
            or value.get("present") is not True
            or not isinstance(value.get("backfill_incomplete"), bool)
            or isinstance(value.get("month_count"), bool)
            or not isinstance(value.get("month_count"), int)
            or value.get("month_count") < 0
            or isinstance(value.get("verified_absent_count"), bool)
            or not isinstance(value.get("verified_absent_count"), int)
            or value.get("verified_absent_count") < 0):
        return False
    return True


def _valid_lineage_pin(value, ticker):
    if set(value) != {
            "ticker", "boundary_date", "verdict", "scope",
            "interval_fingerprints", "review"}:
        return False, "pin fields do not match schema version 1"
    if (value.get("verdict") != "lineage_boundary"
            or value.get("scope") != "price"):
        return False, "pin verdict/scope is not lineage_boundary/price"
    boundary_text = value.get("boundary_date")
    try:
        boundary = date.fromisoformat(boundary_text)
    except (TypeError, ValueError):
        return False, "pin boundary date is malformed"
    if boundary.isoformat() != boundary_text:
        return False, "pin boundary date is not canonical"

    fingerprints = value.get("interval_fingerprints")
    if (not isinstance(fingerprints, list) or not fingerprints
            or len(fingerprints) > 128):
        return False, "pin interval fingerprints are malformed"
    intervals = []
    for fingerprint in fingerprints:
        if not _valid_lineage_fingerprint(fingerprint, ticker,
                                          boundary_text[:7]):
            return False, "pin interval fingerprint is malformed"
        intervals.append(fingerprint["interval"])
    if len(intervals) != len(set(intervals)):
        return False, "pin has contradictory interval fingerprints"

    review = value.get("review")
    if (not isinstance(review, dict)
            or set(review) != {
                "checkpoint", "approval_commit", "approved_by",
                "approved_at", "user_verdict_date"}
            or not _lower_hex(review.get("checkpoint"), 7, 40)
            or not _lower_hex(review.get("approval_commit"), 7, 40)
            or review.get("approved_by") not in {"CLAUDE", "USER"}
            or not isinstance(review.get("approved_at"), str)
            or not review.get("approved_at")):
        return False, "pin review provenance is malformed"
    try:
        approved_at = datetime.fromisoformat(
            review["approved_at"].replace("Z", "+00:00"))
        verdict_date = date.fromisoformat(review.get("user_verdict_date"))
    except (TypeError, ValueError):
        return False, "pin review provenance date is malformed"
    if (verdict_date.isoformat() != review["user_verdict_date"]
            or boundary > verdict_date
            or approved_at.date() < verdict_date):
        return False, "pin review provenance dates contradict the boundary"
    return True, None


def _lineage_boundary_config(root):
    """Hot-load schema-exact reporting pins without ever suppressing on error."""
    out = {"pins": {}, "halted": {}, "error": None}
    path = Path(root) / _LINEAGE_BOUNDARY_FILE
    try:
        encoded = path.read_bytes()
    except FileNotFoundError:
        return out
    except OSError as exc:
        out["error"] = (f"cannot read {_LINEAGE_BOUNDARY_FILE}: "
                        f"{type(exc).__name__}: {exc}")
        return out
    if len(encoded) > _LINEAGE_BOUNDARY_MAX_BYTES:
        out["error"] = f"{_LINEAGE_BOUNDARY_FILE} exceeds policy size limit"
        return out
    try:
        raw = json.loads(
            encoded.decode("utf-8"),
            object_pairs_hook=ss._json_object_without_duplicate_keys)
    except (UnicodeError, ValueError, TypeError) as exc:
        out["error"] = f"{_LINEAGE_BOUNDARY_FILE} is malformed: {exc}"
        return out
    if (not isinstance(raw, dict)
            or set(raw) != {"version", "lineage_boundaries"}):
        out["error"] = (
            f"{_LINEAGE_BOUNDARY_FILE} root fields do not match schema version 1")
        return out
    if raw.get("version") != _LINEAGE_BOUNDARY_VERSION:
        out["error"] = f"{_LINEAGE_BOUNDARY_FILE} requires policy version 1"
        return out
    entries = raw.get("lineage_boundaries")
    if (not isinstance(entries, list) or len(entries) > 10_000):
        out["error"] = "lineage_boundaries is not a bounded list"
        return out
    for index, value in enumerate(entries):
        ticker = _lineage_pin_ticker(value)
        if ticker is None:
            out["pins"].clear()
            out["error"] = (
                f"lineage_boundaries[{index}] has no valid ticker identity")
            return out
        if ticker in out["pins"] or ticker in out["halted"]:
            out["pins"].pop(ticker, None)
            out["halted"][ticker] = (
                "multiple or contradictory lineage boundary pins target this ticker")
            continue
        valid, error = _valid_lineage_pin(value, ticker)
        if not valid:
            out["halted"][ticker] = error
            continue
        out["pins"][ticker] = dict(value)
    return out


def _lineage_boundary_policy(config, root, ticker, interval,
                             interval_current):
    """Return an apply-or-ignore decision; every error direction is fail-open.

    The pin is matched against a freshly captured LINEAGE-ERA identity
    (months at or before the boundary month), not the whole-interval
    fingerprint, so appending a new month cannot stale a settled verdict —
    while any change inside the frozen era still does (F-CLAUDE-80-1).
    """
    if config is None:
        return {"status": "not_applicable"}
    if config.get("error"):
        return {"status": "ignored", "error": str(config["error"])}
    try:
        canonical = ss.canonical_ticker(ticker)
    except Exception as exc:  # noqa: BLE001 - invalid identity cannot suppress
        return {"status": "ignored", "error": f"invalid ticker identity: {exc}"}
    halted = config.get("halted") or {}
    if canonical in halted:
        return {"status": "ignored", "error": str(halted[canonical])}
    pin = (config.get("pins") or {}).get(canonical)
    if pin is None:
        return {"status": "not_configured"}
    expected = next((item for item in pin["interval_fingerprints"]
                     if item.get("interval") == interval), None)
    if expected is None:
        return {
            "status": "ignored",
            "error": f"lineage boundary pin has no fingerprint for {interval}",
        }
    try:
        observed = ss.interval_state_fingerprint_through_month(
            root, canonical, interval, pin["boundary_date"][:7])
    except Exception as exc:  # noqa: BLE001 - unreadable era cannot suppress
        return {"status": "ignored",
                "error": ("lineage-era fingerprint unavailable: "
                          f"{type(exc).__name__}: {exc}")}
    if observed != expected:
        return {"status": "ignored",
                "error": "lineage boundary interval fingerprint is stale"}
    if not interval_current:
        return {"status": "ignored",
                "error": "interval state changed during lineage pin evaluation"}
    return {"status": "applied", "pin": pin}


def _lineage_boundary_public(pin):
    return {
        "ticker": pin["ticker"],
        "boundary_date": pin["boundary_date"],
        "verdict": pin["verdict"],
        "scope": pin["scope"],
        "review": dict(pin["review"]),
    }


def cross_validation_key(ticker, interval=None):
    """Sidecar key for a cross-validation verdict.

    New entries are keyed by full series (`TICKER interval`) so ratio/price
    verdicts for the same ticker cannot overwrite each other. A blank interval
    deliberately falls back to the legacy plain ticker key for compatibility
    with older callers/tests.
    """
    raw_ticker = str(ticker or "").strip()
    if not raw_ticker:
        return ""
    try:
        t = ss.canonical_ticker(raw_ticker)
    except Exception:  # noqa: BLE001 - invalid identities cannot be persisted
        return ""
    iv = str(interval or "").strip()
    if iv and not ss.INTERVAL_RE.fullmatch(iv):
        return ""
    return f"{t} {iv}" if t and iv else t


def _bounded_xval_text(value):
    text = " ".join(str(value).split())
    return text[:_XVAL_ERROR_CAP]


def _summarize_xval_rows(rows):
    fields = {name: 0 for name in _XVAL_EVIDENCE_FIELDS}
    other = 0
    dates = set()
    source_rows = rows if isinstance(rows, list) else []
    for row in source_rows:
        if not isinstance(row, dict):
            other += 1
            continue
        field = row.get("field")
        if isinstance(field, str) and field in fields:
            fields[field] += 1
        else:
            other += 1
        day = row.get("date")
        if isinstance(day, str) and day:
            dates.add(day)
    fields = {key: value for key, value in fields.items() if value}
    if other:
        fields["other"] = other
    return {"source_row_count": len(source_rows),
            "distinct_dates": len(dates), "field_counts": fields}


def _capture_interval_fingerprint(fingerprint_fn, root, ticker, interval,
                                  *, require_present=True):
    try:
        info = fingerprint_fn(root, ticker, interval)
        expected_ticker = ss.canonical_ticker(ticker)
        valid = (isinstance(info, dict)
                 and info.get("schema_version")
                 == ss.INTERVAL_FINGERPRINT_VERSION
                 and info.get("algorithm") == "sha256"
                 and isinstance(info.get("sha256"), str)
                 and len(info["sha256"]) == 64
                 and all(c in "0123456789abcdef" for c in info["sha256"])
                 and info.get("ticker") == expected_ticker
                 and info.get("interval") == interval
                 and isinstance(info.get("present"), bool)
                 and (not require_present or info.get("present") is True)
                 and isinstance(info.get("backfill_incomplete"), bool)
                 and isinstance(info.get("month_count"), int)
                 and not isinstance(info.get("month_count"), bool)
                 and info["month_count"] >= 0
                 and isinstance(info.get("verified_absent_count"), int)
                 and not isinstance(info.get("verified_absent_count"), bool)
                 and info["verified_absent_count"] >= 0)
        if not valid:
            raise ValueError("fingerprint helper returned an invalid contract")
        return dict(info), None
    except Exception as exc:  # noqa: BLE001 - provenance failure is a verdict
        return None, _bounded_xval_text(exc)


_COMBINED_INPUT_DATE_CAP = 100_000


def _combined_dates(value, label):
    if (not isinstance(value, list)
            or len(value) > _COMBINED_INPUT_DATE_CAP):
        raise ValueError(f"combined {label} list is invalid or too large")
    out = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError(f"combined {label} list contains a non-string date")
        try:
            parsed = date.fromisoformat(item)
        except ValueError:
            raise ValueError(f"combined {label} list contains an invalid date") from None
        if parsed.isoformat() != item:
            raise ValueError(f"combined {label} list contains a noncanonical date")
        out.append(item)
    if len(out) != len(set(out)):
        raise ValueError(f"combined {label} list contains duplicates")
    return sorted(out)


def _normalize_combined_observed(observed):
    if not isinstance(observed, dict):
        raise TypeError("combined check returned a non-object result")
    status = observed.get("status")
    if status not in {"flagged", "ok", "error", "inconclusive"}:
        raise ValueError("combined check returned an invalid status")
    candidates = _combined_dates(observed.get("candidates", []), "candidate")
    persistent = _combined_dates(observed.get("persistent", []), "persistent")
    cleared = _combined_dates(observed.get("cleared", []), "cleared")
    candidate_set = set(candidates)
    persistent_set = set(persistent)
    cleared_set = set(cleared)
    if (persistent_set & cleared_set
            or not (persistent_set | cleared_set).issubset(candidate_set)
            or (status == "flagged" and not persistent)
            or (status == "ok" and persistent)):
        raise ValueError("combined check returned inconsistent dates/status")

    priority = [*persistent, *(item for item in cleared
                               if item not in persistent_set)]
    shown_candidates = priority[:_XVAL_ROW_CAP]
    if len(shown_candidates) < _XVAL_ROW_CAP:
        chosen = set(shown_candidates)
        shown_candidates.extend(
            item for item in candidates
            if item not in chosen
        )
        shown_candidates = shown_candidates[:_XVAL_ROW_CAP]
    shown_set = set(shown_candidates)
    result = {
        "status": status,
        "candidates": sorted(shown_candidates),
        "persistent": [item for item in persistent if item in shown_set],
        "cleared": [item for item in cleared if item in shown_set],
        "candidate_count": len(candidates),
        "persistent_count": len(persistent),
        "cleared_count": len(cleared),
        "row_cap": _XVAL_ROW_CAP,
        "truncated": (len(candidates) > len(shown_candidates)
                      or len(persistent) > sum(
                          item in shown_set for item in persistent)
                      or len(cleared) > sum(
                          item in shown_set for item in cleared)),
    }
    note = _bounded_xval_text(observed.get("note", ""))
    if note:
        result["note"] = note
    coverage = observed.get("reference_coverage")
    if coverage is None:
        coverage = {"external": None, "internal": None}
    if not isinstance(coverage, dict):
        raise ValueError("combined reference coverage is not an object")
    normalized_coverage = {}
    for source in ("external", "internal"):
        item = coverage.get(source)
        if item is None:
            normalized_coverage[source] = None
            continue
        if not isinstance(item, dict):
            raise ValueError(f"combined {source} coverage is not an object")
        required = ("first_date", "last_date", "day_count",
                    "derived_first_date", "derived_last_date",
                    "derived_day_count", "head_gap_days",
                    "head_tolerance_days", "head_ok", "requested_range")
        if any(key not in item for key in required):
            raise ValueError(f"combined {source} coverage is incomplete")
        normalized_coverage[source] = {key: item[key] for key in required}
    result["reference_coverage"] = normalized_coverage
    coverage_error = _bounded_xval_text(
        observed.get("reference_coverage_error", ""))
    if status in {"flagged", "ok"} and (
            normalized_coverage["external"] is None
            or normalized_coverage["internal"] is None
            or coverage_error):
        raise ValueError(
            "combined verdict requires both valid reference coverages")
    if coverage_error:
        result["reference_coverage_error"] = coverage_error
    return result


def _interval_provenance(before_value, before_problem, after_value,
                         after_problem, label):
    current = (before_value is not None and after_value is not None
               and before_value == after_value)
    state = after_value or before_value or {}
    fingerprint = {
        "schema_version": ss.INTERVAL_FINGERPRINT_VERSION,
        "algorithm": "sha256",
        "before_sha256": (before_value or {}).get("sha256"),
        "after_sha256": (after_value or {}).get("sha256"),
        "sha256": (after_value or {}).get("sha256") if current else None,
        "current": current,
        "present": state.get("present"),
        "backfill_incomplete": state.get("backfill_incomplete"),
        "month_count": state.get("month_count"),
        "verified_absent_count": state.get("verified_absent_count"),
    }
    errors = [problem for problem in (before_problem, after_problem) if problem]
    if errors:
        fingerprint["error"] = _bounded_xval_text("; ".join(errors))
    elif not current:
        fingerprint["error"] = f"{label} changed during validation"
    return fingerprint


def combined_crosscheck_with_provenance(root, ticker, interval, check_fn,
                                        asof=None, fingerprint_fn=None):
    """Run a complete three-source check under two-interval provenance.

    ``check_fn`` must include every stored read and external/live fetch used by
    the check. A source failure is persisted as ``error``; an unreadable or
    changing selected or stored-daily interval preserves the observed diagnosis
    but publishes ``inconclusive`` so stale evidence cannot affect precedence.
    """
    ticker = ss.canonical_ticker(ticker)
    interval = str(interval or "").strip()
    started_at = datetime.now().astimezone().isoformat(timespec="seconds")
    fp_fn = fingerprint_fn or ss.interval_state_fingerprint
    before, before_error = _capture_interval_fingerprint(
        fp_fn, root, ticker, interval)
    internal_before, internal_before_error = _capture_interval_fingerprint(
        fp_fn, root, ticker, COMBINED_INTERNAL_INTERVAL)
    try:
        result = _normalize_combined_observed(check_fn())
    except Exception as exc:  # noqa: BLE001 - a failed refresh supersedes old evidence
        result = {
            "status": "error",
            "candidates": [],
            "persistent": [],
            "cleared": [],
            "candidate_count": 0,
            "persistent_count": 0,
            "cleared_count": 0,
            "row_cap": _XVAL_ROW_CAP,
            "truncated": False,
            "reference_coverage": {"external": None, "internal": None},
            "note": "combined check raised: "
                    + _bounded_xval_text(f"{type(exc).__name__}: {exc}"),
        }
    result.update({"schema_version": COMBINED_FLAGS_SCHEMA_VERSION,
                   "ticker": ticker, "interval": interval,
                   "internal_interval": COMBINED_INTERNAL_INTERVAL,
                   "source": "3-source"})
    after, after_error = _capture_interval_fingerprint(
        fp_fn, root, ticker, interval)
    internal_after, internal_after_error = _capture_interval_fingerprint(
        fp_fn, root, ticker, COMBINED_INTERNAL_INTERVAL)
    finished_at = datetime.now().astimezone().isoformat(timespec="seconds")

    fingerprint = _interval_provenance(
        before, before_error, after, after_error, "selected interval")
    internal_fingerprint = _interval_provenance(
        internal_before, internal_before_error,
        internal_after, internal_after_error, "internal daily interval")
    current = fingerprint["current"] and internal_fingerprint["current"]

    result.update({"asof": asof or finished_at,
                   "started_at": started_at,
                   "finished_at": finished_at,
                   "interval_fingerprint": fingerprint,
                   "internal_interval_fingerprint": internal_fingerprint})
    if not current:
        observed_status = result["status"]
        observed_note = _bounded_xval_text(result.get("note", ""))
        diagnostic_fingerprint = (
            fingerprint if not fingerprint["current"]
            else internal_fingerprint)
        diagnostic_fingerprint["observed_status"] = observed_status
        if observed_note:
            diagnostic_fingerprint["observed_note"] = observed_note
        reasons = [item.get("error") for item in (
            fingerprint, internal_fingerprint) if not item["current"]]
        result["status"] = "inconclusive"
        result["note"] = ("non-current combined evidence: "
                          + _bounded_xval_text("; ".join(
                              reason or "interval provenance unavailable"
                              for reason in reasons)))
    return result


def _xval_evidence_counts(detail):
    rows = (detail or {}).get("rows") if isinstance(detail, dict) else []
    rows = rows if isinstance(rows, list) else []
    rows = rows[:_XVAL_ROW_CAP]
    fallback = _summarize_xval_rows(rows)
    source_count = (detail or {}).get("source_row_count")
    if (isinstance(source_count, bool) or not isinstance(source_count, int)
            or source_count < len(rows)):
        source_count = fallback["source_row_count"]
    distinct_dates = (detail or {}).get("distinct_dates")
    if (isinstance(distinct_dates, bool) or not isinstance(distinct_dates, int)
            or distinct_dates < 0 or distinct_dates > source_count):
        distinct_dates = fallback["distinct_dates"]
    supplied_fields = (detail or {}).get("field_counts")
    allowed = set(_XVAL_EVIDENCE_FIELDS) | {"other"}
    if (not isinstance(supplied_fields, dict)
            or any(k not in allowed or isinstance(v, bool)
                   or not isinstance(v, int) or v < 0
                   for k, v in supplied_fields.items())
            or sum(supplied_fields.values()) != source_count):
        supplied_fields = fallback["field_counts"]
        source_count = fallback["source_row_count"]
        distinct_dates = fallback["distinct_dates"]
    return {"row_count": source_count, "persisted_row_count": len(rows),
            "row_cap": _XVAL_ROW_CAP, "truncated": source_count > len(rows),
            "distinct_dates": distinct_dates,
            "field_counts": dict(supplied_fields)}


def _xval_severity_reco(res, max_pct):
    """(SEVERITY, recommendation) for a validate_series result — the action
    guidance the data-menu detail popup shows. HIGH = a split/scale boundary or
    a large daily disagreement; MEDIUM/LOW scale with the worst % difference."""
    ls, sf = res.get("level_shift"), res.get("suspected_factor")
    fd = res.get("flagged_days") or []
    if ls or sf:
        factor = sf if sf else (ls or {}).get("factor")
        when = (ls or {}).get("date")
        where = f" at {when}" if when else ""
        return ("HIGH",
                f"Most likely a split / scale boundary (×{factor}{where}) — an "
                f"adjustment-basis mismatch, not bad ticks. Run the basis doctor "
                f"(Verify ▸ basis) to classify and record it, then re-validate.")
    if fd:
        sev = "HIGH" if max_pct >= 10 else ("MEDIUM" if max_pct >= 2 else "LOW")
        return (sev,
                f"{len(fd)} day(s) disagree with the online daily by up to "
                f"{max_pct}%. Check those dates for missing chunks or bad ticks "
                f"and re-fetch; if it persists, the stored basis differs from the "
                f"reference (run the basis doctor).")
    return ("LOW", "Within the rolling-baseline tolerance — no action needed.")


def _xval_detail(res):
    """Per-discrepancy detail for the data-menu double-click popup, built from a
    validate_series result so it matches EXACTLY what set the ✓/⚠/✗ mark: the
    flagged rows (date / field / derived / reference / % off), the level-shift
    or uniform-scale info, day counts, the worst %, plus a SEVERITY and a
    RECOMMENDATION. JSON-serializable for the sidecar."""
    flagged = res.get("flagged") or []
    summary = _summarize_xval_rows(flagged)
    rows = [{"date": f.get("date"), "field": f.get("field"),
             "derived": f.get("derived"), "reference": f.get("reference"),
             "percent": round(abs(f.get("norm_dev", 0.0) or 0.0) * 100, 2)}
            for f in flagged[:_XVAL_ROW_CAP]]
    max_pct = round(max((abs(f.get("norm_dev", 0.0) or 0.0) * 100
                         for f in flagged), default=0.0), 2)
    sev, rec = _xval_severity_reco(res, max_pct)
    return {"severity": sev, "recommendation": rec,
            "flagged_count": len(res.get("flagged_days") or []),
            "checked": res.get("checked", 0), "matched": res.get("matched"),
            "max_percent": max_pct,
            "level_shift": res.get("level_shift"),
            "suspected_factor": res.get("suspected_factor"),
            "rows": rows, "row_cap": _XVAL_ROW_CAP, "shown": len(rows),
            **summary}


def _canonical_lineage_row_date(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


def _lineage_bucketed_result(res, pin):
    """Split only canonical pre-boundary evidence from active scoring.

    The input result and its evidence rows are never mutated. Invalid/undated
    evidence remains active, which is the safe fail-open direction.
    """
    boundary = date.fromisoformat(pin["boundary_date"])
    source_rows = res.get("flagged")
    source_rows = list(source_rows) if isinstance(source_rows, list) else []
    active_rows, known_rows = [], []
    active_days, known_days = set(), set()
    for row in source_rows:
        row_day = (_canonical_lineage_row_date(row.get("date"))
                   if isinstance(row, dict) else None)
        if row_day is not None and row_day < boundary:
            known_rows.append(row)
            known_days.add(row_day.isoformat())
        else:
            active_rows.append(row)
            if row_day is not None:
                active_days.add(row_day.isoformat())

    for value in res.get("flagged_days") or []:
        row_day = _canonical_lineage_row_date(value)
        if row_day is not None and row_day < boundary:
            known_days.add(row_day.isoformat())
        else:
            active_days.add(value if isinstance(value, str) else str(value))

    filtered = dict(res)
    filtered["flagged"] = active_rows
    filtered["flagged_days"] = sorted(active_days)
    checked = filtered.get("checked")
    if isinstance(checked, int) and not isinstance(checked, bool) and checked > 0:
        filtered["matched"] = max(0, checked - len(active_days))
        filtered["score"] = round(max(0.0, 1 - len(active_days) / checked), 4)
    else:
        filtered["score"] = None

    suppressed_shift = None
    shift = res.get("level_shift")
    if isinstance(shift, dict):
        shift_day = _canonical_lineage_row_date(shift.get("date"))
        if shift_day is not None and shift_day < boundary:
            suppressed_shift = dict(shift)
            filtered["level_shift"] = None
            known_days.add(shift_day.isoformat())

    detail = _xval_detail(filtered)
    known_summary = _summarize_xval_rows(known_rows)
    examples = [dict(row) if isinstance(row, dict) else row
                for row in known_rows[:_XVAL_ROW_CAP]]
    known = {
        "boundary_date": pin["boundary_date"],
        "verdict": pin["verdict"],
        "scope": pin["scope"],
        "flagged_count": len(known_days),
        "row_count": known_summary["source_row_count"],
        "distinct_dates": known_summary["distinct_dates"],
        "field_counts": known_summary["field_counts"],
        "examples": examples,
        "example_cap": _XVAL_ROW_CAP,
        "shown": len(examples),
        "truncated": len(known_rows) > len(examples),
    }
    if suppressed_shift is not None:
        known["level_shift"] = suppressed_shift
    detail["known_lineage_era"] = known
    return filtered, detail


def _apply_lineage_boundary(outcome, res, pin):
    filtered, detail = _lineage_bucketed_result(res, pin)
    status, _severity, note = _classify(filtered)
    verdict = {"ok": "validated", "split": "discrepancy",
               "discrepancy": "discrepancy", "inconclusive": "inconclusive",
               "error": "inconclusive"}.get(status, "inconclusive")
    score = (None if (filtered.get("suspected_factor")
                      and not filtered.get("flagged_days"))
             else filtered.get("score"))
    known = detail["known_lineage_era"]
    known_note = (
        f"{known['flagged_count']} pre-{pin['boundary_date']} day(s) classified "
        "as known lineage-era evidence")
    note = _bounded_xval_text(
        f"No active discrepancies; {known_note}" if status == "ok"
        and known["flagged_count"] else f"{note}; {known_note}")

    published = dict(outcome)
    published.update({
        "severity": detail["severity"],
        "detail": detail,
        "lineage_boundary": _lineage_boundary_public(pin),
    })
    if outcome.get("status") in {"validated", "discrepancy"}:
        published.update({"status": verdict, "note": note, "score": score})
    return published


_RATIO_MAX_REASONABLE = 5.0


def _ratio_daily_close(bars):
    by_day = {}
    for b in bars or []:
        try:
            by_day[b[0].date().isoformat()] = float(b[4])
        except Exception:  # noqa: BLE001
            continue
    return by_day


def ratio_structural_check(root, ticker, interval="1m-iv", read_fn=None,
                           max_value=_RATIO_MAX_REASONABLE):
    """Structural validation for IV/HVOL ratio series.

    Ratio kinds are unitless, so the price-reference cross-check is invalid.
    This check keeps the run visible and useful: range sanity, interior-gap
    coverage, and 1m-kind vs 1d-kind close consistency when both are stored.
    """
    kind = ss.kind_of(interval)
    base = {"ticker": ticker, "interval": interval, "kind": kind,
            "checked": 0, "flagged_days": [], "flagged": [],
            "gap_missing_total": 0, "daily_compared": 0}
    reader = read_fn or read_series
    try:
        bars = reader(root, ticker, interval)
    except Exception as exc:  # noqa: BLE001
        return {**base, "error": f"read failed: {exc}"}
    if not bars:
        return {**base, "error": "no stored ratio bars"}
    import math
    bad = []
    dates = set()
    for b in bars:
        try:
            day = b[0].date().isoformat()
            vals = [float(x) for x in b[1:5]]
        except Exception as exc:  # noqa: BLE001
            bad.append({"date": "", "field": "bar", "reason": f"bad row: {exc}"})
            continue
        dates.add(day)
        for field, val in zip(("open", "high", "low", "close"), vals):
            if not math.isfinite(val) or val < 0 or val > max_value:
                bad.append({"date": day, "field": field, "value": val,
                            "reason": f"outside 0..{max_value:g}"})
    try:
        gaps = scan_series_gaps(root, ticker, interval, read_fn=read_fn,
                                max_report=20)
    except Exception as exc:  # noqa: BLE001
        gaps = {"error": str(exc)}
    gap_missing = int((gaps or {}).get("missing_total") or 0)
    if gap_missing:
        for g in (gaps.get("gaps") or [])[:20]:
            bad.append({"date": g.get("date"), "field": "gap",
                        "value": g.get("missing"),
                        "reason": "interior missing bars"})
    daily_bad = []
    daily_compared = 0
    bbase = ss.base_interval(interval)
    if bbase != "1d" and kind:
        daily_iv = ss.with_kind("1d", kind)
        try:
            daily = reader(root, ticker, daily_iv)
        except Exception:  # noqa: BLE001
            daily = []
        if daily:
            src = _ratio_daily_close(bars)
            dst = _ratio_daily_close(daily)
            for day in sorted(set(src) & set(dst)):
                daily_compared += 1
                diff = abs(src[day] - dst[day])
                tol = max(0.02, abs(dst[day]) * 0.20)
                if diff > tol:
                    daily_bad.append({"date": day, "field": "daily-close",
                                      "derived": src[day],
                                      "reference": dst[day],
                                      "reason": "1m ratio close differs from 1d"})
            bad.extend(daily_bad[:20])
    return {**base, "checked": len(dates), "flagged_days": sorted({
                b.get("date") for b in bad if b.get("date")}),
            "flagged": bad[:_XVAL_ROW_CAP],
            "gap_missing_total": gap_missing,
            "daily_compared": daily_compared,
            "daily_mismatches": len(daily_bad)}


def _ratio_xval_detail(res):
    flagged_rows = res.get("flagged") or []
    summary = _summarize_xval_rows(flagged_rows)
    rows = [{"date": r.get("date"), "field": r.get("field"),
             "derived": r.get("value", r.get("derived")),
             "reference": r.get("reference"), "percent": None,
             "reason": r.get("reason")}
            for r in flagged_rows[:_XVAL_ROW_CAP]]
    flagged = len(res.get("flagged_days") or [])
    sev = "MEDIUM" if flagged else "LOW"
    if res.get("error"):
        sev = "MEDIUM"
    rec = ("Investigate ratio range/gaps and re-fetch the listed days."
           if flagged or res.get("error")
           else "Ratio series passes structural checks; no price reference exists.")
    return {"severity": sev, "recommendation": rec,
            "flagged_count": flagged, "checked": res.get("checked", 0),
            "matched": res.get("daily_compared", 0),
            "max_percent": None, "rows": rows, "row_cap": _XVAL_ROW_CAP,
            "shown": len(rows),
            "gap_missing_total": res.get("gap_missing_total", 0),
            "daily_mismatches": res.get("daily_mismatches", 0),
            **summary}


def cross_validate_ticker(root, ticker, interval="1m", rng="5Y",
                          ref_fn=None, read_fn=None, asof=None,
                          fingerprint_fn=None, *, _test_capability=None,
                          _evidence_dir=None, _authority=None, _clock=None,
                          _governors=None, _opener=None):
    """One held cross-validation root with an explicit reference child."""
    operation = fops.begin_operation(
        "validation", _validation_directory(_evidence_dir),
        test_capability=_test_capability, authority=_authority,
        clock=_clock, governors=_governors)
    try:
        fops.require_root_admission(operation)
        pending_saves = []
        pending_cache = []
        with fops.scoped_worker(operation.child(
                "cross-validation", rights={"http.stockanalysis.validation"}),
                close=True) as worker:
            result = _cross_validate_ticker_body(
                root, ticker, interval, rng, ref_fn, read_fn,
                asof or operation.context.captured_now.isoformat(),
                fingerprint_fn, _fetch_worker=worker, _opener=_opener,
                _pending_saves=pending_saves,
                _pending_cache=pending_cache)
        operation.seal()
        _publish_reference_cache(pending_cache, operation.context)
        for item in pending_saves:
            save_daily_reference(*item)
        return result
    finally:
        operation.close()


def _cross_validate_ticker_body(root, ticker, interval="1m", rng="5Y",
                                ref_fn=None, read_fn=None, asof=None,
                                fingerprint_fn=None, *, _fetch_worker=None,
                                _opener=None, _pending_saves=None,
                                _pending_cache=None):
    """Cross-check one series and attach fail-closed interval provenance.

    Stable runs keep the existing validated/discrepancy/unavailable/structural
    classifications. If any stored input's manifest state is unreadable or
    changes during the run, the observed result stays in the record but the
    published status becomes non-current `inconclusive`. Full-history requests
    also fail closed when the received reference does not reach the stored
    head within the documented tolerance. Legacy comparison errors become
    verdicts; guarded authority, ledger, cancellation and refusal faults raise.
    """
    interval = str(interval or "").strip()
    started_at = datetime.now().astimezone().isoformat(timespec="seconds")
    asof = asof or started_at
    fetch = (ref_fn if ref_fn is not None else
             lambda symbol, request_range: fetch_daily_reference(
                 symbol, request_range, _fetch_worker=_fetch_worker,
                 _opener=_opener, _pending_cache=_pending_cache))
    fp_fn = fingerprint_fn or ss.interval_state_fingerprint
    before, before_error = _capture_interval_fingerprint(
        fp_fn, root, ticker, interval)
    ratio_kind = ss.kind_of(interval)
    lineage_applicable = ratio_kind not in ss.RATIO_KINDS
    provider = ("internal-structural" if ratio_kind in ss.RATIO_KINDS
                else "stockanalysis")
    reference_interval = None
    reference_before = reference_before_error = None
    if provider == "internal-structural" and ss.base_interval(interval) != "1d":
        reference_interval = ss.with_kind("1d", ratio_kind)
        reference_fp_fn = (
            fingerprint_fn or ss.optional_interval_state_fingerprint)
        reference_before, reference_before_error = _capture_interval_fingerprint(
            reference_fp_fn, root, ticker, reference_interval,
            require_present=False)
    base = {"schema_version": CROSS_VALIDATION_SCHEMA_VERSION,
            "ticker": ticker, "interval": interval, "asof": asof,
            "provider": provider,
            "requested_range": None if provider == "internal-structural" else rng,
            "reference_coverage": None,
            "started_at": started_at, "score": None}

    def finish(outcome, lineage_result=None):
        after, after_error = _capture_interval_fingerprint(
            fp_fn, root, ticker, interval)
        reference_after = reference_after_error = None
        if reference_interval is not None:
            reference_fp_fn = (
                fingerprint_fn or ss.optional_interval_state_fingerprint)
            reference_after, reference_after_error = _capture_interval_fingerprint(
                reference_fp_fn, root, ticker, reference_interval,
                require_present=False)
        finished_at = datetime.now().astimezone().isoformat(timespec="seconds")
        fingerprint = _interval_provenance(
            before, before_error, after, after_error, "selected interval")
        reference_fingerprint = None
        if reference_interval is not None:
            reference_fingerprint = _interval_provenance(
                reference_before, reference_before_error,
                reference_after, reference_after_error,
                "stored daily ratio interval")
        current = (fingerprint["current"]
                   and (reference_fingerprint is None
                        or reference_fingerprint["current"]))
        published = outcome
        lineage_config = (_lineage_boundary_config(root)
                          if lineage_applicable else None)
        decision = _lineage_boundary_policy(
            lineage_config, root, ticker, interval, current)
        if decision.get("status") == "ignored":
            published = dict(outcome)
            lineage_error = _bounded_xval_text(
                decision.get("error", "lineage boundary pin was ignored"))
            published["lineage_boundary_error"] = lineage_error
            prior_note = _bounded_xval_text(outcome.get("note", ""))
            published["note"] = _bounded_xval_text(
                f"{prior_note + '; ' if prior_note else ''}"
                f"LINEAGE PIN IGNORED: {lineage_error}")
        elif decision.get("status") == "applied":
            pin = decision["pin"]
            try:
                published = (
                    _apply_lineage_boundary(outcome, lineage_result, pin)
                    if lineage_result is not None else dict(outcome))
                published.setdefault(
                    "lineage_boundary", _lineage_boundary_public(pin))
            except Exception as exc:  # noqa: BLE001 - suppression must fail open
                published = dict(outcome)
                lineage_error = _bounded_xval_text(
                    "lineage boundary pin was ignored: "
                    f"{type(exc).__name__}: {exc}")
                published["lineage_boundary_error"] = lineage_error
                prior_note = _bounded_xval_text(outcome.get("note", ""))
                published["note"] = _bounded_xval_text(
                    f"{prior_note + '; ' if prior_note else ''}"
                    f"LINEAGE PIN IGNORED: {lineage_error}")
        result = {**published, "finished_at": finished_at,
                  "interval_fingerprint": fingerprint,
                  "evidence_counts": _xval_evidence_counts(
                      published.get("detail"))}
        if reference_fingerprint is not None:
            result.update({
                "reference_interval": reference_interval,
                "reference_interval_fingerprint": reference_fingerprint,
            })
        if not current:
            observed_status = result.get("status", "inconclusive")
            observed_note = _bounded_xval_text(result.get("note", ""))
            diagnostic_fingerprint = (
                fingerprint if not fingerprint["current"]
                else reference_fingerprint)
            diagnostic_fingerprint["observed_status"] = observed_status
            if observed_note:
                diagnostic_fingerprint["observed_note"] = observed_note
            result["status"] = "inconclusive"
            result["score"] = None
            reasons = [item.get("error") for item in (
                fingerprint, reference_fingerprint) if item is not None
                and not item["current"]]
            result["note"] = (
                "non-current validation evidence: "
                + _bounded_xval_text("; ".join(
                    reason or "interval provenance unavailable"
                    for reason in reasons)))
        return result

    if ss.kind_of(interval) in ss.RATIO_KINDS:
        res = ratio_structural_check(root, ticker, interval, read_fn=read_fn)
        detail = _ratio_xval_detail(res)
        if res.get("error"):
            return finish({**base, "status": "structural-flag",
                           "note": (f"{ss.kind_of(interval).upper()}: structural "
                                    f"check inconclusive ({res['error']}); no "
                                    f"external price reference exists"),
                           "severity": detail["severity"], "detail": detail})
        flagged = bool(res.get("flagged_days") or res.get("gap_missing_total")
                       or res.get("daily_mismatches"))
        status = "structural-flag" if flagged else "structural-ok"
        note = (f"{ss.kind_of(interval).upper()}: no external price reference "
                f"(unitless ratio); structural check "
                f"{'flagged' if flagged else 'ok'}")
        return finish({**base, "status": status, "note": note,
                       "severity": detail["severity"], "detail": detail})
    try:
        reference = fetch(ticker, rng)
    except Exception as exc:  # noqa: BLE001 — a dead source = 'unavailable'
        from fetch_authority import ResponseQuarantined
        if isinstance(exc, ResponseQuarantined):
            # Rejected bytes are not evidence that the ticker is absent.
            # Returning a verdict here would replace prior xval state and
            # allow Add Stocks to credit validation debt.
            raise
        if _terminal_reference_error(exc, _fetch_worker):
            raise
        return finish({**base, "status": "unavailable",
                       "note": "online source error: "
                               + _bounded_xval_text(exc)})
    if not reference:                       # 404 / unknown symbol / empty data
        return finish({**base, "status": "unavailable",
                       "note": "not found on the online source"})
    try:
        res = _validate_series_body(
            root, ticker, interval=interval, rng=rng,
            ref_fn=lambda *a, **k: reference, read_fn=read_fn,
            _pending_saves=_pending_saves)
    except Exception as exc:  # noqa: BLE001
        return finish({**base, "status": "inconclusive",
                       "note": "validate raised: " + _bounded_xval_text(exc)})
    status, _sev, note = _classify(res)
    verdict = {"ok": "validated", "split": "discrepancy",
               "discrepancy": "discrepancy", "inconclusive": "inconclusive",
               "error": "inconclusive"}.get(status, "inconclusive")
    # A uniform wrong-scale series scores a deceptive 1.0; suppress that score.
    score = (None if (res.get("suspected_factor") and not res.get("flagged_days"))
             else res.get("score"))
    detail = _xval_detail(res)
    coverage = res.get("reference_coverage")
    outcome = {**base, "status": verdict, "note": note, "score": score,
               "reference_coverage": coverage,
               "severity": detail["severity"], "detail": detail}
    if not res.get("error") and coverage is None:
        outcome.update({
            "status": "inconclusive",
            "score": None,
            "note": "reference received-range metadata is unavailable: "
                    + _bounded_xval_text(
                        res.get("reference_coverage_error") or "unknown error"),
        })
    elif (coverage is not None
          and coverage.get("full_history_requested")
          and not coverage.get("full_history_head_ok")):
        outcome.update({
            "status": "inconclusive",
            "score": None,
            "note": (
                "full-history reference is shorter than stored history: "
                f"received {coverage['reference_first_date']}.."
                f"{coverage['reference_last_date']}, stored "
                f"{coverage['stored_first_date']}.."
                f"{coverage['stored_last_date']} "
                f"(head gap {coverage['head_gap_days']} days; "
                f"limit {coverage['head_tolerance_days']})"),
        })
    return finish(outcome, lineage_result=res)


def load_cross_validation(root):
    """Load cross-validation sidecar JSON from the data bank.

    Current keys are `TICKER interval`; legacy plain `TICKER` keys are accepted
    and exposed unchanged so old banks keep working until the next write for
    that series records the compound key.
    """
    try:
        data = json.loads((Path(root) / _CROSS_FILE).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def record_cross_validation(root, entry):
    """Persist ONE series' verdict into the sidecar JSON. The read-modify-write
    is LOCK-guarded and the file is written ATOMICALLY (temp + os.replace), so
    overlapping validator threads can't lose entries or corrupt the file.
    Best-effort; returns the path written or None."""
    t = str((entry or {}).get("ticker", "")).strip().upper()
    if not t:
        return None
    return record_cross_validation_many(root, [entry])


def record_cross_validation_many(root, entries):
    """Persist a BATCH of verdicts in ONE lock-guarded read-modify-write —
    per-entry semantics identical to sequential record_cross_validation calls
    (compound `TICKER interval` keys, blank tickers skipped, later duplicates win).
    Rationale: the whole ~1.3 MB sidecar is re-parsed + rewritten per write, so
    a 500-ticker run through the single-entry API costs ~25 s CPU and ~0.65 GB
    of disk churn; batching the run's validator flushes (K=10) makes that
    ~2.5 s + ~66 MB. Compact separators for the same reason as save_manifest.
    Best-effort; returns the path written, or None (nothing written)."""
    keyed = [(cross_validation_key((e or {}).get("ticker", ""),
                                   (e or {}).get("interval", "")), e)
             for e in (entries or [])]
    keyed = [(k, e) for k, e in keyed if k]
    if not keyed:
        return None
    target = Path(root) / _CROSS_FILE
    with _CROSS_LOCK:
        data = load_cross_validation(root)
        for k, e in keyed:
            data[k] = e
            if " " in k:
                try:
                    t = ss.canonical_ticker((e or {}).get("ticker"))
                except Exception:  # noqa: BLE001
                    continue
                legacy = data.get(t)
                legacy_iv = str((legacy or {}).get("interval") or "").strip()
                new_iv = str((e or {}).get("interval") or "").strip()
                if (isinstance(legacy, dict)
                        and (not legacy_iv or legacy_iv == new_iv)):
                    data.pop(t, None)
        try:
            Path(root).mkdir(parents=True, exist_ok=True)
            ss._atomic_write_bytes(
                target,
                json.dumps(data, sort_keys=True,
                           separators=(",", ":")).encode("utf-8"))
            return str(target)
        except Exception:  # noqa: BLE001 — persistence is best-effort
            return None


_EARLIEST_FILE = "_ibkr_earliest.json"
_EARLIEST_LOCK = threading.Lock()


def load_ibkr_earliest(root, include_identity=False):
    """{TICKER -> 'YYYY-MM-DD'} earliest demonstrated IBKR-served date.

    Identity-bound on-disk entries have shape
    ``{"earliest": "YYYY-MM-DD", "conid": positive-int}``. The default
    return value projects them back to the historical date-string contract so
    existing GUI and audit consumers remain compatible. ``include_identity``
    returns the raw values for callers deciding cache authority. Robust to a
    missing/corrupt bank sidecar (returns {}).
    """
    try:
        data = json.loads(
            (Path(root) / _EARLIEST_FILE).read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {}
        if include_identity:
            return data
        return {
            key: (value.get("earliest")
                  if isinstance(value, dict)
                  and isinstance(value.get("earliest"), str)
                  else value)
            for key, value in data.items()
        }
    except (OSError, ValueError):
        return {}


def record_ibkr_earliest(root, ticker, date_str, conid=None):
    """Persist ONE ticker's earliest IBKR date (keyed by UPPER-cased ticker).
    LOCK-guarded read-modify-write + ATOMIC replace, so overlapping fetch threads
    can't lose entries. ``conid`` records identity-bound evidence; omitting it
    preserves the legacy string shape and never downgrades a bound entry.
    Skips the write when unchanged. Best-effort; returns the path or None."""
    t = str(ticker or "").strip().upper()
    d = str(date_str or "").strip()
    if not t or not d:
        return None
    bound = None
    if conid is not None:
        try:
            bound = int(conid)
            if isinstance(conid, bool) or bound <= 0 \
                    or date.fromisoformat(d).isoformat() != d:
                return None
        except (TypeError, ValueError):
            return None
    value = ({"earliest": d, "conid": bound} if bound is not None else d)
    target = Path(root) / _EARLIEST_FILE
    with _EARLIEST_LOCK:
        data = load_ibkr_earliest(root, include_identity=True)
        existing = data.get(t)
        if existing == value:                # unchanged — skip the rewrite
            return str(target)
        if (bound is None and isinstance(existing, dict)
                and existing.get("conid") is not None):
            return str(target)                # never erase stronger binding
        data[t] = value
        try:
            Path(root).mkdir(parents=True, exist_ok=True)
            ss._atomic_write_bytes(
                target,
                json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))
            return str(target)
        except Exception:  # noqa: BLE001 — persistence is best-effort
            return None


_NAMES_FILE = "_ibkr_names.json"
_NAMES_LOCK = threading.Lock()


def load_ibkr_names(root):
    """{TICKER -> 'Company Name'} the issuer long name IBKR reports for each
    ticker, from the bank sidecar. Robust to a missing/corrupt file (-> {})."""
    try:
        data = json.loads(
            (Path(root) / _NAMES_FILE).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def record_ibkr_name(root, ticker, name):
    """Persist ONE ticker's IBKR company name (keyed by UPPER-cased ticker).
    LOCK-guarded read-modify-write + ATOMIC replace, so overlapping fetch/Fix-data
    threads can't lose entries. Skips the write when unchanged. Best-effort;
    returns the path or None (empty ticker/name -> None, never stored)."""
    t = str(ticker or "").strip().upper()
    nm = str(name or "").strip()
    if not t or not nm:
        return None
    target = Path(root) / _NAMES_FILE
    with _NAMES_LOCK:
        data = load_ibkr_names(root)
        if data.get(t) == nm:                # unchanged — skip the rewrite
            return str(target)
        data[t] = nm
        try:
            Path(root).mkdir(parents=True, exist_ok=True)
            ss._atomic_write_bytes(
                target,
                json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))
            return str(target)
        except Exception:  # noqa: BLE001 — persistence is best-effort
            return None


_INTERNAL_FILE = "_internal_validation.json"
_INTERNAL_LOCK = threading.Lock()


def load_internal_validation(root):
    """{TICKER -> internal-audit verdict} from the bank sidecar (robust to a
    missing/corrupt file)."""
    try:
        data = json.loads(
            (Path(root) / _INTERNAL_FILE).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def record_internal_validation(root, entry):
    """Persist ONE ticker's internal-daily verdict (lock-guarded, atomic), keyed
    by UPPER ticker. Best-effort; returns the path or None."""
    t = str((entry or {}).get("ticker", "")).strip().upper()
    if not t:
        return None
    target = Path(root) / _INTERNAL_FILE
    with _INTERNAL_LOCK:
        data = load_internal_validation(root)
        data[t] = entry
        try:
            Path(root).mkdir(parents=True, exist_ok=True)
            ss._atomic_write_bytes(
                target,
                json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))
            return str(target)
        except Exception:  # noqa: BLE001 — persistence is best-effort
            return None


# --- ingest-time cross-check gate -------------------------------------------

class CrossCheckGate:
    """Accumulates per-series cross-validation outcomes DURING an ingest run and
    decides when to raise the mid-run alarm: more than `threshold` of the
    validated series 'ring the alarm' (a SIGNIFICANT data discrepancy — split or
    missing-chunk; NOT a ref-fetch error or an inconclusive no-overlap). Pure
    and THREAD-SAFE (internal lock) so concurrent per-port fetch workers can
    record without racing. The GUI owns the actual validate_series calls and the
    popup; this only counts and decides.

    record(entry) -> 'ask' the FIRST time the alarmed fraction strictly exceeds
    `threshold` once at least `min_sample` series have been seen (asks once);
    otherwise None. `entry` carries at least a 'status' key (from _classify);
    'ticker'/'interval'/'note' are kept for the log."""
    ALARM_STATUSES = ("split", "discrepancy")

    def __init__(self, threshold=0.20, min_sample=5):
        import threading as _threading
        self.threshold = threshold
        self.min_sample = min_sample
        self.results = []
        self.total = 0
        self.alarmed = 0
        self.stopped = False           # user chose Stop at the gate
        self.log_path = None           # set when a log is written
        self._asked = False
        self._lock = _threading.Lock()

    def record(self, entry):
        with self._lock:
            self.results.append(entry)
            self.total += 1
            if entry.get("status") in self.ALARM_STATUSES:
                self.alarmed += 1
            if (not self._asked and self.total >= self.min_sample
                    and self.alarmed > self.threshold * self.total):
                self._asked = True
                return "ask"
            return None

    def fraction(self):
        with self._lock:
            return self.alarmed / self.total if self.total else 0.0

    def alarmed_results(self):
        with self._lock:
            return [dict(r) for r in self.results
                    if r.get("status") in self.ALARM_STATUSES]

    def snapshot(self):
        """A consistent point-in-time view for the popup / log (no lock held by
        the caller)."""
        with self._lock:
            alarmed = [dict(r) for r in self.results
                       if r.get("status") in self.ALARM_STATUSES]
            return {"total": self.total, "alarmed": len(alarmed),
                    "fraction": (self.alarmed / self.total if self.total
                                 else 0.0),
                    "threshold": self.threshold,
                    "alarmed_results": alarmed,
                    "results": [dict(r) for r in self.results]}
