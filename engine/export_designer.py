"""Configurable export rendering — the engine core behind the Export Designer.

The Export Designer dialog needs a CONFIGURABLE export — column pick + reorder,
delimiter, date format, timezone, header on/off, empty-value token, plus joined
VWAP / implied-vol (IV) / historical-vol (HVOL) columns, written as CSV / TSV /
Parquet / JSON — and a LIVE SAMPLE pane. Its Bank data source shows real rows;
its Sample fake source deliberately generates preview-only edge cases. Both
sources stay honest about formatting because they render through the SAME
function the real export uses: render_rows(). This module is that single
formatter, plus the readers and joins that build the rows it formats. It is kept
free of tkinter so it is importable in a worker thread AND a headless test.

Real exports and the Bank data preview remain RAW as stored — no resampling, no
rounding (full stored precision is preserved exactly, like the canonical
export), and no fabrication. The synthetic Sample fake rows are never an
export input. A DAILY IV/HVOL value is FORWARD-FILLED onto each intraday row of
its day (a step function that repeats the day's value on every minute, then
steps at the next session), matching backtesting norms; an INTRADAY IV series
exact-joins by timestamp instead. A missing interior month is exported AROUND,
never invented, and reported in `holes`.

Reuses stock_storage for the byte-exact value formats, the atomic writer, the
typed-Parquet codec, and the interval-token grammar; stock_validate for the
series readers. The output format therefore cannot fork from the storage format.
"""

import bisect
import itertools
import os
import threading
import time as _wall
from datetime import date as _date, datetime as _datetime, time as _time, \
    timedelta as _timedelta
from pathlib import Path

import stock_storage as ss
import stock_validate as sv

_TMP_COUNTER = itertools.count()


class ExportCancelled(Exception):
    """Raised when an export cancel signal is observed at a safe boundary."""


def _cancel_requested(cancel):
    if cancel is None:
        return False
    is_set = getattr(cancel, "is_set", None)
    if callable(is_set):
        return bool(is_set())
    return bool(cancel())


def _check_cancel(cancel):
    if _cancel_requested(cancel):
        raise ExportCancelled("export cancelled")


def _emit_progress(progress, ticker, month_index, month_total, month_key):
    if progress is None:
        return
    progress({"ticker": ticker, "month_index": month_index,
              "month_total": month_total, "month": month_key})

# --- the configurable surface -------------------------------------------------

# Columns the user can pick (and reorder). The INTERNAL keys stay short (used
# for row-dict lookups + saved presets); COLUMN_LABEL maps each to the exported
# HEADER / GUI label, so the abbreviations are spelled out for the user. Labels
# stay lowercase_with_underscores to match the canonical 'open,high,low,close,
# volume' header style and stay tool-friendly (no spaces to quote).
ALL_COLUMNS = ("date", "time", "timestamp", "open", "high", "low", "close",
               "volume", "ticker", "vwap", "iv", "hvol")
PRICE_COLS = ("open", "high", "low", "close")
DEFAULT_COLUMNS = ["date", "time", "open", "high", "low", "close", "volume"]
COLUMN_LABEL = {c: c for c in ALL_COLUMNS}
COLUMN_LABEL["iv"] = "implied_volatility"
COLUMN_LABEL["hvol"] = "historical_volatility"
COLUMN_LABEL["vwap"] = "volume_weighted_average_price"

FILE_TYPES = ("csv", "tsv", "parquet", "json")
DATE_FORMATS = ("iso", "epoch", "date-only", "custom")
TIMEZONES = ("America/New_York", "UTC")
EMPTY_MODES = ("blank", "nan", "null", "zero")
_EMPTY_TOKEN = {"blank": "", "nan": "NaN", "null": "null", "zero": "0"}

# The Sample fake source ("All variations" until 2026-07-31) is preview-only:
# it never participates in an export unless the caller explicitly passes its
# returned rows to a renderer. The flag names below are the stable internal
# contract and deliberately keep the original wording.
SAMPLE_VARIATIONS = True
REQUIRED_VARIATIONS = (
    "normal", "day_gap", "minute_gap", "zero_volume",
    "fractional_volume", "empty_row", "empty_value",
    "dst_boundary", "month_boundary", "kind_scale",
)

_EPOCH = _datetime(1970, 1, 1)

# Vectorized CSV/TSV fast path (see the "vectorized fast path" section at the
# bottom): ON by default; flip False to force the classic per-row renderer —
# the byte-equality harness renders both ways and compares sha256.
FAST_RENDER = True
try:
    import numpy as np                   # pyarrow dependency — always present;
except ImportError:                      # missing -> fast path simply disabled
    np = None


def default_spec():
    """A complete spec with sane defaults — the dialog mutates a copy."""
    return {
        # scope
        "base_interval": "1m",
        "sessions": ["rth"],
        "start_date": None,
        "end_date": None,
        # columns (ordered; only the listed ones are emitted)
        "columns": list(DEFAULT_COLUMNS),
        # format
        "file_type": "csv",
        "delimiter": ",",
        "date_format": "iso",
        "date_custom": "%Y-%m-%d %H:%M:%S",
        "timezone": "America/New_York",
        "header": True,
        "empty": "blank",
    }


# --- value formats (shared with the canonical export where possible) ----------

import re as _re

_SCI = _re.compile(r"^(-?)(\d)(?:\.(\d+))?[eE]([+-]\d+)$")


def _expand_sci(s):
    """Positional expansion of repr's scientific form, sign-aware:
    '8.93e-05' -> '0.0000893', '-1.2e3' -> '-1200'. Same digits = same float."""
    m = _SCI.match(s)
    if not m:
        return s
    sign, lead, frac, exp = m.group(1), m.group(2), m.group(3) or "", int(m.group(4))
    digits = lead + frac
    point = 1 + exp
    if point <= 0:
        body = "0." + "0" * (-point) + digits
    elif point >= len(digits):
        body = digits + "0" * (point - len(digits))
    else:
        body = digits[:point] + "." + digits[point:]
    return sign + body


def num_str(v):
    """Shortest decimal that round-trips to the same float64 — no exponent,
    trailing '.0' dropped (28.2 not 28.20, 198 not 198.0). repr() IS the
    shortest round-trip in CPython; a scientific repr is re-laid positionally
    so the file never carries an exponent. Tolerates 0 and negatives (stored
    prices are positive, but a derived/adjusted value need not be)."""
    f = float(v)
    if f != f or f in (float("inf"), float("-inf")):
        return ""
    s = repr(f)
    if "e" in s or "E" in s:
        s = _expand_sci(s)
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _nth_weekday(year, month, weekday, n):
    """The date of the n-th `weekday` (Mon=0..Sun=6) in (year, month)."""
    first = _date(year, month, 1)
    day1 = 1 + ((weekday - first.weekday()) % 7)
    return _date(year, month, day1 + 7 * (n - 1))


def eastern_offset(dt):
    """US Eastern UTC offset (a timedelta, -4h EDT or -5h EST) for a naive
    US/Eastern datetime, using the post-2007 DST rule: EDT from 02:00 on the
    2nd Sunday of March through 02:00 on the 1st Sunday of November, EST
    otherwise. Exact for US-equity data from 2007 on; pre-2007 daily bars
    (which carry no intraday time) may differ only on the handful of weeks the
    older rule disagreed, bounding any epoch/UTC error there to under an hour."""
    y = dt.year
    start = _datetime.combine(_nth_weekday(y, 3, 6, 2), _time(2, 0))
    end = _datetime.combine(_nth_weekday(y, 11, 6, 1), _time(2, 0))
    return _timedelta(hours=-4) if start <= dt < end else _timedelta(hours=-5)


def _fmt_offset(off):
    sign = "+" if off >= _timedelta(0) else "-"
    secs = abs(int(off.total_seconds()))
    return f"{sign}{secs // 3600:02d}:{(secs % 3600) // 60:02d}"


def epoch_seconds(dt):
    """Absolute Unix seconds for a naive-Eastern datetime (tz-independent)."""
    return int((dt - eastern_offset(dt) - _EPOCH).total_seconds())


def shown_datetime(dt, spec):
    """Wall-clock datetime in the selected export timezone, without tzinfo."""
    if spec.get("timezone", "America/New_York") == "UTC":
        return dt - eastern_offset(dt)
    return dt


def format_date(dt, spec):
    shown = shown_datetime(dt, spec)
    return f"{shown.year:04d}-{shown.month:02d}-{shown.day:02d}"


def format_time(dt, spec):
    shown = shown_datetime(dt, spec)
    return f"{shown.hour:02d}:{shown.minute:02d}:{shown.second:02d}"


def format_timestamp(dt, spec):
    """Format one timestamp per the spec's date_format + timezone. ISO carries
    the correct offset suffix; UTC shifts the wall-clock and shows +00:00;
    epoch is the absolute second count (identical under either timezone)."""
    df = spec.get("date_format", "iso")
    if df == "epoch":
        return str(epoch_seconds(dt))
    shown = shown_datetime(dt, spec)
    shown_off = (_timedelta(0) if spec.get("timezone") == "UTC"
                 else eastern_offset(dt))
    if df == "date-only":
        return f"{shown.year:04d}-{shown.month:02d}-{shown.day:02d}"
    if df == "custom":
        try:
            return shown.strftime(spec.get("date_custom") or "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return shown.isoformat()
    return (f"{shown.year:04d}-{shown.month:02d}-{shown.day:02d}"
            f"T{shown.hour:02d}:{shown.minute:02d}:{shown.second:02d}"
            f"{_fmt_offset(shown_off)}")


# --- row building (price rows + joined auxiliary columns) ---------------------

def attach_vwap(rows):
    """Cumulative per-CALENDAR-DAY VWAP, typical=(H+L+C)/3, weighted by volume,
    reset at each new date (extended-hours rows of the same date share the
    accumulation). When cumulative volume is 0 (thin/ratio rows), VWAP falls
    back to the row's typical price — never a divide-by-zero. A row with an
    absent bar (close is None — synthetic empty-row previews) gets vwap None
    and neither disturbs nor resets the day's accumulation."""
    cur_day, pv, vol = None, 0.0, 0.0
    for r in rows:
        if r.get("close") is None:
            r["vwap"] = None
            continue
        d = r["dt"].date()
        if d != cur_day:
            cur_day, pv, vol = d, 0.0, 0.0
        typ = (float(r["high"]) + float(r["low"]) + float(r["close"])) / 3.0
        v = float(r["volume"])
        pv += typ * v
        vol += v
        r["vwap"] = (pv / vol) if vol > 0 else typ


def _kind_lookup(kind_bars, daily):
    """{date or datetime: value} from IV/HVOL bars. The ratio is the bar's
    CLOSE (bar[4]) — for a computed kind the OHLC carry the day's IV/HVOL and
    close is the conventional end-of-day figure."""
    if daily:
        return {b[0].date(): float(b[4]) for b in kind_bars}
    return {b[0]: float(b[4]) for b in kind_bars}


def forward_fill(rows, daily_lookup, field):
    """Step-function join: each row gets the value of the GREATEST stored date
    <= the row's date. Rows before the first stored date get None."""
    if not daily_lookup:
        for r in rows:
            r[field] = None
        return
    keys = sorted(daily_lookup)
    for r in rows:
        i = bisect.bisect_right(keys, r["dt"].date()) - 1
        r[field] = daily_lookup[keys[i]] if i >= 0 else None


def exact_join(rows, intraday_lookup, field):
    """Timestamp-exact join: a row gets a value only if a bar shares its exact
    timestamp; otherwise None."""
    for r in rows:
        r[field] = intraday_lookup.get(r["dt"])


def _month_floor(d):
    return _date(d.year, d.month, 1)


def _read_series_window(root, canon, interval, start_date=None, end_date=None,
                        seed_previous=False):
    """Read one stored series over the export window.

    IV/HVOL joins only need the export span. Daily forward-fill joins also read
    the latest present month before the export month as a seed, so long holes in
    a ratio series still match the old full-read behavior while reading only one
    extra file. This mirrors the export price reader's manifest SHA gate instead
    of loading an entire decade-long ratio series.
    """
    if start_date is None or end_date is None:
        return sv.read_series(root, canon, interval)
    man = ss.load_manifest(Path(root) / canon) or {}
    months = ss.manifest_months(man, interval)
    read_start = _month_floor(start_date)
    if seed_previous:
        start_mk = ss.month_key(start_date.year, start_date.month)
        prior = [mk for mk, ent in months.items()
                 if (mk < start_mk and isinstance(ent, dict)
                     and ent.get("status") == "present")]
        if prior:
            mk = sorted(prior)[-1]
            read_start = _date(int(mk[:4]), int(mk[5:7]), 1)
    lo = _datetime.combine(read_start, _time.min)
    hi = _datetime.combine(end_date, _time.min) + _timedelta(days=1)
    bars = []
    for y, m in _months_in_range(read_start, end_date):
        mk = ss.month_key(y, m)
        if mk not in months:
            continue
        ent = months.get(mk)
        if isinstance(ent, dict) and ent.get("status") == "MISSING":
            continue
        fp = (ss.find_month_file(root, canon, y, m, interval)
              or ss.month_file_path(root, canon, y, m, interval))
        sha = ent.get("sha256") if isinstance(ent, dict) else None
        b, _stats = ss.read_month_file_fast(fp, sha)
        bars.extend(b)
    return [b for b in bars if lo <= b[0] < hi]


def _kind_source_interval(intervals, base_iv, kind):
    same = ss.with_kind(base_iv, kind)
    daily = ss.with_kind("1d", kind)
    if base_iv != "1d" and same in intervals:
        return same
    if daily in intervals:
        return daily
    if same in intervals:
        return same
    return None


def read_kind_series(root, ticker, base_iv, kind, start_date=None, end_date=None,
                     include_source=False):
    """(bars, mode) for the IV/HVOL series joinable onto a `base_iv` price
    series. Prefers an INTRADAY same-base series ('1m-iv') for an exact join;
    falls back to the DAILY series ('1d-iv') for forward-fill. ([], '') when
    none is stored. `kind` is 'iv' or 'hvol'."""
    canon = ss.canonical_ticker(ticker)
    man = ss.load_manifest(Path(root) / canon) or {}
    ivs = man.get("intervals", {})
    source = _kind_source_interval(ivs, base_iv, kind)
    if source is not None:
        mode = "intraday" if base_iv != "1d" and source == ss.with_kind(
            base_iv, kind) else "daily"
        result = _read_series_window(
            root, canon, source, start_date, end_date,
            seed_previous=mode == "daily"), mode
        return (*result, source) if include_source else result
    return ([], "", None) if include_source else ([], "")


SOURCE_JOIN_LABELS = {"intraday": "exact timestamp", "daily": "step fill"}
# Only IV is ever stored intraday. HVOL is daily by construction, so a daily
# HVOL source is its canonical form, not a substitution — flagging it would
# warn on every ordinary minute export.
INTRADAY_KINDS = ("iv",)
# Columns with no stored series at all: derived in memory from the spine at
# export time. Worth stating — nothing is fetched for these, so a reader could
# otherwise assume they arrived from the provider like the price did.
DERIVED_COLUMNS = {
    "vwap": "computed from this spine · per calendar day, resets each session",
}
# Columns the spine itself supplies: its bars' timestamp fields, their stored
# OHLCV values, and the ticker the series belongs to. The spine line names a
# SERIES ("1d"), which is not the same statement as naming the columns that
# series fills, so these are reported too — otherwise OHLCV is the one group in
# an export with no stated origin at all.
#
# Derived by SUBTRACTION on purpose: a column added to ALL_COLUMNS and not
# classified as derived or as a volatility kind lands here rather than
# vanishing from the report, so the three groups always partition ALL_COLUMNS.
SPINE_COLUMNS = tuple(c for c in ALL_COLUMNS
                      if c not in DERIVED_COLUMNS and c not in ("iv", "hvol"))


def _month_key(d):
    return None if d is None else f"{d.year:04d}-{d.month:02d}"


def _months_within(months, lo, hi):
    """Sorted 'YYYY-MM' keys inside an inclusive month window."""
    return sorted(m for m in (months or ())
                  if (lo is None or m >= lo) and (hi is None or m <= hi))


def source_report(root, ticker, base_iv, sessions=("rth",), columns=(),
                  start_date=None, end_date=None):
    """Provenance for one export spec, resolved from the MANIFEST alone.

    Answers what an exported file cannot: which stored series actually feeds
    each volatility column, how it joins onto the price rows, where a kind
    holds months the price spine cannot emit, and where the price spine has no
    matching kind coverage. Rows are built from price bars, so the former
    drops stored values while the latter emits empty volatility cells.

    Never reads bar data, so a GUI may call it on every keystroke. Returns
    ``{}`` when no manifest is readable.
    """
    try:
        canon = ss.canonical_ticker(ticker)
        man = ss.load_manifest(Path(root) / canon) or {}
    except Exception:  # noqa: BLE001 - provenance never gates a preview
        return {}
    ivs = man.get("intervals", {})
    if not ivs:
        return {}
    lo, hi = _month_key(start_date), _month_key(end_date)
    sessions = list(sessions or ("rth",))

    spine_keys = []
    for session in sessions:
        try:
            key = ss.with_session(base_iv, session)
        except Exception:  # noqa: BLE001 - an unknown session names no series
            continue
        if key in ivs and key not in spine_keys:
            spine_keys.append(key)
    spine_months = set()
    for key in spine_keys:
        spine_months.update((ivs.get(key) or {}).get("months") or {})
    spine_in_range = _months_within(spine_months, lo, hi)

    kinds = []
    for kind in ("iv", "hvol"):
        if kind not in columns:
            continue
        requested = ss.with_kind(base_iv, kind)
        source = _kind_source_interval(ivs, base_iv, kind)
        entry = {"kind": kind, "column": COLUMN_LABEL.get(kind, kind),
                 "requested": requested, "source": source, "mode": "",
                 "join": "", "substituted": False, "months_in_range": 0,
                  "unemittable": [], "coverage_missing": [],
                  "months_covered": 0,
                  "spine_months_in_range": len(spine_in_range),
                  "severity": "absent"}
        if source is not None:
            mode = ("intraday" if base_iv != "1d" and source == requested
                    else "daily")
            in_range = _months_within(
                (ivs.get(source) or {}).get("months"), lo, hi)
            unemittable = [m for m in in_range if m not in spine_months]
            coverage_missing = [m for m in spine_in_range
                                if m not in in_range]
            months_covered = len(spine_in_range) - len(coverage_missing)
            substituted = source != requested and kind in INTRADAY_KINDS
            entry.update({
                "mode": mode, "join": SOURCE_JOIN_LABELS[mode],
                "substituted": substituted,
                "months_in_range": len(in_range),
                "unemittable": unemittable,
                "coverage_missing": coverage_missing,
                "months_covered": months_covered,
                "severity": ("mismatch" if unemittable else
                             "coverage" if coverage_missing else
                             "substituted" if substituted else "ok"),
            })
        kinds.append(entry)

    return {
        "ticker": canon,
        "derived": [{"column": COLUMN_LABEL.get(c, c), "how": how}
                    for c, how in DERIVED_COLUMNS.items() if c in columns],
        "spine": {"interval": base_iv, "series": spine_keys,
                  "sessions": sessions, "months": len(spine_months),
                  "months_in_range": len(spine_in_range),
                  "first": spine_in_range[0] if spine_in_range else None,
                  "last": spine_in_range[-1] if spine_in_range else None,
                  "stored": bool(spine_keys),
                  # In the caller's own column order, so the line reads in the
                  # same order as the exported header.
                  "columns": [COLUMN_LABEL.get(c, c) for c in columns
                              if c in SPINE_COLUMNS]},
        "kinds": kinds,
    }


def _aux_lookups(root, canon, base, columns, start_date=None, end_date=None):
    """Read the IV/HVOL join lookups ONCE (daily series are tiny). Returns
    {kind: (lookup, mode)} for each requested ratio column; mode is
    'daily'/'intraday'/'' (none stored)."""
    out = {}
    for kind in ("iv", "hvol"):
        if kind not in columns:
            continue
        if root is None:
            out[kind] = (None, "", None)
            continue
        kb, mode, source = read_kind_series(
            root, canon, base, kind, start_date, end_date,
            include_source=True)
        if mode == "daily":
            out[kind] = (_kind_lookup(kb, True), "daily", source)
        elif mode == "intraday":
            out[kind] = (_kind_lookup(kb, False), "intraday", source)
        else:
            out[kind] = (None, "", None)
    return out


def build_rows_aux(price_bars, canon, columns, aux):
    """Build render-row dicts from price bars using PRECOMPUTED aux lookups
    (so a streaming export joins IV/HVOL without re-reading per chunk). VWAP is
    per-day, so chunking by month — where a day never spans two months — yields
    the same result as building the whole series at once."""
    rows = [{"dt": b[0], "open": b[1], "high": b[2], "low": b[3],
             "close": b[4], "volume": b[5]} for b in price_bars]
    if "ticker" in columns:
        for r in rows:
            r["ticker"] = canon
    if "vwap" in columns:
        attach_vwap(rows)
    for kind in ("iv", "hvol"):
        if kind not in columns:
            continue
        lk, mode = aux.get(kind, (None, ""))[:2]
        if mode == "daily":
            forward_fill(rows, lk, kind)
        elif mode == "intraday":
            exact_join(rows, lk, kind)
        else:
            for r in rows:
                r[kind] = None
    return rows


def build_rows(price_bars, ticker, columns, root=None, base_iv=None):
    """Turn (dt,o,h,l,c,v) price bars into render-row dicts, attaching only the
    auxiliary columns requested. ticker is a constant per row; vwap is computed;
    iv/hvol are read from the bank and joined (forward-fill daily / exact-join
    intraday). Pass root+base_iv to enable the IV/HVOL join."""
    canon = ss.canonical_ticker(ticker)
    base = ss.base_interval(base_iv) if base_iv is not None else None
    if price_bars:
        dates = [b[0].date() for b in price_bars]
        start_date, end_date = min(dates), max(dates)
    else:
        start_date, end_date = None, None
    aux = _aux_lookups(root if base is not None else None, canon, base, columns,
                       start_date, end_date)
    return build_rows_aux(price_bars, canon, columns, aux)


# --- the ONE formatter (sample and export both call this) ---------------------

def _cell(col, r, spec):
    """One text cell for csv/tsv (and the Parquet preview)."""
    if col == "date":
        return format_date(r["dt"], spec)
    if col == "time":
        return format_time(r["dt"], spec)
    if col == "timestamp":
        return format_timestamp(r["dt"], spec)
    if col in PRICE_COLS:
        v = r.get(col)
        if v is None:  # absent bar (empty-row preview): honor the empty mode
            return _EMPTY_TOKEN[spec.get("empty", "blank")]
        return num_str(v)
    if col == "volume":
        v = r.get("volume")
        if v is None:
            return _EMPTY_TOKEN[spec.get("empty", "blank")]
        return str(int(v))
    if col == "ticker":
        return r.get("ticker", "")
    if col in ("vwap", "iv", "hvol"):
        v = r.get(col)
        if v is None:
            return _EMPTY_TOKEN[spec.get("empty", "blank")]
        return num_str(v)
    return ""


def _render_delimited(rows, spec):
    import csv
    import io
    cols = spec.get("columns") or DEFAULT_COLUMNS
    delim = "\t" if spec.get("file_type") == "tsv" else (spec.get("delimiter") or ",")
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=delim, lineterminator="\n")  # always LF
    if spec.get("header", True):
        w.writerow([COLUMN_LABEL[c] for c in cols])
    for r in rows:
        w.writerow([_cell(c, r, spec) for c in cols])
    return buf.getvalue().encode("utf-8")


def _json_empty(em):
    return {"blank": "", "zero": 0, "nan": None, "null": None}[em]


def _json_value(col, r, spec, em):
    if col == "date":
        return format_date(r["dt"], spec)
    if col == "time":
        return format_time(r["dt"], spec)
    if col == "timestamp":
        return epoch_seconds(r["dt"]) if spec.get("date_format") == "epoch" \
            else format_timestamp(r["dt"], spec)
    if col == "volume":
        v = r.get("volume")
        return int(v) if v is not None else _json_empty(em)
    if col == "ticker":
        return r.get("ticker", "")
    v = r.get(col)
    return float(v) if v is not None else _json_empty(em)


def _json_obj(r, spec, em, cols):
    """One row's JSON object string — shared by the in-memory and streaming
    renderers so they can never diverge."""
    import json
    return json.dumps({COLUMN_LABEL[c]: _json_value(c, r, spec, em)
                       for c in cols}, ensure_ascii=False)


def _render_json(rows, spec):
    cols = spec.get("columns") or DEFAULT_COLUMNS
    em = spec.get("empty", "blank")
    objs = [_json_obj(r, spec, em, cols) for r in rows]
    if not objs:
        return b"[]"
    return ("[\n" + ",\n".join(objs) + "\n]").encode("utf-8")


def _num_or_empty(v, em):
    """Parquet numeric cell (aux ratios AND absent-bar prices): None unless
    empty-mode forces a 0."""
    if v is not None:
        return float(v)
    return 0.0 if em == "zero" else None


def _arrow_table(rows, spec, cols=None):
    """A typed Arrow table for `rows` under `spec` — shared by the in-memory
    and streaming Parquet renderers so their schemas/values match exactly."""
    pa, _pq = ss._require_pyarrow()
    cols = cols or spec.get("columns") or DEFAULT_COLUMNS
    em = spec.get("empty", "blank")
    df = spec.get("date_format", "iso")
    data = {}
    for c in cols:
        label = COLUMN_LABEL[c]
        if c == "date":
            data[label] = pa.array(
                [shown_datetime(r["dt"], spec).date() for r in rows],
                pa.date32())
        elif c == "time":
            data[label] = pa.array(
                [shown_datetime(r["dt"], spec).time() for r in rows],
                pa.time32("s"))
        elif c == "timestamp":
            if df == "epoch":
                data[label] = pa.array([epoch_seconds(r["dt"]) for r in rows],
                                       pa.int64())
            elif df in ("date-only", "custom"):
                data[label] = pa.array([format_timestamp(r["dt"], spec)
                                        for r in rows], pa.string())
            else:  # native timestamp[s] in the chosen tz wall-clock (naive)
                if spec.get("timezone") == "UTC":
                    vals = [r["dt"] - eastern_offset(r["dt"]) for r in rows]
                else:
                    vals = [r["dt"] for r in rows]
                data[label] = pa.array(vals, pa.timestamp("s"))
        elif c in PRICE_COLS:
            data[label] = pa.array([_num_or_empty(r.get(c), em) for r in rows],
                                   pa.float64())
        elif c == "volume":
            data[label] = pa.array(
                [int(v) if v is not None else (0 if em == "zero" else None)
                 for v in (r.get("volume") for r in rows)], pa.int64())
        elif c == "ticker":
            data[label] = pa.array([r.get("ticker", "") for r in rows], pa.string())
        elif c in ("vwap", "iv", "hvol"):
            data[label] = pa.array([_num_or_empty(r.get(c), em) for r in rows],
                                   pa.float64())
    return pa.table(data)


def _render_parquet(rows, spec):
    _pa, pq = ss._require_pyarrow()
    import io
    buf = io.BytesIO()
    pq.write_table(_arrow_table(rows, spec), buf, compression="zstd")
    return buf.getvalue()


def render_rows(rows, spec):
    """The single export formatter: render-row dicts -> file bytes per `spec`.
    csv/tsv/json return encoded text (LF line ends); parquet returns the typed
    columnar bytes. Used by BOTH the live sample (first N rows) and the real
    export (all rows) — so the preview is byte-for-byte the file."""
    ft = spec.get("file_type", "csv")
    if ft in ("csv", "tsv"):
        return _render_delimited(rows, spec)
    if ft == "json":
        return _render_json(rows, spec)
    if ft == "parquet":
        return _render_parquet(rows, spec)
    raise ss.StorageError(f"unknown export file type {ft!r}")


def render_sample_text(rows, spec, max_rows=None):
    """Text for the LIVE SAMPLE pane. csv/tsv/json -> the EXACT bytes decoded
    (byte-for-byte what the file holds). parquet -> a tab-aligned table preview
    built from the SAME cells (binary Parquet can't be shown verbatim)."""
    if max_rows is not None:
        rows = rows[:max_rows]
    if spec.get("file_type") == "parquet":
        cols = spec.get("columns") or DEFAULT_COLUMNS
        lines = []
        if spec.get("header", True):
            lines.append("\t".join(COLUMN_LABEL[c] for c in cols))
        for r in rows:
            lines.append("\t".join(_cell(c, r, spec) for c in cols))
        return "\n".join(lines)
    return render_rows(rows, spec).decode("utf-8")


# --- reading from the bank ----------------------------------------------------

def _months_in_range(start_date, end_date):
    if end_date < start_date:
        raise ss.StorageError(
            f"end_date {end_date} is before start_date {start_date}")
    out, (y, m), last = [], (start_date.year, start_date.month), \
        (end_date.year, end_date.month)
    while (y, m) <= last:
        out.append((y, m))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def _collect_session(root, canon, tok, start_date, end_date, manifest=None):
    """Read + day-trim of ONE session token over the window. Returns
    (bars, months_used, holes). Missing interior months land in holes. With
    `manifest` it uses the sha-safe TRUSTED fast read (export hot path); each file
    falls back to the STRICT read on any sha mismatch (accurate first)."""
    if not ss.INTERVAL_RE.match(tok):
        raise ss.StorageError(f"bad interval token {tok!r}")
    months = ((manifest or {}).get("intervals", {}).get(tok, {}).get("months", {}))
    bars, months_used, holes = [], [], []
    for (y, m) in _months_in_range(start_date, end_date):
        p = ss.find_month_file(root, canon, y, m, tok)
        key = ss.month_key(y, m)
        if p is None:
            holes.append(key)
            continue
        sha = (months.get(key) or {}).get("sha256")
        mb, _stats = ss.read_month_file_fast(p, sha)   # fast on sha-match, else strict
        bars.extend(mb)
        months_used.append(key)
    lo = _datetime.combine(start_date, _time.min)
    hi = _datetime.combine(end_date, _time.min) + _timedelta(days=1)
    bars = [b for b in bars if lo <= b[0] < hi]
    return bars, months_used, holes


def _dedupe_sorted(bars):
    bars.sort(key=lambda b: b[0])
    out, prev = [], None
    for b in bars:
        if prev is not None and b[0] == prev:
            continue
        out.append(b)
        prev = b[0]
    return out


def read_merged_bars(root, ticker, base_iv, sessions, start_date, end_date):
    """Merge the requested sessions of `base_iv` over [start_date, end_date]
    into one timestamp-sorted, deduped bar list. Returns (bars, holes); holes
    are session-tagged ('pre:2024-03', or 'pre:not-archived' when that session
    was never stored)."""
    canon = ss.canonical_ticker(ticker)
    base = ss.base_interval(base_iv)
    man = ss.load_manifest(Path(root) / canon) or {}   # one read -> per-month shas
    bars, holes = [], []
    for sess in sessions:
        tok = ss.with_session(base, sess)
        sb, mu, hl = _collect_session(root, canon, tok, start_date, end_date,
                                      manifest=man)
        bars.extend(sb)
        if mu:
            holes.extend(f"{sess}:{h}" for h in hl)
        elif hl:
            holes.append(f"{sess}:not-archived")
    return _dedupe_sorted(bars), holes


def _first_bars(root, ticker, base, sessions, start_date, end_date, n):
    """Cheap first-n merged, IN-RANGE bars for the live sample. Trims to the
    day-window INSIDE the loop BEFORE the early-stop test, so when the earliest
    month holds only out-of-window bars (a mid-month preset start), the reader
    keeps going to the month that actually holds in-range data — the sample then
    matches the file instead of falsely showing 'no rows'."""
    canon = ss.canonical_ticker(ticker)
    lo = _datetime.combine(start_date, _time.min)
    hi = _datetime.combine(end_date, _time.min) + _timedelta(days=1)
    collected = []
    for (y, m) in _months_in_range(start_date, end_date):
        any_here = False
        for sess in sessions:
            tok = ss.with_session(base, sess)
            if not ss.INTERVAL_RE.match(tok):
                continue
            p = ss.find_month_file(root, canon, y, m, tok)
            if p is None:
                continue
            mb, _stats = ss.read_month_file(p)
            collected.extend(mb)
            any_here = True
        if any_here:
            collected = [b for b in _dedupe_sorted(collected) if lo <= b[0] < hi]
            if len(collected) >= n:
                break
    return collected[:n]


def sample_price_bars(root, ticker, base_iv, sessions, start_date, end_date, n=12):
    """Public first-n in-range price bars for the live sample — the GUI caches
    these per scope so a format-only change never re-reads the disk."""
    return _first_bars(root, ticker, ss.base_interval(base_iv), sessions,
                       start_date, end_date, n)


def synthetic_sample_rows(spec):
    """Return deterministic preview-only render rows and parallel tag sets.

    The rows exercise the Export Designer formatter's edge cases and time
    boundaries without consulting the bank while staying visually plausible:
    one ticker, one price neighbourhood, no cartoon outliers. Two empty
    shapes are always present — ``empty_row`` keeps its timestamp but has no
    bar at all (every numeric cell renders the spec's empty mode), and
    ``empty_value`` is a complete bar missing exactly one cell (the first
    selected aux column, else volume, so it is visible even with the default
    OHLCV columns). Under a ratio-kind spec every bar is rescaled to vol
    magnitude so the series looks like one real vol series. Row shape matches
    :func:`build_rows`: every row has ``dt`` + OHLCV, while optional
    ticker/VWAP/volatility keys are present only when selected by ``spec``.
    """
    cases = (
        # dt, OHLC (None = absent bar), volume, variation tags
        (_datetime(2024, 3, 7, 15, 58),
         (187.21, 188.03, 186.95, 187.82), 1200.0, ("normal",)),
        (_datetime(2024, 3, 7, 15, 59),
         (187.82, 188.10, 187.55, 187.96), 980.0,
         ("day_gap", "dst_boundary")),
        (_datetime(2024, 3, 11, 9, 30),
         (188.40, 189.15, 188.02, 188.91), 2100.0,
         ("day_gap", "minute_gap", "dst_boundary")),
        (_datetime(2024, 3, 11, 9, 35),
         (188.91, 189.02, 188.60, 188.74), 840.0, ("minute_gap",)),
        (_datetime(2024, 3, 11, 9, 36),
         (188.74, 188.91, 188.51, 188.68), 0.0, ("zero_volume",)),
        (_datetime(2024, 3, 11, 9, 37),
         (188.68, 188.86, 188.41, 188.55), 760.0, ("empty_value",)),
        (_datetime(2024, 3, 11, 9, 38),
         (188.55, 188.79, 188.32, 188.64), 1234.75,
         ("fractional_volume",)),
        (_datetime(2024, 3, 11, 9, 39), None, None, ("empty_row",)),
        (_datetime(2024, 3, 29, 15, 59),
         (191.10, 191.44, 190.92, 191.31), 1500.0,
         ("month_boundary",)),
        (_datetime(2024, 4, 1, 9, 30),
         (192.05, 192.66, 191.82, 192.40), 2300.0,
         ("month_boundary",)),
    )
    columns = tuple(spec.get("columns") or DEFAULT_COLUMNS)
    ratio_kind = ss.kind_of(str(spec.get("base_interval") or "1m")) \
        in ss.RATIO_KINDS
    rows, tags = [], []
    for idx, (dt, prices, volume, case_tags) in enumerate(cases):
        row_tags = set(case_tags)
        if ratio_kind and prices is not None:
            # The WHOLE series moves to vol magnitude (~0.34) so a ratio-kind
            # preview looks like one real vol series, not mixed scales.
            prices = tuple(round(p / 550.0, 4) for p in prices)
            if idx == 0:
                row_tags.add("kind_scale")
        absent = prices is None
        row = {
            "dt": dt,
            "open": None if absent else prices[0],
            "high": None if absent else prices[1],
            "low": None if absent else prices[2],
            "close": None if absent else prices[3],
            "volume": volume,
        }
        if "ticker" in columns:
            row["ticker"] = "SAMPLE"
        if "iv" in columns:
            row["iv"] = None if absent else round(0.21 + idx * 0.003, 6)
        if "hvol" in columns:
            row["hvol"] = None if absent else round(0.18 + idx * 0.0025, 6)
        rows.append(row)
        tags.append(frozenset(row_tags))

    if "vwap" in columns:
        attach_vwap(rows)

    # The empty_value row stays a COMPLETE bar missing exactly one cell — the
    # first selected auxiliary column, else volume — so the chosen empty mode
    # is always visible even with the default OHLCV column set; surrounding
    # rows retain values so the hole is unmistakable.
    # Honour the user's displayed column order.  ALL_COLUMNS currently puts
    # vwap before iv/hvol, and custom specs may order the aux fields
    # differently; a hard-coded aux priority would make a different cell the
    # hole than the first selected auxiliary column promised by the preview.
    empty_col = next(
        (c for c in columns if c in ("iv", "hvol", "vwap")),
        "volume")
    empty_idx = next(i for i, row_tags in enumerate(tags)
                     if "empty_value" in row_tags)
    rows[empty_idx][empty_col] = None

    return rows, tags


def sample_rows(root, ticker, spec, n=20):
    """Build the first ~n render-rows of `ticker` for the LIVE SAMPLE — reads
    only the earliest in-range month(s), then joins aux columns. Cheap enough
    to call on a scope change (NOT on every format keystroke — cache these)."""
    base = ss.base_interval(spec.get("base_interval", "1m"))
    sessions = spec.get("sessions") or ["rth"]
    bars = _first_bars(root, ticker, base, sessions,
                       spec["start_date"], spec["end_date"], n)
    cols = spec.get("columns") or DEFAULT_COLUMNS
    return build_rows(bars[:n], ticker, cols, root=root, base_iv=base)


# --- scope (ticker picker + missing-from-bank) --------------------------------

def stored_tickers(root):
    """Sorted canonical tickers that have ANY stored series in the bank."""
    return sorted({
        t for t, _iv in sv.discover_series(
            root, rth_only=False, kinds=None)
    })


def stored_intervals(root, ticker):
    """Sorted interval tokens stored for one ticker (e.g. '1m','1m-pre',
    '1d-iv')."""
    canon = ss.canonical_ticker(ticker)
    man = ss.load_manifest(Path(root) / canon) or {}
    return sorted(man.get("intervals", {}))


def missing_from_bank(root, selected):
    """Split `selected` (raw symbols) into (present, missing) CANONICAL tickers
    vs what the bank actually stores. Order-preserving, deduped. A symbol that
    can't even be canonicalized counts as missing (kept verbatim)."""
    have = set(stored_tickers(root))
    present, missing, seen = [], [], set()
    for s in selected:
        try:
            canon = ss.canonical_ticker(s)
        except ss.StorageError:
            if s not in seen:
                missing.append(s)
                seen.add(s)
            continue
        if canon in seen:
            continue
        seen.add(canon)
        (present if canon in have else missing).append(canon)
    return present, missing


# --- pre-flight estimate (manifests only — no bar reads) ----------------------

def _bytes_per_row(spec):
    ft = spec.get("file_type", "csv")
    cols = spec.get("columns") or DEFAULT_COLUMNS
    if ft == "parquet":
        return max(4.0, len(cols) * 3.5)        # zstd columnar, rough
    sample = {"dt": _datetime(2024, 1, 2, 9, 30, 0), "open": 187.21,
              "high": 187.45, "low": 187.05, "close": 187.33,
              "volume": 1234567, "ticker": "AAPL", "vwap": 187.3,
              "iv": 0.2834, "hvol": 0.1912}
    nohdr = dict(spec)
    nohdr["header"] = False
    txt = render_sample_text([sample], nohdr)
    return float(len((txt if ft == "json" else txt + "\n").encode("utf-8")))


def _span_from_manifest(man, base, sessions):
    """(earliest_date, latest_date) from an already-loaded manifest — 1st of
    the earliest stored month .. last day of the latest, over the requested
    sessions. (None, None) when nothing stored. Month-key granularity."""
    import calendar
    ivs = (man or {}).get("intervals", {})
    keys = []
    for sess in sessions:
        tok = ss.with_session(base, sess)
        keys += list(((ivs.get(tok) or {}).get("months") or {}).keys())
    if not keys:
        return None, None
    keys.sort()
    ey, em = int(keys[0][:4]), int(keys[0][5:7])
    ly, lm = int(keys[-1][:4]), int(keys[-1][5:7])
    return _date(ey, em, 1), _date(ly, lm, calendar.monthrange(ly, lm)[1])


def ticker_span(root, ticker, base_iv, sessions):
    """(earliest_date, latest_date) across the stored months of the requested
    sessions. Cheap (one manifest read). (None, None) when nothing stored."""
    canon = ss.canonical_ticker(ticker)
    man = ss.load_manifest(Path(root) / canon) or {}
    return _span_from_manifest(man, ss.base_interval(base_iv), sessions)


def resolve_range(root, ticker, base_iv, sessions, preset):
    """A named export_csv preset -> (start_date, end_date) for ONE ticker,
    anchored to that ticker's LATEST stored day (so '2yr' = the last two years
    of stored data, not of the wall clock). (None, None) when nothing stored."""
    import export_csv as ec
    e, l = ticker_span(root, ticker, base_iv, sessions)
    if e is None:
        return None, None
    return ec.preset_range(preset, l, e, l)


def estimate_bytes(rows, files, spec):
    """Approximate output bytes for `rows` rows over `files` files under `spec`
    — pure arithmetic (no disk), so the GUI can recompute size on a format-only
    change without re-walking manifests."""
    bpr = _bytes_per_row(spec)
    hdr = 0
    if spec.get("header", True) and spec.get("file_type") in ("csv", "tsv"):
        delim = "\t" if spec.get("file_type") == "tsv" else \
            (spec.get("delimiter") or ",")
        hdr = len(delim.join(COLUMN_LABEL[c]
                             for c in (spec.get("columns") or DEFAULT_COLUMNS)))
    return int(rows * bpr) + files * hdr


def estimate(root, tickers, base_iv, sessions, start_date, end_date, spec,
             preset=None):
    """Pre-flight (files, rows, bytes) from MANIFEST row counts only — no bar
    reads, ONE manifest read per ticker. With `preset`, the window is resolved
    PER TICKER (start/end ignored); otherwise the fixed window is used (a None
    fixed window contributes nothing rather than crashing). Slightly OVER-counts
    (edge months aren't day-trimmed), the safe direction for a size estimate."""
    base = ss.base_interval(base_iv)
    total_rows, files = 0, 0
    for t in tickers:
        try:
            canon = ss.canonical_ticker(t)
        except ss.StorageError:
            continue
        man = ss.load_manifest(Path(root) / canon) or {}
        if preset:
            e, l = _span_from_manifest(man, base, sessions)
            if e is None:
                continue
            import export_csv as ec
            sd, ed_ = ec.preset_range(preset, l, e, l)
        else:
            sd, ed_ = start_date, end_date
            if sd is None or ed_ is None:
                continue
        lo = ss.month_key(sd.year, sd.month)
        hi = ss.month_key(ed_.year, ed_.month)
        ivs = man.get("intervals", {})
        tr = 0
        for sess in sessions:
            tok = ss.with_session(base, sess)
            for mk, e2 in ((ivs.get(tok) or {}).get("months", {})).items():
                if lo <= mk <= hi and isinstance(e2, dict):
                    tr += int(e2.get("rows", 0) or 0)
        if tr > 0:
            files += 1
            total_rows += tr
    return {"files": files, "rows": total_rows,
            "bytes": estimate_bytes(total_rows, files, spec)}


# --- the top-level export (worker side, GUI-free) -----------------------------

def _holes_from(found, missing):
    """Session-tagged holes: 'sess:YYYY-MM' for an interior missing month of a
    stored session, 'sess:not-archived' for a session never stored at all."""
    holes = []
    for sess in found:
        if found[sess]:
            holes.extend(f"{sess}:{k}" for k in missing[sess])
        elif missing[sess]:
            holes.append(f"{sess}:not-archived")
    return holes


def _source_intervals_from_run(base, found, aux=None, aux_written=None):
    selected = [ss.with_session(base, session)
                for session, months in found.items() if months]
    written = (set(aux or {}) if aux_written is None else set(aux_written))
    for kind, value in (aux or {}).items():
        if (kind in written and len(value) >= 3 and value[0]
                and value[2] is not None):
            selected.append(value[2])
    return sorted(set(selected))


def _mark_aux_rows_written(aux_written, rows, columns):
    for kind in ("iv", "hvol"):
        if (kind in columns and kind not in aux_written
                and any(row.get(kind) is not None for row in rows)):
            aux_written.add(kind)


def _mark_aux_timestamps_written(aux_written, aux, columns, timestamps):
    """Track real ratio values emitted by vectorized renderers.

    Empty-mode placeholders do not count as ratio evidence.
    """
    stamps = None
    for kind in ("iv", "hvol"):
        if kind not in columns or kind in aux_written:
            continue
        lookup, mode = aux.get(kind, (None, ""))[:2]
        if not lookup:
            continue
        if stamps is None:
            stamps = list(timestamps)
        if mode == "daily":
            first = min(lookup)
            if any(stamp.date() >= first for stamp in stamps):
                aux_written.add(kind)
        elif mode == "intraday" and any(
                lookup.get(stamp) is not None for stamp in stamps):
            aux_written.add(kind)


def _atomic_replace(tmp, target):
    """os.replace(tmp, target) with the same locked-file backoff the storage
    writer uses (Excel/AV hold the target open). Leaves `target` untouched if
    every attempt fails (raises), never a torn file."""
    delay = ss._REPLACE_BACKOFF_S
    for attempt in range(ss._REPLACE_ATTEMPTS):
        try:
            os.replace(tmp, target)
            return
        except PermissionError:
            if attempt == ss._REPLACE_ATTEMPTS - 1:
                raise ss.StorageError(
                    f"{Path(target).name} is locked (open in Excel or being "
                    f"scanned?) — close it and retry") from None
            _wall.sleep(delay)
            delay *= 2


def _write_storage_csv_rows(fh, bars):
    if not bars:
        return
    payload = ss.EOL.join(ss.format_bar(*b) for b in bars) + ss.EOL
    fh.write(payload.encode("ascii"))


def _export_one_storage(root, ticker, spec, out_path, progress=None,
                        cancel=None):
    """Streaming replacement for legacy export_csv CSV output.

    Emits the canonical storage CSV bytes while using month streaming and
    SHA-gated fast reads. This mode is intentionally CSV-only; Parquet byte
    identity would require the old single-table writer.
    """
    if spec.get("file_type", "csv") != "csv":
        raise ss.StorageError("storage_format export is byte-proven for csv only")
    canon = ss.canonical_ticker(ticker)
    base = ss.base_interval(spec.get("base_interval", "1m"))
    sessions = spec.get("sessions") or ["rth"]
    sd, ed_ = spec["start_date"], spec["end_date"]
    lo = _datetime.combine(sd, _time.min)
    hi = _datetime.combine(ed_, _time.min) + _timedelta(days=1)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(
        f"{out_path.name}.{os.getpid()}-{threading.get_ident()}-"
        f"{next(_TMP_COUNTER)}.tmp")

    found = {s: [] for s in sessions}
    missing = {s: [] for s in sessions}
    per_session = {s: 0 for s in sessions}
    months_used = set()
    total, first_dt, last_dt = 0, None, None
    man_iv = (ss.load_manifest(Path(root) / canon) or {}).get("intervals", {})
    months = _months_in_range(sd, ed_)
    try:
        with open(tmp, "wb") as fh:
            fh.write((ss.HEADER + ss.EOL).encode("ascii"))
            for month_i, (y, m) in enumerate(months, 1):
                key = ss.month_key(y, m)
                _check_cancel(cancel)
                _emit_progress(progress, canon, month_i, len(months), key)
                mbars = []
                chunks = []
                for sess in sessions:
                    tok = ss.with_session(base, sess)
                    if not ss.INTERVAL_RE.match(tok):
                        continue
                    p = ss.find_month_file(root, canon, y, m, tok)
                    if p is None:
                        missing[sess].append(key)
                        continue
                    sha = ((man_iv.get(tok, {}).get("months", {}).get(key)
                            or {}).get("sha256"))
                    found[sess].append(key)
                    months_used.add(key)
                    if np is not None:
                        ts, o, h, l, c, v = _read_month_columns_fast(p, sha)
                        i0 = np.searchsorted(ts, np.datetime64(lo, "s"),
                                             side="left")
                        i1 = np.searchsorted(ts, np.datetime64(hi, "s"),
                                             side="left")
                        per_session[sess] = per_session.get(sess, 0) + max(
                            0, i1 - i0)
                        if i1 > i0:
                            chunks.append((ts[i0:i1], o[i0:i1], h[i0:i1],
                                           l[i0:i1], c[i0:i1], v[i0:i1]))
                    else:
                        sb, _stats = ss.read_month_file_fast(p, sha)
                        sb = [b for b in sb if lo <= b[0] < hi]
                        per_session[sess] = per_session.get(sess, 0) + len(sb)
                        mbars.extend(sb)
                if np is not None:
                    if not chunks:
                        continue
                    ts, o, h, l, c, v = _merge_sessions_fast(chunks)
                    if first_dt is None:
                        first_dt = ts[0].astype(_datetime)
                    last_dt = ts[-1].astype(_datetime)
                    total += len(ts)
                    fh.write(_render_storage_body_fast(ts, o, h, l, c, v))
                else:
                    if not mbars:
                        continue
                    mbars = _dedupe_sorted(mbars)
                    if first_dt is None:
                        first_dt = mbars[0][0]
                    last_dt = mbars[-1][0]
                    total += len(mbars)
                    _write_storage_csv_rows(fh, mbars)
            _check_cancel(cancel)
            fh.flush()
            os.fsync(fh.fileno())
        nbytes = tmp.stat().st_size
        _atomic_replace(tmp, out_path)
        cs = ce = None
        if first_dt is not None:
            cs = f"{ss.format_date(first_dt)} {ss.format_time(first_dt.time())}"
            ce = f"{ss.format_date(last_dt)} {ss.format_time(last_dt.time())}"
        return {"ticker": canon, "rows": total, "covered_start": cs,
                "covered_end": ce, "holes": _holes_from(found, missing),
                "months_used": sorted(months_used),
                "per_session": per_session, "bytes": nbytes,
                "out_path": str(out_path),
                "source_intervals": _source_intervals_from_run(
                    base, found)}
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _resolve_export_dates(root, canon, spec):
    if spec.get("start_date") is not None and spec.get("end_date") is not None:
        return spec
    base = ss.base_interval(spec.get("base_interval", "1m"))
    sessions = spec.get("sessions") or ["rth"]
    earliest, latest = ticker_span(root, canon, base, sessions)
    if earliest is None:
        raise ss.StorageError(f"no stored {base} data for {canon} to export")
    out = dict(spec)
    if out.get("start_date") is None:
        out["start_date"] = earliest
    if out.get("end_date") is None:
        out["end_date"] = latest
    return out


def export_one(root, ticker, spec, out_path, progress=None, cancel=None):
    """STREAM one ticker per `spec` to `out_path`, month-by-month, so peak
    memory is bounded by a single month (~thousands of bars) rather than the
    whole multi-year series — the export reads, builds, and renders one month
    at a time and appends to a same-dir temp, then atomically replaces. The
    streamed bytes are IDENTICAL to render_rows() over the full series (months
    are chronological and non-overlapping; a day never spans two months, so the
    per-day VWAP and the forward-filled IV/HVOL match the whole-series build).

    A ticker that resolves to ZERO in-range rows writes NOTHING (no header-only
    stub) and leaves any existing file untouched. Returns {ticker, rows,
    covered_start, covered_end, holes, bytes, out_path, skipped?}.

    CSV/TSV specs that pass the strict _fast_path_ok gate render through the
    VECTORIZED body renderer (measured 4-14x end-to-end, sha256 byte-identical
    across the verification matrix); everything else — parquet, json, custom
    date formats, exotic delimiters — takes the classic per-row path below."""
    canon = ss.canonical_ticker(ticker)
    _numstr_cache.clear()
    spec = _resolve_export_dates(root, canon, spec)
    _check_cancel(cancel)
    if spec.get("storage_format"):
        return _export_one_storage(root, ticker, spec, out_path,
                                   progress=progress, cancel=cancel)
    if FAST_RENDER and np is not None and _fast_path_ok(spec, canon):
        return _export_one_fast(root, ticker, spec, out_path,
                                progress=progress, cancel=cancel)
    if FAST_RENDER and np is not None and _parquet_fast_ok(spec, canon):
        return _export_one_parquet_fast(root, ticker, spec, out_path,
                                        progress=progress, cancel=cancel)
    base = ss.base_interval(spec.get("base_interval", "1m"))
    sessions = spec.get("sessions") or ["rth"]
    cols = spec.get("columns") or DEFAULT_COLUMNS
    em = spec.get("empty", "blank")
    ft = spec.get("file_type", "csv")
    sd, ed_ = spec["start_date"], spec["end_date"]
    lo = _datetime.combine(sd, _time.min)
    hi = _datetime.combine(ed_, _time.min) + _timedelta(days=1)
    aux = _aux_lookups(root, canon, base, cols, sd, ed_)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(
        f"{out_path.name}.{os.getpid()}-{threading.get_ident()}-"
        f"{next(_TMP_COUNTER)}.tmp")

    found = {s: [] for s in sessions}
    missing = {s: [] for s in sessions}
    aux_written = set()
    total, first_dt, last_dt = 0, None, None
    fh = None
    pqw = None
    json_started = False
    body_spec = dict(spec, header=False)
    try:
        if ft in ("csv", "tsv", "json"):
            fh = open(tmp, "wb")
            if ft in ("csv", "tsv") and spec.get("header", True):
                fh.write(render_rows([], dict(spec, header=True)))   # header line
        man_iv = (ss.load_manifest(Path(root) / canon) or {}).get("intervals", {})
        months = _months_in_range(sd, ed_)
        for month_i, (y, m) in enumerate(months, 1):
            key = ss.month_key(y, m)
            _check_cancel(cancel)
            _emit_progress(progress, canon, month_i, len(months), key)
            mbars, any_here = [], False
            for sess in sessions:
                tok = ss.with_session(base, sess)
                if not ss.INTERVAL_RE.match(tok):
                    continue
                p = ss.find_month_file(root, canon, y, m, tok)
                if p is None:
                    missing[sess].append(key)
                    continue
                sha = ((man_iv.get(tok, {}).get("months", {}).get(key) or {})
                       .get("sha256"))                   # trusted fast read when the
                mb, _stats = ss.read_month_file_fast(p, sha)   # sha matches, else strict
                mbars.extend(mb)
                found[sess].append(key)
                any_here = True
            if not any_here:
                continue
            mbars = [b for b in _dedupe_sorted(mbars) if lo <= b[0] < hi]
            if not mbars:
                continue
            if first_dt is None:
                first_dt = mbars[0][0]
            last_dt = mbars[-1][0]
            rows = build_rows_aux(mbars, canon, cols, aux)
            _mark_aux_rows_written(aux_written, rows, cols)
            total += len(rows)
            if ft in ("csv", "tsv"):
                fh.write(render_rows(rows, body_spec))
            elif ft == "json":
                for r in rows:
                    fh.write(b"[\n" if not json_started else b",\n")
                    json_started = True
                    fh.write(_json_obj(r, spec, em, cols).encode("utf-8"))
            else:  # parquet — one row group per month
                _pa, pq = ss._require_pyarrow()
                tbl = _arrow_table(rows, spec, cols)
                if pqw is None:
                    pqw = pq.ParquetWriter(str(tmp), tbl.schema,
                                           compression="zstd")
                pqw.write_table(tbl)
        # finalize the container
        _check_cancel(cancel)
        if ft == "json":
            fh.write(b"\n]" if json_started else b"[]")
        if fh is not None:
            fh.flush()
            os.fsync(fh.fileno())
            fh.close()
            fh = None
        if pqw is not None:
            pqw.close()
            pqw = None

        holes = _holes_from(found, missing)
        if total == 0:
            # nothing to export — never clobber an existing file with a stub
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass
            return {"ticker": canon, "rows": 0, "covered_start": None,
                    "covered_end": None, "holes": holes, "bytes": 0,
                    "out_path": None, "skipped": "no rows in range"}
        nbytes = tmp.stat().st_size
        _atomic_replace(tmp, out_path)
        cs = f"{ss.format_date(first_dt)} {ss.format_time(first_dt.time())}"
        ce = f"{ss.format_date(last_dt)} {ss.format_time(last_dt.time())}"
        return {"ticker": canon, "rows": total, "covered_start": cs,
                "covered_end": ce, "holes": holes, "bytes": nbytes,
                "out_path": str(out_path),
                "source_intervals": _source_intervals_from_run(
                    base, found, aux, aux_written)}
    finally:
        if fh is not None:
            try:
                fh.close()
            except Exception:  # noqa: BLE001
                pass
        if pqw is not None:
            try:
                pqw.close()
            except Exception:  # noqa: BLE001
                pass
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def filename_for(ticker, spec):
    """The default per-ticker filename: {TICKER}_{interval}.{ext} — interval
    included so 1m/5m exports never collide; ext from the file type (tsv -> tsv,
    parquet -> parquet, json -> json, csv -> csv)."""
    ext = {"csv": "csv", "tsv": "tsv", "parquet": "parquet", "json": "json"}[
        spec.get("file_type", "csv")]
    return f"{ss.canonical_ticker(ticker)}_{spec.get('base_interval', '1m')}.{ext}"


# --- vectorized fast path ------------------------------------------------------
#
# A deep export used to spend ~all its wall time in the per-row Python of
# build_rows_aux + render_rows (~43 s for a 1.5M-row ticker). This section is a
# byte-exact vectorized CSV/TSV body renderer (measured 4-14x end-to-end,
# sha256-identical across the verification matrix incl. every empty-token mode,
# epoch/date-only/UTC stamps, IV/HVOL forward-fill and 1d exports). Accurate
# first, guaranteed three ways:
#   * _fast_path_ok is a STRICT precondition gate decided from the spec alone —
#     csv/tsv only, known columns, iso/epoch/date-only stamps, a 1-char
#     delimiter that provably cannot appear in any cell (so csv QUOTE_MINIMAL
#     could never have fired). ANYTHING else falls to the classic renderer.
#   * every float cell is formatted by the REAL num_str, called once per
#     DISTINCT float bit-pattern (np.unique over the int64 view keeps -0.0/NaN
#     distinct) — the text per value is byte-identical BY CONSTRUCTION.
#   * timestamps use the SAME _nth_weekday DST rule; VWAP and the IV/HVOL
#     forward-fill replicate the exact operation order of the row builders
#     (np.cumsum is sequential like the Python loop; searchsorted(side='right')
#     == bisect_right).
# Month bytes are trusted only under the same manifest-sha gate as
# read_month_file_fast; any drift falls back to the STRICT per-bar read.

_FAST_CELL_CHARS = set("0123456789.-+:TNanul")   # every char a fast cell can hold


def _fast_path_ok(spec, canon):
    """True only when the vectorized renderer is PROVABLY byte-equivalent."""
    if spec.get("file_type", "csv") not in ("csv", "tsv"):
        return False
    if spec.get("date_format", "iso") not in ("iso", "epoch", "date-only"):
        return False                    # 'custom' strftime -> classic renderer
    cols = spec.get("columns") or DEFAULT_COLUMNS
    if not all(c in ALL_COLUMNS for c in cols):
        return False
    delim = "\t" if spec.get("file_type") == "tsv" else \
        (spec.get("delimiter") or ",")
    if len(delim) != 1:
        return False
    cell_chars = _FAST_CELL_CHARS | set(canon)
    # csv.writer QUOTE_MINIMAL quotes a field containing delim, '"', \r or \n.
    # No fast-path cell can contain \r/\n; forbid a delimiter that collides
    # with any possible cell character -> quoting can never fire. A delimiter
    # EQUAL to the quotechar ('"') is arguably still join-equal, but exotic —
    # rejected to keep the equivalence proof trivial.
    if delim == '"' or delim in cell_chars:
        return False
    return True


def _parquet_fast_ok(spec, canon):
    """True for Parquet specs whose typed Arrow output is vector-proven.

    Custom strftime stays on the classic path because it is inherently per-row.
    """
    if spec.get("file_type", "csv") != "parquet":
        return False
    if spec.get("date_format", "iso") not in ("iso", "epoch", "date-only"):
        return False
    cols = spec.get("columns") or DEFAULT_COLUMNS
    return all(c in ALL_COLUMNS for c in cols)


def _dst_boundaries(y0, y1):
    """Sorted DST boundaries [start_y0, end_y0, start_y0+1, ...] as
    datetime64[s] — the same _nth_weekday rule eastern_offset uses."""
    b = []
    for y in range(y0, y1 + 1):
        b.append(np.datetime64(
            _datetime.combine(_nth_weekday(y, 3, 6, 2), _time(2, 0)), "s"))
        b.append(np.datetime64(
            _datetime.combine(_nth_weekday(y, 11, 6, 1), _time(2, 0)), "s"))
    return np.array(b, dtype="datetime64[s]")


def _edt_mask(ts):
    """Boolean EDT mask for naive-Eastern datetime64[s] — index parity in the
    boundary list == inside [start, end); searchsorted(side='right') puts a dt
    equal to a boundary AFTER it (start inclusive, end exclusive), identical
    to eastern_offset's comparison."""
    years = ts.astype("datetime64[Y]").astype(int) + 1970
    bnds = _dst_boundaries(int(years.min()), int(years.max()))
    idx = np.searchsorted(bnds, ts, side="right")
    return (idx % 2) == 1


_NUMSTR_CACHE_MAX = 200_000
_numstr_cache = {}   # float BITS (int) -> num_str text; cleared per export


def _num_str_bulk(farr):
    """num_str for a float64 array via np.unique over the BIT pattern — the
    REAL num_str runs once per distinct value, so each cell's text is
    byte-identical by construction."""
    bits = farr.view(np.int64)
    uniq, inv = np.unique(bits, return_inverse=True)
    lut = np.empty(len(uniq), dtype=object)
    cache = _numstr_cache
    uf = uniq.view(np.float64)
    for i in range(len(uniq)):
        k = int(uniq[i])
        s = cache.get(k)
        if s is None:
            s = num_str(uf[i])
            if len(cache) < _NUMSTR_CACHE_MAX:
                cache[k] = s
        lut[i] = s
    return lut[inv]


def _storage_timestamp_cells_fast(ts):
    days64 = ts.astype("datetime64[D]")
    months = (days64.astype("datetime64[M]").astype(np.int64) % 12) + 1
    month0 = days64.astype("datetime64[M]").astype("datetime64[D]")
    dom = (days64 - month0).astype("timedelta64[D]").astype(np.int64) + 1
    years = days64.astype("datetime64[Y]").astype(np.int64) + 1970
    secs = (ts - days64).astype("timedelta64[s]").astype(np.int64)
    hours = secs // 3600
    minutes = (secs % 3600) // 60
    seconds = secs % 60
    dates = np.char.add(
        np.char.add(np.char.add(months.astype(str), "/"), dom.astype(str)),
        np.char.add("/", years.astype(str)))
    times = np.char.add(
        np.char.add(np.char.add(hours.astype(str), ":"),
                    np.char.zfill(minutes.astype(str), 2)),
        np.char.add(":", np.char.zfill(seconds.astype(str), 2)))
    return dates.tolist(), times.tolist()


def _render_storage_body_fast(ts, o, h, l, c, v):
    """Canonical storage CSV body for one already-sorted chunk, no header."""
    if not len(ts):
        return b""
    dates, times = _storage_timestamp_cells_fast(ts)
    cols = [dates, times, _num_str_bulk(o).tolist(), _num_str_bulk(h).tolist(),
            _num_str_bulk(l).tolist(), _num_str_bulk(c).tolist(),
            [str(x) for x in v.tolist()]]
    return (ss.EOL.join(map(",".join, zip(*cols))) + ss.EOL).encode("ascii")


def _timestamp_array_fast(pa, ts, spec, edt):
    df = spec.get("date_format", "iso")
    off_secs = np.where(edt, -14400, -18000).astype("timedelta64[s]")
    if df == "epoch":
        return pa.array((ts.astype(np.int64) - off_secs.astype(np.int64)).tolist(),
                        pa.int64())
    if df == "date-only":
        if spec.get("timezone", "America/New_York") == "UTC":
            shown = ts - off_secs
            vals = np.datetime_as_string(shown.astype("datetime64[D]")).tolist()
        else:
            vals = np.datetime_as_string(ts.astype("datetime64[D]")).tolist()
        return pa.array(vals, pa.string())
    if spec.get("timezone", "America/New_York") == "UTC":
        vals = (ts - off_secs).astype("datetime64[s]").tolist()
    else:
        vals = ts.astype("datetime64[s]").tolist()
    return pa.array(vals, pa.timestamp("s"))


def _shown_ts_fast(ts, spec, edt):
    if spec.get("timezone", "America/New_York") != "UTC":
        return ts
    off_secs = np.where(edt, -14400, -18000).astype("timedelta64[s]")
    return ts - off_secs


def _date_array_fast(pa, ts, spec, edt):
    shown = _shown_ts_fast(ts, spec, edt)
    return pa.array(shown.astype("datetime64[D]"), pa.date32())


def _time_array_fast(pa, ts, spec, edt):
    shown = _shown_ts_fast(ts, spec, edt)
    days = shown.astype("datetime64[D]")
    seconds = (shown - days).astype("timedelta64[s]").astype(np.int32)
    return pa.array(seconds, pa.time32("s"))


def _aux_array_fast(pa, kind, aux, ts, empty_mode, n):
    lk, mode = aux.get(kind, (None, ""))[:2]
    missing = 0.0 if empty_mode == "zero" else None
    if mode == "daily":
        keys = sorted(lk)
        if not keys:
            return pa.array([missing] * n, pa.float64())
        keys64 = np.array(keys, dtype="datetime64[D]")
        days = ts.astype("datetime64[D]")
        idx = np.searchsorted(keys64, days, side="right") - 1
        vals = [float(lk[k]) for k in keys]
        return pa.array([vals[i] if i >= 0 else missing for i in idx.tolist()],
                        pa.float64())
    if mode == "intraday":
        return pa.array([
            (float(lk[t]) if t in lk else missing)
            for t in ts.astype("datetime64[s]").tolist()
        ], pa.float64())
    return pa.array([missing] * n, pa.float64())


def _arrow_table_fast(ts, o, h, l, c, v, canon, spec, aux):
    pa, _pq = ss._require_pyarrow()
    cols = spec.get("columns") or DEFAULT_COLUMNS
    em = spec.get("empty", "blank")
    n = len(ts)
    edt = _edt_mask(ts) if n else np.array([], dtype=bool)
    price_map = {"open": o, "high": h, "low": l, "close": c}
    data = {}
    for col in cols:
        label = COLUMN_LABEL[col]
        if col == "date":
            data[label] = _date_array_fast(pa, ts, spec, edt)
        elif col == "time":
            data[label] = _time_array_fast(pa, ts, spec, edt)
        elif col == "timestamp":
            data[label] = _timestamp_array_fast(pa, ts, spec, edt)
        elif col in price_map:
            data[label] = pa.array(price_map[col].tolist(), pa.float64())
        elif col == "volume":
            data[label] = pa.array(v.tolist(), pa.int64())
        elif col == "ticker":
            data[label] = pa.array([canon] * n, pa.string())
        elif col == "vwap":
            data[label] = pa.array(_vwap_floats(ts, o, h, l, c, v).tolist(),
                                   pa.float64())
        elif col in ("iv", "hvol"):
            data[label] = _aux_array_fast(pa, col, aux, ts, em, n)
    return pa.table(data)


def _timestamp_cells_fast(ts, spec, edt):
    """Timestamp cell strings per the spec (iso/epoch/date-only), matching
    format_timestamp byte-for-byte (any non-UTC timezone == NY wall clock,
    exactly as format_timestamp branches)."""
    df = spec.get("date_format", "iso")
    off_secs = np.where(edt, -14400, -18000).astype("timedelta64[s]")
    if df == "epoch":
        epoch = ts.astype(np.int64) - off_secs.astype(np.int64)
        return [str(x) for x in epoch.tolist()]
    if spec.get("timezone", "America/New_York") == "UTC":
        shown = ts - off_secs
        if df == "date-only":
            return np.datetime_as_string(
                shown.astype("datetime64[D]")).tolist()
        base = np.datetime_as_string(shown, unit="s")
        return np.char.add(base, "+00:00").tolist()
    if df == "date-only":                       # NY wall clock
        return np.datetime_as_string(ts.astype("datetime64[D]")).tolist()
    base = np.datetime_as_string(ts, unit="s")
    if edt.all():
        return np.char.add(base, "-04:00").tolist()
    if not edt.any():
        return np.char.add(base, "-05:00").tolist()
    suf = np.where(edt, "-04:00", "-05:00")
    return [a + b for a, b in zip(base.tolist(), suf.tolist())]


def _date_time_cells_fast(ts, spec, edt):
    shown = _shown_ts_fast(ts, spec, edt)
    days = shown.astype("datetime64[D]")
    dates = np.datetime_as_string(days).tolist()
    secs = (shown - days).astype("timedelta64[s]").astype(np.int64)
    hours = np.char.zfill((secs // 3600).astype(str), 2)
    minutes = np.char.zfill(((secs % 3600) // 60).astype(str), 2)
    seconds = np.char.zfill((secs % 60).astype(str), 2)
    times = np.char.add(
        np.char.add(np.char.add(hours, ":"), minutes),
        np.char.add(":", seconds)).tolist()
    return dates, times


def _vwap_floats(ts, o, h, l, c, v):
    """Vectorized attach_vwap: per-calendar-day cumulative typical-price VWAP.
    np.cumsum is sequential (left-to-right) like the Python loop, and each day
    cumsums from zero, so no cross-day float re-association can occur."""
    days = ts.astype("datetime64[D]")
    n = len(ts)
    starts = np.flatnonzero(
        np.r_[True, days[1:] != days[:-1]]) if n else np.array([], int)
    typ = (h + l + c) / 3.0
    vf = v.astype(np.float64)
    pvi = typ * vf
    cum_pv = np.empty(n)
    cum_v = np.empty(n)
    ends = np.r_[starts[1:], n]
    for i0, i1 in zip(starts.tolist(), ends.tolist()):
        np.cumsum(pvi[i0:i1], out=cum_pv[i0:i1])
        np.cumsum(vf[i0:i1], out=cum_v[i0:i1])
    pos = cum_v > 0
    return np.where(pos, cum_pv / np.where(pos, cum_v, 1.0), typ)


def _aux_cells_fast(kind, aux, ts, empty_token, n):
    """iv/hvol cells: daily forward-fill via searchsorted == bisect_right;
    intraday exact-join via the same dict lookup; no series -> empty token."""
    lk, mode = aux.get(kind, (None, ""))[:2]
    if mode == "daily":
        keys = sorted(lk)
        keys64 = np.array(keys, dtype="datetime64[D]")
        days = ts.astype("datetime64[D]")
        idx = np.searchsorted(keys64, days, side="right") - 1
        val_strs = [num_str(lk[k]) for k in keys]
        return [val_strs[i] if i >= 0 else empty_token for i in idx.tolist()]
    if mode == "intraday":
        pydt = ts.astype("datetime64[s]").tolist()   # datetime objects
        out = []
        for t in pydt:
            v2 = lk.get(t)
            out.append(empty_token if v2 is None else num_str(v2))
        return out
    return [empty_token] * n


def _render_body_fast(ts, o, h, l, c, v, canon, spec, aux):
    """Body bytes (no header) for one month chunk — byte-equal to
    render_rows(build_rows_aux(bars, ...), dict(spec, header=False))."""
    cols = spec.get("columns") or DEFAULT_COLUMNS
    delim = "\t" if spec.get("file_type") == "tsv" else \
        (spec.get("delimiter") or ",")
    empty_token = _EMPTY_TOKEN[spec.get("empty", "blank")]
    n = len(ts)
    edt = _edt_mask(ts)
    price_map = {"open": o, "high": h, "low": l, "close": c}
    out_cols = []
    date_cells = time_cells = None
    for col in cols:
        if col in ("date", "time"):
            if date_cells is None:
                date_cells, time_cells = _date_time_cells_fast(
                    ts, spec, edt)
            out_cols.append(date_cells if col == "date" else time_cells)
        elif col == "timestamp":
            out_cols.append(_timestamp_cells_fast(ts, spec, edt))
        elif col in price_map:
            out_cols.append(_num_str_bulk(price_map[col]).tolist())
        elif col == "volume":
            out_cols.append([str(x) for x in v.tolist()])
        elif col == "ticker":
            out_cols.append([canon] * n)
        elif col == "vwap":
            vw = _vwap_floats(ts, o, h, l, c, v)
            out_cols.append(_num_str_bulk(vw).tolist())
        elif col in ("iv", "hvol"):
            out_cols.append(_aux_cells_fast(col, aux, ts, empty_token, n))
        else:
            out_cols.append([""] * n)
    if not n:
        return b""
    if len(out_cols) == 1:
        body = "\n".join(out_cols[0])
    else:
        body = "\n".join(map(delim.join, zip(*out_cols)))
    return (body + "\n").encode("utf-8")


def _read_month_columns_fast(path, manifest_sha):
    """(ts64, o, h, l, c, v) numpy columns for one TRUSTED month file — the
    same sha gate as read_month_file_fast (manifest sha must match the raw
    bytes), falling back to the STRICT per-bar read on anything else, so a
    modified/corrupt file still surfaces and is never exported silently."""
    import hashlib
    import io
    p = Path(path)
    if manifest_sha is None or p.suffix != ".parquet":
        bars, _stats = ss.read_month_file(p, validate=True)
        return _bars_to_columns(bars)
    raw = None
    delay = 0.1
    for attempt in range(4):                 # same retry shape as the strict read
        try:
            st0 = p.stat()
            raw = p.read_bytes()
            if p.stat().st_mtime_ns == st0.st_mtime_ns:
                break
        except PermissionError:
            if attempt == 3:
                raise
        _wall.sleep(delay)
        delay *= 2
    if raw is None or hashlib.sha256(raw).hexdigest() != manifest_sha:
        bars, _stats = ss.read_month_file(p, validate=True)   # strict surfaces
        return _bars_to_columns(bars)
    _pa, pq = ss._require_pyarrow()
    tbl = pq.read_table(io.BytesIO(raw))
    if list(tbl.column_names) != ["ts", "open", "high", "low", "close",
                                  "volume"]:
        bars, _stats = ss.read_month_file(p, validate=True)
        return _bars_to_columns(bars)
    return (tbl.column("ts").to_numpy().astype("datetime64[s]"),
            tbl.column("open").to_numpy(),
            tbl.column("high").to_numpy(),
            tbl.column("low").to_numpy(),
            tbl.column("close").to_numpy(),
            tbl.column("volume").to_numpy())


def _bars_to_columns(bars):
    if not bars:
        z = np.array([], dtype="datetime64[s]")
        f = np.array([], dtype=np.float64)
        return z, f, f.copy(), f.copy(), f.copy(), np.array([], np.int64)
    t, o, h, l, c, v = zip(*bars)
    return (np.array(t, dtype="datetime64[s]"),
            np.array(o, np.float64), np.array(h, np.float64),
            np.array(l, np.float64), np.array(c, np.float64),
            np.array(v, np.int64))


def _merge_sessions_fast(chunks):
    """Concatenate per-session column chunks, stable-sort by ts, keep the
    FIRST duplicate ts — same as _dedupe_sorted (stable sort, first in
    pre-sort order wins)."""
    ts = np.concatenate([ch[0] for ch in chunks])
    order = np.argsort(ts, kind="stable")
    ts = ts[order]
    if not len(ts):
        cols = [np.concatenate([ch[i] for ch in chunks]) for i in range(1, 6)]
        return (ts,) + tuple(cols)
    keep = np.r_[True, ts[1:] != ts[:-1]]
    cols = []
    for i in range(1, 6):
        a = np.concatenate([ch[i] for ch in chunks])[order]
        cols.append(a[keep])
    return (ts[keep],) + tuple(cols)


def _export_one_fast(root, ticker, spec, out_path, progress=None, cancel=None):
    """export_one's vectorized twin for gate-eligible CSV/TSV specs: the same
    month-streamed structure, temp + atomic replace, zero-row skip, holes
    bookkeeping and return dict — only the per-month read and body rendering
    are vectorized. Callers reach this ONLY through export_one's gate."""
    canon = ss.canonical_ticker(ticker)
    base = ss.base_interval(spec.get("base_interval", "1m"))
    sessions = spec.get("sessions") or ["rth"]
    cols = spec.get("columns") or DEFAULT_COLUMNS
    sd, ed_ = spec["start_date"], spec["end_date"]
    lo64 = np.datetime64(_datetime.combine(sd, _time.min), "s")
    hi64 = np.datetime64(
        _datetime.combine(ed_, _time.min) + _timedelta(days=1), "s")
    aux = _aux_lookups(root, canon, base, cols, sd, ed_)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(
        f"{out_path.name}.{os.getpid()}-{threading.get_ident()}-"
        f"{next(_TMP_COUNTER)}.tmp")

    found = {s: [] for s in sessions}
    missing = {s: [] for s in sessions}
    aux_written = set()
    total, first_dt, last_dt = 0, None, None
    man_iv = (ss.load_manifest(Path(root) / canon) or {}).get("intervals", {})
    months = _months_in_range(sd, ed_)
    try:
        with open(tmp, "wb") as fh:
            if spec.get("header", True):
                fh.write(render_rows([], dict(spec, header=True)))
            for month_i, (y, m) in enumerate(months, 1):
                key = ss.month_key(y, m)
                _check_cancel(cancel)
                _emit_progress(progress, canon, month_i, len(months), key)
                chunks = []
                for sess in sessions:
                    tok = ss.with_session(base, sess)
                    if not ss.INTERVAL_RE.match(tok):
                        continue
                    p = ss.find_month_file(root, canon, y, m, tok)
                    if p is None:
                        missing[sess].append(key)
                        continue
                    sha = ((man_iv.get(tok, {}).get("months", {}).get(key)
                            or {}).get("sha256"))
                    chunks.append(_read_month_columns_fast(p, sha))
                    found[sess].append(key)
                if not chunks:
                    continue
                ts, o, h, l, c, v = _merge_sessions_fast(chunks)
                i0 = np.searchsorted(ts, lo64, side="left")
                i1 = np.searchsorted(ts, hi64, side="left")
                if i1 <= i0:
                    continue
                ts, o, h, l, c, v = (ts[i0:i1], o[i0:i1], h[i0:i1],
                                     l[i0:i1], c[i0:i1], v[i0:i1])
                if first_dt is None:
                    first_dt = ts[0].astype(_datetime)
                last_dt = ts[-1].astype(_datetime)
                total += len(ts)
                _mark_aux_timestamps_written(
                    aux_written, aux, cols,
                    ts.astype("datetime64[s]").tolist())
                fh.write(_render_body_fast(ts, o, h, l, c, v, canon, spec,
                                           aux))
            _check_cancel(cancel)
            fh.flush()
            os.fsync(fh.fileno())
        holes = _holes_from(found, missing)
        if total == 0:
            # nothing to export — never clobber an existing file with a stub
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass
            return {"ticker": canon, "rows": 0, "covered_start": None,
                    "covered_end": None, "holes": holes, "bytes": 0,
                    "out_path": None, "skipped": "no rows in range"}
        nbytes = tmp.stat().st_size
        _atomic_replace(tmp, out_path)
        cs = f"{ss.format_date(first_dt)} {ss.format_time(first_dt.time())}"
        ce = f"{ss.format_date(last_dt)} {ss.format_time(last_dt.time())}"
        return {"ticker": canon, "rows": total, "covered_start": cs,
                "covered_end": ce, "holes": holes, "bytes": nbytes,
                "out_path": str(out_path),
                "source_intervals": _source_intervals_from_run(
                    base, found, aux, aux_written)}
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _export_one_parquet_fast(root, ticker, spec, out_path, progress=None,
                             cancel=None):
    """Vectorized Parquet export: read month columns, build Arrow tables
    directly, and stream one row group per month. Parquet byte encodings may
    differ from the classic row-dict writer; the contract is schema/value
    equality after read-back.
    """
    _pa, pq = ss._require_pyarrow()
    canon = ss.canonical_ticker(ticker)
    base = ss.base_interval(spec.get("base_interval", "1m"))
    sessions = spec.get("sessions") or ["rth"]
    cols = spec.get("columns") or DEFAULT_COLUMNS
    sd, ed_ = spec["start_date"], spec["end_date"]
    lo64 = np.datetime64(_datetime.combine(sd, _time.min), "s")
    hi64 = np.datetime64(
        _datetime.combine(ed_, _time.min) + _timedelta(days=1), "s")
    aux = _aux_lookups(root, canon, base, cols, sd, ed_)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(
        f"{out_path.name}.{os.getpid()}-{threading.get_ident()}-"
        f"{next(_TMP_COUNTER)}.tmp")

    found = {s: [] for s in sessions}
    missing = {s: [] for s in sessions}
    aux_written = set()
    total, first_dt, last_dt = 0, None, None
    pqw = None
    man_iv = (ss.load_manifest(Path(root) / canon) or {}).get("intervals", {})
    months = _months_in_range(sd, ed_)
    try:
        for month_i, (y, m) in enumerate(months, 1):
            key = ss.month_key(y, m)
            _check_cancel(cancel)
            _emit_progress(progress, canon, month_i, len(months), key)
            chunks = []
            for sess in sessions:
                tok = ss.with_session(base, sess)
                if not ss.INTERVAL_RE.match(tok):
                    continue
                p = ss.find_month_file(root, canon, y, m, tok)
                if p is None:
                    missing[sess].append(key)
                    continue
                sha = ((man_iv.get(tok, {}).get("months", {}).get(key)
                        or {}).get("sha256"))
                chunks.append(_read_month_columns_fast(p, sha))
                found[sess].append(key)
            if not chunks:
                continue
            ts, o, h, l, c, v = _merge_sessions_fast(chunks)
            i0 = np.searchsorted(ts, lo64, side="left")
            i1 = np.searchsorted(ts, hi64, side="left")
            if i1 <= i0:
                continue
            ts, o, h, l, c, v = (ts[i0:i1], o[i0:i1], h[i0:i1],
                                 l[i0:i1], c[i0:i1], v[i0:i1])
            if first_dt is None:
                first_dt = ts[0].astype(_datetime)
            last_dt = ts[-1].astype(_datetime)
            total += len(ts)
            _mark_aux_timestamps_written(
                aux_written, aux, cols,
                ts.astype("datetime64[s]").tolist())
            tbl = _arrow_table_fast(ts, o, h, l, c, v, canon, spec, aux)
            if pqw is None:
                pqw = pq.ParquetWriter(str(tmp), tbl.schema,
                                       compression="zstd")
            pqw.write_table(tbl)
        _check_cancel(cancel)
        if pqw is not None:
            pqw.close()
            pqw = None
        holes = _holes_from(found, missing)
        if total == 0:
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass
            return {"ticker": canon, "rows": 0, "covered_start": None,
                    "covered_end": None, "holes": holes, "bytes": 0,
                    "out_path": None, "skipped": "no rows in range"}
        nbytes = tmp.stat().st_size
        _atomic_replace(tmp, out_path)
        cs = f"{ss.format_date(first_dt)} {ss.format_time(first_dt.time())}"
        ce = f"{ss.format_date(last_dt)} {ss.format_time(last_dt.time())}"
        return {"ticker": canon, "rows": total, "covered_start": cs,
                "covered_end": ce, "holes": holes, "bytes": nbytes,
                "out_path": str(out_path),
                "source_intervals": _source_intervals_from_run(
                    base, found, aux, aux_written)}
    finally:
        if pqw is not None:
            try:
                pqw.close()
            except Exception:  # noqa: BLE001
                pass
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass
