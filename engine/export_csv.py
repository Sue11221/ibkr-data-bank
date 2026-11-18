"""Export a combined CSV from the per-month bar shards (stdlib-only).

The archive stores ONE month per file (TICKER_YYYY-MM_interval.csv). A user
who wants "AAPL 1m, last 2 years, as one file for Excel/another tool" should
not have to hand-stitch 24 shards. This module does exactly that — and ONLY
that: it concatenates existing month files over a date range, trims the two
edge months to the requested day bounds, dedupes by timestamp, and writes a
single canonical CSV byte-for-byte identical in format to a month file
(same HEADER, same M/D/YYYY + H:MM:SS, same CRLF, same shortest-round-trip
prices). RAW as stored — no resampling, no adjustment, no fabrication.

The contract that makes this safe:
  * Each shard is read through stock_storage.read_month_file (STRICT) — an
    externally-modified file fails the export instead of corrupting the
    output, exactly as it would fail a scan.
  * A MISSING interior month is NOT invented: the export proceeds over what
    exists and reports the gap in `holes`, so the caller can see the data is
    discontinuous rather than silently believing it is whole.
  * Edge months are trimmed to the exact [start_date, end_date] day window
    (inclusive); interior months pass through whole.

Kept free of tkinter/pandas exactly like stock_storage — importable in a
worker thread and a headless test, and the output format can never drift
from the storage format because it reuses the same formatter.
"""

from datetime import date, datetime, timedelta

import stock_storage as ss


def _months_in_range(start_date, end_date):
    """Inclusive list of (year, month) from start_date's month through
    end_date's month, in calendar order."""
    if end_date < start_date:
        raise ss.StorageError(
            f"end_date {end_date} is before start_date {start_date}")
    out = []
    y, m = start_date.year, start_date.month
    last = (end_date.year, end_date.month)
    while (y, m) <= last:
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def _collect_series(root, canon, interval, start_date, end_date, *,
                    progress=None, progress_offset=0, progress_total=None):
    """Read + day-trim ONE (canon, interval) series over [start_date,
    end_date]. Returns (bars, months_used, holes). Each shard is read STRICT
    (read_month_file); a missing interior month is reported in holes, not
    fabricated."""
    months = _months_in_range(start_date, end_date)
    total = len(months) if progress_total is None else progress_total
    bars, months_used, holes = [], [], []
    for month_index, (y, m) in enumerate(months, 1):
        key = ss.month_key(y, m)
        if progress is not None:
            progress({
                "ticker": canon,
                "month_index": progress_offset + month_index,
                "month_total": total,
                "month": key,
            })
        path = ss.find_month_file(root, canon, y, m, interval)  # parquet or csv
        if path is None:
            holes.append(key)
            continue
        mbars, _stats = ss.read_month_file(path)   # strict; raises on drift
        bars.extend(mbars)
        months_used.append(key)
    lo = datetime.combine(start_date, datetime.min.time())
    hi = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1)
    bars = [b for b in bars if lo <= b[0] < hi]
    return bars, months_used, holes


def _write_bars(bars, out_path, fmt="csv", *, interval=None):
    """Write bars to out_path in `fmt`: 'csv' = the canonical month-file form
    (HEADER + format_bar, CRLF); 'parquet' = the typed store form
    (ss._bars_to_parquet). Same bars either way (Parquet re-exports byte-
    identical CSV). Sorts + dedupes by timestamp first. Returns
    (rows, covered_start, covered_end)."""
    bars = sorted(bars, key=lambda b: b[0])
    deduped, prev = [], None
    for b in bars:
        if prev is not None and b[0] == prev:
            continue
        deduped.append(b)
        prev = b[0]
    bars = deduped
    allow_zero_prices = ss.kind_of(interval or "") in ss.RATIO_KINDS
    if fmt == "parquet":
        payload = ss._bars_to_parquet(bars)
    elif fmt == "csv":
        lines = [ss.HEADER]
        lines.extend(ss.format_bar(
            *b, allow_zero_prices=allow_zero_prices) for b in bars)
        payload = (ss.EOL.join(lines) + ss.EOL).encode("ascii")
    else:
        raise ss.StorageError(f"unknown export format {fmt!r}")
    ss._atomic_write_bytes(out_path, payload)
    cs = ce = None
    if bars:
        cs = f"{ss.format_date(bars[0][0])} {ss.format_time(bars[0][0].time())}"
        ce = (f"{ss.format_date(bars[-1][0])} "
              f"{ss.format_time(bars[-1][0].time())}")
    return len(bars), cs, ce


def export_sessions_csv(root, ticker, base_interval, sessions, start_date,
                        end_date, out_path, fmt="csv", progress=None):
    """Stitch one or more SESSION series (regular / pre-market / after-hours)
    of the SAME base interval into ONE chronological CSV. Same canonical
    format as a month file, NO session column — the session is implied by the
    timestamp (pre 04:00-09:30, regular 09:30-16:00, after 16:00-20:00), so
    merging by timestamp interleaves them correctly. `sessions` is a list from
    {'rth','pre','post'}. Returns rows / covered / months_used / holes (holes
    tagged by session) / per_session row counts."""
    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()
    canon = ss.canonical_ticker(ticker)
    base = ss.base_interval(base_interval)
    bars, holes, months_used, per_session = [], [], set(), {}
    # The ledger still plans one unit per calendar month. Distributing its
    # observations across session passes reports fractional progress through
    # that month work without changing the raw legacy progress stream.
    month_count = (len(_months_in_range(start_date, end_date))
                   if progress is not None else 0)
    progress_total = month_count * len(sessions)
    for session_index, sess in enumerate(sessions):
        if sess not in ("rth", "pre", "post"):
            raise ss.StorageError(f"unknown session {sess!r}")
        tok = ss.with_session(base, sess)
        if not ss.INTERVAL_RE.match(tok):
            raise ss.StorageError(f"bad interval token {tok!r}")
        sb, mu, hl = _collect_series(
            root, canon, tok, start_date, end_date,
            progress=progress,
            progress_offset=session_index * month_count,
            progress_total=progress_total)
        bars.extend(sb)
        per_session[sess] = len(sb)
        months_used.update(mu)
        if mu:                       # archived: report only the REAL interior gaps
            holes.extend(f"{sess}:{h}" for h in hl)
        elif hl:                     # never archived: one note, not every month
            holes.append(f"{sess}:not-archived")
    rows, cs, ce = _write_bars(bars, out_path, fmt, interval=base)
    return {"rows": rows, "covered_start": cs, "covered_end": ce,
            "months_used": sorted(months_used), "holes": holes,
            "per_session": per_session}


def export_combined_csv(root, ticker, interval, start_date, end_date,
                        out_path, fmt="csv", progress=None):
    """Stitch the per-month shards for `ticker`/`interval` covering
    [start_date, end_date] (inclusive day bounds) into ONE canonical CSV at
    `out_path`. RAW as stored.

    Reads each TICKER_YYYY-MM_interval.csv in range via
    stock_storage.read_month_file (strict), concatenates them, sorts and
    dedupes by timestamp (a later read wins — but shards never overlap, so
    this only guards against accidental duplicates), trims the first and
    last calendar months to the exact start/end days, and writes the same
    header/format a month file uses. A missing interior month is exported
    AROUND, never fabricated, and listed in `holes`.

    Returns a dict:
      rows           int   — bars written
      covered_start  str|None  "M/D/YYYY H:MM:SS" of the first bar written
      covered_end    str|None  ditto for the last bar (None when 0 rows)
      months_used    [str]     "YYYY-MM" of every shard that contributed
      holes          [str]     "YYYY-MM" of in-range months with no shard
    """
    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()
    canon = ss.canonical_ticker(ticker)
    if not ss.INTERVAL_RE.match(interval):
        raise ss.StorageError(f"bad interval token {interval!r}")

    months = _months_in_range(start_date, end_date)
    bars = []
    months_used = []
    holes = []
    for month_index, (y, m) in enumerate(months, 1):
        key = ss.month_key(y, m)
        if progress is not None:
            progress({
                "ticker": canon,
                "month_index": month_index,
                "month_total": len(months),
                "month": key,
            })
        path = ss.find_month_file(root, canon, y, m, interval)  # parquet or csv
        if path is None:
            holes.append(key)
            continue
        mbars, _stats = ss.read_month_file(path)   # strict; raises on drift
        bars.extend(mbars)
        months_used.append(key)

    # Trim the edge months to the exact requested day window. Interior
    # months are entirely inside [start_date, end_date] by construction, so
    # this only ever removes bars from the first/last calendar month.
    lo = datetime.combine(start_date, datetime.min.time())
    hi = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1)
    bars = [b for b in bars if lo <= b[0] < hi]

    # Sort + dedupe by timestamp (defensive — shards do not overlap).
    bars.sort(key=lambda b: b[0])
    deduped = []
    prev = None
    for b in bars:
        if prev is not None and b[0] == prev:
            continue
        deduped.append(b)
        prev = b[0]
    bars = deduped

    rows, cs, ce = _write_bars(bars, out_path, fmt, interval=interval)
    return {"rows": rows, "covered_start": cs, "covered_end": ce,
            "months_used": months_used, "holes": holes}


# --- presets -------------------------------------------------------------------

PRESETS = ("2mo", "6mo", "1yr", "2yr", "5yr", "10yr", "15yr", "furthest")

_PRESET_DAYS = {"2mo": 60, "6mo": 182, "1yr": 365, "2yr": 730,
                "5yr": 1826, "10yr": 3652, "15yr": 5478}


def preset_range(preset, today, earliest, latest):
    """Map a named preset to a concrete (start_date, end_date) day window.

    `today` anchors the lookback presets (end = today); `earliest`/`latest`
    are the first/last stored bar DATES (used by 'furthest', and to clamp a
    lookback that reaches before the archive begins). All four are
    datetime.date. The lookback windows are approximate calendar spans
    (e.g. '2mo' = 60 days) anchored to today — the per-month stitch then
    trims to whatever bars actually fall inside.

    'furthest' = earliest..latest (the whole stored span).
    """
    if isinstance(today, datetime):
        today = today.date()
    if isinstance(earliest, datetime):
        earliest = earliest.date()
    if isinstance(latest, datetime):
        latest = latest.date()
    if preset == "furthest":
        return (earliest, latest)
    if preset not in _PRESET_DAYS:
        raise ss.StorageError(f"unknown preset {preset!r}; expected one of "
                              f"{', '.join(PRESETS)}")
    end = today
    start = today - timedelta(days=_PRESET_DAYS[preset])
    # Don't ask for data older than the archive (keeps holes honest: a hole
    # should mean a genuinely missing interior month, not pre-history).
    if earliest is not None and start < earliest:
        start = earliest
    return (start, end)
