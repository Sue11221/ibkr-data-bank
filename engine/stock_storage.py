"""Stock Data Storage — GUI-free core (paths, byte-exact CSV, manifest, scan).

This module owns the STORAGE CONTRACT for the app's bar archive. Everything
that touches the tree goes through here; display_data.py only builds UI on
top. Kept free of tkinter/pandas/numpy on purpose: it stays importable in
spawned workers and headless tests, and a dependency upgrade can never
change how files are written (stdlib only).

THE TREE
    Stock Data Storage/
      _quarantine/               reserved (ingest evidence) — scans skip
      _ingest_reports/           reserved (ingest run reports) — likewise
      <TICKER>/                  canonical: A-Z 0-9 dash, e.g. BRK-B
        manifest.json            per-ticker index (rebuildable cache)
        <YYYY>/
          <NN-Mon>/              01-Jan .. 12-Dec (English, locale-proof)
            <TICKER>_<YYYY-MM>_<interval>.csv

THE FILE FORMAT (byte-exact, locked to the user's existing archive)
    Date,Time,open,high,low,close,volume\r\n
    6/15/2015,9:30:00,28.24,28.25,28.18,28.18,2641532\r\n
  * M/D/YYYY without zero padding; H:MM:SS without a padded hour. Built by
    hand — strftime cannot produce this portably (%-m is POSIX-only, %#m is
    Windows-only), and that platform fork is exactly how a Mac-born archive
    quietly forks on Windows.
  * Prices: shortest round-trip decimal (repr), trailing ".0" dropped —
    28.20 is written "28.2", 198.0 is written "198". Lossless: the parsed
    float is bit-identical either way.
  * volume: plain int. CRLF line endings, trailing newline, pure ASCII —
    all three measured from the user's real AAPL_10Y_1m.csv.
  * Rows sorted by parsed datetime, strictly unique timestamps, regular
    trading hours only (09:30:00-15:59:59 ET, Mon-Fri).
  * The READER is strict: any drift (padded dates, LF endings, alien
    header, non-ASCII) raises StorageFormatError instead of coercing —
    an externally-modified file must be surfaced, never silently adopted.

WRITE SAFETY
  Whole-month files are replaced atomically: unique-per-call temp file in
  the same directory, fsync, size check, os.replace with retry/backoff for
  the file-open-in-Excel PermissionError. A crash can therefore leave a
  stale month or an inert *.tmp (swept by the next scan once it is an hour
  old), never a torn month. Manifest writes are also atomic, and callers
  commit data files BEFORE manifest entries, so the manifest is only ever
  BEHIND the tree (safe to reconcile by scan). Durability bound, stated
  honestly: os.replace is atomic on NTFS but not flushed through, so a
  POWER CUT within seconds of a write can roll the rename back to the
  prior file — never a torn one — and the next scan heals the manifest.
"""

import hashlib
import itertools
import json
import operator
import os
import re
import sys
import threading
import time as _time
from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, time
from pathlib import Path

import operation_gate

STORAGE_DIR_NAME = "Stock Data Storage"
MANIFEST_NAME = "manifest.json"
VOL_VALUE_RECONCILE_STAGE_DIR = ".vol_value_reconcile_stage"
ORDINARY_CORRECTION_TXN_SENTINEL = "ordinary-write.json"
ORDINARY_CORRECTION_TXN_MARKER = "transaction.json"
_ORDINARY_CORRECTION_TXN_VERSION = 1
INTERVAL_FINGERPRINT_VERSION = 2
BANK_STATE_FINGERPRINT_VERSION = 1
HEADER = "Date,Time,open,high,low,close,volume"
EOL = "\r\n"                      # measured from the existing archive
DEFAULT_INTERVAL = "1m"
# On-disk month-file format. PARQUET is the canonical store (typed, ~3-6x
# smaller, ~20x faster to write than per-row CSV formatting); CSV is still a
# first-class READ format (legacy archives + on-demand export). A bar is
# (naive US/Eastern second-resolution datetime, float64 OHLC, int64 volume), so
# the Parquet schema below round-trips it EXACTLY and format_price() of the
# stored floats reproduces the identical canonical CSV on export.
DEFAULT_DATA_FORMAT = "parquet"
_PARQUET_COLS = ["ts", "open", "high", "low", "close", "volume"]
RTH_FIRST = time(9, 30, 0)        # first bar label of a regular session
RTH_LAST = time(15, 59, 59)       # last second a bar label may carry

# Session variants live in SEPARATE files for the same ticker+interval, keyed by
# an interval-token suffix ("1m" = regular, "1m-pre" = pre-market, "1m-post" =
# after-hours). Each file is validated against its OWN hours so a -pre/-post
# file can hold the out-of-RTH bars an RTH file must reject. Windows are
# inclusive (first <= label <= last).
SESSION_WINDOWS = {
    "rth":  (time(9, 30, 0), time(15, 59, 59)),
    "pre":  (time(4, 0, 0),  time(9, 29, 59)),
    "post": (time(16, 0, 0), time(19, 59, 59)),
}

_TICKER_TRANSACTION_GUARD = threading.RLock()
_TICKER_TRANSACTION_LOCKS = {}
_TICKER_TRANSACTION_LOCAL = threading.local()
_TICKER_TRANSACTION_WAIT_S = 30.0


def _ticker_transaction_lock_path(ticker_dir):
    # First commit may lock a canonical ticker before its directory exists.
    ticker_dir = Path(ticker_dir).resolve(strict=False)
    identity = hashlib.sha256(
        str(ticker_dir).casefold().encode("utf-8")).hexdigest()[:20]
    return (ticker_dir.parent.parent / "Run Logs" /
            f".ticker-bank-{identity}.lock")


@contextmanager
def ticker_transaction(ticker_dir):
    """Serialize one ticker's manifest publication across threads/processes."""
    try:
        ticker_dir = Path(ticker_dir).resolve(strict=False)
    except OSError as exc:
        raise StorageError("ticker transaction directory is unavailable") from exc
    key = str(ticker_dir).casefold()
    with _TICKER_TRANSACTION_GUARD:
        local_lock = _TICKER_TRANSACTION_LOCKS.setdefault(
            key, threading.RLock())
    with local_lock:
        deadline = _time.monotonic() + _TICKER_TRANSACTION_WAIT_S
        lease = None
        while lease is None:
            try:
                lease = operation_gate.acquire(
                    "ticker_bank", owner=f"ticker bank {ticker_dir.name}",
                    path=_ticker_transaction_lock_path(ticker_dir))
            except operation_gate.OperationBusy as exc:
                if _time.monotonic() >= deadline:
                    raise StorageError(
                        f"{ticker_dir.name}: ticker bank transaction is busy") from exc
                _time.sleep(0.02)
            except operation_gate.OperationGateError as exc:
                raise StorageError(
                    f"{ticker_dir.name}: cannot lock ticker bank transaction") from exc
        depths = getattr(_TICKER_TRANSACTION_LOCAL, "depths", None)
        if depths is None:
            depths = {}
            _TICKER_TRANSACTION_LOCAL.depths = depths
        outermost = not depths.get(key, 0)
        try:
            # An ordinary writer carrying exact volatility-correction evidence
            # uses the shared fail-closed stage namespace.  Resolve that WAL
            # before yielding the first transaction on this thread.  The
            # reconcile writer's own marker has no ordinary sentinel and is
            # deliberately left for vol_value_bank to recover.
            if outermost:
                recover_ordinary_correction_transaction(ticker_dir)
            depths[key] = depths.get(key, 0) + 1
            yield
        finally:
            depth = depths.get(key, 0)
            if depth <= 1:
                depths.pop(key, None)
            else:
                depths[key] = depth - 1
            lease.release()
_SESSION_SUFFIX = {"pre": "-pre", "post": "-post"}

MONTH_DIRS = ("01-Jan", "02-Feb", "03-Mar", "04-Apr", "05-May", "06-Jun",
              "07-Jul", "08-Aug", "09-Sep", "10-Oct", "11-Nov", "12-Dec")
_MONTH_DIR_SET = frozenset(MONTH_DIRS)

# Windows refuses (or worse, half-accepts) these as path components, and a
# tree written on Windows must stay portable. PRN is a real ETF ticker, so
# this is not theoretical; reserved symbols get a trailing dash (PRN -> PRN-)
# and the manifest keeps the true symbol.
_WIN_RESERVED = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{i}" for i in range(1, 10)}
    | {f"LPT{i}" for i in range(1, 10)})

# Whitelist regexes: ANYTHING in the tree that does not match these is
# invisible to every reader, writer and scan — never parsed, never deleted,
# only reported. That is what makes "backup of Jan" folders and stray Excel
# scratch files inert instead of dangerous. Tickers REQUIRE a letter (an
# all-digit folder is a misplaced year, not a ticker); interval tokens and
# years are canonical-only so '01m' twins and '0999' files can never fork
# a series.
TICKER_DIR_RE = re.compile(r"^(?=.*[A-Z])[A-Z0-9-]{1,10}$")
YEAR_DIR_RE = re.compile(r"^[12]\d{3}$")
# interval token grammar: <bar>[-<kind>][-<session>]. bar = N s/m/h/d (daily 'd'
# added for IV/HVOL); kind = iv|hvol|bidask (default TRADES, no suffix); session =
# pre|post (default RTH). Order is fixed: kind THEN session, session ALWAYS LAST so
# session_of()/base_interval()'s trailing-suffix logic stays valid. A bare TRADES
# intraday token ('1m', '1m-pre') is unchanged — this regex is a strict superset.
_IV_TOKEN = r"[1-9]\d{0,3}[smhd](?:-(?:iv|hvol|bidask))?(?:-(?:pre|post))?"
INTERVAL_RE = re.compile(r"^" + _IV_TOKEN + r"$")
IDENTITY_CORRECTION_TYPES = frozenset({
    "identity_listing_truncation",
    "identity_truncation",
    "truncation",
})
FILENAME_RE = re.compile(
    r"^([A-Z0-9-]{1,10})_([12]\d{3})-(0[1-9]|1[0-2])_"
    r"(" + _IV_TOKEN + r")\.(csv|parquet)$")  # grp5 = format
# Temps created by _atomic_write_bytes (and ONLY those — the suffix shape
# is ours): the one namespace the scan may delete, once safely stale.
TMP_RE = re.compile(r"\.(?:csv|json|parquet)\.\d+-\d+-\d+\.tmp$")
TMP_MAX_AGE_S = 3600

_DATE_RE = re.compile(r"^([1-9]|1[0-2])/([1-9]|[12]\d|3[01])/([12]\d{3})$")
_TIME_RE = re.compile(r"^(\d|1\d|2[0-3]):([0-5]\d):([0-5]\d)$")
_PRICE_RE = re.compile(r"^(0|[1-9]\d*)(\.\d+)?$")
_VOLUME_RE = re.compile(r"^(0|[1-9]\d*)$")

_REPLACE_ATTEMPTS = 5             # os.replace retries for Excel/AV locks
_REPLACE_BACKOFF_S = 0.2          # doubled each attempt: 0.2 .. 3.2 s

WRITTEN_BY = {"module": "stock_storage 1.0",
              "python": sys.version.split()[0]}


class StorageError(Exception):
    """Base for everything this module raises on purpose."""


class StorageFormatError(StorageError):
    """A file or bar violates the byte-exact storage contract. `problems`
    holds per-row diagnostics (capped) so the UI can show exactly why."""

    def __init__(self, message, problems=None):
        super().__init__(message)
        self.problems = list(problems or [])


# --- paths ------------------------------------------------------------------

def storage_root(project_dir):
    """The tree root: '<project>/Stock Data Storage' (the user's choice)."""
    return Path(project_dir) / STORAGE_DIR_NAME


def canonical_ticker(symbol):
    """Map any vendor/user spelling to the ONE filesystem spelling.

    'ko' -> KO, 'BRK.B' / 'brk b' / 'BRK/B' -> BRK-B, 'PRN' -> 'PRN-'
    (Windows reserved). Raises StorageError for anything that cannot be a
    safe folder name; the original spelling belongs in the manifest, not
    the path."""
    s = re.sub(r"[./ \\]+", "-", str(symbol).strip().upper())
    s = re.sub(r"-{2,}", "-", s).strip("-")
    if not TICKER_DIR_RE.match(s):
        raise StorageError(f"cannot derive a safe ticker folder from "
                           f"{symbol!r} (got {s!r})")
    if s in _WIN_RESERVED:
        s += "-"
    return s


def month_key(year, month):
    return f"{year:04d}-{month:02d}"


def month_dir(root, ticker, year, month):
    return Path(root) / ticker / f"{year:04d}" / MONTH_DIRS[month - 1]


def month_file_path(root, ticker, year, month, interval=DEFAULT_INTERVAL,
                    fmt=None):
    """The canonical month-file path. `fmt` selects the extension (defaults to
    DEFAULT_DATA_FORMAT = parquet); pass 'csv' for the legacy/export form."""
    if not INTERVAL_RE.match(interval):
        raise StorageError(f"bad interval token {interval!r}")
    ext = fmt or DEFAULT_DATA_FORMAT
    if ext not in ("parquet", "csv"):
        raise StorageError(f"unknown month-file format {ext!r}")
    name = f"{ticker}_{month_key(year, month)}_{interval}.{ext}"
    return month_dir(root, ticker, year, month) / name


def find_month_file(root, ticker, year, month, interval=DEFAULT_INTERVAL):
    """The EXISTING month file for this slot regardless of format — Parquet
    preferred, legacy CSV as fallback — or None if neither exists. Use this to
    READ/locate stored data; month_file_path() (canonical Parquet) is for the
    WRITE target. A Parquet beside a legacy CSV twin resolves to the Parquet."""
    for fmt in ("parquet", "csv"):
        p = month_file_path(root, ticker, year, month, interval, fmt=fmt)
        if p.exists():
            return p
    return None


# --- session variants (regular / pre-market / after-hours) -------------------

def session_of(interval):
    """The session an interval token names: '1m-pre'->'pre', '1m-post'->'post',
    '1m'->'rth'."""
    if interval.endswith("-pre"):
        return "pre"
    if interval.endswith("-post"):
        return "post"
    return "rth"


RATIO_KINDS = frozenset({"iv", "hvol"})        # 0-1 ratios, not dollar prices
_KIND_SUFFIXES = ("-iv", "-hvol", "-bidask")


def base_interval(interval):
    """Strip session AND kind suffixes -> the pure IBKR bar interval:
    '1m-post'->'1m', '1m-iv'->'1m', '1d-hvol'->'1d'. Used to look up the IBKR bar
    size, which is the same regardless of kind or session. (Bare TRADES tokens are
    unchanged — the kind loop is a no-op when there's no kind suffix.)"""
    s = interval
    for suf in ("-pre", "-post"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    for suf in _KIND_SUFFIXES:
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s


def kind_of(interval):
    """The data KIND a token names: '1m-iv'->'iv', '1d-hvol'->'hvol',
    '1m-bidask-pre'->'bidask', '1m'/'1m-pre'->'' (TRADES, the default)."""
    s = interval
    for suf in ("-pre", "-post"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    for suf in _KIND_SUFFIXES:
        if s.endswith(suf):
            return suf[1:]
    return ""


def with_kind(base, kind):
    """('1m','iv')->'1m-iv'; ('1d','hvol')->'1d-hvol'; ('1m','' | 'trades')->'1m'."""
    return f"{base}-{kind}" if kind and kind != "trades" else base


def parse_interval(interval):
    """Split a token into (bar_interval, kind, session):
    '1m'->('1m','','rth'), '1d-hvol'->('1d','hvol','rth'),
    '1m-bidask-pre'->('1m','bidask','pre')."""
    return base_interval(interval), kind_of(interval), session_of(interval)


def with_session(base, session):
    """Build a session token: ('1m','post')->'1m-post'; ('1m','rth')->'1m'."""
    return base + _SESSION_SUFFIX.get(session, "")


DAILY_WINDOW = (time(0, 0, 0), time(23, 59, 59))


def session_window(interval):
    """(first_label, last_label) inclusive for the interval's session. A DAILY
    interval spans the WHOLE DAY — a daily bar's timestamp is the date (not an
    intraday time) — so it validates against 00:00:00-23:59:59, never RTH. Bare
    intraday TRADES tokens are unchanged."""
    if base_interval(interval).endswith("d"):
        return DAILY_WINDOW
    return SESSION_WINDOWS[session_of(interval)]


def identity_correction_records(manifest, interval, *, ticker=None):
    """Return strict ``(note, cutover_date)`` identity-floor records.

    Listing corrections without ``intervals`` are ticker-wide. Historical
    scoped records apply to either their exact interval token or its base
    family (for example, ``1d`` also protects ``1d-hvol``). Unknown correction
    types are outside this parser and are ignored; malformed identity records
    raise ``StorageError`` so a fetch caller can fail closed.
    """
    if not isinstance(interval, str) or INTERVAL_RE.fullmatch(interval) is None:
        raise StorageError(f"invalid identity-floor interval: {interval!r}")
    expected_ticker = canonical_ticker(ticker) if ticker is not None else None
    if not isinstance(manifest, dict):
        raise StorageError("manifest is not an object")
    raw = manifest.get("data_corrections")
    if raw is None:
        return []
    if not isinstance(raw, list) or len(raw) > 256:
        raise StorageError("manifest data_corrections are malformed")

    records = []
    for index, note in enumerate(raw):
        if not isinstance(note, dict):
            raise StorageError(f"manifest data correction {index} is malformed")
        correction_type = note.get("type")
        if correction_type not in IDENTITY_CORRECTION_TYPES:
            continue
        if note.get("ticker") is not None:
            try:
                noted_ticker = canonical_ticker(note.get("ticker"))
            except StorageError as exc:
                raise StorageError(
                    f"manifest identity correction {index} ticker is invalid"
                ) from exc
            if expected_ticker is not None and noted_ticker != expected_ticker:
                raise StorageError(
                    f"manifest identity correction {index} ticker mismatches")

        intervals = note.get("intervals")
        if (correction_type == "identity_listing_truncation"
                and intervals is None):
            applies = True
        else:
            if (not isinstance(intervals, list) or not intervals
                    or len(intervals) > 128
                    or not all(
                        isinstance(item, str)
                        and INTERVAL_RE.fullmatch(item) is not None
                        for item in intervals)):
                raise StorageError(
                    f"manifest identity correction {index} intervals are invalid")
            applies = interval in intervals or base_interval(interval) in intervals
        if not applies:
            continue

        value = note.get("cutover")
        if isinstance(value, datetime):
            raise StorageError(
                f"manifest identity correction {index} cutover is invalid")
        if isinstance(value, date):
            boundary = value
        elif isinstance(value, str):
            try:
                boundary = date.fromisoformat(value)
            except ValueError as exc:
                raise StorageError(
                    f"manifest identity correction {index} cutover is invalid"
                ) from exc
            if boundary.isoformat() != value:
                raise StorageError(
                    f"manifest identity correction {index} cutover is invalid")
        else:
            raise StorageError(
                f"manifest identity correction {index} cutover is invalid")
        records.append((note, boundary))
    return records


def identity_floor(manifest, interval, *, ticker=None):
    """Return the latest strict identity cutover applying to ``interval``."""
    records = identity_correction_records(
        manifest, interval, ticker=ticker)
    return max((boundary for _note, boundary in records), default=None)


def _window_for_name(name):
    """The session window implied by a month-file NAME (via its interval
    token), or None (-> RTH default) if the name doesn't match the contract."""
    m = FILENAME_RE.match(name)
    return session_window(m.group(4)) if m else None


# --- the byte-exact value formats --------------------------------------------

def format_date(d):
    """M/D/YYYY without padding — by hand, never strftime (portability)."""
    return f"{d.month}/{d.day}/{d.year}"


def format_time(t):
    """H:MM:SS with an unpadded hour, exactly like the existing archive."""
    return f"{t.hour}:{t.minute:02d}:{t.second:02d}"


_SCI_RE = re.compile(r"^(\d)(?:\.(\d+))?e([+-]\d+)$")


def _expand_sci(s):
    """Positional expansion of repr's scientific form using repr's OWN
    digits — '8.9285714285714e-05' -> '0.000089285714285714'. Same digits
    = same float back, so shortest-round-trip survives without scientific
    notation (which the schema forbids)."""
    m = _SCI_RE.match(s)
    digits = m.group(1) + (m.group(2) or "")
    point = 1 + int(m.group(3))       # decimal-point slot in `digits`
    if point <= 0:
        return "0." + "0" * (-point) + digits
    if point >= len(digits):
        return digits + "0" * (point - len(digits))
    return digits[:point] + "." + digits[point:]


def format_price(v, *, allow_zero=False):
    """Shortest decimal that round-trips to the same float64, trailing '.0'
    dropped (28.2 not 28.20, 198 not 198.0). repr() IS the shortest
    round-trip in CPython; when repr uses scientific notation, the same
    digits are re-laid positionally — every accepted finite float
    therefore round-trips losslessly, including sub-$0.0001
    split-adjusted prices, and the file never contains an exponent. Zero is
    accepted only when ``allow_zero`` is explicit (for ratio-kind OHLC)."""
    f = float(v)
    invalid = f < 0.0 if allow_zero else f <= 0.0
    if f != f or f in (float("inf"), float("-inf")) or invalid:
        qualifier = "non-negative" if allow_zero else "positive"
        raise StorageFormatError(
            f"price must be {qualifier} finite ({v!r})")
    if f == 0.0:
        return "0"
    s = repr(f)
    if "e" in s or "E" in s:
        s = _expand_sci(s)
    if s.endswith(".0"):
        s = s[:-2]
    return s


def format_bar(dt, o, h, lo, c, vol, *, allow_zero_prices=False):
    return (f"{format_date(dt)},{format_time(dt.time())},"
            f"{format_price(o, allow_zero=allow_zero_prices)},"
            f"{format_price(h, allow_zero=allow_zero_prices)},"
            f"{format_price(lo, allow_zero=allow_zero_prices)},"
            f"{format_price(c, allow_zero=allow_zero_prices)},{int(vol)}")


def parse_timestamp(date_s, time_s):
    """Strict parse of the two schema columns -> naive datetime (US/Eastern
    by contract). Returns None when either field is off-format."""
    dm = _DATE_RE.match(date_s)
    tm = _TIME_RE.match(time_s)
    if not dm or not tm:
        return None
    try:
        return datetime(int(dm.group(3)), int(dm.group(1)), int(dm.group(2)),
                        int(tm.group(1)), int(tm.group(2)), int(tm.group(3)))
    except ValueError:            # 2/30, 4/31 — calendar-impossible
        return None


def validate_bar(dt, o, h, lo, c, vol, window=None, *,
                 allow_zero_prices=False):
    """Contract checks for ONE bar. Returns a list of problem strings —
    empty means the bar may be written. Deliberately RELATIVE-only rules
    (no absolute price/volume bounds): BRK.A at $700k and a $0.27 penny
    stock must both pass untouched.

    `window` is the (first_label, last_label) the timestamp must fall within,
    inclusive — it defaults to RTH (09:30:00-15:59:59). A -pre / -post file
    passes its own session window so extended-hours bars validate against the
    hours that file is supposed to hold, not RTH. Ratio-kind callers may
    explicitly allow zero OHLC; negative prices remain invalid."""
    first, last = window if window is not None else (RTH_FIRST, RTH_LAST)
    p = []
    if dt.weekday() > 4:
        p.append("weekend timestamp")
    if dt.microsecond:
        p.append("sub-second timestamp (the storage grid is whole seconds)")
    if not 1900 <= dt.year <= 2100:
        p.append(f"year {dt.year} outside the plausible data range")
    if not (first <= dt.time() <= last):
        p.append(f"outside session hours {first.strftime('%H:%M:%S')}-"
                 f"{last.strftime('%H:%M:%S')}")
    try:
        o, h, lo, c = float(o), float(h), float(lo), float(c)
    except (TypeError, ValueError):
        return ["non-numeric price"]
    for name, v in (("open", o), ("high", h), ("low", lo), ("close", c)):
        invalid = v < 0.0 if allow_zero_prices else v <= 0.0
        if v != v or invalid or v in (float("inf"), float("-inf")):
            qualifier = "non-negative" if allow_zero_prices else "positive"
            p.append(f"{name} not a {qualifier} finite price ({v!r})")
            return p
    if not (lo <= min(o, c) and max(o, c) <= h):
        p.append(f"impossible bar: low<=open/close<=high violated "
                 f"({o},{h},{lo},{c})")
    # Volume: anything that IS exactly an integer (incl. numpy int64 from
    # vendor parsing) passes via __index__; bools and floats do not.
    try:
        if isinstance(vol, bool):
            raise TypeError
        if operator.index(vol) < 0:
            p.append(f"volume must be non-negative (got {vol!r})")
    except TypeError:
        p.append(f"volume must be an integer (got {vol!r})")
    return p


def bucket_bars_by_month(bars):
    """{(year, month): [bars...]} — derived from EACH BAR'S OWN timestamp,
    never from batch metadata, so a fetch spanning Dec 31 -> Jan 2 lands in
    two buckets and a misrouted bar is structurally impossible."""
    out = {}
    for b in bars:
        out.setdefault((b[0].year, b[0].month), []).append(b)
    for v in out.values():
        v.sort(key=lambda b: b[0])
    return out


# --- month-file writer / reader ----------------------------------------------

_TMP_COUNTER = itertools.count()


def _atomic_write_bytes(target, payload):
    """temp-in-same-dir + fsync + size check + os.replace with backoff for
    the open-in-Excel / AV-lock PermissionError. Same-directory temp keeps
    os.replace on one volume (cross-volume replace is not atomic). The
    temp name is unique PER CALL (pid + thread id + counter): a per-pid
    name let two threads truncate each other's temp and commit a torn
    hybrid over good data — proven by the adversarial review."""
    target = Path(target)
    if target.is_dir():
        raise StorageError(f"{target.name} is a DIRECTORY where a file "
                           f"belongs — remove it")
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(
        f"{target.name}.{os.getpid()}-{threading.get_ident()}-"
        f"{next(_TMP_COUNTER)}.tmp")
    try:
        with open(tmp, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        if tmp.stat().st_size != len(payload):
            raise StorageError(f"short write on {tmp.name}")
        delay = _REPLACE_BACKOFF_S
        for attempt in range(_REPLACE_ATTEMPTS):
            try:
                os.replace(tmp, target)
                return
            except PermissionError:
                if attempt == _REPLACE_ATTEMPTS - 1:
                    raise StorageError(
                        f"{target.name} is locked (open in Excel or being "
                        f"scanned?) — close it and retry") from None
                _time.sleep(delay)
                delay *= 2
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _require_pyarrow():
    """pyarrow is REQUIRED for the Parquet store; a clear error beats an
    obscure ImportError surfacing deep inside a write."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        return pa, pq
    except Exception as exc:  # noqa: BLE001
        raise StorageError(
            "Parquet storage needs pyarrow (pip install pyarrow)") from exc


def _bars_to_parquet(bars):
    """Serialize bars [(naive_dt, o,h,l,c, vol)] -> Parquet bytes (zstd). Typed
    schema: ts=timestamp[s] (naive US/Eastern), OHLC=float64, volume=int64 —
    LOSSLESS for the bar contract (whole-second datetimes, IEEE-double prices),
    so a read-back reconstructs identical tuples and format_price() of the stored
    floats reproduces the exact canonical CSV on export."""
    pa, pq = _require_pyarrow()
    import io
    cols = list(zip(*bars)) if bars else ([], [], [], [], [], [])
    tbl = pa.table({
        "ts": pa.array(list(cols[0]), pa.timestamp("s")),
        "open": pa.array([float(x) for x in cols[1]], pa.float64()),
        "high": pa.array([float(x) for x in cols[2]], pa.float64()),
        "low": pa.array([float(x) for x in cols[3]], pa.float64()),
        "close": pa.array([float(x) for x in cols[4]], pa.float64()),
        "volume": pa.array([int(x) for x in cols[5]], pa.int64()),
    })
    buf = io.BytesIO()
    pq.write_table(tbl, buf, compression="zstd")
    return buf.getvalue()


def _parse_parquet_payload(raw, name, window=None, validate=True, *,
                           allow_zero_prices=False):
    """Strict Parquet bytes -> bars, mirroring _parse_month_payload's contract
    (validate_bar against `window`, strictly-increasing unique timestamps). Any
    drift raises StorageFormatError tagged with `name`.

    validate=False -> TRUSTED fast path: after the schema check, zip the already-
    typed columns straight to bars, skipping the per-bar re-validation. Only for our
    OWN bank files (written + verify_codec'd, sha in the manifest) on a hot read."""
    pa, pq = _require_pyarrow()
    import io
    try:
        tbl = pq.read_table(io.BytesIO(raw))
    except Exception as exc:  # noqa: BLE001
        raise StorageFormatError(f"{name}: unreadable Parquet ({exc})") from None
    if list(tbl.column_names) != _PARQUET_COLS:
        raise StorageFormatError(
            f"{name}: schema {list(tbl.column_names)} != {_PARQUET_COLS}")
    ts = tbl.column("ts").to_pylist()
    o = tbl.column("open").to_pylist()
    h = tbl.column("high").to_pylist()
    lo = tbl.column("low").to_pylist()
    c = tbl.column("close").to_pylist()
    v = tbl.column("volume").to_pylist()
    if not validate:                     # TRUSTED read -> skip per-bar re-validation
        return list(zip(ts, o, h, lo, c, v))
    bars, problems, prev = [], [], None
    for i in range(len(ts)):
        dt = ts[i]
        if dt is None:
            problems.append(f"row {i + 1}: null timestamp")
            continue
        if getattr(dt, "tzinfo", None) is not None:
            problems.append(f"row {i + 1}: tz-aware timestamp (must be naive "
                            f"US/Eastern)")
            continue
        try:
            bar = (dt, float(o[i]), float(h[i]), float(lo[i]), float(c[i]),
                   int(v[i]))
        except (TypeError, ValueError):
            problems.append(f"row {i + 1}: non-numeric cell")
            continue
        bad = validate_bar(*bar, window=window,
                           allow_zero_prices=allow_zero_prices)
        if bad:
            problems.append(f"row {i + 1}: " + "; ".join(bad))
            continue
        if prev is not None and dt <= prev:
            problems.append(f"row {i + 1}: timestamps not strictly increasing")
        prev = dt
        bars.append(bar)
        if len(problems) >= 20:
            problems.append("... (more suppressed)")
            break
    if problems:
        raise StorageFormatError(f"{name}: {len(problems)} format problem(s)",
                                 problems)
    return bars


def write_month_file(path, bars, verify_after_write=False,
                     verify_codec=True):
    """Serialize ONE month of bars to `path` atomically; returns the stats
    dict the manifest stores. Strict by design — a violating bar means the
    CALLER's job (filter/quarantine) was not done, so this refuses rather
    than silently dropping data:
      * every bar passes validate_bar (RTH, intra-bar sanity, int volume)
      * all bars in one calendar month, matching the filename
      * strictly unique timestamps (dedupe is ingest's job, not the writer's)

    verify_codec (DEFAULT ON — F2 fidelity): the serialized payload is parsed
    back through the SAME strict reader IN MEMORY and compared to the bars
    given; a mismatch raises StorageError BEFORE anything hits disk, so the
    stored bytes are guaranteed to decode to exactly what was received (no
    precision/format/ordering drift). CPU-only — no extra disk read.

    verify_after_write (DEFAULT OFF — keeps the historical fast path/perf):
    when True, the just-committed file is re-read from disk, its bytes
    hashed, and compared to the in-memory payload sha; a mismatch (a torn
    write, a sync-conflict fork, an AV mangling the bytes between replace
    and now) raises StorageError instead of returning a stats dict that
    claims a hash the file on disk does not actually have."""
    path = Path(path)
    fm = FILENAME_RE.match(path.name)
    if not fm:
        raise StorageError(f"filename {path.name!r} violates the naming "
                           f"contract")
    if not bars:
        raise StorageError("refusing to write an empty month file")
    bars = sorted(bars, key=lambda b: b[0])
    problems = []
    want_ym = (int(fm.group(2)), int(fm.group(3)))
    interval = fm.group(4)
    window = session_window(interval)         # validate against THIS file's hours
    allow_zero_prices = kind_of(interval) in RATIO_KINDS
    prev = None
    for b in bars:
        dt = b[0]
        if (dt.year, dt.month) != want_ym:
            problems.append(f"{dt}: bar belongs to "
                            f"{month_key(dt.year, dt.month)}, file is "
                            f"{month_key(*want_ym)}")
        if prev is not None and dt == prev:
            problems.append(f"{dt}: duplicate timestamp")
        prev = dt
        problems.extend(
            f"{dt}: {msg}" for msg in validate_bar(
                *b, window=window, allow_zero_prices=allow_zero_prices))
        if len(problems) >= 20:
            problems.append("... (more suppressed)")
            break
    if problems:
        raise StorageFormatError(
            f"{path.name}: {len(problems)} contract violation(s) — nothing "
            f"written", problems)
    fmt = fm.group(5)
    if fmt == "parquet":
        payload = _bars_to_parquet(bars)
    else:
        lines = [HEADER]
        if allow_zero_prices:
            lines.extend(format_bar(*b, allow_zero_prices=True) for b in bars)
        else:
            # Preserve the original formatter call contract for ordinary price
            # files (including test/extension wrappers around format_bar).
            lines.extend(format_bar(*b) for b in bars)
        payload = (EOL.join(lines) + EOL).encode("ascii")
    payload_sha = hashlib.sha256(payload).hexdigest()
    if verify_codec:
        # F2 — in-memory CODEC round-trip: decode the payload we just built back
        # through the SAME strict reader and confirm it equals the bars we were
        # given. BOTH codecs are EXACT identities (CSV via repr() shortest
        # round-trip; Parquet via typed second-timestamp/float64/int64), so any
        # mismatch is a real codec bug, caught BEFORE a byte hits disk.
        rt = (_parse_parquet_payload if fmt == "parquet"
              else _parse_month_payload)(
            payload, f"{path.name} (codec round-trip)", window=window,
            allow_zero_prices=allow_zero_prices)
        if rt != bars:
            i = next((k for k in range(min(len(rt), len(bars)))
                      if rt[k] != bars[k]), min(len(rt), len(bars)))
            raise StorageError(
                f"{path.name}: CODEC ROUND-TRIP MISMATCH — serialized bytes "
                f"do not decode back to the bars given ({len(bars)} in, "
                f"{len(rt)} out; first diff at index {i}); refusing to write")
    _atomic_write_bytes(path, payload)
    if verify_after_write:
        # Read-back the bytes we just committed and confirm they still hash
        # to the payload we meant to store (raw bytes, NOT the strict parse
        # — a scrub is about the bytes on disk, not their re-validation).
        disk_sha = _sha256_of_file(path)
        if disk_sha is None:
            raise StorageError(f"{path.name}: verify-after-write could not "
                               f"re-read the file just committed")
        if disk_sha != payload_sha:
            raise StorageError(
                f"{path.name}: verify-after-write MISMATCH — bytes on disk "
                f"({disk_sha[:12]}...) differ from the payload written "
                f"({payload_sha[:12]}...); the file may be torn or forked "
                f"by a sync/AV tool")
    return {"rows": len(bars),
            "first": f"{format_date(bars[0][0])} "
                     f"{format_time(bars[0][0].time())}",
            "last": f"{format_date(bars[-1][0])} "
                    f"{format_time(bars[-1][0].time())}",
            "size": len(payload),
            "sha256": payload_sha,
            "mtime_ns": Path(path).stat().st_mtime_ns}


def read_month_file(path, validate=True):
    """Strict read-back: returns (bars, stats). ANY drift from the contract
    raises StorageFormatError with line numbers — a file that Excel resaved
    (padded dates, LF endings, '28.20'-style non-canonical numeric text)
    must be reported, never silently adopted. A transient sharing violation
    (AV/backup tools) is retried briefly, and the stat/read pair is taken
    consistently so the returned stats always describe the bytes parsed."""
    path = Path(path)
    raw = st = None
    delay = 0.1
    for attempt in range(4):
        try:
            st0 = path.stat()
            raw = path.read_bytes()
            st = path.stat()
            if st.st_mtime_ns == st0.st_mtime_ns:
                break                  # bytes and stats are one version
        except PermissionError:
            if attempt == 3:
                raise StorageError(
                    f"{path.name} is locked (AV/backup tool?) — retry "
                    f"later") from None
        _time.sleep(delay)
        delay *= 2
    else:
        raise StorageError(f"{path.name}: file kept changing during read")
    fm = FILENAME_RE.match(path.name)
    allow_zero_prices = bool(
        fm and kind_of(fm.group(4)) in RATIO_KINDS)
    if path.suffix == ".parquet":
        bars = _parse_parquet_payload(
            raw, path.name, window=_window_for_name(path.name), validate=validate,
            allow_zero_prices=allow_zero_prices)
    else:                                # CSV stays strict (also the F2 codec path)
        bars = _parse_month_payload(raw, path.name,
                                    window=_window_for_name(path.name),
                                    allow_zero_prices=allow_zero_prices)
    return bars, {"rows": len(bars), "size": len(raw),
                  "sha256": hashlib.sha256(raw).hexdigest(),
                  "mtime_ns": st.st_mtime_ns}


def read_month_file_fast(path, manifest_sha=None):
    """Hot TRUSTED read for our OWN bank files. Parses WITHOUT the per-bar
    re-validation, then verifies the file's sha against `manifest_sha`:
      * match  -> the file is byte-for-byte what we validated + hashed on write,
                  so the fast parse is trustworthy (~1.8x faster than strict).
      * mismatch (or no expected sha) -> fall back to the STRICT read, so an
                  externally-modified/corrupt file is STILL surfaced, never exported.
    Returns (bars, stats), identical shape to read_month_file. 'Accurate first':
    the sha gate means speed is taken ONLY when the bytes are provably unchanged."""
    if manifest_sha is None:
        return read_month_file(path, validate=True)
    bars, stats = read_month_file(path, validate=False)
    if stats.get("sha256") != manifest_sha:
        return read_month_file(path, validate=True)   # changed -> re-validate/surface
    return bars, stats


_TS_EPOCH = datetime(1970, 1, 1)      # naive epoch anchor for the ts-only read


def _bars_to_column_arrays(bars):
    import numpy as np
    if not bars:
        z = np.array([], dtype=np.int64)
        f = np.array([], dtype=np.float64)
        return z, f, f.copy(), f.copy(), f.copy(), z.copy()
    ts = np.array([int((b[0] - _TS_EPOCH).total_seconds()) for b in bars],
                  dtype=np.int64)
    return (ts,
            np.array([b[1] for b in bars], dtype=np.float64),
            np.array([b[2] for b in bars], dtype=np.float64),
            np.array([b[3] for b in bars], dtype=np.float64),
            np.array([b[4] for b in bars], dtype=np.float64),
            np.array([b[5] for b in bars], dtype=np.int64))


def read_month_file_ts(path, manifest_sha=None):
    """TIMESTAMP-ONLY hot read for the gap scans, which consume ONLY bar times
    (never o/h/l/c/v). Returns the month's timestamps as a list of int EPOCH
    SECONDS of the stored NAIVE-Eastern wall stamps — the exact ints the parquet
    ts column holds, so `sec // 86400` groups by calendar day bit-exactly like
    datetime.date() (DST days included: the grid is naive wall time, no tz).

    Gate ('accurate first', same bar as read_month_file_fast): the fast parse is
    taken ONLY when the file is parquet AND its bytes sha256-match `manifest_sha`
    (byte-for-byte what was validated + hashed at write time). No sha, a CSV
    month, a sha mismatch, or ANY parse drift (schema, nulls, tz-aware stamps,
    sub-second values — the safe cast refuses truncation) falls back to the
    STRICT read_month_file, so corruption still surfaces identically; the strict
    bars are then reduced to the same epoch ints. Skipping the o/h/l/c/v decode
    + per-bar re-validation is ~10-30x on a 1m month."""
    path = Path(path)
    if manifest_sha and path.suffix == ".parquet":
        try:
            raw = path.read_bytes()
        except OSError:
            raw = None                    # locked/missing -> strict path decides
        if raw is not None and hashlib.sha256(raw).hexdigest() == manifest_sha:
            try:
                pa, pq = _require_pyarrow()
                import io
                pf = pq.ParquetFile(io.BytesIO(raw))
                if pf.schema_arrow.names == _PARQUET_COLS:
                    col = pf.read(columns=["ts"]).column("ts").combine_chunks()
                    if not col.null_count and col.type.tz is None:
                        # stored as timestamp('s') but parquet materializes
                        # ms/us; the SAFE cast back to seconds raises on any
                        # sub-second value -> strict fallback surfaces it.
                        return (col.cast(pa.timestamp("s")).cast(pa.int64())
                                .to_pylist())
            except Exception:  # noqa: BLE001 — any drift -> strict fallback
                pass
    bars, _stats = read_month_file(path, validate=True)      # STRICT fallback
    return [int((b[0] - _TS_EPOCH).total_seconds()) for b in bars]


def read_month_file_cols(path, manifest_sha=None, *, validate=True):
    """COLUMNAR hot read for cross-validation daily derivation.

    Returns `(ts_i64, open_f64, high_f64, low_f64, close_f64, volume_i64)`
    numpy arrays. The fast path is taken only for a Parquet file whose raw bytes
    match the manifest sha and whose schema/null/timestamp contract is intact.
    No sha, CSV, sha mismatch, or any parse drift falls back to
    `read_month_file(validate=validate)` and then converts those tuples to
    arrays.  The default remains strict; report-only invariant auditors may
    opt out so malformed values can be surfaced as findings rather than lost
    behind a whole-month parse failure.
    """
    path = Path(path)
    if manifest_sha and path.suffix == ".parquet":
        try:
            raw = path.read_bytes()
        except OSError:
            raw = None
        if raw is not None and hashlib.sha256(raw).hexdigest() == manifest_sha:
            try:
                pa, pq = _require_pyarrow()
                import io
                pf = pq.ParquetFile(io.BytesIO(raw))
                if pf.schema_arrow.names == _PARQUET_COLS:
                    tbl = pf.read(columns=_PARQUET_COLS)
                    chunks = {name: tbl.column(name).combine_chunks()
                              for name in _PARQUET_COLS}
                    if all(not col.null_count for col in chunks.values()):
                        ts = chunks["ts"]
                        if ts.type.tz is None:
                            return (
                                ts.cast(pa.timestamp("s")).cast(pa.int64())
                                  .to_numpy(zero_copy_only=False),
                                chunks["open"].to_numpy(zero_copy_only=False),
                                chunks["high"].to_numpy(zero_copy_only=False),
                                chunks["low"].to_numpy(zero_copy_only=False),
                                chunks["close"].to_numpy(zero_copy_only=False),
                                chunks["volume"].to_numpy(zero_copy_only=False),
                            )
            except Exception:  # noqa: BLE001 - any drift -> strict fallback
                pass
    bars, _stats = read_month_file(path, validate=validate)
    return _bars_to_column_arrays(bars)


def _parse_month_payload(raw, name, window=None, *, allow_zero_prices=False):
    """Strict bytes -> bars parse, SHARED by read_month_file (a file on disk)
    and write_month_file's in-memory codec round-trip (F2). Any drift from
    the schema raises StorageFormatError tagged with `name` (line numbers
    preserved); returns the bars. Pure: no I/O, no stats. `window` is the
    session hours the bars are validated against (None -> RTH default)."""
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise StorageFormatError(
            f"{name}: non-ASCII bytes (encoding drift): {exc}") from None
    if raw.count(b"\n") != raw.count(b"\r\n") or not raw.endswith(b"\r\n"):
        raise StorageFormatError(
            f"{name}: line endings are not CRLF-with-trailing-newline "
            f"(modified externally?)")
    lines = text.split(EOL)
    if lines and lines[-1] == "":
        lines.pop()
    if not lines or lines[0] != HEADER:
        raise StorageFormatError(
            f"{name}: header is {lines[0]!r} — contract requires "
            f"{HEADER!r}")
    bars, problems = [], []
    prev = None
    for i, line in enumerate(lines[1:], start=2):
        cells = line.split(",")
        if len(cells) != 7:
            problems.append(f"line {i}: {len(cells)} fields")
            continue
        d_s, t_s, o_s, h_s, l_s, c_s, v_s = cells
        dt = parse_timestamp(d_s, t_s)
        if dt is None:
            problems.append(f"line {i}: bad Date/Time {d_s!r} {t_s!r}")
            continue
        if not all(_PRICE_RE.match(x) for x in (o_s, h_s, l_s, c_s)):
            problems.append(f"line {i}: off-format price")
            continue
        try:
            noncanon = any(format_price(
                               float(x), allow_zero=allow_zero_prices) != x
                           for x in (o_s, h_s, l_s, c_s))
        except StorageFormatError:        # non-positive under this kind policy
            problems.append(f"line {i}: non-positive price")
            continue
        if noncanon:
            problems.append(f"line {i}: non-canonical price text (e.g. "
                            f"'28.20' for 28.2) — modified externally?")
            continue
        if not _VOLUME_RE.match(v_s):
            problems.append(f"line {i}: off-format volume {v_s!r}")
            continue
        bar = (dt, float(o_s), float(h_s), float(l_s), float(c_s), int(v_s))
        bad = validate_bar(*bar, window=window,
                           allow_zero_prices=allow_zero_prices)
        if bad:
            problems.append(f"line {i}: " + "; ".join(bad))
            continue
        if prev is not None and dt <= prev:
            problems.append(f"line {i}: timestamps not strictly increasing")
        prev = dt
        bars.append(bar)
        if len(problems) >= 20:
            problems.append("... (more suppressed)")
            break
    if problems:
        raise StorageFormatError(
            f"{name}: {len(problems)} format problem(s)", problems)
    return bars


# --- manifest -----------------------------------------------------------------

def new_manifest(symbol, folder):
    return {"symbol": str(symbol), "folder": folder, "generation": 0,
            "conid": None, "aliases": [], "basis": "unknown",
            "written_by": dict(WRITTEN_BY), "intervals": {}}


def _salvage_manifest_fields(raw_text):
    """Best-effort regex recovery of the fields a rebuild-by-scan can NOT
    reproduce from the tree (symbol/conid are primary data, not cache) out
    of a corrupt manifest's text."""
    out = {}
    m = re.search(r'"symbol"\s*:\s*"([^"]{1,20})"', raw_text)
    if m:
        out["symbol"] = m.group(1)
    m = re.search(r'"conid"\s*:\s*(\d{1,12})', raw_text)
    if m:
        out["conid"] = int(m.group(1))
    return out


def load_manifest(ticker_dir):
    """The manifest dict, or None when absent/corrupt/mis-shaped (caller
    rebuilds by scan — the TREE is the source of truth, the manifest is a
    cache). Shape is validated DEEPLY: a hand-edited manifest with, say,
    "intervals": null is corruption, and returning it would crash every
    consumer — the adversarial review proved one such file used to take
    the whole scan (and the UI inventory) down."""
    p = Path(ticker_dir) / MANIFEST_NAME
    if p.is_dir():
        return None
    try:
        with open(p, "r", encoding="utf-8") as fh:
            m = json.load(fh)
    except (OSError, ValueError):
        return None
    if not isinstance(m, dict):
        return None
    ivs = m.get("intervals")
    if not isinstance(ivs, dict):
        return None
    for ivd in ivs.values():
        if not isinstance(ivd, dict):
            return None
        months = ivd.get("months")
        if not isinstance(months, dict):
            return None
        if not all(isinstance(e, dict) for e in months.values()):
            return None
        if not isinstance(ivd.get("verified_absent", []), list):
            return None
    return m


def save_manifest(ticker_dir, manifest):
    """Atomic manifest write; bumps the generation counter (the backup
    -restored-over-newer-tree detector keys off this). COMPACT separators:
    indent=1 whitespace was measured at 31% of the bank's 86 MB manifest
    weight (~27 MB) — pure padding parsed on every scan/refresh. Existing
    pretty-printed manifests stay readable until their next natural rewrite
    (json.loads accepts both, no migration needed)."""
    manifest["generation"] = int(manifest.get("generation", 0)) + 1
    manifest["written_by"] = dict(WRITTEN_BY)
    payload = json.dumps(manifest, sort_keys=True,
                         separators=(",", ":")).encode("utf-8")
    _atomic_write_bytes(Path(ticker_dir) / MANIFEST_NAME, payload)


# --- correction-ledger-preserving ordinary writes -----------------------------

_ORDINARY_TXN_SENTINEL_PAYLOAD = json.dumps(
    {"kind": "ordinary_correction_write",
     "version": _ORDINARY_CORRECTION_TXN_VERSION},
    sort_keys=True, separators=(",", ":")).encode("utf-8")
_ORDINARY_TXN_MANIFEST_BEFORE = "manifest.before"
_ORDINARY_TXN_MONTH_BEFORE = "month.before"
_ORDINARY_TXN_MONTH_AFTER = "month.after"
_ORDINARY_TXN_MARKER_FIELDS = frozenset({
    "version", "kind", "ticker", "interval", "month", "old_rel",
    "write_rel", "candidate_name", "old_month_sha256",
    "new_month_sha256", "manifest_before_sha256",
    "manifest_after_sha256", "ledger_sha256",
})


def _ordinary_correction_txn_hook(_phase):
    """Offline-test seam for real hard-exit phase coverage."""


def _ordinary_sha_bytes(payload):
    return hashlib.sha256(payload).hexdigest()


def _ordinary_ledger_sha(ledger):
    try:
        raw = json.dumps(
            ledger, sort_keys=True, separators=(",", ":"),
            allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise StorageError(
            "value-correction ledger is not safely serializable") from exc
    return _ordinary_sha_bytes(raw)


def _ordinary_manifest_entry(manifest, interval, key):
    try:
        return manifest["intervals"][interval]["months"][key]
    except (KeyError, TypeError) as exc:
        raise StorageError(
            f"manifest has no usable {interval} {key} month entry") from exc


def _ordinary_correction_entries(manifest):
    """Map every correction-bearing manifest slot to its exact SHA/ledger."""
    try:
        intervals = manifest["intervals"]
    except (KeyError, TypeError) as exc:
        raise StorageError(
            "ordinary correction transaction manifest is malformed") from exc
    if not isinstance(intervals, dict):
        raise StorageError(
            "ordinary correction transaction manifest intervals are malformed")
    out = {}
    for interval, section in intervals.items():
        months = section.get("months") if isinstance(section, dict) else None
        if not isinstance(months, dict):
            raise StorageError(
                "ordinary correction transaction manifest months are "
                "malformed")
        for key, entry in months.items():
            if not isinstance(entry, dict) or "value_corrections" not in entry:
                continue
            sha = entry.get("sha256")
            ledger = entry.get("value_corrections")
            if (not isinstance(sha, str)
                    or re.fullmatch(r"[0-9a-f]{64}", sha) is None
                    or not isinstance(ledger, list) or not ledger):
                raise StorageError(
                    f"ordinary correction transaction has malformed "
                    f"correction evidence at {interval} {key}")
            # Serialization validation also rejects NaN/unsupported objects.
            _ordinary_ledger_sha(ledger)
            out[(interval, key)] = (sha, deepcopy(ledger))
    return out


def _validate_ordinary_correction_transition(
        before, after, target, old_sha, new_sha):
    """Require target SHA advance and identity for every correction ledger."""
    if set(before) != set(after) or target not in before:
        raise StorageError(
            "ordinary correction candidate changed correction-ledger slots")
    for slot, (before_sha, before_ledger) in before.items():
        after_sha, after_ledger = after[slot]
        expected_before = old_sha if slot == target else before_sha
        expected_after = new_sha if slot == target else before_sha
        if (before_sha != expected_before or after_sha != expected_after
                or after_ledger != before_ledger):
            label = "target" if slot == target else "sibling"
            raise StorageError(
                f"ordinary correction candidate changed {label} correction "
                f"evidence at {slot[0]} {slot[1]}")


def _ordinary_read_file(path, label):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise StorageError(f"ordinary correction transaction lacks {label}")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise StorageError(
            f"ordinary correction transaction cannot read {label}") from exc


def _ordinary_owned_temp(name, owned):
    return any(re.fullmatch(
        re.escape(target) + r"\.\d+-\d+-\d+\.tmp\Z", name)
               is not None for target in owned)


def _clear_ordinary_correction_stage(stage_dir, candidate_name=None):
    """Remove only files owned by one validated ordinary transaction.

    The transaction marker is removed first.  A hard exit after that point
    leaves the ordinary sentinel, so the next ticker transaction recognizes
    the directory as markerless completed/pre-publication cleanup and removes
    only its exact allowlist.
    """
    stage_dir = Path(stage_dir)
    if not stage_dir.exists() and not stage_dir.is_symlink():
        return
    if stage_dir.is_symlink() or not stage_dir.is_dir():
        raise StorageError(
            "ordinary correction transaction staging path is unsafe")
    owned = {
        ORDINARY_CORRECTION_TXN_SENTINEL,
        ORDINARY_CORRECTION_TXN_MARKER,
        _ORDINARY_TXN_MANIFEST_BEFORE,
        MANIFEST_NAME,
        _ORDINARY_TXN_MONTH_BEFORE,
        _ORDINARY_TXN_MONTH_AFTER,
    }
    if candidate_name:
        owned.add(str(candidate_name))
    try:
        entries = list(stage_dir.iterdir())
        for entry in entries:
            if (entry.is_symlink() or not entry.is_file()
                    or (entry.name not in owned
                        and not _ordinary_owned_temp(entry.name, owned))):
                raise StorageError(
                    "ordinary correction transaction stage has foreign "
                    "content")
        sentinel = stage_dir / ORDINARY_CORRECTION_TXN_SENTINEL
        if _ordinary_read_file(sentinel, "ordinary sentinel") \
                != _ORDINARY_TXN_SENTINEL_PAYLOAD:
            raise StorageError(
                "ordinary correction transaction sentinel is malformed")
        marker = stage_dir / ORDINARY_CORRECTION_TXN_MARKER
        if marker.exists():
            marker.unlink()
        _ordinary_correction_txn_hook("cleanup_marker")
        for entry in entries:
            if entry not in {marker, sentinel}:
                entry.unlink()
        # Keep the owner sentinel until every backup/candidate is gone.  A
        # hard exit during cleanup therefore remains attributable; the only
        # unavoidable post-sentinel window is an empty directory, which the
        # recovery entry point safely removes for either WAL owner.
        sentinel.unlink()
        stage_dir.rmdir()
    except StorageError:
        raise
    except OSError as exc:
        raise StorageError(
            "ordinary correction transaction stage could not be cleared") \
            from exc


def _ordinary_resolve_rel(ticker_dir, value, label):
    if (not isinstance(value, str) or not value or "\\" in value
            or Path(value).is_absolute()):
        raise StorageError(
            f"ordinary correction transaction {label} path is invalid")
    ticker_dir = Path(ticker_dir).resolve(strict=False)
    path = (ticker_dir / Path(value)).resolve(strict=False)
    if not path.is_relative_to(ticker_dir):
        raise StorageError(
            f"ordinary correction transaction {label} path escapes ticker")
    return path


def _ordinary_canonical_month_paths(ticker_dir, interval, key):
    """Return the only paths one ordinary month transaction may own."""
    ticker_dir = Path(ticker_dir).resolve(strict=False)
    year, month = int(key[:4]), int(key[5:7])
    root = ticker_dir.parent
    csv_path = month_file_path(
        root, ticker_dir.name, year, month, interval,
        fmt="csv").resolve(strict=False)
    parquet_path = month_file_path(
        root, ticker_dir.name, year, month, interval,
        fmt="parquet").resolve(strict=False)
    return csv_path, parquet_path


def _ordinary_backup_month_suffix(payload):
    """Identify the exact codec carried by a staged prewrite month.

    The marker is recoverable state, not authority for the old file format.
    Bind ``old_rel`` to the hash-verified backup bytes so changing a CSV-origin
    marker to the canonical Parquet target (or the inverse) cannot bypass twin
    retirement or make recovery act on the wrong slot.
    """
    if not isinstance(payload, bytes):
        raise StorageError(
            "ordinary correction old month backup is not bytes")
    csv_header = (HEADER + EOL).encode("ascii")
    csv_eol = EOL.encode("ascii")
    is_csv = payload.startswith(csv_header) and payload.endswith(csv_eol)
    is_parquet = (len(payload) >= 8 and payload[:4] == b"PAR1"
                  and payload[-4:] == b"PAR1")
    if is_csv == is_parquet:
        raise StorageError(
            "ordinary correction old month backup format is ambiguous")
    return ".csv" if is_csv else ".parquet"


def _retire_exact_ordinary_csv(path, expected_sha):
    """Delete a legacy twin only while it is the snapshotted old file."""
    path = Path(path)
    if not path.exists() and not path.is_symlink():
        return
    if (path.is_symlink() or not path.is_file()
            or _sha256_of_file(path) != expected_sha):
        raise StorageError(
            "ordinary correction legacy CSV changed while recovery was "
            "pending; recovery remains pending")
    try:
        path.unlink()
    except OSError as exc:
        raise StorageError(
            "ordinary correction transaction could not retire CSV twin; "
            "recovery remains pending") from exc


def _ordinary_load_json(raw, label):
    def unique(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError(f"duplicate key {key!r}")
            out[key] = value
        return out

    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
    except (UnicodeError, ValueError) as exc:
        raise StorageError(
            f"ordinary correction transaction {label} is malformed") from exc


def recover_ordinary_correction_transaction(ticker_dir):
    """Recover only an ordinary correction-ledger-preserving month WAL.

    A reconcile WAL deliberately has no ordinary sentinel and is returned to
    its owner untouched.  Recovery accepts only exact old/old and new/new
    states, or the two atomic one-step intermediates, and then resolves them
    to one coherent pair.  A legacy CSV twin is retired before the marker is
    cleared; an unlink failure therefore remains visible recovery debt.
    """
    ticker_dir = Path(ticker_dir).resolve(strict=False)
    stage_dir = ticker_dir / VOL_VALUE_RECONCILE_STAGE_DIR
    sentinel = stage_dir / ORDINARY_CORRECTION_TXN_SENTINEL
    if not stage_dir.exists() and not stage_dir.is_symlink():
        return None
    if not sentinel.exists() and not sentinel.is_symlink():
        if stage_dir.is_symlink() or not stage_dir.is_dir():
            return None                     # unsafe path: reconcile owner errs
        try:
            entries = list(stage_dir.iterdir())
        except OSError:
            return None
        if not entries:
            # Both WALs define an empty markerless stage as unpublished (or
            # the final instruction-window after owned cleanup). No bank byte
            # can depend on it.
            try:
                stage_dir.rmdir()
            except OSError as exc:
                raise StorageError(
                    "empty correction transaction stage could not be "
                    "cleared") from exc
            return {"status": "cleanup", "ticker": ticker_dir.name}
        sentinel_temps = [
            entry for entry in entries
            if (entry.is_file() and not entry.is_symlink()
                and re.fullmatch(
                    re.escape(ORDINARY_CORRECTION_TXN_SENTINEL)
                    + r"\.\d+-\d+-\d+\.tmp\Z", entry.name))]
        if len(sentinel_temps) == len(entries):
            # Hard death during the first atomic sentinel write. Authority was
            # not touched and this filename is foreign to the reconcile WAL.
            try:
                for entry in sentinel_temps:
                    entry.unlink()
                stage_dir.rmdir()
            except OSError as exc:
                raise StorageError(
                    "ordinary correction sentinel temp could not be "
                    "cleared") from exc
            return {"status": "cleanup", "ticker": ticker_dir.name}
        return None                         # reconcile WAL: different owner
    if stage_dir.is_symlink() or not stage_dir.is_dir():
        raise StorageError(
            "ordinary correction transaction staging path is unsafe")
    if _ordinary_read_file(sentinel, "ordinary sentinel") \
            != _ORDINARY_TXN_SENTINEL_PAYLOAD:
        raise StorageError(
            "ordinary correction transaction sentinel is malformed")

    marker_path = stage_dir / ORDINARY_CORRECTION_TXN_MARKER
    if not marker_path.exists():
        candidates = set()
        try:
            for entry in stage_dir.iterdir():
                if entry.is_file() and not entry.is_symlink() \
                        and FILENAME_RE.fullmatch(entry.name):
                    candidates.add(entry.name)
                    continue
                match = re.fullmatch(
                    r"(.+)\.\d+-\d+-\d+\.tmp\Z", entry.name)
                if (entry.is_file() and not entry.is_symlink()
                        and match is not None
                        and FILENAME_RE.fullmatch(match.group(1))):
                    candidates.add(match.group(1))
        except OSError as exc:
            raise StorageError(
                "ordinary correction transaction stage is unreadable") from exc
        if len(candidates) > 1:
            raise StorageError(
                "ordinary correction transaction has multiple candidates")
        _clear_ordinary_correction_stage(
            stage_dir, next(iter(candidates), None))
        return {"status": "cleanup", "ticker": ticker_dir.name}

    marker = _ordinary_load_json(
        _ordinary_read_file(marker_path, "transaction marker"), "marker")
    if (not isinstance(marker, dict)
            or set(marker) != _ORDINARY_TXN_MARKER_FIELDS
            or marker.get("version") != _ORDINARY_CORRECTION_TXN_VERSION
            or marker.get("kind") != "ordinary_correction_write"
            or marker.get("ticker") != ticker_dir.name):
        raise StorageError(
            "ordinary correction transaction marker fields are invalid")
    interval = marker.get("interval")
    key = marker.get("month")
    if (not isinstance(interval, str) or INTERVAL_RE.fullmatch(interval) is None
            or not isinstance(key, str)
            or re.fullmatch(r"[12]\d{3}-(?:0[1-9]|1[0-2])", key) is None):
        raise StorageError(
            "ordinary correction transaction month identity is invalid")
    candidate_name = marker.get("candidate_name")
    if (not isinstance(candidate_name, str)
            or FILENAME_RE.fullmatch(candidate_name) is None):
        raise StorageError(
            "ordinary correction transaction candidate is invalid")
    old_path = _ordinary_resolve_rel(
        ticker_dir, marker.get("old_rel"), "old month")
    write_path = _ordinary_resolve_rel(
        ticker_dir, marker.get("write_rel"), "write month")
    canonical_csv, canonical_parquet = _ordinary_canonical_month_paths(
        ticker_dir, interval, key)
    if (old_path not in {canonical_csv, canonical_parquet}
            or write_path != canonical_parquet
            or candidate_name != canonical_parquet.name):
        raise StorageError(
            "ordinary correction transaction month paths disagree with "
            "their exact canonical slot")
    old_sha = marker.get("old_month_sha256")
    new_sha = marker.get("new_month_sha256")
    before_manifest_sha = marker.get("manifest_before_sha256")
    after_manifest_sha = marker.get("manifest_after_sha256")
    ledger_sha = marker.get("ledger_sha256")
    for value, label in (
            (old_sha, "old month"), (new_sha, "new month"),
            (before_manifest_sha, "old manifest"),
            (after_manifest_sha, "new manifest"),
            (ledger_sha, "correction ledger")):
        if (not isinstance(value, str)
                or re.fullmatch(r"[0-9a-f]{64}", value) is None):
            raise StorageError(
                f"ordinary correction transaction {label} SHA is invalid")

    manifest_before = _ordinary_read_file(
        stage_dir / _ORDINARY_TXN_MANIFEST_BEFORE, "old manifest backup")
    manifest_after = _ordinary_read_file(
        stage_dir / MANIFEST_NAME, "new manifest backup")
    month_before = _ordinary_read_file(
        stage_dir / _ORDINARY_TXN_MONTH_BEFORE, "old month backup")
    month_after = _ordinary_read_file(
        stage_dir / _ORDINARY_TXN_MONTH_AFTER, "new month backup")
    if (_ordinary_sha_bytes(manifest_before) != before_manifest_sha
            or _ordinary_sha_bytes(manifest_after) != after_manifest_sha
            or _ordinary_sha_bytes(month_before) != old_sha
            or _ordinary_sha_bytes(month_after) != new_sha):
        raise StorageError(
            "ordinary correction transaction backup hash mismatch")
    if old_path.suffix.casefold() != _ordinary_backup_month_suffix(month_before):
        raise StorageError(
            "ordinary correction transaction old month format disagrees "
            "with its exact backup; recovery remains pending")
    before_payload = _ordinary_load_json(manifest_before, "old manifest")
    after_payload = _ordinary_load_json(manifest_after, "new manifest")
    before_corrections = _ordinary_correction_entries(before_payload)
    after_corrections = _ordinary_correction_entries(after_payload)
    _validate_ordinary_correction_transition(
        before_corrections, after_corrections,
        (interval, key), old_sha, new_sha)
    before_entry = _ordinary_manifest_entry(before_payload, interval, key)
    after_entry = _ordinary_manifest_entry(after_payload, interval, key)
    before_ledger = (before_entry.get("value_corrections")
                     if isinstance(before_entry, dict) else None)
    after_ledger = (after_entry.get("value_corrections")
                    if isinstance(after_entry, dict) else None)
    if (before_entry.get("sha256") != old_sha
            or after_entry.get("sha256") != new_sha
            or not isinstance(before_ledger, list) or not before_ledger
            or after_ledger != before_ledger
            or _ordinary_ledger_sha(before_ledger) != ledger_sha):
        raise StorageError(
            "ordinary correction transaction does not preserve its exact "
            "ledger")

    manifest_path = ticker_dir / MANIFEST_NAME
    current_manifest = _ordinary_read_file(manifest_path, "current manifest")
    old_file_sha = _sha256_of_file(old_path)
    new_file_sha = _sha256_of_file(write_path)
    if old_path == write_path:
        old_state = old_file_sha == old_sha
        new_state = new_file_sha == new_sha
    else:
        old_state = old_file_sha == old_sha and new_file_sha is None
        new_state = new_file_sha == new_sha
    manifest_state = (
        "before" if current_manifest == manifest_before else
        "after" if current_manifest == manifest_after else "other")
    if manifest_state == "after" and old_state:
        _atomic_write_bytes(manifest_path, manifest_before)
        if _ordinary_read_file(manifest_path, "restored manifest") \
                != manifest_before:
            raise StorageError(
                "ordinary correction transaction could not restore old "
                "manifest")
        outcome = "aborted"
    elif manifest_state == "before" and new_state:
        _atomic_write_bytes(manifest_path, manifest_after)
        if _ordinary_read_file(manifest_path, "completed manifest") \
                != manifest_after:
            raise StorageError(
                "ordinary correction transaction could not publish new "
                "manifest")
        outcome = "committed"
    elif manifest_state == "before" and old_state:
        outcome = "aborted"
    elif manifest_state == "after" and new_state:
        outcome = "committed"
    else:
        raise StorageError(
            "ordinary correction transaction state is indeterminate")

    if (outcome == "committed" and old_path != write_path
            and old_path.suffix.casefold() == ".csv"):
        _retire_exact_ordinary_csv(old_path, old_sha)
    _clear_ordinary_correction_stage(stage_dir, candidate_name)
    return {
        "status": outcome, "ticker": ticker_dir.name,
        "interval": interval, "month": key,
        "old_month_sha256": old_sha, "new_month_sha256": new_sha,
    }


def write_month_preserving_correction_ledger(
        root, ticker, interval, year, month, bars, manifest,
        entry_updates=None):
    """Atomically publish an ordinary rewrite of a corrected month.

    This path is valid only when the exact active prewrite month SHA is named
    by the current manifest and that entry owns a non-empty
    ``value_corrections`` list.  Both old/new month and manifest bytes are
    staged before a marker is made durable.  The manifest is published first,
    then the month; recovery accepts only exact atomic states and retains the
    identical ledger in either result.

    Returns ``(stats, published_manifest)``.  The caller may pass other safe
    manifest changes in ``manifest`` (for example a newly pinned conId or
    already-committed sibling months); this function exclusively replaces the
    target month entry using ``entry_updates`` plus computed stats and the
    exact ledger from the current manifest.
    """
    root = Path(root).resolve(strict=False)
    ticker = canonical_ticker(ticker)
    ticker_dir = (root / ticker).resolve(strict=False)
    key = month_key(year, month)
    if not isinstance(manifest, dict):
        raise StorageError("candidate manifest is not an object")
    updates = dict(entry_updates or {})
    forbidden = {
        "rows", "first", "last", "size", "mtime_ns", "sha256",
        "value_corrections",
    }
    if forbidden.intersection(updates):
        raise StorageError(
            "ordinary correction month updates contain owned fields")

    with ticker_transaction(ticker_dir):
        stage_dir = ticker_dir / VOL_VALUE_RECONCILE_STAGE_DIR
        if stage_dir.exists() or stage_dir.is_symlink():
            raise StorageError(
                "volatility correction recovery is pending; month write "
                "refused")
        old_path = find_month_file(root, ticker, year, month, interval)
        if old_path is None:
            raise StorageError(
                f"{ticker} {interval} {key}: corrected month is missing")
        old_path = Path(old_path).resolve(strict=False)
        try:
            old_month_bytes = old_path.read_bytes()
            manifest_path = ticker_dir / MANIFEST_NAME
            manifest_before = manifest_path.read_bytes()
        except OSError as exc:
            raise StorageError(
                "ordinary correction transaction cannot snapshot old state") \
                from exc
        old_sha = _ordinary_sha_bytes(old_month_bytes)
        # Derive every provenance decision from the exact bytes captured for
        # the rollback/CAS token. A second load could observe non-cooperating
        # external replacement and bind a ledger from different manifest
        # bytes even while cooperating writers hold the ticker lock.
        manifest_before_payload = _ordinary_load_json(
            manifest_before, "old manifest")
        before_corrections = _ordinary_correction_entries(
            manifest_before_payload)
        current_entry = _ordinary_manifest_entry(
            manifest_before_payload, interval, key)
        ledger = (current_entry.get("value_corrections")
                  if isinstance(current_entry, dict) else None)
        if (not isinstance(current_entry, dict)
                or current_entry.get("sha256") != old_sha
                or not isinstance(ledger, list) or not ledger):
            raise StorageError(
                f"{ticker} {interval} {key}: correction ledger does not "
                "match the active prewrite bytes")
        ledger_sha = _ordinary_ledger_sha(ledger)
        candidate_manifest = deepcopy(manifest)

        created = False
        candidate_name = None
        marker_written = False
        try:
            stage_dir.mkdir(exist_ok=False)
            created = True
            if (stage_dir.is_symlink() or not stage_dir.is_dir()
                    or stage_dir.resolve(strict=True).parent != ticker_dir):
                raise StorageError(
                    "ordinary correction transaction stage is unsafe")
            _atomic_write_bytes(
                stage_dir / ORDINARY_CORRECTION_TXN_SENTINEL,
                _ORDINARY_TXN_SENTINEL_PAYLOAD)
            write_path = month_file_path(
                root, ticker, year, month, interval).resolve(strict=False)
            candidate_path = stage_dir / write_path.name
            candidate_name = candidate_path.name
            stats = write_month_file(candidate_path, bars)
            new_month_bytes = _ordinary_read_file(
                candidate_path, "new month candidate")
            if _ordinary_sha_bytes(new_month_bytes) != stats.get("sha256"):
                raise StorageError(
                    "ordinary correction candidate changed after encoding")
            _ordinary_correction_txn_hook("prepared")
            new_entry = dict(current_entry)
            new_entry.update(stats)
            new_entry.update(updates)
            new_entry["status"] = updates.get("status", "present")
            new_entry["value_corrections"] = deepcopy(ledger)
            manifest_months(candidate_manifest, interval)[key] = new_entry
            _atomic_write_bytes(
                stage_dir / _ORDINARY_TXN_MANIFEST_BEFORE,
                manifest_before)
            _atomic_write_bytes(
                stage_dir / _ORDINARY_TXN_MONTH_BEFORE, old_month_bytes)
            _atomic_write_bytes(
                stage_dir / _ORDINARY_TXN_MONTH_AFTER, new_month_bytes)
            save_manifest(stage_dir, candidate_manifest)
            manifest_after = _ordinary_read_file(
                stage_dir / MANIFEST_NAME, "new manifest candidate")
            after_payload = _ordinary_load_json(
                manifest_after, "new manifest candidate")
            after_corrections = _ordinary_correction_entries(after_payload)
            after_entry = _ordinary_manifest_entry(
                after_payload, interval, key)
            if (after_entry.get("sha256") != stats.get("sha256")
                    or after_entry.get("value_corrections") != ledger):
                raise StorageError(
                    "ordinary correction candidate manifest lost its ledger")
            _validate_ordinary_correction_transition(
                before_corrections, after_corrections, (interval, key),
                old_sha, stats["sha256"])
            marker = {
                "version": _ORDINARY_CORRECTION_TXN_VERSION,
                "kind": "ordinary_correction_write",
                "ticker": ticker,
                "interval": interval,
                "month": key,
                "old_rel": old_path.relative_to(ticker_dir).as_posix(),
                "write_rel": write_path.relative_to(ticker_dir).as_posix(),
                "candidate_name": candidate_name,
                "old_month_sha256": old_sha,
                "new_month_sha256": stats["sha256"],
                "manifest_before_sha256": _ordinary_sha_bytes(
                    manifest_before),
                "manifest_after_sha256": _ordinary_sha_bytes(
                    manifest_after),
                "ledger_sha256": ledger_sha,
            }
            marker_raw = json.dumps(
                marker, sort_keys=True, separators=(",", ":"),
                allow_nan=False).encode("utf-8")
            _atomic_write_bytes(
                stage_dir / ORDINARY_CORRECTION_TXN_MARKER, marker_raw)
            marker_written = True
            _ordinary_correction_txn_hook("marker")

            # Exact CAS immediately before the first authoritative write.
            if (manifest_path.read_bytes() != manifest_before
                    or _sha256_of_file(old_path) != old_sha
                    or (old_path != write_path and write_path.exists())):
                raise StorageError(
                    "ordinary correction month/manifest changed before "
                    "publication")
            _atomic_write_bytes(manifest_path, manifest_after)
            if manifest_path.read_bytes() != manifest_after:
                raise StorageError(
                    "ordinary correction manifest publication was not exact")
            _ordinary_correction_txn_hook("manifest")
            try:
                os.replace(candidate_path, write_path)
            except OSError as exc:
                raise StorageError(
                    "ordinary correction month promotion failed") from exc
            if _sha256_of_file(write_path) != stats["sha256"]:
                raise StorageError(
                    "ordinary correction month promotion was not exact")
            _ordinary_correction_txn_hook("month")
            if (old_path != write_path
                    and old_path.suffix.casefold() == ".csv"):
                _retire_exact_ordinary_csv(old_path, old_sha)
            _ordinary_correction_txn_hook("csv")
            _clear_ordinary_correction_stage(stage_dir, candidate_name)
            return stats, after_payload
        except BaseException:
            # A durable marker owns recovery.  Before the marker, authoritative
            # bytes are untouched, so exact owned cleanup is safe; if cleanup
            # itself fails the sentinel remains as fail-closed debt.
            if created and not marker_written:
                try:
                    _clear_ordinary_correction_stage(
                        stage_dir, candidate_name)
                except BaseException:
                    pass
            raise


def manifest_months(manifest, interval):
    return (manifest.setdefault("intervals", {})
                    .setdefault(interval, {"months": {},
                                           "verified_absent": []})
                    .setdefault("months", {}))


_FINGERPRINT_MANIFEST_MAX_BYTES = 16 * (1 << 20)
_FINGERPRINT_READ_ATTEMPTS = 4
_FINGERPRINT_MONTH_RE = re.compile(r"^[12]\d{3}-(?:0[1-9]|1[0-2])$")
_FINGERPRINT_DATE_RE = re.compile(
    r"^[12]\d{3}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])$")
_FINGERPRINT_STATUSES = frozenset({"present", "format-error", "MISSING"})
_FINGERPRINT_SHA_RE = re.compile(r"^[0-9a-f]{64}$")


def _manifest_stat_signature(st):
    return (getattr(st, "st_dev", None), getattr(st, "st_ino", None),
            st.st_size, st.st_mtime_ns)


def _read_stable_manifest_bytes(path):
    """Read one bounded manifest only when its path and open handle stay put."""
    path = Path(path)
    last_error = "manifest changed while being read"
    for attempt in range(_FINGERPRINT_READ_ATTEMPTS):
        try:
            before = os.stat(path)
            if before.st_size > _FINGERPRINT_MANIFEST_MAX_BYTES:
                raise StorageError(
                    f"{path}: manifest exceeds the provenance read limit")
            with open(path, "rb") as fh:
                opened_before = os.fstat(fh.fileno())
                raw = fh.read(_FINGERPRINT_MANIFEST_MAX_BYTES + 1)
                opened_after = os.fstat(fh.fileno())
            after = os.stat(path)
            signatures = {_manifest_stat_signature(st) for st in
                          (before, opened_before, opened_after, after)}
            if (len(signatures) == 1 and len(raw) == before.st_size
                    and len(raw) <= _FINGERPRINT_MANIFEST_MAX_BYTES):
                return raw
            last_error = "manifest changed while being read"
        except StorageError:
            raise
        except OSError as exc:
            last_error = f"manifest unavailable: {exc}"
        if attempt + 1 < _FINGERPRINT_READ_ATTEMPTS:
            _time.sleep(0.01 * (attempt + 1))
    raise StorageError(f"{path}: {last_error}")


def _json_object_without_duplicate_keys(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise ValueError(f"duplicate JSON key {key!r}")
        out[key] = value
    return out


def interval_state_fingerprint(root, ticker, interval):
    """Return a canonical SHA-256 identity for one manifest interval.

    Only fields that describe the selected series are projected. Manifest
    generation, ticker metadata, and other intervals are deliberately excluded,
    so an IV/HVOL update cannot stale a current TRADES validation. The read is
    bounded and stable; malformed or concurrently replaced manifests fail
    closed with StorageError instead of producing provenance from mixed state.
    """
    return _interval_state_fingerprint(
        root, ticker, interval, allow_missing=False)


def optional_interval_state_fingerprint(root, ticker, interval):
    """Fingerprint an interval's exact state, including stable absence."""
    return _interval_state_fingerprint(
        root, ticker, interval, allow_missing=True)


def interval_state_fingerprint_through_month(root, ticker, interval, month):
    """Identity of one interval's state at or before ``month`` (``YYYY-MM``).

    For policies that freeze a verdict about a bounded historical era: months
    later than ``month`` are excluded, so ordinary forward fetches cannot
    change this value, while any edit, backfill, or absence change INSIDE the
    era still does. The scope is part of the hashed payload, so a scoped
    identity can never collide with the unscoped one even when every stored
    month already falls inside the scope. Malformed month keys and absence
    dates still fail closed regardless of scope; a fully out-of-scope interval
    is a legitimate empty projection, not an error.
    """
    return _interval_state_fingerprint(
        root, ticker, interval, allow_missing=False, through_month=month)


def optional_interval_storage_summary(root, ticker, interval):
    """Return strict interval identity plus canonical month-status counts.

    Unlike the lenient cache loader, this rejects duplicate JSON keys,
    malformed month/date records, unstable reads, and escaping ticker paths.
    It is therefore suitable for safety decisions about proven absence.
    """
    return _interval_state_fingerprint(
        root, ticker, interval, allow_missing=True,
        include_status_counts=True)


def _interval_state_fingerprint(root, ticker, interval, *, allow_missing,
                                include_status_counts=False,
                                through_month=None):
    folder = canonical_ticker(ticker)
    interval = str(interval or "").strip()
    if not INTERVAL_RE.match(interval):
        raise StorageError(f"bad interval token {interval!r}")
    resolved_root = Path(root).resolve()
    ticker_dir = (resolved_root / folder).resolve()
    if ticker_dir.parent != resolved_root:
        raise StorageError(f"ticker folder escapes storage root: {folder!r}")
    path = ticker_dir / MANIFEST_NAME
    raw = _read_stable_manifest_bytes(path)
    try:
        manifest = json.loads(raw.decode("utf-8"),
                              object_pairs_hook=_json_object_without_duplicate_keys)
    except (UnicodeError, ValueError) as exc:
        raise StorageError(f"{path}: malformed manifest ({exc})") from None
    if not isinstance(manifest, dict):
        raise StorageError(f"{path}: manifest root is not an object")
    if manifest.get("folder") != folder:
        raise StorageError(f"{path}: manifest folder does not match {folder}")
    intervals = manifest.get("intervals")
    if not isinstance(intervals, dict):
        raise StorageError(f"{path}: manifest intervals are malformed")
    present = interval in intervals
    ivd = intervals.get(interval)
    if not present:
        if not allow_missing:
            raise StorageError(f"{path}: interval {interval!r} is absent")
        months, verified_absent, backfill_incomplete = {}, [], False
    else:
        if not isinstance(ivd, dict):
            raise StorageError(
                f"{path}: interval {interval!r} state is malformed")
        months = ivd.get("months")
        verified_absent = ivd.get("verified_absent", [])
        backfill_incomplete = ivd.get("backfill_incomplete", False)
        if (not isinstance(months, dict)
                or not isinstance(verified_absent, list)
                or not isinstance(backfill_incomplete, bool)):
            raise StorageError(
                f"{path}: interval {interval!r} state is malformed")

    if through_month is not None and (
            not isinstance(through_month, str)
            or not _FINGERPRINT_MONTH_RE.match(through_month)):
        raise StorageError(f"bad scope month {through_month!r}")

    projected_months = []
    for month, entry in sorted(months.items()):
        if not isinstance(month, str) or not _FINGERPRINT_MONTH_RE.match(month):
            raise StorageError(f"{path}: malformed month key {month!r}")
        if not isinstance(entry, dict):
            raise StorageError(f"{path}: month {month} entry is malformed")
        status = entry.get("status")
        if status not in _FINGERPRINT_STATUSES:
            raise StorageError(f"{path}: month {month} has bad status {status!r}")
        sha = entry.get("sha256")
        if sha is not None and (not isinstance(sha, str)
                                or not _FINGERPRINT_SHA_RE.match(sha)):
            raise StorageError(f"{path}: month {month} has bad sha256")
        rows = entry.get("rows")
        if (rows is not None
                and (isinstance(rows, bool) or not isinstance(rows, int)
                     or rows < 0)):
            raise StorageError(f"{path}: month {month} has bad row count")
        first, last = entry.get("first"), entry.get("last")
        if any(v is not None and (not isinstance(v, str) or len(v) > 64)
               for v in (first, last)):
            raise StorageError(f"{path}: month {month} has bad bounds")
        if (status == "present"
                and (sha is None or not rows or not first or not last)):
            raise StorageError(
                f"{path}: present month {month} lacks canonical state")
        projected_months.append({
            "month": month, "status": status, "sha256": sha, "rows": rows,
            "first": first, "last": last})

    absent = []
    for value in verified_absent:
        if not isinstance(value, str) or not _FINGERPRINT_DATE_RE.match(value):
            raise StorageError(f"{path}: malformed verified-absent date {value!r}")
        try:
            date.fromisoformat(value)
        except ValueError:
            raise StorageError(
                f"{path}: malformed verified-absent date {value!r}") from None
        absent.append(value)
    if len(absent) != len(set(absent)):
        raise StorageError(f"{path}: duplicate verified-absent date")
    absent.sort()

    if through_month is not None:
        # Scope strictly AFTER validation (F-FABLE-80-2): a malformed month
        # key or absence date fails closed identically in scoped and unscoped
        # reads; only canonical out-of-scope state is ever excluded.
        projected_months = [row for row in projected_months
                            if row["month"] <= through_month]
        absent = [value for value in absent if value[:7] <= through_month]

    canonical = {
        "schema_version": INTERVAL_FINGERPRINT_VERSION,
        "ticker": folder,
        "interval": interval,
        "present": present,
        "backfill_incomplete": backfill_incomplete,
        "months": projected_months,
        "verified_absent": absent,
    }
    # Only a scoped read adds the key, so unscoped identities stay byte-stable
    # for every existing consumer while scoped ones can never collide.
    if through_month is not None:
        canonical["scope_through_month"] = through_month
    payload = json.dumps(canonical, sort_keys=True,
                         separators=(",", ":")).encode("utf-8")
    result = {
        "schema_version": INTERVAL_FINGERPRINT_VERSION,
        "algorithm": "sha256",
        "sha256": hashlib.sha256(payload).hexdigest(),
        "ticker": folder,
        "interval": interval,
        "present": present,
        "backfill_incomplete": backfill_incomplete,
        "month_count": len(projected_months),
        "verified_absent_count": len(absent),
    }
    if through_month is not None:
        result["scope_through_month"] = through_month
    if include_status_counts:
        result.update({
            "present_month_count": sum(
                row["status"] == "present" for row in projected_months),
            "missing_month_count": sum(
                row["status"] == "MISSING" for row in projected_months),
            "format_error_month_count": sum(
                row["status"] == "format-error"
                for row in projected_months),
        })
    return result


def bank_manifest_state_fingerprint(root):
    """Fingerprint the active ticker set and every manifest generation/byte hash.

    Health is a bank-wide derived report, so any manifest rewrite must make an
    older report non-current. The digest uses writer generations and full
    manifest SHA-256 values rather than mtimes. Each manifest read is stable and
    bounded; active-folder or manifest-state races retry before failing closed.
    """
    resolved_root = Path(root).resolve()

    def active_dirs():
        try:
            entries = sorted(resolved_root.iterdir(), key=lambda path: path.name)
        except OSError as exc:
            raise StorageError(
                f"{resolved_root}: storage root unavailable ({exc})") from exc
        out = []
        for path in entries:
            try:
                if (path.is_dir() and not path.name.startswith("_")
                        and TICKER_DIR_RE.fullmatch(path.name)):
                    target = path.resolve()
                    if target.parent != resolved_root:
                        raise StorageError(
                            f"ticker folder escapes storage root: {path.name!r}")
                    out.append(target)
            except OSError as exc:
                raise StorageError(
                    f"{path}: ticker folder unavailable ({exc})") from exc
        return out

    def manifest_row(ticker_dir):
        path = ticker_dir / MANIFEST_NAME
        outer_before = os.stat(path)
        raw = _read_stable_manifest_bytes(path)
        outer_after = os.stat(path)
        signature = _manifest_stat_signature(outer_after)
        if _manifest_stat_signature(outer_before) != signature:
            raise StorageError(f"{path}: manifest changed during fingerprint")
        try:
            manifest = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_json_object_without_duplicate_keys)
        except (UnicodeError, ValueError) as exc:
            raise StorageError(
                f"{path}: malformed manifest ({exc})") from None
        generation = manifest.get("generation") if isinstance(
            manifest, dict) else None
        if (not isinstance(manifest, dict)
                or manifest.get("folder") != ticker_dir.name
                or isinstance(generation, bool)
                or not isinstance(generation, int)
                or generation < 0):
            raise StorageError(
                f"{path}: invalid folder/generation state")
        return ({
            "ticker": ticker_dir.name,
            "generation": generation,
            "manifest_sha256": hashlib.sha256(raw).hexdigest(),
        }, signature)

    def parallel_map(fn, paths):
        if len(paths) < 2:
            return [fn(path) for path in paths]
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(
                max_workers=min(16, len(paths)),
                thread_name_prefix="bank-state") as pool:
            return list(pool.map(fn, paths))

    def current_signature(ticker_dir):
        return _manifest_stat_signature(os.stat(ticker_dir / MANIFEST_NAME))

    last_error = "active ticker set changed during fingerprint"
    for attempt in range(_FINGERPRINT_READ_ATTEMPTS):
        try:
            before = active_dirs()
            snapshots = parallel_map(manifest_row, before)
            between = active_dirs()
            if [path.name for path in before] != [path.name for path in between]:
                raise StorageError(
                    "active ticker set changed during fingerprint")
            final_signatures = parallel_map(current_signature, between)
            after = active_dirs()
            if [path.name for path in between] != [path.name for path in after]:
                raise StorageError(
                    "active ticker set changed during fingerprint")
            if [signature for _row, signature in snapshots] != final_signatures:
                raise StorageError(
                    "manifest state changed during fingerprint")
            rows = [row for row, _signature in snapshots]
        except (StorageError, OSError) as exc:
            last_error = f"manifest state unavailable: {exc}"
        else:
            canonical = {
                "schema_version": BANK_STATE_FINGERPRINT_VERSION,
                "manifests": rows,
            }
            payload = json.dumps(
                canonical, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            return {
                "schema_version": BANK_STATE_FINGERPRINT_VERSION,
                "algorithm": "sha256",
                "sha256": hashlib.sha256(payload).hexdigest(),
                "manifest_count": len(rows),
            }
        if attempt + 1 < _FINGERPRINT_READ_ATTEMPTS:
            _time.sleep(0.01 * (attempt + 1))
    raise StorageError(f"{resolved_root}: {last_error}")


# --- sha256 integrity scrub ----------------------------------------------------
#
# The scan fast path trusts size+mtime_ns and never re-hashes; a silent
# bit-flip / torn write / sync-conflict fork whose bytes differ from the
# recorded payload sha therefore goes undetected. These helpers close that
# gap: they recompute a stored month file's sha256 and COMPARE it to the
# value the manifest recorded at write time. Strictly READ-ONLY — a scrub
# never rewrites data, never touches the manifest, never heals; its job is
# to REPORT divergence so a human (or a higher layer) decides what to do.

_SHA_CHUNK = 1 << 20             # 1 MiB streaming read — month files are tiny
                                # today, but this never grows memory unbounded


def _sha256_of_file(path):
    """Stream the file at `path` and return its sha256 hex digest, or None
    when the file is absent/unreadable. Never raises — callers branch on
    None so a missing or locked file degrades to a report, never a crash."""
    h = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(_SHA_CHUNK), b""):
                h.update(chunk)
    except OSError:
        return None
    return h.hexdigest()


def verify_month_file(path, expected_sha):
    """Recompute the sha256 of the stored month file at `path` and compare
    it to `expected_sha` (the hash the manifest recorded at write time).
    Pure read; returns one of:
      "ok"          bytes on disk hash to the recorded value
      "MISMATCH"    file exists but its bytes no longer match (corruption,
                    torn write, external edit, sync-conflict fork)
      "no-hash"     no recorded hash to compare against (expected_sha is
                    falsy — a pre-hash manifest entry); cannot judge
      "missing"     the file is absent or unreadable
    The recorded-sha SEMANTICS are untouched: this only reads what
    write_month_file/read_month_file already stored."""
    actual = _sha256_of_file(path)
    if actual is None:
        return "missing"
    if not expected_sha:
        return "no-hash"
    return "ok" if actual == expected_sha else "MISMATCH"


def scrub_storage(root, progress=None, intervals=None):
    """Walk every ticker manifest under `root` and verify each recorded
    month hash against the bytes actually on disk. Strictly READ-ONLY: no
    file is written, no manifest is saved, nothing is healed — divergence
    is only REPORTED, exactly like a fsck dry run.

    Returns a dict:
      checked      int    — month entries examined
      ok           int    — bytes match the recorded sha256
      mismatched   [str]  — file paths whose bytes DIVERGE from the record
      no_hash      [str]  — entries with no recorded sha to compare against
      missing      [str]  — recorded months whose file is absent/unreadable
      warnings     [str]  — manifests that could not be read, etc.

    Only "present" (and "format-error") month entries with a recorded path
    are scrubbed; MISSING tombstones are skipped — their file is gone by
    design, and the scan already tracks them. A manifest that fails to load
    degrades to a warning, never an abort — one bad ticker never hides the
    health of the rest."""
    root = Path(root)
    out = {"checked": 0, "ok": 0, "mismatched": [], "no_hash": [],
           "missing": [], "warnings": []}
    if not root.is_dir():
        out["warnings"].append(f"storage root does not exist: {root}")
        return out
    try:
        entries = sorted(root.iterdir(), key=lambda p: p.name.lower())
    except OSError as exc:
        out["warnings"].append(f"storage root unreadable: {exc}")
        return out
    ticker_dirs = [e for e in entries
                   if e.is_dir() and not e.name.startswith("_")
                   and TICKER_DIR_RE.match(e.name)]
    for ti, tdir in enumerate(ticker_dirs):
        if progress is not None:
            try:
                progress(ti, len(ticker_dirs), tdir.name)
            except Exception:  # noqa: BLE001 — a UI callback never aborts
                pass
        try:
            _scrub_ticker(root, tdir, out)
        except Exception as exc:  # noqa: BLE001 — one bad ticker must never
            out["warnings"].append(    # hide the health of the healthy rest
                f"{tdir.name}: scrub failed ({exc})")
    return out


def _scrub_ticker(root, tdir, out):
    """Scrub ONE ticker folder's recorded month hashes into `out`."""
    manifest = load_manifest(tdir)
    if manifest is None:
        if (tdir / MANIFEST_NAME).exists():
            out["warnings"].append(
                f"{tdir.name}: manifest unreadable — cannot scrub recorded "
                f"hashes (run a scan to rebuild it)")
        return
    folder = manifest.get("folder", tdir.name)
    for iv, ivd in sorted(manifest.get("intervals", {}).items()):
        for key, entry in sorted(ivd.get("months", {}).items()):
            if not isinstance(entry, dict):
                continue
            if entry.get("status") not in ("present", "format-error"):
                continue                  # MISSING tombstones: file gone by
                #                           design, the scan owns them
            try:
                year_s, month_s = key.split("-")
                # locate the ACTUAL stored file (Parquet or legacy CSV); fall
                # back to the canonical path so a truly-absent month verifies as
                # 'missing' rather than crashing.
                fpath = (find_month_file(root, folder, int(year_s),
                                         int(month_s), iv)
                         or month_file_path(root, folder, int(year_s),
                                            int(month_s), iv))
            except (ValueError, StorageError):
                out["warnings"].append(
                    f"{tdir.name}/{iv}/{key}: cannot derive a month path "
                    f"for this manifest entry")
                continue
            out["checked"] += 1
            verdict = verify_month_file(fpath, entry.get("sha256"))
            if verdict == "ok":
                out["ok"] += 1
            elif verdict == "MISMATCH":
                out["mismatched"].append(str(fpath))
            elif verdict == "no-hash":
                out["no_hash"].append(str(fpath))
            else:                          # "missing"
                out["missing"].append(str(fpath))


# --- environment guards --------------------------------------------------------

def tzdata_problem():
    """None when zoneinfo can serve US/Eastern; else a fix-it message.
    Windows has no OS tz database — without the tzdata package every IBKR
    timestamp conversion would be dead on arrival."""
    try:
        from zoneinfo import ZoneInfo
        ZoneInfo("America/New_York")
        return None
    except Exception:  # noqa: BLE001
        return ("timezone database unavailable — run 'pip install tzdata' "
                "(required before any IBKR update can convert timestamps)")


def synced_root_warning(root):
    """Warn when the storage root sits inside a cloud-sync folder: sync
    locks break atomic replaces and conflict copies silently fork data."""
    resolved = str(Path(root).resolve()).lower()
    for token in ("onedrive", "icloud", "dropbox", "google drive"):
        if token in resolved:
            return (f"storage root appears to be inside a {token} synced "
                    f"folder — sync conflicts can fork data files; consider "
                    f"a non-synced location")
    one = os.environ.get("OneDrive")
    if one and resolved.startswith(str(Path(one).resolve()).lower()):
        return ("storage root is inside your OneDrive folder — sync "
                "conflicts can fork data files; consider a non-synced "
                "location")
    return None


# --- scan ----------------------------------------------------------------------
#
# Digest-gated rescan cache: a no-change rescan of the whole bank costs ~8-9 s of
# dir walking + fresh stats. The cache stores, per CLEAN ticker, a digest of
# fresh os.stat mtime_ns for the ticker/year/month dirs + the manifest's
# (size, mtime_ns), plus that ticker's scan summary. On the next cached scan a
# ticker whose digest re-stats identical replays its summary (~0.8 s bank-wide);
# ANY mismatch, missing path, or racy timestamp falls back to the full
# _scan_ticker walk for that ticker only. Soundness (mutation battery 7/7,
# NTFS semantics verified empirically): every app write is an entry
# create/replace/delete in some digested dir -> that dir's mtime changes; a
# crash that leaves the manifest lagging the tree is caught by the month-dir
# mtime alone; `captured_ns` is stamped BEFORE the ticker's walk, so a write
# landing mid-scan is inside the racy guard and the entry is refused. The one
# blind spot — an external tool rewriting a month file's bytes strictly
# IN PLACE (no rename) — is not an app write pattern (_atomic_write_bytes is
# temp + os.replace) and remains covered by the sha scrub + manual Rescan
# (which the GUI keeps as a full ground-truth scan). NTFS-only: other/unknown
# volumes fall back to the full scan.

DIGEST_CACHE_NAME = "_scan_digest_cache.json"    # at the bank's PARENT, not inside
_DIGEST_RACY_NS = 2_000_000_000                  # 2 s racy-write guard
_DIGEST_VERSION = 1
_DIGEST_MEM = {}                                 # {resolved-root: tickers cache}
_DIGEST_LOCK = threading.Lock()


def _volume_is_ntfs(path):
    """True only for a local FIXED NTFS volume — the semantics the digest
    relies on (dir mtime bumps on child create/replace/delete/rename) were
    verified there. Anything else (FAT/exFAT, network, unknown) -> no cache."""
    if os.name != "nt":
        return False
    try:
        import ctypes
        drive = os.path.splitdrive(str(Path(path).resolve()))[0]
        if not drive or len(drive) != 2 or drive[1] != ":":
            return False                        # UNC / driveless: be conservative
        root = drive + "\\"
        if ctypes.windll.kernel32.GetDriveTypeW(ctypes.c_wchar_p(root)) != 3:
            return False                        # 3 = DRIVE_FIXED
        buf = ctypes.create_unicode_buffer(64)
        ok = ctypes.windll.kernel32.GetVolumeInformationW(
            ctypes.c_wchar_p(root), None, 0, None, None, None, buf, 64)
        return bool(ok) and buf.value.upper() == "NTFS"
    except Exception:  # noqa: BLE001 — detection failure -> full scan
        return False


def _digest_capture(tdir, captured_ns):
    """Fresh-stat digest of one ticker dir (NEVER DirEntry-cached stats).
    `captured_ns` must be stamped BEFORE the ticker's scan walk started, so a
    mutation between the walk and this capture is inside the racy guard.
    Returns the digest dict, or None on any error (-> simply not cached)."""
    tdir = str(tdir)
    d = {"tdir": tdir, "dirs": {}, "manifest": None, "captured_ns": captured_ns}
    try:
        d["dirs"][tdir] = os.stat(tdir).st_mtime_ns
        with os.scandir(tdir) as it:
            years = [e.path for e in it
                     if e.is_dir() and YEAR_DIR_RE.match(e.name)]
        try:
            st = os.stat(os.path.join(tdir, MANIFEST_NAME))
            d["manifest"] = [st.st_size, st.st_mtime_ns]
        except OSError:
            d["manifest"] = None
        for y in sorted(years):
            d["dirs"][y] = os.stat(y).st_mtime_ns
            with os.scandir(y) as it:
                months = [e.path for e in it
                          if e.is_dir() and e.name in _MONTH_DIR_SET]
            for mdir in sorted(months):
                d["dirs"][mdir] = os.stat(mdir).st_mtime_ns
    except OSError:
        return None
    return d


def _digest_verify(d):
    """True only when EVERY digested component fresh-stats IDENTICAL and none
    is within the racy guard of its capture stamp. Missing paths, a changed or
    (dis)appeared manifest, or any drift -> False (full rescan of the ticker)."""
    try:
        if not isinstance(d, dict) or not d.get("dirs"):
            return False
        horizon = int(d["captured_ns"]) - _DIGEST_RACY_NS
        for p, mt in d["dirs"].items():
            st = os.stat(p)
            if st.st_mtime_ns != mt or st.st_mtime_ns >= horizon:
                return False
        try:
            st = os.stat(os.path.join(d["tdir"], MANIFEST_NAME))
            cur = [st.st_size, st.st_mtime_ns]
        except OSError:
            cur = None
        if cur != d.get("manifest"):
            return False
        if cur is not None and cur[1] >= horizon:
            return False
        return True
    except OSError:
        return False
    except Exception:  # noqa: BLE001 — malformed cache entry -> rescan
        return False


def _digest_cache_path(root):
    return Path(root).resolve().parent / DIGEST_CACHE_NAME


def _digest_cache_load(root):
    """The persisted per-ticker digest cache for `root` ({} when absent/stale/
    corrupt/other-bank). In-process memo avoids re-parsing the JSON per scan."""
    key = str(Path(root).resolve())
    with _DIGEST_LOCK:
        mem = _DIGEST_MEM.get(key)
        if mem is not None:
            return mem
    try:
        data = json.loads(_digest_cache_path(root).read_text(encoding="utf-8"))
        if (isinstance(data, dict) and data.get("version") == _DIGEST_VERSION
                and data.get("root") == key
                and isinstance(data.get("tickers"), dict)):
            with _DIGEST_LOCK:
                _DIGEST_MEM[key] = data["tickers"]
            return data["tickers"]
    except (OSError, ValueError):
        pass
    return {}


def _digest_cache_save(root, tickers_cache):
    """Persist the rebuilt cache (atomic, best-effort) + refresh the memo."""
    key = str(Path(root).resolve())
    with _DIGEST_LOCK:
        _DIGEST_MEM[key] = tickers_cache
    try:
        _atomic_write_bytes(
            _digest_cache_path(root),
            json.dumps({"version": _DIGEST_VERSION, "root": key,
                        "tickers": tickers_cache},
                       separators=(",", ":")).encode("utf-8"))
    except Exception:  # noqa: BLE001 — cache persistence is best-effort
        pass


def scan_storage(root, progress=None, workers=None, digest_cache=False):
    """Walk the tree with the strict whitelist and reconcile manifests.

    Returns a dict:
      root_exists    bool
      tickers        {folder: {symbol, intervals: {iv: {months, coverage,
                      rows, n_months}}, flags: [...]}}
      unrecognized   [(path, reason)]  — inert items, reported only
      missing        [(folder, interval, month)]  — manifest-ahead (file
                      vanished: AV quarantine / manual delete); tombstones
                      are KEPT, never auto-refetched
      errors         [(path, message)]  — files that fail the strict read
      warnings       [str]

    Resilience rules, each adversarially tested: a corrupt or mis-shaped
    manifest, a locked or mid-scan-vanishing file, or any single bad
    ticker degrades to a warning/error entry — NEVER an aborted scan. The
    only things a scan may write are manifest.json files in ticker folders
    that hold (or held) data; the only thing it may delete is a stale
    *.tmp left behind by an interrupted _atomic_write_bytes. Fast path: a
    file whose size + mtime_ns match its manifest entry is trusted without
    re-parsing, so rescans of a settled tree are near-instant. Ticker folders
    are scanned CONCURRENTLY (the walk is I/O-bound) into per-ticker results
    that are MERGED in folder order, so the aggregate is identical to a serial
    scan; pass workers=1 to force the serial path.

    digest_cache=True additionally gates each ticker behind the persistent
    digest cache (see the block comment above): an unchanged CLEAN ticker
    replays its cached summary instead of re-walking ~180 dirs — a no-change
    bank rescan drops from ~8-9 s to under 1 s. Only issue-free tickers are
    ever cached (any error/missing/warning/unrecognized/tmp-sweep -> the real
    walk always re-runs), so issues re-report every scan exactly as before.
    A cached summary is SHARED with the in-process memo — consumers treat scan
    results as read-only (they all do). Non-NTFS roots silently fall back to
    the full scan. Default False: existing callers and tests are unchanged;
    the GUI's manual Rescan button also stays a full ground-truth scan."""
    root = Path(root)
    result = {"root_exists": root.is_dir(), "tickers": {}, "unrecognized": [],
              "missing": [], "errors": [], "warnings": []}
    if not result["root_exists"]:
        return result
    try:
        entries = sorted(root.iterdir(), key=lambda p: p.name.lower())
    except OSError as exc:
        result["warnings"].append(f"storage root unreadable: {exc}")
        return result
    ticker_dirs = []
    for e in entries:
        if e.name.startswith("_"):
            continue            # reserved namespace — _quarantine/_ingest_reports
            #                     dirs AND the _*.json sidecars (_data_gaps,
            #                     _cross_validation, _ibkr_names, _ibkr_earliest,
            #                     _consensus_calendar…): owned by the app,
            #                     never tickers, never flagged as junk
        if e.is_dir() and TICKER_DIR_RE.match(e.name):
            ticker_dirs.append(e)
        elif e.is_dir() and YEAR_DIR_RE.match(e.name):
            result["unrecognized"].append(
                (str(e), "year folder at the storage root — dragged out "
                         "of its ticker folder?"))
        else:
            result["unrecognized"].append(
                (str(e), "not a canonical ticker folder"))
    swept = 0
    n = len(ticker_dirs)
    if workers is None:                  # I/O-bound walk -> oversubscribe a little
        workers = max(1, min(16, (os.cpu_count() or 4) + 4))
    use_cache = bool(digest_cache) and _volume_is_ntfs(root)
    cache = _digest_cache_load(root) if use_cache else {}
    fresh_cache = {}          # rebuilt per cached scan; saved only when changed

    def _scan_one(tdir):
        # Each ticker scans into its OWN result dict, so _scan_ticker (and the
        # manifest write it may do) is byte-for-byte the serial logic and is
        # thread-safe — distinct folders, distinct files. A partway failure keeps
        # the partial entries AND the 'scan failed' warning, exactly as the serial
        # loop did. The caller MERGES in folder order -> identical aggregate.
        if use_cache:
            ent = cache.get(tdir.name)
            if isinstance(ent, dict) and _digest_verify(ent.get("digest")):
                return ent, None, 0          # digest HIT -> replay the summary
        t0_ns = _time.time_ns()              # BEFORE the walk: racy-guard anchor
        local = {"tickers": {}, "unrecognized": [], "missing": [],
                 "errors": [], "warnings": []}
        try:
            sw = _scan_ticker(tdir, local)
        except Exception as exc:  # noqa: BLE001 — one bad ticker never aborts
            local["warnings"].append(f"{tdir.name}: scan failed ({exc})")
            sw = 0
        ent = None
        if (use_cache and sw == 0 and tdir.name in local["tickers"]
                and not local["errors"] and not local["missing"]
                and not local["warnings"] and not local["unrecognized"]):
            dg = _digest_capture(tdir, t0_ns)     # CLEAN ticker -> cacheable
            if dg is not None:
                ent = {"digest": dg, "summary": local["tickers"][tdir.name]}
        return ent, local, sw

    def _merge(ti, tdir, trip):
        nonlocal swept
        if progress is not None:
            try:
                progress(ti, n, tdir.name)
            except Exception:  # noqa: BLE001 — a UI callback never aborts
                pass
        ent, local, sw = trip
        if local is None:                    # digest hit: clean by construction,
            result["tickers"][tdir.name] = ent["summary"]   # summary is ALL it
            fresh_cache[tdir.name] = ent                    # ever contributed
            return
        result["tickers"].update(local["tickers"])
        for _k in ("unrecognized", "missing", "errors", "warnings"):
            result[_k].extend(local[_k])
        swept += sw
        if ent is not None:
            fresh_cache[tdir.name] = ent

    if n > 1 and workers > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=workers,
                                thread_name_prefix="scan") as ex:
            for ti, trip in enumerate(ex.map(_scan_one, ticker_dirs)):
                _merge(ti, ticker_dirs[ti], trip)
    else:                                # serial (1 ticker, or workers=1)
        for ti, tdir in enumerate(ticker_dirs):
            _merge(ti, tdir, _scan_one(tdir))
    if swept:
        result["warnings"].append(
            f"swept {swept} stale temp file(s) from interrupted writes")
    if use_cache and fresh_cache != cache:   # all-hit scans rewrite nothing
        _digest_cache_save(root, fresh_cache)
    return result


def _sweep_or_report_tmp(f, result):
    """Stale-temp policy: names matching TMP_RE are produced ONLY by
    _atomic_write_bytes, so they are the one namespace a scan may delete —
    and only once an hour old (a live writer's temp exists milliseconds).
    Returns 1 when swept."""
    try:
        if _time.time() - f.stat().st_mtime > TMP_MAX_AGE_S:
            f.unlink()
            return 1
        result["unrecognized"].append(
            (str(f), "in-flight temp from a running write"))
    except OSError:
        result["unrecognized"].append((str(f), "stale temp (locked)"))
    return 0


def _preserve_matching_value_corrections(target, source):
    """Copy correction evidence only between byte-identical month entries.

    A scan can overlap a writer that has just published correction evidence.
    The fresh ledger remains valid when both entries name the same month-file
    SHA.  A differing (or absent) SHA means the scanner cannot prove that the
    exact-day evidence describes the bytes it walked, so it must not carry it.
    """
    if not isinstance(target, dict) or not isinstance(source, dict):
        return
    source_sha = source.get("sha256")
    if (isinstance(source_sha, str) and source_sha
            and source_sha == target.get("sha256")
            and "value_corrections" in source):
        target["value_corrections"] = deepcopy(
            source["value_corrections"])


def _merge_scan_manifest_for_save(tdir, scanned):
    """Fresh-merge a scanner-rebuilt manifest before saving.

    The scanner's month map is authoritative for files it just walked, but
    top-level metadata and verified_absent can be newer on disk if a writer
    committed while the scan was running.
    """
    fresh = load_manifest(tdir)
    if not fresh:
        return scanned
    merged = deepcopy(fresh)
    for key in ("symbol", "folder", "name", "conid", "basis"):
        if not merged.get(key) and scanned.get(key):
            merged[key] = scanned[key]
    if scanned.get("aliases"):
        aliases = list(merged.get("aliases") or [])
        seen = set(aliases)
        for a in scanned.get("aliases") or []:
            if a not in seen:
                aliases.append(a)
                seen.add(a)
        merged["aliases"] = aliases
    if not merged.get("actions") and scanned.get("actions"):
        merged["actions"] = deepcopy(scanned.get("actions") or [])

    out_intervals = merged.setdefault("intervals", {})
    for iv, scan_ivd in (scanned.get("intervals") or {}).items():
        if not isinstance(scan_ivd, dict):
            continue
        fresh_ivd = out_intervals.setdefault(iv, {})
        va = set()
        if isinstance(fresh_ivd.get("verified_absent"), list):
            va.update(str(x) for x in fresh_ivd.get("verified_absent", []))
        if isinstance(scan_ivd.get("verified_absent"), list):
            va.update(str(x) for x in scan_ivd.get("verified_absent", []))
        for k, v in scan_ivd.items():
            if k in ("months", "verified_absent"):
                continue
            if k not in fresh_ivd:
                fresh_ivd[k] = deepcopy(v)
        fresh_ivd["verified_absent"] = sorted(va)
        scan_months = scan_ivd.get("months")
        if isinstance(scan_months, dict):
            months = fresh_ivd.setdefault("months", {})
            for mk, ent in scan_months.items():
                fresh_ent = months.get(mk)
                scan_ent = deepcopy(ent)
                fresh_sha = (fresh_ent.get("sha256")
                             if isinstance(fresh_ent, dict) else None)
                scan_sha = (scan_ent.get("sha256")
                            if isinstance(scan_ent, dict) else None)
                if (isinstance(fresh_sha, str)
                        and isinstance(scan_sha, str)
                        and fresh_sha != scan_sha):
                    try:
                        year, month = (int(part) for part in mk.split("-", 1))
                        current_path = find_month_file(
                            Path(tdir).parent, Path(tdir).name,
                            year, month, iv)
                    except (TypeError, ValueError, StorageError, OSError):
                        current_path = None
                    current_sha = (_sha256_of_file(current_path)
                                   if current_path is not None else None)
                    if current_sha == fresh_sha:
                        # A writer committed after this scan read the month.
                        # Never regress its fresh manifest entry to stale scan
                        # evidence (especially its correction ledger).
                        continue
                    if current_sha != scan_sha:
                        raise StorageError(
                            f"{Path(tdir).name} {iv} {mk}: month changed "
                            "during manifest merge")
                _preserve_matching_value_corrections(scan_ent, fresh_ent)
                months[mk] = scan_ent
    return merged


def _scan_ticker(tdir, result):
    """Scan ONE ticker folder into `result`; returns swept-temp count."""
    swept = 0
    reconcile_stage = tdir / VOL_VALUE_RECONCILE_STAGE_DIR
    if reconcile_stage.exists() or reconcile_stage.is_symlink():
        try:
            # A prior hard exit in an ordinary correction-bearing rewrite is
            # resolved under the same cross-process ticker lock before the
            # walk. A reconcile WAL is intentionally untouched and remains
            # fenced by the stage check immediately below. Settled tickers pay
            # no extra operation-lock cost on ordinary scans.
            with ticker_transaction(tdir):
                pass
        except StorageError as exc:
            result["errors"].append((
                str(reconcile_stage),
                f"ordinary correction transaction recovery failed: {exc}"))
            return swept
    if reconcile_stage.exists() or reconcile_stage.is_symlink():
        result["errors"].append((
            str(reconcile_stage),
            "volatility correction transaction is pending; ticker scan "
            "refused until recovery"))
        return swept
    mpath = tdir / MANIFEST_NAME
    manifest_blocked = mpath.is_dir()
    if manifest_blocked:
        result["warnings"].append(
            f"{tdir.name}: manifest.json is a DIRECTORY — remove it (the "
            f"manifest cannot be saved while it exists)")
    manifest = None if manifest_blocked else load_manifest(tdir)
    adopted = manifest is not None
    if manifest is None and mpath.is_file():
        # Corrupt/mis-shaped manifest: keep the evidence and salvage what
        # the tree can never reproduce — symbol/conid are PRIMARY data.
        adopted = True
        salvaged = {}
        try:
            salvaged = _salvage_manifest_fields(
                mpath.read_text(encoding="utf-8", errors="replace"))
        except OSError:
            pass
        try:
            mpath.replace(mpath.with_name(
                f"{MANIFEST_NAME}.corrupt-{int(_time.time())}"))
        except OSError:
            pass
        manifest = new_manifest(salvaged.get("symbol", tdir.name),
                                tdir.name)
        if salvaged.get("conid") is not None:
            manifest["conid"] = salvaged["conid"]
        result["warnings"].append(
            f"{tdir.name}: manifest unreadable — rebuilt; original kept "
            f"as {MANIFEST_NAME}.corrupt-*; "
            + ("symbol/conid salvaged" if salvaged else
               "symbol mapping LOST — re-enter it if it differed from "
               "the folder name"))
    if manifest is None:
        manifest = new_manifest(tdir.name, tdir.name)
    found = {}
    # os.scandir over pathlib.iterdir: a DirEntry carries name + type + (on
    # Windows) size + mtime_ns straight from the directory read, so is_file/
    # is_dir/stat below cost NO extra syscall and there is NO per-file Path
    # parsing. DirEntry.path == str(the_old_Path), so messages are byte-identical.
    with os.scandir(tdir) as _yit:
        yentries = sorted(_yit, key=lambda e: e.name)
    for ydir in yentries:
        if ydir.name == MANIFEST_NAME:
            continue
        if ydir.is_file() and TMP_RE.search(ydir.name):
            swept += _sweep_or_report_tmp(Path(ydir.path), result)
            continue
        if ydir.name.startswith(MANIFEST_NAME + ".corrupt-"):
            result["unrecognized"].append(
                (ydir.path, "preserved corrupt manifest (delete after "
                            "review)"))
            continue
        if not (ydir.is_dir() and YEAR_DIR_RE.match(ydir.name)):
            result["unrecognized"].append(
                (ydir.path, "not a YYYY year folder"))
            continue
        n_month_dirs = 0
        with os.scandir(ydir.path) as _mit:
            mentries = sorted(_mit, key=lambda e: e.name)
        for mdir in mentries:
            if not (mdir.is_dir() and mdir.name in _MONTH_DIR_SET):
                result["unrecognized"].append(
                    (mdir.path, "not an NN-Mon month folder"))
                continue
            n_month_dirs += 1
            n_children = 0
            with os.scandir(mdir.path) as _fit:
                fentries = sorted(_fit, key=lambda e: e.name)
            for f in fentries:
                n_children += 1
                if f.is_file() and TMP_RE.search(f.name):
                    swept += _sweep_or_report_tmp(Path(f.path), result)
                    continue
                fm = FILENAME_RE.match(f.name) if f.is_file() else None
                if (not fm or fm.group(1) != tdir.name
                        or fm.group(2) != ydir.name
                        or MONTH_DIRS[int(fm.group(3)) - 1] != mdir.name):
                    result["unrecognized"].append(
                        (f.path, "name does not match its location"))
                    continue
                found[(fm.group(4),
                       f"{fm.group(2)}-{fm.group(3)}")] = f
            if n_children == 0:
                result["unrecognized"].append(
                    (mdir.path, "empty month folder"))
        if n_month_dirs == 0:
            result["unrecognized"].append(
                (ydir.path, "year folder with no month folders"))
    if not found and not adopted:
        # Ticker-SHAPED junk ('OLD', 'TEMP'): report it, write NOTHING
        # into it — the whitelist's never-touch promise, write side.
        result["unrecognized"].append(
            (str(tdir), "ticker-like folder with no data files"))
        return swept
    changed = manifest.get("generation", 0) == 0
    for (iv, key), f in sorted(found.items()):
        months = manifest_months(manifest, iv)
        entry = months.get(key)
        fp = f.path                       # DirEntry path string (== str(the Path))
        try:
            # FRESH stat at COMPARE time, NOT DirEntry.stat() — on Windows the latter
            # is cached at os.scandir() enumeration time, so a file rewritten by a
            # concurrent ingest/Fix-data DURING this scan would keep its stale size+
            # mtime and be wrongly trusted by the fast path (a missed re-parse). The
            # walk above stays syscall-free; only this recognized-file loop re-stats.
            st = os.stat(fp)
        except OSError as exc:
            result["errors"].append((fp, f"unreadable: {exc}"))
            continue
        if (entry and entry.get("status") == "present"
                and entry.get("size") == st.st_size
                and entry.get("mtime_ns") == st.st_mtime_ns):
            continue                      # trusted fast path
        try:
            bars, stats = read_month_file(Path(fp))
        except StorageFormatError as exc:
            result["errors"].append((fp, str(exc)))
            new = dict(entry or {}, status="format-error")
            if new != entry:              # no manifest churn on rescans
                months[key] = new
                changed = True
            continue
        except (StorageError, OSError) as exc:
            result["errors"].append((fp, f"unreadable: {exc}"))
            continue                      # transient — entry left alone
        new_entry = {
            "rows": stats["rows"],
            "first": f"{format_date(bars[0][0])} "
                     f"{format_time(bars[0][0].time())}",
            "last": f"{format_date(bars[-1][0])} "
                    f"{format_time(bars[-1][0].time())}",
            "size": stats["size"], "mtime_ns": stats["mtime_ns"],
            "sha256": stats["sha256"],
            "source": (entry or {}).get("source", "found-by-scan"),
            "status": "present"}
        _preserve_matching_value_corrections(new_entry, entry)
        months[key] = new_entry
        changed = True
    for iv, ivd in manifest.get("intervals", {}).items():
        for key, entry in ivd.get("months", {}).items():
            if ((iv, key) not in found
                    and entry.get("status") in ("present", "format-error")):
                entry["status"] = "MISSING"   # tombstone, never dropped
                result["missing"].append((tdir.name, iv, key))
                changed = True
    if changed and not manifest_blocked:
        try:
            with ticker_transaction(tdir):
                if (reconcile_stage.exists()
                        or reconcile_stage.is_symlink()):
                    raise StorageError(
                        "volatility correction transaction became active")
                merged = _merge_scan_manifest_for_save(tdir, manifest)
                if merged != load_manifest(tdir):
                    save_manifest(tdir, merged)
                manifest = merged
        except StorageError as exc:
            result["warnings"].append(f"{tdir.name}: manifest not "
                                      f"saved ({exc})")
    summary = {"symbol": manifest.get("symbol", tdir.name),
               "name": manifest.get("name", ""),
               "conid": manifest.get("conid"),      # security_id (IBKR conId)
               "intervals": {}, "flags": []}
    for iv, ivd in sorted(manifest.get("intervals", {}).items()):
        present = {k: v for k, v in ivd.get("months", {}).items()
                   if v.get("status") == "present"}
        n_missing = sum(1 for v in ivd.get("months", {}).values()
                        if v.get("status") == "MISSING")
        n_err = sum(1 for v in ivd.get("months", {}).values()
                    if v.get("status") == "format-error")
        if n_missing:
            summary["flags"].append(f"{iv}: {n_missing} month(s) MISSING")
        if n_err:
            summary["flags"].append(f"{iv}: {n_err} month(s) unreadable")
        if not present:
            continue
        keys = sorted(present)
        summary["intervals"][iv] = {
            "n_months": len(keys),
            "first": present[keys[0]]["first"],
            "last": present[keys[-1]]["last"],
            "rows": sum(v.get("rows", 0) for v in present.values())}
    result["tickers"][tdir.name] = summary
    return swept
