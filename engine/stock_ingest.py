"""Stock Data Storage — Tier 1: mixed-bag ingest (GUI-free).

Takes an arbitrary pile of vendor files (CSV in several dialects, parquet,
junk) and lands every provable bar in the Tier-0 tree through the strict
writer. The PARSER is deliberately tolerant — foreign files come padded,
LF-ended, tz-suffixed, pandas-indexed — but everything it cannot PROVE is
refused, never guessed. stock_storage.py stays the only thing that writes
bar bytes; this module decides what may reach it.

POLICY (the A/B/C buckets agreed with the user)
  A  auto-fix silently, count it      provably lossless re-encoding only:
       padded dates/hours, LF endings, BOM, integral-float volume
       ("234682.0"), non-canonical price text ("28.20"), unsorted rows.
  B  auto-fix + log loudly           interpretation that is sound but worth
       eyes: tz-offset/epoch timestamps converted to America/New_York,
       interval taken from MEASURED bar spacing when the filename lies
       (the user's "3y 1s" files are really 1-minute), pandas index
       column ignored, extra vendor columns (average/barCount) ignored,
       symbol column overriding a conflicting filename.
  C  quarantine / flag, never coerce  anything indicating bad raw data or
       unprovable identity: impossible bars, non-positive prices,
       sub-second stamps, in-file same-timestamp contradictions,
       ambiguous M/D-vs-D/M dates, unverifiable intervals, >40% non-RTH
       rows, >5% invalid rows, undecodable/container files, and the
       BASIS GATE below.

THE BASIS GATE (split/vendor-mix protection)
  Before any month of a series is committed, the file's overlap with the
  EXISTING series is dry-run merged. If >=100 timestamps overlap and more
  than 10% of them disagree, the file is on a different adjustment basis
  or from a different vendor (NVDA raw-vs-split-adjusted differs on every
  row) — NOTHING from that file is written for that series, and the
  report says so with a price-vs-volume breakdown. ONE exception (Tier 3
  M2b): when a RECORDED price action (stock_basis, manifest "actions")
  explains the disagreement — incoming ~= existing * factor (or its
  inverse; the file may sit on either side of the boundary) — the file
  merges with a loud series note. The factor is COMPARISON-ONLY: written
  bytes are never scaled and existing values still win conflicts. An
  unexplained halt points at the basis doctor (stock_basis), which
  classifies and records the boundary.

MERGE RULES (per month, through the Tier-0 writer)
  identical duplicate          dropped, counted
  same timestamp, different    EXISTING WINS; full count + capped examples
                               to _ingest_reports/<run>/conflicts.jsonl
  new timestamp                added; month rewritten atomically
  month unchanged              file left untouched (idempotent re-runs)
  existing month unreadable    that month is BLOCKED (never overwritten)

LAYOUT (both reserved to the scanner, which skips "_"-prefixed root dirs)
  Stock Data Storage/_quarantine/<run>/      copies of refused files,
                                             <name>.reason.txt, rejected
                                             row evidence
  Stock Data Storage/_ingest_reports/<run>/  report.json, conflicts.jsonl
  Stock Data Storage/_ingest_reports/ledger.jsonl  append-only sha256
                                             ledger of per-file outcomes;
                                             verify_paths() rides it to
                                             confirm containment without
                                             re-parsing

Source files are READ-ONLY to this module: never moved, never deleted —
quarantine copies, the originals stay where they were.

Ordering: files already in the native byte contract are processed first
(they ARE the archive this tree was born from), then everything else
top-level-first; first writer wins later conflicts and provenance records
who contributed what.

Parallelism: the per-file PARSE stage is pure and runs on a spawn
process pool for large batches (INGEST_WORKERS env caps it; 1 = serial;
default leaves two cores of thermal headroom). COMMITS always run in
the main process, strictly in the contractual file order, so the
resulting tree is byte-identical to a single-core run; any pool problem
silently degrades to the single-core path mid-run.
"""

import csv
import json
import os
import re
import shutil
import time as _time
from collections import Counter, deque
from copy import deepcopy
from itertools import count as _count
from datetime import datetime, timezone
from pathlib import Path

import stock_storage as ss
import stock_basis as sb
import operation_gate

# --- knobs (documented in the module docstring) -------------------------------
NON_RTH_QUARANTINE_FRAC = 0.40
INVALID_QUARANTINE_FRAC = 0.05
BASIS_GATE_MIN_OVERLAP = 100
BASIS_GATE_CONFLICT_FRAC = 0.10
CONFLICT_LOG_CAP_PER_MONTH = 200
REJECTED_ROWS_FILE_CAP = 100_000
INTERVAL_MODE_MIN_FRAC = 0.25       # modal delta must be >=25% of deltas
INTERVAL_MULTIPLE_MIN_FRAC = 0.90   # >=90% of deltas multiples of the mode
MIN_DELTAS_TO_VERIFY = 10
PREFLIGHT_PARQUET_EXPANSION = 8     # CSV-out ≈ 8x parquet-in (measured ~6x)
PREFLIGHT_MARGIN = 1.2
PREFLIGHT_FLOOR = 256 * 1024 ** 2

# --- parallel parse knobs (parse is pure; commits stay serial in-order) -------
INGEST_WORKERS_ENV = "INGEST_WORKERS"  # mirrors VP_SWEEP_WORKERS; 1 = serial
INGEST_MP_MIN_FILES = 4                # below this a pool never pays off
INGEST_MP_MIN_BYTES = 32 * 1024 ** 2   # weighted source bytes (~8s serial)
INGEST_WINDOW_BYTES = 128 * 1024 ** 2  # weighted source bytes in flight; may
                                       # overshoot by exactly one file (at
                                       # least 1 always stays in flight)
INGEST_INFLIGHT_PER_WORKER = 2
VERIFY_MONTH_CACHE_MAX = 64            # months held for compare; files
                                       # arrive ticker-grouped, so a small
                                       # window has near-perfect locality
                                       # (unbounded = MemoryError at S&P
                                       # scale, proven on 18k files)

QUARANTINE_DIR = "_quarantine"
REPORTS_DIR = "_ingest_reports"
LEDGER_NAME = "ledger.jsonl"        # sha256 -> outcome, verify fast path

JUNK_NAMES = frozenset({".ds_store", "thumbs.db", "desktop.ini"})
DATA_EXTS = frozenset({".csv", ".txt", ".parquet", ".pq"})

# header-cell aliases (normalized: lowercase, stripped, spaces/underscores out)
_DATE_NAMES = frozenset({"date", "day"})
_TIME_NAMES = frozenset({"time"})
_COMBINED_NAMES = frozenset({"datetime", "timestamp", "date", "time", "ts",
                             "epoch", "datetimeet", "barstart"})
_OHLCV_NAMES = {"open": "open", "o": "open",
                "high": "high", "h": "high",
                "low": "low", "l": "low",
                "close": "close", "c": "close",
                "volume": "volume", "vol": "volume", "v": "volume"}
_SYMBOL_NAMES = frozenset({"symbol", "ticker", "sym"})
_INDEX_NAMES = frozenset({"", "index", "unnamed:0", "#"})
_ADJ_NAMES = frozenset({"adjclose", "adjustedclose", "adj"})

_SLASH_DATE_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
_ISO_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_TIME_CELL_RE = re.compile(
    r"^(\d{1,2}):(\d{2})(?::(\d{2}))?(?:\.(\d+))?\s*([AaPp])?\.?[Mm]?\.?$")
_EPOCH_RE = re.compile(r"^\d{9,19}(?:\.0+)?$")
_HINT_RE = re.compile(r"_(\d{1,4})(s|m|h)(?![a-z0-9])")

_NY = None


def _ny_zone():
    """ZoneInfo('America/New_York'), cached; None when tzdata is missing
    (the Tier-0 environment guard tells the user how to fix that)."""
    global _NY
    if _NY is None:
        try:
            from zoneinfo import ZoneInfo
            _NY = ZoneInfo("America/New_York")
        except Exception:  # noqa: BLE001
            _NY = False
    return _NY or None


_RUN_SEQ = _count()    # same-second runs must not share their
                       # report/quarantine dirs (proven by selftest)


def _run_id():
    return (f"ing-{datetime.now():%Y%m%d-%H%M%S}-{os.getpid()}"
            f"-{next(_RUN_SEQ)}")


# --- source discovery ----------------------------------------------------------

def _collect_sources(paths, root):
    """Expand files/dirs into the ordered work list. Junk and non-data
    extensions are skipped (reported), anything inside the storage tree is
    refused (self-ingest), duplicates collapse. Ordering: native-contract
    CSVs first (they are the archive's own format and become the golden
    series for any same-run overlap), then shallow-before-deep."""
    root_res = str(Path(root).resolve()).lower()
    seen, files, skipped = set(), [], []
    queue = [Path(p) for p in paths]
    for p in queue:
        try:
            if p.is_dir():
                for f in sorted(p.rglob("*")):
                    if f.is_file():
                        queue.append(f)
                continue
        except OSError as exc:
            skipped.append((str(p), f"unreadable: {exc}"))
            continue
        try:
            rp = str(p.resolve())
        except OSError:
            rp = str(p)
        if rp.lower() in seen:
            continue
        seen.add(rp.lower())
        name = p.name.lower()
        if rp.lower().startswith(root_res + os.sep) or rp.lower() == root_res:
            skipped.append((str(p), "inside the storage tree (not a source)"))
        elif name in JUNK_NAMES or name.endswith(".tmp"):
            skipped.append((str(p), "junk/system file"))
        elif p.suffix.lower() not in DATA_EXTS:
            skipped.append((str(p), f"not a data extension "
                                    f"({p.suffix or 'none'})"))
        elif not p.is_file():
            skipped.append((str(p), "vanished before ingest"))
        else:
            files.append(Path(rp))

    def _order(f):
        try:
            with open(f, "rb") as fh:
                native = fh.read(len(ss.HEADER) + 2).startswith(
                    (ss.HEADER + ss.EOL).encode("ascii"))
        except OSError:
            native = False
        return (0 if native else 1, len(f.parts), str(f).lower())

    files.sort(key=_order)
    return files, skipped


def _sniff(path):
    """First-bytes container check — extensions lie ('parquet renamed
    .csv' is a real vendor habit, and an .xlsx is a zip)."""
    try:
        with open(path, "rb") as fh:
            head = fh.read(8)
    except OSError as exc:
        return f"error:{exc}"
    if not head:
        return "empty"
    if head.startswith(b"PAR1"):
        return "parquet"
    if head.startswith(b"PK\x03\x04"):
        return "zip"
    if head.startswith(b"\x1f\x8b"):
        return "gzip"
    return "text"


def _decode(raw):
    """Bytes -> text with honest encoding handling: UTF-8(-BOM) first,
    UTF-16 when the BOM/NUL pattern says so. Anything else is refused —
    a mojibake'd price is corruption, not data."""
    notes = []
    if raw.startswith(b"\xef\xbb\xbf"):
        notes.append("UTF-8 BOM stripped")
        raw = raw[3:]
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")) or b"\x00" in raw[:4096]:
        try:
            text = raw.decode("utf-16")
            notes.append("decoded as UTF-16")
            return text, notes
        except UnicodeDecodeError:
            raise ValueError("undecodable bytes (not UTF-8 or UTF-16)")
    try:
        return raw.decode("utf-8"), notes
    except UnicodeDecodeError as exc:
        raise ValueError(f"undecodable bytes ({exc})")


def _detect_delimiter(header_line):
    counts = {d: header_line.count(d) for d in (",", ";", "\t")}
    best = max(counts, key=counts.get)
    return best if counts[best] >= 4 else None


def _norm_cell(s):
    return re.sub(r"[\s_]+", "", str(s).strip().lower())


def _map_columns(cells):
    """Header cells -> {role: column-index}. Returns (mapping, notes) or
    (None, reason). Headerless native files (7 fields, first a date) are
    recognized as the archive's own column order."""
    notes = []
    norm = [_norm_cell(c) for c in cells]
    if all(n not in _OHLCV_NAMES and n not in _COMBINED_NAMES
           and n not in _DATE_NAMES for n in norm):
        if len(cells) == 7 and (_SLASH_DATE_RE.match(cells[0].strip())
                                or _ISO_DATE_RE.match(cells[0].strip())):
            notes.append("headerless file — native column order assumed "
                         "(Date,Time,open,high,low,close,volume)")
            return ({"date": 0, "time": 1, "open": 2, "high": 3, "low": 4,
                     "close": 5, "volume": 6, "headerless": True}, notes)
        return None, "no recognizable header"
    m, ignored = {}, []
    for i, n in enumerate(norm):
        if n in _INDEX_NAMES:
            if n in ("", "index", "unnamed:0"):
                notes.append(f"column {i} ({cells[i]!r} — pandas index?) "
                             f"ignored")
            continue
        if n in _OHLCV_NAMES:
            role = _OHLCV_NAMES[n]
            if role in m:               # silent drop hid a second 'close'
                notes.append(f"DUPLICATE {role} column {cells[i]!r} "
                             f"ignored — the FIRST one was used")
            else:
                m[role] = i
        elif n in _SYMBOL_NAMES:
            if "symbol" in m:
                notes.append(f"DUPLICATE symbol column {cells[i]!r} "
                             f"ignored — the FIRST one was used")
            else:
                m["symbol"] = i
        elif n in _DATE_NAMES and "date" not in m:
            m["date"] = i
        elif n in _TIME_NAMES and "time" not in m:
            m["time"] = i
        else:
            if n in _ADJ_NAMES:
                notes.append(f"ADJUSTED-CLOSE column {cells[i]!r} ignored — "
                             f"if 'close' is unadjusted this file is on a "
                             f"different basis than the archive (task #23)")
            ignored.append(cells[i])
    missing = [k for k in ("open", "high", "low", "close", "volume")
               if k not in m]
    if missing:
        return None, f"missing required column(s): {', '.join(missing)}"
    if not ("date" in m and "time" in m):
        # single timestamp-ish column: combined datetime or epoch
        for i, n in enumerate(norm):
            if n in _COMBINED_NAMES and i not in m.values():
                m["combined"] = i
                if cells[i] in ignored:      # claimed, not ignored
                    ignored.remove(cells[i])
                break
        else:
            if "date" in m:          # date-only column carrying datetimes
                m["combined"] = m.pop("date")
    if ignored:
        notes.append("ignored column(s): " + ", ".join(map(repr, ignored)))
    if "date" in m and "time" in m or "combined" in m:
        return m, notes
    return None, "no date/time information in the header"


# --- timestamp parsing ----------------------------------------------------------

def _slash_evidence(date_iter):
    """Prove M/D vs D/M from the data itself, scanning EVERY slash date
    before delivering a verdict — an early exit made the verdict depend
    on row order and let a file with LATE D/M evidence bypass the
    inconsistent-dates quarantine (adversarial review, proven). A
    first-field value >12 disproves M/D, a second-field >12 proves it;
    both present = the file contradicts itself. No proof = no guess."""
    saw_first13 = saw_second13 = False
    n = 0
    for cell in date_iter:
        mm = _SLASH_DATE_RE.match(cell)
        if not mm:
            continue
        n += 1
        if int(mm.group(1)) > 12:
            saw_first13 = True
        if int(mm.group(2)) > 12:
            saw_second13 = True
    if saw_first13 and saw_second13:
        return None, ("slash dates have values >12 in BOTH positions — "
                      "inconsistent date fields")
    if saw_first13:
        return None, ("dates appear to be D/M/YYYY — not supported; "
                      "convert to M/D/YYYY or ISO and re-ingest")
    if saw_second13:
        return "mdy", None
    if n == 0:
        return "none", None          # no slash dates at all (ISO file)
    return None, ("date format ambiguous (every value <=12 in both "
                  "positions) — cannot prove M/D vs D/M; convert to ISO "
                  "YYYY-MM-DD and re-ingest")


def _parse_time_cell(cell):
    """'9:30:00' / '09:30' / '9:30:00.000' / '9:30 PM' -> (h, m, s) or a
    problem string. Non-zero sub-second fractions are refused (the storage
    grid is whole seconds)."""
    tm = _TIME_CELL_RE.match(cell.strip())
    if not tm:
        return f"unparseable time {cell!r}"
    h, mi = int(tm.group(1)), int(tm.group(2))
    s = int(tm.group(3) or 0)
    if tm.group(4) and int(tm.group(4)) != 0:
        return f"sub-second time {cell!r}"
    ap = (tm.group(5) or "").lower()
    if ap == "p" and h != 12:
        h += 12
    elif ap == "a" and h == 12:
        h = 0
    if h > 23 or mi > 59 or s > 59:
        return f"impossible time {cell!r}"
    return (h, mi, s)


def _epoch_scale(v):
    if v < 1e11:
        return 1
    if v < 1e14:
        return 1_000
    if v < 1e17:
        return 1_000_000
    return 1_000_000_000


class _TsParser:
    """Per-file timestamp strategy, decided once from the header mapping +
    a data sample, then applied per row. Tracks which B-bucket conversions
    (tz, epoch) actually fired so the report can say so."""

    def __init__(self, mapping, sample_cells):
        self.mapping = mapping
        self.slash_mode = None        # set by caller after evidence pass
        self.mode = None              # split | combined | epoch
        self.tz_converted = 0
        self.epoch_converted = 0
        self.problem = None
        if "date" in mapping and "time" in mapping:
            self.mode = "split"
        elif "combined" in mapping:
            sample = next((c for c in sample_cells if c.strip()), "")
            self.mode = "epoch" if _EPOCH_RE.match(sample.strip()) \
                else "combined"
        else:
            self.problem = "no timestamp columns"
        if self.mode in ("epoch",) and _ny_zone() is None:
            self.problem = ("epoch timestamps need the timezone database — "
                            "pip install tzdata")

    def parse(self, cells):
        """-> datetime | problem string. Naive, America/New_York wall."""
        m = self.mapping
        if self.mode == "split":
            d = self._date(cells[m["date"]].strip())
            if isinstance(d, str):
                return d
            t = _parse_time_cell(cells[m["time"]])
            if isinstance(t, str):
                return t
            try:
                return datetime(d[0], d[1], d[2], t[0], t[1], t[2])
            except ValueError:
                return f"impossible calendar date {cells[m['date']]!r}"
        cell = cells[m["combined"]].strip()
        if self.mode == "epoch":
            try:
                v = float(cell)
            except ValueError:
                return f"unparseable epoch {cell!r}"
            scale = _epoch_scale(v)
            if v / scale != v // scale:
                return f"fractional epoch second {cell!r}"
            try:
                utc = datetime.fromtimestamp(v / scale, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                return f"epoch {cell!r} out of datetime range"
            self.epoch_converted += 1
            return utc.astimezone(_ny_zone()).replace(tzinfo=None)
        try:
            dt = datetime.fromisoformat(cell)
        except ValueError:
            parts = cell.split("T", 1) if "T" in cell \
                else cell.split(" ", 1)
            if len(parts) != 2:
                return f"unparseable timestamp {cell!r}"
            d = self._date(parts[0].strip())
            if isinstance(d, str):
                return d
            t = _parse_time_cell(parts[1])
            if isinstance(t, str):
                return t
            try:
                return datetime(d[0], d[1], d[2], t[0], t[1], t[2])
            except ValueError:
                return f"impossible calendar date {parts[0]!r}"
        if dt.tzinfo is not None:
            ny = _ny_zone()
            if ny is None:
                return ("tz-aware timestamp needs the timezone database — "
                        "pip install tzdata")
            self.tz_converted += 1
            dt = dt.astimezone(ny).replace(tzinfo=None)
        if dt.microsecond:
            return f"sub-second timestamp {cell!r}"
        return dt

    def _date(self, cell):
        im = _ISO_DATE_RE.match(cell)
        if im:
            return int(im.group(1)), int(im.group(2)), int(im.group(3))
        sm = _SLASH_DATE_RE.match(cell)
        if sm:
            if self.slash_mode != "mdy":
                return f"slash date {cell!r} without proven M/D order"
            return int(sm.group(3)), int(sm.group(1)), int(sm.group(2))
        return f"unparseable date {cell!r}"


# --- interval inference ----------------------------------------------------------

def _interval_token(seconds):
    if seconds <= 0:
        return None
    if seconds % 3600 == 0:
        n, u = seconds // 3600, "h"
    elif seconds % 60 == 0:
        n, u = seconds // 60, "m"
    else:
        n, u = seconds, "s"
    tok = f"{n}{u}"
    return tok if ss.INTERVAL_RE.match(tok) else None


def _hint_token(name):
    """The interval the FILENAME claims ('KO_3y_1s(1)' -> '1s'); None when
    absent or self-contradictory. Only a hint — the data gets the vote.
    Non-canonical tokens ('_0s', '_05m') are discarded: an unvalidated
    hint reached month_file_path and crashed the run (adversarial
    review, proven)."""
    toks = {f"{n}{u}" for n, u in _HINT_RE.findall(name.lower())}
    toks = {t for t in toks if ss.INTERVAL_RE.match(t)}
    return toks.pop() if len(toks) == 1 else None


def _measure_interval(dts):
    """Modal spacing of consecutive bars WITHIN a calendar day (overnight
    and weekend gaps never pollute it). Returns (token, detail) or
    (None, reason). Sparse series (illiquid 1s bars) are why the modal
    fraction floor is 0.25 with a 90% integer-multiple backstop."""
    deltas = Counter()
    for i in range(1, len(dts)):
        a, b = dts[i - 1], dts[i]
        if a.date() == b.date():
            deltas[int((b - a).total_seconds())] += 1
    total = sum(deltas.values())
    if total < MIN_DELTAS_TO_VERIFY:
        return None, f"too few intra-day deltas ({total}) to verify"
    mode, n_mode = deltas.most_common(1)[0]
    tok = _interval_token(mode)
    if tok is None:
        return None, f"modal bar spacing {mode}s is not a canonical interval"
    frac = n_mode / total
    mult = sum(n for d, n in deltas.items()
               if d > 0 and d % mode == 0) / total
    if frac < INTERVAL_MODE_MIN_FRAC or mult < INTERVAL_MULTIPLE_MIN_FRAC:
        return None, (f"bar spacing too irregular to trust (modal {mode}s "
                      f"covers {frac:.0%}, multiples {mult:.0%})")
    return tok, (f"measured {tok} (modal spacing {mode}s, {frac:.0%} of "
                 f"{total} intra-day deltas)")


# --- row pipeline ----------------------------------------------------------------

class _FileParse:
    """Everything one source file produced, before any tree contact.
    Instances cross a process boundary (spawn pickle) on the parallel
    parse path: every attribute must stay stdlib-picklable."""

    def __init__(self, path):
        self.path = Path(path)
        self.status = "ok"            # ok|quarantine|needs-decision|empty|error
        self.reason = ""
        self.notes = []
        # Commit-only provenance.  Ratio-month correction evidence may move
        # forward across an ordinary append only when the manifest entry
        # names the exact bytes read before that append.  Parse workers leave
        # this empty; the serial writer fills it immediately before publish.
        self.prewrite_month_shas = {}
        self.counters = Counter()
        self.groups = {}              # canon -> [bar...]
        self.raw_symbols = {}         # canon -> {raw spellings}
        self.rejected = []            # (line_no, raw, reason) capped
        self.non_rth_examples = []
        self.group_meta = {}          # canon -> {"interval": tok, ...}
        self.merge = {}               # canon -> merge result dict
        self.msgs = []                # parse-stage progress (sans [i/N])
        self.sha256 = None            # raw source bytes, for the ledger
        self.size = 0


def _reject(fp, line_no, raw, reason):
    fp.counters["invalid_rows"] += 1
    if len(fp.rejected) < REJECTED_ROWS_FILE_CAP:
        fp.rejected.append((line_no, raw, reason))


def _parse_rows(fp, header_cells, rows, fallback_ticker):
    """The shared row pipeline (CSV and parquet land here). Fills
    fp.groups with contract-clean bar tuples; everything else is counted
    and (for C-bucket rows) captured as evidence."""
    mapping, notes = _map_columns(header_cells)
    if mapping is None:
        fp.status, fp.reason = "quarantine", notes
        return
    fp.notes.extend(notes)
    n_cols_needed = max(v for k, v in mapping.items() if k != "headerless")

    sample = []
    if "combined" in mapping:
        sample = [r[mapping["combined"]] for r in rows[:5]
                  if len(r) > mapping["combined"]]
    parser = _TsParser(mapping, sample)
    if parser.problem:
        fp.status, fp.reason = "quarantine", parser.problem
        return
    if parser.mode == "split" or parser.mode == "combined":
        idx = mapping.get("date", mapping.get("combined"))
        date_iter = (r[idx].split(" ")[0].split("T")[0]
                     for r in rows if len(r) > idx)
        verdict, why = _slash_evidence(date_iter)
        if verdict is None:
            fp.status, fp.reason = "quarantine", why
            return
        parser.slash_mode = verdict

    sym_idx = mapping.get("symbol")
    sym_cache = {}
    get = mapping.get
    i_o, i_h, i_l, i_c, i_v = (get("open"), get("high"), get("low"),
                               get("close"), get("volume"))
    for line_no, cells in enumerate(rows, start=2):
        if not cells or (len(cells) == 1 and not cells[0].strip()):
            fp.counters["blank_lines"] += 1
            continue
        if len(cells) <= n_cols_needed:
            _reject(fp, line_no, ",".join(cells),
                    f"{len(cells)} field(s), need >{n_cols_needed}")
            continue
        raw = ",".join(cells)
        dt = parser.parse(cells)
        if isinstance(dt, str):
            _reject(fp, line_no, raw, dt)
            continue
        if not 1900 <= dt.year <= 2100:
            _reject(fp, line_no, raw, f"year {dt.year} implausible")
            continue
        if dt.weekday() > 4 or not (ss.RTH_FIRST <= dt.time()
                                    <= ss.RTH_LAST):
            fp.counters["non_rth"] += 1
            if len(fp.non_rth_examples) < 5:
                fp.non_rth_examples.append(raw[:120])
            continue
        try:
            o = float(cells[i_o]); h = float(cells[i_h])
            lo = float(cells[i_l]); c = float(cells[i_c])
        except ValueError:
            _reject(fp, line_no, raw, "non-numeric price")
            continue
        bad = next((f"{n} not positive finite" for n, v in
                    (("open", o), ("high", h), ("low", lo), ("close", c))
                    if v != v or v <= 0.0
                    or v in (float("inf"), float("-inf"))), None)
        if bad:
            _reject(fp, line_no, raw, bad)
            continue
        if not (lo <= min(o, c) and max(o, c) <= h):
            _reject(fp, line_no, raw,
                    f"impossible bar ({o},{h},{lo},{c})")
            continue
        v_cell = cells[i_v].strip()
        try:
            vol = int(v_cell)
        except ValueError:
            try:
                fv = float(v_cell)
            except ValueError:
                _reject(fp, line_no, raw, f"unparseable volume {v_cell!r}")
                continue
            if fv != fv or not fv.is_integer():
                _reject(fp, line_no, raw,
                        f"non-integral volume {v_cell!r}")
                continue
            vol = int(fv)
            fp.counters["volume_float_normalized"] += 1
        if vol < 0:
            _reject(fp, line_no, raw, f"negative volume {vol}")
            continue
        if sym_idx is not None:
            raw_sym = cells[sym_idx].strip()
            canon = sym_cache.get(raw_sym, "?")
            if canon == "?":
                try:
                    canon = ss.canonical_ticker(raw_sym)
                except ss.StorageError:
                    canon = None
                sym_cache[raw_sym] = canon
            if canon is None:
                _reject(fp, line_no, raw, f"bad symbol {raw_sym!r}")
                continue
            fp.raw_symbols.setdefault(canon, set()).add(raw_sym)
        else:
            canon = fallback_ticker
        fp.groups.setdefault(canon, []).append((dt, o, h, lo, c, vol))
        fp.counters["parsed_ok"] += 1

    if parser.tz_converted:
        fp.notes.append(f"{parser.tz_converted:,} tz-aware timestamp(s) "
                        f"converted to America/New_York wall time")
    if parser.epoch_converted:
        fp.notes.append(f"{parser.epoch_converted:,} epoch timestamp(s) "
                        f"interpreted as UTC and converted to New York")
    if fp.counters["volume_float_normalized"]:
        fp.notes.append(f"{fp.counters['volume_float_normalized']:,} "
                        f"integral-float volume(s) normalized (e.g. "
                        f"'234682.0' -> 234682)")


def _finish_groups(fp):
    """Per (file, ticker): sort, settle duplicates, verify the interval.
    Same-timestamp CONTRADICTIONS eject every row at that stamp (the file
    disagrees with itself — C bucket); identical repeats just drop."""
    hint = _hint_token(fp.path.name)
    for canon in sorted(fp.groups):
        bars = sorted(fp.groups[canon], key=lambda b: b[0])
        if not bars:
            continue
        unsorted = any(bars[i][0] != fp.groups[canon][i][0]
                       for i in range(len(bars)))
        if unsorted:
            fp.counters["rows_sorted"] += 1
        uniq, i, dup_n, conf_rows = [], 0, 0, 0
        while i < len(bars):
            j = i
            while j < len(bars) and bars[j][0] == bars[i][0]:
                j += 1
            run = bars[i:j]
            if len(set(run)) == 1:
                uniq.append(run[0])
                dup_n += len(run) - 1
            else:
                conf_rows += len(run)
                if len(fp.rejected) < REJECTED_ROWS_FILE_CAP:
                    for b in run:
                        fp.rejected.append(
                            (0, ss.format_bar(*b),
                             f"in-file contradiction at {b[0]}"))
            i = j
        fp.counters["infile_dup_identical"] += dup_n
        fp.counters["infile_conflict_rows"] += conf_rows
        meta = {"ticker": canon, "rows": len(uniq), "hint": hint}
        tok, detail = _measure_interval([b[0] for b in uniq])
        if tok is None:
            if hint and "too few" in detail:
                meta["interval"] = hint
                meta["interval_note"] = (f"interval {hint} taken from the "
                                         f"FILENAME, unverified ({detail})")
            else:
                meta["interval"] = None
                meta["interval_note"] = f"interval unverifiable: {detail}"
        else:
            meta["interval"] = tok
            meta["interval_note"] = detail
            if hint and hint != tok:
                meta["interval_note"] = (
                    f"FILENAME SAYS {hint} BUT THE DATA IS {tok} — "
                    f"{detail}; stored as {tok}")
        fp.groups[canon] = uniq
        fp.group_meta[canon] = meta


# --- readers ---------------------------------------------------------------------

def _read_csv(fp):
    try:
        raw = fp.path.read_bytes()
    except OSError as exc:
        fp.status, fp.reason = "error", f"unreadable: {exc}"
        return None, None
    if not raw.strip():
        fp.status, fp.reason = "empty", "file is empty"
        return None, None
    try:
        text, notes = _decode(raw)
    except ValueError as exc:
        fp.status, fp.reason = "quarantine", str(exc)
        return None, None
    fp.notes.extend(notes)
    lines = text.splitlines()
    delim = _detect_delimiter(lines[0]) or ","
    if delim != ",":
        fp.notes.append(f"delimiter {delim!r}")
    rows = list(csv.reader(lines, delimiter=delim))
    if not rows:
        fp.status, fp.reason = "empty", "no rows"
        return None, None
    header = rows[0]
    mapping_probe, _n = _map_columns(header)
    if mapping_probe is not None and mapping_probe.get("headerless"):
        return header, rows            # first line IS data
    return header, rows[1:]


def _read_parquet(fp):
    """Parquet -> the same string-cell rows the CSV path uses (str() of a
    float is repr — lossless). Needs pandas+pyarrow; their absence is a
    quarantine reason, not a crash."""
    try:
        import pandas as pd  # noqa: F401
    except Exception:  # noqa: BLE001
        fp.status, fp.reason = ("quarantine",
                                "parquet needs pandas+pyarrow installed")
        return None, None
    try:
        import pandas as pd
        df = pd.read_parquet(fp.path)
    except Exception as exc:  # noqa: BLE001
        fp.status, fp.reason = "quarantine", f"parquet unreadable: {exc}"
        return None, None
    header = [str(c) for c in df.columns]
    cols = []
    for c in df.columns:
        s = df[c]
        if str(s.dtype).startswith("datetime64"):
            if getattr(s.dtype, "tz", None) is not None:
                ny = _ny_zone()
                if ny is None:
                    fp.status, fp.reason = (
                        "quarantine", "tz-aware parquet timestamps need "
                                      "the timezone database (pip install "
                                      "tzdata)")
                    return None, None
                s = s.dt.tz_convert("America/New_York").dt.tz_localize(None)
                fp.notes.append(f"parquet column {c!r}: tz-aware -> "
                                f"New York wall time")
            cols.append([x.isoformat(sep=" ") if x == x else "NaT"
                         for x in s])
        else:
            cols.append([str(x) for x in s.tolist()])
    rows = [list(t) for t in zip(*cols)] if cols else []
    return header, rows


# --- tree merge -------------------------------------------------------------------

def _factor_disagree(bars, exist_map, factor):
    """The gate's overlap recount under a candidate basis factor: a row
    disagrees when any PRICE field (o/h/l/c — a price action never
    explains volume) is off incoming ~= existing*factor by more than
    the basis doctor's PRICE_TOL (the factor was measured/recorded
    under that tolerance; exact equality would refuse cents-rounded
    vendor data and the whole 1/f direction). COMPARISON ONLY."""
    n = 0
    for b in bars:
        e = exist_map.get(b[0])
        if e is not None and any(
                abs(b[k] - e[k] * factor) > sb.PRICE_TOL * e[k] * factor
                for k in (1, 2, 3, 4)):
            n += 1
    return n


def _gate_unlock(root, canon, bars, plan, overlap):
    """Tier 3 M2b: once the raw gate has fired, ask the ticker MANIFEST
    whether a RECORDED price action (stock_basis) explains the overlap
    disagreement — under its factor or the inverse (the file may sit
    on either side of the boundary). Returns the loud series note, or
    None to keep the gate shut. Called lazily so the happy path never
    reads the manifest; a malformed manifest never unlocks."""
    try:
        actions = sb.load_actions(root, canon)
    except Exception:  # noqa: BLE001 — bad manifest keeps the gate shut
        return None
    cands = []
    for act in actions:
        if not isinstance(act, dict) \
                or act.get("applies") not in ("price", "both"):
            continue
        try:
            f = float(act.get("factor"))
        except (TypeError, ValueError):
            continue
        if 0.0 < f < float("inf"):
            cands += [(f, act), (1.0 / f, act)]
    if not cands:
        return None
    exist_map = {b[0]: b for _y, _m, _p, ex, _add, _d, _c, _sha in plan
                 for b in ex}
    for cand, act in cands:
        if (_factor_disagree(bars, exist_map, cand) / overlap
                <= BASIS_GATE_CONFLICT_FRAC):
            return (f"overlap disagrees by recorded basis factor "
                    f"{cand:.6g} (action {act.get('date')}, "
                    f"{act.get('kind')}) — merged; comparison only, "
                    f"stored bytes unchanged, existing values still "
                    f"win conflicts")
    return None


def _merge_series(root, fp, canon, interval, bars, run_id, conflict_sink,
                  cancel):
    """Dry-run the whole file's contribution to one (ticker, interval)
    series against the existing tree, apply the BASIS GATE, then commit
    month by month. Returns the per-series result dict."""
    res = {"ticker": canon, "interval": interval, "months": {},
           "added": 0, "dup_existing": 0, "conflicts": 0,
           "price_conflicts": 0, "blocked_months": [], "written": 0,
           "skipped_identical": 0, "gate": None, "cancelled": False}
    plan = []
    for (y, m), mbars in sorted(ss.bucket_bars_by_month(bars).items()):
        path = ss.month_file_path(root, canon, y, m, interval)   # parquet target
        read_path = ss.find_month_file(root, canon, y, m, interval)   # any format
        existing, exist_map, existing_stats = [], {}, None
        if read_path is not None:
            try:
                existing, existing_stats = ss.read_month_file(read_path)
            except (ss.StorageError, OSError) as exc:
                res["blocked_months"].append(
                    {"month": ss.month_key(y, m),
                     "reason": f"existing file failed the strict read "
                               f"({exc}) — month left untouched"})
                continue
            exist_map = {b[0]: b for b in existing}
        added, n_dup, n_conf, n_price = [], 0, 0, 0
        for b in mbars:
            e = exist_map.get(b[0])
            if e is None:
                added.append(b)
            elif e == b:
                n_dup += 1
            else:
                n_conf += 1
                if any(abs(e[k] - b[k]) > 0.0 for k in (1, 2, 3, 4)):
                    n_price += 1
                conflict_sink(canon, interval, ss.month_key(y, m), e, b)
        res["dup_existing"] += n_dup
        res["conflicts"] += n_conf
        res["price_conflicts"] += n_price
        prewrite_sha = (existing_stats.get("sha256")
                        if isinstance(existing_stats, dict) else None)
        plan.append((y, m, path, existing, added, n_dup, n_conf,
                     prewrite_sha))
    overlap = res["dup_existing"] + res["conflicts"]
    if (overlap >= BASIS_GATE_MIN_OVERLAP
            and res["conflicts"] / overlap > BASIS_GATE_CONFLICT_FRAC):
        note = _gate_unlock(root, canon, bars, plan, overlap)
        if note is None:
            vol_only = res["conflicts"] - res["price_conflicts"]
            res["gate"] = (
                f"BASIS GATE: {res['conflicts']:,} of {overlap:,} "
                f"overlapping timestamps disagree with the existing "
                f"{canon} {interval} series ({res['price_conflicts']:,} "
                f"on price, {vol_only:,} volume-only) — different "
                f"adjustment basis or vendor. NOTHING from this file "
                f"was written for this series; run the basis doctor "
                f"(stock_basis) to classify and record this boundary.")
            return res
        res.setdefault("notes", []).append(note)
    for y, m, path, existing, added, n_dup, n_conf, prewrite_sha in plan:
        key = ss.month_key(y, m)
        if cancel is not None and cancel.is_set():
            res["cancelled"] = True
            res["months"][key] = {"status": "not written (cancelled)"}
            continue
        if not added:
            res["skipped_identical"] += 1
            res["months"][key] = {"status": "unchanged", "dups": n_dup,
                                  "conflicts": n_conf}
            continue
        published = ss.load_manifest(Path(root) / canon) or {}
        published_entry = (((published.get("intervals") or {}).get(interval)
                            or {}).get("months") or {}).get(key) or {}
        published_has_ledger = (
            isinstance(published_entry, dict)
            and "value_corrections" in published_entry)
        if published_has_ledger:
            if (not isinstance(prewrite_sha, str) or not prewrite_sha
                    or published_entry.get("sha256") != prewrite_sha):
                res["blocked_months"].append({
                    "month": key,
                    "reason": "correction metadata does not match the "
                              "active prewrite month; import refused",
                })
                return res
            candidate = deepcopy(published)
            for raw in sorted(fp.raw_symbols.get(canon, ())):
                if raw != canon and raw not in candidate.get("aliases", []):
                    candidate.setdefault("aliases", []).append(raw)
            source = _provenance(
                published_entry.get("source"), run_id, fp.path.name,
                len(added), n_dup, n_conf)
            try:
                stats, _published_manifest = (
                    ss.write_month_preserving_correction_ledger(
                        root, canon, interval, y, m, existing + added,
                        candidate,
                        {"status": "present", "source": source}))
            except (ss.StorageError, OSError) as exc:
                res["months"][key] = {
                    "status": f"WRITE FAILED: {exc}; recovery may be "
                              "required"}
                res["recovery_pending"] = bool(
                    (Path(root) / canon / ss.VOL_VALUE_RECONCILE_STAGE_DIR)
                    .exists())
                return res
            res["added"] += len(added)
            res["written"] += 1
            fp.prewrite_month_shas[(canon, interval, key)] = prewrite_sha
            res["months"][key] = {
                "status": "written", "added": len(added),
                "dups": n_dup, "conflicts": n_conf, "stats": stats,
                "manifest_committed": True,
            }
            continue
        try:
            stats = ss.write_month_file(path, existing + added)
        except (ss.StorageError, OSError) as exc:
            res["months"][key] = {"status": f"WRITE FAILED: {exc}"}
            continue
        # migrate: the Parquet is now canonical — drop a legacy CSV twin so the
        # slot holds exactly one file (the read-fallback no longer needs it).
        csv_twin = ss.month_file_path(root, canon, y, m, interval, fmt="csv")
        if csv_twin != path and csv_twin.exists():
            try:
                csv_twin.unlink()
            except OSError:
                pass
        res["added"] += len(added)
        res["written"] += 1
        fp.prewrite_month_shas[(canon, interval, key)] = prewrite_sha
        res["months"][key] = {"status": "written", "added": len(added),
                              "dups": n_dup, "conflicts": n_conf,
                              "stats": stats}
    return res


def _provenance(old, run_id, src_name, added, dups, conflicts):
    contributions = []
    if isinstance(old, dict) and isinstance(old.get("contributions"), list):
        contributions = old["contributions"]
    elif old and old != "found-by-scan":
        contributions = [{"note": str(old)}]
    contributions.append({"run": run_id, "file": src_name, "added": added,
                          "dups": dups, "conflicts": conflicts})
    return {"kind": "ingest", "contributions": contributions}


def _update_manifest(root, fp, canon, run_id):
    """Fold this file's committed months into the ticker manifest —
    AFTER the data files (manifest only ever behind the tree)."""
    tdir = Path(root) / canon
    manifest = ss.load_manifest(tdir) or ss.new_manifest(canon, canon)
    for raw in sorted(fp.raw_symbols.get(canon, ())):
        if raw != canon and raw not in manifest.get("aliases", []):
            manifest.setdefault("aliases", []).append(raw)
    res = fp.merge.get(canon)
    changed = False
    for iv_res in ([res] if isinstance(res, dict) else res or []):
        months = ss.manifest_months(manifest, iv_res["interval"])
        for key, info in iv_res["months"].items():
            if info.get("status") != "written":
                continue
            if info.get("manifest_committed") is True:
                continue
            old = months.get(key, {})
            new_entry = dict(
                info["stats"], status="present",
                source=_provenance(old.get("source"), run_id, fp.path.name,
                                   info["added"], info["dups"],
                                   info["conflicts"]))
            prewrite_sha = fp.prewrite_month_shas.get(
                (canon, iv_res["interval"], key))
            if (isinstance(old, dict)
                    and isinstance(prewrite_sha, str)
                    and prewrite_sha
                    and old.get("sha256") == prewrite_sha
                    and "value_corrections" in old):
                new_entry["value_corrections"] = deepcopy(
                    old["value_corrections"])
            months[key] = new_entry
            changed = True
    if changed:
        try:
            stage = tdir / ss.VOL_VALUE_RECONCILE_STAGE_DIR
            if stage.exists() or stage.is_symlink():
                raise ss.StorageError(
                    "volatility correction recovery is pending; manifest "
                    "save refused")
            ss.save_manifest(tdir, manifest)
        except (ss.StorageError, OSError) as exc:
            fp.notes.append(f"manifest for {canon} not saved ({exc}) — "
                            f"the next scan will rebuild it")


# --- quarantine / evidence --------------------------------------------------------

class _RunDirs:
    """Lazy creators for _quarantine/<run>/ and _ingest_reports/<run>/ —
    a clean run leaves no empty quarantine folder behind."""

    def __init__(self, root, run_id):
        self.root, self.run_id = Path(root), run_id
        self._q = self._r = None

    def quarantine(self):
        if self._q is None:
            self._q = self.root / QUARANTINE_DIR / self.run_id
            self._q.mkdir(parents=True, exist_ok=True)
        return self._q

    def reports(self):
        if self._r is None:
            self._r = self.root / REPORTS_DIR / self.run_id
            self._r.mkdir(parents=True, exist_ok=True)
        return self._r


def _quarantine_copy(dirs, fp):
    """COPY (never move) a refused file + its reason next to it. A failed
    copy degrades to a note — the source is still in place either way."""
    try:
        q = dirs.quarantine()
        dest = q / fp.path.name
        n = 1
        while dest.exists():
            dest = q / f"{n}-{fp.path.name}"
            n += 1
        shutil.copy2(fp.path, dest)
        dest.with_name(dest.name + ".reason.txt").write_text(
            f"{fp.path}\n{fp.reason}\n", encoding="utf-8")
        return str(dest)
    except OSError as exc:
        fp.notes.append(f"quarantine copy failed ({exc}) — source intact "
                        f"at {fp.path}")
        return None


def _write_rejected_rows(dirs, fp):
    """Forensic record of every refused row. csv.writer does the quoting
    (a hand-rolled f-string let a stray quote in a vendor cell corrupt
    the evidence) and same-basename sources get disambiguated names (the
    second used to clobber the first) — both adversarial-review proven."""
    if not fp.rejected:
        return None
    try:
        q = dirs.quarantine()
        dest = q / f"{fp.path.name}.rejected.csv"
        n = 1
        while dest.exists():
            dest = q / f"{n}-{fp.path.name}.rejected.csv"
            n += 1
        with open(dest, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["source", "source_line", "reason", "raw"])
            for ln, raw, reason in fp.rejected[:REJECTED_ROWS_FILE_CAP]:
                w.writerow([str(fp.path), ln, reason, raw])
            extra = fp.counters["invalid_rows"] - len(fp.rejected)
            if extra > 0:
                w.writerow([str(fp.path), 0,
                            f"... {extra:,} more not captured", ""])
        return str(dest)
    except OSError:
        return None


# --- the public entry --------------------------------------------------------------

def _weighted_size(f):
    """Source bytes weighted like the disk preflight (parquet expands)."""
    try:
        sz = f.stat().st_size
    except OSError:
        sz = 0
    return sz * (PREFLIGHT_PARQUET_EXPANSION
                 if f.suffix.lower() in (".parquet", ".pq") else 1)


def _sha256_file(path_str):
    """Streaming content hash of a source file; None when unreadable.
    Module-level so verify can pool it across workers."""
    import hashlib
    h = hashlib.sha256()
    try:
        with open(path_str, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(chunk)
    except OSError:
        return None
    return h.hexdigest()


def _parse_one(path, emit=None):
    """The PURE per-file stage (sniff -> read -> parse -> whole-file
    policies). No tree access, no writes — safe in a worker process.
    Progress texts are recorded in fp.msgs (without the [i/N] prefix)
    and forwarded live to `emit` when the caller runs serially."""
    fp = _FileParse(path)
    f = fp.path

    def _say(m):
        fp.msgs.append(m)
        if emit is not None:
            emit(m)

    fp.sha256 = _sha256_file(str(f))     # one sequential pre-read; the
    try:                                 # decode that follows hits the
        fp.size = f.stat().st_size       # OS cache
    except OSError:
        fp.size = 0
    _say(f"{f.name}: reading…")
    kind = _sniff(f)
    if kind == "empty":
        fp.status, fp.reason = "empty", "file is empty"
    elif kind == "zip":
        fp.status, fp.reason = (
            "quarantine", "ZIP container (an .xlsx workbook renamed "
                          ".csv?) — export real CSV and re-ingest")
    elif kind == "gzip":
        fp.status, fp.reason = (
            "quarantine", "gzip-compressed — decompress and re-ingest")
    elif kind.startswith("error:"):
        fp.status, fp.reason = "error", kind[6:]
    else:
        if kind == "parquet" and f.suffix.lower() not in (".parquet",
                                                          ".pq"):
            fp.notes.append("parquet magic bytes under a CSV name — "
                            "parsed as parquet")
        header = rows = None
        if kind == "parquet":
            header, rows = _read_parquet(fp)
        else:
            header, rows = _read_csv(fp)
        if header is not None:
            _say(f"{f.name}: parsing {len(rows):,} rows…")
            # filename ticker hint: only the TICKER_ shape counts —
            # a bare 'data.csv' must NOT become ticker DATA
            fallback = None
            if "_" in f.stem:
                try:
                    fallback = ss.canonical_ticker(
                        f.name.split("_")[0])
                except ss.StorageError:
                    fallback = None
            _parse_rows(fp, header, rows, fallback)
            del rows
            if fp.status == "ok":
                _apply_file_policies(fp)
    return fp


def _parse_file_payload(path_str):
    """Worker entry (spawn): module-level and import-clean — spawned
    parse workers import this module afresh, so it must stay free of
    import-time side effects."""
    return _parse_one(Path(path_str))


def _make_pool(workers):
    """Seam for the selftest (forced pool failure). Explicit spawn:
    identical semantics on Windows / macOS / Linux."""
    import multiprocessing as mp
    from concurrent.futures import ProcessPoolExecutor
    return ProcessPoolExecutor(max_workers=workers,
                               mp_context=mp.get_context("spawn"))


def _ingest_workers():
    """INGEST_WORKERS env (>=1) wins; the default leaves thermal
    headroom (same philosophy as VP_SWEEP_WORKERS)."""
    raw = os.environ.get(INGEST_WORKERS_ENV, "").strip()
    if raw.lstrip("+").isdigit() and int(raw) >= 1:
        return int(raw)
    return max(1, (os.cpu_count() or 2) - 2)


def _payloads_serial(files, start, say, cancel):
    """Today's loop head, verbatim semantics: cancel checked before
    each file, then a parse with live progress. This IS the fallback
    path. Yields (index, path, _FileParse|None); None = cancelled."""
    nf = len(files)
    for fi in range(start, nf):
        f = files[fi]
        if cancel is not None and cancel.is_set():
            yield fi, f, None
            continue
        yield fi, f, _parse_one(
            f, emit=lambda m, _fi=fi: say(f"[{_fi + 1}/{nf}] {m}"))


def _payloads_parallel(files, say, cancel, workers, notes):
    """Windowed submit in _order() order, release strictly in
    submission order — commits stay serial and deterministic. ANY pool
    problem degrades to _payloads_serial from the first unreleased
    file (parse is pure, so a re-parse can never double-commit)."""
    nf = len(files)
    n = min(workers, nf)
    try:
        ex = _make_pool(n)
    except Exception as exc:  # noqa: BLE001 — degrade, never abort
        notes.append(f"process pool unavailable ({type(exc).__name__}: "
                     f"{exc}) — single-core parse")
        say(notes[-1])
        yield from _payloads_serial(files, 0, say, cancel)
        return
    say(f"Parsing {nf} file(s) on {n} core(s)… "
        f"(set {INGEST_WORKERS_ENV}=1 for single-core)")
    pending = deque()                # (index, path, weighted, future)
    inflight = 0
    nxt = 0
    try:
        while pending or nxt < nf:
            while (nxt < nf
                   and not (cancel is not None and cancel.is_set())
                   and len(pending) < n * INGEST_INFLIGHT_PER_WORKER
                   and (not pending or inflight < INGEST_WINDOW_BYTES)):
                f = files[nxt]
                w = _weighted_size(f)
                pending.append((nxt, f, w,
                                ex.submit(_parse_file_payload, str(f))))
                inflight += w
                nxt += 1
            if not pending:          # cancelled before anything queued
                break
            fi, f, w, fut = pending.popleft()
            inflight -= w
            if cancel is not None and cancel.is_set():
                fut.cancel()
                yield fi, f, None
                continue
            try:
                fp = fut.result()
            except Exception as exc:  # noqa: BLE001 — worker/pool died
                notes.append(f"parallel parse failed at {f.name} "
                             f"({type(exc).__name__}: {exc}) — "
                             f"finishing on one core")
                say(notes[-1])
                for _i, _f, _w, p in pending:
                    p.cancel()
                pending.clear()
                yield from _payloads_serial(files, fi, say, cancel)
                return
            for m in fp.msgs:        # replay parse progress in order
                say(f"[{fi + 1}/{nf}] {m}")
            yield fi, f, fp
        for fi in range(nxt, nf):    # cancelled before submission
            yield fi, files[fi], None
    finally:
        try:
            ex.shutdown(wait=False, cancel_futures=True)
        except Exception:  # noqa: BLE001 — never mask the outcome
            pass


def _market_operation_path(root):
    """The same root-local operation fence used by bank-writing audits."""
    return (Path(root).resolve(strict=False).parent / "Run Logs"
            / ".market_data_operation.lock")


def _refused_ingest_report(root, exc, progress=None):
    """Return truthful zero-write evidence when the operation fence refuses."""
    run_id = _run_id()
    condition = ("market-data operation busy"
                 if isinstance(exc, operation_gate.OperationBusy)
                 else "market-data operation gate unavailable")
    message = f"{condition}; ingest refused ({exc})"
    report = {
        "run": run_id,
        "root": str(Path(root)),
        "files": [],
        "skipped": [],
        "totals": {},
        "cancelled": False,
        "started": datetime.now().isoformat(timespec="seconds"),
        "seconds": 0.0,
        "aborted": message,
        "report_path": None,
        "operation_gate": {
            "mode": "stock_ingest",
            "path": str(_market_operation_path(root)),
            "acquired": False,
        },
    }
    if progress is not None:
        try:
            progress(message)
        except Exception:  # noqa: BLE001 - reporting cannot alter refusal
            pass
    return report


def ingest_paths(paths, root, progress=None, cancel=None,
                 skip_contained=False):
    """Ingest under the project-wide market-data operation fence.

    The lease is acquired before source collection or any bank/report write
    and remains owned through final report publication.  A writing volatility
    audit/fetch therefore cannot observe an ingest between its month and
    queue scans, while an already-active operation refuses this run with a
    zero-write in-memory report.
    """
    lock_path = _market_operation_path(root)
    try:
        lease = operation_gate.acquire(
            "stock_ingest", owner="stock ingest", path=lock_path)
    except (operation_gate.OperationBusy,
            operation_gate.OperationGateError) as exc:
        return _refused_ingest_report(root, exc, progress=progress)
    with lease:
        return _ingest_paths_locked(
            paths, root, progress=progress, cancel=cancel,
            skip_contained=skip_contained)


def _ingest_paths_locked(paths, root, progress=None, cancel=None,
                         skip_contained=False):
    """Ingest `paths` (files and/or folders) into the storage tree at
    `root`. Returns the run report dict (also saved to
    _ingest_reports/<run>/report.json). Never raises for per-file
    problems; never touches source files; safe to cancel (committed
    months stay, a re-run is idempotent). Large batches PARSE on a
    spawn process pool (INGEST_WORKERS env; 1 = serial); commits always
    run here, in contractual file order — the tree is byte-identical
    to a single-core run. With skip_contained=True (the GUI's folder
    flow), files the LEDGER proves fully contained — clean: no
    conflicts, nothing refused — are skipped before parsing; everything
    else takes the full strict path."""
    t0 = _time.time()
    root = Path(root)
    run_id = _run_id()
    dirs = _RunDirs(root, run_id)

    def say(msg):
        if progress is not None:
            try:
                progress(msg)
            except Exception:  # noqa: BLE001 — a UI callback never aborts
                pass

    report = {"run": run_id, "root": str(root), "files": [],
              "skipped": [], "totals": Counter(), "cancelled": False,
              "started": datetime.now().isoformat(timespec="seconds")}

    if root.exists() and not root.is_dir():
        report["aborted"] = (f"{root} exists and is NOT a directory — "
                             f"remove/rename it first")
        return _finalize(report, dirs, t0, say)

    say("Collecting source files…")
    files, skipped = _collect_sources(paths, root)
    report["skipped"] = [{"path": p, "reason": r} for p, r in skipped]
    if not files:
        report["aborted"] = "no ingestable files found"
        return _finalize(report, dirs, t0, say)

    # disk preflight: output ≈ input for CSV, ~8x for parquet, then margin
    weighted = sum(_weighted_size(f) for f in files)
    need = int(weighted * PREFLIGHT_MARGIN) + PREFLIGHT_FLOOR
    anchor = root if root.exists() else root.parent
    try:
        free = shutil.disk_usage(anchor).free
    except OSError:
        free = None
    if free is not None and free < need:
        report["aborted"] = (f"disk preflight: ~{need / 1e9:.1f} GB needed, "
                             f"{free / 1e9:.1f} GB free — nothing written")
        return _finalize(report, dirs, t0, say)
    report["preflight"] = {"estimated_bytes": need, "free_bytes": free}

    conflict_records = []
    conflict_counts = Counter()

    def conflict_sink(ticker, interval, month, existing, incoming):
        key = (ticker, interval, month)
        conflict_counts[key] += 1
        if conflict_counts[key] <= CONFLICT_LOG_CAP_PER_MONTH:
            conflict_records.append((key, existing, incoming))

    workers = _ingest_workers()
    if skip_contained:
        ledger = _ledger_load(root)
        if ledger:
            say("Ledger check: which files are already contained?…")
            shas = _hash_files(files, cancel, workers)
            manifests = {}
            kept = []
            for fi, f in enumerate(files):
                verdict = None
                for led in reversed(ledger.get(shas[fi], [])
                                    if shas[fi] else []):
                    v = _ledger_verdict(root, led, manifests)
                    if v is not None:
                        verdict = v[0]
                        break
                if verdict == "contained":
                    report["files"].append(
                        {"path": str(f), "sha256": shas[fi],
                         "status": "skipped (already contained — "
                                   "ledger)"})
                    report["totals"]["skipped_contained"] += 1
                else:
                    kept.append(f)
            if len(kept) < len(files):
                say(f"{len(files) - len(kept)} of {len(files)} file(s) "
                    f"already fully contained — skipped (ledger)")
                files = kept
                weighted = sum(_weighted_size(f) for f in files)
    mp_notes = []
    use_mp = (workers > 1 and len(files) >= INGEST_MP_MIN_FILES
              and weighted >= INGEST_MP_MIN_BYTES)
    stream = (_payloads_parallel(files, say, cancel, workers, mp_notes)
              if use_mp else _payloads_serial(files, 0, say, cancel))
    for fi, f, fp in stream:
        if fp is None:
            report["cancelled"] = True
            report["files"].append({"path": str(f),
                                    "status": "not started (cancelled)"})
            continue
        _record_file(report, dirs, fp, run_id, say, fi, len(files),
                     root, cancel, conflict_sink)
    if use_mp:
        report["parallel"] = {"workers": min(workers, len(files)),
                              "notes": mp_notes}

    _write_conflicts(report, dirs, conflict_records)
    return _finalize(report, dirs, t0, say)


def _apply_file_policies(fp):
    """The whole-file C-bucket thresholds, applied after a full parse."""
    c = fp.counters
    timestamped = c["parsed_ok"] + c["non_rth"]
    data_rows = timestamped + c["invalid_rows"]
    if data_rows == 0:
        fp.status, fp.reason = "empty", "no data rows"
        return
    if c["non_rth"] and timestamped \
            and c["non_rth"] / timestamped > NON_RTH_QUARANTINE_FRAC:
        fp.status = "quarantine"
        fp.reason = (f"{c['non_rth']:,} of {timestamped:,} rows "
                     f"({c['non_rth'] / timestamped:.0%}) are outside "
                     f"regular trading hours — this looks like an "
                     f"extended-hours or non-equity file (first examples: "
                     f"{'; '.join(fp.non_rth_examples[:3])})")
        return
    if c["invalid_rows"] / data_rows > INVALID_QUARANTINE_FRAC:
        examples = "; ".join(r for _l, _r, r in fp.rejected[:3])
        fp.status = "quarantine"
        fp.reason = (f"{c['invalid_rows']:,} of {data_rows:,} rows "
                     f"({c['invalid_rows'] / data_rows:.0%}) are invalid "
                     f"— raw data looks damaged ({examples})")
        return
    if not fp.groups:
        fp.status = "needs-decision"
        fp.reason = ("no ticker could be determined — add a symbol column "
                     "or rename the file to start with TICKER_")
        return
    if None in fp.groups:
        n = len(fp.groups.pop(None, []))
        fp.notes.append(f"{n:,} row(s) had no symbol column and the "
                        f"filename gave no usable ticker — not ingested")
        if not fp.groups:
            fp.status = "needs-decision"
            fp.reason = ("ticker unknown — add a symbol column or rename "
                         "the file to start with TICKER_")
            return
    _finish_groups(fp)
    # filename-vs-symbol-column cross-check (symbol column is in-band and
    # wins; a disagreement is loud but not blocking)
    hint = None
    if "_" in fp.path.stem:
        try:
            hint = ss.canonical_ticker(fp.path.name.split("_")[0])
        except ss.StorageError:
            pass
    if (hint and fp.raw_symbols and len(fp.groups) == 1
            and hint not in fp.groups):
        only = next(iter(fp.groups))
        fp.notes.append(f"filename suggests {hint} but the symbol column "
                        f"says {only} — the symbol column wins")


def _record_file(report, dirs, fp, run_id, say, fi, nf, root, cancel,
                 conflict_sink):
    """Merge an ok file into the tree, quarantine a refused one, and fold
    everything into the report."""
    rec = {"path": str(fp.path), "status": fp.status, "notes": fp.notes,
           "counters": dict(fp.counters)}
    if fp.status == "ok":
        results = []
        for canon in sorted(fp.groups):
            meta = fp.group_meta[canon]
            if meta["interval"] is None:
                results.append({"ticker": canon, "interval": None,
                                "rejected": meta["interval_note"]})
                continue
            say(f"[{fi + 1}/{nf}] {fp.path.name}: merging {canon} "
                f"{meta['interval']} ({meta['rows']:,} bars)…")
            try:
                def _merge_and_publish():
                    merged = _merge_series(
                        root, fp, canon, meta["interval"],
                        fp.groups[canon], run_id, conflict_sink, cancel)
                    fp.merge[canon] = merged
                    if (merged.get("gate") is None and merged["written"]
                            and not merged.get("recovery_pending")):
                        _update_manifest(root, fp, canon, run_id)
                    return merged

                tdir = Path(root) / canon
                stage = tdir / ss.VOL_VALUE_RECONCILE_STAGE_DIR
                with ss.ticker_transaction(tdir):
                    if stage.exists() or stage.is_symlink():
                        raise ss.StorageError(
                            "volatility correction recovery is pending; "
                            "ticker ingest refused")
                    # The prewrite read, month replace, correction-ledger
                    # decision and manifest publication are one ticker
                    # transaction.  Even a price import rewrites the whole
                    # manifest, so every kind must stay outside a pending
                    # volatility correction/scanner publication.
                    res = _merge_and_publish()
            except (ss.StorageError, OSError) as exc:
                # one bad series must never abort the run ("never raises
                # for per-file problems" — adversarial review, proven)
                results.append({"ticker": canon,
                                "interval": meta["interval"],
                                "rejected": f"series failed: {exc}"})
                continue
            res["interval_note"] = meta["interval_note"]
            results.append(res)
            if res.get("cancelled"):
                report["cancelled"] = True
        rec["series"] = results
        for r in results:
            for k in ("added", "written", "skipped_identical"):
                report["totals"][k] += r.get(k, 0)
            if r.get("gate"):
                # a gated series' overlap counts are DRY-RUN numbers —
                # mixing them into the committed-merge totals made the
                # head line claim conflicts were "kept" when the whole
                # file was withheld
                report["totals"]["gated_series"] += 1
                report["totals"]["gated_overlap"] += (
                    r.get("conflicts", 0) + r.get("dup_existing", 0))
            else:
                for k in ("dup_existing", "conflicts"):
                    report["totals"][k] += r.get(k, 0)
            if r.get("rejected"):
                report["totals"]["rejected_series"] += 1
    elif fp.status == "quarantine":
        rec["reason"] = fp.reason
        rec["quarantined_to"] = _quarantine_copy(dirs, fp)
        report["totals"]["quarantined_files"] += 1
    else:
        rec["reason"] = fp.reason
    if fp.rejected:
        rec["rejected_rows_file"] = _write_rejected_rows(dirs, fp)
    for k, v in fp.counters.items():
        report["totals"][k] += v
    if fp.sha256:
        led = {"sha256": fp.sha256, "size": fp.size,
               "name": fp.path.name, "run": run_id, "kind": "ingest",
               "status": fp.status,
               "rejected_rows": fp.counters.get("invalid_rows", 0),
               "non_rth": fp.counters.get("non_rth", 0)}
        if fp.status == "ok":
            led["series"] = _ledger_series(rec.get("series", []), root)
        else:
            led["reason"] = fp.reason
        _ledger_append(root, led)
    report["files"].append(rec)


def _write_conflicts(report, dirs, records):
    if not records:
        return
    try:
        dest = dirs.reports() / "conflicts.jsonl"
        with open(dest, "w", encoding="utf-8") as fh:
            for (ticker, interval, month), e, b in records:
                fh.write(json.dumps(
                    {"ticker": ticker, "interval": interval, "month": month,
                     "ts": f"{ss.format_date(e[0])} "
                           f"{ss.format_time(e[0].time())}",
                     "existing": list(e[1:]), "incoming": list(b[1:])})
                    + "\n")
        report["conflict_log"] = str(dest)
        report["conflict_log_note"] = (
            f"{len(records):,} example(s) captured (capped at "
            f"{CONFLICT_LOG_CAP_PER_MONTH}/month — exact counts are in "
            f"each file's series results)")
    except OSError as exc:
        report["conflict_log_note"] = f"conflict log not written: {exc}"


def _finalize(report, dirs, t0, say):
    report["totals"] = dict(report["totals"])
    report["seconds"] = round(_time.time() - t0, 1)
    try:
        payload = json.dumps(report, indent=1, sort_keys=True,
                             default=str).encode("utf-8")
        dest = dirs.reports() / "report.json"
        ss._atomic_write_bytes(dest, payload)
        report["report_path"] = str(dest)
    except (OSError, ss.StorageError, TypeError) as exc:
        report["report_path"] = None
        report.setdefault("notes", []).append(f"report not saved: {exc}")
    say("Ingest finished.")
    return report


# --- UI-facing summary --------------------------------------------------------------

def summarize_report(report):
    """The report as terse human lines for the issues pane (GUI-free so
    the selftest can pin it)."""
    out = []
    if report.get("aborted"):
        out.append(f"INGEST ABORTED: {report['aborted']}")
    t = report.get("totals", {})
    head = (f"INGEST {report['run']}: +{t.get('added', 0):,} rows in "
            f"{t.get('written', 0):,} month file(s), "
            f"{t.get('dup_existing', 0):,} already present, "
            f"{t.get('conflicts', 0):,} conflict(s) (existing kept), "
            f"{t.get('non_rth', 0):,} non-RTH dropped, "
            f"{t.get('invalid_rows', 0):,} invalid row(s)"
            + (f", {t['skipped_contained']:,} file(s) skipped (already "
               f"contained)" if t.get("skipped_contained") else "")
            + (f", {t['gated_series']} series GATED "
               f"({t.get('gated_overlap', 0):,} overlapping rows "
               f"withheld)" if t.get("gated_series") else "")
            + (", CANCELLED" if report.get("cancelled") else "")
            + f" — {report.get('seconds', 0)}s")
    out.append(head)
    for rec in report.get("files", []):
        name = Path(rec["path"]).name
        st = rec["status"]
        if st == "ok":
            for r in rec.get("series", []):
                if r.get("rejected"):
                    out.append(f"  REJECTED  {name} -> {r['ticker']}: "
                               f"{r['rejected']}")
                elif r.get("gate"):
                    out.append(f"  GATED     {name} -> {r['ticker']} "
                               f"{r['interval']}: {r['gate']}")
                else:
                    bits = [f"+{r['added']:,} rows",
                            f"{r['written']} month(s) written"]
                    if r["skipped_identical"]:
                        bits.append(f"{r['skipped_identical']} unchanged")
                    if r["conflicts"]:
                        bits.append(f"{r['conflicts']:,} conflict(s), "
                                    f"existing kept")
                    if r["blocked_months"]:
                        bits.append(f"{len(r['blocked_months'])} month(s) "
                                    f"BLOCKED (unreadable existing file)")
                    note = r.get("interval_note", "")
                    flag = "  !! " if "FILENAME SAYS" in note else ""
                    out.append(f"  ok        {name} -> {r['ticker']} "
                               f"{r['interval']}: " + ", ".join(bits)
                               + (f"{flag}[{note}]" if note else ""))
                    for sn in r.get("notes", []):   # basis-factor unlock
                        out.append(f"  !!        {name} -> {r['ticker']} "
                                   f"{r['interval']}: {sn}")
        else:
            out.append(f"  {st.upper():9} {name}: "
                       f"{rec.get('reason', '')}")
        for n in rec.get("notes", []):
            out.append(f"            {name}: {n}")
    for s in report.get("skipped", []):
        out.append(f"  skipped   {Path(s['path']).name}: {s['reason']}")
    if report.get("conflict_log"):
        out.append(f"  conflict examples: {report['conflict_log']}")
    if report.get("report_path"):
        out.append(f"  full report: {report['report_path']}")
    return out


# --- the source-file ledger + containment verify ------------------------------------

def _ledger_path(root):
    return Path(root) / REPORTS_DIR / LEDGER_NAME


def _ledger_append(root, record):
    """Append-only; the ledger is an ACCELERATOR, never load-bearing —
    a failed write is silently dropped and verify just parses more."""
    try:
        p = _ledger_path(root)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True) + "\n")
    except OSError:
        pass


def _ledger_load(root):
    """sha256 -> [records, oldest first]. Torn/garbage lines are skipped
    (a crash mid-append costs one fast-path hit, nothing else)."""
    out = {}
    try:
        with open(_ledger_path(root), encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                if isinstance(rec, dict) and rec.get("sha256"):
                    out.setdefault(rec["sha256"], []).append(rec)
    except OSError:
        pass
    return out


def _ledger_series(results, root):
    """Ledger view of a file's merge results: per-series outcome +
    months pinned to the stored months' hashes, enough for verify to
    cross-check the manifests later. The sha pin matters for months
    the file did NOT rewrite (all-dup / all-conflict merges leave no
    provenance contribution to check)."""
    out = []
    manifests = {}
    for r in results:
        if r.get("rejected"):
            o = "rejected"
        elif r.get("gate"):
            o = "gated"
        elif r.get("cancelled"):
            o = "cancelled"
        else:
            months = r.get("months") or {}
            done = [k for k, v in months.items()
                    if v.get("status") in ("written", "unchanged")]
            o = "merged" if months and len(done) == len(months) \
                else "partial"
        month_shas = {}
        if o == "merged":
            for mk, v in (r.get("months") or {}).items():
                sha = (v.get("stats") or {}).get("sha256")
                if sha is None:                # unchanged month: ask the
                    tic = r.get("ticker")      # (already updated) manifest
                    man = manifests.get(tic)
                    if man is None:
                        man = ss.load_manifest(Path(root) / tic) or {}
                        manifests[tic] = man
                    sha = (man.get("intervals", {})
                           .get(r.get("interval"), {}).get("months", {})
                           .get(mk, {})).get("sha256")
                month_shas[mk] = sha
        out.append({"ticker": r.get("ticker"),
                    "interval": r.get("interval"), "outcome": o,
                    "conflicts": r.get("conflicts", 0),
                    "months": sorted(r.get("months") or {}),
                    "month_shas": month_shas})
    return out


def _ledger_verdict(root, led, manifests):
    """(verdict, detail) when this ledger record can vouch for the
    file; None -> the record cannot vouch (parse instead). Sound
    because merges are append-only: once a contribution is recorded,
    later merges never remove or change those rows."""
    if led.get("status") != "ok":
        return ("not-contained",
                f"recorded outcome: {led.get('status')}"
                + (f" — {led['reason']}" if led.get("reason") else ""))
    series = led.get("series") or []
    if not series:
        return None
    kind = led.get("kind", "ingest")
    conf = led.get("conflicts_seen", 0) if kind == "verify" else 0
    for s in series:
        if kind == "ingest":
            if s.get("outcome") == "gated":
                return ("not-contained",
                        f"recorded outcome: {s.get('ticker')} "
                        f"{s.get('interval')} was basis-GATED — nothing "
                        f"from this file was written")
            if s.get("outcome") != "merged":
                return None
            conf += s.get("conflicts", 0)
            mks = s.get("months") or []
        else:
            mks = sorted(s.get("month_shas") or {})
        if s.get("interval") is None or not s.get("ticker"):
            return None
        man = manifests.get(s["ticker"])
        if man is None:
            man = ss.load_manifest(Path(root) / s["ticker"]) or {}
            manifests[s["ticker"]] = man
        months = (man.get("intervals", {}).get(s["interval"], {})
                  .get("months", {}))
        for mk in mks:
            info = months.get(mk)
            if not info:
                return None
            if kind == "ingest":
                contribs = ((info.get("source") or {})
                            .get("contributions") or [])
                pinned = (s.get("month_shas") or {}).get(mk)
                if not (any(c.get("run") == led.get("run")
                            and c.get("file") == led.get("name")
                            for c in contribs)
                        or (pinned is not None
                            and info.get("sha256") == pinned)):
                    return None        # neither provenance nor sha vouch
            elif info.get("sha256") != s["month_shas"].get(mk):
                return None                  # month rewritten/grown since
            y, m = mk.split("-")
            if ss.find_month_file(root, s["ticker"], int(y),
                                  int(m), s["interval"]) is None:
                return None
    refused = led.get("rejected_rows", 0) + led.get("non_rth", 0)
    if conf or refused:
        return ("contained-except",
                f"{conf:,} row(s) differ from the archive (archive kept "
                f"its values), {refused:,} refused by policy")
    return ("contained", "")


def _hash_files(files, cancel, workers):
    """sha256 per file, pooled for big batches; serial fallback."""
    if (workers > 1 and len(files) >= INGEST_MP_MIN_FILES
            and sum(_weighted_size(f) for f in files)
            >= INGEST_MP_MIN_BYTES):
        ex = None
        try:
            ex = _make_pool(min(workers, len(files)))
            return list(ex.map(_sha256_file, [str(f) for f in files]))
        except Exception:  # noqa: BLE001 — degrade to serial hashing
            pass
        finally:
            if ex is not None:
                try:
                    ex.shutdown(wait=False, cancel_futures=True)
                except Exception:  # noqa: BLE001
                    pass
    out = []
    for f in files:
        if cancel is not None and cancel.is_set():
            out.append(None)
            continue
        out.append(_sha256_file(str(f)))
    return out


def _verify_compare(fp, root, sha, manifests, month_cache):
    """The strict path's second half: bar-for-bar comparison of a
    parsed payload against the stored months (read-only on the tree).
    Confirmed files are remembered in the ledger (kind=verify, pinned
    to the stored months' hashes) so the next verify skips the parse."""
    if fp.status != "ok":
        return ("not-contained", f"{fp.status}: {fp.reason}")
    missing = differing = 0
    series = []
    for canon in sorted(fp.groups):
        meta = fp.group_meta[canon]
        if meta["interval"] is None:
            return ("not-contained", meta["interval_note"])
        iv = meta["interval"]
        by_month = {}
        for b in fp.groups[canon]:
            by_month.setdefault((b[0].year, b[0].month), []).append(b)
        month_shas = {}
        for (y, m), bars in sorted(by_month.items()):
            ck = (canon, iv, y, m)
            if ck not in month_cache:
                try:
                    stored, _meta = ss.read_month_file(
                        ss.find_month_file(root, canon, y, m, iv)
                        or ss.month_file_path(root, canon, y, m, iv))
                except (ss.StorageError, OSError):
                    stored = []
                month_cache[ck] = {sb[0]: sb for sb in stored}
                while len(month_cache) > VERIFY_MONTH_CACHE_MAX:
                    month_cache.pop(next(iter(month_cache)))
            emap = month_cache[ck]
            for b in bars:
                e = emap.get(b[0])
                if e is None:
                    missing += 1
                elif tuple(e) != tuple(b):
                    differing += 1
            man = manifests.get(canon)
            if man is None:
                man = ss.load_manifest(Path(root) / canon) or {}
                manifests[canon] = man
            mk = ss.month_key(y, m)
            info = (man.get("intervals", {}).get(iv, {})
                    .get("months", {}).get(mk, {}))
            month_shas[mk] = info.get("sha256")
        series.append({"ticker": canon, "interval": iv,
                       "month_shas": month_shas})
    refused = (fp.counters.get("invalid_rows", 0)
               + fp.counters.get("non_rth", 0))
    if missing:
        return ("not-contained",
                f"{missing:,} row(s) missing from the archive"
                + (f", {differing:,} differing" if differing else "")
                + (f", {refused:,} refused by policy" if refused else ""))
    if sha:
        _ledger_append(root, {
            "sha256": sha, "size": fp.size, "name": fp.path.name,
            "run": "", "kind": "verify", "status": "ok",
            "rejected_rows": fp.counters.get("invalid_rows", 0),
            "non_rth": fp.counters.get("non_rth", 0),
            "conflicts_seen": differing, "series": series})
    if differing or refused:
        return ("contained-except",
                f"{differing:,} row(s) differ from the archive (archive "
                f"kept its values), {refused:,} refused by policy")
    return ("contained", "")


def _finalize_verify(report, dirs, t0, say):
    report["totals"] = dict(report["totals"])
    report["seconds"] = round(_time.time() - t0, 1)
    try:
        payload = json.dumps(report, indent=1, sort_keys=True,
                             default=str).encode("utf-8")
        dest = dirs.reports() / "report.json"
        ss._atomic_write_bytes(dest, payload)
        report["report_path"] = str(dest)
    except (OSError, ss.StorageError, TypeError) as exc:
        report["report_path"] = None
        report.setdefault("notes", []).append(f"report not saved: {exc}")
    say("Verify finished.")
    return report


def verify_paths(paths, root, progress=None, cancel=None):
    """Answer 'is everything under `paths` already CONTAINED in the
    tree?' WITHOUT re-ingesting. Fast path: the source-file ledger
    (sha256-keyed outcomes of past runs) cross-checked against the
    manifests and the month files on disk — no parsing. Files the
    ledger cannot vouch for get the full parse-and-compare. The tree
    is never written; confirmations are appended to the ledger so the
    NEXT verify is fast. Returns a report dict saved under
    _ingest_reports/<run>/ like an ingest run."""
    t0 = _time.time()
    root = Path(root)
    run_id = "ver" + _run_id()[3:]
    dirs = _RunDirs(root, run_id)

    def say(msg):
        if progress is not None:
            try:
                progress(msg)
            except Exception:  # noqa: BLE001 — a UI callback never aborts
                pass

    report = {"run": run_id, "root": str(root), "kind": "verify",
              "files": [], "skipped": [], "totals": Counter(),
              "cancelled": False,
              "started": datetime.now().isoformat(timespec="seconds")}
    say("Collecting source files…")
    files, skipped = _collect_sources(paths, root)
    report["skipped"] = [{"path": p, "reason": r} for p, r in skipped]
    if not files:
        report["aborted"] = "no ingestable files found"
        return _finalize_verify(report, dirs, t0, say)
    say(f"Hashing {len(files)} file(s)…")
    shas = _hash_files(files, cancel, _ingest_workers())
    ledger = _ledger_load(root)
    manifests = {}
    month_cache = {}
    nf = len(files)
    recs = {}
    todo = []                  # (original index, path, sha): parse needed
    for fi, f in enumerate(files):
        if cancel is not None and cancel.is_set():
            report["cancelled"] = True
            recs[fi] = {"path": str(f),
                        "verdict": "not checked (cancelled)"}
            continue
        sha = shas[fi]
        verdict = detail = None
        for led in reversed(ledger.get(sha, []) if sha else []):
            v = _ledger_verdict(root, led, manifests)
            if v is not None:
                verdict, detail = v
                break
        if verdict is None:
            todo.append((fi, f, sha))
            continue
        rec = {"path": str(f), "verdict": verdict, "via": "ledger"}
        if detail:
            rec["detail"] = detail
        recs[fi] = rec
    if todo:
        # the parse fallback rides the same pool as the ingest; the
        # comparisons stay here (read-only, cache-friendly)
        say(f"{len(todo)} of {nf} file(s) need a full parse…")
        tfiles = [f for _i, f, _s in todo]
        workers = _ingest_workers()
        mp_notes = []
        big = (workers > 1 and len(tfiles) >= INGEST_MP_MIN_FILES
               and sum(_weighted_size(f) for f in tfiles)
               >= INGEST_MP_MIN_BYTES)
        stream = (_payloads_parallel(tfiles, say, cancel, workers,
                                     mp_notes) if big
                  else _payloads_serial(tfiles, 0, say, cancel))
        for ti, _f, fp in stream:
            fi, f, sha = todo[ti]
            if fp is None:
                report["cancelled"] = True
                recs[fi] = {"path": str(f),
                            "verdict": "not checked (cancelled)"}
                continue
            verdict, detail = _verify_compare(fp, root, sha, manifests,
                                              month_cache)
            rec = {"path": str(f), "verdict": verdict, "via": "parse"}
            if detail:
                rec["detail"] = detail
            recs[fi] = rec
        if mp_notes:
            report.setdefault("notes", []).extend(mp_notes)
    for fi in range(nf):
        rec = recs[fi]
        report["files"].append(rec)
        report["totals"][rec["verdict"]] += 1
        if "via" in rec:
            report["totals"]["via_" + rec["via"]] += 1
    return _finalize_verify(report, dirs, t0, say)


def summarize_verify(report):
    """The verify report as terse human lines (GUI-free, testable)."""
    out = []
    if report.get("aborted"):
        out.append(f"VERIFY ABORTED: {report['aborted']}")
        return out
    t = report.get("totals", {})
    nf = len(report.get("files", []))
    out.append(f"VERIFY {report['run']}: {t.get('contained', 0):,} of "
               f"{nf:,} file(s) fully contained, "
               f"{t.get('contained-except', 0):,} with exceptions, "
               f"{t.get('not-contained', 0):,} NOT contained "
               f"({t.get('via_ledger', 0):,} via ledger, "
               f"{t.get('via_parse', 0):,} parsed) — "
               f"{report.get('seconds', 0)}s")
    for r in report.get("files", []):
        if r.get("verdict") != "contained":
            out.append(f"  {r.get('verdict', '?').upper():17} "
                       f"{Path(r['path']).name}"
                       + (f": {r['detail']}" if r.get("detail") else ""))
    if report.get("cancelled"):
        out.append("  CANCELLED — partial verification")
    return out
