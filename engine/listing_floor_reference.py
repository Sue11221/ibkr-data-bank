"""Row 47 M3 acceptance harness — identity listing-floor enforcement.

User 2026-07-21, verbatim: "do the trunction and make sure it never happens
again."  M1 truncated TKO (`2023-09-12`) and WBD (`2022-04-11`) vol history to
their listing floors; M2 rebuilt TKO price under a HAND-WRITTEN `since` bound.
M3 removes the need for that hand bound: no fetch path may backfill an empty
series past a cutover the manifest already records.

The hole, verified in code and reproduced by this harness: an empty series
derives its start from a head probe or the earliest-on-IBKR cache and then
clamps only against `since`, the 1-second window, and the extended-hours base
cap (`stock_ibkr.py` ~3583-3678 per-series, ~4620-4680 combined prepass).
`data_corrections` is never consulted — its only readers are audit/verify
surfaces.  `vol_value_bank.identity_floor()` already parses the records with
hardened semantics, but `_kind_token` restricts it to exact RTH IV/HVOL tokens
and its only callers are the Row 51 refetch-settle path, so price series and
every gap_fill start derivation remain unprotected.

SCOPE FINDING (this harness, 2026-07-24) — M3 as specified is TOO NARROW.
The plan says "empty-series listing-floor enforcement", but check [7] proves a
THIRD route: the BACKWARD EXTENSION path (`stock_ibkr.py` ~3427-3463) takes a
NON-empty series and backfills `trading_days(bw, first_stored)` with no
corrections consult.  A series sitting exactly on its cutover — which is
precisely what M1 truncation and the M2 rebuild produced for TKO and WBD — is
extended straight back through the floor by any routine update carrying a
deeper `since`.  `stock_validate.FULL_HISTORY_RANGE = "Max"` makes that the
normal deep-run shape, so this is reachable today and would silently undo M1.
M3 must clamp all THREE sites, not two.

OFFLINE ONLY: temp roots, fake adapters, redirected operation gate. No GUI, no
ports, no network, no bank reads or writes. Safe to run while a live fetch runs.

Exit codes:
  0 = M3 feature present (`stock_ibkr.IDENTITY_FLOOR_CLAMP is True`) AND every
      feature check passes.
  3 = feature absent — the baseline proved the CURRENT engine's defects plus
      the invariants that must survive M3. Expected pre-M3.
  1 = harness failure.

Contract pinned here:
  - B1/F1 (kind, the TKO/WBD hole): an EMPTY `1d-hvol` on a ticker whose
    manifest carries a listing-wide `identity_listing_truncation` must not
    request a single session before the cutover.
  - B2/F2 (price): the same clamp on an empty `1m`, WITHOUT a hand-written
    `since` — the bound M2 had to supply by hand becomes structural. Stored
    first bar is asserted too, not just the request plan.
  - B3/F3 (second kind token): `1m-iv` behaves identically.
  - F4a/F4b (precision): an interval-SCOPED correction clamps its own base
    family and MUST NOT clamp an unrelated one. Over-clamping would silently
    shorten healthy history — as damaging as under-clamping.
  - F5 (listing-wide reach): one record with no `intervals` protects price and
    both kind families at once.
  - I1 (control): a ticker with no corrections is untouched in BOTH stages.
  - I2 (never extends): a cutover OLDER than the served head must not drag the
    fetch further back. The floor raises a start; it never lowers one.
  - I3 (`since` still wins): final start = max(derived, since, floor).
  - B4/F6 (fail closed): a MALFORMED correction must not silently evaporate
    into "no floor". Post-M3 the series must halt or clamp, observably —
    either is acceptable, silence is not.
  - B5/F7 (backward extension): a NON-empty series already sitting on its
    cutover must not be extended back through it by a deep-`since` update.
    This is the scope finding above — the post-M1/M2 shape of TKO and WBD.
  - I5 (Row 50 composition): `KIND_EARLIEST_PROBE` stays True and kind series
    still issue their own head probe — final start = max(kind_earliest,
    identity_floor, since).
  - I6 (write purity): a kind run leaves `_ibkr_earliest.json` byte-identical.

Also reported, deliberately OUTSIDE the exit gate: the quarantine-blocklist
question the M3 spec left to be agreed at review (see the SPEC QUESTION block
at the end). It prints evidence and never changes the exit code.

Claude-owned: Codex MUST NOT weaken these assertions; harness changes require
Claude sign-off on the board.
"""
import atexit
import hashlib
import re
import sys
import tempfile
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
import operation_gate as og                                    # noqa: E402

_GATE_OLD = og.LOCK_PATH
_GATE_DIR = tempfile.TemporaryDirectory(prefix="listing_floor_gate_",
                                        ignore_cleanup_errors=True)
og.LOCK_PATH = Path(_GATE_DIR.name) / ".market_data_operation.lock"


def _restore_gate():
    og.LOCK_PATH = _GATE_OLD
    _GATE_DIR.cleanup()


atexit.register(_restore_gate)

import stock_ibkr as sk                                        # noqa: E402
import stock_storage as ss                                     # noqa: E402
import stock_validate as sv                                    # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:  # noqa: BLE001
    pass

# --- test seam -------------------------------------------------------------
# A head probe LEGITIMATELY reads sessions before the floor: that is how the
# engine discovers what the source serves. The contract is about what gets
# BACKFILLED, not what reconnaissance looked at. Wrap the engine's two head
# entry points (depth-counted — `_head_start` delegates to
# `_head_start_evidence`) so the adapter can classify each read. Read-only
# instrumentation: no engine semantics change.
_PROBE = threading.local()


def _in_probe():
    return getattr(_PROBE, "depth", 0) > 0


def _wrap_probe(fn):
    def wrapped(*args, **kwargs):
        _PROBE.depth = getattr(_PROBE, "depth", 0) + 1
        try:
            return fn(*args, **kwargs)
        finally:
            _PROBE.depth -= 1
    return wrapped


sk._head_start_evidence = _wrap_probe(sk._head_start_evidence)
sk._head_start = _wrap_probe(sk._head_start)

NY = ZoneInfo("America/New_York")
TODAY = date(2024, 6, 19)          # Wednesday
HEAD = date(2024, 6, 3)            # what the source would serve from
FLOOR = date(2024, 6, 12)          # the recorded listing cutover
OLD_FLOOR = date(2024, 5, 15)      # a cutover OLDER than the served head
SINCE = date(2024, 5, 1)           # request bound, deliberately older than FLOOR
LATE_SINCE = date(2024, 6, 17)     # request bound LATER than the floor
IV_SHOW = "OPTION_IMPLIED_VOLATILITY"
FAILS = []
N = [0]
START_RE = re.compile(r"full backfill from (\d{4}-\d{2}-\d{2})")


def check(name, ok, detail=""):
    N[0] += 1
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + ("" if ok else f"  {detail}"))
    if not ok:
        FAILS.append(name)


def _bar(stamp, price, span):
    return SimpleNamespace(
        date=stamp, open=price, high=price + span, low=price - span,
        close=price + span / 2, volume=float(1000))


def stub_minute_bars(day, ratio=False, n=3):
    """RTH minute bars. Ratio kinds must stay well under Row 51's ceiling."""
    base, span = (0.25, 0.002) if ratio else (10.0, 0.05)
    out, t = [], datetime.combine(day, ss.RTH_FIRST)
    for i in range(n):
        out.append(_bar(t.replace(tzinfo=NY).astimezone(timezone.utc),
                        base + i * span, span))
        t += timedelta(minutes=1)
    return out


def stub_daily_bar(day, ratio=False):
    """One canonical midnight-stamped bar — what a `1 day` request returns."""
    base, span = (0.25, 0.002) if ratio else (10.0, 0.05)
    return [_bar(datetime.combine(day, datetime.min.time()), base, span)]


SESSIONS = [d for d in (HEAD + timedelta(days=i)
                        for i in range((TODAY - HEAD).days + 1))
            if d.weekday() < 5]
DAYS = {
    ("min", False): {d: stub_minute_bars(d) for d in SESSIONS},
    ("min", True): {d: stub_minute_bars(d, ratio=True) for d in SESSIONS},
    ("day", False): {d: stub_daily_bar(d) for d in SESSIONS},
    ("day", True): {d: stub_daily_bar(d, ratio=True) for d in SESSIONS},
}


class Ledger:
    """Thread-safe record of the sessions the source was asked for.

    `backfill` days are the contract evidence. `probe` days are head/daily
    reconnaissance, which may legitimately look below a floor.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._backfill = set()
        self._probe = set()
        self._heads = []

    def note_day(self, day, probe):
        with self._lock:
            (self._probe if probe else self._backfill).add(day)

    def note_head(self, what):
        with self._lock:
            self._heads.append(str(what))

    def days(self):
        with self._lock:
            return sorted(self._backfill)

    def probe_days(self):
        with self._lock:
            return sorted(self._probe)

    def pre_floor(self, floor):
        """Backfilled sessions strictly before `floor` — the violation set."""
        with self._lock:
            return sorted(d for d in self._backfill if d < floor)

    def head_count(self, what=None):
        with self._lock:
            return sum(1 for w in self._heads if what is None or w == what)


class Adapter:
    """Healthy scripted adapter; records every session it is asked to serve."""

    _CONIDS, _SYMBOLS, _ALLOC = {}, {}, threading.Lock()

    def __init__(self, port, ledger):
        self.port, self.ledger = port, ledger
        self.head = datetime.combine(HEAD, datetime.min.time(),
                                     tzinfo=timezone.utc)

    @classmethod
    def conid_for(cls, symbol):
        with cls._ALLOC:
            symbol = str(symbol).upper()
            if symbol not in cls._CONIDS:
                conid = 61000 + len(cls._CONIDS)
                cls._CONIDS[symbol] = conid
                cls._SYMBOLS[conid] = symbol
            return cls._CONIDS[symbol]

    def account(self):
        return f"DU{self.port}"

    def qualify(self, symbol):
        conid = self.conid_for(symbol)
        return conid, SimpleNamespace(symbol=str(symbol).upper(), conId=conid)

    def contract_for(self, conid):
        return SimpleNamespace(
            symbol=self._SYMBOLS.get(int(conid), "?"), conId=int(conid))

    def qualify_many(self, symbols, chunk=50, progress=None, cancel=None):
        return {s: self.conid_for(s) for s in symbols}

    def head_timestamp(self, contract, what_to_show="TRADES"):
        self.ledger.note_head(what_to_show)
        return self.head

    def is_connected(self):
        return True

    def reconnect(self):
        pass

    def fetch(self, contract, end_dt, duration, bar_size, what_to_show="TRADES"):
        probe = _in_probe()
        ratio = str(what_to_show) != "TRADES"
        grain = "day" if "day" in str(bar_size).lower() else "min"
        table = DAYS[(grain, ratio)]
        end_d = end_dt.date()
        if duration.endswith("S"):
            self.ledger.note_day(end_d, probe)
            return list(table.get(end_d, []))
        n, unit = duration.split()
        span = {"D": 1, "W": 7, "M": 31, "Y": 366}[unit] * int(n)
        start_d = end_d - timedelta(days=span - 1)
        out = []
        for day in sorted(table):
            if start_d <= day <= end_d:
                self.ledger.note_day(day, probe)
                out.extend(table[day])
        return out


def fresh_root():
    root = Path(tempfile.mkdtemp(prefix="listing_floor_")) / ss.STORAGE_DIR_NAME
    root.mkdir(parents=True, exist_ok=True)
    return root


def correction(cutover, ticker, intervals=None, kind="identity_listing_truncation"):
    """A GOOG/TKO/WBD-shaped record as M1 actually writes them."""
    note = {"type": kind, "cutover": cutover, "ticker": ticker,
            "run": "listing_floor_reference", "snapshot": "reference"}
    if intervals is not None:
        note["intervals"] = list(intervals)
    return note


def seed(root, ticker, notes=None):
    """Fresh ticker with an identity-bound manifest and optional corrections."""
    conid = Adapter.conid_for(ticker)
    manifest = ss.new_manifest(ticker, ticker)
    manifest["conid"] = conid
    if notes:
        manifest["data_corrections"] = list(notes)
    ss.save_manifest(Path(root) / ticker, manifest)
    return conid


def run_pass(root, items, ledger, port=2000, since=SINCE, today=TODAY):
    def adapter_factory(host, aports):
        aport = int(aports[0])

        def make():
            return Adapter(aport, ledger)
        return make

    lines = []
    rep = sk.nightly_gap_fill_parallel(
        root, items, [port], progress=lines.append, cancel=None,
        today=today, since=since, adapter_factory=adapter_factory,
        port_up=lambda p: True, reprobe_interval=0.5)
    return rep, lines


def series_entry(rep, ticker, interval):
    for entry in rep.get("series") or []:
        if (isinstance(entry, dict)
                and str(entry.get("ticker", "")).upper() == ticker.upper()
                and str(entry.get("interval", "")) == interval):
            return entry
    return {}


def notes_text(entry):
    return " | ".join(str(n) for n in (entry.get("notes") or []))


def derived_start(entry):
    match = START_RE.search(notes_text(entry))
    return date.fromisoformat(match.group(1)) if match else None


def floor_aware(entry):
    """Whether the engine SAID anything about an identity floor."""
    text = notes_text(entry).lower()
    return any(word in text for word in
               ("cutover", "identity", "listing floor", "listing-floor"))


def stored_first_day(root, ticker, interval):
    try:
        manifest = ss.load_manifest(Path(root) / ss.canonical_ticker(ticker))
        first = sk.series_first_dt(manifest, interval)
        return first.date() if first is not None else None
    except Exception:  # noqa: BLE001
        return None


def sidecar_sha(root):
    path = Path(root) / "_ibkr_earliest.json"
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def per_day_requests(interval):
    """Whether one source request maps to exactly one session.

    Minute-base series are fetched day by day, so the requested span IS the
    planned set. A daily-base series issues one WIDE request (e.g. `1 M`) and
    keeps only its planned days, so request spans over-report there and the
    authoritative evidence is the derived start plus what reached disk.
    """
    return not ss.base_interval(interval).endswith("d")


class Result:
    """One series run, with every independent line of evidence."""

    def __init__(self, entry, ledger, root, ticker, interval):
        self.entry, self.ledger, self.root = entry, ledger, root
        self.ticker, self.interval = ticker, interval
        self.start = derived_start(entry)
        self.stored = stored_first_day(root, ticker, interval)
        self.per_day = per_day_requests(interval)
        self.requested_pre = None

    def measure(self, floor):
        self.requested_pre = (self.ledger.pre_floor(floor) if self.per_day
                              else None)
        return self

    def clamped_at(self, floor):
        """Clamped by every line of evidence that applies to this interval."""
        return (self.start == floor
                and (self.stored is None or self.stored >= floor)
                and not (self.requested_pre or []))

    def reached_before(self, floor):
        """Reached below the floor by any line of evidence."""
        return ((self.start is not None and self.start < floor)
                or (self.stored is not None and self.stored < floor)
                or bool(self.requested_pre))

    def detail(self):
        return (f"start={self.start} stored={self.stored} "
                f"requested_pre={(self.requested_pre or [])[:4]}"
                + ("" if self.per_day else " (daily-base: spans over-report)"))


def one(interval, ticker, notes=None, since=SINCE, today=TODAY, floor=None):
    """Run one empty series and return a measured Result."""
    root = fresh_root()
    seed(root, ticker, notes)
    ledger = Ledger()
    rep, _lines = run_pass(root, [(ticker, interval)], ledger,
                           since=since, today=today)
    res = Result(series_entry(rep, ticker, interval), ledger, root,
                 ticker, interval)
    return res.measure(floor if floor is not None else FLOOR)


FLAG = getattr(sk, "IDENTITY_FLOOR_CLAMP", None) is True
print(f"IDENTITY_FLOOR_CLAMP present: {FLAG}")
print(f"floor={FLOOR}  served head={HEAD}  since={SINCE}  today={TODAY}\n")

WIDE = [correction(FLOOR.isoformat(), "ZZ")]        # listing-wide: no intervals

print("=== [1] kind direction — the TKO/WBD hole (1d-hvol) ================")
R1 = one("1d-hvol", "ZZ", WIDE)
check("fixture: the 1d-hvol series ran and hit the source",
      bool(R1.entry) and bool(R1.ledger.days()),
      f"entry={bool(R1.entry)} served={len(R1.ledger.days())}")
if not FLAG:
    check("B1 defect pinned: an empty 1d-hvol backfilled PAST the recorded "
          "cutover and said nothing about it",
          R1.reached_before(FLOOR) and not floor_aware(R1.entry),
          f"{R1.detail()} floor_aware={floor_aware(R1.entry)}")
else:
    check("F1 empty 1d-hvol stopped exactly at the cutover",
          R1.clamped_at(FLOOR), R1.detail())
    check("F1b the clamp is stated in the series notes",
          floor_aware(R1.entry), f"notes={notes_text(R1.entry)!r}")

print("=== [2] price direction — no hand-written since ====================")
R2 = one("1m", "PZ", [correction(FLOOR.isoformat(), "PZ")])
check("fixture: the 1m series committed real months on disk",
      R2.stored is not None, f"added={R2.entry.get('added')} {R2.detail()}")
if not FLAG:
    check("B2 defect pinned: an empty 1m backfilled past the cutover — the "
          "exact bound M2 had to hand-write as since=2023-09-12",
          R2.reached_before(FLOOR) and R2.stored is not None
          and R2.stored < FLOOR, R2.detail())
else:
    check("F2 empty 1m stopped at the cutover, and no earlier bar reached disk",
          R2.clamped_at(FLOOR) and R2.stored is not None
          and R2.stored >= FLOOR, R2.detail())

print("=== [3] second kind token (1m-iv) =================================")
R3 = one("1m-iv", "IZ", [correction(FLOOR.isoformat(), "IZ")])
if not FLAG:
    check("B3 defect pinned: an empty 1m-iv backfilled past the cutover",
          R3.reached_before(FLOOR), R3.detail())
else:
    check("F3 empty 1m-iv stopped at the cutover",
          R3.clamped_at(FLOOR), R3.detail())

print("=== [4] precision: an interval-SCOPED correction ===================")
R4A = one("1d-hvol", "SZ", [correction(FLOOR.isoformat(), "SZ",
                                       intervals=["1d"])])
R4B = one("1m", "SM", [correction(FLOOR.isoformat(), "SM", intervals=["1d"])])
if FLAG:
    check("F4a a 1d-scoped correction clamps its own base family (1d-hvol)",
          R4A.clamped_at(FLOOR), R4A.detail())
check(("I-pre " if not FLAG else "F4b ")
      + "a 1d-scoped correction does NOT clamp an unrelated 1m series "
        "(over-clamping silently shortens healthy history)",
      R4B.start == HEAD and R4B.reached_before(FLOOR), R4B.detail())

print("=== [5] control + directionality invariants =======================")
R5 = one("1d-hvol", "NC", None)
check("I1 control: a ticker with NO corrections is untouched",
      R5.start == HEAD and R5.reached_before(FLOOR), R5.detail())

R6 = one("1d-hvol", "OF", [correction(OLD_FLOOR.isoformat(), "OF")])
check("I2 a cutover OLDER than the served head never drags the fetch back "
      "(a floor raises a start, it never lowers one)",
      R6.start == HEAD and (R6.stored is None or R6.stored >= HEAD),
      R6.detail())

R7 = one("1d-hvol", "LS", WIDE, since=LATE_SINCE, floor=LATE_SINCE)
check("I3 a `since` LATER than the cutover still wins "
      "(final start = max(derived, since, floor))",
      R7.start == LATE_SINCE
      and (R7.stored is None or R7.stored >= LATE_SINCE), R7.detail())

print("=== [6] malformed corrections must not evaporate ==================")
R8 = one("1d-hvol", "MZ", [correction("not-a-date", "MZ")])
HALTED8 = bool(R8.entry.get("halted")) or bool(R8.entry.get("error"))
if not FLAG:
    check("B4 defect pinned: a MALFORMED correction is silently ignored and "
          "the series backfills deep anyway",
          R8.reached_before(FLOOR) and not HALTED8
          and not floor_aware(R8.entry),
          f"{R8.detail()} halted={HALTED8}")
else:
    check("F6 a malformed correction fails CLOSED — the series halts or "
          "clamps, and says so; silence is not acceptable",
          HALTED8 or not R8.reached_before(FLOOR),
          f"{R8.detail()} halted={HALTED8} notes={notes_text(R8.entry)!r}")
    check("F6b the refusal is observable in the series record",
          floor_aware(R8.entry) or HALTED8, f"notes={notes_text(R8.entry)!r}")

print("=== [7] BACKWARD EXTENSION — the post-M1/M2 shape ==================")
# TKO and WBD now sit EXACTLY on their cutovers. Re-create that shape: seed a
# series bounded at the floor, then run the routine deep update that any "Max"
# range produces. Nothing here is empty, so the empty-series clamp M3 specifies
# would not even be consulted.
ROOT_BW = fresh_root()
seed(ROOT_BW, "BW", [correction(FLOOR.isoformat(), "BW")])
LED_SEED = Ledger()
run_pass(ROOT_BW, [("BW", "1m")], LED_SEED, since=FLOOR)
FIRST_SEED = stored_first_day(ROOT_BW, "BW", "1m")
check("fixture: the seed pass left the series sitting ON the cutover",
      FIRST_SEED == FLOOR, f"seed_first={FIRST_SEED}")
LED_BW = Ledger()
REP_BW, _ = run_pass(ROOT_BW, [("BW", "1m")], LED_BW, since=SINCE)
E9 = series_entry(REP_BW, "BW", "1m")
FIRST_AFTER = stored_first_day(ROOT_BW, "BW", "1m")
PRE9 = LED_BW.pre_floor(FLOOR)
EXTENDED = "backward extension" in notes_text(E9).lower()
if not FLAG:
    check("B5 defect pinned (SCOPE FINDING): a deep update BACKWARD-EXTENDED a "
          "non-empty series straight through its cutover — M1's truncation "
          "silently undone, and the empty-series clamp never applies here",
          bool(PRE9) and FIRST_AFTER is not None and FIRST_AFTER < FLOOR
          and EXTENDED,
          f"pre_floor={len(PRE9)} first {FIRST_SEED}->{FIRST_AFTER} "
          f"extended={EXTENDED}")
else:
    check("F7 the backward extension stopped at the cutover",
          not PRE9, f"pre_floor={PRE9[:5]} notes={notes_text(E9)!r}")
    check("F7b the stored start did not move below the cutover",
          FIRST_AFTER is not None and FIRST_AFTER >= FLOOR,
          f"first {FIRST_SEED}->{FIRST_AFTER}")

print("=== [8] Row 50 composition + write purity =========================")
check("I5 KIND_EARLIEST_PROBE (Row 50) is still enabled",
      getattr(sk, "KIND_EARLIEST_PROBE", None) is True,
      f"flag={getattr(sk, 'KIND_EARLIEST_PROBE', None)!r}")
ROOT_PUR = fresh_root()
seed(ROOT_PUR, "PR", WIDE)
sv.record_ibkr_earliest(ROOT_PUR, "PR", HEAD.isoformat(),
                        conid=Adapter.conid_for("PR"))
SHA_BEFORE = sidecar_sha(ROOT_PUR)
LED_PUR = Ledger()
REP_PUR, _ = run_pass(ROOT_PUR, [("PR", "1m-iv")], LED_PUR)
E10 = series_entry(REP_PUR, "PR", "1m-iv")
check("I5b the kind series still issued its OWN kind head probe "
      "(Row 50 not regressed by the clamp)",
      LED_PUR.head_count(IV_SHOW) >= 1,
      f"iv_heads={LED_PUR.head_count(IV_SHOW)} notes={notes_text(E10)!r}")
check("I6 write purity: the kind run left _ibkr_earliest.json byte-identical",
      sidecar_sha(ROOT_PUR) == SHA_BEFORE,
      f"before={SHA_BEFORE} after={sidecar_sha(ROOT_PUR)}")
if FLAG:
    check("F5 one listing-wide record protected the kind series too",
          not LED_PUR.pre_floor(FLOOR),
          f"pre_floor={LED_PUR.pre_floor(FLOOR)[:5]}")

print("\n=== SPEC QUESTION (reported, NOT gated) ===========================")
print("The M3 spec says the clamp applies to a ticker whose conId carries a")
print("correction \"or is quarantine-listed\", with details to be agreed at")
print("review. Quarantine alone carries no cutover, so it cannot BE a floor —")
print("it can only be an extra reason to go looking for one (e.g. in the")
print("quarantined manifest when the active one was reset). Evidence:")
try:
    import identity as _identity
    print(f"  identity.quarantined_conids available: "
          f"{callable(getattr(_identity, 'quarantined_conids', None))}")
except Exception as exc:  # noqa: BLE001
    print(f"  identity import failed: {exc}")
print("  vol_value_bank.identity_floor exists but rejects price tokens")
print("  (_kind_token requires an exact RTH IV/HVOL token), and its only")
print("  callers are the Row 51 refetch-settle path — so M3 cannot simply")
print("  call it for `1m`/`1d`; it must generalize or share the semantics.")
print("  DECISION FOR CODEX: reuse one parser for both, or justify a second.")

print(f"\n{N[0]} checks, {len(FAILS)} failed"
      + (" (feature stage)" if FLAG else " (baseline stage)"))
if FAILS:
    print("HARNESS FAILURE — fix before green-lighting M3:")
    for name in FAILS:
        print(f"  - {name}")
    sys.exit(1)
if not FLAG:
    print("\n[M3 PENDING] stock_ibkr.IDENTITY_FLOOR_CLAMP absent — the baseline")
    print("defects are pinned. M3 must deliver:")
    print("  - stock_ibkr.IDENTITY_FLOOR_CLAMP = True (M3 flag)")
    print("  - D1: empty-series start derivation consults data_corrections in")
    print("        BOTH sites (per-series ~3583-3678, combined ~4620-4680)")
    print("  - D2: final start = max(derived, since, identity_floor); the")
    print("        floor raises a start and never lowers one")
    print("  - D3: listing-wide records (no `intervals`) protect price AND")
    print("        both kind families; scoped records clamp only their family")
    print("  - D4: malformed corrections fail CLOSED and observably")
    print("  - D5: forward gap fills and correction-free tickers unchanged")
    print("  - D6: one shared floor parser with vol_value_bank.identity_floor,")
    print("        or a recorded justification for a second")
    sys.exit(3)
print("\nM3 ACCEPTED. Exit 0.")
sys.exit(0)
