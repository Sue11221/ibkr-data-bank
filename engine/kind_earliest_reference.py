"""Row 50 M0 acceptance harness — per-kind + per-lifecycle IBKR earliest.

User 2026-07-21 (two directives):
  "i need you to make sure that the code always fetch for new earliest day
   when fetching a new type of data like ohlc earliest day on ibkr should
   not be assumed for iv"
  "for row 50 do it for the vice versa as well. each empty ticker should
   fetch its own new ibkr earliest it self it wasn't there before"

OFFLINE ONLY: temp roots, fake adapters, redirected operation gate. No GUI,
no ports, no network, no bank writes. Safe to run while a live fetch runs.

Exit codes:
  0 = M1 feature present (`stock_ibkr.KIND_EARLIEST_PROBE is True`) AND every
      feature check passes.
  3 = feature absent — the baseline proved the CURRENT engine's documented
      defects plus the invariants that must survive M1. Expected pre-M1.
  1 = harness failure.

Contract pinned here (KIND_EARLIEST_PROBE_PLAN.md §2/§4):
  - B1/F1 (kind direction): an EMPTY `1m-iv` series whose ticker holds a
    canonical identity-bound TRADES entry in `_ibkr_earliest.json` must
    (pre-M1: does NOT) issue its own OPTION_IMPLIED_VOLATILITY head probe
    instead of consuming the TRADES cache date.
  - B2/F2 (continuity cell): a TRADES-family series on a ticker whose price
    data ALREADY EXISTS on disk still hits the strict cache with ZERO head
    calls — the Row 43 latency win is preserved exactly where the data
    lifecycle is continuous.
  - B5/F5 (vice-versa / fresh cell): a ticker whose price data is NOT stored
    (fresh add, vol-only, post-truncation rebuild) must (pre-M1: does NOT)
    ignore the durable cache, fetch its OWN new earliest (exactly one TRADES
    head probe), and RE-RECORD the fresh evidence over the stale entry.
  - B3: with NO sidecar entry the kind live probe is already kind-correct
    (exactly one OPTION_IMPLIED_VOLATILITY head call) — pinned forever.
  - B4/F3: kind runs leave `_ibkr_earliest.json` byte-identical (write
    purity: the flat sidecar stays TRADES-only).
  - F4: `1d-hvol` resolves through HISTORICAL_VOLATILITY.

Claude-owned: Codex MUST NOT weaken these assertions; harness changes
require Claude sign-off on the board.
"""
import atexit
import hashlib
import json
import sys
import tempfile
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
import operation_gate as og                                    # noqa: E402

_GATE_OLD = og.LOCK_PATH
_GATE_DIR = tempfile.TemporaryDirectory(prefix="kind_earliest_gate_",
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

NY = ZoneInfo("America/New_York")
MON = date(2024, 6, 17)
TUE = MON + timedelta(days=1)
WED = MON + timedelta(days=2)
SINCE = MON - timedelta(days=21)
STALE = MON - timedelta(days=14)          # a stale-lifecycle cache date
IV_SHOW = "OPTION_IMPLIED_VOLATILITY"
HV_SHOW = "HISTORICAL_VOLATILITY"
FAILS = []
N = [0]


def check(name, ok, detail=""):
    N[0] += 1
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + ("" if ok else f"  {detail}"))
    if not ok:
        FAILS.append(name)


def fresh_root():
    root = Path(tempfile.mkdtemp(prefix="kind_earliest_")) / ss.STORAGE_DIR_NAME
    root.mkdir(parents=True, exist_ok=True)
    return root


def stub_bars(day, n, base):
    out, t = [], datetime.combine(day, ss.RTH_FIRST)
    for i in range(n):
        p = base + i * 0.01
        out.append(SimpleNamespace(
            date=t.replace(tzinfo=NY).astimezone(timezone.utc),
            open=p, high=p + 0.05, low=p - 0.05, close=p + 0.01,
            volume=float(1000 + i)))
        t += timedelta(minutes=1)
    return out


SHARED_DAYS = {MON: stub_bars(MON, 4, 10.0), TUE: stub_bars(TUE, 4, 11.0)}


class HeadLedger:
    """Thread-safe head-call evidence: (symbol, what_to_show) rows."""

    def __init__(self):
        self._lock = threading.Lock()
        self.heads = []

    def add(self, symbol, what):
        with self._lock:
            self.heads.append((str(symbol).upper(), str(what)))

    def count(self, what=None, symbol=None):
        with self._lock:
            return sum(
                1 for s, w in self.heads
                if (what is None or w == what)
                and (symbol is None or s == str(symbol).upper()))


class KindAdapter:
    """Healthy scripted adapter recording head what_to_show evidence."""

    _CONIDS = {}
    _SYMBOLS = {}
    _ALLOC = threading.Lock()

    def __init__(self, port, ledger, days):
        self.port, self.ledger, self.days = port, ledger, days
        self.head = datetime.combine(MON, datetime.min.time(),
                                     tzinfo=timezone.utc)

    @classmethod
    def conid_for(cls, symbol):
        with cls._ALLOC:
            symbol = str(symbol).upper()
            if symbol not in cls._CONIDS:
                conid = 51000 + len(cls._CONIDS)
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
        self.ledger.add(getattr(contract, "symbol", "?"), what_to_show)
        return self.head

    def is_connected(self):
        return True

    def reconnect(self):
        pass

    def fetch(self, contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        end_d = end_dt.date()
        if duration.endswith("S"):
            return list(self.days.get(end_d, []))
        n, unit = duration.split()
        span = {"D": 1, "W": 7, "M": 31, "Y": 366}[unit] * int(n)
        start_d = end_d - timedelta(days=span - 1)
        out = []
        for d in sorted(self.days):
            if start_d <= d <= end_d:
                out.extend(self.days[d])
        return out


def seed_sidecar(root, ticker, day=MON):
    """Canonical identity-bound TRADES entry via the REAL recorder.

    Cache authority needs BOTH halves (Row 43 fixture revision): the pinned
    manifest conId must equal the live resolved conId, and the sidecar entry
    must be conId-bound to the same identity."""
    conid = KindAdapter.conid_for(ticker)
    manifest = ss.new_manifest(ticker, ticker)
    manifest["conid"] = conid
    ss.save_manifest(Path(root) / ticker, manifest)
    sv.record_ibkr_earliest(root, ticker, day.isoformat(), conid=conid)
    return conid


def sidecar_sha(root):
    p = Path(root) / "_ibkr_earliest.json"
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None


def sidecar_entry(root, ticker):
    p = Path(root) / "_ibkr_earliest.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8")).get(ticker)
    except Exception:  # noqa: BLE001
        return None


def run_pass(root, items, ledger, port=2000):
    def adapter_factory(host, aports):
        aport = int(aports[0])

        def make():
            return KindAdapter(aport, ledger, SHARED_DAYS)
        return make

    lines = []
    rep = sk.nightly_gap_fill_parallel(
        root, items, [port], progress=lines.append, cancel=None,
        today=WED, since=SINCE, adapter_factory=adapter_factory,
        port_up=lambda p: True, reprobe_interval=0.5)
    return rep, lines


def series_entry(rep, ticker, interval):
    for entry in rep.get("series") or []:
        if (isinstance(entry, dict)
                and str(entry.get("ticker", "")).upper() == ticker
                and str(entry.get("interval", "")) == interval):
            return entry
    return {}


def notes_text(entry):
    return " | ".join(str(n) for n in (entry.get("notes") or [])).lower()


FLAG = getattr(sk, "KIND_EARLIEST_PROBE", None) is True
print(f"KIND_EARLIEST_PROBE present: {FLAG}\n")

print("=== [1] kind series vs a TRADES sidecar entry =====================")
ROOT_KV = fresh_root()
seed_sidecar(ROOT_KV, "KV")
LED_KV = HeadLedger()
SHA_KV_BEFORE = sidecar_sha(ROOT_KV)
REP_KV, _ = run_pass(ROOT_KV, [("KV", "1m-iv")], LED_KV)
E_KV = series_entry(REP_KV, "KV", "1m-iv")
check("fixture: the 1m-iv series ran and reported",
      bool(E_KV), f"series entries: {len(REP_KV.get('series') or [])}")
iv_heads = LED_KV.count(what=IV_SHOW, symbol="KV")
cache_note = "cache" in notes_text(E_KV)

if not FLAG:
    check("B1 defect pinned: kind series consumed the TRADES cache date "
          "and issued ZERO of its own kind head probes",
          cache_note and iv_heads == 0,
          f"cache_note={cache_note} iv_heads={iv_heads}")
else:
    check("F1 kind series issued exactly ONE kind head probe and did NOT "
          "consume the TRADES cache",
          (not cache_note) and iv_heads == 1,
          f"cache_note={cache_note} iv_heads={iv_heads}")

check(("B4" if not FLAG else "F3")
      + " write purity: the kind run left _ibkr_earliest.json byte-identical",
      sidecar_sha(ROOT_KV) == SHA_KV_BEFORE,
      f"before={SHA_KV_BEFORE} after={sidecar_sha(ROOT_KV)}")

print("=== [2] TRADES continuity: price data existed before ==============")
ROOT_PX = fresh_root()
seed_sidecar(ROOT_PX, "PX")
LED_SEED = HeadLedger()
REP_SEED, _ = run_pass(ROOT_PX, [("PX", "1m")], LED_SEED)
check("fixture: the PX seed pass committed a real 1m month on disk",
      ss.find_month_file(ROOT_PX, "PX", MON.year, MON.month, "1m")
      is not None,
      f"seed notes: {notes_text(series_entry(REP_SEED, 'PX', '1m'))!r}")
LED_PX = HeadLedger()
REP_PX, _ = run_pass(ROOT_PX, [("PX", "1m-pre")], LED_PX)
E_PX = series_entry(REP_PX, "PX", "1m-pre")
check(("B2" if not FLAG else "F2")
      + " TRADES-family series on a ticker WITH stored price months still "
      "hits the strict cache with ZERO head calls",
      "cache" in notes_text(E_PX) and LED_PX.count(symbol="PX") == 0,
      f"notes={notes_text(E_PX)!r} heads={LED_PX.count(symbol='PX')}")

print("=== [3] kind live probe without a sidecar entry ===================")
ROOT_NV = fresh_root()
LED_NV = HeadLedger()
REP_NV, _ = run_pass(ROOT_NV, [("NV", "1m-iv")], LED_NV)
E_NV = series_entry(REP_NV, "NV", "1m-iv")
check("B3 no-sidecar kind series issues exactly ONE kind-correct head probe",
      LED_NV.count(what=IV_SHOW, symbol="NV") == 1
      and "cache" not in notes_text(E_NV),
      f"iv_heads={LED_NV.count(what=IV_SHOW, symbol='NV')} "
      f"notes={notes_text(E_NV)!r}")

print("=== [4] vice-versa fresh cell: price was NOT there before =========")
ROOT_FR = fresh_root()
seed_sidecar(ROOT_FR, "FR", day=STALE)     # stale-lifecycle entry, NO months
LED_FR = HeadLedger()
REP_FR, _ = run_pass(ROOT_FR, [("FR", "1m")], LED_FR)
E_FR = series_entry(REP_FR, "FR", "1m")
fr_heads = LED_FR.count(what="TRADES", symbol="FR")
fr_cache = "cache" in notes_text(E_FR)
if not FLAG:
    check("B5 defect pinned: an empty-price ticker (no stored TRADES "
          "months) consumed the stale durable cache and issued ZERO fresh "
          "probes",
          fr_cache and fr_heads == 0,
          f"cache_note={fr_cache} trades_heads={fr_heads}")
else:
    check("F5 empty-price ticker fetched its OWN new earliest (exactly one "
          "TRADES head probe) and did NOT consume the stale cache",
          (not fr_cache) and fr_heads == 1,
          f"cache_note={fr_cache} trades_heads={fr_heads}")
    entry = sidecar_entry(ROOT_FR, "FR")
    fresh_date = (entry or {}).get("earliest") if isinstance(entry, dict) \
        else entry
    check("F5b the fresh probe RE-RECORDED the sidecar over the stale date",
          fresh_date == MON.isoformat(),
          f"entry={entry!r} expected earliest={MON.isoformat()!r}")

if FLAG:
    print("=== [5] second kind token (1d-hvol) ===========================")
    ROOT_HV = fresh_root()
    seed_sidecar(ROOT_HV, "HV")
    LED_HV = HeadLedger()
    REP_HV, _ = run_pass(ROOT_HV, [("HV", "1d-hvol")], LED_HV)
    E_HV = series_entry(REP_HV, "HV", "1d-hvol")
    check("F4 1d-hvol issued exactly ONE HISTORICAL_VOLATILITY head probe "
          "and did NOT consume the TRADES cache",
          LED_HV.count(what=HV_SHOW, symbol="HV") == 1
          and "cache" not in notes_text(E_HV),
          f"hv_heads={LED_HV.count(what=HV_SHOW, symbol='HV')} "
          f"notes={notes_text(E_HV)!r}")

print(f"\n{N[0]} checks, {len(FAILS)} failed"
      + (" (feature stage)" if FLAG else " (baseline stage)"))
if FAILS:
    print("HARNESS FAILURE — fix before green-lighting M1:")
    for name in FAILS:
        print(f"  - {name}")
    sys.exit(1)
if not FLAG:
    print("\n[M1 PENDING] stock_ibkr.KIND_EARLIEST_PROBE absent — the baseline")
    print("defects are pinned. M1 must deliver (KIND_EARLIEST_PROBE_PLAN.md §2):")
    print("  - stock_ibkr.KIND_EARLIEST_PROBE = True (M1 flag)")
    print("  - D1: the _cached_pickup_start sidecar consult gated to TRADES")
    print("  - D2: empty kind series issue their own whatToShow head probe")
    print("  - D3: kind runs never write the flat TRADES sidecar")
    print("  - D4: continuity-cell TRADES pickup behavior byte-identical")
    print("  - D5: an empty-price ticker ignores the durable cache, fetches")
    print("        its own new earliest, and re-records the fresh evidence")
    sys.exit(3)
print("\nM1 ACCEPTED. Exit 0.")
sys.exit(0)
