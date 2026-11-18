"""Row 43 M0 acceptance harness — fetch pickup latency (user list item 3).

OFFLINE ONLY: temp roots, fake adapters, redirected operation gate. No GUI,
no ports, no network, no bank writes. Safe to run while a live fetch runs.

Exit codes:
  0 = M1 feature present AND every scenario passes.
  3 = feature absent (M1 not implemented yet) — the baseline proved itself
      against the CURRENT engine (including the documented defects) and the
      exact missing surface is listed. Expected pre-M1 result.
  1 = harness failure.

Contract pinned here (FETCH_PICKUP_LATENCY_PLAN.md §2/§5):
  - Module flag `stock_ibkr.PICKUP_FAST_START` marks M1 present.
  - D1: an EMPTY series whose ticker has a CANONICAL IDENTITY-BOUND entry in
    `_ibkr_earliest.json` — shape {"earliest": "YYYY-MM-DD", "conid": N}
    with N matching the ticker's pinned-manifest AND live conId, seeded via
    the real `stock_validate.record_ibkr_earliest(..., conid=N)` — issues
    ZERO live head_timestamp calls and notes the cache use (a per-series
    note containing "cache", case-insensitive). Legacy ticker-only strings
    carry NO cache authority (fixture revised 2026-07-21, Claude sign-off:
    the pre-M1 CH fixture was an unbound legacy string). A cache-MISS empty
    series still issues exactly ONE head call. A resumed series issues none
    (true today; pinned forever). A malformed sidecar entry must not crash
    the run and must fall back to the live probe for that ticker.
    ROW 50 SUPERSESSION (2026-07-22, Claude sign-off): the cache-hit cell
    additionally seeds REAL stored price months for the ticker (different
    interval), because the user's vice-versa rule strips cache authority
    from tickers with zero stored price months. That empty-ticker cell is
    owned by kind_earliest_reference.py (B5/F5), not this harness.
  - D2: on one port, plan_gap for job k+1 is called BEFORE the final
    historical fetch of job k is issued (pre-planned pickup). Today the
    order is strictly serial — documented as a defect while M1 is absent.
  - D3: kind tokens resolve to their base interval's fetch span
    (`_FETCH_SPAN.get(tok, _FETCH_SPAN[base])`), and no explicit kind key
    may shadow its base with a different span.

Claude-owned: Codex MUST NOT weaken these assertions; harness changes
require Claude sign-off.
"""
import atexit
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
_GATE_DIR = tempfile.TemporaryDirectory(prefix="pickup_ref_gate_",
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
SINCE = MON - timedelta(days=3)
FAILS = []
N = [0]


def check(name, ok, detail=""):
    N[0] += 1
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + ("" if ok else f"  {detail}"))
    if not ok:
        FAILS.append(name)


def fresh_root():
    root = Path(tempfile.mkdtemp(prefix="pickup_ref_")) / ss.STORAGE_DIR_NAME
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


class PickupLedger:
    """Thread-safe evidence: head calls, plan calls, fetch call/return times."""

    def __init__(self):
        self._lock = threading.Lock()
        self.heads = []        # (monotonic, symbol, what_to_show)
        self.plans = []        # (monotonic, ticker)
        self.fetch_calls = []  # (monotonic, symbol, duration)
        self.fetch_rets = []   # (monotonic, symbol, duration)

    def add(self, kind, row):
        with self._lock:
            getattr(self, kind).append(row)

    def heads_for(self, symbol):
        with self._lock:
            return [r for r in self.heads if r[1] == symbol]

    def snapshot(self, kind):
        with self._lock:
            return list(getattr(self, kind))


class PickupAdapter:
    """Healthy scripted adapter that records head/fetch evidence.

    The engine resolves tickers to conIds in a batch pre-pass and then works
    with `contract_for(conid)` objects, so per-symbol conIds and a reverse
    map are REQUIRED for the ledger to attribute head/fetch calls to
    tickers (single-conid fixtures log everything as one anonymous
    contract — measured 2026-07-21)."""

    _CONIDS = {}
    _SYMBOLS = {}
    _ALLOC = threading.Lock()

    def __init__(self, port, ledger, days, slow_s=0.02):
        self.port, self.ledger, self.days = port, ledger, days
        self.slow_s = slow_s
        self.head = datetime.combine(MON, datetime.min.time(),
                                     tzinfo=timezone.utc)

    @classmethod
    def _conid(cls, symbol):
        with cls._ALLOC:
            symbol = str(symbol).upper()
            if symbol not in cls._CONIDS:
                conid = 43000 + len(cls._CONIDS)
                cls._CONIDS[symbol] = conid
                cls._SYMBOLS[conid] = symbol
            return cls._CONIDS[symbol]

    def account(self):
        return f"DU{self.port}"

    def qualify(self, symbol):
        conid = self._conid(symbol)
        return conid, SimpleNamespace(symbol=str(symbol).upper(),
                                      conId=conid)

    def contract_for(self, conid):
        return SimpleNamespace(
            symbol=self._SYMBOLS.get(int(conid), "?"), conId=int(conid))

    def qualify_many(self, symbols, chunk=50, progress=None, cancel=None):
        return {s: self._conid(s) for s in symbols}

    def head_timestamp(self, contract, what_to_show="TRADES"):
        self.ledger.add("heads", (time.monotonic(),
                                  getattr(contract, "symbol", "?"),
                                  what_to_show))
        return self.head

    def is_connected(self):
        return True

    def reconnect(self):
        pass

    def fetch(self, contract, end_dt, duration, bar_size, what_to_show="TRADES"):
        symbol = getattr(contract, "symbol", "?")
        self.ledger.add("fetch_calls", (time.monotonic(), symbol, duration))
        if self.slow_s:
            time.sleep(self.slow_s)
        end_d = end_dt.date()
        if duration.endswith("S"):
            out = list(self.days.get(end_d, []))
        else:
            n, unit = duration.split()
            span = {"D": 1, "W": 7, "M": 31}[unit] * int(n)
            start_d = end_d - timedelta(days=span - 1)
            out = []
            for d in sorted(self.days):
                if start_d <= d <= end_d:
                    out.extend(self.days[d])
        self.ledger.add("fetch_rets", (time.monotonic(), symbol, duration))
        return out


class _PlanTap:
    """Wrap stock_ibkr.plan_gap so per-job plan calls land in the ledger."""

    def __init__(self, ledger):
        self.ledger = ledger
        self.real = sk.plan_gap

    def __enter__(self):
        def wrapped(root, ticker, interval, **kwargs):
            self.ledger.add("plans", (time.monotonic(), str(ticker).upper()))
            return self.real(root, ticker, interval, **kwargs)
        sk.plan_gap = wrapped
        return self

    def __exit__(self, *exc):
        sk.plan_gap = self.real


def run_pass(root, items, ledger, port=2000, since=None):
    """Drive a REAL gap_fill_parallel pass. `since=None` is the update-style
    depth (no explicit lookback): empty series still resolve their start via
    the live head path, while resumed series carry no backfill-earlier
    signal — the exact shape whose pickup latency item 3 targets."""
    def adapter_factory(host, aports):
        aport = int(aports[0])

        def make():
            return PickupAdapter(aport, ledger, SHARED_DAYS)
        return make

    lines = []
    with _PlanTap(ledger):
        rep = sk.nightly_gap_fill_parallel(
            root, items, [port], progress=lines.append, cancel=None,
            today=WED, since=since, adapter_factory=adapter_factory,
            port_up=lambda p: True, reprobe_interval=0.5)
    return rep, lines


def series_entries(rep):
    out = {}
    for entry in rep.get("series") or []:
        if isinstance(entry, dict):
            key = (str(entry.get("ticker", "?")).upper(),
                   str(entry.get("interval", "?")))
            out[key] = entry
    return out


def notes_text(entry):
    if not isinstance(entry, dict):
        return ""
    return " | ".join(str(n) for n in (entry.get("notes") or []))


print("=== [A] one-port pickup evidence run ==============================")
ROOT_A = fresh_root()
# pass 1: populate RS so pass 2 resumes it (real committed months)
_seed_ledger = PickupLedger()
_seed_rep, _ = run_pass(ROOT_A, [("RS", "1m")], _seed_ledger)
check("A0 seed pass: RS populated by a real run",
      len(_seed_rep.get("series") or []) == 1
      and len(_seed_ledger.heads_for("RS")) == 1,
      f"series={len(_seed_rep.get('series') or [])} "
      f"heads={_seed_ledger.heads_for('RS')}")

# cache seeded for CH through the REAL sidecar writer, IDENTITY-BOUND per the
# M1 contract: cache authority requires {"earliest", "conid"} matching the
# exact pinned-manifest + live conId (unbound legacy strings fail closed to a
# live probe — that non-authority is exercised by A4/G1, not CH).
#
# ROW 50 SUPERSESSION (fixture revised 2026-07-22, Claude sign-off): the user's
# vice-versa rule makes "no stored price months = no cache authority" (D5 in
# KIND_EARLIEST_PROBE_PLAN.md), so the cache-hit cell here makes CH a genuinely
# POPULATED ticker by committing a REAL TRADES-family month on a DIFFERENT
# interval (1h) first. The CH 1m series stays an EMPTY pickup (exercises the
# head-start cache), while the TICKER holds a real stored TRADES-family month —
# so CH legitimately RETAINS cache authority both pre- and post-Row-50 D5,
# while the zero-stored-month empty ticker (kind_earliest_reference.py B5/F5)
# does NOT. This closes the Codex finding (2026-07-22, Row 50 marker rev 5):
# the earlier "1d" seed committed nothing — the daily path stores nothing from
# this minute-scale fixture (probed: 1m/2m/5m/15m/30m/1h commit, 1d/3m do not),
# leaving CH lifecycle-identical to the empty ticker. 1h commits and is
# TRADES-family, so a strict D1-D5 now keeps Row 43 F1/F2 AND Row 50 F5 green.
_ch_seed_ledger = PickupLedger()
_ch_seed_rep, _ = run_pass(ROOT_A, [("CH", "1h")], _ch_seed_ledger)
_ch_month = ss.find_month_file(ROOT_A, "CH", MON.year, MON.month, "1h")
_ch_seed_man = ss.load_manifest(ROOT_A / "CH")
check("A0b CH continuity seed: a REAL TRADES-family month is committed on disk "
      "(1h) and manifest-recorded, so CH is a populated ticker (Row 50 D5-safe)",
      len(_ch_seed_rep.get("series") or []) == 1
      and _ch_month is not None and _ch_month.exists()
      and isinstance(_ch_seed_man, dict)
      and "1h" in (_ch_seed_man.get("intervals") or {}),
      f"series={len(_ch_seed_rep.get('series') or [])} month={_ch_month} "
      f"intervals={sorted((_ch_seed_man or {}).get('intervals') or {})}")
CH_CONID = PickupAdapter._conid("CH")     # deterministic; qualify reuses it
_ch_manifest = ss.load_manifest(ROOT_A / "CH") or ss.new_manifest("CH", "CH")
_ch_manifest["conid"] = CH_CONID          # pinned identity == live identity
ss.save_manifest(ROOT_A / "CH", _ch_manifest)
sv.record_ibkr_earliest(ROOT_A, "CH", MON.isoformat(), conid=CH_CONID)
_sidecar = json.loads((ROOT_A / "_ibkr_earliest.json").read_text("utf-8"))
check("A1 sidecar: real writer recorded CH as canonical bound evidence",
      _sidecar.get("CH") == {"earliest": MON.isoformat(),
                             "conid": CH_CONID},
      json.dumps(_sidecar)[:200])

LEDGER = PickupLedger()
REP, LINES = run_pass(ROOT_A, [("CH", "1m"), ("CM", "1m"), ("RS", "1m")],
                      LEDGER)
ENTRIES = series_entries(REP)
check("A2 run: all three series complete",
      len(REP.get("series") or []) == 3
      and {("CH", "1m"), ("CM", "1m"), ("RS", "1m")} <= set(ENTRIES),
      f"series={sorted(ENTRIES)} rep_keys={sorted(REP)}")
check("A3 resumed series issues ZERO live head probes (pinned forever)",
      len(LEDGER.heads_for("RS")) == 0, str(LEDGER.heads_for("RS")))
check("A4 cache-miss empty series issues exactly ONE head probe",
      len(LEDGER.heads_for("CM")) == 1, str(LEDGER.heads_for("CM")))

print("=== [B] span inheritance for kind tokens ==========================")


def span_for(token):
    return sk._FETCH_SPAN.get(token, sk._FETCH_SPAN[ss.base_interval(token)])


_kind_ok = True
_kind_detail = []
for tok in ("1m-iv", "1m-hvol", "5m-iv", "1d-iv", "1d-hvol"):
    base = ss.base_interval(tok)
    if base not in sk._FETCH_SPAN:
        continue
    same = span_for(tok) == sk._FETCH_SPAN[base]
    shadow = tok in sk._FETCH_SPAN and sk._FETCH_SPAN[tok] != sk._FETCH_SPAN[base]
    _kind_detail.append(f"{tok}->{span_for(tok)} (base {base})")
    if not same or shadow:
        _kind_ok = False
check("B1 kind tokens inherit their base interval's measured span",
      _kind_ok, "; ".join(_kind_detail))
check("B2 the measured 1m span governs 1m-iv (the current IV campaign)",
      span_for("1m-iv") == sk._FETCH_SPAN["1m"],
      f"1m-iv->{span_for('1m-iv')} 1m->{sk._FETCH_SPAN['1m']}")

print("=== [feature probe] M1 surface ====================================")
_missing = []
if not getattr(sk, "PICKUP_FAST_START", False):
    _missing.append("stock_ibkr.PICKUP_FAST_START = True (M1 flag)")

if _missing:
    # Document today's defects precisely while the feature is absent.
    check("D1-defect (today): cache-hit empty series STILL issues a live "
          "head probe (sidecar unused by fetch)",
          len(LEDGER.heads_for("CH")) == 1, str(LEDGER.heads_for("CH")))

    def _serial_pickup(ledger, first, second):
        plans = {t: [r for r in ledger.snapshot("plans") if r[1] == t]
                 for t in (first, second)}
        rets = [r for r in ledger.snapshot("fetch_rets") if r[1] == first]
        if not plans[first] or not plans[second] or not rets:
            return False
        return plans[second][-1][0] >= rets[-1][0]

    check("D2-defect (today): pickup is strictly serial — the next series' "
          "plan starts only after the prior series' last fetch returned",
          _serial_pickup(LEDGER, "CH", "CM")
          and _serial_pickup(LEDGER, "CM", "RS"),
          f"plans={LEDGER.snapshot('plans')}")
    print()
    print(f"{N[0]} checks, {len(FAILS)} failed (baseline stage)")
    if FAILS:
        for f in FAILS:
            print(f"  FAILED: {f}")
        print("ALL FAIL — baseline broken")
        sys.exit(1)
    print("FEATURE ABSENT — M1 not implemented yet. Missing surface:")
    for m in _missing:
        print(f"  - {m}")
    print("Required M1 behavior (FETCH_PICKUP_LATENCY_PLAN.md section 2):")
    print("  - D1 cache-hit empty series: ZERO head_timestamp calls; a")
    print("    per-series note containing 'cache'; cache-miss still probes")
    print("    exactly once; malformed sidecar entries fall back to the live")
    print("    probe without crashing the run.")
    print("  - D2 pre-planned pickup: on one port, plan_gap(job k+1) is")
    print("    called BEFORE job k's final historical fetch is issued.")
    print("  - D3 span inheritance stays green (checks B1/B2).")
    print("Expected pre-M1 result. Exit 3.")
    sys.exit(3)

print("=== [F] M1 feature contract =======================================")
check("F1 cache-hit empty series issues ZERO live head probes",
      len(LEDGER.heads_for("CH")) == 0, str(LEDGER.heads_for("CH")))
check("F2 cache use is noted on the CH series",
      "cache" in notes_text(ENTRIES.get(("CH", "1m"))).lower(),
      notes_text(ENTRIES.get(("CH", "1m"))))
check("F3 cache-miss still probes exactly once (no skip without evidence)",
      len(LEDGER.heads_for("CM")) == 1, str(LEDGER.heads_for("CM")))


def _preplanned(ledger, first, second):
    plans = {t: [r for r in ledger.snapshot("plans") if r[1] == t]
             for t in (first, second)}
    calls = [r for r in ledger.snapshot("fetch_calls")
             if r[1] == first and not r[2].endswith("S")]
    if not plans[first] or not plans[second] or not calls:
        return False
    return plans[second][-1][0] <= calls[-1][0]


check("F4 pre-planned pickup: the next series' plan precedes the prior "
      "series' final historical fetch",
      _preplanned(LEDGER, "CH", "CM") and _preplanned(LEDGER, "CM", "RS"),
      f"plans={LEDGER.snapshot('plans')}")

print("=== [G] malformed sidecar entry falls back safely =================")
ROOT_G = fresh_root()
_g_path = ROOT_G / "_ibkr_earliest.json"
_g_path.write_text(json.dumps({"CX": {"earliest": "not-a-date"}}), "utf-8")
G_LEDGER = PickupLedger()
G_REP, _ = run_pass(ROOT_G, [("CX", "1m")], G_LEDGER)
check("G1 malformed cache entry: run completes and falls back to the live "
      "head probe",
      len(G_REP.get("series") or []) == 1
      and len(G_LEDGER.heads_for("CX")) == 1,
      f"series={len(G_REP.get('series') or [])} "
      f"heads={G_LEDGER.heads_for('CX')}")

print()
print(f"{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    for f in FAILS:
        print(f"  FAILED: {f}")
    sys.exit(1)
print("ALL PASS")
sys.exit(0)
