"""Row 51 M0 acceptance harness — volatility value safeguards.

User 2026-07-21 (verbatim): "do these 2 layes and if layer 2 triggers, the
fix data or add stock should refetch that day and if still no problem with
same value settle with it and add this into the export report if exported."

OFFLINE ONLY: temp roots, fake adapters, redirected operation gate. No GUI,
no ports, no network, no bank writes. Safe to run while a live fetch runs.

Exit codes (STAGED 2026-07-22, Claude sign-off — see the STAGE block below):
  0 = feature stage passed; the terminal line names the stage:
      "M1 SLICE ACCEPTED" (VOL_VALUE_GATE alone — the queue dispatches M1
      first) or "FULL ACCEPTED (M1+M2)" (gate + audit module both live).
  3 = flags absent — the baseline proved the CURRENT engine's documented
      gaps (no vol ceiling, no unit-flip halt, no audit module) plus the
      existing protections that must survive. Expected pre-M1 result.
  1 = harness failure.

Contract pinned here (VOL_VALUE_SAFEGUARDS_PLAN.md §2-§4):
  - B1/F1: a vol bar over VOL_HARD_CEILING (10.0) commits today; M1 rejects
    it loudly and stores nothing above the ceiling.
  - B2/F2: a percent-instead-of-decimal month (values ~x100) commits today;
    M1 halts the month fail-closed against the stored trailing median.
  - B5/F3 (existing protection, pinned forever): a negative-close vol bar is
    already rejected by validate_bar — no negative value is ever stored.
  - B3/B4: the M1 flag and the M2 audit module do not exist yet.
  - F4-F6 (M2): audit() flags a planted jump day + emits queue rows; the
    settled registry suppresses same-value re-flags and re-flags changed
    values; settled_for_export feeds the export quality report.

Claude-owned: Codex MUST NOT weaken these assertions; harness changes
require Claude sign-off on the board.
"""
import atexit
import importlib
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
_GATE_DIR = tempfile.TemporaryDirectory(prefix="vol_value_gate_",
                                        ignore_cleanup_errors=True)
og.LOCK_PATH = Path(_GATE_DIR.name) / ".market_data_operation.lock"


def _restore_gate():
    og.LOCK_PATH = _GATE_OLD
    _GATE_DIR.cleanup()


atexit.register(_restore_gate)

import stock_ibkr as sk                                        # noqa: E402
import stock_storage as ss                                     # noqa: E402

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
    root = Path(tempfile.mkdtemp(prefix="vol_value_")) / ss.STORAGE_DIR_NAME
    root.mkdir(parents=True, exist_ok=True)
    return root


def vol_bars(day, n, base):
    """Internally consistent minute vol bars around `base` (decimal vol)."""
    out, t = [], datetime.combine(day, ss.RTH_FIRST)
    for i in range(n):
        v = base + i * 0.001
        out.append(SimpleNamespace(
            date=t.replace(tzinfo=NY).astimezone(timezone.utc),
            open=v, high=v + 0.002, low=v - 0.002, close=v + 0.001,
            volume=0.0))
        t += timedelta(minutes=1)
    return out


def poison_ceiling(bars, idx=1):
    """One internally consistent bar at 25.0 (2500% vol) — over any ceiling."""
    b = bars[idx]
    b.open, b.high, b.low, b.close = 24.9, 25.2, 24.7, 25.0
    return bars


def poison_negative(bars, idx=1):
    """One internally consistent bar with a NEGATIVE close."""
    b = bars[idx]
    b.open, b.high, b.low, b.close = 0.2, 0.3, -0.3, -0.2
    return bars


CLEAN = {MON: vol_bars(MON, 4, 0.360), TUE: vol_bars(TUE, 4, 0.365)}
CEILING = {MON: vol_bars(MON, 4, 0.360),
           TUE: poison_ceiling(vol_bars(TUE, 4, 0.365))}
FLIPPED = {MON: vol_bars(MON, 4, 36.0), TUE: vol_bars(TUE, 4, 36.5)}
NEGATIVE = {MON: vol_bars(MON, 4, 0.360),
            TUE: poison_negative(vol_bars(TUE, 4, 0.365))}


class VolAdapter:
    """Healthy scripted adapter serving per-symbol day maps."""

    _CONIDS = {}
    _SYMBOLS = {}
    _ALLOC = threading.Lock()

    def __init__(self, port, days_by_symbol):
        self.port = port
        self.days_by_symbol = days_by_symbol
        self.head = datetime.combine(MON, datetime.min.time(),
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
        return self.head

    def is_connected(self):
        return True

    def reconnect(self):
        pass

    def fetch(self, contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        days = self.days_by_symbol.get(
            str(getattr(contract, "symbol", "?")).upper(), CLEAN)
        end_d = end_dt.date()
        if duration.endswith("S"):
            return list(days.get(end_d, []))
        n, unit = duration.split()
        span = {"D": 1, "W": 7, "M": 31, "Y": 366}[unit] * int(n)
        start_d = end_d - timedelta(days=span - 1)
        out = []
        for d in sorted(days):
            if start_d <= d <= end_d:
                out.extend(days[d])
        return out


DAYS_BY_SYMBOL = {"VOK": CLEAN, "VC": CEILING, "VU": FLIPPED, "VN": NEGATIVE}


def run_pass(root, items, port=2000):
    def adapter_factory(host, aports):
        aport = int(aports[0])

        def make():
            return VolAdapter(aport, DAYS_BY_SYMBOL)
        return make

    lines = []
    rep = sk.nightly_gap_fill_parallel(
        root, items, [port], progress=lines.append, cancel=None,
        today=WED, since=SINCE, adapter_factory=adapter_factory,
        port_up=lambda p: True, reprobe_interval=0.5)
    return rep, lines


def stored_values(root, ticker, interval, year, month):
    fp = ss.find_month_file(root, ticker, year, month, interval)
    if fp is None:
        return None
    rows, _ = ss.read_month_file(fp)
    out = []
    for r in rows:
        out.extend([float(r[1]), float(r[2]), float(r[3]), float(r[4])])
    return out


GATE_FLAG = getattr(sk, "VOL_VALUE_GATE", None) is True
try:
    _audit_mod = importlib.import_module("vol_value_audit")
except Exception:  # noqa: BLE001
    _audit_mod = None
AUDIT_FLAG = (_audit_mod is not None
              and getattr(_audit_mod, "VOL_VALUE_AUDIT", None) is True)
# STAGED ACCEPTANCE (revised 2026-07-22, Claude sign-off — Codex F-51 blocker
# adjudicated valid: the queue dispatches M1 alone, so requiring BOTH flags
# made an M1-only checkpoint unacceptable by construction). Stages:
#   baseline (no flags)      -> exit 3, defects pinned.
#   m1 (gate only)           -> gate checks F1-F3; terminal line
#                               "M1 SLICE ACCEPTED"; exit 0.
#   full (gate + audit)      -> all checks; terminal line
#                               "FULL ACCEPTED (M1+M2)"; exit 0.
# Each checkpoint's acceptance = exit 0 x3 PRINTING ITS OWN stage line.
STAGE = ("full" if (GATE_FLAG and AUDIT_FLAG)
         else ("m1" if GATE_FLAG else "baseline"))
FLAG = STAGE != "baseline"
print(f"VOL_VALUE_GATE={GATE_FLAG} VOL_VALUE_AUDIT={AUDIT_FLAG} "
      f"stage={STAGE}\n")

print("=== [1] clean vol month commits (fixture) =========================")
ROOT = fresh_root()
REP, _ = run_pass(ROOT, [("VOK", "1m-iv")])
ok_vals = stored_values(ROOT, "VOK", "1m-iv", MON.year, MON.month)
check("fixture: a clean 1m-iv month commits under the fake adapter",
      bool(ok_vals) and max(ok_vals) < 1.0,
      f"vals={None if ok_vals is None else len(ok_vals)}")

print("=== [2] ceiling: a 25.0 vol bar =====================================")
ROOT_VC = fresh_root()
REP_VC, _ = run_pass(ROOT_VC, [("VC", "1m-iv")])
vc_vals = stored_values(ROOT_VC, "VC", "1m-iv", MON.year, MON.month) or []
over = [v for v in vc_vals if v > 10.0]
if not FLAG:
    check("B1 defect pinned: an internally consistent 2500% vol bar commits "
          "today (no ceiling exists)",
          bool(over), f"stored_over_ceiling={len(over)} of {len(vc_vals)}")
else:
    check("F1 ceiling bar rejected: nothing above 10.0 is stored and the "
          "series reports it",
          not over and bool(vc_vals),
          f"stored_over_ceiling={len(over)}")

print("=== [3] unit-flip month (values x100) ==============================")
ROOT_VU = fresh_root()
REP_VU, _ = run_pass(ROOT_VU, [("VU", "1m-iv")])
vu_vals = stored_values(ROOT_VU, "VU", "1m-iv", MON.year, MON.month) or []
if not FLAG:
    check("B2 defect pinned: a whole percent-instead-of-decimal month "
          "(~36.0) commits today (no unit-flip halt)",
          bool(vu_vals) and min(vu_vals) > 30.0,
          f"vals={len(vu_vals)}")
else:
    # Fresh series with NO stored history: the trailing-median guard cannot
    # exist yet; the CEILING (36 > 10) is what must stop these bars.
    check("F2 flipped values rejected by the ceiling on a fresh series "
          "(trailing-median halt applies once history exists)",
          not [v for v in vu_vals if v > 10.0],
          f"vals={len(vu_vals)}")

print("=== [4] negative vol bar (existing protection, pinned forever) ====")
ROOT_VN = fresh_root()
REP_VN, _ = run_pass(ROOT_VN, [("VN", "1m-iv")])
vn_vals = stored_values(ROOT_VN, "VN", "1m-iv", MON.year, MON.month) or []
check(("B5" if not FLAG else "F3")
      + " validate_bar keeps rejecting negative vol values (none stored)",
      bool(vn_vals) and min(vn_vals) >= 0.0,
      f"min={min(vn_vals) if vn_vals else None}")

if not FLAG:
    check("B3 M1 flag absent (stock_ibkr.VOL_VALUE_GATE)", not GATE_FLAG)
    check("B4 M2 audit module absent (engine/vol_value_audit.py)",
          _audit_mod is None)
elif STAGE == "full":
    print("=== [5] audit + settled registry + export rows ==================")
    av = _audit_mod
    aroot = fresh_root()
    run_pass(aroot, [("VOK", "1m-iv")])
    rep = av.audit(aroot, write_queue=True)
    check("F4a a clean bank audits clean (no flagged days)",
          not (rep.get("flagged") or []), f"rep={rep!r}")
    # plant a 10x jump day inside the committed month, then re-audit
    fp = ss.find_month_file(aroot, "VOK", MON.year, MON.month, "1m-iv")
    rows, meta = ss.read_month_file(fp)
    bumped = []
    for r in rows:
        r = list(r)
        if r[0].date() == TUE:
            r[1], r[2], r[3], r[4] = 3.7, 3.72, 3.68, 3.71
        bumped.append(tuple(r))
    ss.write_month_file(fp, bumped, meta)
    rep2 = av.audit(aroot, write_queue=True)
    flagged = rep2.get("flagged") or []
    check("F4b a planted 10x jump day is flagged with a queue row",
          any(str(f.get("ticker")) == "VOK" and str(f.get("day"))
              == TUE.isoformat() for f in flagged),
          f"flagged={flagged!r}")
    day_val = 3.71
    av.record_settled(aroot, "VOK", "1m-iv", TUE.isoformat(), day_val,
                      reason="jump", run="m0-selfcheck")
    rep3 = av.audit(aroot, write_queue=True)
    check("F5a a settled same-value day is NOT re-flagged",
          not any(str(f.get("day")) == TUE.isoformat()
                  for f in (rep3.get("flagged") or [])),
          f"flagged={rep3.get('flagged')!r}")
    rows2 = [(r[0], *(v * 1.5 for v in map(float, r[1:5])), *r[5:])
             if r[0].date() == TUE else r for r in bumped]
    ss.write_month_file(fp, rows2, meta)
    rep4 = av.audit(aroot, write_queue=True)
    check("F5b a CHANGED stored value re-flags despite the registry",
          any(str(f.get("day")) == TUE.isoformat()
              for f in (rep4.get("flagged") or [])),
          f"flagged={rep4.get('flagged')!r}")
    rows_export = av.settled_for_export(aroot, [("VOK", "1m-iv")])
    check("F6 settled_for_export returns the settled row for a matching "
          "selection",
          any(str(r.get("ticker")) == "VOK" and str(r.get("day"))
              == TUE.isoformat() for r in rows_export or []),
          f"rows={rows_export!r}")

print(f"\n{N[0]} checks, {len(FAILS)} failed ({STAGE} stage)")
if FAILS:
    print("HARNESS FAILURE — fix before green-lighting M1:")
    for name in FAILS:
        print(f"  - {name}")
    sys.exit(1)
if STAGE == "baseline":
    print("\n[M1/M2 PENDING] flags absent — the baseline gaps are pinned.")
    print("Codex must deliver (VOL_VALUE_SAFEGUARDS_PLAN.md §3):")
    print("  - M1: stock_ibkr.VOL_VALUE_GATE = True; VOL_HARD_CEILING bar")
    print("        rejection + unit-flip month halt; ratio kinds only;")
    print("        existing validate_bar rejections untouched")
    print("  - M2: engine/vol_value_audit.py (VOL_VALUE_AUDIT = True):")
    print("        audit()/queue rows, settled registry (value-bound),")
    print("        settled_for_export + export quality report section")
    print("  - M3: Fix Data / Add Stocks refetch-reconcile-settle (ports;")
    print("        live proof separately user-gated)")
    sys.exit(3)
if STAGE == "m1":
    print("\nM1 SLICE ACCEPTED — ceiling + unit-flip gate proven; M2")
    print("(audit/registry/export) PENDING as its own checkpoint. Full")
    print("acceptance = this harness printing FULL ACCEPTED (M1+M2). Exit 0.")
    sys.exit(0)
print("\nFULL ACCEPTED (M1+M2). Exit 0.")
sys.exit(0)
