"""Self-tests for stock_ibkr.py (Tier 2 IBKR gap-fill).

Standalone, no test framework, NO network and NO ib_async needed: every
IBKR touch goes through a scripted FakeAdapter. Run:
    python stock_ibkr_selftest.py
"""

import atexit
import copy
import inspect
import sys
import re
import tempfile
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss     # noqa: E402
import operation_gate as og    # noqa: E402

_TEST_GATE_OLD_PATH = og.LOCK_PATH
_TEST_GATE_DIR = tempfile.TemporaryDirectory(
    prefix="ibkr_selftest_gate_", ignore_cleanup_errors=True)
_TEST_GATE_PATH = Path(_TEST_GATE_DIR.name) / ".market_data_operation.lock"
_TEST_GATE_RESTORED = False
og.LOCK_PATH = _TEST_GATE_PATH


def _restore_test_operation_gate():
    """Restore the process-global gate path and remove the suite-owned lock."""
    global _TEST_GATE_RESTORED
    if _TEST_GATE_RESTORED:
        return
    og.LOCK_PATH = _TEST_GATE_OLD_PATH
    _TEST_GATE_RESTORED = True
    _TEST_GATE_DIR.cleanup()


atexit.register(_restore_test_operation_gate)

import stock_ibkr as sk        # noqa: E402
_TEST_AUX_DIRECTORY = sk._auxiliary_directory
sk._auxiliary_directory = lambda evidence: (Path(evidence) if evidence is not None
    else Path(_TEST_GATE_DIR.name) / "auxiliary-evidence")
atexit.register(lambda: setattr(sk, "_auxiliary_directory", _TEST_AUX_DIRECTORY))
import stock_basis as sb       # noqa: E402
import live_spot_probe as lsp  # noqa: E402
import run_log                 # noqa: E402
import addstock_watchdog as aw  # noqa: E402

try:                           # UTF-8 console so check names with ⊘/⚠/… print
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:  # noqa: BLE001 — older/odd stdout: best-effort
    pass

FAILS = []
N = [0]
NY = ZoneInfo("America/New_York")


def check(name, cond, detail=""):
    N[0] += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


check("selftest redirects the operation gate before engine tests",
      og.LOCK_PATH == _TEST_GATE_PATH
      and og.LOCK_PATH != _TEST_GATE_OLD_PATH)


def fresh_root():
    return Path(tempfile.mkdtemp(prefix="ibkr_st_")) / ss.STORAGE_DIR_NAME


def vendor_bars(day, n, base, vol=1000, step_min=1):
    out = []
    t = datetime.combine(day, ss.RTH_FIRST)
    for i in range(n):
        p = base + i * 0.01
        out.append((t, p, p + 0.05, p - 0.05, p + 0.01, vol + i))
        t += timedelta(minutes=step_min)
    return out


def ib_stub(bar):
    """A vendor bar tuple -> an IBKR-style bar stub (aware-UTC dt,
    float volume) — what ib_async hands back with formatDate=2."""
    dt, o, h, lo, c, v = bar
    return SimpleNamespace(date=dt.replace(tzinfo=NY)
                           .astimezone(timezone.utc),
                           open=o, high=h, low=lo, close=c,
                           volume=float(v))


def seed_series(root, ticker, day, n, base, interval="1m", step_min=1):
    bars = vendor_bars(day, n, base, step_min=step_min)
    path = ss.month_file_path(root, ticker, day.year, day.month, interval)
    stats = ss.write_month_file(path, bars)
    tdir = Path(root) / ticker
    man = ss.load_manifest(tdir) or ss.new_manifest(ticker, ticker)
    ss.manifest_months(man, interval)[ss.month_key(day.year, day.month)] \
        = dict(stats, status="present", source="seed")
    ss.save_manifest(tdir, man)
    return bars


class FakeAdapter:
    def __init__(self, days, conid=111, account="DUFAKE", head=None,
                 fail_n=0, fail_forever=False, pace_n=0,
                 pace_forever=False):
        self.days = days                  # {date: [stub...]}
        self.conid, self._account = conid, account
        self.head = head
        self.fail_n, self.fail_forever = fail_n, fail_forever
        self.pace_n, self.pace_forever = pace_n, pace_forever
        self.fetch_calls, self.reconnects = [], 0
        self.qualify_calls = 0
        self.qualify_many_calls = 0
        self.port = 7497

    def account(self):
        return self._account

    def qualify(self, symbol):
        self.qualify_calls += 1
        return self.conid, SimpleNamespace(symbol=symbol)

    def contract_for(self, conid):
        return SimpleNamespace(symbol="?", conId=conid)

    def qualify_many(self, symbols, chunk=50, progress=None, cancel=None):
        # symbols starting with "X_" are treated as not-found
        self.qualify_many_calls += 1
        return {s: (None if s.startswith("X_") else self.conid)
                for s in symbols}

    def head_timestamp(self, contract, what_to_show="TRADES"):
        self.last_head_what = what_to_show
        return self.head

    def is_connected(self):
        return True

    def reconnect(self):
        self.reconnects += 1

    def fetch(self, contract, end_dt, duration, bar_size, what_to_show="TRADES"):
        self.fetch_calls.append((end_dt, duration, bar_size))
        self.last_what = what_to_show
        if self.pace_forever:
            raise sk.PacingViolation("pacing violation: never clears")
        if self.pace_n > 0:
            self.pace_n -= 1
            raise sk.PacingViolation("pacing violation: try later")
        if self.fail_forever:
            raise ConnectionError("permanently down")
        if self.fail_n > 0:
            self.fail_n -= 1
            raise ConnectionError("transient drop")
        end_d = end_dt.date()
        if duration.endswith("S"):       # intra-day window: whole day's
            return list(self.days.get(end_d, []))   # stubs (time-filtered
        n, unit = duration.split()       # downstream)
        span = {"D": 1, "W": 7, "M": 31}[unit] * int(n)
        start_d = end_d - timedelta(days=span - 1)
        out = []
        for d in sorted(self.days):      # IBKR-style: a span returns all
            if start_d <= d <= end_d:    # sessions in (end - duration, end]
                out.extend(self.days[d])
        return out

    def disconnect(self):
        pass


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def time(self):
        return self.t

    def sleep(self, seconds, cancel=None):
        self.t += seconds


def fast_pacer(clock):
    return sk.Pacer(time_fn=clock.time, sleep_fn=clock.sleep)


def _offline_gap_fill_body(*args, **kwargs):
    """Legacy fake-adapter body units, not public-root acceptance.

    These fixtures intentionally use synthetic weekday books and custom fake
    pacers. The owned-root/calendar/LiveIB contracts are instead driven by
    fetch_ibkr_integration_selftest and fetch_a2_ibkr_workflows_selftest.
    Retain their body coverage without weakening production admission or
    pretending a FakeAdapter consumes real LiveIB request capabilities.
    """
    assert sk.fib.current_worker() is None, "body unit must not borrow a worker"
    factory = kwargs["adapter_factory"]
    def fake_only():
        adapter = factory()
        assert not isinstance(adapter, (sk.LiveIB, sk.ReusableAdapter)), "fake adapter required"
        return adapter
    kwargs["adapter_factory"] = fake_only
    return sk.gap_fill.__wrapped__.__wrapped__(*args, **kwargs)


def run_fill(root, selections, adapter, today, cancel=None,
             progress=None, pipeline=False):
    sk.RECONNECT_BACKOFF_S = 0.0          # no real sleeping in tests
    clock = FakeClock()
    return _offline_gap_fill_body(root, selections, progress=progress,
                       cancel=cancel,
                       adapter_factory=lambda: adapter,
                       pacer=fast_pacer(clock), today=today,
                       pipeline=pipeline)


MON = date(2024, 6, 17)                   # Mon
TUE, WED, THU, FRI = (MON + timedelta(days=i) for i in (1, 2, 3, 4))

print("=== [1] pacing governor ============================================")
clock = FakeClock()
p = sk.Pacer(max_requests=5, window_s=600, min_gap_s=0.0,
             time_fn=clock.time, sleep_fn=clock.sleep)
for _ in range(5):
    p.wait_turn()
t_before = clock.time()
p.wait_turn()                              # 6th must wait out the window
check("window enforced: 6th request waits ~600s",
      clock.time() - t_before >= 599, f"waited {clock.time() - t_before}")
clock2 = FakeClock()
p2 = sk.Pacer(max_requests=100, window_s=600, min_gap_s=0.6,
              time_fn=clock2.time, sleep_fn=clock2.sleep)
p2.wait_turn()
t0 = clock2.time()
p2.wait_turn()
check("min-gap enforced", clock2.time() - t0 >= 0.6)
ev = threading.Event()
ev.set()
clock3 = FakeClock()
p3 = sk.Pacer(max_requests=1, window_s=600, min_gap_s=0,
              time_fn=clock3.time,
              sleep_fn=lambda s, c: (_ for _ in ()).throw(sk.Cancelled())
              if c is not None and c.is_set() else clock3.sleep(s))
p3.wait_turn()
try:
    p3.wait_turn(cancel=ev)
    check("cancel honored during pacing wait", False)
except sk.Cancelled:
    check("cancel honored during pacing wait", True)

# Pause: _wait_while_paused blocks while a SET pause Event is held and proceeds
# when cleared; cancel while paused still raises. Pause now engages at MONTH
# boundaries / between series via this helper — NOT in the Pacer's wait_turn
# (so a pause never leaves a partial month). Real threads + real clock.
import time as _rt
pause_ev = threading.Event(); pause_ev.set()
done = []
def _paused_run():
    sk._wait_while_paused(pause_ev, None)
    done.append("ok")
th = threading.Thread(target=_paused_run); th.start()
_rt.sleep(0.4)
check("pause: _wait_while_paused BLOCKS while the pause Event is set", not done)
pause_ev.clear()
th.join(timeout=2)
check("resume: clearing pause lets it proceed", done == ["ok"])
# the Pacer's wait_turn must NOT block on pause any more (moved off the request
# path so the current month can finish before idling).
pp = sk.Pacer(max_requests=100, window_s=600, min_gap_s=0.0)
pp._pause = threading.Event(); pp._pause.set()
pacer_done = []
def _pacer_run():
    pp.wait_turn(metered=False); pacer_done.append("ok")
thp = threading.Thread(target=_pacer_run); thp.start(); thp.join(timeout=2)
check("pause: Pacer.wait_turn does NOT block on pause (moved to month boundary)",
      pacer_done == ["ok"])
# cancel while paused -> Cancelled (the pause loop honors cancel)
pause_ev.set()
cev = threading.Event()
done2 = []
def _paused_cancel():
    try:
        sk._wait_while_paused(pause_ev, cev)
        done2.append("ok")
    except sk.Cancelled:
        done2.append("cancelled")
th2 = threading.Thread(target=_paused_cancel); th2.start()
_rt.sleep(0.3)
cev.set()
th2.join(timeout=2)
check("pause: cancel WHILE paused raises Cancelled", done2 == ["cancelled"])
pause_ev.clear()

# interval-aware pacing (measured live: 1m+ bypasses HMDS, 1s is capped).
# burst_max=0 isolates the metered-WINDOW behavior here; the 6-per-2s burst
# rule (which DOES apply to non-metered too) is tested separately below.
clkm = FakeClock()
pm = sk.Pacer(max_requests=3, window_s=600, min_gap_s=0.0, burst_max=0,
              time_fn=clkm.time, sleep_fn=clkm.sleep)
t = clkm.time()
for _ in range(12):
    pm.wait_turn(metered=False)        # minute+ : cache-served, no cap
check("interval-aware: non-metered (1m+) never waits out the window",
      clkm.time() - t == 0, f"waited {clkm.time() - t}")

# 6-per-2s same-contract BURST rule (applies metered AND non-metered)
clkb = FakeClock()
pb = sk.Pacer(max_requests=1000, window_s=600, min_gap_s=0.0,
              burst_max=6, burst_window_s=2.0,
              time_fn=clkb.time, sleep_fn=clkb.sleep)
tb = clkb.time()
for _ in range(6):
    pb.wait_turn(metered=False)        # a 6-request burst: no throttle
check("burst: 6 requests fit the 2s window unthrottled",
      clkb.time() - tb == 0, f"waited {clkb.time() - tb}")
t7 = clkb.time()
pb.wait_turn(metered=False)            # the 7th must wait the window out
check("burst: 7th request waits out the 2s window",
      clkb.time() - t7 >= 2.0, f"waited {clkb.time() - t7}")
# sustained: 30 requests at 6-per-2s -> >= 4 windows past the free first 6
clkb2 = FakeClock()
pb2 = sk.Pacer(max_requests=1000, window_s=600, min_gap_s=0.0,
               burst_max=6, burst_window_s=2.0,
               time_fn=clkb2.time, sleep_fn=clkb2.sleep)
t30 = clkb2.time()
for _ in range(30):
    pb2.wait_turn(metered=False)
check("burst: sustained 30 requests respect 6 per 2s",
      clkb2.time() - t30 >= (30 - 6) / 6 * 2.0 - 0.5,
      f"elapsed {clkb2.time() - t30}")
# never more than 6 in ANY 2s window (the actual IBKR rule)
clkb3 = FakeClock()
seen = []
pb3 = sk.Pacer(max_requests=1000, window_s=600, min_gap_s=0.0,
               burst_max=6, burst_window_s=2.0,
               time_fn=clkb3.time, sleep_fn=clkb3.sleep)
for _ in range(20):
    pb3.wait_turn(metered=False)
    seen.append(clkb3.time())
worst = max(sum(1 for s in seen if t - 2.0 < s <= t) for t in seen)
check("burst: never >6 same-contract requests in any 2s window", worst <= 6,
      f"worst window held {worst}")
clks = FakeClock()
ps = sk.Pacer(max_requests=3, window_s=600, min_gap_s=0.0,
              time_fn=clks.time, sleep_fn=clks.sleep)
for _ in range(3):
    ps.wait_turn(metered=True)
t = clks.time()
ps.wait_turn(metered=True)             # 4th sub-minute waits out window
check("interval-aware: metered (sub-minute) IS window-capped",
      clks.time() - t >= 599, f"waited {clks.time() - t}")
pm.saturate()                          # a pacing violation forces metering
t = clkm.time()
pm.wait_turn(metered=False)            # now forced-metered -> must wait
check("interval-aware: a pacing violation forces metering on 1m+ too",
      clkm.time() - t > 0, f"waited {clkm.time() - t}")
clkg = FakeClock()                     # burst min-gap applies to BOTH
pg = sk.Pacer(max_requests=100, window_s=600, min_gap_s=0.6,
              time_fn=clkg.time, sleep_fn=clkg.sleep)
pg.wait_turn(metered=False)
t = clkg.time()
pg.wait_turn(metered=False)
check("interval-aware: burst min-gap still applies to non-metered",
      clkg.time() - t >= 0.6, f"waited {clkg.time() - t}")

print("=== [2] gap planning ===============================================")
check("trading days skip the weekend",
      sk.trading_days(date(2024, 6, 14), date(2024, 6, 18))
      == [date(2024, 6, 14), date(2024, 6, 17), date(2024, 6, 18)])
check("1m day = one request",
      sk.day_requests("1m", MON) == [(datetime(2024, 6, 17, 16, 0),
                                      "1 D")])
reqs_1s = sk.day_requests("1s", MON)
check("1s day = 13 windows of 1800s",
      len(reqs_1s) == 13 and all(d == "1800 S" for _e, d in reqs_1s)
      and reqs_1s[0][0] == datetime(2024, 6, 17, 10, 0)
      and reqs_1s[-1][0] == datetime(2024, 6, 17, 16, 0), str(reqs_1s[:2]))
root = fresh_root()
seed_series(root, "TTT", MON, 60, 100.0)
plan = sk.plan_gap(root, "TTT", "1m", today=FRI)
check("plan starts by REFETCHING the last stored session",
      plan["days"][0] == MON and plan["days"][-1] == FRI
      and len(plan["days"]) == 5, str(plan["days"]))
check("plan est_requests: MON..FRI is ONE span",
      plan["est_requests"] == 1, str(plan["est_requests"]))
old = fresh_root()
seed_series(old, "OLD", date(2023, 1, 16), 60, 10.0, interval="1s")
plan_old = sk.plan_gap(old, "OLD", "1s", today=date(2024, 6, 21))
check("1s gap older than ~6 months is clipped + flagged",
      plan_old["clipped_1s"]
      and plan_old["days"][0] >= date(2023, 12, 26), str(plan_old["days"][:1]))
check("unfetchable interval -> error",
      sk.plan_gap(root, "TTT", "4h", today=FRI).get("error"))

print("=== [3] bar conversion =============================================")
cnt = {"non_rth": 0, "invalid": 0, "outside_day": 0}
stubs = [ib_stub(b) for b in vendor_bars(MON, 5, 50.0)]
stubs.append(SimpleNamespace(                      # 16:00 bar -> non-RTH
    date=datetime.combine(MON, sk.RTH_CLOSE, tzinfo=NY)
    .astimezone(timezone.utc), open=1, high=1, low=1, close=1,
    volume=5.0))
stubs.append(SimpleNamespace(                      # previous day leak
    date=datetime(2024, 6, 14, 9, 30, tzinfo=NY).astimezone(timezone.utc),
    open=1, high=1, low=1, close=1, volume=5.0))
stubs.append(SimpleNamespace(            # fractional (adjusted) vol -> ROUNDED, kept
    date=datetime(2024, 6, 17, 10, 0, tzinfo=NY).astimezone(timezone.utc),
    open=1, high=1, low=1, close=1, volume=10.4))
stubs.append(SimpleNamespace(                      # negative volume
    date=datetime(2024, 6, 17, 10, 1, tzinfo=NY).astimezone(timezone.utc),
    open=1, high=1, low=1, close=1, volume=-1.0))
stubs.append(SimpleNamespace(                      # impossible bar
    date=datetime(2024, 6, 17, 10, 2, tzinfo=NY).astimezone(timezone.utc),
    open=5, high=4, low=6, close=5, volume=10.0))
out = sk.convert_bars(stubs, MON, cnt)
check("UTC->NY conversion + filters; fractional (adjusted) vol rounded + kept",
      len(out) == 6 and out[0][0] == datetime(2024, 6, 17, 9, 30)
      and out[5] == (datetime(2024, 6, 17, 10, 0), 1.0, 1.0, 1.0, 1.0, 10)
      and cnt == {"non_rth": 1, "invalid": 2, "outside_day": 1},
      f"{len(out)} {cnt}")
jan = date(2024, 1, 15)                            # EST: 14:30Z = 9:30
est_stub = SimpleNamespace(date=datetime(2024, 1, 15, 14, 30,
                                         tzinfo=timezone.utc),
                           open=1, high=1.1, low=0.9, close=1,
                           volume=7.0)
cnt2 = {"non_rth": 0, "invalid": 0, "outside_day": 0}
out2 = sk.convert_bars([est_stub], jan, cnt2)
check("EST winter offset handled (14:30Z = 9:30 NY)",
      len(out2) == 1 and out2[0][0] == datetime(2024, 1, 15, 9, 30))

print("=== [4] happy-path fill ============================================")
root4 = fresh_root()
seeded = seed_series(root4, "AAA", MON, 390, 100.0)
days = {MON: [ib_stub(b) for b in seeded],            # identical refetch
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 390, 101.0)],
        WED: [],                                       # "holiday"
        THU: [ib_stub(b) for b in vendor_bars(THU, 390, 102.0)],
        FRI: [ib_stub(b) for b in vendor_bars(FRI, 390, 103.0)]}
fake = FakeAdapter(days)
rep = run_fill(root4, [("AAA", "1m")], fake, today=FRI)
r = rep["series"][0]
check("happy path: 3 new sessions added, overlap all dups",
      r["added"] == 3 * 390 and r["dup_existing"] == 390
      and r["conflicts"] == 0 and not r.get("halt"), str(r)[:300])
check("happy path: 1 span only — qualify moved to the batch pre-pass "
      "(A2), so NO per-series qualify (was 5 daily fetches)",
      r["requests"] == 1 and len(fake.fetch_calls) == 1
      and fake.qualify_calls == 0 and fake.qualify_many_calls == 1,
      f"req={r['requests']} fetches={len(fake.fetch_calls)} "
      f"qualify={fake.qualify_calls} qualify_many={fake.qualify_many_calls}")
check("happy path: holiday day tolerated", True)   # no halt above
rb, _ = ss.read_month_file(ss.month_file_path(root4, "AAA", 2024, 6,
                                              "1m"))
check("happy path: month now holds 4 sessions",
      len(rb) == 4 * 390)
man = ss.load_manifest(root4 / "AAA")
check("conId pinned on first contact", man.get("conid") == 111,
      str(man.get("conid")))
src = man["intervals"]["1m"]["months"]["2024-06"]["source"]
check("provenance records the IBKR contribution",
      any(c.get("file") == "IBKR:DUFAKE"
          for c in src.get("contributions", [])), str(src))
check("volume note present (shares)",
      any("volume units" in n for n in r.get("notes", [])),
      str(r.get("notes")))
check("totals + report saved",
      rep["totals"]["added"] == 1170 and rep.get("report_path")
      and Path(rep["report_path"]).is_file())
lines = sk.summarize_report(rep)
check("summary renders", lines and any(line.startswith("IBKR ") for line in lines)
      and any("AAA 1m" in ln for ln in lines), str(lines[:2]))

# B1: summarize_report surfaces the silent-drop reject counters. It is
# pure, so feed it a hand-built report dict. Series ZZZ trips the >1%
# invalid:fetched ratio (5 of 100 = 5%); series QQQ has benign counters.
crep = {"run": "gap_fill", "account": "DU1", "port": 4002,
        "totals": {"added": 600, "written": 1, "dup_existing": 0,
                   "conflicts": 0, "requests": 2, "halted_series": 0},
        "series": [
            {"ticker": "ZZZ", "interval": "1m", "added": 100, "written": 1,
             "requests": 1, "bars_fetched": 100, "blocked_months": [],
             "counters": {"non_rth": 7, "invalid": 5, "outside_day": 2},
             "notes": []},
            {"ticker": "QQQ", "interval": "1m", "added": 500, "written": 0,
             "requests": 1, "bars_fetched": 5000, "blocked_months": [],
             "counters": {"non_rth": 3, "invalid": 0, "outside_day": 0},
             "notes": []}]}
clines = sk.summarize_report(crep)
head = clines[0]
check("B1: run line surfaces aggregated reject counts",
      "dropped 5 invalid" in head and "10 non-RTH" in head
      and "2 outside-day" in head, head)
check("B1: per-series reject line shows the counts",
      any("ZZZ 1m" in ln and "dropped 5 invalid" in ln
          and "7 non-RTH" in ln and "2 outside-day" in ln
          and "100 fetched" in ln for ln in clines),
      str(clines))
check("B1: >1% invalid:fetched trips the LOUDER warning",
      any("WARNING" in ln and "feed-leak" in ln and "5.0%" in ln
          for ln in clines), str(clines))
check("B1: benign series (0 invalid, 3 non-RTH) still gets a terse line, "
      "no warning marker",
      any("QQQ 1m" in ln and "dropped 0 invalid" in ln
          and "3 non-RTH" in ln for ln in clines)
      and not any("QQQ" in ln and "!!" in ln for ln in clines),
      str(clines))
# all-zero counters: no extra reject noise on the run line or per-series.
zrep = {"run": "gap_fill", "account": "DU1", "port": 4002,
        "totals": {"added": 10, "written": 1, "dup_existing": 0,
                   "conflicts": 0, "requests": 1, "halted_series": 0},
        "series": [
            {"ticker": "OK", "interval": "1m", "added": 10, "written": 1,
             "requests": 1, "bars_fetched": 10, "blocked_months": [],
             "counters": {"non_rth": 0, "invalid": 0, "outside_day": 0},
             "notes": []}]}
zlines = sk.summarize_report(zrep)
check("B1: all-zero counters add NO reject text or warning",
      not any("dropped" in ln or "WARNING" in ln for ln in zlines),
      str(zlines))

# idempotent re-run
fake2 = FakeAdapter(days)
rep2 = run_fill(root4, [("AAA", "1m")], fake2, today=FRI)
r2 = rep2["series"][0]
check("re-run: only the last session refetched, all dups, no writes "
      "(A1: pinned -> qualify skipped, 1 request)",
      r2["added"] == 0 and r2["written"] == 0
      and r2["dup_existing"] == 390 and r2["requests"] == 1,
      str({k: r2[k] for k in ("added", "dup_existing", "requests")}))

print("=== [5] conId: A1 pin trust, A2 pre-pass pin, divergence detect =====")
root5 = fresh_root()
seeded = seed_series(root5, "BBB", MON, 60, 10.0)
man = ss.load_manifest(root5 / "BBB")
man["conid"] = 555                                   # already pinned
ss.save_manifest(root5 / "BBB", man)
fake = FakeAdapter({MON: [ib_stub(b) for b in seeded],
                    TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]},
                   conid=555)                        # pre-pass agrees
rep = run_fill(root5, [("BBB", "1m")], fake, today=TUE)
r = rep["series"][0]
check("A1: pinned conId trusted -> qualify SKIPPED, no divergence, fetch "
      "proceeds",
      fake.qualify_calls == 0 and fake.qualify_many_calls == 1
      and r["added"] == 60 and not r.get("halt") and r["requests"] == 1
      and not any("DIVERGENCE" in n for n in r.get("notes", [])),
      f"qualify_calls={fake.qualify_calls} req={r.get('requests')} "
      f"added={r.get('added')} halt={r.get('halt')}")

# A2: a fresh (unpinned) series is pinned STRAIGHT from the batch pre-pass
# — no per-series qualify — and the conId persists for next time.
root5b = fresh_root()
seeded = seed_series(root5b, "CCC", MON, 60, 10.0)    # conid=None
fake2 = FakeAdapter({MON: [ib_stub(b) for b in seeded],
                     TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]},
                    conid=222)
run_fill(root5b, [("CCC", "1m")], fake2, today=TUE)
check("A2: first contact pinned by the pre-pass -> NO per-series qualify, "
      "conId persisted",
      fake2.qualify_calls == 0 and fake2.qualify_many_calls == 1
      and ss.load_manifest(root5b / "CCC").get("conid") == 222,
      f"qualify_calls={fake2.qualify_calls} "
      f"conid={ss.load_manifest(root5b / 'CCC').get('conid')}")

# dedup: a caller-supplied conId map (the GUI's validation already resolved
# these) makes gap_fill SKIP its own pre-pass -> no SECOND qualify_many pass.
root5e = fresh_root()
seeded = seed_series(root5e, "PRE", MON, 60, 10.0)
fake_pre = FakeAdapter({MON: [ib_stub(b) for b in seeded],
                        TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]},
                       conid=333)
_offline_gap_fill_body(root5e, [("PRE", "1m")], adapter_factory=lambda: fake_pre,
            pacer=fast_pacer(FakeClock()), today=TUE, resolved={"PRE": 333})
check("dedup: a pre-resolved map skips gap_fill's pre-pass (no 2nd "
      "qualify_many) yet still pins from it",
      fake_pre.qualify_many_calls == 0 and fake_pre.qualify_calls == 0
      and ss.load_manifest(root5e / "PRE").get("conid") == 333,
      f"qualify_many={fake_pre.qualify_many_calls} "
      f"qualify={fake_pre.qualify_calls}")

# detection: the pre-pass re-resolves a PINNED ticker to a DIFFERENT conId
# (symbol reused / relisted) -> WARN, keep the pin, fetch still proceeds.
root5c = fresh_root()
seeded = seed_series(root5c, "DUP", MON, 60, 10.0)
man = ss.load_manifest(root5c / "DUP")
man["conid"] = 555                                   # pinned to original
ss.save_manifest(root5c / "DUP", man)
fake3 = FakeAdapter({MON: [ib_stub(b) for b in seeded],
                     TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]},
                    conid=999)                        # IBKR now says 999
rep = run_fill(root5c, [("DUP", "1m")], fake3, today=TUE)
r = rep["series"][0]
note = next((n for n in r.get("notes", []) if "DIVERGENCE" in n), "")
check("detection: conId divergence WARNS, no halt, keeps the pinned "
      "contract, fetch proceeds, pin unchanged",
      fake3.qualify_calls == 0 and not r.get("halt") and r["added"] == 60
      and "999" in note and "555" in note
      and ss.load_manifest(root5c / "DUP").get("conid") == 555,
      f"note={note!r} halt={r.get('halt')} added={r.get('added')}")


class DeadPinnedAdapter(FakeAdapter):
    """Pinned conId is dead, but the pre-pass fresh conId serves bars."""
    def __init__(self, days, dead=555, conid=999):
        super().__init__(days, conid=conid)
        self.dead = dead
        self.contract_fetch_conids = []

    def contract_for(self, conid):
        return SimpleNamespace(symbol="XOM", conId=int(conid))

    def fetch(self, contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        cid = getattr(contract, "conId", None)
        self.contract_fetch_conids.append(cid)
        if int(cid) == int(self.dead):
            raise sk.SeriesHalt(
                "contract rejected: No security definition has been found "
                "for the request")
        return super().fetch(contract, end_dt, duration, bar_size,
                             what_to_show)


root5r = fresh_root()
seeded = seed_series(root5r, "XOM", MON, 60, 10.0)
man = ss.load_manifest(root5r / "XOM")
man["conid"] = 555
ss.save_manifest(root5r / "XOM", man)
fake_r = DeadPinnedAdapter(
    {MON: [ib_stub(b) for b in seeded],
     TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]})
rep = run_fill(root5r, [("XOM", "1m")], fake_r, today=TUE)
r = rep["series"][0]
notes = " ".join(r.get("notes", []))
check("P2: dead pinned conId err-200 retries once with fresh conId and "
      "repins after the merge gate accepts it",
      not r.get("halt") and r["added"] == 60
      and fake_r.contract_fetch_conids[:1] == [555]
      and 999 in fake_r.contract_fetch_conids
      and "DIVERGENCE" in notes and "retrying once" in notes
      and ss.load_manifest(root5r / "XOM").get("conid") == 999,
      f"halt={r.get('halt')} calls={fake_r.contract_fetch_conids} "
      f"conid={ss.load_manifest(root5r / 'XOM').get('conid')} "
      f"notes={r.get('notes')}")

# The next run must trust the durable replacement directly: no repeated dead
# request and no second repin transition.
fake_rerun = DeadPinnedAdapter(
    {MON: [ib_stub(b) for b in seeded],
     TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]})
rep = run_fill(root5r, [("XOM", "1m")], fake_rerun, today=TUE)
r = rep["series"][0]
check("P2: durable repin rerun fetches accepted conId directly",
      not r.get("halt") and fake_rerun.contract_fetch_conids == [999]
      and ss.load_manifest(root5r / "XOM").get("conid") == 999
      and not any("retrying once" in n for n in r.get("notes", [])),
      f"halt={r.get('halt')} calls={fake_rerun.contract_fetch_conids} "
      f"notes={r.get('notes')}")

# Positive evidence may be duplicate-only: the overlap gate still compared
# real nonempty bars, while no month bytes needed to change.
root5rdup = fresh_root()
seeded_dup = seed_series(root5rdup, "XDUP", MON, 60, 10.0)
man = ss.load_manifest(root5rdup / "XDUP")
man["conid"] = 555
ss.save_manifest(root5rdup / "XDUP", man)
fake_rdup = DeadPinnedAdapter(
    {MON: [ib_stub(b) for b in seeded_dup]})
rep = run_fill(root5rdup, [("XDUP", "1m")], fake_rdup, today=MON)
r = rep["series"][0]
check("P2: accepted duplicate-only continuity still publishes 555->999",
      not r.get("halt") and r["added"] == 0 and r["written"] == 0
      and r["dup_existing"] == 60
      and ss.load_manifest(root5rdup / "XDUP").get("conid") == 999,
      f"halt={r.get('halt')} added={r.get('added')} "
      f"dups={r.get('dup_existing')} "
      f"conid={ss.load_manifest(root5rdup / 'XDUP').get('conid')}")

root5rpipe = fresh_root()
pipe_seed = seed_series(root5rpipe, "XPIPE", MON, 60, 10.0)
man = ss.load_manifest(root5rpipe / "XPIPE")
man["conid"] = 555
ss.save_manifest(root5rpipe / "XPIPE", man)
fake_rpipe = DeadPinnedAdapter(
    {MON: [ib_stub(b) for b in pipe_seed],
     TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]})
rep = run_fill(
    root5rpipe, [("XPIPE", "1m")], fake_rpipe, today=TUE,
    pipeline=True)
r = rep["series"][0]
check("P2: pipelined gate activates and publishes the same repin CAS",
      not r.get("halt") and r["added"] == 60
      and ss.load_manifest(root5rpipe / "XPIPE").get("conid") == 999,
      f"halt={r.get('halt')} added={r.get('added')} "
      f"conid={ss.load_manifest(root5rpipe / 'XPIPE').get('conid')}")

root5rnone = fresh_root()
seed_series(root5rnone, "XNONE", MON, 60, 10.0)
man = ss.load_manifest(root5rnone / "XNONE")
man["conid"] = 555
ss.save_manifest(root5rnone / "XNONE", man)
fake_rnone = DeadPinnedAdapter({})
rep = run_fill(root5rnone, [("XNONE", "1m")], fake_rnone, today=MON)
r = rep["series"][0]
check("P2: empty fresh response cannot activate or publish repin",
      r.get("halt") and "supplied no nonempty bars" in r["halt"]
      and fake_rnone.contract_fetch_conids == [555, 999]
      and ss.load_manifest(root5rnone / "XNONE").get("conid") == 555,
      f"halt={r.get('halt')} calls={fake_rnone.contract_fetch_conids}")

# A fresh contract that cannot prove continuity never changes the pin or
# merges its bars. Cover both full-overlap rejection and the thin join branch.
root5rbad = fresh_root()
seed_series(root5rbad, "XBAD", MON, 60, 10.0)
man = ss.load_manifest(root5rbad / "XBAD")
man["conid"] = 555
ss.save_manifest(root5rbad / "XBAD", man)
fake_rbad = DeadPinnedAdapter(
    {MON: [ib_stub(b) for b in vendor_bars(MON, 60, 100.0)]})
rep = run_fill(root5rbad, [("XBAD", "1m")], fake_rbad, today=MON)
r = rep["series"][0]
check("P2: fresh-contract overlap rejection leaves old pin and bytes",
      r.get("halt") and "OVERLAP GATE" in r["halt"]
      and r["added"] == 0 and r["written"] == 0
      and ss.load_manifest(root5rbad / "XBAD").get("conid") == 555,
      f"halt={r.get('halt')} conid="
      f"{ss.load_manifest(root5rbad / 'XBAD').get('conid')}")

root5rthin = fresh_root()
seed_series(root5rthin, "XTHIN", MON, 1, 10.0)
man = ss.load_manifest(root5rthin / "XTHIN")
man["conid"] = 555
ss.save_manifest(root5rthin / "XTHIN", man)
fake_rthin = DeadPinnedAdapter(
    {MON: [ib_stub(b) for b in vendor_bars(MON, 1, 100.0)]})
rep = run_fill(root5rthin, [("XTHIN", "1m")], fake_rthin, today=MON)
r = rep["series"][0]
check("P2: fresh-contract thin join rejection leaves old pin and bytes",
      r.get("halt") and "ENTRY GATE" in r["halt"]
      and "JOIN GATE" in r["halt"] and r["added"] == 0
      and ss.load_manifest(root5rthin / "XTHIN").get("conid") == 555,
      f"halt={r.get('halt')} conid="
      f"{ss.load_manifest(root5rthin / 'XTHIN').get('conid')}")

# Empty and ratio series have no price-continuity gate. The old contract may
# be tried, but the divergent fresh contract is not fetched at all.
root5rempty = fresh_root()
empty_manifest = ss.new_manifest("XEMPTY", "XEMPTY")
empty_manifest["conid"] = 555
ss.save_manifest(root5rempty / "XEMPTY", empty_manifest)
fake_rempty = DeadPinnedAdapter(
    {MON: [ib_stub(b) for b in vendor_bars(MON, 60, 10.0)]})
fake_rempty.head = datetime(MON.year, MON.month, MON.day, tzinfo=NY)
rep = run_fill(root5rempty, [("XEMPTY", "1m")], fake_rempty, today=MON)
r = rep["series"][0]
check("P2: empty series halts before fetching unproved fresh contract",
      r.get("halt") and "positive continuity evidence" in r["halt"]
      and fake_rempty.contract_fetch_conids
      and set(fake_rempty.contract_fetch_conids) == {555}
      and ss.load_manifest(root5rempty / "XEMPTY").get("conid") == 555,
      f"halt={r.get('halt')} calls={fake_rempty.contract_fetch_conids}")

root5rratio = fresh_root()
ratio_seed = seed_series(
    root5rratio, "XRATIO", MON, 60, 0.40, interval="1m-iv")
man = ss.load_manifest(root5rratio / "XRATIO")
man["conid"] = 555
ss.save_manifest(root5rratio / "XRATIO", man)
fake_rratio = DeadPinnedAdapter(
    {MON: [ib_stub(b) for b in ratio_seed]})
rep = run_fill(root5rratio, [("XRATIO", "1m-iv")], fake_rratio, today=MON)
r = rep["series"][0]
check("P2: ratio series halts before fetching unproved fresh contract",
      r.get("halt") and "positive continuity evidence" in r["halt"]
      and fake_rratio.contract_fetch_conids == [555]
      and ss.load_manifest(root5rratio / "XRATIO").get("conid") == 555,
      f"halt={r.get('halt')} calls={fake_rratio.contract_fetch_conids}")


class NonContractPinnedAdapter(DeadPinnedAdapter):
    def fetch(self, contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        cid = getattr(contract, "conId", None)
        self.contract_fetch_conids.append(cid)
        if int(cid) == int(self.dead):
            raise sk.SeriesHalt("no market data permission for pinned contract")
        return FakeAdapter.fetch(
            self, contract, end_dt, duration, bar_size, what_to_show)


root5rnon = fresh_root()
seed_series(root5rnon, "XNON", MON, 60, 10.0)
man = ss.load_manifest(root5rnon / "XNON")
man["conid"] = 555
ss.save_manifest(root5rnon / "XNON", man)
fake_rnon = NonContractPinnedAdapter({})
rep = run_fill(root5rnon, [("XNON", "1m")], fake_rnon, today=MON)
r = rep["series"][0]
check("P2: non-contract halt never creates a repin retry",
      r.get("halt") and "permission" in r["halt"]
      and fake_rnon.contract_fetch_conids == [555]
      and ss.load_manifest(root5rnon / "XNON").get("conid") == 555,
      f"halt={r.get('halt')} calls={fake_rnon.contract_fetch_conids}")


class PartialOldPinnedAdapter(DeadPinnedAdapter):
    def __init__(self, days, dead=555, conid=999):
        super().__init__(days, dead=dead, conid=conid)
        self.old_fetches = 0

    def fetch(self, contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        cid = getattr(contract, "conId", None)
        self.contract_fetch_conids.append(cid)
        if int(cid) == int(self.dead):
            self.old_fetches += 1
            if self.old_fetches > 1:
                raise sk.SeriesHalt("contract rejected after partial old work")
        return FakeAdapter.fetch(
            self, contract, end_dt, duration, bar_size, what_to_show)


root5rpartial = fresh_root()
partial_seed = seed_series(root5rpartial, "XPART", MON, 60, 10.0)
man = ss.load_manifest(root5rpartial / "XPART")
man["conid"] = 555
ss.save_manifest(root5rpartial / "XPART", man)
JUL1 = date(2024, 7, 1)
fake_rpartial = PartialOldPinnedAdapter(
    {MON: [ib_stub(b) for b in partial_seed],
     MON + timedelta(days=7):
         [ib_stub(b) for b in vendor_bars(MON + timedelta(days=7), 60, 10.6)]})
rep = run_fill(
    root5rpartial, [("XPART", "1m")], fake_rpartial, today=JUL1)
r = rep["series"][0]
check("P2: old-contract partial work forbids switching to fresh conId",
      r.get("halt") and "contract rejected after partial old work" in r["halt"]
      and r["bars_fetched"] > 0
      and 999 not in fake_rpartial.contract_fetch_conids
      and ss.load_manifest(root5rpartial / "XPART").get("conid") == 555,
      f"halt={r.get('halt')} fetched={r.get('bars_fetched')} "
      f"calls={fake_rpartial.contract_fetch_conids}")


class StillDeadPinnedAdapter(DeadPinnedAdapter):
    def fetch(self, contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        cid = getattr(contract, "conId", None)
        self.contract_fetch_conids.append(cid)
        raise sk.SeriesHalt(
            "contract rejected: No security definition has been found "
            "for the request")


root5rf = fresh_root()
seeded = seed_series(root5rf, "XOM", MON, 60, 10.0)
man = ss.load_manifest(root5rf / "XOM")
man["conid"] = 555
ss.save_manifest(root5rf / "XOM", man)
fake_rf = StillDeadPinnedAdapter(
    {MON: [ib_stub(b) for b in seeded],
     TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]})
rep = run_fill(root5rf, [("XOM", "1m")], fake_rf, today=TUE)
r = rep["series"][0]
check("P2: if fresh conId retry still fails, halt text carries the "
      "divergence context",
      r.get("halt") and "after retrying current conId 999" in r["halt"]
      and "conId DIVERGENCE" in r["halt"]
      and fake_rf.contract_fetch_conids == [555, 999],
      f"halt={r.get('halt')}")

# detection: a PINNED symbol the pre-pass can no longer resolve AT ALL
# (delisted / renamed -> qualify_many returns None) still fetches by the
# pinned conId, with a breadcrumb note.
class GoneSymbolAdapter(FakeAdapter):
    def qualify_many(self, symbols, chunk=50, progress=None, cancel=None):
        self.qualify_many_calls += 1
        return {s: None for s in symbols}      # IBKR resolves nothing


root5d = fresh_root()
seeded = seed_series(root5d, "GONE", MON, 60, 10.0)
man = ss.load_manifest(root5d / "GONE")
man["conid"] = 555
ss.save_manifest(root5d / "GONE", man)
fake4 = GoneSymbolAdapter(
    {MON: [ib_stub(b) for b in seeded],
     TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]}, conid=555)
rep = run_fill(root5d, [("GONE", "1m")], fake4, today=TUE)
r = rep["series"][0]
check("detection: pinned symbol no longer resolves -> breadcrumb, still "
      "fetches by the pin (no halt)",
      not r.get("halt") and r["added"] == 60 and fake4.qualify_calls == 0
      and any("no longer resolves" in n for n in r.get("notes", [])),
      f"halt={r.get('halt')} notes={r.get('notes')}")


class NotFoundAdapter(FakeAdapter):
    """A delisted/not-found symbol: the batch pre-pass resolves it to
    None, so the per-series path qualify()s it and that raises a GENERIC
    (non-SeriesHalt) error — exactly like the live adapter."""
    def qualify(self, symbol):
        if symbol == "DELISTED":
            raise RuntimeError("no SMART/USD stock 'DELISTED' (delisted?)")
        return super().qualify(symbol)

    def qualify_many(self, symbols, chunk=50, progress=None, cancel=None):
        out = super().qualify_many(symbols, chunk, progress, cancel)
        out["DELISTED"] = None                # pre-pass can't resolve it
        return out


rootnf = fresh_root()
seeded = seed_series(rootnf, "GOODA", MON, 60, 10.0)
days = {MON: [ib_stub(b) for b in seeded],
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]}
rep = run_fill(rootnf, [("DELISTED", "1m"), ("GOODA", "1m")],
               NotFoundAdapter(days), today=TUE)
byt = {s["ticker"]: s for s in rep["series"]}
check("not-found stock is SKIPPED; the batch CONTINUES to the others",
      len(rep["series"]) == 2
      and "series failed" in (byt["DELISTED"].get("halt") or "")
      and byt["GOODA"].get("added") == 60
      and not byt["GOODA"].get("halt"),
      str([(s["ticker"], s.get("halt") or f"+{s.get('added')}")
           for s in rep["series"]]))

# Pure-body legacy validation checks with FakeAdapter only. Standalone public
# admission is held until A2-2b; the actual refusal and the owned nightly
# preflight are tested in fetch_a2_ibkr_workflows_selftest. No LiveIB path or
# test-policy activation is exercised by these three body-only unit fixtures.
v = sk._validate_symbols_body.__wrapped__(["AAA", "X_GONE", "BBB", "X_DEAD", "CCC"],
                        adapter_factory=lambda: FakeAdapter({}))
check("validate_symbols: splits found vs not_found, order preserved",
      v["found"] == ["AAA", "BBB", "CCC"]
      and v["not_found"] == ["X_GONE", "X_DEAD"]
      and v["error"] is None, str(v))
def _nodial():
    raise ConnectionError("nobody home")


v = sk._validate_symbols_body.__wrapped__(["AAA"], adapter_factory=_nodial)
check("validate_symbols: TWS down -> error, nothing claimed found",
      v["found"] == [] and "nobody home" in (v["error"] or ""), str(v))

print("=== [6] overlap gate ===============================================")
root6 = fresh_root()
seeded = seed_series(root6, "CCC", MON, 390, 100.0)
tenx = [ib_stub((b[0], b[1] * 10, b[2] * 10, b[3] * 10, b[4] * 10, b[5]))
        for b in seeded]
days = {MON: tenx,
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 390, 1010.0)]}
rep = run_fill(root6, [("CCC", "1m")], FakeAdapter(days), today=TUE)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root6, "CCC", 2024, 6,
                                              "1m"))
check("overlap gate: price disagreement halts, nothing written",
      "OVERLAP GATE" in (r.get("halt") or "") and len(rb) == 390,
      str(r.get("halt"))[:200])

# F6 — pin-after-verify: an UNPINNED series (manifest conid is None) whose
# overlap gate HALTS must NOT leave a conId pinned. Modelled on [6]: the
# seeded archive disagrees grossly (x10) with what IBKR serves, so the
# gate halts and commits nothing. The pre-pass resolved the symbol (so the
# old code pinned it up front) — but persistence now happens ONLY at
# commit, which never runs. The wrong contract must NOT be on the manifest.
root6f = fresh_root()
seeded = seed_series(root6f, "UNPN", MON, 390, 100.0)
assert ss.load_manifest(root6f / "UNPN").get("conid") is None  # unpinned
tenx = [ib_stub((b[0], b[1] * 10, b[2] * 10, b[3] * 10, b[4] * 10, b[5]))
        for b in seeded]
days = {MON: tenx,
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 390, 1010.0)]}
rep = run_fill(root6f, [("UNPN", "1m")], FakeAdapter(days, conid=777),
               today=TUE)
r = rep["series"][0]
check("F6: unpinned series whose overlap gate HALTS leaves conId STILL "
      "None — the wrong contract was NOT persisted before the gate",
      "OVERLAP GATE" in (r.get("halt") or "")
      and ss.load_manifest(root6f / "UNPN").get("conid") is None,
      f"halt={str(r.get('halt'))[:120]} "
      f"conid={ss.load_manifest(root6f / 'UNPN').get('conid')}")

class ZeroCommitHaltAdapter(FakeAdapter):
    def __init__(self):
        super().__init__({}, head=datetime(2024, 6, 17, 13, 30,
                                           tzinfo=timezone.utc))

    def fetch(self, contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        raise sk.SeriesHalt("contract rejected: selftest zero-commit halt")


root6h = fresh_root()
rep = run_fill(root6h, [("HALT0", "1m")], ZeroCommitHaltAdapter(),
               today=TUE)
r = rep["series"][0]
hmap = sk.load_halted_series(root6h)
hrows = sk.halted_series_for_interval(root6h, "1m")
check("P2: zero-commit SeriesHalt writes retry sidecar and update-row helper "
      "lists it",
      r.get("halt") and "HALT0 1m" in hmap
      and hrows and hrows[0][0] == "HALT0"
      and "zero-commit" in hrows[0][2].get("reason", ""),
      f"halt={r.get('halt')} sidecar={hmap} rows={hrows}")
check("P2: halted retry rows skip already-visible stored rows",
      sk.halted_series_for_interval(root6h, "1m",
                                    known={("HALT0", "1m")}) == [])
fake_ok = FakeAdapter(
    {MON: [ib_stub(b) for b in vendor_bars(MON, 60, 20.0)],
     TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 20.6)]},
    head=datetime(2024, 6, 17, 13, 30, tzinfo=timezone.utc))
rep = run_fill(root6h, [("HALT0", "1m")], fake_ok, today=TUE)
r = rep["series"][0]
check("P2: next successful commit clears the halted-series retry sidecar",
      not r.get("halt") and r.get("written", 0) > 0
      and "HALT0 1m" not in sk.load_halted_series(root6h),
      f"halt={r.get('halt')} written={r.get('written')} "
      f"sidecar={sk.load_halted_series(root6h)}")

print("=== [7] volume calibration (lots) ==================================")
root7 = fresh_root()
seeded = seed_series(root7, "DDD", MON, 390, 50.0)
lots_overlap = [ib_stub((b[0], b[1], b[2], b[3], b[4],
                         max(1, b[5] // 100))) for b in seeded]
new_day = vendor_bars(TUE, 390, 51.0, vol=200)     # IBKR-side lots
days = {MON: lots_overlap, TUE: [ib_stub(b) for b in new_day]}
rep = run_fill(root7, [("DDD", "1m")], FakeAdapter(days), today=TUE)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root7, "DDD", 2024, 6,
                                              "1m"))
tue_bars = [b for b in rb if b[0].date() == TUE]
check("lots detected: note + x100 applied to NEW bars",
      any("LOTS" in n for n in r.get("notes", []))
      and tue_bars and tue_bars[0][5] == 200 * 100,
      f"{r.get('notes')} v={tue_bars and tue_bars[0][5]}")

print("=== [8] join gate (split inside the gap) ===========================")
root8 = fresh_root()
seeded = seed_series(root8, "EEE", MON, 390, 100.0)
days = {MON: [ib_stub(b) for b in seeded],
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 390, 101.0)],
        WED: [ib_stub(b) for b in vendor_bars(WED, 390, 10.1)]}  # 10:1!
rep = run_fill(root8, [("EEE", "1m")], FakeAdapter(days), today=WED)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root8, "EEE", 2024, 6,
                                              "1m"))
check("join gate: halt names the split, committed only pre-cliff",
      "JOIN GATE" in (r.get("halt") or "")
      and len(rb) == 2 * 390
      and all(b[0].date() != WED for b in rb), str(r.get("halt"))[:200])
check("join-gate halt still reports the committed rows (not +0)",
      r.get("added") == 390 and r.get("written", 0) >= 1,
      str({k: r.get(k) for k in ("added", "written")}))

print("=== [9] reconnect / resume =========================================")
root9 = fresh_root()
seeded = seed_series(root9, "FFF", MON, 60, 10.0)
days = {MON: [ib_stub(b) for b in seeded],
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]}
fake = FakeAdapter(days, fail_n=2)
rep = run_fill(root9, [("FFF", "1m")], fake, today=TUE)
r = rep["series"][0]
check("two transient drops: reconnects and completes",
      fake.reconnects == 2 and r["added"] == 60 and not r.get("halt"),
      f"reconnects={fake.reconnects} {r.get('halt')}")
fake_dead = FakeAdapter(days, fail_forever=True)
rep = run_fill(root9, [("FFF", "1m")], fake_dead, today=TUE)
check("permanent outage: series halts with resume advice",
      "reconnect attempts" in (rep["series"][0].get("halt") or ""),
      str(rep["series"][0].get("halt")))

print("=== [9b] pacing violations: back off, do NOT reconnect ==============")
sk.PACE_VIOLATION_BACKOFF_S = 0.0     # no real sleeping in tests
root9b = fresh_root()
seeded = seed_series(root9b, "PAC", MON, 60, 10.0)
days = {MON: [ib_stub(b) for b in seeded],
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]}
fp = FakeAdapter(days, pace_n=2)      # rejected twice, then clears
rep = run_fill(root9b, [("PAC", "1m")], fp, today=TUE)
r = rep["series"][0]
check("pacing violation: backs off + retries, completes, NO reconnect",
      r["added"] == 60 and not r.get("halt") and fp.reconnects == 0,
      f"added={r['added']} reconnects={fp.reconnects} {r.get('halt')}")

fp2 = FakeAdapter(days, pace_forever=True)
rep = run_fill(root9b, [("PAC", "1m")], fp2, today=TUE)
r = rep["series"][0]
check("pacing never clears: halts 're-run later', not 'reconnect', "
      "no reconnect attempted",
      "pacing limit did not clear" in (r.get("halt") or "")
      and "reconnect attempts" not in (r.get("halt") or "")
      and fp2.reconnects == 0, str(r.get("halt"))[:160])

check("PacingViolation IS a ConnectionError (graceful elsewhere)",
      issubclass(sk.PacingViolation, ConnectionError))

# Pacer.saturate(): a freshly-saturated window meters the NEXT request
# at the safe rate (waits ~one slot) instead of bursting
cl = FakeClock()
pz = sk.Pacer(max_requests=4, window_s=600, min_gap_s=0.0,
              time_fn=cl.time, sleep_fn=cl.sleep)
pz.saturate()
t0 = cl.time()
pz.wait_turn()
check("Pacer.saturate: next request waits ~one slot (not the full "
      "window, not zero)",
      0 < cl.time() - t0 <= 600 / 4 + 1, f"waited {cl.time() - t0}")

print("=== [10] cancel mid-run ============================================")
root10 = fresh_root()
seeded = seed_series(root10, "GGG", MON, 60, 10.0)
days = {MON: [ib_stub(b) for b in seeded],
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)],
        WED: [ib_stub(b) for b in vendor_bars(WED, 60, 11.2)]}
ev = threading.Event()


def trip(msg):
    if "2024-06-18" in msg:                  # cancel when TUE starts
        ev.set()


rep = run_fill(root10, [("GGG", "1m")], FakeAdapter(days), today=WED,
               cancel=ev, progress=trip)
check("cancel: run flagged, halt says resume",
      rep["cancelled"] and "re-run" in rep["series"][0]["halt"],
      str(rep["series"][0]))
check("cancel: partial counters survive into the report",
      rep["series"][0].get("dup_existing") == 60
      and rep["series"][0].get("added") == 60,   # MON refetch + TUE,
      str(rep["series"][0])[:250])                # both already in the span
rep = run_fill(root10, [("GGG", "1m")], FakeAdapter(days), today=WED)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root10, "GGG", 2024, 6,
                                              "1m"))
check("resume completes to the full series",
      len(rb) == 3 * 60 and not r.get("halt"), f"{len(rb)}")

print("=== [11] empty series / head timestamp =============================")
root11 = fresh_root()
days = {THU: [ib_stub(b) for b in vendor_bars(THU, 60, 5.0)],
        FRI: [ib_stub(b) for b in vendor_bars(FRI, 60, 5.6)]}
fake = FakeAdapter(days, head=datetime(2024, 6, 20, 13, 30,
                                       tzinfo=timezone.utc))
rep = run_fill(root11, [("HHH", "1m")], fake, today=FRI)
r = rep["series"][0]
check("empty series: backfill from head, no gate needed",
      r["added"] == 120 and not r.get("halt")
      and any("empty series" in n for n in r.get("notes", [])),
      str(r)[:250])
check("empty series: head + 1 weekly span counted — qualify is in the "
      "batch pre-pass (A2), not per-series",
      r["requests"] == 2 and fake.qualify_calls == 0, f"req={r['requests']}")
check("empty series: conId persists even with NO prior manifest "
      "(first commit pins it)",
      ss.load_manifest(root11 / "HHH").get("conid") == 111,
      str(ss.load_manifest(root11 / "HHH").get("conid")))

print("=== [12] doctor =====================================================")
check("doctor session: premarket probes the prior completed weekday",
      sk._latest_completed_session_end(
          datetime(2026, 7, 29, 5, 34)) == datetime(2026, 7, 28, 16, 0))
check("doctor session: after the regular close probes today",
      sk._latest_completed_session_end(
          datetime(2026, 7, 29, 16, 1)) == datetime(2026, 7, 29, 16, 0))
check("doctor session: Monday premarket skips the weekend",
      sk._latest_completed_session_end(
          datetime(2024, 6, 17, 8, 0)) == datetime(2024, 6, 14, 16, 0))
check("doctor session: a full-closure holiday skips to the prior session",
      sk._latest_completed_session_end(
          datetime(2024, 7, 4, 14, 0)) == datetime(2024, 7, 3, 13, 0))
check("doctor session: an early close becomes current at 13:00",
      sk._latest_completed_session_end(
          datetime(2025, 7, 3, 13, 1)) == datetime(2025, 7, 3, 13, 0))
# Actual doctor success/dead-port cases moved to the confined A2 workflow suite:
# test_actual_estimate_doctor_and_span_producers and
# test_actual_legacy_lookup_estimate_and_span_outcomes_under_owned_roots.
# Those cases now exercise LiveIB's guarded choke, not an unguarded FakeAdapter.

print("=== [13] entry gate: thin overlap ==================================")
root13 = fresh_root()
seeded = seed_series(root13, "III", MON, 10, 10.0)      # 10-bar session
days = {MON: [ib_stub(b) for b in seeded],              # identical, thin
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.1)]}
rep = run_fill(root13, [("III", "1m")], FakeAdapter(days), today=TUE)
r = rep["series"][0]
check("thin but consistent overlap: proceeds with the THIN note",
      not r.get("halt") and r["added"] == 60
      and any("THIN" in n for n in r.get("notes", [])),
      f"{r.get('halt')} {r.get('notes')}")

root13b = fresh_root()
seeded = seed_series(root13b, "JJJ", MON, 10, 100.0)
tenx = [ib_stub((b[0], b[1] * 10, b[2] * 10, b[3] * 10, b[4] * 10, b[5]))
        for b in seeded]
days = {MON: tenx, TUE: [ib_stub(b) for b in vendor_bars(TUE, 60,
                                                         1001.0)]}
rep = run_fill(root13b, [("JJJ", "1m")], FakeAdapter(days), today=TUE)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root13b, "JJJ", 2024, 6,
                                              "1m"))
check("thin overlap on a different basis: ENTRY GATE halts, no write",
      "ENTRY GATE" in (r.get("halt") or "")
      and "overlap too thin" in (r.get("halt") or "") and len(rb) == 10,
      str(r.get("halt"))[:200])

print("=== [14] entry gate: empty refetch day (proven bypass) =============")
root14 = fresh_root()
seed_series(root14, "KKK", MON, 390, 100.0)
days = {MON: [],                                        # IBKR: no data
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 390, 1000.0)]}  # 10x!
rep = run_fill(root14, [("KKK", "1m")], FakeAdapter(days), today=TUE)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root14, "KKK", 2024, 6,
                                              "1m"))
check("zero overlap is never a pass: 10x next day halts at the gate",
      "ENTRY GATE" in (r.get("halt") or "") and len(rb) == 390
      and r.get("added", 0) == 0, str(r.get("halt"))[:200])

root14b = fresh_root()
seed_series(root14b, "LLL", MON, 390, 100.0)            # last close 103.90
days = {MON: [],
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 390, 103.9)]}
rep = run_fill(root14b, [("LLL", "1m")], FakeAdapter(days), today=TUE)
r = rep["series"][0]
check("zero overlap but continuous: passes with the THIN note",
      not r.get("halt") and r["added"] == 390
      and any("THIN" in n and "0 shared" in n
              for n in r.get("notes", [])),
      f"{r.get('halt')} {r.get('notes')}")

print("=== [15] entry gate: coarse intervals are no longer blind ==========")
root15 = fresh_root()
seeded = seed_series(root15, "MMM", MON, 26, 80.0, interval="15m",
                     step_min=15)                       # full 15m session
tenx = [ib_stub((b[0], b[1] * 10, b[2] * 10, b[3] * 10, b[4] * 10, b[5]))
        for b in seeded]
days = {MON: tenx,
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 26, 801.0,
                                              step_min=15)]}
rep = run_fill(root15, [("MMM", "15m")], FakeAdapter(days), today=TUE)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root15, "MMM", 2024, 6,
                                              "15m"))
check("15m session (26 bars < old min 30) now engages the gate, halts",
      "OVERLAP GATE" in (r.get("halt") or "") and len(rb) == 26,
      str(r.get("halt"))[:200])

print("=== [16] halts/cancels keep the partial counters ===================")
# TWO MONTHLY spans: with the "1 M" default the June data is one span
# and the interruption lands on a LATER (July/Aug) span — proves a
# coarse-span run keeps everything already fetched. Data lives only in
# the June week; intermediate spans come back empty.
LATER = date(2024, 8, 19)                 # ~2 months on -> a second span
root16 = fresh_root()
seeded = seed_series(root16, "NNN", MON, 60, 10.0)
days = {MON: [ib_stub(b) for b in seeded],
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]}
clock16 = FakeClock()


def cancel_sleep(seconds, cancel=None):
    raise sk.Cancelled()              # any pacer sleep = user cancelled


# qualify now runs in the batch pre-pass (off the pacer), so the FIRST
# paced request is the June span=req1 (fits), and the next span=req2 trips
# the max -> sleep -> cancel, AFTER June is committed
pacer16 = sk.Pacer(max_requests=1, window_s=600, min_gap_s=0.0,
                   time_fn=clock16.time, sleep_fn=cancel_sleep,
                   force_metered=True)   # 1m is non-metered by default;
                                         # force it so the window cap bites
rep = _offline_gap_fill_body(root16, [("NNN", "1m")],
                  adapter_factory=lambda: FakeAdapter(days),
                  pacer=pacer16, today=LATER)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root16, "NNN", 2024, 6,
                                              "1m"))
check("pacer-raised cancel between spans: June committed, rest not",
      rep["cancelled"] and len(rb) == 2 * 60, f"{len(rb)}")
check("pacer-raised cancel: partial counters survive into the report",
      r.get("added") == 60 and r.get("dup_existing") == 60
      and "re-run" in (r.get("halt") or ""), str(r)[:250])


class DieLaterAdapter(FakeAdapter):
    """The connection dies on a span AFTER June and account() dies WITH
    it — the give-up flush must run on the src name captured while
    alive (committed June months stay)."""

    def fetch(self, contract, end_dt, duration, bar_size):
        if end_dt.date() >= date(2024, 7, 20):
            self.dead = True
            raise ConnectionError("link down")
        return super().fetch(contract, end_dt, duration, bar_size)

    def account(self):
        if getattr(self, "dead", False):
            raise ConnectionError("socket gone")
        return super().account()


root16b = fresh_root()
seeded = seed_series(root16b, "OOO", MON, 60, 10.0)
days = {MON: [ib_stub(b) for b in seeded],
        TUE: [ib_stub(b) for b in vendor_bars(TUE, 60, 10.6)]}
# pin 1-M spans: this verifies give-up FLUSH logic across a June span that
# commits and a LATER span that dies — independent of the production span size
# (now "2 M", which would put June and the death-zone in one chunk).
_sp16b = sk._FETCH_SPAN["1m"]
sk._FETCH_SPAN["1m"] = ("1 M", 27)
try:
    rep = run_fill(root16b, [("OOO", "1m")], DieLaterAdapter(days),
                   today=LATER)
finally:
    sk._FETCH_SPAN["1m"] = _sp16b
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root16b, "OOO", 2024, 6,
                                              "1m"))
check("give-up flush survives a dead connection (early src capture)",
      "reconnect attempts" in (r.get("halt") or "")
      and r.get("dup_existing") == 60 and len(rb) == 2 * 60,
      str(r.get("halt"))[:200])
# the connection-lost popup data: the give-up records WHERE it got to
check("give-up records stored_through + committed_through for the popup",
      r.get("stored_through") is not None
      and r.get("committed_through") == TUE
      and sk.lost_connection_series(rep) == [r],
      f"from={r.get('stored_through')} to={r.get('committed_through')}")
psum = sk.progress_summary(r)
check("progress_summary: dates + ~trading days + bars",
      "2024-06-17" in psum and "2024-06-18" in psum
      and "0.2 trading days" in psum and "60 bars saved" in psum, psum)
# a NEW (empty) series that gets nowhere still summarizes safely
empty = {"ticker": "ZZZ", "interval": "1m", "added": 0,
         "stored_through": None, "committed_through": None}
ps2 = sk.progress_summary(empty)
check("progress_summary: empty/new series with no progress is safe",
      "new series" in ps2 and "nothing new committed" in ps2, ps2)

print("=== [17] preflight (go/no-go before any write) =====================")
# All three outcomes (go, closed listener, empty data) moved to actual guarded
# roots in the confined workflow suite; ordinary production still refuses.
_preflight_held = False
try:
    sk.preflight(probe_fn=lambda *args: (_ for _ in ()).throw(
        AssertionError("held preflight reached probe")), import_fn=lambda: "selftest-stub")
except sk.AuthorityError:
    _preflight_held = True
check("preflight: default production hold is not bypassed", _preflight_held)

print("=== [18] recorded actions: gates consult the manifest ==============")


def record(root, ticker, day, kind, factor, applies):
    sb.apply_action(root, ticker,
                    {"date": day if isinstance(day, str)
                     else day.isoformat(),
                     "kind": kind, "factor": factor, "applies": applies,
                     "source": "user", "evidence": "selftest",
                     "run": None})


# (a) control: a 4-for-1 split inside the gap, NOTHING recorded — the
# join gate still halts at the cliff and points at the basis doctor
root18 = fresh_root()
seeded = seed_series(root18, "PPP", MON, 390, 100.0)
days18 = {MON: [ib_stub(b) for b in seeded],
          TUE: [ib_stub(b) for b in vendor_bars(TUE, 390, 101.0)],
          WED: [ib_stub(b) for b in vendor_bars(WED, 390, 25.25)]}
rep = run_fill(root18, [("PPP", "1m")], FakeAdapter(days18), today=WED)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root18, "PPP", 2024, 6,
                                              "1m"))
check("no action recorded: 0.25x cliff still halts at the join gate",
      "JOIN GATE" in (r.get("halt") or "")
      and "basis doctor" in (r.get("halt") or "")
      and len(rb) == 2 * 390
      and all(b[0].date() != WED for b in rb),
      str(r.get("halt"))[:200])

# (b) record the split (action date = the cliff day, the FIRST session
# on the NEW basis) and re-run: the fill crosses the boundary
record(root18, "PPP", WED, "split", 0.25, "price")
rep = run_fill(root18, [("PPP", "1m")], FakeAdapter(days18), today=WED)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root18, "PPP", 2024, 6,
                                              "1m"))
check("recorded split: the same fill completes past the cliff",
      not r.get("halt") and len(rb) == 3 * 390
      and any(b[0].date() == WED for b in rb) and r.get("added") == 390,
      f"halt={r.get('halt')} rows={len(rb)}")
check("recorded split: note names the boundary and the factor",
      any("crossed recorded split/basis boundary" in n and str(WED) in n
          and "0.25" in n for n in r.get("notes", [])),
      str(r.get("notes")))

# (c) adjusted-history refetch: the archive is PRE-action, IBKR serves
# the overlap session at 0.25x. Control root first: WITHOUT the action
# the entry gate halts; with it the fill proceeds + boundary day lands.
def quarter_days(seeded_bars):
    adj = [ib_stub((b[0], b[1] * 0.25, b[2] * 0.25, b[3] * 0.25,
                    b[4] * 0.25, b[5])) for b in seeded_bars]
    return {MON: adj,
            TUE: [ib_stub(b) for b in vendor_bars(TUE, 390, 25.3)]}


root18c = fresh_root()
seeded = seed_series(root18c, "QQQ", MON, 390, 100.0)
rep = run_fill(root18c, [("QQQ", "1m")],
               FakeAdapter(quarter_days(seeded)), today=TUE)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root18c, "QQQ", 2024, 6,
                                              "1m"))
check("adjusted refetch, no action: entry gate halts (control)",
      "OVERLAP GATE" in (r.get("halt") or "")
      and "basis doctor" in (r.get("halt") or "") and len(rb) == 390,
      str(r.get("halt"))[:200])

root18d = fresh_root()
seeded = seed_series(root18d, "RRR", MON, 390, 100.0)
record(root18d, "RRR", TUE, "split", 0.25, "price")    # after stored end
rep = run_fill(root18d, [("RRR", "1m")],
               FakeAdapter(quarter_days(seeded)), today=TUE)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root18d, "RRR", 2024, 6,
                                              "1m"))
tue_rows = [b for b in rb if b[0].date() == TUE]
check("adjusted refetch + recorded split: proceeds, boundary day lands",
      not r.get("halt") and len(tue_rows) == 390
      and abs(tue_rows[0][1] - 25.3) < 1e-9
      and abs(rb[0][1] - 100.0) < 1e-9,    # stored overlap stays golden
      f"halt={r.get('halt')} tue={len(tue_rows)}")
check("adjusted refetch: note says the recorded basis matched",
      any("recorded post-action basis" in n
          for n in r.get("notes", [])), str(r.get("notes")))

# (d) volume-scale recorded: fill completes, note explains the break,
# and volumes are NOT multiplied (factors are consumed at read time)
root18e = fresh_root()
seeded = seed_series(root18e, "SSS", MON, 390, 50.0)
record(root18e, "SSS", TUE, "volume-scale", 4.0, "volume")
days18e = {MON: [ib_stub(b) for b in seeded],
           TUE: [ib_stub(b) for b in vendor_bars(TUE, 390, 50.5,
                                                 vol=300)]}
rep = run_fill(root18e, [("SSS", "1m")], FakeAdapter(days18e),
               today=TUE)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root18e, "SSS", 2024, 6,
                                              "1m"))
tue_rows = [b for b in rb if b[0].date() == TUE]
check("volume-scale recorded: fill completes, note says expected",
      not r.get("halt")
      and any("volume basis change recorded (factor 4)" in n
              for n in r.get("notes", [])),
      f"halt={r.get('halt')} notes={r.get('notes')}")
check("volume-scale recorded: volumes stored as served, NOT multiplied",
      tue_rows and tue_rows[0][5] == 300, str(tue_rows[:1]))

# (e) an action OUTSIDE the window relaxes nothing: same 0.25x cliff,
# but the recorded boundary is years earlier -> still halts
root18f = fresh_root()
seeded = seed_series(root18f, "UUU", MON, 390, 100.0)
record(root18f, "UUU", "2023-01-03", "split", 0.25, "price")
days18f = {MON: [ib_stub(b) for b in seeded],
           TUE: [ib_stub(b) for b in vendor_bars(TUE, 390, 101.0)],
           WED: [ib_stub(b) for b in vendor_bars(WED, 390, 25.25)]}
rep = run_fill(root18f, [("UUU", "1m")], FakeAdapter(days18f),
               today=WED)
r = rep["series"][0]
rb, _ = ss.read_month_file(ss.month_file_path(root18f, "UUU", 2024, 6,
                                              "1m"))
check("action outside the window does NOT relax the gates",
      "JOIN GATE" in (r.get("halt") or "") and len(rb) == 2 * 390
      and all(b[0].date() != WED for b in rb)
      and not any("crossed recorded" in n for n in r.get("notes", [])),
      str(r.get("halt"))[:200])

print("=== [19] find / build helpers ======================================")


# Lookup/estimate positives, empty/down/unknown input, monthly grouping and
# since/earliest outcomes moved to actual owned roots in the A2 workflow suite.
# The reviewed calendar correctly counts 14 sessions (Juneteenth closed), not
# this old fixture's 15 guessed weekdays. This legacy corpus pins default hold.
for _root_name, _root_args in (("find_symbol", ("TEST",)), ("company_lookup", ("TEST",)),
        ("validate_symbols", (["TEST"],)), ("doctor", ()),
        ("estimate_backfill", ("TEST", "1m")), ("probe_max_span", ("1m",))):
    _factories = []
    def _forbidden_factory():
        _factories.append(True)
        raise AssertionError("held root reached a factory")
    _held = False
    with tempfile.TemporaryDirectory() as _evidence:
        try:
            getattr(sk, _root_name)(*_root_args, adapter_factory=_forbidden_factory,
                                   evidence_dir=_evidence)
        except sk.AuthorityError:
            _held = True
    check(f"{_root_name}: default A2 hold precedes every factory", _held and not _factories)

root19 = fresh_root()
days19 = {date(2024, 6, 20): [ib_stub(b) for b in
                              vendor_bars(date(2024, 6, 20), 60, 5.0)],
          date(2024, 6, 21): [ib_stub(b) for b in
                              vendor_bars(date(2024, 6, 21), 60, 5.6)]}
sk.RECONNECT_BACKOFF_S = 0.0
clock19 = FakeClock()
rep = _offline_gap_fill_body(root19, [("QQZ", "1m")],
                  adapter_factory=lambda: FakeAdapter(
                      days19, head=datetime(2024, 5, 1, 13, 30,
                                            tzinfo=timezone.utc)),
                  pacer=fast_pacer(clock19), today=date(2024, 6, 21),
                  since=date(2024, 6, 20))
r19 = rep["series"][0]
check("gap_fill: since-limited build fetches only the asked window",
      r19["days_planned"] == 2 and r19["added"] == 120
      and any("backfill limited to 2024-06-20" in n
              for n in r19.get("notes", []))
      and not r19.get("halt"), str(r19)[:250])

root19b = fresh_root()
rep = _offline_gap_fill_body(root19b, [("QQY", "1m")],
                  adapter_factory=lambda: FakeAdapter(
                      days19, head=datetime(2024, 6, 20, 13, 30,
                                            tzinfo=timezone.utc)),
                  pacer=fast_pacer(FakeClock()),
                  today=date(2024, 6, 21), since=date(2019, 1, 1))
r19b = rep["series"][0]
check("gap_fill: depth older than the stock -> earliest till now",
      r19b["days_planned"] == 2 and r19b["added"] == 120
      and any("earliest available" in n for n in r19b.get("notes", []))
      and not r19b.get("halt"), str(r19b)[:250])

import socket as _socket  # noqa: E402 — real loopback probe
_srv = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
_srv.bind(("127.0.0.1", 0))
_srv.listen(1)
_live_port = _srv.getsockname()[1]
check("tws_listening: real listener found, closed port refused",
      sk.tws_listening("127.0.0.1", (_live_port,))
      and not sk.tws_listening("127.0.0.1", (1,)))
_srv.close()

print("=== [20] partial last month: top-up extends, never skips ===========")
root20 = fresh_root()
may29 = date(2024, 5, 29)                          # Wed; May is partial
seeded = seed_series(root20, "PPP", may29, 60, 10.0)
days = {may29: [ib_stub(b) for b in seeded],
        date(2024, 5, 30): [ib_stub(b)
                            for b in vendor_bars(date(2024, 5, 30),
                                                 60, 10.6)],
        date(2024, 5, 31): [ib_stub(b)
                            for b in vendor_bars(date(2024, 5, 31),
                                                 60, 11.2)],
        date(2024, 6, 3): [ib_stub(b)
                           for b in vendor_bars(date(2024, 6, 3),
                                                60, 11.8)]}
rep = run_fill(root20, [("PPP", "1m")], FakeAdapter(days),
               today=date(2024, 6, 3))
mayb, _ = ss.read_month_file(ss.month_file_path(root20, "PPP", 2024, 5,
                                                "1m"))
junb, _ = ss.read_month_file(ss.month_file_path(root20, "PPP", 2024, 6,
                                                "1m"))
check("partial May extended in place AND June started — no month "
      "skipped",
      len(mayb) == 3 * 60 and len(junb) == 60
      and not rep["series"][0].get("halt"),
      f"may={len(mayb)} jun={len(junb)} "
      f"halt={rep['series'][0].get('halt')}")

print("=== [21] coarse-span fetch == per-day, with fewer requests =========")
# the same backfill two ways: normal weekly spans vs FORCED per-day
# (max_cal=0 -> each session its own chunk). The tree must be
# byte-identical; the span run must make strictly fewer fetches.
WEEK2 = [date(2024, 6, d) for d in (17, 18, 19, 20, 21, 24, 25)]


def _span_backfill(span_for_1m):
    root = fresh_root()
    seeded = seed_series(root, "SPN", WEEK2[0], 60, 100.0)
    dd = {WEEK2[0]: [ib_stub(b) for b in seeded]}        # overlap matches
    for i, d in enumerate(WEEK2[1:], 1):
        dd[d] = [ib_stub(b) for b in vendor_bars(d, 60, 100.0 + i)]
    fake = FakeAdapter(dd)
    orig = sk._FETCH_SPAN["1m"]
    sk._FETCH_SPAN["1m"] = span_for_1m
    try:
        rep = run_fill(root, [("SPN", "1m")], fake, today=WEEK2[-1])
    finally:
        sk._FETCH_SPAN["1m"] = orig
    bars, _ = ss.read_month_file(ss.month_file_path(root, "SPN", 2024,
                                                    6, "1m"))
    return bars, len(fake.fetch_calls), rep["series"][0]


span_bars, span_calls, span_r = _span_backfill(("1 W", 6))
day_bars, day_calls, day_r = _span_backfill(("1 D", 0))
check("span fetch tree is byte-identical to the per-day fetch",
      span_bars == day_bars and len(span_bars) == 7 * 60, str(len(span_bars)))
check("span fetch makes strictly fewer requests (2 weeks: 2 vs 7)",
      span_calls == 2 and day_calls == 7
      and span_r["added"] == day_r["added"] == 6 * 60,
      f"span={span_calls} day={day_calls}")

# a split INSIDE a span still halts at the right session boundary
root21 = fresh_root()
seeded = seed_series(root21, "SPL", WEEK2[0], 60, 100.0)
dd = {WEEK2[0]: [ib_stub(b) for b in seeded],
      WEEK2[1]: [ib_stub(b) for b in vendor_bars(WEEK2[1], 60, 101.0)],
      WEEK2[2]: [ib_stub(b)                       # WED: 10x cliff (split)
                 for b in vendor_bars(WEEK2[2], 60, 1010.0)]}
rep = run_fill(root21, [("SPL", "1m")], FakeAdapter(dd), today=WEEK2[2])
r = rep["series"][0]
splb, _ = ss.read_month_file(ss.month_file_path(root21, "SPL", 2024, 6,
                                                "1m"))
check("split inside a span: halts at the cliff, commits only pre-cliff",
      "JOIN GATE" in (r.get("halt") or "") and len(splb) == 2 * 60
      and all(b[0].date() != WEEK2[2] for b in splb),
      f"{len(splb)} {r.get('halt')}")


# Ladder empty-response/back-off and sub-minute refusal now use real owned roots
# in test_actual_legacy_lookup_estimate_and_span_outcomes_under_owned_roots.

print("=== [22] F5 reception-completeness =================================")
# a NORMAL interior trading day that returns NO bars = suspected drop
jm = date(2024, 7, 8)                          # Mon (no holidays that week)
jt, jw, jr, jf = (jm + timedelta(days=i) for i in (1, 2, 3, 4))
root22 = fresh_root()
sd = seed_series(root22, "GAP", jm, 60, 50.0)
days22 = {jm: [ib_stub(b) for b in sd],                       # overlap
          jt: [ib_stub(b) for b in vendor_bars(jt, 60, 50.6)],
          # jw == 2024-07-10 (a normal Wed) is DROPPED — no entry at all
          jr: [ib_stub(b) for b in vendor_bars(jr, 60, 50.7)],
          jf: [ib_stub(b) for b in vendor_bars(jf, 60, 50.8)]}
r = run_fill(root22, [("GAP", "1m")], FakeAdapter(days22),
             today=jf)["series"][0]
check("F5: a dropped INTERIOR trading day is flagged + recorded (no halt)",
      not r.get("halt")
      and any("RECEPTION GAP" in n for n in r.get("notes", []))
      and r.get("completeness", {}).get("missing_interior") == ["2024-07-10"],
      f"notes={r.get('notes')} completeness={r.get('completeness')}")
# an interior HOLIDAY (July 4) returning no bars is NOT a drop — legit close
hm = date(2024, 7, 1)                          # Mon; that week has July 4 (Thu)
ht, hw, hr, hf = (hm + timedelta(days=i) for i in (1, 2, 3, 4))
root22b = fresh_root()
sd = seed_series(root22b, "HOL", hm, 60, 30.0)
days22b = {hm: [ib_stub(b) for b in sd],
           ht: [ib_stub(b) for b in vendor_bars(ht, 60, 30.6)],
           hw: [ib_stub(b) for b in vendor_bars(hw, 60, 30.7)],  # 7/3 half-day
           # hr == 2024-07-04 Independence Day: NO session
           hf: [ib_stub(b) for b in vendor_bars(hf, 60, 30.8)]}
r = run_fill(root22b, [("HOL", "1m")], FakeAdapter(days22b),
             today=hf)["series"][0]
check("F5: an interior HOLIDAY (July 4) returning no bars is NOT flagged",
      not r.get("halt")
      and not any("RECEPTION GAP" in n for n in r.get("notes", []))
      and "completeness" not in r,
      f"notes={r.get('notes')} completeness={r.get('completeness')}")

print("=== [23] F7 random re-fetch spot-check =============================")
import random as _rnd
m23 = date(2024, 7, 8)                          # clean week (no holidays)
t23, w23, r23, f23 = (m23 + timedelta(days=i) for i in (1, 2, 3, 4))


def _days23(sd):
    return {m23: [ib_stub(b) for b in sd],
            t23: [ib_stub(b) for b in vendor_bars(t23, 60, 80.6)],
            w23: [ib_stub(b) for b in vendor_bars(w23, 60, 80.7)],
            r23: [ib_stub(b) for b in vendor_bars(r23, 60, 80.8)],
            f23: [ib_stub(b) for b in vendor_bars(f23, 60, 80.9)]}


root23 = fresh_root()
sd = seed_series(root23, "SPOT", m23, 60, 80.0)
rep = _offline_gap_fill_body(root23, [("SPOT", "1m")],
                  adapter_factory=lambda: FakeAdapter(_days23(sd)),
                  pacer=fast_pacer(FakeClock()), today=f23,
                  spot_check=True, spot_rng=_rnd.Random(0))
r = rep["series"][0]
check("retired F7 arguments preserve report compatibility",
      rep.get("spot_checks_run") == 0
      and "spot_check" not in r
      and not any("SPOT-CHECK" in n for n in r.get("notes", [])),
      f"spot={r.get('spot_check')} runs={rep.get('spot_checks_run')}")


class SpotMismatchAdapter(FakeAdapter):
    """Clean on the SPAN fetches of the main run, but shifts all prices on a
    '1 D' request — which ONLY the F7 spot-check issues — so the re-fetch
    disagrees with what was stored."""
    def fetch(self, contract, end_dt, duration, bar_size):
        out = super().fetch(contract, end_dt, duration, bar_size)
        if duration == "1 D":
            out = [SimpleNamespace(date=b.date, open=b.open + 1,
                                   high=b.high + 1, low=b.low + 1,
                                   close=b.close + 1, volume=b.volume)
                   for b in out]
        return out


root23b = fresh_root()
sd = seed_series(root23b, "BADS", m23, 60, 80.0)
rep = _offline_gap_fill_body(root23b, [("BADS", "1m")],
                  adapter_factory=lambda: SpotMismatchAdapter(_days23(sd)),
                  pacer=fast_pacer(FakeClock()), today=f23,
                  spot_check=True, spot_rng=_rnd.Random(0))
r = rep["series"][0]
check("retired F7 does not issue its mismatch-only re-fetch",
      not r.get("halt")
      and "spot_check" not in r
      and rep.get("spot_checks_run") == 0,
      f"spot={r.get('spot_check')} halt={r.get('halt')}")

print("=== [24] A3 manifest checkpointing (saved once per series) =========")
# three committed months should produce ONE manifest save (the series-end
# checkpoint), not three — the data files are still written per month.
days_a3 = {}
for _mo in (1, 2, 3):
    _d = date(2024, _mo, 8)
    while _d.weekday() > 4:
        _d += timedelta(days=1)
    days_a3[_d] = [ib_stub(b) for b in vendor_bars(_d, 60, 100.0 + _mo)]
fake_a3 = FakeAdapter(days_a3,
                      head=datetime(2024, 1, 1, tzinfo=timezone.utc))
root_a3 = fresh_root()
_saves = []
_orig_save = ss.save_manifest
ss.save_manifest = lambda td, mf: (_saves.append(1), _orig_save(td, mf))[1]
try:
    rep = run_fill(root_a3, [("AAA3", "1m")], fake_a3,
                   today=date(2024, 3, 29))
finally:
    ss.save_manifest = _orig_save
r = rep["series"][0]
man_a3 = ss.load_manifest(root_a3 / "AAA3")
check("A3: 3 committed months -> exactly ONE manifest save (not per-month)",
      len(_saves) == 1 and r.get("written") == 3,
      f"saves={len(_saves)} written={r.get('written')}")
check("A3: the single end-save still persisted ALL months + the conId",
      man_a3.get("conid") == 111
      and len(ss.manifest_months(man_a3, "1m")) == 3,
      f"conid={man_a3.get('conid')} "
      f"months={len(ss.manifest_months(man_a3, '1m'))}")

print("=== [25] fetch/pack PIPELINE (overlap) == serial, crash-safe =========")
# pipeline=True overlaps fetch (producer = main thread, owns the adapter)
# with pack (consumer thread). It MUST produce a byte-identical tree to the
# serial path and stay crash-safe — no deadlock, no corruption — on every
# exit: clean / cancel / fetch give-up / gate cliff / unexpected error.

def _read_tree(root, ticker, interval="1m"):
    out = {}
    files = (list((root / ticker).rglob(f"*_{interval}.parquet"))
             + list((root / ticker).rglob(f"*_{interval}.csv")))
    for p in sorted(files):
        bars, _ = ss.read_month_file(p)
        out[p.stem] = bars        # stem: extension-agnostic key (parquet|csv)
    return out


PSEED = date(2024, 5, 31)                      # Fri — the refetched overlap
PNEW = sk.trading_days(date(2024, 6, 3), date(2024, 7, 12))   # ~29 sessions


def _pipe_run(pipeline, adapter_cls=FakeAdapter, cancel=None, **akw):
    root = fresh_root()
    seeded = seed_series(root, "PIPE", PSEED, 60, 100.0)
    dd = {PSEED: [ib_stub(b) for b in seeded]}          # overlap matches
    for i, d in enumerate(PNEW, 1):
        dd[d] = [ib_stub(b) for b in vendor_bars(d, 60, 100.0 + 0.05 * i)]
    fake = adapter_cls(dd, **akw)
    rep = run_fill(root, [("PIPE", "1m")], fake, today=PNEW[-1],
                   cancel=cancel, pipeline=pipeline)
    return root, rep["series"][0], fake


# --- byte-identical happy path across 3 month files ------------------------
rs, sr, _ = _pipe_run(False)
rp, pr, _ = _pipe_run(True)
ser_tree, pip_tree = _read_tree(rs, "PIPE"), _read_tree(rp, "PIPE")
check("pipeline tree is byte-identical to serial (3 months)",
      ser_tree == pip_tree and len(pip_tree) >= 3,
      f"serial={sorted(ser_tree)} pipe={sorted(pip_tree)}")
check("pipeline report counters match serial exactly",
      (sr["added"], sr["written"], sr["requests"], sr["bars_fetched"],
       sr["counters"], sr.get("committed_through"))
      == (pr["added"], pr["written"], pr["requests"], pr["bars_fetched"],
          pr["counters"], pr.get("committed_through")),
      f"serial+{sr['added']}/{sr['written']}/{sr['requests']}req "
      f"pipe+{pr['added']}/{pr['written']}/{pr['requests']}req")
check("pipeline did not halt on the happy path", not pr.get("halt"),
      str(pr.get("halt")))

# --- determinism: repeat the comparison; ANY divergence = a race ----------
flaky = sum(1 for _ in range(25)
            if _read_tree(_pipe_run(True)[0], "PIPE") != ser_tree)
check("pipeline deterministic over 25 repeats (== serial every time)",
      flaky == 0, f"{flaky}/25 diverged")

# --- gate cliff INSIDE the pipeline: consumer halts, producer stops -------
# a split cliff is deterministic, so the committed pre-cliff tree must be
# byte-identical to the serial cliff run (and the run must not deadlock).
CWK = [date(2024, 6, d) for d in (17, 18, 19)]      # Mon Tue Wed


def _cliff_run(pipeline):
    root = fresh_root()
    seeded = seed_series(root, "PCLF", CWK[0], 60, 100.0)
    dd = {CWK[0]: [ib_stub(b) for b in seeded],
          CWK[1]: [ib_stub(b) for b in vendor_bars(CWK[1], 60, 101.0)],
          CWK[2]: [ib_stub(b) for b in vendor_bars(CWK[2], 60, 1010.0)]}  # 10x
    rep = run_fill(root, [("PCLF", "1m")], FakeAdapter(dd), today=CWK[2],
                   pipeline=pipeline)
    bars, _ = ss.read_month_file(ss.month_file_path(root, "PCLF", 2024, 6,
                                                    "1m"))
    return rep["series"][0], bars


scl_r, scl_b = _cliff_run(False)
pcl_r, pcl_b = _cliff_run(True)
check("pipeline join-cliff halts like serial; pre-cliff bytes identical",
      "JOIN GATE" in (pcl_r.get("halt") or "")
      and pcl_b == scl_b and len(pcl_b) == 2 * 60
      and all(b[0].date() != CWK[2] for b in pcl_b),
      f"halt={pcl_r.get('halt')} bars={len(pcl_b)}")


# --- cancel mid-pipeline: terminates, cancelled, committed stays clean -----
class CancelAfterAdapter(FakeAdapter):
    def __init__(self, days, cancel_ev=None, after=1, **kw):
        super().__init__(days, **kw)
        self._cev, self._after = cancel_ev, after

    def fetch(self, *a):
        r = super().fetch(*a)
        self._after -= 1
        if self._after <= 0 and self._cev is not None:
            self._cev.set()
        return r


cev = threading.Event()
rcx, rep_cx, _ = _pipe_run(True, adapter_cls=CancelAfterAdapter,
                           cancel=cev, cancel_ev=cev, after=1)
cx_clean = True
for p in (list((rcx / "PIPE").rglob("*_1m.parquet"))
          + list((rcx / "PIPE").rglob("*_1m.csv"))):
    try:
        ss.read_month_file(p)               # STRICT reader — any tear raises
    except Exception:                       # noqa: BLE001
        cx_clean = False
check("cancel mid-pipeline: no hang, cancelled, every committed month clean",
      bool(rep_cx.get("halt")) and "cancel" in rep_cx["halt"].lower()
      and cx_clean, f"halt={rep_cx.get('halt')} clean={cx_clean}")


# --- fetch give-up mid-pipeline: resumable halt, prior months stay --------
class DieAfterAdapter(FakeAdapter):
    def __init__(self, days, ok=1, **kw):
        super().__init__(days, **kw)
        self._ok = ok

    def fetch(self, *a):
        if self._ok > 0:
            self._ok -= 1
            return super().fetch(*a)
        raise ConnectionError("down after the first span")


# pin 1-M spans so ok=1 commits the FIRST span and a SECOND span exists to die
# on (a "2 M" span could swallow the whole range into one chunk -> no give-up).
_spgx = sk._FETCH_SPAN["1m"]
sk._FETCH_SPAN["1m"] = ("1 M", 27)
try:
    rgx, rep_gx, fake_gx = _pipe_run(True, adapter_cls=DieAfterAdapter, ok=1)
finally:
    sk._FETCH_SPAN["1m"] = _spgx
gx_clean = True
for p in (list((rgx / "PIPE").rglob("*_1m.parquet"))
          + list((rgx / "PIPE").rglob("*_1m.csv"))):
    try:
        ss.read_month_file(p)
    except Exception:                       # noqa: BLE001
        gx_clean = False
check("fetch give-up mid-pipeline: resumable halt, no hang, committed clean",
      bool(rep_gx.get("halt")) and "resume" in rep_gx["halt"].lower()
      and fake_gx.reconnects >= sk.RECONNECT_ATTEMPTS and gx_clean,
      f"halt={rep_gx.get('halt')} reconnects={fake_gx.reconnects}")

# --- unexpected error in the CONSUMER: surfaces, no deadlock --------------
_orig_commit = sk._commit_month
_boom = [2]


def _boom_commit(*a, **k):
    _boom[0] -= 1
    if _boom[0] <= 0:
        raise RuntimeError("synthetic consumer crash")
    return _orig_commit(*a, **k)


sk._commit_month = _boom_commit
try:
    rbx, rep_bx, _ = _pipe_run(True)
finally:
    sk._commit_month = _orig_commit
check("unexpected consumer error surfaces as a series halt (no deadlock)",
      bool(rep_bx.get("halt"))
      and "synthetic consumer crash" in rep_bx["halt"],
      f"halt={rep_bx.get('halt')}")

print("=== [on_series] ingest-time cross-check hook =======================")


def _oc_setup(tickers):
    """A root seeded with MON data for each ticker + an adapter that serves the
    SAME MON (overlap matches -> clean) plus a fresh TUE, so a today=TUE update
    commits cleanly and on_series fires per series."""
    r = fresh_root()
    for tk in tickers:
        seed_series(r, tk, MON, 390, 100.0)
    d = {MON: [ib_stub(b) for b in vendor_bars(MON, 390, 100.0)],
         TUE: [ib_stub(b) for b in vendor_bars(TUE, 390, 100.0)]}
    return r, d


_oc_seen = []
_r_oc, _oc_days = _oc_setup(["AAA", "BBB"])
_offline_gap_fill_body(_r_oc, [("AAA", "1m"), ("BBB", "1m")],
            adapter_factory=lambda: FakeAdapter(_oc_days),
            pacer=fast_pacer(FakeClock()), today=TUE,
            on_series=lambda t, iv, res: _oc_seen.append((t, iv)))
check("on_series: hook fires once per ingested series, in order",
      _oc_seen == [("AAA", "1m"), ("BBB", "1m")], f"{_oc_seen}")

_oc_lifecycle = []
_r_oc_lifecycle, _oc_lifecycle_days = _oc_setup(["LIFE"])
_offline_gap_fill_body(
    _r_oc_lifecycle, [("LIFE", "1m")],
    adapter_factory=lambda: FakeAdapter(_oc_lifecycle_days),
    pacer=fast_pacer(FakeClock()), today=TUE,
    on_series_start=lambda t, iv: _oc_lifecycle.append(("start", t, iv)),
    on_series=lambda t, iv, res: _oc_lifecycle.append(("complete", t, iv)))
check("on_series_start: observer fires before the successful completion hook",
      _oc_lifecycle == [("start", "LIFE", "1m"),
                        ("complete", "LIFE", "1m")],
      f"{_oc_lifecycle}")

_oc_start_safe = []
_r_oc_start_safe, _oc_start_safe_days = _oc_setup(["SAFE"])
_offline_gap_fill_body(
    _r_oc_start_safe, [("SAFE", "1m")],
    adapter_factory=lambda: FakeAdapter(_oc_start_safe_days),
    pacer=fast_pacer(FakeClock()), today=TUE,
    on_series_start=lambda _t, _iv: (_ for _ in ()).throw(
        RuntimeError("observer failed")),
    on_series=lambda t, iv, res: _oc_start_safe.append((t, iv)))
check("on_series_start: a raising observer is swallowed without changing fill",
      _oc_start_safe == [("SAFE", "1m")], f"{_oc_start_safe}")

_oc_seen2 = []
_cev = threading.Event()
_r_oc2, _oc_days2 = _oc_setup(["CCD", "DDE", "EEF"])
_rep_stop = _offline_gap_fill_body(
    _r_oc2, [("CCD", "1m"), ("DDE", "1m"), ("EEF", "1m")],
    adapter_factory=lambda: FakeAdapter(_oc_days2),
    pacer=fast_pacer(FakeClock()), today=TUE, cancel=_cev,
    on_series=lambda t, iv, res: (_oc_seen2.append((t, iv)) or "stop"))
check("on_series: returning 'stop' halts the run after the first series",
      _oc_seen2 == [("CCD", "1m")], f"{_oc_seen2}")
check("on_series: 'stop' sets cancel + marks validation_stopped",
      _cev.is_set() and _rep_stop.get("validation_stopped") is True,
      f"cancel={_cev.is_set()} vstop={_rep_stop.get('validation_stopped')}")


def _boom_hook(t, iv, res):
    raise RuntimeError("cross-check blew up")


_r_oc3, _oc_days3 = _oc_setup(["FFG"])
_rep_safe = _offline_gap_fill_body(_r_oc3, [("FFG", "1m")],
                        adapter_factory=lambda: FakeAdapter(_oc_days3),
                        pacer=fast_pacer(FakeClock()), today=TUE,
                        on_series=_boom_hook)
check("on_series: a raising hook is swallowed; the run still completes",
      len(_rep_safe.get("series", [])) == 1
      and not _rep_safe.get("validation_stopped"),
      f"{_rep_safe.get('series')}")

# BUG 1 regression: 'stop' with spot_check=True must NOT enter the spot-check
# phase with cancel already set (that would raise Cancelled out of gap_fill and
# DISCARD the finalized report). The run must still return a report with totals.
_oc_seen4 = []
_r_oc5, _oc_days5 = _oc_setup(["GGH", "HHI"])
_rep_sc = _offline_gap_fill_body(
    _r_oc5, [("GGH", "1m"), ("HHI", "1m")],
    adapter_factory=lambda: FakeAdapter(_oc_days5),
    pacer=fast_pacer(FakeClock()), today=TUE, cancel=threading.Event(),
    spot_check=True,
    on_series=lambda t, iv, res: (_oc_seen4.append((t, iv)) or "stop"))
check("on_series 'stop' + spot_check: gap_fill still returns a finalized report "
      "(totals present, report NOT discarded)",
      isinstance(_rep_sc, dict) and "totals" in _rep_sc
      and _rep_sc.get("validation_stopped") is True
      and _oc_seen4 == [("GGH", "1m")],
      f"totals={_rep_sc.get('totals')} seen={_oc_seen4}")

# BUG 2 regression: _merge_recovery must carry validation_stopped through (a
# validator-stop is distinct from a user cancel and lives only on the stopping
# worker's report).
_mr = sk._merge_recovery(
    {"series": [{"ticker": "A"}], "totals": {"added": 1},
     "per_port": {2000: {}}, "validation_stopped": True, "cancelled": False},
    {"series": [], "totals": {}, "per_port": {}, "cancelled": False}, [2000])
check("_merge_recovery preserves validation_stopped (distinct from cancelled)",
      _mr.get("validation_stopped") is True and not _mr.get("cancelled"),
      f"{_mr}")

_orig_gfp = sk.gap_fill_parallel
_rec_calls = []
_rec_events = []
_rec_cancel = threading.Event()


def _fake_gfp(root, selections, ports, **kw):
    _rec_calls.append((list(selections), list(ports), kw.get("cancel"),
                       kw.get("restart_ports")))
    return {
        "run": "ibkr-recovery-test",
        "root": str(root),
        "parallel_ports": list(ports),
        "series": [],
        "totals": {"added": 0, "written": 0, "dup_existing": 0,
                   "conflicts": 0, "requests": 0, "halted_series": 0},
        "cancelled": False,
        "per_port": {int(ports[0]): {"account": "demo", "series": 0,
                                     "added": 0, "aborted": "lost"}},
        "aborted_ports": {int(ports[0]): "lost"},
        "_partition": {int(ports[0]): [("AAA", "1m")]},
        "_unpulled": [("BBB", "1m")],
    }


def _rec_progress(msg):
    _rec_events.append(msg)
    if isinstance(msg, tuple) and msg[0] == "recovering":
        _rec_cancel.set()


sk.gap_fill_parallel = _fake_gfp
try:
    _rec_rep = sk.nightly_gap_fill_parallel_resilient(
        fresh_root(), [("AAA", "1m"), ("BBB", "1m")], [2000],
        restart_ports=lambda ports: {int(p): True for p in ports},
        port_up=lambda _p: True,
        progress=_rec_progress,
        cancel=_rec_cancel,
        max_recover_rounds=1)
finally:
    sk.gap_fill_parallel = _orig_gfp

check("resilient recovery emits interim report before recovery",
      any(isinstance(e, tuple) and e[0] == "report" for e in _rec_events),
      str(_rec_events))
check("resilient recovery emits recovering event with series count",
      any(isinstance(e, tuple) and e[0] == "recovering"
          and e[1].get("series") == 2 for e in _rec_events),
      str(_rec_events))
check("resilient recovery cancel before re-fetch ends without second fetch",
      _rec_rep.get("cancelled") is True and len(_rec_calls) == 1,
      f"rep={_rec_rep} calls={_rec_calls}")
check("resilient wrapper forwards its batch callback into the primary fetch",
      _rec_calls[0][3] is not None, str(_rec_calls))

print("=== [mid-run restart] policy + callback contract =================")

check("mid-run restart: public surface and production defaults are pinned",
      "restart_ports" in inspect.signature(sk.gap_fill_parallel).parameters
      and sk.MIDRUN_RESTART is True
      and sk.MIDRUN_FLEETDOWN_REVIVAL is True
      and sk.REVIVE_AFTER_S == 150.0
      and sk.MIDRUN_RESTART_MAX_PER_PORT == 2
      and sk.BATCH_WINDOW_S == 60.0
      and sk.DECLINE_COOLDOWN_S == 900.0)

_mr_snapshot = {"holding": False, "maintenance_active": False}
check("mid-run restart: healthy-survivor work is controller-eligible",
      sk._midrun_controller_gate(_mr_snapshot, work_remains=True))
check("mid-run restart: pause/cancel/drain/no-work each close the gate",
      not sk._midrun_controller_gate(
          _mr_snapshot, work_remains=True, paused=True)
      and not sk._midrun_controller_gate(
          _mr_snapshot, work_remains=True, cancelled=True)
      and not sk._midrun_controller_gate(
          _mr_snapshot, work_remains=True, draining=True)
      and not sk._midrun_controller_gate(
          _mr_snapshot, work_remains=False))
check("mid-run restart: maintenance and all-dead HOLD remain gated for M1",
      not sk._midrun_controller_gate(
          {"holding": False, "maintenance_active": True}, work_remains=True)
      and not sk._midrun_controller_gate(
          {"holding": True, "maintenance_active": False}, work_remains=True))

_mr_fleet_snapshot = {"holding": True, "maintenance_active": False}
check("mid-run fleet-down restart: one-shot HOLD policy is separately gated",
      sk._midrun_fleetdown_gate(_mr_fleet_snapshot, work_remains=True)
      and not sk._midrun_fleetdown_gate(
          {"holding": False, "maintenance_active": False}, work_remains=True)
      and not sk._midrun_fleetdown_gate(
          {"holding": True, "maintenance_active": True}, work_remains=True)
      and not sk._midrun_fleetdown_gate(
          _mr_fleet_snapshot, work_remains=True, paused=True)
      and not sk._midrun_fleetdown_gate(
          _mr_fleet_snapshot, work_remains=True, cancelled=True)
      and not sk._midrun_fleetdown_gate(
          _mr_fleet_snapshot, work_remains=True, draining=True)
      and not sk._midrun_fleetdown_gate(
          _mr_fleet_snapshot, work_remains=True, offered=True)
      and not sk._midrun_fleetdown_gate(
          _mr_fleet_snapshot, work_remains=False))

_mr_dead = {"state": "DEAD", "grace_seconds": sk.REVIVE_AFTER_S}
_mr_candidate = lambda **over: sk._midrun_port_candidate(
    _mr_dead, listening=over.get("listening", False),
    worker_active=over.get("worker_active", False),
    offers=over.get("offers", 0), now=over.get("now", 1000.0),
    cooldown_until=over.get("cooldown_until", 0.0),
    require_grace=over.get("require_grace", True))
check("mid-run restart: confirmed-dead closed inactive port is eligible",
      _mr_candidate())
check("mid-run restart: listener/worker/cap/cooldown each exclude a port",
      not _mr_candidate(listening=True)
      and not _mr_candidate(worker_active=True)
      and not _mr_candidate(offers=sk.MIDRUN_RESTART_MAX_PER_PORT)
      and not _mr_candidate(now=10.0, cooldown_until=11.0))
check("mid-run restart: REVIVE_AFTER grace is required only at fire gate",
      not sk._midrun_port_candidate(
          {"state": "DEAD", "grace_seconds": sk.REVIVE_AFTER_S - 1.0},
          listening=False, worker_active=False, offers=0, now=10.0,
          cooldown_until=0.0, require_grace=True)
      and sk._midrun_port_candidate(
          {"state": "DEAD", "grace_seconds": sk.REVIVE_AFTER_S - 1.0},
          listening=False, worker_active=False, offers=0, now=10.0,
          cooldown_until=0.0, require_grace=False))

_mr_legacy_calls = []
_mr_legacy = sk._call_restart_ports_batch(
    lambda batch: _mr_legacy_calls.append(list(batch))
    or {2000: True, "3000": False}, [2000, 3000], phase="midrun")
check("mid-run restart: legacy callback stays one-argument and normalized",
      _mr_legacy_calls == [[2000, 3000]]
      and _mr_legacy[2000]["ok"] is True
      and _mr_legacy[3000]["ok"] is False, str(_mr_legacy))

_mr_phase_calls = []


def _mr_phase_callback(batch, phase=None):
    _mr_phase_calls.append((list(batch), phase))
    return {int(p): True for p in batch}


_mr_phase_callback._accepts_restart_phase = True
sk._call_restart_ports_batch(
    _mr_phase_callback, [2000], phase="midrun")
sk._call_restart_ports_batch(
    _mr_phase_callback, [2000, 3000], phase="fleet_down")
check("mid-run restart: opted-in callback receives partial/fleet-down phases",
      _mr_phase_calls == [([2000], "midrun"),
                          ([2000, 3000], "fleet_down")], str(_mr_phase_calls))

_mr_display_source = (
    Path(__file__).resolve().parents[1] / "display_data.py").read_text(
        encoding="utf-8")
check("mid-run restart: popup copy names continuity and brief input block",
      "MID-RUN: the data run continues on surviving ports." in _mr_display_source
      and "App input may be blocked briefly" in _mr_display_source
      and "MID-RUN: every configured TWS port is down." in _mr_display_source
      and "run is holding at a recoverable boundary" in _mr_display_source
      and "resume if relaunch succeeds." in _mr_display_source
      and "restart_ports._accepts_restart_phase = True" in _mr_display_source
      and "batch, emails, state, phase=phase" in _mr_display_source
      and 'fleet_down = phase == "fleet_down"' in _mr_display_source
      and "if fleet_down:" in _mr_display_source)


class _MidrunScript:
    """Small thread-safe hard-death script for controller gate integration."""

    def __init__(self, kill_port=2000, kill_at=2, slow_s=0.03,
                 kill_ports=None):
        self.lock = threading.Lock()
        self.kill_port = int(kill_port)
        self.kill_ports = ({self.kill_port} if kill_ports is None else
                           {int(p) for p in kill_ports})
        self.kill_at = int(kill_at)
        self.slow_s = float(slow_s)
        self.qualifies = {2000: 0, 3000: 0, 4000: 0}
        self.dead = set()
        self.killed_once = set()
        self.death = threading.Event()
        self.restart_calls = []
        self.restart_threads = []

    def qualify(self, port):
        with self.lock:
            self.qualifies[port] += 1
            if (port in self.kill_ports and port not in self.killed_once
                    and port not in self.dead
                    and self.qualifies[port] >= self.kill_at):
                self.dead.add(port)
                self.killed_once.add(port)
                self.death.set()
                return False
            return port not in self.dead

    def open(self, port):
        with self.lock:
            return int(port) not in self.dead

    def revive(self, port):
        with self.lock:
            self.dead.discard(int(port))

    def restart(self, batch):
        normalized = [int(p) for p in batch]
        with self.lock:
            self.restart_calls.append(normalized)
            self.restart_threads.append(threading.current_thread().name)
        for port in normalized:
            self.revive(port)
        return {port: True for port in normalized}


class _MidrunAdapter:
    def __init__(self, port, script, days):
        self.port = int(port)
        self.script = script
        self.days = days
        self.conid = 50000 + self.port

    def _gate(self):
        if not self.script.open(self.port):
            raise ConnectionResetError(f"scripted death on {self.port}")

    def account(self):
        return f"DU{self.port}"

    def qualify_many(self, symbols, chunk=50, progress=None, cancel=None):
        if not self.script.qualify(self.port):
            raise ConnectionResetError(f"scripted death on {self.port}")
        return {symbol: self.conid for symbol in symbols}

    def qualify(self, symbol):
        if not self.script.qualify(self.port):
            raise ConnectionResetError(f"scripted death on {self.port}")
        return self.conid, SimpleNamespace(symbol=symbol)

    def contract_for(self, conid):
        self._gate()
        return SimpleNamespace(symbol="?", conId=conid)

    def head_timestamp(self, contract, what_to_show="TRADES"):
        self._gate()
        return datetime(MON.year, MON.month, MON.day, tzinfo=NY)

    def is_connected(self):
        return self.script.open(self.port)

    def reconnect(self):
        self._gate()

    def fetch(self, contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        self._gate()
        if self.script.slow_s:
            _rt.sleep(self.script.slow_s)
        end_day = end_dt.date()
        if duration.endswith("S"):
            return list(self.days.get(end_day, []))
        count, unit = duration.split()
        span = {"D": 1, "W": 7, "M": 31}[unit] * int(count)
        start_day = end_day - timedelta(days=span - 1)
        return [bar for day in sorted(self.days)
                if start_day <= day <= end_day for bar in self.days[day]]

    def disconnect(self):
        pass


def _run_midrun_gate_case(script, count, *, cancel=None, pause=None,
                          maintenance_provider=None, fleet_grace_s=10.0,
                          adapter_cls=_MidrunAdapter,
                          watchdog_observer=None, progress=None):
    bars = {
        MON: [ib_stub(bar) for bar in vendor_bars(MON, 4, 20.0)],
        TUE: [ib_stub(bar) for bar in vendor_bars(TUE, 4, 21.0)],
    }

    def factory(_host, ports):
        port = int(ports[0])
        return lambda: adapter_cls(port, script, bars)

    gate_maintenance_provider = maintenance_provider

    def watchdog_factory(ports, maintenance_provider=None):
        provider = (gate_maintenance_provider
                    if gate_maintenance_provider is not None
                    else maintenance_provider)
        watchdog_cls = aw.HardDeathWatchdog
        if watchdog_observer is not None:
            class _ObservedWatchdog(aw.HardDeathWatchdog):
                def snapshot(self, *, now=None):
                    value = super().snapshot(now=now)
                    watchdog_observer(value)
                    return value
            watchdog_cls = _ObservedWatchdog
        return watchdog_cls(
            ports, maintenance_provider=provider, port_grace_s=0.2,
            fleet_grace_s=float(fleet_grace_s),
            probe_backoff_s=(0.05, 0.1))

    return sk.nightly_gap_fill_parallel(
        fresh_root(), [(f"MRG{i:02d}", "1m") for i in range(count)],
        [2000, 3000, 4000], today=WED, since=MON - timedelta(days=3),
        cancel=cancel, pause=pause, adapter_factory=factory,
        port_up=script.open, hard_death_watchdog=True,
        watchdog_factory=watchdog_factory, restart_ports=script.restart,
        reprobe_interval=0.2, progress=progress)


def _with_midrun_constants(revive, batch, body, *, fleetdown=None):
    old = (sk.REVIVE_AFTER_S, sk.BATCH_WINDOW_S,
           sk.MIDRUN_RESTART_MAX_PER_PORT, sk.DECLINE_COOLDOWN_S,
           sk.MIDRUN_FLEETDOWN_REVIVAL)
    try:
        sk.REVIVE_AFTER_S = float(revive)
        sk.BATCH_WINDOW_S = float(batch)
        sk.MIDRUN_RESTART_MAX_PER_PORT = 2
        sk.DECLINE_COOLDOWN_S = 1.0
        if fleetdown is not None:
            sk.MIDRUN_FLEETDOWN_REVIVAL = bool(fleetdown)
        return body()
    finally:
        (sk.REVIVE_AFTER_S, sk.BATCH_WINDOW_S,
         sk.MIDRUN_RESTART_MAX_PER_PORT, sk.DECLINE_COOLDOWN_S,
         sk.MIDRUN_FLEETDOWN_REVIVAL) = old


_mr_pause_script = _MidrunScript(slow_s=0.05)
_mr_pause = threading.Event()
_mr_pause_quiet = []


def _mr_pause_watch():
    _mr_pause_script.death.wait(2.0)
    _mr_pause.set()
    _rt.sleep(0.6)
    _mr_pause_quiet.append(not _mr_pause_script.restart_calls)
    _mr_pause.clear()


threading.Thread(target=_mr_pause_watch, daemon=True).start()
_mr_pause_rep = _with_midrun_constants(
    0.3, 0.1, lambda: _run_midrun_gate_case(
        _mr_pause_script, 30, pause=_mr_pause))
check("mid-run restart threaded gate: Pause defers, then re-arms on resume",
      _mr_pause_quiet == [True]
      and _mr_pause_script.restart_calls == [[2000]]
      and not _mr_pause_rep.get("cancelled")
      and len(_mr_pause_rep.get("series") or []) == 30,
      str((_mr_pause_quiet, _mr_pause_script.restart_calls)))

_mr_listen_script = _MidrunScript(slow_s=0.04)


def _mr_self_heal():
    _mr_listen_script.death.wait(2.0)
    _rt.sleep(0.25)
    _mr_listen_script.revive(2000)


threading.Thread(target=_mr_self_heal, daemon=True).start()
_mr_listen_rep = _with_midrun_constants(
    0.5, 0.15, lambda: _run_midrun_gate_case(_mr_listen_script, 20))
check("mid-run restart threaded gate: a listening self-heal is not relaunched",
      not _mr_listen_script.restart_calls
      and (_mr_listen_rep.get("watchdog") or {}).get(
          "states", {}).get("2000") == "HEALTHY",
      str((_mr_listen_script.restart_calls, _mr_listen_rep.get("watchdog"))))

_mr_maint_script = _MidrunScript(kill_at=1, slow_s=0.0)
_mr_maint_cancel = threading.Event()
_mr_maint_release = threading.Event()
_mr_maint_work_blocked = threading.Event()
_mr_maint_controller_ready = threading.Event()
_mr_maint_quiet = []
_mr_maint_cues = []
_mr_maint_samples = []
_mr_maint_samples_lock = threading.Lock()


class _MidrunMaintenanceAdapter(_MidrunAdapter):
    """Keep survivor work present until the controller proves its lease gate."""

    def qualify_many(self, symbols, chunk=50, progress=None, cancel=None):
        if self.port != self.script.kill_port:
            if not self.script.death.wait(5.0):
                raise AssertionError("maintenance test never reached hard death")
            _mr_maint_work_blocked.set()
            if not _mr_maint_release.wait(5.0):
                raise AssertionError("maintenance test survivor was not released")
        return super().qualify_many(
            symbols, chunk=chunk, progress=progress, cancel=cancel)

    def fetch(self, contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        if self.port != self.script.kill_port and self.script.death.is_set():
            _mr_maint_work_blocked.set()
            if not _mr_maint_release.wait(5.0):
                raise AssertionError("maintenance test survivor was not released")
        return super().fetch(
            contract, end_dt, duration, bar_size, what_to_show=what_to_show)


def _mr_maintenance_status():
    return aw.MaintenanceStatus(True, "valid", (2000,))


def _mr_maint_observer(snapshot):
    if threading.current_thread().name != "ibkr-midrun-restart":
        return
    row = (snapshot.get("ports") or {}).get(2000) or {}
    if row.get("state") == "HEALTHY":
        return  # script.death is raised just before watchdog.hard_signal
    sample = (row.get("state"), float(row.get("grace_seconds") or 0.0),
              bool(snapshot.get("maintenance_active")))
    with _mr_maint_samples_lock:
        _mr_maint_samples.append(sample)
        if len(_mr_maint_samples) >= 4:
            _mr_maint_controller_ready.set()


def _mr_maint_watch():
    try:
        death_seen = _mr_maint_script.death.wait(5.0)
        work_retained = death_seen and _mr_maint_work_blocked.wait(5.0)
        controller_seen = (work_retained
                           and _mr_maint_controller_ready.wait(5.0))
        _mr_maint_cues.append(
            (death_seen, work_retained, controller_seen))
        _mr_maint_quiet.append(
            controller_seen and not _mr_maint_script.restart_calls)
    finally:
        _mr_maint_cancel.set()
        _mr_maint_release.set()


_mr_maint_thread = threading.Thread(target=_mr_maint_watch, daemon=True)
_mr_maint_thread.start()
try:
    _mr_maint_rep = _with_midrun_constants(
        0.3, 0.1, lambda: _run_midrun_gate_case(
            _mr_maint_script, 12, cancel=_mr_maint_cancel,
            maintenance_provider=_mr_maintenance_status,
            adapter_cls=_MidrunMaintenanceAdapter,
            watchdog_observer=_mr_maint_observer))
finally:
    _mr_maint_cancel.set()
    _mr_maint_release.set()
    _mr_maint_thread.join(5.0)
check("mid-run restart threaded gate: maintenance freezes grace and offers",
      not _mr_maint_thread.is_alive()
      and _mr_maint_cues == [(True, True, True)]
      and _mr_maint_quiet == [True]
      and len(_mr_maint_samples) >= 4
      and all(state == "SUSPECT" and grace == 0.0 and active
              for state, grace, active in _mr_maint_samples)
      and not _mr_maint_script.restart_calls
      and _mr_maint_rep.get("cancelled"),
      str((_mr_maint_cues, _mr_maint_samples,
           _mr_maint_quiet, _mr_maint_script.restart_calls)))

_mr_false_success = _MidrunScript(slow_s=0.04)


def _mr_claim_success(batch):
    normalized = [int(p) for p in batch]
    with _mr_false_success.lock:
        _mr_false_success.restart_calls.append(normalized)
    return {port: True for port in normalized}  # deliberately leaves socket down


_mr_false_success.restart = _mr_claim_success
_mr_false_success_rep = _with_midrun_constants(
    0.3, 0.1, lambda: _run_midrun_gate_case(_mr_false_success, 24))
_mr_false_success_events = (
    _mr_false_success_rep.get("midrun_restart_events") or [])
check("mid-run restart evidence: final DOWN overrides claimed callback success",
      bool(_mr_false_success.restart_calls)
      and any(event.get("callback_ok") is True
              and event.get("final_probe") == "down"
              and event.get("ok") is False
              for event in _mr_false_success_events),
      str(_mr_false_success_events))

_mr_drain_script = _MidrunScript(slow_s=0.03)
_mr_drain_survivor_blocked = threading.Event()
_mr_drain_controller_seen = threading.Event()


class _MidrunDrainAdapter(_MidrunAdapter):
    """Retain survivor work until the restart controller sees the dead port."""

    def qualify_many(self, symbols, chunk=50, progress=None, cancel=None):
        if self.port != self.script.kill_port:
            if not self.script.death.wait(5.0):
                raise AssertionError("drain test never reached hard death")
            _mr_drain_survivor_blocked.set()
            if not _mr_drain_controller_seen.wait(5.0):
                raise AssertionError("drain test controller never sampled death")
        return super().qualify_many(
            symbols, chunk=chunk, progress=progress, cancel=cancel)


def _mr_drain_observer(snapshot):
    if (threading.current_thread().name == "ibkr-midrun-restart"
            and _mr_drain_survivor_blocked.is_set()
            and ((snapshot.get("ports") or {}).get(2000) or {}).get(
                "state") == "DEAD"):
        _mr_drain_controller_seen.set()


_mr_drain_rep = _with_midrun_constants(
    0.0, 60.0, lambda: _run_midrun_gate_case(
        _mr_drain_script, 12, adapter_cls=_MidrunDrainAdapter,
        watchdog_observer=_mr_drain_observer))
check("mid-run restart threaded gate: drain joins controller with no late fire",
      _mr_drain_survivor_blocked.is_set()
      and _mr_drain_controller_seen.is_set()
      and not _mr_drain_script.restart_calls
      and len(_mr_drain_rep.get("series") or []) == 12
      and not any(row.get("halt")
                  for row in _mr_drain_rep.get("series") or [])
      and not any(thread.name == "ibkr-midrun-restart"
                  for thread in threading.enumerate()),
      str((_mr_drain_survivor_blocked.is_set(),
           _mr_drain_controller_seen.is_set(),
           _mr_drain_script.restart_calls)))

_mr_fleet_ports = [2000, 3000, 4000]
_mr_fleet_success = _MidrunScript(
    kill_at=1, slow_s=0.0, kill_ports=_mr_fleet_ports)
_mr_fleet_success_rep = _with_midrun_constants(
    0.25, 0.1, lambda: _run_midrun_gate_case(
        _mr_fleet_success, 12, fleet_grace_s=0.25), fleetdown=True)
_mr_fleet_success_events = (
    _mr_fleet_success_rep.get("midrun_restart_events") or [])
check("mid-run fleet-down restart: equal-threshold one-shot revives and resumes",
      _mr_fleet_success.restart_calls == [_mr_fleet_ports]
      and _mr_fleet_success.restart_threads == ["ibkr-midrun-restart"]
      and not _mr_fleet_success_rep.get("fleet_down_finalize")
      and not _mr_fleet_success_rep.get("interrupted")
      and not _mr_fleet_success_rep.get("cancelled")
      and not _mr_fleet_success_rep.get("_unpulled")
      and len(_mr_fleet_success_rep.get("series") or []) == 12
      and {row.get("ticker")
           for row in _mr_fleet_success_rep.get("series") or []}
      == {f"MRG{i:02d}" for i in range(12)}
      and not any(row.get("halt")
                  for row in _mr_fleet_success_rep.get("series") or [])
      and set((_mr_fleet_success_rep.get("watchdog") or {}).get(
          "states", {}).values()) == {"HEALTHY"}
      and len(_mr_fleet_success_events) == len(_mr_fleet_ports)
      and all(event.get("phase") == "fleet_down"
              and event.get("round") == 1
              and event.get("callback_ok") is True
              and event.get("final_probe") == "up"
              and event.get("ok") is True
              for event in _mr_fleet_success_events)
      and {event.get("port") for event in _mr_fleet_success_events
           if event.get("phase") == "fleet_down" and event.get("ok")}
      == set(_mr_fleet_ports),
      str((_mr_fleet_success.restart_calls,
           _mr_fleet_success.restart_threads,
           _mr_fleet_success_rep.get("fleet_down_finalize"),
           _mr_fleet_success_events)))

_mr_fleet_success_human = (
    "\n".join(sk.summarize_report(_mr_fleet_success_rep))
    + "\n" + run_log.format_run_log(_mr_fleet_success_rep))
check("mid-run fleet-down restart: successful reroute clears stale human "
      "HALTED/ABORTED status",
      (_mr_fleet_success_rep.get("totals") or {}).get("halted_series") == 0
      and all(not row.get("aborted") for row in
              (_mr_fleet_success_rep.get("per_port") or {}).values())
      and "series HALTED" not in _mr_fleet_success_human
      and "ABORTED:" not in _mr_fleet_success_human,
      _mr_fleet_success_human)

def _run_fleet_teardown_case(*, self_heal):
    """Hold old workers across finalize; optionally reopen their listeners."""
    script = _MidrunScript(
        kill_at=1, slow_s=0.0, kill_ports=_mr_fleet_ports)
    state_lock = threading.Lock()
    teardown_ports = set()
    teardown_all = threading.Event()
    teardown_release = threading.Event()
    teardown_timeout = threading.Event()
    finalize_line = threading.Event()
    finalize_samples = [0]
    quiet = []

    class _FleetTeardownAdapter(_MidrunAdapter):
        def disconnect(self):
            with script.lock:
                was_dead = self.port in script.dead
                if was_dead and self_heal:
                    script.dead.discard(self.port)
            if not was_dead:
                return
            with state_lock:
                teardown_ports.add(self.port)
                if teardown_ports == set(_mr_fleet_ports):
                    teardown_all.set()
            if not teardown_release.wait(5.0):
                teardown_timeout.set()

    def observer(snapshot):
        if (threading.current_thread().name != "ibkr-hard-death-watchdog"
                or not teardown_all.is_set()
                or not snapshot.get("finalize")):
            return
        with state_lock:
            finalize_samples[0] += 1
            if finalize_samples[0] >= 2 and not teardown_release.is_set():
                with script.lock:
                    quiet.append(not script.restart_calls)
                teardown_release.set()

    def progress(message):
        if "FLEET DOWN grace exhausted" in str(message):
            # A pre-fix finalize releases promptly and then fails the assertions.
            finalize_line.set()
            teardown_release.set()

    try:
        report = _with_midrun_constants(
            0.25, 0.1, lambda: _run_midrun_gate_case(
                script, 12, fleet_grace_s=0.25,
                adapter_cls=_FleetTeardownAdapter,
                watchdog_observer=observer, progress=progress), fleetdown=True)
    finally:
        teardown_release.set()
    return {
        "script": script, "report": report, "ports": teardown_ports,
        "all": teardown_all, "timeout": teardown_timeout,
        "finalize_line": finalize_line, "finalize_samples": finalize_samples,
        "quiet": quiet,
    }


_mr_teardown = _run_fleet_teardown_case(self_heal=False)
_mr_teardown_script = _mr_teardown["script"]
_mr_teardown_rep = _mr_teardown["report"]
_mr_teardown_rows = _mr_teardown_rep.get("series") or []
check("mid-run fleet-down restart: finalize waits for dead-worker teardown",
      _mr_teardown["all"].is_set()
      and not _mr_teardown["timeout"].is_set()
      and _mr_teardown["finalize_samples"][0] >= 2
      and _mr_teardown["quiet"] == [True]
      and not _mr_teardown["finalize_line"].is_set()
      and _mr_teardown_script.restart_calls == [_mr_fleet_ports]
      and _mr_teardown_script.restart_threads == ["ibkr-midrun-restart"]
      and not _mr_teardown_rep.get("fleet_down_finalize")
      and not _mr_teardown_rep.get("interrupted")
      and not _mr_teardown_rep.get("_unpulled")
      and len(_mr_teardown_rows) == 12
      and not any(row.get("halt") for row in _mr_teardown_rows),
      str((_mr_teardown["ports"], _mr_teardown["finalize_samples"],
           _mr_teardown["quiet"], _mr_teardown["finalize_line"].is_set(),
           _mr_teardown_script.restart_calls,
           _mr_teardown_rep.get("fleet_down_finalize"))))

_mr_selfheal_teardown = _run_fleet_teardown_case(self_heal=True)
_mr_selfheal_script = _mr_selfheal_teardown["script"]
_mr_selfheal_rep = _mr_selfheal_teardown["report"]
_mr_selfheal_rows = _mr_selfheal_rep.get("series") or []
check("mid-run fleet-down restart: teardown self-heal adopts without relaunch",
      _mr_selfheal_teardown["all"].is_set()
      and not _mr_selfheal_teardown["timeout"].is_set()
      and _mr_selfheal_teardown["finalize_samples"][0] >= 2
      and _mr_selfheal_teardown["quiet"] == [True]
      and not _mr_selfheal_teardown["finalize_line"].is_set()
      and not _mr_selfheal_script.restart_calls
      and not _mr_selfheal_rep.get("midrun_restart_events")
      and not _mr_selfheal_rep.get("fleet_down_finalize")
      and not _mr_selfheal_rep.get("interrupted")
      and not _mr_selfheal_rep.get("_unpulled")
      and len(_mr_selfheal_rows) == 12
      and not any(row.get("halt") for row in _mr_selfheal_rows)
      and set((_mr_selfheal_rep.get("watchdog") or {}).get(
          "states", {}).values()) == {"HEALTHY"},
      str((_mr_selfheal_teardown["ports"],
           _mr_selfheal_teardown["finalize_samples"],
           _mr_selfheal_teardown["quiet"],
           _mr_selfheal_teardown["finalize_line"].is_set(),
           _mr_selfheal_script.restart_calls,
           _mr_selfheal_rep.get("fleet_down_finalize"))))


def _run_fleetdown_fallback(decision, attempted, *, raise_callback=False,
                            enabled=True):
    script = _MidrunScript(
        kill_at=1, slow_s=0.0, kill_ports=_mr_fleet_ports)

    def callback(batch):
        normalized = [int(p) for p in batch]
        with script.lock:
            script.restart_calls.append(normalized)
            script.restart_threads.append(threading.current_thread().name)
        if raise_callback:
            raise RuntimeError("scripted fleet-down callback failure")
        return {
            port: {"ok": False, "attempted": bool(attempted),
                   "decision": decision, "reason": f"scripted {decision}"}
            for port in normalized
        }

    script.restart = callback
    report = _with_midrun_constants(
        0.25, 0.1, lambda: _run_midrun_gate_case(
            script, 12, fleet_grace_s=0.25), fleetdown=enabled)
    return script, report


_mr_expected_seal_debt = {f"MRG{i:02d} 1m" for i in range(12)}
_mr_expected_unpulled = {(f"MRG{i:02d}", "1m") for i in range(12)}


def _fleetdown_debt_signature(report):
    watchdog_states = (report.get("watchdog") or {}).get("states") or {}
    return {
        "fleet_down_finalize": report.get("fleet_down_finalize"),
        "interrupted": report.get("interrupted"),
        "seal_pending": tuple(sorted(report.get("seal_pending") or [])),
        "unpulled": tuple(sorted(
            tuple(item) for item in report.get("_unpulled") or [])),
        "watchdog_states": tuple(sorted(watchdog_states.items())),
    }


_mr_fallback_debts = []
for _mr_fallback_name, _mr_fallback_decision, _mr_fallback_attempted, \
        _mr_fallback_raises in (
            ("decline", "not_now_declined", False, False),
            ("render failure", "popup_render_error", False, False),
            ("launcher failure", "launcher_failed", True, False),
            ("callback failure", "callback_error", False, True)):
    _mr_fallback_script, _mr_fallback_rep = _run_fleetdown_fallback(
        _mr_fallback_decision, _mr_fallback_attempted,
        raise_callback=_mr_fallback_raises)
    _mr_fallback_events = (
        _mr_fallback_rep.get("midrun_restart_events") or [])
    _mr_fallback_debts.append(_fleetdown_debt_signature(_mr_fallback_rep))
    check(f"mid-run fleet-down restart: {_mr_fallback_name} offers once then "
          "uses existing finalize debt",
          _mr_fallback_script.restart_calls == [_mr_fleet_ports]
          and _mr_fallback_script.restart_threads == ["ibkr-midrun-restart"]
          and _mr_fallback_rep.get("fleet_down_finalize") is True
          and _mr_fallback_rep.get("interrupted") is True
          and set(_mr_fallback_rep.get("seal_pending") or [])
          == _mr_expected_seal_debt
          and {tuple(item) for item in _mr_fallback_rep.get("_unpulled") or []}
          == _mr_expected_unpulled
          and set(((_mr_fallback_rep.get("watchdog") or {}).get(
              "states") or {}).values()) == {"DEAD"}
          and len(_mr_fallback_events) == len(_mr_fleet_ports)
          and all(event.get("phase") == "fleet_down"
                  and event.get("decision") == _mr_fallback_decision
                  and event.get("attempted") is _mr_fallback_attempted
                  and event.get("final_probe") == "down"
                  and event.get("ok") is False
                  for event in _mr_fallback_events),
          str((_mr_fallback_script.restart_calls,
               _mr_fallback_rep.get("fleet_down_finalize"),
               _mr_fallback_events)))

_mr_fleet_off_script, _mr_fleet_off_rep = _run_fleetdown_fallback(
    "not_now_declined", False, enabled=False)
_mr_fleet_off_debt = _fleetdown_debt_signature(_mr_fleet_off_rep)
check("mid-run fleet-down restart: feature flag off preserves no-offer finalize",
      not _mr_fleet_off_script.restart_calls
      and _mr_fleet_off_rep.get("fleet_down_finalize") is True
      and not _mr_fleet_off_rep.get("midrun_restart_events"),
      str((_mr_fleet_off_script.restart_calls,
           _mr_fleet_off_rep.get("fleet_down_finalize"))))
check("mid-run fleet-down restart: every failed offer preserves baseline debt",
      _mr_fallback_debts
      and all(debt == _mr_fleet_off_debt for debt in _mr_fallback_debts)
      and set(_mr_fleet_off_rep.get("seal_pending") or [])
      == _mr_expected_seal_debt
      and {tuple(item) for item in _mr_fleet_off_rep.get("_unpulled") or []}
      == _mr_expected_unpulled
      and set(((_mr_fleet_off_rep.get("watchdog") or {}).get(
          "states") or {}).values()) == {"DEAD"},
      str((_mr_fallback_debts, _mr_fleet_off_debt)))

print("=== [progress heartbeat] per-port within-series counters ===========")

_prog_line = sk._progress_msg("HB", "1m", 1, 3, 390)
_series_line = "[1/3] HB 1m ok +390 bars"
_series_re = re.compile(r"^\[\d+/\d+\]\s+(.*)$")
_parsed = sk._parse_progress_msg(_prog_line)
check("progress heartbeat: parser extracts ticker/counts/bars",
      _parsed == {"ticker": "HB", "interval": "1m", "phase": "",
                  "done": 1, "total": 3, "bars": 390},
      str(_parsed))
check("progress heartbeat: regexes are mutually exclusive",
      bool(sk._PROGRESS_RE.match(_prog_line))
      and not _series_re.match(_prog_line)
      and _series_re.match(_series_line)
      and not sk._PROGRESS_RE.match(_series_line))
_hb_out = []
_hb_clock = [10.0]
_hb_throttle = {"last": float("-inf")}
_hb_now = lambda: _hb_clock[0]
_first = sk._maybe_emit_progress(
    _hb_out.append, "HB", "1m", 1, 3, 100, _hb_throttle, now_fn=_hb_now)
_second = sk._maybe_emit_progress(
    _hb_out.append, "HB", "1m", 2, 3, 200, _hb_throttle, now_fn=_hb_now)
_hb_clock[0] += sk.PROGRESS_HEARTBEAT_MIN_S
_third = sk._maybe_emit_progress(
    _hb_out.append, "HB", "1m", 2, 3, 200, _hb_throttle, now_fn=_hb_now)
check("progress heartbeat: throttle suppresses immediate duplicate",
      _first and not _second and _third and len(_hb_out) == 2,
      str(_hb_out))

_hb_root = fresh_root()
_hb_days = {MON: [ib_stub(b) for b in vendor_bars(MON, 60, 50.0)]}
_hb_progress = []
_hb_states = []


def _hb_factory(_host, _ports):
    return lambda: FakeAdapter(
        _hb_days, head=datetime.combine(MON, sk.RTH_OPEN, tzinfo=NY)
        .astimezone(timezone.utc))


_hb_rep = sk.nightly_gap_fill_parallel(
    _hb_root, [("HB", "1m")], [2000, 3000],
    progress=_hb_progress.append, today=MON, adapter_factory=_hb_factory,
    port_status=lambda p, info: _hb_states.append((p, dict(info))))
_hb_detail_states = [s for _p, s in _hb_states
                     if s.get("days_done") and s.get("detail")]
_hb_done_states = [s for _p, s in _hb_states if s.get("state") == "done"]
check("progress heartbeat: parallel parser updates port detail",
      any(s.get("ticker") == "HB" and s.get("detail") == "1/1 days"
          and s.get("bars", 0) > 0 for s in _hb_detail_states),
      str(_hb_states))
check("progress heartbeat: PROGRESS is not forwarded to visible log",
      not any("PROGRESS" in str(x) for x in _hb_progress),
      str(_hb_progress))
check("progress heartbeat: series completion resets detail fields",
      bool(_hb_done_states)
      and all((s.get("days_done"), s.get("days_total"), s.get("bars"),
               s.get("detail")) == (0, 0, 0, "") for s in _hb_done_states),
      str(_hb_done_states))
check("progress heartbeat: fake parallel run completed",
      isinstance(_hb_rep, dict) and not _hb_rep.get("cancelled")
      and _hb_rep.get("totals", {}).get("added", 0) > 0,
      str(_hb_rep))

print("=== [post ticker] Add Stocks check/probe phase ordering ============")

_post_root = fresh_root()
_post_days = {MON: [ib_stub(b) for b in vendor_bars(MON, 60, 70.0)]}
_post_events = []
_post_objects = []
_post_starts = []


def _post_factory(_host, _ports):
    return lambda: FakeAdapter(
        _post_days, head=datetime.combine(MON, sk.RTH_OPEN, tzinfo=NY)
        .astimezone(timezone.utc))


def _post_progress(message):
    if str(message).startswith("["):
        _post_events.append("fill")


def _post_check(adapter, pacer, ticker, _rows, _seed):
    _post_events.append(f"check:{ticker}")
    _post_objects.append((ticker, adapter, pacer))
    return {"selection": {"day": MON.isoformat()}}


def _post_probe(adapter, pacer, ticker, selection):
    _post_events.append(f"probe:{ticker}")
    _post_objects.append((ticker, adapter, pacer))
    return {"ticker": ticker, "day": selection["day"], "verdict": "MATCH",
            "pass_equivalent": True, "request_count": 1,
            "reused_request_count": 0, "network": True, "written": False}


_post_coord = lsp.AddStockProbeCoordinator(
    [("PSTA", "1m"), ("PSTB", "1m")], check_fn=_post_check,
    probe_fn=_post_probe, seed=23)
_post_rep = sk.nightly_gap_fill_parallel(
    _post_root, [("PSTA", "1m"), ("PSTB", "1m")], [2000],
    progress=_post_progress, today=MON, adapter_factory=_post_factory,
    post_pipeline=_post_coord,
    on_series_start=lambda t, iv: _post_starts.append((t, iv)))
_post_result = _post_coord.result()
_first_check = next(i for i, value in enumerate(_post_events)
                    if value.startswith("check:"))
check("post ticker: every unclaimed fill runs before check/probe work",
      _post_events[:_first_check].count("fill") == 2,
      str(_post_events))
check("post ticker: one check and one probe complete per ticker",
      _post_result["completed_count"] == 2
      and _post_result["request_count"] == 2
      and sum(value.startswith("check:") for value in _post_events) == 2
      and sum(value.startswith("probe:") for value in _post_events) == 2,
      str((_post_events, _post_result)))
check("post ticker: parallel workers propagate the series-start observer",
      sorted(_post_starts) == [("PSTA", "1m"), ("PSTB", "1m")],
      str(_post_starts))
check("post ticker: check/probe borrow the same worker adapter and pacer",
      len({id(value[1]) for value in _post_objects}) == 1
      and len({id(value[2]) for value in _post_objects}) == 1,
      str(_post_objects))
check("post ticker: one-port pipeline still returns a complete report",
      isinstance(_post_rep, dict) and not _post_rep.get("cancelled")
      and _post_rep.get("totals", {}).get("added", 0) > 0,
      str(_post_rep))
_post_rep["spot_probe"] = dict(_post_result, report="probe-report.json")
_post_summary = sk.summarize_report(_post_rep)
check("post ticker: GUI summary surfaces aggregate and report path",
      any("WS8 spot probes: 2 ticker(s)" in line for line in _post_summary)
      and any("WS8 report: probe-report.json" in line
              for line in _post_summary),
      str(_post_summary))

print("=== [ReusableAdapter] one connection across many lookups ============")


class _FakeConn:
    def __init__(self, log):
        self.log = log
        self._connected = True
        log.append("connect")

    def is_connected(self):
        return self._connected

    def search(self, text):
        self.log.append(f"search:{text}")
        return [{"symbol": text.upper()}]

    def disconnect(self):
        self._connected = False
        self.log.append("disconnect")


_ralog = []
_ra = sk.ReusableAdapter(lambda: _FakeConn(_ralog))
check("ReusableAdapter is its own factory", _ra() is _ra)
_r1 = _ra.search("ko")
_r2 = _ra.search("aapl")
_ra.disconnect()                         # engine per-call finally — MUST no-op
_r3 = _ra.search("msft")
check("ReusableAdapter connects ONCE across 3 searches",
      _ralog.count("connect") == 1, str(_ralog))
check("ReusableAdapter.disconnect() is a no-op (shared link survives)",
      _ralog.count("disconnect") == 0)
check("searches delegate to the live connection",
      [r[0]["symbol"] for r in (_r1, _r2, _r3)] == ["KO", "AAPL", "MSFT"])
_ra.close()
check("close() really disconnects the shared link",
      _ralog.count("disconnect") == 1)
_ralog.clear()
_ra2 = sk.ReusableAdapter(lambda: _FakeConn(_ralog))
_ra2.search("x")                         # connect #1
_ra2._a._connected = False               # simulate a dropped link
_ra2.search("y")                         # must reconnect
check("a dropped connection is re-established on next use",
      _ralog.count("connect") == 2, str(_ralog))
check("the stale link is DISCONNECTED before reconnecting (no leak / id clash)",
      _ralog.count("disconnect") == 1, str(_ralog))
_ra3log = []
_ra3 = sk.ReusableAdapter(lambda: _FakeConn(_ra3log))
_ra3.use_rth = False                     # non-underscore WRITE
check("__setattr__ delegates a write to the LIVE connection (not the wrapper)",
      getattr(_ra3._a, "use_rth", None) is False, str(_ra3log))

print("=== [series_first_dt] earliest stored bar (extended-hours cap) ======")
_man_ff = {"intervals": {"1m": {"months": {
    "2025-03": {"status": "present", "first": "3/3/2025 9:30:00",
                "last": "3/31/2025 15:59:00"},
    "2025-01": {"status": "present", "first": "1/2/2025 9:30:00",
                "last": "1/31/2025 15:59:00"},
    "2025-02": {"status": "missing"},          # not present -> skipped
}}}}
_ff = sk.series_first_dt(_man_ff, "1m")
_fl = sk.series_last_dt(_man_ff, "1m")
check("series_first_dt returns the EARLIEST present 'first' bar",
      _ff is not None and _ff.date() == date(2025, 1, 2), str(_ff))
check("series_first_dt < series_last_dt for the same series",
      _ff is not None and _fl is not None and _ff < _fl, f"{_ff}..{_fl}")
check("series_first_dt: empty/None manifest -> None",
      sk.series_first_dt({}, "1m") is None
      and sk.series_first_dt(None, "1m") is None)
check("series_first_dt: unknown interval -> None",
      sk.series_first_dt(_man_ff, "5m") is None)

print("=== [manifest merge-save] interval-split sibling safety ============")


def _seed_manifest(ticker, iv):
    m = ss.new_manifest(ticker, ticker)
    ss.manifest_months(m, iv)["2025-01"] = {
        "status": "present", "rows": 100,
        "first": "1/2/2025 9:30:00", "last": "1/31/2025 15:59:00"}
    return m


_mdir = fresh_root() / "AAA"
_mdir.mkdir(parents=True)
_mlock = threading.Lock()
# worker A owns '1m', B owns '1m-pre'. Saved under the same per-ticker lock: B
# must MERGE into A's on-disk section, not clobber it.
sk._save_manifest_safely(_mdir, _seed_manifest("AAA", "1m"), {"notes": []},
                         "1m", _mlock)
sk._save_manifest_safely(_mdir, _seed_manifest("AAA", "1m-pre"), {"notes": []},
                         "1m-pre", _mlock)
_mf = ss.load_manifest(_mdir) or {}
check("merge-save: sequential sibling sections BOTH survive",
      {"1m", "1m-pre"} <= set(_mf.get("intervals", {})))

# truly concurrent: 3 threads hammer the SAME ticker, different sections, 25
# saves each. A blind overwrite would drop sections; lock+merge keeps all 3.
_cdir = fresh_root() / "BBB"
_cdir.mkdir(parents=True)
_clock = threading.Lock()


def _hammer(iv):
    m = _seed_manifest("BBB", iv)
    for _ in range(25):
        sk._save_manifest_safely(_cdir, m, {"notes": []}, iv, _clock)


_ts = [threading.Thread(target=_hammer, args=(iv,))
       for iv in ("1m", "1m-pre", "1m-post")]
for _t in _ts:
    _t.start()
for _t in _ts:
    _t.join()
_cf = ss.load_manifest(_cdir) or {}
check("merge-save: 3 CONCURRENT sibling writers -> all 3 sections survive",
      {"1m", "1m-pre", "1m-post"} <= set(_cf.get("intervals", {})))
# The unscoped internal path now receives the same fresh-merge fence, so a
# stale whole-manifest snapshot also preserves sibling interval sections.
sk._save_manifest_safely(_cdir, _seed_manifest("BBB", "1m"), {"notes": []})
_cf2 = ss.load_manifest(_cdir) or {}
check("unscoped save fresh-merges instead of dropping sibling sections",
      {"1m", "1m-pre", "1m-post"}
      <= set(_cf2.get("intervals", {})))


print("=== [date split] month chunking + same-interval merge ===============")

_ds_days = []
_d = date(2024, 1, 1)
while _d <= date(2024, 12, 31):
    if _d.weekday() < 5:
        _ds_days.append(_d)
    _d += timedelta(days=1)
_ds_chunks = sk.partition_series_by_months(_ds_days, 3, min_months=3)
_ds_flat = [m for c in _ds_chunks for m in c["months"]]
_ds_months = sorted({(d.year, d.month) for d in _ds_days})
check("date split chunker: contiguous disjoint month coverage",
      _ds_flat == _ds_months and len(_ds_flat) == len(set(_ds_flat)),
      str(_ds_chunks))
check("date split chunker: min-month threshold respected",
      len(_ds_chunks) == 3
      and all(len(c["months"]) >= 3 for c in _ds_chunks),
      str(_ds_chunks))
check("date split chunker: too-light series falls back to one chunk",
      len(sk.partition_series_by_months(_ds_days[:25], 4, min_months=3)) == 1)


def _month_manifest(ticker, iv, key):
    m = ss.new_manifest(ticker, ticker)
    ss.manifest_months(m, iv)[key] = {
        "status": "present", "rows": 10,
        "first": f"{int(key[5:7])}/1/{key[:4]} 9:30:00",
        "last": f"{int(key[5:7])}/1/{key[:4]} 15:59:00"}
    return m


_ds_mdir = fresh_root() / "DSM"
_ds_mdir.mkdir(parents=True)
_ds_lock = threading.Lock()
sk._save_manifest_safely(_ds_mdir, _month_manifest("DSM", "1m", "2024-01"),
                         {"notes": []}, "1m", _ds_lock, date_split=True)
sk._save_manifest_safely(_ds_mdir, _month_manifest("DSM", "1m", "2024-02"),
                         {"notes": []}, "1m", _ds_lock, date_split=True)
_ds_mf = ss.load_manifest(_ds_mdir) or {}
check("date split merge-save: same interval sibling months both survive",
      set(ss.manifest_months(_ds_mf, "1m")) >= {"2024-01", "2024-02"})

_ds_state_dir = fresh_root() / "DSSTATE"
_ds_state_dir.mkdir(parents=True)
_ds_state_lock = threading.Lock()
_ds_state_base = _month_manifest("DSSTATE", "1m", "2024-01")
_ds_state_base["intervals"]["1m"]["backfill_incomplete"] = False
ss.save_manifest(_ds_state_dir, _ds_state_base)
_ds_state_owner = ss.load_manifest(_ds_state_dir)
sk._backfill_begin(_ds_state_owner, "1m", date(2011, 6, 1))
sk._backfill_try_seal(
    {"notes": []}, _ds_state_owner, "1m", date(2024, 5, 7))
sk._save_manifest_safely(
    _ds_state_dir, _ds_state_owner, {"notes": []}, "1m", _ds_state_lock,
    date_split=True, interval_state_owner=True)
_ds_state_saved = ss.load_manifest(_ds_state_dir)
check("date split merge-save: earliest chunk persists its backfill seal state",
      sk._backfill_marker(_ds_state_saved, "1m")
      and _ds_state_saved["intervals"]["1m"].get(
          "backfill_incomplete_reason") == "source_history_remaining")

# A later non-earliest chunk still carries the initial false marker in memory;
# it must not overwrite the owner's fresher interval-level decision.
sk._save_manifest_safely(
    _ds_state_dir, _ds_state_base, {"notes": []}, "1m", _ds_state_lock,
    date_split=True, interval_state_owner=False)
_ds_state_after_stale = ss.load_manifest(_ds_state_dir)
check("date split merge-save: non-owner chunk preserves the owner's seal state",
      sk._backfill_marker(_ds_state_after_stale, "1m")
      and _ds_state_after_stale["intervals"]["1m"].get(
          "backfill_served_earliest") == "2011-06-01")

# The owner can later deepen within tolerance and pop the prior short-history
# verdict. Its save must remove those absent scalars without losing unions that
# a sibling committed after the owner loaded its snapshot.
_ds_state_clean_owner = ss.load_manifest(_ds_state_dir)
_ds_state_sibling = copy.deepcopy(_ds_state_clean_owner)
ss.manifest_months(_ds_state_sibling, "1m")["2024-02"] = {
    "status": "present", "rows": 10,
    "first": "2/1/2024 9:30:00", "last": "2/1/2024 15:59:00"}
_ds_state_sibling["intervals"]["1m"]["verified_absent"] = ["2024-02-15"]
sk._save_manifest_safely(
    _ds_state_dir, _ds_state_sibling, {"notes": []}, "1m", _ds_state_lock,
    date_split=True, interval_state_owner=False)
sk._backfill_begin(_ds_state_clean_owner, "1m", date(2024, 4, 1))
_ds_state_clean_sealed = sk._backfill_try_seal(
    {"notes": []}, _ds_state_clean_owner, "1m", date(2024, 5, 7))
sk._save_manifest_safely(
    _ds_state_dir, _ds_state_clean_owner, {"notes": []}, "1m",
    _ds_state_lock, date_split=True, interval_state_owner=True)
_ds_state_clean_saved = ss.load_manifest(_ds_state_dir)
_ds_state_clean_sec = _ds_state_clean_saved["intervals"]["1m"]
check("date split merge-save: owner seal removes stale state and keeps unions",
      _ds_state_clean_sealed
      and not sk._backfill_marker(_ds_state_clean_saved, "1m")
      and "under_backfilled" not in _ds_state_clean_sec
      and "backfill_incomplete_reason" not in _ds_state_clean_sec
      and _ds_state_clean_sec.get("backfill_seal", {}).get("sealed") is True
      and set(ss.manifest_months(_ds_state_clean_saved, "1m"))
      >= {"2024-01", "2024-02"}
      and _ds_state_clean_sec.get("verified_absent") == ["2024-02-15"])

# The sibling snapshot still contains the pre-seal owner state. Saving it after
# the owner cleaned the section may add its new unions, but must never restore
# stale seal scalars beside the sealed verdict.
ss.manifest_months(_ds_state_sibling, "1m")["2024-03"] = {
    "status": "present", "rows": 10,
    "first": "3/1/2024 9:30:00", "last": "3/1/2024 15:59:00"}
_ds_state_sibling["intervals"]["1m"]["verified_absent"].append("2024-03-15")
sk._save_manifest_safely(
    _ds_state_dir, _ds_state_sibling, {"notes": []}, "1m", _ds_state_lock,
    date_split=True, interval_state_owner=False)
_ds_state_after_late_sibling = ss.load_manifest(_ds_state_dir)
_ds_state_after_late_sec = _ds_state_after_late_sibling["intervals"]["1m"]
check("date split merge-save: stale non-owner cannot restore seal state",
      not sk._backfill_marker(_ds_state_after_late_sibling, "1m")
      and "under_backfilled" not in _ds_state_after_late_sec
      and "backfill_incomplete_reason" not in _ds_state_after_late_sec
      and _ds_state_after_late_sec.get("backfill_seal", {}).get("sealed") is True
      and set(ss.manifest_months(_ds_state_after_late_sibling, "1m"))
      >= {"2024-01", "2024-02", "2024-03"}
      and _ds_state_after_late_sec.get("verified_absent")
      == ["2024-02-15", "2024-03-15"])


_ds_root = fresh_root()
_ds_existing = seed_series(_ds_root, "DSP", date(2023, 12, 29), 60, 100.0)
_ds_feed = {}
_d = date(2023, 12, 29)
while _d <= date(2024, 6, 28):
    if _d.weekday() < 5:
        _base = 100.0 + (_d - date(2023, 12, 29)).days * 0.2
        _ds_feed[_d] = [ib_stub(b) for b in vendor_bars(_d, 60, _base)]
    _d += timedelta(days=1)
_ds_feed[date(2023, 12, 29)] = [ib_stub(b) for b in _ds_existing]
_ds_states = []


def _ds_factory(_host, _ports):
    return lambda: FakeAdapter(_ds_feed, head=datetime(2023, 12, 29, 9, 30,
                                                      tzinfo=NY))


_ds_rep = sk.nightly_gap_fill_parallel(
    _ds_root, [("DSP", "1m")], [2000, 3000, 4000],
    today=date(2024, 6, 28), adapter_factory=_ds_factory,
    port_status=lambda p, info: _ds_states.append((p, dict(info))),
    allow_date_split=True, date_split_min_months=2)
_ds_manifest = ss.load_manifest(_ds_root / "DSP") or {}
_ds_have = set(ss.manifest_months(_ds_manifest, "1m"))
_ds_want = {f"2024-{m:02d}" for m in range(1, 7)} | {"2023-12"}
_ds_series = _ds_rep.get("series", [])
check("date split parallel: split jobs were used",
      _ds_rep.get("date_split", {}).get("jobs", 0) >= 2
      and sum(1 for s in _ds_series if s.get("date_split")) >= 2,
      str(_ds_rep.get("date_split")))
check("date split parallel: manifest has every split month",
      _ds_want <= _ds_have, f"missing={sorted(_ds_want - _ds_have)}")
check("date split parallel: no worker halted",
      not any(s.get("halt") for s in _ds_series),
      str([s.get("halt") for s in _ds_series if s.get("halt")]))

print("=== [adaptive concurrency] AIMD decision + swap + OrEvent ==========")

# _OrEvent: is_set() is the OR; None members are ignored; only .is_set() used.
_e1, _e2 = threading.Event(), threading.Event()
_oe = sk._OrEvent(None, _e1, _e2)
check("OrEvent: clear when all clear", _oe.is_set() is False)
_e2.set()
check("OrEvent: set when any set", _oe.is_set() is True)
_e2.clear()
check("OrEvent: clear again after reset", _oe.is_set() is False)

D = sk._adaptive_decide
# args: (n_active, retrying_now, recon_in_window, clean_for, cooling,
#        parked, active_working)  -- recon_in_window = DISTINCT struggling ports
check("decide: quiet, nothing to do -> none",
      D(5, 0, 0, 10.0, False, 0, 5) == "none")
check("decide: single hiccup at 5 active (need >=3) -> none",
      D(5, 1, 0, 0.0, False, 0, 5) == "none")
check("decide: 3 of 5 retrying -> drop1",
      D(5, 3, 0, 0.0, False, 0, 5) == "drop1")
# distinct-port rate mark: ONE struggling port must NOT trip a drop; >=2 does.
check("decide: a single struggling port (rate=1) never drops",
      D(3, 0, 1, 0.0, False, 0, 3) == "none")
check("decide: rate mark = 2 distinct struggling ports -> drop1",
      D(5, 0, 2, 0.0, False, 0, 5) == "drop1")
check("decide: severe (frac AND >=3 distinct ports) with room -> drop2",
      D(5, 3, 3, 0.0, False, 0, 5) == "drop2")
check("decide: severe but dropping 2 would breach floor -> drop1",
      D(2, 2, 3, 0.0, False, 0, 2) == "drop1")
check("decide: at floor (1 active) never drops",
      D(1, 1, 3, 0.0, False, 0, 1) == "none")
check("decide: small fleet needs >=2 retrying, not 1 (n=2,r=1) -> none",
      D(2, 1, 0, 0.0, False, 0, 2) == "none")
check("decide: small fleet both retrying (n=2,r=2) -> drop1",
      D(2, 2, 0, 0.0, False, 0, 2) == "drop1")
check("decide: cooling suppresses a drop",
      D(5, 3, 3, 0.0, True, 0, 5) == "none")
check("decide: clean long enough + parked exist -> add",
      D(3, 0, 0, sk.ADAPT_ADD_CLEAN_S + 1, False, 2, 3) == "add")
check("decide: clean but not long enough -> none",
      D(3, 0, 0, sk.ADAPT_ADD_CLEAN_S - 5, False, 2, 3) == "none")
check("decide: clean+parked but cooling -> none (add waits out cooldown)",
      D(3, 0, 0, sk.ADAPT_ADD_CLEAN_S + 1, True, 2, 3) == "none")
# forced progress overrides cooldown AND the floor: nothing working but parked
# work remains -> must release or join() would deadlock at the tail.
check("decide: no active worker + parked work -> release (beats cooling)",
      D(0, 0, 0, 0.0, True, 3, 0) == "release")
check("decide: no active worker but nothing parked -> none",
      D(0, 0, 0, 0.0, False, 0, 0) == "none")

print("=== [25] disk preflight + WRITE FAILED surfacing ==================")
# --- _disk_preflight: hard floor aborts; soft shortfall only warns ----------
import shutil as _shutil
_real_du = _shutil.disk_usage


def _patch_du(free_bytes):
    _shutil.disk_usage = lambda _p: SimpleNamespace(
        total=free_bytes, used=0, free=free_bytes)


try:
    _patch_du(10 * 1024 ** 3)                     # 10 GB free, tiny run
    rep = {}
    ok = sk._disk_preflight(fresh_root(), 3, None, rep)
    check("preflight: ample free space -> proceeds, no abort, records meta",
          ok is True and "aborted" not in rep
          and rep.get("preflight", {}).get("free_bytes") == 10 * 1024 ** 3)

    _patch_du(100 * 1024 ** 2)                    # 100 MB free -> below floor
    rep = {}
    ok = sk._disk_preflight(fresh_root(), 3, None, rep)
    check("preflight: below the hard floor -> aborts before connecting",
          ok is False and "disk preflight" in rep.get("aborted", ""))

    # free between floor and the per-series estimate -> WARN (note) but proceed
    _patch_du(2 * 1024 ** 3)                      # 2 GB free
    rep = {}
    ok = sk._disk_preflight(fresh_root(), 500, None, rep)   # est ~12.5 GB
    check("preflight: low (under estimate, over floor) -> warns, proceeds",
          ok is True and "aborted" not in rep
          and any("LOW DISK" in n for n in rep.get("notes", [])))

    # the real run path aborts cleanly when the floor is breached
    _patch_du(50 * 1024 ** 2)
    rep = _offline_gap_fill_body(fresh_root(), [("AAA", "1m")],
                      adapter_factory=lambda: FakeAdapter({}),
                      pacer=fast_pacer(FakeClock()), today=date(2024, 7, 12))
    check("preflight: gap_fill aborts (no fetch) when disk is below floor",
          "disk preflight" in rep.get("aborted", "")
          and rep.get("series") == [])
finally:
    _shutil.disk_usage = _real_du

# --- summarize_report: a WRITE FAILED month is surfaced LOUDLY --------------
wf_report = {"run": "ibkr-x", "account": "DU1", "port": 2000,
             "totals": {"added": 0, "written": 0, "dup_existing": 0,
                        "conflicts": 0, "requests": 1, "write_failed": 1},
             "series": [{"ticker": "AAA", "interval": "1m", "added": 0,
                         "written": 0, "requests": 1, "halt": None,
                         "months": {"2024-07": {"status":
                                    "WRITE FAILED: [Errno 28] No space left"}}}]}
lines = sk.summarize_report(wf_report)
blob = "\n".join(lines)
check("summarize: WRITE FAILED months produce a loud run-level line",
      "WRITE FAILED" in blob and "could not be saved" in blob.lower())
check("summarize: the affected series is marked, not labelled plain 'ok'",
      any("WRITE-FAIL AAA 1m" in ln for ln in lines)
      and not any(ln.startswith("  ok        AAA 1m") for ln in lines))

# a clean report shows neither the loud line nor the per-series marker
clean_report = {"run": "ibkr-y", "account": "DU1", "port": 2000,
                "totals": {"added": 60, "written": 1, "dup_existing": 0,
                           "conflicts": 0, "requests": 1, "write_failed": 0},
                "series": [{"ticker": "BBB", "interval": "1m", "added": 60,
                            "written": 1, "requests": 1, "halt": None,
                            "months": {"2024-07": {"status": "written"}}}]}
cblob = "\n".join(sk.summarize_report(clean_report))
check("summarize: a clean run has NO write-failed noise",
      "WRITE FAILED" not in cblob and "WRITE-FAIL" not in cblob)

log_blob = run_log.format_run_log({
    "run": "ibkr-notes", "account": "DU1", "port": 2000,
    "totals": {"added": 0, "written": 0, "dup_existing": 0,
               "conflicts": 0, "requests": 1, "halted_series": 1},
    "series": [{"ticker": "XOM", "interval": "1d", "halt": "contract rejected",
                "notes": ["conId DIVERGENCE: old 13977 vs fresh 895178251",
                          "retrying once with current conId 895178251"]}]})
check("P2: run_log renders per-series notes list, including halted rows",
      "conId DIVERGENCE" in log_blob
      and "retrying once with current conId" in log_blob,
      log_blob)

print("=== [26] kind + daily interval tokens ============================")
check("base_interval strips kind+session",
      ss.base_interval("1m-iv") == "1m" and ss.base_interval("1d-hvol") == "1d"
      and ss.base_interval("1m-bidask-pre") == "1m"
      and ss.base_interval("1m-post") == "1m" and ss.base_interval("1m") == "1m")
check("kind_of reads the kind (default empty=TRADES)",
      ss.kind_of("1m-iv") == "iv" and ss.kind_of("1d-hvol") == "hvol"
      and ss.kind_of("1m-bidask-pre") == "bidask" and ss.kind_of("1m") == ""
      and ss.kind_of("1m-pre") == "")
check("session_of still resolves with a kind present",
      ss.session_of("1m-bidask-pre") == "pre" and ss.session_of("1d-hvol") == "rth"
      and ss.session_of("1m-iv") == "rth")
check("with_kind / parse_interval round-trip",
      ss.with_kind("1m", "iv") == "1m-iv" and ss.with_kind("1m", "") == "1m"
      and ss.parse_interval("1m-bidask-pre") == ("1m", "bidask", "pre")
      and ss.parse_interval("1d-hvol") == ("1d", "hvol", "rth"))
check("INTERVAL_RE accepts daily + kind tokens, rejects junk",
      bool(ss.INTERVAL_RE.match("1d")) and bool(ss.INTERVAL_RE.match("1m-iv"))
      and bool(ss.INTERVAL_RE.match("1d-hvol"))
      and bool(ss.INTERVAL_RE.match("1m-bidask-pre"))
      and not ss.INTERVAL_RE.match("1m-iv-bogus")
      and not ss.INTERVAL_RE.match("1x"))
check("_BAR_SIZES has daily; base lookup works for kind tokens",
      sk._BAR_SIZES[ss.base_interval("1d-hvol")] == ("1 day", None)
      and sk._BAR_SIZES[ss.base_interval("1m-iv")] == ("1 min", None))
check("_what_to_show maps kind -> whatToShow",
      sk._what_to_show("1m") == "TRADES"
      and sk._what_to_show("1m-iv") == "OPTION_IMPLIED_VOLATILITY"
      and sk._what_to_show("1d-hvol") == "HISTORICAL_VOLATILITY"
      and sk._what_to_show("1m-bidask") == "BID_ASK")
# _fetch_request threads a kind's whatToShow to adapter.fetch (5-arg path);
# TRADES default keeps the legacy 4-arg call so existing adapters are untouched.
_end = datetime(2024, 7, 12, 16, tzinfo=NY)
_fa_iv = FakeAdapter({})
sk._fetch_request(_fa_iv, "C", _end, "1 D", "1 min", fast_pacer(FakeClock()),
                  threading.Event(), (lambda *a: None), (lambda: None), "AAA",
                  {"requests": 0}, "x", metered=False,
                  what_to_show="OPTION_IMPLIED_VOLATILITY")
check("_fetch_request passes a kind whatToShow to adapter.fetch",
      getattr(_fa_iv, "last_what", None) == "OPTION_IMPLIED_VOLATILITY")
_fa_tr = FakeAdapter({})
sk._fetch_request(_fa_tr, "C", _end, "1 D", "1 min", fast_pacer(FakeClock()),
                  threading.Event(), (lambda *a: None), (lambda: None), "AAA",
                  {"requests": 0}, "x", metered=False)
check("_fetch_request TRADES default keeps the 4-arg call (last_what=TRADES)",
      getattr(_fa_tr, "last_what", None) == "TRADES")
check("session_window: daily spans the whole day, intraday stays RTH",
      ss.session_window("1d-hvol") == ss.DAILY_WINDOW
      and ss.session_window("1d") == ss.DAILY_WINDOW
      and ss.session_window("1m") == ss.SESSION_WINDOWS["rth"])
_dbar = (datetime(2024, 7, 8, 0, 0, 0), 0.23, 0.24, 0.22, 0.23, 1)
check("daily ratio bar validates vs the daily window, fails RTH",
      ss.validate_bar(*_dbar, window=ss.session_window("1d-hvol")) == []
      and ss.validate_bar(*_dbar, window=ss.session_window("1m")) != [])
check("plan_gap: HVOL must be daily (1m-hvol rejected, 1d-hvol ok)",
      "daily-only" in (sk.plan_gap(fresh_root(), "AAA", "1m-hvol").get("error") or "")
      and not sk.plan_gap(fresh_root(), "AAA", "1d-hvol").get("error"))
check("combine: kind series are excluded from the combined-extended group",
      ss.kind_of("1m-iv") == "iv")  # guard relies on kind_of (combine loop skips it)

# === BUG-HUNT REGRESSIONS (w6heyhaug) — the REALISTIC bar shapes the FakeAdapter
# never produced: ib_async returns a datetime.DATE for daily bars (formatDate=2
# epoch-encodes only intraday) and a -1 VOLUME sentinel for computed (iv/hvol) kinds.
# A) daily tokens no longer KeyError('d') in the seconds-lookup chain.
check("_interval_seconds knows daily (1d -> 86400), session-bars >= 1",
      sk._interval_seconds("1d") == 86400
      and sk._expected_session_bars("1d-iv") >= 1
      and sk._expected_session_bars("1d-hvol") >= 1)
_ddt = datetime(2024, 3, 4, 0, 0, 0)
_ex = [(_ddt, 0.20, 0.21, 0.19, 0.20, 0)]
try:
    sk.entry_gate(_ex, _ex, "1d-iv", 0.20); _eg_ok = True
except KeyError:
    _eg_ok = False
try:
    sk.progress_summary({"ticker": "AAA", "interval": "1d-iv", "added": 5,
                         "stored_through": _ddt, "committed_through": _ddt.date()})
    _ps_ok = True
except KeyError:
    _ps_ok = False
check("entry_gate + progress_summary do not KeyError on a daily series",
      _eg_ok and _ps_ok)
# B) a DAILY date-only bar (the real ib_async shape) is STORED, dated right, vol 0.
_dday = date(2024, 6, 17)   # Monday
_dc = {"non_rth": 0, "invalid": 0, "outside_day": 0}
_dout = sk.split_session_bars(
    [SimpleNamespace(date=_dday, open=0.25, high=0.30, low=0.22, close=0.28,
                     volume=-1.0)], frozenset({_dday}), _dc, "1d-iv")
check("daily date-only IV bar (vol -1) kept, midnight-dated, vol coerced 0",
      _dc["invalid"] == 0 and _dout.get(_dday)
      and _dout[_dday][0][0] == datetime(2024, 6, 17, 0, 0, 0)
      and _dout[_dday][0][5] == 0)
# C) an intraday IV bar carrying the -1 volume sentinel is kept (coerced to 0).
_idt = datetime(2024, 6, 14, 14, 0, 0, tzinfo=timezone.utc)   # 10:00 ET Fri
_ic = {"non_rth": 0, "invalid": 0, "outside_day": 0}
_iout = sk.split_session_bars(
    [SimpleNamespace(date=_idt, open=0.23, high=0.25, low=0.21, close=0.24,
                     volume=-1.0)], frozenset({date(2024, 6, 14)}), _ic, "1m-iv")
check("intraday IV bar with vol=-1 sentinel kept (coerced 0)",
      _ic["invalid"] == 0 and _iout.get(date(2024, 6, 14))
      and _iout[date(2024, 6, 14)][0][5] == 0)
# LIVE-confirmed 2026-06-24: the demo serves vol=1.0 (NOT -1) for IV/HVOL — volume
# is meaningless for computed kinds, so ANY value normalizes to a consistent 0.
_ic2 = {"non_rth": 0, "invalid": 0, "outside_day": 0}
_iout2 = sk.split_session_bars(
    [SimpleNamespace(date=_idt, open=0.23, high=0.25, low=0.21, close=0.24,
                     volume=1.0)], frozenset({date(2024, 6, 14)}), _ic2, "1m-iv")
check("IV bar with the demo's vol=1.0 is also normalized to 0",
      _ic2["invalid"] == 0 and _iout2.get(date(2024, 6, 14))
      and _iout2[date(2024, 6, 14)][0][5] == 0)
# guards: the daily promotion + vol coercion must NOT leak to TRADES.
_tc = {"non_rth": 0, "invalid": 0, "outside_day": 0}
_tout = sk.split_session_bars(
    [SimpleNamespace(date=_dday, open=10.0, high=10.0, low=10.0, close=10.0,
                     volume=100)], frozenset({_dday}), _tc, "1m")
_nc = {"non_rth": 0, "invalid": 0, "outside_day": 0}
sk.split_session_bars(
    [SimpleNamespace(date=_idt, open=10.0, high=10.0, low=9.0, close=9.5,
                     volume=-1.0)], frozenset({date(2024, 6, 14)}), _nc, "1m")
check("daily promotion + vol coercion do NOT leak to TRADES",
      not _tout and _nc["invalid"] == 1)
# E) ratio kinds route around the price/volume gates (the join-gate false-halt fix).
check("ratio kinds are recognised for the gate bypass",
      ss.kind_of("1d-iv") in ss.RATIO_KINDS
      and ss.kind_of("1d-hvol") in ss.RATIO_KINDS)


# GOLD-STANDARD end-to-end: a daily IV series with the REAL ib_async bar shape
# (date objects + vol -1) AND a >1.9x day-over-day jump must STORE every settled
# session with NO false join-gate halt. Without the fixes this halts at the MON->
# TUE 2.25x jump with added==1; this is the regression the green suite once missed.
class _DailyRatioAdapter(FakeAdapter):
    def __init__(self, closes, **kw):
        super().__init__({}, **kw)
        self._closes = closes

    def fetch(self, contract, end_dt, duration, bar_size, what_to_show="TRADES"):
        self.fetch_calls.append((end_dt, duration, bar_size))
        self.last_what = what_to_show
        end_d = end_dt.date()
        return [SimpleNamespace(date=d, open=c, high=c * 1.05, low=c * 0.95,
                                close=c, volume=-1.0)             # the -1 sentinel
                for d, c in sorted(self._closes.items()) if d <= end_d]


_iv_closes = {MON: 0.20, TUE: 0.45, WED: 0.22, THU: 0.25, FRI: 0.50}   # 2.25x MON->TUE
_root_d = fresh_root()
_ad = _DailyRatioAdapter(_iv_closes, head=datetime(2024, 6, 17, tzinfo=NY))
_rs = run_fill(_root_d, [("AAA", "1d-iv")], _ad, today=FRI)["series"][0]
check("END-TO-END daily IV: stores settled sessions, no false halt, right whatToShow",
      not _rs.get("halt") and _rs["added"] >= 4
      and _ad.last_what == "OPTION_IMPLIED_VOLATILITY")

# FINISH-THE-MONTH pause: the shared `process` closure (serial AND pipelined)
# idles at a MONTH boundary with a clear 'fully written' note, after committing
# the finished month — never mid-month. (The "pause for add stocks" UX.)
class _NShotSet:
    """is_set() True for the first n calls, then False — lets the boundary pause
    engage + emit its note, then the wait loop exits at once (no real thread)."""
    def __init__(self, n):
        self._n = n

    def is_set(self):
        if self._n > 0:
            self._n -= 1
            return True
        return False

_psay, _pflush = [], []
_pause_meta = []


def _record_pause_meta(paused, info=None):
    _pause_meta.append((paused, dict(info or {})))


_pres = {"bars_fetched": 0, "_empty_days": [], "notes": [],
         "committed_through": None}
_pproc = sk._make_session_processor(
    _pres, "AAA", "1m", [], None, True, _psay.append,
    (lambda upto_month=None: _pflush.append(upto_month)), {}, [],
    [date(2024, 5, 31), date(2024, 6, 3)], pause=_NShotSet(2),
    cancel=None, on_pause=_record_pause_meta)
_pproc(date(2024, 5, 31),
       [(datetime(2024, 5, 31, 9, 30), 100.0, 100.5, 99.5, 100.2, 1000)], 0, 2)
_pproc(date(2024, 6, 3),
       [(datetime(2024, 6, 3, 9, 30), 100.2, 100.7, 99.8, 100.4, 1000)], 1, 2)
check("finish-month pause idles at the boundary with a 'fully written' note",
      any("fully written" in s and "2024-05" in s for s in _psay)
      and (2024, 6) in _pflush)   # the upto-June flush commits May before idling
check("finish-month pause reports last completed month to monitor",
      any(paused and info.get("last_month") == "2024-05"
          for paused, info in _pause_meta),
      str(_pause_meta))
# control: NO pause -> no idle note
_psay2 = []
_pp2 = sk._make_session_processor(
    {"bars_fetched": 0, "_empty_days": [], "notes": [], "committed_through": None},
    "AAA", "1m", [], None, True, _psay2.append, (lambda upto_month=None: None),
    {}, [], [date(2024, 5, 31), date(2024, 6, 3)], pause=None, cancel=None)
_pp2(date(2024, 5, 31),
     [(datetime(2024, 5, 31, 9, 30), 100.0, 100.5, 99.5, 100.2, 1000)], 0, 2)
_pp2(date(2024, 6, 3),
     [(datetime(2024, 6, 3, 9, 30), 100.2, 100.7, 99.8, 100.4, 1000)], 1, 2)
check("no pause -> no finish-month idle note",
      not any("fully written" in s for s in _psay2))

# F-PAUSE-1 (2026-07-20): a pending Pause must ALSO engage on a NO-DATA
# session — an empty/erroring stretch has no month transition, which let a
# fully empty series (NVR field case) ignore Pause from start to finish and
# let sparse stretches run a month past the press.
_esay, _emeta = [], []
_eres = {"bars_fetched": 0, "_empty_days": [], "notes": [],
         "committed_through": None}
_eproc = sk._make_session_processor(
    _eres, "NVR", "1m-iv", [], None, True, _esay.append,
    (lambda upto_month=None: None), {}, [],
    [date(2024, 5, 31), date(2024, 6, 3)], pause=_NShotSet(2),
    cancel=None, on_pause=lambda p, i=None: _emeta.append((p, dict(i or {}))))
_eproc(date(2024, 5, 31), [], 0, 2)
check("no-data pause: an empty session honors a pending Pause with a clear note",
      any("returned no data" in s and "holding" in s for s in _esay),
      str(_esay))
check("no-data pause: monitor callback fires with empty last_month pre-commit",
      any(p and i.get("last_month") == "" for p, i in _emeta), str(_emeta))
check("no-data pause: the empty day is still recorded for reception-completeness",
      _eres["_empty_days"] == [date(2024, 5, 31)], str(_eres["_empty_days"]))

# A same-month empty must NOT pause with a partial month still buffered. The
# first empty session in the NEXT month proves that prior month complete; it is
# committed before the hold and reported truthfully to the monitor.
_e2say, _e2meta, _e2flush, _e2committed = [], [], [], []
_e2buffer = {}


def _e2flush_fn(upto_month=None):
    _e2flush.append(upto_month)
    for ym in sorted(list(_e2buffer)):
        if upto_month is not None and ym >= upto_month:
            continue
        _e2committed.append(ym)
        _e2buffer.pop(ym)


_e2res = {"bars_fetched": 0, "_empty_days": [], "notes": [],
          "committed_through": None}
_e2proc = sk._make_session_processor(
    _e2res, "WST", "1m-iv", [], None, True, _e2say.append,
    _e2flush_fn, _e2buffer, [],
    [date(2024, 5, 30), date(2024, 5, 31), date(2024, 6, 3)],
    pause=_NShotSet(2), cancel=None,
    on_pause=lambda p, i=None: _e2meta.append((p, dict(i or {}))))
_e2proc(date(2024, 5, 30),
        [(datetime(2024, 5, 30, 9, 30), 0.20, 0.21, 0.19, 0.20, -1)], 0, 3)
_e2proc(date(2024, 5, 31), [], 1, 3)
check("no-data pause: same-month empty does not hold a partial month",
      not any("returned no data" in s for s in _e2say)
      and (2024, 5) in _e2buffer and not _e2committed,
      f"{_e2say} {_e2buffer} {_e2committed}")
_e2proc(date(2024, 6, 3), [], 2, 3)
check("no-data pause: later-month empty commits the prior month before holding",
      (2024, 6) in _e2flush and _e2committed == [(2024, 5)]
      and not _e2buffer and any("returned no data" in s for s in _e2say),
      f"{_e2say} {_e2flush} {_e2buffer} {_e2committed}")
check("no-data pause: monitor reports the month actually committed",
      any(p and i.get("last_month") == "2024-05" for p, i in _e2meta),
      f"{_e2say} {_e2meta}")

# cancel during a no-data hold exits promptly (lossless: nothing was buffered
# by the empty session, and committed months always stay)
_c_pause, _c_cancel = threading.Event(), threading.Event()
_c_pause.set()
_c_cancel.set()
_cres = {"bars_fetched": 0, "_empty_days": [], "notes": [],
         "committed_through": None}
_cproc = sk._make_session_processor(
    _cres, "NVR", "1m-iv", [], None, True, (lambda s: None),
    (lambda upto_month=None: None), {}, [], [date(2024, 5, 31)],
    pause=_c_pause, cancel=_c_cancel)
try:
    _cproc(date(2024, 5, 31), [], 0, 1)
    _c_cancelled = False
except sk.Cancelled:
    _c_cancelled = True
check("no-data pause: cancel during the hold raises Cancelled", _c_cancelled)

# F-DIAG-1: both public run paths share this helper, so the regression pins the
# project-level destination and rejects the former bank-internal Run Logs path.
_diag_bank = fresh_root()
_diag_path = sk._connection_diag_path(_diag_bank)
check("diagnostics log resolves beside the bank at project-level Run Logs",
      _diag_path == (_diag_bank.parent / "Run Logs" /
                     "connection_diagnostics.log")
      and _diag_path != (_diag_bank / "Run Logs" /
                         "connection_diagnostics.log"),
      str(_diag_path))

# BUG #1 (wf w5pm6n5u0): the BACKWARD extension must NOT merge older history when
# the basis-check (boundary) session refetches EMPTY — the entry gate never runs
# on empty bars, so the basis is unverified. Guard: Call B is skipped + noted.
_bx_root = fresh_root()
run_fill(_bx_root, [("AAA", "1m")],                       # seed FRI as first_stored
         FakeAdapter({FRI: [ib_stub(b) for b in vendor_bars(FRI, 60, 5.0)]},
                     head=datetime(2024, 6, 21, tzinfo=NY)), today=FRI)
_first0 = sk.series_first_dt(ss.load_manifest(_bx_root / "AAA"), "1m")
# re-fetch DEEPER: earlier week MIS-SCALED (10.0, an unrecorded 2x split) BUT the
# boundary session FRI comes back EMPTY -> basis unverified -> must NOT merge.
_bx_days = {d: [ib_stub(b) for b in vendor_bars(d, 60, 10.0)]
            for d in (MON, TUE, WED, THU)}
_bx_days[FRI] = []
_bx_rep = _offline_gap_fill_body(_bx_root, [("AAA", "1m")],
                      adapter_factory=lambda: FakeAdapter(
                          _bx_days, head=datetime(2024, 6, 17, tzinfo=NY)),
                      pacer=fast_pacer(FakeClock()), today=FRI, since=MON)
_bx_s = _bx_rep["series"][0]
_first1 = sk.series_first_dt(ss.load_manifest(_bx_root / "AAA"), "1m")
check("backward extension SKIPS (no merge) when the boundary session is empty",
      not _bx_s.get("halt") and _first1 is not None
      and _first1.date() == _first0.date()                # NOT extended earlier
      and any("SKIPPED" in n for n in _bx_s.get("notes", [])))

# --- backward-extension RESUME: interior-hole seal v2 (1d holiday-aware calendar +
#     incomplete-marker gate; BLOCKER wmwbkpx5y + hunt wk8r48gvg #5/#6) ---
_seal_root = tempfile.mkdtemp()
_seal_cal = [date(2024, 3, d) for d in            # every March-2024 trading weekday
             (1, 4, 5, 6, 7, 8, 11, 12, 13, 14, 15, 18, 19, 20, 21, 22,
              25, 26, 27, 28, 29)]
_seal_gap = {date(2024, 3, x) for x in (11, 12, 13, 14, 15, 20)}  # a 5-day RUN + a LONE day
_seal_present = [d for d in _seal_cal if d not in _seal_gap]


def _seal_day_bars(days):
    return [(datetime(d.year, d.month, d.day, 9, 30), 1.0, 1.0, 1.0, 1.0, 100)
            for d in days]


_st1m = ss.write_month_file(
    ss.month_file_path(_seal_root, "TST", 2024, 3, "1m"), _seal_day_bars(_seal_present))
_st1d = ss.write_month_file(
    ss.month_file_path(_seal_root, "TST", 2024, 3, "1d"), _seal_day_bars(_seal_cal))
_seal_man = ss.new_manifest("TST", "TST")
ss.manifest_months(_seal_man, "1m")["2024-03"] = dict(_st1m, status="present")
ss.manifest_months(_seal_man, "1d")["2024-03"] = dict(_st1d, status="present")
_intra = sk._series_stored_days(_seal_root, "TST", "1m", _seal_man)
_cal = sk._series_stored_days(_seal_root, "TST", "1d", _seal_man)
_lo, _hi = min(_intra), max(_intra)
_holes = {d for d in _cal if _lo <= d <= _hi} - _intra
check("interior-seal v2: 1d HOLIDAY-AWARE calendar catches BOTH the run-hole (11-15) "
      "AND the LONE-day hole (20) a cancelled backfill left",
      _holes == _seal_gap, f"{sorted(_holes)}")
check("interior-seal: backfill-incomplete marker starts clear",
      not sk._backfill_marker(_seal_man, "1m"))
sk._backfill_marker(_seal_man, "1m", set_to=True)
check("interior-seal: marker sets", sk._backfill_marker(_seal_man, "1m"))
sk._backfill_marker(_seal_man, "1m", set_to=False)
check("interior-seal: marker clears", not sk._backfill_marker(_seal_man, "1m"))

# WS1 Phase 2: a clean fetch is not the same as complete source coverage. The
# seal uses the actual daily-probe frontier persisted when the backfill begins.
_deep_man = ss.new_manifest("DEEP", "DEEP")
sk._backfill_begin(_deep_man, "1m", date(2011, 6, 1))
_deep_res = {"notes": []}
_deep_sealed = sk._backfill_try_seal(
    _deep_res, _deep_man, "1m", datetime(2024, 5, 7, 9, 30))
_deep_sec = _deep_man["intervals"]["1m"]
check("backfill seal gate: refuses a short series while demonstrated earlier "
      "history remains",
      not _deep_sealed and sk._backfill_marker(_deep_man, "1m")
      and _deep_sec.get("under_backfilled") is True
      and _deep_sec.get("backfill_incomplete_reason")
      == "source_history_remaining"
      and _deep_res.get("under_backfilled", {}).get("served_earliest")
      == "2011-06-01")

_near_man = ss.new_manifest("NEAR", "NEAR")
sk._backfill_begin(_near_man, "1m", date(2024, 4, 1))
_near_res = {"notes": []}
_near_sealed = sk._backfill_try_seal(
    _near_res, _near_man, "1m", datetime(2024, 5, 7, 9, 30))
_near_sec = _near_man["intervals"]["1m"]
check("backfill seal gate: seals a stored frontier within the named tolerance",
      _near_sealed and not sk._backfill_marker(_near_man, "1m")
      and _near_sec.get("backfill_seal", {}).get("verified") is True
      and _near_sec.get("backfill_seal", {}).get("sealed") is True)

_unknown_man = ss.new_manifest("UNKNOWN", "UNKNOWN")
sk._backfill_begin(_unknown_man, "1m", None)
_unknown_res = {"notes": []}
_unknown_sealed = sk._backfill_try_seal(
    _unknown_res, _unknown_man, "1m", datetime(2024, 5, 7, 9, 30))
_unknown_sec = _unknown_man["intervals"]["1m"]
check("backfill seal gate: unknown source reach seals but records UNVERIFIED",
      _unknown_sealed and not sk._backfill_marker(_unknown_man, "1m")
      and _unknown_sec.get("backfill_seal", {}).get("verified") is False
      and any("UNVERIFIED" in note for note in _unknown_res["notes"]))
__import__("shutil").rmtree(_seal_root, ignore_errors=True)

# --- skipped-week (timeout) must NOT mis-mark a real day source-absent (hunt wjo4m9hd0 #1) ---
def _test_targeted_skipped_week():
    import unittest
    from fetch_a2_ibkr_workflows_selftest import Operations
    result = unittest.TestResult()
    unittest.TestSuite([Operations("test_targeted_skipped_week_legacy_contract")]).run(result)
    passed = result.testsRun == 1 and result.wasSuccessful()
    detail = repr(result.errors + result.failures)
    check("skipped-week: a timed-out week's REAL day is UNFILLED, not source_absent",
          passed, detail)
    check("skipped-week: a genuinely-absent day (month fetched, not skipped) IS "
          "source_absent", passed, detail)
    check("skipped-week: genuine absent day stays; a PRE-EXISTING false ⊘ on the "
          "re-skipped day is CLEARED (Fix data re-probe heals it back to ⚠)",
          passed, detail)


_test_targeted_skipped_week()


# --- cancel after a partial month fetch must leave unvisited days retryable ---
def _test_targeted_cancel_partial_month():
    """Cancellation is terminal; prove retryability from durable storage."""
    import unittest
    from fetch_a2_ibkr_workflows_selftest import Operations
    result = unittest.TestResult()
    unittest.TestSuite([Operations(
        "test_targeted_cancel_partial_month_legacy_contract")]).run(result)
    passed = result.testsRun == 1 and result.wasSuccessful()
    detail = repr(result.errors + result.failures)
    check("cancel mid-month: unvisited earlier days stay retryable, not source_absent",
          passed, detail)
    check("cancel mid-month: false source_absent is not persisted",
          passed, detail)


_test_targeted_cancel_partial_month()


# --- fill_missing_days final save must fresh-merge sibling interval manifests ---
def _test_targeted_concurrent_manifest_merge():
    import unittest
    from fetch_a2_ibkr_workflows_selftest import Operations
    result = unittest.TestResult()
    unittest.TestSuite([Operations(
        "test_targeted_concurrent_manifest_merge_legacy_contract")]).run(result)
    check("fill_missing_days merge-save: sibling interval sections both survive",
          result.testsRun == 1 and result.wasSuccessful(),
          repr(result.errors + result.failures))


_test_targeted_concurrent_manifest_merge()


# --- client-id rotation on a 326 (the recurring single-port flap) -----------
def _test_client_id_rotation():
    import types as _types
    taken = {sk.CLIENT_ID_FETCH, sk.CLIENT_ID_FETCH + sk.CLIENT_ID_STRIDE}

    class _Ev:
        def __init__(self):
            self._h = []

        def __iadd__(self, f):
            self._h.append(f)
            return self

        def fire(self, *a):
            for f in list(self._h):
                f(*a)

    mode = ["326"]                 # "326" = numbered error; "apierr" = bare FIN

    class _FakeIB:
        def __init__(self):
            self.errorEvent = _Ev()
            self.client = _types.SimpleNamespace(apiError=_Ev())
            self._conn = False
            self.RequestTimeout = None

        def connect(self, host, port, clientId=0, timeout=8, readonly=True):
            if clientId in taken:
                if mode[0] == "326":                    # numbered IB error 326
                    self.errorEvent.fire(
                        -1, 326, "Unable to connect as the client id is already "
                        "in use. Retry with a unique client id.")
                else:                                   # bare FIN: apiError only
                    self.client.apiError.fire(
                        f"Peer closed connection. clientId {clientId} already "
                        f"in use?")
                raise RuntimeError("peer closed connection")
            self._conn = True

        def reqMarketDataType(self, _n):
            pass

        def isConnected(self):
            return self._conn

        def disconnect(self):
            self._conn = False

    fake = _types.ModuleType("ib_async")
    fake.IB = _FakeIB
    _old = sys.modules.get("ib_async")
    sys.modules["ib_async"] = fake
    opened = []
    try:
        a = sk.LiveIB(ports=(3000,), client_id=sk.CLIENT_ID_FETCH)
        a.connect()
        opened.append(a)
        check("client-id rotation: a 326'd base id rotates to the next free id",
              a.client_id == sk.CLIENT_ID_FETCH + 2 * sk.CLIENT_ID_STRIDE,
              f"got {a.client_id}")
        check("client-id rotation: the rotated connection is live",
              a.is_connected())
        check("client-id rotation: rotated id keeps its family residue (no "
              "cross-operation collision)",
              a.client_id % sk.CLIENT_ID_STRIDE
              == sk.CLIENT_ID_FETCH % sk.CLIENT_ID_STRIDE)
        # bare-FIN variant: the held id is signalled ONLY via client.apiError
        # ("already in use?"), with NO numbered 326 — rotation must still fire.
        mode[0] = "apierr"
        a2 = sk.LiveIB(ports=(3000,), client_id=sk.CLIENT_ID_FETCH)
        a2.connect()
        opened.append(a2)
        check("client-id rotation: a bare-FIN 'already in use?' (apiError, no "
              "326) also rotates",
              a2.client_id == sk.CLIENT_ID_FETCH + 2 * sk.CLIENT_ID_STRIDE,
              f"got {a2.client_id}")
        mode[0] = "326"
        taken.clear()
        b = sk.LiveIB(ports=(3000,), client_id=sk.CLIENT_ID_SEAL)
        b.connect()
        opened.append(b)
        check("client-id rotation: a clean port uses the base id unchanged",
              b.client_id == sk.CLIENT_ID_SEAL, f"got {b.client_id}")
        taken.update(sk.CLIENT_ID_FETCH + k * sk.CLIENT_ID_STRIDE
                     for k in range(sk.CLIENT_ID_RETRIES + 1))
        c = sk.LiveIB(ports=(3000,), client_id=sk.CLIENT_ID_FETCH)
        _raised = False
        try:
            c.connect()
        except ConnectionError:
            _raised = True
        check("client-id rotation: every id busy -> ConnectionError (no hang)",
              _raised)
    finally:
        for adapter in reversed(opened):
            adapter.disconnect()
        if _old is not None:
            sys.modules["ib_async"] = _old
        else:
            sys.modules.pop("ib_async", None)


_test_client_id_rotation()


def _test_headstart_probe_and_timeout():
    """Pin daily reach reconciliation plus the timeout/retry contract."""
    TODAY = date(2026, 6, 29)
    FLOOR = TODAY - timedelta(days=sk.NO_HEAD_FALLBACK_DAYS)
    PROBE_FLOOR = sk._head_probe_floor(TODAY)

    class _Bar:
        def __init__(self, d):
            self.date = d

    class ProbeFake:
        # head_timestamp RAISES (mirrors LiveIB on the demo's 'Query failed'); the
        # big-duration daily probe yields a synthetic first bar at `earliest`.
        def __init__(self, earliest):
            self.earliest = earliest
            self.use_rth = True
            self.fetch_calls = []

        def head_timestamp(self, contract, what_to_show="TRADES"):
            raise sk.SeriesHalt("no head timestamp (selftest)")

        def fetch(self, contract, end_dt, duration, bar_size, what_to_show="TRADES"):
            self.fetch_calls.append((duration, bar_size, what_to_show))
            if (bar_size == "1 day" and duration == sk.HEAD_PROBE_DURATION
                    and self.earliest):
                return [_Bar(self.earliest)]
            return []

    pf = ProbeFake(date(2021, 7, 29))
    st, note = sk._head_start(pf, object(), date(2010, 1, 1), TODAY, "TRADES")
    check("headstart: probe returns recent-IPO real start, not deep since",
          st == date(2021, 7, 29))
    check("headstart: probe note explains the fallback",
          note is not None and "probed real first bar" in note)
    check("headstart: probe issues exactly one 30Y/1day request",
          pf.fetch_calls == [(sk.HEAD_PROBE_DURATION, "1 day", "TRADES")])

    pf2 = ProbeFake(date(2021, 7, 29))
    st2, _ = sk._head_start(pf2, object(), date(2023, 1, 1), TODAY, "TRADES")
    check("headstart: max(probed, since) respects a later user since",
          st2 == date(2023, 1, 1))
    pf2e = ProbeFake(date(2021, 7, 29))
    st2e, _note2e, served2e = sk._head_start_evidence(
        pf2e, object(), date(2023, 1, 1), TODAY, "TRADES")
    check("headstart evidence: preserves the demonstrated source frontier "
          "when a later user since limits the fetch",
          st2e == date(2023, 1, 1) and served2e == date(2021, 7, 29))

    pf3 = ProbeFake(date(1980, 3, 17))     # deep first bar + no user depth
    st3, _ = sk._head_start(pf3, object(), None, TODAY, "TRADES")
    check("headstart: since=None clamps a deep probe to the recent floor",
          st3 == FLOOR)

    pf4 = ProbeFake(date(2024, 5, 1))      # recent first bar + no user depth
    st4, _ = sk._head_start(pf4, object(), None, TODAY, "TRADES")
    check("headstart: since=None keeps a recent probe date above the floor",
          st4 == date(2024, 5, 1))

    pf5 = ProbeFake(None)                   # probe finds nothing
    st5, n5 = sk._head_start(pf5, object(), date(2015, 1, 1), TODAY, "TRADES")
    check("headstart: probe-empty falls back to requested since",
          st5 == date(2015, 1, 1) and "backfilling" in n5)
    pf6 = ProbeFake(None)
    st6, _ = sk._head_start(pf6, object(), None, TODAY, "TRADES")
    check("headstart: probe-empty + no since -> NO_HEAD_FALLBACK floor",
          st6 == FLOOR)

    class HeadOkFake(ProbeFake):
        def __init__(self, head_date, earliest):
            super().__init__(earliest)
            self.head_date = head_date

        def head_timestamp(self, contract, what_to_show="TRADES"):
            return datetime.combine(
                self.head_date, datetime.min.time(), tzinfo=sk._ny())

    ok = HeadOkFake(date(1990, 1, 2), PROBE_FLOOR)
    sto, no = sk._head_start(ok, object(), date(2010, 1, 1), TODAY, "TRADES")
    check("headstart: valid pre-probe-window head is preserved",
          sto == date(1990, 1, 2) and no is None
          and ok.fetch_calls == [(sk.HEAD_PROBE_DURATION, "1 day", "TRADES")])

    pltr = HeadOkFake(date(2024, 11, 26), date(2020, 9, 30))
    stp, np = sk._head_start(
        pltr, object(), date(2011, 6, 1), TODAY, "TRADES")
    check("headstart: false-late PLTR head yields to served daily first bar",
          stp == date(2020, 9, 30) and "disagreed" in np)

    odfl = HeadOkFake(date(1991, 10, 24), date(2024, 5, 7))
    sto, no = sk._head_start(
        odfl, object(), date(2011, 6, 1), TODAY, "TRADES")
    check("headstart: false-deep ODFL head yields to served daily first bar",
          sto == date(2024, 5, 7) and "disagreed" in no)

    empty = HeadOkFake(date(2015, 1, 2), None)
    ste, ne = sk._head_start(
        empty, object(), date(2011, 6, 1), TODAY, "TRADES")
    check("headstart: successful head remains the fallback when probe is empty",
          ste == date(2015, 1, 2) and ne is None)
    empty_e = HeadOkFake(date(2015, 1, 2), None)
    _ste, _ne, served_empty = sk._head_start_evidence(
        empty_e, object(), date(2011, 6, 1), TODAY, "TRADES")
    check("headstart evidence: head-only fallback is not demonstrated seal evidence",
          served_empty is None)

    check("timeout: RequestTimeout & PacingViolation are ConnectionError subclasses "
          "(except-order is load-bearing)",
          issubclass(sk.RequestTimeout, ConnectionError)
          and issubclass(sk.PacingViolation, ConnectionError))

    class ToAdapter:
        def __init__(self, beh):
            self.beh = beh
            self.calls = 0
            self.reconnects = 0
            self.use_rth = True
            self.port = 2000

        def fetch(self, *a, **k):
            self.calls += 1
            return self.beh(self.calls)

        def reconnect(self):
            self.reconnects += 1

    class ToPacer:
        def wait_turn(self, cancel, metered=True):
            pass

        def saturate(self):
            pass

    def _raise(exc):
        raise exc

    saved = sk._interruptible_sleep
    sk._interruptible_sleep = lambda *a, **k: None      # no real sleeping
    try:
        def run_to(beh):
            a = ToAdapter(beh)
            res = {"requests": 0}
            fl = [0]
            try:
                out = sk._fetch_request(
                    a, "C", "E", "1 D", "1 hour", ToPacer(), threading.Event(),
                    lambda m: None, lambda: fl.__setitem__(0, fl[0] + 1),
                    "HOOD", res, "win", metered=False)
                return ("RETURN", out, a)
            except sk.SeriesHalt:
                return ("HALT", None, a)

        r = run_to(lambda n: _raise(sk.RequestTimeout("no answer")))
        check("timeout: always-timeout HALTs after TIMEOUT_RETRIES+1, 0 reconnects",
              r[0] == "HALT" and r[2].calls == sk.TIMEOUT_RETRIES + 1
              and r[2].reconnects == 0)

        def t_then_ok(n):
            if n <= sk.TIMEOUT_RETRIES:
                raise sk.RequestTimeout("slow")
            return ["BAR"]

        r = run_to(t_then_ok)
        check("timeout: a transient timeout recovers on the warm link (no reconnect)",
              r[0] == "RETURN" and r[1] == ["BAR"] and r[2].reconnects == 0)

        r = run_to(lambda n: _raise(sk.ConnectionLost("dropped")))
        check("timeout: a real disconnect STILL reconnects (RECONNECT_ATTEMPTS)",
              r[0] == "HALT" and r[2].reconnects == sk.RECONNECT_ATTEMPTS)
    finally:
        sk._interruptible_sleep = saved


_test_headstart_probe_and_timeout()


def _test_earliest_available_and_sidecar():
    """Actual-served earliest reconciliation and its durable sidecar."""
    import tempfile
    import stock_validate as sv
    TODAY = date(2026, 6, 29)

    class _Bar:
        def __init__(self, d):
            self.date = d

    class EAFake:
        def __init__(self, head=None, earliest=None, bad=False):
            self.head, self.earliest, self.bad = head, earliest, bad
            self.use_rth = True

        def qualify(self, ticker):
            if self.bad:
                raise RuntimeError("no security definition")   # delisted/unknown
            return 111, object()

        def head_timestamp(self, contract, what_to_show="TRADES"):
            if self.head is None:
                raise sk.SeriesHalt("no head (selftest)")       # mirror LiveIB
            return self.head

        def fetch(self, contract, end, dur, bar, what_to_show="TRADES"):
            if bar == "1 day" and dur == sk.HEAD_PROBE_DURATION and self.earliest:
                return [_Bar(self.earliest)]
            return []

    check("earliest_available: valid deep head survives bounded probe floor",
          sk.earliest_available(
              EAFake(head=datetime(1990, 1, 2, tzinfo=sk._ny()),
                     earliest=sk._head_probe_floor(TODAY)), "AAA", TODAY)
          == date(1990, 1, 2))
    check("earliest_available: false-late PLTR head yields to served first bar",
          sk.earliest_available(
              EAFake(head=datetime(2024, 11, 26, tzinfo=sk._ny()),
                     earliest=date(2020, 9, 30)), "PLTR", TODAY)
          == date(2020, 9, 30))
    check("earliest_available: false-deep ODFL head yields to served first bar",
          sk.earliest_available(
              EAFake(head=datetime(1991, 10, 24, tzinfo=sk._ny()),
                     earliest=date(2024, 5, 7)), "ODFL", TODAY)
          == date(2024, 5, 7))
    check("earliest_available: successful head is fallback when probe is empty",
          sk.earliest_available(
              EAFake(head=datetime(2015, 1, 2, tzinfo=sk._ny())), "AAA", TODAY)
          == date(2015, 1, 2))
    check("earliest_available: head fail -> probe first bar (HOOD-style)",
          sk.earliest_available(
              EAFake(head=None, earliest=date(2021, 7, 29)), "HOOD", TODAY)
          == date(2021, 7, 29))
    check("earliest_available: head fail + probe empty -> None",
          sk.earliest_available(EAFake(head=None, earliest=None), "X", TODAY)
          is None)
    check("earliest_available: unknown symbol -> None, never raises",
          sk.earliest_available(EAFake(bad=True), "ZZZ", TODAY) is None)

    sroot = tempfile.mkdtemp()
    sv.record_ibkr_earliest(sroot, "hood", "2021-07-29")
    sv.record_ibkr_earliest(sroot, "AMD", "2015-01-02")
    m = sv.load_ibkr_earliest(sroot)
    check("ibkr_earliest sidecar: round-trips, keyed UPPER",
          m.get("HOOD") == "2021-07-29" and m.get("AMD") == "2015-01-02")
    check("ibkr_earliest sidecar: blank ticker/date is rejected",
          sv.record_ibkr_earliest(sroot, "", "2020-01-01") is None
          and sv.record_ibkr_earliest(sroot, "X", "") is None)

    # WS4 Add Stocks prevention: conId equality alone is insufficient when an
    # old/predecessor series already sits under the current contract's pin.
    id_root = fresh_root()
    old_day = date(2011, 6, 1)
    seed_series(id_root, "OLD", old_day, 1, 10.0)
    old_man = ss.load_manifest(id_root / "OLD")
    old_man["conid"] = 111
    ss.save_manifest(id_root / "OLD", old_man)
    seed_series(id_root, "UNPIN", old_day, 1, 20.0)
    unpin_man = ss.load_manifest(id_root / "UNPIN")
    for entry in ss.manifest_months(unpin_man, "1m").values():
        entry.pop("first", None)  # legacy manifest: month exists, exact first absent
    ss.save_manifest(id_root / "UNPIN", unpin_man)

    clean = sk.check_security_ids(
        id_root, {"OLD": 111},
        served_earliest={"OLD": "2011-06-01"}, check_history=True)
    check("add identity: matching conId + compatible listing history passes",
          clean["OLD"]["status"] == "ok", str(clean["OLD"]))
    conid_bad = sk.check_security_ids(
        id_root, {"OLD": 222},
        served_earliest={"OLD": "2011-06-01"}, check_history=True)
    check("add identity: changed conId remains a blocking mismatch",
          conid_bad["OLD"]["status"] == "mismatch", str(conid_bad["OLD"]))
    history_bad = sk.check_security_ids(
        id_root, {"OLD": 111},
        served_earliest={"OLD": "2023-09-12"}, check_history=True)
    check("add identity: stored predecessor history before current listing blocks",
          history_bad["OLD"]["status"] == "history_mismatch"
          and history_bad["OLD"]["gap_days"] >
          sk.BACKFILL_SEAL_TOLERANCE_DAYS,
          str(history_bad["OLD"]))
    unknown = sk.check_security_ids(
        id_root, {"OLD": 111}, served_earliest={}, check_history=True)
    check("add identity: missing listing evidence is fail-closed",
          unknown["OLD"]["status"] == "history_unverified",
          str(unknown["OLD"]))
    unpinned = sk.check_security_ids(
        id_root, {"UNPIN": 333},
        served_earliest={"UNPIN": "2011-06-01"}, check_history=True)
    check("add identity: unpinned stored history needs compatible frontier",
          unpinned["UNPIN"]["status"] == "ok_unpinned",
          str(unpinned["UNPIN"]))
    new = sk.check_security_ids(
        id_root, {"FRESH": 444}, served_earliest={}, check_history=True)
    check("add identity: genuinely new ticker needs no history probe",
          new["FRESH"]["status"] == "new", str(new["FRESH"]))

    class IdentityFake(EAFake):
        def __init__(self, head=None, earliest=None):
            super().__init__(head=head, earliest=earliest)
            self.fetch_calls = 0
            self.qualify_many_calls = 0

        def contract_for(self, conid):
            return SimpleNamespace(conId=conid, symbol="OLD")

        def qualify_many(self, symbols, chunk=50, progress=None, cancel=None):
            self.qualify_many_calls += 1
            return {symbol: 111 for symbol in symbols}

        def fetch(self, contract, end, dur, bar, what_to_show="TRADES"):
            self.fetch_calls += 1
            return super().fetch(contract, end, dur, bar, what_to_show)

        def disconnect(self):
            pass

    cached_adapter = IdentityFake()
    cached = sk.check_add_identities(
        id_root, {"OLD": 111}, cached_adapter, today=TODAY,
        served_earliest={"OLD": "2011-06-01"})
    check("add identity: cached normal path adds zero historical requests",
          cached["OLD"]["status"] == "ok"
          and cached_adapter.fetch_calls == 0,
          f"{cached['OLD']} fetches={cached_adapter.fetch_calls}")

    probed_adapter = IdentityFake(
        head=datetime(2024, 11, 26, tzinfo=sk._ny()),
        earliest=date(2023, 9, 12))
    def unscoped_refused(symbol, adapter, cache):
        try:
            sk.check_add_identities(id_root, {symbol: 111 if symbol == "OLD" else 333},
                adapter, today=TODAY, served_earliest=cache)
        except sk.AuthorityError:
            return True
        return False
    # Positive probed/unpinned/head-only cases moved to the confined workflow
    # suite's actual temporary-manifest test. Here missing authority must refuse.
    check("add identity: missing cache requires an explicit child before probing",
          unscoped_refused("OLD", probed_adapter, {}) and probed_adapter.fetch_calls == 0)

    unpinned_adapter = IdentityFake(
        head=datetime(2011, 6, 1, tzinfo=sk._ny()),
        earliest=date(2011, 6, 1))
    check("add identity: unpinned refresh refuses without a child",
          unscoped_refused("UNPIN", unpinned_adapter, {"UNPIN": "1990-01-02"})
          and unpinned_adapter.fetch_calls == 0)

    head_only_adapter = IdentityFake(
        head=datetime(2023, 9, 12, tzinfo=sk._ny()), earliest=None)
    check("add identity: head-only candidate refuses without a child",
          unscoped_refused("OLD", head_only_adapter, {}) and head_only_adapter.fetch_calls == 0)
    blocked = sk.blocked_add_identities({
        **clean, "BAD": dict(history_bad["OLD"], symbol="BAD")})
    check("add identity: shared blocker selects only fail-closed verdicts",
          set(blocked) == {"BAD"}, str(blocked))

    sv.record_ibkr_earliest(id_root, "OLD", "2011-06-01")
    validate_adapter = IdentityFake()
    validated = sk._validate_symbols_body.__wrapped__(
        ["OLD", "FRESH"], adapter_factory=lambda: validate_adapter,
        identity_root=id_root, identity_today=TODAY)
    check("validate_symbols: Add Stocks identity gate reuses validation connection",
          validated["secid"]["OLD"]["status"] == "ok"
          and validated["secid"]["FRESH"]["status"] == "new"
          and validate_adapter.qualify_many_calls == 1
          and validate_adapter.fetch_calls == 0,
          str(validated))


_test_earliest_available_and_sidecar()


def _test_company_name_and_sidecar():
    """2026-06-30: company_name (issuer longName via reqContractDetails — metadata,
    '' on ANY failure, never raises) + the ibkr_names sidecar that feeds find-stock,
    Fix-data PHASE 0b, and the 'Company' display."""
    import tempfile
    import stock_validate as sv

    # --- the names sidecar: record/load, keyed UPPER, blank-rejected ---
    sroot = tempfile.mkdtemp()
    sv.record_ibkr_name(sroot, "aapl", "Apple Inc.")
    sv.record_ibkr_name(sroot, "ODFL", "Old Dominion Freight Line, Inc.")
    m = sv.load_ibkr_names(sroot)
    check("ibkr_names sidecar: round-trips, keyed UPPER",
          m.get("AAPL") == "Apple Inc."
          and m.get("ODFL") == "Old Dominion Freight Line, Inc.")
    check("ibkr_names sidecar: blank ticker/name is rejected",
          sv.record_ibkr_name(sroot, "", "X") is None
          and sv.record_ibkr_name(sroot, "X", "") is None)
    check("ibkr_names sidecar: missing file -> {}",
          sv.load_ibkr_names(tempfile.mkdtemp()) == {})

    class _D:                                      # fake ContractDetails
        def __init__(self, ln):
            self.longName = ln
            self.contract = SimpleNamespace(conId=123, secType="STK")

    class _CN:                                     # minimal LiveIB stand-in
        def __init__(self, details):
            self._d = details
            self.ib = self

        def reqContractDetailsAsync(self, c):
            return None                            # the 'coro' _await consumes

        def _await(self, coro, timeout, desc):
            if self._d == "raise":
                raise sk.SeriesHalt("boom (selftest)")
            return self._d

    # Pure parser checks need no ib_async or weakened send guard. Actual company
    # transport/unavailability/result-fault behavior lives in the confined suite.
    cn = lambda owner, symbol: sk.fib.normalize_company(owner._d, 123)["name"]
    _check_company_name_parser(cn, _CN, _D)


def _check_company_name_parser(cn, _CN, _D):
    check("company_name: returns the issuer longName",
          cn(_CN([_D("Apple Inc.")]), "AAPL") == "Apple Inc.")
    check("company_name: skips a blank longName, takes the next detail",
          cn(_CN([_D(""), _D("Apple Inc.")]), "AAPL") == "Apple Inc.")
    check("company_name: no details -> ''", cn(_CN([]), "AAPL") == "")
    rejected = False
    try:
        cn(_CN("malformed response"), "AAPL")
    except sk.AuthorityError:
        rejected = True
    check("company_name: malformed metadata is rejected, not an empty name", rejected)


_test_company_name_and_sidecar()


def _test_fetch_month_bars_cancel():
    """2026-06-29: the pause-finalize gap-seal's per-month fetch must honor cancel
    so the 5-min budget / Resume can abort within ~one fetch instead of ~15 min on
    a poison month (the seal calls adapter.fetch directly, NOT _fetch_request)."""
    class _HangFetch:
        def __init__(self):
            self.calls = 0
            self.use_rth = True

        def fetch(self, *a, **k):
            self.calls += 1
            raise sk.RequestTimeout("demo hangs on this contract")

    hf = _HangFetch()
    b, _skp = sk._fetch_month_bars(hf, object(), 2024, 1, "1m", cancel=lambda: True)
    check("_fetch_month_bars: cancel=True aborts the in-month walk with NO fetch",
          hf.calls == 0 and b == [])
    hf2 = _HangFetch()
    sk._fetch_month_bars(hf2, object(), 2024, 1, "1m", cancel=lambda: False)
    check("_fetch_month_bars: cancel=False still walks the month (fetch attempted)",
          hf2.calls > 0)


_test_fetch_month_bars_cancel()


def _test_identity_floor_combined_prefetch():
    """The combined RTH/pre/post optimization must honor the listing floor."""
    floor_root = fresh_root()
    ticker = "FLOOR"
    floor_day = date(2024, 6, 17)
    manifest = ss.new_manifest(ticker, ticker)
    manifest["conid"] = 111
    manifest["data_corrections"] = [{
        "type": "identity_listing_truncation",
        "ticker": ticker,
        "cutover": floor_day.isoformat(),
    }]
    ss.save_manifest(floor_root / ticker, manifest)

    captured = {}
    original_head = sk._head_start_evidence
    original_prefetch = sk._prefetch_unfiltered
    sk._head_start_evidence = lambda *args, **kwargs: (
        date(2020, 1, 2), None, None)

    def _capture_prefetch(adapter, pacer, contract, selected_ticker, base,
                          session_ivs, days, cancel, say, pre_res,
                          before_last_request=None, days_by_token=None):
        captured["ticker"] = selected_ticker
        captured["days"] = list(days)
        captured["intervals"] = list(session_ivs)
        return {interval: {} for interval in session_ivs}

    sk._prefetch_unfiltered = _capture_prefetch
    report = {}
    try:
        cache = sk._prefetch_combined(
            floor_root, FakeAdapter({}), fast_pacer(FakeClock()), ticker,
            "1m", ["1m", "1m-pre", "1m-post"], {ticker: 111},
            date(2024, 6, 21), date(2019, 1, 1), threading.Event(),
            lambda message: None, report)
    finally:
        sk._head_start_evidence = original_head
        sk._prefetch_unfiltered = original_prefetch

    requested_days = captured.get("days", [])
    check("identity floor: combined prefetch clamps all three session series",
          cache is not None
          and captured.get("intervals") == ["1m", "1m-pre", "1m-post"]
          and requested_days
          and min(requested_days) == floor_day
          and all(day >= floor_day for day in requested_days)
          and any("clamps combined prefetch" in note
                  for note in report.get("notes", [])),
          f"captured={captured} report={report}")


_test_identity_floor_combined_prefetch()


def _test_identity_floor_targeted_fill():
    """Retain all three legacy checks through the confined workflow owner."""
    import unittest
    from fetch_a2_ibkr_workflows_selftest import Operations
    cases = (
        ("test_targeted_identity_boundary_month_legacy_contract",
         "identity floor: Fix Data filters pre-floor bars from a boundary-month response"),
        ("test_targeted_identity_pre_floor_legacy_contract",
         "identity floor: Fix Data refuses a pre-floor day before source contact"),
        ("test_targeted_identity_malformed_legacy_contract",
         "identity floor: malformed Fix Data correction fails closed before source contact"),
    )
    for method, label in cases:
        result = unittest.TestResult()
        unittest.TestSuite([Operations(method)]).run(result)
        check(label, result.testsRun == 1 and result.wasSuccessful(),
              repr(result.errors + result.failures))


_test_identity_floor_targeted_fill()


def _test_empty_month_control_identity_and_cap():
    manifest = ss.new_manifest("VRTX", "VRTX")
    manifest["conid"] = 555
    months = ss.manifest_months(manifest, "1m-iv")
    months["2014-01"] = {"rows": 10}
    months["2014-05"] = {"rows": 10}
    original = sk._fetch_month_bars
    calls = []

    def _served(_adapter, _contract, year, month, _interval, cancel=None):
        calls.append((year, month))
        return ([(datetime(year, month, 3, 9, 30),
                  1.0, 1.0, 1.0, 1.0, 100)], set())

    try:
        sk._fetch_month_bars = _served
        mismatch = sk._positive_empty_month_control(
            object(), {"conId": 999}, manifest, "1m-iv", {(2014, 3)})
        mismatch_calls = list(calls)
        calls.clear()
        matched = sk._positive_empty_month_control(
            object(), {"conId": 555}, manifest, "1m-iv", {(2014, 3)})
        matched_calls = list(calls)
    finally:
        sk._fetch_month_bars = original
    check("empty-month control: mismatched contract cannot prove durable series",
          mismatch is None and mismatch_calls == [],
          f"result={mismatch!r} calls={mismatch_calls}")
    check("empty-month control: nearest two-sided probes are bounded and cached",
          matched == "2014-01"
          and matched_calls == [(2014, 1), (2014, 5)]
          and len(matched_calls) <= sk.EMPTY_MONTH_CONTROL_PROBE_CAP,
          f"result={matched!r} calls={matched_calls}")


_test_empty_month_control_identity_and_cap()


check("selftest temporary operation gate is released",
      og.status(path=_TEST_GATE_PATH)["available"])
_restore_test_operation_gate()
check("selftest restores the production operation-gate path",
      og.LOCK_PATH == _TEST_GATE_OLD_PATH
      and not Path(_TEST_GATE_DIR.name).exists())

print()
print(f"{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    for f in FAILS:
        print(f"  FAILED: {f}")
    sys.exit(1)
print("ALL PASS")
