"""Deterministic integration tests for ADAPTIVE CONCURRENCY.

Live demo-backend chokes are NOT reproducible on demand, so the park / swap /
forced-release ACTUATOR is exercised here against a fault-injecting fake
adapter that forces ConnectionError drops on chosen ports. Real threads, the
real controller, the real _OrEvent park-at-the-request-boundary mechanism — no
network. Timing constants are shrunk so the whole file runs in a few seconds.

    python adaptive_concurrency_test.py
"""

import sys
import threading
from contextvars import copy_context
import time as _rt
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))


def main():
    # The existing registered owner installs containment before subject imports.
    from fetch_a2_ibkr_workflows_selftest import Operations
    import unittest
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite([
        Operations("test_c2_adaptive_scheduler_contracts")]))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())


import stock_storage as ss        # noqa: E402
import stock_ibkr as sk           # noqa: E402

NY = ZoneInfo("America/New_York")
FAILS = []
N = [0]


def check(name, cond, detail=""):
    N[0] += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


_fixture_root = None
_fixture_authority = None
_root_sequence = 0


def fresh_root():
    global _root_sequence
    if _fixture_root is None:
        raise RuntimeError("scheduler driver requires the confined Operations owner")
    _root_sequence += 1
    return Path(_fixture_root) / str(_root_sequence) / ss.STORAGE_DIR_NAME


# ---- fault-injecting fake adapter (per-port) ---------------------------
def _stub(dt, o):
    return SimpleNamespace(date=dt.replace(tzinfo=NY).astimezone(timezone.utc),
                           open=o, high=o + 0.05, low=o - 0.05,
                           close=o + 0.01, volume=100.0)


def _biz_days(end, n):
    out, d = [], end
    while len(out) < n:
        if _fixture_authority.window("1m", d) is not None:
            out.append(d)
        d -= timedelta(days=1)
    return sorted(out)


def make_days(end, n):
    days = {}
    for d in _biz_days(end, n):
        base = datetime.combine(d, ss.RTH_FIRST)
        days[d] = [_stub(base + timedelta(minutes=i), 50.0 + i)
                   for i in range(6)]
    return days


class ChokeAdapter:
    """Per-port fake. `fail_n` ConnectionError drops (the backend-choke
    signature) then serves real bars — so a choked port RECOVERS and the run
    can complete (proving no deadlock)."""

    # earliest available data (the empty-series backfill dereferences this);
    # well before `since` so `since` is what actually bounds the backfill.
    HEAD = datetime(2024, 3, 1, 14, 30, tzinfo=timezone.utc)

    def __init__(self, port, days, fail_n=0, conid=111, account=None, start_barrier=None):
        self.port = port
        self.days = days
        self.fail_n = fail_n
        self.conid = conid
        self._account = account or f"DU{port}"
        self.reconnects = 0
        self.start_barrier = start_barrier

    def account(self):
        return self._account

    def qualify(self, symbol):
        return self.conid, SimpleNamespace(symbol=symbol)

    def qualify_many(self, symbols, chunk=50, progress=None):
        return {s: self.conid for s in symbols}

    def contract_for(self, conid):
        return SimpleNamespace(symbol="?", conId=conid)

    def head_timestamp(self, contract):
        return self.HEAD

    def is_connected(self):
        return True

    def reconnect(self):
        self.reconnects += 1

    def disconnect(self):
        pass

    def fetch(self, contract, end_dt, duration, bar_size):
        if self.start_barrier is not None:
            barrier, self.start_barrier = self.start_barrier, None
            barrier.wait(10)
        if self.fail_n > 0:
            self.fail_n -= 1
            raise ConnectionError("simulated backend drop")
        end_d = end_dt.date()
        if duration.endswith("S"):
            return list(self.days.get(end_d, []))
        n, unit = duration.split()
        span = {"D": 1, "W": 7, "M": 31}[unit] * int(n)
        start_d = end_d - timedelta(days=span - 1)
        out = []
        for d in sorted(self.days):
            if start_d <= d <= end_d:
                out.extend(self.days[d])
        return out


def choke_factory(fail_map, days, start_barrier=None):
    """(host, ports) -> make() ; build a ChokeAdapter for ports[0] with the
    fail count from fail_map (port -> fail_n)."""
    def factory(host, ports):
        p = int(ports[0])

        def make():
            return ChokeAdapter(p, days, fail_n=fail_map.get(p, 0),
                                start_barrier=start_barrier)
        return make
    return factory


# ---- fast timing (restored after) --------------------------------------
_SAVED = {k: getattr(sk, k) for k in (
    "RECONNECT_BACKOFF_S", "RECONNECT_BACKOFF_CAP_S", "ADAPT_TICK_S",
    "ADAPT_WINDOW_S", "ADAPT_COOLDOWN_S", "ADAPT_ADD_CLEAN_S",
    "ADAPT_SWAP_STUCK_S")}


def fast_timing():
    sk.RECONNECT_BACKOFF_S = 0.02
    sk.RECONNECT_BACKOFF_CAP_S = 0.05
    sk.ADAPT_TICK_S = 0.08
    sk.ADAPT_WINDOW_S = 3.0
    sk.ADAPT_COOLDOWN_S = 0.2
    sk.ADAPT_ADD_CLEAN_S = 0.8
    sk.ADAPT_SWAP_STUCK_S = 0.3


def restore_timing():
    for k, v in _SAVED.items():
        setattr(sk, k, v)


def run_capture(ports, fail_map, tickers, adaptive=True, timeout=60.0,
                synchronize=False):
    """Run gap_fill_parallel with injected choke adapters; capture progress +
    per-port states; enforce a NO-DEADLOCK watchdog. Returns (rep, prog, states)."""
    root = fresh_root()
    days = make_days(date(2024, 6, 28), 60)
    selections = [(t, "1m") for t in tickers]
    prog = []
    plock = threading.Lock()
    states = {}

    def progress(m):
        with plock:
            prog.append(m)

    def port_status(p, info):
        with plock:
            states.setdefault(p, []).append(info.get("state"))

    holder = {}
    cancel = threading.Event()
    start_barrier = threading.Barrier(min(len(ports), len(tickers))) if synchronize else None

    def go():
        holder["rep"] = sk.nightly_gap_fill_parallel(
            root, selections, list(ports),
            cancel=cancel,
            progress=progress, since=date(2024, 4, 1), today=date(2024, 6, 28),
            port_status=port_status, adaptive=adaptive,
            adapter_factory=choke_factory(fail_map, days, start_barrier))

    from dynamic_queue_test import _joined_run
    deadlocked = _joined_run(go, timeout, cancel)
    return holder.get("rep"), prog, states, deadlocked


def count(prog, needle):
    return sum(1 for m in prog if needle in m.lower())


def _run_cases(*, temp_root, authority):
    """Run every original assertion; the caller owns containment and storage."""
    global _fixture_root, _root_sequence, _fixture_authority
    _fixture_root, _root_sequence = Path(temp_root), 0
    _fixture_authority = authority
    FAILS.clear()
    N[0] = 0
    saved = {key: getattr(sk, key) for key in _SAVED}
    try:
        print("=== adaptive concurrency :: deterministic actuator tests ============")
        fast_timing()

        # --- A) PARK blocks at the shipped clean boundary before the next turn ---
        park_ev = threading.Event()
        orp = sk._OrEvent(None, park_ev)
        pacer = sk.Pacer(max_requests=100, window_s=600, min_gap_s=0.0)
        pacer._pause = orp
        park_ev.set()
        done = []


        def _blocked():
            # Pacing owns reservations, not worker pause state. The shipped
            # session processor now observes pause at a clean month boundary.
            sk._wait_while_paused(pacer._pause, None)
            pacer.wait_turn(metered=False)
            done.append("through")


        tb = threading.Thread(target=_blocked)
        tb.start()
        try:
            _rt.sleep(0.4)
            check("PARK: a set park event blocks the worker at the request boundary",
                  not done)
        finally:
            park_ev.clear()
            tb.join()  # Also join before cleanup on interrupted assertions.
        check("UNPARK: clearing the park event lets the worker proceed",
              done == ["through"])

        # --- B) forced choke on 3/5 ports -> controller PARKS, run COMPLETES -------
        PORTS = (2000, 3000, 4000, 5000, 6000)
        TKS = ["AAA", "BBB", "CCC", "DDD", "EEE"]
        rep, prog, states, dead = run_capture(
            PORTS, {2000: 9, 3000: 9, 4000: 9}, TKS, adaptive=True)
        check("choke: run did NOT deadlock (returned within watchdog)", not dead)
        check("choke: run produced a report", rep is not None)
        if rep is not None:
            check("choke: controller PARKED at least one port under the drop spike",
                  count(prog, "parking") >= 1,
                  f"park msgs={count(prog,'parking')}")
            check("choke: every series still completed (data fetched)",
                  len(rep.get("series", [])) == len(TKS),
                  f"series={len(rep.get('series', []))}/{len(TKS)}")
            check("choke: bars were actually written",
                  rep.get("totals", {}).get("added", 0) > 0,
                  f"added={rep.get('totals', {}).get('added', 0)}")
            check("choke: no port left aborted (parked work all drained/recovered)",
                  not rep.get("aborted_ports"),
                  f"aborted={rep.get('aborted_ports')}")
            parked_seen = any("parked" in (states.get(p) or []) for p in PORTS)
            check("choke: at least one port observed in the 'parked' state",
                  parked_seen, f"states={ {p: states.get(p) for p in PORTS} }")

        # --- C) healthy backend -> adaptive ON must NOT false-trip a park ----------
        rep2, prog2, _st2, dead2 = run_capture(
            PORTS, {}, TKS, adaptive=True)
        check("healthy: no deadlock", not dead2)
        check("healthy: adaptive ON never parks a healthy backend",
              count(prog2, "parking") == 0, f"parks={count(prog2,'parking')}")
        check("healthy: all series completed",
              rep2 is not None and len(rep2.get("series", [])) == len(TKS))

        # --- D) adaptive OFF: drops are ridden out by reconnect, NO park machinery -
        rep3, prog3, _st3, dead3 = run_capture(
            PORTS, {2000: 9, 3000: 9, 4000: 9}, TKS, adaptive=False)
        check("adaptive OFF: still completes (reconnect rides out the drops)",
              not dead3 and rep3 is not None
              and len(rep3.get("series", [])) == len(TKS))
        check("adaptive OFF: no park/swap messages emitted at all",
              count(prog3, "parking") == 0 and count(prog3, "swapping") == 0)
        check("adaptive OFF: drops DID occur (the test really injected a choke)",
              count(prog3, "reconnect") >= 3, f"reconnects={count(prog3,'reconnect')}")

        # --- E) SWAP: a stuck port + a free parked slot -> 1-for-1 swap ------------
        # 2000/3000 flap briefly (trigger an early park); 4000 stays stuck long enough
        # to cross SWAP_STUCK_S while a parked slot is free; 5000 healthy.
        P4 = (2000, 3000, 4000, 5000)
        T4 = ["AAA", "BBB", "CCC", "DDD"]
        rep4, prog4, st4, dead4 = run_capture(
            P4, {2000: 6, 3000: 6, 4000: 18}, T4, adaptive=True, timeout=90.0,
            synchronize=True)
        check("swap: no deadlock", not dead4)
        check("swap: a stuck port was swapped for an idle parked slot",
              count(prog4, "swapping in") >= 1,
              f"swap msgs={count(prog4,'swapping in')}; parks={count(prog4,'parking')}; "
              f"controller={[m for m in prog4 if any(word in m.lower() for word in ('reconnect', 'parking', 'resuming', 'swapping'))]}")
        check("swap: run still completed every series",
              rep4 is not None and len(rep4.get("series", [])) == len(T4),
              f"series={None if rep4 is None else len(rep4.get('series', []))}/{len(T4)}")

        # --- F) PHANTOM PORTS: 5-port fleet but only 3 tickers -> 2 empty-chunk ports.
        # Choke the real ports to force a park, and DISABLE `add` (huge ADD_CLEAN) so the
        # ONLY way to drain a parked port at the tail is the forced-release guarantee.
        # Pre-fix (phantom ports counted as 'working') active_working never hits 0 ->
        # release never fires -> join() hangs. Post-fix it completes.
        sk.ADAPT_ADD_CLEAN_S = 1000.0
        rep5, prog5, _st5, dead5 = run_capture(
            PORTS, {p: 9 for p in PORTS}, ["AAA", "BBB", "CCC"],
            adaptive=True, timeout=60.0)
        sk.ADAPT_ADD_CLEAN_S = 0.8
        check("phantom: 3 tickers on a 5-port fleet did NOT deadlock at the tail",
              not dead5)
        check("phantom: forced-release drained the parked port(s) (add was disabled)",
              rep5 is not None and count(prog5, "resuming") >= 1,
              f"resume msgs={count(prog5,'resuming')}; parks={count(prog5,'parking')}")
        check("phantom: all 3 series completed",
              rep5 is not None and len(rep5.get("series", [])) == 3,
              f"series={None if rep5 is None else len(rep5.get('series', []))}/3")

        restore_timing()
        print()
        print(f"{N[0]} checks, {len(FAILS)} failed")
        if FAILS:
            for f in FAILS:
                print("   FAILED:", f)
            return 1
        print("ALL PASS")
        return 0
    finally:
        for key, value in saved.items():
            setattr(sk, key, value)
        _fixture_root = None
