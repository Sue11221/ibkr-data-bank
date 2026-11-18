"""Deterministic tests for the DYNAMIC WORK QUEUE in gap_fill_parallel.

Proves the user-visible property: a port that finishes its share does NOT go
idle — it pulls the next ticker from the ONE shared queue, so every port stays
busy to the tail. Also: whole-ticker jobs keep a ticker's sessions TOGETHER (so
the combined extended fetch still works), one connection is reused per port
across many jobs (not reconnected per ticker), few-tickers SPLITS series so the
spare ports still get work, and nothing deadlocks.

    python dynamic_queue_test.py
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
        Operations("test_c2_dynamic_scheduler_contracts")]))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())


import stock_storage as ss        # noqa: E402
import stock_ibkr as sk           # noqa: E402

NY = ZoneInfo("America/New_York")
FAILS = []
N = [0]
_mk_lock = threading.Lock()


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


def _stub(dt, o):
    return SimpleNamespace(date=dt.replace(tzinfo=NY).astimezone(timezone.utc),
                           open=o, high=o + 0.05, low=o - 0.05,
                           close=o + 0.01, volume=100.0)


def _biz(end, n):
    out, d = [], end
    while len(out) < n:
        if _fixture_authority.window("1m", d) is not None:
            out.append(d)
        d -= timedelta(days=1)
    return sorted(out)


def make_days(end, n):
    days = {}
    for d in _biz(end, n):
        base = datetime.combine(d, ss.RTH_FIRST)
        days[d] = [_stub(base + timedelta(minutes=i), 50.0 + i)
                   for i in range(6)]
    return days


HEAD = datetime(2024, 3, 1, 14, 30, tzinfo=timezone.utc)


class SpeedAdapter:
    """Per-port fake that serves real bars. A `gate` event lets a test BLOCK one
    port's fetches until released — a deterministic stand-in for a slow port
    (timing alone is too muddy: the fake's per-ticker ingest dwarfs any sleep)."""

    def __init__(self, port, days, delay=0.0, gate=None):
        self.port = port
        self.days = days
        self.delay = delay
        self.gate = gate
        self._account = f"DU{port}"
        self.use_rth = True

    def account(self):
        return self._account

    def qualify(self, s):
        return 111, SimpleNamespace(symbol=s)

    def qualify_many(self, syms, chunk=50, progress=None):
        return {s: 111 for s in syms}

    def contract_for(self, conid):
        return SimpleNamespace(symbol="?", conId=conid)

    def head_timestamp(self, c):
        return HEAD

    def is_connected(self):
        return True

    def reconnect(self):
        pass

    def disconnect(self):
        pass

    def fetch(self, contract, end_dt, duration, bar_size):
        if self.gate is not None:
            self.gate.wait(timeout=30.0)        # blocked until the test releases
        if self.delay:
            _rt.sleep(self.delay)
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


def speed_factory(delay_map, days, make_counts, gate_map=None):
    """(host, ports) -> make(); one SpeedAdapter for ports[0]. Counts make()
    calls per port so we can prove ONE connection is reused across jobs."""
    gate_map = gate_map or {}

    def factory(host, ports):
        p = int(ports[0])

        def make():
            with _mk_lock:
                make_counts[p] = make_counts.get(p, 0) + 1
            return SpeedAdapter(p, days, delay=delay_map.get(p, 0.0),
                                gate=gate_map.get(p))
        return make
    return factory




def run(ports, sels, delay_map, since, today, watchdog=90.0):
    root = fresh_root()
    days = make_days(today, 70)
    make_counts = {}
    holder = {}
    cancel = threading.Event()

    def go():
        holder["rep"] = sk.nightly_gap_fill_parallel(
            root, sels, list(ports), since=since, today=today,
            cancel=cancel,
            adapter_factory=speed_factory(delay_map, days, make_counts))

    dead = _joined_run(go, watchdog, cancel)
    return holder.get("rep"), make_counts, dead


def _joined_run(go, timeout, cancel, release=None):
    errors = []
    def invoke():
        try:
            go()
        except BaseException as exc:
            errors.append(exc)
    th = threading.Thread(target=copy_context().run, args=(invoke,))
    th.start()
    try:
        th.join(timeout)
        expired = th.is_alive()
    finally:
        if th.is_alive():
            cancel.set()
            if release is not None:
                release()
            # Never return into temporary-root cleanup while a worker is live.
            # The outer gate process deadline is the final hung-worker bound.
            th.join()
    if errors:
        raise errors[0]
    return expired


def part_counts(rep, ports):
    part = rep.get("_partition", {})
    return {p: len(part.get(p, [])) for p in ports}


def _run_cases(*, temp_root, authority):
    """Run every original assertion; the caller owns containment and storage."""
    global _fixture_root, _root_sequence, _fixture_authority
    _fixture_root, _root_sequence = Path(temp_root), 0
    _fixture_authority = authority
    FAILS.clear()
    N[0] = 0
    saved = sk.RECONNECT_BACKOFF_S
    sk.RECONNECT_BACKOFF_S = 0.02
    try:
        print("=== dynamic work queue ================================================")
        today = date(2024, 6, 28)
        since = date(2024, 4, 1)

        # --- A) one BLOCKED port + MANY tickers -> the others pull ALL the slack -----
        # Port 2000 is gated: it holds its ONE in-flight ticker while the four fast
        # ports drain the other 19 from the shared queue. A static even partition would
        # pin 4 tickers to 2000 (3 of them stranded behind the block); the dynamic queue
        # must instead leave 2000 with exactly 1 and spread 19 across the rest. The gate
        # releases once 19 series are committed, so 2000 still finishes (no deadlock).
        PORTS = (2000, 3000, 4000, 5000, 6000)
        TKS = [(f"T{i:02d}", "1m") for i in range(20)]
        gate = threading.Event()
        seen = []
        glock = threading.Lock()


        def a_completed(ticker, interval, result):
            # The numeric progress line announces START, not durable completion.
            # Release the blocked port only after the other 19 actually finish.
            if not result.get("halt"):
                with glock:
                    seen.append((ticker, interval))
                    if len(seen) >= len(TKS) - 1:
                        gate.set()


        def run_gated():
            root = fresh_root()
            days = make_days(today, 70)
            mc = {}
            holder = {}
            cancel = threading.Event()

            def go():
                holder["rep"] = sk.nightly_gap_fill_parallel(
                    root, TKS, list(PORTS), since=since, today=today,
                    cancel=cancel,
                    on_series=a_completed,
                    adapter_factory=speed_factory({}, days, mc, gate_map={2000: gate}))

            dead = _joined_run(go, 120.0, cancel, gate.set)
            return holder.get("rep"), mc, dead


        rep, mc, dead = run_gated()
        check("A: no deadlock", not dead)
        check("A: report returned", rep is not None)
        if rep:
            per = part_counts(rep, PORTS)
            total = sum(per.values())
            static = len(TKS) / len(PORTS)                  # = 4 each, if static
            check("A: every ticker fetched exactly once",
                  total == len(TKS), f"{total}/{len(TKS)} per={per}")
            check("A: all series committed",
                  len(rep.get("series", [])) == len(TKS),
                  f"{len(rep.get('series', []))}/{len(TKS)}")
            check("A: BLOCKED port held exactly ONE ticker (didn't strand its share)",
                  per[2000] == 1, f"slow={per[2000]} per={per}")
            check("A: the other ports pulled ALL 19 remaining (no idle while work left)",
                  sum(per[p] for p in PORTS if p != 2000) == len(TKS) - 1,
                  f"per={per}")
            check("A: a fast port did MORE than the static share (dynamic redistribute)",
                  max(per[p] for p in PORTS if p != 2000) > static, f"per={per}")
            worked = [p for p in PORTS if per[p] > 0]
            check("A: ONE connection reused per working port (not one per ticker)",
                  all(mc.get(p, 0) == 1 for p in worked), f"make_counts={mc}")

        # --- B) whole-ticker jobs keep a ticker's 3 sessions on ONE port (combining) -
        EXT = [(f"E{i}", iv) for i in range(10) for iv in ("1m", "1m-pre", "1m-post")]
        rep, mc, dead = run(PORTS, EXT, {}, since, today)
        check("B: no deadlock", not dead)
        if rep:
            part = rep.get("_partition", {})
            where = {}                                      # ticker -> set(ports)
            for p, jobs in part.items():
                for (t, _iv) in jobs:
                    where.setdefault(t, set()).add(p)
            every_ticker_one_port = all(len(ps) == 1 for ps in where.values())
            check("B: each extended ticker's series all ran on ONE port "
                  "(combining preserved)", every_ticker_one_port,
                  f"split tickers={[t for t, ps in where.items() if len(ps) > 1]}")
            check("B: all 10 tickers present", len(where) == 10, f"{len(where)}")
            full = all(len([1 for (t, _i) in part.get(next(iter(ps)), [])
                            if t == tk]) == 3 for tk, ps in where.items())
            check("B: that one port carried ALL 3 of the ticker's sessions", full)

        # --- C) FEWER tickers than ports -> SPLIT series so spare ports still work ---
        FEW = [(f"F{i}", iv) for i in range(2) for iv in ("1m", "1m-pre", "1m-post")]
        rep, mc, dead = run(PORTS, FEW, {}, since, today)
        check("C: no deadlock", not dead)
        if rep:
            per = part_counts(rep, PORTS)
            worked = sum(1 for p in PORTS if per[p] > 0)
            check("C: more than #tickers ports worked (series split to fill the fleet)",
                  worked > 2, f"ports_worked={worked} per={per}")
            check("C: all 6 series fetched once",
                  sum(per.values()) == len(FEW), f"{sum(per.values())}/{len(FEW)}")

        print(f"\n{N[0]} checks, {len(FAILS)} failed")
        print("ALL PASS" if not FAILS else "FAILURES: " + "; ".join(FAILS))
        return 1 if FAILS else 0
    finally:
        sk.RECONNECT_BACKOFF_S = saved
        _fixture_root = None
