"""Acceptance harness for Fix Data pause-at-the-task-on-hand (M0, owner: CLAUDE).

Contract under test (FIXDATA_PAUSE_BOUNDARY_PLAN.md): once
`fix_data_pipeline.PAUSE_UNIT_BOUNDARIES` exists, a pause request settles after
the SERIES OPERATION in hand — one ticker-series scan step, one fill of one
(ticker, interval) series, one cross-check of one series — never after a whole
scan stage or a ticker's whole multi-series chain. The settled-pause contract
(paused substate only when every worker is parked, Cancel honored only from
that state, touched-series refresh always running, every unit executed exactly
once) must hold before AND after the feature.

Exit codes:
  0 = M1 accepted (invariants + full feature contract green)
  3 = feature absent (invariants green; today's coarse boundaries documented)
  1 = failure (invariant regression, or feature contract violation)

Offline by construction: the pipeline is injection-first — every scan/health/
fill/audit/refresh function here is a scripted fake; the operation gate is
redirected to a temp path so a live production run is never touched. No GUI,
no network, no ports, no bank access. All pause/resume choreography is
EVENT-CHAINED (triggers fire synchronously inside the progress callback);
timeouts exist only as failure guards, never as sequencing.

Codex MUST NOT weaken or remove assertions here; harness changes require
Claude sign-off (FIXDATA_PAUSE_BOUNDARY_PLAN.md section 4).
"""

from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

_HERE = Path(__file__).resolve().parent
for _p in (str(_HERE), str(_HERE.parent)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

if __name__ == "__main__":
    from fetch_a2_ibkr_workflows_selftest import Operations
    raise SystemExit(Operations.run_fixdata_reference_cli("fixdata_pause_reference"))


import operation_gate as og            # noqa: E402
import fix_data_pipeline as fdp        # noqa: E402


FAILURES = []
COUNT = [0]
WAIT_S = 30.0


def check(name, condition, detail=""):
    COUNT[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


def section(title):
    print(f"=== {title} ".ljust(68, "="))


class Env:
    """Scripted injected functions + thread-safe call logs."""

    def __init__(self, scan_series, fill_geometry, audit_items, *, audit_current=False):
        # scan_series: [(ticker, interval), ...] emitted as scan progress.
        # fill_geometry: {ticker: [interval, ...]} -> one missing day each.
        # audit_items: [(ticker, interval), ...] for series_fn.
        self.lock = threading.Lock()
        self.scan_calls = []
        self.health_calls = []
        self.fill_calls = []
        self.audit_calls = []
        self.refresh_calls = []
        self.scan_scopes = []
        self.refresh_scopes = []
        self.connects = []
        self.disconnects = []
        self._scan_series = list(scan_series)
        self._fills = {str(t).upper(): list(ivs)
                       for t, ivs in fill_geometry.items()}
        self._audits = list(audit_items)
        self._audit_current = audit_current

    def adapter_factory(self, port):
        env = self

        class _Adapter:
            def __init__(self):
                with env.lock:
                    env.connects.append(port)

            def disconnect(self):
                with env.lock:
                    env.disconnects.append(port)

        return _Adapter()

    def scan_fn(self, root, write=True, progress=None,
                kinds=("",), preserve_existing=False):
        with self.lock:
            self.scan_scopes.append((kinds, preserve_existing))
        count = len(self._scan_series)
        for index, (ticker, interval) in enumerate(self._scan_series):
            with self.lock:
                self.scan_calls.append((ticker, interval))
            if progress is not None:
                progress(index, count, ticker, interval)
        summary = {}
        for ticker, intervals in self._fills.items():
            for interval in intervals:
                summary[f"{ticker} {interval}"] = {
                    "missing_day_list": ["2026-01-05"],
                    "source_absent_list": [],
                }
        return {"summary": summary, "series_scanned": count,
                "verification_series": []}

    def health_fn(self, root):
        with self.lock:
            self.health_calls.append(time.monotonic())
        return {}

    def series_fn(self, root):
        return list(self._audits)

    def fill_fn(self, adapter, root, ticker, interval, days, cancel=None,
                progress=None, manifest_lock=None):
        with self.lock:
            self.fill_calls.append((str(ticker).upper(), str(interval)))
        time.sleep(0.01)
        return {"added": len(days), "source_absent": [], "blocked": [],
                "unfilled": []}

    def audit_fn(self, adapter, root, ticker, interval, cache_root):
        with self.lock:
            self.audit_calls.append((str(ticker).upper(), str(interval)))
        time.sleep(0.01)
        return {"status": "ok", "_verification_current": self._audit_current}

    def refresh_fn(self, root, touched, kinds=("",)):
        with self.lock:
            self.refresh_calls.append(list(touched))
            self.refresh_scopes.append(kinds)
        return {"missing_days_run": 0, "verification_series": []}

    def counts(self):
        with self.lock:
            return {"scan": len(self.scan_calls),
                    "health": len(self.health_calls),
                    "fill": len(self.fill_calls),
                    "audit": len(self.audit_calls)}


class Collector:
    """Progress recorder with synchronous event triggers and wait handles."""

    def __init__(self, env):
        self.env = env
        self.lock = threading.Lock()
        self.events = []
        self.triggers = []          # (name, predicate, action, once)
        self._fired = set()
        self.paused_evt = threading.Event()
        self.done_evt = threading.Event()
        self.snapshot_at_pause = None
        self.ports_before_pause = None
        self._last_ports = None
        self.paused_count = 0

    def on(self, name, predicate, action):
        self.triggers.append((name, predicate, action))

    def __call__(self, event):
        with self.lock:
            self.events.append(dict(event))
            if event.get("type") == "ports":
                self._last_ports = dict(event.get("states") or {})
            if (event.get("type") == "substate"
                    and event.get("state") == "paused"):
                self.paused_count += 1
                if self.snapshot_at_pause is None:
                    self.snapshot_at_pause = self.env.counts()
                    self.ports_before_pause = (
                        dict(self._last_ports) if self._last_ports else None)
                self.paused_evt.set()
            if event.get("type") == "done":
                self.done_evt.set()
        for name, predicate, action in self.triggers:
            if name in self._fired:
                continue
            try:
                hit = bool(predicate(event))
            except Exception:
                hit = False
            if hit:
                self._fired.add(name)
                action(event)

    def of(self, kind):
        with self.lock:
            return [e for e in self.events if e.get("type") == kind]

    def index_of(self, predicate):
        with self.lock:
            for i, e in enumerate(self.events):
                if predicate(e):
                    return i
        return None


def run_scenario(env, collector, *, ports=(4001,), pause_evt=None,
                 cancel_evt=None, root=None, verification_fn=None):
    pause_evt = pause_evt or threading.Event()
    cancel_evt = cancel_evt or threading.Event()
    box = {}

    def _target():
        box["out"] = fdp.run(
            root=str(root), ports=list(ports),
            adapter_factory=env.adapter_factory, scan_fn=env.scan_fn,
            health_fn=env.health_fn, fill_fn=env.fill_fn,
            audit_fn=env.audit_fn, series_fn=env.series_fn,
            refresh_fn=env.refresh_fn, progress=collector,
            pause_event=pause_evt, cancel_event=cancel_evt,
            verification_fn=verification_fn,
            run_id="fixdata-pause-ref")

    thread = threading.Thread(target=_target, daemon=True,
                              name="fixdata-pause-ref-run")
    thread.start()
    return thread, box, pause_evt, cancel_evt


def finish(thread, box, label, pause_evt=None, cancel_evt=None):
    thread.join(WAIT_S)
    if thread.is_alive():
        # Failure guard only: force the run down and report.
        if cancel_evt is not None:
            cancel_evt.set()
        if pause_evt is not None:
            pause_evt.clear()
        thread.join(5.0)
        check(f"{label}: run() returned", False, "timed out; forced down")
        return None
    return box.get("out")


def scan_event(done):
    return lambda e: e.get("type") == "scan" and e.get("done") == done


def refetch_done(n):
    return lambda e: e.get("type") == "refetch" and e.get("done") == n


def xcheck_done(n):
    return lambda e: e.get("type") == "xcheck" and e.get("done") == n


def main():
    print("=== fixdata_pause_reference ===")
    tmp = Path(tempfile.mkdtemp(prefix="fixdata-pause-ref-"))
    saved_lock = og.LOCK_PATH
    og.LOCK_PATH = tmp / "gate.lock"          # never the production gate
    try:
        feature = getattr(fdp, "PAUSE_UNIT_BOUNDARIES", None)

        # [S0] invariants: clean run, exactly-once units, exact order --------
        section("[S0] clean run: settled contract invariants (both paths)")
        env = Env(scan_series=[("AAA", "1m"), ("BBB", "1m")],
                  fill_geometry={"AAA": ["1m", "5m"], "BBB": ["1m"]},
                  audit_items=[("CCC", "1m"), ("AAA", "1m")],
                  audit_current=True)
        col = Collector(env)
        thread, box, pv, cv = run_scenario(env, col, root=tmp / "s0")
        out = finish(thread, box, "S0", pv, cv)
        if out is not None:
            check("S0: every fill unit ran exactly once, repairs in "
                  "deterministic single-port order",
                  env.fill_calls == [("AAA", "1m"), ("AAA", "5m"),
                                     ("BBB", "1m")],
                  repr(env.fill_calls))
            check("S0: every audit unit ran exactly once, after repairs",
                  sorted(env.audit_calls) == [("AAA", "1m"), ("CCC", "1m")]
                  and env.fill_calls and env.audit_calls,
                  repr(env.audit_calls))
            check("S0: touched-series refresh ran once over the filled series",
                  len(env.refresh_calls) == 1
                  and sorted(env.refresh_calls[0])
                  == [("AAA", "1m"), ("AAA", "5m"), ("BBB", "1m")],
                  repr(env.refresh_calls))
            check("S0: Fix Data kind scope is preserved through scan + refresh",
                  env.scan_scopes == [(("",), True)]
                  and env.refresh_scopes == [("",)],
                  repr((env.scan_scopes, env.refresh_scopes)))
            check("S0: clean end state (not cancelled, no port errors, "
                  "no unprocessed, counters match)",
                  out.get("cancelled") is False
                  and not out.get("port_errors")
                  and not out.get("unprocessed")
                  and out.get("fill_series") == 3
                  and out.get("acc_series") == 2
                  and out.get("checked") == 2)
            check("S0: adapter connected and disconnected by its owner",
                  env.connects == [4001] and env.disconnects == [4001])
            check("S0: health report ran exactly once",
                  len(env.health_calls) == 1)
            check("S0: a run without pause emits no paused substate",
                  col.paused_count == 0)

        if feature is None:
            # [D] document today's coarse boundaries (defect on record) ------
            section("[D1] TODAY: pause during the scan stage is not honored")
            env = Env(scan_series=[("T%02d" % i, "1m") for i in range(10)],
                      fill_geometry={}, audit_items=[])
            col = Collector(env)
            pv = threading.Event()
            col.on("pause@scan3", scan_event(3), lambda e: pv.set())
            thread, box, pv, cv = run_scenario(env, col, root=tmp / "d1",
                                               pause_evt=pv)
            ok = col.paused_evt.wait(WAIT_S)
            snap = col.snapshot_at_pause or {}
            check("D1: pause at scan 3/10 settles only AFTER the whole scan "
                  "AND the health suite (the stage-1 defect)",
                  ok and snap.get("scan") == 10 and snap.get("health") == 1,
                  f"snapshot at pause: {snap}")
            pv.clear()
            out = finish(thread, box, "D1", pv, cv)
            check("D1: resume completes cleanly",
                  out is not None and out.get("cancelled") is False)

            section("[D2] TODAY: a ticker's whole chain runs through a pause")
            env = Env(scan_series=[("AAA", "1m")],
                      fill_geometry={"AAA": ["1m", "5m", "15m"]},
                      audit_items=[])
            col = Collector(env)
            pv = threading.Event()
            col.on("pause@fill1", refetch_done(1), lambda e: pv.set())
            thread, box, pv, cv = run_scenario(env, col, root=tmp / "d2",
                                               pause_evt=pv)
            ok = col.paused_evt.wait(WAIT_S)
            snap = col.snapshot_at_pause or {}
            check("D2: pause during fill 1 of 3 settles only after ALL three "
                  "series filled (the chain defect)",
                  ok and snap.get("fill") == 3, f"snapshot at pause: {snap}")
            pv.clear()
            out = finish(thread, box, "D2", pv, cv)
            check("D2: resume completes with every unit exactly once",
                  out is not None and out.get("cancelled") is False
                  and env.fill_calls == [("AAA", "1m"), ("AAA", "5m"),
                                         ("AAA", "15m")])

            section("[D3] TODAY+FOREVER: cancel only from a settled pause")
            env = Env(scan_series=[("AAA", "1m"), ("BBB", "1m")],
                      fill_geometry={"AAA": ["1m"], "BBB": ["1m"]},
                      audit_items=[])
            col = Collector(env)
            pv = threading.Event()
            col.on("pause@fill1", refetch_done(1), lambda e: pv.set())
            thread, box, pv, cv = run_scenario(env, col, root=tmp / "d3",
                                               pause_evt=pv)
            ok = col.paused_evt.wait(WAIT_S)
            check("D3: pause settles at the chain boundary", ok)
            cv.set()
            pv.clear()                       # GUI order: cancel, then wake
            out = finish(thread, box, "D3", pv, cv)
            if out is not None:
                check("D3: cancel from settled pause fills nothing further "
                      "and reports the un-started ticker",
                      env.fill_calls == [("AAA", "1m")]
                      and out.get("cancelled") is True
                      and any("BBB" in str(u) for u in
                              out.get("unprocessed") or []),
                      f"fills={env.fill_calls} "
                      f"unprocessed={out.get('unprocessed')}")
                check("D3: touched-series refresh STILL runs after cancel",
                      len(env.refresh_calls) == 1
                      and env.refresh_calls[0] == [("AAA", "1m")],
                      repr(env.refresh_calls))
                check("D3: no port errors, no ambiguous in-flight work",
                      not out.get("port_errors")
                      and all("ambiguous" not in str(u).lower()
                              for u in out.get("unprocessed") or []))

            if FAILURES:
                return 1
            section("[probe] M1 unit-boundary surface")
            print("[M1 PENDING] fix_data_pipeline.PAUSE_UNIT_BOUNDARIES is "
                  "absent - pause still settles only after the whole scan"
                  "+health stage and whole ticker chains.")
            print("  Required: per-series pause boundaries (scan step / one "
                  "fill / one cross-check), scan-vs-health boundary, "
                  "cancel-from-mid-chain hold with per-item unprocessed "
                  "reporting. See FIXDATA_PAUSE_BOUNDARY_PLAN.md section 3.")
            return 3

        # [F] feature contract ------------------------------------------------
        section("[probe] M1 unit-boundary surface")
        check("probe: PAUSE_UNIT_BOUNDARIES declares the feature",
              feature is True, repr(feature))

        section("[F1] scan settles at the step in hand")
        env = Env(scan_series=[("T%02d" % i, "1m") for i in range(10)],
                  fill_geometry={}, audit_items=[])
        col = Collector(env)
        pv = threading.Event()
        col.on("pause@scan3", scan_event(3), lambda e: pv.set())
        thread, box, pv, cv = run_scenario(env, col, root=tmp / "f1",
                                           pause_evt=pv)
        ok = col.paused_evt.wait(WAIT_S)
        snap = col.snapshot_at_pause or {}
        check("F1: paused after at most one further scan step, scan NOT "
              "finished, health NOT started",
              ok and 3 <= snap.get("scan", 0) <= 4
              and snap.get("health") == 0, f"snapshot at pause: {snap}")
        pv.clear()
        out = finish(thread, box, "F1", pv, cv)
        check("F1: resume finishes the scan (10 steps) then health, clean end",
              out is not None and out.get("cancelled") is False
              and len(env.scan_calls) == 10 and len(env.health_calls) == 1)

        section("[F1b] cancel from a scan hold aborts the stage cleanly")
        env = Env(scan_series=[("T%02d" % i, "1m") for i in range(10)],
                  fill_geometry={"T00": ["1m"]}, audit_items=[])
        col = Collector(env)
        pv = threading.Event()
        col.on("pause@scan3", scan_event(3), lambda e: pv.set())
        thread, box, pv, cv = run_scenario(env, col, root=tmp / "f1b",
                                           pause_evt=pv)
        ok = col.paused_evt.wait(WAIT_S)
        check("F1b: scan hold reached", ok)
        cv.set()
        pv.clear()
        out = finish(thread, box, "F1b", pv, cv)
        if out is not None:
            check("F1b: cancelled result; health never ran; no worker "
                  "connected; no fills",
                  out.get("cancelled") is True and not env.health_calls
                  and not env.connects and not env.fill_calls,
                  f"health={len(env.health_calls)} connects={env.connects}")

        section("[F2] boundary between scan and health")
        env = Env(scan_series=[("T%02d" % i, "1m") for i in range(4)],
                  fill_geometry={}, audit_items=[])
        col = Collector(env)
        pv = threading.Event()
        col.on("pause@scan4", scan_event(4), lambda e: pv.set())
        thread, box, pv, cv = run_scenario(env, col, root=tmp / "f2",
                                           pause_evt=pv)
        ok = col.paused_evt.wait(WAIT_S)
        snap = col.snapshot_at_pause or {}
        check("F2: pause on the LAST scan step holds before health starts",
              ok and snap.get("scan") == 4 and snap.get("health") == 0,
              f"snapshot at pause: {snap}")
        pv.clear()
        out = finish(thread, box, "F2", pv, cv)
        check("F2: resume runs health exactly once and completes",
              out is not None and len(env.health_calls) == 1
              and out.get("cancelled") is False)

        section("[F3] a fill chain settles after the series in hand")
        env = Env(scan_series=[("AAA", "1m")],
                  fill_geometry={"AAA": ["1m", "5m", "15m"]},
                  audit_items=[])
        col = Collector(env)
        pv = threading.Event()
        col.on("pause@fill1", refetch_done(1), lambda e: pv.set())
        thread, box, pv, cv = run_scenario(env, col, root=tmp / "f3",
                                           pause_evt=pv)
        ok = col.paused_evt.wait(WAIT_S)
        snap = col.snapshot_at_pause or {}
        check("F3: paused with exactly the series in hand finished (1 of 3)",
              ok and snap.get("fill") == 1, f"snapshot at pause: {snap}")
        pv.clear()
        out = finish(thread, box, "F3", pv, cv)
        check("F3: resume runs the remaining two series exactly once each",
              out is not None and out.get("cancelled") is False
              and env.fill_calls == [("AAA", "1m"), ("AAA", "5m"),
                                     ("AAA", "15m")],
              repr(env.fill_calls))

        section("[F3b] a check chain settles after the series in hand")
        env = Env(scan_series=[("AAA", "1m")], fill_geometry={},
                  audit_items=[("AAA", "1m"), ("AAA", "5m"), ("AAA", "15m")])
        col = Collector(env)
        pv = threading.Event()
        col.on("pause@check1", xcheck_done(1), lambda e: pv.set())
        thread, box, pv, cv = run_scenario(env, col, root=tmp / "f3b",
                                           pause_evt=pv)
        ok = col.paused_evt.wait(WAIT_S)
        snap = col.snapshot_at_pause or {}
        check("F3b: paused with exactly one of three cross-checks done",
              ok and snap.get("audit") == 1, f"snapshot at pause: {snap}")
        pv.clear()
        out = finish(thread, box, "F3b", pv, cv)
        check("F3b: resume completes the remaining checks exactly once each",
              out is not None and out.get("cancelled") is False
              and env.audit_calls == [("AAA", "1m"), ("AAA", "5m"),
                                      ("AAA", "15m")],
              repr(env.audit_calls))

        section("[F4] the repair-to-check settle point still holds")
        env = Env(scan_series=[("AAA", "1m")],
                  fill_geometry={"AAA": ["1m"]}, audit_items=[("AAA", "1m")])
        col = Collector(env)
        pv = threading.Event()
        col.on("pause@fill1", refetch_done(1), lambda e: pv.set())
        thread, box, pv, cv = run_scenario(env, col, root=tmp / "f4",
                                           pause_evt=pv)
        ok = col.paused_evt.wait(WAIT_S)
        snap = col.snapshot_at_pause or {}
        check("F4: paused after the repair, before that ticker's check",
              ok and snap.get("fill") == 1 and snap.get("audit") == 0,
              f"snapshot at pause: {snap}")
        pv.clear()
        out = finish(thread, box, "F4", pv, cv)
        check("F4: resume runs the check exactly once; repair precedes check "
              "in the event order",
              out is not None and env.audit_calls == [("AAA", "1m")]
              and (col.index_of(refetch_done(1)) or 0)
              < (col.index_of(xcheck_done(1)) or -1))

        section("[F5] cancel from a mid-chain hold")
        env = Env(scan_series=[("AAA", "1m")],
                  fill_geometry={"AAA": ["1m", "5m", "15m"]},
                  audit_items=[])
        col = Collector(env)
        pv = threading.Event()
        col.on("pause@fill1", refetch_done(1), lambda e: pv.set())
        thread, box, pv, cv = run_scenario(env, col, root=tmp / "f5",
                                           pause_evt=pv)
        ok = col.paused_evt.wait(WAIT_S)
        check("F5: mid-chain hold reached", ok)
        cv.set()
        pv.clear()
        out = finish(thread, box, "F5", pv, cv)
        if out is not None:
            check("F5: no further series filled after cancel; the un-started "
                  "remainder is reported, not ambiguous",
                  env.fill_calls == [("AAA", "1m")]
                  and out.get("cancelled") is True
                  and out.get("unprocessed")
                  and all("ambiguous" not in str(u).lower()
                          for u in out.get("unprocessed") or []),
                  f"fills={env.fill_calls} "
                  f"unprocessed={out.get('unprocessed')}")
            check("F5: completed work stays counted and refresh still runs",
                  out.get("added", 0) >= 1 and len(env.refresh_calls) == 1
                  and env.refresh_calls[0] == [("AAA", "1m")],
                  f"added={out.get('added')} refresh={env.refresh_calls}")
            check("F5: no port errors from the cancel path",
                  not out.get("port_errors"), repr(out.get("port_errors")))

        section("[F6] two ports both settle at their own series boundary")
        env = Env(scan_series=[("AAA", "1m"), ("BBB", "1m")],
                  fill_geometry={"AAA": ["1m", "5m"], "BBB": ["1m", "5m"]},
                  audit_items=[])
        col = Collector(env)
        pv = threading.Event()
        col.on("pause@fill1", refetch_done(1), lambda e: pv.set())
        thread, box, pv, cv = run_scenario(env, col, ports=(4001, 4002),
                                           root=tmp / "f6", pause_evt=pv)
        ok = col.paused_evt.wait(WAIT_S)
        snap = col.snapshot_at_pause or {}
        ports_snap = col.ports_before_pause or {}
        check("F6: settled with at most one series per port after the "
              "trigger (no chain ran through)",
              ok and 1 <= snap.get("fill", 0) <= 2,
              f"snapshot at pause: {snap}")
        check("F6: every port state parked when the settle was announced",
              ports_snap and all(v in ("paused", "done")
                                 for v in ports_snap.values()),
              repr(ports_snap))
        pv.clear()
        out = finish(thread, box, "F6", pv, cv)
        check("F6: resume completes all four series exactly once",
              out is not None and out.get("cancelled") is False
              and sorted(env.fill_calls) == [("AAA", "1m"), ("AAA", "5m"),
                                             ("BBB", "1m"), ("BBB", "5m")],
              repr(env.fill_calls))

        section("[F7] no-pause identity")
        print("F7 is enforced by [S0] above running against the SAME build: "
              "exact single-port sequence, exactly-once units, zero paused "
              "substates without a pause request.")
        return 1 if FAILURES else 0
    finally:
        og.LOCK_PATH = saved_lock


def stale_audit_contract():
    """Separately counted C2 checks; never alter the original pause oracle."""
    from unittest.mock import patch
    import fetch_operation_report as reports

    checks, failures = [], []

    def supplemental(name, condition):
        checks.append(name)
        print(f"[{'PASS' if condition else 'FAIL'}] STALE: {name}")
        if not condition:
            failures.append(name)

    section("[STALE] supplemental audit-currentness contract")
    env = Env(scan_series=[("AAA", "1m"), ("BBB", "1m")],
              fill_geometry={"AAA": ["1m", "5m"], "BBB": ["1m"]},
              audit_items=[("CCC", "1m"), ("AAA", "1m")])
    credits, synced = [], []
    report_fds = {}
    real_mkstemp, real_fsync = reports.tempfile.mkstemp, reports.os.fsync

    def observe_create(*args, **kwargs):
        fd, name = real_mkstemp(*args, **kwargs)
        if kwargs.get("prefix") == ".operation-":
            report_fds[fd] = Path(name)
        return fd, name

    def observe_fsync(fd):
        real_fsync(fd)
        path = report_fds.get(fd)
        if path is not None and path.exists():
            synced.append(json.loads(path.read_text(encoding="utf-8")))

    with tempfile.TemporaryDirectory(prefix="fixdata-stale-ref-") as temporary:
        saved_lock = og.LOCK_PATH
        og.LOCK_PATH = Path(temporary) / "gate.lock"
        try:
            with patch.object(reports.tempfile, "mkstemp", observe_create), \
                    patch.object(reports.os, "fsync", observe_fsync):
                thread, box, pv, cv = run_scenario(env, Collector(env),
                    root=Path(temporary) / "stale",
                    verification_fn=lambda *args: credits.append(args))
                try:
                    thread.join(WAIT_S)
                finally:
                    if thread.is_alive():
                        cv.set()
                        pv.clear()
                        thread.join(5.0)
                out = box.get("out") or {}
                supplemental("run returned and adapter drained",
                    not thread.is_alive() and bool(out)
                    and env.connects == [4001] and env.disconnects == [4001])
                supplemental("both audits counted without verification credit",
                    sorted(env.audit_calls) == [("AAA", "1m"), ("CCC", "1m")]
                    and out.get("checked") == 2 and out.get("acc_series") == 2
                    and credits == [])
                debt = [(t, "1m", "check", "audit-not-current")
                        for t in ("AAA", "CCC")]
                supplemental("exact AAA/CCC 1m debt preserved",
                    sorted(out.get("unprocessed") or []) == debt)
                report = out.get("fetch_ledger") or {}
                supplemental("sealed ledger is partial and UNVERIFIED delivery",
                    report.get("ledger_verified") is True
                    and report.get("verified") is False
                    and report.get("state") == "UNVERIFIED"
                    and report.get("outcome") == "partial")
                path = report.get("report_path")
                persisted = json.loads(Path(path).read_text(encoding="utf-8")) if path else {}
                supplemental("companion retains exact debt and delivery outcome",
                    sorted(persisted.get("fix_data", {}).get("unprocessed") or [])
                    == [list(item) for item in debt]
                    and persisted.get("outcome") == "partial"
                    and persisted.get("state") == "UNVERIFIED"
                    and persisted.get("verified") is False)
                supplemental("matching companion really fsynced before return",
                    bool(persisted) and persisted in synced)
        finally:
            og.LOCK_PATH = saved_lock
    print(f"{len(checks)} supplemental checks, {len(failures)} failed")
    return 1 if failures else 0


def cli_main():
    code = main()
    print(f"\n{COUNT[0]} checks, {len(FAILURES)} failed")
    if FAILURES:
        print("FAILURES: " + ", ".join(FAILURES))
    else:
        print("ALL PASS" + (" (feature pending)" if code == 3 else ""))
    if stale_audit_contract():
        code = 1
    print(f"harness exit={code}")
    return code
