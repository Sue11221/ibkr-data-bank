"""Headless self-tests for the multi-port daily-logout RESTART subsystem.

No TWS, no ports, no GUI, no network: every heavy step (the actual fetch, the
launch/login/enable, the socket probe) is stubbed, so this exercises the
ORCHESTRATION — which ports get restarted, which tickers get re-fetched, how
partial failures are handled — deterministically and fast.

    python engine/tws_restart_selftest.py        # -> '... N passed' + exit 0/1

Covers:
  * stock_ibkr.partition_tickers           (round-robin, de-dup, empty)
  * stock_ibkr._merge_recovery             (drop recovered, fold new aborts)
  * stock_ibkr resilient core/public gate  (pre-flight, recover, cap, targeting)
  * tws_launch._pid_for_config             (config dir -> PID mapping)
  * tws_launch.relaunch_one                (step sequence + failure paths)
  * tws_launch.restart_dead / fleet_health (skip up, restart down, isolate)
  * tws_restart_handshake                  (popup races, guard, launcher, cleanup)
"""
import ast
from datetime import datetime, timezone
import json
import shutil
import sys
import tempfile
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import stock_ibkr as sk
import operation_gate as og
import tws_launch as tl
import tws_bringup as tb
import tws_restart_handshake as th

_PASS = [0]
_FAIL = [0]

# relaunch_one records into <base_dir>/fleet.json; point the tests at a throwaway
# base so they NEVER touch the production %USERPROFILE%\TwsMulti\fleet.json.
_TEST_BASE = tempfile.mkdtemp(prefix="tws_test_base_")


def check(cond, label):
    if cond:
        _PASS[0] += 1
    else:
        _FAIL[0] += 1
        print(f"  FAIL: {label}")


class _Saved:
    """Monkeypatch module attributes, restoring them on exit."""

    def __init__(self, module, **attrs):
        self.module = module
        self.attrs = attrs
        self.old = {}

    def __enter__(self):
        for k, v in self.attrs.items():
            self.old[k] = getattr(self.module, k)
            setattr(self.module, k, v)
        return self

    def __exit__(self, *exc):
        for k, v in self.old.items():
            setattr(self.module, k, v)


class _NoopResourceWarningSweeper:
    """Keep orchestration tests headless unless a test opts into the real one."""

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def record_result(self, *_args, **_kwargs):
        return None


def _noop_resource_warning_sweeper(**_kwargs):
    return _NoopResourceWarningSweeper()


class _HandshakeGuard:
    def __init__(self, owner, on_progress):
        self.owner = owner
        self.on_progress = on_progress
        self.abort = owner.aborted

    def start(self):
        self.owner.events.append("start")
        if "start" in self.owner.fail:
            raise RuntimeError("guard start failed")

    def stop(self):
        self.owner.events.append("stop")
        if "stop" in self.owner.fail:
            raise RuntimeError("guard stop failed for DU123456")

    def block(self):
        self.owner.events.append("block")
        if "block" in self.owner.fail:
            raise RuntimeError("guard block failed")

    def unblock(self):
        self.owner.events.append("unblock")
        if "unblock" in self.owner.fail:
            raise RuntimeError("guard unblock failed")

    def aborted(self):
        self.owner.events.append("aborted")
        if "aborted" in self.owner.fail:
            raise RuntimeError("guard state failed")
        return self.abort


class _HandshakeGuardMod:
    def __init__(self, fail=(), aborted=False):
        self.fail = set(fail)
        self.aborted = bool(aborted)
        self.events = []
        self.guard = None

    def InputGuard(self, on_progress=None, max_block_s=None):
        self.events.append(("init", max_block_s))
        if "init" in self.fail:
            raise RuntimeError("guard init failed")
        self.guard = _HandshakeGuard(self, on_progress)
        return self.guard

    def minimize_all(self):
        self.events.append("minimize")
        if "minimize" in self.fail:
            raise RuntimeError("minimize failed")

    def restore_all(self):
        self.events.append("restore")
        if "restore" in self.fail:
            raise RuntimeError("restore failed for owner@example.com")


def _rendered_decision(token=th.USER_CONFIRMED, go=True):
    def schedule(state):
        th.render_popup_safely(state, lambda: None, lambda: None)
        state.settle(go, token, f"fixture decision {token}")
    return schedule


# --- a configurable fake fetch (gap_fill_parallel shape) -------------------

class FakeFetch:
    """Faithful stand-in for gap_fill_parallel. `script` is a per-call list of
    {'abort': {port: err}, 'cancelled': bool}. Crucially it mimics the real
    <2-port SERIAL fallback: called with a single port it returns the raw
    serial gap_fill shape (NO per_port / aborted_ports — a connect-fail sets
    only the top-level 'aborted'), which is exactly the shape that the
    single-port recovery refetch must normalize before merging."""

    def __init__(self, script):
        self.script = script
        self.calls = []

    def __call__(self, root, selections, ports, **kw):
        idx = len(self.calls)
        self.calls.append({"selections": list(selections),
                           "ports": list(ports), "kw": kw})
        spec = self.script[min(idx, len(self.script) - 1)]
        abort = dict(spec.get("abort", {}))
        cancelled = bool(spec.get("cancelled", False))
        chunks = sk.partition_tickers(selections, ports)
        pl = list(chunks)
        if len(pl) < 2:                       # SERIAL fallback shape
            p = pl[0] if pl else None
            if p is not None and p in abort:
                return {"series": [], "totals": {}, "cancelled": cancelled,
                        "aborted": abort[p], "account": None}
            series = [{"ticker": t, "interval": iv, "added": 1}
                      for (t, iv) in (chunks.get(p, []) if p is not None
                                      else [])]
            return {"series": series, "totals": {"added": len(series)},
                    "cancelled": cancelled, "account": "DUx"}
        series, per_port = [], {}             # PARALLEL merged shape
        for p in chunks:
            if p in abort:
                per_port[p] = {"aborted": abort[p], "series": 0, "added": 0}
                continue
            for (t, iv) in chunks[p]:
                series.append({"ticker": t, "interval": iv, "added": 1})
            per_port[p] = {"aborted": None, "series": len(chunks[p]),
                           "added": len(chunks[p])}
        rep = {"series": series, "totals": {"added": len(series)},
               "cancelled": cancelled, "per_port": per_port}
        if abort:
            rep["aborted_ports"] = dict(abort)
        if not series and abort:
            rep["aborted"] = "all accounts failed"
        return rep


class FakeCancel:
    def __init__(self, value):
        self.value = value

    def is_set(self):
        return self.value


class Restarter:
    """restart_port stub. results: {port: [bool, ...]} consumed per call;
    absent port -> always True."""

    def __init__(self, results=None):
        self.results = {int(k): list(v) for k, v in (results or {}).items()}
        self.calls = []

    def __call__(self, port):
        self.calls.append(int(port))
        seq = self.results.get(int(port))
        if not seq:
            return True
        return seq.pop(0)


SEL4 = [("AAPL", "1m"), ("MSFT", "1m"), ("GOOGL", "1m"), ("AMZN", "1m")]
# sorted tickers AAPL,AMZN,GOOGL,MSFT round-robin over [2000,3000]:
#   2000 -> AAPL,GOOGL   3000 -> AMZN,MSFT
CHUNK_3000 = [("AMZN", "1m"), ("MSFT", "1m")]


def test_partition():
    chunks = sk.partition_tickers(SEL4, [2000, 3000])
    check(chunks[2000] == [("AAPL", "1m"), ("GOOGL", "1m")], "partition 2000")
    check(chunks[3000] == CHUNK_3000, "partition 3000")
    # de-dup ports (2000 repeated) — same as single 2000
    dd = sk.partition_tickers(SEL4, [2000, 2000])
    check(list(dd) == [2000] and len(dd[2000]) == 4, "partition de-dups ports")
    check(sk.partition_tickers([], [2000, 3000]) == {2000: [], 3000: []},
          "partition empty selections")
    check(sk.partition_tickers(SEL4, []) == {}, "partition no ports")


def test_partition_balanced():
    # work-balanced (LPT): a HEAVY ticker must NOT share a port with the two
    # LIGHT ones — round-robin would pile a light onto the heavy port.
    sels = [("HEAVY", "1m"), ("LITE1", "1m"), ("LITE2", "1m")]
    ch = sk.partition_tickers(sels, [2000, 3000],
                              weights={"HEAVY": 100, "LITE1": 1, "LITE2": 1})
    heavy_port = next(p for p, v in ch.items() if ("HEAVY", "1m") in v)
    other = next(p for p in ch if p != heavy_port)
    check(ch[heavy_port] == [("HEAVY", "1m")],
          "LPT: the heavy ticker gets a port to itself")
    check(("LITE1", "1m") in ch[other] and ("LITE2", "1m") in ch[other],
          "LPT: both light tickers share the OTHER port (balances the work)")
    # >= ports tickers -> no port idle, and the partition is deterministic
    w = {"A": 3, "B": 2, "C": 1}
    a = sk.partition_tickers([("A", "1m"), ("B", "1m"), ("C", "1m")], [1, 2, 3], w)
    b = sk.partition_tickers([("A", "1m"), ("B", "1m"), ("C", "1m")], [1, 2, 3], w)
    check(all(a[p] for p in (1, 2, 3)), "LPT: 3 tickers / 3 ports -> none idle")
    check(a == b, "LPT partition is deterministic (recovery re-fetch is safe)")
    # no weights -> unchanged round-robin contract
    check(sk.partition_tickers(SEL4, [2000, 3000])[2000]
          == [("AAPL", "1m"), ("GOOGL", "1m")],
          "without weights the partition is the old round-robin")


def test_partition_split():
    # FEWER tickers than ports: 2 tickers x 3 series (1m/-pre/-post) over 5
    # ports. allow_split spreads the 6 series so NONE of the 5 ports idle —
    # whole-ticker would use only 2.
    sels = [(t, iv) for t in ("AAA", "BBB")
            for iv in ("1m", "1m-pre", "1m-post")]
    ch = sk.partition_tickers(sels, [1, 2, 3, 4, 5], allow_split=True)
    used = [p for p in (1, 2, 3, 4, 5) if ch[p]]
    check(len(used) == 5, f"split: all 5 ports get work (got {len(used)})")
    check(sorted(s for v in ch.values() for s in v) == sorted(sels),
          "split: every series placed exactly once")
    aaa_ports = {p for p in ch if any(t == "AAA" for t, _i in ch[p])}
    check(len(aaa_ports) > 1,
          "split: one ticker's series span multiple ports (the whole point)")
    # default (no allow_split) stays whole-ticker -> only #tickers ports used
    ch2 = sk.partition_tickers(sels, [1, 2, 3, 4, 5])
    check(sum(1 for p in (1, 2, 3, 4, 5) if ch2[p]) == 2,
          "no-split: whole-ticker uses only #tickers ports (2)")
    # split is a NO-OP when tickers >= ports (nothing to spread)
    big = [(f"T{i}", "1m") for i in range(6)]
    ch3 = sk.partition_tickers(big, [1, 2, 3], allow_split=True)
    check(all(ch3[p] for p in (1, 2, 3)) and
          sorted(s for v in ch3.values() for s in v) == sorted(big),
          "split no-op when tickers>=ports: whole-ticker, all ports used")
    # and a no-op when a single ticker has only ONE series (can't split a unit)
    ch4 = sk.partition_tickers([("AAA", "1m")], [1, 2, 3], allow_split=True)
    check(sum(1 for p in (1, 2, 3) if ch4[p]) == 1,
          "split: a lone single-series ticker still occupies just one port")


def test_merge_recovery():
    base = {"series": [{"ticker": "AAPL"}], "totals": {"added": 1},
            "per_port": {2000: {"added": 1}, 3000: {"aborted": "x"}},
            "aborted_ports": {3000: "lost"}, "aborted": "partial",
            "cancelled": False}
    extra = {"series": [{"ticker": "MSFT"}], "totals": {"added": 2},
             "per_port": {3000: {"added": 2}}, "cancelled": False}
    out = sk._merge_recovery(base, extra, [3000])
    check("aborted_ports" not in out, "merge clears recovered abort")
    check(out["totals"]["added"] == 3, "merge sums totals")
    check(len(out["series"]) == 2, "merge appends series")
    check(out["per_port"][3000]["added"] == 2, "merge overrides per_port")
    check("aborted" not in out, "merge clears top-level abort when series exist")
    # a NEW abort in the refetch is preserved
    extra2 = {"series": [], "totals": {}, "aborted_ports": {3000: "again"},
              "per_port": {}, "cancelled": False}
    out2 = sk._merge_recovery(base, extra2, [3000])
    check(out2.get("aborted_ports") == {3000: "again"},
          "merge folds in a fresh abort")


def _resilient_call(root, selections, ports, **kw):
    # The public wrapper intentionally owns the production operation lease.
    # This headless orchestration harness calls the named undecorated core so an
    # unrelated live desktop run cannot make an offline selftest contend for it.
    return sk._gap_fill_parallel_resilient_core(
        root, selections, ports, **kw)


def _resilient(fake, **kw):
    with _Saved(sk, gap_fill_parallel=fake):
        return _resilient_call("root", SEL4, [2000, 3000], **kw)


def test_named_resilient_core_gate_boundary():
    core = sk._gap_fill_parallel_resilient_core
    public = sk.gap_fill_parallel_resilient
    check(core is not public
          and not getattr(core, "_holds_fetch_run_gate", False),
          "named resilient core is a distinct undecorated function")
    check(getattr(public, "_holds_fetch_run_gate", False),
          "public resilient wrapper retains the production fetch gate")

    temp = Path(tempfile.mkdtemp(prefix="restart_core_gate_"))
    lock_path = temp / "operation.lock"
    old_lock_path = og.LOCK_PATH
    fake = FakeFetch([{}])
    lease = None
    try:
        og.LOCK_PATH = lock_path
        lease = og.acquire("external_sweep", owner="restart core selftest")
        with _Saved(sk, gap_fill_parallel=fake):
            rep = core("root", SEL4, [2000, 3000])
            check(rep.get("recovery_rounds") == 0 and len(fake.calls) == 1,
                  "named core runs headlessly without acquiring the gate")
            try:
                public("root", SEL4, [2000, 3000])
                blocked = False
            except og.OperationBusy:
                blocked = True
            check(blocked and len(fake.calls) == 1,
                  "public wrapper blocks on a conflicting operation before fetch")
    finally:
        if lease is not None:
            lease.release()
        og.LOCK_PATH = old_lock_path
        check(og.status(path=lock_path)["available"],
              "temporary gate is released after the boundary test")
        shutil.rmtree(temp, ignore_errors=True)


def test_clean_run():
    fake = FakeFetch([{}])
    r = Restarter()
    rep = _resilient(fake, restart_port=r, port_up=lambda p: True)
    check(len(fake.calls) == 1, "clean run fetches once")
    check(r.calls == [], "clean run never restarts")
    check(rep["recovery_rounds"] == 0, "clean run 0 recovery rounds")
    check("aborted_ports" not in rep, "clean run no aborted_ports")


def test_preflight_restart():
    fake = FakeFetch([{}])
    r = Restarter()
    events = []
    rep = _resilient(fake, restart_port=r, port_up=lambda p: p != 3000,
                     on_recover=lambda ph, p, ok: events.append((ph, p, ok)))
    check(r.calls == [3000], "pre-flight restarts only the down port")
    check(("preflight", 3000, True) in events, "pre-flight notifies on_recover")
    check(rep["recovery_rounds"] == 0, "pre-flight isn't a recovery round")
    check(rep["restart_events"][0]["phase"] == "preflight"
          and rep["restart_events"][0]["round"] == 0,
          "pre-flight event uses phase=preflight and round 0")


def test_preflight_events_survive_interim_and_recovery_merge():
    fake = FakeFetch([{"abort": {3000: "lost"}}, {}])
    progress = []
    rep = _resilient(
        fake, restart_port=Restarter(), port_up=lambda p: p != 2000,
        progress=progress.append)
    interim = [item[1] for item in progress
               if isinstance(item, tuple) and item[0] == "report"]
    check(len(interim) == 1
          and [(e["phase"], e["port"])
               for e in interim[0].get("restart_events", [])]
          == [("preflight", 2000)],
          "interim report retains the completed pre-flight restart event")
    check([(e["phase"], e["port"]) for e in rep["restart_events"]]
          == [("preflight", 2000), ("recover", 3000)],
          "final report retains pre-flight evidence across recovery merge")


def test_final_report_rewritten_after_recovery():
    root = Path(tempfile.mkdtemp(prefix="restart_report_"))
    run = "ibkr-20260714-120000"
    scripted = FakeFetch([{"abort": {3000: "lost"}}, {}])

    def fake(root_arg, selections, ports, **kw):
        rep = scripted(root_arg, selections, ports, **kw)
        if len(scripted.calls) == 1:
            rep.update({"run": run, "root": str(root_arg),
                        "report_path": None})
        return rep

    try:
        with _Saved(sk, gap_fill_parallel=fake):
            rep = _resilient_call(
                root, SEL4, [2000, 3000], restart_port=Restarter())
        path = Path(rep["report_path"])
        saved = json.loads(path.read_text(encoding="utf-8"))
        check(path == root / "_ingest_reports" / run / "report.json",
              "final resilient report uses the internally derived run path")
        check(saved.get("recovery_rounds") == 1
              and not saved.get("aborted_ports")
              and saved.get("report_path") == str(path),
              "final report.json contains post-recovery state and its own path")
        check([(e["phase"], e["port"])
               for e in saved.get("restart_events", [])]
              == [("recover", 3000)],
              "final report.json durably contains restart decision evidence")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_final_report_failure_is_diagnostic():
    root = Path(tempfile.mkdtemp(prefix="restart_report_fail_"))
    rep = {"run": "ibkr-20260714-120001", "totals": {"added": 7}}

    def fail_write(_path, _payload):
        raise OSError("disk failed for trader@example.com DU123456 " + "x" * 400)

    try:
        with _Saved(sk.ss, _atomic_write_bytes=fail_write):
            result = sk._write_parallel_report(root, rep, stage="final")
        note = rep.get("notes", [""])[-1]
        check(result is None and rep.get("report_path") is None
              and rep["totals"] == {"added": 7},
              "report-write failure is diagnostic and preserves completed data")
        check(len(note) <= sk._RESTART_REASON_CAP
              and "trader@example.com" not in note and "DU123456" not in note
              and "[redacted-email]" in note
              and "[redacted-account]" in note,
              "report-write failure note is bounded and redacted")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_final_report_rejects_non_child_run_id():
    writes = []
    rep = {"run": "../escape", "totals": {"added": 7}}
    with _Saved(sk.ss, _atomic_write_bytes=lambda *args: writes.append(args)):
        result = sk._write_parallel_report("root", rep, stage="final")
    check(result is None and rep.get("report_path") is None and not writes
          and "invalid run id" in rep.get("notes", [""])[-1],
          "report writer rejects a run token that could escape its direct child")


def test_recover_one_targeted():
    fake = FakeFetch([{"abort": {3000: "lost"}}, {}])
    r = Restarter()
    rep = _resilient(fake, restart_port=r, port_up=lambda p: True)
    check(r.calls == [3000], "recover restarts only the aborted port")
    check(len(fake.calls) == 2, "recover triggers exactly one refetch")
    check(fake.calls[1]["ports"] == [3000], "refetch runs ONLY on recovered port")
    check(fake.calls[1]["selections"] == CHUNK_3000,
          "refetch targets ONLY the aborted port's tickers")
    check("aborted_ports" not in rep, "successful recovery clears aborted_ports")
    check(rep["recovery_rounds"] == 1, "one recovery round")
    check({s["ticker"] for s in rep["series"]} == {"AAPL", "GOOGL", "AMZN",
                                                   "MSFT"},
          "all four tickers present after recovery")


def test_recover_restart_fails():
    fake = FakeFetch([{"abort": {3000: "lost"}}, {}])
    r = Restarter({3000: [False]})        # restart refuses
    rep = _resilient(fake, restart_port=r)
    check(len(fake.calls) == 1, "no refetch when restart fails")
    check(rep.get("aborted_ports") == {3000: "lost"},
          "failed restart leaves the port aborted")
    check(rep["recovery_rounds"] == 1, "counts the attempted round")
    event = rep["restart_events"][0]
    check(event["decision"] == "legacy_callback"
          and event["callback_ok"] is False and event["ok"] is False,
          "legacy False callback is retained as an explicit failed event")


def test_structured_restart_event():
    fake = FakeFetch([{"abort": {3000: "lost"}}, {}])
    progress, notices = [], []

    def restart(_port):
        return {"ok": True, "attempted": True,
                "decision": "user_confirmed",
                "reason": "launcher verified the port"}

    rep = _resilient(
        fake, restart_port=restart, port_up=lambda _p: True,
        progress=progress.append,
        on_recover=lambda phase, port, ok: notices.append((phase, port, ok)))
    event = rep["restart_events"][0]
    check(event == {
        "phase": "recover", "round": 1, "port": 3000,
        "decision": "user_confirmed", "attempted": True,
        "callback_ok": True, "final_probe": "not_run", "ok": True,
        "recovered_by_probe": False, "reason": "launcher verified the port",
    }, "structured callback fields persist in one normalized recovery event")
    check(notices == [("recover", 3000, True)],
          "on_recover receives the structured callback's final result")
    check(any(isinstance(line, str)
              and "decision=user_confirmed" in line for line in progress),
          "structured restart event emits a deterministic progress line")


def test_failed_restart_self_recovers_on_probe():
    fake = FakeFetch([{"abort": {3000: "lost"}}, {}])

    def timed_out(_port):
        return {"ok": False, "attempted": False,
                "decision": "popup_decision_timeout",
                "reason": "confirmation timed out"}

    rep = _resilient(fake, restart_port=timed_out, port_up=lambda _p: True)
    event = rep["restart_events"][0]
    check(len(fake.calls) == 2 and "aborted_ports" not in rep,
          "a callback failure rescued by the final probe is re-fetched")
    check(event["callback_ok"] is False and event["final_probe"] == "up"
          and event["ok"] is True and event["recovered_by_probe"] is True,
          "self-recovered port preserves callback failure plus probe recovery")
    check(event["attempted"] is False
          and event["decision"] == "popup_decision_timeout",
          "probe recovery never rewrites original decision/attempted evidence")


def test_failed_restart_stays_down_after_probe():
    fake = FakeFetch([{"abort": {3000: "lost"}}, {}])
    probes = {3000: 0}

    def probe(port):
        if port != 3000:
            return True
        probes[3000] += 1
        return probes[3000] == 1       # preflight up; final probe down

    rep = _resilient(fake, restart_port=lambda _p: False, port_up=probe)
    event = rep["restart_events"][0]
    check(len(fake.calls) == 1 and rep.get("aborted_ports") == {3000: "lost"},
          "a final down probe leaves the failed port aborted")
    check(event["final_probe"] == "down" and event["ok"] is False
          and event["recovered_by_probe"] is False,
          "down final probe is explicit and cannot become success")


def test_failed_restart_probe_error():
    fake = FakeFetch([{"abort": {3000: "lost"}}, {}])
    probes = {3000: 0}

    def probe(port):
        if port != 3000:
            return True
        probes[3000] += 1
        if probes[3000] == 1:
            return True
        raise RuntimeError("probe failed for DU123456")

    rep = _resilient(fake, restart_port=lambda _p: False, port_up=probe)
    event = rep["restart_events"][0]
    check(event["final_probe"] == "error" and event["ok"] is False,
          "raising final probe remains a failed recovery")
    check("DU123456" not in event["reason"]
          and "[redacted-account]" in event["reason"],
          "final-probe error detail is redacted before persistence")


def test_invalid_callback_returns():
    invalid = [None, "yes", {"ok": True}]
    for value in invalid:
        fake = FakeFetch([{"abort": {3000: "lost"}}])
        rep = _resilient(fake, restart_port=lambda _p, v=value: v)
        event = rep["restart_events"][0]
        check(event["decision"] == "invalid_callback_return"
              and event["reason"] == "invalid_callback_return"
              and event["ok"] is False,
              f"invalid restart value {type(value).__name__} fails explicitly")
        check(len(fake.calls) == 1,
              f"invalid restart value {type(value).__name__} never re-fetches")

    normalized = sk._normalize_restart_outcome({
        "ok": False, "attempted": False,
        "decision": "popup_decision_timeout", "reason": "timed out",
        "popup_timing": {"scheduled_at": "not-a-time",
                         "rendered_at": "2026-07-16T23:55:11.000Z"},
    })
    check(normalized["popup_timing"]["scheduled_at"] is None
          and normalized["popup_timing"]["rendered_at"]
          == "2026-07-16T23:55:11.000Z",
          "popup diagnostics are normalized without changing the decision")


def test_batch_invalid_and_missing_results():
    for value in (None, "yes", {"wrong": True}):
        fake = FakeFetch([{"abort": {2000: "lost", 3000: "lost"}}])
        with _Saved(sk, gap_fill_parallel=fake):
            invalid = _resilient_call(
                "root", SEL4, [2000, 3000],
                restart_ports=lambda _ps, v=value: v)
        events = invalid["restart_events"]
        check([e["port"] for e in events] == [2000, 3000]
              and all(e["decision"] == "invalid_callback_return"
                      and not e["ok"] for e in events),
              f"invalid batch {type(value).__name__} fails every requested port")

    fake = FakeFetch([{"abort": {2000: "lost", 3000: "lost"}}, {}])
    with _Saved(sk, gap_fill_parallel=fake):
        missing = _resilient_call(
            "root", SEL4, [2000, 3000],
            restart_ports=lambda _ps: {2000: True}, max_recover_rounds=1)
    by_port = {e["port"]: e for e in missing["restart_events"]}
    check(by_port[2000]["decision"] == "legacy_callback"
          and by_port[2000]["ok"]
          and by_port[3000]["decision"] == "missing_batch_result"
          and not by_port[3000]["ok"],
          "partial batch mapping normalizes success and missing requested port")
    check(fake.calls[1]["ports"] == [2000]
          and missing.get("aborted_ports") == {3000: "lost"},
          "missing batch result refetches only the explicitly recovered port")


def test_batch_legacy_structured_decline_and_exception_matrix():
    fake = FakeFetch([{"abort": {2000: "lost", 3000: "lost"}}, {}])
    with _Saved(sk, gap_fill_parallel=fake):
        legacy = _resilient_call(
            "root", SEL4, [2000, 3000],
            restart_ports=lambda _ps: {2000: True, "3000": False},
            max_recover_rounds=1)
    events = legacy["restart_events"]
    check([(e["port"], e["decision"], e["callback_ok"])
           for e in events]
          == [(2000, "legacy_callback", True),
              (3000, "legacy_callback", False)],
          "mixed legacy batch booleans normalize in requested-port order")
    check(fake.calls[1]["ports"] == [2000]
          and legacy.get("aborted_ports") == {3000: "lost"},
          "mixed legacy batch refetches only its successful port")

    fake = FakeFetch([{"abort": {2000: "lost", 3000: "lost"}}, {}])

    def structured(_ports):
        return {
            2000: {"ok": True, "attempted": True,
                   "decision": "user_confirmed",
                   "reason": "operator approved restart"},
            "3000": {"ok": False, "attempted": False,
                     "decision": "user_declined",
                     "reason": "operator declined restart"},
        }

    with _Saved(sk, gap_fill_parallel=fake):
        outcomes = _resilient_call(
            "root", SEL4, [2000, 3000], restart_ports=structured,
            max_recover_rounds=1)
    by_port = {e["port"]: e for e in outcomes["restart_events"]}
    check(by_port[2000]["decision"] == "user_confirmed"
          and by_port[2000]["attempted"] and by_port[2000]["ok"],
          "structured batch success preserves attempted decision evidence")
    check(by_port[3000]["decision"] == "user_declined"
          and not by_port[3000]["attempted"] and not by_port[3000]["ok"]
          and by_port[3000]["reason"] == "operator declined restart",
          "structured explicit decline is preserved for its requested port")
    check(fake.calls[1]["ports"] == [2000]
          and outcomes.get("aborted_ports") == {3000: "lost"},
          "structured decline prevents only the declined port's refetch")

    fake = FakeFetch([{"abort": {2000: "lost", 3000: "lost"}}])

    def batch_error(_ports):
        raise RuntimeError("batch callback failed")

    with _Saved(sk, gap_fill_parallel=fake):
        failed = _resilient_call(
            "root", SEL4, [2000, 3000], restart_ports=batch_error)
    events = failed["restart_events"]
    check([e["port"] for e in events] == [2000, 3000]
          and all(e["decision"] == "callback_error"
                  and not e["attempted"] and not e["ok"] for e in events),
          "batch callback exception fails every requested port explicitly")
    check(len(fake.calls) == 1,
          "batch callback exception never reaches a recovery refetch")


def test_restart_reason_redaction_and_cap():
    fake = FakeFetch([{"abort": {3000: "lost"}}])
    progress = []
    secret_reason = ("contact trader@example.com for account DU123456 "
                     + "x" * 400)

    def restart(_port):
        return {"ok": False, "attempted": True,
                "decision": "relaunch_failed", "reason": secret_reason}

    rep = _resilient(fake, restart_port=restart, progress=progress.append)
    event = rep["restart_events"][0]
    rendered = "\n".join(str(line) for line in progress)
    check(len(event["reason"]) <= sk._RESTART_REASON_CAP,
          "persisted restart reason respects the bounded cap")
    check("trader@example.com" not in event["reason"]
          and "DU123456" not in event["reason"]
          and "[redacted-email]" in event["reason"]
          and "[redacted-account]" in event["reason"],
          "persisted restart reason redacts email and account tokens")
    check("trader@example.com" not in rendered and "DU123456" not in rendered,
          "restart progress text uses the same redacted reason")


def test_restart_event_cap():
    ports = list(range(1000, 1300))
    fake = FakeFetch([{"abort": {p: "lost" for p in ports}}])
    with _Saved(sk, gap_fill_parallel=fake):
        rep = _resilient_call(
            "root", SEL4, ports,
            restart_ports=lambda requested: {p: False for p in requested},
            max_recover_rounds=1)
    events = rep["restart_events"]
    check(len(events) == sk._RESTART_EVENT_CAP,
          "restart event list is capped without changing recovery execution")
    check(rep["restart_events_truncated"] == len(ports) - len(events),
          "restart event truncation count records every omitted event")
    check(events[0]["port"] == ports[0]
          and events[-1]["port"] == ports[sk._RESTART_EVENT_CAP - 1],
          "capped restart events retain deterministic port order")


def test_recover_flaky_two_rounds():
    # aborts, restart ok, refetch STILL aborts, restart ok again, then clean
    fake = FakeFetch([{"abort": {3000: "lost"}},
                      {"abort": {3000: "again"}},
                      {}])
    r = Restarter()
    rep = _resilient(fake, restart_port=r, port_up=lambda p: True,
                     max_recover_rounds=2)
    check(r.calls == [3000, 3000], "restarts the flaky port each round")
    check(len(fake.calls) == 3, "two refetches over two rounds")
    check("aborted_ports" not in rep, "flaky port recovered by round 2")
    check(rep["recovery_rounds"] == 2, "two recovery rounds")


def test_recover_round_cap():
    # persistent abort; restart always 'works' but refetch never clears
    fake = FakeFetch([{"abort": {3000: "lost"}}])     # every call aborts 3000
    r = Restarter()
    rep = _resilient(fake, restart_port=r, port_up=lambda p: True,
                     max_recover_rounds=2)
    check(rep["recovery_rounds"] == 2, "stops at max_recover_rounds")
    check(r.calls == [3000, 3000], "restart not retried beyond the cap")
    check(rep.get("aborted_ports") == {3000: "lost"},
          "uncurable port stays aborted after the cap")


def test_no_callbacks_is_plain():
    fake = FakeFetch([{"abort": {3000: "lost"}}])
    rep = _resilient(fake)               # no restart_port/port_up
    check(len(fake.calls) == 1, "without callbacks it's a plain parallel run")
    check(rep.get("aborted_ports") == {3000: "lost"}, "abort surfaces as-is")
    check(rep["recovery_rounds"] == 0, "no recovery without a restart callback")


# --- tws_launch per-instance mapping + orchestration -----------------------

class _FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(float(seconds))
        self.now += float(seconds)


def test_bringup_wait_stable_and_abort():
    clock = _FakeClock()
    values = iter([None, "DU100", "DU200", "DU200"])
    result = tb.wait_stable(
        lambda: next(values), 10, clock=clock.monotonic, sleep=clock.sleep)
    check(result.ok and result.value == "DU200" and result.probes == 4,
          "bring-up readiness requires the same truthy value twice")
    check(clock.sleeps == [0.5, 0.75, 1.125],
          "bring-up readiness applies deterministic bounded backoff")

    clock = _FakeClock()
    result = tb.wait_stable(
        lambda: False, 2, clock=clock.monotonic, sleep=clock.sleep)
    check(not result.ok and clock.now == 2.0
          and max(clock.sleeps) <= 2.0,
          "bring-up readiness expires exactly at its absolute deadline")
    try:
        tb.wait_stable(lambda: True, 2, abort_check=lambda: True,
                       clock=clock.monotonic, sleep=clock.sleep)
        aborted = None
    except tb.StepFailure as exc:
        aborted = exc
    check(aborted is not None and aborted.code == "aborted",
          "bring-up readiness checks abort before probing or sleeping")


def test_bringup_failure_taxonomy():
    transient = tl.TwsLaunchError(
        "slow account for owner@example.com DU123456",
        code="account_window_absent", phase="account_window")
    permanent = tl.TwsLaunchError(
        "busy", code="config_dir_busy", phase="cleanup")
    check(tb.failure_code(transient) == "account_window_absent"
          and tb.failure_phase(transient) == "account_window"
          and tb.is_retryable(transient),
          "bring-up taxonomy preserves a controlled transient code")
    check(not tb.is_retryable(permanent)
          and tb.failure_code(RuntimeError("api_node_not_found"))
          == "unexpected_error",
          "bring-up taxonomy never parses arbitrary exception text")


def test_bringup_transcript_bounds_and_redaction():
    root = Path(tempfile.mkdtemp(prefix="tws_transcript_"))
    fixed = lambda: datetime(2026, 7, 16, 12, 34, 56,
                             tzinfo=timezone.utc)
    try:
        rec = tb.FleetTranscript(
            "restart_dead", root=root, run_token="abc12345", clock=fixed)
        for i in range(tb.MAX_EVENTS + 3):
            rec.add_event(
                port=2000, attempt=1, phase="owner@example.com",
                code="DU123456", elapsed_ms=i, retryable=True,
                outcome="failed")
        shots = [rec.screenshot_path(
            port=2000, attempt=1, code="owner@example.com")
                 for _ in range(tb.MAX_SCREENSHOTS + 1)]
        path = rec.write({"first_pass": 1, "failed": 1})
        payload = json.loads(path.read_text(encoding="utf-8"))
        text = json.dumps(payload)
        check(len(payload["events"]) == tb.MAX_EVENTS
              and payload["events_truncated"] == 3,
              "bring-up transcript caps events and counts omissions")
        check(shots[-1] is None and payload["screenshots_reserved"]
              == tb.MAX_SCREENSHOTS,
              "bring-up transcript caps diagnostic screenshot reservations")
        check("owner@example.com" not in text and "DU123456" not in text
              and payload["events"][0]["phase"] == "unknown"
              and payload["events"][0]["code"] == "unexpected_error",
              "bring-up transcript persists controlled fields, not identity text")
        check(path.parent.parent == root and not path.with_suffix(".json.tmp").exists(),
              "bring-up transcript stays under its root and publishes atomically")

        attempt3 = tb.FleetTranscript(
            "launch_many", root=root, run_token="ghi12345", clock=fixed)
        attempt3.add_event(
            port=3000, attempt=3, phase="launch", code="ok",
            outcome="succeeded")
        attempt3_shot = attempt3.screenshot_path(
            port=3000, attempt=3, code="config_open_failed")
        attempt3_payload = attempt3.payload()
        check(attempt3_payload["schema_version"] == 3
              and attempt3_payload["events"][0]["attempt"] == 3
              and "-a3-" in attempt3_shot.name,
              "bring-up transcript v3 preserves the final bounded attempt")

        bad_root = root / "not-a-directory"
        bad_root.write_text("occupied", encoding="utf-8")
        progress = []
        failed = tb.FleetTranscript(
            "restart_dead", root=bad_root, run_token="def67890", clock=fixed)
        check(failed.write(on_progress=progress.append) is None
              and progress == [
                  "fleet diagnostics unavailable (transcript write failed)"],
              "transcript write failure is diagnostic-only and bounded")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_account_probe_requires_exact_config_identity():
    cfg = tl.config_dir_for("a@gmail.com")
    other = tl.config_dir_for("b@gmail.com")
    windows = [(1, "DU111 Interactive Brokers", 101, 1200, 800)]
    with _Saved(tl, _all_tws_windows=lambda: windows,
                _proc_cmdlines=lambda: {
                    101: f"tws.exe -J-DjtsConfigDir={other}"}):
        check(tl._account_for_config_now(cfg) is None,
              "an unrelated DU window cannot satisfy config readiness")
    with _Saved(tl, _all_tws_windows=lambda: windows,
                _proc_cmdlines=lambda: {
                    101: f"tws.exe -J-DjtsConfigDir={cfg}"}):
        check(tl._account_for_config_now(cfg) == "DU111",
              "exact config-dir/PID identity satisfies account readiness")


def test_launch_many_post_pass_retry_and_shapes():
    calls = []
    counts = {}

    def launch(email, _cfg, _pin, **_kw):
        calls.append(email)
        counts[email] = counts.get(email, 0) + 1
        if email == "b@gmail.com" and counts[email] == 1:
            raise tl.TwsLaunchError(
                "slow", code="account_window_absent",
                phase="account_window")
        if email == "d@gmail.com":
            raise tl.TwsLaunchError(
                "missing", code="dependency_unavailable", phase="preflight")
        return True

    class _Clock:
        @staticmethod
        def monotonic():
            return 0.0

        @staticmethod
        def sleep(_seconds):
            return None

    recovered = tb.StableResult(False, None, 1, 2)
    with _Saved(
            tl, TWS_EXE=sys.executable, launch_one=launch, time=_Clock,
            _close_all_tws=lambda _log: None,
            _wait_account_for_config_stable=lambda *a, **k: recovered,
            close_one=lambda *a, **k: True,
            _ensure_config_free=lambda *a, **k: True,
            _restore_all_tws=lambda: None,
            dismiss_popups=lambda *a, **k: 0,
            _dbg=lambda *a, **k: None,
            _tws_pids=lambda: set()):
        result = tl.launch_many(
            ["a@gmail.com", "b@gmail.com", "c@gmail.com", "d@gmail.com"],
            close_existing=False, ports=[2000, 3000, 4000, 5000])
    check(calls == ["a@gmail.com", "b@gmail.com", "c@gmail.com",
                    "d@gmail.com", "b@gmail.com"],
          "launch_many retries transient failures only after the serial pass")
    check(result["a@gmail.com"] == "launched"
          and result["b@gmail.com"] == "launched"
          and result["c@gmail.com"] == "launched"
          and result["d@gmail.com"].startswith("FAILED:"),
          "launch_many preserves launched and FAILED return shapes")


def test_launch_many_third_attempt_and_final_cleanup():
    calls = []
    close_calls = []
    counts = {}

    def launch(email, _cfg, _pin, **_kw):
        calls.append(email)
        counts[email] = counts.get(email, 0) + 1
        if email == "b@gmail.com" and counts[email] < 3:
            raise tl.TwsLaunchError(
                "slow b", code="account_window_absent",
                phase="account_window")
        if email == "c@gmail.com":
            raise tl.TwsLaunchError(
                "slow c", code="account_window_absent",
                phase="account_window")
        if email == "d@gmail.com":
            raise tl.TwsLaunchError(
                "missing", code="dependency_unavailable", phase="preflight")
        return True

    class _Clock:
        @staticmethod
        def monotonic():
            return 0.0

        @staticmethod
        def sleep(_seconds):
            return None

    not_ready = tb.StableResult(False, None, 1, 2)
    with _Saved(
            tl, TWS_EXE=sys.executable, launch_one=launch, time=_Clock,
            _close_all_tws=lambda _log: None,
            _wait_account_for_config_stable=lambda *a, **k: not_ready,
            close_one=lambda cfg, **_k: close_calls.append(Path(cfg).name),
            _ensure_config_free=lambda *a, **k: True,
            _restore_all_tws=lambda: None,
            dismiss_popups=lambda *a, **k: 0,
            _dbg=lambda *a, **k: None,
            _tws_pids=lambda: set()):
        result = tl.launch_many(
            ["a@gmail.com", "b@gmail.com", "c@gmail.com", "d@gmail.com"],
            base_dir=_TEST_BASE, close_existing=False,
            ports=[2000, 3000, 4000, 5000])
    check(counts == {"a@gmail.com": 1, "b@gmail.com": 3,
                     "c@gmail.com": 3, "d@gmail.com": 1},
          "launch_many gives only retryable members three bounded attempts")
    check(result["a@gmail.com"] == "launched"
          and result["b@gmail.com"] == "launched"
          and result["c@gmail.com"].startswith("FAILED:")
          and result["d@gmail.com"].startswith("FAILED:"),
          "third-attempt success and exhausted/permanent failures stay distinct")
    check(close_calls == ["inst_b", "inst_c", "inst_b", "inst_c", "inst_c"],
          "final exhausted transient is cleaned after both retry cleanups")


def test_launch_one_requires_stable_exact_account():
    seen = []
    ready = tb.StableResult(True, "DU222", 750, 2)
    with _Saved(
            tl, _launch_proc=lambda *a, **k: None,
            find_login_windows=lambda: [],
            _wait_new_login=lambda *a, **k: 99,
            _drive_login=lambda *a, **k: None,
            _wait_account_for_config_stable=lambda cfg, *a, **k:
            seen.append(cfg) or ready):
        result = tl.launch_one(
            "a@gmail.com", "C:\\tmp\\inst_a", (0, 0), tag="a")
    check(result is True and seen == ["C:\\tmp\\inst_a"],
          "launch_one waits for stable readiness of its exact config dir")

    with _Saved(
            tl, _launch_proc=lambda *a, **k: None,
            find_login_windows=lambda: [],
            _wait_new_login=lambda *a, **k: 99,
            _drive_login=lambda *a, **k: None,
            _wait_account_for_config_stable=lambda *a, **k:
            tb.StableResult(False, None, 60_000, 20)):
        try:
            tl.launch_one("a@gmail.com", "C:\\tmp\\inst_a", (0, 0), tag="a")
            failure = None
        except tl.TwsLaunchError as exc:
            failure = exc
    check(failure is not None and failure.code == "account_window_absent",
          "launch_one classifies a stable-account readiness expiry")


def test_restart_dead_one_shot_retry_and_shapes():
    calls = []

    def relaunch(email, port, **_kw):
        calls.append(port)
        if port == 3000 and calls.count(port) == 1:
            raise tl.TwsLaunchError(
                "slow", code="account_window_absent",
                phase="account_window")
        if port == 4000:
            raise tl.TwsLaunchError(
                "busy", code="config_dir_busy", phase="cleanup")
        return {"email": email, "port": port, "account": "DU1", "ok": True}

    with _Saved(
            tl, port_open=lambda _p, host="127.0.0.1": False,
            relaunch_one=relaunch,
            _wait_port_stable=lambda *a, **k:
            tb.StableResult(False, None, 1, 2)):
        result = tl.restart_dead([
            ("b@gmail.com", 3000), ("c@gmail.com", 4000)])
    check(calls == [3000, 4000, 3000],
          "restart_dead retries one transient after the complete first pass")
    check(isinstance(result[3000], dict) and result[3000]["ok"]
          and result[4000].startswith("FAILED:"),
          "restart_dead preserves success-dict and FAILED return shapes")


def test_restart_dead_skips_retry_after_self_recovery():
    calls = []
    recorder = tb.FleetTranscript(
        "restart_dead", root=tempfile.mkdtemp(prefix="restart_rec_"),
        run_token="self1234",
        clock=lambda: datetime(2026, 7, 16, tzinfo=timezone.utc))

    def relaunch(_email, port, **_kw):
        calls.append(port)
        raise tl.TwsLaunchError(
            "slow", code="port_not_listening", phase="port_readiness")

    try:
        with _Saved(
                tl, port_open=lambda _p, host="127.0.0.1": False,
                relaunch_one=relaunch,
                _wait_port_stable=lambda *a, **k:
                tb.StableResult(True, True, 500, 2),
                load_fleet=lambda *_a, **_k: {3000: {"account": "DU1"}}):
            result = tl.restart_dead(
                [("b@gmail.com", 3000)], recorder=recorder)
        check(calls == [3000] and result[3000]["ok"],
              "restart_dead skips a second launch when the port self-recovers")
        check([event["code"] for event in recorder.events]
              == ["port_not_listening", "self_recovered"],
              "restart_dead records failed attempt and self-recovery distinctly")
    finally:
        shutil.rmtree(recorder.root, ignore_errors=True)


def test_retry_recovery_abort_preserves_partial_results():
    aborted = tb.StepFailure("stop", code="aborted", phase="abort")

    class _Clock:
        @staticmethod
        def monotonic():
            return 0.0

        @staticmethod
        def sleep(_seconds):
            return None

    def launch(*_args, **_kwargs):
        raise tl.TwsLaunchError(
            "slow", code="account_window_absent", phase="account_window")

    with _Saved(
            tl, TWS_EXE=sys.executable, launch_one=launch, time=_Clock,
            _close_all_tws=lambda _log: None,
            _wait_account_for_config_stable=lambda *a, **k: (_ for _ in ()).throw(aborted),
            _restore_all_tws=lambda: None, dismiss_popups=lambda *a, **k: 0,
            _dbg=lambda *a, **k: None, _tws_pids=lambda: set()):
        launched = tl.launch_many(
            ["a@gmail.com", "b@gmail.com"], close_existing=False)
    check(all(value.startswith("FAILED: aborted")
              for value in launched.values()),
          "launch_many recovery abort returns bounded partial results")

    calls = []

    def relaunch(_email, port, **_kwargs):
        calls.append(port)
        raise tl.TwsLaunchError(
            "slow", code="port_not_listening", phase="port_readiness")

    with _Saved(
            tl, port_open=lambda *_a, **_k: False, relaunch_one=relaunch,
            _wait_port_stable=lambda *a, **k: (_ for _ in ()).throw(aborted)):
        restarted = tl.restart_dead([
            ("a@gmail.com", 2000), ("b@gmail.com", 3000)])
    check(calls == [2000, 3000]
          and all(str(value).startswith("FAILED: aborted")
                  for value in restarted.values()),
          "restart_dead recovery abort preserves the first-pass result map")

    class FakeApi:
        def configure(self, *_args, **_kwargs):
            raise tb.StepFailure(
                "slow", code="api_node_not_found", phase="api_navigation")

    sys.modules["tws_api"] = FakeApi()
    try:
        with _Saved(
                tl, **_enable_patches(
                    _wait_port_stable=lambda *a, **k:
                    (_ for _ in ()).throw(aborted))):
            enabled = tl.enable_fleet([
                ("a@gmail.com", 2000), ("b@gmail.com", 3000)])
    finally:
        del sys.modules["tws_api"]
    check(enabled == {2000: "aborted", 3000: "aborted"},
          "enable_fleet recovery abort marks remaining retry candidates")


def test_handshake_pins_unattended_policy_and_single_attempt():
    seen = []

    def launcher(_email, port, **kwargs):
        seen.append((port, kwargs.get("readiness_policy")))
        return {"ok": True}

    result = th.restart_ports_with_confirmation(
        [2000], {2000: "a@gmail.com"}, _rendered_decision(),
        lambda _message: None, _HandshakeGuardMod(), launcher,
        readiness_policy=tb.ReadinessPolicy(
            account_timeout=150.0, port_timeout=60.0))
    policy = seen[0][1]
    check(result[2000]["ok"] and len(seen) == 1,
          "unattended handshake invokes relaunch_one exactly once per port")
    check(policy.account_timeout == 150.0 and policy.port_timeout == 60.0,
          "unattended handshake pins its legacy 150/60 readiness budget")

def test_pid_for_config():
    cfg_a = tl.config_dir_for("a@gmail.com")
    cfg_b = tl.config_dir_for("b@gmail.com")
    fake_cmds = {
        111: f'C:\\Jts\\tws.exe "-J-DjtsConfigDir={cfg_a}"',
        222: f'C:\\Jts\\tws.exe "-J-DjtsConfigDir={cfg_b}"',
    }
    with _Saved(tl, _proc_cmdlines=lambda: fake_cmds):
        check(tl._pid_for_config(cfg_a) == 111, "pid_for_config maps a -> 111")
        check(tl._pid_for_config(cfg_b) == 222, "pid_for_config maps b -> 222")
        check(tl._pid_for_config(tl.config_dir_for("z@gmail.com")) is None,
              "pid_for_config -> None for an absent instance")
    # prefix collision: inst_a must NOT match inst_ab's command line
    cfg_ab = tl.config_dir_for("ab@gmail.com")
    with _Saved(tl, _proc_cmdlines=lambda: {
            333: f'C:\\Jts\\tws.exe "-J-DjtsConfigDir={cfg_ab}"'}):
        check(tl._pid_for_config(cfg_a) is None,
              "pid_for_config 'inst_a' does not match 'inst_ab' (boundary)")
        check(tl._pid_for_config(cfg_ab) == 333, "pid_for_config maps ab -> 333")


def _relaunch_patches(closed, **over):
    """Common stubs so relaunch_one runs end-to-end without a desktop."""
    base = dict(
        TWS_EXE=sys.executable,                    # 'exists' check passes
        _launch_proc=lambda cfg, **k: None,
        _wait_new_login=lambda before, t, abort_check=None: 9999,
        _drive_login=lambda *a, **k: None,
        _wait_account_for_config_stable=lambda cfg, t, abort_check=None,
            policy=None: tb.StableResult(True, "DU999", 1, 2),
        _ensure_config_free=lambda cfg, on_progress=None, abort_check=None: True,
        dismiss_popups=lambda *a, **k: 0,
        _restore_all_tws=lambda: None,
        find_login_windows=lambda: [],
        close_one=lambda cfg, on_progress=None, abort_check=None:
            closed.append(cfg) or True,
        _wait_port_stable=lambda port, t, host="127.0.0.1", abort_check=None,
            policy=None: tb.StableResult(True, True, 1, 2),
    )
    base.update(over)
    return base


def test_relaunch_one_happy():
    closed = []
    with _Saved(tl, **_relaunch_patches(closed)):
        res = tl.relaunch_one("c@gmail.com", 4000, do_enable=False, base_dir=_TEST_BASE)
    check(res["ok"] is True, "relaunch_one returns ok")
    check(res["port"] == 4000 and res["account"] == "DU999",
          "relaunch_one reports port + account")
    check(closed == [tl.config_dir_for("c@gmail.com", _TEST_BASE)],
          "relaunch_one closes ONLY the target instance's config dir")


def test_relaunch_one_enables_port():
    closed, enabled = [], []

    class FakeApi:
        def configure(self, acct, port, log=None, tick_enable=True, **_kw):
            enabled.append((acct, port))
            return True

    sys.modules["tws_api"] = FakeApi()
    try:
        with _Saved(tl, **_relaunch_patches(closed)):
            tl.relaunch_one("d@gmail.com", 5000, do_enable=True,
                            base_dir=_TEST_BASE)
    finally:
        del sys.modules["tws_api"]
    check(enabled == [("DU999", 5000)],
          "relaunch_one re-enables the API on the new account + intended port")


def test_relaunch_one_failures():
    closed = []
    with _Saved(tl, **_relaunch_patches(
            closed, _wait_new_login=lambda b, t, abort_check=None: None)):
        try:
            tl.relaunch_one("c@gmail.com", 4000, do_enable=False, base_dir=_TEST_BASE)
            ok = False
        except tl.TwsLaunchError:
            ok = True
    check(ok, "no login window -> TwsLaunchError")

    closed = []
    with _Saved(tl, **_relaunch_patches(
            closed,
            _wait_port_stable=lambda port, t, host="127.0.0.1",
            abort_check=None, policy=None:
            tb.StableResult(False, None, 1, 2))):
        try:
            tl.relaunch_one("c@gmail.com", 4000, do_enable=False, base_dir=_TEST_BASE)
            ok = False
        except tl.TwsLaunchError:
            ok = True
    check(ok, "port never comes up -> TwsLaunchError")


def test_restart_dead():
    up = {2000: True, 3000: False, 4000: False}
    calls = []

    def fake_relaunch(email, port, on_progress=None, **kw):
        calls.append((email, port))
        if port == 4000:
            raise tl.TwsLaunchError("boom")
        return {"email": email, "port": port, "account": "DU1", "ok": True}

    fleet = [("a@gmail.com", 2000), ("b@gmail.com", 3000), ("c@gmail.com", 4000)]
    with _Saved(tl, port_open=lambda p, host="127.0.0.1": up[p],
                relaunch_one=fake_relaunch):
        res = tl.restart_dead(fleet)
    check(res[2000] == "up", "restart_dead leaves an up port alone")
    check([c[1] for c in calls] == [3000, 4000],
          "restart_dead restarts only the down ports")
    check(isinstance(res[3000], dict) and res[3000]["ok"],
          "restart_dead reports the recovered port")
    check(str(res[4000]).startswith("FAILED"),
          "restart_dead isolates a failed restart")


def test_fleet_health():
    up = {2000: True, 3000: False}
    with _Saved(tl, port_open=lambda p, host="127.0.0.1": up[int(p)]):
        h = tl.fleet_health([2000, 3000])
    check(h == {2000: True, 3000: False}, "fleet_health probes each port")


def test_fleet_manifest():
    base = tempfile.mkdtemp(prefix="fleet_")
    try:
        check(tl.load_fleet(base) == {}, "load_fleet empty when absent")
        tl.record_member(2000, email="a@gmail.com",
                         config_dir=tl.config_dir_for("a@gmail.com", base),
                         account="DU010", base_dir=base)
        tl.record_member(3000, email="b@gmail.com", base_dir=base)
        f = tl.load_fleet(base)
        check(set(f) == {2000, 3000}, "fleet keys round-trip as ints")
        check(f[2000]["account"] == "DU010", "member account recorded")
        # upsert: refresh account WITHOUT clobbering the email
        tl.record_member(2000, account="DU999", base_dir=base)
        f2 = tl.load_fleet(base)
        check(f2[2000]["email"] == "a@gmail.com"
              and f2[2000]["account"] == "DU999",
              "record_member upserts (keeps email, updates account)")
        check(tl.email_for_port(2000, base) == "a@gmail.com",
              "email_for_port returns the recorded email")
        check(tl.email_for_port(9999, base, fallback_index=2) == "c@gmail.com",
              "email_for_port falls back to the lettered index")
        check(tl.email_for_port(9999, base) is None,
              "email_for_port None when unrecorded and no fallback")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_email_from_config_dir():
    check(tl.email_from_config_dir(tl.config_dir_for("d@gmail.com"))
          == "d@gmail.com", "email_from_config_dir reverses config_dir_for")


def test_config_dir_for_account():
    cfg = tl.config_dir_for("e@gmail.com")
    fake_win = [(5, "DU777 Interactive Brokers", 888, 1200, 800)]
    fake_cmd = {888: f"C:\\Jts\\tws.exe -J-DjtsConfigDir={cfg}"}
    with _Saved(tl, _all_tws_windows=lambda: fake_win,
                _proc_cmdlines=lambda: fake_cmd):
        check(tl.account_pid("DU777") == 888, "account_pid finds the window PID")
        check(tl.config_dir_for_account("DU777") == cfg,
              "config_dir_for_account resolves via PID->cmdline")
        check(tl.config_dir_for_account("DU000") is None,
              "config_dir_for_account None for an unknown account")


def test_recover_single_port_not_silently_dropped():
    # H1 regression: the refetch runs on ONE port -> serial shape; if it
    # re-fails it must STAY aborted, never be silently dropped as 'recovered'.
    fake = FakeFetch([{"abort": {3000: "lost"}}, {"abort": {3000: "still down"}}])
    r = Restarter()
    rep = _resilient(fake, restart_port=r, port_up=lambda p: True,
                     max_recover_rounds=1)
    check(len(fake.calls) == 2, "single-port refetch actually runs")
    check(fake.calls[1]["ports"] == [3000], "refetch is the 1-port serial case")
    check(rep.get("aborted_ports") == {3000: "still down"},
          "a re-failing single-port refetch stays aborted (NOT silently dropped)")
    check(rep["per_port"].get(3000, {}).get("aborted"),
          "per_port reflects the still-aborted port")
    check({s["ticker"] for s in rep["series"]} == {"AAPL", "GOOGL"},
          "only the healthy port's tickers are present")


def test_recover_respects_cancel_event():
    # M3: a cancel event set before the run -> no pre-flight, no recovery.
    fake = FakeFetch([{"abort": {3000: "lost"}}])
    r = Restarter()
    rep = _resilient(fake, restart_port=r, port_up=lambda p: False,
                     cancel=FakeCancel(True))
    check(r.calls == [], "cancelled run never restarts a port")
    check(rep["recovery_rounds"] == 0, "cancelled run does no recovery rounds")
    check("restart_events" not in rep,
          "cancel before recovery creates no misleading restart event")


def test_recover_respects_cancelled_flag():
    # M3: report carries cancelled=True (cancel co-occurred with an abort) ->
    # don't drive a relaunch afterwards.
    fake = FakeFetch([{"abort": {3000: "lost"}, "cancelled": True}])
    r = Restarter()
    rep = _resilient(fake, restart_port=r, port_up=lambda p: True)
    check(r.calls == [], "a cancelled report skips recovery")
    check(rep.get("aborted_ports") == {3000: "lost"}, "abort left for next run")
    check(rep["recovery_rounds"] == 0, "no recovery rounds when cancelled")


def test_callbacks_raising():
    # raising restart_port / port_up must never crash the orchestrator.
    fake = FakeFetch([{"abort": {3000: "lost"}}])

    def boom_restart(p):
        raise RuntimeError("restart blew up")

    def boom_up(p):
        raise RuntimeError("probe blew up")

    rep = _resilient(fake, restart_port=boom_restart, port_up=boom_up)
    check(rep.get("aborted_ports") == {3000: "lost"},
          "raising restart/probe callbacks are caught; port left aborted")
    check(rep["restart_events"]
          and all(e["decision"] == "callback_error"
                  and e["final_probe"] == "error" and not e["ok"]
                  for e in rep["restart_events"]),
          "raising callbacks persist explicit callback/probe failures")


def test_parse_cmdlines():
    p = tl._parse_cmdlines
    check(p("") == {} and p("   ") == {}, "_parse_cmdlines: empty -> {}")
    check(p('{"ProcessId": 5, "CommandLine": "x"}') == {5: "x"},
          "_parse_cmdlines: single object -> one entry")
    check(p('[{"ProcessId":5,"CommandLine":"a"},'
            '{"ProcessId":6,"CommandLine":null}]') == {5: "a", 6: ""},
          "_parse_cmdlines: array + null CommandLine -> ''")
    check(p("not json at all") == {}, "_parse_cmdlines: bad JSON -> {}")
    check(p('[{"ProcessId":"abc","CommandLine":"x"}]') == {},
          "_parse_cmdlines: non-numeric ProcessId skipped")
    check(p('[1, {"ProcessId":7,"CommandLine":"y"}]') == {7: "y"},
          "_parse_cmdlines: non-dict element skipped")


def test_gui_lock_serializes():
    import threading
    import time as _t
    active = {"n": 0, "max": 0}
    lk = threading.Lock()

    def drive(*a, **k):
        with lk:
            active["n"] += 1
            active["max"] = max(active["max"], active["n"])
        _t.sleep(0.05)
        with lk:
            active["n"] -= 1

    closed = []
    with _Saved(tl, **_relaunch_patches(closed, _drive_login=drive)):
        threads = [threading.Thread(
            target=lambda c=c: tl.relaunch_one(f"{c}@gmail.com", 4000,
                                               do_enable=False,
                                               base_dir=_TEST_BASE))
            for c in "xy"]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
    check(active["max"] == 1,
          "_GUI_LOCK serializes concurrent relaunch_one (no screen fight)")


def test_relaunch_one_abort_check():
    # ESC/abort: relaunch_one bails (TwsLaunchError) when abort_check is set,
    # before any destructive work.
    closed = []
    with _Saved(tl, **_relaunch_patches(closed)):
        try:
            tl.relaunch_one("c@gmail.com", 4000, do_enable=False,
                            base_dir=_TEST_BASE, abort_check=lambda: True)
            raised = False
        except tl.TwsLaunchError:
            raised = True
    check(raised, "relaunch_one honors abort_check (raises, no relaunch)")
    check(closed == [], "abort before close: nothing was killed")


def test_sleep_or_abort():
    # the input-held phases use _sleep_or_abort so a mid-restart ESC (while
    # BlockInput is held by the worker) is honored within ~step, not after the
    # full sleep — only the owning worker thread can release the freeze.
    import time as _t
    t0 = _t.time()
    tl._sleep_or_abort(0.1, lambda: False)
    check(_t.time() - t0 >= 0.09,
          "_sleep_or_abort sleeps the full time when not aborted")
    t0 = _t.time()
    raised = False
    try:
        tl._sleep_or_abort(5, lambda: True)
    except tl.TwsLaunchError:
        raised = True
    check(raised and (_t.time() - t0) < 1.0,
          "_sleep_or_abort raises PROMPTLY when aborted (not after 5s)")


def test_drive_login_honors_abort():
    # _drive_login (the longest input-held phase) polls abort_check and bails
    # before any GUI drive, so ESC releases input quickly. (Bug-hunt finding #4.)
    raised = False
    try:
        tl._drive_login(0, "a@b.com", (0, 0), lambda m: None, False, "t",
                        abort_check=lambda: True)
    except tl.TwsLaunchError:
        raised = True
    check(raised, "_drive_login bails on abort before driving the GUI")


class _FakeLoginGui:
    def __init__(self, timeline=None):
        self.events = []
        self.timeline = timeline if timeline is not None else []

    def _record(self, event):
        self.events.append(event)
        self.timeline.append(event)

    def hotkey(self, *keys):
        self._record(("hotkey", keys))

    def press(self, key):
        self._record(("press", key))

    def typewrite(self, value, **kwargs):
        self._record(("typewrite", value, kwargs))


class _FakeClipboard:
    def __init__(self, *, available=True, restore_raises=False):
        self.available = available
        self.restore_raises = restore_raises
        self.events = []

    def replace_text(self, value):
        self.events.append(("replace", value))
        if not self.available:
            raise OSError("clipboard unavailable")
        return "prior clipboard text"

    def restore(self, previous):
        self.events.append(("restore", previous))
        if self.restore_raises:
            raise OSError("restore unavailable")


def _exercise_drive_login(locations, *, clipboard=None):
    timeline = []
    gui = _FakeLoginGui(timeline)
    clipboard = clipboard or _FakeClipboard()
    clicks = []
    focus_checks = []
    logs = []
    points = iter(locations)

    def click(x, y):
        clicks.append((x, y))
        timeline.append(("click", (x, y)))

    def require_focus(hwnd, **_kwargs):
        focus_checks.append(hwnd)
        timeline.append(("focus", hwnd))
        return hwnd

    old_gui = sys.modules.get("pyautogui")
    sys.modules["pyautogui"] = gui
    try:
        with _Saved(
                tl,
                _minimize_others=lambda _hwnd: None,
                pin_window=lambda *_args, **_kwargs: (0, 0, 800, 600),
                _dbg=lambda *_args, **_kwargs: None,
                _click=click,
                _locate=lambda *_args, **_kwargs: next(points),
                _require_login_foreground=require_focus,
                _sleep_or_abort=lambda *_args, **_kwargs: None):
            try:
                tl._drive_login(
                    123, "a@gmail.com", (0, 0), logs.append,
                    False, "a", clipboard_factory=lambda: clipboard)
                failure = None
            except tl.TwsLaunchError as exc:
                failure = exc
    finally:
        if old_gui is None:
            sys.modules.pop("pyautogui", None)
        else:
            sys.modules["pyautogui"] = old_gui
    return gui, clipboard, clicks, focus_checks, logs, failure


def test_drive_login_atomic_paste_and_best_effort_restore():
    point = (200, 200)
    clipboard = _FakeClipboard(restore_raises=True)
    gui, clipboard, clicks, focus_checks, logs, failure = (
        _exercise_drive_login([point, point, point], clipboard=clipboard))
    check(failure is None and clicks == [
        (200, 91), (200, 91), (210, 330), point],
          "login paste re-clicks the field immediately before one Ctrl+V")
    check(gui.events.count(("hotkey", ("ctrl", "a"))) == 2
          and gui.events.count(("press", "backspace")) == 2
          and gui.events.count(("hotkey", ("ctrl", "v"))) == 1
          and not any(event[0] == "typewrite" for event in gui.events),
          "login paste double-clears then uses one atomic paste input")
    paste_at = gui.timeline.index(("hotkey", ("ctrl", "v")))
    check(gui.timeline[paste_at - 3:paste_at] == [
        ("focus", 123), ("click", (200, 91)), ("focus", 123)],
          "email field is re-clicked immediately before the guarded Ctrl+V")
    check(clipboard.events == [
        ("replace", "a@gmail.com"),
        ("restore", "prior clipboard text")],
          "clipboard restoration is called and a restore error is non-fatal")
    check(len(focus_checks) >= 9
          and all("a@gmail.com" not in message
                  and "prior clipboard text" not in message
                  for message in logs),
          "every paste input is foreground-gated and clipboard content is not logged")


def test_drive_login_second_round_recovers_in_window():
    point = (200, 200)
    gui, clipboard, clicks, _focus, _logs, failure = _exercise_drive_login(
        [point, point, None, point])
    check(failure is None
          and gui.events.count(("hotkey", ("ctrl", "v"))) == 2
          and gui.events.count(("hotkey", ("ctrl", "a"))) == 4,
          "login retries clear and paste in the same window after a disabled gate")
    check(clicks == [
        (200, 91), (200, 91), (210, 330),
        (200, 91), (200, 91), (210, 330), point],
          "second-round recovery submits only the fresh enabled match")
    check(sum(event[0] == "restore" for event in clipboard.events) == 2,
          "each in-window paste round restores prior clipboard custody")


def test_drive_login_persistent_disabled_never_submits():
    point = (200, 200)
    gui, clipboard, clicks, _focus, _logs, failure = _exercise_drive_login(
        [point, point, None, None, None])
    check(failure is not None
          and tb.failure_code(failure) == "login_drive_failed",
          "three disabled paste rounds end in the typed retryable failure")
    check(gui.events.count(("hotkey", ("ctrl", "v"))) == 3
          and gui.events.count(("hotkey", ("ctrl", "a"))) == 6
          and point not in clicks,
          "persistent disabled state gets three bounded rounds and zero submit clicks")
    check(sum(event[0] == "restore" for event in clipboard.events) == 3,
          "persistent-disabled failure still restores every staged clipboard")


def test_drive_login_clipboard_unavailable_has_one_slow_fallback():
    point = (200, 200)
    unavailable = _FakeClipboard(available=False)
    gui, clipboard, clicks, _focus, _logs, failure = _exercise_drive_login(
        [point, point, None], clipboard=unavailable)
    check(failure is not None
          and tb.failure_code(failure) == "login_drive_failed"
          and gui.events.count(("typewrite", "a@gmail.com", {
              "interval": tl.EMAIL_TYPE_INTERVAL})) == 1,
          "clipboard denial gets exactly one slow typed fallback round")
    check(not any(event == ("hotkey", ("ctrl", "v"))
                  for event in gui.events)
          and clipboard.events == [("replace", "a@gmail.com")]
          and clicks == [(200, 91), (200, 91), (210, 330)],
          "fallback remains enabled-gated with no paste or stale submit")

    succeeds = _FakeClipboard(available=False)
    gui, _clipboard, clicks, _focus, _logs, failure = _exercise_drive_login(
        [point, point, point], clipboard=succeeds)
    check(failure is None
          and gui.events.count(("typewrite", "a@gmail.com", {
              "interval": tl.EMAIL_TYPE_INTERVAL})) == 1
          and clicks[-1] == point,
          "one slow fallback can submit after the enabled gate appears")


def test_login_foreground_revalidation_and_redacted_failure():
    samples = iter([(999, "Other"), (123, "Login")])
    raised = []
    with _Saved(
            tl,
            foreground_window=lambda: next(samples),
            foreground=lambda hwnd: raised.append(hwnd)):
        result = tl._require_login_foreground(
            123, timeout=0.2, interval=0.01,
            clock=lambda: 0.0, sleep=lambda _seconds: None)
    check(result == 123 and raised == [123],
          "login focus proof recovers the exact target before input")

    ticks = iter([0.0, 0.0, 1.0])
    with _Saved(
            tl,
            foreground_window=lambda: (999, "owner@example.com"),
            foreground=lambda _hwnd: None):
        try:
            tl._require_login_foreground(
                123, timeout=0.2, interval=0.01,
                clock=lambda: next(ticks), sleep=lambda _seconds: None)
            failure = None
        except tl.TwsLaunchError as exc:
            failure = exc
    check(failure is not None and tb.failure_code(failure) == "focus_lost"
          and "owner@example.com" not in str(failure)
          and "EMAIL_REDACTED" in str(failure),
          "login focus exhaustion is typed and redacts the foreground holder")


def test_ensure_config_free_blocks_relaunch():
    # M1: if the old instance can't be confirmed gone, relaunch refuses rather
    # than wiping/duplicating a live config dir.
    closed = []
    with _Saved(tl, **_relaunch_patches(
            closed,
            _ensure_config_free=lambda cfg, on_progress=None, abort_check=None:
            False)):
        try:
            tl.relaunch_one("c@gmail.com", 4000, do_enable=False, base_dir=_TEST_BASE)
            ok = False
        except tl.TwsLaunchError:
            ok = True
    check(ok, "relaunch refuses when the config dir isn't confirmed free")


def test_resilient_batch_callback():
    # the batch restart_ports path: ONE call gets ALL aborted ports (so the GUI
    # shows one popup / one input-block for "all logged out").
    fake = FakeFetch([{"abort": {2000: "lost", 3000: "lost"}}, {}])
    calls = []

    def restart_batch(ports):
        calls.append(sorted(int(p) for p in ports))
        return {p: True for p in ports}

    with _Saved(sk, gap_fill_parallel=fake):
        rep = _resilient_call("root", SEL4, [2000, 3000],
                              restart_ports=restart_batch,
                              port_up=lambda p: True)
    check(calls == [[2000, 3000]],
          "restart_ports gets ALL aborted ports in ONE batch call")
    check("aborted_ports" not in rep, "batch recovery cleared the aborts")


def test_resilient_batch_preflight():
    fake = FakeFetch([{}])
    calls = []

    def restart_batch(ports):
        calls.append(sorted(int(p) for p in ports))
        return {p: True for p in ports}

    with _Saved(sk, gap_fill_parallel=fake):
        _resilient_call("root", SEL4, [2000, 3000],
                        restart_ports=restart_batch,
                        port_up=lambda p: p == 2000)
    check(calls == [[3000]],
          "pre-flight batches the down ports into one restart_ports call")


def test_relaunch_one_configure_error_cleans_up():
    # the enable step (tws_api.configure) raises a bare RuntimeError on its
    # normal GUI-match failures; relaunch_one must clean up AND normalize it to
    # TwsLaunchError so restart_dead's per-instance isolation still catches it.
    closed = []

    class BoomApi:
        def configure(self, acct, port, log=None, tick_enable=True, **_kw):
            raise RuntimeError("could not reach API settings")

    sys.modules["tws_api"] = BoomApi()
    try:
        with _Saved(tl, **_relaunch_patches(closed)):
            try:
                tl.relaunch_one("d@gmail.com", 5000, do_enable=True,
                                base_dir=_TEST_BASE)
                raised = None
            except tl.TwsLaunchError as e:
                raised = e
            except Exception as e:  # noqa: BLE001 — must NOT reach here
                raised = e
    finally:
        del sys.modules["tws_api"]
    check(isinstance(raised, tl.TwsLaunchError),
          "configure RuntimeError is normalized to TwsLaunchError")
    check(len(closed) == 2,
          "relaunch cleans up (close_one) after a configure failure")


def test_ensure_config_free_logic():
    cfg = tl.config_dir_for("q@gmail.com")
    with _Saved(tl, _tws_pids=lambda: set(),
                _pid_for_config=lambda c: None):
        check(tl._ensure_config_free(cfg) is True,
              "_ensure_config_free: free when no tws.exe is running")
    with _Saved(tl, _tws_pids=lambda: {1},
                _pid_for_config=lambda c: None):
        check(tl._ensure_config_free(cfg) is True,
              "_ensure_config_free: None confirmed by re-probe -> free")


def _enable_patches(**over):
    """Stubs so enable_fleet runs end-to-end without a desktop. Each instance's
    account is derived from its config dir so a test can assert inst_a got
    pairs[0]'s port, etc."""
    base = dict(
        port_open=lambda p, host="127.0.0.1": False,        # nothing bound yet
        _wait_account_for_config_stable=lambda cfg, t, abort_check=None,
            policy=None: tb.StableResult(
                True, "DU_" + Path(cfg).name, 1, 2),
        _wait_port_stable=lambda port, t, host="127.0.0.1", abort_check=None,
            policy=None: tb.StableResult(True, True, 1, 2),
        dismiss_popups=lambda *a, **k: 0,
        _restore_all_tws=lambda: None,
    )
    base.update(over)
    return base


def test_enable_fleet_happy():
    enabled = []

    class FakeApi:
        def configure(self, acct, port, log=None, tick_enable=True, **_kw):
            enabled.append((acct, port))
            return True

    pairs = [("a@gmail.com", 2000), ("b@gmail.com", 3000)]
    sys.modules["tws_api"] = FakeApi()
    try:
        with _Saved(tl, **_enable_patches()):
            res = tl.enable_fleet(pairs, base_dir=_TEST_BASE)
    finally:
        del sys.modules["tws_api"]
    check(all(isinstance(res[p], dict) and res[p]["ok"] for p in (2000, 3000)),
          "enable_fleet binds every port")
    cfg_a = tl.config_dir_for("a@gmail.com", _TEST_BASE)
    cfg_b = tl.config_dir_for("b@gmail.com", _TEST_BASE)
    check(enabled == [("DU_" + Path(cfg_a).name, 2000),
                      ("DU_" + Path(cfg_b).name, 3000)],
          "enable_fleet configures each instance on its OWN port "
          "(inst_a->pairs[0], inst_b->pairs[1], matched by config dir)")
    f = tl.load_fleet(_TEST_BASE)
    check(f.get(2000, {}).get("account") == "DU_" + Path(cfg_a).name
          and f.get(2000, {}).get("email") == "a@gmail.com",
          "enable_fleet records the fleet.json port->instance map")


def test_enable_fleet_skips_already_up():
    enabled = []

    class FakeApi:
        def configure(self, acct, port, log=None, tick_enable=True, **_kw):
            enabled.append(int(port))
            return True

    pairs = [("a@gmail.com", 2000), ("b@gmail.com", 3000)]
    sys.modules["tws_api"] = FakeApi()
    try:
        with _Saved(tl, **_enable_patches(
                port_open=lambda p, host="127.0.0.1": int(p) == 2000)):
            res = tl.enable_fleet(pairs, base_dir=_TEST_BASE)
    finally:
        del sys.modules["tws_api"]
    check(res[2000] == "up",
          "enable_fleet skips a port that is already listening")
    check(enabled == [3000],
          "enable_fleet drives the config GUI ONLY for the un-bound port")
    check(isinstance(res[3000], dict) and res[3000]["ok"],
          "the down port still gets bound")


def test_enable_fleet_isolates_failure():
    attempts = []

    class FakeApi:
        def configure(self, acct, port, log=None, tick_enable=True, **_kw):
            attempts.append(int(port))
            if int(port) == 3000:                 # bare RuntimeError, like the
                raise RuntimeError("could not reach API settings")   # real GUI
            return True

    pairs = [("a@gmail.com", 2000), ("b@gmail.com", 3000), ("c@gmail.com", 4000)]
    sys.modules["tws_api"] = FakeApi()
    try:
        with _Saved(tl, **_enable_patches()):
            res = tl.enable_fleet(pairs, base_dir=_TEST_BASE)
    finally:
        del sys.modules["tws_api"]
    check(str(res[3000]).startswith("FAILED"),
          "enable_fleet isolates a failed instance (per-port)")
    check(isinstance(res[2000], dict) and res[2000]["ok"]
          and isinstance(res[4000], dict) and res[4000]["ok"],
          "the other instances still bind despite one failure")
    check(attempts == [2000, 3000, 4000],
          "untyped enable failures are permanent and never auto-retried")


def test_enable_fleet_post_pass_transient_retry():
    attempts = []
    stable_calls = {}

    class FakeApi:
        def configure(self, acct, port, log=None, tick_enable=True, **_kw):
            port = int(port)
            attempts.append(port)
            if port == 3000 and attempts.count(port) == 1:
                raise tb.StepFailure(
                    "slow settings", code="api_node_not_found",
                    phase="api_navigation")
            return True

    def stable(port, _timeout, **_kw):
        port = int(port)
        stable_calls[port] = stable_calls.get(port, 0) + 1
        if port == 3000 and stable_calls[port] == 1:
            return tb.StableResult(False, None, 1, 2)
        return tb.StableResult(True, True, 1, 2)

    pairs = [("a@gmail.com", 2000), ("b@gmail.com", 3000),
             ("c@gmail.com", 4000)]
    sys.modules["tws_api"] = FakeApi()
    try:
        with _Saved(tl, **_enable_patches(_wait_port_stable=stable)):
            res = tl.enable_fleet(pairs, base_dir=_TEST_BASE)
    finally:
        del sys.modules["tws_api"]
    check(attempts == [2000, 3000, 4000, 3000],
          "enable_fleet retries a typed transient after the serial first pass")
    check(all(isinstance(res[p], dict) and res[p]["ok"]
              for p in (2000, 3000, 4000)),
          "enable_fleet transient retry preserves success-dict return shapes")


def test_enable_fleet_honors_abort():
    # an ESC/abort -> every port is 'aborted' and NO config GUI is driven.
    enabled = []

    class FakeApi:
        def configure(self, acct, port, log=None, tick_enable=True, **_kw):
            enabled.append(int(port))
            return True

    pairs = [("a@gmail.com", 2000), ("b@gmail.com", 3000)]
    sys.modules["tws_api"] = FakeApi()
    try:
        with _Saved(tl, **_enable_patches()):
            res = tl.enable_fleet(pairs, base_dir=_TEST_BASE,
                                  abort_check=lambda: True)
    finally:
        del sys.modules["tws_api"]
    check(res[2000] == "aborted" and res[3000] == "aborted",
          "enable_fleet marks every port aborted when abort is set")
    check(enabled == [],
          "enable_fleet drives no config GUI once aborted")


def test_close_nuisance_popups():
    closed = []

    class _U:
        def PostMessageW(self, hwnd, msg, w, l):
            closed.append((hwnd, msg))

    fake_wins = [
        (1, "DU007 Interactive Brokers", 100, 1200, 800),    # main — keep
        (2, "Complete your Application", 100, 400, 300),      # nag — close
        (3, "Global Configuration", 100, 500, 400),          # config — keep
    ]
    with _Saved(tl, _all_tws_windows=lambda: fake_wins,
                _win=lambda: (None, None, _U())):
        n = tl.close_nuisance_popups(settle=0)
    check(n == 1 and closed == [(2, 0x0010)],
          "close_nuisance_popups WM_CLOSEs ONLY the matched nag (not main/config)")
    # substring + case-insensitive, and a no-match scan closes nothing
    closed.clear()
    with _Saved(tl, _all_tws_windows=lambda: [
            (9, "please COMPLETE YOUR APPLICATION now", 1, 300, 200)],
            _win=lambda: (None, None, _U())):
        n2 = tl.close_nuisance_popups(settle=0)
    check(n2 == 1, "close_nuisance_popups matches case-insensitive substring")
    closed.clear()
    with _Saved(tl, _all_tws_windows=lambda: [
            (5, "DU1 Interactive Brokers", 1, 1200, 800)],
            _win=lambda: (None, None, _U())):
        check(tl.close_nuisance_popups(settle=0) == 0 and not closed,
              "close_nuisance_popups closes nothing when no nag is present")


def test_close_resource_warnings_is_shape_and_title_bounded():
    closed = []

    class _U:
        def PostMessageW(self, hwnd, msg, w, l):
            closed.append((hwnd, msg))

    fake_wins = [
        (1, "DU007 Interactive Brokers (Demo System)", 100, 1312, 700),
        (2, "DU007 IBKR Trader Workstation (Demo System)", 100, 642, 341),
        # Same resource frame at 125% DPI scaling.
        (3, "DU008 IBKR Trader Workstation (Demo System)", 101, 803, 426),
        # Same account-title family, but a differently shaped API terms dialog.
        (4, "DU007 IBKR Trader Workstation (Demo System)", 100, 642, 430),
        (5, "Global Configuration", 100, 1440, 810),
        (6, "Unrelated warning", 100, 642, 341),
    ]
    with _Saved(tl, _all_tws_windows=lambda: fake_wins,
                _win=lambda: (None, None, _U())):
        count = tl.close_resource_warnings(settle=0)
    check(count == 2 and closed == [(2, 0x0010), (3, 0x0010)],
          "resource-warning cleanup ignores main/config/terms/unrelated windows")
    check(tl.is_resource_warning_window(fake_wins[2])
          and not tl.is_resource_warning_window(fake_wins[3]),
          "resource-warning recognition is DPI-scaled but shape-bounded")


def test_resource_warning_sweeper_exact_tick_and_transcript():
    closed = []
    root = tempfile.mkdtemp(prefix="tws_warning_sweep_")
    recorder = tb.FleetTranscript(
        "fleet_bringup", root=root, run_token="sweep1234")

    class _U:
        def PostMessageW(self, hwnd, msg, _w, _l):
            closed.append((hwnd, msg))
            return None

    rows = [
        (21, "DU123 IBKR Trader Workstation (Demo System)", 5, 642, 341),
        (22, "DU123 IBKR Trader Workstation (Demo System)", 5, 642, 430),
        (23, "Unrelated warning", 6, 642, 341),
    ]
    try:
        with _Saved(
                tl, _all_tws_windows=lambda: rows,
                _win=lambda: (None, None, _U())):
            sweeper = tl.ResourceWarningSweeper(
                recorder=recorder, interval=0.05)
            result = sweeper.tick()
        event = recorder.events[0]
        check(result == "swept" and sweeper.dismissed == 1
              and closed == [(21, 0x0010)],
              "resource-warning sweeper dismisses only an exact match in one tick")
        check(event["phase"] == "resource_warning_sweep"
              and event["outcome"] == "succeeded"
              and event["window_title"].startswith("DU_REDACTED ")
              and "DU123" not in event["window_title"]
              and event["window_geometry"] == {"width": 642, "height": 341},
              "resource-warning dismissal is one redacted transcript event")

        class _FailedU:
            def PostMessageW(self, *_args):
                return False

        logs = []
        with _Saved(
                tl, _all_tws_windows=lambda: rows[:1],
                _win=lambda: (None, None, _FailedU())):
            failed = tl.ResourceWarningSweeper(
                recorder=recorder, on_progress=logs.append, interval=0.05)
            failed.tick()
        check(failed.errors == 1 and logs
              and "DU123" not in logs[0]
              and recorder.events[-1]["outcome"] == "failed",
              "failed warning dismissal is logged, redacted, and evidenced")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_resource_warning_sweeper_interlock_join_and_exception_containment():
    calls = []
    statuses = []
    logs = []

    def closed(*_args, **_kwargs):
        calls.append("close")
        return 0

    sweeper = tl.ResourceWarningSweeper(
        on_progress=logs.append, interval=0.05)
    with _Saved(tl, close_resource_warnings=closed):
        with tl.resource_warning_input_interlock():
            worker = threading.Thread(
                target=lambda: statuses.append(sweeper.tick()))
            worker.start()
            worker.join()
        check(statuses == ["interlocked"] and calls == [],
              "resource-warning sweeper yields throughout guarded input")

        ticked = threading.Event()

        def tick_once(*_args, **_kwargs):
            ticked.set()
            return 0

        with _Saved(tl, close_resource_warnings=tick_once):
            sweeper.start()
            ticked.wait(1.0)
            sweeper.stop()
        check(ticked.is_set() and not sweeper.alive,
              "resource-warning sweeper is finally joined with no orphan")

    attempts = []

    def flaky(*_args, **kwargs):
        attempts.append(True)
        if len(attempts) == 1:
            raise RuntimeError("DU123 transient enumeration error")
        callback = kwargs.get("on_result")
        if callback is not None:
            callback(
                (31, "DU123 IBKR Trader Workstation", 7, 642, 341),
                "succeeded")
        return 1

    recovered = tl.ResourceWarningSweeper(
        on_progress=logs.append, interval=0.05)
    with _Saved(tl, close_resource_warnings=flaky):
        first = recovered.tick()
        second = recovered.tick()
    check(first == "error" and second == "swept"
          and recovered.errors == 1 and recovered.dismissed == 1,
          "resource-warning sweep exceptions are nonfatal and later ticks recover")
    check(any("DU_REDACTED" in row for row in logs)
          and all("DU123" not in row for row in logs),
          "resource-warning sweep error logging is identity-redacted")


def test_resource_warning_sweeper_wraps_both_flow_phases_finally():
    events = []

    class _Probe:
        def __enter__(self):
            events.append("enter")
            return self

        def __exit__(self, *_exc):
            events.append("exit")
            return False

        def record_result(self, *_args):
            events.append("inline")

    def factory(**_kwargs):
        return _Probe()

    def launch_failure(*_args, **_kwargs):
        events.append("launch")
        raise RuntimeError("expected")

    with _Saved(
            tl, _resource_warning_sweeper=factory,
            _launch_many_impl=launch_failure):
        try:
            tl.launch_many([])
        except RuntimeError:
            pass
    check(events == ["enter", "launch", "exit"],
          "launch_many finally-exits its resource-warning sweeper")

    events.clear()

    def enable_success(*_args, **kwargs):
        events.append("enable")
        kwargs["resource_warning_sink"](
            (1, "DU123 IBKR Trader Workstation", 2, 642, 341),
            "succeeded")
        return {"ok": True}

    with _Saved(
            tl, _resource_warning_sweeper=factory,
            _enable_fleet_impl=enable_success):
        result = tl.enable_fleet([])
    check(result == {"ok": True}
          and events == ["enter", "enable", "inline", "exit"],
          "enable_fleet shares inline evidence and finally-exits its sweeper")


def test_handshake_atomic_race_directions():
    stamps = iter([
        "2026-07-16T23:55:09.000Z", "2026-07-16T23:55:10.000Z",
        "2026-07-16T23:55:11.000Z", "2026-07-16T23:55:11.100Z",
        "2026-07-17T00:00:09.000Z"])
    staged = th.RestartDecisionState(timestamp_fn=lambda: next(stamps))
    staged.mark_callback_entered()
    staged.mark_rendered()
    staged.mark_countdown_armed()
    staged.expire()
    timeline = th.popup_timing(staged.snapshot())
    check(timeline == {
        "scheduled_at": "2026-07-16T23:55:09.000Z",
        "callback_entered_at": "2026-07-16T23:55:10.000Z",
        "rendered_at": "2026-07-16T23:55:11.000Z",
        "countdown_armed_at": "2026-07-16T23:55:11.100Z",
        "settled_at": "2026-07-17T00:00:09.000Z",
        "expired_at": "2026-07-17T00:00:09.000Z",
    }, "handshake: popup stages produce a deterministic diagnostic timeline")

    settled_first = th.RestartDecisionState()
    check(settled_first.mark_rendered(),
          "handshake: a live popup can be marked rendered")
    check(settled_first.settle(
        True, th.USER_CONFIRMED, "confirmed"),
        "handshake: settle can win the terminal transition")
    check(not settled_first.expire(),
          "handshake: expiry cannot overwrite a settled confirmation")
    snap = settled_first.snapshot()
    check(snap.rendered and snap.settled and snap.go is True
          and snap.decision == th.USER_CONFIRMED and not snap.expired,
          "handshake: settle-first snapshot remains confirmed")

    expired_first = th.RestartDecisionState()
    check(expired_first.expire(),
          "handshake: expiry can win the terminal transition")
    check(not expired_first.mark_rendered()
          and not expired_first.settle(
              True, th.AUTO_CONFIRMED, "late confirmation"),
          "handshake: expire-first blocks render and late confirmation")
    snap = expired_first.snapshot()
    check(snap.settled and snap.go is False and snap.expired
          and not snap.callback_entered
          and snap.decision == th.EVENT_LOOP_STARVED,
          "handshake: expire-first snapshot records event-loop starvation")
    built = []
    check(not th.render_popup_safely(
              expired_first, lambda: built.append(True), lambda: None)
          and built == [],
          "handshake: an expired queued callback never constructs a popup")

    render_order = th.RestartDecisionState()
    observed = []
    check(th.render_popup_safely(
              render_order,
              lambda: observed.append(render_order.snapshot().rendered),
              lambda: None)
          and observed == [False] and render_order.snapshot().rendered,
          "handshake: rendered becomes true only after complete build")

    unrendered = th.RestartDecisionState()
    check(unrendered.settle(True, th.USER_CONFIRMED, "buggy confirmation"),
          "handshake: an unrendered confirmation terminates without waiting")
    snap = unrendered.snapshot()
    check(snap.go is False and snap.decision == "popup_not_rendered",
          "handshake: an unrendered confirmation fails closed")

    invalid = th.RestartDecisionState()
    invalid.mark_rendered()
    invalid.settle(True, "INVALID TOKEN", "bad token")
    snap = invalid.snapshot()
    check(snap.go is False and snap.decision == "invalid_popup_decision",
          "handshake: invalid confirmation token fails closed")


def test_handshake_schedule_render_timeout_and_declines():
    ports = [2000, 3000]
    emails = {2000: "a@example.com", 3000: "b@example.com"}

    schedule_progress = []
    schedule_mod = _HandshakeGuardMod()

    def schedule_boom(_state):
        raise RuntimeError("queue failed for owner@example.com DU123456")

    result = th.restart_ports_with_confirmation(
        ports, emails, schedule_boom, schedule_progress.append, schedule_mod,
        lambda *args, **kwargs: {"ok": True})
    check(all(value["decision"] == "popup_schedule_error"
              and not value["attempted"] for value in result.values()),
          "handshake: root.after failure is structured and not attempted")
    check(schedule_mod.events == [],
          "handshake: schedule failure creates no InputGuard")
    schedule_text = " ".join(schedule_progress)
    check("owner@example.com" not in schedule_text
          and "DU123456" not in schedule_text,
          "handshake: schedule errors redact account identity")

    cleaned = []
    render_mod = _HandshakeGuardMod()

    def schedule_render_error(state):
        def boom():
            raise RuntimeError("widget failed")
        th.render_popup_safely(
            state, boom, lambda: cleaned.append("cleanup"))

    result = th.restart_ports_with_confirmation(
        ports, emails, schedule_render_error, lambda _message: None,
        render_mod, lambda *args, **kwargs: {"ok": True})
    check(all(value["decision"] == th.POPUP_RENDER_ERROR
              and not value["attempted"] for value in result.values())
          and cleaned == ["cleanup"],
          "handshake: popup render failure settles and cleans up")
    check(render_mod.events == [],
          "handshake: render failure does not minimize or create a guard")

    timeout_mod = _HandshakeGuardMod()
    timeout_progress = []
    result = th.restart_ports_with_confirmation(
        ports, emails, lambda _state: None, timeout_progress.append,
        timeout_mod, lambda *args, **kwargs: {"ok": True},
        decision_timeout=0)
    check(all(value["decision"] == th.EVENT_LOOP_STARVED
              and not value["attempted"] for value in result.values()),
          "handshake: an unentered Tk callback is event-loop starvation")
    check(any("event_loop_starved" in line for line in timeout_progress),
          "handshake: starvation is loud in progress diagnostics")
    check(timeout_mod.events == [],
          "handshake: starved loop does not minimize or create a guard")

    entered_mod = _HandshakeGuardMod()
    result = th.restart_ports_with_confirmation(
        ports, emails, lambda state: state.mark_callback_entered(),
        lambda _message: None, entered_mod,
        lambda *args, **kwargs: {"ok": True}, decision_timeout=0)
    check(all(value["decision"] == th.POPUP_DECISION_TIMEOUT
              and not value["attempted"] for value in result.values()),
          "handshake: an entered but silent callback remains decision timeout")
    check(entered_mod.events == [],
          "handshake: silent entered callback still fails before the guard")

    for token in (th.NOT_NOW_DECLINED, th.ESCAPE_DECLINED,
                  th.WINDOW_CLOSED_DECLINED):
        decline_mod = _HandshakeGuardMod()
        result = th.restart_ports_with_confirmation(
            ports, emails, _rendered_decision(token, go=False),
            lambda _message: None, decline_mod,
            lambda *args, **kwargs: {"ok": True})
        check(all(value["decision"] == token and not value["attempted"]
                  for value in result.values()),
              f"handshake: explicit decline remains distinct ({token})")
        check(decline_mod.events == [],
              f"handshake: explicit decline does not create guard ({token})")


def test_handshake_launcher_outcomes_and_redaction():
    ports = [2000, 3000, 4000]
    emails = {
        2000: "first@example.com",
        3000: "owner@example.com",
        4000: "last@example.com",
    }
    progress = []
    calls = []
    guard_mod = _HandshakeGuardMod()

    def launcher(email, port, on_progress=None, abort_check=None):
        calls.append((email, port, callable(abort_check)))
        if port == 2000:
            on_progress("logged in as DU987654 for first@example.com")
            return {"ok": True}
        if port == 3000:
            raise RuntimeError(
                "login failed for owner@example.com account DU123456 "
                + "x" * 400)
        return {"ok": False}

    result = th.restart_ports_with_confirmation(
        ports, emails, _rendered_decision(th.AUTO_CONFIRMED),
        progress.append, guard_mod, launcher)
    check({key: result[2000][key] for key in
           ("ok", "attempted", "decision", "reason")} == {
        "ok": True, "attempted": True,
        "decision": th.AUTO_CONFIRMED,
        "reason": "launcher reported port listening",
    } and isinstance(result[2000].get("popup_timing"), dict),
          "handshake: successful launcher preserves decision and timeline")
    check(result[3000]["decision"] == "launcher_error"
          and result[3000]["attempted"] and not result[3000]["ok"]
          and len(result[3000]["reason"]) == th.REASON_CAP,
          "handshake: exception after launcher entry is attempted=True")
    check(result[4000]["decision"] == "launcher_failed"
          and result[4000]["attempted"] and not result[4000]["ok"],
          "handshake: launcher false result is attempted and explicit")
    check([port for _email, port, _abort in calls] == ports
          and all(has_abort for _email, _port, has_abort in calls),
          "handshake: each port enters launcher once with abort callback")
    text = " ".join(progress) + " " + result[3000]["reason"]
    check("owner@example.com" not in text and "first@example.com" not in text
          and "DU123456" not in text and "DU987654" not in text
          and "[redacted-email]" in text and "[redacted-account]" in text,
          "handshake: launcher progress and outcome reasons are redacted")
    check(guard_mod.events.count("block") == 3
          and guard_mod.events.count("unblock") == 3
          and guard_mod.events[-2:] == ["stop", "restore"],
          "handshake: every launcher is bracketed and final cleanup runs")
    check(all(sk._normalize_restart_outcome(value) == value
              for value in result.values()),
          "handshake: every callback result satisfies the engine schema")


def test_handshake_cancel_and_late_listener_fences():
    ports = [2000, 3000]
    emails = {2000: "a@example.com", 3000: "b@example.com"}

    calls = []
    guard_mod = _HandshakeGuardMod()
    result = th.restart_ports_with_confirmation(
        ports, emails, _rendered_decision(), lambda _message: None,
        guard_mod,
        lambda _email, port, **_kwargs:
            calls.append(port) or {"ok": True},
        port_up=lambda port: port == 2000)
    check(result[2000]["ok"] and not result[2000]["attempted"]
          and result[2000]["decision"] == "already_listening"
          and calls == [3000],
          "handshake: a port that healed during the popup is never relaunched")

    waiting_cancel = threading.Event()

    def cancel_while_waiting(_state):
        waiting_cancel.set()

    calls.clear()
    result = th.restart_ports_with_confirmation(
        ports, emails, cancel_while_waiting, lambda _message: None,
        _HandshakeGuardMod(),
        lambda *_args, **_kwargs: calls.append(True) or {"ok": True},
        decision_timeout=1.0, cancelled=waiting_cancel.is_set)
    check(all(value["decision"] == "fetch_cancelled"
              and not value["attempted"] for value in result.values())
          and calls == [],
          "handshake: fetch cancel interrupts the popup wait before launch")

    batch_cancel = threading.Event()
    calls.clear()

    def cancel_after_first(_email, port, **_kwargs):
        calls.append(port)
        batch_cancel.set()
        return {"ok": True}

    result = th.restart_ports_with_confirmation(
        ports, emails, _rendered_decision(), lambda _message: None,
        _HandshakeGuardMod(), cancel_after_first,
        cancelled=batch_cancel.is_set)
    check(result[2000]["ok"] and result[2000]["attempted"]
          and result[3000]["decision"] == "fetch_cancelled"
          and not result[3000]["attempted"] and calls == [2000],
          "handshake: cancel between launchers suppresses the remaining batch")


def test_handshake_guard_abort_and_cleanup_failures():
    ports = [2000, 3000]
    emails = {2000: "a@example.com", 3000: "b@example.com"}

    for failure, expected in (("init", "guard_init_error"),
                              ("start", "guard_start_error"),
                              ("minimize", "minimize_error")):
        calls = []
        guard_mod = _HandshakeGuardMod(fail={failure})
        result = th.restart_ports_with_confirmation(
            ports, emails, _rendered_decision(), lambda _message: None,
            guard_mod,
            lambda *args, **kwargs: calls.append(args) or {"ok": True})
        check(all(value["decision"] == expected
                  and not value["attempted"] for value in result.values()),
              f"handshake: {failure} failure is structured and not attempted")
        check(calls == [],
              f"handshake: {failure} failure never enters launcher")

    abort_mod = _HandshakeGuardMod(aborted=True)
    abort_calls = []
    result = th.restart_ports_with_confirmation(
        ports, emails, _rendered_decision(), lambda _message: None,
        abort_mod,
        lambda *args, **kwargs: abort_calls.append(args) or {"ok": True})
    check(all(value["decision"] == "restart_aborted"
              and not value["attempted"] for value in result.values())
          and abort_calls == [],
          "handshake: pre-launch ESC abort skips the complete batch")

    block_mod = _HandshakeGuardMod(fail={"block"})
    block_calls = []
    result = th.restart_ports_with_confirmation(
        ports, emails, _rendered_decision(), lambda _message: None,
        block_mod,
        lambda *args, **kwargs: block_calls.append(args) or {"ok": True})
    check(all(value["decision"] == "guard_block_error"
              and not value["attempted"] for value in result.values())
          and block_calls == [],
          "handshake: block failure fails closed before launcher entry")

    unblock_mod = _HandshakeGuardMod(fail={"unblock"})
    unblock_calls = []
    result = th.restart_ports_with_confirmation(
        ports, emails, _rendered_decision(), lambda _message: None,
        unblock_mod,
        lambda _email, port, **_kwargs:
            unblock_calls.append(port) or {"ok": True})
    check(result[2000]["ok"] and result[2000]["attempted"],
          "handshake: cleanup failure preserves the known current-port result")
    check(result[3000]["decision"] == "guard_cleanup_error"
          and not result[3000]["attempted"] and unblock_calls == [2000],
          "handshake: cleanup uncertainty blocks every later launcher")

    cleanup_progress = []
    cleanup_mod = _HandshakeGuardMod(fail={"stop", "restore"})
    result = th.restart_ports_with_confirmation(
        [2000], emails, _rendered_decision(), cleanup_progress.append,
        cleanup_mod, lambda *args, **kwargs: {"ok": True})
    check(result[2000]["ok"] and result[2000]["attempted"],
          "handshake: final cleanup errors do not replace a known success")
    cleanup_text = " ".join(cleanup_progress)
    check("cleanup failed" in cleanup_text
          and "owner@example.com" not in cleanup_text
          and "DU123456" not in cleanup_text,
          "handshake: cleanup errors are reported and redacted")

    launch_abort_mod = _HandshakeGuardMod()

    def aborting_launcher(*_args, **_kwargs):
        launch_abort_mod.guard.abort = True
        raise RuntimeError("aborted during launcher")

    result = th.restart_ports_with_confirmation(
        [2000], emails, _rendered_decision(), lambda _message: None,
        launch_abort_mod, aborting_launcher)
    check(result[2000]["decision"] == "launcher_aborted"
          and result[2000]["attempted"],
          "handshake: abort after launcher entry remains attempted=True")

    invalid_mod = _HandshakeGuardMod()
    result = th.restart_ports_with_confirmation(
        [2000], emails, _rendered_decision(), lambda _message: None,
        invalid_mod, lambda *args, **kwargs: None)
    check(result[2000]["decision"] == "invalid_launcher_return"
          and result[2000]["attempted"],
          "handshake: malformed launcher return is an attempted failure")


def test_unattended_notice_source_boundary():
    source_path = Path(__file__).resolve().parents[1] / "display_data.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    methods = {
        node.name: node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    targets = {
        "_show_unattended_notice",
        "_storage_ibkr_no_go",
        "_storage_halted_series_popup",
        "_storage_lost_connection_popup",
        "_storage_ibkr_finished_popup",
        "_storage_ibkr_done",
        "_internal_check_after_fetch",
        "_gap_heal_after_fetch",
        "_fixdata_done",
        "_fleet_summary_box",
        "_storage_validate_ports",
        "_render_port_health",
        "_storage_find_no_go",
        "_storage_find_done",
    }
    missing = sorted(targets - methods.keys())
    modal = {}
    for name in sorted(targets & methods.keys()):
        calls = [
            ast.unparse(node.func) for node in ast.walk(methods[name])
            if isinstance(node, ast.Call)
        ]
        found = [call for call in calls if call.startswith("messagebox.")]
        if found:
            modal[name] = found
    check(not missing and not modal,
          "unattended notices: audited completion paths contain no messagebox"
          + (f" (missing={missing}, modal={modal})"
             if missing or modal else ""))
    helper_calls = [
        ast.unparse(node.func) for node in ast.walk(
            methods["_show_unattended_notice"])
        if isinstance(node, ast.Call)
    ]
    check("tk.Toplevel" in helper_calls
          and not any(call.endswith("grab_set") or call.endswith("wait_window")
                      for call in helper_calls),
          "unattended notices: shared Toplevel never grabs or waits")
    confirmation_calls = [
        ast.unparse(node.func) for node in ast.walk(methods["_storage_ibkr_start"])
        if isinstance(node, ast.Call)
    ]
    check("messagebox.askyesno" in confirmation_calls,
          "unattended notices: fresh user confirmation remains modal")


def test_fleet_gui_transcript_wiring_source_boundary():
    source_path = Path(__file__).resolve().parents[1] / "display_data.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    methods = {
        node.name: node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    startup = ast.unparse(methods["_storage_multiport_start"])
    restart = ast.unparse(methods["_storage_restart_dead_start"])
    check("FleetTranscript('fleet_bringup', step_verification=step_verify, "
          "expected_ports=ports)" in startup
          and "launch_many(emails, on_progress=say, recorder=recorder, "
          "ports=ports, memory_cap_result=memory_cap_result)"
          in startup
          and "enable_fleet(pairs, on_progress=say, recorder=recorder)"
          in startup
          and "verify_handshake(p, expected_account, recorder=recorder, "
          "on_progress=say)" in startup,
          "GUI startup shares one transcript across all proof phases")
    check("FleetTranscript('restart_dead')" in restart
          and "restart_dead(fleet, on_progress=say, recorder=recorder)" in restart
          and "recorder.write" in restart,
          "GUI down-port restart owns and publishes one transcript")


def test_fleet_gui_single_completion_surface_source_boundary():
    source_path = Path(__file__).resolve().parents[1] / "display_data.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    methods = {
        node.name: node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    popup = ast.unparse(methods["_storage_fleet_popup"])
    summary = ast.unparse(methods["_fleet_summary_box"])
    lift = ast.unparse(methods["_raise_fleet_completion"])
    startup = ast.unparse(methods["_storage_multiport_start"])
    restart = ast.unparse(methods["_storage_restart_dead_start"])
    check("window_open = bool(win.winfo_exists())" in popup
          and "win if window_open else None" in popup
          and "append if window_open else None" in popup,
          "fleet completion detects live versus backgrounded progress window")
    check("if parent_open and callable(embed)" in summary
          and "self._show_unattended_notice" in summary
          and summary.count("self._show_unattended_notice") == 1,
          "fleet completion chooses embedded summary or one fallback notice")
    check("Problems:" in summary and "port {p}: {r}" in summary,
          "fleet completion keeps per-port problem reasons visible")
    check("win.attributes('-topmost', True)" in lift
          and "win.attributes('-topmost', False)" in lift
          and "win.after(400, drop_topmost)" in lift,
          "fleet completion uses bounded set-then-drop topmost behavior")
    check("embed=append" in startup and "embed=append" in restart,
          "startup and restart share the single-surface completion contract")


def _load_display_method(name):
    source_path = Path(__file__).resolve().parents[1] / "display_data.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    method = next(
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    )
    namespace = {}
    module = ast.Module(body=[method], type_ignores=[])
    exec(compile(module, str(source_path), "exec"), namespace)
    return namespace[name]


class _FakeFleetWindow:
    def __init__(self, exists=True):
        self.exists = exists
        self.events = []
        self.after_callback = None

    def winfo_exists(self):
        return self.exists

    def deiconify(self):
        self.events.append("deiconify")

    def lift(self):
        self.events.append("lift")

    def attributes(self, name, value):
        self.events.append((name, value))

    def after(self, delay, callback):
        self.events.append(("after", delay))
        self.after_callback = callback

    def focus_force(self):
        self.events.append("focus")


class _FakeFleetUi:
    def __init__(self):
        self.root = object()
        self.notices = []
        self.raised = []

    def _show_unattended_notice(self, title, body, *, level="info", parent=None):
        notice = _FakeFleetWindow()
        self.notices.append((title, body, level, parent, notice))
        return notice

    def _raise_fleet_completion(self, win):
        self.raised.append(win)


def test_fleet_gui_single_completion_surface_behavior():
    render = _load_display_method("_fleet_summary_box")
    ui = _FakeFleetUi()
    progress = _FakeFleetWindow()
    embedded = []
    result = render(
        ui, "Start up multi-port",
        {"up": [2000, 3000], "total": 2, "problems": {}},
        parent=progress, embed=embedded.append)
    check(result is progress and len(embedded) == 1 and not ui.notices
          and ui.raised == [progress]
          and "2/2 port(s) up and bound" in embedded[0]
          and "Done" in embedded[0] and "close this window." in embedded[0],
          "fleet completion embeds one success summary in the open progress window")

    embedded.clear()
    ui.raised.clear()
    render(
        ui, "Start up multi-port",
        {"up": [2000], "total": 2,
         "problems": {3000: "enable text not matched"}},
        parent=progress, embed=embedded.append)
    check(len(embedded) == 1 and not ui.notices
          and ui.raised == [progress]
          and "port 3000: enable text not matched" in embedded[0],
          "fleet completion preserves per-port reasons in the embedded surface")

    backgrounded = _FakeFleetWindow(exists=False)
    embedded.clear()
    ui.raised.clear()
    result = render(
        ui, "Restart dead ports",
        {"up": [], "total": 1, "problems": {2000: "login failed"}},
        parent=backgrounded, embed=embedded.append)
    check(not embedded and len(ui.notices) == 1
          and result is ui.notices[0][4] and ui.raised == [result]
          and ui.notices[0][2] == "warning"
          and "port 2000: login failed" in ui.notices[0][1],
          "fleet completion creates one raised fallback after backgrounding")

    ui.notices.clear()
    ui.raised.clear()
    embedded.clear()
    render(ui, "Start up multi-port", None, parent=progress,
           error="fleet startup failed", embed=embedded.append)
    check(len(embedded) == 1 and not ui.notices
          and ui.raised == [progress]
          and "fleet startup failed" in embedded[0],
          "fleet completion keeps errors on the one open completion surface")


def test_fleet_gui_completion_topmost_behavior():
    raise_completion = _load_display_method("_raise_fleet_completion")
    win = _FakeFleetWindow()
    raise_completion(object(), win)
    before_drop = list(win.events)
    if win.after_callback is not None:
        win.after_callback()
    check(before_drop == ["deiconify", "lift", ("-topmost", True),
                          ("after", 400), "focus"],
          "fleet completion lifts and schedules a bounded topmost window")
    check(win.events[-1] == ("-topmost", False),
          "fleet completion drops topmost after the bounded lift")


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    with _Saved(
            tl, _resource_warning_sweeper=_noop_resource_warning_sweeper):
        for t in tests:
            t()
    shutil.rmtree(_TEST_BASE, ignore_errors=True)
    total = _PASS[0] + _FAIL[0]
    print(f"\ntws_restart_selftest: {_PASS[0]}/{total} passed, "
          f"{_FAIL[0]} failed")
    return 1 if _FAIL[0] else 0


if __name__ == "__main__":
    sys.exit(main())
