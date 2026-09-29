"""Row 38 M0 acceptance harness — mid-run automatic fleet restart.

OFFLINE ONLY: temp roots, fake adapters, redirected operation gate. No GUI,
no ports, no network, no bank writes. Safe to run while a live fetch runs.

Exit codes:
  0 = M1 feature present AND every mandatory scenario passes (the M2
      fleet-down scenario runs only when its flag is present; its absence is
      reported as [M2 PENDING], not a failure).
  3 = feature absent (M1 not implemented yet) — the baseline scaffold proved
      itself against the CURRENT engine and the exact missing surface is
      listed. This is the expected pre-M1 result.
  1 = harness failure (a scenario or the baseline failed).

Contract pinned here (FLEET_MIDRUN_AUTORESTART_PLAN.md §4):
  - gap_fill_parallel accepts restart_ports= (resilient wrapper forwards it).
  - Module constants: MIDRUN_RESTART (master switch), REVIVE_AFTER_S,
    MIDRUN_RESTART_MAX_PER_PORT, BATCH_WINDOW_S, DECLINE_COOLDOWN_S.
  - A port that hard-dies and stays dead (socket closed) for REVIVE_AFTER_S
    gets ONE batched restart_ports call; on success the existing monitors
    re-adopt it and the run continues to full completion.
  - Failed/declined batches respect MIDRUN_RESTART_MAX_PER_PORT /
    DECLINE_COOLDOWN_S; cancel suppresses the controller entirely;
    MIDRUN_RESTART=False restores today's behavior byte-for-byte.
  - Evidence lands in report["midrun_restart_events"].
  - M2 (flag MIDRUN_FLEETDOWN_REVIVAL): an all-ports death attempts ONE
    revival during HOLD before fleet_down_finalize.
"""
import atexit
import inspect
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
_GATE_DIR = tempfile.TemporaryDirectory(prefix="midrun_ref_gate_",
                                        ignore_cleanup_errors=True)
og.LOCK_PATH = Path(_GATE_DIR.name) / ".market_data_operation.lock"


def _restore_gate():
    og.LOCK_PATH = _GATE_OLD
    _GATE_DIR.cleanup()


atexit.register(_restore_gate)

import addstock_watchdog as aw                                 # noqa: E402
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
    return Path(tempfile.mkdtemp(prefix="midrun_ref_")) / ss.STORAGE_DIR_NAME


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


class PortScript:
    """Thread-safe per-port death/revival choreography + restart recording."""

    def __init__(self, ports, kill_at_fetch=None, slow_s=0.0,
                 await_restart=False):
        self._lock = threading.Lock()
        self.kill_at = dict(kill_at_fetch or {})    # port -> own physical send#
        self.fetches = {p: 0 for p in ports}
        self.started = set()
        self.start_barrier = threading.Barrier(len(ports), timeout=10.0)
        self.restart_seen = threading.Event()
        self.death_seen = threading.Event()
        self.await_restart = await_restart
        self.sync_failures = []
        self.dead = set()
        self.killed_once = set()      # each scripted death fires ONCE per port
        self.injected_kills = []      # (monotonic, port, own send#) evidence
        self.death_t = {}
        self.restart_calls = []                      # (monotonic, [ports])
        self.slow_s = slow_s

    def on_connect(self, port):
        # Each worker already owns its initial job when its adapter is made.
        # Hold that job until every port has one: real shared pacing must not
        # let survivors drain the queue before the victim ever takes work.
        with self._lock:
            first = port not in self.started
            self.started.add(port)
        if first:
            try:
                self.start_barrier.wait()
            except threading.BrokenBarrierError:
                self.sync_failures.append(f"initial worker barrier: {port}")
                raise

    def on_fetch(self, port):
        # Kill on the victim's own physical send, within its FIRST assigned
        # job, not on a scheduler-dependent second qualification/job. Retain
        # the real production pacer and one-shot death/revival behavior.
        with self._lock:
            self.fetches[port] += 1
            if (port not in self.dead
                    and port not in self.killed_once
                    and self.fetches[port] >= self.kill_at.get(port, 1 << 30)):
                self.dead.add(port)
                self.killed_once.add(port)
                self.injected_kills.append(
                    (time.monotonic(), port, self.fetches[port]))
                self.death_t.setdefault(port, time.monotonic())
                self.death_seen.set()
                return False
            alive = port not in self.dead
        # Keep real fill work in flight until the REAL restart controller has
        # had an opportunity to act. A missing callback fails boundedly; this
        # synchronization neither calls nor impersonates the controller.
        if alive and port not in self.kill_at and self.await_restart:
            if not self.restart_seen.wait(10.0):
                self.sync_failures.append(f"restart opportunity: {port}")
                raise AssertionError("restart controller did not answer in 10s")
        return alive

    def is_dead(self, port):
        with self._lock:
            return port in self.dead

    def port_open(self, port):
        with self._lock:
            return port not in self.dead

    def revive(self, port):
        with self._lock:
            self.dead.discard(port)

    def record_restart(self, batch):
        with self._lock:
            self.restart_calls.append((time.monotonic(), sorted(batch)))
        self.restart_seen.set()


class ScriptedAdapter:
    """FakeAdapter-shaped; raises hard (socket_reset) once its port is dead."""

    def __init__(self, port, script, days, conid_base=41000):
        self.port, self.script, self.days = port, script, days
        self.conid = conid_base + port
        # A usable head lets each series reach its historical-bar sends. None
        # previously failed before fetching, so a series-count-only assertion
        # could mistake failed series for completed fill work.
        self.head = datetime.combine(MON, ss.RTH_FIRST, tzinfo=NY)

    def _gate(self):
        if self.script.is_dead(self.port):
            raise ConnectionResetError(f"scripted death port {self.port}")

    def account(self):
        return f"DU{self.port}"

    def qualify(self, symbol):
        self._gate()
        return self.conid, SimpleNamespace(symbol=symbol)

    def contract_for(self, conid):
        self._gate()
        return SimpleNamespace(symbol="?", conId=conid)

    def qualify_many(self, symbols, chunk=50, progress=None, cancel=None):
        self._gate()
        return {s: self.conid for s in symbols}

    def head_timestamp(self, contract, what_to_show="TRADES"):
        self._gate()
        return self.head

    def is_connected(self):
        return not self.script.is_dead(self.port)

    def reconnect(self):
        self._gate()

    def fetch(self, contract, end_dt, duration, bar_size, what_to_show="TRADES"):
        self._gate()
        if not self.script.on_fetch(self.port):
            raise ConnectionResetError(f"scripted death port {self.port}")
        if self.script.slow_s:
            time.sleep(self.script.slow_s)
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

    def disconnect(self):
        pass


def tiny_watchdog(fleet_grace_s=30.0):
    def factory(ports, maintenance_provider=None):
        return aw.HardDeathWatchdog(
            ports, maintenance_provider=maintenance_provider,
            port_grace_s=0.4, fleet_grace_s=fleet_grace_s,
            probe_backoff_s=(0.05, 0.1))
    return factory


def run_scenario(ports, n_tickers, script, restart_cb=None, cancel=None,
                 fleet_grace_s=30.0, extra=None):
    """Drive a REAL gap_fill_parallel run over fake adapters; return
    (report, progress_lines). restart_cb is passed only when supplied AND the
    engine supports it."""
    tickers = [f"MR{i:02d}" for i in range(n_tickers)]
    # every ticker gets the same 2-day bar shape — completeness is judged by
    # series counts, not per-ticker prices
    shared_days = {MON: stub_bars(MON, 4, 10.0), TUE: stub_bars(TUE, 4, 11.0)}

    def adapter_factory(host, aports):
        port = int(aports[0])

        def make():
            script.on_connect(port)
            return ScriptedAdapter(port, script, shared_days)
        return make

    lines = []

    def progress(msg):
        lines.append(msg)

    kwargs = dict(
        progress=progress, cancel=cancel, today=WED, since=SINCE,
        adapter_factory=adapter_factory,
        port_up=script.port_open,
        reprobe_interval=0.5,
        hard_death_watchdog=True,
        watchdog_factory=tiny_watchdog(fleet_grace_s))
    if restart_cb is not None:
        kwargs["restart_ports"] = restart_cb
    if extra:
        kwargs.update(extra)
    rep = sk.nightly_gap_fill_parallel(
        fresh_root(), [(t, "1m") for t in tickers], list(ports), **kwargs)
    if script.sync_failures:
        raise AssertionError(script.sync_failures)
    if not rep.get("cancelled"):
        failures = [r for r in rep.get("series", []) if r.get("halt")]
        if failures or not any(r.get("added", 0) for r in rep.get("series", [])):
            raise AssertionError(f"fixture did not complete real bar work: {failures}")
    return rep, lines


def series_done(rep):
    return len(rep.get("series") or [])


PORTS = [2000, 3000, 4000]

print("=== [S0] baseline: partial hard death, NO mid-run restart wired ====")
_s0 = PortScript(PORTS, kill_at_fetch={2000: 1}, slow_s=0.02)
_s0rep, _s0lines = run_scenario(PORTS, 9, _s0)
check("S0 scaffold: victim port actually died mid-run",
      _s0.is_dead(2000) and 2000 in _s0.death_t)
check("S0 baseline: run completes every series on the survivors",
      series_done(_s0rep) == 9,
      f"series={series_done(_s0rep)} rep_keys={sorted(_s0rep)}")
_s0wd = _s0rep.get("watchdog") or {}
check("S0 baseline: victim ends watchdog-DEAD with its job rerouted, and is "
      "NOT in aborted_ports (why post-run recovery never relaunches it)",
      _s0wd.get("states", {}).get("2000") == "DEAD"
      and 2000 in (_s0wd.get("rerouted_ports") or [])
      and 2000 not in {int(p) for p in (_s0rep.get("aborted_ports") or {})},
      f"watchdog={_s0wd} aborted={_s0rep.get('aborted_ports')}")
check("S0 baseline: no restart callback existed, none recorded",
      _s0.restart_calls == [], str(_s0.restart_calls))

print("=== [feature probe] M1 surface ====================================")
_sig_ok = "restart_ports" in inspect.signature(sk.gap_fill_parallel).parameters
_CONSTS = ["MIDRUN_RESTART", "REVIVE_AFTER_S", "MIDRUN_RESTART_MAX_PER_PORT",
           "BATCH_WINDOW_S", "DECLINE_COOLDOWN_S"]
_missing = ([] if _sig_ok else ["gap_fill_parallel(restart_ports=...)"]) + \
    [c for c in _CONSTS if not hasattr(sk, c)]
if _missing:
    print()
    print(f"{N[0]} checks, {len(FAILS)} failed (baseline stage)")
    if FAILS:
        for f in FAILS:
            print(f"  FAILED: {f}")
        sys.exit(1)
    print("FEATURE ABSENT — M1 not implemented yet. Missing surface:")
    for m in _missing:
        print(f"  - {m}")
    print("Expected pre-M1 result. Exit 3.")
    sys.exit(3)


def _patched(**over):
    class _Ctx:
        def __enter__(self):
            self.old = {k: getattr(sk, k) for k in over}
            for k, v in over.items():
                setattr(sk, k, v)

        def __exit__(self, *exc):
            for k, v in self.old.items():
                setattr(sk, k, v)
    return _Ctx()


print("=== [S0b] master switch OFF == today's behavior ====================")
_s0b = PortScript(PORTS, kill_at_fetch={2000: 1}, slow_s=0.02)


def _s0b_cb(batch):
    _s0b.record_restart(batch)
    return {int(p): True for p in batch}


with _patched(MIDRUN_RESTART=False):
    _s0brep, _ = run_scenario(PORTS, 9, _s0b, restart_cb=_s0b_cb)
check("S0b: MIDRUN_RESTART=False never invokes the callback",
      _s0b.restart_calls == [], str(_s0b.restart_calls))
check("S0b: run still completes on survivors",
      series_done(_s0brep) == 9, f"series={series_done(_s0brep)}")

print("=== [S1] confirmed-dead port is revived and rejoins the run ========")
_s1 = PortScript(PORTS, kill_at_fetch={2000: 1}, slow_s=0.05, await_restart=True)


def _s1_cb(batch):
    _s1.record_restart(batch)
    for p in batch:
        _s1.revive(int(p))
    return {int(p): True for p in batch}


with _patched(MIDRUN_RESTART=True, REVIVE_AFTER_S=0.6, BATCH_WINDOW_S=0.2,
              MIDRUN_RESTART_MAX_PER_PORT=2, DECLINE_COOLDOWN_S=30.0):
    _s1rep, _s1lines = run_scenario(PORTS, 12, _s1, restart_cb=_s1_cb)
check("S1: exactly one restart batch, containing exactly the dead port",
      len(_s1.restart_calls) == 1 and _s1.restart_calls[0][1] == [2000],
      str(_s1.restart_calls))
check("S1: the restart came only after the confirmation window",
      _s1.restart_calls
      and (_s1.restart_calls[0][0] - _s1.death_t[2000]) >= 0.6 * 0.6,
      f"delta={(_s1.restart_calls[0][0] - _s1.death_t[2000]) if _s1.restart_calls else '-'}")
check("S1: every series completed (run continued through the revival)",
      series_done(_s1rep) == 12, f"series={series_done(_s1rep)}")
check("S1: report carries midrun_restart_events with the revived port ok",
      any(int(e.get("port", -1)) == 2000 and e.get("ok")
          for e in (_s1rep.get("midrun_restart_events") or [])),
      str(_s1rep.get("midrun_restart_events")))
check("S1: revived port rejoined — watchdog reports it HEALTHY at run end",
      (_s1rep.get("watchdog") or {}).get("states", {}).get("2000") == "HEALTHY",
      f"watchdog={_s1rep.get('watchdog')}")

print("=== [S2] permanently dead port: attempts capped, run unharmed ======")
_s2 = PortScript(PORTS, kill_at_fetch={2000: 1}, slow_s=0.05, await_restart=True)


def _s2_cb(batch):
    _s2.record_restart(batch)
    return {int(p): False for p in batch}      # relaunch fails; stays dead


with _patched(MIDRUN_RESTART=True, REVIVE_AFTER_S=0.3, BATCH_WINDOW_S=0.1,
              MIDRUN_RESTART_MAX_PER_PORT=2, DECLINE_COOLDOWN_S=0.2):
    _s2rep, _ = run_scenario(PORTS, 12, _s2, restart_cb=_s2_cb)
check("S2: at least one attempt, never more than the per-port cap",
      1 <= len(_s2.restart_calls) <= 2, str(_s2.restart_calls))
check("S2: permanently dead port ends watchdog-DEAD; every series still "
      "completes on the survivors",
      (_s2rep.get("watchdog") or {}).get("states", {}).get("2000") == "DEAD"
      and series_done(_s2rep) == 12,
      f"watchdog={_s2rep.get('watchdog')} series={series_done(_s2rep)}")

print("=== [S3] declined batch: one offer, then cooldown ==================")
_s3 = PortScript(PORTS, kill_at_fetch={2000: 1}, slow_s=0.05, await_restart=True)


def _s3_cb(batch):
    _s3.record_restart(batch)
    return {int(p): {"ok": False, "decision": "not_now_declined",
                     "attempted": False, "reason": "declined in harness"}
            for p in batch}


with _patched(MIDRUN_RESTART=True, REVIVE_AFTER_S=0.3, BATCH_WINDOW_S=0.1,
              MIDRUN_RESTART_MAX_PER_PORT=5, DECLINE_COOLDOWN_S=120.0):
    _s3rep, _ = run_scenario(PORTS, 12, _s3, restart_cb=_s3_cb)
check("S3: a declined batch is offered once and enters cooldown",
      len(_s3.restart_calls) == 1, str(_s3.restart_calls))
check("S3: run completes on survivors after the decline",
      series_done(_s3rep) == 12, f"series={series_done(_s3rep)}")

print("=== [S4] cancel suppresses the controller ==========================")
_s4 = PortScript(PORTS, kill_at_fetch={2000: 1}, slow_s=0.05)
_s4_cancel = threading.Event()


def _s4_cb(batch):
    _s4.record_restart(batch)
    return {int(p): True for p in batch}


def _s4_watch():
    if not _s4.death_seen.wait(10.0):
        _s4.sync_failures.append("cancel watcher: victim never died")
        return
    time.sleep(0.1)
    _s4_cancel.set()


_s4_watcher = threading.Thread(target=_s4_watch, daemon=True)
_s4_watcher.start()
with _patched(MIDRUN_RESTART=True, REVIVE_AFTER_S=5.0, BATCH_WINDOW_S=0.1,
              MIDRUN_RESTART_MAX_PER_PORT=2, DECLINE_COOLDOWN_S=1.0):
    try:
        _s4rep, _ = run_scenario(PORTS, 12, _s4, restart_cb=_s4_cb,
                                 cancel=_s4_cancel)
    except sk.Cancelled:
        _s4rep = {"cancelled": True}
_s4_watcher.join(timeout=10.0)
if _s4_watcher.is_alive() or _s4.sync_failures:
    raise AssertionError(f"cancel watcher did not finish cleanly: {_s4.sync_failures}")
check("S4: cancel before the confirmation window fires -> zero restarts",
      _s4.restart_calls == [], str(_s4.restart_calls))
check("S4: the run acknowledged the cancel",
      bool(_s4rep.get("cancelled")), str({k: _s4rep.get(k) for k in
                                          ("cancelled",)}))

print("=== [S5/M2] fleet-down revival before finalize =====================")
if not hasattr(sk, "MIDRUN_FLEETDOWN_REVIVAL"):
    print("[M2 PENDING] MIDRUN_FLEETDOWN_REVIVAL absent — scenario skipped "
          "(required for the M2 checkpoint, not for M1).")
else:
    _s5 = PortScript(PORTS, kill_at_fetch={p: 1 for p in PORTS}, slow_s=0.05)

    def _s5_cb(batch):
        _s5.record_restart(batch)
        for p in batch:
            _s5.revive(int(p))
        return {int(p): True for p in batch}

    with _patched(MIDRUN_RESTART=True, MIDRUN_FLEETDOWN_REVIVAL=True,
                  REVIVE_AFTER_S=0.3, BATCH_WINDOW_S=0.2,
                  MIDRUN_RESTART_MAX_PER_PORT=2, DECLINE_COOLDOWN_S=30.0):
        _s5rep, _ = run_scenario(PORTS, 9, _s5, restart_cb=_s5_cb,
                                 fleet_grace_s=8.0)
    _s5_revived = set().union(*[set(b) for _, b in _s5.restart_calls]) \
        if _s5.restart_calls else set()
    check("S5: revival offered during HOLD covers the whole dead fleet",
          _s5_revived == set(PORTS), str(_s5.restart_calls))
    check("S5: run resumed instead of fleet_down finalize",
          not _s5rep.get("fleet_down_finalize")
          and series_done(_s5rep) == 9,
          f"finalize={_s5rep.get('fleet_down_finalize')} "
          f"series={series_done(_s5rep)}")

print()
print(f"{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    for f in FAILS:
        print(f"  FAILED: {f}")
    sys.exit(1)
print("ALL PASS")
sys.exit(0)
