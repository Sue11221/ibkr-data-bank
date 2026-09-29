"""Headless tests for Add Stocks hard-death protection."""

from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))

import addstock_watchdog as aw
import fix_data_pipeline
import live_spot_probe
import stock_ibkr as sk


PASS = [0]
FAIL = [0]
NOW = datetime(2026, 7, 17, 16, 0, tzinfo=timezone.utc)


def check(condition, label):
    if condition:
        PASS[0] += 1
    else:
        FAIL[0] += 1
        print(f"  FAIL: {label}")


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)


class RequestTimeout(ConnectionError):
    pass


class PacingViolation(ConnectionError):
    pass


class ConnectionLost(ConnectionError):
    pass


def test_production_grace_defaults():
    check(aw.PORT_GRACE_S == 150.0
          and aw.FLEET_GRACE_S == 150.0
          and sk.REVIVE_AFTER_S == 150.0
          and aw.PORT_GRACE_S <= aw.FLEET_GRACE_S
          and sk.REVIVE_AFTER_S == aw.FLEET_GRACE_S,
          "production port, fleet, and revival grace defaults are 150s "
          "and coupled")


def test_signal_taxonomy():
    check(aw.hard_signal_kind(ConnectionRefusedError()) == "connection_refused"
          and aw.hard_signal_kind(ConnectionResetError()) == "socket_reset"
          and aw.hard_signal_kind(ConnectionLost()) == "connection_lost",
          "hard classifier accepts only explicit connection-layer failures")
    check(aw.hard_signal_kind(RequestTimeout("slow")) is None
          and aw.hard_signal_kind(PacingViolation("paced")) is None
          and aw.hard_signal_kind(TimeoutError("slow")) is None
          and aw.hard_signal_kind(ConnectionError("generic")) is None,
          "timeouts, pacing, and generic connection prose never signal death")

    seen = []

    class Adapter:
        def soft(self):
            raise RequestTimeout("slow")

        def hard(self):
            raise ConnectionLost("dropped")

    observed = aw.HardSignalAdapter(
        Adapter(), lambda kind, _exc: seen.append(kind))
    for name in ("soft", "hard"):
        try:
            getattr(observed, name)()
        except ConnectionError:
            pass
    check(seen == ["connection_lost"],
          "adapter observation preserves failures and emits hard signals only")


def _watchdog(clock, maintenance=None):
    return aw.HardDeathWatchdog(
        [2000, 3000], clock=clock,
        maintenance_provider=(lambda: maintenance[0]) if maintenance else None,
        port_grace_s=10, fleet_grace_s=20, probe_backoff_s=(2, 4, 8))


def test_state_machine():
    clock = Clock()
    watchdog = _watchdog(clock)
    initial_sequence = watchdog.interrupt_sequence(2000)
    check(watchdog.hard_signal(2000, RequestTimeout("slow")) is False
          and watchdog.state(2000) == aw.HEALTHY,
          "soft signal causes zero watchdog transition")
    check(watchdog.hard_signal(2000, ConnectionLost("drop"))
          and watchdog.state(2000) == aw.SUSPECT
          and not watchdog.holding()
          and watchdog.interrupt_event(2000).is_set()
          and watchdog.interrupt_sequence(2000) == initial_sequence + 1,
          "first hard signal makes one port suspect with durable sequence")
    clock.advance(2)
    check(watchdog.due_probes() == [2000],
          "suspect port follows the first named probe backoff")
    watchdog.probe_result(2000, False)
    clock.advance(8)
    check(watchdog.state(2000) == aw.DEAD and not watchdog.should_finalize(),
          "failed probes exhaust per-port grace without finalizing a live fleet")
    signalled_sequence = watchdog.interrupt_sequence(2000)
    watchdog.probe_result(2000, True)
    check(watchdog.state(2000) == aw.HEALTHY
          and not watchdog.interrupt_event(2000).is_set()
          and watchdog.interrupt_sequence(2000) == signalled_sequence,
          "probe recovery clears the event but preserves interrupt history")

    watchdog.hard_signal(2000, "connection_lost")
    watchdog.hard_signal(3000, "socket_reset")
    check(watchdog.holding() and not watchdog.should_finalize(),
          "zero healthy ports enters HOLD before fleet grace")
    clock.advance(5)
    watchdog.probe_result(3000, True)
    check(not watchdog.holding() and not watchdog.should_finalize(),
          "first recovered port resumes a short all-port blip")
    watchdog.hard_signal(3000, "connection_lost")
    clock.advance(21)
    check(watchdog.should_finalize(),
          "zero healthy ports beyond fleet grace requests degraded finalize")


def test_maintenance_accounting():
    clock = Clock()
    maintenance = [aw.MaintenanceStatus(
        True, "valid", (2000, 3000), NOW + timedelta(minutes=10))]
    watchdog = _watchdog(clock, maintenance)
    watchdog.hard_signal(2000, "connection_refused")
    watchdog.hard_signal(3000, "connection_refused")
    clock.advance(100)
    snapshot = watchdog.snapshot()
    check(snapshot["holding"] and snapshot["maintenance_active"]
          and snapshot["fleet_grace_seconds"] == 0
          and all(row["state"] == aw.SUSPECT
                  for row in snapshot["ports"].values()),
          "valid declared maintenance pauses port and fleet grace")
    maintenance[0] = aw.MaintenanceStatus(
        False, "dead_owner", (2000, 3000))
    clock.advance(21)
    check(watchdog.should_finalize()
          and all(row["state"] == aw.DEAD
                  for row in watchdog.snapshot()["ports"].values()),
          "dead-owner maintenance is void and normal grace resumes")


def test_maintenance_file(base):
    root = base / "maintenance"
    fleet = root / "fleet.json"
    identity = lambda pid: {"pid": int(pid), "creation_filetime": 123456}
    first = aw.begin_maintenance(
        fleet, [2000], 30, now=NOW, identity_fn=identity)
    status = aw.maintenance_status(
        first.path, now=NOW + timedelta(seconds=10), identity_fn=identity,
        margin_s=0)
    check(first.published and status.valid and status.ports == (2000,),
          "maintenance lease publishes beside fleet.json with exact identity")
    second = aw.begin_maintenance(
        fleet, [3000], 60, now=NOW + timedelta(seconds=1),
        identity_fn=identity)
    merged = aw.load_maintenance(second.path)
    check(second.published and merged["ports"] == [2000, 3000]
          and not aw.end_maintenance(first),
          "nested lease merges ports and an older lease cannot clear a newer one")
    check(aw.end_maintenance(second)
          and aw.load_maintenance(first.path)["lease_id"] == first.lease_id
          and aw.end_maintenance(first) and not first.path.exists(),
          "nested lease restores then clears its exact predecessor")

    lease = aw.begin_maintenance(
        fleet, [2000], 5, now=NOW, identity_fn=identity)
    dead = aw.maintenance_status(
        lease.path, now=NOW, identity_fn=lambda _pid: (_ for _ in ()).throw(
            OSError("gone")))
    expired = aw.maintenance_status(
        lease.path, now=NOW + timedelta(seconds=126), identity_fn=identity)
    check(not dead.valid and dead.reason == "dead_owner"
          and not expired.valid and expired.reason == "expired",
          "dead owner and exhausted budget plus margin invalidate the flag")
    aw.end_maintenance(lease)
    lease.path.parent.mkdir(parents=True, exist_ok=True)
    lease.path.write_text("{torn", encoding="ascii")
    invalid = aw.maintenance_status(lease.path, now=NOW, identity_fn=identity)
    check(not invalid.valid and "JSON" in invalid.reason,
          "torn maintenance state fails closed")


def _call_name(node):
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def test_integration_wiring():
    source = Path(__file__).resolve().parents[1] / "display_data.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    methods = {node.name: node for node in ast.walk(tree)
               if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    for name in ("_safe_restart_ports", "_storage_multiport_start",
                 "_storage_restart_dead_start"):
        calls = {_call_name(node.func) for node in ast.walk(methods[name])
                 if isinstance(node, ast.Call)}
        check("addstock_watchdog.maintenance_window" in calls,
              f"{name} declares restart maintenance")

    start = methods["_storage_find_start_fill"]
    parallel = next(
        node for node in ast.walk(start) if isinstance(node, ast.Call)
        and _call_name(node.func) == "stock_ibkr.add_stocks_gap_fill_parallel_resilient")
    keywords = {item.arg: item.value for item in parallel.keywords}
    check(isinstance(keywords.get("hard_death_watchdog"), ast.Constant)
          and keywords["hard_death_watchdog"].value is True
          and "maintenance_path" in keywords,
          "parallel Add Stocks enables watchdog with declared-maintenance path")
    start_calls = {_call_name(node.func) for node in ast.walk(start)
                   if isinstance(node, ast.Call)}
    check("self._finalize_drain_and_seal" in start_calls
          and "addstock_run_manifest.replace_seal_pending" in start_calls,
          "Resume runs the existing sealer and clears only freshly scanned debt")
    done_source = ast.unparse(methods["_storage_find_done"])
    render_source = ast.unparse(methods["_find_render_ports"])
    check("fleet_down_finalize" in done_source
          and all(token in render_source
                  for token in ("suspect", "dead", "probing")),
          "Add Stocks reports degraded finalize and explicit watchdog states")

    watchdog_source = Path(aw.__file__).read_text(encoding="utf-8")
    check(all(token not in watchdog_source for token in (
        "subprocess.", "terminate(", "kill(", "relaunch_one", "restart_dead")),
        "watchdog core has no process lifecycle authority")


def test_port_free_drain():
    checked = []
    probed = []
    coordinator = live_spot_probe.AddStockProbeCoordinator(
        [("AAA", "1m")],
        check_fn=lambda *_args: checked.append("AAA") or {
            "selection": {"day": "2026-07-15"}},
        probe_fn=lambda *_args: probed.append("AAA") or {}, seed=1)
    coordinator.record_series([{"ticker": "AAA", "interval": "1m"}])
    count = coordinator.drain_checks()
    check(count == 1 and checked == ["AAA"] and not probed
          and coordinator.has_pending(),
          "degraded drain finishes checks while leaving live probe debt pending")


def test_parallel_engine_boundaries(base):
    selections = [("AAA", "1m"), ("BBB", "1m"),
                  ("CCC", "1m"), ("DDD", "1m")]
    original = sk.gap_fill

    def run_case(name, fail_once, port_up, fleet_grace=2.0):
        attempts = {2000: 0, 3000: 0}

        class Adapter:
            def __init__(self, port):
                self.port = port

            def is_connected(self):
                return True

            def disconnect(self):
                return None

            def account(self):
                return f"DU{self.port}"

            def fetch(self):
                attempts[self.port] += 1
                if fail_once(self.port, attempts[self.port]):
                    raise sk.ConnectionLost(f"injected drop on {self.port}")

        def factory(_host, ports):
            return lambda: Adapter(int(ports[0]))

        def fake_gap(root, series, progress, cancel, adapter_factory, _pacer,
                     *_args, **kwargs):
            adapter = adapter_factory()
            adapter.fetch()
            rows = []
            for index, (ticker, interval) in enumerate(series, 1):
                if progress is not None:
                    progress(f"[{index}/{len(series)}] {ticker} {interval}")
                row = {"ticker": ticker, "interval": interval, "added": 1}
                rows.append(row)
                callback = kwargs.get("on_series")
                if callback is not None:
                    callback(ticker, interval, row)
            return {
                "run": f"fake-{name}-{adapter.port}", "root": str(root),
                "account": adapter.account(), "port": adapter.port,
                "series": rows, "cancelled": False,
                "totals": {"added": len(rows), "dup_existing": 0,
                           "conflicts": 0, "written": len(rows),
                           "requests": len(rows), "bars_fetched": len(rows),
                           "halted_series": 0, "write_failed": 0},
                "spot_checks_run": 0,
            }

        def watchdog_factory(ports, maintenance_provider=None):
            return aw.HardDeathWatchdog(
                ports, maintenance_provider=maintenance_provider,
                port_grace_s=0.05, fleet_grace_s=fleet_grace,
                probe_backoff_s=(0.01, 0.02))

        sk.gap_fill = sk.fib.worker_scope(fake_gap)
        try:
            return sk.nightly_gap_fill_parallel.__wrapped__(
                base / name, selections, [2000, 3000],
                adapter_factory=factory, port_up=port_up,
                hard_death_watchdog=True,
                watchdog_factory=watchdog_factory)
        finally:
            sk.gap_fill = original

    rerouted = run_case(
        "single-reroute", lambda port, attempt: port == 2000 and attempt == 1,
        lambda port: port == 3000)
    check(not rerouted.get("fleet_down_finalize")
          and not rerouted.get("aborted_ports")
          and rerouted["watchdog"]["rerouted_ports"] == [2000]
          and {(row["ticker"], row["interval"])
               for row in rerouted["series"]} == set(selections),
          "parallel engine immediately reroutes one hard-failed ticker job")

    blip = run_case(
        "fleet-blip", lambda _port, attempt: attempt == 1,
        lambda _port: True)
    check(not blip.get("fleet_down_finalize")
          and not blip.get("aborted_ports")
          and blip["watchdog"]["rerouted_ports"] == [2000, 3000]
          and {(row["ticker"], row["interval"])
               for row in blip["series"]} == set(selections),
          "parallel engine HOLDs then re-adopts ports after an all-port blip")

    dead = run_case(
        "fleet-dead", lambda _port, _attempt: True,
        lambda _port: False, fleet_grace=0.05)
    check(dead.get("fleet_down_finalize") is True
          and dead.get("interrupted") is True
          and dead.get("seal_pending")
          == ["AAA 1m", "BBB 1m", "CCC 1m", "DDD 1m"]
          and dead.get("_unpulled"),
          "parallel engine returns explicit degraded-finalize debt after grace")


def test_recovery_teardown_race(base):
    selections = [(f"RACE{index}", "1m") for index in range(6)]
    original = sk.gap_fill
    attempts = {2000: 0, 3000: 0}
    signal_seen = threading.Event()
    first_probe = threading.Event()
    close_started = threading.Event()
    allow_close = threading.Event()
    deferred_close = threading.Event()
    readopt_started = threading.Event()
    probe_results = []

    class Adapter:
        def __init__(self, port):
            self.port = port

        def is_connected(self):
            return True

        def disconnect(self):
            if self.port == 2000 and attempts[self.port] == 1:
                close_started.set()
                allow_close.wait(3.0)

        def account(self):
            return f"DU{self.port}"

        def fetch(self):
            attempts[self.port] += 1
            if self.port == 2000 and attempts[self.port] == 1:
                raise sk.ConnectionLost("injected delayed-teardown drop")
            if self.port == 2000:
                readopt_started.set()

    def factory(_host, ports):
        return lambda: Adapter(int(ports[0]))

    def report(name, adapter, rows, *, cancelled=False):
        return {
            "run": f"fake-{name}-{adapter.port}", "root": str(base),
            "account": adapter.account(), "port": adapter.port,
            "series": rows, "cancelled": cancelled,
            "totals": {"added": len(rows), "dup_existing": 0,
                       "conflicts": 0, "written": len(rows),
                       "requests": len(rows), "bars_fetched": len(rows),
                       "halted_series": 0, "write_failed": 0},
            "spot_checks_run": 0,
        }

    def fake_gap(_root, series, progress, _cancel, adapter_factory, _pacer,
                 *_args, **kwargs):
        adapter = adapter_factory()
        try:
            adapter.fetch()
        except sk.ConnectionLost:
            signal_seen.set()
            if not first_probe.wait(3.0):
                raise AssertionError("watchdog did not probe active worker")
            # Model gap_fill observing its composite cancel after the monitor
            # has already seen the socket listening again.
            return report("race-cancel", adapter, [], cancelled=True)
        if adapter.port == 3000 and attempts[3000] == 1:
            readopt_started.wait(3.0)
        rows = []
        for index, (ticker, interval) in enumerate(series, 1):
            if progress is not None:
                progress(f"[{index}/{len(series)}] {ticker} {interval}")
            row = {"ticker": ticker, "interval": interval, "added": 1}
            rows.append(row)
            callback = kwargs.get("on_series")
            if callback is not None:
                callback(ticker, interval, row)
        return report("race-ok", adapter, rows)

    class TrackingWatchdog(aw.HardDeathWatchdog):
        def probe_result(self, port, up, *, now=None):
            result = super().probe_result(port, up, now=now)
            if int(port) == 2000:
                probe_results.append(bool(up))
                if close_started.is_set() and not up:
                    deferred_close.set()
                    allow_close.set()
            return result

    def watchdog_factory(ports, maintenance_provider=None):
        return TrackingWatchdog(
            ports, maintenance_provider=maintenance_provider,
            port_grace_s=0.05, fleet_grace_s=5.0,
            probe_backoff_s=(0.01, 0.02))

    def port_up(port):
        if int(port) == 2000 and signal_seen.is_set():
            first_probe.set()
            return True
        return True

    # Keep a regressed implementation bounded instead of leaving its old worker
    # blocked forever after it incorrectly marks the port healthy.
    fallback = threading.Timer(3.0, allow_close.set)
    fallback.daemon = True
    fallback.start()
    sk.gap_fill = sk.fib.worker_scope(fake_gap)
    try:
        result = sk.nightly_gap_fill_parallel.__wrapped__(
            base / "recovery-race", selections, [2000, 3000],
            adapter_factory=factory, port_up=port_up,
            hard_death_watchdog=True,
            watchdog_factory=watchdog_factory)
    finally:
        sk.gap_fill = original
        allow_close.set()
        fallback.cancel()

    check(first_probe.is_set() and deferred_close.is_set()
          and probe_results and probe_results[0] is False
          and any(probe_results),
          "healthy probe stays deferred until old-worker teardown unregisters")
    check(readopt_started.is_set()
          and attempts[2000] >= 2
          and result["watchdog"]["rerouted_ports"] == [2000]
          and not result.get("cancelled")
          and {(row["ticker"], row["interval"])
               for row in result["series"]} == set(selections),
          "cleared event cannot erase hard-interrupt requeue evidence")


def test_post_pipeline_wait_status(base):
    """Real parallel worker loop, fake fill/adapter, blocked WS8 probe."""
    probe_started = threading.Event()
    probe_release = threading.Event()
    status_lock = threading.Lock()
    statuses = []
    calls = []
    holder = {}

    class Adapter:
        def __init__(self, port):
            self.port = int(port)

        def account(self):
            return f"DU{self.port}"

        def is_connected(self):
            return True

        def disconnect(self):
            return None

    def factory(_host, ports):
        return lambda: Adapter(ports[0])

    def fake_gap(root, series, progress, _cancel, adapter_factory, _pacer,
                 *_args, **kwargs):
        adapter = adapter_factory()
        rows = []
        for index, (ticker, interval) in enumerate(series, 1):
            if progress is not None:
                progress(f"[{index}/{len(series)}] {ticker} {interval}")
            row = {"ticker": ticker, "interval": interval, "added": 1}
            rows.append(row)
            callback = kwargs.get("on_series")
            if callback is not None:
                callback(ticker, interval, row)
        return {
            "run": "post-status-fill", "root": str(root),
            "account": adapter.account(), "port": adapter.port,
            "series": rows, "cancelled": False,
            "totals": {"added": len(rows), "dup_existing": 0,
                       "conflicts": 0, "written": len(rows),
                       "requests": 0, "bars_fetched": 0,
                       "halted_series": 0, "write_failed": 0},
            "spot_checks_run": 0,
        }

    def check_fn(adapter, pacer, ticker, rows, seed):
        calls.append(("check", ticker, adapter.port, id(pacer), len(rows), seed))
        return {"selection": {"day": "2026-07-21"}}

    def probe_fn(adapter, pacer, ticker, selection):
        calls.append(("probe", ticker, adapter.port, id(pacer),
                      selection["day"]))
        probe_started.set()
        probe_release.wait(4)
        return {"ticker": ticker, "day": selection["day"],
                "verdict": "MATCH", "pass_equivalent": True,
                "request_count": 1, "reused_request_count": 0,
                "network": True, "written": False}

    coordinator = live_spot_probe.AddStockProbeCoordinator(
        [("WAIT", "1m")], check_fn=check_fn, probe_fn=probe_fn, seed=45)

    def port_status(port, info):
        with status_lock:
            statuses.append((time.monotonic(), int(port), dict(info)))

    def run_body():
        holder["result"] = sk.nightly_gap_fill_parallel.__wrapped__(
            base / "post-status", [("WAIT", "1m")], [2000, 3000],
            adapter_factory=factory, post_pipeline=coordinator,
            port_status=port_status)

    original_gap = sk.gap_fill
    original_interval = fix_data_pipeline.PROBE_WAIT_HEARTBEAT_S
    sk.gap_fill = sk.fib.worker_scope(fake_gap)
    fix_data_pipeline.PROBE_WAIT_HEARTBEAT_S = 0.05
    worker = threading.Thread(target=run_body)
    reached = False
    stall = []
    try:
        worker.start()
        reached = probe_started.wait(4)
        deadline = time.monotonic() + 2
        while reached and time.monotonic() < deadline:
            with status_lock:
                snapshot = list(statuses)
            labels = [str(info.get("detail") or "")
                      for _at, _port, info in snapshot]
            if (any(label.startswith("spot-probe WAIT (0/1)")
                    for label in labels)
                    and any(label.startswith("waiting:")
                            and "0/1" in label for label in labels)):
                break
            time.sleep(0.01)
        with status_lock:
            mark = len(statuses)
        time.sleep(1.1)
        with status_lock:
            stall = list(statuses[mark:])
    finally:
        probe_release.set()
        worker.join(6)
        sk.gap_fill = original_gap
        fix_data_pipeline.PROBE_WAIT_HEARTBEAT_S = original_interval

    labels = [str(info.get("detail") or "")
              for _at, _port, info in stall]
    active = [label for label in labels
              if label.startswith("spot-probe WAIT")]
    waiting = [label for label in labels if label.startswith("waiting:")]
    check(reached and active and waiting
          and all(info.get("state") == "working"
                  for _at, _port, info in stall if info.get("detail")),
          "post pipeline uses visible detail while worker state stays working")
    check(any("(0/1)" in label for label in active)
          and any("0/1" in label for label in waiting)
          and any("· 1s" in label for label in active + waiting),
          "post pipeline active/waiting labels carry shared k/N and elapsed")
    check(len(active) >= 3 and len(stall) < 90,
          "shared monitor heartbeats active probes without a 20ms event storm")

    with status_lock:
        settled_count = len(statuses)
        final_by_port = {}
        for _at, port, info in statuses:
            final_by_port[port] = info
    time.sleep(0.15)
    with status_lock:
        after_settle_count = len(statuses)
    snapshot = coordinator.status_snapshot()
    check(not worker.is_alive()
          and [row[0] for row in calls] == ["check", "probe"]
          and snapshot["probe_done"] == snapshot["probe_total"] == 1
          and set(final_by_port) == {2000, 3000}
          and all(info.get("state") == "done"
                  and not info.get("detail")
                  for info in final_by_port.values())
          and after_settle_count == settled_count
          and isinstance(holder.get("result"), dict),
          "post pipeline drains once, clears labels, and stops heartbeats")


def test_post_pipeline_publication_order(base):
    """A delayed heartbeat callback cannot land after terminal status."""
    probe_started = threading.Event()
    probe_release = threading.Event()
    heartbeat_delayed = threading.Event()
    heartbeat_release = threading.Event()
    terminal_seen = threading.Event()
    status_lock = threading.Lock()
    statuses = []
    active_callbacks = [0]
    holder = {}

    class Adapter:
        def __init__(self, port):
            self.port = int(port)

        def account(self):
            return f"DU{self.port}"

        def is_connected(self):
            return True

        def disconnect(self):
            return None

    def factory(_host, ports):
        return lambda: Adapter(ports[0])

    def fake_gap(root, series, progress, _cancel, adapter_factory, _pacer,
                 *_args, **kwargs):
        adapter = adapter_factory()
        rows = []
        for index, (ticker, interval) in enumerate(series, 1):
            if progress is not None:
                progress(f"[{index}/{len(series)}] {ticker} {interval}")
            row = {"ticker": ticker, "interval": interval, "added": 1}
            rows.append(row)
            callback = kwargs.get("on_series")
            if callback is not None:
                callback(ticker, interval, row)
        return {
            "run": "post-order-fill", "root": str(root),
            "account": adapter.account(), "port": adapter.port,
            "series": rows, "cancelled": False,
            "totals": {"added": len(rows), "dup_existing": 0,
                       "conflicts": 0, "written": len(rows),
                       "requests": 0, "bars_fetched": 0,
                       "halted_series": 0, "write_failed": 0},
            "spot_checks_run": 0,
        }

    def check_fn(_adapter, _pacer, _ticker, _rows, _seed):
        return {"selection": {"day": "2026-07-21"}}

    def probe_fn(_adapter, _pacer, ticker, selection):
        probe_started.set()
        probe_release.wait(4)
        return {"ticker": ticker, "day": selection["day"],
                "verdict": "MATCH", "pass_equivalent": True,
                "request_count": 1, "reused_request_count": 0,
                "network": True, "written": False}

    coordinator = live_spot_probe.AddStockProbeCoordinator(
        [("ORDER", "1m")], check_fn=check_fn, probe_fn=probe_fn,
        seed=46)

    def port_status(port, info):
        delay = False
        detail = str(info.get("detail") or "")
        if detail.startswith("spot-probe ORDER"):
            with status_lock:
                active_callbacks[0] += 1
                delay = active_callbacks[0] == 2
            if delay:
                heartbeat_delayed.set()
                heartbeat_release.wait(4)
        with status_lock:
            statuses.append((int(port), dict(info)))
        if info.get("state") == "done":
            terminal_seen.set()

    def run_body():
        holder["result"] = sk.nightly_gap_fill_parallel.__wrapped__(
            base / "post-order", [("ORDER", "1m")], [2000],
            adapter_factory=factory, post_pipeline=coordinator,
            port_status=port_status)

    original_gap = sk.gap_fill
    original_interval = fix_data_pipeline.PROBE_WAIT_HEARTBEAT_S
    sk.gap_fill = sk.fib.worker_scope(fake_gap)
    fix_data_pipeline.PROBE_WAIT_HEARTBEAT_S = 0.05
    worker = threading.Thread(target=run_body)
    reached_probe = delayed = terminal_crossed = False
    try:
        worker.start()
        reached_probe = probe_started.wait(4)
        delayed = heartbeat_delayed.wait(4)
        probe_release.set()
        # With serialized publication the terminal emitter is queued behind
        # the delayed callback.  Without it, terminal crosses first and the
        # old heartbeat becomes the last delivered row when released.
        terminal_crossed = terminal_seen.wait(0.25)
    finally:
        heartbeat_release.set()
        probe_release.set()
        worker.join(6)
        sk.gap_fill = original_gap
        fix_data_pipeline.PROBE_WAIT_HEARTBEAT_S = original_interval

    with status_lock:
        snapshot = list(statuses)
    done_indexes = [index for index, (_port, info) in enumerate(snapshot)
                    if info.get("state") == "done"]
    terminal_tail = (snapshot[done_indexes[0]:] if done_indexes else [])
    check(reached_probe and delayed and not terminal_crossed
          and active_callbacks[0] >= 2,
          "post status publication serializes a delayed active heartbeat")
    check(not worker.is_alive() and done_indexes
          and all(info.get("state") == "done" and not info.get("detail")
                  for _port, info in terminal_tail)
          and snapshot[-1][1].get("state") == "done"
          and isinstance(holder.get("result"), dict),
          "no active post status can appear after the first terminal row")


def test_post_pipeline_counter_revision(base):
    """Neither heartbeat nor worker snapshots may regress same-key k/N."""
    fill_started = {ticker: threading.Event()
                    for ticker in ("BBB", "CCC")}
    fill_release = {ticker: threading.Event()
                    for ticker in ("BBB", "CCC")}
    probe_a_started = threading.Event()
    probe_a_release = threading.Event()
    arm_heartbeat_snapshot = threading.Event()
    heartbeat_snapshot_blocked = threading.Event()
    heartbeat_snapshot_release = threading.Event()
    arm_worker_snapshot = threading.Event()
    worker_snapshot_blocked = threading.Event()
    worker_snapshot_release = threading.Event()
    status_lock = threading.Lock()
    control_lock = threading.Lock()
    statuses = []
    runner_ident = [None]
    target_worker_name = [None]
    heartbeat_blocked_once = [False]
    worker_blocked_once = [False]
    holder = {}

    class Adapter:
        def __init__(self, port):
            self.port = int(port)

        def account(self):
            return f"DU{self.port}"

        def is_connected(self):
            return True

        def disconnect(self):
            return None

    def factory(_host, ports):
        return lambda: Adapter(ports[0])

    def fake_gap(root, series, progress, _cancel, adapter_factory, _pacer,
                 *_args, **kwargs):
        adapter = adapter_factory()
        rows = []
        for index, (ticker, interval) in enumerate(series, 1):
            if ticker in fill_started:
                fill_started[ticker].set()
                fill_release[ticker].wait(6)
            if progress is not None:
                progress(f"[{index}/{len(series)}] {ticker} {interval}")
            row = {"ticker": ticker, "interval": interval, "added": 1}
            rows.append(row)
            callback = kwargs.get("on_series")
            if callback is not None:
                callback(ticker, interval, row)
        return {
            "run": "post-counter-fill", "root": str(root),
            "account": adapter.account(), "port": adapter.port,
            "series": rows, "cancelled": False,
            "totals": {"added": len(rows), "dup_existing": 0,
                       "conflicts": 0, "written": len(rows),
                       "requests": 0, "bars_fetched": 0,
                       "halted_series": 0, "write_failed": 0},
            "spot_checks_run": 0,
        }

    def check_fn(_adapter, _pacer, _ticker, _rows, _seed):
        return {"selection": {"day": "2026-07-21"}}

    def probe_fn(_adapter, _pacer, ticker, selection):
        if ticker == "AAA":
            probe_a_started.set()
            probe_a_release.wait(5)
        return {"ticker": ticker, "day": selection["day"],
                "verdict": "MATCH", "pass_equivalent": True,
                "request_count": 1, "reused_request_count": 0,
                "network": True, "written": False}

    coordinator = live_spot_probe.AddStockProbeCoordinator(
        [("AAA", "1m"), ("BBB", "1m"), ("CCC", "1m")],
        check_fn=check_fn, probe_fn=probe_fn, seed=47)
    real_status_snapshot = coordinator.status_snapshot

    def controlled_status_snapshot():
        snapshot = real_status_snapshot()
        delay_kind = None
        if (arm_heartbeat_snapshot.is_set()
                and threading.get_ident() == runner_ident[0]):
            with control_lock:
                if not heartbeat_blocked_once[0]:
                    heartbeat_blocked_once[0] = True
                    delay_kind = "heartbeat"
        elif (arm_worker_snapshot.is_set()
              and threading.current_thread().name == target_worker_name[0]):
            with control_lock:
                if not worker_blocked_once[0]:
                    worker_blocked_once[0] = True
                    delay_kind = "worker"
        if delay_kind == "heartbeat":
            heartbeat_snapshot_blocked.set()
            heartbeat_snapshot_release.wait(6)
        elif delay_kind == "worker":
            worker_snapshot_blocked.set()
            worker_snapshot_release.wait(6)
        return snapshot

    coordinator.status_snapshot = controlled_status_snapshot

    def port_status(port, info):
        with status_lock:
            statuses.append((int(port), dict(info)))

    def run_body():
        runner_ident[0] = threading.get_ident()
        holder["result"] = sk.nightly_gap_fill_parallel.__wrapped__(
            base / "post-counter",
            [("AAA", "1m"), ("BBB", "1m"), ("CCC", "1m")],
            [2000, 3000, 4000, 5000], adapter_factory=factory,
            post_pipeline=coordinator, port_status=port_status)

    original_gap = sk.gap_fill
    original_interval = fix_data_pipeline.PROBE_WAIT_HEARTBEAT_S
    sk.gap_fill = sk.fib.worker_scope(fake_gap)
    fix_data_pipeline.PROBE_WAIT_HEARTBEAT_S = 0.05
    worker = threading.Thread(target=run_body)
    setup_ready = heartbeat_blocked = fresh_port = None
    one_index = two_index = None
    heartbeat_regressed = worker_regressed = True
    worker_blocked = False
    try:
        worker.start()
        setup_ready = (all(event.wait(4) for event in fill_started.values())
                       and probe_a_started.wait(4))
        deadline = time.monotonic() + 3
        waiting_ports = set()
        while setup_ready and time.monotonic() < deadline:
            with status_lock:
                snapshot = list(statuses)
            active_zero = any(
                str(info.get("detail") or "").startswith(
                    "spot-probe AAA (0/3)")
                for _port, info in snapshot)
            waiting_ports = {
                port for port, info in snapshot
                if str(info.get("detail") or "").startswith("waiting:")
                and "probes (0/3)" in str(info.get("detail") or "")
            }
            if active_zero and waiting_ports:
                break
            time.sleep(0.01)
        setup_ready = bool(setup_ready and waiting_ports)
        arm_heartbeat_snapshot.set()
        heartbeat_blocked = heartbeat_snapshot_blocked.wait(4)
        probe_a_release.set()
        deadline = time.monotonic() + 3
        while heartbeat_blocked and time.monotonic() < deadline:
            with status_lock:
                snapshot = list(statuses)
            for index, (port, info) in enumerate(snapshot):
                detail = str(info.get("detail") or "")
                if (port in waiting_ports and detail.startswith("waiting:")
                        and "probes (1/3)" in detail):
                    fresh_port = port
                    one_index = index
                    break
            if fresh_port is not None:
                break
            time.sleep(0.01)
        heartbeat_snapshot_release.set()
        time.sleep(0.15)
        with status_lock:
            snapshot = list(statuses)
        if fresh_port is not None:
            heartbeat_regressed = any(
                port == fresh_port
                and str(info.get("detail") or "").startswith("waiting:")
                and "probes (0/3)" in str(info.get("detail") or "")
                for port, info in snapshot[one_index + 1:])

        target_worker_name[0] = f"ibkr-fetch-{fresh_port}"
        arm_worker_snapshot.set()
        worker_blocked = worker_snapshot_blocked.wait(4)
        fill_release["BBB"].set()
        deadline = time.monotonic() + 3
        while worker_blocked and time.monotonic() < deadline:
            with status_lock:
                snapshot = list(statuses)
            for index, (port, info) in enumerate(snapshot):
                detail = str(info.get("detail") or "")
                if (port == fresh_port and detail.startswith("waiting:")
                        and "probes (2/3)" in detail):
                    two_index = index
                    break
            if two_index is not None:
                break
            time.sleep(0.01)
        worker_snapshot_release.set()
        time.sleep(0.15)
        with status_lock:
            snapshot = list(statuses)
        if two_index is not None:
            worker_regressed = any(
                port == fresh_port
                and str(info.get("detail") or "").startswith("waiting:")
                and "probes (1/3)" in str(info.get("detail") or "")
                for port, info in snapshot[two_index + 1:])
    finally:
        heartbeat_snapshot_release.set()
        worker_snapshot_release.set()
        probe_a_release.set()
        for event in fill_release.values():
            event.set()
        worker.join(10)
        sk.gap_fill = original_gap
        fix_data_pipeline.PROBE_WAIT_HEARTBEAT_S = original_interval

    check(setup_ready and heartbeat_blocked and fresh_port is not None
          and one_index is not None and not heartbeat_regressed,
          "heartbeat revision rejects an old 0/3 after waiting 1/3")
    check(worker_blocked and two_index is not None and not worker_regressed,
          "worker revision retry rejects an old 1/3 after waiting 2/3")
    check(not worker.is_alive()
          and coordinator.status_snapshot()["probe_done"] == 3
          and coordinator.status_snapshot()["probe_total"] == 3
          and isinstance(holder.get("result"), dict),
          "counter race fixture drains with stable full-run progress")


def main():
    base = Path(tempfile.mkdtemp(prefix="addstock_watchdog_selftest_"))
    try:
        test_production_grace_defaults()
        test_signal_taxonomy()
        test_state_machine()
        test_maintenance_accounting()
        test_maintenance_file(base)
        test_integration_wiring()
        test_port_free_drain()
        test_parallel_engine_boundaries(base)
        test_recovery_teardown_race(base)
        test_post_pipeline_wait_status(base)
        test_post_pipeline_publication_order(base)
        test_post_pipeline_counter_revision(base)
    finally:
        shutil.rmtree(base, ignore_errors=True)
    total = PASS[0] + FAIL[0]
    print(f"addstock_watchdog_selftest: {PASS[0]}/{total} passed, "
          f"{FAIL[0]} failed")
    return 1 if FAIL[0] else 0


if __name__ == "__main__":
    raise SystemExit(main())
