"""S6 offline Update Data fences; shipped source, single-anchor memory mutants.

Containment: S6X/S6Y only; early network/process denial; fake gate and ib_async
remain seeded during lazy imports. LiveIB._loop is a counted fixture no-op.
Integration seams are replicated, never imported. No host source is written.
Every opened A1 ledger is sealed/verified, including expected failure drives.
Timeouts, containment and ledger failures are harness failures, never mutant kills.
"""
from __future__ import annotations

import ast
from collections import Counter
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import hashlib
import importlib
from pathlib import Path
import sys
import tempfile
import threading
import types
from unittest.mock import patch

ENGINE_ROOT = Path(__file__).resolve().parent
if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))
from check_kit import CheckKit
from source_segment_custody import (
    function_segment_sha256, function_spans, mutation_owner, normalize_source_bytes,
)
# Helpers only: this module has no subject imports at module load.
from fixdata_injected_fence_reference import EarlyTripwire, seeded_modules, execute_module

TEST_ONLY = True
TARGET = ENGINE_ROOT / "stock_ibkr.py"
SYNTHETIC_TICKERS = frozenset({"S6X", "S6Y"})
FORBIDDEN_TICKERS = frozenset({"APO", "JCI", "OKE", "TKO", "WBD", "TMUS"})
CONSTANTS = {
    "CLIENT_ID_FETCH": 7311, "CLIENT_ID_STRIDE": 10, "CLIENT_ID_RETRIES": 8,
    "CONNECT_TIMEOUT_S": 4, "ROTATE_CONNECT_TIMEOUT_S": 3,
    "QUALIFY_TIMEOUT_S": 20.0, "HEAD_PROBE_DURATION": "30 Y",
    "HEAD_PROBE_FLOOR_TOLERANCE_DAYS": 14, "NO_HEAD_FALLBACK_DAYS": 1825,
    "TIMEOUT_RETRIES": 2, "JOIN_RATIO_LOW": 0.55, "JOIN_RATIO_HIGH": 1.9,
}
CONTEXT_ONLY_SEGMENTS = frozenset({
    "LiveIB.__init__", "LiveIB.connect", "_fetch_sessions", "_fill_series",
    "_make_session_processor",
})
EXPECTED_FUNCTION_SEGMENT_SHA256 = {
    "<module>:CLIENT_ID_FETCH": "17a2bf4bc25662ad42e048f4d14aaadcabfe5e49d55d386ebde41b6702092542",
    "<module>:CLIENT_ID_RETRIES": "acd7ce0a1d6e829c81c8f815c118ebefdcffa43d610c8bee896eed77a298a122",
    "<module>:CLIENT_ID_STRIDE": "7289ca6cc3fabf3647600e05d2b8a4fd3cc4f3f92b1485e4ce8a2808207fe668",
    "<module>:CONNECT_TIMEOUT_S": "d0e6c04e7213a103c7aec7be324a6b7fd93e113c1da230ca20b0344e7c727241",
    "<module>:HEAD_PROBE_DURATION": "4ebd90fb60ef9e86fe1a4971b8d1f041f92fea196d996ead7f22121d383d1f4e",
    "<module>:HEAD_PROBE_FLOOR_TOLERANCE_DAYS": "4e4904a4e53179c953f65d9455a43b469091c4707067991cf97fa1f566ed3faa",
    "<module>:JOIN_RATIO_HIGH": "5d7d507bda8ac7a22d17f98b2dbce063163a441733d2c3f69cc5fbbca537aaec",
    "<module>:JOIN_RATIO_LOW": "4ce84a5966618a9aedb92f2bb033a17ec23f9d3f166070e3d937a59f4e18c9d2",
    "<module>:NO_HEAD_FALLBACK_DAYS": "836e0a4b8d8ba35235025c1bdbf361f073f84aec1cfa94f4d712aaf072c8a5f2",
    "<module>:QUALIFY_TIMEOUT_S": "5666266542c0aacb7b87616abe4286e661fe916576fa4a98fb54e1fc36f4bcb1",
    "<module>:ROTATE_CONNECT_TIMEOUT_S": "6a2dbfc700028c0985f404a7be75cefc83d4fa2db62eb74d75db5a61970ac080",
    "<module>:TIMEOUT_RETRIES": "61cd24d612eebdf7178f47c69c4ec8e41f8455415e87b87fbe56e52aefadebd0",
    "LiveIB.__init__": "d7889bef8a28bda102faa8824603beba3ac4aa720ffcaffdb75a2d6cea204a50",
    "LiveIB.connect": "1bd9ed9a537919f39ba1cc4690d969f7db4343fcdff5cbc1955f8129eaa71478",
    "LiveIB._connect_under_gate": "a4060ae2a20453590af3e3c1d0f5ebb2fb307a87a8c5e571463e64e4338d1ec3",
    "LiveIB._on_error": "3613fb6193b9616ee10ceb08364173fcd6a34fcb3608429d7a398bf66b932b12",
    "_actions_between": "b401bf455f5a28586fd7ddfcf48859ff5fa755610cbd90adb8271585c9011107",
    "_backfill_earlier": "9b651431901c577e913faf10d95f4c22deaa4aba1a9b42281a29217849fd74c3",
    "_commit_month_locked": "c062e9494c421468b5a3a5a1d61a517a65ae33f233c9775b98a170a568dcb733",
    "_fetch_sessions": "409a11f33ea1d263097dcbd0ced9771850c0d701e3bd9015b2f5e51c038f5907",
    "_fill_series": "fc2d3952966f7b8f9b347198244774cd254b488a3de4db94cd69abd1629a972c",
    "_head_probe_floor": "566acb9540f26fd22d9d672c459ad3f87ff7407a50b6e67524b0fb9b1a3ec8ac",
    "_head_start_evidence": "5688672e01c61d05ac6052eb9b465194cac83e38b8508cc6ab8c64d5ce562ce0",
    "_make_session_processor": "470ddc1976b4cf4ef55f77c5d154851444cc5368908449e56a669b1a86984b43",
    "_make_session_processor.process": "ca96e3534f3261557dbbaab179d77810a4f43e4cab466be65de49390ad00d7e2",
    "_probe_earliest_daily": "6e96f9e512b9e2172c83e98db98c6812976f08e89443b6a09d8efadffb7eb64d",
    "_reconcile_head_and_served": "39671db0fce8316528707b7aef3d0a7bdceaf71088b4eb5a6dcc7c8907c1e2cc",
    "boundary_factor": "69391d69b9666aa6f3143ca389ad4e193d07b8871b9b14d426b3177bbe7ea647",
    "join_gate": "49a49a4d719301c430a9560f30f36eb21463c3fffb1fd492de6a31ad105d765d",
}
EXPECTED_MODULE_SEGMENT_COUNTS = {"stock": 12}
EXPECTED_FENCE_COUNTS = {"U3.a": 23, "U3.b": 5, "U6b": 16, "U8": 9,
                         "U15.a": 7, "U15.b": 5, "U16": 9,
                         "U17.a": 3, "U17.b": 2, "U17.c": 2, "U17.d": 4}


class HarnessError(BaseException):
    """Cannot be swallowed by production best-effort exception handlers."""


@dataclass(frozen=True)
class Fence:
    fence_id: str
    family: str
    owner: str
    old: str
    new: str
    probe: object


class Event:
    def __init__(self):
        self.handlers = []
    def __iadd__(self, callback):
        self.handlers.append(callback)
        return self
    def emit(self, *args):
        for callback in self.handlers:
            callback(*args)


class Lease:
    def __init__(self, metrics):
        self.metrics = metrics
        self.released = False
    def release(self):
        if not self.released:
            self.metrics["fake_release"] += 1
            self.released = True
    def __enter__(self):
        return self
    def __exit__(self, *exc):
        self.release()


def bounded(callback):
    result = {}
    def work():
        # All canonical mutants are bounded by construction. A trace budget
        # additionally interrupts Python loops, including broad except blocks.
        budget = 2000000
        def trace(frame, event, arg):
            nonlocal budget
            if event == "line":
                budget -= 1
                if budget <= 0:
                    raise HarnessError("bounded drive exhausted trace budget")
            return trace
        sys.settrace(trace)
        try:
            result["value"] = callback()
        except BaseException as exc:
            result["error"] = exc
        finally:
            sys.settrace(None)
    thread = threading.Thread(target=work, daemon=True, name="s6-bounded")
    thread.start()
    thread.join(20)
    if thread.is_alive():
        raise HarnessError("bounded drive exceeded 20 seconds")
    if "error" in result:
        raise result["error"]
    return result.get("value")


class Subject:
    def __init__(self, source, metrics):
        self.metrics = metrics
        self.instances = []
        self.calls = []
        self.leases = []
        self.script = lambda port, cid, attempt: None
        self.api = True
        self.stack = ExitStack()
        gate = types.ModuleType("operation_gate")
        gate.OperationBusy = type("OperationBusy", (RuntimeError,), {})
        def acquire(*args, **kw):
            metrics["fake_acquire"] += 1
            lease = Lease(metrics)
            self.leases.append(lease)
            return lease
        gate.acquire = acquire
        fake = types.ModuleType("ib_async")
        subject = self
        class IB:
            def __init__(self):
                self.errorEvent = Event()
                self.client = types.SimpleNamespace()
                if subject.api:
                    self.client.apiError = Event()
                self.disconnected = 0
                self.market = None
                self.RequestTimeout = None
                subject.instances.append(self)
            def connect(self, host, port, **kw):
                subject.calls.append((host, port, kw, self))
                outcome = subject.script(port, kw["clientId"], len(subject.calls))
                if outcome in ("326", "message", "api"):
                    if outcome == "326":
                        self.errorEvent.emit(-1, "326", "occupied")
                    elif outcome == "message":
                        self.errorEvent.emit(-1, 999, "ALREADY IN USE")
                    else:
                        self.client.apiError.emit("ClientId already in use?")
                    raise RuntimeError("held")
                if isinstance(outcome, BaseException):
                    raise outcome
            def disconnect(self):
                self.disconnected += 1
            def reqMarketDataType(self, value):
                self.market = value
        fake.IB = IB
        fake.StartupFetch = int
        fake.Stock = lambda symbol, exchange, currency: contract(symbol, 0)
        fake.Contract = lambda **kw: types.SimpleNamespace(**kw)
        self.fake = fake
        self.stack.enter_context(seeded_modules({"ib_async": fake, "operation_gate": gate}))
        self.module = execute_module("_s6_stock_subject", TARGET, source)
        self.tmp = self.stack.enter_context(tempfile.TemporaryDirectory(prefix="s6_subject_"))
        self.root = Path(self.tmp)
        self.module.set_diag_log(self.root / "diagnostic.log")
        self.stack.enter_context(patch.object(self.module, "_connection_diag_path",
                                             return_value=self.root / "diagnostic.log"))
        self.stack.enter_context(patch.object(self.module, "_disk_preflight", return_value=True))
        for name in ("_prevent_sleep", "_allow_sleep", "_interruptible_sleep"):
            self.stack.enter_context(patch.object(self.module, name,
                side_effect=lambda *a, **k: metrics.update(["sleep_seam"])))
        self.stack.enter_context(patch.object(self.module.LiveIB, "_loop",
            side_effect=lambda: metrics.update(["loop_seam"])))
    def close(self):
        root = self.root
        for lease in self.leases:
            lease.release()
        self.stack.close()
        if root.exists():
            raise HarnessError("subject temporary root survived cleanup")
        self.metrics["roots_removed"] += 1


class InstantPacer:
    _pause = None
    def __init__(self):
        self.turns = 0
        self.saturations = 0
    def wait_turn(self, *args, **kw):
        self.turns += 1
        return 0.000001
    def saturate(self):
        self.saturations += 1


def contract(symbol="S6X", conid=123):
    if symbol not in SYNTHETIC_TICKERS:
        raise HarnessError("ticker outside positive S6 allowlist")
    return types.SimpleNamespace(symbol=symbol, conId=conid, secType="STK",
                                 exchange="SMART", currency="USD")


def bar(day, price=10):
    return types.SimpleNamespace(date=day, open=price, high=price * 1.01,
                                 low=price * .99, close=price, volume=100)


class RawIB:
    def __init__(self, ny):
        self.head = datetime(2020, 1, 2, 9, 30, tzinfo=ny)
        self.rows = [bar(date(2020, 1, 2))]
        self.failures = []
        self.calls = []
    def reqHeadTimeStampAsync(self, c, **kw):
        contract(c.symbol)
        self.calls.append(("head", kw))
        if isinstance(self.head, BaseException):
            raise self.head
        return self.head
    def reqHistoricalDataAsync(self, c, **kw):
        contract(c.symbol)
        self.calls.append(("bars", kw))
        if self.failures:
            raise self.failures.pop(0)
        return self.rows
    def isConnected(self):
        return True
    def managedAccounts(self):
        return ["S6-OFFLINE"]


@contextmanager
def fixture(subject):
    import fetch_ibkr_bridge as bridge
    from fetch_authority import ScheduleAuthority, NY
    from fetch_run_context import FetchRunContext
    from fetch_ledger import inspect_ledger
    import fetch_governors as gov
    with tempfile.TemporaryDirectory(prefix="probe_", dir=subject.root) as tmp:
        root = Path(tmp)
        virtual_time = [0.0]
        def virtual_sleep(seconds, cancel=None):
            virtual_time[0] += seconds
        registry = gov.GovernorRegistry(time_fn=lambda: virtual_time[0], sleep_fn=virtual_sleep)
        context = FetchRunContext.create(root / "ledgers", authority=ScheduleAuthority.load(),
            clock=lambda: datetime(2026, 8, 4, 17, tzinfo=NY), governors=registry)
        subject.metrics["ledgers_opened"] += 1
        pacer = InstantPacer()
        adapter = subject.module.LiveIB()
        raw = RawIB(NY)
        adapter.ib = raw
        adapter._await = lambda response, timeout, what: response
        @bridge.worker_scope
        def run(callback, pacer=None, cancel=None):
            return callback()
        def drive(callback):
            return bounded(lambda: run(callback, pacer=pacer, _fetch_worker=context.worker("s6")))
        f = types.SimpleNamespace(root=root, context=context, adapter=adapter, raw=raw,
                                  pacer=pacer, drive=drive, ny=NY, module=subject.module)
        real_wait, real_saturate = gov.Governor.wait_turn, gov.Governor.saturate
        def counted_wait(governor, *args, **kwargs):
            elapsed = real_wait(governor, *args, **kwargs)
            pacer.wait_turn()
            return elapsed
        def counted_saturate(governor):
            pacer.saturate()
            return real_saturate(governor)
        with patch.object(subject.module, "_default_pacer", return_value=pacer), \
                patch.object(gov.Governor, "wait_turn", counted_wait), \
                patch.object(gov.Governor, "saturate", counted_saturate):
            try:
                yield f
            finally:
                try:
                    context.seal()
                    events = inspect_ledger(context.ledger.path, require_seal=True)["events"]
                    decisions = [e for e in events if e["event"] == "decision"]
                    results = [e for e in events if e["event"] == "result"]
                    if len(decisions) != len(results) or len(decisions) < len(raw.calls):
                        raise HarnessError("reached request has no completed decision/result")
                    subject.metrics["ledgers_verified"] += 1
                    subject.metrics["physical_requests"] += len(raw.calls)
                except HarnessError:
                    raise
                except BaseException as exc:
                    raise HarnessError(f"ledger cleanup failed: {exc}") from exc
                finally:
                    context.ledger.close()
    if root.exists():
        raise HarnessError("probe temporary root survived cleanup")
    subject.metrics["roots_removed"] += 1


def connect_probe(subject, signal="326", *, no_api=False, startup=True,
                  nonbusy=False, exhaustion=False, failure=None):
    m = subject.module
    subject.api = not no_api
    if not startup:
        del subject.fake.StartupFetch
    base = 7311
    def script(port, cid, attempt):
        if failure is not None:
            return failure
        if exhaustion:
            return signal if port == 7851 and cid < base + 90 else None
        if nonbusy and cid == base + 10 and port == 7851:
            return RuntimeError("nonbusy")
        return signal if cid == base else None
    subject.script = script
    a = m.LiveIB(ports=(7851, 7852))
    if failure is not None:
        try:
            bounded(a.connect)
        except Exception as exc:
            from addstock_watchdog import hard_signal_kind
            kind = hard_signal_kind(failure)
            expected = ConnectionRefusedError if kind == "connection_refused" else (
                m.ConnectionLost if kind in ("connection_lost", "socket_reset") else ConnectionError)
            return (type(exc) is expected and "7851" in str(exc) and "7852" in str(exc)
                    and a.host in str(exc) and str(failure) in str(exc)
                    and [c[2]["clientId"] for c in subject.calls] == [base, base]
                    and all(i.disconnected == 1 for i in subject.instances))
        return False
    for repeat in range(2):
        start = len(subject.calls)
        result = bounded(a.connect)
        calls = subject.calls[start:]
        expected = ([(7851, base + k * 10) for k in range(9)] + [(7852, base)]
                    if exhaustion else [(7851, base), (7851, base + 10), (7852, base), (7852, base + 10)]
                    if nonbusy else [(7851, base), (7851, base + 10)])
        if [(c[1], c[2]["clientId"]) for c in calls] != expected:
            return False
        if result is not a or a.ib is not calls[-1][3] or (a.port, a.client_id) != expected[-1]:
            return False
        if len({id(c[3]) for c in calls}) != len(calls) or a.errors:
            return False
        for c in calls:
            kw = c[2]
            if kw != dict(clientId=kw["clientId"], timeout=4 if kw["clientId"] == base else 3,
                          readonly=True, **({"fetchFields": 0} if startup else {})):
                return False
        if any(c[3].disconnected != 1 for c in calls[:-1]):
            return False
        if a.ib.market != 3 or a.ib.RequestTimeout != 20.0:
            return False
    a.disconnect()
    return True


def head_probe(subject, case):
    m = subject.module
    today, since = date(2026, 8, 3), date(2022, 1, 3)
    with fixture(subject) as f:
        expected, note, served = date(2020, 1, 2), None, date(2020, 1, 2)
        if case in ("disagree", "disagree_none"):
            f.raw.head = datetime(2024, 1, 2, tzinfo=f.ny)
            if case == "disagree_none":
                since = None
            expected = date(2020, 1, 2) if since is None else since
            note = "disagreed with the served daily first bar"
        if case in ("served", "served_none", "missing", "missing_none"):
            f.raw.head = m.SeriesHalt("head unavailable")
            note = "probed real first bar"
            expected = since
            if case.endswith("_none"):
                since = None
                expected = today - timedelta(days=1825)
            if case.startswith("missing"):
                f.raw.rows = []
                served = None
                note = "backfilling ~5y" if since is None else "backfilling from the requested"
        if case == "propagate":
            from fetch_run_context import RequestRefused
            error = RequestRefused("injected head refusal")
            f.raw.head = error
            try:
                f.drive(lambda: m._head_start_evidence(f.adapter, contract(), since, today))
            except RequestRefused as exc:
                return exc is error and [x[0] for x in f.raw.calls] == ["head"]
            return False
        wts = "OPTION_IMPLIED_VOLATILITY" if case == "kind" else "TRADES"
        value = f.drive(lambda: m._head_start_evidence(f.adapter, contract(), since, today, wts))
        return (value[0] == expected and value[2] == served
                and (value[1] is None if note is None else note in value[1])
                and [x[0] for x in f.raw.calls] == ["head", "bars"]
                and all(x[1]["whatToShow"] == wts for x in f.raw.calls))


def daily_probe(subject, case):
    m = subject.module
    with fixture(subject) as f:
        expected = date(2020, 1, 2)
        if case == "datetime":
            f.raw.rows = [bar(datetime(2020, 1, 2))]
        if case == "empty":
            f.raw.rows, expected = [], None
        if case in ("timeout", "timeout_end"):
            f.raw.failures = [m.RequestTimeout("injected")] * (3 if case.endswith("end") else 1)
            if case.endswith("end"):
                expected = None
        if case in ("pacing", "pacing_end"):
            f.raw.failures = [m.PacingViolation("injected")] * (3 if case.endswith("end") else 1)
        try:
            actual = f.drive(lambda: m._probe_earliest_daily(f.adapter, contract(), date(2026, 8, 3)))
        except m.PacingViolation:
            return case == "pacing_end" and len(f.raw.calls) == 3 and f.pacer.saturations == 3
        count = 3 if case == "timeout_end" else 2 if case in ("timeout", "pacing") else 1
        if case == "timeout_end":
            diagnostic = subject.root / "diagnostic.log"
            if not diagnostic.exists() or "PROBE_FAIL" not in diagnostic.read_text(encoding="utf-8"):
                return False
        return (actual == expected and len(f.raw.calls) == count
                and all(c[1]["barSizeSetting"] == "1 day" and c[1]["useRTH"] is True
                        for c in f.raw.calls)
                and f.pacer.saturations == (1 if case == "pacing" else 0))


def result_dict():
    return dict(requests=0, bars_fetched=0, added=0, dup_existing=0, conflicts=0,
                written=0, months={}, blocked_months=[], notes=[], _empty_days=[],
                counters=dict(non_rth=0, invalid=0, outside_day=0))


def stored_bar(day, price=10):
    return (datetime.combine(day, time()), price, price * 1.01, price * .99, price, 100)


def commit_probe(subject, case):
    m = subject.module
    with fixture(subject) as f:
        interval = "1d-iv" if case in ("deferred", "guard_order") else "1d"
        price = .2 if interval == "1d-iv" else 10
        existing = stored_bar(date(2026, 7, 8), price)
        incoming = stored_bar(date(2026, 7, 8), price * 1.1)
        path = m.ss.month_file_path(f.root, "S6X", 2026, 7, interval)
        m.ss.write_month_file(path, [existing])
        if case == "unreadable":
            path.write_bytes(b"not parquet")
        before = path.read_bytes()
        if case == "stage":
            (f.root / "S6X" / m.ss.VOL_VALUE_RECONCILE_STAGE_DIR).mkdir()
        events = []
        res = result_dict()
        def sink(*args):
            events.append(("sink", args))
        original_guard = m._guard_ratio_month_commit
        def guard(*args, **kwargs):
            events.append(("guard", ()))
            return original_guard(*args, **kwargs)
        incoming_rows = [existing, incoming]
        if case == "mixed":
            incoming_rows.append(stored_bar(date(2026, 7, 9), price))
        with patch.object(m, "_guard_ratio_month_commit", side_effect=guard):
            m._commit_month_locked(f.root, "S6X", interval, (2026, 7),
                incoming_rows, "s6", "fixture", res, sink)
        if case in ("stage", "unreadable"):
            return (len(res["blocked_months"]) == 1 and path.read_bytes() == before
                    and not events and not res["months"])
        expected_events = ["guard", "sink"] if interval == "1d-iv" else ["sink", "guard"]
        if case == "mixed":
            rows, _ = m.ss.read_month_file(path)
            return (rows == [existing, incoming_rows[-1]] and res["added"] == 1
                    and res["written"] == 1 and res["conflicts"] == 1
                    and res["dup_existing"] == 1)
        return (path.read_bytes() == before and [e[0] for e in events] == expected_events
                and next(e[1] for e in events if e[0] == "sink") ==
                    ("S6X", interval, "2026-07", existing, incoming)
                and res["dup_existing"] == 1 and res["conflicts"] == 1
                and res["months"] == {"2026-07": dict(status="unchanged", dups=1, conflicts=1)}
                and res["written"] == 0 and res["added"] == 0)


def session_probe(subject, case):
    m = subject.module
    for pipeline in (False, True):
        with fixture(subject) as f:
            days = [date(2026, 7, 30), date(2026, 7, 31)]
            second = 30 if case in ("cliff", "unexplained") else 4 if case == "explained" else 10
            factor = .4 if case in ("explained", "adjusted", "unexplained") else 1.0
            actions = ([dict(date=days[1].isoformat(), applies="price", factor=factor)]
                       if factor != 1 else [])
            manifest = m.ss.new_manifest("S6X", "S6X")
            manifest["actions"] = actions
            m.ss.save_manifest(f.root / "S6X", manifest)
            f.raw.rows = [bar(days[0]), bar(days[1], second)]
            res, buffer = result_dict(), {}
            def flush(upto_month=None):
                for ym in sorted(list(buffer)):
                    if upto_month is None or ym < upto_month:
                        m._commit_month(f.root, "S6X", "1d", ym, buffer.pop(ym),
                                        "s6", "fixture", res, lambda *a: None)
            runner = m._run_days_pipelined if pipeline else m._run_days
            error = None
            try:
                f.drive(lambda: runner(res, days, f.root, f.adapter, f.pacer, "S6X", "1d",
                    contract(), [], None, True, lambda *a: None, None, flush, buffer,
                    m.ss.load_manifest(f.root / "S6X")["actions"]))
            except m.SeriesHalt as exc:
                error = exc
            halt = case in ("cliff", "unexplained")
            if bool(error) != halt:
                return False
            if not halt:
                flush()
            path = m.ss.find_month_file(f.root, "S6X", 2026, 7, "1d")
            if path is None:
                return False
            rows, _ = m.ss.read_month_file(path)
            if [b[0].date() for b in rows] != (days[:1] if halt else days):
                return False
            if halt and error.metadata.get("split_gate") != dict(gate="join", prior_date=days[0],
                    current_date=days[1], observed_factor=second / 10):
                return False
            if case == "unexplained" and "does not explain the jump either" not in str(error):
                return False
            if factor != 1 and not halt and not any("crossed recorded split/basis boundary 2026-07-31"
                    in n for n in res["notes"]):
                return False
    return True


def backward_probe(subject):
    m = subject.module
    with fixture(subject) as f:
        first = date(2026, 7, 31)
        path = m.ss.month_file_path(f.root, "S6X", 2026, 7, "1d")
        stats = m.ss.write_month_file(path, [stored_bar(first, 30)])
        manifest = m.ss.new_manifest("S6X", "S6X")
        m.ss.manifest_months(manifest, "1d")["2026-07"] = dict(stats, status="present", source="fixture")
        m.ss.save_manifest(f.root / "S6X", manifest)
        f.raw.head = datetime(2026, 7, 29, 9, 30, tzinfo=f.ny)
        f.raw.rows = [bar(date(2026, 7, 29)), bar(date(2026, 7, 30), 30), bar(first, 30)]
        res, buffer = result_dict(), {}
        flushes = []
        def flush(upto_month=None):
            flushes.append(upto_month)
            for ym in sorted(list(buffer)):
                if upto_month is None or ym < upto_month:
                    m._commit_month(f.root, "S6X", "1d", ym, buffer.pop(ym), "s6", "fixture",
                                    res, lambda *a: None)
        f.drive(lambda: m._backfill_earlier(res, f.root, f.adapter, f.pacer, "S6X", "1d",
            contract(), manifest, None, date(2026, 7, 29), date(2026, 8, 3), flush,
            buffer, [], lambda *a: None, None, False, None))
        rows, _ = m.ss.read_month_file(path)
        return ([b[0].date() for b in rows] == [date(2026, 7, 29), first]
                and flushes.count(None) == 3
                and any("backward extension stopped" in n for n in res["notes"]))


def fences(source):
    cases = []
    lines = source.split("\n")
    spans = {s.qualname: s for s in function_spans(source)}
    def add(family, label, scope, needle, replacement, probe):
        span = spans[scope]
        old = "\n".join(lines[span.start_line - 1:span.end_line]) + "\n"
        if old.count(needle) != 1:
            raise HarnessError(f"{label}: nonunique anchor within {scope}: {old.count(needle)}")
        new = old.replace(needle, replacement, 1)
        owner = mutation_owner(source, old, new)
        cases.append(Fence(label, family, owner, old, new, probe))
    # Scope rule: the 12 module constants named by S6 v1 section 1.9,
    # not unrelated global reads in large host functions (owned by other slices).
    for node in ast.parse(source).body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or target.id not in CONSTANTS:
            continue
        name = target.id
        family = ("U3.a" if name.startswith("CLIENT_") or name in (
            "CONNECT_TIMEOUT_S", "ROTATE_CONNECT_TIMEOUT_S", "QUALIFY_TIMEOUT_S") else
            "U15.a" if name.startswith("JOIN_") else "U8" if name.startswith("HEAD_PROBE") else "U6b")
        old = "\n".join(lines[node.lineno - 1:node.end_lineno])
        new = f"{name} = {repr('29 Y' if name == 'HEAD_PROBE_DURATION' else CONSTANTS[name] + 1)}"
        owner = mutation_owner(source, old, new)
        if owner != "<module>:" + name:
            raise HarnessError("constant mutation custody mismatch: " + name)
        cases.append(Fence("constant-" + name, family, owner, old, new,
                           lambda s, n=name, v=CONSTANTS[name]: getattr(s.module, n) == v))

    connect = "LiveIB._connect_under_gate"
    cp = lambda s: connect_probe(s)
    for label, needle, replacement, probe in (
        ("busy-326", "any(code == 326 for code, _ in self.errors)", "False", cp),
        ("busy-message", 'any("already in use" in str(m).lower()\n                                   for _, m in self.errors)',
         "False", lambda s: connect_probe(s, "message")),
        ("busy-api", 'any("already in use" in m.lower()\n                                   for m in api_errs)',
         "False", lambda s: connect_probe(s, "api")),
        ("stride-climbs", "k * CLIENT_ID_STRIDE", "k * 0", cp),
        ("fresh-IB-per-attempt", "ib = IB()", "ib = self.ib if self.ib is not None else IB(); self.ib = ib", cp),
        ("error-reset", "self.errors = []", "pass", cp),
        ("connect-readonly", "readonly=True", "readonly=False", cp),
        ("connect-timeout-rotation", "CONNECT_TIMEOUT_S if k == 0 else ROTATE_CONNECT_TIMEOUT_S", "CONNECT_TIMEOUT_S", cp),
        ("skip-startup-fields", 'kw["fetchFields"] = no_fetch', "pass", cp),
        ("no-startup-compatible", "if no_fetch is not None:", "if True:", lambda s: connect_probe(s, startup=False)),
        ("no-api-compatible", "except Exception:  # noqa: BLE001 — ib_async without apiError\n                    pass",
         "except Exception:\n                    raise", lambda s: connect_probe(s, no_api=True)),
        ("delayed-market-type", "ib.reqMarketDataType(3)", "ib.reqMarketDataType(1)", cp),
        ("request-timeout-backstop", "ib.RequestTimeout = QUALIFY_TIMEOUT_S", "ib.RequestTimeout = 0", cp),
        ("failed-attempt-disconnect", "ib.disconnect()         # free the half-built socket", "pass", cp),
        ("success-identity", "self.ib, self.port, self.client_id = ib, port, cid",
         "self.ib, self.port, self.client_id = ib, port, self._base_client_id", cp),
    ):
        add("U3.a", label, connect, needle, replacement, probe)
    add("U3.a", "numbered-error-normalized", "LiveIB._on_error", "code = int(code)", "code = code", cp)
    add("U3.a", "reconnect-restarts-base", connect, "cid = self._base_client_id + k * CLIENT_ID_STRIDE",
        "cid = self.client_id + k * CLIENT_ID_STRIDE", cp)
    add("U3.b", "nine-id-bound-and-port-fallthrough", connect, "range(CLIENT_ID_RETRIES + 1)",
        "range(CLIENT_ID_RETRIES + 2)", lambda s: connect_probe(s, exhaustion=True))
    add("U3.b", "nonbusy-breaks-port", connect, "break                        # other failure — next port",
        "continue                     # other failure — next id", lambda s: connect_probe(s, nonbusy=True))
    for label, failure, needle, replacement in (
        ("last-refused-typed", ConnectionRefusedError("refused"),
         'if kind == "connection_refused" else', "if False else"),
        ("last-lost-typed", ConnectionResetError("connection reset"),
         'if kind in ("connection_lost", "socket_reset") else', "if False else"),
        ("last-generic-typed-and-context", RuntimeError("bad handshake"),
         "                        ConnectionError)", "                        ConnectionRefusedError)"),
    ):
        add("U3.b", label, connect, needle, replacement, lambda s, e=failure: connect_probe(s, failure=e))

    hs = "_head_start_evidence"
    for label, needle, replacement, scenario in (
        ("head-agrees", "return head_date, None, probed", "return head_date, 'wrong note', probed", "agree"),
        ("head-disagrees-since", "max(resolved, since) if since is not None else resolved", "resolved", "disagree"),
        ("head-disagrees-no-depth", "max(resolved, since) if since is not None else resolved", "since", "disagree_none"),
        ("head-failed-served-since", "start = max(probed, since)", "start = probed", "served"),
        ("head-failed-served-no-depth", "start = max(probed, today - timedelta(days=NO_HEAD_FALLBACK_DAYS))", "start = probed", "served_none"),
        ("both-failed-since", "if probed is not None:", "if True:", "missing"),
        ("both-failed-no-depth", "return (today - timedelta(days=NO_HEAD_FALLBACK_DAYS),", "return (today,", "missing_none"),
        ("head-refusal-propagates-no-probe", "except SeriesHalt as exc:", "except Exception as exc:", "propagate"),
        ("head-kind-forwarding", "else adapter.head_timestamp(contract, what_to_show)", "else adapter.head_timestamp(contract)", "kind"),
    ):
        add("U6b", label, hs, needle, replacement, lambda s, c=scenario: head_probe(s, c))
    dp = "_probe_earliest_daily"
    for label, needle, replacement, scenario in (
        ("probe-date-normalized", "return d0\n", "return None\n", "date"),
        ("probe-datetime-normalized", "return d0.date()", "return None", "datetime"),
        ("probe-timeout-retry", "range(TIMEOUT_RETRIES + 1)", "range(1)", "timeout"),
        ("probe-timeout-exhaustion", "if _try >= TIMEOUT_RETRIES:\n                _diag", "if _try >= TIMEOUT_RETRIES + 1:\n                _diag", "timeout_end"),
        ("probe-pacing-saturation", "fib.pacer().saturate()", "pass", "pacing"),
        ("probe-pacing-exhaustion", "if _try >= TIMEOUT_RETRIES:\n                raise", "if _try >= TIMEOUT_RETRIES:\n                return None", "pacing_end"),
    ):
        add("U6b", label, dp, needle, replacement, lambda s, c=scenario: daily_probe(s, c))

    def floor_probe(s):
        m = s.module
        return (m._head_probe_floor(date(2026, 8, 3)) == date(1996, 8, 3)
                and m._head_probe_floor(date(2024, 2, 29)) == date(1994, 2, 28))
    add("U8", "floor-parsed-years", "_head_probe_floor", "years = int(HEAD_PROBE_DURATION.split()[0])", "years = 29", floor_probe)
    add("U8", "floor-leap-fallback", "_head_probe_floor", "day=28", "day=27", floor_probe)
    def reconcile_probe(s):
        fn, today, old, floor, middle = s.module._reconcile_head_and_served, date(2026, 8, 3), date(1990, 1, 2), date(1996, 8, 3), date(2020, 1, 2)
        return (fn(old, None, today) == old and fn(None, middle, today) == middle
                and fn(old, floor + timedelta(days=14), today) == old
                and fn(old, floor + timedelta(days=15), today) == floor + timedelta(days=15)
                and fn(old, middle, today) == middle and fn(date(2024, 1, 2), middle, today) == middle)
    for label, needle, replacement in (
        ("frontier-head-alone", "if served_date is None:", "if served_date is not None:"),
        ("frontier-served-alone", "if head_date is None:", "if head_date is not None:"),
        ("frontier-window-limit", "head_date < floor", "head_date > floor"),
        ("frontier-floor-inclusive-tolerance", "served_date <= floor", "served_date < floor"),
        ("frontier-observed-wins", "    return served_date\n", "    return head_date\n"),
    ):
        # Last return needs full context because served-alone also returns served.
        if label == "frontier-observed-wins":
            needle, replacement = "        return head_date\n    return served_date", "        return head_date\n    return head_date"
        add("U8", label, "_reconcile_head_and_served", needle, replacement, reconcile_probe)

    def join_probe(s):
        fn = s.module.join_gate
        for ratio in (.55, 1, 1.9):
            if fn(10, 10 * ratio, "S6X") is not None:
                return False
        for ratio in (.54, 1.91):
            message = fn(10, 10 * ratio, "S6X")
            if not message or not all(t in message for t in (
                "S6X", f"{ratio:.3f}", "0.55", "1.9", "committed only UP TO the session before the jump")):
                return False
        return fn(10, 4, "S6X", expected_factor=.4) is None
    for label, needle, replacement in (
        ("join-lower-inclusive", "JOIN_RATIO_LOW <= ratio", "JOIN_RATIO_LOW < ratio"),
        ("join-upper-inclusive", "ratio <= JOIN_RATIO_HIGH", "ratio < JOIN_RATIO_HIGH"),
        ("join-outside-bounds", "if not (JOIN_RATIO_LOW <= ratio <= JOIN_RATIO_HIGH):", "if False:"),
        ("join-recorded-divisor", "next_open / prev_close / expected_factor", "next_open / prev_close"),
        ("join-actionable-message", "committed only UP TO the session before the jump; ", "unhelpful message; "),
    ):
        add("U15.a", label, "join_gate", needle, replacement, join_probe)
    processor = "_make_session_processor.process"
    for label, needle, replacement in (
        ("pre-cliff-flush", "flush()                       # everything BEFORE the cliff", "pass"),
        ("cliff-day-excluded", "flush()                       # everything BEFORE the cliff",
         "buffer.setdefault((day.year, day.month), []).extend(bars); flush()"),
        ("split-metadata", '"observed_factor": bars[0][1] / st["prev_close"],', '"observed_factor": None,'),
    ):
        add("U15.b", label, processor, needle, replacement, lambda s: session_probe(s, "cliff"))
    add("U15.b", "backward-flush", "_backfill_earlier", "flush()                                   # keep months committed pre-cliff",
        "pass", backward_probe)
    add("U15.b", "backward-note", "_backfill_earlier", "backward extension stopped ({exc})", "incorrect note ({exc})", backward_probe)

    def factor_probe(s):
        actions = [dict(date=d, factor=f, applies=k) for d, f, k in (
            ("2026-07-01", 9, "price"), ("2026-07-02", .5, "price"),
            ("2026-07-03", 2, "both"), ("2026-07-03", 3, "volume"),
            ("2026-07-04", 5, "price"), ("2026-07-03", "bad", "price"))]
        fn = s.module.boundary_factor
        return (fn(actions, "2026-07-01", "2026-07-03") == 1
                and fn(actions, "2026-07-01", "2026-07-03", applies=("volume", "both")) == 6
                and fn([], "2026-07-01", "2026-07-03") == 1)
    for label, scope, needle, replacement in (
        ("actions-lower-exclusive", "_actions_between", "and lo < str", "and lo <= str"),
        ("actions-upper-inclusive", "_actions_between", 'str(a.get("date", "")) <= hi', 'str(a.get("date", "")) < hi'),
        ("actions-applies-filter", "_actions_between", 'a.get("applies") in applies', "True"),
        ("actions-factor-product", "boundary_factor", 'f *= float(a.get("factor", 1.0))', 'f = float(a.get("factor", 1.0))'),
        ("actions-malformed-ignored", "boundary_factor", "pass        # hand-mangled", "raise       # hand-mangled"),
    ):
        add("U16", label, scope, needle, replacement, factor_probe)
    for label, needle, replacement, scenario in (
        ("factor-consumed-at-join", 'f = boundary_factor(actions, st["prev_day"], day)', "f = 1.0", "explained"),
        ("adjusted-feed-second-chance", "if reason and f != 1.0:", "if False:", "adjusted"),
        ("factor-unexplained-note", "explain the jump either)", "WRONG)", "unexplained"),
        ("factor-crossing-note", "crossed recorded split/basis boundary {dates}", "WRONG {dates}", "explained"),
    ):
        add("U16", label, processor, needle, replacement, lambda s, c=scenario: session_probe(s, c))

    commit = "_commit_month_locked"
    for family, label, needle, replacement, scenario in (
        ("U17.a", "conflict-immediate", "conflict_sink(ticker, interval, ss.month_key(y, m), e, b)", "pass", "immediate"),
        ("U17.a", "conflict-deferred-once", "conflict_sink(ticker, interval, key, existing_bar, incoming_bar)", "pass", "deferred"),
        ("U17.a", "conflict-argument-order", "conflict_sink(ticker, interval, ss.month_key(y, m), e, b)",
         "conflict_sink(ticker, interval, ss.month_key(y, m), b, e)", "immediate"),
        ("U17.b", "conflict-counter", 'res["conflicts"] += confs', 'res["conflicts"] += 0', "immediate"),
        ("U17.b", "duplicate-counter", 'res["dup_existing"] += dups', 'res["dup_existing"] += 0', "immediate"),
        ("U17.c", "existing-bar-preserved", "confs += 1", "confs += 1; added.append(b)", "immediate"),
        ("U17.c", "existing-retained-on-mixed-write", "stats = ss.write_month_file(path, existing + added)",
         "stats = ss.write_month_file(path, added)", "mixed"),
        ("U17.d", "unchanged-status", '"status": "unchanged", "dups": dups', '"status": "written", "dups": dups', "immediate"),
        ("U17.d", "unchanged-no-write", "    if not added:", "    if False:", "immediate"),
        ("U17.d", "pending-stage-blocks", "if correction_stage.exists() or correction_stage.is_symlink():", "if False:", "stage"),
        ("U17.d", "unreadable-blocks", '"reason": f"existing file failed the strict read "', '"reason": f"existing file failed the strict read "', "unreadable"),
    ):
        if label == "unreadable-blocks":
            needle = '            return\n    emap = {b[0]: b for b in existing}'
            replacement = '            pass\n    emap = {b[0]: b for b in existing}'
        add(family, label, commit, needle, replacement, lambda s, c=scenario: commit_probe(s, c))
    return tuple(c for c in cases if c.fence_id != "probe-datetime-normalized")


def datetime_redundancy(source):
    """The real A1 raw guard rejects datetime daily labels before this branch.

    Mutation compiles, identical refusal witness, then every active S6 fence
    must survive it. This is not a new A1 safety fence or a claimed host defect.
    """
    span = next(s for s in function_spans(source) if s.qualname == "_probe_earliest_daily")
    old = "\n".join(source.split("\n")[span.start_line - 1:span.end_line]) + "\n"
    new = old.replace("return d0.date()", "return None", 1)
    if mutation_owner(source, old, new) != "_probe_earliest_daily":
        raise HarnessError("redundancy custody mismatch")
    return old, new


def datetime_refusal_witness(subject):
    from fetch_run_context import RequestRefused
    with fixture(subject) as f:
        f.raw.rows = [bar(datetime(2020, 1, 2, tzinfo=f.ny))]
        try:
            f.drive(lambda: subject.module._probe_earliest_daily(f.adapter, contract(), date(2026, 8, 3)))
        except RequestRefused as exc:
            return (type(exc).__name__, str(exc), tuple(c[0] for c in f.raw.calls))
        return None


def run():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    kit, metrics = CheckKit(), Counter()
    paths = [TARGET, *(ENGINE_ROOT / name for name in (
        "fetch_ibkr_bridge.py", "fetch_run_context.py", "fetch_ledger.py",
        "fetch_authority.py", "fetch_envelopes.py", "fetch_governors.py",
        "fetch_test_policy.py", "addstock_watchdog.py"))]
    raw = {p: p.read_bytes() for p in paths}
    source = normalize_source_bytes(raw[TARGET])
    cases = fences(source)
    owners = frozenset(c.owner for c in cases) | CONTEXT_ONLY_SEGMENTS
    digests = function_segment_sha256(source, owners)
    kit.check("unique fence IDs", len({c.fence_id for c in cases}) == len(cases))
    counts = dict(Counter(c.family for c in cases))
    print("FENCE_COUNTS " + repr(counts) + f" total={len(cases)}", flush=True)
    kit.check("pinned code-derived family inventory", counts == EXPECTED_FENCE_COUNTS, repr(counts))
    kit.check("pinned source segment custody", digests == EXPECTED_FUNCTION_SEGMENT_SHA256)
    kit.check("twelve explicit constant segments", sum(n.startswith("<module>:") for n in owners) == EXPECTED_MODULE_SEGMENT_COUNTS["stock"])
    kit.check("unrelated insertion leaves custody unchanged", function_segment_sha256("# unrelated\n" + source, owners) == digests)
    kit.check("positive ticker allowlist", SYNTHETIC_TICKERS == {"S6X", "S6Y"} and not SYNTHETIC_TICKERS & FORBIDDEN_TICKERS)
    kit.check("tripwire precedes every subject import",
              not any(n in sys.modules for n in ("stock_ibkr", "engine.stock_ibkr", "ib_async", "display_data")))
    tripwire = EarlyTripwire()
    # Import no subject before the tripwire. Gate modules are independently
    # denied, including the package-qualified spelling, before fake seeding.
    with tripwire, ExitStack() as stack:
        if str(ENGINE_ROOT.parent) not in sys.path:
            sys.path.insert(0, str(ENGINE_ROOT.parent))
        for name in ("operation_gate", "engine.operation_gate"):
            gate = importlib.import_module(name)
            def deny_gate(*args, **kw):
                metrics["real_acquire"] += 1
                raise HarnessError("real operation gate reached")
            stack.enter_context(patch.object(gate, "acquire", side_effect=deny_gate))
        expected_tripwires = {"socket", "create_connection", "getaddrinfo", "Popen", "system"}
        import os
        expected_tripwires.update(n for n in dir(os) if n.startswith("spawn") and callable(getattr(os, n)))
        kit.check("tripwire covers every required primitive",
                  {name for _, name, _ in tripwire._patches} == expected_tripwires)
        baseline_ok = {}
        for case in cases:
            for mutant in (False, True):
                label = ("MUTATION " if mutant else "FENCE ") + case.fence_id
                # fences() already proves each exact owner with mutation_owner;
                # the immutable source and unique full anchor cannot change here.
                if source.count(case.old) != 1:
                    raise HarnessError("source anchor custody failed: " + case.fence_id)
                if mutant and not baseline_ok.get(case.fence_id):
                    kit.check(label, False, "baseline failed; no mutation kill can be claimed")
                    continue
                candidate = source.replace(case.old, case.new, 1) if mutant else source
                try:
                    compile(candidate, str(TARGET), "exec")
                    subject = Subject(candidate, metrics)
                except BaseException as exc:
                    kit.check(label, False, "subject did not compile/load: " + repr(exc))
                    continue
                passed, detail = False, ""
                try:
                    passed = case.probe(subject) is True
                except HarnessError:
                    raise
                except Exception as exc:
                    detail = repr(exc)
                finally:
                    subject.close()
                if tripwire.denials or metrics["real_acquire"]:
                    raise HarnessError("containment failure cannot count as mutation kill")
                if not mutant:
                    baseline_ok[case.fence_id] = passed
                kit.check(label, not passed if mutant else passed, detail or "witness result differed")
        old, new = datetime_redundancy(source)
        redundant_source = source.replace(old, new, 1)
        signatures = []
        for candidate in (source, redundant_source):
            subject = Subject(candidate, metrics)
            try:
                signatures.append(datetime_refusal_witness(subject))
            finally:
                subject.close()
        kit.check("REDUNDANT-CONFIRMED datetime daily branch witness parity",
                  signatures[0] == signatures[1] == (
                      "RequestRefused", "daily IB timestamp must be a calendar date", ("bars",)), repr(signatures))
        for case in cases:
            subject = Subject(redundant_source, metrics)
            try:
                survived = case.probe(subject) is True
            except HarnessError:
                raise
            except Exception:
                survived = False
            finally:
                subject.close()
            kit.check("REDUNDANCY-CORPUS " + case.fence_id, survived and baseline_ok[case.fence_id])
    kit.check("early tripwire zero denials", not tripwire.denials, repr(tripwire.denials))
    kit.check("fake gate exercised without real acquire", metrics["fake_acquire"] > 0 and metrics["real_acquire"] == 0)
    kit.check("every ledger sealed and verified", metrics["ledgers_opened"] > 0 and metrics["ledgers_opened"] == metrics["ledgers_verified"])
    kit.check("counted no-op loop used", metrics["loop_seam"] > 0)
    kit.check("all host and dependency bytes unchanged", all(p.read_bytes() == b for p, b in raw.items()))
    kit.check("same-run segment custody unchanged", function_segment_sha256(normalize_source_bytes(TARGET.read_bytes()), owners) == digests)
    print("CONTAINMENT " + repr(dict(metrics)))
    return kit.finish()


if __name__ == "__main__":
    raise SystemExit(run())
