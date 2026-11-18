"""Mutation reference for the Add Stocks watchdog hard-signal layer.

Row 85 slice S4b covers ``hard_signal_kind``, ``HardSignalAdapter``, and
``HardDeathWatchdog``.  Mutants execute only in memory.  Fixtures use injected
clocks and maintenance providers; they create no files, sockets, processes, or
threads.  The classifier cycle mutant terminates through a bounded synthetic
exception fixture rather than a timeout.
"""

from __future__ import annotations

import ast
from collections import Counter
from dataclasses import dataclass
import errno
import hashlib
from pathlib import Path
import sys
import types
from typing import Callable


ENGINE_ROOT = Path(__file__).resolve().parent
TARGET = ENGINE_ROOT / "addstock_watchdog.py"
EXPECTED_FUNCTION_SEGMENT_SHA256 = {
    "HardDeathWatchdog.__init__": "e2e38371a7c0c683006febbb9da40a918e75562546893f225d4590a908af357c",
    "HardDeathWatchdog._advance_locked": "a4d10dc399a1d9a4e23ab2e06d52b5df2353004cf74f7b0f34532cdc74e8e503",
    "HardDeathWatchdog._read_maintenance": "53c812cb83de72ab9e7de38a57ee18e58a9109a3735335b8c5275b4af1254c3b",
    "HardDeathWatchdog.due_probes": "dad89f937c62d8a883c375e3bfdeedf4a14869e0cd648209fbbfa4b0f96724b7",
    "HardDeathWatchdog.hard_signal": "6681c2b902646223eece027bb0236dfcdedd6684df2b86c71fbe6df49616e4ed",
    "HardDeathWatchdog.healthy_ports": "292bff080327527a61d3b70090c11958efadd9e8ad3c1a9f5948e26195a82f5b",
    "HardDeathWatchdog.holding": "5e407c81b6450c8ec6b86b6839c456a87bacca7db3a30e6e56f073d406b2b15e",
    "HardDeathWatchdog.interrupt_event": "27b667c44f71cfc03bb04137fbe411e207faf195d0a122101f30de1698ec1a79",
    "HardDeathWatchdog.interrupt_sequence": "fffe4986fa6da60e3fbdb67d8c12aa5d5820227ae9eca8d2524588e6b25248ab",
    "HardDeathWatchdog.ports": "dd034b3458695e454578507493b9cb1960e3fde614dea096e56ab509f2555e5a",
    "HardDeathWatchdog.probe_result": "cb56e280d6086a2b6675adec1b777a1a89da09ab63d17720a3ff548bd1bb29a2",
    "HardDeathWatchdog.should_finalize": "709dd0d3705f200e38568392f647b9aa649ce183467079ac4a27a60bdaedd28b",
    "HardDeathWatchdog.snapshot": "16673d2771d83a9c06460b8e9824a6ba03e231c3bbe00b02ce09f2424a8b19d3",
    "HardDeathWatchdog.state": "971ca143c9e6463808c523dab196423df02d3820fd81b2d43f7602ef4e92069a",
    "HardSignalAdapter.__call__": "4bb838ef47fc77acd614d86096b03fab9ed03dc8e29996aed0d672c96d95531e",
    "HardSignalAdapter.__getattr__": "519360b632f4daa861d70b1a423148d29d70dc2212f6f5a9ee1480e510a3b088",
    "HardSignalAdapter.__getattr__.observed": "98528c9cb76e677eae2ec6851c0029e282c9a79243727a85050e82ecc69b0076",
    "HardSignalAdapter.__init__": "dc2eba70a69e4dcc0950f6a968f51a065b9e9a46270c9ab7611005a50d79e4ab",
    "HardSignalAdapter.__setattr__": "c75fb849d796345666412bc776e3420ca2d65bb2ec4c1fafc8e6a1586a282e9d",
    "HardSignalAdapter._observe": "18ba8b33d421869576f965b9b4a6ff48ad3208c88d9c04fb591623ed0c520326",
    "hard_signal_kind": "975d8de28c376b2c3e458b8cbe28a35bddbfa6978a6c618f179f8af0b4cd6daf",
}
FORBIDDEN_TICKERS = frozenset({"APO", "JCI", "OKE", "TKO", "WBD", "TMUS"})
SYNTHETIC_TICKERS: frozenset[str] = frozenset()


Probe = Callable[[types.ModuleType], bool]

if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

from source_segment_custody import (  # noqa: E402
    function_segment_sha256,
    mutation_owner,
    mutation_owners,
    normalize_source_bytes,
)


class CheckKit:
    def __init__(self) -> None:
        self.total = 0
        self.failed = 0

    def section(self, title: str) -> None:
        print(f"=== {title} " + "=" * max(1, 66 - len(title)))

    def check(self, name: str, passed: bool, detail: str = "") -> None:
        self.total += 1
        if passed:
            print(f"[PASS] {name}")
            return
        self.failed += 1
        suffix = f" :: {detail}" if detail else ""
        print(f"[FAIL] {name}{suffix}")

    def finish(self) -> int:
        print()
        print(f"{self.total} checks, {self.failed} failed")
        print("ALL PASS" if not self.failed else "FAILURES PRESENT")
        print(f"harness exit={0 if not self.failed else 3}")
        return 0 if not self.failed else 3


@dataclass(frozen=True)
class Mutation:
    old: str
    new: str

    def apply(self, source: str) -> str:
        count = source.count(self.old)
        if count != 1:
            raise AssertionError(
                f"mutation anchor count={count}, expected 1: {self.old!r}")
        return source.replace(self.old, self.new, 1)


@dataclass(frozen=True)
class Fence:
    fence_id: str
    function: str
    description: str
    mutation: Mutation
    probe: Probe


@dataclass(frozen=True)
class Redundancy:
    clause_id: str
    description: str
    mutation: Mutation
    witness: Probe


def mu(old: str, new: str) -> Mutation:
    return Mutation(old, new)


def f(fence_id: str, function: str, description: str,
      old: str, new: str, probe: Probe) -> Fence:
    return Fence(fence_id, function, description, mu(old, new), probe)


def p(call: Callable[..., bool], *args: object) -> Probe:
    return lambda module: call(module, *args)


def source_bytes() -> bytes:
    return TARGET.read_bytes()


def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def normalized_source(payload: bytes) -> str:
    return normalize_source_bytes(payload)


def load_module(source: str) -> types.ModuleType:
    name = "_addstock_watchdog_signal_fence_subject"
    module = types.ModuleType(name)
    module.__file__ = str(TARGET)
    module.__package__ = ""
    previous = sys.modules.get(name)
    sys.modules[name] = module
    try:
        exec(compile(source, str(TARGET), "exec"), module.__dict__)
    finally:
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous
    return module


def evaluate(probe: Probe, module: types.ModuleType) -> tuple[bool, str]:
    try:
        result = probe(module)
    except BaseException as exc:  # exact failure behavior is under test
        return False, f"{type(exc).__name__}: {exc}"
    if result is True:
        return True, ""
    return False, f"probe returned {result!r}"


def capture(call: Callable[[], object]) -> BaseException | None:
    try:
        call()
    except BaseException as exc:
        return exc
    return None


def raises_exact(call: Callable[[], object], error_type: type[BaseException],
                 contains: str | None = None) -> bool:
    exc = capture(call)
    return (type(exc) is error_type
            and (contains is None or contains in str(exc)))


RequestTimeout = type("RequestTimeout", (ConnectionError,), {})
PacingViolation = type("PacingViolation", (ConnectionError,), {})
ConnectionLost = type("ConnectionLost", (ConnectionError,), {})
PlainConnectionLost = type("ConnectionLost", (object,), {})


class BoundedCycle(Exception):
    """A self-cycle that forces a damaged guard to terminate deterministically."""

    def __init__(self) -> None:
        super().__init__("cycle")
        self.cause_reads = 0

    def __getattribute__(self, name: str) -> object:
        if name == "__cause__":
            reads = object.__getattribute__(self, "cause_reads") + 1
            object.__setattr__(self, "cause_reads", reads)
            if reads >= 3:
                return ConnectionRefusedError("bounded terminal")
            return self
        return super().__getattribute__(name)


def with_chain(outer: BaseException, *, cause: BaseException | None = None,
               context: BaseException | None = None) -> BaseException:
    outer.__cause__ = cause
    outer.__context__ = context
    return outer


def win_error(number: int) -> OSError:
    value = OSError("windows socket")
    value.winerror = number
    return value


def probe_classifier(module: types.ModuleType, mode: str) -> bool:
    classify = module.hard_signal_kind
    if mode == "strings":
        return (classify("connection_refused") == "connection_refused"
                and classify("connection_lost") == "connection_lost"
                and classify("socket_reset") == "socket_reset"
                and classify("unknown") is None)
    if mode == "nonexception":
        return (classify(object()) is None
                and classify(PlainConnectionLost()) is None)
    if mode == "chain":
        wrapped = with_chain(Exception("outer"), cause=ConnectionLost("drop"))
        return classify(wrapped) == "connection_lost"
    if mode == "cycle":
        cycle = BoundedCycle()
        return classify(cycle) is None and cycle.cause_reads == 1
    if mode == "soft_name":
        for error_type in (RequestTimeout, PacingViolation):
            outer = with_chain(error_type("soft"),
                               cause=ConnectionLost("hidden hard"))
            if classify(outer) is not None:
                return False
        return True
    if mode == "timeout":
        outer = with_chain(TimeoutError("soft"),
                           cause=ConnectionLost("hidden hard"))
        return classify(outer) is None
    if mode == "lost":
        return classify(ConnectionLost("drop")) == "connection_lost"
    if mode == "refused":
        return classify(ConnectionRefusedError("no listener")) \
            == "connection_refused"
    if mode == "reset":
        return all(classify(exc("reset")) == "socket_reset" for exc in (
            ConnectionResetError, ConnectionAbortedError, BrokenPipeError))
    if mode == "generic_win":
        generic = Exception("not os error")
        generic.winerror = 10061
        return classify(generic) is None
    if mode == "win_refused":
        return classify(win_error(10061)) == "connection_refused"
    if mode == "win_reset":
        return (classify(win_error(10053)) == "socket_reset"
                and classify(win_error(10054)) == "socket_reset"
                and classify(win_error(99999)) is None)
    if mode == "errno_refused":
        refused = OSError("refused")
        refused.errno = errno.ECONNREFUSED
        return classify(refused) == "connection_refused"
    if mode == "errno_reset":
        reset_numbers = {
            errno.ECONNRESET, errno.ECONNABORTED,
            getattr(errno, "ENETRESET", 102),
        }
        return (all(classify(OSError(number, "reset")) == "socket_reset"
                    for number in reset_numbers)
                and classify(OSError(errno.ENOENT, "other")) is None)
    if mode == "precedence":
        outer = with_chain(
            Exception("outer"), cause=ConnectionRefusedError("cause"),
            context=ConnectionLost("context"))
        return classify(outer) == "connection_refused"
    if mode == "soft_outer":
        outer = with_chain(RequestTimeout("soft outer"),
                           cause=ConnectionRefusedError("hidden hard"))
        return classify(outer) is None
    return classify(Exception("generic")) is None


class AdapterDelegate:
    def __init__(self) -> None:
        object.__setattr__(self, "value", 17)
        object.__setattr__(self, "assigned", None)
        object.__setattr__(self, "access_error", ConnectionLost("access"))
        object.__setattr__(self, "set_error", ConnectionLost("set"))

    def method(self, *args: object, **kwargs: object) -> object:
        return args, kwargs

    def hard(self) -> None:
        raise ConnectionLost("call")

    def soft(self) -> None:
        raise RequestTimeout("soft")

    def __getattr__(self, name: str) -> object:
        if name == "broken":
            raise self.access_error
        raise AttributeError(name)

    def __setattr__(self, name: str, value: object) -> None:
        if name == "broken":
            raise self.set_error
        object.__setattr__(self, name, value)


def adapter_fixture(module: types.ModuleType, *, observer_raises: bool = False
                    ) -> tuple[object, AdapterDelegate, list[tuple[object, ...]]]:
    delegate = AdapterDelegate()
    seen: list[tuple[object, ...]] = []

    def observe(*args: object) -> None:
        seen.append(args)
        if observer_raises:
            raise RuntimeError("observer boom")

    return module.HardSignalAdapter(delegate, observe), delegate, seen


def probe_adapter(module: types.ModuleType, mode: str) -> bool:
    adapter, delegate, seen = adapter_fixture(
        module, observer_raises=(mode == "observer_error"))
    if mode == "init":
        return (adapter._delegate is delegate
                and callable(adapter._on_hard_signal))
    if mode == "factory":
        return adapter() is adapter
    if mode == "observe_soft":
        adapter._observe(RequestTimeout("soft"))
        return not seen
    if mode in {"observe_hard", "observer_error"}:
        error = ConnectionLost("hard")
        result = adapter._observe(error)
        return result is None and seen == [("connection_lost", error)]
    if mode == "value":
        return adapter.value == 17
    if mode == "access_error":
        error = capture(lambda: adapter.broken)
        return error is delegate.access_error \
            and seen == [("connection_lost", error)]
    if mode == "method":
        wrapped = adapter.method
        return (wrapped(1, flag=2) == ((1,), {"flag": 2})
                and wrapped.__name__ == "method" and not seen)
    if mode == "call_hard":
        error = capture(adapter.hard)
        return error is not None and type(error).__name__ == "ConnectionLost" \
            and seen == [("connection_lost", error)]
    if mode == "call_soft":
        error = capture(adapter.soft)
        return error is not None and type(error).__name__ == "RequestTimeout" \
            and not seen
    if mode == "local_set":
        adapter._local = "kept-local"
        return adapter._local == "kept-local" \
            and "_local" not in delegate.__dict__
    if mode == "delegate_set":
        adapter.assigned = 99
        return delegate.assigned == 99 and not seen
    error = capture(lambda: setattr(adapter, "broken", 99))
    return error is delegate.set_error \
        and seen == [("connection_lost", error)]


class Clock:
    def __init__(self, value: float = 0.0) -> None:
        self.value = float(value)
        self.calls = 0

    def __call__(self) -> float:
        self.calls += 1
        return self.value


class Provider:
    def __init__(self, *values: object) -> None:
        self.values = list(values)
        self.calls = 0

    def __call__(self) -> object:
        self.calls += 1
        if not self.values:
            return None
        value = self.values[min(self.calls - 1, len(self.values) - 1)]
        if isinstance(value, BaseException):
            raise value
        return value


def status(module: types.ModuleType, valid: bool = False,
           reason: str = "missing", ports: tuple[int, ...] = ()
           ) -> object:
    return module.MaintenanceStatus(valid, reason, ports)


def watchdog(module: types.ModuleType, *, ports: object = (2000, 3000),
             clock: object | None = None, provider: object = None,
             port_grace: object = 10, fleet_grace: object = 20,
             backoff: object = (2, 4, 8)) -> object:
    return module.HardDeathWatchdog(
        ports, clock=clock or Clock(), maintenance_provider=provider,
        port_grace_s=port_grace, fleet_grace_s=fleet_grace,
        probe_backoff_s=backoff)


def probe_init(module: types.ModuleType, mode: str) -> bool:
    if mode == "empty_backoff":
        return raises_exact(
            lambda: watchdog(module, backoff=()), ValueError,
            "positive values")
    if mode == "bad_backoff":
        return all(raises_exact(
            lambda values=values: watchdog(module, backoff=values), ValueError,
            "positive values") for values in ((0,), (-1,), (1, 0)))
    clock = Clock(12.5)
    provider = Provider(status(module, True, "valid", (2000,)))
    ports: object = ([3000, "2000", 3000]
                     if mode == "normalize" else [2000, 3000])
    backoff: object = (["2", 4, 8] if mode == "backoff" else (2, 4, 8))
    port_grace: object = -5 if mode == "grace" else "10"
    fleet_grace: object = -7 if mode == "grace" else "20"
    value = watchdog(
        module, ports=ports, clock=clock, provider=provider,
        port_grace=port_grace, fleet_grace=fleet_grace, backoff=backoff)
    if mode == "normalize":
        return value.ports == (3000, 2000) \
            and tuple(value._records) == (3000, 2000)
    if mode == "backoff":
        return value._backoff == (2.0, 4.0, 8.0)
    if mode == "grace":
        return value._port_grace_s == 0.0 \
            and value._fleet_grace_s == 0.0
    if mode == "clock":
        return value._clock is clock and value._last == 12.5 \
            and clock.calls == 1
    if mode == "provider":
        return value._maintenance_provider is provider
    if mode == "records":
        expected = {
            "state": module.HEALTHY, "grace": 0.0,
            "next_probe": float("inf"), "probe_index": 0,
            "reason": None, "interrupt_sequence": 0,
        }
        return (set(value._events) == {2000, 3000}
                and value._events[2000] is not value._events[3000]
                and value._records == {2000: expected, 3000: expected.copy()}
                and not any(event.is_set() for event in value._events.values()))
    if mode == "initial":
        return (value._fleet_grace == 0.0
                and value._maintenance
                == module.MaintenanceStatus(False, "missing"))
    return value.ports is value._ports and value.ports == (2000, 3000)


def probe_interrupt(module: types.ModuleType, mode: str) -> bool:
    value = watchdog(module)
    if mode == "event":
        return (value.interrupt_event("2000") is value._events[2000]
                and type(capture(lambda: value.interrupt_event(9999))) is KeyError)
    value._records[2000]["interrupt_sequence"] = 7
    return (value.interrupt_sequence("2000") == 7
            and type(capture(lambda: value.interrupt_sequence(9999))) is KeyError)


def probe_read_maintenance(module: types.ModuleType, mode: str) -> bool:
    if mode == "none":
        result = watchdog(module)._read_maintenance()
        return result == module.MaintenanceStatus(False, "missing")
    if mode == "error":
        provider = Provider(RuntimeError("boom"))
        result = watchdog(module, provider=provider)._read_maintenance()
        return (provider.calls == 1 and not result.valid
                and result.reason == "provider_error:RuntimeError")
    if mode == "status":
        expected = status(module, True, "valid", (2000,))
        provider = Provider(expected)
        result = watchdog(module, provider=provider)._read_maintenance()
        return result is expected and provider.calls == 1
    if mode == "dict":
        provider = Provider({"valid": 1, "reason": None,
                             "ports": ["3000", 2000]})
        result = watchdog(module, provider=provider)._read_maintenance()
        numeric = watchdog(module, provider=Provider(
            {"valid": 0, "reason": 123, "ports": []}))._read_maintenance()
        return (type(result) is module.MaintenanceStatus
                and result.valid is True and result.reason == "invalid"
                and result.ports == (3000, 2000)
                and numeric.valid is False and numeric.reason == "123")
    provider = Provider(object())
    result = watchdog(module, provider=provider)._read_maintenance()
    return (not result.valid and result.reason == "invalid_provider_result"
            and provider.calls == 1)


def set_suspect(module: types.ModuleType, value: object,
                ports: tuple[int, ...] = (2000, 3000)) -> None:
    for port in ports:
        record = value._records[port]
        record.update({"state": module.SUSPECT, "grace": 0.0,
                       "next_probe": 2.0, "probe_index": 0,
                       "reason": "connection_lost"})


def probe_advance(module: types.ModuleType, mode: str) -> bool:
    if mode == "backwards":
        clock = Clock(10)
        value = watchdog(module, clock=clock)
        set_suspect(module, value, (2000,))
        healthy, active = value._advance_locked("5")
        return (value._last == 10 and value._records[2000]["grace"] == 0.0
                and healthy == [3000] and active is False)
    if mode == "reread":
        provided = status(module, True, "provider-state", (2000,))
        provider = Provider(provided)
        value = watchdog(module, provider=provider)
        value._advance_locked(1)
        value._advance_locked(2)
        return provider.calls == 2 and value._maintenance is provided
    if mode in {"covered", "invalid_covered", "disjoint"}:
        ports = ((2000, 3000) if mode != "disjoint" else (9999,))
        valid = mode != "invalid_covered"
        provider = Provider(status(module, valid, "fixture", ports))
        value = watchdog(module, provider=provider)
        set_suspect(module, value)
        healthy, active = value._advance_locked(5)
        expected_grace = ({2000: 0.0, 3000: 0.0}
                          if mode == "covered" else {2000: 5.0, 3000: 5.0})
        return (healthy == [] and active is (mode == "covered")
                and {p: value._records[p]["grace"] for p in value.ports}
                == expected_grace
                and value._fleet_grace
                == (0.0 if mode == "covered" else 5.0))
    if mode == "healthy":
        value = watchdog(module)
        set_suspect(module, value, (3000,))
        value._fleet_grace = 9.0
        healthy, active = value._advance_locked(5)
        return (healthy == [2000] and active is False
                and value._fleet_grace == 0.0
                and value._records[2000]["grace"] == 0.0)
    if mode == "dead":
        value = watchdog(module, port_grace=5)
        set_suspect(module, value, (2000,))
        value._advance_locked(5)
        return value._records[2000]["state"] == module.DEAD \
            and value._records[2000]["grace"] == 5.0
    value = watchdog(module)
    set_suspect(module, value)
    healthy, active = value._advance_locked(5)
    return (healthy == [] and active is False and value._fleet_grace == 5.0
            and all(value._records[p]["grace"] == 5.0
                    for p in value.ports))


def probe_hard_signal(module: types.ModuleType, mode: str) -> bool:
    provider = Provider(status(module), status(module))
    clock = Clock(5)
    value = watchdog(module, clock=clock, provider=provider)
    if mode == "soft":
        before = {p: dict(r) for p, r in value._records.items()}
        return (value.hard_signal(2000, RequestTimeout("soft")) is False
                and value._records == before and provider.calls == 0)
    if mode == "unknown":
        error = capture(lambda: value.hard_signal(9999, "connection_lost", now=5))
        return type(error) is KeyError
    if mode == "clock":
        result = value.hard_signal("2000", "connection_lost")
        return result is True and clock.calls == 2
    first = value.hard_signal(2000, "connection_lost", now=5)
    record = value._records[2000]
    if mode == "first":
        return (first is True and record == {
                    "state": module.SUSPECT, "grace": 0.0,
                    "next_probe": 7.0, "probe_index": 0,
                    "reason": "connection_lost", "interrupt_sequence": 1}
                and value._events[2000].is_set() and provider.calls == 2)
    record.update({"grace": 3.0, "probe_index": 2, "next_probe": 99.0})
    provider.calls = 0
    second = value.hard_signal(2000, "socket_reset", now=5)
    return (second is True and record["state"] == module.SUSPECT
            and record["grace"] == 3.0 and record["probe_index"] == 2
            and record["next_probe"] == 99.0
            and record["reason"] == "socket_reset"
            and record["interrupt_sequence"] == 1
            and value._events[2000].is_set() and provider.calls == 2)


def probe_due(module: types.ModuleType, mode: str) -> bool:
    clock = Clock(0)
    value = watchdog(module, clock=clock)
    value.hard_signal(2000, "connection_lost", now=0)
    if mode == "clock":
        clock.value = 2
        before = clock.calls
        return value.due_probes() == [2000] and clock.calls == before + 1
    if mode == "state":
        value._records[3000]["next_probe"] = 0
        return value.due_probes(now=2) == [2000]
    return (value.due_probes(now=1) == []
            and value.due_probes(now=2) == [2000])


def probe_result(module: types.ModuleType, mode: str) -> bool:
    provider = Provider(status(module), status(module), status(module))
    clock = Clock(0)
    value = watchdog(
        module, clock=clock, provider=provider,
        port_grace=100 if mode == "down" else 10)
    value.hard_signal(2000, "connection_lost", now=0)
    provider.calls = 0
    if mode == "unknown":
        return type(capture(
            lambda: value.probe_result(9999, True, now=1))) is KeyError
    if mode == "clock":
        clock.value = 1
        before = clock.calls
        value.probe_result("2000", True)
        return clock.calls == before + 1
    if mode == "up":
        value._records[2000].update({
            "grace": 7.0, "next_probe": 99.0, "probe_index": 2,
            "reason": "socket_reset"})
        result = value.probe_result(2000, True, now=1)
        record = value._records[2000]
        return (result == module.HEALTHY and record == {
                    "state": module.HEALTHY, "grace": 0.0,
                    "next_probe": float("inf"), "probe_index": 0,
                    "reason": None, "interrupt_sequence": 1}
                and not value._events[2000].is_set() and provider.calls == 2)
    if mode == "dead":
        value._records[2000]["grace"] = 10.0
        advances: list[float] = []
        value._advance_locked = lambda stamp: advances.append(stamp)
        result = value.probe_result(2000, False, now=1)
        return result == module.DEAD and advances == [1.0, 1.0]
    first = value.probe_result(2000, False, now=2)
    one = dict(value._records[2000])
    second = value.probe_result(2000, False, now=6)
    two = dict(value._records[2000])
    third = value.probe_result(2000, False, now=14)
    three = dict(value._records[2000])
    return (first == second == third == module.SUSPECT
            and (one["probe_index"], one["next_probe"]) == (1, 6.0)
            and (two["probe_index"], two["next_probe"]) == (2, 14.0)
            and (three["probe_index"], three["next_probe"]) == (2, 22.0))


def finalize_fixture(module: types.ModuleType, mode: str) -> object:
    provider = (Provider(status(module, True, "valid", (2000, 3000)))
                if mode == "maintenance" else None)
    value = watchdog(module, provider=provider, fleet_grace=10)
    set_suspect(module, value)
    value._fleet_grace = 10.0 if mode != "below" else 9.0
    if mode == "healthy":
        value._records[2000]["state"] = module.HEALTHY
    return value


def probe_snapshot(module: types.ModuleType, mode: str) -> bool:
    if mode in {"final", "healthy", "maintenance", "below"}:
        value = finalize_fixture(module, mode)
        value._advance_locked = lambda _stamp: (
            [2000] if mode == "healthy" else [], mode == "maintenance")
        result = value.snapshot(now=0)
        return result["finalize"] is (mode == "final")
    provider = Provider(status(module, True, "planned", (2000,)))
    value = watchdog(module, provider=provider)
    value.hard_signal(2000, "socket_reset", now=0)
    result = value.snapshot(now=3)
    return result == {
        "ports": {
            2000: {"state": module.SUSPECT, "grace_seconds": 0.0,
                   "reason": "socket_reset"},
            3000: {"state": module.HEALTHY, "grace_seconds": 0.0,
                   "reason": None},
        },
        "healthy_ports": [3000],
        "holding": False,
        "fleet_grace_seconds": 0.0,
        "maintenance_active": True,
        "maintenance_reason": "planned",
        "finalize": False,
    }


def probe_wrappers(module: types.ModuleType, mode: str) -> bool:
    value = finalize_fixture(
        module, "below" if mode in {"holding", "final"} else "final")
    if mode == "state":
        return (value.state("2000", now=0) == module.SUSPECT
                and type(capture(lambda: value.state(9999, now=0))) is KeyError)
    if mode == "healthy":
        value._records[2000]["state"] = module.HEALTHY
        return value.healthy_ports(now=0) == [2000]
    if mode == "holding":
        return value.holding(now=0) is True
    return value.should_finalize(now=0) is False


def probe_default_contract(module: types.ModuleType) -> bool:
    defaults = module.HardDeathWatchdog.__init__.__kwdefaults__
    return (defaults["clock"] is module.time.monotonic
            and defaults["maintenance_provider"] is None
            and defaults["port_grace_s"] == module.PORT_GRACE_S == 150.0
            and defaults["fleet_grace_s"] == module.FLEET_GRACE_S == 150.0
            and defaults["probe_backoff_s"]
            == module.PROBE_BACKOFF_S == (5.0, 15.0, 30.0, 60.0)
            and (module.HEALTHY, module.SUSPECT, module.DEAD)
            == ("HEALTHY", "SUSPECT", "DEAD"))


def probe_lease_constant_contract(module: types.ModuleType) -> bool:
    return (
        module.MAINT_MARGIN_S == 120.0
        and module.MAINTENANCE_NAME == "fleet_maintenance.json"
        and module.MAINTENANCE_SCHEMA == 1
        and module.MAINTENANCE_MAX_BYTES == 4096
        and module.MAINTENANCE_MAX_PORTS == 64
        and module.MAINTENANCE_MAX_BUDGET_S == 24 * 60 * 60
        and module._LEASE_RE.pattern == r"[0-9a-f]{32}\Z"
    )


def probe_classifier_constant_contract(module: types.ModuleType) -> bool:
    return (
        module._HARD_TOKENS == frozenset({
            "connection_refused", "connection_lost", "socket_reset",
        })
        and module._SOFT_CLASS_NAMES == frozenset({
            "RequestTimeout", "PacingViolation",
        })
        and module._HARD_ERRNOS == frozenset({
            errno.ECONNREFUSED, errno.ECONNRESET, errno.ECONNABORTED,
            getattr(errno, "ENETRESET", 102),
        })
        and module._HARD_WINERRORS == frozenset({10053, 10054, 10061})
    )


def probe_unknown_port_contract(module: types.ModuleType) -> bool:
    value = watchdog(module)
    calls = (
        lambda: value.interrupt_event(9999),
        lambda: value.interrupt_sequence(9999),
        lambda: value.hard_signal(9999, "connection_lost", now=0),
        lambda: value.probe_result(9999, True, now=0),
        lambda: value.state(9999, now=0),
    )
    return all(type(capture(call)) is KeyError for call in calls)


def fences() -> tuple[Fence, ...]:
    cases = [
        f("C1", "hard_signal_kind", "recognizes direct string signals",
          "if isinstance(value, str):", "if False:",
          p(probe_classifier, "strings")),
        f("C2", "hard_signal_kind", "allows only controlled hard string tokens",
          "return value if value in _HARD_TOKENS else None",
          "return value", p(probe_classifier, "strings")),
        f("C3", "hard_signal_kind", "starts chain traversal with an empty seen set",
          "seen = set()\n    while isinstance(current, BaseException)",
          "seen = {id(value)}\n    while isinstance(current, BaseException)",
          p(probe_classifier, "chain")),
        f("C4", "hard_signal_kind", "walks exceptions only",
          "while isinstance(current, BaseException) and id(current) not in seen:",
          "while id(current) not in seen:", p(probe_classifier, "nonexception")),
        f("C5", "hard_signal_kind", "breaks cause and context cycles",
          "while isinstance(current, BaseException) and id(current) not in seen:",
          "while isinstance(current, BaseException):",
          p(probe_classifier, "cycle")),
        f("C6", "hard_signal_kind", "records each traversed exception identity",
          "seen.add(id(current))", "None", p(probe_classifier, "cycle")),
        f("C7", "hard_signal_kind", "classifies exception types by exact name",
          "name = type(current).__name__", 'name = "Other"',
          p(probe_classifier, "lost")),
        f("C8", "hard_signal_kind", "stops on soft exception class names",
          "if name in _SOFT_CLASS_NAMES or isinstance(current, TimeoutError):",
          "if False or isinstance(current, TimeoutError):",
          p(probe_classifier, "soft_name")),
        f("C9", "hard_signal_kind", "stops independently on TimeoutError",
          "if name in _SOFT_CLASS_NAMES or isinstance(current, TimeoutError):",
          "if name in _SOFT_CLASS_NAMES or False:",
          p(probe_classifier, "timeout")),
        f("C10", "hard_signal_kind", "recognizes ConnectionLost by name",
          'if name == "ConnectionLost":', "if False:",
          p(probe_classifier, "lost")),
        f("C11", "hard_signal_kind", "recognizes ConnectionRefusedError",
          "if isinstance(current, ConnectionRefusedError):", "if False:",
          p(probe_classifier, "refused")),
        f("C12", "hard_signal_kind", "maps reset, abort, and broken-pipe errors",
          "if isinstance(current, (ConnectionResetError, ConnectionAbortedError,\n                                BrokenPipeError)):",
          "if False:", p(probe_classifier, "reset")),
        f("C13", "hard_signal_kind", "gates numeric socket codes on OSError",
          "if isinstance(current, OSError):", "if True:",
          p(probe_classifier, "generic_win")),
        f("C14", "hard_signal_kind", "allows only controlled Windows socket codes",
          'if getattr(current, "winerror", None) in _HARD_WINERRORS:',
          "if False:", p(probe_classifier, "win_reset")),
        f("C15", "hard_signal_kind", "distinguishes refused from reset winerrors",
          "if current.winerror == 10061 else \"socket_reset\")",
          "if current.winerror != 10061 else \"socket_reset\")",
          p(probe_classifier, "win_refused")),
        f("C16", "hard_signal_kind", "allows only controlled errno socket codes",
          'if getattr(current, "errno", None) in _HARD_ERRNOS:',
          "if False:", p(probe_classifier, "errno_reset")),
        f("C17", "hard_signal_kind", "distinguishes refused from reset errno values",
          "if current.errno == errno.ECONNREFUSED else \"socket_reset\")",
          "if current.errno != errno.ECONNREFUSED else \"socket_reset\")",
          p(probe_classifier, "errno_refused")),
        f("C18", "hard_signal_kind", "prefers explicit cause over implicit context",
          "current = current.__cause__ or current.__context__",
          "current = current.__context__ or current.__cause__",
          p(probe_classifier, "precedence")),
        f("C19", "hard_signal_kind", "returns soft after exhausting the chain",
          "current = current.__cause__ or current.__context__\n    return None\n\n\nclass HardSignalAdapter",
          "current = current.__cause__ or current.__context__\n    return \"connection_lost\"\n\n\nclass HardSignalAdapter",
          p(probe_classifier, "generic")),

        f("A1", "HardSignalAdapter.__init__", "stores the delegate locally",
          'object.__setattr__(self, "_delegate", delegate)',
          'object.__setattr__(self, "_delegate", None)',
          p(probe_adapter, "init")),
        f("A2", "HardSignalAdapter.__init__", "stores the observer locally",
          'object.__setattr__(self, "_on_hard_signal", on_hard_signal)',
          'object.__setattr__(self, "_on_hard_signal", None)',
          p(probe_adapter, "observe_hard")),
        f("A3", "HardSignalAdapter.__call__", "preserves adapter-factory shape",
          "return self\n\n    def _observe", "return self._delegate\n\n    def _observe",
          p(probe_adapter, "factory")),
        f("A4", "HardSignalAdapter._observe", "classifies the observed exception",
          "kind = hard_signal_kind(exc)", "kind = None",
          p(probe_adapter, "observe_hard")),
        f("A5", "HardSignalAdapter._observe", "notifies only for a hard kind",
          "if kind is not None:", "if True:",
          p(probe_adapter, "observe_soft")),
        f("A6", "HardSignalAdapter._observe", "passes exact kind and exception",
          "self._on_hard_signal(kind, exc)",
          "self._on_hard_signal(exc, kind)", p(probe_adapter, "observe_hard")),
        f("A7", "HardSignalAdapter._observe", "swallows observer failures",
          "except Exception:  # noqa: BLE001 - observation cannot mask failure",
          "except ValueError:  # noqa: BLE001 - observation cannot mask failure",
          p(probe_adapter, "observer_error")),
        f("A8", "HardSignalAdapter.__getattr__", "reads attributes from the delegate",
          "target = getattr(self._delegate, name)",
          "target = object.__getattribute__(self, name)",
          p(probe_adapter, "value")),
        f("A9", "HardSignalAdapter.__getattr__", "observes attribute-access failure",
          "except Exception as exc:\n            self._observe(exc)\n            raise\n        if not callable(target):",
          "except Exception as exc:\n            None\n            raise\n        if not callable(target):",
          p(probe_adapter, "access_error")),
        f("A10", "HardSignalAdapter.__getattr__", "re-raises attribute-access failure",
          "except Exception as exc:\n            self._observe(exc)\n            raise\n        if not callable(target):",
          "except Exception as exc:\n            self._observe(exc)\n            return None\n        if not callable(target):",
          p(probe_adapter, "access_error")),
        f("A11", "HardSignalAdapter.__getattr__", "passes non-callable attributes through",
          "if not callable(target):\n            return target",
          "if True:\n            return target", p(probe_adapter, "call_hard")),
        f("A12", "HardSignalAdapter.__getattr__", "preserves callable metadata",
          "@functools.wraps(target)\n        def observed",
          "def observed", p(probe_adapter, "method")),
        f("A13", "HardSignalAdapter.__getattr__", "forwards callable arguments and return",
          "return target(*args, **kwargs)", "return None",
          p(probe_adapter, "method")),
        f("A14", "HardSignalAdapter.__getattr__", "observes delegated call failure",
          "except Exception as exc:\n                self._observe(exc)\n                raise\n        return observed",
          "except Exception as exc:\n                None\n                raise\n        return observed",
          p(probe_adapter, "call_hard")),
        f("A15", "HardSignalAdapter.__getattr__", "re-raises delegated call failure",
          "except Exception as exc:\n                self._observe(exc)\n                raise\n        return observed",
          "except Exception as exc:\n                self._observe(exc)\n                return None\n        return observed",
          p(probe_adapter, "call_hard")),
        f("A16", "HardSignalAdapter.__setattr__", "keeps underscore attributes local",
          'if name.startswith("_"):', "if False:",
          p(probe_adapter, "local_set")),
        f("A17", "HardSignalAdapter.__setattr__", "writes local attributes on the wrapper",
          'if name.startswith("_"):\n            object.__setattr__(self, name, value)',
          'if name.startswith("_"):\n            setattr(self._delegate, name, value)',
          p(probe_adapter, "local_set")),
        f("A18", "HardSignalAdapter.__setattr__", "returns after a local write",
          "object.__setattr__(self, name, value)\n            return\n        try:",
          "object.__setattr__(self, name, value)\n            None\n        try:",
          p(probe_adapter, "local_set")),
        f("A19", "HardSignalAdapter.__setattr__", "forwards public writes to delegate",
          "setattr(self._delegate, name, value)",
          "object.__setattr__(self, name, value)",
          p(probe_adapter, "delegate_set")),
        f("A20", "HardSignalAdapter.__setattr__", "observes delegated set failure",
          "except Exception as exc:\n            self._observe(exc)\n            raise\n\n\nclass HardDeathWatchdog",
          "except Exception as exc:\n            None\n            raise\n\n\nclass HardDeathWatchdog",
          p(probe_adapter, "set_error")),
        f("A21", "HardSignalAdapter.__setattr__", "re-raises delegated set failure",
          "except Exception as exc:\n            self._observe(exc)\n            raise\n\n\nclass HardDeathWatchdog",
          "except Exception as exc:\n            self._observe(exc)\n            return None\n\n\nclass HardDeathWatchdog",
          p(probe_adapter, "set_error")),

        f("I1", "HardDeathWatchdog.__init__", "delegates port normalization",
          "normalized = _normalize_ports(ports)\n        backoff = tuple(float(value) for value in probe_backoff_s)",
          "normalized = tuple(ports)\n        backoff = tuple(float(value) for value in probe_backoff_s)",
          p(probe_init, "normalize")),
        f("I2", "HardDeathWatchdog.__init__", "normalizes backoff values to float",
          "backoff = tuple(float(value) for value in probe_backoff_s)",
          "backoff = tuple(probe_backoff_s)", p(probe_init, "backoff")),
        f("I3", "HardDeathWatchdog.__init__", "rejects an empty backoff schedule",
          "if not backoff or any(value <= 0 for value in backoff):",
          "if False or any(value <= 0 for value in backoff):",
          p(probe_init, "empty_backoff")),
        f("I4", "HardDeathWatchdog.__init__", "rejects every nonpositive backoff",
          "if not backoff or any(value <= 0 for value in backoff):",
          "if not backoff or False:", p(probe_init, "bad_backoff")),
        f("I5", "HardDeathWatchdog.__init__", "clamps port grace at zero",
          "self._port_grace_s = max(0.0, float(port_grace_s))",
          "self._port_grace_s = float(port_grace_s)", p(probe_init, "grace")),
        f("I6", "HardDeathWatchdog.__init__", "clamps fleet grace at zero",
          "self._fleet_grace_s = max(0.0, float(fleet_grace_s))",
          "self._fleet_grace_s = float(fleet_grace_s)", p(probe_init, "grace")),
        f("I7", "HardDeathWatchdog.__init__", "initializes every port healthy",
          'port: {"state": HEALTHY, "grace": 0.0,',
          'port: {"state": SUSPECT, "grace": 0.0,',
          p(probe_init, "records")),
        f("I8", "HardDeathWatchdog.__init__", "samples and stores the initial clock",
          "self._last = float(clock())", "self._last = 0.0",
          p(probe_init, "clock")),
        f("I9", "HardDeathWatchdog.__init__", "starts with missing maintenance",
          'self._maintenance = MaintenanceStatus(False, "missing")',
          'self._maintenance = MaintenanceStatus(True, "valid")',
          p(probe_init, "initial")),
        f("I10", "HardDeathWatchdog.ports", "returns the immutable normalized ports",
          "return self._ports\n\n    def interrupt_event",
          "return tuple(reversed(self._ports))\n\n    def interrupt_event",
          p(probe_init, "ports")),

        f("E1", "interrupt_event", "coerces and indexes the requested port",
          "return self._events[int(port)]", "return self._events[port]",
          p(probe_interrupt, "event")),
        f("S1", "interrupt_sequence", "coerces the sequence port key",
          'return self._records[int(port)]["interrupt_sequence"]',
          'return self._records[port]["interrupt_sequence"]',
          p(probe_interrupt, "sequence")),
        f("S2", "interrupt_sequence", "returns the monotonic interrupt generation",
          'return self._records[int(port)]["interrupt_sequence"]',
          "return 0", p(probe_interrupt, "sequence")),

        f("R1", "_read_maintenance", "reports missing when no provider exists",
          "if self._maintenance_provider is None:", "if False:",
          p(probe_read_maintenance, "none")),
        f("R2", "_read_maintenance", "invokes the injected provider",
          "value = self._maintenance_provider()", "value = None",
          p(probe_read_maintenance, "status")),
        f("R3", "_read_maintenance", "fails closed on every provider exception",
          "except Exception as exc:  # noqa: BLE001 - fail closed",
          "except ValueError as exc:  # noqa: BLE001 - fail closed",
          p(probe_read_maintenance, "error")),
        f("R4", "_read_maintenance", "passes MaintenanceStatus through unchanged",
          "if isinstance(value, MaintenanceStatus):\n            return value",
          "if False:\n            return value", p(probe_read_maintenance, "status")),
        f("R5", "_read_maintenance", "coerces only dict provider results",
          "if isinstance(value, dict):", "if False:",
          p(probe_read_maintenance, "dict")),
        f("R6", "_read_maintenance", "canonicalizes dict validity to bool",
          'bool(value.get("valid"))', 'value.get("valid")',
          p(probe_read_maintenance, "dict")),
        f("R7", "_read_maintenance", "canonicalizes missing dict reason",
          'str(value.get("reason") or "invalid")', '"mutated"',
          p(probe_read_maintenance, "dict")),
        f("R8", "_read_maintenance", "canonicalizes dict ports to integer tuple",
          'tuple(int(p) for p in value.get("ports") or ())',
          'tuple(value.get("ports") or ())', p(probe_read_maintenance, "dict")),
        f("R9", "_read_maintenance", "rejects every other provider result",
          'return MaintenanceStatus(False, "invalid_provider_result")',
          'return MaintenanceStatus(False, "missing")',
          p(probe_read_maintenance, "invalid")),

        f("V1", "_advance_locked", "coerces the supplied monotonic stamp",
          "now = float(now)", "now = now", p(probe_advance, "backwards")),
        f("V2", "_advance_locked", "clamps backwards elapsed time to zero",
          "elapsed = max(0.0, now - self._last)",
          "elapsed = now - self._last", p(probe_advance, "backwards")),
        f("V3", "_advance_locked", "never rewinds the accounting clock",
          "self._last = max(self._last, now)", "self._last = now",
          p(probe_advance, "backwards")),
        f("V4", "_advance_locked", "re-reads and stores maintenance every advance",
          "maintenance = self._read_maintenance()\n        self._maintenance = maintenance",
          "maintenance = self._maintenance\n        self._maintenance = maintenance",
          p(probe_advance, "reread")),
        f("V5", "_advance_locked", "covers ports only under valid maintenance",
          "covered = set(maintenance.ports) if maintenance.valid else set()",
          "covered = set(maintenance.ports)",
          p(probe_advance, "invalid_covered")),
        f("V6", "_advance_locked", "uses the maintenance port set for coverage",
          "covered = set(maintenance.ports) if maintenance.valid else set()",
          "covered = set()", p(probe_advance, "covered")),
        f("V7", "_advance_locked", "skips grace accounting for healthy ports",
          'for port, record in self._records.items():\n            if record["state"] == HEALTHY:\n                continue',
          'for port, record in self._records.items():\n            if False:\n                continue',
          p(probe_advance, "healthy")),
        f("V8", "_advance_locked", "accrues port grace only when uncovered",
          "if port not in covered:\n                record[\"grace\"] += elapsed",
          "if False:\n                record[\"grace\"] += elapsed",
          p(probe_advance, "fleet")),
        f("V9", "_advance_locked", "marks a port dead at exact grace",
          'if record["grace"] >= self._port_grace_s:\n                record["state"] = DEAD\n        healthy =',
          'if False:\n                record["state"] = DEAD\n        healthy =',
          p(probe_advance, "dead")),
        f("V10", "_advance_locked", "derives the exact healthy-port list",
          'healthy = [p for p, r in self._records.items()\n                   if r["state"] == HEALTHY]',
          'healthy = [p for p, r in self._records.items()\n                   if r["state"] != HEALTHY]',
          p(probe_advance, "healthy")),
        f("V11", "_advance_locked", "requires valid fleet maintenance",
          "maintenance.valid and set(self._ports) & set(maintenance.ports)",
          "True and set(self._ports) & set(maintenance.ports)",
          p(probe_advance, "invalid_covered")),
        f("V12", "_advance_locked", "requires maintenance overlap with fleet ports",
          "maintenance.valid and set(self._ports) & set(maintenance.ports)",
          "maintenance.valid and set()", p(probe_advance, "covered")),
        f("V13", "_advance_locked", "accrues fleet grace only outside maintenance",
          "elif not maintenance_active:\n            self._fleet_grace += elapsed",
          "elif False:\n            self._fleet_grace += elapsed",
          p(probe_advance, "fleet")),
        f("V14", "_advance_locked", "resets fleet grace when any port is healthy",
          "if healthy:\n            self._fleet_grace = 0.0",
          "if False:\n            self._fleet_grace = 0.0",
          p(probe_advance, "healthy")),

        f("H1", "hard_signal", "classifies the candidate hard signal",
          "kind = hard_signal_kind(value)\n        if kind is None:",
          "kind = None\n        if kind is None:", p(probe_hard_signal, "first")),
        f("H2", "hard_signal", "returns false without state change for soft values",
          "if kind is None:\n            return False",
          "if False:\n            return False", p(probe_hard_signal, "soft")),
        f("H3", "hard_signal", "coerces registered port keys to int",
          "def hard_signal(self, port, value, *, now=None) -> bool:\n        kind = hard_signal_kind(value)\n        if kind is None:\n            return False\n        port = int(port)",
          "def hard_signal(self, port, value, *, now=None) -> bool:\n        kind = hard_signal_kind(value)\n        if kind is None:\n            return False\n        port = port",
          p(probe_hard_signal, "clock")),
        f("H4", "hard_signal", "uses clock only when no explicit stamp exists",
          "stamp = float(self._clock() if now is None else now)\n            self._advance_locked(stamp)\n            record = self._records[port]\n            if record[\"state\"] == HEALTHY:",
          "stamp = float(now)\n            self._advance_locked(stamp)\n            record = self._records[port]\n            if record[\"state\"] == HEALTHY:",
          p(probe_hard_signal, "clock")),
        f("H5", "hard_signal", "advances accounting before the transition",
          "stamp = float(self._clock() if now is None else now)\n            self._advance_locked(stamp)\n            record = self._records[port]\n            if record[\"state\"] == HEALTHY:",
          "stamp = float(self._clock() if now is None else now)\n            None\n            record = self._records[port]\n            if record[\"state\"] == HEALTHY:",
          p(probe_hard_signal, "first")),
        f("H6", "hard_signal", "initializes suspicion only from healthy state",
          'if record["state"] == HEALTHY:\n                record["state"] = SUSPECT',
          'if True:\n                record["state"] = SUSPECT',
          p(probe_hard_signal, "repeat")),
        f("H7", "hard_signal", "sets the first hard transition to SUSPECT",
          'record["state"] = SUSPECT\n                record["grace"] = 0.0',
          'record["state"] = HEALTHY\n                record["grace"] = 0.0',
          p(probe_hard_signal, "first")),
        f("H8", "hard_signal", "schedules the first named probe backoff",
          'record["next_probe"] = stamp + self._backoff[0]',
          'record["next_probe"] = stamp', p(probe_hard_signal, "first")),
        f("H9", "hard_signal", "records the latest hard reason",
          'record["reason"] = kind', "None", p(probe_hard_signal, "repeat")),
        f("H10", "hard_signal", "increments interrupt generation only once per set",
          "if not event.is_set():\n                record[\"interrupt_sequence\"] += 1",
          "if True:\n                record[\"interrupt_sequence\"] += 1",
          p(probe_hard_signal, "repeat")),
        f("H11", "hard_signal", "sets the per-port interrupt event",
          'record["interrupt_sequence"] += 1\n                event.set()',
          'record["interrupt_sequence"] += 1\n                None',
          p(probe_hard_signal, "first")),
        f("H12", "hard_signal", "advances accounting after the transition",
          "event.set()\n            self._advance_locked(stamp)\n        return True",
          "event.set()\n            None\n        return True",
          p(probe_hard_signal, "first")),

        f("D1", "due_probes", "uses the injected clock when now is absent",
          "stamp = float(self._clock() if now is None else now)\n            self._advance_locked(stamp)\n            return [port for port in self._ports",
          "stamp = float(now)\n            self._advance_locked(stamp)\n            return [port for port in self._ports",
          p(probe_due, "clock")),
        f("D2", "due_probes", "excludes healthy ports even if their time is due",
          'if self._records[port]["state"] != HEALTHY\n                    and self._records[port]["next_probe"] <= stamp]',
          'if self._records[port]["state"] == HEALTHY\n                    and self._records[port]["next_probe"] <= stamp]',
          p(probe_due, "state")),
        f("D3", "due_probes", "includes a probe at the exact scheduled stamp",
          'and self._records[port]["next_probe"] <= stamp]',
          'and self._records[port]["next_probe"] < stamp]',
          p(probe_due, "time")),

        f("P1", "probe_result", "coerces registered probe-result ports to int",
          "def probe_result(self, port, up, *, now=None):\n        port = int(port)",
          "def probe_result(self, port, up, *, now=None):\n        port = port",
          p(probe_result, "clock")),
        f("P2", "probe_result", "uses clock only when no explicit probe stamp exists",
          "stamp = float(self._clock() if now is None else now)\n            self._advance_locked(stamp)\n            record = self._records[port]\n            if up:",
          "stamp = float(now)\n            self._advance_locked(stamp)\n            record = self._records[port]\n            if up:",
          p(probe_result, "clock")),
        f("P3", "probe_result", "advances accounting before applying a probe",
          "stamp = float(self._clock() if now is None else now)\n            self._advance_locked(stamp)\n            record = self._records[port]\n            if up:",
          "stamp = float(self._clock() if now is None else now)\n            None\n            record = self._records[port]\n            if up:",
          p(probe_result, "up")),
        f("P4", "probe_result", "fully restores the healthy record on recovery",
          'record.update({"state": HEALTHY, "grace": 0.0,',
          'record.update({"state": SUSPECT, "grace": 0.0,',
          p(probe_result, "up")),
        f("P5", "probe_result", "clears the interrupt event on recovery",
          "self._events[port].clear()", "None", p(probe_result, "up")),
        f("P6", "probe_result", "increments the failed-probe backoff index",
          'record["probe_index"] + 1', 'record["probe_index"] + 0',
          p(probe_result, "down")),
        f("P7", "probe_result", "caps failed-probe escalation at last backoff",
          'index = min(record["probe_index"] + 1,\n                            len(self._backoff) - 1)',
          'index = record["probe_index"] + 1', p(probe_result, "down")),
        f("P8", "probe_result", "reschedules from stamp plus selected backoff",
          'record["next_probe"] = stamp + self._backoff[index]',
          'record["next_probe"] = stamp', p(probe_result, "down")),
        f("P9", "probe_result", "marks down probe dead after exhausted grace",
          'if record["grace"] >= self._port_grace_s:\n                    record["state"] = DEAD',
          'if False:\n                    record["state"] = DEAD',
          p(probe_result, "dead")),
        f("P10", "probe_result", "advances accounting after applying a probe",
          'record["state"] = DEAD\n            self._advance_locked(stamp)\n            return record["state"]',
          'record["state"] = DEAD\n            None\n            return record["state"]',
          p(probe_result, "up")),

        f("N1", "snapshot", "advances and uses current healthy/maintenance state",
          "healthy, maintenance_active = self._advance_locked(stamp)",
          "healthy, maintenance_active = ([], False)",
          p(probe_snapshot, "normal")),
        f("N2", "snapshot", "publishes exact per-port state, grace, and reason",
          'port: {"state": record["state"],\n                           "grace_seconds": record["grace"],',
          'port: {"state": HEALTHY,\n                           "grace_seconds": record["grace"],',
          p(probe_snapshot, "normal")),
        f("N3", "snapshot", "finalize requires no healthy port",
          "\"finalize\": (not healthy and not maintenance_active",
          '"finalize": (True and not maintenance_active',
          p(probe_snapshot, "healthy")),
        f("N4", "snapshot", "finalize requires no maintenance overlap",
          "not healthy and not maintenance_active\n                             and self._fleet_grace",
          "not healthy and True\n                             and self._fleet_grace",
          p(probe_snapshot, "maintenance")),
        f("N5", "snapshot", "finalize requires fleet grace threshold",
          "self._fleet_grace >= self._fleet_grace_s",
          "self._fleet_grace >= 0.0", p(probe_snapshot, "below")),

        f("W1", "state", "coerces and returns the requested port state",
          'return self.snapshot(now=now)["ports"][int(port)]["state"]',
          'return self.snapshot(now=now)["ports"][port]["state"]',
          p(probe_wrappers, "state")),
        f("W2", "healthy_ports", "returns the snapshot healthy-port list",
          'return self.snapshot(now=now)["healthy_ports"]',
          'return self.snapshot(now=now)["ports"]',
          p(probe_wrappers, "healthy")),
        f("W3", "holding", "selects the snapshot holding decision",
          'return bool(self.snapshot(now=now)["holding"])',
          'return bool(self.snapshot(now=now)["finalize"])',
          p(probe_wrappers, "holding")),
        f("W4", "should_finalize", "selects the snapshot finalize decision",
          'return bool(self.snapshot(now=now)["finalize"])',
          'return bool(self.snapshot(now=now)["holding"])',
          p(probe_wrappers, "final")),
    ]
    return tuple(cases)


def redundancy_cases() -> tuple[Redundancy, ...]:
    return (
        Redundancy(
            "RR1",
            "snapshot list(healthy) is redundant because _advance_locked returns a list",
            mu('"healthy_ports": list(healthy),',
               '"healthy_ports": healthy,'),
            p(probe_snapshot, "normal"),
        ),
        Redundancy(
            "RR2",
            "holding bool wrapper is redundant because snapshot holding is bool",
            mu('return bool(self.snapshot(now=now)["holding"])',
               'return self.snapshot(now=now)["holding"]'),
            p(probe_wrappers, "holding"),
        ),
        Redundancy(
            "RR3",
            "should_finalize bool wrapper is redundant because finalize is bool",
            mu('return bool(self.snapshot(now=now)["finalize"])',
               'return self.snapshot(now=now)["finalize"]'),
            p(probe_wrappers, "final"),
        ),
        Redundancy(
            "RR4",
            "snapshot holding inner bool is redundant under unary not",
            mu('"holding": not bool(healthy),',
               '"holding": not healthy,'),
            p(probe_snapshot, "normal"),
        ),
    )


EXPECTED_FENCE_COUNTS = {
    "hard_signal_kind": 19,
    "HardSignalAdapter.__init__": 2,
    "HardSignalAdapter.__call__": 1,
    "HardSignalAdapter._observe": 4,
    "HardSignalAdapter.__getattr__": 8,
    "HardSignalAdapter.__setattr__": 6,
    "HardDeathWatchdog.__init__": 9,
    "HardDeathWatchdog.ports": 1,
    "interrupt_event": 1,
    "interrupt_sequence": 2,
    "_read_maintenance": 9,
    "_advance_locked": 14,
    "hard_signal": 12,
    "due_probes": 3,
    "probe_result": 10,
    "snapshot": 5,
    "state": 1,
    "healthy_ports": 1,
    "holding": 1,
    "should_finalize": 1,
}


def run() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    kit = CheckKit()
    kit.section("Row 85 S4b shipped-source custody and signal inventory")
    before = source_bytes()
    before_raw_hash = digest(before)
    try:
        source = normalized_source(before)
        cases = fences()
        redundancies = redundancy_cases()
        mutations = tuple(
            (item.mutation.old, item.mutation.new)
            for item in (*cases, *redundancies)
        )
        owners = mutation_owners(source, mutations)
        segment_hashes = function_segment_sha256(source, owners)
        kit.check(
            "SOURCE-CUSTODY pinned per-fenced-function EOL-normalized "
            "addstock_watchdog.py source segments",
            segment_hashes == EXPECTED_FUNCTION_SEGMENT_SHA256,
            repr(segment_hashes),
        )
        locality_source = source + (
            "" if source.endswith("\n") else "\n"
        ) + "\ndef _source_custody_unrelated_probe():\n    return None\n"
        kit.check(
            "SOURCE-CUSTODY unrelated function leaves fenced digests unchanged",
            function_segment_sha256(locality_source, owners) == segment_hashes,
        )
        probe_mutation = next(
            item.mutation for item in (*cases, *redundancies)
            if mutation_owner(
                source, item.mutation.old, item.mutation.new
            ) is not None
        )
        probe_owner = mutation_owner(
            source, probe_mutation.old, probe_mutation.new)
        kit.check(
            "SOURCE-CUSTODY fenced-function mutation changes its owned digest",
            function_segment_sha256(
                probe_mutation.apply(source), (probe_owner,)
            ) != {probe_owner: segment_hashes[probe_owner]},
            str(probe_owner),
        )
        baseline = load_module(source)
    except BaseException as exc:
        kit.check("HARNESS-INVENTORY source loads and cases build", False,
                  f"{type(exc).__name__}: {exc}")
        return kit.finish()

    ids = tuple(case.fence_id for case in cases)
    redundant_ids = tuple(case.clause_id for case in redundancies)
    actual_counts = Counter(case.function for case in cases)
    kit.check("HARNESS-INVENTORY final code-derived fence count is 110",
              len(cases) == 110, str(len(cases)))
    kit.check("HARNESS-INVENTORY per-surface counts match derivation",
              dict(actual_counts) == EXPECTED_FENCE_COUNTS,
              repr(dict(actual_counts)))
    kit.check("HARNESS-INVENTORY every S4b method/property surface is covered",
              set(actual_counts) == set(EXPECTED_FENCE_COUNTS),
              repr(sorted(actual_counts)))
    kit.check("HARNESS-INVENTORY fence IDs are unique",
              len(set(ids)) == len(ids), repr(ids))
    kit.check("HARNESS-INVENTORY four redundant clauses are explicit",
              len(redundancies) == 4, str(len(redundancies)))
    kit.check("HARNESS-INVENTORY redundancy IDs are unique",
              len(set(redundant_ids)) == len(redundant_ids),
              repr(redundant_ids))

    tree = ast.parse(source)
    module_functions = {
        node.name for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    module_classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    module_constants = {
        target.id
        for node in tree.body
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        for target in (
            node.targets if isinstance(node, ast.Assign) else (node.target,)
        )
        if isinstance(target, ast.Name)
    }
    kit.check("WHOLE-MODULE every top-level function is inventoried",
              module_functions == {
                  "_utc_now", "_aware_utc", "_timestamp", "_parse_timestamp",
                  "maintenance_path", "maintenance_budget",
                  "_process_identity_from_handle", "get_process_identity",
                  "_normalize_ports", "validate_maintenance", "_read_bytes",
                  "load_maintenance", "maintenance_status", "_atomic_write",
                  "begin_maintenance", "end_maintenance", "maintenance_window",
                  "hard_signal_kind",
              }, repr(sorted(module_functions)))
    kit.check("WHOLE-MODULE every class is inventoried",
              set(module_classes) == {
                  "MaintenanceError", "MaintenanceStatus", "MaintenanceLease",
                  "HardSignalAdapter", "HardDeathWatchdog",
              }, repr(sorted(module_classes)))
    support_class_defs = {
        name: {
            node.name for node in class_node.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for name, class_node in module_classes.items()
        if name in {"MaintenanceError", "MaintenanceStatus", "MaintenanceLease"}
    }
    kit.check("WHOLE-MODULE lease support-class definitions are inventoried",
              support_class_defs == {
                  "MaintenanceError": set(),
                  "MaintenanceStatus": set(),
                  "MaintenanceLease": {"published"},
              }, repr(support_class_defs))
    kit.check("WHOLE-MODULE every constant is claimed; lock is integration-only",
              module_constants == {
                  "PORT_GRACE_S", "FLEET_GRACE_S", "PROBE_BACKOFF_S",
                  "MAINT_MARGIN_S", "MAINTENANCE_NAME", "MAINTENANCE_SCHEMA",
                  "MAINTENANCE_MAX_BYTES", "MAINTENANCE_MAX_PORTS",
                  "MAINTENANCE_MAX_BUDGET_S", "HEALTHY", "SUSPECT", "DEAD",
                  "_LEASE_RE", "_HARD_TOKENS", "_SOFT_CLASS_NAMES",
                  "_HARD_ERRNOS", "_HARD_WINERRORS", "_MAINT_LOCK",
              }, repr(sorted(module_constants)))
    adapter_node = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "HardSignalAdapter")
    watchdog_node = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "HardDeathWatchdog")
    adapter_defs = {
        node.name for node in adapter_node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    watchdog_defs = {
        node.name for node in watchdog_node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    kit.check("WHOLE-SLICE all adapter definitions are claimed",
              adapter_defs == {
                  "__init__", "__call__", "_observe", "__getattr__",
                  "__setattr__"}, repr(sorted(adapter_defs)))
    kit.check("WHOLE-SLICE all watchdog definitions and properties are claimed",
              watchdog_defs == {
                  "__init__", "ports", "interrupt_event", "interrupt_sequence",
                  "_read_maintenance", "_advance_locked", "hard_signal",
                  "due_probes", "probe_result", "snapshot", "state",
                  "healthy_ports", "holding", "should_finalize"},
              repr(sorted(watchdog_defs)))
    kit.check("DEFAULT-CONTRACT signal constants and monotonic default are pinned",
              probe_default_contract(baseline))
    kit.check("WHOLE-MODULE maintenance constants and lease grammar are exact",
              probe_lease_constant_contract(baseline))
    kit.check("WHOLE-MODULE classifier token and socket sets are exact",
              probe_classifier_constant_contract(baseline))
    kit.check("SHIPPED-CONTRACT unknown registered-fleet ports raise raw KeyError",
              probe_unknown_port_contract(baseline))
    kit.check("BOUNDARY lock and Event concurrency remain integration-only",
              "threading.Thread" not in source[source.index(
                  "def hard_signal_kind"):]
              and all("concurr" not in case.description.lower()
                      for case in cases))
    kit.check("CONTAINMENT fixture ticker vocabulary excludes protected names",
              SYNTHETIC_TICKERS.isdisjoint(FORBIDDEN_TICKERS),
              repr(sorted(SYNTHETIC_TICKERS & FORBIDDEN_TICKERS)))

    harness_tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".", 1)[0]
        for node in ast.walk(harness_tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        str(node.module).split(".", 1)[0]
        for node in ast.walk(harness_tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    path_names = {
        target.id
        for node in ast.walk(harness_tree)
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        for target in (
            node.targets if isinstance(node, ast.Assign) else (node.target,)
        )
        if isinstance(target, ast.Name)
        and node.value is not None
        and any(
            isinstance(part, ast.Name) and part.id == "Path"
            for part in ast.walk(node.value)
        )
    }
    write_attributes = {
        node.func.attr for node in ast.walk(harness_tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr in {
            "mkdir", "open", "replace", "rename", "rmdir", "touch",
            "unlink", "write_bytes", "write_text",
        }
        and (
            node.func.attr != "replace"
            or (
                isinstance(node.func.value, ast.Name)
                and node.func.value.id in path_names
            )
            or any(
                isinstance(part, ast.Name) and part.id == "Path"
                for part in ast.walk(node.func.value)
            )
        )
    }
    kit.check("CONTAINMENT no network, process, thread, or temp dependency",
              imported_roots.isdisjoint({
                  "asyncio", "ib_insync", "multiprocessing", "socket",
                  "subprocess", "tempfile", "threading"}),
              repr(sorted(imported_roots)))
    kit.check("CONTAINMENT explicit no-paths-created proof",
              not write_attributes, repr(sorted(write_attributes)))

    kit.section("One named baseline check per hard-signal fence")
    for case in cases:
        passed, detail = evaluate(case.probe, baseline)
        kit.check(
            f"FENCE {case.fence_id} {case.function} {case.description}",
            passed, detail,
        )

    kit.section("One compiling in-memory mutation killed per signal fence")
    for case in cases:
        try:
            mutated_source = case.mutation.apply(source)
            mutant = load_module(mutated_source)
        except BaseException as exc:
            kit.check(f"MUTATION-KILLED {case.fence_id}", False,
                      f"mutant did not load: {type(exc).__name__}: {exc}")
            continue
        survived, detail = evaluate(case.probe, mutant)
        kit.check(f"MUTATION-KILLED {case.fence_id}", not survived,
                  "probe survived" if survived else detail)

    kit.section("Behavior-preserving hard-signal clause deletions")
    for redundant in redundancies:
        try:
            mutated_source = redundant.mutation.apply(source)
            mutant = load_module(mutated_source)
        except BaseException as exc:
            kit.check(f"REDUNDANT-CONFIRMED {redundant.clause_id}", False,
                      f"mutant did not load: {type(exc).__name__}: {exc}")
            continue
        baseline_witness, baseline_detail = evaluate(
            redundant.witness, baseline)
        mutant_witness, mutant_detail = evaluate(redundant.witness, mutant)
        all_survived = True
        first_failure = ""
        for case in cases:
            survived, detail = evaluate(case.probe, mutant)
            if not survived:
                all_survived = False
                first_failure = f"{case.fence_id}: {detail}"
                break
        proof = baseline_witness and mutant_witness and all_survived
        detail = first_failure or baseline_detail or mutant_detail
        kit.check(
            f"REDUNDANT-CONFIRMED {redundant.clause_id} "
            f"{redundant.description}", proof, detail)

    kit.section("Production custody")
    after = source_bytes()
    kit.check("SOURCE-CUSTODY production raw bytes unchanged during run",
              digest(after) == before_raw_hash, digest(after))
    after_segment_hashes = function_segment_sha256(
        normalized_source(after), owners)
    kit.check("SOURCE-CUSTODY fenced-function digests unchanged during run",
              after_segment_hashes == segment_hashes,
              repr(after_segment_hashes))
    return kit.finish()


if __name__ == "__main__":
    raise SystemExit(run())
