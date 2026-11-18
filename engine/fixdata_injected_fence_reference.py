"""Offline mutation reference for Row 85 S5's injected Fix Data slice.

The harness executes the shipped ``fix_data_pipeline.py``, ``stock_ibkr.py``,
and ``export_quality.py`` sources from memory.  A process/network tripwire is
installed before any subject module loads, Fix Data receives a fake
``operation_gate`` module, and the real operation gate is instrumented to prove
zero acquisitions.  Mutants never touch production files; filesystem fixtures
live in fresh temporary roots and are required to leave zero residue.
"""

from __future__ import annotations

import ast
from collections import Counter
import contextlib
from dataclasses import dataclass
from datetime import date, datetime, time
import hashlib
import importlib
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import types
from typing import Callable


ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
TARGETS = {
    "fixdata": ENGINE_ROOT / "fix_data_pipeline.py",
    "stock": ENGINE_ROOT / "stock_ibkr.py",
    "export": ENGINE_ROOT / "export_quality.py",
    "display": PROJECT_ROOT / "display_data.py",
}

# Filled from the reviewed fenced source segments.  EOL normalization
# is deliberate: checkout CRLF smudging is not functional source drift.
EXPECTED_FUNCTION_SEGMENT_SHA256 = {
    "display": {
        "DataViewerApp._exd_export._work": "34f6f79458aa12951be143b54a5aa8a74eaee36bd39d42d4ab64e049e99d2abe",
        "DataViewerApp._storage_export_run._work": "11af92d4323bde742ea0b92d97da75c3d3319d2d9dca36ad9199d53cb45a93a1",
        "DataViewerApp._storage_verify_dialog.run_health.work": "c6e2b5d682c8a218c9fac9560751c137da939e8754bbdcb647f40daa608a8442",
    },
    "export": {
        "ensure_current_health": "db6c3892ffe70644d0b0ba8c756d6a3faf31522fe00ce94ad43b83fe27d7ed11",
    },
    "fixdata": {
        "<module>:CONNECT_ATTEMPTS": "ab5260d4e7372070dea4d47487f9fe603054d4178c28d38ae6d830a3d64ff04c",
        "<module>:CONNECT_RETRY_BACKOFF_S": "2d15a1f5f9df883b634a3b52626eb26f1f5075b334ced2e6b0392511d7e04d33",
        "<module>:run_log_retention": "5607ebafe779187d0f3a711eaf4b6bf96726654af4bbfd6fb6672de55fdc8c10",
        "_run_fix_data_body": "7c0e4aec7f589ef93914c04d7aef9414d316b95f68bdff2d66c8c6066b632fc8",
        "_run_fix_data_body.run_scheduler.worker": "066ba70fc62b09241f45ddf2c1fe7b269a3261ddd7fd6abbd785ff277014a4e9",
        "_run_fix_data_body.run_scheduler.worker.connect_cycle": "157ff137fcec5b3a283bfb17177734a497023e275220d4913ee56f72aa2047a2",
    },
    "stock": {
        "<module>:VOL_HARD_CEILING": "2c43a9e88b03640190f62103eb0c61a15ae55f8856f69075bc4e6e17707507ef",
        "<module>:VOL_VALUE_GATE": "e1449c62472ba29d42e7f20c3c79edad452ded7a657d73f10440d49ffc696215",
        "_guard_ratio_month_commit": "c27e4818a4814f3fc0377f3745f888fcd103765112a90cc827f503f7fd80400d",
        "_guard_ratio_month_commit.halt": "86f34320aeb94560a56037d58668803a791327ff12b0d9c2d811ffcf3f40be27",
        "split_session_bars": "3e721fd069f419fbfd263a3d6cbeb451af678dcd7eda824ecb117fcb7f4dc2c0",
    },
}

FORBIDDEN_TICKERS = frozenset({"APO", "JCI", "OKE", "TKO", "WBD", "TMUS"})
SYNTHETIC_TICKERS = frozenset({"S5X", "S5Y"})
PORT = 7851
DAY = "2024-01-02"
RATIO_INTERVAL = "1d-iv"
EXPECTED_FENCE_COUNTS = {
    "X11": 21,
    "X12.a": 9,
    "X12.b": 7,
    "X13": 4,
    "X16.a": 11,
    "X16.b": 22,
    "X18.a": 2,
    "X18.b": 9,
}
EXPECTED_MODULE_SEGMENT_COUNTS = {
    "display": 0,
    "export": 0,
    "fixdata": 3,
    "stock": 2,
}

if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

if __name__ == "__main__":
    from fetch_a2_ibkr_workflows_selftest import Operations
    raise SystemExit(Operations.run_fixdata_reference_cli("fixdata_injected_fence_reference"))

# Supplied only by the existing confined Operations owner. Fresh in-memory
# subjects do not pass through that owner's patch of the real pipeline.run.
_TEST_CAPABILITY = None
_REFERENCE_ROOT = None

from check_kit import CheckKit  # noqa: E402
from source_segment_custody import (  # noqa: E402
    function_segment_sha256,
    mutation_owner,
    mutation_owners,
    normalize_source_bytes,
)


TEST_ONLY = True
Probe = Callable[["Subjects"], bool]


class HarnessError(RuntimeError):
    """The source, fixture, or deterministic-termination contract drifted."""


class FatalWorker(BaseException):
    """Bypass inner ``except Exception`` and exercise the worker-death path."""


@dataclass(frozen=True)
class Mutation:
    target: str
    old: str
    new: str

    def apply(self, sources: dict[str, str]) -> dict[str, str]:
        source = sources[self.target]
        count = source.count(self.old)
        if count != 1:
            raise HarnessError(
                f"{self.target} mutation anchor count={count}, expected 1: "
                f"{self.old[:140]!r}"
            )
        changed = dict(sources)
        changed[self.target] = source.replace(self.old, self.new, 1)
        return changed


@dataclass(frozen=True)
class Fence:
    fence_id: str
    family: str
    surface: str
    description: str
    mutation: Mutation
    probe: Probe


@dataclass(frozen=True)
class Redundancy:
    clause_id: str
    description: str
    mutation: Mutation
    witness: Probe


@dataclass
class Subjects:
    fixdata: types.ModuleType
    stock: types.ModuleType
    export: types.ModuleType
    display_source: str
    sources: dict[str, str]
    fake_gate_calls: list[tuple[str, dict]]


def mu(target: str, old: str, new: str) -> Mutation:
    return Mutation(target, old, new)


def f(fence_id: str, family: str, surface: str, description: str,
      target: str, old: str, new: str, probe: Probe) -> Fence:
    return Fence(
        fence_id, family, surface, description,
        mu(target, old, new), probe,
    )


def normalized_source(payload: bytes) -> str:
    return normalize_source_bytes(payload)


def source_map() -> dict[str, str]:
    return {
        name: normalized_source(path.read_bytes())
        for name, path in TARGETS.items()
    }


class EarlyTripwire:
    """Deny and count every network/process primitive required by V4-3."""

    def __init__(self) -> None:
        self.denials: Counter[str] = Counter()
        self._patches: list[tuple[object, str, object]] = []

    def _deny(self, label: str) -> Callable[..., object]:
        def denied(*_args: object, **_kwargs: object) -> object:
            self.denials[label] += 1
            raise HarnessError(f"containment tripwire denied {label}")
        return denied

    def _patch(self, owner: object, name: str, replacement: object) -> None:
        self._patches.append((owner, name, getattr(owner, name)))
        setattr(owner, name, replacement)

    def __enter__(self) -> "EarlyTripwire":
        original_socket = socket.socket
        tripwire = self

        class DeniedSocket(original_socket):
            def __new__(cls, *_args: object, **_kwargs: object) -> object:
                tripwire.denials["socket.socket"] += 1
                raise HarnessError("containment tripwire denied socket.socket")

        self._patch(socket, "socket", DeniedSocket)
        self._patch(
            socket, "create_connection", self._deny("socket.create_connection"))
        self._patch(socket, "getaddrinfo", self._deny("socket.getaddrinfo"))
        self._patch(subprocess, "Popen", self._deny("subprocess.Popen"))
        self._patch(os, "system", self._deny("os.system"))
        for name in sorted(value for value in dir(os) if value.startswith("spawn")):
            value = getattr(os, name)
            if callable(value):
                self._patch(os, name, self._deny(f"os.{name}"))
        return self

    def __exit__(self, *_exc: object) -> None:
        for owner, name, original in reversed(self._patches):
            setattr(owner, name, original)


class FakeLease:
    def __enter__(self) -> "FakeLease":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


def fake_operation_gate(calls: list[tuple[str, dict]]) -> types.ModuleType:
    module = types.ModuleType("operation_gate")

    class OperationBusy(RuntimeError):
        pass

    def acquire(mode: str, **kwargs: object) -> FakeLease:
        calls.append((mode, dict(kwargs)))
        return FakeLease()

    module.OperationBusy = OperationBusy
    module.acquire = acquire
    return module


def fake_run_log_retention() -> types.ModuleType:
    module = types.ModuleType("run_log_retention")
    module.prune_run_logs = lambda _root: {
        "deleted": [], "errors": [], "freed_bytes": 0,
    }
    return module


@contextlib.contextmanager
def seeded_modules(values: dict[str, types.ModuleType]):
    previous = {name: sys.modules.get(name) for name in values}
    sys.modules.update(values)
    try:
        yield
    finally:
        for name, prior in previous.items():
            if prior is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = prior


def execute_module(name: str, target: Path, source: str) -> types.ModuleType:
    module = types.ModuleType(name)
    module.__file__ = str(target)
    module.__package__ = ""
    module.__source__ = source
    previous = sys.modules.get(name)
    sys.modules[name] = module
    try:
        exec(compile(source, str(target), "exec"), module.__dict__)
    finally:
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous
    return module


def load_subjects(sources: dict[str, str]) -> Subjects:
    fake_gate_calls: list[tuple[str, dict]] = []
    ib_async = types.ModuleType("ib_async")
    fakes = {
        "operation_gate": fake_operation_gate(fake_gate_calls),
        "run_log_retention": fake_run_log_retention(),
        "ib_async": ib_async,
    }
    with seeded_modules(fakes):
        fixdata = execute_module(
            "_s5_fixdata_subject", TARGETS["fixdata"], sources["fixdata"])
        stock = execute_module(
            "_s5_stock_subject", TARGETS["stock"], sources["stock"])
        export = execute_module(
            "_s5_export_subject", TARGETS["export"], sources["export"])
        # Display is a GUI-heavy read-only surface.  Compile every baseline and
        # mutant, but never execute/import it.
        compile(sources["display"], str(TARGETS["display"]), "exec")
    return Subjects(
        fixdata=fixdata,
        stock=stock,
        export=export,
        display_source=sources["display"],
        sources=sources,
        fake_gate_calls=fake_gate_calls,
    )


def evaluate(probe: Probe, subjects: Subjects) -> tuple[bool, str]:
    try:
        result = probe(subjects)
    except BaseException as exc:
        return False, f"{type(exc).__name__}: {exc}"
    if result is True:
        return True, ""
    return False, f"probe returned {result!r}"


def bounded(call: Callable[[], object], timeout_s: float = 3.0) -> object:
    box: dict[str, object] = {}

    def target() -> None:
        try:
            box["value"] = call()
        except BaseException as exc:
            box["error"] = exc

    thread = threading.Thread(target=target, daemon=True, name="s5-bounded-call")
    thread.start()
    thread.join(timeout=timeout_s)
    if thread.is_alive():
        raise HarnessError(f"fixture exceeded {timeout_s:g}s deterministic bound")
    if "error" in box:
        raise box["error"]  # type: ignore[misc]
    return box.get("value")


class Adapter:
    def __init__(self, *, disconnect_error: BaseException | None = None) -> None:
        self.disconnect_error = disconnect_error
        self.disconnect_calls = 0

    def disconnect(self) -> None:
        self.disconnect_calls += 1
        if self.disconnect_error is not None:
            raise self.disconnect_error


class ScriptedFactory:
    def __init__(self, failures: int = 0, *, prefix: str = "boom",
                 disconnect_error: BaseException | None = None) -> None:
        self.failures = failures
        self.prefix = prefix
        self.calls = 0
        self.adapters: list[Adapter] = []
        self.disconnect_error = disconnect_error
        self.lock = threading.Lock()

    def __call__(self, _port: int) -> Adapter:
        with self.lock:
            self.calls += 1
            call = self.calls
            if call <= self.failures:
                raise RuntimeError(f"{self.prefix}-{call}")
            adapter = Adapter(disconnect_error=self.disconnect_error)
            self.adapters.append(adapter)
            return adapter


def final_states(events: list[dict]) -> dict[int, str]:
    states: dict[int, str] = {}
    for event in events:
        if event.get("type") == "ports" and isinstance(event.get("states"), dict):
            states = dict(event["states"])
    return states


def log_messages(events: list[dict]) -> list[str]:
    return [
        str(event.get("message") or "")
        for event in events if event.get("type") == "log"
    ]


def run_fixdata(subjects: Subjects, *, factory: ScriptedFactory | None = None,
                mode: str = "fill", recover_fn: object = "omit",
                fill_error: BaseException | None = None,
                audit_error: BaseException | None = None,
                reconcile_plan_error: BaseException | None = None,
                reconcile_error: BaseException | None = None,
                health_fn: object = "omit") -> dict[str, object]:
    module = subjects.fixdata
    factory = factory or ScriptedFactory()
    events: list[dict] = []
    fills: list[tuple] = []
    audits: list[tuple] = []
    recover_calls: list[tuple[int, str]] = []
    sleeps: list[float] = []
    residue_paths: list[Path] = []

    def scan(_root: object, **_kwargs: object) -> dict:
        summary = {}
        if mode == "fill":
            summary = {
                "S5X 1d": {
                    "missing_day_list": [DAY],
                    "source_absent_list": [],
                }
            }
        return {
            "summary": summary,
            "series_scanned": 1,
            "verification_series": [],
        }

    def fill(*args: object, **_kwargs: object) -> dict:
        fills.append(tuple(args))
        if fill_error is not None:
            raise fill_error
        return {}

    def audit(*args: object, **_kwargs: object) -> dict:
        audits.append(tuple(args))
        if audit_error is not None:
            raise audit_error
        return {}

    def recover(port: int, reason: str) -> object:
        recover_calls.append((port, reason))
        if callable(recover_fn):
            return recover_fn(port, reason)
        return recover_fn

    def reconcile_plan(_root: object, *, ticker: str | None,
                       kinds: object) -> list[dict]:
        del kinds
        if ticker is not None and reconcile_plan_error is not None:
            raise reconcile_plan_error
        return [
            {
                "ticker": "S5X", "kind": RATIO_INTERVAL,
                "kind_token": RATIO_INTERVAL, "day": DAY,
            },
            {
                "ticker": "S5X", "kind": RATIO_INTERVAL,
                "kind_token": RATIO_INTERVAL, "day": "2024-01-03",
            },
        ]

    def reconcile(*_args: object, **_kwargs: object) -> dict:
        if reconcile_error is not None:
            raise reconcile_error
        return {
            "status": "settled", "ticker": "S5X",
            "kind": RATIO_INTERVAL, "kind_token": RATIO_INTERVAL,
            "day": DAY, "request_count": 1,
        }

    kwargs: dict[str, object] = {}
    if recover_fn != "omit":
        kwargs["port_recover_fn"] = recover
    if health_fn != "omit":
        kwargs["health_fn"] = health_fn
    if mode == "reconcile":
        kwargs.update({
            "reconcile_plan_fn": reconcile_plan,
            "reconcile_fn": reconcile,
        })

    with tempfile.TemporaryDirectory(prefix="row85_s5_") as tmp:
        root = Path(tmp)
        residue_paths.append(root)
        if _REFERENCE_ROOT is None or not root.resolve().is_relative_to(_REFERENCE_ROOT):
            raise HarnessError("in-memory subject root escaped confined reference owner")
        value = bounded(lambda: module.run(
            root=root,
            evidence_dir=root / "fetch-evidence",
            _test_capability=_TEST_CAPABILITY,
            ports=(PORT,),
            adapter_factory=factory,
            scan_fn=scan,
            fill_fn=fill,
            audit_fn=audit,
            series_fn=(lambda _root: [("S5X", "1d")]
                       if mode == "check" else []),
            refresh_fn=lambda _root, _tickers, **_kwargs: {},
            progress=lambda event: events.append(dict(event)),
            sleep_fn=lambda seconds: sleeps.append(float(seconds)),
            **kwargs,
        ))
    if any(path.exists() for path in residue_paths):
        raise HarnessError("temporary fixture residue survived cleanup")
    return {
        "result": value,
        "events": events,
        "states": final_states(events),
        "logs": log_messages(events),
        "fills": fills,
        "audits": audits,
        "recover_calls": recover_calls,
        "sleeps": sleeps,
        "factory": factory,
        "residue": tuple(residue_paths),
    }


def probe_transient_connect(subjects: Subjects) -> bool:
    run = run_fixdata(subjects, factory=ScriptedFactory(failures=2))
    factory = run["factory"]
    return (
        isinstance(factory, ScriptedFactory)
        and factory.calls == 3
        and len(run["fills"]) == 1
        and run["sleeps"] == [0.25, 0.25]
        and run["states"].get(PORT) == "done"
        and run["fills"][0][0] is factory.adapters[0]
    )


def probe_connect_states(subjects: Subjects) -> bool:
    run = run_fixdata(subjects, factory=ScriptedFactory(failures=2))
    states = [
        event.get("states", {}).get(PORT)
        for event in run["events"] if event.get("type") == "ports"
    ]
    return (
        states.count("connecting") == 1
        and "connecting (attempt 2/3)" in states
        and "connecting (attempt 3/3)" in states
        and "connecting (attempt 1/3)" not in states
    )


def probe_last_connect_error(subjects: Subjects) -> bool:
    run = run_fixdata(subjects, factory=ScriptedFactory(failures=99))
    errors = run["result"].get("port_errors") or []
    return (
        run["factory"].calls == 3
        and any("RuntimeError: boom-3" in value for value in errors)
        and run["states"].get(PORT, "").startswith("DEAD - RuntimeError: boom-3")
    )


def probe_recovery_success(subjects: Subjects) -> bool:
    factory = ScriptedFactory(failures=3)

    def heal(_port: int, _reason: str) -> bool:
        factory.failures = 3
        return True

    run = run_fixdata(subjects, factory=factory, recover_fn=heal)
    states = [
        event.get("states", {}).get(PORT)
        for event in run["events"] if event.get("type") == "ports"
    ]
    return (
        factory.calls == 4
        and len(run["recover_calls"]) == 1
        and run["recover_calls"][0] == (PORT, "RuntimeError: boom-3")
        and "recovery requested" in states
        and "connecting (attempt 1/3)" in states
        and len(run["fills"]) == 1
        and run["fills"][0][0] is factory.adapters[0]
    )


def probe_recovery_declined(subjects: Subjects) -> bool:
    run = run_fixdata(
        subjects, factory=ScriptedFactory(failures=99),
        recover_fn=lambda _port, _reason: False,
    )
    errors = run["result"].get("port_errors") or []
    return (
        len(run["recover_calls"]) == 1
        and run["factory"].calls == 3
        and any("RuntimeError: boom-3" in value for value in errors)
        and not run["fills"]
    )


def probe_no_recovery_without_callback(subjects: Subjects) -> bool:
    run = run_fixdata(subjects, factory=ScriptedFactory(failures=99))
    return (
        not run["recover_calls"]
        and not any("recovery callback failed" in value for value in run["logs"])
        and run["factory"].calls == 3
    )


def probe_truthy_recovery_rejected(subjects: Subjects) -> bool:
    run = run_fixdata(
        subjects, factory=ScriptedFactory(failures=99),
        recover_fn=lambda _port, _reason: "bad_recover",
    )
    return (
        len(run["recover_calls"]) == 1
        and run["factory"].calls == 3
        and not run["fills"]
        and run["states"].get(PORT, "").startswith("DEAD")
    )


def probe_raising_recovery_rejected(subjects: Subjects) -> bool:
    def raises(_port: int, _reason: str) -> object:
        raise KeyboardInterrupt("callback-fatal")

    run = run_fixdata(
        subjects, factory=ScriptedFactory(failures=3), recover_fn=raises)
    return (
        len(run["recover_calls"]) == 1
        and not run["fills"]
        and run["states"].get(PORT, "").startswith("DEAD")
        and any(
            "recovery callback failed: KeyboardInterrupt: callback-fatal" in value
            for value in run["logs"]
        )
    )


def probe_worker_death_contract(subjects: Subjects) -> bool:
    run = run_fixdata(
        subjects, mode="fill", fill_error=FatalWorker("fatal-fill"))
    result = run["result"]
    return (
        run["states"].get(PORT) == "DEAD - FatalWorker: fatal-fill"
        and result.get("port_errors") == [
            f"port {PORT}: FatalWorker: fatal-fill"]
        and result.get("halted") == 1
        and result.get("unprocessed") == [
            ("S5X", "1d", "repair", "ambiguous")]
        and any(
            value == f"port {PORT}: DEAD - FatalWorker: fatal-fill"
            for value in run["logs"]
        )
        and result.get("error") is None
    )


def probe_long_worker_death_state(subjects: Subjects) -> bool:
    run = run_fixdata(
        subjects,
        factory=ScriptedFactory(failures=99, prefix="x" * 220),
    )
    state = run["states"].get(PORT, "")
    return len(state) == 160 and state.startswith("DEAD - RuntimeError: ")


def probe_reconcile_death_contract(subjects: Subjects) -> bool:
    run = run_fixdata(
        subjects, mode="reconcile",
        reconcile_error=FatalWorker("fatal-reconcile"),
    )
    result = run["result"]
    rows = result.get("reconcile_rows") or []
    debt = result.get("unprocessed") or []
    return (
        result.get("reconcile_selected") == 2
        and result.get("reconcile_completed") == 1
        and len(rows) == 1
        and rows[0].get("status") == "ambiguous"
        and rows[0].get("request_count_unknown") is True
        and rows[0].get("error") == "FatalWorker: fatal-reconcile"
        and ("S5X", RATIO_INTERVAL, DAY, "reconcile", "ambiguous") in debt
        and (
            "S5X", RATIO_INTERVAL, "2024-01-03", "reconcile",
            "worker-failed-unstarted",
        ) in debt
        and result.get("halted") == 1
        and result.get("error") is None
    )


def probe_disconnect_contract(subjects: Subjects) -> bool:
    factory = ScriptedFactory(
        disconnect_error=RuntimeError("disconnect-boom"))
    run = run_fixdata(
        subjects, mode="fill", factory=factory,
        fill_error=FatalWorker("fatal-fill"),
    )
    return (
        len(factory.adapters) == 1
        and factory.adapters[0].disconnect_calls == 1
        and any(
            value == f"port {PORT} disconnect: RuntimeError: disconnect-boom"
            for value in run["logs"]
        )
        and run["states"].get(PORT) == "DEAD - FatalWorker: fatal-fill"
        and run["result"].get("error") is None
    )


def source_has(target: str, text: str) -> Probe:
    return lambda subjects: text in subjects.sources[target]


@contextlib.contextmanager
def patched(owner: object, name: str, replacement: object):
    original = getattr(owner, name)
    setattr(owner, name, replacement)
    try:
        yield
    finally:
        setattr(owner, name, original)


def split_ratio_case(subjects: Subjects, *, bad_field: str = "high",
                     bad_value: float = 11.0,
                     include_clean: bool = True) -> dict[str, object]:
    fields = {"open": 0.5, "high": 0.5, "low": 0.5, "close": 0.5}
    fields[bad_field] = bad_value
    bars = []
    if include_clean:
        bars.append(types.SimpleNamespace(
            date=datetime(2024, 1, 2, 10, 0),
            open=0.5, high=0.5, low=0.5, close=0.5, volume=1.0,
        ))
    bars.append(types.SimpleNamespace(
        date=datetime(2024, 1, 2, 10, 1),
        volume=1.0, **fields,
    ))
    counters = {"invalid": 0, "outside_day": 0, "non_rth": 0}
    out = subjects.stock.split_session_bars(
        bars, {date(2024, 1, 2)}, counters, "1m-iv")
    return {"out": out, "counters": counters}


def probe_ratio_ceiling_mixed(subjects: Subjects) -> bool:
    result = split_ratio_case(subjects)
    bars = result["out"].get(date(2024, 1, 2), [])
    return (
        result["counters"] == {
            "invalid": 1, "outside_day": 0, "non_rth": 0}
        and len(bars) == 1
        and bars[0][1:5] == (0.5, 0.5, 0.5, 0.5)
    )


def ratio_field_probe(field: str) -> Probe:
    def probe(subjects: Subjects) -> bool:
        result = split_ratio_case(
            subjects, bad_field=field, bad_value=11.0,
            include_clean=False)
        return result["counters"]["invalid"] == 1 and not result["out"]
    return probe


def probe_ratio_exact_ceiling_allowed(subjects: Subjects) -> bool:
    result = split_ratio_case(
        subjects, bad_field="high", bad_value=10.0,
        include_clean=False)
    bars = result["out"].get(date(2024, 1, 2), [])
    return result["counters"]["invalid"] == 0 and len(bars) == 1


def ratio_bar(year: int, month: int, value: float) -> tuple:
    return (datetime(year, month, 1, 0, 0),
            value, value, value, value, 0)


def guard_ratio_case(
        subjects: Subjects, *,
        candidates: tuple[tuple[int, int, float, bool, bool], ...] = (
            (2024, 1, 0.5, True, True),),
        target: tuple[int, int] = (2024, 2),
        incoming_value: float | None = 25.0,
        malformed_incoming: bool = False,
        force_manifest_error: bool = False,
        force_tree_error: bool = False,
        force_read_error: bool = False,
        gate: bool | None = None,
        interval: str = RATIO_INTERVAL) -> dict[str, object]:
    module = subjects.stock
    ss = module.ss
    residue: Path | None = None
    with tempfile.TemporaryDirectory(prefix="row85_s5_ratio_") as tmp:
        root = Path(tmp)
        residue = root
        ticker = "S5X"
        manifest = ss.new_manifest(ticker, ticker)
        months = ss.manifest_months(manifest, interval)
        for year, month, value, claimed, present_file in candidates:
            key = ss.month_key(year, month)
            if claimed:
                months[key] = {"status": "present"}
            if present_file:
                path = ss.month_file_path(
                    root, ticker, year, month, interval, fmt="csv")
                path.parent.mkdir(parents=True, exist_ok=True)
                ss.write_month_file(path, [ratio_bar(year, month, value)])
        added = ([('short',)] if malformed_incoming else
                 [] if incoming_value is None else
                 [ratio_bar(target[0], target[1], incoming_value)])
        res: dict[str, object] = {}
        mstate = {"manifest": manifest}
        original_gate = module.VOL_VALUE_GATE
        if gate is not None:
            module.VOL_VALUE_GATE = gate

        def manifest_fail(_path: object) -> object:
            raise ss.StorageError("manifest-boom")

        def tree_fail(*_args: object, **_kwargs: object) -> object:
            raise ss.StorageError("tree-boom")

        def read_fail(_path: object) -> object:
            raise ss.StorageError("read-boom")

        manifest_context = (
            patched(ss, "load_manifest", manifest_fail)
            if force_manifest_error else contextlib.nullcontext())
        tree_context = (
            patched(module, "_latest_tree_month", tree_fail)
            if force_tree_error else contextlib.nullcontext())
        read_context = (
            patched(ss, "read_month_file", read_fail)
            if force_read_error else contextlib.nullcontext())
        error: BaseException | None = None
        try:
            with manifest_context, tree_context, read_context:
                call_mstate = None if force_manifest_error else mstate
                module._guard_ratio_month_commit(
                    root, ticker, interval, target, added, res, call_mstate)
        except BaseException as exc:
            error = exc
        finally:
            module.VOL_VALUE_GATE = original_gate
    if residue is None or residue.exists():
        raise HarnessError("ratio guard temporary fixture left residue")
    return {"error": error, "res": res}


def halted_record(outcome: dict[str, object], key: str = "2024-02") -> dict:
    res = outcome["res"]
    if not isinstance(res, dict):
        return {}
    return dict((res.get("months") or {}).get(key) or {})


def probe_ratio_guard_halt(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(subjects)
    error = outcome["error"]
    record = halted_record(outcome)
    return (
        type(error) is subjects.stock.SeriesHalt
        and record.get("status") == "HALTED"
        and "unit-flip gate HALTED" in str(record.get("reason"))
        and record.get("prior_month") == "2024-01"
        and record.get("prior_median") == 0.5
        and record.get("incoming_median") == 25.0
        and record.get("ratio") == 50.0
        and getattr(error, "metadata", {}).get("vol_value_gate") == record
    )


def probe_ratio_guard_below(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(subjects, incoming_value=24.0)
    return outcome["error"] is None and not outcome["res"]


def probe_ratio_guard_fresh(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(subjects, candidates=())
    return outcome["error"] is None and not outcome["res"]


def probe_ratio_guard_disabled(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(subjects, gate=False)
    return outcome["error"] is None and not outcome["res"]


def probe_ratio_guard_nonratio(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(subjects, interval="1d")
    return outcome["error"] is None and not outcome["res"]


def probe_ratio_guard_empty(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(subjects, incoming_value=None)
    return outcome["error"] is None and not outcome["res"]


def probe_ratio_guard_manifest_error(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(
        subjects, candidates=(), force_manifest_error=True)
    return (
        type(outcome["error"]) is subjects.stock.SeriesHalt
        and "manifest-boom" in str(outcome["error"])
        and halted_record(outcome).get("status") == "HALTED"
    )


def probe_ratio_guard_tree_error(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(subjects, force_tree_error=True)
    return (
        type(outcome["error"]) is subjects.stock.SeriesHalt
        and "tree-boom" in str(outcome["error"])
    )


def probe_ratio_guard_missing(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(
        subjects, candidates=((2024, 1, 0.5, True, False),))
    return (
        type(outcome["error"]) is subjects.stock.SeriesHalt
        and "file is missing" in str(outcome["error"])
        and halted_record(outcome).get("prior_month") == "2024-01"
    )


def probe_ratio_guard_read_error(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(subjects, force_read_error=True)
    return (
        type(outcome["error"]) is subjects.stock.SeriesHalt
        and "read-boom" in str(outcome["error"])
    )


def probe_ratio_guard_nonpositive(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(
        subjects, candidates=((2024, 1, 0.0, True, True),))
    return (
        type(outcome["error"]) is subjects.stock.SeriesHalt
        and "non-positive OHLC median" in str(outcome["error"])
    )


def probe_ratio_guard_incoming_none(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(subjects, malformed_incoming=True)
    return outcome["error"] is None and not outcome["res"]


def probe_ratio_guard_manifest_latest(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(
        subjects,
        candidates=(
            (2023, 12, 0.1, True, False),
            (2024, 1, 0.5, True, False),
        ),
        incoming_value=30.0,
    )
    record = halted_record(outcome)
    return (
        type(outcome["error"]) is subjects.stock.SeriesHalt
        and "file is missing" in str(outcome["error"])
        and record.get("prior_month") == "2024-01"
    )


def probe_ratio_guard_tree_newer(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(
        subjects,
        candidates=(
            (2023, 12, 0.1, True, True),
            (2024, 1, 0.5, False, True),
        ),
        incoming_value=10.0,
    )
    return outcome["error"] is None and not outcome["res"]


def probe_ratio_guard_after_bound(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(
        subjects,
        candidates=(
            (2023, 12, 0.1, False, True),
            (2024, 1, 0.5, True, False),
        ),
        incoming_value=4.0,
    )
    return (
        type(outcome["error"]) is subjects.stock.SeriesHalt
        and "file is missing" in str(outcome["error"])
        and halted_record(outcome).get("prior_month") == "2024-01"
    )


def probe_ratio_guard_relative_tolerance(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(subjects, incoming_value=25.0 - 1e-11)
    return type(outcome["error"]) is subjects.stock.SeriesHalt


def probe_ratio_guard_absolute_tolerance(subjects: Subjects) -> bool:
    outcome = guard_ratio_case(
        subjects,
        candidates=((2024, 1, 1e-15, True, True),),
        incoming_value=4.95e-14,
    )
    return type(outcome["error"]) is subjects.stock.SeriesHalt


def probe_fixdata_has_no_health_dependency(subjects: Subjects) -> bool:
    tree = ast.parse(subjects.sources["fixdata"])
    forbidden = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            forbidden.extend(
                alias.name for alias in node.names
                if alias.name in {"export_quality", "health_report"})
        elif isinstance(node, ast.ImportFrom):
            if node.module in {"export_quality", "health_report"}:
                forbidden.append(str(node.module))
        elif isinstance(node, ast.Name) and node.id in {
                "export_quality", "health_report"}:
            forbidden.append(node.id)
    return not forbidden


def probe_fixdata_default_skips_health(subjects: Subjects) -> bool:
    run = run_fixdata(subjects, mode="none")
    result = run["result"]
    return (
        result.get("error") is None
        and result.get("health_report") is None
        and result.get("health_queue") == []
        and result.get("coverage_report") is None
        and not any(value.startswith("[health]") for value in run["logs"])
    )


def health_case(subjects: Subjects, *, injected_current: object = "omit",
                injected_audit: object = "omit",
                default_current: bool = True) -> dict[str, object]:
    module = subjects.export
    calls: list[tuple] = []
    cached = {"marker": "cached"}
    fresh = {"marker": "fresh"}

    def load(root: Path) -> dict:
        calls.append(("load", root))
        return cached

    def current(root: Path, value: object) -> bool:
        calls.append(("current", root, value))
        return default_current

    def audit(root: Path, *, write: bool) -> dict:
        calls.append(("default_audit", root, write))
        return fresh

    kwargs: dict[str, object] = {}
    if injected_current != "omit":
        def current_fn(root: Path) -> object:
            calls.append(("injected_current", root))
            return injected_current
        kwargs["current_fn"] = current_fn
    if injected_audit != "omit":
        def audit_fn(root: Path) -> dict:
            calls.append(("injected_audit", root))
            return fresh
        kwargs["audit_fn"] = audit_fn

    residue: Path | None = None
    with tempfile.TemporaryDirectory(prefix="row85_s5_health_") as tmp:
        root = Path(tmp)
        residue = root
        with patched(module.health_report, "load_report", load), \
                patched(module, "_health_report_current", current), \
                patched(module.health_report, "audit", audit):
            value = module.ensure_current_health(root, **kwargs)
    if residue is None or residue.exists():
        raise HarnessError("health fixture left temporary residue")
    return {"value": value, "calls": calls, "cached": cached, "fresh": fresh}


def probe_health_load_then_default_current(subjects: Subjects) -> bool:
    result = health_case(subjects, default_current=True)
    return (
        result["value"] is result["cached"]
        and [call[0] for call in result["calls"]] == ["load", "current"]
        and result["calls"][1][2] is result["cached"]
    )


def probe_health_injected_bool(subjects: Subjects) -> bool:
    result = health_case(subjects, injected_current="truthy", default_current=False)
    return (
        result["value"] is result["cached"]
        and [call[0] for call in result["calls"]]
        == ["load", "injected_current"]
    )


def probe_health_current_returns_cache(subjects: Subjects) -> bool:
    result = health_case(subjects, default_current=True)
    return result["value"] is result["cached"] and all(
        "audit" not in call[0] for call in result["calls"])


def probe_health_injected_audit(subjects: Subjects) -> bool:
    result = health_case(
        subjects, injected_current=False, injected_audit=True,
        default_current=True)
    return (
        result["value"] is result["fresh"]
        and [call[0] for call in result["calls"]]
        == ["load", "injected_current", "injected_audit"]
    )


def probe_health_default_audit(subjects: Subjects) -> bool:
    result = health_case(subjects, injected_current=False)
    return (
        result["value"] is result["fresh"]
        and [call[0] for call in result["calls"]]
        == ["load", "injected_current", "default_audit"]
        and result["calls"][-1][2] is True
    )


def display_health_calls(source: str) -> list[ast.Call]:
    tree = ast.parse(source)
    calls = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if (isinstance(func, ast.Attribute)
                and func.attr == "ensure_current_health"
                and isinstance(func.value, ast.Name)
                and func.value.id == "export_quality"):
            calls.append(node)
    return calls


def probe_display_four_health_calls(subjects: Subjects) -> bool:
    calls = display_health_calls(subjects.display_source)
    return len(calls) == 4


def probe_display_export_health_q(subjects: Subjects) -> bool:
    return (
        '"Checking bank health for export…",\n                }))\n'
        "                export_quality.ensure_current_health(root_dir)\n"
        "                q.put((\"progress\", {" in subjects.display_source
    )


def probe_display_export_health_post(subjects: Subjects) -> bool:
    return (
        '"Checking bank health for export…",\n                }, force=True)\n'
        "                export_quality.ensure_current_health(root_dir)\n"
        "                _post_progress({" in subjects.display_source
    )


def a15_pinned_grade_contract(display_source: str) -> bool:
    tree = ast.parse(display_source)
    worker = next((
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_addstock_consume_portfree_debt"
    ), None)
    if worker is None:
        return False
    attrs = {
        node.attr for node in ast.walk(worker)
        if isinstance(node, ast.Attribute)
    }
    reference = (ENGINE_ROOT / "addstock_vol_debt_reference.py").read_text(
        encoding="utf-8")
    return (
        any("cancel" in name for name in attrs)
        and any("pause" in name for name in attrs)
        and "_storage_find_say" in attrs
        and 'worker = find_method(tree, "_addstock_consume_portfree_debt")'
        in reference
        and 'any("cancel" in name for name in refs)' in reference
        and 'any("pause" in name for name in refs)' in reference
        and '"_storage_find_say" in refs' in reference
    )


def fixdata_fences() -> tuple[Fence, ...]:
    return (
        f("X12A-01", "X12.a", "connect_cycle", "three attempts are exact",
          "fixdata", "CONNECT_ATTEMPTS = 3", "CONNECT_ATTEMPTS = 2",
          probe_transient_connect),
        f("X12A-02", "X12.a", "connect_cycle", "retry backoff is 0.25 seconds",
          "fixdata", "CONNECT_RETRY_BACKOFF_S = 0.25",
          "CONNECT_RETRY_BACKOFF_S = 0.5", probe_transient_connect),
        f("X12A-03", "X12.a", "connect_cycle", "range includes final attempt",
          "fixdata", "for attempt in range(1, CONNECT_ATTEMPTS + 1):",
          "for attempt in range(1, CONNECT_ATTEMPTS):", probe_transient_connect),
        f("X12A-04", "X12.a", "connect_cycle", "first state is preserved",
          "fixdata", "if not (preserve_first_state and attempt == 1):",
          "if True:", probe_connect_states),
        f("X12A-05", "X12.a", "connect_cycle", "attempt state carries exact bound",
          "fixdata", 'f"{CONNECT_ATTEMPTS})")', 'f"X{CONNECT_ATTEMPTS})")',
          probe_connect_states),
        f("X12A-06", "X12.a", "connect_cycle", "sleep begins only after attempt one",
          "fixdata", "if attempt > 1:\n                        sleep_fn",
          "if attempt >= 1:\n                        sleep_fn", probe_transient_connect),
        f("X12A-07", "X12.a", "connect_cycle", "successful adapter returns mid-loop",
          "fixdata", "return adapter_factory(port)",
          "adapter_factory(port)\n                        return None",
          probe_transient_connect),
        f("X12A-08", "X12.a", "connect_cycle", "last error is retained",
          "fixdata", "last_error = exc",
          'last_error = RuntimeError("wrong-last-error")', probe_last_connect_error),
        f("X12A-09", "X12.a", "connect_cycle", "exhaustion raises retained error",
          "fixdata", "raise last_error\n\n            try:",
          'raise RuntimeError("wrong-raised-error")\n\n            try:',
          probe_last_connect_error),
        f("X12B-01", "X12.b", "recovery", "callback is optional",
          "fixdata", "if port_recover_fn is not None:\n",
          "if True:\n", probe_no_recovery_without_callback),
        f("X12B-02", "X12.b", "recovery", "reason uses fetch vocabulary",
          "fixdata", "reason = _error(connect_exc)",
          "reason = str(connect_exc)", probe_recovery_success),
        f("X12B-03", "X12.b", "recovery", "state announces recovery request",
          "fixdata", 'set_port(port, "recovery requested")',
          'set_port(port, "recovering")', probe_recovery_success),
        f("X12B-04", "X12.b", "recovery", "callback receives exact reason",
          "fixdata", "port_recover_fn(port, reason) is True",
          'port_recover_fn(port, "wrong-reason") is True', probe_recovery_success),
        f("X12B-05", "X12.b", "recovery", "decline re-raises connect error",
          "fixdata", "if not recovered:\n                        raise\n",
          'if not recovered:\n                        raise RuntimeError("wrong-decline")\n',
          probe_recovery_declined),
        f("X12B-06", "X12.b", "recovery", "resumed cycle exposes attempt one",
          "fixdata", "adapter = connect_cycle(preserve_first_state=False)",
          "adapter = connect_cycle(preserve_first_state=True)",
          probe_recovery_success),
        f("X12B-07", "X12.b", "recovery", "exactly one resumed cycle runs",
          "fixdata", "adapter = connect_cycle(preserve_first_state=False)\n",
          "adapter = connect_cycle(preserve_first_state=False)\n"
          "                    adapter = connect_cycle(preserve_first_state=False)\n",
          probe_recovery_success),
        f("X13-01", "X13", "recovery rejection", "truthy non-True is rejected",
          "fixdata", "port_recover_fn(port, reason) is True",
          "bool(port_recover_fn(port, reason))", probe_truthy_recovery_rejected),
        f("X13-02", "X13", "recovery rejection", "BaseException is swallowed",
          "fixdata", "except BaseException as recover_exc:",
          "except Exception as recover_exc:", probe_raising_recovery_rejected),
        f("X13-03", "X13", "recovery rejection", "failure log shape is exact",
          "fixdata", "recovery callback failed: ",
          "recovery callback broke: ", probe_raising_recovery_rejected),
        f("X13-04", "X13", "recovery rejection", "raising callback stays unrecovered",
          "fixdata", 'f"{_error(recover_exc)}"))\n',
          'f"{_error(recover_exc)}"))\n                            recovered = True\n',
          probe_raising_recovery_rejected),
        f("X11-01", "X11", "worker death", "reason uses fetch vocabulary",
          "fixdata", "reason = _error(exc)\n"
          '                death_state = f"DEAD - {reason}"[:160]',
          "reason = str(exc)\n"
          '                death_state = f"DEAD - {reason}"[:160]',
          probe_worker_death_contract),
        f("X11-02", "X11", "worker death", "death state carries DEAD prefix",
          "fixdata", 'death_state = f"DEAD - {reason}"[:160]',
          'death_state = f"FAILED - {reason}"[:160]', probe_worker_death_contract),
        f("X11-03", "X11", "worker death", "death state is bounded to 160",
          "fixdata", 'death_state = f"DEAD - {reason}"[:160]',
          'death_state = f"DEAD - {reason}"[:80]', probe_long_worker_death_state),
        f("X11-04", "X11", "reconcile preservation",
          "guard requires a current item",
          "fixdata", "if (current is not None and current[0] == \"reconcile\"",
          "if (current is None and current[0] == \"reconcile\"",
          probe_reconcile_death_contract),
        f("X11-05", "X11", "reconcile preservation",
          "guard requires reconcile phase",
          "fixdata", 'current[0] == "reconcile"\n                        and current_inflight',
          'current[0] != "reconcile"\n                        and current_inflight',
          probe_reconcile_death_contract),
        f("X11-06", "X11", "reconcile preservation",
          "guard requires an in-flight request",
          "fixdata", "and current_inflight and current_index is not None):",
          "and not current_inflight and current_index is not None):",
          probe_reconcile_death_contract),
        f("X11-07", "X11", "reconcile preservation",
          "guard requires a current index",
          "fixdata", "and current_inflight and current_index is not None):",
          "and current_inflight and current_index is None):",
          probe_reconcile_death_contract),
        f("X11-08", "X11", "reconcile preservation",
          "death item detaches the exact planned row",
          "fixdata", 'death_item = dict(_row["reconciles"][_position])',
          "death_item = None", probe_reconcile_death_contract),
        f("X11-09", "X11", "reconcile preservation",
          "death result is normalized ambiguous",
          "fixdata", "death_item, _reconcile_failure(death_item, exc))",
          "death_item, _reconcile_failure(death_item, exc, ambiguous=False))",
          probe_reconcile_death_contract),
        f("X11-10", "X11", "worker death", "port_errors records the failure",
          "fixdata", 'out["port_errors"].append(f"port {port}: {reason}")',
          'out["port_errors"].append(f"broken {port}")',
          probe_worker_death_contract),
        f("X11-11", "X11", "worker death", "current row becomes inactive",
          "fixdata", 'row["active"] = False\n                        row["state"] = "AMBIGUOUS"',
          'row["active"] = True\n                        row["state"] = "AMBIGUOUS"',
          source_has("fixdata", 'row["active"] = False\n                        row["state"] = "AMBIGUOUS"')),
        f("X11-12", "X11", "worker death", "current row state is AMBIGUOUS",
          "fixdata", 'row["state"] = "AMBIGUOUS"\n                        position =',
          'row["state"] = "FAILED"\n                        position =',
          source_has("fixdata", 'row["state"] = "AMBIGUOUS"\n                        position =')),
        f("X11-13", "X11", "worker debt", "in-flight work is ambiguous",
          "fixdata", 'current_reason=("ambiguous"\n                                            if current_inflight',
          'current_reason=("wrong-current"\n                                            if current_inflight',
          probe_worker_death_contract),
        f("X11-14", "X11", "worker debt", "later work stays explicitly unstarted",
          "fixdata", 'current_reason=("ambiguous"\n                                            if current_inflight\n                                            or phase != "reconcile" else None),\n                            later_reason="worker-failed-unstarted"))',
          'current_reason=("ambiguous"\n                                            if current_inflight\n                                            or phase != "reconcile" else None),\n                            later_reason="wrong-later"))',
          probe_reconcile_death_contract),
        f("X11-15", "X11", "worker death", "halted count increments",
          "fixdata", 'out["halted"] += 1\n                        # In-flight work',
          'out["halted"] += 0\n                        # In-flight work',
          probe_worker_death_contract),
        f("X11-16", "X11", "worker death", "active worker count decrements",
          "fixdata", "# In-flight work is never silently retried.\n                        active -= 1",
          "# In-flight work is never silently retried.\n                        active -= 0",
          source_has("fixdata", "# In-flight work is never silently retried.\n                        active -= 1")),
        f("X11-17", "X11", "worker death", "waiters are notified",
          "fixdata", "                    changed.notify_all()\n                emit(\"log\", message=f\"port {port}: {death_state}\")",
          "                    pass\n                emit(\"log\", message=f\"port {port}: {death_state}\")",
          source_has("fixdata", "                    changed.notify_all()\n                emit(\"log\", message=f\"port {port}: {death_state}\")")),
        f("X11-18", "X11", "worker death", "death is emitted live",
          "fixdata", 'emit("log", message=f"port {port}: {death_state}")',
          'emit("log", message=f"worker {port}: {death_state}")',
          probe_worker_death_contract),
        f("X11-19", "X11", "worker cleanup", "adapter disconnect is attempted",
          "fixdata", "if adapter is not None:\n                    try:\n                        fib.without_authority(lambda: adapter.disconnect())()",
          "if False:\n                    try:\n                        fib.without_authority(lambda: adapter.disconnect())()",
          probe_disconnect_contract),
        f("X11-20", "X11", "worker cleanup", "disconnect failure is swallowed and logged",
          "fixdata", 'emit("log", message=f"port {port} disconnect: {_error(exc)}")',
          'emit("log", message=f"disconnect failed: {_error(exc)}")',
          probe_disconnect_contract),
        f("X11-21", "X11", "worker cleanup", "death state survives finally",
          "fixdata", 'set_port(port, death_state or "done")',
          'set_port(port, "done")', probe_worker_death_contract),
    )


def stock_fences() -> tuple[Fence, ...]:
    ratio_gate = (
        "if (VOL_VALUE_GATE and kind in ss.RATIO_KINDS\n"
        "                and any(value > VOL_HARD_CEILING\n"
        "                        for value in (o, h, lo, c))):"
    )
    q_export_context = (
        '"Checking bank health for export…",\n'
        '                }))\n'
        '                export_quality.ensure_current_health(root_dir)\n'
        '                q.put(("progress", {'
    )
    post_export_context = (
        '"Checking bank health for export…",\n'
        '                }, force=True)\n'
        '                export_quality.ensure_current_health(root_dir)\n'
        '                _post_progress({'
    )
    export_cases = (
        f("X18A-01", "X18.a", "pipeline absence",
          "Fix Data has no export/health dependency",
          "fixdata", "import run_log_retention\n",
          "import run_log_retention\nimport export_quality\n",
          probe_fixdata_has_no_health_dependency),
        f("X18A-02", "X18.a", "pipeline default",
          "health_fn=None emits no health report or health failure",
          "fixdata", "if health_fn is not None:\n                        try:",
          "if True:\n                        try:",
          probe_fixdata_default_skips_health),
        f("X18B-01", "X18.b", "ensure_current_health",
          "cached report loads before currency evaluation",
          "export", "root = Path(root).resolve()\n    cached = health_report.load_report(root)",
          "root = Path(root).resolve()\n    _health_report_current(root, None)\n    cached = health_report.load_report(root)",
          probe_health_load_then_default_current),
        f("X18B-02", "X18.b", "ensure_current_health",
          "default currency check sees cached report",
          "export", "_health_report_current(root, cached)\n               if current_fn is None",
          "False\n               if current_fn is None",
          probe_health_load_then_default_current),
        f("X18B-03", "X18.b", "ensure_current_health",
          "injected current result is coerced with bool",
          "export", "else bool(current_fn(root)))",
          "else current_fn(root) is True)", probe_health_injected_bool),
        f("X18B-04", "X18.b", "ensure_current_health",
          "current cache returns without regeneration",
          "export", "if current:\n        return cached",
          "if current:\n        return None", probe_health_current_returns_cache),
        f("X18B-05", "X18.b", "ensure_current_health",
          "stale injected audit result is returned",
          "export", "if audit_fn is not None:\n        return audit_fn(root)",
          "if audit_fn is not None:\n        audit_fn(root)\n        return None",
          probe_health_injected_audit),
        f("X18B-06", "X18.b", "ensure_current_health",
          "default audit explicitly writes refreshed report",
          "export", "return health_report.audit(root, write=True)",
          "return health_report.audit(root, write=False)",
          probe_health_default_audit),
        f("X18B-07", "X18.b", "display callers",
          "all four on-demand health call sites remain",
          "display", "export_quality.ensure_current_health(\n"
          "                        self._storage_root, current_fn=lambda _root: False)",
          "export_quality.ensure_stale_health(\n"
          "                        self._storage_root, current_fn=lambda _root: False)",
          probe_display_four_health_calls),
        f("X18B-08", "X18.b", "display export caller",
          "queued export path refreshes health",
          "display", q_export_context,
          q_export_context.replace("ensure_current_health", "ensure_stale_health"),
          probe_display_export_health_q),
        f("X18B-09", "X18.b", "display export caller",
          "posted export path refreshes health",
          "display", post_export_context,
          post_export_context.replace(
              "ensure_current_health", "ensure_stale_health"),
          probe_display_export_health_post),
    )
    return (
        f("X16A-01", "X16.a", "split ratio ceiling", "gate constant is enabled",
          "stock", "VOL_VALUE_GATE = True", "VOL_VALUE_GATE = False",
          probe_ratio_ceiling_mixed),
        f("X16A-02", "X16.a", "split ratio ceiling", "only ratio kinds gate",
          "stock", ratio_gate,
          ratio_gate.replace("kind in ss.RATIO_KINDS",
                             "kind not in ss.RATIO_KINDS"),
          probe_ratio_ceiling_mixed),
        f("X16A-03", "X16.a", "split ratio ceiling", "any OHLC breach gates",
          "stock", ratio_gate,
          ratio_gate.replace("any(value", "all(value"),
          probe_ratio_ceiling_mixed),
        f("X16A-04", "X16.a", "split ratio ceiling", "open participates",
          "stock", ratio_gate,
          ratio_gate.replace("(o, h, lo, c)", "(h, lo, c)"),
          source_has("stock", ratio_gate)),
        f("X16A-05", "X16.a", "split ratio ceiling", "high participates",
          "stock", ratio_gate,
          ratio_gate.replace("(o, h, lo, c)", "(o, lo, c)"),
          ratio_field_probe("high")),
        f("X16A-06", "X16.a", "split ratio ceiling", "low participates",
          "stock", ratio_gate,
          ratio_gate.replace("(o, h, lo, c)", "(o, h, c)"),
          source_has("stock", ratio_gate)),
        f("X16A-07", "X16.a", "split ratio ceiling", "close participates",
          "stock", ratio_gate,
          ratio_gate.replace("(o, h, lo, c)", "(o, h, lo)"),
          source_has("stock", ratio_gate)),
        f("X16A-08", "X16.a", "split ratio ceiling", "comparison is strict",
          "stock", ratio_gate,
          ratio_gate.replace("value > VOL_HARD_CEILING",
                             "value >= VOL_HARD_CEILING"),
          probe_ratio_exact_ceiling_allowed),
        f("X16A-09", "X16.a", "split ratio ceiling", "ceiling is exactly 10.0",
          "stock", "VOL_HARD_CEILING = 10.0", "VOL_HARD_CEILING = 12.0",
          probe_ratio_ceiling_mixed),
        f("X16A-10", "X16.a", "split ratio ceiling", "rejection is loud",
          "stock", 'counters["invalid"] += 1\n            continue\n        if sentinel_vol:',
          'counters["invalid"] += 0\n            continue\n        if sentinel_vol:',
          probe_ratio_ceiling_mixed),
        f("X16A-11", "X16.a", "split ratio ceiling", "rejected bar drops",
          "stock", 'counters["invalid"] += 1\n            continue\n        if sentinel_vol:',
          'counters["invalid"] += 1\n            pass\n        if sentinel_vol:',
          probe_ratio_ceiling_mixed),

        f("X16B-01", "X16.b", "month guard", "disabled gate returns early",
          "stock", "if (not VOL_VALUE_GATE or ss.kind_of(interval) not in ss.RATIO_KINDS\n            or not added):",
          "if (VOL_VALUE_GATE or ss.kind_of(interval) not in ss.RATIO_KINDS\n            or not added):",
          probe_ratio_guard_halt),
        f("X16B-02", "X16.b", "month guard", "non-ratio kind returns early",
          "stock", "if (not VOL_VALUE_GATE or ss.kind_of(interval) not in ss.RATIO_KINDS\n            or not added):",
          "if (not VOL_VALUE_GATE or ss.kind_of(interval) in ss.RATIO_KINDS\n            or not added):",
          probe_ratio_guard_halt),
        f("X16B-03", "X16.b", "month guard", "empty added set returns early",
          "stock", "if (not VOL_VALUE_GATE or ss.kind_of(interval) not in ss.RATIO_KINDS\n            or not added):",
          "if (not VOL_VALUE_GATE or ss.kind_of(interval) not in ss.RATIO_KINDS\n            or added):",
          probe_ratio_guard_halt),
        f("X16B-04", "X16.b", "halt", "month status is HALTED",
          "stock", 'record = {"status": "HALTED", "reason": reason}',
          'record = {"status": "WARN", "reason": reason}',
          probe_ratio_guard_halt),
        f("X16B-05", "X16.b", "halt", "halt record reaches result months",
          "stock", 'res.setdefault("months", {})[key] = record',
          'res.setdefault("months", {})\n        record = dict(record)',
          probe_ratio_guard_halt),
        f("X16B-06", "X16.b", "halt", "SeriesHalt carries gate metadata",
          "stock", 'raise SeriesHalt(reason, metadata={"vol_value_gate": dict(record)})',
          'raise SeriesHalt(reason, metadata={})', probe_ratio_guard_halt),
        f("X16B-07", "X16.b", "manifest", "manifest read failure halts",
          "stock", 'halt(f"volatility unit gate could not read the manifest before "\n                 f"{key} ({exc}) — month left untouched")',
          "return", probe_ratio_guard_manifest_error),
        f("X16B-08", "X16.b", "prior discovery", "tree discovery failure halts",
          "stock", 'halt(f"volatility unit gate could not establish the prior committed "\n             f"month before {key} ({exc}) — month left untouched")',
          "return", probe_ratio_guard_tree_error),
        f("X16B-09", "X16.b", "prior discovery", "latest claimed month wins",
          "stock", "manifest_prior = max(claimed) if claimed else None",
          "manifest_prior = min(claimed) if claimed else None",
          probe_ratio_guard_manifest_latest),
        f("X16B-10", "X16.b", "prior discovery", "tree scan is bounded after manifest",
          "stock", "root, ticker, interval, ym, after=manifest_prior)",
          "root, ticker, interval, ym, after=None)",
          probe_ratio_guard_after_bound),
        f("X16B-11", "X16.b", "prior discovery", "newer tree month outranks manifest",
          "stock", "prior = tree_prior or manifest_prior",
          "prior = manifest_prior or tree_prior", probe_ratio_guard_tree_newer),
        f("X16B-12", "X16.b", "prior discovery", "fresh series is ceiling-only",
          "stock", "if prior is None:\n        return",
          "if False:\n        return", probe_ratio_guard_fresh),
        f("X16B-13", "X16.b", "prior read", "missing committed file halts",
          "stock", "if prior_path is None:\n        halt(",
          "if False:\n        halt(", probe_ratio_guard_missing),
        f("X16B-14", "X16.b", "prior read", "strict read failure halts",
          "stock", "prior_bars, _ = ss.read_month_file(prior_path)",
          "prior_bars, _ = ([], {})", probe_ratio_guard_read_error),
        f("X16B-15", "X16.b", "prior read", "non-positive median halts",
          "stock", "if prior_median is None or prior_median <= 0.0:\n        halt(",
          "if False:\n        halt(", probe_ratio_guard_nonpositive),
        f("X16B-16", "X16.b", "incoming", "missing incoming median returns",
          "stock", "if incoming_median is None:\n        return",
          "if False:\n        return", probe_ratio_guard_incoming_none),
        f("X16B-17", "X16.b", "threshold", "threshold uses UNIT_FLIP_RATIO",
          "stock", "threshold = UNIT_FLIP_RATIO * prior_median",
          "threshold = (UNIT_FLIP_RATIO + 10.0) * prior_median",
          probe_ratio_guard_halt),
        f("X16B-18", "X16.b", "threshold", "above-threshold comparison polarity",
          "stock", "incoming_median > threshold\n        or math.isclose",
          "incoming_median < threshold\n        or math.isclose",
          probe_ratio_guard_below),
        f("X16B-19", "X16.b", "threshold", "at-threshold is inclusive",
          "stock", "or math.isclose(incoming_median, threshold,\n                        rel_tol=1e-12, abs_tol=1e-15)",
          "or False", probe_ratio_guard_halt),
        f("X16B-20", "X16.b", "threshold", "relative tolerance is pinned",
          "stock", "rel_tol=1e-12, abs_tol=1e-15)",
          "rel_tol=0.0, abs_tol=1e-15)",
          probe_ratio_guard_relative_tolerance),
        f("X16B-21", "X16.b", "threshold", "absolute tolerance is pinned",
          "stock", "rel_tol=1e-12, abs_tol=1e-15)",
          "rel_tol=1e-12, abs_tol=0.0)",
          probe_ratio_guard_absolute_tolerance),
        f("X16B-22", "X16.b", "halt details", "ratio metadata is exact",
          "stock", '"ratio": UNIT_FLIP_RATIO,', '"ratio": 999.0,',
          probe_ratio_guard_halt),
    ) + export_cases


def fences() -> tuple[Fence, ...]:
    return fixdata_fences() + stock_fences()


def redundancy_cases() -> tuple[Redundancy, ...]:
    return ()


def run() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    kit = CheckKit()
    raw_before = {name: path.read_bytes() for name, path in TARGETS.items()}
    sources = {name: normalized_source(value) for name, value in raw_before.items()}
    cases = fences()
    redundancies = redundancy_cases()
    mutations_by_target = {
        name: tuple(
            (item.mutation.old, item.mutation.new)
            for item in (*cases, *redundancies)
            if item.mutation.target == name
        )
        for name in TARGETS
    }
    owners_by_target = {
        name: mutation_owners(sources[name], mutations)
        for name, mutations in mutations_by_target.items()
    }
    segment_hashes = {
        name: function_segment_sha256(sources[name], owners)
        for name, owners in owners_by_target.items()
    }
    module_owners_by_target = {
        name: tuple(sorted(
            owner for owner in owners if owner.startswith("<module>:")
        ))
        for name, owners in owners_by_target.items()
    }

    kit.section("Row 85 S5 custody and pre-import containment")
    for name in ("fixdata", "stock", "export", "display"):
        kit.check(
            f"SOURCE-CUSTODY pinned per-fenced-segment normalized {name} "
            "source segments",
            segment_hashes[name] == EXPECTED_FUNCTION_SEGMENT_SHA256[name],
            repr(segment_hashes[name]),
        )
        kit.check(
            f"SOURCE-CUSTODY {name} module-level anchor count is explicit",
            len(module_owners_by_target[name])
            == EXPECTED_MODULE_SEGMENT_COUNTS[name],
            repr(module_owners_by_target[name]),
        )
    locality_sources = {
        name: "def _source_custody_unrelated_probe():\n    return None\n\n"
        + source
        for name, source in sources.items()
    }
    kit.check(
        "SOURCE-CUSTODY top insertions leave all fenced digests unchanged",
        all(
            function_segment_sha256(locality_sources[name], owners)
            == segment_hashes[name]
            for name, owners in owners_by_target.items()
        ),
    )
    mutation_digest_proofs = []
    for name, source in sources.items():
        item = next(
            item for item in (*cases, *redundancies)
            if item.mutation.target == name
            and mutation_owner(
                source, item.mutation.old, item.mutation.new
            ) is not None
        )
        owner = mutation_owner(source, item.mutation.old, item.mutation.new)
        changed_source = item.mutation.apply(sources)[name]
        mutation_digest_proofs.append(
            function_segment_sha256(changed_source, (owner,))
            != {owner: segment_hashes[name][owner]}
        )
    kit.check(
        "SOURCE-CUSTODY fenced-segment mutations change owned digests",
        all(mutation_digest_proofs), repr(mutation_digest_proofs),
    )
    kit.check("CONTAINMENT exact synthetic ticker allowlist is safe",
              SYNTHETIC_TICKERS == frozenset({"S5X", "S5Y"})
              and SYNTHETIC_TICKERS.isdisjoint(FORBIDDEN_TICKERS),
              repr(sorted(SYNTHETIC_TICKERS & FORBIDDEN_TICKERS)))

    real_gate = importlib.import_module("operation_gate")
    real_acquire = real_gate.acquire
    real_gate_calls: list[tuple[tuple, dict]] = []

    def counted_real_acquire(*args: object, **kwargs: object) -> object:
        real_gate_calls.append((tuple(args), dict(kwargs)))
        return real_acquire(*args, **kwargs)

    real_gate.acquire = counted_real_acquire
    tripwire = EarlyTripwire()
    try:
        with tripwire:
            baseline = load_subjects(sources)

            counts = Counter(case.family for case in cases)
            ids = tuple(case.fence_id for case in cases)
            kit.check("HARNESS-INVENTORY fence IDs are unique",
                      len(ids) == len(set(ids)), repr(ids))
            kit.check("HARNESS-INVENTORY final code-derived fence count is 85",
                      len(cases) == 85, repr(dict(counts)))
            kit.check("HARNESS-INVENTORY per-family counts match derivation",
                      dict(counts) == EXPECTED_FENCE_COUNTS,
                      repr(dict(counts)))
            kit.check("CONSTANT-INVENTORY every scoped S5 constant is exact",
                      baseline.fixdata.CONNECT_ATTEMPTS == 3
                      and baseline.fixdata.CONNECT_RETRY_BACKOFF_S == 0.25
                      and baseline.stock.VOL_VALUE_GATE is True
                      and baseline.stock.VOL_HARD_CEILING == 10.0
                      and baseline.stock.UNIT_FLIP_RATIO == 50.0)
            kit.check("A15 PINNED-GRADE existing GUI AST contract is current",
                      a15_pinned_grade_contract(baseline.display_source),
                      "existing addstock_vol_debt_reference retains engine "
                      "truth plus cancel/pause/progress AST pins")

            kit.section("One named baseline check per S5 fence")
            for case in cases:
                passed, detail = evaluate(case.probe, baseline)
                kit.check(
                    f"FENCE {case.fence_id} {case.family} {case.description}",
                    passed, detail,
                )

            kit.section("One compiling in-memory mutation killed per S5 fence")
            for case in cases:
                try:
                    mutant_sources = case.mutation.apply(sources)
                    mutant = load_subjects(mutant_sources)
                except BaseException as exc:
                    kit.check(
                        f"MUTATION-KILLED {case.fence_id}", False,
                        f"mutant did not load: {type(exc).__name__}: {exc}",
                    )
                    continue
                survived, detail = evaluate(case.probe, mutant)
                kit.check(
                    f"MUTATION-KILLED {case.fence_id}", not survived,
                    "probe survived" if survived else detail,
                )

            for redundant in redundancies:
                del redundant
    except BaseException as exc:
        kit.check("HARNESS-INVENTORY source loads and bounded probes complete", False,
                  f"{type(exc).__name__}: {exc}")
    finally:
        real_gate.acquire = real_acquire

    kit.section("Containment and production custody")
    kit.check("CONTAINMENT fake operation gate is exercised",
              bool(locals().get("baseline")
                   and baseline.fake_gate_calls),
              repr(getattr(locals().get("baseline"), "fake_gate_calls", None)))
    kit.check("CONTAINMENT real operation gate acquire count is zero",
              not real_gate_calls, repr(real_gate_calls))
    kit.check("CONTAINMENT early network/process tripwire had zero denials",
              not tripwire.denials, repr(dict(tripwire.denials)))
    for name, path in TARGETS.items():
        after = path.read_bytes()
        kit.check(f"SOURCE-CUSTODY raw {name} bytes unchanged during run",
                  after == raw_before[name], hashlib.sha256(after).hexdigest())
        after_segment_hashes = function_segment_sha256(
            normalized_source(after), owners_by_target[name])
        kit.check(
            f"SOURCE-CUSTODY fenced-segment {name} digests unchanged during run",
            after_segment_hashes == segment_hashes[name],
            repr(after_segment_hashes),
        )
    return kit.finish()


if __name__ == "__main__":
    raise SystemExit(run())
