"""Mutation reference for the Add Stocks watchdog maintenance-lease layer.

Row 85 slice S4a covers the 17 functions from ``_utc_now`` through
``maintenance_window``.  Mutants execute only in memory; filesystem cases use
fresh nested temporary roots, and process identity reaches only this harness's
own PID or an in-memory Windows-API proxy.
"""

from __future__ import annotations

import ast
from collections import Counter
from contextlib import contextmanager
import ctypes as real_ctypes
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os as real_os
from pathlib import Path
import shutil
import sys
import tempfile
import types
from typing import Callable, Iterator


ENGINE_ROOT = Path(__file__).resolve().parent
TARGET = ENGINE_ROOT / "addstock_watchdog.py"
EXPECTED_FUNCTION_SEGMENT_SHA256 = {
    "<module>:_LEASE_RE": "03898927b16861f2084ab4fac2358b2c7e7452e6be992142e2a8282aa3832204",
    "MaintenanceLease.published": "2834ac24ba0a674f16c31c7b56aee9150fcffad3975125c0d9a0f2e9149596b9",
    "_atomic_write": "d0f590286d21402801c314d805eed01b582b4aac389c2eae8f7b9d3b6a8beb66",
    "_aware_utc": "276d8e960a00a26ba90e16c0245ad54da162cbb6c6a65e546afc8082c36cfc78",
    "_normalize_ports": "5e212e3b52825396f39f9636bab2fb229c37e22aafeaf42f81917cd98027cef7",
    "_parse_timestamp": "c2b1096ac47811acbd9dfaa7d6adf1b68ac5e3cc99a4e5536ae1f93a58fb6cda",
    "_process_identity_from_handle": "1e320d750b051bcdb83421cb3e6bad6b040c0b0af65a9362927b7e7b1b1a4212",
    "_read_bytes": "26fbe362249cef0e0ed30ec92ff78cb736a22eba8fcb814548091e8ce0796a95",
    "_timestamp": "e6542fc9c0ae21370a3401368707c950d616f160865796e7d50539bb1fb18d1e",
    "_utc_now": "1b533d13b4533c56f8f80a28372057b137faa31d505e4750978fa85be6e522dd",
    "begin_maintenance": "38d31dea2657e13b45ad7f1f50e67b9cc0392250e58d40543bb7ab9e6a90c18a",
    "end_maintenance": "5c569b38fc870ea0fcb474460dfdca5d0cee0e08625595b49606ed2f678917a0",
    "get_process_identity": "525e82fc8c3ac8f6c1f481e1d08dc5419ed0c1218ee5f070a2a4e3d1bc54bf7d",
    "load_maintenance": "3cda4160f2cf68ff91e4ee7dc7c7cf3928c160fcfa3915563beafab787ce3ec6",
    "maintenance_budget": "219d98b323945011dc3234378db8c94e82b134d2b4070e3c3da7a392c367c1e9",
    "maintenance_path": "de67b96c08afb2d25be79c9e764fc0289ed9ca3af9ed2765e65f82985923652b",
    "maintenance_status": "bf0457f67acba780ef74111fda4ba3f15394faac7f0ecd38f5e6e033f67f03e5",
    "maintenance_window": "35776689433096355b916fffcbb556506fc7ffa8c2f60b46b23e29b5597462e2",
    "validate_maintenance": "63b9c533cf12a440551a466e416f6239caa8c1232a01ab57145e8f3a747f656f",
}
FORBIDDEN_TICKERS = frozenset({"APO", "JCI", "OKE", "TKO", "WBD", "TMUS"})
SYNTHETIC_TICKERS: frozenset[str] = frozenset()
STAMP = "2026-08-04T12:00:00.000Z"
NOW = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
LATER = datetime(2026, 8, 4, 13, 0, tzinfo=timezone.utc)
LOCAL = timezone(timedelta(hours=2))


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
    name = "_addstock_watchdog_lease_fence_subject"
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


def raises_exact(call: Callable[[], object], error_type: type[BaseException],
                 contains: str | None = None) -> bool:
    try:
        call()
    except BaseException as exc:
        return type(exc) is error_type and (
            contains is None or contains in str(exc))
    return False


def with_attr(module: types.ModuleType, name: str, value: object,
              call: Callable[[], bool]) -> bool:
    previous = getattr(module, name)
    setattr(module, name, value)
    try:
        return call()
    finally:
        setattr(module, name, previous)


_TEMP_BASES: list[Path] = []
_RESIDUE: list[str] = []
_OUTSIDE: list[str] = []


def _inside(base: Path, path: object) -> bool:
    try:
        Path(path).resolve(strict=False).relative_to(base.resolve())
        return True
    except (OSError, TypeError, ValueError):
        return False


def guard_paths(base: Path, *paths: object) -> bool:
    ok = True
    for path in paths:
        if not _inside(base, path):
            _OUTSIDE.append(str(path))
            ok = False
    return ok


@contextmanager
def sandbox(label: str) -> Iterator[tuple[Path, Path]]:
    base = Path(tempfile.mkdtemp(prefix=f"row85_s4a_{label}_")).resolve()
    _TEMP_BASES.append(base)
    fleet = base / "fleet"
    fleet.mkdir(parents=True)
    try:
        yield base, fleet
        for path in base.rglob("*"):
            guard_paths(base, path)
    finally:
        shutil.rmtree(base, ignore_errors=True)
        if base.exists():
            _RESIDUE.append(str(base))


def lease_payload(module: types.ModuleType, *,
                  lease_id: object = "a" * 32,
                  owner_pid: object = 4242,
                  creation: object = 123456,
                  started_at: object = STAMP,
                  budget: object = 60.0,
                  ports: object = (2000, 3000),
                  schema: object = 1) -> dict[str, object]:
    return {
        "schema": schema,
        "lease_id": lease_id,
        "owner_pid": owner_pid,
        "owner_creation_filetime": creation,
        "started_at": started_at,
        "budget_seconds": budget,
        "ports": list(ports) if isinstance(ports, (list, tuple)) else ports,
    }


def write_payload(module: types.ModuleType, path: Path,
                  data: dict[str, object]) -> bytes:
    raw = (json.dumps(data, sort_keys=True, separators=(",", ":")) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return raw


class _Callable:
    def __init__(self, call: Callable[..., object]) -> None:
        self.call = call
        self.argtypes = None
        self.restype = None

    def __call__(self, *args: object) -> object:
        return self.call(*args)


class _Kernel:
    def __init__(self, *, open_handle: int = 99,
                 get_times: bool = True, high: int = 2, low: int = 3) -> None:
        self.closed: list[object] = []
        self.opened: list[tuple[object, ...]] = []
        self.OpenProcess = _Callable(self._open)
        self.CloseHandle = _Callable(self._close)
        self.GetProcessTimes = _Callable(
            lambda handle, creation, exit_time, kernel, user:
            self._times(handle, creation, exit_time, kernel, user))
        self.open_handle = open_handle
        self.get_times = get_times
        self.high = high
        self.low = low

    def _open(self, *args: object) -> int:
        self.opened.append(args)
        return self.open_handle

    def _close(self, handle: object) -> bool:
        self.closed.append(handle)
        return True

    def _times(self, _handle: object, creation: object, *_rest: object) -> bool:
        if not self.get_times:
            return False
        target = creation._obj
        target.dwHighDateTime = self.high
        target.dwLowDateTime = self.low
        return True


def ctypes_proxy(kernel: _Kernel) -> object:
    return types.SimpleNamespace(
        WinDLL=lambda *_args, **_kwargs: kernel,
        POINTER=real_ctypes.POINTER,
        byref=real_ctypes.byref,
        get_last_error=lambda: 5,
    )


def os_proxy(*, name: str = "nt", pid: int | None = None,
             replace: Callable[..., object] = real_os.replace) -> object:
    return types.SimpleNamespace(
        name=name,
        getpid=lambda: real_os.getpid() if pid is None else pid,
        replace=replace,
    )


def probe_utc_now(module: types.ModuleType) -> bool:
    before = datetime.now(timezone.utc)
    value = module._utc_now()
    after = datetime.now(timezone.utc)
    return (isinstance(value, datetime) and value.tzinfo == timezone.utc
            and before <= value <= after)


def probe_aware(module: types.ModuleType, mode: str) -> bool:
    if mode == "callable":
        calls: list[int] = []
        value = module._aware_utc(
            lambda: calls.append(1) or datetime(2026, 8, 4, 14, tzinfo=LOCAL))
        return calls == [1] and value == NOW
    if mode == "none":
        return with_attr(
            module, "_utc_now", lambda: NOW,
            lambda: module._aware_utc(None) == NOW)
    if mode == "type":
        return raises_exact(
            lambda: module._aware_utc("not-a-clock"), TypeError,
            "wall clock must return datetime")
    if mode == "naive":
        value = module._aware_utc(datetime(2026, 8, 4, 12))
        return value == NOW and value.tzinfo == timezone.utc
    value = module._aware_utc(datetime(2026, 8, 4, 14, tzinfo=LOCAL))
    return value == NOW and value.tzinfo == timezone.utc


def probe_timestamp(module: types.ModuleType) -> bool:
    return module._timestamp(datetime(
        2026, 8, 4, 14, 0, 0, 123456, tzinfo=LOCAL)) \
        == "2026-08-04T12:00:00.123Z"


def probe_parse(module: types.ModuleType, mode: str) -> bool:
    if mode == "type":
        return raises_exact(
            lambda: module._parse_timestamp(123), module.MaintenanceError,
            "started_at is invalid")
    if mode == "length":
        long_valid = "2026-08-04T12:00:00." + "1" * 25 + "+00:00"
        return len(long_valid) > 40 and raises_exact(
            lambda: module._parse_timestamp(long_valid), module.MaintenanceError,
            "started_at is invalid")
    if mode == "invalid":
        return raises_exact(
            lambda: module._parse_timestamp("not-a-time"), module.MaintenanceError,
            "started_at is invalid")
    if mode == "naive":
        return raises_exact(
            lambda: module._parse_timestamp("2026-08-04T12:00:00"),
            module.MaintenanceError, "requires a timezone")
    if mode == "z":
        parsed = module._parse_timestamp(STAMP)
        return parsed == NOW and parsed.tzinfo == timezone.utc
    parsed = module._parse_timestamp("2026-08-04T14:00:00+02:00")
    return parsed == NOW and parsed.tzinfo == timezone.utc


def probe_maintenance_path(module: types.ModuleType, mode: str) -> bool:
    with sandbox("path") as (base, fleet):
        if mode == "file":
            value = module.maintenance_path(str(fleet / "FLEET.JSON"))
            expected = fleet / module.MAINTENANCE_NAME
        else:
            value = module.maintenance_path(str(fleet))
            expected = fleet / module.MAINTENANCE_NAME
        return isinstance(value, Path) and value == expected and guard_paths(base, value)


def probe_budget(module: types.ModuleType, mode: str) -> bool:
    if mode == "zero":
        return (module.maintenance_budget(None, per_port_s=10, overhead_s=5) == 15
                and module.maintenance_budget(0, per_port_s=10, overhead_s=5) == 15)
    if mode == "normal":
        return module.maintenance_budget("2", per_port_s="10", overhead_s="5") == 25
    if mode == "floor":
        return module.maintenance_budget(1, per_port_s=-10, overhead_s=-10) == 1.0
    value = module.maintenance_budget(
        100000, per_port_s=100000, overhead_s=100000)
    return value == module.MAINTENANCE_MAX_BUDGET_S


def probe_identity_handle(module: types.ModuleType, mode: str) -> bool:
    kernel = _Kernel(get_times=(mode != "failure"), high=2, low=3)
    old = module.ctypes
    module.ctypes = ctypes_proxy(kernel)
    try:
        if mode == "failure":
            return raises_exact(
                lambda: module._process_identity_from_handle(99), OSError,
                "GetProcessTimes failed")
        return module._process_identity_from_handle(99) == {
            "creation_filetime": (2 << 32) | 3}
    finally:
        module.ctypes = old


def probe_get_identity(module: types.ModuleType, mode: str) -> bool:
    if mode == "self":
        first = module.get_process_identity(real_os.getpid())
        second = module.get_process_identity(str(real_os.getpid()))
        return (first == second and first["pid"] == real_os.getpid()
                and isinstance(first["creation_filetime"], int)
                and first["creation_filetime"] > 0)
    if mode == "bad_pid":
        return all(raises_exact(
            lambda value=value: module.get_process_identity(value), OSError,
            "available only for a Windows PID") for value in (0, -1))
    if mode == "nonwindows":
        return with_attr(
            module, "os", os_proxy(name="posix"),
            lambda: raises_exact(
                lambda: module.get_process_identity(real_os.getpid()), OSError,
                "available only for a Windows PID"))
    if mode == "open_failure":
        kernel = _Kernel(open_handle=0)
        old_ctypes = module.ctypes
        old_helper = module._process_identity_from_handle
        module.ctypes = ctypes_proxy(kernel)
        module._process_identity_from_handle = lambda _handle: {
            "creation_filetime": 123}
        try:
            return raises_exact(
                lambda: module.get_process_identity(4242), OSError,
                "OpenProcess failed")
        finally:
            module.ctypes = old_ctypes
            module._process_identity_from_handle = old_helper
    kernel = _Kernel(open_handle=99)
    old_ctypes = module.ctypes
    old_helper = module._process_identity_from_handle
    module.ctypes = ctypes_proxy(kernel)
    module._process_identity_from_handle = lambda _handle: {
        "creation_filetime": 123}
    try:
        value = module.get_process_identity(4242)
        return (value == {"creation_filetime": 123, "pid": 4242}
                and kernel.opened == [(0x1000, False, 4242)]
                and kernel.closed == [99])
    finally:
        module.ctypes = old_ctypes
        module._process_identity_from_handle = old_helper


def probe_ports(module: types.ModuleType, mode: str) -> bool:
    if mode == "normal":
        return module._normalize_ports(["3000", 2000, 3000]) == (3000, 2000)
    if mode == "bool":
        return raises_exact(
            lambda: module._normalize_ports([True]), module.MaintenanceError,
            "maintenance port is invalid")
    if mode == "invalid":
        return all(raises_exact(
            lambda value=value: module._normalize_ports([value]),
            module.MaintenanceError, "maintenance port is invalid")
            for value in (object(), "bad"))
    if mode == "bounds":
        return (module._normalize_ports([1, 65535]) == (1, 65535)
                and all(raises_exact(
                    lambda value=value: module._normalize_ports([value]),
                    module.MaintenanceError, "maintenance port is invalid")
                    for value in (0, 65536)))
    if mode == "empty":
        return all(raises_exact(
            lambda value=value: module._normalize_ports(value),
            module.MaintenanceError, "empty or oversized")
            for value in (None, []))
    old = module.MAINTENANCE_MAX_PORTS
    module.MAINTENANCE_MAX_PORTS = 3
    try:
        return raises_exact(
            lambda: module._normalize_ports([1, 2, 3, 4]),
            module.MaintenanceError, "empty or oversized")
    finally:
        module.MAINTENANCE_MAX_PORTS = old


def probe_validate(module: types.ModuleType, mode: str) -> bool:
    data = lease_payload(module)
    if mode == "valid":
        data["started_at"] = "2026-08-04T14:00:00+02:00"
        data["budget_seconds"] = 60
        data["ports"] = ["3000", 2000, 3000]
        result = module.validate_maintenance(data)
        return result == {
            "schema": module.MAINTENANCE_SCHEMA,
            "lease_id": "a" * 32,
            "owner_pid": 4242,
            "owner_creation_filetime": 123456,
            "started_at": STAMP,
            "budget_seconds": 60.0,
            "ports": [3000, 2000],
        } and type(result["budget_seconds"]) is float \
            and type(result["ports"]) is list
    if mode == "type":
        class PayloadLike:
            def __init__(self, values: dict[str, object]) -> None:
                self.values = values

            def __iter__(self):
                return iter(self.values)

            def get(self, key: str, default: object = None) -> object:
                return self.values.get(key, default)

        shaped = PayloadLike(data)
        return raises_exact(
            lambda: module.validate_maintenance(shaped), module.MaintenanceError,
            "invalid shape")
    if mode == "keys":
        data["extra"] = True
        return raises_exact(
            lambda: module.validate_maintenance(data), module.MaintenanceError,
            "invalid shape")
    if mode == "schema":
        data["schema"] = 2
        return raises_exact(
            lambda: module.validate_maintenance(data), module.MaintenanceError,
            "schema is unsupported")
    if mode == "lease_type":
        data["lease_id"] = 123
        expected = "lease_id is invalid"
    elif mode == "lease_format":
        data["lease_id"] = "g" * 32
        expected = "lease_id is invalid"
    elif mode == "pid_bool":
        data["owner_pid"] = True
        expected = "owner_pid is invalid"
    elif mode == "pid_type":
        data["owner_pid"] = "4242"
        expected = "owner_pid is invalid"
    elif mode == "pid_bound":
        data["owner_pid"] = 0
        expected = "owner_pid is invalid"
    elif mode == "creation_bool":
        data["owner_creation_filetime"] = True
        expected = "owner identity is invalid"
    elif mode == "creation_type":
        data["owner_creation_filetime"] = "123"
        expected = "owner identity is invalid"
    elif mode == "creation_bound":
        data["owner_creation_filetime"] = -1
        expected = "owner identity is invalid"
    elif mode == "budget_bool":
        data["budget_seconds"] = True
        expected = "budget is invalid"
    elif mode == "budget_type":
        data["budget_seconds"] = "60"
        expected = "budget is invalid"
    elif mode == "budget_low":
        data["budget_seconds"] = 0
        expected = "budget is invalid"
    elif mode == "budget_high":
        data["budget_seconds"] = module.MAINTENANCE_MAX_BUDGET_S + 1
        expected = "budget is invalid"
    elif mode == "timestamp":
        data["started_at"] = "not-a-time"
        expected = "started_at is invalid"
    else:
        data["ports"] = []
        expected = "ports are empty or oversized"
    return raises_exact(
        lambda: module.validate_maintenance(data), module.MaintenanceError,
        expected)


def probe_lease_anchor_redundancy(module: types.ModuleType) -> bool:
    valid = module.validate_maintenance(lease_payload(module))
    invalid = lease_payload(module, lease_id="a" * 32 + "b")
    return (valid["lease_id"] == "a" * 32
            and raises_exact(
                lambda: module.validate_maintenance(invalid),
                module.MaintenanceError, "lease_id is invalid"))


def probe_schema_bool_known_defect(module: types.ModuleType) -> bool:
    result = module.validate_maintenance(
        lease_payload(module, schema=True))
    return result["schema"] == module.MAINTENANCE_SCHEMA \
        and type(result["schema"]) is int


class _FaultPath:
    def __init__(self, *, size: int = 1, payload: bytes = b"x",
                 stat_error: BaseException | None = None,
                 read_error: BaseException | None = None) -> None:
        self.size = size
        self.payload = payload
        self.stat_error = stat_error
        self.read_error = read_error

    def stat(self) -> object:
        if self.stat_error:
            raise self.stat_error
        return types.SimpleNamespace(st_size=self.size)

    def read_bytes(self) -> bytes:
        if self.read_error:
            raise self.read_error
        return self.payload


def probe_read_bytes(module: types.ModuleType, mode: str) -> bool:
    if mode == "stat":
        path = _FaultPath(stat_error=OSError("stat boom"))
        return raises_exact(
            lambda: module._read_bytes(path), module.MaintenanceError,
            "flag is unavailable")
    if mode == "low":
        path = _FaultPath(size=0, payload=b"")
        return raises_exact(
            lambda: module._read_bytes(path), module.MaintenanceError,
            "empty or oversized")
    if mode == "high":
        exact = _FaultPath(
            size=module.MAINTENANCE_MAX_BYTES,
            payload=b"x" * module.MAINTENANCE_MAX_BYTES)
        over = _FaultPath(
            size=module.MAINTENANCE_MAX_BYTES + 1, payload=b"x")
        return (module._read_bytes(exact) == exact.payload
                and raises_exact(lambda: module._read_bytes(over),
                                 module.MaintenanceError,
                                 "empty or oversized"))
    if mode == "read":
        path = _FaultPath(size=7, payload=b"payload")
        return module._read_bytes(path) == b"payload"
    path = _FaultPath(size=7, read_error=OSError("read boom"))
    return raises_exact(
        lambda: module._read_bytes(path), module.MaintenanceError,
        "flag is unreadable")


def probe_load(module: types.ModuleType, mode: str) -> bool:
    with sandbox("load") as (_base, fleet):
        path = fleet / "flag.json"
        if mode == "valid":
            write_payload(module, path, lease_payload(module))
            return module.load_maintenance(str(path)) == module.validate_maintenance(
                lease_payload(module))
        if mode == "unicode":
            path.write_bytes(b"\xff\xfe")
        elif mode == "json":
            path.write_bytes(b"{]")
        else:
            bad = lease_payload(module)
            bad["schema"] = 2
            write_payload(module, path, bad)
        if mode in {"unicode", "json"}:
            return raises_exact(
                lambda: module.load_maintenance(path), module.MaintenanceError,
                "not valid JSON")
        return raises_exact(
            lambda: module.load_maintenance(path), module.MaintenanceError,
            "schema is unsupported")


def probe_status(module: types.ModuleType, mode: str) -> bool:
    with sandbox("status") as (_base, fleet):
        path = fleet / module.MAINTENANCE_NAME
        if mode == "missing":
            result = module.maintenance_status(str(path), now=NOW)
            return (type(result) is module.MaintenanceStatus
                    and not result.valid and result.reason == "missing")
        if mode == "invalid":
            path.write_bytes(b"{torn")
            result = module.maintenance_status(path, now=NOW)
            return not result.valid and "JSON" in result.reason
        data = lease_payload(module)
        if mode == "started":
            data["started_at"] = "2026-08-04T11:00:00.000Z"
        write_payload(module, path, data)
        calls: list[int] = []

        def identity(pid: int) -> dict[str, int]:
            calls.append(pid)
            if mode == "identity_error":
                raise RuntimeError("identity boom")
            if mode == "pid_default":
                return {"creation_filetime": data["owner_creation_filetime"]}
            return {
                "pid": pid + (1 if mode == "pid" else 0),
                "creation_filetime": (
                    999 if mode == "creation" else
                    str(data["owner_creation_filetime"])
                    if mode == "creation_coerce" else
                    data["owner_creation_filetime"]),
            }

        if mode in {"pid", "creation", "identity_error", "dead_payload"}:
            if mode == "dead_payload":
                identity = lambda pid: {"pid": pid + 1,
                                        "creation_filetime": 123456}
            result = module.maintenance_status(
                path, now=NOW, identity_fn=identity, margin_s=0)
            return (not result.valid and result.reason == "dead_owner"
                    and result.ports == (2000, 3000)
                    and result.payload == module.validate_maintenance(data)
                    and (mode == "dead_payload" or calls == [4242]))
        if mode == "boundary":
            exact = module.maintenance_status(
                path, now=NOW + timedelta(seconds=60),
                identity_fn=identity, margin_s=-100)
            over = module.maintenance_status(
                path, now=NOW + timedelta(seconds=60, microseconds=1),
                identity_fn=identity, margin_s=-100)
            return (exact.valid and exact.reason == "valid"
                    and exact.expires_at == NOW + timedelta(seconds=60)
                    and not over.valid and over.reason == "expired"
                     and over.expires_at == exact.expires_at)
        if mode == "started":
            result = module.maintenance_status(
                path, now=datetime(2026, 8, 4, 11, 0, 10, tzinfo=timezone.utc),
                identity_fn=identity, margin_s=5)
            return (result.valid and result.reason == "valid"
                    and result.expires_at
                    == datetime(2026, 8, 4, 11, 1, 5, tzinfo=timezone.utc)
                    and calls == [4242])
        clock_calls: list[int] = []
        result = module.maintenance_status(
            path, now=lambda: clock_calls.append(1) or
            datetime(2026, 8, 4, 14, 0, 10, tzinfo=LOCAL),
            identity_fn=identity, margin_s=5)
        return (result.valid and result.reason == "valid"
                and result.ports == (2000, 3000)
                and result.expires_at == NOW + timedelta(seconds=65)
                and result.payload == module.validate_maintenance(data)
                and calls == [4242] and clock_calls == [1])


def probe_atomic(module: types.ModuleType, mode: str) -> bool:
    with sandbox("atomic") as (base, fleet):
        target = fleet / "nested" / "flag.json"
        calls: list[tuple[Path, Path]] = []

        def replace(source: object, destination: object) -> None:
            source_path, destination_path = Path(source), Path(destination)
            calls.append((source_path, destination_path))
            if mode == "failure":
                raise OSError("replace boom")
            real_os.replace(source_path, destination_path)

        if mode == "failure":
            failed = raises_exact(
                lambda: module._atomic_write(target, b"payload", replace_fn=replace),
                OSError, "replace boom")
            temps = list(target.parent.glob(f".{target.name}.*.tmp")) \
                if target.parent.exists() else []
            return (failed and len(calls) == 1 and not target.exists() and not temps
                    and guard_paths(base, calls[0][0], calls[0][1]))
        result = module._atomic_write(target, b"payload", replace_fn=replace)
        source = calls[0][0]
        parts = source.name.split(".")
        return (result is None and target.read_bytes() == b"payload"
                and len(calls) == 1 and calls[0][1] == target
                and source.parent == target.parent
                and source.name.startswith(f".{target.name}.{real_os.getpid()}.")
                and source.name.endswith(".tmp")
                and len(parts[-2]) == 16
                and all(ch in "0123456789abcdef" for ch in parts[-2])
                and not source.exists() and guard_paths(base, source, target))


def identity(pid: object, creation: int = 123456) -> dict[str, int]:
    return {"pid": int(pid), "creation_filetime": creation}


def probe_lease_property(module: types.ModuleType) -> bool:
    path = Path("synthetic")
    return (module.MaintenanceLease(path, "a" * 32, None).published is True
            and module.MaintenanceLease(path, None, None).published is False
            and module.MaintenanceLease(
                path, "a" * 32, None, "error").published is False)


def probe_begin(module: types.ModuleType, mode: str) -> bool:
    with sandbox("begin") as (base, fleet):
        replace_calls: list[tuple[Path, Path]] = []

        def replace(source: object, destination: object) -> None:
            replace_calls.append((Path(source), Path(destination)))
            if mode == "replace_error":
                raise OSError("publish boom")
            real_os.replace(source, destination)

        if mode in {"ports_error", "budget_low", "budget_high",
                    "identity_error", "replace_error"}:
            ports: object = [True] if mode == "ports_error" else [2000]
            budget = (0 if mode == "budget_low" else
                      module.MAINTENANCE_MAX_BUDGET_S + 1
                      if mode == "budget_high" else 60)

            identity_calls: list[int] = []

            def identify(pid: object) -> dict[str, int]:
                identity_calls.append(int(pid))
                if mode == "identity_error":
                    raise OSError("identity boom")
                return identity(pid)

            lease = module.begin_maintenance(
                fleet, ports, budget, now=NOW, identity_fn=identify,
                replace_fn=replace)
            return (type(lease) is module.MaintenanceLease
                    and not lease.published and lease.lease_id is None
                    and lease.error is not None
                    and not lease.path.exists()
                    and ((not identity_calls)
                         if mode in {"ports_error", "budget_low", "budget_high"}
                         else identity_calls == [real_os.getpid()]))
        if mode == "oversized":
            old = module.MAINTENANCE_MAX_BYTES
            module.MAINTENANCE_MAX_BYTES = 10
            try:
                lease = module.begin_maintenance(
                    fleet, [2000], 60, now=NOW, identity_fn=identity,
                    replace_fn=replace)
            finally:
                module.MAINTENANCE_MAX_BYTES = old
            return (not lease.published and "oversized" in lease.error
                    and not replace_calls and not lease.path.exists())
        if mode in {"nested", "nested_order", "different_creation",
                    "different_pid", "invalid"}:
            path = module.maintenance_path(fleet)
            if mode == "invalid":
                path.write_bytes(b"{broken")
                before = None
            elif mode == "different_pid":
                before_data = lease_payload(
                    module, owner_pid=9999, creation=123456,
                    ports=[3000], budget=30)
                before = write_payload(module, path, before_data)
            else:
                first = module.begin_maintenance(
                    fleet, [1] if mode == "nested_order" else [3000],
                    30, now=NOW, identity_fn=identity)
                before = path.read_bytes()
                if not first.published:
                    return False
            identify = (lambda pid: identity(pid, 654321)) \
                if mode == "different_creation" else identity
            second = module.begin_maintenance(
                fleet, [8] if mode == "nested_order" else [2000],
                100, now=NOW + timedelta(seconds=10),
                identity_fn=identify)
            loaded = module.load_maintenance(path)
            if mode in {"nested", "nested_order"}:
                expected_ports = [1, 8] if mode == "nested_order" else [2000, 3000]
                return (second.published and second.previous == before
                        and loaded["ports"] == expected_ports
                        and loaded["started_at"] == STAMP
                        and loaded["budget_seconds"] == 110.0)
            return (second.published and second.previous is None
                    and loaded["ports"] == [2000]
                    and loaded["started_at"] == "2026-08-04T12:00:10.000Z")
        identity_calls: list[int] = []

        def identify(pid: object) -> dict[str, object]:
            identity_calls.append(int(pid))
            return {"pid": int(pid), "creation_filetime": "123456"}

        lease = module.begin_maintenance(
            str(fleet), [3000, "2000", 3000], "60",
            now=lambda: datetime(2026, 8, 4, 14, tzinfo=LOCAL),
            identity_fn=identify, replace_fn=replace)
        raw = lease.path.read_bytes()
        loaded = module.load_maintenance(lease.path)
        expected_raw = (json.dumps(
            loaded, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        return (type(lease) is module.MaintenanceLease and lease.published
                and lease.previous is None and lease.error is None
                and len(lease.lease_id) == 32
                and loaded["lease_id"] == lease.lease_id
                and loaded["owner_pid"] == real_os.getpid()
                and loaded["owner_creation_filetime"] == 123456
                and loaded["ports"] == [3000, 2000]
                and loaded["started_at"] == STAMP
                and loaded["budget_seconds"] == 60.0
                and identity_calls == [real_os.getpid()]
                and raw == expected_raw and raw.endswith(b"\n") and b" " not in raw
                and len(replace_calls) == 1
                and guard_paths(base, lease.path,
                                replace_calls[0][0], replace_calls[0][1]))


class _UnlinkFailure:
    def unlink(self) -> None:
        raise OSError("unlink boom")


def probe_end(module: types.ModuleType, mode: str) -> bool:
    if mode == "type":
        path = Path("synthetic")
        return (module.end_maintenance(object()) is False
                and module.end_maintenance(
                    module.MaintenanceLease(path, None, None)) is False
                and module.end_maintenance(module.MaintenanceLease(
                    path, "a" * 32, None, "error")) is False)
    if mode == "unlink_error":
        lease = module.MaintenanceLease(
            _UnlinkFailure(), "a" * 32, None)
        old = module.load_maintenance
        module.load_maintenance = lambda _path: {"lease_id": "a" * 32}
        try:
            return module.end_maintenance(lease) is False
        finally:
            module.load_maintenance = old
    with sandbox("end") as (_base, fleet):
        if mode == "unpublished":
            path = module.maintenance_path(fleet)
            write_payload(module, path, lease_payload(module))
            lease = module.MaintenanceLease(
                path, "a" * 32, None, "error")
            return (module.end_maintenance(lease) is False
                    and path.exists())
        if mode == "missing":
            lease = module.MaintenanceLease(
                module.maintenance_path(fleet), "a" * 32, None)
            return module.end_maintenance(lease) is False
        first = module.begin_maintenance(
            fleet, [2000], 30, now=NOW, identity_fn=identity)
        if not first.published:
            return False
        first_raw = first.path.read_bytes()
        if mode == "unlink":
            return module.end_maintenance(first) and not first.path.exists()
        second = module.begin_maintenance(
            fleet, [3000], 60, now=NOW + timedelta(seconds=1),
            identity_fn=identity)
        second_raw = second.path.read_bytes()
        if mode == "mismatch":
            return (not module.end_maintenance(first)
                    and second.path.read_bytes() == second_raw)
        calls: list[tuple[Path, Path]] = []

        def replace(source: object, destination: object) -> None:
            calls.append((Path(source), Path(destination)))
            if mode == "replace_error":
                raise OSError("restore boom")
            real_os.replace(source, destination)

        result = module.end_maintenance(second, replace_fn=replace)
        if mode == "replace_error":
            return not result and second.path.read_bytes() == second_raw
        return (result and second.path.read_bytes() == first_raw
                and len(calls) == 1)


def probe_window(module: types.ModuleType, mode: str) -> bool:
    calls: list[tuple[object, ...]] = []
    lease = (module.MaintenanceLease(
        Path("synthetic"), "a" * 32, None)
        if mode == "success" else module.MaintenanceLease(
            Path("synthetic"), None, None, "planned failure"))

    def begin(*args: object, **kwargs: object) -> object:
        calls.append(("begin", args, kwargs))
        return lease

    def end(value: object) -> bool:
        calls.append(("end", value))
        return True

    old_begin, old_end = module.begin_maintenance, module.end_maintenance
    module.begin_maintenance, module.end_maintenance = begin, end
    progress_calls: list[str] = []

    def progress(message: str) -> None:
        progress_calls.append(message)
        if mode == "progress_error":
            raise RuntimeError("diagnostic boom")

    try:
        if mode == "body_error":
            failed = raises_exact(
                lambda: _consume_window_error(
                    module, progress), RuntimeError, "body boom")
            return failed and calls[-1] == ("end", lease)
        with module.maintenance_window(
                "fleet", [2000], 60, now=NOW,
                identity_fn=identity, progress=progress) as yielded:
            calls.append(("body", yielded))
        begin_call = calls[0]
        return (begin_call[0] == "begin"
                and begin_call[1] == ("fleet", [2000], 60)
                and begin_call[2]["now"] == NOW
                and begin_call[2]["identity_fn"] is identity
                and calls[1] == ("body", lease)
                and calls[2] == ("end", lease)
                and ((not progress_calls) if mode == "success" else
                     (len(progress_calls) == 1
                      and "planned failure" in progress_calls[0])))
    finally:
        module.begin_maintenance, module.end_maintenance = old_begin, old_end


def _consume_window_error(module: types.ModuleType,
                          progress: Callable[[str], None]) -> None:
    with module.maintenance_window(
            "fleet", [2000], 60, now=NOW,
            identity_fn=identity, progress=progress):
        raise RuntimeError("body boom")


def fences() -> tuple[Fence, ...]:
    cases = [
        f("UN1", "_utc_now", "returns an aware UTC wall clock",
          "return datetime.now(timezone.utc)", "return datetime.now()",
          probe_utc_now),

        f("AU1", "_aware_utc", "invokes an injected clock callable",
          "current = value() if callable(value) else value",
          "current = value", p(probe_aware, "callable")),
        f("AU2", "_aware_utc", "uses _utc_now for a None clock",
          "current = _utc_now() if current is None else current",
          "current = current", p(probe_aware, "none")),
        f("AU3", "_aware_utc", "rejects a non-datetime clock result",
          "if not isinstance(current, datetime):", "if False:",
          p(probe_aware, "type")),
        f("AU4", "_aware_utc", "attaches UTC to naive datetime values",
          "if current.tzinfo is None:\n        current = current.replace(tzinfo=timezone.utc)",
          "if False:\n        current = current.replace(tzinfo=timezone.utc)",
          p(probe_aware, "naive")),
        f("AU5", "_aware_utc", "converts aware values to UTC",
          "return current.astimezone(timezone.utc)\n\n\ndef _timestamp",
          "return current\n\n\ndef _timestamp", p(probe_aware, "aware")),

        f("TS1", "_timestamp", "normalizes the source datetime through _aware_utc",
          "_aware_utc(value).isoformat", "value.isoformat", probe_timestamp),
        f("TS2", "_timestamp", "uses millisecond precision",
          'isoformat(timespec="milliseconds")', 'isoformat(timespec="seconds")',
          probe_timestamp),
        f("TS3", "_timestamp", "uses the canonical Z suffix",
          '.replace("+00:00", "Z")', '.replace("+00:00", "+00:00")',
          probe_timestamp),

        f("PT1", "_parse_timestamp", "rejects non-string timestamps",
          "if not isinstance(value, str) or len(value) > 40:",
          "if False or len(value) > 40:", p(probe_parse, "type")),
        f("PT2", "_parse_timestamp", "enforces the timestamp length cap",
          "if not isinstance(value, str) or len(value) > 40:",
          "if not isinstance(value, str) or False:", p(probe_parse, "length")),
        f("PT3", "_parse_timestamp", "normalizes parser failures",
          "except ValueError as exc:\n        raise MaintenanceError(\"started_at is invalid\") from exc",
          "except () as exc:\n        raise MaintenanceError(\"started_at is invalid\") from exc",
          p(probe_parse, "invalid")),
        f("PT4", "_parse_timestamp", "rejects naive timestamp text",
          "if parsed.tzinfo is None:\n        raise MaintenanceError(\"started_at requires a timezone\")",
          "if False:\n        raise MaintenanceError(\"started_at requires a timezone\")",
          p(probe_parse, "naive")),
        f("PT5", "_parse_timestamp", "converts parsed offsets to UTC",
          "return parsed.astimezone(timezone.utc)", "return parsed",
          p(probe_parse, "valid")),

        f("MP1", "maintenance_path", "coerces string paths to Path",
          "path = Path(fleet_or_base)", "path = fleet_or_base",
          p(probe_maintenance_path, "base")),
        f("MP2", "maintenance_path", "recognizes fleet.json case-insensitively",
          'if path.name.lower() == "fleet.json":',
          'if path.name == "fleet.json":', p(probe_maintenance_path, "file")),
        f("MP3", "maintenance_path", "joins the sidecar beneath a base directory",
          "return path / MAINTENANCE_NAME", "return path.parent / MAINTENANCE_NAME",
          p(probe_maintenance_path, "base")),

        f("BU1", "maintenance_budget", "coerces the port count to int",
          "count = max(1, int(port_count or 0))",
          "count = max(1, port_count or 0)", p(probe_budget, "normal")),
        f("BU2", "maintenance_budget", "floors the effective port count at one",
          "count = max(1, int(port_count or 0))",
          "count = max(0, int(port_count or 0))", p(probe_budget, "zero")),
        f("BU3", "maintenance_budget", "includes the floating overhead",
          "float(overhead_s) + count * float(per_port_s)",
          "0.0 + count * float(per_port_s)", p(probe_budget, "normal")),
        f("BU4", "maintenance_budget", "multiplies per-port time by count",
          "float(overhead_s) + count * float(per_port_s)",
          "float(overhead_s) + float(per_port_s)", p(probe_budget, "normal")),
        f("BU5", "maintenance_budget", "floors the computed budget at one second",
          "max(1.0, float(overhead_s) + count * float(per_port_s))",
          "float(overhead_s) + count * float(per_port_s)",
          p(probe_budget, "floor")),
        f("BU6", "maintenance_budget", "caps the computed maintenance budget",
          "return min(MAINTENANCE_MAX_BUDGET_S,\n               max(1.0, float(overhead_s) + count * float(per_port_s)))",
          "return max(1.0, float(overhead_s) + count * float(per_port_s))",
          p(probe_budget, "cap")),

        f("IH1", "_process_identity_from_handle", "fails when GetProcessTimes fails",
          "if not kernel32.GetProcessTimes(\n            handle, ctypes.byref(creation), ctypes.byref(exit_time),\n            ctypes.byref(kernel), ctypes.byref(user)):",
          "if False:", p(probe_identity_handle, "failure")),
        f("IH2", "_process_identity_from_handle", "composes FILETIME high and low words",
          "filetime = (int(creation.dwHighDateTime) << 32) | int(creation.dwLowDateTime)",
          "filetime = int(creation.dwLowDateTime)",
          p(probe_identity_handle, "success")),

        f("GI1", "get_process_identity", "coerces PID to int",
          "pid = int(pid)\n    if pid <= 0 or os.name != \"nt\":",
          "pid = pid\n    if pid <= 0 or os.name != \"nt\":",
          p(probe_get_identity, "self")),
        f("GI2", "get_process_identity", "rejects nonpositive PIDs before OpenProcess",
          'if pid <= 0 or os.name != "nt":',
          'if False or os.name != "nt":', p(probe_get_identity, "bad_pid")),
        f("GI3", "get_process_identity", "fails closed off Windows",
          'if pid <= 0 or os.name != "nt":',
          'if pid <= 0 or False:', p(probe_get_identity, "nonwindows")),
        f("GI4", "get_process_identity", "opens query-limited process access",
          "handle = kernel32.OpenProcess(0x1000, False, pid)",
          "handle = kernel32.OpenProcess(0, False, pid)",
          p(probe_get_identity, "fake")),
        f("GI5", "get_process_identity", "rejects a failed OpenProcess handle",
          "if not handle:\n        raise OSError(ctypes.get_last_error(), \"OpenProcess failed\")",
          "if False:\n        raise OSError(ctypes.get_last_error(), \"OpenProcess failed\")",
          p(probe_get_identity, "open_failure")),
        f("GI6", "get_process_identity", "closes every opened handle",
          "finally:\n        kernel32.CloseHandle(handle)",
          "finally:\n        None", p(probe_get_identity, "fake")),
        f("GI7", "get_process_identity", "attaches the canonical PID to identity",
          'identity["pid"] = pid\n    return identity',
          'identity["pid"] = 0\n    return identity', p(probe_get_identity, "fake")),

        f("NP1", "_normalize_ports", "None port input normalizes to empty then rejects",
          "for value in values or ():", "for value in values:",
          p(probe_ports, "empty")),
        f("NP2", "_normalize_ports", "rejects bool before integer coercion",
          "if isinstance(value, bool):", "if False:", p(probe_ports, "bool")),
        f("NP3", "_normalize_ports", "coerces numeric port text to int",
          "port = int(value)", "port = value", p(probe_ports, "normal")),
        f("NP4", "_normalize_ports", "normalizes conversion failures",
          "except (TypeError, ValueError) as exc:\n            raise MaintenanceError(\"maintenance port is invalid\") from exc",
          "except () as exc:\n            raise MaintenanceError(\"maintenance port is invalid\") from exc",
          p(probe_ports, "invalid")),
        f("NP5", "_normalize_ports", "rejects port zero",
          "if not 1 <= port <= 65535:", "if not 0 <= port <= 65535:",
          p(probe_ports, "bounds")),
        f("NP6", "_normalize_ports", "rejects ports above 65535",
          "if not 1 <= port <= 65535:", "if not 1 <= port <= 65536:",
          p(probe_ports, "bounds")),
        f("NP7", "_normalize_ports", "deduplicates in first-seen order",
          "if port not in ports:\n            ports.append(port)",
          "if True:\n            ports.append(port)", p(probe_ports, "normal")),
        f("NP8", "_normalize_ports", "rejects an empty normalized list",
          "if not ports or len(ports) > MAINTENANCE_MAX_PORTS:",
          "if False or len(ports) > MAINTENANCE_MAX_PORTS:",
          p(probe_ports, "empty")),
        f("NP9", "_normalize_ports", "enforces the port-count cap",
          "if not ports or len(ports) > MAINTENANCE_MAX_PORTS:",
          "if not ports or False:", p(probe_ports, "cap")),
        f("NP10", "_normalize_ports", "returns an immutable tuple",
          "return tuple(ports)\n\n\ndef validate_maintenance",
          "return ports\n\n\ndef validate_maintenance", p(probe_ports, "normal")),

        f("VM1", "validate_maintenance", "requires a real dict payload",
          "if not isinstance(payload, dict) or set(payload) != {",
          "if False or set(payload) != {", p(probe_validate, "type")),
        f("VM2", "validate_maintenance", "requires the exact maintenance key set",
          "or set(payload) != {\n            \"schema\", \"lease_id\", \"owner_pid\", \"owner_creation_filetime\",",
          "or set(payload) == {\n            \"schema\", \"lease_id\", \"owner_pid\", \"owner_creation_filetime\",",
          p(probe_validate, "keys")),
        f("VM3", "validate_maintenance", "rejects unsupported schema values",
          'if payload.get("schema") != MAINTENANCE_SCHEMA:',
          'if False:', p(probe_validate, "schema")),
        f("VM4", "validate_maintenance", "requires a string lease identifier",
          "if not isinstance(lease_id, str) or _LEASE_RE.fullmatch(lease_id) is None:",
          "if False or _LEASE_RE.fullmatch(lease_id) is None:",
          p(probe_validate, "lease_type")),
        f("VM5", "validate_maintenance", "requires canonical lease-id syntax",
          "if not isinstance(lease_id, str) or _LEASE_RE.fullmatch(lease_id) is None:",
          "if not isinstance(lease_id, str) or False:",
          p(probe_validate, "lease_format")),
        f("VM6", "validate_maintenance", "rejects bool owner PIDs",
          "if (isinstance(owner_pid, bool) or not isinstance(owner_pid, int)",
          "if (False or not isinstance(owner_pid, int)",
          p(probe_validate, "pid_bool")),
        f("VM7", "validate_maintenance", "requires an integer owner PID",
          "or not isinstance(owner_pid, int)\n            or owner_pid <= 0):",
          "or False\n            or owner_pid <= 0):",
          p(probe_validate, "pid_type")),
        f("VM8", "validate_maintenance", "requires a positive owner PID",
          "or owner_pid <= 0):\n        raise MaintenanceError(\"maintenance owner_pid is invalid\")",
          "or False):\n        raise MaintenanceError(\"maintenance owner_pid is invalid\")",
          p(probe_validate, "pid_bound")),
        f("VM9", "validate_maintenance", "rejects bool creation identities",
          "if (isinstance(creation, bool) or not isinstance(creation, int)",
          "if (False or not isinstance(creation, int)",
          p(probe_validate, "creation_bool")),
        f("VM10", "validate_maintenance", "requires an integer creation identity",
          "or not isinstance(creation, int)\n            or creation < 0):",
          "or False\n            or creation < 0):",
          p(probe_validate, "creation_type")),
        f("VM11", "validate_maintenance", "rejects negative creation identities",
          "or creation < 0):\n        raise MaintenanceError(\"maintenance owner identity is invalid\")",
          "or False):\n        raise MaintenanceError(\"maintenance owner identity is invalid\")",
          p(probe_validate, "creation_bound")),
        f("VM12", "validate_maintenance", "rejects bool maintenance budgets",
          "if (isinstance(budget, bool) or not isinstance(budget, (int, float))",
          "if (False or not isinstance(budget, (int, float))",
          p(probe_validate, "budget_bool")),
        f("VM13", "validate_maintenance", "requires a numeric maintenance budget",
          "or not isinstance(budget, (int, float))\n            or not 1 <= float(budget) <= MAINTENANCE_MAX_BUDGET_S):",
          "or False\n            or not 1 <= float(budget) <= MAINTENANCE_MAX_BUDGET_S):",
          p(probe_validate, "budget_type")),
        f("VM14", "validate_maintenance", "enforces the budget lower bound",
          "or not 1 <= float(budget) <= MAINTENANCE_MAX_BUDGET_S):",
          "or not 0 <= float(budget) <= MAINTENANCE_MAX_BUDGET_S):",
          p(probe_validate, "budget_low")),
        f("VM15", "validate_maintenance", "enforces the budget upper bound",
          "or not 1 <= float(budget) <= MAINTENANCE_MAX_BUDGET_S):",
          "or not 1 <= float(budget) <= MAINTENANCE_MAX_BUDGET_S + 1):",
          p(probe_validate, "budget_high")),
        f("VM16", "validate_maintenance", "parses and validates started_at",
          'started = _parse_timestamp(payload.get("started_at"))',
          "started = _parse_timestamp(STAMP)", p(probe_validate, "timestamp")),
        f("VM17", "validate_maintenance", "normalizes and validates ports",
          'ports = _normalize_ports(payload.get("ports"))',
          "ports = _normalize_ports([1])", p(probe_validate, "ports")),
        f("VM18", "validate_maintenance", "canonicalizes started_at text",
          '"started_at": _timestamp(started),\n        "budget_seconds": float(budget),',
          '"started_at": payload.get("started_at"),\n        "budget_seconds": float(budget),',
          p(probe_validate, "valid")),
        f("VM19", "validate_maintenance", "canonicalizes budget to float",
          '"budget_seconds": float(budget),',
          '"budget_seconds": budget,', p(probe_validate, "valid")),
        f("VM20", "validate_maintenance", "returns ports as a JSON list",
          '"ports": list(ports),\n    }\n\n\ndef _read_bytes',
          '"ports": ports,\n    }\n\n\ndef _read_bytes', p(probe_validate, "valid")),

        f("RB1", "_read_bytes", "uses the bounded stat size",
          "size = path.stat().st_size", "size = len(path.read_bytes())",
          p(probe_read_bytes, "stat")),
        f("RB2", "_read_bytes", "normalizes stat failures",
          'except OSError as exc:\n        raise MaintenanceError("maintenance flag is unavailable") from exc',
          'except () as exc:\n        raise MaintenanceError("maintenance flag is unavailable") from exc',
          p(probe_read_bytes, "stat")),
        f("RB3", "_read_bytes", "rejects an empty maintenance flag",
          "if size <= 0 or size > MAINTENANCE_MAX_BYTES:",
          "if size < 0 or size > MAINTENANCE_MAX_BYTES:",
          p(probe_read_bytes, "low")),
        f("RB4", "_read_bytes", "accepts exactly the byte limit and rejects above it",
          "if size <= 0 or size > MAINTENANCE_MAX_BYTES:",
          "if size <= 0 or size >= MAINTENANCE_MAX_BYTES:",
          p(probe_read_bytes, "high")),
        f("RB5", "_read_bytes", "returns the bytes read from the sidecar",
          "return path.read_bytes()\n    except OSError as exc:",
          "return b\"\"\n    except OSError as exc:", p(probe_read_bytes, "read")),
        f("RB6", "_read_bytes", "normalizes read failures",
          'except OSError as exc:\n        raise MaintenanceError("maintenance flag is unreadable") from exc',
          'except () as exc:\n        raise MaintenanceError("maintenance flag is unreadable") from exc',
          p(probe_read_bytes, "read_error")),

        f("LM1", "load_maintenance", "coerces the supplied path to Path",
          "raw = _read_bytes(Path(path))", "raw = _read_bytes(path)",
          p(probe_load, "valid")),
        f("LM2", "load_maintenance", "decodes sidecar bytes as UTF-8",
          'payload = json.loads(raw.decode("utf-8"))',
          'payload = json.loads(raw.decode("utf-16"))', p(probe_load, "valid")),
        f("LM3", "load_maintenance", "parses the decoded JSON document",
          'payload = json.loads(raw.decode("utf-8"))',
          "payload = {}", p(probe_load, "valid")),
        f("LM4", "load_maintenance", "normalizes decode and JSON failures",
          'except (UnicodeDecodeError, ValueError) as exc:\n        raise MaintenanceError("maintenance flag is not valid JSON") from exc',
          'except () as exc:\n        raise MaintenanceError("maintenance flag is not valid JSON") from exc',
          p(probe_load, "unicode")),
        f("LM5", "load_maintenance", "validates the parsed maintenance object",
          "return validate_maintenance(payload)\n\n\ndef maintenance_status",
          "return payload\n\n\ndef maintenance_status", p(probe_load, "schema")),

        f("MS1", "maintenance_status", "coerces the supplied status path",
          "path = Path(path)\n    if not path.is_file():",
          "path = path\n    if not path.is_file():", p(probe_status, "missing")),
        f("MS2", "maintenance_status", "reports a missing non-file sidecar",
          'if not path.is_file():\n        return MaintenanceStatus(False, "missing")',
          'if False:\n        return MaintenanceStatus(False, "missing")',
          p(probe_status, "missing")),
        f("MS3", "maintenance_status", "loads the validated sidecar payload",
          "payload = load_maintenance(path)", "payload = {}",
          p(probe_status, "invalid")),
        f("MS4", "maintenance_status", "converts malformed flags to status",
          "except MaintenanceError as exc:\n        return MaintenanceStatus(False, str(exc))",
          "except () as exc:\n        return MaintenanceStatus(False, str(exc))",
          p(probe_status, "invalid")),
        f("MS5", "maintenance_status", "invokes the injected identity provider",
          'actual = identity_fn(payload["owner_pid"])',
          'actual = {"pid": payload["owner_pid"], "creation_filetime": payload["owner_creation_filetime"]}',
          p(probe_status, "valid")),
        f("MS6", "maintenance_status", "defaults an omitted actual PID to the owner PID",
          'actual.get("pid", payload["owner_pid"])',
          'actual.get("pid", 0)', p(probe_status, "pid_default")),
        f("MS7", "maintenance_status", "requires exact PID equality",
          'alive = (int(actual.get("pid", payload["owner_pid"]))\n                 == payload["owner_pid"]\n                 and int(actual["creation_filetime"])',
          'alive = (True\n                 and int(actual["creation_filetime"])',
          p(probe_status, "pid")),
        f("MS8", "maintenance_status", "coerces actual creation FILETIME to int",
          'and int(actual["creation_filetime"])\n                 == payload["owner_creation_filetime"])',
          'and actual["creation_filetime"]\n                 == payload["owner_creation_filetime"])',
          p(probe_status, "creation_coerce")),
        f("MS9", "maintenance_status", "requires exact creation-FILETIME equality",
          'and int(actual["creation_filetime"])\n                 == payload["owner_creation_filetime"])',
          'and True)', p(probe_status, "creation")),
        f("MS10", "maintenance_status", "fails closed on every identity exception",
          "except Exception:  # noqa: BLE001 - identity failure is a dead owner",
          "except OSError:  # noqa: BLE001 - identity failure is a dead owner",
          p(probe_status, "identity_error")),
        f("MS11", "maintenance_status", "dead-owner status carries ports and payload",
          'return MaintenanceStatus(False, "dead_owner", tuple(payload["ports"]),\n                                 payload=payload)',
          'return MaintenanceStatus(False, "dead_owner", (),\n                                 payload=None)',
          p(probe_status, "dead_payload")),
        f("MS12", "maintenance_status", "parses the payload start time",
          'started = _parse_timestamp(payload["started_at"])',
          "started = NOW", p(probe_status, "started")),
        f("MS13", "maintenance_status", "adds the validated lease budget",
          'seconds=float(payload["budget_seconds"]) + max(0.0, float(margin_s)))',
          'seconds=0.0 + max(0.0, float(margin_s)))',
          p(probe_status, "boundary")),
        f("MS14", "maintenance_status", "clamps negative expiry margin to zero",
          "max(0.0, float(margin_s))", "float(margin_s)",
          p(probe_status, "boundary")),
        f("MS15", "maintenance_status", "normalizes the injected current clock",
          "current = _aware_utc(now)", "current = _aware_utc(NOW)",
          p(probe_status, "valid")),
        f("MS16", "maintenance_status", "keeps the exact expiry instant valid",
          "if current > expires:", "if current >= expires:",
          p(probe_status, "boundary")),

        f("AW1", "_atomic_write", "creates the target parent tree",
          "path.parent.mkdir(parents=True, exist_ok=True)", "None",
          p(probe_atomic, "normal")),
        f("AW2", "_atomic_write", "uses an eight-byte random temp token",
          "token = secrets.token_hex(8)", "token = secrets.token_hex(7)",
          p(probe_atomic, "normal")),
        f("AW3", "_atomic_write", "includes the current PID in the temp name",
          'f".{path.name}.{os.getpid()}.{token}.tmp"',
          'f".{path.name}.0.{token}.tmp"', p(probe_atomic, "normal")),
        f("AW4", "_atomic_write", "writes the exact requested payload",
          "temp.write_bytes(payload)", "temp.write_bytes(b\"wrong\")",
          p(probe_atomic, "normal")),
        f("AW5", "_atomic_write", "publishes through the injected replace seam",
          "replace_fn(temp, path)", "os.replace(temp, path)",
          p(probe_atomic, "normal")),
        f("AW6", "_atomic_write", "removes a failed publication temp file",
          "temp.unlink()\n        except FileNotFoundError:",
          "None\n        except FileNotFoundError:", p(probe_atomic, "failure")),
        f("AW7", "_atomic_write", "tolerates temp already moved by replace",
          "except FileNotFoundError:\n            pass\n\n\ndef begin_maintenance",
          "except ():\n            pass\n\n\ndef begin_maintenance", p(probe_atomic, "normal")),

        f("ML1", "MaintenanceLease.published",
          "requires a lease identifier and no publication error",
          "return self.lease_id is not None and self.error is None",
          "return self.lease_id is not None or self.error is None",
          probe_lease_property),

        f("BM1", "begin_maintenance", "derives the canonical sidecar path",
          "path = maintenance_path(fleet_or_base)", "path = Path(fleet_or_base)",
          p(probe_begin, "normal")),
        f("BM2", "begin_maintenance", "creates a 16-byte lease identifier",
          "lease_id = secrets.token_hex(16)", "lease_id = secrets.token_hex(15)",
          p(probe_begin, "normal")),
        f("BM3", "begin_maintenance", "normalizes ports before owner lookup",
          "normalized = _normalize_ports(ports)\n        budget = float(budget_seconds)",
          "normalized = _normalize_ports([1])\n        budget = float(budget_seconds)",
          p(probe_begin, "ports_error")),
        f("BM4", "begin_maintenance", "coerces the requested budget to float",
          "budget = float(budget_seconds)", "budget = budget_seconds",
          p(probe_begin, "normal")),
        f("BM5", "begin_maintenance", "rejects a budget below one before owner lookup",
          "if not 1 <= budget <= MAINTENANCE_MAX_BUDGET_S:",
          "if not 0 <= budget <= MAINTENANCE_MAX_BUDGET_S:",
          p(probe_begin, "budget_low")),
        f("BM6", "begin_maintenance", "rejects an excessive budget before owner lookup",
          "if not 1 <= budget <= MAINTENANCE_MAX_BUDGET_S:",
          "if not 1 <= budget <= MAINTENANCE_MAX_BUDGET_S + 1:",
          p(probe_begin, "budget_high")),
        f("BM7", "begin_maintenance", "reads identity for this process only",
          "identity = identity_fn(os.getpid())", "identity = identity_fn(0)",
          p(probe_begin, "normal")),
        f("BM8", "begin_maintenance", "coerces creation FILETIME to int",
          'creation = int(identity["creation_filetime"])',
          'creation = identity["creation_filetime"]', p(probe_begin, "normal")),
        f("BM9", "begin_maintenance", "normalizes the injected start clock",
          "started = _aware_utc(now)", "started = _aware_utc(NOW)",
          p(probe_begin, "normal")),
        f("BM10", "begin_maintenance", "inspects an existing lease only when it is a file",
          "if path.is_file():", "if False:", p(probe_begin, "nested")),
        f("BM11", "begin_maintenance", "requires same PID before nesting a lease",
          'if (old["owner_pid"] == os.getpid()\n                            and old["owner_creation_filetime"] == creation):',
          'if (True\n                            and old["owner_creation_filetime"] == creation):',
          p(probe_begin, "different_pid")),
        f("BM12", "begin_maintenance", "requires same creation identity before nesting",
          'and old["owner_creation_filetime"] == creation):',
          "and True):", p(probe_begin, "different_creation")),
        f("BM13", "begin_maintenance", "preserves exact previous bytes for restoration",
          "previous = old_raw\n                        normalized = tuple(sorted(",
          "previous = None\n                        normalized = tuple(sorted(",
          p(probe_begin, "nested")),
        f("BM14", "begin_maintenance", "unions new and prior nested ports",
          'set(normalized) | set(old["ports"])',
          "set(normalized)", p(probe_begin, "nested")),
        f("BM15", "begin_maintenance", "sorts the nested port union",
          'normalized = tuple(sorted(\n                            set(normalized) | set(old["ports"])))',
          'normalized = tuple(\n                            set(normalized) | set(old["ports"]))',
          p(probe_begin, "nested_order")),
        f("BM16", "begin_maintenance", "parses the prior lease start",
          'old_start = _parse_timestamp(old["started_at"])',
          "old_start = started", p(probe_begin, "nested")),
        f("BM17", "begin_maintenance", "retains the earliest nested start",
          "started = min(started, old_start)",
          "started = max(started, old_start)", p(probe_begin, "nested")),
        f("BM18", "begin_maintenance", "retains the latest nested expiry",
          "max(old_expiry, new_expiry)", "min(old_expiry, new_expiry)",
          p(probe_begin, "nested")),
        f("BM19", "begin_maintenance", "overwrites an invalid existing sidecar safely",
          "except (MaintenanceError, UnicodeDecodeError, ValueError):\n                    previous = None",
          "except ():\n                    previous = None", p(probe_begin, "invalid")),
        f("BM20", "begin_maintenance", "publishes this process as the owner PID",
          '"owner_pid": os.getpid(),', '"owner_pid": 0,',
          p(probe_begin, "normal")),
        f("BM21", "begin_maintenance", "sorts keys in the canonical JSON encoding",
          "json.dumps(payload, sort_keys=True, separators=(\",\", \":\"))",
          "json.dumps(payload, sort_keys=False, separators=(\",\", \":\"))",
          p(probe_begin, "normal")),
        f("BM22", "begin_maintenance", "uses compact JSON separators",
          'separators=(",", ":")', "separators=None",
          p(probe_begin, "normal")),
        f("BM23", "begin_maintenance", "terminates canonical JSON with one newline",
          '+ "\\n").encode("utf-8")', '+ "").encode("utf-8")',
          p(probe_begin, "normal")),
        f("BM24", "begin_maintenance", "rejects oversized bytes before publication",
          "if len(encoded) > MAINTENANCE_MAX_BYTES:", "if False:",
          p(probe_begin, "oversized")),
        f("BM25", "begin_maintenance", "publishes through the injected replace seam",
          "_atomic_write(path, encoded, replace_fn=replace_fn)",
          "_atomic_write(path, encoded)", p(probe_begin, "replace_error")),
        f("BM26", "begin_maintenance", "never raises from restart publication failure",
          "except Exception as exc:  # noqa: BLE001 - restart remains independently safe",
          "except MaintenanceError as exc:  # noqa: BLE001 - restart remains independently safe",
          p(probe_begin, "identity_error")),

        f("EM1", "end_maintenance", "rejects non-lease objects",
          "if not isinstance(lease, MaintenanceLease) or not lease.published:",
          "if False or not lease.published:", p(probe_end, "type")),
        f("EM2", "end_maintenance", "rejects unpublished leases without mutation",
          "if not isinstance(lease, MaintenanceLease) or not lease.published:",
          "if not isinstance(lease, MaintenanceLease) or False:",
          p(probe_end, "unpublished")),
        f("EM3", "end_maintenance", "returns false when the current lease cannot load",
          "except MaintenanceError:\n            return False",
          "except ():\n            return False", p(probe_end, "missing")),
        f("EM4", "end_maintenance", "never clobbers a newer lease identifier",
          'if current["lease_id"] != lease.lease_id:',
          "if False:", p(probe_end, "mismatch")),
        f("EM5", "end_maintenance", "restores only when previous bytes exist",
          "if lease.previous is not None:", "if False:",
          p(probe_end, "restore")),
        f("EM6", "end_maintenance", "restores the exact prior bytes",
          "_atomic_write(lease.path, lease.previous, replace_fn=replace_fn)",
          "_atomic_write(lease.path, b\"wrong\", replace_fn=replace_fn)",
          p(probe_end, "restore")),
        f("EM7", "end_maintenance", "restores through the injected replace seam",
          "_atomic_write(lease.path, lease.previous, replace_fn=replace_fn)",
          "_atomic_write(lease.path, lease.previous)", p(probe_end, "restore")),
        f("EM8", "end_maintenance", "unlinks a top-level exact lease",
          "lease.path.unlink()\n        except OSError:",
          "None\n        except OSError:", p(probe_end, "unlink")),
        f("EM9", "end_maintenance", "normalizes restoration and unlink failures",
          "except OSError:\n            return False\n    return True",
          "except ():\n            return False\n    return True",
          p(probe_end, "unlink_error")),

        f("MW1", "maintenance_window", "forwards the three maintenance arguments",
          "fleet_or_base, ports, budget_seconds, now=now,",
          "fleet_or_base, (), budget_seconds, now=now,",
          p(probe_window, "success")),
        f("MW2", "maintenance_window", "forwards the injected start clock",
          "budget_seconds, now=now,\n        identity_fn=identity_fn)",
          "budget_seconds, now=None,\n        identity_fn=identity_fn)",
          p(probe_window, "success")),
        f("MW3", "maintenance_window", "forwards the identity provider",
          "identity_fn=identity_fn)\n    if lease.error",
          "identity_fn=get_process_identity)\n    if lease.error",
          p(probe_window, "success")),
        f("MW4", "maintenance_window", "reports only an actual publication error",
          "if lease.error and progress is not None:",
          "if progress is not None:", p(probe_window, "success")),
        f("MW5", "maintenance_window", "includes the publication error in progress text",
          'progress(f"fleet maintenance flag unavailable ({lease.error})")',
          'progress("fleet maintenance flag unavailable")',
          p(probe_window, "error")),
        f("MW6", "maintenance_window", "swallows every diagnostic callback failure",
          "except Exception:  # noqa: BLE001 - diagnostics cannot block restart",
          "except ():\n            # noqa: BLE001 - diagnostics cannot block restart",
          p(probe_window, "progress_error")),
        f("MW7", "maintenance_window", "yields the exact published lease",
          "yield lease\n    finally:", "yield None\n    finally:",
          p(probe_window, "success")),
        f("MW8", "maintenance_window", "ends the lease in finally even on body failure",
          "finally:\n        end_maintenance(lease)",
          "finally:\n        None", p(probe_window, "body_error")),
    ]
    return tuple(cases)


def redundancy_cases() -> tuple[Redundancy, ...]:
    return (
        Redundancy(
            "RR1",
            "_parse_timestamp Z replacement is subsumed by Python's aware-Z parser",
            mu(
                'datetime.fromisoformat(value.replace("Z", "+00:00"))',
                "datetime.fromisoformat(value)",
            ),
            p(probe_parse, "z"),
        ),
        Redundancy(
            "RR2",
            "_LEASE_RE trailing anchor is subsumed by fullmatch",
            mu(
                're.compile(r"[0-9a-f]{32}\\Z")',
                're.compile(r"[0-9a-f]{32}")',
            ),
            probe_lease_anchor_redundancy,
        ),
        Redundancy(
            "RR3",
            "nested budget's one-second floor is subsumed by two validated budgets",
            mu(
                "budget = max(1.0, (max(old_expiry, new_expiry)\n"
                "                                           - started).total_seconds())",
                "budget = (max(old_expiry, new_expiry)\n"
                "                                  - started).total_seconds()",
            ),
            p(probe_begin, "nested"),
        ),
        Redundancy(
            "RR4",
            "invalid-existing reset is subsumed by previous=None initialization",
            mu(
                "except (MaintenanceError, UnicodeDecodeError, ValueError):\n"
                "                    previous = None",
                "except (MaintenanceError, UnicodeDecodeError, ValueError):\n"
                "                    pass",
            ),
            p(probe_begin, "invalid"),
        ),
    )


EXPECTED_FENCE_COUNTS = {
    "_utc_now": 1,
    "_aware_utc": 5,
    "_timestamp": 3,
    "_parse_timestamp": 5,
    "maintenance_path": 3,
    "maintenance_budget": 6,
    "_process_identity_from_handle": 2,
    "get_process_identity": 7,
    "_normalize_ports": 10,
    "validate_maintenance": 20,
    "_read_bytes": 6,
    "load_maintenance": 5,
    "maintenance_status": 16,
    "_atomic_write": 7,
    "MaintenanceLease.published": 1,
    "begin_maintenance": 26,
    "end_maintenance": 9,
    "maintenance_window": 8,
}


def run() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    kit = CheckKit()
    kit.section("Row 85 S4a shipped-source custody and lease inventory")
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
        module_owners = tuple(sorted(
            owner for owner in owners if owner.startswith("<module>:")
        ))
        kit.check(
            "SOURCE-CUSTODY pinned per-fenced-segment EOL-normalized "
            "addstock_watchdog.py source segments",
            segment_hashes == EXPECTED_FUNCTION_SEGMENT_SHA256,
            repr(segment_hashes),
        )
        kit.check(
            "SOURCE-CUSTODY module-level anchor inventory is exactly one",
            module_owners == ("<module>:_LEASE_RE",),
            repr(module_owners),
        )
        locality_source = (
            "def _source_custody_unrelated_probe():\n    return None\n\n"
            + source
        )
        kit.check(
            "SOURCE-CUSTODY top insertion leaves fenced digests unchanged",
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
        module_mutation = next(
            item.mutation for item in (*cases, *redundancies)
            if mutation_owner(
                source, item.mutation.old, item.mutation.new
            ).startswith("<module>:")
        )
        module_owner = mutation_owner(
            source, module_mutation.old, module_mutation.new)
        kit.check(
            "SOURCE-CUSTODY module-level mutation changes its owned digest",
            function_segment_sha256(
                module_mutation.apply(source), (module_owner,)
            ) != {module_owner: segment_hashes[module_owner]},
            module_owner,
        )
        formfeed_source = (
            "def a():\n    return 1\n"
            "\fdef b():\n    return 2\n"
        )
        formfeed_expected = hashlib.sha256(
            "\fdef b():\n    return 2\n".encode("utf-8")
        ).hexdigest()
        kit.check(
            "SOURCE-CUSTODY AST lines split only on LF, not form-feed",
            function_segment_sha256(formfeed_source, ("b",))["b"]
            == formfeed_expected,
        )
        baseline = load_module(source)
    except BaseException as exc:
        kit.check("HARNESS-INVENTORY source loads and cases build", False,
                  f"{type(exc).__name__}: {exc}")
        return kit.finish()

    ids = tuple(case.fence_id for case in cases)
    redundant_ids = tuple(case.clause_id for case in redundancies)
    actual_counts = Counter(case.function for case in cases)
    kit.check("HARNESS-INVENTORY final code-derived fence count is 140",
              len(cases) == 140, str(len(cases)))
    kit.check("HARNESS-INVENTORY per-function counts match derivation",
              dict(actual_counts) == EXPECTED_FENCE_COUNTS,
              repr(dict(actual_counts)))
    kit.check("HARNESS-INVENTORY all 17 lease functions plus property are covered",
              set(actual_counts) == set(EXPECTED_FENCE_COUNTS),
              repr(sorted(actual_counts)))
    kit.check("HARNESS-INVENTORY fence IDs are unique",
              len(set(ids)) == len(ids), repr(ids))
    kit.check("HARNESS-INVENTORY four redundant clauses are explicit",
              len(redundancies) == 4, str(len(redundancies)))
    kit.check("HARNESS-INVENTORY redundancy IDs are unique",
              len(set(redundant_ids)) == len(redundant_ids),
              repr(redundant_ids))
    lease_slice = source[
        source.index("def _utc_now"):source.index("def hard_signal_kind")]
    atomic_slice = source[
        source.index("def _atomic_write"):source.index("def begin_maintenance")]
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
    kit.check("BOUNDARY _MAINT_LOCK concurrency is explicitly integration-only",
              "with _MAINT_LOCK:" in lease_slice
              and all("concurr" not in case.description.lower()
                      for case in cases))
    kit.check("BOUNDARY S4a lease code does not reach the hard-signal layer",
              all(name not in lease_slice for name in (
                  "hard_signal_kind", "HardSignalAdapter", "HardDeathWatchdog",
                  "maintenance_provider")))
    kit.check("SHIPPED-DESIGN lease atomic write intentionally has no fsync",
              "fsync" not in atomic_slice
              and "temp.write_bytes(payload)" in atomic_slice
              and "replace_fn(temp, path)" in atomic_slice)
    kit.check("CONTAINMENT fixture ticker vocabulary excludes protected names",
              SYNTHETIC_TICKERS.isdisjoint(FORBIDDEN_TICKERS),
              repr(sorted(SYNTHETIC_TICKERS & FORBIDDEN_TICKERS)))
    kit.check("CONTAINMENT harness has no process-launch or network dependency",
              imported_roots.isdisjoint({
                  "asyncio", "ib_insync", "socket", "subprocess"}),
              repr(sorted(imported_roots)))

    kit.section("One named baseline check per maintenance-lease fence")
    for case in cases:
        passed, detail = evaluate(case.probe, baseline)
        kit.check(
            f"FENCE {case.fence_id} {case.function} {case.description}",
            passed, detail,
        )

    kit.section("One compiling in-memory mutation killed per lease fence")
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

    kit.section("Behavior-preserving maintenance-lease clause deletions")
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

    kit.section("Pinned shipped defect and filesystem containment")
    known, known_detail = evaluate(probe_schema_bool_known_defect, baseline)
    kit.check("KNOWN-DEFECT schema=True is accepted as schema 1",
              known, known_detail)
    temp_root = Path(tempfile.gettempdir()).resolve()
    kit.check("CONTAINMENT every sandbox base came from system temp",
              bool(_TEMP_BASES)
              and all(_inside(temp_root, base) for base in _TEMP_BASES),
              repr(_TEMP_BASES[:3]))
    kit.check("CONTAINMENT no observed path escaped its fresh temp base",
              not _OUTSIDE, repr(_OUTSIDE))
    kit.check("CONTAINMENT all fresh temp bases were deleted",
              not _RESIDUE and all(not base.exists() for base in _TEMP_BASES),
              repr(_RESIDUE))
    after = source_bytes()
    kit.check("SOURCE-CUSTODY production raw bytes unchanged during run",
              digest(after) == before_raw_hash, digest(after))
    after_segment_hashes = function_segment_sha256(
        normalized_source(after), owners)
    kit.check("SOURCE-CUSTODY fenced-segment digests unchanged during run",
              after_segment_hashes == segment_hashes,
              repr(after_segment_hashes))
    return kit.finish()


if __name__ == "__main__":
    raise SystemExit(run())
