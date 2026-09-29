"""Hard-port-death protection for long Add Stocks runs.

The watchdog owns no TWS lifecycle and no bank writes. It classifies explicit
connection-layer failures, exposes per-port interrupt events, accounts grace
time, and validates the restart orchestrators' short-lived maintenance lease.
All clocks and identity lookups are injectable for deterministic offline tests.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import ctypes
from ctypes import wintypes
import errno
import functools
import json
import os
from pathlib import Path
import re
import secrets
import threading
import time


PORT_GRACE_S = 150.0
FLEET_GRACE_S = 150.0
PROBE_BACKOFF_S = (5.0, 15.0, 30.0, 60.0)
MAINT_MARGIN_S = 120.0
MAINTENANCE_NAME = "fleet_maintenance.json"
MAINTENANCE_SCHEMA = 1
MAINTENANCE_MAX_BYTES = 4096
MAINTENANCE_MAX_PORTS = 64
MAINTENANCE_MAX_BUDGET_S = 24 * 60 * 60

HEALTHY = "HEALTHY"
SUSPECT = "SUSPECT"
DEAD = "DEAD"

_LEASE_RE = re.compile(r"[0-9a-f]{32}\Z")
_HARD_TOKENS = frozenset({
    "connection_refused", "connection_lost", "socket_reset",
})
_SOFT_CLASS_NAMES = frozenset({"RequestTimeout", "PacingViolation"})
_HARD_ERRNOS = frozenset({
    errno.ECONNREFUSED, errno.ECONNRESET, errno.ECONNABORTED,
    getattr(errno, "ENETRESET", 102),
})
_HARD_WINERRORS = frozenset({10053, 10054, 10061})
_MAINT_LOCK = threading.RLock()


class MaintenanceError(ValueError):
    """The maintenance sidecar is malformed or cannot be published."""


@dataclass(frozen=True)
class MaintenanceStatus:
    valid: bool
    reason: str
    ports: tuple[int, ...] = ()
    expires_at: datetime | None = None
    payload: dict | None = None


@dataclass(frozen=True)
class MaintenanceLease:
    path: Path
    lease_id: str | None
    previous: bytes | None
    error: str | None = None

    @property
    def published(self) -> bool:
        return self.lease_id is not None and self.error is None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _aware_utc(value) -> datetime:
    current = value() if callable(value) else value
    current = _utc_now() if current is None else current
    if not isinstance(current, datetime):
        raise TypeError("wall clock must return datetime")
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def _timestamp(value) -> str:
    return (_aware_utc(value).isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"))


def _parse_timestamp(value) -> datetime:
    if not isinstance(value, str) or len(value) > 40:
        raise MaintenanceError("started_at is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MaintenanceError("started_at is invalid") from exc
    if parsed.tzinfo is None:
        raise MaintenanceError("started_at requires a timezone")
    return parsed.astimezone(timezone.utc)


def maintenance_path(fleet_or_base) -> Path:
    """Return the maintenance sidecar beside fleet.json."""
    path = Path(fleet_or_base)
    if path.name.lower() == "fleet.json":
        return path.with_name(MAINTENANCE_NAME)
    return path / MAINTENANCE_NAME


def maintenance_budget(port_count, *, per_port_s=240.0,
                       overhead_s=180.0) -> float:
    count = max(1, int(port_count or 0))
    return min(MAINTENANCE_MAX_BUDGET_S,
               max(1.0, float(overhead_s) + count * float(per_port_s)))


def _process_identity_from_handle(handle):
    creation = wintypes.FILETIME()
    exit_time = wintypes.FILETIME()
    kernel = wintypes.FILETIME()
    user = wintypes.FILETIME()
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetProcessTimes.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME), ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME)]
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    if not kernel32.GetProcessTimes(
            handle, ctypes.byref(creation), ctypes.byref(exit_time),
            ctypes.byref(kernel), ctypes.byref(user)):
        raise OSError(ctypes.get_last_error(), "GetProcessTimes failed")
    filetime = (int(creation.dwHighDateTime) << 32) | int(creation.dwLowDateTime)
    return {"creation_filetime": filetime}


def get_process_identity(pid):
    """Read exact PID + creation FILETIME identity; fail closed off Windows."""
    pid = int(pid)
    if pid <= 0 or os.name != "nt":
        raise OSError("process identity is available only for a Windows PID")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL,
                                     wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(0x1000, False, pid)
    if not handle:
        raise OSError(ctypes.get_last_error(), "OpenProcess failed")
    try:
        identity = _process_identity_from_handle(handle)
    finally:
        kernel32.CloseHandle(handle)
    identity["pid"] = pid
    return identity


def _normalize_ports(values) -> tuple[int, ...]:
    ports = []
    for value in values or ():
        if isinstance(value, bool):
            raise MaintenanceError("maintenance port is invalid")
        try:
            port = int(value)
        except (TypeError, ValueError) as exc:
            raise MaintenanceError("maintenance port is invalid") from exc
        if not 1 <= port <= 65535:
            raise MaintenanceError("maintenance port is invalid")
        if port not in ports:
            ports.append(port)
    if not ports or len(ports) > MAINTENANCE_MAX_PORTS:
        raise MaintenanceError("maintenance ports are empty or oversized")
    return tuple(ports)


def validate_maintenance(payload) -> dict:
    if not isinstance(payload, dict) or set(payload) != {
            "schema", "lease_id", "owner_pid", "owner_creation_filetime",
            "started_at", "budget_seconds", "ports"}:
        raise MaintenanceError("maintenance object has an invalid shape")
    if payload.get("schema") != MAINTENANCE_SCHEMA:
        raise MaintenanceError("maintenance schema is unsupported")
    lease_id = payload.get("lease_id")
    if not isinstance(lease_id, str) or _LEASE_RE.fullmatch(lease_id) is None:
        raise MaintenanceError("maintenance lease_id is invalid")
    owner_pid = payload.get("owner_pid")
    creation = payload.get("owner_creation_filetime")
    budget = payload.get("budget_seconds")
    if (isinstance(owner_pid, bool) or not isinstance(owner_pid, int)
            or owner_pid <= 0):
        raise MaintenanceError("maintenance owner_pid is invalid")
    if (isinstance(creation, bool) or not isinstance(creation, int)
            or creation < 0):
        raise MaintenanceError("maintenance owner identity is invalid")
    if (isinstance(budget, bool) or not isinstance(budget, (int, float))
            or not 1 <= float(budget) <= MAINTENANCE_MAX_BUDGET_S):
        raise MaintenanceError("maintenance budget is invalid")
    started = _parse_timestamp(payload.get("started_at"))
    ports = _normalize_ports(payload.get("ports"))
    return {
        "schema": MAINTENANCE_SCHEMA,
        "lease_id": lease_id,
        "owner_pid": owner_pid,
        "owner_creation_filetime": creation,
        "started_at": _timestamp(started),
        "budget_seconds": float(budget),
        "ports": list(ports),
    }


def _read_bytes(path: Path) -> bytes:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise MaintenanceError("maintenance flag is unavailable") from exc
    if size <= 0 or size > MAINTENANCE_MAX_BYTES:
        raise MaintenanceError("maintenance flag is empty or oversized")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise MaintenanceError("maintenance flag is unreadable") from exc


def load_maintenance(path) -> dict:
    raw = _read_bytes(Path(path))
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise MaintenanceError("maintenance flag is not valid JSON") from exc
    return validate_maintenance(payload)


def maintenance_status(path, *, now=None, identity_fn=get_process_identity,
                       margin_s=MAINT_MARGIN_S) -> MaintenanceStatus:
    path = Path(path)
    if not path.is_file():
        return MaintenanceStatus(False, "missing")
    try:
        payload = load_maintenance(path)
    except MaintenanceError as exc:
        return MaintenanceStatus(False, str(exc))
    try:
        actual = identity_fn(payload["owner_pid"])
        alive = (int(actual.get("pid", payload["owner_pid"]))
                 == payload["owner_pid"]
                 and int(actual["creation_filetime"])
                 == payload["owner_creation_filetime"])
    except Exception:  # noqa: BLE001 - identity failure is a dead owner
        alive = False
    if not alive:
        return MaintenanceStatus(False, "dead_owner", tuple(payload["ports"]),
                                 payload=payload)
    started = _parse_timestamp(payload["started_at"])
    expires = started + timedelta(
        seconds=float(payload["budget_seconds"]) + max(0.0, float(margin_s)))
    current = _aware_utc(now)
    if current > expires:
        return MaintenanceStatus(False, "expired", tuple(payload["ports"]),
                                 expires, payload)
    return MaintenanceStatus(True, "valid", tuple(payload["ports"]),
                             expires, payload)


def _atomic_write(path: Path, payload: bytes, replace_fn=os.replace) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    token = secrets.token_hex(8)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{token}.tmp")
    try:
        temp.write_bytes(payload)
        replace_fn(temp, path)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def begin_maintenance(fleet_or_base, ports, budget_seconds, *, now=None,
                      identity_fn=get_process_identity,
                      replace_fn=os.replace) -> MaintenanceLease:
    """Publish one owner-bound lease. Nested same-process leases restore safely."""
    path = maintenance_path(fleet_or_base)
    previous = None
    lease_id = secrets.token_hex(16)
    try:
        normalized = _normalize_ports(ports)
        budget = float(budget_seconds)
        if not 1 <= budget <= MAINTENANCE_MAX_BUDGET_S:
            raise MaintenanceError("maintenance budget is invalid")
        identity = identity_fn(os.getpid())
        creation = int(identity["creation_filetime"])
        started = _aware_utc(now)
        with _MAINT_LOCK:
            if path.is_file():
                try:
                    old_raw = _read_bytes(path)
                    old = validate_maintenance(json.loads(old_raw.decode("utf-8")))
                    if (old["owner_pid"] == os.getpid()
                            and old["owner_creation_filetime"] == creation):
                        previous = old_raw
                        normalized = tuple(sorted(
                            set(normalized) | set(old["ports"])))
                        old_start = _parse_timestamp(old["started_at"])
                        old_expiry = old_start + timedelta(
                            seconds=float(old["budget_seconds"]))
                        new_expiry = started + timedelta(seconds=budget)
                        started = min(started, old_start)
                        budget = max(1.0, (max(old_expiry, new_expiry)
                                           - started).total_seconds())
                except (MaintenanceError, UnicodeDecodeError, ValueError):
                    previous = None
            payload = validate_maintenance({
                "schema": MAINTENANCE_SCHEMA,
                "lease_id": lease_id,
                "owner_pid": os.getpid(),
                "owner_creation_filetime": creation,
                "started_at": _timestamp(started),
                "budget_seconds": budget,
                "ports": list(normalized),
            })
            encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":"))
                       + "\n").encode("utf-8")
            if len(encoded) > MAINTENANCE_MAX_BYTES:
                raise MaintenanceError("maintenance flag is oversized")
            _atomic_write(path, encoded, replace_fn=replace_fn)
    except Exception as exc:  # noqa: BLE001 - restart remains independently safe
        return MaintenanceLease(path, None, previous,
                                f"{type(exc).__name__}: {str(exc)[:180]}")
    return MaintenanceLease(path, lease_id, previous)


def end_maintenance(lease: MaintenanceLease, *, replace_fn=os.replace) -> bool:
    """Clear only this exact lease; never remove a newer concurrent lease."""
    if not isinstance(lease, MaintenanceLease) or not lease.published:
        return False
    with _MAINT_LOCK:
        try:
            current = load_maintenance(lease.path)
        except MaintenanceError:
            return False
        if current["lease_id"] != lease.lease_id:
            return False
        try:
            if lease.previous is not None:
                _atomic_write(lease.path, lease.previous, replace_fn=replace_fn)
            else:
                lease.path.unlink()
        except OSError:
            return False
    return True


@contextmanager
def maintenance_window(fleet_or_base, ports, budget_seconds, *, now=None,
                       identity_fn=get_process_identity, progress=None):
    lease = begin_maintenance(
        fleet_or_base, ports, budget_seconds, now=now,
        identity_fn=identity_fn)
    if lease.error and progress is not None:
        try:
            progress(f"fleet maintenance flag unavailable ({lease.error})")
        except Exception:  # noqa: BLE001 - diagnostics cannot block restart
            pass
    try:
        yield lease
    finally:
        end_maintenance(lease)


def hard_signal_kind(value) -> str | None:
    """Return a controlled hard-death token; timeouts/pacing always stay soft."""
    if isinstance(value, str):
        return value if value in _HARD_TOKENS else None
    current = value
    seen = set()
    while isinstance(current, BaseException) and id(current) not in seen:
        seen.add(id(current))
        name = type(current).__name__
        if name in _SOFT_CLASS_NAMES or isinstance(current, TimeoutError):
            return None
        if name == "ConnectionLost":
            return "connection_lost"
        if isinstance(current, ConnectionRefusedError):
            return "connection_refused"
        if isinstance(current, (ConnectionResetError, ConnectionAbortedError,
                                BrokenPipeError)):
            return "socket_reset"
        if isinstance(current, OSError):
            if getattr(current, "winerror", None) in _HARD_WINERRORS:
                return ("connection_refused"
                        if current.winerror == 10061 else "socket_reset")
            if getattr(current, "errno", None) in _HARD_ERRNOS:
                return ("connection_refused"
                        if current.errno == errno.ECONNREFUSED else "socket_reset")
        current = current.__cause__ or current.__context__
    return None


class HardSignalAdapter:
    """Observe adapter failures without changing adapter return behavior."""

    def __init__(self, delegate, on_hard_signal):
        object.__setattr__(self, "_delegate", delegate)
        object.__setattr__(self, "_on_hard_signal", on_hard_signal)

    def __call__(self):
        return self

    def _observe(self, exc):
        kind = hard_signal_kind(exc)
        if kind is not None:
            try:
                self._on_hard_signal(kind, exc)
            except Exception:  # noqa: BLE001 - observation cannot mask failure
                pass

    def __getattr__(self, name):
        try:
            target = getattr(self._delegate, name)
        except Exception as exc:
            self._observe(exc)
            raise
        if not callable(target):
            return target

        @functools.wraps(target)
        def observed(*args, **kwargs):
            try:
                return target(*args, **kwargs)
            except Exception as exc:
                self._observe(exc)
                raise
        return observed

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        try:
            setattr(self._delegate, name, value)
        except Exception as exc:
            self._observe(exc)
            raise


class HardDeathWatchdog:
    """Thread-safe HEALTHY/SUSPECT/DEAD state and fleet HOLD accounting."""

    def __init__(self, ports, *, clock=time.monotonic,
                 maintenance_provider=None, port_grace_s=PORT_GRACE_S,
                 fleet_grace_s=FLEET_GRACE_S,
                 probe_backoff_s=PROBE_BACKOFF_S):
        normalized = _normalize_ports(ports)
        backoff = tuple(float(value) for value in probe_backoff_s)
        if not backoff or any(value <= 0 for value in backoff):
            raise ValueError("probe backoff must contain positive values")
        self._ports = normalized
        self._clock = clock
        self._maintenance_provider = maintenance_provider
        self._port_grace_s = max(0.0, float(port_grace_s))
        self._fleet_grace_s = max(0.0, float(fleet_grace_s))
        self._backoff = backoff
        self._lock = threading.RLock()
        self._events = {port: threading.Event() for port in normalized}
        self._records = {
            port: {"state": HEALTHY, "grace": 0.0,
                   "next_probe": float("inf"), "probe_index": 0,
                   "reason": None, "interrupt_sequence": 0}
            for port in normalized
        }
        self._last = float(clock())
        self._fleet_grace = 0.0
        self._maintenance = MaintenanceStatus(False, "missing")

    @property
    def ports(self):
        return self._ports

    def interrupt_event(self, port):
        return self._events[int(port)]

    def interrupt_sequence(self, port):
        """Return a monotonic hard-interrupt generation for one worker."""
        with self._lock:
            return self._records[int(port)]["interrupt_sequence"]

    def _read_maintenance(self):
        if self._maintenance_provider is None:
            return MaintenanceStatus(False, "missing")
        try:
            value = self._maintenance_provider()
        except Exception as exc:  # noqa: BLE001 - fail closed
            return MaintenanceStatus(False, f"provider_error:{type(exc).__name__}")
        if isinstance(value, MaintenanceStatus):
            return value
        if isinstance(value, dict):
            return MaintenanceStatus(
                bool(value.get("valid")), str(value.get("reason") or "invalid"),
                tuple(int(p) for p in value.get("ports") or ()))
        return MaintenanceStatus(False, "invalid_provider_result")

    def _advance_locked(self, now):
        now = float(now)
        elapsed = max(0.0, now - self._last)
        self._last = max(self._last, now)
        maintenance = self._read_maintenance()
        self._maintenance = maintenance
        covered = set(maintenance.ports) if maintenance.valid else set()
        for port, record in self._records.items():
            if record["state"] == HEALTHY:
                continue
            if port not in covered:
                record["grace"] += elapsed
            if record["grace"] >= self._port_grace_s:
                record["state"] = DEAD
        healthy = [p for p, r in self._records.items()
                   if r["state"] == HEALTHY]
        maintenance_active = bool(
            maintenance.valid and set(self._ports) & set(maintenance.ports))
        if healthy:
            self._fleet_grace = 0.0
        elif not maintenance_active:
            self._fleet_grace += elapsed
        return healthy, maintenance_active

    def hard_signal(self, port, value, *, now=None) -> bool:
        kind = hard_signal_kind(value)
        if kind is None:
            return False
        port = int(port)
        with self._lock:
            stamp = float(self._clock() if now is None else now)
            self._advance_locked(stamp)
            record = self._records[port]
            if record["state"] == HEALTHY:
                record["state"] = SUSPECT
                record["grace"] = 0.0
                record["probe_index"] = 0
                record["next_probe"] = stamp + self._backoff[0]
            record["reason"] = kind
            event = self._events[port]
            if not event.is_set():
                record["interrupt_sequence"] += 1
                event.set()
            self._advance_locked(stamp)
        return True

    def due_probes(self, *, now=None):
        with self._lock:
            stamp = float(self._clock() if now is None else now)
            self._advance_locked(stamp)
            return [port for port in self._ports
                    if self._records[port]["state"] != HEALTHY
                    and self._records[port]["next_probe"] <= stamp]

    def probe_result(self, port, up, *, now=None):
        port = int(port)
        with self._lock:
            stamp = float(self._clock() if now is None else now)
            self._advance_locked(stamp)
            record = self._records[port]
            if up:
                record.update({"state": HEALTHY, "grace": 0.0,
                               "next_probe": float("inf"), "probe_index": 0,
                               "reason": None})
                self._events[port].clear()
            else:
                index = min(record["probe_index"] + 1,
                            len(self._backoff) - 1)
                record["probe_index"] = index
                record["next_probe"] = stamp + self._backoff[index]
                if record["grace"] >= self._port_grace_s:
                    record["state"] = DEAD
            self._advance_locked(stamp)
            return record["state"]

    def snapshot(self, *, now=None):
        with self._lock:
            stamp = float(self._clock() if now is None else now)
            healthy, maintenance_active = self._advance_locked(stamp)
            return {
                "ports": {
                    port: {"state": record["state"],
                           "grace_seconds": record["grace"],
                           "reason": record["reason"]}
                    for port, record in self._records.items()
                },
                "healthy_ports": list(healthy),
                "holding": not bool(healthy),
                "fleet_grace_seconds": self._fleet_grace,
                "maintenance_active": maintenance_active,
                "maintenance_reason": self._maintenance.reason,
                "finalize": (not healthy and not maintenance_active
                             and self._fleet_grace >= self._fleet_grace_s),
            }

    def state(self, port, *, now=None):
        return self.snapshot(now=now)["ports"][int(port)]["state"]

    def healthy_ports(self, *, now=None):
        return self.snapshot(now=now)["healthy_ports"]

    def holding(self, *, now=None):
        return bool(self.snapshot(now=now)["holding"])

    def should_finalize(self, *, now=None):
        return bool(self.snapshot(now=now)["finalize"])
