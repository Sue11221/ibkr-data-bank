"""Cross-process exclusion for live market-data operations.

The gate is intentionally small: every live IBKR connection owns ``fetch`` and
the external full-range sweep owns ``external_sweep``. Different modes cannot
overlap. Same-mode acquisitions in one process are re-entrant so the existing
parallel LiveIB fleet can keep multiple connections open under one OS lock.

The lock file lives under ``Run Logs`` and never touches the storage bank.
Process exit also releases the OS lock, including abnormal exits.
"""

from __future__ import annotations

import errno
import os
import re
import threading
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCK_PATH = PROJECT_ROOT / "Run Logs" / ".market_data_operation.lock"

_MODE_RE = re.compile(r"[a-z][a-z0-9_]{0,31}\Z")
_REGISTRY_GUARD = threading.RLock()
_REGISTRY = {}


class OperationGateError(RuntimeError):
    """The operation gate could not be prepared or used."""


class OperationBusy(OperationGateError):
    """A conflicting market-data operation currently owns the gate."""


def _mode(value):
    text = str(value or "").strip().lower()
    if not _MODE_RE.fullmatch(text):
        raise OperationGateError(f"invalid operation mode: {value!r}")
    return text


def _resolved_path(path):
    target = Path(path) if path is not None else LOCK_PATH
    try:
        return target.resolve()
    except OSError as exc:
        raise OperationGateError(f"cannot resolve operation gate: {exc}") from exc


def _lock_file(handle):
    handle.seek(0)
    if os.name == "nt":
        import msvcrt
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return
    import fcntl
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file(handle):
    handle.seek(0)
    if os.name == "nt":
        import msvcrt
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl
    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _is_busy_error(exc):
    return (getattr(exc, "errno", None) in {
                errno.EACCES, errno.EAGAIN, errno.EDEADLK}
            or getattr(exc, "winerror", None) in {32, 33, 36})


class OperationLease:
    """One idempotently releasable acquisition of the operation gate."""

    def __init__(self, key, mode, owner):
        self._key = key
        self.mode = mode
        self.owner = str(owner or mode)
        self._released = False

    @property
    def path(self):
        return str(self._key)

    @property
    def released(self):
        return self._released

    def release(self):
        if self._released:
            return
        with _REGISTRY_GUARD:
            if self._released:
                return
            state = _REGISTRY.get(self._key)
            if state is None or state["mode"] != self.mode:
                self._released = True
                return
            state["count"] -= 1
            if state["count"] == 0:
                handle = state["handle"]
                try:
                    _unlock_file(handle)
                finally:
                    try:
                        handle.close()
                    finally:
                        _REGISTRY.pop(self._key, None)
            self._released = True

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        self.release()
        return False


def acquire(mode, *, owner=None, path=None):
    """Acquire ``mode`` without waiting or raise :class:`OperationBusy`.

    Same-mode acquisitions inside this process share one OS lock and reference
    count it. A different mode conflicts even when requested by the same
    process, which prevents an in-process sweep from bypassing an active fetch.
    """
    mode = _mode(mode)
    target = _resolved_path(path)
    key = str(target).casefold()
    with _REGISTRY_GUARD:
        state = _REGISTRY.get(key)
        if state is not None:
            if state["mode"] != mode:
                raise OperationBusy(
                    f"market-data operation busy ({state['mode']})")
            state["count"] += 1
            return OperationLease(key, mode, owner)

        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            handle = target.open("a+b")
        except OSError as exc:
            raise OperationGateError(
                f"cannot open operation gate {target}: {exc}") from exc
        try:
            _lock_file(handle)
        except OSError as exc:
            handle.close()
            if _is_busy_error(exc):
                raise OperationBusy("another market-data operation is active") from exc
            raise OperationGateError(
                f"cannot lock operation gate {target}: {exc}") from exc

        try:
            if target.stat().st_size == 0:
                handle.seek(0)
                handle.write(b"\0")
                handle.flush()
        except OSError:
            try:
                _unlock_file(handle)
            finally:
                handle.close()
            raise
        _REGISTRY[key] = {"mode": mode, "count": 1, "handle": handle}
        return OperationLease(key, mode, owner)


def status(*, path=None):
    """Return a no-wait availability probe without exposing stale ownership."""
    target = _resolved_path(path)
    try:
        lease = acquire("status_probe", owner="status", path=target)
    except OperationBusy:
        return {"available": False, "path": str(target)}
    lease.release()
    return {"available": True, "path": str(target)}
