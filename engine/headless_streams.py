"""Make console-less app starts print-safe and leave bounded crash logs."""

from __future__ import annotations

import datetime as dt
import os
import stat
import sys
from pathlib import Path


SESSION_LOG_GLOB = "app-session-*.log"
DEFAULT_KEEP = 10
RUN_LOGS_NAME = "Run Logs"


class _NullSink:
    """Last-resort text stream whose basic operations cannot fail."""

    encoding = "utf-8"
    errors = "replace"
    closed = False

    def write(self, value):
        try:
            return len(str(value))
        except BaseException:  # pragma: no cover - hostile object fallback
            return 0

    def flush(self):
        return None

    def close(self):
        return None

    def isatty(self):
        return False

    def writable(self):
        return True


def _usable(stream):
    try:
        return bool(
            stream is not None
            and not getattr(stream, "closed", False)
            and callable(getattr(stream, "write", None))
            and callable(getattr(stream, "flush", None)))
    except BaseException:  # noqa: BLE001 - the public boundary never raises
        return False


def _keep_count(value):
    try:
        if isinstance(value, bool):
            raise ValueError("boolean keep")
        return max(1, int(value))
    except (TypeError, ValueError, OverflowError):
        return DEFAULT_KEEP


def _session_path(run_logs):
    stamp = dt.datetime.now().astimezone().strftime("%Y%m%d-%H%M%S-%f")
    return run_logs / f"app-session-{stamp}-{os.getpid()}.log"


def _rotate(run_logs, keep):
    """Best-effort oldest-first rotation; an open live log always survives."""
    try:
        rows = []
        for path in run_logs.glob(SESSION_LOG_GLOB):
            try:
                info = path.stat()
            except OSError:
                continue
            if stat.S_ISREG(info.st_mode):
                rows.append((int(info.st_mtime_ns), path.name, path))
        rows.sort(reverse=True)
        for _mtime, _name, path in rows[_keep_count(keep):]:
            try:
                path.unlink()
            except OSError:
                pass
    except BaseException:  # noqa: BLE001 - rotation cannot fail app startup
        pass


def _fallback(out_ok, err_ok):
    sink = None
    try:
        sink = open(os.devnull, "a", encoding="utf-8", buffering=1)
    except BaseException:  # noqa: BLE001 - in-memory sink is the final fence
        sink = _NullSink()
    try:
        if not out_ok:
            sys.stdout = sink
    except BaseException:  # pragma: no cover - sys module attrs are writable
        pass
    try:
        if not err_ok:
            sys.stderr = sink
    except BaseException:  # pragma: no cover - sys module attrs are writable
        pass
    return sink


def ensure_streams(root, *, keep=DEFAULT_KEEP):
    """Install UTF-8 session-log streams only where stdout/stderr are invalid.

    A normal terminal run is an identity-preserving no-op.  A pythonw run gets
    one shared line-buffered log for stdout and stderr, followed by a bounded
    rotation of that exact family.  Any filesystem failure degrades to a safe
    sink and never escapes into app startup.
    """
    out_ok = _usable(getattr(sys, "stdout", None))
    err_ok = _usable(getattr(sys, "stderr", None))
    if out_ok and err_ok:
        return None

    stream = None
    try:
        run_logs = Path(root) / RUN_LOGS_NAME
        run_logs.mkdir(parents=True, exist_ok=True)
        path = _session_path(run_logs)
        stream = path.open("a", encoding="utf-8", buffering=1)
        if not out_ok:
            sys.stdout = stream
        if not err_ok:
            sys.stderr = stream
        _rotate(run_logs, keep)
        return path
    except BaseException:  # noqa: BLE001 - app startup must remain print-safe
        try:
            if stream is not None:
                stream.close()
        except BaseException:  # pragma: no cover - cleanup only
            pass
        _fallback(out_ok, err_ok)
        return None


__all__ = ["DEFAULT_KEEP", "SESSION_LOG_GLOB", "ensure_streams"]
