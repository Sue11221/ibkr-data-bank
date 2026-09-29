"""Portable IBKR/TWS application discovery and loopback port probing.

Discovery is intentionally derived on every call.  Only an explicit manual
selection is remembered, and that state is keyed by hostname so a copied
project cannot inherit another machine's executable path.
"""
from __future__ import annotations

import json
import os
import platform
import socket
import tempfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATE_PATH = PROJECT_ROOT / "Run Logs" / "tws_app_paths.json"

# The ordinary TWS/Gateway defaults plus this project's standard demo fleet.
DEFAULT_PORTS = (7497, 4002, 7496, 4001, 2000, 3000, 4000, 5000,
                 6000, 7000, 8000, 9000)

_STATE_VERSION = 1
_MAX_STATE_BYTES = 64 * 1024
_MAX_ROOTS = 64
_MAX_PORTS = 256
_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


class TwsDiscoveryError(ValueError):
    """A manual path or remembered-state file is invalid."""


def _path_key(path: Path) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(path)))


def _dedupe_paths(paths, *, limit=None):
    seen = set()
    result = []
    for raw in paths:
        try:
            path = Path(raw).expanduser()
        except (TypeError, ValueError, OSError):
            continue
        try:
            key = _path_key(path)
        except (OSError, ValueError):
            continue
        if key in seen:
            continue
        seen.add(key)
        result.append(path)
        if limit is not None and len(result) >= limit:
            break
    return result


def candidate_roots() -> list[Path]:
    """Return bounded standard TWS install roots, most conventional first."""
    roots = [Path(r"C:\Jts")]
    for env_name in ("ProgramFiles", "ProgramFiles(x86)"):
        value = os.environ.get(env_name)
        if value:
            base = Path(value)
            roots.extend((base / "Jts",
                          base / "Interactive Brokers" / "TWS",
                          base / "Trader Workstation"))
    local = os.environ.get("LOCALAPPDATA")
    if local:
        base = Path(local)
        roots.extend((base / "Jts", base / "Programs" / "Jts",
                      base / "Programs" / "Trader Workstation",
                      base / "Interactive Brokers" / "TWS"))
    try:
        roots.append(Path.home() / "Jts")
    except (RuntimeError, OSError):
        pass
    return _dedupe_paths(roots, limit=_MAX_ROOTS)


def _valid_executable(path) -> str | None:
    try:
        candidate = Path(path).expanduser().resolve(strict=True)
        if (candidate.name.casefold() != "tws.exe"
                or not candidate.is_file()):
            return None
    except (TypeError, ValueError, OSError, RuntimeError):
        return None
    return str(candidate)


def _root_candidates(root: Path):
    """Yield only shallow, named locations; never recurse through a drive."""
    if root.name.casefold() == "tws.exe":
        yield root
        return
    yield root / "tws.exe"
    yield root / "Jts" / "tws.exe"
    yield root / "TWS" / "tws.exe"
    yield root / "Trader Workstation" / "tws.exe"
    yield root / "Interactive Brokers" / "TWS" / "tws.exe"


def discover_tws(roots=None) -> list[str]:
    """Find ``tws.exe`` under bounded roots in deterministic priority order."""
    if roots is None:
        selected = candidate_roots()
    elif isinstance(roots, (str, bytes, os.PathLike)):
        selected = _dedupe_paths([roots], limit=_MAX_ROOTS)
    else:
        selected = _dedupe_paths(roots, limit=_MAX_ROOTS)
    found = []
    seen = set()
    for root in selected:
        for candidate in _root_candidates(root):
            valid = _valid_executable(candidate)
            if valid is None:
                continue
            key = os.path.normcase(valid)
            if key not in seen:
                seen.add(key)
                found.append(valid)
    return found


def _hostname() -> str | None:
    try:
        value = str(platform.node()).strip()
    except Exception:  # noqa: BLE001 - platform facts may be unavailable
        return None
    return value if value and len(value) <= 255 else None


def _reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise TwsDiscoveryError(f"duplicate remembered-state key: {key!r}")
        result[key] = value
    return result


def _state_file(state_path=None) -> Path:
    return Path(STATE_PATH if state_path is None else state_path).expanduser()


def _read_state(path: Path, *, strict=False) -> dict:
    try:
        if not path.exists():
            return {"version": _STATE_VERSION, "hosts": {}}
        if path.is_symlink() or not path.is_file():
            raise TwsDiscoveryError("remembered TWS state is not a regular file")
        if path.stat().st_size > _MAX_STATE_BYTES:
            raise TwsDiscoveryError("remembered TWS state exceeds its size limit")
        raw = path.read_text(encoding="utf-8")
        state = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
        if (not isinstance(state, dict)
                or set(state) != {"version", "hosts"}
                or state.get("version") != _STATE_VERSION
                or not isinstance(state.get("hosts"), dict)):
            raise TwsDiscoveryError("remembered TWS state has an invalid schema")
        for host, entry in state["hosts"].items():
            if (not isinstance(host, str) or not host or len(host) > 255
                    or not isinstance(entry, dict)
                    or set(entry) != {"path"}
                    or not isinstance(entry.get("path"), str)
                    or not entry["path"] or len(entry["path"]) > 4096):
                raise TwsDiscoveryError("remembered TWS state has an invalid host entry")
        return state
    except TwsDiscoveryError:
        if strict:
            raise
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        if strict:
            raise TwsDiscoveryError(f"cannot read remembered TWS state: {exc}") from exc
    return {"version": _STATE_VERSION, "hosts": {}}


def _atomic_write(path: Path, payload: bytes) -> None:
    if len(payload) > _MAX_STATE_BYTES:
        raise TwsDiscoveryError("remembered TWS state exceeds its size limit")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and (path.is_symlink() or not path.is_file()):
            raise TwsDiscoveryError("remembered TWS state target is unsafe")
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
    except TwsDiscoveryError:
        raise
    except OSError as exc:
        raise TwsDiscoveryError(f"cannot save remembered TWS path: {exc}") from exc


def load_remembered(state_path=None) -> str | None:
    """Load this hostname's still-existing explicit TWS selection."""
    host = _hostname()
    if host is None:
        return None
    state = _read_state(_state_file(state_path), strict=False)
    entry = state["hosts"].get(host)
    if not isinstance(entry, dict):
        return None
    return _valid_executable(entry.get("path"))


def remember_tws(path, state_path=None) -> str:
    """Validate and atomically remember one explicit selection for this host."""
    executable = _valid_executable(path)
    if executable is None:
        raise TwsDiscoveryError("select the existing tws.exe application file")
    host = _hostname()
    if host is None:
        raise TwsDiscoveryError("this machine has no usable hostname")
    target = _state_file(state_path)
    state = _read_state(target, strict=True)
    state["hosts"][host] = {"path": executable}
    payload = (json.dumps(state, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":")) + "\n").encode("utf-8")
    _atomic_write(target, payload)
    return executable


def resolve_tws(roots=None, state_path=None) -> tuple[str | None, str | None]:
    """Re-discover first on every call, then use a validated remembered path."""
    found = discover_tws(roots)
    if found:
        return found[0], "discovered"
    remembered = load_remembered(state_path)
    if remembered is not None:
        return remembered, "remembered"
    return None, None


def scan_listening_ports(candidates=None, host="127.0.0.1", timeout=0.3) -> list[int]:
    """Return candidate ports accepting a TCP connection on local loopback."""
    host = str(host).strip().casefold()
    if host not in _LOOPBACK_HOSTS:
        raise TwsDiscoveryError("port discovery is restricted to loopback")
    try:
        timeout = float(timeout)
    except (TypeError, ValueError) as exc:
        raise TwsDiscoveryError("port-scan timeout must be numeric") from exc
    if not 0 < timeout <= 5.0:
        raise TwsDiscoveryError("port-scan timeout must be within (0, 5] seconds")

    source = DEFAULT_PORTS if candidates is None else candidates
    if isinstance(source, (str, bytes, int)):
        source = [source]
    ports = []
    seen = set()
    try:
        iterator = iter(source)
    except TypeError as exc:
        raise TwsDiscoveryError("port candidates must be iterable") from exc
    for index, raw in enumerate(iterator):
        if index >= _MAX_PORTS * 4:
            break
        if isinstance(raw, bool):
            continue
        try:
            port = int(raw)
        except (TypeError, ValueError, OverflowError):
            continue
        if 1 <= port <= 65535 and port not in seen:
            seen.add(port)
            ports.append(port)
        if len(ports) >= _MAX_PORTS:
            break

    listening = []
    for port in ports:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                listening.append(port)
        except OSError:
            pass
    return listening


__all__ = [
    "TwsDiscoveryError", "candidate_roots", "discover_tws",
    "load_remembered", "remember_tws", "resolve_tws",
    "scan_listening_ports",
]
