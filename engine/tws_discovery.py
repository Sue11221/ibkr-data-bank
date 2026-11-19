"""Portable IBKR/TWS application discovery and loopback port probing.

Discovery is intentionally derived on every call.  Only an explicit manual
selection is remembered, and that state is keyed by hostname so a copied
project cannot inherit another machine's executable path.

``resolve_tws()`` looks in this order and the first valid ``tws.exe`` wins:
the standard install folders, the Windows installed-programs list (uninstall
entries whose display name starts with "Trader Workstation"), "Trader
Workstation" shortcuts on the Desktop and in the Start Menu, then the
remembered manual choice.  Registry values and shortcut files are read as
data only: nothing is run, nothing found is stored, the Desktop and Start
Menu folders are asked from Windows on every call, and every read is capped.
"""
from __future__ import annotations

import json
import os
import platform
import socket
import struct
import tempfile
import uuid
from pathlib import Path

try:  # Windows only; without it the installed-programs source finds nothing.
    import winreg as _winreg
except ImportError:
    _winreg = None
try:  # Without it (or off Windows) the shortcut source finds nothing.
    import ctypes as _ctypes
except ImportError:
    _ctypes = None


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

# Installed-programs list and shortcut sources: names, caps, Windows facts.
_TWS_NAME_PREFIX = "trader workstation"    # display/shortcut name, casefolded
_MAX_TEXT_CHARS = 4096                     # longest registry or shortcut path
_UNINSTALL_KEY = r"Software\Microsoft\Windows\CurrentVersion\Uninstall"
_MAX_UNINSTALL_ENTRIES = 2048              # entries read per registry tree/view
# Windows known-folder ids, asked on every call so a redirected Desktop still
# works, each with the number of subfolder levels searched below it.
_DESKTOP = "B4BFCC3A-DB2C-424C-B029-7FE99A87C641"
_PUBLIC_DESKTOP = "C4AA340D-F20F-4863-AFEF-F87EF2E6BA25"
_START_MENU_PROGRAMS = "A77F5D77-2E2B-44C3-A6A2-ABA601054A51"
_COMMON_START_MENU_PROGRAMS = "0139D44E-6AFE-49F2-8690-3DAFCAE6FFB8"
_MAX_MENU_DEPTH = 2                        # levels below a Start Menu folder
_SHORTCUT_FOLDERS = ((_DESKTOP, 0), (_PUBLIC_DESKTOP, 0),
                     (_START_MENU_PROGRAMS, _MAX_MENU_DEPTH),
                     (_COMMON_START_MENU_PROGRAMS, _MAX_MENU_DEPTH))
_MAX_FOLDER_ENTRIES = 4096                 # directory entries per folder searched
_MAX_SHORTCUT_FILES = 16                   # matching shortcuts read per folder
_MAX_SHORTCUT_BYTES = 64 * 1024            # a larger shortcut file is skipped
_REPARSE_POINT = 0x400                     # junction or link: never descended
_CLOUD_ONLY = 0x1000 | 0x40000 | 0x400000  # offline/recall: a read would download
# Shell-link (.lnk) layout, from the published MS-SHLLINK file format.
_LNK_HEADER_SIZE = 0x4C
_LNK_CLSID = bytes.fromhex("0114020000000000c000000000000046")
_LNK_HAS_ID_LIST = 0x01
_LNK_HAS_LINK_INFO = 0x02
_LNK_STRING_FLAGS = (0x04, 0x08, 0x10, 0x20, 0x40)  # name ... icon location
_LNK_IS_UNICODE = 0x80
_LNK_LOCAL_PATH = 0x01                     # LinkInfo VolumeIDAndLocalBasePath
_LNK_ENV_BLOCK = (0x314, 0xA0000001)       # environment block size, signature


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
                      base / "Interactive Brokers" / "TWS",
                      base / "Programs" / "tws"))
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


def _find_in_roots(roots) -> list[str]:
    found = []
    seen = set()
    for root in roots:
        for candidate in _root_candidates(root):
            valid = _valid_executable(candidate)
            if valid is None:
                continue
            key = os.path.normcase(valid)
            if key not in seen:
                seen.add(key)
                found.append(valid)
    return found


def discover_tws(roots=None) -> list[str]:
    """Find ``tws.exe`` under bounded roots in deterministic priority order."""
    if roots is None:
        selected = candidate_roots()
    elif isinstance(roots, (str, bytes, os.PathLike)):
        selected = _dedupe_paths([roots], limit=_MAX_ROOTS)
    else:
        selected = _dedupe_paths(roots, limit=_MAX_ROOTS)
    return _find_in_roots(selected)


def _path_from(text, *, network=False) -> Path | None:
    """An absolute path from registry, shortcut or Windows folder text.

    A relative path would resolve against the working folder and is refused.
    A path with two leading separators (network share or device) is refused
    too unless ``network`` is set, which only the Windows folder answer uses.
    """
    if not isinstance(text, str):
        return None
    text = text.strip().strip('"').strip()
    if len(text) > _MAX_TEXT_CHARS:
        return None
    if not network and text.replace("/", "\\").startswith("\\\\"):
        return None
    path = Path(text)
    return path if path.is_absolute() else None


# Replaceable providers (test seams): a winreg-like object, and a function
# giving a known folder's path for its id.  None means "not available here".
REGISTRY_API = _winreg


def _close_key(registry, key) -> None:
    try:
        registry.CloseKey(key)
    except OSError:
        pass


def _registry_text(registry, key, name) -> str | None:
    try:
        value, kind = registry.QueryValueEx(key, name)
    except OSError:
        return None
    if (not isinstance(value, str)
            or kind not in (registry.REG_SZ, registry.REG_EXPAND_SZ)):
        return None
    return os.path.expandvars(value) if kind == registry.REG_EXPAND_SZ else value


def _registry_folder(registry, tree, name, access) -> Path | None:
    """The install folder of one uninstall entry named "Trader Workstation"."""
    try:
        entry = registry.OpenKey(tree, name, 0, access)
    except OSError:
        return None
    try:
        display = _registry_text(registry, entry, "DisplayName")
        if (display is None
                or not display.strip().casefold().startswith(_TWS_NAME_PREFIX)):
            return None
        return _path_from(_registry_text(registry, entry, "InstallLocation"))
    finally:
        _close_key(registry, entry)


def _registry_folders(registry) -> list[Path]:
    folders = []
    for hive in (registry.HKEY_CURRENT_USER, registry.HKEY_LOCAL_MACHINE):
        for view in (registry.KEY_WOW64_64KEY, registry.KEY_WOW64_32KEY):
            access = registry.KEY_READ | view
            try:
                tree = registry.OpenKey(hive, _UNINSTALL_KEY, 0, access)
            except OSError:
                continue
            try:
                for index in range(_MAX_UNINSTALL_ENTRIES):
                    try:
                        name = registry.EnumKey(tree, index)
                    except OSError:
                        break
                    folder = _registry_folder(registry, tree, name, access)
                    if folder is not None:
                        folders.append(folder)
            finally:
                _close_key(registry, tree)
    return _dedupe_paths(folders, limit=_MAX_ROOTS)


def discover_registry() -> list[str]:
    """Find ``tws.exe`` through the Windows installed-programs list.

    Reads the uninstall entries of this account's tree, then the machine-wide
    tree, each in the 64-bit then the 32-bit view, at most
    ``_MAX_UNINSTALL_ENTRIES`` per tree and view.  An entry whose display name
    starts with "Trader Workstation" gives its InstallLocation folder, searched
    like a root.  Never raises; without a registry it finds nothing.
    """
    registry = REGISTRY_API
    if registry is None:
        return []
    try:
        return _find_in_roots(_registry_folders(registry))
    except Exception:  # noqa: BLE001 - outside data never stops the search
        return []


def _windows_known_folder(folder_id: str) -> str | None:
    """Ask Windows for a known folder's current path (redirection included).

    None when the call fails or is not available (no ctypes, not Windows).
    """
    try:
        load = _ctypes.WinDLL   # AttributeError without ctypes or off Windows
        guid = (_ctypes.c_ubyte * 16).from_buffer_copy(uuid.UUID(folder_id).bytes_le)
        query = load("shell32").SHGetKnownFolderPath
        query.argtypes = (_ctypes.c_void_p, _ctypes.c_uint32, _ctypes.c_void_p,
                          _ctypes.POINTER(_ctypes.c_void_p))
        query.restype = _ctypes.c_long
        release = load("ole32").CoTaskMemFree
        release.argtypes = (_ctypes.c_void_p,)
        release.restype = None
        buffer = _ctypes.c_void_p()
        status = query(guid, 0, None, _ctypes.byref(buffer))
        try:
            if status != 0 or not buffer.value:
                return None
            return _ctypes.wstring_at(buffer.value)
        finally:
            if buffer.value:
                release(buffer)
    except (OSError, AttributeError, TypeError, ValueError):
        return None


KNOWN_FOLDER_PATH = _windows_known_folder


def _shortcut_folders() -> list[tuple[Path, int]]:
    provider = KNOWN_FOLDER_PATH
    if provider is None:
        return []
    folders = []
    for folder_id, depth in _SHORTCUT_FOLDERS:
        folder = _path_from(provider(folder_id), network=True)
        if folder is not None:
            folders.append((folder, depth))
    return folders


def _shortcut_files(folder: Path, depth: int) -> list[Path]:
    """Matching shortcut files under ``folder``, breadth first, within caps.

    Junctions and links are never followed, and a cloud placeholder whose data
    is not on this machine is skipped rather than downloaded.
    """
    found = []
    budget = _MAX_FOLDER_ENTRIES
    pending = [(folder, 0)]
    while pending and budget > 0 and len(found) < _MAX_SHORTCUT_FILES:
        current, level = pending.pop(0)
        entries = []
        try:
            with os.scandir(current) as listing:
                for entry in listing:
                    entries.append(entry)
                    budget -= 1
                    if budget <= 0:
                        break
        except OSError:
            continue
        # On Windows these DirEntry answers come from the listing itself, so
        # they make no further file-system call and cannot fail.
        for entry in sorted(entries, key=lambda item: item.name.casefold()):
            name = entry.name.casefold()
            attributes = entry.stat(follow_symlinks=False).st_file_attributes
            if entry.is_dir(follow_symlinks=False):
                if level < depth and not attributes & _REPARSE_POINT:
                    pending.append((Path(entry.path), level + 1))
            elif (name.startswith(_TWS_NAME_PREFIX) and name.endswith(".lnk")
                    and entry.is_file(follow_symlinks=False)
                    and not attributes & _CLOUD_ONLY):
                found.append(Path(entry.path))
                if len(found) >= _MAX_SHORTCUT_FILES:
                    break
    return found


def _read_shortcut(path: Path) -> bytes:
    """A shortcut file's bytes, or b"" when unreadable or over the size cap."""
    try:
        with open(path, "rb") as handle:
            data = handle.read(_MAX_SHORTCUT_BYTES + 1)
    except OSError:
        return b""
    return data if len(data) <= _MAX_SHORTCUT_BYTES else b""


def _lnk_string(data: bytes, start: int, end: int, wide: bool) -> str | None:
    """The NUL-terminated string at ``data[start:end]``, or None.

    Callers pass an ``end`` already checked against the data size; ``find``
    and slicing never read outside the data for any ``start``.
    """
    if wide:
        stop = data.find(b"\0\0", start, end)
        while stop >= 0 and (stop - start) % 2:
            stop = data.find(b"\0\0", stop + 1, end)
        codec = "utf-16-le"
    else:
        stop = data.find(b"\0", start, end)
        codec = "mbcs"          # the Windows ANSI code page; absent elsewhere
    if stop < 0:
        return None
    try:
        return data[start:stop].decode(codec)
    except (UnicodeError, LookupError):
        return None


def _shortcut_targets(data: bytes) -> list[str]:
    """The target paths written in a shell-link (.lnk) file's bytes.

    Every offset and length is checked against the data before use; damaged
    or truncated data gives fewer targets or none, never an exception.
    """
    size = len(data)
    if size < _LNK_HEADER_SIZE:
        return []
    header_size, = struct.unpack_from("<I", data, 0)
    flags, = struct.unpack_from("<I", data, 0x14)
    if header_size != _LNK_HEADER_SIZE or data[4:20] != _LNK_CLSID:
        return []
    targets = []
    offset = _LNK_HEADER_SIZE
    if flags & _LNK_HAS_ID_LIST:
        if offset + 2 > size:
            return []
        offset += 2 + struct.unpack_from("<H", data, offset)[0]
    if flags & _LNK_HAS_LINK_INFO:
        if offset + 0x1C > size:
            return []
        (info_size, info_header, info_flags, _volume, local_at, _network,
         suffix_at) = struct.unpack_from("<7I", data, offset)
        end = offset + info_size
        if info_header < 0x1C or info_header > info_size or end > size:
            return []
        if info_flags & _LNK_LOCAL_PATH:
            choices = [((local_at, suffix_at), False)]
            if info_header >= 0x24:     # the Unicode offsets come first
                choices.insert(0, (struct.unpack_from("<2I", data, offset + 0x1C), True))
            for (base_at, tail_at), wide in choices:
                base = _lnk_string(data, offset + base_at, end, wide)
                if base:
                    tail = _lnk_string(data, offset + tail_at, end, wide)
                    targets.append(base + (tail or ""))
                    break
        offset = end
    unit = 2 if flags & _LNK_IS_UNICODE else 1
    for flag in _LNK_STRING_FLAGS:
        if flags & flag:
            if offset + 2 > size:
                return targets
            offset += 2 + struct.unpack_from("<H", data, offset)[0] * unit
    while offset + 8 <= size:
        block_size, signature = struct.unpack_from("<2I", data, offset)
        if block_size < 8 or offset + block_size > size:
            break
        if (block_size, signature) == _LNK_ENV_BLOCK:
            text = (_lnk_string(data, offset + 268, offset + block_size, True)
                    or _lnk_string(data, offset + 8, offset + 268, False))
            if text:
                targets.append(os.path.expandvars(text))
        offset += block_size
    return targets


def discover_shortcuts() -> list[str]:
    """Find ``tws.exe`` through "Trader Workstation" shortcuts.

    Looks at the top level of this account's Desktop and the shared (Public)
    Desktop, and in this account's and the shared Start Menu Programs folders
    down to ``_MAX_MENU_DEPTH`` subfolder levels.  Each shortcut file is read
    as bytes for its recorded target: it is never run or resolved by the shell.
    Never raises; without the Windows folder call it finds nothing.
    """
    try:
        targets = []
        for folder, depth in _shortcut_folders():
            for link in _shortcut_files(folder, depth):
                targets.extend(_shortcut_targets(_read_shortcut(link)))
        roots = [path for path in map(_path_from, targets) if path is not None]
        return _find_in_roots(_dedupe_paths(roots, limit=_MAX_ROOTS))
    except Exception:  # noqa: BLE001 - outside data never stops the search
        return []


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
    """Re-discover first on every call, then use a validated remembered path.

    With ``roots=None`` the automatic search runs in order: the standard
    install folders ("discovered"), the installed-programs list ("registry")
    and "Trader Workstation" shortcuts ("shortcut").  Explicit ``roots``
    replace that whole automatic search, so a caller or test that names its
    roots never reads the registry or shortcuts.  The remembered manual
    choice ("remembered") is the fallback either way.
    """
    found = discover_tws(roots)
    if found:
        return found[0], "discovered"
    if roots is None:
        for source, finder in (("registry", discover_registry),
                               ("shortcut", discover_shortcuts)):
            found = finder()
            if found:
                return found[0], source
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
    "TwsDiscoveryError", "candidate_roots", "discover_registry",
    "discover_shortcuts", "discover_tws", "load_remembered", "remember_tws",
    "resolve_tws", "scan_listening_ports",
]
