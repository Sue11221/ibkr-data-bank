"""Row 54 M0 acceptance harness — IBKR/TWS app auto-discovery, remembered manual
path (machine-scoped), step-1-first resolution, and port auto-scan.

CLAUDE-OWNED reference harness (Codex MUST NOT edit this file).
Plan: TWS_DISCOVERY_PLAN.md. Offline, headless, deterministic. NEVER launches or
touches TWS; no network — the only sockets are loopback listeners this harness
itself opens and closes. Launcher/GUI checks are SOURCE-level (no display_data /
tws_launch import — the launcher module is never executed here).

Row 103 (F7-F14): the installed-programs list and the "Trader Workstation"
shortcuts are tested through the module's seams only, with a fake registry and
shortcut files this harness builds byte by byte in scratch folders; this
machine's registry and shortcuts are never read.

Exit codes:
  3 = feature absent (no TWS_APP_DISCOVERY in tws_launch) + baseline pins hold.
  0 = feature present + all feature checks pass.
  1 = any check failed, or the harness itself broke.
"""

import importlib.util
import json
import os
import re
import socket
import struct
import sys
import tempfile
import shutil
import types
import uuid
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

_TWS_LAUNCH = _HERE / "tws_launch.py"
_DISCOVERY = _HERE / "tws_discovery.py"
_DISPLAY = _HERE.parent / "display_data.py"

FLAG = "TWS_APP_DISCOVERY"
HARD_EXE = r'TWS_EXE = r"C:\Jts\tws.exe"'
RAISE_PIN = "TWS not found"
DISC_NAMES = ("discover_tws", "load_remembered", "remember_tws", "resolve_tws")


def _src(p):
    return p.read_text(encoding="utf-8", errors="replace")


# --- baseline pins (feature absent) -------------------------------------------

def b1_absence():
    if _DISCOVERY.exists():
        return False, "engine/tws_discovery.py already exists"
    if FLAG in _src(_TWS_LAUNCH):
        return False, f"{FLAG} already present in tws_launch.py"
    return True, "no tws_discovery module, no flag"


def b2_hardcode_deadend():
    src = _src(_TWS_LAUNCH)
    if HARD_EXE not in src:
        return False, "hardcoded TWS_EXE literal gone - pin stale"
    n = src.count(RAISE_PIN)
    if n < 3:
        return False, f"only {n} 'TWS not found' raise-sites (pin expects >=3)"
    present = [x for x in DISC_NAMES if x in src]
    if present:
        return False, f"discovery identifiers already in tws_launch: {present}"
    return True, (f"hardcoded C:\\Jts exe + {n} dead-end raise-sites, "
                  "zero discovery/remember flow")


def b3_gui_manual_only():
    src = _src(_DISPLAY)
    hits = [x for x in ("resolve_tws", "tws_discovery", "scan_listening_ports")
            if x in src]
    if hits:
        return False, f"GUI already references discovery: {hits}"
    return True, "GUI has no resolver/browse-flow/port-scan references (manual-only)"


# --- feature checks (flag present) ---------------------------------------------

def _mk_fake_install(base):
    root = Path(base) / "fakeroot"
    exe = root / "Jts" / "tws.exe"
    exe.parent.mkdir(parents=True, exist_ok=True)
    exe.write_bytes(b"MZ fake")
    return root, exe


def f1_discovery():
    import tws_discovery as td
    tmp = tempfile.mkdtemp(prefix="twsdisc_f1_")
    try:
        root, exe = _mk_fake_install(tmp)
        found = [str(Path(p)) for p in td.discover_tws([root])]
        if str(exe) not in found:
            return False, f"discover_tws missed the fake exe: {found}"
        empty = Path(tmp) / "empty"
        empty.mkdir()
        if td.discover_tws([empty]):
            return False, "discover_tws found something in an empty root"
        return True, "finds the fake Jts/tws.exe; empty root -> nothing"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def f2_remember_lifecycle():
    import tws_discovery as td
    tmp = tempfile.mkdtemp(prefix="twsdisc_f2_")
    try:
        root, exe = _mk_fake_install(tmp)
        state = Path(tmp) / "tws_state.json"
        td.remember_tws(exe, state_path=state)
        got = td.load_remembered(state_path=state)
        if got is None or str(Path(got)) != str(exe):
            return False, f"remember/load mismatch: {got!r}"
        import platform
        host = platform.node()
        raw = state.read_text(encoding="utf-8")
        if host not in raw:
            return False, "state file does not key by this machine's hostname"
        exe.unlink()
        if td.load_remembered(state_path=state) is not None:
            return False, "stale (deleted) remembered path still honored"
        exe.write_bytes(b"MZ fake")
        state.write_text(raw.replace(host, "OTHER-MACHINE-X"), encoding="utf-8")
        if td.load_remembered(state_path=state) is not None:
            return False, "foreign-hostname entry honored - copied folder would NOT re-prompt"
        return True, ("remember -> load ok; deleted exe -> None; foreign hostname "
                      "-> None (copied-folder re-prompt holds)")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def f3_resolve_order():
    import tws_discovery as td
    tmp = tempfile.mkdtemp(prefix="twsdisc_f3_")
    try:
        root, exe = _mk_fake_install(tmp)
        other = Path(tmp) / "manual" / "tws.exe"
        other.parent.mkdir(parents=True)
        other.write_bytes(b"MZ fake2")
        state = Path(tmp) / "s.json"
        td.remember_tws(other, state_path=state)
        p, srcname = td.resolve_tws(roots=[root], state_path=state)
        if srcname != "discovered" or str(Path(p)) != str(exe):
            return False, f"discovery did not win step 1: {p!r} {srcname!r}"
        empty = Path(tmp) / "empty"
        empty.mkdir()
        p, srcname = td.resolve_tws(roots=[empty], state_path=state)
        if srcname != "remembered" or str(Path(p)) != str(other):
            return False, f"remembered fallback wrong: {p!r} {srcname!r}"
        p, srcname = td.resolve_tws(roots=[empty], state_path=Path(tmp) / "none.json")
        if p is not None or srcname is not None:
            return False, f"empty world should be (None, None), got {p!r} {srcname!r}"
        return True, "discovered wins; remembered is the fallback; else (None, None)"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def f4_port_scan():
    import tws_discovery as td
    live, socks = [], []
    try:
        for _ in range(2):
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            s.listen(1)
            socks.append(s)
            live.append(s.getsockname()[1])
        dead = []
        for _ in range(2):
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            dead.append(s.getsockname()[1])
            s.close()
        got = set(td.scan_listening_ports(candidates=live + dead))
        if got != set(live):
            return False, f"scan returned {sorted(got)}, expected {sorted(live)}"
        return True, f"exactly the 2 live loopback listeners found, dead ports excluded"
    finally:
        for s in socks:
            s.close()


def f5_gui_pins():
    src = _src(_DISPLAY)
    probs = []
    if "resolve_tws" not in src and "tws_discovery" not in src:
        probs.append("GUI never consults the resolver")
    if "scan_listening_ports" not in src:
        probs.append("ports editor has no scan-helper reference")
    low = src.lower()
    b, d = low.find("browse"), low.find("download")
    if b == -1:
        probs.append("no Browse wording in the GUI")
    elif d != -1 and d < b:
        probs.append("download wording precedes the browse-first flow")
    if probs:
        return False, "; ".join(probs)
    return True, "resolver + scan section referenced; browse-first ordering holds"


def f6_launcher_rewired():
    src = _src(_TWS_LAUNCH)
    if not re.search(r"^TWS_APP_DISCOVERY\s*=\s*True", src, re.M):
        return False, "flag not True at module level in tws_launch"
    if "resolve_tws" not in src:
        return False, "tws_launch raise-sites not routed through resolve_tws"
    return True, "flag True; launcher consults resolve_tws"


# --- Row 103: the installed-programs list and shortcut sources -----------------
#
# Fakes only.  The registry is _FakeRegistry; the Desktop and Start Menu
# folders come from a fake known-folder function over scratch folders; every
# shortcut is built here byte by byte from the published shell-link layout
# (MS-SHLLINK).  main() turns the module's standard folders, registry and
# Windows-folder providers off before any feature check runs, and _Seams turns
# them off again around each check, so no check, and no mutant of the module
# in a mutation run, reads this machine's registry or shortcuts.  The one real
# Windows call (F13) asks for the Windows folder itself.  Folder listings are
# real, except in one F9 case where a stand-in listing (_Listing) gives the
# answers Windows gives for symbolic links, which a PC without the privilege
# to make them cannot produce.

_KEY_READ = 0x20019                    # winreg values, written out here
_KEY_64, _KEY_32 = 0x0100, 0x0200
_SZ, _EXPAND_SZ, _BINARY, _DWORD, _MULTI_SZ = 1, 2, 3, 4, 7
_UNINSTALL = r"Software\Microsoft\Windows\CurrentVersion\Uninstall"
_HKCU, _HKLM = "HKCU", "HKLM"          # the fake's own tree handles
# Known-folder ids, copied from the Windows SDK header KnownFolders.h.
_FOLDER_IDS = {"own_desktop": "B4BFCC3A-DB2C-424C-B029-7FE99A87C641",
               "public_desktop": "C4AA340D-F20F-4863-AFEF-F87EF2E6BA25",
               "own_menu": "A77F5D77-2E2B-44C3-A6A2-ABA601054A51",
               "common_menu": "0139D44E-6AFE-49F2-8690-3DAFCAE6FFB8"}
_WINDOWS_FOLDER = "F38BF404-1D43-42F2-9305-67DE0B28FC23"
_LNK_CLSID = bytes.fromhex("0114020000000000c000000000000046")
_ENV_NAME = "TWSREF_ROOT"              # set by the checks, expanded by the module
_VAR = "%" + _ENV_NAME + "%"
_MISSING = object()


class _Fail(Exception):
    """One unmet expectation; its text is the check's FAIL detail."""


def _need(condition, message):
    if not condition:
        raise _Fail(message)


def _checked(check):
    """Run a check that raises _Fail or returns its PASS detail."""
    def run():
        try:
            return True, check()
        except _Fail as exc:
            return False, str(exc)
    return run


def _key(path):
    if path is None:
        return "<none>"
    return os.path.normcase(os.path.abspath(os.fspath(path)))


def _same(found, expected):
    """found names exactly the expected files, in this order."""
    return [_key(p) for p in found] == [_key(Path(p).resolve()) for p in expected]


def _ansi_safe(path):
    """True when the path survives the Windows ANSI code page unchanged."""
    try:
        return str(path).encode("mbcs").decode("mbcs") == str(path)
    except (UnicodeError, LookupError):
        return False


class _Scratch:
    """One check's scratch folder.  The working folder, the environment and
    any junction are put back, and the folder is removed, afterwards."""

    def __init__(self, prefix):
        self.prefix = prefix
        self.junctions = []
        self.count = 0

    def __enter__(self):
        self.root = Path(tempfile.mkdtemp(prefix=self.prefix)).resolve()
        self.cwd = os.getcwd()
        self.env = {name: os.environ.get(name) for name in (
            _ENV_NAME, "LOCALAPPDATA", "ProgramFiles", "ProgramFiles(x86)", "USERPROFILE")}
        return self

    def __exit__(self, *exc):
        os.chdir(self.cwd)
        for name, value in self.env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        for link in self.junctions:
            try:
                os.rmdir(link)
            except OSError:
                pass
        shutil.rmtree(self.root, ignore_errors=True)
        return False

    def path(self, *parts):
        return self.root.joinpath(*parts)

    def folder(self, *parts):
        path = self.path(*parts)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def fresh(self, stem="case"):
        self.count += 1
        return self.folder(f"{stem}{self.count:03d}")

    def file(self, data, *parts):
        path = self.path(*parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    def exe(self, *parts):
        return self.file(b"MZ fake", *parts)


class _Seams:
    """Swap tws_discovery's seams for one check and restore them afterwards.

    By default every outside source is off: no standard folder, no registry,
    no known folder.  Keyword arguments install what a check needs.
    """
    NAMES = ("candidate_roots", "REGISTRY_API", "KNOWN_FOLDER_PATH", "_ctypes",
             "_MAX_FOLDER_ENTRIES", "_MAX_ROOTS")

    def __init__(self, td, **changes):
        unknown = sorted(set(changes) - set(self.NAMES))
        if unknown:
            raise _Fail(f"harness: unknown seams {unknown}")
        self.td, self.changes = td, changes

    def __enter__(self):
        self.saved = {name: getattr(self.td, name) for name in self.NAMES}
        self.td.candidate_roots = lambda: []
        self.td.REGISTRY_API = None
        self.td.KNOWN_FOLDER_PATH = None
        for name, value in self.changes.items():
            setattr(self.td, name, value)
        return self.td

    def __exit__(self, *exc):
        for name, value in self.saved.items():
            setattr(self.td, name, value)
        return False


class _FakeRegistry:
    """A winreg stand-in holding uninstall entries per (tree, view).

    It answers only the exact uninstall key opened read-only in one explicit
    view, opens an entry only with its tree's access, records every
    enumeration and every call, and refuses to close a key twice.  An entry is
    (name, {value name: (value, kind) or an exception}) or (name, exception);
    an exception in the list itself is raised by that enumeration step.
    """
    HKEY_CURRENT_USER = _HKCU
    HKEY_LOCAL_MACHINE = _HKLM
    KEY_READ = _KEY_READ
    KEY_WOW64_64KEY = _KEY_64
    KEY_WOW64_32KEY = _KEY_32
    REG_SZ = _SZ
    REG_EXPAND_SZ = _EXPAND_SZ

    def __init__(self, trees=None, *, unreadable=(), close_fails=False):
        self.trees = {tree: list(entries) for tree, entries in (trees or {}).items()}
        self.names = {tree: {e[0]: e for e in entries if isinstance(e, tuple)}
                      for tree, entries in self.trees.items()}
        self.unreadable = set(unreadable)
        self.close_fails = close_fails
        self.handles = {}
        self.serial = 0
        self.enumerated = []
        self.calls = 0

    def _handle(self, kind, payload):
        self.serial += 1
        self.handles[self.serial] = (kind, payload)
        return self.serial

    def OpenKey(self, key, sub_key, reserved, access):  # noqa: N802 - winreg name
        self.calls += 1
        if reserved != 0:
            raise OSError("reserved must be 0")
        if key in (self.HKEY_CURRENT_USER, self.HKEY_LOCAL_MACHINE):
            view = access & ~self.KEY_READ
            if (sub_key != _UNINSTALL or access != self.KEY_READ | view
                    or view not in (self.KEY_WOW64_64KEY, self.KEY_WOW64_32KEY)):
                raise PermissionError("only the uninstall key, read-only, one view")
            tree = (key, view)
            if tree in self.unreadable:
                raise PermissionError("this tree cannot be read")
            if tree not in self.trees:
                raise FileNotFoundError("no uninstall key in this tree")
            return self._handle("tree", (tree, access))
        kind, payload = self.handles.get(key, (None, None))
        if kind != "tree":
            raise OSError("an entry is opened only below an open tree")
        tree, tree_access = payload
        if access != tree_access:
            raise PermissionError("an entry is opened with its tree's access")
        entry = self.names[tree].get(sub_key)
        if entry is None:
            raise FileNotFoundError(sub_key)
        if isinstance(entry[1], BaseException):
            raise entry[1]
        return self._handle("entry", entry[1])

    def EnumKey(self, key, index):  # noqa: N802 - winreg name
        self.calls += 1
        kind, payload = self.handles.get(key, (None, None))
        if kind != "tree":
            raise OSError("not an open tree")
        self.enumerated.append((payload[0], index))
        entries = self.trees[payload[0]]
        if index >= len(entries):
            raise OSError(259, "No more data is available")
        if isinstance(entries[index], BaseException):
            raise entries[index]
        return entries[index][0]

    def QueryValueEx(self, key, name):  # noqa: N802 - winreg name
        self.calls += 1
        kind, values = self.handles.get(key, (None, None))
        if kind != "entry":
            raise OSError("not an open entry")
        if name not in values:
            raise FileNotFoundError(name)
        if isinstance(values[name], BaseException):
            raise values[name]
        return values[name]

    def CloseKey(self, key):  # noqa: N802 - winreg name
        if self.handles.pop(key, None) is None:
            raise AssertionError("a key closed twice or never opened")
        if self.close_fails:
            raise OSError("closing failed")


def _entry(name, display="Trader Workstation", location=None, *,
           display_kind=_SZ, location_kind=_SZ, extra=None):
    values = {}
    if display is not None:
        values["DisplayName"] = (display, display_kind)
    if location is not None:
        text = str(location) if isinstance(location, Path) else location
        values["InstallLocation"] = (text, location_kind)
    values.update(extra or {})
    return (name, values)


def _known_folders(**folders):
    """A known-folder function answering the test's own ids from scratch
    folders; it records every id asked."""
    table = {_FOLDER_IDS[slot].casefold(): (str(path) if isinstance(path, Path) else path)
             for slot, path in folders.items()}
    asked = []

    def known_folder(folder_id):
        asked.append(folder_id)
        return table.get(str(folder_id).casefold())
    known_folder.asked = asked
    return known_folder


_UNSET = object()


class _FakeShell:
    """A ctypes stand-in for the Windows folder call (F13).

    The module builds its call from the real ctypes types kept here; the two
    Windows functions are fakes.  SHGetKnownFolderPath answers ``status`` and,
    when ``text`` is given, hands out a buffer holding it; CoTaskMemFree
    records what it frees.  ``wstring_at`` reads only the buffer handed out
    and records every read, so a read at any other address is seen, not made.
    """

    def __init__(self, status, text=None, *, load_error=None):
        import ctypes
        self.real = ctypes
        self.c_ubyte, self.c_void_p, self.c_uint32 = ctypes.c_ubyte, ctypes.c_void_p, ctypes.c_uint32
        self.c_long, self.POINTER = ctypes.c_long, ctypes.POINTER
        self.status, self.load_error = status, load_error
        self.buffer = None if text is None else ctypes.create_unicode_buffer(text)
        self.address = None if self.buffer is None else ctypes.addressof(self.buffer)
        self.functions, self.calls, self.reads, self.freed = {}, [], [], []

    @staticmethod
    def byref(value):
        return value                    # the fake call sets the pointer itself

    def WinDLL(self, name):  # noqa: N802 - ctypes name
        if self.load_error is not None:
            raise self.load_error
        shell = self

        class Function:
            argtypes = restype = _UNSET

            def __init__(self, run):
                self.run = run

            def __call__(self, *args):
                return self.run(*args)

        def query(guid, flags, token, out):
            shell.calls.append((bytes(guid), flags, token))
            if shell.address is not None:
                out.value = shell.address
            return shell.status

        def free(pointer):
            shell.freed.append(pointer.value)

        if name == "shell32":
            self.functions["query"] = Function(query)
            return types.SimpleNamespace(SHGetKnownFolderPath=self.functions["query"])
        if name == "ole32":
            self.functions["free"] = Function(free)
            return types.SimpleNamespace(CoTaskMemFree=self.functions["free"])
        raise OSError(f"no library {name}")

    def wstring_at(self, address):
        self.reads.append(address)
        if address is None or address != self.address:
            return "memory this fake never handed out"
        return self.real.wstring_at(address)


class _LinkEntry:
    """A folder-listing entry for a file symbolic link, answering as CPython's
    os.DirEntry does on Windows: neither a file nor a folder unless followed,
    its own attributes carry the reparse bit, and following it asks the
    target, which raises FileNotFoundError when the target is gone and, with
    ``refused``, PermissionError as for a target this account may not open
    (is_dir and is_file turn only the first into False).  Opening ``path``
    reads what opening the link would: the check puts a copy of the target's
    bytes there, or nothing for a broken link."""

    def __init__(self, folder, name, target, *, refused=False):
        self.name, self.target, self.refused = name, Path(target), refused
        self.path = str(Path(folder) / name)

    def _follow(self):
        if self.refused:
            raise PermissionError(13, "Access is denied", str(self.target))
        return self.target

    def is_symlink(self):
        return True

    def is_dir(self, *, follow_symlinks=True):
        return follow_symlinks and self._follow().is_dir()

    def is_file(self, *, follow_symlinks=True):
        return follow_symlinks and self._follow().is_file()

    def stat(self, *, follow_symlinks=True):
        if follow_symlinks:
            return os.stat(self._follow())
        return types.SimpleNamespace(st_file_attributes=0x20 | 0x400)


class _Listing:
    """Swap os.scandir for one check: each named folder lists the given items
    in order, and an exception item is raised where it stands, as a listing
    that fails part-way does; every other folder is listed for real."""

    def __init__(self, listings):
        self.listings = {_key(folder): list(items) for folder, items in listings.items()}

    def __enter__(self):
        self.real = real = os.scandir
        listings = self.listings

        class Listing:
            def __init__(self, items):
                self.items = items

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def __iter__(self):
                for item in self.items:
                    if isinstance(item, BaseException):
                        raise item
                    yield item

        def scandir(path="."):
            items = listings.get(_key(path))
            return real(path) if items is None else Listing(items)
        os.scandir = scandir
        return self

    def __exit__(self, *exc):
        os.scandir = self.real
        return False


def _ansi_z(text):
    return text.encode("mbcs", "replace") + b"\0"


def _wide_z(text):
    return text.encode("utf-16-le") + b"\0\0"


def _link_info(local, suffix="", *, wide_local=None, wide_suffix="", wide_blob=None,
               header_field=None, size_delta=0, local_at=None, suffix_at=None,
               info_flags=0x01):
    """A LinkInfo with a local base path (MS-SHLLINK 2.3): seven header
    fields, two Unicode offsets when a Unicode path is given, a VolumeID, then
    the NUL-terminated paths.  The keywords damage single fields."""
    wide = wide_blob if wide_blob is not None else (
        None if wide_local is None else _wide_z(wide_local))
    head = 0x1C if wide is None else 0x24
    volume = struct.pack("<4I", 0x11, 3, 0x2A2A2A2A, 0x10) + b"\0"
    ansi_local, ansi_suffix = _ansi_z(local), _ansi_z(suffix)
    local_offset = head + len(volume)
    suffix_offset = local_offset + len(ansi_local)
    tail = volume + ansi_local + ansi_suffix
    extra = b""
    if wide is not None:
        wide_offset = suffix_offset + len(ansi_suffix)
        extra = struct.pack("<2I", wide_offset, wide_offset + len(wide))
        tail += wide + _wide_z(wide_suffix)
    size = head + len(tail)
    fields = struct.pack(
        "<7I", size + size_delta, head if header_field is None else header_field,
        info_flags, head, local_offset if local_at is None else local_at, 0,
        suffix_offset if suffix_at is None else suffix_at)
    return fields + extra + tail


def _env_block(wide="", ansi="", *, wide_raw=None, ansi_raw=None):
    """An EnvironmentVariableDataBlock (MS-SHLLINK 2.5.4): size 0x314, its
    signature, 260 ANSI bytes, then 520 Unicode bytes, each NUL-padded."""
    ansi_part = ansi_raw if ansi_raw is not None else ansi.encode("mbcs").ljust(260, b"\0")
    wide_part = wide_raw if wide_raw is not None else wide.encode("utf-16-le").ljust(520, b"\0")
    return struct.pack("<2I", 0x314, 0xA0000001) + ansi_part + wide_part


def _shortcut(*, link_info=None, id_list=None, strings=(), wide_strings=True,
              blocks=(), terminal=True):
    """A shell-link file (MS-SHLLINK 2.1-2.5) built from its parts."""
    flags, body = 0, b""
    if id_list is not None:
        flags |= 0x01
        body += struct.pack("<H", len(id_list)) + id_list
    if link_info is not None:
        flags |= 0x02
        body += link_info
    if wide_strings:
        flags |= 0x80
    for bit, text in zip((0x04, 0x08, 0x10, 0x20, 0x40), strings):
        flags |= bit
        data = text.encode("utf-16-le" if wide_strings else "mbcs")
        body += struct.pack("<H", len(data) // (2 if wide_strings else 1)) + data
    body += b"".join(blocks) + (b"\0\0\0\0" if terminal else b"")
    header = (struct.pack("<I", 0x4C) + _LNK_CLSID + struct.pack("<2I", flags, 0x20)
              + bytes(24) + struct.pack("<IiIH", 0, 0, 1, 0) + bytes(10))
    return header + body


def _lnk_to(target):
    """The shortcut an installer writes: LinkInfo naming the target as an
    ANSI and as a Unicode local path."""
    text = str(target)
    return _shortcut(link_info=_link_info(text, wide_local=text))


def _shortcuts_in(td, s, files, *, slot="common_menu", seams=None):
    """discover_shortcuts() with one known folder holding {"a/b.lnk": bytes}."""
    folder = s.fresh()
    for relative, data in files.items():
        s.file(data, folder.name, *relative.split("/"))
    with _Seams(td, KNOWN_FOLDER_PATH=_known_folders(**{slot: folder}), **(seams or {})):
        return td.discover_shortcuts()


def _set_attributes(path, *, add=0, remove=0):
    import ctypes
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetFileAttributesW.argtypes = (ctypes.c_wchar_p,)
    kernel.GetFileAttributesW.restype = ctypes.c_uint32
    kernel.SetFileAttributesW.argtypes = (ctypes.c_wchar_p, ctypes.c_uint32)
    kernel.SetFileAttributesW.restype = ctypes.c_int
    current = kernel.GetFileAttributesW(str(path))
    _need(current != 0xFFFFFFFF, f"harness: cannot read attributes of {path.name}")
    _need(kernel.SetFileAttributesW(str(path), (current | add) & ~remove),
          f"harness: cannot set attributes of {path.name}")


def f7_local_programs_root():
    import tws_discovery as td
    standard = _REAL.get("candidate_roots", td.candidate_roots)
    with _Scratch("twsdisc_f7_") as s:
        pf, pf86, local, home = (s.folder(name) for name in ("pf", "pf86", "local", "home"))
        os.environ.update({"ProgramFiles": str(pf), "ProgramFiles(x86)": str(pf86),
                           "LOCALAPPDATA": str(local), "USERPROFILE": str(home)})
        expected = []
        for base in (pf, pf86):
            expected += [base / "Jts", base / "Interactive Brokers" / "TWS",
                         base / "Trader Workstation"]
        expected += [local / "Jts", local / "Programs" / "Jts",
                     local / "Programs" / "Trader Workstation",
                     local / "Interactive Brokers" / "TWS", local / "Programs" / "tws",
                     home / "Jts"]
        roots = standard()          # paths only: nothing is opened
        _need([_key(p) for p in roots[1:]] == [_key(p) for p in expected],
              f"standard folders are not the old list plus LOCALAPPDATA Programs/tws "
              f"after the other LOCALAPPDATA folders: {[str(p) for p in roots]}")
        if os.name == "nt":
            first = roots[0] if roots else Path()
            _need(first.name == "Jts" and first.parent == Path(first.anchor) != Path(),
                  f"the first standard folder is not Jts at a drive root: {first}")
        want = local / "Programs" / "tws"
        exe = s.exe("local", "Programs", "tws", "tws.exe")
        _need(_same(td.discover_tws([want]), [exe]),
              "tws.exe directly in Programs/tws is not found")
        path, source = td.resolve_tws(roots=[want], state_path=s.path("none.json"))
        _need(source == "discovered" and _same([path], [exe]),
              f"resolve_tws gave {path!r} {source!r}")
    return ("the standard folders are the old list with LOCALAPPDATA Programs/tws after "
            "the other LOCALAPPDATA folders; its tws.exe is found")


def f8_registry_source():
    import tws_discovery as td
    trees = ((_HKCU, _KEY_64), (_HKCU, _KEY_32), (_HKLM, _KEY_64), (_HKLM, _KEY_32))
    with _Scratch("twsdisc_f8_") as s:
        state = s.path("none.json")

        def scan(fake):
            with _Seams(td, REGISTRY_API=fake):
                found = td.discover_registry()
            _need(not fake.handles, f"registry keys left open: {sorted(fake.handles)}")
            return found

        for number, tree in enumerate(trees):
            exe = s.exe(f"alone{number}", "tws.exe")
            _need(_same(scan(_FakeRegistry({tree: [_entry("TWS", location=exe.parent)]})), [exe]),
                  f"tree {tree} alone: tws.exe not found")
            with _Seams(td, REGISTRY_API=_FakeRegistry(
                    {tree: [_entry("TWS", location=exe.parent)]})):
                path, source = td.resolve_tws(state_path=state)
            _need(source == "registry" and _same([path], [exe]),
                  f"tree {tree} alone: resolve_tws gave {path!r} {source!r}")

        ordered = [s.exe(name, "tws.exe") for name in ("cu64", "cu32", "lm64", "lm32")]
        fake = _FakeRegistry({tree: [_entry(f"order{number}", location=exe.parent)]
                              for number, (tree, exe) in reversed(list(enumerate(
                                  zip(trees, ordered))))})
        _need(_same(scan(fake), ordered),
              "trees not read this account's then machine-wide, 64-bit then 32-bit view")

        target = s.exe("gate", "tws.exe")
        shallow = s.exe("shallow", "Jts", "tws.exe")
        os.environ[_ENV_NAME] = str(s.root)
        expandable = _VAR + os.sep + "gate"
        forms = (
            ("another program", _entry("x", "Other Program", target.parent), []),
            ("the name later in the text", _entry("x", "IB Trader Workstation", target.parent), []),
            ("spaces and capitals", _entry("x", "  TRADER WORKSTATION 10.37", target.parent),
             [target]),
            ("a quoted folder", _entry("x", location='"' + str(target.parent) + os.sep + '"'),
             [target]),
            ("an expandable folder",
             _entry("x", location=expandable, location_kind=_EXPAND_SZ), [target]),
            ("plain text is not expanded", _entry("x", location=expandable), []),
            ("the shallow Jts candidate", _entry("x", location=shallow.parent.parent),
             [shallow]),
        )
        for label, entry, want in forms:
            got = scan(_FakeRegistry({(_HKLM, _KEY_32): [entry]}))
            _need(_same(got, want), f"{label}: found {got}")

        good = s.exe("good", "tws.exe")
        noise = s.exe("noise", "tws.exe")     # reachable only through entries to skip
        s.exe("rel_inst", "tws.exe")
        os.chdir(s.root)                      # where a relative folder would resolve
        skipped = [
            ("an entry that cannot be opened", PermissionError("denied")),
            _entry("no display name", None, noise.parent),
            _entry("a number for a name", 7, noise.parent, display_kind=_DWORD),
            _entry("a number typed as text", 7, noise.parent),
            _entry("text typed as binary", "Trader Workstation", noise.parent,
                   display_kind=_BINARY),
            _entry("no folder"),
            _entry("bytes for a folder", location=os.fsencode(str(noise.parent))),
            _entry("a list for a folder", location=[str(noise.parent)],
                   location_kind=_MULTI_SZ),
            _entry("an unreadable folder", extra={"InstallLocation": PermissionError("denied")}),
            _entry("a relative folder", location="rel_inst"),
            _entry("the good entry", location=good.parent),
        ]
        got = scan(_FakeRegistry({(_HKCU, _KEY_64): skipped}))
        _need(_same(got, [good]), f"a bad entry stopped the search or was used: {got}")

        fake = _FakeRegistry(
            {(_HKCU, _KEY_64): [_entry("one", location=s.path("nothing")),
                                PermissionError("enumeration denied"),
                                _entry("after the failure", location=noise.parent)],
             (_HKCU, _KEY_32): [_entry("next tree", location=good.parent)]},
            unreadable={(_HKLM, _KEY_64)}, close_fails=True)
        got = scan(fake)
        _need(_same(got, [good]) and ((_HKCU, _KEY_64), 2) not in fake.enumerated,
              f"a failing enumeration, tree or close was not skipped: {got}")

        class Broken(_FakeRegistry):
            def OpenKey(self, *args):  # noqa: N802 - winreg name
                raise RuntimeError("an unexpected failure inside the registry layer")
        with _Seams(td, REGISTRY_API=Broken()):
            _need(td.discover_registry() == [], "an unexpected registry failure was not contained")

        empty = s.folder("empty")
        s.folder("dirnamed", "tws.exe")
        wrong = s.exe("wrong", "notws.exe")
        for label, location in (("a folder without tws.exe", empty),
                                ("a missing folder", s.path("missing")),
                                ("a folder whose tws.exe is a folder", s.path("dirnamed")),
                                ("a file not named tws.exe", wrong)):
            got = scan(_FakeRegistry({(_HKCU, _KEY_64): [_entry("x", location=location)]}))
            _need(got == [], f"{label}: found {got}")
    return ("each tree and view alone and in order; display-name gate; quotes and "
            "REG_EXPAND_SZ; bad entries, unreadable keys, wrong types and bad targets "
            "skipped; every key closed")


def f9_shortcut_source():
    import tws_discovery as td
    if os.name != "nt":
        return "skipped off Windows (shortcut files are a Windows format)"
    with _Scratch("twsdisc_f9_") as s:
        state = s.path("none.json")
        os.environ[_ENV_NAME] = str(s.root)
        for number, slot in enumerate(_FOLDER_IDS):
            exe = s.exe(f"alone{number}", "tws.exe")
            folder = s.fresh("slot")
            s.file(_lnk_to(exe), folder.name, "Trader Workstation.lnk")
            with _Seams(td, KNOWN_FOLDER_PATH=_known_folders(**{slot: folder})):
                found = td.discover_shortcuts()
                path, source = td.resolve_tws(state_path=state)
            _need(_same(found, [exe]), f"{slot} alone: found {found}")
            _need(source == "shortcut" and _same([path], [exe]),
                  f"{slot} alone: resolve_tws gave {path!r} {source!r}")

        exe = s.exe("forms", "tws.exe")
        text = str(exe)
        deep = s.exe("base", "deep", "tws.exe")
        decoy = s.exe("decoy", "tws.exe")
        odd = s.exe("dirAĀ", "tws.exe")  # UTF-16 41 00 00 01: a 00 00 pair at an odd offset
        env_exe = s.exe("envw", "tws.exe")
        s.exe("lit", "tws.exe")
        env_text = _VAR + os.sep + "envw" + os.sep + "tws.exe"
        strings = ("name", "rel" + os.sep + "path", "work", "-arg", "icon,0")
        full = _shortcut(id_list=b"\x04\x00ab\x00\x00",
                         link_info=_link_info(text, wide_local=text), strings=strings,
                         blocks=(struct.pack("<2I", 8, 0xA0000099),
                                 struct.pack("<4I", 0x10, 0xA0000005, 0x26, 0x14),
                                 _env_block(wide=env_text)))
        forms = [
            ("Unicode LinkInfo before an ANSI decoy",
             _shortcut(link_info=_link_info(str(decoy), wide_local=text)), [exe]),
            ("a Unicode path with a 00 00 pair at an odd offset",
             _shortcut(link_info=_link_info("", wide_local=str(odd))), [odd]),
            ("the environment block, Unicode, expanded",
             _shortcut(blocks=(_env_block(wide=env_text),)), [env_exe]),
            ("an id list, LinkInfo, five Unicode strings and three blocks", full,
             [exe, env_exe]),
            ("an id list then the environment block",
             _shortcut(id_list=b"\x04\x00ab\x00\x00", blocks=(_env_block(wide=env_text),)),
             [env_exe]),
            ("an id list and ANSI strings before the environment block",
             _shortcut(id_list=b"\x04\x00ab\x00\x00", strings=strings, wide_strings=False,
                       blocks=(_env_block(wide=env_text),)), [env_exe]),
            ("the environment block as the last bytes",
             _shortcut(blocks=(_env_block(wide=env_text),), terminal=False), [env_exe]),
            ("a LinkInfo path is not expanded",
             _shortcut(link_info=_link_info(
                 "", wide_local=_VAR + os.sep + "lit" + os.sep + "tws.exe")), []),
        ]
        if _ansi_safe(s.root):
            forms += [
                ("ANSI LinkInfo", _shortcut(link_info=_link_info(text)), [exe]),
                ("an ANSI base path and its suffix",
                 _shortcut(link_info=_link_info(str(deep.parent.parent) + os.sep,
                                                "deep" + os.sep + "tws.exe")), [deep]),
                ("the environment block, ANSI only",
                 _shortcut(blocks=(_env_block(ansi=text),)), [exe]),
            ]
        for label, data, want in forms:
            got = _shortcuts_in(td, s, {"Trader Workstation.lnk": data})
            _need(_same(got, want), f"{label}: found {got}")
        _need(td._shortcut_targets(full) == [text, os.path.expandvars(env_text)],
              "the full shortcut did not give exactly its two targets")

        for name, want in (("trader workstation 10.30.LNK", [exe]),
                           ("My Trader Workstation.lnk", []),
                           ("Trader Workstation.url", [])):
            got = _shortcuts_in(td, s, {name: _lnk_to(exe)})
            _need(_same(got, want), f"a file named {name!r}: found {got}")
        got = _shortcuts_in(td, s, {"Trader Workstation.lnk/Trader Workstation.lnk":
                                    _lnk_to(exe)}, slot="own_desktop")
        _need(got == [], f"a folder named like a shortcut on a Desktop: found {got}")

        import _winapi
        outside = s.fresh("outside")
        s.file(_lnk_to(exe), outside.name, "Trader Workstation.lnk")
        menu = s.fresh("menu")
        try:
            _winapi.CreateJunction(str(outside), str(menu / "Linked"))
            s.junctions.append(menu / "Linked")
            joined = True
        except OSError:
            joined = False
        if joined:
            with _Seams(td, KNOWN_FOLDER_PATH=_known_folders(own_menu=menu)):
                got = td.discover_shortcuts()
            _need(got == [], f"a junction in the Start Menu was followed: {got}")
        got = _shortcuts_in(td, s, {"Plain/Trader Workstation.lnk": _lnk_to(exe)},
                            slot="own_menu")
        _need(_same(got, [exe]), f"a plain Start Menu subfolder was not searched: {got}")
        got = _shortcuts_in(td, s, {"A/Trader Workstation.lnk": _lnk_to(exe),
                                    "A/Deep/Trader Workstation.lnk": _lnk_to(deep),
                                    "B/Trader Workstation.lnk": _lnk_to(decoy)},
                            slot="own_menu")
        _need(_same(got, [exe, decoy, deep]),
              f"the Start Menu was not searched level by level in name order: {got}")

        desk = s.fresh("desk")
        offline = s.file(_lnk_to(exe), desk.name, "Trader Workstation.lnk")
        _set_attributes(offline, add=0x1000)
        with _Seams(td, KNOWN_FOLDER_PATH=_known_folders(own_desktop=desk)):
            got = td.discover_shortcuts()
        _set_attributes(offline, remove=0x1000)
        _need(got == [], f"a shortcut marked offline (cloud only) was read: {got}")
        with _Seams(td, KNOWN_FOLDER_PATH=_known_folders(own_desktop=desk)):
            got = td.discover_shortcuts()
        _need(_same(got, [exe]), f"the same shortcut, back online, was missed: {got}")

        desk = s.fresh("desk")
        readonly = s.file(_lnk_to(exe), desk.name, "Trader Workstation.lnk")
        menu = s.fresh("menu")
        sub = s.folder(menu.name, "Sub")
        s.file(_lnk_to(deep), menu.name, "Sub", "Trader Workstation.lnk")
        _set_attributes(readonly, add=0x1)
        try:
            _set_attributes(sub, add=0x1)
            try:
                with _Seams(td, KNOWN_FOLDER_PATH=_known_folders(own_desktop=desk,
                                                                 own_menu=menu)):
                    got = td.discover_shortcuts()
            finally:
                _set_attributes(sub, remove=0x1)
        finally:
            _set_attributes(readonly, remove=0x1)
        _need(_same(got, [exe, deep]),
              f"a read-only shortcut or a read-only Start Menu subfolder was skipped: {got}")

        import msvcrt
        desk = s.fresh("desk")
        locked = s.file(_lnk_to(decoy), desk.name, "Trader Workstation 1.lnk")
        s.file(_lnk_to(exe), desk.name, "Trader Workstation 2.lnk")
        size = locked.stat().st_size
        with open(locked, "rb+") as holder:
            msvcrt.locking(holder.fileno(), msvcrt.LK_NBLCK, size)
            try:
                with _Seams(td, KNOWN_FOLDER_PATH=_known_folders(own_desktop=desk)):
                    got = td.discover_shortcuts()
            finally:
                holder.seek(0)
                msvcrt.locking(holder.fileno(), msvcrt.LK_UNLCK, size)
        _need(_same(got, [exe]), f"an unreadable shortcut stopped the search or was read: {got}")

        linked = "not tried"
        desk = s.fresh("desk")
        menu = s.fresh("menu")
        real = s.file(_lnk_to(decoy), "elsewhere", "Trader Workstation.lnk")
        s.file(_lnk_to(exe), menu.name, "Trader Workstation.lnk")
        try:
            os.symlink(real, desk / "Trader Workstation 1.lnk")
            os.symlink(s.path("elsewhere", "gone.lnk"), desk / "Trader Workstation 2.lnk")
            linked = "checked"
        except OSError:
            linked = "skipped (no symbolic-link privilege)"
        if linked == "checked":
            with _Seams(td, KNOWN_FOLDER_PATH=_known_folders(own_desktop=desk,
                                                             common_menu=menu)):
                got = td.discover_shortcuts()
            _need(_same(got, [exe]), f"a symbolic link named like a shortcut was read, or "
                                     f"a broken one stopped the search: {got}")
        # The same rule where symbolic links cannot be made (that needs a
        # privilege): a stand-in listing with Windows' answers for three file
        # links (to a shortcut, to nothing, to a place this account may not
        # open), beside a listing that fails part-way and is dropped whole.
        desk, failing = s.fresh("desk"), s.fresh("desk")
        s.file(real.read_bytes(), desk.name, "Trader Workstation 1.lnk")
        s.file(_lnk_to(decoy), failing.name, "Trader Workstation.lnk")
        with os.scandir(failing) as listing:
            before = list(listing)
        listings = {desk: [_LinkEntry(desk, "Trader Workstation 1.lnk", real),
                           _LinkEntry(desk, "Trader Workstation 2.lnk",
                                      s.path("elsewhere", "gone.lnk")),
                           _LinkEntry(desk, "Trader Workstation 3.lnk",
                                      s.path("elsewhere"), refused=True)],
                    failing: before + [OSError("the listing failed part-way")]}
        with _Listing(listings), _Seams(td, KNOWN_FOLDER_PATH=_known_folders(
                own_desktop=desk, public_desktop=failing, common_menu=menu)):
            got = td.discover_shortcuts()
        _need(_same(got, [exe]), f"a stand-in file link was read or followed, or a listing "
                                 f"that failed part-way was used: {got}")

        good_menu = s.fresh("menu")
        s.file(_lnk_to(exe), good_menu.name, "Trader Workstation.lnk")
        s.file(_lnk_to(decoy), "Desk", "Trader Workstation.lnk")
        os.chdir(s.root)                      # where a relative folder would resolve
        answers = _known_folders(own_desktop=None, public_desktop="Desk",
                                 own_menu=b"bytes", common_menu=good_menu)
        with _Seams(td, KNOWN_FOLDER_PATH=answers):
            got = td.discover_shortcuts()
        _need(_same(got, [exe]), f"a missing, relative or non-text folder answer was used: {got}")
        with _Seams(td, KNOWN_FOLDER_PATH=_known_folders(own_desktop=s.path("absent"),
                                                         common_menu=good_menu)):
            got = td.discover_shortcuts()
        _need(_same(got, [exe]), f"a known folder missing on disk stopped the search: {got}")

        def failing(folder_id):
            raise RuntimeError("the folder call failed")
        with _Seams(td, KNOWN_FOLDER_PATH=failing):
            _need(td.discover_shortcuts() == [], "a failing folder call was not contained")

        share = os.sep * 2 + "server" + os.sep + "share" + os.sep + "Desk"
        table = _known_folders(own_desktop=share, public_desktop=s.path("p"),
                               own_menu=s.path("m"), common_menu=s.path("c"))
        with _Seams(td, KNOWN_FOLDER_PATH=table):
            listed = td._shortcut_folders()
        _need(listed == [(Path(share), 0), (s.path("p"), 0), (s.path("m"), 2),
                         (s.path("c"), 2)], f"folders and depths: {listed}")
    return ("each known folder alone; LinkInfo (ANSI, Unicode first), environment block, "
            "id list, strings and blocks; name rules; Start Menu level by level; "
            "junction, offline file and locked file skipped, read-only file and folder "
            "read; symbolic links " + linked + "; stand-in file links (broken, refused) "
            "neither read nor followed and a listing failing part-way dropped; bad or "
            "missing folder answers ignored")


def f10_damaged_shortcuts():
    import tws_discovery as td
    if os.name != "nt":
        return "skipped off Windows (shortcut files are a Windows format)"
    with _Scratch("twsdisc_f10_") as s:
        exe = s.exe("ok", "tws.exe")
        decoy = s.exe("decoy", "tws.exe")
        good = _lnk_to(exe)
        # The byte layouts are only parsed, so plain ASCII text stands in for the
        # paths and no case depends on this machine's ANSI code page.
        text, env_text, other = (name + os.sep + "tws.exe" for name in ("ok", "env", "other"))
        info = _link_info(text, wide_local=text)
        env = _env_block(wide=env_text)

        def made(flags, body=b""):
            """The good header with other flags, then the given body."""
            return good[:0x14] + struct.pack("<I", flags) + good[0x18:0x4C] + body

        unended = _link_info(text)[:-2]       # the paths lose their end marks
        unended = struct.pack("<I", len(unended)) + unended[4:]
        # A LinkInfo that is only its 0x1C-byte header and the last bytes of the
        # file; its path offsets point at two of its own fields, "ab" and "cd".
        header_only = struct.pack("<7I", 0x1C, 0x1C, 0x01, int.from_bytes(b"ab\0\0", "little"),
                                  0x0C, int.from_bytes(b"cd\0\0", "little"), 0x14)
        cases = (
            ("an empty file", b"", []),
            ("a cut header", good[:0x4B], []),
            ("a header and nothing else", made(0), []),
            ("a wrong header size", struct.pack("<I", 0x4D) + good[4:], []),
            ("a wrong class id", good[:4] + bytes(16) + good[20:], []),
            ("an id list longer than the file", made(0x01, struct.pack("<H", 0x200) + b"ab"), []),
            ("an id list size cut in half", made(0x01, b"\x05"), []),
            ("a cut LinkInfo header", made(0x02, info[:0x1B]), []),
            ("a LinkInfo longer than the file", made(0x02, _link_info(text, size_delta=64)), []),
            ("a LinkInfo that is only its header, ending the file",
             made(0x02, header_only), ["abcd"]),
            ("a LinkInfo header one below 0x1C",
             made(0x02, _link_info(text, header_field=0x1B)), []),
            ("a LinkInfo header past its LinkInfo",
             made(0x02, _link_info(text, header_field=0x200)), []),
            ("a LinkInfo header of 0x23 has no Unicode offsets",
             made(0x02, _link_info(text, wide_local=other, header_field=0x23)), [text]),
            ("a LinkInfo without its local-path flag",
             made(0x02, _link_info(text, info_flags=0x02)), []),
            ("a path offset past the LinkInfo",
             made(0x02, _link_info(text, local_at=0x400) + env), [env_text]),
            ("a suffix offset past the LinkInfo gives the base path alone",
             made(0x02, _link_info(text, suffix_at=0x400)), [text]),
            ("paths without end marks", made(0x02, unended), []),
            ("broken UTF-16 falls back to the ANSI path",
             made(0x02, _link_info(text, wide_blob=b"\x00\xd8\x00\x00")), [text]),
            ("a string count cut off", made(0x02 | 0x04, info), [text]),
            ("one stray byte where a string count belongs",
             made(0x02 | 0x04, info + b"\x05"), [text]),
            ("a string longer than the file",
             made(0x02 | 0x04, info + struct.pack("<H", 0x7FFF) + b"ab"), [text]),
            ("a block size of zero", made(0, struct.pack("<2I", 0, 0xA0000001) + env), []),
            ("a block size of seven",
             made(0, struct.pack("<2I", 7, 0xA0000099)[:7] + env), []),
            ("an empty block before the environment block",
             made(0, struct.pack("<2I", 8, 0xA0000099) + env), [env_text]),
            ("a block longer than the file", made(0, env[:-1]), []),
            ("seven stray bytes after the last block", made(0, env + b"\x01" * 7), [env_text]),
            ("environment text without end marks",
             made(0, _env_block(ansi_raw=b"A" * 260, wide_raw=b"A\x00" * 260)), []),
            ("ANSI environment text filling its area",
             made(0, _env_block(ansi_raw=b"A" * 260, wide_raw=bytes(520))), []),
            ("ANSI environment text ending on its area's last byte",
             made(0, _env_block(ansi_raw=b"A" * 259 + b"\0", wide_raw=bytes(520))), ["A" * 259]),
            ("the environment signature at another size",
             made(0, struct.pack("<I", 0x310) + env[4:0x310]), []),
            ("another block of the environment size",
             made(0, struct.pack("<2I", 0x314, 0xA0000002) + env[8:]), []),
        )
        for label, data, want in cases:
            try:
                got = td._shortcut_targets(data)
            except Exception as exc:  # noqa: BLE001 - the check is that nothing escapes
                raise _Fail(f"{label}: raised {exc!r}") from exc
            _need(got == want, f"{label}: gave {got!r}, expected {want!r}")

        strings = (   # the string reader alone: (data, start, end, Unicode), result
            ("ANSI text and its end mark", (b"ab\0", 0, 3, False), "ab"),
            ("an end mark just past the end", (b"ab\0", 0, 2, False), None),
            ("an empty string at the very start", (b"\0", 0, 1, False), ""),
            ("an empty Unicode string at the very start", (b"\0\0", 0, 2, True), ""),
            ("a Unicode 00 00 pair at an odd offset",
             ("AĀ".encode("utf-16-le") + b"\0\0", 0, 6, True), "AĀ"),
            ("no Unicode end mark after the start, one before it",
             (b"\0\0xy", 2, 4, True), None),
        )
        for label, args, want in strings:
            try:
                got = td._lnk_string(*args)
            except Exception as exc:  # noqa: BLE001 - the check is that nothing escapes
                raise _Fail(f"string reader, {label}: raised {exc!r}") from exc
            _need(got == want, f"string reader, {label}: gave {got!r}, expected {want!r}")

        desk = s.fresh("desk")
        oversized = _lnk_to(decoy)
        damaged = (b"", good[:100], struct.pack("<I", 0x4D) + good[4:],
                   made(0x01, struct.pack("<H", 0x200) + b"ab"),
                   made(0x02, _link_info(text, size_delta=64)),
                   made(0, struct.pack("<2I", 0, 0xA0000001) + env),
                   oversized + bytes(64 * 1024 + 1 - len(oversized)))
        for number, data in enumerate(damaged, 1):
            s.file(data, desk.name, f"Trader Workstation {number:02d}.lnk")
        menu = s.fresh("menu")
        s.file(good, menu.name, "Trader Workstation.lnk")
        with _Seams(td, KNOWN_FOLDER_PATH=_known_folders(own_desktop=desk, common_menu=menu)):
            got = td.discover_shortcuts()
        _need(_same(got, [exe]), f"damaged shortcuts stopped the search or gave a target: {got}")
    return (f"{len(cases)} damaged byte layouts give only what they still hold and never "
            f"raise; {len(strings)} string-reader bounds; {len(damaged)} damaged files on a "
            f"Desktop do not stop the search")


def f11_resolve_order():
    import tws_discovery as td
    if os.name != "nt":
        return "skipped off Windows (the shortcut source is part of the order)"
    with _Scratch("twsdisc_f11_") as s:
        std = s.exe("std", "tws.exe")
        reg = s.exe("reg", "tws.exe")
        reg2 = s.exe("reg2", "tws.exe")
        lnk = s.exe("lnk", "tws.exe")
        lnk2 = s.exe("lnk2", "tws.exe")
        manual = s.exe("manual", "tws.exe")
        state = s.path("state.json")
        td.remember_tws(manual, state_path=state)
        menu = s.folder("menu")
        s.file(_lnk_to(lnk), "menu", "Trader Workstation.lnk")
        two_menu = s.folder("twomenu")
        s.file(_lnk_to(lnk), "twomenu", "Trader Workstation 1.lnk")
        s.file(_lnk_to(lnk2), "twomenu", "Trader Workstation 2.lnk")

        def registry():
            return _FakeRegistry({(_HKLM, _KEY_64): [_entry("TWS", location=reg.parent)]})

        def resolve(state_file=state, **seams):
            with _Seams(td, **seams):
                return td.resolve_tws(state_path=state_file)

        steps = (
            ("all four answer", dict(candidate_roots=lambda: [std.parent],
                                     REGISTRY_API=registry(),
                                     KNOWN_FOLDER_PATH=_known_folders(common_menu=menu)),
             std, "discovered"),
            ("no standard folder", dict(REGISTRY_API=registry(),
                                        KNOWN_FOLDER_PATH=_known_folders(common_menu=menu)),
             reg, "registry"),
            ("no registry entry", dict(KNOWN_FOLDER_PATH=_known_folders(common_menu=menu)),
             lnk, "shortcut"),
            ("no shortcut", {}, manual, "remembered"),
            ("two registry entries: the first wins",
             dict(REGISTRY_API=_FakeRegistry({(_HKLM, _KEY_64): [
                 _entry("A", location=reg.parent), _entry("B", location=reg2.parent)]})),
             reg, "registry"),
            ("two shortcuts: the first wins",
             dict(KNOWN_FOLDER_PATH=_known_folders(common_menu=two_menu)), lnk, "shortcut"),
        )
        for label, seams, exe, source in steps:
            path, got = resolve(**seams)
            _need(got == source and _same([path], [exe]), f"{label}: {path!r} {got!r}")
        _need(resolve(state_file=s.path("none.json")) == (None, None),
              "nothing anywhere did not give (None, None)")

        empty = s.folder("empty")
        s.folder("direxe", "tws.exe")
        wrong = s.exe("wrong", "tws.txt")
        bad_registry = lambda: _FakeRegistry({(_HKCU, _KEY_64): [  # noqa: E731
            _entry("a", location=empty), _entry("b", location=s.path("gone")),
            _entry("c", location=wrong)]})
        bad_menu = s.folder("badmenu")
        for number, target in enumerate((s.path("gone", "tws.exe"), wrong,
                                         s.path("direxe", "tws.exe")), 1):
            s.file(_lnk_to(target), "badmenu", f"Trader Workstation {number}.lnk")
        path, got = resolve(REGISTRY_API=bad_registry(),
                            KNOWN_FOLDER_PATH=_known_folders(common_menu=menu))
        _need(got == "shortcut" and _same([path], [lnk]),
              f"bad registry targets did not fall through to the shortcut: {path!r} {got!r}")
        path, got = resolve(REGISTRY_API=bad_registry(),
                            KNOWN_FOLDER_PATH=_known_folders(common_menu=bad_menu))
        _need(got == "remembered" and _same([path], [manual]),
              f"bad shortcut targets did not fall through to the remembered choice: "
              f"{path!r} {got!r}")

        class Broken(_FakeRegistry):
            def OpenKey(self, *args):  # noqa: N802 - winreg name
                raise RuntimeError("an unexpected failure inside the registry layer")

        def failing(folder_id):
            raise RuntimeError("the folder call failed")
        path, got = resolve(REGISTRY_API=Broken(),
                            KNOWN_FOLDER_PATH=_known_folders(common_menu=menu))
        _need(got == "shortcut", f"a failing registry stopped the order: {path!r} {got!r}")
        path, got = resolve(KNOWN_FOLDER_PATH=failing)
        _need(got == "remembered", f"a failing folder call stopped the order: {path!r} {got!r}")

        watched = registry()
        asked = _known_folders(common_menu=menu)
        twice = s.exe("twice", "Jts", "tws.exe")
        with _Seams(td, REGISTRY_API=watched, KNOWN_FOLDER_PATH=asked):
            named_empty = td.resolve_tws(roots=[empty], state_path=state)
            named_std = td.resolve_tws(roots=[std.parent], state_path=state)
            listed = td.discover_tws()
            as_text = td.discover_tws(str(std.parent))
            both = td.discover_tws([twice.parent.parent, twice.parent])
        _need(named_empty[1] == "remembered" and _same([named_empty[0]], [manual]),
              f"explicit roots did not go straight to the remembered choice: {named_empty}")
        _need(named_std[1] == "discovered" and _same([named_std[0]], [std]),
              f"explicit roots did not find their tws.exe: {named_std}")
        _need(listed == [], f"discover_tws() searched more than the standard folders: {listed}")
        _need(_same(as_text, [std]), f"discover_tws() with one root as text gave {as_text}")
        _need(_same(both, [twice]), f"one tws.exe reached from two roots was not listed once: "
                                    f"{both}")
        _need(watched.calls == 0 and not asked.asked,
              "explicit roots or discover_tws() read the registry or the shortcut folders")
    return ("discovered > registry > shortcut > remembered > (None, None), the first hit "
            "of a source wins; bad targets and failing sources fall through; explicit "
            "roots and discover_tws() keep their meaning and never read the new sources")


def f12_caps():
    import tws_discovery as td
    stated = {"_MAX_UNINSTALL_ENTRIES": 2048, "_MAX_ROOTS": 64, "_MAX_MENU_DEPTH": 2,
              "_MAX_FOLDER_ENTRIES": 4096, "_MAX_SHORTCUT_FILES": 16,
              "_MAX_SHORTCUT_BYTES": 64 * 1024, "_MAX_TEXT_CHARS": 4096,
              "_REPARSE_POINT": 0x400, "_CLOUD_ONLY": 0x1000 | 0x40000 | 0x400000,
              "_UNINSTALL_KEY": _UNINSTALL}
    moved = {name: getattr(td, name, None) for name, value in stated.items()
             if getattr(td, name, None) != value}
    _need(not moved, f"stated caps or Windows facts moved: {moved}")
    with _Scratch("twsdisc_f12_") as s:
        exe = s.exe("cap", "tws.exe")
        tree = (_HKCU, _KEY_64)

        def entries(count, match):
            return [_entry(f"app{i:05d}", "Trader Workstation" if i == match else "Other",
                           exe.parent if i == match else None) for i in range(count)]

        def registry(fake):
            with _Seams(td, REGISTRY_API=fake):
                return td.discover_registry()

        _need(_same(registry(_FakeRegistry({tree: entries(2048, 2047)})), [exe]),
              "the last entry within the 2048-entry cap was not read")
        fake = _FakeRegistry({tree: entries(2049, 2048)})
        _need(registry(fake) == [], "an entry past the 2048-entry cap was read")
        _need(max(i for t, i in fake.enumerated if t == tree) == 2047,
              "enumeration went past the cap")
        fake = _FakeRegistry({tree: entries(3, 0)})
        _need(_same(registry(fake), [exe])
              and [i for t, i in fake.enumerated if t == tree] == [0, 1, 2, 3],
              "enumeration did not stop at the end of the list")
        decoys = [_entry(f"d{i:02d}", location=s.path(f"none{i:02d}")) for i in range(64)]
        real = _entry("real", location=exe.parent)
        _need(registry(_FakeRegistry({tree: decoys + [real]})) == [],
              "a folder past the 64-folder cap was searched")
        _need(_same(registry(_FakeRegistry({tree: decoys[:63] + [real]})), [exe]),
              "the 64th folder was not searched")
        if os.name != "nt":
            return "registry caps hold; shortcut caps skipped off Windows"

        lnk = _lnk_to(exe)
        gone = _lnk_to(s.path("gone", "tws.exe"))

        def menu(files, slot="own_menu", **seams):
            return _shortcuts_in(td, s, files, slot=slot, seams=seams)

        def numbered(count):
            return {f"Trader Workstation {i:02d}.lnk": gone for i in range(count)}

        def filler(count):
            return {f"a{i}.txt": b"" for i in range(1, count + 1)}

        _need(menu({**numbered(16), "Trader Workstation 16.lnk": lnk}) == [],
              "a 17th shortcut in one folder was read")
        _need(_same(menu({**numbered(15), "Trader Workstation 15.lnk": lnk}), [exe]),
              "the 16th shortcut in one folder was not read")
        _need(menu({**numbered(16), "Sub/Trader Workstation.lnk": lnk}) == [],
              "the search went on below a folder after 16 shortcuts")
        _need(_same(menu({**numbered(15), "Sub/Trader Workstation.lnk": lnk}), [exe]),
              "a subfolder was not searched while under the shortcut cap")
        _need(menu({**filler(4), "Trader Workstation.lnk": lnk}, "own_desktop",
                   _MAX_FOLDER_ENTRIES=4) == [],
              "a folder was listed past its entry budget")
        _need(_same(menu({**filler(3), "Trader Workstation.lnk": lnk}, "own_desktop",
                         _MAX_FOLDER_ENTRIES=4), [exe]),
              "an entry within the budget was not listed")
        _need(menu({"0sub/Trader Workstation.lnk": lnk, **filler(3)},
                   _MAX_FOLDER_ENTRIES=4) == [],
              "a subfolder was listed after the entry budget was spent")
        _need(_same(menu({"0sub/Trader Workstation.lnk": lnk, **filler(2)},
                         _MAX_FOLDER_ENTRIES=4), [exe]),
              "a subfolder within the entry budget was not listed")
        _need(_same(menu({"L1/L2/Trader Workstation.lnk": lnk}), [exe]),
              "a shortcut two levels down the Start Menu was missed")
        _need(menu({"L1/L2/L3/Trader Workstation.lnk": lnk}) == [],
              "the Start Menu was searched three levels down")
        _need(menu({"Sub/Trader Workstation.lnk": lnk}, "own_desktop") == [],
              "a Desktop subfolder was searched")
        exact = lnk + bytes(64 * 1024 - len(lnk))
        _need(_same(menu({"Trader Workstation.lnk": exact}), [exe]),
              "a shortcut of exactly 64 KiB was skipped")
        _need(menu({"Trader Workstation.lnk": exact + b"\0"}) == [],
              "a shortcut over 64 KiB was read")
        sizes = s.fresh("sizes")
        at_cap = s.file(exact, sizes.name, "at the cap.lnk")
        over_cap = s.file(exact + b"\0", sizes.name, "over the cap.lnk")
        _need(td._read_shortcut(at_cap) == exact and td._read_shortcut(over_cap) == b""
              and td._read_shortcut(sizes / "missing.lnk") == b"",
              "the shortcut reader did not give a file of up to 64 KiB whole and "
              "nothing for a larger or missing one")
        three = {"Trader Workstation 1.lnk": _lnk_to(s.path("gone1", "tws.exe")),
                 "Trader Workstation 2.lnk": _lnk_to(s.path("gone2", "tws.exe")),
                 "Trader Workstation 3.lnk": lnk}
        _need(menu(three, _MAX_ROOTS=2) == [], "a target past the roots cap was searched")
        _need(_same(menu(three, _MAX_ROOTS=3), [exe]),
              "a target within the roots cap was not searched")
    return ("2048 registry entries per tree and view, 64 folders; 16 shortcut files per "
            "known folder, 4096 directory entries, Start Menu depth 2 (Desktop 0), "
            "64 KiB per shortcut, 64 targets; stated values pinned")


def f13_sources_unavailable():
    import tws_discovery as td
    with _Scratch("twsdisc_f13_") as s:
        state = s.path("none.json")
        with _Seams(td):
            _need(td.discover_registry() == [] and td.discover_shortcuts() == []
                  and td._shortcut_folders() == [],
                  "a source without its provider found something")
            _need(td.resolve_tws(state_path=state) == (None, None),
                  "resolve_tws without any source did not give (None, None)")
        for label, stand_in in (("without ctypes", None),
                                ("ctypes without WinDLL", types.SimpleNamespace())):
            with _Seams(td, KNOWN_FOLDER_PATH=td._windows_known_folder, _ctypes=stand_in):
                _need(td._windows_known_folder(_FOLDER_IDS["own_desktop"]) is None
                      and td.discover_shortcuts() == [],
                      f"{label}: the Windows folder call did not give None")
        if os.name == "nt":
            got = td._windows_known_folder(_WINDOWS_FOLDER)
            windows = os.environ.get("SystemRoot") or os.environ.get("windir")
            _need(got is not None and windows is not None and _key(got) == _key(windows),
                  f"the real Windows folder call gave {got!r}")
            _need(td._windows_known_folder("00000000-0000-0000-0000-000000000001") is None,
                  "an unknown folder id gave a path")
            _need(td._windows_known_folder("not a folder id") is None,
                  "a malformed folder id did not give None")
            # The call itself, through _FakeShell: flags 0 (the current path, never
            # created), the answer read only on success, the buffer always freed.
            folder_id = _FOLDER_IDS["own_desktop"]
            asked = [(uuid.UUID(folder_id).bytes_le, 0, None)]
            failure = -2147024894                 # 0x80070002 as the signed HRESULT
            calls = (("an answer", 0, "Q-folder", "Q-folder", True, True),
                     ("a failure that still hands out a buffer", failure, "Q-left", None,
                      False, True),
                     ("a failure without a buffer", failure, None, None, False, False),
                     ("success without a buffer", 0, None, None, False, False))
            for label, status, text, want, read, freed in calls:
                shell = _FakeShell(status, text)
                with _Seams(td, _ctypes=shell):
                    got = td._windows_known_folder(folder_id)
                _need(got == want and shell.calls == asked
                      and shell.reads == ([shell.address] if read else [])
                      and shell.freed == ([shell.address] if freed else []),
                      f"the folder call, {label}: gave {got!r}, asked {shell.calls!r}, "
                      f"read {len(shell.reads)}, freed {len(shell.freed)}")
            query, free = shell.functions["query"], shell.functions["free"]
            _need(query.argtypes == (shell.c_void_p, shell.c_uint32, shell.c_void_p,
                                     shell.POINTER(shell.c_void_p))
                  and query.restype is shell.c_long
                  and free.argtypes == (shell.c_void_p,) and free.restype is None,
                  "the two Windows calls are not declared as the Windows SDK states them")
            with _Seams(td, _ctypes=_FakeShell(0, "x", load_error=OSError("no library"))):
                _need(td._windows_known_folder(folder_id) is None,
                      "a Windows library that cannot load did not give None")
        _need(td._windows_known_folder(None) is None,
              "a folder id that is not text did not give None")
        saved = {name: sys.modules.get(name, _MISSING) for name in ("winreg", "ctypes")}
        try:
            sys.modules["winreg"] = None      # an import of these now fails
            sys.modules["ctypes"] = None
            spec = importlib.util.spec_from_file_location("tws_discovery_bare", _DISCOVERY)
            bare = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(bare)
        finally:
            for name, value in saved.items():
                if value is _MISSING:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = value
        _need(bare._winreg is None and bare._ctypes is None and bare.REGISTRY_API is None,
              "the module did not import cleanly without winreg and ctypes")
        _need(bare.KNOWN_FOLDER_PATH is bare._windows_known_folder,
              "the shortcut source is not wired to the Windows folder call")
        bare.candidate_roots = lambda: []
        _need(bare.discover_registry() == [] and bare.discover_shortcuts() == []
              and bare.resolve_tws(state_path=state) == (None, None),
              "without winreg and ctypes the module did not resolve to not found")
    return ("no registry, no folder call, no ctypes, or an import without winreg and "
            "ctypes: imports, finds nothing, resolves to (None, None); the folder call "
            "reads its answer only on success and always frees it; the real Windows "
            "folder call answers")


def f14_path_text():
    import tws_discovery as td
    if os.name != "nt":
        return "skipped off Windows (Windows path rules)"
    with _Scratch("twsdisc_f14_") as s:
        root, sep = str(s.root), os.sep
        longest = root + sep + "a" * (4096 - len(root) - 1)
        share = sep * 2 + "server" + sep + "share"
        cases = (
            ("an absolute path", root, Path(root)),
            ("quotes and spaces around it", '  "' + root + '"  ', Path(root)),
            ("4096 characters", longest, Path(longest)),
            ("4097 characters", longest + "a", None),
            ("a relative path", "folder" + sep + "tws.exe", None),
            ("a path from the drive root only", sep + "folder", None),
            ("a drive without its root", s.root.drive + "folder", None),
            ("a network share", share, None),
            ("a network share in forward slashes", "//server/share", None),
            ("a device path", sep * 2 + "?" + sep + root, None),
            ("an empty text", "", None),
            ("only quotes", '""', None),
            ("no text", None, None),
            ("bytes", os.fsencode(root), None),
            ("a number", 5, None),
        )
        for label, text, want in cases:
            got = td._path_from(text)
            _need(got == want, f"{label}: gave {got!r}")
        _need(td._path_from(share, network=True) == Path(share),
              "a network folder answered by the Windows folder call was refused")
    return ("absolute paths only, quotes and spaces stripped, 4096-character cap, network "
            "and device paths refused except for the Windows folder answer")


_REAL = {}


def _fence_outside_sources():
    """Turn the module's standard folders, registry and Windows-folder
    providers off for this run.  Checks that need a source install their own
    through _Seams (F7 calls the saved standard-folder list with every
    folder variable pointed at scratch), so nothing in this harness, and no
    mutant of the module, looks at this machine's install folders, registry
    or shortcuts."""
    import tws_discovery as td
    _REAL.setdefault("candidate_roots", td.candidate_roots)
    td.candidate_roots = lambda: []
    td.REGISTRY_API = None
    td.KNOWN_FOLDER_PATH = None


BASELINE = [("B1 feature absence", b1_absence),
            ("B2 hardcoded exe + dead-end raises", b2_hardcode_deadend),
            ("B3 GUI manual-only", b3_gui_manual_only)]

FEATURE = [("F1 discovery finds the app", f1_discovery),
           ("F2 remember lifecycle + machine scoping", f2_remember_lifecycle),
           ("F3 step-1-first resolve order", f3_resolve_order),
           ("F4 loopback port scan", f4_port_scan),
           ("F5 GUI pins (resolver, scan section, browse-first)", f5_gui_pins),
           ("F6 flag + launcher rewired", f6_launcher_rewired),
           ("F7 LOCALAPPDATA Programs/tws standard folder", _checked(f7_local_programs_root)),
           ("F8 installed-programs list source", _checked(f8_registry_source)),
           ("F9 Trader Workstation shortcut source", _checked(f9_shortcut_source)),
           ("F10 damaged shortcuts never raise", _checked(f10_damaged_shortcuts)),
           ("F11 resolve order across all sources", _checked(f11_resolve_order)),
           ("F12 stated caps hold", _checked(f12_caps)),
           ("F13 sources unavailable -> not found", _checked(f13_sources_unavailable)),
           ("F14 path text rules", _checked(f14_path_text))]


def main():
    feature = FLAG in _src(_TWS_LAUNCH)
    checks = FEATURE if feature else BASELINE
    mode = "FEATURE" if feature else "BASELINE (feature absent)"
    print(f"tws_discovery_reference — mode: {mode}")
    if feature:
        try:
            _fence_outside_sources()
        except Exception as exc:  # noqa: BLE001
            print(f"  [FAIL] fence on the registry and Windows folders — {exc!r}")
            print("RESULT: 1 check(s) failed -> exit 1")
            return 1
    fails = 0
    for name, fn in checks:
        try:
            ok, detail = fn()
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, f"harness exception: {exc!r}"
        print(f"  [{'PASS' if ok else 'FAIL'}] {name} — {detail}")
        fails += 0 if ok else 1
    if fails:
        print(f"RESULT: {fails} check(s) failed -> exit 1")
        return 1
    if not feature:
        print("RESULT: baseline pins hold, feature not built -> exit 3")
        return 3
    print("RESULT: all feature checks pass -> exit 0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
