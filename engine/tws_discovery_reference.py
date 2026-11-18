"""Row 54 M0 acceptance harness — IBKR/TWS app auto-discovery, remembered manual
path (machine-scoped), step-1-first resolution, and port auto-scan.

CLAUDE-OWNED reference harness (Codex MUST NOT edit this file).
Plan: TWS_DISCOVERY_PLAN.md. Offline, headless, deterministic. NEVER launches or
touches TWS; no network — the only sockets are loopback listeners this harness
itself opens and closes. Launcher/GUI checks are SOURCE-level (no display_data /
tws_launch import — the launcher module is never executed here).

Exit codes:
  3 = feature absent (no TWS_APP_DISCOVERY in tws_launch) + baseline pins hold.
  0 = feature present + all feature checks pass.
  1 = any check failed, or the harness itself broke.
"""

import json
import re
import socket
import sys
import tempfile
import shutil
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


BASELINE = [("B1 feature absence", b1_absence),
            ("B2 hardcoded exe + dead-end raises", b2_hardcode_deadend),
            ("B3 GUI manual-only", b3_gui_manual_only)]

FEATURE = [("F1 discovery finds the app", f1_discovery),
           ("F2 remember lifecycle + machine scoping", f2_remember_lifecycle),
           ("F3 step-1-first resolve order", f3_resolve_order),
           ("F4 loopback port scan", f4_port_scan),
           ("F5 GUI pins (resolver, scan section, browse-first)", f5_gui_pins),
           ("F6 flag + launcher rewired", f6_launcher_rewired)]


def main():
    feature = FLAG in _src(_TWS_LAUNCH)
    checks = FEATURE if feature else BASELINE
    mode = "FEATURE" if feature else "BASELINE (feature absent)"
    print(f"tws_discovery_reference — mode: {mode}")
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
