"""Acceptance harness: no-console launcher + safe headless streams (Row 72).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (STORAGE_OPTIMIZATION_LOG.md, Row 72).

User order (2026-07-28): "is there anyway to remove the terminal window
[every time] the python is running?" -> "do that".

Contract this harness pins:
- ``engine/headless_streams.py``: ``ensure_streams(root, *, keep=10)`` makes
  ``sys.stdout``/``sys.stderr`` SAFE when they are absent (pythonw): installs
  UTF-8 file streams under ``<root>/Run Logs/`` matching
  ``app-session-*.log`` (module literal SESSION_LOG_GLOB), self-rotates that
  family to the newest ``keep`` (DEFAULT_KEEP = 10 - bounded WITHOUT relying
  on Row 71), NEVER raises (a hostile root degrades to a null sink, printing
  stays safe), and is a no-op when streams are already valid. display_data's
  49 ``print(...)`` calls therefore cannot crash a console-less run, and app
  deaths leave a stderr trace (the 2026-07-27 16:32 app death left none).
- ``launch_app.py`` (project root): the console-free entry point - calls
  ``ensure_streams`` BEFORE importing display_data; self-locating; no
  absolute paths.
- ``Start Data Bank.bat`` (project root): double-clickable, ``%~dp0``
  self-locating, discovers pythonw at RUN TIME (a machine fact is never
  stored - portability rule), leaves no persistent console window.
- The visual no-console confirmation on a real double-click stays user-gated
  (Row 34 pattern); this harness proves the offline core.

Change polarity: while ``engine/headless_streams.py`` does not exist the
feature is absent and this harness exits 3 (pending); after Row 72 lands
every check passes and it exits 0. Exit 1 = a check failed.

Offline and headless: temp dirs only; the real ``Run Logs/`` is never
touched; stream swaps are restored in ``finally``; no GUI, no network, no
process launch.

    python engine/headless_launcher_reference.py

Placement note: authored in the Claude scratchpad; lands in engine/ at Row
72's own promotion (one unregistered harness at a time - Row 60 drift
guard); Codex's Row 72 checkpoint registers it in REFERENCE_SUITE_NAMES.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import inspect
import os
import sys
import tempfile
from pathlib import Path

_CANDIDATES = (Path(__file__).resolve().parent,
               Path.cwd() / "engine",
               Path.cwd())
for _cand in _CANDIDATES:
    if (_cand / "run_gates.py").exists():
        ENGINE_ROOT = _cand
        break
else:  # pragma: no cover - misplacement is a setup error, not a finding
    sys.stderr.write("cannot locate engine/ (run_gates.py)\n")
    sys.exit(2)
PROJECT_ROOT = ENGINE_ROOT.parent
if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

try:                                  # harness hygiene (Row 61)
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

from check_kit import CheckKit        # noqa: E402

KIT = CheckKit()
check = KIT.check
section = KIT.section

RUN_LOGS = "Run Logs"
GLOB = "app-session-*.log"
LAUNCHER = "Start Data Bank.bat"
ENTRY = "launch_app.py"
PROBE = "probe-token-row72"

HAS_FEATURE = (ENGINE_ROOT / "headless_streams.py").exists()


def session_files(rl: Path):
    return sorted(p for p in rl.iterdir()
                  if fnmatch.fnmatch(p.name, GLOB))


def headless_call(fn, *args, **kwargs):
    """Run fn with stdout/stderr set to None, restore afterward.

    Returns (crashed_repr_or_None, print_ok, installed_out, installed_err).
    Never lets the swap leak: restoration happens in finally.
    """
    real_out, real_err = sys.stdout, sys.stderr
    crashed = None
    print_ok = False
    inst_out = inst_err = None
    sys.stdout = None
    sys.stderr = None
    try:
        fn(*args, **kwargs)
        print(PROBE)
        if sys.stdout is not None and hasattr(sys.stdout, "flush"):
            sys.stdout.flush()
        print_ok = True
    except BaseException as exc:      # noqa: BLE001 - the contract under test
        crashed = repr(exc)
    finally:
        inst_out, inst_err = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = real_out, real_err
    for stream in (inst_out, inst_err):
        if stream not in (None, real_out, real_err):
            try:
                stream.close()
            except Exception:         # noqa: BLE001 - cleanup only
                pass
    return crashed, print_ok, inst_out, inst_err


def run():
    if not HAS_FEATURE:
        section("BASELINE - a console-less run is unsafe today")
        check("B1 engine/headless_streams.py does not exist yet",
              not (ENGINE_ROOT / "headless_streams.py").exists())
        check("B2 no console-free launcher ships in the project root yet",
              not (PROJECT_ROOT / ENTRY).exists()
              and not (PROJECT_ROOT / LAUNCHER).exists())
        dd = (PROJECT_ROOT / "display_data.py").read_text(
            encoding="utf-8", errors="replace")
        check("B3 nothing makes the app's streams safe today",
              "ensure_streams" not in dd)
        return

    # ---------------- post-implementation acceptance ----------------
    import headless_streams as hs     # noqa: E402  (feature import)

    section("A1. module literals and seams")
    check("A1 session-log family literal is pinned",
          getattr(hs, "SESSION_LOG_GLOB", None) == GLOB,
          str(getattr(hs, "SESSION_LOG_GLOB", None)))
    check("A1b default rotation keep is 10",
          getattr(hs, "DEFAULT_KEEP", None) == 10)
    params = inspect.signature(hs.ensure_streams).parameters
    check("A1c ensure_streams(root, *, keep=...) seams exist",
          "root" in params and "keep" in params, str(sorted(params)))

    section("A2. headless install: printing becomes safe, output lands")
    with tempfile.TemporaryDirectory(prefix="hls_",
                                     ignore_cleanup_errors=True) as tmp:
        root = Path(tmp)
        rl = root / RUN_LOGS
        rl.mkdir()
        for i in range(14):           # rotation pressure, oldest first
            p = rl / f"app-session-2026070{i % 10}-{i:02d}.log"
            p.write_text("old\n", encoding="utf-8")
            ts = (dt.datetime(2026, 6, 1)
                  + dt.timedelta(days=i)).timestamp()
            os.utime(p, (ts, ts))
        crashed, print_ok, inst_out, _ = headless_call(
            hs.ensure_streams, root, keep=10)
        check("A2 ensure_streams never raises under absent streams",
              crashed is None, str(crashed))
        check("A2b print() works after the install (no crash, real stream)",
              print_ok and inst_out is not None)
        landed = [p for p in session_files(rl)
                  if PROBE in p.read_text(encoding="utf-8",
                                          errors="replace")]
        check("A2c the probe line landed in a session log",
              len(landed) == 1, str([p.name for p in landed]))

        section("A3. the session-log family self-rotates (keep=10)")
        check("A3 at most 10 session logs remain after the install",
              len(session_files(rl)) <= 10,
              str(len(session_files(rl))))
        check("A3b the newest log (the live one) survived rotation",
              landed and landed[0] in session_files(rl))

    section("A4. valid streams are a no-op")
    before = sys.stdout
    with tempfile.TemporaryDirectory(prefix="hls_") as tmp:
        (Path(tmp) / RUN_LOGS).mkdir()
        hs.ensure_streams(Path(tmp), keep=10)
    check("A4 already-valid stdout is left in place",
          sys.stdout is before)

    section("A5. hostile root cannot crash a console-less start")
    with tempfile.TemporaryDirectory(prefix="hls_",
                                     ignore_cleanup_errors=True) as tmp:
        hostile = Path(tmp) / "not-a-dir"
        hostile.write_text("file, not a directory", encoding="utf-8")
        crashed, print_ok, _, _ = headless_call(
            hs.ensure_streams, hostile, keep=10)
        check("A5 ensure_streams degrades without raising",
              crashed is None, str(crashed))
        check("A5b printing is still safe on the degraded sink",
              print_ok)

    section("A6. launcher artifacts: self-locating, no console, ordered")
    entry_p = PROJECT_ROOT / ENTRY
    bat_p = PROJECT_ROOT / LAUNCHER
    check("A6 launch_app.py ships in the project root", entry_p.exists())
    check("A6b Start Data Bank.bat ships in the project root",
          bat_p.exists())
    entry_src = entry_p.read_text(encoding="utf-8", errors="replace") \
        if entry_p.exists() else ""
    bat_src = bat_p.read_text(encoding="utf-8", errors="replace") \
        if bat_p.exists() else ""
    i_safe = entry_src.find("ensure_streams")
    i_app = entry_src.find("display_data")
    check("A6c streams are made safe BEFORE display_data is imported",
          0 <= i_safe < i_app, f"safe={i_safe} app={i_app}")
    check("A6d the .bat self-locates with %~dp0", "%~dp0" in bat_src)
    check("A6e the launcher chain runs windowless python (pythonw)",
          "pythonw" in bat_src.lower() or "pythonw" in entry_src.lower())
    combined = entry_src + bat_src
    check("A6f no stored absolute user path in either artifact",
          ":\\Users" not in combined and ":/Users" not in combined)

    section("A7. inventory self-registration (Row 60 fail-closed guard)")
    rg = (ENGINE_ROOT / "run_gates.py").read_text(encoding="utf-8")
    check("A7 this harness is registered in the static reference inventory",
          '"headless_launcher_reference"' in rg
          or "'headless_launcher_reference'" in rg,
          "unregistered: every battery fails closed with inventory drift")


def main():
    run()
    if not HAS_FEATURE:
        KIT.pending(
            "M1",
            "engine/headless_streams.py does not exist: a pythonw launch "
            "would crash on the app's first print() and leave no trace - "
            "this is the pre-change baseline.",
            "M1 adds ensure_streams (safe UTF-8 session-log streams under "
            "Run Logs/app-session-*.log, self-rotating keep-10, never "
            "raises), launch_app.py calling it before the display_data "
            "import, and the %~dp0 Start Data Bank.bat that runs pythonw "
            "discovered at run time.",
            "No console remains after startup; the visual double-click "
            "check stays user-gated; the session-log family is bounded "
            "on its own, independent of Row 71.")
    return KIT.finish(feature_absent=not HAS_FEATURE)


if __name__ == "__main__":
    sys.exit(main())
