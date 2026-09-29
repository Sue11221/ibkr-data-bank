"""
Data Bank — Data Framework
Step 1: GUI to drag-and-drop a raw OHLCV CSV, Parquet, or Excel file and explore it at
different bar intervals.

Features
    • Drag-and-drop a .csv or .parquet file (or click Browse) to load.
    • The base bar interval is auto-detected — the file does not have to
      be 1-minute. 1s, 1m, 5m, 1h, 1D — anything works.
    • Pick a preset interval or type any pandas rule (10min, 45min, 2h,
      3D, 2W, 1ME, 1YE …) and press Enter / Apply.
    • Interactive chart:
        - Hover anywhere to see exact O / H / L / C / Volume + change at
          the cursor (snaps to the nearest bar).
        - Quick range buttons:  1D · 1W · 1M · 3M · 6M · 1Y · 5Y · All
        - Log-scale toggle for price.
        - Volume bars colored green (up) / red (down) by direction.
        - High/Low range shaded under the close line.
        - Currency Y-axis on price, K/M/B suffix on volume.
"""

from __future__ import annotations

VOL_EXTENDED_TICKBOX = True
PORTFREE_DEBT_CONTROLS = True


def _exd_grid_widths(cols, rendered, spec):
    """Return Export Designer grid widths from already-rendered cell text.

    ``rendered`` contains the exact strings that will be inserted into the
    Treeview, so date-format and timezone choices are reflected without a
    second formatting pass.  ``spec`` is accepted as part of the preview
    contract; its choices are already embodied in those strings.
    """
    del spec
    date_floors = {"timestamp": 156, "date": 58, "time": 58}
    widths = {}
    for index, col in enumerate(cols):
        width = max(58, 8 * len(str(col)) + 26)
        if col in date_floors:
            longest = max(
                (len(str(row[index])) for row in rendered
                 if index < len(row)),
                default=0,
            )
            width = max(date_floors[col], width, 8 * longest + 26)
        widths[col] = width
    return widths

# ---------------------------------------------------------------------------
# Dependency bootstrap.  Run BEFORE any third-party import so we can install
# missing packages on first launch without the user having to invoke setup.
# Uses stdlib only.
# ---------------------------------------------------------------------------
import os
import sys
from pathlib import Path
import dependency_manifest as _dependencies


_RESTART_MARKER = "EMA_DATA_BANK_DEP_RESTARTED"


def _dep_is_installed(import_name: str) -> bool:
    """True if `import_name` is importable, WITHOUT importing it (stays cheap and
    side-effect-free for the heavy packages)."""
    import importlib.util
    try:
        return importlib.util.find_spec(import_name) is not None
    except Exception:  # noqa: BLE001 — a broken/partial install counts as missing
        return False


def _dep_pip_install(pip_name: str) -> bool:
    """pip-install one package (global, then --user fallback). True on success."""
    import subprocess
    g = [sys.executable, "-m", "pip", "install", "--upgrade", pip_name]
    u = [sys.executable, "-m", "pip", "install", "--user", "--upgrade", pip_name]
    try:        # timeout: a stalled network must not hang the installer forever
        if subprocess.run(g, capture_output=True, text=True,
                          timeout=600).returncode == 0:
            return True
        return subprocess.run(u, capture_output=True, text=True,
                              timeout=600).returncode == 0
    except Exception:  # noqa: BLE001 — incl. TimeoutExpired
        return False


def _restart_command() -> list:
    """Build the command to re-launch this program. argv[0] (the script) is made
    ABSOLUTE so the relaunch works no matter what the current working directory
    is — a relative script path is the #1 reason a naive execv 'does nothing'
    (the re-exec'd Python can't find the script and dies immediately)."""
    argv = list(sys.argv)
    if argv and not argv[0].startswith("-"):          # not `python -c/-m ...`
        script = Path(argv[0])
        if not script.is_absolute():
            here = Path(__file__).resolve()
            # Prefer this file's real path when argv[0] is just its bare name.
            script = here if here.name == script.name \
                else (Path.cwd() / script).resolve()
        argv[0] = str(script)
    return [sys.executable, *argv]


def _restart_program() -> None:
    """Re-launch the program so freshly installed packages load, then quit.
    Tries os.execve (replace this process); if that fails, spawns one marked
    child and exits. The marker prevents a second install/restart loop."""
    import os
    import subprocess
    cmd = _restart_command()
    child_env = dict(os.environ)
    child_env[_RESTART_MARKER] = "1"
    # Windows: os.execv passes argv to the child UNQUOTED, so a script path
    # with spaces arrives as several broken args — and because the exec itself
    # "succeeds", the fallback below would never run. Popen takes the list
    # form and quotes correctly, so spawn-and-exit is the reliable route there.
    if os.name != "nt":
        try:
            os.execve(cmd[0], cmd, child_env)  # replaces this process on success
        except Exception:  # noqa: BLE001 — fall through to the spawn fallback
            pass
    try:
        subprocess.Popen(cmd, env=child_env)  # one marked child only
    except Exception:  # noqa: BLE001
        return                           # both failed; leave the window up
    os._exit(0)


def _dependency_installer_gui(missing, os_name="this computer",
                              required_pips=None) -> bool:
    """Popup (instead of the program) listing the missing packages with Install
    / Close. On Install: shows a progress bar over all packages, installs each,
    then RESTARTS the program — UNLESS a REQUIRED package failed (then it shows
    the error and keeps Close). `missing` is a list of (pip_name, import_name);
    `required_pips` is the set of pip names that are mandatory (the rest are
    optional accelerators that never block). `os_name` is shown to the user.
    Returns False if no GUI can be created (caller then uses the console path)."""
    import threading
    import queue as _queue
    try:
        import tkinter as tk
        from tkinter import ttk
        root = tk.Tk()
    except Exception:  # noqa: BLE001 — headless / no Tk → console fallback
        return False

    root.title("Data Bank")
    root.resizable(False, False)
    try:
        root.attributes("-topmost", True)
    except Exception:  # noqa: BLE001
        pass

    msgq: "_queue.Queue" = _queue.Queue()
    state = {"busy": False}

    outer = ttk.Frame(root, padding=22)
    outer.pack(fill="both", expand=True)
    ttk.Label(outer, text="Missing required packages",
              font=("TkDefaultFont", 15, "bold")).pack(anchor="w")
    ttk.Label(outer, text=f"Operating system detected:  {os_name}",
              foreground="#555").pack(anchor="w", pady=(2, 0))
    ttk.Label(outer, text="The program can't start until these are installed:",
              foreground="#555").pack(anchor="w", pady=(2, 10))
    lst = ttk.Frame(outer)
    lst.pack(fill="x")
    for pip_name, _imp in missing:
        opt = "" if (required_pips is None or pip_name in required_pips) \
            else "   (optional — speeds things up)"
        ttk.Label(lst, text=f"     •   {pip_name}{opt}",
                  font=("TkDefaultFont", 12)).pack(anchor="w")

    status = ttk.Label(outer, text="", foreground="#333", justify="left")
    bar = ttk.Progressbar(outer, mode="determinate", length=440,
                          maximum=len(missing))
    btns = ttk.Frame(outer)
    btns.pack(fill="x", pady=(16, 0))

    def do_close():
        root.destroy()
        sys.exit(0)

    def worker():
        failed, done = [], 0
        for pip_name, _imp in missing:
            msgq.put(("status",
                      f"Installing  {pip_name}  …    ({done + 1} of {len(missing)})"))
            ok = _dep_pip_install(pip_name)
            done += 1
            if not ok:
                failed.append(pip_name)
            msgq.put(("step", done))
        msgq.put(("done", failed))

    def pump():
        try:
            while True:
                kind, val = msgq.get_nowait()
                if kind == "status":
                    status.config(text=val)
                elif kind == "step":
                    bar["value"] = val
                elif kind == "done":
                    # Block only if a REQUIRED package failed; a failed optional
                    # package still lets the program start.
                    blocking = [f for f in val
                                if required_pips is None or f in required_pips]
                    if blocking:
                        status.config(
                            text="Could not install: " + ", ".join(blocking) +
                            f"\nRun  \"{sys.executable}\" display_data.py --setup"
                            "  for details.")
                        close_btn.config(state="normal")
                        return
                    status.config(text="All set — restarting the program …")
                    root.after(700, _restart_program)
                    return
        except _queue.Empty:
            pass
        root.after(120, pump)

    def do_install():
        if state["busy"]:
            return
        state["busy"] = True
        install_btn.pack_forget()
        close_btn.config(state="disabled")
        status.pack(anchor="w", pady=(14, 6))
        bar.pack(anchor="w")
        threading.Thread(target=worker, daemon=True).start()
        root.after(120, pump)

    close_btn = ttk.Button(btns, text="Close", command=do_close)
    close_btn.pack(side="right", padx=(8, 0))
    install_btn = ttk.Button(btns, text="Install", command=do_install)
    install_btn.pack(side="right")
    root.protocol("WM_DELETE_WINDOW", do_close)

    root.update_idletasks()
    w, h = root.winfo_width(), root.winfo_height()
    sw, sh = root.winfo_screenwidth(), root.winfo_screenheight()
    root.geometry(f"+{max(0, (sw - w) // 2)}+{max(0, (sh - h) // 3)}")
    root.mainloop()
    return True


# ---------------------------------------------------------------------------
# Dependency definitions + installer (formerly setup.py — inlined so the app is
# one file). All stdlib, so it runs before any third-party package is installed.
# Still usable standalone:  python display_data.py --setup
# ---------------------------------------------------------------------------
# The sole package table lives in the sibling stdlib-only module; the startup
# gate and --setup consume its OS-filtered projection, not separate lists.


def detect_os() -> str:
    """Friendly OS name: 'Windows', 'macOS', or 'Linux' (raw platform.system()
    otherwise)."""
    import platform
    return {"Darwin": "macOS"}.get(platform.system(), platform.system() or "unknown")


def required_packages():
    """(pip_name, import_name) packages required for this OS."""
    return _dependencies.packages("required")


def optional_packages():
    """Feature-labelled packages which never block startup."""
    return _dependencies.packages("optional")


REQUIRED_PACKAGES = required_packages()
OPTIONAL_PACKAGES = optional_packages()


def _capture(cmd):
    """Run a command, capturing stdout and stderr as strings. Times out after
    10 minutes so a dead network can't hang the bootstrap indefinitely."""
    import subprocess
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        return 124, "", "timed out after 600 s (network stalled?)"
    return proc.returncode, proc.stdout, proc.stderr


def run_pip(args):
    """Run pip with the given args, print captured output, return (rc, out, err)."""
    cmd = [sys.executable, "-m", "pip", *args]
    print(f"  $ {' '.join(cmd)}")
    rc, out, err = _capture(cmd)
    if out.strip():
        print("  --- pip stdout ---")
        for line in out.rstrip().splitlines():
            print(f"  {line}")
    if err.strip():
        print("  --- pip stderr ---")
        for line in err.rstrip().splitlines():
            print(f"  {line}")
    print(f"  --- exit code: {rc} ---")
    return rc, out, err


def upgrade_pip() -> None:
    print("Upgrading pip / setuptools / wheel ...")
    rc, _, _ = run_pip(["install", "--upgrade", "pip", "setuptools", "wheel"])
    if rc != 0:
        print("  Global upgrade failed — retrying with --user")
        run_pip(["install", "--user", "--upgrade", "pip", "setuptools", "wheel"])


def install(pip_name) -> bool:
    """Try a global install, then fall back to --user. True on success."""
    print(f"Installing {pip_name} ...")
    rc, _, _ = run_pip(["install", "--upgrade", pip_name])
    if rc == 0:
        return True
    print(f"  Global install failed (exit {rc}) — retrying with --user")
    rc, _, _ = run_pip(["install", "--user", "--upgrade", pip_name])
    return rc == 0


def print_diagnostics() -> None:
    import platform
    import ssl
    import urllib.request
    print("Diagnostics")
    print(f"  Python:        {sys.version.split()[0]}")
    print(f"  Executable:    {sys.executable}")
    print(f"  OS detected:   {detect_os()}")
    print(f"  Platform:      {platform.platform()}")
    print(f"  Machine:       {platform.machine()}")
    print(f"  OpenSSL:       {ssl.OPENSSL_VERSION}")
    rc, out, err = _capture([sys.executable, "-m", "pip", "--version"])
    print(f"  Pip:           {(out or err).strip() or 'unavailable'}")
    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen("https://pypi.org/simple/", timeout=10,
                                    context=ctx) as resp:
            print(f"  PyPI reachable: yes (HTTP {resp.status})")
    except Exception as exc:  # noqa: BLE001
        print(f"  PyPI reachable: NO — {type(exc).__name__}: {exc}")
        print("    ↑ Usually means SSL certificates are missing. On macOS "
              "python.org installs, run 'Install Certificates.command'.")


def ensure_dependencies(verbose: bool = True):
    """Install any REQUIRED packages that aren't importable; returns the pip
    names that FAILED (empty on success). The startup gate never installs or
    nags for optional features; only explicit --setup offers those."""
    failed = []
    for pip_name, import_name in REQUIRED_PACKAGES:
        if _dep_is_installed(import_name):
            continue
        if verbose:
            print(f"  Missing: {pip_name}  →  installing …")
        if not install(pip_name):
            failed.append(pip_name)
    return failed


def _setup_main() -> None:
    """Standalone dependency setup — `python display_data.py --setup`. Prints
    diagnostics, upgrades pip, then installs everything verbosely."""
    print("=" * 60)
    print("Data Bank — Dependency Setup")
    print("=" * 60)
    print_diagnostics()
    print("-" * 60)
    print(f"Operating system: {detect_os()}")
    print(f"Required for {detect_os()}: "
          f"{', '.join(p for p, _ in REQUIRED_PACKAGES)}")
    print("Optional features: " + ", ".join(
        f"{row.pip_name} [{row.feature}]"
        for row in _dependencies.for_platform() if row.tier == "optional"))
    print("-" * 60)
    upgrade_pip()
    print("-" * 60)
    failed, skipped, installed = [], [], []
    for pip_name, import_name in REQUIRED_PACKAGES:
        if _dep_is_installed(import_name):
            print(f"✓ {pip_name} already installed — skipping")
            skipped.append(pip_name)
            continue
        (installed if install(pip_name) else failed).append(pip_name)
    opt_failed = []
    for pip_name, import_name in OPTIONAL_PACKAGES:
        if _dep_is_installed(import_name):
            print(f"✓ {pip_name} already installed — skipping")
            skipped.append(pip_name)
            continue
        if install(pip_name):
            installed.append(pip_name)
        else:
            opt_failed.append(pip_name)
            print(f"  (optional) {pip_name} not installed — app runs without it")
    print("-" * 60)
    print("Summary")
    print(f"  Installed: {installed or '(none)'}")
    print(f"  Skipped:   {skipped or '(none)'}")
    print(f"  Failed:    {failed or '(none)'}")
    if opt_failed:
        print(f"  Optional skipped (OK, fallback used): {opt_failed}")
    if failed:
        print("\nOne or more packages failed. The pip output above is the real "
              "cause. Common patterns:\n"
              "  • 'externally-managed-environment' → use a venv:\n"
              f"       \"{sys.executable}\" -m venv .venv\n"
              f"       \"{sys.executable}\" display_data.py --setup\n"
              "  • 'SSL: CERTIFICATE_VERIFY_FAILED' → install certificates "
              "(macOS python.org: run 'Install Certificates.command').\n"
              "  • 'Permission denied' → the --user retry should handle it, "
              "else use a venv.")
        sys.exit(1)
    print("\nAll dependencies ready.")


def _bootstrap_dependencies() -> None:
    """Before any heavy import: confirm every required package is installed. If
    all present, return and let the program start. If any are missing, show the
    installer popup (or a console fallback when no display is available)."""
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))

    # Which OS, and the packages it needs (defined just above — was setup.py).
    os_name = detect_os()
    required = list(required_packages())
    missing_required = [(p, i) for p, i in required if not _dep_is_installed(i)]
    if not missing_required:
        return  # required all present → start (a missing optional just means the
        #         slower fallback path; never block or nag for accelerators)

    if os.environ.get(_RESTART_MARKER) == "1":
        print("Required packages remain missing after one setup restart: "
              + ", ".join(p for p, _ in missing_required)
              + f". Run \"{sys.executable}\" display_data.py --setup "
                "and inspect its diagnostics; not restarting again.")
        raise SystemExit(1)

    # Show/install ONLY missing required packages. Optional features are
    # available through explicit --setup and in-place feature hints.
    required_pips = {p for p, _ in missing_required}
    missing = missing_required
    if _dependency_installer_gui(missing, os_name, required_pips):
        return  # GUI handled it (it either restarts or exits the process)

    # No GUI available — install on the console, then restart.
    print(f"Operating system detected: {os_name}")
    print("Missing dependencies: " + ", ".join(p for p, _ in missing))
    failed = ensure_dependencies(verbose=True)
    if failed:
        print("Failed to install: " + ", ".join(failed))
        sys.exit(1)
    _restart_program()


# Run the dependency gate ONLY in the genuine main process. Spawned grid-search
# workers re-import this module (spawn re-runs the launcher's top level), and
# `current_process().name` is the one signal that stays 'MainProcess' only in
# the real launcher — robust even if some OTHER program imports this one in the
# future. Without this guard, every worker would re-run the installer popup.
import multiprocessing as _mp_bootstrap_guard  # noqa: E402  (stdlib, safe early)

if _mp_bootstrap_guard.current_process().name == "MainProcess":
    if "--setup" in sys.argv[1:]:        # standalone dependency installer/diagnostics
        _setup_main()
        sys.exit(0)
    _bootstrap_dependencies()


# ---------------------------------------------------------------------------
# Normal imports (safe now that deps are installed).
# ---------------------------------------------------------------------------
import io
import multiprocessing as mp
import queue as queue_mod
import re
import shutil
import threading
import time
import tkinter as tk
import warnings

# Resolve the local engine no matter how the application was launched.
_HERE = str(Path(__file__).resolve().parent)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
sys.path.insert(0, str(Path(__file__).resolve().parent / "engine"))
import stock_storage  # noqa: E402  — bar-archive core (GUI-free, stdlib-only)
import stock_ingest   # noqa: E402  — Tier-1 mixed-bag ingest (GUI-free)
import stock_ibkr     # noqa: E402  — Tier-2 IBKR gap-fill (GUI-free)
import addstock_run_manifest  # noqa: E402 - durable Add Stocks run intent
import addstock_watchdog  # noqa: E402 - Add Stocks port-death protection
import tws_restart_handshake  # noqa: E402 - headless restart decision core
import fix_data_pipeline  # noqa: E402 - staged Fix Data orchestration
import live_spot_probe  # noqa: E402 - embedded report-only WS8 checks
import vol_value_audit  # noqa: E402 - durable volatility anomaly queue
import vol_value_reconcile  # noqa: E402 - shared M3 reconcile path
import stock_validate  # noqa: E402  — daily-OHLC cross-validation (GUI-free)
import fetch_eta      # noqa: E402  — work-weighted live batch ETA (GUI-free)
import fleet_sizing  # noqa: E402  — pure Start multi-port memory policy
import tws_vmoptions  # noqa: E402  — portable Start multi-port heap-cap installer
import health as health_report  # noqa: E402  — unified offline bank health report
import split_cache  # noqa: E402  — explicit identity-bound split refresh
import calendar_reconciler  # noqa: E402  — auto-maintained market closure sidecar
import export_csv     # noqa: E402  — combined-CSV export from month shards
import export_designer  # noqa: E402  — configurable Export Designer engine core
import export_batch  # noqa: E402  — shared folder/note batch orchestration
import export_quality  # noqa: E402  — current health before data export
import sp500           # noqa: E402  — S&P 500 preset symbol list

EXPORT_HEALTH_REPORT_NAME = "health_report.json"

# Silence matplotlib's "AutoDateLocator was unable to pick an appropriate
# interval for this date range" warning. The chart still draws correctly
# (it falls back to ~6/12 ticks), the warning is just informational. We
# already widen the locator's intervald table for the common scales below.
warnings.filterwarnings(
    "ignore",
    message=r".*AutoDateLocator was unable.*",
    category=UserWarning,
)
# Silence "Glyph N missing from font(s) ..." — emitted when a Unicode
# symbol isn't in the rendering font. We use ASCII-safe markers below,
# but on some systems even ▲ / ▼ aren't in every monospace font.
warnings.filterwarnings(
    "ignore",
    message=r".*Glyph .* missing from font.*",
    category=UserWarning,
)
# Silence "constrained_layout not applied because axes sizes collapsed to
# zero" — emitted when a chart is laid out while its canvas is still tiny
# (e.g. the Price Volume tab renders on startup while it's the background
# tab, or its short volume panel momentarily collapses). The chart re-lays
# out correctly once the tab is visible / resized.
warnings.filterwarnings(
    "ignore",
    message=r".*constrained_layout not applied.*",
    category=UserWarning,
)
from tkinter import filedialog, messagebox, ttk
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

import numpy as np
import pandas as pd

# matplotlib is heavy (~0.3-0.5s to import) and used ONLY by the PNG-export
# paths, so it is loaded LAZILY there rather than at every app launch. The names
# below appear only in type annotations — which `from __future__ import
# annotations` (top of file) keeps as un-evaluated strings — so a
# TYPE_CHECKING-only import satisfies type checkers without loading matplotlib.




# matplotlib.dates is referenced as `mdates.date2num(...)` across the export
# drawers; route it through the lazy proxy so importing this module stays light.

# Optional Pillow — used to draw the volume-profile histogram as a true
# alpha-blended (transparent) overlay so the candles show through it. tkinter's
# Canvas can't do per-shape transparency (no alpha colors; stipple is a no-op on
# macOS), so we composite an RGBA image instead. Ships with matplotlib; if it's
# absent, the profile falls back to solid bars.
try:
    from PIL import Image as _PILImage, ImageDraw as _PILImageDraw, \
        ImageTk as _PILImageTk
    _HAVE_PIL = True
except ImportError:
    _HAVE_PIL = False

# Optional drag-and-drop support
try:
    from tkinterdnd2 import DND_FILES, TkinterDnD

    DND_AVAILABLE = True
except ImportError:
    DND_AVAILABLE = False

# Optional native macOS pinch-gesture support via PyObjC.
# Tk on macOS does not deliver magnify events, but Cocoa's NSEvent local
# monitor does — we hook into it directly to receive trackpad pinch.
_APPKIT_AVAILABLE = False
if sys.platform == "darwin":
    try:
        from AppKit import NSEvent  # type: ignore

        # NSEventMaskMagnify = 1 << NSEventTypeMagnify (which is 30).
        # NSEventMaskScrollWheel = 1 << NSEventTypeScrollWheel (which is 22).
        # Hard-coded so we don't depend on a particular AppKit constants name.
        _NSEventMaskMagnify = 1 << 30
        _NSEventMaskScrollWheel = 1 << 22
        _APPKIT_AVAILABLE = True
    except ImportError:
        _APPKIT_AVAILABLE = False

# Shared, process-wide flag set by a Cocoa scroll monitor (macOS only):
#   True  → the last scroll came from a trackpad / Magic Mouse (precise deltas)
#   False → from a classic wheel mouse
#   None  → unknown (no monitor — fall back to the delta heuristic)
# A 1-element list so the Cocoa callback can mutate it without touching Tk.
_SCROLL_PRECISE = [None]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS = {"Date", "Time", "open", "high", "low", "close", "volume"}

# Default dataset auto-loaded on startup (when present alongside this script),
# so the app opens straight into Apple data without a manual browse/drop.
DEFAULT_CSV_NAME = "AAPL_10Y_1m.csv"

PRESET_INTERVALS: List[Tuple[str, str]] = [
    ("1 second",   "1s"),
    ("1 minute",   "1min"),
    ("5 minutes",  "5min"),
    ("15 minutes", "15min"),
    ("30 minutes", "30min"),
    ("1 hour",     "1h"),
    ("4 hours",    "4h"),
    ("1 day",      "1D"),
    ("1 week",     "1W"),
    ("1 month",    "1ME"),
]
PRESET_LABEL_TO_RULE = dict(PRESET_INTERVALS)

_LEGACY_TO_MODERN = {"H": "h", "T": "min", "S": "s", "M": "ME", "Y": "YE", "A": "YE"}
_MODERN_TO_LEGACY = {"h": "H", "min": "T", "s": "S", "ME": "M", "YE": "Y"}

_FIXED_LENGTH_UNITS = {
    "min", "h", "H", "T", "s", "S", "D", "W", "ms", "us", "ns",
}

RANGE_PRESETS: List[Tuple[str, Optional[pd.Timedelta]]] = [
    ("1D",  pd.Timedelta(days=1)),
    ("1W",  pd.Timedelta(days=7)),
    ("1M",  pd.Timedelta(days=30)),
    ("3M",  pd.Timedelta(days=90)),
    ("6M",  pd.Timedelta(days=180)),
    ("1Y",  pd.Timedelta(days=365)),
    ("5Y",  pd.Timedelta(days=365 * 5)),
    ("All", None),
]

# TradingView-inspired dark theme palette
UP_COLOR          = "#26A69A"   # bullish candle / volume
DOWN_COLOR        = "#EF5350"   # bearish candle / volume
# Volume-profile bar colors — a light, high-contrast blue / orange pair that
# stays clearly visible (as bars AND as text) on the dark navy background and
# is distinct from the teal/red candles.
PROFILE_UP_COLOR   = "#40C4FF"  # buy volume (bright sky blue)
PROFILE_DOWN_COLOR = "#FFAB40"  # sell volume (bright light orange)
PRICE_COLOR       = "#2962FF"   # accent (pinned crosshair, fallback line)

CANDLE_MAX_BARS = 1000          # show candlesticks when the plot has ≤ this
                                # many bars; beyond that we fall back to a
                                # clean close line — denser wicks just smear
                                # into an unreadable colored band.

# Trackpad two-finger pan speed. The pan scales with the view: one swipe moves
# a fixed FRACTION of the visible range, so it crosses the data fast when
# zoomed out and stays fine when zoomed in (same feel on every tab, since it
# keys off bars-on-screen, not chart pixel width). PAN_SPEED is the single knob
# — raise it to pan faster, lower it to pan slower.
PAN_SPEED = 1.0                 # >1 faster, <1 slower (1.0 ≈ 1:1 on a ~1000px chart)
_PAN_REF_WIDTH = 1000.0         # a swipe of this many px pans one full window at 1.0

# UI font family. macOS keeps the original Helvetica look; Windows/Linux get
# their platform's native face instead of Tk's silent Helvetica substitute.
if sys.platform == "darwin":
    _UI_FONT = "Helvetica"
elif os.name == "nt":
    _UI_FONT = "Segoe UI"
else:
    _UI_FONT = "DejaVu Sans"


# ---------------------------------------------------------------------------
# Data loading / resampling / interval handling
# ---------------------------------------------------------------------------


PARQUET_SUFFIXES = (".parquet", ".parq", ".pq")
EXCEL_SUFFIXES = (".xlsx", ".xlsm", ".xls")
OHLCV_COLS = ["open", "high", "low", "close", "volume"]


def validate_raw_ohlcv(df):
    """Scan a freshly loaded OHLCV frame for abnormal data WITHOUT changing
    anything — the caller always keeps the frame exactly as read from disk.

    Returns (bad, notes): `bad` is a time-sorted COPY of the abnormal raw
    rows (OHLCV columns + a `reason` string; empty frame when clean) and
    `notes` is a list of frame-level issues (timezone-aware index, text
    columns, out-of-order timestamps). Flagged per row: NaN in any OHLCV,
    non-positive PRICES, negative volume, duplicate timestamps (every copy).
    A volume of ZERO is a normal no-trade bar and is deliberately NOT
    flagged."""
    notes = []
    n = len(df)
    cols = [c for c in OHLCV_COLS if c in df.columns]
    if n == 0 or not cols:
        bad = df.iloc[0:0].copy()
        bad["reason"] = pd.Series(dtype=str)
        return bad, notes
    try:
        if getattr(df.index, "tz", None) is not None:
            notes.append("timezone-aware timestamps (sweep ranges compare "
                         "against naive times and may shift)")
    except Exception:  # noqa: BLE001
        pass
    obj_cols = [c for c in cols if not pd.api.types.is_numeric_dtype(df[c])]
    if obj_cols:
        notes.append("non-numeric (text) column(s): " + ", ".join(obj_cols))
    try:
        if not df.index.is_monotonic_increasing:
            notes.append("timestamps not in chronological order")
    except Exception:  # noqa: BLE001
        pass
    num = {c: (pd.to_numeric(df[c], errors="coerce") if c in obj_cols
               else df[c]) for c in cols}
    reasons = []
    for c in cols:
        m = num[c].isna().values
        if m.any():
            reasons.append((f"NaN {c}", m))
    for c in ("open", "high", "low", "close"):
        if c in num:
            m = (num[c] <= 0).values        # NaN compares False — caught above
            if m.any():
                reasons.append((f"{c} <= 0", m))
    if "volume" in num:
        m = (num["volume"] < 0).values
        if m.any():
            reasons.append(("negative volume", m))
    dup = np.asarray(df.index.duplicated(keep=False))
    if dup.any():
        reasons.append(("duplicate timestamp", dup))
    if not reasons:
        bad = df.iloc[0:0][cols].copy()
        bad["reason"] = pd.Series(dtype=str)
        return bad, notes
    any_bad = np.zeros(n, dtype=bool)
    for _, m in reasons:
        any_bad |= m
    bad = df.loc[any_bad, cols].copy()
    rs = np.empty(int(any_bad.sum()), dtype=object)
    rs[:] = ""
    for label, m in reasons:
        sub = m[any_bad]
        rs[sub] = np.where(rs[sub] == "", label, rs[sub] + " + " + label)
    bad["reason"] = rs
    bad = bad.sort_index(kind="mergesort")     # arrange via time (stable)
    return bad, notes


def load_data(path: Path) -> pd.DataFrame:
    """Load a raw OHLCV file into a DatetimeIndexed frame. Supports:
      * .csv          — Date + Time columns (original) OR any flexible layout
                        (a single datetime/timestamp column, lowercase date/time)
      * .parquet      — DatetimeIndex, a Date/Time pair, or a datetime column
      * .xlsx / .xls  — same flexible layouts as parquet (reads the first sheet)
    """
    path = Path(path)
    # iCloud/Files-on-Demand guard: a "dataless" file only materializes on
    # first read — touch one byte HERE so the download (and any failure)
    # happens at a clear point instead of deep inside a lazy parser.
    try:
        with open(path, "rb") as _fh:
            _fh.read(1)
    except OSError:
        pass            # let the real reader raise its own clearer error
    if path.suffix.lower() in PARQUET_SUFFIXES:
        return _load_parquet(path)
    if path.suffix.lower() in EXCEL_SUFFIXES:
        return _load_excel(path)

    df = pd.read_csv(path)

    # Original CSV format: separate Date + Time columns (US m/d/Y h:m:s).
    if {"Date", "Time"} <= set(df.columns):
        missing = EXPECTED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(
                f"CSV is missing required columns: {sorted(missing)}.\n"
                f"Expected columns: {sorted(EXPECTED_COLUMNS)}"
            )
        df["datetime"] = pd.to_datetime(
            df["Date"].astype(str) + " " + df["Time"].astype(str),
            format="%m/%d/%Y %H:%M:%S",
            errors="coerce",
        )
        if df["datetime"].isna().any():
            bad = df["datetime"].isna().sum()
            raise ValueError(f"Could not parse {bad} Date/Time rows.")
        return df.drop(columns=["Date", "Time"]).set_index("datetime").sort_index()

    # Any other CSV layout (a single datetime/timestamp column, lowercase
    # date/time, an already-datetime column, …) → the same flexible normalizer
    # the parquet/Excel loaders use, so a 'datetime' column just works.
    return _normalize_market_frame(df, "CSV")


def _normalize_market_frame(df: pd.DataFrame, source: str = "file") -> pd.DataFrame:
    """Normalize an arbitrary OHLCV DataFrame (read from parquet / Excel / …)
    into the schema load_data() returns for CSVs: a DatetimeIndex named
    'datetime' + open/high/low/close/volume.

    These formats store the timestamp in different ways depending on who wrote
    them, so accept any of: an existing DatetimeIndex, a Date+Time column pair
    (like the CSV), or a single datetime/timestamp column. OHLCV and date/time
    column names are matched CASE-INSENSITIVELY, and a date + time pair is always
    COMBINED so the time-of-day isn't dropped — a date-only column read alone
    makes every intraday bar land at midnight, which the app then mis-detects as
    '1 day' (e.g. for 1-second feeds)."""
    # OHLCV names -> canonical lowercase (writers often capitalize them).
    lower = {str(c).lower(): c for c in df.columns}
    rename = {lower[w]: w for w in OHLCV_COLS if w not in df.columns and w in lower}
    if rename:
        df = df.rename(columns=rename)

    # Resolve the datetime index.
    cmap = {str(c).lower(): c for c in df.columns}
    date_col, time_col = cmap.get("date"), cmap.get("time")

    if isinstance(df.index, pd.DatetimeIndex):
        # A date-only DatetimeIndex (e.g. an Arrow date32 column read as midnight)
        # plus a separate time column → combine, else the intraday resolution is
        # lost and the feed looks daily.
        if time_col is not None and len(df.index) and \
                bool((df.index.normalize() == df.index).all()):
            df = df.set_index(
                df.index.normalize()
                + pd.to_timedelta(df[time_col].astype(str), errors="coerce"))
    else:
        dt_col = cmap.get("datetime") or cmap.get("timestamp")   # full date+time
        if dt_col is None and date_col is None and time_col is None:
            # No named datetime column — fall back to the first column that's
            # already datetime-typed (a DatetimeIndex written to Excel comes back
            # as an unnamed datetime64 column), skipping the OHLCV columns.
            for c in df.columns:
                if c not in OHLCV_COLS and pd.api.types.is_datetime64_any_dtype(df[c]):
                    dt_col = c
                    break
        if dt_col is not None:
            idx = pd.to_datetime(df[dt_col], errors="coerce")
        elif date_col is not None and time_col is not None:
            idx = (pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
                   + pd.to_timedelta(df[time_col].astype(str), errors="coerce"))
        elif date_col is not None:
            idx = pd.to_datetime(df[date_col], errors="coerce")
        elif time_col is not None:
            idx = pd.to_datetime(df[time_col], errors="coerce")
        else:                                   # last resort: parse the index
            idx = pd.to_datetime(df.index, errors="coerce")
        df = df.set_index(pd.DatetimeIndex(idx))

    missing = set(OHLCV_COLS) - set(df.columns)
    if missing:
        raise ValueError(
            f"{source} is missing required columns: {sorted(missing)}.\n"
            f"Need {OHLCV_COLS} plus a datetime index, a Date+Time pair, or a "
            f"datetime/timestamp column."
        )
    if not isinstance(df.index, pd.DatetimeIndex) or df.index.isna().any():
        n_bad = int(pd.isna(df.index).sum())
        raise ValueError(f"Could not parse {n_bad} timestamps in the "
                         f"{source.lower()} (no usable datetime index/column).")

    df.index.name = "datetime"
    return df[OHLCV_COLS].sort_index()


def _load_parquet(path: Path) -> pd.DataFrame:
    """Load a raw OHLCV .parquet file (-> the same schema as a CSV)."""
    try:
        df = pd.read_parquet(path)
    except ImportError as exc:
        raise ImportError(
            "Reading .parquet files needs the 'pyarrow' package. The startup "
            "dependency check installs it automatically — restart the app, or "
            f"run: \"{sys.executable}\" -m pip install pyarrow"
        ) from exc
    return _normalize_market_frame(df, "Parquet")


def _load_excel(path: Path) -> pd.DataFrame:
    """Load a raw OHLCV Excel file (reads the FIRST sheet). .xlsx/.xlsm use
    optional openpyxl; .xls needs optional xlrd.

    The bytes are read fully into memory FIRST, then parsed from a BytesIO
    buffer. .xlsx files are ZIP archives that openpyxl otherwise seeks through
    lazily; on an iCloud-synced or network drive those lazy seeks can stall and
    surface as a noisy 'Exception ignored in ZipFile.__del__: TimeoutError' at
    garbage-collection time. Fetching the bytes up front turns any such stall
    into ONE clean, catchable read error here instead."""
    path = Path(path)
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise OSError(
            f"Could not read the Excel file '{path.name}'. If it lives on an "
            f"iCloud-synced or network drive, make sure it's downloaded and the "
            f"connection is up (Finder → right-click the folder → Download Now).\n"
            f"\nUnderlying error: {exc}"
        ) from exc
    engine = "xlrd" if path.suffix.lower() == ".xls" else "openpyxl"
    try:
        df = pd.read_excel(io.BytesIO(raw), engine=engine)
    except ImportError as exc:
        raise ImportError(
            f"Reading this Excel file needs optional '{engine}'. Run "
            f"\"{sys.executable}\" -m pip install {engine}, or use "
            f"\"{sys.executable}\" display_data.py --setup."
        ) from exc
    return _normalize_market_frame(df, "Excel")


_INTERVAL_UNITS_NS = (
    (86_400_000_000_000, "day"),
    (3_600_000_000_000, "hour"),
    (60_000_000_000, "minute"),
    (1_000_000_000, "second"),
    (1_000_000, "ms"),
    (1_000, "us"),
)


def _snap_interval(td: pd.Timedelta) -> pd.Timedelta:
    """Snap a detected bar spacing to the nearest clean unit, absorbing the
    sub-second jitter that float/epoch timestamp conversion introduces (e.g. a
    1-second feed read back as 0.999999942s). Only snaps when the deviation is a
    tiny fraction (≤0.01%) of the interval, so genuine odd intervals (61s, 3601s)
    are left untouched."""
    ns = int(pd.Timedelta(td).value)
    if ns <= 0:
        return td
    for unit, _label in _INTERVAL_UNITS_NS:
        mult = round(ns / unit)
        if mult >= 1 and abs(ns - mult * unit) <= mult * unit * 1e-4:
            return pd.Timedelta(mult * unit, unit="ns")
    return td


def detect_raw_interval(df: pd.DataFrame) -> Tuple[Optional[pd.Timedelta], str]:
    """Infer the base bar spacing from the mode of consecutive timestamp diffs.
    Ignores zero diffs (duplicate timestamps) and snaps sub-second float jitter,
    so a clean 1-second feed is reported as '1 second', not 'unknown'."""
    if len(df) < 2:
        return None, "unknown"
    diffs = df.index.to_series().diff().dropna()
    diffs = diffs[diffs > pd.Timedelta(0)]          # drop duplicate-timestamp 0s
    if diffs.empty:
        return None, "unknown"
    mode_vals = diffs.mode()
    if mode_vals.empty:
        return None, "unknown"
    td = _snap_interval(pd.Timedelta(mode_vals.iloc[0]))
    if td <= pd.Timedelta(0):
        return None, "unknown"
    return td, format_timedelta(td)


def format_timedelta(td: pd.Timedelta) -> str:
    """Human label for a bar spacing — days/hours/minutes/seconds, and (for
    sub-second feeds) ms/us. Rounds to the nearest nanosecond so float jitter
    doesn't leak into the label."""
    ns = int(round(pd.Timedelta(td).total_seconds() * 1e9))
    if ns <= 0:
        return "unknown"
    for unit, label in _INTERVAL_UNITS_NS:
        if ns % unit == 0:
            n = ns // unit
            if label in ("ms", "us"):
                return f"{n} {label}"
            return f"{n} {label}{'s' if n != 1 else ''}"
    return f"{ns} ns"


def _try_offset(rule: str) -> bool:
    try:
        pd.tseries.frequencies.to_offset(rule)
        return True
    except (ValueError, TypeError):
        return False


def validate_rule(rule: str) -> str:
    """Validate / normalize a pandas resample rule (handles legacy ↔ modern)."""
    rule = (rule or "").strip()
    if not rule:
        raise ValueError("Interval is empty.")

    if _try_offset(rule):
        return rule

    m = re.match(r"^(\d*)([A-Za-z]+)$", rule)
    if m:
        num, unit = m.group(1), m.group(2)
        for mapping in (_LEGACY_TO_MODERN, _MODERN_TO_LEGACY):
            if unit in mapping:
                candidate = f"{num}{mapping[unit]}"
                if _try_offset(candidate):
                    return candidate

    raise ValueError(
        f"'{rule}' is not a valid interval. "
        f"Examples: 5min, 15min, 1h, 4h, 1D, 1W, 1ME."
    )


def rule_to_timedelta(rule: str) -> Optional[pd.Timedelta]:
    """Convert a fixed-length rule to Timedelta. Returns None for M/Y/ME/YE."""
    m = re.match(r"^(\d*)([A-Za-z]+)$", (rule or "").strip())
    if not m or m.group(2) not in _FIXED_LENGTH_UNITS:
        return None
    try:
        return pd.Timedelta(rule)
    except (ValueError, TypeError):
        return None


def timedelta_to_rule(td: Optional[pd.Timedelta]) -> Optional[str]:
    """Express a Timedelta as a pandas resample rule (e.g. 1min, 1h, 1D, 30s).
    Returns None for a missing / non-positive interval."""
    if td is None:
        return None
    secs = int(pd.Timedelta(td).total_seconds())
    if secs <= 0:
        return None
    if secs % 86400 == 0:
        return f"{secs // 86400}D"
    if secs % 3600 == 0:
        return f"{secs // 3600}h"
    if secs % 60 == 0:
        return f"{secs // 60}min"
    return f"{secs}s"


def resample_ohlcv(
    df: pd.DataFrame,
    rule: str,
    raw_interval: Optional[pd.Timedelta] = None,
) -> pd.DataFrame:
    """Resample OHLCV bars (first/max/min/last/sum). Drops empty periods."""
    rule = validate_rule(rule)

    if raw_interval is not None:
        req_td = rule_to_timedelta(rule)
        if req_td is not None and req_td < raw_interval:
            raise ValueError(
                f"Cannot resample to '{rule}' — that is shorter than the raw "
                f"bar interval ({format_timedelta(raw_interval)}). "
                f"Pick the raw interval or a longer one."
            )
        if req_td is not None and req_td == raw_interval:
            return df

    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    # `origin="start"` anchors bin boundaries to the FIRST data timestamp
    # rather than to midnight. For US-stock 1-minute data starting at
    # 09:30, this is the difference between:
    #   default origin (midnight) → 1h bars labeled 09:00, 10:00, … 15:00
    #     where the "09:00" bar actually only contains 09:30–10:00 trading
    #     data, so trade exports show buy_time = 09:00 (before the open)
    #   origin="start" → 1h bars labeled 09:30, 10:30, … 15:30 which match
    #     the market clock (open at 09:30, close at 16:00).
    # Helps every sub-daily ("tick") rule (5min, 15min, 30min, 1h, 4h …).
    #
    # `origin` is silently ignored — and pandas warns loudly — for
    # non-tick / calendar frequencies (D, W, ME/M, Y, B, Q), because
    # those are calendar-anchored, not duration-anchored. So we only
    # pass `origin` when the rule is actually a Tick.
    use_origin = False
    try:
        offset = pd.tseries.frequencies.to_offset(rule)
        use_origin = isinstance(offset, pd.tseries.offsets.Tick)
    except (ValueError, TypeError):
        use_origin = False

    if use_origin:
        try:
            return (
                df.resample(rule, origin="start")
                  .agg(agg)
                  .dropna(subset=["open"])
            )
        except TypeError:
            # Pandas < 1.3 (predates `origin="start"`) — fall through.
            pass
    return df.resample(rule).agg(agg).dropna(subset=["open"])


def summarize(df: pd.DataFrame, interval_label: str) -> str:
    buf = io.StringIO()
    span_days = (df.index.max() - df.index.min()).days

    buf.write("=" * 70 + "\n")
    buf.write(f"DATASET OVERVIEW  —  interval: {interval_label}\n")
    buf.write("=" * 70 + "\n")
    buf.write(f"Bars:           {len(df):,}\n")
    buf.write(f"Columns:        {list(df.columns)}\n")
    buf.write(f"Date range:     {df.index.min()}  →  {df.index.max()}\n")
    buf.write(f"Span:           {span_days:,} days  (~{span_days / 365.25:.2f} years)\n")
    buf.write(f"Distinct days:  {df.index.normalize().nunique():,}\n\n")

    buf.write("First 5 bars:\n")
    buf.write(df.head().to_string() + "\n\n")
    buf.write("Last 5 bars:\n")
    buf.write(df.tail().to_string() + "\n\n")
    buf.write("Summary statistics:\n")
    buf.write(df.describe().to_string() + "\n\n")

    missing = df.isna().sum()
    if missing.any():
        buf.write("Missing values per column:\n")
        buf.write(missing[missing > 0].to_string() + "\n")
    else:
        buf.write("No missing values.\n")
    buf.write("=" * 70 + "\n")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Chart formatters / helpers
# ---------------------------------------------------------------------------


def _volume_fmt(x: float, _pos) -> str:
    if x >= 1e9:
        return f"{x / 1e9:.1f}B"
    if x >= 1e6:
        return f"{x / 1e6:.1f}M"
    if x >= 1e3:
        return f"{x / 1e3:.1f}K"
    return f"{x:.0f}"


def _compact_2dp(x: float) -> str:
    """Compact volume with 2 decimals + lowercase suffix, e.g. 5.14k, 5.31m."""
    x = float(x)
    if x >= 1e9:
        return f"{x / 1e9:.2f}b"
    if x >= 1e6:
        return f"{x / 1e6:.2f}m"
    if x >= 1e3:
        return f"{x / 1e3:.2f}k"
    return f"{x:.2f}"


def _price_fmt(x: float, _pos) -> str:
    return f"${x:,.2f}"


# ---------------------------------------------------------------------------
# Native-Tk-Canvas candlestick chart (no matplotlib / no Agg→Tk blit).
# Tk draws the candle vectors directly via the OS, which is much smoother for
# interactive pan/zoom and works identically on Windows / macOS / Linux.
# ---------------------------------------------------------------------------


def compute_volume_profile(sub: pd.DataFrame, n_bins: int, va_pct: float):
    """Volume-by-price profile over `sub` (raw bars): per-bin up/down volume,
    POC, and the value-area high/low containing `va_pct`% of volume. Returns a
    dict or None."""
    if sub is None or len(sub) < 2:
        return None
    lows = sub["low"].values.astype(float)
    highs = sub["high"].values.astype(float)
    closes = sub["close"].values.astype(float)
    opens = sub["open"].values.astype(float)
    vols = sub["volume"].values.astype(float)
    p_min = float(np.nanmin(lows))
    p_max = float(np.nanmax(highs))
    if not np.isfinite(p_min) or not np.isfinite(p_max) or p_max <= p_min:
        return None
    n_bins = max(4, int(n_bins))
    edges = np.linspace(p_min, p_max, n_bins + 1)
    typical = (highs + lows + closes) / 3.0
    idx = np.clip(np.digitize(typical, edges) - 1, 0, n_bins - 1)
    up = closes >= opens
    up_hist = np.zeros(n_bins)
    down_hist = np.zeros(n_bins)
    np.add.at(up_hist, idx[up], vols[up])
    np.add.at(down_hist, idx[~up], vols[~up])
    total = up_hist + down_hist
    grand = total.sum()
    if grand <= 0:
        return None
    poc = int(np.argmax(total))
    poc_price = (edges[poc] + edges[poc + 1]) / 2.0
    target = (va_pct / 100.0) * grand
    lo = hi = poc
    cum = total[poc]
    while cum < target and (lo > 0 or hi < n_bins - 1):
        below = total[lo - 1] if lo > 0 else -1.0
        above = total[hi + 1] if hi < n_bins - 1 else -1.0
        if above >= below:
            hi += 1
            cum += total[hi]
        else:
            lo -= 1
            cum += total[lo]
    return {"edges": edges, "up": up_hist, "down": down_hist, "total": total,
            "poc_price": poc_price, "val": float(edges[lo]),
            "vah": float(edges[hi + 1]), "grand": grand}


# --- Cross-platform free/total RAM probe (no third-party deps) ---------------
# Sizes the startup port recommendation to the machine without risking OOM.
# Reads the OS's own numbers — Linux /proc/meminfo (MemAvailable), macOS vm_stat
# (free + inactive + purgeable pages), Windows GlobalMemoryStatusEx. EVERY path
# is best-effort: any failure returns None and the caller just runs as usual, so
# the probe can never break the app on an untested OS or configuration.
def _parse_linux_meminfo(text):
    """Available bytes from /proc/meminfo text — MemAvailable (the kernel's own
    reclaimable estimate), or MemFree+Buffers+Cached if that key is absent."""
    info = {}
    for line in text.splitlines():
        key, _, val = line.partition(":")
        info[key.strip()] = val.strip()

    def _kb(key):
        parts = info.get(key, "").split()
        return int(parts[0]) * 1024 if (parts and parts[0].isdigit()) else 0

    if "MemAvailable" in info:
        return _kb("MemAvailable") or None
    return (_kb("MemFree") + _kb("Buffers") + _kb("Cached")) or None


def _parse_macos_vmstat(text):
    """Available bytes from `vm_stat` text — (free + inactive + purgeable) pages
    × the reported page size (inactive/purgeable are reclaimable under pressure)."""
    m = re.search(r"page size of (\d+)", text)
    page = int(m.group(1)) if m else 4096

    def _pages(key):
        mm = re.search(rf"{re.escape(key)}:\s+(\d+)", text)
        return int(mm.group(1)) * page if mm else 0

    return (_pages("Pages free") + _pages("Pages inactive")
            + _pages("Pages purgeable")) or None


def _windows_mem():
    """(total, avail) physical RAM bytes via GlobalMemoryStatusEx, else
    (None, None). DWORD/DWORDLONG are pinned to c_uint32/c_uint64 so the struct
    is the documented 64 bytes on EVERY platform — c_ulong is 4 B on Windows but
    8 B on LP64 Unix, so using it would mis-lay-out (and mis-size) the struct."""
    import ctypes
    class _MEMSTATEX(ctypes.Structure):
        _fields_ = [("dwLength", ctypes.c_uint32),
                    ("dwMemoryLoad", ctypes.c_uint32),
                    ("ullTotalPhys", ctypes.c_uint64),
                    ("ullAvailPhys", ctypes.c_uint64),
                    ("ullTotalPageFile", ctypes.c_uint64),
                    ("ullAvailPageFile", ctypes.c_uint64),
                    ("ullTotalVirtual", ctypes.c_uint64),
                    ("ullAvailVirtual", ctypes.c_uint64),
                    ("ullAvailExtendedVirtual", ctypes.c_uint64)]
    st = _MEMSTATEX()
    st.dwLength = ctypes.sizeof(_MEMSTATEX)
    if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st)):
        return int(st.ullTotalPhys), int(st.ullAvailPhys)
    return None, None


def _available_ram_bytes():
    """Free/reclaimable physical RAM in bytes (cross-platform), or None if the
    OS probe is unavailable/fails (the caller then runs without a RAM estimate)."""
    try:
        plat = sys.platform
        if plat.startswith("linux"):
            with open("/proc/meminfo", "r") as fh:
                return _parse_linux_meminfo(fh.read())
        if plat == "darwin":
            import subprocess
            return _parse_macos_vmstat(
                subprocess.check_output(["vm_stat"], text=True))
        if plat.startswith("win"):
            return _windows_mem()[1]
    except Exception:  # noqa: BLE001 — probe is best-effort, never fatal
        pass
    return None


def _tws_working_set_rows():
    """Best-effort ``(name, working_set_bytes)`` rows for running TWS.

    Measurements are ephemeral and taken only when the startup selector opens.
    ``tasklist`` reports Windows Working Set in KiB; ``ps`` supplies the same
    unit on Linux/macOS.  An unavailable or localized/odd probe simply returns
    no rows, causing the pure policy to use its documented fallback.
    """
    import csv
    import subprocess

    rows = []
    try:
        if sys.platform.startswith("win"):
            result = subprocess.run(
                ["tasklist", "/FI", "IMAGENAME eq tws.exe", "/FO", "CSV",
                 "/NH"],
                capture_output=True, text=True, errors="replace", timeout=10)
            for cells in csv.reader(result.stdout.splitlines()):
                if len(cells) < 5:
                    continue
                digits = "".join(ch for ch in cells[4] if ch.isdigit())
                if digits:
                    rows.append((cells[0].strip(), int(digits) * 1024))
        elif sys.platform == "darwin" or sys.platform.startswith("linux"):
            result = subprocess.run(
                ["ps", "-axo", "comm=,rss="], capture_output=True, text=True,
                errors="replace", timeout=10)
            for line in result.stdout.splitlines():
                parts = line.rsplit(None, 1)
                if len(parts) == 2 and parts[1].isdigit():
                    rows.append((parts[0], int(parts[1]) * 1024))
    except Exception:  # noqa: BLE001 — measurement is advisory, never fatal
        return []
    return rows

class CanvasChart:
    """Fast candlestick + volume chart drawn on a tkinter.Canvas.

    Aggregates the visible bars into a readable number of candles when zoomed
    out and breaks them down to raw bars when zoomed in. Drag to pan (time +
    price), scroll wheel to zoom toward the cursor — cross-platform.
    """

    # Draw raw 1:1 candles until the window exceeds this many bars; beyond
    # that, aggregate. Kept fairly high so zooming in to read the volume
    # profile stays raw (no abrupt candle-count jump while you read it).
    TARGET_CANDLES = 800
    CROSS = "#9598A1"

    def __init__(self, parent, status_var: Optional[tk.StringVar] = None):
        self.df: Optional[pd.DataFrame] = None
        self.status = status_var
        self.start = 0                 # visible window (bar indices)
        self.end = 0
        self._pan_frac_x = 0.0         # carried sub-bar remainder (smooth pan)
        self.k = 1                     # aggregation factor
        self.base_interval = None      # bar spacing of the underlying data
        self.pmin: Optional[float] = None
        self.pmax: Optional[float] = None
        self._manual_y = False
        self.right_margin = 66         # px reserved for price labels
        self.bottom_margin = 22        # px reserved for time labels
        self.vol_frac = 0.18           # volume panel height fraction
        self.top_pad = 10
        self._geom: Optional[dict] = None
        self._drag: Optional[dict] = None
        self._sel: Optional[dict] = None
        self._pin_ts: Optional[pd.Timestamp] = None   # click-pinned inspector
        self._moved = 0.0                  # max finger travel during a press
        self._last = 0.0

        # Overlay / feature config (set by the app's controls).
        self.chart_type = "candle"     # "candle" | "line"
        self.log_scale = False
        self.show_vma = True
        self.vma_period = 20
        self.show_spikes = True
        self.spike_mult = 2.0
        self.vp_on = False
        self.vp_rows = 14
        self.vp_va = 70
        self.vp_range: Optional[Tuple[pd.Timestamp, pd.Timestamp]] = None
        self._vp_photo = None          # ImageTk ref for the transparent VP overlay
        self._vp_cache = None          # cached VP histogram, keyed on range+params
        self._data_ver = 0             # bumped on set_data -> invalidates VP cache
        # Price-panel overlays — EMA lines + crossover markers (EMA Strategy
        # tab). Each line: {values: full-df array, color, width, label}.
        # Each marker set: {idx: int array of bar indices, y: price array,
        # marker: '^'/'v', color}.
        self._ovl_lines: list = []
        self._ovl_markers: list = []

        # Soft light yellow-green theme — easy on the eyes; colors tuned to
        # stay readable on the tinted background.
        self.C_BG = "#EAEFD9"          # canvas background (light green-yellow)
        self.C_GRID = "#D2D9BD"        # gridlines
        self.C_AXIS = "#54583F"        # axis tick labels (warm dark)
        self.C_CROSS = "#6E725C"       # crosshair lines
        self.C_MA = "#E08600"          # volume MA line
        self.C_SPIKE = "#E65100"       # spike marker
        self.C_POC = "#C77F00"         # POC line
        self.C_VA = "#6A4FB0"          # value-area lines
        self.C_BOXBG = "#F2F5E4"       # totals box background
        self.C_BOXEDGE = "#BDC4A4"     # box / range borders
        self.C_WARN = "#C62828"        # warning text
        self.C_BUY = "#4CAF50"         # volume-profile buy bars (lighter green)
        self.C_SELL = "#F4793C"        # volume-profile sell bars (lighter orange)

        self.canvas = tk.Canvas(parent, bg=self.C_BG, highlightthickness=0,
                                bd=0)
        self.canvas.pack(fill=tk.BOTH, expand=True)
        self._wire()

    # -- events ----------------------------------------------------------
    def _wire(self):
        c = self.canvas
        c.bind("<Configure>", lambda _e: self.render())
        c.bind("<ButtonPress-1>", self._on_press)
        c.bind("<B1-Motion>", self._on_drag)
        c.bind("<ButtonRelease-1>", self._on_release)
        c.bind("<Motion>", self._on_motion)
        c.bind("<Leave>", lambda _e: self.canvas.delete("cross"))
        # Plain scroll: trackpad → pan, mouse wheel → zoom (auto-detected).
        # X11 is the exception: a trackpad is indistinguishable from a wheel
        # there (every scroll arrives in ±120 steps), so plain scroll keeps
        # the same pan-price default as the Button-4/5 path below, and zoom
        # stays on Ctrl+scroll. Tk 9 on X11 (TIP 474) retired Button-4/5 and
        # delivers <MouseWheel> instead — so BOTH generations route the same.
        _ws = str(c.tk.call("tk", "windowingsystem"))
        if _ws == "x11":
            c.bind("<MouseWheel>",
                   lambda e: self._on_wheel_linux(self._step(e), e))
        else:
            c.bind("<MouseWheel>", self._on_wheel)      # Win / macOS vertical
        if _ws != "aqua":
            try:                 # Tk 9 (TIP 684): precision touchpads scroll
                c.bind("<TouchpadScroll>", self._on_touchpad)
            except tk.TclError:  # Tk 8.6 — the event name doesn't exist
                pass             # (aqua is excluded: the Cocoa monitor already
                                 #  routes precise scrolls — no double-pan)
        c.bind("<Shift-MouseWheel>", self._on_wheel_h)  # horizontal (trackpad)
        # Ctrl / Cmd + scroll → ALWAYS zoom (explicit, any device).
        c.bind("<Control-MouseWheel>", self._on_zoom_wheel)
        c.bind("<Command-MouseWheel>", self._on_zoom_wheel)
        c.bind("<Button-4>", lambda e: self._on_wheel_linux(+1.0, e))  # Tk 8.6
        c.bind("<Button-5>", lambda e: self._on_wheel_linux(-1.0, e))  # X11
        c.bind("<Control-Button-4>", lambda e: self._wheel(0.85, e.x, e.y))
        c.bind("<Control-Button-5>", lambda e: self._wheel(0.85 ** -1, e.x, e.y))
        def _enter_focus(_e):
            # Take focus for the chart's keyboard shortcuts — but never yank
            # it away from a text field the user is typing in.
            try:
                f = c.focus_get()
            except Exception:  # noqa: BLE001
                f = None
            if not isinstance(f, (tk.Entry, ttk.Entry, tk.Text,
                                  ttk.Combobox, tk.Spinbox, ttk.Spinbox)):
                c.focus_set()
        c.bind("<Enter>", _enter_focus)

    @staticmethod
    def _fmt_interval(td) -> str:
        """Human-readable bar interval, e.g. '30 min', '1 h', '1 d'."""
        if td is None:
            return "?"
        secs = int(pd.Timedelta(td).total_seconds())
        if secs <= 0:
            return "?"
        for unit_secs, label in ((604800, "w"), (86400, "d"),
                                 (3600, "h"), (60, "min")):
            if secs % unit_secs == 0:
                return f"{secs // unit_secs} {label}"
        return f"{secs} s"

    def set_data(self, df: Optional[pd.DataFrame]):
        self.df = df
        self._data_ver += 1            # new data invalidates the VP histogram cache
        if df is None or len(df) == 0:
            self.canvas.delete("all")
            self.base_interval = None
            self._geom = None         # stale geometry/pin/selection from the
            self._pin_ts = None       # previous dataset would ghost-readout
            self.vp_range = None      # (and a tz-mismatched pin can break
            self._sel = None          # every later render)
            return
        n = len(df)
        # Base bar interval = smallest gap between bars (robust to the larger
        # overnight / weekend gaps left by skipped non-trading hours).
        self.base_interval = None
        if n >= 2:
            diffs = np.diff(df.index.values).astype("timedelta64[s]").astype("int64")
            diffs = diffs[diffs > 0]
            if diffs.size:
                self.base_interval = pd.Timedelta(seconds=int(diffs.min()))
        self.end = n
        self.start = 0                         # default view: the whole series,
                                               # zoomed out — not a recent window
        self._pan_frac_x = 0.0                 # fresh data -> clear any pan carry
        self._manual_y = False
        self.render()

    def set_range(self, delta: Optional[pd.Timedelta]):
        if self.df is None or len(self.df) == 0:
            return
        n = len(self.df)
        if delta is None:
            self.start = 0
        else:
            start_ts = self.df.index.max() - delta
            self.start = int(self.df.index.searchsorted(start_ts, "left"))
        self.end = n
        self._manual_y = False
        self.render()

    def set_window(self, start_ts, end_ts) -> bool:
        """Show exactly [start_ts, end_ts] (timestamps). Returns False if the
        range has no bars."""
        if self.df is None or len(self.df) == 0:
            return False
        n = len(self.df)
        s = int(self.df.index.searchsorted(start_ts, "left"))
        e = int(self.df.index.searchsorted(end_ts, "right"))
        s = max(0, min(s, n - 1))
        e = max(s + 1, min(e, n))
        if e - s < 1:
            return False
        self.start, self.end = s, e
        self._manual_y = False
        self.render()
        return True

    # -- aggregation -----------------------------------------------------
    def _aggregate(self):
        df = self.df
        s, e = self.start, self.end
        vis = e - s
        k = max(1, int(np.ceil(vis / self.TARGET_CANDLES)))
        self.k = k
        if k == 1:
            sub = df.iloc[s:e]
            return (sub["open"].values, sub["high"].values, sub["low"].values,
                    sub["close"].values, sub["volume"].values, sub.index)
        # Group every k bars, anchored at absolute index so candles are stable
        # across pans. Vectorized with reduceat — fast even on ~1M rows.
        a0 = (s // k) * k
        a1 = min(len(df), ((e - 1) // k + 1) * k)
        sub = df.iloc[a0:a1]
        m = len(sub)
        if m == 0:
            return (np.array([]),) * 5 + (sub.index,)
        starts = np.arange(0, m, k)
        o = sub["open"].values[starts]
        c = sub["close"].values[np.append(starts[1:], m) - 1]
        h = np.maximum.reduceat(sub["high"].values, starts)
        l = np.minimum.reduceat(sub["low"].values, starts)
        v = np.add.reduceat(sub["volume"].values, starts)
        return o, h, l, c, v, sub.index[starts]

    @staticmethod
    def _nice_ticks(lo, hi, n=6):
        span = hi - lo
        # `not (span > 0)` also catches NaN, and inf would otherwise sail
        # through the loop leaving `step` unassigned (UnboundLocalError).
        if not (span > 0) or not np.isfinite(span):
            return [lo]
        raw = span / n
        mag = 10.0 ** np.floor(np.log10(raw))
        for mult in (1, 2, 2.5, 5, 10):
            if raw <= mult * mag:
                step = mult * mag
                break
        first = np.ceil(lo / step) * step
        return list(np.arange(first, hi + step * 0.5, step))

    @staticmethod
    def _nice_time_ticks(ts0, ts1, n=10):
        """~n nice time-boundary timestamps in [ts0, ts1], snapped to round
        steps (min / hour / day / week / …). Because they're anchored to actual
        TIME (not to fixed screen fractions), the vertical gridlines scroll WITH
        the data as you pan."""
        ts0 = pd.Timestamp(ts0)
        ts1 = pd.Timestamp(ts1)
        span = ts1.value - ts0.value
        if span <= 0:
            return [ts0]
        MIN = 60 * 10 ** 9
        HOUR = 60 * MIN
        DAY = 24 * HOUR
        steps = [MIN, 2 * MIN, 5 * MIN, 10 * MIN, 15 * MIN, 30 * MIN,
                 HOUR, 2 * HOUR, 3 * HOUR, 6 * HOUR, 12 * HOUR,
                 DAY, 2 * DAY, 7 * DAY, 14 * DAY, 30 * DAY, 90 * DAY,
                 180 * DAY, 365 * DAY]
        target = span / max(1, n)
        step = steps[-1]
        for s in steps:
            if s >= target:
                step = s
                break
        out = []
        t = (ts0.value // step + 1) * step
        while t <= ts1.value:
            out.append(pd.Timestamp(t))
            t += step
        return out

    # -- render ----------------------------------------------------------
    def render(self):
        c = self.canvas
        c.delete("all")
        if self.df is None or len(self.df) == 0:
            return
        W = c.winfo_width()
        H = c.winfo_height()
        if W < 60 or H < 60:
            return
        o, h, l, cl, v, ts = self._aggregate()
        N = len(o)
        if N == 0:
            return
        # Absolute df index each drawn candle maps to (last bar of its group —
        # matches the close). Lets overlays (EMAs) be sampled at each candle.
        kk = self.k
        _a0 = (self.start // kk) * kk
        _mm = min(len(self.df), ((self.end - 1) // kk + 1) * kk) - _a0
        _st = np.arange(0, _mm, kk)
        pos = (_a0 + (np.append(_st[1:], _mm) - 1))[:N]

        plot_w = W - self.right_margin
        price_h = (H - self.bottom_margin) * (1.0 - self.vol_frac)
        gap = 6
        vol_top = self.top_pad + price_h + gap
        vol_h = (H - self.bottom_margin) - vol_top
        if vol_h < 8:
            vol_h = 8

        if (not self._manual_y) or self.pmin is None:
            lo = float(np.min(l))
            hi = float(np.max(h))
            pad = (hi - lo) * 0.06 or 0.5
            self.pmin, self.pmax = lo - pad, hi + pad
        pmin, pmax = self.pmin, self.pmax
        # Linear or log price mapping (and its inverse for the crosshair).
        logy = self.log_scale and pmin > 0
        ya = float(np.log(pmin)) if logy else pmin
        yb = float(np.log(pmax)) if logy else pmax
        yr = (yb - ya) or 1.0
        top = self.top_pad

        def py(price):
            val = np.log(price) if (logy and price > 0) else price
            return top + (yb - val) / yr * price_h

        cw = plot_w / N
        half = max(0.5, cw * 0.74) / 2.0

        # gridlines + price labels (right) — denser / more compact
        for tp in self._nice_ticks(pmin, pmax, max(5, int(price_h / 55))):
            y = py(tp)
            if top <= y <= top + price_h:
                c.create_line(0, y, plot_w, y, fill=self.C_GRID)
                c.create_text(plot_w + 5, y, anchor="w",
                              text=_price_fmt(tp, None), fill=self.C_AXIS,
                              font=("TkDefaultFont", 8))

        # Vectorized geometry — compute every candle's x / y in one shot so the
        # draw loops below don't pay per-point Python + scalar-np.log overhead.
        xc_arr = (np.arange(N) + 0.5) * cw
        up = cl >= o

        def _py_arr(p):
            # Matches the scalar py(): log only positive prices, pass others
            # through (and avoid log(<=0) warnings on any stray bad rows).
            val = np.where(p > 0, np.log(np.where(p > 0, p, 1.0)), p) if logy else p
            return top + (yb - val) / yr * price_h

        if self.chart_type == "line":
            ys = _py_arr(cl)
            flat = np.empty(N * 2)
            flat[0::2] = xc_arr
            flat[1::2] = ys
            if N >= 2:
                c.create_line(*flat.tolist(), fill=PRICE_COLOR, width=1.4)
        else:
            yh = _py_arr(h); yl = _py_arr(l)
            yo = _py_arr(o); yc = _py_arr(cl)
            wick_w = max(1, int(min(2, cw * 0.16)))
            # Below ~3 px wide the open/close body is sub-pixel and visually
            # identical to the colored high-low wick, so skip ~N rectangles in
            # zoomed-out views (the common case) — same picture, ~half the items.
            draw_bodies = (2.0 * half) >= 3.0
            for i in range(N):
                col = UP_COLOR if up[i] else DOWN_COLOR
                x = xc_arr[i]
                c.create_line(x, yh[i], x, yl[i], fill=col, width=wick_w)
                if draw_bodies:
                    y1, y2 = yo[i], yc[i]
                    if abs(y1 - y2) < 1:
                        y2 = y1 + 1
                    c.create_rectangle(x - half, min(y1, y2), x + half,
                                       max(y1, y2), fill=col, outline=col)

        # -- price-panel overlays: EMA lines + crossover markers -------------
        df_n = len(self.df)
        for ov in self._ovl_lines:
            vals = ov.get("values")
            if vals is None or len(vals) != df_n:
                continue
            yv = np.asarray(vals)[pos]
            pts = []
            for i in range(len(pos)):
                if yv[i] == yv[i]:                  # skip NaN warm-up
                    pts += [(i + 0.5) * cw, py(float(yv[i]))]
            if len(pts) >= 4:
                c.create_line(*pts, fill=ov.get("color", "#000"),
                              width=ov.get("width", 1.2))
        for mk in self._ovl_markers:
            idx = mk.get("idx")
            yy = mk.get("y")
            if idx is None or yy is None or len(idx) == 0:
                continue
            col = mk.get("color", "#000")
            shape = mk.get("marker", "^")
            # Vectorize the visibility filter so a huge marker set (e.g. 100k+
            # signals) only loops the few hundred actually on screen.
            ia = np.asarray(idx)
            ya = np.asarray(yy, dtype=float)
            vis = (ia >= self.start) & (ia < self.end) & ~np.isnan(ya)
            if not vis.any():
                continue
            for b, yval in zip(ia[vis], ya[vis]):
                i = int((b - _a0) // kk)
                if 0 <= i < N:
                    self._draw_tri((i + 0.5) * cw, py(float(yval)), shape, col)
        if self._ovl_lines:                          # EMA legend, top-left
            ly = 46
            for ov in self._ovl_lines:
                lab = ov.get("label")
                if not lab:
                    continue
                c.create_line(8, ly, 26, ly, fill=ov.get("color", "#000"),
                              width=2)
                c.create_text(30, ly, anchor="w", text=lab, fill=self.C_AXIS,
                              font=("TkDefaultFont", 8))
                ly += 14

        # volume bars (reuse the precomputed x-centers / direction)
        vmax = float(np.max(v)) or 1.0
        vbase = vol_top + vol_h
        vtop = vbase - v / vmax * vol_h
        for i in range(N):
            col = UP_COLOR if up[i] else DOWN_COLOR
            c.create_rectangle(xc_arr[i] - half, vtop[i],
                               xc_arr[i] + half, vbase, fill=col, outline="")

        # volume MA line + spike markers
        n_spikes = self._draw_vol_overlays(c, N, cw, v, vmax, vol_top, vol_h)

        # time gridlines (bottom) — anchored to nice TIME boundaries so they
        # scroll WITH the data when panning (instead of being pinned to fixed
        # screen fractions), and denser / more compact.
        fmt = "%m/%d %H:%M" if (ts[-1] - ts[0]) < pd.Timedelta(days=5) \
            else "%Y-%m-%d"
        last_i = -10
        for tt in self._nice_time_ticks(ts[0], ts[-1], max(4, int(plot_w / 110))):
            i = int(np.clip(np.searchsorted(ts, tt), 0, N - 1))
            if i <= last_i:
                continue                        # at most one gridline per candle
            last_i = i
            xc = (i + 0.5) * cw
            c.create_line(xc, top, xc, vol_top + vol_h, fill=self.C_GRID)
            c.create_text(xc, H - self.bottom_margin + 4, anchor="n",
                          text=pd.Timestamp(tt).strftime(fmt), fill=self.C_AXIS,
                          font=("TkDefaultFont", 8))

        self._geom = dict(N=N, cw=cw, plot_w=plot_w, price_h=price_h,
                          vol_top=vol_top, vol_h=vol_h, pmin=pmin, pmax=pmax,
                          o=o, h=h, l=l, c=cl, v=v, ts=ts, H=H, pos=pos,
                          logy=logy, ya=ya, yb=yb, yr=yr, top=top, py=py)

        # Fixed-range volume profile (after geom so it can map positions).
        vp_status = ""
        if self.vp_on and self.vp_range is not None:
            vp_status = self._draw_vp(c)
        elif self.vp_on:
            vp_status = "VP on — drag across the chart to select a range"

        if self.status is not None:
            base_str = self._fmt_interval(self.base_interval)
            if self.k > 1 and self.base_interval is not None:
                eff_str = self._fmt_interval(self.base_interval * self.k)
                interval_tok = f"Interval: {eff_str} ({self.k}×{base_str})"
            else:
                interval_tok = f"Interval: {base_str}"
            msg = (f"{interval_tok} · {(self.end - self.start):,} bars · "
                   f"{N} candles · {n_spikes} spike(s) · "
                   f"{ts[0]:%Y-%m-%d %H:%M} → {ts[-1]:%Y-%m-%d %H:%M}")
            if vp_status:
                msg += f"  ·  {vp_status}"
            self.status.set(msg)

        # Persistent click-pinned inspector (survives pan/zoom/re-render).
        self._draw_pin()

    # -- click-to-pin inspector ------------------------------------------
    def zoom(self, factor):
        """Zoom toward the chart centre. factor < 1 zooms IN, > 1 zooms OUT."""
        if self._geom is None:
            return
        self._wheel(factor, self._geom["plot_w"] / 2.0, 0)

    def pan(self, dx_px, dy_px=0.0):
        """Pan the view by a pixel offset — like grabbing and dragging the chart.
        dx_px > 0 slides the content RIGHT (reveals earlier bars); dy_px > 0
        slides it DOWN (reveals higher prices). Pass both at once to pan in ANY
        direction. The two-finger trackpad swipe drives this, and feature code
        can call it directly. Returns self."""
        if self.df is None or self._geom is None:
            return self
        g = self._geom
        n = len(self.df)
        moved = False
        if dx_px:                               # horizontal: scale with the view
            vis = self.end - self.start
            if vis > 0:
                # Pan a fixed FRACTION of the visible range per swipe — a swipe of
                # _PAN_REF_WIDTH px moves one full window — so it scales with the
                # zoom level (fast when zoomed out, fine when zoomed in) and is
                # decoupled from the chart's pixel width, so every tab feels the
                # same regardless of its layout. PAN_SPEED is the one speed knob.
                # The carried remainder keeps slow swipes smooth instead of
                # rounding sub-bar motion away.
                self._pan_frac_x += -dx_px * PAN_SPEED * vis / _PAN_REF_WIDTH
                dbars = int(self._pan_frac_x)       # whole bars, truncated toward 0
                self._pan_frac_x -= dbars           # keep the sub-bar remainder
                if dbars:
                    ns, ne = self.start + dbars, self.end + dbars
                    if ns < 0:
                        ns, ne = 0, vis
                        self._pan_frac_x = 0.0      # pinned at edge -> drop carry
                    if ne > n:
                        ns, ne = n - vis, n
                        self._pan_frac_x = 0.0
                    self.start, self.end = max(0, ns), min(n, ne)
                    moved = True
        ph = g.get("price_h", 0.0)
        if dy_px and self.pmin is not None and ph > 0:   # vertical: scale with view
            # Same idea on price: move a fraction of the visible price range,
            # scaled by the same PAN_SPEED knob so both axes share one speed.
            dprice = dy_px * PAN_SPEED / ph * (self.pmax - self.pmin)
            self.pmin += dprice
            self.pmax += dprice
            self._manual_y = True
            moved = True
        if moved:
            self.render()
        return self

    # ====================================================================
    # PUBLIC API — the documented surface feature code should use. Feature
    # tabs drive the chart ONLY through these (plus set_data / set_range /
    # set_window / zoom / pan / render) and never touch a `_name`, so the
    # engine internals stay free to change without breaking any tab.
    # ====================================================================

    # Friendly option name -> the internal attribute it drives.
    _CONFIG_MAP = {
        "kind":             "chart_type",     # "candle" | "line"
        "log":              "log_scale",
        "spikes":           "show_spikes",
        "spike_mult":       "spike_mult",
        "volume_ma":        "show_vma",
        "volume_ma_period": "vma_period",
        "volume_profile":   "vp_on",
        "vp_rows":          "vp_rows",
        "vp_va":            "vp_va",
        "vp_range":         "vp_range",
    }

    @property
    def data(self):
        """The current OHLCV frame (or None). Read-only — load via set_data()."""
        return self.df

    def visible_window(self):
        """(start_ts, end_ts) of the bars currently on screen, or None if empty."""
        if self.df is None or len(self.df) == 0:
            return None
        s = max(0, min(self.start, len(self.df) - 1))
        e = max(s, min(self.end, len(self.df)) - 1)
        return self.df.index[s], self.df.index[e]

    def configure(self, render: bool = True, **opts):
        """Set display options by friendly name, e.g.
        configure(kind="candle", spikes=False, log=True). Pass render=False to
        batch several changes (or pair with set_data) and draw once afterwards."""
        unknown = set(opts) - set(self._CONFIG_MAP)
        if unknown:
            raise KeyError(f"unknown chart option(s): {sorted(unknown)}; "
                           f"valid: {sorted(self._CONFIG_MAP)}")
        for name, value in opts.items():
            setattr(self, self._CONFIG_MAP[name], value)
        if render:
            self.render()
        return self

    def set_lines(self, lines, render: bool = True):
        """Replace all price-panel overlay lines. Each is a dict
        {values, color, width, label} where `values` is a full-length
        (len == len(data)) array sampled at each bar."""
        self._ovl_lines = list(lines)
        if render:
            self.render()
        return self

    def add_line(self, values, color, width=1.2, label="", render: bool = True):
        self._ovl_lines.append({"values": values, "color": color,
                                "width": width, "label": label})
        if render:
            self.render()
        return self

    def set_markers(self, marker_sets, render: bool = True):
        """Replace all marker sets. Each is a dict {idx, y, marker '^'/'v',
        color} placing a triangle at the given bar indices / prices."""
        self._ovl_markers = list(marker_sets)
        if render:
            self.render()
        return self

    def add_markers(self, idx, y, shape="^", color="#26A69A", render: bool = True):
        self._ovl_markers.append({"idx": list(idx), "y": list(y),
                                  "marker": shape, "color": color})
        if render:
            self.render()
        return self

    def clear_overlays(self, render: bool = True):
        """Drop every overlay line and marker set."""
        self._ovl_lines = []
        self._ovl_markers = []
        if render:
            self.render()
        return self

    # Pixel <-> data mapping for features that hand-draw on the canvas. Each
    # needs a prior render() (the geometry must exist) and returns None if not.
    def price_to_y(self, price):
        if self._geom is None:
            return None
        return float(self._geom["py"](price))

    def y_to_price(self, y):
        if self._geom is None:
            return None
        g = self._geom
        val = g["yb"] - (y - g["top"]) / g["price_h"] * g["yr"]
        return float(np.exp(val)) if g["logy"] else float(val)

    def bar_to_x(self, idx):
        if self._geom is None:
            return None
        return float((idx - self.start + 0.5) * self._geom["cw"])

    def x_to_bar(self, x):
        if self._geom is None:
            return None
        return int(self.start + x // self._geom["cw"])

    def _toggle_pin(self, px):
        """Click handler: pin (or unpin) the OHLC inspector at bar under px."""
        g = self._geom
        if g is None or g["cw"] <= 0:
            return
        if px < 0 or px > g["plot_w"]:
            self._pin_ts = None
            self.render()
            return
        i = int(px / g["cw"])
        if i < 0 or i >= g["N"]:
            return
        ts = g["ts"][i]
        # Clicking the same bar again clears the pin.
        if self._pin_ts is not None and self._pin_ts == ts:
            self._pin_ts = None
        else:
            self._pin_ts = ts
        self.render()

    def _draw_pin(self):
        """Draw the pinned vertical line + a persistent OHLC readout box."""
        if self._pin_ts is None or self._geom is None:
            return
        g = self._geom
        ts = g["ts"]
        if self._pin_ts < ts[0] or self._pin_ts > ts[-1]:
            return                                  # pinned bar scrolled away
        pos = int(np.clip(np.searchsorted(ts, self._pin_ts), 0, g["N"] - 1))
        c = self.canvas
        c.delete("pin")
        xc = (pos + 0.5) * g["cw"]
        c.create_line(xc, self.top_pad, xc, g["vol_top"] + g["vol_h"],
                      fill=self.C_POC, width=1.5, tags="pin")
        o, hh, ll, cc, vv = (g["o"][pos], g["h"][pos], g["l"][pos],
                             g["c"][pos], g["v"][pos])
        chg = cc - o
        pct = (chg / o * 100.0) if o else 0.0
        txt = (f"PINNED {g['ts'][pos]:%Y-%m-%d %H:%M}    "
               f"O {o:,.2f}   H {hh:,.2f}   L {ll:,.2f}   C {cc:,.2f}   "
               f"V {_volume_fmt(float(vv), None)}   {chg:+,.2f} ({pct:+.2f}%)")
        # Background box so it stays readable over candles (top-left, 2nd line).
        c.create_rectangle(4, 20, 8 + len(txt) * 6.0, 36,
                           fill=self.C_BOXBG, outline=self.C_BOXEDGE, tags="pin")
        c.create_text(8, 28, anchor="w", text=txt,
                      fill=(UP_COLOR if chg >= 0 else DOWN_COLOR),
                      font=("TkDefaultFont", 9), tags="pin")

    def _draw_tri(self, x, y, shape, color):
        """A small crossover triangle ('^' up / 'v' down) centred at (x, y)."""
        s = 5
        if shape == "v":
            pts = (x - s, y - s, x + s, y - s, x, y + s)
        else:
            pts = (x - s, y + s, x + s, y + s, x, y - s)
        self.canvas.create_polygon(*pts, fill=color, outline="#3a3a3a", width=1)

    # -- overlays --------------------------------------------------------
    def _draw_vol_overlays(self, c, N, cw, v, vmax, vol_top, vol_h):
        if not (self.show_vma or self.show_spikes) or N < 2:
            return 0
        period = max(2, int(self.vma_period or 20))
        ma = pd.Series(v).rolling(period, min_periods=1).mean().values
        if self.show_vma:
            pts = []
            for i in range(N):
                pts += [(i + 0.5) * cw,
                        vol_top + vol_h - (ma[i] / vmax) * vol_h]
            if len(pts) >= 4:
                c.create_line(*pts, fill=self.C_MA, width=1.3)
        n_spikes = 0
        if self.show_spikes:
            mult = float(self.spike_mult or 2.0)
            guides = 0
            for i in range(N):
                if ma[i] > 0 and v[i] >= ma[i] * mult:
                    n_spikes += 1
                    xc = (i + 0.5) * cw
                    yt = vol_top + vol_h - (v[i] / vmax) * vol_h
                    c.create_polygon(xc - 3, yt - 8, xc + 3, yt - 8, xc, yt - 1,
                                     fill=self.C_SPIKE, outline="#BF360C")
                    if guides < 80:
                        c.create_line(xc, self.top_pad, xc, vol_top - 3,
                                      fill="#FF7043", dash=(2, 5))
                        guides += 1
        return n_spikes

    def _draw_vp(self, c) -> str:
        g = self._geom
        ts_view = g["ts"]
        py = g["py"]
        N, cw, plot_w = g["N"], g["cw"], g["plot_w"]
        ts0, ts1 = self.vp_range
        if ts1 < ts0:
            ts0, ts1 = ts1, ts0
        # The histogram depends only on the selected range, the data, and the
        # row/VA settings — NOT on the pan/zoom view — so cache it and skip the
        # (expensive) re-bin + sort on every pan frame. Only the cheap image
        # placement below re-runs as the view moves.
        vp_key = (ts0, ts1, int(self.vp_rows), float(self.vp_va), self._data_ver)
        if self._vp_cache is not None and self._vp_cache[0] == vp_key:
            prof, buy, sell, sub_len, pmn, pmx, largest_gap = self._vp_cache[1]
        else:
            sub = self.df.loc[ts0:ts1]
            prof = compute_volume_profile(sub, self.vp_rows, float(self.vp_va))
            if prof is None:
                return "VP: range too small"
            buy = float(prof["up"].sum())
            sell = float(prof["down"].sum())
            sub_len = len(sub)
            # Range-only stats for the empty-bin warning (view-independent too).
            typ = np.sort((sub["high"].values + sub["low"].values
                           + sub["close"].values) / 3.0)
            pmn = float(sub["low"].min())
            pmx = float(sub["high"].max())
            largest_gap = float(np.max(np.diff(
                np.concatenate(([pmn], typ, [pmx]))))) if len(typ) else 0.0
            self._vp_cache = (vp_key,
                              (prof, buy, sell, sub_len, pmn, pmx, largest_gap))
        if ts1 < ts_view[0] or ts0 > ts_view[-1]:
            return (f"VP: {sub_len:,} bars off-screen · "
                    f"POC {prof['poc_price']:.2f}")
        i0 = int(np.clip(np.searchsorted(ts_view, ts0, "left"), 0, N - 1))
        i1 = int(np.clip(np.searchsorted(ts_view, ts1, "right") - 1, 0, N - 1))
        if i1 < i0:
            i1 = i0
        left_x = i0 * cw
        right_x = min(plot_w, (i1 + 1) * cw)
        span_x = max(6.0, right_x - left_x)
        # Mark the selected range with thin boundary lines only — NO fill, so
        # the candles/values inside stay fully visible.
        for bx in (left_x, right_x):
            c.create_line(bx, g["top"], bx, g["vol_top"] + g["vol_h"],
                          fill=self.C_BOXEDGE)

        edges = prof["edges"]
        up_h, down_h, total = prof["up"], prof["down"], prof["total"]
        centers = (edges[:-1] + edges[1:]) / 2.0
        in_va = (centers >= prof["val"]) & (centers <= prof["vah"])
        max_total = total.max() or 1.0
        profile_w = 0.40 * span_x
        row_px = abs(py(edges[0]) - py(edges[1]))
        show_labels = row_px >= 10
        fsize = int(np.clip(row_px * 0.4, 6, 8))   # smaller label font
        # Draw the buy/sell bars. tkinter's Canvas can't do per-shape
        # transparency (no alpha colors; stipple is a no-op on macOS), so when
        # Pillow is available we composite the whole histogram as ONE
        # alpha-blended RGBA image — the candles show THROUGH it. In-value-area
        # rows are a touch more opaque so the VA still reads. No Pillow → solid.
        if _HAVE_PIL:
            ix0, iy0 = int(left_x), int(g["top"])
            iw, ih = max(1, int(profile_w) + 2), max(1, int(g["price_h"]) + 2)

            def _rgb(hx):
                hx = hx.lstrip("#")
                return (int(hx[0:2], 16), int(hx[2:4], 16), int(hx[4:6], 16))
            buy_rgb, sell_rgb = _rgb(self.C_BUY), _rgb(self.C_SELL)
            img = _PILImage.new("RGBA", (iw, ih), (0, 0, 0, 0))
            draw = _PILImageDraw.Draw(img)
            for bi in range(len(centers)):
                yc = py(centers[bi])
                bh = abs(py(edges[bi]) - py(edges[bi + 1])) * 0.9
                uw = up_h[bi] / max_total * profile_w
                dw = down_h[bi] / max_total * profile_w
                a = 150 if in_va[bi] else 105            # alpha, 0–255
                yt, yb = yc - bh / 2 - iy0, yc + bh / 2 - iy0
                if uw > 0.5:
                    draw.rectangle([left_x - ix0, yt, left_x + uw - ix0, yb],
                                   fill=buy_rgb + (a,))
                if dw > 0.5:
                    draw.rectangle([left_x + uw - ix0, yt,
                                    left_x + uw + dw - ix0, yb],
                                   fill=sell_rgb + (a,))
            self._vp_photo = _PILImageTk.PhotoImage(img)   # keep a ref (no GC)
            c.create_image(ix0, iy0, image=self._vp_photo, anchor="nw")
        else:
            for bi in range(len(centers)):
                yc = py(centers[bi])
                bh = abs(py(edges[bi]) - py(edges[bi + 1])) * 0.9
                uw = up_h[bi] / max_total * profile_w
                dw = down_h[bi] / max_total * profile_w
                if uw > 0.5:
                    c.create_rectangle(left_x, yc - bh / 2, left_x + uw,
                                       yc + bh / 2, fill=self.C_BUY, outline="")
                if dw > 0.5:
                    c.create_rectangle(left_x + uw, yc - bh / 2,
                                       left_x + uw + dw, yc + bh / 2,
                                       fill=self.C_SELL, outline="")

        # Per-row buy/sell numbers, drawn ON TOP of the bars. Buy (dark blue)
        # then sell (red) sit adjacent; the buy text's measured width places the
        # sell text so neither overhangs to the left of the bar.
        if show_labels:
            lf = ("TkDefaultFont", fsize, "bold")
            for bi in range(len(centers)):
                yc = py(centers[bi])
                x = left_x + 2
                if up_h[bi] > 0:
                    s = (f"{_compact_2dp(up_h[bi])}/" if down_h[bi] > 0
                         else _compact_2dp(up_h[bi]))
                    it = c.create_text(x, yc, anchor="w", text=s,
                                       fill="#0D47A1", font=lf, tags="vplabel")
                    bb = c.bbox(it)
                    x = bb[2] if bb else x + len(s) * fsize * 0.6
                if down_h[bi] > 0:
                    c.create_text(x, yc, anchor="w",
                                  text=_compact_2dp(down_h[bi]), fill="#C62828",
                                  font=lf, tags="vplabel")
        # POC + value-area lines
        yp = py(prof["poc_price"])
        c.create_line(left_x, yp, right_x, yp, fill=self.C_POC, width=2)
        for lvl in (prof["vah"], prof["val"]):
            yl = py(lvl)
            c.create_line(left_x, yl, right_x, yl, fill=self.C_VA, dash=(5, 3))
        # Keep the per-row numbers on top of the bars and the POC/VA lines.
        c.tag_raise("vplabel")
        # totals box, top-right
        grand = (buy + sell) or 1.0
        bx, by = plot_w - 232, 8
        c.create_rectangle(bx, by, plot_w - 6, by + 62, fill=self.C_BOXBG,
                           outline=self.C_BOXEDGE)
        c.create_text(bx + 8, by + 6, anchor="nw",
                      text="Selected-range volume", fill=self.C_AXIS,
                      font=("TkDefaultFont", 8))
        c.create_text(bx + 8, by + 22, anchor="nw",
                      text=f"Buy  {_volume_fmt(buy, None):>8}  {100*buy/grand:.0f}%",
                      fill=self.C_BUY, font=("TkDefaultFont", 9))
        c.create_text(bx + 8, by + 40, anchor="nw",
                      text=f"Sell {_volume_fmt(sell, None):>8}  {100*sell/grand:.0f}%",
                      fill=self.C_SELL, font=("TkDefaultFont", 9))

        # Empty-bin warning: outlier wicks can stretch the price range so many
        # bins fall in empty zones (no bar). The biggest gap in the traded
        # prices sets the max rows where every bin still gets data.
        rows_req = int(self.vp_rows)
        rng = pmx - pmn
        max_rows = rows_req
        if rng > 0 and largest_gap > 0:
            max_rows = max(1, int(rng / largest_gap))
        warn = ""
        if rows_req > max_rows:
            warn = (f"⚠ Not enough data for {rows_req} rows — "
                    f"max ~{max_rows} rows for this range")
            c.create_text(plot_w / 2, g["top"] + 3, anchor="n", text=warn,
                          fill=self.C_WARN, font=("TkDefaultFont", 10, "bold"),
                          tags="vpwarn")

        base = (f"VP: {sub_len:,} bars · POC {prof['poc_price']:.2f} · "
                f"VA{int(self.vp_va)}% {prof['val']:.2f}–{prof['vah']:.2f}")
        return f"{base}   ·   {warn}" if warn else base

    # -- interaction -----------------------------------------------------
    def _on_press(self, e):
        self._moved = 0.0
        if self._geom is None:
            return
        if self.vp_on:
            # Volume Profile mode → drag selects a range instead of panning.
            self._sel = dict(x0=e.x)
            return
        self._drag = dict(x=e.x, y=e.y, start=self.start, end=self.end,
                          pmin=self.pmin, pmax=self.pmax,
                          cw=self._geom["cw"], k=self.k)
        self.canvas.config(cursor="fleur")

    def _on_release(self, e):
        if self._sel is not None:
            x0 = self._sel["x0"]
            x1 = e.x
            self._sel = None
            self.canvas.delete("select")
            g = self._geom
            if g and abs(x1 - x0) > 3 and g["cw"] > 0:
                i0 = int(np.clip(int(min(x0, x1) / g["cw"]), 0, g["N"] - 1))
                i1 = int(np.clip(int(max(x0, x1) / g["cw"]), 0, g["N"] - 1))
                ts = g["ts"]
                # With aggregated candles (k>1) a candle's timestamp is its
                # FIRST raw bar — slicing to ts[i1] would drop the candle's
                # remaining bars, so end just before the NEXT candle starts.
                if i1 + 1 < g["N"]:
                    t1 = pd.Timestamp(ts[i1 + 1]) - pd.Timedelta(nanoseconds=1)
                else:
                    t1 = self.df.index.max()
                self.vp_range = (pd.Timestamp(ts[i0]), t1)
                self.render()
            return
        d = self._drag
        self._drag = None
        self.canvas.config(cursor="")
        # A click / light tap pins the OHLC inspector. Treat the gesture as a TAP
        # unless the finger actually travelled a meaningful distance while held
        # (MAX movement, not just the press→release delta — a soft tap can drift
        # out and back). This stops a light trackpad click being misread as drag.
        tap = (d is None) or (self._moved <= 8)
        if tap:
            self._toggle_pin(e.x)

    def _on_drag(self, e):
        d0 = self._drag
        if d0 is not None:
            self._moved = max(self._moved, abs(e.x - d0["x"]),
                              abs(e.y - d0["y"]))
        if self._sel is not None and self._geom is not None:
            self.canvas.delete("select")
            g = self._geom
            x0 = self._sel["x0"]
            self.canvas.create_rectangle(
                min(x0, e.x), self.top_pad, max(x0, e.x),
                g["vol_top"] + g["vol_h"], outline=self.C_CROSS,
                dash=(3, 3), fill="", tags="select")
            return
        if self._drag is None or self.df is None:
            return
        if self._moved <= 8:        # ignore sub-threshold jitter → it's a tap
            return
        now = time.monotonic()
        if now - self._last < 0.012:
            return
        self._last = now
        d = self._drag
        n = len(self.df)
        vis = d["end"] - d["start"]
        # horizontal: pixels → bars (cw px per displayed candle = k bars)
        if d["cw"] > 0:
            dbars = int(round(-(e.x - d["x"]) * d["k"] / d["cw"]))
        else:
            dbars = 0
        ns, ne = d["start"] + dbars, d["start"] + dbars + vis
        if ns < 0:
            ns, ne = 0, vis
        if ne > n:
            ns, ne = n - vis, n
        self.start, self.end = max(0, ns), min(n, ne)
        # vertical: pixels → price (grab — content follows the cursor)
        if self._geom and self._geom["price_h"] > 0:
            dprice = (e.y - d["y"]) / self._geom["price_h"] \
                * (d["pmax"] - d["pmin"])
            self.pmin = d["pmin"] + dprice
            self.pmax = d["pmax"] + dprice
            self._manual_y = True
        self.render()

    @staticmethod
    def _scroll_is_trackpad(e):
        """True → trackpad (pan), False → wheel mouse (zoom). macOS uses the
        native precise-scroll flag; elsewhere falls back to the delta heuristic
        (a real mouse wheel ticks in multiples of 120)."""
        p = _SCROLL_PRECISE[0]
        if p is True:
            return True
        if p is False:
            return False
        d = abs(getattr(e, "delta", 0))
        return not (d >= 120 and d % 120 == 0)

    @staticmethod
    def _step(e):
        d = getattr(e, "delta", 0)
        if d == 0:
            return 0.0
        return d / 120.0 if abs(d) >= 30 else (1.0 if d > 0 else -1.0)

    def _on_wheel(self, e):
        step = self._step(e)
        if step == 0:
            return "break"
        if self._scroll_is_trackpad(e):
            self._pan_price(step)          # trackpad vertical → pan price
        else:
            self._wheel(0.85 ** step, e.x, e.y)   # mouse wheel → zoom
        return "break"

    def _on_wheel_h(self, e):
        step = self._step(e)
        if step != 0:
            self._pan_time(step)           # horizontal swipe → pan time
        return "break"

    def _on_zoom_wheel(self, e):
        step = self._step(e)
        if step != 0:
            self._wheel(0.85 ** step, e.x, e.y)
        return "break"

    def _on_wheel_linux(self, step, e):
        # X11 plain scroll, BOTH Tk generations (Button-4/5 on 8.6,
        # <MouseWheel> on 9). Linux can't tell trackpad from mouse → default
        # to pan price; zoom is always available on Ctrl+scroll.
        self._pan_price(step)
        return "break"

    def _on_touchpad(self, e):
        # Tk 9 <TouchpadScroll> (TIP 684) — Windows/X11 precision touchpads
        # that bypass <MouseWheel>. delta packs both axes: high 16 bits = X,
        # low 16 = Y, signed, in ~pixel-sized steps. A touchpad PANS (the
        # same default as the macOS monitor and the X11 wheel path); zoom
        # stays on Ctrl+scroll. /30 ≈ one pan step per ~30 px of swipe —
        # confirm the feel (and sign) on a real Linux/Windows box.
        d = int(getattr(e, "delta", 0))
        dx, dy = (d >> 16) & 0xFFFF, d & 0xFFFF
        if dx >= 0x8000:
            dx -= 0x10000
        if dy >= 0x8000:
            dy -= 0x10000
        if dy:
            self._pan_price(dy / 30.0)
        if dx:
            self._pan_time(dx / 30.0)
        return "break"

    def _pan_price(self, step):                 # trackpad vertical swipe → price
        if self._geom is not None:
            self.pan(0.0, step * 0.04 * self._geom.get("price_h", 0.0))

    def _pan_time(self, step):                  # trackpad horizontal swipe → time
        g = self._geom
        if g is None:
            return
        cw = g.get("cw", 0.0)
        dx = step * 0.05 * g.get("plot_w", 0.0)
        if cw > 0 and 0 < abs(dx) < cw:         # keep a tiny swipe responsive
            dx = cw if dx > 0 else -cw
        self.pan(dx, 0.0)

    def _wheel(self, factor, px, py):
        # `factor` < 1 zooms in, > 1 zooms out (callers pass e.g. 0.85**step).
        if self.df is None or self._geom is None:
            return
        g = self._geom
        n = len(self.df)
        vis = self.end - self.start
        fracx = min(1.0, max(0.0, px / g["plot_w"])) if g["plot_w"] > 0 else 0.5
        cur = self.start + fracx * vis
        new_vis = max(10.0, vis * factor)
        ns = int(round(cur - fracx * new_vis))
        ne = int(round(ns + new_vis))
        ns = max(0, ns)
        ne = min(n, ne)
        if ne - ns < 6:
            return
        self.start, self.end = ns, ne
        # Auto-fit the price axis to the newly-visible data so the chart never
        # goes flat when zooming out (y stays proportional to the price range).
        self._manual_y = False
        self.render()

    def _on_motion(self, e):
        c = self.canvas
        c.delete("cross")
        g = self._geom
        if g is None:
            return
        if e.x < 0 or e.x > g["plot_w"] or g["cw"] <= 0:
            return                    # cw==0: 60-66px-wide canvas edge case
        i = int(e.x / g["cw"])
        if i < 0 or i >= g["N"]:
            return
        xc = (i + 0.5) * g["cw"]
        c.create_line(xc, self.top_pad, xc, g["vol_top"] + g["vol_h"],
                      fill=self.C_CROSS, dash=(3, 3), tags="cross")
        c.create_line(0, e.y, g["plot_w"], e.y, fill=self.C_CROSS, dash=(3, 3),
                      tags="cross")
        # price tag at the cursor height — a small box in the RIGHT MARGIN only
        # (the right edge is the canvas WIDTH, not the height).
        if e.y <= self.top_pad + g["price_h"]:
            val = g["yb"] - (e.y - g["top"]) / g["price_h"] * g["yr"]
            price = float(np.exp(val)) if g["logy"] else val
            right_edge = g["plot_w"] + self.right_margin
            c.create_rectangle(g["plot_w"], e.y - 8, right_edge, e.y + 8,
                               fill=PRICE_COLOR, outline="", tags="cross")
            c.create_text(g["plot_w"] + 4, e.y, anchor="w",
                          text=f"{price:,.2f}", fill="#FFFFFF",
                          font=("TkDefaultFont", 8), tags="cross")
        # OHLC readout (+ EMA values at this bar), top-left
        o, hh, ll, cc, vv = (g["o"][i], g["h"][i], g["l"][i],
                             g["c"][i], g["v"][i])
        chg = cc - o
        col = UP_COLOR if chg >= 0 else DOWN_COLOR
        txt = (f"{g['ts'][i]:%Y-%m-%d %H:%M}   O {o:.2f}  H {hh:.2f}  "
               f"L {ll:.2f}  C {cc:.2f}   V {_volume_fmt(float(vv), None)}   "
               f"{chg:+.2f}")
        gp = g.get("pos")
        if self._ovl_lines and gp is not None and i < len(gp):
            bits = []
            for ov in self._ovl_lines:
                lab = ov.get("label")
                vals = ov.get("values")
                if not lab or vals is None or len(vals) != len(self.df):
                    continue
                ev = float(vals[gp[i]])
                if ev == ev:
                    bits.append(f"{lab} {ev:.2f}")
            if bits:
                txt += "    " + "  ".join(bits)
        c.create_text(8, 6, anchor="nw", text=txt, fill=col,
                      font=("TkDefaultFont", 9), tags="cross")


# ---------------------------------------------------------------------------
# Trade-book integrity check — public API
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Export organization — public API
# ---------------------------------------------------------------------------






def _stock_find_local(root, text):
    """Find/add-stock dialog's OFFLINE matcher (worker-thread side,
    GUI-free): every ticker directory whose SYMBOL, manifest "name" or
    any alias contains `text` case-insensitively, each as a search-row
    dict whose "stored" summary comes straight from the manifest's
    month keys per interval ("1m 2024-01..2024-03, 5m ..."). An empty
    `text` matches everything (that is how remote matches get their
    in-bank annotation). Never raises — an unreadable dir or corrupt
    manifest is skipped and the rest of the bank still answers. Pure
    logic at module level ON PURPOSE — the headless smoke test runs it
    directly; it must NEVER touch tkinter."""
    needle = str(text or "").strip().lower()
    out = []
    try:
        dirs = sorted(Path(root).iterdir())
    except OSError:
        return out
    for d in dirs:
        try:
            if not (d.is_dir()
                    and stock_storage.TICKER_DIR_RE.match(d.name)):
                continue
            man = stock_storage.load_manifest(d) or {}
            name = str(man.get("name") or "")
            aliases = man.get("aliases") or []
            if not isinstance(aliases, (list, tuple, set)):
                aliases = [aliases]   # hand-edited scalar — tolerate
            hay = [d.name, name, *(str(a) for a in aliases)]
            if not any(needle in h.lower() for h in hay):
                continue
            stored = []
            coverage = {}        # per-interval span so the UI can show ONE iv
            for iv, ivd in sorted((man.get("intervals") or {}).items()):
                mdict = (ivd or {}).get("months") or {}
                # match the database preview's Coverage column: the
                # FIRST..LAST bar across the present months (not just the
                # month keys), using the same manifest first/last fields
                present = sorted(k for k, v in mdict.items()
                                 if isinstance(v, dict)
                                 and v.get("status") == "present")
                if not present:
                    present = sorted(mdict)          # older manifest
                if not present:
                    continue
                fm, lm = mdict.get(present[0]) or {}, \
                    mdict.get(present[-1]) or {}
                first = fm.get("first") or present[0]
                last = lm.get("last") or present[-1]
                stored.append(f"{iv}  {first}  →  {last}")
                coverage[iv] = f"{first}  →  {last}"
            out.append({"symbol": d.name, "name": name, "exchange": "",
                        "currency": "", "stored": "   ".join(stored),
                        "coverage": coverage})
        except (OSError, stock_storage.StorageError):
            continue              # unreadable entry — skip, keep going
        except Exception:  # noqa: BLE001 — one weird manifest must not
            continue              # take the whole search down
    return out


# Wall-clock seconds per IBKR historical request, INTERVAL-AWARE (measured
# live 2026-06-15, mirrors stock_ibkr.Pacer's two regimes):
#   * MINUTE+ bars are served from IBKR's cache and BYPASS the 60/10-min
#     HMDS limit — sustained ~26/min, latency-bound -> ~2.25 s/request.
#   * SUB-MINUTE bars (1s..30s) are genuine HMDS hits, capped at exactly
#     60 requests / 10 min -> ~10 s/request effective (the metered budget).
# The 60/10-min budget is PER ACCOUNT, so a batch SUMS seconds across series
# (then divides by the parallel lanes). qualify/head are OFF the metered budget.
# RECALIBRATED 2026-06-23 after the fetch-speed work:
#   · 1m now fetches a SMALL "1 W" span (~1950 bars, was a "1 M" ~8190-bar fetch).
#     The demo's per-request cost is super-linear in BAR COUNT, so a "1 W" fetch
#     is ~0.2-1s + ~0.4s local pack + the burst min-gap ≈ ~1.5 s (the old ~15 s
#     was the big "1 M" fetch). Request COUNT rose ~4x but each is ~10x cheaper.
#   · Ports parallelize ~LINEARLY (the 2026-06-23 choke-hunt: N=5 sustained = 0
#     drops; the old "demo serializes HMDS, ≈ serial" belief was DISPROVEN), with
#     ~10% per-port contention -> cap 4.5, not 1.5.
#   · EXTENDED hours combine: {rth, pre, post} fetch the unfiltered useRTH=False
#     payload ONCE per span (see _estimate_worktime), so the estimate counts the
#     union ONCE, not 3x.
_EST_SECS_PER_REQ_MINUTE = 1.5            # small "1 W" minute fetch + pack + gap
_EST_SECS_PER_REQ_SECOND = 12.0          # 1s/5s/… still HMDS-metered (~10 s floor)
_EST_QUALIFY_HEAD_SECS = 3.0             # fixed qualify+head per series
_EST_MAX_PARALLEL = 4.5                  # effective parallelism at the 5-port
#                          fleet (~linear to 5; the choke-hunt proved no
#                          concurrency choke, ~10% per-port contention margin).
_EST_MARGINAL_PARALLEL = 0.33            # each port PAST 5 adds only ~0.33x —
#                          measured 2026-06-24: 8 ports ≈ 1.22x of 5 (diminishing
#                          returns). So more ports still shorten the estimate,
#                          just sub-linearly; still bounded by the ticker count.
_EST_DEFAULT_BACKFILL_DAYS = 365 * 3      # 'Earliest traceable' size is unknown
#                          offline; assume ~3 yr so the estimate isn't ~0.


def _est_secs_per_request(interval):
    """Interval-aware seconds/request: sub-minute ('…s') bars are HMDS-metered
    (~10-12 s), minute+ bars now fetch a small "1 W" span (~1.5 s incl. pack +
    burst gap). Keys off the BASE interval so '1m-pre'/'1s-post' classify right."""
    return (_EST_SECS_PER_REQ_SECOND
            if stock_storage.base_interval(str(interval)).endswith("s")
            else _EST_SECS_PER_REQ_MINUTE)


def _est_stored_days(manifest, iv, start, today):
    """Set of trading days in [start, today] ALREADY stored on disk for `iv` — every
    PRESENT manifest month, bounded by that month's recorded first/last bar so a
    partially-filled boundary month isn't over-credited. Pure/offline; mirrors
    series_last_dt's present-month scan. Subtracts PRESENT-month coverage — a slight
    (safe, pessimistic) OVER-charge only on a rare interior MISSING/hole month that
    gap_fill also skips on a clean run; a fully-backfilled series is exact. Empty set
    on a missing/blank/odd manifest (-> full window charged)."""
    months = (manifest or {}).get("intervals", {}).get(iv, {}).get("months", {})
    stored = set()
    for entry in (months or {}).values():
        if not isinstance(entry, dict) or entry.get("status") != "present":
            continue
        fp = str(entry.get("first", "")).split(" ", 1)
        lp = str(entry.get("last", "")).split(" ", 1)
        if len(fp) != 2 or len(lp) != 2:
            continue
        first = stock_storage.parse_timestamp(fp[0], fp[1])
        last = stock_storage.parse_timestamp(lp[0], lp[1])
        if first is None or last is None:
            continue
        lo, hi = max(start, first.date()), min(today, last.date())
        if lo <= hi:
            stored.update(stock_ibkr.trading_days(lo, hi))
    return stored


def _est_parallel(lanes):
    """Effective parallel speedup for `lanes` connected accounts/ports. Linear up
    to the 5-port fleet, then DIMINISHING returns past 5 (measured 2026-06-24:
    8 ports ≈ 1.22x of 5), so more ports keep shortening the estimate — just
    sub-linearly. n=5 -> 4.5x, n=8 -> ~5.5x, n=10 -> ~6.2x."""
    n = max(1, int(lanes or 1))
    if n <= 5:
        return min(float(n), _EST_MAX_PARALLEL)
    return _EST_MAX_PARALLEL + (n - 5) * _EST_MARGINAL_PARALLEL


def _est_humanize_minutes(seconds):
    """Round wall-clock `seconds` to a human '~M min' (or '~H h M min'
    past an hour, '<1 min' for a sliver). Always an estimate."""
    mins = seconds / 60.0
    if seconds <= 0:
        return "~0 min"
    if mins < 1:
        return "<1 min"
    total = int(round(mins))
    if total < 60:
        return f"~{total} min"
    h, m = divmod(total, 60)
    return f"~{h} h {m} min" if m else f"~{h} h"


def _estimate_worktime(root, selections, mode, since=None, today=None,
                       n_accounts=1):
    """Estimate wall-clock time for a BATCH of (ticker, interval) IBKR
    fetches — for the add/update confirm dialog — WITHOUT a network call
    per stock. Pure logic at module level ON PURPOSE (the headless smoke
    test drives it directly); it must NEVER touch tkinter.

    mode='add'    -> FULL backfill per series: the window is
                     `since..today` when a depth/since is given, else
                     `earliest..today`. earliest is unknown offline (it
                     needs IBKR's head timestamp), so with NO since the
                     per-series request count is reported as None and the
                     line is an honest lower bound on the rest.
    mode='update' -> gap-fill per series: the window is
                     `last_stored..today` read from the manifest months
                     (via stock_ibkr.series_last_dt). A series with no gap
                     still costs ~1 overlap/refetch request; an unstored
                     series (no last) is a full backfill and falls back to
                     the `since`/earliest rule above.

    Requests come from stock_ibkr.request_count (the SAME source of truth
    the fetcher uses — spans for whole-session intervals, intra-day
    windows for sub-minute), so the estimate and the run stay in lockstep.
    Requests -> seconds is interval-aware (_est_secs_per_request) and the
    60/10-min budget is PER ACCOUNT, so every series' seconds SUM. A small
    fixed qualify+head cost is added per series (off the metered budget).

    Returns {'requests', 'seconds', 'minutes', 'human', 'series',
    'unknown_series', 'per_interval_breakdown', 'notes'} — never raises;
    an unreadable manifest just contributes its qualify+head floor. It is
    a best-effort ESTIMATE: real time varies with TWS latency, pacing
    back-offs, holidays (which return zero bars) and illiquid sessions."""
    import datetime as _dt
    today = today or _dt.date.today()
    requests = 0
    seconds = 0.0
    series = 0
    unknown = 0                          # series whose size can't be sized
    sized_sidecar = 0                    # 'Earliest traceable' sized from the
    #                                      recorded served-earliest sidecar
    notes = []
    per_series = {}                      # "TICKER interval" -> modeled seconds
    #                                      (feeds the live work-weighted ETA)
    # The per-ticker served-earliest sidecar (auto-recorded at fetch time)
    # turns 'Earliest traceable' from a flat ~3yr guess into that stock's
    # REAL demonstrated depth for every ticker fetched before.
    earliest_map = {}
    if mode != "update":
        try:
            earliest_map = {
                str(k).upper(): _dt.date.fromisoformat(str(v)[:10])
                for k, v in stock_validate.load_ibkr_earliest(root).items()}
        except Exception:  # noqa: BLE001 — sidecar absent/corrupt -> guess
            earliest_map = {}
    by_iv = {}                           # interval -> {requests, seconds,
    #                                       series, unknown}

    def _bucket(iv):
        return by_iv.setdefault(iv, {"requests": 0, "seconds": 0.0,
                                     "series": 0, "unknown": 0})

    # PHASE 1 — size each series' fetch WINDOW (days) + the fixed qualify+head
    # floor (charged per series either way). Requests are charged in phase 2 so
    # an extended SET can be counted as ONE combined fetch, not three.
    series_days = {}                     # (ticker, iv) -> days list | None
    _man_cache = {}                      # ticker -> manifest (one disk read each)
    for ticker, interval in (selections or []):
        iv = str(interval or "").strip()
        # the session suffix (-pre/-post) is not a key in _BAR_SIZES; strip it
        # for the size checks (request_count/series_last_dt suffix-strip
        # internally, so they still get the full token).
        base_iv = stock_storage.base_interval(iv)
        b = _bucket(iv)
        series += 1
        b["series"] += 1
        seconds += _EST_QUALIFY_HEAD_SECS
        b["seconds"] += _EST_QUALIFY_HEAD_SECS
        skey = f"{ticker} {iv}"
        per_series[skey] = per_series.get(skey, 0.0) + _EST_QUALIFY_HEAD_SECS
        if base_iv not in stock_ibkr._BAR_SIZES:
            unknown += 1
            b["unknown"] += 1
            series_days[(ticker, iv)] = None
            continue
        # ONE manifest load per ticker (memoized — a ticker repeats across
        # intervals); reused by the update-mode head AND the stored-month subtract.
        if ticker not in _man_cache:
            try:
                _man_cache[ticker] = stock_storage.load_manifest(
                    Path(root) / ticker)
            except Exception:  # noqa: BLE001 — corrupt/missing -> None (treat new)
                _man_cache[ticker] = None
        man = _man_cache[ticker]
        start = None
        if mode == "update":
            try:
                last = stock_ibkr.series_last_dt(man, iv)
            except Exception:  # noqa: BLE001 — corrupt/missing -> treat new
                last = None
            if last is not None:
                start = last.date()      # refetch the last session onward
        if start is None:                # add, or update of an empty series
            start = since                # depth gives an exact lower bound
        if start is None:                # 'Earliest traceable' — try the sidecar
            side = earliest_map.get(str(ticker).upper())
            if side is not None and side < today:
                start = side             # the stock's DEMONSTRATED depth
                sized_sidecar += 1
            else:                        # truly unknown offline
                start = today - _dt.timedelta(days=_EST_DEFAULT_BACKFILL_DAYS)
                unknown += 1
                b["unknown"] += 1
        if base_iv.endswith("s"):        # 1s only reaches ~6 months — clamp
            floor = today - _dt.timedelta(
                days=stock_ibkr.ONE_SECOND_MAX_AGE_DAYS)
            if start < floor:
                start = floor
        days = stock_ibkr.trading_days(start, today)
        # subtract months ALREADY on disk so the estimate counts only the REAL
        # remaining gaps (not the full window). 'update' is gated out — it already
        # starts at last_stored AND needs a non-empty list for its 1-refetch floor.
        if mode != "update" and man is not None:
            stored = _est_stored_days(man, iv, start, today)
            if stored:
                days = [d for d in days if d not in stored]
        series_days[(ticker, iv)] = days

    # PHASE 2 — charge requests PER GROUP. A full extended set
    # {base, base-pre, base-post} fetches the unfiltered useRTH=False payload
    # ONCE per span (the combined refactor), so its cost is the UNION of the
    # three windows at the extended span — counted ONCE, not summed 3x.
    by_ticker_base = {}
    for (t, ivv) in series_days:
        by_ticker_base.setdefault(
            (t, stock_storage.base_interval(ivv)), []).append(ivv)

    def _charge(iv, days, attr_key=None):
        if days is None:
            return
        reqs = stock_ibkr.request_count(iv, days)
        if mode == "update" and reqs == 0:
            reqs = 1                     # a no-gap update still pays ~1 refetch
        per_req = _est_secs_per_request(iv)
        nonlocal requests, seconds
        requests += reqs
        seconds += reqs * per_req
        bk = _bucket(iv)
        bk["requests"] += reqs
        bk["seconds"] += reqs * per_req
        if attr_key is not None:         # per-series work for the live ETA
            per_series[attr_key] = (per_series.get(attr_key, 0.0)
                                    + reqs * per_req)

    for (t, base), ivs in by_ticker_base.items():
        full = [base, stock_storage.with_session(base, "pre"),
                stock_storage.with_session(base, "post")]
        combinable = (all(x in ivs for x in full)
                      and base in stock_ibkr._BAR_SIZES
                      and stock_ibkr._BAR_SIZES[base][1] is None
                      and all(series_days.get((t, x)) is not None for x in full))
        if combinable:
            union = sorted(set().union(
                *(set(series_days[(t, x)]) for x in full)))
            # the combined fetch chunks at the extended "2 D" span (the -post
            # token) and serves rth+pre+post from one pass. The union's cost is
            # attributed to the BASE series (it performs the fetch); the -pre/
            # -post companions keep only their qualify floor — which is exactly
            # how the run behaves (companions land from the combined cache).
            _charge(stock_storage.with_session(base, "post"), union,
                    attr_key=f"{t} {base}")
        else:
            for iv in ivs:
                _charge(iv, series_days.get((t, iv)), attr_key=f"{t} {iv}")

    if sized_sidecar:
        notes.append(
            f"{sized_sidecar} 'Earliest traceable' series sized from each "
            f"stock's recorded IBKR served-earliest (no guessing).")
    if unknown:
        notes.append(
            f"{unknown} series use 'Earliest traceable' — sized assuming ~"
            f"{_EST_DEFAULT_BACKFILL_DAYS // 365} yr (the true earliest bar is "
            f"only known online); a younger stock finishes faster.")
    if any(stock_storage.base_interval(str(iv or "").strip()).endswith("s")
           for _t, iv in (selections or [])):
        notes.append(
            "1-second bars are HMDS-metered at 60 requests / 10 min "
            "(~10 s each); IBKR only serves ~6 months back.")
    notes.append("Estimate only — real time varies with TWS latency, "
                 "pacing back-offs and market holidays.")
    # Parallel mode splits WHOLE tickers across accounts (each with its own
    # pacing budget), so wall-clock ≈ serial / lanes, where lanes is bounded by
    # both the account count and the number of distinct tickers to fetch.
    serial_seconds = seconds
    n_tickers = len({t for t, _i in (selections or [])})
    lanes = max(1, min(int(n_accounts or 1), n_tickers or 1))
    eff = _est_parallel(lanes)           # ~linear to 5 ports, diminishing past
    seconds = serial_seconds / eff
    if lanes > 1:
        notes.insert(0, f"Parallel mode: {lanes} port(s) — fetches parallelize "
                        f"~linearly to 5, then with diminishing returns "
                        f"(measured), so this assumes ~{eff:g}× (serial ≈ "
                        f"{_est_humanize_minutes(serial_seconds)}).")
    return {"requests": requests, "seconds": seconds,
            "minutes": seconds / 60.0, "human": _est_humanize_minutes(seconds),
            "serial_seconds": serial_seconds, "lanes": lanes,
            "series": series, "unknown_series": unknown,
            "per_series_seconds": per_series,
            "per_interval_breakdown": by_iv, "notes": notes}


# ---------------------------------------------------------------------------
# GUI
# ---------------------------------------------------------------------------


class _FixDataCancelled(Exception):
    """Internal signal: the staged Fix-data run was cancelled at a safe
    per-series boundary (raised in the worker, caught to finalize cleanly)."""


class _ExportProgressCoalescer:
    """Thread-safe latest-event mailbox for the Export Designer.

    Producers replace the pending value for each channel while at most one
    FIFO token is queued.  Because ``run_batch`` invokes progress callbacks
    synchronously, its forced terminal aggregate token is necessarily queued
    before the worker's ``done`` message and therefore cannot be dropped.
    """

    _CHANNELS = ("aggregate", "activity")

    def __init__(self, output):
        self._output = output
        self._lock = threading.Lock()
        self._latest = {channel: None for channel in self._CHANNELS}
        self._queued = set()

    @staticmethod
    def _channel(event):
        return ("aggregate" if isinstance(event, dict)
                and event.get("kind") == "aggregate" else "activity")

    def post(self, event, *, force=False):
        """Publish ``event`` and return True only when a token was enqueued.

        ``force`` documents terminal aggregate intent.  Replacement is already
        lossless: if a token is pending it will drain the newest value; if its
        token was consumed, this call queues another one.
        """
        channel = self._channel(event)
        payload = dict(event) if isinstance(event, dict) else event
        if isinstance(payload, dict) and isinstance(payload.get("detail"), dict):
            payload["detail"] = dict(payload["detail"])
        with self._lock:
            self._latest[channel] = payload
            if channel in self._queued:
                return False
            self._queued.add(channel)
            self._output.put((channel, self))
            return True

    def take(self, channel):
        if channel not in self._CHANNELS:
            raise ValueError(f"unknown export progress channel: {channel}")
        with self._lock:
            payload = self._latest[channel]
            self._latest[channel] = None
            self._queued.discard(channel)
            return payload


class DataViewerApp:
    """Main window: drop zone + interval + interactive chart + overview."""

    # The standard multi-port TWS demo fleet — one instance per account. "Start
    # up multi-port" selects the first N pairs (a@→2000, b@→3000, … h@→9000)
    # after a fresh memory estimate; the ports editor's 'Preset 2000-9000'
    # button still fills the complete set. 8 ports measured ~1.22x of 5
    # (sub-linear but real — 2026-06-24 N-sweep).
    _STANDARD_FLEET = (2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000)

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Data Bank — Data Viewer")
        self.root.geometry("1500x900")

        # Root ✕: open busy modals are tracked so the close handler can cancel
        # and wait for in-flight Data Bank work.
        self._busy_open = []
        self.root.protocol("WM_DELETE_WINDOW", self._on_app_close)

        # Data state
        self.raw_df: Optional[pd.DataFrame] = None
        self.raw_interval: Optional[pd.Timedelta] = None
        self.raw_interval_label: str = "unknown"
        self.current_df: Optional[pd.DataFrame] = None
        self.current_label: str = ""

        # Path of the loaded CSV — used to name exported trade files.
        self.loaded_path: Optional[Path] = None

        # Native macOS pinch — handle via Cocoa NSEvent monitor since Tk
        # doesn't deliver magnify gestures. None on non-Mac or when PyObjC
        # is missing; in that case Cmd-scroll is the zoom path.
        #
        # The Cocoa handler runs inside Cocoa's runloop on macOS — touching
        # Tk widgets directly from there can SEGFAULT the whole process
        # (Cocoa can't catch Python exceptions, and the GIL handoff is
        # fragile). Instead, the handler does NOTHING but Queue.put_nowait
        # the magnification value (atomic, thread-safe). Tk polls the queue
        # at ~30 Hz and processes accumulated frames on its own loop.
        self._pinch_monitor = None
        self._pinch_queue: "queue_mod.Queue[float]" = queue_mod.Queue()
        # Trackpad two-finger swipe → pan. Tk 9 on macOS does NOT deliver a
        # <MouseWheel> event for precise/trackpad scroll, so the Cocoa monitor
        # feeds the (dx, dy) swipe deltas here and the poll loop applies them.
        self._scroll_queue: "queue_mod.Queue[tuple]" = queue_mod.Queue()
        self._unattended_notice_win = None
        self._unattended_notice_text = None

        self._build_top_bar()
        self._build_main_panes()
        self._wire_drag_and_drop()
        self._install_pinch_monitor()
        self._show_placeholder()

        # Default to Apple: auto-load the bundled AAPL dataset once the window
        # is up. Deferred via `after` so the UI paints first, then the (large)
        # CSV load runs — same code path as a manual browse/drop.
        self.root.after(150, self._autoload_default_csv)

    def _autoload_default_csv(self) -> None:
        """Load the bundled default dataset (Apple) if it sits next to this
        script and the user hasn't already loaded something."""
        if self.raw_df is not None:
            return
        default = Path(__file__).resolve().parent / DEFAULT_CSV_NAME
        if default.exists():
            self.status.set(f"Auto-loading default dataset: {default.name} …")
            self.root.update_idletasks()
            self._load_path(default)

    def _show_unattended_notice(self, title, body, *, level="info",
                                parent=None):
        """Append an unattended-run notice without blocking Tk callbacks."""
        try:
            win = self._unattended_notice_win
            if win is None or not win.winfo_exists():
                try:
                    if parent is None or not parent.winfo_exists():
                        parent = self.root
                except Exception:  # noqa: BLE001 - destroyed parent
                    parent = self.root
                # Root ownership keeps accumulated notices alive if a run dialog
                # is closed after its worker completes.
                win = tk.Toplevel(self.root)
                self._unattended_notice_win = win
                win.title("Run notices")
                win.geometry("760x420")
                win.minsize(520, 260)
                win.transient(parent)

                frame = ttk.Frame(win, padding=8)
                frame.pack(fill=tk.BOTH, expand=True)
                yscroll = ttk.Scrollbar(frame, orient="vertical")
                yscroll.pack(side=tk.RIGHT, fill=tk.Y)
                text = tk.Text(
                    frame, wrap="word", font=("Consolas", 9),
                    yscrollcommand=yscroll.set)
                text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
                yscroll.config(command=text.yview)
                self._unattended_notice_text = text

                def close_notice():
                    self._unattended_notice_win = None
                    self._unattended_notice_text = None
                    try:
                        win.destroy()
                    except Exception:  # noqa: BLE001
                        pass

                win.protocol("WM_DELETE_WINDOW", close_notice)
                ttk.Button(win, text="Close", command=close_notice).pack(
                    side=tk.BOTTOM, pady=(0, 8))
            else:
                text = self._unattended_notice_text

            if text is None:
                return None
            label = {
                "error": "ERROR",
                "warning": "WARNING",
            }.get(str(level).lower(), "INFO")
            text.config(state=tk.NORMAL)
            if text.index("end-1c") != "1.0":
                text.insert(tk.END, "\n\n")
            text.insert(tk.END, f"[{label}] {title}\n{body}")
            text.config(state=tk.DISABLED)
            text.see(tk.END)
            win.deiconify()
            win.lift()
            return win
        except Exception:  # noqa: BLE001 - notices never break run finalization
            return None

    # ------------------------------------------------------------------ UI

    def _build_top_bar(self) -> None:
        bar = ttk.Frame(self.root, padding=(10, 8))
        bar.pack(side=tk.TOP, fill=tk.X)

        self.drop_zone = tk.Label(
            bar,
            text=(
                "  Drag & drop a CSV, Parquet, or Excel file here   "
                if DND_AVAILABLE
                else "  Drag-and-drop unavailable — click Browse  "
            ),
            relief="groove", bd=2, padx=18, pady=14,
            bg="#eef3f9", fg="#1f3b5c",
            font=(_UI_FONT, 12, "bold"),
        )
        self.drop_zone.pack(side=tk.LEFT, fill=tk.X, expand=True)

        ttk.Button(bar, text="Browse…", command=self._on_browse).pack(
            side=tk.LEFT, padx=(8, 12)
        )

        interval_frame = ttk.Frame(bar)
        interval_frame.pack(side=tk.LEFT)
        ttk.Label(interval_frame, text="Interval:").grid(row=0, column=0, padx=(0, 4))

        # Preset dropdown — readonly so dropdown clicks are 100% reliable
        # on macOS Tk (no more "had to click 1 hour twice" issue).
        self.interval_var = tk.StringVar(value="")
        self.interval_combo = ttk.Combobox(
            interval_frame, textvariable=self.interval_var,
            values=[label for label, _ in PRESET_INTERVALS],
            state="disabled", width=12,
        )
        self.interval_combo.grid(row=0, column=1)
        # Picking a preset clears any half-typed custom rule so it's clear
        # which input will be applied. Nothing auto-renders.
        self.interval_combo.bind(
            "<<ComboboxSelected>>",
            lambda _e: self.custom_var.set(""),
        )

        # Custom rule entry — typed pandas rules (10min, 45min, 2h, 3D…).
        ttk.Label(interval_frame, text="Custom:").grid(row=0, column=2, padx=(10, 4))
        self.custom_var = tk.StringVar(value="")
        self.custom_entry = ttk.Entry(
            interval_frame, textvariable=self.custom_var, width=10, state="disabled",
        )
        self.custom_entry.grid(row=0, column=3)
        self.custom_entry.bind("<Return>", self._on_interval_apply)

        self.apply_btn = ttk.Button(
            interval_frame, text="Apply", command=self._on_interval_apply, state="disabled",
        )
        self.apply_btn.grid(row=0, column=4, padx=(4, 0))

        # Pending indicator on the Apply button — watch both inputs.
        self.interval_var.trace_add("write", self._on_interval_var_changed)
        self.custom_var.trace_add("write", self._on_interval_var_changed)

        self.raw_interval_lbl = ttk.Label(
            interval_frame, text="(raw: —)", foreground="#666"
        )
        self.raw_interval_lbl.grid(row=0, column=5, padx=(10, 0))

        self.status = tk.StringVar(value="No file loaded.")
        ttk.Label(
            self.root, textvariable=self.status, anchor="w", padding=(10, 2)
        ).pack(side=tk.TOP, fill=tk.X)

    def _build_main_panes(self) -> None:
        # Four tabs: Data Viewer, EMA Strategy, Price Volume, Data Storage.
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        viewer_tab = ttk.Frame(self.notebook)
        self.notebook.add(viewer_tab, text="Data Viewer")
        self._build_viewer_tab(viewer_tab)



        storage_tab = ttk.Frame(self.notebook)
        self.notebook.add(storage_tab, text="Data Storage")
        self._build_storage_tab(storage_tab)

        # Map notebook tab index -> its CanvasChart, so the trackpad pinch zooms
        # whichever tab is currently visible (None = tab has no chart).
        self._tab_charts = [self.viewer_chart, None]

        # Opening the Data Storage tab with no archive folder offers to make one
        # — on later tab clicks (the binding) and if the app starts on it (after).
        self._storage_tab_w = storage_tab
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed,
                            add="+")
        self.root.after(400, self._on_tab_changed)

    def _on_tab_changed(self, _event=None):
        """When the user opens the Data Storage tab, offer to create the
        archive folder if it doesn't exist yet."""
        try:
            if self.notebook.select() == str(self._storage_tab_w):
                self._storage_ensure_folder()
        except Exception:  # noqa: BLE001 — a tab switch must never crash
            pass

    def _storage_ensure_folder(self):
        """No 'Stock Data Storage' folder -> ask to create it, then rescan."""
        root = self._storage_root
        if root.exists():
            return
        if not messagebox.askyesno(
                "Missing storage data",
                "Missing past storage data.  Create new folder?",
                parent=self.root):
            return
        try:
            root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            messagebox.showerror("Could not create folder", str(exc),
                                 parent=self.root)
            return
        self._storage_rescan()

    def _active_chart(self):
        """The CanvasChart on the currently-visible tab (or None)."""
        try:
            active = self.notebook.index(self.notebook.select())
        except Exception:  # noqa: BLE001
            return None
        charts = getattr(self, "_tab_charts", [])
        return charts[active] if 0 <= active < len(charts) else None

    # ------------------------------------------------------------------
    # Data Storage tab (Tier 0: read-only view of the bar archive)
    # ------------------------------------------------------------------

    def _build_storage_tab(self, parent) -> None:
        """The 'Stock Data Storage' tree: one row per (ticker, interval)
        series with its coverage from the manifests, plus everything the
        strict whitelist refused to touch. Tier 1 adds ingest: mixed
        vendor files in, contract-clean month files out, via
        stock_ingest (IBKR updates arrive in a later tier). Scans and
        ingests run on worker threads; the UI is only touched via
        root.after-driven queue pollers (tkinter is not thread-safe)."""
        self._storage_root = stock_storage.storage_root(
            Path(__file__).resolve().parent)
        try:
            calendar_reconciler.load_into_market_calendar(self._storage_root)
        except Exception:  # noqa: BLE001 — a sidecar read must never block launch
            pass
        self._storage_scanning = False
        self._storage_ingesting = False

        bar = ttk.Frame(parent, padding=(8, 6))
        bar.pack(side=tk.TOP, fill=tk.X)
        self._storage_btn = ttk.Button(          # manual = ground-truth full walk
            bar, text="Rescan",
            command=lambda: self._storage_rescan(use_cache=False))
        self._storage_btn.pack(side=tk.LEFT)
        self._storage_ingest_files_btn = ttk.Button(
            bar, text="Ingest files…", command=self._storage_ingest_files)
        self._storage_ingest_files_btn.pack(side=tk.LEFT, padx=(6, 0))
        self._storage_ingest_dir_btn = ttk.Button(
            bar, text="Ingest folder…", command=self._storage_ingest_folder)
        self._storage_ingest_dir_btn.pack(side=tk.LEFT, padx=(6, 0))
        self._storage_cancel_btn = ttk.Button(
            bar, text="Cancel ingest", state=tk.DISABLED,
            command=self._storage_ingest_cancel)
        self._storage_cancel_btn.pack(side=tk.LEFT, padx=(6, 0))
        ttk.Label(bar, text="TWS:").pack(side=tk.LEFT, padx=(12, 2))
        self._storage_tws_parallel = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            bar, text="Activate parallel mode",
            variable=self._storage_tws_parallel,
            command=self._storage_tws_mode_changed).pack(side=tk.LEFT)
        # When on, every fetch ALSO collects each series' pre-market + after-
        # hours bars into their own -pre / -post files (regular hours unchanged).
        # The toggle lives INSIDE the 'Add stock' and 'IBKR update' dialogs (one
        # shared setting); this is just the backing variable.
        self._storage_tws_extended = tk.BooleanVar(value=False)  # default OFF (user 2026-07-29)
        self._storage_tws_daily = tk.BooleanVar(value=True)      # store 1d daily too
        self._storage_tws_port = tk.StringVar(value="7497")
        # Parallel: one port per account. Starts as a single port; the editor's
        # 'Preset 2000-9000' button fills the standard fleet on demand.
        self._storage_tws_ports_list = ["7497"]
        self._storage_tws_portbox = ttk.Frame(bar)
        self._storage_tws_portbox.pack(side=tk.LEFT, padx=(4, 0))
        self._storage_tws_port_entry = ttk.Entry(
            self._storage_tws_portbox, textvariable=self._storage_tws_port,
            width=6)
        self._storage_tws_ports_btn = ttk.Button(
            self._storage_tws_portbox, text="Ports (1)…", width=11,
            command=self._storage_tws_ports_editor)
        self._storage_tws_mode_changed()          # show the Series field first
        self._storage_ibkr_btn = ttk.Button(
            bar, text="IBKR update…", command=self._storage_ibkr_open)
        self._storage_ibkr_btn.pack(side=tk.LEFT, padx=(6, 0))
        self._storage_multiport_btn = ttk.Button(
            bar, text="Start up multi-port",
            command=self._storage_multiport_start)
        self._storage_multiport_btn.pack(side=tk.LEFT, padx=(6, 0))
        self._storage_validate_ports_btn = ttk.Button(
            bar, text="Validate ports",
            command=self._storage_validate_ports)
        self._storage_validate_ports_btn.pack(side=tk.LEFT, padx=(6, 0))
        self._storage_find_btn = ttk.Button(
            bar, text="Add stock",
            command=self._storage_find_dialog)
        self._storage_find_btn.pack(side=tk.LEFT, padx=(6, 0))
        # ONE button: fill gaps (completeness) + cross-check accuracy w/ auto-refetch
        # (correctness), whole-bank. (The old per-ticker 'Verify accuracy' check is
        # still reachable by double-clicking a row — cross-val data is kept even
        # though it no longer has its own column.) Named "Repair data" and
        # placed beside Add stock (user 2026-07-22).
        self._storage_fixdata_btn = ttk.Button(
            bar, text="Repair data…",
            command=self._storage_fixdata_open)
        self._storage_fixdata_btn.pack(side=tk.LEFT, padx=(6, 0))
        # Second toolbar row: the single row above holds ~12 fetch/ingest
        # controls and used to also carry the bank-action buttons + status +
        # the (long) root path. On normal window widths that row overflowed and
        # the trailing buttons were clipped off the right edge, invisible with
        # no way to scroll to them. A dedicated second row starts fresh, so
        # these are always visible regardless of window width; Export sits at
        # the row's right edge (user 2026-07-22). (2026-07-22)
        bar2 = ttk.Frame(parent, padding=(8, 0, 8, 4))
        bar2.pack(side=tk.TOP, fill=tk.X)
        self._storage_tws_app_btn = ttk.Button(
            bar2, text="Set IBKR app...",
            command=self._storage_set_tws_app)
        self._storage_tws_app_btn.pack(side=tk.LEFT)
        self._storage_export_btn = ttk.Button(
            bar2, text="Export…",
            command=self._storage_exd_open)
        self._storage_export_btn.pack(side=tk.RIGHT)
        self._storage_ibkr_running = False
        self._storage_find_building = False
        self._storage_status = tk.StringVar(value="")
        ttk.Label(bar2, textvariable=self._storage_status,
                  anchor="w").pack(side=tk.LEFT, padx=(0, 10))
        self._addstock_debt_status = tk.StringVar(value="")
        ttk.Label(bar2, textvariable=self._addstock_debt_status,
                  foreground="#9a6700", anchor="w").pack(
                      side=tk.LEFT, padx=(0, 8))
        ttk.Label(bar2, text=str(self._storage_root),
                  foreground="#888").pack(side=tk.RIGHT, padx=(0, 10))
        self.root.after(0, self._addstock_refresh_debt_indicator)

        # Environment guards, surfaced right where data work will happen:
        # a missing tz database or a cloud-synced root is visible BEFORE
        # the first ingest/update. Detection only — nothing is changed.
        _warns = [w for w in (stock_storage.tzdata_problem(),
                              stock_storage.synced_root_warning(
                                  self._storage_root)) if w]
        if _warns:
            ttk.Label(parent, text="⚠  " + "    •    ".join(_warns),
                      foreground="#b00000", wraplength=1400,
                      padding=(10, 0, 10, 4),
                      anchor="w").pack(side=tk.TOP, fill=tk.X)

        # Interval filter bar — segmented buttons (All / 1s / 1m / 1h / 1d …) scope
        # the preview to ONE interval category; the text search is relocated to the
        # right of the same strip. Buttons are rebuilt per scan to show only the
        # base intervals actually present (see _storage_build_iv_buttons).
        filt = ttk.Frame(parent, padding=(8, 2, 8, 0))
        filt.pack(side=tk.TOP, fill=tk.X)
        self._storage_filter_bar = filt
        ttk.Label(filt, text="Interval:").pack(side=tk.LEFT)
        self._storage_iv_filter = tk.StringVar(value="all")
        self._storage_iv_btns = ttk.Frame(filt)
        self._storage_iv_btns.pack(side=tk.LEFT, padx=(4, 0))
        self._storage_build_iv_buttons([])           # seed with just "All"

        self._storage_kind_filter = tk.StringVar(value="all")
        self._storage_kind_strip = ttk.Frame(parent, padding=(8, 0, 8, 2))
        ttk.Label(self._storage_kind_strip, text="Data:").pack(side=tk.LEFT)
        self._storage_kind_btns = ttk.Frame(self._storage_kind_strip)
        self._storage_kind_btns.pack(side=tk.LEFT, padx=(4, 0))

        # text search — moved to the RIGHT of the interval bar (was the prime spot)
        self._storage_filter = tk.StringVar(value="")
        self._storage_filter_count = tk.StringVar(value="")
        ttk.Label(filt, textvariable=self._storage_filter_count,
                  foreground="#888").pack(side=tk.RIGHT, padx=(8, 0))
        _fe = ttk.Entry(filt, textvariable=self._storage_filter, width=24)
        _fe.pack(side=tk.RIGHT)
        ttk.Label(filt, text="Find:").pack(side=tk.RIGHT, padx=(0, 4))
        self._storage_filter.trace_add(
            "write", lambda *_a: self._storage_render_rows())
        _fe.bind("<Escape>", lambda _e: self._storage_filter.set(""))

        body = ttk.Frame(parent, padding=(8, 0, 8, 4))
        body.pack(fill=tk.BOTH, expand=True)
        cols = ("ticker", "name", "interval", "months", "coverage",
                "rows", "earliest", "gaps", "flags", "secid")
        # Table look: taller rows + a bold, raised heading so it reads as a grid;
        # zebra-striped rows (tags below) give the clear row-to-row distinction.
        _stl = ttk.Style()
        try:
            _stl.configure("Storage.Treeview", rowheight=25, borderwidth=1)
            _stl.configure("Storage.Treeview.Heading", font=("", 9, "bold"),
                           relief="raised", borderwidth=1, padding=(4, 3))
        except Exception:  # noqa: BLE001 — a theme without these options
            pass
        self._storage_tree = ttk.Treeview(body, columns=cols, show="headings",
                                          height=14, style="Storage.Treeview")
        for c, head, w, anch in (
                ("ticker", "Symbol", 80, "w"),
                ("name", "Data", 120, "center"),
                ("interval", "Interval", 70, "center"),
                ("months", "Years", 70, "e"),
                ("coverage", "Coverage (first bar → last bar)", 340, "w"),
                ("rows", "Rows", 110, "e"),
                ("earliest", "Earliest on IBKR", 130, "center"),
                ("gaps", "Gaps", 90, "center"),
                ("flags", "Flags", 280, "w"),
                ("secid", "Security ID", 100, "e")):
            self._storage_tree.heading(c, text=head)
            self._storage_tree.column(c, width=w, anchor=anch,
                                      stretch=(c in ("coverage", "flags")))
        # zebra striping: alternating row backgrounds for clear row separation
        self._storage_tree.tag_configure("oddrow", background="#eef2f6")
        self._storage_tree.tag_configure("evenrow", background="#ffffff")
        _vsb = ttk.Scrollbar(body, orient="vertical",
                             command=self._storage_tree.yview)
        self._storage_tree.configure(yscrollcommand=_vsb.set)
        self._storage_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        _vsb.pack(side=tk.LEFT, fill=tk.Y)
        # right-click -> reveal the row's files in the OS file manager
        self._storage_tree.bind("<Button-3>", self._storage_tree_menu)
        self._storage_tree.bind("<Button-2>", self._storage_tree_menu)
        # double-click -> expand the row's cross-validation flags (count,
        # severity, % differences, recommendation).
        self._storage_tree.bind("<Double-Button-1>", self._storage_xval_popup)

        issues = ttk.LabelFrame(parent, text="Issues / inert items",
                                padding=(6, 2))
        issues.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=(0, 8))
        self._storage_issues = tk.Text(issues, height=6, wrap="none",
                                       state=tk.DISABLED,
                                       font=("Consolas", 9))
        _isb = ttk.Scrollbar(issues, orient="vertical",
                             command=self._storage_issues.yview)
        self._storage_issues.configure(yscrollcommand=_isb.set)
        self._storage_issues.pack(side=tk.LEFT, fill=tk.X, expand=True)
        _isb.pack(side=tk.LEFT, fill=tk.Y)

        self._storage_rescan()        # initial scan (instant on empty root)

    def _reveal_in_explorer(self, path, select=False) -> str:
        """Open `path` in the OS file manager; when select=True and the
        target is a file, highlight it inside its folder. Returns ''
        on success or a short error. Never raises."""
        import subprocess
        p = Path(path)
        try:
            if sys.platform.startswith("win"):
                if select and p.is_file():
                    # explorer /select wants ONE quoted backslash path
                    # in a single command string; it exits 1 even on
                    # success, so fire-and-forget with Popen
                    native = os.path.normpath(str(p))
                    subprocess.Popen(f'explorer /select,"{native}"')
                else:
                    target = p if p.is_dir() else p.parent
                    os.startfile(os.path.normpath(str(target)))  # noqa
            elif sys.platform == "darwin":
                args = (["open", "-R", str(p)] if select and p.is_file()
                        else ["open", str(p if p.is_dir() else p.parent)])
                subprocess.Popen(args)
            else:
                subprocess.Popen(
                    ["xdg-open", str(p if p.is_dir() else p.parent)])
            return ""
        except Exception as exc:  # noqa: BLE001 — report, never crash
            return str(exc)

    def _storage_row_paths(self, iid):
        """(ticker_dir, latest_month_file_or_None) for a tree row, from
        its symbol+interval and the manifest. Files may not exist yet
        (manifest-ahead) — callers fall back to the folder."""
        try:
            vals = list(self._storage_tree.item(iid)["values"])
        except Exception:  # noqa: BLE001
            return None, None
        if not vals:
            return None, None
        sym = str(vals[0])
        iv = str(vals[2]) if len(vals) > 2 else ""
        tdir = self._storage_root / sym
        latest = None
        try:
            man = stock_storage.load_manifest(tdir)
            months = ((man or {}).get("intervals", {}).get(iv, {})
                      .get("months", {}))
            present = sorted(k for k, v in months.items()
                             if v.get("status") == "present")
            if present:
                y, m = present[-1].split("-")
                latest = (stock_storage.find_month_file(
                    self._storage_root, sym, int(y), int(m), iv)
                    or stock_storage.month_file_path(
                        self._storage_root, sym, int(y), int(m), iv))
        except Exception:  # noqa: BLE001 — folder still works
            pass
        return tdir, latest

    def _storage_tree_menu(self, event) -> None:
        """Right-click on a storage row -> reveal its files in the OS
        file manager. Selects the row under the pointer first so the
        menu always acts on what was clicked."""
        tree = self._storage_tree
        iid = tree.identify_row(event.y)
        if not iid:
            return
        try:
            tree.selection_set(iid)
        except Exception:  # noqa: BLE001
            pass
        tdir, latest = self._storage_row_paths(iid)
        if tdir is None:
            return
        menu = tk.Menu(tree, tearoff=0)
        if latest is not None:
            menu.add_command(
                label="Show latest data file in File Explorer",
                command=lambda: self._storage_reveal(latest, select=True))
        menu.add_command(
            label="Open ticker folder in File Explorer",
            command=lambda: self._storage_reveal(tdir, select=False))
        menu.add_separator()
        menu.add_command(
            label="Copy folder path",
            command=lambda: self._storage_copy_path(tdir))
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()

    def _storage_xval_popup(self, event) -> None:
        """Double-click a storage row -> expand its cross-validation flags:
        count, severity, per-day % differences vs the online daily reference,
        and a recommendation. Reads the enriched sidecar; offers a live re-check."""
        tree = self._storage_tree
        iid = tree.identify_row(event.y)
        if not iid:
            return
        try:
            vals = list(tree.item(iid)["values"]) + [""] * 8
        except Exception:  # noqa: BLE001
            return
        ticker = str(vals[0]).strip()
        interval = str(vals[2]).strip() or "1m"
        if not ticker:
            return
        # column-aware: double-clicking the GAPS cell shows WHERE the gaps are
        try:
            colid = tree.identify_column(event.x)        # e.g. "#8"
            colname = tree["columns"][int(colid[1:]) - 1]
        except Exception:  # noqa: BLE001
            colname = ""
        if colname == "gaps":
            self._storage_gap_show(ticker, interval)
            return
        self._storage_combined_show(ticker)       # 3-source persistent dates first
        self._storage_xval_show(ticker, interval, self._xval_entry(ticker))

    def _storage_gap_show(self, ticker, interval) -> None:
        """Double-click the Gaps cell -> a scrollable map of WHERE the gaps are: the
        whole MISSING TRADING DAYS (connected-pattern) and the interior missing
        MINUTES (from the time-ordered data_gaps.parquet)."""
        root = self._storage_root
        key = f"{str(ticker).strip().upper()} {str(interval).strip()}"
        try:
            entry = (stock_validate.load_data_gaps(root)
                     .get("series", {}).get(key) or {})
        except Exception:  # noqa: BLE001
            entry = {}
        days = entry.get("missing_day_list") or []
        absent = entry.get("source_absent_list") or []
        rows = []
        try:
            gp = Path(root).parent / stock_validate.GAP_REPORT_NAME
            tk_u, iv_s = str(ticker).upper(), str(interval)
            rows = [r for r in (stock_validate._read_gap_parquet(gp) or [])
                    if str(r[0]).upper() == tk_u and str(r[1]) == iv_s]
        except Exception:  # noqa: BLE001
            rows = []
        if not days and not rows and not absent:
            messagebox.showinfo(
                f"Gaps — {ticker} {interval}",
                "No gaps recorded for this series — either ✓ scanned & whole, or it "
                "hasn't been scanned yet (run a fetch / 'Repair data').",
                parent=self.root)
            return
        lines = [f"GAP MAP — {ticker} {interval}", ""]
        if days:
            lines.append(f"▌ FILLABLE missing trading day(s): {len(days)}")
            lines += [f"    {d}" for d in days]
            lines.append("")
        if absent:
            lines.append("⊘ DATA DOES NOT EXIST in the source — source-absent "
                         f"day(s): {len(absent)}")
            lines += [f"    {d}   (confirmed unfetchable)" for d in absent]
            lines.append("")
        if rows:
            lines.append(f"▌ Interior missing minute(s): {len(rows)}")
            # r = (ticker, interval, missing_dt, prev_dt, next_dt, gap_minutes)
            lines += [f"    {r[2]}   (between {r[3]} and {r[4]})" for r in rows]
        try:
            win = tk.Toplevel(self.root)
            win.title(f"Gaps — {ticker} {interval}")
            win.geometry("660x440")
            ysb = ttk.Scrollbar(win, orient="vertical")
            ysb.pack(side=tk.RIGHT, fill=tk.Y)
            txt = tk.Text(win, wrap="none", font=("Consolas", 9),
                          yscrollcommand=ysb.set)
            txt.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            ysb.config(command=txt.yview)
            txt.insert("1.0", "\n".join(lines))
            txt.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001
            pass

    @staticmethod
    def _xval_entries_from_data(data, ticker):
        """All cross-validation verdicts for one ticker.

        New sidecar keys are `TICKER interval`; legacy plain `TICKER` keys are
        still surfaced, tagged, and shown in the popup until a fresh validation
        writes the compound key.
        """
        t = str(ticker or "").strip().upper()
        if not t or not isinstance(data, dict):
            return []
        out = []
        prefix = f"{t} "
        for raw_key, raw_entry in data.items():
            if not isinstance(raw_entry, dict):
                continue
            key = str(raw_key or "").strip()
            key_u = key.upper()
            legacy = False
            if key_u == t:
                legacy = True
                interval = str(raw_entry.get("interval") or "").strip()
            elif key_u.startswith(prefix):
                interval = key[len(prefix):].strip()
            else:
                continue
            entry = dict(raw_entry)
            if interval and not entry.get("interval"):
                entry["interval"] = interval
            entry["_xval_key"] = key
            entry["_xval_legacy"] = legacy
            out.append(entry)
        return sorted(out, key=lambda e: (
            1 if e.get("_xval_legacy") else 0,
            str(e.get("interval") or ""),
            str(e.get("_xval_key") or "")))

    @staticmethod
    def _xval_entry_interval(entry):
        iv = str((entry or {}).get("interval") or "").strip()
        if (entry or {}).get("_xval_legacy"):
            return f"{iv or 'legacy'} (legacy key)"
        return iv or "unknown"

    def _xval_flag_summary(self, entries):
        flagged = [e for e in (entries or [])
                   if e.get("status") in self._XVAL_FLAG_STATUSES]
        if not flagged:
            return ""
        ivs = ", ".join(self._xval_entry_interval(e) for e in flagged[:4])
        if len(flagged) > 4:
            ivs += f", +{len(flagged) - 4} more"
        return (f"⚠ cross-val: {len(flagged)} interval(s) flagged"
                f" ({ivs}) [double-click row] — ")

    def _xval_pick_entry(self, entries, interval):
        want = str(interval or "").strip()
        for e in entries or []:
            if (not e.get("_xval_legacy")
                    and str(e.get("interval") or "").strip() == want):
                return e
        for e in entries or []:
            if e.get("status") in self._XVAL_FLAG_STATUSES:
                return e
        return (entries or [None])[0]

    def _xval_entry(self, ticker):
        try:
            return self._xval_entries_from_data(
                stock_validate.load_cross_validation(self._storage_root), ticker)
        except Exception:  # noqa: BLE001
            return []

    def _storage_combined_show(self, ticker):
        """If the ticker has PERSISTENT 3-source disagreements (external AND
        internal BOTH flag, and the date survived a fresh month refetch on all 3),
        pop the exact dates — the high-signal points to investigate."""
        try:
            entries = stock_validate.combined_entries_for_ticker(
                stock_validate.load_combined_flags(self._storage_root), ticker)
        except Exception:  # noqa: BLE001
            entries = []
        dates = [
            f"{entry.get('interval') or '?'}: {day}"
            for entry in entries
            for day in (entry.get("persistent") or [])
        ]
        if not dates:
            return
        try:
            messagebox.showwarning(
                f"3-source disagreement — {ticker}",
                f"{len(dates)} date(s) where BOTH stockanalysis AND IBKR's own "
                f"daily disagree with your stored intraday series, and the disagreement "
                f"SURVIVED a fresh refetch of that month on all 3 sources:\n\n"
                + "\n".join(f"  • {d}" for d in dates[:40])
                + ("\n  …" if len(dates) > 40 else "")
                + "\n\nThese are genuine data points to investigate.",
                parent=self.root)
        except Exception:  # noqa: BLE001
            pass

    @staticmethod
    def _xval_fmt(v):
        try:
            f = float(v)
            s = f"{f:,.4f}"
            return s.rstrip("0").rstrip(".") if "." in s else s
        except (TypeError, ValueError):
            return "" if v is None else str(v)

    def _storage_xval_show(self, ticker, interval, entry) -> None:
        """The cross-validation detail window for one ticker."""
        entries = entry if isinstance(entry, list) else ([entry] if entry else [])
        entry = self._xval_pick_entry(entries, interval)
        win = tk.Toplevel(self.root)
        win.title(f"Cross-validation — {ticker} {interval}")
        win.geometry("660x500")
        win.transient(self.root)
        top = ttk.Frame(win, padding=(10, 8))
        top.pack(side=tk.TOP, fill=tk.X)
        if not entry:
            ttk.Label(top, text=f"{ticker} hasn't been cross-validated yet.",
                      font=("", 10, "bold")).pack(anchor="w")
            ttk.Label(top, text="Run a live check against the online daily "
                      "reference (stockanalysis.com):",
                      foreground="#555").pack(anchor="w", pady=(4, 6))
            ttk.Button(top, text="Re-validate now",
                       command=lambda: self._storage_xval_revalidate(
                           ticker, interval, win)).pack(anchor="w")
            return
        status = entry.get("status", "?")
        detail = entry.get("detail") or {}
        sev = detail.get("severity") or entry.get("severity") or "—"
        sev_color = {"HIGH": "#cc2222", "MEDIUM": "#c8920a",
                     "LOW": "#1a9e3f"}.get(sev, "#555")
        glyph = {"validated": "✓ validated", "discrepancy": "⚠ discrepancy",
                 "unavailable": "— not on the online source",
                 "inconclusive": "— inconclusive",
                 "structural-ok": "✓ structural check",
                 "structural-flag": "⚠ structural flag"}.get(status, status)
        hdr = ttk.Frame(top)
        hdr.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(hdr, text=f"{ticker} {self._xval_entry_interval(entry)}",
                  font=("", 11, "bold")).pack(side=tk.LEFT)
        ttk.Label(hdr, text=f"   {glyph}").pack(side=tk.LEFT)
        if status in ("discrepancy", "structural-flag"):
            ttk.Label(hdr, text=f"   severity {sev}", foreground=sev_color,
                      font=("", 9, "bold")).pack(side=tk.LEFT)
        score = entry.get("score")
        parts = [f"{detail.get('flagged_count', 0)} day(s) flagged"]
        if detail.get("checked"):
            parts.append(f"of {detail['checked']} checked")
        if isinstance(score, (int, float)):
            parts.append(f"score {score:.1%}")
        if detail.get("max_percent"):
            parts.append(f"worst {detail['max_percent']}% off")
        ttk.Label(top, text="  ·  ".join(parts),
                  foreground="#333").pack(anchor="w", pady=(4, 0))
        if entry.get("note"):
            ttk.Label(top, text=entry["note"], foreground="#666",
                      wraplength=620).pack(anchor="w", pady=(2, 0))
        if len(entries) > 1:
            vf = ttk.LabelFrame(win, text="Verdicts by interval", padding=(6, 2))
            vf.pack(side=tk.TOP, fill=tk.X, padx=10, pady=(6, 0))
            vcols = ("interval", "status", "severity", "note")
            vtv = ttk.Treeview(vf, columns=vcols, show="headings",
                               height=min(5, len(entries)))
            for c, h, w, a in (("interval", "Interval", 110, "w"),
                               ("status", "Status", 110, "w"),
                               ("severity", "Severity", 70, "center"),
                               ("note", "Note", 360, "w")):
                vtv.heading(c, text=h)
                vtv.column(c, width=w, anchor=a)
            for e in entries:
                d = e.get("detail") or {}
                vtv.insert("", tk.END, values=(
                    self._xval_entry_interval(e),
                    e.get("status", ""),
                    d.get("severity") or e.get("severity") or "",
                    str(e.get("note") or "")[:160]))
            vtv.pack(side=tk.LEFT, fill=tk.X, expand=True)
        rec = detail.get("recommendation")
        if rec:
            rf = ttk.LabelFrame(win, text="Recommendation", padding=(8, 4))
            rf.pack(side=tk.TOP, fill=tk.X, padx=10, pady=(6, 0))
            ttk.Label(rf, text=rec, wraplength=620,
                      foreground="#0a3d62").pack(anchor="w")
        rows = detail.get("rows") or []
        tf = ttk.LabelFrame(win, text=f"Flagged days ({len(rows)} shown)",
                            padding=(6, 2))
        tf.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10, pady=(6, 6))
        cols = ("date", "field", "stored", "reference", "pct")
        tv = ttk.Treeview(tf, columns=cols, show="headings", height=10)
        for c, h, w, a in (("date", "Date", 90, "w"),
                           ("field", "Field", 70, "center"),
                           ("stored", "Stored", 110, "e"),
                           ("reference", "Reference", 110, "e"),
                           ("pct", "% off", 70, "e")):
            tv.heading(c, text=h)
            tv.column(c, width=w, anchor=a)
        for r in sorted(rows, key=lambda x: -(x.get("percent") or 0)):
            pct = ("" if r.get("percent") is None
                   else f"{r.get('percent')}%")
            tv.insert("", tk.END, values=(
                r.get("date"), r.get("field"), self._xval_fmt(r.get("derived")),
                self._xval_fmt(r.get("reference")), pct))
        sb = ttk.Scrollbar(tf, command=tv.yview)
        tv.config(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        if not rows:
            ttk.Label(tf, text="(no per-day flags — this mark is a whole-series "
                      "scale / level shift; see the recommendation above)",
                      foreground="#888").pack(anchor="w")
        bf = ttk.Frame(win, padding=(10, 6))
        bf.pack(side=tk.TOP, fill=tk.X)
        ttk.Button(bf, text="Re-validate now",
                   command=lambda: self._storage_xval_revalidate(
                       ticker, interval, win)).pack(side=tk.LEFT)
        ttk.Button(bf, text="Close",
                   command=win.destroy).pack(side=tk.LEFT, padx=(8, 0))
        if entry.get("asof"):
            ttk.Label(bf, text=f"checked {entry['asof']}",
                      foreground="#999").pack(side=tk.RIGHT)

    def _storage_xval_revalidate(self, ticker, interval, win) -> None:
        """Run a LIVE cross-validation (online) on a worker, persist the
        enriched result, then reopen the detail popup with fresh data."""
        import threading
        import queue
        q = queue.Queue()

        def _work_body():
            try:
                entry = stock_validate.cross_validate_ticker(
                    self._storage_root, ticker, interval)
                stock_validate.record_cross_validation(self._storage_root, entry)
                q.put(("ok", entry))
            except Exception as exc:  # noqa: BLE001
                q.put(("err", exc))

        try:
            win.destroy()
        except Exception:  # noqa: BLE001
            pass
        busy = tk.Toplevel(self.root)
        busy.title("Cross-validation")
        busy.transient(self.root)
        ttk.Label(busy, text=f"Re-validating {ticker} {interval} online…",
                  padding=20).pack()
        threading.Thread(target=_work_body, daemon=True,
                         name="xval-revalidate").start()

        def _poll():
            try:
                kind, payload = q.get_nowait()
            except queue.Empty:
                self.root.after(150, _poll)
                return
            try:
                busy.destroy()
            except Exception:  # noqa: BLE001
                pass
            if kind == "ok":
                self._storage_xval_show(ticker, interval,
                                        self._xval_entry(ticker) or payload)
            else:
                try:
                    messagebox.showwarning(
                        "Cross-validation",
                        f"Re-validation failed:\n{payload}", parent=self.root)
                except Exception:  # noqa: BLE001
                    pass
        self.root.after(150, _poll)

    def _storage_reveal(self, path, select=False) -> None:
        err = self._reveal_in_explorer(path, select=select)
        try:
            if err:
                self._storage_status.set(f"Could not open location: "
                                         f"{err}")
            else:
                self._storage_status.set(f"Opened {Path(path).name} in "
                                         f"the file manager.")
        except Exception:  # noqa: BLE001
            pass

    def _storage_copy_path(self, path) -> None:
        try:
            self.root.clipboard_clear()
            self.root.clipboard_append(os.path.normpath(str(path)))
            self._storage_status.set(f"Copied path: {path}")
        except Exception as exc:  # noqa: BLE001
            try:
                self._storage_status.set(f"Copy failed: {exc}")
            except Exception:  # noqa: BLE001
                pass

    # --- automatic cross-validation woven into a fetch ----------------------
    _XVAL_FLAG_STATUSES = frozenset({"discrepancy", "structural-flag"})
    _XVAL_GLYPH = {"validated": "✓", "discrepancy": "⚠",
                   "unavailable": "✗", "inconclusive": "—",
                   "structural-ok": "✓", "structural-flag": "⚠"}

    def _xval_glyph(self, status):
        """Cross-validation status -> main-table glyph. ✓ agrees, ✗ not on the
        online source, ⚠ DISAGREES (investigate), — couldn't check, '' never."""
        return self._XVAL_GLYPH.get(status, "") if status else ""

    @staticmethod
    def _gap_glyph(entry):
        """Gap status -> main-table 'Gaps' cell. '—' never scanned, '✓' scanned &
        whole, '⚠ …' has FILLABLE gaps (N interior bar(s) and/or D whole day(s)),
        '⊘N' = N day(s) the SOURCE genuinely LACKS (data does not exist — not
        fillable). e.g. '⚠ 2d ⊘1' = 2 fillable missing days + 1 source-absent;
        '✓ ⊘1' = whole as far as fetchable, 1 known source-absent day."""
        if not entry:
            return "—"
        n = entry.get("missing_total", 0)
        d = entry.get("missing_days", 0)
        a = entry.get("source_absent", 0)
        if not n and not d:
            glyph = "✓"
            if a:
                glyph += f" ⊘{a:,}"
            return glyph
        parts = []
        if n:
            parts.append(f"{n:,}")
        if d:
            parts.append(f"{d:,}d")
        glyph = "⚠ " + "+".join(parts)
        if a:
            glyph += f" ⊘{a:,}"
        return glyph

    def _xval_start(self, cancel_attr="_storage_ibkr_ev") -> None:
        """Begin a cross-validation session for a fetch run: a queue + ONE
        worker thread that validates each ticker as the fetch COMMITS it
        (overlapping the IBKR fetch), records the verdict to the sidecar, and
        refreshes that ticker's table cell. Replaces any prior session.

        `cancel_attr` names the attribute holding THIS run's cancel Event so the
        worker aborts on the OWNING run's cancel — the update dialog passes its
        own '_storage_ibkr_ev' (default), add-stock passes '_storage_find_ev'.
        (Previously the probe was hardwired to the update event, so an add-stock
        cancel could not stop the online-check backlog.)"""
        import queue
        import threading
        old = getattr(self, "_xval_stop", None)
        if old is not None:
            old.set()                          # retire a still-running session
            prev = getattr(self, "_xval_thread", None)
            if prev is not None and prev.is_alive():
                prev.join(timeout=1.0)         # usually instant; bounds overlap
        self._xval_q = queue.Queue()
        self._xval_done = 0                     # tickers validated (finalize progress)
        self._xval_queued = 0
        self._xval_inflight = 0
        self._xval_stop = threading.Event()
        self._xval_cancel_attr = cancel_attr
        self._xval_say_attr = ("_storage_find_say"
                               if cancel_attr == "_storage_find_ev"
                               else "_storage_ibkr_say")
        self._xval_thread = threading.Thread(
            target=self._xval_worker,
            args=(self._xval_q, self._xval_stop, cancel_attr),
            daemon=True, name="cross-validate")
        self._xval_thread.start()

    def _xval_on_series(self, ticker, interval, res):
        """Engine worker-thread hook (fires per committed series): queue the
        ticker for cross-validation once a REGULAR series commits (skip
        -pre/-post — the regular series carries the daily comparison). NEVER
        returns 'stop'; it must not interfere with the fetch."""
        try:
            # auto-capture the earliest IBKR date the engine stashed on res ->
            # persist to the bank sidecar + paint the 'Earliest on IBKR' cell.
            ed = (res or {}).get("ibkr_earliest")
            if ed:
                written = stock_validate.record_ibkr_earliest(
                    self._storage_root, ticker, ed)
                if written:
                    self._addstock_credit(ticker, "earliest")
                self.root.after(0, lambda t=ticker, d=ed:
                                self._earliest_set_cell(t, d))
            if stock_storage.session_of(str(interval)) != "rth":
                return None                    # -pre/-post -> skip cross-val
            q = getattr(self, "_xval_q", None)
            if q is not None:
                self._xval_queued = getattr(self, "_xval_queued", 0) + 1
                q.put((str(ticker), str(interval)))
            self._gap_incremental_queue(ticker, interval)
        except Exception:  # noqa: BLE001
            pass
        return None

    def _xval_log(self, msg) -> None:
        attr = getattr(self, "_xval_say_attr", "_storage_ibkr_say")
        try:
            self.root.after(0, lambda m=msg, a=attr: getattr(self, a)(m))
        except Exception:  # noqa: BLE001
            pass

    def _add_stock_on_series(self, ticker, interval, res, run_id=None):
        """Persist commit metadata; ticker-level checks run after all series."""
        try:
            ed = (res or {}).get("ibkr_earliest")
            if ed:
                written = stock_validate.record_ibkr_earliest(
                    self._storage_root, ticker, ed)
                if written:
                    self._addstock_credit(
                        ticker, "earliest", run_id=run_id)
                self.root.after(0, lambda t=ticker, d=ed:
                                self._earliest_set_cell(t, d))
            if stock_storage.session_of(str(interval)) == "rth":
                self._gap_incremental_queue(ticker, interval)
            if run_id is not None:
                self._addstock_series_complete(run_id, ticker, interval)
        except Exception:  # noqa: BLE001 - metadata never breaks a fill
            pass
        return None

    def _xval_worker(self, q, stop, cancel_attr="_storage_ibkr_ev") -> None:
        import queue
        seen = set()
        pending = []                # verdicts buffered for a BATCHED sidecar flush

        def _flush():
            # One read-modify-write for up to K verdicts: the single-entry API
            # rewrote the whole ~1.3 MB sidecar per ticker (~25 s CPU + ~0.65 GB
            # written across a 500-ticker run, sharing the disk with the fetch).
            # ZOMBIE GUARD: only THIS session's worker (the one that still owns
            # _xval_q) may write — a retired session's late flush must NOT clobber
            # a newer verdict for the same ticker. On a FAILED write keep the
            # batch so the next flush retries it (don't silently drop verdicts).
            if pending and getattr(self, "_xval_q", None) is q:
                saved = list(pending)
                try:
                    ok = stock_validate.record_cross_validation_many(
                        self._storage_root, saved)
                except Exception:  # noqa: BLE001 — persistence is best-effort
                    ok = None
                if ok is not None:
                    pending.clear()
                    credited = {}
                    for entry in saved:
                        ticker = str((entry or {}).get("ticker") or "")
                        interval = str((entry or {}).get("interval") or "")
                        current = self._addstock_current_xval_intervals(
                            [entry])
                        if ticker and interval and current:
                            credited.setdefault(ticker, []).append(interval)
                    for ticker, intervals in credited.items():
                        self._addstock_credit(ticker, "xval", intervals)

        try:
            while True:
                cev = getattr(self, cancel_attr, None)
                if cev is not None and cev.is_set():  # user CANCELLED the run -> ABORT:
                    return                            # drop the queue, don't drain it
                    #   (each item is a slow online cross_validate_ticker call; draining
                    #   the whole queue on Cancel is exactly the "cancel doesn't work" lag.
                    #   The finally-flush below still persists the verdicts already done.)
                try:
                    item = q.get(timeout=0.4)
                except queue.Empty:
                    if stop.is_set():          # finalize/pause: drained AND told to stop
                        return
                    _flush()                   # idle moment -> keep the sidecar current
                    continue
                if item is None:
                    return
                ticker, interval = item
                key = stock_validate.cross_validation_key(ticker, interval)
                if key in seen:                    # one verdict per series per run
                    continue
                seen.add(key)
                if getattr(self, "_xval_q", None) is q:
                    self._xval_inflight = 1
                try:
                    try:
                        done_next = getattr(self, "_xval_done", 0) + 1
                        total = max(done_next, getattr(self, "_xval_queued", 0))
                        self._xval_log(
                            f"cross-check {done_next}/{total}: {ticker} {interval}...")
                        entry = stock_validate.cross_validate_ticker(
                            self._storage_root, ticker, interval)
                        if entry.get("status") in ("structural-ok", "structural-flag"):
                            self._xval_log(entry.get("note") or
                                           f"{ticker} {interval}: structural check done")
                        # cross-val still records to its sidecar (feeds the Flags column
                        # + the double-click detail popup) — it just no longer owns a
                        # table column, so there is no cell to repaint here. Buffered:
                        # the popup may lag the run by up to 10 tickers until a flush.
                        pending.append(entry)
                        if len(pending) >= 10:
                            _flush()
                    except Exception as exc:  # noqa: BLE001 — validation never breaks a run
                        self._xval_log(
                            f"cross-check failed for {ticker} {interval}: {exc}")
                finally:
                    if getattr(self, "_xval_q", None) is q:
                        self._xval_done = getattr(self, "_xval_done", 0) + 1
                        self._xval_inflight = 0
        finally:
            _flush()               # EVERY exit path (cancel/drain/None) persists

    def _xval_finish(self) -> None:
        """Signal the validator to drain its queue, then stop."""
        ev = getattr(self, "_xval_stop", None)
        if ev is not None:
            ev.set()

    def _xval_retire(self) -> None:
        """Retire an old async validator before ticker-level coordination."""
        ev = getattr(self, "_xval_stop", None)
        if ev is not None:
            ev.set()
        thread = getattr(self, "_xval_thread", None)
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
        self._xval_q = None
        self._xval_done = 0
        self._xval_queued = 0
        self._xval_inflight = 0

    def _xval_pending_count(self, q=None) -> int:
        """Best-effort count of cross-validation work not yet drained."""
        inflight = max(0, int(getattr(self, "_xval_inflight", 0) or 0))
        try:
            q_left = max(0, int(q.qsize() or 0)) if q is not None else 0
        except Exception:  # noqa: BLE001
            q_left = 0
        return q_left + inflight

    def _xval_finalize_status(self, q=None) -> str:
        """Compact finalize readout for the GUI status line."""
        done = max(0, int(getattr(self, "_xval_done", 0) or 0))
        remaining = self._xval_pending_count(q)
        total = done + remaining
        line = f"Finalizing: cross-val {done}/{total}"
        if remaining:
            line += f" ({remaining} left)"
        return line

    def _pause_status_text(self, states=None, total_ports=0,
                           sealing=False) -> str:
        """Shared update/add-stock pause detail: live port-idle + xval count."""
        states = states or {}
        try:
            total = max(int(total_ports or 0), len(states))
        except Exception:  # noqa: BLE001
            total = len(states)
        idle_states = {"paused", "done", "error", "dead"}
        idle = 0
        for st in states.values():
            try:
                state = str((st or {}).get("state") or "")
            except Exception:  # noqa: BLE001
                state = ""
            if state in idle_states:
                idle += 1
        if total:
            line = f"Pausing: {idle}/{total} ports at a clean boundary"
        else:
            line = "Pausing: waiting for a clean boundary"
        line += f" - cross-val {self._xval_pending_count(getattr(self, '_xval_q', None))} left"
        if sealing:
            line += " - sealing..."
        return line

    def _earliest_set_cell(self, ticker, text) -> None:
        """Set the 'Earliest on IBKR' cell (column index 6) on EVERY row of
        `ticker`. Row layout is the full 9-wide (ticker,name,interval,months,
        coverage,rows,earliest,gaps,flags); pad to the LIVE column count so a
        column change can't reintroduce a width desync."""
        tree = getattr(self, "_storage_tree", None)
        if tree is None:
            return
        tkey = str(ticker).strip().upper()
        try:
            ncols = len(tree["columns"])
            for iid in tree.get_children():
                v = list(tree.item(iid)["values"])
                if v and str(v[0]).strip().upper() == tkey:
                    while len(v) < ncols:
                        v.append("")
                    v[6] = text
                    tree.item(iid, values=v)
        except Exception:  # noqa: BLE001 — tree gone / mid-rebuild
            pass

    def _storage_rescan(self, use_cache=True) -> None:
        """Kick a tree scan on a daemon thread. The worker NEVER touches
        tkinter — it only feeds a queue that _storage_poll (registered on
        the GUI thread) drains. Cross-thread tk calls are the classic
        intermittent-crash source; this is the safe pattern.

        use_cache=True (all AUTO rescans: app start, post-fetch, post-ingest,
        post-heal/Fix-data) rides the digest cache — unchanged tickers replay
        their summaries, ~8-9 s -> <1 s. The manual Rescan BUTTON passes
        use_cache=False: a user-invoked ground-truth full walk."""
        if self._storage_busy():
            return
        self._storage_scanning = True
        self._storage_btn.config(state=tk.DISABLED)
        self._storage_status.set("Scanning…")
        import queue
        import threading
        q = queue.Queue()
        self._storage_q = q

        def _progress(i, n, name):
            q.put(("progress", f"Scanning {name} ({i + 1}/{n})…"))

        def _work():
            try:
                res = stock_storage.scan_storage(self._storage_root,
                                                 progress=_progress,
                                                 digest_cache=use_cache)
            except Exception as exc:  # noqa: BLE001 — surface, never crash
                res = exc
            q.put(("done", res))

        try:
            threading.Thread(target=_work, daemon=True,
                             name="storage-scan").start()
        except Exception as exc:  # noqa: BLE001 — thread exhaustion: reset
            self._storage_scanning = False  # state or Rescan is dead for
            self._storage_q = None          # the rest of the session
            self._storage_btn.config(state=tk.NORMAL)
            self._storage_status.set(f"Scan failed to start: {exc}")
            return
        self.root.after(80, self._storage_poll)

    def _storage_poll(self) -> None:
        """GUI-thread drain of the scan worker's queue — the only place
        scan results touch widgets. Re-arms itself until 'done' arrives."""
        q = getattr(self, "_storage_q", None)
        if q is None:
            return
        import queue
        try:
            while True:
                kind, payload = q.get_nowait()
                if kind == "progress":
                    self._storage_status.set(payload)
                else:                      # ("done", result-or-exception)
                    self._storage_q = None
                    self._storage_scan_done(payload)
                    return
        except queue.Empty:
            pass
        except Exception as exc:  # noqa: BLE001 — a display bug must leave
            self._storage_q = None         # a truthful status line, never a
            self._storage_scanning = False  # stuck "Scanning..." forever
            try:
                self._storage_btn.config(state=tk.NORMAL)
                self._storage_status.set(
                    f"Scan finished; display failed: {exc}")
            except Exception:  # noqa: BLE001
                pass
            return
        try:
            self.root.after(80, self._storage_poll)
        except Exception:  # noqa: BLE001 — window closing
            pass

    def _storage_scan_done(self, res) -> None:
        """Populate the table + issues pane from a finished scan (GUI
        thread). `res` is the scan dict, or the exception a failed scan
        raised."""
        self._storage_scanning = False
        try:
            self._storage_btn.config(state=tk.NORMAL)
        except Exception:  # noqa: BLE001 — window closing
            return
        if isinstance(res, Exception):
            # Keep the last good table: a failed RESCAN wiping the whole
            # inventory reads as "my data is gone" — it is not.
            self._storage_status.set(f"Scan failed: {res}")
            return
        tree = self._storage_tree
        tree.delete(*tree.get_children())
        if not res["root_exists"]:
            self._storage_status.set(
                "No storage tree yet — the first ingest will create "
                f"'{stock_storage.STORAGE_DIR_NAME}'.")
            self._storage_set_issues([])
            return
        n_series = 0
        rows = []
        try:                                   # per-ticker earliest IBKR date
            emap = stock_validate.load_ibkr_earliest(self._storage_root)
        except Exception:  # noqa: BLE001
            emap = {}
        try:                                   # per-ticker IBKR company name
            nmap = stock_validate.load_ibkr_names(self._storage_root)
        except Exception:  # noqa: BLE001
            nmap = {}
        try:                                   # per-series interior-gap mark
            gmap = stock_validate.load_data_gaps(
                self._storage_root).get("series", {})
        except Exception:  # noqa: BLE001
            gmap = {}
        try:                                   # per-ticker 3-source double-flag
            cfmap = stock_validate.load_combined_flags(self._storage_root)
        except Exception:  # noqa: BLE001
            cfmap = {}
        try:                                   # per-series cross-validation verdicts
            xvmap = stock_validate.load_cross_validation(self._storage_root)
        except Exception:  # noqa: BLE001
            xvmap = {}
        for folder in sorted(res["tickers"]):
            t = res["tickers"][folder]
            _ces = stock_validate.combined_entries_for_ticker(cfmap, folder)
            _cf = [day for _ce in _ces
                   for day in (_ce.get("persistent") or [])]
            _cfmark = (f"⚑ 3-SOURCE disagreement: {len(_cf)} date(s) "
                       f"[double-click row] — " if _cf else "")
            _xvmark = self._xval_flag_summary(
                self._xval_entries_from_data(xvmap, folder))
            flags = _cfmark + _xvmark + "; ".join(t["flags"])
            nm = t.get("name", "") or nmap.get(str(folder).upper(), "")
            ed = emap.get(str(folder).upper(), "—") or "—"
            secid = t.get("conid") or "—"        # security_id (IBKR conId)
            if not t["intervals"]:
                rows.append((folder, nm, "—", "—", "—", "—",
                             ed, "—", flags or "no data files", secid))
                continue
            for iv, s in sorted(t["intervals"].items()):
                n_series += 1
                gg = self._gap_glyph(gmap.get(f"{str(folder).upper()} {iv}"))
                rows.append((folder, nm, iv, f"~{s['n_months'] / 12:.1f}",
                             f"{s['first']}  →  {s['last']}",
                             f"{s['rows']:,}", ed, gg, flags, secid))
                flags = ""            # ticker-level flags shown once
        self._storage_all_rows = rows
        self._storage_rows_version = getattr(self, "_storage_rows_version", 0) + 1
        self._storage_render_sig = None
        self._storage_build_iv_buttons({r[2] for r in rows})
        self._storage_render_rows()
        lines = []
        lines.extend(f"WARNING   {w}" for w in res["warnings"])
        lines.extend(f"MISSING   {f}/{iv} {m} — file vanished (AV "
                     f"quarantine? manual delete?) — tombstone kept, "
                     f"never auto-refetched"
                     for f, iv, m in res["missing"])
        lines.extend(f"UNREADABLE {p}: {msg}" for p, msg in res["errors"])
        lines.extend(f"inert     {p} ({r})" for p, r in res["unrecognized"])
        prefix = getattr(self, "_storage_issue_prefix", None)
        if prefix:                    # the just-finished ingest's summary
            lines = list(prefix) + ["", "--- tree scan ---"] + lines
            self._storage_issue_prefix = None
        self._storage_set_issues(lines)
        n_issues = (len(res["missing"]) + len(res["errors"])
                    + len(res["warnings"]))
        bits = [f"{len(res['tickers'])} ticker(s), {n_series} series",
                f"{n_issues} issue(s)" if n_issues else "no issues"]
        if res["unrecognized"]:
            bits.append(f"{len(res['unrecognized']):,} inert item(s)")
        self._storage_status.set(" — ".join(bits) + ".")

    def _storage_refresh_gaps_col(self) -> None:
        """Repaint ONLY the Gaps column of the main table from the freshly
        written `_data_gaps.json` sidecar — no disk walk (the cached scan rows
        are reused; only tuple index 7 changes). Falls back to a real rescan
        when no cached rows exist yet (table never scanned this session)."""
        rows = getattr(self, "_storage_all_rows", None)
        if rows is None:
            self._storage_rescan()
            return
        try:
            gmap = stock_validate.load_data_gaps(
                self._storage_root).get("series", {})
        except Exception:  # noqa: BLE001
            gmap = {}
        out = []
        for r in rows:
            if r[2] != "—":                # series rows only (col 2 = interval)
                gg = self._gap_glyph(gmap.get(f"{str(r[0]).upper()} {r[2]}"))
                r = r[:7] + (gg,) + r[8:]
            out.append(r)
        self._storage_all_rows = out
        self._storage_rows_version = getattr(self, "_storage_rows_version", 0) + 1
        self._storage_render_sig = None
        self._storage_render_rows()

    @staticmethod
    def _iv_seconds(iv):
        """Sort key — an interval token's bar length in seconds (s<m<h<d), so the
        filter buttons read 1s, 1m, 5m, 1h, 1d in natural order."""
        import re
        mt = re.match(r"(\d+)\s*([smhd])", str(iv))
        units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        return int(mt.group(1)) * units[mt.group(2)] if mt else 1 << 40

    @staticmethod
    def _storage_kind_key(interval) -> str:
        return stock_storage.kind_of(str(interval)) or "ohlc"

    @staticmethod
    def _storage_kind_name(kind) -> str:
        return {"ohlc": "OHLC", "iv": "IV", "hvol": "HVOL"}.get(
            str(kind), str(kind).upper())

    @classmethod
    def _storage_data_label(cls, interval) -> str:
        if str(interval) == "—":
            return "—"
        label = cls._storage_kind_name(cls._storage_kind_key(interval))
        try:
            sess = stock_storage.session_of(str(interval))
        except Exception:  # noqa: BLE001
            sess = "rth"
        if sess in ("pre", "post"):
            label += f" ({sess})"
        return label

    @classmethod
    def _storage_row_matches(cls, row, base_filter="all", kind_filter="all",
                             query="") -> bool:
        iv = str(row[2])
        if base_filter != "all" and stock_storage.base_interval(iv) != base_filter:
            return False
        if (base_filter != "all" and kind_filter != "all"
                and cls._storage_kind_key(iv) != kind_filter):
            return False
        q = str(query or "").strip().lower()
        if q:
            hay = " ".join((str(row[0]), str(row[1]),
                            cls._storage_data_label(iv))).lower()
            if q not in hay:
                return False
        return True

    def _storage_build_iv_buttons(self, intervals) -> None:
        """Rebuild the segmented interval bar: 'All' + one button per distinct BASE
        interval present (1m-pre/1m-iv collapse under their base 1m), sorted by bar
        length. If the active filter is no longer present, fall back to 'All'."""
        frame = getattr(self, "_storage_iv_btns", None)
        if frame is None:
            return
        for w in frame.winfo_children():
            w.destroy()
        bases = sorted({stock_storage.base_interval(str(iv)) for iv in intervals},
                       key=self._iv_seconds)
        if self._storage_iv_filter.get() not in (["all"] + bases):
            self._storage_iv_filter.set("all")

        def _interval_changed():
            try:
                self._storage_kind_filter.set("all")
            except Exception:  # noqa: BLE001
                pass
            self._storage_build_kind_buttons()
            self._storage_render_rows()

        def _mk(value, label):
            ttk.Radiobutton(frame, text=label, value=value, style="Toolbutton",
                            variable=self._storage_iv_filter,
                            command=_interval_changed
                            ).pack(side=tk.LEFT, padx=1)
        _mk("all", "All")
        for b in bases:
            _mk(b, b)
        self._storage_build_kind_buttons()

    def _storage_build_kind_buttons(self) -> None:
        strip = getattr(self, "_storage_kind_strip", None)
        frame = getattr(self, "_storage_kind_btns", None)
        var = getattr(self, "_storage_kind_filter", None)
        if strip is None or frame is None or var is None:
            return
        for w in frame.winfo_children():
            w.destroy()
        try:
            base = self._storage_iv_filter.get()
        except Exception:  # noqa: BLE001
            base = "all"
        rows = getattr(self, "_storage_all_rows", []) or []
        if base == "all":
            var.set("all")
            try:
                strip.pack_forget()
            except Exception:  # noqa: BLE001
                pass
            return
        present = {self._storage_kind_key(r[2]) for r in rows
                   if str(r[2]) != "—"
                   and stock_storage.base_interval(str(r[2])) == base}
        order = [k for k in ("ohlc", "iv", "hvol") if k in present]
        order += sorted(k for k in present if k not in set(order))
        if not order:
            var.set("all")
            try:
                strip.pack_forget()
            except Exception:  # noqa: BLE001
                pass
            return
        if var.get() not in (["all"] + order):
            var.set("all")
        try:
            if not strip.winfo_ismapped():
                strip.pack(side=tk.TOP, fill=tk.X,
                           after=getattr(self, "_storage_filter_bar", None))
        except Exception:  # noqa: BLE001
            try:
                strip.pack(side=tk.TOP, fill=tk.X)
            except Exception:  # noqa: BLE001
                pass

        def _mk(value, label):
            ttk.Radiobutton(frame, text=label, value=value, style="Toolbutton",
                            variable=var,
                            command=self._storage_render_rows).pack(
                                side=tk.LEFT, padx=1)
        _mk("all", "All")
        for k in order:
            _mk(k, self._storage_kind_name(k))

    def _storage_render_rows(self) -> None:
        """(Re)paint the preview tree from the cached scan rows, keeping only rows
        that match BOTH the active interval-filter button (by base interval; 'All'
        = any) and the 'Find:' box (case-insensitive substring on symbol/name)."""
        tree = getattr(self, "_storage_tree", None)
        if tree is None:
            return
        rows = getattr(self, "_storage_all_rows", [])
        q = ""
        try:
            q = self._storage_filter.get().strip().lower()
        except Exception:  # noqa: BLE001
            pass
        ivf = "all"
        try:
            ivf = self._storage_iv_filter.get()
        except Exception:  # noqa: BLE001
            pass
        kindf = "all"
        try:
            kindf = self._storage_kind_filter.get()
        except Exception:  # noqa: BLE001
            pass
        if ivf == "all":
            kindf = "all"
        sig = (getattr(self, "_storage_rows_version", 0), len(rows), q, ivf,
               kindf)
        if sig == getattr(self, "_storage_render_sig", None):
            return
        try:
            yview = tree.yview()
            y0 = float(yview[0]) if yview else 0.0
        except Exception:  # noqa: BLE001
            y0 = 0.0
        try:
            tree.delete(*tree.get_children())
        except Exception:  # noqa: BLE001 — window closing
            return
        shown = 0
        for r in rows:
            if not self._storage_row_matches(r, ivf, kindf, q):
                continue
            try:
                vals = list(r)
                vals[1] = self._storage_data_label(r[2])
                tree.insert("", tk.END, values=vals,
                            tags=("oddrow" if shown % 2 else "evenrow",))
                shown += 1
            except Exception:  # noqa: BLE001
                break
        try:
            tree.yview_moveto(y0)
        except Exception:  # noqa: BLE001
            pass
        self._storage_render_sig = sig
        try:
            if q or ivf != "all":
                self._storage_filter_count.set(
                    f"{shown} of {len(rows)} row(s)")
            else:
                self._storage_filter_count.set("")
        except Exception:  # noqa: BLE001
            pass

    def _storage_set_issues(self, lines) -> None:
        try:
            txt = self._storage_issues
            txt.config(state=tk.NORMAL)
            txt.delete("1.0", tk.END)
            shown = list(lines[:400])
            if len(lines) > 400:
                shown.append(f"... ({len(lines) - 400:,} more not shown)")
            txt.insert(tk.END, "\n".join(shown) if shown
                       else "(nothing to report)")
            txt.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001
            pass

    # ---- Tier 1: ingest -------------------------------------------------

    def _storage_ingest_files(self) -> None:
        if self._storage_busy():
            return
        from tkinter import filedialog
        paths = filedialog.askopenfilenames(
            parent=self.root, title="Ingest data files",
            initialdir=str(self._storage_root.parent),
            filetypes=[("Data files", "*.csv *.txt *.parquet *.pq"),
                       ("All files", "*.*")])
        if paths:
            self._storage_ingest_start(list(paths))

    def _storage_ingest_folder(self) -> None:
        if self._storage_busy():
            return
        from tkinter import filedialog
        path = filedialog.askdirectory(
            parent=self.root, title="Ingest a folder (recursive)",
            initialdir=str(self._storage_root.parent), mustexist=True)
        if path:
            self._storage_ingest_start([path])

    def _storage_ingest_start(self, paths) -> None:
        """Run stock_ingest on a daemon thread. Same discipline as the
        scan: the worker never touches tkinter, results arrive through a
        queue drained on the GUI thread. Sources are read-only to the
        engine; cancelling is safe (whole months commit atomically)."""
        if self._storage_busy():
            return
        import queue
        import threading
        self._storage_ingesting = True
        self._storage_btn.config(state=tk.DISABLED)
        self._storage_ingest_files_btn.config(state=tk.DISABLED)
        self._storage_ingest_dir_btn.config(state=tk.DISABLED)
        self._storage_cancel_btn.config(state=tk.NORMAL)
        self._storage_status.set("Ingest starting…")
        q = queue.Queue()
        ev = threading.Event()
        self._storage_iq = q
        self._storage_cancel_ev = ev

        def _work():
            try:
                rep = stock_ingest.ingest_paths(
                    paths, self._storage_root,
                    progress=lambda m: q.put(("progress", m)), cancel=ev,
                    skip_contained=True)
            except Exception as exc:  # noqa: BLE001 — surface, never crash
                rep = exc
            q.put(("done", rep))

        try:
            threading.Thread(target=_work, daemon=True,
                             name="storage-ingest").start()
        except Exception as exc:  # noqa: BLE001 — thread exhaustion
            self._storage_ingest_reset()
            self._storage_status.set(f"Ingest failed to start: {exc}")
            return
        self.root.after(80, self._storage_ingest_poll)

    def _storage_ingest_cancel(self) -> None:
        ev = getattr(self, "_storage_cancel_ev", None)
        if ev is not None:
            ev.set()
            self._storage_cancel_btn.config(state=tk.DISABLED)
            self._storage_status.set(
                "Cancelling — finishing the current month write…")

    def _storage_ingest_reset(self) -> None:
        """Back to idle button states, truthfully, even mid-failure."""
        self._storage_ingesting = False
        self._storage_iq = None
        self._storage_cancel_ev = None
        try:
            self._storage_btn.config(state=tk.NORMAL)
            self._storage_ingest_files_btn.config(state=tk.NORMAL)
            self._storage_ingest_dir_btn.config(state=tk.NORMAL)
            self._storage_cancel_btn.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001 — window closing
            pass

    def _storage_ingest_poll(self) -> None:
        q = getattr(self, "_storage_iq", None)
        if q is None:
            return
        import queue
        try:
            while True:
                kind, payload = q.get_nowait()
                if kind == "progress":
                    self._storage_status.set(payload)
                else:
                    self._storage_ingest_done(payload)
                    return
        except queue.Empty:
            pass
        except Exception as exc:  # noqa: BLE001 — truthful status, never
            self._storage_ingest_reset()    # a stuck "Ingesting…" forever
            try:
                self._storage_status.set(
                    f"Ingest finished; display failed: {exc}")
            except Exception:  # noqa: BLE001
                pass
            return
        try:
            self.root.after(80, self._storage_ingest_poll)
        except Exception:  # noqa: BLE001 — window closing
            pass

    def _storage_ingest_done(self, rep) -> None:
        """Render the run report (GUI thread), then rescan so the table
        reflects the new tree."""
        self._storage_ingest_reset()
        if isinstance(rep, Exception):
            self._storage_status.set(f"Ingest failed: {rep}")
            return
        self._last_ingest_report = rep
        try:
            lines = stock_ingest.summarize_report(rep)
        except Exception as exc:  # noqa: BLE001
            lines = [f"(report rendering failed: {exc})"]
        self._storage_set_issues(lines)
        self._storage_issue_prefix = lines    # survives the rescan below
        t = rep.get("totals", {})
        bits = [f"Ingest done: +{t.get('added', 0):,} rows",
                f"{t.get('written', 0)} month file(s)"]
        if t.get("conflicts"):
            bits.append(f"{t['conflicts']:,} conflict(s) — existing kept")
        if t.get("quarantined_files"):
            bits.append(f"{t['quarantined_files']} file(s) quarantined")
        if rep.get("cancelled"):
            bits.append("CANCELLED — finished part is committed; re-run "
                        "to complete")
        self._storage_status.set(" — ".join(bits)
                                 + ". Details below; rescanning…")
        self._storage_rescan()

    # ---- Tier 2: IBKR gap-fill ------------------------------------------

    def _storage_busy(self) -> bool:
        return (self._storage_scanning or self._storage_ingesting
                or getattr(self, "_storage_ibkr_running", False)
                or getattr(self, "_storage_find_building", False)
                or getattr(self, "_storage_export_busy", False)
                or getattr(self, "_storage_exd_running", False)
                or getattr(self, "_storage_fixdata_running", False))

    def _storage_tws_app_dialog(self, *, missing=False):
        """Browse-first modal for a missing app and the manual backup action."""
        import tws_discovery

        result = {"path": None}
        download_shown = {"value": False}
        top = tk.Toplevel(self.root)
        top.title("IBKR/TWS was not found" if missing else "Set IBKR app")
        top.transient(self.root)
        top.resizable(False, False)
        message = tk.StringVar(value=(
            "IBKR/TWS was not found in its standard locations.\n"
            "Browse to the IBKR app (tws.exe); a valid choice is remembered "
            "for this machine."
            if missing else
            "Browse to the IBKR app (tws.exe). The choice is remembered only "
            "for this machine and re-validated on every start."))
        ttk.Label(top, textvariable=message, justify=tk.LEFT,
                  wraplength=470).pack(fill=tk.X, padx=14, pady=(14, 10))
        actions = ttk.Frame(top)
        actions.pack(fill=tk.X, padx=14, pady=(0, 14))

        def show_download_guidance():
            download_shown["value"] = True
            message.set(
                "IBKR/TWS still was not found. It appears not to be installed "
                "and needs to be downloaded before multi-port startup can "
                "continue. You can browse again after installing it.")
            browse_btn.config(text="Browse again...")
            cancel_btn.config(text="Close")

        def browse():
            selected = filedialog.askopenfilename(
                parent=top,
                title="Browse to the IBKR app...",
                filetypes=(("IBKR/TWS application", "tws.exe"),
                           ("Windows applications", "*.exe"),
                           ("All files", "*.*")))
            if not selected:
                if missing:
                    show_download_guidance()
                return
            try:
                result["path"] = tws_discovery.remember_tws(selected)
            except Exception as exc:  # noqa: BLE001 - keep the chooser open
                message.set(
                    "That selection cannot be used. Choose the existing "
                    f"tws.exe application file.\n\n{exc}")
                return
            top.destroy()

        def cancel():
            if missing and not download_shown["value"]:
                show_download_guidance()
                return
            top.destroy()

        browse_btn = ttk.Button(
            actions, text="Browse to the IBKR app...", command=browse)
        browse_btn.pack(side=tk.LEFT)
        cancel_btn = ttk.Button(actions, text="Cancel", command=cancel)
        cancel_btn.pack(side=tk.RIGHT)
        top.protocol("WM_DELETE_WINDOW", cancel)
        top.update_idletasks()
        x = self.root.winfo_rootx() + max(
            0, (self.root.winfo_width() - top.winfo_width()) // 2)
        y = self.root.winfo_rooty() + max(
            0, (self.root.winfo_height() - top.winfo_height()) // 2)
        top.geometry(f"+{x}+{y}")
        top.grab_set()
        self.root.wait_window(top)
        return result["path"]

    def _storage_set_tws_app(self):
        """Always-available manual backup for selecting ``tws.exe``."""
        selected = self._storage_tws_app_dialog(missing=False)
        if selected:
            self._storage_status.set(
                f"IBKR/TWS app remembered for this machine: {selected}")
        return selected

    def _storage_resolve_tws_app(self):
        """Run discovery first on every startup, then offer the browse flow."""
        import tws_discovery
        try:
            resolved, _source = tws_discovery.resolve_tws()
        except Exception:  # noqa: BLE001 - the explicit chooser is the fallback
            resolved = None
        if resolved is not None:
            return resolved
        return self._storage_tws_app_dialog(missing=True)

    def _tws_ports(self):
        """The TWS port(s) to use, read on the GUI thread. SERIES mode ->
        the single field as a one-tuple. PARALLEL mode -> every valid port
        the user added (one per account). Falls back to the engine default
        list when nothing valid is set.

        NOTE (UI scaffolding): the concurrent-fetch engine is a later step.
        Until it lands, the engine treats a multi-port tuple as FAILOVER
        (first that answers), so a Parallel run currently executes serially
        on the first reachable port — the extra ports are captured and ready
        for the orchestrator, not yet fetched in parallel."""
        if self._parallel_enabled():
            ports = self._valid_port_list(self._storage_tws_ports_list)
            if ports:
                return tuple(ports)
            return stock_ibkr.PORTS_DEFAULT
        try:
            p = int(str(self._storage_tws_port.get()).strip())
            if 1 <= p <= 65535:
                return (p,)
        except (ValueError, AttributeError):
            pass
        return stock_ibkr.PORTS_DEFAULT

    def _tws_factory(self):
        """A live adapter factory pinned to the chosen port — pass to
        the engine calls that take adapter_factory (find_symbol,
        estimate_backfill, gap_fill). Build on the GUI thread, call in
        the worker."""
        return stock_ibkr.live_adapter_factory(
            stock_ibkr.HOST_DEFAULT, self._tws_ports())

    def _parallel_enabled(self):
        """True when the storage-tab TWS mode is Parallel (default Series)."""
        try:
            return bool(self._storage_tws_parallel.get())
        except Exception:  # noqa: BLE001 — called before the bar is built
            return False

    def _extended_enabled(self):
        """True when '+ extended hours' is ticked."""
        try:
            return bool(self._storage_tws_extended.get())
        except Exception:  # noqa: BLE001
            return False

    @staticmethod
    def _extended_applicable(tokens):
        """True iff a canonical token can have pre/post session variants.

        Extended sessions apply only to unsuffixed sub-daily TRADES tokens.
        Daily, volatility-kind, and already session-suffixed tokens are inert.
        This decision intentionally has no GUI or shared-variable side effects.
        """
        for raw in tokens or ():
            token = str(raw or "").strip().lower()
            if ("-" not in token and len(token) > 1
                    and token[:-1].isdigit()
                    and token[-1] in ("s", "m", "h")):
                return True
        return False

    def _set_extended_checkbox_state(self, checkbox, tokens):
        """Render applicability without changing the shared checked value."""
        applicable = self._extended_applicable(tokens)
        checkbox.config(
            state="normal" if applicable else "disabled",
            text=("+ extended hours" if applicable
                  else "+ extended hours (n/a)"))
        return applicable

    def _expand_extended(self, selections):
        """Add pre/post variants for unsuffixed sub-daily TRADES tokens.

        Daily, volatility-kind, and already session-suffixed selections pass
        through. Order-preserving and de-duplicated.
        """
        if not self._extended_enabled():
            return list(selections)
        out, seen = [], set()
        for t, iv in selections:
            token = str(iv or "").strip().lower()
            subdaily = ("-" not in token and len(token) > 1
                        and token[:-1].isdigit()
                        and token[-1] in ("s", "m", "h"))
            variants = ([iv, f"{iv}-pre", f"{iv}-post"]
                        if subdaily else [iv])
            for v in variants:
                if (t, v) not in seen:
                    seen.add((t, v))
                    out.append((t, v))
        return out

    def _daily_enabled(self):
        """True when 'store daily (1d)' is ticked."""
        try:
            return bool(self._storage_tws_daily.get())
        except Exception:  # noqa: BLE001
            return False

    def _expand_daily(self, selections):
        """If 'store daily (1d)' is on, ALSO fetch+store each SUB-DAILY TRADES
        ticker's daily (1d) series so the bank keeps a daily alongside the
        intraday (and the internal check can compare 1m->daily vs the stored 1d).
        Adds (ticker,'1d') ONCE per ticker; kinds (-iv/-hvol) and already-daily
        selections are left alone. Order-preserving, de-duplicated."""
        if not self._daily_enabled():
            return list(selections)
        out, seen = [], set()
        for t, iv in selections:
            if (t, iv) not in seen:
                seen.add((t, iv))
                out.append((t, iv))
            if (stock_storage.kind_of(iv) == ""
                    and stock_storage.base_interval(iv) != "1d"
                    and (t, "1d") not in seen):
                seen.add((t, "1d"))
                out.append((t, "1d"))
        return out

    def _tws_estimate_lanes(self):
        """How many accounts a run would fan work across — feeds the parallel
        time estimate. Parallel mode -> the configured ports that are actually
        LISTENING (so the estimate tracks the LIVE fleet, e.g. 5 vs 8), counting
        only ports the run could really use. Falls back to the configured count if
        none probe as up yet (fleet not started); Series mode -> 1. The localhost
        probe is fast (a closed port refuses instantly)."""
        try:
            if self._parallel_enabled():
                ports = self._valid_port_list(self._tws_ports())
                if not ports:
                    return 1
                live = sum(1 for p in ports
                           if stock_ibkr._port_open("127.0.0.1", p, 0.4))
                return max(1, live or len(set(ports)))
        except Exception:  # noqa: BLE001
            pass
        return 1

    @staticmethod
    def _valid_port_list(raw):
        """[anything] -> ordered, de-duplicated list of valid TCP ports."""
        out = []
        for v in raw or ():
            try:
                p = int(str(v).strip())
            except (ValueError, TypeError):
                continue
            if 1 <= p <= 65535 and p not in out:
                out.append(p)
        return out

    def _fleet_callbacks(self, say, ports, cancel=None):
        """(restart_ports, port_up) for gap_fill_parallel_resilient — built on
        the fetch WORKER thread (touches tkinter only via root.after).
        restart_ports is the SAFE-RESTART wrapper: a Yes/No popup (30s
        auto-CONTINUE — unattended runs self-heal the daily logout; ESC/'Not
        now' declines), then minimize-all + BLOCK input (InputGuard: ESC abort +
        watchdog) while relaunching each port (abort-aware), then restore.
        port -> email comes from the recorded fleet map. (None, None) if the
        launcher is unavailable (then no auto-restart)."""
        plist = list(dict.fromkeys(int(p) for p in ports))
        try:
            import tws_launch
            import tws_inputguard
        except Exception:  # noqa: BLE001 — launcher unavailable
            return None, None

        def port_up(p):
            try:
                return bool(tws_launch.port_open(int(p)))
            except Exception:  # noqa: BLE001
                return False

        def restart_ports(batch, phase=None):
            return self._safe_restart_ports(
                [int(p) for p in batch], plist, say, tws_launch,
                tws_inputguard, phase=phase,
                cancelled=(cancel.is_set if cancel is not None else None))

        # stock_ibkr preserves the one-argument callback contract by default;
        # this explicit opt-in lets production copy identify a mid-run offer.
        restart_ports._accepts_restart_phase = True

        return restart_ports, port_up

    def _safe_restart_ports(self, batch, plist, say, tws_launch, guard_mod,
                            phase=None, cancelled=None):
        """Request one rendered confirmation, then safely restart the batch.

        Worker-thread safe. Every port returns the strict structured outcome
        consumed by stock_ibkr's restart-event normalizer.
        """
        if not batch:
            return {}
        try:
            emails = {
                p: tws_launch.email_for_port(
                    p, fallback_index=(plist.index(p) if p in plist else 0))
                for p in batch
            }
        except Exception as exc:  # noqa: BLE001
            reason = tws_restart_handshake.error_reason(
                "fleet account mapping failed", exc)
            return tws_restart_handshake.failed_outcomes(
                batch, "fleet_mapping_error", reason)

        def schedule_popup(state):
            self.root.after(
                0, lambda: self._fleet_restart_popup(
                    batch, emails, state, phase=phase))

        budget = addstock_watchdog.maintenance_budget(
            len(batch), per_port_s=220.0, overhead_s=180.0)
        with addstock_watchdog.maintenance_window(
                tws_launch.fleet_path(), batch, budget, progress=say):
            return tws_restart_handshake.restart_ports_with_confirmation(
                batch, emails, schedule_popup, say, guard_mod,
                tws_launch.relaunch_one,
                readiness_policy=tws_launch.bringup.ReadinessPolicy(
                    account_timeout=150.0, port_timeout=60.0),
                cancelled=cancelled, port_up=tws_launch.port_open)

    def _fleet_restart_popup(self, ports, emails, decision, phase=None):
        """Yes/No confirmation on the GUI thread with a 30s auto-CONTINUE countdown
        (user choice 2026-07-08, reverting the P1 auto-decline: silence = restart,
        so an unattended overnight run survives the daily logout). A complete
        layout is marked rendered before any confirmation can drive TWS. Worker
        expiry wins atomically over every delayed callback."""
        popup = {"win": None, "countdown": None, "left": 30}

        def cleanup():
            win = popup["win"]
            popup["win"] = None
            if win is None:
                return
            try:
                win.grab_release()
            except Exception:  # noqa: BLE001
                pass
            try:
                win.destroy()
            except Exception:  # noqa: BLE001
                pass

        def settle(go, token, reason):
            decision.settle(go, token, reason)
            cleanup()

        def build():
            # Parent to whichever fetch dialog is ON TOP (Add-stock / IBKR-
            # update), else the main window. This keeps the warning visible.
            parent = self.root
            for attr in ("_storage_find_win", "_storage_ibkr_win"):
                w = getattr(self, attr, None)
                try:
                    if w is not None and w.winfo_exists():
                        parent = w
                        break
                except Exception:  # noqa: BLE001
                    pass

            fleet_down = phase == "fleet_down"
            midrun = phase in ("midrun", "fleet_down")
            win = tk.Toplevel(parent)
            popup["win"] = win
            if fleet_down:
                win.title("Restart the confirmed-dead TWS fleet mid-run?")
            else:
                win.title("Restart confirmed-dead TWS port(s) mid-run?" if midrun
                          else "Restart logged-out TWS port(s)?")
            win.transient(parent)
            try:
                win.grab_set()
                win.lift()
                win.attributes("-topmost", True)

                def drop_topmost():
                    try:
                        win.attributes("-topmost", False)
                    except Exception:  # noqa: BLE001
                        pass

                win.after(400, drop_topmost)
                win.focus_force()
            except Exception:  # noqa: BLE001
                pass
            try:
                import tws_inputguard
                frozen = ("FREEZES your mouse/keyboard, "
                          if tws_inputguard.is_elevated()
                          else "(won't freeze input unless the app runs as "
                               "admin) ")
            except Exception:  # noqa: BLE001
                frozen = ""
            ttk.Label(
                win, justify=tk.LEFT, wraplength=470,
                text=(("MID-RUN: every configured TWS port is down. The data "
                       "run is holding at a recoverable boundary and will "
                       "resume if relaunch succeeds. App input may be blocked "
                       "briefly during relaunch.\n\n")
                      if fleet_down else
                      (("MID-RUN: the data run continues on surviving ports. "
                        "App input may be blocked briefly during relaunch.\n\n")
                       if midrun else ""))
                + (f"{len(ports)} TWS port(s) need a restart (reconnect "
                      f"failed or logged out):\n   "
                      + ", ".join(f"{p} ({emails[p]})" for p in ports)
                      + f"\n\nThis MINIMIZES your windows, {frozen}and drives "
                      "the TWS login (~1-2 min per port). Press ESC to abort. "
                      "Ctrl+Alt+Del is the failsafe.")).pack(
                anchor=tk.W, padx=12, pady=(12, 6))
            countdown = ttk.Label(win, foreground="#666")
            countdown.pack(anchor=tk.W, padx=12)
            popup["countdown"] = countdown
            btns = ttk.Frame(win)
            btns.pack(anchor=tk.E, padx=12, pady=(8, 12))
            ttk.Button(
                btns, text="Restart now",
                command=lambda: settle(
                    True, tws_restart_handshake.USER_CONFIRMED,
                    "restart confirmed in rendered popup")).pack(
                side=tk.LEFT, padx=(0, 6))
            ttk.Button(
                btns, text="Not now",
                command=lambda: settle(
                    False, tws_restart_handshake.NOT_NOW_DECLINED,
                    "restart declined with Not now")).pack(side=tk.LEFT)
            win.protocol(
                "WM_DELETE_WINDOW",
                lambda: settle(
                    False, tws_restart_handshake.WINDOW_CLOSED_DECLINED,
                    "restart popup closed without confirmation"))
            win.bind(
                "<Escape>",
                lambda _event: settle(
                    False, tws_restart_handshake.ESCAPE_DECLINED,
                    "restart declined with Escape"))
            win.update_idletasks()
            width, height = win.winfo_width(), win.winfo_height()
            win.geometry(
                f"+{max(0, (win.winfo_screenwidth() - width) // 2)}"
                f"+{max(0, (win.winfo_screenheight() - height) // 3)}")

        def tick():
            state = decision.snapshot()
            if state.settled:
                cleanup()
                return
            if popup["left"] <= 0:
                settle(True, tws_restart_handshake.AUTO_CONFIRMED,
                       "rendered popup auto-confirmed after 30 seconds")
                return
            try:
                popup["countdown"].config(
                    text=f"Auto-RESTARTS in {popup['left']}s... "
                         f"(ESC or 'Not now' to decline)")
                popup["left"] -= 1
                popup["win"].after(1000, tick)
                decision.mark_countdown_armed()
            except Exception as exc:  # noqa: BLE001
                decision.settle(
                    False, tws_restart_handshake.POPUP_RENDER_ERROR,
                    tws_restart_handshake.error_reason(
                        "restart popup update failed", exc))
                cleanup()

        if not tws_restart_handshake.render_popup_safely(
                decision, build, cleanup):
            return
        tick()

    def _fleet_preflight(self, say, ports, cancel=None):
        """Before a run, restart any down fleet port (the daily-logout case) as
        ONE batch (one popup). Only acts in Parallel mode; a no-op when every
        port is up or the launcher is unavailable. Worker-thread safe. `cancel`
        is honoured on entry AND before the (unbounded, minutes-long) restart
        batch — a user cancel must never launch a TWS relaunch."""
        if cancel is not None and cancel.is_set():
            return
        if not self._parallel_enabled():
            return
        restart_ports, port_up = self._fleet_callbacks(
            say, ports, cancel=cancel)
        if restart_ports is None or port_up is None:
            return
        down = [p for p in dict.fromkeys(int(x) for x in ports)
                if not port_up(p)]
        if down:
            if cancel is not None and cancel.is_set():
                return
            say(f"{len(down)} port(s) down (logged out?) — restarting before "
                f"the run: {', '.join(map(str, down))}")
            restart_ports(down)

    def _storage_tws_mode_changed(self):
        """Swap the single-port field (Series) for the 'Ports (N)…' editor
        button (Parallel). Default is Series; while it's unticked the
        multi-port fleet is ignored entirely."""
        try:
            parallel = bool(self._storage_tws_parallel.get())
        except Exception:  # noqa: BLE001
            parallel = False
        for w in (self._storage_tws_port_entry, self._storage_tws_ports_btn):
            w.pack_forget()
        if parallel:
            self._storage_tws_ports_btn.pack(side=tk.LEFT)
            self._storage_tws_update_ports_btn()
        else:
            self._storage_tws_port_entry.pack(side=tk.LEFT)

    def _storage_tws_update_ports_btn(self):
        """Refresh the 'Ports (N)…' button to show how many valid ports."""
        n = len(self._valid_port_list(self._storage_tws_ports_list))
        try:
            self._storage_tws_ports_btn.config(text=f"Ports ({n})…")
        except tk.TclError:
            pass

    def _storage_tws_ports_editor(self):
        """Add/remove the Parallel-mode ports — one per IBKR account/Gateway.
        '+ Add port' appends a row; ✕ removes one; 'Preset 2000-9000' fills the
        standard 8-instance fleet; saved on Done."""
        fleet = tuple(str(p) for p in self._STANDARD_FLEET)
        top = tk.Toplevel(self.root)
        top.title("Parallel ports")
        top.transient(self.root)
        top.resizable(False, False)
        ttk.Label(
            top, text="One port per account / Gateway — each account is a "
                      "separate 60-request / 10-min budget:"
        ).grid(row=0, column=0, columnspan=2, sticky="w",
               padx=10, pady=(10, 2))
        ttk.Label(
            top, foreground="#777",
            text="Parallel fetch isn't wired to run concurrently yet — for "
                 "now a run uses the first reachable port. The ports you add "
                 "here are saved and ready for it."
        ).grid(row=1, column=0, columnspan=2, sticky="w", padx=10, pady=(0, 8))
        rows = ttk.Frame(top)
        rows.grid(row=2, column=0, columnspan=2, sticky="we", padx=10)
        row_vars = []

        scan_box = ttk.LabelFrame(top, text="Scan for live ports")
        scan_box.grid(row=3, column=0, columnspan=2, sticky="we",
                      padx=10, pady=(10, 0))
        scan_status = tk.StringVar(
            value="Probe the standard local TWS/Gateway ports.")
        ttk.Label(scan_box, textvariable=scan_status).pack(
            anchor="w", padx=8, pady=(6, 2))
        scan_results = ttk.Frame(scan_box)
        scan_results.pack(fill=tk.X, padx=8, pady=(0, 6))

        def render():
            for child in rows.winfo_children():
                child.destroy()
            for i, var in enumerate(row_vars):
                ttk.Entry(rows, textvariable=var, width=8).grid(
                    row=i, column=0, sticky="w", pady=2)
                ttk.Button(rows, text="✕", width=3,
                           command=lambda k=i: remove(k)).grid(
                    row=i, column=1, padx=(6, 0))

        def add(initial=""):
            row_vars.append(tk.StringVar(value=str(initial)))
            render()

        def remove(k):
            if 0 <= k < len(row_vars):
                row_vars.pop(k)
            if not row_vars:                       # never leave it fully empty
                row_vars.append(tk.StringVar(value=""))
            render()

        def preset():
            row_vars.clear()                       # fill with the 8-port fleet
            for p in fleet:
                row_vars.append(tk.StringVar(value=p))
            render()

        def add_scanned(port):
            port = str(port)
            if port in {v.get().strip() for v in row_vars}:
                return
            blank = next((v for v in row_vars if not v.get().strip()), None)
            if blank is not None:
                blank.set(port)
                render()
            else:
                add(port)

        def finish_scan(found, error=None):
            try:
                if not top.winfo_exists():
                    return
            except tk.TclError:
                return
            scan_btn.config(state=tk.NORMAL)
            for child in scan_results.winfo_children():
                if child is not scan_btn:
                    child.destroy()
            if error is not None:
                scan_status.set(f"Port scan failed: {error}")
                return
            if not found:
                scan_status.set("No listening TWS/Gateway ports were found.")
                return
            scan_status.set(
                "Listening: " + ", ".join(map(str, found))
                + " (choose any port to add it)")
            for port in found:
                ttk.Button(
                    scan_results, text=f"+ {port}",
                    command=lambda p=port: add_scanned(p)).pack(
                        side=tk.LEFT, padx=(0, 5))

        def scan():
            import threading
            import tws_discovery
            configured = self._valid_port_list([v.get() for v in row_vars])
            candidates = sorted(
                set(configured) | set(self._STANDARD_FLEET)
                | set(stock_ibkr.PORTS_DEFAULT))
            scan_btn.config(state=tk.DISABLED)
            scan_status.set("Scanning local loopback ports...")
            completed = threading.Event()
            payload = {}

            def work():
                try:
                    payload["found"] = tws_discovery.scan_listening_ports(
                        candidates=candidates)
                except Exception as exc:  # noqa: BLE001 - report in the popup
                    payload["found"], payload["error"] = [], exc
                finally:
                    completed.set()

            def poll():
                if completed.is_set():
                    finish_scan(payload.get("found", []), payload.get("error"))
                    return
                try:
                    top.after(50, poll)
                except Exception:  # noqa: BLE001 - dialog may close mid-scan
                    pass

            threading.Thread(target=work, daemon=True).start()
            top.after(50, poll)

        def done():
            ports = self._valid_port_list([v.get() for v in row_vars])
            self._storage_tws_ports_list = [str(p) for p in ports] or ["7497"]
            self._storage_tws_update_ports_btn()
            top.destroy()

        for p in (self._storage_tws_ports_list or ["7497"]):
            add(p)
        scan_btn = ttk.Button(
            scan_results, text="Scan for live ports", command=scan)
        scan_btn.pack(side=tk.LEFT)
        btns = ttk.Frame(top)
        btns.grid(row=4, column=0, columnspan=2, sticky="we",
                  padx=10, pady=10)
        ttk.Button(btns, text="+ Add port",
                   command=lambda: add("")).pack(side=tk.LEFT)
        ttk.Button(btns, text="Preset 2000–9000",
                   command=preset).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(btns, text="Apply", command=done).pack(side=tk.RIGHT)
        top.update_idletasks()                     # centre the popup on screen
        w, h = top.winfo_width(), top.winfo_height()
        x = (top.winfo_screenwidth() - w) // 2
        y = (top.winfo_screenheight() - h) // 2
        top.geometry(f"+{x}+{y}")
        top.grab_set()

    def _storage_tws_gate(self) -> bool:
        """Connectivity is a PRECONDITION for the TWS-backed dialogs,
        not a button inside them: nothing is listening -> popup, the
        window never opens. (TWS may still refuse later — login/API
        settings — which the preflight catches before any write.)"""
        try:
            if stock_ibkr.tws_listening(ports=self._tws_ports()):
                return True
        except Exception:  # noqa: BLE001 — treat a probe crash as down
            pass
        try:
            messagebox.showwarning(
                "Not connected",
                "TWS / IB Gateway is not running (or not logged in).\n\n"
                "Connect first — this window doesn't work without it.",
                parent=self.root)
        except Exception:  # noqa: BLE001 — headless
            pass
        return False

    def _storage_ibkr_open(self) -> None:
        """The IBKR update dialog: per-series gap view, selection, and
        the paced fetch on a worker thread. TWS connectivity is gated
        at the door — the window only opens when something listens."""
        if self._storage_busy():
            return
        if not self._storage_tws_gate():
            return
        if getattr(self, "_storage_ibkr_win", None) is not None:
            try:
                self._storage_ibkr_win.lift()
                return
            except Exception:  # noqa: BLE001 — destroyed window
                self._storage_ibkr_win = None
        win = tk.Toplevel(self.root)
        win.title("IBKR update — gap-fill from TWS/Gateway")
        win.geometry("860x560")
        win.transient(self.root)
        self._storage_ibkr_win = win
        win.protocol("WM_DELETE_WINDOW", self._storage_ibkr_close)

        top = ttk.Frame(win, padding=(8, 6))
        top.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(top, text="Interval:").pack(side=tk.LEFT, padx=(0, 2))
        self._ibkr_interval = tk.StringVar(value="1m")
        halted_ivs = set()
        try:
            halted_ivs = {
                str(k).rsplit(" ", 1)[1]
                for k in stock_ibkr.load_halted_series(self._storage_root)
                if " " in str(k)}
        except Exception:  # noqa: BLE001 - advisory retry rows only
            halted_ivs = set()
        ivs = sorted({"1m", "1s", "1h"} | halted_ivs | {
            iv for _t, _ivs in self._storage_known_series()
            for iv in _ivs})
        iv_box = ttk.Combobox(top, textvariable=self._ibkr_interval,
                              values=ivs, width=5, state="readonly")
        iv_box.pack(side=tk.LEFT)
        # every row here is a plan for ONE interval, so a changed combobox
        # must re-plan — otherwise the table silently keeps describing the
        # interval the user just navigated away from.
        iv_box.bind("<<ComboboxSelected>>",
                    self._storage_ibkr_interval_changed)
        ttk.Button(top, text="Refresh",
                   command=self._storage_ibkr_refresh).pack(
            side=tk.LEFT, padx=(12, 0))
        ttk.Button(top, text="Select all",
                   command=lambda: self._ibkr_tree.selection_set(
                       self._ibkr_tree.get_children())).pack(
            side=tk.LEFT, padx=(6, 0))
        self._ibkr_start_btn = ttk.Button(
            top, text="Start update", command=self._storage_ibkr_start)
        self._ibkr_start_btn.pack(side=tk.LEFT, padx=(12, 0))
        self._ibkr_pause_btn = ttk.Button(
            top, text="Pause", state=tk.DISABLED,
            command=self._storage_ibkr_pause)
        self._ibkr_pause_btn.pack(side=tk.LEFT, padx=(6, 0))
        self._ibkr_cancel_btn = ttk.Button(
            top, text="Cancel", state=tk.DISABLED,
            command=self._storage_ibkr_cancel)
        self._ibkr_cancel_btn.pack(side=tk.LEFT, padx=(6, 0))
        # The options live on their OWN row. Packed beside the action buttons
        # they needed ~938 px inside an 860 px window, so the right-hand group
        # (Lookback especially) was squeezed off the edge — the combobox text
        # itself always fitted, the ROW did not. A second row cannot overflow
        # no matter how narrow the window gets, and nothing is pushed off.
        opts = ttk.Frame(win, padding=(8, 0, 8, 6))
        opts.pack(side=tk.TOP, fill=tk.X)
        # How far back a NEW extended-hours (-pre/-post) series backfills on an
        # update. Default 'From data start' caps it at the regular series' own
        # first stored bar (engine series_first_dt) — i.e. an update never
        # reaches before your data starts. A shorter pick limits it further.
        ttk.Label(opts, text="Lookback:").pack(side=tk.LEFT)
        self._ibkr_depth = tk.StringVar(value="From data start")
        _depth_values = ("From data start", "6 months", "1 year",
                         "2 years", "3 years", "5 years")
        self._ibkr_depth_cb = ttk.Combobox(
            opts, textvariable=self._ibkr_depth, values=_depth_values,
            # derived, so adding a longer option can never silently clip it
            width=max(len(v) for v in _depth_values), state="readonly")
        self._ibkr_depth_cb.pack(side=tk.LEFT, padx=(4, 16))
        ttk.Checkbutton(
            opts, text="store daily (1d)",
            variable=self._storage_tws_daily).pack(side=tk.LEFT)
        # also collect each series' pre-market + after-hours bars (-pre/-post
        # files; regular hours unchanged). Shared with the 'Add stock' dialog.
        self._ibkr_extended_cb = ttk.Checkbutton(
            opts, text="+ extended hours",
            variable=self._storage_tws_extended)
        self._ibkr_extended_cb.pack(side=tk.LEFT, padx=(12, 0))

        mid = ttk.Frame(win, padding=(8, 0))
        mid.pack(fill=tk.BOTH, expand=True)
        cols = ("ticker", "name", "interval", "stored_end", "gap_days",
                "est")
        self._ibkr_tree = ttk.Treeview(mid, columns=cols,
                                       show="headings", height=10,
                                       selectmode="extended")
        for c, head, w, anch in (
                ("ticker", "Symbol", 75, "w"),
                ("name", "Company name", 180, "w"),
                ("interval", "Interval", 65, "center"),
                ("stored_end", "Stored end", 175, "w"),
                ("gap_days", "Sessions", 90, "e"),
                ("est", "Est. requests (~6/min)", 180, "e")):
            self._ibkr_tree.heading(c, text=head)
            self._ibkr_tree.column(c, width=w, anchor=anch)
        _vs = ttk.Scrollbar(mid, orient="vertical",
                            command=self._ibkr_tree.yview)
        self._ibkr_tree.configure(yscrollcommand=_vs.set)
        self._ibkr_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        _vs.pack(side=tk.LEFT, fill=tk.Y)
        self._ibkr_tree.bind("<<TreeviewSelect>>",
                             self._storage_ibkr_selection_changed)

        out = ttk.LabelFrame(win, text="Connection doctor / progress",
                             padding=(6, 2))
        out.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=(4, 8))
        _pf = ttk.Frame(out)
        _pf.pack(fill=tk.X, pady=(0, 2))
        self._ibkr_progress = ttk.Progressbar(_pf, mode="determinate")
        self._ibkr_progress.pack(fill=tk.X)
        # Keep the full progress detail visible.  Beside the expanding bar a
        # fixed 42-character label clipped both the X/Y-day counter and the
        # genuine pacing suffix before either could be verified on screen.
        self._ibkr_progress_lbl = ttk.Label(_pf, text="", anchor="w")
        self._ibkr_progress_lbl.pack(fill=tk.X, pady=(2, 0))
        self._ibkr_port_frame = ttk.Frame(out)
        self._ibkr_port_frame.pack(fill=tk.X, pady=(0, 2))
        self._ibkr_port_rows = {}
        self._ibkr_port_states = {}
        self._ibkr_out = tk.Text(out, height=9, wrap="word",
                                 state=tk.DISABLED, font=("Consolas", 9))
        self._ibkr_out.pack(fill=tk.X, expand=True)
        self._storage_ibkr_refresh()

    def _storage_ibkr_close(self) -> None:
        if getattr(self, "_storage_ibkr_running", False):
            self._storage_ibkr_say(
                "An update is running — Cancel it before closing.")
            return
        try:
            self._storage_ibkr_win.destroy()
        except Exception:  # noqa: BLE001
            pass
        self._storage_ibkr_win = None

    def _fmt_dur(self, s) -> str:
        s = int(max(0, s))
        h, s = divmod(s, 3600)
        mm, s = divmod(s, 60)
        return (f"{h}h{mm:02d}m" if h
                else (f"{mm}m{s:02d}s" if mm else f"{s}s"))

    def _export_progress_surface(self, parent, prefix, summary_var,
                                 activity_var):
        """Create one shared determinate export-progress surface.

        The left label is stable aggregate progress/ETA.  The right label is
        deliberately secondary, latest-item activity.  Both Export dialogs use
        this helper so their bar semantics cannot drift apart.
        """
        surface = ttk.Frame(parent, padding=(10, 0))
        surface.pack(side=tk.TOP, fill=tk.X)
        bar = ttk.Progressbar(
            surface, mode="determinate", maximum=1, value=0)
        bar.pack(side=tk.TOP, fill=tk.X, pady=(2, 1))
        line = ttk.Frame(surface)
        line.pack(side=tk.TOP, fill=tk.X)
        summary = ttk.Label(
            line, textvariable=summary_var, foreground="#555", anchor="w")
        summary.pack(side=tk.LEFT, fill=tk.X, expand=True)
        activity = ttk.Label(
            line, textvariable=activity_var, foreground="#777", anchor="e")
        activity.pack(side=tk.RIGHT, padx=(12, 0))
        setattr(self, f"_{prefix}_progress_surface", surface)
        setattr(self, f"_{prefix}_progress", bar)
        setattr(self, f"_{prefix}_progress_summary", summary)
        setattr(self, f"_{prefix}_progress_activity", activity)
        return bar

    def _export_progress_reset(self, prefix, summary="", *, now=None):
        """Reset a named export bar and its private throughput state."""
        started = time.monotonic() if now is None else float(now)
        state = {
            "start": started,
            "samples": [(0, 0.0)],
            "units_done": 0,
            "units_total": 0,
            "files_done": 0,
            "files_total": 0,
            "frozen": False,
            "finished": False,
            "last_summary": str(summary or ""),
        }
        setattr(self, f"_{prefix}_progress_state", state)
        try:
            getattr(self, f"_{prefix}_progress").configure(
                maximum=1, value=0)
        except Exception:  # noqa: BLE001 - dialog may have closed
            pass
        try:
            getattr(self, f"_{prefix}_status").set(str(summary or ""))
        except Exception:  # noqa: BLE001
            pass
        try:
            getattr(self, f"_{prefix}_activity").set("")
        except Exception:  # noqa: BLE001
            pass
        return state

    def _export_progress_apply(self, prefix, event, *, now=None):
        """Apply one structured event on the Tk thread.

        Only aggregate events can move the determinate bar or stable summary;
        every raw event is confined to the secondary activity label.
        """
        if not isinstance(event, dict) or event.get("kind") != "aggregate":
            try:
                getattr(self, f"_{prefix}_activity").set(
                    export_batch.progress_text(event))
            except Exception:  # noqa: BLE001 - dialog may have closed
                pass
            return "activity"

        state = getattr(self, f"_{prefix}_progress_state", None)
        if not isinstance(state, dict):
            state = self._export_progress_reset(prefix, now=now)
        if state.get("finished"):
            return "finished"

        try:
            incoming_total = max(0, int(event.get("units_total", 0) or 0))
            incoming_done = max(0, int(event.get("units_done", 0) or 0))
            incoming_files_total = max(
                0, int(event.get("files_total", 0) or 0))
            incoming_files_done = max(
                0, int(event.get("files_done", 0) or 0))
        except (TypeError, ValueError):
            return "invalid"

        # A successful cancel finishes below units_total.  If a COMPLETE
        # terminal aggregate was already queued when the user clicked Cancel,
        # the click was simply too late: reconcile that aggregate to 100%
        # rather than leaving a partial bar beside a successful result.
        complete_terminal = (
            incoming_total > 0 and incoming_done >= incoming_total
            and incoming_files_total > 0
            and incoming_files_done >= incoming_files_total)
        if state.get("frozen"):
            if not complete_terminal:
                return "frozen"
            state["frozen"] = False

        prior_total = int(state.get("units_total", 0) or 0)
        prior_done = int(state.get("units_done", 0) or 0)
        units_total = max(prior_total, incoming_total)
        units_done = max(prior_done, incoming_done)
        if units_total > 0:
            units_done = min(units_done, units_total)
        files_total = max(
            int(state.get("files_total", 0) or 0), incoming_files_total)
        files_done = max(
            int(state.get("files_done", 0) or 0), incoming_files_done)
        if files_total > 0:
            files_done = min(files_done, files_total)

        current = time.monotonic() if now is None else float(now)
        elapsed = max(0.0, current - float(state["start"]))
        samples = state["samples"]
        if units_done > prior_done:
            samples.append((units_done, elapsed))
            cutoff = elapsed - self._BATCH_RATE_WINDOW_S
            while (len(samples) > self._BATCH_MIN_SAMPLES
                   and samples[0][1] < cutoff):
                samples.pop(0)

        state.update({
            "units_done": units_done,
            "units_total": units_total,
            "files_done": files_done,
            "files_total": files_total,
        })
        maximum = max(1, units_total)
        if units_total != prior_total or units_done != prior_done:
            try:
                getattr(self, f"_{prefix}_progress").configure(
                    maximum=maximum, value=min(units_done, maximum))
            except Exception:  # noqa: BLE001 - dialog may have closed
                pass

        percent = (100 * units_done // units_total
                   if units_total > 0 else 0)
        summary = (f"{percent}% — months {units_done}/{units_total} — "
                   f"files {files_done}/{files_total}")
        remaining = None
        if len(samples) >= 2:
            old_done, old_at = samples[0]
            new_done, new_at = samples[-1]
            if new_done > old_done and new_at > old_at:
                seconds_per_unit = (new_at - old_at) / (new_done - old_done)
                remaining = max(0, units_total - units_done) * seconds_per_unit
        if units_total > 0 and units_done >= units_total:
            summary += f"  ·  {self._fmt_dur(elapsed)} elapsed"
        elif remaining is not None:
            summary += f"  ·  ~{self._fmt_dur(remaining)} left"
        else:
            summary += "  ·  starting…"
        state["last_summary"] = summary
        try:
            getattr(self, f"_{prefix}_status").set(summary)
        except Exception:  # noqa: BLE001 - dialog may have closed
            pass
        return "aggregate"

    def _export_progress_freeze(self, prefix):
        """Freeze a bar at the last displayed aggregate (cancel semantics)."""
        state = getattr(self, f"_{prefix}_progress_state", None)
        if isinstance(state, dict):
            state["frozen"] = True

    def _export_progress_finish(self, prefix):
        """Stop later events from rewriting a terminal UI summary."""
        state = getattr(self, f"_{prefix}_progress_state", None)
        if isinstance(state, dict):
            state["finished"] = True
        self._export_progress_pulse_cancel(prefix)
        try:
            getattr(self, f"_{prefix}_activity").set("")
        except Exception:  # noqa: BLE001
            pass

    # ---- liveness pulse (text only; the determinate bar stays honest) -----
    # Between worker events the summary is static, so a long month render, a
    # Parquet flush, or the end-of-run quality note made the dialog LOOK
    # frozen — and once months hit N/N it LOOKED finished while files were
    # still being finalized. The pulse rewrites only the summary TEXT (ticking
    # elapsed clock, animated dots, an explicit "finalizing" phase) from the
    # same state `_export_progress_apply` maintains. It never moves the bar,
    # starts at each run's reset, and stops itself on the finished flag or a
    # destroyed dialog — so the terminal summary set after finish() persists.
    _EXPORT_PULSE_MS = 400

    def _export_progress_pulse_start(self, prefix):
        self._export_progress_pulse_cancel(prefix)
        try:
            handle = self.root.after(
                self._EXPORT_PULSE_MS,
                lambda: self._export_progress_pulse(prefix))
        except Exception:  # noqa: BLE001 — app shutting down
            handle = None
        setattr(self, f"_{prefix}_progress_pulse_after", handle)

    def _export_progress_pulse_cancel(self, prefix):
        handle = getattr(self, f"_{prefix}_progress_pulse_after", None)
        setattr(self, f"_{prefix}_progress_pulse_after", None)
        if handle is not None:
            try:
                self.root.after_cancel(handle)
            except Exception:  # noqa: BLE001
                pass

    def _export_progress_pulse(self, prefix):
        setattr(self, f"_{prefix}_progress_pulse_after", None)
        state = getattr(self, f"_{prefix}_progress_state", None)
        if not isinstance(state, dict) or state.get("finished"):
            return
        bar = getattr(self, f"_{prefix}_progress", None)
        try:
            if bar is None or not bar.winfo_exists():
                return
        except Exception:  # noqa: BLE001
            return
        now = time.monotonic()
        elapsed = max(0.0, now - float(state.get("start", now)))
        dots = "." * (1 + int(elapsed * 2) % 3)
        base = str(state.get("last_summary") or "")
        units_total = int(state.get("units_total", 0) or 0)
        units_done = int(state.get("units_done", 0) or 0)
        if state.get("frozen"):
            text = f"Cancelling at the next safe boundary{dots}"
        elif units_total > 0 and units_done >= units_total:
            # Every month is in but the run has NOT reported done: files are
            # still flushing / the quality note is still being written.
            text = (f"{base}  ·  finalizing{dots}" if base
                    else f"Finalizing{dots}")
        else:
            live = f"{self._fmt_dur(elapsed)} elapsed{dots}"
            text = f"{base}  ·  {live}" if base else live
        try:
            getattr(self, f"_{prefix}_status").set(text)
        except Exception:  # noqa: BLE001 — dialog may have closed
            pass
        self._export_progress_pulse_start(prefix)

    # Live ETA measures REALIZED throughput over this trailing window, so the
    # "left" readout tracks the real recent rate — and GROWS on a slowdown or a
    # stall — instead of counting a frozen start-of-run estimate down to zero.
    # MIN_SAMPLES are kept regardless of age so one ultra-slow series (e.g. a
    # ticker the demo has no deep data for) can't collapse the window to a single
    # 2-point straddle and whipsaw the projected rate.
    _BATCH_RATE_WINDOW_S = 900.0
    _BATCH_MIN_SAMPLES = 5
    # The live ETA STARTS at the pre-run estimate and PURELY counts it DOWN for the
    # first _BATCH_WARMUP_START completions, then blends toward the measured rate,
    # reaching full trust at _BATCH_WARMUP_FULL — so it never jumps UP at the start
    # yet still tracks a real slowdown once it has enough signal.
    _BATCH_WARMUP_START = 5
    _BATCH_WARMUP_FULL = 20

    def _batch_elapsed(self, live, now):
        """Wall seconds since the run started MINUS time spent paused, so the
        elapsed clock — and the ETA derived from it — FREEZES while every port is
        paused and resumes on Resume. All ETA timestamps (samples, last_t) are kept
        on this same pause-adjusted scale so the measured rate ignores paused time."""
        e = now - live["start"] - live.get("paused_total", 0.0)
        pa = live.get("paused_at")
        if pa is not None:                      # currently paused -> hold here
            e -= (now - pa)
        return max(0.0, e)

    def _batch_pause(self) -> None:
        """Freeze the elapsed/ETA clock — call once every active port has paused."""
        import time as _t
        live = getattr(self, "_batch_live", None)
        if live and live.get("paused_at") is None:
            live["paused_at"] = _t.time()

    def _batch_resume(self) -> None:
        """Resume the elapsed/ETA clock on Resume (bank the paused interval)."""
        import time as _t
        live = getattr(self, "_batch_live", None)
        if live and live.get("paused_at") is not None:
            live["paused_total"] = (live.get("paused_total", 0.0)
                                    + (_t.time() - live["paused_at"]))
            live["paused_at"] = None

    def _batch_remaining(self, live, elapsed):
        """PROJECT the seconds-left snapshot — recomputed only at each '[i/N]' marker
        (not every tick). = the unfinished series x the throughput measured over the
        last _BATCH_RATE_WINDOW_S, blended with the pre-run estimate, weighted ZERO
        for the first _BATCH_WARMUP_START completions (so the readout simply counts
        DOWN from the estimate at the start, never jumping up) then ramping to full
        by _BATCH_WARMUP_FULL (so a real slowdown is reflected). Between markers the
        readout just counts THIS value down — see _batch_shown; it never ticks UP.
        None only when there is neither an estimate nor a measured rate yet."""
        n = int(live.get("n") or 1)
        i = int(live.get("i") or 0)
        remaining_series = max(0, n - i)
        if remaining_series <= 0:
            return 0.0
        est = getattr(self, "_batch_est_seconds", None)
        est_rem = (max(0.0, est - elapsed) if (est and est > 0) else None)
        sps = None                              # RECENT seconds-per-series
        samples = live.get("samples") or []     # samples store EFFECTIVE elapsed
        if len(samples) >= 2:
            oi, ot = samples[0]
            ni, nt = samples[-1]
            if ni > oi and nt > ot:
                sps = (nt - ot) / (ni - oi)
        if sps is None:                         # no measured rate yet -> estimate
            return est_rem
        meas_rem = remaining_series * sps       # projected time for the rest
        if est_rem is None:                     # no estimate -> pure measured
            return meas_rem
        span = float(self._BATCH_WARMUP_FULL - self._BATCH_WARMUP_START)
        w = max(0.0, min(1.0, (i - self._BATCH_WARMUP_START) / span))
        return w * meas_rem + (1.0 - w) * est_rem

    def _batch_shown(self, live, elapsed):
        """The DISPLAYED seconds-left: the last marker's projection (eta0) COUNTED
        DOWN by the time since it was set, so between the sparse markers the readout
        ticks DOWN 1s/s like a normal clock — it never ticks UP. A new marker
        re-projects eta0 (which may step up or down — that one adjustment is fine).
        None before any projection exists."""
        eta0 = live.get("eta0")
        if eta0 is None:
            return None
        return max(0.0, eta0 - (elapsed - live.get("eta_at", elapsed)))

    def _batch_label(self, i, n, elapsed, remaining, detail=None) -> str:
        base = f"{i}/{n}  ·  {self._fmt_dur(elapsed)} elapsed"
        if n > 0 and i >= n:
            # the LAST series is in flight — markers fire at series START, so [N/N]
            # means it just BEGAN; never assert "~0s left" while it still fetches.
            return base + "  ·  finishing…"
        if remaining is None:
            return base
        if remaining < 1:
            # bottomed out but not done: "starting…" before the first completion
            # (slow connect/spin-up at 0/N), "finishing…" between series mid-run.
            return base + ("  ·  starting…" if i == 0 else "  ·  finishing…")
        return base + f"  ·  ~{self._fmt_dur(remaining)} left"

    def _batch_with_detail(self, text, detail) -> str:
        return text + (f"  |  {detail}" if detail else "")

    @staticmethod
    def _batch_visible_detail(detail, pacing_detail) -> str:
        """Keep ordinary progress visible while a pacing wait is active."""
        parts = [str(value).strip() for value in (detail, pacing_detail)
                 if str(value or "").strip()]
        return "  |  ".join(parts)

    def _batch_seed(self, start_attr, lbl, n, est_secs) -> None:
        """Show the pre-run WALL-CLOCK estimate as the live ETA the instant the
        run starts — BEFORE the first '[i/N]' marker (which only lands after
        connect/qualify, and in the combined extended path after the first
        ticker's prefetch). As series finish, _batch_bar_update switches the
        readout to the MEASURED throughput. No-op without a positive estimate."""
        import time as _t
        self._batch_est_seconds = est_secs
        # Work-weighted projector: seeded with the per-series modeled seconds
        # so the live readout retires WORK, not a count of unequal series (the
        # count-based rate swung 9 min <-> 16 h inside one run, 2026-07-17).
        pred = getattr(self, "_batch_pred_map", None)
        self._batch_proj = (fetch_eta.EtaProjector(pred, est_secs)
                            if pred else None)
        if not est_secs or est_secs <= 0 or lbl is None:
            return
        now = _t.time()
        setattr(self, start_attr, now)     # elapsed counts from the true start
        self._batch_live = {"lbl": lbl, "start": now, "i": 0,
                            "n": max(1, int(n or 1)), "samples": [],
                            "eta0": est_secs, "eta_at": 0.0,
                            "paused_total": 0.0, "paused_at": None,
                            "detail": "", "pacing_detail": "",
                            "seeded": True}
        try:
            lbl.config(text=self._batch_with_detail(
                self._batch_label(0, max(1, int(n or 1)), 0.0, est_secs),
                ""))
        except Exception:  # noqa: BLE001 — dialog closed
            pass
        if not getattr(self, "_batch_tick_on", False):
            self._batch_tick_on = True
            try:
                self.root.after(1000, self._batch_tick)
            except Exception:  # noqa: BLE001
                self._batch_tick_on = False

    def _batch_tick(self) -> None:
        """Refresh the elapsed/left readout EVERY SECOND so the clock moves between
        the sparse '[i/N]' markers — COUNTING the last projection DOWN (never up),
        with both elapsed and the ETA frozen while paused. Stops itself when no run
        is active."""
        import time as _t
        live = getattr(self, "_batch_live", None)
        if not live:
            self._batch_tick_on = False
            return
        eff = self._batch_elapsed(live, _t.time())
        rem = self._batch_shown(live, eff)
        try:
            live["lbl"].config(text=self._batch_with_detail(
                self._batch_label(live["i"], live["n"], eff, rem),
                self._batch_visible_detail(
                    live.get("detail"), live.get("pacing_detail"))))
        except Exception:  # noqa: BLE001 — dialog closed
            self._batch_live = None
        try:
            self.root.after(1000, self._batch_tick)
        except Exception:  # noqa: BLE001
            self._batch_tick_on = False

    def _batch_bar_update(self, raw, bar, lbl, start_attr) -> None:
        """Drive a determinate progress bar from gap_fill's '[i/N] …' batch
        markers so a mass run shows whole-batch progress + a live ETA. The
        elapsed/left readout then ticks every second via _batch_tick."""
        import re
        import time as _t
        m = re.match(r"^\[(\d+)/(\d+)\]", str(raw or ""))
        if not (m and bar):
            return
        i, n = int(m.group(1)), int(m.group(2))
        start = getattr(self, start_attr, None)
        if start is None:
            start = _t.time()
            setattr(self, start_attr, start)
        now = _t.time()
        live = getattr(self, "_batch_live", None)
        if not live or live.get("lbl") is not lbl:
            # no seed (or a different label) -> start fresh throughput state here
            live = {"lbl": lbl, "start": start, "i": 0, "n": n, "samples": [],
                    "eta0": None, "eta_at": 0.0,
                    "paused_total": 0.0, "paused_at": None,
                    "detail": "", "pacing_detail": ""}
            self._batch_live = live
        # Work-weighted projector (seeded runs only): every marker — INCLUDING
        # out-of-order ones the display drops below — is a series START on its
        # port lane, retiring that lane's previous series. Identity comes from
        # the marker's own "TICKER interval" tail; parallel lines end
        # "(port N)" or, after a hard reroute, "(port N, resumed)".
        # Unparsable identity still advances lane accounting.
        proj = getattr(self, "_batch_proj", None) if live.get("seeded") else None
        if proj is not None:
            tail = str(raw or "")[m.end():].strip()
            pm = re.match(r"(\S+)\s+(\S+)", tail)
            key = None
            if pm and not pm.group(2).startswith("("):
                key = f"{pm.group(1)} {pm.group(2)}"
            pp = re.search(r"\(port (\d+)(?:,\s*resumed)?\)\s*$", tail)
            try:
                proj.on_marker(key, self._batch_elapsed(live, now),
                               port=int(pp.group(1)) if pp else None)
            except Exception:  # noqa: BLE001 — ETA must never break the run
                proj = None
        if i <= live["i"]:
            # stale / out-of-order / duplicate global-counter marker (the parallel
            # path can deliver [n] before [n-1]) -> ignore it so the bar + ETA stay
            # MONOTONIC and a backward i can't whipsaw the projected rate.
            live["n"] = n
            return
        eff = self._batch_elapsed(live, now)   # i increased -> a real completion
        live["i"] = i
        live["n"] = n
        samples = live["samples"]
        samples.append((i, eff))           # throughput sample (series_done, eff-elapsed)
        cutoff = eff - self._BATCH_RATE_WINDOW_S
        while len(samples) > self._BATCH_MIN_SAMPLES and samples[0][1] < cutoff:
            samples.pop(0)                 # keep the recent window (>= MIN samples)
        if proj is not None:
            try:                           # RE-PROJECT from retired WORK
                live["eta0"] = proj.projection(eff)
            except Exception:  # noqa: BLE001 — fall back to the legacy rate
                live["eta0"] = self._batch_remaining(live, eff)
        else:
            live["eta0"] = self._batch_remaining(live, eff)
        live["eta_at"] = eff
        eta = live["eta0"]
        if not getattr(self, "_batch_tick_on", False):
            self._batch_tick_on = True
            try:
                self.root.after(1000, self._batch_tick)
            except Exception:  # noqa: BLE001
                self._batch_tick_on = False
        try:
            bar.config(maximum=n, value=i)
            lbl.config(text=self._batch_with_detail(
                self._batch_label(i, n, eff, eta),
                self._batch_visible_detail(
                    live.get("detail"), live.get("pacing_detail"))))
        except Exception:  # noqa: BLE001 — dialog closed
            pass

    def _fetch_pacing_detail(self, raw) -> str | None:
        try:
            pacing = stock_ibkr._parse_pacing_status_msg(raw)
        except Exception:  # noqa: BLE001
            pacing = None
        if pacing is not None:
            # An empty string is a meaningful paired CLEAR. Callers distinguish
            # it from None (an ordinary text-log line).
            return str(pacing.get("label") or "")
        return None

    def _fetch_progress_detail(self, raw) -> str | None:
        try:
            info = stock_ibkr._parse_progress_msg(raw)
        except Exception:  # noqa: BLE001
            info = None
        if not info:
            return None
        phase = "backfill " if info.get("phase") == "backfill" else ""
        detail = (f"{info['ticker']} {info['interval']} - "
                  f"{phase}{info['done']}/{info['total']} days")
        bars = int(info.get("bars") or 0)
        if bars:
            detail += f" | {bars:,} bars"
        return detail

    def _batch_set_detail(self, lbl, detail) -> None:
        if lbl is None:
            return
        import time as _t
        live = getattr(self, "_batch_live", None)
        try:
            if live and live.get("lbl") is lbl:
                live["detail"] = detail
                eff = self._batch_elapsed(live, _t.time())
                rem = self._batch_shown(live, eff)
                lbl.config(text=self._batch_with_detail(
                    self._batch_label(live.get("i", 0), live.get("n", 1),
                                      eff, rem),
                    self._batch_visible_detail(
                        detail, live.get("pacing_detail"))))
            else:
                lbl.config(text=detail)
        except Exception:  # noqa: BLE001
            pass

    def _batch_set_pacing_detail(self, lbl, detail) -> None:
        """Overlay engine-confirmed pacing without losing ordinary progress."""
        if lbl is None:
            return
        import time as _t
        live = getattr(self, "_batch_live", None)
        try:
            if live and live.get("lbl") is lbl:
                live["pacing_detail"] = detail
                eff = self._batch_elapsed(live, _t.time())
                rem = self._batch_shown(live, eff)
                lbl.config(text=self._batch_with_detail(
                    self._batch_label(live.get("i", 0), live.get("n", 1),
                                      eff, rem),
                    self._batch_visible_detail(
                        live.get("detail"), detail)))
            else:
                lbl.config(text=detail)
        except Exception:  # noqa: BLE001
            pass

    def _trim_text_log(self, t, maxlines=1000, slack=200) -> None:
        """Cap a scrolling log Text so a multi-day mass run can't bloat it.
        Trims in BATCHES (only once it exceeds maxlines+slack, back down to
        maxlines) so it isn't churning a delete on every single message."""
        try:
            n = int(t.index("end-1c").split(".")[0])
            if n > maxlines + slack:
                t.delete("1.0", f"{n - maxlines + 1}.0")
        except Exception:  # noqa: BLE001 — dialog closed / odd index
            pass

    def _storage_ibkr_say(self, text, append=True) -> None:
        self._batch_bar_update(text, getattr(self, "_ibkr_progress", None),
                               getattr(self, "_ibkr_progress_lbl", None),
                               "_ibkr_run_start")
        try:
            t = self._ibkr_out
            t.config(state=tk.NORMAL)
            if not append:
                t.delete("1.0", tk.END)
            t.insert(tk.END, text + "\n")
            self._trim_text_log(t)
            t.see(tk.END)
            t.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001 — dialog closed
            pass

    def _drain_progress_batched(self, q, cap=300):
        """Drain up to `cap` queued messages in ONE tick, BATCHING the 'progress'
        lines so the log updates once — not once-per-message. A fast multi-port
        fetch (per-span heartbeats × N ports) can emit thousands of messages a
        second; the old per-message insert + autoscroll starved the tk event loop
        and the window went 'Not Responding'. Returns (lines, last_marker,
        terminal, backlog, detail): `lines` flush together; `last_marker` is the NEWEST
        '[i/N]' (only that one drives a bar update); `terminal` is the first
        non-progress (kind, payload) which stops the drain for the caller to
        route; `backlog` means the cap was hit (re-poll sooner); `detail` carries
        the newest ordinary and pacing-label updates independently. UI-only: the
        worker keeps fetching at full speed (it only ever q.put()s, never waits)."""
        import queue
        import re
        lines, last_marker, terminal, backlog = [], None, None, False
        last_detail = None
        last_pacing_detail = None
        saw_detail = False
        for _ in range(cap):
            try:
                kind, payload = q.get_nowait()
            except queue.Empty:
                break
            if kind == "progress":
                s = str(payload)
                pacing_detail = self._fetch_pacing_detail(s)
                if pacing_detail is not None:
                    last_pacing_detail = pacing_detail
                    saw_detail = True
                    continue
                detail = self._fetch_progress_detail(s)
                if detail is not None:
                    last_detail = detail
                    saw_detail = True
                    continue
                # The embedded Add Stocks WS8 verification emits one
                # "WS8 check: TICKER..." / "WS8 probe: TICKER..." line per
                # ticker. Mirror the TICKER onto the persistent progress label
                # (in ADDITION to the scrolling log — fall through, don't
                # continue) so the indicator names what it is verifying instead
                # of freezing on the last completed fetch detail.
                ws8 = re.match(r"^WS8 (check|probe): (.+?)\.\.\.\s*$", s)
                if ws8 is not None:
                    stage = ("cross-check" if ws8.group(1) == "check"
                             else "live probe")
                    last_detail = f"Verifying {ws8.group(2)} — WS8 {stage}"
                    saw_detail = True
                lines.append(s)
                if re.match(r"^\[\d+/\d+\]", s):
                    last_marker = s
            else:
                terminal = (kind, payload)
                break
        else:
            backlog = True
        details = ({"detail": last_detail, "pacing": last_pacing_detail}
                   if saw_detail else None)
        return lines, last_marker, terminal, backlog, details

    def _drain_ibkr_progress(self, q, cap=300):
        """IBKR update drain with non-terminal structured engine events.

        The generic drain treats every non-progress message as terminal. The
        update flow also receives interim report and recovery-phase events while
        the worker continues running, so keep those separate for the poller.
        """
        import queue
        import re
        lines, last_marker, terminal, backlog, events = [], None, None, False, []
        last_detail = None
        last_pacing_detail = None
        saw_detail = False
        for _ in range(cap):
            try:
                kind, payload = q.get_nowait()
            except queue.Empty:
                break
            if kind == "progress":
                s = str(payload)
                pacing_detail = self._fetch_pacing_detail(s)
                if pacing_detail is not None:
                    last_pacing_detail = pacing_detail
                    saw_detail = True
                    continue
                detail = self._fetch_progress_detail(s)
                if detail is not None:
                    last_detail = detail
                    saw_detail = True
                    continue
                lines.append(s)
                if re.match(r"^\[\d+/\d+\]", s):
                    last_marker = s
            elif kind in ("report", "recovering"):
                events.append((kind, payload))
            else:
                terminal = (kind, payload)
                break
        else:
            backlog = True
        details = ({"detail": last_detail, "pacing": last_pacing_detail}
                   if saw_detail else None)
        return lines, last_marker, terminal, backlog, events, details

    def _batch_say(self, out, prog_attr, lbl_attr, start_attr, lines,
                   last_marker, detail=None):
        """Flush a BATCH of progress lines to a log Text in ONE insert + one
        autoscroll, and drive the batch bar from only the NEWEST marker. The
        per-message path (_storage_*_say) stays for one-off messages."""
        if last_marker is not None:
            self._batch_bar_update(last_marker, getattr(self, prog_attr, None),
                                   getattr(self, lbl_attr, None), start_attr)
        if isinstance(detail, dict) and set(detail) == {"detail", "pacing"}:
            target = getattr(self, lbl_attr, None)
            if detail["detail"] is not None:
                self._batch_set_detail(target, detail["detail"])
            if detail["pacing"] is not None:
                self._batch_set_pacing_detail(target, detail["pacing"])
        elif detail is not None:
            self._batch_set_detail(getattr(self, lbl_attr, None), detail)
        if not lines or out is None:
            return
        try:
            out.config(state=tk.NORMAL)
            out.insert(tk.END, "\n".join(lines) + "\n")
            self._trim_text_log(out)
            out.see(tk.END)
            out.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001 — dialog closed
            pass

    def _storage_cached_known_series(self):
        """[(ticker, {intervals})] from the already-rendered storage table.
        Returns None when the table has not been scanned this session."""
        rows = getattr(self, "_storage_all_rows", None)
        if rows is None:
            return None
        by_ticker = {}
        for r in rows:
            try:
                ticker = str(r[0]).strip().upper()
                interval = str(r[2]).strip()
            except (IndexError, TypeError):
                continue
            if not ticker or not stock_storage.INTERVAL_RE.match(interval):
                continue
            by_ticker.setdefault(ticker, set()).add(interval)
        return sorted(by_ticker.items())

    def _storage_known_series(self):
        """[(ticker, {intervals})] from the manifests — offline."""
        cached = self._storage_cached_known_series()
        if cached is not None:
            return cached
        out = []
        try:
            for d in sorted(self._storage_root.iterdir()):
                if not (d.is_dir()
                        and stock_storage.TICKER_DIR_RE.match(d.name)):
                    continue
                m = stock_storage.load_manifest(d)
                if m:
                    out.append((d.name, set(m.get("intervals", {}))))
        except OSError:
            pass
        return out

    def _storage_verify_dialog(self) -> None:
        """'Verify accuracy' — cross-check a stored series' daily OHLC against a
        free online daily reference (stockanalysis.com), flagging only
        SIGNIFICANT discrepancies (splits, scale errors, wrong symbol, missing
        chunks), not penny/auction/dividend-basis noise. The check runs on a
        worker thread (it reaches the internet + aggregates) and drains through
        a queue via root.after — same discipline as the other storage dialogs."""
        import queue
        import threading
        if self._storage_busy():
            return
        known = self._storage_known_series()
        if not known:
            messagebox.showinfo("Verify accuracy", "No stored series yet.",
                                parent=self.root)
            return
        win = tk.Toplevel(self.root)
        win.title("Verify accuracy — cross-check vs the internet")
        win.transient(self.root)
        top = ttk.Frame(win)
        top.pack(anchor=tk.W, padx=10, pady=(10, 4))
        ttk.Label(top, text="Ticker:").pack(side=tk.LEFT)
        tvar = tk.StringVar(value=known[0][0])
        tcb = ttk.Combobox(top, textvariable=tvar, width=10,
                           values=[t for t, _ in known])
        tcb.pack(side=tk.LEFT, padx=(4, 12))
        ttk.Label(top, text="Interval:").pack(side=tk.LEFT)
        ivar = tk.StringVar(value="1m")
        icb = ttk.Combobox(top, textvariable=ivar, width=8)
        icb.pack(side=tk.LEFT, padx=(4, 12))
        ttk.Label(top, text="Sensitivity:").pack(side=tk.LEFT)
        svar = tk.StringVar(value="normal")
        ttk.Combobox(top, textvariable=svar, width=8, state="readonly",
                     values=["strict", "normal", "loose"]).pack(side=tk.LEFT)

        def set_ivs(_e=None):
            t = tvar.get().strip().upper()
            for tk_, ivs in known:
                if tk_ == t:
                    rth = sorted(i for i in ivs if "-" not in i) or ["1m"]
                    icb.config(values=rth)
                    if ivar.get() not in rth:
                        ivar.set(rth[0])
                    return
        tcb.bind("<<ComboboxSelected>>", set_ivs)
        set_ivs()

        body = ttk.Frame(win)
        body.pack(fill=tk.BOTH, expand=True, padx=10, pady=(6, 4))
        out = tk.Text(body, height=16, width=82, wrap="word", state=tk.DISABLED)
        vs = ttk.Scrollbar(body, orient="vertical", command=out.yview)
        out.configure(yscrollcommand=vs.set)
        out.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vs.pack(side=tk.LEFT, fill=tk.Y)
        ttk.Label(
            win, foreground="#888", wraplength=680,
            text=("Health report is offline. Verify uses stockanalysis.com; "
                  "split refresh explicitly uses Yahoo/SEC plus the daily "
                  "reference.")).pack(anchor=tk.W, padx=10)

        def say(line):
            try:
                out.config(state=tk.NORMAL)
                out.insert(tk.END, str(line).rstrip("\n") + "\n")
                out.see(tk.END)
                out.config(state=tk.DISABLED)
            except Exception:  # noqa: BLE001
                pass

        q = queue.Queue()
        busy = {"v": False, "kind": None}
        refresh_cancel = threading.Event()

        def begin(kind):
            if busy["v"]:
                return False
            busy["v"] = True
            busy["kind"] = kind
            refresh_cancel.clear()
            try:
                cancel_btn.config(
                    state=(tk.NORMAL if kind == "split_refresh"
                           else tk.DISABLED))
            except Exception:  # noqa: BLE001 - controls may be closing
                pass
            return True

        def finish():
            busy["v"] = False
            busy["kind"] = None
            try:
                cancel_btn.config(state=tk.DISABLED)
            except Exception:  # noqa: BLE001
                pass

        def poll():
            try:
                kind, payload = q.get_nowait()
            except queue.Empty:
                try:
                    if win.winfo_exists():
                        self.root.after(150, poll)
                except Exception:  # noqa: BLE001
                    pass
                return
            if kind == "split_progress":
                say(payload)
                self.root.after(120, poll)
                return
            finish()
            if kind == "error":
                say(f"FAILED: {payload}")
            elif kind == "health":
                for line in health_report.summarize_report(payload):
                    say(line)
            elif kind == "split_refresh":
                refresh = payload.get("refresh") or {}
                status = str(refresh.get("status") or "error")
                ticker = str(refresh.get("ticker") or "?")
                if status == "refreshed":
                    say(
                        f"Split references refreshed for {ticker}: "
                        f"{refresh.get('event_count', 0)} event(s), "
                        f"{refresh.get('candidate_count', 0)} candidate(s).")
                elif status == "cancelled":
                    say(
                        f"Split refresh cancelled for {ticker} at "
                        f"{refresh.get('stage') or 'a safe boundary'}; "
                        "no new cache was committed.")
                else:
                    say(
                        f"Split refresh failed for {ticker}: "
                        f"{refresh.get('error') or 'unknown error'} "
                        f"(last-good preserved="
                        f"{bool(refresh.get('preserved_last_good'))}).")
                health = payload.get("health")
                if isinstance(health, dict):
                    for line in health_report.summarize_report(health):
                        say(line)
            elif payload.get("error"):
                say(payload["error"])
            else:
                self._render_verify(payload, say)

        def run():
            if not begin("verify"):
                return
            t = tvar.get().strip().upper()
            iv = ivar.get().strip() or "1m"
            sens = svar.get()
            say(f"\nValidating {t} {iv} ({sens}) against the internet…")

            def work():
                try:
                    res = stock_validate.validate_series(
                        self._storage_root, t, iv, sensitivity=sens,
                        save_reference=True)   # snapshot the online daily ref
                    try:                       # extended hours has NO online ref
                        ext = stock_validate.validate_extended(
                            self._storage_root, t, iv)
                        if not ext.get("error"):
                            res["extended"] = ext
                    except Exception:  # noqa: BLE001
                        pass
                    q.put(("done", res))
                except Exception as exc:  # noqa: BLE001
                    q.put(("error", repr(exc)))

            threading.Thread(target=work, daemon=True).start()
            self.root.after(120, poll)

        def run_health():
            if not begin("health"):
                return
            say("\nRunning offline bank health report...")

            def work():
                try:
                    res = export_quality.ensure_current_health(
                        self._storage_root, current_fn=lambda _root: False)
                    q.put(("health", res))
                except Exception as exc:  # noqa: BLE001
                    q.put(("error", repr(exc)))

            threading.Thread(target=work, daemon=True).start()
            self.root.after(120, poll)

        def run_split_refresh():
            if not begin("split_refresh"):
                return
            try:
                ticker = stock_storage.canonical_ticker(
                    tvar.get().strip().upper())
            except Exception as exc:  # noqa: BLE001
                finish()
                say(f"Invalid ticker: {exc}")
                return
            say(
                f"\nRefreshing cached split references for {ticker} "
                "(explicit network operation)...")

            def work():
                try:
                    from split_provider import YahooSecProvider
                    provider = YahooSecProvider(timeout=30)
                    result = split_cache.refresh_ticker(
                        self._storage_root, ticker, provider,
                        progress=lambda stage, symbol: q.put((
                            "split_progress",
                            f"Split refresh {symbol}: "
                            f"{str(stage).replace('_', ' ')}...")),
                        cancel=refresh_cancel.is_set)
                    health = None
                    if result.get("status") == "refreshed":
                        health = export_quality.ensure_current_health(
                            self._storage_root,
                            current_fn=lambda _root: False)
                    q.put(("split_refresh", {
                        "refresh": result,
                        "health": health,
                    }))
                except Exception as exc:  # noqa: BLE001
                    q.put(("error", repr(exc)))

            threading.Thread(
                target=work, daemon=True,
                name=f"split-refresh-{ticker}").start()
            self.root.after(120, poll)

        def cancel_split_refresh():
            if busy.get("kind") != "split_refresh":
                return
            refresh_cancel.set()
            say(
                "Split refresh cancellation requested; an in-flight HTTP "
                "request may finish before the no-commit checkpoint.")
            try:
                cancel_btn.config(state=tk.DISABLED)
            except Exception:  # noqa: BLE001
                pass

        btns = ttk.Frame(win)
        btns.pack(anchor=tk.E, padx=10, pady=(4, 10))
        ttk.Button(btns, text="Health report",
                   command=run_health).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(btns, text="Refresh split references",
                   command=run_split_refresh).pack(side=tk.LEFT, padx=(0, 8))
        cancel_btn = ttk.Button(
            btns, text="Cancel refresh", command=cancel_split_refresh,
            state=tk.DISABLED)
        cancel_btn.pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(btns, text="Verify against the internet",
                   command=run).pack(side=tk.LEFT)
        win.update_idletasks()
        w, h = win.winfo_width(), win.winfo_height()
        win.geometry(f"+{max(0, (win.winfo_screenwidth() - w) // 2)}"
                     f"+{max(0, (win.winfo_screenheight() - h) // 3)}")

    def _render_verify(self, res, say):
        """Format a stock_validate.validate_series result into the dialog."""
        say(f"\n{res['ticker']} {res.get('interval')}: checked "
            f"{res.get('checked')} day(s) vs the reference "
            f"({res.get('days_derived')} stored days, "
            f"{res.get('sensitivity')} tolerance).")
        if res.get("reference_saved"):
            say(f"📄 online daily reference saved: {res['reference_saved']}")
        ext = res.get("extended")
        if ext:
            say(f"\nExtended hours (-pre/-post): {ext['days_checked']} day(s) "
                f"checked — score {ext['score']} (boundary continuity + "
                f"structure; no online reference exists for extended hours).")
            if ext["flagged"]:
                for f in ext["flagged"][:10]:
                    say(f"   ⚠ {f['date']} {f['kind']}: {f['detail']}")
                if len(ext["flagged"]) > 10:
                    say(f"   … and {len(ext['flagged']) - 10} more")
            else:
                say("   ✓ extended sessions join the regular session cleanly.")
        line = (f"match score: {res.get('score')}   basis ratio: "
                f"{res.get('basis_ratio')}")
        if res.get("suspected_factor"):
            line += (f"   ⚠ suspected uniform split/basis factor "
                     f"{res['suspected_factor']}")
        say(line)
        shift = res.get("level_shift")
        if shift:
            say(f"⛔ LEVEL SHIFT at {shift['date']}: the data steps by "
                f"×{shift['factor']} there (≈{shift['pct']:.1%}) — a mid-series "
                f"split / basis change that one side is (un)adjusted for. "
                f"Re-fetch or reconcile that boundary; the daily reference is "
                f"raw, so a clean step usually means a stored adjustment.")
        flagged = res.get("flagged") or []
        if not flagged:
            say("✓ no significant discrepancies — the stored data matches the "
                "reference within tolerance.")
            return
        say(f"⚠ {len(res.get('flagged_days') or [])} day(s) flagged:")
        for f in flagged[:25]:
            if f.get("field") == "level_shift":
                continue                    # already reported prominently above
            extra = (f"norm_dev {f['norm_dev']:.2%}" if "norm_dev" in f
                     else f"ratio {f.get('ratio')}")
            say(f"   {f['date']}  {f['field']}: stored {f['derived']} vs "
                f"ref {f['reference']}  ({extra})")
        if len(flagged) > 25:
            say(f"   … and {len(flagged) - 25} more")
        say("(high/low = price discrepancy; volume = missing/extra bars; a "
            "consistent factor = split/adjustment basis difference.)")

    def _storage_name_map(self):
        """{ticker: company name} for the tables that show a name beside the
        symbol: the IBKR-captured name (the _ibkr_names.json sidecar that
        find-stock / Fix data fill), with the manifest's own name winning when
        set. Offline."""
        try:
            names = dict(stock_validate.load_ibkr_names(self._storage_root))
        except Exception:  # noqa: BLE001
            names = {}
        try:
            for d in sorted(self._storage_root.iterdir()):
                if not (d.is_dir()
                        and stock_storage.TICKER_DIR_RE.match(d.name)):
                    continue
                m = stock_storage.load_manifest(d)
                if m and m.get("name"):
                    names[d.name] = m["name"]      # manifest name wins when set
        except OSError:
            pass
        return names

    def _storage_ibkr_refresh(self) -> None:
        """Offline gap plan per ticker for the chosen interval — looking
        never needs the network. STREAMING: each ticker's manifest is parsed
        ONCE and feeds name + stored-intervals + plan together (this used to
        load all ~500 manifests THREE times on the GUI thread — name map,
        known-series, plan_gap — a measured ~4 s freeze per refresh; the
        single-parse loop is ~1.3-1.7 s with a few MB peak instead of the
        ~220 MB three-sweep)."""
        iv = self._ibkr_interval.get()
        tree = self._ibkr_tree
        tree.delete(*tree.get_children())
        try:                    # sidecar names once; manifest name wins when set
            names = dict(stock_validate.load_ibkr_names(self._storage_root))
        except Exception:  # noqa: BLE001
            names = {}
        try:
            dirs = sorted(self._storage_root.iterdir())
        except OSError:
            dirs = []
        listed = set()
        for d in dirs:
            try:
                if not (d.is_dir()
                        and stock_storage.TICKER_DIR_RE.match(d.name)):
                    continue
            except OSError:
                continue
            m = stock_storage.load_manifest(d)
            if not m or iv not in m.get("intervals", {}):
                continue          # only series STORED at the chosen interval
                                  # (1s won't list 1m-only tickers, etc.)
            ticker = d.name
            listed.add((ticker.upper(), iv))
            nm = m.get("name") or names.get(ticker, "")
            plan = stock_ibkr.plan_gap(self._storage_root, ticker, iv,
                                       manifest=m)
            if plan.get("error"):
                tree.insert("", tk.END, values=(
                    ticker, nm, iv, plan["error"], "—", "—"))
            elif plan["empty_series"]:
                tree.insert("", tk.END, values=(
                    ticker, nm, iv, "(none stored — FULL backfill)", "?",
                    "needs connection"))
            else:
                est = plan["est_requests"]
                tree.insert("", tk.END, values=(
                    ticker, nm, iv,
                    f"{plan['last_stored']:%m/%d/%Y %H:%M}"
                    + ("  [1s window clipped]" if plan["clipped_1s"]
                       else ""),
                    len(plan["days"]),
                    f"{est:,}  (~{est * _est_secs_per_request(iv) / 60:.0f} "
                    f"min)"))
        try:
            halted = stock_ibkr.halted_series_for_interval(
                self._storage_root, iv, known=listed)
        except Exception:  # noqa: BLE001 - advisory retry rows only
            halted = []
        for ticker, _iv, rec in halted:
            reason = str((rec or {}).get("reason") or "halted")
            if len(reason) > 90:
                reason = reason[:87] + "..."
            tree.insert("", tk.END, values=(
                ticker, names.get(ticker, ""), iv, "(halted - retry)", "?",
                reason))
        self._ibkr_planned_iv = iv      # what the rows on screen describe
        self._storage_ibkr_selection_changed()

    def _storage_ibkr_interval_changed(self, _evt=None) -> None:
        """Re-plan when the combobox moves to a DIFFERENT interval.

        Re-picking the same value is a deliberate no-op: the replan parses
        every ticker manifest (~1.3-1.7 s on the GUI thread, see
        `_storage_ibkr_refresh`), which is too expensive to spend on a
        selection that changed nothing. Set on completion, so a replan that
        dies part-way is retried rather than suppressed.
        """
        if self._ibkr_interval.get() == getattr(self, "_ibkr_planned_iv", None):
            return
        self._storage_ibkr_refresh()

    def _storage_ibkr_selection_changed(self, _evt=None) -> None:
        """Keep the update dialog's extended option truthful for its rows."""
        try:
            tree = self._ibkr_tree
            tokens = []
            for item in tree.selection():
                values = tree.item(item).get("values", ())
                if len(values) > 2:
                    tokens.append(str(values[2]))
            self._set_extended_checkbox_state(
                self._ibkr_extended_cb, tokens)
        except Exception:  # noqa: BLE001 - dialog may be closing/rebuilding
            pass

    def _storage_ibkr_start(self) -> None:
        if self._storage_busy():
            return
        sel = self._ibkr_tree.selection()
        if not sel:
            self._storage_ibkr_say("Select one or more rows first "
                                   "(or click 'Select all').")
            return
        selections = [(str(self._ibkr_tree.item(i)["values"][0]),
                       str(self._ibkr_tree.item(i)["values"][2]))
                      for i in sel]      # cols: symbol, name, interval…
        # Expand to -pre/-post NOW (when '+ extended hours' is on) so the
        # estimate below and the actual run operate on the SAME list — extended
        # turns each interval into up to 3 series, so estimating pre-expansion
        # would undersize the approval popup by up to ~3x.
        selections = self._expand_daily(self._expand_extended(selections))
        # Lookback: how far back a NEW extended-hours (-pre/-post) series
        # backfills. 'From data start' (None) lets the engine cap each one at
        # its regular series' own first stored bar; a fixed depth limits it to
        # today-N. Existing series ignore this (they only extend forward).
        import datetime as _dt2
        _depth = (self._ibkr_depth.get()
                  if hasattr(self, "_ibkr_depth") else "From data start")
        _ddays = {"6 months": 183, "1 year": 365, "2 years": 730,
                  "3 years": 1095, "5 years": 1825}
        update_since = (_dt2.date.today() - _dt2.timedelta(days=_ddays[_depth])
                        if _depth in _ddays else None)
        # interval-aware wall-clock estimate BEFORE the run (GUI-free
        # helper, offline from the manifest gaps): sized as an UPDATE, so
        # already-current series cost ~1 overlap request each.
        wt = None
        try:
            wt = _estimate_worktime(self._storage_root, selections,
                                    "update", since=update_since,
                                    n_accounts=self._tws_estimate_lanes())
            est_line = (f"Update {len(selections)} series: ~"
                        f"{wt['requests']:,} request(s), estimated "
                        f"wall-clock time {wt['human']}.")
            for n in wt.get("notes", []):
                if n.startswith("1-second"):
                    est_line += "\n\n" + n
            go = messagebox.askyesno(
                "IBKR update", est_line + "\n\nProceed?",
                parent=self._storage_ibkr_win)
        except Exception:  # noqa: BLE001 — never block on a bad estimate
            go = True
        if not go:
            self._storage_ibkr_say(
                "Update declined — nothing fetched or written.")
            return
        import queue
        import threading
        self._storage_ibkr_running = True
        self._ibkr_start_btn.config(state=tk.DISABLED)
        # Cancel is GATED — unlocks only at 'Paused ✓' (a complete bank). To stop,
        # Pause first; it finalizes (months + xval + gap-seal), then Cancel.
        self._ibkr_cancel_btn.config(state=tk.DISABLED)
        self._ibkr_pause_btn.config(state=tk.NORMAL, text="Pause")
        self._storage_btn.config(state=tk.DISABLED)
        self._ibkr_selections = list(selections)   # for the pause-finalize seal
        self._ibkr_port_states = {}                # per-port pause state (parallel)
        self._ibkr_port_lock = threading.Lock()    # worker-thread writes vs GUI reads
        self._ibkr_pause_state = "running"         # pause/cancel state machine
        self._ibkr_run_start = None
        try:
            self._ibkr_progress.config(value=0, maximum=1)
            self._ibkr_progress_lbl.config(text="")
        except Exception:  # noqa: BLE001
            pass
        # show the recalibrated WALL-CLOCK estimate as the live ETA RIGHT NOW
        # (before connect + the first '[i/N]' marker); it then blends toward the
        # measured rate as series complete. No-op if the estimate failed.
        self._batch_pred_map = (wt or {}).get("per_series_seconds")
        self._batch_seed("_ibkr_run_start", self._ibkr_progress_lbl,
                         len(selections), (wt or {}).get("seconds"))
        q = queue.Queue()
        ev = threading.Event()
        pev = threading.Event()
        self._storage_ibkr_q = q
        self._storage_ibkr_ev = ev
        self._storage_ibkr_pause_ev = pev
        self._storage_ibkr_say(
            "Checking TWS before touching the archive…", append=False)
        ports = self._tws_ports()              # resolve on the GUI thread
        self._ibkr_build_port_monitor(ports)
        factory = self._tws_factory()
        # selections were already expanded to -pre/-post (if enabled) up front,
        # so the estimate and the run match.
        parallel = (self._parallel_enabled() and len(set(ports)) > 1
                    and len({t for t, _i in selections}) > 1)
        import time as _rt
        self._ibkr_run_params = {
            "flow": "IBKR update", "mode": "parallel" if parallel else "serial",
            "ports": list(ports),
            "extended": bool(self._storage_tws_extended.get()),
            "started_clock": _rt.time()}

        def _work():
            nonlocal selections
            def say(m):
                if (isinstance(m, tuple) and len(m) == 2
                        and m[0] in ("report", "recovering")):
                    q.put(m)
                else:
                    q.put(("progress", m))
            # The engine's explicit nightly root owns identity preflight and
            # fill together. Authority/ledger errors reach this task's error
            # channel instead of being swallowed as best-effort validation.
            if parallel:
                # Parallel/fleet: the resilient orchestrator PRE-FLIGHTS (auto-
                # restarts any logged-out port — the daily-logout case), fetches,
                # then restarts + re-fetches only the ports that aborted. This
                # replaces the all-or-nothing preflight: a dud port no longer
                # kills the whole run, it self-heals or is skipped per-ticker.
                restart_ports, port_up = self._fleet_callbacks(
                    say, ports, cancel=ev)
                say(f"Starting parallel update for {len(selections)} series "
                    f"(auto-restarting any logged-out port)…")
                try:
                    rep = stock_ibkr.nightly_update(
                        self._storage_root, selections, ports,
                        preflight_factory=factory,
                        restart_ports=restart_ports, port_up=port_up,
                        progress=say, cancel=ev, pause=pev, since=update_since,
                        port_status=self._ibkr_port_status_cb,   # per-port pause state
                        # adaptive OFF: 2026-06-23 choke-hunt showed the demo
                        # choke is NOT caused by local port count (N=5 sustained
                        # = 0 drops); flip to True only to re-test that premise.
                        on_series=self._xval_on_series, adaptive=False)
                except Exception as exc:  # noqa: BLE001
                    rep = exc
                q.put(("done", rep))
                return
            # Serial nightly updates need the same explicit A1 context and
            # worker as the fleet path. Never call unscoped gap_fill/preflight.
            # One selected port preserves the user's parallel-off choice.
            serial_ports = list(ports[:1])
            restart_ports, port_up = self._fleet_callbacks(
                say, serial_ports, cancel=ev)
            say(f"Starting single-port update for {len(selections)} series…")
            try:
                rep = stock_ibkr.nightly_update(
                    self._storage_root, selections, serial_ports,
                    preflight_factory=factory,
                    restart_ports=restart_ports, port_up=port_up,
                    progress=say, cancel=ev, pause=pev, since=update_since,
                    port_status=self._ibkr_port_status_cb,
                    on_series=self._xval_on_series, adaptive=False)
            except Exception as exc:  # noqa: BLE001
                rep = exc
            q.put(("done", rep))

        self._xval_start()                     # cross-validate as tickers commit
        try:
            threading.Thread(target=_work, daemon=True,
                             name="ibkr-fetch").start()
        except Exception as exc:  # noqa: BLE001
            self._storage_ibkr_done(exc)
            return
        self.root.after(100, self._storage_ibkr_poll)

    def _storage_ibkr_cancel(self) -> None:
        ev = getattr(self, "_storage_ibkr_ev", None)
        if ev is not None:
            ev.set()
            self._xval_finish()           # also stop cross-validation (the worker sees
            #                               the cancel Event above and DROPS its queue
            #                               instead of draining slow online checks)
            self._ibkr_cancel_btn.config(state=tk.DISABLED)
            self._storage_ibkr_say(
                "Cancelling — committed months stay; a re-run resumes "
                "where this stopped.")

    def _storage_ibkr_pause(self) -> None:
        """Pause = finalize the bank to a COMPLETE state (finish month -> drain
        cross-validation -> seal gaps), THEN Cancel/exit unlocks. Resume continues
        + restarts cross-validation."""
        pev = getattr(self, "_storage_ibkr_pause_ev", None)
        if pev is None:
            return
        if pev.is_set():                           # RESUME (from pausing/finalizing/paused)
            pev.clear()
            self._ibkr_pause_state = "running"
            self._batch_resume()                   # un-freeze the elapsed/ETA clock
            try:
                self._ibkr_pause_btn.config(text="Pause")
                self._ibkr_cancel_btn.config(state=tk.DISABLED)   # re-gate
            except Exception:  # noqa: BLE001
                pass
            self._xval_start()                     # the finalize stopped it -> restart
            self._storage_ibkr_say("Resumed.")
        else:                                      # PAUSE -> begin the FINALIZE sequence
            pev.set()
            self._ibkr_pause_state = "pausing"
            try:
                self._ibkr_pause_btn.config(text="Pausing…")
            except Exception:  # noqa: BLE001
                pass
            self._ibkr_pause_status_detail()
            self._storage_ibkr_say(
                "Pausing — each port finishes its month, then cross-validation "
                "drains and gaps are SEALED. 'Paused ✓' (and Cancel/exit) unlock "
                "only when the bank is COMPLETE. Resume to continue.")

    def _ibkr_clear_port_monitor(self) -> None:
        f = getattr(self, "_ibkr_port_frame", None)
        if f is not None:
            for child in list(f.winfo_children()):
                try:
                    child.destroy()
                except Exception:  # noqa: BLE001
                    pass
        self._ibkr_port_rows = {}
        self._ibkr_port_states = {}

    def _ibkr_build_port_monitor(self, ports) -> None:
        import threading
        self._ibkr_clear_port_monitor()
        self._ibkr_port_lock = threading.Lock()
        self._ibkr_port_states = {}
        f = getattr(self, "_ibkr_port_frame", None)
        if f is None:
            return
        for p in list(dict.fromkeys(int(x) for x in (ports or ()))):
            row = ttk.Frame(f)
            row.pack(fill=tk.X)
            dot = tk.Canvas(row, width=12, height=12, highlightthickness=0)
            oid = dot.create_oval(2, 2, 10, 10, fill="#888", outline="")
            dot.pack(side=tk.LEFT, padx=(2, 6))
            var = tk.StringVar(value=f"port {p}: waiting...")
            lbl = ttk.Label(row, textvariable=var, font=("Consolas", 9, "bold"),
                            foreground="#888888")
            lbl.pack(side=tk.LEFT)
            self._ibkr_port_rows[p] = (dot, oid, var, lbl)

    def _ibkr_pause_status_detail(self, states=None, sealing=False) -> str:
        if states is None:
            lock = getattr(self, "_ibkr_port_lock", None)
            if lock is not None:
                with lock:
                    states = dict(getattr(self, "_ibkr_port_states", {}) or {})
            else:
                states = dict(getattr(self, "_ibkr_port_states", {}) or {})
        rows = getattr(self, "_ibkr_port_rows", None) or {}
        total = len(rows)
        if not total and (getattr(self, "_ibkr_run_params", {}) or {}).get(
                "mode") == "serial":
            total = 1
        line = self._pause_status_text(states, total, sealing=sealing)
        self._batch_set_detail(getattr(self, "_ibkr_progress_lbl", None), line)
        return line

    @staticmethod
    def _pacing_detail_from_states(states) -> str:
        """Return the longest still-active per-port pacer wait, if any."""
        active = []
        for info in (states or {}).values():
            info = info or {}
            if str(info.get("state") or "working") != "working":
                continue
            label = str(info.get("pacing_detail") or "")
            if not label:
                continue
            try:
                seconds = max(0.0, float(info.get("pacing_seconds") or 0.0))
            except (TypeError, ValueError):
                seconds = 0.0
            active.append((seconds, label))
        return max(active, default=(0.0, ""), key=lambda row: row[0])[1]

    def _ibkr_render_ports(self) -> None:
        import time
        rows = getattr(self, "_ibkr_port_rows", None)
        lock = getattr(self, "_ibkr_port_lock", None)
        if not rows or lock is None:
            return
        try:
            with lock:
                states = dict(getattr(self, "_ibkr_port_states", {}) or {})
        except Exception:  # noqa: BLE001
            states = {}
        now = time.time()
        green, red, yellow, grey, cyan = ("#1a9e3f", "#cc2222", "#c8920a",
                                          "#888888", "#1f8fb0")
        for p, (dot, oid, var, lbl) in rows.items():
            st = states.get(p) or {}
            if not st:
                color, text = grey, f"port {p}: waiting..."
            else:
                age = now - float(st.get("ts") or now)
                state = str(st.get("state") or "working")
                tk_ = st.get("ticker") or "-"
                cnt = int(st.get("count") or 0)
                detail = str(st.get("detail") or "")
                pacing_detail = str(st.get("pacing_detail") or "")
                detail_part = f" - {detail}" if detail else ""
                if state == "error":
                    color = red
                    text = f"port {p}: OFFLINE  {str(st.get('error', ''))[:44]}"
                elif state == "suspect":
                    color = yellow
                    text = (f"port {p}: SUSPECT - hard disconnect; probing "
                            f"({cnt} done)")
                elif state == "dead":
                    color = red
                    text = (f"port {p}: DEAD - probe grace exhausted; "
                            f"still probing ({cnt} done)")
                elif state == "probing":
                    color = yellow
                    text = f"port {p}: PROBING for recovery ({cnt} done)"
                elif state in ("retrying", "recovering"):
                    color = yellow
                    label = "RECOVERING" if state == "recovering" else "RETRYING"
                    text = f"port {p}: {label}  {tk_}{detail_part}  ({cnt} done)"
                elif state == "queued":
                    color = grey
                    text = f"port {p}: queued..."
                elif state == "connecting":
                    color = yellow
                    text = f"port {p}: connecting..."
                elif state == "done":
                    color = green
                    text = (f"port {p}: ONLINE  done - {cnt} series, "
                            f"{int(st.get('added') or 0):,} bars")
                elif state == "parked":
                    color = grey
                    text = f"port {p}: PARKED  easing backend load ({cnt} done)"
                elif state == "paused":
                    color = cyan
                    last_month = str(st.get("last_month") or "")
                    where = (f"finished {last_month}" if last_month
                             else "clean boundary")
                    text = f"port {p}: PAUSED  {where} ({cnt} done)"
                elif pacing_detail:
                    # An engine-confirmed pacer wait is healthy activity; it
                    # outranks the generic 45-second quiet heuristic.
                    color = yellow
                    text = (f"port {p}: ONLINE  {tk_} - {pacing_detail}  "
                            f"({cnt} done)")
                elif age > 45:
                    color = red
                    text = f"port {p}: OFFLINE? quiet {age:.0f}s on {tk_}"
                else:
                    color = green
                    text = f"port {p}: ONLINE  {tk_}{detail_part}  ({cnt} done)"
            try:
                dot.itemconfig(oid, fill=color)
                lbl.config(foreground=color)
                var.set(text)
            except Exception:  # noqa: BLE001
                pass
        self._batch_set_pacing_detail(
            getattr(self, "_ibkr_progress_lbl", None),
            self._pacing_detail_from_states(states))
        if getattr(self, "_ibkr_pause_state", None) in ("pausing", "finalizing"):
            self._ibkr_pause_status_detail(
                states, sealing=getattr(self, "_ibkr_pause_state", None)
                == "finalizing")

    def _ibkr_port_status_cb(self, port, info) -> None:
        """Engine worker-thread callback: store this port's status snapshot so the
        pause finalize trigger knows when EVERY port has idled at its boundary."""
        import time
        lock = getattr(self, "_ibkr_port_lock", None)
        try:
            info = dict(info or {})
        except Exception:  # noqa: BLE001
            info = {}
        info["ts"] = time.time()
        try:
            if lock is not None:
                with lock:
                    self._ibkr_port_states[int(port)] = info
            else:
                self._ibkr_port_states[int(port)] = info
        except Exception:  # noqa: BLE001
            pass

    def _ibkr_pause_check(self) -> None:
        """From the run poll: once every active port reports 'paused' (or, for a
        SERIAL run with no per-port states, the moment pause takes), kick off the
        finalize exactly ONCE."""
        pev = getattr(self, "_storage_ibkr_pause_ev", None)
        if (pev is None or not pev.is_set()
                or getattr(self, "_ibkr_pause_state", None) != "pausing"):
            return
        # Mode comes from the run params set SYNCHRONOUSLY at start — NOT from whether
        # _ibkr_port_states is populated, which is legitimately EMPTY during a PARALLEL
        # run's preflight/spin-up window (port probes + logged-out-port restart happen
        # before the first worker heartbeat). Misreading that empty window as 'serial'
        # finalizes early and falsely unlocks Cancel mid-fetch.
        mode = (getattr(self, "_ibkr_run_params", {}) or {}).get("mode", "serial")
        if mode == "parallel":
            lock = getattr(self, "_ibkr_port_lock", None)
            if lock is not None:
                with lock:
                    states = dict(getattr(self, "_ibkr_port_states", {}) or {})
            else:
                states = dict(getattr(self, "_ibkr_port_states", {}) or {})
            if not states:
                return                             # no port has heart-beat yet -> wait
            stz = {p: (v or {}).get("state") for p, v in states.items()}
            pending = [p for p, s in stz.items()
                       if s not in ("done", "error", "dead", None)]
            if pending and not all(stz[p] == "paused" for p in pending):
                return
        # serial OR every parallel port paused -> kick the finalize. DON'T latch
        # 'finalizing' here: _storage_ibkr_finalize commits it only AFTER it spawns
        # a daemon, so if a PRIOR finalize is still draining a slow poison month
        # (a re-pause) the state stays 'pausing' and this poll simply retries.
        self._storage_ibkr_finalize()

    def _seal_aborted(self):
        """cancel hook for a pause-finalize gap-seal: True once the user RESUMED
        (state left 'finalizing' for 'running') so the seal stops promptly instead
        of racing the resumed fetch workers. No-op for the standalone 'Fill gaps'
        heal (no finalize state is active -> never aborts)."""
        return (getattr(self, "_ibkr_pause_state", None) == "running"
                or getattr(self, "_find_pause_state", None) == "running")

    def _finalize_drain_and_seal(self, series, ports, say):
        """Shared pause-FINALIZE body (runs on a daemon thread). (1) DRAIN the
        cross-validation queue, POLLING so we can show a live readout via `say`
        instead of a blind blocking join — HARD-capped at 180 s so a wedged online
        check can't hold the finalize. (2) SEAL the committed series' gaps under a
        5-minute OVERALL budget + a per-month cancel, so neither a slow connect nor
        a poison ticker (one the demo hangs on) can wedge the finalize forever — the
        finalize ALWAYS completes, so Cancel always eventually unlocks. `say` must
        marshal to the GUI thread itself. Returns the bars sealed. Best-effort."""
        import time as _t
        root = self._storage_root
        try:
            self._xval_finish()
            th = getattr(self, "_xval_thread", None)
            q0 = getattr(self, "_xval_q", None)         # THIS session's queue
            last_line = ""
            if q0 is not None:
                last_line = self._xval_finalize_status(q0)
                say(last_line)
            if th is not None and th.is_alive():
                dl = _t.monotonic() + 180
                while th.is_alive() and _t.monotonic() < dl:
                    # Resume rebinds _xval_q to a new session; ignore q0 then.
                    if getattr(self, "_xval_q", None) is q0:
                        line = self._xval_finalize_status(q0)
                        if line != last_line:
                            say(line)
                            last_line = line
                    th.join(timeout=0.5)
            if q0 is not None and getattr(self, "_xval_q", None) is q0:
                line = self._xval_finalize_status(q0)
                if line != last_line:
                    say(line)
        except Exception:  # noqa: BLE001
            pass
        sealed = 0
        series_left = 0            # series whose gaps the budget/Resume left unsealed
        adapter = None
        todo = []
        seal_started = False
        seal_error = False
        try:
            say("Finalizing: sealing gaps ...")
            seal_started = True
            cal = (getattr(self, "_gap_cal_cache", None)
                   or stock_validate.consensus_calendar(root))
            self._gap_cal_cache = cal
            for t, iv in series:
                s = stock_validate.scan_series_gaps(root, t, iv, calendar_days=cal)
                md = s.get("missing_days") or []
                if md:
                    todo.append((t, iv, md))
            if todo and ports:
                seal_dl = _t.monotonic() + 300            # overall seal budget
                stop = (lambda: bool(self._seal_aborted())
                        or _t.monotonic() > seal_dl)       # resumed OR budget spent
                adapter = stock_ibkr.ReusableAdapter(stock_ibkr.live_adapter_factory(
                    host=stock_ibkr.HOST_DEFAULT, ports=(ports[0],),
                    client_id=stock_ibkr.CLIENT_ID_SEAL))
                for n, (t, iv, md) in enumerate(todo, 1):
                    if stop():
                        series_left = len(todo) - n + 1
                        say(f"Finalizing — seal time budget reached; "
                            f"{series_left} series left (re-run to finish)…")
                        break
                    say(f"Finalizing — sealing gaps {n}/{len(todo)} ({t})…")
                    try:
                        r = stock_ibkr.fill_missing_days(
                            adapter, root, t, iv, md, cancel=stop)
                        sealed += r.get("added", 0)
                    except (stock_ibkr.AuthorityError, stock_ibkr.LedgerError,
                            stock_ibkr.Cancelled):
                        raise
                    except Exception:  # noqa: BLE001
                        pass
                import datetime as _dt
                stock_validate.update_gap_report(
                    root, [(t, iv) for t, iv, _ in todo],
                    asof=_dt.datetime.now().isoformat(timespec="seconds"))
        except Exception:  # noqa: BLE001
            seal_error = True
            pass
        finally:
            if adapter is not None:
                try:
                    adapter.close()
                except Exception:  # noqa: BLE001
                    pass
            if seal_started:
                try:
                    if series_left:
                        say(f"Finalizing: sealing gaps stopped (+{sealed:,} bars; "
                            f"{series_left} series left)")
                    elif seal_error:
                        say(f"Finalizing: sealing gaps ended (+{sealed:,} bars)")
                    elif todo and not ports:
                        say(f"Finalizing: sealing gaps done (+{sealed:,} bars; "
                            "no TWS port available)")
                    elif not todo:
                        say("Finalizing: sealing gaps done (+0 bars; no gaps found)")
                    else:
                        say(f"Finalizing: sealing gaps done (+{sealed:,} bars)")
                except Exception:  # noqa: BLE001
                    pass
        return sealed, series_left

    def _storage_ibkr_finalize(self) -> None:
        """Update-dialog pause FINALIZE: drain cross-validation, then SEAL the
        committed series' gaps on the distinct CLIENT_ID_SEAL, then 'Paused ✓' +
        unlock Cancel. Daemon; best-effort. Re-entrancy-guarded: a rapid
        resume-then-re-pause never spawns a SECOND concurrent seal daemon."""
        import threading
        th0 = getattr(self, "_ibkr_finalize_thread", None)
        if th0 is not None and th0.is_alive():
            return                  # prior daemon still draining -> poll retries
        root = self._storage_root
        series = [(t, iv) for (t, iv) in (getattr(self, "_ibkr_selections", []) or [])
                  if stock_storage.session_of(str(iv)) == "rth"]
        ports = list(self._tws_ports() or ())
        self._ibkr_finalize_id = getattr(self, "_ibkr_finalize_id", 0) + 1
        fid = self._ibkr_finalize_id              # stale-daemon guard

        def _work():
            say = lambda m: self.root.after(
                0, lambda mm=m: self._storage_ibkr_say(mm))
            sealed, left = self._finalize_drain_and_seal(series, ports, say)
            try:
                self.root.after(0, lambda s=sealed, l=left, i=fid:
                                self._storage_ibkr_paused_complete(s, l, i))
            except Exception:  # noqa: BLE001 — root destroyed by app-close mid-run
                pass

        try:
            t = threading.Thread(target=_work, daemon=True,
                                 name="ibkr-pause-finalize")
            self._ibkr_finalize_thread = t
            t.start()
            self._ibkr_pause_state = "finalizing"   # commit ONLY now (real daemon
            self._batch_pause()                     # all ports idle -> freeze elapsed
            try:                                    # running) so the poll stops
                self._ibkr_pause_btn.config(text="Finalizing…")   # re-triggering
            except Exception:  # noqa: BLE001
                pass
            self._ibkr_pause_status_detail(sealing=True)
        except Exception:  # noqa: BLE001
            self._storage_ibkr_paused_complete(0, 0, fid)

    def _storage_ibkr_paused_complete(self, sealed, series_left=0, fid=None) -> None:
        """GUI thread: Update finalize done. Fully paused, unlock Cancel (safe
        exit). Ignore if the user resumed mid-finalize, OR if this is a STALE
        daemon's callback (a newer finalize cycle has started). The wording is
        HONEST: it only claims 'bank COMPLETE' when every series was sealed."""
        if fid is not None and fid != getattr(self, "_ibkr_finalize_id", 0):
            return                  # a newer finalize cycle owns the state now
        if getattr(self, "_ibkr_pause_state", None) != "finalizing":
            return
        self._ibkr_pause_state = "paused"
        try:
            self._ibkr_pause_btn.config(text="Paused ✓")
            self._ibkr_cancel_btn.config(state=tk.NORMAL)   # safe-exit unlocks
            self._batch_set_detail(getattr(self, "_ibkr_progress_lbl", None),
                                   "Paused ✓")
        except Exception:  # noqa: BLE001
            pass
        if series_left:
            self._storage_ibkr_say(
                f"⏸ PAUSED — months written, cross-validation done, +{sealed:,} "
                f"gap bar(s) sealed; {series_left} series still have gaps (re-run "
                f"to finish). Safe to Cancel/close, or Resume.")
        else:
            self._storage_ibkr_say(
                f"⏸ PAUSED — bank COMPLETE: months written, cross-validation done, "
                f"+{sealed:,} gap bar(s) sealed. Safe to Cancel/close, or Resume.")
        try:
            self._storage_ibkr_refresh()
        except Exception:  # noqa: BLE001
            pass

    def _storage_ibkr_interim_report(self, rep) -> None:
        """Write a fetch-phase log before recovery starts.

        The final completion handler still writes the canonical run log. This
        writes <run>-interim.log so a long recovery has an on-disk trail.
        """
        if not isinstance(rep, dict):
            return
        try:
            import copy
            out = copy.deepcopy(rep)
        except Exception:  # noqa: BLE001
            out = dict(rep)
        run = str(out.get("run") or "ibkr-run")
        if not run.endswith("-interim"):
            out["run"] = f"{run}-interim"
        notes = list(out.get("notes") or [])
        notes.insert(0, "INTERIM fetch-phase log: recovery is still running; "
                        "the final log overwrites nothing and writes at true "
                        "completion.")
        out["notes"] = notes
        self._storage_ibkr_say(
            "Fetch phase ended with an aborted port; writing interim run log "
            "before recovery.")
        self._write_run_log(out, getattr(self, "_ibkr_run_params", None),
                            self._storage_ibkr_say)

    def _storage_ibkr_recovering(self, payload) -> None:
        if not isinstance(payload, dict):
            payload = {}
        raw_ports = list(payload.get("ports") or [])
        ports = ", ".join(str(p) for p in (payload.get("ports") or [])) or "?"
        series = int(payload.get("series") or 0)
        rnd = int(payload.get("round") or 1)
        msg = (f"Recovering: re-fetching up to {series:,} series on "
               f"port(s) {ports} (round {rnd}).")
        self._storage_ibkr_say(msg)
        try:
            import time
            lock = getattr(self, "_ibkr_port_lock", None)
            if lock is not None:
                with lock:
                    states = getattr(self, "_ibkr_port_states", {})
                    for p in raw_ports:
                        states[int(p)] = {"state": "recovering",
                                          "ticker": "recovery",
                                          "count": 0,
                                          "added": 0,
                                          "ts": time.time()}
            self._ibkr_render_ports()
        except Exception:  # noqa: BLE001
            pass
        try:
            self._ibkr_progress_lbl.config(text=msg)
            self._ibkr_cancel_btn.config(state=tk.NORMAL)
        except Exception:  # noqa: BLE001
            pass

    def _storage_ibkr_poll(self) -> None:
        q = getattr(self, "_storage_ibkr_q", None)
        if q is None:
            return
        try:
            lines, marker, terminal, backlog, events, detail = (
                self._drain_ibkr_progress(q))
        except Exception as exc:  # noqa: BLE001
            self._storage_ibkr_q = None
            self._storage_ibkr_done(RuntimeError(f"display failed: {exc}"))
            return
        if lines or detail is not None:
            try:
                self._batch_say(getattr(self, "_ibkr_out", None), "_ibkr_progress",
                                "_ibkr_progress_lbl", "_ibkr_run_start",
                                lines, marker, detail)
            except Exception as exc:  # noqa: BLE001
                self._storage_ibkr_q = None
                self._storage_ibkr_done(RuntimeError(f"display failed: {exc}"))
                return
        for kind, payload in events:
            if kind == "report":
                self._storage_ibkr_interim_report(payload)
            elif kind == "recovering":
                self._storage_ibkr_recovering(payload)
        try:
            self._ibkr_render_ports()
        except Exception:  # noqa: BLE001
            pass
        if terminal is not None:
            kind, payload = terminal
            self._storage_ibkr_q = None
            if kind == "preflight":
                self._storage_ibkr_no_go(payload)
            else:
                self._storage_ibkr_done(payload)
            return
        try:
            self._ibkr_pause_check()           # advance the pause-finalize state machine
        except Exception:  # noqa: BLE001
            pass
        try:                                   # backlog -> come back sooner
            self.root.after(10 if backlog else 100, self._storage_ibkr_poll)
        except Exception:  # noqa: BLE001
            pass

    def _storage_ibkr_no_go(self, failures) -> None:
        """Preflight said no: nothing was fetched or written. The app
        being closed gets a direct popup; anything else pops the
        doctor's advice. A clean preflight never gets a popup."""
        self._storage_ibkr_running = False
        self._storage_ibkr_ev = None
        self._storage_ibkr_pause_ev = None
        self._ibkr_pause_state = None              # reset the pause state machine
        self._batch_live = None                    # stop the 1-second elapsed tick
        try:
            self._ibkr_start_btn.config(state=tk.NORMAL)
            self._ibkr_cancel_btn.config(state=tk.DISABLED)
            self._ibkr_pause_btn.config(state=tk.DISABLED, text="Pause")
            self._storage_btn.config(state=tk.NORMAL)
        except Exception:  # noqa: BLE001 — dialog closed mid-run
            pass
        self._storage_ibkr_mark_ports_done(
            RuntimeError("; ".join(str(f) for f in failures)))
        self._ibkr_render_ports()
        for line in failures:
            self._storage_ibkr_say(f"[FAIL] {line}")
        self._storage_ibkr_say("Nothing was fetched or written.")
        if any("nothing listening" in f for f in failures):
            msg = ("TWS / IB Gateway isn't open (or isn't logged in).\n\n"
                   "Start it, log in to the PAPER account, then press "
                   "'Start update' again.")
        else:
            msg = ("The connection check failed — nothing was "
                   "written.\n\n" + "\n\n".join(failures))
        try:
            self._show_unattended_notice(
                "IBKR update", msg, level="warning",
                parent=self._storage_ibkr_win)
        except Exception:  # noqa: BLE001 — headless / dialog closed
            pass

    def _storage_halted_series_popup(self, rep, parent) -> None:
        """Raise one completion popup for any halted series.

        P2 broadens the old reconnect-only popup: pacing, timeout, contract,
        and gate halts also need a visible end-of-run summary.
        """
        try:
            halted = [s for s in (rep or {}).get("series", [])
                      if s.get("halt")]
        except Exception:  # noqa: BLE001
            return
        if not halted:
            return
        lines = [
            f"{len(halted)} series halted during the run.",
            "",
            "Committed data stays. Re-run after fixing the listed issue; "
            "zero-commit halts are listed as retry rows in the update dialog.",
            "",
            "Halted series:"]
        for s in halted[:12]:
            halt = str(s.get("halt") or "")
            try:
                if "reconnect attempts" in halt:
                    detail = stock_ibkr.progress_summary(s)
                else:
                    detail = (f"{s.get('ticker', '?')} "
                              f"{s.get('interval', '?')}: {halt}")
            except Exception:  # noqa: BLE001
                detail = (f"{s.get('ticker', '?')} "
                          f"{s.get('interval', '?')}: {halt or 'halted'}")
            lines.append("  - " + detail)
        if len(halted) > 12:
            lines.append(f"  ... and {len(halted) - 12} more")
        try:
            self._show_unattended_notice(
                "IBKR update - halted series", "\n".join(lines),
                level="warning", parent=parent)
        except Exception:  # noqa: BLE001 - headless / dialog closed
            pass

    def _storage_lost_connection_popup(self, rep, parent) -> None:
        """If the network gave up after N reconnects on any series, raise
        ONE popup that names the lost connection and shows how much
        progress was SAVED per series (original stored end -> committed
        end, ~trading days). Committed data stays; a re-run resumes."""
        try:
            lost = stock_ibkr.lost_connection_series(rep)
        except Exception:  # noqa: BLE001
            return
        if not lost:
            return
        lines = ["Connection lost — TWS did not recover after "
                 f"{stock_ibkr.RECONNECT_ATTEMPTS} reconnect attempts.",
                 "", "Progress saved (committed data stays — re-run to "
                 "resume where it stopped):"]
        for s in lost:
            try:
                lines.append("  • " + stock_ibkr.progress_summary(s))
            except Exception:  # noqa: BLE001
                lines.append(f"  • {s.get('ticker', '?')} "
                             f"{s.get('interval', '?')}: progress unknown")
        try:
            self._show_unattended_notice(
                "Connection lost", "\n".join(lines), level="warning",
                parent=parent)
        except Exception:  # noqa: BLE001 — headless / dialog closed
            pass

    def _write_run_log(self, rep, params, say):
        """Persist a human-readable run log (the parameters USED + what
        happened) to a 'Run Logs' folder beside the data bank, and report the
        path via `say`. Best-effort — a log write never breaks completion.
        Shared by the IBKR-update and Add-stock flows."""
        if isinstance(rep, Exception) or not rep:
            return
        try:
            import time as _t
            from datetime import datetime as _dt
            from pathlib import Path as _Path
            import run_log
            ex = dict(params or {})
            if ex.get("started_clock"):
                sc = ex.pop("started_clock")
                ex["elapsed_s"] = _t.time() - sc
                ex["started_at"] = _dt.fromtimestamp(sc).isoformat(
                    timespec="seconds")
                ex["finished"] = _dt.now().isoformat(timespec="seconds")
            path = run_log.write_run_log(
                rep, _Path(self._storage_root).parent / "Run Logs", ex)
            if path:
                say(f"\n📄 run log saved: {path}")
        except Exception:  # noqa: BLE001
            pass

    def _storage_ibkr_mark_ports_done(self, rep) -> None:
        import time
        rows = getattr(self, "_ibkr_port_rows", None) or {}
        lock = getattr(self, "_ibkr_port_lock", None)
        if not rows or lock is None:
            return
        per_port = {}
        aborted = {}
        if isinstance(rep, dict):
            per_port = rep.get("per_port") or {}
            aborted = rep.get("aborted_ports") or {}

        def _pget(d, port, default=None):
            return d.get(port, d.get(str(port), default))

        try:
            with lock:
                states = getattr(self, "_ibkr_port_states", {})
                for p in rows:
                    cur = dict(states.get(p) or {})
                    if not isinstance(rep, dict):
                        states[p] = {"state": "error", "error": str(rep),
                                     "ticker": "-", "count": cur.get("count", 0),
                                     "added": cur.get("added", 0),
                                     "ts": time.time()}
                        continue
                    pp = _pget(per_port, p, {}) or {}
                    err = _pget(aborted, p)
                    if err:
                        states[p] = {"state": "error", "error": str(err),
                                     "ticker": "-",
                                     "count": pp.get("series", cur.get("count", 0)),
                                     "added": pp.get("added", cur.get("added", 0)),
                                     "ts": time.time()}
                    else:
                        states[p] = {"state": "done", "ticker": "-",
                                     "account": pp.get("account"),
                                     "count": pp.get("series", cur.get("count", 0)),
                                     "added": pp.get("added", cur.get("added", 0)),
                                     "ts": time.time()}
        except Exception:  # noqa: BLE001
            pass

    def _storage_ibkr_finished_popup(self, rep, parent) -> None:
        if not isinstance(rep, dict):
            return
        totals = rep.get("totals") or {}
        partition = rep.get("_partition") or {}
        unpulled = len(rep.get("_unpulled") or [])
        if partition:
            pulled = sum(len(v or []) for v in partition.values())
        else:
            pulled = len(getattr(self, "_ibkr_selections", []) or
                         rep.get("series", []) or [])
        target = len(getattr(self, "_ibkr_selections", []) or [])
        if target and not partition and not unpulled:
            pulled = target
        aborted = rep.get("aborted_ports") or {}
        halted = int((totals or {}).get("halted_series") or 0)
        lines = [
            f"Rows added: {int(totals.get('added') or 0):,}",
            f"Series committed: {len(rep.get('series') or []):,}",
            f"Series pulled by a worker: {pulled:,}",
            f"Series still unpulled: {unpulled:,}",
            f"Recovery rounds: {int(rep.get('recovery_rounds') or 0):,}",
            f"Halted series: {halted:,}",
        ]
        if target:
            lines.insert(2, f"Series selected: {target:,}")
        if aborted:
            lines.append("")
            lines.append("Ports still offline:")
            for p, err in list(aborted.items())[:8]:
                lines.append(f"  port {p}: {err}")
            if len(aborted) > 8:
                lines.append(f"  ... {len(aborted) - 8} more")
        issues = bool(rep.get("cancelled") or rep.get("aborted") or aborted
                      or unpulled or halted or rep.get("validation_stopped"))
        try:
            if issues:
                self._show_unattended_notice(
                    "IBKR update finished with issues", "\n".join(lines),
                    level="warning", parent=parent)
            else:
                self._show_unattended_notice(
                    "IBKR update complete", "\n".join(lines), parent=parent)
        except Exception:  # noqa: BLE001
            pass

    def _storage_ibkr_done(self, rep) -> None:
        self._storage_ibkr_running = False
        self._storage_ibkr_ev = None
        self._storage_ibkr_pause_ev = None
        self._ibkr_pause_state = None              # reset the pause state machine
        self._batch_live = None                    # stop the 1-second elapsed tick
        self._xval_finish()                        # let cross-validation drain
        try:
            self._ibkr_start_btn.config(state=tk.NORMAL)
            self._ibkr_cancel_btn.config(state=tk.DISABLED)
            self._ibkr_pause_btn.config(state=tk.DISABLED, text="Pause")
            self._storage_btn.config(state=tk.NORMAL)
        except Exception:  # noqa: BLE001 — dialog closed mid-run
            pass
        if isinstance(rep, Exception):
            self._storage_ibkr_mark_ports_done(rep)
            self._ibkr_render_ports()
            self._storage_ibkr_say(f"IBKR update failed: {rep}")
            try:
                self._storage_status.set(f"IBKR update failed: {rep}")
                self._show_unattended_notice(
                    "IBKR update failed", str(rep), level="error",
                    parent=getattr(self, "_storage_ibkr_win", None)
                    or self.root)
            except Exception:  # noqa: BLE001
                pass
            return
        self._last_ibkr_report = rep
        try:
            lines = stock_ibkr.summarize_report(rep)
        except Exception as exc:  # noqa: BLE001
            lines = [f"(report rendering failed: {exc})"]
        for ln in lines:
            self._storage_ibkr_say(ln)
        self._storage_set_issues(lines)
        self._storage_issue_prefix = lines    # survives the rescan
        self._write_run_log(rep, getattr(self, "_ibkr_run_params", None),
                            self._storage_ibkr_say)
        self._storage_ibkr_mark_ports_done(rep)
        self._ibkr_render_ports()
        self._storage_ibkr_finished_popup(
            rep, getattr(self, "_storage_ibkr_win", None) or self.root)
        self._storage_halted_series_popup(
            rep, getattr(self, "_storage_ibkr_win", None) or self.root)
        try:
            self._storage_ibkr_refresh()      # gap table reflects the fill
        except Exception:  # noqa: BLE001 — dialog closed mid-run
            pass
        # A CANCELLED run is a DELIBERATE partial stop — do NOT run the network
        # post-run workers: the gap-heal would re-fetch the not-yet-fetched days
        # (defeating "cancel with no consequence"), and the gap/internal popups
        # would flag the deliberately-unfetched span. Committed data is intact and
        # a normal re-run resumes where this stopped.
        if isinstance(rep, dict) and rep.get("cancelled"):
            self._storage_ibkr_say(
                "Cancelled — committed data is intact; skipping the post-run "
                "gap-heal/scan (a normal re-run resumes where this stopped).")
        else:
            self._gap_scan_after_fetch(rep)        # interior-gap scan of fetched series
            self._gap_heal_after_fetch(rep)        # connected-pattern: fetch + seal missing days
            self._internal_check_after_fetch(rep)  # internal daily check vs the stored 1d
            self._calendar_maintain_after_fetch(rep)
        self._storage_rescan()

    def _calendar_maintain_after_fetch(self, rep=None, say=None) -> None:
        """Refresh the auto-maintained market-closure sidecar after data writes.

        The reconciler is offline and writes only `_special_closures.json`.
        It runs quietly unless it adds a date or finds an unresolved issue.
        """
        if not isinstance(rep, dict) or rep.get("cancelled") or rep.get("aborted"):
            return
        import threading
        root = self._storage_root
        say = say or self._storage_ibkr_say

        def _work():
            try:
                report = calendar_reconciler.maintain(root, write=True)
                added = list(report.get("auto_added") or [])
                missing = list(report.get("missing") or [])
                if added:
                    msg = ("calendar maintain: auto-pinned closure(s) "
                           + ", ".join(added))
                elif missing:
                    msg = ("calendar maintain: unresolved missing closure(s) "
                           + ", ".join(missing))
                else:
                    return
            except Exception as exc:  # noqa: BLE001
                msg = f"calendar maintain skipped: {type(exc).__name__}: {exc}"
            try:
                self.root.after(0, lambda m=msg: say(m))
            except Exception:  # noqa: BLE001
                pass

        try:
            threading.Thread(target=_work, daemon=True,
                             name="calendar-maintain").start()
        except Exception:  # noqa: BLE001
            pass

    def _internal_check_after_fetch(self, rep) -> None:
        """Auto-fire the INTERNAL daily check on the just-fetched tickers: for each
        ticker with a sub-daily series, compare its stored 1m aggregated-to-daily
        against the stored 1d (no re-fetch — daily is now stored alongside), record
        the verdict to _internal_validation.json, and pop a ✗ alert on any CONFIRMED
        disagreement (volume mismatch or >3% price). Daemon worker; best-effort."""
        # keep (ticker, base_interval) pairs — the audit must read the SERIES
        # THAT WAS FETCHED, not a hardcoded '1m' (else 5m/15m/30m/1h/1s never check).
        try:
            pairs, seen = [], set()
            for r in sorted((rep or {}).get("series", []),
                            key=lambda r: (str(r.get("ticker") or ""),
                                           str(r.get("interval") or ""))):
                t, iv = r.get("ticker"), r.get("interval")
                if (not (t and iv)
                        or stock_storage.kind_of(str(iv)) != ""
                        or stock_storage.session_of(str(iv)) != "rth"):
                    continue                       # Price/TRADES RTH only
                base = stock_storage.base_interval(str(iv))
                if base == "1d" or (t, base) in seen:
                    continue                       # skip the daily ref + dedup
                seen.add((t, base))
                pairs.append((t, base))
        except Exception:  # noqa: BLE001
            pairs = []
        if not pairs:
            return
        import datetime as _dt
        import queue
        import threading
        root = self._storage_root
        q = queue.Queue()

        def _work():
            out = []
            for t, iv in pairs:
                try:
                    daily = stock_validate.read_series(root, t, "1d")
                    if not daily:
                        continue                 # no stored 1d -> can't check
                    ref = stock_validate.parse_ibkr_daily_reference(daily)
                    v = stock_validate.internal_daily_audit(
                        root, t, iv, daily_ref=ref)   # the REAL interval, not '1m'
                    v["asof"] = _dt.datetime.now().isoformat(timespec="seconds")
                    stock_validate.record_internal_validation(root, v)
                    out.append(v)
                except Exception:  # noqa: BLE001
                    pass
            q.put(out)

        def _poll():
            try:
                out = q.get_nowait()
            except queue.Empty:
                try:
                    self.root.after(200, _poll)
                except Exception:  # noqa: BLE001
                    pass
                return
            bad = [v for v in out if v.get("status") == "disagree"]
            try:
                self._storage_ibkr_say(
                    f"internal daily check: {len(out)} ticker(s) vs stored 1d, "
                    f"{len(bad)} disagreement(s)")
            except Exception:  # noqa: BLE001
                pass
            if bad:
                lines = "\n".join(
                    f"  ✗ {v['ticker']}: {len(v.get('confirmed') or {})} day(s)"
                    for v in bad[:20])
                try:
                    self._show_unattended_notice(
                        "Internal daily check",
                        f"{len(bad)} ticker(s) DISAGREE with IBKR's own daily "
                        f"(volume, or >3% price):\n{lines}\n\nDetails in "
                        f"_internal_validation.json.", level="warning",
                        parent=getattr(self, "_storage_ibkr_win", None)
                        or self.root)
                except Exception:  # noqa: BLE001
                    pass

        try:
            threading.Thread(target=_work, daemon=True,
                             name="internal-check").start()
            self.root.after(200, _poll)
        except Exception:  # noqa: BLE001
            pass

    def _gap_heal_after_fetch(self, rep) -> None:
        """Auto-heal the CONNECTED-PATTERN gaps of the just-fetched series after an
        Add stock / Update run, using the run's live fleet ports (the chosen trigger:
        fetch when ports are already up, else the 'Fill gaps' button)."""
        try:
            series = sorted({(r.get("ticker"), r.get("interval"))
                             for r in (rep or {}).get("series", [])
                             if r.get("ticker") and r.get("interval")
                             and stock_storage.session_of(
                                 str(r.get("interval"))) == "rth"})
        except Exception:  # noqa: BLE001
            series = []
        if series:
            self._storage_heal_gaps(series, source="run")

    def _storage_heal_gaps(self, series=None, source="manual") -> None:
        """CONNECTED-PATTERN heal: find whole MISSING trading days, fetch the
        fetchable ones via a fleet port (merge — adds missing bars, keeps existing),
        then re-scan so only the genuinely UNFETCHABLE remain flagged. `series` (a
        run's touched pairs) heals just those; series=None scans the WHOLE bank (the
        'Fill gaps' button). Daemon worker; needs a live port; best-effort — with no
        port it just flags, never disturbing the finished run."""
        import datetime as _dt
        import queue
        import threading
        root = self._storage_root
        ports = [p for p in (self._tws_ports() or ())]
        q = queue.Queue()

        def _work():
            out = {"todo": 0, "added": 0, "still": 0, "no_ports": not ports,
                   "error": None}
            adapter = None
            try:
                todo = []
                if series is not None:               # auto-heal: the touched series
                    # the consensus calendar is a ~bank-wide read (slow); build it
                    # ONCE per session and reuse — a slightly stale calendar only
                    # ever UNDER-flags very recent days (safe), and the manual
                    # 'Fill gaps' full scan refreshes the picture anyway.
                    cal = getattr(self, "_gap_cal_cache", None)
                    if cal is None:
                        cal = stock_validate.consensus_calendar(root)
                        self._gap_cal_cache = cal
                    for t, iv in [(t, iv) for (t, iv) in series
                                  if stock_storage.session_of(
                                      str(iv)) == "rth"]:
                        s = stock_validate.scan_series_gaps(
                            root, t, iv, calendar_days=cal)
                        md = s.get("missing_days") or []
                        if md:
                            todo.append((t, iv, md))
                else:                                # manual: full-bank scan + sidecar
                    full = stock_validate.scan_all_gaps(
                        root, write=True, preserve_existing=True)
                    if full.get("error"):
                        raise RuntimeError(
                            f"gap scan failed: {full['error']}")
                    for key, v in full.get("summary", {}).items():
                        md = v.get("missing_day_list") or []
                        if md:
                            t, _sep, iv = key.rpartition(" ")
                            todo.append((t, iv, md))
                out["todo"] = len(todo)
                if todo and ports:
                    adapter = stock_ibkr.ReusableAdapter(stock_ibkr.live_adapter_factory(
                        host=stock_ibkr.HOST_DEFAULT, ports=(ports[0],),
                        client_id=stock_ibkr.CLIENT_ID_FETCH))
                    for t, iv, md in todo:
                        try:
                            r = stock_ibkr.fill_missing_days(
                                adapter, root, t, iv, md, cancel=self._seal_aborted)
                            out["added"] += r.get("added", 0)
                        except (stock_ibkr.AuthorityError, stock_ibkr.LedgerError,
                                stock_ibkr.Cancelled):
                            raise
                        except Exception:  # noqa: BLE001 — one series failing is ok
                            pass
                if todo:                             # re-scan -> sidecar = remainder
                    asof = _dt.datetime.now().isoformat(timespec="seconds")
                    res = stock_validate.update_gap_report(
                        root, [(t, iv) for t, iv, _ in todo], asof=asof)
                    out["still"] = res.get("missing_days_run", 0)
            except Exception as exc:  # noqa: BLE001
                out["error"] = str(exc)
            finally:
                if adapter is not None:
                    try:
                        adapter.close()
                    except Exception:  # noqa: BLE001
                        pass
            q.put(out)

        def _poll():
            try:
                out = q.get_nowait()
            except queue.Empty:
                try:
                    self.root.after(300, _poll)
                except Exception:  # noqa: BLE001 — window closing
                    pass
                return
            try:
                if out.get("error"):
                    self._storage_ibkr_say(f"(gap heal skipped: {out['error']})")
                elif not out.get("todo"):
                    if source == "manual":
                        self._storage_ibkr_say(
                            "gap heal: no missing trading days — the bank is whole.")
                elif out.get("no_ports"):
                    self._storage_ibkr_say(
                        f"gap heal: {out['todo']} series have missing days but NO "
                        f"live port — flagged only. Start the fleet, then 'Fill gaps'.")
                else:
                    self._storage_ibkr_say(
                        f"gap heal: +{out['added']:,} bar(s) filled across "
                        f"{out['todo']} series; {out['still']:,} day(s) still "
                        f"unfetchable (flagged).")
            except Exception:  # noqa: BLE001
                pass
            try:
                self._storage_ibkr_refresh()         # repaint the Gaps column
            except Exception:  # noqa: BLE001
                pass

        try:
            threading.Thread(target=_work, daemon=True, name="gap-heal").start()
            self.root.after(300, _poll)
        except Exception:  # noqa: BLE001 — thread exhaustion: skip silently
            pass
    # ---- Fix data: dedicated staged + pausable + multi-port window --------

    _FIXDATA_STAGES = ((1, "①  Scan for gaps"),
                       (2, "②  Refetch missing days"),
                       (3, "③  Cross-check accuracy"))
    _FIXDATA_KIND_OPTIONS = (
        ("", "Price (TRADES)"),
        ("iv", "Implied volatility (IV)"),
        ("hvol", "Historical volatility (HVOL)"),
    )

    def _storage_fixdata_open(self) -> None:
        """The dedicated Fix-data window: a THREE-STAGE pipeline you can watch —
        ① scan the whole bank for gaps, ② refetch the missing days, ③ cross-check
        accuracy — run in PARALLEL across the fleet ports with live per-port
        activity. Pausable at a safe per-series boundary (Pause → 'Paused ✓');
        Cancel is GATED until paused, exactly like the fetch dialogs (a safe
        finalize-then-exit)."""
        if getattr(self, "_fixdata_win", None) is not None:
            try:
                if self._fixdata_win.winfo_exists():
                    self._fixdata_win.lift()
                    return
            except Exception:  # noqa: BLE001
                pass
        import threading
        self._fixdata_pause = threading.Event()
        self._fixdata_cancel = threading.Event()
        self._fixdata_q = None
        self._fixdata_substate = "idle"
        self._fixdata_close_on_done = False
        self._fixdata_port_vars = {}
        self._fixdata_port_states = {}

        win = tk.Toplevel(self.root)
        win.title("Repair data — scan · refetch · cross-check")
        win.geometry("760x620")
        win.minsize(680, 540)
        win.transient(self.root)
        self._fixdata_win = win
        win.protocol("WM_DELETE_WINDOW", self._fixdata_on_close)

        top = ttk.Frame(win, padding=(10, 8))
        top.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(top, text="Whole-bank repair: scan → refetch gaps → "
                  "cross-check accuracy, in parallel across the fleet.",
                  wraplength=720, foreground="#444").pack(side=tk.TOP, anchor="w")
        # Two option rows: run modifiers, then the kind selector. One row
        # overflowed the fixed 760px window once the Deep scan box landed,
        # clipping "Implied volatility" clean out of view (user 2026-07-27).
        options = ttk.Frame(top)
        options.pack(side=tk.TOP, fill=tk.X, anchor="w", pady=(6, 0))
        self._fixdata_deep_scan = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            options, text="Deep scan (re-probe ⊘ days; higher cost)",
            variable=self._fixdata_deep_scan).pack(
                side=tk.LEFT, padx=(12, 0))
        recovery = ttk.Frame(top)
        recovery.pack(side=tk.TOP, fill=tk.X, anchor="w", pady=(4, 0))
        self._fixdata_restart_dead = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            recovery,
            text=("Offer TWS restart after 3 failed port connects "
                  "(takes over the screen)"),
            variable=self._fixdata_restart_dead).pack(side=tk.LEFT)
        kinds = ttk.Frame(top)
        kinds.pack(side=tk.TOP, fill=tk.X, anchor="w", pady=(4, 0))
        ttk.Label(kinds, text="Fix which data:").pack(
            side=tk.LEFT, padx=(0, 4))
        self._fixdata_kind_vars = {}
        for kind, label in self._FIXDATA_KIND_OPTIONS:
            var = tk.BooleanVar(value=True)
            self._fixdata_kind_vars[kind] = var
            ttk.Checkbutton(kinds, text=label, variable=var).pack(
                side=tk.LEFT, padx=(0, 6))

        stages = ttk.LabelFrame(win, text="Progress", padding=(10, 6))
        stages.pack(side=tk.TOP, fill=tk.X, padx=10, pady=(4, 0))
        self._fixdata_names = {}
        self._fixdata_bars = {}
        self._fixdata_counts = {}
        for r, (idx, name) in enumerate(self._FIXDATA_STAGES):
            lbl = ttk.Label(stages, text=name, width=24)
            lbl.grid(row=r, column=0, sticky="w", pady=3)
            bar = ttk.Progressbar(stages, mode="determinate", length=300,
                                  maximum=100)
            bar.grid(row=r, column=1, sticky="we", padx=(6, 6), pady=3)
            cnt = tk.StringVar(value="—")
            ttk.Label(stages, textvariable=cnt, width=26).grid(
                row=r, column=2, sticky="w")
            self._fixdata_names[idx] = lbl
            self._fixdata_bars[idx] = bar
            self._fixdata_counts[idx] = cnt
        stages.columnconfigure(1, weight=1)

        self._fixdata_ports_frame = ttk.LabelFrame(
            win, text="Per-port activity", padding=(10, 6))
        self._fixdata_ports_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=(8, 0))
        ttk.Label(self._fixdata_ports_frame,
                  text="(starts when you press Start)",
                  foreground="#888").pack(anchor="w")

        logf = ttk.LabelFrame(win, text="Log", padding=(6, 2))
        logf.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10, pady=(8, 0))
        self._fixdata_log = tk.Text(logf, wrap="word", font=("Consolas", 9),
                                    height=8, state=tk.DISABLED)
        lsb = ttk.Scrollbar(logf, command=self._fixdata_log.yview)
        self._fixdata_log.config(yscrollcommand=lsb.set)
        lsb.pack(side=tk.RIGHT, fill=tk.Y)
        self._fixdata_log.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self._fixdata_status = tk.StringVar(value="Ready.")
        ttk.Label(win, textvariable=self._fixdata_status, foreground="#555",
                  anchor="w", padding=(12, 2)).pack(side=tk.TOP, fill=tk.X)

        btm = ttk.Frame(win, padding=(10, 8))
        btm.pack(side=tk.BOTTOM, fill=tk.X)
        self._fixdata_start_btn = ttk.Button(btm, text="Start",
                                             command=self._fixdata_start)
        self._fixdata_start_btn.pack(side=tk.LEFT)
        self._fixdata_pause_btn = ttk.Button(btm, text="Pause",
                                             command=self._fixdata_pause_toggle,
                                             state=tk.DISABLED)
        self._fixdata_pause_btn.pack(side=tk.LEFT, padx=(6, 0))
        self._fixdata_cancel_btn = ttk.Button(btm, text="Close",
                                              command=self._fixdata_cancel_click)
        self._fixdata_cancel_btn.pack(side=tk.RIGHT)

    def _fixdata_log_clear(self):
        try:
            self._fixdata_log.config(state=tk.NORMAL)
            self._fixdata_log.delete("1.0", tk.END)
            self._fixdata_log.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001
            pass

    def _fixdata_log_add(self, text):
        self._fixdata_log_add_many([text])

    def _fixdata_log_add_many(self, lines):
        lines = [str(line) for line in (lines or [])]
        if not lines:
            return
        try:
            self._fixdata_log.config(state=tk.NORMAL)
            self._fixdata_log.insert(tk.END, "\n".join(lines) + "\n")
            self._trim_text_log(self._fixdata_log)
            self._fixdata_log.see(tk.END)
            self._fixdata_log.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001
            pass

    def _fixdata_highlight_stage(self, idx):
        for i, lbl in self._fixdata_names.items():
            try:
                lbl.config(font=("", 9, "bold") if i == idx else ("", 9))
            except Exception:  # noqa: BLE001
                pass

    def _fixdata_build_port_rows(self, ports):
        for ch in list(self._fixdata_ports_frame.winfo_children()):
            try:
                ch.destroy()
            except Exception:  # noqa: BLE001
                pass
        self._fixdata_port_vars = {}
        self._fixdata_port_states = {}
        for p in ports:
            var = tk.StringVar(value="idle")
            self._fixdata_port_vars[p] = var
            row = ttk.Frame(self._fixdata_ports_frame)
            row.pack(side=tk.TOP, fill=tk.X)
            ttk.Label(row, text=f"port {p}", width=10,
                      font=("Consolas", 9)).pack(side=tk.LEFT)
            ttk.Label(row, textvariable=var, foreground="#225",
                      font=("Consolas", 9)).pack(side=tk.LEFT)

    def _fixdata_pause_status_text(self) -> str:
        states = getattr(self, "_fixdata_port_states", {}) or {}
        total = max(len(getattr(self, "_fixdata_port_vars", {}) or {}),
                    len(states))
        idle = sum(1 for s in states.values() if str(s) in ("paused", "done"))
        if total:
            return f"Pausing: {idle}/{total} ports at a clean boundary"
        return "Pausing: waiting for a clean boundary"

    def _fixdata_update_pause_status(self) -> None:
        if getattr(self, "_fixdata_substate", None) == "pausing":
            try:
                self._fixdata_status.set(self._fixdata_pause_status_text())
            except Exception:  # noqa: BLE001
                pass

    def _fixdata_set_buttons(self, state):
        """state ∈ idle|running|pausing|paused|finalizing|done."""
        self._fixdata_substate = state
        try:
            self._fixdata_start_btn.config(
                state=tk.NORMAL if state in ("idle", "done") else tk.DISABLED)
            if state == "running":
                self._fixdata_pause_btn.config(state=tk.NORMAL, text="Pause")
            elif state == "pausing":
                self._fixdata_pause_btn.config(state=tk.NORMAL,
                                               text="Pausing...")
            elif state == "paused":
                self._fixdata_pause_btn.config(state=tk.NORMAL, text="Resume")
            else:
                self._fixdata_pause_btn.config(state=tk.DISABLED, text="Pause")
            # Cancel is GATED: only a safe exit once paused (or before/after a run)
            if state in ("idle", "done"):
                self._fixdata_cancel_btn.config(state=tk.NORMAL, text="Close")
            elif state == "paused":
                self._fixdata_cancel_btn.config(state=tk.NORMAL, text="Cancel")
            else:                                   # running / pausing / finalizing
                self._fixdata_cancel_btn.config(state=tk.DISABLED, text="Cancel")
        except Exception:  # noqa: BLE001
            pass

    def _fixdata_selected_kinds(self):
        """Freeze the UI's ordered kind selection on the GUI thread."""
        selected = []
        variables = getattr(self, "_fixdata_kind_vars", {}) or {}
        for kind, _label in self._FIXDATA_KIND_OPTIONS:
            try:
                enabled = bool(variables[kind].get())
            except Exception:  # noqa: BLE001 - missing/destroyed widget is off
                enabled = False
            if enabled:
                selected.append(kind)
        return tuple(selected)

    def _fixdata_start(self):
        kinds = self._fixdata_selected_kinds()
        if not kinds:
            messagebox.showinfo(
                "Repair data", "Select at least one data kind to fix.",
                parent=self._fixdata_win)
            return
        if self._storage_busy():
            self._fixdata_status.set("Busy — another storage task is running.")
            return
        ports = [p for p in (self._tws_ports() or ())]
        if not ports:
            messagebox.showinfo(
                "Repair data", "No live TWS port — start the fleet first.",
                parent=self._fixdata_win)
            return
        import queue
        import threading
        self._fixdata_pause.clear()
        self._fixdata_cancel.clear()
        self._fixdata_close_on_done = False
        self._fixdata_build_port_rows(ports)
        for idx in self._fixdata_bars:
            self._fixdata_bars[idx].config(value=0, maximum=100)
            self._fixdata_counts[idx].set("—")
        self._storage_fixdata_running = True
        self._fixdata_set_buttons("running")
        self._fixdata_status.set("Running — scanning the bank…")
        self._fixdata_highlight_stage(1)
        self._fixdata_log_clear()
        self._fixdata_log_add("Repair data started.")
        q = queue.Queue()
        self._fixdata_q = q
        self._fixdata_run_id = f"fixdata-{time.time_ns()}"
        root = self._storage_root
        addstock_debt_run_id = None
        addstock_xval_debt = set()
        addstock_gap_debt = set()
        try:
            debt_manifest = addstock_run_manifest.load_run(root)
            if debt_manifest is not None:
                addstock_debt_run_id = debt_manifest["run_id"]
                for ticker, record in debt_manifest["tickers"].items():
                    for interval, series in record["series"].items():
                        if stock_storage.session_of(interval) != "rth":
                            continue
                        key = (ticker, interval)
                        if not series["xval"]:
                            addstock_xval_debt.add(key)
                        if not series["gaps"]:
                            addstock_gap_debt.add(key)
        except addstock_run_manifest.ManifestError:
            pass
        try:
            deep_scan = bool(self._fixdata_deep_scan.get())
        except Exception:  # noqa: BLE001
            deep_scan = False
        try:
            restart_dead = bool(self._fixdata_restart_dead.get())
        except Exception:  # noqa: BLE001
            restart_dead = False
        pause_evt, cancel_evt = self._fixdata_pause, self._fixdata_cancel
        recovery_lock = threading.Lock()

        def _engine_run():
            try:
                def _adapter(port):
                    return stock_ibkr.LiveIB(
                        host=stock_ibkr.HOST_DEFAULT, ports=(port,),
                        client_id=stock_ibkr.CLIENT_ID_FETCH).connect()

                def _current_gap_debt(bank_root, candidates, result):
                    if (not candidates or not isinstance(result, dict)
                            or result.get("error") or not result.get("sidecar")):
                        return []
                    evidence = stock_validate.evaluate_gap_evidence(
                        bank_root, sorted(candidates))
                    return [(row["ticker"], row["interval"])
                            for row in evidence.get("rows") or []
                            if row.get("current") is True]

                def _scan(bank_root, **kwargs):
                    result = stock_validate.scan_all_gaps(bank_root, **kwargs)
                    source_absent_days = sum(
                        len(value.get("source_absent_list") or [])
                        for value in (result.get("summary") or {}).values())
                    if not deep_scan:
                        for value in (result.get("summary") or {}).values():
                            value["source_absent_list"] = []
                    q.put({
                        "run_id": self._fixdata_run_id, "seq": 0,
                        "type": "log",
                        "message": (
                            f"[scan] Deep scan {'includes' if deep_scan else 'off; skips'} "
                            f"{source_absent_days:,} ⊘ source-absent day(s)"),
                    })
                    result["verification_series"] = _current_gap_debt(
                        bank_root, addstock_gap_debt, result)
                    return result

                def _series(_bank_root):
                    return [(ticker, interval)
                            for ticker, intervals in self._storage_known_series()
                            for interval in intervals
                            if (stock_storage.kind_of(str(interval)) == ""
                                and stock_storage.session_of(
                                    str(interval)) == "rth"
                                and stock_storage.base_interval(
                                    str(interval)) != "1d")]

                def _refresh(bank_root, touched, **kwargs):
                    result = stock_validate.update_gap_report(
                        bank_root, touched,
                        asof=stock_ibkr.now_ny().isoformat(timespec="seconds"),
                        **kwargs)
                    candidates = addstock_gap_debt.intersection(
                        (str(ticker).upper(), str(interval))
                        for ticker, interval in touched)
                    result["verification_series"] = _current_gap_debt(
                        bank_root, candidates, result)
                    return result

                def _verification(stage, ticker, interval):
                    if not addstock_debt_run_id:
                        return
                    key = (str(ticker).upper(), str(interval))
                    debt = (addstock_xval_debt if stage == "xval"
                            else addstock_gap_debt)
                    if key in debt:
                        self._addstock_credit(
                            key[0], stage, [key[1]],
                            run_id=addstock_debt_run_id)

                def _probe_plan(bank_root, tickers, seed):
                    return live_spot_probe.build_embedded_plan(
                        bank_root, tickers, seed=seed, interval="1m")

                def _probe_report(bank_root, parent_run_id, seed, rows):
                    verdicts = {}
                    for row in rows:
                        verdict = str(row.get("verdict") or "PROBE_ERROR")
                        verdicts[verdict] = verdicts.get(verdict, 0) + 1
                    report = {
                        "kind": "fix_data_spot_probe_report",
                        "version": 1,
                        "parent_run_id": parent_run_id,
                        "seed": seed,
                        "report_only": True,
                        "probes": rows,
                        "verdict_counts": verdicts,
                        "request_count": sum(int(
                            row.get("request_count") or 0) for row in rows),
                        "reused_request_count": sum(int(
                            row.get("reused_request_count") or 0)
                            for row in rows),
                        "probe_issue_count": sum(
                            row.get("pass_equivalent") is not True
                            for row in rows),
                        "bank_written": False,
                        "written": False,
                        "artifact_written": False,
                    }
                    target = (Path(live_spot_probe.RUN_LOGS_ROOT)
                              / f"fix-data-spot-probe-{parent_run_id}.json")
                    live_spot_probe.write_artifact(
                        target, report, bank_root=bank_root,
                        run_logs_root=live_spot_probe.RUN_LOGS_ROOT)
                    return str(target.resolve())

                ratio_kinds = frozenset(
                    kind for kind in kinds
                    if kind in stock_storage.RATIO_KINDS)

                def _reconcile_plan(bank_root, *, ticker, kinds):
                    selected = frozenset(
                        kind for kind in kinds
                        if kind in stock_storage.RATIO_KINDS)
                    if not selected:
                        return []
                    scope = (None if ticker is None
                             else [str(ticker).strip().upper()])
                    # The unscoped call discovers candidate tickers before
                    # scheduling.  The scheduler calls this seam again with an
                    # exact ticker only after that ticker's fills; only this
                    # fresh queue slice is executable.  An incomplete focused
                    # audit must never fall back to a preserved stale row.
                    report = vol_value_audit.audit(
                        bank_root, tickers=scope, write_queue=True,
                        operation_mode="fetch")
                    if report.get("complete") is not True:
                        label = ("candidate discovery" if ticker is None
                                 else f"re-audit for {scope[0]}")
                        raise RuntimeError(
                            f"volatility {label} is incomplete")
                    return [
                        row for row in vol_value_reconcile.plan(
                            bank_root, tickers=scope)
                        if (stock_storage.kind_of(row["kind_token"])
                            in selected
                            and stock_storage.session_of(row["kind_token"])
                            == "rth")
                    ]

                def _reconcile_report(bank_root, parent_run_id, rows, *,
                                      requested_count, summary):
                    target = (
                        Path(live_spot_probe.RUN_LOGS_ROOT)
                        / ("fix-data-vol-value-reconcile-"
                           f"{parent_run_id}.json"))
                    return vol_value_reconcile.write_artifact(
                        target, rows, bank_root=bank_root,
                        run_logs_root=live_spot_probe.RUN_LOGS_ROOT,
                        run_id=parent_run_id,
                        requested_count=requested_count, summary=summary)

                port_recover_fn = None
                if restart_dead:
                    restart_ports, _port_up = self._fleet_callbacks(
                        lambda message: q.put(("log", str(message))),
                        ports, cancel=cancel_evt)
                    if restart_ports is None:
                        q.put(("log", "TWS restart recovery is unavailable; "
                               "failed ports will remain DEAD."))
                    else:
                        def _port_recover(port, reason):
                            q.put(("log", f"port {port}: connect retries "
                                   f"exhausted ({reason}); offering restart."))
                            # Screen-driving recovery is serialized even when
                            # several repair workers fail at the same time.
                            with recovery_lock:
                                outcomes = restart_ports(
                                    [int(port)], phase="midrun") or {}
                            if not isinstance(outcomes, dict):
                                return outcomes is True
                            outcome = outcomes.get(
                                int(port), outcomes.get(str(int(port))))
                            if type(outcome) is bool:
                                return outcome
                            return (isinstance(outcome, dict)
                                    and outcome.get("ok") is True)

                        port_recover_fn = _port_recover

                fix_data_pipeline.run(
                    root=root, ports=ports, adapter_factory=_adapter,
                    scan_fn=_scan, engine_tasks=True,
                    audit_fn=None, series_fn=_series,
                    refresh_fn=_refresh, progress=q.put,
                    pause_event=pause_evt, cancel_event=cancel_evt,
                    run_id=self._fixdata_run_id,
                    probe_plan_fn=_probe_plan,
                    probe_report_fn=_probe_report,
                    probe_seed=live_spot_probe.default_seed(),
                    verification_fn=_verification, kinds=kinds,
                    reconcile_plan_fn=(
                        _reconcile_plan if ratio_kinds else None),
                    reconcile_pacer_factory=(
                        stock_ibkr.Pacer if ratio_kinds else None),
                    reconcile_report_fn=(
                        _reconcile_report if ratio_kinds else None),
                    port_recover_fn=port_recover_fn)
            except Exception as exc:  # noqa: BLE001
                if getattr(exc, "fix_data_reported", False):
                    return
                q.put({"run_id": self._fixdata_run_id, "seq": 0,
                       "type": "done",
                       "result": {"error": f"worker init failed: {exc}"}})

        try:
            self._fixdata_thread = threading.Thread(
                target=_engine_run, daemon=True, name="fix-data-staged")
            self._fixdata_thread.start()
            self.root.after(250, self._fixdata_poll)
        except Exception as exc:  # noqa: BLE001
            self._storage_fixdata_running = False
            self._fixdata_set_buttons("idle")
            self._fixdata_status.set(f"Could not start: {exc}")

    def _fixdata_poll(self):
        q = getattr(self, "_fixdata_q", None)
        if q is None:
            return
        import queue
        win = getattr(self, "_fixdata_win", None)
        log_lines = []
        done_out = None
        drained = 0
        backlog = False
        try:
            while drained < 300:
                raw = q.get_nowait()
                if isinstance(raw, dict):
                    if raw.get("run_id") != getattr(
                            self, "_fixdata_run_id", None):
                        continue
                    kind = raw.get("type")
                    if kind == "stage":
                        payload = [raw.get("stage")]
                    elif kind == "scan":
                        payload = [raw.get("done", 0), raw.get("total", 0)]
                    elif kind in ("refetch", "reconcile", "xcheck", "probe"):
                        payload = [raw.get("done", 0), raw.get("total", 0),
                                   raw.get("metric", 0)]
                    elif kind == "ports":
                        payload = [raw.get("states") or {}]
                    elif kind == "substate":
                        payload = [raw.get("state")]
                    elif kind == "log":
                        payload = [raw.get("message", "")]
                    elif kind == "done":
                        payload = [raw.get("result") or {}]
                    elif kind == "blocked":
                        kind = "log"
                        payload = [raw.get("error", "Repair data is busy")]
                    else:
                        continue
                else:
                    kind, *payload = raw
                drained += 1
                if kind == "stage":
                    idx = payload[0]
                    if win is not None and win.winfo_exists():
                        self._fixdata_highlight_stage(idx)
                        self._fixdata_status.set(
                            {1: "Scanning the bank for gaps…",
                             2: "Repairing gaps and cross-checking accuracy…",
                             3: "Cross-checking accuracy…"}.get(idx, "Working…"))
                elif kind == "scan":
                    i, n = payload
                    self._fixdata_bars[1].config(maximum=max(n, 1), value=i)
                    self._fixdata_counts[1].set(f"{i}/{n} series")
                elif kind == "refetch":
                    done, total, added = payload
                    self._fixdata_bars[2].config(maximum=max(total, 1), value=done)
                    self._fixdata_counts[2].set(
                        f"{done}/{total} series · +{added:,} bars")
                elif kind == "reconcile":
                    done, total, unresolved = payload
                    if done or total:
                        self._fixdata_status.set(
                            "Repairing/checking - volatility refetch "
                            f"{done}/{total} - {unresolved} unresolved")
                elif kind == "xcheck":
                    done, total, flagged = payload
                    self._fixdata_bars[3].config(maximum=max(total, 1), value=done)
                    self._fixdata_counts[3].set(
                        f"{done}/{total} series · {flagged} ⚑")
                elif kind == "probe":
                    done, total, issues = payload
                    if done or total:
                        self._fixdata_status.set(
                            f"Repairing/checking · WS8 probes {done}/{total} "
                            f"· {issues} issue(s)")
                elif kind == "ports":
                    self._fixdata_port_states = dict(payload[0])
                    for p, s in payload[0].items():
                        var = self._fixdata_port_vars.get(p)
                        if var is not None:
                            var.set("▸ " + s)
                    self._fixdata_update_pause_status()
                elif kind == "substate":
                    self._fixdata_apply_substate(payload[0])
                elif kind == "log":
                    log_lines.append(payload[0])
                elif kind == "done":
                    self._fixdata_q = None
                    done_out = payload[0]
                    break
            if drained >= 300 and done_out is None:
                backlog = True
        except queue.Empty:
            pass
        except Exception:  # noqa: BLE001
            pass
        if log_lines:
            self._fixdata_log_add_many(log_lines)
        if done_out is not None:
            self._fixdata_done(done_out)
            return
        # safety net: a worker that died WITHOUT emitting ('done') would strand
        # the busy-gate and leak this timer forever. If the thread is gone and
        # no 'done' arrived (queue now drained), finalize as an error instead.
        th = getattr(self, "_fixdata_thread", None)
        if (th is not None and not th.is_alive()
                and getattr(self, "_fixdata_q", None) is q):
            self._fixdata_q = None
            self._fixdata_done({"error": "worker stopped unexpectedly",
                                "added": 0, "absent": 0, "checked": 0,
                                "flagged": 0, "still": 0, "cancelled": False,
                                "port_errors": [], "halted": 0})
            return
        try:
            self.root.after(10 if backlog else 250, self._fixdata_poll)
        except Exception:  # noqa: BLE001
            pass

    def _fixdata_apply_substate(self, s):
        # Ignore a STALE substate event: only honor 'paused' while a pause is
        # actually in effect, and 'running' only while it isn't — so a race near
        # a stage boundary (or a cancel) can't leave the buttons/substate stuck
        # in 'paused' with Cancel wrongly enabled against live work.
        try:
            paused_now = self._fixdata_pause.is_set()
        except Exception:  # noqa: BLE001
            paused_now = False
        if s == "paused":
            if not paused_now:
                return
            self._fixdata_set_buttons("paused")
            self._fixdata_status.set(
                "Paused ✓ — Resume to continue, or Cancel for a safe exit.")
        elif s == "running":
            if paused_now:
                return
            self._fixdata_set_buttons("running")
            self._fixdata_status.set("Running…")

    def _fixdata_pause_toggle(self):
        if not getattr(self, "_storage_fixdata_running", False):
            return
        if self._fixdata_pause.is_set():
            self._fixdata_pause.clear()                 # resume
            self._fixdata_set_buttons("running")
            self._fixdata_status.set("Resuming…")
        else:
            self._fixdata_pause.set()                    # request pause
            self._fixdata_set_buttons("pausing")
            self._fixdata_status.set(self._fixdata_pause_status_text())

    def _fixdata_cancel_click(self):
        """The bottom-right button. Before/after a run it's 'Close'; while PAUSED
        it's 'Cancel' (a safe finalize-then-stop). It is disabled while actively
        running, so a cancel only ever happens from a paused, safe boundary."""
        running = getattr(self, "_storage_fixdata_running", False)
        if not running:
            self._fixdata_destroy()
            return
        if self._fixdata_substate == "paused":
            self._fixdata_cancel.set()
            self._fixdata_pause.clear()      # let workers wake to see the cancel
            self._fixdata_set_buttons("finalizing")
            self._fixdata_status.set("Cancelling — finalizing the gap report…")

    def _fixdata_on_close(self):
        """Window-X. Gated like the fetch dialogs: a clean exit needs a paused
        (or finished) run. While actively running, ask the user to pause first."""
        if not getattr(self, "_storage_fixdata_running", False):
            self._fixdata_destroy()
            return
        if self._fixdata_substate == "paused":
            self._fixdata_close_on_done = True
            self._fixdata_cancel.set()
            self._fixdata_pause.clear()
            self._fixdata_set_buttons("finalizing")
            self._fixdata_status.set("Cancelling — finalizing, then closing…")
            return
        messagebox.showinfo(
            "Repair data", "A run is in progress. Press Pause first; once it shows "
            "'Paused ✓' you can Cancel to exit safely.", parent=self._fixdata_win)

    def _fixdata_done(self, out):
        self._storage_fixdata_running = False
        self._fixdata_highlight_stage(0)
        self._fixdata_set_buttons("done")
        added = int(out.get("added") or 0)
        absent = int(out.get("absent") or 0)
        checked = int(out.get("checked") or 0)
        flagged = int(out.get("flagged") or 0)
        still = int(out.get("still") or 0)
        halted = int(out.get("halted") or 0)
        cov_forward = int(out.get("coverage_forward") or 0)
        cov_front = int(out.get("coverage_front") or 0)
        cov_unknown = int(out.get("coverage_unknown") or 0)
        health_identity = int(out.get("health_identity") or 0)
        health_frontier = int(out.get("health_frontier") or 0)
        health_calendar = int(out.get("health_calendar") or 0)
        health_triage = int(out.get("health_triage") or 0)
        health_split_repair = int(out.get("health_split_repair") or 0)
        health_split_confirm = int(out.get("health_split_confirm") or 0)
        health_split_operational = int(
            out.get("health_split_operational") or 0)
        probe_completed = int(out.get("probe_completed") or 0)
        probe_issues = int(out.get("probe_issues") or 0)
        probe_requests = int(out.get("probe_request_count") or 0)
        reconcile_completed = int(out.get("reconcile_completed") or 0)
        reconcile_candidates = int(
            out.get("reconcile_candidate_tickers") or 0)
        reconcile_planned = int(out.get("reconcile_planned_tickers") or 0)
        reconcile_plan_failed = int(out.get("reconcile_plan_failed") or 0)
        reconcile_selection_unknown = int(
            out.get("reconcile_selection_unknown") or 0)
        reconcile_requests = int(out.get("reconcile_request_count") or 0)
        reconcile_request_unknown = int(
            out.get("reconcile_request_unknown") or 0)
        reconcile_settled = int(out.get("reconcile_settled") or 0)
        reconcile_corrected = int(out.get("reconcile_corrected") or 0)
        reconcile_unresolved = int(out.get("reconcile_unresolved") or 0)
        reconcile_queue_pending = int(
            out.get("reconcile_queue_pending") or 0)
        health_split_actions = list(out.get("health_split_actions") or [])
        cov_queue = list(out.get("coverage_queue") or [])
        health_queue = list(out.get("health_queue") or cov_queue)
        port_errors = list(out.get("port_errors") or [])
        if out.get("error"):
            head = f"Repair data error: {out['error']}"
        elif out.get("cancelled"):
            head = (f"Cancelled — partial: +{added:,} bar(s) filled, "
                    f"{absent:,} source-absent, {halted:,} halted/retry, "
                    f"{checked} checked, {flagged} ⚑.")
        else:
            head = (f"Done — gaps: +{added:,} bar(s) filled, "
                    f"{still:,} still missing, {absent:,} source-absent (⊘), "
                    f"{halted:,} halted/retry. Accuracy: {checked} checked, "
                    f"{flagged} flagged (⚑).")
        if cov_forward or cov_front:
            head += (f" Coverage: {cov_forward} stale, "
                     f"{cov_front} front-short.")
        if health_identity or health_frontier or health_calendar or health_triage:
            head += (f" Health: {health_identity} identity, "
                     f"{health_frontier} frontier, "
                     f"{health_calendar} calendar, "
                     f"{health_triage} triage.")
        head += (f" Splits: {health_split_repair} repair, "
                 f"{health_split_confirm} confirm, "
                 f"{health_split_operational} operational.")
        head += (f" WS8: {probe_completed} probed, {probe_requests} request(s), "
                 f"{probe_issues} issue(s).")
        if (reconcile_completed or reconcile_candidates
                or out.get("reconcile_error")):
            head += (f" Volatility: {reconcile_completed} refetched, "
                     f"{reconcile_settled} settled, "
                      f"{reconcile_corrected} corrected, "
                      f"{reconcile_unresolved} unresolved, "
                      f"{reconcile_queue_pending} queue row(s) pending"
                      + (f", {reconcile_request_unknown} request state(s) "
                         "unknown" if reconcile_request_unknown else "")
                      + (f", {reconcile_selection_unknown} ticker selection(s) "
                         "unknown" if reconcile_selection_unknown else "")
                      + ".")
        log_lines = []
        if port_errors:
            head += (f"  ⚠ {len(port_errors)} port(s) failed.")
            log_lines.extend("  " + pe for pe in port_errors)
        if health_queue:
            log_lines.append("Health repair queue: " + ", ".join(health_queue))
        self._fixdata_status.set(head)
        log_lines.append(head)
        self._fixdata_log_add_many(log_lines)
        try:
            self._storage_rescan()           # repaint the main table (Gaps/⚑/⊘)
        except Exception:  # noqa: BLE001
            pass
        try:
            body = (f"Gaps filled: +{added:,} bar(s)\n"
                    f"Source-absent days: {absent:,}\n"
                    f"Halted/retry days or months: {halted:,}\n"
                    f"Still missing after refresh: {still:,}\n"
                    f"Accuracy checked: {checked:,}\n"
                    f"Accuracy flagged: {flagged:,}")
            body += (f"\nCoverage forward-stale: {cov_forward:,}\n"
                     f"Coverage front-short: {cov_front:,}\n"
                     f"Coverage unknown earliest: {cov_unknown:,}")
            body += (f"\nIdentity issues: {health_identity:,}\n"
                     f"Frozen frontiers: {health_frontier:,}\n"
                     f"Calendar missing closures: {health_calendar:,}\n"
                     f"Triage needs-human: {health_triage:,}")
            body += (f"\nSplit repair rows: {health_split_repair:,}\n"
                     f"Split needs-confirmation: {health_split_confirm:,}\n"
                     f"Split operational warnings: "
                     f"{health_split_operational:,}")
            body += (f"\nWS8 probes completed: {probe_completed:,}\n"
                     f"WS8 requests: {probe_requests:,}\n"
                     f"WS8 probe issues: {probe_issues:,}")
            body += (f"\nVolatility days refetched: {reconcile_completed:,}\n"
                      f"Volatility candidate tickers: "
                      f"{reconcile_candidates:,}\n"
                      f"Volatility tickers freshly replanned: "
                      f"{reconcile_planned:,}\n"
                      f"Volatility replan failures: "
                      f"{reconcile_plan_failed:,}\n"
                      f"Volatility ticker selections unknown: "
                      f"{reconcile_selection_unknown:,}\n"
                      f"Volatility requests: {reconcile_requests:,}\n"
                      f"Volatility request states unknown: "
                      f"{reconcile_request_unknown:,}\n"
                      f"Volatility settled: {reconcile_settled:,}\n"
                     f"Volatility corrected: {reconcile_corrected:,}\n"
                     f"Volatility unresolved: {reconcile_unresolved:,}\n"
                     f"Volatility queue rows pending: "
                     f"{reconcile_queue_pending:,}")
            if out.get("probe_report"):
                body += f"\nWS8 report: {out['probe_report']}"
            if out.get("reconcile_report"):
                body += ("\nVolatility reconciliation report: "
                         f"{out['reconcile_report']}")
            if out.get("reconcile_error"):
                body += ("\nVolatility reconciliation report error: "
                         f"{out['reconcile_error']}")
            if health_split_actions:
                body += "\nSplit actions:\n" + "\n".join(
                    health_split_actions)
            if health_queue:
                body += "\nHealth repair queue: " + ", ".join(health_queue)
            if out.get("coverage_report"):
                body += f"\nCoverage report: {out['coverage_report']}"
            if out.get("health_report"):
                body += f"\nHealth report: {out['health_report']}"
            if out.get("split_report"):
                body += f"\nSplit report: {out['split_report']}"
            if port_errors:
                shown = "\n".join(port_errors[:5])
                more = "" if len(port_errors) <= 5 else (
                    f"\n... {len(port_errors) - 5} more")
                body += f"\n\nPort errors:\n{shown}{more}"
            parent = getattr(self, "_fixdata_win", None) or self.root
            if out.get("error"):
                self._show_unattended_notice(
                    "Repair data error", body, level="error", parent=parent)
            elif (out.get("cancelled") or flagged or halted or still
                  or cov_forward or cov_front or cov_unknown
                  or health_identity or health_frontier or health_calendar
                  or health_triage or health_split_repair
                   or probe_issues or reconcile_unresolved
                   or reconcile_queue_pending
                   or reconcile_request_unknown
                   or reconcile_plan_failed
                   or reconcile_selection_unknown
                  or out.get("reconcile_error")
                  or port_errors):
                self._show_unattended_notice(
                    "Repair data finished with issues", body, level="warning",
                    parent=parent)
            else:
                self._show_unattended_notice(
                    "Repair data complete", body, parent=parent)
        except Exception:  # noqa: BLE001
            pass
        if getattr(self, "_fixdata_close_on_done", False):
            self._fixdata_destroy()

    def _fixdata_destroy(self):
        win = getattr(self, "_fixdata_win", None)
        self._fixdata_win = None
        self._fixdata_q = None
        if win is not None:
            try:
                win.destroy()
            except Exception:  # noqa: BLE001
                pass

    def _gap_incremental_begin(self) -> None:
        import queue
        import threading
        addstock_run_id = getattr(self, "_addstock_run_id", None)
        self._gap_inc_q = queue.Queue()
        self._gap_inc_seen = set()
        self._gap_inc_lock = threading.Lock()

        def _work():
            import datetime as _dt
            q = self._gap_inc_q
            root = self._storage_root
            while True:
                item = q.get()
                if item is None:
                    return
                t, iv = item
                try:
                    cal = getattr(self, "_gap_cal_cache", None)
                    if cal is None:
                        cal = stock_validate.consensus_calendar(root)
                        self._gap_cal_cache = cal
                    res = stock_validate.update_gap_report(
                        root, [(t, iv)], calendar_days=cal,
                        asof=_dt.datetime.now().isoformat(timespec="seconds"))
                    if not res.get("error") and res.get("sidecar"):
                        current = self._addstock_current_gap_intervals(t, [iv])
                        if current:
                            self._addstock_credit(
                                t, "gaps", current,
                                run_id=addstock_run_id)
                    msg = (f"gap scan {t} {iv}: "
                           f"{int(res.get('missing_run') or 0):,} missing bar(s), "
                           f"{int(res.get('missing_days_run') or 0):,} "
                           f"missing day(s)")
                except Exception as exc:  # noqa: BLE001
                    msg = f"gap scan {t} {iv}: skipped ({exc})"
                try:
                    self.root.after(
                        0, lambda m=msg: (
                            self._storage_find_say(m),
                            self._storage_refresh_gaps_col()))
                except Exception:  # noqa: BLE001
                    pass

        try:
            th = threading.Thread(target=_work, daemon=True,
                                  name="gap-scan-incremental")
            self._gap_inc_thread = th
            th.start()
        except Exception:  # noqa: BLE001
            self._gap_inc_q = None

    def _gap_incremental_queue(self, ticker, interval) -> None:
        if getattr(self, "_storage_find_phase", None) != "run":
            return
        try:
            if stock_storage.session_of(str(interval)) != "rth":
                return
        except Exception:  # noqa: BLE001
            return
        if self._addstock_interval_empty_on_disk(ticker, interval):
            return
        q = getattr(self, "_gap_inc_q", None)
        lock = getattr(self, "_gap_inc_lock", None)
        if q is None or lock is None:
            return
        key = (str(ticker).strip().upper(), str(interval))
        with lock:
            seen = getattr(self, "_gap_inc_seen", set())
            if key in seen:
                return
            seen.add(key)
            self._gap_inc_seen = seen
        try:
            q.put(key)
        except Exception:  # noqa: BLE001
            pass

    def _gap_incremental_stop(self) -> None:
        q = getattr(self, "_gap_inc_q", None)
        self._gap_inc_q = None
        self._gap_inc_seen = set()
        self._gap_inc_lock = None
        if q is not None:
            try:
                q.put(None)
            except Exception:  # noqa: BLE001
                pass

    def _gap_scan_after_fetch(self, rep, say=None, *, skip_empty=False) -> None:
        """Automatically scan the JUST-FETCHED intraday regular-session series for
        INTERIOR gaps (missing minutes between two stored bars), merge them into
        the time-ordered `data_gaps.parquet` at the outermost folder + the
        `_data_gaps.json` sidecar, then refresh the 'Gaps' column. Runs on a daemon
        worker so it never blocks the GUI; best-effort — a gap-scan failure never
        disturbs the finished fetch."""
        try:
            series = sorted({(r.get("ticker"), r.get("interval"))
                             for r in (rep or {}).get("series", [])
                             if r.get("ticker") and r.get("interval")
                             and stock_storage.session_of(
                                 str(r.get("interval"))) == "rth"})
        except Exception:  # noqa: BLE001
            series = []
        if skip_empty:
            series = [
                (ticker, interval) for ticker, interval in series
                if not self._addstock_interval_empty_on_disk(ticker, interval)
            ]
        if not series:
            return
        import datetime as _dt
        import queue
        import threading
        asof = _dt.datetime.now().isoformat(timespec="seconds")
        q = queue.Queue()
        root = self._storage_root
        say = say or self._storage_ibkr_say

        def _work():
            try:
                res = stock_validate.update_gap_report(root, series, asof=asof)
            except Exception as exc:  # noqa: BLE001
                res = exc
            q.put(res)

        def _poll():
            try:
                res = q.get_nowait()
            except queue.Empty:
                try:
                    self.root.after(200, _poll)
                except Exception:  # noqa: BLE001 — window closing
                    pass
                return
            self._gap_scan_report(res, say=say)
            if isinstance(res, dict) and not res.get("error") \
                    and res.get("sidecar"):
                credited = {}
                for ticker, interval in series:
                    credited.setdefault(ticker, []).append(interval)
                for ticker, intervals in credited.items():
                    current = self._addstock_current_gap_intervals(
                        ticker, intervals)
                    if current:
                        self._addstock_credit(ticker, "gaps", current)

        try:
            threading.Thread(target=_work, daemon=True, name="gap-scan").start()
            self.root.after(200, _poll)
        except Exception:  # noqa: BLE001 — thread exhaustion: skip silently
            pass

    def _gap_scan_report(self, res, say=None) -> None:
        """GUI thread: announce the post-fetch gap scan and repaint the Gaps
        column (deferred until any in-flight tree scan finishes)."""
        say = say or self._storage_ibkr_say
        if isinstance(res, Exception):
            say(f"(gap scan skipped: {res})")
        elif res.get("error"):
            say(f"(gap scan: {res['error']})")
        elif res.get("missing_run"):
            worst = ", ".join(
                f"{k} ({v:,})"
                for k, v in list(res.get("by_series", {}).items())[:6])
            say(
                f"⚠ data gaps: {res['missing_run']:,} missing intraday bar(s) "
                f"in the fetched series ({res.get('gap_series', 0)} series) — "
                f"recorded time-ordered in {stock_validate.GAP_REPORT_NAME} "
                f"(may include no-trade minutes on thin names). Worst: {worst}")
        else:
            say(
                "✓ no interior gaps in the fetched series (every stored minute "
                "is contiguous within its session).")

        def _refresh():
            if self._storage_busy():           # wait out the post-fetch rescan
                try:
                    self.root.after(300, _refresh)
                except Exception:  # noqa: BLE001
                    pass
                return
            try:
                # Repaint the Gaps column from the just-written sidecar — this
                # used to kick a SECOND full bank walk (~9 s) right behind the
                # post-fetch rescan, just to refresh one column.
                self._storage_refresh_gaps_col()
            except Exception:  # noqa: BLE001
                pass
        _refresh()

    # ---- Multi-port TWS demo fleet: start up + restart dead ports --------

    def _storage_fleet_popup(self, title, intro, job_fn, on_done=None):
        """Open a centered progress popup and run job_fn(say) on a worker
        thread, draining its progress through a queue (the worker NEVER touches
        tkinter — it only calls `say`). job_fn formats its own result via `say`
        and returns when done. Shared by 'Start up multi-port' and 'Restart
        dead ports'; each call owns its window + queue, so the two are
        independent and can't clobber each other's state.

        `on_done(kind, payload, win, append)` (optional) fires on the GUI
        thread once the job ends. `win` and `append` are None if the user
        closed the progress window, so the callback can choose one surviving
        completion surface without losing background-run notification."""
        import queue
        import threading
        win = tk.Toplevel(self.root)
        win.title(title)
        win.transient(self.root)
        ttk.Label(win, justify=tk.LEFT, wraplength=540, text=intro).pack(
            anchor=tk.W, padx=10, pady=(10, 6))
        body = ttk.Frame(win)
        body.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        txt = tk.Text(body, height=16, width=76, wrap="word",
                      state=tk.DISABLED)
        vs = ttk.Scrollbar(body, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=vs.set)
        txt.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vs.pack(side=tk.LEFT, fill=tk.Y)
        win.update_idletasks()                          # centre on screen
        w, h = win.winfo_width(), win.winfo_height()
        win.geometry(f"+{max(0, (win.winfo_screenwidth() - w) // 2)}"
                     f"+{max(0, (win.winfo_screenheight() - h) // 3)}")

        q = queue.Queue()
        state = {"running": True}

        def append(line):
            try:
                txt.config(state=tk.NORMAL)
                txt.insert(tk.END, str(line).rstrip("\n") + "\n")
                txt.see(tk.END)
                txt.config(state=tk.DISABLED)
            except Exception:  # noqa: BLE001 — window closing
                pass

        def on_close():
            if state["running"] and not messagebox.askyesno(
                    title, "This is still running. Close the window anyway? "
                    "It keeps going in the background.", parent=win):
                return
            try:
                win.destroy()
            except Exception:  # noqa: BLE001
                pass

        win.protocol("WM_DELETE_WINDOW", on_close)

        def worker():
            try:
                result = job_fn(lambda m: q.put(("line", m)))
                q.put(("done", result))             # capture the job's summary
            except Exception as exc:  # noqa: BLE001 — surfaced in the log
                q.put(("error", repr(exc)))

        def poll():
            final = None                            # ('done'|'error', payload)
            try:
                while True:
                    kind, payload = q.get_nowait()
                    if kind == "line":
                        append(payload)
                    else:
                        final = (kind, payload)
                        break
            except queue.Empty:
                pass
            if final is None:
                # keep draining until the job ends — NOT gated on the window
                # existing, so the completion popup still fires if the user
                # closed the log window to background the run.
                self.root.after(150, poll)
                return
            state["running"] = False
            kind, payload = final
            if on_done is not None:
                try:
                    try:
                        window_open = bool(win.winfo_exists())
                    except Exception:  # noqa: BLE001 - destroyed window
                        window_open = False
                    on_done(kind, payload,
                            win if window_open else None,
                            append if window_open else None)
                except Exception:  # noqa: BLE001 — a popup must never crash poll
                    pass
            else:
                append("\nDone — you can close this window."
                       if kind == "done" else f"\nFAILED: {payload}")

        threading.Thread(target=worker, daemon=True).start()
        self.root.after(120, poll)

    def _raise_fleet_completion(self, win):
        """Briefly lift a non-modal fleet completion surface above TWS."""
        if win is None:
            return
        try:
            win.deiconify()
            win.lift()
            win.attributes("-topmost", True)

            def drop_topmost():
                try:
                    win.attributes("-topmost", False)
                except Exception:  # noqa: BLE001 - window may be closed
                    pass

            win.after(400, drop_topmost)
            win.focus_force()
        except Exception:  # noqa: BLE001 - completion remains recorded
            pass

    def _fleet_summary_box(self, title, summary, parent=None, error=None,
                           embed=None):
        """Non-modal completion notice shared by every fleet action (start-up,
        'Restart dead ports', auto-restart-on-disconnect). Call on the GUI
        thread. `summary` is {up: [ports], total: int, problems: {port: reason},
        aborted: bool, nothing: bool}."""
        if error is not None:
            notice_title, body, level = title, str(error), "error"
        else:
            summary = summary or {}
            up = summary.get("up", [])
            total = summary.get("total", len(up))
            problems = summary.get("problems", {})
            aborted = summary.get("aborted", False)
            if summary.get("nothing"):
                notice_title = f"{title} — done"
                body = (f"All {total} port(s) already up — "
                        "nothing to restart.")
                level = "info"
            elif up and len(up) == total and not problems and not aborted:
                notice_title = f"{title} — done"
                body = (f"{len(up)}/{total} port(s) up and bound:\n"
                        f"{', '.join(map(str, up))}.\n\nReady to fetch.")
                level = "info"
            else:
                lines = [f"{len(up)}/{total} port(s) up"
                         + (" — aborted by ESC" if aborted else "") + "."]
                if up:
                    lines.append("Up: " + ", ".join(map(str, up)))
                if problems:
                    lines.append("\nProblems:")
                    lines += [f"  • port {p}: {r}"
                              for p, r in problems.items()]
                notice_title = f"{title} — finished with problems"
                body, level = "\n".join(lines), "warning"

        try:
            parent_open = parent is not None and parent.winfo_exists()
        except Exception:  # noqa: BLE001 - destroyed progress window
            parent_open = False
        if parent_open and callable(embed):
            embed(f"\n{notice_title}\n{body}\n\n"
                  "Done — you can close this window.")
            self._raise_fleet_completion(parent)
            return parent

        notice = self._show_unattended_notice(
            notice_title, body, level=level, parent=self.root)
        self._raise_fleet_completion(notice)
        return notice

    def _storage_multiport_choose_count(self, memory_cap_result=None):
        """Return the user-confirmed first-N fleet size, or ``None`` on Cancel."""
        cap = len(self._STANDARD_FLEET)
        measured_free = _available_ram_bytes()
        working_rows = _tws_working_set_rows()
        working_sets = fleet_sizing.tws_working_set_bytes(working_rows)
        sampled = fleet_sizing.sample_per_instance_bytes(working_rows)
        cap_verified = bool(
            memory_cap_result is not None
            and getattr(memory_cap_result, "verified", False))
        reclaimable = sum(working_sets)
        if cap_verified:
            free_bytes = (fleet_sizing.effective_free_bytes(
                measured_free, working_sets)
                if measured_free is not None else None)
            per_instance = fleet_sizing.CAPPED_PER_INSTANCE_BYTES
            reserve_frac = fleet_sizing.DEFAULT_RESERVE_FRAC
        else:
            # A failed/unverified installer keeps the exact conservative path:
            # measured free only, live median/fallback basis, 25% reserve.
            free_bytes = measured_free
            per_instance = (sampled if sampled is not None
                            else fleet_sizing.DEFAULT_PER_INSTANCE_BYTES)
            reserve_frac = fleet_sizing.UNCAPPED_RESERVE_FRAC
        auto_count = fleet_sizing.auto_select(
            free_bytes, per_instance, cap=cap,
            reserve_frac=reserve_frac) if free_bytes is not None else 0
        initial_count = auto_count if auto_count > 0 else 1
        result = {"count": None}

        top = tk.Toplevel(self.root)
        top.title("Start up multi-port")
        top.transient(self.root)
        top.resizable(False, False)
        outer = ttk.Frame(top, padding=14)
        outer.pack(fill=tk.BOTH, expand=True)
        ttk.Label(
            outer, text="Choose how many TWS demo instances to start",
            font=("TkDefaultFont", 11, "bold")).pack(anchor=tk.W)
        ttk.Label(
            outer, justify=tk.LEFT, wraplength=560,
            text=(f"AUTO keeps {int(reserve_frac * 100)}% of "
                  f"{'effective' if cap_verified else 'currently free'} "
                  "physical RAM in reserve. You can choose a different "
                  "count; Launch asks again if it exceeds the AUTO "
                  "recommendation.")).pack(
                     anchor=tk.W, pady=(3, 10))

        chooser = ttk.Frame(outer)
        chooser.pack(fill=tk.X)
        ttk.Label(chooser, text="Port count:").pack(side=tk.LEFT)
        count_var = tk.StringVar(master=top, value=str(initial_count))
        spinner = ttk.Spinbox(
            chooser, from_=1, to=cap, width=4, textvariable=count_var,
            state="readonly")
        spinner.pack(side=tk.LEFT, padx=(6, 12))
        auto_text = (f"AUTO: {auto_count} port(s)"
                     if auto_count > 0 else
                     "AUTO: 0 ports fit the reserve")
        ttk.Label(chooser, text=auto_text, foreground="#245f9e").pack(
            side=tk.LEFT)

        estimate_var = tk.StringVar(master=top)
        ttk.Label(
            outer, textvariable=estimate_var,
            font=("TkDefaultFont", 10, "bold")).pack(
                anchor=tk.W, pady=(10, 2))
        if cap_verified:
            source_text = (
                f"Per-instance source: verified {tws_vmoptions.HEAP_CAP_OPTION} "
                f"cap with a {fleet_sizing.CAPPED_PER_INSTANCE_BYTES / fleet_sizing.GIB:.2f} "
                "GiB planning basis (heap plus native/runtime overhead).")
        elif sampled is not None:
            source_text = (
                "Per-instance source: live median of running tws process "
                "working sets; memory cap not verified.")
        else:
            source_text = (
                "Per-instance source: documented 1.6 GiB fallback; memory "
                "cap not verified.")
        ttk.Label(outer, text=source_text, foreground="#555").pack(
            anchor=tk.W)
        if cap_verified and measured_free is not None:
            ttk.Label(
                outer, foreground="#555", wraplength=560,
                text=(
                    f"Effective free RAM: {free_bytes / fleet_sizing.GIB:.1f} "
                    f"GiB = {measured_free / fleet_sizing.GIB:.1f} GiB "
                    f"currently free + {reclaimable / fleet_sizing.GIB:.1f} "
                    "GiB from TWS instances this launch will close.")).pack(
                        anchor=tk.W, pady=(2, 0))
        if free_bytes is None:
            ttk.Label(
                outer, foreground="#a06000",
                text="Available RAM could not be measured; AUTO cannot "
                     "certify one port, so Launch requires an explicit "
                     "override.").pack(
                         anchor=tk.W, pady=(3, 0))

        ttk.Separator(outer).pack(fill=tk.X, pady=10)
        ttk.Label(
            outer, justify=tk.LEFT, wraplength=560,
            text="Launch closes any TWS already running, logs each selected "
                 "demo instance in, then opens Global Config → API and binds "
                 "its port. It drives the screen for about 1–2 minutes per "
                 "instance, so do not start it during a data fetch.").pack(
                     anchor=tk.W)
        ttk.Label(
            outer, justify=tk.LEFT, wraplength=560, foreground="#a06000",
            text=fleet_sizing.OVER_AUTO_WARNING).pack(anchor=tk.W, pady=(7, 0))

        def selected_count():
            try:
                value = int(count_var.get())
            except (TypeError, ValueError, tk.TclError):
                return None
            return value if 1 <= value <= cap else None

        def refresh_estimate(*_args):
            count = selected_count()
            estimate_var.set(
                fleet_sizing.format_estimate(
                    count or 0, per_instance, free_bytes,
                    effective=cap_verified))

        def cancel():
            top.destroy()

        def launch():
            count = selected_count()
            if count is None:
                return
            if count > auto_count and not messagebox.askyesno(
                    "Exceeds AUTO memory budget",
                    f"AUTO recommends {auto_count} port(s) with a "
                    f"{int(reserve_frac * 100)}% RAM reserve.\n\n"
                    f"{fleet_sizing.OVER_AUTO_WARNING}\n\n"
                    f"Start {count} port(s) anyway?",
                    parent=top):
                return
            result["count"] = count
            top.destroy()

        count_var.trace_add("write", refresh_estimate)
        refresh_estimate()
        buttons = ttk.Frame(outer)
        buttons.pack(fill=tk.X, pady=(12, 0))
        ttk.Button(buttons, text="Cancel", command=cancel).pack(side=tk.RIGHT)
        launch_btn = ttk.Button(buttons, text="Launch", command=launch)
        launch_btn.pack(side=tk.RIGHT, padx=(0, 7))
        top.protocol("WM_DELETE_WINDOW", cancel)
        top.update_idletasks()
        x = self.root.winfo_rootx() + max(
            0, (self.root.winfo_width() - top.winfo_width()) // 2)
        y = self.root.winfo_rooty() + max(
            0, (self.root.winfo_height() - top.winfo_height()) // 2)
        top.geometry(f"+{x}+{y}")
        top.grab_set()
        launch_btn.focus_set()
        self.root.wait_window(top)
        return result["count"]

    def _storage_multiport_start(self) -> None:
        """Launch and configure the selected first N standard demo instances."""
        if self._storage_busy():
            return
        tws_executable = self._storage_resolve_tws_app()
        if tws_executable is None:
            return
        memory_cap_result = tws_vmoptions.ensure_memory_cap(tws_executable)
        if not memory_cap_result.verified:
            self._storage_status.set(
                "TWS memory cap was not verified; Start Multi Port will use "
                "the conservative AUTO policy.")
        count = self._storage_multiport_choose_count(memory_cap_result)
        if count is None:
            return
        ports = list(self._STANDARD_FLEET[:count])
        n = len(ports)
        emails = [f"{chr(ord('a') + i)}@gmail.com" for i in range(n)]
        ports_str = ", ".join(map(str, ports))
        step_verify = os.environ.get("EMA_MULTIPORT_STEP_VERIFY") == "1"

        # adopt the fleet as the active ports (GUI thread) so the later parallel
        # fetch, 'Restart dead ports' and health checks see the selected first N.
        self._storage_tws_ports_list = [str(p) for p in ports]
        try:
            self._storage_tws_update_ports_btn()
        except Exception:  # noqa: BLE001 — button may be hidden (Series mode)
            pass

        def _job_without_maintenance(say):
            import tws_launch
            recorder = tws_launch.bringup.FleetTranscript(
                "fleet_bringup", step_verification=step_verify,
                expected_ports=ports)
            say(f"starting {n} instance(s) — closing any running TWS first…")
            try:
                res = tws_launch.launch_many(
                    emails, on_progress=say, recorder=recorder, ports=ports,
                    memory_cap_result=memory_cap_result)
            except Exception:
                recorder.write(on_progress=say)
                raise
            launched = [e for e, s in res.items() if s == "launched"]
            say(f"\n{len(launched)}/{len(res)} instance(s) launched.")
            problems = {}                        # port -> reason, for the popup
            for e, s in res.items():
                if s != "launched":
                    say(f"  {e}: {s}")
                    problems[ports[emails.index(e)]] = f"launch: {s}"
            if step_verify and len(launched) != n:
                say("STEP VERIFICATION HALTED at the first launch/login "
                    "failure; later steps were not attempted.")
                transcript = recorder.write(
                    {"first_pass": n, "succeeded": len(launched),
                     "failed": n - len(launched)}, on_progress=say)
                say(f"step evidence: {transcript or recorder.run_dir}")
                return {
                    "up": [], "total": n, "problems": problems,
                    "run_dir": str(recorder.run_dir),
                    "verification_complete": False,
                }
            if not launched:
                say("no instances launched — nothing to enable.")
                recorder.write({"first_pass": n, "failed": n},
                               on_progress=say)
                return {"up": [], "total": n, "problems": problems}
            # pair each launched instance with its intended port (a@→ports[0],
            # b@→ports[1], …) and enable + bind it. enable_fleet matches each
            # instance by its config dir, so the DUxxx order doesn't matter.
            pairs = [(e, p) for e, p in zip(emails, ports)
                     if res.get(e) == "launched"]
            say(f"\nenabling API + binding port(s) for {len(pairs)} "
                f"instance(s) — this opens each instance's config dialog…")
            try:
                en = tws_launch.enable_fleet(
                    pairs, on_progress=say, recorder=recorder)
            except Exception:
                recorder.write(on_progress=say)
                raise

            def _ok(s):
                return s == "up" or (isinstance(s, dict) and s.get("ok"))
            up = sorted(p for p, s in en.items() if _ok(s))
            say(f"\n{len(up)}/{len(en)} port(s) enabled + listening"
                + (f": {', '.join(map(str, up))}" if up else ""))
            for p, s in en.items():
                if not _ok(s):
                    say(f"  port {p}: {s}")
                    problems[p] = str(s)
            if step_verify and len(up) != n:
                say("STEP VERIFICATION HALTED at the first API/bind/listener "
                    "failure; later steps were not attempted.")
                transcript = recorder.write(
                    {"first_pass": n, "succeeded": len(up),
                     "failed": n - len(up)}, on_progress=say)
                say(f"step evidence: {transcript or recorder.run_dir}")
                return {
                    "up": up, "total": n, "problems": problems,
                    "run_dir": str(recorder.run_dir),
                    "verification_complete": False,
                }
            if step_verify:
                say("\nverifying a real API handshake and exact account "
                    "match on every listener...")
                handshakes = []
                for p in ports:
                    state = en.get(p)
                    expected_account = (
                        state.get("account") if isinstance(state, dict)
                        else None)
                    try:
                        tws_launch.verify_handshake(
                            p, expected_account, recorder=recorder,
                            on_progress=say)
                        handshakes.append(p)
                    except Exception as exc:  # noqa: BLE001 - halt exact step
                        problems[p] = (
                            f"handshake: {tws_launch.bringup.failure_code(exc)}")
                        say(f"STEP VERIFICATION HALTED at port {p} handshake; "
                            "later handshakes were not attempted.")
                        transcript = recorder.write(
                            {"first_pass": n,
                             "succeeded": len(handshakes),
                             "failed": n - len(handshakes)},
                            on_progress=say)
                        say(f"step evidence: {transcript or recorder.run_dir}")
                        return {
                            "up": up, "total": n, "problems": problems,
                            "run_dir": str(recorder.run_dir),
                            "verification_complete": False,
                        }
            if len(up) == n:
                say("Fleet up and every port bound — ready to fetch.")
            transcript = recorder.write(
                {"first_pass": n, "succeeded": len(up),
                 "failed": n - len(up)}, on_progress=say)
            say(f"step evidence: {transcript or recorder.run_dir}")
            return {
                "up": up, "total": n, "problems": problems,
                "run_dir": str(recorder.run_dir),
                "verification_complete": bool(
                    transcript is not None
                    and recorder.verification_complete()),
            }

        def job(say):
            import tws_launch
            budget = addstock_watchdog.maintenance_budget(
                len(ports), per_port_s=300.0, overhead_s=180.0)
            with addstock_watchdog.maintenance_window(
                    tws_launch.fleet_path(), ports, budget, progress=say):
                return _job_without_maintenance(say)

        def done_popup(kind, payload, win, append):
            self._fleet_summary_box(
                "Start up multi-port", payload, parent=win,
                embed=append,
                error=(f"The fleet startup failed:\n\n{payload}"
                       if kind == "error" else None))

        self._storage_fleet_popup(
            "Start up multi-port — launch + enable the TWS demo fleet",
            f"Launching + binding {n} TWS demo instance(s): "
            f"{', '.join(emails)} on port(s) {ports_str}.\n"
            "Keep the screen clear — the login and the config dialogs are "
            "driven by image matching.",
            job, on_done=done_popup)

    def _storage_validate_ports(self) -> None:
        """'Validate ports' — NON-DESTRUCTIVE fleet health check. Probes each
        configured port's API socket (up/down) and reports the account it is
        recorded as serving (fleet.json). No restart, no login, no screen
        takeover — just a status report. The socket probes can take ~1s each, so
        they run on a worker thread and the result is shown via root.after."""
        import threading
        ports = self._valid_port_list(self._storage_tws_ports_list)
        if not ports:
            self._show_unattended_notice(
                "Validate ports",
                "No ports are configured in the ports editor.",
                parent=self.root)
            return

        def work():
            try:
                import tws_launch
                health = tws_launch.fleet_health(ports)
                fleet = tws_launch.load_fleet()
            except Exception as exc:  # noqa: BLE001
                self.root.after(
                    0, lambda e=exc: self._show_unattended_notice(
                        "Validate ports", f"Health check failed:\n\n{e!r}",
                        level="error", parent=self.root))
                return
            rows = [(p, bool(health.get(p)),
                     (fleet.get(p) or {}).get("account"),
                     (fleet.get(p) or {}).get("email")) for p in ports]
            self.root.after(0, lambda: self._render_port_health(rows))

        threading.Thread(target=work, daemon=True).start()

    def _render_port_health(self, rows):
        """Show the port-health report (GUI thread). `rows` is
        [(port, up: bool, account, email)]."""
        up = [p for p, ok, _a, _e in rows if ok]
        width = max((len(str(p)) for p, *_ in rows), default=4)
        lines = []
        for p, ok, acct, email in rows:
            who = acct or email or "(not bound / no record)"
            lines.append(f"  {str(p).rjust(width)}   "
                         f"{'UP  ' if ok else 'DOWN'}   {who}")
        body = (f"{len(up)}/{len(rows)} port(s) healthy:\n\n"
                + "\n".join(lines))
        if len(up) == len(rows):
            self._show_unattended_notice(
                "Validate ports — all healthy", body, parent=self.root)
        else:
            down = [p for p, ok, _a, _e in rows if not ok]
            body += (f"\n\nDown: {', '.join(map(str, down))}.\n"
                     "Use 'Start up multi-port' to (re)bring up the fleet.")
            self._show_unattended_notice(
                "Validate ports — some down", body, level="warning",
                parent=self.root)

    def _storage_restart_dead_start(self) -> None:
        """Restart ONLY the ports whose API socket is currently down — the TWS
        demo's daily auto-logout case. Up ports are left untouched; each dead
        one is relaunched, re-logged-in, and its port re-bound. Restarts run
        one at a time (they take over the screen). (No longer on a toolbar
        button — retained for programmatic/optional use; recovery during a
        fetch goes through _safe_restart_ports.)"""
        if self._storage_busy():
            return
        ports = self._valid_port_list(self._storage_tws_ports_list)
        if not ports:
            messagebox.showinfo(
                "Restart dead ports",
                "No ports are configured in the ports editor.",
                parent=self.root)
            return
        if not messagebox.askyesno(
                "Restart dead ports",
                f"Check {len(ports)} port(s) {', '.join(map(str, ports))} and "
                "restart any that are logged out?\n\nUp ports are left alone. "
                "Each restart takes over the screen for ~1–2 min to drive the "
                "re-login. Don't run this while a data fetch is in progress.",
                parent=self.root):
            return

        def _job_without_maintenance(say):
            import tws_launch
            recorder = tws_launch.bringup.FleetTranscript("restart_dead")
            fleet = [(tws_launch.email_for_port(p, fallback_index=i)
                      or f"{chr(ord('a') + i)}@gmail.com", p)
                     for i, p in enumerate(ports)]
            health = tws_launch.fleet_health(ports)
            dead = [p for p in ports if not health.get(p)]
            if not dead:
                say(f"all {len(ports)} port(s) are up — nothing to restart.")
                recorder.write({"first_pass": len(ports),
                                "succeeded": len(ports)}, on_progress=say)
                return {"up": list(ports), "total": len(ports), "problems": {},
                        "nothing": True}
            say(f"down: {', '.join(map(str, dead))} — restarting only those…")
            try:
                res = tws_launch.restart_dead(
                    fleet, on_progress=say, recorder=recorder)
            except Exception:
                recorder.write(on_progress=say)
                raise

            def _ok(s):
                return s == "up" or isinstance(s, dict)
            up = sorted(p for p, s in res.items() if _ok(s))
            problems = {p: str(s) for p, s in res.items() if not _ok(s)}
            say(f"\n{len(up)}/{len(res)} port(s) now up.")
            for p, s in res.items():
                if not _ok(s):
                    say(f"  port {p}: {s}")
            recorder.write({"first_pass": len(dead), "succeeded": len(up),
                            "failed": len(problems)}, on_progress=say)
            return {"up": up, "total": len(ports), "problems": problems}

        def job(say):
            import tws_launch
            budget = addstock_watchdog.maintenance_budget(
                len(ports), per_port_s=220.0, overhead_s=60.0)
            with addstock_watchdog.maintenance_window(
                    tws_launch.fleet_path(), ports, budget, progress=say):
                return _job_without_maintenance(say)

        def done_popup(kind, payload, win, append):
            self._fleet_summary_box(
                "Restart dead ports", payload, parent=win,
                embed=append,
                error=(f"Restart failed:\n\n{payload}"
                       if kind == "error" else None))

        self._storage_fleet_popup(
            "Restart dead ports — recovering logged-out instances",
            f"Probing {len(ports)} port(s) and restarting any that are down. "
            "Keep the screen clear during a restart.",
            job, on_done=done_popup)

    # ---- Export combined CSV ---------------------------------------------

    def _storage_export_dialog(self) -> None:
        """The 'Export file…' dialog: pick a stored series, choose EITHER a
        named preset (last 2mo/6mo/1yr/2yr/5yr/10yr, or the furthest stored
        span) OR explicit start/end dates, then write ONE canonical CSV
        stitched from the month shards. The stitch runs on a worker thread
        (a 10-year 1m series is millions of bars) with the same
        queue/after discipline as the other storage dialogs — the worker
        never touches tkinter."""
        if self._storage_busy():
            return
        if getattr(self, "_storage_export_win", None) is not None:
            try:
                self._storage_export_win.lift()
                return
            except Exception:  # noqa: BLE001 — destroyed window
                self._storage_export_win = None
        known = self._storage_known_series()
        if not known:
            try:
                messagebox.showinfo(
                    "Export file",
                    "No stored series yet — add or ingest data first.",
                    parent=self.root)
            except Exception:  # noqa: BLE001
                pass
            return
        self._export_known = sorted(t for t, _ivs in known)
        self._export_list = []                  # ordered, deduped (add-order)
        win = tk.Toplevel(self.root)
        win.title("Export to folder")
        win.geometry("720x600")
        win.transient(self.root)
        self._storage_export_win = win
        self._storage_export_busy = False
        win.protocol("WM_DELETE_WINDOW", self._storage_export_close)

        # --- pickers: searchable source -> numbered, add-order export list ---
        pick = ttk.Frame(win, padding=(10, 8))
        pick.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        left = ttk.LabelFrame(pick, text="In your data bank", padding=(6, 4))
        left.grid(row=0, column=0, sticky="nsew")
        sf = ttk.Frame(left)
        sf.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(sf, text="Search:").pack(side=tk.LEFT)
        self._export_search = tk.StringVar()
        ttk.Entry(sf, textvariable=self._export_search).pack(
            side=tk.LEFT, fill=tk.X, expand=True, padx=(4, 0))
        self._export_search.trace_add(
            "write", lambda *_a: self._export_render_source())
        slb = ttk.Frame(left)
        slb.pack(side=tk.TOP, fill=tk.BOTH, expand=True, pady=(4, 0))
        self._export_source_lb = tk.Listbox(
            slb, selectmode="extended", exportselection=False, height=12,
            font=("Consolas", 9))
        ssb = ttk.Scrollbar(slb, command=self._export_source_lb.yview)
        self._export_source_lb.config(yscrollcommand=ssb.set)
        ssb.pack(side=tk.RIGHT, fill=tk.Y)
        self._export_source_lb.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._export_source_lb.bind(
            "<Double-Button-1>", lambda _e: self._export_add_selected())

        mid = ttk.Frame(pick, padding=(8, 0))
        mid.grid(row=0, column=1, sticky="ns")
        ttk.Button(mid, text="Add →",
                   command=self._export_add_selected).pack(
            side=tk.TOP, pady=(28, 4), fill=tk.X)
        ttk.Button(mid, text="← Remove",
                   command=self._export_remove_selected).pack(
            side=tk.TOP, pady=4, fill=tk.X)
        ttk.Separator(mid).pack(side=tk.TOP, fill=tk.X, pady=8)
        ttk.Button(mid, text="S&P 500",
                   command=self._export_sp500_preset).pack(
            side=tk.TOP, pady=4, fill=tk.X)
        ttk.Button(mid, text="Add shown",
                   command=self._export_add_all).pack(
            side=tk.TOP, pady=4, fill=tk.X)
        ttk.Button(mid, text="Clear",
                   command=self._export_clear_list).pack(
            side=tk.TOP, pady=4, fill=tk.X)

        right = ttk.LabelFrame(pick, text="Export list (add order)",
                               padding=(6, 4))
        right.grid(row=0, column=2, sticky="nsew")
        rlb = ttk.Frame(right)
        rlb.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self._export_list_lb = tk.Listbox(
            rlb, selectmode="extended", exportselection=False, height=12,
            font=("Consolas", 9))
        rsb = ttk.Scrollbar(rlb, command=self._export_list_lb.yview)
        self._export_list_lb.config(yscrollcommand=rsb.set)
        rsb.pack(side=tk.RIGHT, fill=tk.Y)
        self._export_list_lb.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._export_list_lb.bind(
            "<Double-Button-1>", lambda _e: self._export_remove_selected())
        self._export_count_lbl = ttk.Label(right, text="0 in list",
                                            foreground="#888")
        self._export_count_lbl.pack(side=tk.TOP, anchor="w", pady=(4, 0))
        pick.columnconfigure(0, weight=1)
        pick.columnconfigure(2, weight=1)
        pick.rowconfigure(0, weight=1)

        # --- shared settings (apply to every ticker in the list) ---
        opt = ttk.Frame(win, padding=(10, 2))
        opt.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(opt, text="Interval:").grid(row=0, column=0, sticky="w")
        self._export_interval = tk.StringVar(value="1m")
        ttk.Combobox(opt, textvariable=self._export_interval, width=6,
                     state="readonly",
                     values=["1m", "2m", "5m", "15m", "30m", "1h"]).grid(
            row=0, column=1, sticky="w", padx=(4, 14))
        ttk.Label(opt, text="Hours:").grid(row=0, column=2, sticky="w")
        self._export_hours = tk.StringVar(value="Regular hours")
        ttk.Combobox(opt, textvariable=self._export_hours, width=24,
                     state="readonly",
                     values=["Regular hours",
                             "Regular + extended (one file)",
                             "Pre-market only", "After-hours only"]).grid(
            row=0, column=3, sticky="w", padx=(4, 14))
        ttk.Label(opt, text="Format:").grid(row=0, column=4, sticky="w")
        self._export_fmt = tk.StringVar(value="parquet")
        ttk.Radiobutton(opt, text="Parquet", value="parquet",
                        variable=self._export_fmt).grid(row=0, column=5,
                                                        sticky="w", padx=(4, 0))
        ttk.Radiobutton(opt, text="CSV", value="csv",
                        variable=self._export_fmt).grid(row=0, column=6,
                                                        sticky="w", padx=(4, 0))

        # Range source: preset OR explicit dates.
        self._export_mode = tk.StringVar(value="preset")
        rng = ttk.LabelFrame(win, text="Range (shared)", padding=(10, 4))
        rng.pack(side=tk.TOP, fill=tk.X, padx=10, pady=(2, 0))
        ttk.Radiobutton(rng, text="Preset:", value="preset",
                        variable=self._export_mode).grid(
            row=0, column=0, sticky="w")
        self._export_preset = tk.StringVar(value="2yr")
        ttk.Combobox(rng, textvariable=self._export_preset,
                     values=list(export_csv.PRESETS), width=10,
                     state="readonly").grid(row=0, column=1, sticky="w",
                                            padx=(4, 0), pady=2)
        ttk.Label(rng, text="(furthest = each ticker's whole stored span)",
                  foreground="#888").grid(row=0, column=2, sticky="w",
                                          padx=(8, 0))
        ttk.Radiobutton(rng, text="Dates:", value="dates",
                        variable=self._export_mode).grid(
            row=1, column=0, sticky="w")
        de = ttk.Frame(rng)
        de.grid(row=1, column=1, columnspan=2, sticky="w", pady=2)
        ttk.Label(de, text="Start").pack(side=tk.LEFT)
        self._export_start = ttk.Entry(de, width=12)
        self._export_start.pack(side=tk.LEFT, padx=(4, 10))
        ttk.Label(de, text="End").pack(side=tk.LEFT)
        self._export_end = ttk.Entry(de, width=12)
        self._export_end.pack(side=tk.LEFT, padx=(4, 0))
        ttk.Label(rng, text="dates as YYYY-MM-DD, inclusive",
                  foreground="#888").grid(row=2, column=1, columnspan=2,
                                          sticky="w")

        btns = ttk.Frame(win, padding=(10, 8))
        btns.pack(side=tk.TOP, fill=tk.X)
        self._export_run_btn = ttk.Button(
            btns, text="Export to folder…",
            command=self._storage_export_run)
        self._export_run_btn.pack(side=tk.LEFT)
        ttk.Button(btns, text="Close",
                   command=self._storage_export_close).pack(
            side=tk.LEFT, padx=(8, 0))

        self._export_status = tk.StringVar(value="")
        self._export_activity = tk.StringVar(value="")
        self._export_progress_surface(
            win, "export", self._export_status, self._export_activity)

        out = ttk.LabelFrame(win, text="Result", padding=(6, 2))
        out.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10,
                 pady=(0, 10))
        self._export_out = tk.Text(out, height=5, wrap="word",
                                   state=tk.DISABLED, font=("Consolas", 9))
        self._export_out.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._export_render_source()
        self._export_render_list()
        self._storage_export_say(
            "Search + Add tickers (or 'S&P 500'), choose interval / range / "
            "hours / format, then 'Export to folder…' writes ONE file per "
            "ticker. RAW as stored — missing months reported, never invented.",
            append=False)

    # ---- export list helpers ----------------------------------------------
    def _export_render_source(self) -> None:
        """Repopulate the source list with the stored tickers matching the
        search box (substring, case-insensitive)."""
        lb = getattr(self, "_export_source_lb", None)
        if lb is None:
            return
        q = (self._export_search.get() or "").strip().upper()
        lb.delete(0, tk.END)
        for t in self._export_known:
            if not q or q in t:
                lb.insert(tk.END, t)

    def _export_render_list(self) -> None:
        """Repaint the export list as a 1. 2. 3. … add-order list + count."""
        lb = getattr(self, "_export_list_lb", None)
        if lb is None:
            return
        lb.delete(0, tk.END)
        for i, t in enumerate(self._export_list, 1):
            lb.insert(tk.END, f"{i}. {t}")
        cl = getattr(self, "_export_count_lbl", None)
        if cl is not None:
            cl.config(text=f"{len(self._export_list)} in list")

    def _export_add_selected(self) -> None:
        lb = getattr(self, "_export_source_lb", None)
        if lb is None:
            return
        for i in lb.curselection():
            t = lb.get(i)
            if t not in self._export_list:        # dedupe, keep add-order
                self._export_list.append(t)
        self._export_render_list()

    def _export_add_all(self) -> None:
        """Add every ticker currently SHOWN (i.e. matching the search)."""
        lb = getattr(self, "_export_source_lb", None)
        if lb is None:
            return
        for i in range(lb.size()):
            t = lb.get(i)
            if t not in self._export_list:
                self._export_list.append(t)
        self._export_render_list()

    def _export_remove_selected(self) -> None:
        lb = getattr(self, "_export_list_lb", None)
        if lb is None:
            return
        for i in sorted(lb.curselection(), reverse=True):
            if 0 <= i < len(self._export_list):
                del self._export_list[i]
        self._export_render_list()

    def _export_clear_list(self) -> None:
        self._export_list = []
        self._export_render_list()

    def _current_sp500_async(self, on_ready) -> None:
        """Fetch the CURRENT S&P 500 list OFF the GUI thread (datahub/Wikipedia,
        bundled-snapshot fallback), cache it for the session, then call
        on_ready(tickers, source, asof) on the GUI thread. Repeat clicks reuse the
        session cache (instant); the engine also caches to disk for offline."""
        cached = getattr(self, "_sp500_live", None)
        if cached:
            on_ready(*cached)
            return
        import threading
        try:
            cache_path = str(Path(self._storage_root).parent / "sp500_current.json")
        except Exception:  # noqa: BLE001
            cache_path = None

        def _work():
            try:
                res = sp500.current_sp500(cache_path=cache_path)
            except Exception:  # noqa: BLE001 — current_sp500 shouldn't raise
                res = (list(sp500.SP500), "bundled", None)

            def _apply():
                self._sp500_live = res
                try:
                    on_ready(*res)
                except Exception:  # noqa: BLE001 — dialog closed mid-fetch
                    pass
            try:
                self.root.after(0, _apply)
            except Exception:  # noqa: BLE001
                pass
        threading.Thread(target=_work, daemon=True, name="sp500-fetch").start()

    @staticmethod
    def _sp500_src_label(source, asof, n):
        """Human label telling the user whether they got the LIVE index or a
        fallback — so a stale list is never mistaken for current."""
        if source == "bundled":
            return f"bundled snapshot, {n} names — OFFLINE, may be out of date"
        if source == "cache":
            return f"cached {asof}, {n} names — offline"
        return f"current via {source}, {n} names, fetched {asof}"

    def _export_sp500_preset(self) -> None:
        """Add the CURRENT S&P 500 constituents PRESENT in the bank (live list,
        fetched off-thread; bundled-snapshot fallback offline). If any are not
        stored, list them and ask whether to proceed with the ones we have."""
        self._storage_export_say("Fetching the current S&P 500 list…")
        self._current_sp500_async(self._export_sp500_apply)

    def _export_sp500_apply(self, tickers, source, asof) -> None:
        win = getattr(self, "_storage_export_win", None)
        try:
            if win is None or not win.winfo_exists():
                return
        except Exception:  # noqa: BLE001
            return
        members = set(tickers)
        n = len(members)
        label = self._sp500_src_label(source, asof, n)
        have = sorted(t for t in self._export_known if t in members)
        missing = sorted(members - set(self._export_known))
        if not have:
            messagebox.showinfo(
                "S&P 500",
                f"None of the {n} S&P 500 constituents ({label}) are in your "
                f"data bank yet — fetch them first.", parent=win)
            return
        if missing:
            shown = ", ".join(missing[:30])
            if len(missing) > 30:
                shown += f", …and {len(missing) - 30} more"
            go = messagebox.askyesno(
                "S&P 500 — some not in your bank",
                f"{len(have)} of {n} S&P 500 constituents ({label}) are in your "
                f"bank and will be added.\n\n{len(missing)} are NOT stored:\n"
                f"{shown}\n\nAdd the {len(have)} you have?", parent=win)
            if not go:
                return
        for t in have:
            if t not in self._export_list:
                self._export_list.append(t)
        self._export_render_list()
        self._storage_export_say(f"S&P 500 preset: added {len(have)} ({label}).")

    def _storage_export_say(self, text, append=True) -> None:
        try:
            t = self._export_out
            t.config(state=tk.NORMAL)
            if not append:
                t.delete("1.0", tk.END)
            t.insert(tk.END, text + "\n")
            t.see(tk.END)
            t.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001 — dialog closed
            pass

    def _storage_export_close(self) -> None:
        if getattr(self, "_storage_export_busy", False):
            self._storage_export_say(
                "An export is running — let it finish before closing.")
            return
        try:
            self._storage_export_win.destroy()
        except Exception:  # noqa: BLE001
            pass
        self._storage_export_win = None

    def _storage_export_run(self) -> None:
        """Validate on the GUI thread, ask for a FOLDER, then export ONE file
        per ticker in the list on a worker thread (queue + root.after). Shared
        interval/range/hours/format apply to every ticker; a ticker that fails
        (or has no data) is reported, never aborts the batch."""
        if getattr(self, "_storage_export_busy", False):
            return
        tickers = list(getattr(self, "_export_list", []))
        if not tickers:
            self._storage_export_say(
                "Add at least one ticker to the export list first.")
            return
        interval = self._export_interval.get().strip() or "1m"
        hours = self._export_hours.get()
        fmt = self._export_fmt.get().strip() or "parquet"
        mode = self._export_mode.get()
        start_date = end_date = preset = None
        from datetime import datetime as _dt
        if mode == "dates":
            try:
                start_date = _dt.strptime(
                    self._export_start.get().strip(), "%Y-%m-%d").date()
                end_date = _dt.strptime(
                    self._export_end.get().strip(), "%Y-%m-%d").date()
            except ValueError:
                self._storage_export_say(
                    "Dates must be YYYY-MM-DD (e.g. 2022-01-31).")
                return
            if end_date < start_date:
                self._storage_export_say("End date is before start date.")
                return
        else:
            preset = self._export_preset.get().strip() or "2yr"

        folder = filedialog.askdirectory(
            parent=self._storage_export_win, title="Choose an export folder",
            initialdir=str(self._storage_root.parent), mustexist=True)
        if not folder:
            return
        parent_dir = Path(folder)
        ext = "parquet" if fmt == "parquet" else "csv"

        self._storage_export_busy = True
        try:
            self._export_run_btn.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001
            pass
        preparing = (f"Preparing {len(tickers)} ticker(s) in {parent_dir} "
                     f"as {ext}…")
        self._export_progress_reset("export", preparing)
        self._export_progress_pulse_start("export")
        self._storage_export_say(preparing, append=False)
        import queue
        import threading
        q = queue.Queue()
        self._storage_export_q = q
        root_dir = self._storage_root

        def _work():
            try:
                q.put(("progress", {
                    "kind": "health",
                    "message": "Checking bank health for export…",
                }))
                export_quality.ensure_current_health(root_dir)
                q.put(("progress", {
                    "kind": "health",
                    "message": ("Bank health current; "
                                f"{EXPORT_HEALTH_REPORT_NAME} will be bundled…"),
                }))
                payload = export_batch.run_storage_batch(
                    root_dir, tickers, parent_dir, interval=interval,
                    mode=mode, start_date=start_date, end_date=end_date,
                    preset=preset, hours_mode=hours, file_format=fmt,
                    progress=lambda event: q.put(("progress", event)))
            except Exception as exc:  # noqa: BLE001
                payload = exc
            try:
                q.put(("done", payload))
            except Exception:  # noqa: BLE001
                pass

        try:
            threading.Thread(target=_work, daemon=True,
                             name="folder-export").start()
        except Exception as exc:  # noqa: BLE001 — thread exhaustion
            self._storage_export_done(exc)
            return
        self.root.after(100, self._storage_export_poll)

    def _storage_export_poll(self) -> None:
        q = getattr(self, "_storage_export_q", None)
        if q is None:
            return
        import queue
        try:
            while True:
                kind, payload = q.get_nowait()
                if kind == "progress":
                    self._export_progress_apply("export", payload)
                    self._storage_export_say(
                        export_batch.progress_text(payload))
                else:
                    self._storage_export_q = None
                    self._storage_export_done(payload)
                    return
        except queue.Empty:
            pass
        except Exception as exc:  # noqa: BLE001 — truthful end state,
            self._storage_export_q = None   # never a stuck busy flag
            self._storage_export_done(RuntimeError(f"display failed: "
                                                   f"{exc}"))
            return
        try:
            self.root.after(100, self._storage_export_poll)
        except Exception:  # noqa: BLE001 — window closing
            pass

    def _storage_export_done(self, payload) -> None:
        """Render a finished FOLDER export (GUI thread): one line per ticker
        (rows written or why it produced nothing), plus a summary messagebox.
        A per-ticker error/empty is reported, never silently dropped."""
        self._storage_export_busy = False
        self._export_progress_finish("export")
        try:
            self._export_run_btn.config(state=tk.NORMAL)
        except Exception:  # noqa: BLE001 — dialog closed mid-run
            pass
        if isinstance(payload, Exception):
            try:
                self._export_status.set(f"Export failed: {payload}")
            except Exception:  # noqa: BLE001
                pass
            self._storage_export_say(f"Export failed: {payload}")
            try:
                messagebox.showwarning(
                    "Export", f"Export failed:\n{payload}",
                    parent=getattr(self, "_storage_export_win", None)
                    or self.root)
            except Exception:  # noqa: BLE001
                pass
            return
        folder = payload.get("bundle_folder") or payload.get("folder", "")
        ext = payload.get("format", "")
        results = payload.get("results") or []
        ok, empty, failed, cancelled, absent, total_rows = 0, 0, 0, 0, 0, 0
        lines = []
        for r in results:
            tkr = r.get("ticker", "?")
            if r.get("not_in_bank"):
                absent += 1
                lines.append(f"  - {tkr}: not in bank")
            elif r.get("cancelled"):
                cancelled += 1
                lines.append(f"  - {tkr}: cancelled")
            elif r.get("error"):
                failed += 1
                lines.append(f"  ✗ {tkr}: {r['error']}")
            elif r.get("rows", 0) > 0:
                ok += 1
                total_rows += r["rows"]
                hole = (f"  (missing {len(r['holes'])} month(s))"
                        if r.get("holes") else "")
                lines.append(f"  ✓ {tkr}: {r['rows']:,} rows{hole}")
            else:
                empty += 1
                lines.append(f"  – {tkr}: no rows in range (empty file written)")
        head = (f"Exported {ok} file(s) ({total_rows:,} rows total) to {folder} "
                f"as {ext}." + (f"  {empty} empty, {failed} failed, "
                                 f"{cancelled} cancelled, {absent} absent."
                                 if (empty or failed or cancelled or absent)
                                 else ""))
        note_line = ""
        note_path = payload.get("note_path")
        note_error = payload.get("note_error")
        quality = payload.get("quality_summary") or {}
        health_path = payload.get("health_report")
        health_line = (f"Health report: {Path(health_path).name}"
                       if health_path else "")
        if note_path:
            note_line = (f"Quality note: {Path(note_path).name}  "
                         f"confirmed bad={quality.get('CONFIRMED_BAD', 0)}, "
                         f"review={quality.get('REVIEW_REQUIRED', 0)}, "
                         f"known={quality.get('KNOWN_DIFFERENCE', 0)}, "
                         f"unverifiable={quality.get('UNVERIFIABLE', 0)}, "
                         f"stale={quality.get('NO_CURRENT_EVIDENCE', 0)}.")
        elif note_error:
            note_line = f"Quality note FAILED: {note_error}"
        detail = head + "\n" + "\n".join(lines)
        if health_line:
            detail += "\n" + health_line
        if note_line:
            detail += "\n" + note_line
        try:
            self._export_status.set(head)
        except Exception:  # noqa: BLE001
            pass
        self._storage_export_say(detail)
        try:
            messagebox.showinfo(
                "Export", head
                + (("\n\n" + health_line) if health_line else "")
                + (("\n" + note_line) if note_line else ""),
                parent=getattr(self, "_storage_export_win", None) or self.root)
        except Exception:  # noqa: BLE001
            pass

    # ---- Export designer (configurable, live-preview) ---------------------

    _EXD_INTERVAL_ORDER = ["1s", "5s", "10s", "15s", "30s", "1m", "2m", "3m",
                           "5m", "10m", "15m", "30m", "1h", "2h", "4h", "1d"]
    _EXD_DELIMS = {"comma": ",", "semicolon": ";", "pipe": "|", "tab": "\t"}
    _EXD_SESSIONS = {"Regular hours": ["rth"],
                     "Regular + extended": ["rth", "pre", "post"],
                     "Pre-market only": ["pre"], "After-hours only": ["post"]}
    _EXD_SAMPLE_SOURCES = ("Sample fake", "Bank data")

    def _exd_bank_inventory(self):
        """(tickers, base_trade_intervals) from the scanned table when present.
        Falls back to one manifest pass if Export Designer opens before a scan."""
        known = self._storage_cached_known_series()
        if known is None:
            try:
                pairs = stock_validate.discover_series(self._storage_root,
                                                       rth_only=False,
                                                       kinds=None)
            except Exception:  # noqa: BLE001
                pairs = []
        else:
            pairs = [(t, iv) for t, ivs in known for iv in ivs]
        tickers = sorted({t for t, _iv in pairs})
        bases = {stock_storage.base_interval(iv) for _t, iv in pairs
                 if stock_storage.kind_of(iv) == ""}
        order = self._EXD_INTERVAL_ORDER
        intervals = (sorted(bases, key=lambda x: (
                     order.index(x) if x in order else 99, x))
                     if bases else ["1m"])
        return tickers, intervals

    def _storage_exd_open(self) -> None:
        """The Export Designer: configurable columns / format / scope with a
        LIVE SAMPLE that renders Sample fake rows (invented, covering every
        variation) or the first real bank rows through the SAME formatter the
        export uses. Builds on the
        export_designer engine core (GUI-free, headless-tested)."""
        if getattr(self, "_exd_win", None) is not None:
            try:
                if self._exd_win.winfo_exists():
                    self._exd_win.lift()
                    return
            except Exception:  # noqa: BLE001
                pass
        self._exd_known, self._exd_ivs = self._exd_bank_inventory()
        self._exd_picked = set()
        self._exd_pick_src = None
        self._exd_after = None
        self._exd_drag_from = None
        self._exd_sample_key = None
        self._exd_sample_cache = (None, None, None)
        self._exd_est_key = None
        self._exd_est_cache = (0, 0)
        self._exd_q = None
        self._exd_cancel_ev = None
        self._exd_export_after = None
        self._exd_sample_tab = tk.StringVar(value="export")
        self._exd_sample_source = tk.StringVar(
            value=self._EXD_SAMPLE_SOURCES[0])
        self._exd_issue_counts = {"err": 0, "warn": 0}
        self._exd_sample_issues = []
        self._exd_sample_span = (None, None)

        win = tk.Toplevel(self.root)
        win.title("Export")
        win.geometry("1000x700")
        win.minsize(900, 600)
        win.transient(self.root)
        self._exd_win = win
        win.protocol("WM_DELETE_WINDOW", self._exd_close)

        # --- control variables ---
        self._exd_filetype = tk.StringVar(value="csv")
        self._exd_delim = tk.StringVar(value="comma")
        self._exd_datefmt = tk.StringVar(value="iso")
        self._exd_datecustom = tk.StringVar(value="%Y-%m-%d %H:%M:%S")
        self._exd_tz = tk.StringVar(value="America/New_York")
        self._exd_header = tk.BooleanVar(value=True)
        self._exd_empty = tk.StringVar(value="blank")
        ivs = self._exd_available_intervals()
        self._exd_interval = tk.StringVar(value=("1m" if "1m" in ivs else ivs[0]))
        self._exd_range = tk.StringVar(value="2yr")
        self._exd_session = tk.StringVar(value="Regular hours")
        self._exd_estimate = tk.StringVar(value="est: —")
        self._exd_status = tk.StringVar(value="")
        self._exd_activity = tk.StringVar(value="")
        # Engine defaults define both the initial order and checked columns.
        self._exd_colstate = [
            [col, col in export_designer.DEFAULT_COLUMNS]
            for col in export_designer.ALL_COLUMNS
        ]
        self._EXD_ROW_H = 30
        self._exd_col_items = {}     # key -> {bg, grip, ck, tx} canvas ids
        self._exd_col_y = {}         # key -> current top-y on the canvas
        self._exd_col_target = {}    # key -> glide destination top-y
        self._exd_col_jobs = {}      # key -> pending after-id (mid-glide)
        self._exd_drag_key = None    # column being dragged, or None
        self._exd_drag_moved = False
        self._exd_drag_dy = 0
        self._exd_press_x = 0
        self._exd_hover_key = None
        self._exd_col_sel = None     # last-clicked row (the ↑/↓ target)

        # Bottom controls (progress surface + action buttons) are packed BEFORE
        # the expanding body, so when the window is too short the sample grid
        # (which has its own scrollbar) gives up the space and the action
        # buttons — Export in particular — are NEVER clipped off the bottom edge
        # (user 2026-07-22; previously the body packed first and clipped them).
        bottom = ttk.Frame(win)
        bottom.pack(side=tk.BOTTOM, fill=tk.X)
        self._export_progress_surface(
            bottom, "exd", self._exd_status, self._exd_activity)
        btm = ttk.Frame(bottom, padding=(8, 6))
        btm.pack(side=tk.TOP, fill=tk.X)
        ttk.Button(btm, text="Load preset ▾",
                   command=self._exd_load_preset_menu).pack(side=tk.LEFT)
        ttk.Button(btm, text="Save preset",
                   command=self._exd_save_preset).pack(side=tk.LEFT, padx=(6, 0))
        self._exd_export_btn = ttk.Button(btm, text="Export…",
                                          command=self._exd_export)
        self._exd_export_btn.pack(side=tk.RIGHT)
        self._exd_cancel_btn = ttk.Button(
            btm, text="Cancel export", command=self._exd_cancel_export,
            state=tk.DISABLED)
        self._exd_cancel_btn.pack(side=tk.RIGHT, padx=(0, 6))
        ttk.Button(btm, text="Close", command=self._exd_close).pack(
            side=tk.RIGHT, padx=(0, 6))

        upper = ttk.Frame(win, padding=8)
        upper.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        upper.columnconfigure(0, weight=0)
        upper.columnconfigure(1, weight=1)
        upper.rowconfigure(0, weight=1)

        # === DATA (columns + scope) ===
        dataf = ttk.LabelFrame(upper, text="Data", padding=(8, 6))
        dataf.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        colf = ttk.LabelFrame(
            dataf, text="Columns — click ☑ to include · drag to reorder",
            padding=(6, 4))
        colf.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self._EXD_COL_W = 196
        clbw = ttk.Frame(colf)
        clbw.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._exd_colcv = tk.Canvas(
            clbw, width=self._EXD_COL_W,
            height=self._EXD_ROW_H * len(self._exd_colstate) + 6,
            bg="#ffffff", highlightthickness=1, highlightbackground="#cfd6dd")
        self._exd_colcv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._exd_colcv.bind("<ButtonPress-1>", self._exd_col_press)
        self._exd_colcv.bind("<B1-Motion>", self._exd_col_drag)
        self._exd_colcv.bind("<ButtonRelease-1>", self._exd_col_release)
        self._exd_colcv.bind("<Motion>", self._exd_col_hover)
        self._exd_colcv.bind("<Leave>", lambda _e: self._exd_col_sethover(None))
        cbtn = ttk.Frame(colf)
        cbtn.pack(side=tk.LEFT, fill=tk.Y, padx=(6, 0))
        ttk.Button(cbtn, text="↑", width=3,
                   command=lambda: self._exd_col_move(-1)).pack(pady=(0, 4))
        ttk.Button(cbtn, text="↓", width=3,
                   command=lambda: self._exd_col_move(1)).pack()

        scopef = ttk.LabelFrame(dataf, text="Scope", padding=(6, 4))
        scopef.pack(side=tk.TOP, fill=tk.X, pady=(8, 0))
        self._exd_tickers_btn = ttk.Button(
            scopef, text="Tickers [0 sel ▾]", command=self._exd_open_picker)
        ttk.Label(scopef, text="Tickers:").grid(row=0, column=0, sticky="w",
                                                pady=2)
        self._exd_tickers_btn.grid(row=0, column=1, sticky="we", pady=2,
                                   padx=(4, 0))
        ttk.Label(scopef, text="Interval:").grid(row=1, column=0, sticky="w",
                                                 pady=2)
        self._exd_interval_cb = ttk.Combobox(
            scopef, textvariable=self._exd_interval, values=ivs, width=8,
            state="readonly")
        self._exd_interval_cb.grid(row=1, column=1, sticky="w", pady=2,
                                   padx=(4, 0))
        self._exd_interval_cb.bind("<<ComboboxSelected>>",
                                   lambda _e: self._exd_on_format_change())
        ttk.Label(scopef, text="Range:").grid(row=2, column=0, sticky="w",
                                              pady=2)
        ttk.Combobox(scopef, textvariable=self._exd_range,
                     values=list(export_csv.PRESETS), width=8, state="readonly"
                     ).grid(row=2, column=1, sticky="w", pady=2, padx=(4, 0))
        ttk.Label(scopef, text="Session:").grid(row=3, column=0, sticky="w",
                                                pady=2)
        self._exd_session_cb = ttk.Combobox(
            scopef, textvariable=self._exd_session,
            values=list(self._EXD_SESSIONS), width=20, state="readonly")
        self._exd_session_cb.grid(row=3, column=1, sticky="w", pady=2,
                                  padx=(4, 0))
        scopef.columnconfigure(1, weight=1)
        for _v in (self._exd_range, self._exd_session):
            _v.trace_add("write", lambda *_a: self._exd_schedule_render())

        # === right column: FORMAT + SAMPLE ===
        rightf = ttk.Frame(upper)
        rightf.grid(row=0, column=1, sticky="nsew")
        fmtf = ttk.LabelFrame(rightf, text="Format", padding=(8, 6))
        fmtf.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(fmtf, text="File type:").grid(row=0, column=0, sticky="w", pady=2)
        ttk.Combobox(fmtf, textvariable=self._exd_filetype,
                     values=list(export_designer.FILE_TYPES), width=10,
                     state="readonly").grid(row=0, column=1, sticky="w", padx=(4, 14))
        ttk.Checkbutton(fmtf, text="Header row", variable=self._exd_header,
                        command=self._exd_schedule_render).grid(
            row=0, column=2, columnspan=2, sticky="w")
        ttk.Label(fmtf, text="Delimiter:").grid(row=1, column=0, sticky="w", pady=2)
        self._exd_delim_cb = ttk.Combobox(
            fmtf, textvariable=self._exd_delim, values=list(self._EXD_DELIMS),
            width=10, state="readonly")
        self._exd_delim_cb.grid(row=1, column=1, sticky="w", padx=(4, 14))
        ttk.Label(fmtf, text="Empty value:").grid(row=1, column=2, sticky="w")
        ttk.Combobox(fmtf, textvariable=self._exd_empty,
                     values=list(export_designer.EMPTY_MODES), width=8,
                     state="readonly").grid(row=1, column=3, sticky="w", padx=(4, 0))
        ttk.Label(fmtf, text="Timestamp format:").grid(
            row=2, column=0, sticky="w", pady=2)
        ttk.Combobox(fmtf, textvariable=self._exd_datefmt,
                     values=list(export_designer.DATE_FORMATS), width=10,
                     state="readonly").grid(row=2, column=1, sticky="w", padx=(4, 14))
        ttk.Label(fmtf, text="Timezone:").grid(row=2, column=2, sticky="w")
        ttk.Combobox(fmtf, textvariable=self._exd_tz,
                     values=list(export_designer.TIMEZONES), width=18,
                     state="readonly").grid(row=2, column=3, sticky="w", padx=(4, 0))
        ttk.Label(fmtf, text="Custom timestamp:").grid(
            row=3, column=0, sticky="w", pady=2)
        self._exd_datecustom_ent = ttk.Entry(
            fmtf, textvariable=self._exd_datecustom, width=28)
        self._exd_datecustom_ent.grid(row=3, column=1, columnspan=3, sticky="we",
                                      padx=(4, 0))
        fmtf.columnconfigure(3, weight=1)
        for _cb in (self._exd_filetype, self._exd_delim, self._exd_datefmt,
                    self._exd_tz, self._exd_empty):
            _cb.trace_add("write", lambda *_a: self._exd_on_format_change())
        self._exd_datecustom.trace_add(
            "write", lambda *_a: self._exd_schedule_render())

        samplef = ttk.LabelFrame(rightf, text="Live sample — formatted rows",
                                 padding=(4, 2))
        samplef.pack(side=tk.TOP, fill=tk.BOTH, expand=True, pady=(8, 0))
        tabbar = ttk.Frame(samplef)
        tabbar.pack(side=tk.TOP, fill=tk.X, pady=(0, 4))
        self._exd_sample_tab_btns = {}
        for key, label in (("export", "Export"),
                           ("issues", "Issues (0)")):
            btn = tk.Button(tabbar, text=label, bd=1, relief=tk.FLAT,
                            padx=10, pady=3, cursor="hand2",
                            font=("Segoe UI", 9),
                            command=lambda k=key: self._exd_show_sample_tab(k))
            btn.pack(side=tk.LEFT, padx=(0, 4))
            self._exd_sample_tab_btns[key] = btn
        self._exd_sample_source_cb = ttk.Combobox(
            tabbar, textvariable=self._exd_sample_source,
            values=self._EXD_SAMPLE_SOURCES, width=14, state="readonly")
        self._exd_sample_source_cb.pack(side=tk.RIGHT)
        self._exd_sample_source_cb.bind(
            "<<ComboboxSelected>>", lambda _e: self._exd_schedule_render())
        ttk.Label(tabbar, text="Preview:").pack(side=tk.RIGHT, padx=(0, 4))
        stack = ttk.Frame(samplef)
        stack.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        stack.rowconfigure(0, weight=1)
        stack.columnconfigure(0, weight=1)
        self._exd_sample_frames = {}
        for key in ("export", "issues"):
            fr = ttk.Frame(stack)
            fr.grid(row=0, column=0, sticky="nsew")
            self._exd_sample_frames[key] = fr

        expf = self._exd_sample_frames["export"]
        self._exd_sample_msg = ttk.Label(expf, foreground="#555", anchor="w")
        self._exd_sample_msg.pack(side=tk.TOP, fill=tk.X, pady=(0, 2))
        # Source line: names the price series every row is built from (the
        # spine), the columns derived from it, and the series feeding each
        # volatility column. It sits with the rendered output because it
        # describes what the export will contain. Labels are created once and
        # re-texted, so a 160 ms re-render never rebuilds widgets or flickers.
        # Deliberately compact: a subordinate 8pt face, no padding, and at most
        # five short lines (see _exd_render_source), so it annotates the grid
        # instead of pushing it down.
        self._exd_source_frame = ttk.Frame(expf)
        self._exd_source_frame.pack(side=tk.TOP, fill=tk.X)
        self._exd_source_spine = ttk.Label(
            self._exd_source_frame, foreground="#0f5aa8", anchor="w",
            font=self._EXD_SOURCE_FONT)
        self._exd_source_stored = ttk.Label(
            self._exd_source_frame, foreground="#555", anchor="w",
            font=self._EXD_SOURCE_FONT)
        self._exd_source_derived = ttk.Label(
            self._exd_source_frame, foreground="#555", anchor="w",
            font=self._EXD_SOURCE_FONT)
        self._exd_source_kinds = [
            ttk.Label(self._exd_source_frame, foreground="#555", anchor="w",
                      font=self._EXD_SOURCE_FONT)
            for _ in range(2)]
        self._exd_source_alert = ttk.Label(
            self._exd_source_frame, foreground="#b00000", anchor="w",
            wraplength=900, justify=tk.LEFT, font=self._EXD_SOURCE_FONT)
        sgrid = ttk.Frame(expf)
        sgrid.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self._exd_sample = ttk.Treeview(sgrid, show="headings", height=12,
                                        selectmode="none")
        sxv = ttk.Scrollbar(sgrid, orient="vertical",
                            command=self._exd_sample.yview)
        sxh = ttk.Scrollbar(sgrid, orient="horizontal",
                            command=self._exd_sample.xview)
        self._exd_sample.config(yscrollcommand=sxv.set, xscrollcommand=sxh.set)
        sxv.pack(side=tk.RIGHT, fill=tk.Y)
        sxh.pack(side=tk.BOTTOM, fill=tk.X)
        self._exd_sample.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._exd_sample.tag_configure("odd", background="#f6f8fa")  # zebra rows

        issuesf = self._exd_sample_frames["issues"]
        self._exd_issues_msg = ttk.Label(issuesf, foreground="#555", anchor="w")
        self._exd_issues_msg.pack(side=tk.TOP, fill=tk.X, pady=(0, 2))
        igrid = ttk.Frame(issuesf)
        igrid.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self._exd_issues = ttk.Treeview(
            igrid, show="headings", height=12, selectmode="none",
            columns=("level", "message", "fix"))
        iyv = ttk.Scrollbar(igrid, orient="vertical",
                            command=self._exd_issues.yview)
        ixh = ttk.Scrollbar(igrid, orient="horizontal",
                            command=self._exd_issues.xview)
        self._exd_issues.config(yscrollcommand=iyv.set, xscrollcommand=ixh.set)
        iyv.pack(side=tk.RIGHT, fill=tk.Y)
        ixh.pack(side=tk.BOTTOM, fill=tk.X)
        self._exd_issues.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._exd_issues.heading("level", text="Level")
        self._exd_issues.heading("message", text="What is wrong")
        self._exd_issues.heading("fix", text="How to fix it")
        self._exd_issues.column("level", width=70, anchor=tk.CENTER,
                                stretch=False)
        self._exd_issues.column("message", width=360, anchor=tk.W,
                                stretch=True)
        self._exd_issues.column("fix", width=300, anchor=tk.W,
                                stretch=True)
        self._exd_issues.tag_configure("err", background="#ffe8e8",
                                       foreground="#8a1f11")
        self._exd_issues.tag_configure("warn", background="#fff4d6",
                                       foreground="#6f4b00")
        self._exd_issues.tag_configure("ok", background="#eaf7ed",
                                       foreground="#176329")
        self._exd_show_sample_tab("export")
        ttk.Label(rightf, textvariable=self._exd_estimate,
                  foreground="#555").pack(side=tk.TOP, fill=tk.X, pady=(4, 0))

        self._exd_render_columns()
        self._exd_on_format_change()        # set enabled states + first render

    def _exd_available_intervals(self):
        """Base TRADES intervals present in the bank (kind-less), nicely
        ordered; ['1m'] if the bank is empty."""
        ivs = getattr(self, "_exd_ivs", None)
        if ivs:
            return list(ivs)
        _tickers, ivs = self._exd_bank_inventory()
        self._exd_ivs = ivs
        return list(ivs)

    # --- columns list ---

    def _exd_render_columns(self, keep=None):
        """Build/refresh the Canvas checklist from _exd_colstate. Each row is a
        group tagged row:<key> so it can GLIDE as one unit. `keep` is ignored —
        the canvas keeps its own hover/selection state."""
        cv = getattr(self, "_exd_colcv", None)
        if cv is None:
            return
        for _job in list(self._exd_col_jobs.values()):   # stop glides on old items
            try:
                self._exd_win.after_cancel(_job)
            except Exception:  # noqa: BLE001
                pass
        self._exd_col_jobs = {}
        cv.delete("all")
        self._exd_col_items = {}
        H, W = self._EXD_ROW_H, self._EXD_COL_W
        for i, (key, on) in enumerate(self._exd_colstate):
            y = float(i * H + 3)
            tag = ("colrow", f"row:{key}")
            bg = cv.create_rectangle(3, y, W - 3, y + H - 4, width=1,
                                     fill="#ffffff", outline="#eceff3", tags=tag)
            grip = cv.create_text(15, y + (H - 4) / 2, text="∷",
                                  fill="#c2c8d0", font=("Segoe UI", 11), tags=tag)
            ck = cv.create_text(32, y + (H - 4) / 2,
                                text=("☑" if on else "☐"),
                                fill=("#1f6feb" if on else "#aab2bd"),
                                font=("Segoe UI", 13), tags=tag)
            tx = cv.create_text(50, y + (H - 4) / 2,
                                text=export_designer.COLUMN_LABEL.get(key, key),
                                anchor="w", fill=("#1a1a1a" if on else "#9aa3ad"),
                                font=("Segoe UI", 10), tags=tag)
            self._exd_col_items[key] = {"bg": bg, "grip": grip, "ck": ck, "tx": tx}
            self._exd_col_y[key] = y
            self._exd_col_target[key] = y
        self._exd_col_repaint()

    def _exd_col_repaint(self):
        cv = getattr(self, "_exd_colcv", None)
        if cv is None:
            return
        for key, it in self._exd_col_items.items():
            drag = (key == self._exd_drag_key)
            sel = (key == self._exd_col_sel)
            hov = (key == self._exd_hover_key)
            fill = "#e8f0ff" if drag else ("#f3f6fa" if hov else "#ffffff")
            outline = "#1f6feb" if sel else ("#9db8e0" if drag else "#eceff3")
            try:
                cv.itemconfig(it["bg"], fill=fill, outline=outline,
                              width=(2 if (sel or drag) else 1))
            except Exception:  # noqa: BLE001
                pass

    def _exd_glide(self, key):
        """Ease row <key> toward its target y (ease-out); self-correcting if the
        target shifts mid-glide. Rows glide concurrently — that's the animation."""
        cv = getattr(self, "_exd_colcv", None)
        if cv is None or not cv.winfo_exists():
            self._exd_col_jobs.pop(key, None)
            return
        cur, tgt = self._exd_col_y.get(key), self._exd_col_target.get(key)
        if cur is None or tgt is None:
            self._exd_col_jobs.pop(key, None)
            return
        rem = tgt - cur
        if abs(rem) < 0.6:
            cv.move(f"row:{key}", 0, rem)
            self._exd_col_y[key] = tgt
            self._exd_col_jobs.pop(key, None)
            return
        dy = rem * 0.38
        cv.move(f"row:{key}", 0, dy)
        self._exd_col_y[key] = cur + dy
        try:
            self._exd_col_jobs[key] = self._exd_win.after(
                16, lambda: self._exd_glide(key))
        except Exception:  # noqa: BLE001
            self._exd_col_jobs.pop(key, None)

    def _exd_col_layout(self, exclude=None, animate=True):
        """Set each row's target y from the current colstate order; glide there
        (the dragged `exclude` row is positioned live by the drag handler)."""
        H = self._EXD_ROW_H
        cv = getattr(self, "_exd_colcv", None)
        for i, (key, _on) in enumerate(self._exd_colstate):
            self._exd_col_target[key] = float(i * H + 3)
            if key == exclude:
                continue
            if not animate and cv is not None:
                cv.move(f"row:{key}", 0, self._exd_col_target[key]
                        - self._exd_col_y.get(key, self._exd_col_target[key]))
                self._exd_col_y[key] = self._exd_col_target[key]
            elif animate and key not in self._exd_col_jobs:
                self._exd_glide(key)

    def _exd_col_keyat(self, y):
        if not self._exd_colstate:
            return None, -1
        H, n = self._EXD_ROW_H, len(self._exd_colstate)
        if y < 3 or y >= n * H + 3:           # whitespace below/above the rows
            return None, -1                   # -> clean no-op (no clamp/toggle)
        return self._exd_colstate[int((y - 3) // H)][0], int((y - 3) // H)

    def _exd_col_index(self, key):
        for i, (k, _on) in enumerate(self._exd_colstate):
            if k == key:
                return i
        return -1

    def _exd_col_sethover(self, key):
        if key == self._exd_hover_key:
            return
        self._exd_hover_key = key
        self._exd_col_repaint()

    def _exd_col_hover(self, event):
        if self._exd_drag_key is not None:
            return
        key, _i = self._exd_col_keyat(event.y)
        self._exd_col_sethover(key)

    def _exd_col_press(self, event):
        key, _i = self._exd_col_keyat(event.y)
        if key is None:
            return
        self._exd_drag_key = key
        self._exd_drag_moved = False
        self._exd_press_x = event.x
        self._exd_drag_dy = event.y - self._exd_col_y.get(key, event.y)
        job = self._exd_col_jobs.pop(key, None)     # stop any glide on the grab
        if job is not None:
            try:
                self._exd_win.after_cancel(job)
            except Exception:  # noqa: BLE001
                pass
        try:
            self._exd_colcv.tag_raise(f"row:{key}")  # lift above its neighbours
        except Exception:  # noqa: BLE001
            pass
        self._exd_col_repaint()

    def _exd_col_drag(self, event):
        key = self._exd_drag_key
        if key is None:
            return
        H, n = self._EXD_ROW_H, len(self._exd_colstate)
        if not self._exd_drag_moved:
            if abs(event.y - (self._exd_col_y.get(key, 0)
                              + self._exd_drag_dy)) < 4:
                return                               # under threshold -> a click
            self._exd_drag_moved = True
        top = max(3.0, min((n - 1) * H + 3.0, event.y - self._exd_drag_dy))
        cv = self._exd_colcv
        cv.move(f"row:{key}", 0, top - self._exd_col_y.get(key, top))
        self._exd_col_y[key] = top
        tgt_idx = int(max(0, min(n - 1, round((top - 3) / H))))
        cur_idx = self._exd_col_index(key)
        if tgt_idx != cur_idx:                       # reorder + slide the others
            self._exd_colstate.insert(tgt_idx, self._exd_colstate.pop(cur_idx))
            self._exd_col_layout(exclude=key)
            self._exd_schedule_render()

    def _exd_col_release(self, _event):
        key = self._exd_drag_key
        if key is None:
            return
        self._exd_drag_key = None
        self._exd_col_sel = key
        if not self._exd_drag_moved:
            if 22 <= self._exd_press_x < 46:         # the ☑ glyph band -> toggle
                idx = self._exd_col_index(key)
                self._exd_colstate[idx][1] = not self._exd_colstate[idx][1]
                self._exd_render_columns()
                self._exd_schedule_render()
                return
            self._exd_col_repaint()                  # grip/body click -> just select
            return
        self._exd_col_layout()                       # glide the dragged row home
        self._exd_col_repaint()
        self._exd_schedule_render()

    def _exd_col_toggle(self):
        key = self._exd_col_sel or self._exd_hover_key
        if key is None:
            return
        idx = self._exd_col_index(key)
        self._exd_colstate[idx][1] = not self._exd_colstate[idx][1]
        self._exd_render_columns()
        self._exd_schedule_render()

    def _exd_col_move(self, delta):
        key = self._exd_col_sel or self._exd_hover_key
        if key is None and self._exd_colstate:
            key = self._exd_colstate[0][0]
        if key is None:
            return
        i = self._exd_col_index(key)
        j = i + delta
        if 0 <= j < len(self._exd_colstate):
            self._exd_colstate[i], self._exd_colstate[j] = \
                self._exd_colstate[j], self._exd_colstate[i]
            self._exd_col_layout()                   # animated swap
            self._exd_schedule_render()

    # --- spec assembly + live render ---

    def _exd_spec(self):
        spec = export_designer.default_spec()
        spec["columns"] = [k for k, on in self._exd_colstate if on]
        spec["file_type"] = self._exd_filetype.get()
        spec["delimiter"] = self._EXD_DELIMS.get(self._exd_delim.get(), ",")
        spec["date_format"] = self._exd_datefmt.get()
        spec["date_custom"] = self._exd_datecustom.get() or "%Y-%m-%d %H:%M:%S"
        spec["timezone"] = self._exd_tz.get()
        spec["header"] = bool(self._exd_header.get())
        spec["empty"] = self._exd_empty.get()
        spec["base_interval"] = self._exd_interval.get() or "1m"
        spec["range_preset"] = self._exd_range.get() or "furthest"
        base = stock_storage.base_interval(spec["base_interval"])
        if base.endswith("d"):           # daily has no extended sessions
            spec["sessions"] = ["rth"]
        else:
            spec["sessions"] = self._EXD_SESSIONS.get(
                self._exd_session.get(), ["rth"])
        return spec

    def _exd_on_format_change(self):
        """Adjust dependent control enabled-states, then schedule a render."""
        try:
            ft = self._exd_filetype.get()
            self._exd_delim_cb.config(
                state=("readonly" if ft == "csv" else "disabled"))
            self._exd_datecustom_ent.config(
                state=("normal" if self._exd_datefmt.get() == "custom"
                       else "disabled"))
            base = stock_storage.base_interval(self._exd_interval.get() or "1m")
            self._exd_session_cb.config(
                state=("disabled" if base.endswith("d") else "readonly"))
        except Exception:  # noqa: BLE001 — dialog closing
            pass
        self._exd_schedule_render()

    def _exd_schedule_render(self):
        win = getattr(self, "_exd_win", None)
        if win is None:
            return
        if getattr(self, "_exd_after", None) is not None:
            try:
                win.after_cancel(self._exd_after)
            except Exception:  # noqa: BLE001
                pass
        try:
            self._exd_after = win.after(160, self._exd_render)
        except Exception:  # noqa: BLE001
            self._exd_after = None

    def _exd_sample_ticker(self):
        if self._exd_picked:
            return sorted(self._exd_picked)[0]
        return self._exd_known[0] if self._exd_known else None

    @staticmethod
    def _exd_human_bytes(n):
        f = float(n)
        for u in ("B", "KB", "MB", "GB", "TB"):
            if f < 1024 or u == "TB":
                return f"{int(f)} {u}" if u == "B" else f"{f:.1f} {u}"
            f /= 1024
        return f"{int(n)} B"

    @staticmethod
    def _exd_issue(level, msg, fix=""):
        return {"level": level, "msg": msg, "fix": fix}

    @staticmethod
    def _exd_range_text(preset, sd=None, ed_=None):
        label = "Earliest traceable" if preset == "furthest" else str(preset)
        if sd is not None and ed_ is not None:
            return f"{label}: {sd.isoformat()} to {ed_.isoformat()}"
        return label

    @staticmethod
    def _exd_span_text(sd, ed_):
        if sd is None or ed_ is None:
            return "unknown"
        return f"{sd.isoformat()} to {ed_.isoformat()}"

    def _exd_show_sample_tab(self, tab=None):
        if tab is None:
            var = getattr(self, "_exd_sample_tab", None)
            try:
                tab = var.get() if var is not None else "export"
            except Exception:  # noqa: BLE001
                tab = "export"
        if tab not in ("export", "issues"):
            tab = "export"
        try:
            self._exd_sample_tab.set(tab)
        except Exception:  # noqa: BLE001
            pass
        frames = getattr(self, "_exd_sample_frames", {})
        fr = frames.get(tab)
        if fr is not None:
            try:
                fr.tkraise()
            except Exception:  # noqa: BLE001
                pass
        self._exd_repaint_sample_tabs()

    def _exd_repaint_sample_tabs(self):
        btns = getattr(self, "_exd_sample_tab_btns", {})
        if not btns:
            return
        try:
            cur = self._exd_sample_tab.get()
        except Exception:  # noqa: BLE001
            cur = "export"
        counts = getattr(self, "_exd_issue_counts", {"err": 0, "warn": 0})
        err_n = int(counts.get("err", 0) or 0)
        warn_n = int(counts.get("warn", 0) or 0)
        total_n = err_n + warn_n
        labels = {"export": "Export", "issues": f"Issues ({total_n})"}
        for key, btn in btns.items():
            selected = key == cur
            bg = "#e8f0ff" if selected else "#f6f8fa"
            fg = "#1a1a1a"
            active = "#dce9ff" if selected else "#eef2f6"
            if key == "issues":
                if err_n:
                    bg = "#ffd7d7" if selected else "#ffe8e8"
                    active = "#ffc9c9"
                    fg = "#8a1f11"
                elif warn_n:
                    bg = "#ffe9aa" if selected else "#fff4d6"
                    active = "#ffdf8a"
                    fg = "#6f4b00"
            try:
                btn.config(text=labels[key], bg=bg, fg=fg,
                           activebackground=active, activeforeground=fg,
                           relief=(tk.SOLID if selected else tk.FLAT))
            except Exception:  # noqa: BLE001
                pass

    def _exd_set_empty_tree(self, tv, msg):
        try:
            tv.delete(*tv.get_children())
            tv["columns"] = ("message",)
            tv.heading("message", text="")
            tv.column("message", width=520, anchor=tk.W, stretch=True)
            tv.insert("", tk.END, values=(msg,))
        except Exception:  # noqa: BLE001
            pass

    def _exd_set_export_grid(self, rows, cols, spec, msg, hard_error=False):
        tv = getattr(self, "_exd_sample", None)
        if tv is None:
            return
        try:
            self._exd_sample_msg.config(text=msg)
            tv.configure(show=("headings" if spec.get("header", True) else ""))
            if hard_error or not rows:
                self._exd_set_empty_tree(
                    tv, "Nothing to preview - see Issues" if hard_error else msg)
                return
            rendered = [
                [export_designer._cell(c, row, spec) for c in cols]
                for row in rows
            ]
            widths = _exd_grid_widths(cols, rendered, spec)
            tv.delete(*tv.get_children())
            tv["columns"] = list(cols)
            for c in cols:
                tv.heading(c, text=export_designer.COLUMN_LABEL.get(c, c))
                anchor = tk.W if c in ("timestamp", "ticker") else tk.E
                tv.column(c, width=widths[c], anchor=anchor, stretch=False)
            for i, values in enumerate(rendered):
                tv.insert("", tk.END, tags=(("odd",) if i % 2 else ()),
                          values=values)
        except Exception:  # noqa: BLE001
            pass

    _EXD_SOURCE_FG = {"ok": "#1f6f43", "substituted": "#9a6700",
                      "coverage": "#9a6700", "mismatch": "#b00000",
                      "absent": "#9a6700"}
    _EXD_SOURCE_FONT = ("Segoe UI", 8)

    @staticmethod
    def _exd_months(n):
        return f"{n} month" if n == 1 else f"{n} months"

    def _exd_render_source(self, report):
        """Render the export preview's source line from a manifest-only report.

        Every row is built from ONE price series; other columns are derived
        from it or joined onto it. This states which series won, how each
        column arrives, how many stored volatility months the price range
        cannot emit, and whether the price spine lacks volatility coverage.

        Every selected column is accounted for exactly once, in one of three
        groups: supplied by the spine itself (its timestamps, stored OHLCV,
        ticker), derived from the spine in memory, or joined from a separate
        stored series. Naming the spine SERIES is not the same statement as
        naming the columns it fills, so the spine-supplied group gets its own
        line without claiming every value is byte-for-byte stored.

        Bounded to five short single-purpose lines so it annotates the grid
        rather than displacing it — spine, stored, derived, and one per
        volatility column (max two) — with a sixth only when a real mismatch
        earns the mismatch alert.
        """
        frame = getattr(self, "_exd_source_frame", None)
        if frame is None:
            return
        for lbl in self._exd_source_kinds:
            lbl.pack_forget()
        self._exd_source_stored.pack_forget()
        self._exd_source_derived.pack_forget()
        self._exd_source_alert.pack_forget()
        if not report:
            self._exd_source_spine.pack_forget()
            return
        # A report from an older payload has no "columns" key; treat that as
        # "nothing to attribute" rather than raising inside a preview.
        spine = report["spine"]
        # The caller's session list is a request, not proof those series exist.
        # Derive the rendered suffix from the manifest-backed series that won.
        requested_sessions = list(spine.get("sessions") or [])
        stored_sessions = [
            "pre" if key.endswith("-pre") else
            "post" if key.endswith("-post") else "rth"
            for key in (spine.get("series") or [])
        ]
        extra = "+".join(s for s in stored_sessions if s != "rth")
        missing_sessions = [s for s in requested_sessions
                            if s not in stored_sessions]
        series = spine["interval"] + (f" +{extra}" if extra else "")
        missing_note = (f"   ·   requested {'+'.join(missing_sessions)} not stored"
                        if missing_sessions else "")
        span = (f"{spine['first']} → {spine['last']}" if spine["first"]
                else "no stored months in range")
        self._exd_source_spine.config(
            text=f"spine   {report['ticker']} \\ {series}   ·   "
                 f"{self._exd_months(spine['months_in_range'])}   ·   "
                 f"{span}{missing_note}")
        self._exd_source_spine.pack(side=tk.TOP, fill=tk.X)
        stored = spine.get("columns") or []
        if stored:
            self._exd_source_stored.config(
                text=f"      {', '.join(stored)}   ←   supplied by this spine")
            self._exd_source_stored.pack(side=tk.TOP, fill=tk.X)
        derived = report.get("derived") or []
        if derived:
            names = ", ".join(d["column"] for d in derived)
            self._exd_source_derived.config(
                text=f"      {names}   ←   {derived[0]['how']}")
            self._exd_source_derived.pack(side=tk.TOP, fill=tk.X)
        mismatch = None
        zero_coverage = None
        for lbl, kind in zip(self._exd_source_kinds, report["kinds"]):
            if kind["source"] is None:
                text = (f"      {kind['column']}   ←   {kind['requested']} "
                        "not stored · exports empty")
            else:
                coverage_missing = kind.get("coverage_missing") or []
                covered = kind.get("months_covered", kind["months_in_range"])
                coverage_total = kind.get("spine_months_in_range",
                                          kind["months_in_range"])
                coverage_text = (f"{covered} of "
                                 f"{self._exd_months(coverage_total)}"
                                 if coverage_missing else
                                 self._exd_months(kind["months_in_range"]))
                text = (f"      {kind['column']}   ←   {kind['source']}   ·   "
                        f"{kind['join']}   ·   "
                        f"{coverage_text}")
                if kind["substituted"]:
                    text += f"   ·   for {kind['requested']} (one value a day)"
                if kind["unemittable"]:
                    text += (f"   ·   "
                             f"{self._exd_months(len(kind['unemittable']))} "
                             "unemittable")
            lbl.config(text=text,
                       foreground=self._EXD_SOURCE_FG.get(
                           kind["severity"], "#555"))
            lbl.pack(side=tk.TOP, fill=tk.X)
            if kind["unemittable"] and mismatch is None:
                mismatch = kind
            if (kind["source"] is not None
                    and kind.get("spine_months_in_range", 0)
                    and kind.get("months_covered", kind["months_in_range"]) == 0
                    and zero_coverage is None):
                zero_coverage = kind
        if mismatch is not None:
            months = mismatch["unemittable"]
            self._exd_source_alert.config(
                text=(f"■  {months[0]} → {months[-1]} missing: "
                      f"{mismatch['source']} stores {len(months)} months the "
                      "price series does not, and rows come from price."),
                foreground=self._EXD_SOURCE_FG["mismatch"])
            self._exd_source_alert.pack(side=tk.TOP, fill=tk.X)
        elif zero_coverage is not None:
            total = zero_coverage["spine_months_in_range"]
            self._exd_source_alert.config(
                text=(f"■  no {zero_coverage['column']} coverage: "
                      f"{zero_coverage['source']} has 0 of "
                      f"{self._exd_months(total)} in this spine; "
                      "exported cells are empty."),
                foreground=self._EXD_SOURCE_FG["coverage"])
            self._exd_source_alert.pack(side=tk.TOP, fill=tk.X)

    def _exd_set_issues(self, issues):
        issues = list(issues or [])
        self._exd_sample_issues = issues
        err_n = sum(1 for x in issues if x.get("level") == "err")
        warn_n = sum(1 for x in issues if x.get("level") == "warn")
        self._exd_issue_counts = {"err": err_n, "warn": warn_n}
        tv = getattr(self, "_exd_issues", None)
        if tv is not None:
            try:
                tv.delete(*tv.get_children())
                for issue in issues:
                    level = str(issue.get("level", "ok"))
                    chip = {"err": "ERR", "warn": "WARN", "ok": "OK"}.get(
                        level, level.upper())
                    tv.insert("", tk.END, tags=(level,),
                              values=(chip, issue.get("msg", ""),
                                      issue.get("fix", "")))
            except Exception:  # noqa: BLE001
                pass
        msg = (f"{err_n} error(s), {warn_n} warning(s)"
               if err_n or warn_n else "No preview issues.")
        try:
            self._exd_issues_msg.config(text=msg)
        except Exception:  # noqa: BLE001
            pass
        self._exd_repaint_sample_tabs()

    def _exd_set_grid(self, rows, cols, spec, msg=""):
        """Paint the live-sample GRID: one Treeview column per selected data
        column, each cell formatted EXACTLY as the file would write it
        (export_designer._cell honours the date format / tz / empty-value spec)."""
        self._exd_set_export_grid(rows, cols, spec, msg)

    def _exd_sample_bars(self, tkr, spec):
        """First in-range price bars for `tkr`, CACHED per scope. A format-only
        change (delimiter/date/tz/columns/…) reuses the cached bars and never
        re-reads the disk on the GUI thread; only a SCOPE change (ticker /
        interval / sessions / range) re-reads. Returns (bars, start, end) or
        (None, None, None) when the ticker has no stored data."""
        key = (tkr, spec["base_interval"], tuple(spec["sessions"]),
               spec["range_preset"])
        if getattr(self, "_exd_sample_key", None) == key:
            return self._exd_sample_cache
        span_start, span_end = export_designer.ticker_span(
            self._storage_root, tkr, spec["base_interval"], spec["sessions"])
        self._exd_sample_span = (span_start, span_end)
        if span_start is None:
            val = (None, None, None)
        else:
            sd, ed_ = export_csv.preset_range(
                spec["range_preset"], span_end, span_start, span_end)
            bars = export_designer.sample_price_bars(
                self._storage_root, tkr, spec["base_interval"],
                spec["sessions"], sd, ed_, n=12)
            val = (bars, sd, ed_)
        self._exd_sample_key = key
        self._exd_sample_cache = val
        return val

    def _exd_preview_content(self, spec, cols, source=None):
        """Build one preview payload without mixing synthetic and bank paths.

        The default is intentionally ``Sample fake`` (renamed from ``All
        variations`` 2026-07-31 at the user's request; it is preview-only
        invented data covering every variation). That branch calls only the
        in-code sample generator; it cannot touch the bank loader or poison its
        scope cache. ``Bank data`` retains the previous sample/build/cache path
        unchanged. Both branches return rows for the same grid renderer.

        The branch test below is ``!= "Bank data"``, so a future rename of the
        synthetic label can never accidentally route a preview at the bank.
        """
        if source is None:
            source_var = getattr(self, "_exd_sample_source", None)
            source = (source_var.get() if source_var is not None
                      else self._EXD_SAMPLE_SOURCES[0])
        if source != "Bank data":
            rows, tags = export_designer.synthetic_sample_rows(spec)
            return {
                "rows": list(rows)[:200],
                "variation_tags": list(tags)[:200],
                "issues": [],
                "msg": (f"Sample fake  ·  invented rows, not your bank  ·  "
                        f"{spec['base_interval']}  ·  {spec['file_type']}"
                        + ("   (binary Parquet — shown as the table it encodes)"
                           if spec["file_type"] == "parquet" else "")),
                "hard_error": False,
                "ticker": None,
                "label": "Sample fake",
            }

        issues = []
        rows = []
        msg = ""
        hard_error = False
        tkr = self._exd_sample_ticker()
        if tkr is None:
            hard_error = True
            issues.append(self._exd_issue(
                "err", "No tickers in the bank to preview.",
                "Add a stock first."))
        else:
            bars, sd, ed_ = self._exd_sample_bars(tkr, spec)
            if sd is None:
                hard_error = True
                issues.append(self._exd_issue(
                    "err", f"No stored {spec['base_interval']} data for {tkr}.",
                    "Fetch it, or pick another interval."))
            elif not bars:
                hard_error = True
                rng = self._exd_range_text(spec["range_preset"], sd, ed_)
                span = self._exd_span_text(
                    *getattr(self, "_exd_sample_span", (None, None)))
                issues.append(self._exd_issue(
                    "err", f"No {tkr} rows in the selected range ({rng}).",
                    "Widen the range or pick Earliest traceable; "
                    f"stored span is {span}."))
            else:
                msg = (f"{tkr}  ·  {spec['base_interval']}  ·  "
                       f"{spec['file_type']}"
                       + ("   (binary Parquet — shown as the table it encodes)"
                          if spec["file_type"] == "parquet" else ""))
                rows = export_designer.build_rows(
                    bars, tkr, cols, root=self._storage_root,
                    base_iv=spec["base_interval"])[:200]
                from datetime import date as _date
                if "volume" in cols and sd < _date(2014, 1, 1):
                    issues.append(self._exd_issue(
                        "warn",
                        "Volume for pre-2014 bars is split-adjusted, not raw "
                        "share counts (known IBKR-demo deep-history behavior) "
                        "- affects the volume column only.",
                        "Use price columns normally; treat pre-2014 volume as "
                        "adjusted."))
        if hard_error:
            msg = "Nothing to preview - see Issues"
        # Manifest-only provenance for the source line: which stored series
        # actually feeds each column, and where a kind holds months the price
        # spine cannot emit. Never reads bars, so it is safe per keystroke.
        source_report = {}
        if tkr and not hard_error:
            try:
                source_report = export_designer.source_report(
                    self._storage_root, tkr, spec["base_interval"],
                    spec["sessions"], cols, sd, ed_)
            except Exception:  # noqa: BLE001 - provenance is never a gate
                source_report = {}
        return {
            "rows": list(rows),
            "variation_tags": [],
            "issues": issues,
            "msg": msg,
            "hard_error": hard_error,
            "ticker": tkr,
            "label": tkr,
            "source_report": source_report,
        }

    def _exd_render(self):
        self._exd_after = None
        win = getattr(self, "_exd_win", None)
        if win is None or not win.winfo_exists():
            return
        spec = self._exd_spec()
        cols = spec["columns"]
        _rows, issues = [], []
        _msg = ""
        hard_error = False
        tkr = None
        preview_label = None
        variation_tags = []
        source_report = {}
        try:
            if not cols:
                hard_error = True
                issues.append(self._exd_issue(
                    "err", "Tick at least one column to preview.",
                    "Tick a column in Data > Columns."))
            else:
                preview = self._exd_preview_content(spec, cols)
                _rows = preview["rows"]
                variation_tags = preview["variation_tags"]
                issues.extend(preview["issues"])
                _msg = preview["msg"]
                hard_error = preview["hard_error"]
                tkr = preview["ticker"]
                preview_label = preview["label"]
                source_report = preview.get("source_report") or {}
        except Exception as exc:  # noqa: BLE001
            hard_error = True
            issues.append(self._exd_issue(
                "err", f"Preview unavailable: {exc}",
                "Check the selected ticker, interval, and format, then retry."))
        self._exd_render_source({} if hard_error else source_report)
        if hard_error:
            _msg = "Nothing to preview - see Issues"
        # estimate — file/row counts cached per scope; bytes recompute O(1)
        est_rows_for_issue = None
        try:
            tickers = sorted(self._exd_picked)
            if not tickers:
                st = self._exd_sample_ticker()
                tickers = [st] if st else []
            ekey = (tuple(tickers), spec["base_interval"],
                    tuple(spec["sessions"]), spec["range_preset"])
            if getattr(self, "_exd_est_key", None) != ekey:
                est = export_designer.estimate(
                    self._storage_root, tickers, spec["base_interval"],
                    spec["sessions"], None, None, spec, preset=spec["range_preset"])
                self._exd_est_key = ekey
                self._exd_est_cache = (est["files"], est["rows"])
            files, rows = self._exd_est_cache
            est_rows_for_issue = rows
            nbytes = export_designer.estimate_bytes(rows, files, spec)
            self._exd_estimate.set(
                f"est: {files} file(s) · ~{self._exd_human_bytes(nbytes)}"
                f" · {rows:,} rows"
                + ("" if self._exd_picked else "  (preview ticker only — pick tickers)"))
        except Exception:  # noqa: BLE001
            self._exd_estimate.set("est: —")
        if _rows and not any(i.get("level") in ("err", "warn") for i in issues):
            if preview_label == "Sample fake":
                ready_msg = (f"Ready - {preview_label} · "
                             f"{spec['base_interval']} · {spec['file_type']} · "
                             f"{len(_rows)} fake rows shown.")
                ready_fix = ("Export tab uses the real file formatter; choose "
                             "Bank data to inspect stored rows.")
            else:
                total = (f"{est_rows_for_issue:,}"
                         if isinstance(est_rows_for_issue, int)
                         else "the estimate")
                ready_msg = (f"Ready - {tkr} · {spec['base_interval']} · "
                             f"{spec['file_type']} · {len(_rows)} shown of "
                             f"{total}.")
                ready_fix = ("The Export tab matches the file; the source "
                             "line above it names the series behind each "
                             "column.")
            issues.append(self._exd_issue(
                "ok", ready_msg, ready_fix))
        self._exd_last_preview = {
            "issues": list(issues),
            "export_rows": list(_rows),
            "variation_tags": list(variation_tags),
            "hard_error": hard_error,
            "ticker": tkr,
            "label": preview_label,
        }
        self._exd_set_export_grid(_rows, cols, spec, _msg, hard_error)
        self._exd_set_issues(issues)
        # export button label
        try:
            n = len(self._exd_picked)
            self._exd_export_btn.config(
                text=("Export file…" if n == 1 else f"Export {n} files…")
                if n else "Export…")
        except Exception:  # noqa: BLE001
            pass

    # --- ticker picker popup ---

    def _exd_open_picker(self):
        if getattr(self, "_exd_pick_win", None) is not None:
            try:
                if self._exd_pick_win.winfo_exists():
                    self._exd_pick_win.lift()
                    return
            except Exception:  # noqa: BLE001
                pass
        pop = tk.Toplevel(self._exd_win)
        pop.title("Choose tickers to export")
        pop.geometry("440x540")
        pop.transient(self._exd_win)
        self._exd_pick_win = pop
        pop.protocol("WM_DELETE_WINDOW", self._exd_pick_done)
        top = ttk.Frame(pop, padding=(10, 8))
        top.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(top, text="Filter:").pack(side=tk.LEFT)
        self._exd_pick_filter = tk.StringVar(value="")
        ttk.Entry(top, textvariable=self._exd_pick_filter).pack(
            side=tk.LEFT, fill=tk.X, expand=True, padx=(4, 0))
        self._exd_pick_filter.trace_add(
            "write", lambda *_a: self._exd_pick_render())
        row = ttk.Frame(pop, padding=(10, 0))
        row.pack(side=tk.TOP, fill=tk.X)
        ttk.Button(row, text="Select all",
                   command=lambda: self._exd_pick_bulk(True)).pack(side=tk.LEFT)
        ttk.Button(row, text="Clear",
                   command=lambda: self._exd_pick_bulk(False)).pack(
            side=tk.LEFT, padx=(6, 0))
        ttk.Button(row, text="S&P 500 ✦",
                   command=self._exd_pick_sp500).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(row, text="All stored",
                   command=self._exd_pick_all_stored).pack(side=tk.LEFT, padx=(6, 0))
        midf = ttk.Frame(pop, padding=(10, 6))
        midf.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self._exd_pick_lb = tk.Listbox(midf, selectmode="browse",
                                       font=("Consolas", 10), activestyle="none",
                                       exportselection=False)
        psb = ttk.Scrollbar(midf, command=self._exd_pick_lb.yview)
        self._exd_pick_lb.config(yscrollcommand=psb.set)
        psb.pack(side=tk.RIGHT, fill=tk.Y)
        self._exd_pick_lb.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._exd_pick_lb.bind("<<ListboxSelect>>", self._exd_pick_click)
        self._exd_pick_status = tk.StringVar(value="")
        ttk.Label(pop, textvariable=self._exd_pick_status, foreground="#555",
                  padding=(10, 2)).pack(side=tk.TOP, fill=tk.X)
        bot = ttk.Frame(pop, padding=(10, 8))
        bot.pack(side=tk.BOTTOM, fill=tk.X)
        ttk.Button(bot, text="Done", command=self._exd_pick_done).pack(side=tk.RIGHT)
        self._exd_pick_render()
        try:
            pop.wait_visibility()
            pop.grab_set()
        except Exception:  # noqa: BLE001
            pass

    def _exd_pick_visible(self):
        q = (self._exd_pick_filter.get() or "").strip().upper()
        return [t for t in self._exd_known if not q or q in t]

    def _exd_pick_render(self):
        lb = getattr(self, "_exd_pick_lb", None)
        if lb is None:
            return
        self._exd_pick_view = self._exd_pick_visible()
        lb.delete(0, tk.END)
        for t in self._exd_pick_view:
            lb.insert(tk.END, f"{'☑' if t in self._exd_picked else '☐'} {t}")
        extra = f" · {self._exd_pick_src}" if self._exd_pick_src else ""
        self._exd_pick_status.set(
            f"{len(self._exd_picked)} / {len(self._exd_known)} selected{extra}")

    def _exd_pick_click(self, _evt=None):
        lb = self._exd_pick_lb
        sel = lb.curselection()
        if not sel:
            return
        i = sel[0]
        if i < len(getattr(self, "_exd_pick_view", [])):
            t = self._exd_pick_view[i]
            if t in self._exd_picked:
                self._exd_picked.discard(t)
            else:
                self._exd_picked.add(t)
        self._exd_pick_render()

    def _exd_pick_bulk(self, value):
        for t in self._exd_pick_visible():
            if value:
                self._exd_picked.add(t)
            else:
                self._exd_picked.discard(t)
        self._exd_pick_render()

    def _exd_pick_all_stored(self):
        self._exd_picked = set(self._exd_known)
        self._exd_pick_src = "All stored"
        self._exd_pick_render()

    def _exd_pick_sp500(self):
        self._exd_pick_status.set("Fetching the current S&P 500…")
        self._current_sp500_async(self._exd_pick_sp500_apply)

    def _exd_pick_sp500_apply(self, syms, source, asof):
        have = set(self._exd_known)
        inter = [s for s in syms if s in have]
        self._exd_picked |= set(inter)
        self._exd_pick_src = ("S&P 500: "
                              + self._sp500_src_label(source, asof, len(syms))
                              + f" — {len(inter)} of your {len(have)} stored")
        if getattr(self, "_exd_pick_win", None) is not None:
            try:
                if self._exd_pick_win.winfo_exists():
                    self._exd_pick_render()
            except Exception:  # noqa: BLE001
                pass

    def _exd_pick_done(self):
        pop = getattr(self, "_exd_pick_win", None)
        if pop is not None:
            try:
                pop.grab_release()
            except Exception:  # noqa: BLE001
                pass
            try:
                pop.destroy()
            except Exception:  # noqa: BLE001
                pass
        self._exd_pick_win = None
        self._exd_update_tickers_btn()
        self._exd_schedule_render()

    def _exd_update_tickers_btn(self):
        try:
            self._exd_tickers_btn.config(
                text=f"Tickers [{len(self._exd_picked)} sel ▾]")
        except Exception:  # noqa: BLE001
            pass

    # --- presets ---

    def _exd_preset_path(self):
        return Path(self._storage_root).parent / "export_presets.json"

    def _exd_collect_preset(self):
        spec = self._exd_spec()
        keep = ("columns", "file_type", "delimiter", "date_format",
                "date_custom", "timezone", "header", "empty", "base_interval",
                "range_preset")
        out = {k: spec[k] for k in keep}
        out["session_label"] = self._exd_session.get()
        return out

    def _exd_save_preset(self):
        from tkinter import simpledialog
        name = simpledialog.askstring("Save preset", "Preset name:",
                                      parent=self._exd_win)
        if not name:
            return
        try:
            import json
            p = self._exd_preset_path()
            data = {}
            if p.exists():
                data = json.loads(p.read_text(encoding="utf-8"))
            data[name] = self._exd_collect_preset()
            p.write_text(json.dumps(data, indent=1), encoding="utf-8")
            self._exd_status.set(f"Saved preset '{name}'.")
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("Preset", f"Could not save preset:\n{exc}",
                                   parent=self._exd_win)

    def _exd_load_preset_menu(self):
        import json
        try:
            p = self._exd_preset_path()
            data = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        except Exception:  # noqa: BLE001
            data = {}
        m = tk.Menu(self._exd_win, tearoff=0)
        if not data:
            m.add_command(label="(no saved presets)", state="disabled")
        for name in sorted(data):
            m.add_command(label=name,
                          command=lambda n=name: self._exd_apply_preset(data[n]))
        try:
            m.tk_popup(self._exd_win.winfo_pointerx(),
                       self._exd_win.winfo_pointery())
        finally:
            m.grab_release()

    def _exd_apply_preset(self, p):
        try:
            cols = list(p.get("columns") or export_designer.DEFAULT_COLUMNS)
            order = [c for c in cols if c in export_designer.ALL_COLUMNS]
            rest = [c for c in export_designer.ALL_COLUMNS if c not in order]
            self._exd_colstate = [[c, c in cols] for c in (order + rest)]
            self._exd_render_columns()
            self._exd_filetype.set(p.get("file_type", "csv"))
            inv = {",": "comma", ";": "semicolon", "|": "pipe", "\t": "tab"}
            self._exd_delim.set(inv.get(p.get("delimiter", ","), "comma"))
            self._exd_datefmt.set(p.get("date_format", "iso"))
            self._exd_datecustom.set(p.get("date_custom", "%Y-%m-%d %H:%M:%S"))
            self._exd_tz.set(p.get("timezone", "America/New_York"))
            self._exd_header.set(bool(p.get("header", True)))
            self._exd_empty.set(p.get("empty", "blank"))
            if p.get("base_interval"):
                self._exd_interval.set(p["base_interval"])
            if p.get("range_preset"):
                self._exd_range.set(p["range_preset"])
            if p.get("session_label"):
                self._exd_session.set(p["session_label"])
            self._exd_on_format_change()
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("Preset", f"Could not apply preset:\n{exc}",
                                   parent=self._exd_win)

    # --- export ---

    def _exd_confirm_missing(self, present, missing):
        if not missing:
            return True
        total = len(present) + len(missing)
        if len(missing) >= 5:
            msg = (f"{len(missing)} of {total} selected tickers have no stored "
                   f"data — they'll be SKIPPED (nothing to export).\n\n"
                   f"Continue exporting the {len(present)} stored one(s)?")
        else:
            names = "\n".join(f"   • {m}" for m in missing)
            msg = (f"{len(missing)} selected ticker(s) have no stored data and "
                   f"will be SKIPPED:\n\n{names}\n\nContinue exporting the "
                   f"{len(present)} stored one(s)?")
        return messagebox.askokcancel("Not in your data bank", msg,
                                      parent=self._exd_win)

    def _exd_export(self):
        if self._storage_busy():
            self._exd_status.set("Busy — wait for the current task to finish.")
            return
        spec = self._exd_spec()
        if not spec["columns"]:
            messagebox.showwarning("Export", "Tick at least one column first.",
                                   parent=self._exd_win)
            return
        picked = sorted(self._exd_picked)
        if not picked:
            messagebox.showwarning("Export",
                                   "Choose at least one ticker (Tickers ▾).",
                                   parent=self._exd_win)
            return
        present, missing = export_designer.missing_from_bank(
            self._storage_root, picked)
        if not self._exd_confirm_missing(present, missing):
            return
        if not present:
            messagebox.showwarning(
                "Export", "None of the selected tickers are in the bank.",
                parent=self._exd_win)
            return
        desk = Path.home() / "Desktop"
        initial = str(desk if desk.exists() else self._storage_root.parent)
        folder = filedialog.askdirectory(
            parent=self._exd_win, title="Choose an export folder",
            initialdir=initial, mustexist=True)
        if not folder:
            return
        destination = Path(folder)

        self._storage_exd_running = True
        try:
            self._exd_export_btn.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001
            pass
        try:
            self._exd_cancel_btn.config(state=tk.NORMAL)
        except Exception:  # noqa: BLE001
            pass
        self._export_progress_reset(
            "exd", f"Exporting {len(present)} file(s)…")
        self._export_progress_pulse_start("exd")
        import queue
        import threading
        cancel_ev = threading.Event()
        q = queue.Queue()
        self._exd_q = q
        self._exd_cancel_ev = cancel_ev
        root_dir = self._storage_root
        spec_w = dict(spec)
        progress_mailbox = _ExportProgressCoalescer(q)
        self._exd_progress_coalescer = progress_mailbox

        def _post_progress(event, force=False):
            if getattr(self, "_exd_q", None) is not q:
                return
            terminal = False
            if isinstance(event, dict) and event.get("kind") == "aggregate":
                try:
                    files_total = int(event.get("files_total", 0) or 0)
                    terminal = (files_total > 0
                                and int(event.get("files_done", 0) or 0)
                                >= files_total)
                except (TypeError, ValueError):
                    terminal = False
            progress_mailbox.post(event, force=bool(force or terminal))

        def _work():
            payload = None
            try:
                _post_progress({
                    "kind": "health",
                    "message": "Checking bank health for export…",
                }, force=True)
                export_quality.ensure_current_health(root_dir)
                _post_progress({
                    "kind": "health",
                    "message": ("Bank health current; "
                                f"{EXPORT_HEALTH_REPORT_NAME} will be bundled…"),
                }, force=True)
                payload = export_batch.run_designer_batch(
                    root_dir, picked, present, destination, spec_w,
                    progress=_post_progress, cancel=cancel_ev)
            except Exception as exc:  # noqa: BLE001
                payload = exc
            finally:
                try:
                    q.put(("done", payload))
                except Exception:  # noqa: BLE001
                    pass

        try:
            threading.Thread(target=_work, daemon=True,
                             name="export-designer").start()
        except Exception as exc:  # noqa: BLE001
            self._exd_export_done(exc)
            return
        self._exd_export_after = self.root.after(100, self._exd_export_poll)

    def _exd_cancel_export(self):
        ev = getattr(self, "_exd_cancel_ev", None)
        if ev is None:
            return
        ev.set()
        self._export_progress_freeze("exd")
        try:
            self._exd_cancel_btn.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001
            pass
        try:
            self._exd_status.set("Cancelling export at next safe boundary...")
        except Exception:  # noqa: BLE001
            pass

    def _exd_export_poll(self):
        self._exd_export_after = None
        q = getattr(self, "_exd_q", None)
        if q is None:
            return
        win = getattr(self, "_exd_win", None)
        try:
            exists = bool(win is not None and win.winfo_exists())
        except Exception:  # noqa: BLE001
            exists = False
        if not exists:
            ev = getattr(self, "_exd_cancel_ev", None)
            if ev is not None:
                ev.set()
            self._exd_q = None
            self._exd_cancel_ev = None
            self._exd_progress_coalescer = None
            self._storage_exd_running = False
            return
        import queue
        try:
            while True:
                kind, payload = q.get_nowait()
                if kind in {"aggregate", "activity"}:
                    event = payload.take(kind)
                    if event is not None:
                        self._export_progress_apply("exd", event)
                else:
                    self._exd_q = None
                    self._exd_export_done(payload)
                    return
        except queue.Empty:
            pass
        except Exception as exc:  # noqa: BLE001
            self._exd_q = None
            self._exd_export_done(RuntimeError(f"display failed: {exc}"))
            return
        try:
            if getattr(self, "_exd_q", None) is q:
                self._exd_export_after = self.root.after(
                    100, self._exd_export_poll)
        except Exception:  # noqa: BLE001
            pass

    def _exd_export_done(self, payload):
        self._storage_exd_running = False
        self._exd_q = None
        self._exd_cancel_ev = None
        self._exd_progress_coalescer = None
        self._exd_export_after = None
        self._export_progress_finish("exd")
        try:
            self._exd_export_btn.config(state=tk.NORMAL)
        except Exception:  # noqa: BLE001
            pass
        try:
            self._exd_cancel_btn.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001
            pass
        if isinstance(payload, Exception):
            self._exd_status.set(f"Export failed: {payload}")
            try:
                messagebox.showwarning(
                    "Export", f"Export failed:\n{payload}",
                    parent=getattr(self, "_exd_win", None) or self.root)
            except Exception:  # noqa: BLE001
                pass
            return
        results = payload.get("results") or []
        folder = payload.get("bundle_folder") or payload.get("folder", "")
        ok = tot = empty = failed = cancelled = absent = 0
        lines = []
        for r in results:
            t = r.get("ticker", "?")
            if r.get("not_in_bank"):
                absent += 1
                lines.append(f"  - {t}: not in bank")
            elif r.get("cancelled"):
                cancelled += 1
                lines.append(f"  - {t}: cancelled")
            elif r.get("error"):
                failed += 1
                lines.append(f"  ✗ {t}: {r['error']}")
            elif r.get("rows", 0) > 0:
                ok += 1
                tot += r["rows"]
                hole = (f"  (missing {len(r['holes'])} month(s))"
                        if r.get("holes") else "")
                lines.append(f"  ✓ {t}: {r['rows']:,} rows{hole}")
            else:
                empty += 1
                lines.append(f"  – {t}: no rows in range")
        head = (f"Exported {ok} file(s) ({tot:,} rows total) to {folder}."
                + (f"  {empty} empty, {failed} failed, {absent} absent."
                   if (empty or failed or absent) else ""))
        if cancelled:
            head = f"Export cancelled.  {head}  {cancelled} cancelled."
        if payload.get("folder_removed"):
            head += "  Empty batch folder removed."
        note_line = ""
        note_path = payload.get("note_path")
        note_error = payload.get("note_error")
        quality = payload.get("quality_summary") or {}
        health_path = payload.get("health_report")
        health_line = (f"Health report: {Path(health_path).name}"
                       if health_path else "")
        if note_path:
            note_line = (f"Quality note: {Path(note_path).name}  "
                         f"confirmed bad={quality.get('CONFIRMED_BAD', 0)}, "
                         f"review={quality.get('REVIEW_REQUIRED', 0)}, "
                         f"known={quality.get('KNOWN_DIFFERENCE', 0)}, "
                         f"unverifiable={quality.get('UNVERIFIABLE', 0)}, "
                         f"stale={quality.get('NO_CURRENT_EVIDENCE', 0)}.")
        elif note_error:
            note_line = f"Quality note FAILED: {note_error}"
        self._exd_status.set(
            head + (("  " + health_line) if health_line else "")
            + (("  " + note_line) if note_line else ""))
        try:
            body = head + "\n\n" + "\n".join(lines[:25])
            if health_line:
                body += "\n\n" + health_line
            if note_line:
                body += "\n\n" + note_line
            messagebox.showinfo("Export", body,
                                parent=getattr(self, "_exd_win", None) or self.root)
        except Exception:  # noqa: BLE001
            pass

    def _exd_close(self):
        win = getattr(self, "_exd_win", None)
        if getattr(self, "_exd_after", None) is not None and win is not None:
            try:
                win.after_cancel(self._exd_after)
            except Exception:  # noqa: BLE001
                pass
        self._exd_after = None
        ev = getattr(self, "_exd_cancel_ev", None)
        if ev is not None:
            ev.set()
        job = getattr(self, "_exd_export_after", None)
        if job is not None:
            try:
                self.root.after_cancel(job)
            except Exception:  # noqa: BLE001
                pass
        self._exd_export_after = None
        self._exd_q = None
        self._exd_cancel_ev = None
        self._exd_progress_coalescer = None
        self._export_progress_finish("exd")
        self._storage_exd_running = False
        for _job in list(getattr(self, "_exd_col_jobs", {}).values()):
            try:
                if win is not None:
                    win.after_cancel(_job)
            except Exception:  # noqa: BLE001
                pass
        self._exd_col_jobs = {}
        pop = getattr(self, "_exd_pick_win", None)
        if pop is not None:
            try:
                pop.destroy()
            except Exception:  # noqa: BLE001
                pass
        self._exd_pick_win = None
        if win is not None:
            try:
                win.destroy()
            except Exception:  # noqa: BLE001
                pass
        self._exd_win = None

    # ---- Find / add stock -------------------------------------------------

    def _addstock_archive_dir(self):
        return Path(self._storage_root).resolve().parent / "Run Logs"

    def _addstock_refresh_debt_indicator(self) -> None:
        """Refresh the small persistent Add Stocks recovery/debt count."""
        label = getattr(self, "_addstock_debt_status", None)
        if label is None:
            return
        try:
            data = addstock_run_manifest.load_run(self._storage_root)
            if data is None:
                text = ""
            else:
                status = addstock_run_manifest.summary(data)
                parts = []
                if status["pending"]:
                    parts.append(f"{status['pending']} pending")
                if status["built_unverified"]:
                    parts.append(
                        f"{status['built_unverified']} verification debt")
                text = "Add Stocks: " + (", ".join(parts) or data["state"])
        except addstock_run_manifest.ManifestError:
            text = "Add Stocks recovery needs attention"
        try:
            label.set(text)
        except Exception:  # noqa: BLE001 - storage tab may be closing
            pass

    def _addstock_current_xval_intervals(self, entries):
        return addstock_run_manifest.current_xval_intervals(
            self._storage_root, entries)

    def _addstock_current_gap_intervals(self, ticker, intervals):
        try:
            evidence = stock_validate.evaluate_gap_evidence(
                self._storage_root,
                [(ticker, interval) for interval in intervals or ()])
            return [row["interval"] for row in evidence.get("rows") or []
                    if row.get("current") is True]
        except Exception:  # noqa: BLE001 - failed evidence remains debt
            return []

    def _addstock_interval_empty_on_disk(self, ticker, interval):
        """Return True only when strict storage evidence proves no data."""
        try:
            summary = stock_storage.optional_interval_storage_summary(
                self._storage_root, ticker, interval)
            return bool(
                summary.get("present_month_count") == 0
                and summary.get("format_error_month_count") == 0
                and summary.get("backfill_incomplete") is False)
        except Exception:  # noqa: BLE001 - uncertainty must retain the debt
            return False

    def _addstock_reconcile_evidence(self, data):
        """Credit only still-current sidecars after a process interruption."""
        if data is None:
            raise addstock_run_manifest.RunMismatch(
                "active recovery disappeared before evidence reconciliation")
        run_id = data["run_id"]

        def _archive_completed(result):
            if not (isinstance(result, dict) and result.get("archived")):
                return False
            active = addstock_run_manifest.load_run(self._storage_root)
            if active is not None:
                raise addstock_run_manifest.RunMismatch(
                    "another active recovery appeared after completion")
            return True

        xval = stock_validate.load_cross_validation(self._storage_root)
        earliest = stock_validate.load_ibkr_earliest(self._storage_root)
        gap_series = []
        xval_entries = {}
        for ticker, record in data["tickers"].items():
            for interval, series in record["series"].items():
                if (series["state"] != "complete"
                        or stock_storage.session_of(interval) != "rth"):
                    continue
                if not series["xval"]:
                    key = stock_validate.cross_validation_key(ticker, interval)
                    entry = xval.get(key)
                    if isinstance(entry, dict):
                        xval_entries.setdefault(ticker, []).append(entry)
                if not series["gaps"]:
                    gap_series.append((ticker, interval))
        for ticker, entries in xval_entries.items():
            intervals = self._addstock_current_xval_intervals(entries)
            if intervals:
                result = self._addstock_credit(
                    ticker, "xval", intervals, run_id=run_id)
                if _archive_completed(result):
                    return None
        if gap_series:
            try:
                evidence = stock_validate.evaluate_gap_evidence(
                    self._storage_root, gap_series)
            except Exception:  # noqa: BLE001 - unreadable evidence stays debt
                evidence = None
            if evidence is not None:
                grouped = {}
                for row in evidence.get("rows") or []:
                    if row.get("current") is True:
                        grouped.setdefault(row["ticker"], []).append(
                            row["interval"])
                for ticker, intervals in grouped.items():
                    result = self._addstock_credit(
                        ticker, "gaps", intervals, run_id=run_id)
                    if _archive_completed(result):
                        return None
        for ticker, record in data["tickers"].items():
            if not record["earliest"] and earliest.get(ticker):
                result = self._addstock_credit(
                    ticker, "earliest", run_id=run_id)
                if _archive_completed(result):
                    return None
        current = addstock_run_manifest.load_run(self._storage_root)
        if current is None:
            raise addstock_run_manifest.RunMismatch(
                "active recovery disappeared; completion was not proven")
        if current.get("run_id") != run_id:
            raise addstock_run_manifest.RunMismatch(
                "active recovery changed during evidence reconciliation")
        return current

    def _addstock_credit(self, ticker, stage, intervals=None, run_id=None):
        """Credit one completed verification stage without affecting its owner."""
        try:
            if run_id is None:
                data = addstock_run_manifest.load_run(self._storage_root)
                if data is None:
                    return None
                run_id = data["run_id"]
            result = addstock_run_manifest.mark_verification(
                self._storage_root, run_id, ticker, stage, intervals,
                archive_dir=self._addstock_archive_dir())
        except addstock_run_manifest.ManifestError:
            return None
        try:
            self.root.after(0, self._addstock_refresh_debt_indicator)
        except Exception:  # noqa: BLE001
            pass
        return result

    def _addstock_series_started(self, run_id, ticker, interval):
        try:
            addstock_run_manifest.mark_series_started(
                self._storage_root, run_id, ticker, interval)
        except addstock_run_manifest.ManifestError:
            return
        try:
            self.root.after(0, self._addstock_refresh_debt_indicator)
        except Exception:  # noqa: BLE001
            pass

    def _addstock_series_complete(self, run_id, ticker, interval):
        empty = self._addstock_interval_empty_on_disk(ticker, interval)
        try:
            addstock_run_manifest.mark_series_complete(
                self._storage_root, run_id, ticker, interval,
                empty=empty)
        except addstock_run_manifest.ManifestError:
            return
        if empty and stock_storage.session_of(str(interval)) == "rth":
            note = (
                f"{ticker} {interval}: empty series — no data to verify "
                "(kind not computable with current history); "
                "verification excused")
            try:
                self.root.after(
                    0, lambda message=note:
                    self._storage_find_say(message))
            except Exception:  # noqa: BLE001 - dialog may have closed
                pass
        try:
            self.root.after(0, self._addstock_refresh_debt_indicator)
        except Exception:  # noqa: BLE001
            pass

    def _addstock_finish_manifest(self, run_id, report):
        if not run_id:
            return None
        try:
            if isinstance(report, dict):
                for row in report.get("series") or []:
                    if not row.get("halt"):
                        continue
                    try:
                        addstock_run_manifest.mark_series_pending(
                            self._storage_root, run_id,
                            row.get("ticker"), row.get("interval"),
                            note=str(row.get("halt") or "series incomplete"))
                    except addstock_run_manifest.ManifestError:
                        continue
                if report.get("fleet_down_finalize"):
                    reason = "fleet_down"
                elif report.get("cancelled"):
                    reason = "user_cancel"
                elif report.get("aborted"):
                    reason = "aborted"
                else:
                    reason = "verification_debt"
                seal_pending = report.get("seal_pending") or []
            else:
                reason = "worker_error"
                seal_pending = []
            result = addstock_run_manifest.mark_fetch_finished(
                self._storage_root, run_id, reason=reason,
                seal_pending=seal_pending,
                archive_dir=self._addstock_archive_dir())
        except addstock_run_manifest.ManifestError:
            result = None
        try:
            self.root.after(0, self._addstock_refresh_debt_indicator)
        except Exception:  # noqa: BLE001
            pass
        return result

    def _addstock_offer_recovery(self) -> None:
        """Offer Resume or explicit Discard for durable unfinished intent."""
        self._storage_find_set_recovery_blocked(True)
        try:
            data = addstock_run_manifest.load_run(self._storage_root)
        except addstock_run_manifest.ManifestError as exc:
            try:
                discard = messagebox.askyesno(
                    "Add Stocks recovery file is invalid",
                    "The durable Add Stocks recovery file is torn, oversized, "
                    f"or invalid:\n\n{exc}\n\nDiscard it explicitly? Its original "
                    "bytes will be preserved under Run Logs.",
                    parent=self._storage_find_win)
            except Exception:  # noqa: BLE001
                discard = False
            if discard:
                try:
                    target = addstock_run_manifest.discard_active(
                        self._storage_root, self._addstock_archive_dir())
                    self._storage_find_set_recovery_blocked(False)
                    self._storage_find_say(
                        f"Invalid recovery state discarded to {target}.")
                except addstock_run_manifest.ManifestError as discard_exc:
                    self._storage_find_set_recovery_blocked(True)
                    self._storage_find_say(
                        f"Could not discard recovery state: {discard_exc}")
            else:
                try:
                    self._storage_find_build_btn.config(state=tk.DISABLED)
                except Exception:  # noqa: BLE001
                    pass
                self._storage_find_say(
                    "New builds are blocked until the invalid recovery state "
                    "is explicitly discarded.")
            self._addstock_refresh_debt_indicator()
            return
        if data is None:
            self._storage_find_set_recovery_blocked(False)
            self._addstock_refresh_debt_indicator()
            return
        if data["state"] == "complete":
            try:
                target = addstock_run_manifest.archive_complete(
                    self._storage_root, data["run_id"],
                    self._addstock_archive_dir())
                self._storage_find_set_recovery_blocked(False)
                self._storage_find_say(
                    f"Completed recovery state archived to {target}.")
            except addstock_run_manifest.ManifestError as exc:
                self._storage_find_set_recovery_blocked(True)
                self._storage_find_say(
                    f"Completed recovery state needs attention: {exc}")
            self._addstock_refresh_debt_indicator()
            return
        status = addstock_run_manifest.summary(data)
        try:
            choice = messagebox.askyesnocancel(
                "Resume interrupted Add Stocks run?",
                f"Run {data['run_id']} has {status['pending']} pending ticker(s) "
                f"and {status['built_unverified']} built-but-unverified "
                "ticker(s).\n\nYes = Resume\nNo = Discard explicitly "
                "(preserve a copy under Run Logs)\nCancel = keep it for later",
                parent=self._storage_find_win)
        except Exception:  # noqa: BLE001
            choice = None
        if choice is True:
            self._addstock_resume_active(data)
        elif choice is False:
            try:
                target = addstock_run_manifest.discard_active(
                    self._storage_root, self._addstock_archive_dir())
                self._storage_find_set_recovery_blocked(False)
                self._storage_find_say(
                    f"Interrupted run discarded explicitly to {target}.")
            except addstock_run_manifest.ManifestError as exc:
                self._storage_find_set_recovery_blocked(True)
                self._storage_find_say(f"Could not discard run: {exc}")
        else:
            try:
                self._storage_find_build_btn.config(state=tk.DISABLED)
            except Exception:  # noqa: BLE001
                pass
            self._storage_find_say(
                "Interrupted run kept. Reopen Add Stocks to Resume or Discard; "
                "new builds remain blocked meanwhile.")
        self._addstock_refresh_debt_indicator()

    def _addstock_resume_active(self, data) -> None:
        self._storage_find_set_recovery_blocked(True)
        try:
            current = addstock_run_manifest.load_run(self._storage_root)
            if current is None or current["run_id"] != data["run_id"]:
                raise addstock_run_manifest.RunMismatch(
                    "active recovery state changed before Resume")
            current = self._addstock_reconcile_evidence(current)
            if current is None:
                self._storage_find_set_recovery_blocked(False)
                self._storage_find_say(
                    "Saved verification evidence completed the interrupted run.")
                self._addstock_refresh_debt_indicator()
                return
            selections = addstock_run_manifest.pending_selections(current)
        except addstock_run_manifest.ManifestError as exc:
            self._storage_find_set_recovery_blocked(True)
            self._storage_find_say(f"Cannot resume Add Stocks: {exc}")
            return
        if not selections:
            self._addstock_consume_portfree_debt(current)
            return
        since = current["params"].get("since")
        if since:
            try:
                import datetime as _dt
                since = _dt.date.fromisoformat(since)
            except ValueError:
                since = None
        self._storage_find_building = True
        self._storage_find_job = {
            "sym": selections[0][0], "iv": selections[0][1],
            "since": since, "selections": list(selections),
            "names": {}, "new": [],
            "topups": sorted({ticker for ticker, _iv in selections}),
            "skipped": [],
        }
        self._storage_find_say(
            f"Resuming {current['run_id']}: {len(selections)} series remain.")
        self._storage_find_start_fill(
            selections, resume_manifest=current)

    def _addstock_consume_portfree_debt(self, data) -> None:
        """Consume xval/gap debt when no fetch series remains."""
        import threading
        run_id = data["run_id"]
        cancel_ev = threading.Event()
        pause_ev = threading.Event()
        self._storage_find_building = True
        self._storage_find_phase = "verification"
        self._storage_find_set_recovery_blocked(True)
        self._storage_find_ev = cancel_ev
        self._storage_find_pause_ev = pause_ev
        self._find_pause_state = "running"
        self._storage_find_pause_generation = 0
        try:
            self._storage_find_build_btn.config(state=tk.DISABLED)
            self._storage_find_search_btn.config(state=tk.DISABLED)
            self._storage_find_pause_btn.config(state=tk.NORMAL, text="Pause")
            self._storage_find_cancel_btn.config(state=tk.NORMAL)
        except Exception:  # noqa: BLE001
            pass
        self._storage_find_say(
            f"Finishing port-free verification debt for {run_id}...")

        def _post_guarded(callback):
            def _render():
                if getattr(self, "_storage_find_ev", None) is cancel_ev:
                    callback()
            try:
                self.root.after(0, _render)
            except Exception:  # noqa: BLE001 - root destroyed during shutdown
                pass

        def _post_progress(message):
            _post_guarded(lambda m=message: self._storage_find_say(m))

        def _post_paused(next_unit, generation):
            def _render():
                if (cancel_ev.is_set() or not pause_ev.is_set()
                        or getattr(
                            self, "_storage_find_pause_generation", None)
                        != generation):
                    return
                self._find_pause_state = "paused"
                try:
                    self._storage_find_pause_btn.config(
                        state=tk.NORMAL, text="Resume")
                    self._storage_find_cancel_btn.config(state=tk.NORMAL)
                except Exception:  # noqa: BLE001 - dialog may have closed
                    pass
                self._storage_find_say(
                    f"Verification paused before {next_unit}; completed "
                    "evidence is saved. Resume continues the remaining debt.")
            _post_guarded(_render)

        def _work():
            notes = []

            def _result(*, cancelled=False, completed=False):
                return {
                    "notes": notes,
                    "cancelled": bool(cancelled),
                    "completed": bool(completed),
                }

            def _checkpoint(next_unit):
                if cancel_ev.is_set():
                    return "cancelled"
                if pause_ev.is_set():
                    announced_generation = None
                    while pause_ev.is_set():
                        generation = getattr(
                            self, "_storage_find_pause_generation", None)
                        if generation != announced_generation:
                            _post_paused(next_unit, generation)
                            announced_generation = generation
                        if cancel_ev.wait(0.1):
                            return "cancelled"
                    if cancel_ev.is_set():
                        return "cancelled"
                try:
                    active = addstock_run_manifest.load_run(
                        self._storage_root)
                except addstock_run_manifest.ManifestError as exc:
                    notes.append(f"verification manifest is unreadable: {exc}")
                    return "stopped"
                if active is None:
                    notes.append(
                        "active recovery disappeared; completion was not "
                        "proven")
                    return "stopped"
                if active.get("run_id") != run_id:
                    notes.append(
                        "active recovery changed; stale verification stopped")
                    return "stopped"
                return "continue"

            state = _checkpoint("the first verification series")
            if state != "continue":
                return _result(cancelled=state == "cancelled")
            healed = addstock_run_manifest.exempt_empty_verification(
                self._storage_root, run_id,
                empty_fn=self._addstock_interval_empty_on_disk,
                archive_dir=self._addstock_archive_dir())
            for ticker, interval in healed.get("exempted") or ():
                _post_progress(
                    f"{ticker} {interval}: empty series — no data to verify "
                    "(kind not computable with current history); "
                    "verification excused")
            if healed.get("archived"):
                return _result(completed=True)
            current = addstock_run_manifest.load_run(self._storage_root)
            if current is None:
                notes.append(
                    "active recovery disappeared after empty-series healing; "
                    "completion was not proven")
                return _result()
            if current.get("run_id") != run_id:
                notes.append(
                    "active recovery changed before verification began")
                return _result()

            debt = []
            for ticker, record in current["tickers"].items():
                xval_intervals = [
                    iv for iv, state in record["series"].items()
                    if stock_storage.session_of(iv) == "rth"
                    and not state["xval"]]
                gap_intervals = [
                    iv for iv, state in record["series"].items()
                    if stock_storage.session_of(iv) == "rth"
                    and not state["gaps"]]
                if xval_intervals or gap_intervals:
                    debt.append((ticker, xval_intervals, gap_intervals))

            total = len(debt)
            for position, (ticker, xval_intervals, gap_intervals) in enumerate(
                    debt, 1):
                for interval in xval_intervals:
                    state = _checkpoint(
                        f"{ticker} ({position}/{total}) xval {interval}")
                    if state != "continue":
                        return _result(cancelled=state == "cancelled")
                    _post_progress(
                        f"Verifying {ticker} ({position}/{total}): "
                        f"xval {interval}...")
                    entry = stock_validate.cross_validate_ticker(
                        self._storage_root, ticker, interval)
                    if stock_validate.record_cross_validation_many(
                            self._storage_root, [entry]) is not None:
                        current_intervals = (
                            self._addstock_current_xval_intervals([entry]))
                        if interval in current_intervals:
                            result = self._addstock_credit(
                                ticker, "xval", [interval], run_id=run_id)
                            if (isinstance(result, dict)
                                    and result.get("archived")):
                                return _result(completed=True)
                        else:
                            notes.append(
                                f"{ticker} {interval}: non-current xval "
                                "remains debt")
                    else:
                        notes.append(
                            f"{ticker} {interval}: xval was not saved")

                for interval in gap_intervals:
                    state = _checkpoint(
                        f"{ticker} ({position}/{total}) gaps {interval}")
                    if state != "continue":
                        return _result(cancelled=state == "cancelled")
                    _post_progress(
                        f"Verifying {ticker} ({position}/{total}): "
                        f"gaps {interval}...")
                    result = stock_validate.update_gap_report(
                        self._storage_root,
                        [(ticker, interval)])
                    if not result.get("error") and result.get("sidecar"):
                        current_intervals = (
                            self._addstock_current_gap_intervals(
                                ticker, [interval]))
                        if interval in current_intervals:
                            credit = self._addstock_credit(
                                ticker, "gaps", [interval], run_id=run_id)
                            if (isinstance(credit, dict)
                                    and credit.get("archived")):
                                return _result(completed=True)
                        else:
                            notes.append(
                                f"{ticker} {interval}: non-current gap evidence "
                                "remains debt")
                    else:
                        notes.append(
                            f"{ticker} {interval}: gap scan was not saved")

            if not debt:
                notes.append(
                    "no port-free xval/gap series remain; any other "
                    "verification debt stays resumable")
            return _result()

        def _done(result):
            if getattr(self, "_storage_find_ev", None) is not cancel_ev:
                return
            try:
                active = addstock_run_manifest.load_run(self._storage_root)
            except addstock_run_manifest.ManifestError:
                active = "unreadable"
            completed = bool(result.get("completed"))
            cancelled = result.get("cancelled") or cancel_ev.is_set()
            notes = list(result.get("notes") or ())
            # Any non-complete outcome leaves recovery ownership unresolved.
            # Search may resume, but a competing Build stays fail-closed until
            # the saved run is completed or explicitly discarded on reopen.
            block_new_build = not (completed and active is None)
            if active is None and not completed and not any(
                    "disappeared" in note for note in notes):
                notes.append(
                    "active recovery disappeared before completion could be "
                    "proven")
            elif active == "unreadable" and not any(
                    "unreadable" in note for note in notes):
                notes.append(
                    "active recovery is unreadable; new builds remain blocked")
            elif (isinstance(active, dict)
                  and active.get("run_id") != run_id
                  and not any("changed" in note for note in notes)):
                notes.append(
                    "a different active recovery now exists; stale callbacks "
                    "were stopped")
            self._storage_find_set_recovery_blocked(block_new_build)
            self._storage_find_idle()
            self._addstock_refresh_debt_indicator()
            if notes:
                self._storage_find_say("; ".join(notes))
            if completed:
                self._storage_find_say("Port-free verification debt finished.")
            elif cancelled:
                self._storage_find_say(
                    "Verification cancelled - completed evidence remains "
                    "credited; remaining debt will be offered on Resume.")
            elif not notes:
                self._storage_find_say(
                    "Port-free verification pass finished; unresolved "
                    "evidence remains resumable debt.")

        def _runner():
            try:
                result = _work()
            except Exception as exc:  # noqa: BLE001
                result = {
                    "notes": [f"verification debt failed: {exc}"],
                    "cancelled": False,
                    "completed": False,
                }
            try:
                self.root.after(0, lambda r=result: _done(r))
            except Exception:  # noqa: BLE001
                pass

        try:
            threading.Thread(target=_runner, daemon=True,
                             name="addstock-verification-debt").start()
        except Exception as exc:  # noqa: BLE001
            _done({
                "notes": [f"verification debt failed to start: {exc}"],
                "cancelled": False,
                "completed": False,
            })

    def _storage_find_dialog(self) -> None:
        """The 'Find / add stock…' dialog: type a company name or
        ticker, see what the data bank already holds (offline, instant)
        plus what TWS's name search knows (when reachable), and either
        BUILD a brand-new series from the IBKR head or TOP UP a stored
        one. Same threading discipline as the IBKR dialog: workers
        never touch tkinter; results arrive through queues drained by
        root.after pollers."""
        if self._storage_busy():
            return
        if not self._storage_tws_gate():
            return
        if getattr(self, "_storage_find_win", None) is not None:
            try:
                self._storage_find_win.lift()
                return
            except Exception:  # noqa: BLE001 — destroyed window
                self._storage_find_win = None
        win = tk.Toplevel(self.root)
        win.title("Add stock — search by name or symbol")
        win.geometry("860x680")
        win.minsize(820, 600)
        win.transient(self.root)
        self._storage_find_win = win
        self._storage_find_busy = False        # a search is running
        self._storage_find_building = False    # a fill is running
        self._storage_find_ev = None
        # Fail closed until the after(0) recovery check proves there is no
        # active/torn Add Stocks intent. Every later Build-enable path consults
        # this recovery-ownership gate instead of blindly enabling the button.
        self._storage_find_recovery_blocked = True
        self._storage_find_job = None
        self._storage_find_items = {}          # canonical sym -> row id
        self._storage_find_remote_seen = set()
        # ONE TWS connection reused across every search/estimate in this dialog
        # (a fresh connect per lookup costs ~5-10s); + a per-session result cache
        # so re-searching the same term never re-hits IBKR. Closed on dialog close.
        # the persistent search connection uses a SEPARATE client-id so it can't
        # clash with a fetch worker's CLIENT_ID_FETCH on the same port (the clash
        # that aborted port 2000 in the Add-stock runs).
        self._find_reuse_adapter = stock_ibkr.ReusableAdapter(
            stock_ibkr.live_adapter_factory(
                stock_ibkr.HOST_DEFAULT, self._tws_ports(),
                client_id=stock_ibkr.CLIENT_ID_SEARCH))
        self._find_search_cache = {}
        self._find_queue = {}                  # canon_sym -> {name, stored}: the
        #              build list, accumulated across searches (in-dialog only)
        # ONE long-lived search thread OWNS the reused connection — ib_async ties
        # a connection to the thread/event-loop that created it, so searches must
        # all run on this thread (a fresh thread per search would use the wrong
        # loop and the 2nd search would return nothing).
        import queue as _fq
        import threading as _fth
        self._find_req_q = _fq.Queue()
        _fth.Thread(target=self._find_search_loop, daemon=True,
                    name="stock-find").start()
        win.protocol("WM_DELETE_WINDOW", self._storage_find_close)

        top = ttk.Frame(win, padding=(8, 6))
        top.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(top, text="Find:").pack(side=tk.LEFT)
        self._storage_find_entry = ttk.Entry(top, width=34)
        self._storage_find_entry.pack(side=tk.LEFT, padx=(4, 0))
        self._storage_find_entry.bind("<Return>",
                                      self._storage_find_search)
        self._storage_find_search_btn = ttk.Button(
            top, text="Search", command=self._storage_find_search)
        self._storage_find_search_btn.pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(top, text="Preset: S&P 500",
                   command=self._storage_find_sp500).pack(
            side=tk.LEFT, padx=(12, 0))
        # also collect each built series' pre-market + after-hours bars
        # (-pre/-post files; regular hours unchanged). Shared with 'IBKR update'.
        self._storage_find_extended_cb = ttk.Checkbutton(
            top, text="+ extended hours",
            variable=self._storage_tws_extended)
        self._storage_find_extended_cb.pack(side=tk.RIGHT)
        ttk.Checkbutton(
            top, text="store daily (1d)",
            variable=self._storage_tws_daily,
            command=self._find_refresh_coverage).pack(side=tk.RIGHT)

        mid = ttk.Frame(win, padding=(8, 0))
        mid.pack(fill=tk.BOTH, expand=True)
        cols = ("symbol", "name", "exchange", "currency", "stored", "secid")
        self._storage_find_tree = ttk.Treeview(
            mid, columns=cols, show="headings", height=8,
            selectmode="extended")
        for c, head, w, anch in (
                ("symbol", "Symbol", 80, "w"),
                ("name", "Company name", 230, "w"),
                ("exchange", "Exch", 70, "center"),
                ("currency", "Ccy", 55, "center"),
                ("stored", "In data bank (coverage: first → last)",
                 300, "w"),
                ("secid", "Security ID", 90, "e")):
            self._storage_find_tree.heading(c, text=head)
            self._storage_find_tree.column(
                c, width=w, anchor=anch,
                stretch=(c in ("name", "stored")))
        _vs = ttk.Scrollbar(mid, orient="vertical",
                            command=self._storage_find_tree.yview)
        self._storage_find_tree.configure(yscrollcommand=_vs.set)
        self._storage_find_tree.pack(side=tk.LEFT, fill=tk.BOTH,
                                     expand=True)
        _vs.pack(side=tk.LEFT, fill=tk.Y)
        self._storage_find_tree.bind("<<TreeviewSelect>>",
                                     self._storage_find_sel)

        out = ttk.LabelFrame(win, text="Search / build progress",
                             padding=(6, 2))
        out.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=(4, 8))
        _pf = ttk.Frame(out)
        _pf.pack(fill=tk.X, pady=(0, 2))
        self._storage_find_progress = ttk.Progressbar(_pf,
                                                       mode="determinate")
        self._storage_find_progress.pack(fill=tk.X)
        self._storage_find_progress_lbl = ttk.Label(_pf, text="", anchor="w")
        self._storage_find_progress_lbl.pack(fill=tk.X, pady=(2, 0))
        # Per-port live monitor: one fixed row per port, rebuilt per parallel
        # run and updated in place (never grows) from a bounded status dict.
        self._find_port_frame = ttk.Frame(out)
        self._find_port_frame.pack(fill=tk.X, pady=(0, 2))
        self._find_port_rows = {}
        self._storage_find_out = tk.Text(out, height=6, wrap="word",
                                         state=tk.DISABLED,
                                         font=("Consolas", 9))
        self._storage_find_out.tag_config("warn", foreground="red")
        self._storage_find_out.pack(fill=tk.X, expand=True)

        # Build LIST: accumulate stocks from DIFFERENT searches and build them
        # together (a new search clears the results, but not this list). Visible
        # so you can see exactly what's queued and remove any of them.
        qrow = ttk.Frame(win, padding=(8, 2))
        qrow.pack(side=tk.BOTTOM, fill=tk.X)
        ttk.Label(qrow, text="Build list:").pack(side=tk.LEFT, anchor="n",
                                                 pady=(2, 0))
        _qlf = ttk.Frame(qrow)
        _qlf.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(6, 0))
        self._find_queue_list = tk.Listbox(_qlf, height=4, selectmode="extended",
                                           font=("Consolas", 9))
        _qsb = ttk.Scrollbar(_qlf, orient="vertical",
                             command=self._find_queue_list.yview)
        self._find_queue_list.configure(yscrollcommand=_qsb.set)
        self._find_queue_list.pack(side=tk.LEFT, fill=tk.X, expand=True)
        _qsb.pack(side=tk.LEFT, fill=tk.Y)
        _qbtn = ttk.Frame(qrow)
        _qbtn.pack(side=tk.LEFT, padx=(6, 0), anchor="n")
        ttk.Button(_qbtn, text="+ Add selected", width=15,
                   command=self._storage_find_queue_add).pack(fill=tk.X)
        ttk.Button(_qbtn, text="− Remove", width=15,
                   command=self._storage_find_queue_remove).pack(
            fill=tk.X, pady=(2, 0))
        ttk.Button(_qbtn, text="Clear all", width=15,
                   command=self._storage_find_queue_clear).pack(
            fill=tk.X, pady=(2, 0))

        act = ttk.Frame(win, padding=(8, 4))
        act.pack(side=tk.BOTTOM, fill=tk.X)
        ttk.Label(act, text="Interval:").pack(side=tk.LEFT)
        self._storage_find_iv = tk.StringVar(value="1m")
        _ivcb = ttk.Combobox(act, textvariable=self._storage_find_iv,
                             values=("1s", "1m", "1h"),
                             width=5, state="readonly")
        self._storage_find_iv_cb = _ivcb          # the kind handler adjusts these
        _ivcb.pack(side=tk.LEFT, padx=(4, 0))
        _ivcb.bind("<<ComboboxSelected>>", self._storage_find_iv_changed)
        ttk.Label(act, text="Data:").pack(side=tk.LEFT, padx=(12, 0))
        self._storage_find_kind = tk.StringVar(value="Trades")
        _kindcb = ttk.Combobox(act, textvariable=self._storage_find_kind,
                               values=("Trades", "Implied Vol", "Hist Vol"),
                               width=11, state="readonly")
        _kindcb.pack(side=tk.LEFT, padx=(4, 0))
        _kindcb.bind("<<ComboboxSelected>>", self._storage_find_kind_changed)
        ttk.Label(act, text="History:").pack(side=tk.LEFT, padx=(12, 0))
        self._storage_find_depth = tk.StringVar(
            value="Earliest traceable")
        self._storage_find_depth_cb = ttk.Combobox(
            act, textvariable=self._storage_find_depth,
            values=("Earliest traceable", "6 months", "1 year",
                    "2 years", "3 years", "5 years", "10 years",
                    "15 years"),
            width=16, state="readonly")
        self._storage_find_depth_cb.pack(side=tk.LEFT, padx=(4, 0))
        self._storage_find_build_btn = ttk.Button(
            act, text="Build series…",
            state=tk.DISABLED,
            command=self._storage_find_build)
        self._storage_find_build_btn.pack(side=tk.LEFT, padx=(12, 0))
        self._storage_find_pause_btn = ttk.Button(
            act, text="Pause", state=tk.DISABLED,
            command=self._storage_find_pause)
        self._storage_find_pause_btn.pack(side=tk.LEFT, padx=(6, 0))
        self._storage_find_cancel_btn = ttk.Button(
            act, text="Cancel", state=tk.DISABLED,
            command=self._storage_find_cancel)
        self._storage_find_cancel_btn.pack(side=tk.LEFT, padx=(6, 0))

        self._storage_find_say(
            "Type a company name or stock code, then Search. Rows "
            "already in the data bank show their stored coverage; rows "
            "TWS found can be built from scratch. Pick a row and an "
            "interval, then Build — nothing is fetched without your "
            "go-ahead.", append=False)
        self._find_refresh_coverage()      # header shows the selected interval(s)
        try:
            self._storage_find_entry.focus_set()
        except Exception:  # noqa: BLE001
            pass
        self.root.after(0, self._addstock_offer_recovery)

    def _storage_find_close(self) -> None:
        if getattr(self, "_storage_find_building", False):
            phase = getattr(self, "_storage_find_phase", None)
            if phase == "verification":
                ev = getattr(self, "_storage_find_ev", None)
                if ev is not None and ev.is_set():
                    self._storage_find_say(
                        "Verification cancellation is still settling the "
                        "current series. Close after it stops; completed "
                        "evidence stays credited and remaining debt stays "
                        "resumable.")
                else:
                    self._storage_find_say(
                        "Port-free verification is running - choose Cancel, "
                        "then close after it stops. Completed evidence stays "
                        "credited and remaining debt stays resumable.")
                return
            if phase in ("preflight", "estimate"):
                # READ-ONLY phase (nothing fetched, nothing written): closing
                # is safe — signal the worker to stop at its next checkpoint
                # and reset to idle so a reopened dialog can build again.
                ev = getattr(self, "_storage_find_ev", None)
                if ev is not None:
                    ev.set()
                self._storage_find_idle()
            else:
                self._storage_find_say(
                    "A build is running — Pause it first (it finalizes the bank "
                    "to a complete, validated, gap-sealed state); once it reads "
                    "'Paused ✓', Cancel unlocks and you can safely close.")
                return
        if getattr(self, "_storage_find_busy", False):
            # a search worker is driving the shared TWS connection — closing now
            # would disconnect it cross-thread (ib_async isn't thread-safe). The
            # search self-clears in a few seconds; just wait it out.
            self._storage_find_say(
                "A search is running — wait for it to finish, then close.")
            return
        try:                                   # tell the search thread to
            rq = getattr(self, "_find_req_q", None)   # disconnect the shared
            if rq is not None:                        # link ON ITS thread + exit
                rq.put(None)
        except Exception:  # noqa: BLE001
            pass
        self._find_req_q = None
        self._find_reuse_adapter = None
        try:
            self._storage_find_win.destroy()
        except Exception:  # noqa: BLE001
            pass
        self._storage_find_win = None
        self._storage_find_busy = False
        self._storage_find_sq = None
        self._storage_find_bq = None
        self._storage_find_ev = None
        self._storage_find_job = None

    def _storage_find_say(self, text, append=True, warn=False) -> None:
        self._batch_bar_update(
            text, getattr(self, "_storage_find_progress", None),
            getattr(self, "_storage_find_progress_lbl", None),
            "_storage_find_run_start")
        try:
            t = self._storage_find_out
            t.config(state=tk.NORMAL)
            if not append:
                t.delete("1.0", tk.END)
            t.insert(tk.END, text + "\n", ("warn",) if warn else ())
            self._trim_text_log(t)
            t.see(tk.END)
            t.config(state=tk.DISABLED)
        except Exception:  # noqa: BLE001 — dialog closed
            pass

    # ---- per-port live monitor (bounded; no growth over a long run) ------
    def _find_clear_port_monitor(self) -> None:
        f = getattr(self, "_find_port_frame", None)
        if f is not None:
            for child in list(f.winfo_children()):
                try:
                    child.destroy()
                except Exception:  # noqa: BLE001
                    pass
        self._find_port_rows = {}
        self._find_port_states = {}

    def _find_build_port_monitor(self, ports) -> None:
        """One fixed row per port — updated in place from the bounded
        _find_port_states dict the engine heartbeats fill (never grows)."""
        import threading
        self._find_clear_port_monitor()
        self._find_port_lock = threading.Lock()
        self._find_port_states = {}
        f = getattr(self, "_find_port_frame", None)
        if f is None:
            return
        for p in ports:
            row = ttk.Frame(f)
            row.pack(fill=tk.X)
            dot = tk.Canvas(row, width=12, height=12, highlightthickness=0)
            oid = dot.create_oval(2, 2, 10, 10, fill="#888", outline="")
            dot.pack(side=tk.LEFT, padx=(2, 6))
            var = tk.StringVar(value=f"port {p}: waiting…")
            lbl = ttk.Label(row, textvariable=var, font=("Consolas", 9, "bold"),
                            foreground="#888888")
            lbl.pack(side=tk.LEFT)
            self._find_port_rows[p] = (dot, oid, var, lbl)

    def _find_port_status_cb(self, port, info) -> None:
        """Engine worker-thread callback: stamp + store in the bounded dict
        (one entry per port, OVERWRITTEN — never grows). The GUI thread
        renders it via _find_render_ports."""
        import time
        lock = getattr(self, "_find_port_lock", None)
        if lock is None:
            return
        try:
            info = dict(info or {})
        except Exception:  # noqa: BLE001
            info = {}
        info["ts"] = time.time()
        with lock:
            self._find_port_states[int(port)] = info

    def _find_pause_status_detail(self, states=None, sealing=False) -> str:
        if states is None:
            lock = getattr(self, "_find_port_lock", None)
            if lock is not None:
                with lock:
                    states = dict(getattr(self, "_find_port_states", {}) or {})
            else:
                states = dict(getattr(self, "_find_port_states", {}) or {})
        rows = getattr(self, "_find_port_rows", None) or {}
        total = len(rows)
        if not total and (getattr(self, "_find_run_params", {}) or {}).get(
                "mode") == "serial":
            total = 1
        line = self._pause_status_text(states, total, sealing=sealing)
        self._batch_set_detail(
            getattr(self, "_storage_find_progress_lbl", None), line)
        return line

    def _find_render_ports(self) -> None:
        import time
        rows = getattr(self, "_find_port_rows", None)
        lock = getattr(self, "_find_port_lock", None)
        # SERIAL build has no per-port row. Its pacer emits a separate positive
        # safe-boundary event, and only that event may start drain/seal finalization.
        # This prevents an atomic volatility reconciliation from overlapping the sealer.
        if not rows:
            pev = getattr(self, "_storage_find_pause_ev", None)
            serial_safe = getattr(
                self, "_storage_find_serial_pause_safe", None)
            if (pev is not None and pev.is_set()
                    and serial_safe is not None and serial_safe.is_set()
                    and getattr(self, "_find_pause_state", None) == "pausing"):
                # latch 'finalizing' only after a daemon spawns (re-pause-safe)
                self._storage_find_finalize()
        if not rows or lock is None:
            return
        with lock:
            states = dict(getattr(self, "_find_port_states", {}))
        now = time.time()
        GREEN, RED, YELLOW, GREY, CYAN = ("#1a9e3f", "#cc2222", "#c8920a",
                                          "#888888", "#1f8fb0")
        for p, (dot, oid, var, lbl) in rows.items():
            st = states.get(p)
            if not st:                                   # not started yet
                color, text = GREY, f"port {p}: waiting…"
            else:
                age = now - st.get("ts", now)
                state = st.get("state", "working")
                tk_ = st.get("ticker") or "—"
                cnt = st.get("count", 0)
                detail = str(st.get("detail") or "")
                pacing_detail = str(st.get("pacing_detail") or "")
                if detail:
                    tk_ = f"{tk_} - {detail}"
                if state == "error":                     # red = OFFLINE
                    color = RED
                    text = f"port {p}: OFFLINE  ✕ {str(st.get('error', ''))[:40]}"
                elif state == "suspect":
                    color = YELLOW
                    text = (f"port {p}: SUSPECT - hard disconnect; probing "
                            f"({cnt} done)")
                elif state == "dead":
                    color = RED
                    text = (f"port {p}: DEAD - probe grace exhausted; "
                            f"still probing ({cnt} done)")
                elif state == "probing":
                    color = YELLOW
                    text = f"port {p}: PROBING for recovery ({cnt} done)"
                elif state in ("retrying", "recovering"):
                    color = YELLOW
                    label = "RECOVERING" if state == "recovering" else "RETRYING…"
                    text = f"port {p}: {label}  {tk_}  ({cnt} done)"
                elif state == "done":                    # green = ONLINE (done)
                    color = GREEN
                    text = (f"port {p}: ONLINE  ✓ done — {cnt} series, "
                            f"{st.get('added', 0):,} bars")
                elif state == "parked":                  # grey = intentionally idled
                    color = GREY                          # (adaptive throttle) — NOT
                    text = (f"port {p}: PARKED  ⏸ easing backend "
                            f"load  ({cnt} done)")        # subject to quiet-timeout
                elif state == "paused":                  # cyan = idled at clean boundary
                    color = CYAN                          # (user Pause) — quiet is BY
                    last_month = str(st.get("last_month") or "")
                    where = (f"finished {last_month}" if last_month
                             else "clean boundary")
                    text = (f"port {p}: PAUSED  ⏸ {where}"
                            f"  ({cnt} done)")            # DESIGN, never "offline?"
                elif pacing_detail:
                    # Exact engine wait state outranks the quiet-age fallback.
                    color = YELLOW
                    text = (f"port {p}: ONLINE  {tk_} - {pacing_detail}  "
                            f"({cnt} done)")
                elif age > 45:
                    color = YELLOW
                    text = f"port {p}: ONLINE - waiting {age:.0f}s on {tk_}"
                else:                                    # green = ONLINE
                    color = GREEN
                    text = f"port {p}: ONLINE  ● {tk_}  ({cnt} done)"
            try:
                dot.itemconfig(oid, fill=color)
                lbl.config(foreground=color)             # the coloured FONT
                var.set(text)
            except Exception:  # noqa: BLE001 — dialog closed
                pass
        self._batch_set_pacing_detail(
            getattr(self, "_storage_find_progress_lbl", None),
            self._pacing_detail_from_states(states))
        # PAUSE STATE MACHINE: once every still-active port has idled at its clean
        # boundary, kick off the FINALIZE (drain xval + seal gaps) exactly ONCE.
        # 'Paused ✓' (and Cancel/exit) unlock only when the finalize completes — i.e.
        # the bank is a COMPLETE, usable snapshot. A done/error port never blocks it.
        pev = getattr(self, "_storage_find_pause_ev", None)
        if pev is not None and pev.is_set() \
                and getattr(self, "_find_pause_state", None) == "pausing":
            self._find_pause_status_detail(states)
            stz = {p: (states.get(p) or {}).get("state") for p in rows}
            pending = [p for p, s in stz.items()
                       if s not in ("done", "error", "dead", None)]
            if pending and all(stz[p] == "paused" for p in pending):
                # latch 'finalizing' only after a daemon spawns (re-pause-safe)
                self._storage_find_finalize()        # drain xval + seal -> Paused ✓
        elif getattr(self, "_find_pause_state", None) == "finalizing":
            self._find_pause_status_detail(states, sealing=True)

    def _storage_find_kind_changed(self, _evt=None) -> None:
        """Constrain the interval picker to what each data KIND supports:
        Hist Vol = DAILY only; Implied Vol = 1m or daily; Trades = 1s/1m/1h.
        The token built at fetch time becomes e.g. '1m-iv' / '1d-hvol'."""
        try:
            kind = {"Trades": "", "Implied Vol": "iv",
                    "Hist Vol": "hvol"}.get(self._storage_find_kind.get(), "")
            opts = (("1d",) if kind == "hvol"
                    else ("1m", "1d") if kind == "iv"
                    else ("1s", "1m", "1h"))
            self._storage_find_iv_cb.config(values=opts)
            if self._storage_find_iv.get() not in opts:
                self._storage_find_iv.set(opts[0])
            base = str(self._storage_find_iv.get() or opts[0]).strip()
            token = f"{base}-{kind}" if kind else base
            self._set_extended_checkbox_state(
                self._storage_find_extended_cb, (token,))
            self._storage_find_iv_changed()       # re-narrow the depth picker
        except Exception:  # noqa: BLE001 — dialog closing
            pass

    def _find_selected_ivs(self):
        """The interval token(s) the coverage column should show: the picked
        sub-daily interval, plus '1d' when 'store daily (1d)' is ticked — i.e.
        exactly what a Build of this row would produce."""
        iv = (self._storage_find_iv.get() or "1m").strip()
        ivs = [iv]
        try:
            if bool(self._storage_tws_daily.get()) and "1d" not in ivs:
                ivs.append("1d")
        except Exception:  # noqa: BLE001
            pass
        return ivs

    def _find_cov_text(self, key):
        """The stored-coverage cell for one row, filtered to the SELECTED
        interval(s) (e.g. '1m  2024-01-02 → 2026-06-26   1d  …'). Blank for an
        interval the bank doesn't hold yet (so an absent series reads as a
        fresh build, not a top-up)."""
        cov = (getattr(self, "_find_cov_by_key", {}) or {}).get(key) or {}
        parts = []
        for iv in self._find_selected_ivs():
            c = cov.get(iv)
            if c:
                parts.append(f"{iv}  {c}")
        return "   ".join(parts)

    def _find_refresh_coverage(self):
        """Re-render every result row's coverage cell (and the column heading)
        for the CURRENT interval / daily selection — called when either changes
        so the column always reflects what you're about to build."""
        tree = getattr(self, "_storage_find_tree", None)
        if tree is None:
            return
        try:
            ivs = self._find_selected_ivs()
            tree.heading("stored",
                         text=f"In data bank — {' + '.join(ivs)} (first → last)")
        except Exception:  # noqa: BLE001
            pass
        for key, iid in list((getattr(self, "_storage_find_items", {})
                              or {}).items()):
            try:
                vals = list(tree.item(iid)["values"])
                vals += [""] * (5 - len(vals))
                vals[4] = self._find_cov_text(key)
                tree.item(iid, values=vals)
            except Exception:  # noqa: BLE001 — one bad row mustn't stop the rest
                continue

    def _storage_find_iv_changed(self, _evt=None) -> None:
        """IBKR keeps only ~6 months of 1-SECOND history (the same cap
        for every stock — it is a system-wide retention, not per-name),
        so for 1s the depth picker offers ONLY '6 months' / 'Earliest
        traceable'; minute+ keeps the full range. Also refresh the coverage
        column so it shows the newly-selected interval."""
        try:
            iv = (self._storage_find_iv.get() or "1m").strip()
            if iv.endswith("s"):
                opts = ("Earliest traceable", "6 months")
            else:
                opts = ("Earliest traceable", "6 months", "1 year",
                        "2 years", "3 years", "5 years", "10 years",
                        "15 years")
            self._storage_find_depth_cb.config(values=opts)
            if self._storage_find_depth.get() not in opts:
                self._storage_find_depth.set("Earliest traceable")
        except Exception:  # noqa: BLE001 — dialog closing
            pass
        self._find_refresh_coverage()

    def _storage_find_sel(self, _evt=None) -> None:
        """Build button text follows the selected row: a series the
        bank already holds tops up; an absent one builds in full. A
        directory hit is NOT a fetchability promise — warn up front
        for the two common traps (foreign-currency listing, delisted
        name) instead of letting the build fail cryptically."""
        try:
            sel = self._storage_find_tree.selection()
            if not sel:
                return
            if len(sel) > 1:
                self._storage_find_build_btn.config(
                    text=f"Build / top up {len(sel)} selected…")
                return
            vals = list(self._storage_find_tree.item(sel[0])["values"])
            vals += [""] * (5 - len(vals))
            stored = str(vals[4]).strip()
            self._storage_find_build_btn.config(
                text="Top up to latest" if stored
                else "Build series…")
            ccy = str(vals[3]).strip().upper()
            if not stored and ccy and ccy != "USD":
                self._storage_find_say(
                    f"note: {vals[0]} is a {ccy} listing — fetching "
                    f"here is SMART/USD only, so a build will likely "
                    f"be refused. (IBKR's name search also lists "
                    f"delisted symbols, e.g. CS/Credit Suisse.)")
        except Exception:  # noqa: BLE001 — dialog closing
            pass

    def _storage_find_search(self, _evt=None) -> None:
        """Kick the lookup worker: local matches first (instant, works
        without TWS), then the TWS name search — its unreachability is
        said, not fatal (decided with the engine's find_symbol)."""
        if (getattr(self, "_storage_find_busy", False)
                or getattr(self, "_storage_find_building", False)):
            return
        try:
            text = self._storage_find_entry.get().strip()
        except Exception:  # noqa: BLE001 — dialog closing
            return
        if not text:
            self._storage_find_say(
                "Type a company name or ticker first.")
            return
        self._storage_find_busy = True
        try:
            self._storage_find_search_btn.config(state=tk.DISABLED)
            self._storage_find_build_btn.config(state=tk.DISABLED)
            tree = self._storage_find_tree
            tree.delete(*tree.get_children())
        except Exception:  # noqa: BLE001 — dialog closing
            pass
        self._storage_find_items = {}
        self._storage_find_remote_seen = set()
        self._storage_find_say(f"Searching for {text!r}…",
                               append=False)
        import queue
        q = queue.Queue()
        self._storage_find_sq = q
        root_dir = self._storage_root
        ckey = text.lower()
        cached = self._find_search_cache.get(ckey)
        if cached is not None:
            # cached this session — no IBKR, no thread; serve straight to poll.
            try:
                local = _stock_find_local(root_dir, text)
            except Exception:  # noqa: BLE001
                local = []
            q.put(("local", local))
            q.put(("remote", cached))
            q.put(("sdone", None))
        else:
            rq = getattr(self, "_find_req_q", None)
            if rq is None:                     # thread gone (dialog closing)
                self._storage_find_busy = False
                self._storage_find_sq = None
                try:
                    self._storage_find_search_btn.config(state=tk.NORMAL)
                    self._storage_find_build_btn.config(
                        state=self._storage_find_build_state())
                except Exception:  # noqa: BLE001
                    pass
                self._storage_find_say("Search unavailable — reopen the dialog.")
                return
            rq.put((text, root_dir, q))        # the search thread does the work
        self.root.after(100, self._storage_find_search_poll)

    def _find_search_loop(self) -> None:
        """The ONE long-lived per-dialog thread that runs name searches on the
        REUSED TWS connection (which it owns on its own event loop). A fresh
        thread per search would bind ib_async to the wrong loop and find
        nothing. Drains _find_req_q until a None sentinel, then disconnects the
        shared link ON THIS THREAD."""
        import stock_ibkr
        reuse = self._find_reuse_adapter
        while True:
            try:
                req = self._find_req_q.get()
            except Exception:  # noqa: BLE001
                break
            if req is None:
                break
            text, root_dir, out_q = req
            try:
                local = _stock_find_local(root_dir, text)
            except Exception:  # noqa: BLE001
                local = []
            out_q.put(("local", local))
            try:
                matches, err = stock_ibkr.find_symbol(text, adapter_factory=reuse)
            except Exception as exc:  # noqa: BLE001
                matches, err = [], f"name search crashed: {exc}"
            if matches:
                try:
                    _locals = _stock_find_local(root_dir, "")
                    stored_map = {r["symbol"]: r["stored"] for r in _locals}
                    cov_map = {r["symbol"]: r.get("coverage", {})
                               for r in _locals}
                except Exception:  # noqa: BLE001
                    stored_map, cov_map = {}, {}
                rows = []
                for m in matches:
                    sym = str(m.get("symbol", "")).strip().upper()
                    _nm = str(m.get("name", "") or "")
                    try:
                        key = stock_storage.canonical_ticker(sym)
                    except Exception:  # noqa: BLE001 — odd spelling
                        key = sym
                    rows.append({"symbol": sym,
                                 "name": _nm,
                                 "exchange": str(m.get("exchange", "") or ""),
                                 "currency": str(m.get("currency", "") or ""),
                                 "stored": stored_map.get(key, ""),
                                 "coverage": cov_map.get(key, {})})
                    # find-stock fetches the company name for free — remember the USD
                    # listing's so adding it later (or the column) shows it with no probe
                    if _nm and str(m.get("currency", "")).upper() == "USD":
                        try:
                            stock_validate.record_ibkr_name(root_dir, sym, _nm)
                        except Exception:  # noqa: BLE001
                            pass
                self._find_search_cache[text.lower()] = rows
                out_q.put(("remote", rows))
            if err:
                out_q.put(("err", err))
            out_q.put(("sdone", None))
        try:
            reuse.close()        # disconnect the shared link on the OWNING thread
        except Exception:  # noqa: BLE001
            pass

    def _storage_find_search_poll(self) -> None:
        q = getattr(self, "_storage_find_sq", None)
        if q is None:
            return
        import queue
        try:
            while True:
                kind, payload = q.get_nowait()
                if kind == "local":
                    self._storage_find_rows(payload, remote=False)
                    self._storage_find_say(
                        f"{len(payload)} match(es) in the data bank; "
                        f"asking TWS…")
                elif kind == "remote":
                    self._storage_find_rows(payload, remote=True)
                    self._storage_find_say(
                        f"TWS returned {len(payload)} match(es).")
                elif kind == "err":
                    self._storage_find_say(str(payload))
                else:                              # ("sdone", None)
                    self._storage_find_sq = None
                    self._storage_find_busy = False
                    try:
                        self._storage_find_search_btn.config(
                            state=tk.NORMAL)
                        self._storage_find_build_btn.config(
                            state=self._storage_find_build_state())
                        n = len(self._storage_find_tree.get_children())
                    except Exception:  # noqa: BLE001 — dialog closed
                        return
                    self._storage_find_say(
                        f"Search done — {n} row(s). Pick one, then "
                        f"Build." if n else
                        "Search done — no matches anywhere.")
                    return
        except queue.Empty:
            pass
        except Exception as exc:  # noqa: BLE001 — truthful end state,
            self._storage_find_sq = None     # never a stuck busy flag
            self._storage_find_busy = False
            try:
                self._storage_find_search_btn.config(state=tk.NORMAL)
                self._storage_find_build_btn.config(
                    state=self._storage_find_build_state())
            except Exception:  # noqa: BLE001
                pass
            self._storage_find_say(
                f"Search finished; display failed: {exc}")
            return
        try:
            self.root.after(100, self._storage_find_search_poll)
        except Exception:  # noqa: BLE001 — window closing
            pass

    def _storage_find_rows(self, matches, remote) -> None:
        """Render one batch of matches (GUI thread). Dedupe by
        canonical symbol; the FIRST remote hit (USD sorts first) may
        enrich a local row's name/exchange/currency, but the local
        'stored' summary always wins."""
        try:
            tree = self._storage_find_tree
        except AttributeError:
            return
        if getattr(self, "_find_cov_by_key", None) is None:
            self._find_cov_by_key = {}
        for m in matches or []:
            try:
                sym = str(m.get("symbol", "")).strip().upper()
                if not sym:
                    continue
                try:
                    key = stock_storage.canonical_ticker(sym)
                except Exception:  # noqa: BLE001 — junk symbol: show
                    key = sym      # it anyway, dedupe on the spelling
                cov = m.get("coverage") or {}
                if cov:                       # remember the FULL per-iv span so the
                    self._find_cov_by_key[key] = cov   # column can switch interval
                # the visible cell shows only the SELECTED interval(s) coverage
                stored_cell = self._find_cov_text(key)
                secid_cell = str(m.get("conId", "") or "") or "—"
                vals = (sym, str(m.get("name", "") or ""),
                        str(m.get("exchange", "") or ""),
                        str(m.get("currency", "") or ""),
                        stored_cell, secid_cell)
                iid = self._storage_find_items.get(key)
                if iid is None:
                    self._storage_find_items[key] = tree.insert(
                        "", tk.END, values=vals)
                elif (remote and key not in
                        self._storage_find_remote_seen):
                    old = list(tree.item(iid)["values"])
                    old += [""] * (6 - len(old))
                    # the remote hit carries the authoritative conId; keep the
                    # old cell only when the remote result lacks one
                    sec = str(m.get("conId", "") or "") or str(old[5]) or "—"
                    tree.item(iid, values=(
                        str(old[0]), vals[1] or str(old[1]), vals[2],
                        vals[3], stored_cell, sec))
                if remote:
                    self._storage_find_remote_seen.add(key)
            except Exception:  # noqa: BLE001 — one bad row must not
                continue       # lose the rest of the batch
        self._storage_find_sel()    # keep the Build label honest

    def _storage_find_update_queue_lbl(self) -> None:
        """Repopulate the build-list box from self._find_queue IN ADD ORDER
        (first added on top, last at the bottom — the dict preserves insertion
        order); rows added by the most recent '+ Queue' are HIGHLIGHTED until
        the next add/remove."""
        try:
            lb = self._find_queue_list
            lb.delete(0, tk.END)
            new = getattr(self, "_find_queue_new", None) or set()
            for i, s in enumerate(self._find_queue or {}, 1):
                nm = (self._find_queue.get(s) or {}).get("name", "")
                lb.insert(tk.END, f"{i}. {s:<8} {nm}".rstrip())
                if s in new:                       # just-added -> green highlight
                    lb.itemconfig(i - 1, background="#d8f5d8",
                                  foreground="#0a6b2e")
        except Exception:  # noqa: BLE001 — dialog closing
            pass

    def _bank_has_series(self, ticker, token) -> bool:
        """True when the bank ALREADY stores this EXACT (ticker, token) series
        — token carries the data kind ('1m' trades vs '1m-iv' vs '1d-hvol'), so
        an IV build of a trades-only ticker correctly reads as NEW. Cached scan
        table first (offline, instant); manifest fallback when unscanned."""
        canon = str(ticker).strip().upper()
        tok = str(token).strip()
        known = self._storage_cached_known_series()
        if known is not None:
            for t, ivs in known:
                if str(t).strip().upper() == canon:
                    return tok in {str(x).strip() for x in ivs}
            return False
        try:                                   # unscanned -> read this manifest
            man = stock_storage.load_manifest(
                self._storage_root / stock_storage.canonical_ticker(ticker))
            return bool(man) and tok in (man.get("intervals") or {})
        except Exception:  # noqa: BLE001
            return False

    def _storage_find_queue_remove(self) -> None:
        """Remove the selected item(s) from the build list."""
        try:
            sel = list(self._find_queue_list.curselection())
        except Exception:  # noqa: BLE001
            return
        if not sel:
            self._storage_find_say("Select item(s) in the build list to remove.")
            return
        syms = list(self._find_queue or {})        # add-order — matches the list
        removed = 0
        for i in sel:
            if 0 <= i < len(syms) and \
                    self._find_queue.pop(syms[i], None) is not None:
                removed += 1
        self._find_queue_new = set()               # drop the new-row highlight
        self._storage_find_update_queue_lbl()
        self._storage_find_say(
            f"Removed {removed}; build list now {len(self._find_queue)}.")

    def _storage_find_queue_add(self, _evt=None) -> None:
        """Add the currently-selected search results to the build queue. The
        queue accumulates across searches so several stocks found in DIFFERENT
        searches can be built together."""
        try:
            sel = self._storage_find_tree.selection()
        except Exception:  # noqa: BLE001
            return
        if getattr(self, "_find_queue", None) is None:
            self._find_queue = {}
        added = 0
        just = set()
        for iid in sel:
            try:
                v = list(self._storage_find_tree.item(iid)["values"])
            except Exception:  # noqa: BLE001
                continue
            v += [""] * (5 - len(v))
            try:
                key = stock_storage.canonical_ticker(str(v[0]))
            except Exception:  # noqa: BLE001 — odd spelling
                key = str(v[0]).strip().upper()
            if key and key not in self._find_queue:
                self._find_queue[key] = {"name": str(v[1]).strip(),
                                         "stored": str(v[4]).strip()}
                just.add(key)
                added += 1
        self._find_queue_new = just            # highlight exactly this batch
        self._storage_find_update_queue_lbl()
        if added:
            self._storage_find_say(
                f"Queued {added}; build queue now {len(self._find_queue)}: "
                + ", ".join(self._find_queue))
        else:
            self._storage_find_say(
                "Select one or more results first, then '+ Queue selected'.")

    def _storage_find_queue_clear(self) -> None:
        self._find_queue = {}
        self._find_queue_new = set()
        self._storage_find_update_queue_lbl()
        self._storage_find_say("Build queue cleared.")

    def _storage_find_build(self) -> None:
        """Build/top-up the selected row(s) AND anything in the build queue:
        preflight first (go/no-go), then — for a series the bank does not hold
        yet — a sized estimate the user must approve before the paced fetch
        starts. Stored series skip the estimate and top up directly."""
        if (getattr(self, "_storage_find_busy", False)
                or getattr(self, "_storage_find_building", False)
                or self._storage_busy()):
            return
        try:
            sel = self._storage_find_tree.selection()
        except Exception:  # noqa: BLE001 — dialog closing
            sel = ()
        rows = []
        # seed from the build QUEUE (stocks accumulated across searches), then
        # add any currently-selected results (deduped against the queue).
        for _qs, _qinfo in (getattr(self, "_find_queue", None) or {}).items():
            rows.append({"sym": _qs, "name": _qinfo.get("name", ""),
                         "stored": _qinfo.get("stored", "")})
        for iid in sel:
            try:
                v = list(self._storage_find_tree.item(iid)["values"])
            except Exception:  # noqa: BLE001 — dialog closing
                continue
            v += [""] * (5 - len(v))
            try:
                s = stock_storage.canonical_ticker(str(v[0]))
            except stock_storage.StorageError as exc:
                self._storage_find_say(f"{v[0]}: {exc} — skipped")
                continue
            if s not in [r["sym"] for r in rows]:
                rows.append({"sym": s, "name": str(v[1]).strip(),
                             "stored": str(v[4]).strip()})
        if not rows:
            self._storage_find_say(
                "Select a result, or use '+ Queue selected' to add stocks "
                "to build.")
            return
        # NB: the queue is emptied in _storage_find_start_fill (only after every
        # confirm approves) — NOT here, so a declined cost prompt keeps it.
        iv = (self._storage_find_iv.get() or "1m").strip()
        try:                                      # fold in the data KIND -> token
            _kind = {"Trades": "", "Implied Vol": "iv",
                     "Hist Vol": "hvol"}.get(self._storage_find_kind.get(), "")
            iv = stock_storage.with_kind(iv, _kind)
        except Exception:  # noqa: BLE001
            pass
        # "History" depth -> a since-date the engine clamps the
        # backfill start to; "Earliest traceable" = IBKR's head. A
        # stock younger than the depth starts at its own earliest bar
        # (the engine never skips it).
        depth = (self._storage_find_depth.get()
                 if hasattr(self, "_storage_find_depth") else "")
        _days = {"6 months": 183, "1 year": 365, "2 years": 730,
                 "3 years": 1095, "5 years": 1826, "10 years": 3652,
                 "15 years": 5478}
        since = None
        if depth in _days:
            import datetime as _dt
            since = (_dt.date.today()
                     - _dt.timedelta(days=_days[depth]))
        sym, name = rows[0]["sym"], rows[0]["name"]
        # NEW-vs-TOPUP must key on the ACTUAL token being built (`iv`, kind
        # folded in above) — NOT the row's flattened 'stored' string, which
        # covers ALL of a ticker's intervals/kinds. Otherwise Data=IV on a
        # trades-stored ticker mis-classifies as a top-up and launches an
        # unsized, unapproved full IV backfill (hunt-confirmed).
        stored = self._bank_has_series(sym, iv)
        self._storage_find_building = True
        self._storage_find_phase = "preflight"   # read-only phase: close/cancel OK
        self._storage_find_job = {
            "sym": sym, "iv": iv, "name": name, "since": since,
            "selections": [(r["sym"], iv) for r in rows],
            "names": {r["sym"]: r["name"] for r in rows if r["name"]},
            "new": [r["sym"] for r in rows
                    if not self._bank_has_series(r["sym"], iv)],
            "topups": [r["sym"] for r in rows
                       if self._bank_has_series(r["sym"], iv)]}
        njobs = len(rows)
        try:
            self._storage_find_build_btn.config(state=tk.DISABLED)
            self._storage_find_search_btn.config(state=tk.DISABLED)
            self._storage_find_cancel_btn.config(state=tk.NORMAL)
        except Exception:  # noqa: BLE001
            pass
        import queue
        import threading
        q = queue.Queue()
        ev = threading.Event()
        self._storage_find_bq = q
        self._storage_find_ev = ev
        self._storage_find_say(
            "Checking TWS before touching the archive…")
        root_dir = self._storage_root
        ports = self._tws_ports()              # resolve on the GUI thread
        factory = self._tws_factory()
        since = (self._storage_find_job or {}).get("since")

        symbols = [s for s, _iv in self._storage_find_job["selections"]]

        def _work():
            def _user_cancelled():
                if ev.is_set():
                    q.put(("cancelled", None))   # own terminal: nothing was
                    return True                  # fetched, nothing written
                return False

            # 0) in Parallel mode, restart any logged-out fleet port first so
            #    the daily auto-logout doesn't fail the go/no-go below.
            self._fleet_preflight(lambda m: q.put(("progress", m)), ports,
                                  cancel=ev)
            if _user_cancelled():
                return
            # 1) go/no-go BEFORE anything can be written
            try:
                ok, failures = stock_ibkr.preflight(ports=ports, cancel=ev)
            except Exception as exc:  # noqa: BLE001
                ok, failures = False, [f"preflight crashed: {exc}"]
            if _user_cancelled():
                return
            if not ok:
                q.put(("preflight", failures))
                return
            # 2) SCAN every selected symbol and its identity before any bank
            # write; a missing cached frontier may use one bounded daily probe.
            q.put(("progress", f"Preflight ok — checking {len(symbols)} "
                               f"symbol(s) and identities at IBKR before any "
                               f"bank write…"))
            try:
                vres = stock_ibkr.validate_symbols(
                    symbols, adapter_factory=factory,
                    progress=lambda m: q.put(("progress", m)), cancel=ev,
                    identity_root=self._storage_root)
            except Exception as exc:  # noqa: BLE001
                vres = {"found": [], "not_found": [],
                        "error": f"validation crashed: {exc}"}
            if _user_cancelled():
                return
            q.put(("validate", vres))

        try:
            threading.Thread(target=_work, daemon=True,
                             name="stock-validate").start()
        except Exception as exc:  # noqa: BLE001 — thread exhaustion
            self._storage_find_done(exc)
            return
        self.root.after(100, self._storage_find_build_poll)

    def _storage_find_build_poll(self) -> None:
        q = getattr(self, "_storage_find_bq", None)
        if q is None:
            return
        try:
            lines, marker, terminal, backlog, detail = (
                self._drain_progress_batched(q))
        except Exception as exc:  # noqa: BLE001 — truthful end state
            self._storage_find_bq = None
            self._storage_find_done(RuntimeError(f"display failed: {exc}"))
            return
        if lines or detail is not None:
            self._batch_say(getattr(self, "_storage_find_out", None),
                            "_storage_find_progress", "_storage_find_progress_lbl",
                            "_storage_find_run_start", lines, marker, detail)
        if terminal is not None:
            kind, payload = terminal
            self._storage_find_bq = None
            if kind == "cancelled":            # user cancel in a READ-ONLY phase
                self._storage_find_say(
                    "Cancelled — nothing was fetched or written.")
                self._storage_find_idle()
            elif kind == "start_fill":         # single stored top-up -> full path
                self._storage_find_start_fill(payload)
            elif kind == "preflight":
                self._storage_find_no_go(payload)
            elif kind == "estimate":
                self._storage_find_confirm(payload)
            elif kind == "validate":
                self._storage_find_validate_decision(payload)
            elif kind == "mass":
                self._storage_find_confirm_mass()
            else:                              # ("done", report)
                self._storage_find_done(payload)
            return
        self._find_render_ports()                 # refresh per-port dots/text
        try:
            self.root.after(10 if backlog else 100, self._storage_find_build_poll)
        except Exception:  # noqa: BLE001 — window closing
            pass

    def _storage_find_build_state(self):
        """Return the truthful Build state for durable recovery ownership."""
        return (tk.DISABLED
                if getattr(self, "_storage_find_recovery_blocked", False)
                else tk.NORMAL)

    def _storage_find_set_recovery_blocked(self, blocked) -> None:
        blocked = bool(blocked)
        if not blocked:
            try:
                blocked = (addstock_run_manifest.load_run(
                    self._storage_root) is not None)
            except addstock_run_manifest.ManifestError:
                blocked = True
        self._storage_find_recovery_blocked = blocked
        try:
            self._storage_find_build_btn.config(
                state=self._storage_find_build_state())
        except Exception:  # noqa: BLE001 - dialog may not exist yet
            pass

    def _storage_find_idle(self) -> None:
        """Back to idle button states, truthfully, even mid-failure."""
        self._storage_find_building = False
        self._storage_find_phase = None
        self._storage_find_ev = None
        self._storage_find_pause_ev = None
        self._storage_find_serial_pause_safe = None
        self._storage_find_pause_generation = None
        self._storage_find_mass_q = None
        self._find_pause_state = None              # reset the pause state machine
        self._batch_live = None                    # stop the 1-second elapsed tick
        self._xval_finish()                        # let cross-validation drain
        self._storage_find_busy_cursor(False)
        try:
            self._storage_find_build_btn.config(
                state=self._storage_find_build_state())
            self._storage_find_search_btn.config(state=tk.NORMAL)
            self._storage_find_cancel_btn.config(state=tk.DISABLED)
            self._storage_find_pause_btn.config(state=tk.DISABLED,
                                                text="Pause")
        except Exception:  # noqa: BLE001 — dialog closed
            pass

    def _storage_find_confirm(self, est) -> None:
        """GUI-thread gate between the estimate and the paced fetch:
        nothing is fetched or written until the user says yes."""
        job = getattr(self, "_storage_find_job", None) or {}
        sym, iv = job.get("sym", "?"), job.get("iv", "1m")
        _ev = getattr(self, "_storage_find_ev", None)
        if ((isinstance(est, dict) and est.get("cancelled"))
                or (_ev is not None and _ev.is_set())):
            self._storage_find_say(
                "Cancelled — nothing was fetched or written.")
            self._storage_find_idle()
            return
        if not isinstance(est, dict) or est.get("error"):
            why = (est.get("error") if isinstance(est, dict)
                   else repr(est))
            self._storage_find_say(f"Could not size the backfill: "
                                   f"{why}")
            if "no SMART/USD" in str(why):
                self._storage_find_say(
                    "IBKR's directory knows this NAME, but no live "
                    "SMART-routed USD contract exists behind it — the "
                    "symbol is delisted or only trades on a foreign "
                    "exchange. It cannot be fetched from here.")
            self._storage_find_say("Nothing was fetched or written.")
            self._storage_find_idle()
            return
        self._storage_find_say(
            f"estimate: {est.get('sessions', 0):,} session(s) from "
            f"{est.get('start', '?')}, ~{est.get('est_requests', 0):,}"
            f" request(s), ~{est.get('est_minutes', 0):,} min.")
        msg = (f"Build {sym} {iv}: ~{est.get('sessions', 0):,} "
               f"sessions, ~{est.get('est_requests', 0):,} requests, "
               f"~{est.get('est_minutes', 0):,} min of paced "
               f"fetching.")
        # interval-aware wall-clock estimate (GUI-free helper); the
        # online head fixed the exact start, so size from THAT window
        try:
            wt = _estimate_worktime(
                self._storage_root,
                self._expand_daily(self._expand_extended([(sym, iv)])),
                "add", since=est.get("start"),
                n_accounts=self._tws_estimate_lanes())
            msg += f"\n\nEstimated wall-clock time: {wt['human']}."
            self._batch_est_seconds = wt.get("seconds")   # FRESH live-ETA seed for
            self._batch_pred_map = wt.get("per_series_seconds")
        except Exception:  # noqa: BLE001 — never block the confirm
            self._batch_est_seconds = None                # THIS build — never let
            #                  the single-ticker path inherit a prior run's estimate
            self._batch_pred_map = None
        if est.get("clipped_1s"):
            msg += ("\n\nNote: IBKR only serves 1-second bars a few "
                    "months back — the start was clipped "
                    "accordingly.")
        msg += "\n\nProceed?"
        try:
            go = messagebox.askyesno("Find / add stock", msg,
                                     parent=self._storage_find_win)
        except Exception:  # noqa: BLE001 — dialog closed
            go = False
        if not go:
            self._storage_find_say(
                "Build declined — nothing fetched or written.")
            self._storage_find_idle()
            return
        self._storage_find_start_fill([(sym, iv)])

    def _storage_find_sp500(self) -> None:
        """One click = every CURRENT S&P 500 constituent listed AND selected. The
        list is fetched live OFF-thread (datahub/Wikipedia → bundled snapshot
        fallback) so it reflects today's index; works even on an EMPTY bank, so the
        whole index can be built from scratch. Stored symbols top up; the rest
        build in full. Pair with the interval + History pickers, then Build."""
        if (getattr(self, "_storage_find_busy", False)
                or getattr(self, "_storage_find_building", False)):
            return
        self._storage_find_say("Fetching the current S&P 500 list…")
        self._current_sp500_async(self._storage_find_sp500_apply)

    def _storage_find_sp500_apply(self, syms, source, asof) -> None:
        try:
            n = len(syms)
            label = self._sp500_src_label(source, asof, n)
            tree = self._storage_find_tree
            tree.delete(*tree.get_children())
            self._storage_find_items = {}
            self._storage_find_remote_seen = set()
            try:                       # coverage for the ones already held
                _locals = _stock_find_local(self._storage_root, "")
                stored_map = {r["symbol"]: r["stored"] for r in _locals}
                cov_map = {r["symbol"]: r.get("coverage", {}) for r in _locals}
            except Exception:  # noqa: BLE001
                stored_map, cov_map = {}, {}
            rows = [{"symbol": s, "name": "", "exchange": "", "currency": "USD",
                     "stored": stored_map.get(s, ""),
                     "coverage": cov_map.get(s, {})}
                    for s in syms]
            self._storage_find_rows(rows, remote=False)
            kids = tree.get_children()
            if kids:
                tree.selection_set(kids)
            held = sum(1 for s in syms if stored_map.get(s))
            self._storage_find_say(
                f"S&P 500 preset ({label}): {n} symbols selected "
                f"({held} already stored → top up, {n - held} to build). "
                f"Pick interval + history, then Build.")
            self._storage_find_sel()
        except Exception as exc:  # noqa: BLE001 — surface, don't vanish
            try:
                self._storage_find_say(f"S&P 500 preset failed: {exc}")
            except Exception:  # noqa: BLE001 — dialog truly gone
                pass

    def _storage_find_validate_decision(self, vres) -> None:
        """GUI-thread gate AFTER validation, BEFORE the writable fill.
        Not-found symbols in a batch -> a stop-or-skip popup; if the
        user skips, the job is narrowed to the found set and the fetch
        proceeds. A lone not-found symbol just reports and stops."""
        job = getattr(self, "_storage_find_job", None) or {}
        sels = job.get("selections") or []
        total = len(sels)
        if not isinstance(vres, dict):
            vres = {"found": [], "not_found": [], "error": str(vres)}
        _ev = getattr(self, "_storage_find_ev", None)
        if vres.get("cancelled") or (_ev is not None and _ev.is_set()):
            self._storage_find_say(
                "Cancelled — nothing was fetched or written.")
            self._storage_find_idle()
            return
        win = getattr(self, "_storage_find_win", None) or self.root
        if vres.get("error"):
            self._storage_find_say(f"Could not verify symbols: "
                                   f"{vres['error']}  Nothing fetched.")
            try:
                messagebox.showwarning(
                    "Connection problem",
                    f"Could not check the symbols at TWS:\n\n"
                    f"{vres['error']}\n\nNothing was built.", parent=win)
            except Exception:  # noqa: BLE001
                pass
            self._storage_find_idle()
            return
        found = list(vres.get("found") or [])
        not_found = list(vres.get("not_found") or [])
        if not found:
            # non-blocking (unattended-safe): a loud RED line, no modal
            self._storage_find_say(
                f"WARNING: NONE of the {total} selected stock(s) were found "
                f"at IBKR (delisted / wrong symbols) — nothing to build.",
                warn=True)
            self._storage_find_idle()
            return
        if not_found:
            shown = ", ".join(not_found[:25]) + (
                f", …(+{len(not_found) - 25} more)"
                if len(not_found) > 25 else "")
            # AUTO-SKIP — no blocking modal, so an unattended mass build never
            # stalls on a Yes/No. A loud RED warning goes to the log NOW; the
            # count popup + a saved skip-log come at the END (_storage_find_done).
            # (found is non-empty here: the all-not-found case returned above.)
            self._storage_find_say(
                f"WARNING: {len(not_found)} of {total} NOT found at IBKR "
                f"(delisted / wrong symbol) — SKIPPED; building the "
                f"{len(found)} found.  Skipped: {shown}", warn=True)
            job["skipped"] = list(not_found)
        # IDENTITY VALIDATION is mandatory: no result means the pre-write gate
        # itself failed, so fail closed instead of silently starting the fetch.
        if "secid" not in vres:
            self._storage_find_say(
                "IDENTITY CHECK FAILED: no identity verdict was returned. "
                "Nothing was fetched or written; retry the build.", warn=True)
            self._storage_find_idle()
            return
        secid = vres.get("secid") or {}
        checked_symbols = {
            d.get("symbol") for d in secid.values() if isinstance(d, dict)
        }
        unchecked = sorted(s for s in found if s not in checked_symbols)
        if unchecked:
            shown = ", ".join(unchecked[:25])
            self._storage_find_say(
                f"IDENTITY CHECK FAILED: no verdict for {shown}. Nothing "
                "was fetched or written; retry the build.", warn=True)
            self._storage_find_idle()
            return
        blocked = stock_ibkr.blocked_add_identities(secid)
        if blocked:
            for d in sorted(blocked.values(), key=lambda x: x["symbol"]):
                status = d.get("status")
                if status == "mismatch":
                    msg = (
                        f"SECURITY ID MISMATCH: {d['symbol']} stored="
                        f"{d['stored']} != IBKR now={d['current']} - possible "
                        "ticker REUSE or dead-pin repin. SKIPPED to protect "
                        "the stored series; verify the company and repin.")
                elif status == "history_mismatch":
                    msg = (
                        f"LISTING HISTORY MISMATCH: {d['symbol']} has stored "
                        f"TRADES data from {d['stored_first']}, but the current "
                        f"IBKR contract serves from {d['current_earliest']} "
                        f"({d['gap_days']:,} days later). Possible ticker "
                        "REUSE; SKIPPED to protect the stored series.")
                else:
                    msg = (
                        f"IDENTITY HISTORY UNVERIFIED: {d['symbol']} has stored "
                        "TRADES data, but IBKR did not provide demonstrated "
                        "listing-history evidence. SKIPPED; retry when the "
                        "daily-history probe is available.")
                self._storage_find_say(msg, warn=True)
            job.setdefault("skipped", []).extend(sorted(blocked))
            found = [s for s in found if s not in blocked]
            if not found:
                self._storage_find_say(
                    "Nothing left to build after the identity/reuse gate.",
                    warn=True)
                self._storage_find_idle()
                return
        # narrow the job to the FOUND set, then start the fetch phase
        fset = set(found)
        job["selections"] = [(s, iv) for (s, iv) in sels if s in fset]
        job["new"] = [s for s in (job.get("new") or []) if s in fset]
        job["topups"] = [s for s in (job.get("topups") or [])
                         if s in fset]
        job["names"] = {s: n for s, n in (job.get("names") or {}).items()
                        if s in fset}
        # keep the conId map from validation so the fetch can REUSE it
        # (gap_fill skips its own pre-pass -> no second "Checking …" pass).
        job["resolved"] = {s: c for s, c in (vres.get("resolved") or {}).items()
                           if s in fset}
        if job["selections"]:
            job["sym"], job["iv"] = job["selections"][0]
        self._storage_find_fetch()

    def _storage_find_fetch(self) -> None:
        """Phase 2 (after the existence scan): the actual paced fetch
        for the FOUND selections — one sized confirmation for a batch, a
        sized estimate for a single new stock, a direct top-up for a
        stored one."""
        job = getattr(self, "_storage_find_job", None) or {}
        sels = job.get("selections") or []
        if not sels:
            self._storage_find_idle()
            return
        import queue
        import threading
        q = queue.Queue()
        self._storage_find_bq = q
        # FRESH events for phase 2: a Cancel clicked during phase 1 was consumed
        # by its gate — reusing the SET event here silently poisoned the fill
        # (instant 0-bar abort after the user clicked Proceed; hunt-confirmed).
        ev = threading.Event()
        self._storage_find_ev = ev
        pev = threading.Event()
        self._storage_find_pause_ev = pev
        serial_pause_safe = threading.Event()
        self._storage_find_serial_pause_safe = serial_pause_safe
        self._storage_find_run_start = None
        njobs = len(sels)
        sym, iv = sels[0]
        stored = sym in set(job.get("topups") or [])
        run_now = stored and njobs == 1          # direct top-up = a REAL run
        self._storage_find_phase = "run" if run_now else "estimate"
        self._find_pause_state = "running" if run_now else None
        try:
            if run_now:
                # Cancel is GATED during a run — it unlocks only at 'Paused ✓'
                # (a complete bank). To stop: Pause (finalizes), then Cancel.
                self._storage_find_pause_btn.config(state=tk.NORMAL,
                                                    text="Pause")
                self._storage_find_cancel_btn.config(state=tk.DISABLED)
            else:
                # estimate/confirm phase is READ-ONLY: Cancel works directly;
                # Pause is meaningless here (it used to drive a bogus finalize).
                self._storage_find_pause_btn.config(state=tk.DISABLED,
                                                    text="Pause")
                self._storage_find_cancel_btn.config(state=tk.NORMAL)
            self._storage_find_progress.config(value=0, maximum=1)
            self._storage_find_progress_lbl.config(text="")
        except Exception:  # noqa: BLE001 — dialog closed
            pass
        since = job.get("since")
        root_dir = self._storage_root
        factory = self._tws_factory()

        def _work():
            if njobs > 1:
                q.put(("mass", None))
            elif stored:
                # A single stored top-up goes through the SAME full path as a
                # queue/confirm run (_storage_find_start_fill) so it ALSO gets
                # -pre/-post + 1d expansion, a cross-val session on the right
                # cancel event, and run-log params — instead of a bare gap_fill
                # that silently ignored '+ extended hours' / 'store daily'.
                q.put(("start_fill", [(sym, iv)]))
            else:
                q.put(("progress", f"Sizing a backfill of {sym} {iv}…"))
                try:
                    est = stock_ibkr.estimate_backfill(
                        sym, iv, adapter_factory=factory, since=since,
                        cancel=ev)
                except Exception as exc:  # noqa: BLE001
                    est = {"error": f"estimate crashed: {exc}"}
                q.put(("estimate", est))

        try:
            threading.Thread(target=_work, daemon=True,
                             name="stock-build").start()
        except Exception as exc:  # noqa: BLE001 — thread exhaustion
            self._storage_find_done(exc)
            return
        self.root.after(100, self._storage_find_build_poll)

    def _storage_find_confirm_mass(self) -> None:
        """GUI-thread gate for a MULTI-row queue: ONE sized
        confirmation instead of per-stock estimates (each estimate
        costs paced requests — a 500-stock queue would burn the whole
        budget before fetching a single bar). Depth-limited queues get
        an exact per-stock session bound; 'Earliest traceable' is
        honestly unknown per stock."""
        job = getattr(self, "_storage_find_job", None) or {}
        _ev = getattr(self, "_storage_find_ev", None)
        if _ev is not None and _ev.is_set():
            self._storage_find_say(
                "Cancelled — nothing was fetched or written.")
            self._storage_find_idle()
            return
        sels = job.get("selections") or []
        new = job.get("new") or []
        topups = job.get("topups") or []
        iv = job.get("iv", "1m")
        since = job.get("since")
        self._storage_find_confirm_mass_async(sels, new, topups, iv, since)
        return
        lines = [f"Queue {len(sels)} series at {iv}: {len(new)} new "
                 f"build(s), {len(topups)} top-up(s)."]
        if since is not None and new:
            import datetime as _dt
            sessions = len(stock_ibkr.trading_days(since,
                                                   _dt.date.today()))
            per = len(stock_ibkr.day_requests(iv, since)) or 1
            reqs = sessions * per * len(new)
            lines.append(
                f"New builds reach back to {since}: up to "
                f"~{sessions:,} sessions each, ~{reqs:,} requests "
                f"total (~{round(reqs * _est_secs_per_request(iv) / 60):,} "
                f"min paced). Stocks "
                f"younger than that automatically fetch from their "
                f"earliest bar.")
        elif new:
            lines.append(
                "Depth is 'Earliest traceable': each new build "
                "fetches the stock's FULL history — potentially years "
                "(thousands of requests) per stock.")
        # interval-aware wall-clock estimate for the WHOLE queue (GUI-free
        # helper, no per-stock network call): new builds size from `since`
        # (a lower bound when depth is 'Earliest traceable'), top-ups from
        # the manifest gap.
        try:
            # expand to -pre/-post (if '+ extended hours' is on) BEFORE
            # estimating, so the queue estimate matches what the fill actually
            # fetches (each interval becomes up to 3 series) — _expand_extended
            # is applied to the real run in _storage_find_start_fill.
            add_sels = self._expand_daily(
                self._expand_extended([(s, iv) for s in new]))
            up_sels = self._expand_daily(
                self._expand_extended([(s, iv) for s in topups]))
            lanes = self._tws_estimate_lanes()
            wt_add = _estimate_worktime(self._storage_root, add_sels,
                                        "add", since=since, n_accounts=lanes)
            wt_up = _estimate_worktime(self._storage_root, up_sels,
                                       "update", since=since, n_accounts=lanes)
            # add + top-ups run as ONE parallel batch over all tickers, so the
            # combined wall-clock is the summed SERIAL time over the EFFECTIVE
            # parallelism for the ports the whole ticker set can fill — ~linear to
            # 5, diminishing past (measured 2026-06-24), bounded by the ticker count.
            eff = _est_parallel(min(lanes, len(set(new) | set(topups))))
            est_secs = (wt_add["serial_seconds"]
                        + wt_up["serial_seconds"]) / eff
            self._batch_est_seconds = est_secs   # seed the live ETA
            total = _est_humanize_minutes(est_secs)
            tail = (" (a lower bound — full-history builds are unsized "
                    "offline)" if wt_add["unknown_series"] else "")
            lines.append(f"Estimated wall-clock time for the queue: "
                         f"{total}{tail}.")
        except Exception:  # noqa: BLE001 — never block the confirm
            pass
        lines.append("The queue runs sequentially under pacing; "
                     "Cancel keeps everything already committed and a "
                     "re-run resumes.")
        try:
            go = messagebox.askyesno(
                "Find / add stock", "\n\n".join(lines) + "\n\nProceed?",
                parent=self._storage_find_win)
        except Exception:  # noqa: BLE001 — dialog closed
            go = False
        if not go:
            self._storage_find_say(
                "Queue declined — nothing fetched or written.")
            self._storage_find_idle()
            return
        self._storage_find_start_fill(sels)

    def _storage_find_confirm_mass_async(self, sels, new, topups, iv, since) -> None:
        """Compute mass queue wall-clock sizing off the Tk thread."""
        # Snapshot Tk-backed toggles on the GUI thread. The worker only does
        # filesystem/calendar math and never touches widgets.
        add_sels = self._expand_daily(
            self._expand_extended([(s, iv) for s in new]))
        up_sels = self._expand_daily(
            self._expand_extended([(s, iv) for s in topups]))
        lanes = self._tws_estimate_lanes()
        import queue
        import threading
        q = queue.Queue()
        self._storage_find_mass_q = q
        self._storage_find_phase = "estimate"
        self._storage_find_say(
            f"Sizing {len(sels)} queued series off the UI thread...")
        self._storage_find_busy_cursor(True)

        def _work():
            import datetime as _dt
            lines = [f"Queue {len(sels)} series at {iv}: {len(new)} new "
                     f"build(s), {len(topups)} top-up(s)."]
            if since is not None and new:
                sessions = len(stock_ibkr.trading_days(since, _dt.date.today()))
                per = len(stock_ibkr.day_requests(iv, since)) or 1
                reqs = sessions * per * len(new)
                lines.append(
                    f"New builds reach back to {since}: up to "
                    f"~{sessions:,} sessions each, ~{reqs:,} requests "
                    f"total (~{round(reqs * _est_secs_per_request(iv) / 60):,} "
                    f"min paced). Stocks younger than that automatically "
                    f"fetch from their earliest bar.")
            elif new:
                lines.append(
                    "Depth is 'Earliest traceable': each new build fetches the "
                    "stock's FULL history — potentially years (thousands of "
                    "requests) per stock.")
            batch_est_seconds = None
            pred_map = None
            try:
                wt_add = _estimate_worktime(self._storage_root, add_sels,
                                            "add", since=since,
                                            n_accounts=lanes)
                wt_up = _estimate_worktime(self._storage_root, up_sels,
                                           "update", since=since,
                                           n_accounts=lanes)
                eff = _est_parallel(min(lanes, len(set(new) | set(topups))))
                est_secs = (wt_add["serial_seconds"]
                            + wt_up["serial_seconds"]) / eff
                batch_est_seconds = est_secs
                pred_map = dict(wt_add.get("per_series_seconds") or {})
                pred_map.update(wt_up.get("per_series_seconds") or {})
                total = _est_humanize_minutes(est_secs)
                tail = (" (a lower bound — full-history builds are unsized "
                        "offline)" if wt_add["unknown_series"] else "")
                lines.append(f"Estimated wall-clock time for the queue: "
                             f"{total}{tail}.")
            except Exception as exc:  # noqa: BLE001
                lines.append(f"Estimated wall-clock time unavailable: {exc}")
            lines.append("The queue runs sequentially under pacing; Cancel "
                         "keeps everything already committed and a re-run "
                         "resumes.")
            q.put({"lines": lines, "sels": list(sels),
                   "batch_est_seconds": batch_est_seconds,
                   "pred_map": pred_map})

        try:
            threading.Thread(target=_work, daemon=True,
                             name="stock-mass-estimate").start()
            self.root.after(100, self._storage_find_confirm_mass_poll)
        except Exception as exc:  # noqa: BLE001
            self._storage_find_mass_q = None
            self._storage_find_busy_cursor(False)
            self._storage_find_say(f"Could not size the queue: {exc}")
            self._storage_find_idle()

    def _storage_find_busy_cursor(self, on: bool) -> None:
        try:
            cur = "watch" if on else ""
            win = getattr(self, "_storage_find_win", None)
            if win is not None:
                win.config(cursor=cur)
            self.root.config(cursor=cur)
        except Exception:  # noqa: BLE001
            pass

    def _storage_find_confirm_mass_poll(self) -> None:
        q = getattr(self, "_storage_find_mass_q", None)
        if q is None:
            return
        import queue
        try:
            payload = q.get_nowait()
        except queue.Empty:
            ev = getattr(self, "_storage_find_ev", None)
            if ev is not None and ev.is_set():
                self._storage_find_mass_q = None
                self._storage_find_busy_cursor(False)
                self._storage_find_say(
                    "Cancelled — nothing was fetched or written.")
                self._storage_find_idle()
                return
            try:
                self.root.after(100, self._storage_find_confirm_mass_poll)
            except Exception:  # noqa: BLE001
                pass
            return
        self._storage_find_mass_q = None
        self._storage_find_busy_cursor(False)
        ev = getattr(self, "_storage_find_ev", None)
        if ev is not None and ev.is_set():
            self._storage_find_say(
                "Cancelled — nothing was fetched or written.")
            self._storage_find_idle()
            return
        self._storage_find_confirm_mass_ready(payload)

    def _storage_find_confirm_mass_ready(self, payload) -> None:
        payload = payload or {}
        lines = list(payload.get("lines") or [])
        sels = list(payload.get("sels") or [])
        self._batch_est_seconds = payload.get("batch_est_seconds")
        self._batch_pred_map = payload.get("pred_map")
        try:
            go = messagebox.askyesno(
                "Find / add stock", "\n\n".join(lines) + "\n\nProceed?",
                parent=self._storage_find_win)
        except Exception:  # noqa: BLE001
            go = False
        if not go:
            self._storage_find_say(
                "Queue declined — nothing fetched or written.")
            self._storage_find_idle()
            return
        self._storage_find_start_fill(sels)

    def _storage_find_start_fill(self, selections, resume_manifest=None) -> None:
        """The approved fill: same engine path as the IBKR dialog —
        full backfill, top-up and multi-series queues are the one
        gap_fill code path (it processes the list sequentially under
        pacing, halting only the series that misbehaves)."""
        root_dir = self._storage_root
        factory = self._tws_factory()          # GUI-thread port resolve
        ports = self._tws_ports()
        _job = getattr(self, "_storage_find_job", None) or {}
        since = _job.get("since")
        try:
            if resume_manifest is not None:
                manifest = addstock_run_manifest.resume_run(
                    root_dir, resume_manifest["run_id"])
                sels = addstock_run_manifest.pending_selections(manifest)
            else:
                sels = self._expand_daily(
                    self._expand_extended(selections))  # + pre/post + daily
                earliest = stock_validate.load_ibkr_earliest(root_dir)
                manifest = addstock_run_manifest.create_run(
                    root_dir, sels,
                    params={
                        "mode": "add", "since": since,
                        "extended": bool(self._storage_tws_extended.get()),
                        "store_daily": bool(self._storage_tws_daily.get()),
                        "ports_at_start": list(ports),
                    },
                    earliest_tickers=earliest)
            # From this point a durable active run exists.  Keep new builds
            # blocked even if the fail-safe heal below reports a manifest
            # error after creation/resume.
            self._storage_find_set_recovery_blocked(True)
            healed = addstock_run_manifest.exempt_empty_verification(
                root_dir, manifest["run_id"],
                empty_fn=self._addstock_interval_empty_on_disk,
                archive_dir=self._addstock_archive_dir())
            for ticker, interval in healed.get("exempted") or ():
                self._storage_find_say(
                    f"{ticker} {interval}: empty series — no data to verify "
                    "(kind not computable with current history); "
                    "verification excused")
            if healed.get("archived"):
                self._storage_find_set_recovery_blocked(False)
                self._storage_find_say(
                    "Empty-series verification completed the saved run.")
                self._storage_find_idle()
                self._addstock_refresh_debt_indicator()
                return
            if healed.get("exempted"):
                current = addstock_run_manifest.load_run(root_dir)
                if (current is None
                        or current.get("run_id") != manifest["run_id"]):
                    raise addstock_run_manifest.RunMismatch(
                        "active run changed during empty-series healing")
                manifest = current
                if resume_manifest is not None:
                    sels = addstock_run_manifest.pending_selections(manifest)
        except addstock_run_manifest.ManifestError as exc:
            self._storage_find_say(
                f"Add Stocks run did not start: {exc}. The in-memory queue "
                "was preserved.")
            self._storage_find_idle()
            self._addstock_refresh_debt_indicator()
            return
        if not sels:
            self._storage_find_say(
                "No fetch series remain; finishing verification debt instead.")
            self._addstock_consume_portfree_debt(manifest)
            return
        run_id = manifest["run_id"]
        seal_debt = []
        for item in manifest.get("finalize", {}).get("seal_pending") or []:
            parts = str(item).split()
            if len(parts) == 2:
                seal_debt.append((parts[0], parts[1]))
        self._addstock_run_id = run_id
        # The fill is approved and starting. Empty the in-memory queue only
        # after durable intent was written successfully above.
        if getattr(self, "_find_queue", None):
            self._find_queue = {}
            self._storage_find_update_queue_lbl()
        import queue
        import threading
        q = queue.Queue()
        # FRESH events for the APPROVED run: every cancel clicked before this
        # moment was consumed by a phase gate — a leftover SET event must never
        # leak in and instantly abort the run the user just approved.
        ev = threading.Event()
        self._storage_find_ev = ev
        pev = threading.Event()
        self._storage_find_pause_ev = pev
        serial_pause_safe = threading.Event()
        self._storage_find_serial_pause_safe = serial_pause_safe
        self._storage_find_run_start = None
        self._storage_find_phase = "run"           # writes begin: close re-gates
        self._find_pause_state = "running"         # pause/cancel state machine
        self._gap_incremental_begin()
        try:
            self._storage_find_pause_btn.config(state=tk.NORMAL, text="Pause")
            # Cancel is GATED — it unlocks only at 'Paused ✓' (a complete bank).
            # To stop, Pause first; it finalizes (month + xval + seal), then Cancel.
            self._storage_find_cancel_btn.config(state=tk.DISABLED)
            self._storage_find_progress.config(value=0, maximum=1)
            self._storage_find_progress_lbl.config(text="")
        except Exception:  # noqa: BLE001 — dialog closed
            pass
        self._storage_find_bq = q
        self._xval_retire()
        probe_seed = live_spot_probe.default_seed()
        reconcile_tokens = {}
        for _ticker, _interval in sels:
            if (stock_storage.kind_of(_interval) in stock_storage.RATIO_KINDS
                    and stock_storage.session_of(_interval) == "rth"):
                reconcile_tokens.setdefault(
                    str(_ticker).upper(), set()).add(str(_interval))

        def _reconcile_plan(ticker, _series_rows):
            tokens = sorted(reconcile_tokens.get(str(ticker).upper(), ()))
            if not tokens:
                return []
            audit_report = vol_value_audit.audit(
                root_dir, tickers=[ticker], write_queue=True,
                operation_mode="fetch")
            if audit_report.get("complete") is not True:
                raise RuntimeError(
                    f"volatility re-audit for {str(ticker).upper()} is "
                    "incomplete")
            return vol_value_reconcile.plan(
                root_dir, tickers=[ticker], kinds=tokens)

        def _probe_check(adapter, pacer, ticker, series_rows, seed):
            del adapter, pacer
            # Recheck this ticker at the actual WS8 boundary.  D2 normally
            # exempted an empty result at completion; this bounded fallback
            # covers a transient manifest read failure without rescanning disk
            # for every other ticker on every coordinator callback.
            confirmed_empty = set()
            for row in series_rows:
                interval = str(row.get("interval") or "")
                if (stock_storage.session_of(interval) == "rth"
                        and self._addstock_interval_empty_on_disk(
                            ticker, interval)):
                    confirmed_empty.add(interval)
            healed = addstock_run_manifest.exempt_empty_verification(
                root_dir, run_id,
                empty_fn=lambda candidate, interval: (
                    candidate == ticker
                    and interval in confirmed_empty),
                archive_dir=self._addstock_archive_dir())
            for empty_ticker, empty_interval in (
                    healed.get("exempted") or ()):
                q.put((
                    "progress",
                    f"{empty_ticker} {empty_interval}: empty series — no data "
                    "to verify (kind not computable with current history); "
                    "verification excused"))
            if healed.get("archived"):
                raise addstock_run_manifest.RunMismatch(
                    "active Add Stocks run archived before WS8 verification")
            entries = []
            seen = set()
            for row in series_rows:
                interval = str(row.get("interval") or "")
                if stock_storage.session_of(interval) != "rth":
                    continue
                if interval in confirmed_empty:
                    continue
                key = stock_validate.cross_validation_key(ticker, interval)
                if key in seen:
                    continue
                seen.add(key)
                entries.append(stock_validate.cross_validate_ticker(
                    root_dir, ticker, interval))
            if entries:
                if stock_validate.record_cross_validation_many(
                        root_dir, entries) is None:
                    raise RuntimeError("cross-validation results were not saved")
                current = self._addstock_current_xval_intervals(entries)
                if current:
                    self._addstock_credit(
                        ticker, "xval", current, run_id=run_id)
            plan = live_spot_probe.build_embedded_plan(
                root_dir, [ticker], seed=seed, interval="1m")
            if plan.get("rows"):
                return {"row": dict(plan["rows"][0])}
            selection = (plan.get("items") or {}).get(ticker)
            if selection is None:
                raise RuntimeError("WS8 selection did not return the ticker")
            return {"selection": dict(selection)}

        probe_coordinator = live_spot_probe.AddStockProbeCoordinator(
            sels, check_fn=_probe_check, engine_requests=True,
            seed=probe_seed, cancel=ev,
            progress=lambda m: q.put(("progress", m)), pause=pev,
            reconcile_plan_fn=(_reconcile_plan if reconcile_tokens else None),
            reconcile_run_id=run_id)

        def _attach_probe_report(rep):
            if not isinstance(rep, dict):
                return rep
            gate = live_spot_probe.fleet_probe_gate(rep, ports)
            if reconcile_tokens:
                rep["vol_value_reconcile"] = (
                    probe_coordinator.reconcile_result(**gate))
            evidence = probe_coordinator.result(**gate)
            rows = list(evidence.pop("rows"))
            rep["spot_probe"] = dict(evidence)
            rep["spot_probe"]["rows"] = rows
            verdicts = {}
            for row in rows:
                verdict = str(row.get("verdict") or "PROBE_ERROR")
                verdicts[verdict] = verdicts.get(verdict, 0) + 1
            artifact = {
                "kind": "add_stocks_spot_probe_report",
                "version": 1,
                "parent_run_id": rep.get("run"),
                "seed": evidence["seed"],
                "report_only": True,
                "probes": rows,
                "verdict_counts": verdicts,
                "request_count": evidence["request_count"],
                "reused_request_count": evidence["reused_request_count"],
                "probe_issue_count": evidence["issue_count"],
                "status": evidence.get("status", "complete"),
                "skip_reason": evidence.get("skip_reason"),
                "skipped_count": evidence.get("skipped_count", 0),
                "down_ports": evidence.get("down_ports", []),
                "bank_written": False,
                "written": False,
                "artifact_written": False,
            }
            try:
                target = (Path(live_spot_probe.RUN_LOGS_ROOT)
                          / f"add-stocks-spot-probe-{rep.get('run')}.json")
                live_spot_probe.write_artifact(
                    target, artifact, bank_root=root_dir,
                    run_logs_root=live_spot_probe.RUN_LOGS_ROOT)
                rep["spot_probe"]["report"] = str(target.resolve())
            except Exception as exc:  # noqa: BLE001 - parent run still succeeds
                rep["spot_probe"]["report_error"] = str(exc)[:500]
            if reconcile_tokens:
                # The engine writes report.json before returning. Re-publish
                # atomically after attaching M3 evidence so reconciliation is
                # durable rather than UI/return-only.
                saved = stock_ibkr._write_parallel_report(
                    root_dir, rep, stage="post-pipeline")
                if saved:
                    rep["vol_value_reconcile"]["report"] = str(saved)
                else:
                    rep["vol_value_reconcile"]["report_error"] = (
                        "post-pipeline parent report was not saved")
            return rep
        # show the queue's WALL-CLOCK estimate (from _storage_find_confirm_mass)
        # as the live ETA from the start; blends toward measured as series finish.
        self._batch_seed("_storage_find_run_start",
                         self._storage_find_progress_lbl, len(sels),
                         getattr(self, "_batch_est_seconds", None))
        use_parallel = (self._parallel_enabled() and len(set(ports)) > 1
                        and len({s[0] for s in sels}) > 1)
        if use_parallel:
            self._find_build_port_monitor(ports)   # one fixed row per port
        else:
            self._find_clear_port_monitor()

        _job = getattr(self, "_storage_find_job", None) or {}
        since = _job.get("since")
        resolved_map = _job.get("resolved")      # reuse validation's conIds
        import time as _rt
        self._find_run_params = {
            "flow": "Add stock", "mode": "parallel" if use_parallel else "serial",
            "ports": list(ports), "since": since,
            "extended": bool(self._storage_tws_extended.get()),
            "durable_run_id": run_id,
            "started_clock": _rt.time()}

        def _work():
            def say(m):
                q.put(("progress", m))
            port_probe = None
            try:
                if use_parallel:
                    restart_ports, port_up = self._fleet_callbacks(
                        say, ports, cancel=ev)
                    port_probe = port_up
                    try:
                        import tws_launch as _tws_launch
                        maintenance_flag = addstock_watchdog.maintenance_path(
                            _tws_launch.fleet_path())
                    except Exception:  # noqa: BLE001 - absence fails closed
                        maintenance_flag = None
                    rep = stock_ibkr.add_stocks_gap_fill_parallel_resilient(
                        root_dir, sels, ports,
                        restart_ports=restart_ports, port_up=port_up,
                        progress=say,
                        cancel=ev, pause=pev, since=since,
                        resolved=resolved_map,
                        port_status=self._find_port_status_cb,
                        # adaptive OFF — see _storage_ibkr_start note (premise
                        # empirically unsupported; default off).
                        on_series_start=lambda t, iv: self._addstock_series_started(
                            run_id, t, iv),
                        on_series=lambda t, iv, res: self._add_stock_on_series(
                            t, iv, res, run_id=run_id),
                        adaptive=False,
                        post_pipeline=probe_coordinator, engine_post_tasks=True,
                        hard_death_watchdog=True,
                        maintenance_path=maintenance_flag)
                else:
                    reuser = stock_ibkr.ReusableAdapter(factory)
                    parent_pacer = stock_ibkr.Pacer()
                    parent_pacer._on_pause = (
                        lambda paused, _info=None: (
                            serial_pause_safe.set() if paused
                            else serial_pause_safe.clear()))
                    try:
                        rep = stock_ibkr.add_stocks_gap_fill(
                            root_dir, sels, progress=say, cancel=ev,
                            pause=pev, since=since, adapter_factory=reuser,
                            pacer=parent_pacer, resolved=resolved_map,
                            on_series_start=lambda t, iv:
                                self._addstock_series_started(run_id, t, iv),
                             on_series=lambda t, iv, res:
                                 self._add_stock_on_series(
                                     t, iv, res, run_id=run_id),
                             post_pipeline=probe_coordinator, engine_post_tasks=True)
                    finally:
                        reuser.close()
            except Exception as exc:  # noqa: BLE001
                rep = exc
            if (seal_debt and isinstance(rep, dict)
                    and not rep.get("fleet_down_finalize")
                    and not rep.get("cancelled") and not rep.get("aborted")):
                remaining = [f"{ticker} {interval}"
                             for ticker, interval in seal_debt]
                try:
                    seal_ports = list(ports)
                    if port_probe is not None:
                        healthy = []
                        for port in ports:
                            try:
                                if port_probe(port):
                                    healthy.append(port)
                            except Exception:  # noqa: BLE001
                                continue
                        seal_ports = healthy
                    say(f"Resume seal: checking {len(seal_debt)} pending "
                        "series before completion...")
                    sealed, _left = self._finalize_drain_and_seal(
                        seal_debt, seal_ports, say)
                    calendar = stock_validate.consensus_calendar(root_dir)
                    remaining = []
                    for ticker, interval in seal_debt:
                        scan = stock_validate.scan_series_gaps(
                            root_dir, ticker, interval,
                            calendar_days=calendar)
                        if scan.get("missing_days"):
                            remaining.append(f"{ticker} {interval}")
                    addstock_run_manifest.replace_seal_pending(
                        root_dir, run_id, remaining,
                        archive_dir=self._addstock_archive_dir())
                    rep["resume_seal_added"] = int(sealed or 0)
                    rep["seal_pending"] = remaining
                    say("Resume seal finished: "
                        f"+{int(sealed or 0):,} bars; "
                        f"{len(remaining)} series still pending.")
                except Exception as exc:  # noqa: BLE001 - debt remains durable
                    rep["seal_pending"] = remaining
                    rep.setdefault("notes", []).append(
                        f"resume seal failed: {type(exc).__name__}: {exc}")
                    say("Resume seal failed; seal debt remains recorded for "
                        "the next Resume.")
            if isinstance(rep, dict) and rep.get("fleet_down_finalize"):
                drained = probe_coordinator.drain_checks()
                rep["port_free_checks_drained"] = int(drained)
                say("Fleet-down finalize: port-free cross-validation drained; "
                    "volatility reconciles, live probes, and gap seals remain "
                    "pending for Resume.")
            q.put(("done", _attach_probe_report(rep)))

        try:
            threading.Thread(target=_work, daemon=True,
                             name="stock-build").start()
        except Exception as exc:  # noqa: BLE001 — thread exhaustion
            self._storage_find_done(exc)
            return
        if len(selections) == 1:
            self._storage_find_say(
                f"Building {selections[0][0]} {selections[0][1]} "
                + (f"since {since}…" if since
                   else "from the IBKR head…"))
        else:
            self._storage_find_say(
                f"Queued {len(selections)} series "
                + (f"since {since}…" if since
                   else "from each stock's IBKR head…"))
        self.root.after(100, self._storage_find_build_poll)

    def _storage_find_cancel(self) -> None:
        ev = getattr(self, "_storage_find_ev", None)
        if ev is not None:
            if getattr(self, "_storage_find_phase", None) == "verification":
                ev.set()
                pev = getattr(self, "_storage_find_pause_ev", None)
                if pev is not None:
                    pev.clear()
                self._find_pause_state = "cancelling"
                try:
                    self._storage_find_cancel_btn.config(state=tk.DISABLED)
                    self._storage_find_pause_btn.config(
                        state=tk.DISABLED, text="Pause")
                except Exception:  # noqa: BLE001
                    pass
                self._storage_find_say(
                    "Cancelling verification - the current series finishes "
                    "atomically; completed evidence stays credited and "
                    "remaining debt stays resumable.")
                return
            ev.set()
            try:
                self._storage_find_cancel_btn.config(state=tk.DISABLED)
            except Exception:  # noqa: BLE001
                pass
            if getattr(self, "_storage_find_phase", None) in ("preflight",
                                                              "estimate"):
                self._storage_find_say(
                    "Cancelling — nothing has been fetched or written; "
                    "stopping at the next checkpoint…")
            else:
                self._storage_find_say(
                    "Cancelling — committed months stay; a re-run resumes "
                    "where this stopped.")

    def _storage_find_pause(self) -> None:
        """Toggle build/top-up or port-free verification pause state.

        Fetch pauses stop new IBKR requests at the next safe boundary;
        verification pauses stop before the next evidence series. Committed
        data/evidence is untouched and Resume continues.
        """
        pev = getattr(self, "_storage_find_pause_ev", None)
        if pev is None:
            return
        if getattr(self, "_storage_find_phase", None) == "verification":
            cancel_ev = getattr(self, "_storage_find_ev", None)
            if cancel_ev is not None and cancel_ev.is_set():
                return
            self._storage_find_pause_generation = (
                getattr(self, "_storage_find_pause_generation", 0) + 1)
            if pev.is_set():
                pev.clear()
                self._find_pause_state = "running"
                try:
                    self._storage_find_pause_btn.config(
                        state=tk.NORMAL, text="Pause")
                    self._storage_find_cancel_btn.config(state=tk.NORMAL)
                except Exception:  # noqa: BLE001
                    pass
                self._storage_find_say(
                    "Verification resumed; remaining series continue.")
            else:
                pev.set()
                self._find_pause_state = "pausing"
                try:
                    self._storage_find_pause_btn.config(
                        state=tk.NORMAL, text="Pausing...")
                    self._storage_find_cancel_btn.config(state=tk.NORMAL)
                except Exception:  # noqa: BLE001
                    pass
                self._storage_find_say(
                    "Pausing verification - the current series finishes "
                    "atomically, then the remaining debt holds.")
            return
        if pev.is_set():                           # RESUME (from pausing/finalizing/paused)
            pev.clear()
            self._find_pause_state = "running"
            self._batch_resume()                   # un-freeze the elapsed/ETA clock
            try:
                self._storage_find_pause_btn.config(text="Pause")
                self._storage_find_cancel_btn.config(state=tk.DISABLED)   # re-gate
            except Exception:  # noqa: BLE001
                pass
            # restart on add-stock's OWN cancel event (not the update dialog's)
            self._xval_start(cancel_attr="_storage_find_ev")
            self._storage_find_say("Resumed.")
        else:                                      # PAUSE -> begin the FINALIZE sequence
            pev.set()
            self._find_pause_state = "pausing"
            try:
                self._storage_find_pause_btn.config(text="Pausing…")
            except Exception:  # noqa: BLE001
                pass
            self._find_pause_status_detail()
            self._storage_find_say(
                "Pausing — each port finishes its month, then the cross-validation "
                "drains and any gaps are SEALED. The button reaches 'Paused ✓' (and "
                "Cancel/exit unlocks) only when the bank is COMPLETE. Resume to continue.")

    def _storage_find_finalize(self) -> None:
        """Pause FINALIZE: every port has idled at its boundary — now make the bank
        COMPLETE before Cancel/exit unlocks. (1) DRAIN the cross-validation queue so
        no committed ticker is left unchecked; (2) SEAL the committed series' gaps on
        a DISTINCT client-id (CLIENT_ID_SEAL coexists with the paused fetch workers'
        CLIENT_ID_FETCH AND the dialog's search adapter); (3) report 'Paused ✓'.
        Daemon; best-effort. Re-entrancy-guarded against a 2nd concurrent seal."""
        import threading
        th0 = getattr(self, "_find_finalize_thread", None)
        if th0 is not None and th0.is_alive():
            return                  # prior daemon still draining -> poll retries
        root = self._storage_root
        job = getattr(self, "_storage_find_job", None) or {}
        series = [(t, iv) for (t, iv) in (job.get("selections") or [])
                  if stock_storage.session_of(str(iv)) == "rth"]
        ports = list(self._tws_ports() or ())
        self._find_finalize_id = getattr(self, "_find_finalize_id", 0) + 1
        fid = self._find_finalize_id              # stale-daemon guard

        def _work():
            say = lambda m: self.root.after(
                0, lambda mm=m: self._storage_find_say(mm))
            sealed, left = self._finalize_drain_and_seal(series, ports, say)
            try:
                self.root.after(0, lambda s=sealed, l=left, i=fid:
                                self._storage_find_paused_complete(s, l, i))
            except Exception:  # noqa: BLE001 — root destroyed by app-close mid-run
                pass

        try:
            t = threading.Thread(target=_work, daemon=True, name="pause-finalize")
            self._find_finalize_thread = t
            t.start()
            self._find_pause_state = "finalizing"   # commit ONLY now (real daemon)
            self._batch_pause()                     # all ports idle -> freeze elapsed
            try:
                self._storage_find_pause_btn.config(text="Finalizing…")
            except Exception:  # noqa: BLE001
                pass
            self._find_pause_status_detail(sealing=True)
        except Exception:  # noqa: BLE001 — thread exhaustion
            self._storage_find_paused_complete(0, 0, fid)

    def _storage_find_paused_complete(self, sealed, series_left=0, fid=None) -> None:
        """GUI thread: finalize done. Mark fully paused and unlock Cancel (the safe
        exit). Ignore if the user RESUMED mid-finalize, OR if this is a STALE
        daemon's callback (a newer finalize cycle exists). The wording only claims
        'bank COMPLETE' when every series was actually sealed."""
        if fid is not None and fid != getattr(self, "_find_finalize_id", 0):
            return                  # a newer finalize cycle owns the state now
        if getattr(self, "_find_pause_state", None) != "finalizing":
            return
        self._find_pause_state = "paused"
        try:
            self._storage_find_pause_btn.config(text="Paused ✓")
            self._storage_find_cancel_btn.config(state=tk.NORMAL)   # safe-exit unlocks
            self._batch_set_detail(
                getattr(self, "_storage_find_progress_lbl", None),
                "Paused ✓")
        except Exception:  # noqa: BLE001
            pass
        if series_left:
            self._storage_find_say(
                f"⏸ PAUSED — months written, cross-validation done, +{sealed:,} "
                f"gap bar(s) sealed; {series_left} series still have gaps (re-run "
                f"to finish). Safe to Cancel/close, or Resume.")
        else:
            self._storage_find_say(
                f"⏸ PAUSED — bank COMPLETE: months written, cross-validation done, "
                f"+{sealed:,} gap bar(s) sealed. Safe to Cancel/close, or Resume.")
        try:
            self._storage_ibkr_refresh()           # repaint Gaps / Earliest columns
        except Exception:  # noqa: BLE001
            pass

    def _storage_find_no_go(self, failures) -> None:
        """Preflight said no: nothing was fetched or written. Same
        wording policy as the IBKR dialog — the app being closed gets
        a direct popup; anything else pops the failure text."""
        self._storage_find_idle()
        for line in failures:
            self._storage_find_say(f"[FAIL] {line}")
        self._storage_find_say("Nothing was fetched or written.")
        if any("nothing listening" in f for f in failures):
            msg = ("TWS / IB Gateway isn't open (or isn't logged "
                   "in).\n\nStart it, log in to the PAPER account, "
                   "then press Build again.")
        else:
            msg = ("The connection check failed — nothing was "
                   "written.\n\n" + "\n\n".join(failures))
        try:
            self._show_unattended_notice(
                "Find / add stock", msg, level="warning",
                parent=self._storage_find_win)
        except Exception:  # noqa: BLE001 — headless / dialog closed
            pass

    def _storage_find_done(self, rep) -> None:
        """Render a finished build (GUI thread): report lines into the
        say box AND the storage tab's issues pane, the found company
        name into the manifest (when the fill ran and the manifest has
        none), then refresh the inventory views."""
        job = getattr(self, "_storage_find_job", None) or {}
        addstock_run_id = getattr(self, "_addstock_run_id", None)
        self._addstock_run_id = None
        self._storage_find_job = None
        self._gap_incremental_stop()
        typed_cancel = isinstance(rep, stock_ibkr.RequestCancelled)
        # A terminal engine cancellation carries UNVERIFIED evidence. Rendering
        # that evidence must not turn it into successful fetch/debt finalization.
        finish_result = (None if typed_cancel else
                         self._addstock_finish_manifest(addstock_run_id, rep))
        if addstock_run_id:
            try:
                active_recovery = addstock_run_manifest.load_run(
                    self._storage_root)
            except addstock_run_manifest.ManifestError:
                active_recovery = "unreadable"
            completed = (
                isinstance(finish_result, dict)
                and bool(finish_result.get("archived"))
                and active_recovery is None)
            self._storage_find_set_recovery_blocked(not completed)
        self._storage_find_idle()
        self._find_render_ports()                 # settle dots on done/error
        # a build changed the data bank — drop the per-session search cache so a
        # re-search reflects the new 'in data bank' status (not a frozen row).
        self._find_search_cache = {}
        if typed_cancel:
            lines = ["Build cancelled (UNVERIFIED). Committed results were kept; "
                     "unfinished recovery work remains pending."]
            attached = None
            try:
                source_report = getattr(rep, "fetch_report", None)
                ledger = getattr(rep, "fetch_ledger", None)
                if isinstance(source_report, dict):
                    attached = dict(source_report)
                    if isinstance(ledger, dict):
                        attached["fetch_ledger"] = ledger
                    else:
                        ledger = attached.get("fetch_ledger")
                    lines.extend(stock_ibkr.summarize_report(attached))
                    # Aborted summaries stop before totals; retain the engine's
                    # committed counts without implying successful completion.
                    totals = attached.get("totals")
                    if attached.get("aborted") and isinstance(totals, dict):
                        if "added" in totals and "written" in totals:
                            lines.append(f"Committed results kept: +{totals['added']:,} rows in "
                                         f"{totals['written']:,} month file(s).")
                else:
                    lines.append("Engine completion report unavailable.")
                    if isinstance(ledger, dict):
                        lines.append(f"Fetch ledger {ledger.get('state', 'UNVERIFIED')}: "
                                     f"{ledger.get('path', '(path unavailable)')}")
                if not isinstance(ledger, dict):
                    lines.append("Fetch ledger evidence unavailable; verification is not established.")
            except Exception:  # noqa: BLE001 - presentation cannot finalize recovery
                lines.append("Cancellation evidence unavailable or could not be rendered.")
            for line in lines:
                self._storage_find_say(line)
            try:
                self._storage_set_issues(lines)
                self._storage_issue_prefix = lines
                if attached is not None:
                    self._write_run_log(attached, getattr(self, "_find_run_params", None),
                                        self._storage_find_say)
            except Exception:  # noqa: BLE001 - report persistence is best effort here
                self._storage_find_say("Cancellation display log unavailable; engine evidence is unchanged.")
            return
        if isinstance(rep, Exception):
            self._storage_find_say(f"Build failed: {rep}")
            return
        try:
            lines = stock_ibkr.summarize_report(rep)
        except Exception as exc:  # noqa: BLE001
            lines = [f"(report rendering failed: {exc})"]
        for ln in lines:
            self._storage_find_say(ln)
        try:
            self._storage_set_issues(lines)
            self._storage_issue_prefix = lines   # survives the rescan
        except Exception:  # noqa: BLE001 — main window closing
            pass
        self._write_run_log(rep, getattr(self, "_find_run_params", None),
                            self._storage_find_say)
        if isinstance(rep, dict):
            self._storage_halted_series_popup(
                rep, getattr(self, "_storage_find_win", None) or self.root)
            if rep.get("fleet_down_finalize"):
                pending = len(rep.get("seal_pending") or [])
                self._storage_find_say(
                    "FLEET DOWN: fetch stopped at a recoverable boundary; "
                    f"{pending} series require seal-on-resume.", warn=True)
                try:
                    self._show_unattended_notice(
                        "Add Stocks interrupted - fleet down",
                        "All TWS ports stayed unavailable past the fleet grace "
                        "window. Committed months and port-free checks were "
                        "kept. Volatility reconciles, live probes, and gap "
                        "seals are recorded as pending; Resume the interrupted "
                        "Add Stocks run after "
                        "a port is available.",
                        level="warning",
                        parent=getattr(self, "_storage_find_win", None)
                        or self.root)
                except Exception:  # noqa: BLE001 - dialog may be closing
                    pass
            if (not rep.get("cancelled") and not rep.get("aborted")
                    and not rep.get("fleet_down_finalize")):
                self._gap_scan_after_fetch(
                    rep, say=self._storage_find_say, skip_empty=True)
                self._calendar_maintain_after_fetch(
                    rep, say=self._storage_find_say)
        ran = isinstance(rep, dict) and not rep.get("aborted")
        names = job.get("names") or {}
        if ran and names:
            # searched-for company names belong in the manifests so
            # the OFFLINE search finds these tickers by name next time
            # — never overwrite a name the user already has
            for s, nm in names.items():
                try:
                    tdir = self._storage_root / s
                    man = stock_storage.load_manifest(tdir)
                    if man and not man.get("name"):
                        man["name"] = nm
                        stock_storage.save_manifest(tdir, man)
                except Exception:  # noqa: BLE001 — cosmetic write only
                    pass
        syms = [s for s, _iv in (job.get("selections") or [])]
        if ran and syms:
            # flip the built rows' bank column so the Build button
            # reads 'Top up to latest' without a fresh search
            try:
                stored_map = {
                    r["symbol"]: r["stored"]
                    for r in _stock_find_local(self._storage_root, "")}
                for s in syms:
                    iid = self._storage_find_items.get(s)
                    if iid is not None and stored_map.get(s):
                        v = list(
                            self._storage_find_tree.item(iid)["values"])
                        v += [""] * (5 - len(v))
                        v[4] = stored_map[s]
                        self._storage_find_tree.item(iid, values=v)
                self._storage_find_sel()
            except Exception:  # noqa: BLE001 — dialog closing
                pass
        try:
            self._storage_rescan()
        except Exception:  # noqa: BLE001 — main window closing
            pass
        try:
            self._storage_ibkr_refresh()     # gap table, if it's open
        except Exception:  # noqa: BLE001 — IBKR dialog closed/never up
            pass
        # R2: unattended summary — symbols AUTO-SKIPPED at validation get a
        # saved log and an end-of-run count popup (the run never stalled on a
        # modal to ask).
        skipped = job.get("skipped") or []
        if skipped:
            logpath = None
            try:
                import datetime as _dt
                logpath = (self._storage_root /
                           f"skipped_symbols_{_dt.datetime.now():%Y%m%d-%H%M%S}"
                           f".txt")
                logpath.write_text(
                    "Symbols SKIPPED (no live SMART/USD contract at IBKR — "
                    "delisted or wrong symbol):\n" + "\n".join(skipped) + "\n",
                    encoding="utf-8")
            except Exception:  # noqa: BLE001 — could not write the log
                logpath = None
            self._storage_find_say(
                f"WARNING: {len(skipped)} stock(s) were SKIPPED (not found "
                f"at IBKR)." + (f"  Log saved: {logpath}" if logpath else ""),
                warn=True)
            try:
                self._show_unattended_notice(
                    "Build finished — some stocks skipped",
                    f"{len(skipped)} of the selected stock(s) were SKIPPED "
                    f"because IBKR has no live SMART/USD contract for them "
                    f"(delisted or wrong symbol). The rest were built.\n\n"
                    + (f"A list of the skipped symbols was saved to:\n"
                       f"{logpath}" if logpath
                       else "(the skip-log could not be written)"),
                    level="warning",
                    parent=getattr(self, "_storage_find_win", None)
                    or self.root)
            except Exception:  # noqa: BLE001 — dialog closed
                pass

    def _build_viewer_tab(self, parent) -> None:
        """Original chart view inside the Data Viewer tab."""
        paned = ttk.PanedWindow(parent, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True)

        # Left — compact "Dataset overview" card (app font, key facts + a small
        # per-column range table). weight=0 keeps it at its requested width so
        # the chart pane takes the rest; the sash is still draggable.
        left = ttk.Frame(paned)
        paned.add(left, weight=0)
        ov = ttk.Frame(left, padding=(10, 8))
        ov.pack(fill=tk.BOTH, expand=True)

        ttk.Label(ov, text="Dataset overview",
                  font=(_UI_FONT, 12, "bold")).pack(anchor="w", pady=(0, 4))

        # Message line — placeholder / errors; cleared when data is shown.
        self.ov_message = ttk.Label(ov, text="", foreground="#777",
                                    wraplength=300, justify="left")
        self.ov_message.pack(anchor="w", fill=tk.X)

        # Key facts, as tidy "label  value" rows in the default font.
        self._ov_vars = {}
        facts = ttk.Frame(ov)
        facts.pack(fill=tk.X, pady=(4, 0))
        for r, key in enumerate(("Interval", "Bars", "Distinct days",
                                 "Span", "From", "To", "Missing")):
            ttk.Label(facts, text=key, foreground="#666").grid(
                row=r, column=0, sticky="w", padx=(0, 12), pady=1)
            var = tk.StringVar(value="—")
            self._ov_vars[key] = var
            ttk.Label(facts, textvariable=var).grid(
                row=r, column=1, sticky="w", pady=1)

        ttk.Separator(ov).pack(fill=tk.X, pady=(8, 4))
        ttk.Label(ov, text="Per-column range", foreground="#666").pack(anchor="w")

        # Small stats table — expands to fill the rest of the column height.
        cols = ("col", "min", "mean", "max")
        self.ov_tree = ttk.Treeview(ov, columns=cols, show="headings", height=5)
        for c, w, anc, head in (("col", 64, "w", ""), ("min", 78, "e", "Min"),
                                ("mean", 78, "e", "Mean"),
                                ("max", 78, "e", "Max")):
            self.ov_tree.heading(c, text=head)
            self.ov_tree.column(c, width=w, anchor=anc, stretch=True)
        self.ov_tree.pack(fill=tk.BOTH, expand=True, pady=(2, 0))

        # Right — chart + range bar (takes all the extra width).
        right = ttk.Frame(paned)
        paned.add(right, weight=1)

        # All controls pack left-to-right with no labels — the buttons /
        # combobox / checkbox are self-evident. Dropping the "Range:" and
        # "Type:" labels saves ~100 px so everything fits without clipping
        # on narrower windows.
        range_bar = ttk.Frame(right)
        range_bar.pack(side=tk.TOP, fill=tk.X, pady=(0, 4))

        for label, delta in RANGE_PRESETS:
            ttk.Button(
                range_bar, text=label, width=4,
                command=lambda d=delta: self.viewer_chart.set_range(d),
            ).pack(side=tk.LEFT, padx=1)

        ttk.Button(
            range_bar, text="Reset zoom",
            command=lambda: self.viewer_chart.set_range(None),
        ).pack(side=tk.LEFT, padx=(10, 4))

        # Explicit zoom buttons (reliable on a trackpad, where plain scroll
        # pans instead of zooming).
        ttk.Button(range_bar, text="＋", width=2,
                   command=lambda: self.viewer_chart.zoom(0.7)).pack(
            side=tk.LEFT, padx=(2, 1))
        ttk.Button(range_bar, text="－", width=2,
                   command=lambda: self.viewer_chart.zoom(1.0 / 0.7)).pack(
            side=tk.LEFT, padx=(0, 4))

        # Chart-type selector: Auto / Candle / Line
        self.chart_type_var = tk.StringVar(value="Auto")
        chart_type_combo = ttk.Combobox(
            range_bar, textvariable=self.chart_type_var,
            values=["Auto", "Candle", "Line"], state="readonly", width=8,
        )
        chart_type_combo.pack(side=tk.LEFT, padx=(15, 0))
        chart_type_combo.bind("<<ComboboxSelected>>",
                              lambda _e: self._viewer_apply_controls())

        self.log_scale_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            range_bar, text="Log",
            variable=self.log_scale_var, command=self._viewer_apply_controls,
        ).pack(side=tk.LEFT, padx=(10, 4))

        # Visible chart: the same fast native-Tk-canvas renderer as the
        # Price Volume tab (smooth pan/zoom, no matplotlib).
        self.viewer_chart = CanvasChart(right, status_var=self.status)
        self.viewer_chart.configure(spikes=False)  # no orange spike guide lines








    # ------------------------------------------------------------ export
    # Map dialog direction label -> engine key.















    def _wire_drag_and_drop(self) -> None:
        if not DND_AVAILABLE:
            return
        self.drop_zone.drop_target_register(DND_FILES)
        self.drop_zone.dnd_bind("<<Drop>>", self._on_drop)


    # ------------------------------------------------------------ handlers

    def _on_browse(self) -> None:
        # Open the file dialog rooted at the folder containing this script,
        # so CSVs sitting alongside display_data.py are one click away.
        script_dir = Path(__file__).resolve().parent
        path = filedialog.askopenfilename(
            title="Select a raw OHLCV file (CSV, Parquet, or Excel)",
            initialdir=str(script_dir),
            filetypes=[("Data files", "*.csv *.parquet *.parq *.pq "
                                      "*.xlsx *.xlsm *.xls"),
                       ("CSV files", "*.csv"),
                       ("Parquet files", "*.parquet *.parq *.pq"),
                       ("Excel files", "*.xlsx *.xlsm *.xls"),
                       ("All files", "*.*")],
        )
        if path:
            self._load_path(Path(path))

    def _on_drop(self, event) -> None:
        raw = self.root.tk.splitlist(event.data)
        if not raw:
            return
        self._load_path(Path(raw[0]))
        if len(raw) > 1:        # say so instead of silently ignoring the rest
            try:
                self.status.set(self.status.get()
                                + f"   ({len(raw) - 1} more dropped file(s) "
                                  "ignored — load one at a time)")
            except Exception:  # noqa: BLE001
                pass

    def _on_interval_apply(self, _event=None) -> None:
        if self.raw_df is None:
            return
        # Custom field wins when it has text — that's how the user signals
        # "I want this exact rule, not the preset". Otherwise use the
        # selected preset.
        custom = self.custom_var.get().strip()
        preset = self.interval_var.get().strip()
        text = custom or preset
        if not text:
            self.status.set(
                "Pick a preset or type a custom interval (e.g. 10min, 2h, 3D)."
            )
            return

        if text in PRESET_LABEL_TO_RULE:
            rule = PRESET_LABEL_TO_RULE[text]
            label = text
        else:
            rule = text
            label = text

        # Validate the rule synchronously — gives instant feedback on bad
        # input without spinning up a thread.
        try:
            normalized = validate_rule(rule)
        except ValueError as exc:
            self.status.set(f"Interval error: {exc}")
            return

        # Block intervals FINER than the raw bar — you can't view data at a
        # resolution it doesn't have (e.g. 1-second / tick on 1-minute data).
        # Pop up a clear message and don't apply it.
        req_td = rule_to_timedelta(normalized)
        if (self.raw_interval is not None and req_td is not None
                and req_td < self.raw_interval):
            messagebox.showwarning(
                "Interval too fine",
                f"Can't view at “{label}”.\n\n"
                f"The loaded data's smallest bar is "
                f"{format_timedelta(self.raw_interval)}, so it can't be shown at "
                f"a finer interval ({format_timedelta(req_td)}).\n\n"
                f"Pick {format_timedelta(self.raw_interval)} or a longer one.",
                parent=self.root)
            # Don't let the rejected pick linger in the inputs.
            self.interval_var.set(self.current_label or "")
            self.custom_var.set("")
            self.apply_btn.configure(text="Apply")
            return

        # Lock the UI behind a busy modal. The resample runs on a worker
        # thread so the progress bar keeps animating. Rendering happens on
        # the main thread after the worker finishes.
        busy = self._show_busy(f"Resampling and rendering: {label} …")
        self.apply_btn.configure(state="disabled")

        raw_df = self.raw_df
        raw_interval = self.raw_interval

        def worker() -> None:
            try:
                resampled = resample_ohlcv(raw_df, normalized, raw_interval)
            except Exception as exc:  # noqa: BLE001
                self._safe_after(
                    0, lambda e=exc: self._finish_apply(busy, None, label, rule, e)
                )
                return
            self._safe_after(
                0, lambda: self._finish_apply(busy, resampled, label, rule, None)
            )

        threading.Thread(target=worker, daemon=True).start()

    def _finish_apply(self, busy, resampled, label: str, rule: str, exc) -> None:
        """Main-thread completion handler for the worker-threaded resample."""
        try:
            if exc is not None:
                self.status.set(f"Interval error: {exc}")
                return
            if resampled is None or resampled.empty:
                self.status.set("Resample produced no rows.")
                return

            self.current_df = resampled
            detected_td, _ = detect_raw_interval(resampled)
            self._current_interval = detected_td or self.raw_interval
            self.current_label = label
            self._render_current()
            self.apply_btn.configure(text="Apply")
            # Clear the custom field so the displayed inputs match what
            # actually got applied. Mirror the applied label into the
            # preset combobox text.
            self.custom_var.set("")
            self.interval_var.set(label)
            self.status.set(
                f"Interval: {label}  ({rule})  —  {len(resampled):,} bars  |  "
                f"{resampled.index.min()}  →  {resampled.index.max()}"
            )
        finally:
            self.apply_btn.configure(state="normal")
            self._hide_busy(busy)


    def _on_interval_var_changed(self, *_args) -> None:
        """Indicate on the Apply button that a change is waiting to render."""
        if not hasattr(self, "apply_btn"):
            return
        try:
            custom = self.custom_var.get().strip()
            preset = self.interval_var.get().strip()
        except (tk.TclError, AttributeError):
            return
        pending = custom or preset
        if not pending or pending == self.current_label:
            self.apply_btn.configure(text="Apply")
        else:
            self.apply_btn.configure(text="Apply ▶")


    # -- native macOS pinch via PyObjC -------------------------------------

    def _install_pinch_monitor(self) -> None:
        """Listen for NSEventTypeMagnify and forward pinch deltas into a queue.

        The Cocoa-side handler is intentionally minimal: it only reads the
        magnification scalar and calls Queue.put_nowait() — both operations
        are atomic and don't touch Tk widgets. A Tk timer (`_poll_pinch_queue`)
        drains the queue at ~30 Hz and runs the actual zoom logic from
        within Tk's own event loop, where it's safe.

        Set the DISPLAY_DATA_NO_PINCH environment variable to skip the
        monitor entirely (in case PyObjC + this Python combo proves
        unstable in some way).
        """
        if not _APPKIT_AVAILABLE or self._pinch_monitor is not None:
            return
        if os.environ.get("DISPLAY_DATA_NO_PINCH"):
            return

        pq = self._pinch_queue   # local captures — closures avoid self refs
        sq = self._scroll_queue

        def handler(ns_event):
            # CRITICAL: do not touch Tk here, do not call self.* anything,
            # do not raise. Cocoa cannot handle a Python exception bubbling
            # up out of this callback — it'll SEGFAULT the process.
            try:
                etype = int(ns_event.type())
                if etype == 22:        # NSEventTypeScrollWheel
                    precise = bool(ns_event.hasPreciseScrollingDeltas())
                    _SCROLL_PRECISE[0] = precise   # trackpad vs mouse (Tk path)
                    if precise:
                        # Trackpad swipe: Tk 9 never delivers a <MouseWheel> for
                        # these, so queue the deltas and pan from the poll loop.
                        dx = float(ns_event.scrollingDeltaX())
                        dy = float(ns_event.scrollingDeltaY())
                        if dx or dy:
                            sq.put_nowait((dx, dy))
                else:                   # NSEventTypeMagnify
                    mag = float(ns_event.magnification())
                    if mag != 0.0:
                        pq.put_nowait(mag)
            except BaseException:
                pass
            return ns_event

        try:
            self._pinch_monitor = NSEvent.addLocalMonitorForEventsMatchingMask_handler_(
                _NSEventMaskMagnify | _NSEventMaskScrollWheel, handler
            )
        except Exception:  # noqa: BLE001
            self._pinch_monitor = None
            return

        # Kick off the Tk-side poll loop (only when monitor was installed).
        self.root.after(33, self._poll_pinch_queue)

    def _poll_pinch_queue(self) -> None:
        """Drain accumulated pinch / trackpad-scroll frames from the Cocoa
        handler and apply them on Tk's own loop. A two-finger swipe pans the
        visible chart — UNLESS a modal dialog (e.g. the sweep-search popup) is
        open, in which case the swipe scrolls THAT dialog instead of leaking
        through to the chart behind it. Tk 9 delivers precise scroll only to the
        Cocoa monitor (never as <MouseWheel>), so the dialog cannot catch the
        swipe on its own — the poll loop has to route it."""
        try:
            # Drain both queues every tick so neither backs up.
            total = 0.0
            while True:
                try:
                    total += self._pinch_queue.get_nowait()
                except queue_mod.Empty:
                    break
            sdx = sdy = 0.0
            while True:
                try:
                    dx, dy = self._scroll_queue.get_nowait()
                    sdx += dx
                    sdy += dy
                except queue_mod.Empty:
                    break

            # A modal dialog owns input: the chart behind it must NOT move.
            # grab_current() is the open modal (or None). Send the swipe to its
            # scrollable canvas if it registered one; otherwise swallow it.
            grabbed = None
            try:
                grabbed = self.root.grab_current() or None
            except Exception:  # noqa: BLE001
                grabbed = None
            if grabbed is not None:
                if sdy:
                    self._scroll_modal(grabbed, sdy)
                return

            if abs(total) > 1e-6:
                # Route the pinch to whichever tab's chart is visible. Guard on
                # the chart having rendered (_geom set).
                ch = self._active_chart()
                if ch is not None:
                    step = total * 30.0
                    ch.zoom(0.95 ** step)   # public API (no-op until first render)
            # Trackpad two-finger swipe → pan the visible chart 1:1 with the
            # fingers. Sum the queued deltas, then drive the public pan.
            if sdx or sdy:
                ch = self._active_chart()
                if ch is not None:
                    ch.pan(sdx, sdy)
        except Exception:  # noqa: BLE001 — never bubble out of a Tk timer
            pass
        finally:
            # Re-arm the poll loop until the app exits.
            try:
                self.root.after(33, self._poll_pinch_queue)
            except tk.TclError:
                # Root was destroyed (app closing). Stop polling.
                pass

    def _scroll_modal(self, grabbed, sdy) -> None:
        """Scroll the open modal dialog's registered canvas by a trackpad swipe,
        so the swipe scrolls the popup instead of the chart behind it. A dialog
        opts in by setting `<toplevel>._scroll_canvas`; dialogs without one just
        swallow the swipe (the chart still stays put). Direction matches the
        chart's direct-manipulation pan and macOS natural scrolling: swipe down
        (sdy > 0) → content follows the fingers down → reveals the top."""
        try:
            canvas = getattr(grabbed.winfo_toplevel(), "_scroll_canvas", None)
            if canvas is None:
                return
            bbox = canvas.bbox("all")
            if not bbox:
                return
            height = bbox[3] - bbox[1]
            if height <= 0:
                return
            top_frac = canvas.yview()[0]
            canvas.yview_moveto(min(1.0, max(0.0, top_frac - sdy / height)))
        except Exception:  # noqa: BLE001
            pass

    # -- keyboard zoom shortcuts -------------------------------------------


    # -------------------------------------------------------- loading flow

    def _show_abnormal_popup(self, fname, bad, notes, n_total,
                             parent=None, on_decision=None):
        """LOUD import gate for abnormal raw data. Diagnose-only — the frame
        itself is never edited — but the FILE IS NOT USED unless the user
        clicks 'Bypass anyway'. Shows the first rows (timestamp + raw OHLCV +
        reason) with a '+N more' overflow line and a full time-sorted CSV
        export. Without `on_decision` it BLOCKS (main thread) and returns
        True = bypass / False = don't use; with `on_decision` it returns
        immediately and calls on_decision(bool) once the user chooses (the
        window ✕ counts as don't-use)."""
        n_bad = len(bad)
        if n_bad == 0 and not notes:
            if on_decision is not None:
                on_decision(True)        # nothing abnormal → nothing to gate
            return True
        decision = {"bypass": False, "done": False}
        dlg = tk.Toplevel(self.root)
        dlg.title(f"⚠ Abnormal data — {fname}")
        dlg.transient(parent or self.root)
        frm = ttk.Frame(dlg, padding=14)
        frm.pack(fill=tk.BOTH, expand=True)
        head = (f"{fname}: {n_bad:,} abnormal row(s) of {n_total:,} "
                f"({100.0 * n_bad / max(n_total, 1):.4f}%)"
                if n_bad else f"{fname}: values OK, but:")
        ttk.Label(frm, text=head,
                  font=(_UI_FONT, 13, "bold")).pack(anchor="w")
        ttk.Label(frm, foreground="#a33", text=
                  "Nothing was changed or dropped — but this file will NOT "
                  "be used unless you choose 'Bypass anyway' (computing on "
                  "abnormal data can give unreliable results)."
                  ).pack(anchor="w", pady=(2, 0))
        for nt in notes:
            ttk.Label(frm, text="• " + nt,
                      foreground="#a33").pack(anchor="w", pady=(2, 0))
        show_n = 10
        if n_bad:
            txt = tk.Text(frm, width=92, height=min(show_n, n_bad) + 1,
                          font=("Courier", 11), relief="solid", borderwidth=1)
            txt.pack(fill=tk.BOTH, expand=True, pady=(8, 0))

            def _fmt(v):
                try:
                    return f"{float(v):g}"
                except Exception:  # noqa: BLE001 — text cell: show verbatim
                    return str(v)

            for ts, row in bad.head(show_n).iterrows():
                vals = " ".join(f"{c[0].upper()}={_fmt(row[c])}"
                                for c in OHLCV_COLS if c in bad.columns)
                txt.insert("end", f"{ts}  {vals}  — {row['reason']}\n")
            if n_bad > show_n:
                txt.insert("end", f"… +{n_bad - show_n:,} more abnormal "
                                  "row(s) — Export log has them all")
            txt.configure(state="disabled")

        def _finish(bypass):
            if decision["done"]:
                return
            decision["done"] = True
            decision["bypass"] = bool(bypass)
            try:
                dlg.destroy()
            finally:
                if on_decision is not None:
                    on_decision(decision["bypass"])

        def _export():
            p = filedialog.asksaveasfilename(
                parent=dlg, defaultextension=".csv",
                initialfile=f"{Path(fname).stem}_abnormal_rows.csv",
                filetypes=[("CSV", "*.csv"), ("All files", "*.*")])
            if not p:
                return
            try:        # the exact raw values, already time-sorted
                bad.to_csv(p, index_label="timestamp", encoding="utf-8")
                self.status.set(f"Abnormal-row log → {p}")
            except Exception as exc:  # noqa: BLE001
                messagebox.showwarning("Export failed", str(exc), parent=dlg)

        btns = ttk.Frame(frm)
        btns.pack(fill=tk.X, pady=(10, 0))
        ttk.Button(btns, text="Export log…", command=_export).pack(side=tk.LEFT)
        ttk.Button(btns, text="Don't use this file",
                   command=lambda: _finish(False)).pack(side=tk.RIGHT)
        ttk.Button(btns, text="Bypass anyway — compute as-is",
                   command=lambda: _finish(True)).pack(side=tk.RIGHT,
                                                       padx=(0, 8))
        dlg.protocol("WM_DELETE_WINDOW", lambda: _finish(False))
        try:                        # X11: wait for the map before grabbing
            dlg.wait_visibility()
            dlg.grab_set()
        except Exception:  # noqa: BLE001
            pass
        if on_decision is None:     # blocking mode (main thread): wait here
            try:
                self.root.wait_window(dlg)
            except Exception:  # noqa: BLE001
                pass
            return decision["bypass"]
        return None

    def _load_path(self, path: Path) -> None:
        if not path.exists():
            self._set_text(f"File not found: {path}")
            self.status.set(f"File not found: {path}")
            return
        if path.suffix.lower() not in (".csv",) + PARQUET_SUFFIXES + EXCEL_SUFFIXES:
            self._set_text(f"Unsupported file type: {path.name}")
            self.status.set("Only .csv, .parquet and Excel files are supported.")
            return

        self.status.set(f"Loading {path.name} ...")
        self.root.update_idletasks()

        try:
            df = load_data(path)
        except Exception as exc:  # noqa: BLE001
            self._set_text(f"Failed to load {path.name}:\n\n{exc}")
            self.status.set(f"Error loading {path.name}")
            return

        # LOUD import gate — abnormal data is NOT computed unless the user
        # explicitly bypasses. Diagnose-only: the frame is never edited.
        try:
            _bad, _vnotes = validate_raw_ohlcv(df)
        except Exception:  # noqa: BLE001 — screen failure must not block loading
            _bad, _vnotes = [], []
        if len(_bad) or _vnotes:
            if not self._show_abnormal_popup(path.name, _bad, _vnotes,
                                             len(df)):
                self.status.set(f"Load cancelled — {path.name} has abnormal "
                                "data (nothing was loaded).")
                return

        self.raw_df = df
        self.loaded_path = path
        self.raw_interval, self.raw_interval_label = detect_raw_interval(df)
        self.current_df = df
        self._current_interval = self.raw_interval
        self.current_label = f"{self.raw_interval_label} (raw)"

        self.raw_interval_lbl.configure(text=f"(raw: {self.raw_interval_label})")
        self.interval_var.set(self.raw_interval_label)
        # Preset = readonly (only dropdown picks); Custom = free typing.
        self.interval_combo.configure(state="readonly")
        self.custom_entry.configure(state="normal")
        self.apply_btn.configure(state="normal")

        self._render_current()
        self.status.set(
            f"Loaded {path.name} — {len(df):,} bars at {self.raw_interval_label} | "
            f"{df.index.min().date()} → {df.index.max().date()}"
        )

    # -------------------------------------------------------------- render

    def _render_current(self) -> None:
        """Render freshly loaded / resampled data on the native canvas chart."""
        if self.current_df is None:
            return
        self._update_overview(self.current_df, self.current_label)
        vc = self.viewer_chart
        t = self.chart_type_var.get()
        vc.configure(kind="line" if t == "Line" else "candle",   # Auto → candle
                     log=bool(self.log_scale_var.get()), render=False)
        vc.set_data(self.current_df)                          # renders the chart

    def _viewer_apply_controls(self) -> None:
        """Push the Data Viewer's chart-type / Log controls onto the native
        canvas chart and redraw, preserving the current view."""
        vc = getattr(self, "viewer_chart", None)
        if vc is None or vc.data is None:
            return
        t = self.chart_type_var.get()
        vc.configure(kind="line" if t == "Line" else "candle",
                     log=bool(self.log_scale_var.get()))


    # -- busy modal (animated progress bar during slow Apply) --------------

    def _show_busy(
        self, message: str, total: Optional[int] = None,
        cancellable: bool = False,
    ) -> tk.Toplevel:
        """Pop a modal Toplevel with a progress bar.

        - If `total` is None: indeterminate (animated) — for unknown-length
          work like resample.
        - If `total` is an int: determinate, with a "current / total" label
          that the worker updates via `_update_busy`.
        - If `cancellable` is True: add a Cancel button that sets a
          `threading.Event` (attached as `busy._cancel_event`). The worker
          should poll this event and exit cleanly when it's set. Use
          `self._is_cancelled(busy)` to check.

        grab_set() captures all input until the dialog is destroyed.
        """
        busy = tk.Toplevel(self.root)
        busy.title("")
        busy.transient(self.root)
        busy.resizable(False, False)

        self.root.update_idletasks()
        rx, ry = self.root.winfo_rootx(), self.root.winfo_rooty()
        rw, rh = self.root.winfo_width(), self.root.winfo_height()
        bw = 420
        bh = 140 if total is not None else 110
        if cancellable:
            bh += 44  # room for the cancel button
        busy.geometry(f"{bw}x{bh}+{rx + (rw - bw) // 2}+{ry + (rh - bh) // 2}")

        frame = ttk.Frame(busy, padding=20)
        frame.pack(fill=tk.BOTH, expand=True)
        msg_lbl = ttk.Label(frame, text=message, font=(_UI_FONT, 11))
        msg_lbl.pack(pady=(0, 10))
        # Stored so later phases (e.g. chart generation) can swap the
        # message via `_start_busy_phase` without rebuilding the modal.
        busy._msg_label = msg_lbl  # type: ignore[attr-defined]

        if total is None or total <= 0:
            pb = ttk.Progressbar(frame, mode="indeterminate", length=360)
            pb.pack()
            self._start_pulse(pb)  # aqua: native self-animation (no timer);
                                   # win32/x11: 50 ms Tk timer
            busy._pb = pb          # type: ignore[attr-defined]
            busy._pb_total = None  # type: ignore[attr-defined]
            busy._pb_label = None  # type: ignore[attr-defined]
            busy._pulsing = True   # type: ignore[attr-defined]
        else:
            pb = ttk.Progressbar(
                frame, mode="determinate", length=360,
                maximum=total, value=0,
            )
            pb.pack(pady=(0, 6))
            lbl = ttk.Label(
                frame, text=f"0 / {total:,}  (0%)",
                font=(_UI_FONT, 10),
            )
            lbl.pack()
            busy._pb = pb            # type: ignore[attr-defined]
            busy._pb_total = total   # type: ignore[attr-defined]
            busy._pb_label = lbl     # type: ignore[attr-defined]
            busy._pulsing = False    # type: ignore[attr-defined]

        if cancellable:
            cancel_event = threading.Event()
            busy._cancel_event = cancel_event  # type: ignore[attr-defined]

            def _on_cancel() -> None:
                cancel_event.set()
                try:
                    cancel_btn.configure(
                        state=tk.DISABLED, text="Cancelling…"
                    )
                except Exception:  # noqa: BLE001
                    pass
                lbl_ = getattr(busy, "_pb_label", None)
                if lbl_ is not None:
                    try:
                        lbl_.configure(text="Cancelling — finishing current batch…")
                    except Exception:  # noqa: BLE001
                        pass

            cancel_btn = ttk.Button(frame, text="Cancel", command=_on_cancel)
            cancel_btn.pack(pady=(10, 0))
            busy._cancel_btn = cancel_btn  # type: ignore[attr-defined]
            # Closing the window via the OS X button == clicking Cancel.
            busy.protocol("WM_DELETE_WINDOW", _on_cancel)
        else:
            # Non-cancellable: clicking the OS close button does nothing.
            busy.protocol("WM_DELETE_WINDOW", lambda: None)

        try:                        # X11: wait for the map before grabbing
            busy.wait_visibility()  # (grab on an unmapped window -> TclError)
            busy.grab_set()
        except Exception:  # noqa: BLE001
            pass
        busy.update()
        self._busy_open.append(busy)    # tracked for the root-close guard
        return busy


    def _safe_after(self, *args):
        """root.after() that tolerates a vanished main loop.

        Worker threads schedule GUI updates with `root.after(0, …)`. If the main
        window's event loop has already exited — e.g. the app was closed while a
        background sweep was still running — Tk raises
        'RuntimeError: main thread is not in main loop' (and, late in teardown, a
        TclError). Swallow those so the daemon worker doesn't die with an
        unhandled traceback during shutdown. Returns the after-id, or None if the
        callback was dropped because the loop is gone."""
        try:
            return self.root.after(*args)
        except (RuntimeError, tk.TclError):
            return None


    # How a combo's wall time scales with its interval's bar count. NOT 1.0:
    # measured per-combo time runs only ~5x from 1min→1D while bar counts run
    # ~1400x, because the histogram cache is SHARED across intervals on the same
    # day (the finest interval pays the build once; the rest reuse it) and a
    # fixed per-combo overhead dominates. Empirically (AAPL, many grids) time ∝
    # bars**~0.25, and 0.25 gave the lowest, most consistent early-ETA error
    # (~10% vs ~120% for a flat combo count, ~75% for a full bar weight).







    @staticmethod
    def _start_pulse(pb):
        """Animate an indeterminate bar the platform-correct way. On macOS
        Aqua the native control animates ITSELF — Tk's start() timer then
        re-steps the value on top of it, and each step redraws the control
        and resets the native animation phase. Under a GIL-heavy prepare the
        timer ticks arrive in bursts, which is the visible flicker. So on
        aqua: no timer at all. win32/x11 bars don't self-animate and keep
        the 50 ms timer."""
        try:
            if str(pb.tk.call("tk", "windowingsystem")) != "aqua":
                pb.start(50)
        except Exception:  # noqa: BLE001 — worst case: a static bar
            pass

    def _hide_busy(self, busy) -> None:
        if busy is None:
            return
        try:
            self._busy_open.remove(busy)
        except (ValueError, AttributeError):
            pass
        try:
            busy.grab_release()
        except Exception:  # noqa: BLE001
            pass
        try:
            busy.destroy()
        except Exception:  # noqa: BLE001
            pass

    def _on_app_close(self):
        """Root ✕ guard. With no job running, quit at once. With a busy modal
        OR a staged Fix-data run up: confirm, fire the SAME safe-cancel flags the
        jobs already watch (so workers stop at a clean boundary and finalize),
        then quit when teardown finishes — 15 s hard cap so a wedged job can't
        trap the user."""
        live = [b for b in getattr(self, "_busy_open", [])
                if b is not None and b.winfo_exists()]
        fixdata = bool(getattr(self, "_storage_fixdata_running", False))
        if not live and not fixdata:
            self.root.destroy()
            return
        what = ("A Repair-data run" if fixdata and not live
                else "A sweep/export" if live and not fixdata
                else "Background jobs")
        if not messagebox.askyesno(
                "Job still running",
                f"{what} is still running.\n\nCancel it and quit?",
                parent=self.root, icon="warning", default="no"):
            return
        for b in live:
            ev = getattr(b, "_cancel_event", None)
            if ev is not None:
                ev.set()
        if fixdata:                       # route Fix-data through its safe path
            try:
                self._fixdata_cancel.set()
                self._fixdata_pause.clear()   # wake paused workers to see cancel
            except Exception:  # noqa: BLE001
                pass
        t0 = time.monotonic()

        def _poll():
            still = [b for b in live if b.winfo_exists()]
            busy = bool(getattr(self, "_storage_fixdata_running", False))
            if (not still and not busy) or time.monotonic() - t0 > 15.0:
                self.root.destroy()
            else:
                self.root.after(200, _poll)
        _poll()


    # ----------------------------------------------------------- text pane

    def _set_text(self, content: str) -> None:
        # The overview is now a structured card; this just drives its message
        # line (placeholder / error text).
        if hasattr(self, "ov_message"):
            self.ov_message.configure(text=content)

    def _update_overview(self, df, label) -> None:
        """Fill the compact dataset-overview card (key facts + range table)."""
        if not hasattr(self, "ov_message"):
            return
        self.ov_message.configure(text="")
        span_days = (df.index.max() - df.index.min()).days
        miss = int(df.isna().sum().sum())
        v = self._ov_vars
        v["Interval"].set(str(label))
        v["Bars"].set(f"{len(df):,}")
        v["Distinct days"].set(f"{df.index.normalize().nunique():,}")
        v["Span"].set(f"{span_days:,} days  (~{span_days / 365.25:.2f} yr)")
        v["From"].set(f"{df.index.min():%Y-%m-%d %H:%M}")
        v["To"].set(f"{df.index.max():%Y-%m-%d %H:%M}")
        v["Missing"].set("none" if miss == 0 else f"{miss:,}")
        self.ov_tree.delete(*self.ov_tree.get_children())
        for c in ("open", "high", "low", "close", "volume"):
            if c not in df.columns:
                continue
            s = df[c]
            if c == "volume":
                vals = (_volume_fmt(float(s.min()), None),
                        _volume_fmt(float(s.mean()), None),
                        _volume_fmt(float(s.max()), None))
            else:
                vals = (f"{s.min():,.2f}", f"{s.mean():,.2f}", f"{s.max():,.2f}")
            self.ov_tree.insert("", "end", values=(c, *vals))

    def _show_placeholder(self) -> None:
        hint = (
            "No data loaded.\n\n"
            "Drop a CSV, Parquet, or Excel file onto the bar above, or click Browse.\n"
            "Columns: open, high, low, close, volume + time "
            "(CSV: Date, Time; Parquet: a datetime index/column).\n\n"
            "Once loaded: hover for O/H/L/C/V, click a bar to pin it, "
            "drag to pan, and use ＋ / － or pinch to zoom."
        )
        if not DND_AVAILABLE:
            hint += ("\nDrag-and-drop is optional. To enable it, run "
                     f"\"{sys.executable}\" -m pip install tkinterdnd2 "
                     "or use --setup.\n")
        self._set_text(hint)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    global DND_AVAILABLE
    root = None
    if DND_AVAILABLE:
        try:
            root = TkinterDnD.Tk()
        except Exception:  # noqa: BLE001 — tkinterdnd2 imported but the tkdnd
            DND_AVAILABLE = False        # Tcl binary failed to load (common on
            root = None                  # Linux/ARM): run without drag-and-drop
    if root is None:
        root = tk.Tk()
    DataViewerApp(root)
    root.mainloop()


if __name__ == "__main__":
    # No-op for a normal `python display_data.py` run, but REQUIRED if the app is
    # ever frozen (PyInstaller / py2exe) on Windows: without it each spawned
    # multiprocessing worker re-launches the GUI instead of running its task.
    mp.freeze_support()
    sys.exit(main())
