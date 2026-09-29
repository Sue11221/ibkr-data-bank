# Data Bank

> **Status: work in progress.** The engine and its offline test battery are
> usable today; packaging, documentation and the release process are still
> being built, and interfaces may change without notice.

Data Bank is a local desktop application and storage engine for acquiring,
validating, inspecting, repairing and exporting equity market data (daily and
one-minute bars) through the Interactive Brokers TWS API, with cross-checks
against public daily references.

The market data itself is **not** part of this repository. The application
builds and maintains its own `Stock Data Storage/` folder from your own
brokerage data access.

## Layout

- `display_data.py` is the desktop application (Tkinter); `launch_app.py` is
  its portable launcher and `Start Data Bank.bat` the Windows shortcut.
- `engine/` contains the storage, validation, IBKR, export, fleet and offline
  test machinery used by the application. Every `*_selftest.py` and
  `*_reference.py` in it is an offline test suite.
- `ops/` contains portable operational helpers, including the self-registering
  midnight fleet-restart installer for Windows Task Scheduler.
- `tools/` contains the session-schedule generator and its evidence.
- `Stock Data Storage/`, `Run Logs/`, `scans/`, `_quarantine/` and
  `_derived_daily_cache/` are created at run time and are git-ignored.

## Setup

Use Python 3.14. Open `Start Data Bank.bat` (Windows) or run
`python launch_app.py`. The startup check requires only the packages needed for
the app and the data bank; missing optional features do not stop the app.

For an explicit installation run `python display_data.py --setup` with the
interpreter you intend to use, or `python -m pip install -r requirements.txt`.
Optional drag-and-drop (`tkinterdnd2`), Excel input (`openpyxl`/`xlrd`), chart
overlays (`pillow`), Windows TWS auto-login (`pyautogui`/`opencv-python`) and
macOS pinch (`pyobjc-framework-Cocoa`) can be installed only if wanted.

Do not register the midnight scheduled task just to set up the app. When you
want the nightly update, run `ops/register_midnight_task.bat` interactively on
Windows and confirm its dry-run output and Task Scheduler entry.

## Tests

The offline battery runs without a broker, network or data bank:

    python engine/run_gates.py all

Individual suites run directly, for example
`python engine/stock_storage_selftest.py`. The full battery takes roughly
20 minutes on a laptop; the IBKR workflow suite alone is the largest.

## Safety model

Every network send site is inventoried (`engine/fetch_send_inventory.py
--check`) and refused by default; live fetching is enabled deliberately, path
by path, after its offline acceptance. Data files are written through
manifests and fences so that a partial session can never be committed as a
completed bar.

## Portability

Project paths are derived from the files being executed. After copying or
moving the folder, rerun `ops/register_midnight_task.bat` to update the one
Windows Task Scheduler integration; its default action is a non-mutating dry
run, and registration requires the explicit `--register` choice.

## Read-only status surface

`.mcp.json` declares a stdio MCP server (`engine/tier0_mcp.py`) exposing eight
fixed-root, offline, read-only status tools over the local data bank.
