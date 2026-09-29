"""Scheduled TWS fleet restart — the midnight daily-logout recovery.

User-ordered 2026-07-21 ("can you schedule at midnight to restart all ports?").
Runs from Windows Task Scheduler shortly after the ~23:55 IBKR demo daily
logoff and brings every DOWN port back via the engine's own recovery path
(`tws_launch.restart_dead` -> `relaunch_one`: close that instance, relaunch
its config dir, drive the demo login, re-bind the port). Nothing new is
invented here — this is the same machinery the GUI's restart button uses.

Fail-safe order:
  1. Operation gate HELD (a fetch/fix/export run is active) -> SKIP, exit 2.
     An active run is never disturbed; Row 38's in-app revival owns mid-run
     restarts once activated.
  2. No fleet manifest (fleet.json never recorded) -> nothing to target,
     exit 3. Run "Start up multi-port" once in the app to create it.
  3. `restart_dead`: ports whose API socket answers are left UNTOUCHED;
     each dead port is restarted one at a time (module GUI lock).

The scheduled task fires 00:05 and repeats every 30 min until 01:05, so a
port that fails while the demo's own reset window is still closing gets two
more self-healing chances; the script is idempotent (up ports untouched,
gate-busy skips cleanly).

Requires an INTERACTIVE, UNLOCKED user session (the login is driven by
image matching). If the machine is locked or rebooted overnight the pass
fails per-port and logs it — check the log below in the morning.

Log: "Run Logs/midnight_restart.log" (append; one dated block per firing).
Exit: 0 = all up / restarted, 1 = at least one port FAILED, 2 = skipped
(gate busy), 3 = no fleet manifest. `--check` reports state, changes nothing.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path


def resolve_project_root(script_path=None):
    """Resolve the project whether this runner is at root or under ``ops``."""
    script = Path(script_path or __file__).resolve()
    for candidate in (script.parent, script.parent.parent):
        if (candidate / "engine").is_dir():
            return candidate
    raise RuntimeError(
        f"cannot locate the Data Bank project from {script}")


PROJECT_ROOT = resolve_project_root()
sys.path.insert(0, str(PROJECT_ROOT / "engine"))

import operation_gate  # noqa: E402
import tws_launch  # noqa: E402

LOG_PATH = PROJECT_ROOT / "Run Logs" / "midnight_restart.log"


def _log_line(handle, message):
    stamp = dt.datetime.now().strftime("%H:%M:%S")
    line = f"{stamp} {message}"
    print(line, flush=True)
    handle.write(line + "\n")
    handle.flush()


def fleet_pairs():
    fleet = tws_launch.load_fleet()
    return [(entry.get("email"), port)
            for port, entry in sorted(fleet.items())
            if entry.get("email")]


def check_only():
    gate = operation_gate.status()
    pairs = fleet_pairs()
    print(f"operation gate available: {gate.get('available')}")
    print(f"fleet manifest ports: {[port for _e, port in pairs] or 'NONE'}")
    for email, port in pairs:
        alive = tws_launch.port_open(port)
        print(f"  port {port} ({email}): {'UP' if alive else 'DOWN'}")
    print("check-only: no restart attempted")
    return 0


def main(argv):
    if "--check" in argv:
        return check_only()
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(f"\n=== midnight restart {dt.date.today().isoformat()} "
                     f"{dt.datetime.now().strftime('%H:%M:%S')} ===\n")

        gate = operation_gate.status()
        if not gate.get("available"):
            _log_line(handle, "SKIP: operation gate is held (a run is "
                              "active) - not touching the fleet")
            return 2

        pairs = fleet_pairs()
        if not pairs:
            _log_line(handle, "NO FLEET: fleet.json has no recorded ports - "
                              "run 'Start up multi-port' once to create it")
            return 3

        _log_line(handle, f"fleet: {[port for _e, port in pairs]}")
        results = tws_launch.restart_dead(
            pairs, on_progress=lambda m: _log_line(handle, m))

        up = sorted(p for p, r in results.items() if r == "up")
        failed = sorted(p for p, r in results.items()
                        if isinstance(r, str) and r.startswith("FAILED"))
        restarted = sorted(p for p in results
                           if p not in up and p not in failed)
        _log_line(handle, f"RESULT: already-up={up} restarted={restarted} "
                          f"failed={failed}")
        return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
