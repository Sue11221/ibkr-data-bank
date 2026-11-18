"""Self-register the Data Bank midnight fleet-restart task.

The installer travels inside the project and derives every action path from
its own location.  Re-running ``--register`` after a folder move updates the
same Task Scheduler entry to the new location.  The default is a read-only
dry run; only the explicit ``--register`` action changes Task Scheduler.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:  # noqa: BLE001 - redirected/older streams
    pass

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
TASK_NAME = "Data Bank - Midnight Fleet Restart"
START_TIME = "00:05"
REPEAT_MINUTES = "30"
REPEAT_DURATION = "01:00"


class InstallerError(RuntimeError):
    """The portable task action could not be derived or registered."""


def find_restart_script(*, project_root=PROJECT_ROOT, script_dir=HERE):
    """Find the runner before or after M2b's root-to-ops organization."""
    project_root = Path(project_root).resolve()
    script_dir = Path(script_dir).resolve()
    candidates = (
        script_dir / "restart_fleet.py",
        project_root / "restart_fleet.py",
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise InstallerError(
        "restart_fleet.py is missing beside this installer and at project root")


def task_action(*, project_root=PROJECT_ROOT, script_dir=HERE,
                python_exe=None):
    """Return Task Scheduler's quoted command, derived on this machine."""
    executable = Path(python_exe or sys.executable).resolve()
    runner = find_restart_script(
        project_root=project_root, script_dir=script_dir)
    return subprocess.list2cmdline((str(executable), str(runner)))


def create_command(*, project_root=PROJECT_ROOT, script_dir=HERE,
                   python_exe=None):
    """Build the fixed, interactive daily schedule without invoking a shell."""
    return [
        "schtasks.exe", "/Create", "/F",
        "/TN", TASK_NAME,
        "/TR", task_action(
            project_root=project_root,
            script_dir=script_dir,
            python_exe=python_exe,
        ),
        "/SC", "DAILY",
        "/ST", START_TIME,
        "/RI", REPEAT_MINUTES,
        "/DU", REPEAT_DURATION,
        "/IT",
        "/RL", "LIMITED",
    ]


def query_command():
    return ["schtasks.exe", "/Query", "/TN", TASK_NAME, "/FO", "LIST", "/V"]


def plan(*, project_root=PROJECT_ROOT, script_dir=HERE, python_exe=None):
    runner = find_restart_script(
        project_root=project_root, script_dir=script_dir)
    command = create_command(
        project_root=project_root,
        script_dir=script_dir,
        python_exe=python_exe,
    )
    return {
        "kind": "ema_midnight_task_registration",
        "task_name": TASK_NAME,
        "project_root": str(Path(project_root).resolve()),
        "restart_script": str(runner),
        "schedule": {
            "start": START_TIME,
            "repeat_minutes": int(REPEAT_MINUTES),
            "duration": REPEAT_DURATION,
            "interactive_only": True,
        },
        "command": command,
    }


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument(
        "--register", action="store_true",
        help="create/update the Task Scheduler entry (system mutation)")
    actions.add_argument(
        "--check", action="store_true",
        help="query the existing task without changing it")
    actions.add_argument(
        "--dry-run", action="store_true",
        help="print the derived registration plan (default)")
    parser.add_argument(
        "--json", action="store_true",
        help="render structured output")
    return parser


def _run(command, runner):
    return runner(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def main(argv=None, *, runner=subprocess.run):
    args = _parser().parse_args(argv)
    payload = plan()
    payload["action"] = "dry_run"
    payload["system_mutation"] = False
    exit_code = 0

    if args.check:
        result = _run(query_command(), runner)
        payload.update({
            "action": "check",
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        })
        exit_code = 0 if result.returncode == 0 else 1
    elif args.register:
        if os.name != "nt":
            raise InstallerError("Task Scheduler registration requires Windows")
        result = _run(payload["command"], runner)
        payload.update({
            "action": "register",
            "system_mutation": True,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        })
        exit_code = 0 if result.returncode == 0 else 1

    if args.json or payload["action"] == "dry_run":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        if payload.get("stdout"):
            print(payload["stdout"].rstrip())
        if payload.get("stderr"):
            print(payload["stderr"].rstrip(), file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
