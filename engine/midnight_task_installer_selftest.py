"""Offline contract tests for the self-locating midnight-task installer."""
from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace


try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:  # noqa: BLE001 - redirected/older streams
    pass

ENGINE = Path(__file__).resolve().parent
ROOT = ENGINE.parent
OPS = ROOT / "ops"
sys.path.insert(0, str(ENGINE))

import check_kit  # noqa: E402


KIT = check_kit.CheckKit()
check = KIT.check
section = KIT.section


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def option(command, name):
    index = command.index(name)
    return command[index + 1]


def current_restart_script():
    candidates = (OPS / "restart_fleet.py", ROOT / "restart_fleet.py")
    return next(path for path in candidates if path.is_file())


def main():
    installer_path = OPS / "register_midnight_task.py"
    batch_path = OPS / "register_midnight_task.bat"
    restart_path = current_restart_script()
    installer = load_module("row52_midnight_installer", installer_path)
    restart = load_module("row52_restart_fleet", restart_path)

    section("[T1] current-tree self-location")
    check("installer derives the current project root from its own file",
          installer.PROJECT_ROOT == ROOT.resolve())
    check("installer finds the restart runner before or after M2b",
          installer.find_restart_script() == restart_path.resolve())
    check("fleet runner resolves the current project root",
          restart.resolve_project_root(restart_path) == ROOT.resolve())

    section("[T2] pre-move and post-move layouts")
    with tempfile.TemporaryDirectory(prefix="midnight-installer-") as temp:
        temp_root = Path(temp)
        before = temp_root / "Before Move"
        after = temp_root / "After Move"
        for root in (before, after):
            (root / "engine").mkdir(parents=True)
            (root / "ops").mkdir()
        before_runner = before / "restart_fleet.py"
        after_runner = after / "ops" / "restart_fleet.py"
        before_runner.write_text("# fixture\n", encoding="utf-8")
        after_runner.write_text("# fixture\n", encoding="utf-8")

        check("installer finds the root-level runner before M2b",
              installer.find_restart_script(
                  project_root=before, script_dir=before / "ops")
              == before_runner.resolve())
        check("installer finds the colocated ops runner after M2b",
              installer.find_restart_script(
                  project_root=after, script_dir=after / "ops")
              == after_runner.resolve())
        check("fleet runner resolves a root-level script layout",
              restart.resolve_project_root(before_runner) == before.resolve())
        check("fleet runner resolves an ops script layout",
              restart.resolve_project_root(after_runner) == after.resolve())

        fake_python = after / "Runtime" / "python.exe"
        command = installer.create_command(
            project_root=after,
            script_dir=after / "ops",
            python_exe=fake_python,
        )
        check("registration uses one fixed task identity",
              command[:5] == [
                  "schtasks.exe", "/Create", "/F", "/TN",
                  installer.TASK_NAME])
        check("schedule is daily at 00:05 with the reviewed retry window",
              option(command, "/SC") == "DAILY"
              and option(command, "/ST") == "00:05"
              and option(command, "/RI") == "30"
              and option(command, "/DU") == "01:00")
        check("schedule is limited and interactive-only",
              command.count("/IT") == 1
              and option(command, "/RL") == "LIMITED")
        action = option(command, "/TR")
        check("task action derives both executable and runner from inputs",
              str(fake_python.resolve()) in action
              and str(after_runner.resolve()) in action)
        check("a moved task action never points back to this project",
              str(ROOT.resolve()) not in action)

        section("[T3] mutation fence and copied execution")
        runner_calls = []

        def forbidden_runner(*args, **kwargs):
            runner_calls.append((args, kwargs))
            raise AssertionError("dry-run must not invoke Task Scheduler")

        dry_stdout = io.StringIO()
        with contextlib.redirect_stdout(dry_stdout):
            dry_code = installer.main(
                ["--dry-run", "--json"], runner=forbidden_runner)
        dry_payload = json.loads(dry_stdout.getvalue())
        check("dry-run is the default-safe non-mutating action",
              dry_code == 0
              and not runner_calls
              and dry_payload["action"] == "dry_run"
              and dry_payload["system_mutation"] is False)

        def fake_runner(command_arg, **kwargs):
            runner_calls.append((tuple(command_arg), kwargs))
            return SimpleNamespace(
                returncode=0, stdout="registered\n", stderr="")

        register_stdout = io.StringIO()
        with contextlib.redirect_stdout(register_stdout):
            register_code = installer.main(
                ["--register", "--json"], runner=fake_runner)
        register_payload = json.loads(register_stdout.getvalue())
        check("explicit register uses the injected no-shell runner once",
              register_code == 0
              and len(runner_calls) == 1
              and runner_calls[0][0][0] == "schtasks.exe"
              and runner_calls[0][1].get("check") is False
              and register_payload["system_mutation"] is True)

        relocated = temp_root / "Relocated Project"
        relocated_ops = relocated / "ops"
        (relocated / "engine").mkdir(parents=True)
        relocated_ops.mkdir()
        copied_installer = relocated_ops / installer_path.name
        copied_batch = relocated_ops / batch_path.name
        copied_restart = relocated_ops / "restart_fleet.py"
        shutil.copy2(installer_path, copied_installer)
        shutil.copy2(batch_path, copied_batch)
        shutil.copy2(restart_path, copied_restart)
        copied = subprocess.run(
            [sys.executable, "-B", str(copied_installer),
             "--dry-run", "--json"],
            cwd=temp_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            check=False,
        )
        copied_payload = (
            json.loads(copied.stdout) if copied.returncode == 0 else {})
        check("an untouched copied installer resolves only the copied tree",
              copied.returncode == 0
              and copied_payload.get("project_root") == str(relocated.resolve())
              and copied_payload.get("restart_script")
              == str(copied_restart.resolve())
              and str(ROOT.resolve()) not in copied.stdout,
              repr((copied.returncode, copied.stdout, copied.stderr)))
        check("copy-run did not rewrite any shipped installer file",
              digest(copied_installer) == digest(installer_path)
              and digest(copied_batch) == digest(batch_path)
              and digest(copied_restart) == digest(restart_path))

    section("[T4] one-click wrapper")
    batch = batch_path.read_text(encoding="utf-8", errors="replace")
    check("batch wrapper is self-locating and requires explicit registration",
          "%~dp0register_midnight_task.py" in batch
          and "--register" in batch
          and str(ROOT.resolve()) not in batch)
    return KIT.finish()


if __name__ == "__main__":
    raise SystemExit(main())
