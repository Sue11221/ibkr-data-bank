"""Offline Row 95 setup contract: one manifest, bounded gate, copied launch.

All install, restart, import-absence, and relocated-copy cases are simulated.
No pip, network, GUI, bank, or scheduled-task action is performed.
"""

from __future__ import annotations

import ast
import builtins
import hashlib
import importlib.util
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from unittest.mock import patch


ENGINE = Path(__file__).resolve().parent
ROOT = ENGINE.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ENGINE))

import check_kit  # noqa: E402
import dependency_manifest as manifest  # noqa: E402


KIT = check_kit.CheckKit()
check = KIT.check
section = KIT.section
MARKER = "EMA_DATA_BANK_DEP_RESTARTED"


def source_paths():
    return (ROOT / "display_data.py", ROOT / "launch_app.py",
            *sorted(ENGINE.glob("*.py")),
            *sorted((ROOT / "ops").glob("*.py")))


def direct_imports():
    local = {path.stem for path in ROOT.glob("*.py")}
    local.update(path.stem for path in ENGINE.glob("*.py"))
    local.update(path.stem for path in (ROOT / "ops").glob("*.py"))
    local.update(("engine", "ops"))
    found = set()
    for path in source_paths():
        tree = ast.parse(path.read_text(encoding="utf-8"), str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots = (alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
                roots = (node.module.split(".")[0],)
            else:
                continue
            for name in roots:
                if name not in sys.stdlib_module_names and name not in local:
                    found.add(name)
    return found


def indirect_imports():
    display = ast.parse((ROOT / "display_data.py").read_text(encoding="utf-8"))
    excel_engines = set()
    uses_excel_engine = False
    for node in ast.walk(display):
        if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == "engine"
                for target in node.targets):
            excel_engines.update(child.value for child in ast.walk(node.value)
                                 if isinstance(child, ast.Constant)
                                 and child.value in {"xlrd", "openpyxl"})
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                and node.func.attr == "read_excel":
            uses_excel_engine |= any(
                kw.arg == "engine" and isinstance(kw.value, ast.Name)
                and kw.value.id == "engine" for kw in node.keywords)
    api = ast.parse((ENGINE / "tws_api.py").read_text(encoding="utf-8"))
    confidence_locator = any(
        isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr == "locateCenterOnScreen"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "pyautogui"
        and any(kw.arg == "confidence" for kw in node.keywords)
        for node in ast.walk(api))
    return ((excel_engines if uses_excel_engine else set())
            | ({"cv2"} if confidence_locator else set()))


def bootstrap_functions():
    """Compile only the real pre-import gate functions, never the GUI module."""
    source = ROOT / "display_data.py"
    tree = ast.parse(source.read_text(encoding="utf-8"), str(source))
    names = {"_dep_is_installed", "required_packages", "optional_packages",
             "ensure_dependencies", "_bootstrap_dependencies",
             "_restart_program", "_setup_main"}
    functions = [node for node in tree.body
                 if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in functions} == names
    namespace = {"__file__": str(source), "sys": sys, "os": os,
                 "Path": Path, "_dependencies": manifest,
                 "_RESTART_MARKER": MARKER,
                 "REQUIRED_PACKAGES": manifest.packages("required"),
                 "OPTIONAL_PACKAGES": manifest.packages("optional")}
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), "exec"),
         namespace)
    return namespace


def manifest_contract():
    section("[M] manifest, AST imports and generated pip text")
    rows = manifest.DEPENDENCIES
    names = [row.import_name for row in rows]
    pips = [row.pip_name for row in rows]
    check("manifest has one unique pip/import pair per dependency",
          len(names) == len(set(names)) == len(pips) == len(set(pips)) == 11)
    check("every row has a valid tier, feature and OS filter",
          all(row.tier in {"required", "optional"} and row.feature
              and set(row.os_filter) <= {"Windows", "Darwin", "Linux"}
              for row in rows))
    check("only the four bank/app essentials block startup",
          {row.import_name for row in rows if row.tier == "required"}
          == {"pandas", "numpy", "pyarrow", "ib_async"})
    check("Windows and macOS features remain OS-scoped",
          {row.import_name for row in manifest.for_platform("Windows")}
          - {row.import_name for row in manifest.for_platform("Linux")}
          == {"pyautogui", "cv2"}
          and {row.import_name for row in manifest.for_platform("Darwin")}
          - {row.import_name for row in manifest.for_platform("Linux")}
          == {"AppKit"})
    direct, indirect = direct_imports(), indirect_imports()
    check("every direct third-party import maps to the manifest",
          direct <= set(names), repr(sorted(direct - set(names))))
    check("Excel engines and the OpenCV confidence backend are AST-bound",
          indirect == {"openpyxl", "xlrd", "cv2"}, repr(sorted(indirect)))
    used = direct | indirect
    check("no manifest entry is unused by direct or indirect imports",
          set(names) == used, repr(sorted(set(names) ^ used)))
    check("matplotlib is absent from both shipped imports and manifest",
          "matplotlib" not in used and "matplotlib" not in pips)
    rendered = manifest.render_requirements().encode("utf-8")
    check("requirements.txt byte-equals the one manifest rendering",
          (ROOT / "requirements.txt").read_bytes() == rendered)
    check("pip file includes both tiers and OS environment markers",
          b"# Required packages" in rendered and b"# Optional packages" in rendered
          and b'sys_platform == "win32"' in rendered
          and b'sys_platform == "darwin"' in rendered)
    check("an omitted or invented manifest entry makes coverage fail",
          set(names[:-1]) != used and set(names + ["invented"]) != used)


def gate_contract():
    section("[G] patched find_spec, no real installation or restart")
    gate = bootstrap_functions()
    queries, gui, installs, restarts = [], [], [], []

    def present(name):
        queries.append(name)
        return object()

    gate["detect_os"] = lambda: "Windows"
    gate["_dependency_installer_gui"] = lambda *args: gui.append(args) or False
    gate["install"] = lambda name: installs.append(name) or True
    real_restart = gate["_restart_program"]
    gate["_restart_program"] = lambda: restarts.append(True)
    with patch.object(importlib.util, "find_spec", side_effect=present):
        gate["_bootstrap_dependencies"]()
    check("present REQUIRED packages start without GUI, install or restart",
          not gui and not installs and not restarts)
    check("startup never even probes optional package presence",
          queries == [name for _pip, name in manifest.packages("required")],
          repr(queries))

    gate["_dependency_installer_gui"] = lambda *args: gui.append(args) or True
    for missing_pip, missing_import in manifest.packages("required"):
        gui.clear(); installs.clear(); restarts.clear()

        def one_missing(name):
            return None if name == missing_import else object()

        with patch.object(importlib.util, "find_spec", side_effect=one_missing):
            gate["_bootstrap_dependencies"]()
        check(f"missing required {missing_pip} reaches only its blocking popup",
              len(gui) == 1 and gui[0][0] == [(missing_pip, missing_import)]
              and not installs and not restarts)

    queries.clear()
    gui.clear()
    gate["_dependency_installer_gui"] = lambda *args: gui.append(args) or False

    def missing_pandas_and_optional(name):
        queries.append(name)
        return None if name in {"pandas", "tkinterdnd2"} else object()

    with patch.object(importlib.util, "find_spec",
                      side_effect=missing_pandas_and_optional), \
            patch("sys.stdout", io.StringIO()):
        gate["_bootstrap_dependencies"]()
    check("headless gate installs only the missing required package",
          installs == ["pandas"] and len(gui) == 1
          and gui[0][0] == [("pandas", "pandas")]
          and restarts == [True], repr((installs, gui, restarts)))
    check("absent optional alongside required is neither probed nor installed",
          not set(queries) & {row.import_name for row in manifest.DEPENDENCIES
                              if row.tier == "optional"}
          and not set(installs) & {row.pip_name for row in manifest.DEPENDENCIES
                                   if row.tier == "optional"},
          repr((queries, installs)))

    gui.clear(); installs.clear(); restarts.clear()
    with patch.object(importlib.util, "find_spec",
                      side_effect=missing_pandas_and_optional), \
            patch.dict(os.environ, {MARKER: "1"}), \
            patch("sys.stdout", io.StringIO()):
        try:
            gate["_bootstrap_dependencies"]()
        except SystemExit as exc:
            halted = exc.code == 1
        else:
            halted = False
    check("marked child fails after one restart without another attempt",
          halted and not gui and not installs and not restarts)

    # The spawned child receives the marker; the parent's environment does not.
    if MARKER not in os.environ:
        gate["_restart_program"] = real_restart
        gate["_restart_command"] = lambda: [sys.executable, "display_data.py"]
        launched = []
        with patch("os.execve", side_effect=OSError("simulated")), \
                patch("subprocess.Popen", side_effect=lambda cmd, **kw:
                      launched.append((cmd, kw)) or object()), \
                patch("os._exit", side_effect=SystemExit(0)):
            try:
                gate["_restart_program"]()
            except SystemExit as exc:
                exited = exc.code == 0
            else:
                exited = False
        check("restart marks exactly one spawned child, not the parent",
              exited and len(launched) == 1
              and launched[0][1]["env"][MARKER] == "1"
              and MARKER not in os.environ)
    else:
        check("restart marker belongs only to a child in this test", False)

    setup_installs = []
    gate["print_diagnostics"] = lambda: None
    gate["upgrade_pip"] = lambda: None
    gate["_dep_is_installed"] = lambda _name: False
    gate["install"] = lambda name: setup_installs.append(name) or True
    with patch("sys.stdout", io.StringIO()):
        gate["_setup_main"]()
    check("explicit --setup offers exactly the current-OS manifest set",
          setup_installs == [row.pip_name for row in manifest.for_platform()],
          repr(setup_installs))

    app_launcher = (ROOT / "Start Data Bank.bat").read_text(encoding="utf-8")
    task_launcher = (ROOT / "ops" / "register_midnight_task.bat").read_text(
        encoding="utf-8")
    check("both launchers prefer pinned 3.14 before PATH then py -3",
          all("%~dp0" in source
              and all(token in source for token in
                      ("pythoncore-3.14-64", "where.exe", "py -3"))
              and source.find("pythoncore-3.14-64") < source.find("where.exe")
              < source.find("py -3") for source in
              (app_launcher, task_launcher)))


_ABSENCE_CODE = r'''
import builtins, importlib.util, sys
target = sys.argv[1]
old_import = builtins.__import__
old_find = importlib.util.find_spec
def hidden_import(name, *args, **kwargs):
    if name.split(".")[0] == target:
        raise ImportError("hidden optional " + target)
    return old_import(name, *args, **kwargs)
def hidden_find(name, *args, **kwargs):
    if name.split(".")[0] == target:
        return None
    return old_find(name, *args, **kwargs)
builtins.__import__ = hidden_import
importlib.util.find_spec = hidden_find
import display_data as app
assert target not in {name for _, name in app.REQUIRED_PACKAGES}
if target == "PIL":
    assert app._HAVE_PIL is False
if target == "tkinterdnd2":
    assert app.DND_AVAILABLE is False
'''


def _process_detail(result):
    """Keep failed subprocess evidence within CheckKit's one-line contract."""
    return " ".join((result.stdout + result.stderr).split())[-500:]


def absence_contract():
    section("[A] each optional package hidden during real app import")
    for row in manifest.for_platform():
        if row.tier != "optional":
            continue
        result = subprocess.run(
            [sys.executable, "-B", "-c", _ABSENCE_CODE, row.import_name],
            cwd=ROOT, capture_output=True, text=True, timeout=40,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        check(f"absence of optional {row.pip_name} still imports the app",
              result.returncode == 0,
              _process_detail(result))

    import display_data as app
    with tempfile.TemporaryDirectory(prefix="row95-excel-") as folder:
        for suffix, engine in ((".xlsx", "openpyxl"), (".xls", "xlrd")):
            path = Path(folder) / ("input" + suffix)
            path.write_bytes(b"offline fixture")
            with patch.object(app.pd, "read_excel", side_effect=ImportError(engine)):
                try:
                    app._load_excel(path)
                except ImportError as exc:
                    message = str(exc)
                else:
                    message = ""
            check(f"missing {engine} gives an interpreter-specific feature hint",
                  engine in message and sys.executable in message)


def copied_contract():
    section("[C] relocated source copy and unchanged source custody")
    custody = [*source_paths(), ROOT / "dependency_manifest.py",
               ROOT / "requirements.txt"]
    before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in custody}
    with tempfile.TemporaryDirectory(prefix="row95-copy-") as folder:
        copy = Path(folder) / "Relocated Data Bank"
        (copy / "engine").mkdir(parents=True)
        for path in ROOT.glob("*.py"):
            shutil.copy2(path, copy / path.name)
        for path in ENGINE.glob("*.py"):
            shutil.copy2(path, copy / "engine" / path.name)
        shutil.copy2(ROOT / "requirements.txt", copy / "requirements.txt")
        script = (
            "import launch_app, display_data, dependency_manifest as m\n"
            "from pathlib import Path\n"
            "root = Path.cwd().resolve()\n"
            "assert Path(display_data.__file__).resolve().parent == root\n"
            "assert Path(m.__file__).resolve().parent == root\n"
            "assert display_data.REQUIRED_PACKAGES == m.packages('required')\n"
            "assert (root/'requirements.txt').read_bytes() == "
            "m.render_requirements().encode('utf-8')\n"
        )
        result = subprocess.run(
            [sys.executable, "-B", "-c", script], cwd=copy,
            capture_output=True, text=True, timeout=60,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": str(copy)})
        check("copied launch chain resolves app and manifest from copied root",
              result.returncode == 0,
              _process_detail(result))
    check("relocated import writes no changed source bytes",
          before == {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                     for path in custody})


def main():
    manifest_contract()
    gate_contract()
    absence_contract()
    copied_contract()
    return KIT.finish()


if __name__ == "__main__":
    raise SystemExit(main())
