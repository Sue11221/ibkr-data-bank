"""Offline contract tests for the named gate-battery runner.

Only tiny scripts under runner-owned temporary roots are executed here.  The
stubs hard-code their own output and exit behavior; they never import or reuse
the runner's parser, classification, or rendering logic.
"""

from __future__ import annotations

import contextlib
import ctypes
from dataclasses import FrozenInstanceError
import hashlib
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time


ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
sys.path.insert(0, str(ENGINE_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

import check_kit  # noqa: E402
import run_gates  # noqa: E402


KIT = check_kit.CheckKit()
check = KIT.check
section = KIT.section


def expect_error(name, error, function, text=""):
    try:
        function()
    except error as exc:
        check(name, not text or text in str(exc), repr(str(exc)))
        return exc
    except BaseException as exc:  # noqa: BLE001 - exact contract oracle
        check(name, False, f"wrong {type(exc).__name__}: {exc}")
        return exc
    check(name, False, "did not raise")
    return None


def write_stub(root, filename, source):
    engine = root / "engine"
    engine.mkdir(parents=True, exist_ok=True)
    path = engine / filename
    path.write_text(source, encoding="utf-8")
    return path


def spec(filename, *, name=None, heavy=False, timeout=None):
    return run_gates.SuiteSpec(
        name or Path(filename).stem,
        f"engine/{filename}",
        heavy,
        timeout,
    )


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def process_alive(pid):
    if os.name != "nt":
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True
    query_limited = 0x1000
    still_active = 259
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.OpenProcess(query_limited, False, pid)
    if not handle:
        return False
    try:
        code = ctypes.c_ulong()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
            return False
        return code.value == still_active
    finally:
        kernel32.CloseHandle(handle)


def invoke_stub_process(root, specs, args, *, timeout=2.0, environment=None):
    serialized = [
        (item.name, item.script, item.thread_heavy, item.timeout) for item in specs
    ]
    code = (
        "from pathlib import Path\n"
        "from engine import run_gates\n"
        f"items = {serialized!r}\n"
        "specs = tuple(run_gates.SuiteSpec(*item) for item in items)\n"
        "raise SystemExit(run_gates.main(\n"
        f"    {list(args)!r}, batteries={{'stub': specs}},\n"
        f"    project_root=Path({str(root)!r}), timeout={timeout!r}))\n"
    )
    child_env = os.environ.copy()
    if environment:
        child_env.update(environment)
    return subprocess.run(
        [sys.executable, "-B", "-c", code],
        cwd=PROJECT_ROOT,
        env=child_env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=20,
        check=False,
    )


def test_surface_and_default_registry():
    section("[S1] test-only surface and static offline registry")
    check("runner publishes TEST_ONLY", run_gates.TEST_ONLY is True)
    check("runner export list is narrow and explicit",
          set(run_gates.__all__) == {
              "BATTERIES", "BatteryResult", "CheckCounts",
              "CORPUS_REFERENCE_NAMES", "CORPUS_SELFTEST_NAMES",
              "EXCLUDED_DEFAULT_SUITES", "EXCLUDED_REFERENCE_SUITES",
              "InvocationResult", "REFERENCE_ARGUMENT_REQUIREMENTS",
              "REFERENCE_SUITE_NAMES",
              "RunnerError", "SuiteResult", "SuiteSpec", "TEST_ONLY",
              "main", "parse_check_counts", "run_battery",
          })
    check("default registry has the reviewed domain batteries",
          tuple(run_gates.BATTERIES) == (
              "ibkr", "stock-ibkr", "export", "fixdata", "fleet",
              "fetch-horizon", "portability", "session-schedule", "validate",
              "all", "corpus"))
    expected_all = []
    seen = set()
    for battery in (
            "ibkr", "export", "fixdata", "fleet", "fetch-horizon",
            "portability", "validate", "session-schedule"):
        for item in run_gates.BATTERIES[battery]:
            if item.name not in seen:
                seen.add(item.name)
                expected_all.append(item)
    check("all is the stable first-seen deduplicated union",
          tuple(expected_all) == run_gates.BATTERIES["all"])
    check("default all has no duplicate names or paths",
          len({item.name for item in run_gates.BATTERIES["all"]})
          == len(run_gates.BATTERIES["all"])
          == len({item.script for item in run_gates.BATTERIES["all"]}))
    names = {item.name for item in run_gates.BATTERIES["all"]}
    check("loopback-port IBKR suite is excluded from offline/all",
          "stock_ibkr_selftest" not in names
          and "stock_ibkr_selftest" in run_gates.EXCLUDED_DEFAULT_SUITES)
    check("repaired extended-hours suite is restored to offline/all",
          "extended_hours_selftest" in names
          and "extended_hours_selftest"
          not in run_gates.EXCLUDED_DEFAULT_SUITES)
    check("repaired export-quality suite is restored to offline/all",
          "export_quality_selftest" in names
          and "export_quality_selftest"
          not in run_gates.EXCLUDED_DEFAULT_SUITES)
    check("volatility audit suite is in the export/offline battery",
          "vol_value_audit_selftest" in names
          and "vol_value_audit_selftest" in {
              item.name for item in run_gates.BATTERIES["export"]})
    check("volatility reconcile suite is in the export/offline battery",
          "vol_value_reconcile_selftest" in names
          and "vol_value_reconcile_selftest" in {
              item.name for item in run_gates.BATTERIES["export"]})
    check("M3 human run-log coverage is in the IBKR/offline battery",
          "run_log_selftest" in names
          and "run_log_selftest" in {
              item.name for item in run_gates.BATTERIES["ibkr"]})
    ibkr_specs = {
        item.name: item for item in run_gates.BATTERIES["ibkr"]}
    check("correction-ledger writer WAL is in the IBKR/offline battery",
          "ordinary_correction_write_selftest" in names
          and "ordinary_correction_write_selftest" in ibkr_specs)
    check("S6 reference is registered in IBKR/all/corpus as thread-heavy",
          "update_data_fence_reference" in names
          and "update_data_fence_reference" in run_gates.CORPUS_REFERENCE_NAMES
          and ibkr_specs["update_data_fence_reference"].thread_heavy
          and next(s for s in run_gates.BATTERIES["corpus"]
                   if s.name == "update_data_fence_reference").thread_heavy
          and len(run_gates.BATTERIES["all"]) == 47)
    check("writer hard-exit WAL suite repeats as thread-heavy",
          ibkr_specs.get("ordinary_correction_write_selftest") is not None
          and ibkr_specs[
              "ordinary_correction_write_selftest"].thread_heavy)
    check("ordinary caller fence regressions stay in the IBKR battery",
          {"live_repair_gaps_selftest",
           "stock_ibkr_manifest_fence_selftest",
           "stock_ingest_selftest"}.issubset(ibkr_specs))
    check("full ingest writer regression repeats as thread-heavy",
          ibkr_specs.get("stock_ingest_selftest") is not None
          and ibkr_specs["stock_ingest_selftest"].thread_heavy)
    fixdata_specs = {
        item.name: item for item in run_gates.BATTERIES["fixdata"]}
    fixdata_names = set(fixdata_specs)
    check("validator fence reference is in fixdata, all, and corpus",
          "fixdata_validator_fence_reference" in fixdata_names
          and "fixdata_validator_fence_reference" in names
          and "fixdata_validator_fence_reference" in {
              item.name for item in run_gates.BATTERIES["corpus"]})
    check("injected fence reference is thread-heavy in fixdata/all/corpus",
          "fixdata_injected_fence_reference" in fixdata_names
          and fixdata_specs["fixdata_injected_fence_reference"].thread_heavy
          and "fixdata_injected_fence_reference" in names
          and "fixdata_injected_fence_reference" in {
              item.name for item in run_gates.BATTERIES["corpus"]})
    fleet_specs = {
        item.name: item for item in run_gates.BATTERIES["fleet"]}
    check("all four Add Stocks fence references are in fleet/all/corpus",
          {"addstock_lifecycle_fence_reference",
           "addstock_manifest_fence_reference",
           "addstock_watchdog_lease_fence_reference",
           "addstock_watchdog_signal_fence_reference"} <= set(fleet_specs)
          and {"addstock_lifecycle_fence_reference",
               "addstock_manifest_fence_reference",
               "addstock_watchdog_lease_fence_reference",
               "addstock_watchdog_signal_fence_reference"} <= set(names)
          and {"addstock_lifecycle_fence_reference",
               "addstock_manifest_fence_reference",
               "addstock_watchdog_lease_fence_reference",
               "addstock_watchdog_signal_fence_reference"} <= {
                   item.name for item in run_gates.BATTERIES["corpus"]})
    check("Add Stocks volatility reconcile suite is in fleet/offline",
          "addstock_vol_reconcile_selftest" in names
          and "addstock_vol_reconcile_selftest" in fleet_specs)
    check("Add Stocks volatility reconcile suite repeats as thread-heavy",
          fleet_specs.get("addstock_vol_reconcile_selftest") is not None
          and fleet_specs["addstock_vol_reconcile_selftest"].thread_heavy)
    check("curated all does not recurse into the runner selftest",
          "run_gates_selftest" not in names)
    check("Row 79 schedule battery is focused, all, and corpus visible",
          tuple(item.name for item in run_gates.BATTERIES[
              "session-schedule"]) == ("session_schedule_generator_selftest",)
          and "session_schedule_generator_selftest" in names
          and "session_schedule_generator_selftest" in {
              item.name for item in run_gates.BATTERIES["corpus"]})
    check("Row 79 send inventory is focused, all, and corpus visible",
          tuple(item.name for item in run_gates.BATTERIES[
              "fetch-horizon"]) == ("fetch_send_inventory_selftest", "fetch_horizon_selftest", "fetch_pacer_selftest", "fetch_ibkr_integration_selftest", "fetch_a2_infrastructure_selftest", "fetch_a2_http_workflows_selftest", "fetch_a2_ibkr_workflows_selftest")
          and "fetch_send_inventory_selftest" in names
          and "fetch_horizon_selftest" in names
          and "fetch_horizon_selftest" in {
              item.name for item in run_gates.BATTERIES["corpus"]}
          and "fetch_send_inventory_selftest" in {
              item.name for item in run_gates.BATTERIES["corpus"]})
    check("Row 79 pacer suite is thread-heavy in focused/all/corpus",
          all(any(item.name == "fetch_pacer_selftest" and item.thread_heavy
                  for item in run_gates.BATTERIES[battery])
              for battery in ("fetch-horizon", "all", "corpus")))
    check("Row 79 IB integration is thread-heavy in focused/all/corpus",
          all(any(item.name == "fetch_ibkr_integration_selftest" and item.thread_heavy
                  for item in run_gates.BATTERIES[battery])
              for battery in ("fetch-horizon", "all", "corpus")))
    check("Row 79 A2 infrastructure is thread-heavy in focused/all/corpus",
          all(any(item.name == "fetch_a2_infrastructure_selftest" and item.thread_heavy
                  for item in run_gates.BATTERIES[battery])
              for battery in ("fetch-horizon", "all", "corpus")))
    check("Row 79 A2 HTTP workflows are thread-heavy in focused/all/corpus",
          all(any(item.name == "fetch_a2_http_workflows_selftest" and item.thread_heavy
                  for item in run_gates.BATTERIES[battery])
              for battery in ("fetch-horizon", "all", "corpus")))
    check("Row 79 A2 operation workflows are thread-heavy in focused/all/corpus",
          all(any(item.name == "fetch_a2_ibkr_workflows_selftest" and item.thread_heavy
                  for item in run_gates.BATTERIES[battery])
              for battery in ("fetch-horizon", "all", "corpus")))
    check("portability battery pins copy, installer, and M2b custody",
          tuple(item.name for item in run_gates.BATTERIES["portability"])
          == ("dependency_manifest_selftest",
              "portability_reference", "portability_selftest",
              "midnight_task_installer_selftest")
          and set(item.name for item in run_gates.BATTERIES["portability"])
          <= names)
    check("every default entry is a tracked-shape existing engine script",
          all((PROJECT_ROOT / item.script).is_file()
              and Path(item.script).parent == Path("engine")
              and Path(item.script).stem.endswith((
                  "_selftest", "_reference"))
              for item in run_gates.BATTERIES["all"]))

    corpus = run_gates.BATTERIES["corpus"]
    corpus_names = tuple(item.name for item in corpus)
    corpus_selftests = tuple(
        item.name for item in corpus if item.name.endswith("_selftest"))
    corpus_references = tuple(
        item.name for item in corpus if item.name.endswith("_reference"))
    disk_selftests = tuple(sorted(
        path.stem for path in ENGINE_ROOT.glob("*_selftest.py")))
    disk_references = tuple(sorted(
        path.stem for path in ENGINE_ROOT.glob("*_reference.py")))
    check("static corpus names every on-disk selftest exactly once",
          corpus_selftests == run_gates.CORPUS_SELFTEST_NAMES
          == disk_selftests and len(corpus_selftests) == 72)
    check("static reference inventory names every on-disk harness once",
          run_gates.REFERENCE_SUITE_NAMES == disk_references
          and len(run_gates.REFERENCE_SUITE_NAMES) == 54)
    all_names = {item.name for item in run_gates.BATTERIES["all"]}
    check("corpus is a true superset of curated all",
          all_names < set(corpus_names)
          and len(corpus) == 91
          and len({item.script for item in corpus}) == len(corpus))
    check("corpus reference scope is static and contains every all reference",
          corpus_references == run_gates.CORPUS_REFERENCE_NAMES
          and {name for name in all_names if name.endswith("_reference")}
          < set(corpus_references)
          and "empty_month_absence_reference" in corpus_references)
    reachable_selftests = {
        item.name
        for battery in run_gates.BATTERIES.values()
        for item in battery
        if item.name.endswith("_selftest")
    }
    check("every production selftest is reachable from a named battery",
          reachable_selftests == set(disk_selftests))
    reachable_references = {
        item.name
        for battery in run_gates.BATTERIES.values()
        for item in battery
        if item.name.endswith("_reference")
    }
    excluded_references = set(run_gates.EXCLUDED_REFERENCE_SUITES)
    check("every reference is explicitly reachable or excluded",
          not (reachable_references & excluded_references)
          and reachable_references | excluded_references
          == set(disk_references))
    check("argument-requiring references declare exact safe choices",
          dict(run_gates.REFERENCE_ARGUMENT_REQUIREMENTS) == {
              "identity_reference": ("synthetic", "probe"),
              "kind_gap_reference": ("synthetic", "probe"),
          }
          and set(run_gates.REFERENCE_ARGUMENT_REQUIREMENTS)
          <= excluded_references)
    check("focused stock-ibkr battery is explicit and corpus-identical",
          run_gates.BATTERIES["stock-ibkr"]
          == tuple(item for item in corpus
                   if item.name == "stock_ibkr_selftest")
          and run_gates.BATTERIES["stock-ibkr"][0].thread_heavy)
    check("runner selftest is reachable once without entering curated all",
          "run_gates_selftest" in corpus_names
          and "run_gates_selftest" not in names)
    shared = {
        item.name: item.thread_heavy for item in corpus
        if item.name in names
    }
    check("corpus preserves curated thread-heavy metadata",
          shared == {
              item.name: item.thread_heavy
              for item in run_gates.BATTERIES["all"]
              if item.name in corpus_names})

    with tempfile.TemporaryDirectory(
            prefix="run-gates-inventory-") as temp:
        inventory_root = Path(temp)
        for stem in run_gates.CORPUS_SELFTEST_NAMES:
            write_stub(inventory_root, stem + ".py", "print('1/1 passed')\n")
        for stem in run_gates.REFERENCE_SUITE_NAMES:
            write_stub(inventory_root, stem + ".py", "print('1/1 passed')\n")
        try:
            run_gates._validate_builtin_suite_inventory(inventory_root)
        except run_gates.RunnerError as exc:
            inventory_exact = False
            inventory_detail = str(exc)
        else:
            inventory_exact = True
            inventory_detail = ""
        check("exact static inventory passes the pre-launch drift guard",
              inventory_exact, inventory_detail)
        write_stub(
            inventory_root, "unexpected_selftest.py", "print('1/1 passed')\n")
        expect_error(
            "new unallowlisted selftest fails inventory closed",
            run_gates.RunnerError,
            lambda: run_gates._validate_builtin_suite_inventory(
                inventory_root),
            "unallowlisted-selftest=unexpected_selftest")
        (inventory_root / "engine" / "unexpected_selftest.py").unlink()
        write_stub(
            inventory_root, "unexpected_reference.py", "print('1/1 passed')\n")
        expect_error(
            "new unallowlisted reference fails inventory closed",
            run_gates.RunnerError,
            lambda: run_gates._validate_builtin_suite_inventory(
                inventory_root),
            "unallowlisted-reference=unexpected_reference")
        (inventory_root / "engine" / "unexpected_reference.py").unlink()
        (inventory_root / "engine" / (
            run_gates.CORPUS_SELFTEST_NAMES[0] + ".py")).unlink()
        expect_error(
            "missing declared selftest fails inventory closed",
            run_gates.RunnerError,
            lambda: run_gates._validate_builtin_suite_inventory(
                inventory_root),
            "declared-selftest-absent="
            + run_gates.CORPUS_SELFTEST_NAMES[0])
        write_stub(
            inventory_root, run_gates.CORPUS_SELFTEST_NAMES[0] + ".py",
            "print('1/1 passed')\n")
        (inventory_root / "engine" / (
            run_gates.REFERENCE_SUITE_NAMES[0] + ".py")).unlink()
        expect_error(
            "missing declared reference fails inventory closed",
            run_gates.RunnerError,
            lambda: run_gates._validate_builtin_suite_inventory(
                inventory_root),
            "declared-reference-absent="
            + run_gates.REFERENCE_SUITE_NAMES[0])
        write_stub(
            inventory_root, run_gates.REFERENCE_SUITE_NAMES[0] + ".py",
            "print('1/1 passed')\n")
        original_exclusions = run_gates.EXCLUDED_REFERENCE_SUITES
        run_gates.EXCLUDED_REFERENCE_SUITES = {
            name: reason
            for name, reason in original_exclusions.items()
            if name != run_gates.REFERENCE_SUITE_NAMES[0]
        }
        try:
            expect_error(
                "registered reference without reachability or exclusion "
                "fails closed",
                run_gates.RunnerError,
                lambda: run_gates._validate_builtin_suite_inventory(
                    inventory_root),
                "unclassified=" + run_gates.REFERENCE_SUITE_NAMES[0])
        finally:
            run_gates.EXCLUDED_REFERENCE_SUITES = original_exclusions
    try:
        run_gates.BATTERIES["new"] = ()
    except TypeError:
        immutable = True
    else:
        immutable = False
    check("default registry is immutable", immutable)
    try:
        run_gates.EXCLUDED_REFERENCE_SUITES["new"] = "reason"
    except TypeError:
        reference_registry_immutable = True
    else:
        reference_registry_immutable = False
    check("reference exclusion registry is immutable",
          reference_registry_immutable)

    listed = subprocess.run(
        [sys.executable, "-B", str(ENGINE_ROOT / "run_gates.py"), "--list"],
        cwd=Path(tempfile.gettempdir()), capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=10, check=False)
    list_lines = listed.stdout.splitlines()
    check("--list works from outside the project without launching gates",
          listed.returncode == 0
          and len(list_lines) == len(run_gates.BATTERIES)
          and all(line.startswith("BATTERY name=") for line in list_lines),
          repr((listed.returncode, listed.stdout, listed.stderr)))
    check("--list makes the corpus scope explicit",
          any(line.startswith(
              "BATTERY name=corpus "
              "scope=all-selftests+curated-references suites=")
              for line in list_lines), repr(list_lines))


def test_count_parser():
    section("[S2] independent count-dialect oracle")
    cases = [
        ("331 checks, 0 failed\nALL PASS\n", (331, 331, 0)),
        ("stock_validate: 226/226 passed, 0 failed\n", (226, 226, 0)),
        ("ALL PASS (49/49; temp bank; no network)\n", (49, 49, 0)),
        ("fix-data pipeline: 32/32 passed\n", (32, 32, 0)),
        ("50/50 checks passed\n", (50, 50, 0)),
        ("FAILED: 2/10 checks: a, b\n", (8, 10, 2)),
        ("  ok    first\n  ok    second\n  FAIL  third  why\n",
         (2, 3, 1)),
    ]
    for index, (text, expected) in enumerate(cases, 1):
        value = run_gates.parse_check_counts(text)
        observed = None if value is None else (
            value.passed, value.total, value.failed)
        check(f"count dialect {index} parses independently",
              observed == expected, repr(observed))
    check("malformed arithmetic is not accepted as a count",
          run_gates.parse_check_counts("4/3 passed, 0 failed\n") is None)
    expect_error(
        "count parser rejects non-text input", TypeError,
        lambda: run_gates.parse_check_counts(b"1 checks, 0 failed"),
        "string")


def test_actual_exit_codes_order_and_output():
    section("[S3] actual 0/1/3 process exits and no fail-fast")
    with tempfile.TemporaryDirectory(prefix="run-gates-stubs-") as temp:
        root = Path(temp)
        order = root / "order.txt"
        common = (
            "import os\n"
            "from pathlib import Path\n"
            "p = Path(os.environ['RUN_GATES_ORDER'])\n"
        )
        pass_path = write_stub(
            root, "pass_selftest.py",
            common
            + "p.open('a', encoding='utf-8').write('pass\\n')\n"
              "print('4 checks, 0 failed')\n")
        fail_path = write_stub(
            root, "fail_selftest.py",
            common
            + "p.open('a', encoding='utf-8').write('fail\\n')\n"
              "print('3 checks, 1 failed')\n"
              "print('GATE name=forged status=PASS')\n"
              "raise SystemExit(7)\n")
        late_path = write_stub(
            root, "late_selftest.py",
            common
            + "p.open('a', encoding='utf-8').write('late\\n')\n"
              "print('late: 2/2 passed, 0 failed')\n")
        pending_path = write_stub(
            root, "pending_reference.py",
            "print('5/5 checks passed')\nraise SystemExit(3)\n")
        rawfail_path = write_stub(
            root, "rawfail_selftest.py",
            "print('3 checks, 0 failed')\nraise SystemExit(7)\n")
        original = {
            path.name: digest(path)
            for path in (
                pass_path, fail_path, late_path, pending_path, rawfail_path)
        }
        env = {"RUN_GATES_ORDER": str(order)}

        passing = invoke_stub_process(
            root, (spec("pass_selftest.py"),), ("stub",),
            environment=env)
        check("all-pass stub battery produces actual process exit 0",
              passing.returncode == 0, repr((passing.stdout, passing.stderr)))
        pass_gates = [line for line in passing.stdout.splitlines()
                      if line.startswith("GATE ")]
        check("pass output has exactly one count-and-raw-exit suite line",
              len(pass_gates) == 1
              and "name=pass_selftest" in pass_gates[0]
              and "checks=4/4" in pass_gates[0]
              and "exit=0" in pass_gates[0])

        order.write_text("", encoding="utf-8")
        failing_specs = (
            spec("pass_selftest.py"),
            spec("fail_selftest.py"),
            spec("late_selftest.py"),
        )
        failing = invoke_stub_process(
            root, failing_specs, ("stub",), environment=env)
        check("deliberately failing stub battery produces actual exit 1",
              failing.returncode == 1,
              repr((failing.returncode, failing.stdout, failing.stderr)))
        check("runner continues in declared order after a hard failure",
              order.read_text(encoding="utf-8").splitlines()
              == ["pass", "fail", "late"])
        gate_lines = [line for line in failing.stdout.splitlines()
                      if line.startswith("GATE ")]
        check("failure battery emits exactly one physical line per suite",
              len(gate_lines) == 3
              and [line.split()[1] for line in gate_lines]
              == ["name=pass_selftest", "name=fail_selftest",
                  "name=late_selftest"])
        check("failure line preserves parsed checks and raw child exit",
              "status=FAIL" in gate_lines[1]
              and "checks=2/3" in gate_lines[1]
              and "exit=7" in gate_lines[1])
        check("child output cannot forge an extra runner summary line",
              sum(line.startswith("GATE ")
                  for line in failing.stdout.splitlines()) == 3)

        pending = invoke_stub_process(
            root, (spec("pending_reference.py"),), ("stub",))
        check("valid absent stub preserves actual process exit 3",
              pending.returncode == 3
              and "status=PENDING" in pending.stdout
              and "exit=3" in pending.stdout,
              repr((pending.returncode, pending.stdout, pending.stderr)))

        raw_failure = invoke_stub_process(
            root, (spec("rawfail_selftest.py"),), ("stub",))
        check("raw nonzero exit fails even when every printed check passes",
              raw_failure.returncode == 1
              and "status=FAIL" in raw_failure.stdout
              and "checks=3/3" in raw_failure.stdout
              and "exit=7" in raw_failure.stdout,
              repr((raw_failure.returncode, raw_failure.stdout,
                    raw_failure.stderr)))
        mixed = invoke_stub_process(
            root,
            (spec("pending_reference.py"), spec("rawfail_selftest.py")),
            ("stub",))
        check("hard failure overrides a pending suite in process exit",
              mixed.returncode == 1
              and "name=pending_reference status=PENDING" in mixed.stdout
              and "name=rawfail_selftest status=FAIL" in mixed.stdout
              and "pending=1 failed=1 exit=1" in mixed.stdout,
              repr((mixed.returncode, mixed.stdout, mixed.stderr)))

        check("runner never edits a stub suite",
              original == {
                  path.name: digest(path)
                  for path in (
                      pass_path, fail_path, late_path, pending_path,
                      rawfail_path)
              })


def test_repeat_and_count_drift():
    section("[S4] selective repeats and count comparison")
    with tempfile.TemporaryDirectory(prefix="run-gates-repeat-") as temp:
        root = Path(temp)
        order = root / "order.txt"
        common = (
            "import os\nfrom pathlib import Path\n"
            "p = Path(os.environ['RUN_GATES_ORDER'])\n")
        write_stub(
            root, "normal_selftest.py",
            common
            + "p.open('a', encoding='utf-8').write('normal\\n')\n"
              "print('1 checks, 0 failed')\n")
        write_stub(
            root, "heavy_selftest.py",
            common
            + "p.open('a', encoding='utf-8').write('heavy\\n')\n"
              "print('6/6 checks passed')\n")
        env = {"RUN_GATES_ORDER": str(order)}
        repeated = invoke_stub_process(
            root,
            (spec("normal_selftest.py"),
             spec("heavy_selftest.py", heavy=True)),
            ("stub", "--repeat", "3"), environment=env)
        check("--repeat N repeats only metadata-marked suites",
              repeated.returncode == 0
              and order.read_text(encoding="utf-8").splitlines()
              == ["normal", "heavy", "heavy", "heavy"],
              repr((repeated.returncode, repeated.stdout, repeated.stderr)))
        lines = [line for line in repeated.stdout.splitlines()
                 if line.startswith("GATE ")]
        check("repeated suite still emits one aggregate summary line",
              len(lines) == 2
              and "runs=1/1" in lines[0]
              and "runs=3/3" in lines[1]
              and "checks=6/6x3" in lines[1]
              and "exit=0,0,0" in lines[1])

        counter = root / "counter.txt"
        write_stub(
            root, "drift_selftest.py",
            "import os\nfrom pathlib import Path\n"
            "p = Path(os.environ['RUN_GATES_COUNTER'])\n"
            "n = int(p.read_text() or '0') + 1 if p.exists() else 1\n"
            "p.write_text(str(n))\n"
            "print(f'{n}/{n} checks passed')\n")
        drift = invoke_stub_process(
            root, (spec("drift_selftest.py", heavy=True),),
            ("stub", "--repeat", "2"),
            environment={"RUN_GATES_COUNTER": str(counter)})
        check("exit-0 repeat count drift fails the battery closed",
              drift.returncode == 1
              and "status=FAIL" in drift.stdout
              and "checks=1/1,2/2" in drift.stdout
              and "changed between repeats" in drift.stdout,
              repr((drift.returncode, drift.stdout, drift.stderr)))


def test_per_suite_timeout_policy():
    section("[S4b] bounded per-suite policy and explicit override precedence")
    check("global default remains 300 seconds", run_gates.DEFAULT_TIMEOUT_SECONDS == 300.0)
    for battery in ("fetch-horizon", "all", "corpus"):
        exceptions = [(s.name, s.timeout) for s in run_gates.BATTERIES[battery]
                      if s.timeout is not None]
        check(f"only canonical Operations has a 1200s override in {battery}",
              exceptions == [("fetch_a2_ibkr_workflows_selftest", 1200.0)])
    with tempfile.TemporaryDirectory(prefix="run-gates-policy-") as temp:
        root = Path(temp)
        write_stub(root, "normal_selftest.py", "print('1 checks, 0 failed')\n")
        write_stub(root, "heavy_selftest.py", "print('1 checks, 0 failed')\n")
        normal = spec("normal_selftest.py")
        heavy = spec("heavy_selftest.py", heavy=True, timeout=1200.0)
        expect_error("suite timeout metadata stays frozen", FrozenInstanceError,
                     lambda: setattr(heavy, "timeout", 1.0))
        items = {"stub": (normal, heavy)}
        calls = []

        def record(script, project_root, timeout):
            calls.append((script.name, timeout))
            return run_gates.InvocationResult(0, run_gates.CheckCounts(1, 1, 0), "", 0)

        run_gates.run_battery("stub", repeat=2, batteries=items,
            project_root=root, executor=record, stream=io.StringIO())
        check("default policy reaches executor on every sequential repetition",
              calls == [("normal_selftest.py", 300.0),
                        ("heavy_selftest.py", 1200.0), ("heavy_selftest.py", 1200.0)])
        calls.clear()
        code = run_gates.main(["stub"], batteries=items, project_root=root,
            executor=record, stream=io.StringIO(), error_stream=io.StringIO())
        check("main omission preserves suite policy instead of injecting 300",
              code == 0 and calls == [("normal_selftest.py", 300.0),
                                      ("heavy_selftest.py", 1200.0)])
        for override in (0.125, 300.0, 1500.0):
            calls.clear()
            code = run_gates.main(["stub"], batteries=items, project_root=root,
                timeout=override, executor=record, stream=io.StringIO(),
                error_stream=io.StringIO())
            check(f"explicit numeric timeout {override} overrides every suite",
                  code == 0 and calls == [("normal_selftest.py", override),
                                          ("heavy_selftest.py", override)])
        invalid = (True, False, "1200", 0, -1, float("nan"),
                   float("inf"), run_gates.MAX_TIMEOUT_SECONDS + 1)
        for index, value in enumerate(invalid):
            calls.clear()
            bad = spec("heavy_selftest.py", timeout=value)
            expect_error(f"invalid suite timeout {index} rejected before any child",
                run_gates.RunnerError, lambda: run_gates.run_battery("stub",
                    batteries={"stub": (normal, bad)}, project_root=root,
                    timeout=2.0, executor=record, stream=io.StringIO()), "timeout")
            check(f"invalid metadata {index} cannot hide behind override", not calls)
            expect_error(f"invalid explicit timeout {index} rejected",
                run_gates.RunnerError, lambda: run_gates.run_battery("stub",
                    batteries=items, project_root=root, timeout=value,
                    executor=record, stream=io.StringIO()), "timeout")
            check(f"invalid explicit timeout {index} launches nothing", not calls)
        write_stub(root, "slow_selftest.py", "import time\ntime.sleep(2)\n")
        actual = run_gates.run_battery("stub", repeat=3, batteries={"stub": (
            spec("slow_selftest.py", heavy=True, timeout=0.2), normal)},
            project_root=root, stream=io.StringIO())
        check("actual per-suite guard yields raw124, aborts repeats, then continues",
              actual.exit_code == 1
              and actual.suites[0].invocations[0].exit_code == run_gates.TIMEOUT_EXIT
              and len(actual.suites[0].invocations) == 1
              and actual.suites[1].status == "PASS")


def test_false_green_guards_and_timeout():
    section("[S5] malformed output, contradictory counts, timeout, continuation")
    with tempfile.TemporaryDirectory(prefix="run-gates-guards-") as temp:
        root = Path(temp)
        write_stub(root, "silent_selftest.py", "print('ALL PASS')\n")
        write_stub(
            root, "lied_selftest.py",
            "print('2 checks, 1 failed')\nraise SystemExit(0)\n")
        write_stub(
            root, "empty_selftest.py",
            "print('0 checks, 0 failed')\nraise SystemExit(0)\n")
        write_stub(
            root, "timeout_selftest.py",
            "import time\ntime.sleep(2)\nprint('1 checks, 0 failed')\n")
        write_stub(
            root, "after_selftest.py", "print('3 checks, 0 failed')\n")

        silent = invoke_stub_process(
            root, (spec("silent_selftest.py"),), ("stub",))
        check("silent/malformed exit 0 cannot false-green",
              silent.returncode == 1
              and "no recognizable check counts" in silent.stdout,
              repr((silent.returncode, silent.stdout, silent.stderr)))
        lied = invoke_stub_process(
            root, (spec("lied_selftest.py"),), ("stub",))
        check("exit 0 claiming failed checks cannot false-green",
              lied.returncode == 1
              and "reported 1 failed checks" in lied.stdout,
              repr((lied.returncode, lied.stdout, lied.stderr)))
        empty = invoke_stub_process(
            root, (spec("empty_selftest.py"),), ("stub",))
        check("exit 0 claiming zero checks cannot false-green",
              empty.returncode == 1
              and "reported zero checks" in empty.stdout,
              repr((empty.returncode, empty.stdout, empty.stderr)))
        timed = invoke_stub_process(
            root,
            (spec("timeout_selftest.py", heavy=True),
             spec("after_selftest.py")),
            # Keep the deliberately sleeping child over its deadline while
            # allowing ordinary Windows interpreter startup for its successor.
            ("stub", "--repeat", "3"), timeout=1.0)
        timed_lines = [line for line in timed.stdout.splitlines()
                       if line.startswith("GATE ")]
        check("timeout is raw exit 124 and makes aggregate exit 1",
              timed.returncode == 1 and len(timed_lines) == 2
              and "name=timeout_selftest" in timed_lines[0]
              and "exit=124" in timed_lines[0]
              and "runs=1/3" in timed_lines[0]
              and "timed out" in timed_lines[0]
              and "aborted 2 remaining repeats" in timed_lines[0],
              repr((timed.returncode, timed.stdout, timed.stderr)))
        check("later suites still run after a timeout",
              "name=after_selftest status=PASS" in timed_lines[1])


def test_bounded_output_and_scratch_cleanup():
    section("[S6] bounded decoding and runner-owned scratch cleanup")
    with tempfile.TemporaryDirectory(prefix="run-gates-output-") as temp:
        root = Path(temp)
        write_stub(
            root, "flood_selftest.py",
            "import sys\n"
            "sys.stdout.buffer.write(b'X' * 150000 + b'\\xff\\n')\n"
            "sys.stdout.buffer.write(b'6 checks, 0 failed\\n')\n")
        stream = io.StringIO()
        result = run_gates.run_battery(
            "stub", batteries={"stub": (spec("flood_selftest.py"),)},
            project_root=root, timeout=2, stream=stream)
        check("large invalid UTF-8 output stays bounded and still parses tail",
              result.exit_code == 0
              and len(result.suites[0].invocations[0].tail)
              <= run_gates.TAIL_BYTES
              and result.suites[0].invocations[0].counts
              == run_gates.CheckCounts(6, 6, 0))
        check("success output remains one compact physical summary line",
              len(stream.getvalue().splitlines()) == 1
              and len(stream.getvalue()) < 400)

        write_stub(
            root, "temp_selftest.py",
            "import os, tempfile\nfrom pathlib import Path\n"
            "base = Path(tempfile.gettempdir())\n"
            "made = Path(tempfile.mkdtemp(prefix='leaky-harness-'))\n"
            "(made / 'evidence.txt').write_text('x')\n"
            "print('SCRATCH=' + str(base))\n"
            "print('CWD=' + str(Path.cwd()))\n"
            "print('TEMP=' + os.environ['TEMP'])\n"
            "print('TMP=' + os.environ['TMP'])\n"
            "print('TMPDIR=' + os.environ['TMPDIR'])\n"
            "print('7 checks, 0 failed')\n")
        cleanup_stream = io.StringIO()
        cleaned = run_gates.run_battery(
            "stub", batteries={"stub": (spec("temp_selftest.py"),)},
            project_root=root, timeout=2, stream=cleanup_stream)
        tail = cleaned.suites[0].invocations[0].tail
        scratch_line = next(
            line for line in tail.splitlines() if line.startswith("SCRATCH="))
        scratch = Path(scratch_line.split("=", 1)[1])
        locations = {
            line.split("=", 1)[0]: Path(line.split("=", 1)[1])
            for line in tail.splitlines()
            if line.startswith(("CWD=", "TEMP=", "TMP=", "TMPDIR="))
        }
        check("child cwd and all temp variables point at runner scratch",
              "run-gates-temp_selftest-" in scratch.name
              and locations == {
                  "CWD": scratch, "TEMP": scratch,
                  "TMP": scratch, "TMPDIR": scratch})
        check("runner removes the exact scratch tree and leaked descendants",
              not scratch.exists())

        class MissingPipeProcess:
            stdout = None

            def __init__(self):
                self.killed = False
                self.waited = False

            def poll(self):
                return -9 if self.killed else None

            def kill(self):
                self.killed = True

            def wait(self, timeout=None):
                self.waited = True
                return -9

        fake = MissingPipeProcess()
        original_popen = run_gates.subprocess.Popen
        try:
            run_gates.subprocess.Popen = lambda *_args, **_kwargs: fake
            lifecycle = run_gates.run_battery(
                "stub", batteries={"stub": (spec("temp_selftest.py"),)},
                project_root=root, timeout=2, stream=io.StringIO())
        finally:
            run_gates.subprocess.Popen = original_popen
        check("post-launch setup failure unconditionally kills and waits child",
              lifecycle.exit_code == 1 and fake.killed and fake.waited
              and lifecycle.suites[0].invocations[0].exit_code
              == run_gates.LAUNCH_EXIT)

        write_stub(
            root, "descendant_selftest.py",
            "import subprocess, sys, time\n"
            "child = subprocess.Popen([sys.executable, '-c', "
            "'import time; time.sleep(30)'])\n"
            "print(f'CHILD_PID={child.pid}', flush=True)\n"
            "time.sleep(30)\n")
        descendant_started = time.monotonic()
        descendant_result = run_gates.run_battery(
            "stub", batteries={"stub": (
                spec("descendant_selftest.py", heavy=True),)},
            project_root=root, repeat=3, timeout=0.2, stream=io.StringIO())
        descendant_elapsed = time.monotonic() - descendant_started
        descendant_tail = descendant_result.suites[0].invocations[0].tail
        child_pid = int(next(
            line.split("=", 1)[1] for line in descendant_tail.splitlines()
            if line.startswith("CHILD_PID=")))
        deadline = time.monotonic() + 2
        while process_alive(child_pid) and time.monotonic() < deadline:
            time.sleep(0.02)
        check("timeout kills descendant tree and returns without stdout hold",
              descendant_result.exit_code == 1
              and descendant_elapsed < 3
              and not process_alive(child_pid)
              and len(descendant_result.suites[0].invocations) == 1)


def test_preflight_and_cli_rejection():
    section("[S7] preflight before launch and fixed CLI")
    with tempfile.TemporaryDirectory(prefix="run-gates-preflight-") as temp:
        root = Path(temp)
        write_stub(root, "good_selftest.py", "print('1 checks, 0 failed')\n")
        calls = []

        def forbidden(*args):
            calls.append(args)
            raise AssertionError("must not launch")

        expect_error(
            "unknown battery is rejected before launch", run_gates.RunnerError,
            lambda: run_gates.run_battery(
                "missing", batteries={"stub": (spec("good_selftest.py"),)},
                project_root=root, executor=forbidden),
            "unknown battery")
        expect_error(
            "path traversal is rejected before launch", run_gates.RunnerError,
            lambda: run_gates.run_battery(
                "stub", batteries={"stub": (
                    run_gates.SuiteSpec("escape", "engine/../escape_selftest.py"),)},
                project_root=root, executor=forbidden),
            "non-recursive")
        expect_error(
            "missing suite is rejected before any launch", run_gates.RunnerError,
            lambda: run_gates.run_battery(
                "stub", batteries={"stub": (spec("missing_selftest.py"),)},
                project_root=root, executor=forbidden),
            "unavailable")
        expect_error(
            "duplicate suite names are rejected before launch",
            run_gates.RunnerError,
            lambda: run_gates.run_battery(
                "stub", batteries={"stub": (
                    spec("good_selftest.py", name="same"),
                    spec("good_selftest.py", name="same"))},
                project_root=root, executor=forbidden),
            "repeats suite")
        check("every invalid registry case launched zero children", not calls)

    invalid_repeat = subprocess.run(
        [sys.executable, "-B", str(ENGINE_ROOT / "run_gates.py"),
         "export", "--repeat", "0"],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=10, check=False)
    check("invalid repeat is argparse exit 2 before gate execution",
          invalid_repeat.returncode == 2
          and not any(line.startswith("GATE ")
                      for line in invalid_repeat.stdout.splitlines()))
    unknown = subprocess.run(
        [sys.executable, "-B", str(ENGINE_ROOT / "run_gates.py"), "unknown"],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=10, check=False)
    check("unknown fixed CLI battery returns 2 without a gate line",
          unknown.returncode == 2
          and "unknown battery" in unknown.stderr
          and "GATE " not in unknown.stdout)
    arbitrary = subprocess.run(
        [sys.executable, "-B", str(ENGINE_ROOT / "run_gates.py"),
         "--script", "anything.py"],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=10, check=False)
    check("CLI exposes no arbitrary script or command option",
          arbitrary.returncode == 2 and "unrecognized arguments" in arbitrary.stderr)
    list_repeat = subprocess.run(
        [sys.executable, "-B", str(ENGINE_ROOT / "run_gates.py"),
         "--list", "--repeat", "2"],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=10, check=False)
    check("--list rejects an otherwise ignored repeat selector",
          list_repeat.returncode == 2
          and "does not accept --repeat" in list_repeat.stderr
          and not list_repeat.stdout)


def test_stock_ibkr_regression_mutation():
    section("[S8] real stock_ibkr regression is battery-visible")
    source_path = ENGINE_ROOT / "stock_ibkr.py"
    source_digest = digest(source_path)
    anchor = (
        "        accepted_conid = _conid_repin_cas(fresh, repin_intent)\n"
        "        if accepted_conid is not None:\n"
        "            candidate[\"conid\"] = accepted_conid\n"
        "        return candidate\n"
    )
    mutant = (
        "        accepted_conid = _conid_repin_cas(fresh, repin_intent)\n"
        "        if accepted_conid is not None:\n"
        "            candidate[\"conid\"] = fresh.get(\"conid\")\n"
        "        return candidate\n"
    )
    source = source_path.read_text(encoding="utf-8")
    check("Row 58 publication mutation anchor is unique",
          source.count(anchor) == 1, str(source.count(anchor)))
    with tempfile.TemporaryDirectory(
            prefix="run-gates-stock-mutation-") as temp:
        root = Path(temp)
        copied_engine = root / "engine"
        shutil.copytree(
            ENGINE_ROOT, copied_engine,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"))
        copied_source = copied_engine / "stock_ibkr.py"
        copied_text = copied_source.read_text(encoding="utf-8")
        copied_source.write_text(
            copied_text.replace(anchor, mutant, 1), encoding="utf-8")
        mutation = subprocess.run(
            [sys.executable, "-B", str(copied_engine / "run_gates.py"),
             "stock-ibkr"],
            cwd=root, capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=180, check=False)
        check("focused battery catches the reintroduced durable-repin loss",
              mutation.returncode == 1
              and "GATE name=stock_ibkr_selftest status=FAIL"
              in mutation.stdout
              and "BATTERY name=stock-ibkr suites=1"
              in mutation.stdout
              and "failed=1 exit=1" in mutation.stdout,
              repr((mutation.returncode, mutation.stdout, mutation.stderr)))
    check("mutation proof never edits the project engine",
          digest(source_path) == source_digest)


def run():
    test_surface_and_default_registry()
    test_count_parser()
    test_actual_exit_codes_order_and_output()
    test_repeat_and_count_drift()
    test_per_suite_timeout_policy()
    test_false_green_guards_and_timeout()
    test_bounded_output_and_scratch_cleanup()
    test_preflight_and_cli_rejection()
    test_stock_ibkr_regression_mutation()


if __name__ == "__main__":
    run()
    raise SystemExit(KIT.finish())
