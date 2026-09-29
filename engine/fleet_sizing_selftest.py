"""Offline selftests for Row 74 fleet sizing and GUI source wiring."""
from __future__ import annotations

import ast
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
ENGINE = ROOT / "engine"
sys.path.insert(0, str(ENGINE))

import fleet_sizing as fs  # noqa: E402

GIB = 1024 ** 3
CHECKS: list[tuple[str, bool]] = []


def check(name, ok, detail=""):
    passed = bool(ok)
    CHECKS.append((name, passed))
    line = f"[{'PASS' if passed else 'FAIL'}] {name}"
    if detail and not passed:
        line += f" :: {detail}"
    print(line)


def _method_map(tree):
    return {
        node.name: node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def test_policy_edges():
    check("fallback is the documented 1.6 GiB",
          fs.DEFAULT_PER_INSTANCE_BYTES == 8 * GIB // 5)
    check("sampling accepts case and executable suffix",
          fs.sample_per_instance_bytes(
              [("TWS.EXE", GIB), (r"C:\\Jts\\tws.exe", 3 * GIB)])
          == 2 * GIB)
    check("sampling ignores malformed and non-positive rows",
          fs.sample_per_instance_bytes(
              [("tws", 0), ("tws", -1), ("python", GIB), ("bad",)])
          is None)
    check("sampling accepts a generator",
          fs.sample_per_instance_bytes(
              (("tws", value) for value in (GIB, 2 * GIB, 9 * GIB)))
          == 2 * GIB)
    check("working-set extraction is TWS-only and positive",
          fs.tws_working_set_bytes([
              ("tws.exe", GIB), ("python", 9 * GIB),
              ("TWS", -1), (r"C:\\Jts\\tws.exe", 2 * GIB), ("bad",),
          ]) == [GIB, 2 * GIB])
    check("estimate clamps malformed and negative inputs",
          fs.estimate_bytes(-1, GIB) == 0
          and fs.estimate_bytes(2, -GIB) == 0
          and fs.estimate_bytes("bad", GIB) == 0)
    check("zero-cost policy can fill its cap",
          fs.auto_select(GIB, 0, cap=8) == 8)
    check("explicit floor is capped and may override reserve",
          fs.auto_select(0, 2 * GIB, cap=8, floor=1) == 1
          and fs.auto_select(0, 2 * GIB, cap=3, floor=9) == 3)
    check("invalid reserve fails safely to zero capacity",
          fs.auto_select(16 * GIB, 2 * GIB, reserve_frac=1.0) == 0
          and fs.auto_select(16 * GIB, 2 * GIB, reserve_frac=-0.1) == 0)
    check("effective free is pure, additive, and non-negative",
          fs.effective_free_bytes(3 * GIB, [GIB, GIB // 2])
          == 4 * GIB + GIB // 2
          and fs.effective_free_bytes(-GIB, [-1, "bad"]) == 0
          and fs.effective_free_bytes(GIB, None) == GIB)
    normal_free = fs.effective_free_bytes(
        int(2.7 * GIB), [640 * 1024 * 1024] * 8)
    check("verified-cap normal-state calibration recommends exactly five",
          fs.auto_select(normal_free, fs.CAPPED_PER_INSTANCE_BYTES) == 5)
    check("external memory pressure structurally backs AUTO below five",
          1 <= fs.auto_select(5 * GIB,
                              fs.CAPPED_PER_INSTANCE_BYTES) <= 4)
    check("cap and reserve policy constants are exact",
          fs.CAPPED_PER_INSTANCE_BYTES == 5 * GIB // 4
          and fs.DEFAULT_RESERVE_FRAC == 0.15
          and fs.UNCAPPED_RESERVE_FRAC == 0.25)
    check("uncapped fallback retains the old 25% conservative result",
          fs.auto_select(
              8 * GIB, fs.DEFAULT_PER_INSTANCE_BYTES,
              reserve_frac=fs.UNCAPPED_RESERVE_FRAC) == 3
          and fs.auto_select(
              8 * GIB, fs.DEFAULT_PER_INSTANCE_BYTES,
              reserve_frac=fs.DEFAULT_RESERVE_FRAC) == 4)
    check("over-AUTO warning is authoritative and single-line",
          "recommend" in fs.OVER_AUTO_WARNING.lower()
          and "port start" in fs.OVER_AUTO_WARNING.lower()
          and "\n" not in fs.OVER_AUTO_WARNING)
    check("numeric estimate label is exact",
          fs.format_estimate(6, 2 * GIB, 16 * GIB)
          == "6 port(s) ≈ 12.0 GiB of ~16.0 GiB free")
    check("cap-aware estimate names effective free honestly",
          fs.format_estimate(
              5, fs.CAPPED_PER_INSTANCE_BYTES, int(7.7 * GIB),
              effective=True)
          == "5 port(s) ≈ 6.2 GiB of ~7.7 GiB effective free")
    check("unavailable-free label stays honest and single-line",
          fs.format_estimate(1, fs.DEFAULT_PER_INSTANCE_BYTES, None)
          == "1 port(s) ≈ 1.6 GiB; free RAM unavailable")


def test_gui_source_wiring():
    source = (ROOT / "display_data.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    methods = _method_map(tree)
    required = {
        "_tws_working_set_rows", "_storage_multiport_choose_count",
        "_storage_multiport_start", "_storage_restart_dead_start",
    }
    check("all sizing integration methods exist",
          required <= methods.keys(), sorted(required - methods.keys()))
    if not required <= methods.keys():
        return

    sample = ast.unparse(methods["_tws_working_set_rows"])
    choose = ast.unparse(methods["_storage_multiport_choose_count"])
    start = ast.unparse(methods["_storage_multiport_start"])
    restart = ast.unparse(methods["_storage_restart_dead_start"])

    check("process measurement is ephemeral stdlib probing",
          "tasklist" in sample and "ps" in sample
          and "subprocess.run" in sample and "psutil" not in sample)
    check("dialog samples both free RAM and live TWS working sets at open",
          "_available_ram_bytes()" in choose
          and "_tws_working_set_rows()" in choose
          and "fleet_sizing.sample_per_instance_bytes" in choose
          and "fleet_sizing.tws_working_set_bytes" in choose)
    check("dialog uses capped effective-free and conservative fallback paths",
          "fleet_sizing.effective_free_bytes" in choose
          and "fleet_sizing.CAPPED_PER_INSTANCE_BYTES" in choose
          and "fleet_sizing.DEFAULT_RESERVE_FRAC" in choose
          and "fleet_sizing.UNCAPPED_RESERVE_FRAC" in choose
          and "cap_verified" in choose)
    check("dialog uses fallback, AUTO policy, and authoritative formatter",
          "fleet_sizing.DEFAULT_PER_INSTANCE_BYTES" in choose
          and "fleet_sizing.auto_select" in choose
          and "fleet_sizing.format_estimate" in choose
          and "effective=cap_verified" in choose)
    check("dialog offers bounded spinner and explicit over-budget override",
          "ttk.Spinbox" in choose and "from_=1" in choose and "to=cap" in choose
          and "if count > auto_count" in choose
          and "messagebox.askyesno" in choose)
    check("one over-AUTO warning feeds the label and confirmation",
          choose.count("fleet_sizing.OVER_AUTO_WARNING") == 2)
    check("dialog explains measured, reclaimable, and effective free RAM",
          "Effective free RAM" in choose
          and "measured_free" in choose and "reclaimable" in choose)
    check("dialog is modal and returns only a confirmed count",
          "top.grab_set()" in choose and "self.root.wait_window(top)" in choose
          and "result['count'] = count" in choose)
    check("startup launches the first N fleet pairs",
          "self._storage_multiport_choose_count(memory_cap_result)" in start
          and "self._STANDARD_FLEET[:count]" in start
          and "emails = [f\"{chr(ord('a') + i)}@gmail.com\" for i in range(n)]"
          in start)
    check("startup installs the cap before selection and carries its result",
          "tws_vmoptions.ensure_memory_cap(tws_executable)" in start
          and start.index("tws_vmoptions.ensure_memory_cap")
          < start.index("self._storage_multiport_choose_count")
          < start.index("tws_launch.launch_many")
          and "memory_cap_result=memory_cap_result" in start)
    check("startup preserves launch/enable transcript plumbing",
          "recorder=recorder" in start and "ports=ports" in start
          and "enable_fleet(pairs, on_progress=say, recorder=recorder)" in start)
    check("restart-dead flow is not rewired through startup sizing",
          "_storage_multiport_choose_count" not in restart
          and "fleet_sizing" not in restart)


def main():
    test_policy_edges()
    test_gui_source_wiring()
    failed = [name for name, ok in CHECKS if not ok]
    print(f"{len(CHECKS)} checks, {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
