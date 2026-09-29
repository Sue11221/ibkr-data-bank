"""Acceptance harness: port-free verification debt for volatilities (Row 48).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (VOL_VERIFICATION_DEBT_PLAN.md).

Offline and deterministic: temp bank via testbank, a REAL addstock_run_manifest
run driven to the exact stuck-debt state, and the REAL engine verification
functions. The GUI is never imported - its debt worker is pinned by AST reads
of the display_data.py source. Reports via the Row 41 check_kit. Exit contract:
1 = check failed; 3 = green but the M1 flags are absent (expected pre-M1);
0 = acceptance.
"""

from __future__ import annotations

import ast
import datetime as dt
import sys
import tempfile
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
sys.path.insert(0, str(ENGINE_ROOT))

from check_kit import CheckKit  # noqa: E402
import addstock_run_manifest as arm  # noqa: E402
import gap_evidence  # noqa: E402
import stock_storage as ss  # noqa: E402
import stock_validate as sv  # noqa: E402
from testbank import build_bank, isolated_gates  # noqa: E402

SOURCE = PROJECT_ROOT / "display_data.py"
MONTHS = ["2026-06", "2026-07"]
VOL_IVS = ("1m-iv", "1d-hvol")

KIT = CheckKit()
check = KIT.check
section = KIT.section


def seed(root):
    build_bank(root, {
        "VOLT": {"1m-iv": MONTHS, "1d-hvol": MONTHS,
                 "1m": MONTHS, "1d": MONTHS},
    }, bars_per_day=4, fmt="parquet")


def drive_debt_run(root):
    """Create a real run manifest holding exactly vol-series verification debt."""
    run = arm.create_run(
        root, [("VOLT", iv) for iv in VOL_IVS], params={})
    rid = run["run_id"]
    for iv in VOL_IVS:
        arm.mark_series_started(root, rid, "VOLT", iv)
        arm.mark_series_complete(root, rid, "VOLT", iv)
    arm.mark_fetch_finished(root, rid, reason="verification_debt")
    return rid


def discharge_xval(root, rid):
    # Pure structural fixture: the public HTTP-capable root is held by default.
    entries = [sv._cross_validate_ticker_body(root, "VOLT", iv) for iv in VOL_IVS]
    wrote = sv.record_cross_validation_many(root, entries)
    current = arm.current_xval_intervals(root, entries)
    if current:
        arm.mark_verification(root, rid, "VOLT", "xval", current)
    return entries, wrote, current


def discharge_gaps(root, rid):
    gap_series = [("VOLT", iv) for iv in VOL_IVS]
    scan = sv.update_gap_report(root, gap_series)
    evidence = sv.evaluate_gap_evidence(root, gap_series)
    rows = evidence.get("rows") or []
    current = [r["interval"] for r in rows if r.get("current") is True]
    if current:
        arm.mark_verification(root, rid, "VOLT", "gaps", current)
    return scan, evidence, current


def discharge_earliest(root, rid):
    sv.record_ibkr_earliest(root, "VOLT", "2026-06-01")
    data = arm.load_run(root)
    if data is not None and not data["tickers"]["VOLT"]["earliest"]:
        arm.mark_verification(root, rid, "VOLT", "earliest")


def find_method(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == name:
                return node
    return None


def attribute_names(node):
    return {sub.attr for sub in ast.walk(node)
            if isinstance(sub, ast.Attribute)}


def module_flag(module, name):
    return getattr(module, name, None) is True


def source_flag(tree, name):
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if (isinstance(target, ast.Name) and target.id == name
                        and isinstance(node.value, ast.Constant)
                        and node.value.value is True):
                    return True
    return False


def main():
    print("=== Port-free verification debt for volatilities (Row 48) ===\n")
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
    engine_flag = module_flag(gap_evidence, "KIND_SERIES_EVIDENCE")
    gui_flag = source_flag(tree, "PORTFREE_DEBT_CONTROLS")
    flags = engine_flag and gui_flag

    with tempfile.TemporaryDirectory(prefix="vol_debt_ref_") as temp:
        root = Path(temp) / ss.STORAGE_DIR_NAME
        root.mkdir(parents=True)
        seed(root)
        rid = drive_debt_run(root)

        section("[B] engine truth: which stages can discharge for vol series")
        entries, wrote, xval_current = discharge_xval(root, rid)
        check("B1 vol xval debt discharges TODAY (structural provider, "
              "current fingerprints, both series credited)",
              wrote is not None
              and all(e["provider"] == "internal-structural" for e in entries)
              and all(e["status"] in ("structural-ok", "structural-flag")
                      for e in entries)
              and sorted(xval_current) == sorted(VOL_IVS),
              repr([(e["status"], e["interval_fingerprint"]["current"])
                    for e in entries]))

        scan, evidence, gap_current = discharge_gaps(root, rid)
        sidecar = sv.load_data_gaps(root)
        sidecar_keys = sorted(sidecar.get("series") or {})
        scanned_ok = (not scan.get("error")
                      and scan.get("series_scanned") == len(VOL_IVS)
                      and sidecar_keys
                      == [f"VOLT {iv}" for iv in sorted(VOL_IVS)])
        requested = ((evidence.get("source") or {}).get("requested_series"))
        if not flags:
            check("B2 defect doc: explicitly requested kind series are "
                  "silently dropped from gap evidence (scanned + sidecar "
                  "entries exist, evaluator sees zero)",
                  scanned_ok and requested == 0 and gap_current == [],
                  f"scan_ok={scanned_ok} requested={requested} "
                  f"current={gap_current}")
            check("B3 defect doc: is_primary_price_interval rejects RTH kind "
                  "tokens (gap_evidence.py:33 root cause)",
                  not gap_evidence.is_primary_price_interval("1m-iv")
                  and not gap_evidence.is_primary_price_interval("1d-hvol")
                  and gap_evidence.is_primary_price_interval("1m"))

        discharge_earliest(root, rid)
        data = arm.load_run(root)
        if not flags:
            status = arm.summary(data) if data is not None else None
            check("B4 defect doc: after every dischargeable stage the ticker "
                  "stays built_unverified == 1 (the recurring resume prompt)",
                  data is not None
                  and data["tickers"]["VOLT"]["state"] == "built"
                  and status["built_unverified"] == 1,
                  repr(status))

        worker = find_method(tree, "_addstock_consume_portfree_debt")
        check("B5 the port-free debt worker exists in display_data source",
              worker is not None)
        refs = attribute_names(worker) if worker is not None else set()
        controls = {name for name in refs
                    if "cancel" in name or "pause" in name}
        if not flags:
            check("B6 defect doc: the debt worker references NO cancel or "
                  "pause handle (unpausable, uncancellable today)",
                  worker is not None and not controls, repr(sorted(controls)))

        if flags:
            section("[F] feature: dischargeable vol debt + controllable pass")
            check("F1 both M1 flags present "
                  "(gap_evidence.KIND_SERIES_EVIDENCE + "
                  "display_data.PORTFREE_DEBT_CONTROLS)",
                  engine_flag and gui_flag)
            check("F2 the same manifest flow now discharges END-TO-END: kind "
                  "gap rows current, gaps credited, run manifest completes",
                  sorted(gap_current) == sorted(VOL_IVS)
                  and arm.load_run(root) is None,
                  f"gap_current={gap_current} "
                  f"manifest={'archived' if arm.load_run(root) is None else 'active'}")
            check("F3 policy fences hold: extended sessions still refused and "
                  "TRADES-only discovery is unchanged on the mixed bank",
                  gap_evidence._normalize_series([("VOLT", "1m-pre")]) == []
                  and [iv for _t, iv in
                       gap_evidence.discover_primary_series(root)[0]]
                  == ["1d", "1m"],
                  repr(gap_evidence.discover_primary_series(root)[0]))
            check("F4 the debt worker wires cancel + pause and emits "
                  "per-ticker progress through _storage_find_say",
                  worker is not None
                  and any("cancel" in name for name in refs)
                  and any("pause" in name for name in refs)
                  and "_storage_find_say" in refs,
                  repr(sorted(controls)))

    if not flags:
        KIT.pending(
            "M1",
            "gap_evidence.KIND_SERIES_EVIDENCE absent - not implemented",
            "display_data.PORTFREE_DEBT_CONTROLS absent - not implemented",
            "D1: _normalize_series accepts RTH kind tokens (discovery and "
            "-pre/-post exclusion unchanged)",
            "D3: the debt worker honors the Add Stocks cancel/pause controls "
            "between series and reports per-ticker progress",
            "acceptance: this reference exit 0 x3",
        )
        return KIT.finish(feature_absent=True)
    return KIT.finish()


if __name__ == "__main__":
    with isolated_gates():
        raise SystemExit(main())
