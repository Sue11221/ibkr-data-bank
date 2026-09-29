"""Acceptance harness: complete removal of the ticked-gap / ignore-list feature (Row 65).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (STORAGE_OPTIMIZATION_LOG.md, Row 65).

Removal polarity: the "feature" being delivered is an ABSENCE. While the
ticked-gap plumbing still exists this harness exits 3 (removal pending); after
a complete removal every check passes and it exits 0. Exit 1 = a check failed
(incl. over-deletion of the ADJACENT source-absent logic, which shares an
expression with the ignore subtraction in scan_series_gaps).

Offline and headless: temp-dir bank fixtures only; display_data and the report
layers are checked at SOURCE level (tkinter never imported); no network, no
production bank, no process.

    python engine/gap_ignore_removal_reference.py

Registration note: run_gates' fail-closed inventory (Row 60) refuses every
battery while this file exists unregistered - Codex's Row 65 checkpoint must
add it to REFERENCE_SUITE_NAMES (R6 asserts that self-registration), exactly
as done for empty_month_absence_reference at 486b847.
"""

from __future__ import annotations

import ast
import inspect
import json
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

import stock_storage as ss            # noqa: E402
import stock_validate as sv           # noqa: E402
import fix_data_pipeline as fdp       # noqa: E402
from check_kit import CheckKit        # noqa: E402

KIT = CheckKit()
check = KIT.check
section = KIT.section

DISPLAY_SOURCE = PROJECT_ROOT / "display_data.py"
TICKER = "VRTX"
CONID = 275850

# The exact feature tokens. Deliberately precise: cross-validation's separate
# ignored_legacy / ignored_stale / ignored_malformed vocabulary and generic
# prose uses of the word "ignored" must SURVIVE the removal untouched.
STORE_ATTRS = ("load_gap_ignores", "ignored_days", "set_ignored_days",
               "_GAP_IGNORES_FILE", "_GAP_IGNORES_LOCK")
RESULT_KEYS = ("ignored", "ignored_count")
AGGREGATE_KEYS = ("ignored_days_total", "ignored_days_run")
UI_TOKENS = ("Include ticked gaps", "_fixdata_include_ignored",
             "set_ignored_days", "ignored_list")
REPORT_TOKENS = ('"ignored"', "'ignored'", "ignored_list", "ignored_count",
                 "ignored_days_total", "ignored_days_run")
REPORT_SOURCES = ("gap_evidence.py", "health.py", "tier0_queries.py",
                  "export_quality.py")
XVAL_SURVIVORS = ("ignored_legacy", "ignored_stale", "ignored_malformed")

FEATURE_PRESENT = any(hasattr(sv, name) for name in STORE_ATTRS)

_TMP = []


def fresh_root():
    tmp = Path(tempfile.mkdtemp(prefix="gap_ignore_removal_"))
    _TMP.append(tmp)
    root = tmp / ss.STORAGE_DIR_NAME
    root.mkdir(parents=True, exist_ok=True)
    return root


def cleanup():
    for tmp in _TMP:
        shutil.rmtree(tmp, ignore_errors=True)


def functions_with_param(module, param):
    out = []
    for name, obj in vars(module).items():
        if callable(obj) and getattr(obj, "__module__", "") == module.__name__:
            try:
                if param in inspect.signature(obj).parameters:
                    out.append(name)
            except (TypeError, ValueError):
                continue
    return sorted(out)


def seeded_scan(root, *, stray_sidecar=None):
    """One seeded series: 3 stored days, 1 fillable gap, 1 verified-absent day.

    Returns scan_series_gaps' result via the injected-reader path. The absent
    day guards the ADJACENT subtraction: if the removal clobbers it, C-checks
    fail loudly rather than the feature disappearing together with its
    neighbour.
    """
    stored = ["2014-05-01", "2014-05-02", "2014-05-07"]
    absent_day, gap_day = "2014-05-05", "2014-05-06"
    manifest = ss.new_manifest(TICKER, TICKER)
    manifest["conid"] = CONID
    sec = manifest.setdefault("intervals", {}).setdefault("1d", {})
    sec.setdefault("months", {})
    sec["verified_absent"] = [absent_day]
    sec["verified_absent_evidence"] = {
        absent_day: {"method": "day_in_served_month", "control": "2014-05",
                     "at": datetime.now().isoformat(timespec="seconds")}}
    ss.save_manifest(Path(root) / TICKER, manifest)
    if stray_sidecar is not None:
        (Path(root) / "_gap_ignores.json").write_text(
            json.dumps(stray_sidecar), encoding="utf-8")

    def read_fn(_root, _ticker, _interval):
        return [(datetime.fromisoformat(f"{d}T00:00:00"),
                 10.0, 10.5, 9.5, 10.25, 1000) for d in sorted(stored)]

    from datetime import date as _date
    calendar = [_date.fromisoformat(d)
                for d in stored + [absent_day, gap_day]]
    res = sv.scan_series_gaps(root, TICKER, "1d", read_fn=read_fn,
                              calendar_days=calendar)
    return res, gap_day, absent_day


def run():
    if FEATURE_PRESENT:
        section("BASELINE - ticked-gap plumbing still present (removal pending)")
        check("B1 store functions exist today (the state Row 65 removes)",
              all(hasattr(sv, n) for n in
                  ("load_gap_ignores", "ignored_days", "set_ignored_days")))
        check("B2 include_ignored is threaded today",
              "include_ignored"
              in inspect.signature(sv.scan_series_gaps).parameters)
        res, gap_day, absent_day = seeded_scan(fresh_root())
        check("B3 scanner emits the ignored bucket today",
              "ignored" in res and "ignored_count" in res, str(sorted(res)))
        check("B3b adjacent source-absent split works today (must survive)",
              absent_day in (res.get("source_absent") or [])
              and gap_day in (res.get("missing_days") or []),
              f"absent={res.get('source_absent')} missing={res.get('missing_days')}")
        return

    # ---------------- post-removal acceptance ----------------
    section("R1. the store is gone")
    leftovers = [n for n in STORE_ATTRS if hasattr(sv, n)]
    check("R1 stock_validate carries no ignore-store attribute",
          not leftovers, f"left: {leftovers}")

    section("R2. no signature still threads include_ignored")
    for mod in (sv, fdp):
        holders = functions_with_param(mod, "include_ignored")
        check(f"R2 {mod.__name__} has no include_ignored parameter anywhere",
              not holders, f"still on: {holders}")

    section("R3. scanner behavior - clean keys, inert stray sidecar, "
            "adjacent logic intact")
    res, gap_day, absent_day = seeded_scan(fresh_root())
    check("R3 result carries no ignored keys",
          not any(k in res for k in RESULT_KEYS), str(sorted(res)))
    check("R3b the fillable gap is still reported",
          res.get("missing_days") == [gap_day],
          f"missing_days={res.get('missing_days')}")
    check("R3c the ADJACENT source-absent split survived the removal",
          res.get("source_absent") == [absent_day]
          and absent_day not in (res.get("missing_days") or []),
          f"source_absent={res.get('source_absent')}")
    stray = {TICKER: {"1d": [gap_day]}}
    res2, gap_day2, _ = seeded_scan(fresh_root(), stray_sidecar=stray)
    check("R3d a leftover _gap_ignores.json on disk is INERT",
          res2.get("missing_days") == [gap_day2]
          and not any(k in res2 for k in RESULT_KEYS),
          f"missing_days={res2.get('missing_days')}")

    section("R4. UI source - tickbox and Gap Map ticks gone, Deep scan kept")
    src = DISPLAY_SOURCE.read_text(encoding="utf-8")
    ast.parse(src)
    gone = [t for t in UI_TOKENS if t in src]
    check("R4 display_data has no ticked-gap token",
          not gone, f"still present: {gone}")
    check("R4b the Row 63/64 Deep scan tickbox is untouched",
          "Deep scan" in src and "_fixdata_deep_scan" in src)

    section("R5. report layers dropped the bucket; xval vocabulary survives")
    for name in REPORT_SOURCES:
        text = (ENGINE_ROOT / name).read_text(encoding="utf-8")
        hits = [t for t in REPORT_TOKENS if t in text]
        check(f"R5 {name} carries no ignore-bucket token",
              not hits, f"still present: {hits}")
    xq = (ENGINE_ROOT / "export_quality.py").read_text(encoding="utf-8")
    check("R5b cross-validation ignored_* categories were NOT touched",
          all(t in xq for t in XVAL_SURVIVORS),
          "over-deletion: xval vocabulary went missing")

    section("R6. inventory self-registration (Row 60 fail-closed guard)")
    rg = (ENGINE_ROOT / "run_gates.py").read_text(encoding="utf-8")
    check("R6 this harness is registered in the static reference inventory",
          '"gap_ignore_removal_reference"' in rg
          or "'gap_ignore_removal_reference'" in rg,
          "unregistered: every battery fails closed with inventory drift")

    section("R7. aggregate report keys are gone")
    text = (ENGINE_ROOT / "stock_validate.py").read_text(encoding="utf-8")
    hits = [t for t in AGGREGATE_KEYS if t in text]
    check("R7 stock_validate emits no ignored aggregate", not hits,
          f"still present: {hits}")


def main():
    try:
        run()
        if FEATURE_PRESENT:
            KIT.pending(
                "M1",
                "Ticked-gap plumbing still present: this is the pre-removal "
                "baseline.",
                "M1 removes: the _gap_ignores.json store trio + lock, the "
                "include_ignored threading (stock_validate x4, "
                "fix_data_pipeline x2), the ignored/ignored_count result "
                "keys and both aggregates, the Fix Data tickbox, the Gap Map "
                "tick column/save, and the gap_evidence/health/tier0/"
                "export_quality bucket lines.",
                "Cross-validation's ignored_legacy/stale/malformed and the "
                "Deep scan tickbox MUST survive untouched.")
        return KIT.finish(feature_absent=FEATURE_PRESENT)
    finally:
        cleanup()


if __name__ == "__main__":
    sys.exit(main())
