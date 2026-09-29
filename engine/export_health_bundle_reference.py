"""Acceptance harness: exports bundle the current health report (Row 69).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (STORAGE_OPTIMIZATION_LOG.md, Row 69).

User order (2026-07-28): "I only want it when I export it within the folder.
To make sure the consumer does know this file exist, have 1 folder inside
another folder while the health report sits beside it if more than [one]
ticker, and if only 1 ticker, just have 1 folder where health report and the
data lives."

Layout contract this harness pins (report filename literal
``health_report.json``; inner data folder literal ``Data``):
- ticker_count >= 2:  <parent>/Export <stamp> - N tickers - <iv> - <fmt>/
                          health_report.json      <- beside the inner folder
                          Data/<exported files>
- ticker_count == 1:  <parent>/Export <stamp> - 1 ticker - <iv> - <fmt>/
                          health_report.json + <exported files>  (one flat folder)
The report written is the CURRENT one obtained through Row 66's
export_quality.ensure_current_health seam (regenerate-only-when-stale); the
bank-root sidecar cache remains engine plumbing and is not removed.

Change polarity: while export_batch_folder still refuses single-ticker
folders and has no bundle layout the feature is absent and this harness exits
3 (pending); after Row 69 lands every check passes and it exits 0.

Offline and headless: temp dirs only; no bank, no network, no tkinter.

    python engine/export_health_bundle_reference.py

Placement note: authored in the Claude scratchpad; lands in engine/ at Row
69's own promotion (one unregistered harness at a time - Row 60 drift guard);
Codex's Row 69 checkpoint registers it in REFERENCE_SUITE_NAMES (A6).
"""

from __future__ import annotations

import inspect
import sys
import tempfile
from pathlib import Path

_CANDIDATES = (Path(__file__).resolve().parent,
               Path.cwd() / "engine",
               Path.cwd())
for _cand in _CANDIDATES:
    if (_cand / "export_batch_folder.py").exists():
        ENGINE_ROOT = _cand
        break
else:  # pragma: no cover - misplacement is a setup error, not a finding
    sys.stderr.write("cannot locate engine/ (export_batch_folder.py)\n")
    sys.exit(2)
PROJECT_ROOT = ENGINE_ROOT.parent
if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

try:                                  # harness hygiene (Row 61)
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

import datetime as dt                 # noqa: E402

import export_batch_folder as ebf     # noqa: E402
from check_kit import CheckKit        # noqa: E402

KIT = CheckKit()
check = KIT.check
section = KIT.section

DISPLAY_SOURCE = PROJECT_ROOT / "display_data.py"
NOW = dt.datetime(2026, 7, 28, 12, 0, 0).astimezone()
REPORT_NAME = "health_report.json"
DATA_DIR_NAME = "Data"

HAS_BUNDLE = callable(getattr(ebf, "bundle_paths", None))


def run():
    if not HAS_BUNDLE:
        section("BASELINE - exports carry no health report today")
        raised = False
        try:
            ebf.folder_name(1, "1d", "csv", now=NOW)
        except ebf.BatchFolderError:
            raised = True
        check("B1 single-ticker folders are refused today", raised)
        check("B2 no bundle layout exists yet",
              not hasattr(ebf, "bundle_paths"))
        src = (ENGINE_ROOT / "export_batch_folder.py").read_text(
            encoding="utf-8")
        check("B3 the module knows nothing of health reports today",
              REPORT_NAME not in src)
        return

    # ---------------- post-implementation acceptance ----------------
    section("A1. single-ticker exports get a folder, singular name")
    name1 = ebf.folder_name(1, "1d", "csv", now=NOW)
    check("A1 folder_name(1) works and reads '1 ticker'",
          "1 ticker " in name1 + " " and "tickers" not in name1, name1)
    namen = ebf.folder_name(3, "1d", "csv", now=NOW)
    check("A1b multi form unchanged ('3 tickers')", "3 tickers" in namen,
          namen)

    section("A2. layout contract - multi: Data/ inside, report beside")
    sig = inspect.signature(ebf.bundle_paths).parameters
    check("A2 bundle_paths(batch_dir, ticker_count) signature",
          list(sig)[:2] == ["batch_dir", "ticker_count"], str(list(sig)))
    with tempfile.TemporaryDirectory(prefix="bundle_") as tmp:
        batch = Path(tmp) / "Export test - 3 tickers - 1d - csv"
        batch.mkdir()
        data_dir, report_path = ebf.bundle_paths(batch, 3)
        check("A2b multi: data dir is <batch>/Data",
              Path(data_dir) == batch / DATA_DIR_NAME, str(data_dir))
        check("A2c multi: report sits beside the Data folder",
              Path(report_path) == batch / REPORT_NAME, str(report_path))

        section("A3. layout contract - single: one flat folder")
        single = Path(tmp) / "Export test - 1 ticker - 1d - csv"
        single.mkdir()
        data_dir1, report_path1 = ebf.bundle_paths(single, 1)
        check("A3 single: data lives in the batch folder itself",
              Path(data_dir1) == single, str(data_dir1))
        check("A3b single: report lives right beside the data",
              Path(report_path1) == single / REPORT_NAME, str(report_path1))

        section("A4. existing batch-folder creation is untouched")
        made = ebf.create_batch_folder(tmp, 3, "1d", "csv", now=NOW)
        check("A4 create_batch_folder still creates the stamped folder",
              Path(made).is_dir() and "3 tickers" in Path(made).name,
              str(made))

    section("A5. the export flow writes the CURRENT report")
    src = DISPLAY_SOURCE.read_text(encoding="utf-8")
    check("A5 export flow copies via the Row 66 currency seam",
          "ensure_current_health" in src and REPORT_NAME in src,
          "export must obtain the report through ensure_current_health")
    ebf_src = (ENGINE_ROOT / "export_batch_folder.py").read_text(
        encoding="utf-8")
    check("A5b the layout module pins the literal names",
          REPORT_NAME in ebf_src and DATA_DIR_NAME in ebf_src)

    section("A6. inventory self-registration (Row 60 fail-closed guard)")
    rg = (ENGINE_ROOT / "run_gates.py").read_text(encoding="utf-8")
    check("A6 this harness is registered in the static reference inventory",
          '"export_health_bundle_reference"' in rg
          or "'export_health_bundle_reference'" in rg,
          "unregistered: every battery fails closed with inventory drift")


def main():
    run()
    if not HAS_BUNDLE:
        KIT.pending(
            "M1",
            "export_batch_folder has no bundle layout yet: this is the "
            "pre-change baseline.",
            "M1 adds: folder_name accepts ticker_count=1 (singular '1 "
            "ticker'); bundle_paths(batch_dir, ticker_count) -> (data_dir, "
            "report_path) with Data/ + beside-report for >=2 and one flat "
            "folder for ==1; every export flow creates the folder, writes "
            "data into data_dir, and copies the ensure_current_health "
            "report to report_path as health_report.json.",
            "Rendered data bytes stay byte-exact; the bank-root sidecar "
            "cache stays; single-file naming inside the folder unchanged.")
    return KIT.finish(feature_absent=not HAS_BUNDLE)


if __name__ == "__main__":
    sys.exit(main())
