"""Acceptance harness: Run Logs keep themselves at a suitable size (Row 71).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (STORAGE_OPTIMIZATION_LOG.md, Row 71).

User order (2026-07-28): "for the run log, I want to automatically keep it in
a suitable size when the program runs itself."

Retention contract this harness pins (engine/run_log_retention.py):
- Knobs are module literals: DEFAULT_MAX_TOTAL_BYTES (100 MB),
  DEFAULT_KEEP_PER_FAMILY (8), DEFAULT_MIN_AGE_DAYS (14), FAMILY_PATTERNS.
- ``plan_prune(root, ...)`` is PURE (deletes nothing);
  ``prune_run_logs(root, ...)`` executes and returns a dict with at least
  ``deleted`` (paths), ``freed_bytes``, ``errors``.
- Deletion is ALLOWLIST-driven and fail-closed: only files matching
  FAMILY_PATTERNS are ever candidates; unknown files survive any pressure.
  The patterns must cover the real artifact families - this harness uses
  production filename shapes (fix-data-vol-value-reconcile-*.json,
  fix-data-spot-probe-*.json).
- Floors dominate the cap: the newest KEEP_PER_FAMILY files of each family
  and anything younger than MIN_AGE_DAYS are never deleted, even over cap.
- Over cap: oldest eligible first, stopping as soon as the total is under
  the cap.
- ``prune_run_logs`` never raises; per-file failures land in ``errors``.
- Every prune appends to a BOUNDED audit sidecar
  (``Run Logs/_retention_audit.json``, <= 50 entries) which is itself never
  a deletion candidate.
- Production wiring exists: a run-completion seam calls ``prune_run_logs``
  (exception-guarded in the caller so retention can never fail a run).

Change polarity: while ``engine/run_log_retention.py`` does not exist the
feature is absent and this harness exits 3 (pending); after Row 71 lands
every check passes and it exits 0. Exit 1 = a check failed.

Offline and headless: temp dirs only; injected ``now``; no bank, no network,
no tkinter, no real "Run Logs" surface.

    python engine/run_log_retention_reference.py

Placement note: authored in the Claude scratchpad; lands in engine/ at Row
71's own promotion (one unregistered harness at a time - Row 60 drift
guard); Codex's Row 71 checkpoint registers it in REFERENCE_SUITE_NAMES.
"""

from __future__ import annotations

import datetime as dt
import inspect
import json
import os
import stat
import sys
import tempfile
from pathlib import Path

_CANDIDATES = (Path(__file__).resolve().parent,
               Path.cwd() / "engine",
               Path.cwd())
for _cand in _CANDIDATES:
    if (_cand / "run_gates.py").exists():
        ENGINE_ROOT = _cand
        break
else:  # pragma: no cover - misplacement is a setup error, not a finding
    sys.stderr.write("cannot locate engine/ (run_gates.py)\n")
    sys.exit(2)
PROJECT_ROOT = ENGINE_ROOT.parent
if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

try:                                  # harness hygiene (Row 61)
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

from check_kit import CheckKit        # noqa: E402

KIT = CheckKit()
check = KIT.check
section = KIT.section

RUN_LOGS = "Run Logs"
AUDIT_NAME = "_retention_audit.json"
NOW = dt.datetime(2026, 7, 28, 12, 0, 0).astimezone()
DAY = 86400.0

HAS_FEATURE = (ENGINE_ROOT / "run_log_retention.py").exists()

RECON = "fix-data-vol-value-reconcile-fixdata-17851823250557{:03d}.json"
PROBE = "fix-data-spot-probe-fixdata-17851823250557{:03d}.json"
UNKNOWN = "mystery-external-tool.log"
FILE_BYTES = 10_000


def build_root(tmp, *, ages_days):
    """Create Run Logs/ with 12 reconcile + 12 probe files plus one unknown.

    ``ages_days`` maps index 1..12 to the age (days before NOW) applied to
    BOTH families at that index; the unknown file is always 200 days old.
    Index 1 is the oldest end of each family by construction.
    """
    root = Path(tmp)
    rl = root / RUN_LOGS
    rl.mkdir()
    for i in range(1, 13):
        for pattern in (RECON, PROBE):
            p = rl / pattern.format(i)
            p.write_bytes(b"x" * FILE_BYTES)
            ts = (NOW - dt.timedelta(days=ages_days[i])).timestamp()
            os.utime(p, (ts, ts))
    u = rl / UNKNOWN
    u.write_bytes(b"y" * 50_000)
    ts = (NOW - dt.timedelta(days=200)).timestamp()
    os.utime(u, (ts, ts))
    return root, rl


def names(rl):
    return sorted(p.name for p in rl.iterdir())


def run():
    if not HAS_FEATURE:
        section("BASELINE - nothing bounds Run Logs today")
        check("B1 engine/run_log_retention.py does not exist yet",
              not (ENGINE_ROOT / "run_log_retention.py").exists())
        prod_refs = []
        for rel in ("fix_data_pipeline.py", "stock_validate.py",
                    "stock_ibkr.py"):
            src = (ENGINE_ROOT / rel).read_text(encoding="utf-8",
                                                errors="replace")
            if "prune_run_logs" in src:
                prod_refs.append(rel)
        dd = (PROJECT_ROOT / "display_data.py").read_text(
            encoding="utf-8", errors="replace")
        if "prune_run_logs" in dd:
            prod_refs.append("display_data.py")
        check("B2 no production path prunes Run Logs today (unbounded growth)",
              not prod_refs, str(prod_refs))
        return

    # ---------------- post-implementation acceptance ----------------
    import run_log_retention as rlr   # noqa: E402  (feature import)

    section("A1. knobs are pinned module literals")
    check("A1 default cap is 100 MB",
          getattr(rlr, "DEFAULT_MAX_TOTAL_BYTES", None) == 100 * 1024 * 1024,
          str(getattr(rlr, "DEFAULT_MAX_TOTAL_BYTES", None)))
    check("A1b default per-family floor is 8",
          getattr(rlr, "DEFAULT_KEEP_PER_FAMILY", None) == 8)
    check("A1c default minimum age is 14 days",
          getattr(rlr, "DEFAULT_MIN_AGE_DAYS", None) == 14)
    check("A1d an explicit family allowlist exists",
          bool(getattr(rlr, "FAMILY_PATTERNS", None)))
    params = set(inspect.signature(rlr.prune_run_logs).parameters)
    check("A1e prune_run_logs exposes the override seams",
          {"now", "max_total_bytes", "keep_per_family",
           "min_age_days"} <= params, str(sorted(params)))

    section("A2. allowlist is fail-closed; floors dominate the cap")
    ages = {i: 100 - i for i in range(1, 13)}      # 99d .. 88d, all old
    with tempfile.TemporaryDirectory(prefix="rlr_") as tmp:
        root, rl = build_root(tmp, ages_days=ages)
        plan = rlr.plan_prune(root, now=NOW, max_total_bytes=1)
        check("A2 plan_prune is pure (no file removed by planning)",
              len(names(rl)) == 25, str(len(names(rl))))
        res = rlr.prune_run_logs(root, now=NOW, max_total_bytes=1)
        left = names(rl)
        check("A2b the unknown file survives absurd cap pressure",
              UNKNOWN in left)
        check("A2c newest 8 of each family survive (floor beats cap)",
              all(RECON.format(i) in left and PROBE.format(i) in left
                  for i in range(5, 13)), str(left))
        check("A2d exactly the 8 over-floor files were deleted",
              sorted(Path(p).name for p in res.get("deleted", []))
              == sorted([RECON.format(i) for i in range(1, 5)]
                        + [PROBE.format(i) for i in range(1, 5)]),
              str(res.get("deleted")))
        check("A2e freed_bytes matches the deletions",
              res.get("freed_bytes") == 8 * FILE_BYTES,
              str(res.get("freed_bytes")))

        section("A6. bounded audit trail")
        audit_path = rl / AUDIT_NAME
        check("A6 a prune appends a valid audit entry",
              audit_path.exists(), str(audit_path))
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
        entries = audit if isinstance(audit, list) else audit.get("entries")
        check("A6b audit entries are a bounded list (<= 50)",
              isinstance(entries, list) and 1 <= len(entries) <= 50,
              str(type(entries)))
        res2 = rlr.prune_run_logs(root, now=NOW, max_total_bytes=1)
        check("A6c the audit sidecar is never a deletion candidate",
              audit_path.exists() and not any(
                  Path(p).name == AUDIT_NAME
                  for p in res2.get("deleted", [])))

    section("A3. over cap deletes oldest-first and stops under the cap")
    ages3 = {i: 100 - 2 * i for i in range(1, 13)}  # distinct, interleaved
    with tempfile.TemporaryDirectory(prefix="rlr_") as tmp:
        root, rl = build_root(tmp, ages_days=ages3)
        # 25 files: 24 known x 10k + 50k unknown = 290k. Cap forces freeing
        # >= 25k; oldest eligible are recon#1..4 / probe#1..4 (same ages ->
        # deterministic need: 3 deletions reach 30k freed >= 25k).
        cap = 290_000 - 25_000
        res = rlr.prune_run_logs(root, now=NOW, max_total_bytes=cap)
        gone = sorted(Path(p).name for p in res.get("deleted", []))
        check("A3 exactly three files freed the shortfall",
              len(gone) == 3, str(gone))
        oldest_pool = {RECON.format(1), PROBE.format(1),
                       RECON.format(2), PROBE.format(2)}
        check("A3b the deletions came from the oldest eligible files",
              set(gone) <= oldest_pool, str(gone))
        check("A3c the newest eligible files were untouched",
              RECON.format(4) in names(rl) and PROBE.format(4) in names(rl))

    section("A5. minimum age protects even over cap")
    ages5 = {i: 2 for i in range(1, 13)}            # everything 2 days old
    with tempfile.TemporaryDirectory(prefix="rlr_") as tmp:
        root, rl = build_root(tmp, ages_days=ages5)
        res = rlr.prune_run_logs(root, now=NOW, max_total_bytes=1)
        check("A5 nothing younger than the floor age is deleted",
              res.get("deleted") in ([],) and len(names(rl)) >= 25,
              str(res.get("deleted")))

    section("A7. under cap is a clean no-op")
    with tempfile.TemporaryDirectory(prefix="rlr_") as tmp:
        root, rl = build_root(tmp, ages_days={i: 100 for i in range(1, 13)})
        res = rlr.prune_run_logs(root, now=NOW,
                                 max_total_bytes=10 * 1024 * 1024)
        check("A7 under the cap nothing is deleted",
              res.get("deleted") == [] and len(names(rl)) == 25,
              str(res.get("deleted")))

    section("A8. prune never raises; per-file failure is recorded")
    with tempfile.TemporaryDirectory(prefix="rlr_",
                                     ignore_cleanup_errors=True) as tmp:
        root, rl = build_root(tmp, ages_days={i: 100 for i in range(1, 13)})
        victim = rl / RECON.format(1)
        os.chmod(victim, stat.S_IREAD)
        try:
            res = rlr.prune_run_logs(root, now=NOW, max_total_bytes=1)
            raised = False
        except BaseException:         # noqa: BLE001 - the contract under test
            raised = True
            res = {}
        check("A8 a locked file cannot crash the prune",
              not raised)
        check("A8b the failure is visible (deleted anyway or in errors)",
              (not victim.exists()) or bool(res.get("errors")),
              f"exists={victim.exists()} errors={res.get('errors')}")
        for p in rl.iterdir():        # let TemporaryDirectory clean up
            try:
                os.chmod(p, stat.S_IWRITE | stat.S_IREAD)
            except OSError:
                pass

    section("A9. production wiring + inventory registration")
    wired = []
    for rel in ("fix_data_pipeline.py", "stock_validate.py",
                "stock_ibkr.py"):
        if "prune_run_logs" in (ENGINE_ROOT / rel).read_text(
                encoding="utf-8", errors="replace"):
            wired.append(rel)
    if "prune_run_logs" in (PROJECT_ROOT / "display_data.py").read_text(
            encoding="utf-8", errors="replace"):
        wired.append("display_data.py")
    check("A9 a production run-completion seam calls prune_run_logs",
          bool(wired), "no production caller found")
    rg = (ENGINE_ROOT / "run_gates.py").read_text(encoding="utf-8")
    check("A9b this harness is registered in the static reference inventory",
          '"run_log_retention_reference"' in rg
          or "'run_log_retention_reference'" in rg,
          "unregistered: every battery fails closed with inventory drift")


def main():
    run()
    if not HAS_FEATURE:
        KIT.pending(
            "M1",
            "engine/run_log_retention.py does not exist: Run Logs grow "
            "without bound today - this is the pre-change baseline.",
            "M1 adds the retention module (allowlist FAMILY_PATTERNS, "
            "floors keep newest 8/family and <14 days, oldest-first until "
            "under the 100 MB default cap, never raises, bounded "
            "_retention_audit.json) plus one exception-guarded "
            "run-completion call site.",
            "Unknown files are never deleted; the audit sidecar is never a "
            "candidate; retention failure can never fail a run.")
    return KIT.finish(feature_absent=not HAS_FEATURE)


if __name__ == "__main__":
    sys.exit(main())
