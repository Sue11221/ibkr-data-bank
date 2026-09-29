"""Acceptance harness: health report moves out of Fix Data, onto export (Row 66).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (STORAGE_OPTIMIZATION_LOG.md, Row 66).

User direction (2026-07-27): "I don't need it to generate a health report for
repair data. I just need it for export." Facts that anchor the contract:
display_data's Fix Data wiring is the ONLY production writer of
_health_report.json; export_quality consumes it through the Tier 0 cache WITH
a currency check; and the repair run writes it BEFORE the fills, so the report
is already non-current for export the moment any fill lands. Relocation, not
deletion: repair stops generating it, export regenerates it on demand when the
cache is non-current. Bonus (the original complaint): the remaining silent
planning window gets three [plan] log lines.

Change polarity: while fix_data_pipeline.run still REQUIRES health_fn the
feature is absent and this harness exits 3 (pending); after Row 66 lands every
check passes and it exits 0. Exit 1 = a check failed.

Offline and headless: temp-dir fixtures; injected fakes; operation_gate is
patched IN-PROCESS so the harness can never touch the real fetch lease;
display_data is checked at SOURCE level (tkinter never imported); no network,
no production bank, no process.

    python engine/fixdata_health_relocation_reference.py

Placement note: authored in the Claude scratchpad while the Row 65
protected-exclusive freeze is active (an unregistered engine/*_reference.py
would trip the Row 60 fail-closed inventory mid-battery); it moves to engine/
at freeze lift, and Codex's Row 66 checkpoint registers it in
REFERENCE_SUITE_NAMES (A7 asserts that self-registration).
"""

from __future__ import annotations

import ast
import contextlib
import inspect
import sys
import tempfile
import threading
from pathlib import Path

_CANDIDATES = (Path(__file__).resolve().parent,
               Path.cwd() / "engine",
               Path.cwd())
for _cand in _CANDIDATES:
    if (_cand / "fix_data_pipeline.py").exists():
        ENGINE_ROOT = _cand
        break
else:  # pragma: no cover - misplacement is a setup error, not a finding
    sys.stderr.write("cannot locate engine/ (fix_data_pipeline.py)\n")
    sys.exit(2)
PROJECT_ROOT = ENGINE_ROOT.parent
if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

try:                                  # harness hygiene (Row 61)
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

if __name__ == "__main__":
    from fetch_a2_ibkr_workflows_selftest import Operations
    raise SystemExit(Operations.run_fixdata_reference_cli("fixdata_health_relocation_reference"))


import fix_data_pipeline as fdp       # noqa: E402
import export_quality as xq           # noqa: E402
from check_kit import CheckKit        # noqa: E402

KIT = CheckKit()
check = KIT.check
section = KIT.section

DISPLAY_SOURCE = PROJECT_ROOT / "display_data.py"
HEALTH_SIDECAR = "_health_report.json"
PLAN_STEPS = ("accuracy series", "value candidates", "spot probes")

_sig = inspect.signature(fdp.run).parameters
HEALTH_OPTIONAL = ("health_fn" in _sig
                   and _sig["health_fn"].default is None)


def run_pipeline(health_fn="omit"):
    """Drive fdp.run with inert fakes; return (result, events, bank Path)."""
    events = []
    lock = threading.Lock()

    def record(event):
        with lock:
            events.append(dict(event))

    kwargs = {}
    if health_fn != "omit":
        kwargs["health_fn"] = health_fn

    real_acquire = fdp.operation_gate.acquire
    fdp.operation_gate.acquire = (
        lambda mode, **kw: contextlib.nullcontext())
    try:
        with tempfile.TemporaryDirectory(prefix="health_reloc_") as tmp:
            res = fdp.run(
                root=tmp, ports=(7497,),
                adapter_factory=lambda port: None,
                scan_fn=lambda root, **kw: {
                    "summary": {}, "series_scanned": 0,
                    "verification_series": []},
                fill_fn=lambda *a, **kw: {},
                audit_fn=lambda *a, **kw: {},
                series_fn=lambda root: [],
                refresh_fn=lambda root, tickers, **kw: {},
                progress=record,
                sleep_fn=lambda s: None,
                **kwargs,
            )
            sidecar_exists = (Path(tmp) / HEALTH_SIDECAR).exists()
    finally:
        fdp.operation_gate.acquire = real_acquire
    return res, events, sidecar_exists


def logs(events):
    return [str(e.get("message") or "")
            for e in events if e.get("type") == "log"]


def run():
    if not HEALTH_OPTIONAL:
        section("BASELINE - repair still generates the health report")
        check("B1 health_fn is a required parameter today",
              _sig["health_fn"].default is inspect.Parameter.empty)
        calls = []
        res, events, _ = run_pipeline(
            health_fn=lambda root: calls.append(root) or {})
        check("B2 pipeline completes under inert fakes (fixture is valid)",
              any(e.get("type") == "done" for e in events)
              and res.get("error") is None,
              f"error={res.get('error')}")
        check("B3 the pipeline invokes health today (what Row 66 removes)",
              len(calls) == 1, f"calls={len(calls)}")
        src = DISPLAY_SOURCE.read_text(encoding="utf-8")
        check("B4 production wiring generates health mid-repair today",
              "def _health(" in src and "health_fn=_health" in src)
        return

    # ---------------- post-implementation acceptance ----------------
    section("A1. repair pipeline: health is skipped by default")
    res, events, sidecar = run_pipeline()          # health_fn omitted -> None
    check("A1 run completes cleanly with no health_fn",
          res.get("error") is None
          and any(e.get("type") == "done" for e in events),
          f"error={res.get('error')}")
    check("A1b no health sidecar is written by a repair run", not sidecar)
    check("A1c health result fields stay at their defaults",
          res.get("health_report") is None and res.get("health_queue") == []
          and res.get("coverage_report") is None,
          f"health_report={type(res.get('health_report'))}")

    section("A2. the injected test seam survives (selftests keep working)")
    calls = []
    res2, events2, _ = run_pipeline(
        health_fn=lambda root: calls.append(root) or {"health_queue": ["q"]})
    check("A2 an explicitly injected health_fn is still honored",
          len(calls) == 1 and res2.get("error") is None,
          f"calls={len(calls)} error={res2.get('error')}")
    check("A2b its fields still reach the result",
          res2.get("health_queue") == ["q"])

    section("A3. production wiring: generation left the repair run")
    src = DISPLAY_SOURCE.read_text(encoding="utf-8")
    ast.parse(src)
    check("A3 the mid-repair _health generator is gone",
          "def _health(" not in src)
    check("A3b display_data no longer calls health_report.audit directly",
          "health_report.audit(" not in src,
          "generation must go through export_quality.ensure_current_health")

    section("A4. export regenerates on demand (the user's actual need)")
    check("A4 export_quality.ensure_current_health exists",
          callable(getattr(xq, "ensure_current_health", None)))
    ens_sig = inspect.signature(xq.ensure_current_health).parameters
    check("A4b injectable seams: audit_fn and current_fn",
          "audit_fn" in ens_sig and "current_fn" in ens_sig)
    with tempfile.TemporaryDirectory(prefix="health_reloc_") as tmp:
        audits = []
        fake_report = {"kind": "health_report", "marker": "fresh"}
        got = xq.ensure_current_health(
            tmp, audit_fn=lambda root: audits.append(root) or fake_report,
            current_fn=lambda root: False)
        check("A4c non-current cache -> regenerates exactly once",
              len(audits) == 1 and got.get("marker") == "fresh",
              f"audits={len(audits)} got={got!r:.80}")
        audits2 = []
        got2 = xq.ensure_current_health(
            tmp, audit_fn=lambda root: audits2.append(root) or fake_report,
            current_fn=lambda root: True)
        check("A4d current cache -> no regeneration",
              not audits2, f"audits={len(audits2)}")
        check("A4e current branch still returns a usable report or None",
              got2 is None or isinstance(got2, dict))
    check("A4f the export flow is wired to it",
          "ensure_current_health" in src,
          "display_data never invokes the regeneration path")

    section("A5. the remaining silent window shows [plan] life signs")
    plan_lines = [m for m in logs(events) if m.startswith("[plan]")]
    check("A5 three [plan] log lines during planning",
          len(plan_lines) == 3, str(plan_lines))
    check("A5b they name the three steps",
          all(any(step in line for line in plan_lines)
              for step in PLAN_STEPS), str(plan_lines))
    s_scan = next((e.get("seq") for e in events if e.get("type") == "log"
                   and str(e.get("message", "")).startswith("[scan]")), None)
    s_plans = [e.get("seq") for e in events if e.get("type") == "log"
               and str(e.get("message", "")).startswith("[plan]")]
    s_done = next((e.get("seq") for e in events
                   if e.get("type") == "done"), None)
    check("A5c ordered after the scan summary, before done",
          None not in (s_scan, s_done) and len(s_plans) == 3
          and s_scan < min(s_plans) and max(s_plans) < s_done,
          f"scan={s_scan} plans={s_plans} done={s_done}")

    section("A6. selftest fleet: no orphaned required-health_fn callers")
    # 30+ selftest sites inject health_fn explicitly; the seam stays, so no
    # churn is required - this guards against Codex instead RENAMING it.
    check("A6 the parameter is still named health_fn",
          "health_fn" in inspect.signature(fdp.run).parameters)

    section("A7. inventory self-registration (Row 60 fail-closed guard)")
    rg = (ENGINE_ROOT / "run_gates.py").read_text(encoding="utf-8")
    check("A7 this harness is registered in the static reference inventory",
          '"fixdata_health_relocation_reference"' in rg
          or "'fixdata_health_relocation_reference'" in rg,
          "unregistered: every battery fails closed with inventory drift")


def main():
    run()
    if not HEALTH_OPTIONAL:
        KIT.pending(
            "M1",
            "fix_data_pipeline still requires health_fn: this is the "
            "pre-change baseline.",
            "M1 makes health_fn optional (default None = skip; injected fn "
            "still honored so the 30+ selftest sites keep working), drops "
            "the _health generator from display_data's Fix Data wiring, and "
            "adds the three [plan] log lines to the planning window.",
            "M2 adds export_quality.ensure_current_health(root, *, "
            "audit_fn=None, current_fn=None): regenerate (default "
            "health.audit write=True) only when the cache is non-current "
            "for the bank state; wire the export flow to call it.",
            "Health result keys stay in the result dict at defaults; no "
            "other event or result contract changes.")
    return KIT.finish(feature_absent=not HEALTH_OPTIONAL)


if __name__ == "__main__":
    sys.exit(main())
