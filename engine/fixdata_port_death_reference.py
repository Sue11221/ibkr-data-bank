"""Acceptance harness: repair port workers die LOUDLY, in fetch vocabulary (Row 67).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (STORAGE_OPTIMIZATION_LOG.md, Row 67).

User report (2026-07-27 live run): six of eight port workers died at stage-2
spinup and the dialog showed them as "done" - indistinguishable from finished
- while the deaths were recorded only in port_errors, which nothing prints
until the run ends. The fetch panel's convention is explicit:
"port N: DEAD - <reason>" (display_data.py:9307). The repair dialog renders
the engine's port-state strings verbatim, so parity is an ENGINE vocabulary
change only - display_data is untouched by this row.

Change polarity: while a worker whose adapter connect raises still ends with
port state "done" the feature is absent and this harness exits 3 (pending);
after Row 67 lands every check passes and it exits 0. Exit 1 = a check failed.

Offline and headless: injected fakes; operation_gate patched IN-PROCESS (the
harness can never touch the real fetch lease); no network, no bank, no
process, no tkinter.

    python engine/fixdata_port_death_reference.py

Placement note: authored in the Claude scratchpad while the Row 65
protected-exclusive freeze is active; moves to engine/ at freeze lift, and
Codex's Row 67 checkpoint registers it in REFERENCE_SUITE_NAMES (A6 asserts
that self-registration).
"""

from __future__ import annotations

import contextlib
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
if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

try:                                  # harness hygiene (Row 61)
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

if __name__ == "__main__":
    from fetch_a2_ibkr_workflows_selftest import Operations
    raise SystemExit(Operations.run_fixdata_reference_cli("fixdata_port_death_reference"))


import fix_data_pipeline as fdp       # noqa: E402
from check_kit import CheckKit        # noqa: E402

KIT = CheckKit()
check = KIT.check
section = KIT.section

GOOD_PORT, DEAD_PORT = 7501, 7502
BOOM = "RuntimeError: boom"


def run_pipeline():
    """Two ports: one healthy (idle run), one whose adapter connect raises."""
    events = []
    lock = threading.Lock()

    def record(event):
        with lock:
            events.append(dict(event))

    def adapter_factory(port):
        if port == DEAD_PORT:
            raise RuntimeError("boom")
        return None

    real_acquire = fdp.operation_gate.acquire
    fdp.operation_gate.acquire = (
        lambda mode, **kw: contextlib.nullcontext())
    try:
        with tempfile.TemporaryDirectory(prefix="port_death_") as tmp:
            res = fdp.run(
                root=tmp, ports=(GOOD_PORT, DEAD_PORT),
                adapter_factory=adapter_factory,
                scan_fn=lambda root, **kw: {
                    "summary": {}, "series_scanned": 0,
                    "verification_series": []},
                health_fn=lambda root, **kw: {},
                fill_fn=lambda *a, **kw: {},
                audit_fn=lambda *a, **kw: {},
                series_fn=lambda root: [],
                refresh_fn=lambda root, tickers, **kw: {},
                progress=record,
                sleep_fn=lambda s: None,
            )
    finally:
        fdp.operation_gate.acquire = real_acquire
    return res, events


def final_states(events):
    last = None
    for e in events:
        if e.get("type") == "ports" and isinstance(e.get("states"), dict):
            last = e["states"]
    return last or {}


def death_logs(events):
    return [str(e.get("message") or "") for e in events
            if e.get("type") == "log" and str(DEAD_PORT) in
            str(e.get("message") or "")]


def run():
    res, events = run_pipeline()
    states = final_states(events)
    dead_state = str(states.get(DEAD_PORT, ""))
    feature_present = dead_state.upper().startswith("DEAD")

    if not feature_present:
        section("BASELINE - a dead worker is indistinguishable from done")
        check("B1 the run itself completes on the surviving port",
              res.get("error") is None
              and any(e.get("type") == "done" for e in events),
              f"error={res.get('error')}")
        check("B2 the dead port's terminal state is the misleading 'done'",
              dead_state == "done", f"state={dead_state!r}")
        check("B3 the death is recorded in port_errors (mechanism exists)",
              any(f"port {DEAD_PORT}" in err and BOOM in err
                  for err in res.get("port_errors") or []),
              f"port_errors={res.get('port_errors')}")
        check("B4 the death is SILENT in the live log today",
              not death_logs(events), str(death_logs(events)))
        return feature_present

    # ---------------- post-implementation acceptance ----------------
    section("A1. terminal state speaks the fetch panel's vocabulary")
    check("A1 dead worker ends 'DEAD - <reason>', not 'done'",
          dead_state.startswith("DEAD - ") and "boom" in dead_state,
          f"state={dead_state!r}")
    check("A1b the reason is bounded (no traceback dumps in a status cell)",
          len(dead_state) <= 160, f"len={len(dead_state)}")

    section("A2. the death is loud in the live log")
    dl = death_logs(events)
    check("A2 a log event names the dead port and reason at death time",
          any("DEAD" in line and "boom" in line for line in dl), str(dl))

    section("A3. clean exits and the result contract are untouched")
    check("A3 the healthy port still ends 'done'",
          states.get(GOOD_PORT) == "done",
          f"state={states.get(GOOD_PORT)!r}")
    check("A3b port_errors still carries the failure",
          any(f"port {DEAD_PORT}" in err and BOOM in err
              for err in res.get("port_errors") or []),
          f"port_errors={res.get('port_errors')}")
    check("A3c the run still completes on the surviving port",
          res.get("error") is None
          and any(e.get("type") == "done" for e in events),
          f"error={res.get('error')}")
    check("A3d ports events still carry plain string states",
          all(isinstance(v, str) for v in states.values()))

    section("A6. inventory self-registration (Row 60 fail-closed guard)")
    rg = (ENGINE_ROOT / "run_gates.py").read_text(encoding="utf-8")
    check("A6 this harness is registered in the static reference inventory",
          '"fixdata_port_death_reference"' in rg
          or "'fixdata_port_death_reference'" in rg,
          "unregistered: every battery fails closed with inventory drift")
    return feature_present


def main():
    feature_present = run()
    if not feature_present:
        KIT.pending(
            "M1",
            "Worker death still ends in the 'done' state: this is the "
            "pre-change baseline.",
            "M1 changes fix_data_pipeline ONLY: on a worker's exception "
            "path the terminal port state becomes 'DEAD - <bounded "
            "reason>' (fetch-panel vocabulary, display renders it "
            "verbatim) and one log event announces the death live; clean "
            "exits keep 'done'; port_errors and every other contract "
            "stay unchanged.")
    return KIT.finish(feature_absent=not feature_present)


if __name__ == "__main__":
    sys.exit(main())
