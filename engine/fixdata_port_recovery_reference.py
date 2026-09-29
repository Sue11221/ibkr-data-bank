"""Acceptance harness: repair port workers reconnect and escalate (Row 68).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (STORAGE_OPTIMIZATION_LOG.md, Row 68).

User order (2026-07-27): "make it so that the repair data also have the
ability to try and reconnect to ibkr and also restart if port is completely
down." Today a worker gets ONE connect attempt at birth; any failure is
permanent for the run (six of eight ports died this way in the live vol run).

Contract this harness pins:
- Connect-time resilience: the worker retries the adapter connect (bounded,
  3 attempts) before giving up.
- Escalation seam: run() gains port_recover_fn=None. After retries are
  exhausted the engine calls it ONCE per port with (port, reason); True means
  "the environment did something (e.g. restarted TWS) - try one more connect
  cycle"; False/None means accept death. The engine stays headless: it never
  knows what recovery means - display_data wires the existing
  'Restart dead ports' machinery behind a USER-GATED surface (default off).
- Death still speaks Row 67's vocabulary (DEAD - <reason>) when everything
  fails, and a port that never needed recovery behaves byte-identically to
  today.

Change polarity: while run() has no port_recover_fn parameter the feature is
absent and this harness exits 3 (pending); after Row 68 lands every check
passes and it exits 0. Exit 1 = a check failed.

Offline and headless: injected fakes; operation_gate patched IN-PROCESS; no
network, no bank, no process, no tkinter.

    python engine/fixdata_port_recovery_reference.py

Placement note: authored in the Claude scratchpad during the Row 65 freeze;
moves to engine/ at freeze lift; Codex's Row 68 checkpoint registers it in
REFERENCE_SUITE_NAMES (A7 asserts that self-registration).
"""

from __future__ import annotations

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
if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

try:                                  # harness hygiene (Row 61)
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

if __name__ == "__main__":
    from fetch_a2_ibkr_workflows_selftest import Operations
    raise SystemExit(Operations.run_fixdata_reference_cli("fixdata_port_recovery_reference"))


import fix_data_pipeline as fdp       # noqa: E402
from check_kit import CheckKit        # noqa: E402

KIT = CheckKit()
check = KIT.check
section = KIT.section

PORT = 7601
DAY = "2024-01-02"

HAS_SEAM = "port_recover_fn" in inspect.signature(fdp.run).parameters


class Factory:
    """Adapter factory failing a scripted number of times, thread-safe."""

    def __init__(self, fail_first=0, heal_on_recover=False):
        self.fail_remaining = fail_first
        self.heal_on_recover = heal_on_recover
        self.calls = 0
        self.lock = threading.Lock()

    def __call__(self, port):
        with self.lock:
            self.calls += 1
            if self.fail_remaining:
                self.fail_remaining -= 1
                raise RuntimeError("boom")
        return None

    def heal(self):
        with self.lock:
            self.fail_remaining = 0


def run_pipeline(factory, recover_fn="omit"):
    events = []
    fills = []
    elock = threading.Lock()

    def record(event):
        with elock:
            events.append(dict(event))

    kwargs = {}
    if recover_fn != "omit":
        kwargs["port_recover_fn"] = recover_fn

    real_acquire = fdp.operation_gate.acquire
    fdp.operation_gate.acquire = (
        lambda mode, **kw: contextlib.nullcontext())
    try:
        with tempfile.TemporaryDirectory(prefix="port_recover_") as tmp:
            res = fdp.run(
                root=tmp, ports=(PORT,),
                adapter_factory=factory,
                scan_fn=lambda root, **kw: {
                    "summary": {"TST 1d": {"missing_day_list": [DAY],
                                           "source_absent_list": []}},
                    "series_scanned": 1, "verification_series": []},
                health_fn=lambda root, **kw: {},
                fill_fn=lambda *a, **kw: fills.append(a) or {},
                audit_fn=lambda *a, **kw: {},
                series_fn=lambda root: [],
                refresh_fn=lambda root, tickers, **kw: {},
                progress=record,
                sleep_fn=lambda s: None,
                **kwargs,
            )
    finally:
        fdp.operation_gate.acquire = real_acquire
    return res, events, fills


def final_state(events):
    last = {}
    for e in events:
        if e.get("type") == "ports" and isinstance(e.get("states"), dict):
            last = e["states"]
    return str(last.get(PORT, ""))


def run():
    if not HAS_SEAM:
        section("BASELINE - one connect attempt, failure is permanent")
        factory = Factory(fail_first=1)
        res, events, fills = run_pipeline(factory)
        check("B1 a single connect failure strands the whole fill queue",
              (res.get("halted") or 0) >= 1 or res.get("unprocessed"),
              f"halted={res.get('halted')} unprocessed={len(res.get('unprocessed') or [])}")
        check("B2 the factory is tried exactly once today (no retry exists)",
              factory.calls == 1, f"calls={factory.calls}")
        check("B3 the fill never ran", not fills, f"fills={len(fills)}")
        check("B4 run() has no recovery seam today",
              "port_recover_fn"
              not in inspect.signature(fdp.run).parameters)
        return

    # ---------------- post-implementation acceptance ----------------
    section("A1. connect-time retry heals a transient failure")
    factory = Factory(fail_first=2)          # attempts 1,2 fail; 3 succeeds
    res, events, fills = run_pipeline(factory)
    check("A1 the worker survives two transient connect failures",
          len(fills) == 1 and res.get("error") is None,
          f"fills={len(fills)} error={res.get('error')}")
    check("A1b exactly three connect attempts (bounded policy)",
          factory.calls == 3, f"calls={factory.calls}")
    check("A1c the port finishes 'done'", final_state(events) == "done",
          f"state={final_state(events)!r}")

    section("A2. exhaustion without a recover seam dies in Row 67 vocabulary")
    factory = Factory(fail_first=99)
    res2, events2, fills2 = run_pipeline(factory)
    check("A2 bounded attempts then death (no infinite churn)",
          factory.calls == 3, f"calls={factory.calls}")
    check("A2b terminal state is DEAD - <reason>",
          final_state(events2).startswith("DEAD"),
          f"state={final_state(events2)!r}")
    check("A2c the stranded work is honest debt",
          (res2.get("halted") or 0) >= 1 or res2.get("unprocessed"),
          f"halted={res2.get('halted')}")

    section("A3. the escalation seam - recover, then one more cycle")
    factory = Factory(fail_first=99)
    recover_calls = []

    def recover(port, reason):
        recover_calls.append((port, str(reason)))
        factory.heal()                        # 'the environment fixed TWS'
        return True

    res3, events3, fills3 = run_pipeline(factory, recover_fn=recover)
    check("A3 recover_fn called exactly once, with the port",
          len(recover_calls) == 1 and recover_calls[0][0] == PORT,
          f"calls={recover_calls}")
    check("A3b a truthful reason string reaches the recover seam",
          "boom" in recover_calls[0][1] or "RuntimeError"
          in recover_calls[0][1], f"reason={recover_calls[0][1]!r}")
    check("A3c the revived port completes the fill",
          len(fills3) == 1 and res3.get("error") is None,
          f"fills={len(fills3)} error={res3.get('error')}")
    check("A3d the port ends 'done' after revival",
          final_state(events3) == "done", f"state={final_state(events3)!r}")

    section("A4. recover declined -> death stands, exactly once")
    factory = Factory(fail_first=99)
    declined = []
    res4, events4, fills4 = run_pipeline(
        factory, recover_fn=lambda port, reason: declined.append(port))
    check("A4 recover_fn consulted once, decline accepted",
          len(declined) == 1 and final_state(events4).startswith("DEAD"),
          f"declined={declined} state={final_state(events4)!r}")
    check("A4b no fill ran on the dead port", not fills4)

    section("A5. a crashing recover_fn never breaks the run")
    factory = Factory(fail_first=99)

    def bad_recover(port, reason):
        raise RuntimeError("observer crash must be swallowed")

    res5, events5, _ = run_pipeline(factory, recover_fn=bad_recover)
    check("A5 run completes; the port is DEAD, not the pipeline",
          any(e.get("type") == "done" for e in events5)
          and final_state(events5).startswith("DEAD"),
          f"state={final_state(events5)!r}")

    section("A6. healthy ports are byte-identical to today")
    factory = Factory(fail_first=0)
    res6, events6, fills6 = run_pipeline(factory)
    check("A6 one connect, fill done, port 'done', no recovery involvement",
          factory.calls == 1 and len(fills6) == 1
          and final_state(events6) == "done",
          f"calls={factory.calls} fills={len(fills6)}")

    section("A7. inventory self-registration (Row 60 fail-closed guard)")
    rg = (ENGINE_ROOT / "run_gates.py").read_text(encoding="utf-8")
    check("A7 this harness is registered in the static reference inventory",
          '"fixdata_port_recovery_reference"' in rg
          or "'fixdata_port_recovery_reference'" in rg,
          "unregistered: every battery fails closed with inventory drift")


def main():
    run()
    if not HAS_SEAM:
        KIT.pending(
            "M1",
            "run() has no port_recover_fn seam: this is the pre-change "
            "baseline (one connect attempt, permanent death).",
            "M1 (engine): bounded 3-attempt connect retry per worker; on "
            "exhaustion consult port_recover_fn(port, reason) once - True "
            "buys one more connect cycle, False/None/exception accepts "
            "death in Row 67's DEAD vocabulary; healthy ports unchanged.",
            "M2 (wiring): display_data offers the existing 'Restart dead "
            "ports' machinery through the seam behind a USER-GATED surface "
            "(default OFF - the restart flow takes over the screen).")
    return KIT.finish(feature_absent=not HAS_SEAM)


if __name__ == "__main__":
    sys.exit(main())
