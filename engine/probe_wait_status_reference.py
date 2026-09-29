"""Acceptance harness: truthful port states while Live Spot Check work waits (Row 45).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (PROBE_WAIT_STATUS_PLAN.md).

Offline only: scripted fix_data_pipeline.run with a probe blocked on an Event;
no bank, no network, no GUI. The operation gate is isolated with the Row 41
testbank kit. Exit contract: 1 = a check failed; 3 = checks green but
fix_data_pipeline.PROBE_WAIT_STATUS is absent (expected pre-M1); 0 = acceptance.
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

ENGINE_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ENGINE_ROOT))

if __name__ == "__main__":
    from fetch_a2_ibkr_workflows_selftest import Operations
    raise SystemExit(Operations.run_fixdata_reference_cli("probe_wait_status_reference"))


import fix_data_pipeline as pipeline  # noqa: E402
import testbank  # noqa: E402

FAILURES = []
COUNT = [0]
_fixture_root = None  # Bound to the existing offline owner's temporary directory.


def check(name, condition, detail=""):
    COUNT[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


class Adapter:
    def __init__(self, port):
        self.port = port

    def disconnect(self):
        pass


class Capture:
    """Thread-safe event log with per-call sequence marks."""

    def __init__(self):
        self.lock = threading.Lock()
        self.events = []

    def __call__(self, event):
        with self.lock:
            self.events.append(dict(event))

    def mark(self):
        with self.lock:
            return len(self.events)

    def since(self, mark, kind=None):
        with self.lock:
            window = self.events[mark:]
        if kind is None:
            return list(window)
        return [e for e in window if e.get("type") == kind]

    def last_states(self):
        with self.lock:
            snaps = [e for e in self.events if e.get("type") == "ports"]
        return dict(snaps[-1]["states"]) if snaps else {}

    def port_saw(self, port, state):
        with self.lock:
            return any(e.get("type") == "ports"
                       and (e.get("states") or {}).get(port) == state
                       for e in self.events)


def drive(stall_s, probe_started, probe_release, capture):
    """Run the two-port, one-ticker, probe-blocked scenario; returns result."""

    def probe_fn(_adapter, _root, ticker, _selection):
        probe_started.set()
        probe_release.wait(10)
        return {"ticker": ticker, "verdict": "MATCH",
                "pass_equivalent": True, "request_count": 1,
                "reused_request_count": 0}

    holder = {}

    def body():
        holder["result"] = pipeline.run(
            root=_fixture_root, ports=(2000, 3000),
            adapter_factory=lambda port: Adapter(port),
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {},
            fill_fn=lambda *_a, **_k: {},
            audit_fn=lambda *_a, **_k: {"status": "ok"},
            series_fn=lambda _root: [("AAA", "1m")],
            refresh_fn=lambda *_a, **_k: {},
            probe_plan_fn=lambda _root, tickers, seed: {
                "seed": seed,
                "items": {ticker: {"day": "2026-01-02"}
                          for ticker in tickers}},
            probe_fn=probe_fn,
            probe_seed=45, run_id="row45-reference",
            progress=capture)

    worker = threading.Thread(target=body)
    with testbank.isolated_gates():
        worker.start()
        started = probe_started.wait(10)
        # The spot-probe label is set BEFORE probe_fn runs. Every worker
        # emits "connecting" exactly once at startup and the late worker's
        # transition can land after the probe begins, so wait until BOTH
        # ports have APPEARED as "connecting" in some snapshot before
        # opening the measurement window (bounded poll = failure guard).
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if all(capture.port_saw(p, "connecting") for p in (2000, 3000)):
                break
            time.sleep(0.01)
        mark = capture.mark()
        time.sleep(stall_s)
        stall_events = capture.since(mark, kind="ports")
        stall_states = capture.last_states()
        probe_release.set()
        worker.join(10)
    return started, worker, holder.get("result"), stall_events, stall_states


def waiting_port_state(states):
    other = {p: s for p, s in states.items() if not str(s).startswith(
        "spot-probe")}
    return next(iter(other.values()), None)


def main():
    print("=== Port wait-status reference (Row 45) ===\n")
    flag = getattr(pipeline, "PROBE_WAIT_STATUS", None) is True

    probe_started = threading.Event()
    probe_release = threading.Event()
    capture = Capture()
    started, worker, result, stall_events, stall_states = drive(
        0.35, probe_started, probe_release, capture)

    check("fixture reaches the blocked-probe stall", started
          and stall_states, repr(stall_states))
    probing = [s for s in stall_states.values()
               if str(s).startswith("spot-probe")]
    waiting = waiting_port_state(stall_states)

    if not flag:
        check("B1 the waiting port never gains a 'waiting:' label today",
              waiting is not None
              and not str(waiting).startswith("waiting:"), repr(waiting))
        check("B2 zero ports events are emitted during the stall "
              "(no heartbeat exists today)",
              len(stall_events) == 0, repr(stall_events[:4]))
        check("B3 the probing port label is static 'spot-probe AAA' "
              "(no elapsed suffix today)",
              probing == ["spot-probe AAA"], repr(probing))
    else:
        check("F1 the waiting port label starts 'waiting:' and names "
              "live counts",
              waiting is not None and str(waiting).startswith("waiting:")
              and any(word in str(waiting)
                      for word in ("probe", "check", "fill")), repr(waiting))
        check("F5 probe-phase labels carry the completed/total counter "
              "(in-flight first probe = 0/1)",
              any("(0/1)" in str(s) for s in probing)
              and waiting is not None and "0/1" in str(waiting),
              repr((probing, waiting)))
        beats = [e for e in stall_events
                 if any(str(s).startswith(("waiting:", "spot-probe"))
                        for s in (e.get("states") or {}).values())]
        check("F2 heartbeats re-emit during the stall",
              len(beats) >= 2, f"{len(beats)} events in stall window")
        probe_labels = [s for e in stall_events
                        for s in (e.get("states") or {}).values()
                        if str(s).startswith("spot-probe AAA")]
        with_elapsed = [s for s in probe_labels if "·" in str(s)
                        or " s" in str(s) or str(s).rstrip("s")[-1:].isdigit()]
        check("F3 the probing port label carries a growing elapsed marker",
              len(with_elapsed) >= 1, repr(probe_labels[:4]))
        all_settled = [e for e in stall_events
                       if (e.get("states") or {})
                       and all(str(s) in ("paused", "done")
                               for s in e["states"].values())]
        check("F4 no stall snapshot claims all-paused/done "
              "(cancel-gate honesty)", not all_settled,
              repr(all_settled[:2]))

    check("B4 release drains cleanly: run completes, probe counted, "
          "all ports done",
          result is not None and result.get("probe_completed") == 1
          and not worker.is_alive()
          and all(s == "done"
                  for s in capture.last_states().values()),
          repr((result or {}).get("probe_completed")))
    probe_events = capture.since(0, kind="probe")
    check("B5 the run-level probe counter data exists today "
          "(probe events carry done/total; final event is 1/1)",
          any(e.get("done") == 1 and e.get("total") == 1
              for e in probe_events), repr(probe_events[:3]))

    stage = "feature stage" if flag else "baseline stage"
    print(f"\n{COUNT[0]} checks, {len(FAILURES)} failed ({stage})")
    if FAILURES:
        print("FAILURES:")
        for name in FAILURES:
            print(f"  - {name}")
        return 1
    if not flag:
        print("\n[M1 PENDING] fix_data_pipeline.PROBE_WAIT_STATUS absent - "
              "M1 not implemented yet. Missing surface:")
        print("  - fix_data_pipeline.PROBE_WAIT_STATUS = True (M1 flag)")
        print("  - 'waiting: <live counts>' set on every unproductive claim "
              "pass (change-only via set_port)")
        print("  - PROBE_WAIT_HEARTBEAT_S re-emission with growing elapsed "
              "while waiting and during in-flight probe requests")
        print("  - probe-phase labels carry the completed/total counter "
              "from the scheduler's existing done/totals, e.g. "
              "'spot-probe T (12/503)' (D6, user request)")
        print("  - Add Stocks post-pipeline parity via port_status "
              "(Codex selftests + Claude review)")
        print("Expected pre-M1 result. Exit 3.")
        return 3
    print("\nM1 ACCEPTED. Exit 0.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
