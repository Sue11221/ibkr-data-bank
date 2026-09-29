"""Offline tests for the extracted Fix Data stage-barrier engine."""

from __future__ import annotations

import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


def main():
    # Preserve this entry point, with confinement owned by the registered suite.
    from fetch_a2_ibkr_workflows_selftest import Operations
    import unittest
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite([
        Operations("test_c2_legacy_fixdata_scheduler_contracts")]))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())


import fix_data_pipeline as pipeline  # noqa: E402
import operation_gate  # noqa: E402


checks = []


def check(name, value):
    checks.append(bool(value))
    print(f"[{'PASS' if value else 'FAIL'}] {name}")


def atomic_reconcile_outcome(
        row, state, *, indeterminate=False, month_write_completed=None):
    old_sha = "a" * 64
    new_sha = "b" * 64
    rolled_back = state == "rolled_back"
    if month_write_completed is None:
        month_write_completed = not rolled_back
    bank_written = (False if rolled_back else
                    (None if indeterminate else True))
    correction_recorded = None if indeterminate else False
    correction = {
        "version": 1,
        "type": "vol_value_refetch_reconcile",
        "day": row["day"],
        "run": "offline-atomic-test",
        "source": "IBKR:vol-value-reconcile",
        "confirmed": "2026-07-22T12:00:00Z",
        "what_to_show": "OPTION_IMPLIED_VOLATILITY",
        "request_count": 1,
        "bars": 390,
        "old_value": 0.2,
        "new_value": 0.3,
        "old_day_sha256": "c" * 64,
        "new_day_sha256": "d" * 64,
        "old_month_sha256": old_sha,
        "new_month_sha256": new_sha,
        "reason": "jump_up",
        "reasons": ["jump_up"],
    }
    evidence = {
        "rollback_verified": rolled_back,
        "month_write_completed": bool(month_write_completed),
        "bank_written": bank_written,
        "correction_recorded": correction_recorded,
        "month_state": ("original" if rolled_back else
                        ("indeterminate" if indeterminate else "corrected")),
        "manifest_state": "malformed" if indeterminate else "original",
        "original_month_file": "2026-01-1d-iv.parquet",
        "written_month_file": "2026-01-1d-iv.parquet",
        "old_month_sha256": old_sha,
        "new_month_sha256": new_sha,
        "observed_original_sha256": (
            None if indeterminate else (old_sha if rolled_back else new_sha)),
        "observed_written_sha256": (
            None if indeterminate else (old_sha if rolled_back else new_sha)),
        "correction": correction,
        "restore_errors": (["manifest restore failed"]
                           if indeterminate else []),
    }
    result = {
        "status": "unresolved" if rolled_back else "ambiguous",
        "ticker": row["ticker"],
        "kind": row["kind_token"],
        "kind_token": row["kind_token"],
        "day": row["day"],
        "request_count": 1,
        "queue_resolved": False,
        "error": "synthetic post-month publication failure",
        "reason": "jump_up",
        "reasons": ["jump_up"],
        "commit_state": state,
        "rollback_verified": rolled_back,
        "bank_write_completed": bool(month_write_completed),
        "bank_written": bank_written,
        "correction_recorded": correction_recorded,
        "commit_evidence": evidence,
        "hostile_extra": object(),
        "oversized_extra": "x" * 100_000,
    }
    if bank_written is not None:
        result["bank_committed"] = bank_written
    return result


class Adapter:
    def __init__(self, port, trace):
        self.port = port
        self.trace = trace
        self.owner = threading.get_ident()

    def assert_owner(self):
        assert threading.get_ident() == self.owner

    def disconnect(self):
        self.assert_owner()
        self.trace.append(("disconnect", self.port))


def run_case(*, failed_ports=(), pause=None, cancel=None, lock_path=None,
             kinds=("",), refresh_error=None):
    trace = []
    events = []

    def factory(port):
        trace.append(("connect", port))
        if port in failed_ports:
            raise RuntimeError("connect failed")
        return Adapter(port, trace)

    def scan(*_args, **_kwargs):
        trace.append(("scan", _kwargs.get("kinds"),
                      _kwargs.get("preserve_existing")))
        return {"series_scanned": 2, "summary": {
            "AAA 1m": {"missing_day_list": ["2026-01-02"]},
            "BBB 1m": {},
        }, "verification_series": [("AAA", "1m"), ("BBB", "1m")]}

    def health(_root):
        trace.append(("health",))
        return {"health_triage": 2}

    def fill(adapter, _root, ticker, interval, _days, **_kwargs):
        adapter.assert_owner()
        trace.append(("fill", ticker, interval, adapter.port))
        return {"added": 3, "source_absent": []}

    def audit(adapter, _root, ticker, interval, cache_root):
        adapter.assert_owner()
        trace.append(("audit", ticker, interval, adapter.port, cache_root))
        return {"status": "flagged" if ticker == "BBB" else "ok"}

    def refresh(_root, touched, **_kwargs):
        trace.append(("refresh", tuple(touched), _kwargs.get("kinds")))
        if refresh_error:
            return {"error": refresh_error}
        return {"missing_days_run": 0,
                "verification_series": list(touched)}

    old_path = operation_gate.LOCK_PATH
    if lock_path is not None:
        operation_gate.LOCK_PATH = Path(lock_path)
    try:
        result = pipeline.run(
            root="bank", ports=(2000, 3000), adapter_factory=factory,
            scan_fn=scan, health_fn=health, fill_fn=fill, audit_fn=audit,
            series_fn=lambda _root: [("AAA", "1m"), ("BBB", "1m")],
            refresh_fn=refresh, progress=events.append,
            pause_event=pause, cancel_event=cancel, cache_root="cache",
            run_id="test-run", sleep_fn=lambda _seconds: None,
            verification_fn=lambda stage, ticker, interval:
                trace.append(("verify", stage, ticker, interval)),
            kinds=kinds)
    finally:
        operation_gate.LOCK_PATH = old_path
    return result, trace, events


with tempfile.TemporaryDirectory(prefix="fixdata_pipeline_") as tmp:
    lock = Path(tmp) / "gate.lock"
    result, trace, events = run_case(lock_path=lock)
    fills = [i for i, row in enumerate(trace) if row[0] == "fill"]
    audits = [i for i, row in enumerate(trace) if row[0] == "audit"]
    aaa_fill = next(i for i, row in enumerate(trace)
                    if row[:2] == ("fill", "AAA"))
    aaa_audit = next(i for i, row in enumerate(trace)
                     if row[:2] == ("audit", "AAA"))
    check("each ticker audit follows its repair boundary", aaa_fill < aaa_audit)
    check("adapters are thread-owned and disconnected by their owners",
          len([row for row in trace if row[0] == "disconnect"]) == 2)
    check("final counters and touched refresh are preserved",
          result["added"] == 3 and result["checked"] == 2
          and result["flagged"] == 1 and result["health_triage"] == 2
          and any(row[0] == "refresh" for row in trace))
    check("default kind scope reaches scan and refresh with preservation",
          ("scan", ("",), True) in trace
          and any(row[0] == "refresh" and row[2] == ("",)
                  for row in trace))
    check("verification callbacks follow successful persisted boundaries",
          ("verify", "gaps", "BBB", "1m") in trace
          and ("verify", "gaps", "AAA", "1m") in trace
          and {(row[2], row[3]) for row in trace
               if row[:2] == ("verify", "xval")}
          == {("AAA", "1m"), ("BBB", "1m")})
    check("every progress event carries the exact run id and sequence",
          events and all(event["run_id"] == "test-run" for event in events)
          and [event["seq"] for event in events]
          == list(range(1, len(events) + 1)))
    legacy_stage_two = [event for event in events
                        if event.get("type") == "stage"
                        and event.get("stage") == 2]
    check("legacy stage-two wording stays repair/check without reconciliation",
          len(legacy_stage_two) == 1
          and legacy_stage_two[0].get("state") == "repair/check")
    operation_events = [event for event in events
                        if event["type"] in ("refetch", "xcheck")
                        and event.get("done", 0)]
    check("operation progress identifies ticker, interval, operation, and port",
          operation_events and all(
              event.get("ticker") and event.get("interval")
              and event.get("operation") in ("repair", "check")
              and event.get("port") in (2000, 3000)
              for event in operation_events))

    selected, selected_trace, _selected_events = run_case(
        lock_path=lock, kinds=("iv", "hvol"))
    check("selected kind tuple is frozen unchanged through scan and refresh",
          selected["error"] is None
          and ("scan", ("iv", "hvol"), True) in selected_trace
          and any(row[0] == "refresh"
                  and row[2] == ("iv", "hvol")
                  for row in selected_trace))

    all_kinds, all_kinds_trace, _ = run_case(
        lock_path=lock, kinds=None)
    single_kind, single_kind_trace, _ = run_case(
        lock_path=lock, kinds="iv")
    check("legacy None and single-text kind scopes retain their meanings",
          all_kinds["error"] is None
          and ("scan", None, True) in all_kinds_trace
          and any(row[0] == "refresh" and row[2] is None
                  for row in all_kinds_trace)
          and single_kind["error"] is None
          and ("scan", ("iv",), True) in single_kind_trace
          and any(row[0] == "refresh" and row[2] == ("iv",)
                  for row in single_kind_trace))

    refresh_failed, refresh_trace, refresh_events = run_case(
        lock_path=lock, refresh_error="report replacement refused")
    refresh_index = next(index for index, row in enumerate(refresh_trace)
                         if row[0] == "refresh")
    check("post-fill refresh persistence failure is a truthful run error",
          "report replacement refused" in (refresh_failed["error"] or "")
          and not any(row[0] == "verify" and row[1] == "gaps"
                      for row in refresh_trace[refresh_index + 1:])
          and any(event.get("type") == "log"
                  and "gap refresh failed" in event.get("message", "")
                  for event in refresh_events))

    failed_scan_calls = []
    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        failed_scan = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: failed_scan_calls.append(
                ("connect", port)),
            scan_fn=lambda *_a, **_k: {
                "error": "existing report cannot be preserved",
                "summary": {"AAA 1m": {
                    "missing_day_list": ["2026-01-02"]}}},
            health_fn=lambda _root: failed_scan_calls.append(("health",)),
            fill_fn=lambda *_a, **_k: failed_scan_calls.append(("fill",)),
            audit_fn=lambda *_a, **_k: failed_scan_calls.append(("audit",)),
            series_fn=lambda _root: failed_scan_calls.append(("series",)),
            refresh_fn=lambda *_a, **_k: failed_scan_calls.append(("refresh",)),
            run_id="failed-scan")
    finally:
        operation_gate.LOCK_PATH = old_path
    check("stage-1 persistence failure stops before ports, fill, or audit",
          "existing report cannot be preserved" in (failed_scan["error"] or "")
          and failed_scan_calls == [])

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        observer_safe = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: Adapter(port, []),
            scan_fn=lambda *_a, **_k: {
                "summary": {}, "sidecar": "gaps.json",
                "verification_series": [("AAA", "1m")]},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=lambda *_a, **_k: {"status": "ok"},
            series_fn=lambda _root: [("AAA", "1m")],
            refresh_fn=lambda *_a, **_k: {}, run_id="observer-safe",
            verification_fn=lambda *_args: (_ for _ in ()).throw(
                RuntimeError("observer failed")))
    finally:
        operation_gate.LOCK_PATH = old_path
    check("verification observer errors cannot alter Fix Data control flow",
          observer_safe["error"] is None and observer_safe["checked"] == 1)

    current_trace = []
    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        noncurrent = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: Adapter(port, []),
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=lambda *_a, **_k: {
                "status": "inconclusive", "_verification_current": False},
            series_fn=lambda _root: [("AAA", "1m")],
            refresh_fn=lambda *_a, **_k: {}, run_id="noncurrent",
            verification_fn=lambda *args: current_trace.append(args))
    finally:
        operation_gate.LOCK_PATH = old_path
    check("non-current cross-check evidence remains verification debt",
          noncurrent["checked"] == 1 and current_trace == [])

    failed, failed_trace, _events = run_case(
        failed_ports=(2000,), lock_path=lock)
    check("one connect failure leaves work for healthy ports",
          failed["checked"] == 2 and failed["added"] == 3
          and len(failed["port_errors"]) == 1)

    all_failed, _trace, _events = run_case(
        failed_ports=(2000, 3000), lock_path=lock)
    check("all-port failure reports every unprocessed item",
          all_failed["unprocessed"] == [
              ("AAA", "1m", "repair", "worker-unavailable"),
              ("AAA", "1m", "check", "worker-unavailable"),
              ("BBB", "1m", "check", "worker-unavailable"),
          ]
          and len(all_failed["port_errors"]) == 2)

    def retry_scan(*_args, **_kwargs):
        return {
            "series_scanned": 1,
            "summary": {"AAA 1m": {
                "missing_day_list": ["2026-01-02"],
                "source_absent_list": [],
            }},
            "verification_series": [],
        }

    retry_calls = []
    retry_trace = []
    retry_events = []
    retry_sleeps = []
    retry_fills = []

    def transient_factory(port):
        retry_calls.append(port)
        if len(retry_calls) <= 2:
            raise RuntimeError("synthetic transient connect refusal")
        return Adapter(port, retry_trace)

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        retry_healed = pipeline.run(
            root="bank", ports=(2000,), adapter_factory=transient_factory,
            scan_fn=retry_scan, health_fn=lambda _root: {},
            fill_fn=lambda *args, **_kwargs:
                retry_fills.append(args) or {"added": 1},
            audit_fn=lambda *_args, **_kwargs: {},
            series_fn=lambda _root: [],
            refresh_fn=lambda *_args, **_kwargs: {},
            progress=retry_events.append,
            sleep_fn=retry_sleeps.append,
            run_id="connect-retry-heals")
    finally:
        operation_gate.LOCK_PATH = old_path
    retry_states = [
        str(event.get("states", {}).get(2000, ""))
        for event in retry_events if event.get("type") == "ports"]
    retry_backoffs = [
        delay for delay in retry_sleeps
        if delay == pipeline.CONNECT_RETRY_BACKOFF_S]
    check("connect retries heal two transient refusals within one cycle",
          retry_calls == [2000, 2000, 2000]
          and len(retry_fills) == 1
          and retry_healed["error"] is None
          and retry_states[-1:] == ["done"])
    check("retry status is visible and backoff remains bounded",
          "connecting (attempt 2/3)" in retry_states
          and "connecting (attempt 3/3)" in retry_states
          and retry_backoffs
          == [pipeline.CONNECT_RETRY_BACKOFF_S] * 2)

    exhaust_calls = []
    exhaust_events = []
    exhaust_sleeps = []
    exhaust_recoveries = []
    exhaust_fills = []

    def exhausted_factory(port):
        exhaust_calls.append(port)
        raise RuntimeError("synthetic permanent connect refusal")

    def approve_one_recovery(port, reason):
        exhaust_recoveries.append((port, str(reason)))
        return True

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        exhausted = pipeline.run(
            root="bank", ports=(2000,), adapter_factory=exhausted_factory,
            scan_fn=retry_scan, health_fn=lambda _root: {},
            fill_fn=lambda *args, **_kwargs:
                exhaust_fills.append(args) or {"added": 1},
            audit_fn=lambda *_args, **_kwargs: {},
            series_fn=lambda _root: [],
            refresh_fn=lambda *_args, **_kwargs: {},
            progress=exhaust_events.append,
            sleep_fn=exhaust_sleeps.append,
            port_recover_fn=approve_one_recovery,
            run_id="connect-recovery-exhausted")
    finally:
        operation_gate.LOCK_PATH = old_path
    exhaust_states = [
        str(event.get("states", {}).get(2000, ""))
        for event in exhaust_events if event.get("type") == "ports"]
    exhaust_backoffs = [
        delay for delay in exhaust_sleeps
        if delay == pipeline.CONNECT_RETRY_BACKOFF_S]
    check("approved recovery buys exactly one non-recursive retry cycle",
          exhaust_calls == [2000] * 6
          and len(exhaust_recoveries) == 1
          and exhaust_recoveries[0][0] == 2000
          and "permanent connect refusal" in exhaust_recoveries[0][1]
          and exhaust_backoffs
          == [pipeline.CONNECT_RETRY_BACKOFF_S] * 4)
    check("second-cycle exhaustion remains honest DEAD work debt",
          not exhaust_fills
          and exhaust_states[-1].startswith("DEAD - ")
          and len(exhausted["port_errors"]) == 1
          and exhausted["unprocessed"])

    all_failed_reconcile_trace = []
    all_failed_reconcile_row = {
        "ticker": "AAA", "kind": "1d-iv", "kind_token": "1d-iv",
        "day": "2026-01-03",
    }

    def fail_reconcile_adapter(port):
        all_failed_reconcile_trace.append(("connect", port))
        raise RuntimeError("synthetic adapter refusal")

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        all_failed_reconcile = pipeline.run(
            root="bank", ports=(2000, 3000),
            adapter_factory=fail_reconcile_adapter,
            scan_fn=lambda *_a, **_k: {"summary": {
                "AAA 1m": {"missing_day_list": ["2026-01-02"]}}},
            health_fn=lambda _root: {},
            fill_fn=lambda *_a, **_k:
                all_failed_reconcile_trace.append(("fill",)) or {},
            audit_fn=lambda *_a, **_k:
                all_failed_reconcile_trace.append(("audit",)) or {},
            series_fn=lambda _root: [("AAA", "1m")],
            refresh_fn=lambda *_a, **_k: {},
            reconcile_plan_fn=lambda _root, **_kwargs: [
                dict(all_failed_reconcile_row)],
            reconcile_fn=lambda *_a, **_k:
                all_failed_reconcile_trace.append(("reconcile",)) or {},
            reconcile_pacer_factory=object,
            run_id="all-adapters-fail-reconcile")
    finally:
        operation_gate.LOCK_PATH = old_path
    check("all-adapter failure retains exact downstream replan debt",
          all_failed_reconcile["unprocessed"] == [
              ("AAA", "1m", "repair", "worker-unavailable"),
              ("AAA", None, "reconcile-plan", "worker-unavailable"),
              ("AAA", "1m", "check", "worker-unavailable"),
          ]
          and all_failed_reconcile["reconcile_candidate_tickers"] == 1
          and all_failed_reconcile["reconcile_selected"] == 0
          and all_failed_reconcile["reconcile_completed"] == 0
          and all_failed_reconcile["reconcile_queue_pending"] == 0
          and all_failed_reconcile["reconcile_selection_unknown"] == 1
          and len(all_failed_reconcile["port_errors"]) == 2
          and not any(row[0] in ("fill", "audit", "reconcile")
                      for row in all_failed_reconcile_trace))

    overlap_trace = []
    fill_started = threading.Event()
    fill_release = threading.Event()
    unrelated_checked = threading.Event()

    def overlap_fill(adapter, _root, ticker, interval, _days, **_kwargs):
        adapter.assert_owner()
        overlap_trace.append(("fill-start", ticker, adapter.port))
        fill_started.set()
        fill_release.wait(2)
        overlap_trace.append(("fill-end", ticker, adapter.port))
        return {"added": 1}

    def overlap_audit(adapter, _root, ticker, interval, _cache):
        adapter.assert_owner()
        overlap_trace.append(("audit", ticker, adapter.port))
        if ticker == "BBB":
            unrelated_checked.set()
        return {"status": "ok"}

    overlap_result = []
    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        overlap_thread = threading.Thread(target=lambda: overlap_result.append(
            pipeline.run(
                root="bank", ports=(2000, 3000),
                adapter_factory=lambda port: Adapter(port, overlap_trace),
                scan_fn=lambda *_a, **_k: {"summary": {
                    "AAA 1m": {"missing_day_list": ["2026-01-02"]},
                    "BBB 1m": {}}},
                health_fn=lambda _root: {}, fill_fn=overlap_fill,
                audit_fn=overlap_audit,
                series_fn=lambda _root: [("AAA", "1m"), ("BBB", "1m")],
                refresh_fn=lambda *_a, **_k: {}, run_id="overlap")))
        overlap_thread.start()
        check("blocked repair starts", fill_started.wait(2))
        check("unrelated check finishes while another repair is in flight",
              unrelated_checked.wait(1))
        check("same-ticker check cannot cross its repair boundary",
              not any(row[:2] == ("audit", "AAA") for row in overlap_trace))
        fill_release.set()
        overlap_thread.join(2)
    finally:
        operation_gate.LOCK_PATH = old_path
        fill_release.set()
    check("overlap run drains without losing counters",
          overlap_result and overlap_result[0]["checked"] == 2
          and overlap_result[0]["added"] == 1 and not overlap_thread.is_alive())

    priority_trace = []
    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        priority = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: Adapter(port, priority_trace),
            scan_fn=lambda *_a, **_k: {"summary": {
                "AAA 1m": {"missing_day_list": ["2026-01-02"]},
                "BBB 1m": {"missing_day_list": ["2026-01-03"]}}},
            health_fn=lambda _root: {},
            fill_fn=lambda adapter, _root, ticker, interval, _days, **_k:
                (adapter.assert_owner(), priority_trace.append(("fill", ticker)),
                 {"added": 1})[-1],
            audit_fn=lambda adapter, _root, ticker, interval, _cache:
                (adapter.assert_owner(), priority_trace.append(("audit", ticker)),
                 {"status": "ok"})[-1],
            series_fn=lambda _root: [("AAA", "1m"), ("BBB", "1m")],
            refresh_fn=lambda *_a, **_k: {}, run_id="priority",
            probe_plan_fn=lambda _root, tickers, seed: {
                "seed": seed, "items": {ticker: {"day": "2026-01-02"}
                                          for ticker in tickers}},
            probe_fn=lambda adapter, _root, ticker, _selection:
                (adapter.assert_owner(), priority_trace.append(("probe", ticker)),
                 {"ticker": ticker, "verdict": "MATCH",
                  "pass_equivalent": True, "request_count": 1})[-1],
            probe_seed=7)
    finally:
        operation_gate.LOCK_PATH = old_path
    priority_ops = [row[0] for row in priority_trace
                    if row[0] in ("fill", "audit", "probe")]
    check("single port claims every waiting repair before lower-priority checks",
          priority_ops == ["fill", "fill", "audit", "audit",
                           "probe", "probe"]
          and priority["added"] == 2 and priority["checked"] == 2
          and priority["probe_completed"] == 2)

    reconcile_trace = []
    reconcile_events = []
    reconcile_reports = []
    reconcile_pacers = []
    reconcile_locks = []
    reconcile_plan_rows = [
        {"ticker": "AAA", "kind": "1d-iv", "kind_token": "1d-iv",
         "day": "2026-01-02", "value": 0.2, "reason": "jump_up",
         "reasons": ["jump_up"]},
        {"ticker": "AAA", "kind": "1d-iv", "kind_token": "1d-iv",
         "day": "2026-01-03", "value": 0.3, "reason": "jump_down",
         "reasons": ["jump_down"]},
        {"ticker": "BBB", "kind": "1d-hvol",
         "kind_token": "1d-hvol", "day": "2026-01-04", "value": 0.4,
         "reason": "jump_up", "reasons": ["jump_up"]},
    ]

    class ReconcilePacer:
        pass

    def reconcile_pacer_factory():
        value = ReconcilePacer()
        reconcile_pacers.append(value)
        return value

    def reconcile_plan(_root, *, ticker, kinds):
        reconcile_trace.append(("plan", ticker, tuple(kinds)))
        return {"rows": [
            dict(row) for row in reconcile_plan_rows
            if ticker is None or row["ticker"] == ticker
        ]}

    def reconcile_one(adapter, pacer, _root, row, parent_run_id, *,
                      cancel, manifest_lock, progress):
        adapter.assert_owner()
        assert callable(cancel) and not cancel()
        assert parent_run_id == "reconcile-order"
        assert isinstance(pacer, ReconcilePacer)
        reconcile_locks.append((row["ticker"], id(manifest_lock)))
        progress("bounded fake request")
        reconcile_trace.append((
            "reconcile", row["ticker"], row["kind_token"], row["day"],
            adapter.port, id(pacer)))
        if row["ticker"] == "BBB":
            raise RuntimeError("synthetic source refusal")
        status = ("settled" if row["day"] == "2026-01-02"
                  else "corrected")
        return {
            "status": status, "ticker": row["ticker"],
            "kind": row["kind_token"], "kind_token": row["kind_token"],
            "day": row["day"], "request_count": 1,
            "queue_resolved": True,
            **({"registry_committed": True} if status == "settled"
               else {"bank_committed": True}),
        }

    def reconcile_report(_root, parent_run_id, rows, *, requested_count,
                         summary):
        reconcile_reports.append(
            (parent_run_id, list(rows), requested_count, dict(summary)))
        return "reconcile-report.json"

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        reconciled = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: Adapter(port, reconcile_trace),
            scan_fn=lambda *_a, **_k: {"summary": {
                "AAA 1d-iv": {"missing_day_list": ["2026-01-01"]},
                "BBB 1d-hvol": {"missing_day_list": ["2026-01-01"]}}},
            health_fn=lambda _root: {},
            fill_fn=lambda adapter, _root, ticker, interval, _days, **_k:
                (adapter.assert_owner(),
                 reconcile_trace.append(("fill", ticker, interval)),
                 {"added": 1})[-1],
            audit_fn=lambda adapter, _root, ticker, interval, _cache:
                (adapter.assert_owner(),
                 reconcile_trace.append(("audit", ticker, interval)),
                 {"status": "ok"})[-1],
            series_fn=lambda _root: [("AAA", "1m"), ("BBB", "1m")],
            refresh_fn=lambda *_a, **_k: {},
            progress=reconcile_events.append,
            run_id="reconcile-order", kinds=("iv", "hvol"),
            reconcile_plan_fn=reconcile_plan,
            reconcile_fn=reconcile_one,
            reconcile_pacer_factory=reconcile_pacer_factory,
            reconcile_report_fn=reconcile_report,
            probe_plan_fn=lambda _root, tickers, seed: {
                "seed": seed, "items": {
                    ticker: {"day": "2026-01-05"} for ticker in tickers}},
            probe_fn=lambda adapter, _root, ticker, _selection:
                (adapter.assert_owner(),
                 reconcile_trace.append(("probe", ticker)),
                 {"ticker": ticker, "verdict": "MATCH",
                  "pass_equivalent": True, "request_count": 1})[-1])
    finally:
        operation_gate.LOCK_PATH = old_path
    reconcile_ops = [row[0] for row in reconcile_trace
                     if row[0] in ("fill", "reconcile", "audit", "probe")]
    check("reconcile phase orders repair, each queue row, check, then probe",
          reconcile_ops == ["fill", "fill", "reconcile", "reconcile",
                            "reconcile", "audit", "audit", "probe", "probe"])
    check("reconcile callback receives one worker pacer and one ticker lock",
          len(reconcile_pacers) == 1
          and len({row[-1] for row in reconcile_trace
                   if row[0] == "reconcile"}) == 1
          and len({lock_id for ticker, lock_id in reconcile_locks
                   if ticker == "AAA"}) == 1)
    check("reconcile plan receives the frozen caller kind scope",
          ("plan", None, ("iv", "hvol")) in reconcile_trace
          and ("plan", "AAA", ("iv", "hvol")) in reconcile_trace
          and ("plan", "BBB", ("iv", "hvol")) in reconcile_trace)
    check("reconcile ledger distinguishes source outcomes truthfully",
          reconciled["reconcile_selected"] == 3
          and reconciled["reconcile_completed"] == 3
           and reconciled["reconcile_request_count"] == 2
          and reconciled["reconcile_request_unknown"] == 1
          and reconciled["reconcile_settled"] == 1
          and reconciled["reconcile_corrected"] == 1
          and reconciled["reconcile_unresolved"] == 1
          and reconciled["reconcile_queue_resolved"] == 2
           and [row["status"] for row in reconciled["reconcile_rows"]]
          == ["settled", "corrected", "ambiguous"]
          and "synthetic source refusal"
          in reconciled["reconcile_rows"][2]["error"])
    check("reconcile report sees completed rows and original requested total",
          reconciled["reconcile_report"] == "reconcile-report.json"
          and len(reconcile_reports) == 1
          and reconcile_reports[0][0] == "reconcile-order"
          and len(reconcile_reports[0][1]) == 3
          and reconcile_reports[0][2] == 3
          and reconcile_reports[0][3] == {
              "completed_count": 3, "request_count": 2,
              "request_count_unknown": 1,
              "settled_count": 1, "corrected_count": 1,
              "unresolved_count": 1, "queue_resolved_count": 2,
              "queue_pending_count": 1, "rows_truncated": 0,
          })
    reconcile_stage_two = [event for event in reconcile_events
                           if event.get("type") == "stage"
                           and event.get("stage") == 2]
    check("reconcile-enabled stage-two wording names the added phase",
          len(reconcile_stage_two) == 1
          and reconcile_stage_two[0].get("state")
          == "repair/reconcile/check")
    reconcile_done_events = [event for event in reconcile_events
                             if event.get("type") == "reconcile"
                             and event.get("done")]
    check("reconcile progress is exact with truthful dynamic totals and identity",
          [event["done"] for event in reconcile_done_events] == [1, 2, 3]
          and [event["total"] for event in reconcile_done_events] == [2, 2, 3]
          and all(event.get("ticker") and event.get("interval")
                  and event.get("day") and event.get("port") == 2000
                  and event.get("operation") == "reconcile"
                  for event in reconcile_done_events))

    drift_repaired = [False]
    drift_plan_trace = []
    drift_reconciled = []
    obsolete_row = {
        "ticker": "CTX", "kind_token": "1d-iv", "kind": "1d-iv",
        "day": "2026-05-01", "reason": "jump_up",
        "reasons": ["jump_up"],
    }
    current_rows = [
        {"ticker": "CTX", "kind_token": "1d-iv", "kind": "1d-iv",
         "day": "2026-05-02", "reason": "jump_down",
         "reasons": ["jump_down"]},
        {"ticker": "CTX", "kind_token": "1d-hvol", "kind": "1d-hvol",
         "day": "2026-05-03", "reason": "hard_ceiling",
         "reasons": ["hard_ceiling"]},
    ]

    def drift_plan(_root, *, ticker, kinds):
        drift_plan_trace.append((ticker, drift_repaired[0], tuple(kinds)))
        if ticker is None:
            return [dict(obsolete_row)]
        assert ticker == "CTX" and drift_repaired[0]
        return [dict(row) for row in current_rows]

    def drift_reconcile(adapter, _pacer, _root, row, _run_id, **_kwargs):
        adapter.assert_owner()
        drift_reconciled.append((row["kind_token"], row["day"]))
        status = "settled" if row["day"] == "2026-05-02" else "corrected"
        return {
            "status": status, "ticker": row["ticker"],
            "kind_token": row["kind_token"], "day": row["day"],
            "request_count": 1, "queue_resolved": True,
            **({"registry_committed": True} if status == "settled"
               else {"bank_committed": True}),
        }

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        drift = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: Adapter(port, []),
            scan_fn=lambda *_a, **_k: {"summary": {
                "CTX 1d-iv": {"missing_day_list": ["2026-05-01"]}}},
            health_fn=lambda _root: {},
            fill_fn=lambda adapter, *_a, **_k: (
                adapter.assert_owner(), drift_repaired.__setitem__(0, True),
                {"added": 1})[-1],
            audit_fn=lambda *_a, **_k: {"status": "ok"},
            series_fn=lambda _root: [], refresh_fn=lambda *_a, **_k: {},
            kinds=("iv", "hvol"), reconcile_plan_fn=drift_plan,
            reconcile_fn=drift_reconcile, reconcile_pacer_factory=object,
            run_id="post-repair-context-drift")
    finally:
        operation_gate.LOCK_PATH = old_path
    check("post-repair re-audit drops obsolete context and handles new rows",
          drift_plan_trace == [
              (None, False, ("iv", "hvol")),
              ("CTX", True, ("iv", "hvol")),
          ]
          and ("1d-iv", "2026-05-01") not in drift_reconciled
          and drift_reconciled == [
              ("1d-iv", "2026-05-02"),
              ("1d-hvol", "2026-05-03"),
          ]
          and drift["reconcile_candidate_tickers"] == 1
          and drift["reconcile_planned_tickers"] == 1
          and drift["reconcile_selected"] == 2
          and drift["reconcile_completed"] == 2
          and drift["reconcile_settled"] == 1
          and drift["reconcile_corrected"] == 1
          and drift["reconcile_selection_unknown"] == 0)

    failed_replan_calls = []

    def failed_replan(_root, *, ticker, kinds):
        del kinds
        if ticker is None:
            return [dict(obsolete_row)]
        raise RuntimeError("focused audit incomplete")

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        failed_current = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: Adapter(port, []),
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=lambda *_a, **_k: {"status": "ok"},
            series_fn=lambda _root: [], refresh_fn=lambda *_a, **_k: {},
            reconcile_plan_fn=failed_replan,
            reconcile_fn=lambda *_a, **_k: failed_replan_calls.append(True),
            reconcile_pacer_factory=object, run_id="failed-current-replan")
    finally:
        operation_gate.LOCK_PATH = old_path
    check("failed post-repair replan remains unknown ticker debt, never stale work",
          failed_replan_calls == []
          and failed_current["reconcile_candidate_tickers"] == 1
          and failed_current["reconcile_planned_tickers"] == 0
          and failed_current["reconcile_plan_failed"] == 1
          and failed_current["reconcile_selection_unknown"] == 1
          and failed_current["reconcile_selected"] == 0
          and failed_current["reconcile_queue_pending"] == 0
          and failed_current["unprocessed"] == [
              ("CTX", None, "reconcile-plan", "planning-failed")]
          and "focused audit incomplete"
          in (failed_current["reconcile_error"] or ""))
    check("legacy runs expose an inert reconciliation ledger without planning",
          result["reconcile_selected"] == 0
          and result["reconcile_completed"] == 0
          and result["reconcile_rows"] == []
          and result["reconcile_report"] is None)

    owner_rows = [
        {"ticker": "OWNER", "kind": "1d-iv", "kind_token": "1d-iv",
         "day": "2026-01-06"},
        {"ticker": "OWNER", "kind": "1d-hvol",
         "kind_token": "1d-hvol", "day": "2026-01-07"},
    ]
    owner_ports = []
    owner_pacers = []
    owner_guard = threading.Lock()
    owner_active = [0]
    owner_max_active = [0]

    def owned_reconcile(adapter, pacer, _root, row, _run_id, **_kwargs):
        adapter.assert_owner()
        with owner_guard:
            owner_active[0] += 1
            owner_max_active[0] = max(owner_max_active[0], owner_active[0])
            owner_ports.append(adapter.port)
        time.sleep(0.02)
        with owner_guard:
            owner_active[0] -= 1
        return {
            "status": "settled", "ticker": row["ticker"],
            "kind_token": row["kind_token"], "day": row["day"],
            "request_count": 1, "queue_resolved": True,
            "registry_committed": True,
        }

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        owned = pipeline.run(
            root="bank", ports=(2000, 3000),
            adapter_factory=lambda port: Adapter(port, []),
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=lambda *_a, **_k: {"status": "ok"},
            series_fn=lambda _root: [], refresh_fn=lambda *_a, **_k: {},
            reconcile_plan_fn=lambda _root, **_kwargs: list(owner_rows),
            reconcile_fn=owned_reconcile,
            reconcile_pacer_factory=lambda: (
                owner_pacers.append(object()) or owner_pacers[-1]))
    finally:
        operation_gate.LOCK_PATH = old_path
    check("two ports keep one ticker's reconcile rows under one owner",
          owned["reconcile_completed"] == 2
          and len(owner_ports) == 2 and len(set(owner_ports)) == 1
          and owner_max_active[0] == 1 and len(owner_pacers) == 1)

    malformed_rows = [
        {"ticker": "BAD", "kind": "1d-iv", "kind_token": "1d-iv",
         "day": "2026-01-08"},
        {"ticker": "BAD", "kind": "1d-hvol",
         "kind_token": "1d-hvol", "day": "2026-01-09"},
    ]
    malformed_reports = []

    def malformed_reconcile(adapter, _pacer, _root, row, _run_id,
                            **_kwargs):
        adapter.assert_owner()
        if row["day"] == "2026-01-08":
            return None
        return {
            "status": "settled", "ticker": row["ticker"],
            "kind_token": row["kind_token"], "day": row["day"],
            "queue_resolved": True,
        }

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        malformed = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: Adapter(port, []),
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=lambda *_a, **_k: {"status": "ok"},
            series_fn=lambda _root: [], refresh_fn=lambda *_a, **_k: {},
            reconcile_plan_fn=lambda _root, **_kwargs:
                list(malformed_rows),
            reconcile_fn=malformed_reconcile,
            reconcile_pacer_factory=object,
            reconcile_report_fn=lambda _root, _run, rows,
            **kwargs: malformed_reports.append(
                (list(rows), dict(kwargs))) or "malformed-report.json",
            run_id="malformed-reconcile")
    finally:
        operation_gate.LOCK_PATH = old_path
    check("malformed reconcile envelopes are ambiguous, never false success",
          malformed["reconcile_selected"] == 2
          and malformed["reconcile_completed"] == 2
          and malformed["reconcile_request_count"] == 0
          and malformed["reconcile_request_unknown"] == 2
          and malformed["reconcile_settled"] == 0
          and malformed["reconcile_corrected"] == 0
          and malformed["reconcile_unresolved"] == 2
          and malformed["reconcile_queue_resolved"] == 0
          and malformed["reconcile_queue_pending"] == 2
          and [row["status"] for row in malformed["reconcile_rows"]]
          == ["ambiguous", "ambiguous"]
          and all(row.get("request_count_unknown") is True
                  and row.get("queue_resolved") is False
                  for row in malformed["reconcile_rows"])
          and "non-object" in malformed["reconcile_rows"][0]["error"]
          and "request_count" in malformed["reconcile_rows"][1]["error"]
          and malformed_reports == [(
              malformed["reconcile_rows"], {
                  "requested_count": 2,
                  "summary": {
                      "completed_count": 2, "request_count": 0,
                      "request_count_unknown": 2,
                      "settled_count": 0, "corrected_count": 0,
                      "unresolved_count": 2,
                      "queue_resolved_count": 0,
                      "queue_pending_count": 2,
                      "rows_truncated": 0,
                  },
              })])
    hostile = pipeline._reconcile_result(malformed_rows[0], {
        "status": "settled", "ticker": "BAD", "kind_token": "1d-iv",
        "day": "2026-01-08", "request_count": 1,
        "queue_resolved": True, "registry_committed": True,
        "hostile": object(),
        "huge": "x" * 100_000, "non_finite_extra": float("nan"),
    })
    check("untrusted reconcile extras cannot poison the bounded artifact row",
          hostile["status"] == "settled"
          and hostile["queue_resolved"] is True
          and "hostile" not in hostile and "huge" not in hostile
          and "non_finite_extra" not in hostile)

    atomic_row = {
        "ticker": "ATOMIC", "kind": "1d-iv", "kind_token": "1d-iv",
        "day": "2026-01-10", "reason": "jump_up",
        "reasons": ["jump_up"],
    }
    rolled_back = pipeline._reconcile_result(
        atomic_row, atomic_reconcile_outcome(atomic_row, "rolled_back"))
    recovery_required = pipeline._reconcile_result(
        atomic_row,
        atomic_reconcile_outcome(atomic_row, "recovery_required"))
    indeterminate_recovery = pipeline._reconcile_result(
        atomic_row, atomic_reconcile_outcome(
            atomic_row, "recovery_required", indeterminate=True))
    split_recovery_raw = atomic_reconcile_outcome(
        atomic_row, "recovery_required")
    split_recovery_raw.update({
        "bank_written": False,
        "bank_committed": False,
        "correction_recorded": True,
    })
    split_recovery_raw["commit_evidence"].update({
        "bank_written": False,
        "correction_recorded": True,
        "month_state": "original",
        "manifest_state": "corrected",
        "observed_original_sha256": "a" * 64,
        "observed_written_sha256": "a" * 64,
    })
    split_recovery = pipeline._reconcile_result(
        atomic_row, split_recovery_raw)
    check("Fix Data preserves exact verified-rollback evidence and provenance",
          rolled_back["status"] == "unresolved"
          and rolled_back["commit_state"] == "rolled_back"
          and rolled_back["rollback_verified"] is True
          and rolled_back["bank_write_completed"] is False
          and rolled_back["bank_written"] is False
          and rolled_back["correction_recorded"] is False
          and rolled_back["bank_committed"] is False
          and rolled_back["reason"] == "jump_up"
          and rolled_back["reasons"] == ["jump_up"]
          and rolled_back["commit_evidence"]["month_state"] == "original"
          and "hostile_extra" not in rolled_back
          and "oversized_extra" not in rolled_back)
    check("Fix Data preserves recovery-required corrected-byte truth",
          recovery_required["status"] == "ambiguous"
          and recovery_required["commit_state"] == "recovery_required"
          and recovery_required["rollback_verified"] is False
          and recovery_required["bank_written"] is True
          and recovery_required["correction_recorded"] is False
          and recovery_required["bank_committed"] is True
          and recovery_required["commit_evidence"]["month_state"]
          == "corrected"
          and recovery_required["commit_evidence"]["manifest_state"]
          == "original")
    check("Fix Data preserves fully indeterminate recovery truth",
          indeterminate_recovery["status"] == "ambiguous"
          and indeterminate_recovery["bank_written"] is None
          and indeterminate_recovery["correction_recorded"] is None
          and "bank_committed" not in indeterminate_recovery
          and indeterminate_recovery["commit_evidence"]["month_state"]
          == "indeterminate")
    check("Fix Data preserves split month/manifest recovery truth",
          split_recovery["status"] == "ambiguous"
          and split_recovery["bank_written"] is False
          and split_recovery["correction_recorded"] is True
          and split_recovery["bank_committed"] is False
          and split_recovery["commit_evidence"]["month_state"] == "original"
          and split_recovery["commit_evidence"]["manifest_state"]
          == "corrected")

    atomic_pipeline_rows = [
        dict(atomic_row),
        dict(atomic_row, day="2026-01-11"),
    ]
    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        atomic_pipeline = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: Adapter(port, []),
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=lambda *_a, **_k: {"status": "ok"},
            series_fn=lambda _root: [], refresh_fn=lambda *_a, **_k: {},
            reconcile_plan_fn=lambda _root, **_kwargs:
                list(atomic_pipeline_rows),
            reconcile_fn=lambda _adapter, _pacer, _root, row, _run, **_kwargs:
                atomic_reconcile_outcome(
                    row, "rolled_back" if row["day"] == "2026-01-10"
                    else "recovery_required"),
            run_id="atomic-coordinator-offline")
    finally:
        operation_gate.LOCK_PATH = old_path
    check("Fix Data coordinator retains atomic states in its durable row ledger",
          atomic_pipeline["reconcile_completed"] == 2
          and atomic_pipeline["reconcile_request_count"] == 2
          and atomic_pipeline["reconcile_request_unknown"] == 0
          and atomic_pipeline["reconcile_unresolved"] == 2
          and atomic_pipeline["reconcile_queue_pending"] == 2
          and [row["commit_state"]
               for row in atomic_pipeline["reconcile_rows"]]
          == ["rolled_back", "recovery_required"])

    registry_committed = pipeline._reconcile_result(atomic_row, {
        "status": "settled", "ticker": "ATOMIC",
        "kind_token": "1d-iv", "day": "2026-01-10",
        "request_count": 1, "queue_resolved": False,
        "registry_committed": True,
        "reason": "jump_up", "reasons": ["jump_up"],
    })
    check("Fix Data keeps durable settlement separate from queue retirement",
          registry_committed["status"] == "settled"
          and registry_committed["registry_committed"] is True
          and registry_committed["queue_resolved"] is False)
    ordinary_correction = pipeline._reconcile_result(atomic_row, {
        "status": "corrected", "ticker": "ATOMIC",
        "kind_token": "1d-iv", "day": "2026-01-10",
        "request_count": 1, "queue_resolved": False,
        "bank_committed": True,
        "stored_value": None, "served_value": 0.3,
        "reason": "hard_nonfinite", "reasons": ["hard_nonfinite"],
    })
    check("hard-nonfinite correction keeps None stored value and durable proof",
          ordinary_correction["status"] == "corrected"
          and ordinary_correction["bank_committed"] is True
          and ordinary_correction["queue_resolved"] is False
          and ordinary_correction["stored_value"] is None
          and ordinary_correction["served_value"] == 0.3
          and "commit_state" not in ordinary_correction)

    missing_settlement_proof = pipeline._reconcile_result(atomic_row, {
        "status": "settled", "ticker": "ATOMIC",
        "kind_token": "1d-iv", "day": "2026-01-10",
        "request_count": 1, "queue_resolved": True,
    })
    contradictory_correction_proof = pipeline._reconcile_result(atomic_row, {
        "status": "corrected", "ticker": "ATOMIC",
        "kind_token": "1d-iv", "day": "2026-01-10",
        "request_count": 1, "queue_resolved": True,
        "bank_committed": False,
    })
    check("Fix Data rejects status text or queue retirement without proof",
          missing_settlement_proof["status"] == "ambiguous"
          and contradictory_correction_proof["status"] == "ambiguous"
          and missing_settlement_proof["request_count_unknown"] is True
          and contradictory_correction_proof["request_count_unknown"] is True
          and "committed registry" in missing_settlement_proof["error"]
          and "committed bank" in contradictory_correction_proof["error"])

    malformed_atomic = atomic_reconcile_outcome(
        atomic_row, "recovery_required")
    malformed_atomic["commit_evidence"]["hostile"] = object()
    malformed_atomic = pipeline._reconcile_result(
        atomic_row, malformed_atomic)
    oversized_atomic = atomic_reconcile_outcome(
        atomic_row, "recovery_required")
    oversized_atomic["commit_evidence"]["restore_errors"] = ["x" * 321]
    oversized_atomic = pipeline._reconcile_result(
        atomic_row, oversized_atomic)
    check("malformed or oversized atomic evidence fails closed and stays bounded",
          malformed_atomic["status"] == oversized_atomic["status"]
          == "ambiguous"
          and malformed_atomic["request_count_unknown"] is True
          and oversized_atomic["request_count_unknown"] is True
          and "commit_evidence" not in malformed_atomic
          and "commit_evidence" not in oversized_atomic
          and "fields" in malformed_atomic["error"]
          and "restore error" in oversized_atomic["error"])

    truncated_rows = [
        {"ticker": "CAP", "kind": "1d-iv", "kind_token": "1d-iv",
         "day": f"2026-02-0{index}"}
        for index in range(1, 4)
    ]
    truncated_reports = []

    def truncated_reconcile(adapter, _pacer, _root, row, _run_id,
                            **_kwargs):
        adapter.assert_owner()
        status = {
            "2026-02-01": "settled",
            "2026-02-02": "corrected",
            "2026-02-03": "unresolved",
        }[row["day"]]
        return {
            "status": status, "ticker": row["ticker"],
            "kind_token": row["kind_token"], "day": row["day"],
            "request_count": int(status != "unresolved"),
            "queue_resolved": status != "unresolved",
            **({"registry_committed": True} if status == "settled"
               else {"bank_committed": True} if status == "corrected"
               else {}),
        }

    old_result_limit = pipeline.MAX_RECONCILE_RESULTS
    old_path = operation_gate.LOCK_PATH
    pipeline.MAX_RECONCILE_RESULTS = 1
    operation_gate.LOCK_PATH = lock
    try:
        truncated = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: Adapter(port, []),
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=lambda *_a, **_k: {"status": "ok"},
            series_fn=lambda _root: [], refresh_fn=lambda *_a, **_k: {},
            reconcile_plan_fn=lambda _root, **_kwargs:
                list(truncated_rows),
            reconcile_fn=truncated_reconcile,
            reconcile_pacer_factory=object,
            reconcile_report_fn=lambda _root, _run, rows,
            **kwargs: truncated_reports.append(
                (list(rows), dict(kwargs))) or "truncated-report.json",
            run_id="truncated-reconcile")
    finally:
        operation_gate.LOCK_PATH = old_path
        pipeline.MAX_RECONCILE_RESULTS = old_result_limit
    check("reconcile evidence truncation preserves every exact total",
          truncated["reconcile_selected"] == 3
          and truncated["reconcile_completed"] == 3
          and truncated["reconcile_request_count"] == 2
          and truncated["reconcile_request_unknown"] == 0
          and truncated["reconcile_settled"] == 1
          and truncated["reconcile_corrected"] == 1
          and truncated["reconcile_unresolved"] == 1
          and truncated["reconcile_queue_resolved"] == 2
          and truncated["reconcile_queue_pending"] == 1
          and len(truncated["reconcile_rows"]) == 1
          and truncated["reconcile_rows"][0]["day"] == "2026-02-01"
          and truncated["reconcile_rows_truncated"] == 2
          and truncated_reports == [(
              truncated["reconcile_rows"], {
                  "requested_count": 3,
                  "summary": {
                      "completed_count": 3, "request_count": 2,
                      "request_count_unknown": 0,
                      "settled_count": 1, "corrected_count": 1,
                      "unresolved_count": 1,
                      "queue_resolved_count": 2,
                      "queue_pending_count": 1,
                      "rows_truncated": 2,
                  },
              })])

    oversized_trace = []

    def oversized_plan(_root, **_kwargs):
        for index in range(5):
            oversized_trace.append(("plan-row", index))
            yield {
                "ticker": f"BIG{index}", "kind_token": "1d-iv",
                "day": f"2026-03-0{index + 1}",
            }

    def oversized_adapter(port):
        oversized_trace.append(("adapter", port))
        return Adapter(port, oversized_trace)

    old_item_limit = pipeline.MAX_RECONCILE_ITEMS
    old_path = operation_gate.LOCK_PATH
    pipeline.MAX_RECONCILE_ITEMS = 2
    operation_gate.LOCK_PATH = lock
    try:
        oversized = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=oversized_adapter,
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=lambda *_a, **_k: {"status": "ok"},
            series_fn=lambda _root: [], refresh_fn=lambda *_a, **_k: {},
            reconcile_plan_fn=oversized_plan,
            reconcile_fn=lambda *_a, **_k:
                oversized_trace.append(("reconcile",)) or {},
            reconcile_pacer_factory=object,
            reconcile_report_fn=lambda *_a, **_k:
                oversized_trace.append(("report",)) or "unexpected.json",
            run_id="oversized-plan")
    finally:
        operation_gate.LOCK_PATH = old_path
        pipeline.MAX_RECONCILE_ITEMS = old_item_limit
    oversized_adapter_index = next(
        (index for index, row in enumerate(oversized_trace)
         if row[0] == "adapter"), len(oversized_trace))
    check("oversized reconcile plan stops at MAX plus one before adapter work",
          [row for row in oversized_trace if row[0] == "plan-row"]
          == [("plan-row", 0), ("plan-row", 1), ("plan-row", 2)]
          and oversized_adapter_index >= 3
          and not any(row[0] in ("reconcile", "report")
                      for row in oversized_trace)
          and "bounded item limit" in (oversized["reconcile_error"] or "")
          and oversized["reconcile_selected"] == 0
          and oversized["reconcile_completed"] == 0)

    two_port_evidence = []
    for cycle in range(3):
        concurrent_rows = [
            {"ticker": f"P{index}", "kind_token": "1d-iv",
             "day": f"2026-04-0{index + 1}"}
            for index in range(4)
        ]
        first_wave = threading.Barrier(2)
        concurrent_started = []
        concurrent_guard = threading.Lock()
        concurrent_events = []

        def concurrent_reconcile(adapter, _pacer, _root, row, _run_id,
                                 **_kwargs):
            adapter.assert_owner()
            with concurrent_guard:
                concurrent_started.append((row["ticker"], adapter.port))
            if row["ticker"] in {"P0", "P1"}:
                first_wave.wait(2)
            return {
                "status": "settled", "ticker": row["ticker"],
                "kind_token": row["kind_token"], "day": row["day"],
                "request_count": 1, "queue_resolved": True,
                "registry_committed": True,
            }

        old_path = operation_gate.LOCK_PATH
        operation_gate.LOCK_PATH = lock
        try:
            concurrent = pipeline.run(
                root="bank", ports=(2000, 3000),
                adapter_factory=lambda port: Adapter(port, []),
                scan_fn=lambda *_a, **_k: {"summary": {}},
                health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
                audit_fn=lambda *_a, **_k: {"status": "ok"},
                series_fn=lambda _root: [],
                refresh_fn=lambda *_a, **_k: {},
                progress=concurrent_events.append,
                reconcile_plan_fn=lambda _root, ticker, rows=concurrent_rows,
                **_kwargs: [dict(row) for row in rows
                            if ticker is None or row["ticker"] == ticker],
                reconcile_fn=concurrent_reconcile,
                reconcile_pacer_factory=object,
                run_id=f"two-port-progress-{cycle}")
        finally:
            operation_gate.LOCK_PATH = old_path
        concurrent_done = [
            event for event in concurrent_events
            if event.get("type") == "reconcile" and event.get("done")]
        two_port_evidence.append((
            concurrent, concurrent_started, concurrent_done,
            {(row["ticker"], row["kind_token"], row["day"])
             for row in concurrent_rows},
        ))
    check("two-port reconcile progress is monotonic and exactly once",
          all(
              result["reconcile_selected"] == 4
              and result["reconcile_completed"] == 4
              and result["reconcile_settled"] == 4
              and result["reconcile_queue_resolved"] == 4
              and {port for _ticker, port in started} == {2000, 3000}
              and [event["done"] for event in done] == [1, 2, 3, 4]
              and all(event["done"] <= event["total"] <= 4
                      for event in done)
              and [event["total"] for event in done]
              == sorted(event["total"] for event in done)
              and len({(event["ticker"], event["interval"], event["day"])
                       for event in done}) == 4
              and {(event["ticker"], event["interval"], event["day"])
                   for event in done} == expected
              for result, started, done, expected in two_port_evidence))

    probe_trace = []
    probe_lock = threading.Lock()
    probe_release = threading.Event()
    audit_release = threading.Event()
    active_probes = [0]
    max_probes = [0]
    max_probes_while_checking = [0]
    audits_done = [0]
    reports = []

    def capped_probe(adapter, _root, ticker, _selection):
        adapter.assert_owner()
        with probe_lock:
            active_probes[0] += 1
            max_probes[0] = max(max_probes[0], active_probes[0])
            if audits_done[0] < 4:
                max_probes_while_checking[0] = max(
                    max_probes_while_checking[0], active_probes[0])
            probe_trace.append(("probe-start", ticker, adapter.port))
        audit_release.set()
        probe_release.wait(0.2)
        with probe_lock:
            active_probes[0] -= 1
            probe_trace.append(("probe-end", ticker, adapter.port))
        return {"ticker": ticker, "verdict": (
                    "PROBE_ERROR" if ticker == "DDD" else "MATCH"),
                "pass_equivalent": ticker != "DDD", "request_count": 1,
                "reused_request_count": 0}

    def probe_audit(adapter, _root, ticker, interval, _cache):
        adapter.assert_owner()
        if ticker != "AAA":
            audit_release.wait(2)
        with probe_lock:
            probe_trace.append(("audit", ticker))
            audits_done[0] += 1
        return {"status": "ok"}

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        probed = pipeline.run(
            root="bank", ports=(2000, 3000, 4000, 5000),
            adapter_factory=lambda port: Adapter(port, probe_trace),
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=probe_audit,
            series_fn=lambda _root: [(ticker, "1m") for ticker in
                                     ("AAA", "BBB", "CCC", "DDD")],
            refresh_fn=lambda *_a, **_k: {},
            probe_plan_fn=lambda _root, tickers, seed: {
                "seed": seed, "items": {ticker: {"day": "2026-01-02"}
                                          for ticker in tickers}},
            probe_fn=capped_probe,
            probe_report_fn=lambda _root, run_id, seed, rows:
                reports.append((run_id, seed, list(rows))) or "report.json",
            probe_seed=11, run_id="probe-cap")
    finally:
        operation_gate.LOCK_PATH = old_path
        probe_release.set()
        audit_release.set()
    check("each embedded probe follows its ticker accuracy boundary",
          all(next(i for i, row in enumerate(probe_trace)
                   if row[:2] == ("audit", ticker))
              < next(i for i, row in enumerate(probe_trace)
                     if row[:2] == ("probe-start", ticker))
              for ticker in ("AAA", "BBB", "CCC", "DDD")))
    check("probe concurrency is capped at two while checks are active",
          1 <= max_probes_while_checking[0] <= 2 and max_probes[0] >= 1)
    check("probe rows aggregate once without changing repair/check success",
          probed["checked"] == 4 and probed["probe_completed"] == 4
          and probed["probe_request_count"] == 4
          and probed["probe_issues"] == 1
          and probed["probe_report"] == "report.json"
          and len(reports) == 1 and len(reports[0][2]) == 4)

    heartbeat_started = threading.Event()
    heartbeat_release = threading.Event()
    heartbeat_capture_lock = threading.Lock()
    heartbeat_events = []
    heartbeat_result = []

    def heartbeat_progress(event):
        with heartbeat_capture_lock:
            heartbeat_events.append(dict(event))

    def heartbeat_probe(adapter, _root, ticker, _selection):
        adapter.assert_owner()
        heartbeat_started.set()
        heartbeat_release.wait(3)
        return {"ticker": ticker, "verdict": "MATCH",
                "pass_equivalent": True, "request_count": 1,
                "reused_request_count": 0}

    def heartbeat_run():
        heartbeat_result.append(pipeline.run(
            root="bank", ports=(2000, 3000),
            adapter_factory=lambda port: Adapter(port, []),
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=lambda *_a, **_k: {"status": "ok"},
            series_fn=lambda _root: [("HEART", "1m")],
            refresh_fn=lambda *_a, **_k: {},
            probe_plan_fn=lambda _root, tickers, seed: {
                "seed": seed,
                "items": {ticker: {"day": "2026-01-02"}
                          for ticker in tickers}},
            probe_fn=heartbeat_probe, probe_seed=45,
            run_id="probe-heartbeat", progress=heartbeat_progress))

    old_interval = pipeline.PROBE_WAIT_HEARTBEAT_S
    old_path = operation_gate.LOCK_PATH
    pipeline.PROBE_WAIT_HEARTBEAT_S = 0.05
    operation_gate.LOCK_PATH = lock
    heartbeat_thread = threading.Thread(target=heartbeat_run)
    reached = False
    stall = []
    try:
        heartbeat_thread.start()
        reached = heartbeat_started.wait(3)
        deadline = time.monotonic() + 2
        while reached and time.monotonic() < deadline:
            with heartbeat_capture_lock:
                port_events = [event for event in heartbeat_events
                               if event.get("type") == "ports"]
            if any(
                    any(str(value).startswith("waiting:")
                        for value in (event.get("states") or {}).values())
                    and any(str(value).startswith("spot-probe HEART")
                            for value in (event.get("states") or {}).values())
                    for event in port_events):
                break
            time.sleep(0.01)
        with heartbeat_capture_lock:
            mark = len(heartbeat_events)
        time.sleep(1.1)
        with heartbeat_capture_lock:
            stall = list(heartbeat_events[mark:])
    finally:
        heartbeat_release.set()
        heartbeat_thread.join(5)
        pipeline.PROBE_WAIT_HEARTBEAT_S = old_interval
        operation_gate.LOCK_PATH = old_path

    stall_ports = [event for event in stall if event.get("type") == "ports"]
    stall_labels = [str(value) for event in stall_ports
                    for value in (event.get("states") or {}).values()]
    check("wait status names live probe counts from the existing ledger",
          reached
          and any(label.startswith("waiting:") and "0/1" in label
                  for label in stall_labels)
          and any(label.startswith("spot-probe HEART (0/1)")
                  for label in stall_labels))
    check("wait/check/probe heartbeat is repeated and elapsed grows",
          len(stall_ports) >= 4
          and any("· 1s" in label for label in stall_labels))
    with heartbeat_capture_lock:
        final_ports = [event for event in heartbeat_events
                       if event.get("type") == "ports"]
        settled_count = len(final_ports)
    time.sleep(0.15)
    with heartbeat_capture_lock:
        after_settle_count = len([event for event in heartbeat_events
                                  if event.get("type") == "ports"])
    check("heartbeat monitor stops at the final all-done boundary",
          heartbeat_result and heartbeat_result[0]["probe_completed"] == 1
          and not heartbeat_thread.is_alive()
          and final_ports
          and all(value == "done" for value in
                  final_ports[-1].get("states", {}).values())
          and after_settle_count == settled_count)

    solo_check_started = threading.Event()
    solo_check_release = threading.Event()
    solo_events_lock = threading.Lock()
    solo_events = []
    solo_result = []

    def solo_progress(event):
        with solo_events_lock:
            solo_events.append(dict(event))

    def solo_audit(adapter, _root, _ticker, _interval, _cache_root):
        adapter.assert_owner()
        solo_check_started.set()
        solo_check_release.wait(3)
        return {"status": "ok"}

    def solo_run():
        solo_result.append(pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: Adapter(port, []),
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=solo_audit,
            series_fn=lambda _root: [("SOLO", "1m")],
            refresh_fn=lambda *_a, **_k: {},
            probe_plan_fn=lambda *_a, **_k: {"items": {}},
            probe_fn=lambda *_a, **_k: {}, run_id="check-heartbeat",
            progress=solo_progress))

    old_interval = pipeline.PROBE_WAIT_HEARTBEAT_S
    old_path = operation_gate.LOCK_PATH
    pipeline.PROBE_WAIT_HEARTBEAT_S = 0.05
    operation_gate.LOCK_PATH = lock
    solo_thread = threading.Thread(target=solo_run)
    solo_reached = False
    try:
        solo_thread.start()
        solo_reached = solo_check_started.wait(3)
        time.sleep(1.1)
    finally:
        solo_check_release.set()
        solo_thread.join(5)
        pipeline.PROBE_WAIT_HEARTBEAT_S = old_interval
        operation_gate.LOCK_PATH = old_path

    with solo_events_lock:
        solo_ports = [event for event in solo_events
                      if event.get("type") == "ports"]
    solo_labels = [str(value) for event in solo_ports
                   for value in (event.get("states") or {}).values()]
    check("one active port emits repeated blocked-check heartbeats",
          solo_reached
          and sum(label.startswith("cross-check SOLO 1m")
                  for label in solo_labels) >= 2
          and any(label.startswith("cross-check SOLO 1m") and "· 1s" in label
                  for label in solo_labels))
    check("one-port check heartbeat settles at done",
          solo_result and solo_result[0]["checked"] == 1
          and not solo_thread.is_alive() and solo_ports
          and solo_ports[-1].get("states") == {2000: "done"})

    transition_tickers = [f"COUNT{i}" for i in range(5)]
    transition_started = {ticker: threading.Event()
                          for ticker in transition_tickers}
    transition_release = {ticker: threading.Event()
                          for ticker in transition_tickers}
    transition_probe_started = {ticker: threading.Event()
                                for ticker in transition_tickers}
    transition_probe_release = threading.Event()
    transition_events_lock = threading.Lock()
    transition_events = []
    transition_result = []

    def transition_progress(event):
        with transition_events_lock:
            transition_events.append(dict(event))

    def transition_audit(adapter, _root, ticker, _interval, _cache_root):
        adapter.assert_owner()
        transition_started[ticker].set()
        transition_release[ticker].wait(5)
        return {"status": "ok"}

    def transition_probe(adapter, _root, ticker, _selection):
        adapter.assert_owner()
        transition_probe_started[ticker].set()
        transition_probe_release.wait(5)
        return {"ticker": ticker, "verdict": "MATCH",
                "pass_equivalent": True, "request_count": 1,
                "reused_request_count": 0}

    def transition_run():
        transition_result.append(pipeline.run(
            root="bank", ports=(2000, 3000, 4000, 5000, 6000),
            adapter_factory=lambda port: Adapter(port, []),
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=transition_audit,
            series_fn=lambda _root: [(ticker, "1m")
                                     for ticker in transition_tickers],
            refresh_fn=lambda *_a, **_k: {},
            probe_plan_fn=lambda _root, tickers, seed: {
                "seed": seed,
                "items": {ticker: {"day": "2026-01-02"}
                          for ticker in tickers}},
            probe_fn=transition_probe, probe_seed=48,
            run_id="wait-count-transition", progress=transition_progress))

    old_interval = pipeline.PROBE_WAIT_HEARTBEAT_S
    old_path = operation_gate.LOCK_PATH
    pipeline.PROBE_WAIT_HEARTBEAT_S = 0.05
    operation_gate.LOCK_PATH = lock
    transition_thread = threading.Thread(target=transition_run)
    all_checks_active = False
    first_wait = second_wait = None
    waiting_port = None
    try:
        transition_thread.start()
        all_started = all(event.wait(4)
                          for event in transition_started.values())
        deadline = time.monotonic() + 2
        active_snapshots = []
        while all_started and time.monotonic() < deadline:
            with transition_events_lock:
                port_events = [event for event in transition_events
                               if event.get("type") == "ports"]
            active_snapshots = [
                event for event in port_events
                if len(event.get("states") or {}) == 5
                and all(str(value).startswith("cross-check COUNT")
                        for value in event["states"].values())]
            if len(active_snapshots) >= 2:
                break
            time.sleep(0.01)
        all_checks_active = bool(all_started and len(active_snapshots) >= 2)

        for ticker in transition_tickers[:2]:
            transition_release[ticker].set()
        probes_started = all(transition_probe_started[ticker].wait(3)
                             for ticker in transition_tickers[:2])
        transition_release[transition_tickers[2]].set()
        deadline = time.monotonic() + 3
        while probes_started and time.monotonic() < deadline:
            with transition_events_lock:
                port_events = [event for event in transition_events
                               if event.get("type") == "ports"]
            for event in port_events:
                for port, value in (event.get("states") or {}).items():
                    label = str(value)
                    if (label.startswith("waiting:")
                            and "2 probes running" in label
                            and "2 checks running" in label
                            and "1 probe queued" in label):
                        waiting_port = port
                        first_wait = label
                        break
                if waiting_port is not None:
                    break
            if waiting_port is not None:
                break
            time.sleep(0.01)

        transition_release[transition_tickers[3]].set()
        deadline = time.monotonic() + 3
        while waiting_port is not None and time.monotonic() < deadline:
            with transition_events_lock:
                port_events = [event for event in transition_events
                               if event.get("type") == "ports"]
            labels = [str((event.get("states") or {}).get(waiting_port, ""))
                      for event in port_events]
            second_wait = next((label for label in reversed(labels)
                                if label.startswith("waiting:")
                                and "2 probes running" in label
                                and "1 check running" in label
                                and "2 probes queued" in label), None)
            if second_wait is not None:
                break
            time.sleep(0.01)
    finally:
        for event in transition_release.values():
            event.set()
        transition_probe_release.set()
        transition_thread.join(8)
        pipeline.PROBE_WAIT_HEARTBEAT_S = old_interval
        operation_gate.LOCK_PATH = old_path

    check("all-active check workers heartbeat without a waiting port",
          all_checks_active)
    check("one waiting port republishes truthful live-count transitions",
          waiting_port is not None and first_wait is not None
          and second_wait is not None and first_wait != second_wait)
    check("count-transition fixture drains every check and probe once",
          transition_result
          and transition_result[0]["checked"] == 5
          and transition_result[0]["probe_completed"] == 5
          and not transition_thread.is_alive())

    death_calls = []
    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        death = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: Adapter(port, death_calls),
            scan_fn=lambda *_a, **_k: {"summary": {
                "AAA 1m": {"missing_day_list": ["2026-01-02"]}}},
            health_fn=lambda _root: {},
            fill_fn=lambda *_a, **_k: (_ for _ in ()).throw(
                SystemExit("worker died")),
            audit_fn=lambda *_a: death_calls.append(("audit",)) or
                {"status": "ok"},
            series_fn=lambda _root: [("AAA", "1m")],
            refresh_fn=lambda *_a, **_k: {}, run_id="death",
            reconcile_plan_fn=lambda *_a, **_k: [{
                "ticker": "AAA", "kind_token": "1d-iv",
                "day": "2026-01-03"}],
            reconcile_fn=lambda *_a, **_k:
                death_calls.append(("reconcile",)) or {},
            reconcile_pacer_factory=object,
            probe_plan_fn=lambda _root, _tickers, seed: {
                "seed": seed, "items": {"AAA": {"day": "2026-01-04"}}},
            probe_fn=lambda *_a, **_k:
                death_calls.append(("probe",)) or {})
    finally:
        operation_gate.LOCK_PATH = old_path
    check("ambiguous in-flight worker death is reported and never retried",
          death["unprocessed"] == [
              ("AAA", "1m", "repair", "ambiguous"),
              ("AAA", None, "reconcile-plan",
               "worker-failed-unstarted"),
              ("AAA", "1m", "check", "worker-failed-unstarted"),
              ("AAA", None, "probe", "worker-failed-unstarted"),
          ]
          and death["halted"] == 1
          and len(death["port_errors"]) == 1
          and not any(row[0] in ("audit", "reconcile", "probe")
                      for row in death_calls))

    calls = []
    lease = operation_gate.acquire("external_sweep", path=lock)
    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        blocked = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda _port: calls.append("adapter"),
            scan_fn=lambda *_a, **_k: calls.append("scan"),
            health_fn=lambda _r: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=lambda *_a, **_k: {}, series_fn=lambda _r: [],
            refresh_fn=lambda *_a, **_k: {}, run_id="blocked")
    finally:
        operation_gate.LOCK_PATH = old_path
        lease.release()
    check("busy gate exits before adapters, scans, or writes",
          blocked["error"] and calls == [])

    pause = threading.Event()
    cancel = threading.Event()
    fill_started = threading.Event()
    fill_release = threading.Event()
    paused = threading.Event()
    pause_events = []
    pause_trace = []
    pause_result = []

    def pause_progress(event):
        pause_events.append(event)
        if event.get("type") == "substate" and event.get("state") == "paused":
            paused.set()

    def slow_fill(adapter, *_args, **_kwargs):
        adapter.assert_owner()
        fill_started.set()
        fill_release.wait(2)
        pause_trace.append(("fill-finished", adapter.port))
        return {"added": 1}

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        thread = threading.Thread(target=lambda: pause_result.append(pipeline.run(
            root="bank", ports=(2000, 3000),
            adapter_factory=lambda port: Adapter(port, pause_trace),
            scan_fn=lambda *_a, **_k: {"summary": {
                "AAA 1m": {"missing_day_list": ["2026-01-02"]}}},
            health_fn=lambda _root: {}, fill_fn=slow_fill,
            audit_fn=lambda *_a: {"status": "ok"},
            series_fn=lambda _root: [("AAA", "1m")],
            refresh_fn=lambda *_a, **_k: pause_trace.append(("refresh",)) or {},
            progress=pause_progress, pause_event=pause, cancel_event=cancel,
            run_id="pause-test")))
        thread.start()
        check("pause test reaches an in-flight fill", fill_started.wait(2))
        pause.set()
        check("pause is not settled while one worker remains in a fill",
              not paused.wait(0.1))
        fill_release.set()
        check("pause settles after every worker reaches paused/done",
              paused.wait(2))
        cancel.set()
        pause.clear()
        thread.join(2)
    finally:
        operation_gate.LOCK_PATH = old_path
        fill_release.set()
    check("cancel is honored only after settled pause and still refreshes",
          pause_result and pause_result[0]["cancelled"]
          and any(row[0] == "refresh" for row in pause_trace)
          and not thread.is_alive())

    # Production scan_all_gaps catches ordinary Exception subclasses around
    # progress callbacks. The private scan-cancel sentinel must still escape.
    scan_pause = threading.Event()
    scan_cancel = threading.Event()
    scan_paused = threading.Event()
    scan_calls = []
    scan_health = []
    scan_connects = []
    scan_result = []

    def swallowing_scan(_root, write=True, progress=None, kinds=("",),
                        preserve_existing=False):
        scan_calls.append(("scope", kinds, preserve_existing))
        for index in range(6):
            scan_calls.append(index)
            try:
                progress(index, 6, f"T{index}", "1m")
            except Exception:
                pass
        return {"summary": {}, "series_scanned": len(scan_calls)}

    def scan_cancel_progress(event):
        if event.get("type") == "scan" and event.get("done") == 2:
            scan_pause.set()
        if (event.get("type") == "substate"
                and event.get("state") == "paused"):
            scan_paused.set()

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        scan_thread = threading.Thread(target=lambda: scan_result.append(
            pipeline.run(
                root="bank", ports=(2000,),
                adapter_factory=lambda port: scan_connects.append(port),
                scan_fn=swallowing_scan,
                health_fn=lambda _root: scan_health.append(True) or {},
                fill_fn=lambda *_a, **_k: {},
                audit_fn=lambda *_a, **_k: {"status": "ok"},
                series_fn=lambda _root: [],
                refresh_fn=lambda *_a, **_k: {},
                progress=scan_cancel_progress,
                pause_event=scan_pause, cancel_event=scan_cancel,
                run_id="scan-cancel")))
        scan_thread.start()
        scan_hold = scan_paused.wait(2)
        scan_cancel.set()
        scan_pause.clear()
        scan_thread.join(2)
    finally:
        operation_gate.LOCK_PATH = old_path
        scan_cancel.set()
        scan_pause.clear()
    check("scan cancel reaches a settled per-series hold", scan_hold)
    check("scan cancel escapes an Exception-swallowing production callback",
          scan_result and scan_result[0]["cancelled"]
          and scan_result[0]["error"] is None
          and scan_calls == [("scope", ("",), True), 0, 1]
          and not scan_health and not scan_connects
          and not scan_thread.is_alive())

    # Pin exact per-item debt when cancellation retires a claimed repair row.
    unit_pause = threading.Event()
    unit_cancel = threading.Event()
    unit_paused = threading.Event()
    unit_trace = []
    unit_refresh = []
    unit_result = []

    def unit_progress(event):
        if event.get("type") == "refetch" and event.get("done") == 1:
            unit_pause.set()
        if (event.get("type") == "substate"
                and event.get("state") == "paused"):
            unit_paused.set()

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        unit_thread = threading.Thread(target=lambda: unit_result.append(
            pipeline.run(
                root="bank", ports=(2000,),
                adapter_factory=lambda port: Adapter(port, unit_trace),
                scan_fn=lambda *_a, **_k: {"summary": {
                    "AAA 1m": {"missing_day_list": ["2026-01-02"]},
                    "AAA 5m": {"missing_day_list": ["2026-01-02"]},
                    "AAA 15m": {"missing_day_list": ["2026-01-02"]}}},
                health_fn=lambda _root: {},
                fill_fn=lambda adapter, _root, ticker, interval, _days,
                **_kwargs: (adapter.assert_owner(),
                            unit_trace.append(("fill", ticker, interval)),
                            {"added": 1})[-1],
                audit_fn=lambda adapter, _root, ticker, interval, _cache:
                    (adapter.assert_owner(),
                     unit_trace.append(("audit", ticker, interval)),
                     {"status": "ok"})[-1],
                series_fn=lambda _root: [("AAA", "1m"), ("AAA", "5m")],
                refresh_fn=lambda _root, touched, **_kwargs:
                    unit_refresh.append(list(touched)) or {},
                progress=unit_progress, pause_event=unit_pause,
                cancel_event=unit_cancel, run_id="unit-cancel",
                probe_plan_fn=lambda _root, _tickers, seed: {
                    "seed": seed, "items": {"AAA": {"day": "2026-01-02"}}},
                probe_fn=lambda *_a, **_k:
                    unit_trace.append(("probe",)) or {})))
        unit_thread.start()
        unit_hold = unit_paused.wait(2)
        unit_cancel.set()
        unit_pause.clear()
        unit_thread.join(2)
    finally:
        operation_gate.LOCK_PATH = old_path
        unit_cancel.set()
        unit_pause.clear()
    expected_unit_debt = [
        ("AAA", "5m", "repair", "cancelled-unstarted"),
        ("AAA", "15m", "repair", "cancelled-unstarted"),
        ("AAA", "1m", "check", "cancelled-unstarted"),
        ("AAA", "5m", "check", "cancelled-unstarted"),
        ("AAA", None, "probe", "cancelled-unstarted"),
    ]
    check("mid-repair cancel reaches the unit boundary", unit_hold)
    check("mid-repair cancel runs only the completed series",
          [row for row in unit_trace if row[0] == "fill"]
          == [("fill", "AAA", "1m")]
          and not any(row[0] in ("audit", "probe") for row in unit_trace))
    check("mid-repair cancel records exact downstream per-item debt",
          unit_result and unit_result[0]["unprocessed"] == expected_unit_debt
          and unit_result[0]["halted"] == 0)
    check("mid-repair cancel preserves completed work and refresh",
          unit_result and unit_result[0]["cancelled"]
          and unit_result[0]["added"] == 1
          and unit_result[0]["error"] is None
          and not unit_result[0]["port_errors"]
          and unit_refresh == [[("AAA", "1m")]]
          and not unit_thread.is_alive())

    # Pin the matching check-chain path, including its deferred probe.
    check_pause = threading.Event()
    check_cancel = threading.Event()
    check_paused = threading.Event()
    check_trace = []
    check_result = []

    def check_progress(event):
        if event.get("type") == "xcheck" and event.get("done") == 1:
            check_pause.set()
        if (event.get("type") == "substate"
                and event.get("state") == "paused"):
            check_paused.set()

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        check_thread = threading.Thread(target=lambda: check_result.append(
            pipeline.run(
                root="bank", ports=(2000,),
                adapter_factory=lambda port: Adapter(port, check_trace),
                scan_fn=lambda *_a, **_k: {"summary": {}},
                health_fn=lambda _root: {},
                fill_fn=lambda *_a, **_k: {},
                audit_fn=lambda adapter, _root, ticker, interval, _cache:
                    (adapter.assert_owner(),
                     check_trace.append(("audit", ticker, interval)),
                     {"status": "ok"})[-1],
                series_fn=lambda _root: [("AAA", "1m"), ("AAA", "5m"),
                                         ("AAA", "15m")],
                refresh_fn=lambda *_a, **_k: {},
                progress=check_progress, pause_event=check_pause,
                cancel_event=check_cancel, run_id="check-cancel",
                probe_plan_fn=lambda _root, _tickers, seed: {
                    "seed": seed, "items": {"AAA": {"day": "2026-01-02"}}},
                probe_fn=lambda *_a, **_k:
                    check_trace.append(("probe",)) or {})))
        check_thread.start()
        check_hold = check_paused.wait(2)
        check_cancel.set()
        check_pause.clear()
        check_thread.join(2)
    finally:
        operation_gate.LOCK_PATH = old_path
        check_cancel.set()
        check_pause.clear()
    expected_check_debt = [
        ("AAA", "5m", "check", "cancelled-unstarted"),
        ("AAA", "15m", "check", "cancelled-unstarted"),
        ("AAA", None, "probe", "cancelled-unstarted"),
    ]
    check("mid-check cancel reaches the unit boundary", check_hold)
    check("mid-check cancel records exact remaining checks and probe",
          check_result
          and check_result[0]["unprocessed"] == expected_check_debt
          and check_result[0]["checked"] == 1
          and check_result[0]["halted"] == 0
          and check_result[0]["cancelled"]
          and check_result[0]["error"] is None
          and not check_result[0]["port_errors"]
          and check_trace.count(("audit", "AAA", "1m")) == 1
          and not any(row[0] == "probe" for row in check_trace)
          and not check_thread.is_alive())

    reconcile_pause = threading.Event()
    reconcile_cancel = threading.Event()
    reconcile_paused = threading.Event()
    reconcile_cancel_trace = []
    reconcile_cancel_result = []
    reconcile_cancel_rows = [
        {"ticker": "AAA", "kind": "1d-iv", "kind_token": "1d-iv",
         "day": "2026-02-02"},
        {"ticker": "AAA", "kind": "1d-iv", "kind_token": "1d-iv",
         "day": "2026-02-03"},
        {"ticker": "AAA", "kind": "1d-hvol",
         "kind_token": "1d-hvol", "day": "2026-02-04"},
    ]

    def reconcile_cancel_progress(event):
        if event.get("type") == "reconcile" and event.get("done") == 1:
            reconcile_pause.set()
        if (event.get("type") == "substate"
                and event.get("state") == "paused"):
            reconcile_paused.set()

    def reconcile_cancel_one(adapter, _pacer, _root, row, _run_id,
                             **_kwargs):
        adapter.assert_owner()
        reconcile_cancel_trace.append(
            ("reconcile", row["kind_token"], row["day"]))
        return {
            "status": "settled", "ticker": row["ticker"],
            "kind_token": row["kind_token"], "day": row["day"],
            "request_count": 1, "queue_resolved": True,
            "registry_committed": True,
        }

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        reconcile_cancel_thread = threading.Thread(
            target=lambda: reconcile_cancel_result.append(pipeline.run(
                root="bank", ports=(2000,),
                adapter_factory=lambda port: Adapter(
                    port, reconcile_cancel_trace),
                scan_fn=lambda *_a, **_k: {"summary": {}},
                health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
                audit_fn=lambda adapter, _root, ticker, interval, _cache:
                    (adapter.assert_owner(),
                     reconcile_cancel_trace.append(
                         ("audit", ticker, interval)),
                     {"status": "ok"})[-1],
                series_fn=lambda _root: [
                    ("AAA", "1m"), ("AAA", "5m")],
                refresh_fn=lambda *_a, **_k: {},
                progress=reconcile_cancel_progress,
                pause_event=reconcile_pause, cancel_event=reconcile_cancel,
                run_id="reconcile-cancel",
                reconcile_plan_fn=lambda _root, **_kwargs:
                    list(reconcile_cancel_rows),
                reconcile_fn=reconcile_cancel_one,
                reconcile_pacer_factory=object,
                reconcile_report_fn=lambda _root, _run, rows,
                **kwargs: {"rows": len(rows), **kwargs},
                probe_plan_fn=lambda _root, _tickers, seed: {
                    "seed": seed,
                    "items": {"AAA": {"day": "2026-02-05"}}},
                probe_fn=lambda *_a, **_k:
                    reconcile_cancel_trace.append(("probe",)) or {})),
            daemon=True)
        reconcile_cancel_thread.start()
        reconcile_hold = reconcile_paused.wait(2)
        reconcile_cancel.set()
        reconcile_pause.clear()
        reconcile_cancel_thread.join(2)
    finally:
        operation_gate.LOCK_PATH = old_path
        reconcile_cancel.set()
        reconcile_pause.clear()
    expected_reconcile_cancel_debt = [
        ("AAA", "1d-iv", "2026-02-03", "reconcile",
         "cancelled-unstarted"),
        ("AAA", "1d-hvol", "2026-02-04", "reconcile",
         "cancelled-unstarted"),
        ("AAA", "1m", "check", "cancelled-unstarted"),
        ("AAA", "5m", "check", "cancelled-unstarted"),
        ("AAA", None, "probe", "cancelled-unstarted"),
    ]
    check("mid-reconcile pause settles after exactly the row in hand",
          reconcile_hold
          and reconcile_cancel_trace[:1]
          == [("reconcile", "1d-iv", "2026-02-02")]
          and not any(row[0] in ("audit", "probe")
                      for row in reconcile_cancel_trace))
    check("mid-reconcile cancel records exact day/check/probe debt",
          reconcile_cancel_result
          and reconcile_cancel_result[0]["unprocessed"]
          == expected_reconcile_cancel_debt
          and reconcile_cancel_result[0]["cancelled"]
          and reconcile_cancel_result[0]["reconcile_selected"] == 3
          and reconcile_cancel_result[0]["reconcile_completed"] == 1
          and reconcile_cancel_result[0]["reconcile_settled"] == 1
          and reconcile_cancel_result[0]["reconcile_report"]
          == {
              "rows": 1, "requested_count": 3,
              "summary": {
                  "completed_count": 1, "request_count": 1,
                  "request_count_unknown": 0,
                  "settled_count": 1, "corrected_count": 0,
                  "unresolved_count": 0, "queue_resolved_count": 1,
                  "queue_pending_count": 2, "rows_truncated": 0,
              },
          }
          and not reconcile_cancel_thread.is_alive())

    reconcile_death_trace = []
    death_rows = [
        {"ticker": "DIE", "kind": "1d-iv", "kind_token": "1d-iv",
         "day": "2026-03-02"},
        {"ticker": "DIE", "kind": "1d-hvol",
         "kind_token": "1d-hvol", "day": "2026-03-03"},
    ]

    def die_reconcile(adapter, _pacer, _root, row, _run_id, **_kwargs):
        adapter.assert_owner()
        reconcile_death_trace.append(("reconcile", row["day"]))
        raise KeyboardInterrupt("synthetic reconcile worker death")

    old_path = operation_gate.LOCK_PATH
    operation_gate.LOCK_PATH = lock
    try:
        reconcile_death = pipeline.run(
            root="bank", ports=(2000,),
            adapter_factory=lambda port: Adapter(port, reconcile_death_trace),
            scan_fn=lambda *_a, **_k: {"summary": {}},
            health_fn=lambda _root: {}, fill_fn=lambda *_a, **_k: {},
            audit_fn=lambda adapter, _root, ticker, interval, _cache:
                (adapter.assert_owner(), {"status": "ok"})[-1],
            series_fn=lambda _root: [("DIE", "1m")],
            refresh_fn=lambda *_a, **_k: {},
            reconcile_plan_fn=lambda _root, **_kwargs: list(death_rows),
            reconcile_fn=die_reconcile,
            reconcile_pacer_factory=object,
            probe_plan_fn=lambda _root, _tickers, seed: {
                "seed": seed, "items": {"DIE": {"day": "2026-03-04"}}},
            probe_fn=lambda *_a, **_k: {})
    finally:
        operation_gate.LOCK_PATH = old_path
    expected_death_debt = [
        ("DIE", "1d-iv", "2026-03-02", "reconcile", "ambiguous"),
        ("DIE", "1d-hvol", "2026-03-03", "reconcile",
         "worker-failed-unstarted"),
        ("DIE", "1m", "check", "worker-failed-unstarted"),
        ("DIE", None, "probe", "worker-failed-unstarted"),
    ]
    check("reconcile worker death is exact, ambiguous, and never retried",
          reconcile_death["unprocessed"] == expected_death_debt
          and reconcile_death["reconcile_selected"] == 2
          and reconcile_death["reconcile_completed"] == 1
          and reconcile_death["reconcile_request_count"] == 0
          and reconcile_death["reconcile_request_unknown"] == 1
          and reconcile_death["reconcile_unresolved"] == 1
          and reconcile_death["reconcile_queue_pending"] == 2
          and len(reconcile_death["reconcile_rows"]) == 1
          and reconcile_death["reconcile_rows"][0]["status"] == "ambiguous"
          and reconcile_death["reconcile_rows"][0][
              "request_count_unknown"] is True
          and reconcile_death["halted"] == 1
          and len(reconcile_death["port_errors"]) == 1
          and reconcile_death_trace.count(
              ("reconcile", "2026-03-02")) == 1
          and ("reconcile", "2026-03-03") not in reconcile_death_trace)

print(f"fix-data pipeline: {sum(checks)}/{len(checks)} passed")
raise SystemExit(0 if all(checks) else 1)
