"""Offline checks for Add Stocks volatility-reconcile coordination.

The suite exercises only ``AddStockProbeCoordinator`` with in-memory callbacks,
events, and sentinel adapter/pacer objects.  It opens no socket, launches no
process or GUI, and reads or writes no market-data bank.

Run:
    python -B engine/addstock_vol_reconcile_selftest.py
"""

from __future__ import annotations

import ast
import inspect
import json
import socket
import tempfile
import sys
import threading
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parent))

import live_spot_probe as probe  # noqa: E402
import operation_gate  # noqa: E402
import stock_ibkr as stock  # noqa: E402
import stock_validate as validate  # noqa: E402


FAILURES: list[str] = []
CHECKS = 0


def check(name: str, condition, detail: object = "") -> None:
    global CHECKS
    CHECKS += 1
    passed = bool(condition)
    print(f"[{'PASS' if passed else 'FAIL'}] {name}"
          + ("" if passed or not detail else f"  {detail}"))
    if not passed:
        FAILURES.append(name)


def fill_rows(ticker: str, *intervals: str) -> list[dict]:
    return [{"ticker": ticker, "interval": interval}
            for interval in intervals]


def queue_row(ticker: str, kind_token: str, day: str) -> dict:
    return {
        "ticker": ticker,
        "kind": kind_token,
        "kind_token": kind_token,
        "day": day,
        "value": 0.5,
        "reason": "jump_up",
        "reasons": ["jump_up"],
    }


def terminal_row(ticker: str, *, requests: int = 0) -> dict:
    return {
        "ticker": ticker,
        "verdict": "MATCH" if requests else "NO_LIVE_DATA",
        "pass_equivalent": True,
        "request_count": requests,
        "reused_request_count": 0,
        "network": bool(requests),
        "written": False,
    }


def atomic_reconcile_outcome(
        row: dict, state: str, *, indeterminate: bool = False,
        month_write_completed: bool | None = None) -> dict:
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


def test_empty_plan_flows_to_port_free_check() -> None:
    events: list[tuple] = []
    reconcile_calls: list[tuple] = []

    def plan_fn(ticker, rows):
        events.append(("plan", ticker, tuple(row["interval"] for row in rows)))
        return []

    def reconcile_fn(*args, **kwargs):
        reconcile_calls.append((args, kwargs))
        raise AssertionError("an empty plan must not execute reconciliation")

    def check_fn(adapter, pacer, ticker, rows, seed):
        events.append(("check", ticker, adapter, pacer, len(rows), seed))
        return {"row": terminal_row(ticker)}

    coordinator = probe.AddStockProbeCoordinator(
        [("AAA", "1m"), ("AAA", "1d")],
        check_fn=check_fn,
        probe_fn=lambda *_args: (_ for _ in ()).throw(
            AssertionError("terminal check must not probe")),
        reconcile_plan_fn=plan_fn,
        reconcile_fn=reconcile_fn,
        reconcile_run_id="empty-plan-selftest",
        seed=11,
    )
    coordinator.record_series(fill_rows("AAA", "1m", "1d"))
    plan_task = coordinator.claim()
    coordinator.execute(plan_task, object(), object())
    ready = coordinator.status_snapshot()
    drained = coordinator.drain_checks()
    result = coordinator.result()
    reconcile_result = coordinator.reconcile_result()

    check("empty plan is the first phase and then exposes one check",
          plan_task == ("reconcile-plan", "AAA")
          and ready["reconcile_plan_ready"] == 0
          and ready["reconcile_ready"] == 0
          and ready["check_ready"] == 1,
          (plan_task, ready))
    check("empty plan reaches the port-free check without reconciliation",
          drained == 1
          and [event[0] for event in events] == ["plan", "check"]
          and events[0][2] == ("1d", "1m")
          and events[1][2:4] == (None, None)
          and not reconcile_calls,
          events)
    check("empty-plan reconciliation evidence is separately complete",
          reconcile_result == {
              "kind": "vol_value_reconcile",
              "version": 1,
              "enabled": True,
              "planned_count": 0,
              "completed_count": 0,
              "request_count": 0,
              "request_count_unknown": 0,
              "settled_count": 0,
              "corrected_count": 0,
              "unresolved_count": 0,
              "plan_error_count": 0,
              "queue_resolved_count": 0,
              "queue_pending_count": 0,
              "rows": [],
              "rows_truncated": 0,
              "pending_rows": [],
              "pending_rows_truncated": 0,
              "pending_plan_tickers": [],
              "pending_plan_tickers_truncated": 0,
              "pending_fill_tickers": [],
              "pending_fill_tickers_truncated": 0,
              "halted_fill_tickers": [],
              "halted_fill_tickers_truncated": 0,
              "status": "complete",
          }
          and result["checked_count"] == result["completed_count"] == 1
          and "reconcile_result" not in result,
          (reconcile_result, result))


def test_two_rows_serialize_before_check_and_probe() -> None:
    events: list[tuple] = []
    locks: list[object] = []
    adapter = object()
    pacer = object()
    planned = [
        queue_row("SER", "1m-iv", "2024-06-18"),
        queue_row("SER", "1d-hvol", "2024-06-17"),
    ]

    def plan_fn(ticker, rows):
        events.append(("plan", ticker, len(rows)))
        return list(planned)  # coordinator owns deterministic sorting

    def reconcile_fn(got_adapter, got_pacer, row, run_id, *, manifest_lock):
        events.append(("reconcile", row["kind_token"], row["day"], run_id,
                       got_adapter, got_pacer))
        locks.append(manifest_lock)
        status = "settled" if len(locks) == 1 else "corrected"
        return {
            "status": status,
            "ticker": row["ticker"],
            "kind": row["kind_token"],
            "kind_token": row["kind_token"],
            "day": row["day"],
            "request_count": 1,
            "queue_resolved": True,
            **({"registry_committed": True} if status == "settled"
               else {"bank_committed": True}),
        }

    def check_fn(got_adapter, got_pacer, ticker, rows, seed):
        events.append(("check", ticker, got_adapter, got_pacer, len(rows), seed))
        return {"selection": {"day": "2024-06-19"}}

    def probe_fn(got_adapter, got_pacer, ticker, selection):
        events.append(("probe", ticker, selection["day"], got_adapter, got_pacer))
        return terminal_row(ticker, requests=1) | {"day": selection["day"]}

    coordinator = probe.AddStockProbeCoordinator(
        [("SER", "1m"), ("SER", "1d")],
        check_fn=check_fn,
        probe_fn=probe_fn,
        reconcile_plan_fn=plan_fn,
        reconcile_fn=reconcile_fn,
        reconcile_run_id="serial-selftest",
        seed=23,
    )
    coordinator.record_series(fill_rows("SER", "1m", "1d"))
    plan_task = coordinator.claim()
    coordinator.execute(plan_task, adapter, pacer)
    plan_ready = coordinator.status_snapshot()

    first = coordinator.claim()
    first_active = coordinator.status_snapshot()
    blocked_while_first_active = coordinator.claim()
    coordinator.execute(first, adapter, pacer)
    after_first = coordinator.status_snapshot()

    second = coordinator.claim()
    blocked_while_second_active = coordinator.claim()
    coordinator.execute(second, adapter, pacer)
    after_second = coordinator.status_snapshot()

    check_task = coordinator.claim()
    coordinator.execute(check_task, adapter, pacer)
    probe_task = coordinator.claim()
    coordinator.execute(probe_task, adapter, pacer)
    final_status = coordinator.status_snapshot()
    reconciliation = coordinator.reconcile_result()
    ws8 = coordinator.result()

    check("two exact rows are serialized under one ticker owner",
          plan_ready["reconcile_ready"] == 2
          and first[:2] == ("reconcile", "SER")
          and first[2]["kind_token"] == "1d-hvol"
          and first_active["active_reconciles"] == 1
          and first_active["reconcile_ready"] == 1
          and blocked_while_first_active is None
          and after_first["reconcile_completed"] == 1
          and after_first["reconcile_ready"] == 1
          and second[:2] == ("reconcile", "SER")
          and second[2]["kind_token"] == "1m-iv"
          and blocked_while_second_active is None
          and after_second["reconcile_completed"] == 2
          and after_second["check_ready"] == 1,
          (plan_ready, first, first_active, after_first, second, after_second))
    check("all reconciliation completes before check and probe",
          [event[0] for event in events]
          == ["plan", "reconcile", "reconcile", "check", "probe"]
          and check_task == ("check", "SER")
          and probe_task[:2] == ("probe", "SER")
          and final_status["probe_done"] == 1
          and final_status["reconcile_completed"] == 2,
          (events, check_task, probe_task, final_status))
    check("both rows borrow the same adapter, pacer, run id, and ticker lock",
          len(locks) == 2 and locks[0] is locks[1]
          and all(event[3] == "serial-selftest"
                  and event[4:6] == (adapter, pacer)
                  for event in events if event[0] == "reconcile"),
          (events, locks))
    check("reconciliation has a separate exact result ledger",
          reconciliation["status"] == "complete"
          and reconciliation["planned_count"] == 2
          and reconciliation["completed_count"] == 2
          and reconciliation["settled_count"] == 1
          and reconciliation["corrected_count"] == 1
          and reconciliation["request_count"] == 2
          and reconciliation["queue_resolved_count"] == 2
          and [row["kind_token"] for row in reconciliation["rows"]]
          == ["1d-hvol", "1m-iv"]
          and not reconciliation["pending_rows"]
          and ws8["request_count"] == 1
          and len(ws8["rows"]) == 1
          and "kind_token" not in ws8["rows"][0]
          and "reconcile_result" not in ws8,
          (reconciliation, ws8))


def test_pause_finishes_inflight_then_prevents_next_claim() -> None:
    pause = threading.Event()
    rows = [
        queue_row("PAUSE", "1d-hvol", "2024-06-17"),
        queue_row("PAUSE", "1m-iv", "2024-06-18"),
    ]
    calls: list[str] = []

    def reconcile_fn(_adapter, _pacer, row, _run_id, *, manifest_lock):
        del manifest_lock
        calls.append(row["kind_token"])
        if len(calls) == 1:
            pause.set()  # the already-claimed row still reaches its boundary
        return {
            "status": "settled", "ticker": row["ticker"],
            "kind": row["kind_token"], "kind_token": row["kind_token"],
            "day": row["day"], "request_count": 1,
            "queue_resolved": True,
            "registry_committed": True,
        }

    coordinator = probe.AddStockProbeCoordinator(
        [("PAUSE", "1m")],
        check_fn=lambda *_args: {"row": terminal_row("PAUSE")},
        probe_fn=lambda *_args: terminal_row("PAUSE", requests=1),
        reconcile_plan_fn=lambda *_args: list(rows),
        reconcile_fn=reconcile_fn,
        pause=pause,
        seed=31,
    )
    coordinator.record_series(fill_rows("PAUSE", "1m"))
    plan_task = coordinator.claim()
    coordinator.execute(plan_task, object(), object())
    first = coordinator.claim()
    coordinator.execute(first, object(), object())
    paused_status = coordinator.status_snapshot()
    paused_result = coordinator.reconcile_result()
    blocked = coordinator.claim()
    pause.clear()
    second = coordinator.claim()
    coordinator.execute(second, object(), object())

    check("Pause lets the claimed row finish, then prevents a new claim",
          calls == ["1d-hvol", "1m-iv"]
          and blocked is None
          and paused_status["active_reconciles"] == 0
          and paused_status["reconcile_completed"] == 1
          and paused_status["reconcile_ready"] == 1
          and paused_result["status"] == "pending"
          and paused_result["completed_count"] == 1
          and len(paused_result["pending_rows"]) == 1
          and second[:2] == ("reconcile", "PAUSE"),
          (calls, paused_status, paused_result, second))


def test_cancel_and_port_free_drain_never_claim_reconcile() -> None:
    cancel = threading.Event()
    reconcile_calls: list[dict] = []
    planned = [queue_row("STOP", "1m-iv", "2024-06-18")]
    coordinator = probe.AddStockProbeCoordinator(
        [("STOP", "1m")],
        check_fn=lambda *_args: {"row": terminal_row("STOP")},
        probe_fn=lambda *_args: terminal_row("STOP", requests=1),
        reconcile_plan_fn=lambda *_args: list(planned),
        reconcile_fn=lambda _adapter, _pacer, row, _run, **_kwargs:
            reconcile_calls.append(dict(row)) or {
                "status": "settled", "ticker": row["ticker"],
                "kind_token": row["kind_token"], "day": row["day"],
                "request_count": 1, "queue_resolved": True,
                "registry_committed": True,
            },
        cancel=cancel,
        seed=37,
    )
    coordinator.record_series(fill_rows("STOP", "1m"))

    # The degraded finalizer may execute only already-ready offline checks.  It
    # must not even run the reconciliation planner.
    before_plan_drain = coordinator.drain_checks(object(), object())
    plan_task = coordinator.claim()
    coordinator.execute(plan_task, object(), object())
    ready = coordinator.status_snapshot()
    after_plan_drain = coordinator.drain_checks(object(), object())
    cancel.set()
    cancelled_claim = coordinator.claim()
    pending = coordinator.reconcile_result(skip_reason="fleet_down",
                                           down_ports=[3000, 2000])

    check("drain_checks never claims planning, reconcile, or probe work",
          before_plan_drain == after_plan_drain == 0
          and plan_task == ("reconcile-plan", "STOP")
          and ready["reconcile_ready"] == 1
          and not reconcile_calls
          and coordinator.has_pending(),
          (before_plan_drain, plan_task, ready, after_plan_drain,
           reconcile_calls))
    check("Cancel prevents a pending reconciliation claim without hiding debt",
          cancelled_claim is None
          and not reconcile_calls
          and pending["status"] == "pending"
          and pending["skip_reason"] == "fleet_down"
          and pending["down_ports"] == [2000, 3000]
          and pending["planned_count"] == 1
          and pending["completed_count"] == 0
          and len(pending["pending_rows"]) == 1,
          (cancelled_claim, pending))


def test_malformed_and_hard_failure_outcomes_stay_visible() -> None:
    row = queue_row("BAD", "1m-iv", "2024-06-18")
    malformed = probe.AddStockProbeCoordinator(
        [("BAD", "1m")],
        check_fn=lambda *_args: {"row": terminal_row("BAD")},
        probe_fn=lambda *_args: terminal_row("BAD", requests=1),
        reconcile_plan_fn=lambda *_args: [dict(row)],
        reconcile_fn=lambda *_args, **_kwargs: {
            "status": "settled", "ticker": "BAD",
            "kind_token": "1m-iv", "day": "2024-06-18",
            "request_count": 0, "queue_resolved": True,
        },
        seed=41,
    )
    malformed.record_series(fill_rows("BAD", "1m"))
    malformed.execute(malformed.claim(), object(), object())
    malformed.execute(malformed.claim(), object(), object())
    malformed.drain_checks()
    malformed_result = malformed.reconcile_result()
    check("malformed reconcile envelopes retire active state as visible debt",
          not malformed.has_pending()
          and malformed_result["status"] == "incomplete"
          and malformed_result["completed_count"] == 1
          and malformed_result["unresolved_count"] == 1
          and malformed_result["request_count_unknown"] == 1
          and malformed_result["queue_pending_count"] == 1
          and malformed_result["rows"][0]["status"] == "ambiguous"
          and "exactly one request" in malformed_result["rows"][0]["error"],
          malformed_result)

    hard_rows = [
        queue_row("HARD", "1d-hvol", "2024-06-17"),
        queue_row("HARD", "1m-iv", "2024-06-18"),
    ]
    hard = probe.AddStockProbeCoordinator(
        [("HARD", "1m")],
        check_fn=lambda *_args: {"row": terminal_row("HARD")},
        probe_fn=lambda *_args: terminal_row("HARD", requests=1),
        reconcile_plan_fn=lambda *_args: list(hard_rows),
        reconcile_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            KeyboardInterrupt("synthetic hard exit")),
        seed=43,
    )
    hard.record_series(fill_rows("HARD", "1m"))
    hard.execute(hard.claim(), object(), object())
    hard_task = hard.claim()
    hard_raised = False
    try:
        hard.execute(hard_task, object(), object())
    except probe.EvidenceError:
        hard_raised = True
    hard_result = hard.reconcile_result()
    check("hard reconcile exit becomes ambiguous debt without an active-task leak",
          hard_raised and not hard.has_pending()
          and hard_result["status"] == "incomplete"
          and hard_result["completed_count"] == 1
          and hard_result["unresolved_count"] == 1
          and hard_result["request_count_unknown"] == 1
          and hard_result["queue_pending_count"] == 2
          and hard_result["rows"][0]["status"] == "ambiguous"
          and len(hard_result["pending_rows"]) == 1
          and hard_result["pending_rows"][0]["debt_reason"]
          == "worker-failed-unstarted",
          hard_result)

    contradictory_rejected = False
    try:
        probe._normalize_addstock_reconcile(
            "BAD", row, {
                "status": "settled", "ticker": "BAD",
                "kind_token": "1m-iv", "day": "2024-06-18",
                "request_count": 1, "request_count_unknown": True,
                "queue_resolved": True,
            })
    except probe.EvidenceError:
        contradictory_rejected = True
    check("known and unknown request evidence cannot coexist",
          contradictory_rejected)
    stripped = probe._normalize_addstock_reconcile(
        "BAD", row, {
            "status": "settled", "ticker": "BAD",
            "kind_token": "1m-iv", "day": "2024-06-18",
            "request_count": 1, "queue_resolved": True,
            "registry_committed": True,
            "hostile": object(), "huge": "x" * 100_000,
            "non_finite_extra": float("nan"),
        })
    check("Add Stocks strips hostile reconcile extras before reporting",
          stripped["status"] == "settled"
          and "hostile" not in stripped and "huge" not in stripped
          and "non_finite_extra" not in stripped)

    plan_failure = probe.AddStockProbeCoordinator(
        [("PLAN", "1m")],
        check_fn=lambda *_args: {"row": terminal_row("PLAN")},
        probe_fn=lambda *_args: terminal_row("PLAN", requests=1),
        reconcile_plan_fn=lambda *_args: (_ for _ in ()).throw(
            RuntimeError("synthetic plan refusal")),
        reconcile_fn=lambda *_args, **_kwargs: {},
        seed=47,
    )
    plan_failure.record_series(fill_rows("PLAN", "1m"))
    plan_failure.execute(plan_failure.claim(), object(), object())
    plan_failure.drain_checks()
    plan_result = plan_failure.reconcile_result()
    check("planning failure has its own count and cannot report complete",
          plan_result["status"] == "incomplete"
          and plan_result["plan_error_count"] == 1
          and plan_result["planned_count"] == 0
          and plan_result["completed_count"] == 0
          and plan_result["rows"][0]["phase"] == "plan",
          plan_result)


def test_hostile_plan_rows_never_reach_pending_evidence() -> None:
    reconcile_calls: list[str] = []

    def plan(ticker, _rows):
        row = queue_row(ticker, "1d-iv", "2026-01-09")
        row["value"] = float("nan")
        if ticker == "EXTRA":
            row["hostile_extra"] = {
                "nested": [object()],
                "huge": "LEAK_MARKER" * 10_000,
            }
        return [row]

    coordinator = probe.AddStockProbeCoordinator(
        [("EXTRA", "1m"), ("NAN", "1m")],
        check_fn=lambda _adapter, _pacer, ticker, *_args: {
            "row": terminal_row(ticker)},
        probe_fn=lambda *_args: (_ for _ in ()).throw(
            AssertionError("terminal checks must not probe")),
        reconcile_plan_fn=plan,
        reconcile_fn=lambda _adapter, _pacer, row, *_args, **_kwargs:
            reconcile_calls.append(row["ticker"]),
        seed=51,
    )
    coordinator.record_series(
        fill_rows("EXTRA", "1m") + fill_rows("NAN", "1m"))
    coordinator.execute(coordinator.claim(), object(), object())
    coordinator.execute(coordinator.claim(), object(), object())
    coordinator.drain_checks(object(), object())
    result = coordinator.reconcile_result()
    serialized = None
    try:
        serialized = json.dumps(result, allow_nan=False, sort_keys=True)
    except (TypeError, ValueError):
        pass
    check("hostile Add Stocks plan rows never reach pending/abandoned evidence",
          not reconcile_calls and not coordinator.has_pending()
          and result["status"] == "incomplete"
          and result["plan_error_count"] == 2
          and result["planned_count"] == result["completed_count"] == 0
          and result["pending_rows"] == []
          and result["pending_rows_truncated"] == 0
          and len(result["rows"]) == 2
          and all(row.get("phase") == "plan"
                  and len(row.get("error", "")) <= 500
                  for row in result["rows"])
          and serialized is not None
          and "LEAK_MARKER" not in serialized,
          result)


def test_atomic_commit_evidence_is_bounded_and_truthful() -> None:
    rows = [
        queue_row("ATOMIC", "1d-iv", "2026-01-10"),
        queue_row("ATOMIC", "1d-iv", "2026-01-11"),
        queue_row("ATOMIC", "1d-iv", "2026-01-12"),
    ]

    def reconcile(_adapter, _pacer, row, _run_id, **_kwargs):
        if row["day"] == "2026-01-10":
            return atomic_reconcile_outcome(row, "rolled_back")
        if row["day"] == "2026-01-11":
            return atomic_reconcile_outcome(row, "recovery_required")
        return {
            "status": "settled", "ticker": row["ticker"],
            "kind_token": row["kind_token"], "day": row["day"],
            "request_count": 1, "queue_resolved": False,
            "registry_committed": True,
            "reason": "jump_up", "reasons": ["jump_up"],
        }

    coordinator = probe.AddStockProbeCoordinator(
        [("ATOMIC", "1m")],
        check_fn=lambda *_args: {"row": terminal_row("ATOMIC")},
        probe_fn=lambda *_args: terminal_row("ATOMIC", requests=1),
        reconcile_plan_fn=lambda *_args: list(rows),
        reconcile_fn=reconcile,
        seed=53,
    )
    coordinator.record_series(fill_rows("ATOMIC", "1m"))
    coordinator.execute(coordinator.claim(), object(), object())
    for _row in rows:
        coordinator.execute(coordinator.claim(), object(), object())
    coordinator.drain_checks()
    result = coordinator.reconcile_result()
    rolled_back, recovery_required, registry_committed = result["rows"]
    json_safe = True
    try:
        json.dumps(result["rows"], allow_nan=False)
    except (TypeError, ValueError):
        json_safe = False
    check("Add Stocks preserves verified rollback and strips callback extras",
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
          and "oversized_extra" not in rolled_back,
          rolled_back)
    check("Add Stocks preserves recovery-required corrected-byte truth",
          recovery_required["status"] == "ambiguous"
          and recovery_required["commit_state"] == "recovery_required"
          and recovery_required["rollback_verified"] is False
          and recovery_required["bank_written"] is True
          and recovery_required["correction_recorded"] is False
          and recovery_required["bank_committed"] is True
          and recovery_required["commit_evidence"]["month_state"]
          == "corrected"
          and recovery_required["commit_evidence"]["manifest_state"]
          == "original",
          recovery_required)
    check("Add Stocks separates durable settlement from queue retirement",
          registry_committed["status"] == "settled"
          and registry_committed["registry_committed"] is True
          and registry_committed["queue_resolved"] is False
          and result["settled_count"] == 1
          and result["unresolved_count"] == 2
          and result["queue_pending_count"] == 3,
          result)
    check("retained atomic coordinator evidence is bounded JSON-safe data",
          json_safe and result["request_count"] == 3
          and result["request_count_unknown"] == 0,
          result)
    ordinary_correction = probe._normalize_addstock_reconcile(
        "ATOMIC", rows[0], {
            "status": "corrected", "ticker": "ATOMIC",
            "kind_token": "1d-iv", "day": "2026-01-10",
            "request_count": 1, "queue_resolved": False,
            "bank_committed": True,
            "stored_value": None, "served_value": 0.3,
            "reason": "hard_nonfinite", "reasons": ["hard_nonfinite"],
        })
    check("Add Stocks keeps hard-nonfinite None and durable correction proof",
          ordinary_correction["status"] == "corrected"
          and ordinary_correction["bank_committed"] is True
          and ordinary_correction["queue_resolved"] is False
          and ordinary_correction["stored_value"] is None
          and ordinary_correction["served_value"] == 0.3
          and "commit_state" not in ordinary_correction,
          ordinary_correction)
    indeterminate_recovery = probe._normalize_addstock_reconcile(
        "ATOMIC", rows[0], atomic_reconcile_outcome(
            rows[0], "recovery_required", indeterminate=True))
    check("Add Stocks preserves fully indeterminate recovery truth",
          indeterminate_recovery["status"] == "ambiguous"
          and indeterminate_recovery["bank_written"] is None
          and indeterminate_recovery["correction_recorded"] is None
          and "bank_committed" not in indeterminate_recovery
          and indeterminate_recovery["commit_evidence"]["month_state"]
          == "indeterminate",
          indeterminate_recovery)
    split_recovery_raw = atomic_reconcile_outcome(
        rows[0], "recovery_required")
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
    split_recovery = probe._normalize_addstock_reconcile(
        "ATOMIC", rows[0], split_recovery_raw)
    check("Add Stocks preserves split month/manifest recovery truth",
          split_recovery["status"] == "ambiguous"
          and split_recovery["bank_written"] is False
          and split_recovery["correction_recorded"] is True
          and split_recovery["bank_committed"] is False
          and split_recovery["commit_evidence"]["month_state"] == "original"
          and split_recovery["commit_evidence"]["manifest_state"]
          == "corrected",
          split_recovery)

    missing_proof_rejected = False
    contradictory_proof_rejected = False
    try:
        probe._normalize_addstock_reconcile("ATOMIC", rows[0], {
            "status": "settled", "ticker": "ATOMIC",
            "kind_token": "1d-iv", "day": "2026-01-10",
            "request_count": 1, "queue_resolved": True,
        })
    except probe.EvidenceError as exc:
        missing_proof_rejected = "committed registry" in str(exc)
    try:
        probe._normalize_addstock_reconcile("ATOMIC", rows[0], {
            "status": "corrected", "ticker": "ATOMIC",
            "kind_token": "1d-iv", "day": "2026-01-10",
            "request_count": 1, "queue_resolved": True,
            "bank_committed": False,
        })
    except probe.EvidenceError as exc:
        contradictory_proof_rejected = "committed bank" in str(exc)
    check("Add Stocks rejects success or queue retirement without proof",
          missing_proof_rejected and contradictory_proof_rejected)

    bad_rows = [
        queue_row("BADATOM", "1d-iv", "2026-01-13"),
        queue_row("BADATOM", "1d-iv", "2026-01-14"),
    ]

    def malformed(_adapter, _pacer, row, _run_id, **_kwargs):
        value = atomic_reconcile_outcome(row, "recovery_required")
        if row["day"] == "2026-01-13":
            value["commit_evidence"]["hostile"] = object()
        else:
            value["commit_evidence"]["restore_errors"] = ["x" * 321]
        return value

    rejected = probe.AddStockProbeCoordinator(
        [("BADATOM", "1m")],
        check_fn=lambda *_args: {"row": terminal_row("BADATOM")},
        probe_fn=lambda *_args: terminal_row("BADATOM", requests=1),
        reconcile_plan_fn=lambda *_args: list(bad_rows),
        reconcile_fn=malformed,
        seed=59,
    )
    rejected.record_series(fill_rows("BADATOM", "1m"))
    rejected.execute(rejected.claim(), object(), object())
    for _row in bad_rows:
        rejected.execute(rejected.claim(), object(), object())
    rejected.drain_checks()
    rejected_result = rejected.reconcile_result()
    check("malformed and oversized Add Stocks atomic evidence fails closed",
          rejected_result["completed_count"] == 2
          and rejected_result["request_count"] == 0
          and rejected_result["request_count_unknown"] == 2
          and rejected_result["unresolved_count"] == 2
          and rejected_result["queue_pending_count"] == 2
          and all(row["status"] == "ambiguous"
                  and row["request_count_unknown"] is True
                  and "commit_evidence" not in row
                  for row in rejected_result["rows"])
          and "fields" in rejected_result["rows"][0]["error"]
          and "restore error" in rejected_result["rows"][1]["error"],
          rejected_result)


def test_unpublished_and_halted_fills_cannot_report_reconcile_complete() -> None:
    common = {
        "check_fn": lambda *_args: {"row": terminal_row("UNUSED")},
        "probe_fn": lambda *_args: terminal_row("UNUSED", requests=1),
        "reconcile_plan_fn": lambda *_args: [],
        "reconcile_fn": lambda *_args, **_kwargs: {},
        "seed": 49,
    }
    waiting = probe.AddStockProbeCoordinator(
        [("WAIT", "1m")], **common)
    waiting_result = waiting.reconcile_result(skip_reason="fleet_down")
    check("missing terminal fill publication stays visible as planning debt",
          waiting_result["status"] == "pending"
          and waiting_result["pending_fill_tickers"] == ["WAIT"]
          and waiting_result["halted_fill_tickers"] == []
          and waiting_result["planned_count"] == 0,
          waiting_result)

    halted = probe.AddStockProbeCoordinator(
        [("HALT", "1m")], **common)
    halted.record_series([
        {"ticker": "HALT", "interval": "1m", "halt": "source-refused"},
    ])
    halted_result = halted.reconcile_result()
    check("terminal halted fill is incomplete rather than falsely complete",
          halted_result["status"] == "incomplete"
          and halted_result["pending_fill_tickers"] == []
          and halted_result["halted_fill_tickers"] == ["HALT"]
          and halted_result["planned_count"] == 0,
          halted_result)

    ticker_count = probe.MAX_RECONCILE_RESULTS + 2
    selections = [(f"P{i:05d}", "1m") for i in range(ticker_count)]
    capped = probe.AddStockProbeCoordinator(selections, **common)
    capped.record_series([
        {"ticker": ticker, "interval": interval}
        for ticker, interval in selections
    ])
    capped_result = capped.reconcile_result()
    check("pending planning evidence is bounded without hiding its exact total",
          capped_result["status"] == "pending"
          and len(capped_result["pending_plan_tickers"])
          == probe.MAX_RECONCILE_RESULTS
          and capped_result["pending_plan_tickers_truncated"] == 2
          and (len(capped_result["pending_plan_tickers"])
               + capped_result["pending_plan_tickers_truncated"])
          == ticker_count,
          {key: capped_result[key] for key in (
              "status", "pending_plan_tickers_truncated")})

    summary_lines = stock.summarize_report({
        "run": "m3-summary", "account": "offline", "port": "fake",
        "totals": {}, "series": [],
        "vol_value_reconcile": {
            **capped_result,
            "request_count_unknown": 2,
            "report": "offline-report.json",
        },
    })
    summary_blob = "\n".join(summary_lines)
    check("human summary includes unknown requests, truncated debt, and report path",
          "2 request state(s) unknown" in summary_blob
          and f"{ticker_count:,} unstarted" in summary_blob
          and "Volatility report: offline-report.json" in summary_blob,
          summary_blob)

    with tempfile.TemporaryDirectory(
            prefix="addstock_m3_report_selftest_") as tmp:
        report_root = Path(tmp) / "Stocks Data"
        report_root.mkdir()
        durable = {
            "run": "addstock-m3-durable", "root": str(report_root),
            "series": [], "totals": {},
            "vol_value_reconcile": halted_result,
        }
        saved = stock._write_parallel_report(
            report_root, durable, stage="post-pipeline-selftest")
        persisted = (json.loads(Path(saved).read_text(encoding="utf-8"))
                     if saved else {})
    check("post-pipeline machine report durably contains bounded M3 evidence",
          bool(saved)
          and persisted.get("vol_value_reconcile") == halted_result
          and persisted.get("report_path") == saved,
          persisted)


def test_serial_gap_fill_post_pipeline_holds_lease_and_pause_boundary() -> None:
    """Exercise the exact serial post-pipeline loop without a live adapter."""
    pause = threading.Event()
    cancel = threading.Event()
    allow_first_finish = threading.Event()
    preclaim_pause_seen = threading.Event()
    successor_pause_seen = threading.Event()
    first_started = threading.Event()
    state_lock = threading.Lock()
    gate_active = threading.Event()
    claimed: list[str] = []
    executed: list[str] = []
    finalized: list[str] = []
    pause_claim_snapshots: list[list[str]] = []
    lease_samples: list[tuple[str, bool]] = []
    overlap_samples: list[tuple[str, bool]] = []
    worker_errors: list[str] = []
    result_box: list[dict] = []

    @contextmanager
    def fake_fetch_lease(kind, owner=None):
        lease_samples.append((f"enter:{kind}:{owner}",
                              gate_active.is_set()))
        gate_active.set()
        try:
            yield object()
        finally:
            gate_active.clear()
            lease_samples.append(("exit", gate_active.is_set()))

    def on_pause(paused, _info=None):
        if not paused:
            return
        with state_lock:
            pause_claim_snapshots.append(list(claimed))
            count = len(pause_claim_snapshots)
        if count == 1:
            preclaim_pause_seen.set()
        elif count == 2:
            successor_pause_seen.set()

    pacer = SimpleNamespace(_pause=None, _on_pause=on_pause)

    class FakeAdapter:
        port = 0

        def __init__(self):
            self.disconnected = False

        def account(self):
            return "OFFLINE"

        def disconnect(self):
            self.disconnected = True

    adapter = FakeAdapter()

    class FakePostPipeline:
        def __init__(self):
            self.tasks = ["first", "second"]
            self.recorded_rows: list[dict] = []

        def record_series(self, rows):
            lease_samples.append(("record", gate_active.is_set()))
            self.recorded_rows = [dict(row) for row in rows]

        def claim(self):
            with state_lock:
                if len(claimed) >= len(self.tasks):
                    lease_samples.append(("claim-empty",
                                          gate_active.is_set()))
                    return None
                task = self.tasks[len(claimed)]
                predecessor_complete = (not claimed
                                        or finalized == claimed)
                overlap_samples.append((task, predecessor_complete))
                claimed.append(task)
                lease_samples.append((f"claim:{task}",
                                      gate_active.is_set()))
                return task

        def has_pending(self):
            with state_lock:
                return len(claimed) < len(self.tasks)

        def execute(self, task, got_adapter, got_pacer):
            with state_lock:
                executed.append(task)
                lease_samples.append((f"execute:{task}",
                                      gate_active.is_set()))
                overlap_samples.append(
                    (f"identity:{task}",
                     got_adapter is adapter and got_pacer is pacer))
            if task == "first":
                # Simulate Pause arriving while the atomic post task owns its
                # source/finalization boundary.  The production loop must let
                # this task finish but must not claim its successor.
                pause.set()
                first_started.set()
                if not allow_first_finish.wait(5):
                    worker_errors.append("first task release timed out")
            with state_lock:
                finalized.append(task)

    pipeline = FakePostPipeline()

    def fake_fill(*_args, **_kwargs):
        return {
            "ticker": "SERIAL", "interval": "1m", "added": 0,
            "dup_existing": 0, "conflicts": 0, "written": 0,
            "requests": 0, "bars_fetched": 0,
        }

    def run_gap_fill(root):
        try:
            result_box.append(stock.nightly_gap_fill(
                root, [("SERIAL", "1m")], adapter_factory=lambda: adapter,
                pacer=pacer, pause=pause, cancel=cancel,
                today=date(2024, 6, 20), resolved={"SERIAL": 12345},
                run_id="serial-post-selftest", post_pipeline=pipeline,
                _pickup_plan_ahead=False))
        except Exception as exc:  # noqa: BLE001 - report exact thread failure
            worker_errors.append(f"{type(exc).__name__}: {exc}")

    pause.set()  # pre-set Pause must block the very first post-task claim
    with tempfile.TemporaryDirectory(prefix="addstock-post-offline-") as tmp:
        with (patch.object(operation_gate, "acquire", fake_fetch_lease),
              patch.object(socket, "socket",
                           side_effect=AssertionError("socket forbidden")),
              patch.object(socket, "create_connection",
                           side_effect=AssertionError("network forbidden")),
              patch.object(stock, "live_adapter_factory",
                           side_effect=AssertionError("live adapter forbidden")),
              patch.object(stock, "set_diag_log", lambda *_args: None),
              patch.object(stock, "_disk_preflight",
                           lambda *_args, **_kwargs: True),
              patch.object(stock, "_prevent_sleep", lambda: None),
              patch.object(stock, "_allow_sleep", lambda: None),
              patch.object(stock, "_fill_series", fake_fill),
              patch.object(stock, "_finalize",
                           lambda report, _dirs, _say: report),
              patch.object(stock.ingest, "_RunDirs",
                           lambda _root, _run_id: object()),
              patch.object(stock.ss, "load_manifest", lambda *_args: {}),
              patch.object(validate, "load_ibkr_earliest",
                           lambda *_args, **_kwargs: {})):
            worker = threading.Thread(
                target=run_gap_fill, args=(Path(tmp),), daemon=True)
            worker.start()

            preclaim_reached = preclaim_pause_seen.wait(5)
            with state_lock:
                preclaim_blocked = not claimed and not executed
            pause.clear()

            first_reached = first_started.wait(5)
            with state_lock:
                inflight_blocked = claimed == ["first"] \
                    and executed == ["first"] and not finalized
            allow_first_finish.set()

            successor_boundary_reached = successor_pause_seen.wait(5)
            with state_lock:
                successor_blocked = (claimed == ["first"]
                                     and finalized == ["first"])
            pause.clear()
            worker.join(5)
            if worker.is_alive():
                cancel.set()
                pause.clear()
                allow_first_finish.set()
                worker.join(5)

    check("serial gap_fill post tasks stay inside the fetch lease",
          preclaim_reached and first_reached and successor_boundary_reached
          and not worker.is_alive() and not worker_errors
          and adapter.disconnected and not gate_active.is_set()
          and pipeline.recorded_rows
          and [row["ticker"] for row in pipeline.recorded_rows] == ["SERIAL"]
          and all(held for label, held in lease_samples
                  if label == "record" or label.startswith("claim:")
                  or label.startswith("execute:"))
          and result_box and result_box[0]["account"] == "OFFLINE",
          (worker_errors, lease_samples, result_box))
    check("pre-set and in-task Pause block successor claims at finalization",
          preclaim_blocked and inflight_blocked and successor_blocked
          and pause_claim_snapshots == [[], ["first"]]
          and claimed == executed == finalized == ["first", "second"]
          and all(ok for _task, ok in overlap_samples),
          (preclaim_blocked, inflight_blocked, successor_blocked,
           pause_claim_snapshots, claimed, executed, finalized,
           overlap_samples))


def test_serial_safe_cancel_preserves_pending_plan_and_gui_signal() -> None:
    pause = threading.Event()
    cancel = threading.Event()
    boundary = threading.Event()
    result_box: list[dict] = []
    errors: list[str] = []
    plan_calls: list[str] = []

    pacer = SimpleNamespace(
        _pause=None,
        _on_pause=lambda paused, _info=None: boundary.set() if paused else None)

    class FakeAdapter:
        port = 0

        def account(self):
            return "OFFLINE"

        def disconnect(self):
            return None

    adapter = FakeAdapter()
    coordinator = probe.AddStockProbeCoordinator(
        [("CANCEL", "1m")],
        check_fn=lambda *_args: {"row": terminal_row("CANCEL")},
        probe_fn=lambda *_args: terminal_row("CANCEL", requests=1),
        reconcile_plan_fn=lambda *_args: plan_calls.append("plan") or [],
        reconcile_fn=lambda *_args, **_kwargs: {},
        pause=pause, cancel=cancel, seed=53,
    )

    def fake_fill(*_args, **_kwargs):
        return {
            "ticker": "CANCEL", "interval": "1m", "added": 0,
            "dup_existing": 0, "conflicts": 0, "written": 0,
            "requests": 0, "bars_fetched": 0,
        }

    @contextmanager
    def fake_lease(*_args, **_kwargs):
        yield object()

    def run(root):
        try:
            result_box.append(stock.nightly_gap_fill(
                root, [("CANCEL", "1m")],
                adapter_factory=lambda: adapter, pacer=pacer,
                pause=pause, cancel=cancel, today=date(2024, 6, 20),
                resolved={"CANCEL": 12345}, run_id="serial-cancel-selftest",
                post_pipeline=coordinator, _pickup_plan_ahead=False))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{type(exc).__name__}: {exc}")

    pause.set()
    with tempfile.TemporaryDirectory(prefix="addstock-cancel-offline-") as tmp:
        with (patch.object(operation_gate, "acquire", fake_lease),
              patch.object(socket, "socket",
                           side_effect=AssertionError("socket forbidden")),
              patch.object(socket, "create_connection",
                           side_effect=AssertionError("network forbidden")),
              patch.object(stock, "live_adapter_factory",
                           side_effect=AssertionError("live adapter forbidden")),
              patch.object(stock, "set_diag_log", lambda *_args: None),
              patch.object(stock, "_disk_preflight",
                           lambda *_args, **_kwargs: True),
              patch.object(stock, "_prevent_sleep", lambda: None),
              patch.object(stock, "_allow_sleep", lambda: None),
              patch.object(stock, "_fill_series", fake_fill),
              patch.object(stock, "_finalize",
                           lambda report, _dirs, _say: report),
              patch.object(stock.ingest, "_RunDirs",
                           lambda _root, _run_id: object()),
              patch.object(stock.ss, "load_manifest", lambda *_args: {}),
              patch.object(validate, "load_ibkr_earliest",
                           lambda *_args, **_kwargs: {})):
            worker = threading.Thread(target=run, args=(Path(tmp),), daemon=True)
            worker.start()
            reached = boundary.wait(5)
            cancel.set()
            pause.clear()
            worker.join(5)
    pending = coordinator.reconcile_result(skip_reason="user_cancel")
    check("safe serial cancel publishes terminal fills as pending M3 planning debt",
          reached and not worker.is_alive() and not errors and result_box
          and not plan_calls
          and pending["status"] == "pending"
          and pending["pending_plan_tickers"] == ["CANCEL"]
          and pending["planned_count"] == pending["completed_count"] == 0
          and pending["request_count"] == 0,
          (errors, result_box, pending))

    display_path = Path(__file__).resolve().parents[1] / "display_data.py"
    tree = ast.parse(display_path.read_text(encoding="utf-8"))
    method = next(
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_storage_find_start_fill")
    method_source = ast.get_source_segment(
        display_path.read_text(encoding="utf-8"), method) or ""
    event_pos = method_source.find("serial_pause_safe = threading.Event()")
    attr_pos = method_source.find("self._storage_find_serial_pause_safe")
    callback_pos = method_source.find("parent_pacer._on_pause")
    check("Add Stocks creates the serial safe-boundary event in its run scope",
          0 <= event_pos < attr_pos < callback_pos
          and "serial_pause_safe.set() if paused" in method_source,
          (event_pos, attr_pos, callback_pos))

    reconcile_attach = method_source.find('rep["vol_value_reconcile"]')
    durable_publish = method_source.find(
        "stock_ibkr._write_parallel_report", reconcile_attach)
    attach_return = method_source.find("return rep", durable_publish)
    check("Add Stocks atomically republishes attached M3 evidence before return",
          0 <= reconcile_attach < durable_publish < attach_return,
          (reconcile_attach, durable_publish, attach_return))

    audit_pos = method_source.find("audit_report = vol_value_audit.audit")
    complete_pos = method_source.find(
        'audit_report.get("complete") is not True', audit_pos)
    stale_stop_pos = method_source.find("raise RuntimeError", complete_pos)
    plan_pos = method_source.find("vol_value_reconcile.plan", audit_pos)
    check("Add Stocks refuses a preserved stale queue after incomplete re-audit",
          0 <= audit_pos < complete_pos < stale_stop_pos < plan_pos,
          (audit_pos, complete_pos, stale_stop_pos, plan_pos))

    parallel_source = inspect.getsource(stock.gap_fill_parallel)
    publish_marker = parallel_source.find(
        "Publish a ticker to the post pipeline only after")
    hard_check = parallel_source.rfind(
        "hard_interrupted = _watchdog_interrupted_since", 0, publish_marker)
    publish = parallel_source.find(
        "post_pipeline.record_series", publish_marker)
    finish = parallel_source.find("_finish_job()", publish)
    requeue = parallel_source.rfind(
        "_finish_job(job, requeue=True)", 0, publish_marker)
    check("watchdog reroute decision precedes every successful post publication",
          0 <= hard_check < requeue < publish_marker < publish < finish,
          (hard_check, requeue, publish_marker, publish, finish))


def main() -> int:
    test_empty_plan_flows_to_port_free_check()
    test_two_rows_serialize_before_check_and_probe()
    test_pause_finishes_inflight_then_prevents_next_claim()
    test_cancel_and_port_free_drain_never_claim_reconcile()
    test_malformed_and_hard_failure_outcomes_stay_visible()
    test_hostile_plan_rows_never_reach_pending_evidence()
    test_atomic_commit_evidence_is_bounded_and_truthful()
    test_unpublished_and_halted_fills_cannot_report_reconcile_complete()
    test_serial_gap_fill_post_pipeline_holds_lease_and_pause_boundary()
    test_serial_safe_cancel_preserves_pending_plan_and_gui_signal()
    print()
    print(f"addstock_vol_reconcile_selftest: {CHECKS - len(FAILURES)}/"
          f"{CHECKS} passed, {len(FAILURES)} failed")
    if FAILURES:
        print("Failures:")
        for failure in FAILURES:
            print(f" - {failure}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
