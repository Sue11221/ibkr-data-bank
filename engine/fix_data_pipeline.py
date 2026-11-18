"""Injected, repair-priority Fix Data orchestration.

The engine owns scheduling and lifecycle only. Production callers inject bank
scanners, repair/audit functions, and an adapter factory; tests use fakes.
"""

from __future__ import annotations

import json
import math
import re
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import date, datetime, time as day_time
from pathlib import Path
from collections import deque
from itertools import islice

import operation_gate
import run_log_retention
import fetch_ibkr_bridge as fib
import fetch_operations as fops
import fetch_operation_report as freport
from fetch_authority import AuthorityError
from fetch_ledger import LedgerError
from fetch_run_context import RequestCancelled, RequestRefused

_TERMINAL = (AuthorityError, LedgerError, RequestCancelled)


class _TerminalState:
    def __init__(self):
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.first = None

    def fail(self, error):
        with self.lock:
            if self.first is None:
                self.first = error
            self.stop.set()


class _ObservedIterator:
    def __init__(self, source):
        self.source = source

    def __iter__(self):
        return self

    def __next__(self):
        return fib.without_authority(lambda: _plain_data(next(self.source)))()


def _plain_data(value):
    """Detach observer results before the scheduler inspects them with a child."""
    if value is None:
        return None
    for kind in (bool, str, bytes, int, float):
        if isinstance(value, kind):
            return kind(value)
    if type(value) in (date, datetime, day_time):
        return value
    if isinstance(value, dict):
        return {_plain_data(key): _plain_data(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        kind = next(kind for kind in (list, tuple, set, frozenset) if isinstance(value, kind))
        return kind(_plain_data(item) for item in value)
    if isinstance(value, Iterator):
        return _ObservedIterator(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError("observer returned unsupported scheduler data")


def _prepare_options(options):
    options = dict(options)
    options.setdefault("_reference_opener", None)
    if options["_reference_opener"] is not None and options["engine_tasks"] is not True:
        raise RequestRefused("raw reference transport requires fixed engine tasks")
    options["root"] = Path(options["root"]).resolve()
    options["ports"] = tuple(int(port) for port in options["ports"])
    if type(options["engine_tasks"]) is not bool:
        raise RequestRefused("engine_tasks must be an explicit boolean")
    if options["engine_tasks"] and any(options[name] is not None
            for name in ("fill_fn", "probe_fn", "reconcile_fn")):
        raise RequestRefused("fixed engine tasks cannot be combined with request callbacks")
    if not options["engine_tasks"] and not callable(options["fill_fn"]):
        raise TypeError("observer mode requires fill_fn")
    options["run_id"] = str(options["run_id"] or uuid.uuid4().hex)
    kinds = options["kinds"]
    if isinstance(kinds, bytes):
        raise TypeError("kinds must contain text values, not bytes")
    options["kinds"] = kinds if kinds is None else tuple(
        str(kind) for kind in ((kinds,) if isinstance(kinds, str) else kinds))
    for name in ("scan_fn", "health_fn", "fill_fn", "audit_fn", "series_fn",
                 "refresh_fn", "probe_plan_fn", "probe_fn", "probe_report_fn",
                 "reconcile_plan_fn", "reconcile_fn", "reconcile_report_fn"):
        callback = options[name]
        if callback is not None:
            def observe(*args, _callback=callback, _name=name, **kwargs):
                value = _callback(*args, **kwargs)
                if _name == "reconcile_fn":
                    # This existing bounded schema intentionally discards
                    # hostile extras. Apply it before general detachment so
                    # ignored objects cannot erase valid atomic evidence.
                    value = _reconcile_result(args[3], value)
                return _plain_data(value)
            options[name] = fib.without_authority(observe)
    for name in ("adapter_factory", "progress", "verification_fn", "sleep_fn",
                 "reconcile_pacer_factory", "port_recover_fn", "_reference_opener"):
        if options[name] is not None:
            options[name] = fib.without_authority(options[name])
    for name in ("pause_event", "cancel_event"):
        options[name] = fib.observer_object(options[name])
    return options


def run(*, root, ports, adapter_factory, scan_fn, health_fn=None, fill_fn=None,
        audit_fn, series_fn, refresh_fn, progress=None, pause_event=None,
        cancel_event=None, cache_root=None, run_id=None, sleep_fn=time.sleep,
        probe_plan_fn=None, probe_fn=None, probe_report_fn=None, probe_seed=None,
        verification_fn=None, kinds=("",), reconcile_plan_fn=None, reconcile_fn=None,
        reconcile_pacer_factory=None, reconcile_report_fn=None, port_recover_fn=None,
        engine_tasks=False, evidence_dir=None, _test_capability=None,
        _reference_opener=None):
    """One held repair parent, admitted before any factory, lease or bank work."""
    import stock_ibkr as sk
    options = dict(locals())
    for name in ("sk", "evidence_dir", "_test_capability"):
        options.pop(name)
    operation = fops.begin_operation("repair", sk._auxiliary_directory(evidence_dir),
                                     test_capability=_test_capability)
    context, terminal, completion = operation.context, _TerminalState(), {}
    output, failure, outcome = None, None, "returned"
    try:
        fops.require_root_admission(operation)
        if _reference_opener is not None and context.test_capability is None:
            raise RequestRefused("injected repair reference transport is offline-only")
        options = fib.without_authority(_prepare_options)(options)
        observed_progress = options["progress"]
        if observed_progress is not None:
            def body_progress(event):
                if event.get("type") == "done":
                    completion["done_event"] = dict(event)
                else:
                    observed_progress(event)
            options["progress"] = body_progress
        output = _run_fix_data_body(**options, _terminal=terminal, _completion=completion,
            _fetch_child=operation.child("fixdata-root"))
        if terminal.first is not None:
            raise terminal.first
        if output.get("error") or output.get("port_errors"):
            outcome = "failed"
        elif output.get("cancelled"):
            outcome = "cancelled"
        elif output.get("unprocessed") or output.get("halted") or output.get("probe_issues"):
            outcome = "partial"
        operation.seal()
    except BaseException as exc:
        failure, outcome = terminal.first if terminal.first is not None else exc, "failed"
    try:
        operation.close()
    except BaseException as exc:
        failure, outcome = failure if failure is not None else exc, "failed"
    # Detach exception diagnostics separately: arbitrary __str__/__bool__
    # hooks must not interrupt cleanup or replace the original failure.
    evidence = fib.without_authority(freport.evidence)(context, "repair", outcome=outcome)
    detached = {}
    try:
        detached = fib.without_authority(_detach_completion)(completion)
        evidence["fix_data"] = detached.get("result")
    except BaseException as exc:
        failure = failure if failure is not None else exc
        outcome = "failed"
        evidence.update(fix_data=None, completion_failure=_completion_error(exc),
                        outcome=outcome, verified=False, state="UNVERIFIED")
    if failure is not None:
        evidence.update(failure=_completion_error(failure), verified=False, state="UNVERIFIED")
    # Prepared IDs are plain strings. Do not invoke a rejected input's hooks
    # just to populate a fallback final event after admission/preparation fails.
    final_run_id = options.get("run_id")
    final_run_id = final_run_id if type(final_run_id) is str else ""
    try:
        evidence["report_path"] = freport.write_report(evidence, evidence_dir)
    except freport.OperationReportError as exc:
        evidence.update(verified=False, state="UNVERIFIED", outcome="report_failed",
                        operation_outcome=outcome, report_persisted=False,
                        report_failure=_completion_error(exc))
        primary = failure if failure is not None else exc
        _attach_failure_detail(primary, "fetch_ledger", evidence)
        _notify_done(progress, detached, evidence, primary, final_run_id)
        raise primary
    if failure is not None:
        _attach_failure_detail(failure, "fetch_ledger", evidence)
        _notify_done(progress, detached, evidence, failure, final_run_id)
        raise failure
    output["fetch_ledger"] = evidence
    _notify_done(progress, detached, evidence, None, final_run_id)
    return output


def _detach_completion(completion):
    detached = _plain_data(completion)
    for name in ("result", "done_event"):
        value = detached.get(name)
        if value is not None and type(value) is not dict:
            raise TypeError("completion " + name + " must be a dictionary")
    # Scheduler-only carriers (e.g. lazy iterators/bytes) are not companion
    # evidence. Reject them before persistence and before final delivery, not
    # later inside the writer after they have entered the notification value.
    json.dumps(detached, allow_nan=False)
    return detached


def _completion_error(error):
    """Total diagnostic after close; never supersede the primary failure.

    The scheduler's _error intentionally propagates newly reached terminal
    hooks while work is active. This delivery-only boundary has no work left
    to stop and must still attempt the companion and final notification.
    """
    def describe():
        try:
            name = type(error).__name__
        except BaseException:
            name = "Exception"
        if type(name) is not str:
            name = "Exception"
        try:
            return f"{name}: {error}"[:MAX_ERROR]
        except BaseException:
            return f"{name}: message unavailable"[:MAX_ERROR]
    return fib.without_authority(describe)()


def _attach_failure_detail(error, name, value):
    def attach():
        try:
            setattr(error, name, value)
        except BaseException:
            # Diagnostics also exist in the companion/final event. A hostile
            # exception object cannot veto delivery of the primary failure.
            pass
    fib.without_authority(attach)()


def _notify_done(progress, completion, evidence, error, run_id):
    def notify():
        # Both inputs are detached before persistence. Never revisit the raw
        # body completion here, including when detachment itself failed.
        result = dict(completion.get("result") or {"run_id": run_id})
        result["fetch_ledger"] = evidence
        if error is not None:
            result["error"] = _completion_error(error)
        delivered = False
        if progress is not None:
            try:
                event = dict(completion.get("done_event") or {"seq": 0})
                event.update(type="done", run_id=result.get("run_id"), result=result)
                progress(event)
                delivered = True
            except BaseException:
                pass
        if error is not None:
            _attach_failure_detail(error, "fix_data_reported", delivered)
    fib.without_authority(notify)()


@fib.worker_scope
def _fixed_fill(adapter, root, ticker, interval, days, *, cancel, progress, manifest_lock,
                run_id, completion):
    import stock_ibkr as sk
    return sk.fill_missing_days(adapter, root, ticker, interval, days,
        cancel=cancel, progress=progress, manifest_lock=manifest_lock,
        run_id=run_id, _completion=completion,
        _fetch_child=fops.narrow_worker_child(fib.current_worker(), "fill-" + uuid.uuid4().hex,
                                             rights=fops._SHARED))


@fib.worker_scope
def _fixed_audit(adapter, root, ticker, interval, cache_root=None, *, cancel=None,
                 _reference_opener=None):
    """The only request-owning combined audit inside the repair parent."""
    import live_combined_flags as combined
    import stock_ibkr as sk
    event = (sk._CallableCancel(cancel) if callable(cancel)
             and not hasattr(cancel, "is_set") else cancel)
    return combined._audit_ticker(adapter, root, ticker, interval,
        require_persisted=True, cancel=event, _reference_opener=_reference_opener,
        _fetch_child=fops.narrow_worker_child(
            fib.current_worker(), "combined-audit-" + uuid.uuid4().hex,
            rights={"ibkr.choke.qualify",
                    "ibkr.live_combined_flags.minute_month",
                    "ibkr.live_combined_flags.daily_refetch",
                    "http.stockanalysis.validation"}))


@fib.worker_scope
def _fixed_reconcile(adapter, pacer, root, row, run_id, *, cancel, progress, manifest_lock):
    import vol_value_reconcile
    return vol_value_reconcile.reconcile_one(adapter, pacer, root, row, run_id,
        cancel=cancel, progress=progress, manifest_lock=manifest_lock,
        _fetch_child=fops.narrow_worker_child(fib.current_worker(), "reconcile-" + uuid.uuid4().hex,
                                             rights={"ibkr.vol_value_bank.ratio_day"}))


@fib.worker_scope
def _fixed_probe(adapter, root, ticker, selection, *, cancel, pacer):
    import live_spot_probe
    day = fib.without_authority(lambda: _plain_data(selection["day"]))()
    return live_spot_probe.run_embedded_probe(root, ticker=ticker, day=day,
        interval="1m", adapter=adapter, cancel=cancel, pacer=pacer,
        _fetch_child=fops.narrow_worker_child(fib.current_worker(), "probe-" + uuid.uuid4().hex,
                                             rights={"ibkr.live_spot_probe.minute"}))


MAX_ERROR = 500
MAX_RECONCILE_ITEMS = 500_000
MAX_RECONCILE_RESULTS = 4_096
MAX_COMMIT_EVIDENCE_BYTES = 24 * 1024
PAUSE_UNIT_BOUNDARIES = True
PROBE_WAIT_STATUS = True
PROBE_WAIT_HEARTBEAT_S = 2.0
CONNECT_ATTEMPTS = 3
CONNECT_RETRY_BACKOFF_S = 0.25

_PROBE_WAIT_POLL_S = 0.05
_PROBE_WAIT_INITIAL_S = 0.05
_RECONCILE_STATUSES = frozenset({
    "settled", "corrected", "unresolved", "stale", "cancelled",
    "ambiguous",
})
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_ATOMIC_RESULT_FIELDS = frozenset({
    "commit_state", "rollback_verified", "bank_write_completed",
    "bank_written", "correction_recorded", "commit_evidence",
    "bank_committed",
})
_ATOMIC_REQUIRED_FIELDS = _ATOMIC_RESULT_FIELDS - {"bank_committed"}
_COMMIT_EVIDENCE_FIELDS = frozenset({
    "rollback_verified", "month_write_completed", "bank_written",
    "correction_recorded", "month_state", "manifest_state",
    "original_month_file", "written_month_file", "old_month_sha256",
    "new_month_sha256", "observed_original_sha256",
    "observed_written_sha256", "correction", "restore_errors",
})
_CORRECTION_FIELDS = frozenset({
    "version", "type", "day", "run", "source", "confirmed",
    "what_to_show", "request_count", "bars", "old_value", "new_value",
    "old_day_sha256", "new_day_sha256", "old_month_sha256",
    "new_month_sha256", "reason", "reasons",
})
_MONTH_STATES = frozenset({
    "original", "corrected", "corrected-active-original-retained",
    "indeterminate",
})
_MANIFEST_STATES = frozenset({
    "original", "unreadable", "corrected", "other-valid", "malformed",
})
_VOL_ANOMALY_REASONS = frozenset({
    "hard_nonfinite", "hard_negative", "hard_ceiling", "hard_ohlc",
    "hard_volume", "unit_flip_month", "jump_up", "jump_down",
    "iv_zero_run", "iv_hvol_band",
})
_VOL_WHAT_TO_SHOW = frozenset({
    "OPTION_IMPLIED_VOLATILITY", "HISTORICAL_VOLATILITY",
})


class _ScanPauseCancelled(BaseException):
    """Abort a scan hold without being swallowed by progress observers."""


def _error(exc):
    def format_error():
        try:
            name = type(exc).__name__
        except _TERMINAL:
            if not isinstance(exc, _TERMINAL):
                raise  # Newly reached terminal diagnostics stop ordinary work.
            name = "Exception"
        except BaseException:
            name = "Exception"
        try:
            return f"{name}: {exc}"[:MAX_ERROR]
        except _TERMINAL:
            if not isinstance(exc, _TERMINAL):
                raise
            return f"{name}: message unavailable"[:MAX_ERROR]
        except BaseException:
            return f"{name}: message unavailable"[:MAX_ERROR]
    return fib.without_authority(format_error)()


def _result(run_id):
    return {
        "run_id": run_id,
        "added": 0, "absent": 0, "checked": 0, "flagged": 0,
        "still": 0, "cancelled": False, "error": None,
        "port_errors": [], "fill_series": 0, "acc_series": 0,
        "halted": 0, "unprocessed": [],
        "fill_completions": [],
        "coverage_forward": 0, "coverage_front": 0,
        "coverage_unknown": 0, "coverage_report": None,
        "coverage_queue": [], "health_identity": 0,
        "health_frontier": 0, "health_calendar": 0,
        "health_triage": 0, "health_split_repair": 0,
        "health_split_confirm": 0, "health_split_operational": 0,
        "health_split_actions": [], "split_report": None,
        "health_report": None, "health_queue": [],
        "probe_seed": None, "probe_selected": 0, "probe_completed": 0,
        "probe_request_count": 0, "probe_reused_request_count": 0,
        "probe_issues": 0, "probe_rows": [], "probe_report": None,
        "reconcile_selected": 0, "reconcile_completed": 0,
        "reconcile_candidate_tickers": 0,
        "reconcile_planned_tickers": 0, "reconcile_plan_failed": 0,
        "reconcile_selection_unknown": 0,
        "reconcile_request_count": 0, "reconcile_settled": 0,
        "reconcile_request_unknown": 0,
        "reconcile_corrected": 0, "reconcile_unresolved": 0,
        "reconcile_queue_resolved": 0, "reconcile_queue_pending": 0,
        "reconcile_rows": [],
        "reconcile_rows_truncated": 0,
        "reconcile_report": None, "reconcile_error": None,
    }


def _health_fields(out, health):
    if not isinstance(health, dict):
        return
    for key in tuple(out):
        if key.startswith(("health_", "coverage_")) or key == "split_report":
            if key in health:
                out[key] = health[key]


def _reconcile_identity(row):
    """Return the exact durable identity for one detached reconcile row."""
    if not isinstance(row, dict):
        raise TypeError("reconcile plan rows must be objects")
    ticker = str(row.get("ticker") or "").strip().upper()
    kind = str(row.get("kind_token") or row.get("kind") or "").strip()
    day = str(row.get("day") or "").strip()
    if not ticker or not kind or not day:
        raise ValueError("reconcile plan row identity is incomplete")
    return ticker, kind, day


def _reconcile_debt(row, reason):
    ticker, kind, day = _reconcile_identity(row)
    return ticker, kind, day, "reconcile", str(reason)


def _reconcile_items(value, *, ticker=None):
    """Validate, detach, and de-duplicate a callback's bounded work plan."""
    if value is None:
        return []
    if isinstance(value, dict):
        if "items" in value:
            value = value["items"]
        elif "rows" in value:
            value = value["rows"]
        else:
            raise TypeError("reconcile plan object must contain items or rows")
    if isinstance(value, (str, bytes)):
        raise TypeError("reconcile plan must be an iterable of objects")
    try:
        rows = [dict(row) for row in islice(
            iter(value), MAX_RECONCILE_ITEMS + 1)]
    except (TypeError, ValueError) as exc:
        raise TypeError("reconcile plan must be an iterable of objects") from exc
    if len(rows) > MAX_RECONCILE_ITEMS:
        raise ValueError("reconcile plan exceeds the bounded item limit")
    seen = set()
    expected_ticker = (None if ticker is None
                       else str(ticker).strip().upper())
    for row in rows:
        key = _reconcile_identity(row)
        if expected_ticker is not None and key[0] != expected_ticker:
            raise ValueError(
                "post-repair reconcile plan escaped its ticker scope")
        if key in seen:
            raise ValueError(f"duplicate reconcile plan row: {' '.join(key)}")
        seen.add(key)
    return rows


def _reconcile_failure(row, exc, *, ambiguous=True):
    ticker, kind, day = _reconcile_identity(row)
    result = {
        "status": "ambiguous" if ambiguous else "unresolved",
        "ticker": ticker, "kind": kind,
        "kind_token": kind, "day": day, "request_count": 0,
        "queue_resolved": False, "error": _error(exc),
    }
    if ambiguous:
        result["request_count_unknown"] = True
    return result


def _commit_text(value, label, *, limit=320):
    if (not isinstance(value, str) or not value
            or len(value) > limit or any(c in value for c in "\r\n\x00")):
        raise ValueError(f"reconcile {label} is invalid")
    return value


def _commit_sha(value, label, *, nullable=False):
    if value is None and nullable:
        return None
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"reconcile {label} is invalid")
    return value


def _commit_number(value, label, *, nullable=False):
    if value is None and nullable:
        return None
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(float(value))):
        raise ValueError(f"reconcile {label} must be finite")
    return float(value)


def _commit_reasons(reason, reasons):
    if (not isinstance(reason, str) or not reason or len(reason) > 320
            or any(c in reason for c in "\r\n\x00")
            or not isinstance(reasons, list) or not reasons or len(reasons) > 16
            or any(not isinstance(item, str) or not item
                   or len(item) > 320 or any(c in item for c in "\r\n\x00")
                   for item in reasons)
            or reasons != sorted(set(reasons))
            or any(item not in _VOL_ANOMALY_REASONS for item in reasons)
            or reason != ",".join(reasons)):
        raise ValueError("reconcile atomic reason fields are invalid")
    return reason, list(reasons)


def _commit_correction(value, *, normalized):
    if not isinstance(value, dict):
        raise ValueError("reconcile correction evidence fields are invalid")
    value = dict(value)
    if set(value) != _CORRECTION_FIELDS:
        raise ValueError("reconcile correction evidence fields are invalid")
    if value.get("version") != 1 \
            or value.get("type") != "vol_value_refetch_reconcile":
        raise ValueError("reconcile correction evidence version/type is invalid")
    if value.get("day") != normalized["day"]:
        raise ValueError("reconcile correction day disagrees")
    if value.get("request_count") != 1:
        raise ValueError("reconcile correction request count is invalid")
    bars = value.get("bars")
    if type(bars) is not int or not 0 < bars <= 2_000_000:
        raise ValueError("reconcile correction bar count is invalid")
    reason, reasons = _commit_reasons(
        value.get("reason"), value.get("reasons"))
    if (normalized.get("reason") != reason
            or normalized.get("reasons") != reasons):
        raise ValueError("reconcile correction provenance disagrees")
    what = _commit_text(
        value.get("what_to_show"), "correction what_to_show", limit=64)
    if what not in _VOL_WHAT_TO_SHOW:
        raise ValueError("reconcile correction what_to_show is invalid")
    return {
        "version": 1,
        "type": "vol_value_refetch_reconcile",
        "day": normalized["day"],
        "run": _commit_text(value.get("run"), "correction run"),
        "source": _commit_text(value.get("source"), "correction source"),
        "confirmed": _commit_text(
            value.get("confirmed"), "correction confirmed"),
        "what_to_show": what,
        "request_count": 1,
        "bars": bars,
        "old_value": _commit_number(
            value.get("old_value"), "correction old_value", nullable=True),
        "new_value": _commit_number(
            value.get("new_value"), "correction new_value"),
        "old_day_sha256": _commit_sha(
            value.get("old_day_sha256"), "correction old-day SHA-256"),
        "new_day_sha256": _commit_sha(
            value.get("new_day_sha256"), "correction new-day SHA-256"),
        "old_month_sha256": _commit_sha(
            value.get("old_month_sha256"), "correction old-month SHA-256"),
        "new_month_sha256": _commit_sha(
            value.get("new_month_sha256"), "correction new-month SHA-256"),
        "reason": reason,
        "reasons": reasons,
    }


def _retain_atomic_commit(result, normalized):
    """Retain only the exact bounded rollback/current-state evidence schema."""
    present = set(result).intersection(_ATOMIC_REQUIRED_FIELDS)
    if not present:
        return
    if not _ATOMIC_REQUIRED_FIELDS.issubset(result):
        raise ValueError("reconcile atomic commit evidence is incomplete")
    state = result.get("commit_state")
    if (not isinstance(state, str)
            or state not in {"rolled_back", "recovery_required"}):
        raise ValueError("reconcile atomic commit state is invalid")
    if (normalized["request_count"] != 1 or normalized["queue_resolved"]
            or normalized["status"] != (
                "unresolved" if state == "rolled_back" else "ambiguous")):
        raise ValueError("reconcile atomic commit outcome is contradictory")
    rollback = result.get("rollback_verified")
    month_written = result.get("bank_write_completed")
    bank_written = result.get("bank_written")
    recorded = result.get("correction_recorded")
    if (type(rollback) is not bool or type(month_written) is not bool
            or (bank_written is not None and type(bank_written) is not bool)
            or (recorded is not None and type(recorded) is not bool)):
        raise ValueError("reconcile atomic commit truth fields are invalid")
    if rollback != (state == "rolled_back"):
        raise ValueError("reconcile atomic rollback state disagrees")
    committed_present = "bank_committed" in result
    if isinstance(bank_written, bool):
        if (not committed_present
                or result.get("bank_committed") is not bank_written):
            raise ValueError("reconcile atomic bank commit truth disagrees")
    elif committed_present:
        raise ValueError("indeterminate bank state cannot claim bank_committed")
    evidence = result.get("commit_evidence")
    if not isinstance(evidence, dict):
        raise ValueError("reconcile commit_evidence fields are invalid")
    evidence = dict(evidence)
    if set(evidence) != _COMMIT_EVIDENCE_FIELDS:
        raise ValueError("reconcile commit_evidence fields are invalid")
    if (evidence.get("rollback_verified") is not rollback
            or evidence.get("month_write_completed") is not month_written
            or evidence.get("bank_written") is not bank_written
            or evidence.get("correction_recorded") is not recorded):
        raise ValueError("reconcile commit_evidence truth disagrees")
    month_state = evidence.get("month_state")
    manifest_state = evidence.get("manifest_state")
    if (not isinstance(month_state, str) or month_state not in _MONTH_STATES
            or not isinstance(manifest_state, str)
            or manifest_state not in _MANIFEST_STATES):
        raise ValueError("reconcile commit_evidence state is invalid")
    expected_bank_written = {
        "original": False,
        "corrected": True,
        "corrected-active-original-retained": True,
        "indeterminate": None,
    }[month_state]
    expected_correction_recorded = {
        "original": False,
        "corrected": True,
        "other-valid": False,
        "unreadable": None,
        "malformed": None,
    }[manifest_state]
    if (bank_written is not expected_bank_written
            or recorded is not expected_correction_recorded):
        raise ValueError("reconcile commit_evidence state/truth disagrees")
    correction = _commit_correction(
        evidence.get("correction"), normalized=normalized)
    errors = evidence.get("restore_errors")
    if (not isinstance(errors, list) or len(errors) > 4
            or any(not isinstance(item, str) or len(item) > 320
                   or any(c in item for c in "\r\n\x00") for item in errors)):
        raise ValueError("reconcile restore error evidence is invalid")
    original_file = _commit_text(
        evidence.get("original_month_file"), "original month filename",
        limit=255)
    written_file = _commit_text(
        evidence.get("written_month_file"), "written month filename",
        limit=255)
    if any(c in original_file + written_file for c in "/\\"):
        raise ValueError("reconcile month evidence filename is invalid")
    safe = {
        "rollback_verified": rollback,
        "month_write_completed": month_written,
        "bank_written": bank_written,
        "correction_recorded": recorded,
        "month_state": month_state,
        "manifest_state": manifest_state,
        "original_month_file": original_file,
        "written_month_file": written_file,
        "old_month_sha256": _commit_sha(
            evidence.get("old_month_sha256"), "old month SHA-256"),
        "new_month_sha256": _commit_sha(
            evidence.get("new_month_sha256"), "new month SHA-256"),
        "observed_original_sha256": _commit_sha(
            evidence.get("observed_original_sha256"),
            "observed original SHA-256", nullable=True),
        "observed_written_sha256": _commit_sha(
            evidence.get("observed_written_sha256"),
            "observed written SHA-256", nullable=True),
        "correction": correction,
        "restore_errors": list(errors),
    }
    if (correction["old_month_sha256"] != safe["old_month_sha256"]
            or correction["new_month_sha256"] != safe["new_month_sha256"]):
        raise ValueError("reconcile commit_evidence correction SHA disagrees")
    if state == "rolled_back" and not (
            bank_written is False and recorded is False
            and month_state == manifest_state == "original"):
        raise ValueError("verified rollback evidence is contradictory")
    try:
        encoded = json.dumps(
            safe, ensure_ascii=False, allow_nan=False,
            separators=(",", ":"), sort_keys=True).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("reconcile commit_evidence is not JSON-safe") from exc
    if len(encoded) > MAX_COMMIT_EVIDENCE_BYTES:
        raise ValueError("reconcile commit_evidence exceeds its byte limit")
    normalized.update({
        "commit_state": state,
        "rollback_verified": rollback,
        "bank_write_completed": month_written,
        "bank_written": bank_written,
        "correction_recorded": recorded,
        "commit_evidence": safe,
    })


def _reconcile_result(row, value):
    """Normalize callback evidence without allowing identity/counter lies."""
    expected = _reconcile_identity(row)
    if not isinstance(value, dict):
        return _reconcile_failure(
            row, TypeError("reconcile callback returned a non-object result"))
    result = dict(value)
    actual = (
        str(result.get("ticker") or "").strip().upper(),
        str(result.get("kind_token") or result.get("kind")
            or "").strip(),
        str(result.get("day") or "").strip(),
    )
    if actual != expected:
        return _reconcile_failure(
            row, ValueError("reconcile callback changed the planned identity"))
    request_count = result.get("request_count")
    if type(request_count) is not int:
        return _reconcile_failure(
            row, ValueError("reconcile request_count must be an integer"))
    if request_count not in (0, 1):
        return _reconcile_failure(
            row, ValueError("reconcile request_count must be 0 or 1"))
    request_unknown = result.get("request_count_unknown", False)
    if not isinstance(request_unknown, bool):
        return _reconcile_failure(
            row, ValueError(
                "reconcile request_count_unknown must be boolean"))
    if request_unknown and request_count:
        return _reconcile_failure(
            row, ValueError(
                "reconcile request count cannot be both known and unknown"))
    status = str(result.get("status") or "").strip().lower()
    if status not in _RECONCILE_STATUSES:
        return _reconcile_failure(
            row, ValueError("reconcile status is invalid"))
    queue_resolved = result.get("queue_resolved")
    if not isinstance(queue_resolved, bool):
        return _reconcile_failure(
            row, ValueError("reconcile queue_resolved must be boolean"))
    if queue_resolved and status not in {"settled", "corrected"}:
        return _reconcile_failure(
            row, ValueError("only settled/corrected rows resolve queue debt"))
    if status in {"settled", "corrected"} and request_count != 1:
        return _reconcile_failure(
            row, ValueError("settled/corrected rows require one request"))
    normalized = {
        "status": status, "ticker": expected[0], "kind": expected[1],
        "kind_token": expected[1], "day": expected[2],
        "request_count": request_count,
        "request_count_unknown": request_unknown,
        "queue_resolved": queue_resolved,
    }
    error = result.get("error")
    if error is not None:
        if not isinstance(error, str):
            return _reconcile_failure(
                row, ValueError("reconcile error evidence must be text"))
        normalized["error"] = error[:MAX_ERROR]
    for field in ("stored_value", "served_value"):
        if field not in result:
            continue
        value = result[field]
        if field == "stored_value" and value is None:
            normalized[field] = None
            continue
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(float(value))):
            return _reconcile_failure(
                row, ValueError(f"reconcile {field} must be finite"))
        normalized[field] = float(value)
    for field in (
            "same_value", "same_day", "bank_committed",
            "registry_committed"):
        if field not in result:
            continue
        if not isinstance(result[field], bool):
            return _reconcile_failure(
                row, ValueError(f"reconcile {field} must be boolean"))
        normalized[field] = result[field]
    if status == "settled" and normalized.get("registry_committed") is not True:
        return _reconcile_failure(
            row, ValueError(
                "settled reconcile rows require a committed registry entry"))
    if status != "settled" and normalized.get("registry_committed") is True:
        return _reconcile_failure(
            row, ValueError(
                "reconcile registry commit truth disagrees with status"))
    if status == "corrected" and normalized.get("bank_committed") is not True:
        return _reconcile_failure(
            row, ValueError(
                "corrected reconcile rows require committed bank bytes"))
    if queue_resolved and not (
            (status == "settled"
             and normalized.get("registry_committed") is True)
            or (status == "corrected"
                and normalized.get("bank_committed") is True)):
        return _reconcile_failure(
            row, ValueError(
                "reconcile queue retirement lacks durable action proof"))
    if "what_to_show" in result:
        value = result["what_to_show"]
        if (not isinstance(value, str) or not value
                or len(value) > 64 or any(c in value for c in "\r\n\x00")):
            return _reconcile_failure(
                row, ValueError("reconcile what_to_show is invalid"))
        normalized["what_to_show"] = value
    if "month_sha256" in result:
        value = result["month_sha256"]
        if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
            return _reconcile_failure(
                row, ValueError("reconcile month_sha256 is invalid"))
        normalized["month_sha256"] = value
    if "reason" in result:
        value = result["reason"]
        if (not isinstance(value, str) or len(value) > 320
                or any(c in value for c in "\r\n\x00")):
            return _reconcile_failure(
                row, ValueError("reconcile reason is invalid"))
        normalized["reason"] = value
    if "reasons" in result:
        value = result["reasons"]
        if (not isinstance(value, list) or len(value) > 16
                or any(not isinstance(item, str) or not item
                       or len(item) > 320
                       or any(c in item for c in "\r\n\x00")
                       for item in value)
                or value != sorted(set(value))):
            return _reconcile_failure(
                row, ValueError("reconcile reasons are invalid"))
        normalized["reasons"] = list(value)
    if ("reason" in result) != ("reasons" in result):
        return _reconcile_failure(
            row, ValueError("reconcile reason fields must travel together"))
    if ("reason" in normalized and "reasons" in normalized
            and (normalized["reason"] != ",".join(normalized["reasons"])
                 or any(item not in _VOL_ANOMALY_REASONS
                        for item in normalized["reasons"]))):
        return _reconcile_failure(
            row, ValueError("reconcile reason fields disagree"))
    try:
        _retain_atomic_commit(result, normalized)
    except _TERMINAL:
        raise
    except Exception as exc:  # noqa: BLE001 - hostile callback evidence
        return _reconcile_failure(row, exc)
    if (normalized.get("bank_committed") is True
            and status != "corrected"
            and "commit_state" not in normalized):
        return _reconcile_failure(
            row, ValueError(
                "bank commit proof requires correction/recovery evidence"))
    return normalized


@fib.worker_scope
def _run_fix_data_body(*, root, ports, adapter_factory, scan_fn, health_fn=None, fill_fn,
        audit_fn, series_fn, refresh_fn, progress=None, pause_event=None,
        cancel_event=None, cache_root=None,
        run_id=None, sleep_fn=time.sleep, probe_plan_fn=None, probe_fn=None,
        probe_report_fn=None, probe_seed=None, verification_fn=None,
        kinds=("",), reconcile_plan_fn=None, reconcile_fn=None,
        reconcile_pacer_factory=None, reconcile_report_fn=None,
        port_recover_fn=None, engine_tasks=False, _terminal, _completion,
        _reference_opener=None):
    """Run scan, then overlap repair/reconcile/check/probe ticker chains."""
    run_id = str(run_id or uuid.uuid4().hex)
    out = _result(run_id)
    _completion["result"] = out
    ports = tuple(dict.fromkeys(int(port) for port in ports))
    if kinds is None:
        pass
    elif isinstance(kinds, str):
        kinds = (kinds,)
    elif isinstance(kinds, bytes):
        raise TypeError("kinds must contain text values, not bytes")
    else:
        kinds = tuple(kinds)
    pause_event = pause_event or threading.Event()
    cancel_event = cancel_event or threading.Event()
    callback = progress or (lambda _event: None)
    emit_lock = threading.Lock()
    seq = [0]

    if not engine_tasks and (reconcile_plan_fn is None) != (reconcile_fn is None):
        raise TypeError(
            "reconcile_plan_fn and reconcile_fn must be supplied together")
    for name, value in (
            ("reconcile_plan_fn", reconcile_plan_fn),
            ("reconcile_fn", reconcile_fn),
            ("reconcile_pacer_factory", reconcile_pacer_factory),
            ("reconcile_report_fn", reconcile_report_fn)):
        if value is not None and not callable(value):
            raise TypeError(f"{name} must be callable")

    def verify(stage, ticker, interval):
        if verification_fn is None:
            return
        try:
            verification_fn(stage, ticker, interval)
        except _TERMINAL:
            raise
        except Exception:
            pass

    def emit(kind, **fields):
        with emit_lock:
            seq[0] += 1
            event = {"run_id": run_id, "seq": seq[0], "type": kind}
            event.update(fields)
            try:
                callback(event)
            except _TERMINAL as exc:
                # A progress failure must stop admission without throwing out
                # of a worker's bookkeeping/finally and skipping child joins.
                _terminal.fail(exc)
            except Exception:
                pass

    if not ports:
        out["error"] = "no live TWS ports"
        emit("done", result=out)
        return out

    try:
        lease = operation_gate.acquire("fetch", owner="Fix data run")
    except operation_gate.OperationBusy as exc:
        out["error"] = str(exc)[:MAX_ERROR]
        emit("blocked", error=out["error"])
        emit("done", result=out)
        return out
    except _TERMINAL:
        raise
    except Exception as exc:
        out["error"] = _error(exc)
        emit("done", result=out)
        return out

    touched = []
    touched_lock = threading.Lock()
    cancel_allowed = threading.Event()

    def should_cancel():
        return _terminal.stop.is_set() or (cancel_event.is_set() and cancel_allowed.is_set())

    def pause_boundary():
        if not pause_event.is_set():
            return not should_cancel()
        cancel_allowed.set()
        emit("substate", state="paused")
        while pause_event.is_set() and not should_cancel():
            sleep_fn(0.01)
        if should_cancel():
            return False
        if not cancel_event.is_set():
            cancel_allowed.clear()
        emit("substate", state="running")
        return True

    def record_probe(row):
        value = dict(row) if isinstance(row, dict) else {
            "verdict": "PROBE_ERROR", "pass_equivalent": False,
            "error": {"type": "TypeError",
                      "message": "probe returned a non-object result"},
        }
        with touched_lock:
            out["probe_rows"].append(value)
            out["probe_completed"] += 1
            out["probe_request_count"] += int(
                value.get("request_count") or 0)
            out["probe_reused_request_count"] += int(
                value.get("reused_request_count") or 0)
            if value.get("pass_equivalent") is not True:
                out["probe_issues"] += 1

    def record_reconcile(planned, value):
        normalized = _reconcile_result(planned, value)
        with touched_lock:
            if len(out["reconcile_rows"]) < MAX_RECONCILE_RESULTS:
                out["reconcile_rows"].append(normalized)
            else:
                out["reconcile_rows_truncated"] += 1
            out["reconcile_completed"] += 1
            out["reconcile_request_count"] += normalized["request_count"]
            out["reconcile_request_unknown"] += int(
                normalized.get("request_count_unknown") is True)
            if normalized["status"] == "settled":
                out["reconcile_settled"] += 1
            elif normalized["status"] == "corrected":
                out["reconcile_corrected"] += 1
            else:
                out["reconcile_unresolved"] += 1
            if normalized["queue_resolved"]:
                out["reconcile_queue_resolved"] += 1
            else:
                out["reconcile_queue_pending"] += 1
        return normalized

    def run_scheduler(fill_items, audit_items, reconcile_items, probe_items):
        """Dispatch ticker chains with repair priority and durable handoffs.

        ``reconcile_items`` is deliberately candidate discovery only.  A
        ticker's exact queue slice is audited and planned again after all of
        that ticker's fills, so pre-repair detector context can never drive a
        source request or settlement.
        """
        ticker_rows = {}
        order = []

        def row_for(ticker):
            key = str(ticker).upper()
            if key not in ticker_rows:
                ticker_rows[key] = {
                    "ticker": ticker, "fills": [], "checks": [],
                    "reconciles": [], "reconcile_candidate": False,
                    "reconcile_plan_current": False,
                    "reconcile_plan_attempted": False, "probe": None,
                    "state": "SCANNED", "active": False,
                    "owner_lock": threading.Lock(),
                    "manifest_lock": threading.Lock(),
                }
                order.append(key)
            return ticker_rows[key]

        for ticker, interval, days in fill_items:
            row = row_for(ticker)
            row["fills"].append((ticker, interval, days))
            # When volatility reconciliation is enabled, every repaired
            # ticker is a candidate even if the pre-repair audit found no row:
            # a fill may introduce a new detector finding.
            if reconcile_plan_fn is not None:
                row["reconcile_candidate"] = True
        for ticker, interval in audit_items:
            row_for(ticker)["checks"].append((ticker, interval))
        for item in reconcile_items:
            ticker, _kind, _day = _reconcile_identity(item)
            # Pre-repair rows select a ticker only.  They are intentionally not
            # retained as executable rows; the post-repair replan replaces the
            # ticker slice from durable current evidence.
            row_for(ticker)["reconcile_candidate"] = True
        for ticker, selection in probe_items.items():
            row_for(ticker)["probe"] = selection

        repair_ready = deque()
        reconcile_ready = deque()
        check_ready = deque()
        probe_ready = deque()
        for key in order:
            row = ticker_rows[key]
            if row["fills"]:
                row["state"] = "REPAIRING"
                repair_ready.append(key)
            elif row["reconcile_candidate"]:
                row["state"] = "READY_TO_RECONCILE"
                reconcile_ready.append(key)
            else:
                row["state"] = "REPAIR_NOT_NEEDED"
                check_ready.append(key)

        lock = threading.Lock()
        reconcile_publish_lock = threading.Lock()
        changed = threading.Condition(lock)
        states = {port: "idle" for port in ports}
        raw_states = dict(states)
        status_meta = {
            port: {"key": ("plain", "idle"), "base": "idle",
                   "mode": "plain", "ticker": None, "interval": None,
                   "day": None,
                   "since": time.monotonic(), "heartbeat": False,
                   "next": float("inf")}
            for port in ports
        }
        ports_emit_lock = threading.Lock()
        out["reconcile_candidate_tickers"] = sum(
            row["reconcile_candidate"] for row in ticker_rows.values())
        totals = {"fill": len(fill_items),
                  # Reconcile row totals become known only after each
                  # post-repair ticker replan succeeds.
                  "reconcile": 0,
                  "audit": len(audit_items),
                  "probe": len(probe_items)}
        done = {"fill": 0, "reconcile": 0, "audit": 0, "probe": 0}
        active = 0
        active_reconciles = 0
        active_checks = 0
        active_probes = 0

        def heartbeat_interval():
            try:
                return max(0.02, float(PROBE_WAIT_HEARTBEAT_S))
            except (TypeError, ValueError):
                return 2.0

        def render_status_locked(meta, now):
            if meta["mode"] == "probe":
                base = (f"spot-probe {meta['ticker']} "
                        f"({done['probe']}/{totals['probe']})")
            elif meta["mode"] == "reconcile-plan":
                base = f"value-replan {meta['ticker']}"
            elif meta["mode"] == "reconcile":
                base = (f"value-reconcile {meta['ticker']} "
                        f"{meta['interval']} {meta['day']} "
                        f"({done['reconcile']}/{totals['reconcile']})")
            elif meta["mode"] == "check":
                base = f"cross-check {meta['ticker']} {meta['interval']}"
            else:
                base = meta["base"]
            if not meta["heartbeat"]:
                return base
            return f"{base} · {max(0, int(now - meta['since']))}s"

        def update_port_locked(port, state, *, heartbeat=False, mode="plain",
                               ticker=None, interval=None, day=None,
                               notify=True):
            now = time.monotonic()
            state = str(state)
            key = (str(mode), state, ticker, interval, day)
            meta = status_meta[port]
            semantic_change = (meta["key"] != key
                               or meta["heartbeat"] != bool(heartbeat))
            if semantic_change:
                meta.update({
                    "key": key, "base": state, "mode": str(mode),
                    "ticker": ticker, "interval": interval, "since": now,
                    "day": day,
                    "heartbeat": bool(heartbeat),
                    "next": (now + min(_PROBE_WAIT_INITIAL_S,
                                        heartbeat_interval())
                             if heartbeat else float("inf")),
                })
            raw_states[port] = state
            states[port] = render_status_locked(meta, now)
            safe = all(value in ("paused", "done")
                       for value in raw_states.values())
            if pause_event.is_set() and safe:
                cancel_allowed.set()
            elif not pause_event.is_set() and not cancel_event.is_set():
                cancel_allowed.clear()
            if notify:
                changed.notify_all()
            return safe, semantic_change

        def emit_ports(repeat=1):
            # Serialize and recapture after acquiring the publication lock.  A
            # heartbeat captured before a terminal transition can therefore
            # never publish stale work after the terminal snapshot.
            for _index in range(max(0, int(repeat))):
                with ports_emit_lock:
                    with changed:
                        snapshot = dict(states)
                    emit("ports", states=snapshot)

        def set_port(port, state, *, heartbeat=False, mode="plain",
                     ticker=None, interval=None, day=None):
            with changed:
                safe, _semantic_change = update_port_locked(
                    port, state, heartbeat=heartbeat, mode=mode,
                    ticker=ticker, interval=interval, day=day)
            emit_ports()
            return safe

        def set_waiting_locked(port, state):
            meta = status_meta[port]
            key = ("waiting", str(state), None, None, None)
            if meta["key"] == key and meta["heartbeat"]:
                return False
            update_port_locked(
                port, state, heartbeat=True, mode="waiting",
                notify=True)
            return True

        def emit_due_heartbeats():
            now = time.monotonic()
            interval = heartbeat_interval()
            due = 0
            with changed:
                for port in ports:
                    meta = status_meta[port]
                    if (not meta["heartbeat"]
                            or now + 1e-9 < meta["next"]):
                        continue
                    meta["next"] = now + interval
                    states[port] = render_status_locked(meta, now)
                    due += 1
            # Keep the existing one-event-per-port evidence convention.
            emit_ports(due)

        def boundary(port):
            if not pause_event.is_set():
                return not should_cancel()
            safe = set_port(port, "paused")
            if safe:
                emit("substate", state="paused")
            while pause_event.is_set() and not should_cancel():
                sleep_fn(0.01)
            if should_cancel():
                return False
            set_port(port, "idle")
            emit("substate", state="running")
            return True

        def waiting_state_locked():
            parts = []
            fill_running = max(
                0, active - active_reconciles - active_checks - active_probes)
            if fill_running:
                parts.append(f"{fill_running} fill chain"
                             f"{'s' if fill_running != 1 else ''} running")
            if active_checks:
                parts.append(f"{active_checks} check"
                             f"{'s' if active_checks != 1 else ''} running")
            if active_reconciles:
                parts.append(f"{active_reconciles} reconcile chain"
                             f"{'s' if active_reconciles != 1 else ''} running")
            if active_probes:
                parts.append(f"{active_probes} probe"
                             f"{'s' if active_probes != 1 else ''} running "
                             "(cap 2)")
            if repair_ready:
                parts.append(f"{len(repair_ready)} fill chain"
                             f"{'s' if len(repair_ready) != 1 else ''} queued")
            if reconcile_ready:
                parts.append(f"{len(reconcile_ready)} reconcile chain"
                             f"{'s' if len(reconcile_ready) != 1 else ''} queued")
            if check_ready:
                parts.append(f"{len(check_ready)} check"
                             f"{'s' if len(check_ready) != 1 else ''} queued")
            if probe_ready:
                parts.append(f"{len(probe_ready)} probe"
                             f"{'s' if len(probe_ready) != 1 else ''} queued")
            if totals["probe"] and (active_probes or probe_ready):
                parts.append(f"probes ({done['probe']}/{totals['probe']})")
            if not parts:
                parts.append("work in flight")
            return "waiting: " + " · ".join(parts)

        def claim(port):
            nonlocal active, active_reconciles, active_checks, active_probes
            while True:
                should_emit = False
                with changed:
                    if should_cancel():
                        return None
                    if pause_event.is_set():
                        return "pause", None
                    phase = None
                    queue = None
                    if repair_ready:
                        phase, queue = "repair", repair_ready
                    elif reconcile_ready:
                        phase, queue = "reconcile", reconcile_ready
                    elif check_ready:
                        # Keep one checker moving and cap probes at two while
                        # checks remain. With one worker checks drain first.
                        if probe_ready and active_checks and active_probes < 2:
                            phase, queue = "probe", probe_ready
                        else:
                            phase, queue = "check", check_ready
                    elif probe_ready:
                        if not active_checks or active_probes < 2:
                            phase, queue = "probe", probe_ready
                    if queue is not None:
                        key = queue.popleft()
                        row = ticker_rows[key]
                        row["active"] = True
                        active += 1
                        if phase == "reconcile":
                            active_reconciles += 1
                        elif phase == "check":
                            active_checks += 1
                        elif phase == "probe":
                            active_probes += 1
                        return phase, key
                    if active == 0:
                        return None
                    should_emit = set_waiting_locked(
                        port, waiting_state_locked())
                    if not should_emit:
                        changed.wait(_PROBE_WAIT_POLL_S)
                        continue
                # The status callback stays outside the scheduler lock.  The
                # next iteration rechecks all queues before entering the same
                # 50 ms condition wait, so this does not lose a work wakeup.
                emit_ports()

        def publish(key, phase):
            nonlocal active, active_reconciles, active_checks, active_probes
            with changed:
                row = ticker_rows[key]
                row["active"] = False
                active -= 1
                if phase == "repair":
                    if row["reconcile_candidate"]:
                        row["state"] = "READY_TO_RECONCILE"
                        reconcile_ready.append(key)
                    else:
                        row["state"] = "READY_TO_CHECK"
                        check_ready.append(key)
                elif phase == "reconcile":
                    active_reconciles -= 1
                    row["state"] = "READY_TO_CHECK"
                    check_ready.append(key)
                elif phase == "check" and row["probe"] is not None:
                    active_checks -= 1
                    row["state"] = "READY_TO_PROBE"
                    probe_ready.append(key)
                elif phase == "probe":
                    active_probes -= 1
                    row["state"] = "COMPLETE"
                else:
                    if phase == "check":
                        active_checks -= 1
                    row["state"] = "COMPLETE"
                changed.notify_all()

        def scheduled_debt(row, phase, *, start=0, current_reason=None,
                           later_reason="worker-failed-unstarted"):
            """Enumerate exact in-flight and downstream ticker-chain debt."""
            pending = []

            def reason(index):
                return (current_reason
                        if current_reason is not None and index == start
                        else later_reason)

            def reconcile_debt(reconcile_start, debt_reason, *, use_current):
                if not row["reconcile_candidate"]:
                    return []
                if not row["reconcile_plan_current"]:
                    return [(str(row["ticker"]).upper(), None,
                             "reconcile-plan",
                             (current_reason if use_current
                              and current_reason is not None else debt_reason))]
                return [
                    _reconcile_debt(item, (
                        current_reason if use_current
                        and current_reason is not None
                        and index == reconcile_start else debt_reason))
                    for index, item in enumerate(
                        row["reconciles"][reconcile_start:], reconcile_start)
                ]

            if phase == "repair":
                pending.extend(
                    (str(ticker).upper(), str(interval), "repair",
                     reason(index))
                    for index, (ticker, interval, _days) in enumerate(
                        row["fills"][start:], start))
                pending.extend(reconcile_debt(
                    0, later_reason, use_current=False))
                pending.extend(
                    (str(ticker).upper(), str(interval), "check",
                     later_reason)
                    for ticker, interval in row["checks"])
            elif phase == "reconcile":
                pending.extend(reconcile_debt(
                    start, later_reason, use_current=True))
                pending.extend(
                    (str(ticker).upper(), str(interval), "check",
                     later_reason)
                    for ticker, interval in row["checks"])
            elif phase == "check":
                pending.extend(
                    (str(ticker).upper(), str(interval), "check",
                     reason(index))
                    for index, (ticker, interval) in enumerate(
                        row["checks"][start:], start))
            elif phase == "probe":
                pending.append((str(row["ticker"]).upper(), None, "probe",
                                current_reason or later_reason))
                return pending
            if phase in ("repair", "reconcile", "check") \
                    and row["probe"] is not None:
                pending.append((str(row["ticker"]).upper(), None, "probe",
                                later_reason))
            return pending

        def cancel_active(key, phase, *, fill_start=0, reconcile_start=0,
                          check_start=0):
            """Retire one claimed row and enumerate work never started."""
            nonlocal active, active_reconciles, active_checks, active_probes
            row = ticker_rows[key]
            if phase == "repair":
                start = fill_start
            elif phase == "reconcile":
                start = reconcile_start
            elif phase == "check":
                start = check_start
            else:
                start = 0
            pending = scheduled_debt(
                row, phase, start=start,
                later_reason=("terminal-stop-unstarted" if _terminal.stop.is_set()
                              else "cancelled-unstarted"))
            with changed:
                row["active"] = False
                row["state"] = "CANCELLED_PARTIAL"
                active -= 1
                if phase == "reconcile":
                    active_reconciles -= 1
                elif phase == "check":
                    active_checks -= 1
                elif phase == "probe":
                    active_probes -= 1
                if not _terminal.stop.is_set():
                    out["cancelled"] = True
                out["unprocessed"].extend(pending)
                changed.notify_all()

        @fib.worker_scope
        def worker(port):
            nonlocal active, active_reconciles, active_checks, active_probes
            adapter = None
            reconcile_pacer = None
            reconcile_pacer_ready = False
            current = None
            current_index = None
            current_inflight = False
            death_state = None

            def connect_cycle(*, preserve_first_state):
                last_error = None
                for attempt in range(1, CONNECT_ATTEMPTS + 1):
                    if _terminal.stop.is_set():
                        raise _terminal.first
                    if not (preserve_first_state and attempt == 1):
                        set_port(
                            port,
                            f"connecting (attempt {attempt}/"
                            f"{CONNECT_ATTEMPTS})")
                    if attempt > 1:
                        sleep_fn(CONNECT_RETRY_BACKOFF_S)
                    try:
                        return adapter_factory(port)
                    except _TERMINAL:
                        raise
                    except Exception as exc:  # bounded connect resilience
                        last_error = exc
                assert last_error is not None
                raise last_error

            try:
                set_port(port, "connecting")
                try:
                    adapter = connect_cycle(preserve_first_state=True)
                except _TERMINAL:
                    raise
                except Exception as connect_exc:
                    recovered = False
                    if port_recover_fn is not None:
                        reason = _error(connect_exc)
                        set_port(port, "recovery requested")
                        try:
                            recovered = (
                                port_recover_fn(port, reason) is True)
                        except _TERMINAL:
                            raise
                        except BaseException as recover_exc:
                            emit("log", message=(
                                f"port {port}: recovery callback failed: "
                                f"{_error(recover_exc)}"))
                    if not recovered:
                        raise
                    # A successful recovery buys exactly one more complete
                    # bounded cycle.  It never recursively re-enters recovery.
                    adapter = connect_cycle(preserve_first_state=False)
                while boundary(port):
                    current = claim(port)
                    if current is None:
                        break
                    if current[0] == "pause":
                        continue
                    phase, key = current
                    current_index = None
                    current_inflight = False
                    row = ticker_rows[key]
                    with row["owner_lock"]:
                        if phase == "repair":
                            for fill_index, (ticker, interval, days) in enumerate(
                                    row["fills"]):
                                current_index = fill_index
                                set_port(port, f"refetch {ticker} {interval}")
                                completion = {}
                                returned = False
                                try:
                                    call = _fixed_fill if engine_tasks else fill_fn
                                    dispatch = ({"_fetch_child": fops.narrow_worker_child(
                                        fib.current_worker(), "fill-task-" + uuid.uuid4().hex,
                                        rights=fops._SHARED), "run_id": run_id,
                                        "completion": completion} if engine_tasks else {})
                                    value = call(
                                        adapter, root, ticker, interval, days,
                                        cancel=should_cancel,
                                        progress=lambda n, count, day, t=ticker,
                                        i=interval: emit("log", message=(
                                            f"fill {t} {i}: {n}/{count} {day}")),
                                        manifest_lock=row["manifest_lock"], **dispatch)
                                    with lock:
                                        out["added"] += int(value.get("added", 0))
                                        out["absent"] += len(value.get(
                                            "source_absent") or [])
                                        out["halted"] += (len(value.get("blocked") or [])
                                                          + len(value.get("unfilled") or []))
                                    with touched_lock:
                                        touched.append((ticker, interval))
                                    returned = True
                                except _TERMINAL:
                                    raise
                                except Exception as exc:
                                    with lock:
                                        out["halted"] += 1
                                    emit("log", message=(
                                        f"fill {ticker} {interval}: halted ({_error(exc)})"))
                                finally:
                                    if engine_tasks:
                                        # Only the engine-owned receipt crosses this
                                        # boundary, not exception-supplied progress.
                                        with lock:
                                            out["fill_completions"].append(completion)
                                            if not returned:
                                                out["added"] += sum(int(month.get("added", 0))
                                                    for month in completion.get("months", {}).values()
                                                    if month.get("manifest_published") is True)
                                                out["absent"] += len(completion.get("source_absent", []))
                                        if not returned and completion.get("published_months"):
                                            with touched_lock:
                                                touched.append((ticker, interval))
                                with lock:
                                    done["fill"] += 1
                                    n = done["fill"]
                                    metric = out["added"]
                                emit("refetch", done=n, total=totals["fill"],
                                     metric=metric, ticker=ticker,
                                     interval=interval, operation="repair",
                                     port=port)
                                if not boundary(port):
                                    cancel_active(
                                        key, phase,
                                        fill_start=fill_index + 1)
                                    current = None
                                    return
                            row["state"] = (
                                "READY_TO_RECONCILE"
                                if row["reconcile_candidate"]
                                else "READY_TO_CHECK")
                        elif phase == "reconcile":
                            ticker = str(row["ticker"]).upper()
                            row["state"] = "REPLANNING"
                            set_port(
                                port, f"value-replan {ticker}",
                                heartbeat=True, mode="reconcile-plan",
                                ticker=ticker)
                            plan_error = None
                            try:
                                current_inflight = True
                                current_items = _reconcile_items(
                                    reconcile_plan_fn(
                                        root, ticker=ticker, kinds=kinds),
                                    ticker=ticker)
                                with changed:
                                    if (totals["reconcile"]
                                            + len(current_items)
                                            > MAX_RECONCILE_ITEMS):
                                        raise ValueError(
                                            "post-repair reconcile plans exceed "
                                            "the bounded item limit")
                                    row["reconciles"] = current_items
                                    row["reconcile_plan_attempted"] = True
                                    row["reconcile_plan_current"] = True
                                    totals["reconcile"] += len(current_items)
                                    with touched_lock:
                                        out["reconcile_selected"] += len(
                                            current_items)
                                        out["reconcile_planned_tickers"] += 1
                                    changed.notify_all()
                                current_inflight = False
                            except _TERMINAL:
                                raise
                            except Exception as exc:
                                current_inflight = False
                                plan_error = _error(exc)
                                with changed:
                                    row["reconciles"] = []
                                    row["reconcile_plan_attempted"] = True
                                    row["reconcile_plan_current"] = False
                                    out["halted"] += 1
                                    out["unprocessed"].append((
                                        ticker, None, "reconcile-plan",
                                        "planning-failed"))
                                    changed.notify_all()
                                with touched_lock:
                                    out["reconcile_plan_failed"] += 1
                                    out["reconcile_error"] = (
                                        f"{out['reconcile_error']}; {plan_error}"
                                        if out["reconcile_error"] else plan_error
                                    )[:MAX_ERROR]
                                emit("log", message=(
                                    f"volatility post-repair replan {ticker} "
                                    f"failed: {plan_error}"))
                            if plan_error is None:
                                emit("log", message=(
                                    f"volatility post-repair replan {ticker}: "
                                    f"{len(row['reconciles'])} current row(s)"))
                                # Planning itself is an atomic unit.  Pause or
                                # cancel may take effect before the first source
                                # request, with exact current rows now known.
                                if not boundary(port):
                                    cancel_active(
                                        key, phase, reconcile_start=0)
                                    current = None
                                    return
                            row["state"] = "RECONCILING"
                            if row["reconciles"] and not reconcile_pacer_ready:
                                if reconcile_pacer_factory is not None:
                                    reconcile_pacer = reconcile_pacer_factory()
                                reconcile_pacer_ready = True
                            for reconcile_index, item in enumerate(
                                    row["reconciles"]):
                                current_index = reconcile_index
                                ticker, kind, day = _reconcile_identity(item)
                                set_port(
                                    port,
                                    f"value-reconcile {ticker} {kind} {day}",
                                    heartbeat=True, mode="reconcile",
                                    ticker=ticker, interval=kind, day=day)
                                try:
                                    current_inflight = True
                                    call = _fixed_reconcile if engine_tasks else reconcile_fn
                                    dispatch = ({"_fetch_child": fops.narrow_worker_child(
                                        fib.current_worker(), "reconcile-task-" + uuid.uuid4().hex,
                                        rights={"ibkr.vol_value_bank.ratio_day"})} if engine_tasks else {})
                                    value = call(
                                        adapter, reconcile_pacer, root,
                                        dict(item), run_id,
                                        cancel=should_cancel,
                                        manifest_lock=row["manifest_lock"],
                                        progress=lambda message, t=ticker,
                                        k=kind, d=day: emit(
                                            "log", message=(
                                                f"reconcile {t} {k} {d}: "
                                                f"{message}")), **dispatch)
                                except _TERMINAL:
                                    raise
                                except Exception as exc:
                                    value = _reconcile_failure(item, exc)
                                with reconcile_publish_lock:
                                    normalized = record_reconcile(item, value)
                                    current_inflight = False
                                    current_index = reconcile_index + 1
                                    with lock:
                                        done["reconcile"] += 1
                                        n = done["reconcile"]
                                    with touched_lock:
                                        unresolved = out["reconcile_unresolved"]
                                        settled = out["reconcile_settled"]
                                        corrected = out["reconcile_corrected"]
                                        requests = out["reconcile_request_count"]
                                    emit(
                                        "reconcile", done=n,
                                        total=totals["reconcile"],
                                        metric=unresolved, settled=settled,
                                        corrected=corrected, requests=requests,
                                        status=normalized["status"],
                                        ticker=ticker, interval=kind, day=day,
                                        operation="reconcile", port=port)
                                if not boundary(port):
                                    cancel_active(
                                        key, phase,
                                        reconcile_start=reconcile_index + 1)
                                    current = None
                                    return
                        elif phase == "check":
                            row["state"] = "CHECKING"
                            for check_index, (ticker, interval) in enumerate(
                                    row["checks"]):
                                current_index = check_index
                                set_port(
                                    port, f"cross-check {ticker} {interval}",
                                    heartbeat=True, mode="check",
                                    ticker=ticker, interval=interval)
                                try:
                                    if engine_tasks and audit_fn is None:
                                        value = _fixed_audit(
                                            adapter, root, ticker, interval, cache_root,
                                            cancel=should_cancel,
                                            _reference_opener=_reference_opener,
                                            _fetch_child=fops.narrow_worker_child(
                                                fib.current_worker(),
                                                "audit-task-" + uuid.uuid4().hex,
                                                rights={"ibkr.choke.qualify",
                                                    "ibkr.live_combined_flags.minute_month",
                                                    "ibkr.live_combined_flags.daily_refetch",
                                                    "http.stockanalysis.validation"}))
                                    else:
                                        value = audit_fn(
                                            adapter, root, ticker, interval, cache_root)
                                    with lock:
                                        out["checked"] += 1
                                        if value.get("status") == "flagged":
                                            out["flagged"] += 1
                                    current_audit = (value.get("status") not in
                                        {"error", "inconclusive", "blocked", "pending"}
                                        and value.get("_verification_current", True))
                                    if current_audit:
                                        verify("xval", ticker, interval)
                                    else:
                                        with lock:
                                            out["unprocessed"].append((ticker, interval,
                                                "check", "audit-not-current"))
                                except _TERMINAL:
                                    raise
                                except Exception as exc:
                                    with lock:
                                        out["unprocessed"].append((ticker, interval,
                                            "check", "audit-failed"))
                                    emit("log", message=(
                                        f"cross-check {ticker} {interval}: {_error(exc)}"))
                                with lock:
                                    done["audit"] += 1
                                    n = done["audit"]
                                    metric = out["flagged"]
                                emit("xcheck", done=n, total=totals["audit"],
                                     metric=metric, ticker=ticker,
                                     interval=interval, operation="check",
                                     port=port)
                                if not boundary(port):
                                    cancel_active(
                                        key, phase,
                                        check_start=check_index + 1)
                                    current = None
                                    return
                        else:
                            row["state"] = "PROBING"
                            current_index = 0
                            ticker = row["ticker"]
                            set_port(
                                port, f"spot-probe {ticker}", heartbeat=True,
                                mode="probe", ticker=ticker)
                            try:
                                if engine_tasks:
                                    value = _fixed_probe(adapter, root, ticker, row["probe"],
                                        cancel=should_cancel, pacer=reconcile_pacer,
                                        _fetch_child=fops.narrow_worker_child(fib.current_worker(),
                                            "probe-task-" + uuid.uuid4().hex,
                                            rights={"ibkr.live_spot_probe.minute"}))
                                else:
                                    value = probe_fn(adapter, root, ticker, row["probe"])
                            except _TERMINAL:
                                raise
                            except Exception as exc:
                                value = {
                                    "ticker": ticker,
                                    "verdict": "PROBE_ERROR",
                                    "pass_equivalent": False,
                                    "request_count": 0,
                                    "reused_request_count": 0,
                                    "error": {"type": type(exc).__name__,
                                              "message": str(exc)[:MAX_ERROR]},
                                }
                            record_probe(value)
                            with lock:
                                done["probe"] += 1
                                n = done["probe"]
                                issues = out["probe_issues"]
                            emit("probe", done=n, total=totals["probe"],
                                 metric=issues, ticker=ticker,
                                 operation="probe", port=port)
                    publish(key, phase)
                    current = None
                    current_index = None
            except BaseException as exc:
                if isinstance(exc, _TERMINAL):
                    _terminal.fail(exc)
                reason = _error(exc)
                death_state = f"DEAD - {reason}"[:160]
                # A hard worker death may happen after the reconcile callback
                # entered its one allowed source request.  Preserve that
                # in-flight row as a normalized ambiguous terminal outcome;
                # the remaining rows below stay explicit unstarted debt.
                # This mirrors Add Stocks and prevents a false claim of zero
                # unknown request states after a process/thread failure.
                death_item = None
                if (current is not None and current[0] == "reconcile"
                        and current_inflight and current_index is not None):
                    _phase, _key = current
                    _row = ticker_rows[_key]
                    _position = (0 if current_index is None
                                 else int(current_index))
                    if 0 <= _position < len(_row["reconciles"]):
                        death_item = dict(_row["reconciles"][_position])
                if death_item is not None:
                    with reconcile_publish_lock:
                        record_reconcile(
                            death_item, _reconcile_failure(death_item, exc))
                        with lock:
                            done["reconcile"] += 1
                with changed:
                    out["port_errors"].append(f"port {port}: {reason}")
                    if current is not None:
                        phase, key = current
                        row = ticker_rows[key]
                        row["active"] = False
                        row["state"] = "AMBIGUOUS"
                        position = (0 if current_index is None
                                    else int(current_index))
                        out["unprocessed"].extend(scheduled_debt(
                            row, phase, start=position,
                            current_reason=("ambiguous"
                                            if current_inflight
                                            or phase != "reconcile" else None),
                            later_reason="worker-failed-unstarted"))
                        out["halted"] += 1
                        # In-flight work is never silently retried.
                        active -= 1
                        if phase == "reconcile":
                            active_reconciles -= 1
                        elif phase == "check":
                            active_checks -= 1
                        elif phase == "probe":
                            active_probes -= 1
                    changed.notify_all()
                emit("log", message=f"port {port}: {death_state}")
            finally:
                if adapter is not None:
                    try:
                        fib.without_authority(lambda: adapter.disconnect())()
                    except _TERMINAL as exc:
                        _terminal.fail(exc)
                    except Exception as exc:
                        emit("log", message=f"port {port} disconnect: {_error(exc)}")
                set_port(port, death_state or "done")

        def worker_entry(port, child):
            # Include scope entry/argument binding and scope exit in the
            # failure boundary; worker's inner try cannot catch those.
            try:
                worker(port, _fetch_child=child)
            except BaseException as exc:
                _terminal.fail(exc)
                with changed:
                    out["port_errors"].append(f"port {port} lifecycle: {_error(exc)}")
                    changed.notify_all()

        threads = []
        for port in ports:
            if _terminal.stop.is_set():
                break
            child = None
            thread = None
            try:
                child = fops.narrow_worker_child(fib.current_worker(), f"fixdata-port-{port}")
                thread = threading.Thread(target=worker_entry, args=(port, child), daemon=True,
                    name=f"fixdata-scheduler-{port}")
                # Own the thread before start: it may launch and then raise.
                threads.append(thread)
                thread.start()
            except BaseException as exc:
                _terminal.fail(exc)
                if child is not None and (thread is None or thread.ident is None):
                    try:
                        child.close()
                    except BaseException as cleanup_exc:
                        _terminal.fail(cleanup_exc)
                out["port_errors"].append(f"port {port} start: {_error(exc)}")
                break
        alive = [thread for thread in threads if thread.ident is not None]
        drain_only = False
        while alive:
            try:
                poll = (_PROBE_WAIT_POLL_S if drain_only else
                        min(_PROBE_WAIT_POLL_S,
                            max(0.01, heartbeat_interval() / 2.0)))
                alive[0].join(timeout=poll)
                alive = [thread for thread in alive if thread.is_alive()]
                if not drain_only:
                    emit_due_heartbeats()
            except BaseException as exc:
                # Guard the whole drain iteration, not only join. Once it
                # fails, avoid re-entering failing heartbeat observers while
                # retaining the first failure and joining launched workers.
                _terminal.fail(exc)
                drain_only = True
        with lock:
            remaining = (list(repair_ready) + list(reconcile_ready)
                         + list(check_ready)
                         + list(probe_ready))
        for key in remaining:
            row = ticker_rows[key]
            phase = {
                "REPAIRING": "repair",
                "READY_TO_RECONCILE": "reconcile",
                "REPAIR_NOT_NEEDED": "check",
                "READY_TO_CHECK": "check",
                "READY_TO_PROBE": "probe",
            }.get(row["state"])
            if phase is None:
                out["unprocessed"].append((key, row["state"]))
            else:
                out["unprocessed"].extend(scheduled_debt(
                    row, phase, later_reason="worker-unavailable"))
            out["halted"] += 1
        with touched_lock:
            # This is a count of ticker scopes whose current queue membership
            # is unknown, not a guessed queue-row count.  Exact known rows stay
            # in reconcile_selected/reconcile_queue_pending.
            out["reconcile_selection_unknown"] += sum(
                row["reconcile_candidate"]
                and not row["reconcile_plan_current"]
                for row in ticker_rows.values())

    try:
        with lease:
            emit("stage", stage=1)
            if _terminal.first is not None:
                raise _terminal.first

            def scan_progress(index, count, ticker, interval):
                emit("scan", done=index + 1, total=count,
                     ticker=ticker, interval=interval)
                if not pause_boundary():
                    # The production scanner calls progress before the named
                    # series. This is therefore the boundary after the prior
                    # series; the explicit post-scan boundary covers the last.
                    # Production scan_all_gaps intentionally swallows ordinary
                    # progress Exception subclasses. This private BaseException
                    # sentinel is caught immediately around scan_fn below.
                    raise _ScanPauseCancelled()

            scan_cancelled = False
            try:
                full = scan_fn(
                    root, write=True, progress=scan_progress,
                    kinds=kinds, preserve_existing=True)
                if full.get("error"):
                    raise RuntimeError(
                        f"gap scan failed: {full['error']}")
            except _ScanPauseCancelled:
                scan_cancelled = True
                out["cancelled"] = True
            if not scan_cancelled:
                todo = []
                for key, value in (full.get("summary") or {}).items():
                    days = list(value.get("missing_day_list") or [])
                    days += list(value.get("source_absent_list") or [])
                    if days:
                        ticker, _sep, interval = key.rpartition(" ")
                        todo.append((ticker, interval, sorted(set(days))))
                repair_series = {(str(ticker).upper(), str(interval))
                                 for ticker, interval, _days in todo}
                for ticker, interval in full.get("verification_series") or ():
                    key = (str(ticker).upper(), str(interval))
                    if key not in repair_series:
                        verify("gaps", *key)
                out["fill_series"] = len(todo)
                emit("log", message=(
                    f"[scan] {full.get('series_scanned', 0)} series scanned; "
                    f"{len(todo)} with gaps"))
                emit("log", message="[plan] listing accuracy series")
                emit("log", message="[plan] discovering value candidates")
                emit("log", message="[plan] selecting spot probes")
                if not pause_boundary():
                    out["cancelled"] = True
                else:
                    if health_fn is not None:
                        try:
                            _health_fields(out, health_fn(root))
                        except _TERMINAL:
                            raise
                        except Exception as exc:
                            emit("log", message=(
                                f"[health] report skipped: {_error(exc)}"))
                    if not pause_boundary():
                        out["cancelled"] = True
                    else:
                        emit(
                            "stage", stage=2,
                            state=("repair/reconcile/check"
                                   if reconcile_plan_fn is not None
                                   else "repair/check"))
                        emit("refetch", done=0, total=len(todo), metric=0)
                        acc = list(series_fn(root))
                        out["acc_series"] = len(acc)
                        emit("xcheck", done=0, total=len(acc), metric=0)
                        reconcile_items = []
                        if (reconcile_plan_fn is not None
                                and (engine_tasks or reconcile_fn is not None)):
                            try:
                                reconcile_items = _reconcile_items(
                                    reconcile_plan_fn(
                                        root, ticker=None, kinds=kinds))
                            except _TERMINAL:
                                raise
                            except Exception as exc:
                                out["reconcile_error"] = _error(exc)
                                out["reconcile_plan_failed"] += 1
                                out["reconcile_selection_unknown"] += 1
                                out["unprocessed"].append((
                                    None, None, "reconcile-plan",
                                    "initial-planning-failed"))
                                emit("log", message=(
                                    "volatility candidate discovery "
                                    f"skipped: {out['reconcile_error']}"))
                            emit(
                                "reconcile", done=0,
                                total=0, metric=0,
                                settled=0, corrected=0, requests=0)
                        probe_items = {}
                        if (probe_plan_fn is not None
                                and (engine_tasks or probe_fn is not None)):
                            tickers = sorted({str(row[0]).upper()
                                              for row in todo + acc})
                            try:
                                plan = probe_plan_fn(root, tickers, probe_seed)
                                if not isinstance(plan, dict):
                                    raise TypeError(
                                        "probe plan must be an object")
                                out["probe_seed"] = plan.get("seed")
                                probe_items = dict(plan.get("items") or {})
                                for row in plan.get("rows") or []:
                                    record_probe(row)
                            except _TERMINAL:
                                raise
                            except Exception as exc:
                                emit("log", message=(
                                    "spot-probe planning skipped: "
                                    f"{_error(exc)}"))
                        out["probe_selected"] = len(probe_items)
                        emit("probe", done=out["probe_completed"],
                             total=(out["probe_completed"]
                                    + len(probe_items)),
                             metric=out["probe_issues"])
                        run_scheduler(
                            todo, acc, reconcile_items, probe_items)
                        if _terminal.first is not None:
                            out["error"] = _error(_terminal.first)
                        with touched_lock:
                            # Every selected row originated in the durable
                            # queue. Rows not retired remain debt even when a
                            # worker stopped before publishing an outcome.
                            out["reconcile_queue_pending"] = max(
                                0, out["reconcile_selected"]
                                - out["reconcile_queue_resolved"])
                        if not _terminal.stop.is_set() and (should_cancel() or not pause_boundary()):
                            out["cancelled"] = True
                        if (reconcile_report_fn is not None
                                and out["reconcile_selected"]):
                            try:
                                out["reconcile_report"] = reconcile_report_fn(
                                    root, run_id,
                                    list(out["reconcile_rows"]),
                                    requested_count=out["reconcile_selected"],
                                    summary={
                                        "completed_count":
                                            out["reconcile_completed"],
                                        "request_count":
                                            out["reconcile_request_count"],
                                        "request_count_unknown":
                                            out["reconcile_request_unknown"],
                                        "settled_count":
                                            out["reconcile_settled"],
                                        "corrected_count":
                                            out["reconcile_corrected"],
                                        "unresolved_count":
                                            out["reconcile_unresolved"],
                                        "queue_resolved_count":
                                            out["reconcile_queue_resolved"],
                                        "queue_pending_count":
                                            out["reconcile_queue_pending"],
                                        "rows_truncated":
                                            out["reconcile_rows_truncated"],
                                    })
                            except _TERMINAL:
                                raise
                            except Exception as exc:
                                message = _error(exc)
                                out["error"] = "reconcile report failed: " + message
                                out["reconcile_error"] = (
                                    f"{out['reconcile_error']}; {message}"
                                    if out["reconcile_error"] else message)
                                emit("log", message=(
                                    "volatility reconciliation report skipped: "
                                    f"{message}"))
                        if (probe_report_fn is not None
                                and out["probe_rows"]):
                            try:
                                out["probe_report"] = probe_report_fn(
                                    root, run_id, out["probe_seed"],
                                    list(out["probe_rows"]))
                            except _TERMINAL:
                                raise
                            except Exception as exc:
                                out["probe_issues"] += 1
                                out["error"] = "probe report failed: " + _error(exc)
                                emit("log", message=(
                                    "spot-probe report skipped: "
                                    f"{_error(exc)}"))
            try:
                if touched:
                    refreshed = refresh_fn(
                        root, sorted(set(touched)), kinds=kinds)
                    if refreshed.get("error"):
                        raise RuntimeError(
                            f"gap refresh failed: {refreshed['error']}")
                    out["still"] = int(
                        refreshed.get("missing_days_run", 0))
                    for ticker, interval in (
                            refreshed.get("verification_series") or ()):
                        if not _terminal.stop.is_set():
                            verify("gaps", ticker, interval)
            except _TERMINAL:
                raise
            except Exception as exc:
                out["error"] = _error(exc)
                emit("log", message=f"gap refresh failed: {out['error']}")
    except _TERMINAL as exc:
        _terminal.fail(exc)
        out["error"] = _error(exc)
    except Exception as exc:
        out["error"] = _error(exc)
    finally:
        try:
            retention = fib.without_authority(
                lambda: _plain_data(run_log_retention.prune_run_logs(root)))()
            deleted = len(retention.get("deleted") or ())
            failures = len(retention.get("errors") or ())
            if deleted or failures:
                emit("log", message=(
                    "Run Logs retention: "
                    f"deleted {deleted}, errors {failures}, "
                    f"freed {int(retention.get('freed_bytes') or 0)} bytes"))
        except _TERMINAL as exc:
            _terminal.fail(exc)
            out["error"] = _error(_terminal.first)
        except Exception as exc:  # noqa: BLE001 - ordinary retention failure is nonfatal
            emit("log", message=f"Run Logs retention skipped: {_error(exc)}")
        emit("done", result=out)
    return out
