"""Shared Row 51 M3 volatility refetch/reconcile orchestration.

The module consumes only validated rows from ``_vol_value_queue.json``.  Its
market-data work is delegated to :mod:`vol_value_bank`, while queue/registry
publication is delegated to :mod:`vol_value_audit`.  Fix Data and Add Stocks
therefore use one decision path and cannot disagree about settlement versus
correction.

No connection is opened here.  Callers supply an already-owned adapter and
pacer under their normal ``fetch`` operation lease.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import uuid
from contextlib import contextmanager, nullcontext
from pathlib import Path

import stock_storage as storage
import vol_value_audit as audit
import vol_value_bank as bank
import fetch_ibkr_bridge as fib
from fetch_operations import narrow_worker_child
from fetch_authority import AuthorityError
from fetch_ledger import LedgerError


VOL_VALUE_RECONCILE = True
MAX_ERROR = 500
MAX_LEDGER_ROWS = 4_096
MAX_LEDGER_ITEMS = audit.MAX_QUEUE_ROWS
MAX_ARTIFACT_BYTES = 4 * 1024 * 1024

_SUMMARY_FIELDS = frozenset({
    "completed_count", "request_count", "request_count_unknown",
    "settled_count", "corrected_count",
    "unresolved_count", "queue_resolved_count", "queue_pending_count",
    "rows_truncated",
})

_CLOSE_REASONS = frozenset({
    "jump_up", "jump_down", "iv_zero_run", "iv_hvol_band",
})
_STRONG_REASONS = frozenset({"unit_flip_month"})
_ROW_STATUSES = frozenset({
    "settled", "corrected", "unresolved", "stale", "cancelled", "ambiguous",
})


class VolValueReconcileError(RuntimeError):
    """The shared reconcile contract was invoked with unsafe input."""


def _error(exc):
    return fib.without_authority(lambda: f"{type(exc).__name__}: {exc}"[:MAX_ERROR])()


def _run_id(value):
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    if (not value or len(value) > audit.MAX_TEXT
            or any(char in value for char in "\r\n\x00")):
        raise VolValueReconcileError("invalid reconcile run id")
    return value


def _key(row):
    if not isinstance(row, dict):
        raise VolValueReconcileError("reconcile row must be an object")
    try:
        return (str(row["ticker"]), str(row["kind_token"]), str(row["day"]))
    except KeyError as exc:
        raise VolValueReconcileError("reconcile row key is incomplete") from exc


def _same_value(left, right):
    if left is None or right is None:
        return False
    try:
        left, right = float(left), float(right)
    except (TypeError, ValueError, OverflowError):
        return False
    return (math.isfinite(left) and math.isfinite(right)
            and (abs(left - right) < audit.SETTLE_TOL
                 or math.isclose(
                     abs(left - right), audit.SETTLE_TOL,
                     rel_tol=1e-12, abs_tol=1e-15)))


def _base(row, *, status="unresolved", request_count=0, error=None):
    ticker, token, day = _key(row)
    try:
        reasons = bank.canonical_anomaly_reasons(
            row.get("reasons"), reason=row.get("reason"))
    except Exception:  # noqa: BLE001 - malformed caller input retains no provenance
        reasons = []
    result = {
        "status": status,
        "ticker": ticker,
        "kind": token,
        "kind_token": token,
        "day": day,
        "request_count": int(request_count),
        "queue_resolved": status in {"settled", "corrected"},
        "reason": ",".join(reasons),
        "reasons": reasons,
    }
    if error:
        result["error"] = str(error)[:MAX_ERROR]
    return result


def _validated_current_snapshot(root, supplied, *, manifest_lock=None, today=None):
    """Refresh one ticker's full finding context and snapshot the exact row.

    A durable queue row can depend on another ratio kind (notably the paired
    IV/HVOL coherence finding).  Correcting an earlier row can therefore make
    a later preplanned row disappear even though that old queue payload has not
    yet been rebuilt.  Re-audit the complete ticker slice under the same ticker
    writer fence before every request, then require the supplied row to remain
    byte-for-byte current.  The caller already owns ``fetch``; using that same
    operation mode keeps this check re-entrant inside Fix Data/Add Stocks.
    """
    wanted_key = _key(supplied)
    try:
        root = Path(root).resolve()
        rows = audit.queue_snapshot(root, tickers=[wanted_key[0]])
    except Exception as exc:  # noqa: BLE001 - normalize the public boundary
        raise VolValueReconcileError(
            f"cannot validate volatility queue: {_error(exc)}") from exc
    matches = [row for row in rows if _key(row) == wanted_key]
    if len(matches) != 1 or matches[0] != supplied:
        return None, None

    current = matches[0]
    guard = manifest_lock if manifest_lock is not None else nullcontext()
    ticker_dir = root / current["ticker"]
    try:
        # Lock order matches bank publication: caller manifest lock -> shared
        # ticker transaction -> audit sidecar transaction.  This makes the
        # cross-kind scan and the day snapshot one coherent local-bank view.
        with guard, storage.ticker_transaction(ticker_dir):
            # A prior process death must resolve to exact old/old or new/new
            # bank state before the audit is allowed to replace this ticker's
            # queue slice.  Audit itself deliberately treats an active stage
            # as incomplete, so recovery cannot be deferred until snapshot.
            bank.recover_ratio_transaction(root, current["ticker"])
            report = audit.audit(
                root, tickers=[current["ticker"]], write_queue=True,
                operation_mode="fetch")
            if not isinstance(report, dict) or report.get("complete") is not True:
                raise VolValueReconcileError(
                    "focused volatility audit is incomplete")
            rows = audit.queue_snapshot(root, tickers=[current["ticker"]])
            matches = [row for row in rows if _key(row) == wanted_key]
            if len(matches) != 1 or matches[0] != supplied:
                return None, None
            snapshot = bank.snapshot_ratio_day(
                root, current["ticker"], current["kind_token"], current["day"], today=today)
    except (VolValueReconcileError, AuthorityError, LedgerError, bank.ibkr.Cancelled):
        raise
    except Exception as exc:  # noqa: BLE001 - normalize the public boundary
        raise VolValueReconcileError(
            f"cannot refresh volatility finding context: {_error(exc)}") from exc
    return matches[0], snapshot


@contextmanager
def _post_fetch_current_context(root, current, *, manifest_lock=None):
    """Revalidate the complete ticker context and hold it through action."""
    root = Path(root).resolve()
    guard = manifest_lock if manifest_lock is not None else nullcontext()
    ticker_dir = root / current["ticker"]
    with guard, storage.ticker_transaction(ticker_dir):
        bank.recover_ratio_transaction(root, current["ticker"])
        report = audit.audit(
            root, tickers=[current["ticker"]], write_queue=True,
            operation_mode="fetch")
        if not isinstance(report, dict) or report.get("complete") is not True:
            raise VolValueReconcileError(
                "post-fetch focused volatility audit is incomplete")
        rows = audit.queue_snapshot(root, tickers=[current["ticker"]])
        matches = [row for row in rows if _key(row) == _key(current)]
        yield (matches[0]
               if len(matches) == 1 and matches[0] == current else None)


def plan(root, tickers=None, kinds=None):
    """Return detached, deterministic queue rows for an exact selection.

    ``kinds`` contains full interval/kind tokens (for example ``1m-iv``), not
    loose kind names.  Production callers that expose ``iv``/``hvol`` controls
    translate those controls before calling this seam.
    """
    selected = None
    if kinds is not None:
        if isinstance(kinds, (str, bytes)):
            raise VolValueReconcileError("kinds must be an iterable, not text")
        try:
            selected = {str(item).strip() for item in kinds}
        except TypeError as exc:
            raise VolValueReconcileError("kinds must be iterable") from exc
        if any(not token
               or storage.INTERVAL_RE.fullmatch(token) is None
               or storage.kind_of(token) not in storage.RATIO_KINDS
               or storage.session_of(token) != "rth"
               for token in selected):
            raise VolValueReconcileError(
                "kinds must contain exact RTH IV/HVOL interval tokens")
    rows = audit.queue_snapshot(Path(root), tickers=tickers)
    if selected is not None:
        rows = [row for row in rows if row["kind_token"] in selected]
    return rows


def _request_count(exc, fallback=0):
    value = getattr(exc, "request_count", fallback)
    if isinstance(value, bool):
        return int(fallback)
    try:
        return max(0, min(1, int(value)))
    except (TypeError, ValueError, OverflowError):
        return int(fallback)


def _apply_served(root, current, snapshot, served, run_id):
    """Settle or correct while the caller holds the post-fetch ticker fence."""
    requests = int(served.get("request_count") or 0)
    stored_value = snapshot.get("stored_value")
    reasons = frozenset(current.get("reasons") or ())
    same_close = _same_value(stored_value, served.get("served_value"))
    exact_day = (
        snapshot.get("stored_day_sha256")
        == served.get("served_day_sha256"))
    close_only = bool(reasons) and reasons.issubset(_CLOSE_REASONS)
    strong_only = (bool(reasons)
                   and reasons.issubset(_CLOSE_REASONS | _STRONG_REASONS)
                   and bool(reasons & _STRONG_REASONS))

    if same_close and (close_only or (strong_only and exact_day)):
        try:
            evidence = bank.settlement_evidence(snapshot, served, reasons)
            # The outer post-fetch context already holds the caller manifest
            # lock.  Bank guards still take the re-entrant ticker transaction.
            with bank.finalization_guard(
                    root, snapshot, served, action="settled"):
                finalized = audit.finalize_reconcile(
                    root, current, action="settled", run=str(run_id),
                    value=served["served_value"], reason=current["reason"],
                    evidence=evidence)
        except audit.ReconcileFinalizeError as exc:
            if exc.action != "settled" or not exc.registry_committed:
                return _base(
                    current, request_count=requests, error=_error(exc))
            result = _base(
                current, status="settled", request_count=requests,
                error=_error(exc))
            result.update({
                "registry_committed": True,
                "queue_resolved": exc.queue_resolved,
                "stored_value": stored_value,
                "served_value": served["served_value"],
                "same_value": True,
                "same_day": exact_day,
                "what_to_show": served["what_to_show"],
            })
            return result
        except bank.RatioDayStale as exc:
            return _base(
                current, status="stale", request_count=requests,
                error=_error(exc))
        except Exception as exc:  # noqa: BLE001 - no queue lie on publication error
            return _base(
                current, request_count=requests, error=_error(exc))
        result = _base(
            current, status=finalized.get("status", "unresolved"),
            request_count=requests)
        result["queue_resolved"] = finalized.get("status") == "settled"
        result.update({
            "registry_committed": finalized.get("status") == "settled",
            "stored_value": stored_value,
            "served_value": served["served_value"],
            "same_value": True,
            "same_day": exact_day,
            "what_to_show": served["what_to_show"],
        })
        if finalized.get("status") != "settled":
            result["error"] = "queue row changed before settlement publication"
        return result

    try:
        correction = bank.replace_ratio_day(
            root, snapshot, served, str(run_id), reasons=current["reasons"])
    except Exception as exc:  # noqa: BLE001 - bank/queue stay conservatively owed
        if isinstance(exc, bank.RatioDayCommitError):
            evidence = dict(exc.evidence)
            rollback_verified = evidence.get("rollback_verified") is True
            result = _base(
                current,
                status=("unresolved" if rollback_verified else "ambiguous"),
                request_count=requests, error=_error(exc))
            result.update({
                "commit_state": ("rolled_back" if rollback_verified
                                 else "recovery_required"),
                "rollback_verified": rollback_verified,
                "bank_write_completed":
                    evidence.get("month_write_completed") is True,
                "bank_written": evidence.get("bank_written"),
                "correction_recorded": evidence.get("correction_recorded"),
                "commit_evidence": evidence,
            })
            if isinstance(evidence.get("bank_written"), bool):
                result["bank_committed"] = evidence["bank_written"]
            return result
        return _base(
            current,
            status=("stale" if isinstance(exc, bank.RatioDayStale)
                    else "unresolved"),
            request_count=requests, error=_error(exc))

    bank_verified_for_finalization = False
    try:
        with bank.finalization_guard(
                root, snapshot, served, action="corrected",
                correction=correction):
            bank_verified_for_finalization = True
            finalized = audit.finalize_reconcile(
                root, current, action="corrected", run=str(run_id))
    except audit.ReconcileFinalizeError as exc:
        result = _base(
            current, status="corrected", request_count=requests,
            error=_error(exc))
        result["queue_resolved"] = exc.queue_resolved
        result["bank_committed"] = True
        result["month_sha256"] = correction.get("month_sha256")
        return result
    except Exception as exc:  # replace returned: the correction is durable
        result = _base(
            current, status="corrected", request_count=requests,
            error=_error(exc))
        result["queue_resolved"] = False
        result["bank_committed"] = True
        result["month_sha256"] = correction.get("month_sha256")
        result["finalization_verified"] = bank_verified_for_finalization
        return result

    result = _base(current, status="corrected", request_count=requests)
    result.update({
        "queue_resolved": finalized.get("status") == "corrected",
        "bank_committed": True,
        "stored_value": stored_value,
        "served_value": served["served_value"],
        "same_value": same_close,
        "same_day": exact_day,
        "what_to_show": served["what_to_show"],
        "month_sha256": correction.get("month_sha256"),
    })
    if finalized.get("status") != "corrected":
        result["error"] = "bank corrected; queue row changed before retirement"
    return result


@fib.worker_scope
def reconcile_one(adapter, pacer, root, row, run_id, cancel=None,
                  manifest_lock=None, progress=None):
    """Reconcile one exact durable queue row.

    A close-only finding settles when the fresh final close is within the
    inclusive settlement tolerance.  Month-scale findings additionally require
    the complete stored and served day fingerprints to match.  Hard-invalid
    findings never settle; a valid served day must replace them.  Any failure
    leaves the durable queue row untouched.
    """
    try:
        run_id = fib.without_authority(_run_id)(run_id)
        current, snapshot = fib.without_authority(_validated_current_snapshot)(
            root, row, manifest_lock=manifest_lock,
            today=fib.current_worker().context.captured_now.date())
    except (AuthorityError, LedgerError, bank.ibkr.Cancelled):
        raise
    except Exception as exc:  # noqa: BLE001 - return bounded workflow evidence
        return fib.without_authority(_base)(row, error=_error(exc))
    if current is None:
        return fib.without_authority(_base)(
            row, status="stale", error="queue row changed before refetch")

    # A queue generated from a different bank revision cannot authorize a
    # request.  ``None`` intentionally covers hard non-finite final closes.
    queued_value = current.get("value")
    stored_value = snapshot.get("stored_value")
    if ((queued_value is None) != (stored_value is None)
            or (queued_value is not None
                and float(queued_value) != float(stored_value))):
        return _base(
            current, status="stale",
            error="stored final close changed after queue publication")

    try:
        served = bank.fetch_ratio_day(
            adapter, pacer, snapshot, cancel=cancel, progress=progress,
            _fetch_child=narrow_worker_child(fib.current_worker(), "ratio-day-" + uuid.uuid4().hex,
                rights={"ibkr.vol_value_bank.ratio_day"}))
    except (AuthorityError, LedgerError, bank.ibkr.Cancelled):
        raise
    except Exception as exc:  # noqa: BLE001 - source failures remain retryable
        fib.notify_request_error(exc)
        return _base(
            current, request_count=_request_count(exc), error=_error(exc))

    requests = int(served.get("request_count") or 0)
    if (served.get("served_bars") != snapshot.get("stored_bars")
            or served.get("served_grid_sha256")
            != snapshot.get("stored_grid_sha256")):
        return _base(
            current, request_count=requests,
            error="served day timestamp grid/count differs from stored day")
    try:
        # Same-mode fetch leases are intentionally re-entrant, so another
        # worker can change a paired ratio kind while this request is in
        # flight. Re-audit the complete ticker after the response, and keep
        # the manifest/ticker fence held through settlement or correction.
        @fib.without_authority
        def publish():
            with _post_fetch_current_context(
                    root, current, manifest_lock=manifest_lock) as refreshed:
                if refreshed is None:
                    return _base(
                        current, status="stale", request_count=requests,
                        error="finding context changed during refetch")
                return _apply_served(root, refreshed, snapshot, served, run_id)
        return publish()
    except (AuthorityError, LedgerError, bank.ibkr.Cancelled):
        raise
    except Exception as exc:  # noqa: BLE001 - no durable action was attempted
        return _base(current, request_count=requests, error=_error(exc))


def _summary(value, observed, provided_count, requested_count):
    if not isinstance(value, dict) or set(value) != _SUMMARY_FIELDS:
        raise VolValueReconcileError("reconcile summary fields are invalid")
    result = {}
    for key in sorted(_SUMMARY_FIELDS):
        count = value.get(key)
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise VolValueReconcileError(
                f"reconcile summary {key} is invalid")
        result[key] = count
    if result["completed_count"] != (
            provided_count + result["rows_truncated"]):
        raise VolValueReconcileError(
            "reconcile summary completed/row counts disagree")
    if (result["settled_count"] + result["corrected_count"]
            + result["unresolved_count"] != result["completed_count"]):
        raise VolValueReconcileError(
            "reconcile summary status counts disagree")
    if result["request_count"] > result["completed_count"]:
        raise VolValueReconcileError(
            "reconcile summary request count exceeds one per row")
    if result["request_count_unknown"] > result["completed_count"]:
        raise VolValueReconcileError(
            "reconcile summary unknown-request count is invalid")
    if (result["request_count"] + result["request_count_unknown"]
            > result["completed_count"]):
        raise VolValueReconcileError(
            "reconcile summary request states exceed completed rows")
    if result["queue_resolved_count"] > (
            result["settled_count"] + result["corrected_count"]):
        raise VolValueReconcileError(
            "reconcile summary queue count exceeds resolved outcomes")
    if (result["queue_resolved_count"] + result["queue_pending_count"]
            != requested_count):
        raise VolValueReconcileError(
            "reconcile summary queue-debt counts disagree")

    if (result["settled_count"] < observed["settled"]
            or result["corrected_count"] < observed["corrected"]
            or result["unresolved_count"] < observed["unresolved"]
            or result["request_count"] < observed["requests"]
            or result["request_count_unknown"] < observed["unknown"]
            or result["queue_resolved_count"] < observed["queue"]
            or result["queue_pending_count"]
            < provided_count - observed["queue"]):
        raise VolValueReconcileError(
            "reconcile summary understates retained row evidence")
    return result


def ledger(rows, *, requested_count=None, summary=None):
    """Return bounded aggregate evidence for workflow/artifact consumers."""
    if isinstance(rows, (str, bytes)):
        raise VolValueReconcileError("reconcile rows must be an iterable, not text")
    try:
        iterator = iter(rows)
    except TypeError as exc:
        raise VolValueReconcileError("reconcile rows must be iterable") from exc
    kept = []
    provided_count = 0
    observed = {"settled": 0, "corrected": 0, "unresolved": 0,
                "requests": 0, "unknown": 0, "queue": 0}
    for raw_row in iterator:
        if provided_count >= MAX_LEDGER_ITEMS:
            raise VolValueReconcileError(
                "reconcile row iterable exceeds the item limit")
        if not isinstance(raw_row, dict):
            raise VolValueReconcileError("reconcile row must be an object")
        row = dict(raw_row)
        provided_count += 1
        if len(kept) < MAX_LEDGER_ROWS:
            kept.append(row)
        status = row.get("status")
        if not isinstance(status, str) or status not in _ROW_STATUSES:
            raise VolValueReconcileError("reconcile row status is invalid")
        queue_resolved = row.get("queue_resolved")
        if not isinstance(queue_resolved, bool):
            raise VolValueReconcileError(
                "reconcile row queue_resolved must be boolean")
        registry_committed = row.get("registry_committed", False)
        bank_committed = row.get("bank_committed", False)
        if (not isinstance(registry_committed, bool)
                or not isinstance(bank_committed, bool)):
            raise VolValueReconcileError(
                "reconcile row durable-action proofs must be boolean")
        if status == "settled" and not registry_committed:
            raise VolValueReconcileError(
                "settled row lacks committed registry evidence")
        if status == "corrected" and not bank_committed:
            raise VolValueReconcileError(
                "corrected row lacks committed bank evidence")
        if registry_committed and status != "settled":
            raise VolValueReconcileError(
                "registry commit evidence disagrees with row status")
        if queue_resolved and not (
                (status == "settled" and registry_committed)
                or (status == "corrected" and bank_committed)):
            raise VolValueReconcileError(
                "resolved queue row lacks its durable action proof")
        bucket = status if status in {"settled", "corrected"} else "unresolved"
        observed[bucket] += 1
        requests = row.get("request_count") or 0
        if (isinstance(requests, bool) or not isinstance(requests, int)
                or requests not in (0, 1)):
            raise VolValueReconcileError(
                "reconcile row request_count must be 0 or 1")
        request_unknown = row.get("request_count_unknown", False)
        if not isinstance(request_unknown, bool):
            raise VolValueReconcileError(
                "reconcile row request_count_unknown must be boolean")
        if request_unknown and requests:
            raise VolValueReconcileError(
                "reconcile row request state is contradictory")
        observed["requests"] += requests
        observed["unknown"] += int(request_unknown)
        observed["queue"] += int(queue_resolved)
    if requested_count is not None:
        if (isinstance(requested_count, bool)
                or not isinstance(requested_count, int)
                or requested_count < 0
                or requested_count > MAX_LEDGER_ITEMS):
            raise VolValueReconcileError("requested_count is invalid")
    requested = (provided_count if requested_count is None
                 else requested_count)
    if requested < provided_count:
        raise VolValueReconcileError(
            "requested_count is smaller than completed_count")
    if summary is None:
        totals = {
            "completed_count": provided_count,
            "request_count": observed["requests"],
            "request_count_unknown": observed["unknown"],
            "settled_count": observed["settled"],
            "corrected_count": observed["corrected"],
            "unresolved_count": observed["unresolved"],
            "queue_resolved_count": observed["queue"],
            "queue_pending_count": requested - observed["queue"],
            "rows_truncated": max(0, provided_count - len(kept)),
        }
    else:
        totals = _summary(summary, observed, provided_count, requested)
    if requested < totals["completed_count"]:
        raise VolValueReconcileError(
            "requested_count is smaller than completed_count")
    return {
        "kind": "vol_value_reconcile",
        "version": 1,
        "requested_count": requested,
        "completed_count": totals["completed_count"],
        "request_count": totals["request_count"],
        "request_count_unknown": totals["request_count_unknown"],
        "settled_count": totals["settled_count"],
        "corrected_count": totals["corrected_count"],
        "unresolved_count": totals["unresolved_count"],
        "queue_resolved_count": totals["queue_resolved_count"],
        "queue_pending_count": totals["queue_pending_count"],
        "rows": kept,
        "rows_truncated": totals["completed_count"] - len(kept),
    }


def write_artifact(path, rows, *, bank_root, run_logs_root, run_id,
                   requested_count=None, summary=None):
    """Write one bounded Fix Data reconciliation artifact outside the bank."""
    run_id = _run_id(run_id)
    try:
        target = Path(path).resolve()
        bank_root = Path(bank_root).resolve()
        run_logs_root = Path(run_logs_root).resolve()
    except OSError as exc:
        raise VolValueReconcileError(
            f"cannot resolve reconcile artifact path: {exc}") from exc
    if (target.parent != run_logs_root or target.suffix.lower() != ".json"
            or target == bank_root or bank_root in target.parents
            or run_logs_root == bank_root or bank_root in run_logs_root.parents):
        raise VolValueReconcileError(
            "reconcile artifact must be a direct JSON child outside the bank")
    report = ledger(
        rows, requested_count=requested_count, summary=summary)
    report.update({
        "parent_run_id": run_id,
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(
            microsecond=0).isoformat().replace("+00:00", "Z"),
        "artifact": str(target),
        "artifact_written": True,
        "market_data_written": report["corrected_count"] > 0,
        "bank_metadata_written": bool(
            report["settled_count"] or report["corrected_count"]
            or report["queue_resolved_count"]),
    })
    report["bank_written"] = bool(
        report["market_data_written"] or report["bank_metadata_written"])
    report["written"] = report["bank_written"]
    try:
        encoded = (json.dumps(
            report, indent=2, sort_keys=True, allow_nan=False) + "\n").encode(
                "utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise VolValueReconcileError(
            "reconcile artifact is not JSON-safe") from exc
    if len(encoded) > MAX_ARTIFACT_BYTES:
        raise VolValueReconcileError(
            "reconcile artifact exceeds the size limit")
    try:
        run_logs_root.mkdir(parents=True, exist_ok=True)
        storage._atomic_write_bytes(target, encoded)
    except (OSError, storage.StorageError) as exc:
        raise VolValueReconcileError(
            f"reconcile artifact could not be written: {exc}") from exc
    return str(target)


__all__ = [
    "MAX_LEDGER_ITEMS",
    "MAX_LEDGER_ROWS",
    "MAX_ARTIFACT_BYTES",
    "VOL_VALUE_RECONCILE",
    "VolValueReconcileError",
    "ledger",
    "plan",
    "reconcile_one",
    "write_artifact",
]
