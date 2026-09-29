"""Operation-scoped authorization, durable attempts and detached publication.

LiveIB supplies verified account binding and raw transport normalization. HTTP
support remains offline-test-only until the separately reviewed A2 activation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import base64
import math
import threading
from itertools import count
from pathlib import Path
from types import MappingProxyType
from typing import Mapping
import uuid
from urllib.error import HTTPError

from fetch_authority import (AuthorityError, BAR_SIZES, CalendarUnsupported,
                             ResponseQuarantined, ScheduleAuthority,
                             TOKENS, aware_ny, canonical_bytes, digest_value, parse_token,
                             strict_json)
from fetch_envelopes import Envelope, decide, instant, parse_envelope
from fetch_ledger import FetchLedger, LedgerError, identity
from fetch_governors import default_registry


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _portable_quarantine_locator(path):
    """Keep published evidence locators valid after a project copy."""
    try:
        target = Path(path).resolve(strict=True)
        project = PROJECT_ROOT.resolve(strict=True)
        return target.relative_to(project).as_posix()
    except ValueError:
        # Test evidence can intentionally live outside the project. Its
        # quarantine is always beside the ledger, so the filename is enough.
        return Path(path).name
    except OSError as exc:
        raise LedgerError("response quarantine locator is unavailable") from exc


A1_PRODUCERS = MappingProxyType({
    "ibkr.gap_fill.month_daily": "ibkr-bars",
    "ibkr.gap_fill.month_intraday": "ibkr-bars",
    "ibkr.gap_fill.session_fetch": "ibkr-bars",
    "ibkr.gap_fill.head_daily_probe": "ibkr-bars",
    "ibkr.gap_fill.head_timestamp": "ibkr-head",
    "ibkr.earliest_available.head_timestamp": "ibkr-head",
    "ibkr.choke.qualify": "ibkr-metadata",
    "ibkr.choke.qualify_many": "ibkr-metadata",
})


class RequestRefused(AuthorityError):
    pass


class RequestCancelled(Exception):
    """A cancellation is terminal, never a provider retry signal."""


@dataclass(frozen=True)
class FetchRunContext:
    operation_id: str
    captured_now: datetime
    authority: ScheduleAuthority
    horizons: Mapping[str, datetime | None]
    ledger: FetchLedger = field(repr=False, compare=False)
    governors: object = field(default_factory=default_registry, repr=False, compare=False)
    test_capability: object = field(default=None, repr=False, compare=False)
    _account_bindings: dict = field(default_factory=dict, init=False, repr=False, compare=False)
    _binding_lock: object = field(default_factory=threading.Lock, init=False, repr=False, compare=False)
    _publication_failures: list = field(default_factory=list, init=False, repr=False, compare=False)

    def __post_init__(self):
        # Freeze a detached map even if a caller directly constructs a context
        # instead of using create(). No worker may retain a mutable horizon map.
        if set(self.horizons) != set(TOKENS):
            raise AuthorityError("context horizon denominator is incomplete")
        frozen = {token: aware_ny(value) if value is not None else None
                  for token, value in self.horizons.items()}
        object.__setattr__(self, "captured_now", aware_ny(self.captured_now))
        object.__setattr__(self, "horizons", MappingProxyType(frozen))

    @classmethod
    def create(cls, directory: Path, *, authority=None, clock=None, fault=None,
               governors=None, test_capability=None):
        authority = authority if authority is not None else ScheduleAuthority.load()
        clock = clock if clock is not None else lambda: datetime.now(timezone.utc)
        now = authority.check_clock(clock())  # Exactly one wall-clock read for horizons.
        horizons = MappingProxyType({token: authority.horizon(token, now) for token in TOKENS})
        operation = uuid.uuid4().hex
        ledger = FetchLedger(directory, operation, fault=fault)
        return cls(operation, now, authority, horizons, ledger,
                   default_registry() if governors is None else governors, test_capability)

    def permits(self, producer, envelope):
        from fetch_operations import check_request, operation_purpose
        check_request(self, producer)
        purpose = operation_purpose(self)
        if (envelope.variant == "ibkr-bars-unfiltered"
                and (producer != "ibkr.live_kind_smoke.iv_rth"
                     or purpose != "diagnostic")):
            return False
        if (envelope.variant == "ibkr-bars" and purpose == "diagnostic"
                and producer.startswith("ibkr.live_kind_smoke.")):
            base, kind, _session = parse_token(envelope.token)
            if (kind == "hvol" and base != "1d"
                    and (producer != "ibkr.live_kind_smoke.hvol_intraday"
                         or envelope.token != "1m-hvol")):
                return False
        if producer == "ibkr.two_account_pacing_probe.hammer":
            if (purpose != "diagnostic" or envelope.variant != "ibkr-bars"
                    or envelope.token != "1s" or envelope.duration != "1800 S"):
                return False
        if (purpose in (None, "nightly")
                and A1_PRODUCERS.get(producer) == envelope.variant):
            return envelope.variant != "ibkr-metadata" or envelope.method == "qualification"
        if self.test_capability is not None:
            from fetch_test_policy import is_live_capability, capability_allows
            if not is_live_capability(self.test_capability):
                raise RequestRefused("invalid test-policy capability: requires live tripwires")
            if envelope.variant == "ibkr-metadata" and {
                    "ibkr.choke.qualify": "qualification",
                    "ibkr.choke.qualify_many": "qualification",
                    "ibkr.metadata.company_name": "company_name",
                    "ibkr.metadata.symbol_search": "symbol_search"}.get(producer) != envelope.method:
                return False
            return capability_allows(self.test_capability, producer, envelope.variant, envelope.endpoint)
        return False

    def bind_account(self, worker_id, governor_identity):
        """An operation worker cannot silently change accounts on reconnect.

        A new account requires an explicitly new worker ID or a new operation.
        Only identity is retained here; adapters retain no request context.
        """
        worker_id = identity(worker_id)
        with self._binding_lock:
            previous = self._account_bindings.setdefault(worker_id, governor_identity)
            if previous != governor_identity:
                raise RequestRefused("worker account changed; create an explicit new worker binding")

    def worker(self, worker_id: str):
        from fetch_operations import check_worker
        check_worker(self, worker_id)
        return FetchWorker(self, identity(worker_id))

    def record_publication_refusal(self, ids, error):
        """Result durability does not assert delivery; retain that distinction.

        Root companion evidence persists this list even when authority expires.
        Ledger schema1 is unchanged; a missing companion is not delivery proof.
        """
        with self._binding_lock:
            self._publication_failures.append({
                **ids, "state": "returned_unpublished",
                "error": f"{type(error).__name__}: {error}"[:500]})

    def publication_refusals(self):
        with self._binding_lock:
            return [dict(item) for item in self._publication_failures]

    def seal(self):
        from fetch_operations import check_seal
        check_seal(self)
        return self.ledger.seal()


@dataclass(frozen=True)
class FetchWorker:
    context: FetchRunContext
    worker_id: str

    def request(self, producer_id: str, envelope: Mapping):
        from fetch_operations import check_worker
        check_worker(self.context, self.worker_id)
        request = parse_envelope(envelope)
        if not self.context.permits(producer_id, request):
            raise RequestRefused("producer is not authorized for this A1 variant")
        return LogicalRequest(self, producer_id, uuid.uuid4().hex, request)


def row_evidence(rows: list[dict], *, key="timestamp") -> dict:
    stamps = [row[key] for row in rows]
    # Canonical UTC timestamps and YYYY-MM-DD labels both sort chronologically.
    return {"count": len(rows), "min": min(stamps) if stamps else None,
            "max": max(stamps) if stamps else None, "digest": digest_value(rows)}


def normalize_rows(response) -> list[dict]:
    """Default fixture wire format; production supplies an explicit normalizer.

    Every row is a finite JSON object with an aware timestamp. Serialize now
    to detach nested caller objects; no mutable transport response is returned.
    """
    if not isinstance(response, (list, tuple)):
        raise AuthorityError("series response must be a row sequence")
    rows = []
    for raw in response:
        if not isinstance(raw, dict) or "timestamp" not in raw:
            raise AuthorityError("response row lacks authoritative timestamp")
        row = dict(raw)
        row["timestamp"] = instant(row["timestamp"]).astimezone(timezone.utc).isoformat()
        rows.append(strict_json(canonical_bytes(row)))
    return rows


def filter_rows(rows: list[dict], envelope: Envelope, context: FetchRunContext) -> list[dict]:
    accepted = []
    base, _, _ = parse_token(envelope.token)
    for row in rows:
        stamp = aware_ny(datetime.fromisoformat(row["timestamp"]))
        try:
            if not context.authority.accepts_label(envelope.token, stamp, context.captured_now):
                continue
            if base == "1d":
                close = context.authority.window(envelope.token, stamp.date())[1]
                in_range = (envelope.intended_start.date() <= stamp.date()
                            and close <= envelope.intended_end)
            else:
                close = context.authority.window(envelope.token, stamp.date())[1]
                period_end = min(stamp + timedelta(seconds=BAR_SIZES[base][1]), close)
                in_range = (envelope.intended_start <= stamp < envelope.intended_end
                            and period_end <= envelope.intended_end)
            if in_range:
                accepted.append(row)
        except CalendarUnsupported:
            continue  # Unexpected out-of-coverage response rows are observed, not consumed.
    return accepted


def unfiltered_diagnostic_evidence(rows: list[dict], envelope: Envelope,
                                   context: FetchRunContext) -> dict:
    """Count the whole response before RTH/settlement filtering can hide it."""
    if envelope.variant != "ibkr-bars-unfiltered":
        raise RequestRefused("pre-filter evidence requires the unfiltered variant")
    outside = []
    outside_count = 0
    for row in rows:
        stamp = aware_ny(datetime.fromisoformat(row["timestamp"]))
        try:
            window = context.authority.window(envelope.token, stamp.date())
        except CalendarUnsupported:
            window = None
        if window is None or not window[0] <= stamp < window[1]:
            outside_count += 1
            if len(outside) < 5:
                outside.append(stamp.isoformat())
    return {"rows_received": len(rows), "rows_outside_rth": outside_count,
            "first_outside_timestamps": outside}


@dataclass(frozen=True)
class LogicalRequest:
    worker: FetchWorker
    producer_id: str
    logical_id: str
    envelope: Envelope
    _terminal: object = field(default_factory=threading.Event, init=False, repr=False, compare=False)
    _attempt_lock: object = field(default_factory=threading.Lock, init=False, repr=False, compare=False)
    _http_attempts: object = field(default_factory=lambda: count(1), init=False, repr=False, compare=False)

    def execute(self, transport, *, acquire_turn, normalizer=None, governor=None,
                cancel=None, attempt_evidence=None):
        if not self._attempt_lock.acquire(blocking=False):
            raise RequestRefused("logical request already has an active attempt")
        try:
            if self._terminal.is_set():
                raise RequestRefused("logical request is terminal")
            try:
                from fetch_operations import request_scope
                with request_scope(self.worker):
                    return self._execute(transport, acquire_turn=acquire_turn,
                        normalizer=normalizer, governor=governor, cancel=cancel,
                        attempt_evidence=attempt_evidence)
            except (AuthorityError, LedgerError, RequestCancelled,
                    ResponseQuarantined):
                self._terminal.set()
                raise
            except BaseException as exc:
                if not isinstance(exc, Exception):
                    self._terminal.set()
                raise
        finally:
            self._attempt_lock.release()

    def _execute(self, transport, *, acquire_turn, normalizer=None, governor=None,
                 cancel=None, attempt_evidence=None):
        """One synchronous physical attempt. Call again explicitly for a retry.

        acquire_turn returns its measured nonnegative wait seconds; production
        wiring cannot omit it. Async bridging belongs to A1-3, not this seam.
        """
        context = self.worker.context
        if not isinstance(context, FetchRunContext):
            raise RequestRefused("request has no FetchRunContext")
        if context.operation_id != context.ledger.operation_id:
            raise RequestRefused("context/ledger operation mismatch")
        if not context.permits(self.producer_id, self.envelope):
            raise RequestRefused("producer is not authorized for this A1 variant")
        if governor is not None and self.envelope.variant.startswith("ibkr-"):
            context.bind_account(self.worker.worker_id, governor.identity)
        ids = dict(operation_id=context.operation_id, worker_id=self.worker.worker_id,
                   producer_id=self.producer_id, logical_id=self.logical_id,
                   attempt_id=uuid.uuid4().hex)
        decision = decide(self.envelope, context.authority, context.horizons,
                          captured_now=context.captured_now)
        if (self.envelope.variant == "http-series"
                and self.envelope.endpoint == "stockanalysis.history"):
            from fetch_http_labels import verified_stockanalysis_attempt_evidence
            attempt_evidence = verified_stockanalysis_attempt_evidence(
                context, self.envelope, attempt_evidence)
        elif attempt_evidence is not None:
            raise RequestRefused("attempt evidence is not valid for this endpoint")
        if self.envelope.variant.startswith("http-"):
            expected = context.governors.provider(self.envelope.endpoint)
            if governor is not expected:
                raise RequestRefused("HTTP request lacks its canonical provider governor")
            acquire_turn = lambda: governor.wait_turn(cancel)
        wait = 0.0
        if decision.effective is not None:
            wait = acquire_turn()
            if type(wait) not in {int, float} or not math.isfinite(wait) or wait < 0:
                raise RequestRefused("pacer must return a finite nonnegative wait")
        payload = {"requested": decision.requested.wire(),
                   "effective": decision.effective.wire() if decision.effective else None,
                   "decision": decision.state, "reason": decision.reason,
                   "horizon": decision.horizon.isoformat() if decision.horizon else None,
                   "authority_fingerprint": context.authority.fingerprint,
                   "table_digest": context.authority.table_digest,
                   "consensus_digest": context.authority.consensus_digest,
                   "captured_now": context.captured_now.isoformat(),
                   "pacer_wait_seconds": wait}
        if governor is not None:
            payload.update(governor.evidence())
        if attempt_evidence is not None:
            payload["http_request"] = attempt_evidence
        # A callback/pacer may have closed or tampered with a test guard.
        if not context.permits(self.producer_id, self.envelope):
            raise RequestRefused("producer authorization expired before send")
        context.ledger.decision(ids, payload)  # Must be durable before transport.
        if decision.effective is None:
            context.ledger.result(ids, {"outcome": "empty", "refusal": decision.reason})
            raise RequestRefused(f"{decision.state}: {decision.reason}")
        response, observed, accepted, error = None, None, None, None
        http = decision.effective.variant.startswith("http-")
        http_attempt, http_error, http_policy_fault = None, None, None
        try:
            # Durability hooks may run arbitrary callbacks. Recheck the live
            # capability after the decision is durable, immediately before send.
            if not context.permits(self.producer_id, self.envelope):
                raise RequestRefused("producer authorization expired before transport")
            if cancel is not None and cancel.is_set():
                raise RequestCancelled("request cancelled before transport")
            if http:
                http_attempt = next(self._http_attempts)
            response = transport(decision.effective)
            if http and (not isinstance(response, bytes) or normalizer is None):
                raise RequestRefused("HTTP execution requires raw bytes and an explicit normalizer")
            if (decision.effective.variant == "http-series"
                    and decision.effective.endpoint == "stockanalysis.history"):
                from fetch_http_labels import (filter_stockanalysis_rows,
                                               normalize_stockanalysis_rows)
                observed = normalize_stockanalysis_rows(normalizer(response))
                accepted = filter_stockanalysis_rows(observed, decision.effective, context)
                result = {"outcome": "returned" if observed else "empty",
                          "observed": row_evidence(observed, key="label"),
                          "accepted": row_evidence(accepted, key="label"),
                          "dropped": len(observed) - len(accepted)}
            elif decision.effective.variant in {"ibkr-bars", "ibkr-bars-unfiltered",
                                                "http-series"}:
                # Re-normalize the adapter output to enforce canonical finite evidence.
                observed = normalize_rows(normalizer(response) if normalizer else response)
                accepted = filter_rows(observed, decision.effective, context)
                result = {"outcome": "returned" if observed else "empty",
                          "observed": row_evidence(observed), "accepted": row_evidence(accepted),
                          "dropped": len(observed) - len(accepted)}
                if decision.effective.variant == "ibkr-bars-unfiltered":
                    result.update(unfiltered_diagnostic_evidence(
                        observed, decision.effective, context))
                    result["rows_accepted"] = len(accepted)
            else:
                # Qualification/head normalizer must produce JSON metadata, never bars.
                accepted = strict_json(canonical_bytes(normalizer(response) if normalizer else response))
                result = {"outcome": "empty" if accepted is None else "returned",
                          "response_digest": digest_value(accepted)}
            if http:
                result["raw_response_digest"] = hashlib.sha256(response).hexdigest()
        except Exception as exc:
            # Import at call time: the bridge imports this module during setup.
            # Provider error hooks are observers, not another worker invocation.
            from fetch_ibkr_bridge import without_authority
            error = exc
            try:
                message = without_authority(str)(exc)
            except BaseException as formatting:
                # Diagnostics cannot replace an established terminal stop.
                # A NEW terminal formatter failure must not become retryable
                # merely because its original provider error was ordinary.
                if not isinstance(exc, (AuthorityError, LedgerError,
                                        RequestCancelled, ResponseQuarantined)) and (
                        isinstance(formatting, (AuthorityError, LedgerError, RequestCancelled))
                        or not isinstance(formatting, Exception)):
                    error = formatting
                message = "exception details unavailable"
            result = {"outcome": "timeout" if isinstance(error, TimeoutError) else "error",
                      "error_type": type(error).__name__, "error": message}
            if http and isinstance(error, HTTPError):
                from fetch_governors import http_error_evidence
                try:
                    http_error = without_authority(http_error_evidence)(error)
                except BaseException as metadata_error:
                    http_error = {"status": None, "retry_after": None,
                                  "metadata_error": "HTTP header observer failed"}
                    http_policy_fault = (metadata_error if isinstance(metadata_error,
                        (AuthorityError, LedgerError, RequestCancelled)) or
                        not isinstance(metadata_error, Exception) else
                        AuthorityError("invalid HTTP error metadata"))
                http_error["attempt"] = http_attempt
                result["http_error"] = http_error
        try:
            context.ledger.result(ids, result)  # Must be durable before any publication.
        except LedgerError:
            quarantine = observed if observed is not None else response
            if isinstance(response, bytes):
                # JSON cannot encode raw HTTP bytes (metadata has no observed
                # rows). Preserve them losslessly even when result fsync fails.
                quarantine = {"raw_response_base64": base64.b64encode(response).decode("ascii"),
                              "observed": observed}
            context.ledger.quarantine(ids["attempt_id"], quarantine)
            raise
        if error is not None:
            if isinstance(response, bytes) and self.envelope.variant.startswith("http-"):
                quarantine_path = context.ledger.quarantine(ids["attempt_id"], {
                    "raw_response_base64": base64.b64encode(response).decode("ascii"),
                    "observed": observed})
                if isinstance(error, ResponseQuarantined):
                    if quarantine_path is None:
                        raise LedgerError("StockAnalysis response quarantine failed") from error
                    error.quarantine_path = _portable_quarantine_locator(
                        quarantine_path)
            elif isinstance(error, ResponseQuarantined):
                raise LedgerError("StockAnalysis response has no quarantinable bytes") from error
            if http_policy_fault is not None:
                raise http_policy_fault from error
            if http_error is not None:
                if "metadata_error" in http_error:
                    status = http_error["status"]
                    if type(status) is int and (status in (408, 429) or 500 <= status <= 599):
                        # Metadata is unusable, not permission to ignore the
                        # status. A zero server delay selects the local floor;
                        # keep the original metadata error in durable evidence.
                        governor.http_backoff(http_error["status"],
                            retry_after="0", attempt=http_attempt, max_wait_s=60.0)
                    raise AuthorityError(http_error["metadata_error"]) from error
                # Result and any rejected bytes are durable before policy can
                # stop. The shared governor retains even a too-long deadline.
                try:
                    governor.http_backoff(http_error["status"],
                        retry_after=http_error["retry_after"], attempt=http_attempt,
                        max_wait_s=60.0)
                except AuthorityError as policy_error:
                    raise policy_error from error
            raise error
        # Bound operations also retain authority across transport, normalization
        # and result durability callbacks, up to detached publication.
        from fetch_operations import check_request
        try:
            check_request(context, self.producer_id)
        except BaseException as exc:
            context.record_publication_refusal(ids, exc)
            raise
        return accepted
