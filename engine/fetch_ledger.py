"""Serialized two-sided JSONL journal. No result is publishable before fsync.

A durable seal receipt is published only AFTER seal fsync succeeds. Without
that receipt even a complete-looking seal left by a failed fsync is unverified.
The later G5 audit adds producer and horizon semantics to this structural audit.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re
import threading

from fetch_authority import AuthorityError, canonical_bytes, strict_json


SCHEMA = 1
IDS = ("operation_id", "worker_id", "producer_id", "logical_id", "attempt_id")
_ID = re.compile(r"[A-Za-z0-9_.:-]{1,200}\Z")


class LedgerError(RuntimeError):
    """The operation is unverified and must stop consuming responses."""


def identity(value: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise LedgerError("invalid ledger identity")
    return value


def _track(event, pending, seen, logical, *, apply):
    if event["event"] == "seal":
        return
    attempt = event["attempt_id"]
    binding = tuple(event[key] for key in IDS)
    if event["event"] == "decision":
        if attempt in seen:
            raise LedgerError("duplicate attempt decision")
        group = event["logical_id"]
        signature = (event["worker_id"], event["producer_id"],
                     hashlib.sha256(canonical_bytes(event["payload"].get("requested"))).digest())
        if group in logical and logical[group] != signature:
            raise LedgerError("logical request changed across retries")
        if apply:
            seen.add(attempt)
            pending[attempt] = binding
            logical[group] = signature
    elif event["event"] == "result":
        if pending.get(attempt) != binding:
            raise LedgerError("result without matching decision or duplicate result")
        if apply:
            del pending[attempt]
    else:
        raise LedgerError("unknown event")


def _pairs(events):
    pending, seen, logical = {}, set(), {}
    for event in events:
        _track(event, pending, seen, logical, apply=True)
    return pending


class FetchLedger:
    """One writer shared by all workers; no lock is held across transport I/O.

    fault(event, stage) is an offline injection seam; stages bracket the actual
    append/flush/fsync calls. File failures and injection failures poison alike.
    """

    def __init__(self, directory: Path, operation_id: str, *, fault=None):
        self.operation_id = identity(operation_id)
        self._lock = threading.RLock()
        self._fault = fault or (lambda event, stage: None)
        self._count = 0
        self._pending, self._seen, self._logical = {}, set(), {}
        self._hash = hashlib.sha256()
        self._failed = None
        self._sealed = False
        self._closed = False
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        stem = "fetch-" + datetime.now(timezone.utc).strftime("%Y%m%d")
        for suffix in range(1_000_000):
            self.path = directory / f"{stem}-{suffix:06d}.jsonl"
            try:
                self._file = self.path.open("xb")
                break
            except FileExistsError:
                continue
        else:
            raise LedgerError("ledger filename space exhausted")
        self.receipt_path = self.path.with_suffix(".seal.json")

    @property
    def verified(self):
        with self._lock:
            return self._sealed and self._failed is None

    @property
    def failure(self):
        with self._lock:
            return self._failed

    def _live(self):
        if self._failed or self._sealed or self._closed:
            raise LedgerError(f"ledger unavailable: {self._failed or 'sealed/closed'}")

    def _append(self, kind, ids, payload):
        self._live()
        if set(ids) != set(IDS) or ids["operation_id"] != self.operation_id:
            raise LedgerError("event identity shape/operation mismatch")
        ids = {key: identity(value) for key, value in ids.items()}
        event = dict(ids, schema_version=SCHEMA, seq=self._count + 1,
                     timestamp=datetime.now(timezone.utc).isoformat(), event=kind,
                     payload=payload)
        # Serialize/detach before validation so callers cannot mutate the journal.
        encoded = canonical_bytes(event) + b"\n"
        event = strict_json(encoded)
        _track(event, self._pending, self._seen, self._logical, apply=False)
        try:
            self._fault(kind, "append")
            if self._file.write(encoded) != len(encoded):
                raise OSError("short ledger append")
            self._fault(kind, "flush")
            self._file.flush()
            self._fault(kind, "fsync")
            os.fsync(self._file.fileno())
        except Exception as exc:
            self._failed = f"{kind} durability failure: {type(exc).__name__}: {exc}"
            raise LedgerError(self._failed) from exc
        _track(event, self._pending, self._seen, self._logical, apply=True)
        self._count += 1
        self._hash.update(encoded)
        return event

    def decision(self, ids: dict, payload: dict):
        with self._lock:
            return self._append("decision", ids, payload)

    def result(self, ids: dict, payload: dict):
        with self._lock:
            return self._append("result", ids, payload)

    def seal(self):
        from fetch_operations import check_ledger_lifecycle
        check_ledger_lifecycle(self)
        with self._lock:
            self._live()
            if self._pending:
                raise LedgerError("outcome_unknown: cannot seal unmatched decisions")
            ids = {key: "__run__" for key in IDS}
            ids["operation_id"] = self.operation_id
            payload = {"prior_count": self._count, "prior_sha256": self._hash.hexdigest()}
            self._append("seal", ids, payload)
            receipt = {"schema_version": SCHEMA, "operation_id": self.operation_id,
                       "file_sha256": self._hash.hexdigest(), "event_count": self._count}
            pending = self.receipt_path.with_suffix(".pending")
            try:
                self._fault("receipt", "append")
                with pending.open("xb") as handle:
                    encoded = canonical_bytes(receipt) + b"\n"
                    if handle.write(encoded) != len(encoded):
                        raise OSError("short receipt append")
                    self._fault("receipt", "flush")
                    handle.flush()
                    self._fault("receipt", "fsync")
                    os.fsync(handle.fileno())
                self._fault("receipt", "publish")
                # Exclusive ledger names make the final receipt ours alone.
                if self.receipt_path.exists():
                    raise OSError("seal receipt already exists")
                pending.rename(self.receipt_path)
            except Exception as exc:
                self._failed = f"seal receipt failure: {type(exc).__name__}: {exc}"
                raise LedgerError(self._failed) from exc
            self._sealed = True
            return receipt

    def quarantine(self, attempt_id: str, response: object):
        """Best effort only; the original ledger failure always remains fatal."""
        with self._lock:
            path = self.path.with_suffix(f".{identity(attempt_id)}.quarantine.json")
            try:
                with path.open("xb") as handle:
                    handle.write(canonical_bytes({"failure": self._failed, "response": response}))
                    handle.flush()
                    os.fsync(handle.fileno())
                return path
            except Exception:
                return None

    def close(self):
        from fetch_operations import check_ledger_lifecycle
        check_ledger_lifecycle(self, closing=True)
        with self._lock:
            self._closed = True
            self._file.close()


def inspect_ledger(path: Path, *, require_seal=True) -> dict:
    """Reject malformed evidence; unsealed inspection never reports verified."""
    path = Path(path)
    try:
        data = path.read_bytes()
        lines = data.splitlines(keepends=True)
        events, prior, operation = [], hashlib.sha256(), None
        sealed = False
        for index, line in enumerate(lines, 1):
            if not line.endswith(b"\n") or not line.strip():
                raise LedgerError("truncated or empty ledger record")
            event = strict_json(line)
            if not isinstance(event, dict) or set(event) != set(IDS) | {
                    "schema_version", "seq", "timestamp", "event", "payload"}:
                raise LedgerError("event schema mismatch")
            if (type(event["schema_version"]) is not int or event["schema_version"] != SCHEMA
                    or type(event["seq"]) is not int or event["seq"] != index):
                raise LedgerError("schema version or sequence gap")
            for key in IDS:
                identity(event[key])
            operation = operation or event["operation_id"]
            if operation != event["operation_id"]:
                raise LedgerError("mixed operations")
            stamp = datetime.fromisoformat(event["timestamp"])
            if stamp.tzinfo is None or stamp.utcoffset() is None:
                raise LedgerError("naive event timestamp")
            if not isinstance(event["payload"], dict):
                raise LedgerError("payload must be an object")
            if sealed:
                raise LedgerError("record after seal")
            if event["event"] == "seal":
                if (event["payload"] != {"prior_count": index - 1, "prior_sha256": prior.hexdigest()}
                        or type(event["payload"]["prior_count"]) is not int
                        or any(event[key] != "__run__" for key in IDS if key != "operation_id")):
                    raise LedgerError("seal content/count/identity mismatch")
                sealed = True
            events.append(event)
            prior.update(line)
        pending = _pairs(events)
        receipt_path = path.with_suffix(".seal.json")
        verified = False
        if sealed and not pending and receipt_path.exists():
            receipt = strict_json(receipt_path.read_bytes())
            expected = {"schema_version": SCHEMA, "operation_id": operation,
                        "file_sha256": prior.hexdigest(), "event_count": len(events)}
            if receipt != expected or type(receipt["schema_version"]) is not int or type(receipt["event_count"]) is not int:
                raise LedgerError("durable seal receipt mismatch")
            verified = True
        if require_seal and not verified:
            raise LedgerError("UNVERIFIED: missing/failed seal or outcome_unknown")
        return {"verified": verified, "events": events,
                "outcome_unknown": sorted(pending), "operation_id": operation}
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        raise LedgerError(f"malformed/unreadable ledger: {exc}") from exc
