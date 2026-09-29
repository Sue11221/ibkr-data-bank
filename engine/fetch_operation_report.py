"""Durable operation companions; ledger schema1 remains unchanged.

The companion records root completion and known publication refusals. A sealed
ledger alone proves durable attempts, never successful root delivery. Reports
live beside their invocation's ledger or in an explicit evidence directory.
"""

import json
import os
from pathlib import Path
import tempfile

from fetch_ledger import LedgerError, inspect_ledger


class OperationReportError(LedgerError):
    pass


def evidence(context, purpose, *, outcome, error=None):
    from fetch_ibkr_bridge import without_authority
    def describe_error():
        try:
            return f"{type(error).__name__}: {error}"[:500]
        except BaseException:
            return "terminal failure; diagnostic unavailable"
    failure = (without_authority(describe_error)() if error is not None
               else context.ledger.failure)
    failures = context.publication_refusals()
    value = {
        "report_schema_version": 1,
        "operation_id": context.operation_id,
        "purpose": purpose,
        "path": str(context.ledger.path),
        "captured_now": context.captured_now.isoformat(),
        "ledger_verified": context.ledger.verified,
        "outcome": outcome,
        "failure": failure,
        "publication_refusals": failures,
        "outcome_unknown": [],
        "delivery_semantics": "returned ledger results do not prove root or caller delivery",
    }
    try:
        value["outcome_unknown"] = inspect_ledger(
            context.ledger.path, require_seal=False).get("outcome_unknown", [])
    except LedgerError as exc:
        value["inspection_error"] = str(exc)[:500]
    value["verified"] = bool(value["ledger_verified"] and outcome == "returned"
        and error is None and not value["failure"] and not failures
        and not value["outcome_unknown"] and "inspection_error" not in value)
    value["state"] = "VERIFIED" if value["verified"] else "UNVERIFIED"
    return value


def write_report(value, directory=None, *, fault=None):
    """Atomic, fsynced, invocation-specific report; failures always surface."""
    parent = Path(directory) if directory is not None else Path(value["path"]).parent
    target = parent / (value["operation_id"] + ".operation.json")
    temporary = None
    try:
        parent.mkdir(parents=True, exist_ok=True)
        if fault is not None:
            fault("report", "create")
        fd, temporary = tempfile.mkstemp(prefix=".operation-", suffix=".tmp", dir=parent)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
            if fault is not None:
                fault("report", "flush")
            stream.flush()
            if fault is not None:
                fault("report", "fsync")
            os.fsync(stream.fileno())
        if fault is not None:
            fault("report", "replace")
        os.replace(temporary, target)
        temporary = None
    except Exception as exc:
        raise OperationReportError(f"operation report could not be persisted: {type(exc).__name__}: {exc}") from exc
    finally:
        if temporary is not None:
            try:
                Path(temporary).unlink()
            except OSError:
                pass
    return str(target)
