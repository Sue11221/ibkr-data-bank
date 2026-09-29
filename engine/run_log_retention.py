"""Bound recurring Run Logs artifacts without touching unknown evidence.

The caller supplies either the project root (which owns ``Run Logs``) or the
bank root beside it.  Planning is read-only; execution is best-effort and
never lets retention failure escape into a completed market-data run.
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import json
import os
import stat
import tempfile
from pathlib import Path


DEFAULT_MAX_TOTAL_BYTES = 100 * 1024 * 1024
DEFAULT_KEEP_PER_FAMILY = 8
DEFAULT_MIN_AGE_DAYS = 14

RUN_LOGS_NAME = "Run Logs"
AUDIT_NAME = "_retention_audit.json"
MAX_AUDIT_ENTRIES = 50
MAX_AUDIT_ITEMS = 100

# Deliberately narrow.  One-off diagnostics, reviewer evidence, lock files,
# watcher output, and external-tool logs are not inferred as disposable.
FAMILY_PATTERNS = (
    "fix-data-spot-probe-*.json",
    "fix-data-vol-value-reconcile-*.json",
)


def _error(exc):
    return f"{type(exc).__name__}: {exc}"[:500]


def _nonnegative_int(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _clock(value):
    if value is None:
        return dt.datetime.now().astimezone()
    if not isinstance(value, dt.datetime):
        raise TypeError("now must be a datetime")
    return value.astimezone() if value.tzinfo is None else value


def _run_logs_path(root):
    base = Path(root)
    if base.name.casefold() == RUN_LOGS_NAME.casefold():
        return base
    direct = base / RUN_LOGS_NAME
    if direct.exists():
        return direct
    sibling = base.parent / RUN_LOGS_NAME
    if sibling.exists():
        return sibling
    return direct


def _family(name):
    if name == AUDIT_NAME:
        return None
    for pattern in FAMILY_PATTERNS:
        if fnmatch.fnmatchcase(name, pattern):
            return pattern
    return None


def _inventory(run_logs):
    rows = []
    errors = []
    if not run_logs.exists():
        return rows, errors
    try:
        children = list(run_logs.iterdir())
    except OSError as exc:
        return rows, [{"path": str(run_logs), "error": _error(exc)}]
    for path in children:
        try:
            info = path.stat()
        except OSError as exc:
            errors.append({"path": str(path), "error": _error(exc)})
            continue
        if not stat.S_ISREG(info.st_mode):
            continue
        rows.append({
            "path": path,
            "name": path.name,
            "size": max(0, int(info.st_size)),
            "mtime": float(info.st_mtime),
            "mtime_ns": int(info.st_mtime_ns),
            "family": _family(path.name),
        })
    return rows, errors


def _build_plan(root, *, now, max_total_bytes, keep_per_family,
                min_age_days):
    now = _clock(now)
    max_total_bytes = _nonnegative_int(
        max_total_bytes, "max_total_bytes")
    keep_per_family = _nonnegative_int(
        keep_per_family, "keep_per_family")
    min_age_days = _nonnegative_int(min_age_days, "min_age_days")
    run_logs = _run_logs_path(root)
    rows, errors = _inventory(run_logs)
    total_bytes = sum(row["size"] for row in rows)

    protected = set()
    families = {}
    for row in rows:
        if row["family"] is not None:
            families.setdefault(row["family"], []).append(row)
    for members in families.values():
        newest = sorted(
            members, key=lambda row: (row["mtime_ns"], row["name"]),
            reverse=True)
        protected.update(
            row["path"] for row in newest[:keep_per_family])

    cutoff = now.timestamp() - min_age_days * 86400.0
    eligible = [
        row for row in rows
        if (row["family"] is not None
            and row["path"] not in protected
            and row["mtime"] <= cutoff)
    ]
    eligible.sort(key=lambda row: (row["mtime_ns"], row["name"]))

    planned = []
    projected = total_bytes
    # An incomplete inventory cannot safely justify deletion.
    if not errors:
        for row in eligible:
            if projected <= max_total_bytes:
                break
            planned.append(row)
            projected = max(0, projected - row["size"])

    return {
        "run_logs": run_logs,
        "now": now,
        "max_total_bytes": max_total_bytes,
        "keep_per_family": keep_per_family,
        "min_age_days": min_age_days,
        "total_bytes": total_bytes,
        "projected_bytes": projected,
        "over_cap": total_bytes > max_total_bytes,
        "errors": errors,
        "eligible": eligible,
        "planned": planned,
    }


def plan_prune(root, *, now=None,
               max_total_bytes=DEFAULT_MAX_TOTAL_BYTES,
               keep_per_family=DEFAULT_KEEP_PER_FAMILY,
               min_age_days=DEFAULT_MIN_AGE_DAYS):
    """Return a pure, deterministic retention plan without changing disk."""
    plan = _build_plan(
        root, now=now, max_total_bytes=max_total_bytes,
        keep_per_family=keep_per_family, min_age_days=min_age_days)
    return {
        "run_logs": str(plan["run_logs"]),
        "total_bytes": plan["total_bytes"],
        "projected_bytes": plan["projected_bytes"],
        "over_cap": plan["over_cap"],
        "candidates": [str(row["path"]) for row in plan["planned"]],
        "eligible": [str(row["path"]) for row in plan["eligible"]],
        "errors": list(plan["errors"]),
    }


def _read_audit(path):
    if not path.exists():
        return []
    value = json.loads(path.read_text(encoding="utf-8"))
    entries = value if isinstance(value, list) else value.get("entries")
    if not isinstance(entries, list):
        raise ValueError("retention audit entries must be a list")
    return entries[-MAX_AUDIT_ENTRIES:]


def _write_audit(path, entries):
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "entries": list(entries)[-MAX_AUDIT_ENTRIES:],
    }
    fd = None
    temporary = None
    try:
        fd, temporary = tempfile.mkstemp(
            prefix="._retention_audit-", suffix=".tmp",
            dir=str(path.parent))
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            fd = None
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if fd is not None:
            os.close(fd)
        if temporary is not None:
            try:
                Path(temporary).unlink()
            except OSError:
                pass


def _audit_entry(plan, result, before_bytes, after_bytes):
    deleted = list(result["deleted"])
    errors = list(result["errors"])
    return {
        "at": plan["now"].isoformat(timespec="seconds"),
        "before_bytes": before_bytes,
        "after_bytes_before_audit": after_bytes,
        "max_total_bytes": plan["max_total_bytes"],
        "keep_per_family": plan["keep_per_family"],
        "min_age_days": plan["min_age_days"],
        "deleted_count": len(deleted),
        "deleted": deleted[:MAX_AUDIT_ITEMS],
        "deleted_truncated": max(0, len(deleted) - MAX_AUDIT_ITEMS),
        "freed_bytes": result["freed_bytes"],
        "error_count": len(errors),
        "errors": errors[:MAX_AUDIT_ITEMS],
        "errors_truncated": max(0, len(errors) - MAX_AUDIT_ITEMS),
    }


def prune_run_logs(root, *, now=None,
                   max_total_bytes=DEFAULT_MAX_TOTAL_BYTES,
                   keep_per_family=DEFAULT_KEEP_PER_FAMILY,
                   min_age_days=DEFAULT_MIN_AGE_DAYS):
    """Apply retention best-effort and return deletion/error evidence.

    Unknown files are never candidates.  A clean under-cap call is a true
    no-op (including no audit growth); over-cap attempts are audit-recorded
    even when age/family floors prevent deletion.
    """
    result = {"deleted": [], "freed_bytes": 0, "errors": []}
    try:
        plan = _build_plan(
            root, now=now, max_total_bytes=max_total_bytes,
            keep_per_family=keep_per_family, min_age_days=min_age_days)
    except Exception as exc:  # noqa: BLE001 - public never-raise boundary
        result["errors"].append({"path": str(root), "error": _error(exc)})
        return result

    result["errors"].extend(plan["errors"])
    if not plan["over_cap"]:
        return result

    audit_path = plan["run_logs"] / AUDIT_NAME
    result["audit"] = str(audit_path)
    try:
        history = _read_audit(audit_path)
    except Exception as exc:  # noqa: BLE001 - corrupt evidence fails closed
        result["errors"].append({
            "path": str(audit_path), "error": _error(exc)})
        return result

    current_bytes = plan["total_bytes"]
    if not plan["errors"]:
        for row in plan["eligible"]:
            if current_bytes <= plan["max_total_bytes"]:
                break
            path = row["path"]
            try:
                size = max(0, int(path.stat().st_size))
                path.unlink()
            except OSError as exc:
                result["errors"].append({
                    "path": str(path), "error": _error(exc)})
                continue
            result["deleted"].append(str(path))
            result["freed_bytes"] += size
            current_bytes = max(0, current_bytes - size)

    entry = _audit_entry(
        plan, result, plan["total_bytes"], current_bytes)
    try:
        _write_audit(audit_path, history + [entry])
    except Exception as exc:  # noqa: BLE001 - visible, never run-fatal
        result["errors"].append({
            "path": str(audit_path), "error": _error(exc)})
    return result


__all__ = [
    "AUDIT_NAME",
    "DEFAULT_KEEP_PER_FAMILY",
    "DEFAULT_MAX_TOTAL_BYTES",
    "DEFAULT_MIN_AGE_DAYS",
    "FAMILY_PATTERNS",
    "plan_prune",
    "prune_run_logs",
]
