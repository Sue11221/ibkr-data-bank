"""Guarded identity/reuse reference gate for DATA_INTEGRITY_HARDENING WS4.

USAGE (Row 61 doc): a subcommand is REQUIRED — `synthetic` or `probe`.
Invoked bare, argparse exits 2: that is a USAGE error, not a failing gate, and
a batch runner must not record it as red. Batch/automated runs use `synthetic`
(status=pass; writes to a temporary directory only). `run_gates.py` encodes
exactly this contract in REFERENCE_ARGUMENT_REQUIREMENTS.

The production probe is offline and manifest-only.  It acquires the shared
operation gate for the whole scan, fingerprints every manifest it reads before
and after, fails closed on unreadable current manifests, and can write one JSON
artifact outside the bank.  It never changes a bank manifest, bar file, or
sidecar.

Exit codes:

* 0 -- the bank is clean and all synthetic acceptance checks pass;
* 1 -- the scan is authoritative but reports identity findings;
* 2 -- the gate, guard, manifest coverage, arguments, or output failed.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))

import operation_gate as og
import stock_storage as ss


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BANK = PROJECT_ROOT / ss.STORAGE_DIR_NAME
PROBE_GATE_MODE = "identity_sweep"
PROBE_GATE_OWNER = "row5-identity-read-only-sweep"
SCHEMA_VERSION = 2
QUARANTINE_REASON_NAME = "QUARANTINE_REASON.txt"


class ReferenceError(RuntimeError):
    """The identity reference could not produce authoritative evidence."""


def _is_within(path, root):
    try:
        resolved_path = os.path.normcase(str(Path(path).resolve()))
        resolved_root = os.path.normcase(str(Path(root).resolve()))
        return os.path.commonpath([resolved_path, resolved_root]) == resolved_root
    except ValueError:
        return False


def _hash_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


def _json_digest(payload):
    encoded = json.dumps(
        payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _utc_now():
    return datetime.now(timezone.utc).isoformat(
        timespec="milliseconds").replace("+00:00", "Z")


def _file_stamp(path):
    path = Path(path)
    stat = path.stat()
    return {
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sha256": _hash_file(path),
    }


def _canonical_active_dirs(root):
    try:
        entries = sorted(Path(root).iterdir(), key=lambda path: path.name.casefold())
    except OSError as exc:
        raise ReferenceError(f"cannot list bank: {exc}") from exc
    return [
        path for path in entries
        if path.is_dir() and not path.name.startswith("_")
        and ss.TICKER_DIR_RE.match(path.name)
    ]


def _read_manifest(path, errors, label):
    path = Path(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        errors.append(f"{label}: {type(exc).__name__}: {exc}")
        return None
    if not isinstance(payload, dict):
        errors.append(f"{label}: manifest is not an object")
        return None
    return payload


def _conid(manifest):
    value = (manifest or {}).get("conid")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.isdecimal():
            return int(text)
    return None


def _aliases(manifest):
    raw = (manifest or {}).get("aliases") or []
    if not isinstance(raw, list):
        return set()
    return {str(value).strip().upper() for value in raw if str(value).strip()}


def _relative(path, root):
    try:
        return Path(path).resolve().relative_to(Path(root).resolve()).as_posix()
    except (OSError, ValueError):
        return str(path)


def _quarantine_reason(manifest_path, qroot, project_root):
    current = Path(manifest_path).parent
    qroot = Path(qroot)
    while True:
        candidate = current / QUARANTINE_REASON_NAME
        if candidate.is_file():
            try:
                payload = candidate.read_bytes()
            except OSError as exc:
                return None, f"{type(exc).__name__}: {exc}"
            if not payload.strip():
                return None, "quarantine reason is empty"
            stamp = _file_stamp(candidate)
            stamp["path"] = _relative(candidate, project_root)
            return stamp, None
        if current == qroot:
            return None, "quarantine reason is missing"
        if qroot not in current.parents:
            return None, "quarantine manifest escapes the quarantine root"
        current = current.parent


def _quarantine_records(project_root, errors=None):
    errors = errors if errors is not None else []
    out = {}
    qroot = Path(project_root) / "_quarantine"
    if not qroot.exists():
        return out
    try:
        manifests = sorted(
            qroot.rglob(ss.MANIFEST_NAME), key=lambda path: str(path).casefold())
    except OSError as exc:
        errors.append(f"quarantine: {type(exc).__name__}: {exc}")
        return out
    for manifest_path in manifests:
        manifest = _read_manifest(
            manifest_path, errors,
            f"quarantine {manifest_path.relative_to(qroot)}")
        if manifest is None:
            continue
        conid = _conid(manifest)
        if conid is None or conid <= 0:
            errors.append(
                f"quarantine {manifest_path.relative_to(qroot)}: "
                "missing or invalid conId")
            continue
        symbol = (manifest.get("symbol") or manifest_path.parent.name)
        manifest_stamp = _file_stamp(manifest_path)
        manifest_stamp["path"] = _relative(manifest_path, project_root)
        reason, reason_error = _quarantine_reason(
            manifest_path, qroot, project_root)
        record = {
            "symbol": str(symbol),
            "manifest": manifest_stamp,
            "reason": reason,
        }
        if reason_error:
            record["reason_error"] = reason_error
        out.setdefault(conid, []).append(record)
    return {key: values for key, values in sorted(out.items())}


def _quarantined_conids(project_root, errors=None):
    records = _quarantine_records(project_root, errors)
    return {
        key: sorted({record["symbol"] for record in values})
        for key, values in records.items()
    }


def quarantined_conids(project_root=PROJECT_ROOT):
    """Every readable conId under project-root ``_quarantine``."""
    return _quarantined_conids(project_root)


def _manifest_day(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 64:
        raise ValueError("manifest first timestamp is missing or malformed")
    token = value.strip().split()[0]
    try:
        return datetime.strptime(token, "%m/%d/%Y").date()
    except ValueError:
        try:
            parsed = date.fromisoformat(token)
        except ValueError as exc:
            raise ValueError(
                f"manifest first timestamp is invalid: {value!r}") from exc
        if parsed.isoformat() != token:
            raise ValueError(
                f"manifest first timestamp is invalid: {value!r}")
        return parsed


def _month_key(value):
    if not isinstance(value, str) or len(value) != 7 or value[4] != "-":
        return None
    try:
        year, month = int(value[:4]), int(value[5:])
    except ValueError:
        return None
    if not (1000 <= year <= 2999 and 1 <= month <= 12):
        return None
    return f"{year:04d}-{month:02d}"


def _required_text(note, field):
    value = note.get(field) if isinstance(note, dict) else None
    if not isinstance(value, str) or not value.strip() or len(value) > 1000:
        raise ValueError(f"identity correction {field} is missing or malformed")
    return value.strip()


def _interval_proofs(manifest, ticker):
    proofs = []
    errors = []
    intervals = manifest.get("intervals") if isinstance(manifest, dict) else None
    if not isinstance(intervals, dict):
        return [], ["active manifest intervals are malformed"]
    for interval, section in sorted(intervals.items()):
        label = f"{ticker} {interval}"
        if (not isinstance(interval, str)
                or ss.INTERVAL_RE.fullmatch(interval) is None):
            errors.append(f"{label}: interval token is malformed")
            continue
        months = section.get("months") if isinstance(section, dict) else None
        if not isinstance(months, dict):
            errors.append(f"{label}: month state is malformed")
            continue
        present = []
        interval_bad = False
        for month, entry in sorted(months.items()):
            if _month_key(month) != month or not isinstance(entry, dict):
                errors.append(f"{label}: month entry {month!r} is malformed")
                interval_bad = True
                continue
            status = entry.get("status")
            if status not in {"present", "MISSING", "format-error"}:
                errors.append(f"{label}: month {month} has invalid status")
                interval_bad = True
                continue
            if status == "format-error":
                errors.append(f"{label}: month {month} is unreadable")
                interval_bad = True
                continue
            if status != "present":
                continue
            rows = entry.get("rows")
            if isinstance(rows, bool) or not isinstance(rows, int) or rows <= 0:
                errors.append(f"{label}: present month {month} is not non-empty")
                interval_bad = True
                continue
            try:
                first_day = _manifest_day(entry.get("first"))
            except ValueError as exc:
                errors.append(f"{label}: month {month}: {exc}")
                interval_bad = True
                continue
            if first_day.strftime("%Y-%m") != month:
                errors.append(
                    f"{label}: first stored day disagrees with month {month}")
                interval_bad = True
                continue
            present.append((month, first_day))
        if not present:
            continue
        if interval_bad:
            continue
        try:
            records = ss.identity_correction_records(
                manifest, interval, ticker=ticker)
            listing = [
                (note, boundary) for note, boundary in records
                if note.get("type") == "identity_listing_truncation"
            ]
            if not listing:
                raise ValueError(
                    "no applicable identity_listing_truncation correction")
            note, floor = max(
                enumerate(listing), key=lambda item: (item[1][1], item[0]))[1]
            run = _required_text(note, "run")
            snapshot = _required_text(note, "snapshot")
        except (ss.StorageError, ValueError) as exc:
            errors.append(f"{label}: {exc}")
            continue
        first_month, first_day = min(present, key=lambda item: item[1])
        proofs.append({
            "interval": interval,
            "present_month_count": len(present),
            "first_month": first_month,
            "first_day": first_day.isoformat(),
            "identity_floor": floor.isoformat(),
            "correction": {
                "type": "identity_listing_truncation",
                "run": run,
                "cutover": floor.isoformat(),
                "snapshot": snapshot,
            },
            "on_or_after_floor": first_day >= floor,
        })
        if first_day < floor:
            errors.append(
                f"{label}: first stored day {first_day.isoformat()} precedes "
                f"identity floor {floor.isoformat()}")
    if not proofs and not errors:
        errors.append("active manifest has no non-empty interval to prove")
    return proofs, errors


def _reuse_resolution(root, project_root, ticker, conid, manifest, records):
    errors = []
    if _conid(manifest) != conid:
        errors.append("active conId changed during identity evaluation")
    if str(manifest.get("symbol") or "").strip().upper() != ticker:
        errors.append("active manifest symbol does not match its ticker folder")
    folder = manifest.get("folder")
    if folder is not None and str(folder).strip().upper() != ticker:
        errors.append("active manifest folder identity is inconsistent")

    active_path = Path(root) / ticker / ss.MANIFEST_NAME
    try:
        active_stamp = _file_stamp(active_path)
        active_stamp["path"] = _relative(active_path, root)
        current = json.loads(active_path.read_text(encoding="utf-8"))
        if current != manifest:
            raise ValueError("active manifest changed while proof was derived")
    except (OSError, ValueError) as exc:
        active_stamp = None
        errors.append(f"active manifest provenance failed: {exc}")

    quarantine_manifests = []
    quarantine_reasons = []
    seen_reasons = set()
    if not records:
        errors.append("matching quarantine manifest is missing")
    for record in records or []:
        manifest_stamp = record.get("manifest")
        if isinstance(manifest_stamp, dict):
            quarantine_manifests.append(dict(manifest_stamp))
        else:
            errors.append("quarantine manifest provenance is malformed")
        reason = record.get("reason")
        if not isinstance(reason, dict):
            errors.append(str(record.get("reason_error") or
                              "quarantine reason is missing"))
            continue
        key = (reason.get("path"), reason.get("sha256"))
        if key not in seen_reasons:
            seen_reasons.add(key)
            quarantine_reasons.append(dict(reason))

    proofs, proof_errors = _interval_proofs(manifest, ticker)
    errors.extend(proof_errors)
    corrections = []
    seen_corrections = set()
    for proof in proofs:
        correction = proof["correction"]
        key = (correction["run"], correction["cutover"],
               correction["snapshot"])
        if key not in seen_corrections:
            seen_corrections.add(key)
            corrections.append(dict(correction))
    return {
        "ticker": ticker,
        "conid": conid,
        "resolution": "resolved" if not errors else "unresolved",
        "active_manifest": active_stamp,
        "quarantine_symbols": sorted({
            record.get("symbol") for record in records or []
            if record.get("symbol")
        }),
        "quarantine_manifests": quarantine_manifests,
        "quarantine_reasons": quarantine_reasons,
        "corrections": corrections,
        "intervals": proofs,
        "errors": errors,
    }


def audit_identity(root, project_root=None, *, strict=False):
    """Audit canonical active ticker manifests for high-precision reuse signals.

    ``strict=True`` refuses partial evidence if a canonical active manifest or a
    discovered quarantine manifest is unreadable or structurally invalid.
    """
    root = Path(root).resolve()
    project_root = (Path(project_root).resolve() if project_root is not None
                    else root.parent)
    errors = []
    active = _canonical_active_dirs(root)
    by_conid = {}
    aliases = {}
    manifests = {}
    for ticker_dir in active:
        manifest_path = ticker_dir / ss.MANIFEST_NAME
        manifest = _read_manifest(
            manifest_path, errors, f"active {ticker_dir.name}")
        if manifest is None:
            continue
        conid = _conid(manifest)
        if conid is None or conid <= 0:
            errors.append(
                f"active {ticker_dir.name}: missing or invalid conId")
            continue
        raw_aliases = manifest.get("aliases")
        if raw_aliases is not None and not isinstance(raw_aliases, list):
            errors.append(f"active {ticker_dir.name}: aliases is not a list")
        by_conid.setdefault(conid, []).append(ticker_dir.name)
        aliases[ticker_dir.name] = _aliases(manifest)
        manifests[ticker_dir.name] = manifest

    duplicate = {}
    for conid, tickers in by_conid.items():
        if len(tickers) <= 1:
            continue
        renamed = all(
            any(other in aliases.get(ticker, set())
                or ticker in aliases.get(other, set())
                for other in tickers if other != ticker)
            for ticker in tickers
        )
        if not renamed:
            duplicate[conid] = sorted(tickers)

    quarantine_records = _quarantine_records(project_root, errors)
    quarantine = {
        conid: sorted({record["symbol"] for record in records})
        for conid, records in quarantine_records.items()
    }
    reuse = sorted(
        (ticker, conid)
        for conid, tickers in by_conid.items()
        if conid in quarantine
        for ticker in tickers
    )
    if strict and errors:
        detail = "; ".join(errors[:10])
        if len(errors) > 10:
            detail += f"; ... +{len(errors) - 10} more"
        raise ReferenceError(f"manifest coverage is incomplete: {detail}")
    resolved = []
    unresolved = []
    for ticker, conid in reuse:
        verdict = _reuse_resolution(
            root, project_root, ticker, conid, manifests.get(ticker) or {},
            quarantine_records.get(conid) or [])
        if verdict["resolution"] == "resolved":
            resolved.append(verdict)
        else:
            unresolved.append(verdict)
    return {
        "dup_conid": dict(sorted(duplicate.items())),
        "quarantine_reuse": reuse,
        "quarantine_reuse_resolved": resolved,
        "quarantine_reuse_unresolved": unresolved,
        "quarantined_conids": quarantine,
        "ticker_count": len(active),
        "manifest_count": len(manifests),
        "manifest_errors": errors,
        "manifests": manifests,
    }


def _interval_context(manifest):
    context = {}
    raw_intervals = (manifest or {}).get("intervals") or {}
    if not isinstance(raw_intervals, dict):
        return context
    for token, section in sorted(raw_intervals.items()):
        months = section.get("months") if isinstance(section, dict) else None
        month_keys = sorted(months) if isinstance(months, dict) else []
        context[str(token)] = {
            "month_count": len(month_keys),
            "first_month": month_keys[0] if month_keys else None,
            "last_month": month_keys[-1] if month_keys else None,
        }
    return context


def _flagged_context(audit):
    tickers = set()
    for values in (audit.get("dup_conid") or {}).values():
        tickers.update(values)
    tickers.update(item[0] for item in audit.get("quarantine_reuse") or [])
    context = {}
    for ticker in sorted(tickers):
        manifest = (audit.get("manifests") or {}).get(ticker) or {}
        corrections = []
        for item in manifest.get("data_corrections") or []:
            if isinstance(item, dict):
                corrections.append({
                    key: item.get(key)
                    for key in ("type", "cutover", "day", "run")
                    if item.get(key) is not None
                })
        context[ticker] = {
            "conid": _conid(manifest),
            "symbol": manifest.get("symbol"),
            "intervals": _interval_context(manifest),
            "data_corrections": corrections,
        }
    return context


def _guard_snapshot(root, project_root):
    root = Path(root).resolve()
    project_root = Path(project_root).resolve()
    active_dirs = _canonical_active_dirs(root)
    active = {}
    for ticker_dir in active_dirs:
        path = ticker_dir / ss.MANIFEST_NAME
        active[ticker_dir.name] = (
            _file_stamp(path) if path.is_file() else {"exists": False})
    qroot = project_root / "_quarantine"
    quarantine = {}
    quarantine_reasons = {}
    if qroot.exists():
        for path in sorted(
                qroot.rglob(ss.MANIFEST_NAME), key=lambda item: str(item).casefold()):
            quarantine[path.relative_to(qroot).as_posix()] = _file_stamp(path)
        for path in sorted(
                qroot.rglob(QUARANTINE_REASON_NAME),
                key=lambda item: str(item).casefold()):
            quarantine_reasons[path.relative_to(qroot).as_posix()] = (
                _file_stamp(path))
    return {
        "active_directories": [path.name for path in active_dirs],
        "active_manifests": active,
        "quarantine_manifests": quarantine,
        "quarantine_reasons": quarantine_reasons,
    }


def _write_manifest(ticker_dir, conid, symbol, aliases=None, *, first=None,
                    corrections=None, interval="1m"):
    ticker_dir.mkdir(parents=True, exist_ok=True)
    entry = {"status": "present"}
    if first is not None:
        entry.update({
            "rows": 1,
            "first": first,
            "last": first,
            "sha256": "a" * 64,
        })
    payload = {
        "conid": conid,
        "symbol": symbol,
        "folder": ticker_dir.name,
        "aliases": aliases or [],
        "intervals": {
            interval: {"months": {"2020-01": entry}},
        },
    }
    if corrections is not None:
        payload["data_corrections"] = list(corrections)
    (ticker_dir / ss.MANIFEST_NAME).write_text(
        json.dumps(payload), encoding="utf-8")


def _listing_correction(ticker, cutover="2020-01-02"):
    return {
        "type": "identity_listing_truncation",
        "ticker": ticker,
        "cutover": cutover,
        "run": f"fixture-{ticker.lower()}-listing-floor",
        "snapshot": f"_quarantine/{ticker}-fixture/snapshot",
    }


def _fixture_flags():
    """Prove duplicate/reuse detection and the rename exemption."""
    temp = Path(tempfile.mkdtemp(prefix="idfix_"))
    project_root = temp / "proj"
    bank = project_root / ss.STORAGE_DIR_NAME
    try:
        _write_manifest(bank / "AAA", 111, "AAA")
        _write_manifest(bank / "BBB", 111, "BBB")
        _write_manifest(bank / "CCC", 999, "CCC")
        _write_manifest(bank / "DDD", 222, "DDD")
        _write_manifest(bank / "OLD", 333, "OLD", aliases=["NEW"])
        _write_manifest(bank / "NEW", 333, "NEW", aliases=["OLD"])
        _write_manifest(project_root / "_quarantine" / "ZZZ", 999, "ZZZ")
        report = audit_identity(bank, project_root=project_root, strict=True)
        duplicate_ok = report["dup_conid"] == {111: ["AAA", "BBB"]}
        reuse_ok = report["quarantine_reuse"] == [("CCC", 999)]
        rename_ok = 333 not in report["dup_conid"]
        return duplicate_ok, reuse_ok, rename_ok, report
    finally:
        shutil.rmtree(temp, ignore_errors=True)


def real_bank_probe(root, *, project_root=None):
    root = Path(root).resolve()
    if not root.is_dir():
        raise ReferenceError(f"bank is not a directory: {root}")
    project_root = (Path(project_root).resolve() if project_root is not None
                    else root.parent)
    before = _guard_snapshot(root, project_root)
    audit = audit_identity(root, project_root=project_root, strict=True)
    duplicate_ok, reuse_ok, rename_ok, _fixture = _fixture_flags()
    after = _guard_snapshot(root, project_root)
    if before != after:
        raise ReferenceError("read-only guard changed while identity sweep ran")
    before_digest = _json_digest(before)
    after_digest = _json_digest(after)
    synthetic_pass = duplicate_ok and reuse_ok and rename_ok
    clean = (not audit["dup_conid"]
             and not audit["quarantine_reuse_unresolved"])
    return {
        "kind": "identity_reference_real_bank_probe",
        "status": "pass" if clean and synthetic_pass else "findings",
        "bank": str(root),
        "project_root": str(project_root),
        "write_mode": False,
        "ticker_count": audit["ticker_count"],
        "manifest_count": audit["manifest_count"],
        "manifest_errors": audit["manifest_errors"],
        "dup_conid": audit["dup_conid"],
        "quarantine_reuse": audit["quarantine_reuse"],
        "quarantine_reuse_resolved": audit["quarantine_reuse_resolved"],
        "quarantine_reuse_unresolved": audit["quarantine_reuse_unresolved"],
        "quarantined_conids": audit["quarantined_conids"],
        "flagged_context": _flagged_context(audit),
        "synthetic_checks": {
            "duplicate_detected": duplicate_ok,
            "quarantine_reuse_detected": reuse_ok,
            "legitimate_rename_exempt": rename_ok,
        },
        "read_only_guard_unchanged": True,
        "guard_before": before,
        "guard_after": after,
        "guard_before_sha256": before_digest,
        "guard_after_sha256": after_digest,
    }


def guarded_real_bank_probe(root, *, output=None, project_root=None,
                            gate_path=None):
    bank = Path(root).resolve()
    artifact = Path(output).resolve() if output is not None else None
    if artifact is not None and _is_within(artifact, bank):
        raise ReferenceError("probe output must stay outside the bank")
    if artifact is not None and artifact.exists():
        raise ReferenceError(f"refusing to overwrite probe artifact: {artifact}")
    with og.acquire(
            PROBE_GATE_MODE, owner=PROBE_GATE_OWNER,
            path=gate_path) as lease:
        started_at = _utc_now()
        started = time.monotonic()
        payload = real_bank_probe(bank, project_root=project_root)
        payload["schema_version"] = SCHEMA_VERSION
        payload["command"] = "probe"
        payload["exit_code"] = 0 if payload["status"] == "pass" else 1
        payload["started_at_utc"] = started_at
        payload["completed_at_utc"] = _utc_now()
        payload["elapsed_seconds"] = round(time.monotonic() - started, 6)
        payload["operation_gate"] = {
            "mode": lease.mode,
            "path": lease.path,
            "owner": PROBE_GATE_OWNER,
            "acquired": True,
            "artifact_written_under_lease": artifact is not None,
        }
        if artifact is not None:
            payload["artifact"] = str(artifact)
            _write_json(artifact, payload)
    return payload


def _write_json(path, payload):
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        # Publishing through a hard link is atomic and no-clobber: unlike
        # os.replace(), a target created by another process is never replaced.
        os.link(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return path


def _check(checks, name, passed, detail=None):
    checks.append({
        "name": name,
        "passed": bool(passed),
        "detail": None if passed else str(detail or "condition was false"),
    })


def synthetic_gate():
    checks = []
    with contextlib.redirect_stderr(io.StringIO()):
        try:
            _parser().parse_args([])
            bare_exit = None
        except SystemExit as exc:
            bare_exit = exc.code
    _check(
        checks,
        "bare CLI requires an explicit probe or synthetic command",
        bare_exit == 2,
        f"exit={bare_exit!r}",
    )

    duplicate_ok, reuse_ok, rename_ok, fixture = _fixture_flags()
    _check(checks, "fixture flags a duplicate conId", duplicate_ok, fixture)
    _check(checks, "fixture flags quarantined-conId reuse", reuse_ok, fixture)
    _check(checks, "fixture exempts a legitimate rename", rename_ok, fixture)

    with tempfile.TemporaryDirectory(prefix="idref_safety_") as temp_name:
        temp = Path(temp_name)
        resolution_project = temp / "resolution-project"
        resolution_bank = resolution_project / ss.STORAGE_DIR_NAME
        active_dir = resolution_bank / "RSLV"
        quarantine_dir = resolution_project / "_quarantine" / "RSLV-OLD"
        _write_manifest(
            active_dir, 7777, "RSLV", first="01/03/2020 09:30:00",
            corrections=[_listing_correction("RSLV")])
        _write_manifest(quarantine_dir, 7777, "RSLV-OLD")
        reason_path = quarantine_dir / QUARANTINE_REASON_NAME
        reason_path.write_text(
            "reviewed predecessor evidence\n", encoding="utf-8")
        resolved_audit = audit_identity(
            resolution_bank, project_root=resolution_project, strict=True)
        _check(
            checks,
            "correction-backed raw reuse resolves with provenance",
            resolved_audit["quarantine_reuse"] == [("RSLV", 7777)]
            and len(resolved_audit["quarantine_reuse_resolved"]) == 1
            and not resolved_audit["quarantine_reuse_unresolved"]
            and len(resolved_audit["quarantine_reuse_resolved"][0][
                "quarantine_manifests"]) == 1
            and len(resolved_audit["quarantine_reuse_resolved"][0][
                "quarantine_reasons"]) == 1,
            resolved_audit,
        )

        active_path = active_dir / ss.MANIFEST_NAME
        active_payload = json.loads(active_path.read_text(encoding="utf-8"))
        active_payload["intervals"]["1m"]["months"]["2020-01"][
            "first"] = "01/01/2020 09:30:00"
        active_path.write_text(json.dumps(active_payload), encoding="utf-8")
        pre_floor = audit_identity(
            resolution_bank, project_root=resolution_project, strict=True)
        _check(
            checks,
            "pre-floor drift revokes a resolved verdict",
            not pre_floor["quarantine_reuse_resolved"]
            and len(pre_floor["quarantine_reuse_unresolved"]) == 1
            and "precedes identity floor" in " ".join(
                pre_floor["quarantine_reuse_unresolved"][0]["errors"]),
            pre_floor,
        )

        active_payload["intervals"]["1m"]["months"]["2020-01"][
            "first"] = "01/03/2020 09:30:00"
        active_payload["data_corrections"] = []
        active_path.write_text(json.dumps(active_payload), encoding="utf-8")
        missing_correction = audit_identity(
            resolution_bank, project_root=resolution_project, strict=True)
        _check(
            checks,
            "missing listing correction remains unresolved",
            len(missing_correction["quarantine_reuse_unresolved"]) == 1
            and "no applicable identity_listing_truncation" in " ".join(
                missing_correction["quarantine_reuse_unresolved"][0][
                    "errors"]),
            missing_correction,
        )

        active_payload["data_corrections"] = [_listing_correction("RSLV")]
        active_path.write_text(json.dumps(active_payload), encoding="utf-8")
        reason_path.unlink()
        missing_reason = audit_identity(
            resolution_bank, project_root=resolution_project, strict=True)
        _check(
            checks,
            "missing quarantine reason remains unresolved",
            len(missing_reason["quarantine_reuse_unresolved"]) == 1
            and "reason is missing" in " ".join(
                missing_reason["quarantine_reuse_unresolved"][0]["errors"]),
            missing_reason,
        )

        reason_path.write_text(
            "reviewed predecessor evidence\n", encoding="utf-8")
        active_payload["data_corrections"][0]["cutover"] = "not-a-date"
        active_path.write_text(json.dumps(active_payload), encoding="utf-8")
        malformed = audit_identity(
            resolution_bank, project_root=resolution_project, strict=True)
        _check(
            checks,
            "malformed listing correction remains unresolved",
            len(malformed["quarantine_reuse_unresolved"]) == 1
            and "cutover is invalid" in " ".join(
                malformed["quarantine_reuse_unresolved"][0]["errors"]),
            malformed,
        )

        active_payload["data_corrections"] = [_listing_correction("RSLV")]
        active_payload["intervals"]["1d"] = {"months": {
            "2019-12": {
                "status": "present", "rows": 1,
                "first": "12/31/2019 00:00:00",
                "last": "12/31/2019 00:00:00", "sha256": "b" * 64,
            },
        }}
        active_path.write_text(json.dumps(active_payload), encoding="utf-8")
        new_interval = audit_identity(
            resolution_bank, project_root=resolution_project, strict=True)
        _check(
            checks,
            "new pre-floor interval fails the whole collision closed",
            len(new_interval["quarantine_reuse_unresolved"]) == 1
            and any(item["interval"] == "1d"
                    for item in new_interval[
                        "quarantine_reuse_unresolved"][0]["intervals"])
            and "precedes identity floor" in " ".join(
                new_interval["quarantine_reuse_unresolved"][0]["errors"]),
            new_interval,
        )

        active_payload["intervals"].pop("1d")
        active_path.write_text(json.dumps(active_payload), encoding="utf-8")
        mismatch = _reuse_resolution(
            resolution_bank, resolution_project, "RSLV", 8888,
            active_payload, _quarantine_records(resolution_project).get(7777))
        _check(
            checks,
            "conId drift fails closed without a cached resolution",
            mismatch["resolution"] == "unresolved"
            and "active conId changed" in " ".join(mismatch["errors"]),
            mismatch,
        )

        project_root = temp / "project"
        bank = project_root / ss.STORAGE_DIR_NAME
        _write_manifest(bank / "CLEAN", 444, "CLEAN")
        bad = bank / "BROKEN"
        bad.mkdir(parents=True)
        (bad / ss.MANIFEST_NAME).write_text("{not json", encoding="utf-8")
        try:
            audit_identity(bank, project_root=project_root, strict=True)
            strict_rejected = False
        except ReferenceError:
            strict_rejected = True
        _check(checks, "strict probe rejects unreadable active manifests",
               strict_rejected)
        shutil.rmtree(bad)

        invalid_conids = [
            ("MISSING", object()),
            ("NULL", None),
            ("BOOL", True),
            ("FRAC", 123.9),
            ("ZERO", 0),
            ("NEG", -1),
        ]
        for ticker, value in invalid_conids:
            invalid_dir = bank / ticker
            invalid_dir.mkdir()
            invalid_manifest = {
                "symbol": ticker, "aliases": [], "intervals": {},
            }
            if ticker != "MISSING":
                invalid_manifest["conid"] = value
            (invalid_dir / ss.MANIFEST_NAME).write_text(
                json.dumps(invalid_manifest), encoding="utf-8")
            try:
                audit_identity(bank, project_root=project_root, strict=True)
                conid_rejected = False
            except ReferenceError:
                conid_rejected = True
            _check(checks, f"strict probe rejects {ticker.lower()} active conIds",
                   conid_rejected)
            shutil.rmtree(invalid_dir)

        invalid_aliases = [
            ("ADICT", {}),
            ("ASTR", ""),
            ("AZERO", 0),
            ("ABOOL", False),
        ]
        for ticker, aliases in invalid_aliases:
            invalid_dir = bank / ticker
            invalid_dir.mkdir()
            (invalid_dir / ss.MANIFEST_NAME).write_text(
                json.dumps({
                    "conid": 445,
                    "symbol": ticker,
                    "aliases": aliases,
                    "intervals": {},
                }),
                encoding="utf-8",
            )
            try:
                audit_identity(bank, project_root=project_root, strict=True)
                aliases_rejected = False
            except ReferenceError:
                aliases_rejected = True
            _check(
                checks,
                f"strict probe rejects {ticker.lower()} non-list aliases",
                aliases_rejected,
            )
            shutil.rmtree(invalid_dir)

        invalid_quarantine = project_root / "_quarantine" / "BAD"
        invalid_quarantine.mkdir(parents=True)
        (invalid_quarantine / ss.MANIFEST_NAME).write_text(
            json.dumps({"symbol": "BAD", "aliases": [], "intervals": {}}),
            encoding="utf-8")
        try:
            audit_identity(bank, project_root=project_root, strict=True)
            quarantine_conid_rejected = False
        except ReferenceError:
            quarantine_conid_rejected = True
        _check(checks, "strict probe rejects missing quarantine conIds",
               quarantine_conid_rejected)
        shutil.rmtree(project_root / "_quarantine")

        artifact = temp / "identity-probe.json"
        gate_path = temp / "operation.lock"
        payload = guarded_real_bank_probe(
            bank, output=artifact, project_root=project_root,
            gate_path=gate_path)
        _check(checks, "guarded clean-bank probe passes",
               payload.get("status") == "pass", payload)
        _check(checks, "whole-run operation gate is recorded",
               (payload.get("operation_gate") or {}).get("mode")
               == PROBE_GATE_MODE, payload)
        metadata_ok = (
            payload.get("schema_version") == SCHEMA_VERSION
            and payload.get("command") == "probe"
            and payload.get("exit_code") == 0
            and str(payload.get("started_at_utc") or "").endswith("Z")
            and str(payload.get("completed_at_utc") or "").endswith("Z")
            and payload.get("elapsed_seconds", -1) >= 0
            and (payload.get("operation_gate") or {}).get("owner")
            == PROBE_GATE_OWNER
            and (payload.get("operation_gate") or {}).get("acquired") is True
            and (payload.get("operation_gate") or {}).get(
                "artifact_written_under_lease") is True
        )
        _check(checks, "artifact records provenance and exit contract",
               metadata_ok, payload)
        _check(checks, "read-only guard remains unchanged",
               payload.get("read_only_guard_unchanged") is True, payload)
        guard_before = payload.get("guard_before")
        guard_after = payload.get("guard_after")
        guard_evidence_ok = (
            guard_before == guard_after
            and payload.get("guard_before_sha256") == _json_digest(guard_before)
            and payload.get("guard_after_sha256") == _json_digest(guard_after)
            and payload.get("guard_before_sha256")
            == payload.get("guard_after_sha256")
        )
        _check(checks, "artifact embeds matching before/after bank guards",
               guard_evidence_ok, payload)
        loaded = json.loads(artifact.read_text(encoding="utf-8"))
        _check(checks, "artifact is outside the bank and round-trips",
               not _is_within(artifact, bank)
               and loaded.get("status") == "pass", loaded)
        try:
            guarded_real_bank_probe(
                bank, output=artifact, project_root=project_root,
                gate_path=gate_path)
            overwrite_rejected = False
        except ReferenceError:
            overwrite_rejected = True
        _check(checks, "existing artifacts are never overwritten",
               overwrite_rejected)

        busy_artifact = temp / "busy.json"
        with og.acquire("fetch", owner="synthetic-writer", path=gate_path):
            try:
                guarded_real_bank_probe(
                    bank, output=busy_artifact, project_root=project_root,
                    gate_path=gate_path)
                busy_rejected = False
            except og.OperationBusy:
                busy_rejected = True
        _check(checks, "competing operation fails closed with no artifact",
               busy_rejected and not busy_artifact.exists())

        try:
            guarded_real_bank_probe(
                bank, output=bank / "forbidden.json",
                project_root=project_root, gate_path=gate_path)
            inside_rejected = False
        except ReferenceError:
            inside_rejected = True
        _check(checks, "artifact inside the bank is refused", inside_rejected)

    return {
        "kind": "identity_reference_synthetic",
        "status": "pass" if all(item["passed"] for item in checks) else "fail",
        "check_count": len(checks),
        "checks": checks,
        "writes": "temporary directory only",
    }


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("probe", "synthetic"))
    parser.add_argument("--bank", default=str(DEFAULT_BANK))
    parser.add_argument("--project-root")
    parser.add_argument("--output")
    return parser


def main(argv=None):
    try:
        args = _parser().parse_args(argv)
        if args.command == "synthetic":
            payload = synthetic_gate()
            exit_code = 0 if payload["status"] == "pass" else 2
        else:
            payload = guarded_real_bank_probe(
                args.bank, output=args.output, project_root=args.project_root)
            exit_code = payload["exit_code"]
    except Exception as exc:  # noqa: BLE001 - bounded CLI failure envelope
        payload = {
            "kind": "identity_reference_error",
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
        }
        exit_code = 2
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
