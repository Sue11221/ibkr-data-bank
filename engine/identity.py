"""Offline identity/reuse audit for the storage bank.

This is DATA_INTEGRITY_HARDENING.md WS4 detector scope. It uses manifests and
the project-root quarantine folder only; it never resolves contracts or fetches
market data. The online add-stock refusal hook is a separate later step.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import sys
from pathlib import Path

import stock_storage as ss


IDENTITY_REPORT_FILE = "_identity_audit.json"
IDENTITY_REPORT_VERSION = 2
QUARANTINE_REASON_NAME = "QUARANTINE_REASON.txt"


def _coerce_conid(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _norm_symbol(value):
    text = str(value or "").strip().upper()
    return text or None


def _active_ticker_dirs(root):
    try:
        entries = sorted(Path(root).iterdir(), key=lambda p: p.name.lower())
    except OSError:
        return []
    return [
        p for p in entries
        if p.is_dir() and not p.name.startswith("_")
        and ss.TICKER_DIR_RE.match(p.name)
    ]


def _sha256(payload):
    return hashlib.sha256(payload).hexdigest()


def _relative_path(path, project_root):
    path = Path(path)
    project_root = Path(project_root)
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except (OSError, ValueError):
        return str(path)


def _file_provenance(path, project_root, *, require_content=False):
    path = Path(path)
    try:
        payload = path.read_bytes()
    except OSError as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if require_content and not payload.strip():
        return None, "file is empty"
    return {
        "path": _relative_path(path, project_root),
        "bytes": len(payload),
        "sha256": _sha256(payload),
    }, None


def _nearest_quarantine_reason(manifest_path, quarantine_root, project_root):
    """Return the nearest reason file shared by a quarantine snapshot tree."""
    manifest_path = Path(manifest_path)
    quarantine_root = Path(quarantine_root)
    current = manifest_path.parent
    while True:
        candidate = current / QUARANTINE_REASON_NAME
        if candidate.is_file():
            stamp, error = _file_provenance(
                candidate, project_root, require_content=True)
            return stamp, error
        if current == quarantine_root:
            return None, "quarantine reason is missing"
        if quarantine_root not in current.parents:
            return None, "quarantine manifest escapes the quarantine root"
        current = current.parent


def _quarantine_records(project_root=None):
    """Return readable quarantine manifest provenance grouped by conId."""
    project_root = Path(project_root) if project_root is not None else Path(".")
    qroot = project_root / "_quarantine"
    out = {}
    if not qroot.exists():
        return out
    try:
        manifests = sorted(
            qroot.rglob(ss.MANIFEST_NAME), key=lambda p: str(p).lower())
    except OSError:
        return out
    for mf in manifests:
        try:
            raw = mf.read_bytes()
            man = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeError, ValueError):
            continue
        if not isinstance(man, dict):
            continue
        conid = _coerce_conid(man.get("conid"))
        if conid is None:
            continue
        reason, reason_error = _nearest_quarantine_reason(
            mf, qroot, project_root)
        record = {
            "symbol": _manifest_symbol(man, mf.parent.name),
            "manifest": {
                "path": _relative_path(mf, project_root),
                "bytes": len(raw),
                "sha256": _sha256(raw),
            },
            "reason": reason,
        }
        if reason_error:
            record["reason_error"] = reason_error
        out.setdefault(conid, []).append(record)
    return {conid: records for conid, records in sorted(out.items())}


def _manifest_aliases(manifest):
    aliases = set()
    raw = (manifest or {}).get("aliases") or []
    if isinstance(raw, list):
        for val in raw:
            sym = _norm_symbol(val)
            if sym:
                aliases.add(sym)
    return aliases


def _manifest_symbol(manifest, fallback):
    return (_norm_symbol((manifest or {}).get("symbol"))
            or _norm_symbol((manifest or {}).get("folder"))
            or _norm_symbol(fallback)
            or str(fallback))


def _project_root_for(root, project_root=None):
    if project_root is not None:
        return Path(project_root)
    root = Path(root)
    if root.name == ss.STORAGE_DIR_NAME:
        return root.parent
    return Path(".").resolve()


def quarantined_conids(project_root=None):
    """Return conIds found under project-root _quarantine as a reuse blocklist."""
    records = _quarantine_records(project_root)
    return {
        conid: sorted({record["symbol"] for record in items})
        for conid, items in records.items()
    }


def _is_legit_rename_group(tickers, aliases):
    tickers = list(tickers)
    if len(tickers) < 2:
        return False
    # Match the reference harness: every ticker in the shared-conId group must
    # be cross-linked to at least one peer by aliases.
    for ticker in tickers:
        linked = False
        for other in tickers:
            if other == ticker:
                continue
            if (other in aliases.get(ticker, set())
                    or ticker in aliases.get(other, set())):
                linked = True
                break
        if not linked:
            return False
    return True


def _sort_duplicate_map(groups):
    return {conid: sorted(tickers) for conid, tickers in sorted(groups.items())}


def _manifest_day(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 64:
        raise ValueError("manifest first timestamp is missing or malformed")
    token = value.strip().split()[0]
    try:
        return _dt.datetime.strptime(token, "%m/%d/%Y").date()
    except ValueError:
        try:
            parsed = _dt.date.fromisoformat(token)
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


def _text_field(note, name):
    value = note.get(name) if isinstance(note, dict) else None
    if not isinstance(value, str) or not value.strip() or len(value) > 1000:
        raise ValueError(f"identity correction {name} is missing or malformed")
    return value.strip()


def _interval_identity_proofs(manifest, ticker):
    """Recompute strict listing-floor proof for every stored interval."""
    errors = []
    proofs = []
    raw_intervals = manifest.get("intervals") if isinstance(manifest, dict) else None
    if not isinstance(raw_intervals, dict):
        return [], ["active manifest intervals are malformed"]

    for interval, section in sorted(raw_intervals.items()):
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
                errors.append(
                    f"{label}: month {month} is unreadable, so its floor is unknown")
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
            run = _text_field(note, "run")
            snapshot = _text_field(note, "snapshot")
        except (ss.StorageError, ValueError) as exc:
            errors.append(f"{label}: {exc}")
            continue

        first_month, first_day = min(present, key=lambda item: item[1])
        proof = {
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
        }
        proofs.append(proof)
        if first_day < floor:
            errors.append(
                f"{label}: first stored day {first_day.isoformat()} precedes "
                f"identity floor {floor.isoformat()}")

    if not proofs and not errors:
        errors.append("active manifest has no non-empty interval to prove")
    return proofs, errors


def _active_manifest_provenance(root, ticker, manifest):
    path = Path(root) / ticker / ss.MANIFEST_NAME
    try:
        raw = path.read_bytes()
        current = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        return None, f"active manifest is unreadable: {type(exc).__name__}: {exc}"
    if not isinstance(current, dict) or current != manifest:
        return None, "active manifest changed while identity proof was derived"
    return {
        "path": f"{ticker}/{ss.MANIFEST_NAME}",
        "bytes": len(raw),
        "sha256": _sha256(raw),
    }, None


def _reuse_resolution(root, project_root, ticker, conid, manifest, records):
    """Derive one collision verdict from current evidence; never cache it."""
    errors = []
    if _coerce_conid((manifest or {}).get("conid")) != conid:
        errors.append("active conId changed during identity evaluation")
    if _norm_symbol((manifest or {}).get("symbol")) != ticker:
        errors.append("active manifest symbol does not match its ticker folder")
    folder = _norm_symbol((manifest or {}).get("folder"))
    if folder is not None and folder != ticker:
        errors.append("active manifest folder identity is inconsistent")

    active_stamp, active_error = _active_manifest_provenance(
        root, ticker, manifest)
    if active_error:
        errors.append(active_error)

    quarantine_manifests = []
    quarantine_reasons = []
    reason_seen = set()
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
        if key not in reason_seen:
            reason_seen.add(key)
            quarantine_reasons.append(dict(reason))

    interval_proofs, interval_errors = _interval_identity_proofs(
        manifest or {}, ticker)
    errors.extend(interval_errors)
    corrections = []
    correction_seen = set()
    for proof in interval_proofs:
        correction = proof["correction"]
        key = (correction["run"], correction["cutover"],
               correction["snapshot"])
        if key not in correction_seen:
            correction_seen.add(key)
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
        "intervals": interval_proofs,
        "errors": errors,
    }


def audit(root, project_root=None, write=False, asof=None):
    """Return an offline identity report.

    ``write=True`` persists only ``_identity_audit.json`` in the storage root.
    It does not touch ticker manifests or bar data.
    """
    root = Path(root)
    project_root = _project_root_for(root, project_root)
    asof = asof or _dt.datetime.now().isoformat(timespec="seconds")

    by_conid = {}
    aliases = {}
    manifests = {}
    summary = {}
    errors = []
    ticker_count = 0

    for tdir in _active_ticker_dirs(root):
        ticker = tdir.name
        ticker_count += 1
        man = ss.load_manifest(tdir)
        if not man:
            errors.append({"ticker": ticker, "error": "manifest unavailable"})
            continue
        conid = _coerce_conid(man.get("conid"))
        manifests[ticker] = man
        aliases[ticker] = _manifest_aliases(man)
        summary[ticker] = {
            "ticker": ticker,
            "symbol": _manifest_symbol(man, ticker),
            "conid": conid,
            "aliases": sorted(aliases[ticker]),
            "flags": [],
        }
        if conid is not None:
            by_conid.setdefault(conid, []).append(ticker)

    dup = {}
    for conid, tickers in by_conid.items():
        if len(tickers) > 1 and not _is_legit_rename_group(tickers, aliases):
            dup[conid] = sorted(tickers)
            for ticker in tickers:
                if ticker in summary:
                    summary[ticker]["flags"].append("duplicate-conid")

    quarantine_records = _quarantine_records(project_root)
    qconids = {
        conid: sorted({record["symbol"] for record in records})
        for conid, records in quarantine_records.items()
    }
    reuse = []
    reuse_detail = []
    reuse_resolved = []
    reuse_unresolved = []
    for conid, tickers in sorted(by_conid.items()):
        if conid not in qconids:
            continue
        for ticker in sorted(tickers):
            reuse.append((ticker, conid))
            verdict = _reuse_resolution(
                root, project_root, ticker, conid,
                manifests.get(ticker) or {},
                quarantine_records.get(conid) or [])
            reuse_detail.append({
                "ticker": ticker,
                "conid": conid,
                "quarantine_symbols": list(qconids.get(conid) or []),
                "resolution": verdict["resolution"],
            })
            if verdict["resolution"] == "resolved":
                reuse_resolved.append(verdict)
            else:
                reuse_unresolved.append(verdict)
            if ticker in summary:
                summary[ticker]["flags"].append("quarantined-conid-reuse")
                summary[ticker]["flags"].append(
                    "quarantined-conid-reuse-" + verdict["resolution"])

    duplicate_conids = [
        {"conid": conid, "tickers": tickers}
        for conid, tickers in _sort_duplicate_map(dup).items()
    ]
    report = {
        "kind": "identity_audit",
        "version": IDENTITY_REPORT_VERSION,
        "asof": asof,
        "root": str(root),
        "project_root": str(project_root),
        "ticker_count": ticker_count,
        "dup_conid": _sort_duplicate_map(dup),
        "duplicate_conids": duplicate_conids,
        "quarantine_reuse": reuse,
        "quarantine_reuse_detail": reuse_detail,
        "quarantine_reuse_resolved": reuse_resolved,
        "quarantine_reuse_unresolved": reuse_unresolved,
        "quarantined_conids": qconids,
        "summary": summary,
        "errors": errors,
        "clean": not dup and not reuse_unresolved,
    }
    if write:
        report["report_path"] = str(root / IDENTITY_REPORT_FILE)
        write_report(root, report)
    return report


def audit_identity(root, project_root=None):
    """Compatibility wrapper matching the reference harness naming."""
    return audit(root, project_root=project_root)


def write_report(root, report):
    target = Path(root) / IDENTITY_REPORT_FILE
    out = dict(report or {})
    out["report_path"] = str(target)
    payload = json.dumps(out, indent=2, sort_keys=True).encode("utf-8")
    ss._atomic_write_bytes(target, payload)
    return str(target)


def load_report(root):
    try:
        data = json.loads(
            (Path(root) / IDENTITY_REPORT_FILE).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _reuse_subset(report, key):
    value = report.get(key)
    if isinstance(value, list):
        return value
    if key == "quarantine_reuse_unresolved":
        # Version-1 reports did not adjudicate raw reuse. Treat every raw
        # collision as unresolved so an old cache can never inherit a pass.
        return [
            {"ticker": item[0], "conid": item[1],
             "resolution": "unresolved", "errors": [
                 "legacy report has no recomputed resolution proof"]}
            for item in (report.get("quarantine_reuse") or [])
            if isinstance(item, (list, tuple)) and len(item) >= 2
        ]
    return []


def summary_counts(report):
    dup_groups = report.get("dup_conid") or {}
    dup_tickers = set()
    for tickers in dup_groups.values():
        dup_tickers.update(str(t) for t in (tickers or []))
    raw_reuse = report.get("quarantine_reuse") or []
    resolved = _reuse_subset(report, "quarantine_reuse_resolved")
    unresolved = _reuse_subset(report, "quarantine_reuse_unresolved")
    return {
        "duplicate_conids": len(dup_groups),
        "duplicate_tickers": len(dup_tickers),
        "quarantine_reuse": len(raw_reuse),
        "quarantine_reuse_resolved": len(resolved),
        "quarantine_reuse_unresolved": len(unresolved),
        "quarantined_conids": len(report.get("quarantined_conids") or {}),
        "errors": len(report.get("errors") or []),
        "tickers": int(report.get("ticker_count") or 0),
    }


def format_queue(report):
    out = []
    for tickers in (report.get("dup_conid") or {}).values():
        out.extend(str(t) for t in tickers or [] if t)
    for item in _reuse_subset(report, "quarantine_reuse_unresolved"):
        if isinstance(item, dict) and item.get("ticker"):
            out.append(str(item["ticker"]))
        elif isinstance(item, (list, tuple)) and item:
            out.append(str(item[0]))
    return sorted(set(out))


def _names(vals, max_names=12):
    vals = [str(v) for v in vals if v]
    if not vals:
        return "none"
    if len(vals) <= max_names:
        return ", ".join(vals)
    return ", ".join(vals[:max_names]) + f", ... +{len(vals) - max_names}"


def summarize_report(report, max_names=12):
    c = summary_counts(report)
    lines = [
        ("Identity audit: "
         f"{c['tickers']} ticker(s), duplicate_conid={c['duplicate_conids']}, "
         f"quarantine_reuse={c['quarantine_reuse']} raw / "
         f"{c['quarantine_reuse_resolved']} resolved / "
         f"{c['quarantine_reuse_unresolved']} unresolved.")
    ]
    dup_names = []
    for conid, tickers in (report.get("dup_conid") or {}).items():
        dup_names.append(f"{conid}: {'/'.join(str(t) for t in tickers)}")
    reuse_names = []
    for item in report.get("quarantine_reuse") or []:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            reuse_names.append(f"{item[0]}:{item[1]}")
    lines.append("Identity: duplicate conIds -> "
                 + _names(dup_names, max_names=max_names))
    lines.append("Identity: quarantined-conId reuse -> "
                 + _names(reuse_names, max_names=max_names))
    resolved_names = [
        f"{item.get('ticker')}:{item.get('conid')}"
        for item in _reuse_subset(report, "quarantine_reuse_resolved")
        if isinstance(item, dict)
    ]
    unresolved_names = [
        f"{item.get('ticker')}:{item.get('conid')}"
        for item in _reuse_subset(report, "quarantine_reuse_unresolved")
        if isinstance(item, dict)
    ]
    lines.append("Identity: resolved quarantined-conId reuse -> "
                 + _names(resolved_names, max_names=max_names))
    lines.append("Identity: unresolved quarantined-conId reuse -> "
                 + _names(unresolved_names, max_names=max_names))
    if report.get("errors"):
        lines.append(f"Identity: manifest errors -> {c['errors']}")
    queue = format_queue(report)
    if queue:
        lines.append("Identity review queue: " + ", ".join(queue))
    rp = report.get("report_path")
    if rp:
        lines.append(f"Identity report saved: {rp}")
    return lines


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    write = "--write" in argv
    if "--no-write" in argv:
        write = False

    root = ss.storage_root(".")
    project_root = None
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in ("--write", "--no-write"):
            i += 1
            continue
        if arg == "--project-root":
            if i + 1 >= len(argv):
                print("--project-root requires a path", file=sys.stderr)
                return 2
            project_root = Path(argv[i + 1])
            i += 2
            continue
        if not arg.startswith("-"):
            root = Path(arg)
        i += 1

    report = audit(root, project_root=project_root, write=write)
    for line in summarize_report(report):
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
