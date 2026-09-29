"""Offline split audit over cached history and the current stored bank.

The audit never refreshes split history and never imports a live provider.  It
classifies only identity-gated cache evidence, fingerprinted detector output,
strictly validated stored daily bars, and durable manifest corrections.
Archived seam candidates are reported in a separate historical scope and can
never enter the current bank repair queue.
"""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import hashlib
import json
import math
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import deep_seam_scan as seam_scan  # noqa: E402
import split_cache as cache  # noqa: E402
import split_detector as detector  # noqa: E402
import stock_storage as storage  # noqa: E402


REPORT_FILE = "_split_audit.json"
REPORT_KIND = "split_audit"
REPORT_VERSION = 1
MAX_EVENT_BAR_GAP_DAYS = 10
VERDICTS = tuple(detector.VERDICT_POLICY)

_ACTIONS = {
    "PHANTOM": "confirm, then use the existing guarded correction flow",
    "MISSING": "review the unadjusted real split; do not auto-correct",
    "REGRESSION": "halt repair work and review the correction recurrence",
    "IDENTITY_BASIS": "review predecessor or identity lineage; do not apply xN",
    "UNVERIFIABLE": "obtain or refresh authoritative evidence",
    "STALE": "refresh split references",
    "REAL": "no repair; retain the confirmed event as information",
    "RESOLVED": "no repair; retain the durable correction record",
    "CLEAN": "no action",
}


class SplitAuditError(RuntimeError):
    """The requested offline evidence cannot be read safely."""


def _utc_now(now=None):
    current = now or dt.datetime.now(dt.timezone.utc)
    if not isinstance(current, dt.datetime):
        raise SplitAuditError("audit time is not a datetime")
    if current.tzinfo is None:
        current = current.replace(tzinfo=dt.timezone.utc)
    return current.astimezone(dt.timezone.utc)


def _day(value):
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value or "")[:10])
    except ValueError as exc:
        raise SplitAuditError(f"invalid date: {value!r}") from exc


def _manifest_months(manifest):
    intervals = manifest.get("intervals") if isinstance(manifest, dict) else None
    daily = intervals.get("1d") if isinstance(intervals, dict) else None
    months = daily.get("months") if isinstance(daily, dict) else None
    if not isinstance(months, dict) or not months:
        raise SplitAuditError("daily manifest months are unavailable")
    return months


def _manifest_fingerprint(manifest):
    return seam_scan.manifest_fingerprint(_manifest_months(manifest))


def _manifest_corrections(manifest):
    raw = manifest.get("data_corrections") if isinstance(manifest, dict) else None
    if raw is None or raw == {}:
        return []
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, (list, tuple)):
        return list(raw)
    return raw


def _manifest_identity(manifest, ticker):
    folder = storage.canonical_ticker(manifest.get("folder") or ticker)
    if folder != ticker:
        raise cache.CacheIdentityError(
            f"manifest folder {folder} does not match {ticker}")
    return cache.normalize_identity({
        "ticker": ticker,
        "conid": manifest.get("conid"),
        "provider_symbol": (
            manifest.get("provider_symbol") or manifest.get("symbol")),
    })


def _list_manifest_tickers(root):
    """List canonical manifest folders without parsing every manifest twice."""
    out = []
    for path in sorted(Path(root).iterdir(), key=lambda item: item.name.casefold()):
        if (not path.is_dir() or path.name.startswith("_")
                or not (path / storage.MANIFEST_NAME).is_file()):
            continue
        try:
            ticker = storage.canonical_ticker(path.name)
        except Exception:  # noqa: BLE001 - noncanonical folders are not tickers
            continue
        if ticker == path.name.upper():
            out.append(ticker)
    return out


def _manifest_evidence_state(manifest):
    """Canonical state that identity/correction/fingerprint verdicts consume."""
    return json.dumps({
        "folder": manifest.get("folder"),
        "symbol": manifest.get("symbol"),
        "provider_symbol": manifest.get("provider_symbol"),
        "conid": manifest.get("conid"),
        "data_corrections": manifest.get("data_corrections"),
        "series_fingerprint": _manifest_fingerprint(manifest),
    }, sort_keys=True, separators=(",", ":"))


def _tag(row, row_type, *, scope="current", provenance=None):
    tagged = dict(row)
    tagged["row_type"] = row_type
    tagged["scope"] = scope
    tagged["evidence"] = dict(tagged.get("evidence") or {})
    if provenance:
        tagged["evidence"]["provenance"] = provenance
    if row_type != "candidate":
        tagged["needs_confirmation"] = False
    if scope == "archived":
        tagged["historical_repair_queue_eligible"] = bool(
            tagged.get("repair_queue"))
        tagged["repair_queue"] = False
        tagged["current_bank_effect"] = False
    return tagged


def _audit_row(ticker, verdict, reason, *, row_type, scope="current",
               date=None, factor=None, evidence=None, provenance=None):
    policy = detector.VERDICT_POLICY[verdict]
    row = {
        "ticker": str(ticker or "").strip().upper(),
        "verdict": verdict,
        "confidence": "low" if policy["severity"] == "operational" else "high",
        "severity": policy["severity"],
        "repair_queue": policy["repair_queue"],
        "needs_confirmation": (
            policy["needs_confirmation"] if row_type == "candidate" else False),
        "reason": reason,
        "recommended_action": _ACTIONS[verdict],
        "matched": None,
        "evidence": dict(evidence or {}),
    }
    if date is not None:
        row["date"] = _day(date).isoformat()
    if factor is not None:
        row["factor"] = round(float(factor), 8)
    return _tag(
        row, row_type, scope=scope, provenance=provenance)


def _cache_summary(loaded):
    keys = (
        "status", "usable", "path", "age_seconds", "reason", "error",
        "other_identity_files",
    )
    return {key: loaded[key] for key in keys if key in loaded}


def _history(payload):
    return {
        "events": payload.get("events"),
        "coverage": payload.get("coverage"),
        "source_status": "ok",
    }


def _unavailable_history(status):
    return {
        "events": [],
        "coverage": {},
        "source_status": f"cache_{status}",
    }


def _strict_daily_bars(root, ticker, expected_fingerprint):
    """Read all daily bars and prove the files and manifest stayed exact."""
    root = Path(root)
    manifest = storage.load_manifest(root / ticker)
    if not manifest:
        raise SplitAuditError(f"manifest unavailable for {ticker}")
    months = _manifest_months(manifest)
    fingerprint = seam_scan.manifest_fingerprint(months)
    if expected_fingerprint and fingerprint != expected_fingerprint:
        raise SplitAuditError("daily manifest changed before event-bar read")

    by_day = {}
    for month in sorted(months):
        entry = months.get(month)
        if not isinstance(entry, dict):
            raise SplitAuditError(f"daily manifest month {month} is malformed")
        try:
            year, number = int(str(month)[:4]), int(str(month)[5:7])
        except (TypeError, ValueError) as exc:
            raise SplitAuditError(f"invalid daily manifest month {month!r}") from exc
        path = (storage.find_month_file(root, ticker, year, number, "1d")
                or storage.month_file_path(root, ticker, year, number, "1d"))
        try:
            bars, stats = storage.read_month_file(path)
        except Exception as exc:  # noqa: BLE001 - evidence boundary
            raise SplitAuditError(f"unreadable daily month {month}: {exc}") from exc
        expected = {
            "rows": entry.get("rows"),
            "sha256": str(entry.get("sha256") or "").lower(),
            "mtime_ns": entry.get("mtime_ns"),
        }
        actual = {
            "rows": stats.get("rows"),
            "sha256": str(stats.get("sha256") or "").lower(),
            "mtime_ns": stats.get("mtime_ns"),
        }
        if actual != expected:
            raise SplitAuditError(
                f"daily month {month} does not match its manifest entry")
        for bar in bars:
            key = bar[0].date()
            if key in by_day:
                raise SplitAuditError(
                    f"duplicate daily date {key.isoformat()} across months")
            by_day[key] = bar

    current = storage.load_manifest(root / ticker)
    if not current:
        raise SplitAuditError(f"manifest disappeared while reading {ticker}")
    current_fingerprint = _manifest_fingerprint(current)
    if current_fingerprint != fingerprint:
        raise SplitAuditError("daily manifest changed during event-bar read")
    if not by_day:
        raise SplitAuditError("daily series contains no bars")
    return by_day, fingerprint


def _event_observation(bars, event_date):
    event_day = _day(event_date)
    days = sorted(bars)
    index = bisect.bisect_left(days, event_day)
    if index == 0 or index >= len(days):
        return None, {"reason": "event_has_no_two_sided_stored_bars"}
    pre_day, post_day = days[index - 1], days[index]
    if ((event_day - pre_day).days > MAX_EVENT_BAR_GAP_DAYS
            or (post_day - event_day).days > MAX_EVENT_BAR_GAP_DAYS):
        return None, {
            "reason": "nearest_event_bars_are_too_distant",
            "pre_date": pre_day.isoformat(),
            "post_date": post_day.isoformat(),
        }
    pre, post = bars[pre_day], bars[post_day]
    return {
        "pre_close": pre[4],
        "post_open": post[1],
        "post_close": post[4],
    }, {
        "pre_date": pre_day.isoformat(),
        "post_date": post_day.isoformat(),
    }


def _event_rows(root, ticker, history, current_fingerprint):
    normalized = detector.normalize_history(history)
    events = [event for event in normalized["events"] if event["confirmed"]]
    if not events:
        return []
    try:
        bars, read_fingerprint = _strict_daily_bars(
            root, ticker, current_fingerprint)
    except SplitAuditError as exc:
        rows = []
        for event in events:
            row = detector.classify_event_adjustment(ticker, event, None)
            row["reason"] = "event_bars_unverifiable"
            row["evidence"] = {"error": str(exc)}
            rows.append(_tag(row, "event"))
        return rows

    rows = []
    for event in events:
        observation, lookup = _event_observation(bars, event["ex_date"])
        row = detector.classify_event_adjustment(ticker, event, observation)
        row["evidence"] = dict(row.get("evidence") or {})
        row["evidence"].update({
            "bar_lookup": lookup,
            "series_fingerprint": read_fingerprint,
        })
        rows.append(_tag(row, "event"))
    return rows


def _iter_objects(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _iter_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_objects(child)


def _matching_triage(rows, ticker, boundary):
    choices = []
    for row in rows:
        if str(row.get("ticker") or "").strip().upper() != ticker:
            continue
        row_day = row.get("boundary", row.get("date", row.get("ex_date")))
        try:
            delta = abs((_day(row_day) - _day(boundary)).days)
        except SplitAuditError:
            continue
        if delta <= detector.DATE_TOL_DAYS:
            choices.append((delta, str(row.get("classification") or ""), row))
    return min(choices, key=lambda item: (item[0], item[1]))[2] if choices else {}


def load_archive(path):
    """Load candidate-shaped rows from a read-only deep-seam run artifact."""
    path = Path(path)
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise SplitAuditError(f"cannot read archive {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SplitAuditError("deep-seam archive is not an object")
    if payload.get("read_only") is not True:
        raise SplitAuditError("deep-seam archive is not marked read_only")
    source_rows = payload.get("rows")
    if not isinstance(source_rows, list):
        raise SplitAuditError("deep-seam archive rows are not a list")

    triage_rows = list(_iter_objects(payload.get("triage")))
    digest = hashlib.sha256(raw).hexdigest()
    candidates, errors, seen = [], [], set()
    for index, source in enumerate(source_rows):
        if not isinstance(source, dict):
            errors.append(f"row {index}: not an object")
            continue
        if str(source.get("verdict") or "").strip().upper() != "SEAM":
            continue
        ticker = str(source.get("ticker") or "").strip().upper()
        boundary = source.get("boundary", source.get("date"))
        triage = _matching_triage(triage_rows, ticker, boundary)
        triage_evidence = (
            triage.get("evidence") if isinstance(triage.get("evidence"), dict)
            else {})
        candidate_raw = {
            "ticker": ticker,
            "date": triage.get("ex_date", source.get("ex_date", boundary)),
            "factor": source.get("factor", source.get("step")),
            "deep_cv": source.get(
                "deep_cv", triage.get("deep_cv", triage_evidence.get("deep_cv"))),
            "current_anchor_sound": source.get(
                "current_anchor_sound", triage.get("current_anchor_sound")),
            "volume": source.get("volume", triage.get("volume")),
        }
        try:
            candidate = detector.normalize_candidate(candidate_raw)
        except (TypeError, ValueError) as exc:
            errors.append(f"row {index}: {exc}")
            continue
        key = (candidate["ticker"], candidate["date"], candidate["factor"])
        if key in seen:
            continue
        seen.add(key)
        candidates.append({
            "candidate": candidate,
            "provenance": {
                "artifact": str(path.resolve()),
                "artifact_sha256": digest,
                "row_index": index,
                "archived_boundary": str(boundary or "")[:10],
                "triage_classification": triage.get("classification"),
            },
        })
    candidates.sort(key=lambda item: (
        item["candidate"]["ticker"], item["candidate"]["date"],
        item["candidate"]["factor"]))
    return {
        "path": str(path.resolve()),
        "sha256": digest,
        "candidate_count": len(candidates),
        "candidates": candidates,
        "errors": errors,
    }


def _factor_error(value, expected):
    left = max(float(value), 1.0 / float(value))
    right = max(float(expected), 1.0 / float(expected))
    return abs(left - right) / right


def _matching_correction(candidate, corrections):
    if not isinstance(corrections, (list, tuple)):
        return None
    for correction in corrections:
        if not isinstance(correction, dict):
            continue
        if str(correction.get("type") or "") not in detector.CORRECTION_TYPES:
            continue
        if not str(correction.get("applied") or "").strip():
            continue
        ticker = str(correction.get("ticker") or candidate["ticker"]).upper()
        if ticker != candidate["ticker"]:
            continue
        correction_date = correction.get(
            "ex_date", correction.get("boundary", correction.get("cutover")))
        try:
            if abs((_day(candidate["date"]) - _day(correction_date)).days) > detector.DATE_TOL_DAYS:
                continue
            if (correction.get("factor") is not None
                    and _factor_error(candidate["factor"], correction["factor"])
                    > detector.RATIO_TOL):
                continue
        except (SplitAuditError, TypeError, ValueError, ZeroDivisionError):
            continue
        return correction
    return None


def _enrich_archived_candidate(candidate, corrections):
    enriched = dict(candidate)
    correction = _matching_correction(enriched, corrections)
    used = []
    evidence = (correction.get("triage_evidence")
                if isinstance(correction, dict) else None)
    if not isinstance(evidence, dict):
        return detector.normalize_candidate(enriched), used
    if enriched.get("deep_cv") is None:
        value = evidence.get("deep_cv")
        try:
            value = float(value)
            if math.isfinite(value) and value >= 0:
                enriched["deep_cv"] = value
                used.append("deep_cv")
        except (TypeError, ValueError):
            pass
    if enriched.get("current_anchor_sound") is None:
        explicit = evidence.get("current_anchor_sound")
        if isinstance(explicit, bool):
            enriched["current_anchor_sound"] = explicit
            used.append("current_anchor_sound")
        else:
            try:
                r_now = float(evidence.get("r_now"))
                if math.isfinite(r_now) and r_now > 0:
                    enriched["current_anchor_sound"] = (
                        abs(r_now - 1.0) <= seam_scan.TOL_NOW)
                    used.append("r_now_to_current_anchor_sound")
            except (TypeError, ValueError):
                pass
    return detector.normalize_candidate(enriched), used


def _signature(row):
    return (
        row.get("ticker"), row.get("verdict"), row.get("reason"),
        row.get("date"), row.get("factor"),
    )


def _dedupe(rows):
    out, seen = [], set()
    for row in rows:
        key = _signature(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def audit_ticker(root, ticker, *, archive_candidates=None, max_age=None,
                 now=None):
    root = Path(root)
    try:
        ticker = storage.canonical_ticker(ticker)
    except Exception as exc:  # noqa: BLE001 - canonical boundary
        raise SplitAuditError(f"invalid ticker {ticker!r}: {exc}") from exc
    manifest = storage.load_manifest(root / ticker)
    if not manifest:
        row = _audit_row(
            ticker, "UNVERIFIABLE", "manifest_unavailable",
            row_type="source")
        archived_rows = []
        history = _unavailable_history("manifest_unavailable")
        for archived in archive_candidates or []:
            archived_row = detector.classify_candidate(
                archived["candidate"], history, corrections=[])
            archived_rows.append(_tag(
                archived_row, "candidate", scope="archived",
                provenance=archived.get("provenance")))
        return {
            "ticker": ticker,
            "identity": None,
            "cache": {"status": "identity_error", "usable": False},
            "fingerprint": {"status": "unavailable"},
            "detection_current": False,
            "source_provenance": {},
            "rows": [row],
            "archived_rows": archived_rows,
        }

    corrections = _manifest_corrections(manifest)
    try:
        current_fingerprint = _manifest_fingerprint(manifest)
        initial_manifest_state = _manifest_evidence_state(manifest)
        fingerprint_error = None
    except SplitAuditError as exc:
        current_fingerprint = None
        initial_manifest_state = None
        fingerprint_error = str(exc)

    try:
        identity = _manifest_identity(manifest, ticker)
        loaded = cache.load_cache(
            root, identity, max_age=max_age, now=now)
    except cache.SplitCacheError as exc:
        identity = None
        loaded = {
            "status": "identity_error",
            "usable": False,
            "error": str(exc),
        }

    status = loaded.get("status", "unknown")
    payload = loaded.get("cache") if isinstance(loaded.get("cache"), dict) else None
    candidates = []
    cached_fingerprint = None
    source_provenance = {}
    current_rows = []
    history = _unavailable_history(status)
    detection_current = False

    if payload is not None:
        detection = payload.get("detection")
        if isinstance(detection, dict):
            cached_fingerprint = detection.get("series_fingerprint")
        source_provenance = {
            "provider": payload.get("provider"),
            "fetched_at": payload.get("fetched_at"),
            "coverage": payload.get("coverage"),
            "sources": payload.get("sources"),
            "reference_asof": (
                detection.get("reference_asof")
                if isinstance(detection, dict) else None),
        }

    if status == "ok" and payload is not None:
        history = _history(payload)
        detection = payload["detection"]
        candidates = list(detection["candidates"])
        detection_current = bool(
            current_fingerprint
            and cached_fingerprint
            and current_fingerprint == cached_fingerprint)
        if detection_current:
            for candidate in candidates:
                current_rows.append(_tag(
                    detector.classify_candidate(
                        candidate, history, corrections=corrections),
                    "candidate"))
        else:
            reason = (
                "candidate_fingerprint_missing" if not cached_fingerprint
                else "candidate_fingerprint_mismatch")
            evidence = {
                "cached": cached_fingerprint,
                "current": current_fingerprint,
            }
            if fingerprint_error:
                evidence["error"] = fingerprint_error
            if candidates:
                for candidate in candidates:
                    current_rows.append(_audit_row(
                        ticker, "STALE", reason, row_type="candidate",
                        date=candidate["date"], factor=candidate["factor"],
                        evidence=evidence))
            else:
                current_rows.append(_audit_row(
                    ticker, "STALE", reason, row_type="detection",
                    evidence=evidence))
        current_rows.extend(_event_rows(
            root, ticker, history, current_fingerprint))
    else:
        verdict = "STALE" if status == "stale" else "UNVERIFIABLE"
        current_rows.append(_audit_row(
            ticker, verdict, f"split_cache_{status}", row_type="source",
            evidence={key: loaded[key] for key in ("error", "reason")
                      if key in loaded}))

    manifest_changed = False
    final_manifest = storage.load_manifest(root / ticker)
    try:
        final_manifest_state = _manifest_evidence_state(final_manifest)
        final_fingerprint = _manifest_fingerprint(final_manifest)
    except (AttributeError, SplitAuditError):
        final_manifest_state = None
        final_fingerprint = None
    if initial_manifest_state != final_manifest_state:
        manifest_changed = True
        detection_current = False
        current_fingerprint = final_fingerprint
        corrections = []
        current_rows = [
            row for row in current_rows if row.get("row_type") == "source"]
        current_rows.append(_audit_row(
            ticker, "UNVERIFIABLE", "manifest_changed_during_audit",
            row_type="source",
            evidence={
                "initial_series_fingerprint": (
                    json.loads(initial_manifest_state)["series_fingerprint"]
                    if initial_manifest_state else None),
                "final_series_fingerprint": final_fingerprint,
            }))
        if candidates:
            for candidate in candidates:
                current_rows.append(_audit_row(
                    ticker, "STALE", "manifest_changed_during_audit",
                    row_type="candidate", date=candidate["date"],
                    factor=candidate["factor"], evidence={
                        "cached": cached_fingerprint,
                        "current": final_fingerprint,
                    }))
        elif status == "ok":
            current_rows.append(_audit_row(
                ticker, "STALE", "manifest_changed_during_audit",
                row_type="detection", evidence={
                    "cached": cached_fingerprint,
                    "current": final_fingerprint,
                }))
        if status == "ok":
            normalized = detector.normalize_history(history)
            for event in normalized["events"]:
                if event["confirmed"]:
                    current_rows.append(_audit_row(
                        ticker, "UNVERIFIABLE",
                        "manifest_changed_during_audit", row_type="event",
                        date=event["ex_date"], factor=event["factor"]))

    correction_rows = detector.reconcile_corrections(
        ticker, candidates, history, corrections,
        detection_current=detection_current)
    current_rows.extend(_tag(row, "correction") for row in correction_rows)
    current_rows = _dedupe(current_rows)

    event_rows = [row for row in current_rows if row["row_type"] == "event"]
    correction_rows = [
        row for row in current_rows if row["row_type"] == "correction"]
    if (detection_current and not candidates
            and all(row["verdict"] == "REAL" for row in event_rows)
            and all(row["verdict"] == "RESOLVED" for row in correction_rows)):
        current_rows.append(_audit_row(
            ticker, "CLEAN",
            "current_detection_has_no_candidates_and_events_are_adjusted",
            row_type="summary"))

    archived_rows = []
    for archived in archive_candidates or []:
        candidate, used = _enrich_archived_candidate(
            archived["candidate"], corrections)
        provenance = dict(archived.get("provenance") or {})
        if used:
            provenance["captured_correction_evidence"] = used
        row = detector.classify_candidate(candidate, history, corrections=[])
        archived_rows.append(_tag(
            row, "candidate", scope="archived", provenance=provenance))

    fingerprint_status = (
        "current" if detection_current
        else "changed_during_audit" if manifest_changed
        else f"cache_{status}" if status != "ok"
        else "unavailable" if current_fingerprint is None
        else "missing" if cached_fingerprint is None
        else "mismatch")
    return {
        "ticker": ticker,
        "identity": identity,
        "cache": _cache_summary(loaded),
        "fingerprint": {
            "status": fingerprint_status,
            "cached": cached_fingerprint,
            "current": current_fingerprint,
            **({"error": fingerprint_error} if fingerprint_error else {}),
        },
        "detection_current": detection_current,
        "source_provenance": source_provenance,
        "rows": current_rows,
        "archived_rows": archived_rows,
    }


def _counts(rows):
    counts = Counter(row.get("verdict") for row in rows)
    return {verdict: counts.get(verdict, 0) for verdict in VERDICTS}


def _decorate_ticker(result):
    rows = result["rows"]
    archived = result["archived_rows"]
    result["verdict_counts"] = _counts(rows)
    result["archived_verdict_counts"] = _counts(archived)
    result["repair_queue"] = [row for row in rows if row.get("repair_queue")]
    result["needs_confirmation"] = [
        row for row in rows
        if row.get("row_type") == "candidate"
        and row.get("needs_confirmation")]
    result["stale_unverifiable_rows"] = [
        row for row in rows if row.get("verdict") in {"STALE", "UNVERIFIABLE"}]
    result["data_clean"] = not result["repair_queue"]
    result["evidence_complete"] = not result["stale_unverifiable_rows"]
    return result


def audit(root, tickers=None, *, archive_path=None, max_age=None, now=None,
          write=False):
    """Build the complete offline report and optionally write its sidecar."""
    root = Path(root)
    if not root.is_dir():
        raise SplitAuditError(f"storage root does not exist: {root}")
    current = _utc_now(now)
    if tickers is None:
        names = _list_manifest_tickers(root)
    else:
        try:
            names = sorted({storage.canonical_ticker(name) for name in tickers})
        except Exception as exc:  # noqa: BLE001 - canonical boundary
            raise SplitAuditError(f"invalid ticker list: {exc}") from exc

    archive = None
    archived_by_ticker = {}
    if archive_path is not None:
        archive = load_archive(archive_path)
        for item in archive["candidates"]:
            archived_by_ticker.setdefault(
                item["candidate"]["ticker"], []).append(item)

    ticker_results = []
    for ticker in names:
        ticker_results.append(_decorate_ticker(audit_ticker(
            root, ticker,
            archive_candidates=archived_by_ticker.get(ticker, []),
            max_age=max_age, now=current)))

    rows = [row for item in ticker_results for row in item["rows"]]
    archived_rows = [
        row for item in ticker_results for row in item["archived_rows"]]
    repair_queue = [row for row in rows if row.get("repair_queue")]
    severe_rows = [row for row in rows if row.get("severity") == "severe"]
    needs_confirmation = [
        row for row in rows
        if row.get("row_type") == "candidate"
        and row.get("needs_confirmation")]
    operational = [
        row for row in rows if row.get("verdict") in {"STALE", "UNVERIFIABLE"}]
    report = {
        "kind": REPORT_KIND,
        "version": REPORT_VERSION,
        "generated_at": current.isoformat(timespec="seconds"),
        "root": str(root.resolve()),
        "output_path": str((root / REPORT_FILE).resolve()),
        "network": False,
        "report_only": True,
        "written": bool(write),
        "summary": {
            "ticker_count": len(ticker_results),
            "row_count": len(rows),
            "archive_row_count": len(archived_rows),
            "repair_queue_count": len(repair_queue),
            "severe_count": len(severe_rows),
            "needs_confirmation_count": len(needs_confirmation),
            "operational_warning_count": len(operational),
            "data_clean": not repair_queue,
            "evidence_complete": not operational,
        },
        "verdict_counts": _counts(rows),
        "archived_verdict_counts": _counts(archived_rows),
        "repair_queue": repair_queue,
        "severe_rows": severe_rows,
        "needs_confirmation": needs_confirmation,
        "stale_unverifiable_rows": operational,
        "tickers": ticker_results,
        "archive": ({
            "path": archive["path"],
            "sha256": archive["sha256"],
            "candidate_count": archive["candidate_count"],
            "errors": archive["errors"],
            "unmatched_tickers": sorted(
                set(archived_by_ticker) - set(names)),
        } if archive is not None else None),
    }
    if write:
        encoded = (json.dumps(
            report, sort_keys=True, separators=(",", ":")) + "\n").encode(
                "utf-8")
        storage._atomic_write_bytes(root / REPORT_FILE, encoded)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=storage.STORAGE_DIR_NAME)
    parser.add_argument("--ticker", action="append", dest="tickers")
    parser.add_argument("--archive")
    parser.add_argument("--max-age-hours", type=float)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args(argv)
    max_age = (None if args.max_age_hours is None
               else dt.timedelta(hours=args.max_age_hours))
    try:
        report = audit(
            args.root, args.tickers, archive_path=args.archive,
            max_age=max_age, write=args.write)
    except (SplitAuditError, OSError, storage.StorageError) as exc:
        print(json.dumps({
            "kind": REPORT_KIND,
            "version": REPORT_VERSION,
            "network": False,
            "status": "error",
            "error": str(exc),
        }, sort_keys=True))
        return 2
    print(json.dumps(
        report, sort_keys=True, indent=2 if args.pretty else None))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
