"""Read-only split evidence enrichment for an already-failed IBKR gate.

This module never fetches evidence and never records an action.  It may attach
one bounded, unapplied proposal when a current identity-bound cache explains the
same boundary that the existing entry/join gate rejected.
"""

from __future__ import annotations

import datetime as dt
import math
from pathlib import Path

import split_cache
import split_detector
import stock_basis
import stock_storage


MAX_CACHE_AGE = dt.timedelta(days=30)
ALLOWED_GATES = frozenset({"entry_overlap", "entry_thin", "join"})


def _date(value):
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text = str(value or "").strip()
    if len(text) < 10:
        raise ValueError("date is missing")
    return dt.date.fromisoformat(text[:10])


def _positive(value):
    if isinstance(value, bool):
        raise ValueError("factor is not positive")
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("factor is not positive")
    return number


def _relative_error(value, expected):
    return abs(value - expected) / abs(expected)


def _record_date(row):
    if not isinstance(row, dict):
        return None
    for key in ("date", "ex_date", "boundary", "cutover"):
        if row.get(key) is not None:
            try:
                return _date(row[key])
            except (TypeError, ValueError):
                return None
    return None


def _near(day, event_day):
    return abs((day - event_day).days) <= split_detector.DATE_TOL_DAYS


def _manifest_conflicts(manifest, event_day):
    """Fail closed on an action/correction already occupying this boundary."""
    actions = manifest.get("actions", [])
    if not isinstance(actions, list):
        return True
    for action in actions:
        if not isinstance(action, dict):
            return True
        applies = action.get("applies")
        if applies == "volume":
            continue
        if applies not in ("price", "both"):
            return True
        action_day = _record_date(action)
        if action_day is None or _near(action_day, event_day):
            return True

    corrections = manifest.get("data_corrections", [])
    if not isinstance(corrections, list):
        return True
    for correction in corrections:
        if not isinstance(correction, dict):
            return True
        correction_day = _record_date(correction)
        if correction_day is None or _near(correction_day, event_day):
            return True
    return False


def _bounded(value, cap=160):
    return " ".join(str(value or "").split())[:cap]


def enrich_halt(root, ticker, interval, *, gate, prior_date, current_date,
                observed_factor, original_halt, run_id=None,
                overlap_verdict=None, max_age=MAX_CACHE_AGE, now=None,
                identity_loader=None, cache_loader=None,
                manifest_loader=None):
    """Return one unapplied split proposal, or ``None`` without side effects.

    The caller invokes this only after the existing gate has failed.  Every
    evidence, identity, schema, or race problem suppresses enrichment and leaves
    the original halt untouched.
    """
    if gate not in ALLOWED_GATES:
        return None
    try:
        ticker = stock_storage.canonical_ticker(ticker)
        prior_day = _date(prior_date)
        current_day = _date(current_date)
        observed = _positive(observed_factor)
        if prior_day > current_day:
            return None
        if gate == "entry_overlap":
            if not isinstance(overlap_verdict, dict):
                return None
            if overlap_verdict.get("kind") != "split":
                return None
            if _relative_error(
                    _positive(overlap_verdict.get("factor")), observed
                    ) > split_detector.RAW_SPLIT_PRICE_TOL:
                return None

        identity_loader = identity_loader or split_cache.manifest_identity
        cache_loader = cache_loader or split_cache.load_cache
        manifest_loader = manifest_loader or stock_storage.load_manifest
        ticker_dir = Path(root) / ticker
        identity_before = split_cache.normalize_identity(
            identity_loader(root, ticker))
        if identity_before.get("conid") is None:
            return None
        manifest_before = manifest_loader(ticker_dir)
        if not isinstance(manifest_before, dict):
            return None

        loaded = cache_loader(
            root, identity_before, max_age=max_age, now=now)
        if not isinstance(loaded, dict) or loaded.get("status") != "ok" \
                or loaded.get("usable") is not True:
            return None
        payload = loaded.get("cache")
        if not isinstance(payload, dict):
            return None
        history = split_detector.normalize_history({
            "events": payload.get("events"),
            "coverage": payload.get("coverage"),
            "source_status": payload.get("provider_status", "ok"),
        })
        if history["errors"] or history["conflicts"]:
            return None

        tolerance = dt.timedelta(days=split_detector.DATE_TOL_DAYS)
        window_start = prior_day - tolerance
        window_end = current_day + tolerance
        matches = [
            event for event in history["events"]
            if window_start <= _date(event["ex_date"]) <= window_end
        ]
        if len(matches) != 1 or matches[0].get("confirmed") is not True:
            return None
        event = matches[0]
        event_day = _date(event["ex_date"])
        expected = 1.0 / _positive(event["ratio"])
        if _relative_error(observed, expected) \
                > split_detector.RAW_SPLIT_PRICE_TOL:
            return None
        if _manifest_conflicts(manifest_before, event_day):
            return None

        identity_after = split_cache.normalize_identity(
            identity_loader(root, ticker))
        manifest_after = manifest_loader(ticker_dir)
        if identity_after != identity_before:
            return None
        if not isinstance(manifest_after, dict):
            return None
        if (manifest_after.get("actions", [])
                != manifest_before.get("actions", [])):
            return None
        if (manifest_after.get("data_corrections", [])
                != manifest_before.get("data_corrections", [])):
            return None

        explanation = (
            f"confirmed cached split event {event['ratio']:.8g} new shares "
            f"per old share at {event_day.isoformat()}; canonical price "
            f"factor {expected:.8g} matched observed {observed:.8g}")
        verdict = {
            "kind": "split",
            "factor": expected,
            "applies": "price",
            "confidence": "high",
            "explanation": explanation,
        }
        action, _why = stock_basis.propose_action(
            ticker, interval, verdict, event_day, run_id=run_id)
        if action is None:
            return None
        stock_basis.validate_action(action)

        source_ids = [_bounded(item, 96) for item in event["source_ids"][:8]]
        return {
            "status": "confirmed_split_proposal",
            "ticker": ticker,
            "interval": str(interval),
            "gate": gate,
            "boundary_date": event_day.isoformat(),
            "observed_factor": round(observed, 8),
            "event": {
                "ex_date": event_day.isoformat(),
                "ratio": round(float(event["ratio"]), 8),
                "source_ids": source_ids,
            },
            "cache": {
                "path": Path(str(loaded.get("path") or "")).name,
                "fetched_at": _bounded(payload.get("fetched_at"), 48),
                "provider": _bounded(payload.get("provider"), 80),
            },
            "action": action,
            "requires_user_approval": True,
            "applied": False,
            "message": (
                f"cached confirmed split evidence matched; proposed price "
                f"factor {expected:.8g} is not recorded"),
            "original_halt": _bounded(original_halt, 600),
        }
    except Exception:  # noqa: BLE001 - enrichment can never replace the halt
        return None
