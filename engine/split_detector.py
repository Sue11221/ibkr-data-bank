"""Pure split-history normalization and classification.

This module has no filesystem or network behavior. Refresh/cache code supplies
normalized evidence later; the functions here only decide what that evidence
proves. No verdict applies a correction or records a basis action.
"""

from __future__ import annotations

import datetime as dt
import math
import statistics as st

import stock_storage as storage


DATE_TOL_DAYS = 10
RATIO_TOL = 0.06
CV_UNIFORM = 0.08
SMOOTH_PRICE_TOL = 0.12
RAW_SPLIT_PRICE_TOL = 0.15
VOLUME_MIN_PAIRS = 10
VOLUME_MAX_SPREAD = 0.50
VOLUME_INVERSE_TOL = 0.25
VOLUME_CONTRADICTION_TOL = 0.75

COMPLETE_EVIDENCE_LEVELS = {
    "issuer_explicit_history",
    "provider_documented_complete",
}
COVERAGE_EVIDENCE_LEVELS = COMPLETE_EVIDENCE_LEVELS | {
    "provisional",
    "unknown",
}
CONFIRMED_EVENT_LEVELS = {
    "authoritative",
    "authoritative_event",
    "confirmed",
    "corroborated",
}
CORRECTION_TYPES = (
    {"phantom_split_correction"} | set(storage.IDENTITY_CORRECTION_TYPES))

VERDICT_POLICY = {
    "PHANTOM": {"severity": "severe", "repair_queue": True,
                "needs_confirmation": False},
    "MISSING": {"severity": "severe", "repair_queue": True,
                "needs_confirmation": False},
    "REGRESSION": {"severity": "severe", "repair_queue": True,
                   "needs_confirmation": False},
    "IDENTITY_BASIS": {"severity": "review", "repair_queue": True,
                       "needs_confirmation": False},
    "UNVERIFIABLE": {"severity": "operational", "repair_queue": False,
                     "needs_confirmation": True},
    "STALE": {"severity": "operational", "repair_queue": False,
              "needs_confirmation": False},
    "REAL": {"severity": "info", "repair_queue": False,
             "needs_confirmation": False},
    "RESOLVED": {"severity": "info", "repair_queue": False,
                 "needs_confirmation": False},
    "CLEAN": {"severity": "info", "repair_queue": False,
              "needs_confirmation": False},
}


def _date(value, label="date"):
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text = str(value or "").strip()
    if len(text) < 10:
        raise ValueError(f"missing {label}")
    try:
        return dt.date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ValueError(f"invalid {label}: {value!r}") from exc


def _positive(value, label):
    if isinstance(value, bool):
        raise ValueError(f"invalid {label}: {value!r}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {label}: {value!r}") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"invalid {label}: {value!r}")
    return number


def _nonnegative(value, label):
    if isinstance(value, bool):
        raise ValueError(f"invalid {label}: {value!r}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {label}: {value!r}") from exc
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"invalid {label}: {value!r}")
    return number


def _factor(value):
    ratio = _positive(value, "ratio")
    return max(ratio, 1.0 / ratio)


def _relative_error(value, expected):
    return abs(float(value) - float(expected)) / abs(float(expected))


def _source_ids(value):
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple, set)):
        return []
    normalized = set()
    for item in value:
        if item is None:
            continue
        text = str(item).strip()
        if text:
            normalized.add(text)
    return sorted(normalized)


def _valid_timestamp(value):
    text = str(value or "").strip()
    if not text:
        return False
    try:
        dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _result(ticker, verdict, reason, *, date=None, factor=None,
            confidence=None, matched=None, evidence=None,
            needs_confirmation=None):
    policy = dict(VERDICT_POLICY[verdict])
    if needs_confirmation is not None:
        policy["needs_confirmation"] = bool(needs_confirmation)
    if confidence is None:
        confidence = {
            "severe": "high",
            "review": "high",
            "operational": "low",
            "info": "high",
        }[policy["severity"]]
    row = {
        "ticker": str(ticker or "").strip().upper(),
        "verdict": verdict,
        "confidence": confidence,
        "severity": policy["severity"],
        "repair_queue": policy["repair_queue"],
        "needs_confirmation": policy["needs_confirmation"],
        "reason": reason,
        "recommended_action": _recommended_action(verdict),
        "matched": matched,
        "evidence": dict(evidence or {}),
    }
    if date is not None:
        row["date"] = _date(date).isoformat()
    if factor is not None:
        row["factor"] = round(float(factor), 8)
    return row


def _recommended_action(verdict):
    return {
        "PHANTOM": "confirm, then use the existing guarded correction flow",
        "MISSING": "review the unadjusted real split; do not auto-correct",
        "REGRESSION": "halt repair work and review the correction recurrence",
        "IDENTITY_BASIS": "review predecessor or identity lineage; do not apply xN",
        "UNVERIFIABLE": "obtain or refresh authoritative evidence",
        "STALE": "refresh split references",
        "REAL": "no repair; retain the confirmed event as information",
        "RESOLVED": "no repair; retain the durable correction record",
        "CLEAN": "no action",
    }[verdict]


def normalize_event(raw):
    """Return one canonical split event.

    Ratios use ``new_shares_per_old_share``. ``factor`` is orientation-free and
    therefore supports both forward and reverse splits.
    """
    if not isinstance(raw, dict):
        raise ValueError("event is not an object")
    event_date = _date(
        raw.get("ex_date", raw.get("date", raw.get("event_date"))),
        "event date")
    ratio_value = raw.get("ratio")
    if ratio_value is None:
        numerator = _positive(raw.get("numerator"), "event numerator")
        denominator = _positive(raw.get("denominator"), "event denominator")
        ratio_value = numerator / denominator
    ratio = _positive(ratio_value, "event ratio")
    convention = str(
        raw.get("ratio_convention") or "new_shares_per_old_share"
    ).strip().lower()
    if convention in {"old_shares_per_new_share", "old/new", "old_per_new"}:
        ratio = 1.0 / ratio
    elif convention not in {
        "new_shares_per_old_share", "new/old", "new_per_old",
    }:
        raise ValueError(f"unknown ratio convention: {convention}")
    confidence = str(raw.get("confidence") or "provisional").strip().lower()
    confirmed = (raw.get("confirmed") is True
                 or confidence in CONFIRMED_EVENT_LEVELS)
    ids = _source_ids(raw.get("source_ids"))
    if not ids and raw.get("source_id"):
        ids = _source_ids([raw.get("source_id")])
    if not ids:
        raise ValueError("event has no source identifier")
    if confidence == "corroborated" and len(ids) < 2:
        raise ValueError("corroborated event needs two source identifiers")
    return {
        "ex_date": event_date.isoformat(),
        "ratio": ratio,
        "factor": _factor(ratio),
        "ratio_convention": "new_shares_per_old_share",
        "confidence": "confirmed" if confirmed else "provisional",
        "confirmed": confirmed,
        "source_ids": ids,
    }


def normalize_coverage(raw):
    """Validate and derive negative-history completeness evidence."""
    raw = raw if isinstance(raw, dict) else {}
    errors = []
    level = str(raw.get("evidence_level") or "unknown").strip().lower()
    if level not in COVERAGE_EVIDENCE_LEVELS:
        errors.append(f"unknown coverage evidence level: {level}")
        level = "unknown"
    start = end = None
    try:
        start = _date(raw.get("from"), "coverage start")
        end = _date(raw.get("through"), "coverage end")
        if start > end:
            raise ValueError("coverage start is after coverage end")
    except ValueError as exc:
        errors.append(str(exc))
    basis = raw.get("complete_basis")
    ids = _source_ids(raw.get("source_ids"))
    basis_valid = isinstance(basis, dict) and bool(basis)
    if basis_valid:
        ids = sorted(set(ids) | set(_source_ids(basis.get("source_ids"))))
        ids = sorted(set(ids) | set(_source_ids(basis.get("source_id"))))
        required = {
            "kind", "payload_sha256", "captured_at", "reason",
        }
        missing = sorted(
            key for key in required if not str(basis.get(key) or "").strip())
        if missing:
            errors.append(
                "complete basis missing fields: " + ", ".join(missing))
            basis_valid = False
        elif str(basis.get("kind")).strip().lower() != level:
            errors.append("complete basis kind does not match evidence level")
            basis_valid = False
        digest = str(basis.get("payload_sha256") or "").strip().lower()
        if (len(digest) != 64
                or any(ch not in "0123456789abcdef" for ch in digest)):
            errors.append("complete basis payload_sha256 is invalid")
            basis_valid = False
        if not _valid_timestamp(basis.get("captured_at")):
            errors.append("complete basis captured_at is invalid")
            basis_valid = False
        locator_present = any(str(basis.get(key) or "").strip() for key in (
            "source_url", "filing_accession", "record_id"))
        support_present = any(str(basis.get(key) or "").strip() for key in (
            "statement_excerpt", "record_id"))
        if not locator_present:
            errors.append("complete basis has no source locator")
            basis_valid = False
        if not support_present:
            errors.append("complete basis has no captured support")
            basis_valid = False
    complete = bool(
        level in COMPLETE_EVIDENCE_LEVELS
        and start is not None
        and end is not None
        and basis_valid
        and ids
    )
    if raw.get("complete") and not complete:
        errors.append("coverage claimed complete without complete evidence")
    return {
        "from": start.isoformat() if start else None,
        "through": end.isoformat() if end else None,
        "evidence_level": level,
        "complete": complete,
        "complete_basis": basis if basis_valid else None,
        "source_ids": ids,
        "errors": errors,
    }


def normalize_history(raw):
    """Normalize history and expose malformed/conflicting source evidence."""
    raw = raw if isinstance(raw, dict) else {}
    errors = []
    events = []
    raw_events = raw.get("events")
    if raw_events is None:
        raw_events = []
    elif not isinstance(raw_events, (list, tuple)):
        errors.append("events is not a list")
        raw_events = []
    for index, event in enumerate(raw_events):
        try:
            events.append(normalize_event(event))
        except ValueError as exc:
            errors.append(f"event {index}: {exc}")
    events.sort(key=lambda row: (row["ex_date"], row["factor"], row["source_ids"]))
    conflicts = []
    for index, left in enumerate(events):
        left_date = _date(left["ex_date"])
        for right in events[index + 1:]:
            day_delta = abs((left_date - _date(right["ex_date"])).days)
            if day_delta > DATE_TOL_DAYS:
                if _date(right["ex_date"]) > left_date:
                    break
                continue
            if _relative_error(left["ratio"], right["ratio"]) > RATIO_TOL:
                conflicts.append({"left": left, "right": right})
    source_status = str(raw.get("source_status") or "ok").strip().lower()
    if source_status != "ok":
        errors.append(f"source status is {source_status}")
    return {
        "events": events,
        "coverage": normalize_coverage(raw.get("coverage")),
        "source_status": source_status,
        "conflicts": conflicts,
        "errors": errors,
    }


def _normalize_candidate(raw):
    if not isinstance(raw, dict):
        raise ValueError("candidate is not an object")
    ticker = str(raw.get("ticker") or "").strip().upper()
    if not ticker:
        raise ValueError("candidate ticker is missing")
    event_date = _date(
        raw.get("date", raw.get("boundary", raw.get("ex_date"))),
        "candidate date")
    step = _positive(raw.get("factor", raw.get("step")), "candidate factor")
    abs_factor = _factor(step)
    if math.isclose(abs_factor, 1.0, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError("candidate factor is a no-op")
    deep_cv = raw.get("deep_cv")
    if deep_cv is not None:
        deep_cv = _nonnegative(deep_cv, "deep_cv")
    return {
        "ticker": ticker,
        "date": event_date.isoformat(),
        "factor": step,
        "abs_factor": abs_factor,
        "deep_cv": deep_cv,
        "current_anchor_sound": raw.get("current_anchor_sound"),
        "volume": raw.get("volume"),
    }


def normalize_candidate(raw):
    """Validate and normalize one persisted detector candidate."""
    return _normalize_candidate(raw)


def _matches(date_value, factor, event):
    return (
        abs((_date(date_value) - _date(event["ex_date"])).days)
        <= DATE_TOL_DAYS
        and _relative_error(_factor(factor), event["factor"]) <= RATIO_TOL
    )


def _coverage_contains(coverage, date_value):
    if not coverage.get("complete"):
        return False
    day = _date(date_value)
    return _date(coverage["from"]) <= day <= _date(coverage["through"])


def volume_corroboration(candidate):
    """Classify optional stored/reference volume-step evidence."""
    volume = candidate.get("volume") if isinstance(candidate, dict) else None
    if not isinstance(volume, dict) or volume.get("comparable") is not True:
        return {"status": "unavailable"}
    try:
        pairs_pre = int(volume.get("pairs_pre") or 0)
        pairs_post = int(volume.get("pairs_post") or 0)
        spread = _nonnegative(volume.get("spread"), "volume spread")
        step = _positive(volume.get("step"), "volume step")
        price_step = _positive(candidate.get("factor"), "candidate factor")
    except (TypeError, ValueError) as exc:
        return {"status": "unavailable", "error": str(exc)}
    evidence = {
        "pairs_pre": pairs_pre,
        "pairs_post": pairs_post,
        "spread": round(spread, 6),
        "step": round(step, 8),
    }
    if (pairs_pre < VOLUME_MIN_PAIRS or pairs_post < VOLUME_MIN_PAIRS
            or spread > VOLUME_MAX_SPREAD):
        return {"status": "noisy", **evidence}
    expected = 1.0 / price_step
    error = _relative_error(step, expected)
    evidence.update({
        "expected_inverse_step": round(expected, 8),
        "inverse_error": round(error, 6),
    })
    if error <= VOLUME_INVERSE_TOL:
        return {"status": "corroborates", **evidence}
    if error >= VOLUME_CONTRADICTION_TOL:
        return {"status": "contradicts", **evidence}
    return {"status": "noisy", **evidence}


def _normalize_correction(raw, ticker):
    if not isinstance(raw, dict):
        return None
    correction_type = str(raw.get("type") or "").strip()
    if correction_type not in CORRECTION_TYPES:
        return None
    if not str(raw.get("applied") or "").strip():
        return None
    correction_ticker = str(raw.get("ticker") or ticker).strip().upper()
    if correction_ticker != ticker:
        return None
    if correction_type in storage.IDENTITY_CORRECTION_TYPES:
        try:
            if not storage.identity_correction_records(
                    {"data_corrections": [raw]}, "1d", ticker=ticker):
                return None
        except storage.StorageError:
            return None
    try:
        correction_date = _date(
            raw.get("ex_date", raw.get("boundary", raw.get("cutover"))),
            "correction date")
    except ValueError:
        return None
    factor = raw.get("factor")
    try:
        factor = _factor(factor) if factor is not None else None
    except ValueError:
        factor = None
    if correction_type == "phantom_split_correction" and factor is None:
        return None
    return {
        "type": correction_type,
        "date": correction_date.isoformat(),
        "factor": factor,
        "applied": raw.get("applied"),
        "source": raw,
    }


def _correction_input_errors(corrections, ticker):
    if corrections is None:
        return []
    if not isinstance(corrections, (list, tuple)):
        return ["corrections is not a list"]
    errors = []
    for index, raw in enumerate(corrections):
        if not isinstance(raw, dict):
            errors.append(f"correction {index} is not an object")
            continue
        correction_type = str(raw.get("type") or "").strip()
        if correction_type not in CORRECTION_TYPES:
            continue
        correction_ticker = str(raw.get("ticker") or ticker).strip().upper()
        if correction_ticker != ticker:
            continue
        if correction_type in storage.IDENTITY_CORRECTION_TYPES:
            try:
                applicable = storage.identity_correction_records(
                    {"data_corrections": [raw]}, "1d", ticker=ticker)
            except storage.StorageError:
                errors.append(f"correction {index} is malformed")
                continue
            if not applicable:
                continue
        if _normalize_correction(raw, ticker) is None:
            errors.append(f"correction {index} is malformed")
    return errors


def _matching_corrections(candidate, corrections):
    matches = []
    rows = corrections if isinstance(corrections, (list, tuple)) else []
    for raw in rows:
        correction = _normalize_correction(raw, candidate["ticker"])
        if correction is None:
            continue
        if abs((_date(candidate["date"]) - _date(correction["date"])).days) > DATE_TOL_DAYS:
            continue
        if (correction["factor"] is not None
                and _relative_error(candidate["abs_factor"], correction["factor"])
                > RATIO_TOL):
            continue
        matches.append(correction)
    return matches


def classify_candidate(candidate, history, corrections=None):
    """Classify one detected split-shaped price/reference seam."""
    try:
        cand = _normalize_candidate(candidate)
    except ValueError as exc:
        ticker = candidate.get("ticker") if isinstance(candidate, dict) else ""
        return _result(ticker, "UNVERIFIABLE", "invalid_candidate",
                       evidence={"errors": [str(exc)]})
    hist = normalize_history(history)
    volume = volume_corroboration(cand)
    evidence = {
        "abs_factor": round(cand["abs_factor"], 8),
        "deep_cv": cand["deep_cv"],
        "current_anchor_sound": cand["current_anchor_sound"],
        "coverage": hist["coverage"],
        "volume": volume,
    }
    source_conflicts = hist["conflicts"]
    matches = [event for event in hist["events"]
               if _matches(cand["date"], cand["factor"], event)]
    confirmed = [event for event in matches if event["confirmed"]]
    provisional = [event for event in matches if not event["confirmed"]]
    correction_matches = _matching_corrections(cand, corrections)
    correction_errors = _correction_input_errors(
        corrections, cand["ticker"])
    evidence.update({
        "history_errors": hist["errors"],
        "source_conflicts": source_conflicts,
        "provisional_matches": provisional,
        "corrections": correction_matches,
        "correction_errors": correction_errors,
    })

    if source_conflicts:
        return _result(cand["ticker"], "UNVERIFIABLE", "source_conflict",
                       date=cand["date"], factor=cand["factor"], evidence=evidence)
    if hist["errors"]:
        return _result(cand["ticker"], "UNVERIFIABLE",
                       "source_unavailable_or_malformed",
                       date=cand["date"], factor=cand["factor"], evidence=evidence)
    if correction_errors:
        return _result(cand["ticker"], "UNVERIFIABLE",
                       "correction_records_malformed",
                       date=cand["date"], factor=cand["factor"], evidence=evidence)
    if matches and correction_matches:
        return _result(cand["ticker"], "UNVERIFIABLE",
                       "event_evidence_conflicts_with_correction",
                       date=cand["date"], factor=cand["factor"],
                       matched=matches[0], evidence=evidence)
    if correction_matches:
        return _result(cand["ticker"], "REGRESSION",
                       "corrected_candidate_reappeared",
                       date=cand["date"], factor=cand["factor"],
                       matched=correction_matches[0], evidence=evidence)
    if confirmed:
        confidence = "medium" if volume["status"] == "contradicts" else "high"
        return _result(cand["ticker"], "REAL", "confirmed_event_match",
                       date=cand["date"], factor=cand["factor"],
                       confidence=confidence, matched=confirmed[0], evidence=evidence,
                       needs_confirmation=False)
    if provisional:
        return _result(cand["ticker"], "UNVERIFIABLE",
                       "matching_event_is_provisional",
                       date=cand["date"], factor=cand["factor"],
                       matched=provisional[0], evidence=evidence)
    if not _coverage_contains(hist["coverage"], cand["date"]):
        return _result(cand["ticker"], "UNVERIFIABLE",
                       "negative_history_not_proven",
                       date=cand["date"], factor=cand["factor"], evidence=evidence)
    if cand["current_anchor_sound"] is not True:
        return _result(cand["ticker"], "UNVERIFIABLE",
                       "current_anchor_not_proven",
                       date=cand["date"], factor=cand["factor"], evidence=evidence)
    if cand["deep_cv"] is None:
        return _result(cand["ticker"], "UNVERIFIABLE",
                       "uniformity_not_measured",
                       date=cand["date"], factor=cand["factor"], evidence=evidence)
    if volume["status"] == "contradicts":
        return _result(cand["ticker"], "UNVERIFIABLE",
                       "comparable_volume_contradicts_price_step",
                       date=cand["date"], factor=cand["factor"], evidence=evidence)
    if cand["deep_cv"] >= CV_UNIFORM:
        return _result(cand["ticker"], "IDENTITY_BASIS",
                       "nonuniform_deep_ratio",
                       date=cand["date"], factor=cand["factor"], evidence=evidence,
                       needs_confirmation=False)
    confidence = "high" if volume["status"] == "corroborates" else "medium"
    return _result(cand["ticker"], "PHANTOM",
                   "complete_history_has_no_event_and_ratio_is_uniform",
                   date=cand["date"], factor=cand["factor"],
                   confidence=confidence, evidence=evidence,
                   needs_confirmation=False)


def _history_from_real_splits(real_splits, coverage=None):
    events = []
    source_status = "ok"
    if real_splits is None:
        real_splits = {}
    elif not isinstance(real_splits, dict):
        real_splits = {}
        source_status = "malformed"
    for event_date, ratio in real_splits.items():
        events.append({
            "ex_date": event_date,
            "ratio": ratio,
            "confidence": "confirmed",
            "source_ids": ["caller-confirmed-history"],
        })
    return {
        "events": events,
        "source_status": source_status,
        "coverage": coverage or {
            "evidence_level": "provisional",
            "complete": False,
        },
    }


def classify_step(ticker, step_date, step_factor, real_splits=None, *,
                  coverage=None, deep_cv=0.0, current_anchor_sound=True,
                  volume=None, corrections=None):
    """Compatibility entry point for one already-detected step.

    Without typed complete ``coverage``, an unmatched step is UNVERIFIABLE.
    """
    event_map = real_splits
    if (isinstance(real_splits, dict)
            and isinstance(real_splits.get(ticker), dict)):
        event_map = real_splits[ticker]
    return classify_candidate(
        {
            "ticker": ticker,
            "date": step_date,
            "factor": step_factor,
            "deep_cv": deep_cv,
            "current_anchor_sound": current_anchor_sound,
            "volume": volume,
        },
        _history_from_real_splits(event_map, coverage=coverage),
        corrections=corrections,
    )


def classify_event_adjustment(ticker, event, observation):
    """Classify whether stored prices smooth one confirmed real split."""
    try:
        normalized = normalize_event(event)
    except ValueError as exc:
        return _result(ticker, "UNVERIFIABLE", "invalid_event",
                       evidence={"errors": [str(exc)]})
    if not normalized["confirmed"]:
        return _result(ticker, "UNVERIFIABLE", "event_is_provisional",
                       date=normalized["ex_date"], matched=normalized)
    observation = observation if isinstance(observation, dict) else {}
    ratios = []
    try:
        if observation.get("observed_ratio") is not None:
            ratios.append(_positive(observation["observed_ratio"],
                                    "observed price ratio"))
        else:
            pre_close = _positive(observation.get("pre_close"), "pre close")
            for key in ("post_open", "post_close"):
                if observation.get(key) is not None:
                    ratios.append(_positive(observation[key], key) / pre_close)
    except ValueError as exc:
        return _result(ticker, "UNVERIFIABLE", "insufficient_event_bars",
                       date=normalized["ex_date"], matched=normalized,
                       evidence={"errors": [str(exc)]})
    if not ratios:
        return _result(ticker, "UNVERIFIABLE", "insufficient_event_bars",
                       date=normalized["ex_date"], matched=normalized)
    observed = st.median(ratios)
    expected_raw = 1.0 / normalized["ratio"]
    smooth_errors = [_relative_error(value, 1.0) for value in ratios]
    raw_errors = [_relative_error(value, expected_raw) for value in ratios]
    smooth_error = max(smooth_errors)
    raw_error = max(raw_errors)
    evidence = {
        "observed_ratios": [round(value, 8) for value in ratios],
        "observed_ratio": round(observed, 8),
        "expected_raw_ratio": round(expected_raw, 8),
        "smooth_error": round(smooth_error, 6),
        "raw_error": round(raw_error, 6),
    }
    smooth_match = all(
        error <= SMOOTH_PRICE_TOL for error in smooth_errors)
    raw_match = all(
        error <= RAW_SPLIT_PRICE_TOL for error in raw_errors)
    evidence.update({
        "smooth_match": smooth_match,
        "raw_match": raw_match,
    })
    if smooth_match and raw_match:
        return _result(ticker, "UNVERIFIABLE",
                       "ratio_too_small_to_distinguish",
                       date=normalized["ex_date"], matched=normalized,
                       evidence=evidence)
    if smooth_match:
        return _result(ticker, "REAL", "stored_prices_are_split_adjusted",
                       date=normalized["ex_date"], matched=normalized,
                       evidence=evidence, needs_confirmation=False)
    if raw_match:
        return _result(ticker, "MISSING", "stored_prices_show_raw_split_jump",
                       date=normalized["ex_date"], matched=normalized,
                       evidence=evidence, needs_confirmation=False)
    if (any(error <= SMOOTH_PRICE_TOL for error in smooth_errors)
            and any(error <= RAW_SPLIT_PRICE_TOL for error in raw_errors)):
        reason = "event_price_bars_conflict"
    else:
        reason = "event_price_jump_is_ambiguous"
    return _result(ticker, "UNVERIFIABLE", reason,
                   date=normalized["ex_date"], matched=normalized,
                   evidence=evidence)


def find_missing_splits(ticker, history, observations):
    """Return confirmed events whose observed stored prices are unadjusted."""
    hist = normalize_history(history)
    if hist["errors"] or hist["conflicts"]:
        return []
    observations = observations if isinstance(observations, dict) else {}
    missing = []
    for event in hist["events"]:
        if not event["confirmed"]:
            continue
        row = classify_event_adjustment(
            ticker, event, observations.get(event["ex_date"]))
        if row["verdict"] == "MISSING":
            missing.append(row)
    return missing


def reconcile_corrections(ticker, candidates, history, corrections, *,
                          detection_current=False):
    """Reconcile durable corrections against a current candidate refresh.

    ``detection_current`` must be true before an absent candidate can become
    RESOLVED or a present one can become REGRESSION. Phase 3 supplies this from
    its series/reference fingerprint check.
    """
    ticker = str(ticker or "").strip().upper()
    correction_errors = _correction_input_errors(corrections, ticker)
    if correction_errors:
        return [_result(
            ticker, "UNVERIFIABLE", "correction_records_malformed",
            evidence={"correction_errors": correction_errors})]
    candidate_input_valid = (
        candidates is not None
        and not isinstance(candidates, (dict, str, bytes)))
    try:
        candidates = list(candidates) if candidate_input_valid else []
    except TypeError:
        candidates = []
        candidate_input_valid = False
    candidate_errors = []
    if candidate_input_valid:
        for index, candidate in enumerate(candidates):
            try:
                _normalize_candidate(candidate)
            except ValueError as exc:
                candidate_errors.append(f"candidate {index}: {exc}")
    if candidate_errors:
        candidate_input_valid = False
    detection_current = detection_current is True and candidate_input_valid
    rows = []
    used = set()
    seen = set()
    for raw_correction in corrections or []:
        correction = _normalize_correction(raw_correction, ticker)
        if correction is None:
            continue
        key = (correction["type"], correction["date"], correction["factor"])
        if key in seen:
            continue
        seen.add(key)
        if detection_current is not True:
            rows.append(_result(
                ticker, "UNVERIFIABLE", "correction_detection_not_current",
                date=correction["date"], factor=correction["factor"],
                matched=correction,
                evidence={"candidate_errors": candidate_errors}))
            continue
        matched_index = None
        for index, candidate in enumerate(candidates):
            if index in used:
                continue
            try:
                cand = _normalize_candidate(candidate)
            except ValueError:
                continue
            if _matching_corrections(cand, [raw_correction]):
                matched_index = index
                break
        if matched_index is None:
            rows.append(_result(
                ticker, "RESOLVED", "recorded_correction_has_no_candidate",
                date=correction["date"], factor=correction["factor"],
                matched=correction, needs_confirmation=False))
        else:
            used.add(matched_index)
            rows.append(classify_candidate(
                candidates[matched_index], history, corrections=[raw_correction]))
    rows.sort(key=lambda row: (row.get("date") or "", row["verdict"]))
    return rows
