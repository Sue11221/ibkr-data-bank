"""Pure self-tests for split_detector.py.

Run:
    python engine/split_detector_selftest.py

The fixtures are in-memory. They perform no filesystem writes and no network
operations.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import split_detector as sd  # noqa: E402


FAILS = []
N = [0]


def check(name, condition, detail=""):
    N[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILS.append(name)


def complete_coverage(start="1980-01-01", through="2026-12-31",
                      level="issuer_explicit_history"):
    source_id = "issuer-complete-history"
    return {
        "from": start,
        "through": through,
        "evidence_level": level,
        "complete": True,
        "complete_basis": {
            "kind": level,
            "source_url": "https://issuer.example/split-history",
            "payload_sha256": "a" * 64,
            "captured_at": "2026-07-09T12:00:00Z",
            "statement_excerpt": "Complete split history fixture.",
            "reason": "issuer explicitly enumerates every split",
            "source_ids": [source_id],
        },
        "source_ids": [source_id],
    }


def provisional_coverage(start="1980-01-01", through="2026-12-31"):
    return {
        "from": start,
        "through": through,
        "evidence_level": "provisional",
        "complete": False,
        "complete_basis": None,
        "source_ids": ["discovery-feed"],
    }


def event(ex_date, ratio, *, confidence="confirmed", source_ids=None,
          ratio_convention="new_shares_per_old_share"):
    return {
        "ex_date": ex_date,
        "ratio": ratio,
        "ratio_convention": ratio_convention,
        "confidence": confidence,
        "source_ids": source_ids or ["authoritative-event"],
    }


def history(events=None, coverage=None, source_status="ok"):
    return {
        "events": list(events or []),
        "coverage": coverage if coverage is not None else provisional_coverage(),
        "source_status": source_status,
    }


def candidate(ticker="TEST", date="2014-01-13", factor=4.0,
              deep_cv=0.001, current_anchor_sound=True, volume=None):
    return {
        "ticker": ticker,
        "date": date,
        "factor": factor,
        "deep_cv": deep_cv,
        "current_anchor_sound": current_anchor_sound,
        "volume": volume,
    }


def correction(ticker="FTNT", date="2014-01-13", factor=4.0,
               applied="2026-07-09T12:00:00Z"):
    return {
        "type": "phantom_split_correction",
        "ticker": ticker,
        "ex_date": date,
        "factor": factor,
        "applied": applied,
    }


def verdict(name, row, expected):
    check(name, row["verdict"] == expected,
          f"got {row['verdict']}: {row}")


# Normalization and typed completeness.
forward = sd.normalize_event(event("2022-06-23", 5.0))
check("event: forward ratio stays new/old",
      forward["ratio"] == 5.0 and forward["factor"] == 5.0, str(forward))

reverse = sd.normalize_event(event("2021-08-02", 10.0,
                                   ratio_convention="old/new"))
check("event: old/new reverse ratio is inverted",
      reverse["ratio"] == 0.1 and reverse["factor"] == 10.0, str(reverse))

try:
    sd.normalize_event(event("2022-01-01", 2.0,
                             confidence="corroborated",
                             source_ids=["only-one-source"]))
    corroboration_rejected = False
except ValueError:
    corroboration_rejected = True
check("event: corroborated requires two sources", corroboration_rejected)

complete = sd.normalize_coverage(complete_coverage())
check("coverage: captured typed evidence derives complete",
      complete["complete"] and not complete["errors"], str(complete))

documented = sd.normalize_coverage(
    complete_coverage(level="provider_documented_complete"))
check("coverage: documented complete provider evidence is supported",
      documented["complete"] and not documented["errors"], str(documented))

weak_complete = complete_coverage()
weak_complete["complete_basis"] = {
    "kind": "issuer_explicit_history",
    "source_ids": ["issuer-complete-history"],
}
weak = sd.normalize_coverage(weak_complete)
check("coverage: nonempty but incomplete basis cannot prove completeness",
      not weak["complete"] and bool(weak["errors"]), str(weak))

wrong_kind = complete_coverage()
wrong_kind["complete_basis"]["kind"] = "provider_documented_complete"
wrong = sd.normalize_coverage(wrong_kind)
check("coverage: basis kind must match evidence level",
      not wrong["complete"] and bool(wrong["errors"]), str(wrong))

bad_capture = complete_coverage()
bad_capture["complete_basis"]["captured_at"] = "not-a-timestamp"
bad_time = sd.normalize_coverage(bad_capture)
check("coverage: captured evidence needs an ISO timestamp",
      not bad_time["complete"] and bool(bad_time["errors"]), str(bad_time))


# Core candidate classifications.
ftnt_history = history([
    event("2011-06-02", 2.0, source_ids=["ftnt-2011"]),
    event("2022-06-23", 5.0, source_ids=["ftnt-2022"]),
], complete_coverage())

ftnt_phantom = sd.classify_candidate(
    candidate("FTNT", "2014-01-13", 4.0, deep_cv=0.0009), ftnt_history)
verdict("candidate: archived FTNT 2014 x4 is PHANTOM",
        ftnt_phantom, "PHANTOM")
check("policy: PHANTOM is severe repair work",
      ftnt_phantom["severity"] == "severe"
      and ftnt_phantom["repair_queue"]
      and not ftnt_phantom["needs_confirmation"], str(ftnt_phantom))

ftnt_real = sd.classify_candidate(
    candidate("FTNT", "2022-06-23", 5.0), ftnt_history)
verdict("candidate: FTNT 2022 x5 is REAL", ftnt_real, "REAL")

nvda_real = sd.classify_candidate(
    candidate("NVDA", "2024-06-10", 10.0),
    history([event("2024-06-10", 10.0, source_ids=["nvda-2024"])],
            provisional_coverage()))
verdict("candidate: confirmed NVDA event is REAL with provisional coverage",
        nvda_real, "REAL")

reverse_real = sd.classify_candidate(
    candidate("GE", "2021-08-02", 0.125),
    history([event("2021-08-02", 0.125, source_ids=["ge-2021"])],
            provisional_coverage()))
verdict("candidate: reverse split matches by absolute factor",
        reverse_real, "REAL")

wbd = sd.classify_candidate(
    candidate("WBD", "2021-03-30", 2.0, deep_cv=0.1289),
    history([], complete_coverage()))
verdict("candidate: drifting WBD x2-like seam is IDENTITY_BASIS",
        wbd, "IDENTITY_BASIS")
check("policy: IDENTITY_BASIS is review repair work",
      wbd["severity"] == "review" and wbd["repair_queue"], str(wbd))

incomplete = sd.classify_candidate(
    candidate("OLD", "2004-02-02", 2.0),
    history([], provisional_coverage()))
verdict("candidate: incomplete negative history is UNVERIFIABLE",
        incomplete, "UNVERIFIABLE")
check("policy: UNVERIFIABLE needs confirmation but is not repair work",
      incomplete["needs_confirmation"] and not incomplete["repair_queue"],
      str(incomplete))

pre_xbrl = sd.classify_candidate(
    candidate("LEGACY", "2004-02-02", 3.0),
    history([], {
        "from": "2009-01-01",
        "through": "2026-12-31",
        "evidence_level": "unknown",
        "complete": False,
        "source_ids": ["companyfacts-missing-pre-xbrl"],
    }))
verdict("candidate: missing pre-XBRL CompanyFacts is UNVERIFIABLE",
        pre_xbrl, "UNVERIFIABLE")

provisional_event = sd.classify_candidate(
    candidate("DISC", "2020-01-02", 2.0),
    history([event("2020-01-02", 2.0, confidence="provisional",
                   source_ids=["discovery-only"])], provisional_coverage()))
verdict("candidate: provisional event match is UNVERIFIABLE",
        provisional_event, "UNVERIFIABLE")

outside_coverage = sd.classify_candidate(
    candidate("OLD", "1990-01-02", 2.0),
    history([], complete_coverage(start="2000-01-01")))
verdict("candidate: complete evidence outside candidate date is UNVERIFIABLE",
        outside_coverage, "UNVERIFIABLE")

bad_anchor = sd.classify_candidate(
    candidate("ANCH", current_anchor_sound=False),
    history([], complete_coverage()))
verdict("candidate: unproven current anchor fails closed",
        bad_anchor, "UNVERIFIABLE")

no_uniformity = sd.classify_candidate(
    candidate("NOCV", deep_cv=None), history([], complete_coverage()))
verdict("candidate: missing uniformity measurement fails closed",
        no_uniformity, "UNVERIFIABLE")

bad_uniformity = sd.classify_candidate(
    candidate("BADCV", deep_cv="not-a-number"),
    history([], complete_coverage()))
verdict("candidate: malformed uniformity does not escape as an exception",
        bad_uniformity, "UNVERIFIABLE")

no_op = sd.classify_candidate(
    candidate("NOOP", factor=1.0), history([], complete_coverage()))
verdict("candidate: a no-op factor is invalid rather than PHANTOM",
        no_op, "UNVERIFIABLE")

string_false_event = event(
    "2020-01-02", 2.0, confidence="provisional",
    source_ids=["discovery-only"])
string_false_event["confirmed"] = "false"
string_false = sd.classify_candidate(
    candidate("BOOL", "2020-01-02", 2.0),
    history([string_false_event], provisional_coverage()))
verdict("candidate: string false cannot promote an event to confirmed",
        string_false, "UNVERIFIABLE")

no_provenance = sd.classify_candidate(
    candidate("NOSRC", "2020-01-02", 2.0),
    history([{
        "ex_date": "2020-01-02",
        "ratio": 2.0,
        "confidence": "provisional",
    }], complete_coverage()))
verdict("candidate: event without provenance makes history malformed",
        no_provenance, "UNVERIFIABLE")

events_not_list = sd.classify_candidate(
    candidate("BADHIST"),
    {"events": 5, "coverage": complete_coverage(), "source_status": "ok"})
verdict("candidate: non-list event history fails closed",
        events_not_list, "UNVERIFIABLE")

conflicting = sd.classify_candidate(
    candidate("CONFLICT", "2021-08-02", 10.0),
    history([
        event("2021-08-02", 10.0, source_ids=["forward-claim"]),
        event("2021-08-02", 0.1, source_ids=["reverse-claim"]),
    ], complete_coverage()))
verdict("candidate: same-factor opposite-orientation source conflict fails closed",
        conflicting, "UNVERIFIABLE")
check("candidate: source conflict has a distinct reason",
      conflicting["reason"] == "source_conflict", str(conflicting))

source_down = sd.classify_candidate(
    candidate("DOWN"), history([], complete_coverage(), source_status="down"))
verdict("candidate: source outage fails closed", source_down, "UNVERIFIABLE")
check("candidate: source outage is not mislabeled as a conflict",
      source_down["reason"] == "source_unavailable_or_malformed",
      str(source_down))


# Volume corroborates or fails closed, but never creates a verdict.
corroborating_volume = {
    "comparable": True,
    "pairs_pre": 20,
    "pairs_post": 20,
    "spread": 0.05,
    "step": 0.25,
}
volume_yes = sd.classify_candidate(
    candidate("VOLY", volume=corroborating_volume),
    history([], complete_coverage()))
verdict("volume: comparable inverse step retains PHANTOM", volume_yes, "PHANTOM")
check("volume: inverse step raises PHANTOM confidence",
      volume_yes["confidence"] == "high"
      and volume_yes["evidence"]["volume"]["status"] == "corroborates",
      str(volume_yes))

noisy_volume = dict(corroborating_volume, pairs_pre=3, spread=0.9)
volume_noisy = sd.classify_candidate(
    candidate("VOLN", volume=noisy_volume), history([], complete_coverage()))
verdict("volume: noisy evidence is neutral", volume_noisy, "PHANTOM")
check("volume: noisy evidence does not raise confidence",
      volume_noisy["confidence"] == "medium"
      and volume_noisy["evidence"]["volume"]["status"] == "noisy",
      str(volume_noisy))

volume_missing = sd.classify_candidate(
    candidate("VOLM", volume=None), history([], complete_coverage()))
verdict("volume: missing evidence is neutral", volume_missing, "PHANTOM")

missing_spread = dict(corroborating_volume)
missing_spread.pop("spread")
volume_incomplete = sd.classify_candidate(
    candidate("VOLI", volume=missing_spread), history([], complete_coverage()))
check("volume: incomplete comparable evidence cannot raise confidence",
      volume_incomplete["verdict"] == "PHANTOM"
      and volume_incomplete["confidence"] == "medium"
      and volume_incomplete["evidence"]["volume"]["status"] == "unavailable",
      str(volume_incomplete))

contradictory_volume = dict(corroborating_volume, step=4.0)
volume_no = sd.classify_candidate(
    candidate("VOLX", volume=contradictory_volume),
    history([], complete_coverage()))
verdict("volume: strong comparable contradiction fails closed",
        volume_no, "UNVERIFIABLE")

volume_only = sd.classify_candidate(
    candidate("VOLO", volume=corroborating_volume),
    history([], provisional_coverage()))
verdict("volume: inverse evidence cannot replace complete history",
        volume_only, "UNVERIFIABLE")


# Corrections resolve cleanly and recur severely.
resolved = sd.reconcile_corrections(
    "FTNT", [], ftnt_history, [correction()], detection_current=True)
check("correction: absent corrected candidate is RESOLVED",
      len(resolved) == 1 and resolved[0]["verdict"] == "RESOLVED",
      str(resolved))

regression = sd.reconcile_corrections(
    "FTNT", [candidate("FTNT", "2014-01-13", 4.0)],
    ftnt_history, [correction()], detection_current=True)
check("correction: corrected candidate recurrence is REGRESSION",
      len(regression) == 1
      and regression[0]["verdict"] == "REGRESSION"
      and regression[0]["severity"] == "severe",
      str(regression))

unapplied = sd.classify_candidate(
    candidate("FTNT", "2014-01-13", 4.0), ftnt_history,
    corrections=[correction(applied=None)])
verdict("correction: unapplied proposal fails closed",
        unapplied, "UNVERIFIABLE")

malformed_correction = sd.classify_candidate(
    candidate("FTNT", "2014-01-13", 4.0), ftnt_history,
    corrections={"type": "phantom_split_correction"})
verdict("correction: malformed correction container fails closed",
        malformed_correction, "UNVERIFIABLE")

event_correction_conflict = sd.classify_candidate(
    candidate("FTNT", "2022-06-23", 5.0), ftnt_history,
    corrections=[correction(date="2022-06-23", factor=5.0)])
verdict("correction: confirmed real event plus correction fails closed",
        event_correction_conflict, "UNVERIFIABLE")

provisional_correction_conflict = sd.classify_candidate(
    candidate("FTNT", "2014-01-13", 4.0),
    history([event("2014-01-13", 4.0, confidence="provisional",
                   source_ids=["discovery-only"])], complete_coverage()),
    corrections=[correction()])
verdict("correction: provisional event evidence also fails closed",
        provisional_correction_conflict, "UNVERIFIABLE")

not_refreshed = sd.reconcile_corrections(
    "FTNT", [], ftnt_history, [correction()])
check("correction: empty candidates without current refresh are UNVERIFIABLE",
      len(not_refreshed) == 1
      and not_refreshed[0]["verdict"] == "UNVERIFIABLE",
      str(not_refreshed))

malformed_refresh = sd.reconcile_corrections(
    "FTNT", [{"ticker": "FTNT", "factor": 4.0}],
    ftnt_history, [correction()], detection_current=True)
check("correction: malformed refreshed candidate cannot produce RESOLVED",
      len(malformed_refresh) == 1
      and malformed_refresh[0]["verdict"] == "UNVERIFIABLE"
      and malformed_refresh[0]["evidence"]["candidate_errors"],
      str(malformed_refresh))


def identity_correction(correction_type, intervals=None, cutover="2022-04-11"):
    note = {
        "type": correction_type,
        "ticker": "WBD",
        "boundary": cutover,
        "cutover": cutover,
        "applied": "2026-07-09T12:00:00Z",
    }
    if intervals is not None:
        note["intervals"] = list(intervals)
    return note


identity_results = {}
for correction_type in (
        "identity_listing_truncation", "identity_truncation", "truncation"):
    intervals = None if correction_type == "identity_listing_truncation" else ["1d"]
    identity_results[correction_type] = sd.reconcile_corrections(
        "WBD", [], history([], complete_coverage()),
        [identity_correction(correction_type, intervals)],
        detection_current=True)
check("correction: all factorless identity types can be RESOLVED",
      all(
          len(rows) == 1 and rows[0]["verdict"] == "RESOLVED"
          for rows in identity_results.values()),
      repr(identity_results))

identity_scope_miss = sd.reconcile_corrections(
    "WBD", [], history([], complete_coverage()),
    [identity_correction("identity_truncation", ["1m"])],
    detection_current=True)
check("correction: a nonmatching identity scope is ignored for daily price",
      identity_scope_miss == [], repr(identity_scope_miss))

identity_malformed = sd.reconcile_corrections(
    "WBD", [], history([], complete_coverage()),
    [identity_correction("identity_truncation")],
    detection_current=True)
check("correction: malformed recognized identity scope fails closed",
      len(identity_malformed) == 1
      and identity_malformed[0]["verdict"] == "UNVERIFIABLE"
      and identity_malformed[0]["reason"] == "correction_records_malformed",
      repr(identity_malformed))


# Stored adjustment checks, including reverse splits and conflicting bars.
forward_event = event("2022-06-23", 5.0, source_ids=["ftnt-2022"])
forward_missing = sd.classify_event_adjustment(
    "FTNT", forward_event,
    {"pre_close": 100.0, "post_open": 20.0, "post_close": 20.5})
verdict("adjustment: forward raw inverse jump is MISSING",
        forward_missing, "MISSING")

forward_smooth = sd.classify_event_adjustment(
    "FTNT", forward_event,
    {"pre_close": 100.0, "post_open": 99.0, "post_close": 101.0})
verdict("adjustment: smooth forward event is REAL", forward_smooth, "REAL")

reverse_event = event("2021-08-02", 0.125, source_ids=["ge-2021"])
reverse_missing = sd.classify_event_adjustment(
    "GE", reverse_event,
    {"pre_close": 10.0, "post_open": 80.0, "post_close": 79.0})
verdict("adjustment: reverse raw jump is MISSING", reverse_missing, "MISSING")

small_forward_event = event(
    "2020-01-02", 1.10, source_ids=["small-forward"])
small_forward_raw = sd.classify_event_adjustment(
    "SMALLF", small_forward_event, {"observed_ratio": 1.0 / 1.10})
verdict("adjustment: small forward raw jump is ambiguous",
        small_forward_raw, "UNVERIFIABLE")
check("adjustment: small forward overlap has an explicit reason",
      small_forward_raw["reason"] == "ratio_too_small_to_distinguish"
      and small_forward_raw["evidence"]["smooth_match"]
      and small_forward_raw["evidence"]["raw_match"],
      str(small_forward_raw))

small_reverse_event = event(
    "2020-02-03", 0.90, source_ids=["small-reverse"])
small_reverse_raw = sd.classify_event_adjustment(
    "SMALLR", small_reverse_event, {"observed_ratio": 1.0 / 0.90})
verdict("adjustment: small reverse raw jump is ambiguous",
        small_reverse_raw, "UNVERIFIABLE")

small_but_smooth_only = sd.classify_event_adjustment(
    "SMOOTHD", small_forward_event, {"observed_ratio": 1.10})
verdict("adjustment: distinguishable small-ratio observation remains REAL",
        small_but_smooth_only, "REAL")

distinguishable_raw = sd.classify_event_adjustment(
    "RAW12", event("2020-03-02", 1.20, source_ids=["raw-1.2"]),
    {"observed_ratio": 1.0 / 1.20})
verdict("adjustment: distinguishable 1.20 raw jump remains MISSING",
        distinguishable_raw, "MISSING")

ambiguous_jump = sd.classify_event_adjustment(
    "FTNT", forward_event,
    {"pre_close": 100.0, "post_open": 50.0, "post_close": 51.0})
verdict("adjustment: intermediate jump is UNVERIFIABLE",
        ambiguous_jump, "UNVERIFIABLE")

mixed_bars = sd.classify_event_adjustment(
    "FTNT", forward_event,
    {"pre_close": 100.0, "post_open": 20.0, "post_close": 100.0})
verdict("adjustment: raw/smooth post bars conflict and fail closed",
        mixed_bars, "UNVERIFIABLE")
check("adjustment: mixed bars report explicit conflict",
      mixed_bars["reason"] == "event_price_bars_conflict", str(mixed_bars))

missing_rows = sd.find_missing_splits(
    "FTNT", ftnt_history, {
        "2011-06-02": {"observed_ratio": 0.5},
        "2022-06-23": {"observed_ratio": 1.0},
    })
check("adjustment: find_missing_splits returns only unadjusted real event",
      len(missing_rows) == 1
      and missing_rows[0]["date"] == "2011-06-02",
      str(missing_rows))


# Compatibility entry point remains fail closed without complete coverage.
compat_unknown = sd.classify_step(
    "NONE", "2019-01-02", 3.0, real_splits={})
verdict("compatibility: unmatched step without typed coverage is UNVERIFIABLE",
        compat_unknown, "UNVERIFIABLE")

compat_phantom = sd.classify_step(
    "NONE", "2019-01-02", 3.0, real_splits={},
    coverage=complete_coverage())
verdict("compatibility: typed complete history permits PHANTOM",
        compat_phantom, "PHANTOM")

compat_full_map = sd.classify_step(
    "FTNT", "2022-06-23", 5.0,
    real_splits={"FTNT": {"2022-06-23": 5.0}})
verdict("compatibility: legacy ticker-to-history map still matches REAL",
        compat_full_map, "REAL")

compat_bad_map = sd.classify_step(
    "BADMAP", "2019-01-02", 3.0, real_splits=["not", "a", "map"],
    coverage=complete_coverage())
verdict("compatibility: malformed history map fails closed",
        compat_bad_map, "UNVERIFIABLE")


print(f"\n{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED:", ", ".join(FAILS))
    sys.exit(1)
print("ALL PASS")
