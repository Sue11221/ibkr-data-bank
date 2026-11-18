"""Self-tests for triage_classifier.py.

Standalone, no network. Run:
    python engine/triage_classifier_selftest.py
"""

import datetime as dt
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss  # noqa: E402
import triage_classifier as tc  # noqa: E402


FAILS = []
N = [0]


def check(name, cond, detail=""):
    N[0] += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


def bdays(start, end):
    day = start
    one = dt.timedelta(days=1)
    out = []
    while day <= end:
        if day.weekday() < 5:
            out.append(day.isoformat())
        day += one
    return out


def ref_for(days, close=100.0):
    return {d: (close, close, close, close, 1000) for d in days}


def stored_for(days, boundary, ratio_fn, close=100.0):
    b = dt.date.fromisoformat(boundary)
    out = {}
    for day in days:
        d = dt.date.fromisoformat(day)
        out[day] = close * ratio_fn(d, b)
    return out


BOUNDARY = "2020-04-01"
DAYS = bdays(dt.date(2020, 1, 1), dt.date(2020, 6, 30))
REF = ref_for(DAYS)


real = tc.classify_series("REAL", BOUNDARY, stored_for(
    DAYS, BOUNDARY, lambda _d, _b: 1.0), REF)
check("triage: flat ratio is REAL", real["verdict"] == "REAL", str(real))

benign = tc.classify_series("BEN", BOUNDARY, stored_for(
    DAYS, BOUNDARY, lambda d, b: 0.8 if d < b else 1.0), REF)
check("triage: non-integer step is BENIGN_ACTION",
      benign["verdict"] == "BENIGN_ACTION", str(benign))

phantom = tc.classify_series("PHAN", BOUNDARY, stored_for(
    DAYS, BOUNDARY, lambda d, b: 0.25 if d < b else 1.0), REF)
check("triage: uniform integer step is PHANTOM",
      phantom["verdict"] == "PHANTOM", str(phantom))
check("triage: PHANTOM is needs-human",
      phantom["bucket"] == "needs-human", str(phantom))

local = tc.classify_series("LOC", BOUNDARY, stored_for(
    DAYS, BOUNDARY, lambda d, b: 0.25 if d < b else 1.2), REF)
check("triage: current-anchor mismatch wins as LOCAL_SUSPECT",
      local["verdict"] == "LOCAL_SUSPECT", str(local))

id_boundary = "2021-03-30"
id_days = bdays(dt.date(2018, 1, 2), dt.date(2021, 6, 30))
id_ref = ref_for(id_days)


def id_ratio(day, boundary):
    if day >= boundary:
        return 1.0
    if day.year <= 2018:
        return 0.25
    if day.year == 2019:
        return 0.35
    return 0.5


identity = tc.classify_series("WBD", id_boundary, stored_for(
    id_days, id_boundary, id_ratio), id_ref)
check("triage: non-uniform split-magnitude step is IDENTITY_BASIS",
      identity["verdict"] == "IDENTITY_BASIS", str(identity))

resolved_root = Path(tempfile.mkdtemp(prefix="triage_resolved_st_"))
resolved_dir = resolved_root / "WBD"
resolved_dir.mkdir()
resolved_manifest = ss.new_manifest("WBD", "WBD")
resolved_path = ss.month_file_path(
    resolved_root, "WBD", 2022, 4, "1d")
resolved_path.parent.mkdir(parents=True, exist_ok=True)
resolved_bar = (
    dt.datetime(2022, 4, 11), 10.0, 11.0, 9.0, 10.0, 100)
resolved_stats = ss.write_month_file(
    resolved_path, [resolved_bar], verify_after_write=True)
ss.manifest_months(resolved_manifest, "1d")["2022-04"] = {
    **resolved_stats, "status": "present"}


def identity_note(correction_type, intervals=None, cutover="2022-04-11"):
    note = {
        "type": correction_type,
        "ticker": "WBD",
        "cutover": cutover,
        "applied": "TEST",
        "snapshot": "snapshot",
    }
    if intervals is not None:
        note["intervals"] = list(intervals)
    return note


identity_types = (
    "identity_listing_truncation", "identity_truncation", "truncation")
resolved_by_type = {}
for correction_type in identity_types:
    intervals = None if correction_type == "identity_listing_truncation" else ["1d"]
    resolved_manifest["data_corrections"] = [
        identity_note(correction_type, intervals)]
    ss.save_manifest(resolved_dir, resolved_manifest)
    resolved_by_type[correction_type] = tc.classify_flag(
        resolved_root, "WBD", id_boundary,
        ref_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("resolved boundary must not fetch")))

resolved = resolved_by_type["identity_truncation"]
check("triage: all identity correction types resolve an old boundary",
      all(
          row["verdict"] == "RESOLVED"
          and row["bucket"] == "auto-benign"
          for row in resolved_by_type.values()),
      repr(resolved_by_type))

resolved_manifest["data_corrections"] = [
    identity_note("identity_truncation", ["1m"])]
ss.save_manifest(resolved_dir, resolved_manifest)
scope_miss = tc._resolved_identity_truncation(
    resolved_root, "WBD", id_boundary)
resolved_manifest["data_corrections"] = [
    identity_note("identity_listing_truncation", cutover="not-a-date")]
ss.save_manifest(resolved_dir, resolved_manifest)
malformed_miss = tc._resolved_identity_truncation(
    resolved_root, "WBD", id_boundary)
check("triage: nonmatching scope and malformed identity notes fail closed",
      scope_miss is None and malformed_miss is None,
      repr((scope_miss, malformed_miss)))

# Restore the legacy exact-scope shape used by the partial-truncation fixture.
resolved_manifest["data_corrections"] = [
    identity_note("identity_truncation", ["1d"])]
ss.save_manifest(resolved_dir, resolved_manifest)

partial_root = Path(tempfile.mkdtemp(prefix="triage_partial_st_"))
partial_dir = partial_root / "WBD"
partial_dir.mkdir()
partial_manifest = ss.new_manifest("WBD", "WBD")
partial_path = ss.month_file_path(
    partial_root, "WBD", 2022, 4, "1d")
partial_path.parent.mkdir(parents=True, exist_ok=True)
partial_bars = [
    (dt.datetime(2022, 4, 1), 9.0, 10.0, 8.0, 9.0, 100),
    resolved_bar,
]
partial_stats = ss.write_month_file(
    partial_path, partial_bars, verify_after_write=True)
ss.manifest_months(partial_manifest, "1d")["2022-04"] = {
    **partial_stats, "status": "present"}
partial_manifest["data_corrections"] = list(
    resolved_manifest["data_corrections"])
ss.save_manifest(partial_dir, partial_manifest)
check("triage: correction note plus pre-cutover daily bar is not resolved",
      tc._resolved_identity_truncation(
          partial_root, "WBD", id_boundary) is None)
partial_classified = tc.classify_flag(
    partial_root, "WBD", id_boundary,
    ref_fn=lambda *_args, **_kwargs: {
        "2022-04-01": (9.0, 9.0, 9.0, 9.0, 100),
        "2022-04-11": (10.0, 10.0, 10.0, 10.0, 100),
    })
check("triage: partial correction state fails closed instead of RESOLVED",
      partial_classified["verdict"] != "RESOLVED", str(partial_classified))

short = tc.classify_series("SHORT", BOUNDARY, stored_for(
    DAYS[:10], BOUNDARY, lambda _d, _b: 1.0), ref_for(DAYS[:10]))
check("triage: too little overlap is SHORT",
      short["verdict"] == "SHORT", str(short))

scan_report = {
    "triage": {
        "confirmed_phantom_split_tickers": [
            {"ticker": "FTNT", "boundary": "2014-01-10",
             "ex_date": "2014-01-13", "verdict": "SEAM"},
        ],
        "split_like_review_tickers": [
            {"ticker": "WBD", "boundary": "2021-03-30",
             "verdict": "SEAM"},
        ],
        "benign_or_reference_basis_steps": [
            {"ticker": "ADP", "boundary": "2014-09-25",
             "verdict": "SEAM"},
        ],
    },
    "rows": [
        {"ticker": "FTNT", "boundary": "2014-01-10", "verdict": "SEAM"},
    ],
}
flags = tc.flags_from_report(scan_report)
check("triage: scan analysis flags are extracted without raw-row duplicates",
      flags == [
          {"ticker": "FTNT", "boundary": "2014-01-13",
           "verdict": "SEAM"},
          {"ticker": "WBD", "boundary": "2021-03-30",
           "verdict": "SEAM"},
          {"ticker": "ADP", "boundary": "2014-09-25",
           "verdict": "SEAM"},
      ],
      str(flags))

report = {
    "classified": [phantom, identity, benign, real],
    "counts": {"needs_human": 2, "auto_benign": 2,
               "by_verdict": {"PHANTOM": 1, "IDENTITY_BASIS": 1,
                              "BENIGN_ACTION": 1, "REAL": 1}},
    "input_count": 4,
}
counts = tc.summary_counts(report)
check("triage: summary counts expose needs-human and auto-benign",
      counts["needs_human"] == 2 and counts["auto_benign"] == 2,
      str(counts))
check("triage: queue includes only needs-human tickers",
      tc.format_queue(report) == ["PHAN", "WBD"],
      str(tc.format_queue(report)))

split_context = {
    "kind": "split_audit",
    "tickers": [{
        "ticker": "PHAN",
        "rows": [
            {"ticker": "PHAN", "date": BOUNDARY, "verdict": "PHANTOM",
             "scope": "current", "row_type": "candidate",
             "confidence": "high", "repair_queue": True,
             "needs_confirmation": False,
             "reason": "complete history has no event",
             "recommended_action": "confirm guarded correction"},
            {"ticker": "ASK", "date": BOUNDARY,
             "verdict": "UNVERIFIABLE", "scope": "current",
             "row_type": "candidate", "confidence": "low",
             "repair_queue": False, "needs_confirmation": True,
             "reason": "negative history is incomplete",
             "recommended_action": "obtain authoritative evidence"},
            {"ticker": "OLD", "date": BOUNDARY, "verdict": "STALE",
             "scope": "current", "row_type": "candidate",
             "confidence": "low", "repair_queue": False,
             "needs_confirmation": False,
             "recommended_action": "refresh split references"},
            {"ticker": "ARCH", "date": BOUNDARY, "verdict": "PHANTOM",
             "scope": "archived", "row_type": "candidate",
             "repair_queue": False},
        ],
    }],
}
context_rows, context_errors = tc.classify_flags(
    resolved_root,
    [{"ticker": "PHAN", "boundary": BOUNDARY},
     {"ticker": "ASK", "boundary": BOUNDARY},
     {"ticker": "OLD", "boundary": BOUNDARY}],
    ref_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("exact split context must win before reference")),
    split_context=split_context)
context_by_ticker = {row["ticker"]: row for row in context_rows}
check("triage: exact current split context wins before ratio reference",
      not context_errors
      and context_by_ticker["PHAN"]["verdict"] == "PHANTOM"
      and context_by_ticker["PHAN"]["evidence"]["source"] == "split_audit",
      str(context_rows))
check("triage: split confirmation and operational buckets stay non-repair",
      context_by_ticker["ASK"]["bucket"] == "needs-confirmation"
      and context_by_ticker["OLD"]["bucket"] == "operational"
      and tc.format_queue({"classified": context_rows}) == ["PHAN"],
      str(context_rows))
context_report = tc.audit(
    resolved_root,
    flags=[{"ticker": "PHAN", "boundary": BOUNDARY},
           {"ticker": "ASK", "boundary": BOUNDARY},
           {"ticker": "OLD", "boundary": BOUNDARY}],
    ref_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("context audit must stay offline")),
    split_context=split_context)
context_counts = tc.summary_counts(context_report)
check("triage: context summary preserves confirmation/operational counts",
      context_counts["needs_human"] == 1
      and context_counts["needs_confirmation"] == 1
      and context_counts["operational"] == 1,
      str(context_counts))
conflicting_context = {
    "rows": [
        {"ticker": "PHAN", "date": BOUNDARY, "verdict": "PHANTOM"},
        {"ticker": "PHAN", "date": BOUNDARY, "verdict": "REAL"},
    ]}
check("triage: archived, absent, or conflicting context is never guessed",
      tc._classify_from_split_context(
          {"ticker": "ARCH", "boundary": BOUNDARY}, split_context) is None
      and tc._classify_from_split_context(
          {"ticker": "NONE", "boundary": BOUNDARY}, split_context) is None
      and tc._classify_from_split_context(
          {"ticker": "PHAN", "boundary": BOUNDARY},
          conflicting_context) is None)

print(f"\n{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED:", ", ".join(FAILS))
    sys.exit(1)
print("ALL PASS")
