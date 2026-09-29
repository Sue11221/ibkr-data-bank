"""Offline deterministic tests for deep_seam_scan Phase 3A behavior."""

import copy
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import deep_seam_scan as seam  # noqa: E402
import split_detector as detector  # noqa: E402
import stock_validate as validate  # noqa: E402


FAILS = []
N = [0]


def check(name, condition, detail=""):
    N[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILS.append(name)


def ratio_series(ratios, start=dt.date(2010, 1, 1)):
    stored, reference, days = {}, {}, []
    for index, ratio in enumerate(ratios):
        day = (start + dt.timedelta(days=index)).isoformat()
        days.append(day)
        reference[day] = (100.0, 101.0, 99.0, 100.0, 1000)
        stored[day] = 100.0 * float(ratio)
    return stored, reference, days


# Pure CLEAN and SHORT behavior.
stored, reference, _days = ratio_series([1.0] * 180)
clean = seam.detect_steps("CLEAN", stored, reference)
check("pure: flat ratios are CLEAN with no candidates",
      clean["verdict"] == "CLEAN" and clean["candidates"] == []
      and clean["max_step"] == 1.0, str(clean))

stored, reference, _days = ratio_series([1.0] * (3 * seam.WIN - 1))
short = seam.detect_steps("SHORT", stored, reference)
check("pure: fewer than three windows is SHORT",
      short["verdict"] == "SHORT" and short["n"] == 3 * seam.WIN - 1,
      str(short))


# One rolling signal becomes one strongest clustered candidate.
stored, reference, days = ratio_series([0.25] * 120 + [1.0] * 120)
single = seam.detect_steps("ONE", stored, reference)
candidate = single["candidates"][0]
check("cluster: one abrupt step yields exactly one candidate",
      single["verdict"] == "SEAM" and len(single["candidates"]) == 1,
      str(single))
check("cluster: strongest boundary is the actual transition",
      candidate["date"] == days[120] and candidate["factor"] == 4.0,
      str(candidate))
check("candidate: current anchor and split metadata are retained",
      candidate["current_anchor_sound"] is True
      and candidate["split_like"] is True
      and candidate["near_split"] == 4
      and candidate["cluster_size"] > 1, str(candidate))
try:
    normalized = detector.normalize_candidate(candidate)
except ValueError:
    normalized = None
check("candidate: emitted row satisfies classifier schema",
      normalized is not None and normalized["ticker"] == "ONE",
      str(normalized))


# Two separated events remain distinct and deterministic.
stored, reference, days = ratio_series(
    [0.25] * 120 + [0.5] * 120 + [1.0] * 120)
multi_a = seam.detect_steps("MULTI", stored, reference)
multi_b = seam.detect_steps("MULTI", stored, reference)
check("multi: separated events produce two candidates",
      [row["date"] for row in multi_a["candidates"]]
      == [days[120], days[240]], str(multi_a))
check("multi: candidate output is byte-order deterministic",
      multi_a == multi_b)
check("multi: oriented factors and current anchor are preserved",
      [row["factor"] for row in multi_a["candidates"]] == [2.0, 2.0]
      and all(row["current_anchor_sound"] is True
              for row in multi_a["candidates"]), str(multi_a))

stored, reference, days = ratio_series([1.0] * 120 + [0.5] * 120)
reverse = seam.detect_steps("REVERSE", stored, reference)
check("candidate: reverse-oriented step retains factor below one",
      reverse["candidates"][0]["date"] == days[120]
      and reverse["candidates"][0]["factor"] == 0.5
      and reverse["candidates"][0]["abs_factor"] == 2.0,
      str(reverse))


# Storage wrapper returns all candidates/fingerprint; legacy wrapper returns
# only the strongest candidate and keeps the old result shape.
stored, reference, days = ratio_series(
    [0.25] * 120 + [0.5] * 120 + [2.0] * 120)
original_snapshot = seam._stored_1d_snapshot
try:
    seam._stored_1d_snapshot = lambda _root, _ticker: (stored, "a" * 64)
    wrapped = seam.scan_ticker_steps("unused", "WRAP", ref=reference)
    legacy = seam.scan_ticker("unused", "WRAP", ref=reference)
finally:
    seam._stored_1d_snapshot = original_snapshot
check("wrapper: all candidates carry the exact series fingerprint",
      len(wrapped["candidates"]) == 2
      and wrapped["series_fingerprint"] == "a" * 64, str(wrapped))
check("compatibility: scan_ticker returns only the largest legacy summary",
      legacy == {
          "ticker": "WRAP",
          "verdict": "SEAM",
          "boundary": days[240],
          "factor": 4.0,
          "abs_factor": 4.0,
          "split_like": True,
          "near_split": 4,
      }, str(legacy))


# Explicitly supplied empty reference must not fall through to a network fetch.
original_fetch = seam.sv.fetch_daily_reference
original_snapshot = seam._stored_1d_snapshot
fetch_calls = []
try:
    seam.sv.fetch_daily_reference = lambda *_args, **_kwargs: fetch_calls.append(1)
    seam._stored_1d_snapshot = lambda _root, _ticker: ({}, "b" * 64)
    empty_ref = seam.scan_ticker_steps("unused", "EMPTY", ref={})
finally:
    seam.sv.fetch_daily_reference = original_fetch
    seam._stored_1d_snapshot = original_snapshot
check("wrapper: supplied empty reference never triggers network fallback",
      empty_ref["verdict"] == "SHORT" and not fetch_calls, str(empty_ref))


# Reference and stored-series failures are structured and fail closed.
original_fetch = seam.sv.fetch_daily_reference
try:
    seam.sv.fetch_daily_reference = lambda *_args, **_kwargs: (
        (_ for _ in ()).throw(RuntimeError("reference down")))
    no_reference = seam.scan_ticker_steps("unused", "FAILREF")
finally:
    seam.sv.fetch_daily_reference = original_fetch
check("wrapper: reference failure is UNVERIFIABLE",
      no_reference["verdict"] == "UNVERIFIABLE"
      and "reference down" in no_reference["why"], str(no_reference))

original_snapshot = seam._stored_1d_snapshot
try:
    seam._stored_1d_snapshot = lambda *_args, **_kwargs: (
        (_ for _ in ()).throw(seam.SeamScanError("stored month unreadable")))
    no_stored = seam.scan_ticker_steps("unused", "FAILSTORE", ref=reference)
finally:
    seam._stored_1d_snapshot = original_snapshot
check("wrapper: stored-series failure is UNVERIFIABLE",
      no_stored["verdict"] == "UNVERIFIABLE"
      and "stored month unreadable" in no_stored["why"], str(no_stored))


# Fingerprint contract and before/after race rejection.
months = {
    "2020-01": {"sha256": "a" * 64, "mtime_ns": 10, "rows": 20},
    "2020-02": {"sha256": "b" * 64, "mtime_ns": 11, "rows": 21},
}
check("fingerprint: scanner matches the established validation digest",
      seam.manifest_fingerprint(months) == validate._cal_fingerprint(months))


def manifest(entry):
    return {
        "symbol": "RACE",
        "folder": "RACE",
        "generation": 1,
        "conid": 1,
        "aliases": [],
        "basis": "unknown",
        "written_by": {},
        "intervals": {
            "1d": {"months": {"2020-01": entry}, "verified_absent": []},
        },
    }


first = manifest({"sha256": "c" * 64, "mtime_ns": 1, "rows": 1})
second = manifest({"sha256": "d" * 64, "mtime_ns": 2, "rows": 1})
load_sequence = iter([copy.deepcopy(first), copy.deepcopy(second)])
original_load = seam.ss.load_manifest
original_find = seam.ss.find_month_file
original_read = seam.ss.read_month_file
try:
    seam.ss.load_manifest = lambda _path: next(load_sequence)
    seam.ss.find_month_file = lambda *_args, **_kwargs: Path("fixture.parquet")
    seam.ss.read_month_file = lambda _path: ([
        (dt.datetime(2020, 1, 2, 16), 1.0, 1.0, 1.0, 1.0, 1),
    ], {})
    try:
        seam._stored_1d_snapshot("unused", "RACE")
    except seam.SeamScanError as exc:
        race_error = str(exc)
    else:
        race_error = ""
finally:
    seam.ss.load_manifest = original_load
    seam.ss.find_month_file = original_find
    seam.ss.read_month_file = original_read
check("fingerprint: concurrent manifest change is rejected",
      "changed while reading" in race_error, race_error)

original_load = seam.ss.load_manifest
original_find = seam.ss.find_month_file
original_read = seam.ss.read_month_file
try:
    seam.ss.load_manifest = lambda _path: copy.deepcopy(first)
    seam.ss.find_month_file = lambda *_args, **_kwargs: Path("fixture.parquet")
    seam.ss.read_month_file = lambda _path: (
        (_ for _ in ()).throw(RuntimeError("bad parquet")))
    try:
        seam._stored_1d_snapshot("unused", "STRICT")
    except seam.SeamScanError as exc:
        strict_error = str(exc)
    else:
        strict_error = ""
finally:
    seam.ss.load_manifest = original_load
    seam.ss.find_month_file = original_find
    seam.ss.read_month_file = original_read
check("storage: unreadable month is rejected instead of skipped",
      "unreadable 1d month" in strict_error and "bad parquet" in strict_error,
      strict_error)


print(f"\n{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED:", ", ".join(FAILS))
    raise SystemExit(1)
print("ALL PASS")
