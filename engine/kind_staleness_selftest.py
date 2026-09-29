"""Deterministic self-tests for production per-kind staleness classification."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import coverage_audit as coverage  # noqa: E402


FAILS = []
CHECKS = [0]


def check(name, condition, detail=""):
    CHECKS[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILS.append(name)


def fixture(primary_months, kinds):
    intervals = {}
    if primary_months:
        intervals[coverage.PRIMARY_INTERVAL] = {
            "months": {
                month: {"status": "present"} for month in primary_months
            },
        }
    for token, months in kinds.items():
        intervals[token] = {
            "months": {
                month: {"status": "present"} for month in months
            },
        }
    return {"intervals": intervals}


def verdict(rows, kind):
    for row in rows:
        if row["kind"] == kind:
            return row
    return None


rows = coverage.classify_kind_staleness(fixture(
    ["2014-06", "2026-07"], {"1m-iv": ["2014-05", "2014-06"]}))
check("deep-frozen kind flags",
      verdict(rows, "1m-iv")["verdict"] == "KIND_FORWARD_STALE")

rows = coverage.classify_kind_staleness(fixture(
    ["2026-06", "2026-07"], {"1m-iv": ["2026-07"]}))
check("kind at primary frontier is current",
      verdict(rows, "1m-iv")["verdict"] == "CURRENT")

rows = coverage.classify_kind_staleness(fixture(
    ["2026-06", "2026-07"], {"1d": ["2026-06"]}))
check("one-month lag is current",
      verdict(rows, "1d")["verdict"] == "CURRENT")

rows = coverage.classify_kind_staleness(fixture(
    ["2026-05", "2026-07"], {"1d": ["2026-05"]}))
check("two-month lag is stale",
      verdict(rows, "1d") == {
          "kind": "1d", "kind_last": "2026-05",
          "primary_last": "2026-07", "lag_months": 2,
          "verdict": "KIND_FORWARD_STALE",
      }, str(rows))

rows = coverage.classify_kind_staleness(fixture(
    ["2019-01", "2020-05"], {
        "1d": ["2020-05"], "1m-iv": ["2020-04"],
    }))
check("delisted ticker kinds remain current",
      all(row["verdict"] == "CURRENT" for row in rows), str(rows))

rows = coverage.classify_kind_staleness(fixture(
    [], {"1d": ["2020-05"]}))
check("missing exact primary is unevaluated",
      verdict(rows, "1d")["verdict"] == "UNEVALUATED")

rows = coverage.classify_kind_staleness(fixture(
    ["2026-07"], {"1d": []}))
check("zero-month kind is skipped", verdict(rows, "1d") is None, str(rows))

rows = coverage.classify_kind_staleness(fixture(
    ["2017-01", "2026-07"], {
        "1d": ["2017-02"], "1d-hvol": ["2026-07"],
    }))
check("CMCSA shape isolates frozen daily kind",
      verdict(rows, "1d")["verdict"] == "KIND_FORWARD_STALE"
      and verdict(rows, "1d-hvol")["verdict"] == "CURRENT", str(rows))

rows = coverage.classify_kind_staleness(fixture(
    ["2026-07"], {
        "1m-pre": ["2026-07"], "1m-post": ["2026-05"],
    }))
check("session kinds use the same threshold",
      verdict(rows, "1m-pre")["verdict"] == "CURRENT"
      and verdict(rows, "1m-post")["verdict"] == "KIND_FORWARD_STALE",
      str(rows))

print(f"\n{CHECKS[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED:", ", ".join(FAILS))
    raise SystemExit(1)
print("ALL PASS")
