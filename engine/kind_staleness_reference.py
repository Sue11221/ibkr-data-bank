"""WS1b reference/acceptance harness — per-kind forward-staleness detection.

Spec-by-example for the detector that closes the CMCSA-1d class of silent
freeze: a NON-PRIMARY interval kind (1d, 1m-iv, 1d-hvol, 1m-pre, ...) whose
frontier stopped while the ticker's PRIMARY 1m series stayed current. The
existing coverage audit is primary-only (`coverage_audit.PRIMARY_INTERVAL`);
the gap scanner is interior-only — so this class is invisible today.

Rule pinned here:
  lag_months = months_between(kind_last_present, primary_last_present)
  lag >= STALE_MIN_MONTHS (2)  ->  KIND_FORWARD_STALE
  lag <  2                     ->  CURRENT   (partial-month / fetch-timing noise)
  no primary months            ->  UNEVALUATED (primary coverage is WS1's job)
  kind with zero present months -> skipped (not a series)

Comparing against PRIMARY (not the calendar) makes a delisted ticker CURRENT
on every kind automatically — the whole ticker stops together.

Report-only. The default gate is deterministic: live-bank rows are observations,
not invariants. Use ``--live-expectation pre-resume`` before the interrupted IV
campaign is resumed and ``--live-expectation post-resume`` after it completes.
Offline: manifest reads only; safe beside a live fetch (atomic replaces).
"""

import argparse
import json
import sys
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
BANK = ROOT / "Stock Data Storage"

PRIMARY = "1m"
STALE_MIN_MONTHS = 2

FAILURES = []
CHECKS = [0]


def check(name, cond, detail=""):
    CHECKS[0] += 1
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def months_between(earlier, later):
    ey, em = int(earlier[:4]), int(earlier[5:7])
    ly, lm = int(later[:4]), int(later[5:7])
    return (ly - ey) * 12 + (lm - em)


def classify_ticker(manifest):
    """-> list of {kind, kind_last, primary_last, lag_months, verdict}."""
    intervals = manifest.get("intervals") or {}

    def present_months(token):
        item = intervals.get(token) or {}
        months = item.get("months") or {}
        return sorted(
            str(key)[:7] for key, value in months.items()
            if isinstance(value, dict)
            and str(value.get("status") or "present").lower() == "present")

    primary = present_months(PRIMARY)
    rows = []
    for token in sorted(intervals):
        if token == PRIMARY:
            continue
        kind_months = present_months(token)
        if not kind_months:
            continue
        if not primary:
            rows.append({"kind": token, "kind_last": kind_months[-1],
                         "primary_last": None, "lag_months": None,
                         "verdict": "UNEVALUATED"})
            continue
        lag = months_between(kind_months[-1], primary[-1])
        rows.append({
            "kind": token,
            "kind_last": kind_months[-1],
            "primary_last": primary[-1],
            "lag_months": lag,
            "verdict": ("KIND_FORWARD_STALE" if lag >= STALE_MIN_MONTHS
                        else "CURRENT"),
        })
    return rows


def fixture(primary_months, kinds):
    intervals = {}
    if primary_months:
        intervals[PRIMARY] = {
            "months": {m: {"status": "present"} for m in primary_months}}
    for token, months in kinds.items():
        intervals[token] = {
            "months": {m: {"status": "present"} for m in months}}
    return {"intervals": intervals}


def verdict_of(rows, kind):
    for row in rows:
        if row["kind"] == kind:
            return row["verdict"]
    return None


# --- fixtures ----------------------------------------------------------------
rows = classify_ticker(fixture(
    ["2014-06", "2026-07"], {"1m-iv": ["2014-05", "2014-06"]}))
check("deep-frozen kind flags (FTNT-iv shape, 145-month lag)",
      verdict_of(rows, "1m-iv") == "KIND_FORWARD_STALE")

rows = classify_ticker(fixture(
    ["2026-06", "2026-07"], {"1m-iv": ["2026-07"]}))
check("kind at the primary frontier is CURRENT (FOXA shape)",
      verdict_of(rows, "1m-iv") == "CURRENT")

rows = classify_ticker(fixture(
    ["2026-06", "2026-07"], {"1d": ["2026-06"]}))
check("one month behind is CURRENT (partial-month tolerance)",
      verdict_of(rows, "1d") == "CURRENT")

rows = classify_ticker(fixture(
    ["2026-05", "2026-07"], {"1d": ["2026-05"]}))
check("two months behind flags (tolerance boundary)",
      verdict_of(rows, "1d") == "KIND_FORWARD_STALE")

rows = classify_ticker(fixture(
    ["2019-01", "2020-05"], {"1d": ["2020-05"], "1m-iv": ["2020-04"]}))
check("delisted ticker is CURRENT on every kind (primary frozen too)",
      verdict_of(rows, "1d") == "CURRENT"
      and verdict_of(rows, "1m-iv") == "CURRENT")

rows = classify_ticker(fixture(
    [], {"1d": ["2020-05"]}))
check("no primary months -> UNEVALUATED (never guessed stale)",
      verdict_of(rows, "1d") == "UNEVALUATED")

rows = classify_ticker(fixture(
    ["2026-07"], {"1d": []}))
check("kind with zero present months is skipped",
      verdict_of(rows, "1d") is None)

rows = classify_ticker(fixture(
    ["2017-01", "2026-07"],
    {"1d": ["2017-02"], "1d-hvol": ["2026-07"]}))
check("CMCSA regression shape: frozen 1d flags, current sibling kind clean",
      verdict_of(rows, "1d") == "KIND_FORWARD_STALE"
      and verdict_of(rows, "1d-hvol") == "CURRENT")

rows = classify_ticker(fixture(
    ["2026-07"], {"1m-pre": ["2026-07"], "1m-post": ["2026-05"]}))
check("session kinds use the same rule (post 2 months behind flags)",
      verdict_of(rows, "1m-pre") == "CURRENT"
      and verdict_of(rows, "1m-post") == "KIND_FORWARD_STALE")

# --- real-bank known answers (read-only; bank may be mid-fetch) ---------------
def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live-expectation",
        choices=("observe", "pre-resume", "post-resume"),
        default="observe",
        help=("gate the mutable Friday IV rows for an explicit campaign phase; "
              "default: print observations only"),
    )
    return parser.parse_args()


def bank_verdict(ticker, kind):
    try:
        mf = json.loads(
            (BANK / ticker / ss.MANIFEST_NAME).read_text(encoding="utf-8"))
        return verdict_of(classify_ticker(mf), kind), None
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        return None, f"{type(exc).__name__}: {exc}"


ARGS = parse_args()
DEEP_STUBS = ("FLEX", "FRT", "FSLR", "FTNT", "FTV", "GD", "GDDY")
LIVE_ROWS = tuple((ticker, "1m-iv") for ticker in DEEP_STUBS) + (
    ("FOXA", "1m-iv"),
    ("CMCSA", "1d"),
)
live = {}
for ticker, kind in LIVE_ROWS:
    verdict, error = bank_verdict(ticker, kind)
    live[(ticker, kind)] = verdict
    detail = f"verdict={verdict}" if error is None else f"unavailable={error}"
    print(f"[OBSERVE] real bank: {ticker} {kind}  ({detail})")

if ARGS.live_expectation != "observe":
    stub_expected = (
        "KIND_FORWARD_STALE"
        if ARGS.live_expectation == "pre-resume" else "CURRENT")
    for ticker in DEEP_STUBS:
        check(
            f"{ARGS.live_expectation}: {ticker} 1m-iv is {stub_expected}",
            live[(ticker, "1m-iv")] == stub_expected,
            f"observed={live[(ticker, '1m-iv')]}",
        )
    check(
        f"{ARGS.live_expectation}: FOXA 1m-iv remains CURRENT",
        live[("FOXA", "1m-iv")] == "CURRENT",
        f"observed={live[('FOXA', '1m-iv')]}",
    )
    check(
        f"{ARGS.live_expectation}: CMCSA 1d repair remains CURRENT",
        live[("CMCSA", "1d")] == "CURRENT",
        f"observed={live[('CMCSA', '1d')]}",
    )

# --- bank-wide sweep (informational, non-gating: bank may be mid-fetch) -------
stale = []
unevaluated = 0
tickers = 0
try:
    bank_children = sorted(BANK.iterdir())
except OSError as exc:
    bank_children = []
    print(f"\nbank-wide sweep unavailable: {type(exc).__name__}: {exc}")
for child in bank_children:
    if not child.is_dir() or child.name.startswith("_"):
        continue
    manifest_path = child / ss.MANIFEST_NAME
    if not manifest_path.exists():
        continue
    tickers += 1
    try:
        mf = json.loads(manifest_path.read_text(encoding="utf-8"))
        rows = classify_ticker(mf)
    except (OSError, ValueError, TypeError, AttributeError):
        continue
    for row in rows:
        if row["verdict"] == "KIND_FORWARD_STALE":
            stale.append((child.name, row["kind"], row["kind_last"],
                          row["lag_months"]))
        elif row["verdict"] == "UNEVALUATED":
            unevaluated += 1

print(f"\nbank-wide sweep: {tickers} tickers, "
      f"{len(stale)} KIND_FORWARD_STALE rows, {unevaluated} unevaluated")
for name, kind, last, lag in stale:
    print(f"  {name} {kind}: last={last} lag={lag}mo")

print(f"\nkind_staleness_reference: {CHECKS[0] - len(FAILURES)} passed, "
      f"{len(FAILURES)} failed")
if FAILURES:
    print("FAILURES:", FAILURES)
    sys.exit(1)
print("GATE PASS")
