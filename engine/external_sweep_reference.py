"""Reference harness for WS7 — bank-wide FULL-RANGE external sweep.

Spec-by-example for the classifier at the heart of WS7 (see
DATA_INTEGRITY_HARDENING.md): compare each ticker's WHOLE stored daily close
history against a full-range external reference, month by month, and route
every ticker to exactly one verdict. This harness pins the verdict semantics
and the known real-bank signatures every implementation must reproduce.

Offline + deterministic: fixtures are embedded, the fetcher is injectable,
there is no network call, no bank read, and no write of any kind. Exit 0 =
all checks pass.

Method validated LIVE 2026-07-10 on 8 real tickers (AAPL NVDA TSLA AMZN KO
ADP MSFT JNJ, full 2011->2026 range, scratchpad probe):
- close ratio stored/external = 1.0000 across the full range everywhere,
  EXCEPT ADP's constant 0.8773 deep offset ending 2014-09 — its real CDK
  spinoff (IBKR spinoff-adjusts, the reference does not). That shape is the
  HISTORIC_BASIS_OFFSET class: benign, but recorded with factor + boundary.
- FTNT's pre-correction shape (0.25 deep -> 1.0 forward) and WBD's
  pre-truncation shape (drifting 1.05->2.17 deep) are the two FAULT shapes
  this sweep exists to catch; both are pinned as fixtures below.
- volume ratio drifts smoothly ~0.9 (2011) -> ~0.5 (2026) on ALL tickers
  alike (IBKR venue share vs consolidated tape) with no convention break:
  volume is INFORMATIONAL ONLY and never a verdict input.

VERDICTS (exactly one per ticker):
  CLEAN                 full-range ratio ~= 1.0, no steps
  HISTORIC_BASIS_OFFSET current segment ~= 1.0; older segment(s) constant at
                        a non-split-like factor (spinoff/convention class)
  SEAM_CANDIDATE        interior step to/from a split-like factor (>= 1.25x
                        as ratio or inverse) — route to split-detector triage
  DRIFT_ANOMALY         a long segment whose ratio DRIFTS (WBD class)
  CURRENT_MISMATCH      the CURRENT segment is off 1.0 (wrong scale/symbol —
                        most severe; immediate review)
  UNVERIFIABLE          not enough overlap to say anything (fail closed)
Report-only: the sweep NEVER writes, corrects, or records basis actions.
"""

import math
import sys
from statistics import median

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass


# --- tunables (the reference values; implementations may expose knobs) ------

CLEAN_TOL = 0.02          # |log ratio| within 2% of 1.0 counts as "at 1.0"
STEP_TOL = 0.05           # >5% month-over-month median jump = a step
DRIFT_CV = 0.05           # log-ratio stdev/|mean| above this in a segment
DRIFT_MIN_MONTHS = 6      # ...only judged on segments at least this long
CURRENT_MONTHS = 6        # the "current" anchor = median of the last N months
MIN_MONTHS = 12           # fewer overlapping months -> UNVERIFIABLE
SPLIT_MIN_MAGNITUDE = 1.25   # smallest real split ratio (5:4)
SPLIT_MAX_DENOMINATOR = 10
SPLIT_RATIO_TOL = 0.03

VERDICTS = ("CLEAN", "HISTORIC_BASIS_OFFSET", "SEAM_CANDIDATE",
            "DRIFT_ANOMALY", "CURRENT_MISMATCH", "UNVERIFIABLE")


# --- core classifier ---------------------------------------------------------

def monthly_medians(stored, external):
    """{date_iso: close} x2 -> sorted [(month, median stored/external, n)].
    Only dates present on BOTH sides count; zero/negative closes are skipped."""
    per_month = {}
    for day, sc in stored.items():
        ec = external.get(day)
        if not ec or not sc or ec <= 0 or sc <= 0:
            continue
        per_month.setdefault(day[:7], []).append(sc / ec)
    return [(m, median(v), len(v)) for m, v in sorted(per_month.items())]


def detect_steps(months, step_tol=STEP_TOL):
    """Consecutive-month median jumps beyond step_tol (log scale)."""
    steps = []
    for i in range(1, len(months)):
        prev, cur = months[i - 1][1], months[i][1]
        if abs(math.log(cur / prev)) > math.log(1 + step_tol):
            steps.append({"month": months[i][0], "from": prev, "to": cur})
    return steps


def segment(months, steps):
    """Split the month list into constant-basis runs at each step month."""
    cut_months = {s["month"] for s in steps}
    segments, run = [], []
    for row in months:
        if row[0] in cut_months and run:
            segments.append(run)
            run = []
        run.append(row)
    if run:
        segments.append(run)
    out = []
    for run in segments:
        logs = [math.log(r[1]) for r in run]
        mean = sum(logs) / len(logs)
        var = sum((x - mean) ** 2 for x in logs) / len(logs)
        out.append({
            "start": run[0][0], "end": run[-1][0], "months": len(run),
            "ratio": math.exp(mean), "log_sd": math.sqrt(var),
        })
    return out


def split_like(factor, *, max_den=SPLIT_MAX_DENOMINATOR, tol=SPLIT_RATIO_TOL,
               min_magnitude=SPLIT_MIN_MAGNITUDE):
    """True when a ratio looks like a real split factor: at least 5:4 in
    magnitude AND within tol of a small integer ratio n:m."""
    if factor <= 0:
        return False
    g = factor if factor >= 1 else 1.0 / factor
    if g < min_magnitude:
        return False
    for den in range(1, max_den + 1):
        num = round(g * den)
        if num <= den or num > max_den * 4:
            continue
        if abs(g / (num / den) - 1) <= tol:
            return True
    return False


def classify(months, *, clean_tol=CLEAN_TOL, step_tol=STEP_TOL,
             drift_cv=DRIFT_CV, current_months=CURRENT_MONTHS,
             min_months=MIN_MONTHS):
    """[(month, ratio, n)] -> {verdict, steps, segments, current_ratio,...}.
    Exactly one verdict; precedence: UNVERIFIABLE > CURRENT_MISMATCH >
    DRIFT_ANOMALY > SEAM_CANDIDATE > HISTORIC_BASIS_OFFSET > CLEAN."""
    if len(months) < min_months:
        return {"verdict": "UNVERIFIABLE", "months": len(months),
                "steps": [], "segments": []}
    steps = detect_steps(months, step_tol)
    segments = segment(months, steps)
    current = median(r for _m, r, _n in months[-current_months:])
    out = {"verdict": None, "months": len(months), "steps": steps,
           "segments": segments, "current_ratio": current}
    if abs(math.log(current)) > math.log(1 + clean_tol):
        out["verdict"] = "CURRENT_MISMATCH"
        return out
    for seg in segments:
        if (seg["months"] >= DRIFT_MIN_MONTHS
                and seg["log_sd"] > drift_cv):
            out["verdict"] = "DRIFT_ANOMALY"
            out["drift_segment"] = seg
            return out
    off = [seg for seg in segments
           if abs(math.log(seg["ratio"])) > math.log(1 + clean_tol)]
    if not off:
        out["verdict"] = "CLEAN"
        return out
    factors = [seg["ratio"] / current for seg in off]
    if any(split_like(f) for f in factors):
        out["verdict"] = "SEAM_CANDIDATE"
    else:
        out["verdict"] = "HISTORIC_BASIS_OFFSET"
    out["offset_segments"] = off
    return out


def sweep_ticker(ticker, stored, fetcher):
    """One-ticker pipeline shape: injectable fetcher(ticker) -> {date: close}.
    Fail-closed: a fetcher error is UNVERIFIABLE, never a guess."""
    try:
        external = fetcher(ticker)
    except Exception as exc:  # noqa: BLE001 - fail closed, record why
        return {"ticker": ticker, "verdict": "UNVERIFIABLE",
                "error": f"{type(exc).__name__}", "months": 0,
                "steps": [], "segments": []}
    result = classify(monthly_medians(stored, external))
    result["ticker"] = ticker
    return result


# --- fixtures: the shapes this sweep exists to tell apart -------------------

def _months(spec):
    """[(count, ratio_fn)] -> [(month, ratio, n)] with deterministic months."""
    out, y, m = [], 2011, 1
    idx = 0
    for count, fn in spec:
        for i in range(count):
            out.append((f"{y:04d}-{m:02d}", fn(i, idx), 21))
            idx += 1
            m += 1
            if m > 12:
                m, y = 1, y + 1
    return out


def _noise(i, _idx):
    return 1.0 + (0.003 if i % 2 else -0.003)


FIXTURES = {
    # flat 1.0 +- 0.3% for 5 years -> CLEAN
    "clean": (_months([(60, _noise)]), "CLEAN"),
    # ADP/CDK shape: constant 0.8773 for 38 months, then 1.0
    "spinoff_offset": (_months([(38, lambda i, x: 0.8773),
                                (22, _noise)]), "HISTORIC_BASIS_OFFSET"),
    # FTNT shape: deep quarter-basis then 1.0 (phantom 4:1 signature)
    "phantom_step": (_months([(30, lambda i, x: 0.25),
                              (30, _noise)]), "SEAM_CANDIDATE"),
    # WBD shape: deep ratio drifting 1.05 -> 2.17, then a clean current run
    "drifting_deep": (_months([(24, lambda i, x: 1.05 + (2.17 - 1.05) * i / 23),
                               (36, _noise)]), "DRIFT_ANOMALY"),
    # wrong scale/symbol the whole way -> most severe
    "current_mismatch": (_months([(60, lambda i, x: 0.5)]), "CURRENT_MISMATCH"),
    # too little overlap to judge
    "too_short": (_months([(8, _noise)]), "UNVERIFIABLE"),
}


# --- checks ------------------------------------------------------------------

_passed = 0
_failed = 0


def check(name, ok, detail=""):
    global _passed, _failed
    if ok:
        _passed += 1
        print(f"[PASS] {name}")
    else:
        _failed += 1
        print(f"[FAIL] {name}  {detail}")


def main():
    for name, (months, expected) in FIXTURES.items():
        got = classify(months)
        check(f"fixture {name} -> {expected}",
              got["verdict"] == expected, f"got {got['verdict']}")

    got = classify(FIXTURES["spinoff_offset"][0])
    check("spinoff offset records factor ~0.8773",
          abs(got["offset_segments"][0]["ratio"] / got["current_ratio"]
              - 0.8773) < 0.01, str(got.get("offset_segments")))
    check("spinoff 0.8773 is NOT split-like (magnitude floor)",
          not split_like(0.8773))
    check("phantom 0.25 IS split-like (4:1)", split_like(0.25))
    check("2.0 / 0.5 / 1.5 are split-like",
          all(split_like(f) for f in (2.0, 0.5, 1.5)))
    check("1.05 and 0.96 are not split-like",
          not any(split_like(f) for f in (1.05, 0.96)))

    got = classify(FIXTURES["drifting_deep"][0])
    check("drift verdict pins the drifting segment, not the clean run",
          got.get("drift_segment", {}).get("start") == "2011-01")

    smooth = _months([(60, lambda i, x: 0.9 - 0.4 * x / 59)])
    check("smooth venue-style drift produces no STEP",
          not detect_steps(smooth))

    stored = {f"2020-{m:02d}-15": 10.0 for m in range(1, 13)}
    stored.update({f"2021-{m:02d}-15": 10.0 for m in range(1, 13)})
    external = dict(stored)
    result = sweep_ticker("TEST", stored, lambda t: external)
    check("injectable end-to-end pipeline -> CLEAN",
          result["verdict"] == "CLEAN", result["verdict"])
    result = sweep_ticker("TEST", stored,
                          lambda t: (_ for _ in ()).throw(OSError("down")))
    check("fetcher failure fails CLOSED as UNVERIFIABLE",
          result["verdict"] == "UNVERIFIABLE" and result["error"] == "OSError")

    partial = {d: c for i, (d, c) in enumerate(sorted(stored.items()))
               if i % 2 == 0}
    mm = monthly_medians(stored, partial)
    check("join uses only shared dates",
          all(n == 1 for _m, _r, n in mm) and len(mm) == 12)

    print(f"\nexternal_sweep_reference: {_passed} passed, {_failed} failed")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
