"""Reference harness for WS8 — random LIVE minute-level spot probe.

Spec-by-example for the user's safety net (2026-07-10): randomly sample a
stored (ticker, day), fetch that day's 1m bars LIVE from IBKR, and compare
against the stored bars adjusted by the recorded basis ledger. This pins the
comparison semantics every implementation must reproduce.

Offline + deterministic: fixtures are embedded, no network, no bank read, no
write. Exit 0 = all checks pass.

WHY the comparison must be BASIS- and CORRECTION-AWARE (the raw idea's trap):
- After a real split, IBKR re-serves ALL history re-based. stored x recorded
  cumulative factor == live is the CORRECT state (CRWD: stored 690.5 vs live
  172.63, factor 0.25 recorded). A naive equality check would flag every
  properly-reconciled ticker.
- FTNT's deep history is DELIBERATELY different from IBKR's feed (phantom x4
  corrected; IBKR still serves the wrong deep basis). A probe day in a
  documented correction region MUST classify as DOCUMENTED_DIVERGENCE (pass),
  or a naive "mismatch -> refetch ticker" would re-import the fault and undo
  the repair. Same protection covers any future correction/truncation.

VERDICTS (exactly one per probed day; precedence top-down):
  NO_LIVE_DATA           live returned nothing -> reconcile vs gap/source-
                         absent records; never a refetch trigger by itself
  COVERAGE_MISMATCH      bar-count asymmetry beyond tolerance -> month-refetch
                         CANDIDATE (queued, human-confirmed)
  MATCH                  all shared minutes agree after the recorded basis
                         ledger (the expected steady state)
  DOCUMENTED_DIVERGENCE  uniform factor equals a documented correction's
                         expected divergence -> PASS (repair holding)
  BASIS_STEP             uniform clean factor NOT explained by ledger or
                         corrections -> new corporate action; route to the
                         basis-action flow (NOT a refetch)
  SCATTERED_MISMATCH     non-uniform diffs -> IBKR revision or local damage;
                         month-refetch CANDIDATE (queued, human-confirmed)
REMEDIES ARE QUEUED, NEVER AUTOMATIC; whole-ticker rebuild stays a
human-confirmed last resort (quarantine + rebuild pattern).
"""

import sys
from statistics import median

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass


PRICE_TOL = 0.005        # 0.5% per-bar close agreement
FACTOR_SPREAD_TOL = 0.02  # uniform-factor max deviation around the median
SCATTER_MIN_BARS = 3     # fewer off-bars than this on a matched day = MATCH
COVERAGE_RATIO_MIN = 0.5  # shared bars must cover >= half of each side

VERDICTS = ("NO_LIVE_DATA", "COVERAGE_MISMATCH", "MATCH",
            "DOCUMENTED_DIVERGENCE", "BASIS_STEP", "SCATTERED_MISMATCH")


def pick_probe(months, seed):
    """Deterministic (month, index) pick from a seed — reproducible probes."""
    if not months:
        raise ValueError("no stored months to probe")
    ordered = sorted(months)
    a = (seed * 1103515245 + 12345) & 0x7FFFFFFF
    month = ordered[a % len(ordered)]
    b = (a * 1103515245 + 12345) & 0x7FFFFFFF
    return month, b


def classify_day(stored, live, *, ledger_factor=1.0, correction_factor=None,
                 price_tol=PRICE_TOL, spread_tol=FACTOR_SPREAD_TOL):
    """Compare one stored day against one live-fetched day.

    stored/live: {minute_key: close}. ledger_factor: product of recorded
    basis-action factors so that stored * ledger_factor ~= live is the
    reconciled state. correction_factor: documented expected stored/live
    divergence for this date (e.g. FTNT deep = 4.0), or None.
    """
    if not live:
        return {"verdict": "NO_LIVE_DATA", "shared": 0}
    shared = sorted(set(stored) & set(live))
    out = {"stored_bars": len(stored), "live_bars": len(live),
           "shared": len(shared)}
    if (len(shared) < COVERAGE_RATIO_MIN * len(stored)
            or len(shared) < COVERAGE_RATIO_MIN * len(live)):
        out["verdict"] = "COVERAGE_MISMATCH"
        return out
    ratios = []
    for key in shared:
        sc, lc = stored[key], live[key]
        if sc and lc and sc > 0 and lc > 0:
            ratios.append(lc / sc)
    if not ratios:
        out["verdict"] = "COVERAGE_MISMATCH"
        return out
    med = median(ratios)
    spread = max(abs(r / med - 1) for r in ratios)
    out.update({"median_factor": med, "max_spread": spread})

    off_ledger = sum(1 for r in ratios
                     if abs(r / ledger_factor - 1) > price_tol)
    if off_ledger < SCATTER_MIN_BARS:
        out["verdict"] = "MATCH"
        return out
    if correction_factor:
        # stored = live * correction_factor -> live/stored = 1/correction
        expected = 1.0 / float(correction_factor)
        off_corr = sum(1 for r in ratios
                       if abs(r / expected - 1) > price_tol)
        if off_corr < SCATTER_MIN_BARS:
            out["verdict"] = "DOCUMENTED_DIVERGENCE"
            return out
    if spread <= spread_tol:
        out["verdict"] = "BASIS_STEP"
        return out
    out["verdict"] = "SCATTERED_MISMATCH"
    return out


# --- fixtures ---------------------------------------------------------------

def _day(base=100.0, n=390, factor=1.0, jitter=0):
    out = {}
    for i in range(n):
        px = (base + (i % 7) * 0.13) * factor
        if jitter and i % (n // jitter or 1) == 0:
            px *= 1.03
        out[f"09:{30 + i // 60:02d}:{i % 60:02d}"] = round(px, 4)
    return out


_passed = _failed = 0


def check(name, ok, detail=""):
    global _passed, _failed
    if ok:
        _passed += 1
        print(f"[PASS] {name}")
    else:
        _failed += 1
        print(f"[FAIL] {name}  {detail}")


def main():
    base = _day()

    r = classify_day(base, dict(base))
    check("identical day -> MATCH", r["verdict"] == "MATCH", r["verdict"])

    live = {k: v * 0.25 for k, v in base.items()}
    r = classify_day(base, live, ledger_factor=0.25)
    check("CRWD shape: recorded 4:1 ledger factor 0.25 -> MATCH",
          r["verdict"] == "MATCH", r["verdict"])

    r = classify_day(base, live, ledger_factor=1.0)
    check("same data WITHOUT ledger -> BASIS_STEP (new action, not refetch)",
          r["verdict"] == "BASIS_STEP", r["verdict"])

    live = {k: v / 4.0 for k, v in base.items()}
    r = classify_day(base, live, ledger_factor=1.0, correction_factor=4.0)
    check("FTNT shape: stored = live x4 documented -> DOCUMENTED_DIVERGENCE",
          r["verdict"] == "DOCUMENTED_DIVERGENCE", r["verdict"])

    live = dict(base)
    keys = sorted(live)
    for k in keys[::30]:      # 13 bars revised non-uniformly
        live[k] = live[k] * 1.04
    r = classify_day(base, live)
    check("13 revised bars -> SCATTERED_MISMATCH (month-refetch candidate)",
          r["verdict"] == "SCATTERED_MISMATCH", r["verdict"])

    live = dict(base)
    live[keys[5]] *= 1.02     # single revised bar stays MATCH
    r = classify_day(base, live)
    check("1 revised bar under scatter floor -> MATCH",
          r["verdict"] == "MATCH", r["verdict"])

    half = {k: base[k] for k in keys[:150]}
    r = classify_day(base, half)
    check("live serves 150/390 bars -> COVERAGE_MISMATCH",
          r["verdict"] == "COVERAGE_MISMATCH", r["verdict"])

    r = classify_day(base, {})
    check("empty live day -> NO_LIVE_DATA", r["verdict"] == "NO_LIVE_DATA")

    m1, s1 = pick_probe(["2015-03", "2019-11", "2023-06"], seed=42)
    m2, s2 = pick_probe(["2015-03", "2019-11", "2023-06"], seed=42)
    m3, _s3 = pick_probe(["2015-03", "2019-11", "2023-06"], seed=43)
    check("probe pick is seed-deterministic", (m1, s1) == (m2, s2),
          f"{(m1, s1)} vs {(m2, s2)}")
    check("different seed still yields a valid pick", isinstance(m3, str))

    print(f"\nlive_spot_probe_reference: {_passed} passed, {_failed} failed")
    return 1 if _failed else 0


if __name__ == "__main__":
    sys.exit(main())
