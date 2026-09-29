"""Phantom-split AUTO-FIX — REFERENCE + dry-run proof (the correction engine's contract).

A PHANTOM split (FTNT class, per triage_classifier_reference.py) is the ONE anomaly
class with a clean, reversible fix: the stored deep segment was uniformly divided by a
split that NEVER happened, so multiplying it back is correct and lossless.

This module PLANS and PROVES that correction OFFLINE — it reads stored bars, computes
the corrected values IN MEMORY, and checks them against the external truth. IT WRITES
NOTHING. It is the contract + pre-write safety proof for the production engine Codex
builds (which does the actual snapshot + bank write + manifest note + re-verify).

Correction recipe (for a confirmed phantom of integer factor N, ex-date D):
    bars with date < D, in 1m + 1d ONLY:   O/H/L/C *= N ;   volume /= N
    (1d-hvol is scale-invariant — untouched.)

SAFETY the production engine MUST honor (enforced by Claude review):
  * FIRE ONLY on triage verdict == PHANTOM (uniform integer factor, externally
    verified). NEVER on BENIGN_ACTION (correct data) or IDENTITY_BASIS (a splice —
    a wrong xN corrupts it further). The classifier is the gate.
  * SNAPSHOT the affected months first (reversible) and write a durable
    manifest["data_corrections"] note {type, ex_date, factor, verified_vs, applied}
    so a future re-fetch that re-applies the phantom is re-detected + re-corrected,
    never silently re-broken.
  * Gate: this dry-run exits 0 BEFORE the write (recipe proven); ftnt_seam_reference.py
    exits 0 AFTER the write (bank actually corrected).

Gate here: applying the recipe to FTNT in memory makes the deep segment match the
external anchors AND the ex-date boundary go continuous.
Run:  python engine/phantom_fix_reference.py    (exit 0 = recipe proven correct)

OWNERSHIP:
  [CLAUDE] this reference + proof define the recipe + "done" (the contract).
  [CODEX]  writes the production engine/phantom_fix.py (snapshot + bank write +
           manifest note + re-verify) and applies it to FTNT. Claude reviews the
           diff + the before/after.
"""
import sys
import datetime as dt
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss

# --- confirmed FTNT phantom (proven vs stockanalysis over 640 days; see log) ---
EX_DATE = dt.date(2014, 1, 13)             # first CORRECT day (phantom-split ex-date)
FACTOR = 4                                 # uniform x4 the external does not carry
# external (stockanalysis, split-adjusted) closes = the CORRECT post-fix values
DEEP_ANCHORS = {"2012-06-01": 4.08, "2013-06-03": 3.70, "2013-11-01": 3.95}
PRICE_TOL = 0.03                           # 3% cross-source close tolerance
BOUNDARY_TOL = 0.15                        # post-fix continuity: |ratio-1| < 15%


def _close_map(root, ticker, y0, y1, iv):
    out = {}
    for y in range(y0, y1 + 1):
        for m in range(1, 13):
            p = (ss.find_month_file(root, ticker, y, m, iv)
                 or ss.month_file_path(root, ticker, y, m, iv))
            try:
                b, _ = ss.read_month_file(p)
            except Exception:  # noqa: BLE001
                continue
            for x in b:
                out[str(x[0])[:10]] = x[4]
    return out


def plan_correction(root, ticker, ex_date, intervals=("1d", "1m")):
    """List (interval, 'YYYY-MM', n_bars) that WOULD be corrected. Reads only, no writes."""
    plan = []
    exi = ex_date.isoformat()
    man = ss.load_manifest(Path(root) / ss.canonical_ticker(ticker)) or {}
    for iv in intervals:
        for ym in ss.manifest_months(man, iv):
            y, m = int(ym[:4]), int(ym[5:7])
            p = (ss.find_month_file(root, ticker, y, m, iv)
                 or ss.month_file_path(root, ticker, y, m, iv))
            try:
                b, _ = ss.read_month_file(p)
            except Exception:  # noqa: BLE001
                continue
            n = sum(1 for x in b if str(x[0])[:10] < exi)
            if n:
                plan.append((iv, ym, n))
    return plan


def _boundary_ratio(cm, ex_date, factor=1):
    exi = ex_date.isoformat()
    pre = [d for d in cm if d < exi]
    post = [d for d in cm if d >= exi]
    if not (pre and post):
        return None, None, None
    last_pre, first_post = max(pre), min(post)
    corr_pre = cm[last_pre] * factor
    ratio = (cm[first_post] / corr_pre) if corr_pre else 0
    return last_pre, first_post, ratio


def verify_current(root, ticker="FTNT", ex_date=EX_DATE):
    """Check whether the bank is already corrected; [] means post-fix PASS."""
    cm = _close_map(root, ticker, 2011, 2015, "1d")
    fails = []
    for d, want in DEEP_ANCHORS.items():
        got = cm.get(d)
        if got is None:
            fails.append(f"missing deep day {d}")
            continue
        if abs(got / want - 1.0) > PRICE_TOL:
            fails.append(f"{d}: stored {got:.3f} vs correct ~{want:.2f} "
                         f"(off {got / want:.3f}x)")
    last_pre, first_post, ratio = _boundary_ratio(cm, ex_date)
    if ratio is not None and abs(ratio - 1.0) > BOUNDARY_TOL:
        fails.append(f"boundary {last_pre}->{first_post} close ratio "
                     f"{ratio:.3f} (want ~1.0)")
    return fails


def verify_recipe(root, ticker="FTNT", ex_date=EX_DATE, factor=FACTOR):
    """Apply the recipe in memory; return a list of failures ([] = correct)."""
    cm = _close_map(root, ticker, 2011, 2015, "1d")
    fails = []
    # 1) corrected deep closes must match the external (correct) closes
    for d, want in DEEP_ANCHORS.items():
        got = cm.get(d)
        if got is None:
            fails.append(f"missing deep day {d}")
            continue
        corr = got * factor
        if abs(corr / want - 1.0) > PRICE_TOL:
            fails.append(f"{d}: stored {got:.3f} x{factor} = {corr:.2f} vs correct ~{want:.2f} "
                         f"(off {corr / want:.3f}x)")
    # 2) boundary continuity AFTER correction (pre x factor should meet unchanged post)
    last_pre, first_post, ratio = _boundary_ratio(cm, ex_date, factor)
    if ratio is not None:
        if abs(ratio - 1.0) > BOUNDARY_TOL:
            fails.append(f"boundary {last_pre}(x{factor})->{first_post} close ratio "
                         f"{ratio:.3f} (want ~1.0)")
    return fails


def _prewrite_only_main():
    root = ss.storage_root(".")
    plan = plan_correction(root, "FTNT", EX_DATE)
    by_iv = {}
    for iv, _ym, n in plan:
        by_iv[iv] = by_iv.get(iv, 0) + n
    total = sum(by_iv.values())
    print(f"DRY RUN — FTNT phantom x{FACTOR} correction (ex-date {EX_DATE}); NO writes:")
    print(f"  would correct {total} bars across {len(plan)} month-files: "
          + ", ".join(f"{iv}={c}" for iv, c in sorted(by_iv.items())))
    print(f"  recipe: date < {EX_DATE}  ->  O/H/L/C *= {FACTOR},  volume /= {FACTOR}  "
          f"(1d-hvol untouched)")

    fails = verify_recipe(root)
    if fails:
        print("\nPROOF: FAIL — corrected values do NOT match the external truth:")
        for f in fails:
            print(f"  - {f}")
        print("\nGATE: FAIL")
        sys.exit(1)
    print(f"\nPROOF: applying x{FACTOR} in memory makes the deep segment match the external "
          f"anchors\n       and the ex-date boundary continuous — the recipe is correct.")
    print("GATE: PASS")
    sys.exit(0)

def main():
    root = ss.storage_root(".")
    plan = plan_correction(root, "FTNT", EX_DATE)
    by_iv = {}
    for iv, _ym, n in plan:
        by_iv[iv] = by_iv.get(iv, 0) + n
    total = sum(by_iv.values())
    print(f"READ-ONLY - FTNT phantom x{FACTOR} correction reference "
          f"(ex-date {EX_DATE}); NO writes:")
    print(f"  candidate pre-date rows: {total} bars across {len(plan)} month-files: "
          + ", ".join(f"{iv}={c}" for iv, c in sorted(by_iv.items())))
    print(f"  recipe: date < {EX_DATE} -> O/H/L/C *= {FACTOR}, volume /= {FACTOR} "
          "(1d-hvol untouched)")

    current_fails = verify_current(root)
    if not current_fails:
        print("\nPOST-FIX PROOF: stored deep values already match the external anchors")
        print("       and the ex-date boundary is continuous. Do NOT apply the x4 recipe again.")
        print("GATE: PASS")
        return 0

    recipe_fails = verify_recipe(root)
    if not recipe_fails:
        print(f"\nPRE-FIX PROOF: applying x{FACTOR} in memory makes the deep segment")
        print("       match external anchors and makes the ex-date boundary continuous.")
        print("GATE: PASS")
        return 0

    print("\nPROOF: FAIL - neither current values nor corrected values match the external truth:")
    print("  current-state failures:")
    for fail in current_fails:
        print(f"  - {fail}")
    print("  recipe failures:")
    for fail in recipe_fails:
        print(f"  - {fail}")
    print("\nGATE: FAIL")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
