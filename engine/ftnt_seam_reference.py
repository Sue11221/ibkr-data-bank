"""FTNT phantom-split correction — REFERENCE + acceptance gate.

Defect: IBKR's demo feed carries a PHANTOM 4:1 split for FTNT with ex-date
2014-01-13 that never happened. Its continuous-adjusted series therefore divides
every pre-2014-01-13 price by an extra 4 (on top of the real 5:1 of 2022-06), so
the deep segment (2011-06 .. 2014-01-10) sits at exactly 1/4 of the true value.
Verified externally over 640 days: stored/stockanalysis = 0.25003 (std .0007)
for the deep segment vs 1.00004 for 2014-01-13 -> now.

FIX (Codex TODO): for FTNT bars with date < 2014-01-13, in 1m and 1d ONLY,
multiply O/H/L/C by 4 and divide volume by 4 (undo the phantom split). Leave
1d-hvol alone (volatility is scale-invariant). Record a durable manifest note so
a future re-fetch that re-applies the phantom split is caught + re-corrected.

This gate is OFFLINE (baked-in external anchors measured from stockanalysis):
  exit 0 once the deep segment is corrected, exit 1 while the seam is present.
Run:  python engine/ftnt_seam_reference.py
"""
import sys, datetime as dt
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass
sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss

EX_DATE = dt.date(2014, 1, 13)            # phantom-split ex-date (first CORRECT day)
# external (stockanalysis, split-adjusted) closes = the CORRECT post-fix values
DEEP_ANCHORS = {"2012-06-01": 4.08, "2013-06-03": 3.70, "2013-11-01": 3.95}
PRICE_TOL = 0.03                          # 3% — cross-source close tolerance
BOUNDARY_TOL = 0.15                       # post-fix continuity: |ratio-1| < 15%


def _close_map(root, y0, y1, iv):
    out = {}
    for y in range(y0, y1 + 1):
        for m in range(1, 13):
            p = (ss.find_month_file(root, "FTNT", y, m, iv)
                 or ss.month_file_path(root, "FTNT", y, m, iv))
            try:
                b, _ = ss.read_month_file(p)
            except Exception:  # noqa: BLE001
                continue
            for x in b:
                out[str(x[0])[:10]] = x[4]
    return out


def check(root):
    cm = _close_map(root, 2011, 2015, "1d")
    fails = []
    # 1) deep anchors must match the external (correct) closes
    for d, want in DEEP_ANCHORS.items():
        got = cm.get(d)
        if got is None:
            fails.append(f"missing deep day {d}")
        elif abs(got / want - 1.0) > PRICE_TOL:
            fails.append(f"{d}: stored {got:.2f} vs correct ~{want:.2f} "
                         f"(off {got/want:.3f}x — deep still on phantom basis)")
    # 2) boundary continuity across the ex-date
    pre = [d for d in cm if d < EX_DATE.isoformat()]
    post = [d for d in cm if d >= EX_DATE.isoformat()]
    if pre and post:
        last_pre, first_post = max(pre), min(post)
        ratio = cm[first_post] / cm[last_pre] if cm[last_pre] else 0
        if abs(ratio - 1.0) > BOUNDARY_TOL:
            fails.append(f"boundary {last_pre}->{first_post} close ratio "
                         f"{ratio:.3f} (want ~1.0; ~4.0 = phantom seam present)")
    return fails


if __name__ == "__main__":
    fails = check(ss.storage_root("."))
    if fails:
        print("FTNT seam gate: FAIL (phantom 4:1 seam still present / uncorrected)")
        for f in fails:
            print(f"  - {f}")
        print("\nGATE: FAIL")
        sys.exit(1)
    print("FTNT seam gate: deep segment matches the external reference; boundary "
          "continuous.\nGATE: PASS")
    sys.exit(0)
