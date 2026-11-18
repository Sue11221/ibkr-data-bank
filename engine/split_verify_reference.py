"""External split verification — REFERENCE + acceptance gate (WS6).

The FTNT lesson generalized: a stored split/boundary must PROVE itself against
outside data before we trust it. The discriminator is a STEP in the
stored / external-reference ratio across the boundary:

  * REAL split  -> BOTH sources adjust for it, so stored/external stays FLAT
                   across the ex-date (ratio step ~= 1.0)  -> quiet flag, OK.
  * PHANTOM split (FTNT) -> only the STORED series jumps; the external source
                   is continuous, so stored/external STEPS at the ex-date
                   (ratio step != 1.0)  -> BIG WARNING, do NOT trust/auto-apply.
  * Un-fetchable / no overlap -> cannot verify -> BIG WARNING (fail closed).

This is what turns "we happened to catch FTNT" into a standing guard: run it on
every detected split boundary; step => warn + block auto-adjust + human review.

Gate (re-expected 2026-07-27, Row 61): classify BOTH FTNT@2014-01-13 and
NVDA@2024-06-10 (a real 10:1 split) as REAL.  FTNT's stored phantom was
corrected by phantom_fix.py on 2026-07-09, so its boundary is now genuinely
flat; this gate FAILING on FTNT would mean the phantom has returned.  (The
original expectation of PHANTOM inverted silently after the correction — the
2026-07-24 fleet audit caught it.)  Run:  python engine/split_verify_reference.py
"""
import sys, datetime as dt, statistics as st
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss
import stock_validate as sv

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

STEP_TOL = 0.05          # |ratio step - 1| within 5% = FLAT = real; else a step
WIN = 20                 # trading days sampled each side of the boundary


def _stored_closes(root, ticker, d0, d1):
    out = {}
    y = d0.year
    while y <= d1.year:
        for m in range(1, 13):
            p = (ss.find_month_file(root, ticker, y, m, "1d")
                 or ss.month_file_path(root, ticker, y, m, "1d"))
            try:
                b, _ = ss.read_month_file(p)
            except Exception:  # noqa: BLE001
                continue
            for x in b:
                iso = str(x[0])[:10]
                if d0.isoformat() <= iso <= d1.isoformat():
                    out[iso] = x[4]
        y += 1
    return out


def classify_boundary(root, ticker, boundary):
    """Return dict: side ratios, step, verdict REAL|PHANTOM|UNVERIFIABLE."""
    lo = (boundary - dt.timedelta(days=90))
    hi = (boundary + dt.timedelta(days=90))
    try:
        ref = sv.fetch_daily_reference(ticker, rng="Max")
    except Exception as e:  # noqa: BLE001
        return {"verdict": "UNVERIFIABLE", "why": f"no external ref ({e})"}
    stored = _stored_closes(root, ticker, lo, hi)
    def side(before):
        rs = []
        for iso, c in sorted(stored.items()):
            d = dt.date.fromisoformat(iso)
            if (d < boundary) == before and iso in ref and ref[iso][3]:
                rs.append(c / ref[iso][3])
        rs = rs[-WIN:] if before else rs[:WIN]
        return st.median(rs) if rs else None
    pre, post = side(True), side(False)
    if pre is None or post is None:
        return {"verdict": "UNVERIFIABLE", "why": "no overlap with external",
                "pre": pre, "post": post}
    step = post / pre if pre else 0
    verdict = "REAL" if abs(step - 1.0) <= STEP_TOL else "PHANTOM"
    return {"verdict": verdict, "pre_ratio": pre, "post_ratio": post,
            "step": step}


if __name__ == "__main__":
    root = ss.storage_root(".")
    # FTNT expectation FLIPPED 2026-07-27 (Row 61): phantom_fix.py corrected the
    # stored phantom on 2026-07-09 (pre-2014-01-13 prices restored x4), so BOTH
    # sources now track flat across the boundary and the healthy verdict is
    # REAL.  A PHANTOM verdict here would mean the phantom has RETURNED - that
    # is the regression this gate now guards against.  (Before the correction
    # this case expected PHANTOM; the audit of 2026-07-24 caught the inversion.)
    cases = [("FTNT", dt.date(2014, 1, 13), "REAL"),
             ("NVDA", dt.date(2024, 6, 10), "REAL")]
    ok = True
    for tkr, bnd, expect in cases:
        r = classify_boundary(root, tkr, bnd)
        got = r["verdict"]
        mark = "OK" if got == expect else "MISCLASSIFIED"
        if got != expect:
            ok = False
        extra = (f"stored/ext {r.get('pre_ratio',0):.3f} -> {r.get('post_ratio',0):.3f}"
                 f" (step x{r.get('step',0):.3f})") if "step" in r else r.get("why", "")
        flag = "🛑 BIG WARNING" if got in ("PHANTOM", "UNVERIFIABLE") else "⚑ ok flag"
        print(f"  {tkr} @ {bnd}: {got:12} [{mark}]  {flag}   {extra}")
    print("\nGATE:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)
