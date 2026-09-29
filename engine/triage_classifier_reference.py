"""Triage classifier — REFERENCE + acceptance gate (post-WS5 triage layer).

Turns each raw anomaly flag from the WS5 health report / deep-seam scan into a
DETERMINISTIC, self-explaining verdict so a HUMAN (not an AI) can act on a short,
ranked worklist. There is NO judgment here — just the exact tests we already ran
by hand on FTNT / WBD / SPGI / ADP / NVDA / EXC, encoded once so the program can
reproduce them forever.

Given a boundary/seam flag (ticker, ex-date), it computes offline-checkable
features against the external reference and classifies into one of:

  REAL            stored/external stays FLAT across the date -> both sources adjust
                  -> a real corporate action (e.g. NVDA 10:1).   bucket=auto-benign
  BENIGN_ACTION   stepped at a NON-split factor (1.14, 1.40 ...) = a spinoff /
                  special-div the RAW external doesn't adjust (ADP CDK, EXC CEG);
                  stored current anchor is correct.             bucket=auto-benign
  PHANTOM         stepped at a UNIFORM integer split factor the external LACKS
                  (FTNT 4:1) -> stored is WRONG.                 bucket=needs-human
  IDENTITY_BASIS  stepped at a split-magnitude factor but the DEEP ratio is
                  NON-uniform / drifting = a predecessor splice, NOT a clean split
                  (WBD Discovery->WBD). Do NOT xN-correct.       bucket=needs-human
  LOCAL_SUSPECT   stored does NOT match external at CURRENT dates -> the live data
                  itself is off (not just history).             bucket=needs-human
  UNVERIFIABLE / SHORT   no external ref / too little overlap.   bucket=needs-human
  RESOLVED        an old identity boundary was intentionally removed by a verified
                  identity_truncation correction.                bucket=auto-benign

Each verdict carries {verdict, confidence, bucket, recommended_action, evidence}
-- the self-explaining shape the production engine must reproduce.

DECISION TREE (in order; first match wins):
  1. |stored/external at CURRENT dates - 1| > TOL_NOW      -> LOCAL_SUSPECT
  2. |step across the ex-date - 1|        <= TOL_FLAT      -> REAL
  3. step is near an integer split (>=2, within INT_TOL):
       deep-ratio CV <  CV_UNIFORM  -> PHANTOM        (uniform split the ext lacks)
       deep-ratio CV >= CV_UNIFORM  -> IDENTITY_BASIS (non-uniform = splice)
  4. otherwise (non-split-magnitude step)               -> BENIGN_ACTION

Why this separates our cases: a REAL split of ANY ratio shows in BOTH sources ->
FLAT -> caught at (2). Only a one-sided (phantom) or predecessor-splice step is
left; a split-magnitude integer step that is uniformly offset in the deep is a
clean phantom (FTNT), while a split-magnitude step whose deep ratio DRIFTS is a
messy identity/basis splice (WBD). Small non-integer steps (1.14/1.40) are
spinoff/special-div convention differences (benign).

Gate: correctly classify every case we already know the answer to.
Run:  python engine/triage_classifier_reference.py    (exit 0 = all correct)

OWNERSHIP: Codex maintains this reference contract and the production engine.
"""
import sys
import datetime as dt
import statistics as st
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss
import stock_validate as sv

TOL_NOW = 0.05        # |stored/ext - 1| at current dates within this = current data OK
TOL_FLAT = 0.05       # |step - 1| within this = FLAT = a real action both sources adjust
INT_TOL = 0.04        # |f - nearest split|/split within this = a split-magnitude factor
CV_UNIFORM = 0.08     # deep-ratio coefficient of variation below this = uniform (phantom)
WIN = 20              # trading days each side of the ex-date for the step
NOW_WIN = 20          # current-anchor window (recent trading days). Kept SHORT so a RECENT
                      # corporate action (e.g. FDX FedEx-Freight spinoff 2026-06-01) cannot drag
                      # the tail median into the pre-action basis and false-trip LOCAL_SUSPECT.
SPLIT_SET = [2, 3, 4, 5, 6, 8, 10, 12, 15, 20]


def _stored_1d(root, ticker):
    out = {}
    man = ss.load_manifest(Path(root) / ss.canonical_ticker(ticker)) or {}
    for ym in ss.manifest_months(man, "1d"):
        y, m = int(ym[:4]), int(ym[5:7])
        p = (ss.find_month_file(root, ticker, y, m, "1d")
             or ss.month_file_path(root, ticker, y, m, "1d"))
        try:
            b, _ = ss.read_month_file(p)
        except Exception:  # noqa: BLE001
            continue
        for x in b:
            out[str(x[0])[:10]] = x[4]
    return out


def _cv(vals):
    vals = [v for v in vals if v is not None]
    if len(vals) < 2:
        return 0.0
    m = st.mean(vals)
    return (st.pstdev(vals) / m) if m else 0.0


def _resolved_identity_truncation(root, ticker, boundary):
    if isinstance(boundary, str):
        boundary = dt.date.fromisoformat(boundary[:10])
    man = ss.load_manifest(Path(root) / ss.canonical_ticker(ticker)) or {}
    raw_months = (((man.get("intervals") or {}).get("1d") or {})
                  .get("months") or {})
    present = sorted(
        str(month)[:7] for month, entry in raw_months.items()
        if isinstance(entry, dict)
        and str(entry.get("status") or "present").lower() == "present")
    try:
        corrections = ss.identity_correction_records(
            man, "1d", ticker=ticker)
    except ss.StorageError:
        return None
    for note, cutover in corrections:
        if (boundary < cutover and present
                and present[0] >= cutover.isoformat()[:7]):
            return note
    return None


def classify_flag(root, ticker, boundary, ref=None):
    """Classify one boundary/seam flag. `boundary` = ex-date (date or 'YYYY-MM-DD')."""
    if isinstance(boundary, str):
        boundary = dt.date.fromisoformat(boundary)
    bi = boundary.isoformat()
    base = {"ticker": ticker, "boundary": bi}

    resolved = _resolved_identity_truncation(root, ticker, boundary)
    if resolved is not None:
        return {**base, "verdict": "RESOLVED", "confidence": "high",
                "bucket": "auto-benign",
                "recommended_action":
                    "boundary removed by verified identity truncation; no action",
                "evidence": {"correction_type": resolved.get("type"),
                             "cutover": resolved.get("cutover"),
                             "applied": resolved.get("applied"),
                             "snapshot": resolved.get("snapshot")}}

    try:
        ref = ref or sv.fetch_daily_reference(ticker, rng="Max")
    except Exception as e:  # noqa: BLE001
        return {**base, "verdict": "UNVERIFIABLE", "confidence": "low",
                "bucket": "needs-human", "recommended_action": "no external ref",
                "evidence": {"error": str(e)[:60]}}

    stored = _stored_1d(root, ticker)
    days = sorted(d for d in (set(stored) & set(ref)) if ref[d][3] and stored[d])
    if len(days) < 3 * WIN:
        return {**base, "verdict": "SHORT", "confidence": "low",
                "bucket": "needs-human", "recommended_action": "insufficient overlap",
                "evidence": {"overlap_days": len(days)}}

    ratio = {d: stored[d] / ref[d][3] for d in days}

    # (1) current-anchor: does stored match external at recent dates?
    r_now = st.median([ratio[d] for d in days[-NOW_WIN:]])

    # (2) step across the ex-date (median window each side)
    pre_days = [d for d in days if d < bi][-WIN:]
    post_days = [d for d in days if d >= bi][:WIN]
    pre = st.median([ratio[d] for d in pre_days]) if pre_days else None
    post = st.median([ratio[d] for d in post_days]) if post_days else None
    step = (post / pre) if (pre and post and pre) else 1.0

    # (3) deep-ratio uniformity: CV of per-year medians BEFORE the ex-date
    by_year = {}
    for d in days:
        if d < bi:
            by_year.setdefault(d[:4], []).append(ratio[d])
    deep_cv = _cv([st.median(v) for v in by_year.values()])

    f = step if step >= 1 else (1.0 / step if step else 1.0)
    near = min(SPLIT_SET, key=lambda k: abs(k - f))
    is_split_mag = abs(near - f) / near < INT_TOL

    ev = {"r_now": round(r_now, 4),
          "pre_ratio": round(pre, 4) if pre else None,
          "post_ratio": round(post, 4) if post else None,
          "step": round(step, 4), "abs_factor": round(f, 3),
          "near_split": near if is_split_mag else None,
          "deep_cv": round(deep_cv, 4), "deep_years": len(by_year)}

    if abs(r_now - 1.0) > TOL_NOW:
        v, c, act = ("LOCAL_SUSPECT", "high",
                     "stored disagrees with external at CURRENT dates — check the live "
                     "data, not just history")
    elif abs(step - 1.0) <= TOL_FLAT:
        v, c, act = ("REAL", "high",
                     "both sources adjust across the date — real corporate action, no defect")
    elif is_split_mag and deep_cv < CV_UNIFORM:
        v, c, act = ("PHANTOM", "high",
                     f"uniform x{near} split the external lacks — external-verify, then "
                     f"phantom-split correction (JOIN-GATE: never auto-apply)")
    elif is_split_mag:
        v, c, act = ("IDENTITY_BASIS", "high",
                     "split-magnitude step but NON-uniform deep ratio — predecessor splice; "
                     "identity + basis lineage review, NOT an xN correction")
    else:
        v, c, act = ("BENIGN_ACTION", "high",
                     "non-split step — spinoff/special-div the raw external doesn't adjust; "
                     "stored current anchor is correct")

    bucket = "needs-human" if v in ("PHANTOM", "IDENTITY_BASIS", "LOCAL_SUSPECT") else "auto-benign"
    return {**base, "verdict": v, "confidence": c, "bucket": bucket,
            "recommended_action": act, "evidence": ev}


if __name__ == "__main__":
    root = ss.storage_root(".")
    # every current-bank case we already know the answer to (the contract).
    # FTNT was a PHANTOM before the 2026-07-09 bank correction; synthetic
    # selftests keep PHANTOM detection covered after the real bank is fixed.
    cases = [
        ("FTNT", "2014-01-13", "REAL"),            # corrected IBKR-source phantom 4:1
        ("WBD",  "2021-03-30", "IDENTITY_BASIS"),  # Discovery->WBD splice, not a split
        ("ADP",  "2014-09-25", "BENIGN_ACTION"),   # real CDK spinoff (raw ext unadjusted)
        ("EXC",  "2022-02-01", "BENIGN_ACTION"),   # real CEG spinoff
        ("NVDA", "2024-06-10", "REAL"),            # real 10:1 split (both adjust -> flat)
        ("FDX",  "2026-05-18", "BENIGN_ACTION"),   # FedEx Freight spinoff (dist 2026-06-01) — recent
                                                   # action; regression vs the old NOW_WIN=60 false positive
    ]
    ok = True
    print("Triage classifier — known-case acceptance gate:\n")
    for tkr, bnd, expect in cases:
        if (expect == "IDENTITY_BASIS"
                and _resolved_identity_truncation(root, tkr, bnd) is not None):
            expect = "RESOLVED"
        r = classify_flag(root, tkr, bnd)
        got = r["verdict"]
        mark = "OK" if got == expect else f"MISCLASSIFIED (want {expect})"
        if got != expect:
            ok = False
        print(f"  {tkr:5} @ {bnd}: {got:14} [{mark}]")
        print(f"        bucket={r.get('bucket')}  | {r.get('recommended_action', '')[:72]}")
        print(f"        {r.get('evidence')}")
    print("\nGATE:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)
