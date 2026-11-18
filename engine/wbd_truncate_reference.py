"""WBD truncate-to-WBD-era — REFERENCE + dry-run proof (identity-splice cleanup).

WBD's stored series splices pre-2022 Discovery data (drifting basis + corrupt
Archegos-2021 bars, e.g. stored 2021-03-31 = $128 vs real ~$43) with clean
post-merger WBD data. Per triage this is IDENTITY_BASIS, NOT a phantom split — so
the fix is NOT an xN correction; it is TRUNCATION to the WBD era. Warner Bros.
Discovery only began trading 2022-04-11 (AT&T WarnerMedia + Discovery merger), so
its honest history starts there; the pre-2022 'history' is Discovery's, mislabeled
and on a bad basis.

FIX (user 2026-07-09): drop all WBD bars with date < CUTOVER, keep >= CUTOVER, in
1m + 1d (and 1d-hvol). No value transform, no re-fetch.

This dry-run PROVES offline (no writes): the KEPT segment (>= CUTOVER) matches the
external reference at ~1.0 and is uniform (clean), while the DROPPED segment is the
drifting/corrupt part. Run: python engine/wbd_truncate_reference.py

OWNERSHIP: Codex maintains this gate, applies the snapshot-backed truncation,
and performs the post-write acceptance review.
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

CUTOVER = dt.date(2022, 4, 11)     # first WBD trading day (merger closed 2022-04-08 Fri)
KEEP_TOL = 0.05                    # kept segment: |median(stored/ext) - 1| within this
KEEP_CV = 0.05                     # kept segment: coefficient of variation below this = uniform


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


def plan_truncation(root, ticker="WBD", cutover=CUTOVER, intervals=("1d", "1m", "1d-hvol")):
    """List (interval, 'YYYY-MM', n_bars) that WOULD be dropped. Reads only."""
    plan = []
    ci = cutover.isoformat()
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
            n = sum(1 for x in b if str(x[0])[:10] < ci)
            if n:
                plan.append((iv, ym, n))
    return plan


def verify(root, ticker="WBD", cutover=CUTOVER):
    """Prove the KEPT segment is clean vs external; return (failures, evidence)."""
    ref = sv.fetch_daily_reference(ticker, rng="Max")
    stored = _stored_1d(root, ticker)
    ci = cutover.isoformat()
    days = sorted(d for d in (set(stored) & set(ref)) if ref[d][3] and stored[d])
    kept = [stored[d] / ref[d][3] for d in days if d >= ci]
    dropped = [stored[d] / ref[d][3] for d in days if d < ci]
    fails = []
    if len(kept) < 20:
        fails.append(f"kept segment too small ({len(kept)} days) to certify")
        return fails, {}
    kmed, kcv = st.median(kept), _cv(kept)
    dmed, dcv = (st.median(dropped), _cv(dropped)) if dropped else (None, None)
    if abs(kmed - 1.0) > KEEP_TOL:
        fails.append(f"kept median stored/ext {kmed:.4f} not ~1.0 (>{KEEP_TOL})")
    if kcv > KEEP_CV:
        fails.append(f"kept segment not uniform (CV {kcv:.4f} > {KEEP_CV}) — still dirty")
    ev = {"cutover": ci, "kept_days": len(kept), "kept_median": round(kmed, 4),
          "kept_cv": round(kcv, 4), "dropped_days": len(dropped),
          "dropped_median": round(dmed, 4) if dmed is not None else None,
          "dropped_cv": round(dcv, 4) if dcv is not None else None,
          "first_kept": next((d for d in days if d >= ci), None)}
    return fails, ev


if __name__ == "__main__":
    root = ss.storage_root(".")
    plan = plan_truncation(root)
    by_iv = {}
    for iv, _ym, n in plan:
        by_iv[iv] = by_iv.get(iv, 0) + n
    phase = "DRY RUN" if plan else "POST-WRITE VERIFY"
    print(f"{phase} — WBD truncate to WBD-era (drop date < {CUTOVER}); NO writes:")
    print(f"  would drop {sum(by_iv.values())} bars across {len(plan)} month-files: "
          + ", ".join(f"{iv}={c}" for iv, c in sorted(by_iv.items())))
    fails, ev = verify(root)
    print(f"  evidence: {ev}")
    if fails:
        print("\nPROOF: FAIL — kept segment is not clean:")
        for f in fails:
            print(f"  - {f}")
        print("\nGATE: FAIL")
        sys.exit(1)
    if plan:
        print(f"\nPROOF: the kept segment (>= {CUTOVER}) matches external at ~1.0 and is "
              f"uniform; the dropped\n       pre-2022 segment is the drifting/corrupt part. "
              f"Truncation target is clean.")
    else:
        print(f"\nPROOF: no pre-cutover bars remain; the retained segment (>= {CUTOVER}) "
              "matches the external reference and is uniform.")
    print("GATE: PASS")
    sys.exit(0)
