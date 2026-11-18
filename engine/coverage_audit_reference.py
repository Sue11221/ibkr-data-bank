"""Coverage-completeness audit — REFERENCE prototype + acceptance gate for
COVERAGE_AUDIT_PLAN.md Phase 1. Offline (manifest + _ibkr_earliest, no network).

Joins what the bank ALREADY knows but never cross-referenced: stored coverage vs
IBKR earliest-available (front) and vs the latest trading month (forward). This is
the join that surfaced ODFL. Codex's production detector must reproduce the gate.

Run:  python engine/coverage_audit_reference.py   (exit 0 = gate holds)
"""
import sys, os, json, statistics
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass
sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss

FORWARD_STALE_MONTHS = 2       # stored_last this far behind bank_latest -> forward-truncated
FRONT_LATE_MONTHS = 18         # stored_first this far AFTER the baseline norm ...
FRONT_AVAIL_MONTHS = 18        # ... AND IBKR earliest this far BEFORE stored_first -> under-backfill


def mono(ym):                  # "YYYY-MM" -> month ordinal
    y, m = int(ym[:4]), int(ym[5:7]); return y * 12 + (m - 1)


def identity_floor(manifest, interval, ticker=None):
    boundary = ss.identity_floor(manifest, interval, ticker=ticker)
    return boundary.isoformat()[:7] if boundary is not None else None


def audit(root):
    root = Path(root)
    active = sorted(d for d in os.listdir(root)
                    if (root / d).is_dir() and not d.startswith("_"))
    earliest = {}
    ep = root / "_ibkr_earliest.json"
    if ep.exists():
        earliest = json.load(open(ep))

    def ev(t):
        v = earliest.get(t)
        if isinstance(v, dict):
            v = v.get("earliest") or v.get("date")
        return str(v)[:7] if v else None      # "YYYY-MM" or None

    cov = {}
    for t in active:
        man = ss.load_manifest(root / t)
        if not man:
            continue
        intervals = man.get("intervals", {})
        interval = "1m" if intervals.get("1m") else "1d"
        mo = sorted((intervals.get(interval) or {}).get("months", {}))
        if mo:
            cov[t] = (
                mo[0], mo[-1], len(mo),
                identity_floor(man, interval, ticker=t),
            )

    bank_latest = max(v[1] for v in cov.values())
    baseline = sorted(v[0] for v in cov.values())[len(cov) // 2]      # median start
    bl, base = mono(bank_latest), mono(baseline)

    forward, front, unknown = [], [], []
    for t, (sf, sl, n, floor) in cov.items():
        if bl - mono(sl) >= FORWARD_STALE_MONTHS:
            forward.append((t, sl, bl - mono(sl)))
        e = ev(t)
        if floor is not None and mono(sf) >= mono(floor):
            if e is None or mono(floor) > mono(e):
                e = floor
        if e is None:
            unknown.append(t)
        elif (mono(sf) - base >= FRONT_LATE_MONTHS
              and mono(sf) - mono(e) >= FRONT_AVAIL_MONTHS):
            front.append((t, sf, e, mono(sf) - mono(e)))
    return {"bank_latest": bank_latest, "baseline": baseline, "n": len(cov),
            "forward": sorted(forward), "front": sorted(front),
            "unknown": sorted(unknown)}


if __name__ == "__main__":
    r = audit(ss.storage_root("."))
    print(f"tickers={r['n']}  bank_latest={r['bank_latest']}  baseline_start={r['baseline']}")
    print(f"\nFORWARD-STALE ({len(r['forward'])}):")
    for t, sl, b in r["forward"]:
        print(f"  {t:6} last={sl} ({b}mo behind)")
    print(f"\nFRONT-SHORT / under-backfill ({len(r['front'])}):")
    for t, sf, e, d in r["front"]:
        print(f"  {t:6} stored_first={sf}  ibkr_earliest={e}  (~{d // 12}yr missing)")
    print(f"\nUNKNOWN earliest (needs head-probe): {len(r['unknown'])}")

    fwd = {t for t, *_ in r["forward"]}
    frt = {t for t, *_ in r["front"]}
    # Repaired-bank ground truth (updated 2026-07-09): HLT/PCG/KDP/FTNT re-fetched
    # forward-fresh (FORWARD-STALE empty), and ODFL's earliest was corrected from the
    # unreachable advertised head (1991-10-24) to the actual IBKR-SERVED date
    # (2024-05-07) — IBKR genuinely serves nothing deeper, so ODFL is no longer a false
    # FRONT-SHORT and the bank is coverage-COMPLETE on an actual-fetchable-data basis.
    # (FTNT's deep phantom-split seam is a price-value defect coverage can't see; its
    # frontier is current.) Catch-ability is proven by health_selftest.py fixtures;
    # this gate is now a pure REGRESSION tripwire — the bank is coverage-CLEAN, so ANY
    # forward-stale or front-short flags a real regression.
    EXP_FWD = set()
    EXP_FRT = set()
    ok = (fwd == EXP_FWD and frt == EXP_FRT)
    print("\nACCEPTANCE GATE (repaired bank, 2026-07-09 — coverage-clean):")
    print(f"  FORWARD-STALE == {{}} (all repaired)          : {fwd == EXP_FWD}"
          + ("" if fwd == EXP_FWD else f"  got {sorted(fwd)}"))
    print(f"  FRONT-SHORT   == {{}} (ODFL served-earliest ok): {frt == EXP_FRT}"
          + ("" if frt == EXP_FRT else f"  got {sorted(frt)}"))
    print("\nGATE:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)
