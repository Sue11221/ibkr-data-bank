"""Acceptance harness: Live Spot Check day-level + volatility extension (Row 44).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (LIVE_SPOT_PROBE_PLAN.md, Extension section).

Offline only: no bank, no network, no GUI, no operation-gate surface (the
classifier and discovery run against throwaway fixture roots built with the
Row 41 testbank kit). Exit contract per the plan:

  exit 1 - any check failed (baseline regression or feature violation)
  exit 3 - checks green but a milestone flag is absent (M1 and/or M2 pending)
  exit 0 - both milestones present and every check green
"""

from __future__ import annotations

import datetime as dt
import sys
import tempfile
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

ENGINE_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ENGINE_ROOT))

import live_spot_probe as lsp  # noqa: E402
import testbank  # noqa: E402

FAILURES = []
COUNT = [0]

# Mon 2024-06-17 .. Fri 2024-06-21: five consecutive weekdays.
DAYS = [dt.date(2024, 6, 17) + dt.timedelta(days=i) for i in range(5)]
KEYS = [d.isoformat() for d in DAYS]


def check(name, condition, detail=""):
    COUNT[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


def day_map(values):
    return {key: value for key, value in zip(KEYS, values)}


def baseline_classifier_day_keyed():
    """P1-P6: all six verdicts over DAY-keyed maps - classify_day needs no fork."""
    stored = day_map([50.0, 51.0, 52.0, 53.0, 54.0])

    r = lsp.classify_day(stored, dict(stored))
    check("P1 MATCH on identical day-keyed closes",
          r["verdict"] == "MATCH" and r["off_bar_count"] == 0, repr(r))

    r = lsp.classify_day(stored, {k: v * 4.0 for k, v in stored.items()})
    check("P2 uniform x4.0 on day keys is BASIS_STEP",
          r["verdict"] == "BASIS_STEP"
          and abs(r["median_factor"] - 4.0) < 1e-9, repr(r))

    r = lsp.classify_day(stored, {k: v * 0.25 for k, v in stored.items()},
                         ledger_factor=1.0, correction_factor=4.0)
    check("P3 documented correction region on day keys is DOCUMENTED_DIVERGENCE",
          r["verdict"] == "DOCUMENTED_DIVERGENCE"
          and r["off_bar_count"] == 0, repr(r))

    ratios = [1.5, 0.7, 1.3, 1.0, 1.0]
    r = lsp.classify_day(
        stored, {k: v * f for (k, v), f in zip(stored.items(), ratios)})
    check("P4 three isolated day outliers are SCATTERED_MISMATCH",
          r["verdict"] == "SCATTERED_MISMATCH", repr(r))

    r = lsp.classify_day(stored, {KEYS[0]: 50.0, KEYS[1]: 51.0})
    check("P5 live covering 2 of 5 days is COVERAGE_MISMATCH",
          r["verdict"] == "COVERAGE_MISMATCH", repr(r))

    r = lsp.classify_day(stored, {})
    check("P6 empty live day map is NO_LIVE_DATA",
          r["verdict"] == "NO_LIVE_DATA", repr(r))


def baseline_selection_domain():
    """D1: today's candidate space is minute-only; 1d-only tickers are invisible."""
    with tempfile.TemporaryDirectory(prefix="daylevel-domain-") as temp:
        root = Path(temp) / "bank"
        testbank.build_bank(root, {
            "AAA": {"1d": ["2024-05", "2024-06"]},
            "BBB": {"1m": ["2024-06"]},
        })
        candidates, errors, overflow = lsp.discover_candidates(root)
        check("D1a discover_candidates today selects only minute-bearing tickers",
              [c["ticker"] for c in candidates] == ["BBB"],
              repr([c["ticker"] for c in candidates]))
        check("D1b a 1d-only ticker is silently invisible (no error row either)",
              not errors and overflow == 0, repr((errors, overflow)))


def baseline_vol_defects():
    """V1/V2: why vol kinds need M2 (documented current behavior, not bugs here)."""
    stored = day_map([0.010, 0.010, 0.010, 0.010, 0.010])
    live = {k: v * 1.01 for k, v in stored.items()}
    r = lsp.classify_day(stored, live)
    check("V1 defect doc: uniform 0.0001-absolute vol wiggle is BASIS_STEP today "
          "(relative PRICE_TOL; M2 must MATCH under absolute VOL_TOL)",
          r["verdict"] == "BASIS_STEP", repr(r))

    try:
        lsp.classify_day({KEYS[0]: 0.0}, {KEYS[0]: 0.01})
    except lsp.EvidenceError:
        zero_ok = True
    except BaseException:  # noqa: BLE001 - wrong-type failure detail below
        zero_ok = False
    else:
        zero_ok = False
    check("V2 defect doc: a zero-vol stored close raises EvidenceError today "
          "(M2 must handle zero-vol days explicitly, never crash)", zero_ok)


def feature_daylevel():
    """M1 checks - armed only when DAYLEVEL_SPOT_CHECK is True."""
    with tempfile.TemporaryDirectory(prefix="daylevel-feature-") as temp:
        root = Path(temp) / "bank"
        testbank.build_bank(root, {
            "AAA": {"1d": ["2024-05", "2024-06"]},
            "BBB": {"1m": ["2024-06"]},
        })
        candidates, errors, overflow = lsp.discover_candidates(
            root, interval="1d")
        check("F1 interval='1d' discovery selects 1d-bearing tickers",
              [c["ticker"] for c in candidates] == ["AAA"]
              and not errors and overflow == 0,
              repr(([c["ticker"] for c in candidates], errors, overflow)))
        candidates, errors, overflow = lsp.discover_candidates(root)
        check("F2 no-arg discovery is unchanged (backward-compatible default)",
              [c["ticker"] for c in candidates] == ["BBB"]
              and not errors and overflow == 0,
              repr(([c["ticker"] for c in candidates], errors, overflow)))


def feature_volkind():
    """M2 checks - armed only when VOLKIND_SPOT_CHECK is True."""
    stored = day_map([0.010, 0.010, 0.010, 0.010, 0.010])
    r = lsp.classify_day(stored, {k: v * 1.01 for k, v in stored.items()},
                         vol_mode=True)
    check("G1 vol mode: 0.0001-absolute wiggle is MATCH under VOL_TOL",
          r["verdict"] == "MATCH", repr(r))
    r = lsp.classify_day(stored, {k: v + 0.05 for k, v in stored.items()},
                         vol_mode=True)
    check("G2 vol mode: uniform 0.05-absolute offset is UNIFORM_RECOMPUTE, "
          "never BASIS_STEP (splits do not move vol)",
          r["verdict"] == "UNIFORM_RECOMPUTE", repr(r))
    try:
        r = lsp.classify_day(day_map([0.0, 0.01, 0.01, 0.01, 0.01]),
                             day_map([0.0, 0.01, 0.01, 0.01, 0.01]),
                             vol_mode=True)
        zero_handled = isinstance(r, dict) and "verdict" in r
    except BaseException as exc:  # noqa: BLE001 - the pin is "never a crash"
        zero_handled = False
        r = repr(exc)
    check("G3 vol mode: zero-vol days classify without crashing", zero_handled,
          repr(r))


def main():
    print("=== Live Spot Check day-level + volatility extension "
          "(Row 44 reference) ===\n")
    print("--- baseline: classifier is day-key agnostic ---")
    baseline_classifier_day_keyed()
    print("--- baseline: selection domain is minute-only today ---")
    baseline_selection_domain()
    print("--- baseline: vol-kind defect documentation ---")
    baseline_vol_defects()

    daylevel = getattr(lsp, "DAYLEVEL_SPOT_CHECK", None) is True
    volkind = getattr(lsp, "VOLKIND_SPOT_CHECK", None) is True

    if daylevel:
        print("--- feature: M1 day-level discovery ---")
        feature_daylevel()
    if volkind:
        print("--- feature: M2 volatility mode ---")
        feature_volkind()

    stage = ("baseline stage" if not daylevel
             else "M1 stage" if not volkind else "full extension")
    print(f"\n{COUNT[0]} checks, {len(FAILURES)} failed ({stage})")
    if FAILURES:
        print("FAILURES:")
        for name in FAILURES:
            print(f"  - {name}")
        return 1

    if not daylevel:
        print("\n[M1 PENDING] DAYLEVEL_SPOT_CHECK absent - M1 not implemented "
              "yet. Missing surface:")
        print("  - live_spot_probe.DAYLEVEL_SPOT_CHECK = True (M1 flag)")
        print("  - discover_candidates(root, interval='1d') selects 1d months; "
              "no-arg default unchanged")
        print("  - one bounded year-span request satisfies every probe day in "
              "the span (read-only reuse rules)")
        print("Expected pre-M1 result. Exit 3.")
        return 3
    if not volkind:
        print("\n[M2 PENDING] VOLKIND_SPOT_CHECK absent - M2 not implemented "
              "(HARD-SEQUENCED behind Row 30). Missing surface:")
        print("  - live_spot_probe.VOLKIND_SPOT_CHECK = True (M2 flag)")
        print("  - classify_day(..., vol_mode=True): absolute VOL_TOL MATCH, "
              "UNIFORM_RECOMPUTE for uniform offsets, zero-vol never crashes")
        print("  - NO_LIVE_DATA reconciliation against Row 30 kind gap records")
        print("This IS M1's acceptance state. Exit 3.")
        return 3
    print("\nFULL EXTENSION ACCEPTED. Exit 0.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
