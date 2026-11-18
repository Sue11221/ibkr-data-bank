"""Calendar reconciler — REFERENCE prototype + acceptance gate for
DATA_INTEGRITY_HARDENING WS5. Offline, no network.

Bridges the bank's TWO calendars so the static rule-based one stops needing
hand edits:
  * consensus_calendar (data-derived, SELF-UPDATING): the set of dates >=2
    tickers actually traded.
  * market_calendar.is_trading_day (STATIC rule-based): weekday AND not a
    computed/pinned holiday.
A weekday inside the bank's active range that the WHOLE bank did NOT trade, yet
`is_trading_day()` still calls a trading day, is an ad-hoc NYSE closure the
static list is MISSING (a national day of mourning, a weather closure, ...).
This is the bridge that would have found Sandy 2012 + Bush 2018 automatically.

Run:  python engine/calendar_reconciler_reference.py   (exit 0 = static list complete)
"""
import sys, datetime as dt
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass
sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss
import stock_validate as sv
import market_calendar as mc

# a weekday counts as a market-wide closure only if the market was demonstrably
# ACTIVE around it (a consensus trading day within +/- this many days on each
# side) — so a sparse-history edge or a lone bank gap can't masquerade as one.
ACTIVE_WINDOW_DAYS = 4


def reconcile(root, min_tickers=2):
    cal = sv.consensus_calendar(root, min_tickers=min_tickers)   # set[date]
    if not cal:
        return {"error": "no consensus calendar (bank has no 1d series)"}
    lo, hi = min(cal), max(cal)
    one = dt.timedelta(days=1)

    def market_active_around(d):
        for k in range(1, ACTIVE_WINDOW_DAYS + 1):
            if (d - k * one) in cal or (d + k * one) in cal:
                return True
        return False

    data_closed, d = [], lo
    while d <= hi:
        if d.weekday() < 5 and d not in cal and market_active_around(d):
            data_closed.append(d)
        d += one
    # of the market-wide closures, which does the STATIC calendar still get wrong?
    missing = [d for d in data_closed if mc.is_trading_day(d)]
    return {"range": (lo, hi), "data_closed": data_closed, "missing": missing}


if __name__ == "__main__":
    r = reconcile(ss.storage_root("."))
    if r.get("error"):
        print("SKIP:", r["error"])
        sys.exit(0)
    lo, hi = r["range"]
    dc = r["data_closed"]
    missing = r["missing"]
    print(f"consensus range: {lo} .. {hi}   market-wide closures seen: {len(dc)}")

    # ad-hoc closures = data-closed weekdays that are NOT standard computed
    # holidays (these are the ones the static list must pin explicitly)
    KNOWN_ADHOC = [dt.date(2012, 10, 29), dt.date(2012, 10, 30),
                   dt.date(2018, 12, 5), dt.date(2025, 1, 9)]
    seen = [d for d in KNOWN_ADHOC if d in set(dc)]
    print("known ad-hoc closures the data-layer detects: "
          + ", ".join(str(d) for d in seen))

    print(f"\nMISSING from the static rule-based calendar ({len(missing)}):")
    for d in missing:
        print(f"  {d}  (whole bank closed, but is_trading_day=True)")
    if not missing:
        print("  none — static calendar agrees with the data on every closure")

    gate = (len(missing) == 0 and len(seen) == len(KNOWN_ADHOC))
    print("\nACCEPTANCE GATE:")
    print(f"  static list complete (0 missing closures) : {len(missing) == 0}")
    print(f"  reconciler detects the ad-hoc closures     : {len(seen) == len(KNOWN_ADHOC)}"
          + ("" if len(seen) == len(KNOWN_ADHOC) else f"  (saw {seen})"))
    print("\nGATE:", "PASS" if gate else "FAIL")
    sys.exit(0 if gate else 1)
