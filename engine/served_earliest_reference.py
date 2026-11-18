"""Actual-served-earliest — REFERENCE + acceptance gate (coverage/WS1 hardening).

IBKR's advertised head (reqHeadTimeStamp) can OVERSTATE what it will actually serve:
ODFL advertises TRADES from 1991-10-24 but every real historical request returns
nothing before 2024-05-07. Recording the advertised head made the coverage audit flag
ODFL as under-backfilled forever (a false FRONT-SHORT). The fix: the "earliest" we
record + audit against must be the ACTUAL SERVED earliest — the earliest a real
historical request returns — never the advertised head alone.

Decision rule (given advertised_head, stored_first, and probe_earliest = the earliest
bar an actual deep request returns):
  * probe_earliest is None, or NOT meaningfully before stored_first  -> IBKR serves
    nothing deeper than we already store -> served_earliest = stored_first; NOT
    front-short (the bank is complete on an actual-fetchable basis).      [ODFL]
  * probe_earliest is meaningfully before stored_first  -> real data exists we haven't
    stored -> served_earliest = probe_earliest; FRONT-SHORT (backfill it). [real gap]

The advertised head is only ever an upper bound used to decide WHETHER to probe; it is
NEVER recorded as the earliest.

Gate: classify ODFL (advertised 1991, stored 2024-05, probe 2024-05-07) as NOT
front-short, and a synthetic genuine gap as FRONT-SHORT.
Run:  python engine/served_earliest_reference.py   (exit 0 = rule correct)

OWNERSHIP:
  [CLAUDE] this reference + gate define the semantics + "done" (the contract), and the
           one-time ODFL sidecar correction (`_ibkr_earliest.json` 1991 -> 2024-05-07).
  [CODEX]  implements probe-on-suspicion in stock_ibkr.py's earliest path — when the
           advertised head is far earlier than stored_first, issue ONE real deep request
           and record the SERVED earliest (not the advertised head) to
           `_ibkr_earliest.json`, so every future ticker's earliest is fetchable-real.
           Claude reviews the diff.
"""
import sys
import datetime as dt

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

GAP_MONTHS = 2          # probe earlier than stored_first by >= this = a real fetchable gap


def _to_date(value):
    s = str(value)[:10]
    if len(s) == 7:      # 'YYYY-MM' -> first of month
        s += "-01"
    return dt.date.fromisoformat(s)


def classify(advertised_head, stored_first, probe_earliest, gap_months=GAP_MONTHS):
    """Return {served_earliest, front_short, why}. Dates: 'YYYY-MM[-DD]' or None."""
    sf = _to_date(stored_first)
    if probe_earliest is None:
        return {"served_earliest": sf.isoformat(), "front_short": False,
                "why": "deep request returned no bars — IBKR serves nothing before "
                       "stored_first (advertised head unreachable)"}
    pe = _to_date(probe_earliest)
    months = (sf.year - pe.year) * 12 + (sf.month - pe.month)
    if months >= gap_months:
        return {"served_earliest": pe.isoformat(), "front_short": True,
                "why": f"real bars exist {months}mo before stored_first — genuine "
                       f"under-backfill, fetch it"}
    return {"served_earliest": pe.isoformat(), "front_short": False,
            "why": "served earliest ~= stored_first — complete on a fetchable basis "
                   "(advertised head, if earlier, is unreachable)"}


if __name__ == "__main__":
    cases = [
        # (name, advertised_head, stored_first, probe_earliest, expect_front_short)
        ("ODFL — advertised 1991, IBKR caps at 2024-05",
         "1991-10-24", "2024-05", "2024-05-07", False),
        ("deep request empty — IBKR serves nothing deeper",
         "1991-10-24", "2024-05", None, False),
        ("genuine under-backfill — data to 2011, only stored from 2020",
         "2011-06-01", "2020-01", "2011-06-15", True),
        ("already complete — stored back to the served head",
         "2011-06-01", "2011-06", "2011-06-01", False),
    ]
    ok = True
    print("Actual-served-earliest — decision-rule gate:\n")
    for name, adv, sf, pe, expect in cases:
        r = classify(adv, sf, pe)
        got = r["front_short"]
        mark = "OK" if got == expect else f"WRONG (want front_short={expect})"
        if got != expect:
            ok = False
        print(f"  {name}")
        print(f"    -> served_earliest={r['served_earliest']}  front_short={got}  [{mark}]")
        print(f"       {r['why']}")
    print("\nGATE:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)
