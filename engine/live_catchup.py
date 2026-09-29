"""Pre-update CATCH-UP (no IBKR ports): (1) external cross-validation for every
ticker that doesn't have a verdict yet, (2) connected-pattern gap DETECTION across
the whole bank (flags gaps; does NOT seal — sealing needs ports + happens on the
next update/heal). Cross-val is stockanalysis (network); the gap scan is offline.

    python live_catchup.py
"""
import sys
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import stock_validate as sv          # noqa: E402
from live_internal_revalidate import _bank_root   # noqa: E402


def main():
    root = _bank_root()
    print(f"bank={root}", flush=True)

    # 1) cross-validate the ones not yet validated
    xmap = sv.load_cross_validation(root)
    tickers = sorted({t for t, _iv in sv.discover_series(root, rth_only=True)})
    todo = [t for t in tickers if t.upper() not in xmap]
    print(f"[1] {len(tickers)} tickers, {len(todo)} NOT yet cross-validated",
          flush=True)
    done = disc = 0
    for t in todo:
        try:
            r = sv.cross_validate_ticker(
                root, t, interval="1m", rng=sv.FULL_HISTORY_RANGE)
            sv.record_cross_validation(root, r)
            done += 1
            if r.get("status") == "discrepancy":
                disc += 1
        except Exception as exc:  # noqa: BLE001
            print(f"    {t}: xval failed -> {type(exc).__name__}: {exc}", flush=True)
        if done and done % 15 == 0:
            print(f"    cross-val {done}/{len(todo)}…", flush=True)
    print(f"[1] cross-val done: {done}/{len(todo)} ({disc} discrepancy)", flush=True)

    # 2) gap DETECTION across the bank (offline; writes the sidecar so the Gaps
    #    column reflects every series — old data included)
    print("[2] scanning the whole bank for gaps (offline)…", flush=True)
    res = sv.scan_all_gaps(
        root, write=True, asof=datetime.now().isoformat(timespec="seconds"),
        preserve_existing=True)
    if res.get("error"):
        print(f"[2] gap scan failed: {res['error']}", file=sys.stderr,
              flush=True)
        return 1
    gappy = {k: v for k, v in res.get("summary", {}).items()
             if v.get("missing_total") or v.get("missing_days")}
    print(f"[2] scanned {res.get('series_scanned')} series; "
          f"{len(gappy)} have gaps; "
          f"interior_missing={res.get('missing_total')} "
          f"missing_days_total={res.get('missing_days_total')}", flush=True)
    for k, v in sorted(gappy.items(),
                       key=lambda kv: -(kv[1].get("missing_total", 0)
                                        + kv[1].get("missing_days", 0)))[:25]:
        print(f"    {k}: interior={v.get('missing_total',0)} "
              f"missing_days={v.get('missing_days',0)}", flush=True)
    print("DONE.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
