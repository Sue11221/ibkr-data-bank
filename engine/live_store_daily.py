"""LIVE — fetch + STORE the daily (1d) TRADES series for every ticker in the bank.

Daily is a first-class storable interval (the kind/daily feature). This reuses the
production gap_fill path (full validation, entry/join gates, atomic month commits),
so a stored 1d series is byte-exact and gated like any other. Parallel across free
fleet ports; daily is cheap (one '10 Y' request ~0.3s). OFF-PEAK only; needs ports.

    python live_store_daily.py                  # whole bank, ~15yr, auto ports
    python live_store_daily.py --tickers AAPL,MSFT --years 10
    python live_store_daily.py --ports 2000,3000
"""
import argparse
import sys
import threading
import time
from datetime import timedelta
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import stock_ibkr as sk                         # noqa: E402
# reuse the bank discovery + port helpers from the internal-revalidate tool
from live_internal_revalidate import (_bank_root, _bank_series,   # noqa: E402
                                      _free_ports)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tickers", default="", help="comma subset (default: whole bank)")
    ap.add_argument("--ports", default="", help="comma ports (default: auto free fleet)")
    ap.add_argument("--years", type=int, default=15, help="how far back to store daily")
    args = ap.parse_args(argv)

    root = _bank_root()
    if not root.exists():
        print(f"bank not found: {root}")
        return 2
    only = {t.strip().upper() for t in args.tickers.split(",") if t.strip()} or None
    # one daily series per ticker that has any sub-daily data in the bank
    tickers = sorted({t for t, _i in _bank_series(root, only)})
    if not tickers:
        print("no tickers found in the bank.")
        return 0
    ports = [int(x) for x in args.ports.split(",") if x.strip()] or _free_ports()
    if not ports:
        print("NO free demo ports (2000..9000). Pause the run / pass --ports.")
        return 2
    today = sk.now_ny().date()
    since = today - timedelta(days=365 * args.years)
    print(f"bank={root}\ntickers={len(tickers)}  ports={ports}  since={since}")

    by_port = {p: [] for p in ports}
    for i, t in enumerate(tickers):
        by_port[ports[i % len(ports)]].append(t)
    results, lock = [], threading.Lock()

    def worker(port, tks):
        factory = sk.live_adapter_factory(host=sk.HOST_DEFAULT, ports=(port,),
                                          client_id=sk.CLIENT_ID_FETCH)
        for t in tks:
            try:
                rep = sk.gap_fill(root, [(t, "1d")], progress=lambda m: None,
                                  adapter_factory=factory, pacer=sk.Pacer(),
                                  today=today, since=since)
                s = (rep.get("series") or [{}])[0]
                rec = {"ticker": t, "added": s.get("added"),
                       "halt": s.get("halt"), "written": s.get("written")}
            except Exception as exc:  # noqa: BLE001
                rec = {"ticker": t, "added": None,
                       "halt": f"{type(exc).__name__}: {exc}"}
            with lock:
                results.append(rec)
                tag = "ERR" if rec.get("halt") else "OK "
                print(f"  [{tag}] {t:6} 1d added={rec.get('added')}"
                      + (f"  HALT {rec['halt']}" if rec.get("halt") else ""))

    t0 = time.monotonic()
    threads = [threading.Thread(target=worker, args=(p, by_port[p]), daemon=True)
               for p in ports]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    wall = time.monotonic() - t0

    ok = [r for r in results if not r.get("halt")]
    bad = [r for r in results if r.get("halt")]
    total_bars = sum((r.get("added") or 0) for r in ok)
    print("\n" + "=" * 56)
    print(f"DONE {len(results)} tickers in {wall:.1f}s  -> stored 1d daily series")
    print(f"  OK={len(ok)}  halted/err={len(bad)}  total daily bars added={total_bars}")
    for r in bad:
        print(f"  ! {r['ticker']}: {r['halt']}")
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
