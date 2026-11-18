"""Measure the LOCAL pack throughput on THIS machine — how fast the engine
can turn received bars into committed month files via the REAL write path
(format -> F2 codec round-trip -> sha/stats -> atomic temp+fsync+replace).

This is the denominator for "how many account pipes until the computer, not
IBKR pacing, is the bottleneck." Pure CPU+disk, no network.

Run:  python engine/pack_throughput_bench.py
"""

import shutil
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss          # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:  # noqa: BLE001
    pass


def gen_bars(year, month, count, step_s):
    """`count` valid RTH weekday bars in ONE month (so it lands in one file).
    step_s = 60 for 1-minute, 1 for 1-second. Flat OHLC (low<=o/c<=high holds)
    — only the per-bar ENCODE/PARSE cost matters here, not the values."""
    bars = []
    d = datetime(year, month, 1, 9, 30, 0)
    while d.weekday() > 4:
        d += timedelta(days=1)
    while len(bars) < count:
        if d.month != month:
            raise RuntimeError(f"{count} bars overflow month {month}")
        t = d.replace(hour=9, minute=30, second=0)
        end = d.replace(hour=15, minute=59, second=59)
        while t <= end and len(bars) < count:
            bars.append((t, 100.0, 100.5, 99.5, 100.25, 1000))
            t += timedelta(seconds=step_s)
        d = (d + timedelta(days=1))
        while d.weekday() > 4:
            d += timedelta(days=1)
    return bars


def bench(name, count, step_s, interval):
    bars = gen_bars(2024, 1, count, step_s)
    iters = min(120, max(4, int(1_500_000 / count)))
    root = Path(tempfile.mkdtemp(prefix="packbench_"))
    path = ss.month_file_path(root, "BENCH", 2024, 1, interval)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        ss.write_month_file(path, bars)                     # warm caches
        size = path.stat().st_size
        t0 = time.perf_counter()
        for _ in range(iters):                              # re-commit (atomic
            ss.write_month_file(path, bars)                 # temp+fsync+replace)
        dt = time.perf_counter() - t0
    finally:
        shutil.rmtree(root, ignore_errors=True)
    bars_s = count * iters / dt
    mb_s = size * iters / dt / 1e6
    per_file_ms = dt / iters * 1000
    print(f"  {name:<12} {count:>8,} bars/file  x{iters:<3}  "
          f"file={size/1e6:5.2f}MB  {per_file_ms:7.1f} ms/file  "
          f"{bars_s:>10,.0f} bars/s  {mb_s:6.1f} MB/s")
    return bars_s


def main():
    print("=" * 92)
    print("LOCAL PACK THROUGHPUT — real write_month_file (encode + F2 round-trip "
          "+ fsync)")
    print("=" * 92)
    rates = {}
    rates["1m_month"] = bench("1m month", 8190, 60, "1m")   # ~21 trading days 1m
    rates["1s_day"] = bench("1s day", 23400, 1, "1s")       # one RTH day of 1s
    rates["1s_8day"] = bench("1s ~8day", 200000, 1, "1s")
    rates["1s_month"] = bench("1s month", 491400, 1, "1s")  # ~21 trading days 1s

    # steady-state per-bar rate: use the largest payload (fsync amortized away)
    W = rates["1s_month"]
    print("\n" + "-" * 92)
    print(f"  Steady-state pack rate W ≈ {W:,.0f} bars/s (largest payload, "
          f"per-file fsync amortized)")

    # ---- per-pipe FETCH rates (measured live earlier) -----------------------
    # 1-second: 60 HMDS req / 10 min PER ACCOUNT, 1800 bars/req (1800 S window)
    sec_per_acct = 60 * 1800 / 600.0                     # = 180 bars/s/account
    # 1-minute: cache-served, NOT account-limited; ~26 req/min per CONNECTION,
    # ~8,190 bars per "1 M" request (latency-bound ~2.25 s/req)
    min_per_conn = 8190 / 2.25                           # ≈ 3,640 bars/s/connection

    print(f"\n  Per-pipe FETCH rates (live-measured):")
    print(f"    1-second : {sec_per_acct:8,.0f} bars/s per ACCOUNT "
          f"(60 req/10min x 1800 bars, HARD per-account cap)")
    print(f"    1-minute : {min_per_conn:8,.0f} bars/s per CONNECTION "
          f"(cache-served, ~26 req/min x 8,190 bars, NOT account-capped)")

    # ---- crossover: pipes needed to saturate the local writer ---------------
    # derate the writer for the rest of the commit path the bench omits
    # (convert_bars validate, gate checks, manifest update, top-up read+merge)
    for derate, why in ((1.0, "write only (best case)"),
                        (0.6, "incl. convert/gates/manifest (~realistic)"),
                        (0.4, "top-up read+merge+rewrite (heaviest)")):
        Wd = W * derate
        n_sec = Wd / sec_per_acct
        n_min = Wd / min_per_conn
        print(f"\n  If effective pack rate = {Wd:,.0f} bars/s  [{why}]:")
        print(f"    1-second accounts to saturate the writer : ~{n_sec:,.0f}")
        print(f"    1-minute connections to saturate the writer: ~{n_min:,.0f}")
    print()


if __name__ == "__main__":
    main()
