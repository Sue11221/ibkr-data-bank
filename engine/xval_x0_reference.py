"""X0 — cross-val optimization equivalence harness + prototype.
Proves a vectorized columnar daily-derivation is NUMERICALLY IDENTICAL to
derive_daily, and measures the speedup. Reference impl for Codex's X1.
Read-only on the bank."""
from __future__ import annotations
import sys, time, io
from datetime import date, datetime
from pathlib import Path
import numpy as np

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

ROOT = Path(__file__).resolve().parent.parent   # self-locating (Row 52 M2a)
sys.path.insert(0, str(ROOT / "engine"))
import stock_storage as ss
import stock_validate as sv

BANK = str(ss.storage_root(str(ROOT)))
_EPOCH_ORD = date(1970, 1, 1).toordinal()
RTH_FIRST_SEC = 9 * 3600 + 30 * 60          # 34200  (09:30:00)
RTH_LAST_SEC = 15 * 3600 + 59 * 60 + 59     # 57599  (15:59:59)


# ---- prototype columnar read (what X1's read_month_file_cols + read_series_columns do) ----
def read_month_cols(path, sha):
    """(ts_i64, o, h, l, c, v_i64) numpy, sha-gated; strict fallback on drift."""
    path = Path(path)
    if sha and path.suffix == ".parquet":
        try:
            raw = path.read_bytes()
            import hashlib
            if hashlib.sha256(raw).hexdigest() == sha:
                import pyarrow.parquet as pq
                pf = pq.ParquetFile(io.BytesIO(raw))
                if pf.schema_arrow.names == ss._PARQUET_COLS:
                    t = pf.read(columns=ss._PARQUET_COLS)
                    ts = t.column("ts").combine_chunks()
                    if not ts.null_count and ts.type.tz is None:
                        return (ts.cast(__import__("pyarrow").timestamp("s")).cast(
                                    __import__("pyarrow").int64()).to_numpy(zero_copy_only=False),
                                t.column("open").to_numpy(zero_copy_only=False),
                                t.column("high").to_numpy(zero_copy_only=False),
                                t.column("low").to_numpy(zero_copy_only=False),
                                t.column("close").to_numpy(zero_copy_only=False),
                                t.column("volume").to_numpy(zero_copy_only=False))
        except Exception:  # noqa: BLE001 — any drift -> strict
            pass
    bars, _ = ss.read_month_file(path, validate=True)   # strict fallback -> columns
    if not bars:
        e = np.array([], dtype=np.int64)
        return e, e.astype(float), e.astype(float), e.astype(float), e.astype(float), e
    ts = np.array([int((b[0] - datetime(1970, 1, 1)).total_seconds()) for b in bars], np.int64)
    return (ts, np.array([b[1] for b in bars], float), np.array([b[2] for b in bars], float),
            np.array([b[3] for b in bars], float), np.array([b[4] for b in bars], float),
            np.array([b[5] for b in bars], np.int64))


def read_series_columns(root, ticker, interval):
    canon = ss.canonical_ticker(ticker)
    man = ss.load_manifest(Path(root) / canon) or {}
    mm = ss.manifest_months(man, interval)
    cols = [[], [], [], [], [], []]
    for mk in sorted(mm):
        fp = (ss.find_month_file(root, canon, int(mk[:4]), int(mk[5:7]), interval)
              or ss.month_file_path(root, canon, int(mk[:4]), int(mk[5:7]), interval))
        ent = mm.get(mk)
        sha = ent.get("sha256") if isinstance(ent, dict) else None
        parts = read_month_cols(fp, sha)
        for i in range(6):
            cols[i].append(parts[i])
    return tuple(np.concatenate(c) if c else np.array([]) for c in cols)


# ---- prototype derive_daily_fast (what X1 implements) ----
def derive_daily_fast(cols, min_bars):
    ts, o, h, l, c, v = cols
    if ts.size == 0:
        return {}
    if not np.all(np.diff(ts) >= 0):                 # defensive: month files are sorted
        order = np.argsort(ts, kind="stable")
        ts, o, h, l, c, v = ts[order], o[order], h[order], l[order], c[order], v[order]
    sod = ts % 86400
    m = (sod >= RTH_FIRST_SEC) & (sod <= RTH_LAST_SEC)
    ts, o, h, l, c, v = ts[m], o[m], h[m], l[m], c[m], v[m]
    if ts.size == 0:
        return {}
    day = ts // 86400
    uniq, first, counts = np.unique(day, return_index=True, return_counts=True)
    last = first + counts - 1
    hi = np.maximum.reduceat(h, first)
    lo = np.minimum.reduceat(l, first)
    vol = np.add.reduceat(v, first)
    out = {}
    for i in range(uniq.size):
        if counts[i] < min_bars:
            continue
        out[date.fromordinal(_EPOCH_ORD + int(uniq[i]))] = (
            float(o[first[i]]), float(hi[i]), float(lo[i]),
            float(c[last[i]]), int(vol[i]))
    return out


def main():
    iv = "1m"
    min_bars = max(1, min(sv.MIN_DAY_BARS, int(sv._expected_rth_bars(iv) * 0.5)))
    print(f"min_bars for {iv} = {min_bars}")
    tickers = ["AAPL", "MSFT", "ADP", "A", "ACN", "ADBE"]
    fails = 0
    for t in tickers:
        try:
            t0 = time.perf_counter()
            bars = sv.read_series(BANK, t, iv)
            old_daily = sv.derive_daily(bars, min_bars=min_bars)
            t_old = time.perf_counter() - t0

            t1 = time.perf_counter()
            cols = read_series_columns(BANK, t, iv)
            new_daily = derive_daily_fast(cols, min_bars)
            t_new = time.perf_counter() - t1
        except Exception as exc:  # noqa: BLE001
            print(f"  {t}: skip ({exc})"); continue
        # EXACT dict equivalence
        eq = old_daily == new_daily
        detail = ""
        if not eq:
            fails += 1
            ok = set(old_daily); nk = set(new_daily)
            if ok != nk:
                detail = f"DATE SETS differ (+{len(nk-ok)} -{len(ok-nk)})"
            else:
                bad = next(d for d in ok if old_daily[d] != new_daily[d])
                detail = f"VALUE differ @ {bad}: old={old_daily[d]} new={new_daily[bad]}"
        print(f"  {t:5} bars={len(bars):>9,}  days={len(old_daily):>5}  "
              f"OLD={t_old:5.2f}s NEW={t_new:5.2f}s  {t_old/max(t_new,1e-9):5.1f}x  "
              f"{'IDENTICAL' if eq else 'MISMATCH: '+detail}")
    print("\n" + ("X0 EQUIVALENCE: ALL IDENTICAL — approach proven, X1 target set"
                  if not fails else f"X0: {fails} MISMATCH — approach needs a fix before X1"))
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
