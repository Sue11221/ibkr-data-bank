"""Real-bank export benchmark harness for archive/_completed_work_archive/EXPORT_OPTIMIZATION_PLAN.md units.

Runs the same export paths against the local Stock Data Storage tree and writes
a JSON artifact under Run Logs. Kept stdlib-only except for project modules.
"""
import argparse
import ctypes
import json
import shutil
import sys
import tempfile
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import export_csv as ec  # noqa: E402
import export_designer as ed  # noqa: E402
import stock_storage as ss  # noqa: E402


class _PMCEX(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_uint32),
        ("PageFaultCount", ctypes.c_uint32),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
        ("PrivateUsage", ctypes.c_size_t),
    ]


def _peak_rss_mb():
    try:
        psapi = ctypes.WinDLL("Psapi.dll")
        kernel = ctypes.WinDLL("Kernel32.dll")
        get_info = psapi.GetProcessMemoryInfo
        get_info.argtypes = [ctypes.c_void_p, ctypes.POINTER(_PMCEX),
                             ctypes.c_uint32]
        get_info.restype = ctypes.c_int
        kernel.GetCurrentProcess.restype = ctypes.c_void_p
        counters = _PMCEX()
        counters.cb = ctypes.sizeof(counters)
        if not get_info(kernel.GetCurrentProcess(), ctypes.byref(counters),
                        counters.cb):
            return -1.0
        return round(counters.PeakWorkingSetSize / (1024 * 1024), 1)
    except Exception:
        return -1.0


def _span(root, ticker, interval="1m"):
    e, l = ed.ticker_span(root, ticker, interval, ["rth"])
    if e is None:
        raise RuntimeError(f"{ticker} {interval} not present")
    return e, l


def _timed(fn):
    t0 = time.perf_counter()
    res = fn()
    return res, time.perf_counter() - t0


def _fixture_designer_deep(root, ticker, out_dir):
    sd, edate = _span(root, ticker, "1m")
    spec = dict(ed.default_spec(), start_date=sd, end_date=edate,
                file_type="csv")
    out = out_dir / f"{ticker}_designer.csv"
    res, wall = _timed(lambda: ed.export_one(root, ticker, spec, out))
    return {"fixture": "a", "path": "designer_csv", "ticker": ticker,
            "start": str(sd), "end": str(edate), "wall_s": round(wall, 3),
            "peak_rss_mb": _peak_rss_mb(),
            "detail": {"rows": res.get("rows"), "bytes": out.stat().st_size}}


def _fixture_fisv_kind(root, out_dir):
    ticker = "FISV"
    sd, edate = date(2012, 2, 10), date(2012, 2, 24)
    cols = ["timestamp", "close", "hvol", "vwap"]
    spec = dict(ed.default_spec(), columns=cols, start_date=sd,
                end_date=edate, file_type="csv")
    out = out_dir / "FISV_kind.csv"
    res, wall = _timed(lambda: ed.export_one(root, ticker, spec, out))
    return {"fixture": "b", "path": "designer_kind", "ticker": ticker,
            "start": str(sd), "end": str(edate), "wall_s": round(wall, 3),
            "peak_rss_mb": _peak_rss_mb(),
            "detail": {"rows": res.get("rows"), "bytes": out.stat().st_size}}


def _fixture_legacy_compare(root, ticker, out_dir):
    sd, edate = _span(root, ticker, "1m")
    old = out_dir / f"{ticker}_old_legacy.csv"
    new = out_dir / f"{ticker}_new_legacy.csv"
    old_res, old_wall = _timed(
        lambda: ec.export_combined_csv(root, ticker, "1m", sd, edate, old))
    spec = dict(ed.default_spec(), base_interval="1m", sessions=["rth"],
                start_date=sd, end_date=edate, file_type="csv",
                storage_format=True)
    new_res, new_wall = _timed(lambda: ed.export_one(root, ticker, spec, new))
    same = old.read_bytes() == new.read_bytes()
    return {"fixture": "d", "path": "legacy_csv_compare", "ticker": ticker,
            "start": str(sd), "end": str(edate),
            "old_wall_s": round(old_wall, 3),
            "new_wall_s": round(new_wall, 3),
            "speedup": round(old_wall / new_wall, 2) if new_wall else None,
            "peak_rss_mb": _peak_rss_mb(),
            "detail": {"rows_old": old_res.get("rows"),
                       "rows_new": new_res.get("rows"),
                       "bytes": new.stat().st_size,
                       "byte_identical": same}}


def _parquet_sig(path):
    import pyarrow.parquet as pq
    tbl = pq.read_table(path)
    return ([str(f.type) for f in tbl.schema],
            list(tbl.column_names),
            {name: tbl.column(name).to_pylist() for name in tbl.column_names})


def _fixture_parquet_compare(root, ticker, out_dir):
    sd, edate = _span(root, ticker, "1m")
    spec = dict(ed.default_spec(), start_date=sd, end_date=edate,
                file_type="parquet")
    classic = out_dir / f"{ticker}_classic.parquet"
    fast = out_dir / f"{ticker}_fast.parquet"
    old_fast = ed.FAST_RENDER
    try:
        ed.FAST_RENDER = False
        rc, classic_wall = _timed(lambda: ed.export_one(root, ticker, spec, classic))
        ed.FAST_RENDER = True
        rf, fast_wall = _timed(lambda: ed.export_one(root, ticker, spec, fast))
    finally:
        ed.FAST_RENDER = old_fast
    return {"fixture": "e", "path": "parquet_compare", "ticker": ticker,
            "start": str(sd), "end": str(edate),
            "classic_wall_s": round(classic_wall, 3),
            "fast_wall_s": round(fast_wall, 3),
            "speedup": round(classic_wall / fast_wall, 2) if fast_wall else None,
            "peak_rss_mb": _peak_rss_mb(),
            "detail": {"rows_classic": rc.get("rows"),
                       "rows_fast": rf.get("rows"),
                       "classic_bytes": classic.stat().st_size,
                       "fast_bytes": fast.stat().st_size,
                       "readback_equal": _parquet_sig(classic) == _parquet_sig(fast)}}


def _fixture_parquet_kind_compare(root, out_dir):
    ticker = "FISV"
    sd, edate = date(2012, 2, 10), date(2012, 2, 24)
    spec = dict(ed.default_spec(), columns=["timestamp", "close", "hvol", "vwap"],
                start_date=sd, end_date=edate, file_type="parquet")
    classic = out_dir / "FISV_kind_classic.parquet"
    fast = out_dir / "FISV_kind_fast.parquet"
    old_fast = ed.FAST_RENDER
    try:
        ed.FAST_RENDER = False
        rc, classic_wall = _timed(lambda: ed.export_one(root, ticker, spec, classic))
        ed.FAST_RENDER = True
        rf, fast_wall = _timed(lambda: ed.export_one(root, ticker, spec, fast))
    finally:
        ed.FAST_RENDER = old_fast
    return {"fixture": "f", "path": "parquet_kind_compare", "ticker": ticker,
            "start": str(sd), "end": str(edate),
            "classic_wall_s": round(classic_wall, 3),
            "fast_wall_s": round(fast_wall, 3),
            "speedup": round(classic_wall / fast_wall, 2) if fast_wall else None,
            "peak_rss_mb": _peak_rss_mb(),
            "detail": {"rows_classic": rc.get("rows"),
                       "rows_fast": rf.get("rows"),
                       "classic_bytes": classic.stat().st_size,
                       "fast_bytes": fast.stat().st_size,
                       "readback_equal": _parquet_sig(classic) == _parquet_sig(fast)}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=str(Path.cwd() / "Stock Data Storage"))
    ap.add_argument("--ticker", default="ADP")
    ap.add_argument("--unit", default="export-bench")
    ap.add_argument("--out", default=None)
    ns = ap.parse_args()
    root = str(Path(ns.root).resolve())
    run_logs = Path.cwd() / "Run Logs"
    run_logs.mkdir(exist_ok=True)
    out_path = (Path(ns.out) if ns.out else
                run_logs / f"export-bench-{date.today().isoformat()}-e1.json")
    tmp = Path(tempfile.mkdtemp(prefix="export_bench_"))
    try:
        results = [
            _fixture_designer_deep(root, ns.ticker, tmp),
            _fixture_fisv_kind(root, tmp),
            _fixture_legacy_compare(root, ns.ticker, tmp),
            _fixture_parquet_compare(root, ns.ticker, tmp),
            _fixture_parquet_kind_compare(root, tmp),
        ]
        payload = {"date": date.today().isoformat(), "unit": ns.unit,
                   "root": root, "results": results}
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps(payload, indent=2))
        return 0 if all(r.get("detail", {}).get("byte_identical", True)
                        for r in results) else 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
