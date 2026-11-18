"""Self-tests for phantom_fix.py.

Standalone, no network. Run:
    python engine/phantom_fix_selftest.py
"""

import datetime as dt
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import phantom_fix  # noqa: E402
import stock_storage as ss  # noqa: E402
import triage_classifier as tc  # noqa: E402


FAILS = []
N = [0]


def check(name, cond, detail=""):
    N[0] += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


def bdays(start, end):
    day = start
    one = dt.timedelta(days=1)
    out = []
    while day <= end:
        if day.weekday() < 5:
            out.append(day)
        day += one
    return out


def write_series(root, ticker, interval, days, boundary):
    tdir = Path(root) / ticker
    tdir.mkdir(parents=True, exist_ok=True)
    man = ss.load_manifest(tdir) or ss.new_manifest(ticker, ticker)
    months = ss.manifest_months(man, interval)
    by_month = {}
    for day in days:
        ratio = 0.25 if day < boundary else 1.0
        price = 100.0 * ratio
        ts = dt.datetime(day.year, day.month, day.day)
        if interval == "1m":
            ts = dt.datetime(day.year, day.month, day.day, 9, 30)
        by_month.setdefault((day.year, day.month), []).append(
            (ts, price, price, price, price, 400))
    for (year, month), bars in sorted(by_month.items()):
        path = ss.month_file_path(root, ticker, year, month, interval,
                                  fmt="csv")
        months[f"{year:04d}-{month:02d}"] = ss.write_month_file(path, bars)
    ss.save_manifest(tdir, man)


project = Path(tempfile.mkdtemp(prefix="phantom_fix_st_")) / "proj"
root = project / ss.STORAGE_DIR_NAME
root.mkdir(parents=True)
ticker = "PHAN"
boundary = dt.date(2020, 4, 1)
days = bdays(dt.date(2020, 1, 2), dt.date(2020, 6, 30))
for interval in ("1d", "1m"):
    write_series(root, ticker, interval, days, boundary)


def fake_ref(tkr, rng="Max"):
    return {day.isoformat(): (100.0, 100.0, 100.0, 100.0, 1000)
            for day in days}


before = tc.classify_flag(root, ticker, boundary, ref_fn=fake_ref)
check("phantom_fix: synthetic fixture starts as PHANTOM",
      before["verdict"] == "PHANTOM", str(before))

dry = phantom_fix.apply_correction(
    root, ticker=ticker, ex_date=boundary, factor=4,
    intervals=("1d", "1m"), dry_run=True, ref_fn=fake_ref)
check("phantom_fix: dry-run plans both intervals without writing",
      dry["dry_run"] and dry["affected_file_count"] == 6,
      str((dry.get("dry_run"), dry.get("affected_file_count"))))

snapshot = project / "snap"
res = phantom_fix.apply_correction(
    root, ticker=ticker, ex_date=boundary, factor=4,
    intervals=("1d", "1m"), snapshot_root=snapshot, ref_fn=fake_ref,
    asof="TEST")
check("phantom_fix: apply rewrites planned files and snapshots them first",
      not res["dry_run"]
      and res["affected_file_count"] == 6
      and len(res["snapshot_files"]) == 6
      and all(Path(p).exists() for p in res["snapshot_files"]),
      str(res))

after = tc.classify_flag(root, ticker, boundary, ref_fn=fake_ref)
check("phantom_fix: corrected series is no longer PHANTOM",
      after["verdict"] == "REAL", str(after))

man = ss.load_manifest(root / ticker)
check("phantom_fix: manifest records durable correction note",
      man.get("data_corrections")
      and man["data_corrections"][-1]["type"]
      == phantom_fix.CORRECTION_TYPE,
      str(man.get("data_corrections")))

jan_path = ss.month_file_path(root, ticker, 2020, 1, "1d", fmt="csv")
jan, _ = ss.read_month_file(jan_path)
check("phantom_fix: prices multiplied and volume divided",
      jan[0][4] == 100.0 and jan[0][5] == 100,
      str(jan[0]))

print(f"\n{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED:", ", ".join(FAILS))
    sys.exit(1)
print("ALL PASS")
