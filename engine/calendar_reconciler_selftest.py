"""Self-tests for calendar_reconciler.py.

Standalone, no network:
    python engine/calendar_reconciler_selftest.py
"""

import datetime as dt
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import calendar_reconciler as cr  # noqa: E402
import market_calendar as mc  # noqa: E402
import stock_storage as ss  # noqa: E402


FAILS = []
N = [0]


def check(label, cond, detail=""):
    N[0] += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {label}"
          + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(label)


def seed_1d(root, ticker, days):
    root = Path(root)
    tdir = root / ticker
    tdir.mkdir(parents=True, exist_ok=True)
    man = ss.new_manifest(ticker, ticker)
    grouped = {}
    for d in days:
        grouped.setdefault((d.year, d.month), []).append(d)
    for (year, month), ds in grouped.items():
        bars = [
            (dt.datetime(d.year, d.month, d.day), 1.0, 1.1, 0.9, 1.0, 100)
            for d in sorted(ds)
        ]
        stats = ss.write_month_file(
            ss.month_file_path(root, ticker, year, month, "1d", fmt="csv"),
            bars)
        ss.manifest_months(man, "1d")[f"{year:04d}-{month:02d}"] = {
            "status": "present", **stats,
        }
    ss.save_manifest(tdir, man)


base = Path(tempfile.mkdtemp(prefix="calrec_st_"))
root = base / ss.STORAGE_DIR_NAME
root.mkdir(parents=True)

mon = dt.date(2020, 1, 6)
tue = dt.date(2020, 1, 7)
wed = dt.date(2020, 1, 8)

seed_1d(root, "AAA", [mon, wed])
seed_1d(root, "BBB", [mon, wed])
mc.clear_special_closures()
report = cr.maintain(root, write=True, asof="TEST")

check("high-confidence zero-trade weekday is auto-added",
      report.get("auto_added") == [tue.isoformat()],
      str(report))
check("sidecar exists after auto-maintain",
      (root / cr.SPECIAL_CLOSURES_FILE).is_file())
loaded = cr.load_sidecar(root)
check("sidecar round-trip preserves the date",
      cr.sidecar_dates(root) == [tue.isoformat()],
      str(loaded))
mc.clear_special_closures()
mc.load_special_closures(root)
check("market_calendar honors loaded sidecar closure",
      not mc.is_trading_day(tue))

root2 = base / "Sparse Bank"
root2.mkdir()
seed_1d(root2, "AAA", [mon, tue, wed])
seed_1d(root2, "BBB", [mon, wed])
mc.clear_special_closures()
report2 = cr.maintain(root2, write=True, asof="TEST")
check("ambiguous one-ticker gap is not auto-added",
      report2.get("auto_added") == [] and report2.get("missing") == [],
      str(report2))
check("ambiguous day is reported for visibility",
      any(r.get("date") == tue.isoformat()
          for r in report2.get("ambiguous") or []),
      str(report2.get("ambiguous")))
check("no sidecar written for ambiguous-only case",
      not (root2 / cr.SPECIAL_CLOSURES_FILE).exists())

mc.clear_special_closures()
print(f"\n{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED:", ", ".join(FAILS))
    sys.exit(1)
print("ALL PASS")
