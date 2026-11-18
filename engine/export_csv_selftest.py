"""Self-tests for export_csv.py — combined-CSV stitch + presets.

Standalone, framework-free, no network: python engine/export_csv_selftest.py

Builds synthetic month files in a temp dir via stock_storage.write_month_file,
then exercises export_combined_csv over a range (row count, covered span,
edge-trim, dedup, and that a missing interior month shows up in holes) and the
preset_range date math. ASCII output only.
"""

import shutil
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss     # noqa: E402
import export_csv as ec        # noqa: E402

FAILS = []
N = [0]


def check(name, cond, detail=""):
    N[0] += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


def session_bars(d, n=5, base=100.0):
    """`n` consecutive 1-minute RTH bars on calendar date `d` (a
    datetime.date), starting at 9:30. Stays well inside RTH for n<=389."""
    out = []
    t = datetime(d.year, d.month, d.day, 9, 30, 0)
    for i in range(n):
        p = base + i * 0.01
        out.append((t, p, p + 0.05, p - 0.05, p + 0.01, 1000 + i))
        t += timedelta(minutes=1)
    return out


def first_weekday(year, month, day):
    """A datetime.date on/after (year, month, day) that is a weekday — the
    writer rejects weekend timestamps, so test data must land Mon-Fri."""
    d = date(year, month, day)
    while d.weekday() > 4:
        d += timedelta(days=1)
    return d


def main():
    tmp = Path(tempfile.mkdtemp(prefix="export_csv_selftest_"))
    try:
        run(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print()
    print(f"{N[0]} checks, {len(FAILS)} failed")
    if FAILS:
        print(f"FAILURES: {FAILS}")
        return 1
    print("ALL PASS")
    return 0


def run(tmp):
    root = ss.storage_root(tmp)
    iv = "1m"

    # Three months of data for AAPL: Jan, Feb, Apr 2024 (March deliberately
    # MISSING to test the interior-hole path). Each month gets two trading
    # days near the start and end of the month so edge-trim is observable.
    plan = {
        (2024, 1): [first_weekday(2024, 1, 3), first_weekday(2024, 1, 29)],
        (2024, 2): [first_weekday(2024, 2, 1), first_weekday(2024, 2, 26)],
        (2024, 4): [first_weekday(2024, 4, 1), first_weekday(2024, 4, 29)],
    }
    rows_per_day = 5
    for (y, m), days in plan.items():
        bars = []
        for d in days:
            bars.extend(session_bars(d, n=rows_per_day))
        p = ss.month_file_path(root, "AAPL", y, m, iv)
        ss.write_month_file(p, bars)

    print("=== [1] full span: every present month, no holes inside it =====")
    out = tmp / "aapl_full.csv"
    r = ec.export_combined_csv(root, "AAPL", iv,
                               date(2024, 1, 1), date(2024, 2, 29), out)
    # Jan(2 days) + Feb(2 days), 5 bars each = 20.
    check("full Jan-Feb row count = 20", r["rows"] == 20, str(r))
    check("months_used = Jan,Feb",
          r["months_used"] == ["2024-01", "2024-02"], str(r))
    check("no holes in a fully-present span", r["holes"] == [], str(r))
    check("covered_start is the first Jan bar",
          r["covered_start"] is not None
          and r["covered_start"].endswith("9:30:00")
          and r["covered_start"].startswith("1/"), str(r))
    check("output file exists", out.is_file())

    print("=== [2] read the export back: canonical, strict-clean ==========")
    # write_month_file enforces one-month-per-file via the FILENAME contract,
    # but read_month_file itself accepts any canonical multi-month CSV, so a
    # direct parse of the combined file must succeed and round-trip.
    bars_back, _st = ss.read_month_file(out)
    check("export round-trips through the strict reader",
          len(bars_back) == 20, str(len(bars_back)))
    check("export bytes end CRLF + canonical header",
          out.read_bytes().startswith(ss.HEADER.encode("ascii"))
          and out.read_bytes().endswith(b"\r\n"))

    print("=== [3] interior hole (March missing) is reported, not faked ===")
    out3 = tmp / "aapl_hole.csv"
    r3 = ec.export_combined_csv(root, "AAPL", iv,
                                date(2024, 1, 1), date(2024, 4, 30), out3)
    check("March shows up as a hole",
          r3["holes"] == ["2024-03"], str(r3))
    check("present months still used (Jan,Feb,Apr)",
          r3["months_used"] == ["2024-01", "2024-02", "2024-04"], str(r3))
    check("hole does not fabricate rows (3 present months * 10 = 30)",
          r3["rows"] == 30, str(r3))

    print("=== [4] edge-trim: start/end days clip the edge months =========")
    # Start the day AFTER Jan's first trading day -> that first day's 5 bars
    # must drop; end before Feb's last trading day -> Feb's last day drops.
    jan_first = plan[(2024, 1)][0]
    jan_last = plan[(2024, 1)][1]
    feb_first = plan[(2024, 2)][0]
    feb_last = plan[(2024, 2)][1]
    out4 = tmp / "aapl_trim.csv"
    r4 = ec.export_combined_csv(
        root, "AAPL", iv,
        jan_first + timedelta(days=1),        # excludes Jan's first day
        feb_last - timedelta(days=1),         # excludes Feb's last day
        out4)
    # Remaining: Jan-last (5) + Feb-first (5) = 10.
    check("edge-trim drops the two clipped edge days (10 rows)",
          r4["rows"] == 10, str(r4))
    check("covered_start is Jan's LAST trading day after trim",
          r4["covered_start"].startswith(
              ss.format_date(jan_last) + " "), str(r4))
    check("covered_end is Feb's FIRST trading day after trim",
          r4["covered_end"].startswith(
              ss.format_date(feb_first) + " "), str(r4))

    print("=== [5] dedup across the seam (defensive) ======================")
    # Build a single-month file, export a 1-month window twice into the same
    # bar set conceptually: the reader already forbids dup timestamps within
    # a file, so we instead verify the dedup path keeps strictly increasing
    # timestamps in the OUTPUT (no duplicate seam rows between months).
    ts = [b[0] for b in bars_back]
    check("exported timestamps strictly increasing (no dup seam)",
          all(ts[i] < ts[i + 1] for i in range(len(ts) - 1)))

    print("=== [6] empty span writes a header-only canonical file =========")
    out6 = tmp / "aapl_empty.csv"
    r6 = ec.export_combined_csv(root, "AAPL", iv,
                                date(2023, 1, 1), date(2023, 1, 31), out6)
    check("no bars in an out-of-range window", r6["rows"] == 0, str(r6))
    check("covered span is None when empty",
          r6["covered_start"] is None and r6["covered_end"] is None, str(r6))
    check("empty export is just the header line",
          out6.read_bytes() == (ss.HEADER + ss.EOL).encode("ascii"),
          repr(out6.read_bytes()))

    print("=== [7] zero-HVOL export remains canonical =====================")
    ratio_day = first_weekday(2024, 6, 3)
    ratio_bar = (datetime.combine(ratio_day, datetime.min.time()),
                 0.0, 0.0, 0.0, 0.0, 0)
    ratio_path = ss.month_file_path(root, "RATIO", 2024, 6, "1d-hvol")
    ss.write_month_file(ratio_path, [ratio_bar])
    ratio_out = tmp / "RATIO_2024-06_1d-hvol.csv"
    ratio_res = ec.export_combined_csv(
        root, "RATIO", "1d-hvol", ratio_day, ratio_day, ratio_out)
    ratio_back, _ratio_stats = ss.read_month_file(ratio_out)
    check("flat-zero HVOL exports and strictly reads back",
          ratio_res["rows"] == 1 and ratio_back == [ratio_bar],
          str(ratio_res))

    print("=== [8] preset_range math ======================================")
    today = date(2024, 6, 15)
    earliest = date(2014, 1, 2)
    latest = date(2024, 6, 14)
    s, e = ec.preset_range("2mo", today, earliest, latest)
    check("2mo end = today", e == today, str(e))
    check("2mo start = today - 60d",
          s == today - timedelta(days=60), str(s))
    s, e = ec.preset_range("1yr", today, earliest, latest)
    check("1yr start = today - 365d",
          s == today - timedelta(days=365), str(s))
    s, e = ec.preset_range("furthest", today, earliest, latest)
    check("furthest = earliest..latest",
          s == earliest and e == latest, f"{s}..{e}")
    # A lookback that reaches before the archive clamps to earliest.
    s, e = ec.preset_range("10yr", today, date(2020, 1, 1), latest)
    check("10yr clamps start to earliest when archive is younger",
          s == date(2020, 1, 1), str(s))
    raised = False
    try:
        ec.preset_range("3mo", today, earliest, latest)
    except ss.StorageError:
        raised = True
    check("unknown preset raises StorageError", raised)

    print("=== [9] bad inputs refuse cleanly ==============================")
    raised = False
    try:
        ec.export_combined_csv(root, "AAPL", "1m",
                               date(2024, 2, 1), date(2024, 1, 1),
                               tmp / "rev.csv")
    except ss.StorageError:
        raised = True
    check("end before start raises StorageError", raised)
    raised = False
    try:
        ec.export_combined_csv(root, "AAPL", "bogus",
                               date(2024, 1, 1), date(2024, 1, 31),
                               tmp / "bad.csv")
    except ss.StorageError:
        raised = True
    check("bad interval token raises StorageError", raised)


if __name__ == "__main__":
    sys.exit(main())
