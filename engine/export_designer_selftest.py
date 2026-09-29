"""Headless tests for the configurable Export Designer engine — no network, no
tkinter. Seeds a REAL synthetic bank (month files + manifests via the storage
writer) so the read/join/render/round-trip paths are exercised for real.
    python engine/export_designer_selftest.py"""
import ast
import copy
import sys
import shutil
import tempfile
import textwrap
from types import SimpleNamespace
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss
import export_designer as ed
import export_csv as ec

_PASS = [0]
_FAIL = [0]
_DISPLAY_DATA = Path(__file__).resolve().parents[1] / "display_data.py"


def check(cond, name):
    if cond:
        _PASS[0] += 1
    else:
        _FAIL[0] += 1
        print("  FAIL:", name)


def _display_def(name, extra_globals=None):
    """Return one display_data function without importing the tkinter app."""
    src = _DISPLAY_DATA.read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                and node.name == name:
            segment = ast.get_source_segment(src, node)
            ns = dict(extra_globals or {})
            exec(compile(textwrap.dedent(segment),
                         f"<display_data:{name}>", "exec"), ns)
            return ns[name], segment
    raise AssertionError(f"display_data function not found: {name}")


# --- seeding ----------------------------------------------------------------

def price_day(d, n, base=100.0, step_min=1):
    """n RTH price bars starting 9:30, 1-minute (or step_min) apart."""
    out, t = [], datetime.combine(d, time(9, 30))
    for i in range(n):
        p = round(base + i * 0.01, 2)
        out.append((t, p, round(p + 0.05, 2), round(p - 0.05, 2),
                    round(p + 0.02, 2), 100 + i))
        t += timedelta(minutes=step_min)
    return out


def iv_daily(dates, values):
    """One daily ratio bar per date (OHLC = the ratio, volume 0, midnight)."""
    return [(datetime.combine(d, time(0, 0, 0)), v, v, v, v, 0)
            for d, v in zip(dates, values)]


def seed(root, ticker, bars, interval="1m"):
    """Write a real stored series (month files + manifest). Accumulates onto an
    existing manifest so several intervals can be seeded for one ticker."""
    canon = ss.canonical_ticker(ticker)
    tdir = Path(root) / canon
    tdir.mkdir(parents=True, exist_ok=True)
    bym = defaultdict(list)
    for b in bars:
        bym[(b[0].year, b[0].month)].append(b)
    man = ss.load_manifest(tdir) or ss.new_manifest(ticker, canon)
    months = ss.manifest_months(man, interval)
    for (y, m), bb in bym.items():
        ss.write_month_file(ss.month_file_path(root, canon, y, m, interval), bb)
        months[f"{y}-{m:02d}"] = {"status": "present", "rows": len(bb)}
    ss.save_manifest(tdir, man)


def mkbank():
    base = tempfile.mkdtemp(prefix="ed_")
    root = str(Path(base) / "Stock Data Storage")
    Path(root).mkdir(parents=True)
    return base, root


# --- value formats ----------------------------------------------------------

def test_num_str():
    check(ed.num_str(28.20) == "28.2", "trailing zero dropped (28.20 -> 28.2)")
    check(ed.num_str(198.0) == "198", ".0 dropped (198.0 -> 198)")
    check(ed.num_str(0.0) == "0", "zero formats as 0")
    check(ed.num_str(1234567.0) == "1234567", "large int-valued price")
    v = 8.9285714285714e-05
    s = ed.num_str(v)
    check("e" not in s and "E" not in s and float(s) == v,
          "sub-penny: positional (no exponent) AND exact round-trip")
    check(ed.num_str(-1.5) == "-1.5", "negatives format (defensive)")


def test_eastern_offset():
    h5, h4 = timedelta(hours=-5), timedelta(hours=-4)
    check(ed.eastern_offset(datetime(2024, 1, 15)) == h5, "January -> EST (-5)")
    check(ed.eastern_offset(datetime(2024, 7, 15)) == h4, "July -> EDT (-4)")
    # 2024 spring-forward = 2nd Sunday March = Mar 10, 02:00
    check(ed.eastern_offset(datetime(2024, 3, 10, 1, 59)) == h5,
          "just before spring-forward is still EST")
    check(ed.eastern_offset(datetime(2024, 3, 10, 2, 0)) == h4,
          "at spring-forward switches to EDT")
    # 2024 fall-back = 1st Sunday November = Nov 3, 02:00
    check(ed.eastern_offset(datetime(2024, 11, 3, 1, 59)) == h4,
          "just before fall-back is still EDT")
    check(ed.eastern_offset(datetime(2024, 11, 3, 2, 0)) == h5,
          "at fall-back switches to EST")
    check(ed.eastern_offset(datetime(2025, 3, 9, 2, 0)) == h4,
          "2025 spring-forward is Mar 9 (rule is per-year, not fixed date)")


def test_format_timestamp():
    win = datetime(2024, 1, 2, 9, 30, 0)
    summ = datetime(2024, 7, 1, 9, 30, 0)
    ny = {"date_format": "iso", "timezone": "America/New_York"}
    utc = {"date_format": "iso", "timezone": "UTC"}
    check(ed.format_timestamp(win, ny) == "2024-01-02T09:30:00-05:00",
          "NY iso carries -05:00 in winter")
    check(ed.format_timestamp(summ, ny) == "2024-07-01T09:30:00-04:00",
          "NY iso carries -04:00 in summer")
    check(ed.format_timestamp(win, utc) == "2024-01-02T14:30:00+00:00",
          "UTC shifts EST 09:30 -> 14:30 and shows +00:00")
    check(ed.format_timestamp(summ, utc) == "2024-07-01T13:30:00+00:00",
          "UTC shifts EDT 09:30 -> 13:30")
    check(ed.format_timestamp(win, {"date_format": "date-only"}) == "2024-01-02",
          "date-only drops the time")
    check(ed.format_timestamp(win, {"date_format": "custom",
                                    "date_custom": "%Y/%m/%d %H%M"})
          == "2024/01/02 0930", "custom strftime honored")
    check(ed.format_date(summ, ny) == "2024-07-01"
          and ed.format_time(summ, ny) == "09:30:00",
          "split NY date/time carries no EDT offset suffix")
    late = datetime(2024, 1, 2, 20, 30, 0)
    check(ed.format_date(late, utc) == "2024-01-03"
          and ed.format_time(late, utc) == "01:30:00",
          "split UTC date/time shifts the wall clock across midnight")


def test_epoch_tz_invariant():
    import calendar
    dt = datetime(2024, 7, 1, 9, 30, 0)        # EDT -4 -> 13:30 UTC
    a = ed.format_timestamp(dt, {"date_format": "epoch",
                                 "timezone": "America/New_York"})
    b = ed.format_timestamp(dt, {"date_format": "epoch", "timezone": "UTC"})
    check(a == b, "epoch is identical under NY and UTC (absolute instant)")
    check(int(a) == calendar.timegm((2024, 7, 1, 13, 30, 0, 0, 0, 0)),
          "epoch is the correct absolute second count (EDT -4)")


# --- rendering --------------------------------------------------------------

def test_render_csv_basic():
    bars = price_day(date(2024, 1, 2), 3)
    rows = ed.build_rows(bars, "AAPL", ed.DEFAULT_COLUMNS)
    spec = ed.default_spec()
    out = ed.render_rows(rows, spec)
    lines = out.decode("utf-8").split("\n")
    check(lines[0] == "date,time,open,high,low,close,volume",
          "default header uses separate date and time")
    f = lines[1].split(",")
    check(f[:2] == ["2024-01-02", "09:30:00"]
          and "-05:00" not in lines[1],
          "default row uses suffix-free wall-clock date/time")
    check(f[2] == ed.num_str(bars[0][1]) and f[6] == str(bars[0][5]),
          "values are RAW as stored, full precision")
    check(b"\r\n" not in out, "line ends are LF, never CRLF")


def test_render_csv_options():
    bars = price_day(date(2024, 1, 2), 2)
    cols = ["close", "open", "timestamp"]            # deliberately reordered
    rows = ed.build_rows(bars, "AAPL", cols)
    spec = ed.default_spec()
    spec["columns"] = cols
    check(ed.render_rows(rows, spec).decode().split("\n")[0] == "close,open,timestamp",
          "column reorder is respected in output")
    s2 = dict(spec, delimiter=";")
    check(ed.render_rows(rows, s2).decode().split("\n")[0] == "close;open;timestamp",
          "delimiter honored")
    s3 = dict(spec, file_type="tsv")
    check("\t" in ed.render_rows(rows, s3).decode().split("\n")[0],
          "tsv uses tabs regardless of delimiter")
    s4 = dict(spec, header=False)
    check(ed.render_rows(rows, s4).decode().split("\n")[0].startswith(
        ed.num_str(rows[0]["close"])), "header off -> first line is data")


def test_empty_modes():
    rows = ed.build_rows(price_day(date(2024, 1, 2), 1), "X", ["close", "iv"])
    base = dict(ed.default_spec(), columns=["close", "iv"])

    def iv_cell(mode):
        line = ed.render_rows(rows, dict(base, empty=mode)).decode().split("\n")[1]
        return line.split(",")[1]
    check(iv_cell("blank") == "", "blank empty -> nothing between delimiters")
    check(iv_cell("nan") == "NaN", "nan token")
    check(iv_cell("null") == "null", "null token")
    check(iv_cell("zero") == "0", "zero token")


def test_csv_quoting_custom_date():
    rows = ed.build_rows(price_day(date(2024, 1, 2), 1), "X", ["timestamp", "close"])
    spec = dict(ed.default_spec(), columns=["timestamp", "close"],
                date_format="custom", date_custom="%Y, %b %d")
    txt = ed.render_rows(rows, spec).decode()
    check('"2024, Jan 02"' in txt,
          "a delimiter inside a formatted field is quoted (csv-correct)")


def test_sample_equals_export():
    rows = ed.build_rows(price_day(date(2024, 1, 2), 20), "AAPL",
                         ed.DEFAULT_COLUMNS)
    spec = ed.default_spec()
    full = ed.render_rows(rows, spec)
    samp = ed.render_rows(rows[:5], spec)
    check(full.startswith(samp),
          "LIVE SAMPLE is a byte-prefix of the full CSV export (same formatter)")
    check(ed.render_sample_text(rows, spec, max_rows=5) == samp.decode(),
          "render_sample_text equals the exact bytes for a text format")


def test_render_json():
    import json
    rows = ed.build_rows(price_day(date(2024, 1, 2), 2), "AAPL",
                         ["timestamp", "close", "volume", "iv"])
    spec = dict(ed.default_spec(), file_type="json",
                columns=["timestamp", "close", "volume", "iv"], empty="null")
    data = json.loads(ed.render_rows(rows, spec).decode())
    check(len(data) == 2 and isinstance(data[0]["close"], float),
          "json: numbers stay numbers")
    check(isinstance(data[0]["volume"], int), "json: volume is an int")
    ivk = ed.COLUMN_LABEL["iv"]        # json keys carry the EXPORT labels
    check(data[0][ivk] is None, "json: missing iv -> null")
    check(data[0]["timestamp"] == "2024-01-02T09:30:00-05:00",
          "json: iso timestamp string")
    d2 = json.loads(ed.render_rows(rows, dict(spec, date_format="epoch")).decode())
    check(isinstance(d2[0]["timestamp"], int), "json: epoch timestamp -> int")
    d3 = json.loads(ed.render_rows(rows, dict(spec, empty="zero")).decode())
    check(d3[0][ivk] == 0, "json: zero empty-mode -> 0")
    check(ed.render_rows([], spec) == b"[]", "json: empty rows -> []")
    default_data = json.loads(ed.render_rows(
        ed.build_rows(price_day(date(2024, 7, 1), 1), "AAPL",
                      ed.DEFAULT_COLUMNS),
        dict(ed.default_spec(), file_type="json")).decode())
    check(default_data[0]["date"] == "2024-07-01"
          and default_data[0]["time"] == "09:30:00",
          "json default has separate suffix-free date/time strings")


def test_render_parquet():
    try:
        import io
        import pyarrow.parquet as pq
    except ImportError:
        return
    bars = price_day(date(2024, 1, 2), 3)
    cols = ["timestamp", "open", "close", "volume", "iv"]
    rows = ed.build_rows(bars, "AAPL", cols)             # iv None (no bank)
    spec = dict(ed.default_spec(), file_type="parquet", columns=cols, empty="null")
    ivk = ed.COLUMN_LABEL["iv"]        # parquet schema carries the EXPORT labels
    tbl = pq.read_table(io.BytesIO(ed.render_rows(rows, spec)))
    check(list(tbl.column_names) == [ed.COLUMN_LABEL[c] for c in cols],
          "parquet: schema columns == labels")
    check(tbl.column("open").to_pylist() == [b[1] for b in bars],
          "parquet: prices lossless float64")
    check(tbl.column("volume").to_pylist() == [b[5] for b in bars],
          "parquet: volume int64")
    check(tbl.column(ivk).to_pylist() == [None, None, None],
          "parquet: missing iv is null")
    check(str(tbl.schema.field("timestamp").type).startswith("timestamp"),
          "parquet: native timestamp type for iso date_format")
    tbl2 = pq.read_table(io.BytesIO(
        ed.render_rows(rows, dict(spec, date_format="epoch"))))
    check(str(tbl2.schema.field("timestamp").type) == "int64",
          "parquet: epoch date_format -> int64 timestamp")
    tbl3 = pq.read_table(io.BytesIO(
        ed.render_rows(rows, dict(spec, empty="zero"))))
    check(tbl3.column(ivk).to_pylist() == [0.0, 0.0, 0.0],
          "parquet: zero empty-mode -> 0.0 (not null)")


def test_empty_rows_csv():
    spec = ed.default_spec()
    check(ed.render_rows([], spec)
          == b"date,time,open,high,low,close,volume\n",
          "csv: empty rows -> header line only")
    check(ed.render_rows([], dict(spec, header=False)) == b"",
          "csv: empty rows, header off -> empty bytes")


# --- VWAP + joins -----------------------------------------------------------

def test_vwap():
    d = date(2024, 1, 2)
    bars = [(datetime.combine(d, time(9, 30)), 10, 12, 8, 10, 100),   # typ 10
            (datetime.combine(d, time(9, 31)), 10, 14, 10, 12, 200),  # typ 12
            (datetime.combine(d, time(9, 32)), 12, 12, 12, 12, 0)]    # typ 12, v0
    rows = ed.build_rows(bars, "X", ["vwap"])
    check(abs(rows[0]["vwap"] - 10.0) < 1e-9, "vwap row0 = typical price")
    check(abs(rows[1]["vwap"] - 3400 / 300) < 1e-9,
          "vwap row1 = (10*100 + 12*200)/300")
    check(abs(rows[2]["vwap"] - 3400 / 300) < 1e-9,
          "vwap row2: a zero-volume bar doesn't move cumulative vwap")
    d2 = date(2024, 1, 3)
    rows2 = ed.build_rows(bars + [(datetime.combine(d2, time(9, 30)),
                                   20, 22, 18, 20, 50)], "X", ["vwap"])
    check(abs(rows2[3]["vwap"] - 20.0) < 1e-9, "vwap resets at each new day")
    rowz = ed.build_rows([(datetime.combine(d, time(9, 30)), 5, 6, 4, 5, 0)],
                         "X", ["vwap"])
    check(abs(rowz[0]["vwap"] - 5.0) < 1e-9,
          "vwap: a first bar with zero volume falls back to typical price")


def test_forward_fill_unit():
    rows = [{"dt": datetime.combine(date(2024, 1, d), time(9, 30))}
            for d in (2, 3, 4, 5)]
    ed.forward_fill(rows, {date(2024, 1, 3): 0.2, date(2024, 1, 5): 0.3}, "iv")
    check(rows[0]["iv"] is None, "before first stored date -> None")
    check(rows[1]["iv"] == 0.2, "on a stored date -> that value")
    check(rows[2]["iv"] == 0.2, "a gap day forward-fills the prior value")
    check(rows[3]["iv"] == 0.3, "next stored date steps to the new value")
    empty = [{"dt": datetime(2024, 1, 2, 9, 30)}]
    ed.forward_fill(empty, {}, "iv")
    check(empty[0]["iv"] is None, "no stored values -> all None")


def test_iv_join_daily_forward_fill():
    base, root = mkbank()
    try:
        price = (price_day(date(2024, 1, 2), 5) + price_day(date(2024, 1, 3), 5)
                 + price_day(date(2024, 1, 4), 5))
        seed(root, "AAPL", price, "1m")
        seed(root, "AAPL", iv_daily([date(2024, 1, 2), date(2024, 1, 4)],
                                    [0.20, 0.25]), "1d-iv")
        rows = ed.build_rows(price, "AAPL", ["iv"], root=root, base_iv="1m")
        by = defaultdict(set)
        for r in rows:
            by[r["dt"].date()].add(r["iv"])
        check(by[date(2024, 1, 2)] == {0.20}, "Jan 2 rows -> that day's IV 0.20")
        check(by[date(2024, 1, 3)] == {0.20},
              "Jan 3 (no IV bar) forward-fills 0.20 onto every minute")
        check(by[date(2024, 1, 4)] == {0.25}, "Jan 4 rows step to 0.25")
        _bars, _mode, source = ed.read_kind_series(
            root, "AAPL", "1m", "iv", include_source=True)
        check(source == "1d-iv",
              "kind selection metadata names the daily IV fallback")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_iv_join_intraday_exact():
    base, root = mkbank()
    try:
        price = price_day(date(2024, 1, 2), 5)
        seed(root, "BBB", price, "1m")
        iv1m = [(price[i][0], 0.10, 0.10, 0.10, 0.10, 0) for i in range(3)]
        seed(root, "BBB", iv1m, "1m-iv")
        rows = ed.build_rows(price, "BBB", ["iv"], root=root, base_iv="1m")
        check([r["iv"] for r in rows] == [0.10, 0.10, 0.10, None, None],
              "intraday IV exact-joins by timestamp; unmatched rows -> None")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_iv_intraday_preferred_over_daily():
    base, root = mkbank()
    try:
        price = price_day(date(2024, 1, 2), 3)
        seed(root, "CCC", price, "1m")
        seed(root, "CCC", iv_daily([date(2024, 1, 2)], [0.99]), "1d-iv")
        seed(root, "CCC", [(price[i][0], 0.11, 0.11, 0.11, 0.11, 0)
                           for i in range(3)], "1m-iv")
        rows = ed.build_rows(price, "CCC", ["iv"], root=root, base_iv="1m")
        check([r["iv"] for r in rows] == [0.11, 0.11, 0.11],
              "an intraday IV series is preferred over the daily one")
        out = Path(base) / "CCC.csv"
        result = ed.export_one(
            root, "CCC",
            dict(ed.default_spec(), columns=["close", "iv"],
                 start_date=date(2024, 1, 2), end_date=date(2024, 1, 2)),
            out)
        check(result["source_intervals"] == ["1m", "1m-iv"],
              "export result follows the preferred intraday IV series")
    finally:
        shutil.rmtree(base, ignore_errors=True)


# --- reading from the bank --------------------------------------------------

def test_read_merged_sessions():
    base, root = mkbank()
    try:
        d = date(2024, 1, 2)
        rth = price_day(d, 5)                                   # 9:30-9:34
        pre = [(datetime.combine(d, time(9, 15 + i)), 99.0, 99.05, 98.95,
                99.0, 5) for i in range(3)]                     # 9:15-9:17 pre
        seed(root, "DDD", rth, "1m")
        seed(root, "DDD", pre, "1m-pre")
        bars, holes = ed.read_merged_bars(root, "DDD", "1m", ["rth", "pre"],
                                          d, d)
        check(len(bars) == 8, "merged 5 RTH + 3 pre bars")
        check(bars[0][0].time() == time(9, 15) and bars[-1][0].time() == time(9, 34),
              "merge is timestamp-sorted (pre interleaves before RTH)")
        _, holes2 = ed.read_merged_bars(root, "DDD", "1m", ["rth", "post"], d, d)
        check(any("post:not-archived" in h for h in holes2),
              "a never-stored session is reported as not-archived, not invented")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_read_merged_holes_and_trim():
    base, root = mkbank()
    try:
        # Jan + Mar stored, Feb missing (interior hole); ask Jan..Mar
        seed(root, "EEE", price_day(date(2024, 1, 2), 5), "1m")
        seed(root, "EEE", price_day(date(2024, 3, 1), 5), "1m")
        bars, holes = ed.read_merged_bars(root, "EEE", "1m", ["rth"],
                                          date(2024, 1, 1), date(2024, 3, 31))
        check(any("2024-02" in h for h in holes),
              "a missing interior month is reported as a hole")
        check(len(bars) == 10, "data exported AROUND the hole (5 Jan + 5 Mar)")
        # day-window trim: ask only Jan 2 onwards within the start month
        b2, _ = ed.read_merged_bars(root, "EEE", "1m", ["rth"],
                                    date(2024, 1, 3), date(2024, 1, 31))
        check(b2 == [], "start-day trim removes Jan-2 bars when window starts Jan-3")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_missing_from_bank():
    base, root = mkbank()
    try:
        seed(root, "AAPL", price_day(date(2024, 1, 2), 3), "1m")
        seed(root, "MSFT", price_day(date(2024, 1, 2), 3), "1m")
        seed(root, "VOLONLY", iv_daily([date(2024, 1, 2)], [0.2]),
             "1d-iv")
        check(ed.stored_tickers(root) == ["AAPL", "MSFT", "VOLONLY"],
              "stored_tickers keeps a ticker whose only series is a data kind")
        present, missing = ed.missing_from_bank(
            root, ["aapl", "MSFT", "NOPE", "brk.b"])
        check(present == ["AAPL", "MSFT"], "stored symbols (canonicalized) are present")
        check(missing == ["NOPE", "BRK-B"],
              "absent symbols are missing, canonicalized (brk.b -> BRK-B)")
        p2, _ = ed.missing_from_bank(root, ["AAPL", "aapl", "AAPL"])
        check(p2 == ["AAPL"], "duplicate selections collapse")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_estimate():
    base, root = mkbank()
    try:
        seed(root, "AAPL", price_day(date(2024, 1, 2), 10)
             + price_day(date(2024, 2, 1), 10), "1m")
        spec = ed.default_spec()
        e = ed.estimate(root, ["AAPL"], "1m", ["rth"],
                        date(2024, 1, 1), date(2024, 2, 28), spec)
        check(e["rows"] == 20 and e["files"] == 1,
              "estimate sums manifest row counts over the range")
        e2 = ed.estimate(root, ["AAPL"], "1m", ["rth"],
                         date(2024, 1, 1), date(2024, 1, 31), spec)
        check(e2["rows"] == 10, "estimate respects the month range (Feb excluded)")
        e3 = ed.estimate(root, ["AAPL", "ZZZ"], "1m", ["rth"],
                         date(2024, 1, 1), date(2024, 2, 28), spec)
        check(e3["files"] == 1, "a ticker with no in-range data isn't a file")
        check(e["bytes"] > 0, "estimate produces a positive byte figure")
    finally:
        shutil.rmtree(base, ignore_errors=True)


# --- end-to-end export ------------------------------------------------------

def test_ticker_span_and_resolve_range():
    base, root = mkbank()
    try:
        seed(root, "AAPL", price_day(date(2022, 5, 2), 5)
             + price_day(date(2024, 8, 1), 5), "1m")
        e, l = ed.ticker_span(root, "AAPL", "1m", ["rth"])
        check(e == date(2022, 5, 1) and l == date(2024, 8, 31),
              "ticker_span spans 1st-of-earliest-month .. last-of-latest-month")
        sd, ed_ = ed.resolve_range(root, "AAPL", "1m", ["rth"], "furthest")
        check(sd == e and ed_ == l, "resolve_range furthest = the full span")
        sd2, ed2 = ed.resolve_range(root, "AAPL", "1m", ["rth"], "6mo")
        check(ed2 == l and sd2 > date(2024, 1, 1),
              "resolve_range 6mo anchors to the LATEST stored day, clamps start")
        check(ed.ticker_span(root, "ZZZ", "1m", ["rth"]) == (None, None),
              "ticker_span of an absent ticker -> (None, None)")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_estimate_preset():
    base, root = mkbank()
    try:
        seed(root, "AAPL", price_day(date(2020, 1, 2), 10)
             + price_day(date(2024, 8, 1), 10), "1m")
        spec = ed.default_spec()
        e = ed.estimate(root, ["AAPL"], "1m", ["rth"], None, None, spec,
                        preset="furthest")
        check(e["rows"] == 20, "estimate(furthest) counts the whole span")
        e2 = ed.estimate(root, ["AAPL"], "1m", ["rth"], None, None, spec,
                         preset="6mo")
        check(e2["rows"] == 10,
              "estimate(6mo) counts only the recent months near the latest day")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_export_one_csv_roundtrip():
    base, root = mkbank()
    try:
        bars = price_day(date(2024, 1, 2), 5)
        seed(root, "AAPL", bars, "1m")
        out = Path(base) / "AAPL_1m.csv"
        spec = dict(ed.default_spec(), start_date=date(2024, 1, 1),
                    end_date=date(2024, 1, 31))
        res = ed.export_one(root, "AAPL", spec, out)
        check(res["rows"] == 5 and res["bytes"] > 0, "export wrote 5 rows")
        check(res["covered_start"] == "1/2/2024 9:30:00",
              "covered_start is the canonical first-bar stamp")
        lines = out.read_text(encoding="utf-8").split("\n")
        check(lines[0] == "date,time,open,high,low,close,volume",
              "default header on disk uses date/time")
        f = lines[1].split(",")
        check(f[2] == ed.num_str(bars[0][1]) and f[6] == str(bars[0][5]),
              "round-trip: values on disk equal the stored bar, raw")
        check(f[:2] == ["2024-01-02", "09:30:00"]
              and "-05:00" not in lines[1],
              "round-trip: split date/time has no offset suffix")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_export_one_parquet_roundtrip():
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return
    base, root = mkbank()
    try:
        bars = price_day(date(2024, 1, 2), 5)
        seed(root, "AAPL", bars, "1m")
        out = Path(base) / "AAPL_1m.parquet"
        spec = dict(ed.default_spec(), file_type="parquet",
                    start_date=date(2024, 1, 1), end_date=date(2024, 1, 31))
        res = ed.export_one(root, "AAPL", spec, out)
        tbl = pq.read_table(out)
        check(tbl.num_rows == 5 and res["rows"] == 5, "parquet export 5 rows")
        check(tbl.column("open").to_pylist() == [b[1] for b in bars],
              "parquet round-trip: prices lossless")
        check(tbl.column("volume").to_pylist() == [b[5] for b in bars],
              "parquet round-trip: volume exact")
        check(str(tbl.schema.field("date").type) == "date32[day]"
              and str(tbl.schema.field("time").type).startswith("time32["),
              "parquet default uses typed date and time columns")
        check(tbl.column("date").to_pylist()[0] == date(2024, 1, 2)
              and tbl.column("time").to_pylist()[0] == time(9, 30),
              "parquet default date/time values match the stored wall clock")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_sample_rows_and_filename():
    base, root = mkbank()
    try:
        seed(root, "AAPL", price_day(date(2024, 1, 2), 50)
             + price_day(date(2024, 2, 1), 50), "1m")
        spec = dict(ed.default_spec(), start_date=date(2024, 1, 1),
                    end_date=date(2024, 2, 28))
        rows = ed.sample_rows(root, "AAPL", spec, n=10)
        check(len(rows) == 10 and rows[0]["dt"].date() == date(2024, 1, 2),
              "sample_rows returns the first n from the earliest in-range month")
        check(ed.filename_for("aapl", spec) == "AAPL_1m.csv",
              "filename pattern {TICKER}_{interval}.{ext}")
        check(ed.filename_for("aapl", dict(spec, file_type="parquet"))
              == "AAPL_1m.parquet", "parquet extension")
        check(ed.filename_for("aapl", dict(spec, file_type="json"))
              == "AAPL_1m.json", "json extension")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_synthetic_sample_rows_contract():
    spec = dict(ed.default_spec(), columns=list(ed.ALL_COLUMNS))
    before_spec = copy.deepcopy(spec)
    storage_names = ("load_manifest", "read_month_file", "find_month_file")
    originals = {name: getattr(ss, name) for name in storage_names}

    def forbidden_storage(*_args, **_kwargs):
        raise AssertionError("synthetic preview attempted a storage read")

    try:
        for name in storage_names:
            setattr(ss, name, forbidden_storage)
        rows1, tags1 = ed.synthetic_sample_rows(spec)
        rows2, tags2 = ed.synthetic_sample_rows(spec)
    finally:
        for name, value in originals.items():
            setattr(ss, name, value)

    check(spec == before_spec,
          "synthetic sample leaves its caller's spec byte-for-byte equivalent")
    check(rows1 == rows2 and tags1 == tags2 and rows1 is not rows2,
          "synthetic sample is deterministic and returns fresh containers")
    check(len(rows1) == len(tags1) >= 10
          and all(a["dt"] < b["dt"] for a, b in zip(rows1, rows1[1:])),
          "synthetic rows/tags are parallel and datetimes strictly increase")
    _kind_rows, kind_tags = ed.synthetic_sample_rows(
        dict(spec, base_interval="1d-hvol"))
    union = (set().union(*(set(t) for t in tags1))
             | set().union(*(set(t) for t in kind_tags)))
    check(set(ed.REQUIRED_VARIATIONS) <= union,
          "synthetic sample tags cover every declared variation")
    required = {"dt", "open", "high", "low", "close", "volume"}
    check(all(required <= set(row) for row in rows1),
          "every synthetic row has the production render-row OHLCV shape")
    check(all({"ticker", "vwap", "iv", "hvol"} <= set(row)
              for row in rows1),
          "selected optional columns are populated on synthetic rows")
    empty_idx = next(i for i, row_tags in enumerate(tags1)
                     if "empty_value" in row_tags)
    ordered_aux = [c for c in spec["columns"]
                   if c in ("iv", "hvol", "vwap")]
    empty_col = ordered_aux[0]
    check(rows1[empty_idx].get(empty_col) is None
          and all(rows1[empty_idx].get(c) is not None
                  for c in ordered_aux[1:])
          and rows1[empty_idx - 1].get(empty_col) is not None
          and rows1[empty_idx + 1].get(empty_col) is not None,
          "empty_value follows displayed aux order and has populated neighbors")
    custom_aux = ["date", "open", "hvol", "iv", "volume"]
    custom_rows, custom_tags = ed.synthetic_sample_rows(
        dict(ed.default_spec(), columns=custom_aux))
    custom_empty = next(i for i, row_tags in enumerate(custom_tags)
                        if "empty_value" in row_tags)
    check(custom_rows[custom_empty].get("hvol") is None
          and custom_rows[custom_empty].get("iv") is not None,
          "empty_value follows a custom displayed aux order")
    default_rows, _ = ed.synthetic_sample_rows(ed.default_spec())
    check(all(not ({"ticker", "vwap", "iv", "hvol"} & set(row))
              for row in default_rows),
          "unselected optional columns stay out of the synthetic row shape")

    real_spec = dict(ed.default_spec(), columns=list(ed.DEFAULT_COLUMNS))
    real_rows = ed.build_rows(price_day(date(2024, 6, 17), 3), "AAPL",
                              real_spec["columns"])
    export_before = ed.render_rows(real_rows, real_spec)
    ed.synthetic_sample_rows(spec)
    check(ed.render_rows(real_rows, real_spec) == export_before,
          "generating a fake preview cannot change real export bytes")


def test_sample_source_default_and_switch():
    calls = defaultdict(int)
    bars = [(datetime(2024, 6, 17, 9, 30), 100.0, 101.0, 99.0,
             100.5, 50.0)]

    def synthetic(spec):
        calls["synthetic"] += 1
        return ([{"dt": bars[0][0], "open": 1.0, "high": 1.1,
                  "low": 0.9, "close": 1.0, "volume": 1.0}],
                [frozenset({"normal"})])

    def ticker_span(*_args, **_kwargs):
        calls["ticker_span"] += 1
        return date(2024, 6, 17), date(2024, 6, 17)

    def sample_price_bars(*_args, **_kwargs):
        calls["sample_price_bars"] += 1
        return list(bars)

    def build_rows(got_bars, ticker, cols, **_kwargs):
        calls["build_rows"] += 1
        check(got_bars == bars and ticker == "AAPL" and cols == ["open"],
              "Bank data preview keeps the legacy build_rows inputs")
        return [{"dt": bars[0][0], "open": 100.0, "high": 101.0,
                 "low": 99.0, "close": 100.5, "volume": 50.0}]

    preview_engine = SimpleNamespace(
        synthetic_sample_rows=synthetic,
        ticker_span=ticker_span,
        sample_price_bars=sample_price_bars,
        build_rows=build_rows,
    )
    sample_bars_fn, _ = _display_def(
        "_exd_sample_bars",
        {"export_designer": preview_engine,
         "export_csv": SimpleNamespace(
             preset_range=lambda _p, _end, start, end: (start, end))})
    preview_fn, _ = _display_def(
        "_exd_preview_content", {"export_designer": preview_engine})

    host = SimpleNamespace(
        _EXD_SAMPLE_SOURCES=("Sample fake", "Bank data"),
        _exd_sample_source=SimpleNamespace(get=lambda: "Sample fake"),
        _exd_sample_key=None,
        _exd_sample_cache=(None, None, None),
        _exd_sample_span=(None, None),
        _storage_root=Path("must-not-be-read"),
    )

    def sample_ticker():
        calls["sample_ticker"] += 1
        return "AAPL"

    host._exd_sample_ticker = sample_ticker
    host._exd_sample_bars = lambda ticker, spec: sample_bars_fn(
        host, ticker, spec)
    host._exd_issue = lambda level, msg, fix: {
        "level": level, "msg": msg, "fix": fix}
    host._exd_range_text = lambda *_args: "range"
    host._exd_span_text = lambda *_args: "span"

    spec = dict(ed.default_spec(), columns=["open"], range_preset="2yr")
    all_first = preview_fn(host, spec, spec["columns"])
    check(calls == {"synthetic": 1}
          and not all_first["hard_error"] and all_first["ticker"] is None,
          "default Sample fake works without a ticker or any bank call")

    bank_first = preview_fn(host, spec, spec["columns"], "Bank data")
    cached_key = host._exd_sample_key
    cached_value = host._exd_sample_cache
    all_second = preview_fn(host, spec, spec["columns"], "Sample fake")
    bank_second = preview_fn(host, spec, spec["columns"], "Bank data")
    check(calls["synthetic"] == 2 and calls["sample_ticker"] == 2
          and calls["build_rows"] == 2,
          "Fake -> Bank -> Fake -> Bank routes only to the selected source")
    check(calls["ticker_span"] == 1 and calls["sample_price_bars"] == 1,
          "source switching preserves the existing Bank data scope cache")
    check(host._exd_sample_key == cached_key
          and host._exd_sample_cache == cached_value,
          "synthetic previews never poison the bank cache")
    # The raw-bank preview tab was removed 2026-07-31; the surviving contract
    # is that Bank data stays ticker-bound and stable while the synthetic
    # source never binds a ticker or reads the bank.
    check(bank_first["ticker"] == bank_second["ticker"] == "AAPL"
          and bank_first["rows"] == bank_second["rows"]
          and all_second["ticker"] is None,
          "Bank data stays stable while the synthetic preview stays "
          "ticker-free")

    _open_fn, open_src = _display_def("_storage_exd_open")
    full_src = _DISPLAY_DATA.read_text(encoding="utf-8")
    check('_EXD_SAMPLE_SOURCES = ("Sample fake", "Bank data")' in full_src
          and "value=self._EXD_SAMPLE_SOURCES[0]" in open_src,
          "GUI source selector defaults explicitly to Sample fake")


def test_sample_source_export_isolation():
    segments = {}
    for name in ("_exd_spec", "_exd_collect_preset", "_exd_export"):
        _fn, segments[name] = _display_def(name)
    check("_exd_sample_source" not in segments["_exd_spec"],
          "preview source is absent from the production export spec")
    check("_exd_sample_source" not in segments["_exd_collect_preset"],
          "preview source is absent from saved presets")
    check("_exd_sample_source" not in segments["_exd_export"]
          and "synthetic_sample_rows" not in segments["_exd_export"],
          "export action cannot consume the preview source or synthetic rows")
    _render_fn, render_src = _display_def("_exd_render")
    check("self._exd_preview_content(spec, cols)" in render_src,
          "the live render path delegates source selection to the tested seam")
    check("export_designer.estimate(" in render_src
          and "estimate_bytes(rows, files, spec)" in render_src,
          "preview estimate remains bank-backed and source-agnostic")


def test_bottom_controls_reserved_before_expanding_body():
    """The Tk pack order must reserve the action row before the body expands."""
    _open_fn, src = _display_def("_storage_exd_open")
    anchors = {
        "bottom": src.find("bottom = ttk.Frame(win)"),
        "bottom_pack": src.find("bottom.pack(side=tk.BOTTOM, fill=tk.X)"),
        "progress": src.find("self._export_progress_surface("),
        "actions": src.find("btm = ttk.Frame(bottom"),
        "export": src.find("self._exd_export_btn = ttk.Button"),
        "upper": src.find("upper = ttk.Frame(win, padding=8)"),
        "upper_pack": src.find(
            "upper.pack(side=tk.TOP, fill=tk.BOTH, expand=True)"),
    }
    check(all(pos >= 0 for pos in anchors.values())
          and list(anchors.values()) == sorted(anchors.values()),
          "bottom progress/actions reserve space before the expanding body")
    check('self._export_progress_surface(\n            bottom, "exd"' in src
          and "btm = ttk.Frame(bottom, padding=(8, 6))" in src
          and "btm = ttk.Frame(win" not in src,
          "progress and action rows share the bottom-reserved container")
    check(src.count("self._exd_export_btn = ttk.Button") == 1
          and "self._exd_export_btn.pack(side=tk.RIGHT)" in src,
          "the single Export action remains right-packed in the reserved row")
    check('win.geometry("1000x700")' in src
          and "win.minsize(900, 600)" in src,
          "Export Designer opens tall but remains resizable to the short-height contract")


def _exercise_export_grid():
    width_fn, _ = _display_def("_exd_grid_widths")
    cell_calls = []
    width_calls = []

    def cell(col, row, spec):
        cell_calls.append((col, row["dt"]))
        return ed._cell(col, row, spec)

    def widths(cols, rendered, spec):
        width_calls.append((list(cols), copy.deepcopy(rendered)))
        return width_fn(cols, rendered, spec)

    grid_engine = SimpleNamespace(COLUMN_LABEL=ed.COLUMN_LABEL, _cell=cell)
    grid_fn, _ = _display_def(
        "_exd_set_export_grid",
        {"export_designer": grid_engine,
         "_exd_grid_widths": widths,
         "tk": SimpleNamespace(W="w", E="e", END="end")})

    class FakeTree:
        def __init__(self):
            self.show = None
            self.columns = []
            self.headings = {}
            self.widths = {}
            self.inserted = []

        def configure(self, **kwargs):
            self.show = kwargs.get("show", self.show)

        def get_children(self):
            return tuple(range(len(self.inserted)))

        def delete(self, *_items):
            self.inserted = []

        def __setitem__(self, key, value):
            if key == "columns":
                self.columns = list(value)

        def heading(self, col, **kwargs):
            self.headings[col] = kwargs

        def column(self, col, **kwargs):
            self.widths[col] = kwargs["width"]

        def insert(self, _parent, _where, **kwargs):
            self.inserted.append(list(kwargs.get("values", ())))

    class FakeLabel:
        def __init__(self):
            self.text = None

        def config(self, **kwargs):
            self.text = kwargs.get("text")

    tv = FakeTree()
    host = SimpleNamespace(_exd_sample=tv, _exd_sample_msg=FakeLabel(),
                           empty_calls=[])
    host._exd_set_empty_tree = lambda _tv, msg: host.empty_calls.append(msg)
    cols = ["timestamp", "date", "time", "open"]
    rows = [
        {"dt": datetime(2024, 6, 17, 9, 30), "open": 123.45,
         "high": 124.0, "low": 122.0, "close": 123.0, "volume": 1},
        {"dt": datetime(2024, 6, 17, 9, 31), "open": 123.55,
         "high": 124.0, "low": 122.0, "close": 123.0, "volume": 1},
    ]
    spec = dict(ed.default_spec(), columns=cols, header=False,
                date_format="iso", timezone="America/New_York")
    grid_fn(host, rows, cols, spec, "sample")
    first_inserted = copy.deepcopy(tv.inserted)
    first_widths = dict(tv.widths)
    first_show = tv.show
    grid_fn(host, rows, cols, dict(spec, header=True), "sample")
    second_show = tv.show
    calls_after_rows = len(cell_calls)
    grid_fn(host, [], cols, dict(spec, header=False), "empty")
    empty_hidden = tv.show
    grid_fn(host, [], cols, dict(spec, header=True), "empty")
    empty_shown = tv.show
    return {
        "width_fn": width_fn,
        "cols": cols,
        "spec": spec,
        "cell_calls": cell_calls,
        "width_calls": width_calls,
        "first_inserted": first_inserted,
        "first_widths": first_widths,
        "first_show": first_show,
        "second_show": second_show,
        "calls_after_rows": calls_after_rows,
        "empty_hidden": empty_hidden,
        "empty_shown": empty_shown,
        "empty_calls": host.empty_calls,
    }


def test_export_grid_header_roundtrip():
    got = _exercise_export_grid()
    check(got["first_show"] == "" and got["second_show"] == "headings",
          "header False -> True hides and then restores populated headings")
    check(got["empty_hidden"] == "" and got["empty_shown"] == "headings"
          and len(got["empty_calls"]) == 2,
          "header state also round-trips across empty/error-style grid paints")


def test_export_grid_width_runtime():
    got = _exercise_export_grid()
    check(got["calls_after_rows"] == 2 * 2 * 4
          and len(got["cell_calls"]) == got["calls_after_rows"],
          "each populated grid paint formats every cell exactly once")
    check(len(got["width_calls"]) == 2
          and got["width_calls"][0][1] == got["first_inserted"],
          "the exact rendered matrix feeds both width calculation and insertion")
    check(got["first_widths"] == {
        "timestamp": 226, "date": 106, "time": 90, "open": 58},
          "ISO/NY rendered content widens date-bearing columns only")
    empty = got["width_fn"](got["cols"], [], got["spec"])
    ragged = got["width_fn"](got["cols"], [["x"], []], got["spec"])
    check(empty == {"timestamp": 156, "date": 58, "time": 58, "open": 58}
          and ragged["timestamp"] == 156 and ragged["open"] == 58,
          "width helper keeps legacy floors for empty and ragged inputs")


def test_export_full_pipeline_with_aux_columns():
    """Everything at once: merged sessions + ticker + vwap + forward-filled IV
    + reordered columns, exported and parsed back."""
    base, root = mkbank()
    try:
        d = date(2024, 1, 2)
        seed(root, "AAPL", price_day(d, 4), "1m")
        seed(root, "AAPL", iv_daily([d], [0.30]), "1d-iv")
        out = Path(base) / "AAPL_full.csv"
        cols = ["ticker", "timestamp", "close", "vwap", "iv", "volume"]
        spec = dict(ed.default_spec(), columns=cols,
                    start_date=d, end_date=d)
        res = ed.export_one(root, "AAPL", spec, out)
        lines = out.read_text(encoding="utf-8").split("\n")
        check(lines[0] == ",".join(ed.COLUMN_LABEL[c] for c in cols),
              "full pipeline: header reflects custom column order (labels)")
        first = lines[1].split(",")
        check(first[0] == "AAPL", "ticker column present")
        check(first[4] == "0.3", "forward-filled IV present on the minute row")
        check(res["rows"] == 4, "full pipeline exported all 4 rows")
        check(res["source_intervals"] == ["1d-iv", "1m"],
              "export result records its price and daily-IV source intervals")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_first_bars_midmonth_matches_export():
    """Bug-hunt #1 regression: when the earliest in-range month holds only
    out-of-window bars (a mid-month start), the live sample must still show the
    rows the export writes — _first_bars trims INSIDE the loop now."""
    base, root = mkbank()
    try:
        # Jan bars on days 2/3/4 (before a Jan-15 start) + Feb in-range bars
        seed(root, "AAA", eds_jan(), "1m")
        seed(root, "AAA", price_day(date(2024, 2, 1), 20), "1m")
        sd, ed_ = date(2024, 1, 15), date(2024, 2, 28)
        fb = ed._first_bars(root, "AAA", "1m", ["rth"], sd, ed_, 12)
        merged, _ = ed.read_merged_bars(root, "AAA", "1m", ["rth"], sd, ed_)
        check(len(fb) == 12 and fb == merged[:12],
              "live sample no longer falsely empties when the start is mid-month")
        check(all(b[0].date() >= date(2024, 1, 15) for b in fb),
              "sample rows are all genuinely in-range")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def eds_jan():
    return (price_day(date(2024, 1, 2), 10) + price_day(date(2024, 1, 3), 10)
            + price_day(date(2024, 1, 4), 10))


def _equiv_bank():
    base, root = mkbank()
    px = (price_day(date(2024, 1, 2), 8) + price_day(date(2024, 1, 3), 8)
          + price_day(date(2024, 2, 1), 8))
    seed(root, "AAPL", px, "1m")
    pre = [(datetime.combine(date(2024, 1, 2), time(9, 15 + i)), 99.0, 99.1,
            98.9, 99.0, 5) for i in range(3)]
    seed(root, "AAPL", pre, "1m-pre")
    seed(root, "AAPL", iv_daily([date(2024, 1, 2), date(2024, 2, 1)],
                                [0.22, 0.28]), "1d-iv")
    return base, root


def test_streaming_byte_equivalence():
    """Streamed export_one must be byte-for-byte the in-memory render over the
    whole series (the property the live sample relies on), exercised with merged
    sessions + vwap + forward-filled IV across a month boundary."""
    base, root = _equiv_bank()
    try:
        cols = ["ticker", "timestamp", "open", "high", "low", "close",
                "volume", "vwap", "iv"]
        sd, ed_ = date(2024, 1, 1), date(2024, 2, 28)
        bars, _ = ed.read_merged_bars(root, "AAPL", "1m", ["rth", "pre"], sd, ed_)
        ref_rows = ed.build_rows(bars, "AAPL", cols, root=root, base_iv="1m")
        for ft in ("csv", "tsv", "json"):
            spec = dict(ed.default_spec(), columns=cols, file_type=ft,
                        sessions=["rth", "pre"], start_date=sd, end_date=ed_)
            ref = ed.render_rows(ref_rows, spec)
            out = Path(base) / f"eq.{ft}"
            res = ed.export_one(root, "AAPL", spec, out)
            disk = out.read_bytes()
            check(disk == ref,
                  f"streamed {ft} export is byte-identical to the whole-series render")
            check(res["rows"] == len(ref_rows),
                  f"streamed {ft} reports the full row count")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_storage_format_matches_export_csv():
    """E1 regression: the legacy Export-file CSV route now goes through
    export_designer.export_one, but the bytes must stay canonical export_csv
    bytes."""
    base, root = _equiv_bank()
    try:
        sd, ed_ = date(2024, 1, 1), date(2024, 2, 28)
        old = Path(base) / "old.csv"
        new = Path(base) / "new.csv"
        old_res = ec.export_combined_csv(root, "AAPL", "1m", sd, ed_, old)
        spec = dict(ed.default_spec(), base_interval="1m", sessions=["rth"],
                    start_date=sd, end_date=ed_, file_type="csv",
                    storage_format=True)
        new_res = ed.export_one(root, "AAPL", spec, new)
        check(new.read_bytes() == old.read_bytes(),
              "storage_format csv is byte-identical to export_csv combined output")
        check(new_res["rows"] == old_res["rows"]
              and new_res["holes"] == [f"rth:{h}" for h in old_res["holes"]],
              "storage_format csv preserves row count and session-tagged holes")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_storage_format_sessions_match_export_csv():
    base, root = _equiv_bank()
    try:
        sd, ed_ = date(2024, 1, 1), date(2024, 2, 28)
        old = Path(base) / "old_sessions.csv"
        new = Path(base) / "new_sessions.csv"
        old_res = ec.export_sessions_csv(
            root, "AAPL", "1m", ["rth", "pre", "post"], sd, ed_, old)
        spec = dict(ed.default_spec(), base_interval="1m",
                    sessions=["rth", "pre", "post"], start_date=sd,
                    end_date=ed_, file_type="csv", storage_format=True)
        new_res = ed.export_one(root, "AAPL", spec, new)
        check(new.read_bytes() == old.read_bytes(),
              "storage_format csv is byte-identical for merged session export")
        check(new_res["per_session"] == old_res["per_session"],
              "storage_format csv preserves per-session row counts")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_storage_format_empty_header_matches_export_csv():
    base, root = mkbank()
    try:
        seed(root, "AAPL", price_day(date(2024, 1, 2), 5), "1m")
        old = Path(base) / "old_empty.csv"
        new = Path(base) / "new_empty.csv"
        sd, ed_ = date(2023, 1, 1), date(2023, 1, 31)
        ec.export_combined_csv(root, "AAPL", "1m", sd, ed_, old)
        spec = dict(ed.default_spec(), base_interval="1m", sessions=["rth"],
                    start_date=sd, end_date=ed_, file_type="csv",
                    storage_format=True)
        res = ed.export_one(root, "AAPL", spec, new)
        check(new.read_bytes() == old.read_bytes() == (ss.HEADER + ss.EOL).encode("ascii"),
              "storage_format csv preserves legacy header-only empty export")
        check(res["rows"] == 0 and res.get("out_path") == str(new),
              "storage_format empty export still writes the legacy file")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_streaming_parquet_multimonth():
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return
    base, root = _equiv_bank()
    try:
        cols = ["timestamp", "open", "close", "volume", "iv"]
        sd, ed_ = date(2024, 1, 1), date(2024, 2, 28)
        bars, _ = ed.read_merged_bars(root, "AAPL", "1m", ["rth"], sd, ed_)
        spec = dict(ed.default_spec(), columns=cols, file_type="parquet",
                    sessions=["rth"], start_date=sd, end_date=ed_, empty="null")
        out = Path(base) / "eq.parquet"
        res = ed.export_one(root, "AAPL", spec, out)
        tbl = pq.read_table(out)
        check(tbl.num_rows == len(bars) == res["rows"],
              "streamed parquet has every row across both months (row groups)")
        check(tbl.column("close").to_pylist() == [b[4] for b in bars],
              "streamed parquet values are lossless and in order")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def _parquet_sig(path):
    import pyarrow.parquet as pq
    tbl = pq.read_table(path)
    return ([str(f.type) for f in tbl.schema],
            list(tbl.column_names),
            {name: tbl.column(name).to_pylist() for name in tbl.column_names})


def test_parquet_fast_matches_classic_readback():
    try:
        import pyarrow.parquet  # noqa: F401
    except ImportError:
        return
    base, root = _equiv_bank()
    try:
        # Add an intraday HVOL series so the parquet vector path exercises both
        # daily forward-fill and exact timestamp aux joins.
        px, _holes = ed.read_merged_bars(
            root, "AAPL", "1m", ["rth"],
            date(2024, 1, 1), date(2024, 2, 28))
        seed(root, "AAPL", [(b[0], 0.18, 0.18, 0.18, 0.18, 0)
                            for b in px], "1m-hvol")
        specs = [
            dict(ed.default_spec(), file_type="parquet", timezone="UTC",
                 sessions=["rth", "pre"], start_date=date(2024, 1, 1),
                 end_date=date(2024, 2, 28)),
            dict(ed.default_spec(), file_type="parquet",
                 columns=["ticker", "timestamp", "open", "close", "volume",
                          "vwap", "iv", "hvol"],
                 sessions=["rth", "pre"],
                 start_date=date(2024, 1, 1), end_date=date(2024, 2, 28),
                 empty="null"),
            dict(ed.default_spec(), file_type="parquet",
                 columns=["timestamp", "close", "iv"],
                 date_format="epoch", sessions=["rth"],
                 start_date=date(2024, 1, 1), end_date=date(2024, 2, 28),
                 empty="zero"),
            dict(ed.default_spec(), file_type="parquet",
                 columns=["timestamp", "close"], date_format="date-only",
                 timezone="UTC", sessions=["rth"],
                 start_date=date(2024, 1, 1), end_date=date(2024, 2, 28)),
        ]
        old_fast = ed.FAST_RENDER
        try:
            for i, spec in enumerate(specs):
                classic = Path(base) / f"classic_{i}.parquet"
                fast = Path(base) / f"fast_{i}.parquet"
                ed.FAST_RENDER = False
                rc = ed.export_one(root, "AAPL", spec, classic)
                ed.FAST_RENDER = True
                rf = ed.export_one(root, "AAPL", spec, fast)
                check(rf["rows"] == rc["rows"],
                      f"parquet fast reports classic row count ({i})")
                check(_parquet_sig(fast) == _parquet_sig(classic),
                      f"parquet fast read-back equals classic path ({i})")
        finally:
            ed.FAST_RENDER = old_fast
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_parquet_fast_gate_and_merge_unification():
    if ed.np is None:
        return
    check(ed._parquet_fast_ok(dict(ed.default_spec(), file_type="parquet"),
                              "AAPL"),
          "parquet fast gate accepts default parquet")
    check(not ed._parquet_fast_ok(
          dict(ed.default_spec(), file_type="parquet", date_format="custom"),
          "AAPL"),
          "parquet custom date stays on classic path")
    check(not ed._parquet_fast_ok(dict(ed.default_spec(), file_type="json"),
                                  "AAPL"),
          "json stays on classic path")
    ts = ed.np.array([datetime(2024, 1, 2, 9, 31),
                      datetime(2024, 1, 2, 9, 30),
                      datetime(2024, 1, 2, 9, 31)],
                     dtype="datetime64[s]")
    o = ed.np.array([2.0, 1.0, 3.0])
    h = o + 0.1
    lo = o - 0.1
    c = o + 0.01
    v = ed.np.array([20, 10, 30], dtype=ed.np.int64)
    mts, mo, _mh, _ml, _mc, mv = ed._merge_sessions_fast([(ts, o, h, lo, c, v)])
    check(mts.tolist() == [datetime(2024, 1, 2, 9, 30),
                           datetime(2024, 1, 2, 9, 31)]
          and mo.tolist() == [1.0, 2.0] and mv.tolist() == [10, 20],
          "fast merge sorts/dedupes a single session like the classic path")


def test_default_date_time_csv_fast_matches_classic():
    base, root = _equiv_bank()
    try:
        specs = [
            dict(ed.default_spec(), start_date=date(2024, 1, 1),
                 end_date=date(2024, 2, 28)),
            dict(ed.default_spec(), timezone="UTC",
                 start_date=date(2024, 1, 1), end_date=date(2024, 2, 28)),
        ]
        old_fast = ed.FAST_RENDER
        try:
            for i, spec in enumerate(specs):
                classic = Path(base) / f"date_time_classic_{i}.csv"
                fast = Path(base) / f"date_time_fast_{i}.csv"
                ed.FAST_RENDER = False
                ed.export_one(root, "AAPL", spec, classic)
                ed.FAST_RENDER = True
                ed.export_one(root, "AAPL", spec, fast)
                payload = fast.read_bytes()
                check(payload == classic.read_bytes(),
                      f"default date/time fast bytes equal classic ({i})")
                check(b"-04:00" not in payload and b"-05:00" not in payload
                      and b"+00:00" not in payload,
                      f"default date/time export has no offset suffix ({i})")
        finally:
            ed.FAST_RENDER = old_fast
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_windowed_kind_series_read():
    base, root = mkbank()
    try:
        seed(root, "FISV", iv_daily([
            date(2023, 12, 29), date(2024, 1, 31),
            date(2024, 2, 20), date(2024, 3, 5)],
            [0.20, 0.25, 0.31, 0.40]), "1d-iv")
        bars, mode = ed.read_kind_series(
            root, "FISV", "1m", "iv",
            start_date=date(2024, 2, 10), end_date=date(2024, 2, 28))
        check(mode == "daily", "windowed kind read chooses daily IV fallback")
        check([b[0].date() for b in bars] == [date(2024, 1, 31),
                                              date(2024, 2, 20)],
              "windowed kind read keeps one look-back month and skips outside months")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_windowed_kind_export_byte_identity():
    """Batch C regression: the optimized windowed IV/HVOL read must render the
    same bytes as the old full-read aux join for a mid-history window. The Jan
    IV value is the forward-fill seed for early February rows."""
    base, root = mkbank()
    try:
        prices = (price_day(date(2024, 2, 12), 4)
                  + price_day(date(2024, 2, 20), 4, base=101.0))
        seed(root, "FISV", prices, "1m")
        seed(root, "FISV", iv_daily([
            date(2023, 12, 29), date(2024, 1, 31),
            date(2024, 2, 20), date(2024, 3, 5)],
            [0.20, 0.25, 0.31, 0.40]), "1d-iv")
        hvol = [(b[0], 0.18 + i * 0.001, 0.18 + i * 0.001,
                 0.18 + i * 0.001, 0.18 + i * 0.001, 0)
                for i, b in enumerate(prices)]
        seed(root, "FISV", hvol, "1m-hvol")
        cols = ["timestamp", "close", "iv", "hvol", "vwap"]
        sd, ed_ = date(2024, 2, 10), date(2024, 2, 28)
        bars, _holes = ed.read_merged_bars(root, "FISV", "1m", ["rth"],
                                           sd, ed_)
        full_aux = ed._aux_lookups(root, "FISV", "1m", cols)
        ref_rows = ed.build_rows_aux(bars, "FISV", cols, full_aux)
        spec = dict(ed.default_spec(), columns=cols, start_date=sd,
                    end_date=ed_, file_type="csv")
        ref = ed.render_rows(ref_rows, spec)
        out = Path(base) / "fisv_window.csv"
        res = ed.export_one(root, "FISV", spec, out)
        disk = out.read_bytes()
        check(disk == ref,
              "windowed kind export is byte-identical to full-read aux join")
        first = disk.decode("utf-8").splitlines()[1].split(",")
        check(first[2] == "0.25" and res["rows"] == len(ref_rows),
              "one-month look-back seeds early-window IV forward-fill")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_windowed_kind_seed_walkback_long_hole():
    """Required Batch-C follow-up: a daily ratio series can have a long interior
    hole, so the seed must walk back to the latest present month, not just one
    calendar month."""
    base, root = mkbank()
    try:
        prices = price_day(date(2017, 6, 15), 4)
        seed(root, "WBD", prices, "1m")
        seed(root, "WBD", iv_daily([date(2016, 6, 30), date(2017, 11, 1)],
                                   [0.42, 0.55]), "1d-hvol")
        cols = ["timestamp", "close", "hvol"]
        sd, ed_ = date(2017, 6, 1), date(2017, 6, 30)
        bars, _holes = ed.read_merged_bars(root, "WBD", "1m", ["rth"], sd, ed_)
        full_aux = ed._aux_lookups(root, "WBD", "1m", cols)
        ref = ed.render_rows(ed.build_rows_aux(bars, "WBD", cols, full_aux),
                             dict(ed.default_spec(), columns=cols,
                                  start_date=sd, end_date=ed_))
        out = Path(base) / "wbd_hole.csv"
        res = ed.export_one(
            root, "WBD",
            dict(ed.default_spec(), columns=cols, start_date=sd,
                 end_date=ed_, file_type="csv"),
            out)
        kb, mode = ed.read_kind_series(root, "WBD", "1m", "hvol",
                                       start_date=sd, end_date=ed_)
        check(mode == "daily"
              and [b[0].date() for b in kb] == [date(2016, 6, 30)],
              "seed walk-back reads the latest prior present daily-kind month only")
        check(out.read_bytes() == ref and res["rows"] == len(bars),
              "WBD-style long-hole window is byte-identical to full-read aux join")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_export_clears_numstr_cache_per_file():
    base, root = mkbank()
    try:
        seed(root, "AAPL", price_day(date(2024, 1, 2), 5), "1m")
        ed._numstr_cache.clear()
        ed._numstr_cache[123] = "sentinel"
        spec = dict(ed.default_spec(), start_date=date(2024, 1, 1),
                    end_date=date(2024, 1, 31))
        ed.export_one(root, "AAPL", spec, Path(base) / "AAPL_cache.csv")
        check(123 not in ed._numstr_cache,
              "export_one clears the numeric formatter cache at file start")
        check(len(ed._numstr_cache) <= ed._NUMSTR_CACHE_MAX,
              "numeric formatter cache remains bounded")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_export_one_default_spec_resolves_dates():
    base, root = mkbank()
    try:
        bars = price_day(date(2024, 1, 2), 5)
        seed(root, "AAPL", bars, "1m")
        out = Path(base) / "AAPL_default.csv"
        res = ed.export_one(root, "AAPL", ed.default_spec(), out)
        check(res["rows"] == 5 and out.exists(),
              "export_one(default_spec) derives the ticker coverage dates")
        check(out.read_text(encoding="utf-8").splitlines()[1]
              == "2024-01-02,09:30:00,100,100.05,99.95,100.02,100",
              "default-spec export uses separate suffix-free date/time")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_export_progress_month_boundaries():
    base, root = mkbank()
    try:
        bars = price_day(date(2024, 1, 2), 2) + price_day(
            date(2024, 2, 2), 2, base=101.0)
        seed(root, "AAPL", bars, "1m")
        seen = []
        spec = dict(ed.default_spec(), start_date=date(2024, 1, 1),
                    end_date=date(2024, 2, 29))
        res = ed.export_one(root, "AAPL", spec, Path(base) / "AAPL_prog.csv",
                            progress=seen.append)
        got = [(e["ticker"], e["month_index"], e["month_total"], e["month"])
               for e in seen]
        check(res["rows"] == len(bars), "progress export writes all rows")
        check(got == [("AAPL", 1, 2, "2024-01"),
                      ("AAPL", 2, 2, "2024-02")],
              "export progress reports ticker and month i/N boundaries")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_export_cancel_month_boundary_preserves_target():
    base, root = mkbank()
    try:
        bars = price_day(date(2024, 1, 2), 2) + price_day(
            date(2024, 2, 2), 2, base=101.0)
        seed(root, "AAPL", bars, "1m")
        out = Path(base) / "AAPL_cancel.csv"
        out.write_text("SENTINEL\n", encoding="utf-8")
        seen = []
        stop = [False]

        def progress(info):
            seen.append(info["month"])
            if info["month"] == "2024-01":
                stop[0] = True

        spec = dict(ed.default_spec(), start_date=date(2024, 1, 1),
                    end_date=date(2024, 2, 29))
        cancelled = False
        try:
            ed.export_one(root, "AAPL", spec, out, progress=progress,
                          cancel=lambda: stop[0])
        except ed.ExportCancelled:
            cancelled = True
        check(cancelled, "export cancel raises ExportCancelled")
        check(seen == ["2024-01"],
              "cancel is observed before the next month starts")
        check(out.read_text(encoding="utf-8") == "SENTINEL\n",
              "cancelled export leaves the previous target untouched")
        check(not list(Path(base).glob("AAPL_cancel.csv.*.tmp")),
              "cancelled export removes the same-dir temp file")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_export_zero_rows_no_clobber():
    """Bug-hunt #5 regression: a present ticker that resolves to zero in-range
    rows must NOT write a header-only stub over an existing file."""
    base, root = mkbank()
    try:
        seed(root, "AAPL", price_day(date(2024, 1, 2), 5), "1m")
        out = Path(base) / "AAPL_1m.csv"
        out.write_text("SENTINEL — do not clobber\n", encoding="utf-8")
        spec = dict(ed.default_spec(), start_date=date(2099, 1, 1),
                    end_date=date(2099, 12, 31))
        res = ed.export_one(root, "AAPL", spec, out)
        check(res["rows"] == 0 and res.get("out_path") is None
              and res.get("skipped"), "zero-row export reports skipped, writes nothing")
        check(out.read_text(encoding="utf-8") == "SENTINEL — do not clobber\n",
              "an existing file is left UNTOUCHED when there is nothing to export")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_estimate_none_guard():
    """Bug-hunt #6 regression: estimate with a None fixed window and no preset
    contributes nothing instead of raising AttributeError."""
    base, root = mkbank()
    try:
        seed(root, "AAPL", price_day(date(2024, 1, 2), 5), "1m")
        e = ed.estimate(root, ["AAPL"], "1m", ["rth"], None, None,
                        ed.default_spec())          # no preset, None window
        check(e == {"files": 0, "rows": 0, "bytes": 0},
              "estimate(None, None, no preset) returns zeros, never crashes")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_estimate_bytes_pure():
    spec = ed.default_spec()
    b = ed.estimate_bytes(1000, 3, spec)
    check(b > 0, "estimate_bytes is a positive pure function of rows/files/spec")
    check(ed.estimate_bytes(0, 0, spec) == 0, "estimate_bytes(0,0) == 0")


def main():
    for _k, t in sorted((k, v) for k, v in globals().items()
                        if k.startswith("test_")):
        t()
    total = _PASS[0] + _FAIL[0]
    print(f"\nexport_designer_selftest: {_PASS[0]}/{total} passed, "
          f"{_FAIL[0]} failed")
    return 1 if _FAIL[0] else 0


if __name__ == "__main__":
    sys.exit(main())
