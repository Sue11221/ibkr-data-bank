"""Self-tests for stock_ingest.py (Tier 1 mixed-bag ingest).

Standalone, no test framework: python stock_ingest_selftest.py
Every check prints PASS/FAIL; exit code 1 on any failure.
Fixtures are tiny synthetic files modeled on the REAL raw folder:
native CRLF CSVs, the '3y 1s'-style ISO+symbol files (whose names lie
about the interval), IBKR pandas dumps with tz offsets and float
volumes, and a real-1s parquet with 16:00:00 bars.
"""

import csv as _csv
import json
import os
import pickle
import re
import shutil
import sys
import tempfile
import threading
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss   # noqa: E402
import stock_ingest as si    # noqa: E402
import stock_basis as sb     # noqa: E402
import operation_gate        # noqa: E402
import vol_value_audit as vva  # noqa: E402

FAILS = []
N = [0]


def check(name, cond, detail=""):
    N[0] += 1
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"  -- {detail}" if detail and not cond
                               else ""))
    if not cond:
        FAILS.append(name)


def sandbox():
    d = Path(tempfile.mkdtemp(prefix="ingest_st_"))
    return d, d / "src", d / ss.STORAGE_DIR_NAME


def fresh():
    d, src, root = sandbox()
    src.mkdir()
    return d, src, root


def native_lines(bars):
    return [ss.HEADER] + [ss.format_bar(*b) for b in bars]


def write_native(path, bars):
    path.write_bytes((ss.EOL.join(native_lines(bars)) + ss.EOL)
                     .encode("ascii"))


def mk_bars(y, mo, d, n, base=100.0, h0=9, m0=30, step_s=60, vol=1000):
    out = []
    t = datetime(y, mo, d, h0, m0, 0)
    for i in range(n):
        p = base + i * 0.01
        out.append((t, p, p + 0.05, p - 0.05, p + 0.01, vol + i))
        t = t + timedelta(seconds=step_s)
    return out


def run(paths, root, **kw):
    return si.ingest_paths(paths, root, **kw)


def month_path(root, tic, y, m, iv):
    return ss.month_file_path(root, tic, y, m, iv)


def file_rec(rep, name):
    for r in rep["files"]:
        if Path(r["path"]).name == name:
            return r
    return None


def series_of(rec, ticker=None):
    out = [s for s in rec.get("series", [])
           if ticker is None or s.get("ticker") == ticker]
    return out[0] if len(out) == 1 else out


# --- section [9] helpers: parallel-vs-serial equivalence ----------------------
_RUN_RE = re.compile(r"(?:ing|ver)-\d{8}-\d{6}-\d+(?:-\d+)?")


def norm_text(text, root, src):
    text = _RUN_RE.sub("<RUN>", text)
    text = re.sub(r'"mtime_ns"\s*:\s*\d+', '"mtime_ns":0', text)
    # save_manifest uses compact JSON; older pretty-printed fixtures placed a
    # space after the colon.  Both spellings carry the same volatile clock.
    return text.replace(str(root), "<ROOT>").replace(str(src), "<SRC>")


def norm_report(rep, root, src):
    """Comparison-ready deep copy: volatile keys out, run-ids and
    sandbox paths normalized."""
    rep = json.loads(json.dumps(rep, default=str))
    for k in ("run", "started", "seconds", "report_path", "conflict_log",
              "parallel"):
        rep.pop(k, None)
    if isinstance(rep.get("preflight"), dict):
        rep["preflight"].pop("free_bytes", None)

    def walk(x):
        if isinstance(x, str):
            return norm_text(x, root, src)
        if isinstance(x, list):
            return [walk(v) for v in x]
        if isinstance(x, dict):
            return {k: walk(v) for k, v in x.items()
                    if k != "mtime_ns"}        # wall clock, never equal
        return x
    return walk(rep)


def tree_snapshot(root, src):
    """{normalized relpath: normalized bytes}, _ingest_reports excluded
    (covered by norm_report)."""
    snap = {}
    root = Path(root)
    if not root.exists():
        return snap
    for f in sorted(root.rglob("*")):
        if not f.is_file():
            continue
        rel = f.relative_to(root).as_posix()
        if rel.startswith(si.REPORTS_DIR):
            continue
        data = f.read_bytes()
        try:
            data = norm_text(data.decode("utf-8"), root,
                             src).encode("utf-8")
        except UnicodeDecodeError:
            pass
        snap[_RUN_RE.sub("<RUN>", rel)] = data
    return snap


class parallel_env:
    """Force the MP path on tiny corpora: INGEST_WORKERS=n + zeroed
    thresholds, restored on exit. The PARENT decides MP and runs the
    commits, so patching parent module attributes is enough."""

    def __init__(self, n):
        self.n = n

    def __enter__(self):
        self.env = os.environ.get(si.INGEST_WORKERS_ENV)
        self.mf, self.mb = si.INGEST_MP_MIN_FILES, si.INGEST_MP_MIN_BYTES
        os.environ[si.INGEST_WORKERS_ENV] = str(self.n)
        si.INGEST_MP_MIN_FILES = 0
        si.INGEST_MP_MIN_BYTES = 0
        return self

    def __exit__(self, *exc):
        if self.env is None:
            os.environ.pop(si.INGEST_WORKERS_ENV, None)
        else:
            os.environ[si.INGEST_WORKERS_ENV] = self.env
        si.INGEST_MP_MIN_FILES, si.INGEST_MP_MIN_BYTES = self.mf, self.mb
        return False


def build_p9_corpus(src):
    """Mixed corpus exercising ordering, gating, conflicts, quarantine,
    evidence and junk paths — built identically for every run."""
    write_native(src / "PNA_2024_01_1m.csv", mk_bars(2024, 1, 15, 60, 10.0))
    write_native(src / "PNA_2024_02_1m.csv", mk_bars(2024, 2, 19, 60, 11.0))
    write_native(src / "PNB_2024_01_1m.csv", mk_bars(2024, 1, 15, 60, 20.0))
    write_native(src / "PNB_2024_02_1m.csv", mk_bars(2024, 2, 19, 60, 21.0))
    write_native(src / "PNC_2024_01_1m.csv", mk_bars(2024, 1, 15, 60, 30.0))
    write_native(src / "PNC_2024_02_1m.csv", mk_bars(2024, 2, 19, 60, 31.0))
    # same-series pair: the SECOND file's dry-run must see the first's
    # committed bars (ordering contract)
    write_native(src / "PAA_a_1m.csv", mk_bars(2024, 3, 18, 10, 40.0))
    write_native(src / "PAA_b_1m.csv",
                 mk_bars(2024, 3, 18, 6, 40.10, h0=9, m0=40))
    # same-run basis-gate pair: identical stamps, 10x prices -> the
    # LATER file must be the gated one
    bars_pbb = mk_bars(2024, 3, 18, 150, 50.0)
    write_native(src / "PBB_a_1m.csv", bars_pbb)
    write_native(src / "PBB_b_1m.csv",
                 [(t, o * 10, h * 10, lo * 10, c * 10, v)
                  for t, o, h, lo, c, v in bars_pbb])
    # conflict pair (50 overlaps < gate min): existing wins, logged
    bars_pcc = mk_bars(2024, 3, 18, 50, 60.0)
    write_native(src / "PCC_a_1m.csv", bars_pcc)
    write_native(src / "PCC_b_1m.csv",
                 [(t, o, h, lo, c, v + 7)
                  for t, o, h, lo, c, v in bars_pcc])
    # >40% non-RTH -> quarantine
    write_native(src / "PDD_ext_1m.csv",
                 mk_bars(2024, 3, 18, 40, 70.0, h0=4, m0=0))
    # no derivable ticker -> needs-decision
    (src / "data.csv").write_text(
        "datetime,open,high,low,close,volume\n"
        "2024-03-04 09:30:00,1,1.1,0.9,1,5\n")
    # <5% invalid rows -> ok + rejected-rows evidence
    good = native_lines(mk_bars(2024, 3, 18, 58, 80.0))
    bad = ["03/18/2024,11:00:00,5,4,6,5,10",     # high < low
           "03/18/2024,11:01:00,-1,1,1,1,10"]    # negative price
    (src / "PEE_x_1m.csv").write_bytes(
        (ss.EOL.join(good + bad) + ss.EOL).encode("ascii"))
    (src / "PFF_empty_1m.csv").write_bytes(b"")
    (src / "junk_noheader.csv").write_bytes(b"1,2,3\n4,5,6\n")
    write_native(src / "PGG_2024_01_1m.txt", mk_bars(2024, 1, 15, 30, 90.0))


def _run():
    print("=== [1] format flavors ============================================")
    d, src, root = fresh()
    # -- native CRLF M/D/YYYY spanning two months
    bars_jan = mk_bars(2024, 1, 15, 30, base=50.0)     # Mon
    bars_feb = mk_bars(2024, 2, 19, 20, base=60.0)     # Mon
    write_native(src / "ZZT_hist_1m.csv", bars_jan + bars_feb)
    rep = run([src / "ZZT_hist_1m.csv"], root)
    rec = file_rec(rep, "ZZT_hist_1m.csv")
    s = series_of(rec)
    check("native: ok status", rec["status"] == "ok", str(rec))
    check("native: 2 months written", s["written"] == 2, str(s))
    check("native: 50 rows added", s["added"] == 50)
    check("native: interval measured 1m", s["interval"] == "1m",
          s.get("interval_note", ""))
    jan = month_path(root, "ZZT", 2024, 1, "1m")
    check("native: month file exists at canonical path", jan.is_file(),
          str(jan))
    rb, _st = ss.read_month_file(jan)
    check("native: strict read-back returns the same bars", rb == bars_jan)
    # stored as canonical Parquet now; the byte-identical-to-CSV check is moot —
    # read-back equality (above) proves fidelity, and storage [1b] covers the
    # Parquet->CSV export identity.
    check("native: stored as canonical Parquet", jan.suffix == ".parquet")
    check("native: report saved",
          rep.get("report_path") and Path(rep["report_path"]).is_file())

    # -- ISO combined + symbol column + LF + LYING filename (_1s, data is 1m)
    iso = ["datetime,open,high,low,close,volume,symbol"]
    for b in mk_bars(2024, 3, 4, 40, base=70.0):
        iso.append(f"{b[0]:%Y-%m-%d %H:%M:%S},{b[1]},{b[2]},{b[3]},{b[4]},"
                   f"{b[5]},zzu")
    (src / "ZZU_3y_1s.csv").write_bytes(("\n".join(iso) + "\n").encode())
    rep = run([src / "ZZU_3y_1s.csv"], root)
    s = series_of(file_rec(rep, "ZZU_3y_1s.csv"))
    check("mislabel: measured 1m beats filename 1s", s["interval"] == "1m")
    check("mislabel: note says so loudly",
          "FILENAME SAYS 1s" in s["interval_note"], s["interval_note"])
    check("mislabel: lands under canonical KO-style folder",
          month_path(root, "ZZU", 2024, 3, "1m").is_file())
    man = ss.load_manifest(root / "ZZU")
    check("mislabel: lowercase symbol recorded as alias",
          man and "zzu" in man.get("aliases", []), str(man and man.get("aliases")))

    # -- IBKR pandas dump: index col, tz offsets, float volume, extra cols
    ib = [",date,open,high,low,close,volume,average,barCount"]
    for i, b in enumerate(mk_bars(2024, 4, 15, 25, base=80.0)):
        ib.append(f"{i},{b[0]:%Y-%m-%d %H:%M:%S}-04:00,{b[1]},{b[2]},{b[3]},"
                  f"{b[4]},{float(b[5])},81.0,42")
    (src / "ZZV_10Y_1m.csv").write_bytes(("\n".join(ib) + "\n").encode())
    rep = run([src / "ZZV_10Y_1m.csv"], root)
    rec = file_rec(rep, "ZZV_10Y_1m.csv")
    s = series_of(rec)
    check("ibkr-dump: ingested ok", rec["status"] == "ok" and s["added"] == 25,
          str(rec))
    notes = " | ".join(rec["notes"])
    check("ibkr-dump: index column noted", "index" in notes, notes)
    check("ibkr-dump: tz conversion noted", "tz-aware" in notes, notes)
    check("ibkr-dump: float volume normalized",
          "integral-float volume" in notes, notes)
    check("ibkr-dump: extra columns ignored note",
          "average" in notes and "barCount" in notes, notes)
    rb, _ = ss.read_month_file(month_path(root, "ZZV", 2024, 4, "1m"))
    check("ibkr-dump: tz offset produced correct NY wall time",
          rb[0][0] == datetime(2024, 4, 15, 9, 30, 0), str(rb[0][0]))

    # -- parquet, incl. a 16:00:00 bar (non-RTH) — needs pandas/pyarrow
    try:
        import pandas as pd
        have_pd = True
    except Exception:  # noqa: BLE001
        have_pd = False
    if have_pd:
        rows = []
        t = datetime(2024, 5, 13, 9, 30, 1)
        for i in range(20):
            rows.append({"date": f"{t:%Y-%m-%d}", "time": f"{t:%H:%M:%S}",
                         "open": 71.0 + i * 0.01, "high": 71.1 + i * 0.01,
                         "low": 70.9 + i * 0.01, "close": 71.05 + i * 0.01,
                         "volume": 100 + i, "symbol": "ZZW"})
            t += timedelta(seconds=1)
        rows.append({"date": "2024-05-13", "time": "16:00:00", "open": 71.0,
                     "high": 71.1, "low": 70.9, "close": 71.0, "volume": 5,
                     "symbol": "ZZW"})
        pq = src / "zzw_1s_2024.parquet"
        pd.DataFrame(rows).to_parquet(pq)
        rep = run([pq], root)
        rec = file_rec(rep, pq.name)
        s = series_of(rec)
        check("parquet: ingested as 1s", s["interval"] == "1s",
              str(s.get("interval_note")))
        check("parquet: 16:00:00 bar dropped as non-RTH",
              rec["counters"].get("non_rth") == 1 and s["added"] == 20,
              str(rec["counters"]))
        # parquet magic under a .csv name
        dis = src / "ZZX_1s_disguised.csv"
        dis.write_bytes(pq.read_bytes())
        rep = run([dis], root)
        rec = file_rec(rep, dis.name)
        check("parquet-in-disguise: detected and parsed as parquet",
              rec["status"] == "ok"
              and any("parquet magic" in n for n in rec["notes"]),
              str(rec["notes"]))
    else:
        print("[skip] pandas/pyarrow not importable — parquet checks skipped")

    # -- BOM + semicolon; utf-16; AM/PM + no-seconds times; epoch; Z-suffix
    bom = ["Date;Time;Open;High;Low;Close;Volume"]
    for b in mk_bars(2024, 6, 17, 15, base=30.0):
        bom.append(f"{b[0].month}/{b[0].day}/{b[0].year};"
                   f"{b[0]:%I:%M %p};{b[1]};{b[2]};{b[3]};{b[4]};{b[5]}")
    (src / "ZZY_x_1m.csv").write_bytes(
        b"\xef\xbb\xbf" + ("\n".join(bom) + "\n").encode())
    rep = run([src / "ZZY_x_1m.csv"], root)
    rec = file_rec(rep, "ZZY_x_1m.csv")
    s = series_of(rec)
    check("bom+semicolon+AM/PM: ingested", rec["status"] == "ok"
          and s["added"] == 15, str(rec))
    rb, _ = ss.read_month_file(month_path(root, "ZZY", 2024, 6, "1m"))
    check("AM/PM + minute-times parsed to 9:30:00",
          rb[0][0] == datetime(2024, 6, 17, 9, 30, 0), str(rb[0][0]))

    u16 = ["Date,Time,open,high,low,close,volume"]
    for b in mk_bars(2024, 7, 15, 12, base=40.0):
        u16.append(ss.format_bar(*b))
    (src / "ZZZQ_x_1m.csv").write_bytes(("\n".join(u16) + "\n")
                                        .encode("utf-16"))
    rep = run([src / "ZZZQ_x_1m.csv"], root)
    rec = file_rec(rep, "ZZZQ_x_1m.csv")
    check("utf-16: decoded and ingested", rec["status"] == "ok"
          and any("UTF-16" in n for n in rec["notes"]), str(rec["notes"]))

    try:
        from zoneinfo import ZoneInfo
        ny = ZoneInfo("America/New_York")
        have_tz = True
    except Exception:  # noqa: BLE001
        have_tz = False
    if have_tz:
        t0 = datetime(2024, 6, 18, 9, 30, 0, tzinfo=ny)
        ep = ["timestamp,open,high,low,close,volume"]
        for i in range(12):
            ep.append(f"{int((t0 + timedelta(minutes=i)).timestamp())},"
                      f"10.0,10.1,9.9,10.05,{100 + i}")
        (src / "ZZE_epoch_1m.csv").write_bytes(("\n".join(ep) + "\n").encode())
        rep = run([src / "ZZE_epoch_1m.csv"], root)
        rec = file_rec(rep, "ZZE_epoch_1m.csv")
        check("epoch: ingested with conversion note",
              rec["status"] == "ok"
              and any("epoch" in n for n in rec["notes"]), str(rec))
        rb, _ = ss.read_month_file(month_path(root, "ZZE", 2024, 6, "1m"))
        check("epoch: UTC->NY produced 9:30 wall time",
              rb[0][0] == datetime(2024, 6, 18, 9, 30, 0), str(rb[0][0]))

        zi = ["datetime,open,high,low,close,volume"]
        t0u = datetime(2024, 6, 18, 13, 30, 0)      # 13:30Z == 9:30 EDT
        for i in range(12):
            zi.append(f"{(t0u + timedelta(minutes=i)).isoformat()}Z,"
                      f"11.0,11.1,10.9,11.05,{200 + i}")
        (src / "ZZF_ziso_1m.csv").write_bytes(("\n".join(zi) + "\n").encode())
        rep = run([src / "ZZF_ziso_1m.csv"], root)
        rb, _ = ss.read_month_file(month_path(root, "ZZF", 2024, 6, "1m"))
        check("Z-suffix ISO: converted to 9:30 NY",
              rb[0][0] == datetime(2024, 6, 18, 9, 30, 0), str(rb[0][0]))

    # -- headerless native
    hl = [ss.format_bar(*b) for b in mk_bars(2024, 8, 19, 14, base=20.0)]
    (src / "ZZG_raw_1m.csv").write_bytes((ss.EOL.join(hl) + ss.EOL)
                                         .encode("ascii"))
    rep = run([src / "ZZG_raw_1m.csv"], root)
    rec = file_rec(rep, "ZZG_raw_1m.csv")
    check("headerless native: ingested with note",
          rec["status"] == "ok"
          and any("headerless" in n for n in rec["notes"]), str(rec))
    check("headerless native: all 14 rows landed",
          series_of(rec)["added"] == 14)

    print("=== [2] identity ===================================================")
    d2, src2, root2 = fresh()
    # no symbol col, no TICKER_ name
    body = ["Date,Time,open,high,low,close,volume"] + \
           [ss.format_bar(*b) for b in mk_bars(2024, 1, 15, 12)]
    (src2 / "data.csv").write_bytes(("\n".join(body) + "\n").encode())
    rep = run([src2 / "data.csv"], root2)
    rec = file_rec(rep, "data.csv")
    check("bare 'data.csv': needs-decision, never guessed",
          rec["status"] == "needs-decision", str(rec))
    check("bare 'data.csv': nothing written",
          not (root2 / "DATA").exists())

    # mixed-bag symbol column -> two tickers from ONE file
    mixed = ["datetime,open,high,low,close,volume,symbol"]
    for b in mk_bars(2024, 2, 5, 15, base=10.0):
        mixed.append(f"{b[0]:%Y-%m-%d %H:%M:%S},{b[1]},{b[2]},{b[3]},{b[4]},"
                     f"{b[5]},AAA")
    for b in mk_bars(2024, 2, 5, 15, base=90.0):
        mixed.append(f"{b[0]:%Y-%m-%d %H:%M:%S},{b[1]},{b[2]},{b[3]},{b[4]},"
                     f"{b[5]},BBB")
    (src2 / "mixed_bag_1m.csv").write_bytes(("\n".join(mixed) + "\n").encode())
    rep = run([src2 / "mixed_bag_1m.csv"], root2)
    rec = file_rec(rep, "mixed_bag_1m.csv")
    check("mixed-bag: two series out of one file",
          len(rec.get("series", [])) == 2
          and month_path(root2, "AAA", 2024, 2, "1m").is_file()
          and month_path(root2, "BBB", 2024, 2, "1m").is_file(), str(rec))

    # filename hint disagrees with symbol column -> symbol wins, loud note
    dis = ["datetime,open,high,low,close,volume,symbol"]
    for b in mk_bars(2024, 3, 4, 12, base=15.0):
        dis.append(f"{b[0]:%Y-%m-%d %H:%M:%S},{b[1]},{b[2]},{b[3]},{b[4]},"
                   f"{b[5]},CCC")
    (src2 / "DDD_export_1m.csv").write_bytes(("\n".join(dis) + "\n").encode())
    rep = run([src2 / "DDD_export_1m.csv"], root2)
    rec = file_rec(rep, "DDD_export_1m.csv")
    check("name-vs-symbol clash: symbol column wins",
          (root2 / "CCC").exists() and not (root2 / "DDD").exists())
    check("name-vs-symbol clash: note recorded",
          any("symbol column wins" in n for n in rec["notes"]),
          str(rec["notes"]))

    print("=== [3] row policies ===============================================")
    d3, src3, root3 = fresh()
    g = mk_bars(2024, 1, 15, 120)     # plenty of clean rows: 5 invalid
    #                                   must stay under the 5% threshold
    lines = native_lines(g)
    lines.append(ss.format_bar(*g[5]))                       # identical dup
    cf = g[7]
    lines.append(ss.format_bar(cf[0], cf[1] + 1.0, cf[2] + 1.0, cf[3],
                               cf[4] + 1.0, cf[5]))          # contradiction
    b9 = g[9]
    lines.append(ss.format_bar(b9[0].replace(hour=16, minute=0), *b9[1:]))
    lines.append(ss.format_bar(b9[0].replace(hour=9, minute=29, second=59),
                               *b9[1:]))                     # non-RTH x2
    sat = datetime(2024, 1, 13, 10, 0, 0)                     # Saturday
    lines.append(ss.format_bar(sat, 1.0, 1.1, 0.9, 1.0, 5))  # weekend
    lines.append("1/15/2024,10:30:00,5,4,6,5,10")             # impossible bar
    lines.append("1/15/2024,10:31:00,5,6,4,5,-3")             # negative volume
    lines.append("1/15/2024,10:32:00,5,6,4,5,10.5")           # frac volume
    lines.append("1/15/2024,10:33:00,0,6,4,5,10")             # zero price
    lines.append("1/15/2024,10:34:00.5,5,6,4,5,10")           # sub-second
    (src3 / "EEE_mess_1m.csv").write_bytes(("\n".join(lines) + "\n").encode())
    rep = run([src3 / "EEE_mess_1m.csv"], root3)
    rec = file_rec(rep, "EEE_mess_1m.csv")
    c = rec["counters"]
    check("dups: identical dropped+counted", c.get("infile_dup_identical") == 1,
          str(c))
    check("contradiction: both rows ejected",
          c.get("infile_conflict_rows") == 2, str(c))
    check("non-RTH: 3 dropped (16:00, 9:29:59, Saturday)",
          c.get("non_rth") == 3, str(c))
    check("invalid: 5 rejected rows", c.get("invalid_rows") == 5, str(c))
    s = series_of(rec)
    check("clean remainder ingested (120 uniq - contradicted bar = 119)",
          s["added"] == 119, str(s))
    rb, _ = ss.read_month_file(month_path(root3, "EEE", 2024, 1, "1m"))
    check("contradicted timestamp absent from the tree",
          all(b[0] != cf[0] for b in rb))
    check("rejected-rows evidence file written",
          rec.get("rejected_rows_file")
          and Path(rec["rejected_rows_file"]).is_file(), str(rec))

    # >40% non-RTH -> whole-file quarantine
    xh = native_lines(mk_bars(2024, 1, 15, 10))
    for i in range(10):
        t = datetime(2024, 1, 15, 17, i, 0)
        xh.append(ss.format_bar(t, 2.0, 2.1, 1.9, 2.0, 7))
    (src3 / "FFF_ext_1m.csv").write_bytes(("\n".join(xh) + "\n").encode())
    rep = run([src3 / "FFF_ext_1m.csv"], root3)
    rec = file_rec(rep, "FFF_ext_1m.csv")
    check(">40% non-RTH: quarantined", rec["status"] == "quarantine",
          str(rec))
    check(">40% non-RTH: copy + reason exist",
          rec.get("quarantined_to") and Path(rec["quarantined_to"]).is_file()
          and Path(rec["quarantined_to"] + ".reason.txt").is_file(), str(rec))
    check(">40% non-RTH: nothing written", not (root3 / "FFF").exists())
    check("source file untouched where it was",
          (src3 / "FFF_ext_1m.csv").is_file())

    # >5% invalid -> quarantine
    bad = native_lines(mk_bars(2024, 1, 15, 10))
    bad += ["1/15/2024,10:30:00,5,4,6,5,10"] * 3              # impossible x3
    (src3 / "GGG_bad_1m.csv").write_bytes(("\n".join(bad) + "\n").encode())
    rep = run([src3 / "GGG_bad_1m.csv"], root3)
    check(">5% invalid rows: quarantined",
          file_rec(rep, "GGG_bad_1m.csv")["status"] == "quarantine")

    # D/M dates and ambiguous dates
    dm = ["Date,Time,open,high,low,close,volume",
          "15/06/2024,9:30:00,1,1.1,0.9,1,5"]
    (src3 / "HHH_dm_1m.csv").write_bytes(("\n".join(dm) + "\n").encode())
    rep = run([src3 / "HHH_dm_1m.csv"], root3)
    rec = file_rec(rep, "HHH_dm_1m.csv")
    check("D/M dates: quarantined with explanation",
          rec["status"] == "quarantine" and "D/M" in rec["reason"], str(rec))
    amb = ["Date,Time,open,high,low,close,volume"]
    for i in range(12):
        amb.append(f"3/4/2024,{9}:{30 + i:02d}:00,1,1.1,0.9,1,5")
    (src3 / "III_amb_1m.csv").write_bytes(("\n".join(amb) + "\n").encode())
    rep = run([src3 / "III_amb_1m.csv"], root3)
    rec = file_rec(rep, "III_amb_1m.csv")
    check("ambiguous M/D-vs-D/M: quarantined, never guessed",
          rec["status"] == "quarantine" and "ambiguous" in rec["reason"],
          str(rec))

    print("=== [4] interval verification ======================================")
    d4, src4, root4 = fresh()
    ir = ["Date,Time,open,high,low,close,volume"]
    secs = [0, 7, 11, 24, 31, 45, 52, 64, 71, 83, 95, 99, 107, 118]
    for i, s_ in enumerate(secs):
        t = datetime(2024, 1, 15, 9, 30, 0) + timedelta(seconds=s_)
        ir.append(ss.format_bar(t, 1.0, 1.1, 0.9, 1.0, 5 + i))
    (src4 / "JJJ_odd_1m.csv").write_bytes(("\n".join(ir) + "\n").encode())
    rep = run([src4 / "JJJ_odd_1m.csv"], root4)
    s = series_of(file_rec(rep, "JJJ_odd_1m.csv"))
    check("irregular spacing: series rejected, not written",
          s.get("rejected") and not (root4 / "JJJ").exists(), str(s))

    few = native_lines(mk_bars(2024, 1, 15, 4))
    (src4 / "KKK_tiny_5m.csv").write_bytes(("\n".join(few) + "\n").encode())
    rep = run([src4 / "KKK_tiny_5m.csv"], root4)
    s = series_of(file_rec(rep, "KKK_tiny_5m.csv"))
    check("too-few rows + filename hint: hint used, marked unverified",
          s.get("interval") == "5m"
          and "unverified" in s.get("interval_note", ""), str(s))

    few2 = native_lines(mk_bars(2024, 1, 16, 4))
    (src4 / "LLLtiny.csv").write_bytes(("\n".join(few2) + "\n").encode())
    rep = run([src4 / "LLLtiny.csv"], root4)
    rec = file_rec(rep, "LLLtiny.csv")
    s = series_of(rec) if rec.get("series") else None
    check("too-few rows, no hint: rejected",
          rec["status"] != "ok" or (s and s.get("rejected")), str(rec))

    two = native_lines(mk_bars(2024, 1, 15, 30, step_s=120))
    (src4 / "MMM_x_2m.csv").write_bytes(("\n".join(two) + "\n").encode())
    rep = run([src4 / "MMM_x_2m.csv"], root4)
    s = series_of(file_rec(rep, "MMM_x_2m.csv"))
    check("2-minute data verified as 2m", s["interval"] == "2m",
          str(s.get("interval_note")))

    print("=== [5] merge / conflicts / gate / idempotency =====================")
    d5, src5, root5 = fresh()
    base = mk_bars(2024, 1, 15, 10, base=100.0)
    write_native(src5 / "NNN_a_1m.csv", base)
    rep = run([src5 / "NNN_a_1m.csv"], root5)
    p = month_path(root5, "NNN", 2024, 1, "1m")
    mt0 = p.stat().st_mtime_ns

    # re-run: idempotent, no churn
    rep = run([src5 / "NNN_a_1m.csv"], root5)
    s = series_of(file_rec(rep, "NNN_a_1m.csv"))
    check("idempotent re-run: nothing written",
          s["written"] == 0 and s["dup_existing"] == 10
          and s["skipped_identical"] == 1, str(s))
    check("idempotent re-run: file mtime untouched",
          p.stat().st_mtime_ns == mt0)

    # second file: 8 identical + 1 conflicting + 2 new -> existing wins
    nb = list(base)
    conf_bar = (base[3][0], base[3][1] + 0.5, base[3][2] + 0.5, base[3][3],
                base[3][4] + 0.5, base[3][5])
    newer = mk_bars(2024, 1, 15, 2, base=101.0, h0=10, m0=0)
    write_native(src5 / "NNN_b_1m.csv",
                 [conf_bar] + base[:3] + base[4:] + newer)
    rep = run([src5 / "NNN_b_1m.csv"], root5)
    s = series_of(file_rec(rep, "NNN_b_1m.csv"))
    check("merge: 9 dups, 1 conflict, 2 added",
          s["dup_existing"] == 9 and s["conflicts"] == 1 and s["added"] == 2,
          str(s))
    rb, _ = ss.read_month_file(p)
    keep = [b for b in rb if b[0] == base[3][0]][0]
    check("merge: EXISTING value kept on conflict", keep == base[3],
          str(keep))
    check("merge: conflict example in conflicts.jsonl",
          rep.get("conflict_log") and Path(rep["conflict_log"]).is_file()
          and "NNN" in Path(rep["conflict_log"]).read_text(), str(rep.get(
              "conflict_log")))
    man = ss.load_manifest(root5 / "NNN")
    contribs = (man["intervals"]["1m"]["months"]["2024-01"]["source"]
                ["contributions"])
    check("provenance: both files recorded with counts",
          {c["file"] for c in contribs if "file" in c}
          == {"NNN_a_1m.csv", "NNN_b_1m.csv"}, str(contribs))

    # basis gate: >=100 overlapping, all conflicting on price
    big = mk_bars(2024, 2, 19, 150, base=200.0)
    write_native(src5 / "OOO_a_1m.csv", big)
    run([src5 / "OOO_a_1m.csv"], root5)
    shifted = [(b[0], b[1] * 10, b[2] * 10, b[3] * 10, b[4] * 10, b[5])
               for b in big] + mk_bars(2024, 2, 20, 5, base=2000.0)
    write_native(src5 / "OOO_b_1m.csv", shifted)
    po = month_path(root5, "OOO", 2024, 2, "1m")
    bytes_before = po.read_bytes()
    rep = run([src5 / "OOO_b_1m.csv"], root5)
    s = series_of(file_rec(rep, "OOO_b_1m.csv"))
    check("basis gate: fired", bool(s.get("gate")), str(s)[:300])
    check("basis gate: names price conflicts",
          s.get("gate") and "price" in s["gate"], str(s.get("gate")))
    check("basis gate: NOTHING written (even the new bars)",
          po.read_bytes() == bytes_before
          and not month_path(root5, "OOO", 2024, 2, "1m").with_name(
              "x").parent.joinpath("OOO_2024-02_1m.csv.extra").exists())
    check("basis gate: counted in totals",
          rep["totals"].get("gated_series", 0) == 1, str(rep["totals"]))

    # small overlap (<100): per-row existing-wins, no gate
    sm = mk_bars(2024, 3, 18, 50, base=10.0)
    write_native(src5 / "PPP_a_1m.csv", sm)
    run([src5 / "PPP_a_1m.csv"], root5)
    sm2 = [(b[0], b[1] + 0.5, b[2] + 0.5, b[3], b[4] + 0.5, b[5])
           for b in sm[:30]] + mk_bars(2024, 3, 18, 5, base=11.0, h0=11)
    write_native(src5 / "PPP_b_1m.csv", sm2)
    rep = run([src5 / "PPP_b_1m.csv"], root5)
    s = series_of(file_rec(rep, "PPP_b_1m.csv"))
    check("small overlap: no gate, conflicts logged, new rows added",
          not s.get("gate") and s["conflicts"] == 30 and s["added"] == 5,
          str({k: s[k] for k in ("conflicts", "added")}))

    # corrupt existing month -> BLOCKED, sibling month still written
    cor = mk_bars(2024, 4, 15, 10, base=20.0)
    write_native(src5 / "QQQ_a_1m.csv", cor)
    run([src5 / "QQQ_a_1m.csv"], root5)
    pq_ = month_path(root5, "QQQ", 2024, 4, "1m")
    pq_.write_bytes(b"corrupt-not-a-valid-parquet-file")       # unreadable store
    cor_bytes = pq_.read_bytes()
    both = cor + mk_bars(2024, 5, 13, 8, base=21.0)
    write_native(src5 / "QQQ_b_1m.csv", both)
    rep = run([src5 / "QQQ_b_1m.csv"], root5)
    s = series_of(file_rec(rep, "QQQ_b_1m.csv"))
    check("corrupt existing month: BLOCKED, not overwritten",
          len(s["blocked_months"]) == 1 and pq_.read_bytes() == cor_bytes,
          str(s["blocked_months"]))
    check("corrupt existing month: sibling month still written",
          month_path(root5, "QQQ", 2024, 5, "1m").is_file())

    print("=== [6] run mechanics ==============================================")
    d6, src6, root6 = fresh()
    (src6 / ".DS_Store").write_bytes(b"junk")
    (src6 / "notes.py").write_text("x=1", encoding="ascii")
    (src6 / "stale.csv.123-456-789.tmp").write_bytes(b"x")
    (src6 / "empty.csv").write_bytes(b"")
    (src6 / "book_x_1m.csv").write_bytes(b"PK\x03\x04zipzipzip")
    (src6 / "gz_x_1m.csv").write_bytes(b"\x1f\x8bgz")
    sub = src6 / "deeper"
    sub.mkdir()
    write_native(sub / "RRR_x_1m.csv", mk_bars(2024, 1, 15, 12))
    rep = run([src6], root6)            # FOLDER ingest, recursive
    reasons = {Path(s_["path"]).name: s_["reason"] for s_ in rep["skipped"]}
    check("folder ingest: junk skipped",
          ".DS_Store" in reasons and "notes.py" in reasons
          and "stale.csv.123-456-789.tmp" in reasons, str(reasons))
    check("folder ingest: subfolder file found and ingested",
          file_rec(rep, "RRR_x_1m.csv")["status"] == "ok")
    check("empty file: reported empty",
          file_rec(rep, "empty.csv")["status"] == "empty")
    zr = file_rec(rep, "book_x_1m.csv")
    check("zip/xlsx-in-disguise: quarantined with hint",
          zr["status"] == "quarantine" and "xlsx" in zr["reason"], str(zr))
    check("gzip: quarantined",
          file_rec(rep, "gz_x_1m.csv")["status"] == "quarantine")

    # self-ingest refusal
    inside = root6 / "RRR" / "2024" / "01-Jan" / "RRR_2024-01_1m.csv"
    rep = run([inside], root6)
    check("file inside the storage tree refused",
          rep.get("aborted") == "no ingestable files found"
          and any("inside the storage tree" in s_["reason"]
                  for s_ in rep["skipped"]), str(rep.get("skipped")))

    # scan: reserved dirs invisible, tree healthy
    res = ss.scan_storage(root6)
    flagged = [p for p, _r in res["unrecognized"]
               if "_quarantine" in p or "_ingest_reports" in p]
    check("scan: reserved _dirs not flagged as junk", not flagged,
          str(flagged))
    check("scan: ingested ticker visible with rows",
          res["tickers"].get("RRR", {}).get("intervals", {})
          .get("1m", {}).get("rows") == 12, str(res["tickers"]))

    # cancel pre-set: nothing starts; cancel mid-run: partial persists
    d7, src7, root7 = fresh()
    write_native(src7 / "SSS_a_1m.csv", mk_bars(2024, 1, 15, 10))
    write_native(src7 / "TTT_b_1m.csv", mk_bars(2024, 1, 15, 10))
    ev = threading.Event()
    ev.set()
    rep = run([src7 / "SSS_a_1m.csv"], root7, cancel=ev)
    check("pre-set cancel: file not started, run flagged cancelled",
          rep["cancelled"]
          and rep["files"][0]["status"] == "not started (cancelled)"
          and not (root7 / "SSS").exists(), str(rep["files"]))
    ev2 = threading.Event()
    seen = []

    def trip(msg):
        seen.append(msg)
        if "TTT_b_1m.csv: merging" in msg:
            ev2.set()                       # cancel right before TTT commits

    rep = run([src7 / "SSS_a_1m.csv", src7 / "TTT_b_1m.csv"], root7,
              progress=trip, cancel=ev2)
    check("mid-run cancel: first file committed, second not",
          (root7 / "SSS").exists() and not month_path(
              root7, "TTT", 2024, 1, "1m").exists(), str(rep["totals"]))
    rep = run([src7 / "SSS_a_1m.csv", src7 / "TTT_b_1m.csv"], root7)
    s1 = series_of(file_rec(rep, "SSS_a_1m.csv"))
    s2 = series_of(file_rec(rep, "TTT_b_1m.csv"))
    check("resume after cancel: completes idempotently",
          s1["written"] == 0 and s1["dup_existing"] == 10
          and s2["written"] == 1 and s2["added"] == 10,
          f"{s1} / {s2}")

    # disk preflight (forced)
    d8, src8, root8 = fresh()
    write_native(src8 / "UUU_x_1m.csv", mk_bars(2024, 1, 15, 5))
    _floor = si.PREFLIGHT_FLOOR
    si.PREFLIGHT_FLOOR = 10 ** 18
    rep = run([src8 / "UUU_x_1m.csv"], root8)
    si.PREFLIGHT_FLOOR = _floor
    check("disk preflight: aborts before writing anything",
          rep.get("aborted", "").startswith("disk preflight")
          and not (root8 / "UUU").exists(), str(rep.get("aborted")))

    # totals reconcile + summary lines render
    d9, src9, root9 = fresh()
    write_native(src9 / "VVV_x_1m.csv", mk_bars(2024, 1, 15, 25))
    rep = run([src9 / "VVV_x_1m.csv"], root9)
    t = rep["totals"]
    check("totals reconcile: parsed == added (clean run)",
          t.get("parsed_ok") == 25 and t.get("added") == 25
          and t.get("written") == 1, str(t))
    lines_out = si.summarize_report(rep)
    check("summary: head line + ok line render",
          lines_out and lines_out[0].startswith("INGEST ")
          and any("VVV" in ln for ln in lines_out), str(lines_out[:3]))
    rj = json.loads(Path(rep["report_path"]).read_text(encoding="utf-8"))
    check("report.json round-trips", rj["run"] == rep["run"])

    print("=== [7] gap pack (added after the rehearsal review) ================")
    # 7.1 SAME series fed by TWO files in ONE run: the second file's dry-run
    # must see the first file's just-committed months, and provenance must
    # list both contributions.
    d10, src10, root10 = fresh()
    a1 = mk_bars(2024, 1, 15, 10, base=100.0)
    a2 = mk_bars(2024, 1, 15, 6, base=101.0, h0=11)     # same month, later
    write_native(src10 / "WWA_a_1m.csv", a1)
    write_native(src10 / "WWA_b_1m.csv", a2)
    rep = run([src10 / "WWA_a_1m.csv", src10 / "WWA_b_1m.csv"], root10)
    sa = series_of(file_rec(rep, "WWA_a_1m.csv"))
    sb2 = series_of(file_rec(rep, "WWA_b_1m.csv"))   # sb = stock_basis
    check("one run, two files, one series: both committed",
          sa["added"] == 10 and sb2["added"] == 6
          and sb2["dup_existing"] == 0 and sb2["conflicts"] == 0,
          f"{sa} / {sb2}")
    rb, _ = ss.read_month_file(month_path(root10, "WWA", 2024, 1, "1m"))
    check("one run, two files: merged month holds all 16 bars sorted",
          len(rb) == 16 and rb == sorted(a1 + a2, key=lambda b: b[0]))
    man = ss.load_manifest(root10 / "WWA")
    contribs = (man["intervals"]["1m"]["months"]["2024-01"]["source"]
                ["contributions"])
    check("one run, two files: provenance lists both",
          {c.get("file") for c in contribs}
          == {"WWA_a_1m.csv", "WWA_b_1m.csv"}, str(contribs))

    # 7.2 writer fault on ONE month: that month reports WRITE FAILED, the
    # sibling month still lands, the run continues truthfully.
    _orig_write = ss.write_month_file
    def _boom(path, bars):
        if "2024-02" in Path(path).name:
            raise ss.StorageError("simulated AV lock")
        return _orig_write(path, bars)
    ss.write_month_file = _boom
    try:
        write_native(src10 / "WWC_x_1m.csv",
                     mk_bars(2024, 1, 15, 8, base=5.0)
                     + mk_bars(2024, 2, 19, 8, base=6.0))
        rep = run([src10 / "WWC_x_1m.csv"], root10)
    finally:
        ss.write_month_file = _orig_write
    s = series_of(file_rec(rep, "WWC_x_1m.csv"))
    check("writer fault: failed month reported, sibling written",
          s["months"]["2024-02"]["status"].startswith("WRITE FAILED")
          and s["months"]["2024-01"]["status"] == "written"
          and s["added"] == 8 and s["written"] == 1, str(s["months"]))
    check("writer fault: only the good month is on disk",
          month_path(root10, "WWC", 2024, 1, "1m").is_file()
          and not month_path(root10, "WWC", 2024, 2, "1m").exists())

    # 7.3 manifest.json is a DIRECTORY: data still lands, note explains.
    (root10 / "WWD").mkdir()
    (root10 / "WWD" / ss.MANIFEST_NAME).mkdir()
    write_native(src10 / "WWD_x_1m.csv", mk_bars(2024, 1, 15, 8, base=7.0))
    rep = run([src10 / "WWD_x_1m.csv"], root10)
    rec = file_rec(rep, "WWD_x_1m.csv")
    check("manifest-blocked: bars written anyway",
          month_path(root10, "WWD", 2024, 1, "1m").is_file())
    check("manifest-blocked: note says manifest not saved",
          any("manifest for WWD not saved" in n for n in rec["notes"]),
          str(rec["notes"]))

    # 7.4 epoch milliseconds and microseconds scales
    try:
        from zoneinfo import ZoneInfo
        _ny = ZoneInfo("America/New_York")
        _t0 = datetime(2024, 6, 18, 9, 30, 0, tzinfo=_ny)
        for name, mul in (("WWE_ms_1m.csv", 1_000),
                          ("WWF_us_1m.csv", 1_000_000)):
            epl = ["timestamp,open,high,low,close,volume"]
            for i in range(12):
                v = int((_t0 + timedelta(minutes=i)).timestamp()) * mul
                epl.append(f"{v},10.0,10.1,9.9,10.05,{100 + i}")
            (src10 / name).write_bytes(("\n".join(epl) + "\n").encode())
        rep = run([src10 / "WWE_ms_1m.csv", src10 / "WWF_us_1m.csv"], root10)
        rb1, _ = ss.read_month_file(month_path(root10, "WWE", 2024, 6, "1m"))
        rb2, _ = ss.read_month_file(month_path(root10, "WWF", 2024, 6, "1m"))
        check("epoch ms + us scales -> correct NY times",
              rb1[0][0] == datetime(2024, 6, 18, 9, 30, 0)
              and rb2[0][0] == datetime(2024, 6, 18, 9, 30, 0),
              f"{rb1[0][0]} / {rb2[0][0]}")
    except Exception as _e:  # noqa: BLE001
        check("epoch ms + us scales -> correct NY times", False, str(_e))

    # 7.5 .txt extension carries data
    write_native(src10 / "WWG_x_1m.txt", mk_bars(2024, 1, 15, 8, base=8.0))
    rep = run([src10 / "WWG_x_1m.txt"], root10)
    check(".txt data file ingested",
          file_rec(rep, "WWG_x_1m.txt")["status"] == "ok"
          and month_path(root10, "WWG", 2024, 1, "1m").is_file())

    # 7.6 conflicting filename hints (_5m_1m) + too few rows = no hint
    write_native(src10 / "WWH_5m_1m.csv", mk_bars(2024, 1, 15, 4, base=9.0))
    rep = run([src10 / "WWH_5m_1m.csv"], root10)
    s = series_of(file_rec(rep, "WWH_5m_1m.csv"))
    check("conflicting hints: treated as hintless -> rejected",
          s.get("rejected") and not (root10 / "WWH").exists(), str(s))

    # 7.7 headerless ISO-date 7-column file
    hl = [f"{b[0]:%Y-%m-%d},{ss.format_time(b[0].time())},{b[1]},{b[2]},"
          f"{b[3]},{b[4]},{b[5]}" for b in mk_bars(2024, 1, 15, 14, base=12.0)]
    (src10 / "WWI_x_1m.csv").write_bytes(("\n".join(hl) + "\n").encode())
    rep = run([src10 / "WWI_x_1m.csv"], root10)
    rec = file_rec(rep, "WWI_x_1m.csv")
    check("headerless ISO: ingested with note",
          rec["status"] == "ok"
          and any("headerless" in n for n in rec["notes"])
          and series_of(rec)["added"] == 14, str(rec))

    # 7.8 progress callback that raises must never hurt the run
    write_native(src10 / "WWJ_x_1m.csv", mk_bars(2024, 1, 15, 8, base=13.0))
    def _bad_progress(msg):
        raise RuntimeError("ui died")
    rep = run([src10 / "WWJ_x_1m.csv"], root10, progress=_bad_progress)
    check("raising progress callback: run completes",
          file_rec(rep, "WWJ_x_1m.csv")["status"] == "ok"
          and month_path(root10, "WWJ", 2024, 1, "1m").is_file())

    # 7.9 the same path listed twice ingests once
    rep = run([src10 / "WWJ_x_1m.csv", src10 / "WWJ_x_1m.csv"], root10)
    check("duplicate path collapses to one file record",
          len([r for r in rep["files"]
               if Path(r["path"]).name == "WWJ_x_1m.csv"]) == 1)

    # 7.10 symbol column present but EMPTY everywhere -> quarantine, reasons
    es = ["datetime,open,high,low,close,volume,symbol"]
    for b in mk_bars(2024, 1, 15, 12, base=14.0):
        es.append(f"{b[0]:%Y-%m-%d %H:%M:%S},{b[1]},{b[2]},{b[3]},{b[4]},"
                  f"{b[5]},")
    (src10 / "WWK_x_1m.csv").write_bytes(("\n".join(es) + "\n").encode())
    rep = run([src10 / "WWK_x_1m.csv"], root10)
    rec = file_rec(rep, "WWK_x_1m.csv")
    check("empty symbol column: quarantined as invalid raw data",
          rec["status"] == "quarantine" and "invalid" in rec["reason"],
          str(rec))

    # 7.11 existing month file REGION-LOCKED during merge (Windows): the
    # month is blocked, nothing corrupts, the run continues.
    if os.name == "nt":
        import msvcrt
        lock_target = month_path(root10, "WWA", 2024, 1, "1m")
        ext = mk_bars(2024, 1, 15, 3, base=120.0, h0=12) \
            + mk_bars(2024, 2, 19, 3, base=121.0)
        write_native(src10 / "WWA_c_1m.csv", ext)
        bytes_before = lock_target.read_bytes()
        fh = open(lock_target, "r+b")
        try:
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 4096)
            rep = run([src10 / "WWA_c_1m.csv"], root10)
        finally:
            try:
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 4096)
            except OSError:
                pass
            fh.close()
        s = series_of(file_rec(rep, "WWA_c_1m.csv"))
        check("locked month: blocked, sibling month written, no corruption",
              len(s["blocked_months"]) == 1
              and lock_target.read_bytes() == bytes_before
              and month_path(root10, "WWA", 2024, 2, "1m").is_file(),
              str(s["blocked_months"]))
    else:
        print("[skip] region-lock check is Windows-only")

    # 7.13 raw OSError mid-write (disk-full AFTER preflight, device error):
    # must degrade to a per-month WRITE FAILED, never crash the run.
    _orig_write2 = ss.write_month_file
    def _enospc(path, bars):
        if "2024-02" in Path(path).name:
            raise OSError(28, "No space left on device")
        return _orig_write2(path, bars)
    ss.write_month_file = _enospc
    try:
        write_native(src10 / "WWM_x_1m.csv",
                     mk_bars(2024, 1, 15, 8, base=16.0)
                     + mk_bars(2024, 2, 19, 8, base=17.0))
        rep = run([src10 / "WWM_x_1m.csv"], root10)
    finally:
        ss.write_month_file = _orig_write2
    s = series_of(file_rec(rep, "WWM_x_1m.csv"))
    check("disk-full mid-run: month fails, run completes, report saved",
          s["months"]["2024-02"]["status"].startswith("WRITE FAILED")
          and s["months"]["2024-01"]["status"] == "written"
          and rep.get("report_path")
          and Path(rep["report_path"]).is_file(), str(s["months"]))

    # 7.12 storage root path occupied by a FILE -> hard abort, clear message
    d11, src11, root11 = fresh()
    root11.write_text("not a dir", encoding="ascii")
    write_native(src11 / "WWL_x_1m.csv", mk_bars(2024, 1, 15, 8, base=15.0))
    rep = run([src11 / "WWL_x_1m.csv"], root11)
    check("root-is-a-file: aborted with explanation",
          "NOT a directory" in rep.get("aborted", ""), str(rep.get("aborted")))

    print("=== [8] adversarial-review regressions =============================")

    # 8.1 (HIGH) date-evidence verdict must NOT depend on row order: a file
    # with early M/D proof AND late D/M evidence is internally inconsistent
    # and must quarantine, never commit a guessed date.
    check("slash evidence: same verdict regardless of order",
          si._slash_evidence(iter(["3/15/2024", "16/03/2024"]))
          == si._slash_evidence(iter(["16/03/2024", "3/15/2024"]))
          and si._slash_evidence(iter(["3/15/2024", "16/03/2024"]))[0] is None)
    d12, src12, root12 = fresh()
    body = ["Date,Time,open,high,low,close,volume"]
    t = datetime(2024, 6, 17, 9, 30, 0)
    for i in range(60):
        body.append(f"6/17/2024,{t:%H:%M:%S},10,10.1,9.9,10,{5 + i}")
        t += timedelta(minutes=1)
    body.append("05/06/2024,11:00:00,20,20.1,19.9,20,7")   # ambiguous
    body.append("16/03/2024,12:00:00,30,30.1,29.9,30,9")   # late D/M evidence
    (src12 / "XYZ_x_1m.csv").write_bytes(("\n".join(body) + "\n").encode())
    rep = run([src12 / "XYZ_x_1m.csv"], root12)
    rec = file_rec(rep, "XYZ_x_1m.csv")
    check("mixed M/D + D/M evidence: whole file quarantined, nothing written",
          rec["status"] == "quarantine" and "BOTH positions" in rec["reason"]
          and not (root12 / "XYZ").exists(), str(rec))

    # 8.2 (MED) leading-zero filename hint must not crash the run
    check("leading-zero hint discarded", si._hint_token("AB_0s.csv") is None
          and si._hint_token("AB_05m.csv") is None
          and si._hint_token("KO_3y_1s(1).csv") == "1s")
    write_native(src12 / "AB_0s.csv", mk_bars(2024, 1, 15, 5, base=1.0))
    write_native(src12 / "ZGOOD_hist_1m.csv", mk_bars(2024, 2, 19, 30,
                                                      base=2.0))
    rep = run([src12 / "AB_0s.csv", src12 / "ZGOOD_hist_1m.csv"], root12)
    sa = series_of(file_rec(rep, "AB_0s.csv"))
    check("0s-hinted tiny file: series rejected, run survives, report saved",
          sa.get("rejected") and rep.get("report_path")
          and Path(rep["report_path"]).is_file(), str(sa))
    check("file after the bad one still committed",
          series_of(file_rec(rep, "ZGOOD_hist_1m.csv"))["added"] == 30)

    # 8.2b defense-in-depth: ANY StorageError out of a series merge becomes
    # a rejected series, never a dead run
    _orig_mfp = ss.month_file_path
    def _mfp_boom(root_, tic, y, m, interval=ss.DEFAULT_INTERVAL, fmt=None):
        if tic == "WWN":
            raise ss.StorageError("simulated path failure")
        return _orig_mfp(root_, tic, y, m, interval, fmt=fmt)
    ss.month_file_path = _mfp_boom
    try:
        write_native(src12 / "WWN_x_1m.csv", mk_bars(2024, 1, 15, 8, base=3.0))
        write_native(src12 / "WWO_x_1m.csv", mk_bars(2024, 1, 15, 8, base=4.0))
        rep = run([src12 / "WWN_x_1m.csv", src12 / "WWO_x_1m.csv"], root12)
    finally:
        ss.month_file_path = _orig_mfp
    check("series-level StorageError: rejected series, sibling file lands",
          "series failed" in str(series_of(file_rec(rep, "WWN_x_1m.csv")))
          and series_of(file_rec(rep, "WWO_x_1m.csv"))["added"] == 8,
          str(rep["files"]))

    # 8.3 (MED) rejected-rows evidence must survive hostile cell content
    d13, src13, root13 = fresh()
    lines = ["Date,Time,open,high,low,close,volume"]
    t = datetime(2024, 1, 15, 9, 30, 0)
    for i in range(40):
        tt = t + timedelta(minutes=i)
        lines.append(f"{tt.month}/{tt.day}/{tt.year},"
                     f"{tt.hour}:{tt.minute:02d}:00,5,6,4,5,{10 + i}")
    lines.append('"oops"",""x",10:30:00,5,6,4,5,10')        # quote injection
    (src13 / "ZINJ_x_1m.csv").write_bytes(("\n".join(lines) + "\n").encode())
    rep = run([src13 / "ZINJ_x_1m.csv"], root13)
    rec = file_rec(rep, "ZINJ_x_1m.csv")
    rows_ev = list(_csv.reader(
        Path(rec["rejected_rows_file"]).read_text(encoding="utf-8")
        .splitlines()))
    check("evidence CSV survives quote injection (uniform field counts)",
          all(len(r) == len(rows_ev[0]) for r in rows_ev if r),
          str(rows_ev[:3]))

    # 8.4 (MED) same-basename sources keep SEPARATE rejected evidence
    d14, src14, root14 = fresh()
    for sub, tag, base in (("AAA", "FILE1", 50), ("BBB", "FILE2", 90)):
        dd = src14 / sub
        dd.mkdir()
        lx = ["Date,Time,open,high,low,close,volume"]
        for i in range(40):
            tt = datetime(2024, 1, 15, 9, 30, 0) + timedelta(minutes=i)
            lx.append(f"{tt.month}/{tt.day}/{tt.year},"
                      f"{tt.hour}:{tt.minute:02d}:00,{base},{base + 1},"
                      f"{base - 1},{base},{10 + i}")
        lx.append(f"BADROW-{tag},10:30:00,5,6,4,5,10")
        (dd / "KOO_x_1m.csv").write_bytes(("\n".join(lx) + "\n").encode())
    rep = run([src14], root14)
    ev_paths = {r.get("rejected_rows_file") for r in rep["files"]
                if r.get("rejected_rows_file")}
    ev_text = "".join(Path(p).read_text(encoding="utf-8") for p in ev_paths)
    check("same-basename evidence files kept apart, both preserved",
          len(ev_paths) == 2 and "FILE1" in ev_text and "FILE2" in ev_text,
          str(ev_paths))

    # 8.5 (LOW) duplicate OHLCV column -> loud note, first one used
    dup = ["Date,Time,open,high,low,close,close,volume"]
    for b in mk_bars(2024, 1, 15, 12, base=20.0):
        dup.append(f"{ss.format_bar(*b).rsplit(',', 1)[0]},999,"
                   f"{b[5]}")                       # second 'close' = 999
    (src14 / "WWP_x_1m.csv").write_bytes(("\n".join(dup) + "\n").encode())
    rep = run([src14 / "WWP_x_1m.csv"], root14)
    rec = file_rec(rep, "WWP_x_1m.csv")
    rbp, _ = ss.read_month_file(month_path(root14, "WWP", 2024, 1, "1m"))
    check("duplicate close column: noted, FIRST one stored",
          any("DUPLICATE close" in n for n in rec["notes"])
          and rbp[0][4] == 20.01, str(rec["notes"]) + f" close={rbp[0][4]}")

    # 8.6 gated series' dry-run overlap stays OUT of the merge totals
    d15, src15, root15 = fresh()
    gb = mk_bars(2024, 2, 19, 120, base=200.0)
    write_native(src15 / "WWQ_a_1m.csv", gb)
    run([src15 / "WWQ_a_1m.csv"], root15)
    write_native(src15 / "WWQ_b_1m.csv",
                 [(b[0], b[1] + 1, b[2] + 1, b[3] + 1, b[4] + 1, b[5])
                  for b in gb])
    rep = run([src15 / "WWQ_b_1m.csv"], root15)
    t_ = rep["totals"]
    check("gate: dry-run overlap not counted as kept conflicts",
          t_.get("conflicts", 0) == 0 and t_.get("gated_overlap") == 120
          and t_.get("gated_series") == 1, str(t_))
    check("gate: head line says GATED with withheld count",
          "GATED" in si.summarize_report(rep)[0]
          and "120" in si.summarize_report(rep)[0],
          si.summarize_report(rep)[0])

    print("=== [9] parallel parse ============================================")
    # (a) serial vs parallel: byte-identical trees, identical reports
    dS, srcS, rootS = fresh()
    build_p9_corpus(srcS)
    dP, srcP, rootP = fresh()
    build_p9_corpus(srcP)
    os.environ[si.INGEST_WORKERS_ENV] = "1"
    try:
        repS = run([srcS], rootS)
    finally:
        os.environ.pop(si.INGEST_WORKERS_ENV, None)
    with parallel_env(3):
        repP = run([srcP], rootP)
    check("parallel engaged (workers=3); serial run stayed serial",
          repP.get("parallel", {}).get("workers") == 3
          and "parallel" not in repS, str(repP.get("parallel")))
    snapS, snapP = tree_snapshot(rootS, srcS), tree_snapshot(rootP, srcP)
    diff = (set(snapS) ^ set(snapP)) or {k for k in snapS
                                         if snapS[k] != snapP[k]}
    check("tree byte-equality: parallel == serial", snapS == snapP,
          f"diffs: {sorted(diff)[:3]}")
    nS, nP = norm_report(repS, rootS, srcS), norm_report(repP, rootP, srcP)
    check("normalized reports equal", nS == nP,
          next((f"{k}" for k in nS if nS.get(k) != nP.get(k)), "?"))
    fp9 = si._parse_one(srcS / "PEE_x_1m.csv")
    fp9b = pickle.loads(pickle.dumps(fp9))
    check("payload pickle round-trip preserves the evidence",
          fp9b.groups.keys() == fp9.groups.keys()
          and fp9b.rejected == fp9.rejected
          and fp9b.counters == fp9.counters and fp9b.msgs == fp9.msgs)

    # (b) cancel mid-parallel-run: committed months stay, rest drains
    dB, srcB, rootB = fresh()
    for i in range(1, 9):
        write_native(srcB / f"PC{i}_2024_01_1m.csv",
                     mk_bars(2024, 1, 15, 30, float(i * 10)))
    ev9 = threading.Event()
    merges = [0]

    def watch9(m):
        if ": merging" in m:
            merges[0] += 1
            if merges[0] == 3:
                ev9.set()

    with parallel_env(3):
        repB = run([srcB], rootB, progress=watch9, cancel=ev9)
    present = [i for i in range(1, 9)
               if month_path(rootB, f"PC{i}", 2024, 1, "1m").exists()]
    check("cancel mid-run: only pre-cancel files committed",
          present == [1, 2], str(present))
    check("cancel mid-run: flagged + report saved",
          repB["cancelled"] and Path(repB["report_path"]).is_file())
    later = [r for r in repB["files"]
             if Path(r["path"]).name[2] in "5678"]
    check("cancel mid-run: queued files recorded as not started",
          len(later) == 4 and all(r["status"] == "not started (cancelled)"
                                  for r in later),
          str([(Path(r['path']).name, r['status']) for r in later]))
    with parallel_env(3):
        repB2 = run([srcB], rootB)
    present2 = [i for i in range(1, 9)
                if month_path(rootB, f"PC{i}", 2024, 1, "1m").exists()]
    check("cancel mid-run: re-run completes idempotently",
          present2 == list(range(1, 9)) and not repB2["cancelled"]
          and series_of(file_rec(repB2, "PC1_2024_01_1m.csv"))["added"]
          == 0, str(present2))

    # (c) forced pool failures degrade to the serial path, losslessly
    dC, srcC, rootC = fresh()
    build_p9_corpus(srcC)

    def _boom_pool(w):
        raise RuntimeError("no pool for you")

    real_make = si._make_pool
    si._make_pool = _boom_pool
    try:
        with parallel_env(3):
            repC = run([srcC], rootC)
    finally:
        si._make_pool = real_make
    check("pool-creation failure: degrades, completes, tree identical",
          tree_snapshot(rootC, srcC) == snapS
          and norm_report(repC, rootC, srcC) == nS
          and any("pool unavailable" in n
                  for n in repC["parallel"]["notes"]),
          str(repC.get("parallel")))

    dC2, srcC2, rootC2 = fresh()
    build_p9_corpus(srcC2)

    class _FakeExec:
        def __init__(self):
            self.k = 0

        def submit(self, fn, path_str):
            self.k += 1
            k = self.k

            class _F:
                def result(_self):
                    if k == 3:
                        raise RuntimeError("worker died")
                    return fn(path_str)

                def cancel(_self):
                    pass
            return _F()

        def shutdown(self, **kw):
            pass

    si._make_pool = lambda w: _FakeExec()
    try:
        with parallel_env(3):
            repC2 = run([srcC2], rootC2)
    finally:
        si._make_pool = real_make
    check("mid-stream worker death: serial resume, no loss, no dupes",
          tree_snapshot(rootC2, srcC2) == snapS
          and norm_report(repC2, rootC2, srcC2) == nS
          and any("parallel parse failed" in n
                  for n in repC2["parallel"]["notes"]),
          str(repC2.get("parallel")))

    # (d) parallel run-to-run determinism
    dD, srcD, rootD = fresh()
    build_p9_corpus(srcD)
    with parallel_env(3):
        repD = run([srcD], rootD)
    check("parallel run-to-run determinism",
          tree_snapshot(rootD, srcD) == snapP
          and norm_report(repD, rootD, srcD) == nP)

    print("=== [10] verify: ledger fast path =================================")
    dV, srcV, rootV = fresh()
    build_p9_corpus(srcV)
    with parallel_env(3):
        run([srcV], rootV)
    check("ledger written by ingest",
          (rootV / si.REPORTS_DIR / si.LEDGER_NAME).is_file())
    repV = si.verify_paths([srcV], rootV)
    fv = {Path(r["path"]).name: r for r in repV["files"]}
    check("verify: whole folder resolved WITHOUT parsing",
          repV["totals"].get("via_parse", 0) == 0
          and repV["totals"].get("via_ledger", 0) == len(repV["files"]),
          str(repV["totals"]))
    check("verify: verdict census (11 contained / 2 except / 5 not)",
          repV["totals"].get("contained") == 11
          and repV["totals"].get("contained-except") == 2
          and repV["totals"].get("not-contained") == 5,
          str(repV["totals"]))
    check("verify: gated + quarantined files are called out",
          "GATED" in fv["PBB_b_1m.csv"].get("detail", "")
          and fv["PDD_ext_1m.csv"]["verdict"] == "not-contained",
          str(fv["PBB_b_1m.csv"]))
    check("verify: conflict file contained-except with counts",
          fv["PCC_b_1m.csv"]["verdict"] == "contained-except"
          and "50" in fv["PCC_b_1m.csv"].get("detail", ""),
          str(fv["PCC_b_1m.csv"]))
    lines = si.summarize_verify(repV)
    check("verify: summary head line renders",
          lines and lines[0].startswith("VERIFY ")
          and "11 of 18" in lines[0], str(lines[:1]))

    shutil.copy2(srcV / "PNA_2024_01_1m.csv", srcV / "renamed_blob.csv")
    repV2 = si.verify_paths([srcV / "renamed_blob.csv"], rootV)
    r2 = repV2["files"][0]
    check("verify: renamed byte-identical copy contained via ledger",
          r2["verdict"] == "contained" and r2["via"] == "ledger", str(r2))

    month_path(rootV, "PNB", 2024, 1, "1m").unlink()
    repV3 = si.verify_paths([srcV / "PNB_2024_01_1m.csv"], rootV)
    r3 = repV3["files"][0]
    check("verify: deleted month file detected (parse, missing rows)",
          r3["verdict"] == "not-contained" and r3["via"] == "parse"
          and "missing" in r3.get("detail", ""), str(r3))

    write_native(srcV / "PNA_subset.csv", mk_bars(2024, 1, 15, 30, 10.0))
    repV4 = si.verify_paths([srcV / "PNA_subset.csv"], rootV)
    repV5 = si.verify_paths([srcV / "PNA_subset.csv"], rootV)
    check("verify: unknown-but-contained file parses once, then ledger",
          repV4["files"][0]["verdict"] == "contained"
          and repV4["files"][0]["via"] == "parse"
          and repV5["files"][0]["verdict"] == "contained"
          and repV5["files"][0]["via"] == "ledger",
          f"{repV4['files'][0]} / {repV5['files'][0]}")

    write_native(srcV / "PNA_extra_1m.csv",
                 mk_bars(2024, 1, 15, 60, 10.0)
                 + mk_bars(2024, 1, 16, 5, 10.7))
    repV6 = si.verify_paths([srcV / "PNA_extra_1m.csv"], rootV)
    check("verify: extra unstored rows -> not contained (5 missing)",
          repV6["files"][0]["verdict"] == "not-contained"
          and "5" in repV6["files"][0].get("detail", ""),
          str(repV6["files"][0]))

    with open(si._ledger_path(rootV), "a", encoding="utf-8") as fh:
        fh.write('{"sha256": "deadbeef", "trunc')
    repV7 = si.verify_paths([srcV / "PNA_2024_01_1m.csv"], rootV)
    check("verify: torn ledger tail tolerated, fast path intact",
          repV7["files"][0]["verdict"] == "contained"
          and repV7["files"][0]["via"] == "ledger",
          str(repV7["files"][0]))

    # the same fast path INSIDE ingest (the GUI folder flow)
    dW, srcW, rootW = fresh()
    build_p9_corpus(srcW)
    with parallel_env(3):
        run([srcW], rootW)
    snapW = tree_snapshot(rootW, srcW)
    with parallel_env(3):
        repW = run([srcW], rootW, skip_contained=True)
    skipped = [r for r in repW["files"]
               if str(r.get("status", "")).startswith("skipped (already")]
    check("ingest skip_contained: clean files skipped, tree untouched",
          len(skipped) == 11
          and repW["totals"].get("skipped_contained") == 11
          and tree_snapshot(rootW, srcW) == snapW,
          f"skipped={len(skipped)}")
    pcc = file_rec(repW, "PCC_b_1m.csv")
    check("ingest skip_contained: conflict file still fully re-checked",
          pcc["status"] == "ok" and series_of(pcc)["conflicts"] == 50
          and "skipped" in si.summarize_report(repW)[0],
          str(pcc)[:200])

    # bounded month cache: a tiny cap must not change parse-path verdicts
    # (unbounded cache = MemoryError on 18k-file folders, proven live)
    si._ledger_path(rootW).unlink()           # force the parse path
    repX1 = si.verify_paths([srcW], rootW)
    si._ledger_path(rootW).unlink()
    cap = si.VERIFY_MONTH_CACHE_MAX
    si.VERIFY_MONTH_CACHE_MAX = 2
    try:
        repX2 = si.verify_paths([srcW], rootW)
    finally:
        si.VERIFY_MONTH_CACHE_MAX = cap
    check("verify month cache: cap=2 produces identical verdicts",
          norm_report(repX1, rootW, srcW) == norm_report(repX2, rootW,
                                                         srcW)
          and repX1["totals"].get("via_parse", 0) > 0,
          str(repX2["totals"]))

    print("=== [11] basis gate: recorded-factor awareness (M2b) ==============")
    # (a) regression: with NO recorded action a 10x file still gates,
    # and the halt now points at the basis doctor (stock_basis)
    dF, srcF, rootF = fresh()
    ga = mk_bars(2024, 2, 19, 150, base=200.0)
    write_native(srcF / "FGA_a_1m.csv", ga)
    run([srcF / "FGA_a_1m.csv"], rootF)
    write_native(srcF / "FGA_b_1m.csv",
                 [(t, o * 10, h * 10, lo * 10, c * 10, v)
                  for t, o, h, lo, c, v in ga])
    rep = run([srcF / "FGA_b_1m.csv"], rootF)
    s = series_of(file_rec(rep, "FGA_b_1m.csv"))
    check("factor gate (a): no recorded action -> still GATED, no notes",
          bool(s.get("gate")) and "notes" not in s, str(s)[:300])
    check("factor gate (a): halt points at the basis doctor",
          "run the basis doctor (stock_basis)" in s.get("gate", "")
          and "task #23" not in s.get("gate", ""), str(s.get("gate")))

    # (b) a recorded price-basis action whose factor explains the
    # overlap unlocks the merge: new rows land, conflicts keep existing
    bb = mk_bars(2024, 3, 18, 150, base=40.0)
    write_native(srcF / "FGB_a_1m.csv", bb)
    run([srcF / "FGB_a_1m.csv"], rootF)
    sb.apply_action(rootF, "FGB", {
        "date": "2024-03-19", "kind": "price-basis", "factor": 10.0,
        "applies": "price", "source": "user",
        "evidence": "selftest: vendor file 10x vs base archive",
        "run": None})
    newer = mk_bars(2024, 3, 18, 10, base=400.0, h0=12)
    write_native(srcF / "FGB_b_1m.csv",
                 [(t, o * 10, h * 10, lo * 10, c * 10, v)
                  for t, o, h, lo, c, v in bb] + newer)
    rep = run([srcF / "FGB_b_1m.csv"], rootF)
    s = series_of(file_rec(rep, "FGB_b_1m.csv"))
    check("factor gate (b): recorded factor unlocks the merge",
          not s.get("gate") and s["added"] == 10 and s["conflicts"] == 150
          and s["dup_existing"] == 0 and s["written"] == 1, str(s)[:300])
    rb, _ = ss.read_month_file(month_path(rootF, "FGB", 2024, 3, "1m"))
    keep = [b_ for b_ in rb if b_[0] == bb[0][0]][0]
    check("factor gate (b): existing values kept, new rows on disk",
          len(rb) == 160 and keep == bb[0] and newer[0] in rb,
          f"len={len(rb)} keep={keep}")
    check("factor gate (b): series note names factor + action",
          any("recorded basis factor 10 " in n and "price-basis" in n
              and "existing values still win" in n
              for n in s.get("notes", [])), str(s.get("notes")))
    check("factor gate (b): counted as a normal merge, not gated",
          rep["totals"].get("gated_series", 0) == 0
          and rep["totals"].get("conflicts") == 150
          and rep["totals"].get("added") == 10, str(rep["totals"]))
    check("factor gate (b): summary renders the loud basis note",
          any("recorded basis factor" in ln
              for ln in si.summarize_report(rep)),
          str(si.summarize_report(rep)[:4]))
    repVb = si.verify_paths([srcF / "FGB_b_1m.csv"], rootF)
    rv = repVb["files"][0]
    check("factor gate (b): normal ledger record (verify via ledger)",
          rv["verdict"] == "contained-except" and rv.get("via") == "ledger"
          and "150" in rv.get("detail", ""), str(rv))

    # (c) a volume-scale action never unlocks a PRICE disagreement
    cc = mk_bars(2024, 4, 15, 150, base=60.0)
    write_native(srcF / "FGC_a_1m.csv", cc)
    run([srcF / "FGC_a_1m.csv"], rootF)
    sb.apply_action(rootF, "FGC", {
        "date": "2024-04-16", "kind": "volume-scale", "factor": 10.0,
        "applies": "volume", "source": "user",
        "evidence": "selftest: volume basis only", "run": None})
    write_native(srcF / "FGC_b_1m.csv",
                 [(t, o * 10, h * 10, lo * 10, c * 10, v)
                  for t, o, h, lo, c, v in cc])
    pc = month_path(rootF, "FGC", 2024, 4, "1m")
    before = pc.read_bytes()
    rep = run([srcF / "FGC_b_1m.csv"], rootF)
    s = series_of(file_rec(rep, "FGC_b_1m.csv"))
    check("factor gate (c): volume-scale action does NOT unlock price",
          bool(s.get("gate")) and pc.read_bytes() == before,
          str(s.get("gate")))

    # (d) a recorded factor that does not match the data still gates
    dd_ = mk_bars(2024, 5, 13, 150, base=80.0)
    write_native(srcF / "FGD_a_1m.csv", dd_)
    run([srcF / "FGD_a_1m.csv"], rootF)
    sb.apply_action(rootF, "FGD", {
        "date": "2024-05-14", "kind": "price-basis", "factor": 2.0,
        "applies": "price", "source": "user",
        "evidence": "selftest: recorded 2x, file is 10x", "run": None})
    write_native(srcF / "FGD_b_1m.csv",
                 [(t, o * 10, h * 10, lo * 10, c * 10, v)
                  for t, o, h, lo, c, v in dd_])
    rep = run([srcF / "FGD_b_1m.csv"], rootF)
    s = series_of(file_rec(rep, "FGD_b_1m.csv"))
    check("factor gate (d): non-matching recorded factor still GATES",
          bool(s.get("gate")) and "basis doctor" in s.get("gate", ""),
          str(s.get("gate")))

    print("=== [12] partial last month is extended, never skipped ============")
    dY, srcY, rootY = fresh()
    half = mk_bars(2024, 5, 13, 60, 30.0)          # half a month stored
    write_native(srcY / "PQQ_2024_05_1m.csv", half)
    run([srcY / "PQQ_2024_05_1m.csv"], rootY)
    full = half + mk_bars(2024, 5, 20, 60, 30.7)   # fuller re-export,
    write_native(srcY / "PQQ_2024_05_full_1m.csv", full)  # same month
    repY0 = si.verify_paths([srcY / "PQQ_2024_05_full_1m.csv"], rootY)
    check("verify never claims a half month contains the full month",
          repY0["files"][0]["verdict"] == "not-contained"
          and "60" in repY0["files"][0].get("detail", ""),
          str(repY0["files"][0]))
    repY = run([srcY / "PQQ_2024_05_full_1m.csv"], rootY,
               skip_contained=True)
    sY = series_of(file_rec(repY, "PQQ_2024_05_full_1m.csv"))
    rbY, _ = ss.read_month_file(month_path(rootY, "PQQ", 2024, 5, "1m"))
    check("fuller file extends the SAME month in place (no skip)",
          sY.get("added") == 60 and len(rbY) == 120
          and not sY.get("gate"), f"added={sY.get('added')} rows="
                                  f"{len(rbY)}")
    repY2 = si.verify_paths([srcY / "PQQ_2024_05_full_1m.csv"], rootY)
    check("after the extension the full-month file verifies contained",
          repY2["files"][0]["verdict"] == "contained",
          str(repY2["files"][0]))

    print("=== [13] ratio ingest: correction transaction fences =============")

    def ratio_fp(src_path, ticker, interval, bars):
        fp = si._FileParse(src_path)
        fp.groups = {ticker: list(bars)}
        fp.raw_symbols = {ticker: {ticker}}
        fp.group_meta = {ticker: {
            "ticker": ticker, "rows": len(bars), "hint": interval,
            "interval": interval, "interval_note": "selftest ratio",
        }}
        return fp

    def record_ratio(root_, fp):
        report = {"files": [], "totals": Counter(), "cancelled": False}
        si._record_file(
            report, si._RunDirs(root_, "ratio-transaction-selftest"), fp,
            "ratio-transaction-selftest", lambda _msg: None, 0, 1, root_,
            None, lambda *_args: None)
        return report["files"][0]

    def ratio_bar(day, value):
        ts = datetime(2024, 6, day, 16, 0)
        return (ts, value, value + 0.01, value - 0.01,
                value + 0.005, 0)

    dR, srcR, rootR = fresh()
    tickerR, intervalR = "VST", "1d-iv"
    stageR = rootR / tickerR / ss.VOL_VALUE_RECONCILE_STAGE_DIR
    stageR.mkdir(parents=True)
    # An empty markerless directory is provably unpublished cleanup debt and
    # is now removed automatically.  Non-empty reconcile-owned evidence must
    # remain fenced from every ordinary writer, regardless of kind.
    (stageR / "transaction.json").write_text(
        "{}", encoding="utf-8")
    fpR = ratio_fp(srcR / "VST_ratio.csv", tickerR, intervalR,
                   [ratio_bar(3, 0.20)])
    entered = []
    real_transaction = ss.ticker_transaction

    @contextmanager
    def tracked_transaction(ticker_dir):
        entered.append(Path(ticker_dir).name)
        with real_transaction(ticker_dir):
            yield

    ss.ticker_transaction = tracked_transaction
    try:
        recR = record_ratio(rootR, fpR)
    finally:
        ss.ticker_transaction = real_transaction
    seriesR = series_of(recR)
    check("ratio stage fence: shared ticker transaction is acquired",
          entered == [tickerR], str(entered))
    check("ratio stage fence: active correction recovery refuses import",
          "rejected" in seriesR and "correction recovery is pending"
          in seriesR.get("rejected", "")
          and not month_path(rootR, tickerR, 2024, 6, intervalR).exists(),
          str(seriesR))
    price = (datetime(2024, 6, 3, 9, 30), 10.0, 10.1, 9.9, 10.05, 10)
    recRP = record_ratio(
        rootR, ratio_fp(srcR / "VST_price.csv", tickerR, "1m", [price]))
    seriesRP = series_of(recRP)
    check("stage fence: price import cannot clobber correction manifest",
          "correction recovery is pending" in seriesRP.get("rejected", "")
          and not month_path(rootR, tickerR, 2024, 6, "1m").exists(),
          str(seriesRP))

    def seed_corrected_month(root_, ticker, manifest_sha_matches=True):
        interval = "1d-hvol"
        bars = [ratio_bar(3, 0.30)]
        path = month_path(root_, ticker, 2024, 6, interval)
        stats = ss.write_month_file(path, bars)
        manifest = ss.new_manifest(ticker, ticker)
        entry = dict(stats, status="present", source="selftest")
        if not manifest_sha_matches:
            entry["sha256"] = "0" * 64
        entry["value_corrections"] = [{
            "day": "2024-06-03", "stored": 0.30, "served": 0.30,
            "request_id": "selftest-correction",
        }]
        ss.manifest_months(manifest, interval)["2024-06"] = entry
        ss.save_manifest(root_ / ticker, manifest)
        return interval, stats, entry["value_corrections"]

    tickerP = "VPR"
    intervalP, beforeP, ledgerP = seed_corrected_month(rootR, tickerP)
    fpP = ratio_fp(srcR / "VPR_ratio.csv", tickerP, intervalP,
                   [ratio_bar(4, 0.31)])
    recP = record_ratio(rootR, fpP)
    afterP = (ss.load_manifest(rootR / tickerP)["intervals"][intervalP]
              ["months"]["2024-06"])
    check("ratio ledger: exact prewrite SHA preserves correction evidence",
          series_of(recP).get("written") == 1
          and afterP.get("value_corrections") == ledgerP
          and afterP.get("sha256") != beforeP.get("sha256"), str(afterP))

    tickerM = "VMM"
    intervalM, _beforeM, _ledgerM = seed_corrected_month(
        rootR, tickerM, manifest_sha_matches=False)
    fpM = ratio_fp(srcR / "VMM_ratio.csv", tickerM, intervalM,
                   [ratio_bar(4, 0.32)])
    recM = record_ratio(rootR, fpM)
    afterM = (ss.load_manifest(rootR / tickerM)["intervals"][intervalM]
              ["months"]["2024-06"])
    seriesM = series_of(recM)
    check("ratio ledger: mismatched manifest/file provenance blocks rewrite",
          seriesM.get("written") == 0
          and seriesM.get("blocked_months")
          and "does not match" in seriesM["blocked_months"][0]["reason"]
          and afterM.get("value_corrections") == _ledgerM
          and afterM.get("sha256") == "0" * 64,
          f"series={seriesM}, manifest={afterM}")

    print("=== [14] whole-run market operation fence ========================")
    dO, srcO, rootO = fresh()
    sourceO = srcO / "OPG_hist_1m.csv"
    write_native(sourceO, mk_bars(2024, 6, 17, 12, 20.0))
    lockO = si._market_operation_path(rootO)
    blocker = operation_gate.acquire(
        "vol_value_audit", owner="ingest overlap selftest", path=lockO)
    try:
        refusedO = run([sourceO], rootO)
    finally:
        blocker.release()
    check("operation fence: busy audit refuses ingest before bank writes",
          refusedO.get("report_path") is None
          and refusedO.get("operation_gate", {}).get("acquired") is False
          and "ingest refused" in refusedO.get("aborted", "")
          and not rootO.exists(), str(refusedO))

    audit_refusals = []

    def overlap_probe(message):
        phase = ("collect" if str(message).startswith("Collecting source")
                 else "finalize" if message == "Ingest finished."
                 else None)
        if phase is None:
            return
        try:
            vva.audit(rootO, write_queue=True)
        except vva.VolValueAuditError as exc:
            audit_refusals.append((phase, str(exc)))
        else:
            audit_refusals.append((phase, "NOT REFUSED"))

    acceptedO = run([sourceO], rootO, progress=overlap_probe)
    check("operation fence: audit is excluded through report finalization",
          [phase for phase, _error in audit_refusals]
          == ["collect", "finalize"]
          and all("refused" in error for _message, error in audit_refusals)
          and acceptedO.get("report_path")
          and Path(acceptedO["report_path"]).is_file(),
          repr(audit_refusals))
    afterO = operation_gate.acquire(
        "vol_value_audit", owner="post-ingest selftest", path=lockO)
    afterO.release()
    check("operation fence: successful ingest releases the root-local lease",
          afterO.released
          and month_path(rootO, "OPG", 2024, 6, "1m").is_file(),
          str(acceptedO))

    print()
    print(f"{N[0]} checks, {len(FAILS)} failed")
    if FAILS:
        for f_ in FAILS:
            print(f"  FAILED: {f_}")
        sys.exit(1)
    print("ALL PASS")


if __name__ == "__main__":
    _run()
