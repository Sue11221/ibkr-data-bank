"""Self-tests for stock_storage.py — run any time: python stock_storage_selftest.py

Byte-exact golden tests use the REAL first rows of the user's archive
(AAPL_10Y_1m.csv), so a pandas/Python upgrade or an edit that drifts the
format fails here before it can fork the tree. ASCII output only."""

import os
import shutil
import sys
import tempfile
import threading
import time as _time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss  # noqa: E402

FAILURES = []


def check(name, cond, detail=""):
    if cond:
        print(f"  ok    {name}")
    else:
        FAILURES.append(name)
        print(f"  FAIL  {name}  {detail}")


def expect_raise(name, fn, exc=ss.StorageError):
    try:
        fn()
    except exc:
        print(f"  ok    {name}")
    except Exception as e:  # noqa: BLE001
        FAILURES.append(name)
        print(f"  FAIL  {name}  wrong exception {type(e).__name__}: {e}")
    else:
        FAILURES.append(name)
        print(f"  FAIL  {name}  did not raise")


GOLDEN_BYTES = (b"Date,Time,open,high,low,close,volume\r\n"
                b"6/15/2015,9:30:00,28.24,28.25,28.18,28.18,2641532\r\n"
                b"6/15/2015,9:31:00,28.17,28.21,28.16,28.2,313860\r\n")
GOLDEN_BARS = [
    (datetime(2015, 6, 15, 9, 30, 0), 28.24, 28.25, 28.18, 28.18, 2641532),
    (datetime(2015, 6, 15, 9, 31, 0), 28.17, 28.21, 28.16, 28.2, 313860),
]


def main():
    tmp = Path(tempfile.mkdtemp(prefix="ss_selftest_"))
    try:
        run(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILURE(S): {FAILURES}")
        return 1
    print("ALL STORAGE SELF-TESTS PASSED")
    return 0


def run(tmp):
    root = ss.storage_root(tmp)

    print("[1] golden byte-exact round trip (real archive rows) — CSV codec")
    p = ss.month_file_path(root, "AAPL", 2015, 6, "1m", fmt="csv")
    stats = ss.write_month_file(p, GOLDEN_BARS)
    check("write returns row count", stats["rows"] == 2)
    check("bytes identical to the real archive format",
          p.read_bytes() == GOLDEN_BYTES,
          repr(p.read_bytes()[:90]))
    bars, rstats = ss.read_month_file(p)
    check("read-back equals input bars", bars == GOLDEN_BARS)
    p2 = ss.month_file_path(root, "AAPL2", 2015, 6, "1m", fmt="csv")
    ss.write_month_file(p2, bars)
    check("second-generation bytes identical (round-trip stable)",
          p2.read_bytes() == GOLDEN_BYTES)

    print("[1b] Parquet codec — default format, lossless round trip")
    pq = ss.month_file_path(root, "AAPLPQ", 2015, 6, "1m")
    check("default month-file is .parquet", pq.suffix == ".parquet")
    pqstats = ss.write_month_file(pq, GOLDEN_BARS)
    pqbars, _ = ss.read_month_file(pq)
    check("parquet read-back equals input bars (lossless)", pqbars == GOLDEN_BARS)
    check("parquet export identity (stored floats -> canonical CSV)",
          [ss.format_bar(*b) for b in pqbars]
          == [ss.format_bar(*b) for b in GOLDEN_BARS])
    check("find_month_file prefers parquet",
          ss.find_month_file(root, "AAPLPQ", 2015, 6, "1m").suffix == ".parquet")
    try:
        import numpy as np
        want_ts = [int((b[0] - ss._TS_EPOCH).total_seconds())
                   for b in GOLDEN_BARS]

        def _col_lists(cols):
            return [c.tolist() for c in cols]

        want_cols = [want_ts,
                     [b[1] for b in GOLDEN_BARS],
                     [b[2] for b in GOLDEN_BARS],
                     [b[3] for b in GOLDEN_BARS],
                     [b[4] for b in GOLDEN_BARS],
                     [b[5] for b in GOLDEN_BARS]]
        cols = ss.read_month_file_cols(pq, pqstats["sha256"])
        check("read_month_file_cols parquet sha-match -> six numpy columns",
              _col_lists(cols) == want_cols
              and cols[0].dtype == np.int64 and cols[5].dtype == np.int64)
        check("read_month_file_cols sha mismatch strict-fallback equal",
              _col_lists(ss.read_month_file_cols(pq, "bad-sha")) == want_cols)
        check("read_month_file_cols CSV/no-sha strict-fallback equal",
              _col_lists(ss.read_month_file_cols(p, None)) == want_cols)
    except ImportError:
        print("  info  numpy not installed - columnar read tests skipped")

    print("[2] value formatting edges")
    check("trailing zero trimmed", ss.format_price(28.20) == "28.2")
    check("integer-valued price trimmed", ss.format_price(198.0) == "198")
    check("six-figure price with cents",
          ss.format_price(712345.05) == "712345.05")
    check("penny stock", ss.format_price(0.27) == "0.27")
    check("sub-penny survives", ss.format_price(27.1234) == "27.1234")
    check("tiny price avoids scientific notation",
          ss.format_price(0.00001) == "0.00001")
    check("round-trip float identity",
          float(ss.format_price(198.85)) == 198.85)
    check("unpadded date", ss.format_date(datetime(2015, 6, 1)) == "6/1/2015")
    check("unpadded hour",
          ss.format_time(datetime(2015, 6, 1, 9, 5, 7).time()) == "9:05:07")

    print("[3] contract rejections (writer)")
    d = datetime(2023, 1, 3, 10, 0, 0)
    good = (d, 10.0, 11.0, 9.0, 10.5, 100)
    pth = ss.month_file_path(root, "T", 2023, 1, "1m")
    expect_raise("empty month refused",
                 lambda: ss.write_month_file(pth, []))
    expect_raise("premarket bar refused", lambda: ss.write_month_file(
        pth, [(datetime(2023, 1, 3, 9, 29, 0), 10, 11, 9, 10, 1)]))
    expect_raise("16:00 auction bar refused", lambda: ss.write_month_file(
        pth, [(datetime(2023, 1, 3, 16, 0, 0), 10, 11, 9, 10, 1)]))
    expect_raise("weekend bar refused", lambda: ss.write_month_file(
        pth, [(datetime(2023, 1, 7, 10, 0, 0), 10, 11, 9, 10, 1)]))
    expect_raise("high<low refused", lambda: ss.write_month_file(
        pth, [(d, 10.0, 9.0, 11.0, 10.0, 1)]))
    expect_raise("close above high refused", lambda: ss.write_month_file(
        pth, [(d, 10.0, 11.0, 9.0, 11.5, 1)]))
    expect_raise("zero price refused", lambda: ss.write_month_file(
        pth, [(d, 0.0, 11.0, 0.0, 10.0, 1)]))
    expect_raise("zero remains invalid through the ordinary price formatter",
                 lambda: ss.format_price(0.0), ss.StorageFormatError)
    check("ratio-aware formatter encodes zero canonically",
          ss.format_price(0.0, allow_zero=True) == "0")
    ratio_zero = (datetime(2023, 1, 3, 0, 0, 0),
                  0.0, 0.0, 0.0, 0.0, 0)
    ratio_pq = ss.month_file_path(root, "RZ", 2023, 1, "1d-hvol")
    ratio_csv = ss.month_file_path(
        root, "RZ", 2023, 1, "1d-hvol", fmt="csv")
    ratio_pq_stats = ss.write_month_file(ratio_pq, [ratio_zero])
    ratio_csv_stats = ss.write_month_file(ratio_csv, [ratio_zero])
    check("flat-zero HVOL survives strict Parquet write/read",
          ratio_pq_stats["rows"] == 1
          and ss.read_month_file(ratio_pq)[0] == [ratio_zero])
    check("flat-zero HVOL survives strict CSV codec write/read",
          ratio_csv_stats["rows"] == 1
          and ss.read_month_file(ratio_csv)[0] == [ratio_zero]
          and b",0,0,0,0,0\r\n" in ratio_csv.read_bytes())
    ratio_negative = (datetime(2023, 1, 3, 0, 0, 0),
                      -0.01, 0.0, -0.01, 0.0, 0)
    expect_raise("negative ratio price remains invalid",
                 lambda: ss.write_month_file(ratio_pq, [ratio_negative]),
                 ss.StorageFormatError)
    expect_raise("negative volume refused", lambda: ss.write_month_file(
        pth, [(d, 10.0, 11.0, 9.0, 10.5, -1)]))
    expect_raise("float volume refused", lambda: ss.write_month_file(
        pth, [(d, 10.0, 11.0, 9.0, 10.5, 100.0)]))
    expect_raise("duplicate timestamp refused", lambda: ss.write_month_file(
        pth, [good, good]))
    expect_raise("cross-month bar refused", lambda: ss.write_month_file(
        pth, [good, (datetime(2023, 2, 1, 10, 0), 10, 11, 9, 10, 1)]))
    check("zero-volume bar ACCEPTED (BRK.A minutes)",
          ss.write_month_file(pth, [(d, 10.0, 11.0, 9.0, 10.5, 0)])["rows"]
          == 1)

    print("[4] strict reader rejects drift")
    drift = root / "T" / "2023" / "01-Jan"
    lf = drift / "T_2023-01_5m.csv"
    lf.write_bytes(b"Date,Time,open,high,low,close,volume\n"
                   b"1/3/2023,10:00:00,10,11,9,10.5,100\n")
    expect_raise("LF-only endings rejected",
                 lambda: ss.read_month_file(lf), ss.StorageFormatError)
    pad = drift / "T_2023-01_2m.csv"
    pad.write_bytes(b"Date,Time,open,high,low,close,volume\r\n"
                    b"01/03/2023,10:00:00,10,11,9,10.5,100\r\n")
    expect_raise("zero-padded date rejected",
                 lambda: ss.read_month_file(pad), ss.StorageFormatError)
    hdr = drift / "T_2023-01_3m.csv"
    hdr.write_bytes(b"date,time,Open,High,Low,Close,Volume\r\n")
    expect_raise("wrong-case header rejected",
                 lambda: ss.read_month_file(hdr), ss.StorageFormatError)
    utf = drift / "T_2023-01_4m.csv"
    utf.write_bytes(b"\xef\xbb\xbfDate,Time,open,high,low,close,volume\r\n")
    expect_raise("UTF-8 BOM rejected",
                 lambda: ss.read_month_file(utf), ss.StorageFormatError)

    print("[5] atomic replace survives a transient Excel-style lock")
    lock_target = ss.month_file_path(root, "LOCK", 2023, 1, "1m")
    ss.write_month_file(lock_target, [good])
    fh = open(lock_target, "r")          # Windows: blocks os.replace
    threading.Timer(0.7, fh.close).start()
    t0 = _time.perf_counter()
    ss.write_month_file(lock_target, [good,
                        (datetime(2023, 1, 3, 10, 1), 10.5, 11, 10, 11, 5)])
    waited = _time.perf_counter() - t0
    bars2, _ = ss.read_month_file(lock_target)
    check("write retried past the lock and succeeded",
          len(bars2) == 2 and waited >= 0.5, f"waited {waited:.2f}s")
    check("no temp litter left behind",
          not list(lock_target.parent.glob("*.tmp")))

    print("[6] ticker canonicalization")
    check("lowercase normalized", ss.canonical_ticker("ko") == "KO")
    check("dot class share", ss.canonical_ticker("BRK.B") == "BRK-B")
    check("space class share", ss.canonical_ticker("brk b") == "BRK-B")
    check("slash class share", ss.canonical_ticker("BRK/B") == "BRK-B")
    check("windows reserved escaped", ss.canonical_ticker("PRN") == "PRN-")
    check("trailing dot stripped", ss.canonical_ticker("AAPL.") == "AAPL")
    expect_raise("absurd symbol refused",
                 lambda: ss.canonical_ticker("THIS-IS-WAY-TOO-LONG"))
    expect_raise("empty symbol refused", lambda: ss.canonical_ticker("  "))

    print("[7] month bucketing from each bar's own date")
    seam = [(datetime(2025, 12, 31, 15, 59), 1.0, 1.0, 1.0, 1.0, 1),
            (datetime(2026, 1, 2, 9, 30), 1.0, 1.0, 1.0, 1.0, 1)]
    buckets = ss.bucket_bars_by_month(seam)
    check("Dec31->Jan2 lands in two buckets",
          set(buckets) == {(2025, 12), (2026, 1)})

    print("[8] scanner: whitelist, manifest reconcile, tombstones")
    sroot = ss.storage_root(tmp / "scan")
    ko = ss.month_file_path(sroot, "KO", 2025, 11, "1s")
    ss.write_month_file(ko, [(datetime(2025, 11, 3, 9, 30, 0),
                              60.0, 60.1, 59.9, 60.05, 1200),
                             (datetime(2025, 11, 3, 9, 30, 1),
                              60.05, 60.05, 60.0, 60.0, 0)])
    aapl = ss.month_file_path(sroot, "AAPL", 2025, 12, "1m")
    ss.write_month_file(aapl, [(datetime(2025, 12, 1, 9, 30),
                                250.0, 251.0, 249.5, 250.5, 9000)])
    (sroot / ".DS_Store").write_bytes(b"junk")
    (sroot / "backup of stuff").mkdir()
    (sroot / "KO" / "2025" / "11-Nov" / "notes.txt").write_text("hi")
    (sroot / "KO" / "2025" / "11-Nov" / "AAPL_2025-11_1m.csv").write_bytes(
        b"misplaced")
    (sroot / "KO" / "manifest.json").write_text("{ corrupt json",
                                                encoding="utf-8")
    res = ss.scan_storage(sroot)
    check("both tickers found", set(res["tickers"]) == {"AAPL", "KO"})
    check("KO 1s coverage right",
          res["tickers"]["KO"]["intervals"]["1s"]["rows"] == 2)
    badnames = {Path(p).name for p, _r in res["unrecognized"]}
    check("junk + misplaced file inert",
          {".DS_Store", "backup of stuff", "notes.txt",
           "AAPL_2025-11_1m.csv"} <= badnames, str(badnames))
    check("corrupt manifest rebuilt with warning",
          any("manifest unreadable" in w for w in res["warnings"]))
    man = ss.load_manifest(sroot / "KO")
    check("rebuilt manifest persisted",
          man is not None and "1s" in man["intervals"])
    res2 = ss.scan_storage(sroot)
    check("second scan clean (fast path, no rewrites)",
          set(res2["tickers"]) == {"AAPL", "KO"} and not res2["missing"])
    aapl.unlink()                         # simulate AV quarantine / deletion
    res3 = ss.scan_storage(sroot)
    check("vanished file becomes a MISSING tombstone",
          ("AAPL", "1m", "2025-12") in res3["missing"])
    check("tombstone survives in manifest",
          ss.load_manifest(sroot / "AAPL")["intervals"]["1m"]["months"]
          ["2025-12"]["status"] == "MISSING")
    res4 = ss.scan_storage(sroot)
    check("tombstone not re-reported as new", res4["missing"] == [])

    pinp = ss.month_file_path(sroot, "PIN", 2025, 10, "1m")
    ss.write_month_file(pinp, [(datetime(2025, 10, 1, 9, 30),
                                10.0, 10.1, 9.9, 10.0, 100)])
    orig_read = ss.read_month_file
    fired = {"x": False}

    def patched_read(path):
        if (not fired["x"]) and Path(path).name.startswith("PIN_"):
            fired["x"] = True

            def pin_manifest():
                manp = ss.new_manifest("PIN", "PIN")
                manp["conid"] = 123456
                manp["name"] = "Pinned Name"
                manp["aliases"] = ["PIN-OLD"]
                manp["basis"] = "raw"
                manp["actions"] = [{"date": "2025-10-01", "kind": "split",
                                     "factor": 2.0, "applies": "price"}]
                ss.manifest_months(manp, "1m")
                manp["intervals"]["1m"]["verified_absent"] = ["2025-10-02"]
                ss.save_manifest(sroot / "PIN", manp)

            th = threading.Thread(target=pin_manifest)
            th.start()
            th.join()
        return orig_read(path)

    ss.read_month_file = patched_read
    try:
        ss.scan_storage(sroot, workers=1)
    finally:
        ss.read_month_file = orig_read
    pinm = ss.load_manifest(sroot / "PIN") or {}
    pini = ((pinm.get("intervals") or {}).get("1m") or {})
    check("scan manifest save: concurrent conid/name pin survives",
          fired["x"] and pinm.get("conid") == 123456
          and pinm.get("name") == "Pinned Name"
          and pinm.get("aliases") == ["PIN-OLD"]
          and pinm.get("basis") == "raw"
          and pinm.get("actions")
          and "2025-10" in (pini.get("months") or {}),
          str(pinm))
    check("scan manifest save: verified_absent survives fresh merge",
          pini.get("verified_absent") == ["2025-10-02"], str(pini))

    print("[8b] interval-state provenance fingerprint")
    froot = ss.storage_root(tmp / "fingerprint")
    fpdir = froot / "FP"
    fpdir.mkdir(parents=True)

    def fp_entry(sha, rows=1):
        return {"status": "present", "sha256": sha, "rows": rows,
                "first": "1/2/2025 9:30:00", "last": "1/2/2025 15:59:00"}

    fman = ss.new_manifest("FP", "FP")
    ss.manifest_months(fman, "1m")["2025-01"] = fp_entry("a" * 64)
    ss.manifest_months(fman, "1d")["2025-01"] = fp_entry("b" * 64)
    ss.save_manifest(fpdir, fman)
    bank_state1 = ss.bank_manifest_state_fingerprint(froot)
    check("bank state fingerprint is deterministic without a manifest write",
          bank_state1 == ss.bank_manifest_state_fingerprint(froot)
          and bank_state1.get("schema_version")
          == ss.BANK_STATE_FINGERPRINT_VERSION
          and bank_state1.get("manifest_count") == 1)
    fp1 = ss.interval_state_fingerprint(froot, "fp", "1m")
    ss.save_manifest(fpdir, fman)  # generation/written_by are excluded
    bank_state2 = ss.bank_manifest_state_fingerprint(froot)
    check("bank state fingerprint changes on a metadata-only manifest save",
          bank_state2["sha256"] != bank_state1["sha256"])
    fp2 = ss.interval_state_fingerprint(froot, "FP", "1m")
    check("interval fingerprint is deterministic across metadata-only save",
          fp1 == fp2, (fp1, fp2))
    check("interval fingerprint contract identifies exact series",
          fp1.get("schema_version") == ss.INTERVAL_FINGERPRINT_VERSION
          and fp1.get("ticker") == "FP" and fp1.get("interval") == "1m"
          and fp1.get("month_count") == 1)

    ss.manifest_months(fman, "1d")["2025-01"] = fp_entry("c" * 64)
    ss.save_manifest(fpdir, fman)
    fp_other = ss.interval_state_fingerprint(froot, "FP", "1m")
    check("unrelated interval change does not stale selected interval",
          fp_other == fp1, (fp1, fp_other))
    ss.manifest_months(fman, "1m")["2025-01"] = fp_entry("a" * 64, rows=2)
    ss.save_manifest(fpdir, fman)
    fp_month = ss.interval_state_fingerprint(froot, "FP", "1m")
    check("selected interval month-state change alters fingerprint",
          fp_month["sha256"] != fp1["sha256"])
    fman["intervals"]["1m"]["verified_absent"] = [
        "2025-01-04", "2025-01-03"]
    ss.save_manifest(fpdir, fman)
    fp_absent = ss.interval_state_fingerprint(froot, "FP", "1m")
    check("verified-absent state alters fingerprint",
          fp_absent["sha256"] != fp_month["sha256"]
          and fp_absent["verified_absent_count"] == 2)
    fman["intervals"]["1m"]["verified_absent"].reverse()
    ss.save_manifest(fpdir, fman)
    check("verified-absent ordering is canonical",
          ss.interval_state_fingerprint(froot, "FP", "1m") == fp_absent)
    before_backfill = ss.interval_state_fingerprint(froot, "FP", "1m")
    fman["intervals"]["1m"]["backfill_incomplete"] = True
    ss.save_manifest(fpdir, fman)
    during_backfill = ss.interval_state_fingerprint(froot, "FP", "1m")
    check("backfill-incomplete state alters fingerprint v2",
          during_backfill["sha256"] != before_backfill["sha256"]
          and during_backfill["backfill_incomplete"] is True)
    fman["intervals"]["1m"]["backfill_incomplete"] = False
    ss.save_manifest(fpdir, fman)
    check("clearing backfill-incomplete restores canonical state",
          ss.interval_state_fingerprint(froot, "FP", "1m")
          == before_backfill)

    optional_missing = ss.optional_interval_state_fingerprint(
        froot, "FP", "5m")
    check("optional interval fingerprint represents stable absence",
          optional_missing["present"] is False
          and optional_missing["month_count"] == 0
          and optional_missing["backfill_incomplete"] is False)
    present_summary = ss.optional_interval_storage_summary(
        froot, "FP", "1m")
    absent_summary = ss.optional_interval_storage_summary(
        froot, "FP", "5m")
    check("strict interval summary reports canonical month statuses",
          present_summary["present_month_count"] == 1
          and present_summary["missing_month_count"] == 0
          and present_summary["format_error_month_count"] == 0
          and absent_summary["present_month_count"] == 0
          and absent_summary["missing_month_count"] == 0
          and absent_summary["format_error_month_count"] == 0)

    original_stable_read = ss._read_stable_manifest_bytes
    race_calls = [0]

    def persistent_manifest_race(path):
        raw = original_stable_read(path)
        if Path(path).resolve() == (fpdir / ss.MANIFEST_NAME).resolve():
            race_calls[0] += 1
            raced_manifest = ss.load_manifest(fpdir)
            raced_manifest["fingerprint_race_fixture"] = race_calls[0]
            ss.save_manifest(fpdir, raced_manifest)
        return raw

    ss._read_stable_manifest_bytes = persistent_manifest_race
    try:
        expect_raise(
            "bank state fingerprint rejects a persistent cross-manifest-pass race",
            lambda: ss.bank_manifest_state_fingerprint(froot))
    finally:
        ss._read_stable_manifest_bytes = original_stable_read
    check("bank state fingerprint performs a whole-bank stability recheck",
          race_calls[0] >= 2, race_calls[0])

    bad_dir = froot / "BADFP"
    bad_dir.mkdir(parents=True)
    (bad_dir / ss.MANIFEST_NAME).write_text("{broken", encoding="utf-8")
    expect_raise("fingerprint rejects malformed manifest",
                 lambda: ss.interval_state_fingerprint(froot, "BADFP", "1m"))
    expect_raise("bank state fingerprint rejects a malformed active manifest",
                 lambda: ss.bank_manifest_state_fingerprint(froot))
    miss_dir = froot / "MISSFP"
    miss_dir.mkdir(parents=True)
    miss = ss.new_manifest("MISSFP", "MISSFP")
    ss.manifest_months(miss, "1d")["2025-01"] = fp_entry("d" * 64)
    ss.save_manifest(miss_dir, miss)
    expect_raise("fingerprint requires exact interval",
                 lambda: ss.interval_state_fingerprint(froot, "MISSFP", "1m"))
    wrong_dir = froot / "WRONG"
    wrong_dir.mkdir(parents=True)
    wrong = ss.new_manifest("WRONG", "OTHER")
    ss.manifest_months(wrong, "1m")["2025-01"] = fp_entry("e" * 64)
    ss.save_manifest(wrong_dir, wrong)
    expect_raise("fingerprint rejects manifest-folder mismatch",
                 lambda: ss.interval_state_fingerprint(froot, "WRONG", "1m"))

    dup_dir = froot / "DUPFP"
    dup_dir.mkdir(parents=True)
    (dup_dir / ss.MANIFEST_NAME).write_text(
        '{"folder":"DUPFP","folder":"DUPFP","intervals":{"1m":'
        '{"months":{"2025-01":{"status":"present","sha256":"'
        + "f" * 64
        + '","rows":1,"first":"1/2/2025 9:30:00",'
          '"last":"1/2/2025 15:59:00"}},"verified_absent":[]}}}',
        encoding="utf-8")
    expect_raise("fingerprint rejects duplicate JSON object keys",
                 lambda: ss.interval_state_fingerprint(
                     froot, "DUPFP", "1m"))

    incomplete_dir = froot / "INCOMPLETEFP"
    incomplete_dir.mkdir(parents=True)
    incomplete = ss.new_manifest("INCOMPLETEFP", "INCOMPLETEFP")
    ss.manifest_months(incomplete, "1m")["2025-01"] = {
        "status": "present", "rows": 1,
        "first": "1/2/2025 9:30:00", "last": "1/2/2025 15:59:00",
    }
    ss.save_manifest(incomplete_dir, incomplete)
    expect_raise("fingerprint rejects present month without canonical state",
                 lambda: ss.interval_state_fingerprint(
                     froot, "INCOMPLETEFP", "1m"))

    fp_path = fpdir / ss.MANIFEST_NAME
    original_signature = ss._manifest_stat_signature
    signature_calls = [0]

    def one_signature_race(stat):
        signature_calls[0] += 1
        signature = original_signature(stat)
        if signature_calls[0] == 1:
            return signature + ("first-read-race",)
        return signature

    ss._manifest_stat_signature = one_signature_race
    try:
        retried_raw = ss._read_stable_manifest_bytes(fp_path)
    finally:
        ss._manifest_stat_signature = original_signature
    check("stable manifest reader retries a stat-signature race",
          signature_calls[0] >= 8 and retried_raw == fp_path.read_bytes(),
          str(signature_calls[0]))

    original_limit = ss._FINGERPRINT_MANIFEST_MAX_BYTES
    ss._FINGERPRINT_MANIFEST_MAX_BYTES = 1
    try:
        expect_raise("stable manifest reader enforces its size cap",
                     lambda: ss._read_stable_manifest_bytes(fp_path))
    finally:
        ss._FINGERPRINT_MANIFEST_MAX_BYTES = original_limit

    print("[9] environment guards (informational)")
    tz = ss.tzdata_problem()
    print(f"  info  tzdata: {'OK' if tz is None else tz}")
    sw = ss.synced_root_warning(Path.home() / "OneDrive" / "x")
    check("synced-root detector fires on OneDrive path", sw is not None)
    check("synced-root detector quiet on temp",
          ss.synced_root_warning(tmp) is None)

    print("[10] adversarial regressions: format fidelity")
    for v in (8.9285714285714e-05, 9.87654321e-05, 1e-11,
              1.6468924128427563e-10, 712345.0500001):
        check(f"price {v!r} round-trips losslessly",
              float(ss.format_price(v)) == v)
    tb = (datetime(2023, 1, 3, 10, 0), 1e-11, 1e-10, 1e-11, 5e-11, 10)
    ptiny = ss.month_file_path(root, "TINY", 2023, 1, "1m")
    ss.write_month_file(ptiny, [tb])
    bars_t, _ = ss.read_month_file(ptiny)
    check("sub-penny bar survives write->read bitwise", bars_t == [tb])
    expect_raise("microsecond timestamp refused", lambda: ss.write_month_file(
        ptiny, [(datetime(2023, 1, 3, 10, 0, 0, 250000),
                 10.0, 11.0, 9.0, 10.5, 1)]))
    expect_raise("year 999 refused (filename contract)",
                 lambda: ss.write_month_file(
                     ss.month_file_path(root, "OLDY", 999, 1, "1m"),
                     [(datetime(999, 1, 4, 10, 0), 1.0, 1.0, 1.0, 1.0, 1)]))
    expect_raise("year 2101 refused (plausibility bound)",
                 lambda: ss.write_month_file(
                     ss.month_file_path(root, "FUT", 2101, 1, "1m"),
                     [(datetime(2101, 1, 3, 10, 0), 1.0, 1.0, 1.0, 1.0, 1)]))
    expect_raise("bool volume refused", lambda: ss.write_month_file(
        ptiny, [(datetime(2023, 1, 4, 10, 0), 1.0, 1.0, 1.0, 1.0, True)]))
    big = (datetime(2023, 1, 4, 10, 0), 1.0, 1.0, 1.0, 1.0, 2**53 + 1)
    pbig = ss.month_file_path(root, "BIGV", 2023, 1, "1m")
    ss.write_month_file(pbig, [big])
    check("volume > 2**53 round-trips exactly",
          ss.read_month_file(pbig)[0][0][5] == 2**53 + 1)
    try:
        import numpy as np
        pnp = ss.month_file_path(root, "NPV", 2023, 1, "1m")
        stats_np = ss.write_month_file(
            pnp, [(datetime(2023, 1, 4, 10, 0),
                   np.float64(28.24), np.float64(28.25), np.float64(28.18),
                   np.float64(28.2), np.int64(123456))])
        check("numpy float prices + int64 volume accepted",
              stats_np["rows"] == 1)
    except ImportError:
        print("  info  numpy not installed — vendor-scalar test skipped")
    drift2 = root / "T" / "2023" / "01-Jan"
    nc = drift2 / "T_2023-01_6m.csv"
    nc.write_bytes(b"Date,Time,open,high,low,close,volume\r\n"
                   b"1/3/2023,10:00:00,28.20,29,28.1,28.5,100\r\n")
    expect_raise("non-canonical price text '28.20' rejected",
                 lambda: ss.read_month_file(nc), ss.StorageFormatError)
    ooo = drift2 / "T_2023-01_7m.csv"
    ooo.write_bytes(b"Date,Time,open,high,low,close,volume\r\n"
                    b"1/3/2023,10:01:00,10,11,9,10.5,1\r\n"
                    b"1/3/2023,10:00:00,10,11,9,10.5,1\r\n")
    expect_raise("out-of-order rows rejected on read",
                 lambda: ss.read_month_file(ooo), ss.StorageFormatError)

    print("[11] adversarial regressions: concurrency + temp hygiene")
    storm = ss.month_file_path(root, "STORM", 2024, 2, "1m")
    bars_a = [(datetime(2024, 2, d, 10, 0), 10.0, 11.0, 9.0, 10.5, d)
              for d in range(1, 24) if datetime(2024, 2, d).weekday() < 5]
    bars_b = [(datetime(2024, 2, d, 11, 0), 20.0, 21.0, 19.0, 20.5, d)
              for d in range(1, 24) if datetime(2024, 2, d).weekday() < 5]

    def _storm(bars):
        for _ in range(30):
            try:
                ss.write_month_file(storm, bars)
            except ss.StorageError:
                pass                       # contention is fine; torn is not

    th1 = threading.Thread(target=_storm, args=(bars_a,))
    th2 = threading.Thread(target=_storm, args=(bars_b,))
    th1.start(); th2.start(); th1.join(); th2.join()
    bars_f, _ = ss.read_month_file(storm)   # raises if torn
    check("two-thread write storm leaves a whole, readable file",
          bars_f in (bars_a, bars_b))
    check("no temp litter after the storm",
          not list(storm.parent.glob("*.tmp")))
    stale = storm.parent / (storm.name + ".123-456-7.tmp")
    stale.write_bytes(b"junk")
    old = _time.time() - 7200
    os.utime(stale, (old, old))
    fresh = storm.parent / (storm.name + ".123-456-8.tmp")
    fresh.write_bytes(b"junk")
    res_t = ss.scan_storage(root)
    check("stale tmp swept by scan", not stale.exists()
          and any("swept" in w for w in res_t["warnings"]))
    check("fresh tmp kept and reported", fresh.exists()
          and any("in-flight" in r for _p, r in res_t["unrecognized"]))
    fresh.unlink()

    print("[12] adversarial regressions: scanner & manifest")
    r2 = ss.storage_root(tmp / "scan2")
    ko2 = ss.month_file_path(r2, "KO", 2025, 11, "1m", fmt="csv")  # byte-flip below
    ss.write_month_file(ko2, [(datetime(2025, 11, 3, 9, 30),
                               60.0, 60.1, 59.9, 60.0, 5)])
    aap2 = ss.month_file_path(r2, "AAPL", 2025, 11, "1m")
    ss.write_month_file(aap2, [(datetime(2025, 11, 3, 9, 30),
                                250.0, 251.0, 249.0, 250.0, 7)])
    (r2 / "2023").mkdir()
    (r2 / "OLD").mkdir()
    res = ss.scan_storage(r2)
    check("year-like root folder is NOT adopted as a ticker",
          "2023" not in res["tickers"]
          and any("year folder at the storage root" in r
                  for _p, r in res["unrecognized"]))
    check("no manifest planted into the year folder",
          not (r2 / "2023" / "manifest.json").exists())
    check("ticker-shaped junk folder not adopted, nothing written",
          "OLD" not in res["tickers"]
          and not (r2 / "OLD" / "manifest.json").exists())
    expect_raise("interval '01m' refused by the path builder",
                 lambda: ss.month_file_path(r2, "KO", 2025, 11, "01m"))
    twin = ko2.parent / "KO_2025-11_01m.csv"
    twin.write_bytes(ko2.read_bytes())
    res = ss.scan_storage(r2)
    check("'01m' twin series file is inert",
          "01m" not in res["tickers"]["KO"]["intervals"])
    twin.unlink()
    for bad in ('{"intervals": null}', '{"intervals": []}',
                '{"intervals": {"1m": null}}',
                '{"intervals": {"1m": {"months": {"2025-11": "x"}}}}'):
        (r2 / "KO" / "manifest.json").write_text(bad, encoding="utf-8")
        res = ss.scan_storage(r2)
        check(f"scan survives manifest shape {bad[:26]!r}",
              "AAPL" in res["tickers"] and "KO" in res["tickers"])
        for c in (r2 / "KO").glob("manifest.json.corrupt-*"):
            c.unlink()
    (r2 / "KO" / "manifest.json").write_text(
        '{"symbol": "KO.X", "conid": 99887, "intervals": BROKEN',
        encoding="utf-8")
    res = ss.scan_storage(r2)
    man2 = ss.load_manifest(r2 / "KO")
    check("symbol + conid salvaged from corrupt manifest",
          man2 is not None and man2["symbol"] == "KO.X"
          and man2["conid"] == 99887)
    corrupt_copies = list((r2 / "KO").glob("manifest.json.corrupt-*"))
    check("corrupt manifest preserved as evidence", bool(corrupt_copies))
    for c in corrupt_copies:
        c.unlink()
    badf = aap2.parent / "AAPL_2025-11_1s.csv"
    badf.write_bytes(b"Date,Time,open,high,low,close,volume\n"
                     b"11/3/2025,10:00:00,1,2,0.5,1.5,1\n")
    ss.scan_storage(r2)
    g1 = ss.load_manifest(r2 / "AAPL")["generation"]
    res = ss.scan_storage(r2)
    g2 = ss.load_manifest(r2 / "AAPL")["generation"]
    check("format-error file: zero manifest churn on rescan", g1 == g2)
    badf.unlink()
    res = ss.scan_storage(r2)
    check("vanished format-error month becomes MISSING",
          ("AAPL", "1s", "2025-11") in res["missing"])
    saved = aap2.read_bytes()
    aap2.unlink()
    res = ss.scan_storage(r2)
    check("deleted month tombstoned", ("AAPL", "1m", "2025-11")
          in res["missing"])
    aap2.write_bytes(saved)
    res = ss.scan_storage(r2)
    check("restored month flips back to present",
          res["tickers"]["AAPL"]["intervals"]["1m"]["rows"] == 1
          and ("AAPL", "1m", "2025-11") not in res["missing"])
    ent = ss.load_manifest(r2 / "KO")["intervals"]["1m"]["months"]["2025-11"]
    data = ko2.read_bytes().replace(b"60.1", b"60.2")   # same byte length
    ko2.write_bytes(data)
    sec = ent["mtime_ns"] // 1_000_000_000
    frac = ent["mtime_ns"] % 1_000_000_000
    # NTFS stores timestamps in 100 ns ticks — forge one full tick (same
    # integer second, the granularity the OLD fast path compared at).
    forged = sec * 1_000_000_000 + (frac + (100 if frac < 999_999_800
                                            else -100))
    os.utime(ko2, ns=(forged, forged))
    res = ss.scan_storage(r2)
    ent2 = ss.load_manifest(r2 / "KO")["intervals"]["1m"]["months"]["2025-11"]
    check("same-size same-second rewrite detected (mtime_ns fast path)",
          ent2["sha256"] != ent["sha256"])
    orig_read = ss.read_month_file

    def _deny(path):
        if Path(path).name == ko2.name:
            raise PermissionError(13, "locked by AV", str(path))
        return orig_read(path)

    ss.read_month_file = _deny
    try:
        os.utime(ko2)                      # force a re-parse attempt
        res = ss.scan_storage(r2)
    finally:
        ss.read_month_file = orig_read
    check("locked file does not abort the scan (others still listed)",
          "AAPL" in res["tickers"]
          and any("unreadable" in m for _p, m in res["errors"]))

    # FAST-PATH FRESHNESS (review wgj4u1yl6): a file rewritten DURING a scan — in the
    # gap between os.scandir() enumerating its dir and the fast-path stat — must be
    # CAUGHT. DirEntry.stat() is cached at scandir time on Windows, so the fast path
    # MUST re-stat fresh at compare time (os.stat). Inject a rewrite right after KO's
    # month dir is enumerated, then assert the scan reconciled the manifest to it.
    ss.scan_storage(r2, workers=1)                     # settle: manifest matches disk
    _pre = ss.load_manifest(r2 / "KO")["intervals"]["1m"]["months"][
        "2025-11"]["sha256"]
    _new = ko2.read_bytes().replace(b"60.2", b"60.35", 1)   # +1 byte -> new size+sha
    _tgt = os.path.normcase(os.path.abspath(str(ko2.parent)))
    _orig_sd = os.scandir
    _fired = {"x": False}

    class _GapInject:                                  # CM + iterator like os.scandir
        def __init__(self, p):
            self._p = p

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

        def __iter__(self):
            ents = list(_orig_sd(self._p))             # DirEntries: stats captured NOW
            ko2.write_bytes(_new)                      # rewrite AFTER capture, pre-stat
            _fired["x"] = True
            return iter(ents)

    def _patched(p):                                   # wrap ONLY the KO month dir
        if not _fired["x"] and os.path.normcase(
                os.path.abspath(str(p))) == _tgt:
            return _GapInject(p)
        return _orig_sd(p)

    os.scandir = _patched
    try:
        ss.scan_storage(r2, workers=1)                 # serial -> deterministic gap
    finally:
        os.scandir = _orig_sd
    _post = ss.load_manifest(r2 / "KO")["intervals"]["1m"]["months"][
        "2025-11"]["sha256"]
    check("mid-scan rewrite (enumerate->stat gap) caught by a fresh compare-time "
          "stat", _fired["x"] and _post != _pre)
    ss.scan_storage(r2)                    # settle after the lock test
    emp = r2 / "KO" / "2025" / "03-Mar"
    emp.mkdir(parents=True)
    res = ss.scan_storage(r2)
    check("empty month folder reported",
          any("empty month folder" in m for _p, m in res["unrecognized"]))

    print("[13] sha256 integrity scrub")
    sc = ss.storage_root(tmp / "scrub")
    sp = ss.month_file_path(sc, "KO", 2025, 11, "1m", fmt="csv")  # byte-flip below
    wstats = ss.write_month_file(sp, [(datetime(2025, 11, 3, 9, 30),
                                       60.0, 60.1, 59.9, 60.0, 5)])
    ss.scan_storage(sc)                    # record the hash in the manifest
    # (a) a cleanly written, manifest-recorded month scrubs OK
    res = ss.scrub_storage(sc)
    check("clean tree scrubs OK",
          res["checked"] == 1 and res["ok"] == 1
          and not res["mismatched"] and not res["missing"]
          and not res["no_hash"], str(res))
    # per-file verify agrees with the recorded write-time sha
    check("verify_month_file OK on a clean file",
          ss.verify_month_file(sp, wstats["sha256"]) == "ok")

    # (b) flip a byte on disk and assert the scrub flags MISMATCH. Keep the
    # same byte length and integer second so the OLD size+mtime fast path
    # would have trusted it — the scrub must catch what the scan does not.
    ent = ss.load_manifest(sc / "KO")["intervals"]["1m"]["months"]["2025-11"]
    raw = sp.read_bytes()
    flipped = raw.replace(b"60.1", b"60.2")   # same byte length
    check("corruption is same byte length (defeats size check)",
          len(flipped) == len(raw) and flipped != raw)
    sec = ent["mtime_ns"] // 1_000_000_000
    frac = ent["mtime_ns"] % 1_000_000_000
    sp.write_bytes(flipped)
    os.utime(sp, ns=(sec * 1_000_000_000 + frac, sec * 1_000_000_000 + frac))
    res = ss.scrub_storage(sc)
    check("flipped byte flagged as MISMATCH by scrub",
          res["checked"] == 1 and res["ok"] == 0
          and [Path(p).name for p in res["mismatched"]] == [sp.name],
          str(res))
    check("verify_month_file flags MISMATCH directly",
          ss.verify_month_file(sp, ent["sha256"]) == "MISMATCH")
    check("scrub is read-only (recorded hash untouched)",
          ss.load_manifest(sc / "KO")["intervals"]["1m"]["months"]
          ["2025-11"]["sha256"] == ent["sha256"])

    # (c) a missing recorded hash and a missing file are REPORTED, not crashed
    check("verify_month_file: no recorded hash -> no-hash",
          ss.verify_month_file(sp, None) == "no-hash")
    check("verify_month_file: empty recorded hash -> no-hash",
          ss.verify_month_file(sp, "") == "no-hash")
    gone = sp.parent / "KO_2025-11_99m.csv"   # never created
    check("verify_month_file: absent file -> missing",
          ss.verify_month_file(gone, "deadbeef") == "missing")
    # drop the recorded sha from the manifest -> scrub reports no_hash
    man = ss.load_manifest(sc / "KO")
    man["intervals"]["1m"]["months"]["2025-11"].pop("sha256", None)
    ss.save_manifest(sc / "KO", man)
    res = ss.scrub_storage(sc)
    check("scrub reports a hash-less entry as no_hash",
          [Path(p).name for p in res["no_hash"]] == [sp.name]
          and not res["mismatched"], str(res))
    # delete the file but keep the (present) manifest entry -> scrub: missing
    sp.unlink()
    res = ss.scrub_storage(sc)
    # the file is gone, so scrub reports the CANONICAL (parquet) path as missing
    # — compare stems (ticker_month_interval), extension-agnostic.
    check("scrub reports a vanished recorded month as missing",
          [Path(p).stem for p in res["missing"]] == [sp.stem], str(res))
    # empty / absent root degrades to a warning, never a crash
    res = ss.scrub_storage(ss.storage_root(tmp / "no_such_root"))
    check("scrub of an absent root warns, does not crash",
          res["checked"] == 0 and bool(res["warnings"]))

    print("[14] verify-after-write toggle (default OFF)")
    vp = ss.month_file_path(sc, "VW", 2025, 11, "1m")
    vstats = ss.write_month_file(vp, [(datetime(2025, 11, 3, 9, 30),
                                       70.0, 70.1, 69.9, 70.0, 9)],
                                 verify_after_write=True)
    check("verify-after-write succeeds on a good write",
          vstats["rows"] == 1
          and ss.verify_month_file(vp, vstats["sha256"]) == "ok")
    # The toggle must not change the bytes written or the recorded sha — a
    # default-off write and a verify-on write of the same bars agree exactly.
    vp2 = ss.month_file_path(sc, "VW2", 2025, 11, "1m")
    dstats = ss.write_month_file(vp2, [(datetime(2025, 11, 3, 9, 30),
                                        70.0, 70.1, 69.9, 70.0, 9)])
    check("verify-on and default-off writes agree byte-for-byte",
          dstats["sha256"] == vstats["sha256"]
          and vp2.read_bytes() == vp.read_bytes())

    print("[15] F2 codec round-trip (formatted-faithfully, default ON)")
    good = [(datetime(2025, 11, 3, 9, 30), 70.0, 70.1, 69.9, 70.05, 9),
            (datetime(2025, 11, 3, 9, 31), 70.05, 70.2, 70.0, 70.1, 13)]
    rp = ss.month_file_path(sc, "RT", 2025, 11, "1m", fmt="csv")  # corrupts format_bar
    rs = ss.write_month_file(rp, good)
    check("faithful write passes the round-trip and reads back identical",
          rs["rows"] == 2 and ss.read_month_file(rp)[0] == good)
    # A serializer that corrupts ONE field: the payload still PARSES (a valid
    # line) but decodes to a DIFFERENT bar -> MISMATCH, refuse before writing.
    orig_fb = ss.format_bar
    rp2 = ss.month_file_path(sc, "RT2", 2025, 11, "1m", fmt="csv")
    try:
        ss.format_bar = lambda dt, o, h, lo, c, v: orig_fb(
            dt, o, h, lo, c, v + 1)               # bump volume
        expect_raise("codec round-trip CATCHES a corrupt serializer",
                     lambda: ss.write_month_file(rp2, good))
    finally:
        ss.format_bar = orig_fb
    check("round-trip refusal wrote NOTHING to disk", not rp2.exists())
    # verify_codec=False opts out (old fast path) — proves the check is the
    # only thing standing between a corrupt serializer and the disk.
    rp3 = ss.month_file_path(sc, "RT3", 2025, 11, "1m", fmt="csv")
    try:
        ss.format_bar = lambda dt, o, h, lo, c, v: orig_fb(
            dt, o, h, lo, c, v + 1)
        ss.write_month_file(rp3, good, verify_codec=False)
        check("verify_codec=False skips the round-trip (corrupt write lands)",
              rp3.exists() and ss.read_month_file(rp3)[0][0][5] == 10)
    finally:
        ss.format_bar = orig_fb

    print("[16] shared identity-floor parser")
    identity_manifest = {
        "data_corrections": [
            {
                "type": "identity_listing_truncation",
                "ticker": "TKO",
                "cutover": "2023-09-12",
            },
            {
                "type": "identity_truncation",
                "ticker": "TKO",
                "intervals": ["1d"],
                "cutover": "2023-10-01",
            },
            {
                "type": "truncation",
                "ticker": "TKO",
                "intervals": ["1m-pre"],
                "cutover": "2024-01-02",
            },
            {
                "type": "phantom_split_correction",
                "ticker": "TKO",
                "cutover": "2099-01-01",
            },
        ],
    }
    check("identity vocabulary is authoritative and complete",
          ss.IDENTITY_CORRECTION_TYPES
          == frozenset({"identity_listing_truncation",
                        "identity_truncation", "truncation"}))
    check("listing-wide floor protects ordinary price intervals",
          ss.identity_floor(identity_manifest, "1m", ticker="TKO")
          == datetime(2023, 9, 12).date())
    check("base-scoped legacy floor protects derived ratio intervals",
          ss.identity_floor(identity_manifest, "1d-hvol", ticker="TKO")
          == datetime(2023, 10, 1).date())
    check("exact session scope wins only for that session",
          ss.identity_floor(identity_manifest, "1m-pre", ticker="TKO")
          == datetime(2024, 1, 2).date()
          and ss.identity_floor(identity_manifest, "1m-post", ticker="TKO")
          == datetime(2023, 9, 12).date())
    check("non-identity corrections are ignored by the floor parser",
          ss.identity_floor(identity_manifest, "1d", ticker="TKO")
          != datetime(2099, 1, 1).date())
    expect_raise(
        "identity ticker mismatch fails closed",
        lambda: ss.identity_floor(identity_manifest, "1m", ticker="WBD"))
    for label, malformed in (
            ("non-list correction container", {"data_corrections": {}}),
            ("non-object correction record", {"data_corrections": ["bad"]}),
            ("legacy correction without scope", {"data_corrections": [{
                "type": "identity_truncation", "cutover": "2023-09-12"}]}),
            ("invalid scoped interval", {"data_corrections": [{
                "type": "truncation", "intervals": ["bogus"],
                "cutover": "2023-09-12"}]}),
            ("non-canonical cutover", {"data_corrections": [{
                "type": "identity_listing_truncation",
                "cutover": "2023-9-12"}]}),
    ):
        expect_raise(
            f"{label} fails closed",
            lambda value=malformed: ss.identity_floor(value, "1m"))

    print("[17] digest-gated rescan cache (scan_storage digest_cache=True)")
    import json as _json
    droot = Path(tmp) / "dcbank"
    droot.mkdir()
    if not ss._volume_is_ntfs(droot):
        print("  ok    (skipped: temp dir not on a fixed NTFS volume)")
        return

    def _dbars(y, m, n=5):
        return [(datetime(y, m, 3, 9, 30 + i), 1.0, 2.0, 0.5, 1.5, 10 + i)
                for i in range(n)]

    for t in ("AAA", "BBB", "CCC"):
        for (y, m) in ((2025, 11), (2025, 12)):
            ss.write_month_file(ss.month_file_path(droot, t, y, m, "1m"),
                                _dbars(y, m))

    def _norm(r):
        return _json.dumps(r, sort_keys=True, default=str)

    calls = []
    orig_st = ss._scan_ticker

    def _counting(td, res):
        calls.append(td.name)
        return orig_st(td, res)

    def _cached_scan_walks():
        calls.clear()
        ss._scan_ticker = _counting
        try:
            r = ss.scan_storage(droot, digest_cache=True)
        finally:
            ss._scan_ticker = orig_st
        return r, list(calls)

    guard0 = ss._DIGEST_RACY_NS
    try:
        ss._DIGEST_RACY_NS = 50_000_000        # 50 ms guard: test-speed aging
        full0 = ss.scan_storage(droot)         # settles (heals) the manifests
        _time.sleep(0.12)                      # age the heals past the guard —
        #   a capture within the guard of a manifest write is REFUSED (racy),
        #   so an un-aged build would correctly re-walk everything next scan
        build = ss.scan_storage(droot, digest_cache=True)   # captures digests
        check("digest build scan == plain scan", _norm(build) == _norm(full0))
        check("cache file lands at the bank PARENT",
              (Path(tmp) / ss.DIGEST_CACHE_NAME).exists())
        _time.sleep(0.12)                      # age captures past the guard
        hit, walked = _cached_scan_walks()
        check("no-change rescan: 0 ticker walks (all digest hits)",
              walked == [], walked)
        check("no-change rescan result IDENTICAL", _norm(hit) == _norm(build))

        # (a) crash pattern: month rewritten via temp+os.replace, manifest lagging
        ss.write_month_file(ss.month_file_path(droot, "AAA", 2025, 12, "1m"),
                            _dbars(2025, 12, 7))
        _time.sleep(0.12)
        got, walked = _cached_scan_walks()
        want = ss.scan_storage(droot)
        check("lagging-manifest rewrite: CAUGHT (AAA re-walked)",
              "AAA" in walked and "CCC" not in walked, walked)
        check("lagging-manifest rewrite: cached == fresh full scan",
              _norm(got) == _norm(want))
        check("mutation visible (rows updated)",
              got["tickers"]["AAA"]["intervals"]["1m"]["rows"] == 12)

        # (b) manifest-ONLY atomic rewrite (seal pattern)
        man_b = ss.load_manifest(droot / "BBB")
        man_b["intervals"]["1m"]["backfill_incomplete"] = True
        ss.save_manifest(droot / "BBB", man_b)
        _time.sleep(0.12)
        got, walked = _cached_scan_walks()
        check("manifest-only rewrite: CAUGHT (BBB re-walked)",
              "BBB" in walked, walked)
        check("manifest-only rewrite: cached == fresh full scan",
              _norm(got) == _norm(ss.scan_storage(droot)))

        # (c) external delete -> MISSING tombstone. NOTE the semantics the cache
        # must preserve: result["missing"] reports the present->MISSING
        # TRANSITION (that one scan); afterwards the tombstone persists as a
        # summary FLAG ("N month(s) MISSING") on every scan, cached or not.
        os.unlink(ss.month_file_path(droot, "CCC", 2025, 11, "1m"))
        _time.sleep(0.12)
        got, walked = _cached_scan_walks()
        check("external delete: CAUGHT (CCC re-walked)", "CCC" in walked, walked)
        check("missing tombstone reported on the transition scan",
              any(m[0] == "CCC" and m[2] == "2025-11" for m in got["missing"]))
        _time.sleep(0.12)                      # steady state: cached == full
        got2, _w = _cached_scan_walks()
        want2 = ss.scan_storage(droot)
        check("post-tombstone steady state: cached == fresh full scan",
              _norm(got2) == _norm(want2))
        check("MISSING flag re-reports every scan via the summary",
              any("MISSING" in f for f in got2["tickers"]["CCC"]["flags"])
              and got2["tickers"]["CCC"]["flags"]
              == want2["tickers"]["CCC"]["flags"])

        # (d) non-NTFS fence -> silently a full scan, identical result
        orig_ntfs = ss._volume_is_ntfs
        try:
            ss._volume_is_ntfs = lambda p: False
            got = ss.scan_storage(droot, digest_cache=True)
            check("non-NTFS root: falls back to the full scan, identical",
                  _norm(got) == _norm(ss.scan_storage(droot)))
        finally:
            ss._volume_is_ntfs = orig_ntfs
    finally:
        ss._DIGEST_RACY_NS = guard0
        ss._scan_ticker = orig_st
        with ss._DIGEST_LOCK:                  # drop the test bank's memo entry
            ss._DIGEST_MEM.pop(str(Path(droot).resolve()), None)


if __name__ == "__main__":
    sys.exit(main())
