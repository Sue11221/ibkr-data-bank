"""Headless tests for the daily-OHLC validator — mocked reference, no network.
    python engine/stock_validate_selftest.py"""
import json
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_validate as sv

_PASS = [0]
_FAIL = [0]


def check(cond, name):
    if cond:
        _PASS[0] += 1
    else:
        _FAIL[0] += 1
        print("  FAIL:", name)


def day_bars(d, n, base, step_min=1):
    out = []
    t = datetime.combine(d, time(9, 30))
    for i in range(n):
        p = round(base + i * 0.01, 2)
        out.append((t, p, round(p + 0.05, 2), round(p - 0.05, 2),
                    round(p + 0.02, 2), 100 + i))
        t += timedelta(minutes=step_min)
    return out


def cols_from_bars(bars):
    import numpy as np
    if not bars:
        z = np.array([], dtype=np.int64)
        f = np.array([], dtype=np.float64)
        return z, f, f.copy(), f.copy(), f.copy(), z.copy()
    return (
        np.array([int((b[0] - sv._EPOCH_DT).total_seconds()) for b in bars],
                 dtype=np.int64),
        np.array([b[1] for b in bars], dtype=np.float64),
        np.array([b[2] for b in bars], dtype=np.float64),
        np.array([b[3] for b in bars], dtype=np.float64),
        np.array([b[4] for b in bars], dtype=np.float64),
        np.array([b[5] for b in bars], dtype=np.int64),
    )


def ref_from_derived(derived, factor=1.0):
    return {d.isoformat(): (o * factor, h * factor, lo * factor, c * factor, v)
            for d, (o, h, lo, c, v) in derived.items()}


def xval_fingerprint(_root, ticker, interval, sha="a" * 64):
    return {"schema_version": sv.ss.INTERVAL_FINGERPRINT_VERSION,
            "algorithm": "sha256", "sha256": sha,
            "ticker": sv.ss.canonical_ticker(ticker), "interval": interval,
            "present": True, "backfill_incomplete": False,
            "month_count": 1, "verified_absent_count": 0}


def test_parse_reference():
    raw = ('{"status":200,"data":['
           '{"t":"2026-06-16","o":395.79,"h":396.84,"l":390.69,"c":393.83,'
           '"a":393.83,"v":31471335}]}')
    out = sv._parse_reference(raw)
    check(out == {"2026-06-16": (395.79, 396.84, 390.69, 393.83, 31471335)},
          "parse_reference reads raw OHLCV")


def test_derive_daily():
    d = date(2026, 6, 15)
    bars = day_bars(d, 100, 300.0)
    # add an out-of-RTH bar that must be ignored
    bars.append((datetime.combine(d, time(18, 0)), 999, 999, 999, 999, 1))
    der = sv.derive_daily(bars)
    o, h, lo, c, v = der[d]
    check(o == 300.0, "derive open = first bar open")
    check(abs(h - (300.0 + 99 * 0.01 + 0.05)) < 1e-9, "derive high = max")
    check(abs(lo - (300.0 - 0.05)) < 1e-9, "derive low = min")
    check(v == sum(100 + i for i in range(100)), "derive volume = sum (RTH only)")
    check(sv.derive_daily(bars, min_bars=200) == {}, "min_bars drops thin days")


def test_derive_daily_fast_edges():
    try:
        import numpy  # noqa: F401
    except ImportError:
        return
    d1 = date(2026, 6, 15)
    d2 = date(2026, 6, 16)
    bars = [
        (datetime.combine(d1, time(9, 29, 59)), 1, 9, 1, 9, 1),
        (datetime.combine(d1, time(9, 30, 0)), 10, 11, 9, 10.5, 100),
        (datetime.combine(d1, time(9, 31, 0)), 10.5, 12, 10, 11, 101),
        (datetime.combine(d1, time(15, 59, 59)), 11, 13, 10.5, 12, 102),
        (datetime.combine(d1, time(16, 0, 0)), 99, 100, 98, 99, 1),
        (datetime.combine(d2, time(9, 30, 0)), 20, 20, 20, 20, 200),
    ]
    old = sv.derive_daily(bars, min_bars=2)
    fast = sv.derive_daily_fast(cols_from_bars(bars), min_bars=2)
    check(fast == old and list(fast) == [d1],
          "derive_daily_fast: RTH boundaries and min_bars match derive_daily")

    dup = [
        (datetime.combine(d1, time(9, 31)), 2, 5, 1, 2.5, 20),
        (datetime.combine(d1, time(9, 30)), 1, 4, 0.5, 1.5, 10),
        (datetime.combine(d1, time(9, 31)), 3, 6, 2, 3.5, 30),
    ]
    check(sv.derive_daily_fast(cols_from_bars(dup), min_bars=1)
          == sv.derive_daily(dup, min_bars=1),
          "derive_daily_fast: stable sort preserves duplicate timestamp semantics")

    daily = [
        (datetime.combine(d1, time(0, 0)), 10, 11, 9, 10.5, 100),
        (datetime.combine(d2, time(0, 0)), 20, 21, 19, 20.5, 200),
    ]
    daily_window = sv.ss.session_window("1d")
    daily_slow = sv.derive_daily(
        daily, min_bars=1, window=daily_window)
    daily_fast = sv.derive_daily_fast(
        cols_from_bars(daily), min_bars=1, window=daily_window)
    check(daily_fast == daily_slow and list(daily_fast) == [d1, d2],
          "derive_daily_fast: canonical-midnight daily rows use full-day window")
    check(sv.derive_daily(daily, min_bars=1) == {},
          "derive_daily: default remains RTH and excludes midnight rows")


def test_columnar_read_and_validate_series_fast_path():
    try:
        import numpy  # noqa: F401
        sv.ss._require_pyarrow()
    except Exception:  # noqa: BLE001
        return
    import shutil
    import tempfile
    root = tempfile.mkdtemp(prefix="sv_xval_")
    try:
        bars = day_bars(date(2026, 6, 15), 60, 100.0) + day_bars(
            date(2026, 7, 1), 60, 110.0)
        for chunk in (bars[:60], bars[60:]):
            y, m = chunk[0][0].year, chunk[0][0].month
            stats = sv.ss.write_month_file(
                sv.ss.month_file_path(root, "AAPL", y, m, "1m"), chunk)
            tdir = Path(root) / "AAPL"
            man = sv.ss.load_manifest(tdir) or sv.ss.new_manifest("AAPL", "AAPL")
            sv.ss.manifest_months(man, "1m")[f"{y:04d}-{m:02d}"] = dict(
                stats, status="present")
            sv.ss.save_manifest(tdir, man)
        cols = sv.read_series_columns(root, "AAPL", "1m")
        check([c.size for c in cols] == [len(bars)] * 6,
              "read_series_columns: concatenates six month columns")
        min_bars = max(1, min(sv.MIN_DAY_BARS,
                              int(sv._expected_rth_bars("1m") * 0.5)))
        derived = sv.derive_daily_fast(cols, min_bars=min_bars)
        check(derived == sv.derive_daily(bars, min_bars=min_bars),
              "read_series_columns + derive_daily_fast equals tuple path")
        res = sv._validate_series_body(
            root, "AAPL", "1m", ref_fn=lambda t, rng: ref_from_derived(derived))
        check(res.get("score") == 1.0 and res.get("days_derived") == len(derived),
              "validate_series: production path validates via columnar daily derivation")

        daily_bars = [
            (datetime(2026, 6, 15), 100.0, 101.0, 99.0, 100.5, 1000),
            (datetime(2026, 7, 1), 110.0, 111.0, 109.0, 110.5, 2000),
        ]
        for bar in daily_bars:
            y, m = bar[0].year, bar[0].month
            stats = sv.ss.write_month_file(
                sv.ss.month_file_path(root, "AAPL", y, m, "1d"), [bar])
            tdir = Path(root) / "AAPL"
            man = sv.ss.load_manifest(tdir)
            sv.ss.manifest_months(man, "1d")[f"{y:04d}-{m:02d}"] = dict(
                stats, status="present")
            sv.ss.save_manifest(tdir, man)
        daily_ref = {
            bar[0].date().isoformat(): tuple(bar[1:]) for bar in daily_bars}
        daily_res = sv._validate_series_body(
            root, "AAPL", "1d", ref_fn=lambda t, rng: daily_ref)
        check(daily_res.get("score") == 1.0
              and daily_res.get("days_derived") == len(daily_bars),
              "validate_series: columnar 1d path accepts canonical midnight rows")
        daily_xval = sv._cross_validate_ticker_body(
            root, "AAPL", "1d", ref_fn=lambda t, rng: daily_ref)
        check(daily_xval.get("status") == "validated"
              and (daily_xval.get("detail") or {}).get("checked")
              == len(daily_bars),
              "cross_validate_ticker: columnar 1d evidence is conclusive")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_compare_clean():
    der = sv.derive_daily(day_bars(date(2026, 6, 15), 80, 300.0))
    res = sv.compare_daily(der, ref_from_derived(der))
    check(res["score"] == 1.0 and not res["flagged"], "clean compare: score 1.0")
    check(res["suspected_factor"] is None, "clean: no suspected factor")


def test_compare_split():
    days = {}
    for i in range(6):
        d = date(2026, 6, 1) + timedelta(days=i)
        days.update(sv.derive_daily(day_bars(d, 80, 300.0 + i)))
    # reference is HALF (stored data is 2x — an unhandled 2:1 split / wrong basis)
    ref = ref_from_derived(days, factor=0.5)
    res = sv.compare_daily(days, ref)
    check(res["suspected_factor"] == 2.0,
          f"uniform 2x surfaces basis factor 2.0 (got {res['suspected_factor']})")
    check(res["score"] == 1.0,
          "a uniform factor is a basis, NOT a flood of per-day flags")


def test_compare_volume_missing():
    days = {}
    for i in range(4):
        days.update(sv.derive_daily(
            day_bars(date(2026, 6, 1) + timedelta(days=i), 80, 300.0)))
    ref = ref_from_derived(days)
    bad = date(2026, 6, 2).isoformat()
    o, h, lo, c, v = ref[bad]
    ref[bad] = (o, h, lo, c, v * 3)             # we have 1/3 the day's volume
    res = sv.compare_daily(days, ref)
    check(res["flagged_days"] == [bad],
          "volume far outside the band flags a missing chunk")


def test_compare_missing_chunk():
    days = {}
    for i in range(5):
        d = date(2026, 6, 1) + timedelta(days=i)
        days.update(sv.derive_daily(day_bars(d, 80, 300.0)))
    ref = ref_from_derived(days)
    # one day's TRUE high was 5% higher (we missed the peak bar)
    bad = date(2026, 6, 3)
    o, h, lo, c, v = ref[bad.isoformat()]
    ref[bad.isoformat()] = (o, h * 1.05, lo, c, v)
    res = sv.compare_daily(days, ref)
    check(res["flagged_days"] == [bad.isoformat()],
          "missing-chunk: only the one bad day is flagged")
    check(res["suspected_factor"] is None, "one bad day != a factor")


def test_tolerance_boundary():
    days = {}
    for i in range(10):
        days.update(sv.derive_daily(
            day_bars(date(2026, 6, 1) + timedelta(days=i), 80, 300.0)))
    ref = ref_from_derived(days)                 # 10 clean days -> basis ~1.0
    bad = date(2026, 6, 5).isoformat()
    o, h, lo, c, v = ref[bad]
    ref[bad] = (o, h * 1.005, lo, c, v)          # ONE day's high off 0.5%
    check(sv.compare_daily(days, ref, tol_hl=0.004)["flagged_days"] == [bad],
          "a 0.5% one-day high anomaly FLAGS at 0.4% tolerance")
    check(not sv.compare_daily(days, ref, tol_hl=0.01)["flagged_days"],
          "the same 0.5% anomaly is IGNORED at 1.0% tolerance")


def test_compare_midseries_split():
    # 40 trading days; the stored data is 2x the reference for the FIRST half
    # only — a 2:1 split applied to part of the series. A centered rolling median
    # self-adapts to the sustained run and would silently absorb it (score 1.0);
    # the explicit step detector must catch it. (Bug-hunt finding #1.)
    days = sorted(date(2026, 1, 1) + timedelta(days=i) for i in range(40))
    derived = {d: (300.0, 301.0, 299.0, 300.5, 1000) for d in days}
    ref = {}
    for i, d in enumerate(days):
        f = 2.0 if i < 20 else 1.0               # stored/ref = 2 then 1 (a STEP)
        o, h, lo, c, v = derived[d]
        ref[d.isoformat()] = (o / f, h / f, lo / f, c / f, v)
    res = sv.compare_daily(derived, ref)
    ls = res.get("level_shift")
    check(ls is not None, "mid-series split: level_shift is DETECTED (not absorbed)")
    check(bool(ls) and abs(ls["factor"] - 0.5) < 1e-6,
          f"mid-series split: step factor ~0.5 (got {ls})")
    check(bool(ls) and abs(ls["index"] - 20) <= 7,
          f"mid-series split: boundary localised near day 20 (got {ls})")
    check(res["score"] is not None and res["score"] < 1.0,
          f"mid-series split: score is NOT a clean 1.0 (got {res['score']})")
    check(bool(res["flagged"]),
          "mid-series split: at least one flag raised (verdict can't read clean)")
    # the bimodal ratio median is a meaningless ~1.5 — must NOT be reported as a
    # uniform factor (bug-hunt finding #7).
    check(res["suspected_factor"] is None,
          f"split: NO phantom uniform factor (got {res['suspected_factor']})")


def test_sustained_block_flagged():
    # a contiguous 13-day systematic-high block (8% off) inside a 60-day series —
    # a run >= half the window that the rolling median would otherwise adopt as
    # its own baseline. The step detector must light up at the block edge.
    days = sorted(date(2026, 1, 1) + timedelta(days=i) for i in range(60))
    derived = {d: (300.0, 301.0, 299.0, 300.5, 1000) for d in days}
    ref = {}
    for i, d in enumerate(days):
        f = 1.08 if 20 <= i < 33 else 1.0
        o, h, lo, c, v = derived[d]
        ref[d.isoformat()] = (o / f, h / f, lo / f, c / f, v)
    res = sv.compare_daily(derived, ref)
    check(res.get("level_shift") is not None,
          "sustained 8% block: level_shift detected (not averaged away)")


def test_uniform_factor_still_clean():
    # a WHOLE-series uniform 2x (a global basis, not a mid-series step) must still
    # be reported as a single basis factor, NOT a level_shift — the rolling
    # baseline + suspected_factor path the validator was designed around.
    days = sorted(date(2026, 1, 1) + timedelta(days=i) for i in range(40))
    derived = {d: (300.0, 301.0, 299.0, 300.5, 1000) for d in days}
    ref = {d.isoformat(): (150.0, 150.5, 149.5, 150.25, 1000) for d in days}
    res = sv.compare_daily(derived, ref)
    check(res.get("level_shift") is None,
          "uniform 2x: no spurious level_shift (it's a flat basis)")
    check(res["suspected_factor"] == 2.0,
          f"uniform 2x: surfaces basis factor 2.0 (got {res['suspected_factor']})")


def test_epoch_reference_key():
    # stockanalysis has been seen to return `t` as a Unix epoch; an un-normalised
    # epoch key never matches date.isoformat() -> zero overlap -> silent clean.
    # _norm_date_key must convert it (bug-hunt finding #8).
    check(sv._norm_date_key(1718496000) == "2024-06-16",
          f"epoch seconds -> ISO (got {sv._norm_date_key(1718496000)})")
    check(sv._norm_date_key(1718496000000) == "2024-06-16",
          "epoch milliseconds -> ISO")
    check(sv._norm_date_key("2026-06-16T00:00:00") == "2026-06-16",
          "ISO datetime string -> date")
    raw = '{"data":[{"t":1718496000,"o":1,"h":2,"l":1,"c":2,"v":5}]}'
    check("2024-06-16" in sv._parse_reference(raw),
          "parse_reference normalises an epoch t to an ISO key")


def test_zero_overlap_inconclusive():
    # no overlapping days (e.g. mismatched date keys) must be INCONCLUSIVE, not a
    # green 'matches within tolerance' on nothing (bug-hunt finding #8).
    bars = day_bars(date(2026, 6, 1), 60, 300.0)
    ref = {"1999-01-04": (1.0, 1.0, 1.0, 1.0, 1)}      # no shared dates
    res = sv._validate_series_body("root", "AAPL", "1m",
                             read_fn=lambda *a: bars,
                             ref_fn=lambda *a, **k: ref)
    check("error" in res and "overlap" in res["error"],
          f"zero overlap -> explicit inconclusive error (got {res})")


def test_coarse_interval_floor():
    # 15m RTH data is ~26 bars/day; a flat MIN_DAY_BARS=50 would drop EVERY day
    # and report 'no stored RTH bars' on complete data. The interval-aware floor
    # must accept it (bug-hunt finding #6).
    bars = []
    for i in range(4):
        d = date(2026, 6, 1) + timedelta(days=i)
        bars += day_bars(d, 26, 300.0 + i, step_min=15)   # 26 fifteen-min bars
    der = sv.derive_daily(bars, min_bars=13)
    ref = ref_from_derived(der)
    res = sv._validate_series_body("root", "AAPL", "15m",
                             read_fn=lambda *a: bars,
                             ref_fn=lambda *a, **k: ref)
    check("error" not in res and res.get("checked") == 4,
          f"15m coarse interval validates (not 'no bars') (got {res})")
    # and the 50-bar flat floor really would have dropped them
    check(sv.derive_daily(bars, min_bars=50) == {},
          "sanity: the old flat MIN_DAY_BARS=50 drops a complete 15m day")


def test_interval_seconds():
    check(sv._interval_seconds("1m") == 60, "1m -> 60s")
    check(sv._interval_seconds("15m") == 900, "15m -> 900s")
    check(sv._interval_seconds("1h") == 3600, "1h -> 3600s")
    check(sv._interval_seconds("1s-post") == 1, "1s-post (suffix) -> 1s")
    check(sv._expected_rth_bars("1m") == 390, "1m -> 390 bars/session")
    check(sv._expected_rth_bars("15m") == 26, "15m -> 26 bars/session")


def test_minute_chain():
    base = datetime(2026, 6, 15, 9, 30)
    sec, minute = [], []
    for m in range(5):
        mt = base + timedelta(minutes=m)
        secs = [(mt + timedelta(seconds=s), 300 + s * 0.001,
                 300 + s * 0.001 + 0.02, 300 + s * 0.001 - 0.02,
                 300 + s * 0.001 + 0.005, 10) for s in range(60)]
        sec.extend(secs)
        o = secs[0][1]
        minute.append((mt, o, max(x[2] for x in secs), min(x[3] for x in secs),
                       secs[-1][4], sum(x[5] for x in secs)))
    res = sv.compare_minute_chain(sec, minute)
    check(res["common_minutes"] == 5 and res["score"] == 1.0,
          "1s->1m chain reconstructs the 1m bars exactly")


def test_reference_cache():
    # Retained A2 cache/parser behavior with a strictly local injected opener.
    # Default A1 refusal is separately exercised below and in the choke suite.
    from unittest.mock import patch
    with patch("fetch_ibkr_bridge.refuse_a2", return_value=None):
        _reference_cache_fixture()


def test_reference_a1_hold():
    from fetch_run_context import RequestRefused
    calls = []
    sv._REF_CACHE.clear()
    try:
        sv.fetch_daily_reference("A1-HOLD", _opener=lambda url: calls.append(url))
    except RequestRefused:
        refused = True
    else:
        refused = False
    check(refused and not calls, "A1 hold blocks the HTTP seam including an injected opener")


def _reference_cache_fixture():
    sv._REF_CACHE.clear()
    calls = []

    def opener(url):
        calls.append(url)
        return '{"data":[{"t":"2026-06-16","o":1,"h":2,"l":1,"c":2,"v":5}]}'

    sv.fetch_daily_reference("ZZ", _now=1000.0, _opener=opener)
    sv.fetch_daily_reference("ZZ", _now=1100.0, _opener=opener)   # within TTL
    check(len(calls) == 1, "reference cached within the 15-min TTL (one call)")
    sv.fetch_daily_reference("ZZ", _now=2000.0, _opener=opener)   # past TTL
    check(len(calls) == 2, "reference re-fetched after the TTL")

    sv._REF_CACHE.clear()
    class_calls = []

    def class_opener(url):
        class_calls.append(url)
        return '{"data":[{"t":"2026-06-16","o":1,"h":2,"l":1,"c":2,"v":5}]}'

    sv.fetch_daily_reference("BF-B", _now=3000.0, _opener=class_opener)
    sv.fetch_daily_reference("BF.B", _now=3100.0, _opener=class_opener)
    check(len(class_calls) == 1 and "/BF.B/history" in class_calls[0],
          "class-share aliases use one dotted StockAnalysis URL/cache identity")
    check(sv.stockanalysis_symbol("BRK-B") == "BRK.B"
          and sv.stockanalysis_symbol("ABC-B") == "ABC-B",
          "StockAnalysis symbol mapping is exact and narrowly scoped")
    sv.clear_reference_cache("BF-B", "5Y")
    check(not sv._REF_CACHE,
          "class-share cache clear uses the mapped provider identity")


def test_validate_series_end_to_end():
    bars = []
    for i in range(4):
        bars += day_bars(date(2026, 6, 1) + timedelta(days=i), 60, 300.0 + i)
    der = sv.derive_daily(bars, min_bars=50)
    ref = ref_from_derived(der)
    res = sv._validate_series_body("root", "AAPL", "1m",
                             read_fn=lambda *a: bars,
                             ref_fn=lambda *a, **k: ref)
    check(res.get("score") == 1.0 and res["days_derived"] == 4,
          "validate_series end-to-end (injected read+ref) = clean")


def test_validate_canonical_daily_series():
    bars = [
        (datetime(2023, 12, 20), 100.0, 101.0, 99.0, 100.5, 1000),
        (datetime(2023, 12, 21), 101.0, 102.0, 100.0, 101.5, 1100),
        (datetime(2023, 12, 22), 102.0, 103.0, 101.0, 102.5, 1200),
    ]
    ref = {bar[0].date().isoformat(): tuple(bar[1:]) for bar in bars}
    res = sv._validate_series_body(
        "root", "AAPL", "1d", read_fn=lambda *args: bars,
        ref_fn=lambda *args, **kwargs: ref)
    check(res.get("score") == 1.0 and res.get("days_derived") == len(bars),
          "validate_series: injected canonical-midnight 1d rows validate")
    xval = sv._cross_validate_ticker_body(
        "root", "AAPL", "1d", read_fn=lambda *args: bars,
        ref_fn=lambda *args, **kwargs: ref,
        fingerprint_fn=xval_fingerprint)
    check(xval.get("status") == "validated"
          and (xval.get("detail") or {}).get("checked") == len(bars),
          "cross_validate_ticker: injected canonical 1d evidence is conclusive")


# --- batch online RE-validation --------------------------------------------

def test_clear_reference_cache():
    sv._REF_CACHE.clear()
    sv._REF_CACHE[("AAA", "5Y")] = (1.0, {})
    sv._REF_CACHE[("AAA", "1Y")] = (1.0, {})
    sv._REF_CACHE[("BBB", "5Y")] = (1.0, {})
    sv.clear_reference_cache("AAA", "5Y")
    check(("AAA", "5Y") not in sv._REF_CACHE and ("AAA", "1Y") in sv._REF_CACHE,
          "clear_reference_cache(ticker, rng) drops only that key")
    sv.clear_reference_cache("AAA")
    check(("AAA", "1Y") not in sv._REF_CACHE and ("BBB", "5Y") in sv._REF_CACHE,
          "clear_reference_cache(ticker) drops every range for that ticker")
    sv.clear_reference_cache()
    check(not sv._REF_CACHE, "clear_reference_cache() drops everything")


def test_classify():
    c = sv._classify
    check(c({"checked": 10, "score": 1.0, "flagged_days": []})[0] == "ok",
          "_classify: clean -> ok")
    check(c({"level_shift": {"factor": 0.5, "date": "2026-01-20"}})[:2]
          == ("split", 4), "_classify: level_shift -> split (sev 4)")
    check(c({"suspected_factor": 2.0})[:2] == ("split", 4),
          "_classify: suspected_factor -> split (sev 4)")
    check(c({"flagged_days": ["2026-06-02", "2026-06-03"]})[:2]
          == ("discrepancy", 3), "_classify: flagged days -> discrepancy (sev 3)")
    check(c({"error": "reference fetch failed: x"})[:2] == ("error", 2),
          "_classify: fetch error -> error (sev 2)")
    check(c({"error": "no overlapping days ..."})[:2] == ("inconclusive", 1),
          "_classify: no overlap -> inconclusive (sev 1)")
    check(c({"error": "no stored RTH bars to validate"})[:2]
          == ("inconclusive", 1), "_classify: no stored bars -> inconclusive")


def test_discover_series():
    import re as _re
    import shutil
    import tempfile
    root = tempfile.mkdtemp(prefix="sv_disc_")
    try:
        for name in ("AAPL", "MSFT", "junk1"):     # junk1 won't match [A-Z]+
            (Path(root) / name).mkdir()
        mans = {
            "AAPL": {"intervals": {
                "1m": {}, "1m-pre": {}, "15m": {}, "1d": {},
                "1m-iv": {}, "1m-iv-pre": {}, "1d-hvol": {}}},
            "MSFT": {"intervals": {
                "30m": {}, "1d": {}, "1d-hvol": {}}},
        }
        old_load = sv.ss.load_manifest
        old_re = getattr(sv.ss, "TICKER_DIR_RE", None)
        sv.ss.TICKER_DIR_RE = _re.compile(r"^[A-Z]+$")
        sv.ss.load_manifest = lambda d: mans.get(Path(d).name)
        try:
            got = sv.discover_series(root)
            kinds = sv.discover_series(
                root, kinds=("", "iv", "hvol"))
            iv_only = sv.discover_series(root, kinds=("iv",))
            everything = sv.discover_series(
                root, rth_only=False, kinds=None)
            cal_bars = {
                ("AAPL", "1d"): [
                    (datetime(2024, 1, d), 1, 1, 1, 1, 1)
                    for d in (2, 3)],
                ("MSFT", "1d"): [
                    (datetime(2024, 1, d), 1, 1, 1, 1, 1)
                    for d in (2, 3)],
                ("AAPL", "1d-hvol"): [
                    (datetime(2024, 1, d), .2, .2, .2, .2, 0)
                    for d in (2, 3, 4)],
                ("MSFT", "1d-hvol"): [
                    (datetime(2024, 1, d), .2, .2, .2, .2, 0)
                    for d in (2, 3, 4)],
            }
            calendar = sv.consensus_calendar(
                root, read_fn=lambda _r, t, iv: cal_bars[(t, iv)],
                min_tickers=2)
        finally:
            sv.ss.load_manifest = old_load
            if old_re is not None:
                sv.ss.TICKER_DIR_RE = old_re
        check(("AAPL", "1m") in got and ("AAPL", "15m") in got
              and ("MSFT", "30m") in got,
              "discover_series finds RTH series from the manifests")
        check(("AAPL", "1m-pre") not in got,
              "discover_series drops -pre/-post extended series")
        check(("AAPL", "1m-iv") in kinds
              and ("AAPL", "1d-hvol") in kinds
              and ("AAPL", "1m-iv-pre") not in kinds,
              "discover_series includes selected RTH kinds but not kind sessions")
        check(iv_only == [("AAPL", "1m-iv")],
              "discover_series kinds selection includes exactly the requested kind")
        check(("AAPL", "1m-iv-pre") in everything
              and ("AAPL", "1m-pre") in everything,
              "discover_series kinds=None and rth_only=False preserve all series")
        legacy = [("AAPL", iv) for iv in sorted(mans["AAPL"]["intervals"])
                  if "-" not in iv]
        legacy += [("MSFT", iv) for iv in sorted(mans["MSFT"]["intervals"])
                   if "-" not in iv]
        check(got == legacy,
              "discover_series default is byte-identical to the legacy filter")
        check(calendar == {date(2024, 1, 2), date(2024, 1, 3)},
              "consensus_calendar excludes volatility-contributed days")
        check(all(t in ("AAPL", "MSFT") for t, _ in got),
              "discover_series skips non-ticker dirs (junk1)")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _clean_series_data(n_days=5, base=300.0, start=date(2026, 6, 1)):
    bars = []
    for i in range(n_days):
        bars += day_bars(start + timedelta(days=i), 60, base + i)
    der = sv.derive_daily(bars, min_bars=50)
    return bars, ref_from_derived(der)


def test_revalidate_library_basic():
    bars, ref = _clean_series_data()
    rep = sv._revalidate_library_body(
        "root", series=[("AAPL", "1m"), ("MSFT", "1m")],
        read_fn=lambda root, t, iv: bars, ref_fn=lambda t, rng: ref)
    check(rep["summary"]["total"] == 2, "revalidate covers both series")
    check(rep["summary"]["ok"] == 2 and rep["summary"]["problems"] == 0,
          "two clean series -> ok=2, problems=0")
    check(all(r["status"] == "ok" for r in rep["results"]),
          "every result classified ok")


def test_revalidate_library_sorts_and_flags():
    clean_bars, clean_ref = _clean_series_data()

    # mid-series 2:1 step (constant base so the step comes only from the factor)
    split_bars = []
    for i in range(40):
        split_bars += day_bars(date(2026, 1, 1) + timedelta(days=i), 60, 300.0)
    split_der = sv.derive_daily(split_bars, min_bars=50)
    split_ref = {}
    for i, d in enumerate(sorted(split_der)):
        f = 2.0 if i < 20 else 1.0
        o, h, lo, c, v = split_der[d]
        split_ref[d.isoformat()] = (o / f, h / f, lo / f, c / f, v)

    # one-day high anomaly (a gap/missing chunk)
    gap_bars, gap_ref = _clean_series_data()
    bad = date(2026, 6, 3).isoformat()
    o, h, lo, c, v = gap_ref[bad]
    gap_ref[bad] = (o, h * 1.05, lo, c, v)

    novr_ref = {"1999-01-04": (1.0, 1.0, 1.0, 1.0, 1)}   # no shared dates

    data = {"AAPL": (clean_bars, clean_ref), "SPLT": (split_bars, split_ref),
            "GAPS": (gap_bars, gap_ref), "ERRT": (clean_bars, "RAISE"),
            "NOVR": (clean_bars, novr_ref)}

    def read_fn(root, t, iv):
        return data[t][0]

    def ref_fn(t, rng):
        r = data[t][1]
        if r == "RAISE":
            raise RuntimeError("boom")
        return r

    rep = sv._revalidate_library_body(
        "root", series=[("AAPL", "1m"), ("SPLT", "1m"), ("GAPS", "1m"),
                        ("ERRT", "1m"), ("NOVR", "1m")],
        read_fn=read_fn, ref_fn=ref_fn)
    order = [r["ticker"] for r in rep["results"]]
    check(order == ["SPLT", "GAPS", "ERRT", "NOVR", "AAPL"],
          f"results sorted worst-first (got {order})")
    by_t = {r["ticker"]: r["status"] for r in rep["results"]}
    check(by_t["SPLT"] == "split", "SPLT classified as split")
    check(by_t["GAPS"] == "discrepancy", "GAPS classified as discrepancy")
    check(by_t["ERRT"] == "error", "ERRT (ref raised) classified as error")
    check(by_t["NOVR"] == "inconclusive", "NOVR classified as inconclusive")
    check(by_t["AAPL"] == "ok", "AAPL classified as ok")
    s = rep["summary"]
    check(s["total"] == 5 and s["ok"] == 1 and s["problems"] == 2
          and s["needs_attention"] == 2,
          f"summary counts: total5/ok1/problems2/attention2 (got {s})")


def test_revalidate_force_fresh_clears_cache():
    sv._REF_CACHE[("ZZ", "5Y")] = (1000.0, {"x": (1, 1, 1, 1, 1)})
    rep = sv._revalidate_library_body("root", series=[], force_fresh=True)
    check(not sv._REF_CACHE,
          "force_fresh=True clears the reference cache before revalidating")
    check(rep["summary"]["total"] == 0, "empty series -> empty report")
    sv._REF_CACHE[("ZZ", "5Y")] = (1000.0, {"x": (1, 1, 1, 1, 1)})
    sv._revalidate_library_body("root", series=[], force_fresh=False)
    check(sv._REF_CACHE, "force_fresh=False leaves the cache intact")
    sv._REF_CACHE.clear()


def test_revalidate_discovers_when_series_none():
    # series=None -> discover_series is used. Patch it to avoid disk, and patch
    # validate_series so we only assert the wiring (discover -> validate -> sort).
    old_disc, old_val = sv.discover_series, sv._validate_series_body
    sv.discover_series = lambda root: [("AAPL", "1m"), ("MSFT", "1m")]
    sv._validate_series_body = lambda root, t, iv, **k: (
        {"checked": 5, "score": 1.0, "flagged_days": []} if t == "AAPL"
        else {"suspected_factor": 2.0, "score": 1.0, "flagged_days": []})
    try:
        rep = sv._revalidate_library_body("root", force_fresh=False)
    finally:
        sv.discover_series, sv._validate_series_body = old_disc, old_val
    check(rep["summary"]["total"] == 2, "series=None discovers + validates all")
    check(rep["results"][0]["ticker"] == "MSFT",
          "the discovered split (MSFT) sorts ahead of the clean AAPL")


def test_read_series_recent_months():
    import re as _re
    saved = {k: getattr(sv.ss, k, None) for k in
             ("canonical_ticker", "load_manifest", "manifest_months",
              "month_file_path", "find_month_file", "read_month_file",
              "read_month_file_fast")}
    sv.ss.canonical_ticker = lambda t: t
    sv.ss.load_manifest = lambda d: {"intervals": {}}
    sv.ss.manifest_months = lambda man, iv: {"2026-01": {}, "2026-02": {},
                                             "2026-03": {}}
    sv.ss.month_file_path = lambda root, canon, y, m, iv, fmt=None: f"{y}-{m:02d}"
    sv.ss.find_month_file = lambda root, canon, y, m, iv: f"{y}-{m:02d}"
    sv.ss.read_month_file = lambda path: ([path], None)
    sv.ss.read_month_file_fast = lambda path, sha=None: ([path], None)
    try:
        allm = sv.read_series("root", "T", "1m")
        last2 = sv.read_series("root", "T", "1m", recent_months=2)
    finally:
        for k, v in saved.items():
            if v is not None:
                setattr(sv.ss, k, v)
    check(allm == ["2026-01", "2026-02", "2026-03"],
          f"read_series reads all months by default (got {allm})")
    check(last2 == ["2026-02", "2026-03"],
          f"recent_months=2 reads ONLY the last 2 months (got {last2})")


def test_crosscheck_gate():
    g = sv.CrossCheckGate(threshold=0.20, min_sample=5)
    for _ in range(4):
        check(g.record({"status": "ok"}) is None, "gate: silent below min_sample")
    check(g.record({"status": "ok"}) is None, "gate: 5 clean -> no ask")

    g2 = sv.CrossCheckGate(threshold=0.20, min_sample=5)
    acts = [g2.record({"status": s}) for s in
            ["ok", "ok", "ok", "ok", "split"]]        # 1/5 = exactly 20%
    check(all(a is None for a in acts),
          "gate: exactly 20% does NOT trip (strict greater-than)")
    check(g2.record({"status": "split"}) == "ask",     # 2/6 = 33% > 20%
          "gate: crossing >20% trips 'ask'")
    check(g2.record({"status": "split"}) is None, "gate: asks only once")

    g3 = sv.CrossCheckGate(threshold=0.20, min_sample=3)
    for s in ("error", "inconclusive", "ok"):
        g3.record({"status": s})
    check(g3.fraction() == 0.0 and g3.alarmed == 0,
          "gate: error/inconclusive/ok do NOT ring the alarm")
    check(g3.record({"status": "discrepancy",          # 1/4 = 25% > 20%
                     "ticker": "X"}) == "ask",
          "gate: a 'discrepancy' counts and trips at >20%")
    al = g3.alarmed_results()
    check(len(al) == 1 and al[0]["status"] == "discrepancy",
          "gate: alarmed_results filters to the significant ones")
    snap = g3.snapshot()
    check(snap["total"] == 4 and snap["alarmed"] == 1
          and abs(snap["fraction"] - 0.25) < 1e-9,
          f"gate: snapshot totals (got {snap})")


def test_revalidate_split_mag_ordering():
    old = sv._validate_series_body

    def fake(root, t, iv, **k):
        if t == "BIGSCALE":      # uniform wrong scale: factor 4, deceptive 1.0
            return {"suspected_factor": 4.0, "level_shift": None,
                    "score": 1.0, "flagged_days": [], "checked": 30}
        return {"suspected_factor": None,             # mild mid-series step
                "level_shift": {"factor": 0.95, "date": "2026-03-04"},
                "score": 0.97, "flagged_days": ["2026-03-04"], "checked": 30}

    sv._validate_series_body = fake
    try:
        rep = sv._revalidate_library_body(
            "root", series=[("SMALLSTEP", "1m"), ("BIGSCALE", "1m")],
            force_fresh=False)
    finally:
        sv._validate_series_body = old
    order = [r["ticker"] for r in rep["results"]]
    check(order == ["BIGSCALE", "SMALLSTEP"],
          f"revalidate: bigger split magnitude sorts first (got {order})")
    big = next(r for r in rep["results"] if r["ticker"] == "BIGSCALE")
    check(big["score"] is None,
          "revalidate: a uniform wrong-scale series suppresses the misleading "
          "1.0 score")


def test_save_daily_reference():
    import shutil
    import tempfile
    base = tempfile.mkdtemp(prefix="sv_ref_")
    root = Path(base) / "Stock Data Storage"
    ref = {"2026-06-15": (294.04, 294.19, 292.01, 292.39, 1076188),
           "2026-06-16": (292.5, 293.0, 291.90, 292.1, 500000)}
    try:
        p = sv.save_daily_reference(root, "aapl", "5Y", ref, asof="2026-06-22")
        check(p is not None, "save_daily_reference returns a path")
        pp = Path(p)
        check(pp.name == "AAPL_daily_5Y_asof-2026-06-22.csv",
              f"sidecar stamped with TICKER/range/asof (got {pp.name})")
        check("Validation Reference" in p
              and "Stock Data Storage" not in str(pp.parent),
              "saved ALONGSIDE the data bank, NOT inside it")
        lines = pp.read_text(encoding="utf-8").splitlines()
        check(lines[0] == "Date,open,high,low,close,volume",
              "reference header is Date,open,high,low,close,volume")
        check(lines[1] == "2026-06-15,294.04,294.19,292.01,292.39,1076188",
              f"row formatted, trailing zeros dropped (got {lines[1]})")
        check(lines[2].split(",")[3] == "291.9",
              "trailing zero dropped (291.90 -> 291.9)")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_save_daily_reference_storage_error_is_best_effort():
    import tempfile
    with tempfile.TemporaryDirectory(prefix="sv_ref_fault_") as base:
        root = Path(base) / "Stock Data Storage"
        ref = {"2026-06-15": (294.04, 294.19, 292.01, 292.39, 1076188)}
        path = sv.save_daily_reference(
            root, "AAPL", "5Y", ref, asof="2026-06-22")
        check(path is not None, "reference fault: seed sidecar saved")
        before = Path(path).read_bytes()
        original = sv.ss._atomic_write_bytes

        def fail_write(*_args, **_kwargs):
            raise sv.ss.StorageError("injected short write")

        sv.ss._atomic_write_bytes = fail_write
        try:
            try:
                failed = sv.save_daily_reference(
                    root, "AAPL", "5Y",
                    {"2026-06-15": (1, 1, 1, 1, 1)},
                    asof="2026-06-22")
            except sv.ss.StorageError:
                failed = "raised"
        finally:
            sv.ss._atomic_write_bytes = original
        check(failed is None,
              "reference fault: storage failure remains best-effort")
        check(Path(path).read_bytes() == before,
              "reference fault: prior sidecar remains byte-identical")


def test_validate_series_saves_reference():
    import shutil
    import tempfile
    bars = []
    for i in range(4):
        bars += day_bars(date(2026, 6, 1) + timedelta(days=i), 60, 300.0 + i)
    ref = ref_from_derived(sv.derive_daily(bars, min_bars=50))
    base = tempfile.mkdtemp(prefix="sv_vsref_")
    root = str(Path(base) / "Stock Data Storage")
    try:
        res = sv._validate_series_body(root, "AAPL", "1m",
                                 read_fn=lambda *a: bars,
                                 ref_fn=lambda *a, **k: ref, save_reference=True)
        check(res.get("reference_saved")
              and Path(res["reference_saved"]).is_file(),
              "validate_series(save_reference=True) writes + returns the path")
        res2 = sv._validate_series_body(root, "AAPL", "1m",
                                  read_fn=lambda *a: bars,
                                  ref_fn=lambda *a, **k: ref)
        check(res2.get("reference_saved") is None,
              "validate_series default does NOT save a reference (opt-in)")
    finally:
        shutil.rmtree(base, ignore_errors=True)


# --- extended-hours validation ----------------------------------------------

def _ext_bars(d, start_t, n, base, vol=10):
    out = []
    t = datetime.combine(d, start_t)
    for i in range(n):
        p = round(base + i * 0.01, 2)
        out.append((t, p, round(p + 0.03, 2), round(p - 0.03, 2),
                    round(p + 0.01, 2), vol + i))
        t += timedelta(minutes=1)
    return out


def _ext_read(rth, pre, post):
    def rd(root, ticker, interval):
        if interval.endswith("-pre"):
            return pre
        if interval.endswith("-post"):
            return post
        return rth
    return rd


def test_validate_extended_clean():
    d = date(2026, 6, 15)
    rth = day_bars(d, 80, 300.0)               # open 300.0, close ~300.81
    pre = _ext_bars(d, time(9, 20), 9, 299.90)     # ends ≈ 300.0
    post = _ext_bars(d, time(16, 0), 9, 300.81)    # starts ≈ 300.81
    res = sv.validate_extended("r", "AAPL", "1m",
                               read_fn=_ext_read(rth, pre, post))
    check(res.get("score") == 1.0 and not res["flagged"],
          f"clean extended day: score 1.0, no flags (got {res.get('flagged')})")
    check(res["pre_days"] == 1 and res["post_days"] == 1,
          "extended: pre/post day counts reported")


def test_validate_extended_boundary():
    d = date(2026, 6, 15)
    rth = day_bars(d, 80, 300.0)
    pre2x = _ext_bars(d, time(9, 20), 9, 600.0)          # 2× scale error
    post = _ext_bars(d, time(16, 0), 9, 300.81)
    res = sv.validate_extended("r", "AAPL", "1m",
                               read_fn=_ext_read(rth, pre2x, post))
    check("boundary_pre" in {f["kind"] for f in res["flagged"]}
          and res["score"] < 1.0,
          "a 2× pre-market scale error flags boundary_pre")
    pre = _ext_bars(d, time(9, 20), 9, 299.90)
    post2x = _ext_bars(d, time(16, 0), 9, 600.0)         # 2× scale error
    res2 = sv.validate_extended("r", "AAPL", "1m",
                                read_fn=_ext_read(rth, pre, post2x))
    check("boundary_post" in {f["kind"] for f in res2["flagged"]},
          "a 2× after-hours scale error flags boundary_post")


def test_validate_extended_structure():
    d = date(2026, 6, 15)
    rth = day_bars(d, 80, 300.0)
    post = _ext_bars(d, time(16, 0), 9, 300.81)
    bad = _ext_bars(d, time(9, 20), 9, 299.90)
    bt, o, _h, lo, c, v = bad[3]
    bad[3] = (bt, o, c - 1.0, lo, c, v)                  # high below close
    res = sv.validate_extended("r", "AAPL", "1m",
                               read_fn=_ext_read(rth, bad, post))
    check("ohlc" in {f["kind"] for f in res["flagged"]},
          "an invalid-OHLC extended bar flags ohlc")
    badwin = _ext_bars(d, time(9, 20), 9, 299.90)
    bt, o, h, lo, c, v = badwin[0]
    badwin[0] = (datetime.combine(d, time(10, 0)), o, h, lo, c, v)  # out of pre
    res2 = sv.validate_extended("r", "AAPL", "1m",
                                read_fn=_ext_read(rth, badwin, post))
    check("window" in {f["kind"] for f in res2["flagged"]},
          "an extended bar outside its session window flags window")


def test_validate_extended_volume():
    d = date(2026, 6, 15)
    rth = day_bars(d, 80, 300.0)               # RTH volume ~11160
    pre = _ext_bars(d, time(9, 20), 9, 299.90, vol=5000)   # absurd ext volume
    post = _ext_bars(d, time(16, 0), 9, 300.81, vol=5000)
    res = sv.validate_extended("r", "AAPL", "1m",
                               read_fn=_ext_read(rth, pre, post))
    check("volume" in {f["kind"] for f in res["flagged"]},
          "extended volume exceeding RTH flags volume")


def test_validate_extended_no_data():
    res = sv.validate_extended("r", "AAPL", "1m",
                               read_fn=lambda root, t, iv: [])
    check("error" in res and "no extended" in res["error"],
          "validate_extended errors clearly when no -pre/-post is stored")


def test_validate_extended_no_rth_inconclusive():
    # extended present but NO RTH to anchor the boundary check -> must be
    # INCONCLUSIVE, never a clean score 1.0 (bug-hunt finding #7). Here pre is
    # 2× scaled, which a boundary check WOULD catch — but there's no RTH anchor.
    d = date(2026, 6, 15)
    pre = _ext_bars(d, time(9, 20), 9, 600.0)
    post = _ext_bars(d, time(16, 0), 9, 300.81)
    res = sv.validate_extended("r", "AAPL", "1m",
                               read_fn=_ext_read([], pre, post))   # RTH empty
    check("error" in res and "anchor" in res["error"],
          f"no-RTH extended check is inconclusive, not score 1.0 (got {res})")


def test_cross_validate_ticker():
    bars = []
    for i in range(25):
        bars += day_bars(date(2026, 6, 1) + timedelta(days=i), 60, 300.0 + i)
    ref = ref_from_derived(sv.derive_daily(bars, min_bars=50))
    v = sv._cross_validate_ticker_body("root", "AAPL", "1m",
                                 read_fn=lambda *a: bars,
                                 ref_fn=lambda *a, **k: ref, asof="T",
                                 fingerprint_fn=xval_fingerprint)
    check(v["status"] == "validated" and v["ticker"] == "AAPL",
          f"cross_validate: matching data -> validated (got {v['status']})")
    fp = v.get("interval_fingerprint") or {}
    try:
        aware_times = (datetime.fromisoformat(v["started_at"]).utcoffset()
                       is not None
                       and datetime.fromisoformat(v["finished_at"]).utcoffset()
                       is not None)
    except (KeyError, TypeError, ValueError):
        aware_times = False
    check(v.get("schema_version") == sv.CROSS_VALIDATION_SCHEMA_VERSION
          and v.get("provider") == "stockanalysis"
          and v.get("requested_range") == "5Y" and aware_times
          and fp.get("current") is True
          and fp.get("sha256")
          == fp.get("before_sha256") == fp.get("after_sha256") == "a" * 64,
          f"cross_validate: schema-v4 provenance is complete ({v})")
    coverage = v.get("reference_coverage") or {}
    check(coverage.get("reference_first_date") == sorted(ref)[0]
          and coverage.get("reference_last_date") == sorted(ref)[-1]
          and coverage.get("reference_day_count") == len(ref)
          and coverage.get("stored_first_date") == "2026-06-01"
          and coverage.get("stored_last_date") == "2026-06-25"
          and coverage.get("stored_day_count") == 25
          and coverage.get("full_history_requested") is False
          and coverage.get("full_history_head_ok") is None,
          f"cross_validate: received and stored ranges are explicit ({coverage})")

    old_bars = day_bars(date(2020, 1, 2), 60, 100.0)
    full_bars = old_bars + bars
    full_ref = ref_from_derived(sv.derive_daily(full_bars, min_bars=50))
    short_ref = {day: row for day, row in full_ref.items()
                 if day >= "2026-01-01"}
    v_short = sv._cross_validate_ticker_body(
        "root", "AAPL", "1m", rng="Max",
        read_fn=lambda *a: full_bars,
        ref_fn=lambda *a, **k: short_ref,
        fingerprint_fn=xval_fingerprint)
    short_coverage = v_short.get("reference_coverage") or {}
    check(v_short.get("status") == "inconclusive"
          and short_coverage.get("full_history_requested") is True
          and short_coverage.get("full_history_head_ok") is False
          and short_coverage.get("head_gap_days", 0)
          > sv.FULL_HISTORY_HEAD_TOLERANCE_DAYS
          and "shorter than stored history" in v_short.get("note", ""),
          f"cross_validate: short Max response fails closed ({v_short})")
    v_full = sv._cross_validate_ticker_body(
        "root", "AAPL", "1m", rng="Max",
        read_fn=lambda *a: full_bars,
        ref_fn=lambda *a, **k: full_ref,
        fingerprint_fn=xval_fingerprint)
    full_coverage = v_full.get("reference_coverage") or {}
    check(v_full.get("status") == "validated"
          and full_coverage.get("full_history_requested") is True
          and full_coverage.get("full_history_head_ok") is True
          and full_coverage.get("head_gap_days") == 0,
          f"cross_validate: complete Max response can publish ({v_full})")
    # one wildly-off day -> a flagged discrepancy (not a clean tick)
    ref_bad = ref_from_derived(sv.derive_daily(bars, min_bars=50))
    bad = sorted(ref_bad)[12]
    o, h, lo, c, vv = ref_bad[bad]
    ref_bad[bad] = (o * 10, h * 10, lo * 10, c * 10, vv)
    vb = sv._cross_validate_ticker_body("root", "AAPL", "1m",
                                  read_fn=lambda *a: bars,
                                  ref_fn=lambda *a, **k: ref_bad, asof="T",
                                  fingerprint_fn=xval_fingerprint)
    check(vb["status"] == "discrepancy",
          f"cross_validate: a wildly-off day -> discrepancy (got {vb['status']})")
    # the discrepancy verdict carries the ENRICHED detail for the data-menu
    # double-click expander: severity + recommendation + per-day % rows.
    det = vb.get("detail") or {}
    check(vb.get("severity") in ("HIGH", "MEDIUM", "LOW")
          and det.get("severity") == vb.get("severity"),
          f"cross_validate: discrepancy carries a severity (got {vb.get('severity')})")
    check(bool(det.get("recommendation")) and det.get("flagged_count", 0) >= 1
          and any((r.get("percent") or 0) > 0 for r in det.get("rows", [])),
          f"cross_validate: detail has recommendation + flagged % rows ({det})")
    counts = vb.get("evidence_counts") or {}
    check(counts.get("row_count") == len(det.get("rows", []))
          and counts.get("distinct_dates", 0) >= 1
          and any(counts.get("field_counts", {}).get(f, 0)
                  for f in ("open", "high", "low", "close")),
          f"cross_validate: bounded field/date counts match detail ({counts})")
    import tempfile as _tf
    from pathlib import Path as _P
    _r = _P(_tf.mkdtemp())
    sv.record_cross_validation(_r, vb)
    _back = sv.load_cross_validation(_r).get("AAPL 1m", {})
    check((_back.get("detail") or {}).get("rows") == det.get("rows"),
          "cross_validate: enriched detail survives the sidecar JSON round-trip")
    # unknown symbol (empty online ref) -> unavailable
    ve = sv._cross_validate_ticker_body("root", "NOPE", "1m",
                                  read_fn=lambda *a: bars,
                                  ref_fn=lambda *a, **k: {}, asof="T",
                                  fingerprint_fn=xval_fingerprint)
    check(ve["status"] == "unavailable",
          f"cross_validate: empty online ref -> unavailable (got {ve['status']})")

    # a dead/raising source -> unavailable, never propagates
    def _boom(*a, **k):
        raise RuntimeError("HTTP 404")
    vx = sv._cross_validate_ticker_body("root", "AAPL", "1m",
                                  read_fn=lambda *a: bars, ref_fn=_boom,
                                  fingerprint_fn=xval_fingerprint)
    check(vx["status"] == "unavailable",
          "cross_validate: a dead source is 'unavailable', not an exception")

    fingerprints = [xval_fingerprint(None, "AAPL", "1m", "b" * 64),
                    xval_fingerprint(None, "AAPL", "1m", "c" * 64)]

    def racing_fp(*_args):
        return fingerprints.pop(0)

    vr = sv._cross_validate_ticker_body("root", "AAPL", "1m",
                                  read_fn=lambda *a: bars,
                                  ref_fn=lambda *a, **k: ref_bad,
                                  fingerprint_fn=racing_fp)
    rfp = vr.get("interval_fingerprint") or {}
    check(vr.get("status") == "inconclusive"
          and rfp.get("current") is False
          and rfp.get("observed_status") == "discrepancy"
          and "changed" in rfp.get("error", ""),
          f"cross_validate: interval race cannot publish warning ({vr})")

    def broken_fp(*_args):
        raise RuntimeError("bad manifest " + "x" * 500)

    vm = sv._cross_validate_ticker_body("root", "AAPL", "1m",
                                  read_fn=lambda *a: bars,
                                  ref_fn=lambda *a, **k: ref,
                                  fingerprint_fn=broken_fp)
    mfp = vm.get("interval_fingerprint") or {}
    check(vm.get("status") == "inconclusive"
          and mfp.get("current") is False
          and mfp.get("observed_status") == "validated"
          and len(mfp.get("error", "")) <= sv._XVAL_ERROR_CAP,
          f"cross_validate: malformed provenance fails closed and bounded ({vm})")


def test_lineage_boundary_pins_are_bounded_and_fail_open():
    import copy
    import shutil
    import tempfile

    root = Path(tempfile.mkdtemp(prefix="sv_lineage_"))
    policy_path = root / sv._LINEAGE_BOUNDARY_FILE
    # A pin binds the LINEAGE-ERA identity (months at or before the boundary
    # month), so the fixture needs a real manifest: the policy re-captures that
    # identity from storage on every evaluation (F-CLAUDE-80-1).
    ticker_dir = root / "APO"
    ticker_dir.mkdir(parents=True, exist_ok=True)
    _manifest = sv.ss.new_manifest("APO", "APO")
    for _month in ("2021-12", "2022-01"):
        sv.ss.manifest_months(_manifest, "1m")[_month] = {
            "status": "present", "sha256": "c" * 64, "rows": 1500,
            "first": "12/15/2021 9:30:00", "last": "1/14/2022 16:00:00"}
    sv.ss.save_manifest(ticker_dir, _manifest)
    runtime_fp = sv.ss.interval_state_fingerprint_through_month(
        root, "APO", "1m", "2022-01")
    pin = {
        "ticker": "APO",
        "boundary_date": "2022-01-03",
        "verdict": "lineage_boundary",
        "scope": "price",
        "interval_fingerprints": [runtime_fp],
        "review": {
            "checkpoint": "ffe8fcb",
            "approval_commit": "9648b08",
            "approved_by": "USER",
            "approved_at": "2026-07-31T12:00:00-04:00",
            "user_verdict_date": "2026-07-31",
        },
    }
    observed = {
        "checked": 10,
        "matched": 8,
        "score": 0.8,
        "basis_ratio": 1.0,
        "suspected_factor": None,
        "level_shift": {
            "date": "2021-12-30", "factor": 1.5,
            "lead": 1.0, "trail": 1.5, "pct": 0.5,
        },
        "max_norm_dev": 0.5,
        "flagged": [
            {"date": "2021-12-30", "field": "high", "derived": 15.0,
             "reference": 10.0, "norm_dev": 0.5},
            {"date": "2021-12-30", "field": "level_shift",
             "derived": 1.5, "reference": 1.0, "norm_dev": 0.5},
            {"date": "2022-01-03", "field": "low", "derived": 13.0,
             "reference": 10.0, "norm_dev": 0.3},
        ],
        "flagged_days": ["2021-12-30", "2022-01-03"],
        "reference_coverage": {"full_history_requested": False},
    }
    real_validate = sv._validate_series_body

    def write_policy(entries, **extra):
        payload = {"version": 1, "lineage_boundaries": entries}
        payload.update(extra)
        policy_path.write_text(json.dumps(payload), encoding="utf-8")

    def run(ticker="APO", interval="1m", result=None, sha="a" * 64):
        sv._validate_series_body = lambda *_a, **_k: copy.deepcopy(result or observed)
        return sv._cross_validate_ticker_body(
            root, ticker, interval, ref_fn=lambda *_a, **_k: {"ok": (1,)},
            read_fn=lambda *_a, **_k: [],
            fingerprint_fn=lambda r, t, iv: xval_fingerprint(r, t, iv, sha))

    try:
        write_policy([pin])
        real_bars = []
        for offset in range(25):
            real_bars += day_bars(
                date(2021, 12, 15) + timedelta(days=offset),
                60, 200.0 + offset)
        real_reference = ref_from_derived(
            sv.derive_daily(real_bars, min_bars=50))
        for bad_day in ("2021-12-22", "2022-01-03"):
            o, h, lo, c, volume = real_reference[bad_day]
            real_reference[bad_day] = (
                o * 10, h * 10, lo * 10, c * 10, volume)
        real_path = sv._cross_validate_ticker_body(
            root, "APO", "1m", read_fn=lambda *_a: real_bars,
            ref_fn=lambda *_a, **_k: real_reference,
            fingerprint_fn=xval_fingerprint)
        real_known = (real_path.get("detail") or {}).get(
            "known_lineage_era") or {}
        check(real_path.get("status") == "discrepancy"
              and real_path.get("detail", {}).get("flagged_count") == 1
              and real_known.get("flagged_count") == 1
              and {row.get("date") for row in real_path.get(
                  "detail", {}).get("rows", [])} == {"2022-01-03"}
              and {row.get("date") for row in real_known.get("examples", [])}
              == {"2021-12-22"},
              f"lineage pin: real validator buckets only the pre-boundary spike "
              f"({real_path})")

        applied = run()
        detail = applied.get("detail") or {}
        known = detail.get("known_lineage_era") or {}
        check(applied.get("status") == "discrepancy"
              and applied.get("score") == 0.9
              and detail.get("flagged_count") == 1
              and [row.get("date") for row in detail.get("rows", [])]
              == ["2022-01-03"],
              f"lineage pin: the boundary day stays fully active ({applied})")
        check(known.get("flagged_count") == 1
              and known.get("row_count") == 2
              and known.get("field_counts")
              == {"high": 1, "level_shift": 1}
              and known.get("examples") == observed["flagged"][:2]
              and known.get("level_shift") == observed["level_shift"],
              f"lineage pin: pre-boundary evidence moves intact to its bucket ({known})")
        check((applied.get("evidence_counts") or {}).get("row_count") == 1
              and applied.get("lineage_boundary", {}).get("boundary_date")
              == "2022-01-03"
              and "lineage_boundary_error" not in applied,
              f"lineage pin: active evidence/provenance stay explicit ({applied})")

        pre_only = copy.deepcopy(observed)
        pre_only["matched"] = 9
        pre_only["score"] = 0.9
        pre_only["flagged"] = pre_only["flagged"][:2]
        pre_only["flagged_days"] = ["2021-12-30"]
        cleared = run(result=pre_only)
        check(cleared.get("status") == "validated"
              and cleared.get("score") == 1.0
              and cleared.get("severity") == "LOW"
              and cleared.get("detail", {}).get("flagged_count") == 0
              and cleared.get("detail", {}).get(
                  "known_lineage_era", {}).get("flagged_count") == 1,
              f"lineage pin: a known-only era no longer scores as active ({cleared})")

        malformed = copy.deepcopy(pin)
        malformed["unknown"] = True
        write_policy([malformed])
        bad = run()
        check(bad.get("status") == "discrepancy"
              and bad.get("score") == 0.8
              and bad.get("detail", {}).get("flagged_count") == 2
              and "schema" in bad.get("lineage_boundary_error", "")
              and "LINEAGE PIN IGNORED" in bad.get("note", "")
              and "known_lineage_era" not in bad.get("detail", {}),
              f"lineage pin: unknown fields fail open and loudly ({bad})")

        write_policy([pin, copy.deepcopy(pin)])
        duplicate = run()
        check(duplicate.get("score") == 0.8
              and "multiple or contradictory" in duplicate.get(
                  "lineage_boundary_error", ""),
              f"lineage pin: duplicate ticker pins fail open ({duplicate})")

        stale_pin = copy.deepcopy(pin)
        stale_pin["interval_fingerprints"][0]["sha256"] = "b" * 64
        write_policy([stale_pin])
        stale = run()
        check(stale.get("score") == 0.8
              and "fingerprint is stale" in stale.get(
                  "lineage_boundary_error", ""),
              f"lineage pin: stale fingerprints fail open ({stale})")

        write_policy([pin])
        unbound_interval = run(interval="1d")
        check(unbound_interval.get("score") == 0.8
              and "no fingerprint for 1d" in unbound_interval.get(
                  "lineage_boundary_error", ""),
              f"lineage pin: an unbound price interval fails open "
              f"({unbound_interval})")

        racing_fingerprints = iter((
            xval_fingerprint(root, "APO", "1m", "a" * 64),
            xval_fingerprint(root, "APO", "1m", "b" * 64),
        ))
        sv._validate_series_body = lambda *_a, **_k: copy.deepcopy(observed)
        raced = sv._cross_validate_ticker_body(
            root, "APO", "1m", ref_fn=lambda *_a, **_k: {"ok": (1,)},
            fingerprint_fn=lambda *_a: next(racing_fingerprints))
        check(raced.get("status") == "inconclusive"
              and raced.get("detail", {}).get("flagged_count") == 2
              and "known_lineage_era" not in raced.get("detail", {})
              and "changed during lineage pin evaluation" in raced.get(
                  "lineage_boundary_error", ""),
              f"lineage pin: an interval race cannot suppress evidence ({raced})")

        write_policy([pin])
        nvda = run(ticker="NVDA")
        check(nvda.get("score") == 0.8
              and "lineage_boundary" not in nvda
              and "lineage_boundary_error" not in nvda,
              f"lineage pin: an unpinned ticker is unchanged ({nvda})")

        write_policy([pin], unexpected=True)
        bars = [(datetime(2024, 1, 2, 9, 30 + i),
                 0.2, 0.21, 0.19, 0.2, 100) for i in range(3)]
        vol = sv._cross_validate_ticker_body(
            root, "APO", "1d-hvol", read_fn=lambda *_a: bars,
            fingerprint_fn=xval_fingerprint)
        check(vol.get("provider") == "internal-structural"
              and "lineage_boundary" not in vol
              and "lineage_boundary_error" not in vol
              and "known_lineage_era" not in (vol.get("detail") or {}),
              f"lineage pin: volatility kinds never read/apply price pins ({vol})")
    finally:
        sv._validate_series_body = real_validate
        shutil.rmtree(root, ignore_errors=True)


def test_cross_validation_persist():
    import shutil
    import tempfile
    base = tempfile.mkdtemp(prefix="sv_cross_")
    root = str(Path(base) / "Stock Data Storage")
    try:
        check(sv.load_cross_validation(root) == {},
              "load_cross_validation: missing file -> {}")
        Path(root).mkdir(parents=True, exist_ok=True)
        (Path(root) / "_cross_validation.json").write_text(
            json.dumps({"AAPL": {"ticker": "AAPL", "status": "legacy"}}),
            encoding="utf-8")
        check(sv.load_cross_validation(root).get("AAPL", {}).get("status")
              == "legacy",
              "load_cross_validation exposes legacy plain-ticker keys")
        p = sv.record_cross_validation(
            root, {"ticker": "aapl", "interval": "1m",
                   "status": "validated", "note": "ok", "asof": "T",
                   "schema_version": 2, "provider": "stockanalysis",
                   "interval_fingerprint": {
                       "schema_version": 1, "algorithm": "sha256",
                       "sha256": "a" * 64,
                       "before_sha256": "a" * 64,
                       "after_sha256": "a" * 64, "current": True}})
        check(p is not None and "_cross_validation.json" in p,
              "record_cross_validation writes the sidecar JSON")
        data = sv.load_cross_validation(root)
        check("AAPL 1m" in data and data["AAPL 1m"]["status"] == "validated",
              "record/load round-trips, keyed by ticker + interval")
        check(data["AAPL 1m"].get("schema_version") == 2
              and data["AAPL 1m"].get("interval_fingerprint", {}).get("current")
              is True,
              "record/load preserves schema-v2 provenance unchanged")
        check("AAPL" not in data,
              "record rewrites matching legacy plain-ticker keys")
        sv.record_cross_validation(root, {"ticker": "AAPL",
                                          "interval": "1m",
                                          "status": "discrepancy"})
        check(sv.load_cross_validation(root)["AAPL 1m"]["status"] == "discrepancy",
              "record overwrites the same series' prior verdict")
        sv.record_cross_validation(root, {"ticker": "AAPL",
                                          "interval": "1m-iv",
                                          "status": "validated"})
        data = sv.load_cross_validation(root)
        check(data["AAPL 1m"]["status"] == "discrepancy"
              and data["AAPL 1m-iv"]["status"] == "validated",
              "record keeps 1m and 1m-iv cross-validation verdicts separate")
        check(any(e.get("status") in ("discrepancy", "structural-flag")
                  for k, e in data.items() if k.startswith("AAPL ")),
              "cross-validation ticker aggregate still sees interval flags")
        check(sv.record_cross_validation(root, {"ticker": ""}) is None,
              "record with no ticker is a no-op (None)")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_cross_validation_counts_survive_row_cap():
    start = date(2025, 1, 1)
    flagged = [{"date": (start + timedelta(days=i)).isoformat(),
                "field": "volume", "derived": 1, "reference": 2,
                "norm_dev": 0.6}
               for i in range(sv._XVAL_ROW_CAP)]
    flagged.append({"date": (start + timedelta(days=sv._XVAL_ROW_CAP)).isoformat(),
                    "field": "open", "derived": 1, "reference": 2,
                    "norm_dev": 0.5})
    res = {"flagged": flagged,
           "flagged_days": sorted({row["date"] for row in flagged}),
           "checked": len(flagged), "matched": 0,
           "level_shift": None, "suspected_factor": None}
    detail = sv._xval_detail(res)
    counts = sv._xval_evidence_counts(detail)
    check(len(detail["rows"]) == sv._XVAL_ROW_CAP
          and counts.get("row_count") == sv._XVAL_ROW_CAP + 1
          and counts.get("persisted_row_count") == sv._XVAL_ROW_CAP
          and counts.get("truncated") is True
          and counts.get("distinct_dates") == sv._XVAL_ROW_CAP + 1
          and counts.get("field_counts", {}).get("open") == 1,
          f"full field/date counts retain OHLC evidence beyond row cap ({counts})")


def test_cross_validate_manifest_fingerprint_integration():
    import shutil
    import tempfile
    base = Path(tempfile.mkdtemp(prefix="sv_xval_fp_"))
    root = base / "bank"
    ticker_dir = root / "AAPL"
    ticker_dir.mkdir(parents=True)
    bars = []
    for i in range(25):
        bars += day_bars(date(2026, 6, 1) + timedelta(days=i), 60, 300.0 + i)
    ref = ref_from_derived(sv.derive_daily(bars, min_bars=50))

    def entry(sha, rows):
        return {"status": "present", "sha256": sha, "rows": rows,
                "first": "6/1/2026 9:30:00", "last": "6/25/2026 10:29:00"}

    try:
        manifest = sv.ss.new_manifest("AAPL", "AAPL")
        sv.ss.manifest_months(manifest, "1m")["2026-06"] = entry(
            "a" * 64, len(bars))
        sv.ss.manifest_months(manifest, "1d")["2026-06"] = entry("b" * 64, 25)
        sv.ss.save_manifest(ticker_dir, manifest)
        stable = sv._cross_validate_ticker_body(
            root, "AAPL", "1m", read_fn=lambda *a: bars,
            ref_fn=lambda *a, **k: ref)
        check(stable.get("status") == "validated"
              and stable.get("interval_fingerprint", {}).get("current") is True,
              f"default manifest fingerprint integrates with validator ({stable})")

        def mutate_other(*_args, **_kwargs):
            current = sv.ss.load_manifest(ticker_dir)
            current["intervals"]["1d"]["months"]["2026-06"]["rows"] += 1
            sv.ss.save_manifest(ticker_dir, current)
            return ref

        isolated = sv._cross_validate_ticker_body(
            root, "AAPL", "1m", read_fn=lambda *a: bars,
            ref_fn=mutate_other)
        check(isolated.get("status") == "validated"
              and isolated.get("interval_fingerprint", {}).get("current") is True,
              f"unrelated interval write does not stale validation ({isolated})")

        def mutate_selected(*_args, **_kwargs):
            current = sv.ss.load_manifest(ticker_dir)
            current["intervals"]["1m"]["months"]["2026-06"]["rows"] += 1
            sv.ss.save_manifest(ticker_dir, current)
            return ref

        racing = sv._cross_validate_ticker_body(
            root, "AAPL", "1m", read_fn=lambda *a: bars,
            ref_fn=mutate_selected)
        rfp = racing.get("interval_fingerprint") or {}
        check(racing.get("status") == "inconclusive"
              and rfp.get("current") is False
              and rfp.get("observed_status") == "validated",
              f"selected interval write makes validation non-current ({racing})")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_intraday_gaps_clean():
    d = date(2024, 7, 8)
    res = sv.find_intraday_gaps(day_bars(d, 60, 50.0), "1m")
    check(res["gap_count"] == 0 and res["missing_total"] == 0,
          "find_intraday_gaps: a contiguous 1m session has zero gaps")


def test_intraday_gaps_single_minute():
    d = date(2024, 7, 8)
    bars = day_bars(d, 30, 50.0)               # 9:30..9:59 contiguous 1m
    holed = bars[:10] + bars[11:]              # drop 9:40 -> 9:39 jumps to 9:41
    res = sv.find_intraday_gaps(holed, "1m")
    check(res["gap_count"] == 1 and res["missing_total"] == 1,
          "find_intraday_gaps: one missing minute -> 1 gap, 1 missing")
    g = res["gaps"][0]
    check(g["missing"] == 1 and g["date"] == d.isoformat()
          and "09:39" in g["after"] and "09:41" in g["before"],
          "find_intraday_gaps: gap carries date + the two bounding stamps")


def test_intraday_gaps_multi_and_cross_day():
    d1, d2 = date(2024, 7, 8), date(2024, 7, 9)
    b1 = day_bars(d1, 30, 50.0)
    b1 = b1[:10] + b1[13:]                     # drop 3 consecutive minutes
    b2 = day_bars(d2, 30, 51.0)               # next day, fully contiguous
    res = sv.find_intraday_gaps(b1 + b2, "1m")
    check(res["gap_count"] == 1 and res["missing_total"] == 3
          and res["largest_gap"] == 3,
          "find_intraday_gaps: a 3-minute hole is counted as missing=3")
    check(res["days_with_gaps"] == [d1.isoformat()],
          "find_intraday_gaps: the overnight day-boundary is NOT a gap")


def test_intraday_gaps_5m_and_nonintraday():
    d = date(2024, 7, 8)
    b = day_bars(d, 12, 20.0, step_min=5)     # contiguous 5m grid
    res = sv.find_intraday_gaps(b[:4] + b[5:], "5m")
    check(res["step_seconds"] == 300 and res["gap_count"] == 1
          and res["missing_total"] == 1,
          "find_intraday_gaps: a 5m interior gap is detected (step-aware)")
    bad = sv.find_intraday_gaps(day_bars(d, 5, 1.0), "1d")
    check("error" in bad,
          "find_intraday_gaps: a non-intraday interval returns an error")


def test_scan_series_gaps_injected():
    d = date(2024, 7, 8)
    bars = day_bars(d, 20, 70.0)
    holed = bars[:5] + bars[7:]               # drop 9:35 + 9:36
    res = sv.scan_series_gaps("ignored", "ZZZ", "1m",
                              read_fn=lambda *a, **k: holed)
    check(res.get("ticker") == "ZZZ" and res["missing_total"] == 2,
          "scan_series_gaps: reads via read_fn, attaches ticker, counts 2")
    empty = sv.scan_series_gaps("ignored", "ZZZ", "1m",
                                read_fn=lambda *a, **k: [])
    check("error" in empty, "scan_series_gaps: empty series -> error")


def test_scan_all_gaps_aggregate_and_order():
    d1, d2 = date(2024, 7, 8), date(2024, 7, 9)
    a = day_bars(d2, 20, 10.0); a = a[:5] + a[7:]      # A: drop d2 9:35,9:36
    b = day_bars(d1, 20, 20.0); b = b[:10] + b[11:]    # B: drop d1 9:40
    reads = {("A", "1m"): a, ("B", "1m"): b}
    res = sv.scan_all_gaps("ignored", series=[("A", "1m"), ("B", "1m")],
                           write=False,
                           read_fn=lambda root, t, iv: reads[(t, iv)])
    check(res["series_scanned"] == 2 and res["gap_series"] == 2
          and res["missing_total"] == 3,
          "scan_all_gaps: aggregates missing across series (2 + 1)")
    check(res["summary"]["A 1m"]["missing_total"] == 2
          and res["summary"]["B 1m"]["missing_total"] == 1,
          "scan_all_gaps: per-series summary carries each count")
    sa = sv.scan_series_gaps("x", "A", "1m", read_fn=lambda *_: a)
    sb = sv.scan_series_gaps("x", "B", "1m", read_fn=lambda *_: b)
    rows = sv._gap_report_rows([sa, sb])
    check([r[0] for r in rows] == ["B", "A", "A"],
          "_gap_report_rows: one row per missing bar, EARLIEST time first")
    check(rows[0][2].strftime("%H:%M") == "09:40"
          and rows[1][2].strftime("%H:%M") == "09:35",
          "_gap_report_rows: missing timestamps reconstructed from after+step")


def test_scan_all_gaps_progress():
    d = date(2024, 7, 8)
    reads = {("A", "1m"): day_bars(d, 10, 1.0), ("B", "1m"): day_bars(d, 10, 2.0)}
    seen = []
    sv.scan_all_gaps("ignored", series=[("A", "1m"), ("B", "1m")], write=False,
                     read_fn=lambda r, t, iv: reads[(t, iv)],
                     progress=lambda i, n, t, iv: seen.append((i, n, t, iv)))
    check(seen == [(0, 2, "A", "1m"), (1, 2, "B", "1m")],
          "scan_all_gaps calls progress(i,n,ticker,interval) once per series, in order")


def test_data_gaps_sidecar():
    import shutil
    import tempfile
    base = tempfile.mkdtemp(prefix="sv_gaps_")
    root = str(Path(base) / "Stock Data Storage")
    try:
        check(sv.load_data_gaps(root) == {}, "load_data_gaps: missing -> {}")
        p = sv.record_data_gaps(root, {"AAPL 1m": {"missing_total": 3,
                                                   "gap_events": 1, "days": 1}},
                                asof="T")
        check(p is not None and "_data_gaps.json" in p,
              "record_data_gaps writes the sidecar JSON")
        data = sv.load_data_gaps(root)
        check(data.get("series", {}).get("AAPL 1m", {}).get("missing_total") == 3
              and data.get("asof") == "T"
              and data.get("kind") == sv.gap_evidence.KIND
              and data.get("schema_version") == sv.gap_evidence.SCHEMA_VERSION,
              "load_data_gaps round-trips series + asof + schema")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_gap_evidence_series_scope_policy():
    import shutil
    import tempfile

    gap = sv.gap_evidence
    accepted = ("1m", "1m-iv", "1d-hvol", "1m-bidask")
    rejected = ("1m-pre", "1m-iv-pre", "1d-hvol-post",
                "1m-bidask-pre")
    check(all(gap.is_gap_evidence_interval(token) for token in accepted)
          and not any(gap.is_gap_evidence_interval(token)
                      for token in rejected),
          "gap evidence policy: explicit RTH TRADES/kinds are accepted, "
          "pre/post variants are rejected")
    check(gap.is_primary_price_interval("1m")
          and not any(gap.is_primary_price_interval(token)
                      for token in (*accepted[1:], *rejected)),
          "gap evidence policy: primary discovery remains RTH TRADES-only")

    requested = [
        (" aapl ", "1m"), ("AAPL", "1m"),
        ("aapl", "1m-iv"), ("AAPL", "1d-hvol"),
        ("AAPL", "1m-bidask"),
        *(("AAPL", token) for token in rejected),
    ]
    normalized = gap._normalize_series(requested)
    check(normalized == [
        ("AAPL", "1d-hvol"), ("AAPL", "1m"),
        ("AAPL", "1m-bidask"), ("AAPL", "1m-iv"),
    ], "gap evidence normalization: canonical ticker, sorted dedupe, and "
       "session filtering are preserved")
    repeated = gap._normalize_series(
        [("aapl", "1m-iv")] * (gap.MAX_SERIES + 1))
    check(repeated == [("AAPL", "1m-iv")],
          "gap evidence normalization: MAX_SERIES applies after dedupe")
    try:
        gap._normalize_series([
            (f"T{index:04d}", "1m")
            for index in range(gap.MAX_SERIES + 1)
        ])
    except gap.GapEvidenceError as exc:
        bounded = "too many requested gap-evidence series" in str(exc)
    else:
        bounded = False
    check(bounded, "gap evidence normalization: unique series over MAX_SERIES "
          "fail closed")

    root = tempfile.mkdtemp(prefix="sv_gap_scope_")
    try:
        ticker_dir = Path(root) / "AAPL"
        ticker_dir.mkdir()
        manifest = sv.ss.new_manifest("AAPL", "AAPL")
        for token in (*accepted, *rejected):
            sv.ss.manifest_months(manifest, token)
        sv.ss.save_manifest(ticker_dir, manifest)
        discovered, errors = gap.discover_primary_series(root)
        check(discovered == [("AAPL", "1m")] and errors == [],
              "gap evidence discovery: a mixed manifest discovers only RTH "
              "TRADES series")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_scan_all_gaps_writes_parquet_outermost():
    import shutil
    import tempfile
    base = tempfile.mkdtemp(prefix="sv_gapwr_")
    root = str(Path(base) / "Stock Data Storage")
    Path(root).mkdir(parents=True)
    d = date(2024, 7, 8)
    a = day_bars(d, 20, 10.0); a = a[:5] + a[7:]       # drop 9:35, 9:36
    try:
        res = sv.scan_all_gaps(root, series=[("A", "1m")], write=True,
                               read_fn=lambda *_: a, asof="T")
        check(res.get("path") and Path(res["path"]).name == "data_gaps.parquet"
              and Path(res["path"]).parent == Path(base),
              "scan_all_gaps: parquet written at the OUTERMOST folder (bank parent)")
        check(res.get("sidecar") and Path(res["sidecar"]).exists(),
              "scan_all_gaps: per-series sidecar written inside the bank")
        try:
            import pyarrow.parquet as pq
            tbl = pq.read_table(res["path"])
            check(tbl.num_rows == 2
                  and list(tbl.column_names) == sv._GAP_REPORT_COLS,
                  "gap parquet: one row per missing bar with the typed schema")
        except ImportError:
            pass
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_update_gap_report_merge():
    import shutil
    import tempfile
    base = tempfile.mkdtemp(prefix="sv_gapmrg_")
    root = str(Path(base) / "Stock Data Storage")
    Path(root).mkdir(parents=True)
    d = date(2024, 7, 8)
    A1 = day_bars(d, 20, 10.0); A1 = A1[:5] + A1[7:]       # A: drop 9:35, 9:36
    B = day_bars(d, 20, 20.0); B = B[:10] + B[11:]         # B: drop 9:40
    store = {("A", "1m"): A1, ("B", "1m"): B}
    try:
        sv.scan_all_gaps(root, series=[("A", "1m"), ("B", "1m")], write=True,
                         read_fn=lambda r, t, iv: store[(t, iv)], asof="T1")
        store[("A", "1m")] = (lambda b: b[:8] + b[9:])(day_bars(d, 20, 10.0))
        res = sv.update_gap_report(root, [("A", "1m")],
                                   read_fn=lambda r, t, iv: store[(t, iv)],
                                   asof="T2")
        check(res.get("missing_run") == 1,
              "update_gap_report: re-scans only the touched series (A now 1 miss)")
        rows = sv._read_gap_parquet(Path(base) / sv.GAP_REPORT_NAME)
        a_rows = [r for r in rows if r[0] == "A"]
        b_rows = [r for r in rows if r[0] == "B"]
        check(len(a_rows) == 1 and len(b_rows) == 1,
              "update_gap_report: A's rows REPLACED (2->1), B's rows KEPT")
        miss = [r[2] for r in rows]
        check(miss == sorted(miss),
              "update_gap_report: the merged report stays time-ordered")
        gj = sv.load_data_gaps(root)
        check(gj["series"]["A 1m"]["missing_total"] == 1
              and gj["series"]["B 1m"]["missing_total"] == 1
              and gj["asof"] == "T2",
              "update_gap_report: sidecar merges A, keeps B, restamps asof")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def alt_bars(d, n, base, step_min=2):
    """n bars spaced step_min apart (so each consecutive pair is a 1-missing gap
    for a 1m series) — a wide-row seed for the concurrency test."""
    out = []
    t = datetime.combine(d, time(9, 30))
    for i in range(n):
        p = round(base + i * 0.01, 2)
        out.append((t, p, round(p + 0.05, 2), round(p - 0.05, 2),
                    round(p + 0.02, 2), 100 + i))
        t += timedelta(minutes=step_min)
    return out


def _seed_bank(root, ticker, bars, interval="1m"):
    """Write a real stored series (month files + manifest) for round-trip tests."""
    from collections import defaultdict
    tdir = Path(root) / ticker
    tdir.mkdir(parents=True, exist_ok=True)
    bym = defaultdict(list)
    for b in bars:
        bym[(b[0].year, b[0].month)].append(b)
    man = sv.ss.new_manifest(ticker, ticker)
    months = sv.ss.manifest_months(man, interval)
    for (y, m), bb in bym.items():
        stats = sv.ss.write_month_file(
            sv.ss.month_file_path(root, ticker, y, m, interval), bb)
        months[f"{y}-{m:02d}"] = dict(stats, status="present")
    sv.ss.save_manifest(tdir, man)


def test_gap_evidence_provenance_current_stale_and_race():
    import json as _json
    import shutil
    import tempfile

    base = Path(tempfile.mkdtemp(prefix="sv_gap_prov_"))
    root = base / "Stock Data Storage"
    calendar = {date(2024, 1, day) for day in (2, 3, 4, 5, 8)}
    bars = day_bars(date(2024, 1, 2), 20, 10.0)
    bars += day_bars(date(2024, 1, 8), 20, 11.0)
    try:
        _seed_bank(root, "TMUS", bars)
        produced = sv.scan_all_gaps(
            root, series=[("TMUS", "1m")], write=True,
            calendar_days=calendar, asof="2026-07-13T20:00:00-04:00")
        entry = produced["summary"]["TMUS 1m"]
        check(entry["missing_days"] == 3
              and entry["largest_missing_day_run"] == {
                  "count": 3, "start": "2024-01-03", "end": "2024-01-05"}
              and entry["interval_fingerprint"]["current"] is True,
              "gap evidence: producer pins a current three-session missing run")
        sidecar = sv.load_data_gaps(root)
        valid_sidecar = _json.loads(_json.dumps(sidecar))
        check(sidecar.get("kind") == sv.gap_evidence.KIND
              and sidecar.get("schema_version")
              == sv.gap_evidence.SCHEMA_VERSION,
              "gap evidence: sidecar carries the versioned schema")
        current = sv.evaluate_gap_evidence(root, [("TMUS", "1m")])
        check(current["current_series"] == 1
              and current["fillable"][0]["missing_days"] == 3
              and not current["unavailable"],
              "gap evidence: strict consumer accepts current producer output")

        mixed = _json.loads(_json.dumps(valid_sidecar))
        legacy_entry = _json.loads(_json.dumps(
            mixed["series"]["TMUS 1m"]))
        legacy_entry.pop("interval_fingerprint")
        mixed["series"]["OLD 1m"] = legacy_entry
        mixed_result = sv.gap_evidence.evaluate_payload(
            root, mixed, [("TMUS", "1m"), ("OLD", "1m")])
        legacy_rows = [item for item in mixed_result["unavailable"]
                       if item["ticker"] == "OLD"]
        check(legacy_rows and legacy_rows[0]["state"] == "legacy"
              and "rescanned" in legacy_rows[0]["reason"],
              "mixed v1 sidecar labels untouched fingerprint-less entries legacy")

        invalid_asof = _json.loads(_json.dumps(valid_sidecar))
        invalid_asof["asof"] = "not-a-timestamp"
        invalid_result = sv.gap_evidence.evaluate_payload(
            root, invalid_asof, [("TMUS", "1m")])
        check(invalid_result["unavailable"][0]["state"] == "source_invalid",
              "gap evidence rejects an unparseable schema-v1 as-of timestamp")

        repaired = sorted(bars + day_bars(date(2024, 1, 3), 20, 10.5),
                          key=lambda bar: bar[0])
        stats = sv.ss.write_month_file(
            sv.ss.month_file_path(root, "TMUS", 2024, 1, "1m"), repaired)
        manifest = sv.ss.load_manifest(root / "TMUS")
        sv.ss.manifest_months(manifest, "1m")["2024-01"] = dict(
            stats, status="present")
        sv.ss.save_manifest(root / "TMUS", manifest)
        stale = sv.evaluate_gap_evidence(root, [("TMUS", "1m")])
        check(not stale["rows"] and stale["unavailable"][0]["state"] == "stale",
              "gap evidence: interval rewrite immediately stales cached evidence")

        calls = [0]

        def racing_fingerprint(_root, ticker, interval):
            calls[0] += 1
            return {
                "schema_version": sv.ss.INTERVAL_FINGERPRINT_VERSION,
                "algorithm": "sha256",
                "sha256": ("a" if calls[0] == 1 else "b") * 64,
                "ticker": ticker,
                "interval": interval,
                "present": True,
                "backfill_incomplete": False,
                "month_count": 1,
                "verified_absent_count": 0,
            }

        raced = sv.scan_all_gaps(
            root, series=[("TMUS", "1m")], write=False,
            read_fn=lambda *_args: repaired, calendar_days=calendar,
            fingerprint_fn=racing_fingerprint)
        raced_fp = raced["summary"]["TMUS 1m"]["interval_fingerprint"]
        check(raced_fp["current"] is False
              and "changed during gap scan" in raced_fp["error"],
              "gap evidence: producer race is explicit and non-current")

        original_scanner = sv.scan_series_gaps

        def crashing_scanner(*_args, **_kwargs):
            raise RuntimeError("fixture unexpected scanner crash")

        sv.scan_series_gaps = crashing_scanner
        try:
            crashed = sv.scan_all_gaps(
                root, series=[("TMUS", "1m")], write=False,
                calendar_days=calendar)
        finally:
            sv.scan_series_gaps = original_scanner
        crashed_entry = crashed["summary"]["TMUS 1m"]
        check(crashed["series_failed"] == 1
              and "unexpected scanner crash" in crashed_entry["scan_error"]
              and crashed_entry["interval_fingerprint"]["current"] is False,
              "gap evidence: unexpected scanner failure is published non-current")

        malformed = _json.loads(_json.dumps(valid_sidecar))
        malformed["series"]["TMUS 1m"]["missing_days"] += 1
        (root / sv._GAPS_FILE).write_text(
            _json.dumps(malformed), encoding="utf-8")
        rejected = sv.evaluate_gap_evidence(root, [("TMUS", "1m")])
        check(rejected["unavailable"][0]["state"] == "malformed",
              "gap evidence: inconsistent counts fail closed")

        malformed_runs = _json.loads(_json.dumps(valid_sidecar))
        malformed_runs["series"]["TMUS 1m"]["missing_day_runs"][0][
            "end"] = "2024-01-08"
        malformed_runs["series"]["TMUS 1m"]["largest_missing_day_run"][
            "end"] = "2024-01-08"
        (root / sv._GAPS_FILE).write_text(
            _json.dumps(malformed_runs), encoding="utf-8")
        rejected_runs = sv.evaluate_gap_evidence(root, [("TMUS", "1m")])
        check(rejected_runs["unavailable"][0]["state"] == "malformed",
              "gap evidence: run bounds must partition the missing-date list")

        malformed_provenance = _json.loads(_json.dumps(valid_sidecar))
        malformed_provenance["series"]["TMUS 1m"][
            "interval_fingerprint"]["month_count"] = "one"
        (root / sv._GAPS_FILE).write_text(
            _json.dumps(malformed_provenance), encoding="utf-8")
        rejected_provenance = sv.evaluate_gap_evidence(
            root, [("TMUS", "1m")])
        check(rejected_provenance["unavailable"][0]["state"] == "malformed",
              "gap evidence: malformed provenance counts fail closed")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_gap_rounding_deterministic():
    d = date(2024, 7, 8)
    mk = lambda secs: [(datetime.combine(d, time(9, 30)), 1., 1., 1., 1., 9),
                       (datetime.combine(d, time(9, 30)) + timedelta(seconds=secs),
                        1., 1., 1., 1., 9)]
    # round-half-UP is symmetric + grid-exact: 180s(true 2) -> 2; 150/210 -> 2/3
    check(sv.find_intraday_gaps(mk(180), "1m")["missing_total"] == 2,
          "rounding: an exact 3-step delta is 2 missing")
    check(sv.find_intraday_gaps(mk(150), "1m")["missing_total"] == 2
          and sv.find_intraday_gaps(mk(210), "1m")["missing_total"] == 3,
          "rounding: half-step jitter rounds up symmetrically (no banker's skew)")


def test_gap_mixed_tz_returns_error():
    from zoneinfo import ZoneInfo
    tz = ZoneInfo("America/New_York")
    d = date(2024, 7, 8)
    bars = [(datetime.combine(d, time(9, 30), tzinfo=tz), 1., 1., 1., 1., 9),
            (datetime.combine(d, time(9, 33)), 1., 1., 1., 1., 9)]
    r = sv.find_intraday_gaps(bars, "1m")
    check("error" in r, "mixed tz-aware/naive bars -> error dict, not a crash")


def test_scan_series_gaps_canonical_ticker():
    d = date(2024, 7, 8)
    bars = day_bars(d, 20, 10.0)
    bars = bars[:10] + bars[11:]                 # one interior hole
    res = sv.scan_series_gaps("x", "brk.b", "1m", read_fn=lambda *_: bars)
    check(res.get("ticker") == sv.ss.canonical_ticker("brk.b"),
          "scan_series_gaps tags the CANONICAL ticker (vendor spelling collapses)")


def test_update_gap_report_errored_scan_keeps_rows():
    import shutil
    import tempfile
    base = tempfile.mkdtemp(prefix="sv_gaperr_")
    root = str(Path(base) / "Stock Data Storage")
    Path(root).mkdir(parents=True)
    d = date(2024, 7, 8)
    aaa = day_bars(d, 20, 10.0); aaa = aaa[:5] + aaa[7:]        # 2 missing
    try:
        sv.scan_all_gaps(root, series=[("AAA", "1m")], write=True,
                         read_fn=lambda *_: aaa, asof="T1")
        # re-scan AAA but the read now MISSES (transient empty) -> scan errors
        res = sv.update_gap_report(root, [("AAA", "1m")],
                                   read_fn=lambda *_: [], asof="T2")
        rows = sv._read_gap_parquet(Path(base) / sv.GAP_REPORT_NAME)
        check(res.get("series_scanned") == 0
              and len([r for r in rows if r[0] == "AAA"]) == 2,
              "errored re-scan PRESERVES recorded rows (no silent deletion)")
        gj = sv.load_data_gaps(root)
        entry = gj["series"]["AAA 1m"]
        check(entry.get("scan_error")
              and entry.get("interval_fingerprint", {}).get("current") is False,
              "errored re-scan makes sidecar evidence loudly non-current")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_scan_all_gaps_preserves_unselected_kinds():
    import copy
    import shutil
    import tempfile

    base = Path(tempfile.mkdtemp(prefix="sv_gappreserve_"))
    first_base = Path(tempfile.mkdtemp(prefix="sv_gapfirst_"))
    root = base / "Stock Data Storage"
    first_root = first_base / "Stock Data Storage"
    day = date(2024, 7, 8)
    calendar = {date(2024, 7, d) for d in (8, 9, 10)}
    price = day_bars(day, 20, 10.0)
    price = price[:5] + price[7:]               # two Price rows
    iv = day_bars(day, 20, 0.20)
    iv = iv[:10] + iv[11:]                     # one IV row
    iv_clean = day_bars(day, 20, 0.20)
    hvol = [
        (datetime(2024, 7, 8), 0.0, 0.0, 0.0, 0.0, 0),
        (datetime(2024, 7, 10), 0.25, 0.25, 0.25, 0.25, 0),
    ]
    try:
        _seed_bank(root, "PXR", price, interval="1m")
        _seed_bank(root, "IVR", iv, interval="1m-iv")
        _seed_bank(root, "HVR", hvol, interval="1d-hvol")
        seeded = sv.scan_all_gaps(
            root, write=True, calendar_days=calendar, kinds=None, asof="T1")
        check(not seeded.get("error") and seeded["series_scanned"] == 3,
              "preserve setup writes Price, IV, and HVOL evidence")
        before_side = sv.load_data_gaps(root)["series"]
        price_entry = copy.deepcopy(before_side["PXR 1m"])
        hvol_entry = copy.deepcopy(before_side["HVR 1d-hvol"])
        report_path = base / sv.GAP_REPORT_NAME
        before_rows = sv._read_gap_parquet(report_path)
        price_rows = [row for row in before_rows if row[1] == "1m"]
        iv_rows = [row for row in before_rows if row[1] == "1m-iv"]

        failed = sv.scan_all_gaps(
            root, write=True, calendar_days=calendar, kinds=("iv",),
            preserve_existing=True, read_fn=lambda *_args: [], asof="T2")
        failed_rows = sv._read_gap_parquet(report_path)
        failed_side = sv.load_data_gaps(root)["series"]
        check(not failed.get("error") and iv_rows
              and [row for row in failed_rows if row[1] == "1m-iv"] == iv_rows,
              "failed selected scan retains prior IV parquet evidence")
        check(failed_side["PXR 1m"] == price_entry
              and failed_side["HVR 1d-hvol"] == hvol_entry
              and failed_side["IVR 1m-iv"].get("scan_error")
              and failed_side["IVR 1m-iv"]["interval_fingerprint"][
                  "current"] is False,
              "failed selected scan preserves unselected sidecar entries and "
              "publishes non-current IV evidence")

        cleaned = sv.scan_all_gaps(
            root, write=True, calendar_days=calendar, kinds=("iv",),
            preserve_existing=True,
            read_fn=lambda _root, _ticker, _interval: iv_clean, asof="T3")
        clean_rows = sv._read_gap_parquet(report_path)
        clean_side = sv.load_data_gaps(root)["series"]
        check(not cleaned.get("error")
              and [row for row in clean_rows if row[1] == "1m"] == price_rows
              and not [row for row in clean_rows if row[1] == "1m-iv"],
              "IV-only clean scan replaces IV rows and preserves Price rows")
        check(clean_side["PXR 1m"] == price_entry
              and clean_side["HVR 1d-hvol"] == hvol_entry
              and clean_side["IVR 1m-iv"]["missing_total"] == 0,
              "IV-only clean scan preserves Price/HVOL entries exactly")

        sidecar_path = root / sv._GAPS_FILE
        sidecar_before_corrupt = sidecar_path.read_bytes()
        report_path.write_bytes(b"not a parquet report")
        corrupt_before = report_path.read_bytes()
        refused = sv.scan_all_gaps(
            root, write=True, calendar_days=calendar, kinds=("iv",),
            preserve_existing=True,
            read_fn=lambda _root, _ticker, _interval: iv_clean, asof="T4")
        check("partial replacement" in (refused.get("error") or "")
              and report_path.read_bytes() == corrupt_before
              and sidecar_path.read_bytes() == sidecar_before_corrupt,
              "unreadable prior report fails closed without touching either "
              "gap artifact")

        _seed_bank(first_root, "FIRST", iv, interval="1m-iv")
        first = sv.scan_all_gaps(
            first_root, write=True, calendar_days=calendar, kinds=("iv",),
            preserve_existing=True, asof="FIRST")
        check(not first.get("error") and Path(first["path"]).is_file()
              and Path(first["sidecar"]).is_file(),
              "first selected preserve scan succeeds without prior artifacts")
    finally:
        shutil.rmtree(base, ignore_errors=True)
        shutil.rmtree(first_base, ignore_errors=True)


def test_update_gap_report_corrupt_parquet_rebuilds():
    import io
    import shutil
    import tempfile
    base = tempfile.mkdtemp(prefix="sv_gapcor_")
    root = str(Path(base) / "Stock Data Storage")
    Path(root).mkdir(parents=True)
    d = date(2024, 7, 8)
    fff = day_bars(d, 20, 10.0); fff = fff[:5] + fff[7:]       # 2 missing
    ggg = day_bars(d, 20, 20.0); ggg = ggg[:10] + ggg[11:]     # 1 missing
    _seed_bank(root, "FFF", fff)
    _seed_bank(root, "GGG", ggg)
    try:
        sv.scan_all_gaps(root, write=True, asof="T1")          # real read
        pa, _ = sv.ss._require_pyarrow()
        import pyarrow.parquet as pq
        buf = io.BytesIO()
        pq.write_table(pa.table({"x": [1]}), buf)              # wrong schema
        (Path(base) / sv.GAP_REPORT_NAME).write_bytes(buf.getvalue())
        sv.update_gap_report(root, [("GGG", "1m")], asof="T2")  # -> full rebuild
        names = {r[0] for r in sv._read_gap_parquet(Path(base) / sv.GAP_REPORT_NAME)}
        check("FFF" in names and "GGG" in names,
              "corrupt/old-schema report -> full rebuild keeps every series")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_update_gap_report_rebuild_preserves_kinds():
    import shutil
    import tempfile
    root = tempfile.mkdtemp(prefix="sv_gapkind_")
    old_read = sv._read_gap_parquet
    old_scan = sv.scan_all_gaps
    captured = {}

    def fake_scan(*args, **kwargs):
        captured.update(kwargs)
        return {"summary": {}, "missing_total": 0}

    try:
        sv._read_gap_parquet = lambda _path: None
        sv.scan_all_gaps = fake_scan
        sv.update_gap_report(
            root, [], calendar_days=set(), kinds=("iv", "hvol"))
        check(captured.get("kinds") is None,
              "corrupt-report rebuild scans every kind to avoid evidence loss")
    finally:
        sv._read_gap_parquet = old_read
        sv.scan_all_gaps = old_scan
        shutil.rmtree(root, ignore_errors=True)


def test_update_gap_report_concurrent_no_clobber():
    import shutil
    import tempfile
    import threading
    base = tempfile.mkdtemp(prefix="sv_gapconc_")
    root = str(Path(base) / "Stock Data Storage")
    Path(root).mkdir(parents=True)
    d = date(2024, 7, 8)
    seed = alt_bars(d, 150, 50.0)                # ~149 gap rows -> wide RMW window
    aaa = day_bars(d, 20, 10.0); aaa = aaa[:5] + aaa[7:]
    bbb = day_bars(d, 20, 20.0); bbb = bbb[:10] + bbb[11:]
    store = {("AAA", "1m"): aaa, ("BBB", "1m"): bbb}
    try:
        sv.scan_all_gaps(root, series=[("SEED", "1m")], write=True,
                         read_fn=lambda r, t, iv: seed, asof="T0")
        ok = True
        for _ in range(5):
            barrier = threading.Barrier(2)

            def upd(tic):
                barrier.wait()
                sv.update_gap_report(root, [(tic, "1m")],
                                     read_fn=lambda r, t, iv: store[(t, iv)])
            ths = [threading.Thread(target=upd, args=(x,))
                   for x in ("AAA", "BBB")]
            for th in ths:
                th.start()
            for th in ths:
                th.join()
            names = {r[0] for r in
                     sv._read_gap_parquet(Path(base) / sv.GAP_REPORT_NAME)}
            ok = ok and {"SEED", "AAA", "BBB"} <= names
        check(ok, "concurrent update_gap_report calls never clobber each "
                  "other's rows (parquet lock holds across 5 trials)")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_parse_ibkr_daily_reference():
    bars = [(datetime(2024, 7, 8), 10.0, 11.0, 9.5, 10.5, 1000),
            (datetime(2024, 7, 9), 10.5, 12.0, 10.0, 11.5, 2000)]
    ref = sv.parse_ibkr_daily_reference(bars)
    check(ref.get("2024-07-08") == (10.0, 11.0, 9.5, 10.5, 1000.0)
          and "2024-07-09" in ref,
          "parse_ibkr_daily_reference: tuples -> {iso:(o,h,l,c,v)}")


def test_internal_crosscheck_wires():
    d = date(2024, 7, 8)
    bars = day_bars(d, 60, 50.0)                      # one RTH day of 1m bars
    ref = {d.isoformat(): (bars[0][1], max(b[2] for b in bars),
                           min(b[3] for b in bars), bars[-1][4],
                           sum(b[5] for b in bars))}
    res = sv.internal_crosscheck("x", "AAA", "1m", daily_ref=ref,
                                 read_fn=lambda *a, **k: bars)
    check(isinstance(res, dict) and res.get("source") == "ibkr-daily"
          and res.get("ticker") == "AAA",
          "internal_crosscheck wires stored->derive_daily->compare vs IBKR daily")


def test_crossval_ratio_structural():
    from datetime import datetime as _dt
    r = sv._validate_series_body("x", "AAA", "1m-iv",
                           ref_fn=lambda *a: {}, read_fn=lambda *a: [])
    check(r.get("skipped") is True and not r.get("error"),
          "validate_series skips a ratio kind (1m-iv)")
    bars = [(_dt(2024, 1, 2, 9, 30 + i), 0.2, 0.21, 0.19, 0.2, 100)
            for i in range(3)]
    cv = sv._cross_validate_ticker_body("x", "AAA", "1d-hvol",
                                  read_fn=lambda *_a: bars,
                                  fingerprint_fn=xval_fingerprint)
    check(cv.get("status") == "structural-ok"
          and "no external price reference" in cv.get("note", "")
          and cv.get("provider") == "internal-structural"
          and cv.get("requested_range") is None,
          "cross_validate_ticker structurally checks a ratio kind")
    bad = list(bars)
    bad[1] = (_dt(2024, 1, 2, 9, 31), 0.2, 9.0, 0.19, 0.2, 100)
    cv_bad = sv._cross_validate_ticker_body("x", "AAA", "1m-iv",
                                      read_fn=lambda *_a: bad,
                                      fingerprint_fn=xval_fingerprint)
    check(cv_bad.get("status") == "structural-flag"
          and cv_bad.get("detail", {}).get("flagged_count") == 1
          and cv_bad.get("reference_interval") == "1d-iv"
          and cv_bad.get("reference_interval_fingerprint", {}).get("current")
          is True,
          "cross_validate_ticker flags absurd ratio values with dual provenance")
    daily_shas = iter(("b" * 64, "c" * 64))

    def ratio_race_fp(root, ticker, interval):
        sha = next(daily_shas) if interval == "1d-iv" else "a" * 64
        return xval_fingerprint(root, ticker, interval, sha)

    cv_race = sv._cross_validate_ticker_body(
        "x", "AAA", "1m-iv", read_fn=lambda *_a: bad,
        fingerprint_fn=ratio_race_fp)
    reference_fp = cv_race.get("reference_interval_fingerprint") or {}
    check(cv_race.get("status") == "inconclusive"
          and reference_fp.get("current") is False
          and reference_fp.get("observed_status") == "structural-flag",
          "stored-daily ratio mutation makes structural evidence non-current")
    cv2 = sv._cross_validate_ticker_body("x", "AAA", "1m",
                                   ref_fn=lambda *a: {}, read_fn=lambda *a: [],
                                   fingerprint_fn=xval_fingerprint)
    check(cv2.get("status") != "skipped",
          "a TRADES interval is NOT skipped")


def test_internal_daily_audit():
    from datetime import datetime as _dt, date as _date

    def _day(d, base, close_last):
        return [(_dt(d.year, d.month, d.day, 9, 30), base, base + 1, base - 1,
                 base, 100),
                (_dt(d.year, d.month, d.day, 9, 31), base, base + 1, base - 1,
                 base, 100),
                (_dt(d.year, d.month, d.day, 15, 59), base, base + 1, base - 1,
                 close_last, 100)]
    d1, d2 = _date(2024, 6, 3), _date(2024, 6, 4)
    bars = _day(d1, 100.0, 100.2) + _day(d2, 101.0, 101.3)
    # d1: O/H/L exact, close 100.2 vs 100.25 = 0.05% -> OK; d2: HIGH 102 vs 105 -> FLAG
    ref = {str(d1): (100.0, 101.0, 99.0, 100.25, 300.0),
           str(d2): (101.0, 110.0, 100.0, 101.30, 450.0)}   # high 7%>3%, vol 50%>0.5%
    cand = sv.audit_internal_daily(sv.derive_daily(bars), ref)
    check(d2 in cand and "high" in cand[d2] and "volume" in cand[d2]
          and d1 not in cand,
          "audit flags a >3% price AND a volume drop, not the clean day")
    res = sv.internal_daily_audit("x", "AAA", "1m", daily_ref=ref,
                                  read_fn=lambda *a: bars, refetch=None)
    check(res["status"] == "disagree" and str(d2) in res["confirmed"],
          "internal_daily_audit: no refetch -> confirmed disagreement")

    def _refetch(d):                        # the 'fixed' fetch now agrees
        good = {d: (101.0, 110.0, 100.0, 101.30, 450.0)}
        return good, {d: (101.0, 110.0, 100.0, 101.30, 450.0)}
    res2 = sv.internal_daily_audit("x", "AAA", "1m", daily_ref=ref,
                                   read_fn=lambda *a: bars, refetch=_refetch)
    check(res2["status"] == "ok" and str(d2) in res2["transient"],
          "internal_daily_audit: refetch clears the date -> transient, ok")
    cand2 = sv.audit_internal_daily(
        sv.derive_daily(_day(d1, 100.0, 100.2)),
        {str(d1): (101.5, 101.0, 99.0, 102.0, 300.0)})   # open 1.5% + close 1.8% gaps
    check(not cand2, "open/close auction gaps < 3% do NOT flag (volume matched)")


def test_combined_double_flag():
    from datetime import date as _date, timedelta as _td
    days, d = [], _date(2024, 1, 1)
    while len(days) < 25:
        if d.weekday() < 5:
            days.append(d)
        d += _td(days=1)
    base = (100.0, 101.0, 99.0, 100.5, 1000.0)
    off = (110.0, 111.0, 109.0, 110.5, 1000.0)        # +10% (ext) / >3% (int)
    derived = {dd: base for dd in days}
    X, Y, Z = days[10], days[15], days[20]
    ext = {dd.isoformat(): base for dd in days}
    ext[X.isoformat()] = off                          # external flags X
    ext[Y.isoformat()] = off                          # external flags Y (ext-only)
    intl = {dd: base for dd in days}
    intl[X] = off                                     # internal flags X
    intl[Z] = off                                     # internal flags Z (int-only)
    combined = sv.combined_double_flag(derived, ext, intl)
    check(combined == [X.isoformat()],
          "double-flag = external ∩ internal (X only; Y ext-only, Z int-only out)")
    check(not sv.combined_double_flag(derived, ext, {dd: base for dd in days}),
          "no double-flag when internal agrees everywhere")
    # combined_crosscheck refetch SPLICE: an empty/partial refetch must NOT clear a
    # real candidate (the stored day's value is kept -> stays persistent); a refetch
    # that actually fixes the month clears it.
    from datetime import datetime as _dt
    bars = [(_dt(dd.year, dd.month, dd.day, 9, 30), *derived[dd]) for dd in days]
    res_empty = sv.combined_crosscheck(
        "x", "AAA", "1m", ext_ref=ext, int_ref=intl,
        read_fn=lambda *a: bars, refetch_month=lambda y, m: ({}, {}, {}))
    check(res_empty["persistent"] == [X.isoformat()] and not res_empty["cleared"],
          "combined_crosscheck: empty refetch KEEPS the candidate persistent")

    def _rf_fix(y, m):
        md = {dd: base for dd in days if dd.year == y and dd.month == m}
        return md, {dd.isoformat(): base for dd in md}, {dd: base for dd in md}
    res_fix = sv.combined_crosscheck(
        "x", "AAA", "1m", ext_ref=ext, int_ref=intl,
        read_fn=lambda *a: bars, refetch_month=_rf_fix)
    check(res_fix["cleared"] == [X.isoformat()] and not res_fix["persistent"],
          "combined_crosscheck: a fixing refetch CLEARS the candidate")


def test_combined_crosscheck_provenance():
    day = "2026-06-18"
    def coverage(requested_range="Max", head_ok=True):
        common = {
            "first_date": "2026-06-01", "last_date": "2026-06-30",
            "day_count": 20,
            "derived_first_date": "2026-06-01",
            "derived_last_date": "2026-06-30", "derived_day_count": 20,
            "head_gap_days": 0, "head_tolerance_days": 7,
            "head_ok": head_ok,
        }
        return {
            "external": {**common, "requested_range": requested_range},
            "internal": {**common, "requested_range": None,
                         "head_ok": True},
        }

    flagged = {"ticker": "AAA", "interval": "1m", "source": "3-source",
               "status": "flagged", "candidates": [day],
               "persistent": [day], "cleared": [],
               "reference_coverage": coverage()}
    stable = sv.combined_crosscheck_with_provenance(
        "root", "aaa", "1m", lambda: flagged,
        asof="2026-07-13T12:05:00-04:00",
        fingerprint_fn=xval_fingerprint)
    fp = stable.get("interval_fingerprint") or {}
    ifp = stable.get("internal_interval_fingerprint") or {}
    try:
        aware_times = (datetime.fromisoformat(stable["started_at"]).utcoffset()
                       is not None
                       and datetime.fromisoformat(stable["finished_at"]).utcoffset()
                       is not None)
    except (KeyError, TypeError, ValueError):
        aware_times = False
    check(stable.get("schema_version") == sv.COMBINED_FLAGS_SCHEMA_VERSION
          and stable.get("ticker") == "AAA" and stable.get("status") == "flagged"
          and aware_times and fp.get("current") is True
          and stable.get("internal_interval") == sv.COMBINED_INTERNAL_INTERVAL
          and ifp.get("current") is True
          and fp.get("sha256")
          == fp.get("before_sha256") == fp.get("after_sha256") == "a" * 64
          and ifp.get("sha256") == ifp.get("before_sha256")
          == ifp.get("after_sha256") == "a" * 64,
          f"combined provenance: stable producer envelope is current ({stable})")

    missing_coverage = sv.combined_crosscheck_with_provenance(
        "root", "AAA", "1m", lambda: {
            "status": "flagged", "candidates": [day],
            "persistent": [day], "cleared": [],
        }, fingerprint_fn=xval_fingerprint)
    check(missing_coverage.get("status") == "error"
          and not missing_coverage.get("persistent"),
          "combined provenance: flagged result without coverage fails closed")

    bounded_range = sv.combined_crosscheck_with_provenance(
        "root", "AAA", "1m", lambda: {
            "status": "inconclusive", "candidates": [],
            "persistent": [], "cleared": [],
            "reference_coverage": coverage("5Y", None),
        }, fingerprint_fn=xval_fingerprint)
    check(bounded_range.get("status") == "inconclusive"
          and bounded_range.get("reference_coverage", {}).get(
              "external", {}).get("head_ok") is None,
          "combined provenance: non-Max inconclusive coverage is preserved")

    fingerprints = [xval_fingerprint(None, "AAA", "1m", "b" * 64),
                    xval_fingerprint(None, "AAA", "1d", "d" * 64),
                    xval_fingerprint(None, "AAA", "1m", "c" * 64),
                    xval_fingerprint(None, "AAA", "1d", "d" * 64)]
    racing = sv.combined_crosscheck_with_provenance(
        "root", "AAA", "1m", lambda: flagged,
        fingerprint_fn=lambda *_args: fingerprints.pop(0))
    rfp = racing.get("interval_fingerprint") or {}
    check(racing.get("status") == "inconclusive"
          and racing.get("persistent") == [day]
          and rfp.get("current") is False
          and rfp.get("observed_status") == "flagged"
          and "changed" in rfp.get("error", ""),
          f"combined provenance: interval race cannot publish a flag ({racing})")

    internal_shas = iter(("d" * 64, "e" * 64))

    def internal_race_fp(root, ticker, interval):
        sha = "a" * 64 if interval == "1m" else next(internal_shas)
        return xval_fingerprint(root, ticker, interval, sha)

    internal_race = sv.combined_crosscheck_with_provenance(
        "root", "AAA", "1m", lambda: flagged,
        fingerprint_fn=internal_race_fp)
    ir_primary = internal_race.get("interval_fingerprint") or {}
    ir_internal = internal_race.get("internal_interval_fingerprint") or {}
    check(internal_race.get("status") == "inconclusive"
          and ir_primary.get("current") is True
          and ir_internal.get("current") is False
          and ir_internal.get("observed_status") == "flagged"
          and "internal daily interval changed" in ir_internal.get("error", ""),
          f"combined provenance: stored-daily race cannot publish a flag "
          f"({internal_race})")

    broken = sv.combined_crosscheck_with_provenance(
        "root", "AAA", "1m", lambda: {**flagged, "status": "ok",
                                        "persistent": []},
        fingerprint_fn=lambda *_args: (_ for _ in ()).throw(
            RuntimeError("bad manifest " + "x" * 500)))
    bfp = broken.get("interval_fingerprint") or {}
    check(broken.get("status") == "inconclusive"
          and bfp.get("observed_status") == "ok"
          and len(bfp.get("error", "")) <= sv._XVAL_ERROR_CAP,
          f"combined provenance: unreadable state fails closed and bounded ({broken})")

    failed = sv.combined_crosscheck_with_provenance(
        "root", "AAA", "1m",
        lambda: (_ for _ in ()).throw(RuntimeError("source offline")),
        fingerprint_fn=xval_fingerprint)
    check(failed.get("status") == "error"
          and failed.get("candidates") == []
          and failed.get("interval_fingerprint", {}).get("current") is True
          and "source offline" in failed.get("note", ""),
          f"combined provenance: source failure supersedes old evidence ({failed})")

    many_days = [
        (date(2024, 1, 1) + timedelta(days=index)).isoformat()
        for index in range(sv._XVAL_ROW_CAP + 25)
    ]
    bounded = sv.combined_crosscheck_with_provenance(
        "root", "AAA", "1m", lambda: {
            "status": "flagged", "candidates": many_days,
            "persistent": many_days, "cleared": [],
            "reference_coverage": coverage(),
            "unexpected_blob": "x" * 1_000_000,
        }, fingerprint_fn=xval_fingerprint)
    check(bounded.get("persistent_count") == len(many_days)
          and len(bounded.get("persistent") or []) == sv._XVAL_ROW_CAP
          and bounded.get("truncated") is True
          and "unexpected_blob" not in bounded
          and len(json.dumps(bounded)) < 20_000,
          "combined producer whitelists and bounds mass discrepancies")

    import tempfile
    import shutil
    root = Path(tempfile.mkdtemp(prefix="combined_keys_"))
    try:
        second = sv.combined_crosscheck_with_provenance(
            "root", "AAA", "5m", lambda: {
                "status": "ok", "candidates": [],
                "persistent": [], "cleared": [],
                "reference_coverage": coverage(),
            }, fingerprint_fn=xval_fingerprint)
        sv.record_combined_flags(root, bounded)
        sv.record_combined_flags(root, second)
        stored = sv.load_combined_flags(root)
        check(set(stored) == {"AAA 1m", "AAA 5m"}
              and len(sv.combined_entries_for_ticker(stored, "AAA")) == 2,
              "combined persistence keeps simultaneous 1m and 5m verdicts")
        stored["AAA"] = {"schema_version": 1, "ticker": "AAA",
                         "interval": "1m"}
        check(len(sv.combined_entries_for_ticker(
                  stored, "AAA",
                  schema_version=sv.COMBINED_FLAGS_SCHEMA_VERSION)) == 2,
              "combined catch-up accounting ignores legacy-schema rows")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_connected_pattern_gaps():
    from datetime import datetime as _dt, date as _date

    def _daily(days):
        return [(_dt(d.year, d.month, d.day), 1.0, 1.0, 1.0, 1.0, 100)
                for d in days]
    cal = {_date(2024, 1, d) for d in (2, 3, 4, 5, 8)}     # trading days
    series = _daily([_date(2024, 1, d) for d in (2, 3, 5, 8)])  # 4th absent
    md = sv.find_missing_days(series, cal, "1d")
    check(md["missing_days"] == ["2024-01-04"],
          "find_missing_days: flags a calendar day absent WITHIN the span")
    # a calendar day BEFORE the series' first (the 1st) is never flagged
    md2 = sv.find_missing_days(series, cal | {_date(2024, 1, 1)}, "1d")
    check(md2["missing_days"] == ["2024-01-04"],
          "find_missing_days: out-of-span calendar days are NOT flagged")
    # a fully-covered series -> no missing days
    full = _daily([_date(2024, 1, d) for d in (2, 3, 4, 5, 8)])
    check(not sv.find_missing_days(full, cal, "1d")["missing_days"],
          "find_missing_days: complete series flags nothing")
    # empty / no-calendar are safe
    check(sv.find_missing_days([], cal)["missing_days"] == []
          and sv.find_missing_days(series, None)["missing_days"] == [],
          "find_missing_days: empty bars / no calendar -> no flags, no crash")


def test_source_absent_split():
    import tempfile
    import shutil
    import stock_storage as _ss
    from datetime import datetime as _dt, date as _date
    from pathlib import Path as _P
    root = tempfile.mkdtemp()
    try:
        canon = _ss.canonical_ticker("ZZ")
        (_P(root) / canon).mkdir(parents=True, exist_ok=True)
        man = _ss.new_manifest("ZZ", "ZZ")
        _ss.manifest_months(man, "1m")["2024-01"] = {"status": "present", "rows": 3}
        man["intervals"]["1m"]["verified_absent"] = ["2024-01-04"]
        _ss.save_manifest(_P(root) / canon, man)
        check(sv.verified_absent_days(root, "ZZ", "1m") == {"2024-01-04"},
              "verified_absent_days: reads the manifest's source-absent set")
        bars = [(_dt(2024, 1, d, 9, 30), 1.0, 1.0, 1.0, 1.0, 100) for d in (2, 3, 5)]
        cal = {_date(2024, 1, d) for d in (2, 3, 4, 5)}       # Jan 4 absent
        res = sv.scan_series_gaps(root, "ZZ", "1m",
                                  read_fn=lambda *_a: bars, calendar_days=cal)
        check(res.get("source_absent") == ["2024-01-04"],
              "scan_series_gaps: a verified_absent missing day -> source_absent")
        check(res.get("missing_days") == [] and res.get("source_absent_count") == 1,
              "scan_series_gaps: a source-absent day is NOT a fillable gap")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_source_absent_reprobe_budget_shared_across_scan():
    import json
    import shutil
    import tempfile
    import stock_storage as _ss
    from datetime import datetime as _dt, date as _date, timedelta as _delta
    from pathlib import Path as _P

    root = tempfile.mkdtemp()
    try:
        missing = [f"2024-01-{day:02d}" for day in range(3, 10)]
        old = (_dt.now() - _delta(days=120)).isoformat(timespec="seconds")
        for ticker in ("AA", "BB"):
            man = _ss.new_manifest(ticker, ticker)
            sec = man["intervals"].setdefault("1d", {"months": {}})
            sec["verified_absent"] = list(missing)
            sec["verified_absent_evidence"] = {
                day: {"method": "empty_month_control",
                      "control": "2023-12", "at": old}
                for day in missing
            }
            _ss.save_manifest(_P(root) / ticker, man)
        (_P(root) / "_absence_expiry.json").write_text(json.dumps({
            "weak_days": 30, "strong_days": 365,
            "reprobe_budget_per_run": 3,
        }), encoding="utf-8")
        bars = [(_dt(2024, 1, day, 9, 30), 1.0, 1.0, 1.0, 1.0, 100)
                for day in (2, 10)]
        calendar = {_date(2024, 1, day) for day in range(2, 11)}
        result = sv.scan_all_gaps(
            root, series=[("AA", "1d"), ("BB", "1d")], write=False,
            read_fn=lambda *_a: bars, calendar_days=calendar)
        summary = result.get("summary") or {}
        due = sum(len(row.get("missing_day_list") or [])
                  for row in summary.values())
        visible = sum(len(row.get("source_absent_list") or [])
                      for row in summary.values())
        check(due == 3 and visible == 14,
              "scan_all_gaps: one shared run budget releases 3/14 expired "
              "source-absent days while all 14 stay visible")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_known_deliberate_absence_policy_is_exact_and_fail_closed():
    import copy
    import shutil
    import tempfile
    import stock_storage as _ss
    from datetime import datetime as _dt, date as _date, timedelta as _delta
    from pathlib import Path as _P

    root = tempfile.mkdtemp()
    policy_path = _P(root) / "_absence_expiry.json"
    old = (_dt.now() - _delta(days=120)).isoformat(timespec="seconds")
    stored = ["2024-01-02", "2024-01-10"]
    calendar = {_date.fromisoformat(day) for day in
                stored + ["2024-01-04", "2024-01-08"]}
    bars = [(_dt.fromisoformat(f"{day}T09:30:00"),
             1.0, 1.0, 1.0, 1.0, 100) for day in stored]

    def seed(ticker, absent):
        man = _ss.new_manifest(ticker, ticker)
        sec = man["intervals"].setdefault("1d", {"months": {}})
        sec["verified_absent"] = list(absent)
        sec["verified_absent_evidence"] = {
            day: {"method": "empty_month_control",
                  "control": "2023-12", "at": old}
            for day in absent
        }
        _ss.save_manifest(_P(root) / ticker, man)

    def pin(ticker, start, end):
        return {
            "ticker": ticker,
            "interval": "1d",
            "start": start,
            "end": end,
            "disposition": "do_not_refetch",
            "interval_fingerprint": _ss.interval_state_fingerprint(
                root, ticker, "1d"),
            "review": {
                "checkpoint": "647ae47",
                "approval_commit": "143af70",
                "approved_by": "CLAUDE",
                "approved_at": "2026-07-31",
            },
        }

    def write_policy(entries, **extra):
        payload = {
            "version": 1,
            "weak_days": 30,
            "strong_days": 365,
            "reprobe_budget_per_run": 1,
            "known_deliberate": entries,
        }
        payload.update(extra)
        policy_path.write_text(json.dumps(payload), encoding="utf-8")

    def scan(ticker="AA", cal=None, budget=None):
        return sv.scan_series_gaps(
            root, ticker, "1d", read_fn=lambda *_a: bars,
            calendar_days=calendar if cal is None else cal,
            absence_reprobe_budget=budget)

    try:
        seed("AA", ["2024-01-04", "2024-01-08"])
        man = _ss.load_manifest(_P(root) / "AA")
        del man["intervals"]["1d"]["verified_absent_evidence"]["2024-01-04"]
        _ss.save_manifest(_P(root) / "AA", man)
        exact = pin("AA", "2024-01-04", "2024-01-04")
        write_policy([exact])
        manifest_path = _P(root) / "AA" / _ss.MANIFEST_NAME
        manifest_before = manifest_path.read_bytes()
        result = scan()
        check(result.get("source_absent")
              == ["2024-01-04", "2024-01-08"],
              "known_deliberate: exact pin keeps every absent day visible")
        check(result.get("missing_days") == ["2024-01-08"],
              "known_deliberate: only the exact pinned range is subtracted")
        check((result.get("source_absence_policy") or {}).get("status")
              == "applied"
              and (result.get("source_absence_policy") or {}).get(
                  "pinned_count") == 1,
              "known_deliberate: an applied pin is visible and counted")
        check(manifest_path.read_bytes() == manifest_before,
              "known_deliberate: a pinned legacy row keeps original evidence")

        real_absent_state = sv._verified_absent_state
        absent_reads = [0]

        def drifting_absent_state(*args, **kwargs):
            absent_reads[0] += 1
            absent, evidence = real_absent_state(*args, **kwargs)
            if absent_reads[0] == 2:
                absent = set(absent) - {"2024-01-08"}
            return absent, evidence

        sv._verified_absent_state = drifting_absent_state
        try:
            drift = scan()
        finally:
            sv._verified_absent_state = real_absent_state
        check("changed during policy evaluation" in drift.get("error", "")
              and drift.get("missing_days") == [],
              "known_deliberate: a mixed absence snapshot halts repair")

        man = _ss.load_manifest(_P(root) / "AA")
        man["intervals"]["1d"]["backfill_incomplete"] = True
        _ss.save_manifest(_P(root) / "AA", man)
        stale = scan()
        check("policy halted automated repair" in stale.get("error", "")
              and stale.get("missing_days") == [],
              "known_deliberate: stale fingerprint halts all series repair")
        check(stale.get("source_absent")
              == ["2024-01-04", "2024-01-08"],
              "known_deliberate: stale policy does not hide source absence")

        man["intervals"]["1d"]["backfill_incomplete"] = False
        _ss.save_manifest(_P(root) / "AA", man)
        exact = pin("AA", "2024-01-04", "2024-01-04")
        malformed = copy.deepcopy(exact)
        malformed["interval_fingerprint"] = "not-a-fingerprint"
        write_policy([malformed])
        bad = scan()
        check("policy halted automated repair" in bad.get("error", "")
              and bad.get("missing_days") == [],
              "known_deliberate: an identifiable malformed pin halts its series")

        new_gap_calendar = {_date.fromisoformat(day) for day in
                            stored + ["2024-01-06"]}
        write_policy([exact])
        exact_new_gap = scan(cal=new_gap_calendar)
        check(exact_new_gap.get("missing_days") == ["2024-01-06"]
              and not exact_new_gap.get("error"),
              "known_deliberate: a valid pin does not silence a new outside gap")
        write_policy([malformed])
        bad_new_gap = scan(cal=new_gap_calendar)
        check("policy halted automated repair" in bad_new_gap.get("error", "")
              and bad_new_gap.get("missing_days") == [],
              "known_deliberate: an invalid pin also halts new outside gaps")

        contradictory = pin("AA", "2024-01-06", "2024-01-06")
        write_policy([contradictory])
        conflict = scan()
        check("contradicts current absence state" in conflict.get("error", "")
              and conflict.get("missing_days") == [],
              "known_deliberate: a non-overlapping range fails closed")

        exact = pin("AA", "2024-01-04", "2024-01-08")
        write_policy([exact, copy.deepcopy(exact)])
        duplicate = scan()
        check("multiple known_deliberate pins" in duplicate.get("error", "")
              and duplicate.get("missing_days") == [],
              "known_deliberate: duplicate series pins fail closed")

        seed("BB", ["2024-01-04"])
        write_policy([exact])
        unrelated = scan(
            "BB", cal={_date.fromisoformat(day) for day in
                       stored + ["2024-01-04"]})
        check(unrelated.get("missing_days") == ["2024-01-04"]
              and not unrelated.get("error"),
              "known_deliberate: a pin never silences an unrelated series")

        exact = pin("AA", "2024-01-04", "2024-01-08")
        write_policy([exact])
        shared = sv.scan_all_gaps(
            root, series=[("AA", "1d"), ("BB", "1d")], write=False,
            read_fn=lambda *_a: bars,
            calendar_days={_date.fromisoformat(day) for day in
                           stored + ["2024-01-04", "2024-01-08"]})
        summary = shared.get("summary") or {}
        check((summary.get("AA 1d") or {}).get("missing_days") == 0
              and (summary.get("BB 1d") or {}).get("missing_days") == 2
              and "2024-01-04" in (
                  (summary.get("BB 1d") or {}).get("missing_day_list") or []),
              "known_deliberate: pinned rows consume none of the shared budget")

        policy_path.write_text("{", encoding="utf-8")
        global_bad = scan(
            "BB", cal={_date.fromisoformat(day) for day in
                       stored + ["2024-01-04"]})
        check("policy halted automated repair" in global_bad.get("error", "")
              and global_bad.get("missing_days") == [],
              "known_deliberate: unreadable global policy can never release work")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_tombstone_gap_scan_resilience():
    import tempfile
    import shutil
    import stock_storage as _ss
    from datetime import datetime as _dt, date as _date
    from pathlib import Path as _P

    root = tempfile.mkdtemp()
    try:
        def put_month(ticker, interval, y, m, days):
            bars = [(_dt(d.year, d.month, d.day, 9, 30),
                     1.0, 1.0, 1.0, 1.0, 100) for d in days]
            stats = _ss.write_month_file(
                _ss.month_file_path(root, ticker, y, m, interval), bars)
            man = _ss.load_manifest(_P(root) / ticker) or _ss.new_manifest(ticker, ticker)
            _ss.manifest_months(man, interval)[f"{y:04d}-{m:02d}"] = dict(
                stats, status="present")
            _ss.save_manifest(_P(root) / ticker, man)

        put_month("BAD", "1m", 2023, 12, [_date(2023, 12, 29)])
        put_month("BAD", "1m", 2024, 2, [_date(2024, 2, 2)])
        man = _ss.load_manifest(_P(root) / "BAD")
        _ss.manifest_months(man, "1m")["2024-01"] = {"status": "MISSING"}
        _ss.manifest_months(man, "1d")["2024-01"] = {"status": "MISSING"}
        _ss.save_manifest(_P(root) / "BAD", man)

        cal = {_date(2024, 1, d) for d in (2, 3, 4)}
        res = sv.scan_series_gaps(root, "BAD", "1m", calendar_days=cal)
        check(not res.get("error") and set(res.get("missing_days", [])) >=
              {"2024-01-02", "2024-01-03", "2024-01-04"},
              "scan_series_gaps: MISSING tombstone month becomes visible missing days")
        check(len(sv.read_series(root, "BAD", "1m")) == 2
              and len(sv.read_series_ts(root, "BAD", "1m")) == 2,
              "read_series/read_series_ts: MISSING tombstones are skipped, not opened")

        put_month("CAL1", "1d", 2024, 1, [_date(2024, 1, 2), _date(2024, 1, 3)])
        put_month("CAL2", "1d", 2024, 1, [_date(2024, 1, 2), _date(2024, 1, 3)])
        cc = sv.consensus_calendar(root, min_tickers=2)
        check({_date(2024, 1, 2), _date(2024, 1, 3)} <= cc,
              "consensus_calendar: tombstoned ticker does not abort calendar build")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def main():
    for t in [v for k, v in sorted(globals().items())
              if k.startswith("test_")]:
        t()
    total = _PASS[0] + _FAIL[0]
    print(f"\nstock_validate_selftest: {_PASS[0]}/{total} passed, "
          f"{_FAIL[0]} failed")
    return 1 if _FAIL[0] else 0


if __name__ == "__main__":
    sys.exit(main())
