"""Self-tests for extended-hours collection (pre-market / after-hours stored in
SEPARATE files from regular hours, keyed by a -pre / -post interval-token
suffix). No TWS / no network: a fake adapter returns all-hours bars and the
engine keeps only the requested session's, validated against that session's
window.   python engine/extended_hours_selftest.py
"""
import sys
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_ibkr as sk
import stock_storage as ss

NY = sk._ny()
_PASS = [0]
_FAIL = [0]


def check(cond, name):
    if cond:
        _PASS[0] += 1
    else:
        _FAIL[0] += 1
        print("  FAIL:", name)


def stub(dt_et, price, vol=100):
    """IBKR-style bar stub (aware-UTC dt, float volume) at a naive-ET time."""
    return SimpleNamespace(
        date=dt_et.replace(tzinfo=NY).astimezone(timezone.utc),
        open=price, high=price + 0.05, low=price - 0.05,
        close=price + 0.01, volume=float(vol))


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def time(self):
        return self.t

    def sleep(self, seconds, cancel=None):
        self.t += seconds


class ExtAdapter:
    """Returns all-hours bars from self.days regardless of use_rth (the engine
    does the session filtering); records the use_rth it was driven with."""

    def __init__(self, days, head=None):
        self.days = days
        self.head = head
        self.use_rth = True
        self.port = 7497
        self.fetch_calls = []

    def account(self):
        return "DUFAKE"

    def qualify(self, symbol):
        return 111, SimpleNamespace(symbol=symbol)

    def contract_for(self, conid):
        return SimpleNamespace(symbol="?", conId=conid)

    def qualify_many(self, symbols, chunk=50, progress=None):
        return {s: 111 for s in symbols}

    def head_timestamp(self, contract):
        return self.head

    def is_connected(self):
        return True

    def reconnect(self):
        pass

    def disconnect(self):
        pass

    def fetch(self, contract, end_dt, duration, bar_size):
        self.fetch_calls.append((end_dt, duration, bar_size, self.use_rth))
        end_d = end_dt.date()
        if duration.endswith("S"):
            return list(self.days.get(end_d, []))
        n, unit = duration.split()
        span = {"D": 1, "W": 7, "M": 31}[unit] * int(n)
        start_d = end_d - timedelta(days=span - 1)
        out = []
        for d in sorted(self.days):
            if start_d <= d <= end_d:
                out.extend(self.days[d])
        return out


def run(root, selections, adapter, today, since=None, progress=None):
    sk.RECONNECT_BACKOFF_S = 0.0
    clk = FakeClock()
    return sk.nightly_gap_fill(root, selections, progress=progress, cancel=None,
                       adapter_factory=lambda: adapter,
                       pacer=sk.Pacer(time_fn=clk.time, sleep_fn=clk.sleep),
                       today=today, since=since)


def fresh_root():
    return Path(tempfile.mkdtemp(prefix="ext_")) / ss.STORAGE_DIR_NAME


# --- A. pure session helpers -------------------------------------------------

def test_session_helpers():
    check(ss.session_of("1m") == "rth", "session_of rth")
    check(ss.session_of("1m-pre") == "pre", "session_of pre")
    check(ss.session_of("1m-post") == "post", "session_of post")
    check(ss.base_interval("1m-post") == "1m"
          and ss.base_interval("1s-pre") == "1s", "base_interval strips suffix")
    check(ss.base_interval("1m") == "1m", "base_interval no-op for rth")
    check(ss.with_session("1m", "post") == "1m-post"
          and ss.with_session("1m", "rth") == "1m", "with_session")
    check(ss.session_window("1m-pre") == (time(4, 0), time(9, 29, 59)),
          "pre window")
    check(ss.session_window("1m-post") == (time(16, 0), time(19, 59, 59)),
          "post window")
    check(ss.session_window("1m") == (time(9, 30), time(15, 59, 59)),
          "rth window")
    check(bool(ss.INTERVAL_RE.match("1m-post"))
          and bool(ss.INTERVAL_RE.match("1s-pre")),
          "INTERVAL_RE accepts -pre/-post")
    check(not ss.INTERVAL_RE.match("1m-mid"), "INTERVAL_RE rejects other suffix")


def test_engine_session_spec():
    check(sk._session_spec("1m")[0] is True, "rth use_rth True")
    check(sk._session_spec("1m-pre")[0] is False
          and sk._session_spec("1m-post")[0] is False, "ext use_rth False")
    check(sk._expected_session_bars("1m") == 390, "expected rth 1m = 390")
    check(sk._expected_session_bars("1m-pre") == 330, "expected pre 1m = 330")
    check(sk._expected_session_bars("1m-post") == 240, "expected post 1m = 240")
    check(sk._expected_session_bars("1s-post") == 14400, "expected post 1s")


def test_request_windows():
    day = date(2024, 6, 17)
    check(sk.day_requests("1m-post", day) == [(datetime(2024, 6, 17, 20, 0),
                                               "1 D")],
          "1m-post: one '1 D' request ending 20:00")
    check(sk.day_requests("1m-pre", day) == [(datetime(2024, 6, 17, 9, 30),
                                              "1 D")],
          "1m-pre: one '1 D' request ending 09:30")
    reqs = sk.day_requests("1s-post", day)
    check(reqs[0][0] == datetime(2024, 6, 17, 16, 30)
          and reqs[-1][0] == datetime(2024, 6, 17, 20, 0),
          "1s-post: intra-day windows 16:00->20:00")
    sc = sk.span_chunks("1m-post", [day])
    check(sc[0][0] == datetime(2024, 6, 17, 20, 0),
          "span_chunks post ends at 20:00")


# --- B. split keeps only the requested session -------------------------------

def test_split_filters_session():
    day = date(2024, 6, 17)
    raw = [stub(datetime.combine(day, time(8, 0)), 100),    # pre
           stub(datetime.combine(day, time(10, 0)), 101),   # rth
           stub(datetime.combine(day, time(17, 0)), 102)]   # post
    vd = frozenset((day,))
    for iv, keep_t in (("1m", time(10, 0)), ("1m-pre", time(8, 0)),
                       ("1m-post", time(17, 0))):
        ctr = {"non_rth": 0, "invalid": 0, "outside_day": 0}
        bars = sk.split_session_bars(raw, vd, ctr, iv).get(day, [])
        check(len(bars) == 1 and bars[0][0].time() == keep_t,
              f"split keeps only the {ss.session_of(iv)} bar")
        check(ctr["non_rth"] == 2,
              f"split drops the 2 out-of-session bars for {iv}")


# --- C. storage validates each file against its OWN session ------------------

def test_storage_session_validate():
    root = fresh_root()
    day = date(2024, 6, 17)
    post_bars = [(datetime.combine(day, time(16, 0)), 100.0, 100.1, 99.9,
                  100.05, 5),
                 (datetime.combine(day, time(18, 30)), 101.0, 101.1, 100.9,
                  101.05, 7)]
    p = ss.month_file_path(root, "AAPL", 2024, 6, "1m-post")
    ss.write_month_file(p, post_bars)
    back, _ = ss.read_month_file(p)
    check(back == post_bars, "post bars round-trip in a -post file")

    def rejects(interval, bar):
        try:
            ss.write_month_file(
                ss.month_file_path(root, "MSFT", 2024, 6, interval), [bar])
            return False
        except ss.StorageFormatError:
            return True

    rth_bar = (datetime.combine(day, time(10, 0)), 1.0, 1.0, 1.0, 1.0, 1)
    post_bar = (datetime.combine(day, time(18, 0)), 1.0, 1.0, 1.0, 1.0, 1)
    check(rejects("1m-post", rth_bar), "RTH-time bar rejected from a -post file")
    check(rejects("1m", post_bar), "post-time bar rejected from an RTH file")
    check(rejects("1m-pre", post_bar), "post-time bar rejected from a -pre file")


# --- D. end-to-end: gap_fill stores each session in its own file --------------

def _three_day_book():
    # Three actual open sessions. June 19 is closed in the committed authority;
    # an owned nightly root must not accept fabricated holiday bars.
    mon = date(2024, 6, 10)
    days = {}
    for i in range(3):
        d = mon + timedelta(days=i)
        days[d] = (
            [stub(datetime.combine(d, time(8, 0 + i)), 50 + i)]          # pre
            + [stub(datetime.combine(d, time(10, 0)), 100 + i),
               stub(datetime.combine(d, time(11, 0)), 101 + i)]          # rth
            + [stub(datetime.combine(d, time(17, 0 + i)), 150 + i)])      # post
    return mon, days


def _stored_times(root, ticker, interval):
    man = ss.load_manifest(Path(root) / ticker) or {}
    months = sorted(ss.manifest_months(man, interval))
    out = []
    for mk in months:
        bars, _ = ss.read_month_file(
            ss.month_file_path(root, ticker, int(mk[:4]), int(mk[5:7]),
                               interval))
        out.extend(b[0].time() for b in bars)
    return out


def test_end_to_end_sessions():
    mon, days = _three_day_book()
    today = mon + timedelta(days=2)
    for interval, want in (("1m", {time(10, 0), time(11, 0)}),
                           ("1m-post", {time(17, 0), time(17, 1), time(17, 2)}),
                           ("1m-pre", {time(8, 0), time(8, 1), time(8, 2)})):
        root = fresh_root()
        adapter = ExtAdapter(dict(days),
                             head=datetime(2024, 6, 1, tzinfo=timezone.utc))
        rep = run(root, [("AAPL", interval)], adapter, today, since=mon)
        check(rep["totals"]["added"] > 0, f"{interval}: bars stored")
        times = set(_stored_times(root, "AAPL", interval))
        check(times == want,
              f"{interval}: stored ONLY its session bars (got {sorted(times)})")
        # the file actually carries the session suffix
        man = ss.load_manifest(Path(root) / "AAPL") or {}
        check(interval in (man.get("intervals") or {}),
              f"{interval}: manifest has the {interval} series")
        # extended series drove the adapter with useRTH=False
        if interval != "1m":
            check(all(call[3] is (call[2] == "1 day") for call in adapter.fetch_calls),
                  f"{interval}: fetched with useRTH=False")
        else:
            check(all(call[3] is True for call in adapter.fetch_calls),
                  "1m: fetched with useRTH=True")


def test_combined_extended_fetch():
    """The full {1m, 1m-pre, 1m-post} set must populate each scoped cache.
    Each token requests its own spans once, while each file still keeps only
    its own session's bars. The cache prevents a second per-series fetch."""
    mon, days = _three_day_book()
    today = mon + timedelta(days=2)
    root = fresh_root()
    adapter = ExtAdapter(dict(days),
                         head=datetime(2024, 6, 1, tzinfo=timezone.utc))
    messages = []
    rep = run(root, [("AAPL", "1m"), ("AAPL", "1m-pre"), ("AAPL", "1m-post")],
              adapter, today, since=mon, progress=messages.append)
    check(any("separate token-scoped requests" in message for message in messages)
          and not any("useRTH=False once" in message for message in messages),
          "combined: progress describes token-specific requests honestly")
    check(rep["totals"]["added"] > 0, "combined: bars stored")
    check(set(_stored_times(root, "AAPL", "1m")) == {time(10, 0), time(11, 0)},
          "combined: rth file keeps only rth bars")
    check(set(_stored_times(root, "AAPL", "1m-pre"))
          == {time(8, 0), time(8, 1), time(8, 2)},
          "combined: pre file keeps only pre bars")
    check(set(_stored_times(root, "AAPL", "1m-post"))
          == {time(17, 0), time(17, 1), time(17, 2)},
          "combined: post file keeps only post bars")
    # Each token now owns its envelope. The daily probe and RTH span use True;
    # the two pre and two post spans use False. No extra per-series refetch.
    rths = [c[3] for c in adapter.fetch_calls]
    check(rths == [True, True, False, False, False, False],
          f"combined: canonical per-token RTH flags (got {rths})")
    check(len(adapter.fetch_calls) == 6,
          f"combined: five scoped spans plus one daily probe, {len(adapter.fetch_calls)} calls")


def test_combined_falls_back_partial_set():
    """A PARTIAL set (only rth + pre, no post) must NOT use the combined path —
    each series fetches independently (rth useRTH=True, pre useRTH=False)."""
    mon, days = _three_day_book()
    today = mon + timedelta(days=2)
    root = fresh_root()
    adapter = ExtAdapter(dict(days),
                         head=datetime(2024, 6, 1, tzinfo=timezone.utc))
    run(root, [("AAPL", "1m"), ("AAPL", "1m-pre")], adapter, today, since=mon)
    check(set(_stored_times(root, "AAPL", "1m")) == {time(10, 0), time(11, 0)},
          "partial: rth bars stored")
    check(set(_stored_times(root, "AAPL", "1m-pre"))
          == {time(8, 0), time(8, 1), time(8, 2)},
          "partial: pre bars stored")
    # the RTH series fetched useRTH=True (NOT routed through the combined path)
    check(any(c[3] is True for c in adapter.fetch_calls),
          "partial: rth still fetched with useRTH=True (no combine)")


def test_export_sessions():
    import export_csv as xc
    root = fresh_root()
    day = date(2024, 6, 17)

    def seed(interval, t, price):
        bars = [(datetime.combine(day, t), price, price + 0.1, price - 0.1,
                 price + 0.05, 9)]
        stats = ss.write_month_file(
            ss.month_file_path(root, "AAPL", 2024, 6, interval), bars)
        tdir = Path(root) / "AAPL"
        man = ss.load_manifest(tdir) or ss.new_manifest("AAPL", "AAPL")
        ss.manifest_months(man, interval)[ss.month_key(2024, 6)] = dict(
            stats, status="present", source="seed")
        ss.save_manifest(tdir, man)

    seed("1m-pre", time(8, 0), 50)
    seed("1m", time(10, 0), 100)
    seed("1m-post", time(17, 0), 150)

    out = root.parent / "combined.csv"
    res = xc.export_sessions_csv(root, "AAPL", "1m", ["rth", "pre", "post"],
                                day, day, out)
    check(res["rows"] == 3, "export_sessions merges all 3 sessions")
    lines = out.read_bytes().decode().splitlines()
    check(lines[0] == ss.HEADER, "export header is canonical (no session col)")
    times = [ln.split(",")[1] for ln in lines[1:]]
    check(times == ["8:00:00", "10:00:00", "17:00:00"],
          "export merged in chronological time order")

    out2 = root.parent / "post_only.csv"
    res2 = xc.export_sessions_csv(root, "AAPL", "1m", ["post"], day, day, out2)
    check(res2["rows"] == 1 and res2["per_session"] == {"post": 1},
          "export single session (post) only")
    l2 = out2.read_bytes().decode().splitlines()
    check(l2[0] == ss.HEADER and l2[1].split(",")[1] == "17:00:00",
          "post-only export is correct + canonical")
    # single-series export of an extended token via the existing function
    out3 = root.parent / "pre_only.csv"
    r3 = xc.export_combined_csv(root, "AAPL", "1m-pre", day, day, out3)
    check(r3["rows"] == 1, "export_combined_csv handles a -pre token")


def test_convert_bars_forwards_interval():
    # REGRESSION for the HIGH bug: convert_bars must forward `interval` to
    # split_session_bars, or sub-minute -pre/-post series store nothing and the
    # spot-check false-alarms.
    day = date(2024, 6, 17)
    raw = [stub(datetime.combine(day, time(8, 0)), 50),
           stub(datetime.combine(day, time(10, 0)), 100),
           stub(datetime.combine(day, time(17, 0)), 150)]
    for iv, want_t in (("1s", time(10, 0)), ("1s-pre", time(8, 0)),
                       ("1s-post", time(17, 0))):
        ctr = {"non_rth": 0, "invalid": 0, "outside_day": 0}
        out = sk.convert_bars(raw, day, ctr, iv)
        check(len(out) == 1 and out[0][0].time() == want_t,
              f"convert_bars forwards interval for {iv} "
              f"(got {[b[0].time() for b in out]})")


class _WindowAdapter(ExtAdapter):
    """Sub-minute fetch returns ONLY the bars inside each intra-day window, so
    the per-day windowing path is exercised faithfully (no duplication)."""

    def fetch(self, contract, end_dt, duration, bar_size):
        self.fetch_calls.append((end_dt, duration, bar_size, self.use_rth))
        db = self.days.get(end_dt.date(), [])
        if duration.endswith("S"):
            secs = int(duration.split()[0])
            start = end_dt - timedelta(seconds=secs)
            out = []
            for b in db:
                bt = b.date.astimezone(NY).replace(tzinfo=None)
                if start <= bt < end_dt:
                    out.append(b)
            return out
        return super().fetch(contract, end_dt, duration, bar_size)


def test_subminute_extended_end_to_end():
    # the SUB-MINUTE extended path (day_requests windows + convert_bars) must
    # store the session's bars — the path the HIGH bug silently emptied.
    day = date(2024, 6, 17)
    book = {day: [stub(datetime.combine(day, time(8, 0, 5)), 50),     # pre
                  stub(datetime.combine(day, time(10, 0, 5)), 100),   # rth
                  stub(datetime.combine(day, time(17, 0, 5)), 150)]}  # post
    for iv, want_t in (("1s-post", time(17, 0, 5)),
                       ("1s-pre", time(8, 0, 5))):
        root = fresh_root()
        ad = _WindowAdapter(dict(book),
                            head=datetime(2024, 6, 1, tzinfo=timezone.utc))
        run(root, [("AAPL", iv)], ad, day, since=day)
        times = set(_stored_times(root, "AAPL", iv))
        check(times == {want_t},
              f"sub-minute {iv} stored its session bar (got {sorted(times)})")
        check(all(c[3] is (c[2] == "1 day") for c in ad.fetch_calls),
              f"sub-minute {iv} fetched with useRTH=False")


def test_storage_boundary_bars():
    # the exact session-edge seconds must validate + round-trip in their file
    root = fresh_root()
    day = date(2024, 6, 17)
    cases = {
        "1m-post": [(datetime.combine(day, time(16, 0, 0)), 1.0, 1.0, 1.0,
                     1.0, 1),
                    (datetime.combine(day, time(19, 59, 59)), 2.0, 2.0, 2.0,
                     2.0, 2)],
        "1m-pre": [(datetime.combine(day, time(4, 0, 0)), 1.0, 1.0, 1.0,
                    1.0, 1),
                   (datetime.combine(day, time(9, 29, 59)), 2.0, 2.0, 2.0,
                    2.0, 2)],
    }
    for iv, bars in cases.items():
        p = ss.month_file_path(root, "ZZ", 2024, 6, iv)
        ss.write_month_file(p, bars)            # verify_codec ON by default
        back, _ = ss.read_month_file(p)
        check(back == bars, f"{iv} boundary bars round-trip (codec + read)")


def test_filename_re_dash_ticker():
    m = ss.FILENAME_RE.match("BRK-B_2024-06_1m-post.csv")
    check(m and m.groups() == ("BRK-B", "2024", "06", "1m-post", "csv"),
          "FILENAME_RE parses a dash ticker + -post suffix")
    mp = ss.FILENAME_RE.match("BRK-B_2024-06_1m-post.parquet")
    check(mp and mp.groups() == ("BRK-B", "2024", "06", "1m-post", "parquet"),
          "FILENAME_RE parses a .parquet month file")
    m2 = ss.FILENAME_RE.match("BRK-B_2024-06_1m-pre.csv")
    check(m2 and m2.group(4) == "1m-pre", "FILENAME_RE: BRK-B -pre")
    m3 = ss.FILENAME_RE.match("BRK-B_2024-06_1m.csv")
    check(m3 and m3.group(4) == "1m", "FILENAME_RE: BRK-B plain RTH")


def test_export_holes_not_archived():
    import export_csv as xc
    root = fresh_root()
    day = date(2024, 6, 17)
    # seed only rth + post (NO pre)
    for iv, t, pr in (("1m", time(10, 0), 100), ("1m-post", time(17, 0), 150)):
        stats = ss.write_month_file(
            ss.month_file_path(root, "AAPL", 2024, 6, iv),
            [(datetime.combine(day, t), pr, pr + .1, pr - .1, pr + .05, 9)])
        tdir = Path(root) / "AAPL"
        man = ss.load_manifest(tdir) or ss.new_manifest("AAPL", "AAPL")
        ss.manifest_months(man, iv)[ss.month_key(2024, 6)] = dict(
            stats, status="present", source="seed")
        ss.save_manifest(tdir, man)
    out = root.parent / "holes.csv"
    res = xc.export_sessions_csv(root, "AAPL", "1m", ["rth", "pre", "post"],
                                day, day, out)
    check(res["per_session"]["pre"] == 0, "never-archived session has 0 rows")
    check("pre:not-archived" in res["holes"],
          "never-archived session reported once, not per-month")
    check(res["rows"] == 2, "rth+post bars exported")


def test_estimate_backfill_session_head():
    # Real head-kind/session evidence now runs over all 132 tokens in the
    # confined workflow suite. This legacy fixture checks the production hold.
    import tempfile
    for iv in ("1m", "1m-post", "1m-pre"):
        called, refused = [], False
        def forbidden():
            called.append(True)
            raise AssertionError("held estimate reached its factory")
        with tempfile.TemporaryDirectory() as evidence:
            try:
                sk.estimate_backfill("AAPL", iv, adapter_factory=forbidden,
                                     evidence_dir=evidence)
            except sk.AuthorityError:
                refused = True
        check(refused and not called, f"estimate_backfill {iv} stays held before connection")


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
    total = _PASS[0] + _FAIL[0]
    print(f"\nextended_hours_selftest: {_PASS[0]}/{total} passed, "
          f"{_FAIL[0]} failed")
    return 1 if _FAIL[0] else 0


if __name__ == "__main__":
    sys.exit(main())
