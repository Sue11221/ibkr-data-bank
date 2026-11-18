"""LIVE — needs IBKR ports, run OFF-PEAK; DO NOT run during a fetch run.

Live smoke tests for the kind/daily feature (OPTION_IMPLIED_VOLATILITY "iv"
and HISTORICAL_VOLATILITY "hvol") against the REAL IBKR demo. A bug hunt found
the feature broke in production though all offline selftests passed, because
the FakeAdapter fed idealized bars. These tests CONFIRM how the demo really
responds:

  - daily bars arrive as a datetime.DATE (formatDate=2 epoch-encodes only
    intraday) and must store on the correct calendar date (no tz off-by-one);
  - IBKR serves an env-dependent volume PLACEHOLDER for iv/hvol (demo=1.0;
    some report -1) -> normalized to 0 on store;
  - whatToShow returns 0-1 RATIOS, not dollar prices; IV is RTH-only;
    HVOL is daily-only ('1 min' HVOL returns ZERO bars).

This module is BUILD-ONLY at import time: every test lives inside a function and
nothing runs until the `if __name__ == "__main__":` guard. Run it LATER, by
hand, off-peak, against a demo TWS/Gateway (ports 2000-9000, readonly,
reqMarketDataType(3) is set by LiveIB.connect()).

Usage:
    python live_kind_smoke.py --port 2000 --ticker AAPL
    python live_kind_smoke.py --port 7497            # paper default

Covers TODO §A tests 1-6 and §B test 7 (span probe). The span probe can also be
run on its own from live_span_probe.py if present, but it is included here too.
"""

import argparse
import random
import shutil
import sys
import tempfile
import time as _time
from datetime import date, datetime, timedelta
from pathlib import Path

# Make the engine package importable whether run from the engine dir or above it.
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import stock_ibkr as sk          # noqa: E402
import stock_storage as ss       # noqa: E402
import stock_basis as sb         # noqa: E402
import fetch_ibkr_bridge as fib  # noqa: E402
import fetch_operations as fops  # noqa: E402
import fetch_operation_report as freport  # noqa: E402
import fetch_diagnostic_cli as cli  # noqa: E402
import operation_gate             # noqa: E402
from fetch_authority import AuthorityError  # noqa: E402
from fetch_envelopes import carrier_start  # noqa: E402
from fetch_ledger import LedgerError, inspect_ledger  # noqa: E402
from fetch_run_context import RequestCancelled, RequestRefused  # noqa: E402

_IV_DAILY = "ibkr.live_kind_smoke.iv_daily"
_IV_INTRADAY = "ibkr.live_kind_smoke.iv_intraday"
_HVOL_DAILY = "ibkr.live_kind_smoke.hvol_daily"
_HVOL_INTRADAY = "ibkr.live_kind_smoke.hvol_intraday"
_IV_RTH = "ibkr.live_kind_smoke.iv_rth"
_SPAN = "ibkr.live_kind_smoke.span_probe"
_RAW_RIGHTS = frozenset({"ibkr.choke.qualify", _IV_DAILY, _IV_INTRADAY,
                         _HVOL_DAILY, _HVOL_INTRADAY, _IV_RTH})
_SPAN_RIGHTS = frozenset({"ibkr.choke.qualify", _SPAN})
_FILL_RIGHTS = frozenset({
    "ibkr.gap_fill.month_daily", "ibkr.gap_fill.month_intraday",
    "ibkr.gap_fill.session_fetch", "ibkr.gap_fill.head_daily_probe",
    "ibkr.gap_fill.head_timestamp", "ibkr.earliest_available.head_timestamp",
    "ibkr.choke.qualify", "ibkr.choke.qualify_many",
})
_TERMINAL = (AuthorityError, LedgerError, RequestCancelled)


def _request_end(token, supplied):
    """Use the parent's frozen settled horizon unless an explicit end is given."""
    context = fib.current_worker().context
    horizon = context.horizons[token]
    if horizon is None:
        raise RequestRefused(f"kind diagnostic has no settled {token} horizon")
    return fib.ny_bound(supplied) if supplied is not None else horizon


def _raw_result(request):
    """Read this logical send's durable pre-filter diagnostic result."""
    context = fib.current_worker().context
    events = inspect_ledger(context.ledger.path, require_seal=False)["events"]
    matches = [event["payload"] for event in events
               if event["event"] == "result" and event["logical_id"] == request.logical_id]
    if len(matches) != 1:
        raise LedgerError("unfiltered diagnostic result is missing or duplicated")
    return matches[0]


def _observer_text(exc):
    """Format an external exception without exposing the current worker."""
    return fib.without_authority(str)(exc)


def _disconnect_observer(adapter, primary=None):
    """Resolve disconnect descriptors under observer isolation; keep stops loud."""
    if adapter is None:
        return
    try:
        fib.without_authority(lambda: adapter.disconnect())()
    except _TERMINAL as cleanup:
        if isinstance(primary, _TERMINAL):
            raise primary from cleanup
        raise
    except Exception as exc:  # noqa: BLE001 - ordinary cleanup is advisory
        print(f"  disconnect raised (non-fatal): {_observer_text(exc)}")


# --------------------------------------------------------------------------
# tiny PASS/FAIL harness
# --------------------------------------------------------------------------
class _Tally:
    """Counts check() results; exit code is 0 iff every check passed."""

    def __init__(self):
        self.passed = 0
        self.failed = 0

    def check(self, cond, msg):
        if cond:
            self.passed += 1
            print(f"  PASS  {msg}")
        else:
            self.failed += 1
            print(f"  FAIL  {msg}")
        return bool(cond)

    def exit_code(self):
        return 0 if self.failed == 0 else 1


def _banner(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


# --------------------------------------------------------------------------
# shared connect helper
# --------------------------------------------------------------------------
def connect(port, client_id=sk.CLIENT_ID_FETCH):
    """Open ONE LiveIB on `port` (connect() sets reqMarketDataType(3)).
    Returns the connected adapter; the CALLER must disconnect() in a finally."""
    adapter = sk.LiveIB(
        host=sk.HOST_DEFAULT,
        ports=(int(port),),
        client_id=client_id,
    ).connect()
    print(f"  connected on port {adapter.port}, account={adapter.account()}")
    return adapter


def _qualify(adapter, ticker):
    conid, contract = adapter.qualify(ticker)
    print(f"  qualified {ticker}: conId={conid}")
    return conid, contract


def _is_ratio(x):
    """A 0-1 ratio (IV/HVOL) — NOT a dollar price. Allow a hair over 1.0 for
    the rare very-high-IV print, but reject anything that looks like a price."""
    try:
        return 0.0 <= float(x) <= 2.0
    except (TypeError, ValueError):
        return False


def _show_bars(bars, n=3):
    for b in bars[:n]:
        print(f"    date={b.date!r}  o={b.open} h={b.high} "
              f"l={b.low} c={b.close} vol={b.volume}")


# --------------------------------------------------------------------------
# TEST 1 — RAW DAILY SHAPE (§A)
# --------------------------------------------------------------------------
@fib.worker_scope
def test_raw_daily_shape(adapter, contract, t, end_dt):
    """fetch '5 D' '1 day' OPTION_IMPLIED_VOLATILITY -> daily IV bars.
    Assert: len>0, type(bar.date) is datetime.date (NOT datetime), and report
    bar.volume (expect the -1 sentinel)."""
    _banner("[1] RAW DAILY SHAPE — IV '5 D' '1 day'")
    end = _request_end("1d-iv", end_dt)
    request = fib.bar_request(_IV_DAILY, contract, "1d-iv", end, "5 D")
    with fib.adapter_session(adapter, True), fib.send_scope(request, fib.acquire_turn):
        bars = adapter.fetch(contract, end.replace(tzinfo=None), "5 D", "1 day",
                             what_to_show="OPTION_IMPLIED_VOLATILITY")
    print(f"  fetched {len(bars)} bar(s)")
    _show_bars(bars)
    t.check(len(bars) > 0, "daily IV fetch returned at least one bar")
    if not bars:
        return
    b = bars[0]
    # CRITICAL: daily bars are datetime.date, NOT datetime (formatDate=2 only
    # epoch-encodes intraday). A bare date must NOT be astimezone'd.
    t.check(isinstance(b.date, date) and not isinstance(b.date, datetime),
            f"daily bar.date is datetime.date, not datetime "
            f"(got {type(b.date).__name__})")
    # Volume is an env-dependent PLACEHOLDER for computed kinds (demo serves 1.0,
    # some report -1) and is normalized to 0 on STORE — report it, don't gate on it.
    t.check(isinstance(b.volume, (int, float)),
            f"raw daily IV volume is a numeric placeholder = {b.volume} "
            f"(normalized to 0 on store)")
    # values are 0-1 ratios, not dollar prices.
    t.check(_is_ratio(b.close),
            f"daily IV close looks like a 0-1 ratio (got {b.close})")


# --------------------------------------------------------------------------
# TEST 2 — RAW IV INTRADAY (§A)
# --------------------------------------------------------------------------
@fib.worker_scope
def test_raw_iv_intraday(adapter, contract, t, end_dt):
    """fetch '1 D' '1 min' OPTION_IMPLIED_VOLATILITY -> intraday IV bars look
    like 0-1 ratios (not dollar prices), volume is -1."""
    _banner("[2] RAW IV INTRADAY — IV '1 D' '1 min'")
    end = _request_end("1m-iv", end_dt)
    request = fib.bar_request(_IV_INTRADAY, contract, "1m-iv", end, "1 D")
    with fib.adapter_session(adapter, True), fib.send_scope(request, fib.acquire_turn):
        bars = adapter.fetch(contract, end.replace(tzinfo=None), "1 D", "1 min",
                             what_to_show="OPTION_IMPLIED_VOLATILITY")
    print(f"  fetched {len(bars)} bar(s)")
    _show_bars(bars)
    t.check(len(bars) > 0, "intraday IV fetch returned bars")
    if not bars:
        return
    # intraday bars carry a datetime (aware-UTC via formatDate=2), not a date.
    b = bars[0]
    t.check(isinstance(b.date, datetime),
            f"intraday IV bar.date is a datetime (got {type(b.date).__name__})")
    ratios_ok = all(_is_ratio(x.close) for x in bars)
    t.check(ratios_ok, "all intraday IV closes look like 0-1 ratios")
    vols = sorted({x.volume for x in bars})
    t.check(all(isinstance(x.volume, (int, float)) for x in bars),
            f"intraday IV volume is a numeric placeholder {vols[:3]} "
            f"(normalized to 0 on store)")


# --------------------------------------------------------------------------
# TEST 3 — HVOL DAILY + daily-only (§A)
# --------------------------------------------------------------------------
@fib.worker_scope
def test_hvol_daily(adapter, contract, t, end_dt):
    """fetch '5 D' '1 day' HISTORICAL_VOLATILITY -> bars exist, ratios; and
    confirm the ENGINE GUARD makes HVOL daily-only (plan_gap rejects '1m-hvol').
    The raw '1 min' HVOL observation is not stored by this diagnostic;
    separately authorized C2 ratio correction paths are unchanged."""
    _banner("[3] HVOL DAILY — '5 D' '1 day' + daily-only at '1 min'")
    end = _request_end("1d-hvol", end_dt)
    request = fib.bar_request(_HVOL_DAILY, contract, "1d-hvol", end, "5 D")
    with fib.adapter_session(adapter, True), fib.send_scope(request, fib.acquire_turn):
        bars = adapter.fetch(contract, end.replace(tzinfo=None), "5 D", "1 day",
                             what_to_show="HISTORICAL_VOLATILITY")
    print(f"  daily HVOL: fetched {len(bars)} bar(s)")
    _show_bars(bars)
    t.check(len(bars) > 0, "daily HVOL fetch returned bars")
    if bars:
        b = bars[0]
        t.check(isinstance(b.date, date) and not isinstance(b.date, datetime),
                f"daily HVOL bar.date is datetime.date "
                f"(got {type(b.date).__name__})")
        t.check(_is_ratio(b.close),
                f"daily HVOL close looks like a 0-1 ratio (got {b.close})")
        t.check(isinstance(b.volume, (int, float)),
                f"daily HVOL volume is a numeric placeholder = {b.volume} "
                f"(normalized to 0 on store)")

    # HVOL daily-only: the RAW IBKR '1 min' HVOL response is QUIRKY on the demo
    # (a few stale points, not a clean 0 or 390) — informational only. What
    # actually protects the bank is the ENGINE GUARD: plan_gap REJECTS any
    # sub-daily HVOL token and the GUI forces HVOL to 1d. Verify that guard.
    print("  raw IBKR HVOL '1 min' (informational; no store here):")
    try:
        minute_end = _request_end("1m-hvol", end_dt)
        request = fib.bar_request(_HVOL_INTRADAY, contract, "1m-hvol",
                                  minute_end, "1 D")
        with fib.adapter_session(adapter, True), fib.send_scope(request, fib.acquire_turn):
            m = adapter.fetch(contract, minute_end.replace(tzinfo=None), "1 D", "1 min",
                              what_to_show="HISTORICAL_VOLATILITY")
        print(f"    returned {len(m)} bar(s) — quirky, irrelevant to the bank")
    except sk.SeriesHalt as exc:
        print(f"    halted: {_observer_text(exc)}")
    err = (sk.plan_gap(".", "X", "1m-hvol") or {}).get("error") or ""
    t.check("daily" in err.lower(),
            f"engine GUARD: plan_gap rejects sub-daily HVOL ('1m-hvol') -> {err!r}")


# --------------------------------------------------------------------------
# TEST 4 — IV RTH-ONLY (§A)
# --------------------------------------------------------------------------
@fib.worker_scope
def test_iv_rth_only(adapter, contract, t, end_dt):
    """Fetch unfiltered IV and check durable pre-filter RTH evidence."""
    _banner("[4] IV RTH-ONLY — intraday IV with use_rth=False")
    context = fib.current_worker().context
    end = _request_end("1m-iv", end_dt)
    if end_dt is None:
        # The approved raw diagnostic must not consume the captured current
        # day, even when its intraday horizon has already settled.
        day = min(end.date(), context.captured_now.date() - timedelta(days=1))
        while day >= context.authority.first_date:
            candidate = context.authority.window("1m-iv", day)
            if candidate is not None and candidate[1] <= end:
                end = candidate[1]
                break
            day -= timedelta(days=1)
        else:
            raise RequestRefused("kind IV diagnostic has no prior settled RTH session")
    window = context.authority.window("1m-iv", end.date())
    if window is None:
        raise RequestRefused("kind IV diagnostic has no covered RTH session")
    request = fib.bar_request(_IV_RTH, contract, "1m-iv", end, "1 D",
                              start=window[0], variant="ibkr-bars-unfiltered")
    with fib.send_scope(request, fib.acquire_turn):
        bars = adapter.fetch(contract, end.replace(tzinfo=None), "1 D", "1 min",
                             what_to_show="OPTION_IMPLIED_VOLATILITY")
    evidence = _raw_result(request)
    print(f"  fetched {len(bars)} bar(s) with use_rth=False")
    t.check(evidence["rows_accepted"] > 0,
            "IV fetch (use_rth=False) still returns settled RTH bars")
    if not bars:
        t.check(evidence["rows_outside_rth"] == 0,
                "all raw IV bars fall inside the RTH session")
        return

    print(f"  raw bars outside RTH window: {evidence['rows_outside_rth']}")
    if evidence["first_outside_timestamps"]:
        print(f"    first few outside: {evidence['first_outside_timestamps']}")
    t.check(evidence["rows_outside_rth"] == 0,
            "all raw IV bars fall inside the RTH session (IV is RTH-only)")


# --------------------------------------------------------------------------
# TEST 5 — END-TO-END STORE (§A)
# --------------------------------------------------------------------------
@fib.worker_scope
def test_end_to_end_store(port, ticker, t, *, adapter_factory=None, cancel=None):
    """Drive gap_fill for the selected ticker's IV/HVOL daily/minute tokens
    into a temp root; assert report series added>0, no halt; then READ BACK and
    assert bars persisted on the correct calendar dates with values in [0,1] and
    volume stored as 0 (the -1 sentinel coerced)."""
    _banner("[5] END-TO-END STORE — gap_fill iv/hvol + readback")
    worker = fib.current_worker()
    root = Path(fib.without_authority(tempfile.mkdtemp)(
        prefix="live_kind_")) / ss.STORAGE_DIR_NAME
    try:
        factory = adapter_factory or sk.live_adapter_factory(
            host=sk.HOST_DEFAULT, ports=(int(port),),
            client_id=sk.CLIENT_ID_FETCH)
        today = worker.context.captured_now.date()
        since = today - timedelta(days=21)        # ~3 weeks of trading days
        selections = [(ticker, "1d-iv"), (ticker, "1m-iv"), (ticker, "1d-hvol")]

        # pre-resolve once (as the GUI does) to skip a second qualify sweep.
        val = sk.validate_symbols([ticker], adapter_factory=factory,
            cancel=cancel, identity_today=today,
            _fetch_child=fops.narrow_worker_child(worker, "kind-store-identity",
                rights={"ibkr.choke.qualify_many"}))
        resolved = val.get("resolved") or {}
        print(f"  resolved: {resolved}")

        report = sk.gap_fill(
            root, selections,
            progress=lambda m: print(f"    > {m}"),
            adapter_factory=factory,
            pacer=sk.Pacer(),                     # fresh window, not the singleton
            today=today,
            since=since,
            resolved=resolved,
            cancel=cancel,
            _fetch_child=fops.narrow_worker_child(worker, "kind-store-fill",
                rights=_FILL_RIGHTS),
        )
        t.check(not report.get("aborted"),
                f"gap_fill did not abort (aborted={report.get('aborted')})")
        series = {(s["ticker"], s["interval"]): s
                  for s in report.get("series", [])}
        for sel in selections:
            s = series.get(sel)
            t.check(s is not None, f"series record present for {sel}")
            if s is None:
                continue
            t.check(not s.get("halt"),
                    f"{sel} did not halt (halt={s.get('halt')})")
            t.check((s.get("added") or 0) > 0,
                    f"{sel} added>0 (added={s.get('added')})")

        # ---- READ BACK each series and validate stored shape ----
        for interval in ("1d-iv", "1m-iv", "1d-hvol"):
            _readback_ratio_series(root, ticker, interval, t)

    finally:
        fib.without_authority(shutil.rmtree)(root.parent, ignore_errors=True)


def _readback_ratio_series(root, ticker, interval, t):
    bars, notes = sb.read_series(root, ticker, interval)
    print(f"  readback {interval}: {len(bars)} bar(s) notes={notes}")
    t.check(len(bars) > 0, f"{interval}: bars persisted on disk")
    if not bars:
        return
    is_daily = ss.base_interval(interval) == "1d"
    bad_range = []
    bad_vol = []
    bad_date = []
    seen_dates = set()
    for tup in bars:
        # bars tuple is EXACTLY (dt, open, high, low, close, volume)
        dt, o, h, lo, c, v = tup
        seen_dates.add(dt.date())
        if not all(0.0 <= float(x) <= 1.0 for x in (o, h, lo, c)):
            bad_range.append(tup)
        # -1 sentinel must be coerced to 0 on store for ratio kinds.
        if float(v) != 0.0:
            bad_vol.append(v)
        if is_daily:
            # stored daily bar is naive midnight on the CALENDAR date (no tz
            # off-by-one); intraday bars keep their real time.
            if (dt.tzinfo is not None or dt.hour or dt.minute or dt.second):
                bad_date.append(dt)
    print(f"    dates: {sorted(seen_dates)}")
    t.check(not bad_range,
            f"{interval}: all OHLC values in [0,1] "
            f"({len(bad_range)} offenders)")
    t.check(not bad_vol,
            f"{interval}: stored volume coerced to 0 "
            f"({len(bad_vol)} non-zero: {bad_vol[:5]})")
    if is_daily:
        t.check(not bad_date,
                f"{interval}: daily bars are naive-midnight on the correct "
                f"calendar date ({len(bad_date)} off: {bad_date[:5]})")


# --------------------------------------------------------------------------
# TEST 6 — DAILY UPDATE (re-run idempotency / _interval_seconds fix) (§A)
# --------------------------------------------------------------------------
@fib.worker_scope
def test_daily_update_twice(port, ticker, t, *, adapter_factory=None, cancel=None):
    """Run gap_fill for ('AAPL','1d-iv') TWICE into the SAME root; assert the
    2nd run does not halt with "series failed: 'd'" (the _interval_seconds fix)
    and appends / does not crash."""
    _banner("[6] DAILY UPDATE — gap_fill '1d-iv' twice, same root")
    worker = fib.current_worker()
    root = Path(fib.without_authority(tempfile.mkdtemp)(
        prefix="live_kind2_")) / ss.STORAGE_DIR_NAME
    try:
        factory = adapter_factory or sk.live_adapter_factory(
            host=sk.HOST_DEFAULT, ports=(int(port),),
            client_id=sk.CLIENT_ID_FETCH)
        today = worker.context.captured_now.date()
        since = today - timedelta(days=21)
        selections = [(ticker, "1d-iv")]

        def _run(label):
            print(f"  --- run {label} ---")
            rep = sk.gap_fill(
                root, selections,
                progress=lambda m: print(f"    > {m}"),
                adapter_factory=factory,
                pacer=sk.Pacer(),
                today=today, since=since,
                cancel=cancel,
                _fetch_child=fops.narrow_worker_child(worker,
                    "kind-update-" + label.split()[0], rights=_FILL_RIGHTS),
            )
            s = next((x for x in rep.get("series", [])
                      if (x["ticker"], x["interval"]) == (ticker, "1d-iv")),
                     None)
            return rep, s

        rep1, s1 = _run("1 (build)")
        t.check(s1 is not None and not s1.get("halt"),
                f"run1 '1d-iv' did not halt (halt="
                f"{s1.get('halt') if s1 else 'MISSING'})")

        rep2, s2 = _run("2 (update)")
        t.check(not rep2.get("aborted"),
                f"run2 did not abort (aborted={rep2.get('aborted')})")
        halt2 = (s2 or {}).get("halt")
        t.check(s2 is not None, "run2 produced a '1d-iv' series record")
        # The specific regression: a daily re-run halting with "series failed: 'd'"
        # (the _interval_seconds token bug). ANY halt mentioning the 'd' KeyError
        # is the failure we are guarding against.
        bad = bool(halt2) and ("'d'" in str(halt2)
                               or "series failed" in str(halt2))
        t.check(not bad,
                f"run2 did NOT halt with the _interval_seconds 'd' failure "
                f"(halt={halt2})")
    finally:
        fib.without_authority(shutil.rmtree)(root.parent, ignore_errors=True)


# --------------------------------------------------------------------------
# TEST 7 — SPAN PROBE (§B)
# --------------------------------------------------------------------------
@fib.worker_scope
def test_span_probe(adapter, contract, t, end_dt):
    """Time a single daily TRADES fetch over increasing durations to help pick
    _FETCH_SPAN['1d']. Prints bars + seconds for each duration; soft PASS as long
    as the longest covered span returns bars within the per-request timeout."""
    _banner("[7] SPAN PROBE (§B) — daily TRADES over '1 Y'..'10 Y'")
    durations = ["1 Y", "2 Y", "5 Y", "10 Y"]
    results = []
    end = _request_end("1d", end_dt)
    first_covered = fib.current_worker().context.authority.first_date
    for dur in durations:
        if carrier_start(end, dur).date() < first_covered:
            print(f"  {dur:>5}: UNSUPPORTED by covered calendar (starts before "
                  f"{first_covered})")
            results.append((dur, None, None))
            continue
        t0 = _time.monotonic()
        try:
            request = fib.bar_request(_SPAN, contract, "1d", end, dur)
            with fib.adapter_session(adapter, True), fib.send_scope(request, fib.acquire_turn):
                bars = adapter.fetch(contract, end.replace(tzinfo=None), dur, "1 day",
                                     what_to_show="TRADES")
            dt_s = _time.monotonic() - t0
            n = len(bars)
            first = bars[0].date if bars else None
            last = bars[-1].date if bars else None
            print(f"  {dur:>5}: {n:>5} bars in {dt_s:6.2f}s  "
                  f"({first} .. {last})")
            results.append((dur, n, dt_s))
        except sk.RequestTimeout as exc:
            dt_s = _time.monotonic() - t0
            print(f"  {dur:>5}: TIMEOUT after {dt_s:6.2f}s ({_observer_text(exc)})")
            results.append((dur, 0, dt_s))
        except (sk.SeriesHalt, sk.ConnectionError) as exc:
            print(f"  {dur:>5}: error ({type(exc).__name__}: {_observer_text(exc)})")
            results.append((dur, 0, None))
    # Soft check: the longest probed span returned data (so _FETCH_SPAN['1d']
    # could be widened toward it). Not a hard correctness gate.
    supported = [row for row in results if row[1] is not None]
    longest = supported[-1] if supported else (None, 0, None)
    t.check(longest[1] > 0,
            f"longest covered daily span {longest[0]!r} returned bars "
            f"({longest[1]})")
    print("  (use the bars/seconds above to choose _FETCH_SPAN['1d'])")


# --------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------
@fib.worker_scope
def run_raw_group(port, ticker, t, end_dt=None, *, adapter_factory=None, cancel=None):
    """Tests 1-4 (raw IV/HVOL bar SHAPES) on ONE shared connection. Self-contained
    (connect + qualify + disconnect) so the parallel runner can fire it on its own
    port. Ordinary connection/halt errors become failed checks; terminal
    authority, ledger and cancellation errors still propagate."""
    adapter = None
    try:
        if cancel is not None and cancel.is_set():
            raise RequestCancelled("kind raw group cancelled")
        _banner(f"CONNECT raw (port {port})")
        adapter = fib.without_authority(
            adapter_factory if adapter_factory is not None else lambda: connect(port))()
        with fib.qualification_scope("qualify", [ticker], fib.acquire_turn,
                                     cancel=cancel):
            _conid, contract = _qualify(adapter, ticker)
        for fn, rights in ((test_raw_daily_shape, {_IV_DAILY}),
                           (test_raw_iv_intraday, {_IV_INTRADAY}),
                           (test_hvol_daily, {_HVOL_DAILY, _HVOL_INTRADAY}),
                           (test_iv_rth_only, {_IV_RTH})):
            if cancel is not None and cancel.is_set():
                raise RequestCancelled("kind raw group cancelled")
            try:
                fn(adapter, contract, t, end_dt,
                   _fetch_child=fops.narrow_worker_child(
                       fib.current_worker(), fn.__name__, rights=rights))
            except _TERMINAL:
                raise
            except sk.SeriesHalt as exc:
                t.check(False, f"{fn.__name__} halted: {_observer_text(exc)}")
            except sk.ConnectionError as exc:
                t.check(False, f"{fn.__name__} connection error: {_observer_text(exc)}")
    except _TERMINAL:
        raise
    except sk.SeriesHalt as exc:
        t.check(False, f"raw qualify halted: {_observer_text(exc)}")
    except ConnectionError as exc:
        t.check(False, f"raw: could not connect on port {port}: {_observer_text(exc)}")
    finally:
        _disconnect_observer(adapter, sys.exc_info()[1])


@fib.worker_scope
def run_span_group(port, ticker, t, end_dt=None, *, adapter_factory=None, cancel=None):
    """Test 7 (§B daily span probe) on its own connection."""
    adapter = None
    try:
        if cancel is not None and cancel.is_set():
            raise RequestCancelled("kind span group cancelled")
        _banner(f"CONNECT span (port {port})")
        adapter = fib.without_authority(
            adapter_factory if adapter_factory is not None else lambda: connect(port))()
        with fib.qualification_scope("qualify", [ticker], fib.acquire_turn,
                                     cancel=cancel):
            _conid, contract = _qualify(adapter, ticker)
        test_span_probe(adapter, contract, t, end_dt,
            _fetch_child=fops.narrow_worker_child(
                fib.current_worker(), "span-requests", rights={_SPAN}))
    except _TERMINAL:
        raise
    except sk.SeriesHalt as exc:
        t.check(False, f"span qualify halted: {_observer_text(exc)}")
    except ConnectionError as exc:
        t.check(False, f"span: could not connect on port {port}: {_observer_text(exc)}")
    finally:
        _disconnect_observer(adapter, sys.exc_info()[1])


def _kind_body(operation, port, ticker, skip_span, adapter_factory, cancel):
    if type(port) is not int or not 0 < port < 65536:
        raise RequestRefused("kind diagnostic port must be a valid integer")
    if type(ticker) is not str or not ticker.strip() or ticker != ticker.strip():
        raise RequestRefused("kind diagnostic ticker must be a canonical symbol")
    if type(skip_span) is not bool:
        raise RequestRefused("kind diagnostic skip_span must be boolean")
    if cancel is not None and cancel.is_set():
        raise RequestCancelled("kind diagnostic cancelled before connection")
    tally = fib.observer_object(_Tally())
    print(f"LIVE kind/daily smoke — port={port} ticker={ticker} "
          f"captured={operation.context.captured_now.isoformat()}")
    run_raw_group(port, ticker, tally, adapter_factory=adapter_factory, cancel=cancel,
        _fetch_child=operation.child("kind-raw", rights=_RAW_RIGHTS))
    if not skip_span:
        run_span_group(port, ticker, tally, adapter_factory=adapter_factory,
            cancel=cancel, _fetch_child=operation.child("kind-span", rights=_SPAN_RIGHTS))
    try:
        test_end_to_end_store(port, ticker, tally, adapter_factory=adapter_factory,
            cancel=cancel, _fetch_child=operation.child("kind-store",
                rights=_FILL_RIGHTS))
    except _TERMINAL:
        raise
    except ConnectionError as exc:
        tally.check(False, f"end-to-end store could not connect: {_observer_text(exc)}")
    try:
        test_daily_update_twice(port, ticker, tally, adapter_factory=adapter_factory,
            cancel=cancel, _fetch_child=operation.child("kind-update",
                rights=_FILL_RIGHTS))
    except _TERMINAL:
        raise
    except ConnectionError as exc:
        tally.check(False, f"daily-update could not connect: {_observer_text(exc)}")
    _banner("SUMMARY")
    print(f"  passed={tally.passed}  failed={tally.failed}")
    return {"exit_code": tally.exit_code(),
            "status": "complete" if tally.failed == 0 else "partial",
            "passed": tally.passed, "failed": tally.failed}


def run_kind_smoke(*, port=2000, ticker="AAPL", skip_span=False,
                   adapter_factory=None, cancel=None, evidence_dir=None,
                   _test_capability=None):
    """One held parent across every raw, span, store and update group."""
    directory = sk._auxiliary_directory(evidence_dir).resolve()
    operation = fops.begin_operation("diagnostic", directory,
                                     test_capability=_test_capability)
    context = operation.context
    output, failure, outcome, lease = None, None, "returned", None
    try:
        fops.require_root_admission(operation)
        lease = fib.without_authority(operation_gate.acquire)(
            "fetch", owner="Kind smoke diagnostic")
        output = _kind_body(operation, port, ticker, skip_span, adapter_factory,
                            fib.observer_object(cancel))
        if output["status"] != "complete":
            outcome = "partial"
        operation.seal()
    except BaseException as exc:
        failure, outcome = exc, "failed"
    try:
        operation.close()
    except BaseException as exc:
        failure, outcome = failure if failure is not None else exc, "failed"
    if lease is not None:
        try:
            fib.without_authority(lambda: lease.release())()
        except BaseException as exc:
            failure, outcome = failure if failure is not None else exc, "failed"
    evidence = fib.without_authority(freport.evidence)(
        context, "diagnostic", outcome=outcome, error=failure)
    try:
        evidence["report_path"] = fib.without_authority(freport.write_report)(
            evidence, directory)
        evidence["report_persisted"] = True
    except BaseException as exc:
        evidence.update(verified=False, state="UNVERIFIED", report_persisted=False,
            outcome="report_failed",
            report_failure=f"{type(exc).__name__}: {_observer_text(exc)}"[:500])
        failure = failure if failure is not None else exc
    if failure is not None:
        try:
            fib.without_authority(setattr)(failure, "fetch_ledger", evidence)
        except BaseException:
            pass
        raise failure
    output["fetch_ledger"] = evidence
    return output


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="LIVE kind/daily smoke tests against the IBKR demo "
                    "(run OFF-PEAK; never during a fetch run).")
    ap.add_argument("--port", type=int, default=2000,
                    help="single demo port to connect to (default: 2000)")
    ap.add_argument("--ticker", default="AAPL",
                    help="symbol to test (default: AAPL)")
    ap.add_argument("--skip-span", action="store_true",
                    help="skip the slow §B span probe (test 7)")
    args = ap.parse_args(argv)

    try:
        return run_kind_smoke(port=args.port, ticker=args.ticker,
                              skip_span=args.skip_span)["exit_code"]
    except cli.TERMINAL as exc:
        return cli.terminal_exit(exc)


if __name__ == "__main__":
    sys.exit(main())
